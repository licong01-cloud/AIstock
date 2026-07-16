"""Scheduler-owned authority preload for MiniQMT Phase 1 quote projection.

Only this adapter may call calendar and market-data authorities.  It publishes
an all-or-nothing immutable context to the P1-C quote writer; callback and
writer paths never import a provider or open a database connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
import threading
from typing import Any, Callable, Iterable, Mapping

from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    DepthQuantityUnit,
    MarketCode,
    PriceBasis,
    SessionSegment,
    TradabilitySnapshot,
    TradabilityState,
    canonical_sha256,
    ensure_utc,
    exact_symbol,
)
from backend.execution_algos.adaptive_is.reasons import (
    QuoteContractError,
    QuoteContractReasonCode,
    QuoteContractStage,
    quote_contract_error,
)
from backend.execution_algos.board_lot import board_lot_rule
from backend.miniqmt_quote_contract_config import QuoteContractPolicy
from backend.services.miniqmt_execution_runtime.quote_eligibility import (
    A_SHARE_EQUITY_PHASE_SCHEDULE_VERSION,
    CHINA_TZ,
    ClockContinuityTracker,
    MINIQMT_QUOTE_CLOCK_DOMAIN_ID,
    QuoteEvaluationContext,
    QuoteEvaluationContextStore,
    QuoteSymbolContext,
    build_execution_clock_event,
)
from backend.services.miniqmt_execution_runtime.quote_normalizer import MINIQMT_NORMALIZER_MAP_VERSION
from backend.services.paper_trading_v2.market_data import (
    EquityInstrumentMetadataProvider,
    LimitPriceProvider,
    PreviousCloseProvider,
    SuspendStatusProvider,
)
from backend.services.trading_calendar_status import TradingCalendarStatusService


MINIQMT_WHOLE_QUOTE_DEPTH_UNIT_EVIDENCE_VERSION = (
    f"xtquant_whole_quote_bidVol_askVol_shares_v1:{MINIQMT_NORMALIZER_MAP_VERSION}"
)
A_SHARE_BOARD_LOT_AUTHORITY_VERSION = "backend.execution_algos.board_lot:a_share_board_lot_v20260504"


@dataclass(frozen=True)
class QuoteContextSymbolSpec:
    """Frozen plan metadata for one active Phase 1 symbol.

    Price tick, lot size, and depth-unit evidence are intentionally required;
    neither context preload nor the callback can infer them from a quote.
    """

    symbol: str
    depth_quantity_unit: DepthQuantityUnit
    unit_evidence_version: str
    price_tick: Decimal
    lot_size: int
    intraday_halt: bool
    intraday_halt_source: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", exact_symbol(self.symbol)[0])
        try:
            object.__setattr__(self, "depth_quantity_unit", DepthQuantityUnit(self.depth_quantity_unit))
        except (TypeError, ValueError) as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.UNIT_UNPROVEN,
                "context symbol depth unit must be explicitly registered",
                stage=QuoteContractStage.UNIT,
                context={"symbol": self.symbol, "depth_quantity_unit": str(self.depth_quantity_unit)},
            ) from exc
        if not self.unit_evidence_version.strip() or not self.intraday_halt_source.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "context symbol unit and intraday-halt evidence versions are required",
                context={"symbol": self.symbol},
            )
        if not isinstance(self.intraday_halt, bool):
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "context symbol intraday_halt must be an explicit boolean authority result",
                context={"symbol": self.symbol, "intraday_halt_type": type(self.intraday_halt).__name__},
            )
        if isinstance(self.lot_size, bool) or not isinstance(self.lot_size, int) or self.lot_size <= 0:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "context symbol lot_size must be an explicit positive integer",
                context={"symbol": self.symbol, "lot_size": self.lot_size},
            )
        try:
            tick = Decimal(str(self.price_tick))
        except Exception as exc:  # Decimal errors become registry-loud failures.
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "context symbol price_tick must be an explicit positive decimal",
                context={"symbol": self.symbol, "price_tick": str(self.price_tick)},
            ) from exc
        if not tick.is_finite() or tick <= 0:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "context symbol price_tick must be an explicit positive decimal",
                context={"symbol": self.symbol, "price_tick": str(self.price_tick)},
            )
        object.__setattr__(self, "price_tick", tick)


class MiniQMTInstrumentQuoteSpecProvider:
    """Translate complete xtdata instrument authority into strict context specs."""

    def __init__(self, instrument_reader: Callable[[str], Mapping[str, Any]]) -> None:
        if not callable(instrument_reader):
            raise TypeError("MiniQMT instrument quote spec provider requires a callable authority reader")
        self._instrument_reader = instrument_reader

    def get_symbol_spec(self, symbol: str) -> QuoteContextSymbolSpec:
        normalized_symbol, market = exact_symbol(symbol)
        try:
            raw = self._instrument_reader(normalized_symbol)
        except QuoteContractError:
            raise
        except Exception as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "MiniQMT instrument-detail authority read failed",
                stage=QuoteContractStage.TRADABILITY,
                context={"symbol": normalized_symbol, "exception_type": type(exc).__name__},
            ) from exc
        if not isinstance(raw, Mapping) or not raw:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "MiniQMT instrument-detail authority returned no record",
                stage=QuoteContractStage.TRADABILITY,
                context={"symbol": normalized_symbol},
            )
        code = str(raw.get("InstrumentID") or "").strip().upper()
        exchange = str(raw.get("ExchangeID") or raw.get("ExchangeCode") or "").strip().upper()
        expected_code, expected_market = normalized_symbol.split(".", 1)
        accepted_exchange_ids = {
            MarketCode.SH: frozenset({"SH", "SSE"}),
            MarketCode.SZ: frozenset({"SZ", "SZSE"}),
            MarketCode.BJ: frozenset({"BJ", "BSE"}),
        }[market]
        if code != expected_code or exchange not in accepted_exchange_ids or expected_market != market.value:
            raise quote_contract_error(
                QuoteContractReasonCode.SYMBOL_INVALID,
                "MiniQMT instrument-detail identity differs from the exact requested symbol",
                context={
                    "symbol": normalized_symbol,
                    "instrument_id": code,
                    "exchange_id": exchange,
                },
            )
        price_tick = _required_positive_decimal(raw.get("PriceTick"), field_name="PriceTick", symbol=normalized_symbol)
        qmt_min_limit_order_volume = _required_nonnegative_int(
            raw.get("MinLimitOrderVolume"),
            field_name="MinLimitOrderVolume",
            symbol=normalized_symbol,
        )
        is_trading = raw.get("IsTrading")
        instrument_status = _required_nonnegative_int(
            raw.get("InstrumentStatus"),
            field_name="InstrumentStatus",
            symbol=normalized_symbol,
        )
        if not isinstance(is_trading, bool):
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "MiniQMT instrument-detail trading status is incomplete",
                stage=QuoteContractStage.TRADABILITY,
                context={
                    "symbol": normalized_symbol,
                    "is_trading_type": type(is_trading).__name__,
                    "instrument_status": instrument_status,
                },
            )
        try:
            board_min_qty, board_increment = board_lot_rule(normalized_symbol)
        except ValueError as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "canonical A-share board-lot authority does not recognize the exact MiniQMT symbol",
                stage=QuoteContractStage.TRADABILITY,
                context={
                    "symbol": normalized_symbol,
                    "authority_version": A_SHARE_BOARD_LOT_AUTHORITY_VERSION,
                    "error": str(exc),
                },
            ) from exc
        if qmt_min_limit_order_volume > 0 and qmt_min_limit_order_volume != board_min_qty:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "MiniQMT instrument-detail minimum order volume conflicts with canonical A-share board-lot authority",
                stage=QuoteContractStage.TRADABILITY,
                context={
                    "symbol": normalized_symbol,
                    "qmt_min_limit_order_volume": qmt_min_limit_order_volume,
                    "canonical_board_min_qty": board_min_qty,
                    "canonical_board_increment": board_increment,
                    "authority_version": A_SHARE_BOARD_LOT_AUTHORITY_VERSION,
                },
            )
        authority_payload = {
            "symbol": normalized_symbol,
            "instrument_id": code,
            "exchange_id": exchange,
            "price_tick": price_tick,
            "lot_size": board_min_qty,
            "board_lot_increment": board_increment,
            "board_lot_authority_version": A_SHARE_BOARD_LOT_AUTHORITY_VERSION,
            "qmt_min_limit_order_volume": qmt_min_limit_order_volume,
            "qmt_min_limit_order_volume_state": (
                "CONSISTENT_POSITIVE_AUTHORITY" if qmt_min_limit_order_volume > 0 else "UNAVAILABLE_ZERO_SENTINEL"
            ),
            "is_trading": is_trading,
            "instrument_status": instrument_status,
            "intraday_halt": instrument_status >= 1,
            "intraday_halt_semantics": "xtquant.InstrumentStatus>=1",
            "depth_unit_evidence_version": MINIQMT_WHOLE_QUOTE_DEPTH_UNIT_EVIDENCE_VERSION,
        }
        authority_sha256 = canonical_sha256(authority_payload)
        return QuoteContextSymbolSpec(
            symbol=normalized_symbol,
            depth_quantity_unit=DepthQuantityUnit.SHARES,
            unit_evidence_version=(
                f"{MINIQMT_WHOLE_QUOTE_DEPTH_UNIT_EVIDENCE_VERSION}:{authority_sha256}"
            ),
            price_tick=price_tick,
            lot_size=board_min_qty,
            intraday_halt=instrument_status >= 1,
            intraday_halt_source=f"xtquant.get_instrument_detail.InstrumentStatus>=1:{authority_sha256}",
        )


@dataclass(frozen=True)
class _RegisteredRuntimeQuoteContext:
    policy: QuoteContractPolicy
    symbol_specs: tuple[QuoteContextSymbolSpec, ...]


class MiniQMTQuoteContextAuthorityAdapter:
    """Build and atomically publish a complete P1-C context from authorities."""

    def __init__(
        self,
        *,
        context_store: QuoteEvaluationContextStore,
        trading_calendar_service: TradingCalendarStatusService | Any,
        suspend_status_provider: SuspendStatusProvider,
        limit_price_provider: LimitPriceProvider,
        previous_close_provider: PreviousCloseProvider,
        equity_metadata_provider: EquityInstrumentMetadataProvider,
        clock_continuity_tracker: ClockContinuityTracker | None = None,
        symbol_specs_provider: Callable[[], Iterable[QuoteContextSymbolSpec]] | None = None,
        policy_provider: Callable[[], QuoteContractPolicy] | None = None,
        runtime_symbol_spec_provider: Callable[[str], QuoteContextSymbolSpec] | None = None,
        clock_domain_id: str = MINIQMT_QUOTE_CLOCK_DOMAIN_ID,
    ) -> None:
        self._context_store = context_store
        self._calendar = trading_calendar_service
        self._suspend = suspend_status_provider
        self._limit = limit_price_provider
        self._previous_close = previous_close_provider
        self._metadata = equity_metadata_provider
        self._continuity = clock_continuity_tracker or ClockContinuityTracker()
        self._symbol_specs_provider = symbol_specs_provider
        self._policy_provider = policy_provider
        self._runtime_symbol_spec_provider = runtime_symbol_spec_provider
        self._runtime_contexts: dict[str, _RegisteredRuntimeQuoteContext] = {}
        self._runtime_context_lock = threading.RLock()
        self._clock_domain_id = clock_domain_id
        if not self._clock_domain_id.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "quote context lifecycle clock domain id is required",
                stage=QuoteContractStage.CLOCK,
            )

    @property
    def context_store(self) -> QuoteEvaluationContextStore:
        return self._context_store

    def preload(
        self,
        *,
        symbol_specs: Iterable[QuoteContextSymbolSpec],
        policy: QuoteContractPolicy,
        clock_at_utc: datetime,
        clock_monotonic_ns: int,
        clock_domain_id: str,
        source: str = "simulation_lifecycle_scheduler",
    ) -> QuoteEvaluationContext:
        """Publish a full context or invalidate the old one loudly; never partial."""

        with self._runtime_context_lock:
            return self._preload_locked(
                symbol_specs=symbol_specs,
                policy=policy,
                clock_at_utc=clock_at_utc,
                clock_monotonic_ns=clock_monotonic_ns,
                clock_domain_id=clock_domain_id,
                source=source,
            )

    def _preload_locked(
        self,
        *,
        symbol_specs: Iterable[QuoteContextSymbolSpec],
        policy: QuoteContractPolicy,
        clock_at_utc: datetime,
        clock_monotonic_ns: int,
        clock_domain_id: str,
        source: str,
    ) -> QuoteEvaluationContext:

        try:
            at_utc = ensure_utc(clock_at_utc, field_name="quote_context.clock_at_utc")
            trade_date = at_utc.astimezone(CHINA_TZ).date()
            specs = self._normalize_specs(symbol_specs)
            calendar_set = self._build_calendar_snapshot_set(trade_date=trade_date, effective_at_utc=at_utc)
            clock = build_execution_clock_event(
                calendar_snapshot_set=calendar_set,
                clock_at_utc=at_utc,
                clock_monotonic_ns=clock_monotonic_ns,
                clock_domain_id=clock_domain_id,
                source=source,
                observed_at_utc=at_utc,
            )
            continuity = self._continuity.observe(
                clock=clock,
                calendar_snapshot_set=calendar_set,
                max_negative_skew_ms=policy.max_negative_skew_ms,
            )
            symbols = {
                spec.symbol: self._build_symbol_context(spec=spec, trade_date=trade_date, observed_at_utc=at_utc)
                for spec in specs
            }
            context = QuoteEvaluationContext(
                calendar_snapshot_set=calendar_set,
                clock=clock,
                continuity_generation=continuity.generation,
                continuity_valid=continuity.valid,
                policy=policy,
                symbols=symbols,
            )
        except QuoteContractError as error:
            self._context_store.invalidate(error)
            raise
        except Exception as exc:  # Provider errors are translated once, at the scheduler boundary.
            error = quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "MiniQMT quote context authority preload failed",
                stage=QuoteContractStage.TRADABILITY,
                context={"exception_type": type(exc).__name__},
            )
            self._context_store.invalidate(error)
            raise error from exc
        self._context_store.publish(context)
        return context

    def advance_clock(
        self,
        *,
        clock_at_utc: datetime,
        clock_monotonic_ns: int,
        source: str = "miniqmt_b0_quote_v2_lifecycle_tick",
    ) -> QuoteEvaluationContext:
        """Advance only the paired clock sample; never call a provider or broker."""

        with self._runtime_context_lock:
            context = self._context_store.snapshot()
            if context is None:
                raise quote_contract_error(
                    QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                    "runtime clock advance requires a published authority context",
                    stage=QuoteContractStage.CLOCK,
                )
            at_utc = ensure_utc(clock_at_utc, field_name="quote_context.clock_at_utc")
            clock = build_execution_clock_event(
                calendar_snapshot_set=context.calendar_snapshot_set,
                clock_at_utc=at_utc,
                clock_monotonic_ns=clock_monotonic_ns,
                clock_domain_id=self._clock_domain_id,
                source=source,
                observed_at_utc=at_utc,
            )
            continuity = self._continuity.observe(
                clock=clock,
                calendar_snapshot_set=context.calendar_snapshot_set,
                max_negative_skew_ms=context.policy.max_negative_skew_ms,
            )
            advanced = QuoteEvaluationContext(
                calendar_snapshot_set=context.calendar_snapshot_set,
                clock=clock,
                continuity_generation=continuity.generation,
                continuity_valid=continuity.valid,
                policy=context.policy,
                symbols=context.symbols,
            )
            self._context_store.publish(advanced)
            return advanced

    def health(self) -> dict[str, object]:
        with self._runtime_context_lock:
            runtime_contexts = dict(self._runtime_contexts)
        return {
            **self._context_store.health(),
            "registered_runtime_count": len(runtime_contexts),
            "registered_runtime_ids": sorted(runtime_contexts),
            "registered_policy_sha256": sorted(
                {registration.policy.policy_sha256 for registration in runtime_contexts.values()}
            ),
        }

    def registered_runtime_count(self) -> int:
        with self._runtime_context_lock:
            return len(self._runtime_contexts)

    def prepare_runtime_context(
        self,
        *,
        runtime_id: str,
        symbols: Iterable[str],
        execution_policy: Mapping[str, Any],
        clock_at_utc: datetime,
        clock_monotonic_ns: int,
        source: str = "simulation_lifecycle_scheduler_plan",
    ) -> QuoteEvaluationContext:
        exact_runtime_id = str(runtime_id or "").strip()
        if not exact_runtime_id:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "runtime quote context requires an authoritative runtime_id",
            )
        if self._runtime_symbol_spec_provider is None:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "runtime quote context requires an injected instrument authority",
                stage=QuoteContractStage.TRADABILITY,
                context={"runtime_id": exact_runtime_id},
            )
        policy = QuoteContractPolicy.from_execution_policy(execution_policy)
        normalized_symbols = tuple(sorted({exact_symbol(symbol)[0] for symbol in symbols}))
        if not normalized_symbols:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "runtime quote context requires the exact active parent symbol set",
                context={"runtime_id": exact_runtime_id},
            )
        specs = tuple(self._runtime_symbol_spec_provider(symbol) for symbol in normalized_symbols)
        with self._runtime_context_lock:
            prospective = dict(self._runtime_contexts)
            prospective[exact_runtime_id] = _RegisteredRuntimeQuoteContext(policy=policy, symbol_specs=specs)
            merged_policy, merged_specs = self._merge_runtime_contexts(prospective)
            context = self.preload(
                symbol_specs=merged_specs,
                policy=merged_policy,
                clock_at_utc=clock_at_utc,
                clock_monotonic_ns=clock_monotonic_ns,
                clock_domain_id=self._clock_domain_id,
                source=source,
            )
            self._runtime_contexts = prospective
            return context

    def release_runtime_context(self, runtime_id: str) -> None:
        exact_runtime_id = str(runtime_id or "").strip()
        if not exact_runtime_id:
            raise ValueError("runtime_id is required to release MiniQMT quote context")
        with self._runtime_context_lock:
            self._runtime_contexts.pop(exact_runtime_id, None)

    def refresh_lifecycle(self, *, clock_at_utc: datetime, clock_monotonic_ns: int) -> QuoteEvaluationContext:
        """Called by scheduler lifecycle only when explicit sources are configured."""

        with self._runtime_context_lock:
            runtime_contexts = dict(self._runtime_contexts)
        if runtime_contexts:
            policy, specs = self._merge_runtime_contexts(runtime_contexts)
            return self.preload(
                symbol_specs=specs,
                policy=policy,
                clock_at_utc=clock_at_utc,
                clock_monotonic_ns=clock_monotonic_ns,
                clock_domain_id=self._clock_domain_id,
            )
        if self._symbol_specs_provider is None or self._policy_provider is None:
            error = quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "quote context lifecycle refresh requires explicit symbol-spec and policy providers",
            )
            self._context_store.invalidate(error)
            raise error
        return self.preload(
            symbol_specs=self._symbol_specs_provider(),
            policy=self._policy_provider(),
            clock_at_utc=clock_at_utc,
            clock_monotonic_ns=clock_monotonic_ns,
            clock_domain_id=self._clock_domain_id,
        )

    @staticmethod
    def _merge_runtime_contexts(
        runtime_contexts: Mapping[str, _RegisteredRuntimeQuoteContext],
    ) -> tuple[QuoteContractPolicy, tuple[QuoteContextSymbolSpec, ...]]:
        policies = {registration.policy.policy_sha256: registration.policy for registration in runtime_contexts.values()}
        if len(policies) != 1:
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "one MiniQMT quote data session cannot mix frozen quote policies",
                context={"policy_sha256": sorted(policies), "legacy_fallback": False},
            )
        specs_by_symbol: dict[str, QuoteContextSymbolSpec] = {}
        for registration in runtime_contexts.values():
            for spec in registration.symbol_specs:
                previous = specs_by_symbol.get(spec.symbol)
                if previous is not None and previous != spec:
                    raise quote_contract_error(
                        QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                        "one MiniQMT quote data session received conflicting instrument authority",
                        context={"symbol": spec.symbol, "legacy_fallback": False},
                    )
                specs_by_symbol[spec.symbol] = spec
        return next(iter(policies.values())), tuple(specs_by_symbol[symbol] for symbol in sorted(specs_by_symbol))

    def _build_calendar_snapshot_set(self, *, trade_date: date, effective_at_utc: datetime) -> CalendarSnapshotSet:
        try:
            status = self._calendar.status(as_of_date=trade_date)
        except Exception as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "authoritative trading calendar status is unavailable",
                stage=QuoteContractStage.CALENDAR,
                context={"trade_date": trade_date.isoformat(), "exception_type": type(exc).__name__},
            ) from exc
        if not isinstance(status, dict) or status.get("as_of_date") != trade_date.isoformat() or not status.get("is_trading_day"):
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "authoritative calendar does not prove the scheduler date is a trading day",
                stage=QuoteContractStage.CALENDAR,
                context={"trade_date": trade_date.isoformat(), "calendar_status": dict(status) if isinstance(status, dict) else None},
            )
        cache = status.get("cache") if isinstance(status.get("cache"), dict) else {}
        checksum = cache.get("checksum")
        if not isinstance(checksum, str) or not checksum.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "authoritative calendar cache checksum is required for P1-C context",
                stage=QuoteContractStage.CALENDAR,
                context={"trade_date": trade_date.isoformat()},
            )
        source_version = f"{status.get('source')}:{checksum}:{A_SHARE_EQUITY_PHASE_SCHEDULE_VERSION}"
        segments = (
            SessionSegment(start_local=time(9, 15), end_local=time(9, 25)),
            SessionSegment(start_local=time(9, 30), end_local=time(11, 30)),
            SessionSegment(start_local=time(13, 0), end_local=time(14, 57)),
            SessionSegment(start_local=time(14, 57), end_local=time(15, 0)),
        )
        snapshots = {
            market: CalendarSnapshot(
                calendar_id=f"calendar_{market.value}_{trade_date.isoformat()}_{checksum[:16]}",
                market=market,
                trade_date=trade_date,
                timezone="Asia/Shanghai",
                session_segments=segments,
                effective_at_utc=effective_at_utc,
                source_version=source_version,
            )
            for market in MarketCode
        }
        set_payload = {
            "trade_date": trade_date,
            "checksum": checksum,
            "phase_schedule_version": A_SHARE_EQUITY_PHASE_SCHEDULE_VERSION,
            "snapshots": snapshots,
        }
        return CalendarSnapshotSet(
            snapshot_set_id=f"calendar_set_{canonical_sha256(set_payload)}",
            snapshot_by_market=snapshots,
        )

    @staticmethod
    def _normalize_specs(symbol_specs: Iterable[QuoteContextSymbolSpec]) -> tuple[QuoteContextSymbolSpec, ...]:
        specs = tuple(symbol_specs)
        normalized: dict[str, QuoteContextSymbolSpec] = {}
        for spec in specs:
            if not isinstance(spec, QuoteContextSymbolSpec):
                raise quote_contract_error(
                    QuoteContractReasonCode.PAYLOAD_INVALID,
                    "quote context preload requires QuoteContextSymbolSpec values",
                )
            if spec.symbol in normalized:
                raise quote_contract_error(
                    QuoteContractReasonCode.PAYLOAD_INVALID,
                    "quote context preload symbol specs must be exact and unique",
                    context={"symbol": spec.symbol},
                )
            normalized[spec.symbol] = spec
        return tuple(normalized.values())

    def _build_symbol_context(
        self,
        *,
        spec: QuoteContextSymbolSpec,
        trade_date: date,
        observed_at_utc: datetime,
    ) -> QuoteSymbolContext:
        metadata = self._metadata.get_equity_metadata(spec.symbol, trade_date)
        if metadata.symbol != spec.symbol:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "exact-symbol stock-basic metadata conflicts with context specification",
                context={"spec_symbol": spec.symbol, "metadata_symbol": metadata.symbol},
            )
        suspend = self._suspend.get_suspend_status(spec.symbol, trade_date)
        limit = self._limit.get_limit_price(spec.symbol, trade_date)
        previous_close = self._previous_close.get_previous_close(spec.symbol, trade_date)
        if suspend.symbol != spec.symbol or limit.symbol != spec.symbol or previous_close.symbol != spec.symbol:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "tradability authority returned a symbol different from the requested exact symbol",
                context={"symbol": spec.symbol},
            )
        listed_equity = metadata.is_listed_a_share_equity
        if not listed_equity:
            state = TradabilityState.STATUS_UNKNOWN
        elif bool(suspend.is_suspended):
            state = TradabilityState.SUSPENDED
        elif bool(spec.intraday_halt):
            state = TradabilityState.INTRADAY_HALT
        else:
            state = TradabilityState.TRADABLE
        pre_close = _positive_decimal_or_none(
            limit.pre_close if limit.pre_close is not None else previous_close.pre_close,
            field_name="pre_close",
            symbol=spec.symbol,
        )
        limit_up = _positive_decimal_or_none(limit.up_limit, field_name="limit_up", symbol=spec.symbol)
        limit_down = _positive_decimal_or_none(limit.down_limit, field_name="limit_down", symbol=spec.symbol)
        source_version = canonical_sha256(
            {
                "metadata": metadata.source_version,
                "suspend": suspend.source,
                "limit": getattr(limit, "source", "market.stk_limit"),
                "previous_close": previous_close.source,
                "intraday_halt_source": spec.intraday_halt_source,
                "unit_evidence_version": spec.unit_evidence_version,
            }
        )
        tradability = TradabilitySnapshot(
            schema_version="adaptive_is_tradability_snapshot_v1",
            tradability_id=f"tradability_{canonical_sha256({'symbol': spec.symbol, 'trade_date': trade_date, 'source_version': source_version})}",
            symbol=spec.symbol,
            market=exact_symbol(spec.symbol)[1],
            board=metadata.market,
            trade_date=trade_date,
            price_basis=PriceBasis.RAW_CNY_PER_SHARE,
            pre_close=pre_close,
            limit_up=limit_up,
            limit_down=limit_down,
            price_tick=spec.price_tick,
            lot_size=spec.lot_size,
            is_suspended=bool(suspend.is_suspended),
            suspension_source=suspend.source,
            security_status=f"list_status:{metadata.list_status}",
            openint_status=None,
            observed_at_utc=observed_at_utc,
            source="market.authority.preload",
            source_version=source_version,
            state=state,
            validation_reasons=()
            if listed_equity
            else (QuoteContractReasonCode.TRADABILITY_DATA_INVALID,),
        )
        return QuoteSymbolContext(
            symbol=spec.symbol,
            board=metadata.market,
            depth_quantity_unit=spec.depth_quantity_unit,
            unit_evidence_version=spec.unit_evidence_version,
            tradability=tradability,
            product_type=metadata.product_type,
            product_type_proven_equity=listed_equity,
            authority_source_version=source_version,
        )


def _required_positive_decimal(value: Any, *, field_name: str, symbol: str) -> Decimal:
    parsed = _positive_decimal_or_none(value, field_name=field_name, symbol=symbol)
    if parsed is None:
        raise quote_contract_error(
            QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
            "instrument authority requires a positive decimal field",
            stage=QuoteContractStage.TRADABILITY,
            context={"symbol": symbol, "field": field_name, "value": value},
        )
    return parsed


def _required_nonnegative_int(value: Any, *, field_name: str, symbol: str) -> int:
    if isinstance(value, bool):
        parsed = None
    else:
        try:
            decimal_value = Decimal(str(value))
            parsed = int(decimal_value) if decimal_value == decimal_value.to_integral_value() else None
        except (ArithmeticError, TypeError, ValueError):
            parsed = None
    if parsed is None or parsed < 0:
        raise quote_contract_error(
            QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
            "instrument authority requires a non-negative integral field",
            stage=QuoteContractStage.TRADABILITY,
            context={"symbol": symbol, "field": field_name, "value": value},
        )
    return parsed


def _positive_decimal_or_none(value: Any, *, field_name: str, symbol: str) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise quote_contract_error(
            QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
            "tradability authority returned a non-numeric price field",
            context={"symbol": symbol, "field": field_name, "value": str(value)},
        ) from exc
    if not parsed.is_finite() or parsed <= 0:
        raise quote_contract_error(
            QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
            "tradability authority returned a non-positive price field",
            context={"symbol": symbol, "field": field_name, "value": str(value)},
        )
    return parsed


__all__ = [
    "MINIQMT_WHOLE_QUOTE_DEPTH_UNIT_EVIDENCE_VERSION",
    "MiniQMTInstrumentQuoteSpecProvider",
    "MiniQMTQuoteContextAuthorityAdapter",
    "QuoteContextSymbolSpec",
]
