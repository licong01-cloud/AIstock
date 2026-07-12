"""Pure P1-C quote clock, ordering, freshness, and eligibility contracts.

This module deliberately has no database, FastAPI, broker, Paper Trading, or
scheduler imports.  Scheduler-owned code preloads immutable authority context;
the Phase 1 single writer only projects frames against that context.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, time, timedelta
from decimal import Decimal
from enum import Enum
import threading
from types import MappingProxyType
from typing import Mapping
from zoneinfo import ZoneInfo

from backend.execution_algos.adaptive_is.contracts import (
    ActionQuoteEligibility,
    CalendarSnapshotSet,
    ControlRevision,
    DepthQuantityUnit,
    EligibilityState,
    ExecutionClockEvent,
    FiveLevelQuote,
    MarketCode,
    MarketPhase,
    QuoteBatchAggregateState,
    QuoteCapability,
    QuoteSnapshotBatch,
    QuoteValidationState,
    TradabilitySnapshot,
    TradabilityState,
    canonical_sha256,
    ensure_utc,
    exact_symbol,
    require_identity,
    require_sha256,
)
from backend.execution_algos.adaptive_is.reasons import (
    QuoteContractError,
    QuoteContractReasonCode,
    QuoteContractStage,
    quote_contract_error,
)
from backend.miniqmt_quote_contract_config import QuoteContractPolicy
from backend.services.miniqmt_execution_runtime.quote_normalizer import RawQuoteFrame


A_SHARE_EQUITY_PHASE_SCHEDULE_VERSION = "A_SHARE_EQUITY_PHASE_SCHEDULE_V1_20260706"
CHINA_TZ = ZoneInfo("Asia/Shanghai")

# These are exact, versioned normalizer-map values.  The evaluator does not
# trim, case-fold, parse numerics, or otherwise infer an exchange phase.
OPENINT_PHASE_BY_NORMALIZER_VERSION: Mapping[str, Mapping[str, MarketPhase]] = MappingProxyType(
    {
        "miniqmt_quote_normalizer_map_v2": MappingProxyType(
            {
                "PRE_OPEN": MarketPhase.PRE_OPEN,
                "OPEN": MarketPhase.CONTINUOUS,
                "CONTINUOUS": MarketPhase.CONTINUOUS,
                "CLOSING_AUCTION": MarketPhase.CLOSING_AUCTION,
                "CLOSED": MarketPhase.CLOSED,
            }
        )
    }
)


def phase_for_shanghai_time(value: datetime) -> MarketPhase:
    """Project the versioned A-share equity schedule without heuristic dates."""

    local = ensure_utc(value, field_name="phase.clock_at_utc").astimezone(CHINA_TZ).timetz().replace(tzinfo=None)
    if time(9, 15) <= local < time(9, 25):
        return MarketPhase.PRE_OPEN
    if time(9, 30) <= local <= time(11, 30):
        return MarketPhase.CONTINUOUS
    if time(13, 0) <= local < time(14, 57):
        return MarketPhase.CONTINUOUS
    if time(14, 57) <= local < time(15, 0):
        return MarketPhase.CLOSING_AUCTION
    return MarketPhase.CLOSED


def build_execution_clock_event(
    *,
    calendar_snapshot_set: CalendarSnapshotSet,
    clock_at_utc: datetime,
    clock_monotonic_ns: int,
    clock_domain_id: str,
    source: str,
    observed_at_utc: datetime | None = None,
) -> ExecutionClockEvent:
    """Create the only P1-C clock shape from a paired wall/monotonic sample."""

    at_utc = ensure_utc(clock_at_utc, field_name="clock_at_utc")
    local_trade_date = at_utc.astimezone(CHINA_TZ).date()
    snapshot_dates = {snapshot.trade_date for snapshot in calendar_snapshot_set.snapshot_by_market.values()}
    if snapshot_dates != {local_trade_date}:
        raise quote_contract_error(
            QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
            "calendar snapshot trade date conflicts with scheduler clock",
            stage=QuoteContractStage.CALENDAR,
            context={
                "clock_trade_date": local_trade_date.isoformat(),
                "snapshot_trade_dates": sorted(item.isoformat() for item in snapshot_dates),
                "calendar_snapshot_set_id": calendar_snapshot_set.snapshot_set_id,
            },
        )
    phase = phase_for_shanghai_time(at_utc)
    event_payload = {
        "clock_at_utc": at_utc,
        "clock_monotonic_ns": clock_monotonic_ns,
        "clock_domain_id": clock_domain_id,
        "clock_trade_date": local_trade_date,
        "calendar_snapshot_set_id": calendar_snapshot_set.snapshot_set_id,
        "calendar_snapshot_set_sha256": calendar_snapshot_set.set_sha256,
        "phase_schedule_version": A_SHARE_EQUITY_PHASE_SCHEDULE_VERSION,
        "source": source,
    }
    return ExecutionClockEvent(
        clock_event_id=f"clock_{canonical_sha256(event_payload)}",
        clock_at_utc=at_utc,
        clock_monotonic_ns=clock_monotonic_ns,
        clock_domain_id=clock_domain_id,
        clock_trade_date=local_trade_date,
        calendar_snapshot_set_id=calendar_snapshot_set.snapshot_set_id,
        phase_by_market={market: phase for market in MarketCode},
        phase_schedule_version=A_SHARE_EQUITY_PHASE_SCHEDULE_VERSION,
        source=source,
        observed_at_utc=observed_at_utc or at_utc,
    )


@dataclass(frozen=True)
class ClockContinuityResult:
    generation: int
    valid: bool
    reset_reason: str | None


class ClockContinuityTracker:
    """Stateful but deterministic continuity tracker owned by scheduler code."""

    def __init__(self) -> None:
        self._last_clock: ExecutionClockEvent | None = None
        self._last_calendar_set_sha256: str | None = None
        self._generation = 0

    def observe(
        self,
        *,
        clock: ExecutionClockEvent,
        calendar_snapshot_set: CalendarSnapshotSet,
        max_negative_skew_ms: int,
    ) -> ClockContinuityResult:
        if max_negative_skew_ms < 0:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "max_negative_skew_ms cannot be negative",
            )
        current_identity = (
            clock.clock_domain_id,
            calendar_snapshot_set.set_sha256,
            clock.phase_schedule_version,
        )
        previous = self._last_clock
        previous_identity = (
            previous.clock_domain_id,
            self._last_calendar_set_sha256,
            previous.phase_schedule_version,
        ) if previous is not None else None
        reset_reason: str | None = None
        valid = True
        if previous is None:
            self._generation = 1
        elif current_identity != previous_identity:
            self._generation += 1
            reset_reason = "CLOCK_CONTINUITY_IDENTITY_CHANGED"
        else:
            wall_delta_ms = _timedelta_ms(clock.clock_at_utc - previous.clock_at_utc)
            if clock.clock_monotonic_ns < previous.clock_monotonic_ns:
                self._generation += 1
                reset_reason = "MONOTONIC_CLOCK_ROLLBACK"
                valid = False
            elif wall_delta_ms < -max_negative_skew_ms:
                self._generation += 1
                reset_reason = "WALL_CLOCK_ROLLBACK"
                valid = False
        self._last_clock = clock
        self._last_calendar_set_sha256 = calendar_snapshot_set.set_sha256
        return ClockContinuityResult(generation=self._generation, valid=valid, reset_reason=reset_reason)


@dataclass(frozen=True)
class QuoteSymbolContext:
    """Preloaded, side-neutral authority and unit evidence for one symbol."""

    symbol: str
    board: str
    depth_quantity_unit: DepthQuantityUnit
    unit_evidence_version: str
    tradability: TradabilitySnapshot | None
    product_type: str
    product_type_proven_equity: bool
    authority_source_version: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", exact_symbol(self.symbol)[0])
        if not isinstance(self.product_type_proven_equity, bool):
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "quote symbol context product-type proof must be an explicit boolean",
                stage=QuoteContractStage.TRADABILITY,
                context={
                    "symbol": self.symbol,
                    "product_type_proven_equity_type": type(self.product_type_proven_equity).__name__,
                },
            )
        if not self.board.strip() or not self.unit_evidence_version.strip() or not self.product_type.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "quote symbol context requires board, unit evidence, and product type",
            )
        try:
            object.__setattr__(self, "depth_quantity_unit", DepthQuantityUnit(self.depth_quantity_unit))
        except (TypeError, ValueError) as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.UNIT_UNPROVEN,
                "quote symbol context has an unregistered depth unit",
                stage=QuoteContractStage.UNIT,
                context={"symbol": self.symbol, "depth_quantity_unit": str(self.depth_quantity_unit)},
            ) from exc
        if self.tradability is not None and self.tradability.symbol != self.symbol:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "symbol context tradability does not match its symbol",
                context={"symbol": self.symbol, "tradability_symbol": self.tradability.symbol},
            )


@dataclass(frozen=True)
class QuoteEvaluationContext:
    """Immutable scheduler-published projection context; no callback reads providers."""

    calendar_snapshot_set: CalendarSnapshotSet
    clock: ExecutionClockEvent
    continuity_generation: int
    continuity_valid: bool
    policy: QuoteContractPolicy
    symbols: Mapping[str, QuoteSymbolContext]
    context_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            isinstance(self.continuity_generation, bool)
            or not isinstance(self.continuity_generation, int)
            or self.continuity_generation <= 0
        ):
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "quote evaluation context requires an explicit positive integer continuity generation",
            )
        if not isinstance(self.continuity_valid, bool):
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "quote evaluation context continuity validity must be an explicit boolean",
                context={"continuity_valid_type": type(self.continuity_valid).__name__},
            )
        if self.clock.calendar_snapshot_set_id != self.calendar_snapshot_set.snapshot_set_id:
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "clock calendar snapshot id conflicts with immutable context",
                stage=QuoteContractStage.CALENDAR,
            )
        snapshot_dates = {snapshot.trade_date for snapshot in self.calendar_snapshot_set.snapshot_by_market.values()}
        if snapshot_dates != {self.clock.clock_trade_date}:
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "quote evaluation context clock trade date conflicts with calendar snapshot set",
                stage=QuoteContractStage.CALENDAR,
                context={
                    "clock_trade_date": self.clock.clock_trade_date.isoformat(),
                    "snapshot_trade_dates": sorted(item.isoformat() for item in snapshot_dates),
                },
            )
        if self.clock.phase_schedule_version != A_SHARE_EQUITY_PHASE_SCHEDULE_VERSION:
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "quote evaluation context has an unknown phase schedule version",
                stage=QuoteContractStage.CALENDAR,
                context={"phase_schedule_version": self.clock.phase_schedule_version},
            )
        normalized: dict[str, QuoteSymbolContext] = {}
        for raw_symbol, item in self.symbols.items():
            symbol = exact_symbol(raw_symbol)[0]
            if symbol != item.symbol or symbol in normalized:
                raise quote_contract_error(
                    QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                    "quote evaluation context contains invalid symbol authority mapping",
                    context={"symbol": symbol},
                )
            normalized[symbol] = item
        object.__setattr__(self, "symbols", MappingProxyType(normalized))
        payload = {
            "calendar_snapshot_set_id": self.calendar_snapshot_set.snapshot_set_id,
            "calendar_snapshot_set_sha256": self.calendar_snapshot_set.set_sha256,
            "clock_event_id": self.clock.clock_event_id,
            "clock_domain_id": self.clock.clock_domain_id,
            "phase_schedule_version": self.clock.phase_schedule_version,
            "continuity_generation": self.continuity_generation,
            "continuity_valid": self.continuity_valid,
            "policy_sha256": self.policy.policy_sha256,
            "symbols": normalized,
        }
        object.__setattr__(self, "context_id", f"ctx_{canonical_sha256(payload)}")

    def symbol_context(self, symbol: str) -> QuoteSymbolContext | None:
        return self.symbols.get(exact_symbol(symbol)[0])


class QuoteEvaluationContextStore:
    """Atomic scheduler-to-writer context handoff with a read-only health view."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._context: QuoteEvaluationContext | None = None
        self._last_error: QuoteContractError | None = None

    def publish(self, context: QuoteEvaluationContext) -> None:
        if not isinstance(context, QuoteEvaluationContext):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "quote evaluation context store accepts only immutable context",
            )
        with self._lock:
            self._context = context
            self._last_error = None

    def invalidate(self, error: QuoteContractError) -> None:
        if not isinstance(error, QuoteContractError):
            raise TypeError("quote context invalidation requires QuoteContractError")
        with self._lock:
            self._context = None
            self._last_error = error

    def snapshot(self) -> QuoteEvaluationContext | None:
        with self._lock:
            return self._context

    def health(self) -> dict[str, object]:
        with self._lock:
            context = self._context
            error = self._last_error
        return {
            "status": "READY" if context is not None else "INVALID",
            "context_id": context.context_id if context is not None else None,
            "clock_event_id": context.clock.clock_event_id if context is not None else None,
            "calendar_snapshot_set_id": context.calendar_snapshot_set.snapshot_set_id if context is not None else None,
            "policy_sha256": context.policy.policy_sha256 if context is not None else None,
            "continuity_generation": context.continuity_generation if context is not None else None,
            "continuity_valid": context.continuity_valid if context is not None else False,
            "symbols": sorted(context.symbols) if context is not None else [],
            "last_error": error.as_loud_payload() if error is not None else None,
        }


class OrderingDisposition(str, Enum):
    ACCEPTED = "ACCEPTED"
    ACCEPTED_CORRECTION = "ACCEPTED_CORRECTION"
    EXACT_DUPLICATE = "EXACT_DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    STALE_GENERATION = "STALE_GENERATION"
    MISSING_SOURCE_TIME = "MISSING_SOURCE_TIME"


@dataclass(frozen=True)
class OrderingDecision:
    disposition: OrderingDisposition
    accepted: bool


class QuoteOrderingTracker:
    """Single-writer ordering state; it has no clock or database authority."""

    def __init__(self) -> None:
        self._active_generation: int | None = None
        self._seen_identities: set[tuple[str, int, str, str, str]] = set()
        self._latest_exchange_time_by_symbol: dict[str, datetime] = {}
        self._latest_ingress_sequence_by_symbol: dict[str, int] = {}

    def activate_generation(self, generation: int) -> None:
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise quote_contract_error(
                QuoteContractReasonCode.ORDERING_REJECTED,
                "active ingress generation must be a non-negative integer",
                context={"generation": generation},
            )
        if self._active_generation is None or generation > self._active_generation:
            self._active_generation = generation
            self._seen_identities.clear()
            self._latest_exchange_time_by_symbol.clear()
            self._latest_ingress_sequence_by_symbol.clear()
        elif generation < self._active_generation:
            raise quote_contract_error(
                QuoteContractReasonCode.ORDERING_REJECTED,
                "cannot activate an older ingress generation",
                context={"generation": generation, "active_generation": self._active_generation},
            )

    def decide(self, *, frame: RawQuoteFrame, quote: FiveLevelQuote) -> OrderingDecision:
        if self._active_generation is None:
            self.activate_generation(frame.ingress_generation)
        assert self._active_generation is not None
        if frame.ingress_generation < self._active_generation:
            return OrderingDecision(OrderingDisposition.STALE_GENERATION, accepted=False)
        if frame.ingress_generation > self._active_generation:
            return OrderingDecision(OrderingDisposition.STALE_GENERATION, accepted=False)
        identity = (
            frame.source_session_id,
            frame.ingress_generation,
            frame.symbol,
            canonical_sha256(frame.source_timestamp_raw),
            frame.source_payload_sha256,
        )
        if identity in self._seen_identities:
            return OrderingDecision(OrderingDisposition.EXACT_DUPLICATE, accepted=False)
        source_time = quote.source_exchange_time_utc
        if source_time is not None:
            previous = self._latest_exchange_time_by_symbol.get(frame.symbol)
            previous_sequence = self._latest_ingress_sequence_by_symbol.get(frame.symbol)
            if previous is not None and source_time < previous:
                return OrderingDecision(OrderingDisposition.OUT_OF_ORDER, accepted=False)
            correction = previous is not None and source_time == previous
            if correction and previous_sequence is not None and frame.ingress_sequence <= previous_sequence:
                return OrderingDecision(OrderingDisposition.OUT_OF_ORDER, accepted=False)
            self._latest_exchange_time_by_symbol[frame.symbol] = source_time
            self._latest_ingress_sequence_by_symbol[frame.symbol] = frame.ingress_sequence
            self._seen_identities.add(identity)
            return OrderingDecision(
                OrderingDisposition.ACCEPTED_CORRECTION if correction else OrderingDisposition.ACCEPTED,
                accepted=True,
            )
        self._seen_identities.add(identity)
        return OrderingDecision(OrderingDisposition.MISSING_SOURCE_TIME, accepted=True)

    def health(self) -> dict[str, object]:
        return {
            "active_generation": self._active_generation,
            "exact_identity_count": len(self._seen_identities),
            "latest_exchange_time_symbols": sorted(self._latest_exchange_time_by_symbol),
            "latest_ingress_sequence_symbols": sorted(self._latest_ingress_sequence_by_symbol),
        }


@dataclass(frozen=True)
class NormalizedQuoteObservation:
    """Accepted in-memory observation.  It is explicitly not durable evidence."""

    frame: RawQuoteFrame
    quote: FiveLevelQuote
    tradability: TradabilitySnapshot | None
    context_id: str
    market_data_id: str
    ordering_disposition: OrderingDisposition

    def __post_init__(self) -> None:
        if self.frame.symbol != self.quote.symbol:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "normalized observation frame and quote symbols conflict",
            )
        if self.tradability is not None and self.tradability.symbol != self.quote.symbol:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "normalized observation tradability and quote symbols conflict",
            )
        if not self.context_id.startswith("ctx_") or not self.market_data_id.startswith("md_"):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "normalized observation requires deterministic context and market-data identities",
            )
        try:
            disposition = OrderingDisposition(self.ordering_disposition)
        except (TypeError, ValueError) as exc:
            raise quote_contract_error(
                QuoteContractReasonCode.ORDERING_REJECTED,
                "normalized observation requires a registered ordering disposition",
                context={"ordering_disposition": str(self.ordering_disposition)},
            ) from exc
        object.__setattr__(self, "ordering_disposition", disposition)


class BoundedNormalizedQuoteStore:
    """Bounded latest accepted normalized observation by admitted symbol."""

    def __init__(self, *, max_symbols: int) -> None:
        if isinstance(max_symbols, bool) or not isinstance(max_symbols, int) or max_symbols <= 0:
            raise quote_contract_error(
                QuoteContractReasonCode.CAPACITY_EXCEEDED,
                "normalized quote store max_symbols must be a positive integer",
                context={"max_symbols": max_symbols},
            )
        self._lock = threading.RLock()
        self._max_symbols = max_symbols
        self._admitted: set[str] = set()
        self._latest_by_symbol: dict[str, NormalizedQuoteObservation] = {}

    def replace_admitted(self, symbols: tuple[str, ...]) -> None:
        normalized = {exact_symbol(symbol)[0] for symbol in symbols}
        if len(normalized) > self._max_symbols:
            raise quote_contract_error(
                QuoteContractReasonCode.CAPACITY_EXCEEDED,
                "normalized quote admission exceeds configured capacity",
                context={"requested_symbol_count": len(normalized), "max_symbols": self._max_symbols},
            )
        with self._lock:
            self._admitted = normalized
            self._latest_by_symbol = {
                symbol: observation
                for symbol, observation in self._latest_by_symbol.items()
                if symbol in normalized
            }

    def accept(self, observation: NormalizedQuoteObservation) -> None:
        with self._lock:
            if observation.quote.symbol not in self._admitted:
                raise quote_contract_error(
                    QuoteContractReasonCode.UNEXPECTED_SYMBOL,
                    "normalized quote store rejected a symbol outside the active lease union",
                    context={"symbol": observation.quote.symbol, "max_symbols": self._max_symbols},
                )
            self._latest_by_symbol[observation.quote.symbol] = observation

    def get(self, symbol: str, *, context_id: str | None = None) -> NormalizedQuoteObservation | None:
        normalized = exact_symbol(symbol)[0]
        with self._lock:
            observation = self._latest_by_symbol.get(normalized)
        if observation is None or (context_id is not None and observation.context_id != context_id):
            return None
        return observation

    def snapshot(self) -> dict[str, NormalizedQuoteObservation]:
        with self._lock:
            return dict(self._latest_by_symbol)

    def health(self) -> dict[str, object]:
        with self._lock:
            return {
                "max_symbols": self._max_symbols,
                "admitted_symbols": sorted(self._admitted),
                "stored_symbols": sorted(self._latest_by_symbol),
                "stored_count": len(self._latest_by_symbol),
            }


def deterministic_market_data_id(
    *,
    frame: RawQuoteFrame,
    quote: FiveLevelQuote,
    tradability: TradabilitySnapshot,
    calendar_snapshot_set: CalendarSnapshotSet,
    policy: QuoteContractPolicy,
) -> str:
    """Stable P1-C in-memory id; P1-D must persist this exact value."""

    payload = {
        "source_session_id": frame.source_session_id,
        "ingress_generation": frame.ingress_generation,
        "symbol": frame.symbol,
        "source_timestamp_raw": frame.source_timestamp_raw,
        "source_payload_sha256": frame.source_payload_sha256,
        "normalized_quote_sha256": quote.normalized_quote_sha256,
        "tradability_evidence_sha256": tradability.evidence_sha256,
        "calendar_snapshot_set_sha256": calendar_snapshot_set.set_sha256,
        "policy_sha256": policy.policy_sha256,
    }
    return f"md_{canonical_sha256(payload)}"


@dataclass(frozen=True)
class FreshnessResult:
    ready: bool
    wall_receive_age_ms: int | None
    monotonic_receive_age_ms: int | None
    source_lag_ms: int | None
    exchange_age_ms: int | None
    clock_age_divergence_ms: int | None
    failure_reasons: tuple[str, ...]


def evaluate_freshness(*, quote: FiveLevelQuote, clock: ExecutionClockEvent, policy: QuoteContractPolicy) -> FreshnessResult:
    if quote.clock_domain_id != clock.clock_domain_id:
        return FreshnessResult(False, None, None, None, None, None, ("CLOCK_DOMAIN_MISMATCH",))
    wall_receive_age_ms = _timedelta_ms(clock.clock_at_utc - quote.received_at_utc)
    monotonic_receive_age_ms = _ns_to_ms(clock.clock_monotonic_ns - quote.received_monotonic_ns)
    if quote.source_exchange_time_utc is None:
        return FreshnessResult(
            False,
            wall_receive_age_ms,
            monotonic_receive_age_ms,
            None,
            None,
            None,
            ("EXCHANGE_TIMESTAMP_MISSING",),
        )
    source_lag_ms = _timedelta_ms(quote.received_at_utc - quote.source_exchange_time_utc)
    exchange_age_ms = _timedelta_ms(clock.clock_at_utc - quote.source_exchange_time_utc)
    divergence_ms = abs(wall_receive_age_ms - monotonic_receive_age_ms)
    raw_ages = {
        "wall_receive_age_ms": wall_receive_age_ms,
        "monotonic_receive_age_ms": monotonic_receive_age_ms,
        "source_lag_ms": source_lag_ms,
        "exchange_age_ms": exchange_age_ms,
    }
    failures = [
        f"{name}:NEGATIVE_SKEW"
        for name, value in raw_ages.items()
        if value < -policy.max_negative_skew_ms
    ]
    if wall_receive_age_ms > policy.max_receive_age_ms:
        failures.append("wall_receive_age_ms:STALE")
    if monotonic_receive_age_ms > policy.max_receive_age_ms:
        failures.append("monotonic_receive_age_ms:STALE")
    if source_lag_ms > policy.max_source_lag_ms:
        failures.append("source_lag_ms:STALE")
    if exchange_age_ms > policy.max_exchange_age_ms:
        failures.append("exchange_age_ms:STALE")
    if divergence_ms > policy.max_clock_age_divergence_ms:
        failures.append("clock_age_divergence_ms:STALE")
    return FreshnessResult(
        ready=not failures,
        wall_receive_age_ms=wall_receive_age_ms,
        monotonic_receive_age_ms=monotonic_receive_age_ms,
        source_lag_ms=source_lag_ms,
        exchange_age_ms=exchange_age_ms,
        clock_age_divergence_ms=divergence_ms,
        failure_reasons=tuple(failures),
    )


@dataclass(frozen=True)
class ActionQuoteRequest:
    runtime_id: str
    parent_intent_id: str
    algo_instance_id: str
    symbol: str
    side: str
    control_revision: ControlRevision
    policy_sha256: str
    config_sha256: str
    adapter_sha256: str
    dependency_group_id: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("runtime_id", "parent_intent_id", "algo_instance_id"):
            object.__setattr__(self, field_name, require_identity(getattr(self, field_name), field_name=f"request.{field_name}"))
        object.__setattr__(self, "symbol", exact_symbol(self.symbol)[0])
        side = str(self.side).strip().upper()
        if side not in {"BUY", "SELL"}:
            raise quote_contract_error(QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE, "action quote request side must be BUY or SELL")
        object.__setattr__(self, "side", side)
        try:
            revision = ControlRevision(self.control_revision)
        except (TypeError, ValueError) as exc:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "action quote request has unknown control revision") from exc
        if revision != ControlRevision.B0_QUOTE_V2:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "P1-C eligibility requests require the explicit B0_QUOTE_V2 revision",
            )
        object.__setattr__(self, "control_revision", revision)
        for field_name in ("policy_sha256", "config_sha256", "adapter_sha256"):
            object.__setattr__(self, field_name, require_sha256(getattr(self, field_name), field_name=f"request.{field_name}"))
        if self.dependency_group_id is not None:
            object.__setattr__(
                self,
                "dependency_group_id",
                require_identity(self.dependency_group_id, field_name="request.dependency_group_id"),
            )


@dataclass(frozen=True)
class EligibilityEvaluation:
    eligibility: ActionQuoteEligibility
    diagnostics: tuple[str, ...]
    freshness: FreshnessResult | None


class ActionQuoteEvaluator:
    """Total, deterministic per-symbol precedence evaluator for P1-C."""

    def evaluate(
        self,
        *,
        request: ActionQuoteRequest,
        context: QuoteEvaluationContext,
        observation: NormalizedQuoteObservation | None,
    ) -> EligibilityEvaluation:
        if request.policy_sha256 != context.policy.policy_sha256:
            raise quote_contract_error(
                QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
                "action quote request policy hash conflicts with scheduler-published context",
                context={"request_policy_sha256": request.policy_sha256, "context_policy_sha256": context.policy.policy_sha256},
            )
        symbol_context = context.symbol_context(request.symbol)
        diagnostics: list[str] = []
        phase = context.clock.phase_by_market[exact_symbol(request.symbol)[1]]
        if not context.continuity_valid:
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.CLOCK_INVALID,
                reason=QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                stage=QuoteContractStage.CLOCK,
                diagnostics=("CLOCK_CONTINUITY_INVALID",),
            )
        if observation is not None:
            quote = observation.quote
            if (
                quote.clock_domain_id != context.clock.clock_domain_id
                or quote.clock_trade_date != context.clock.clock_trade_date
                or (
                    quote.source_trade_date is not None
                    and quote.source_trade_date != context.clock.clock_trade_date
                )
            ):
                return self._failure(
                    request=request,
                    context=context,
                    observation=observation,
                    state=EligibilityState.CLOCK_INVALID,
                    reason=QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                    stage=QuoteContractStage.CLOCK,
                    diagnostics=("QUOTE_CLOCK_OR_TRADE_DATE_CONFLICT",),
                )
            openint_phase = _registered_openint_phase(observation.quote)
            if openint_phase is not None and openint_phase != phase:
                return self._failure(
                    request=request,
                    context=context,
                    observation=observation,
                    state=EligibilityState.CLOCK_INVALID,
                    reason=QuoteContractReasonCode.MARKET_PHASE_MISMATCH,
                    stage=QuoteContractStage.CALENDAR,
                    diagnostics=(f"OPENINT_PHASE={openint_phase.value}", f"CALENDAR_PHASE={phase.value}"),
                )
        if phase != MarketPhase.CONTINUOUS:
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.WRONG_SESSION,
                reason=QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
                stage=QuoteContractStage.ELIGIBILITY,
                diagnostics=(f"MARKET_PHASE={phase.value}",),
            )
        if observation is None:
            return self._failure(
                request=request,
                context=context,
                observation=None,
                state=EligibilityState.WAITING_FIRST_QUOTE,
                reason=QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE,
                stage=QuoteContractStage.BOOTSTRAP,
                diagnostics=("NORMALIZED_QUOTE_UNAVAILABLE_FOR_CONTEXT",),
            )
        quote = observation.quote
        if quote.validation_state == QuoteValidationState.INVALID:
            reason = (
                QuoteContractReasonCode.DEPTH_SCHEMA_INVALID
                if QuoteContractReasonCode.DEPTH_SCHEMA_INVALID in quote.validation_reasons
                else QuoteContractReasonCode.PAYLOAD_INVALID
            )
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.INVALID,
                reason=reason,
                stage=QuoteContractStage.NORMALIZE if reason == QuoteContractReasonCode.DEPTH_SCHEMA_INVALID else QuoteContractStage.INGRESS,
                diagnostics=tuple(reason_item.value for reason_item in quote.validation_reasons),
            )
        missing_capability = _missing_capability_reason(
            quote=quote,
            symbol_context=symbol_context,
            policy=context.policy,
        )
        if missing_capability is not None:
            reason, stage, detail = missing_capability
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.CAPABILITY_MISSING,
                reason=reason,
                stage=stage,
                diagnostics=(detail,),
            )
        if _registered_openint_phase(quote) is None:
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.CAPABILITY_MISSING,
                reason=QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                stage=QuoteContractStage.TRADABILITY,
                diagnostics=("OPENINT_CROSS_EVIDENCE_MISSING_OR_UNREGISTERED",),
            )
        freshness = evaluate_freshness(quote=quote, clock=context.clock, policy=context.policy)
        if not freshness.ready:
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.STALE,
                reason=QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
                stage=QuoteContractStage.ELIGIBILITY,
                diagnostics=freshness.failure_reasons,
                freshness=freshness,
            )
        assert symbol_context is not None and symbol_context.tradability is not None
        tradability = symbol_context.tradability
        if tradability.state in {TradabilityState.DATA_INVALID, TradabilityState.STATUS_UNKNOWN}:
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.INVALID,
                reason=QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                stage=QuoteContractStage.TRADABILITY,
                diagnostics=(f"TRADABILITY_STATE={tradability.state.value}",),
                freshness=freshness,
            )
        if tradability.state in {TradabilityState.SUSPENDED, TradabilityState.INTRADAY_HALT}:
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.SUSPENDED,
                reason=QuoteContractReasonCode.MARKET_NOT_TRADABLE,
                stage=QuoteContractStage.TRADABILITY,
                diagnostics=(f"TRADABILITY_STATE={tradability.state.value}",),
                freshness=freshness,
            )
        opposite_prices, opposite_quantities = _opposite_book(quote=quote, side=request.side)
        if _side_limit_blocked(side=request.side, prices=opposite_prices, tradability=tradability):
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.LIMIT_BLOCKED,
                reason=QuoteContractReasonCode.MARKET_NOT_TRADABLE,
                stage=QuoteContractStage.TRADABILITY,
                diagnostics=("SIDE_LIMIT_BLOCKED",),
                freshness=freshness,
            )
        if opposite_prices is None or opposite_quantities is None or opposite_prices[0] is None or sum(opposite_quantities) <= 0:
            return self._failure(
                request=request,
                context=context,
                observation=observation,
                state=EligibilityState.NO_OPPOSITE_DEPTH,
                reason=QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
                stage=QuoteContractStage.ELIGIBILITY,
                diagnostics=("OPPOSITE_DEPTH_UNAVAILABLE",),
                freshness=freshness,
            )
        eligibility = ActionQuoteEligibility(
            runtime_id=request.runtime_id,
            parent_intent_id=request.parent_intent_id,
            algo_instance_id=request.algo_instance_id,
            symbol=request.symbol,
            side=request.side,
            market_data_id=observation.market_data_id,
            clock_event_id=context.clock.clock_event_id,
            tradability_id=tradability.tradability_id,
            control_revision=request.control_revision,
            policy_sha256=request.policy_sha256,
            config_sha256=request.config_sha256,
            adapter_sha256=request.adapter_sha256,
            state=EligibilityState.READY,
            reason_code=None,
            stage=None,
            evaluated_at_utc=context.clock.clock_at_utc,
        )
        return EligibilityEvaluation(eligibility=eligibility, diagnostics=tuple(diagnostics), freshness=freshness)

    def _failure(
        self,
        *,
        request: ActionQuoteRequest,
        context: QuoteEvaluationContext,
        observation: NormalizedQuoteObservation | None,
        state: EligibilityState,
        reason: QuoteContractReasonCode,
        stage: QuoteContractStage,
        diagnostics: tuple[str, ...],
        freshness: FreshnessResult | None = None,
    ) -> EligibilityEvaluation:
        eligibility = ActionQuoteEligibility(
            runtime_id=request.runtime_id,
            parent_intent_id=request.parent_intent_id,
            algo_instance_id=request.algo_instance_id,
            symbol=request.symbol,
            side=request.side,
            market_data_id=observation.market_data_id if observation is not None else None,
            clock_event_id=context.clock.clock_event_id,
            tradability_id=observation.tradability.tradability_id if observation and observation.tradability else None,
            control_revision=request.control_revision,
            policy_sha256=request.policy_sha256,
            config_sha256=request.config_sha256,
            adapter_sha256=request.adapter_sha256,
            state=state,
            reason_code=reason,
            stage=stage.value,
            evaluated_at_utc=context.clock.clock_at_utc,
        )
        return EligibilityEvaluation(eligibility=eligibility, diagnostics=diagnostics, freshness=freshness)


def build_quote_snapshot_batch(
    *,
    batch_id: str,
    runtime_id: str,
    context: QuoteEvaluationContext,
    requests: Mapping[str, ActionQuoteRequest],
    observations: Mapping[str, NormalizedQuoteObservation],
) -> QuoteSnapshotBatch:
    """Build an observation-only batch; grouping comes only from frozen request metadata."""

    authoritative_runtime_id = require_identity(runtime_id, field_name="batch.runtime_id")
    normalized_requests: dict[str, ActionQuoteRequest] = {}
    for raw_symbol, request in requests.items():
        symbol = exact_symbol(raw_symbol)[0]
        if symbol != request.symbol or symbol in normalized_requests:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "batch requests must use exact, unique symbol identities",
            )
        if request.runtime_id != authoritative_runtime_id:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "batch request runtime identity conflicts with the authoritative batch runtime",
                context={
                    "symbol": symbol,
                    "batch_runtime_id": authoritative_runtime_id,
                    "request_runtime_id": request.runtime_id,
                },
            )
        normalized_requests[symbol] = request
    evaluator = ActionQuoteEvaluator()
    evaluations = {
        symbol: evaluator.evaluate(
            request=request,
            context=context,
            observation=observations.get(symbol) if observations.get(symbol, None) and observations[symbol].context_id == context.context_id else None,
        )
        for symbol, request in normalized_requests.items()
    }
    groups: dict[str, tuple[str, ...]] = {}
    for symbol, request in normalized_requests.items():
        if request.dependency_group_id is not None:
            groups.setdefault(request.dependency_group_id, tuple())
            groups[request.dependency_group_id] = (*groups[request.dependency_group_id], symbol)
    group_watermark_ms: dict[str, int] = {}
    group_max_skew_ms: dict[str, int] = {}
    for group_id, symbols in groups.items():
        items = [observations.get(symbol) for symbol in symbols]
        ready_items = [item for item in items if item is not None and item.context_id == context.context_id]
        same_domain = bool(ready_items) and all(item.quote.clock_domain_id == context.clock.clock_domain_id for item in ready_items)
        monotonic_values = [item.quote.received_monotonic_ns for item in ready_items]
        skew_ms = _ns_to_ms(max(monotonic_values) - min(monotonic_values)) if monotonic_values else 0
        group_watermark_ms[group_id] = max(monotonic_values) // 1_000_000 if monotonic_values else 0
        group_max_skew_ms[group_id] = skew_ms
        all_ready = len(ready_items) == len(symbols) and all(
            evaluations[symbol].eligibility.state == EligibilityState.READY for symbol in symbols
        )
        if same_domain and all_ready and skew_ms <= context.policy.max_dependency_group_skew_ms:
            continue
        for symbol in symbols:
            current = evaluations[symbol]
            if current.eligibility.state != EligibilityState.READY:
                continue
            blocked = replace(
                current.eligibility,
                state=EligibilityState.STALE,
                reason_code=QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
                stage=QuoteContractStage.ELIGIBILITY.value,
            )
            evaluations[symbol] = EligibilityEvaluation(
                eligibility=blocked,
                diagnostics=(*current.diagnostics, "DEPENDENCY_GROUP_NOT_READY_OR_SKEWED"),
                freshness=current.freshness,
            )
    active_symbols = tuple(normalized_requests)
    states = {item.eligibility.state for item in evaluations.values()}
    if not active_symbols:
        aggregate = QuoteBatchAggregateState.NO_ACTIVE_SYMBOLS
    elif states == {EligibilityState.READY}:
        aggregate = QuoteBatchAggregateState.OBSERVED
    elif states.intersection({EligibilityState.INVALID, EligibilityState.CLOCK_INVALID}):
        aggregate = QuoteBatchAggregateState.INVALID
    else:
        aggregate = QuoteBatchAggregateState.PARTIAL
    return QuoteSnapshotBatch(
        batch_id=batch_id,
        runtime_id=authoritative_runtime_id,
        clock_event_id=context.clock.clock_event_id,
        policy_sha256=context.policy.policy_sha256,
        active_symbols=active_symbols,
        dependency_groups=groups,
        eligibility_by_symbol={symbol: item.eligibility for symbol, item in evaluations.items()},
        quote_by_symbol={
            symbol: observation.quote
            for symbol, observation in observations.items()
            if symbol in normalized_requests and observation.context_id == context.context_id
        },
        group_watermark_ms=group_watermark_ms,
        group_max_skew_ms=group_max_skew_ms,
        aggregate_state=aggregate,
    )


def _registered_openint_phase(quote: FiveLevelQuote) -> MarketPhase | None:
    if not isinstance(quote.openint_status, str):
        return None
    mapping = OPENINT_PHASE_BY_NORMALIZER_VERSION.get(quote.normalizer_map_version)
    return mapping.get(quote.openint_status) if mapping is not None else None


def _missing_capability_reason(
    *,
    quote: FiveLevelQuote,
    symbol_context: QuoteSymbolContext | None,
    policy: QuoteContractPolicy,
) -> tuple[QuoteContractReasonCode, QuoteContractStage, str] | None:
    if symbol_context is None:
        return (QuoteContractReasonCode.TRADABILITY_DATA_INVALID, QuoteContractStage.TRADABILITY, "SYMBOL_CONTEXT_MISSING")
    if not symbol_context.product_type_proven_equity:
        return (QuoteContractReasonCode.TRADABILITY_DATA_INVALID, QuoteContractStage.TRADABILITY, "PRODUCT_TYPE_NOT_PROVEN_EQUITY")
    if symbol_context.tradability is None:
        return (QuoteContractReasonCode.TRADABILITY_DATA_INVALID, QuoteContractStage.TRADABILITY, "TRADABILITY_AUTHORITY_MISSING")
    available = set(quote.quote_capabilities)
    available.add(QuoteCapability.CALENDAR)
    available.add(QuoteCapability.TRADABILITY)
    missing = set(policy.required_capabilities) - available
    if not missing and quote.validation_state != QuoteValidationState.CAPABILITY_MISSING:
        return None
    if QuoteCapability.EXCHANGE_TIMESTAMP in missing or quote.source_exchange_time_utc is None:
        return (QuoteContractReasonCode.TIMESTAMP_INVALID, QuoteContractStage.NORMALIZE, "EXCHANGE_TIMESTAMP_MISSING")
    if QuoteCapability.DEPTH_UNIT_SHARES in missing or quote.depth_quantity_unit == DepthQuantityUnit.UNKNOWN:
        return (QuoteContractReasonCode.UNIT_UNPROVEN, QuoteContractStage.UNIT, "DEPTH_UNIT_UNPROVEN")
    if QuoteCapability.FIVE_LEVEL_DEPTH in missing:
        return (QuoteContractReasonCode.DEPTH_CAPABILITY_MISSING, QuoteContractStage.NORMALIZE, "FIVE_LEVEL_DEPTH_MISSING")
    return (QuoteContractReasonCode.TRADABILITY_DATA_INVALID, QuoteContractStage.TRADABILITY, "REQUIRED_CAPABILITY_MISSING")


def _opposite_book(
    *,
    quote: FiveLevelQuote,
    side: str,
) -> tuple[
    tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None,
    tuple[int, int, int, int, int] | None,
]:
    return (quote.ask_prices, quote.ask_quantities) if side == "BUY" else (quote.bid_prices, quote.bid_quantities)


def _side_limit_blocked(
    *,
    side: str,
    prices: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None,
    tradability: TradabilitySnapshot,
) -> bool:
    if prices is None or prices[0] is None:
        return False
    if side == "BUY" and tradability.limit_up is not None:
        return prices[0] >= tradability.limit_up
    if side == "SELL" and tradability.limit_down is not None:
        return prices[0] <= tradability.limit_down
    return False


def _timedelta_ms(value: timedelta) -> int:
    microseconds = value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds
    return microseconds // 1_000 if microseconds >= 0 else -((-microseconds + 999) // 1_000)


def _ns_to_ms(value: int) -> int:
    return value // 1_000_000 if value >= 0 else -((-value + 999_999) // 1_000_000)


__all__ = [
    "A_SHARE_EQUITY_PHASE_SCHEDULE_VERSION",
    "ActionQuoteEvaluator",
    "ActionQuoteRequest",
    "BoundedNormalizedQuoteStore",
    "ClockContinuityResult",
    "ClockContinuityTracker",
    "EligibilityEvaluation",
    "FreshnessResult",
    "NormalizedQuoteObservation",
    "OPENINT_PHASE_BY_NORMALIZER_VERSION",
    "OrderingDecision",
    "OrderingDisposition",
    "QuoteEvaluationContext",
    "QuoteEvaluationContextStore",
    "QuoteOrderingTracker",
    "QuoteSymbolContext",
    "build_execution_clock_event",
    "build_quote_snapshot_batch",
    "deterministic_market_data_id",
    "evaluate_freshness",
    "phase_for_shanghai_time",
]
