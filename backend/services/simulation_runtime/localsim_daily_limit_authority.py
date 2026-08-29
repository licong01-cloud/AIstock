"""LocalSIM daily-limit authority: stk_limit first, TDX reference only for availability gaps."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime
import math
from typing import Any
from zoneinfo import ZoneInfo

from backend.execution_algos.board_lot import board_lot_rule
from backend.services.dataset_release.a_share_limit_rule import (
    AShareBoard,
    AShareLimitRuleError,
    classify_a_share_board,
)
from backend.services.paper_trading_v2.market_data import parse_tdx_reference_pre_close
from backend.services.simulation_runtime.models import (
    DAILY_LIMIT_AUTHORITY_BY_BROKER_V2,
    DailyLimitAuthorityV2,
    DailyLimitResolverV2,
    DailyTradingAuthorityStateV2,
    DailyTradingContextSourcesV2,
    DailyTradingContextV2,
    DailyTradingSymbolFactV2,
    SimulationBrokerBackend,
    canonical_json_sha256,
)
from backend.services.trading_core.a_share_live_limit_rule import (
    LIVE_REFERENCE_LIMIT_RULE_VERSION,
    LiveReferenceLimitRuleError,
    derive_live_reference_limit_prices,
)
from backend.services.trading_core.errors import DataUnavailableError


LOCAL_SIM_SYMBOL_UNAVAILABLE = "DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE"
StkLimitAttemptLoader = Callable[..., Mapping[str, Any]]
SupportingFactLoader = Callable[..., Mapping[str, Any]]
TdxReferenceReader = Callable[[list[str]], Mapping[str, Mapping[str, Any]]]


class LocalSimDailyLimitAuthorityProvider:
    """Freeze one broker-bound V2 carrier without retrying or requerying in the minute loop."""

    def __init__(
        self,
        *,
        stk_limit_attempt_loader: StkLimitAttemptLoader,
        supporting_fact_loader: SupportingFactLoader,
        tdx_reference_reader: TdxReferenceReader,
    ) -> None:
        if not all(callable(item) for item in (stk_limit_attempt_loader, supporting_fact_loader, tdx_reference_reader)):
            raise TypeError("LocalSIM daily authority requires callable batch loaders")
        self._stk_limit_attempt_loader = stk_limit_attempt_loader
        self._supporting_fact_loader = supporting_fact_loader
        self._tdx_reference_reader = tdx_reference_reader

    def load(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        as_of_time: datetime,
        calendar_service_snapshot: Mapping[str, Any],
        binding_identity: str,
        package_identity: str,
        release_identity: str,
    ) -> DailyTradingContextV2:
        captured_at = (
            as_of_time.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
            if as_of_time.tzinfo is None
            else as_of_time.astimezone(ZoneInfo("Asia/Shanghai"))
        )
        normalized = _exact_symbols(symbols)
        if captured_at.date() != trade_date:
            raise _error(
                "LocalSIM authority time conflicts with trade date", "DAILY_TRADING_CONTEXT_TRADE_DATE_MISMATCH"
            )
        if calendar_service_snapshot.get("is_trading_day") is not True:
            raise _error(
                "LocalSIM authority requires a trading-day snapshot", "DAILY_TRADING_CONTEXT_CALENDAR_SNAPSHOT_INVALID"
            )

        attempt = self._stk_limit_attempt_loader(symbols=list(normalized), trade_date=trade_date)
        rows, availability, refresh_identity = _stk_limit_attempt(attempt, normalized, trade_date)
        supporting = self._supporting_fact_loader(symbols=list(normalized), trade_date=trade_date)
        st_facts, suspend_facts, stock_st_source, suspend_source = _supporting_facts(
            supporting, symbols=normalized, trade_date=trade_date
        )
        direct, derive_symbols, failed = _classify_stk_rows(rows, normalized, trade_date, availability)
        reference_symbols = tuple(
            sorted(set(derive_symbols).union(symbol for symbol, row in direct.items() if row["pre_close"] is None))
        )
        reference_failures: dict[str, str] = {}
        tdx_rows: Mapping[str, Mapping[str, Any]] = {}
        if reference_symbols:
            try:
                raw_tdx = self._tdx_reference_reader(list(reference_symbols))
            except DataUnavailableError as exc:
                reason = str(exc.context.get("reason_code") or "DAILY_LIMIT_TDX_REFERENCE_BATCH_UNAVAILABLE")
                reference_failures.update({symbol: reason for symbol in reference_symbols})
                raw_tdx = {}
            except Exception:
                reference_failures.update(
                    {symbol: "DAILY_LIMIT_TDX_REFERENCE_BATCH_UNAVAILABLE" for symbol in reference_symbols}
                )
                raw_tdx = {}
            tdx_rows = _tdx_batch(raw_tdx, reference_symbols)

        facts: dict[str, DailyTradingSymbolFactV2] = {}
        stk_hashes: dict[str, str] = {}
        tdx_hashes: dict[str, str] = {}
        for symbol in normalized:
            base = _base_fact(symbol, trade_date, st_facts[symbol], suspend_facts[symbol])
            if symbol in failed:
                facts[symbol] = _failed_fact(base, failed[symbol])
                continue
            direct_row = direct.get(symbol)
            needs_reference = symbol in derive_symbols or (direct_row is not None and direct_row["pre_close"] is None)
            reference = None
            if needs_reference:
                raw_quote = tdx_rows.get(symbol)
                if raw_quote is not None:
                    try:
                        reference = parse_tdx_reference_pre_close(
                            symbol=symbol,
                            quote=raw_quote,
                            trade_date=trade_date,
                            as_of_time=captured_at,
                        )
                    except DataUnavailableError as exc:
                        reference_failures[symbol] = str(
                            exc.context.get("reason_code") or "DAILY_LIMIT_TDX_REFERENCE_INVALID"
                        )
                        reference = None
                if reference is None:
                    reason = reference_failures.get(symbol, "DAILY_LIMIT_TDX_REFERENCE_MISSING")
                    facts[symbol] = _failed_fact(base, reason)
                    continue
                tdx_hashes[symbol] = str(reference["evidence_hash"])
            if direct_row is not None:
                pre_close = direct_row["pre_close"] if direct_row["pre_close"] is not None else reference["pre_close"]
                evidence_payload = {
                    **direct_row,
                    "reference_evidence_hash": reference["evidence_hash"] if reference else None,
                }
                evidence_hash = canonical_json_sha256(evidence_payload)
                stk_hashes[symbol] = evidence_hash
                try:
                    facts[symbol] = DailyTradingSymbolFactV2(
                        **base,
                        authority_state=DailyTradingAuthorityStateV2.READY,
                        limit_authority=DailyLimitAuthorityV2.TUSHARE_STK_LIMIT,
                        has_daily_limit=True,
                        pre_close=pre_close,
                        up_limit=direct_row["up_limit"],
                        down_limit=direct_row["down_limit"],
                        price_tick=0.01,
                        source_evidence_hash=evidence_hash,
                    )
                except ValueError:
                    facts[symbol] = _failed_fact(base, LOCAL_SIM_SYMBOL_UNAVAILABLE)
                continue
            assert reference is not None
            try:
                derived = derive_live_reference_limit_prices(
                    ts_code=symbol,
                    trade_date=trade_date,
                    reference_pre_close=reference["pre_close"],
                    reference_evidence_hash=reference["evidence_hash"],
                    price_tick="0.01",
                    is_st=st_facts[symbol]["is_st"],
                )
                facts[symbol] = DailyTradingSymbolFactV2(
                    **base,
                    authority_state=DailyTradingAuthorityStateV2.READY,
                    limit_authority=DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1,
                    has_daily_limit=True,
                    pre_close=float(derived.reference_pre_close),
                    up_limit=float(derived.up_limit),
                    down_limit=float(derived.down_limit),
                    price_tick=float(derived.price_tick),
                    source_evidence_hash=derived.reference_evidence_hash,
                    rule_version=derived.rule_version,
                    derivation_hash=derived.derivation_hash,
                )
            except (LiveReferenceLimitRuleError, TypeError, ValueError):
                facts[symbol] = _failed_fact(base, LOCAL_SIM_SYMBOL_UNAVAILABLE)

        if all(fact.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED for fact in facts.values()):
            raise _error("LocalSIM authority failed for every plan symbol", "DAILY_TRADING_CONTEXT_AUTHORITY_INVALID")
        calendar_payload = dict(calendar_service_snapshot)
        calendar_snapshot_id = f"tcal_{canonical_json_sha256(calendar_payload)[:16]}"
        symbol_set_hash = canonical_json_sha256(list(normalized))
        plan_identity = canonical_json_sha256(
            {
                "binding_identity": binding_identity,
                "package_identity": package_identity,
                "release_identity": release_identity,
                "trade_date": trade_date.isoformat(),
                "symbol_set_hash": symbol_set_hash,
            }
        )
        stk_source = {
            "schema_version": "stk_limit_authority_source_v1",
            "source": "market.stk_limit",
            "trade_date": trade_date.isoformat(),
            "availability": availability,
            "refresh_identity": refresh_identity,
            "attempt_hash": canonical_json_sha256(_evidence_value(dict(attempt))),
            "symbol_evidence_hashes": stk_hashes,
            "batch_hash": canonical_json_sha256(stk_hashes),
        }
        tdx_source = None
        if reference_symbols or tdx_hashes:
            tdx_source = {
                "schema_version": "tdx_reference_batch_v1",
                "source": "TDX_REALTIME.batch_quote.K.Last",
                "trade_date": trade_date.isoformat(),
                "requested_symbols": list(reference_symbols),
                "symbol_evidence_hashes": tdx_hashes,
                "batch_hash": canonical_json_sha256(tdx_hashes),
            }
        actual = tuple(sorted({fact.limit_authority for fact in facts.values()}, key=lambda value: value.value))
        allowed = tuple(
            sorted(DAILY_LIMIT_AUTHORITY_BY_BROKER_V2[SimulationBrokerBackend.LOCAL_SIM], key=lambda value: value.value)
        )
        versions = tuple(sorted({fact.rule_version for fact in facts.values() if fact.rule_version}))
        sources = DailyTradingContextSourcesV2.build(
            resolver=DailyLimitResolverV2.LOCALSIM_STK_LIMIT_TDX_V1,
            allowed_source_kinds=allowed,
            actual_source_kinds=actual,
            trade_date=trade_date,
            read_at=captured_at,
            rule_versions=versions,
            stock_st=stock_st_source,
            suspend_d=suspend_source,
            stk_limit=stk_source,
            tdx_reference=tdx_source,
        )
        return DailyTradingContextV2.build(
            trade_date=trade_date,
            plan_identity=plan_identity,
            binding_identity=binding_identity,
            package_identity=package_identity,
            calendar_service_snapshot_id=calendar_snapshot_id,
            captured_at=captured_at,
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            sources=sources,
            symbols=facts,
        )

    @staticmethod
    def to_pre_trade_statuses(context: DailyTradingContextV2) -> dict[str, dict[str, Any]]:
        if context.broker_backend is not SimulationBrokerBackend.LOCAL_SIM:
            raise ValueError("LocalSIM daily authority refuses a cross-broker context")
        statuses: dict[str, dict[str, Any]] = {}
        carrier = context.carrier_payload()
        for symbol, fact in context.symbols.items():
            failed = fact.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED
            statuses[symbol] = {
                "schema_version": "pre_trade_tradability_status_v1",
                "symbol": symbol,
                "trade_date": context.trade_date.isoformat(),
                "is_tradable": not failed and not fact.is_suspended,
                "reason_code": fact.authority_reason_code
                if failed
                else "SUSPENDED_BY_SUSPEND_D"
                if fact.is_suspended
                else "PRE_TRADE_TRADABLE",
                "source": "daily_trading_context_v2",
                "suspend_status": {
                    "is_suspended": fact.is_suspended,
                    "suspend_type": fact.suspend_type,
                    "suspend_timing": fact.suspend_timing,
                    "source": fact.suspend_source,
                },
                "quote_evidence": None,
                "daily_trading_context": {
                    "schema_version": "daily_trading_context_reference_v2",
                    "context_id": context.context_id,
                    "context_hash": context.context_hash,
                    "trade_date": context.trade_date.isoformat(),
                    "symbol_set_hash": context.symbol_set_hash,
                    "broker_backend": context.broker_backend.value,
                    "authority_state": fact.authority_state.value,
                    "limit_authority": fact.limit_authority.value,
                    "source_evidence_hash": fact.source_evidence_hash,
                    "symbol_fact": fact.canonical_payload(),
                    "context": carrier,
                },
            }
        return statuses


def _exact_symbols(symbols: list[str]) -> tuple[str, ...]:
    raw = [str(value or "").strip() for value in symbols if str(value or "").strip()]
    aliases = [value.upper() for value in raw]
    if not raw or len(raw) != len(set(aliases)) or raw != aliases:
        raise _error("LocalSIM authority requires exact unique symbols", "DAILY_TRADING_CONTEXT_SYMBOL_ALIAS_COLLISION")
    try:
        for symbol in aliases:
            classify_a_share_board(symbol)
    except AShareLimitRuleError as exc:
        raise _error(
            "LocalSIM authority symbol board is unsupported",
            "DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE",
        ) from exc
    return tuple(sorted(aliases))


def _stk_limit_attempt(
    payload: Mapping[str, Any], symbols: tuple[str, ...], trade_date: date
) -> tuple[list[Any], str, Any]:
    if not isinstance(payload, Mapping) or payload.get("schema_version") != "stk_limit_authority_attempt_v1":
        raise _error("stk_limit attempt schema is invalid", "DAILY_TRADING_CONTEXT_STK_LIMIT_ATTEMPT_INVALID")
    if payload.get("trade_date") != trade_date.isoformat() or tuple(payload.get("symbol_set") or ()) != symbols:
        raise _error("stk_limit attempt identity is invalid", "DAILY_TRADING_CONTEXT_STK_LIMIT_ATTEMPT_INVALID")
    availability = payload.get("availability")
    if availability not in {"AVAILABLE", "ZERO_ROWS", "UNAVAILABLE"} or not isinstance(payload.get("rows"), list):
        raise _error("stk_limit availability is invalid", "DAILY_TRADING_CONTEXT_STK_LIMIT_ATTEMPT_INVALID")
    if availability != "AVAILABLE" and payload["rows"]:
        raise _error("unavailable stk_limit attempt carries rows", "DAILY_TRADING_CONTEXT_STK_LIMIT_ATTEMPT_INVALID")
    return list(payload["rows"]), str(availability), payload.get("refresh_identity")


def _classify_stk_rows(
    rows: list[Any], symbols: tuple[str, ...], trade_date: date, availability: str
) -> tuple[dict[str, dict[str, Any]], tuple[str, ...], dict[str, str]]:
    direct: dict[str, dict[str, Any]] = {}
    failed: dict[str, str] = {}
    if availability != "AVAILABLE":
        return direct, symbols, failed
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise _error("stk_limit row is not a mapping", "DAILY_TRADING_CONTEXT_STK_LIMIT_BATCH_INVALID")
        symbol = str(raw.get("symbol") or "").strip()
        if symbol not in symbols or symbol in direct or symbol in failed:
            raise _error("stk_limit batch identity is invalid", "DAILY_TRADING_CONTEXT_STK_LIMIT_BATCH_INVALID")
        if raw.get("trade_date") != trade_date.isoformat():
            raise _error("stk_limit row is cross-date", "DAILY_TRADING_CONTEXT_STK_LIMIT_CROSS_DATE")
        try:
            pre_close = None if raw.get("pre_close") is None else float(raw["pre_close"])
            up_limit, down_limit = float(raw["up_limit"]), float(raw["down_limit"])
            valid = (
                all(math.isfinite(value) and value > 0 for value in (up_limit, down_limit)) and down_limit < up_limit
            )
            if pre_close is not None:
                valid = valid and math.isfinite(pre_close) and down_limit < pre_close < up_limit
            if not valid:
                raise ValueError(symbol)
        except (KeyError, TypeError, ValueError):
            failed[symbol] = "DAILY_TRADING_CONTEXT_STK_LIMIT_INVALID"
            continue
        direct[symbol] = {
            "schema_version": "stk_limit_symbol_evidence_v1",
            "source": "market.stk_limit",
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "pre_close": pre_close,
            "up_limit": up_limit,
            "down_limit": down_limit,
        }
    missing = tuple(symbol for symbol in symbols if symbol not in direct and symbol not in failed)
    return direct, missing, failed


def _tdx_batch(payload: Any, requested: tuple[str, ...]) -> Mapping[str, Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        return {}
    normalized: dict[str, Mapping[str, Any]] = {}
    for key, value in payload.items():
        symbol = str(key or "").strip()
        if (
            symbol != symbol.upper()
            or symbol not in requested
            or symbol in normalized
            or not isinstance(value, Mapping)
        ):
            raise _error("TDX reference batch identity is invalid", "DAILY_LIMIT_TDX_REFERENCE_BATCH_INVALID")
        normalized[symbol] = value
    return normalized


def _supporting_facts(
    payload: Mapping[str, Any], *, symbols: tuple[str, ...], trade_date: date
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Any], dict[str, Any]]:
    if not isinstance(payload, Mapping) or payload.get("schema_version") != "daily_trading_supporting_facts_v1":
        raise _error("LocalSIM supporting facts are invalid", "DAILY_TRADING_CONTEXT_SUPPORTING_FACT_INVALID")
    if payload.get("trade_date") != trade_date.isoformat() or tuple(payload.get("symbol_set") or ()) != symbols:
        raise _error("LocalSIM supporting fact identity is invalid", "DAILY_TRADING_CONTEXT_SUPPORTING_FACT_INVALID")
    st, suspend = payload.get("stock_st_facts"), payload.get("suspend_facts")
    if (
        not isinstance(st, Mapping)
        or not isinstance(suspend, Mapping)
        or set(st) != set(symbols)
        or set(suspend) != set(symbols)
    ):
        raise _error("LocalSIM supporting facts lack exact coverage", "DAILY_TRADING_CONTEXT_SUPPORTING_FACT_INVALID")
    for symbol in symbols:
        st_row = st[symbol]
        suspend_row = suspend[symbol]
        if (
            not isinstance(st_row, Mapping)
            or type(st_row.get("is_st")) is not bool
            or type(st_row.get("source")) is not str
            or not str(st_row.get("source")).strip()
            or type(st_row.get("evidence_hash")) is not str
            or len(str(st_row.get("evidence_hash"))) != 64
            or not isinstance(suspend_row, Mapping)
            or type(suspend_row.get("is_suspended")) is not bool
        ):
            raise _error(
                "LocalSIM supporting symbol fact is invalid",
                "DAILY_TRADING_CONTEXT_SUPPORTING_FACT_INVALID",
            )
    return dict(st), dict(suspend), dict(payload["stock_st"]), dict(payload["suspend_d"])


def _base_fact(symbol: str, trade_date: date, st: Mapping[str, Any], suspend: Mapping[str, Any]) -> dict[str, Any]:
    min_quantity, increment = board_lot_rule(symbol)
    board_kind = classify_a_share_board(symbol)
    board = "STAR" if board_kind is AShareBoard.STAR else "CHINEXT" if board_kind is AShareBoard.CHINEXT else "MAIN"
    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "is_st": st["is_st"],
        "st_source": str(st["source"]),
        "st_evidence_hash": str(st["evidence_hash"]),
        "is_suspended": suspend["is_suspended"],
        "suspend_type": suspend.get("suspend_type"),
        "suspend_timing": suspend.get("suspend_timing"),
        "suspend_source": "market.suspend_d",
        "board": board,
        "lot_rule": {"min_quantity": min_quantity, "increment": increment},
    }


def _failed_fact(base: dict[str, Any], reason: str) -> DailyTradingSymbolFactV2:
    evidence = canonical_json_sha256(
        {
            "schema_version": "localsim_daily_authority_failure_v1",
            "symbol": base["symbol"],
            "trade_date": base["trade_date"].isoformat(),
            "reason_code": reason,
        }
    )
    return DailyTradingSymbolFactV2(
        **base,
        authority_state=DailyTradingAuthorityStateV2.SYMBOL_FAILED,
        limit_authority=DailyLimitAuthorityV2.UNAVAILABLE,
        has_daily_limit=False,
        source_evidence_hash=evidence,
        authority_reason_code=reason,
    )


def _evidence_value(value: Any) -> Any:
    if value is None or type(value) in {str, int, bool}:
        return value
    if type(value) is float:
        return value if math.isfinite(value) else {"invalid_float": repr(value)}
    if isinstance(value, Mapping):
        return {str(key): _evidence_value(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_evidence_value(item) for item in value]
    return {"invalid_type": type(value).__name__, "repr": repr(value)}


def _error(message: str, reason_code: str) -> DataUnavailableError:
    return DataUnavailableError(message, context={"reason_code": reason_code})


__all__ = ["LIVE_REFERENCE_LIMIT_RULE_VERSION", "LocalSimDailyLimitAuthorityProvider"]
