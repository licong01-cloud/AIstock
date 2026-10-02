"""MiniQMT-only daily limit authority for simulation plan confirmation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime
import math
from typing import Any
from zoneinfo import ZoneInfo

from backend.execution_algos.board_lot import board_lot_rule
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.simulation_data.daily_context import (
    DAILY_LIMIT_AUTHORITY_BY_BROKER_V2,
    DailyLimitAuthorityV2,
    DailyLimitResolverV2,
    DailyTradingAuthorityStateV2,
    DailyTradingContextSourcesV2,
    DailyTradingContextV2,
    DailyTradingSymbolFactV2,
    SimulationBrokerBackend,
)


MINIQMT_NO_DAILY_LIMIT_RULE_VERSION = "miniqmt_no_daily_limit_v1_20260826"
MINIQMT_SYMBOL_UNAVAILABLE = "DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE"

InstrumentBatchReader = Callable[[list[str]], Mapping[str, Mapping[str, Any]]]
SupportingFactLoader = Callable[..., Mapping[str, Any]]


class MiniQMTDailyLimitAuthorityProvider:
    """Freeze exact MiniQMT instrument detail once without Tushare or TDX reads."""

    def __init__(
        self,
        *,
        instrument_batch_reader: InstrumentBatchReader,
        supporting_fact_loader: SupportingFactLoader,
    ) -> None:
        if not callable(instrument_batch_reader) or not callable(supporting_fact_loader):
            raise TypeError("MiniQMT daily authority requires callable instrument and supporting fact loaders")
        self._instrument_batch_reader = instrument_batch_reader
        self._supporting_fact_loader = supporting_fact_loader

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
        runtime_identity: str,
        quote_continuity_identity: str,
    ) -> DailyTradingContextV2:
        captured_at = (
            as_of_time.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
            if as_of_time.tzinfo is None
            else as_of_time.astimezone(ZoneInfo("Asia/Shanghai"))
        )
        normalized = _exact_symbols(symbols)
        if captured_at.date() != trade_date:
            raise _batch_error(
                "MiniQMT instrument authority time conflicts with the plan trade date",
                reason_code="DAILY_TRADING_CONTEXT_TRADE_DATE_MISMATCH",
                trade_date=trade_date,
            )
        if calendar_service_snapshot.get("is_trading_day") is not True:
            raise _batch_error(
                "MiniQMT instrument authority requires an authoritative trading-day snapshot",
                reason_code="DAILY_TRADING_CONTEXT_CALENDAR_SNAPSHOT_INVALID",
                trade_date=trade_date,
            )
        try:
            raw_batch = self._instrument_batch_reader(list(normalized))
        except DataUnavailableError:
            raise
        except Exception as exc:
            raise _batch_error(
                "MiniQMT instrument-detail batch read failed",
                reason_code="DAILY_TRADING_CONTEXT_AUTHORITY_UNAVAILABLE",
                trade_date=trade_date,
                symbols=normalized,
                error_type=type(exc).__name__,
            ) from exc
        batch = _exact_batch(raw_batch, normalized, trade_date=trade_date)
        supporting = self._supporting_fact_loader(symbols=list(normalized), trade_date=trade_date)
        st_facts, suspend_facts, stock_st_source, suspend_source = _supporting_facts(
            supporting,
            symbols=normalized,
            trade_date=trade_date,
        )
        runtime_identity = _required_identity(runtime_identity, field="runtime_identity")
        quote_continuity_identity = _required_identity(
            quote_continuity_identity,
            field="quote_continuity_identity",
        )

        facts: dict[str, DailyTradingSymbolFactV2] = {}
        instrument_hashes: dict[str, str] = {}
        failures: list[dict[str, str]] = []
        for symbol in normalized:
            raw = batch[symbol]
            fact, evidence_hash = _symbol_fact(
                symbol=symbol,
                trade_date=trade_date,
                raw=raw,
                st=st_facts[symbol],
                suspend=suspend_facts[symbol],
                runtime_identity=runtime_identity,
                quote_continuity_identity=quote_continuity_identity,
            )
            facts[symbol] = fact
            instrument_hashes[symbol] = evidence_hash
            if fact.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED:
                failures.append({"symbol": symbol, "reason_code": str(fact.authority_reason_code)})
        if len(failures) == len(normalized):
            raise _batch_error(
                "MiniQMT instrument authority failed for every plan symbol",
                reason_code="DAILY_TRADING_CONTEXT_AUTHORITY_INVALID",
                trade_date=trade_date,
                symbols=normalized,
                failures=failures[:20],
            )

        calendar_payload = _strict_json_mapping(calendar_service_snapshot, field="calendar_service_snapshot")
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
        instrument_source = {
            "schema_version": "miniqmt_instrument_detail_batch_v1",
            "source": "xtdata.get_instrument_detail",
            "iscomplete": True,
            "trade_date": trade_date.isoformat(),
            "read_at": captured_at.isoformat(),
            "runtime_identity": runtime_identity,
            "quote_continuity_identity": quote_continuity_identity,
            "symbol_set": list(normalized),
            "batch_hash": canonical_json_sha256(instrument_hashes),
            "symbol_evidence_hashes": instrument_hashes,
        }
        actual_sources = tuple(sorted({fact.limit_authority for fact in facts.values()}, key=lambda item: item.value))
        allowed_sources = tuple(
            sorted(
                DAILY_LIMIT_AUTHORITY_BY_BROKER_V2[SimulationBrokerBackend.MINIQMT_SIM],
                key=lambda item: item.value,
            )
        )
        rule_versions = tuple(sorted({fact.rule_version for fact in facts.values() if fact.rule_version}))
        sources = DailyTradingContextSourcesV2.build(
            resolver=DailyLimitResolverV2.MINIQMT_INSTRUMENT_DETAIL_V1,
            allowed_source_kinds=allowed_sources,
            actual_source_kinds=actual_sources,
            trade_date=trade_date,
            read_at=captured_at,
            rule_versions=rule_versions,
            stock_st=stock_st_source,
            suspend_d=suspend_source,
            miniqmt_instrument=instrument_source,
        )
        return DailyTradingContextV2.build(
            trade_date=trade_date,
            plan_identity=plan_identity,
            binding_identity=binding_identity,
            package_identity=package_identity,
            calendar_service_snapshot_id=calendar_snapshot_id,
            captured_at=captured_at,
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            sources=sources,
            symbols=facts,
        )

    @staticmethod
    def to_pre_trade_statuses(context: DailyTradingContextV2) -> dict[str, dict[str, Any]]:
        if context.broker_backend is not SimulationBrokerBackend.MINIQMT_SIM:
            raise ValueError("MiniQMT daily authority refuses a cross-broker context")
        statuses: dict[str, dict[str, Any]] = {}
        carrier = context.carrier_payload()
        for symbol, fact in context.symbols.items():
            unavailable = fact.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED
            suspended = fact.is_suspended
            statuses[symbol] = {
                "schema_version": "pre_trade_tradability_status_v1",
                "symbol": symbol,
                "trade_date": context.trade_date.isoformat(),
                "is_tradable": not unavailable and not suspended,
                "reason_code": (
                    str(fact.authority_reason_code)
                    if unavailable
                    else "SUSPENDED_BY_SUSPEND_D"
                    if suspended
                    else "PRE_TRADE_TRADABLE"
                ),
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


def _symbol_fact(
    *,
    symbol: str,
    trade_date: date,
    raw: Mapping[str, Any],
    st: Mapping[str, Any],
    suspend: Mapping[str, Any],
    runtime_identity: str,
    quote_continuity_identity: str,
) -> tuple[DailyTradingSymbolFactV2, str]:
    min_quantity, increment = board_lot_rule(symbol)
    board = _board(symbol)
    base = {
        "symbol": symbol,
        "trade_date": trade_date,
        "is_st": bool(st["is_st"]),
        "st_source": str(st["source"]),
        "st_evidence_hash": str(st["evidence_hash"]),
        "is_suspended": bool(suspend["is_suspended"]),
        "suspend_type": suspend.get("suspend_type"),
        "suspend_timing": suspend.get("suspend_timing"),
        "suspend_source": "market.suspend_d",
        "board": board,
        "lot_rule": {"min_quantity": min_quantity, "increment": increment},
    }
    evidence_payload = _instrument_evidence(
        symbol=symbol,
        trade_date=trade_date,
        raw=raw,
        runtime_identity=runtime_identity,
        quote_continuity_identity=quote_continuity_identity,
    )
    evidence_hash = canonical_json_sha256(evidence_payload)
    if board == "UNKNOWN":
        return _failed_fact(base=base, evidence_hash=evidence_hash, reason=MINIQMT_SYMBOL_UNAVAILABLE), evidence_hash
    reason = _instrument_identity_reason(symbol=symbol, trade_date=trade_date, raw=raw)
    if reason is not None:
        return _failed_fact(base=base, evidence_hash=evidence_hash, reason=reason), evidence_hash
    try:
        pre_close = _positive_price(raw.get("PreClose"), field="PreClose")
        price_tick = _positive_price(raw.get("PriceTick"), field="PriceTick")
        up_limit = _optional_nonnegative_price(raw.get("UpStopPrice"), field="UpStopPrice")
        down_limit = _optional_nonnegative_price(raw.get("DownStopPrice"), field="DownStopPrice")
    except ValueError:
        return _failed_fact(base=base, evidence_hash=evidence_hash, reason=MINIQMT_SYMBOL_UNAVAILABLE), evidence_hash
    if up_limit is not None and up_limit > 0 and down_limit is not None and down_limit > 0:
        try:
            fact = DailyTradingSymbolFactV2(
                **base,
                authority_state=DailyTradingAuthorityStateV2.READY,
                limit_authority=DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1,
                has_daily_limit=True,
                pre_close=pre_close,
                up_limit=up_limit,
                down_limit=down_limit,
                price_tick=price_tick,
                source_evidence_hash=evidence_hash,
            )
        except ValueError:
            fact = _failed_fact(base=base, evidence_hash=evidence_hash, reason=MINIQMT_SYMBOL_UNAVAILABLE)
        return fact, evidence_hash
    if (up_limit in (None, 0.0)) and (down_limit in (None, 0.0)):
        no_limit_reason = _no_daily_limit_reason(raw=raw, trade_date=trade_date)
        if no_limit_reason is not None:
            try:
                fact = DailyTradingSymbolFactV2(
                    **base,
                    authority_state=DailyTradingAuthorityStateV2.NO_DAILY_LIMIT,
                    limit_authority=DailyLimitAuthorityV2.NO_DAILY_LIMIT,
                    has_daily_limit=False,
                    pre_close=pre_close,
                    price_tick=price_tick,
                    source_evidence_hash=evidence_hash,
                    rule_version=MINIQMT_NO_DAILY_LIMIT_RULE_VERSION,
                    authority_reason_code=no_limit_reason,
                )
            except ValueError:
                fact = _failed_fact(base=base, evidence_hash=evidence_hash, reason=MINIQMT_SYMBOL_UNAVAILABLE)
            return fact, evidence_hash
    return _failed_fact(base=base, evidence_hash=evidence_hash, reason=MINIQMT_SYMBOL_UNAVAILABLE), evidence_hash


def _failed_fact(*, base: dict[str, Any], evidence_hash: str, reason: str) -> DailyTradingSymbolFactV2:
    return DailyTradingSymbolFactV2(
        **base,
        authority_state=DailyTradingAuthorityStateV2.SYMBOL_FAILED,
        limit_authority=DailyLimitAuthorityV2.UNAVAILABLE,
        has_daily_limit=False,
        source_evidence_hash=evidence_hash,
        authority_reason_code=reason,
    )


def _instrument_identity_reason(*, symbol: str, trade_date: date, raw: Mapping[str, Any]) -> str | None:
    code, suffix = symbol.split(".", 1)
    instrument_id = str(raw.get("InstrumentID") or "").strip()
    exchange = str(raw.get("ExchangeID") or "").strip().upper()
    exchange_suffix = {
        "SH": "SH",
        "SSE": "SH",
        "SHSE": "SH",
        "SZ": "SZ",
        "SZSE": "SZ",
        "BJ": "BJ",
        "BSE": "BJ",
    }.get(exchange)
    if instrument_id != code or exchange_suffix != suffix:
        return MINIQMT_SYMBOL_UNAVAILABLE
    if type(raw.get("IsTrading")) is not bool:
        return MINIQMT_SYMBOL_UNAVAILABLE
    status = raw.get("InstrumentStatus")
    if type(status) is not int or status < 0:
        return MINIQMT_SYMBOL_UNAVAILABLE
    trading_day = _parse_date(raw.get("TradingDay"))
    if trading_day != trade_date:
        return MINIQMT_SYMBOL_UNAVAILABLE
    return None


def _no_daily_limit_reason(*, raw: Mapping[str, Any], trade_date: date) -> str | None:
    explicit = str(raw.get("NoDailyLimitReason") or "").strip().upper()
    day_count = raw.get("DayCountFromIPO")
    open_date = _parse_date(raw.get("OpenDate"))
    if (
        explicit in {"", "IPO_FIRST_FIVE_TRADING_DAYS"}
        and type(day_count) is int
        and 1 <= day_count <= 5
        and open_date is not None
        and open_date <= trade_date
    ):
        return "IPO_FIRST_FIVE_TRADING_DAYS"
    if explicit in {"RELISTING_FIRST_DAY", "DELISTING_ARRANGEMENT_FIRST_DAY"}:
        effective_date = _parse_date(raw.get("NoDailyLimitDate"))
        if effective_date == trade_date:
            return explicit
    return None


def _instrument_evidence(
    *,
    symbol: str,
    trade_date: date,
    raw: Mapping[str, Any],
    runtime_identity: str,
    quote_continuity_identity: str,
) -> dict[str, Any]:
    fields = (
        "InstrumentID",
        "ExchangeID",
        "PreClose",
        "UpStopPrice",
        "DownStopPrice",
        "PriceTick",
        "InstrumentStatus",
        "IsTrading",
        "TradingDay",
        "OpenDate",
        "DayCountFromIPO",
        "NoDailyLimitReason",
        "NoDailyLimitDate",
    )
    values: dict[str, Any] = {}
    for field in fields:
        value = raw.get(field)
        if value is None or type(value) in {str, int, float, bool}:
            values[field] = value
        elif isinstance(value, (date, datetime)):
            values[field] = value.isoformat()
        else:
            values[field] = {"invalid_type": type(value).__name__}
    return {
        "schema_version": "miniqmt_instrument_detail_symbol_evidence_v1",
        "source": "xtdata.get_instrument_detail",
        "iscomplete": True,
        "symbol": symbol,
        "trade_date": trade_date.isoformat(),
        "runtime_identity": runtime_identity,
        "quote_continuity_identity": quote_continuity_identity,
        "fields": values,
    }


def _supporting_facts(
    payload: Mapping[str, Any],
    *,
    symbols: tuple[str, ...],
    trade_date: date,
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Any], dict[str, Any]]:
    if not isinstance(payload, Mapping) or payload.get("schema_version") != "daily_trading_supporting_facts_v1":
        raise _batch_error(
            "MiniQMT supporting daily facts are invalid",
            reason_code="DAILY_TRADING_CONTEXT_SUPPORTING_FACT_INVALID",
            trade_date=trade_date,
        )
    if payload.get("trade_date") != trade_date.isoformat() or tuple(payload.get("symbol_set") or ()) != symbols:
        raise _batch_error(
            "MiniQMT supporting daily fact identity conflicts with the plan",
            reason_code="DAILY_TRADING_CONTEXT_SUPPORTING_FACT_INVALID",
            trade_date=trade_date,
        )
    st = payload.get("stock_st_facts")
    suspend = payload.get("suspend_facts")
    stock_st_source = payload.get("stock_st")
    suspend_source = payload.get("suspend_d")
    if not all(isinstance(value, Mapping) for value in (st, suspend, stock_st_source, suspend_source)):
        raise _batch_error(
            "MiniQMT supporting daily fact payload is incomplete",
            reason_code="DAILY_TRADING_CONTEXT_SUPPORTING_FACT_INVALID",
            trade_date=trade_date,
        )
    assert isinstance(st, Mapping) and isinstance(suspend, Mapping)
    if set(st) != set(symbols) or set(suspend) != set(symbols):
        raise _batch_error(
            "MiniQMT supporting daily facts do not exactly cover symbols",
            reason_code="DAILY_TRADING_CONTEXT_SUPPORTING_FACT_INVALID",
            trade_date=trade_date,
        )
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
            raise _batch_error(
                "MiniQMT supporting daily symbol fact is invalid",
                reason_code="DAILY_TRADING_CONTEXT_SUPPORTING_FACT_INVALID",
                trade_date=trade_date,
                symbol=symbol,
            )
    return (
        {symbol: st[symbol] for symbol in symbols},
        {symbol: suspend[symbol] for symbol in symbols},
        dict(stock_st_source),
        dict(suspend_source),
    )


def _exact_symbols(symbols: list[str]) -> tuple[str, ...]:
    raw = [str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()]
    canonical = [symbol.upper() for symbol in raw]
    if (
        not canonical
        or len(raw) != len(set(canonical))
        or any(symbol != upper for symbol, upper in zip(raw, canonical))
    ):
        raise DataUnavailableError(
            "MiniQMT daily authority requires exact unique uppercase symbols",
            context={"reason_code": "DAILY_TRADING_CONTEXT_SYMBOL_ALIAS_COLLISION"},
        )
    return tuple(sorted(canonical))


def _exact_batch(
    raw_batch: Mapping[str, Mapping[str, Any]],
    symbols: tuple[str, ...],
    *,
    trade_date: date,
) -> dict[str, Mapping[str, Any]]:
    if not isinstance(raw_batch, Mapping):
        raise _batch_error(
            "MiniQMT instrument-detail batch payload is invalid",
            reason_code="DAILY_TRADING_CONTEXT_AUTHORITY_INVALID",
            trade_date=trade_date,
        )
    normalized: dict[str, Mapping[str, Any]] = {}
    for key, value in raw_batch.items():
        if type(key) is not str or key != key.strip() or key != key.upper() or key in normalized:
            raise _batch_error(
                "MiniQMT instrument-detail batch contains alias or duplicate keys",
                reason_code="DAILY_TRADING_CONTEXT_AUTHORITY_INVALID",
                trade_date=trade_date,
            )
        if not isinstance(value, Mapping):
            raise _batch_error(
                "MiniQMT instrument-detail batch contains an invalid row",
                reason_code="DAILY_TRADING_CONTEXT_AUTHORITY_INVALID",
                trade_date=trade_date,
            )
        normalized[key] = value
    if set(normalized) != set(symbols):
        raise _batch_error(
            "MiniQMT instrument-detail batch does not exactly cover the plan",
            reason_code="DAILY_TRADING_CONTEXT_AUTHORITY_INVALID",
            trade_date=trade_date,
            missing=sorted(set(symbols) - set(normalized))[:20],
            extras=sorted(set(normalized) - set(symbols))[:20],
        )
    return normalized


def _strict_json_mapping(payload: Mapping[str, Any], *, field: str) -> dict[str, Any]:
    result = dict(payload)
    try:
        canonical_json_sha256(result)
    except Exception as exc:
        raise DataUnavailableError(
            f"{field} must be canonical JSON evidence",
            context={"reason_code": "DAILY_TRADING_CONTEXT_CALENDAR_SNAPSHOT_INVALID"},
        ) from exc
    return result


def _required_identity(value: str, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise DataUnavailableError(
            f"MiniQMT daily authority requires {field}",
            context={"reason_code": "DAILY_TRADING_CONTEXT_AUTHORITY_IDENTITY_MISSING", "field": field},
        )
    return text


def _positive_price(value: Any, *, field: str) -> float:
    if type(value) is bool:
        raise ValueError(field)
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(field)
    return number


def _optional_nonnegative_price(value: Any, *, field: str) -> float | None:
    if value is None:
        return None
    if type(value) is bool:
        raise ValueError(field)
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(field)
    return number


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if type(value) is int:
        value = str(value)
    if type(value) is not str:
        return None
    text = value.strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _board(symbol: str) -> str:
    code = symbol.split(".", 1)[0]
    if code.startswith(("688", "689")):
        return "STAR"
    if code.startswith(("300", "301", "302")):
        return "CHINEXT"
    if code.startswith(("60", "00")):
        return "MAIN"
    return "UNKNOWN"


def _batch_error(message: str, *, reason_code: str, trade_date: date, **context: Any) -> DataUnavailableError:
    return DataUnavailableError(
        message,
        context={"reason_code": reason_code, "trade_date": trade_date.isoformat(), **context},
    )


__all__ = [
    "MINIQMT_NO_DAILY_LIMIT_RULE_VERSION",
    "MiniQMTDailyLimitAuthorityProvider",
]
