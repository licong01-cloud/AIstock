"""TDX current-day quote and causal-minute helpers.

This module never reads historical minute tables and never writes market data.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import math
from typing import Any, Iterator, Mapping
from zoneinfo import ZoneInfo
import requests

from backend.data_service.tdx_adapter import TDX_DEFAULT_PORT, _to_tdx_code, fetch_minute_kline_tdx
from backend.services.simulation_data.contracts import (
    PRICE_COMPARE_EPSILON,
    PRICE_TICK,
    PRICE_UNIT_DIVISOR,
    TDX_REALTIME_BATCH_QUOTE_LIMIT,
    TDX_REALTIME_QUOTE_MAX_AGE,
    TDX_REALTIME_QUOTE_MAX_FUTURE_SKEW,
    DailyStStatus,
    CausalMinuteBatch,
    MINUTE_VOLUME_HAND_SIZE,
    MinuteExecutionMarketInput,
    MinuteDataSource,
    TdxMinuteFetcher,
    StStatusProvider,
    _canonical_json_sha256,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import MinuteBar
from backend.services.simulation_data.daily_context import DailyTradingSymbolFactV1, DailyTradingSymbolFactV2
from backend.services.simulation_data.frozen_daily_fact import parse_frozen_daily_symbol_fact


_ASIA_SHANGHAI = ZoneInfo("Asia/Shanghai")


class TdxCausalMinuteProvider:
    """Current-day TDX reader; it has no database or fallback dependency."""

    def __init__(self, *, fetcher: TdxMinuteFetcher | None = None) -> None:
        self._fetcher = fetcher or fetch_minute_kline_tdx

    def load(
        self,
        *,
        symbol: str,
        trade_date: date,
        observed_until: datetime,
        frozen_daily_fact: Mapping[str, Any],
    ) -> CausalMinuteBatch:
        normalized = str(symbol or "").strip()
        if (
            not normalized
            or observed_until.tzinfo is None
            or observed_until.utcoffset() is None
            or observed_until.astimezone(_ASIA_SHANGHAI).date() != trade_date
        ):
            raise DataUnavailableError(
                "TDX causal minute request identity is invalid",
                context={"symbol": normalized, "trade_date": trade_date.isoformat()},
            )
        fact = parse_frozen_daily_symbol_fact(normalized, trade_date, frozen_daily_fact)
        try:
            rows = self._fetcher(normalized, trade_date) or []
        except Exception as exc:
            raise DataUnavailableError(
                "TDX causal minute fetch failed",
                context={"symbol": normalized, "trade_date": trade_date.isoformat()},
            ) from exc
        until_cmp = _naive(observed_until)
        bars = tuple(
            bar
            for bar in (_tdx_minute_bar(normalized, trade_date, row, fact) for row in rows)
            if _naive(bar.bar_time) <= until_cmp
        )
        payload = {
            "symbol": normalized,
            "trade_date": trade_date.isoformat(),
            "observed_until": observed_until.isoformat(),
            "source": MinuteDataSource.TDX_REALTIME.value,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        return CausalMinuteBatch(
            symbol=normalized,
            trade_date=trade_date,
            observed_until=observed_until,
            bars=bars,
            batch_hash=_canonical_json_sha256(payload),
        )

    def load_market_input(
        self,
        *,
        symbol: str,
        trade_date: date,
        observed_until: datetime,
        frozen_daily_fact: Mapping[str, Any],
    ) -> MinuteExecutionMarketInput:
        batch = self.load(
            symbol=symbol,
            trade_date=trade_date,
            observed_until=observed_until,
            frozen_daily_fact=frozen_daily_fact,
        )
        return MinuteExecutionMarketInput(
            symbol=batch.symbol,
            trade_date=batch.trade_date,
            source=batch.source,
            minute_bars=list(batch.bars),
            market_context={
                "feed_mode": "tdx_causal_current_day",
                "observed_until": batch.observed_until.isoformat(),
                "batch_hash": batch.batch_hash,
                "daily_trading_context": dict(frozen_daily_fact),
            },
        )


def _tdx_minute_bar(
    symbol: str,
    trade_date: date,
    raw: Mapping[str, Any],
    fact: DailyTradingSymbolFactV1 | DailyTradingSymbolFactV2,
) -> MinuteBar:
    bar_time = raw.get("time") or raw.get("bar_time") or raw.get("trade_time")
    if not isinstance(bar_time, datetime) or bar_time.date() != trade_date:
        raise DataUnavailableError("TDX causal minute bar timestamp is invalid", context={"symbol": symbol})
    prices = {
        name: _positive_number(raw.get(name), field=name, symbol=symbol) for name in ("open", "high", "low", "close")
    }
    volume = _nonnegative_number(raw.get("volume"), field="volume", symbol=symbol)
    amount_raw = raw.get("amount")
    amount = None if amount_raw is None else _nonnegative_number(amount_raw, field="amount", symbol=symbol)
    return MinuteBar(
        symbol=symbol,
        bar_time=bar_time,
        **prices,
        volume=int(round(volume * MINUTE_VOLUME_HAND_SIZE)),
        amount=amount,
        is_suspended=fact.is_suspended,
        limit_up=fact.up_limit,
        limit_down=fact.down_limit,
    )


def _positive_number(value: Any, *, field: str, symbol: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DataUnavailableError(
            "TDX causal minute numeric field is invalid", context={"symbol": symbol, "field": field}
        ) from exc
    if not math.isfinite(number) or number <= 0:
        raise DataUnavailableError(
            "TDX causal minute price must be positive", context={"symbol": symbol, "field": field}
        )
    return number


def _nonnegative_number(value: Any, *, field: str, symbol: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DataUnavailableError(
            "TDX causal minute numeric field is invalid", context={"symbol": symbol, "field": field}
        ) from exc
    if not math.isfinite(number) or number < 0:
        raise DataUnavailableError(
            "TDX causal minute value must be non-negative", context={"symbol": symbol, "field": field}
        )
    return number


def _naive(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value
    return value.astimezone(_ASIA_SHANGHAI).replace(tzinfo=None)


def fetch_tdx_realtime_quotes(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """Fetch raw TDX /api/batch-quote rows keyed by AIstock symbol."""

    normalized_symbols = _normalize_symbol_list(symbols)
    if not normalized_symbols:
        return {}
    url = f"http://localhost:{TDX_DEFAULT_PORT}/api/batch-quote"
    quotes: dict[str, dict[str, Any]] = {}
    for chunk_index, chunk in enumerate(_chunks(normalized_symbols, TDX_REALTIME_BATCH_QUOTE_LIMIT), start=1):
        response = requests.post(url, json={"codes": [_to_tdx_code(symbol) for symbol in chunk]}, timeout=5)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("code") != 0:
            raise RuntimeError(
                "TDX batch-quote returned invalid response: "
                f"chunk_index={chunk_index} chunk_size={len(chunk)} total_symbols={len(normalized_symbols)} "
                f"payload={payload!r}"
            )
        items = payload.get("data") or []
        if not isinstance(items, list):
            raise RuntimeError(
                "TDX batch-quote data payload must be a list: "
                f"chunk_index={chunk_index} chunk_size={len(chunk)} total_symbols={len(normalized_symbols)} "
                f"payload_type={type(items).__name__}"
            )
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = _symbol_from_tdx_quote(item)
            if symbol:
                quotes[symbol] = dict(item)
    return quotes


def parse_tdx_reference_pre_close(
    *,
    symbol: str,
    quote: Mapping[str, Any],
    trade_date: date,
    as_of_time: datetime,
) -> dict[str, Any]:
    """Validate one TDX K.Last reference and return hash-ready CNY/share evidence."""

    normalized = str(symbol or "").strip().upper()
    if normalized != symbol or not isinstance(quote, Mapping):
        raise DataUnavailableError(
            "TDX reference quote identity is invalid",
            context={"reason_code": "DAILY_LIMIT_TDX_REFERENCE_INVALID", "symbol": symbol},
        )
    raw_quote = dict(quote)
    kline = raw_quote.get("K") if isinstance(raw_quote.get("K"), dict) else {}
    source_pre_close = _first_number(kline, ("Last",))
    basis = _quote_price_basis(raw_quote, source="TDX_REALTIME.batch_quote.pre_close")
    pre_close = (
        source_pre_close / PRICE_UNIT_DIVISOR
        if source_pre_close is not None and basis == "raw_li"
        else source_pre_close
    )
    timestamp = _require_tdx_quote_timestamp(
        symbol=normalized,
        quote=raw_quote,
        trade_date=trade_date,
        as_of_time=as_of_time,
        source="TDX_REALTIME.batch_quote.pre_close",
        max_quote_age=TDX_REALTIME_QUOTE_MAX_AGE,
    )
    if pre_close is None or not math.isfinite(pre_close) or pre_close <= 0:
        raise DataUnavailableError(
            "TDX reference K.Last is missing or invalid",
            context={"reason_code": "DAILY_LIMIT_TDX_REFERENCE_INVALID", "symbol": normalized},
        )
    evidence = {
        "schema_version": "tdx_reference_pre_close_evidence_v1",
        "source": "TDX_REALTIME.batch_quote.K.Last",
        "symbol": normalized,
        "trade_date": trade_date.isoformat(),
        "quote_timestamp": timestamp.isoformat(),
        "source_price_basis": basis,
        "source_pre_close": source_pre_close,
        "pre_close": pre_close,
    }
    return {**evidence, "evidence_hash": _canonical_json_sha256(evidence)}


def quote_tradability_evidence(
    *,
    symbol: str,
    quote: dict[str, Any],
    source: str,
    trade_date: date,
    as_of_time: datetime,
    st_status_provider: StStatusProvider,
    frozen_daily_fact: Mapping[str, Any] | None = None,
    side: str | None = None,
    max_quote_age: timedelta = TDX_REALTIME_QUOTE_MAX_AGE,
) -> dict[str, Any]:
    kline = quote.get("K") if isinstance(quote.get("K"), dict) else {}
    bid_price, bid_volume = _best_quote_level(quote, side="bid")
    ask_price, ask_volume = _best_quote_level(quote, side="ask")
    open_price = _first_number(kline, ("Open", "open"))
    high_price = _first_number(kline, ("High", "high"))
    low_price = _first_number(kline, ("Low", "low"))
    last_price = _first_number(kline, ("Close", "close"))
    if last_price is None:
        last_price = _first_number(quote, ("lastPrice", "last_price", "price", "close"))
    pre_close_price = _first_number(kline, ("Last", "pre_close", "PreClose", "preClose", "preclose"))
    if pre_close_price is None:
        pre_close_price = _first_number(quote, ("pre_close", "preClose", "preclose", "lastClose", "last_close"))
    total_hand = _first_number(quote, ("TotalHand", "total_hand", "volume", "vol"))
    amount = _first_number(quote, ("Amount", "amount"))
    quote_timestamp = _require_tdx_quote_timestamp(
        symbol=symbol,
        quote=quote,
        trade_date=trade_date,
        as_of_time=as_of_time,
        source=source,
        max_quote_age=max_quote_age,
    )
    ohl_zero = not any(_is_positive(value) for value in (open_price, high_price, low_price))
    turnover_zero = not _is_positive(total_hand) and not _is_positive(amount)
    book_empty = not (_is_positive(bid_price) and _is_positive(bid_volume)) and not (
        _is_positive(ask_price) and _is_positive(ask_volume)
    )
    no_tradable_market = bool(book_empty and (ohl_zero or turnover_zero))
    common_payload = {
        "schema_version": "pre_trade_quote_tradability_evidence_v1",
        "symbol": symbol,
        "quote_source": source,
        "quote_present": True,
        "quote_timestamp": quote_timestamp.isoformat(),
        "quote_age_seconds": max(0.0, (_normalize_datetime_for_compare(as_of_time) - quote_timestamp).total_seconds()),
        "quote_max_age_seconds": max_quote_age.total_seconds(),
        "last_price": last_price,
        "pre_close": pre_close_price,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "total_hand": total_hand,
        "amount": amount,
        "bid_price_1": bid_price,
        "bid_volume_1": bid_volume,
        "ask_price_1": ask_price,
        "ask_volume_1": ask_volume,
        "ohl_zero": ohl_zero,
        "turnover_zero": turnover_zero,
        "book_empty": book_empty,
        "no_tradable_market": no_tradable_market,
    }
    quote_feed_health = quote.get("quote_feed_health")
    if isinstance(quote_feed_health, dict):
        common_payload["quote_feed_health"] = dict(quote_feed_health)
    if no_tradable_market:
        return common_payload
    frozen_fact = None
    if frozen_daily_fact is not None:
        try:
            from backend.services.simulation_data.daily_context import (
                DailyTradingAuthorityStateV2,
                DailyTradingSymbolFactV1,
                DailyTradingSymbolFactV2,
            )

            frozen_fact = (
                DailyTradingSymbolFactV2.model_validate(dict(frozen_daily_fact))
                if "authority_state" in frozen_daily_fact
                else DailyTradingSymbolFactV1.model_validate(dict(frozen_daily_fact))
            )
        except Exception as exc:
            raise DataUnavailableError(
                "pre-trade quote carries an invalid frozen daily trading fact",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_QUOTE_FACT_INVALID",
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc
        if (
            isinstance(frozen_fact, DailyTradingSymbolFactV2)
            and frozen_fact.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED
        ):
            raise DataUnavailableError(
                "pre-trade quote refuses an unavailable daily limit authority",
                context={
                    "reason_code": str(frozen_fact.authority_reason_code),
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                },
            )
        if frozen_fact.symbol != symbol or frozen_fact.trade_date != trade_date:
            raise DataUnavailableError(
                "pre-trade quote frozen daily fact identity conflicts with the quote",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_QUOTE_FACT_CONFLICT",
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                },
            )
        st_status = DailyStStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_st=frozen_fact.is_st,
            source=frozen_fact.st_source,
        )
    else:
        st_status = _require_st_status(
            st_status_provider,
            symbol=symbol,
            trade_date=trade_date,
            context_source=f"{source}.quote_tradability",
        )
    if last_price is None or last_price <= 0:
        raise DataUnavailableError(
            f"{_quote_source_label(source)} realtime quote last price is missing or invalid",
            context={
                "reason_code": "REALTIME_QUOTE_LAST_PRICE_MISSING",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "quote_source": source,
                "last_price": last_price,
            },
        )
    if frozen_fact is None and (pre_close_price is None or pre_close_price <= 0):
        raise DataUnavailableError(
            f"{_quote_source_label(source)} realtime quote previous close is missing or invalid",
            context={
                "reason_code": "REALTIME_QUOTE_PRE_CLOSE_MISSING",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "quote_source": source,
                "pre_close": pre_close_price,
            },
        )
    if frozen_fact is not None:
        pre_close_price = frozen_fact.pre_close
        price_basis = frozen_fact.price_basis
        limit_up = frozen_fact.up_limit
        limit_down = frozen_fact.down_limit
        if (
            isinstance(frozen_fact, DailyTradingSymbolFactV2)
            and frozen_fact.authority_state is DailyTradingAuthorityStateV2.NO_DAILY_LIMIT
        ):
            limit_pct = None
            limit_source = "MINIQMT_INSTRUMENT_DETAIL_V1:frozen_daily_trading_context_v2:no_daily_limit"
        else:
            if pre_close_price is None or limit_up is None or limit_down is None:
                raise DataUnavailableError(
                    "frozen daily trading fact has incomplete limit prices",
                    context={
                        "reason_code": "DAILY_TRADING_CONTEXT_QUOTE_FACT_INVALID",
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                    },
                )
            limit_pct = max(limit_up / pre_close_price - 1.0, 1.0 - limit_down / pre_close_price)
            limit_source = (
                f"{frozen_fact.limit_authority.value}:frozen_daily_trading_context_v2"
                if isinstance(frozen_fact, DailyTradingSymbolFactV2)
                else "market.stk_limit:frozen_daily_trading_context_v1"
            )
    else:
        price_basis = _quote_price_basis(quote, source=source)
        limit_pct = _a_share_daily_limit_pct(symbol, st_status=st_status)
        limit_up = _round_quote_price_tick(pre_close_price * (1.0 + limit_pct), price_basis=price_basis)
        limit_down = _round_quote_price_tick(pre_close_price * (1.0 - limit_pct), price_basis=price_basis)
        limit_source = "derived_quote_compatibility"
    if limit_down is not None and limit_up is not None and limit_down >= limit_up:
        raise DataUnavailableError(
            f"{_quote_source_label(source)} realtime quote derived limit price range is invalid",
            context={
                "reason_code": "REALTIME_QUOTE_LIMIT_RANGE_INVALID",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "quote_source": source,
                "pre_close": pre_close_price,
                "price_basis": price_basis,
                "limit_pct": limit_pct,
                "limit_up": limit_up,
                "limit_down": limit_down,
            },
        )
    normalized_side = _normalize_order_side(side)
    at_limit_up = bool(limit_up is not None and last_price >= limit_up - PRICE_COMPARE_EPSILON)
    at_limit_down = bool(limit_down is not None and last_price <= limit_down + PRICE_COMPARE_EPSILON)
    blocked_sides: list[str] = []
    if at_limit_up:
        blocked_sides.append("BUY")
    if at_limit_down:
        blocked_sides.append("SELL")
    side_block_reason_code = None
    if normalized_side == "BUY" and at_limit_up:
        side_block_reason_code = "LIMIT_UP_BUY_BLOCKED"
    elif normalized_side == "SELL" and at_limit_down:
        side_block_reason_code = "LIMIT_DOWN_SELL_BLOCKED"
    limit_state_reason_code = (
        "REALTIME_QUOTE_LIMIT_STATE_REQUIRES_SIDE" if blocked_sides and normalized_side is None else None
    )
    return {
        **common_payload,
        "pre_close": pre_close_price,
        "limit_pct": limit_pct,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "quote_price_basis": price_basis,
        "is_st": st_status.is_st,
        "st_status_source": st_status.source,
        "limit_price_source": limit_source,
        "at_limit_up": at_limit_up,
        "at_limit_down": at_limit_down,
        "blocked_sides": blocked_sides,
        "requested_side": normalized_side,
        "side_block_reason_code": side_block_reason_code,
        "limit_state_reason_code": limit_state_reason_code,
    }


def _normalize_side_by_symbol(side_by_symbol: dict[str, Any] | None, symbols: list[str]) -> dict[str, str]:
    if not side_by_symbol:
        return {}
    normalized: dict[str, str] = {}
    for symbol in symbols:
        side = _normalize_order_side(side_by_symbol.get(symbol))
        if side is not None:
            normalized[symbol] = side
    return normalized


def _normalize_order_side(side: Any) -> str | None:
    if side is None:
        return None
    value = getattr(side, "value", side)
    normalized = str(value or "").strip().upper()
    if not normalized:
        return None
    if normalized in {"BUY", "B"}:
        return "BUY"
    if normalized in {"SELL", "S"}:
        return "SELL"
    raise DataUnavailableError(
        "pre-trade quote side is invalid",
        context={"reason_code": "PRE_TRADE_SIDE_INVALID", "side": str(value)},
    )


def _require_tdx_quote_timestamp(
    *,
    symbol: str,
    quote: dict[str, Any],
    trade_date: date,
    as_of_time: datetime,
    source: str,
    max_quote_age: timedelta,
) -> datetime:
    raw_timestamp = _extract_tdx_quote_timestamp_raw(quote)
    quote_feed_health = _quote_feed_health_payload(quote)
    if raw_timestamp is None:
        raise DataUnavailableError(
            f"{_quote_source_label(source)} realtime quote timestamp is missing",
            context={
                "reason_code": "REALTIME_QUOTE_TIMESTAMP_MISSING",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "as_of_time": as_of_time.isoformat(),
                "quote_source": source,
                "timestamp_fields_checked": _tdx_quote_timestamp_field_names(),
                "quote_feed_health": quote_feed_health,
            },
        )
    try:
        quote_timestamp = _parse_tdx_quote_timestamp(raw_timestamp, trade_date=trade_date)
    except ValueError as exc:
        raise DataUnavailableError(
            f"{_quote_source_label(source)} realtime quote timestamp is invalid",
            context={
                "reason_code": "REALTIME_QUOTE_TIMESTAMP_INVALID",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "as_of_time": as_of_time.isoformat(),
                "quote_source": source,
                "raw_timestamp": raw_timestamp,
                "parse_error": str(exc),
                "quote_feed_health": quote_feed_health,
            },
        ) from exc
    if quote_timestamp.date() != trade_date:
        raise DataUnavailableError(
            f"{_quote_source_label(source)} realtime quote timestamp date does not match trade_date",
            context={
                "reason_code": "REALTIME_QUOTE_DATE_MISMATCH",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "as_of_time": as_of_time.isoformat(),
                "quote_source": source,
                "quote_timestamp": quote_timestamp.isoformat(),
                "raw_timestamp": raw_timestamp,
                "quote_feed_health": quote_feed_health,
            },
        )
    as_of_cmp = _normalize_datetime_for_compare(as_of_time)
    age = as_of_cmp - quote_timestamp
    if age > max_quote_age:
        raise DataUnavailableError(
            f"{_quote_source_label(source)} realtime quote is stale",
            context={
                "reason_code": "REALTIME_QUOTE_STALE",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "as_of_time": as_of_time.isoformat(),
                "quote_source": source,
                "quote_timestamp": quote_timestamp.isoformat(),
                "quote_age_seconds": age.total_seconds(),
                "max_quote_age_seconds": max_quote_age.total_seconds(),
                "raw_timestamp": raw_timestamp,
                "quote_feed_health": quote_feed_health,
            },
        )
    if quote_timestamp - as_of_cmp > TDX_REALTIME_QUOTE_MAX_FUTURE_SKEW:
        raise DataUnavailableError(
            f"{_quote_source_label(source)} realtime quote timestamp is in the future",
            context={
                "reason_code": "REALTIME_QUOTE_FUTURE_TIMESTAMP",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "as_of_time": as_of_time.isoformat(),
                "quote_source": source,
                "quote_timestamp": quote_timestamp.isoformat(),
                "max_future_skew_seconds": TDX_REALTIME_QUOTE_MAX_FUTURE_SKEW.total_seconds(),
                "raw_timestamp": raw_timestamp,
                "quote_feed_health": quote_feed_health,
            },
        )
    return quote_timestamp


def _quote_feed_health_payload(quote: dict[str, Any]) -> dict[str, Any] | None:
    payload = quote.get("quote_feed_health")
    return dict(payload) if isinstance(payload, dict) else None


def _extract_tdx_quote_timestamp_raw(quote: dict[str, Any]) -> Any | None:
    for key in _tdx_quote_timestamp_field_names():
        if key in quote and quote.get(key) not in (None, ""):
            return quote.get(key)
    kline = quote.get("K")
    if isinstance(kline, dict):
        for key in _tdx_quote_timestamp_field_names():
            if key in kline and kline.get(key) not in (None, ""):
                return kline.get(key)
    return None


def _tdx_quote_timestamp_field_names() -> tuple[str, ...]:
    return (
        "ServerTime",
        "serverTime",
        "server_time",
        "timestamp",
        "quote_timestamp",
        "quoteTime",
        "quote_time",
        "time",
        "Time",
        "datetime",
        "date_time",
        "update_time",
    )


def _parse_tdx_quote_timestamp(value: Any, *, trade_date: date) -> datetime:
    if isinstance(value, datetime):
        return _normalize_datetime_for_compare(value)
    text = str(value).strip()
    if not text:
        raise ValueError("empty timestamp")
    if text.isdigit():
        if len(text) <= 6:
            return _parse_tdx_intraday_time(text, trade_date=trade_date)
        if len(text) == 8:
            if _looks_like_yyyymmdd(text):
                raise ValueError("date-only timestamp has no intraday time")
            return _parse_tdx_intraday_centisecond_time(text, trade_date=trade_date)
        if len(text) == 7:
            return _parse_tdx_intraday_centisecond_time(text, trade_date=trade_date)
        if len(text) == 14:
            return datetime.strptime(text, "%Y%m%d%H%M%S")
        numeric = int(text)
        if numeric >= 10**12:
            return datetime.fromtimestamp(numeric / 1000)
        if numeric >= 10**9:
            return datetime.fromtimestamp(numeric)
        raise ValueError(f"unsupported numeric timestamp length {len(text)}")
    normalized = text.replace("/", "-")
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        return _normalize_datetime_for_compare(datetime.fromisoformat(normalized))
    except ValueError as iso_exc:
        iso_parse_error = str(iso_exc)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(normalized, fmt)
        except ValueError:
            continue
        if fmt.startswith("%H"):
            return datetime.combine(trade_date, parsed.time())
        return parsed
    raise ValueError(f"unsupported timestamp format {text!r}; iso_parse_error={iso_parse_error}")


def _parse_tdx_intraday_time(value: str, *, trade_date: date) -> datetime:
    if len(value) <= 4:
        padded = value.zfill(4)
        hour = int(padded[:2])
        minute = int(padded[2:4])
        second = 0
    else:
        padded = value.zfill(6)
        hour = int(padded[:2])
        minute = int(padded[2:4])
        second = int(padded[4:6])
    if hour > 23 or minute > 59 or second > 59:
        raise ValueError(f"invalid intraday timestamp {value!r}")
    return datetime(trade_date.year, trade_date.month, trade_date.day, hour, minute, second)


def _parse_tdx_intraday_centisecond_time(value: str, *, trade_date: date) -> datetime:
    if len(value) == 7:
        hour = int(value[0])
        minute = int(value[1:3])
        second = int(value[3:5])
        centisecond = int(value[5:7])
    elif len(value) == 8:
        hour = int(value[0:2])
        minute = int(value[2:4])
        second = int(value[4:6])
        centisecond = int(value[6:8])
    else:
        raise ValueError(f"unsupported TDX centisecond intraday timestamp length {len(value)}")
    if hour > 23 or centisecond > 99:
        raise ValueError(f"invalid TDX centisecond intraday timestamp {value!r}")
    if minute > 59:
        if minute in {97, 98, 99}:
            # TDX ServerTime is the Go bridge's raw ReversedBytes0 sequence.
            # RCA evidence shows 97/98/99 are late-hour sentinels, not HHMM.
            minute = 59
            second = 0
            centisecond = 0
        else:
            raise ValueError(f"invalid TDX centisecond intraday timestamp {value!r}")
    if second > 59:
        # Some TDX servers expose HHMM plus a non-clock intra-minute sequence
        # in the final four digits (for example 10158777 at 10:15). The minute
        # is still authoritative for freshness; do not turn it into 10:16:27.
        second = 0
        centisecond = 0
    parsed = datetime(trade_date.year, trade_date.month, trade_date.day, hour, minute) + timedelta(
        seconds=second,
        milliseconds=centisecond * 10,
    )
    if parsed.date() != trade_date:
        raise ValueError(f"TDX centisecond intraday timestamp overflows trade_date {value!r}")
    return parsed


def _looks_like_yyyymmdd(value: str) -> bool:
    if len(value) != 8 or not value.startswith(("19", "20")):
        return False
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return False
    return True


def _chunks(values: list[str], size: int) -> Iterator[list[str]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _normalize_datetime_for_compare(value: datetime) -> datetime:
    return value.replace(tzinfo=None) if value.tzinfo is not None else value


def _require_st_status(
    st_status_provider: StStatusProvider | None,
    *,
    symbol: str,
    trade_date: date,
    context_source: str,
) -> DailyStStatus:
    if st_status_provider is None:
        raise DataUnavailableError(
            "ST status provider is required",
            context={
                "reason_code": "ST_STATUS_PROVIDER_MISSING",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "source": context_source,
            },
        )
    try:
        status = st_status_provider.get_st_status(symbol, trade_date)
    except DataUnavailableError as exc:
        if exc.context.get("reason_code"):
            raise
        raise DataUnavailableError(
            "ST status is unavailable",
            context={
                "reason_code": "ST_STATUS_UNAVAILABLE",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "source": context_source,
                "provider_error": exc.message,
                "provider_context": exc.context,
            },
        ) from exc
    except Exception as exc:
        raise DataUnavailableError(
            "ST status provider failed",
            context={
                "reason_code": "ST_STATUS_PROVIDER_FAILED",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "source": context_source,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        ) from exc
    if not isinstance(status, DailyStStatus):
        raise DataUnavailableError(
            "ST status provider returned invalid payload",
            context={
                "reason_code": "ST_STATUS_INVALID_PAYLOAD",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "source": context_source,
                "payload_type": type(status).__name__,
            },
        )
    return status


def _a_share_daily_limit_pct(symbol: str, *, st_status: DailyStStatus) -> float:
    if st_status.is_st:
        return 0.05
    code = str(symbol or "").split(".")[0]
    suffix = str(symbol or "").split(".")[-1].upper() if "." in str(symbol or "") else ""
    if suffix in {"BJ", "BSE"} or code.startswith(("4", "8")):
        return 0.30
    if code.startswith(("300", "301", "302", "688", "689")):
        return 0.20
    return 0.10


def _round_price_tick_raw(value: float) -> float:
    return _round_quote_price_tick(value, price_basis="raw_li")


def _round_price_tick_yuan(value: float) -> float:
    return float(Decimal(str(value)).quantize(PRICE_TICK, rounding=ROUND_HALF_UP))


def _round_quote_price_tick(value: float, *, price_basis: str) -> float:
    if price_basis == "yuan":
        return _round_price_tick_yuan(value)
    if price_basis == "raw_li":
        raw_tick = PRICE_TICK * Decimal(str(PRICE_UNIT_DIVISOR))
        rounded_units = (Decimal(str(value)) / raw_tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return float(rounded_units * raw_tick)
    raise DataUnavailableError(
        "realtime quote price basis is invalid",
        context={
            "reason_code": "REALTIME_QUOTE_PRICE_BASIS_INVALID",
            "price_basis": price_basis,
        },
    )


def _quote_price_basis(quote: dict[str, Any], *, source: str) -> str:
    normalized_source = str(source or "").upper()
    if normalized_source.startswith("MINIQMT_REALTIME"):
        # MiniQMT/xtdata L1 prices are yuan-denominated. Some broker payloads
        # have carried stale raw_li metadata; trusting it collapses A-share
        # limit prices to an integer tick and blocks unattended pre-run.
        return "yuan"
    if normalized_source.startswith("TDX_REALTIME"):
        # The TDX Go bridge exposes prices in li. Source identity, rather than
        # optional payload metadata, owns this basis contract.
        return "raw_li"
    raw_basis = str(quote.get("price_basis") or quote.get("quote_price_basis") or "").strip().lower()
    if raw_basis in {"yuan", "raw_li"}:
        return raw_basis
    if raw_basis:
        raise DataUnavailableError(
            "realtime quote price basis is invalid",
            context={
                "reason_code": "REALTIME_QUOTE_PRICE_BASIS_INVALID",
                "quote_source": source,
                "price_basis": raw_basis,
            },
        )
    return "raw_li"


def _quote_source_label(source: str) -> str:
    if str(source or "").upper().startswith("MINIQMT_REALTIME"):
        return "MiniQMT"
    return "TDX"


def _normalize_symbol_list(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    for symbol in symbols:
        text = str(symbol or "").strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _symbol_from_tdx_quote(row: dict[str, Any]) -> str | None:
    code = str(row.get("Code") or row.get("code") or "").strip()
    if not code:
        return None
    exchange_raw = row.get("Exchange", row.get("exchange"))
    if isinstance(exchange_raw, (int, float)):
        exchange = {0: "SZ", 1: "SH", 2: "BJ"}.get(int(exchange_raw), "")
    else:
        exchange = str(exchange_raw or "").strip().upper()
    if not exchange and len(code) == 6:
        if code.startswith(("0", "2", "3")):
            exchange = "SZ"
        elif code.startswith(("6", "9")):
            exchange = "SH"
        elif code.startswith(("4", "8")):
            exchange = "BJ"
    if not exchange:
        return None
    return f"{code}.{exchange}"


def _best_quote_level(quote: dict[str, Any], *, side: str) -> tuple[float | None, float | None]:
    if side == "bid":
        price = _first_number(quote, ("bid_price_1", "bidPrice1", "bid_price", "bidPrice", "bid", "bid1"))
        volume = _first_number(quote, ("bid_volume_1", "bidVol1", "bid_volume", "bidVol", "bidVolume", "bid_vol"))
        levels = quote.get("BuyLevel")
    else:
        price = _first_number(quote, ("ask_price_1", "askPrice1", "ask_price", "askPrice", "ask", "ask1"))
        volume = _first_number(quote, ("ask_volume_1", "askVol1", "ask_volume", "askVol", "askVolume", "ask_vol"))
        levels = quote.get("SellLevel")
    if (price is None or volume is None) and isinstance(levels, list) and levels:
        first_level = levels[0]
        if isinstance(first_level, dict):
            price = price if price is not None else _first_number(first_level, ("Price", "price"))
            volume = (
                volume if volume is not None else _first_number(first_level, ("Number", "number", "Volume", "volume"))
            )
    return price, volume


def _first_number(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if isinstance(value, (list, tuple)):
            value = value[0] if value else None
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _is_positive(value: Any) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False
