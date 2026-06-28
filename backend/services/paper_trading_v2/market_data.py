"""Minute market data input builder for Paper Trading v2.

The provider is read-only. It does not modify ``backend/data_service`` and it
does not silently fall back between data sources. The caller must choose the
source explicitly; missing minute bars, limit prices, or previous close fail
the run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any, Callable, Iterator, Protocol

import requests

from backend.data_service.tdx_adapter import TDX_DEFAULT_PORT, _to_tdx_code, fetch_minute_kline_tdx
from backend.db.pg_pool import get_conn
from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.trading_core.errors import (
    BrokerMarketSourceMismatchError,
    DataUnavailableError,
)
from backend.services.trading_core.limit_price_provider import (
    DailyLimitPrice,
    StkLimitPriceProvider,
)
from backend.services.trading_core.models import MinuteBar
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.paper_trading_v2.day_features import DbV25DayFeatureProvider, V25DayFeatureProvider, V25DayFeatures


PRICE_UNIT_DIVISOR = 1000.0
MINUTE_VOLUME_HAND_SIZE = 100
PRICE_TICK = Decimal("0.01")
TDX_REALTIME_QUOTE_MAX_AGE = timedelta(minutes=5)
TDX_REALTIME_QUOTE_MAX_FUTURE_SKEW = timedelta(seconds=30)
TDX_REALTIME_BATCH_QUOTE_LIMIT = 50
PRICE_COMPARE_EPSILON = 1e-6

TdxMinuteFetcher = Callable[[str, date], list[dict[str, Any]]]
RealtimeQuoteFetcher = Callable[[list[str]], dict[str, dict[str, Any]]]
ConnFactory = Callable[[], Iterator[Any]]


class MinuteDataSource(str, Enum):
    """Supported authoritative minute data sources.

    MINIQMT_REALTIME is the channel emitted by the miniQMT-bound BrokerBackend
    (Strategy Engine design 2026-05-08 §3.6.4, R-Q9 D3). The xtdata fetch
    function for it is intentionally NOT wired here yet; this enum value plus
    ``ALLOWED_MARKET_SOURCES`` / ``assert_broker_market_source_match`` only
    establish the strong-binding invariant. Concrete miniQMT minute fetch will
    be added when MiniQMTSim BrokerBackend lands (Task #20 follow-up after
    PoC re-test, task #10).
    """

    TDX_REALTIME = "TDX_REALTIME"
    DB_HISTORICAL = "DB_HISTORICAL"
    MINIQMT_REALTIME = "MINIQMT_REALTIME"


# Strong binding between BrokerBackend and minute data channel
# (Strategy Engine design 2026-05-08 §3.6.4, R-Q9 D3). Cross-pairing is a
# fail-fast invariant; do NOT add a fallback path.
ALLOWED_MARKET_SOURCES: dict[str, set[MinuteDataSource]] = {
    "local_sim": {MinuteDataSource.TDX_REALTIME, MinuteDataSource.DB_HISTORICAL},
    "minqmt_sim": {MinuteDataSource.MINIQMT_REALTIME},
    "minqmt_live": {MinuteDataSource.MINIQMT_REALTIME},
}


def assert_broker_market_source_match(
    broker_id: str,
    source: MinuteDataSource,
) -> None:
    """Validate broker_id <-> MinuteDataSource binding (R-Q9 D3 fail-fast).

    Called at portfolio bootstrap, live_session bootstrap, and Engine.init().
    Raises BrokerMarketSourceMismatchError on any mismatch. Never silently
    falls back (feedback_no_silent_errors).
    """

    if not isinstance(source, MinuteDataSource):
        raise BrokerMarketSourceMismatchError(
            "minute data source must be a MinuteDataSource enum value",
            context={"broker_id": broker_id, "given_source": repr(source)},
        )
    allowed = ALLOWED_MARKET_SOURCES.get(broker_id)
    if allowed is None:
        raise BrokerMarketSourceMismatchError(
            f"unknown broker_id {broker_id!r}",
            context={
                "broker_id": broker_id,
                "given_source": source.value,
                "known_broker_ids": sorted(ALLOWED_MARKET_SOURCES.keys()),
            },
        )
    if source not in allowed:
        raise BrokerMarketSourceMismatchError(
            f"broker_id {broker_id!r} requires market source in "
            f"{sorted(s.value for s in allowed)}; got {source.value}",
            context={
                "broker_id": broker_id,
                "given_source": source.value,
                "allowed": sorted(s.value for s in allowed),
            },
        )


@dataclass(frozen=True)
class MinuteExecutionMarketInput:
    """Minute bars and execution context for one symbol/trade date."""

    symbol: str
    trade_date: date
    source: MinuteDataSource
    minute_bars: list[MinuteBar]
    market_context: dict[str, Any]


@dataclass(frozen=True)
class DailySuspendStatus:
    """Explicit daily suspension status loaded from the authoritative source."""

    symbol: str
    trade_date: date
    is_suspended: bool
    suspend_type: str | None = None
    suspend_timing: str | None = None
    source: str = "market.suspend_d"


@dataclass(frozen=True)
class PreTradeTradabilityStatus:
    """Daily pre-trade tradability evidence for order-generation gates."""

    symbol: str
    trade_date: date
    is_tradable: bool
    reason_code: str
    source: str
    suspend_status: dict[str, Any] | None = None
    quote_evidence: dict[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "pre_trade_tradability_status_v1",
            "symbol": self.symbol,
            "trade_date": self.trade_date.isoformat(),
            "is_tradable": self.is_tradable,
            "reason_code": self.reason_code,
            "source": self.source,
            "suspend_status": self.suspend_status,
            "quote_evidence": self.quote_evidence,
        }


@dataclass(frozen=True)
class PreviousClose:
    """Authoritative previous close resolved from audited daily kline data."""

    symbol: str
    trade_date: date
    previous_trade_date: date
    pre_close: float
    source: str = "market.kline_daily_raw.previous_trading_day_close"


@dataclass(frozen=True)
class DailyStStatus:
    """Point-in-time ST/*ST status from the authoritative local market table."""

    symbol: str
    trade_date: date
    is_st: bool
    source: str = "market.stock_st"
    start_date: date | None = None
    end_date: date | None = None


class SuspendStatusProvider(Protocol):
    """Provider boundary for daily suspension status."""

    def get_suspend_status(self, symbol: str, trade_date: date) -> DailySuspendStatus:
        ...


class PreviousCloseProvider(Protocol):
    """Provider boundary for explicit previous close lookup."""

    def get_previous_close(self, symbol: str, trade_date: date) -> PreviousClose:
        ...


class StStatusProvider(Protocol):
    """Provider boundary for point-in-time ST/*ST status."""

    def get_st_status(self, symbol: str, trade_date: date) -> DailyStStatus:
        ...


class LimitPriceProvider(Protocol):
    """Provider boundary for daily limit prices."""

    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        ...


class DbSuspendStatusProvider:
    """Read daily A-share suspension rows from ``market.suspend_d``."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self.conn_factory = conn_factory or get_conn

    def get_suspend_status(self, symbol: str, trade_date: date) -> DailySuspendStatus:
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT suspend_type, suspend_timing
                        FROM market.suspend_d
                        WHERE ts_code = %s
                          AND trade_date = %s
                          AND suspend_type = 'S'
                        ORDER BY suspend_timing NULLS FIRST
                        LIMIT 1
                        """,
                        (symbol, trade_date),
                    )
                    row = cur.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "suspend status query failed",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "table": "market.suspend_d",
                },
            ) from exc
        if row is None:
            return DailySuspendStatus(symbol=symbol, trade_date=trade_date, is_suspended=False)
        return DailySuspendStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_suspended=True,
            suspend_type=str(row[0]) if row[0] is not None else "S",
            suspend_timing=str(row[1]) if row[1] is not None else None,
        )


class DbStStatusProvider:
    """Read point-in-time ST/*ST rows from ``market.stock_st``."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self.conn_factory = conn_factory or get_conn

    def get_st_status(self, symbol: str, trade_date: date) -> DailyStStatus:
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            raise DataUnavailableError(
                "symbol is required for ST status lookup",
                context={"reason_code": "ST_STATUS_SYMBOL_MISSING", "trade_date": trade_date.isoformat()},
            )
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH latest_stock_st_snapshot AS (
                            SELECT max(ann_date) AS latest_ann_date
                            FROM market.stock_st
                            WHERE ann_date <= %s
                        )
                        SELECT s.ts_code, s.start_date, s.end_date, latest.latest_ann_date
                        FROM latest_stock_st_snapshot latest
                        LEFT JOIN LATERAL (
                            SELECT ts_code, start_date, end_date, ann_date
                            FROM market.stock_st
                            WHERE ts_code = %s
                              AND (
                                (start_date IS NULL AND end_date IS NULL AND ann_date = latest.latest_ann_date)
                                OR (
                                  (start_date IS NOT NULL OR end_date IS NOT NULL)
                                  AND COALESCE(start_date, ann_date) <= %s
                                  AND (end_date IS NULL OR end_date >= %s)
                                )
                              )
                            ORDER BY
                              CASE
                                WHEN start_date IS NULL AND end_date IS NULL THEN ann_date
                                ELSE COALESCE(start_date, ann_date)
                              END DESC,
                              ann_date DESC
                            LIMIT 1
                        ) s ON TRUE
                        """,
                        (trade_date, normalized_symbol, trade_date, trade_date),
                    )
                    row = cur.fetchone()
                    if row is not None and row[3] is None:
                        raise DataUnavailableError(
                            "ST status source has no snapshot on or before trade_date",
                            context={
                                "reason_code": "ST_STATUS_SOURCE_EMPTY",
                                "symbol": normalized_symbol,
                                "trade_date": trade_date.isoformat(),
                                "table": "market.stock_st",
                            },
                        )
        except Exception as exc:
            if isinstance(exc, DataUnavailableError):
                raise
            raise DataUnavailableError(
                "ST status query failed",
                context={
                    "reason_code": "ST_STATUS_QUERY_FAILED",
                    "symbol": normalized_symbol,
                    "trade_date": trade_date.isoformat(),
                    "table": "market.stock_st",
                },
            ) from exc
        if row is None or row[0] is None:
            return DailyStStatus(symbol=normalized_symbol, trade_date=trade_date, is_st=False)
        return DailyStStatus(
            symbol=normalized_symbol,
            trade_date=trade_date,
            is_st=True,
            source=f"market.stock_st.latest_ann_date:{row[3].isoformat()}",
            start_date=row[1],
            end_date=row[2],
        )


class PreTradeTradabilityProvider:
    """Combine suspend_d and realtime quote evidence before order creation.

    The provider is intentionally read-only. If a realtime quote fetcher is
    configured and fails, callers get DataUnavailableError instead of silently
    falling back to stale close prices.
    """

    def __init__(
        self,
        *,
        suspend_status_provider: SuspendStatusProvider | None = None,
        realtime_quote_fetcher: RealtimeQuoteFetcher | None = None,
        realtime_quote_source: str | None = None,
        st_status_provider: StStatusProvider | None = None,
        require_realtime_quote: bool = False,
    ) -> None:
        self.suspend_status_provider = suspend_status_provider or DbSuspendStatusProvider()
        self.realtime_quote_fetcher = realtime_quote_fetcher
        self.realtime_quote_source = realtime_quote_source or "not_configured"
        self.st_status_provider = st_status_provider or DbStStatusProvider()
        self.require_realtime_quote = bool(require_realtime_quote)

    def get_statuses(
        self,
        symbols: list[str],
        trade_date: date,
        *,
        require_realtime_quote: bool | None = None,
        as_of_time: datetime | None = None,
        side_by_symbol: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        normalized_symbols = _normalize_symbol_list(symbols)
        if not normalized_symbols:
            return {}
        require_quote = self.require_realtime_quote if require_realtime_quote is None else bool(require_realtime_quote)
        effective_as_of_time = as_of_time or datetime.now()
        normalized_sides = _normalize_side_by_symbol(side_by_symbol, normalized_symbols)
        quotes: dict[str, dict[str, Any]] = {}
        if require_quote:
            if self.realtime_quote_fetcher is None:
                raise DataUnavailableError(
                    "pre-trade realtime quote fetcher is required",
                    context={
                        "reason_code": "REALTIME_QUOTE_FETCHER_MISSING",
                        "trade_date": trade_date.isoformat(),
                        "symbols": normalized_symbols,
                        "quote_source": self.realtime_quote_source,
                    },
                )
            try:
                quotes = self.realtime_quote_fetcher(normalized_symbols)
            except DataUnavailableError:
                raise
            except Exception as exc:
                raise DataUnavailableError(
                    "pre-trade realtime quote fetch failed",
                    context={
                        "reason_code": "REALTIME_QUOTE_FETCH_FAILED",
                        "trade_date": trade_date.isoformat(),
                        "symbols": normalized_symbols,
                        "quote_source": self.realtime_quote_source,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                ) from exc

        statuses: dict[str, dict[str, Any]] = {}
        for symbol in normalized_symbols:
            suspend = self.suspend_status_provider.get_suspend_status(symbol, trade_date)
            suspend_payload = {
                "is_suspended": bool(suspend.is_suspended),
                "suspend_type": suspend.suspend_type,
                "suspend_timing": suspend.suspend_timing,
                "source": suspend.source,
            }
            if suspend.is_suspended:
                statuses[symbol] = PreTradeTradabilityStatus(
                    symbol=symbol,
                    trade_date=trade_date,
                    is_tradable=False,
                    reason_code="SUSPENDED_BY_SUSPEND_D",
                    source="market.suspend_d",
                    suspend_status=suspend_payload,
                ).to_payload()
                continue

            quote_payload = None
            if require_quote:
                quote = quotes.get(symbol)
                if not isinstance(quote, dict):
                    statuses[symbol] = PreTradeTradabilityStatus(
                        symbol=symbol,
                        trade_date=trade_date,
                        is_tradable=False,
                        reason_code="REALTIME_QUOTE_MISSING",
                        source=self.realtime_quote_source,
                        suspend_status=suspend_payload,
                        quote_evidence={"quote_source": self.realtime_quote_source, "quote_present": False},
                    ).to_payload()
                    continue
                quote_payload = quote_tradability_evidence(
                    symbol=symbol,
                    quote=quote,
                    source=self.realtime_quote_source,
                    trade_date=trade_date,
                    as_of_time=effective_as_of_time,
                    st_status_provider=self.st_status_provider,
                    side=normalized_sides.get(symbol),
                )
                if quote_payload["no_tradable_market"]:
                    statuses[symbol] = PreTradeTradabilityStatus(
                        symbol=symbol,
                        trade_date=trade_date,
                        is_tradable=False,
                        reason_code="NO_TRADABLE_REALTIME_QUOTE",
                        source=self.realtime_quote_source,
                        suspend_status=suspend_payload,
                        quote_evidence=quote_payload,
                    ).to_payload()
                    continue
                blocked_reason_code = quote_payload.get("side_block_reason_code") or quote_payload.get("limit_state_reason_code")
                if blocked_reason_code:
                    statuses[symbol] = PreTradeTradabilityStatus(
                        symbol=symbol,
                        trade_date=trade_date,
                        is_tradable=False,
                        reason_code=str(blocked_reason_code),
                        source=self.realtime_quote_source,
                        suspend_status=suspend_payload,
                        quote_evidence=quote_payload,
                    ).to_payload()
                    continue

            statuses[symbol] = PreTradeTradabilityStatus(
                symbol=symbol,
                trade_date=trade_date,
                is_tradable=True,
                reason_code="OK",
                source=self.realtime_quote_source if require_quote else "market.suspend_d",
                suspend_status=suspend_payload,
                quote_evidence=quote_payload,
            ).to_payload()
        return statuses


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


def quote_tradability_evidence(
    *,
    symbol: str,
    quote: dict[str, Any],
    source: str,
    trade_date: date,
    as_of_time: datetime,
    st_status_provider: StStatusProvider,
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
    if no_tradable_market:
        return common_payload
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
    if pre_close_price is None or pre_close_price <= 0:
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
    price_basis = _quote_price_basis(quote, source=source)
    limit_pct = _a_share_daily_limit_pct(symbol, st_status=st_status)
    limit_up = _round_quote_price_tick(pre_close_price * (1.0 + limit_pct), price_basis=price_basis)
    limit_down = _round_quote_price_tick(pre_close_price * (1.0 - limit_pct), price_basis=price_basis)
    if limit_down >= limit_up:
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
    at_limit_up = bool(last_price >= limit_up - PRICE_COMPARE_EPSILON)
    at_limit_down = bool(last_price <= limit_down + PRICE_COMPARE_EPSILON)
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
    limit_state_reason_code = "REALTIME_QUOTE_LIMIT_STATE_REQUIRES_SIDE" if blocked_sides and normalized_side is None else None
    return {
        **common_payload,
        "limit_pct": limit_pct,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "quote_price_basis": price_basis,
        "is_st": st_status.is_st,
        "st_status_source": st_status.source,
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
            },
        )
    return quote_timestamp


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
        if minute == 99 and second > 59:
            # TDX quote ServerTime can use 99:SScc as a late-session sequence
            # sentinel; treat only that narrow encoding as the 59th minute.
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
    if str(source or "").upper().startswith("MINIQMT_REALTIME"):
        # MiniQMT/xtdata L1 prices are yuan-denominated. Some broker payloads
        # have carried stale raw_li metadata; trusting it collapses A-share
        # limit prices to an integer tick and blocks unattended pre-run.
        return "yuan"
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
            volume = volume if volume is not None else _first_number(first_level, ("Number", "number", "Volume", "volume"))
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


class DbPreviousCloseProvider:
    """Resolve pre_close from audited previous trading-day daily kline rows."""

    def __init__(
        self,
        *,
        conn_factory: ConnFactory | None = None,
        refresh_audit: DataRefreshAuditRepository | Any | None = None,
    ) -> None:
        self.conn_factory = conn_factory or get_conn
        self.refresh_audit = refresh_audit or DataRefreshAuditRepository(conn_factory=self.conn_factory)

    def get_previous_close(self, symbol: str, trade_date: date) -> PreviousClose:
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            raise DataUnavailableError("symbol is required for previous close lookup")
        previous_trade_date = self._previous_trading_day(trade_date)
        self.refresh_audit.require_success(dataset="kline_daily_raw", trade_date=previous_trade_date)
        row = self._query_previous_close(normalized_symbol, previous_trade_date)
        if row is None:
            raise DataUnavailableError(
                "previous close row is missing in market.kline_daily_raw",
                context={
                    "symbol": normalized_symbol,
                    "trade_date": trade_date.isoformat(),
                    "previous_trade_date": previous_trade_date.isoformat(),
                    "table": "market.kline_daily_raw",
                },
            )
        pre_close = PaperV2MinuteMarketDataProvider._positive_price_from_li(
            row[0],
            "close_li",
            normalized_symbol,
            previous_trade_date,
        )
        return PreviousClose(
            symbol=normalized_symbol,
            trade_date=trade_date,
            previous_trade_date=previous_trade_date,
            pre_close=pre_close,
        )

    def _previous_trading_day(self, trade_date: date) -> date:
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT max(cal_date)
                        FROM market.trading_calendar
                        WHERE cal_date < %s
                          AND is_trading = TRUE
                        """,
                        (trade_date,),
                    )
                    row = cur.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "previous close trading calendar query failed",
                context={"trade_date": trade_date.isoformat()},
            ) from exc
        if row is None or row[0] is None:
            raise DataUnavailableError(
                "previous trading day is missing for pre_close lookup",
                context={"trade_date": trade_date.isoformat()},
            )
        return row[0]

    def _query_previous_close(self, symbol: str, previous_trade_date: date) -> tuple[Any, ...] | None:
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT close_li
                        FROM market.kline_daily_raw
                        WHERE ts_code = %s
                          AND trade_date = %s
                        ORDER BY CASE WHEN adjust_type = 'none' THEN 0 ELSE 1 END
                        LIMIT 1
                        """,
                        (symbol, previous_trade_date),
                    )
                    return cur.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "previous close kline query failed",
                context={
                    "symbol": symbol,
                    "previous_trade_date": previous_trade_date.isoformat(),
                    "table": "market.kline_daily_raw",
                },
            ) from exc


class _InjectedLimitProviderPreviousCloseProvider:
    """Use an injected non-DB limit provider only as a unit-test previous close source."""

    def __init__(self, limit_price_provider: LimitPriceProvider) -> None:
        self.limit_price_provider = limit_price_provider

    def get_previous_close(self, symbol: str, trade_date: date) -> PreviousClose:
        limit_price = self.limit_price_provider.get_limit_price(symbol, trade_date)
        if limit_price.pre_close is None or float(limit_price.pre_close) <= 0:
            raise DataUnavailableError(
                "pre_close is required for minute execution context",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": "injected_limit_provider.pre_close"},
            )
        return PreviousClose(
            symbol=symbol,
            trade_date=trade_date,
            previous_trade_date=trade_date,
            pre_close=float(limit_price.pre_close),
            source="injected_limit_provider.pre_close",
        )


class _InjectedLimitProviderStStatusProvider:
    """Keep legacy unit-test limit-provider fixtures deterministic without DB access."""

    def get_st_status(self, symbol: str, trade_date: date) -> DailyStStatus:
        return DailyStStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_st=False,
            source="injected_limit_provider.non_st_unit_test_fixture",
        )


class PaperV2MinuteMarketDataProvider:
    """Build strict minute execution inputs from TDX or historical DB data."""

    def __init__(
        self,
        *,
        limit_price_provider: LimitPriceProvider | None = None,
        suspend_status_provider: SuspendStatusProvider | None = None,
        previous_close_provider: PreviousCloseProvider | None = None,
        st_status_provider: StStatusProvider | None = None,
        day_feature_provider: V25DayFeatureProvider | None = None,
        tdx_fetcher: TdxMinuteFetcher | None = None,
        conn_factory: ConnFactory | None = None,
    ) -> None:
        self.conn_factory = conn_factory or get_conn
        self.limit_price_provider = limit_price_provider or StkLimitPriceProvider()
        self.suspend_status_provider = suspend_status_provider or DbSuspendStatusProvider(conn_factory=self.conn_factory)
        if previous_close_provider is not None:
            self.previous_close_provider = previous_close_provider
        elif limit_price_provider is not None and not isinstance(limit_price_provider, StkLimitPriceProvider):
            self.previous_close_provider = _InjectedLimitProviderPreviousCloseProvider(limit_price_provider)
        else:
            self.previous_close_provider = DbPreviousCloseProvider(conn_factory=self.conn_factory)
        if st_status_provider is not None:
            self.st_status_provider = st_status_provider
        elif (
            previous_close_provider is None
            and limit_price_provider is not None
            and not isinstance(limit_price_provider, StkLimitPriceProvider)
        ):
            self.st_status_provider = _InjectedLimitProviderStStatusProvider()
        else:
            self.st_status_provider = DbStStatusProvider(conn_factory=self.conn_factory)
        self.day_feature_provider = day_feature_provider or DbV25DayFeatureProvider(conn_factory=self.conn_factory)
        self.tdx_fetcher = tdx_fetcher or fetch_minute_kline_tdx

    def load_symbol_input(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource = MinuteDataSource.TDX_REALTIME,
        min_bars: int = 1,
        require_suspend_status: bool = False,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        symbol = str(symbol or "").strip()
        if not symbol:
            raise DataUnavailableError("symbol is required for minute market data")
        if min_bars <= 0:
            raise DataUnavailableError(
                "min_bars must be positive",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "min_bars": min_bars},
            )

        limit_price, pre_close_source, limit_price_source = self._limit_price_with_required_pre_close(
            symbol,
            trade_date,
            source=source,
        )
        suspend_status = None
        if require_suspend_status:
            if self.suspend_status_provider is None:
                raise DataUnavailableError(
                    "suspend status provider is required",
                    context={"symbol": symbol, "trade_date": trade_date.isoformat()},
                )
            suspend_status = self.suspend_status_provider.get_suspend_status(symbol, trade_date)
        day_features = self._load_day_features(symbol=symbol, trade_date=trade_date, required=require_day_features)

        raw_bars = self._load_raw_bars(symbol, trade_date, source)
        minute_bars = self._build_minute_bars(
            symbol=symbol,
            trade_date=trade_date,
            raw_bars=raw_bars,
            limit_price=limit_price,
            source=source,
            require_suspend_status=require_suspend_status,
            suspend_status=suspend_status,
        )
        if len(minute_bars) < min_bars:
            raise DataUnavailableError(
                "insufficient minute bars for requested execution context",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "source": source.value,
                    "bar_count": len(minute_bars),
                    "min_bars": min_bars,
                },
            )

        context = self._build_market_context(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=minute_bars,
            limit_price=limit_price,
            pre_close_source=pre_close_source,
            limit_price_source=limit_price_source,
            suspend_status=suspend_status,
            day_features=day_features,
        )
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=minute_bars,
            market_context=context,
        )

    def load_completed_day(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        expected_bars: int,
        require_suspend_status: bool = False,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        """Load a completed historical day with no realtime fallback."""

        if source != MinuteDataSource.DB_HISTORICAL:
            raise DataUnavailableError(
                "completed-day minute feed requires an explicit historical DB source",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": source.value},
            )
        return self.load_symbol_input(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            min_bars=expected_bars,
            require_suspend_status=require_suspend_status,
            require_day_features=require_day_features,
        )

    def load_observed_intraday(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        until_time: datetime,
        require_suspend_status: bool = False,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        """Load only observed intraday bars up to ``until_time``.

        Empty observed bars are returned as an explicit waiting input. Fetch
        failures still raise; this method never falls back to historical DB.
        """

        symbol = str(symbol or "").strip()
        if not symbol:
            raise DataUnavailableError("symbol is required for observed intraday minute feed")
        if source != MinuteDataSource.TDX_REALTIME:
            raise DataUnavailableError(
                "observed intraday minute feed requires TDX_REALTIME",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": source.value},
            )
        if until_time.date() != trade_date:
            raise DataUnavailableError(
                "observed intraday until_time must match trade_date",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "until_time": until_time.isoformat()},
            )

        limit_price, pre_close_source, limit_price_source = self._limit_price_with_required_pre_close(
            symbol,
            trade_date,
            source=source,
        )
        suspend_status = None
        if require_suspend_status:
            if self.suspend_status_provider is None:
                raise DataUnavailableError(
                    "suspend status provider is required",
                    context={"symbol": symbol, "trade_date": trade_date.isoformat()},
                )
            suspend_status = self.suspend_status_provider.get_suspend_status(symbol, trade_date)
        day_features = self._load_day_features(symbol=symbol, trade_date=trade_date, required=require_day_features)

        raw_bars = self._load_raw_bars_from_tdx(symbol, trade_date, allow_empty=True)
        minute_bars = self._build_minute_bars(
            symbol=symbol,
            trade_date=trade_date,
            raw_bars=raw_bars,
            limit_price=limit_price,
            source=source,
            require_suspend_status=require_suspend_status,
            suspend_status=suspend_status,
        )
        until_cmp = self._naive_for_compare(until_time)
        observed = [bar for bar in minute_bars if self._naive_for_compare(bar.bar_time) <= until_cmp]
        context = self._build_market_context(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=observed,
            limit_price=limit_price,
            pre_close_source=pre_close_source,
            limit_price_source=limit_price_source,
            suspend_status=suspend_status,
            day_features=day_features,
        )
        context["until_time"] = until_time.isoformat()
        context["feed_mode"] = "observed_intraday"
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=observed,
            market_context=context,
        )

    def load_new_bars(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        after_time: datetime | None,
        until_time: datetime,
        require_suspend_status: bool = False,
    ) -> list[MinuteBar]:
        """Load new observed bars after a persisted cursor."""

        observed = self.load_observed_intraday(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            until_time=until_time,
            require_suspend_status=require_suspend_status,
        ).minute_bars
        if after_time is None:
            return observed
        after_cmp = self._naive_for_compare(after_time)
        return [bar for bar in observed if self._naive_for_compare(bar.bar_time) > after_cmp]

    def latest_available_bar_time(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        source: MinuteDataSource,
        as_of_time: datetime,
    ) -> datetime | None:
        """Return the latest common observed bar time across symbols."""

        normalized_symbols = [str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()]
        if not normalized_symbols:
            raise DataUnavailableError(
                "symbols are required for latest available minute bar time",
                context={"trade_date": trade_date.isoformat(), "source": source.value},
            )
        if source != MinuteDataSource.TDX_REALTIME:
            raise DataUnavailableError(
                "latest available live minute bar time requires TDX_REALTIME",
                context={"trade_date": trade_date.isoformat(), "source": source.value},
            )
        as_of_cmp = self._naive_for_compare(as_of_time)
        latest_by_symbol: list[datetime] = []
        for symbol in normalized_symbols:
            raw_bars = self._load_raw_bars_from_tdx(symbol, trade_date, allow_empty=True)
            bar_times = []
            for raw in raw_bars:
                bar_time = raw.get("time") or raw.get("bar_time") or raw.get("trade_time")
                if not isinstance(bar_time, datetime):
                    raise DataUnavailableError(
                        "minute bar time is missing or invalid",
                        context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": source.value},
                    )
                if bar_time.date() != trade_date:
                    raise DataUnavailableError(
                        "minute bar date does not match requested trade_date",
                        context={
                            "symbol": symbol,
                            "trade_date": trade_date.isoformat(),
                            "bar_time": bar_time.isoformat(),
                            "source": source.value,
                        },
                    )
                if self._naive_for_compare(bar_time) <= as_of_cmp:
                    bar_times.append(bar_time)
            if not bar_times:
                return None
            latest_by_symbol.append(max(bar_times))
        return min(latest_by_symbol)

    def _load_raw_bars(
        self,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
    ) -> list[dict[str, Any]]:
        if source == MinuteDataSource.TDX_REALTIME:
            return self._load_raw_bars_from_tdx(symbol, trade_date, allow_empty=False)

        if source == MinuteDataSource.DB_HISTORICAL:
            return self._load_raw_bars_from_db(symbol, trade_date)

        raise DataUnavailableError(
            "unsupported minute data source",
            context={"symbol": symbol, "source": str(source)},
        )

    def _load_raw_bars_from_tdx(
        self,
        symbol: str,
        trade_date: date,
        *,
        allow_empty: bool,
    ) -> list[dict[str, Any]]:
        try:
            raw_bars = self.tdx_fetcher(symbol, trade_date)
        except Exception as exc:
            raise DataUnavailableError(
                "TDX minute data fetch failed",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            ) from exc
        if not raw_bars and not allow_empty:
            raise DataUnavailableError(
                "TDX returned no minute bars",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            )
        return raw_bars or []

    def _load_raw_bars_from_db(self, symbol: str, trade_date: date) -> list[dict[str, Any]]:
        with self.conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_time, open_li, high_li, low_li, close_li,
                           volume_hand, amount_li
                    FROM market.kline_minute_raw
                    WHERE ts_code = %s
                      AND trade_time >= %s::date
                      AND trade_time < %s::date + interval '1 day'
                    ORDER BY trade_time ASC
                    """,
                    (symbol, trade_date, trade_date),
                )
                rows = cur.fetchall()
        if not rows:
            raise DataUnavailableError(
                "historical DB returned no minute bars",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            )
        return [
            {
                "time": row[0],
                "open": self._positive_price_from_li(row[1], "open_li", symbol, trade_date),
                "high": self._positive_price_from_li(row[2], "high_li", symbol, trade_date),
                "low": self._positive_price_from_li(row[3], "low_li", symbol, trade_date),
                "close": self._positive_price_from_li(row[4], "close_li", symbol, trade_date),
                "volume": row[5],
                "amount": float(row[6]) / PRICE_UNIT_DIVISOR if row[6] is not None else None,
            }
            for row in rows
        ]

    def _build_minute_bars(
        self,
        *,
        symbol: str,
        trade_date: date,
        raw_bars: list[dict[str, Any]],
        limit_price: DailyLimitPrice,
        source: MinuteDataSource,
        require_suspend_status: bool = False,
        suspend_status: DailySuspendStatus | None = None,
    ) -> list[MinuteBar]:
        minute_bars: list[MinuteBar] = []
        for raw in raw_bars:
            bar_time = raw.get("time") or raw.get("bar_time") or raw.get("trade_time")
            if not isinstance(bar_time, datetime):
                raise DataUnavailableError(
                    "minute bar time is missing or invalid",
                    context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": source.value},
                )
            if bar_time.date() != trade_date:
                raise DataUnavailableError(
                    "minute bar date does not match requested trade_date",
                    context={
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "bar_time": bar_time.isoformat(),
                        "source": source.value,
                    },
                )
            minute_bars.append(
                MinuteBar(
                    symbol=symbol,
                    bar_time=bar_time,
                    open=self._positive_float(raw.get("open"), "open", symbol, bar_time),
                    high=self._positive_float(raw.get("high"), "high", symbol, bar_time),
                    low=self._positive_float(raw.get("low"), "low", symbol, bar_time),
                    close=self._positive_float(raw.get("close"), "close", symbol, bar_time),
                    volume=self._volume_hands_to_shares(raw.get("volume"), symbol, bar_time),
                    amount=self._optional_non_negative_float(raw.get("amount"), "amount", symbol, bar_time),
                    is_suspended=(
                        bool(suspend_status.is_suspended)
                        if suspend_status is not None
                        else self._parse_suspend_status(
                            raw,
                            symbol=symbol,
                            bar_time=bar_time,
                            require_suspend_status=require_suspend_status,
                        )
                    ),
                    limit_up=limit_price.up_limit,
                    limit_down=limit_price.down_limit,
                )
            )

        minute_bars.sort(key=lambda item: item.bar_time)
        for prev, cur in zip(minute_bars, minute_bars[1:]):
            if cur.bar_time <= prev.bar_time:
                raise DataUnavailableError(
                    "minute bars must be strictly increasing",
                    context={
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "bar_time": cur.bar_time.isoformat(),
                        "source": source.value,
                    },
                )
        return minute_bars

    def _build_market_context(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        minute_bars: list[MinuteBar],
        limit_price: DailyLimitPrice,
        pre_close_source: str,
        limit_price_source: str,
        suspend_status: DailySuspendStatus | None = None,
        day_features: V25DayFeatures | None = None,
    ) -> dict[str, Any]:
        # The V24 implementation currently consumes these legacy "full_day_*"
        # names. In realtime TDX mode they mean "observed bars so far"; callers
        # can enforce min_bars=31 before invoking V24.
        context = {
            "stock_id": symbol,
            "trade_date": trade_date.isoformat(),
            "data_source": source.value,
            "price_basis": "raw",
            "limit_price_basis": "raw",
            "prev_close_basis": "raw",
            "generated_at": datetime.now(UTC).isoformat(),
            "observed_bar_count": len(minute_bars),
            "observed_only": source == MinuteDataSource.TDX_REALTIME,
            "prev_close": limit_price.pre_close,
            "prev_close_source": pre_close_source,
            "limit_up": limit_price.up_limit,
            "limit_down": limit_price.down_limit,
            "limit_price_source": limit_price_source,
            "suspend_status": (
                None
                if suspend_status is None
                else {
                    "is_suspended": suspend_status.is_suspended,
                    "suspend_type": suspend_status.suspend_type,
                    "suspend_timing": suspend_status.suspend_timing,
                    "source": suspend_status.source,
                }
            ),
            "full_day_open": [bar.open for bar in minute_bars],
            "full_day_close": [bar.close for bar in minute_bars],
            "full_day_volume": [bar.volume for bar in minute_bars],
            "full_day_high": [bar.high for bar in minute_bars],
            "full_day_low": [bar.low for bar in minute_bars],
        }
        if day_features is not None:
            context.update(day_features.market_context_payload())
        return context

    def _limit_price_with_required_pre_close(
        self,
        symbol: str,
        trade_date: date,
        *,
        source: MinuteDataSource,
    ) -> tuple[DailyLimitPrice, str, str]:
        if source == MinuteDataSource.TDX_REALTIME:
            return self._derived_realtime_limit_price_from_previous_close(symbol, trade_date)
        return self._stk_limit_price_with_required_pre_close(symbol, trade_date)

    def _stk_limit_price_with_required_pre_close(self, symbol: str, trade_date: date) -> tuple[DailyLimitPrice, str, str]:
        limit_price = self.limit_price_provider.get_limit_price(symbol, trade_date)
        if limit_price.pre_close is not None and float(limit_price.pre_close) > 0:
            return limit_price, "market.stk_limit.pre_close", "market.stk_limit.limit_price"
        previous_close = self._required_previous_close(
            symbol,
            trade_date,
            requested_source="market.stk_limit.pre_close",
        )
        return (
            DailyLimitPrice(
                symbol=limit_price.symbol,
                trade_date=limit_price.trade_date,
                pre_close=previous_close.pre_close,
                up_limit=limit_price.up_limit,
                down_limit=limit_price.down_limit,
            ),
            previous_close.source,
            "market.stk_limit.limit_price",
        )

    def _derived_realtime_limit_price_from_previous_close(
        self,
        symbol: str,
        trade_date: date,
    ) -> tuple[DailyLimitPrice, str, str]:
        previous_close = self._required_previous_close(
            symbol,
            trade_date,
            requested_source="TDX_REALTIME.previous_close",
        )
        st_status = self._required_st_status(symbol, trade_date, context_source="TDX_REALTIME.derived_limit_price")
        limit_pct = _a_share_daily_limit_pct(symbol, st_status=st_status)
        up_limit = self._round_price_tick(previous_close.pre_close * (1.0 + limit_pct))
        down_limit = self._round_price_tick(previous_close.pre_close * (1.0 - limit_pct))
        if down_limit >= up_limit:
            raise DataUnavailableError(
                "derived realtime limit price range is invalid",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "pre_close": previous_close.pre_close,
                    "limit_pct": limit_pct,
                    "up_limit": up_limit,
                    "down_limit": down_limit,
                    "source": previous_close.source,
                    "st_status_source": st_status.source,
                    "is_st": st_status.is_st,
                },
            )
        return (
            DailyLimitPrice(
                symbol=symbol,
                trade_date=trade_date,
                pre_close=previous_close.pre_close,
                up_limit=up_limit,
                down_limit=down_limit,
            ),
            previous_close.source,
            (
                f"derived_from_previous_close.{previous_close.source}."
                f"a_share_board_limit_pct_{limit_pct:.2f}.{st_status.source}"
            ),
        )

    def _required_previous_close(self, symbol: str, trade_date: date, *, requested_source: str) -> PreviousClose:
        if self.previous_close_provider is None:
            raise DataUnavailableError(
                "pre_close is required for minute execution context",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": requested_source},
            )
        previous_close = self.previous_close_provider.get_previous_close(symbol, trade_date)
        if previous_close.pre_close <= 0:
            raise DataUnavailableError(
                "previous close provider returned invalid pre_close",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "previous_trade_date": previous_close.previous_trade_date.isoformat(),
                    "pre_close": previous_close.pre_close,
                    "source": previous_close.source,
                },
            )
        return previous_close

    def _required_st_status(self, symbol: str, trade_date: date, *, context_source: str) -> DailyStStatus:
        return _require_st_status(
            self.st_status_provider,
            symbol=symbol,
            trade_date=trade_date,
            context_source=context_source,
        )

    @staticmethod
    def _round_price_tick(value: float) -> float:
        return float(Decimal(str(value)).quantize(PRICE_TICK, rounding=ROUND_HALF_UP))

    def _load_day_features(self, *, symbol: str, trade_date: date, required: bool) -> V25DayFeatures | None:
        if not required:
            return None
        if self.day_feature_provider is None:
            raise DataUnavailableError(
                "V25 day_features provider is required",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            )
        return self.day_feature_provider.load_day_features(symbol=symbol, trade_date=trade_date)

    @staticmethod
    def _positive_price_from_li(value: Any, column: str, symbol: str, trade_date: date) -> float:
        try:
            parsed = float(value) / PRICE_UNIT_DIVISOR
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                f"invalid {column} in market.kline_minute_raw",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "value": value},
            ) from exc
        if parsed <= 0:
            raise DataUnavailableError(
                f"invalid {column} in market.kline_minute_raw",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "value": value},
            )
        return parsed

    @staticmethod
    def _positive_float(value: Any, field: str, symbol: str, bar_time: datetime) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                f"minute bar {field} is invalid",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            ) from exc
        if parsed <= 0:
            raise DataUnavailableError(
                f"minute bar {field} must be positive",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            )
        return parsed

    @staticmethod
    def _optional_non_negative_float(
        value: Any,
        field: str,
        symbol: str,
        bar_time: datetime,
    ) -> float | None:
        if value is None:
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                f"minute bar {field} is invalid",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            ) from exc
        if parsed < 0:
            raise DataUnavailableError(
                f"minute bar {field} must be non-negative",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            )
        return parsed

    @staticmethod
    def _volume_hands_to_shares(value: Any, symbol: str, bar_time: datetime) -> int:
        try:
            volume_hand = float(value)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                "minute bar volume is invalid",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            ) from exc
        if volume_hand < 0:
            raise DataUnavailableError(
                "minute bar volume must be non-negative",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            )
        return int(round(volume_hand * MINUTE_VOLUME_HAND_SIZE))

    @staticmethod
    def _parse_suspend_status(
        raw: dict[str, Any],
        *,
        symbol: str,
        bar_time: datetime,
        require_suspend_status: bool,
    ) -> bool:
        for key in ("is_suspended", "suspended", "suspend_status"):
            if key in raw:
                value = raw[key]
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return bool(value)
                normalized = str(value).strip().lower()
                if normalized in {"1", "true", "yes", "y", "suspended"}:
                    return True
                if normalized in {"0", "false", "no", "n", "active", "trading"}:
                    return False
                raise DataUnavailableError(
                    "minute bar suspend status is invalid",
                    context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
                )
        if require_suspend_status:
            raise DataUnavailableError(
                "minute bar suspend status is required",
                context={"symbol": symbol, "bar_time": bar_time.isoformat()},
            )
        return False

    @staticmethod
    def _naive_for_compare(value: datetime) -> datetime:
        return value.replace(tzinfo=None) if value.tzinfo is not None else value


class TradeCalendarProvider:
    """Read-only trading calendar validator for authoritative day runs."""

    def __init__(self, conn_factory: ConnFactory | None = None, calendar_service: TradingCalendarStatusService | Any | None = None) -> None:
        self.conn_factory = conn_factory or get_conn
        self.calendar_service = calendar_service or TradingCalendarStatusService(conn_factory=self.conn_factory)

    def ensure_trading_day(self, trade_date: date) -> None:
        self.calendar_service.ensure_trading_day(trade_date)

    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        return self.calendar_service.list_trading_days(start_date, end_date)
