"""Minute market data input builder for Paper Trading v2.

The provider is read-only. It does not modify ``backend/data_service`` and it
does not silently fall back between data sources. The caller must choose the
source explicitly; missing minute bars, limit prices, or previous close fail
the run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Callable, Iterator

from backend.data_service.tdx_adapter import fetch_minute_kline_tdx
from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.limit_price_provider import (
    DailyLimitPrice,
    StkLimitPriceProvider,
)
from backend.services.trading_core.models import MinuteBar


PRICE_UNIT_DIVISOR = 1000.0
MINUTE_VOLUME_HAND_SIZE = 100

TdxMinuteFetcher = Callable[[str, date], list[dict[str, Any]]]
ConnFactory = Callable[[], Iterator[Any]]


class MinuteDataSource(str, Enum):
    """Supported authoritative minute data sources."""

    TDX_REALTIME = "TDX_REALTIME"
    DB_HISTORICAL = "DB_HISTORICAL"


@dataclass(frozen=True)
class MinuteExecutionMarketInput:
    """Minute bars and execution context for one symbol/trade date."""

    symbol: str
    trade_date: date
    source: MinuteDataSource
    minute_bars: list[MinuteBar]
    market_context: dict[str, Any]


class PaperV2MinuteMarketDataProvider:
    """Build strict minute execution inputs from TDX or historical DB data."""

    def __init__(
        self,
        *,
        limit_price_provider: StkLimitPriceProvider | None = None,
        tdx_fetcher: TdxMinuteFetcher | None = None,
        conn_factory: ConnFactory | None = None,
    ) -> None:
        self.limit_price_provider = limit_price_provider or StkLimitPriceProvider()
        self.tdx_fetcher = tdx_fetcher or fetch_minute_kline_tdx
        self.conn_factory = conn_factory or get_conn

    def load_symbol_input(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource = MinuteDataSource.TDX_REALTIME,
        min_bars: int = 1,
    ) -> MinuteExecutionMarketInput:
        symbol = str(symbol or "").strip()
        if not symbol:
            raise DataUnavailableError("symbol is required for minute market data")
        if min_bars <= 0:
            raise DataUnavailableError(
                "min_bars must be positive",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "min_bars": min_bars},
            )

        limit_price = self.limit_price_provider.get_limit_price(symbol, trade_date)
        if limit_price.pre_close is None or limit_price.pre_close <= 0:
            raise DataUnavailableError(
                "pre_close is required for minute execution context",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            )

        raw_bars = self._load_raw_bars(symbol, trade_date, source)
        minute_bars = self._build_minute_bars(
            symbol=symbol,
            trade_date=trade_date,
            raw_bars=raw_bars,
            limit_price=limit_price,
            source=source,
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
        )
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=minute_bars,
            market_context=context,
        )

    def _load_raw_bars(
        self,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
    ) -> list[dict[str, Any]]:
        if source == MinuteDataSource.TDX_REALTIME:
            try:
                raw_bars = self.tdx_fetcher(symbol, trade_date)
            except Exception as exc:
                raise DataUnavailableError(
                    "TDX minute data fetch failed",
                    context={"symbol": symbol, "trade_date": trade_date.isoformat()},
                ) from exc
            if not raw_bars:
                raise DataUnavailableError(
                    "TDX returned no minute bars",
                    context={"symbol": symbol, "trade_date": trade_date.isoformat()},
                )
            return raw_bars

        if source == MinuteDataSource.DB_HISTORICAL:
            return self._load_raw_bars_from_db(symbol, trade_date)

        raise DataUnavailableError(
            "unsupported minute data source",
            context={"symbol": symbol, "source": str(source)},
        )

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
    ) -> dict[str, Any]:
        # The V24 implementation currently consumes these legacy "full_day_*"
        # names. In realtime TDX mode they mean "observed bars so far"; callers
        # can enforce min_bars=31 before invoking V24.
        return {
            "stock_id": symbol,
            "trade_date": trade_date.isoformat(),
            "data_source": source.value,
            "generated_at": datetime.now(UTC).isoformat(),
            "observed_bar_count": len(minute_bars),
            "observed_only": source == MinuteDataSource.TDX_REALTIME,
            "prev_close": limit_price.pre_close,
            "limit_up": limit_price.up_limit,
            "limit_down": limit_price.down_limit,
            "full_day_close": [bar.close for bar in minute_bars],
            "full_day_volume": [bar.volume for bar in minute_bars],
            "full_day_high": [bar.high for bar in minute_bars],
            "full_day_low": [bar.low for bar in minute_bars],
        }

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
