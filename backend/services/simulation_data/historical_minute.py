"""Completed-day historical minute reader with a strict date boundary."""

from __future__ import annotations

from datetime import date, datetime
import math
from typing import Any, Callable, Iterator, Mapping

from backend.db.pg_pool import get_conn
from backend.services.simulation_data.contracts import (
    HistoricalMinuteBatch,
    MINUTE_VOLUME_HAND_SIZE,
    MinuteDataSource,
    PRICE_UNIT_DIVISOR,
    _canonical_json_sha256,
)
from backend.services.simulation_data.daily_context import DailyTradingSymbolFactV1, DailyTradingSymbolFactV2
from backend.services.simulation_data.frozen_daily_fact import parse_frozen_daily_symbol_fact
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import MinuteBar


ConnFactory = Callable[[], Iterator[Any]]


class HistoricalMinuteProvider:
    """Read DB history only when the requested day is already complete."""

    def __init__(self, *, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def load_completed_day(
        self,
        *,
        symbol: str,
        trade_date: date,
        current_trading_date: date,
        frozen_daily_fact: Mapping[str, Any],
    ) -> HistoricalMinuteBatch:
        normalized = str(symbol or "").strip()
        if not normalized or trade_date >= current_trading_date:
            raise DataUnavailableError(
                "historical minute source is forbidden for the current or future trading day",
                context={
                    "symbol": normalized,
                    "trade_date": trade_date.isoformat(),
                    "current_trading_date": current_trading_date.isoformat(),
                },
            )
        fact = parse_frozen_daily_symbol_fact(normalized, trade_date, frozen_daily_fact)
        rows = self._read_rows(normalized, trade_date)
        bars = tuple(self._bar(normalized, trade_date, row, fact) for row in rows)
        payload = {
            "symbol": normalized,
            "trade_date": trade_date.isoformat(),
            "current_trading_date": current_trading_date.isoformat(),
            "source": MinuteDataSource.DB_HISTORICAL.value,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        return HistoricalMinuteBatch(
            symbol=normalized,
            trade_date=trade_date,
            current_trading_date=current_trading_date,
            bars=bars,
            batch_hash=_canonical_json_sha256(payload),
        )

    def _read_rows(self, symbol: str, trade_date: date) -> list[tuple[Any, ...]]:
        with self._conn_factory() as conn:
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
                rows = list(cur.fetchall())
        if not rows:
            raise DataUnavailableError(
                "historical DB returned no minute bars",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            )
        return rows

    @staticmethod
    def _bar(
        symbol: str,
        trade_date: date,
        row: tuple[Any, ...],
        fact: DailyTradingSymbolFactV1 | DailyTradingSymbolFactV2,
    ) -> MinuteBar:
        bar_time = row[0]
        if not isinstance(bar_time, datetime) or bar_time.date() != trade_date:
            raise DataUnavailableError("historical minute timestamp is invalid", context={"symbol": symbol})
        prices = [_raw_price(value, symbol=symbol) for value in row[1:5]]
        volume = _nonnegative(row[5], symbol=symbol, field="volume_hand")
        amount = None if row[6] is None else _nonnegative(row[6], symbol=symbol, field="amount_li") / PRICE_UNIT_DIVISOR
        return MinuteBar(
            symbol=symbol,
            bar_time=bar_time,
            open=prices[0],
            high=prices[1],
            low=prices[2],
            close=prices[3],
            volume=int(round(volume * MINUTE_VOLUME_HAND_SIZE)),
            amount=amount,
            is_suspended=fact.is_suspended,
            limit_up=fact.up_limit,
            limit_down=fact.down_limit,
        )


def _raw_price(value: Any, *, symbol: str) -> float:
    number = _nonnegative(value, symbol=symbol, field="price_li") / PRICE_UNIT_DIVISOR
    if number <= 0:
        raise DataUnavailableError("historical minute price must be positive", context={"symbol": symbol})
    return number


def _nonnegative(value: Any, *, symbol: str, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DataUnavailableError(
            "historical minute numeric field is invalid", context={"symbol": symbol, "field": field}
        ) from exc
    if not math.isfinite(number) or number < 0:
        raise DataUnavailableError(
            "historical minute numeric field must be non-negative", context={"symbol": symbol, "field": field}
        )
    return number
