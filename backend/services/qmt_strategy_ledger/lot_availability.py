"""Canonical T+1 sell-availability helpers for MiniQMT strategy lots."""

from __future__ import annotations

from bisect import bisect_right
from datetime import date
from typing import Any, Protocol, Sequence

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError


class TradingCalendarProvider(Protocol):
    def is_trading_day(self, trade_date: date) -> bool:
        ...

    def next_trading_day_after(self, trade_date: date) -> date:
        ...


class DbTradingCalendarProvider:
    """Read A-share trading days from ``market.trading_calendar``."""

    def __init__(self, conn_factory: Any | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def is_trading_day(self, trade_date: date) -> bool:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT is_trading
                    FROM market.trading_calendar
                    WHERE cal_date = %s
                    """,
                    (trade_date,),
                )
                row = cur.fetchone()
        if row is None:
            raise DataUnavailableError(
                "trading calendar row is required for MiniQMT lot availability",
                context={"trade_date": trade_date.isoformat()},
            )
        return bool(row[0])

    def next_trading_day_after(self, trade_date: date) -> date:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MIN(cal_date)
                    FROM market.trading_calendar
                    WHERE cal_date > %s AND is_trading = TRUE
                    """,
                    (trade_date,),
                )
                row = cur.fetchone()
        next_day = row[0] if row else None
        if next_day is None:
            raise DataUnavailableError(
                "next trading day is missing for MiniQMT lot availability",
                context={"open_date": trade_date.isoformat()},
            )
        return next_day


class StaticTradingCalendarProvider:
    """Small deterministic calendar for unit tests and non-DB validation."""

    def __init__(self, trading_days: Sequence[date]) -> None:
        self._days = sorted(set(trading_days))
        if not self._days:
            raise ValueError("trading_days is required")

    def is_trading_day(self, trade_date: date) -> bool:
        return trade_date in self._days

    def next_trading_day_after(self, trade_date: date) -> date:
        idx = bisect_right(self._days, trade_date)
        if idx >= len(self._days):
            raise DataUnavailableError(
                "next trading day is missing for MiniQMT lot availability",
                context={"open_date": trade_date.isoformat()},
            )
        return self._days[idx]


def tplus1_unlocked(open_date: date, as_of_date: date, calendar: TradingCalendarProvider) -> bool:
    """Return True only on/after the next valid trading day after ``open_date``."""

    if not calendar.is_trading_day(as_of_date):
        return False
    if as_of_date <= open_date:
        return False
    return as_of_date >= calendar.next_trading_day_after(open_date)


def effective_lot_available_quantity(lot: Any, as_of_date: date, calendar: TradingCalendarProvider) -> int:
    """Derive gross strategy sellable shares before pending-sell reservation."""

    remaining_quantity = max(int(lot.remaining_quantity), 0)
    stored_available = min(max(int(lot.available_quantity), 0), remaining_quantity)
    if remaining_quantity <= 0:
        return 0
    if tplus1_unlocked(lot.open_date, as_of_date, calendar):
        return max(stored_available, remaining_quantity)
    return stored_available


def pending_sell_quantity(intents: Sequence[Any]) -> int:
    return sum(max(int(intent.quantity), 0) for intent in intents)


def effective_strategy_available_sell_quantity(
    *,
    lots: Sequence[Any],
    pending_sell_intents: Sequence[Any],
    as_of_date: date,
    calendar: TradingCalendarProvider,
) -> int:
    gross_available = sum(effective_lot_available_quantity(lot, as_of_date, calendar) for lot in lots)
    return max(0, gross_available - pending_sell_quantity(pending_sell_intents))
