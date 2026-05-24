from __future__ import annotations

from datetime import datetime

from backend.schedulers.strategy_scheduler import StrategyScheduler


class _CalendarStub:
    def __init__(self, *, is_trading_day: bool) -> None:
        self.is_trading_day = is_trading_day
        self.checked_dates = []

    def status(self, *, as_of_date):
        self.checked_dates.append(as_of_date)
        return {"is_trading_day": self.is_trading_day}


def test_strategy_scheduler_uses_official_calendar_not_weekday() -> None:
    scheduler = StrategyScheduler.__new__(StrategyScheduler)
    calendar = _CalendarStub(is_trading_day=True)
    scheduler._trading_calendar_status = calendar

    assert scheduler._is_trading_time(datetime(2026, 5, 23, 10, 0)) is True
    assert calendar.checked_dates == [datetime(2026, 5, 23).date()]


def test_strategy_scheduler_blocks_when_official_calendar_says_non_trading() -> None:
    scheduler = StrategyScheduler.__new__(StrategyScheduler)
    scheduler._trading_calendar_status = _CalendarStub(is_trading_day=False)

    assert scheduler._is_trading_time(datetime(2026, 5, 25, 10, 0)) is False
