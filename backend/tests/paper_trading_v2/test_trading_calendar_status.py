from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import DataUnavailableError


def _calendar_rows(start: date, end: date) -> list[dict]:
    rows: list[dict] = []
    current = start
    while current <= end:
        rows.append({"cal_date": current, "is_trading": current.weekday() < 5})
        current += timedelta(days=1)
    return rows


class _FakeCursor:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.execute_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql) -> None:
        self.execute_count += 1

    def fetchall(self):
        return list(self.rows)


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self.cursor_obj = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


def test_trading_calendar_status_refreshes_cache_once_then_reads_file(tmp_path) -> None:
    cursor = _FakeCursor(_calendar_rows(date(2026, 5, 1), date(2026, 6, 30)))
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    first = service.status(as_of_date=date(2026, 5, 25))
    second = service.status(as_of_date=date(2026, 5, 26))
    days = service.list_trading_days(date(2026, 5, 25), date(2026, 5, 29))

    assert first["is_trading_day"] is True
    assert second["is_trading_day"] is True
    assert days == [date(2026, 5, 25), date(2026, 5, 26), date(2026, 5, 27), date(2026, 5, 28), date(2026, 5, 29)]
    assert cursor.execute_count == 1
    assert (tmp_path / "calendar_cache.json").exists()


def test_trading_calendar_status_warns_when_next_month_not_fully_covered(tmp_path) -> None:
    cursor = _FakeCursor(_calendar_rows(date(2026, 5, 1), date(2026, 6, 15)))
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    status = service.status(as_of_date=date(2026, 5, 25))

    assert status["warnings"][0]["code"] == "TRADING_CALENDAR_NEXT_MONTH_INCOMPLETE"
    assert status["warnings"][0]["required_end"] == "2026-06-30"


def test_trading_calendar_status_missing_current_row_fails_fast(tmp_path) -> None:
    cursor = _FakeCursor(_calendar_rows(date(2026, 5, 1), date(2026, 5, 24)))
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    with pytest.raises(DataUnavailableError, match="current date"):
        service.status(as_of_date=date(2026, 5, 25))


def test_trading_calendar_status_ensure_trading_day_rejects_non_trading_day(tmp_path) -> None:
    cursor = _FakeCursor(_calendar_rows(date(2026, 5, 1), date(2026, 6, 30)))
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    with pytest.raises(DataUnavailableError, match="not a trading day"):
        service.ensure_trading_day(date(2026, 5, 30))
