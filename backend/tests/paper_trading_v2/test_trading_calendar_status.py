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

    def execute(self, sql, params=None) -> None:
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


def test_trading_calendar_status_allows_empty_trading_day_range(tmp_path) -> None:
    cursor = _FakeCursor([
        {"cal_date": date(2026, 5, 23), "is_trading": False},
        {"cal_date": date(2026, 5, 24), "is_trading": False},
    ])
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    assert service.list_trading_days(date(2026, 5, 23), date(2026, 5, 24), allow_empty=True) == []

    with pytest.raises(DataUnavailableError, match="no trading days"):
        service.list_trading_days(date(2026, 5, 23), date(2026, 5, 24))


def test_trading_calendar_status_latest_and_next_helpers(tmp_path) -> None:
    cursor = _FakeCursor([
        {"cal_date": date(2026, 5, 22), "is_trading": True},
        {"cal_date": date(2026, 5, 23), "is_trading": False},
        {"cal_date": date(2026, 5, 24), "is_trading": False},
        {"cal_date": date(2026, 5, 25), "is_trading": True},
    ])
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    assert service.is_trading_day(date(2026, 5, 23)) is False
    assert service.latest_trading_day_on_or_before(date(2026, 5, 24)) == date(2026, 5, 22)
    assert service.next_trading_day(date(2026, 5, 22)) == date(2026, 5, 25)
    assert service.next_trading_day(date(2026, 5, 25), inclusive=True) == date(2026, 5, 25)


def test_trading_calendar_status_rejects_missing_rows_even_when_allow_empty(tmp_path) -> None:
    cursor = _FakeCursor([
        {"cal_date": date(2026, 5, 23), "is_trading": False},
        {"cal_date": date(2026, 5, 25), "is_trading": True},
    ])
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    with pytest.raises(DataUnavailableError, match="rows are missing"):
        service.list_trading_days(date(2026, 5, 23), date(2026, 5, 25), allow_empty=True)


def test_trading_calendar_status_conn_helper_rejects_missing_rows() -> None:
    cursor = _FakeCursor([
        {"cal_date": date(2026, 5, 23), "is_trading": False},
    ])

    with pytest.raises(DataUnavailableError, match="rows are missing"):
        TradingCalendarStatusService.list_trading_days_from_conn(
            _FakeConn(cursor),
            date(2026, 5, 23),
            date(2026, 5, 24),
            allow_empty=True,
        )


def test_trading_calendar_status_latest_helper_rejects_missing_as_of_row(tmp_path) -> None:
    cursor = _FakeCursor([
        {"cal_date": date(2026, 5, 22), "is_trading": True},
        {"cal_date": date(2026, 5, 24), "is_trading": False},
    ])
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    with pytest.raises(DataUnavailableError, match="latest trading-day lookup"):
        service.latest_trading_day_on_or_before(date(2026, 5, 23))


def test_trading_calendar_status_latest_helper_rejects_missing_intermediate_rows(tmp_path) -> None:
    cursor = _FakeCursor([
        {"cal_date": date(2026, 5, 22), "is_trading": True},
        {"cal_date": date(2026, 5, 24), "is_trading": False},
    ])
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    with pytest.raises(DataUnavailableError, match="rows are missing"):
        service.latest_trading_day_on_or_before(date(2026, 5, 24))


def test_trading_calendar_status_next_helper_rejects_missing_intermediate_rows(tmp_path) -> None:
    cursor = _FakeCursor([
        {"cal_date": date(2026, 5, 22), "is_trading": True},
        {"cal_date": date(2026, 5, 25), "is_trading": True},
    ])
    service = TradingCalendarStatusService(
        conn_factory=lambda: _FakeConn(cursor),
        cache_path=tmp_path / "calendar_cache.json",
        now_provider=lambda: datetime(2026, 5, 25, 9, 0, 0),
    )

    with pytest.raises(DataUnavailableError, match="rows are missing"):
        service.next_trading_day(date(2026, 5, 22))
