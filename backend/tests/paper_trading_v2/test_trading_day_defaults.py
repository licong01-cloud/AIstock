from __future__ import annotations

from datetime import date

import pytest

from backend.routers import paper_trading_v2


class _DefaultsCursor:
    def __init__(self, *, audit_dates: dict[str, date | None], minute_raw_date: date | None = None) -> None:
        self.audit_dates = audit_dates
        self.minute_raw_date = minute_raw_date
        self._row: tuple[date | None] = (None,)

    def execute(self, sql: str, params: tuple) -> None:
        if "FROM market.dataset_date_refresh_audit" in sql:
            self._row = (self.audit_dates.get(str(params[0])),)
            return
        if "FROM market.kline_minute_raw" in sql:
            self._row = (self.minute_raw_date,)
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> tuple[date | None]:
        return self._row


def test_trading_day_defaults_uses_minute_bar_readiness_not_daily_only() -> None:
    ready_date, dataset_dates, missing = paper_trading_v2._resolve_defaults_data_ready_end(
        _DefaultsCursor(
            audit_dates={
                "kline_minute_raw": date(2026, 6, 9),
                "stk_limit": date(2026, 6, 10),
                "suspend_d": date(2026, 6, 10),
            },
        ),
        as_of=date(2026, 6, 10),
        require_minute_data=True,
    )

    assert ready_date == date(2026, 6, 9)
    assert dataset_dates["kline_minute_raw"] == date(2026, 6, 9)
    assert missing == []


def test_trading_day_defaults_falls_back_to_raw_minute_max_when_audit_missing() -> None:
    ready_date, dataset_dates, missing = paper_trading_v2._resolve_defaults_data_ready_end(
        _DefaultsCursor(
            audit_dates={
                "kline_minute_raw": None,
                "stk_limit": date(2026, 6, 9),
                "suspend_d": date(2026, 6, 9),
            },
            minute_raw_date=date(2026, 6, 8),
        ),
        as_of=date(2026, 6, 10),
        require_minute_data=True,
    )

    assert ready_date == date(2026, 6, 8)
    assert dataset_dates["kline_minute_raw"] == date(2026, 6, 8)
    assert missing == []


def test_trading_day_defaults_reports_missing_required_dataset() -> None:
    ready_date, dataset_dates, missing = paper_trading_v2._resolve_defaults_data_ready_end(
        _DefaultsCursor(
            audit_dates={
                "kline_minute_raw": date(2026, 6, 9),
                "stk_limit": date(2026, 6, 9),
                "suspend_d": None,
            },
        ),
        as_of=date(2026, 6, 10),
        require_minute_data=True,
    )

    assert ready_date is None
    assert dataset_dates["kline_minute_raw"] == date(2026, 6, 9)
    assert missing == ["suspend_d"]


def test_trading_day_defaults_skips_data_gate_when_not_required() -> None:
    ready_date, dataset_dates, missing = paper_trading_v2._resolve_defaults_data_ready_end(
        pytest.fail,  # type: ignore[arg-type]
        as_of=date(2026, 6, 10),
        require_minute_data=False,
    )

    assert ready_date is None
    assert dataset_dates == {}
    assert missing == []
