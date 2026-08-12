from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from backend.services.advisory_forward.service import AdvisoryForwardService
from backend.services.advisory_forward.service import _maturity_date


class _Calendar:
    def is_trading_day(self, value: date) -> bool:
        return value == date(2026, 8, 14)

    def next_trading_day(self, value: date, *, inclusive: bool = False) -> date:
        assert not inclusive
        if value == date(2026, 8, 14):
            return date(2026, 8, 17)
        return date.fromordinal(value.toordinal() + 1)


class _Repository:
    def pending_settlements(self, *, on_or_before: date):
        return []

    def list_runs(self, **_kwargs):
        return []


class _Programs:
    def list_programs(self, *, include_archived: bool):
        assert not include_archived
        return []


def test_friday_after_close_targets_next_trading_day_without_weekend_backfill() -> None:
    now = datetime(2026, 8, 14, 16, 31, tzinfo=ZoneInfo("Asia/Shanghai"))
    service = AdvisoryForwardService(
        repository=_Repository(),
        program_service=_Programs(),
        model_service=SimpleNamespace(),
        calendar=_Calendar(),
        now_provider=lambda: now,
    )

    result = service.run_once()

    assert result["publication_due"] is True
    assert result["decision_as_of_trade_date"] == "2026-08-14"
    assert result["target_trade_date"] == "2026-08-17"


def test_weekend_does_not_publish_latest_prior_trading_day() -> None:
    now = datetime(2026, 8, 15, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    service = AdvisoryForwardService(
        repository=_Repository(),
        program_service=_Programs(),
        model_service=SimpleNamespace(),
        calendar=_Calendar(),
        now_provider=lambda: now,
    )

    result = service.run_once()

    assert result["publication_due"] is False
    assert result["decision_as_of_trade_date"] is None


def test_model_maturity_uses_longest_declared_outcome_or_holding_horizon() -> None:
    calendar = SimpleNamespace(
        next_trading_day=lambda value, inclusive=False: date.fromordinal(value.toordinal() + 1)
    )

    maturity = _maturity_date(
        date(2026, 8, 17),
        horizons=[1, 3, 5, 10, 20, 25],
        calendar=calendar,
    )

    assert maturity == date.fromordinal(date(2026, 8, 17).toordinal() + 25)
