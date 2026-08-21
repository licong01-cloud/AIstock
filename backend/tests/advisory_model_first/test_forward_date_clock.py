from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

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
    def retryable_model_observations(self, *, limit: int):
        assert limit == 1
        return []

    def pending_settlements(self, *, on_or_before: date):
        return []

    def list_runs(self, **_kwargs):
        return []


class _Programs:
    def list_programs(self, *, include_archived: bool):
        assert not include_archived
        return []


class _PendingRepository(_Repository):
    def pending_settlements(self, *, on_or_before: date):
        assert on_or_before == date(2026, 8, 14)
        return [
            {
                "forward_run_id": "run-a-1",
                "program_id": "program-a",
                "target_trade_date": date(2026, 8, 12),
            },
            {
                "forward_run_id": "run-b-1",
                "program_id": "program-b",
                "target_trade_date": date(2026, 8, 12),
            },
            {
                "forward_run_id": "run-a-2",
                "program_id": "program-a",
                "target_trade_date": date(2026, 8, 13),
            },
        ]


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


def test_later_settlement_is_skipped_after_same_program_blocks() -> None:
    now = datetime(2026, 8, 14, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    service = AdvisoryForwardService(
        repository=_PendingRepository(),
        program_service=_Programs(),
        model_service=SimpleNamespace(),
        calendar=_Calendar(),
        now_provider=lambda: now,
    )
    settled: list[str] = []

    def settle(pending):
        settled.append(pending["forward_run_id"])
        if pending["forward_run_id"] == "run-a-1":
            return {"program_id": "program-a", "status": "WAITING_DATA"}
        return {"program_id": pending["program_id"], "status": "SETTLED"}

    service._settle = settle

    result = service.run_once()

    assert settled == ["run-a-1", "run-b-1"]
    assert result["publication_due"] is False
    assert result["results"][-1] == {
        "program_id": "program-a",
        "forward_run_id": "run-a-2",
        "status": "SKIPPED_PREVIOUS_SETTLEMENT_PENDING",
        "stage": "TARGET_OPEN_SETTLE",
        "reason_code": "ADVISORY_FORWARD_PREVIOUS_SETTLEMENT_PENDING",
        "target_trade_date": "2026-08-13",
    }


def test_after_close_environment_controls_publication_cutoff(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_ADVISORY_FORWARD_AFTER_CLOSE_TIME", "17:15:00")
    now = datetime(2026, 8, 14, 17, 14, tzinfo=ZoneInfo("Asia/Shanghai"))
    service = AdvisoryForwardService(
        repository=_Repository(),
        program_service=_Programs(),
        model_service=SimpleNamespace(),
        calendar=_Calendar(),
        now_provider=lambda: now,
    )

    assert service.status()["after_close_time"] == "17:15:00"
    assert service.run_once()["publication_due"] is False


@pytest.mark.parametrize("raw", ("invalid", "24:00", "16:60", "16:30:01"))
def test_invalid_after_close_environment_fails_visibly(monkeypatch, raw) -> None:
    monkeypatch.setenv("AISTOCK_ADVISORY_FORWARD_AFTER_CLOSE_TIME", raw)

    with pytest.raises(ValueError, match="AFTER_CLOSE_TIME|clock range|seconds"):
        AdvisoryForwardService(
            repository=_Repository(),
            program_service=_Programs(),
            model_service=SimpleNamespace(),
            calendar=_Calendar(),
        )


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
