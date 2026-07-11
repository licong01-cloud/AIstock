from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any, Mapping

from backend.services.simulation_runtime.scheduler import SimulationLifecycleBackgroundScheduler


def test_eod_observation_hook_failure_isolated_from_post_close_scheduler_outcome() -> None:
    lifecycle = _Lifecycle()
    scheduler = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=_Calendar(),
        tca_eod_observation_hook=_RaisingObservationHook(),
    )

    result = scheduler.run_once(as_of_time=datetime(2026, 7, 10, 7, 5, tzinfo=UTC))

    assert lifecycle.post_close_calls == 1
    assert result["reason"] == "eod_reconcile"
    assert result["terminalized_runs"] == [
        {"run_id": "run-1", "post_close_terminalization": True, "status": "SUCCEEDED"}
    ]
    assert result["summary"]["stale_terminalized_count"] == 1
    assert result["tca_eod_observation"] == [
        {
            "status": "FAILED",
            "reason_code": "ADAPTIVE_IS_TCA_EOD_HOOK_EXCEPTION",
            "stage": "TCA_EOD_SCHEDULER_SEAM",
            "error_type": "RuntimeError",
        }
    ]
    assert result["tca_eod_observation_metrics"][0]["reason_code"] == "ADAPTIVE_IS_TCA_EOD_HOOK_EXCEPTION"
    assert result["tca_eod_observation_metrics"][0]["stage"] == "TCA_EOD_SCHEDULER_SEAM"
    assert result["alerts"][0]["alert_type"] == "MINIQMT_TCA_OBSERVATION_FAILURE"
    assert result["alerts"][0]["execution_gate"] is False


class _Calendar:
    def status(self, *, as_of_date: date) -> dict[str, Any]:
        return {"as_of_date": as_of_date.isoformat(), "is_trading_day": True, "next_trading_day": None}


class _Lifecycle:
    def __init__(self) -> None:
        self.post_close_calls = 0

    def status(self) -> dict[str, Any]:
        return {}

    def post_close_reconcile_once(self, **_: Any) -> Any:
        self.post_close_calls += 1
        return SimpleNamespace(
            results=(),
            stale_run_results=({"run_id": "run-1", "post_close_terminalization": True, "status": "SUCCEEDED"},),
            total_bindings=0,
            planned_count=0,
            reused_count=0,
            submitted_count=0,
            failed_count=0,
            stale_terminalized_count=1,
        )


class _RaisingObservationHook:
    def observe_post_reconciliation(self, **_: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
        raise RuntimeError("observation hook failed")
