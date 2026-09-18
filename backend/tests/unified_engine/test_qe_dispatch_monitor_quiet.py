from __future__ import annotations

import inspect
import json
from unittest.mock import AsyncMock, MagicMock

from backend.routers import quantevolver_evolution
from backend.services.quantevolver import correlation_scheduler as correlation_scheduler_module
from backend.services.quantevolver.correlation_scheduler import CorrelationScheduler
from backend.services.quantevolver.factor_metrics_scheduler import FactorMetricsScheduler


class _ObservationService:
    def __init__(self, *, running_task: dict, terminal_task: dict) -> None:
        self.running_task = running_task
        self.terminal_task = terminal_task
        self.wait_calls = 0
        self.get_task_calls = 0

    def wait_for_task_observation(self, _task_id, *, after_generation, timeout):
        assert timeout >= 60
        self.wait_calls += 1
        if self.wait_calls <= 100:
            return after_generation + 1, dict(self.running_task)
        return after_generation + 1, dict(self.terminal_task)

    def get_task(self, _task_id):
        self.get_task_calls += 1
        raise AssertionError("observation-backed monitor must not poll PostgreSQL")

    async def get_task_results(self, _task_id):
        return {"latest_result": {"success": True}}


def test_correlation_submit_uses_schema_valid_job_type_and_keeps_dataset_identity(monkeypatch) -> None:
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    scheduler = CorrelationScheduler()
    dispatch = MagicMock()
    dispatch.create_and_submit_task = AsyncMock(
        return_value={"task_id": "dispatch-rejected", "status": "failed"}
    )
    scheduler._dispatch_service = dispatch

    monkeypatch.setattr(correlation_scheduler_module, "get_conn", lambda: connection)
    monkeypatch.setattr(scheduler, "_resolve_factor_names", lambda *_args: ["factor_a"])
    monkeypatch.setattr(scheduler, "_update_job_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scheduler, "_update_schedule_status", lambda *_args, **_kwargs: None)

    scheduler.submit_job(
        schedule_id=None,
        dataset="correlation_full",
        options={"factor_names": ["factor_a"], "node_id": "node-1"},
    )

    connection.commit.assert_called_once_with()
    cursor.execute.assert_called_once()
    insert_sql, insert_params = cursor.execute.call_args.args
    assert "INSERT INTO market.ingestion_jobs" in insert_sql
    assert insert_params[1] == "init"
    summary = json.loads(insert_params[2])
    assert summary["job_type"] == "init"
    assert summary["dataset"] == "correlation_full"
    dispatch_request = dispatch.create_and_submit_task.await_args.args[0]
    assert dispatch_request["task_type"] == "correlation_compute"
    assert dispatch_request["payload"]["factor_names"] == ["factor_a"]


def test_correlation_monitor_100_unchanged_wakes_write_no_business_state(monkeypatch) -> None:
    scheduler = CorrelationScheduler()
    running = {
        "task_id": "dispatch-1",
        "status": "running",
        "progress_pct": 0,
        "remote_task_id": "remote-1",
        "node_id": "node-1",
        "log_tail": None,
    }
    service = _ObservationService(
        running_task=running,
        terminal_task={**running, "status": "success", "progress_pct": 100},
    )
    scheduler._dispatch_service = service
    updates: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(
        scheduler,
        "_update_job_status",
        lambda job_id, status, summary: updates.append((job_id, status, summary)),
    )
    monkeypatch.setattr(scheduler, "_update_schedule_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scheduler,
        "_build_terminal_summary",
        lambda *_args, **_kwargs: {"dispatch_status": "success", "progress": 100},
    )
    initial_summary = {
        "dispatch_task_id": "dispatch-1",
        "remote_task_id": "remote-1",
        "node_id": "node-1",
        "dispatch_status": "running",
        "progress": 0,
        "message": None,
        "eligible_factor_count": 5,
        "counters": scheduler._build_counters("running", 0),
    }

    scheduler._monitor_dispatch_task(
        "job-1",
        None,
        "dispatch-1",
        5,
        scheduler._dispatch_summary_fingerprint("running", initial_summary),
    )

    assert service.wait_calls == 101
    assert service.get_task_calls == 0
    assert [(status, summary["dispatch_status"]) for _, status, summary in updates] == [
        ("success", "success")
    ]


def test_factor_metrics_monitor_100_unchanged_wakes_write_no_business_state(monkeypatch) -> None:
    scheduler = FactorMetricsScheduler()
    running = {
        "task_id": "dispatch-2",
        "status": "running",
        "progress_pct": 0,
        "remote_task_id": "remote-2",
        "node_id": "node-1",
        "log_tail": None,
    }
    service = _ObservationService(
        running_task=running,
        terminal_task={**running, "status": "success", "progress_pct": 100},
    )
    scheduler._dispatch_service = service
    updates: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(
        scheduler,
        "_update_job_status",
        lambda job_id, status, summary: updates.append((job_id, status, summary)),
    )
    monkeypatch.setattr(scheduler, "_update_schedule_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scheduler,
        "_build_terminal_summary",
        lambda *_args, **_kwargs: {"dispatch_status": "success", "progress": 100},
    )
    initial_summary = {
        "dispatch_task_id": "dispatch-2",
        "remote_task_id": "remote-2",
        "node_id": "node-1",
        "dispatch_status": "running",
        "progress": 0,
        "message": None,
        "counters": scheduler._build_counters("running", 0),
    }

    scheduler._monitor_dispatch_task(
        "job-2",
        None,
        "dispatch-2",
        False,
        scheduler._dispatch_summary_fingerprint("running", initial_summary),
    )

    assert service.wait_calls == 101
    assert service.get_task_calls == 0
    assert [(status, summary["dispatch_status"]) for _, status, summary in updates] == [
        ("success", "success")
    ]


def test_direct_correlation_path_consumes_observations_not_global_sync() -> None:
    source = inspect.getsource(quantevolver_evolution._run_correlation_compute_via_dispatch)
    assert "get_task_observation" in source
    assert "wait_for_task_observation" in source
    assert "publish_task_observation(created)" not in source
    assert "sync_running_tasks" not in source
    assert ".get_task(" not in source
    assert "sleep(2)" not in source


def test_factor_metrics_invalid_remote_numbers_are_explicit_not_silent(caplog) -> None:
    task = {
        "task_id": "dispatch-invalid",
        "status": "success",
        "progress_pct": "not-a-number",
    }
    summary = FactorMetricsScheduler._build_terminal_summary(
        task,
        {"db_result": {"inserted": {"unexpected": "shape"}}},
        "success",
    )

    assert summary["inserted_rows_error"] == "invalid_inserted_count"
    assert "inserted_rows" not in summary
    assert FactorMetricsScheduler._coerce_progress("not-a-number") == 0
    assert "invalid inserted count" in caplog.text
    assert "invalid progress" in caplog.text
