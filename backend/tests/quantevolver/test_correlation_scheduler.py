from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

from backend.services.quantevolver import correlation_scheduler as scheduler_module
from backend.services.quantevolver.correlation_scheduler import CorrelationScheduler


def test_submit_uses_schema_valid_job_type_and_keeps_dataset_identity(monkeypatch) -> None:
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    scheduler = CorrelationScheduler()
    dispatch = MagicMock()
    dispatch.create_and_submit_task = AsyncMock(
        return_value={"task_id": "dispatch-rejected", "status": "failed"}
    )
    scheduler._dispatch_service = dispatch

    monkeypatch.setattr(scheduler_module, "get_conn", lambda: connection)
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
