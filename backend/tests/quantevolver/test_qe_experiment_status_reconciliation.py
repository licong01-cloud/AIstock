from __future__ import annotations

import asyncio
import json
from typing import Any

from backend.routers import quantevolver
from backend.services.quantevolver import qe_workspace_client


class _Cursor:
    description = [
        ("status",),
        ("qe_task_id",),
        ("qe_loop_id",),
        ("result_metrics",),
        ("alpha_mode",),
        ("custom_params",),
    ]

    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[Any, ...] | None]] = []

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[Any, ...] | None = None) -> None:
        self.statements.append((statement, params))

    def fetchone(self) -> tuple[Any, ...]:
        return ("running", "task-1", "Loop1", None, "single", {})


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.commits = 0

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self, **_kwargs: object) -> _Cursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1


class _WorkspaceClient:
    async def __aenter__(self) -> _WorkspaceClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def get_loop_status(self, _task_id: str, _loop_id: str) -> dict[str, str]:
        return {"status": "completed"}

    async def get_loop_metrics(self, _task_id: str, _loop_id: str) -> dict[str, Any]:
        raise RuntimeError("Qlib backtest artifact is absent")


def test_completed_remote_loop_with_missing_qlib_result_is_failed(
    monkeypatch,
) -> None:
    cursor = _Cursor()
    connection = _Connection(cursor)
    monkeypatch.setattr(quantevolver, "get_conn", lambda: connection)
    monkeypatch.setattr(qe_workspace_client, "QEWorkspaceClient", _WorkspaceClient)

    result = asyncio.run(quantevolver.reconcile_experiment_run_status("experiment-1"))

    assert result["status"] == "failed"
    assert result["artifact_status"] == "failed"
    update = next(
        (statement, params)
        for statement, params in cursor.statements
        if "UPDATE qe_experiments" in statement
    )
    assert "SET status = 'failed'" in update[0]
    lifecycle = json.loads(update[1][0])
    assert lifecycle["qe_completion_lifecycle"]["runtime_status"] == "completed"
    assert lifecycle["qe_completion_lifecycle"]["experiment_status"] == "failed"
    assert connection.commits == 1


def test_multi_alpha_artifact_failure_is_not_persisted_as_completed(monkeypatch) -> None:
    cursor = _Cursor()
    connection = _Connection(cursor)
    monkeypatch.setattr(quantevolver, "get_conn", lambda: connection)

    quantevolver._mark_multi_alpha_artifact_failure("experiment-2", "collector failed")

    statement, params = cursor.statements[-1]
    assert "SET status = 'failed'" in statement
    lifecycle = json.loads(params[0])
    assert lifecycle["multi_alpha_lifecycle"]["runtime_status"] == "completed"
    assert lifecycle["multi_alpha_lifecycle"]["experiment_status"] == "failed"

