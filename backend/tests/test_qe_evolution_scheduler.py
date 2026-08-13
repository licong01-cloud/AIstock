from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Iterator

import pytest

from backend.services.quantevolver import experiment_config_builders
from backend.services.quantevolver import qe_evolution_service as evolution_module
from backend.services.quantevolver.executors import backtest as backtest_module
from backend.services.quantevolver.executors.base import ExecutionResult
from backend.services.quantevolver.qe_evolution_service import (
    QE_LOOP_RETRY_MODE_FULL_TRAIN,
    AutoEvolutionScheduler,
)


class _RetryState:
    def __init__(self) -> None:
        self.loop = {
            "loop_id": "task_retry_Loop1",
            "status": "failed",
            "config_json": {
                "model_id": "lgbm",
                "factor_list": ["f1"],
                "runtime_flags": {"random_seed": 7},
            },
            "node_id": "wsl2-5080",
        }
        self.task = {
            "task_id": "task_retry",
            "task_type": "evolution",
            "node_id": "wsl2-5080",
            "status": "failed",
        }


class _Cursor:
    def __init__(self, state: _RetryState) -> None:
        self.state = state
        self.rowcount = 0
        self._result: Any = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        normalized = " ".join(sql.split())
        self.rowcount = 0
        self._result = None
        if normalized.startswith(
            "SELECT loop_id, status, config_json, node_id FROM qe_evolution_loops"
        ):
            self._result = dict(self.state.loop)
            return
        if normalized.startswith("SELECT * FROM qe_evolution_tasks"):
            self._result = dict(self.state.task)
            return
        if (
            normalized.startswith("UPDATE qe_evolution_loops")
            and "SET status = 'running'" in normalized
            and "config_json = %s" in normalized
        ):
            config_json, loop_id, expected_statuses = params
            assert loop_id == self.state.loop["loop_id"]
            if self.state.loop["status"] not in expected_statuses:
                return
            self.state.loop["status"] = "running"
            self.state.loop["config_json"] = json.loads(config_json)
            self.rowcount = 1
            self._result = {"loop_id": loop_id}
            return
        if normalized.startswith("UPDATE qe_evolution_tasks SET status = 'running'"):
            self.state.task["status"] = "running"
            self.rowcount = 1
            return
        if "SET config_json = jsonb_set" in normalized:
            state_value, loop_id, retry_attempt_id = params
            metadata = self.state.loop["config_json"]["_qe_retry_submission"]
            assert loop_id == self.state.loop["loop_id"]
            assert retry_attempt_id == metadata["retry_attempt_id"]
            metadata["state"] = state_value
            self.rowcount = 1
            return
        raise AssertionError(f"unexpected SQL in retry test: {normalized}")

    def fetchone(self) -> Any:
        return self._result


class _Connection:
    def __init__(self, state: _RetryState) -> None:
        self.state = state

    def cursor(self, **_kwargs: Any) -> _Cursor:
        return _Cursor(self.state)

    def commit(self) -> None:
        return None


@contextmanager
def _connection_provider(state: _RetryState, **_kwargs: Any) -> Iterator[_Connection]:
    yield _Connection(state)


class _RetryConfig:
    model_id = "lgbm"
    hmm = None
    extra_params: dict[str, Any] = {}

    @staticmethod
    def build_runtime_flags() -> dict[str, Any]:
        return {"random_seed": 7}


def test_capacity_waiting_retry_reuses_same_attempt_identity_on_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _RetryState()
    captured_source_ids: list[str] = []
    captured_claim_ids: list[str] = []

    async def preflight(_node_id: str) -> None:
        return None

    class FakeExecutor:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def submit(self, _cfg: Any, ctx: Any, **_kwargs: Any) -> ExecutionResult:
            captured_source_ids.append(ctx.submission_source_execution_id)
            captured_claim_ids.append(ctx.submission_source_claim_id)
            if len(captured_source_ids) == 1:
                state.loop["status"] = "pending"
                return ExecutionResult(
                    job_id="Loop1",
                    status="waiting_capacity",
                    detail={"qe_submission": {"state": "waiting_capacity"}},
                )
            return ExecutionResult(
                job_id="Loop1",
                status="submitted",
                wsl_command="python qrun.py conf.yaml",
            )

    monkeypatch.setattr(
        evolution_module,
        "get_conn",
        lambda **kwargs: _connection_provider(state, **kwargs),
    )
    monkeypatch.setattr(evolution_module, "preflight_qe_node", preflight)
    monkeypatch.setattr(evolution_module, "QEResourcePhaseService", SimpleNamespace)
    monkeypatch.setattr(
        experiment_config_builders,
        "build_config_from_retry_loop",
        lambda *_args, **_kwargs: _RetryConfig(),
    )
    monkeypatch.setattr(backtest_module, "BacktestExecutor", FakeExecutor)

    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    scheduler._get_workspace_client_for_node_id = lambda _node_id: object()  # type: ignore[method-assign]
    scheduler._get_callback_url_for_node = lambda _node_id: None  # type: ignore[method-assign]
    scheduler._resolve_gpu_execution_contract = (  # type: ignore[method-assign]
        lambda **_kwargs: ("parallel", False)
    )

    first = asyncio.run(
        scheduler.retry_loop(
            "task_retry",
            1,
            retry_mode=QE_LOOP_RETRY_MODE_FULL_TRAIN,
        )
    )
    second = asyncio.run(
        scheduler.retry_loop(
            "task_retry",
            1,
            _capacity_resume=True,
        )
    )

    assert first["mode"] == "queued_capacity"
    assert second["mode"] == QE_LOOP_RETRY_MODE_FULL_TRAIN
    assert captured_source_ids[0] == captured_source_ids[1]
    assert captured_source_ids[0].startswith("task_retry_Loop1:retry:")
    assert captured_claim_ids == ["task_retry_Loop1", "task_retry_Loop1"]
    metadata = state.loop["config_json"]["_qe_retry_submission"]
    assert metadata["source_execution_id"] == captured_source_ids[0]
    assert metadata["state"] == "submitted"


def test_remote_acceptance_is_not_failed_when_resource_session_sync_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingResourceService:
        @staticmethod
        def mark_session_submitted(_session_id: str) -> None:
            raise RuntimeError("resource session write failed")

    AutoEvolutionScheduler._mark_resource_session_submitted_after_remote_acceptance(
        service=FailingResourceService(),  # type: ignore[arg-type]
        session_id="qers_test",
        task_id="qe_task",
        loop_index=3,
    )

    assert "QE_RESOURCE_SESSION_POST_ACCEPTANCE_SYNC_FAILED" in caplog.text
    assert "remote execution remains active" in caplog.text
