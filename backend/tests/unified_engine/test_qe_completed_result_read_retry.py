"""Completed-result reads must not turn transport uncertainty into failed work."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from backend.services.quantevolver import qe_workspace_client as workspace


@pytest.mark.parametrize("method", ["get_loop_metrics", "get_enhanced_metrics"])
def test_completed_result_read_retries_timeout_without_execution(monkeypatch, method):
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ReadTimeout("", request=request)
        return httpx.Response(200, json={"IC": 0.07})

    sleep = AsyncMock()
    monkeypatch.setattr(workspace.asyncio, "sleep", sleep)

    async def run():
        client = workspace.QEWorkspaceClient.__new__(workspace.QEWorkspaceClient)
        client.base_url = "http://node/api/v1/qe_workspace"
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client.client = http
            assert await getattr(client, method)("task", "task_Loop3") == {"IC": 0.07}

    asyncio.run(run())
    assert [r.method for r in calls] == ["GET", "GET"]
    assert all("/tasks/task/loops/Loop3/" in r.url.path for r in calls)
    sleep.assert_awaited_once_with(5)


@pytest.mark.parametrize("method", ["get_loop_metrics", "get_enhanced_metrics"])
@pytest.mark.parametrize("failure", ["timeout", 503, 404, 403, "empty", "invalid"])
def test_result_read_is_bounded_and_real_contract_errors_still_fail(monkeypatch, method, failure):
    calls = []

    def handler(request):
        calls.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("", request=request)
        if failure == "empty":
            return httpx.Response(200, json={})
        if failure == "invalid":
            return httpx.Response(200, text="not-json")
        return httpx.Response(failure, json={"detail": "unavailable"})

    monkeypatch.setattr(workspace.asyncio, "sleep", AsyncMock())
    transient = failure in {"timeout", 503}

    async def run():
        client = workspace.QEWorkspaceClient.__new__(workspace.QEWorkspaceClient)
        client.base_url = "http://node/api/v1/qe_workspace"
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client.client = http
            with pytest.raises(Exception) as error:
                await getattr(client, method)("task", "Loop3")
            assert isinstance(error.value, workspace.QEWorkspaceResultReadUnavailable) == transient
            if transient:
                assert error.value.reason_code == "qe_completed_result_read_unavailable"
                assert error.value.__cause__ is not None
                assert "error_type=" in str(error.value)

    asyncio.run(run())
    assert len(calls) == (3 if transient else 2 if failure == 404 else 1)
    assert all(r.method == "GET" for r in calls)


class _MemoryDB:
    """Drive the real completion methods without any database or execution."""

    def __init__(self, task_type):
        self.task_type = task_type
        self.loop_status = "running"
        self.task_status = "running"
        self.statements = []
        self.next_row = None
        self.analysis = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self, **_):
        return self

    def commit(self):
        pass

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        self.statements.append((sql, params))
        if "SELECT task_type" in sql:
            self.next_row = {"task_type": self.task_type}
        elif "SELECT config_json" in sql:
            self.next_row = {"config_json": {"model_id": "original_model"}}
        elif "SELECT base_experiment_id" in sql:
            self.next_row = {"base_experiment_id": "base"}
        elif "SELECT current_loop" in sql:
            self.next_row = {"current_loop": 3, "max_loops": 6}
        elif "RETURNING loop_id" in sql:
            self.next_row = {"loop_id": "task_Loop3"} if self.loop_status == "running" else None
            if self.next_row:
                self.loop_status = "processing"
        elif "SET status = 'running'," in sql:
            if self.loop_status == "processing":
                self.loop_status = "running"
                self.analysis = (self.analysis or {}) | json.loads(params[0])
        elif "UPDATE qe_evolution_loops" in sql and "status = 'failed'" in sql:
            self.loop_status = "failed"
        elif "UPDATE qe_evolution_loops" in sql and "status = 'completed'" in sql:
            self.loop_status = "completed"
            self.metrics = json.loads(params[0])
            if "agent_analysis - '_result_collection'" in sql:
                self.analysis.pop("_result_collection", None)
        elif "UPDATE qe_evolution_tasks" in sql:
            self.task_status = params[0] if "status = %s" in sql else "failed"

    def fetchone(self):
        return self.next_row

    def fetchall(self):
        return [("completed", 6)]


@pytest.mark.parametrize("task_type", ["custom_evo", "strategy_evo", "auto"])
@pytest.mark.parametrize("transient", [True, False])
def test_completion_releases_claim_only_for_transient_result_reads(monkeypatch, task_type, transient):
    from backend.services.quantevolver import qe_evolution_service as evolution

    db = _MemoryDB(task_type)
    monkeypatch.setattr(evolution, "get_conn", lambda: db)
    scheduler = evolution.AutoEvolutionScheduler.__new__(evolution.AutoEvolutionScheduler)
    error = workspace.QEWorkspaceResultReadUnavailable(
        task_id="task", loop_id="Loop3", endpoint="metrics", error=httpx.ReadTimeout(""),
    ) if transient else RuntimeError("missing result artifact")
    client = SimpleNamespace(get_loop_metrics=AsyncMock(side_effect=error))
    scheduler.agents = SimpleNamespace(reset_trace=lambda: None, get_trace=lambda: [])
    scheduler._get_loop_node_id = lambda *_: "wsl2-5080"
    scheduler._get_workspace_client_for_loop = lambda *_: client
    scheduler._get_workspace_client_for_task = lambda *_: client
    assert asyncio.run(scheduler.process_completed_loop("task", "task_Loop3")) is False
    if transient:
        assert (db.loop_status, db.task_status) == ("running", "running")
        assert db.analysis["_result_collection"]["reason_code"] == error.reason_code
        assert not any("UPDATE qe_evolution_tasks" in sql for sql, _ in db.statements)
    else:
        assert db.loop_status == "failed"


@pytest.mark.parametrize("status", ["cancelled", "completed"])
def test_deferred_read_cannot_resurrect_concurrent_terminal_state(monkeypatch, status):
    from backend.services.quantevolver import qe_evolution_service as evolution

    db = _MemoryDB("custom_evo")
    db.loop_status = status
    monkeypatch.setattr(evolution, "get_conn", lambda: db)
    scheduler = evolution.AutoEvolutionScheduler.__new__(evolution.AutoEvolutionScheduler)
    scheduler._defer_completed_result_read("task_Loop3", workspace.QEWorkspaceResultReadUnavailable(
        task_id="task", loop_id="Loop3", endpoint="metrics", error=httpx.ReadTimeout(""),
    ))
    assert db.loop_status == status
    assert "AND status = 'processing'" in db.statements[-1][0]


def test_next_existing_reconciliation_pass_registers_results_without_resubmitting(monkeypatch):
    from backend.services.quantevolver import qe_evolution_service as evolution

    db = _MemoryDB("custom_evo")
    monkeypatch.setattr(evolution, "get_conn", lambda: db)
    monkeypatch.setattr(evolution, "merge_qe_minute_runtime_contract", lambda *_a, **_k: {})
    scheduler = evolution.AutoEvolutionScheduler.__new__(evolution.AutoEvolutionScheduler)
    error = workspace.QEWorkspaceResultReadUnavailable(
        task_id="task", loop_id="Loop3", endpoint="metrics", error=httpx.ReadTimeout(""),
    )
    client = SimpleNamespace(
        get_loop_metrics=AsyncMock(side_effect=[error, {"IC": 0.07}]),
        get_enhanced_metrics=AsyncMock(return_value={"source": "existing"}),
    )
    scheduler._get_loop_node_id = lambda *_: "wsl2-5080"
    scheduler._get_workspace_client_for_loop = lambda *_: client
    archive_calls = []
    scheduler._archive_completed_loop_best_effort = lambda *a: archive_calls.append(a)
    scheduler._record_research_backtest_best_effort = lambda *_: None

    async def run():
        assert not await scheduler.process_completed_loop("task", "task_Loop3")
        assert db.loop_status == "running"
        assert await scheduler.process_completed_loop("task", "task_Loop3")

    asyncio.run(run())
    assert (db.loop_status, db.task_status) == ("completed", "completed")
    assert db.metrics["IC"] == 0.07
    assert db.metrics["enhanced_metrics"] == {"source": "existing"}
    assert db.analysis == {}
    assert archive_calls == [("task", "task_Loop3", 3)]
    assert len([sql for sql, _ in db.statements if "INSERT INTO qe_experiments" in sql]) == 1
