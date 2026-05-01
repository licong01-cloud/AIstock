import asyncio
import subprocess

import pytest

from backend.services.quantevolver import qe_evolution_service as qes
from backend.services.quantevolver.qe_workspace_client import QELoopWorkspaceCleanupUnavailable


class FakeConnContext:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        self.state["entered"] += 1
        return FakeConn(self.state)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.state["exited"] += 1
        return False


class FakeConn:
    def __init__(self, state):
        self.state = state

    def cursor(self, *args, **kwargs):
        return FakeCursor(self.state)

    def commit(self):
        self.state["commits"] += 1


class FakeCursor:
    def __init__(self, state):
        self.state = state
        self.sql = ""
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, sql, params=None):
        self.sql = " ".join(str(sql).split())
        self.state["sql"].append(self.sql)
        self.rowcount = 1 if self.sql.startswith("DELETE") else 0

    def fetchone(self):
        if "pg_try_advisory_lock" in self.sql:
            return (True,)
        if "FROM qe_evolution_tasks" in self.sql:
            return {
                "task_id": "task-a",
                "task_type": "custom_evo",
                "node_id": "node-a",
                "strategy_evo_config": {
                    "loops": [
                        {"loop_index": 1, "node_id": "node-a", "label": "base"},
                        {"loop_index": 2, "node_id": "node-b", "label": "old"},
                    ],
                    "node_parallelism": {"node-a": 1, "node-b": 1},
                },
                "strategy_evo_execution_mode": "serial",
            }
        if "FROM qe_evolution_loops" in self.sql:
            return {
                "loop_id": "task-a_Loop2",
                "loop_index": 2,
                "status": "completed",
                "node_id": "node-b",
                "experiment_id": "task-a_L2",
            }
        return None

    def fetchall(self):
        if "FROM qe_evolution_loops" in self.sql:
            return []
        return []


def _patch_fake_db(monkeypatch):
    state = {"entered": 0, "exited": 0, "commits": 0, "sql": []}
    monkeypatch.setattr(qes, "get_conn", lambda: FakeConnContext(state))
    return state


def test_rerun_custom_evo_loop_uses_get_conn_context_manager_for_lock(monkeypatch):
    state = _patch_fake_db(monkeypatch)
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)

    async def fake_delete(task_id, loop_index):
        return {"loop_id": f"{task_id}_Loop{loop_index}", "deleted_counts": {}}

    scheduler.delete_custom_evo_loop_result = fake_delete

    result = asyncio.run(
        scheduler.rerun_custom_evo_loop(
            "task-a",
            2,
            {"loop_index": 2, "node_id": "node-b", "label": "replacement"},
            execution_mode="parallel_2",
            node_parallelism={"node-a": 1, "node-b": 1},
        )
    )

    assert result["loop_id"] == "task-a_Loop2"
    assert state["entered"] == state["exited"]
    assert any("pg_advisory_unlock" in sql for sql in state["sql"])


def test_append_custom_evo_loops_uses_get_conn_context_manager_for_lock(monkeypatch):
    state = _patch_fake_db(monkeypatch)
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)

    result = asyncio.run(
        scheduler.append_custom_evo_loops(
            "task-a",
            [{"node_id": "node-b", "label": "new"}],
            execution_mode="parallel_2",
            node_parallelism={"node-a": 1, "node-b": 1},
            ack_failed_loop_warning=True,
        )
    )

    assert result["new_loop_indexes"] == [3]
    assert state["entered"] == state["exited"]
    assert any("pg_advisory_unlock" in sql for sql in state["sql"])


def test_delete_custom_evo_loop_result_uses_filesystem_cleanup_when_api_route_missing(monkeypatch):
    _patch_fake_db(monkeypatch)
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)

    class MissingCleanupClient:
        async def cleanup_loop_workspace(self, task_id, loop_name):
            raise QELoopWorkspaceCleanupUnavailable(f"missing endpoint: {task_id}/{loop_name}")

    monkeypatch.setattr(scheduler, "_get_workspace_client_for_node_id", lambda node_id: MissingCleanupClient())
    monkeypatch.setattr(scheduler, "_cleanup_local_custom_evo_loop_dirs", lambda task_id, loop_index: [])
    monkeypatch.setattr(
        scheduler,
        "_cleanup_loop_workspace_via_node_filesystem",
        lambda node_id, task_id, loop_name, reason: {
            "ok": True,
            "method": "node_filesystem_local",
            "node_id": node_id,
            "existed": False,
            "reason": reason,
        },
    )

    result = asyncio.run(scheduler.delete_custom_evo_loop_result("task-a", 2))

    assert result["remote_cleanup"]["method"] == "node_filesystem_local"
    assert result["remote_cleanup"]["node_id"] == "node-b"
    assert "missing endpoint" in result["remote_cleanup"]["reason"]


def test_local_node_filesystem_loop_cleanup_deletes_only_target_loop(tmp_path, monkeypatch):
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    workspace = tmp_path / "qe_workspace"
    target = workspace / "task-a" / "Loop2"
    sibling = workspace / "task-a" / "Loop1"
    target.mkdir(parents=True)
    sibling.mkdir(parents=True)
    (target / "run.log").write_text("old", encoding="utf-8")
    (sibling / "run.log").write_text("keep", encoding="utf-8")

    monkeypatch.setattr(
        scheduler,
        "_get_compute_node_for_loop_cleanup",
        lambda node_id: {
            "node_id": "wsl2-5080",
            "api_base_url": "http://127.0.0.1:9000",
            "workspace_base": str(workspace),
        },
    )

    result = scheduler._cleanup_loop_workspace_via_node_filesystem(
        "wsl2-5080",
        "task-a",
        "Loop2",
        reason="api cleanup unavailable",
    )

    assert result["method"] == "node_filesystem_local"
    assert result["existed"] is True
    assert not target.exists()
    assert sibling.exists()


def test_remote_node_filesystem_loop_cleanup_uses_ssh_and_reports_success(monkeypatch):
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    captured = {}

    monkeypatch.setattr(
        scheduler,
        "_get_compute_node_for_loop_cleanup",
        lambda node_id: {
            "node_id": "rdagent-node1",
            "api_base_url": "http://192.168.50.215:9000",
            "ssh_user": "lc999",
            "workspace_base": "/home/lc999/projects/RD-Agent-main/qe_workspace",
        },
    )

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(cmd, 0, stdout="existed=True\n", stderr="")

    monkeypatch.setattr(qes.subprocess, "run", fake_run)

    result = scheduler._cleanup_loop_workspace_via_node_filesystem(
        "rdagent-node1",
        "task-a",
        "Loop15",
        reason="api cleanup unavailable",
    )

    assert result["method"] == "node_filesystem_ssh"
    assert result["node_id"] == "rdagent-node1"
    assert result["existed"] is True
    assert captured["cmd"][:5] == ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
    assert captured["cmd"][5] == "lc999@192.168.50.215"
    assert "python3 -c" in captured["cmd"][6]
    assert "/home/lc999/projects/RD-Agent-main/qe_workspace" in captured["cmd"][6]
    assert "Loop15" in captured["cmd"][6]
    assert captured["kwargs"]["check"] is False


def test_remote_node_filesystem_loop_cleanup_fails_fast_on_ssh_error(monkeypatch):
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)

    monkeypatch.setattr(
        scheduler,
        "_get_compute_node_for_loop_cleanup",
        lambda node_id: {
            "node_id": "rdagent-node1",
            "api_base_url": "http://192.168.50.215:9000",
            "ssh_user": "lc999",
            "workspace_base": "/home/lc999/projects/RD-Agent-main/qe_workspace",
        },
    )

    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 255, stdout="", stderr="Permission denied")

    monkeypatch.setattr(qes.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError) as exc:
        scheduler._cleanup_loop_workspace_via_node_filesystem(
            "rdagent-node1",
            "task-a",
            "Loop15",
            reason="api cleanup unavailable",
        )

    assert "Remote loop workspace cleanup via ssh failed" in str(exc.value)
    assert "Permission denied" in str(exc.value)
