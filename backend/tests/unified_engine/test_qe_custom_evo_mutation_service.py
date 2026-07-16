import asyncio
import inspect

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


def test_removed_resource_telemetry_is_explicitly_normalized_false(caplog):
    assert qes._normalize_removed_resource_telemetry(
        True,
        context="unit-test",
    ) is False
    assert "QE_RESOURCE_MONITORING_DISABLED" in caplog.text


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
            node_parallelism={"node-a": 1, "node-b": 1},
            ack_failed_loop_warning=True,
        )
    )

    assert result["new_loop_indexes"] == [3]
    assert state["entered"] == state["exited"]
    assert any("pg_advisory_unlock" in sql for sql in state["sql"])


def test_delete_custom_evo_loop_result_fails_fast_when_api_route_missing(monkeypatch):
    state = _patch_fake_db(monkeypatch)
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)

    class MissingCleanupClient:
        async def cleanup_loop_workspace(self, task_id, loop_name):
            raise QELoopWorkspaceCleanupUnavailable(f"missing endpoint: {task_id}/{loop_name}")

    monkeypatch.setattr(scheduler, "_get_workspace_client_for_node_id", lambda node_id: MissingCleanupClient())

    def fail_local_cleanup(task_id, loop_index):
        raise AssertionError("local cleanup must not run when node API cleanup is unavailable")

    monkeypatch.setattr(scheduler, "_cleanup_local_custom_evo_loop_dirs", fail_local_cleanup)

    with pytest.raises(RuntimeError, match="node API"):
        asyncio.run(scheduler.delete_custom_evo_loop_result("task-a", 2))

    assert not any(sql.startswith("DELETE") for sql in state["sql"])


def test_local_custom_evo_loop_cleanup_skips_worker_workspace(tmp_path, monkeypatch):
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    worker_root = tmp_path / "worker_qe_workspace"
    experiments_root = tmp_path / "qe_experiments"
    sota_root = tmp_path / "qe_sota_assets"

    worker_loop = worker_root / "task-a" / "Loop2"
    exp_loop = experiments_root / "task-a" / "Loop2"
    sota_loop = sota_root / "task-a" / "Loop2"
    for path in [worker_loop, exp_loop, sota_loop]:
        path.mkdir(parents=True)
        (path / "run.log").write_text("old", encoding="utf-8")

    monkeypatch.setenv("QE_WORKSPACE_WIN", str(worker_root))
    from backend.services.quantevolver import config_composer

    monkeypatch.setattr(config_composer, "QE_EXPERIMENTS_ROOT", experiments_root)
    monkeypatch.setattr(qes, "SOTA_ASSETS_DIR", str(sota_root))

    cleaned = scheduler._cleanup_local_custom_evo_loop_dirs("task-a", 2)

    assert worker_loop.exists(), "worker workspace must not be touched from Windows"
    assert not exp_loop.exists()
    assert not sota_loop.exists()
    assert str(worker_root) not in "\n".join(cleaned)


def test_custom_evo_cleanup_has_no_direct_node_filesystem_fallbacks() -> None:
    source = inspect.getsource(qes.AutoEvolutionScheduler)

    assert "_cleanup_loop_workspace_via_node_filesystem" not in source
    assert "_node_workspace_to_windows_path" not in source
    assert "_remove_loop_dir_under_root" not in source
    assert "node_filesystem_local" not in source
    assert "node_filesystem_ssh" not in source
    assert "subprocess.run" not in source


def test_evolution_delete_task_no_worker_workspace_direct_cleanup() -> None:
    source = inspect.getsource(qes.AutoEvolutionScheduler.delete_task)

    assert "QE_WORKSPACE_WIN" not in source
    assert "RDAGENT_WORKSPACE_WIN" not in source
    assert "cleanup_task_workspace" in source
    assert "remove_aistock_artifact_tree" in source
