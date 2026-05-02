import inspect
from pathlib import Path

import pytest

from backend.routers import quantevolver as qe_router
from backend.services.quantevolver import qe_evolution_service as qes
from backend.services.strategy_package.workspace_policy import remove_aistock_artifact_tree
from backend.services.trading_core.errors import StrategyPackageValidationError

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_qe_experiment_delete_uses_node_api_and_local_cleanup_policy() -> None:
    text = Path(qe_router.__file__).read_text(encoding="utf-8")
    segment = text[text.index("async def delete_experiment"): text.index("# ============================================================", text.index("async def delete_experiment"))]

    assert "QE_WORKSPACE_WIN" not in segment
    assert "RDAGENT_WORKSPACE_WIN" not in segment
    assert "cleanup_task_workspace" in segment
    assert "remove_aistock_artifact_tree" in segment
    assert "unlink_aistock_artifact_files" in segment
    assert "worker_workspace_cleanup_mode" in segment
    assert "delete_experiment" in qe_router.delete_experiment.__name__


def test_rdagent_task_delete_uses_node_api_and_guarded_dispatch_cleanup() -> None:
    router_path = REPO_ROOT / "backend" / "routers" / "rdagent.py"
    text = router_path.read_text(encoding="utf-8")
    start = text.index("def delete_task(task_id: str)")
    end = text.index("    parts = []", start)
    segment = text[start:end]

    assert "delete_task_on_node" in segment
    assert "remove_aistock_artifact_tree" in segment
    assert "shutil.rmtree" not in segment
    assert "RDAGENT_WORKSPACE_WIN" not in segment
    assert "QE_WORKSPACE_WIN" not in segment


def test_cleanup_policy_refuses_worker_workspace_delete(tmp_path, monkeypatch) -> None:
    worker_root = tmp_path / "worker_qe_workspace"
    target = worker_root / "task-a"
    target.mkdir(parents=True)
    monkeypatch.setenv("QE_WORKSPACE_WIN", str(worker_root))

    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        remove_aistock_artifact_tree(
            target,
            purpose="unit test worker cleanup refusal",
            allowed_roots=[worker_root],
        )

    assert target.exists()


def test_cleanup_policy_removes_only_child_under_explicit_local_root(tmp_path) -> None:
    root = tmp_path / "qe_experiments"
    target = root / "task-a"
    sibling = root / "task-b"
    target.mkdir(parents=True)
    sibling.mkdir(parents=True)

    removed = remove_aistock_artifact_tree(
        target,
        purpose="unit test local cleanup",
        allowed_roots=[root],
    )

    assert removed is True
    assert not target.exists()
    assert sibling.exists()
    with pytest.raises(StrategyPackageValidationError, match="root itself"):
        remove_aistock_artifact_tree(
            root,
            purpose="unit test root cleanup refusal",
            allowed_roots=[root],
        )


def test_evolution_service_cleanup_source_has_no_worker_filesystem_deletes() -> None:
    source = Path(qes.__file__).read_text(encoding="utf-8")
    cleanup_segment = source[source.index("    async def delete_custom_evo_loop_result"): source.index("    def _normalize_full_custom_evo_nodes")]

    assert "QE_WORKSPACE_WIN" not in inspect.getsource(qes.AutoEvolutionScheduler.delete_task)
    assert "QE_WORKSPACE_WIN" not in cleanup_segment
    assert "_cleanup_loop_workspace_via_node_filesystem" not in source
    assert "node_filesystem_local" not in source
    assert "node_filesystem_ssh" not in source


class _FakeDeleteConn:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def cursor(self, *args, **kwargs):
        return _FakeDeleteCursor(self.state)

    def commit(self):
        self.state["commits"] += 1


class _FakeDeleteCursor:
    def __init__(self, state):
        self.state = state
        self.sql = ""
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, sql, params=None):
        self.sql = " ".join(str(sql).split())
        self.state["sql"].append(self.sql)

    def fetchone(self):
        if "SELECT status FROM qe_experiments" in self.sql:
            return ("completed",)
        return None

    def fetchall(self):
        if "SELECT DISTINCT assigned_node_id" in self.sql:
            return [("node-b",)]
        return []


def test_qe_experiment_delete_cleans_local_assets_without_worker_workspace(tmp_path, monkeypatch) -> None:
    import asyncio

    from backend.services.quantevolver import config_composer
    import backend.services.quantevolver.qe_workspace_client as workspace_client_module

    experiment_id = "qe_cleanup_unit"
    worker_root = tmp_path / "worker_qe_workspace"
    experiments_root = tmp_path / "qe_experiments"
    sota_root = tmp_path / "qe_sota_assets"
    worker_dir = worker_root / experiment_id
    experiment_dir = experiments_root / experiment_id
    sota_dir = sota_root / experiment_id
    optuna_dir = sota_root / "optuna_studies"
    for directory in [worker_dir, experiment_dir, sota_dir, optuna_dir]:
        directory.mkdir(parents=True)
    (worker_dir / "run.log").write_text("worker", encoding="utf-8")
    (experiment_dir / "manifest.json").write_text("{}", encoding="utf-8")
    (sota_dir / "metrics.json").write_text("{}", encoding="utf-8")
    (optuna_dir / f"{experiment_id}_study.db").write_text("db", encoding="utf-8")

    monkeypatch.setenv("QE_WORKSPACE_WIN", str(worker_root))
    monkeypatch.setenv("QE_SOTA_ASSETS_DIR", str(sota_root))
    monkeypatch.setattr(config_composer, "QE_EXPERIMENTS_ROOT", experiments_root)

    db_state = {"sql": [], "commits": 0}
    monkeypatch.setattr(qe_router, "get_conn", lambda: _FakeDeleteConn(db_state))

    cleanup_calls = []

    class FakeWorkspaceClient:
        def __init__(self, node_id="default"):
            self.node_id = node_id

        @classmethod
        def for_node(cls, node_id):
            return cls(node_id)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

        async def cleanup_task_workspace(self, task_id):
            cleanup_calls.append((self.node_id, task_id))
            return True

    monkeypatch.setattr(workspace_client_module, "QEWorkspaceClient", FakeWorkspaceClient)

    result = asyncio.run(qe_router.delete_experiment(experiment_id))

    assert result["ok"] is True
    assert result["worker_workspace_cleanup_mode"] == "node_api_only"
    assert ("default", experiment_id) in cleanup_calls
    assert ("node-b", experiment_id) in cleanup_calls
    assert worker_dir.exists(), "Windows side must not delete worker workspace"
    assert not experiment_dir.exists()
    assert not sota_dir.exists()
    assert not (optuna_dir / f"{experiment_id}_study.db").exists()
    assert any(sql.startswith("DELETE FROM qe_experiments") for sql in db_state["sql"])

class _FakeRDAgentConn:
    def __init__(self, state):
        self.state = state

    def cursor(self):
        return _FakeRDAgentCursor(self.state)

    def commit(self):
        self.state["commits"] += 1

    def close(self):
        self.state["closed"] = True


class _FakeRDAgentCursor:
    def __init__(self, state):
        self.state = state
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.state["sql"].append(" ".join(str(sql).split()))
        if "candidate_loops" in str(sql):
            self.rowcount = 2
        elif "candidate_tasks" in str(sql):
            self.rowcount = 1
        else:
            self.rowcount = 0

    def close(self):
        self.state["cursor_closed"] = True


class _FakeRDAgentCandidateService:
    def __init__(self, state):
        self.state = state

    def delete_task_on_node(self, task_id):
        self.state["remote_calls"].append(task_id)
        return {"ok": True}

    def _get_db_connection(self):
        return _FakeRDAgentConn(self.state)


def test_rdagent_task_delete_cleans_dispatch_only_after_node_api(monkeypatch) -> None:
    from backend.routers import rdagent as rdagent_router

    task_id = "rdagent_cleanup_unit"
    dispatch_dir = REPO_ROOT / "dispatch_logs" / task_id
    dispatch_dir.mkdir(parents=True, exist_ok=True)
    (dispatch_dir / "run.log").write_text("local", encoding="utf-8")
    state = {"remote_calls": [], "sql": [], "commits": 0, "closed": False, "cursor_closed": False}
    monkeypatch.setattr(rdagent_router, "get_candidate_service", lambda: _FakeRDAgentCandidateService(state))

    try:
        result = rdagent_router.delete_task(task_id)
    finally:
        if dispatch_dir.exists():
            # Test-owned local artifact only; keep cleanup contained to this task id.
            remove_aistock_artifact_tree(
                dispatch_dir,
                purpose="test cleanup fallback",
                allowed_roots=[REPO_ROOT / "dispatch_logs"],
                ignore_errors=True,
            )

    assert result["ok"] is True
    assert result["remote_warning"] is None
    assert result["dispatch_deleted"] is True
    assert state["remote_calls"] == [task_id]
    assert not dispatch_dir.exists()
    assert any("rdagent_candidate_loops" in sql for sql in state["sql"])
    assert any("rdagent_candidate_tasks" in sql for sql in state["sql"])
