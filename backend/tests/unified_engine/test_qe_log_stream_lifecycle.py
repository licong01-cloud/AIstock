import asyncio
import inspect
import json
from pathlib import Path

import pytest

from backend.services.quantevolver import qe_evolution_service as qes


def _collect_stream(stream):
    async def _collect():
        return [chunk async for chunk in stream]

    return asyncio.run(_collect())


def test_deleted_task_log_stream_is_terminal_and_does_not_create_log_file(tmp_path, monkeypatch):
    monkeypatch.setattr(qes, "SOTA_ASSETS_DIR", str(tmp_path))
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    scheduler._get_task_status = lambda task_id: None

    chunks = _collect_stream(scheduler.stream_task_logs("qe_deleted"))

    assert len(chunks) == 1
    assert '"event": "task_deleted"' in chunks[0]
    assert not (tmp_path / "qe_deleted" / "logs" / "evolution.log").exists()


def test_terminal_task_missing_workspace_does_not_persist_waiting_spam(tmp_path, monkeypatch):
    monkeypatch.setattr(qes, "SOTA_ASSETS_DIR", str(tmp_path))

    class DummyClient:
        async def stream_task_logs(self, task_id):
            payload = {"status": "waiting", "logs": [f"Task directory not found yet: {task_id}"]}
            yield "data: " + json.dumps(payload)

    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    scheduler.task_exists = lambda task_id: True
    scheduler._get_task_status = lambda task_id: "failed"
    scheduler._get_workspace_client_for_task = lambda task_id: DummyClient()

    chunks = _collect_stream(scheduler.stream_task_logs("qe_missing"))

    assert len(chunks) == 2
    assert '"event": "task_log_terminal"' in chunks[-1]
    assert "no live log stream was opened" in chunks[-1]
    log_path = tmp_path / "qe_missing" / "logs" / "evolution.log"
    assert not log_path.exists()


def test_terminal_task_reads_existing_tail_without_opening_upstream(tmp_path, monkeypatch):
    monkeypatch.setattr(qes, "SOTA_ASSETS_DIR", str(tmp_path))
    log_path = tmp_path / "qe_done" / "logs" / "evolution.log"
    log_path.parent.mkdir(parents=True)
    log_path.write_text("old-1\nold-2\n", encoding="utf-8")

    class FailingClient:
        async def stream_task_logs(self, task_id):
            raise AssertionError("terminal task must not open upstream stream")

    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    scheduler.task_exists = lambda task_id: True
    scheduler._get_task_status = lambda task_id: "completed"
    scheduler._get_workspace_client_for_task = lambda task_id: FailingClient()

    chunks = _collect_stream(scheduler.stream_task_logs("qe_done"))

    joined = "".join(chunks)
    assert '"event": "task_log_tail"' in joined
    assert "old-1" in joined
    assert "old-2" in joined
    assert '"event": "task_log_terminal"' in joined


def test_custom_evo_log_stream_node_plan_includes_strategy_and_loop_nodes(monkeypatch):
    task_row = {
        "task_id": "qe_dist",
        "task_type": "custom_evo",
        "node_id": "node-a",
        "strategy_evo_config": {
            "loops": [
                {"loop_index": 1, "node_id": "node-a"},
                {"loop_index": 2, "node_id": ""},
                {"loop_index": 3, "node_id": "node-b"},
            ]
        },
    }
    loop_rows = [
        {"loop_index": 1, "node_id": "node-a"},
        {"loop_index": 2, "node_id": "node-a"},
        {"loop_index": 3, "node_id": "node-b"},
        {"loop_index": 4, "node_id": "node-c"},
    ]

    class FakeCursor:
        def __init__(self):
            self.kind = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def execute(self, sql, params):
            if "FROM qe_evolution_tasks" in sql:
                self.kind = "task"
            elif "FROM qe_evolution_loops" in sql:
                self.kind = "loops"

        def fetchone(self):
            return task_row if self.kind == "task" else None

        def fetchall(self):
            return loop_rows if self.kind == "loops" else []

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def cursor(self, *args, **kwargs):
            return FakeCursor()

    monkeypatch.setattr(qes, "get_conn", lambda: FakeConn())
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)

    plan = scheduler._get_log_stream_node_plan_for_task("qe_dist")

    assert plan["node_ids"] == ["node-a", "node-b", "node-c"]
    assert plan["warnings"] == []


def test_distributed_task_log_stream_fans_in_all_loop_nodes(tmp_path, monkeypatch):
    monkeypatch.setattr(qes, "SOTA_ASSETS_DIR", str(tmp_path))

    class DummyClient:
        def __init__(self, payloads):
            self.payloads = payloads

        async def stream_task_logs(self, task_id):
            for payload in self.payloads:
                yield "data: " + json.dumps(payload, ensure_ascii=False)

    clients = {
        "node-a": DummyClient([{"status": "running", "logs": ["[Loop1] local-node line"]}]),
        "node-b": DummyClient([{"status": "running", "logs": ["[Loop2] remote-node line"]}]),
    }

    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    scheduler._get_task_status = lambda task_id: "running"
    scheduler._get_log_stream_node_plan_for_task = lambda task_id: {
        "task_id": task_id,
        "task_type": "custom_evo",
        "node_ids": ["node-a", "node-b"],
        "warnings": [],
    }
    scheduler._get_workspace_client_for_node_id = lambda node_id: clients[node_id]

    chunks = _collect_stream(scheduler.stream_task_logs("qe_dist"))
    joined = "".join(chunks)

    assert "[node-a] [Loop1] local-node line" in joined
    assert "[node-b] [Loop2] remote-node line" in joined
    log_text = (tmp_path / "qe_dist" / "logs" / "evolution.log").read_text(encoding="utf-8")
    assert "[Log Nodes] node-a, node-b" in log_text
    assert "[node-a] [Loop1] local-node line" in log_text
    assert "[node-b] [Loop2] remote-node line" in log_text


def test_delete_task_captures_node_id_before_db_record_deletion():
    source = inspect.getsource(qes.AutoEvolutionScheduler.delete_task)

    assert "status, node_id FROM qe_evolution_tasks" in source
    assert "task_node_id = task.get(\"node_id\")" in source
    assert "self._get_workspace_client_for_node_id(task_node_id)" in source


class _FakeEvolutionDeleteConn:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def cursor(self, *args, **kwargs):
        return _FakeEvolutionDeleteCursor(self.state)

    def commit(self):
        self.state["events"].append("db_commit")
        self.state["commits"] += 1


class _FakeEvolutionDeleteCursor:
    def __init__(self, state):
        self.state = state
        self.rowcount = 1
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        self.state["sql"].append(normalized)
        self._rows = []
        if normalized.startswith("SELECT task_id, task_name, status, node_id FROM qe_evolution_tasks"):
            self._rows = [self.state["task_row"]]
        elif normalized.startswith("SELECT task_id, task_name FROM qe_evolution_tasks"):
            self._rows = self.state.get("dependent_forks", [])
        elif normalized.startswith("SELECT experiment_id FROM qe_experiments"):
            self._rows = [{"experiment_id": eid} for eid in self.state.get("sub_experiment_ids", [])]
        elif normalized.startswith("DELETE "):
            self.state["events"].append("db_delete")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


def test_delete_task_removes_local_sota_log_cache_after_node_api(tmp_path, monkeypatch):
    from backend.services.quantevolver import config_composer

    task_id = "qe_delete_unit"
    experiments_root = tmp_path / "qe_experiments"
    sota_root = tmp_path / "qe_sota_assets"
    experiment_dir = experiments_root / task_id
    sota_task_dir = sota_root / task_id
    log_path = sota_task_dir / "logs" / "evolution.log"
    optuna_dir = sota_root / "optuna_studies"
    log_path.parent.mkdir(parents=True)
    experiment_dir.mkdir(parents=True)
    optuna_dir.mkdir(parents=True)
    log_path.write_text("old log\n", encoding="utf-8")
    (experiment_dir / "manifest.json").write_text("{}", encoding="utf-8")
    (optuna_dir / f"{task_id}_study.db").write_text("db", encoding="utf-8")

    monkeypatch.setattr(qes, "SOTA_ASSETS_DIR", str(sota_root))
    monkeypatch.setattr(config_composer, "QE_EXPERIMENTS_ROOT", experiments_root)

    state = {
        "sql": [],
        "events": [],
        "commits": 0,
        "task_row": {"task_id": task_id, "task_name": "delete unit", "status": "failed", "node_id": "node-a"},
        "sub_experiment_ids": [f"{task_id}_Loop1"],
    }
    monkeypatch.setattr(qes, "get_conn", lambda: _FakeEvolutionDeleteConn(state))

    class FakeWorkspaceClient:
        async def cleanup_task_workspace(self, cleanup_task_id):
            state["events"].append("node_cleanup")
            assert cleanup_task_id == task_id
            return True

    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    scheduler._get_workspace_client_for_node_id = lambda node_id: FakeWorkspaceClient()

    result = asyncio.run(scheduler.delete_task(task_id))

    assert result["deleted_counts"]["cleaned_dirs"] == 2
    assert result["deleted_counts"]["optuna_files_deleted"] == 1
    assert state["events"].index("node_cleanup") < state["events"].index("db_delete")
    assert state["commits"] == 1
    assert not experiment_dir.exists()
    assert not sota_task_dir.exists()
    assert not (optuna_dir / f"{task_id}_study.db").exists()


def test_delete_task_fails_before_local_and_db_delete_when_log_stream_is_still_open(tmp_path, monkeypatch):
    from backend.services.quantevolver import config_composer

    task_id = "qe_stream_open"
    experiments_root = tmp_path / "qe_experiments"
    sota_root = tmp_path / "qe_sota_assets"
    experiment_dir = experiments_root / task_id
    sota_task_dir = sota_root / task_id
    experiment_dir.mkdir(parents=True)
    sota_task_dir.mkdir(parents=True)

    monkeypatch.setattr(qes, "SOTA_ASSETS_DIR", str(sota_root))
    monkeypatch.setattr(config_composer, "QE_EXPERIMENTS_ROOT", experiments_root)

    state = {
        "sql": [],
        "events": [],
        "commits": 0,
        "task_row": {"task_id": task_id, "task_name": "stream open", "status": "failed", "node_id": "node-a"},
        "sub_experiment_ids": [],
    }
    monkeypatch.setattr(qes, "get_conn", lambda: _FakeEvolutionDeleteConn(state))

    class FakeWorkspaceClient:
        async def cleanup_task_workspace(self, cleanup_task_id):
            state["events"].append("node_cleanup")
            return True

    async def still_open(*args, **kwargs):
        return 1

    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    scheduler._get_workspace_client_for_node_id = lambda node_id: FakeWorkspaceClient()
    scheduler._wait_for_log_streams_closed = still_open

    with pytest.raises(RuntimeError, match=task_id):
        asyncio.run(scheduler.delete_task(task_id))

    assert "node_cleanup" in state["events"]
    assert not any(sql.startswith("DELETE ") for sql in state["sql"])
    assert state["commits"] == 0
    assert experiment_dir.exists()
    assert sota_task_dir.exists()


def test_frontend_log_stream_requires_expanded_log_panel():
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "frontend" / "src" / "app" / "quantevolver" / "evolution" / "page.tsx").read_text(encoding="utf-8")
    log_effect = source[
        source.index("Log files are touched only after the operator expands the log panel."):
        source.index("const fetchSourceTasks = useCallback")
    ]

    assert "const [logsCollapsed, setLogsCollapsed] = useState(true);" in source
    assert "if (!activeTaskId || logsCollapsed)" in log_effect
    assert log_effect.index("if (!activeTaskId || logsCollapsed)") < log_effect.index("new EventSource")
    assert log_effect.index("if (!activeTaskId || logsCollapsed)") < log_effect.index("/logs/tail?tail=200")
