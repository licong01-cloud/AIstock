import asyncio
import inspect
import json

from backend.services.quantevolver import qe_evolution_service as qes


def _collect_stream(stream):
    async def _collect():
        return [chunk async for chunk in stream]

    return asyncio.run(_collect())


def test_deleted_task_log_stream_is_terminal_and_does_not_create_log_file(tmp_path, monkeypatch):
    monkeypatch.setattr(qes, "SOTA_ASSETS_DIR", str(tmp_path))
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    scheduler.task_exists = lambda task_id: False

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

    assert len(chunks) == 1
    assert '"event": "task_log_workspace_missing"' in chunks[0]
    log_path = tmp_path / "qe_missing" / "logs" / "evolution.log"
    assert log_path.exists()
    assert "Task directory not found yet" not in log_path.read_text(encoding="utf-8")


def test_delete_task_captures_node_id_before_db_record_deletion():
    source = inspect.getsource(qes.AutoEvolutionScheduler.delete_task)

    assert "status, node_id FROM qe_evolution_tasks" in source
    assert "task_node_id = task.get(\"node_id\")" in source
    assert "self._get_workspace_client_for_node_id(task_node_id)" in source
