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


def test_delete_task_captures_node_id_before_db_record_deletion():
    source = inspect.getsource(qes.AutoEvolutionScheduler.delete_task)

    assert "status, node_id FROM qe_evolution_tasks" in source
    assert "task_node_id = task.get(\"node_id\")" in source
    assert "self._get_workspace_client_for_node_id(task_node_id)" in source
