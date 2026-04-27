import asyncio

from backend.routers import quantevolver as qt


class _Cursor:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        return None

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return _Cursor(self._row)


def test_terminal_experiment_logs_are_local_tail_only(tmp_path, monkeypatch):
    task_id = "qe_terminal_exp"
    loop_id = "Loop1"
    run_log = tmp_path / task_id / loop_id / "run.log"
    run_log.parent.mkdir(parents=True)
    run_log.write_text("final line 1\nfinal line 2\n", encoding="utf-8")

    row = (task_id, loop_id, "completed", str(tmp_path / task_id), {})
    monkeypatch.setattr(qt, "get_conn", lambda: _Conn(row))

    async def _run():
        response = await qt.stream_experiment_logs("exp_terminal")
        body_parts = []
        async for chunk in response.body_iterator:
            body_parts.append(chunk.decode() if isinstance(chunk, bytes) else chunk)
        return "".join(body_parts)

    body = asyncio.run(_run())

    assert "showing local log tail only" in body
    assert "final line 1" in body
    assert "final line 2" in body
    assert "AIstock authoritative final status: completed" in body


def test_terminal_experiment_log_tail_endpoint_does_not_open_stream(tmp_path, monkeypatch):
    task_id = "qe_tail_exp"
    loop_id = "Loop1"
    run_log = tmp_path / task_id / loop_id / "run.log"
    run_log.parent.mkdir(parents=True)
    run_log.write_text("a\nb\nc\n", encoding="utf-8")

    row = (task_id, loop_id, "failed", str(tmp_path / task_id))
    monkeypatch.setattr(qt, "get_conn", lambda: _Conn(row))

    result = asyncio.run(qt.get_experiment_logs_tail("exp_tail", tail=2))

    assert result["status"] == "success"
    assert result["data"]["terminal"] is True
    assert result["data"]["logs"] == ["b", "c"]
