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


class _NodeClient:
    def __init__(self, text: str = "final line 1\nfinal line 2\n"):
        self.text = text
        self.calls = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.closed = True

    async def close(self):
        self.closed = True

    async def get_workspace_file(self, task_id, loop_id, file_path):
        self.calls.append((task_id, loop_id, file_path))
        return self.text

    async def stream_task_logs(self, task_id):
        if False:
            yield ""
        raise RuntimeError("live stream unavailable")


def test_terminal_experiment_logs_use_node_tail_only(monkeypatch):
    task_id = "qe_terminal_exp"
    loop_id = "Loop1"
    row = (task_id, loop_id, "completed", {"execution_node_id": "node-a"})
    node_client = _NodeClient()

    monkeypatch.setattr(qt, "get_conn", lambda: _Conn(row))
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient.for_node",
        lambda node_id: node_client,
    )

    async def _run():
        response = await qt.stream_experiment_logs("exp_terminal")
        body_parts = []
        async for chunk in response.body_iterator:
            body_parts.append(chunk.decode() if isinstance(chunk, bytes) else chunk)
        return "".join(body_parts)

    body = asyncio.run(_run())

    assert "showing QE node log tail only" in body
    assert "final line 1" in body
    assert "final line 2" in body
    assert "AIstock authoritative final status: completed" in body
    assert "local run.log" not in body
    assert node_client.calls == [(task_id, loop_id, "run.log")]


def test_terminal_experiment_log_tail_endpoint_uses_node_api(monkeypatch):
    task_id = "qe_tail_exp"
    loop_id = "Loop1"
    row = (task_id, loop_id, "failed", {"execution_node_id": "node-a"})
    node_client = _NodeClient("a\nb\nc\n")

    monkeypatch.setattr(qt, "get_conn", lambda: _Conn(row))
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient.for_node",
        lambda node_id: node_client,
    )

    result = asyncio.run(qt.get_experiment_logs_tail("exp_tail", tail=2))

    assert result["status"] == "success"
    assert result["data"]["terminal"] is True
    assert result["data"]["logs"] == ["b", "c"]
    assert result["data"]["log_source"] == "qe_workspace_api"
    assert result["data"]["node_id"] == "node-a"
    assert "log_path" not in result["data"]
    assert node_client.calls == [(task_id, loop_id, "run.log")]


def test_active_log_stream_falls_back_to_node_tail_not_local_fs(monkeypatch):
    task_id = "qe_active_exp"
    loop_id = "Loop1"
    row = (task_id, loop_id, "running", {"execution_node_id": "node-a"})
    node_client = _NodeClient("tail after stream failure\n")

    monkeypatch.setattr(qt, "get_conn", lambda: _Conn(row))
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient.for_node",
        lambda node_id: node_client,
    )

    async def _run():
        response = await qt.stream_experiment_logs("exp_active")
        body_parts = []
        async for chunk in response.body_iterator:
            body_parts.append(chunk.decode() if isinstance(chunk, bytes) else chunk)
        return "".join(body_parts)

    body = asyncio.run(_run())

    assert "showing QE node run.log tail via API" in body
    assert "tail after stream failure" in body
    assert "local run.log" not in body
    assert node_client.calls == [(task_id, loop_id, "run.log")]
