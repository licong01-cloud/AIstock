from __future__ import annotations

import asyncio

from fastapi.responses import Response

from backend.routers import rdagent_catalog_admin


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, *_args, **_kwargs):
        return None

    def fetchone(self):
        return ("ws_1", {"node_id": "node-api-1", "workspace_path": "/mnt/f/worker"}, "node-api-1")


class _Conn:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def cursor(self):
        return _Cursor()


def test_get_loop_file_serves_image_through_node_api(monkeypatch) -> None:
    calls: list[tuple[str, str, str, str]] = []

    class FakeClient:
        def __init__(self, node_id: str) -> None:
            self.node_id = node_id

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def download_workspace_file_bytes(self, task_id: str, loop_id: str, file_path: str) -> bytes:
            calls.append((self.node_id, task_id, loop_id, file_path))
            return b"png-bytes"

    monkeypatch.setattr(rdagent_catalog_admin, "get_conn", lambda: _Conn())
    monkeypatch.setattr(
        rdagent_catalog_admin.QEWorkspaceClient,
        "for_node",
        staticmethod(lambda node_id: FakeClient(node_id)),
    )

    response = asyncio.run(rdagent_catalog_admin.get_loop_file("task_1", 2, "ret_curve.png"))

    assert isinstance(response, Response)
    assert response.body == b"png-bytes"
    assert response.media_type == "image/png"
    assert calls == [("node-api-1", "task_1", "Loop2", "ret_curve.png")]
