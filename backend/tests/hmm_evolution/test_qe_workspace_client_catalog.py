from __future__ import annotations

import asyncio

import httpx
import pytest

from backend.services.hmm_evolution.runtime import ManagedQEWorkspaceReadClient
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceCatalogUnavailable,
    QEWorkspaceClient,
)


class _Cursor:
    def __init__(self, row: dict[str, str] | None) -> None:
        self.row = row
        self.params: tuple[object, ...] | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def execute(self, _sql: str, params: tuple[object, ...]) -> None:
        self.params = params

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def cursor(self, **_kwargs):
        return self._cursor


def test_workspace_client_requires_explicit_catalog_completeness() -> None:
    async def run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"files": []})

        client = QEWorkspaceClient(base_url="https://qe.example/api/v1/qe_workspace")
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            with pytest.raises(QEWorkspaceCatalogUnavailable, match="catalog_completeness"):
                await client.list_workspace_files("qe_task", "Loop8")
        finally:
            await client.close()

    asyncio.run(run())


def test_workspace_client_ignores_process_proxy_environment(monkeypatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:9")

    client = QEWorkspaceClient(base_url="http://192.168.50.215:9000/api/v1/qe_workspace")
    try:
        assert client.client._trust_env is False
    finally:
        asyncio.run(client.close())


def test_workspace_log_stream_ignores_process_proxy_environment(monkeypatch) -> None:
    captured: list[bool | None] = []

    class _Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            return None

        def raise_for_status(self) -> None:
            return None

        async def aiter_lines(self):
            yield "worker-ready"

    class _StreamClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            return None

        def stream(self, _method: str, _url: str) -> _Response:
            return _Response()

    def build_client(*, timeout, trust_env=None):
        del timeout
        captured.append(trust_env)
        return _StreamClient()

    client = QEWorkspaceClient(base_url="http://192.168.50.215:9000/api/v1/qe_workspace")
    asyncio.run(client.close())
    monkeypatch.setattr(httpx, "AsyncClient", build_client)

    async def run() -> None:
        stream = client.stream_task_logs("qe_task")
        assert await anext(stream) == "worker-ready"
        await stream.aclose()

    asyncio.run(run())
    assert captured == [False]


def test_workspace_client_list_and_stat_are_read_only_catalog_operations() -> None:
    seen_methods: list[str] = []

    async def run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            seen_methods.append(request.method)
            return httpx.Response(
                200,
                json={
                    "catalog_completeness": "complete",
                    "files": [
                        {
                            "relative_path": "reports/result.json",
                            "size_bytes": 10,
                            "sha256": "a" * 64,
                        }
                    ],
                },
            )

        client = QEWorkspaceClient(base_url="https://qe.example/api/v1/qe_workspace")
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            catalog = await client.list_workspace_files("qe_task", "Loop8")
            stat = await client.stat_workspace_file(
                "qe_task", "Loop8", "reports/result.json"
            )
        finally:
            await client.close()
        assert catalog["catalog_completeness"] == "complete"
        assert stat["relative_path"] == "reports/result.json"

    asyncio.run(run())
    assert seen_methods == ["GET", "GET"]


def test_workspace_client_resolves_loop_node_before_task_node(monkeypatch) -> None:
    cursor = _Cursor({"node_id": "node-loop"})
    monkeypatch.setattr(
        "backend.db.pg_pool.get_conn",
        lambda: _Connection(cursor),
    )
    captured: list[str] = []
    monkeypatch.setattr(
        QEWorkspaceClient,
        "for_node",
        classmethod(lambda cls, node_id: captured.append(node_id) or cls("https://qe.example")),
    )

    client = QEWorkspaceClient.for_task_loop("qe_task", "Loop8")

    assert captured == ["node-loop"]
    assert cursor.params == ("Loop8", "Loop8", "qe_task_Loop8", "qe_task")
    asyncio.run(client.close())


def test_workspace_client_rejects_task_without_authoritative_node(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.db.pg_pool.get_conn",
        lambda: _Connection(_Cursor({"node_id": None})),
    )

    with pytest.raises(ValueError, match="no authoritative compute node"):
        QEWorkspaceClient.for_task_loop("qe_task", "Loop8")


def test_managed_client_resolves_node_for_every_read(monkeypatch) -> None:
    resolved: list[tuple[str, str | None]] = []

    class _ResolvedClient:
        base_url = "https://node-a/api/v1/qe_workspace"

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            return None

        async def list_workspace_files(self, task_id: str, loop_id: str):
            return {"task_id": task_id, "loop_id": loop_id}

    def resolve(_cls, task_id: str, loop_id: str | None = None):
        resolved.append((task_id, loop_id))
        return _ResolvedClient()

    monkeypatch.setattr(QEWorkspaceClient, "for_task_loop", classmethod(resolve))

    payload = asyncio.run(
        ManagedQEWorkspaceReadClient().list_workspace_files("qe_task", "Loop8")
    )

    assert payload == {"task_id": "qe_task", "loop_id": "Loop8"}
    assert resolved == [("qe_task", "Loop8")]
