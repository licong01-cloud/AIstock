from __future__ import annotations

import asyncio

import httpx
import pytest

from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceCatalogUnavailable,
    QEWorkspaceClient,
)


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
