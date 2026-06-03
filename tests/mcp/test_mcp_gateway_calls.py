from __future__ import annotations

import asyncio

import httpx
import pytest
from mcp.server.fastmcp.exceptions import ToolError

from backend.mcp.gateway import create_gateway


def _run(coro):
    return asyncio.run(coro)


def test_validation_gateway_health_calls_loopback_validation_api() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url)))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="validation", transport=httpx.MockTransport(handler))
        await mcp.call_tool("health", {})

    _run(exercise())
    assert seen == [("GET", "http://127.0.0.1:8001/api/v1/validation/health")]


def test_qe_gateway_confirmed_run_requires_confirmation_and_uses_backend_path() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url)))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="qe", transport=httpx.MockTransport(handler))
        with pytest.raises(ToolError, match="confirm_run must equal"):
            await mcp.call_tool("qe_experiment_run_confirmed", {"experiment_id": "exp-1"})
        await mcp.call_tool(
            "qe_experiment_run_confirmed",
            {"experiment_id": "exp-1", "node_id": "node-1", "confirm_run": "QE_EXPERIMENT_RUN"},
        )

    _run(exercise())
    assert seen == [("POST", "http://127.0.0.1:8001/api/v1/quantevolver/experiments/exp-1/run?engine_mode=unified&node_id=node-1")]


def test_qe_archive_confirmed_backfill_requires_confirmation_and_uses_backend_path() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url)))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="qe", transport=httpx.MockTransport(handler))
        with pytest.raises(ToolError, match="confirm_backfill must equal"):
            await mcp.call_tool("qe_archive_backfill_execute_confirmed", {})
        await mcp.call_tool("qe_archive_backfill_execute_confirmed", {"confirm_backfill": "QE_ARCHIVE_BACKFILL"})

    _run(exercise())
    assert seen == [("POST", "http://127.0.0.1:8001/api/v1/qe-archive/backfill/execute")]
