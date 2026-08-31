from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from typing import Any

import httpx

from backend.mcp.common import AIstockApiClient
from backend.mcp.modules import qe_archive
from backend.mcp.registry import ModuleRegistry


class _FakeMCP:
    def __init__(self) -> None:
        self.tools: dict[str, Callable[..., Any]] = {}

    def tool(
        self,
        name: str | None = None,
        **_kwargs: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.tools[name or func.__name__] = func
            return func

        return decorator


def _registered_tools() -> tuple[dict[str, Callable[..., Any]], list[dict[str, Any]]]:
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(
            {
                "method": request.method,
                "path": request.url.path,
                "headers": dict(request.headers),
                "body": json.loads(request.content.decode("utf-8")) if request.content else {},
            }
        )
        return httpx.Response(200, json={"status": "success", "data": {"ok": True}})

    mcp = _FakeMCP()
    registry = ModuleRegistry(
        mcp=mcp,
        base_url="http://127.0.0.1:8001/api/v1",
        env_name="test",
        transport=httpx.MockTransport(handler),
    )
    qe_archive.register(registry)
    return mcp.tools, calls


def test_durable_recovery_mcp_reuses_preview_idempotency_without_global_client_change() -> None:
    tools, calls = _registered_tools()

    tools["multi_alpha_combine_backtest_recovery_preview"](
        run_id="macb_source",
        child_id="macbc_target",
        retry_mode="backtest_only",
        idempotency_key="stable-recovery-key",
    )
    tools["multi_alpha_combine_backtest_child_recovery_execute"](
        run_id="macb_source",
        child_id="macbc_target",
        retry_mode="backtest_only",
        scope_hash="b" * 64,
        preview_command_id="macmd_" + "a" * 64,
        idempotency_key="stable-recovery-key",
    )

    assert [call["headers"]["idempotency-key"] for call in calls] == [
        "stable-recovery-key",
        "stable-recovery-key",
    ]
    assert calls[1]["body"]["preview_command_id"] == "macmd_" + "a" * 64
    assert "headers" not in inspect.signature(AIstockApiClient.request).parameters
    assert "headers" not in inspect.signature(AIstockApiClient.post).parameters
