from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from backend.mcp.modules import external_research
from backend.mcp.registry import ModuleRegistry


class FakeMCP:
    def __init__(self) -> None:
        self.tools: dict[str, Callable[..., Any]] = {}

    def tool(self, name: str | None = None, **_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.tools[name or func.__name__] = func
            return func

        return decorator


def _decode_json_body(request: httpx.Request) -> dict[str, Any]:
    if not request.content:
        return {}
    return json.loads(request.content.decode("utf-8"))


def _registry_with_capture() -> tuple[ModuleRegistry, FakeMCP, list[dict[str, Any]]]:
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call = {
            "method": request.method,
            "path": request.url.path,
            "query": dict(request.url.params),
            "body": _decode_json_body(request),
        }
        calls.append(call)
        return httpx.Response(200, json={"ok": True, "summary_first": True, "path": call["path"]})

    mcp = FakeMCP()
    registry = ModuleRegistry(
        mcp=mcp,
        base_url="http://127.0.0.1:8001/api/v1",
        env_name="test",
        transport=httpx.MockTransport(handler),
    )
    external_research.register(registry)
    return registry, mcp, calls


def test_external_research_module_registers_four_tools_and_calls_facade_only() -> None:
    registry, mcp, calls = _registry_with_capture()

    assert external_research.TOOL_COUNT == 4
    assert registry.tool_count("external_research") == 4
    assert set(mcp.tools) == set(external_research.TOOL_NAMES)

    mcp.tools["external_research_search_web"]("HMM factor", limit=2)
    mcp.tools["external_research_search_papers"]("factor timing", provider="paper_search", limit=2)
    mcp.tools["external_research_fetch_extract"]("https://example.org/paper", max_chars=500)
    mcp.tools["external_research_save_evidence"](
        {
            "evidence": {"source": "paper", "url": "https://example.org/paper", "as_of": "2026-06-01", "evidence_ref": "ref"},
            "target_branch": "external.factor.hmm",
        }
    )

    assert [call["path"] for call in calls] == [
        "/api/v1/external-research/search-web",
        "/api/v1/external-research/search-papers",
        "/api/v1/external-research/fetch-extract",
        "/api/v1/external-research/save-evidence-candidate",
    ]
    assert all(call["method"] == "POST" for call in calls)
    assert calls[0]["body"]["limit"] == 2
    assert calls[3]["body"]["target_branch"] == "external.factor.hmm"


def test_external_research_list_tools_smoke_uses_static_introspection_not_8001() -> None:
    result = subprocess.run(
        [sys.executable, "debug_tools/mcp/list_tools_smoke.py", "--server", "aistock-external-research"],
        cwd=Path(__file__).resolve().parents[3],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["server"] == "aistock-external-research"
    assert payload["tool_count"] == 4
    assert set(payload["tools"]) == set(external_research.TOOL_NAMES)
    assert payload["introspection_mode"] == "static_in_process"
    assert payload["production_8001_touched"] is False
