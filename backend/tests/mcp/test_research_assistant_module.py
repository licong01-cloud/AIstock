"""Contract tests for the Research Assistant MCP module."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx

from backend.mcp.modules import research_assistant
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
        return httpx.Response(200, json={"ok": True, "call": call})

    mcp = FakeMCP()
    registry = ModuleRegistry(
        mcp=mcp,
        base_url="http://127.0.0.1:8011/api/v1",
        env_name="test",
        transport=httpx.MockTransport(handler),
    )
    research_assistant.register(registry)
    return registry, mcp, calls


def test_research_assistant_module_registers_exactly_12_tools() -> None:
    registry, mcp, _calls = _registry_with_capture()

    assert registry.tool_count("research_assistant") == 12
    assert registry.total_tool_count() == 12
    assert set(mcp.tools) == {
        "assistant_health",
        "assistant_create_task",
        "assistant_add_task_event",
        "assistant_chat_turn",
        "assistant_build_prompt_bundle",
        "assistant_list_prompt_nodes",
        "assistant_create_memory_candidate",
        "assistant_build_context_pack",
        "assistant_list_mcp_tools",
        "assistant_preflight_mcp_tool",
        "assistant_list_approvals",
        "assistant_create_temp_memory",
    }


def test_research_assistant_tools_call_expected_http_contracts() -> None:
    _registry, mcp, calls = _registry_with_capture()
    tools = mcp.tools

    tools["assistant_health"]()
    assert calls[-1] == {"method": "GET", "path": "/api/v1/research-assistant/health", "query": {}, "body": {}}

    tools["assistant_create_task"]({"title": "QE 实验规划"})
    assert calls[-1] == {"method": "POST", "path": "/api/v1/research-assistant/tasks", "query": {}, "body": {"title": "QE 实验规划"}}

    tools["assistant_add_task_event"]("rat_1", {"event_type": "planned", "message": "ok"})
    assert calls[-1] == {"method": "POST", "path": "/api/v1/research-assistant/tasks/rat_1/events", "query": {}, "body": {"event_type": "planned", "message": "ok"}}

    tools["assistant_chat_turn"]({"message": "帮我设计一个 QE 实验草案，先不要执行。"})
    assert calls[-1]["method"] == "POST"
    assert calls[-1]["path"] == "/api/v1/research-assistant/chat/turn"
    assert calls[-1]["body"]["message"].startswith("帮我设计一个 QE")

    tools["assistant_build_prompt_bundle"]({"user_message": "QE 实验草案", "phase": "planning"})
    assert calls[-1]["method"] == "POST"
    assert calls[-1]["path"] == "/api/v1/research-assistant/prompt-bundles"

    tools["assistant_list_prompt_nodes"](phase="planning", category="domain", status="enabled", search="QE", limit=8, offset=1)
    assert calls[-1] == {
        "method": "GET",
        "path": "/api/v1/research-assistant/prompt-nodes",
        "query": {"phase": "planning", "category": "domain", "status": "enabled", "search": "QE", "limit": "8", "offset": "1"},
        "body": {},
    }

    tools["assistant_create_memory_candidate"]({"memory_type": "core", "subject_key": "assistant", "title": "记忆"})
    assert calls[-1]["path"] == "/api/v1/research-assistant/memories"
    assert calls[-1]["body"]["approval_status"] == "draft"

    tools["assistant_build_context_pack"]({"task_id": "rat_1", "token_budget": 16000})
    assert calls[-1] == {"method": "POST", "path": "/api/v1/research-assistant/context-packs", "query": {}, "body": {"task_id": "rat_1", "token_budget": 16000}}

    tools["assistant_list_mcp_tools"](server_key="research-assistant", risk_level="high", search="issue", limit=7, offset=2)
    assert calls[-1] == {
        "method": "GET",
        "path": "/api/v1/research-assistant/mcp/tools",
        "query": {"server_key": "research-assistant", "risk_level": "high", "search": "issue", "limit": "7", "offset": "2"},
        "body": {},
    }

    tools["assistant_preflight_mcp_tool"]({"server_key": "aistock-validation", "tool_name": "mcp_github_issue_sync_bug"})
    assert calls[-1]["method"] == "POST"
    assert calls[-1]["path"] == "/api/v1/research-assistant/mcp/preflight"
    assert "assistant_create_issue_candidate" not in tools

    tools["assistant_list_approvals"](status="pending", risk_level="high", limit=3, offset=1)
    assert calls[-1] == {
        "method": "GET",
        "path": "/api/v1/research-assistant/approvals",
        "query": {"status": "pending", "risk_level": "high", "limit": "3", "offset": "1"},
        "body": {},
    }

    tools["assistant_create_temp_memory"]({"task_id": "rat_1", "content_text": "worker progress"})
    assert calls[-1]["path"] == "/api/v1/research-assistant/temp-memories"
