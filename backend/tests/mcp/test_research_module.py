"""Contract tests for the Research Pipeline MCP module."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from backend.mcp.modules import research
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
    research.register(registry)
    return registry, mcp, calls


def test_research_module_registers_exactly_12_tools() -> None:
    registry, mcp, _calls = _registry_with_capture()

    assert registry.tool_count("research") == 12
    assert registry.total_tool_count() == 12
    assert set(mcp.tools) == {
        "research_create_experiment",
        "research_list_experiments",
        "research_get_experiment",
        "research_run_stage",
        "research_retry_stage",
        "research_get_stage_result",
        "research_compare_baseline",
        "research_list_artifact_refs",
        "research_get_pipeline_types",
        "research_create_issue",
        "research_promote",
        "research_reject",
    }


def test_research_tools_call_expected_http_contracts() -> None:
    _registry, mcp, calls = _registry_with_capture()
    tools = mcp.tools

    assert tools["research_create_experiment"]({"name": "hmm", "pipeline_type": "hmm_research"})["ok"] is True
    assert calls[-1] == {
        "method": "POST",
        "path": "/api/v1/research-pipeline/experiments",
        "query": {},
        "body": {"name": "hmm", "pipeline_type": "hmm_research"},
    }

    tools["research_list_experiments"](
        status="running",
        pipeline_type="hmm_research",
        search="flow",
        limit=7,
        offset=3,
    )
    assert calls[-1] == {
        "method": "GET",
        "path": "/api/v1/research-pipeline/experiments",
        "query": {"status": "running", "pipeline_type": "hmm_research", "search": "flow", "limit": "7", "offset": "3"},
        "body": {},
    }

    tools["research_get_experiment"]("exp_1")
    assert calls[-1]["method"] == "GET"
    assert calls[-1]["path"] == "/api/v1/research-pipeline/experiments/exp_1"

    tools["research_run_stage"](
        "exp_1",
        "offline_validation",
        payload={"reason": "dogfood"},
        confirm=research.RESEARCH_RUN_STAGE_CONFIRM,
    )
    assert calls[-1] == {
        "method": "POST",
        "path": "/api/v1/research-pipeline/experiments/exp_1/stages/offline_validation/run",
        "query": {},
        "body": {"reason": "dogfood", "confirm": research.RESEARCH_RUN_STAGE_CONFIRM},
    }

    tools["research_retry_stage"](
        "exp_1",
        "qe_shadow",
        payload={"attempt_reason": "transient backend"},
        confirm=research.RESEARCH_RETRY_STAGE_CONFIRM,
    )
    assert calls[-1] == {
        "method": "POST",
        "path": "/api/v1/research-pipeline/experiments/exp_1/stages/qe_shadow/retry",
        "query": {},
        "body": {"attempt_reason": "transient backend", "confirm": research.RESEARCH_RETRY_STAGE_CONFIRM},
    }

    tools["research_get_stage_result"]("exp_1", "qe_shadow")
    assert calls[-1]["method"] == "GET"
    assert calls[-1]["path"] == "/api/v1/research-pipeline/experiments/exp_1/stages/qe_shadow"

    tools["research_compare_baseline"]("exp_1", {"baseline": "v25_1"})
    assert calls[-1] == {
        "method": "POST",
        "path": "/api/v1/research-pipeline/experiments/exp_1/compare",
        "query": {},
        "body": {"baseline": "v25_1"},
    }

    tools["research_list_artifact_refs"]("exp_1", domain_type="model", status="candidate", limit=5)
    assert calls[-1] == {
        "method": "GET",
        "path": "/api/v1/research-pipeline/experiments/exp_1/artifact-refs",
        "query": {"domain_type": "model", "status": "candidate", "limit": "5"},
        "body": {},
    }

    tools["research_get_pipeline_types"]()
    assert calls[-1]["method"] == "GET"
    assert calls[-1]["path"] == "/api/v1/research-pipeline/pipeline-types"

    tools["research_create_issue"]({"title": "Research blocked", "severity": "medium"})
    assert calls[-1] == {
        "method": "POST",
        "path": "/api/v1/research-pipeline/issues",
        "query": {},
        "body": {"title": "Research blocked", "severity": "medium"},
    }

    tools["research_promote"](
        "exp_1",
        "https://github.com/example/repo/issues/1",
        payload={"target": "candidate"},
        confirm=research.RESEARCH_PROMOTE_CONFIRM,
    )
    assert calls[-1] == {
        "method": "POST",
        "path": "/api/v1/research-pipeline/experiments/exp_1/promote",
        "query": {},
        "body": {
            "target": "candidate",
            "issue_url": "https://github.com/example/repo/issues/1",
            "confirm": research.RESEARCH_PROMOTE_CONFIRM,
        },
    }

    tools["research_reject"]("exp_1", {"reason": "underperformed baseline"})
    assert calls[-1] == {
        "method": "POST",
        "path": "/api/v1/research-pipeline/experiments/exp_1/reject",
        "query": {},
        "body": {"reason": "underperformed baseline"},
    }


@pytest.mark.parametrize(
    ("tool_name", "args", "kwargs", "expected"),
    [
        (
            "research_run_stage",
            ("exp_1", "offline_validation"),
            {},
            research.RESEARCH_RUN_STAGE_CONFIRM,
        ),
        (
            "research_retry_stage",
            ("exp_1", "offline_validation"),
            {},
            research.RESEARCH_RETRY_STAGE_CONFIRM,
        ),
        (
            "research_promote",
            ("exp_1", "https://github.com/example/repo/issues/1"),
            {},
            research.RESEARCH_PROMOTE_CONFIRM,
        ),
    ],
)
def test_confirmed_research_tools_reject_before_http(
    tool_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    expected: str,
) -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError, match=expected):
        mcp.tools[tool_name](*args, **kwargs)

    assert calls == []


@pytest.mark.parametrize(
    ("tool_name", "args", "kwargs"),
    [
        ("research_get_experiment", ("../exp_1",), {}),
        ("research_get_stage_result", ("exp_1", "stage/name"), {}),
        (
            "research_run_stage",
            ("exp_1", "../stage"),
            {"confirm": research.RESEARCH_RUN_STAGE_CONFIRM},
        ),
        ("research_list_artifact_refs", ("exp 1",), {}),
    ],
)
def test_research_tools_reject_unsafe_path_fragments_before_http(
    tool_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError):
        mcp.tools[tool_name](*args, **kwargs)

    assert calls == []


def test_research_promote_requires_issue_url_before_http() -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError, match="issue_url"):
        mcp.tools["research_promote"](
            "exp_1",
            "",
            confirm=research.RESEARCH_PROMOTE_CONFIRM,
        )

    assert calls == []


def test_research_tool_surfaces_backend_error_without_fallback() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="research backend unavailable")

    mcp = FakeMCP()
    registry = ModuleRegistry(
        mcp=mcp,
        base_url="http://127.0.0.1:8011/api/v1",
        env_name="test",
        transport=httpx.MockTransport(handler),
    )
    research.register(registry)

    with pytest.raises(RuntimeError) as exc_info:
        mcp.tools["research_get_pipeline_types"]()

    message = str(exc_info.value)
    assert "GET" in message
    assert "/pipeline-types" in message
    assert "503" in message
    assert "research backend unavailable" in message
