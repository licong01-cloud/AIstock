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


def test_research_module_registers_exactly_16_tools() -> None:
    registry, mcp, _calls = _registry_with_capture()

    assert registry.tool_count("research") == 16
    assert registry.total_tool_count() == 16
    assert set(mcp.tools) == {
        "research_create_experiment",
        "research_list_experiments",
        "research_get_experiment",
        "research_run_stage",
        "research_retry_stage",
        "research_get_stage_result",
        "research_compare_baseline",
        "research_list_artifact_refs",
        "research_list_backtest_records",
        "research_hmm_backfill_preview",
        "research_hmm_backfill_execute",
        "research_get_backfill_run",
        "research_get_pipeline_types",
        "research_create_issue",
        "research_promote",
        "research_reject",
    }


def test_research_mcp_uses_compact_defaults_and_refines_large_payloads() -> None:
    registry, mcp, calls = _registry_with_capture()
    tools = mcp.tools

    tools["research_list_experiments"]()
    assert calls[-1]["query"]["limit"] == "20"

    tools["research_list_artifact_refs"]("exp_1")
    assert calls[-1]["query"]["limit"] == "20"

    tools["research_list_backtest_records"]("exp_1")
    assert calls[-1]["query"]["limit"] == "10"
    assert calls[-1]["query"]["detail"] == "summary"

    large = {"data": "x" * 200}

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=large)

    client = registry.client("research-pipeline")
    client.max_response_bytes = 64
    client._transport = httpx.MockTransport(handler)
    result = client.get("/experiments")

    assert result["status"] == "requires_refinement"
    assert result["mcp_response_too_large"] is True
    assert result["mcp_response_refinement_required"] is True
    assert result["partial_payload_returned"] is False
    assert result["path"] == "/experiments"
    assert "preview" not in result
    assert result["retry_with"]["params"]["limit"] == 20
    assert result["retry_with"]["params"]["detail"] == "summary"


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
    assert calls[-1]["query"] == {"detail": "summary"}

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
    assert calls[-1]["query"] == {"detail": "summary"}

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

    tools["research_list_backtest_records"](
        "exp_1",
        research_domain="hmm",
        dedup_status="primary",
        qe_archive_representative=True,
        source_task_id="task_1",
        hmm_config_sig="hmm_sig",
        non_hmm_config_sig="base_sig",
        limit=9,
        offset=2,
    )
    assert calls[-1] == {
        "method": "GET",
        "path": "/api/v1/research-pipeline/experiments/exp_1/backtest-records",
        "query": {
            "research_domain": "hmm",
            "dedup_status": "primary",
            "qe_archive_representative": "true",
            "source_task_id": "task_1",
            "hmm_config_sig": "hmm_sig",
            "non_hmm_config_sig": "base_sig",
            "limit": "9",
            "offset": "2",
            "detail": "summary",
        },
        "body": {},
    }

    tools["research_hmm_backfill_preview"](
        "exp_1",
        {
            "source_mode": "historical_file",
            "source_scope": {"path": "research/hmm", "source_file": "docs/archive.json"},
            "policy": {"dedup": "strict"},
            "created_by": "codex",
        },
    )
    assert calls[-1] == {
        "method": "POST",
        "path": "/api/v1/research-pipeline/experiments/exp_1/hmm-backtests/backfill-preview",
        "query": {},
        "body": {
            "source_mode": "historical_file",
            "source_scope": {"path": "research/hmm", "source_file": "docs/archive.json"},
            "policy": {"dedup": "strict"},
            "created_by": "codex",
        },
    }

    tools["research_hmm_backfill_execute"](
        "exp_1",
        payload={"preview_id": "preview_1", "dry_run": False},
        confirm=research.RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM,
    )
    assert calls[-1] == {
        "method": "POST",
        "path": "/api/v1/research-pipeline/experiments/exp_1/hmm-backtests/backfill-execute",
        "query": {},
        "body": {
            "preview_id": "preview_1",
            "dry_run": False,
            "confirm": research.RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM,
        },
    }

    tools["research_hmm_backfill_execute"](
        "exp_1",
        payload={"confirm": "WRONG", "dry_run": True},
        confirm=research.RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM,
    )
    assert calls[-1]["body"] == {"confirm": research.RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM, "dry_run": True}

    tools["research_get_backfill_run"]("rp_bf_1")
    assert calls[-1]["method"] == "GET"
    assert calls[-1]["path"] == "/api/v1/research-pipeline/backfill-runs/rp_bf_1"

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
        (
            "research_hmm_backfill_execute",
            ("exp_1",),
            {},
            research.RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM,
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
        ("research_list_backtest_records", ("exp/1",), {}),
        ("research_hmm_backfill_preview", ("../exp_1",), {}),
        (
            "research_hmm_backfill_execute",
            ("exp 1",),
            {"confirm": research.RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM},
        ),
        ("research_get_backfill_run", ("rp_bf/1",), {}),
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


def test_research_backfill_tools_reject_source_file_outside_project_before_http() -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError, match="source_file"):
        mcp.tools["research_hmm_backfill_preview"](
            "exp_1",
            {"source_scope": {"source_file": "C:/outside/archive.json"}},
        )

    with pytest.raises(ValueError, match="source_file"):
        mcp.tools["research_hmm_backfill_execute"](
            "exp_1",
            payload={"source_scope": {"source_file": "C:/outside/archive.json"}},
            confirm=research.RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM,
        )

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
