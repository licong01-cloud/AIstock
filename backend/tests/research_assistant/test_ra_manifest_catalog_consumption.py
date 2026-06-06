from __future__ import annotations

import json
from typing import Any

import pytest

from backend.mcp.tool_manifest import TOOL_MANIFEST, TOOL_MANIFEST_BY_NAME
from backend.services.research_assistant.mcp_catalog_sync import (
    canonicalize_server_key,
    default_mcp_servers,
    default_mcp_tools,
    gateway_catalog,
)
from backend.services.research_assistant.models import ChatTurnRequest, McpPreflightRequest
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService


class _SingleToolCallLlm:
    def __init__(self, *, server_key: str, tool_name: str) -> None:
        self.server_key = server_key
        self.tool_name = tool_name
        self.calls: list[dict[str, Any]] = []

    def complete(self, **kwargs: Any) -> LlmCallResult:
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return LlmCallResult(
                content=json.dumps(
                    {
                        "tool_calls": [
                            {
                                "server_key": self.server_key,
                                "tool_name": self.tool_name,
                                "payload_json": {"limit": 1},
                                "stable_call_id": f"call:{self.tool_name}",
                                "reason": "phase5a catalog consumption assertion",
                            }
                        ]
                    }
                ),
                provider="fake",
                model="fake-react",
                duration_ms=1,
                usage={},
            )
        return LlmCallResult(
            content="Grounded result source=test://phase5a as_of=2026-06-04.",
            provider="fake",
            model="fake-react",
            duration_ms=1,
            usage={},
        )


def _seeded_service(*, llm_client: Any | None = None) -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=llm_client)
    svc.seed_catalogs()
    return svc


def _add_legacy_alias_cache_rows(svc: ResearchAssistantService) -> None:
    svc.repository.create_record(
        "mcp_servers",
        {
            "server_id": "mcp_server_aistock_qe_archive_legacy",
            "server_key": "aistock-qe-archive",
            "title": "Legacy QE archive MCP",
            "status": "ready",
            "health_json": {"legacy_alias": True},
        },
    )
    svc.repository.create_record(
        "mcp_tools",
        {
            "tool_id": "mcp_tool_aistock_qe_archive_qe_archive_query_run_leaderboard_legacy",
            "server_key": "aistock-qe-archive",
            "tool_name": "qe_archive_query_run_leaderboard",
            "title": "legacy qe archive leaderboard",
            "description": "legacy alias cache row that must not become catalog truth",
            "risk_level": "low",
            "side_effect_level": "read_only",
            "requires_approval": False,
            "input_schema_json": {"type": "object"},
            "output_schema_json": {"type": "object"},
            "preflight_schema_json": {},
            "required_confirmations": [],
            "status": "enabled",
        },
    )


def test_ra_catalog_matches_gateway_manifest_without_legacy_drift() -> None:
    svc = _seeded_service()
    catalog = gateway_catalog()
    tools = default_mcp_tools()
    servers = default_mcp_servers()

    assert len(tools) == len(TOOL_MANIFEST) == 209
    assert len(servers) == 9
    assert {tool["tool_name"] for tool in tools} == {entry.tool_name for entry in TOOL_MANIFEST}
    assert {tool["server_key"] for tool in tools} <= set(catalog.server_key_to_modules)
    assert not {"aistock-qe-archive", "aistock-factor-library", "aistock-execution-policy"} & {tool["server_key"] for tool in tools}

    page = svc.list_mcp_tools(limit=500)
    assert page["source"] == "gateway_manifest_derived_catalog"
    assert page["manifest_tool_count"] == len(TOOL_MANIFEST)
    assert page["total"] == len(TOOL_MANIFEST)
    react_pairs = {(entry.server_key, entry.tool_name) for entry in svc._react_tool_catalog_entries()}
    page_pairs = {(tool["server_key"], tool["tool_name"]) for tool in page["items"]}
    assert react_pairs == page_pairs


def test_server_key_aliases_canonicalize_and_runtime_overlay_cannot_lower_risk() -> None:
    svc = _seeded_service()
    assert canonicalize_server_key("aistock-qe-archive") == "aistock-qe"

    tool, server = svc._resolve_mcp_catalog_tool("aistock-qe-archive", "qe_archive_query_run_leaderboard")
    assert tool["server_key"] == "aistock-qe"
    assert tool["canonical_server_key"] == "aistock-qe"
    assert tool["legacy_server_alias"] == "aistock-qe-archive"
    assert server["server_key"] == "aistock-qe"

    risky = TOOL_MANIFEST_BY_NAME["mcp_github_issue_create"]
    svc.repository.create_record(
        "mcp_tools",
        {
            "tool_id": "mcp_tool_aistock_validation_mcp_github_issue_create_overlay",
            "server_key": "aistock-validation",
            "tool_name": risky.tool_name,
            "title": risky.tool_name,
            "description": "malformed runtime overlay attempts to lower risk",
            "risk_level": "low",
            "side_effect_level": "read_only",
            "requires_approval": False,
            "input_schema_json": {"type": "object"},
            "output_schema_json": {"type": "object"},
            "preflight_schema_json": {},
            "required_confirmations": [],
            "status": "enabled",
        },
    )
    tool, _server = svc._resolve_mcp_catalog_tool("aistock-validation", risky.tool_name)
    assert tool["manifest_risk_level"] == "external_network"
    assert tool["risk_level"] == "high"
    assert tool["side_effect_level"] == "draft_only"
    assert tool["requires_approval"] is True


def test_catalog_readiness_uses_manifest_counts_not_legacy_cache_rows() -> None:
    svc = _seeded_service()
    _add_legacy_alias_cache_rows(svc)

    readiness = svc.catalog_readiness()
    checks = {item["catalog"]: item for item in readiness["checks"]}

    assert readiness["ready"] is True
    assert checks["mcp_servers"]["source"] == "gateway_manifest_derived_catalog"
    assert checks["mcp_servers"]["present"] == len(default_mcp_servers()) == 9
    assert checks["mcp_tools"]["source"] == "gateway_manifest_derived_catalog"
    assert checks["mcp_tools"]["present"] == len(TOOL_MANIFEST) == 209


def test_assistant_list_mcp_tools_summary_adapter_uses_manifest_not_legacy_cache() -> None:
    svc = _seeded_service()
    _add_legacy_alias_cache_rows(svc)
    tool, _server = svc._resolve_mcp_catalog_tool("research-assistant", "assistant_list_mcp_tools")

    all_items, all_total = svc._summary_adapter_items(tool, {}, limit=500, offset=0)
    assert all_total == len(TOOL_MANIFEST) == 209
    assert not any(item["server_key"] == "aistock-qe-archive" for item in all_items)

    alias_items, alias_total = svc._summary_adapter_items(
        tool,
        {"server_key": "aistock-qe-archive", "search": "leaderboard"},
        limit=20,
        offset=0,
    )
    assert alias_total == 1
    assert alias_items[0]["server_key"] == "aistock-qe"
    assert alias_items[0]["tool_name"] == "qe_archive_query_run_leaderboard"


@pytest.mark.parametrize(
    ("server_key", "tool_name"),
    [
        ("aistock-qe", "qe_archive_query_run_leaderboard"),
        ("aistock-local-data", "local_data_list_sync_targets"),
        ("aistock-validation", "list_validation_runs"),
        ("aistock-external-research", "external_research_search_web"),
        ("aistock-external-research", "external_research_search_papers"),
        ("aistock-external-research", "external_research_fetch_extract"),
    ],
)
def test_a2_and_external_read_only_tools_map_to_auto_executable_entries(server_key: str, tool_name: str) -> None:
    svc = _seeded_service()
    tool, _server = svc._resolve_mcp_catalog_tool(server_key, tool_name)
    assert tool["risk_level"] == "low"
    assert tool["side_effect_level"] == "read_only"
    assert tool["requires_approval"] is False

    entry = next(item for item in svc._react_tool_catalog_entries() if item.server_key == tool["server_key"] and item.tool_name == tool_name)
    assert entry.risk_level == "low"
    assert entry.side_effect_level == "read_only"
    assert entry.requires_approval is False


def test_preflight_uses_manifest_risk_and_canonical_server_key() -> None:
    svc = _seeded_service()
    readonly = svc.preflight_mcp_tool(
        McpPreflightRequest(server_key="aistock-qe-archive", tool_name="qe_archive_query_run_leaderboard", payload_json={"limit": 1})
    )
    assert readonly["passed"] is True
    assert readonly["requires_approval"] is False
    assert readonly["server_key"] == "aistock-qe"
    assert readonly["requested_server_key"] == "aistock-qe-archive"
    assert readonly["catalog_source"] == "gateway_manifest_derived_catalog"

    high_risk = svc.preflight_mcp_tool(
        McpPreflightRequest(server_key="aistock-external-research", tool_name="external_research_save_evidence", payload_json={})
    )
    assert high_risk["passed"] is False
    assert high_risk["approval_required"] is True
    assert high_risk["assistant_usable"] == "preflight_required"
    assert high_risk["side_effect_level"] == "draft_only"


@pytest.mark.parametrize(
    ("message", "server_key", "tool_name"),
    [
        ("Query QE archive run leaderboard.", "aistock-qe", "qe_archive_query_run_leaderboard"),
        ("Search external research web.", "aistock-external-research", "external_research_search_web"),
        ("Search external research papers.", "aistock-external-research", "external_research_search_papers"),
        ("Fetch and extract external evidence.", "aistock-external-research", "external_research_fetch_extract"),
    ],
)
def test_read_only_tools_enter_execute_read_only_path(message: str, server_key: str, tool_name: str) -> None:
    fake = _SingleToolCallLlm(server_key=server_key, tool_name=tool_name)
    svc = _seeded_service(llm_client=fake)
    result = svc.chat_turn(ChatTurnRequest(message=message, dialogue_mode_override="analysis"))

    assert result["cards"]["mcp_execution_result"]["auto_executed"] is True
    assert result["cards"]["mcp_execution_result"]["status"] == "succeeded"
    assert result["cards"]["mcp_execution_result"]["tool_name"] == tool_name
    assert result["cards"]["mcp_execution_result"]["server_key"] == server_key
    assert len(fake.calls) >= 2


def test_external_save_evidence_remains_preflight_not_auto_execute() -> None:
    fake = _SingleToolCallLlm(server_key="aistock-external-research", tool_name="external_research_save_evidence")
    svc = _seeded_service(llm_client=fake)
    result = svc.chat_turn(ChatTurnRequest(message="Save external evidence candidate.", dialogue_mode_override="analysis"))

    execution = result["cards"]["mcp_execution_result"]
    assert execution["auto_executed"] is False
    assert execution["status"] in {"approval_required", "preflight_required", "preflight_failed"}
    assert result["cards"]["action_proposals"]


def test_chat_turn_does_not_spawn_cli_or_full_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    blocked_calls: list[Any] = []

    real_popen = __import__("subprocess").Popen

    def guarded_popen(*args: Any, **kwargs: Any) -> Any:
        command = args[0] if args else kwargs.get("args")
        rendered = " ".join(str(part) for part in command) if isinstance(command, (list, tuple)) else str(command)
        if any(token in rendered.lower() for token in ("claude", "codex", "aistock_mcp_gateway.py", "--profile=full")):
            blocked_calls.append((args, kwargs))
            raise AssertionError("chat_turn must not spawn MCP gateway, Claude Code, Codex, or full profile CLI")
        return real_popen(*args, **kwargs)

    monkeypatch.setattr("subprocess.Popen", guarded_popen)
    fake = _SingleToolCallLlm(server_key="aistock-local-data", tool_name="local_data_list_sync_targets")
    svc = _seeded_service(llm_client=fake)
    result = svc.chat_turn(ChatTurnRequest(message="List local data sync targets.", dialogue_mode_override="analysis"))

    assert blocked_calls == []
    assert result["cards"]["mcp_execution_result"]["auto_executed"] is True



