from __future__ import annotations

import json
from typing import Any

from backend.services.research_assistant.models import ChatTurnRequest
from backend.services.research_assistant.react_grounding import (
    McpToolCall,
    McpToolResult,
    ModelTurn,
    ReactGroundingConfig,
    ToolGateDecision,
    run_react_grounding_loop,
)
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService, _extract_litellm_tool_calls


class RecordingProvider:
    def __init__(self) -> None:
        self.executed: list[McpToolCall] = []
        self.preflighted: list[McpToolCall] = []

    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        self.executed.append(call)
        return McpToolResult(server_key=call.server_key, tool_name=call.tool_name, status="succeeded", summary="ok", source_refs=["test://ok"], as_of="2026-06-01")

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        self.preflighted.append(call)
        return McpToolResult(server_key=call.server_key, tool_name=call.tool_name, status="preflight_required", summary="preflight", source_refs=["test://preflight"], as_of="2026-06-01")


def test_catalog_outside_tool_is_rejected_and_not_executed() -> None:
    provider = RecordingProvider()
    calls = [
        ModelTurn(
            content=json.dumps(
                {
                    "tool_calls": [
                        {"server_key": "ghost", "tool_name": "missing", "payload_json": {}, "stable_call_id": "z"}
                    ]
                }
            ),
            provider="fake",
            model="fake",
            duration_ms=1,
            usage={},
        ),
        ModelTurn(content="No valid tool was available.", provider="fake", model="fake", duration_ms=1, usage={}),
    ]

    def model_complete(messages: list[dict[str, Any]]) -> ModelTurn:
        return calls.pop(0)

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "call a missing tool"}],
        model_complete=model_complete,
        mcp_provider=provider,
        catalog_entries=[],
        config=ReactGroundingConfig(max_tool_iterations=2),
    )

    assert provider.executed == []
    assert provider.preflighted == []
    assert result.tool_results[0].status == "rejected"
    assert result.tool_results[0].executed is False


def test_native_function_tool_calls_are_parsed_into_mcp_calls() -> None:
    class NativeMessage:
        tool_calls = [
            {
                "id": "native-call-1",
                "function": {
                    "name": "stock_analysis_get_quote",
                    "arguments": "{\"symbol\":\"600584\",\"analysis_date\":\"2026-06-16\"}",
                },
            }
        ]

    calls = _extract_litellm_tool_calls(
        NativeMessage(),
        {"stock_analysis_get_quote": {"server_key": "aistock-stock-analysis", "tool_name": "stock_analysis_get_quote"}},
    )

    assert len(calls) == 1
    assert calls[0].server_key == "aistock-stock-analysis"
    assert calls[0].tool_name == "stock_analysis_get_quote"
    assert calls[0].payload_json == {"symbol": "600584", "analysis_date": "2026-06-16"}
    assert calls[0].reason == "native_function_call:stock_analysis_get_quote"


class HighRiskToolLlm:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def complete(self, **kwargs: Any) -> LlmCallResult:
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return LlmCallResult(
                content=json.dumps(
                    {
                        "tool_calls": [
                            {
                                "server_key": "aistock-validation",
                                "tool_name": "mcp_github_issue_sync_bug",
                                "payload_json": {"bug_id": "BUG-120"},
                                "stable_call_id": "call_sync_bug",
                            }
                        ]
                    }
                ),
                provider="fake",
                model="fake",
                duration_ms=1,
                usage={},
            )
        return LlmCallResult(
            content="Preflight confirmation card is ready; source=preflight as_of=2026-06-01.",
            provider="fake",
            model="fake",
            duration_ms=1,
            usage={},
        )


class GuardedResearchAssistantService(ResearchAssistantService):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.execute_calls: list[tuple[Any, Any]] = []

    def execute_action_proposal(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.execute_calls.append((args, kwargs))
        raise AssertionError("execute_action_proposal must not run inside ReAct for high-risk tools")


def test_high_risk_tool_creates_preflight_card_without_execute() -> None:
    fake = HighRiskToolLlm()
    svc = GuardedResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake)
    svc.seed_catalogs()

    result = svc.chat_turn(ChatTurnRequest(message="Sync BUG-120 GitHub issue status"))

    assert svc.execute_calls == []
    execution = result["cards"]["mcp_execution_result"]
    assert execution["auto_executed"] is False
    assert execution["status"] in {"approval_required", "preflight_required"}
    assert execution["action_proposal_id"]
    assert result["cards"]["action_proposals"]
    events = svc.repository.list_records("mcp_tool_events", limit=100)["items"]
    assert {event["event_type"] for event in events}.issubset({"preflight"})
    assert svc.repository.list_records("action_proposals", limit=100)["total"] == 1
