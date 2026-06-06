from __future__ import annotations

import json
from typing import Any

from backend.services.research_assistant.react_grounding import (
    McpToolCall,
    McpToolResult,
    ModelTurn,
    ReactGroundingConfig,
    ToolCatalogEntry,
    ToolGateDecision,
    run_react_grounding_loop,
)


class DeterministicRetryProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        self.calls.append((call.server_key, call.tool_name, call.stable_call_id))
        if call.tool_name == "alpha_fail":
            return McpToolResult(
                server_key=call.server_key,
                tool_name=call.tool_name,
                status="failed",
                summary="deterministic failure",
                error_json={"code": "fixture_fail"},
                executed=True,
            )
        return McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status="succeeded",
            summary="deterministic success",
            source_refs=["test://beta"],
            as_of="2026-06-01",
            executed=True,
        )

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        raise AssertionError("retry fixture uses read-only tools only")


def test_reflexion_retry_uses_deterministic_fake_llm_and_provider() -> None:
    provider = DeterministicRetryProvider()
    scripted = [
        ModelTurn(
            content=json.dumps(
                {
                    "tool_calls": [
                        {"server_key": "server", "tool_name": "zeta_ok", "stable_call_id": "b", "payload_json": {}},
                        {"server_key": "server", "tool_name": "alpha_fail", "stable_call_id": "a", "payload_json": {}},
                    ]
                }
            ),
            provider="fake",
            model="fake",
            duration_ms=1,
            usage={},
        ),
        ModelTurn(
            content=json.dumps(
                {
                    "tool_calls": [
                        {"server_key": "server", "tool_name": "beta_ok", "stable_call_id": "c", "payload_json": {}}
                    ]
                }
            ),
            provider="fake",
            model="fake",
            duration_ms=1,
            usage={},
        ),
        ModelTurn(
            content="Retry succeeded with 1 sourced result; source=test://beta as_of=2026-06-01.",
            provider="fake",
            model="fake",
            duration_ms=1,
            usage={},
        ),
    ]
    seen_messages: list[str] = []

    def model_complete(messages: list[dict[str, Any]]) -> ModelTurn:
        seen_messages.append("\n".join(str(item.get("content", "")) for item in messages if isinstance(item, dict)))
        return scripted.pop(0)

    catalog = [
        ToolCatalogEntry(server_key="server", tool_name="alpha_fail", status="enabled", risk_level="low", side_effect_level="read_only"),
        ToolCatalogEntry(server_key="server", tool_name="beta_ok", status="enabled", risk_level="low", side_effect_level="read_only"),
        ToolCatalogEntry(server_key="server", tool_name="zeta_ok", status="enabled", risk_level="low", side_effect_level="read_only"),
    ]

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "retry deterministically"}],
        model_complete=model_complete,
        mcp_provider=provider,
        catalog_entries=catalog,
        config=ReactGroundingConfig(max_tool_iterations=4),
    )

    assert provider.calls == [("server", "alpha_fail", "a"), ("server", "zeta_ok", "b"), ("server", "beta_ok", "c")]
    assert any("REACT_RETRY_DIRECTIVE" in text for text in seen_messages)
    assert result.evidence_guard.allowed is True
    assert "source=test://beta" in result.final_text
