from __future__ import annotations

import json
from typing import Any

from backend.services.research_assistant.models import ChatTurnRequest
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.react_grounding import (
    McpToolCall,
    McpToolResult,
    ModelTurn,
    ReactGroundingConfig,
    ToolCatalogEntry,
    ToolGateDecision,
    run_react_grounding_loop,
    tool_result_message,
)
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService, _litellm_compatible_messages


class DeterministicToolLoopLlm:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def complete(self, **kwargs: Any) -> LlmCallResult:
        self.calls.append(kwargs)
        messages = kwargs.get("messages") if isinstance(kwargs.get("messages"), list) else []
        joined = "\n".join(str(item.get("content", "")) for item in messages if isinstance(item, dict))
        if len(self.calls) == 1:
            return LlmCallResult(
                content=json.dumps(
                    {
                        "tool_calls": [
                            {
                                "server_key": "aistock-factor-library",
                                "tool_name": "factor_library_list",
                                "payload_json": {"limit": 3},
                                "stable_call_id": "call_factor_list",
                                "reason": "Need audited factor catalog summary.",
                            }
                        ]
                    }
                ),
                provider="fake",
                model="fake-react",
                duration_ms=1,
                usage={"prompt_tokens": 10, "completion_tokens": 4},
            )
        assert "TOOL_RESULT" in joined
        assert "aistock-factor-library" in joined
        assert "factor_library_list" in joined
        return LlmCallResult(
            content=(
                "thought: keep private\n"
                "observation: tool returned summary\n"
                "Reflexion: retry notes stay in trace\n"
                "Final answer: factor catalog has 1 summary item; "
                "source=research_assistant_catalog_summary_adapter as_of=2026-06-01."
            ),
            provider="fake",
            model="fake-react",
            duration_ms=2,
            usage={"prompt_tokens": 12, "completion_tokens": 8},
        )


def _service(fake: DeterministicToolLoopLlm) -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake)
    svc.seed_catalogs()
    return svc


def test_chat_turn_reacts_tool_result_back_into_messages_before_final_answer() -> None:
    fake = DeterministicToolLoopLlm()
    svc = _service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="List factor library entries as a compact summary."))

    assert len(fake.calls) >= 2
    second_messages = fake.calls[1]["messages"]
    second_joined = "\n".join(str(item.get("content", "")) for item in second_messages if isinstance(item, dict))
    assert "TOOL_RESULT" in second_joined
    assert "factor_library_list" in second_joined
    text = result["assistant_message"]["content_text"]
    assert "source=" not in text
    assert "as_of=" not in text
    assert "summary-first" not in text
    assert "research_assistant_catalog_summary_adapter" not in text
    lowered = text.lower()
    assert "thought:" not in lowered
    assert "observation:" not in lowered
    assert "reflexion:" not in lowered
    assert result["cards"]["react_grounding"]["tool_result_count"] >= 1
    assert result["cards"]["mcp_execution_result"]["auto_executed"] is True
    assert any(event["event_type"] == "mcp_done" for event in result["task_events"])


def test_react_tool_observations_are_not_provider_native_tool_messages() -> None:
    message = tool_result_message(
        McpToolResult(
            server_key="aistock-factor-library",
            tool_name="factor_library_list",
            status="succeeded",
            summary="factor list",
            source_refs=["test://factor-library"],
            as_of="2026-06-01",
            stable_call_id="call_factor_list",
        )
    )

    assert message["role"] == "user"
    assert "TOOL_RESULT" in str(message["content"])
    assert "tool_call_id" not in message


def test_litellm_compatible_messages_wrap_legacy_tool_messages() -> None:
    messages = _litellm_compatible_messages(
        [
            {"role": "system", "content": "system"},
            {"role": "tool", "content": {"type": "TOOL_RESULT", "summary": "legacy observation"}},
            {"role": "user", "content": "continue"},
        ]
    )

    assert [item["role"] for item in messages] == ["system", "user", "user"]
    wrapped = json.loads(str(messages[1]["content"]))
    assert wrapped["type"] == "INTERNAL_TOOL_OBSERVATION"
    assert "legacy observation" in wrapped["content"]
    assert all(item["role"] != "tool" or item.get("tool_call_id") for item in messages)


class _SectionOnlyStockProvider:
    result = McpToolResult(
        server_key="aistock-stock-analysis",
        tool_name="stock_analysis_get_quote",
        status="succeeded",
        payload_json={
            "response_mode": "stock_analysis_evidence_card",
            "sections": [
                {
                    "dataset": "quote",
                    "source_refs": ["stock-ref:quote:000688"],
                    "as_of": "2026-06-16",
                    "summary": "quote evidence",
                },
                {
                    "dataset": "fund_flow",
                    "source_refs": ["stock-ref:fund_flow:000688"],
                    "as_of": "2026-06-16",
                    "summary": "fund-flow evidence",
                },
            ],
        },
        summary="section-only stock evidence",
        executed=True,
    )

    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del call, decision
        return self.result

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        raise AssertionError("BUG-413 stock read-only loop should not request preflight")


class _Bug413RegeneratingLlm:
    def __init__(self) -> None:
        self.calls: list[list[dict[str, Any]]] = []

    def complete(self, messages: list[dict[str, Any]]) -> ModelTurn:
        self.calls.append(messages)
        if len(self.calls) == 1:
            return ModelTurn(
                content=json.dumps(
                    {
                        "tool_calls": [
                            {
                                "server_key": "aistock-stock-analysis",
                                "tool_name": "stock_analysis_get_quote",
                                "payload_json": {"symbol": "000688"},
                                "stable_call_id": "stock_000688",
                                "reason": "Need stock evidence.",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )
        if len(self.calls) == 2:
            return ModelTurn(
                content="国城矿业近期走势需要结合行情和资金流观察，不能只看单一指标。",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )
        joined = "\n".join(str(item.get("content", "")) for item in messages if isinstance(item, dict))
        assert "REACT_EVIDENCE_GUARD_REPAIR_DIRECTIVE" in joined
        assert "missing_inline_tool_evidence" in joined
        assert "stock-ref:quote:000688" in joined
        assert "2026-06-16" in joined
        return ModelTurn(
            content=(
                "国城矿业综合分析：基本情况和近期走势只能基于已返回行情、资金流证据谨慎判断；"
                "未来趋势需继续跟踪基本面和资金变化。"
                "来源 stock-ref:quote:000688，截至 2026-06-16；"
                "来源 stock-ref:fund_flow:000688，截至 2026-06-16。"
            ),
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


def test_bug_413_guard_violation_regenerates_with_exact_citation_options() -> None:
    fake = _Bug413RegeneratingLlm()

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "国城矿业 基本情况/近期走势/未来趋势 全方位分析"}],
        model_complete=fake.complete,
        mcp_provider=_SectionOnlyStockProvider(),
        catalog_entries=[
            ToolCatalogEntry(
                server_key="aistock-stock-analysis",
                tool_name="stock_analysis_get_quote",
                status="enabled",
                risk_level="low",
                side_effect_level="read_only",
            )
        ],
        config=ReactGroundingConfig(
            max_tool_iterations=4,
            user_message="国城矿业 基本情况/近期走势/未来趋势 全方位分析",
        ),
    )

    assert len(fake.calls) == 3
    assert result.evidence_guard.allowed is True
    assert result.evidence_guard.reason == "ok"
    assert result.stopped_reason == "final_answer"
    assert "stock-ref:quote:000688" in result.final_text
    assert any(step.get("evidence_guard_reason") == "missing_inline_tool_evidence" for step in result.trace_steps)
    assert any(step.get("repair") == "regenerate_with_evidence_citation_options" for step in result.trace_steps)
