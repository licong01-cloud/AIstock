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
    compose_with_evidence_guard,
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

    result = svc.chat_turn(ChatTurnRequest(developer_diagnostics=True, message="List factor library entries as a compact summary."))

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


class _SeededMultiToolProvider:
    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del decision
        payloads = {
            "qe_archive_query_promotion_candidates": {
                "response_mode": "qe_warehouse_business_summary",
                "source": "qe_archive_read_adapter",
                "as_of": "2026-06-17",
                "items": [{"factor_set_hash": "fs_qe_promoted", "passes_gate": True}],
            },
            "strategy_governance_list_packages": {
                "response_mode": "summary",
                "source": "research_assistant_catalog_summary_adapter",
                "as_of": "2026-06-17",
                "items": [{"package_id": "pkg_qe"}],
            },
            "strategy_governance_get_paper_readiness": {
                "response_mode": "summary",
                "source": "research_assistant_catalog_summary_adapter",
                "as_of": "2026-06-17",
                "items": [{"package_id": "pkg_qe", "paper_ready": True}],
            },
        }
        payload = payloads[call.tool_name]
        return McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status="succeeded",
            payload_json=payload,
            source_refs=[str(payload["source"])],
            as_of=str(payload["as_of"]),
            executed=True,
        )

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del call, decision
        raise AssertionError("seeded read-only route candidates must not request preflight")


class _SeededMultiToolSynthesisLlm:
    def __init__(self) -> None:
        self.calls = 0

    def complete(self, messages: list[dict[str, Any]]) -> ModelTurn:
        self.calls += 1
        joined = "\n".join(str(item.get("content", "")) for item in messages if isinstance(item, dict))
        assert "graph_context" in joined
        assert "qe_archive_query_promotion_candidates" in joined
        assert "strategy_governance_list_packages" in joined
        assert "strategy_governance_get_paper_readiness" in joined
        return ModelTurn(
            content=(
                "Bottom-line：QE 候选可以先沉淀成策略包，再进入 Paper v2 验证；"
                "综合看应先核对 passes_gate 和 paper_ready。"
                "来源 graph_context，截至 LIVE；来源 qe_archive_read_adapter，截至 2026-06-17；"
                "来源 research_assistant_catalog_summary_adapter，截至 2026-06-17。"
            ),
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


def test_seeded_route_candidates_execute_multiple_tools_and_synthesize_with_graph_context() -> None:
    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "QE成果怎么利用"}],
        model_complete=_SeededMultiToolSynthesisLlm().complete,
        mcp_provider=_SeededMultiToolProvider(),
        catalog_entries=[
            ToolCatalogEntry(server_key="aistock-qe", tool_name="qe_archive_query_promotion_candidates", status="enabled"),
            ToolCatalogEntry(server_key="aistock-trading-ops", tool_name="strategy_governance_list_packages", status="enabled"),
            ToolCatalogEntry(server_key="aistock-trading-ops", tool_name="strategy_governance_get_paper_readiness", status="enabled"),
        ],
        config=ReactGroundingConfig(max_tool_iterations=6, user_message="QE成果怎么利用"),
        seeded_tool_calls=[
            McpToolCall(server_key="aistock-qe", tool_name="qe_archive_query_promotion_candidates", stable_call_id="route:qe"),
            McpToolCall(server_key="aistock-trading-ops", tool_name="strategy_governance_list_packages", stable_call_id="route:packages"),
            McpToolCall(server_key="aistock-trading-ops", tool_name="strategy_governance_get_paper_readiness", stable_call_id="route:paper"),
        ],
        initial_tool_results=[
            McpToolResult(
                server_key="research-assistant",
                tool_name="graph_context",
                status="succeeded",
                payload_json={"response_mode": "graph_context", "graph_context": {"relation_refs": [{"relation_type": "promotes_to"}]}, "as_of": "LIVE"},
                source_refs=["graph_context"],
                as_of="LIVE",
                executed=True,
            )
        ],
    )

    assert result.stopped_reason == "final_answer"
    assert result.evidence_guard.allowed is True
    assert len(result.tool_calls) == 3
    assert len(result.tool_results) == 4
    assert "Bottom-line" in result.final_text
    assert any(step.get("preloaded_tool_result_count") == 1 for step in result.trace_steps)


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
                    "dataset": "kline",
                    "source_refs": ["stock-ref:kline:000688"],
                    "as_of": "2026-06-16",
                    "summary": "one-year kline evidence",
                },
                {
                    "dataset": "financials",
                    "source_refs": ["stock-ref:financials:000688"],
                    "as_of": "2026-06-16",
                    "summary": "financial evidence",
                },
                {
                    "dataset": "fund_flow",
                    "source_refs": ["stock-ref:fund_flow:000688"],
                    "as_of": "2026-06-16",
                    "summary": "fund-flow evidence",
                },
                {
                    "dataset": "quarterly",
                    "source_refs": ["stock-ref:quarterly:000688"],
                    "as_of": "2026-06-16",
                    "summary": "quarterly evidence",
                },
                {
                    "dataset": "margin_financing",
                    "source_refs": ["stock-ref:margin:000688"],
                    "as_of": "2026-06-16",
                    "summary": "margin financing evidence",
                },
                {
                    "dataset": "technicals",
                    "source_refs": ["stock-ref:technicals:000688"],
                    "as_of": "2026-06-16",
                    "summary": "technical indicator evidence",
                },
                {
                    "dataset": "fundamentals",
                    "source": "external_research_provider",
                    "source_refs": ["external-research:guocheng:000688"],
                    "as_of": "2026-06-16",
                    "summary": "external web fundamentals evidence",
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
                "未来趋势只给驱动、情景和风险，不预测方向，也不构成投资建议。"
                "来源 stock-ref:quote:000688，截至 2026-06-16；"
                "来源 stock-ref:fund_flow:000688，截至 2026-06-16。"
            ),
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


def test_bug_413_guard_violation_fails_closed_without_forced_model_regeneration() -> None:
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

    assert len(fake.calls) == 2
    assert result.evidence_guard.allowed is False
    assert result.evidence_guard.reason == "missing_inline_tool_evidence"
    assert result.stopped_reason == "missing_inline_tool_evidence"
    assert any(step.get("evidence_guard_reason") == "missing_inline_tool_evidence" for step in result.trace_steps)
    assert not any(step.get("repair") == "regenerate_with_evidence_citation_options" for step in result.trace_steps)


class _EarlyStopProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del decision
        self.calls.append((call.server_key, call.tool_name))
        return McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status="succeeded",
            payload_json={"source": "source:primary", "as_of": "2026-06-26", "items": [{"value": "ok"}]},
            source_refs=["source:primary"],
            as_of="2026-06-26",
            summary="primary evidence",
            executed=True,
        )

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del call, decision
        raise AssertionError("early-stop read-only test must not request preflight")


class _EarlyStopLlm:
    def __init__(self) -> None:
        self.calls = 0

    def complete(self, messages: list[dict[str, Any]]) -> ModelTurn:
        self.calls += 1
        if self.calls == 1:
            return ModelTurn(
                content="",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
                tool_calls=[
                    McpToolCall(
                        server_key="aistock-primary",
                        tool_name="primary_lookup",
                        stable_call_id="primary_001",
                    )
                ],
            )
        joined = "\n".join(str(item.get("content", "")) for item in messages if isinstance(item, dict))
        assert "source:primary" in joined
        return ModelTurn(
            content="The answer is grounded in the primary read-only result; source=source:primary as_of=2026-06-26.",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


def test_bug_529_model_early_stop_after_evidence_is_not_forced_to_more_tools() -> None:
    fake = _EarlyStopLlm()
    provider = _EarlyStopProvider()

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "answer after gathering evidence"}],
        model_complete=fake.complete,
        mcp_provider=provider,
        catalog_entries=[
            ToolCatalogEntry(server_key="aistock-primary", tool_name="primary_lookup", status="enabled", risk_level="low"),
            ToolCatalogEntry(server_key="aistock-extra", tool_name="extra_lookup", status="enabled", risk_level="low"),
        ],
        config=ReactGroundingConfig(max_tool_iterations=24, user_message="answer after gathering evidence"),
        fallback_tool_calls=lambda: [
            McpToolCall(server_key="aistock-extra", tool_name="extra_lookup", stable_call_id="extra_001")
        ],
    )

    assert fake.calls == 2
    assert provider.calls == [("aistock-primary", "primary_lookup")]
    assert result.iterations == 2
    assert result.stopped_reason == "final_answer"
    assert result.evidence_guard.allowed is True
    assert not any(step.get("fallback_tool_call_count") for step in result.trace_steps)


class _ZeroEvidenceFallbackProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del decision
        self.calls.append((call.server_key, call.tool_name))
        return McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status="succeeded",
            payload_json={"source": "source:zero-evidence-fallback", "as_of": "2026-06-26", "items": [{"value": "covered"}]},
            source_refs=["source:zero-evidence-fallback"],
            as_of="2026-06-26",
            summary="fallback evidence",
            executed=True,
        )

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del call, decision
        raise AssertionError("zero-evidence fallback must stay read-only")


class _ZeroEvidenceAnswerLlm:
    def __init__(self) -> None:
        self.calls = 0

    def complete(self, messages: list[dict[str, Any]]) -> ModelTurn:
        self.calls += 1
        if self.calls == 1:
            return ModelTurn(
                content="I can answer now without tool evidence.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )
        joined = "\n".join(str(item.get("content", "")) for item in messages if isinstance(item, dict))
        assert "source:zero-evidence-fallback" in joined
        return ModelTurn(
            content="The answer uses the read-only fallback evidence; source=source:zero-evidence-fallback as_of=2026-06-26.",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


def test_bug_529_zero_evidence_answer_attempt_forces_read_only_evidence_without_question_classifier() -> None:
    fake = _ZeroEvidenceAnswerLlm()
    provider = _ZeroEvidenceFallbackProvider()

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "non-keyword wording"}],
        model_complete=fake.complete,
        mcp_provider=provider,
        catalog_entries=[ToolCatalogEntry(server_key="aistock-generic-read", tool_name="generic_lookup", status="enabled", risk_level="low")],
        config=ReactGroundingConfig(max_tool_iterations=24, user_message="non-keyword wording"),
        fallback_tool_calls=lambda: [
            McpToolCall(server_key="aistock-generic-read", tool_name="generic_lookup", stable_call_id="generic_001")
        ],
    )

    assert fake.calls == 2
    assert provider.calls == [("aistock-generic-read", "generic_lookup")]
    assert result.evidence_guard.allowed is True
    assert result.stopped_reason == "final_answer"
    assert any(step.get("reason") == "zero_evidence_answer_attempt" for step in result.trace_steps)


def test_bug_529_guard_redline_still_fails_closed_when_no_repair_or_fallback_exists() -> None:
    def model_complete(messages: list[dict[str, Any]]) -> ModelTurn:
        del messages
        return ModelTurn(
            content="There are 42 unsupported facts without source or date.",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "answer from existing evidence"}],
        model_complete=model_complete,
        mcp_provider=_EarlyStopProvider(),
        catalog_entries=[],
        config=ReactGroundingConfig(max_tool_iterations=24, user_message="answer from existing evidence"),
        initial_tool_results=[
            McpToolResult(
                server_key="aistock-existing",
                tool_name="existing_lookup",
                status="succeeded",
                payload_json={"source": "source:existing", "as_of": "2026-06-26", "items": [{"value": "known"}]},
                source_refs=["source:existing"],
                as_of="2026-06-26",
                summary="existing evidence",
                executed=True,
            )
        ],
    )

    assert result.evidence_guard.allowed is False
    assert result.evidence_guard.reason == "missing_inline_tool_evidence"
    assert result.stopped_reason == "missing_inline_tool_evidence"
    assert result.iterations == 1


def _bug_509_stock_result(sections: list[dict[str, Any]], *, tool_name: str = "stock_analysis_get_quote") -> McpToolResult:
    return McpToolResult(
        server_key="aistock-stock-analysis",
        tool_name=tool_name,
        status="succeeded",
        payload_json={
            "response_mode": "stock_analysis_evidence_card",
            "source": "stock_analysis_read_adapter",
            "source_refs": ["stock-ref:quote:000688"],
            "as_of": "2026-06-16",
            "sections": sections,
        },
        source_refs=["stock-ref:quote:000688"],
        as_of="2026-06-16",
        summary="stock depth evidence card",
        executed=True,
    )


def test_t9_4_stock_depth_guard_no_longer_blocks_undercovered_but_sourced_answer() -> None:
    result = _bug_509_stock_result(
        [
            {"dataset": "quote", "source_refs": ["stock-ref:quote:000688"], "as_of": "2026-06-16"},
            {"dataset": "kline", "source_refs": ["stock-ref:kline:000688"], "as_of": "2026-06-16"},
        ]
    )

    guard = compose_with_evidence_guard(
        "Bottom-line: 000688 only has quote and kline evidence here. source stock-ref:quote:000688 as_of 2026-06-16.",
        [result],
        ReactGroundingConfig(max_tool_iterations=10, user_message="stock depth all-round fundamental analysis for 000688"),
    )

    assert guard.allowed is True
    assert guard.reason == "ok"
    assert "stock_depth_required_evidence_missing" not in guard.text
    assert "source stock-ref:quote:000688 as_of 2026-06-16" in guard.text


def test_bug_509_stock_depth_guard_accepts_full_stock_card_with_web_evidence() -> None:
    results = [
        _bug_509_stock_result([{"dataset": "quote", "source_refs": ["stock-ref:quote:000688"], "as_of": "2026-06-16"}], tool_name="stock_analysis_get_quote"),
        _bug_509_stock_result([{"dataset": "kline", "source_refs": ["stock-ref:kline:000688"], "as_of": "2026-06-16"}], tool_name="stock_analysis_get_kline"),
        _bug_509_stock_result([{"dataset": "technicals", "source_refs": ["stock-ref:technicals:000688"], "as_of": "2026-06-16"}], tool_name="stock_analysis_get_technicals"),
        _bug_509_stock_result([{"dataset": "fund_flow", "source_refs": ["stock-ref:fund_flow:000688"], "as_of": "2026-06-16"}], tool_name="stock_analysis_get_fund_flow"),
        _bug_509_stock_result([{"dataset": "financials", "source_refs": ["stock-ref:financials:000688"], "as_of": "2026-06-16"}], tool_name="stock_analysis_get_financials"),
        _bug_509_stock_result([{"dataset": "quarterly", "source_refs": ["stock-ref:quarterly:000688"], "as_of": "2026-06-16"}], tool_name="stock_analysis_get_quarterly"),
        _bug_509_stock_result([{"dataset": "margin_financing", "source_refs": ["stock-ref:margin:000688"], "as_of": "2026-06-16"}], tool_name="stock_analysis_get_margin_financing"),
        McpToolResult(
            server_key="aistock-external-research",
            tool_name="external_research_search_web",
            status="succeeded",
            payload_json={
                "items": [
                    {
                        "title": "Guocheng Mining business background",
                        "url": "https://research.example.com/guocheng-mining",
                        "source": "external_research_provider",
                        "as_of": "2026-06-16",
                        "evidence_ref": "external-research:guocheng:000688",
                    }
                ],
                "source_refs": ["external-research:guocheng:000688"],
                "as_of": "2026-06-16",
            },
            source_refs=["external-research:guocheng:000688"],
            as_of="2026-06-16",
            summary="web evidence",
            executed=True,
        ),
    ]

    guard = compose_with_evidence_guard(
        "Bottom-line: 000688 has market, history, fund-flow and fundamental evidence. source stock-ref:quote:000688 as_of 2026-06-16.",
        results,
        ReactGroundingConfig(max_tool_iterations=10, user_message="stock depth all-round fundamental analysis for 000688"),
    )

    assert guard.allowed is True
    assert guard.reason == "ok"


class _LocalDataSyncProvider:
    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del decision
        assert call.tool_name == "local_data_get_preset_daily_status"
        return McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status="succeeded",
            payload_json={
                "response_mode": "local_data_daily_sync_status",
                "source": "local_data_facade_read_adapter",
                "as_of": "2026-06-17",
                "group_counts": {"success": 1, "failed": 0},
            },
            source_refs=["local_data_facade_read_adapter"],
            as_of="2026-06-17",
            summary="success=1 failed=0",
            executed=True,
        )

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del call, decision
        raise AssertionError("local-data read-only recovery must not require preflight")


class _RecoveringCatalogRejectionLlm:
    def __init__(self) -> None:
        self.calls = 0

    def complete(self, messages: list[dict[str, Any]]) -> ModelTurn:
        del messages
        self.calls += 1
        if self.calls == 1:
            return ModelTurn(
                content="",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
                tool_calls=[
                    McpToolCall(
                        server_key="aistock-local-data",
                        tool_name="local_data_get_unack_alert_count",
                        stable_call_id="uncovered_alert_count",
                    )
                ],
            )
        if self.calls == 2:
            return ModelTurn(
                content="",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
                tool_calls=[
                    McpToolCall(
                        server_key="aistock-local-data",
                        tool_name="local_data_get_preset_daily_status",
                        stable_call_id="covered_daily_status",
                    )
                ],
            )
        return ModelTurn(
            content="Local data sync succeeded: success=1 failed=0; source=local_data_facade_read_adapter as_of=2026-06-17.",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


class _UnrecoveredCatalogRejectionLlm:
    def __init__(self) -> None:
        self.calls = 0

    def complete(self, messages: list[dict[str, Any]]) -> ModelTurn:
        del messages
        self.calls += 1
        if self.calls == 1:
            return ModelTurn(
                content="",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
                tool_calls=[
                    McpToolCall(
                        server_key="aistock-local-data",
                        tool_name="local_data_get_unack_alert_count",
                        stable_call_id="uncovered_alert_count",
                    )
                ],
            )
        return ModelTurn(
            content="Insufficient evidence: no covered local-data result.",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


def test_bug_404_412_recovered_catalog_rejection_does_not_override_grounded_answer() -> None:
    fake = _RecoveringCatalogRejectionLlm()

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "Was yesterday's local data sync OK?"}],
        model_complete=fake.complete,
        mcp_provider=_LocalDataSyncProvider(),
        catalog_entries=[
            ToolCatalogEntry(
                server_key="aistock-local-data",
                tool_name="local_data_get_preset_daily_status",
                status="enabled",
                risk_level="low",
                side_effect_level="read_only",
            )
        ],
        config=ReactGroundingConfig(max_tool_iterations=4, user_message="Was yesterday's local data sync OK?"),
    )

    assert result.evidence_guard.allowed is True
    assert result.evidence_guard.reason == "ok"
    assert "success=1" in result.final_text
    assert "reason_code=capability_not_found" not in result.final_text
    assert "Insufficient evidence" not in result.final_text
    rejected = result.tool_results[0]
    assert rejected.status == "rejected"
    assert rejected.error_json["reason_code"] == "capability_not_found"
    assert rejected.error_json["catalog_reason"] == "tool_not_in_audited_catalog"
    assert rejected.error_json["recoverable_catalog_rejection"] is True



class _EmptyMcpThenExternalProvider:
    def __init__(self, *, external_has_items: bool, external_stub: bool = False) -> None:
        self.external_has_items = external_has_items
        self.external_stub = external_stub
        self.calls: list[tuple[str, str]] = []

    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del decision
        self.calls.append((call.server_key, call.tool_name))
        if call.tool_name == "stock_analysis_get_quote":
            return McpToolResult(
                server_key=call.server_key,
                tool_name=call.tool_name,
                status="succeeded",
                payload_json={"response_mode": "stock_analysis_evidence_card", "items": [], "total": 0},
                source_refs=["stock_analysis_empty_fixture"],
                as_of="2026-06-23",
                summary="no stock rows",
                executed=True,
            )
        if call.tool_name == "external_research_search_web":
            url = "https://example.org/research/guocheng" if self.external_stub else "https://research.example.net/guocheng"
            source = "example_web_index" if self.external_stub else "trusted_research_index"
            provider = "deterministic_offline_external_research" if self.external_stub else "live_external_research_provider"
            items = (
                [
                    {
                        "title": "Guocheng Mining external evidence",
                        "summary": "industry status context",
                        "url": url,
                        "source": source,
                        "as_of": "2026-06-23",
                        "evidence_ref": "external-evidence:guocheng",
                        "provider": provider,
                    }
                ]
                if self.external_has_items
                else []
            )
            return McpToolResult(
                server_key=call.server_key,
                tool_name=call.tool_name,
                status="succeeded",
                payload_json={
                    "response_mode": "summary",
                    "domain": "external_research.web",
                    "items": items,
                    "total": len(items),
                    "source": source,
                    "as_of": "2026-06-23",
                    "provider": provider,
                },
                source_refs=[source] if items else [],
                as_of="2026-06-23" if items else None,
                summary="external evidence" if items else "no external evidence",
                executed=True,
            )
        raise AssertionError(f"unexpected tool call: {call.server_key}/{call.tool_name}")

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        del call, decision
        raise AssertionError("BUG-495 fallback tools must be read-only")


class _EmptyMcpThenExternalLlm:
    def __init__(self, *, source: str = "trusted_research_index") -> None:
        self.calls = 0
        self.source = source

    def complete(self, messages: list[dict[str, Any]]) -> ModelTurn:
        self.calls += 1
        joined = "\n".join(str(item.get("content", "")) for item in messages if isinstance(item, dict))
        if self.calls == 1:
            return ModelTurn(content="", provider="fake", model="fake-primary", duration_ms=1, usage={})
        assert "external_research_search_web" in joined
        assert self.source in joined
        return ModelTurn(
            content=f"External evidence can supplement industry context; source={self.source} as_of=2026-06-23.",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


class _EmptyMcpExternalNoDataLlm:
    def __init__(self) -> None:
        self.calls = 0

    def complete(self, messages: list[dict[str, Any]]) -> ModelTurn:
        del messages
        self.calls += 1
        return ModelTurn(content="", provider="fake", model="fake-primary", duration_ms=1, usage={})


_BUG_495_USER_MESSAGE = "Guocheng Mining industry status information query"


def _bug_495_catalog_entries() -> list[ToolCatalogEntry]:
    return [
        ToolCatalogEntry(
            server_key="aistock-stock-analysis",
            tool_name="stock_analysis_get_quote",
            status="enabled",
            risk_level="low",
            side_effect_level="read_only",
        ),
        ToolCatalogEntry(
            server_key="aistock-external-research",
            tool_name="external_research_search_web",
            status="enabled",
            risk_level="low",
            side_effect_level="read_only",
        ),
    ]


def _bug_495_seeded_stock_call() -> list[McpToolCall]:
    return [
        McpToolCall(server_key="aistock-stock-analysis", tool_name="stock_analysis_get_quote", stable_call_id="route:stock"),
    ]


def test_bug_495_empty_mcp_information_query_forces_external_research_and_cites_source() -> None:
    fake = _EmptyMcpThenExternalLlm()
    provider = _EmptyMcpThenExternalProvider(external_has_items=True)

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": _BUG_495_USER_MESSAGE}],
        model_complete=fake.complete,
        mcp_provider=provider,
        catalog_entries=_bug_495_catalog_entries(),
        config=ReactGroundingConfig(max_tool_iterations=4, user_message=_BUG_495_USER_MESSAGE),
        seeded_tool_calls=_bug_495_seeded_stock_call(),
    )

    assert provider.calls == [
        ("aistock-stock-analysis", "stock_analysis_get_quote"),
        ("aistock-external-research", "external_research_search_web"),
    ]
    assert result.evidence_guard.allowed is True
    assert result.stopped_reason == "final_answer"
    assert "trusted_research_index" in result.final_text
    assert "2026-06-23" in result.final_text
    assert "example.org" not in result.final_text
    assert any(step.get("fallback") == "external_research_after_empty_mcp" for step in result.trace_steps)


def test_bug_495_stub_external_research_is_not_treated_as_evidence() -> None:
    fake = _EmptyMcpExternalNoDataLlm()
    provider = _EmptyMcpThenExternalProvider(external_has_items=True, external_stub=True)

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": _BUG_495_USER_MESSAGE}],
        model_complete=fake.complete,
        mcp_provider=provider,
        catalog_entries=_bug_495_catalog_entries(),
        config=ReactGroundingConfig(max_tool_iterations=4, user_message=_BUG_495_USER_MESSAGE),
        seeded_tool_calls=_bug_495_seeded_stock_call(),
    )

    assert provider.calls == [
        ("aistock-stock-analysis", "stock_analysis_get_quote"),
        ("aistock-external-research", "external_research_search_web"),
    ]
    assert result.evidence_guard.allowed is False
    assert result.evidence_guard.reason == "no_data_source_after_mcp_and_external_research"
    assert result.stopped_reason == "no_data_source"
    assert "example.org" not in result.final_text
    assert "deterministic_offline" not in result.final_text
    assert "aistock-external-research/external_research_search_web" in result.final_text


def test_bug_495_empty_mcp_and_empty_external_research_reports_no_data_source() -> None:
    fake = _EmptyMcpExternalNoDataLlm()
    provider = _EmptyMcpThenExternalProvider(external_has_items=False)

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": _BUG_495_USER_MESSAGE}],
        model_complete=fake.complete,
        mcp_provider=provider,
        catalog_entries=_bug_495_catalog_entries(),
        config=ReactGroundingConfig(max_tool_iterations=4, user_message=_BUG_495_USER_MESSAGE),
        seeded_tool_calls=_bug_495_seeded_stock_call(),
    )

    assert provider.calls == [
        ("aistock-stock-analysis", "stock_analysis_get_quote"),
        ("aistock-external-research", "external_research_search_web"),
    ]
    assert result.evidence_guard.allowed is False
    assert result.evidence_guard.reason == "no_data_source_after_mcp_and_external_research"
    assert result.stopped_reason == "no_data_source"
    assert "aistock-stock-analysis/stock_analysis_get_quote" in result.final_text
    assert "aistock-external-research/external_research_search_web" in result.final_text
    assert "Insufficient evidence" not in result.final_text


def test_bug_404_412_unrecovered_catalog_rejection_reports_loud_capability_error() -> None:
    fake = _UnrecoveredCatalogRejectionLlm()

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "Was yesterday's local data sync OK?"}],
        model_complete=fake.complete,
        mcp_provider=_LocalDataSyncProvider(),
        catalog_entries=[
            ToolCatalogEntry(
                server_key="aistock-local-data",
                tool_name="local_data_get_preset_daily_status",
                status="enabled",
                risk_level="low",
                side_effect_level="read_only",
            )
        ],
        config=ReactGroundingConfig(max_tool_iterations=3, user_message="Was yesterday's local data sync OK?"),
    )

    assert result.evidence_guard.allowed is False
    assert result.evidence_guard.reason == "explicit_tool_error"
    assert "reason_code=capability_not_found" in result.final_text
    assert "tool_not_in_audited_catalog" in result.final_text
    assert "Insufficient evidence" not in result.final_text
