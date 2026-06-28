from __future__ import annotations

from typing import Any

from backend.services.research_assistant.react_grounding import (
    McpToolCall,
    McpToolResult,
    ModelTurn,
    ReactGroundingConfig,
    ToolCatalogEntry,
    ToolGateDecision,
    compose_with_evidence_guard,
    run_react_grounding_loop,
)


def _result(tool_name: str, *, source: str, as_of: str) -> McpToolResult:
    return McpToolResult(
        server_key="aistock-test",
        tool_name=tool_name,
        status="succeeded",
        payload_json={"source": source, "as_of": as_of},
        source_refs=[source],
        as_of=as_of,
        executed=True,
    )


def test_future_answer_allows_grounded_non_directional_answer_without_style_template() -> None:
    decision = compose_with_evidence_guard(
        "国城矿业未来更应观察成交和资金面是否延续，当前只读证据显示波动加大；来源 stock_ref，截至 2026-06-17。",
        [_result("stock_analysis_get_quote", source="stock_ref", as_of="2026-06-17")],
        ReactGroundingConfig(max_tool_iterations=4, user_message="国城矿业未来趋势怎样？"),
    )

    assert decision.allowed is True
    assert decision.reason != "future_answer_boundary_missing"
    assert decision.reason == "ok"


def test_future_answer_still_blocks_directional_prediction() -> None:
    decision = compose_with_evidence_guard(
        "国城矿业未来一个月将上涨；来源 stock_ref，截至 2026-06-17。",
        [_result("stock_analysis_get_quote", source="stock_ref", as_of="2026-06-17")],
        ReactGroundingConfig(max_tool_iterations=4, user_message="国城矿业未来趋势如何？"),
    )

    assert decision.allowed is False
    assert decision.reason == "future_answer_boundary_missing"


def test_t9_1_blocks_directional_prediction_without_future_question_terms() -> None:
    decision = compose_with_evidence_guard(
        "国城矿业必然上涨；来源 stock_ref，截至 2026-06-17。",
        [_result("stock_analysis_get_quote", source="stock_ref", as_of="2026-06-17")],
        ReactGroundingConfig(max_tool_iterations=4, user_message="这只票怎么样？"),
    )

    assert decision.allowed is False
    assert decision.reason == "future_answer_boundary_missing"


def test_future_answer_allows_negated_directional_marker_context() -> None:
    decision = compose_with_evidence_guard(
        "基于现有证据无法判断方向、不会上涨，也不会给出涨跌预测；来源 stock_ref，截至 2026-06-17。",
        [_result("stock_analysis_get_quote", source="stock_ref", as_of="2026-06-17")],
        ReactGroundingConfig(
            max_tool_iterations=4,
            user_message="国城矿业未来趋势如何？",
            future_directional_markers=("上涨", "下跌"),
        ),
    )

    assert decision.allowed is True
    assert decision.reason == "ok"


def test_t9_1_allows_historical_price_restatement_without_prediction() -> None:
    decision = compose_with_evidence_guard(
        "历史事实：该股上月上涨了 10%，昨日涨停；来源 stock_ref，截至 2026-06-17。",
        [_result("stock_analysis_get_quote", source="stock_ref", as_of="2026-06-17")],
        ReactGroundingConfig(max_tool_iterations=4, user_message="把这只票近期表现说明一下"),
    )

    assert decision.allowed is True
    assert decision.reason == "ok"


def test_t9_1_directional_prediction_guard_is_wording_invariant_without_future_terms() -> None:
    messages = (
        "这只票怎么样？",
        "国城矿业怎么看？",
        "这家公司现在值得关注吗？",
    )
    for message in messages:
        decision = compose_with_evidence_guard(
            "国城矿业必然上涨；来源 stock_ref，截至 2026-06-17。",
            [_result("stock_analysis_get_quote", source="stock_ref", as_of="2026-06-17")],
            ReactGroundingConfig(max_tool_iterations=4, user_message=message),
        )

        assert decision.allowed is False
        assert decision.reason == "future_answer_boundary_missing"


def test_future_answer_allows_driver_scenario_risk_without_directional_prediction() -> None:
    decision = compose_with_evidence_guard(
        "Bottom-line：只看驱动、情景和风险，不预测方向，也不构成投资建议；"
        "驱动是成交和资金流，情景是放量/缩量两种验证路径，风险是样本窗口短。"
        "来源 stock_ref，截至 2026-06-17。",
        [_result("stock_analysis_get_quote", source="stock_ref", as_of="2026-06-17")],
        ReactGroundingConfig(max_tool_iterations=4, user_message="这只股票未来趋势如何？"),
    )

    assert decision.allowed is True
    assert decision.reason == "ok"


def _qe_leaderboard_results() -> list[McpToolResult]:
    return [
        McpToolResult(
            server_key="aistock-qe",
            tool_name="qe_archive_query_run_leaderboard",
            status="succeeded",
            payload_json={
                "source": "qe_archive:leaderboard",
                "as_of": "2026-06-24",
                "items": [
                    {
                        "rank": 1,
                        "loop_id": "loop-001",
                        "cagr": 1.12,
                        "model_type": "seed_LSTM_10D_hs64_d02",
                        "verification_status": "not_verified",
                    },
                    {
                        "rank": 10,
                        "loop_id": "loop-010",
                        "cagr": 1.06,
                        "model_type": "seed_TCN_10D_d02",
                        "verification_status": "not_verified",
                    },
                ],
            },
            source_refs=["qe_archive:leaderboard"],
            as_of="2026-06-24",
            executed=True,
        ),
        McpToolResult(
            server_key="aistock-qe",
            tool_name="qe_archive_health",
            status="succeeded",
            payload_json={"source": "qe_archive:health", "as_of": "2026-06-24", "item": {"run_count": 10}},
            source_refs=["qe_archive:health"],
            as_of="2026-06-24",
            executed=True,
        ),
    ]


def test_factual_qe_ranking_list_allows_structured_list_with_unverified_risk_label() -> None:
    decision = compose_with_evidence_guard(
        """
| rank | loop | CAGR | model | verification | source | as_of |
| 1 | loop-001 | 112.00% | seed_LSTM_10D_hs64_d02 | not_verified - unverified backtest risk; do not treat as real returns | qe_archive:leaderboard | 2026-06-24 |
| 10 | loop-010 | 106.00% | seed_TCN_10D_d02 | not_verified - unverified backtest risk; do not treat as real returns | qe_archive:leaderboard | 2026-06-24 |
Sources: qe_archive:leaderboard as_of 2026-06-24; qe_archive:health as_of 2026-06-24.
""",
        _qe_leaderboard_results(),
        ReactGroundingConfig(max_tool_iterations=4, user_message="目前QE实验排名前10位的loop年化收益分别是多少？分别使用了什么模型？"),
    )

    assert decision.allowed is True
    assert decision.reason == "ok"


def test_react_loop_allows_realistic_factual_qe_ranking_list_output() -> None:
    class NoToolProvider:
        def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            raise AssertionError("preloaded QE evidence should be enough for final factual list")

        def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            raise AssertionError("preloaded QE evidence should not trigger preflight")

    def model_complete(messages: list[dict[str, Any]]) -> ModelTurn:
        return ModelTurn(
            content=(
                "按当前 QE archive 排名清单：\n"
                "| rank | loop | CAGR | model | verification | source | as_of |\n"
                "| 1 | loop-001 | 112.00% | seed_LSTM_10D_hs64_d02 | not_verified - 未验证回测风险，勿当真实收益 | qe_archive:leaderboard | 2026-06-24 |\n"
                "| 10 | loop-010 | 106.00% | seed_TCN_10D_d02 | not_verified - 未验证回测风险，勿当真实收益 | qe_archive:leaderboard | 2026-06-24 |\n"
                "来源: qe_archive:leaderboard as_of 2026-06-24；qe_archive:health as_of 2026-06-24。"
            ),
            provider="fake-realistic",
            model="fake-qe-list",
            duration_ms=1,
            usage={},
        )

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "目前QE实验排名前10位的loop年化收益分别是多少？分别使用了什么模型？"}],
        model_complete=model_complete,
        mcp_provider=NoToolProvider(),
        catalog_entries=[],
        config=ReactGroundingConfig(
            max_tool_iterations=2,
            user_message="目前QE实验排名前10位的loop年化收益分别是多少？分别使用了什么模型？",
        ),
        initial_tool_results=_qe_leaderboard_results(),
    )

    assert result.evidence_guard is not None
    assert result.evidence_guard.allowed is True
    assert result.evidence_guard.reason == "ok"
    assert result.stopped_reason == "final_answer"
    assert "Insufficient evidence" not in result.final_text


def test_factual_qe_ranking_list_requires_row_level_source_and_as_of() -> None:
    decision = compose_with_evidence_guard(
        """
| rank | loop | CAGR | model | verification |
| 1 | loop-001 | 112.00% | seed_LSTM_10D_hs64_d02 | not_verified - unverified backtest risk; do not treat as real returns |
| 10 | loop-010 | 106.00% | seed_TCN_10D_d02 | not_verified - unverified backtest risk; do not treat as real returns |
Sources: qe_archive:leaderboard as_of 2026-06-24; qe_archive:health as_of 2026-06-24.
""",
        _qe_leaderboard_results(),
        ReactGroundingConfig(max_tool_iterations=4, user_message="目前QE实验排名前10位的loop年化收益分别是多少？分别使用了什么模型？"),
    )

    assert decision.allowed is False
    assert decision.reason == "factual_list_row_evidence_missing"


def test_factual_qe_ranking_list_requires_unverified_risk_label() -> None:
    decision = compose_with_evidence_guard(
        """
| rank | loop | CAGR | model | verification | source | as_of |
| 1 | loop-001 | 112.00% | seed_LSTM_10D_hs64_d02 | not_verified | qe_archive:leaderboard | 2026-06-24 |
| 10 | loop-010 | 106.00% | seed_TCN_10D_d02 | not_verified | qe_archive:leaderboard | 2026-06-24 |
Sources: qe_archive:leaderboard as_of 2026-06-24; qe_archive:health as_of 2026-06-24.
""",
        _qe_leaderboard_results(),
        ReactGroundingConfig(max_tool_iterations=4, user_message="目前QE实验排名前10位的loop年化收益分别是多少？分别使用了什么模型？"),
    )

    assert decision.allowed is False
    assert decision.reason == "unverified_evidence_risk_label_missing"


def test_factual_qe_ranking_list_still_blocks_placeholder_facts() -> None:
    decision = compose_with_evidence_guard(
        """
| rank | loop | CAGR | model | verification | source | as_of |
| 1 | loop-001 | XX% | seed_LSTM_10D_hs64_d02 | not_verified - unverified backtest risk; do not treat as real returns | qe_archive:leaderboard | 2026-06-24 |
Sources: qe_archive:leaderboard as_of 2026-06-24.
""",
        _qe_leaderboard_results(),
        ReactGroundingConfig(max_tool_iterations=4, user_message="目前QE实验排名前10位的loop年化收益分别是多少？分别使用了什么模型？"),
    )

    assert decision.allowed is False
    assert decision.reason.startswith("placeholder_blocked")


def test_judgement_question_still_blocks_source_listing_without_synthesis() -> None:
    decision = compose_with_evidence_guard(
        "Source 1: quote source source_a as_of 2026-06-17. Source 2: fund flow source source_b as_of 2026-06-17.",
        [
            _result("tool_a", source="source_a", as_of="2026-06-17"),
            _result("tool_b", source="source_b", as_of="2026-06-17"),
        ],
        ReactGroundingConfig(max_tool_iterations=4, user_message="please synthesize and analyze this stock"),
    )

    assert decision.allowed is False
    assert decision.reason == "multi_source_synthesis_missing"


def test_multi_source_listing_is_blocked_without_synthesis_judgement() -> None:
    decision = compose_with_evidence_guard(
        "工具1：来源 source_a，截至 2026-06-17。工具2：来源 source_b，截至 2026-06-17。",
        [
            _result("tool_a", source="source_a", as_of="2026-06-17"),
            _result("tool_b", source="source_b", as_of="2026-06-17"),
        ],
        ReactGroundingConfig(max_tool_iterations=4, user_message="综合分析一下这些信息"),
    )

    assert decision.allowed is False
    assert decision.reason == "multi_source_synthesis_missing"


def test_multi_source_bottom_line_synthesis_is_allowed() -> None:
    decision = compose_with_evidence_guard(
        "Bottom-line：综合判断应先处理 A，再用 B 做交叉验证；来源 source_a，截至 2026-06-17；来源 source_b，截至 2026-06-17。",
        [
            _result("tool_a", source="source_a", as_of="2026-06-17"),
            _result("tool_b", source="source_b", as_of="2026-06-17"),
        ],
        ReactGroundingConfig(max_tool_iterations=4, user_message="综合分析一下这些信息"),
    )

    assert decision.allowed is True
    assert decision.reason == "ok"



def _stock_depth_partial_results() -> list[McpToolResult]:
    return [
        McpToolResult(
            server_key="aistock-stock-analysis",
            tool_name="stock_analysis_get_quote",
            status="succeeded",
            payload_json={
                "sections": [
                    {
                        "dataset": "quote",
                        "source": "stock_quote:000688",
                        "as_of": "2026-06-24",
                        "items": [{"price": 10.2}],
                    }
                ]
            },
            source_refs=["stock_quote:000688"],
            as_of="2026-06-24",
            executed=True,
            side_effect_level="read_only",
        ),
        McpToolResult(
            server_key="aistock-stock-analysis",
            tool_name="stock_analysis_get_kline",
            status="succeeded",
            payload_json={
                "sections": [
                    {
                        "dataset": "kline",
                        "source": "stock_kline:000688",
                        "as_of": "2026-06-24",
                        "items": [{"close": 10.2}, {"close": 9.7}],
                    }
                ]
            },
            source_refs=["stock_kline:000688"],
            as_of="2026-06-24",
            executed=True,
            side_effect_level="read_only",
        ),
        McpToolResult(
            server_key="aistock-stock-analysis",
            tool_name="stock_analysis_get_fund_flow",
            status="succeeded",
            payload_json={
                "sections": [
                    {
                        "dataset": "fund_flow",
                        "source": "stock_fund_flow:000688",
                        "as_of": "2026-06-24",
                        "items": [{"net_inflow": 1200}],
                    }
                ]
            },
            source_refs=["stock_fund_flow:000688"],
            as_of="2026-06-24",
            executed=True,
            side_effect_level="read_only",
        ),
        McpToolResult(
            server_key="aistock-stock-analysis",
            tool_name="stock_analysis_get_financials",
            status="succeeded",
            payload_json={"dataset": "financials", "source": "stock_financials:000688", "as_of": "2026-06-24", "items": []},
            source_refs=["stock_financials:000688"],
            as_of="2026-06-24",
            executed=True,
            side_effect_level="read_only",
        ),
    ]


def test_read_only_partial_stock_depth_evidence_degrades_with_sources_and_gaps() -> None:
    class NoToolProvider:
        def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            raise AssertionError("preloaded partial read-only evidence should be enough to test degradation")

        def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            raise AssertionError("read-only degradation must not use preflight")

    def model_complete(messages: list[dict[str, Any]]) -> ModelTurn:
        return ModelTurn(
            content=(
                "Bottom-line: available read-only evidence only covers market, history, and fund flow; "
                "drivers/scenarios/risks are incomplete, and this is not a direction forecast or investment advice. "
                "stock_quote:000688 as_of 2026-06-24; stock_kline:000688 as_of 2026-06-24; "
                "stock_fund_flow:000688 as_of 2026-06-24."
            ),
            provider="fake",
            model="fake-partial-stock-depth",
            duration_ms=1,
            usage={},
        )

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "stock depth analysis: limit down future trend and fundamental for 000688"}],
        model_complete=model_complete,
        mcp_provider=NoToolProvider(),
        catalog_entries=[],
        config=ReactGroundingConfig(max_tool_iterations=1, user_message="stock depth analysis: limit down future trend and fundamental for 000688"),
        initial_tool_results=_stock_depth_partial_results(),
    )

    assert result.evidence_guard.allowed is True
    assert result.evidence_guard.reason == "read_only_partial_evidence_degraded"
    assert result.stopped_reason == "read_only_partial_evidence_degraded"
    assert "Insufficient evidence" not in result.final_text
    assert "stock_quote:000688" in result.final_text
    assert "2026-06-24" in result.final_text
    assert "Missing / not covered" in result.final_text
    assert "original_reason=stock_depth_required_evidence_missing" in result.final_text


def test_non_read_only_partial_evidence_still_fails_closed() -> None:
    class FakeProvider:
        def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            raise AssertionError("write action should not execute as read-only")

        def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            return McpToolResult(
                server_key=call.server_key,
                tool_name=call.tool_name,
                status="preflight_required",
                payload_json={"preflight_only": True},
                source_refs=["preflight"],
                as_of="2026-06-24",
                executed=False,
                blocked_reason="preflight_confirmation_required",
                side_effect_level=decision.side_effect_level,
            )

    def model_complete(messages: list[dict[str, Any]]) -> ModelTurn:
        return ModelTurn(
            content="Insufficient evidence: max tool iterations reached without reliable evidence.",
            provider="fake",
            model="fake-write",
            duration_ms=1,
            usage={},
            tool_calls=[
                McpToolCall(
                    server_key="aistock-write",
                    tool_name="submit_order",
                    stable_call_id="write_001",
                    risk_level="high",
                    side_effect_level="production_sensitive",
                )
            ],
        )

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "submit the write action"}],
        model_complete=model_complete,
        mcp_provider=FakeProvider(),
        catalog_entries=[
            ToolCatalogEntry(
                server_key="aistock-write",
                tool_name="submit_order",
                status="enabled",
                risk_level="high",
                side_effect_level="production_sensitive",
                requires_approval=True,
            )
        ],
        config=ReactGroundingConfig(max_tool_iterations=1, user_message="submit the write action"),
        initial_tool_results=_stock_depth_partial_results(),
    )

    assert result.evidence_guard.allowed is False
    assert result.evidence_guard.reason != "read_only_partial_evidence_degraded"
    assert "Insufficient evidence" in result.final_text


def test_read_only_degradation_does_not_override_placeholder_redline() -> None:
    class NoToolProvider:
        def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            raise AssertionError("preloaded evidence should be enough")

        def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            raise AssertionError("preflight should not run")

    def model_complete(messages: list[dict[str, Any]]) -> ModelTurn:
        return ModelTurn(
            content="XX% change; stock_quote:000688 as_of 2026-06-24.",
            provider="fake",
            model="fake-placeholder",
            duration_ms=1,
            usage={},
        )

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "stock depth analysis: limit down future trend and fundamental for 000688"}],
        model_complete=model_complete,
        mcp_provider=NoToolProvider(),
        catalog_entries=[],
        config=ReactGroundingConfig(max_tool_iterations=1, user_message="stock depth analysis: limit down future trend and fundamental for 000688"),
        initial_tool_results=_stock_depth_partial_results(),
    )

    assert result.evidence_guard.allowed is False
    assert result.evidence_guard.reason.startswith("placeholder_blocked")


def test_read_only_partial_degradation_is_based_on_side_effect_not_question_keywords() -> None:
    class GenericReadProvider:
        def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            return McpToolResult(
                server_key=call.server_key,
                tool_name=call.tool_name,
                status="succeeded",
                payload_json={"source": "generic_read_source", "as_of": "2026-06-24", "items": [{"value": "available"}]},
                source_refs=["generic_read_source"],
                as_of="2026-06-24",
                summary="one read-only evidence item is available",
                executed=True,
                side_effect_level=decision.side_effect_level,
            )

        def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
            raise AssertionError("read-only tool should execute without preflight")

    def model_complete(messages: list[dict[str, Any]]) -> ModelTurn:
        return ModelTurn(
            content="",
            provider="fake",
            model="fake-generic-read-only",
            duration_ms=1,
            usage={},
            tool_calls=[
                McpToolCall(
                    server_key="aistock-generic-read",
                    tool_name="generic_lookup",
                    stable_call_id="generic_read_001",
                    risk_level="low",
                    side_effect_level="read_only",
                )
            ],
        )

    result = run_react_grounding_loop(
        messages=[{"role": "user", "content": "tell me about this arbitrary thing"}],
        model_complete=model_complete,
        mcp_provider=GenericReadProvider(),
        catalog_entries=[
            ToolCatalogEntry(
                server_key="aistock-generic-read",
                tool_name="generic_lookup",
                status="enabled",
                risk_level="low",
                side_effect_level="read_only",
            )
        ],
        config=ReactGroundingConfig(max_tool_iterations=1, user_message="tell me about this arbitrary thing"),
    )

    assert result.evidence_guard.allowed is True
    assert result.evidence_guard.reason == "read_only_partial_evidence_degraded"
    assert "generic_read_source" in result.final_text
    assert "original_reason=max_tool_iterations_exhausted" in result.final_text
