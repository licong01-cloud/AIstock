from __future__ import annotations

from typing import Any

from backend.services.research_assistant.react_grounding import (
    McpToolCall,
    McpToolResult,
    ModelTurn,
    ReactGroundingConfig,
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


def test_future_answer_blocks_directional_prediction_without_driver_scenario_risk_boundary() -> None:
    decision = compose_with_evidence_guard(
        "明天一定会上涨；来源 stock_ref，截至 2026-06-17。",
        [_result("stock_analysis_get_quote", source="stock_ref", as_of="2026-06-17")],
        ReactGroundingConfig(max_tool_iterations=4, user_message="这只股票未来趋势如何？"),
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

