from __future__ import annotations

from backend.services.research_assistant.react_grounding import McpToolResult, ReactGroundingConfig, compose_with_evidence_guard


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

