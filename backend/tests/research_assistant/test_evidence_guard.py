from __future__ import annotations

from backend.services.research_assistant.react_grounding import (
    McpToolResult,
    ReactGroundingConfig,
    compose_with_evidence_guard,
)


def _result() -> McpToolResult:
    return McpToolResult(
        server_key="server",
        tool_name="tool",
        status="succeeded",
        summary="summary",
        source_refs=["test://source"],
        as_of="2026-06-01",
        payload_json={"summary_first": True},
    )


def _section_only_stock_result() -> McpToolResult:
    return McpToolResult(
        server_key="aistock-stock-analysis",
        tool_name="stock_analysis_get_quote",
        status="succeeded",
        summary="section-level stock evidence",
        payload_json={
            "response_mode": "stock_analysis_evidence_card",
            "sections": [
                {
                    "dataset": "quote",
                    "summary": "quote evidence",
                    "source_refs": ["stock-ref:quote:000688"],
                    "as_of": "2026-06-16",
                },
                {
                    "dataset": "fund_flow",
                    "summary": "fund-flow evidence",
                    "source_refs": ["stock-ref:fund_flow:000688"],
                    "as_of": "2026-06-16",
                },
            ],
        },
        executed=True,
    )


def _recoverable_catalog_rejection() -> McpToolResult:
    return McpToolResult(
        server_key="aistock-local-data",
        tool_name="local_data_get_unack_alert_count",
        status="rejected",
        summary="approved capability not found for tool",
        error_json={
            "reason_code": "capability_not_found",
            "catalog_reason": "tool_not_in_audited_catalog",
            "server_key": "aistock-local-data",
            "tool_name": "local_data_get_unack_alert_count",
            "exception_type": "KeyError",
            "message": "approved capability not found for tool: aistock-local-data/local_data_get_unack_alert_count",
            "recoverable_catalog_rejection": True,
        },
        executed=False,
        blocked_reason="capability_not_found",
    )


def _local_data_success_result() -> McpToolResult:
    return McpToolResult(
        server_key="aistock-local-data",
        tool_name="local_data_get_preset_daily_status",
        status="succeeded",
        summary="success=1 failed=0",
        source_refs=["local_data_facade_read_adapter"],
        as_of="2026-06-17",
        payload_json={"source": "local_data_facade_read_adapter", "as_of": "2026-06-17"},
        executed=True,
    )


def test_evidence_guard_allows_sourced_numeric_answer() -> None:
    decision = compose_with_evidence_guard(
        "Metric is 12%; source=test://source as_of=2026-06-01.",
        [_result()],
        ReactGroundingConfig(max_tool_iterations=2),
    )

    assert decision.allowed is True
    assert "12%" in decision.text


def test_evidence_guard_blocks_unsourced_numeric_answer() -> None:
    decision = compose_with_evidence_guard(
        "Metric is 12%.",
        [],
        ReactGroundingConfig(max_tool_iterations=2),
    )

    assert decision.allowed is False
    assert "Insufficient evidence" in decision.text


def test_evidence_guard_blocks_placeholders() -> None:
    decision = compose_with_evidence_guard(
        "PE is XX and revenue is approxX.",
        [_result()],
        ReactGroundingConfig(max_tool_iterations=2),
    )

    assert decision.allowed is False
    assert "placeholder" in decision.reason


def test_evidence_guard_blocks_fabricated_source_and_date_tokens() -> None:
    decision = compose_with_evidence_guard(
        "Metric is 12%; source=fake://made-up as_of=2099-01-01.",
        [_result()],
        ReactGroundingConfig(max_tool_iterations=2),
    )

    assert decision.allowed is False
    assert decision.reason == "missing_inline_tool_evidence"


def test_bug_413_multisource_paraphrase_records_actual_guard_reason() -> None:
    decision = compose_with_evidence_guard(
        "国城矿业近期走势需要结合行情和资金流观察，不能只看单一指标。",
        [_section_only_stock_result()],
        ReactGroundingConfig(
            max_tool_iterations=2,
            user_message="国城矿业 基本情况/近期走势/未来趋势 全方位分析",
        ),
    )

    assert decision.allowed is False
    assert decision.reason == "missing_inline_tool_evidence"
    assert decision.source_count == 2
    assert decision.as_of_count == 1


def test_evidence_guard_accepts_section_level_source_and_as_of_citations() -> None:
    decision = compose_with_evidence_guard(
        "国城矿业近期走势结合行情和资金流观察；来源 stock-ref:quote:000688，截至 2026-06-16。",
        [_section_only_stock_result()],
        ReactGroundingConfig(
            max_tool_iterations=2,
            user_message="国城矿业 基本情况/近期走势/未来趋势 全方位分析",
        ),
    )

    assert decision.allowed is True
    assert decision.reason == "ok"


def test_bug_404_413_grounded_answer_takes_precedence_over_recovered_catalog_error() -> None:
    decision = compose_with_evidence_guard(
        "Local data sync succeeded: success=1 failed=0; source=local_data_facade_read_adapter as_of=2026-06-17.",
        [_recoverable_catalog_rejection(), _local_data_success_result()],
        ReactGroundingConfig(max_tool_iterations=3),
    )

    assert decision.allowed is True
    assert decision.reason == "ok"
    assert "capability_not_found" not in decision.text


def test_bug_404_413_unrecovered_catalog_error_replaces_insufficient() -> None:
    decision = compose_with_evidence_guard(
        "Insufficient evidence: no covered local-data result.",
        [_recoverable_catalog_rejection()],
        ReactGroundingConfig(max_tool_iterations=3),
    )

    assert decision.allowed is False
    assert decision.reason == "explicit_tool_error"
    assert "reason_code=capability_not_found" in decision.text
    assert "tool_not_in_audited_catalog" in decision.text
    assert "Insufficient evidence" not in decision.text
