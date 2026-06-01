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
