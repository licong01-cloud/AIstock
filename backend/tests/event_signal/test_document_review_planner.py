import datetime as dt

from backend.services.event_signal.document_review_planner import (
    LLM_STAGE_FIRST_BATCH,
    LLM_STAGE_NONE,
    LLM_STAGE_SAMPLED,
    QUEUE_DOCUMENT_CANDIDATE,
    QUEUE_DOCUMENT_REQUIRED,
    QUEUE_SKIP,
    plan_document_review,
    plan_review_batch,
    summarize_decisions,
)


def _row(event_type: str, risk_level: str = "P2_REVIEW", needs_llm: str = "YES", ann_id: int = 1) -> dict:
    return {
        "ann_id": ann_id,
        "ts_code": "000001.SZ",
        "event_type": event_type,
        "risk_level": risk_level,
        "needs_llm": needs_llm,
        "title": "sample title",
        "effective_trade_date": dt.date(2026, 5, 6),
    }


def test_st_title_risk_skips_pdf_and_llm():
    decision = plan_document_review(_row("stock_st_imposed", risk_level="P0_BLOCK", needs_llm="NO"))

    assert decision.queue_action == QUEUE_SKIP
    assert decision.llm_stage == LLM_STAGE_NONE
    assert decision.require_document is False
    assert decision.require_llm is False
    assert "title_or_structured_data_sufficient" in decision.reason_codes


def test_unclassified_archive_is_not_auto_llm():
    decision = plan_document_review(_row("unclassified_archive", risk_level="P4_NEUTRAL", needs_llm="SAMPLE_ONLY"))

    assert decision.queue_action == QUEUE_SKIP
    assert decision.llm_stage == LLM_STAGE_NONE
    assert "unclassified_archive_for_rule_mining_not_auto_llm" in decision.reason_codes


def test_first_batch_high_risk_title_requires_document_and_llm():
    decision = plan_document_review(
        _row("audit_opinion_internal_control_risk", risk_level="P1_HIGH", needs_llm="OPTIONAL")
    )

    assert decision.queue_action == QUEUE_DOCUMENT_REQUIRED
    assert decision.llm_stage == LLM_STAGE_FIRST_BATCH
    assert decision.require_document is True
    assert decision.require_llm is True
    assert decision.route_event_types == ("audit_opinion_internal_control_risk",)
    assert decision.priority_score > 85


def test_context_sensitive_inquiry_is_sampled_unless_linked_to_financial_anomaly():
    base = plan_document_review(_row("inquiry_concern_letter"))
    linked = plan_document_review(_row("inquiry_concern_letter"), context={"financial_anomaly": True})

    assert base.queue_action == QUEUE_DOCUMENT_CANDIDATE
    assert base.llm_stage == LLM_STAGE_SAMPLED
    assert "context_sensitive_sample_first" in base.reason_codes
    assert linked.queue_action == QUEUE_DOCUMENT_REQUIRED
    assert linked.llm_stage == LLM_STAGE_FIRST_BATCH
    assert "context_linked_high_value_review" in linked.reason_codes


def test_amount_sensitive_event_requires_document_only_after_material_threshold():
    small = plan_document_review(_row("litigation_arbitration_freeze"), context={"amount_yuan": 1_000_000})
    large = plan_document_review(_row("litigation_arbitration_freeze"), context={"amount_yuan": 80_000_000})

    assert small.queue_action == QUEUE_DOCUMENT_CANDIDATE
    assert "amount_sensitive_needs_threshold" in small.reason_codes
    assert large.queue_action == QUEUE_DOCUMENT_REQUIRED
    assert "amount_sensitive_high_value" in large.reason_codes
    assert "material_amount_yuan" in large.reason_codes


def test_structured_financial_titles_are_skipped_unless_anomaly_context_requests_sampling():
    neutral = plan_document_review(_row("performance_forecast_revision_impairment"))
    anomaly = plan_document_review(
        _row("performance_forecast_revision_impairment"),
        context={"financial_anomaly": True},
    )

    assert neutral.queue_action == QUEUE_SKIP
    assert "structured_financial_source_preferred" in neutral.reason_codes
    assert anomaly.queue_action == QUEUE_DOCUMENT_CANDIDATE
    assert anomaly.llm_stage == LLM_STAGE_SAMPLED
    assert "financial_structured_anomaly_linked" in anomaly.reason_codes


def test_batch_planner_dedupes_by_announcement_and_summarizes_counts():
    rows = [
        _row("stock_st_imposed", risk_level="P0_BLOCK", needs_llm="NO", ann_id=1),
        _row("audit_opinion_internal_control_risk", risk_level="P1_HIGH", needs_llm="OPTIONAL", ann_id=1),
        _row("inquiry_concern_letter", ann_id=2),
    ]

    decisions = plan_review_batch(rows)
    summary = summarize_decisions(decisions)

    assert [decision.ann_id for decision in decisions] == [1, 2]
    assert decisions[0].event_type == "audit_opinion_internal_control_risk"
    assert summary["rows"] == 2
    assert summary["document_required_or_candidate"] == 2
    assert summary["by_action"][QUEUE_DOCUMENT_REQUIRED] == 1
    assert summary["by_action"][QUEUE_DOCUMENT_CANDIDATE] == 1
