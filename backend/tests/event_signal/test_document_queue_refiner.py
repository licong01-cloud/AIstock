import datetime as dt
from decimal import Decimal

from backend.services.event_signal.document_queue_refiner import (
    REFINED_DEFER_MATERIALITY,
    build_materiality_context,
    dedupe_refined_decisions,
    extract_amounts_yuan,
    extract_percentages,
    normalize_title_signature,
    refine_document_review_decision,
    summarize_refined_decisions,
)
from backend.services.event_signal.document_review_planner import (
    LLM_STAGE_FIRST_BATCH,
    QUEUE_DOCUMENT_REQUIRED,
    QUEUE_SAMPLE_ONLY,
)


def _row(event_type: str, title: str, ann_id: int = 1) -> dict:
    return {
        "ann_id": ann_id,
        "ts_code": "000001.SZ",
        "event_type": event_type,
        "risk_level": "P2_REVIEW",
        "action": "warn_review",
        "needs_llm": "YES",
        "title": title,
        "ann_date": dt.date(2026, 5, 6),
        "effective_trade_date": dt.date(2026, 5, 7),
    }


def test_extract_amounts_and_percentages_from_chinese_title():
    amounts = extract_amounts_yuan("关于涉及诉讼金额1.25亿元及担保3000万元的公告")
    percentages = extract_percentages("占最近一期净资产6.5%的公告")

    assert amounts == (Decimal("125000000.00"), Decimal("30000000"))
    assert percentages == (Decimal("0.065"),)


def test_materiality_context_uses_title_and_explicit_context():
    materiality = build_materiality_context("涉案金额300万元", context={"amount_yuan": 80_000_000})

    assert materiality.max_amount_yuan == Decimal("80000000")
    assert materiality.material_amount is True
    assert materiality.is_material is True


def test_amount_sensitive_candidate_without_amount_is_deferred():
    decision = refine_document_review_decision(
        _row("litigation_arbitration_freeze", "关于公司诉讼事项进展的公告")
    )

    assert decision.refined_action == REFINED_DEFER_MATERIALITY
    assert decision.require_document is False
    assert "no_materiality_evidence_defer_download" in decision.reason_codes


def test_amount_sensitive_candidate_with_material_amount_is_upgraded():
    decision = refine_document_review_decision(
        _row("litigation_arbitration_freeze", "关于公司涉及重大诉讼金额8000万元的公告")
    )

    assert decision.refined_action == QUEUE_DOCUMENT_REQUIRED
    assert decision.refined_llm_stage == LLM_STAGE_FIRST_BATCH
    assert decision.require_document is True
    assert "materiality_upgrade_to_first_batch" in decision.reason_codes


def test_context_sensitive_candidate_is_sample_only_before_download():
    decision = refine_document_review_decision(
        _row("inquiry_concern_letter", "关于年报问询函回复的公告")
    )

    assert decision.refined_action == QUEUE_SAMPLE_ONLY
    assert decision.require_document is False
    assert decision.require_llm is True
    assert "context_sensitive_sample_before_download" in decision.reason_codes


def test_normalize_title_signature_removes_dates_and_numbers():
    signature = normalize_title_signature("关于2026年5月6日涉及诉讼金额8000万元的进展公告")

    assert "2026" not in signature
    assert "8000" not in signature
    assert signature.endswith("进展") is False


def test_dedupe_keeps_highest_priority_for_same_title_bucket():
    first = refine_document_review_decision(
        _row("litigation_arbitration_freeze", "关于重大诉讼金额8000万元的公告", ann_id=1)
    )
    second = refine_document_review_decision(
        _row("litigation_arbitration_freeze", "关于重大诉讼金额9000万元的公告", ann_id=2)
    )

    deduped = dedupe_refined_decisions([first, second])
    summary = summarize_refined_decisions(deduped)

    assert len(deduped) == 1
    assert summary["document_rows"] == 1
