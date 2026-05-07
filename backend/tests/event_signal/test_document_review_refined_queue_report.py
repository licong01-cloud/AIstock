import datetime as dt

from backend.services.event_signal.document_review_refined_queue_report import build_refined_queue_summary


def _row(ann_id: int, event_type: str, title: str) -> dict:
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


def test_build_refined_queue_summary_downgrades_and_dedupes_candidates():
    rows = [
        _row(1, "litigation_arbitration_freeze", "关于重大诉讼金额8000万元的公告"),
        _row(2, "litigation_arbitration_freeze", "关于重大诉讼金额9000万元的公告"),
        _row(3, "litigation_arbitration_freeze", "关于诉讼事项进展的公告"),
        _row(4, "inquiry_concern_letter", "关于年报问询函回复的公告"),
    ]

    summary = build_refined_queue_summary(rows)

    assert summary["raw"]["rows"] == 4
    assert summary["raw"]["document_rows"] == 2
    assert summary["deduped_document_queue"]["document_rows"] == 1
    assert summary["dedupe_removed_document_rows"] == 1
    assert summary["raw"]["by_action"]["defer_until_materiality"] == 1
    assert summary["raw"]["by_action"]["sample_only"] == 1
    assert summary["top_document_candidates"][0]["event_type"] == "litigation_arbitration_freeze"
