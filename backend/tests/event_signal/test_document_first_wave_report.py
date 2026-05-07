import datetime as dt

from backend.services.event_signal.document_first_wave_report import build_first_wave_payload
from backend.services.event_signal.document_first_wave_sampler import FirstWaveConfig
from backend.services.event_signal.document_queue_refiner import refine_document_review_decision


def _decision(ann_id: int, event_type: str, title: str):
    return refine_document_review_decision(
        {
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
    )


def test_build_first_wave_payload_dedupes_then_caps_selected_rows():
    decisions = [
        _decision(1, "litigation_arbitration_freeze", "关于重大诉讼金额8000万元的公告"),
        _decision(2, "litigation_arbitration_freeze", "关于重大诉讼金额9000万元的公告"),
        _decision(3, "guarantee_financial_assistance_related_party", "关于提供担保金额1亿元的公告"),
    ]

    payload = build_first_wave_payload(
        refined_document_decisions=decisions,
        config=FirstWaveConfig(
            total_cap=2,
            default_event_type_cap=2,
            per_event_year_cap=2,
            event_type_caps={
                "litigation_arbitration_freeze": 1,
                "guarantee_financial_assistance_related_party": 1,
            },
        ),
    )

    assert payload["eligible_deduped_document_rows"] == 2
    assert payload["first_wave"]["selected_rows"] == 2
    assert payload["first_wave"]["by_event_type"]["litigation_arbitration_freeze"] == 1
    assert payload["first_wave"]["by_event_type"]["guarantee_financial_assistance_related_party"] == 1
    assert len(payload["selected_examples"]) == 2
