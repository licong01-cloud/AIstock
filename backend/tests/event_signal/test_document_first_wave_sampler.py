import datetime as dt

from backend.services.event_signal.document_first_wave_sampler import (
    FirstWaveConfig,
    select_first_wave_candidates,
    summarize_first_wave,
)
from backend.services.event_signal.document_queue_refiner import refine_document_review_decision


def _decision(ann_id: int, event_type: str, title: str, year: int):
    return refine_document_review_decision(
        {
            "ann_id": ann_id,
            "ts_code": "000001.SZ",
            "event_type": event_type,
            "risk_level": "P2_REVIEW" if event_type != "regulatory_investigation_penalty" else "P1_HIGH",
            "action": "warn_review",
            "needs_llm": "YES",
            "title": title,
            "ann_date": dt.date(year, 5, 6),
            "effective_trade_date": dt.date(year, 5, 7),
        }
    )


def test_select_first_wave_candidates_caps_by_event_type_and_year():
    decisions = [
        _decision(i, "litigation_arbitration_freeze", f"关于重大诉讼金额{8000 + i}万元的公告", 2026)
        for i in range(1, 6)
    ] + [
        _decision(100 + i, "regulatory_investigation_penalty", f"关于收到监管处罚决定书第{i}号的公告", 2025)
        for i in range(1, 6)
    ]
    config = FirstWaveConfig(
        total_cap=5,
        default_event_type_cap=2,
        per_event_year_cap=2,
        event_type_caps={"litigation_arbitration_freeze": 2, "regulatory_investigation_penalty": 3},
    )

    selected = select_first_wave_candidates(decisions, config=config)
    summary = summarize_first_wave(selected, eligible_count=len(decisions))

    assert len(selected) == 4
    assert summary["eligible_document_rows"] == 10
    assert summary["by_event_type"]["litigation_arbitration_freeze"] == 2
    assert summary["by_event_type"]["regulatory_investigation_penalty"] == 2
    assert summary["by_year"]["2026"] == 2
    assert summary["by_year"]["2025"] == 2


def test_first_wave_prefers_material_amount_over_non_material_within_event():
    material = _decision(1, "litigation_arbitration_freeze", "关于重大诉讼金额8000万元的公告", 2026)
    deferred = _decision(2, "litigation_arbitration_freeze", "关于重大诉讼进展的公告", 2026)

    selected = select_first_wave_candidates(
        [deferred, material],
        config=FirstWaveConfig(total_cap=2, default_event_type_cap=2, per_event_year_cap=2),
    )

    assert [item.base.ann_id for item in selected] == [1]
