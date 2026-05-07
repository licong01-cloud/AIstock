from backend.services.event_signal.document_review_queue_report import estimate_queue_from_group_counts


def test_estimate_queue_from_group_counts_weights_planner_decisions():
    payload = estimate_queue_from_group_counts(
        [
            {
                "event_type": "stock_st_imposed",
                "risk_level": "P0_BLOCK",
                "action": "block_buy",
                "needs_llm": "NO",
                "rows": 10,
            },
            {
                "event_type": "audit_opinion_internal_control_risk",
                "risk_level": "P1_HIGH",
                "action": "warn_high",
                "needs_llm": "OPTIONAL",
                "rows": 3,
            },
            {
                "event_type": "inquiry_concern_letter",
                "risk_level": "P2_REVIEW",
                "action": "warn_review",
                "needs_llm": "YES",
                "rows": 7,
            },
        ]
    )

    assert payload["source_rows"] == 20
    assert payload["document_required_or_candidate_rows"] == 10
    assert payload["llm_candidate_rows"] == 10
    assert payload["document_llm_candidate_rows"] == 10
    assert payload["by_action"]["skip"] == 10
    assert payload["by_action"]["document_required"] == 3
    assert payload["by_action"]["document_candidate"] == 7
    assert payload["by_llm_stage"]["none"] == 10
    assert payload["by_llm_stage"]["first_batch"] == 3
    assert payload["by_llm_stage"]["sampled"] == 7
