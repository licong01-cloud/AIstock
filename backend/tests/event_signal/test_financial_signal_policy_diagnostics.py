from backend.services.event_signal.financial_signal_policy_diagnostics import (
    build_financial_policy_diagnostics,
    recommend_financial_policy,
)


def _metric(event_type: str, window_name: str = "T0", mean: float = -0.01, negative_rate: float = 0.62) -> dict:
    return {
        "event_type": event_type,
        "window_name": window_name,
        "rows": 100,
        "valid_raw_returns": 95,
        "mean_raw_return": mean,
        "negative_return_rate": negative_rate,
        "down_limit_rate": 0.03,
    }


def test_negative_financial_event_recommends_warn_review_not_hard_block():
    aggregates = {("financial_forecast_loss", "T0"): _metric("financial_forecast_loss")}

    rec = recommend_financial_policy("financial_forecast_loss", aggregates)

    assert rec.recommended_action == "warn_review"
    assert rec.recommended_risk_level == "P2_REVIEW"
    assert rec.enable_alpha is False
    assert rec.hard_block_candidate is False
    assert "negative_financial_event_supported_by_event_study" in rec.reason_codes


def test_positive_growth_event_remains_record_only_and_alpha_disabled():
    aggregates = {
        ("financial_forecast_large_growth", "T0"): _metric(
            "financial_forecast_large_growth",
            mean=0.01,
            negative_rate=0.45,
        )
    }

    rec = recommend_financial_policy("financial_forecast_large_growth", aggregates)

    assert rec.recommended_action == "record_only"
    assert rec.recommended_risk_level == "P3_POSITIVE_CANDIDATE"
    assert rec.enable_alpha is False
    assert "positive_alpha_disabled_until_model_validation" in rec.reason_codes


def test_expectation_miss_is_review_not_block_when_event_study_is_mixed():
    aggregates = {
        ("financial_positive_but_miss_expectation", "T0_T20"): _metric(
            "financial_positive_but_miss_expectation",
            "T0_T20",
            mean=0.02,
            negative_rate=0.49,
        )
    }

    rec = recommend_financial_policy("financial_positive_but_miss_expectation", aggregates)

    assert rec.recommended_action == "warn_review"
    assert rec.hard_block_candidate is False
    assert "expectation_miss_signal_is_review_not_block" in rec.reason_codes


def test_build_financial_policy_diagnostics_has_safe_stage_boundary():
    payload = build_financial_policy_diagnostics(
        [
            _metric("financial_forecast_loss"),
            _metric("financial_forecast_large_growth", mean=0.01, negative_rate=0.45),
        ]
    )

    assert payload["summary"]["rows"] == 2
    assert payload["summary"]["warn_review"] == 1
    assert payload["summary"]["record_only"] == 1
    assert payload["stage_boundary"]["trading_consumption_enabled"] is False
    assert payload["stage_boundary"]["alpha_overlay_enabled"] is False
    assert payload["stage_boundary"]["hard_block_enabled"] is False
