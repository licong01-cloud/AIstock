from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.historical_price_replay_contracts import (
    AdvisoryHistoricalPriceReplayRequestV1,
    AdvisoryHistoricalPriceOutcomeRowV1,
    build_historical_price_replay_receipt,
    build_historical_price_replay_request,
)


def _request(**changes):
    payload = {
        "price_range_bundle_id": "a" * 64,
        "price_range_manifest_sha256": "b" * 64,
        "prediction_source_sha256": "c" * 64,
        "decision_start_trade_date": date(2026, 1, 5),
        "decision_end_trade_date": date(2026, 1, 6),
        "replay_as_of_date": date(2026, 1, 8),
        "pit_universe_key": "canonical-pit-v1",
    }
    payload.update(changes)
    return build_historical_price_replay_request(**payload)


def test_request_is_permanently_navigation_only_historical_evidence():
    request = _request()
    assert request.evidence_level == "HISTORICAL_REPLAY"
    assert request.decision_use == "NAVIGATION_ONLY"
    assert request.sealed_holdout_consumed is False
    assert request.binding_activated is False
    payload = request.model_dump(mode="json")
    payload["evidence_level"] = "PROSPECTIVE_OOS"
    with pytest.raises(ValidationError):
        AdvisoryHistoricalPriceReplayRequestV1.model_validate(payload)


def test_request_rejects_unmatured_historical_window():
    with pytest.raises(ValidationError, match="must end before"):
        _request(replay_as_of_date=date(2026, 1, 6))


def test_outcome_contract_recomputes_interval_metrics():
    row = AdvisoryHistoricalPriceOutcomeRowV1(
        decision_as_of_trade_date=date(2026, 1, 5),
        target_trade_date=date(2026, 1, 6),
        symbol="000001.SZ",
        model_prediction_status="AVAILABLE",
        market_outcome_status="AVAILABLE",
        market_outcome_reason="target_open_observed",
        actual_open=10.0,
        decision_reference_price=10.0,
        calibrated_low=9.8,
        calibrated_mid=10.0,
        calibrated_high=10.2,
        calibrated_gap_q10=-0.02,
        calibrated_gap_q50=0.0,
        calibrated_gap_q90=0.02,
        actual_entry_gap_return=0.0,
        model_space_covered=True,
        model_space_lower_miss=False,
        model_space_upper_miss=False,
        covered=True,
        lower_miss=False,
        upper_miss=False,
        interval_width_bps=400.0,
        absolute_mid_error_bps=0.0,
    )
    assert row.covered is True
    with pytest.raises(ValidationError, match="width differs"):
        AdvisoryHistoricalPriceOutcomeRowV1(
            **{**row.model_dump(), "interval_width_bps": 399.0}
        )


def test_receipt_cannot_be_promoted_to_activation_evidence():
    from backend.services.advisory_model_first.historical_price_replay_contracts import (
        AdvisoryHistoricalPriceReplayMetricsV1,
    )

    metrics = AdvisoryHistoricalPriceReplayMetricsV1(
        decision_date_count=1,
        candidate_count=1,
        market_available_count=1,
        not_applicable_count=0,
        market_unavailable_count=0,
        model_available_market_available_count=1,
        model_unavailable_count=0,
        calibrated_coverage=1.0,
        lower_miss_rate=0.0,
        upper_miss_rate=0.0,
        mean_interval_width_bps=400.0,
        median_interval_width_bps=400.0,
        mean_absolute_mid_error_bps=0.0,
        median_absolute_mid_error_bps=0.0,
        model_space_coverage=1.0,
        model_space_lower_miss_rate=0.0,
        model_space_upper_miss_rate=0.0,
        tick_rounding_rescue_count=0,
        tick_rounding_harm_count=0,
    )
    receipt = build_historical_price_replay_receipt(
        status="PUBLISHED",
        replay_id=_request().replay_id,
        request_sha256=_request().request_sha256,
        prediction_snapshot_sha256="d" * 64,
        outcome_sha256="e" * 64,
        manifest_sha256="f" * 64,
        published_at=datetime(2026, 1, 8, tzinfo=timezone.utc),
        metrics=metrics,
    )
    assert receipt.decision_use == "NAVIGATION_ONLY"
    assert receipt.binding_activated is False
