from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.daily_price_envelope_contracts import (
    AdvisoryDailyPriceEnvelopeV1,
)


def _candidate(symbol: str = "000001.SZ") -> dict[str, object]:
    return {
        "symbol": symbol,
        "status": "EXPERIMENTAL_SHADOW",
        "availability_status": "AVAILABLE",
        "projection_condition": "NEXT_TRADING_DAY_VALID_OPEN_AT_PREDICTED_ENTRY_MID",
        "decision_reference_price": 10.0,
        "decision_price_trade_date": "2026-07-20",
        "target_raw_price_multiplier": 1.0,
        "entry_price_range": {
            "condition": "NEXT_TRADING_DAY_VALID_OPEN",
            "low": 9.9,
            "mid": 10.0,
            "high": 10.1,
        },
        "calibrated_entry_price_range": None,
        "entry_gap_calibration": {
            "state": "UNCALIBRATED",
            "method": None,
            "delta": None,
            "nominal_coverage": 0.8,
        },
        "take_profit_price": {"low": 10.5, "high": 11.0, "horizon_trade_days": 5},
        "protective_price": {
            "status": "NOT_APPLICABLE",
            "policy_activation_price": None,
            "model_peak_low": None,
            "model_peak_high": None,
            "floor_low": None,
            "floor_high": None,
        },
        "stop_loss_price": {
            "status": "AVAILABLE",
            "low": 9.2,
            "high": 9.5,
            "hard_stop_price": 9.2,
        },
        "tick_size": 0.01,
        "regulatory_price_range": {
            "status": "LIMITED",
            "low": 9.0,
            "high": 11.0,
            "rule_id": "MAIN_10PCT_V1",
            "source": "DECISION_TIME_BOARD_ST_RULE",
        },
        "review_policy": {
            "review_policy_sha256": "a" * 64,
            "stop_loss_bps": 800,
            "take_profit_bps": 1800,
            "trailing_stop_bps": 700,
            "take_profit_mode": "trailing",
        },
        "reason_code": None,
        "message": None,
    }


def _envelope() -> dict[str, object]:
    return {
        "status": "EXPERIMENTAL_SHADOW",
        "availability_status": "AVAILABLE",
        "decision_as_of_trade_date": "2026-07-20",
        "target_trade_date": "2026-07-21",
        "calibration_state": "UNCALIBRATED",
        "nominal_coverage": 0.8,
        "package_id": "package-1",
        "package_manifest_sha256": "b" * 64,
        "style_profile_hash": "c" * 64,
        "parent_bundle_id": "parent-1",
        "outcome_bundle_id": "outcome-1",
        "price_range_bundle_id": "price-1",
        "model_version": "advprreq-runtime",
        "review_policy_sha256": "a" * 64,
        "source_bundle_schema_version": "advisory_price_range_bundle_v1",
        "candidates": [_candidate()],
        "reason_code": None,
        "message": None,
    }


def test_available_envelope_has_frozen_daily_identity_and_no_binary_output() -> None:
    payload = AdvisoryDailyPriceEnvelopeV1(**_envelope()).as_payload()

    assert payload["schema_version"] == "advisory_daily_price_envelope_v1"
    assert payload["objective_contract"] == "RISK_MANAGED_ADVISORY"
    assert payload["price_basis"] == "UNADJUSTED_CNY_DECISION_CLOSE"
    assert payload["entry_admission_model_status"] == "RETIRED_NON_IDENTIFIABLE"
    assert "entry_executable_probability" not in payload["candidates"][0]


@pytest.mark.parametrize("field", ["best_buy_minute", "order_quantity", "fill_probability"])
def test_execution_and_minute_fields_are_forbidden(field: str) -> None:
    payload = _envelope()
    candidate = deepcopy(payload["candidates"][0])
    candidate[field] = 1
    payload["candidates"] = [candidate]

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        AdvisoryDailyPriceEnvelopeV1(**payload)


def test_available_envelope_fails_closed_on_missing_identity() -> None:
    payload = _envelope()
    payload["price_range_bundle_id"] = None

    with pytest.raises(ValidationError, match="incomplete identity"):
        AdvisoryDailyPriceEnvelopeV1(**payload)


def test_envelope_availability_must_match_candidate_statuses() -> None:
    payload = _envelope()
    payload["availability_status"] = "PARTIAL"

    with pytest.raises(ValidationError, match="does not match candidate statuses"):
        AdvisoryDailyPriceEnvelopeV1(**payload)


def test_unavailable_envelope_requires_typed_error_and_no_candidates() -> None:
    payload = AdvisoryDailyPriceEnvelopeV1(
        status="PRICE_RANGE_UNAVAILABLE",
        availability_status="UNAVAILABLE",
        calibration_state="UNCALIBRATED",
        reason_code="ADVISORY_PRICE_RANGE_BUNDLE_UNAVAILABLE",
        message="no compatible bundle",
    ).as_payload()

    assert payload["candidates"] == []
    assert payload["reason_code"] == "ADVISORY_PRICE_RANGE_BUNDLE_UNAVAILABLE"


def test_target_trade_date_must_follow_decision_trade_date() -> None:
    payload = _envelope()
    payload["target_trade_date"] = payload["decision_as_of_trade_date"]

    with pytest.raises(ValidationError, match="must follow decision trade date"):
        AdvisoryDailyPriceEnvelopeV1(**payload)


def test_candidate_calibration_state_must_match_calibrated_range() -> None:
    payload = _envelope()
    candidate = deepcopy(payload["candidates"][0])
    candidate["calibrated_entry_price_range"] = deepcopy(
        candidate["entry_price_range"]
    )
    payload["candidates"] = [candidate]

    with pytest.raises(ValidationError, match="does not match calibrated range"):
        AdvisoryDailyPriceEnvelopeV1(**payload)


def test_envelope_identity_must_match_available_candidate_identity() -> None:
    payload = _envelope()
    candidate = deepcopy(payload["candidates"][0])
    candidate["review_policy"] = deepcopy(candidate["review_policy"])
    candidate["review_policy"]["review_policy_sha256"] = "d" * 64
    payload["candidates"] = [candidate]

    with pytest.raises(ValidationError, match="review policy differs"):
        AdvisoryDailyPriceEnvelopeV1(**payload)


def test_envelope_rejects_duplicate_candidate_symbols() -> None:
    payload = _envelope()
    payload["candidates"] = [_candidate(), _candidate()]
    payload["availability_status"] = "AVAILABLE"

    with pytest.raises(ValidationError, match="duplicate candidate symbols"):
        AdvisoryDailyPriceEnvelopeV1(**payload)
