from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.prospective_price_contracts import (
    AdvisoryPriceProspectivePredictionReceiptV1,
    build_advisory_price_prospective_prediction_receipt,
    build_frozen_price_prospective_request,
    target_open_utc,
)


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64
SHA_E = "e" * 64
SHA_F = "f" * 64


def _request_values(**overrides):
    values = {
        "created_at": datetime(2026, 9, 15, 8, 0, tzinfo=timezone.utc),
        "model_frozen_at": datetime(2026, 9, 14, 18, 10, tzinfo=timezone.utc),
        "target_open_at": target_open_utc(date(2026, 9, 16)),
        "program_id": "advp_program",
        "binding_version_id": "advb_binding",
        "list_version_id": "advl_list",
        "review_run_id": "advr_review",
        "selection_run_id": "selrun_selection",
        "candidate_count": 20,
        "candidate_symbols_sha256": SHA_F,
        "decision_as_of_trade_date": date(2026, 9, 15),
        "target_trade_date": date(2026, 9, 16),
        "package_id": "pkg_alpha",
        "manifest_sha256": SHA_A,
        "style_profile_id": "short_rebound_v1",
        "style_profile_hash": SHA_B,
        "selection_runtime_semantics_hash": SHA_C,
        "parent_bundle_id": SHA_D,
        "parent_bundle_manifest_sha256": SHA_E,
        "outcome_bundle_id": SHA_F,
        "outcome_bundle_manifest_sha256": SHA_A,
        "price_range_bundle_id": SHA_B,
        "price_range_bundle_manifest_sha256": SHA_C,
        "feature_schema_hash": SHA_D,
        "review_policy_sha256": SHA_E,
        "component_roles": {"lstm": "lstm_role", "fund": "fund_role"},
        "terminal_weights": {"lstm_role": 0.7, "fund_role": 0.3},
    }
    values.update(overrides)
    return values


def test_prospective_request_freezes_clock_identity_and_flags() -> None:
    request = build_frozen_price_prospective_request(**_request_values())

    assert request.request_id == f"advprpros_{request.request_sha256[:24]}"
    assert request.evidence_level == "PROSPECTIVE_OOS"
    assert request.realized_outcome_access_allowed is False
    assert request.target_open_at == datetime(2026, 9, 16, 1, 30, tzinfo=timezone.utc)


def test_prospective_request_hash_includes_created_at() -> None:
    first = build_frozen_price_prospective_request(**_request_values())
    second = build_frozen_price_prospective_request(
        **_request_values(created_at=datetime(2026, 9, 15, 8, 1, tzinfo=timezone.utc))
    )

    assert first.request_sha256 != second.request_sha256


@pytest.mark.parametrize(
    "overrides, message",
    [
        (
            {"created_at": datetime(2026, 9, 16, 1, 30, tzinfo=timezone.utc)},
            "before target open",
        ),
        (
            {"model_frozen_at": datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)},
            "predates the frozen model",
        ),
        ({"decision_as_of_trade_date": date(2026, 9, 16)}, "must precede"),
        (
            {"target_open_at": datetime(2026, 9, 16, 1, 31, tzinfo=timezone.utc)},
            "09:30 Asia/Shanghai",
        ),
    ],
)
def test_prospective_request_rejects_clock_drift(overrides, message) -> None:
    with pytest.raises(ValidationError, match=message):
        build_frozen_price_prospective_request(**_request_values(**overrides))


def test_prospective_request_rejects_role_weight_drift_and_extra_fields() -> None:
    with pytest.raises(ValidationError, match="differ from component roles"):
        build_frozen_price_prospective_request(**_request_values(terminal_weights={"another_role": 1.0}))

    request = build_frozen_price_prospective_request(**_request_values())
    with pytest.raises(ValidationError):
        request.model_validate({**request.model_dump(mode="json"), "realized_return": 1.0})


def test_prospective_receipt_requires_zero_side_effects_and_balanced_counts() -> None:
    values = {
        "status": "PUBLISHED",
        "request_id": "advprpros_" + "1" * 24,
        "request_sha256": SHA_A,
        "prediction_bundle_id": SHA_B,
        "prediction_sha256": SHA_C,
        "manifest_sha256": SHA_D,
        "decision_as_of_trade_date": date(2026, 9, 15),
        "target_trade_date": date(2026, 9, 16),
        "published_at": datetime(2026, 9, 15, 8, 5, tzinfo=timezone.utc),
        "candidate_count": 20,
        "available_count": 19,
        "unavailable_count": 1,
        "elapsed_seconds": 1.25,
        "parent_bundle_id": SHA_E,
        "outcome_bundle_id": SHA_F,
        "price_range_bundle_id": SHA_A,
    }
    receipt = build_advisory_price_prospective_prediction_receipt(**values)
    assert receipt.realized_outcome_accessed is False
    assert receipt.binding_activated is False
    assert receipt.database_written is False
    assert receipt.sealed_holdout_consumed is False

    with pytest.raises(ValidationError, match="do not add up"):
        build_advisory_price_prospective_prediction_receipt(**{**values, "available_count": 18})

    with pytest.raises(ValidationError, match="receipt_sha256 mismatch"):
        AdvisoryPriceProspectivePredictionReceiptV1.model_validate(
            {**receipt.model_dump(mode="json"), "elapsed_seconds": 9.0}
        )
