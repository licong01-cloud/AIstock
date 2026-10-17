import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.entry_price_contracts import (
    AdvisoryEntryPriceEnvelopeV2, EntryPriceCandidateV2,
)
from backend.services.advisory_model_first.entry_price_service import score_entry_price_bundle
from backend.tests.advisory_model_first.test_entry_price_service import D, T, entry_inputs


def envelope_payload():
    row = score_entry_price_bundle(**entry_inputs())[0].candidate.model_dump(mode="json")
    return {
        "program_id": "advp_example", "binding_version_id": "advb_example",
        "package_id": "pkg_example", "package_manifest_sha256": "a" * 64,
        "style_profile_hash": "b" * 64, "review_policy_sha256": "c" * 64,
        "universe_identity_sha256": "d" * 64, "candidate_projection_sha256": "e" * 64,
        "feature_schema_sha256": "f" * 64, "role_binding_sha256": "a" * 64,
        "price_range_bundle_id": "b" * 64, "price_range_bundle_manifest_sha256": "c" * 64,
        "training_lineage": {"parent_bundle_id": "d" * 64, "outcome_bundle_id": "e" * 64},
        "decision_as_of_trade_date": D, "target_trade_date": T, "nominal_coverage": 0.8,
        "calibration_state": "CALIBRATED_INTERVAL", "availability_status": "AVAILABLE",
        "auxiliary_availability": "UNAVAILABLE", "candidate_count": 1,
        "available_count": 1, "unavailable_count": 0, "candidates": [row],
    }


def test_available_entry_does_not_claim_auxiliary_availability():
    model = AdvisoryEntryPriceEnvelopeV2(**envelope_payload())
    assert model.availability_status == "AVAILABLE"
    assert model.auxiliary_availability == "UNAVAILABLE"
    assert model.evidence_state == "EXPERIMENTAL"


@pytest.mark.parametrize("field,value", [
    ("available_count", 0), ("auxiliary_availability", "AVAILABLE"),
    ("role_binding_sha256", None), ("nominal_coverage", 0.9),
    ("target_trade_date", D), ("best_buy_minute", "09:45"),
])
def test_envelope_rejects_inconsistent_identity_counts_or_contract(field, value):
    payload = envelope_payload()
    payload[field] = value
    with pytest.raises(ValidationError):
        AdvisoryEntryPriceEnvelopeV2(**payload)


def test_zero_candidates_cannot_be_available_and_unknown_binding_remains_null():
    payload = dict(program_id="advp_example", candidate_count=0, available_count=0,
                   unavailable_count=0, availability_status="UNAVAILABLE",
                   auxiliary_availability="UNAVAILABLE", reason_code="NO_CANDIDATES", message="no candidates")
    model = AdvisoryEntryPriceEnvelopeV2(**payload)
    assert model.role_binding_sha256 is None
    payload["availability_status"] = "AVAILABLE"
    with pytest.raises(ValidationError):
        AdvisoryEntryPriceEnvelopeV2(**payload)


@pytest.mark.parametrize("invalid", ["tick", "regulatory", "nested_extra"])
def test_candidate_rejects_invalid_price_or_hidden_execution_fields(invalid):
    row = envelope_payload()["candidates"][0]
    if invalid == "tick":
        row["entry_price"]["raw_range"]["mid"] = 10.001
    elif invalid == "regulatory":
        row["entry_price"]["raw_range"]["high"] = 20
    else:
        row["entry_price"]["raw_range"]["buy_now"] = True
    with pytest.raises(ValidationError):
        EntryPriceCandidateV2(**row)


@pytest.mark.parametrize("drift", [False, True])
def test_auxiliary_attachment_requires_same_model_policy_and_projection(drift):
    from backend.services.advisory_model_first.entry_price_service import merge_auxiliary_prices
    from backend.services.advisory_model_first.price_range_inference import (
        score_price_range_bundle, available_price_range_envelope,
    )
    from backend.tests.advisory_model_first.test_entry_price_service import POLICY
    from backend.tests.advisory_model_first.test_price_range_inference import _outcome

    payload = envelope_payload()
    entry = AdvisoryEntryPriceEnvelopeV2(**payload)
    inputs = entry_inputs()
    rows = score_price_range_bundle(
        inputs["bundle"], inputs["features"], contexts=inputs["contexts"], context_unavailable=(),
        outcome_candidates=[_outcome(s) for s in inputs["features"].instrument],
        review_policy=POLICY, review_policy_sha256=entry.review_policy_sha256, target_trade_date=T,
    )
    legacy = available_price_range_envelope(
        decision_as_of_trade_date=D, target_trade_date=T, calibration_state="CALIBRATED_INTERVAL",
        nominal_coverage=0.8, package_id=entry.package_id,
        package_manifest_sha256=entry.package_manifest_sha256, style_profile_hash=entry.style_profile_hash,
        parent_bundle_id=entry.training_lineage.parent_bundle_id,
        outcome_bundle_id=entry.training_lineage.outcome_bundle_id,
        price_range_bundle_id="f" * 64 if drift else entry.price_range_bundle_id,
        model_version="frozen", review_policy_sha256=entry.review_policy_sha256,
        source_bundle_schema_version="advisory_price_range_bundle_v4", candidates=rows,
    )
    merged = merge_auxiliary_prices(entry, legacy)
    assert merged.candidates[0].entry_price == entry.candidates[0].entry_price
    assert merged.auxiliary_availability == ("UNAVAILABLE" if drift else "AVAILABLE")
    if drift:
        assert merged.candidates[0].take_profit.reason_code == "OUTCOME_ROLE_IDENTITY_MISMATCH"
