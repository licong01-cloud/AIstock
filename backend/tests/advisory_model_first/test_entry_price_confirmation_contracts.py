from datetime import date
import json

import pandas as pd
import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.entry_price_confirmation_contracts import (
    AdvisoryEntryPriceConfirmationRequestV1, build_entry_price_confirmation_request,
)
from backend.services.advisory_model_first.research_control_contracts import build_window_contract
from backend.tests.advisory_model_first.test_entry_price_service import integrated_service


def request_values(tmp_path, *, formal=True, dates=20):
    scope = integrated_service()[2]["scope"]
    calendar = pd.bdate_range("2026-07-01", periods=dates + 1).date.tolist()
    reference = dict(role="test", artifact_uri=str(tmp_path / "evidence.json"), sha256="a" * 64, size_bytes=10)
    window = build_window_contract(
        package_id=scope.package_id, manifest_sha256=scope.package_manifest_sha256,
        runtime_semantics_hash=scope.selection_runtime_semantics_hash,
        baseline_policy_sha256=scope.review_policy_sha256, shadow_policy_sha256="a" * 64,
        cost_policy_sha256="b" * 64, source_policy="frozen", artifact_root_uri=tmp_path.as_posix(),
        sealed_consumption_receipt_uri=f"{tmp_path.as_posix()}/sealed_holdout_consumption_receipt.json",
        windows=[
            dict(window_id="training", dataset_identity="data", start_date="2025-01-01", end_date="2025-03-01", state="DEVELOPMENT_CONSUMED", purpose="train"),
            dict(window_id="validation", dataset_identity="data", start_date="2025-07-17", end_date="2025-09-24", state="DEVELOPMENT_CONSUMED", purpose="validation"),
            dict(window_id="oldtest", dataset_identity="data", start_date="2025-11-07", end_date="2026-03-10", state="FROZEN_TEST_CONSUMED", purpose="test"),
            dict(window_id="target", dataset_identity="data", start_date=calendar[1], end_date=calendar[-1], state="SEALED_UNCONSUMED", purpose="confirmation"),
        ],
    )
    return dict(
        program_id="advp_fixture", binding_version_id="advb_fixture", scope=scope,
        data_identity=dict(
            profile_generation="profile", release_id="release", data_root_uri="/node/data", complete=True,
            dataset_identity="data", vintage_evidence=reference, candidate_provenance=reference,
            latest_model_training_date="2025-07-01", latest_transform_fit_date="2025-07-01",
            latest_calibration_date="2025-09-24", latest_upstream_training_date="2025-07-01",
            consumption_review=reference,
            qualification="ELIGIBLE_LOCKED_HISTORICAL_OOT" if formal else "CONSUMED_OR_NON_VINTAGE",
        ),
        days=[dict(
            decision_as_of_trade_date=d, target_trade_date=t, list_version_id=f"list_{t}",
            review_run_id=f"review_{t}", selection_run_id=f"selection_{t}",
            candidate_symbols=[f"{i:06}.SZ" for i in range(20)], candidate_source_sha256="c" * 64,
        ) for d, t in zip(calendar, calendar[1:])],
        target_calendar=calendar[1:], replay_as_of=date(2026, 9, 28),
        control=dict(validation_labels=reference, validation_dates=["2025-07-17", "2025-09-24"], q10=-0.04, q50=0, q90=0.04),
        window_contract=window, registry_path=str(tmp_path / "registry.jsonl"),
        hypothesis_family_id="ENTRY_PRICE", parent_lineage=["v4"], frontier_id="frozen", candidate_id="v4",
        study_type="CONFIRMATION" if formal else "EXPLORATORY_SCREEN",
        decision_use="DIRECTION_GATE" if formal else "NAVIGATION_ONLY",
        evidence_level="LOCKED_HISTORICAL_OOT" if formal else "HISTORICAL_REPLAY",
    )


def test_request_roundtrip_and_result_tampering(tmp_path):
    request = build_entry_price_confirmation_request(**request_values(tmp_path))
    assert AdvisoryEntryPriceConfirmationRequestV1.model_validate_json(request.model_dump_json()) == request
    payload = request.model_dump(mode="json")
    payload["control"]["q10"] = -0.05
    with pytest.raises(ValidationError, match="hash mismatch"):
        AdvisoryEntryPriceConfirmationRequestV1.model_validate(payload)


@pytest.mark.parametrize("violation", ["future_fit", "consumed", "missing_day", "duplicate", "shallow", "threshold", "early_replay", "contract_scope"])
def test_prepare_rejects_leakage_or_changed_contract(tmp_path, violation):
    values = request_values(tmp_path)
    if violation == "future_fit":
        values["data_identity"]["latest_upstream_training_date"] = "2026-07-01"
    elif violation == "consumed":
        values["data_identity"]["qualification"] = "CONSUMED_OR_NON_VINTAGE"
    elif violation == "missing_day":
        values["days"].pop(1)
    elif violation == "duplicate":
        values["days"][0]["candidate_symbols"][1] = values["days"][0]["candidate_symbols"][0]
    elif violation == "shallow":
        values["days"][0]["candidate_symbols"].pop()
    elif violation == "threshold":
        values["criteria"] = dict(minimum_coverage=0.7)
    elif violation == "early_replay":
        values["replay_as_of"] = values["target_calendar"][-1]
    else:
        values["scope"] = values["scope"].model_copy(update={"review_policy_sha256": "d" * 64})
    with pytest.raises((ValidationError, ValueError)):
        build_entry_price_confirmation_request(**values)


@pytest.mark.parametrize("violation", [None, "vintage", "candidate", "lineage", "consumed", "incomplete", "coordinate", "coordinate_rows"])
def test_formal_qualification_requires_matching_review_contents(tmp_path, violation):
    from backend.services.advisory_model_first.entry_price_confirmation import _verify_qualification_review
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
    from backend.services.advisory_model_first.price_range_contracts import canonical_json_sha256

    values = request_values(tmp_path)
    for field in ("vintage_evidence", "candidate_provenance", "consumption_review"):
        values["data_identity"][field] = dict(values["data_identity"][field], artifact_uri=str(tmp_path / f"{field}.json"))
    request = build_entry_price_confirmation_request(**values)
    vintage = request.data_identity.model_dump(mode="json", include={
        "profile_generation", "release_id", "data_root_uri", "complete", "dataset_identity",
        "latest_model_training_date", "latest_transform_fit_date", "latest_calibration_date", "latest_upstream_training_date",
    })
    vintage.update(scope_sha256=canonical_json_sha256(request.scope.model_dump(mode="json")), pit_visibility_verified=violation != "vintage")
    vintage["entry_coordinate_review"] = dict(schema_version="advisory_entry_coordinate_review_v1", status="PASS",
        validation_labels_sha256=request.control.validation_labels.sha256,
        scope_sha256=canonical_json_sha256(request.scope.model_dump(mode="json")),
        projection_producer_version=request.projection_producer_version,
        checked_validation_rows=999 if violation == "coordinate_rows" else 1000, unavailable_rows=0,
        tolerance_abs_gap=0.000001, maximum_abs_gap_difference=0.000961 if violation == "coordinate" else 0.0000001)
    candidate = dict(days_sha256=canonical_json_sha256([day.model_dump(mode="json") for day in request.days]),
                     pit_candidate_generation_verified=violation != "candidate")
    consumption = dict(complete=violation != "incomplete", reviewed_lineage=list(request.parent_lineage),
                       hypothesis_family_id=request.hypothesis_family_id, consumed_windows=[])
    if violation == "lineage":
        consumption["reviewed_lineage"] = ["unrelated"]
    if violation == "consumed":
        consumption["consumed_windows"] = [dict(start_date="2026-07-01", end_date="2026-07-03")]
    for field, payload in zip(("vintage_evidence", "candidate_provenance", "consumption_review"), (vintage, candidate, consumption)):
        (tmp_path / f"{field}.json").write_text(json.dumps(payload), encoding="utf-8")
    if violation:
        with pytest.raises(AdvisoryModelFirstError):
            _verify_qualification_review(request, validation_rows=1000)
    else:
        _verify_qualification_review(request, validation_rows=1000)
