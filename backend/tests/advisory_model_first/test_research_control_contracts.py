from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.research_control_contracts import (
    AdvisoryResearchWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ParentLegEvidenceV1,
    ParentPredictionExtensionStatus,
    PostCutoffInferenceEvidenceV1,
    ResearchResultClass,
    ResearchStudyType,
    ResearchWindowState,
    build_n0_completion_receipt,
    build_parent_extension_receipt,
    build_trial_record,
    build_window_access_request,
    build_window_contract,
)


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def _evidence(role: str = "fixture") -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=f"F:/fixture/{role}.json",
        sha256=HASH_A,
        size_bytes=10,
    )


def _trial_values() -> dict[str, object]:
    return {
        "experiment_id": "ADVISORY-P0-D",
        "attempt_id": "formal-v1",
        "research_stage": "STAGE_A",
        "study_type": ResearchStudyType.CANDIDATE_MODEL,
        "hypothesis_family_id": "P0_FIXED_INFORMATION_SET",
        "parent_lineage": ("P0-C",),
        "unique_variable": "binary_meta_label",
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "dataset_identity": "p0c",
        "schema_identity": "schema-v2",
        "policy_identity": HASH_B,
        "planned_trial_count": 168,
        "generated_trial_count": 168,
        "evaluated_trial_count": 168,
        "selected_trial_count": 1,
        "consumed_windows": (),
        "result_class": ResearchResultClass.NEGATIVE,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "evidence_refs": (_evidence(),),
    }


def test_trial_record_identity_is_deterministic_and_count_monotonicity_is_enforced():
    first_recorded_at = datetime(2026, 8, 30, tzinfo=timezone.utc)
    first = build_trial_record(
        **_trial_values(),
        recorded_at=first_recorded_at,
    )
    second = build_trial_record(
        **_trial_values(),
        recorded_at=first_recorded_at + timedelta(microseconds=1),
    )

    assert first.registry_entry_id == second.registry_entry_id
    assert first.record_sha256 == second.record_sha256
    assert first.recorded_at != second.recorded_at

    invalid = _trial_values()
    invalid["selected_trial_count"] = 169
    with pytest.raises(ValidationError, match="selected <= evaluated"):
        build_trial_record(**invalid)


@pytest.mark.parametrize(
    ("study_type", "decision_use"),
    (
        (ResearchStudyType.ORACLE_DIAGNOSTIC, DecisionUse.ACTIVATION_EVIDENCE),
        (ResearchStudyType.LEARNABILITY_AUDIT, DecisionUse.ACTIVATION_EVIDENCE),
        (ResearchStudyType.CANDIDATE_MODEL, DecisionUse.ACTIVATION_EVIDENCE),
        (ResearchStudyType.ACTIVATION, DecisionUse.DIRECTION_GATE),
        (ResearchStudyType.EXPLORATORY_SCREEN, DecisionUse.DIRECTION_GATE),
    ),
)
def test_activation_evidence_is_isolated_by_study_type(study_type, decision_use):
    values = _trial_values()
    values.update(study_type=study_type, decision_use=decision_use)
    with pytest.raises(ValidationError):
        build_trial_record(**values)


def test_confirmed_result_cannot_be_relabelled_from_a_candidate_study():
    values = _trial_values()
    values["result_class"] = ResearchResultClass.CONFIRMED
    with pytest.raises(ValidationError, match="CONFIRMED result requires CONFIRMATION"):
        build_trial_record(**values)


def test_incomplete_result_cannot_close_a_direction():
    values = _trial_values()
    values.update(
        result_class=ResearchResultClass.INCOMPLETE_NEGATIVE,
        decision_use=DecisionUse.DIRECTION_GATE,
    )
    with pytest.raises(ValidationError, match="incomplete results are navigation-only"):
        build_trial_record(**values)


def test_window_contract_and_access_request_hashes_bind_exact_identity():
    contract = build_window_contract(
        package_id="package",
        manifest_sha256=HASH_A,
        runtime_semantics_hash=HASH_B,
        baseline_policy_sha256=HASH_A,
        shadow_policy_sha256=HASH_B,
        cost_policy_sha256=HASH_C,
        source_policy="pit-v1",
        artifact_root_uri="F:/fixture/window-contract",
        sealed_consumption_receipt_uri=(
            "F:/fixture/window-contract/sealed_holdout_consumption_receipt.json"
        ),
        windows=(
            AdvisoryResearchWindowV1(
                window_id="dev",
                dataset_identity="dev-dataset",
                start_date=date(2024, 1, 1),
                end_date=date(2025, 1, 1),
                state=ResearchWindowState.DEVELOPMENT_CONSUMED,
                purpose="development",
            ),
            AdvisoryResearchWindowV1(
                window_id="test",
                dataset_identity="test-dataset",
                start_date=date(2024, 6, 1),
                end_date=date(2025, 1, 1),
                state=ResearchWindowState.FROZEN_TEST_CONSUMED,
                purpose="test",
            ),
            AdvisoryResearchWindowV1(
                window_id="replay",
                dataset_identity="replay-dataset",
                start_date=date(2025, 2, 1),
                end_date=date(2025, 3, 1),
                state=ResearchWindowState.HISTORICAL_REPLAY_CONSUMED,
                purpose="replay",
            ),
            AdvisoryResearchWindowV1(
                window_id="sealed",
                dataset_identity="sealed-dataset",
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                state=ResearchWindowState.SEALED_UNCONSUMED,
                purpose="confirmation",
            ),
        ),
    )
    request = build_window_access_request(
        contract_sha256=contract.contract_sha256,
        study_type=ResearchStudyType.CONFIRMATION,
        objective_contract=ObjectiveContract.ALPHA_RANKING,
        decision_use=DecisionUse.DIRECTION_GATE,
        dataset_identity="sealed-dataset",
        policy_identity=HASH_A,
        start_date="2026-01-01",
        end_date="2026-03-31",
        frontier_id="frontier-1",
        candidate_id="candidate-1",
    )

    assert request.request_id.startswith("advwindowaccess_")
    assert request.functional_payload()["start_date"] == "2026-01-01"


def test_parent_extension_and_completion_receipt_builders_are_self_verifying():
    leg = ParentLegEvidenceV1(
        leg_id="leg-a",
        representative_run_id="run-a",
        prediction_ref=_evidence("prediction"),
        prediction_row_count=100,
        prediction_date_start=date(2024, 1, 1),
        prediction_date_end=date(2026, 3, 10),
        runtime_asset_root="F:/runtime/leg-a",
        runtime_ready=True,
        runtime_refs=(_evidence("model"),),
    )
    post_cutoff = PostCutoffInferenceEvidenceV1(
        artifact_ref=_evidence("post-cutoff"),
        comparison_state_ref=_evidence("state"),
        decision_trade_date=date(2026, 5, 20),
        target_trade_date=date(2026, 5, 21),
        candidate_count=20,
        parent_candidate_artifact_hash=HASH_B,
        parent_candidate_set_hash=HASH_C,
        observed_duration_seconds=10.5,
    )
    parent = build_parent_extension_receipt(
        status=ParentPredictionExtensionStatus.FROZEN_MODEL_CAN_INFER,
        package_id="package",
        manifest_sha256=HASH_A,
        runtime_semantics_id="runtime-v1",
        runtime_semantics_hash=HASH_B,
        common_historical_prediction_cutoff=date(2026, 3, 10),
        target_extension_start=date(2026, 3, 11),
        target_extension_end=date(2026, 6, 30),
        legs=(leg,),
        post_cutoff_evidence=post_cutoff,
    )
    completion = build_n0_completion_receipt(
        registry_ref=_evidence("registry"),
        route_ref=_evidence("route"),
        parent_spike_ref=_evidence("parent"),
        window_contract_ref=_evidence("window"),
    )

    assert parent.status == ParentPredictionExtensionStatus.FROZEN_MODEL_CAN_INFER
    assert parent.receipt_id.startswith("advparentext_")
    assert completion.status == "COMPLETE"
    assert completion.next_task == "N1_TIER1_ORACLE_LEARNABILITY"
