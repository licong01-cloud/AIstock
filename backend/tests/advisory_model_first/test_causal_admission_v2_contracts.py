from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from backend.services.advisory_model_first.causal_admission_v2_contracts import (
    CAUSAL_ADMISSION_FEATURE_COLUMNS,
    EXPANDING_ARM_ID,
    STATIC_ARM_ID,
    FrozenAdvisoryCausalAdmissionRequestV2,
    build_causal_admission_receipt,
    build_causal_admission_request,
)


def _request_values() -> dict[str, object]:
    return {
        "parent_v1_bundle_path": "/tmp/v1",
        "parent_v1_bundle_id": "1" * 64,
        "parent_v1_request_sha256": "2" * 64,
        "parent_v1_manifest_sha256": "3" * 64,
        "package_id": "pkg_test",
        "manifest_sha256": "4" * 64,
        "program_id": "program_test",
        "binding_version_id": "binding_test",
        "policy_identity": "5" * 64,
        "shadow_policy_sha256": "6" * 64,
        "cost_policy_sha256": "7" * 64,
        "dataset_identity": "8" * 64,
        "cumulative_evaluated_trial_count_prior": 1284,
        "cumulative_candidate_index_prior": 88,
        "registry_path": "/tmp/registry.jsonl",
        "registry_sha256_at_request": "9" * 64,
        "registry_record_count_at_request": 32,
        "auxiliary_route_path": "/tmp/current_auxiliary_route.md",
        "auxiliary_route_sha256_at_request": "b" * 64,
        "repository_root": "/tmp/repo",
        "repository_commit": "a" * 40,
        "output_root": "/tmp/output",
        "created_at": datetime(2026, 9, 7, tzinfo=timezone.utc),
    }


def test_request_freezes_r0_arms_features_and_limits() -> None:
    request = build_causal_admission_request(**_request_values())

    assert [arm.arm_id for arm in request.arms] == [STATIC_ARM_ID, EXPANDING_ARM_ID]
    assert [arm.candidate_index for arm in request.arms] == [89, 90]
    assert len(CAUSAL_ADMISSION_FEATURE_COLUMNS) == 20
    assert request.planned_trial_count == 2
    assert request.max_estimator_fit_count == 32
    assert request.pre_run_mde_bps == 44.2485434246
    assert request.confirmatory_capable is False
    assert request.request_id == f"advcausal_{request.request_sha256[:24]}"
    encoded = json.dumps(request.model_dump(mode="json"))
    assert FrozenAdvisoryCausalAdmissionRequestV2.model_validate_json(encoded) == request


def test_request_rejects_result_time_arm_or_gate_drift() -> None:
    request = build_causal_admission_request(**_request_values())
    payload = request.model_dump()
    payload["action_utility_buffer_bps"] = 0.0
    with pytest.raises(ValueError):
        FrozenAdvisoryCausalAdmissionRequestV2.model_validate(payload)

    payload = request.model_dump()
    payload["arms"][1]["candidate_index"] = 99
    with pytest.raises(ValueError, match="candidate indices"):
        FrozenAdvisoryCausalAdmissionRequestV2.model_validate(payload)

    payload = request.model_dump()
    payload["inner_start"] = "2024-11-29"
    with pytest.raises(ValueError, match="dates differ"):
        FrozenAdvisoryCausalAdmissionRequestV2.model_validate(payload)


def test_receipt_never_becomes_activation_evidence() -> None:
    receipt = build_causal_admission_receipt(
        request_sha256="1" * 64,
        bundle_id="2" * 64,
        evidence_class="CAUSAL_ADMISSION_V2_1_SELECTED_ZERO",
        evaluated_trial_count=2,
        selected_trial_count=0,
        selected_arm_id=None,
    )
    assert receipt.runtime_eligible is False
    assert receipt.sealed_holdout_accessed is False
    assert receipt.receipt_id == f"advcausalrcpt_{receipt.receipt_sha256[:24]}"
