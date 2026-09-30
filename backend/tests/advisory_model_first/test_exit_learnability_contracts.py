from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.exit_learnability_contracts import (
    EXIT_CATEGORICAL_FEATURE_COLUMNS,
    EXIT_FEATURE_COLUMNS,
    EXIT_FEATURE_SCHEMA_VERSION,
    ExitLearnabilitySupportV1,
    build_exit_learnability_receipt,
    build_exit_learnability_request,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ResearchResultClass,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    Tier1EvidenceState,
    Tier1MetricInferenceV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _ref(role: str) -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=f"/tmp/{role}",
        sha256="a" * 64,
        size_bytes=1,
    )


def make_exit_learnability_request(**overrides: object):
    values = {
        "n2_action_request_path": "/tmp/n2-request.json",
        "n2_action_request_ref": _ref("exit_learnability_n2_action_request"),
        "n2_action_bundle_path": "/tmp/n2-bundle",
        "n2_action_manifest_ref": _ref("exit_learnability_n2_action_manifest"),
        "n2_action_receipt_ref": _ref("exit_learnability_n2_action_receipt"),
        "exit_labels_ref": _ref("exit_learnability_exit_labels"),
        "exit_decisions_ref": _ref("exit_learnability_exit_decisions"),
        "exit_episode_best_ref": _ref("exit_learnability_exit_episode_best"),
        "n1_request_path": "/tmp/n1-request.json",
        "n1_request_ref": _ref("exit_learnability_n1_request"),
        "n1_bundle_path": "/tmp/n1-bundle",
        "n1_manifest_ref": _ref("exit_learnability_n1_manifest"),
        "policy_dataset_root": "/tmp/policy",
        "policy_dataset_manifest_ref": _ref("exit_learnability_policy_dataset_manifest"),
        "candidate_episode_labels_ref": _ref("exit_learnability_candidate_episode_labels"),
        "cpcv_paths_ref": _ref("exit_learnability_cpcv_paths"),
        "parent_spike_path": "/tmp/parent.json",
        "parent_spike_ref": _ref("exit_learnability_parent_spike"),
        "research_window_contract_ref": _ref("n0_window_contract"),
        "registry_path": "/tmp/registry.jsonl",
        "route_path": "/tmp/route.md",
        "dataset_identity": "b" * 64,
        "parent_feature_schema_hash": "c" * 64,
        "feature_schema_hash": canonical_json_sha256(
            {
                "feature_schema_version": EXIT_FEATURE_SCHEMA_VERSION,
                "feature_columns": list(EXIT_FEATURE_COLUMNS),
                "categorical_columns": list(EXIT_CATEGORICAL_FEATURE_COLUMNS),
            }
        ),
        "baseline_policy_sha256": "d" * 64,
        "shadow_policy_sha256": "e" * 64,
        "cost_policy_sha256": "f" * 64,
        "intervention_policy_sha256": "1" * 64,
        "decision_start": date(2024, 7, 4),
        "decision_end": date(2026, 2, 2),
        "outcome_cutoff": date(2026, 3, 10),
        "qlib_daily_root": "/tmp/qlib",
        "repository_root": "/tmp/repo",
        "repository_commit": "2" * 40,
        "output_root": "/tmp/output",
    }
    values.update(overrides)
    return build_exit_learnability_request(**values)


def test_request_freezes_single_trial_feature_and_oof_contract() -> None:
    request = make_exit_learnability_request()

    assert request.feature_columns == EXIT_FEATURE_COLUMNS
    assert request.model_spec.alpha == 100.0
    assert request.model_spec.solver == "svd"
    assert request.model_spec.expected_ready_path_count == 28
    assert request.model_spec.expected_oof_predictions_per_row == 7
    assert request.inference_spec.exit_threshold_bps == 5.0
    assert (
        request.planned_trial_count,
        request.generated_trial_count,
        request.evaluated_trial_count,
        request.selected_trial_count,
    ) == (1, 0, 0, 0)
    assert request.sealed_holdout_accessed is False


def test_request_rejects_unknown_fields_and_feature_drift() -> None:
    payload = make_exit_learnability_request().model_dump(mode="json")
    payload["feature_columns"] = list(EXIT_FEATURE_COLUMNS[:-1])
    with pytest.raises(ValidationError, match="feature roster drift"):
        type(make_exit_learnability_request()).model_validate(payload)

    payload = make_exit_learnability_request().model_dump(mode="json")
    payload["unexpected"] = True
    with pytest.raises(ValidationError, match="Extra inputs"):
        type(make_exit_learnability_request()).model_validate(payload)


def test_receipt_classification_is_derived_from_support_power_and_interval() -> None:
    metric = Tier1MetricInferenceV1(
        point_estimate_bps=10.0,
        confidence_lower_bps=6.0,
        confidence_upper_bps=14.0,
        bootstrap_standard_error_bps=1.0,
        mde_bps=2.8,
        economic_threshold_bps=5.0,
        evidence_state=Tier1EvidenceState.HIGH,
        evaluated_day_count=100,
    )
    support = ExitLearnabilitySupportV1(
        evaluated_episode_count=500,
        evaluated_entry_day_count=100,
        evaluated_action_day_count=120,
        intervention_episode_count=100,
        intervention_action_day_count=60,
        intervention_action_day_fraction=0.5,
        intervention_days_by_regime={"UP_OR_FLAT": 30, "DOWN": 30},
        effective_intervention_block_count=3,
        support_sufficient=True,
        reason_codes=(),
    )
    receipt = build_exit_learnability_receipt(
        request_sha256="a" * 64,
        feature_schema_hash="b" * 64,
        feature_row_count=1000,
        oof_row_count=1000,
        evaluated_episode_count=500,
        evaluated_entry_day_count=100,
        row_diagnostics={},
        episode_diagnostics={},
        policy_lift=metric,
        intervention_support=support,
        oracle_mean_lift_bps=100.0,
        oracle_capture_ratio=0.1,
        evidence_sufficient=True,
        evidence_reason_codes=(),
        result_class=ResearchResultClass.CONTROL_READY,
        decision_use=DecisionUse.DIRECTION_GATE,
        source_identity_sha256="c" * 64,
        result_files_sha256="d" * 64,
        resource_report_sha256="e" * 64,
    )

    assert receipt.decision_use == DecisionUse.DIRECTION_GATE
    payload = receipt.model_dump(mode="json")
    payload["result_class"] = ResearchResultClass.EXPLORATORY.value
    with pytest.raises(ValidationError, match="classification differs"):
        type(receipt).model_validate(payload)
