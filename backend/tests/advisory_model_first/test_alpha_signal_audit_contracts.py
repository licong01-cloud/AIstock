from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.alpha_signal_audit_contracts import (
    ARM_IDS,
    FUNDGROWTH_LEG_ID,
    LSTM_LEG_ID,
    PARENT_TERMINAL_WEIGHTS,
    AlphaAuditArmV1,
    build_three_arm_alpha_audit_request,
    frozen_alpha_audit_arms,
)
from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.tests.advisory_model_first.test_oracle_mini_contract import HASH_A, HASH_B, HASH_C


def _ref(role: str, digest: str) -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=f"/evidence/{role}.json",
        sha256=digest,
        size_bytes=10,
    )


def _values() -> dict:
    runs = {LSTM_LEG_ID: "run_lstm", FUNDGROWTH_LEG_ID: "run_fund"}
    return {
        "n0_completion_ref": _ref("n0_completion", HASH_B),
        "n0_completion_receipt_sha256": HASH_C,
        "research_window_contract_ref": _ref("research_window", HASH_C),
        "research_window_contract_sha256": HASH_A,
        "n1_request_ref": _ref("n1_frozen_request", HASH_A),
        "n1_request_sha256": HASH_B,
        "n1_bundle_path": "/artifacts/n1/bundle",
        "n1_bundle_manifest_ref": EvidenceReferenceV1(
            role="n1_formal_bundle_manifest",
            artifact_uri="/artifacts/n1/bundle/manifest.json",
            sha256=HASH_C,
            size_bytes=10,
        ),
        "n1_bundle_id": HASH_C,
        "registry_path": "/artifacts/n0/trial_registry.jsonl",
        "program_id": "program",
        "binding_version_id": "binding",
        "package_id": "package",
        "manifest_sha256": HASH_A,
        "selection_runtime_semantics_hash": HASH_B,
        "baseline_policy_sha256": HASH_A,
        "shadow_policy_sha256": HASH_B,
        "cost_policy_sha256": HASH_C,
        "split_policy_sha256": "d" * 64,
        "pit_spans_sha256": "e" * 64,
        "feature_schema_hash": "f" * 64,
        "representative_seed_run_ids": runs,
        "prediction_artifacts": {
            run_id: PredictionArtifactDescriptor(
                run_id=run_id,
                run_key=run_id,
                artifact_uri=f"/predictions/{run_id}.pkl",
                artifact_sha256=digest,
                size_bytes=100,
                row_count=1000,
                date_start="2024-07-04",
                date_end="2026-03-10",
            )
            for run_id, digest in (("run_lstm", HASH_A), ("run_fund", HASH_B))
        },
        "parent_terminal_weights": PARENT_TERMINAL_WEIGHTS,
        "repository_root": "/repo",
        "repository_commit": "7" * 40,
        "output_root": "/artifacts/n2a",
        "created_at": "2026-08-31T00:00:00Z",
    }


def test_three_arm_request_is_stable_and_has_zero_search_surface() -> None:
    first = build_three_arm_alpha_audit_request(**_values())
    second_values = _values()
    second_values["created_at"] = "2026-08-31T01:00:00Z"
    second = build_three_arm_alpha_audit_request(**second_values)

    assert first.request_sha256 == second.request_sha256
    assert tuple(item.arm_id for item in first.arms) == ARM_IDS
    assert first.parent_terminal_weights == PARENT_TERMINAL_WEIGHTS
    assert first.study_type.value == "ORACLE_DIAGNOSTIC"
    assert first.decision_use.value == "NAVIGATION_ONLY"


def test_three_arm_request_rejects_weight_and_roster_drift() -> None:
    values = _values()
    changed = deepcopy(PARENT_TERMINAL_WEIGHTS)
    changed[LSTM_LEG_ID] += 0.01
    values["parent_terminal_weights"] = changed
    with pytest.raises(ValidationError, match="parent terminal weights"):
        build_three_arm_alpha_audit_request(**values)

    with pytest.raises(ValidationError, match="weights differ"):
        AlphaAuditArmV1(
            arm_id="LSTM_ONLY",
            terminal_weights={LSTM_LEG_ID: 0.9},
        )


def test_three_arm_request_rejects_fourth_or_reordered_arm() -> None:
    values = _values()
    arms = list(frozen_alpha_audit_arms())
    values["arms"] = (arms[1], arms[0], arms[2])
    with pytest.raises(ValidationError, match="roster/order"):
        build_three_arm_alpha_audit_request(**values)

    values = _values()
    values["arms"] = (*arms, arms[0])
    with pytest.raises((ValidationError, ValueError), match="exactly three"):
        build_three_arm_alpha_audit_request(**values)
