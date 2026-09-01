from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.entry_exit_formal_contracts import (
    ENTRY_ARM_IDS,
    EXIT_INTERVENTION_POLICY_SHA256,
    ORACLE_ENTRY_POLICY_SHA256,
    ActionSupportSpecV1,
    EntryFormalArmSpecV1,
    build_n2_action_receipt,
    build_n2_action_request,
)
from backend.services.advisory_model_first.entry_guard_decision import (
    EntryGuardMode,
    build_entry_guard_policy,
)
from backend.services.advisory_model_first.research_control_contracts import (
    EvidenceReferenceV1,
)


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def _ref(role: str, digest: str = HASH_A, *, uri: str | None = None) -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=uri or f"/evidence/{role}.json",
        sha256=digest,
        size_bytes=10,
    )


def _policies_and_arms():
    policies = tuple(
        build_entry_guard_policy(mode)
        for mode in (
            EntryGuardMode.NO_GUARD,
            EntryGuardMode.FIXED_GAP_3,
            EntryGuardMode.FIXED_GAP_5,
            EntryGuardMode.FROZEN_DYNAMIC,
        )
    )
    by_mode = {item.mode: item for item in policies}
    specs = (
        ("NO_GUARD_BASELINE", EntryGuardMode.NO_GUARD, "BASELINE_TOP5", False),
        ("FIXED_3_CASH", EntryGuardMode.FIXED_GAP_3, "CASH", False),
        ("FIXED_3_REPLACE", EntryGuardMode.FIXED_GAP_3, "RANK_ONLY_REPLACEMENT", False),
        ("FIXED_5_CASH", EntryGuardMode.FIXED_GAP_5, "CASH", False),
        ("FIXED_5_REPLACE", EntryGuardMode.FIXED_GAP_5, "RANK_ONLY_REPLACEMENT", False),
        ("DYNAMIC_Q90_CASH", EntryGuardMode.FROZEN_DYNAMIC, "CASH", False),
        (
            "DYNAMIC_Q90_REPLACE",
            EntryGuardMode.FROZEN_DYNAMIC,
            "RANK_ONLY_REPLACEMENT",
            False,
        ),
        ("PERFECT_SKIP_CASH_ORACLE", None, "CASH", True),
        ("PERFECT_SKIP_REPLACE_ORACLE", None, "RANK_ONLY_REPLACEMENT", True),
    )
    arms = tuple(
        EntryFormalArmSpecV1(
            arm_id=arm_id,
            guard_mode=mode,
            guard_policy_sha256=(ORACLE_ENTRY_POLICY_SHA256 if mode is None else by_mode[mode].policy_sha256),
            fill_policy=fill_policy,
            oracle=oracle,
        )
        for arm_id, mode, fill_policy, oracle in specs
    )
    return policies, arms


def _request_values() -> dict:
    policies, arms = _policies_and_arms()
    return {
        "n1_request_path": "/artifacts/n1/request.json",
        "n1_request_ref": _ref("n2_action_n1_request"),
        "n1_bundle_path": "/artifacts/n1/bundle",
        "n1_bundle_manifest_ref": _ref("n2_action_n1_bundle_manifest"),
        "policy_dataset_manifest_ref": _ref("n2_action_policy_dataset_manifest"),
        "m4_request_path": "/artifacts/m4/request.json",
        "m4_request_ref": _ref("n2_action_m4_request"),
        "m4_bundle_path": "/artifacts/m4/bundle",
        "m4_bundle_manifest_ref": _ref("n2_action_m4_bundle_manifest"),
        "m4_predictions_ref": _ref("n2_action_m4_test_predictions"),
        "n0_completion_ref": _ref("n0_completion"),
        "parent_spike_path": "/artifacts/n0/parent_prediction_extension_receipt.json",
        "parent_spike_ref": _ref("n2_action_parent_spike"),
        "research_window_contract_ref": _ref("n0_window_contract"),
        "registry_path": "/artifacts/n0/trial_registry.jsonl",
        "route_path": "/artifacts/n0/current_route.md",
        "dataset_identity": HASH_A,
        "feature_schema_hash": HASH_B,
        "baseline_policy_sha256": HASH_A,
        "shadow_policy_sha256": HASH_B,
        "cost_policy_sha256": HASH_C,
        "entry_guard_policies": policies,
        "entry_arms": arms,
        "exit_intervention_policy_sha256": EXIT_INTERVENTION_POLICY_SHA256,
        "entry_support_spec": ActionSupportSpecV1(),
        "exit_support_spec": ActionSupportSpecV1(),
        "qlib_daily_root": "/data/qlib",
        "suspend_data_root": "/data/suspend",
        "repository_root": "/repo",
        "repository_commit": "1" * 40,
        "output_root": "/artifacts/n2",
        "created_at": "2026-09-02T00:00:00Z",
    }


def test_request_freezes_zero_trial_action_surface_and_stable_identity() -> None:
    first = build_n2_action_request(**_request_values())
    later = _request_values()
    later["created_at"] = "2026-09-02T01:00:00Z"
    second = build_n2_action_request(**later)

    assert first.request_sha256 == second.request_sha256
    assert tuple(item.arm_id for item in first.entry_arms) == ENTRY_ARM_IDS
    assert first.planned_trial_count == 0
    assert first.sealed_holdout_accessed is False
    assert first.resource_max_rss_bytes == 8 * 1024**3
    assert first.entry_support_spec.required_regimes == ("UP_OR_FLAT", "DOWN")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda values: values.update(
            entry_arms=(
                values["entry_arms"][0].model_copy(update={"fill_policy": "CASH"}),
                *values["entry_arms"][1:],
            )
        ),
        lambda values: values.update(
            entry_arms=(
                *values["entry_arms"][:-1],
                values["entry_arms"][-1].model_copy(update={"oracle": False}),
            )
        ),
        lambda values: values.update(exit_intervention_policy_sha256=HASH_A),
        lambda values: values.update(n1_request_ref=_ref("wrong_role")),
    ],
)
def test_request_rejects_arm_policy_and_evidence_drift(mutation) -> None:  # noqa: ANN001
    values = _request_values()
    mutation(values)
    with pytest.raises(ValidationError):
        build_n2_action_request(**values)


def test_receipt_is_navigation_only_and_binds_summaries() -> None:
    entry = {"arms": {"NO_GUARD_BASELINE": {"available_day_count": 60}}}
    exit_summary = {"episode_count": 100, "evaluable_episode_count": 99}
    receipt = build_n2_action_receipt(
        request_sha256=HASH_A,
        entry_summary=deepcopy(entry),
        exit_summary=deepcopy(exit_summary),
        source_identity_sha256=HASH_B,
        result_files_sha256=HASH_A,
        resource_report_sha256=HASH_C,
        created_at="2026-09-02T00:00:00Z",
    )

    assert receipt.entry_summary == entry
    assert receipt.exit_summary == exit_summary
    assert receipt.decision_use.value == "NAVIGATION_ONLY"
    assert receipt.deployable is False
