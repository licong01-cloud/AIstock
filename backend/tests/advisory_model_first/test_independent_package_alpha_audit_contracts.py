from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.independent_package_alpha_audit_contracts import (
    ARM_IDS,
    FACTOR_CLOSURE_50,
    FACTOR_CLOSURE_57,
    PACKAGE_378_ID,
    PACKAGE_5A5_ID,
    PACKAGE_B668_ID,
    PKG_378_ARM_ID,
    PKG_5A5_ARM_ID,
    PKG_B668_ARM_ID,
    FrozenPackageAuditArmV1,
    WorkspaceFileDescriptorV1,
    build_independent_package_alpha_audit_receipt,
    build_independent_package_alpha_audit_request,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.tests.advisory_model_first.test_oracle_mini_contract import HASH_A, HASH_B, HASH_C


def _ref(role: str, digest: str, *, uri: str | None = None) -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=uri or f"/evidence/{role}.json",
        sha256=digest,
        size_bytes=10,
    )


def _workspace_files(seed: str) -> tuple[WorkspaceFileDescriptorV1, ...]:
    names = (
        "factor_order.json",
        "manifest.json",
        "model/params.pkl",
        "strategy_package_factor_entry.py",
    )
    return tuple(
        WorkspaceFileDescriptorV1(relative_path=name, sha256=(seed * 64)[:64], size_bytes=index + 1)
        for index, name in enumerate(names)
    )


def _package(
    *, arm_id: str, package_id: str, status: str, factor_count: int, factor_closure: str, seed: str
) -> FrozenPackageAuditArmV1:
    return FrozenPackageAuditArmV1(
        arm_id=arm_id,
        package_id=package_id,
        package_status=status,
        manifest_sha256=(seed * 64)[:64],
        package_snapshot_ref=_ref(f"n2b_package_snapshot__{arm_id}", (seed * 64)[:64]),
        factor_count=factor_count,
        factor_closure_sha256=factor_closure,
        model_closure_sha256=(("f" if seed != "f" else "e") * 64),
        workspace_root=f"/artifacts/workspaces/{package_id}",
        workspace_files=_workspace_files(seed),
    )


def _packages() -> tuple[FrozenPackageAuditArmV1, ...]:
    return (
        _package(
            arm_id=PKG_378_ARM_ID,
            package_id=PACKAGE_378_ID,
            status="BACKTEST_APPROVED",
            factor_count=57,
            factor_closure=FACTOR_CLOSURE_57,
            seed="a",
        ),
        _package(
            arm_id=PKG_5A5_ARM_ID,
            package_id=PACKAGE_5A5_ID,
            status="PAPER_ENABLED",
            factor_count=57,
            factor_closure=FACTOR_CLOSURE_57,
            seed="b",
        ),
        _package(
            arm_id=PKG_B668_ARM_ID,
            package_id=PACKAGE_B668_ID,
            status="SELECTION_ENABLED",
            factor_count=50,
            factor_closure=FACTOR_CLOSURE_50,
            seed="c",
        ),
    )


def _values() -> dict:
    return {
        "n0_completion_ref": _ref("n0_completion", HASH_A),
        "n0_completion_receipt_sha256": HASH_B,
        "research_window_contract_ref": _ref("n0_window_contract", HASH_B),
        "research_window_contract_sha256": HASH_C,
        "n1_request_ref": _ref("n1_frozen_request", HASH_A),
        "n1_request_sha256": HASH_B,
        "n1_bundle_path": "/artifacts/n1/bundle",
        "n1_bundle_manifest_ref": _ref(
            "n1_formal_bundle_manifest", HASH_C, uri="/artifacts/n1/bundle/manifest.json"
        ),
        "n1_bundle_id": HASH_C,
        "n2a_request_ref": _ref("n2a_frozen_request", HASH_A),
        "n2a_request_sha256": HASH_B,
        "n2a_bundle_path": "/artifacts/n2a/bundle",
        "n2a_bundle_manifest_ref": _ref(
            "n2a_formal_bundle_manifest", HASH_C, uri="/artifacts/n2a/bundle/manifest.json"
        ),
        "n2a_bundle_id": HASH_C,
        "registry_path": "/artifacts/n0/trial_registry.jsonl",
        "program_id": "program",
        "binding_version_id": "binding",
        "current_parent_package_id": "pkg_parent",
        "current_parent_manifest_sha256": HASH_A,
        "selection_runtime_semantics_hash": HASH_B,
        "baseline_policy_sha256": HASH_A,
        "shadow_policy_sha256": HASH_B,
        "cost_policy_sha256": HASH_C,
        "split_policy_sha256": "d" * 64,
        "pit_spans_sha256": "e" * 64,
        "feature_schema_hash": "f" * 64,
        "packages": _packages(),
        "repository_root": "/repo",
        "repository_commit": "7" * 40,
        "prediction_store_root": "/artifacts/n2b/prediction_store",
        "output_root": "/artifacts/n2b",
        "created_at": "2026-08-31T00:00:00Z",
    }


def test_request_is_stable_and_has_fixed_zero_trial_surface() -> None:
    first = build_independent_package_alpha_audit_request(**_values())
    later = _values()
    later["created_at"] = "2026-08-31T01:00:00Z"
    second = build_independent_package_alpha_audit_request(**later)

    assert first.request_sha256 == second.request_sha256
    assert first.arm_ids == ARM_IDS
    assert tuple(item.package_id for item in first.packages) == (
        PACKAGE_378_ID,
        PACKAGE_5A5_ID,
        PACKAGE_B668_ID,
    )
    assert first.study_type.value == "ORACLE_DIAGNOSTIC"
    assert first.decision_use.value == "NAVIGATION_ONLY"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda rows: (rows[1], rows[0], rows[2]), "package arm order"),
        (
            lambda rows: (
                rows[0].model_copy(update={"package_status": "RETIRED"}),
                rows[1],
                rows[2],
            ),
            "lifecycle status",
        ),
        (
            lambda rows: (
                rows[0],
                rows[1].model_copy(update={"factor_closure_sha256": HASH_A}),
                rows[2],
            ),
            "factor asset closures",
        ),
    ],
)
def test_request_rejects_roster_lifecycle_and_closure_drift(mutation, message: str) -> None:  # noqa: ANN001
    values = _values()
    values["packages"] = mutation(values["packages"])
    with pytest.raises(ValidationError, match=message):
        build_independent_package_alpha_audit_request(**values)


def test_workspace_descriptor_rejects_missing_or_escaping_files() -> None:
    values = _values()
    rows = list(values["packages"])
    rows[0] = rows[0].model_copy(update={"workspace_files": rows[0].workspace_files[:-1]})
    values["packages"] = tuple(rows)
    with pytest.raises(ValidationError, match="missing required"):
        build_independent_package_alpha_audit_request(**values)

    with pytest.raises(ValidationError, match="stay relative"):
        WorkspaceFileDescriptorV1(relative_path="../model.pkl", sha256=HASH_A, size_bytes=1)


def test_receipt_requires_every_frozen_arm_and_zero_trials() -> None:
    counts = {arm_id: 1 for arm_id in ARM_IDS}
    receipt = build_independent_package_alpha_audit_receipt(
        request_sha256=HASH_A,
        source_identity_sha256=HASH_B,
        prediction_identity_sha256=HASH_C,
        causality_parity_sha256="d" * 64,
        result_files_sha256="e" * 64,
        arm_ids=ARM_IDS,
        decision_date_count=386,
        signal_row_count_by_arm=counts,
        evaluable_recall_day_count_by_arm=counts,
        evaluable_top5_day_count_by_arm=counts,
        created_at="2026-08-31T00:00:00Z",
    )
    assert receipt.planned_trial_count == 0
    assert receipt.sealed_holdout_accessed is False

    changed = deepcopy(counts)
    changed.pop(ARM_IDS[-1])
    with pytest.raises(ValidationError, match="every arm"):
        build_independent_package_alpha_audit_receipt(
            request_sha256=HASH_A,
            source_identity_sha256=HASH_B,
            prediction_identity_sha256=HASH_C,
            causality_parity_sha256="d" * 64,
            result_files_sha256="e" * 64,
            arm_ids=ARM_IDS,
            decision_date_count=386,
            signal_row_count_by_arm=changed,
            evaluable_recall_day_count_by_arm=counts,
            evaluable_top5_day_count_by_arm=counts,
            created_at="2026-08-31T00:00:00Z",
        )
