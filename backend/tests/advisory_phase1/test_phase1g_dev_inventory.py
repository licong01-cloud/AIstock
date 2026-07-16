from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_text
from backend.services.advisory_phase1.phase1g_artifact_ref import (
    Phase1GArtifactRootBinding,
    Phase1GImmutableArtifactResolver,
)
from backend.services.advisory_phase1.phase1g_command_factory import Phase1GCommandContext
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY,
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    Phase1GInputArtifactKind,
)
from backend.services.advisory_phase1.phase1g_dev_evidence_contract import (
    InventoryStatus,
    L3SourceClassification,
    L4TargetClassification,
    Phase1GDevEvidenceError,
    REASON_L3_SOURCE_PENDING,
    REASON_REAL_INPUT_PENDING,
    REASON_UNEXPECTED_ERROR,
)
from backend.services.advisory_phase1.phase1g_dev_inventory import (
    Phase1GDevInventory,
    _inventory_reason,
)
from backend.services.advisory_phase1.phase1g_schema_guard import Phase1GSchemaGuardEvidence
from backend.services.advisory_phase1.release_schema_contract import TargetLabel
from backend.services.advisory_phase1.readiness_plan import (
    HandoffReadiness,
    OperationDisposition,
    Phase1EExecutionPlan,
    PlannedOperationType,
    PlanUnitKind,
)
from backend.tests.advisory_phase1.phase1g_test_support import (
    phase1e_plan,
    release_receipt,
    write_phase1e_plan_artifact,
)


class _SchemaGuard:
    def __init__(self, evidence: Phase1GSchemaGuardEvidence) -> None:
        self.evidence = evidence
        self.calls = 0

    def verify(self, **_kwargs):  # type: ignore[no-untyped-def]
        self.calls += 1
        return self.evidence


class _KnownStalePlanningService:
    def plan_batch(self, _request):  # type: ignore[no-untyped-def]
        raise ValueError("typed test plan is intentionally incomplete")


def test_inventory_reports_pending_without_fabricating_l3_or_l4(tmp_path) -> None:
    release_root = tmp_path / "release"
    phase1e_root = tmp_path / "phase1e"
    release_root.mkdir()
    phase1e_root.mkdir()
    receipt = release_receipt()
    receipt_path = release_root / "receipts" / f"{receipt.receipt_content_hash}.json"
    receipt_path.parent.mkdir()
    receipt_path.write_text(
        canonical_json_text(receipt.model_dump(mode="json")) + "\n",
        encoding="utf-8",
    )
    resolver = Phase1GImmutableArtifactResolver(
        bindings=(
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                root=release_root,
                expected_store_policy_hash=str(
                    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash
                ),
            ),
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
                root=phase1e_root,
                expected_store_policy_hash=str(
                    PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash
                ),
            ),
        )
    )
    context = Phase1GCommandContext(
        connection_config=SimpleNamespace(target_label=TargetLabel.DEV),
        artifact_resolver=resolver,
        result_store=None,
        service=None,
    )
    inventory = Phase1GDevInventory(
        context=context,
        release_receipt_root=release_root,
        phase1e_artifact_root=phase1e_root,
        now_provider=lambda: datetime(2026, 7, 16, tzinfo=UTC),
    )
    evidence = Phase1GSchemaGuardEvidence(
        release_receipt_hash=receipt.receipt_content_hash,
        catalog_fingerprint=receipt.post_catalog_fingerprint,
        database_identity=receipt.database_identity,
    )
    guard = _SchemaGuard(evidence)
    inventory._schema_guard = guard
    result = inventory.run()
    assert guard.calls == 1
    assert result.inventory_status is InventoryStatus.L3_SOURCE_PENDING
    assert result.l3_source_candidates == ()
    assert result.l4_target_candidates == ()
    assert REASON_L3_SOURCE_PENDING in result.reason_codes
    assert REASON_REAL_INPUT_PENDING in result.reason_codes
    assert result.release_receipt_refs[0].file_sha256 == hashlib.sha256(
        receipt_path.read_bytes()
    ).hexdigest()


def test_inventory_does_not_follow_reparse_like_plan_file(tmp_path) -> None:
    # A malformed regular file is reported as invalid input evidence and never a target.
    release_root = tmp_path / "release"
    phase1e_root = tmp_path / "phase1e"
    release_root.mkdir()
    phase1e_root.mkdir()
    receipt = release_receipt()
    receipt_path = release_root / "receipts" / f"{receipt.receipt_content_hash}.json"
    receipt_path.parent.mkdir()
    receipt_path.write_text(
        canonical_json_text(receipt.model_dump(mode="json")) + "\n",
        encoding="utf-8",
    )
    bad = phase1e_root / "advisory" / "phase1e" / "plans" / "aa" / ("a" * 64 + ".json")
    bad.parent.mkdir(parents=True)
    bad.write_text(json.dumps({"semantic_hash": "a" * 64}), encoding="utf-8")
    resolver = Phase1GImmutableArtifactResolver(
        bindings=(
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                root=release_root,
                expected_store_policy_hash=str(PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash),
            ),
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
                root=phase1e_root,
                expected_store_policy_hash=str(PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash),
            ),
        )
    )
    context = Phase1GCommandContext(
        connection_config=SimpleNamespace(target_label=TargetLabel.DEV),
        artifact_resolver=resolver,
        result_store=None,
        service=None,
    )
    inventory = Phase1GDevInventory(
        context=context,
        release_receipt_root=release_root,
        phase1e_artifact_root=phase1e_root,
        now_provider=lambda: datetime(2026, 7, 16, tzinfo=UTC),
    )
    inventory._schema_guard = _SchemaGuard(
        Phase1GSchemaGuardEvidence(
            release_receipt_hash=receipt.receipt_content_hash,
            catalog_fingerprint=receipt.post_catalog_fingerprint,
            database_identity=receipt.database_identity,
        )
    )
    result = inventory.run()
    assert result.l3_source_eligible_count == 0
    assert result.l4_single_executable_count == 0


def test_inventory_retains_incomplete_and_diagnostic_plan_classifications(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    release_root = tmp_path / "release"
    phase1e_root = tmp_path / "phase1e"
    release_root.mkdir()
    phase1e_root.mkdir()
    receipt = release_receipt()
    receipt_path = release_root / "receipts" / f"{receipt.receipt_content_hash}.json"
    receipt_path.parent.mkdir()
    receipt_path.write_text(
        canonical_json_text(receipt.model_dump(mode="json")) + "\n",
        encoding="utf-8",
    )
    incomplete = phase1e_plan(
        artifact_store_policy_hash=str(
            PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash
        )
    )
    write_phase1e_plan_artifact(
        root=phase1e_root,
        plan=incomplete,
        store_policy_hash=str(
            PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash
        ),
    )
    diagnostic_payload = incomplete.model_dump(mode="python")
    binding = dict(diagnostic_payload["evidence_binding"])
    binding.update(
        admission_scope_id=None,
        admission_scope_hash=None,
        phase1_handoff_bundle_hash=None,
        admission_scope_set_hash=None,
        oos_interval_hash=None,
        evidence_binding_hash=None,
    )
    diagnostic_payload.update(
        scope_key=None,
        target_key={
            "program_id": "program-diagnostic",
            "decision_trade_date": datetime(2026, 7, 2, tzinfo=UTC).date(),
            "package_id": binding["package_id"],
            "manifest_sha256": binding["manifest_sha256"],
            "audit_target_id": "audit-target-diagnostic",
            "target_scope_hash": binding["target_scope_hash"],
        },
        evidence_binding=binding,
        plan_unit_kind=PlanUnitKind.TARGET_DIAGNOSTIC,
        handoff_readiness=HandoffReadiness.BLOCKED,
        source_readiness=None,
        capacity_status=None,
        reason_codes=("ADVISORY_PHASE1E_NO_ADMISSION_SCOPE",),
        missing_evidence=({"reason_code": "ADVISORY_PHASE1E_NO_ADMISSION_SCOPE"},),
        planned_operations=tuple(
            {
                "operation_type": operation_type,
                "operation_disposition": OperationDisposition.NOT_APPLICABLE,
                "contract_schema_version": "advisory_phase1e_template_v1",
            }
            for operation_type in PlannedOperationType
        ),
        workload_projection=None,
        resource_budget_by_role=None,
        memory_budget=None,
        temporary_store_budget=None,
        durable_store_budget=None,
        missing_capacity_measurements=(),
        capacity_request_hash=None,
        capacity_receipt_hash=None,
        capacity_workload_covered=None,
        resource_values_frozen=None,
        plan_hash=None,
        plan_id=None,
    )
    diagnostic = Phase1EExecutionPlan.model_validate(diagnostic_payload)
    write_phase1e_plan_artifact(
        root=phase1e_root,
        plan=diagnostic,
        store_policy_hash=str(
            PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash
        ),
    )
    resolver = Phase1GImmutableArtifactResolver(
        bindings=(
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                root=release_root,
                expected_store_policy_hash=str(
                    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash
                ),
            ),
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
                root=phase1e_root,
                expected_store_policy_hash=str(
                    PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash
                ),
            ),
        )
    )
    inventory = Phase1GDevInventory(
        context=Phase1GCommandContext(
            connection_config=SimpleNamespace(target_label=TargetLabel.DEV),
            artifact_resolver=resolver,
            result_store=None,
            service=_KnownStalePlanningService(),
        ),
        release_receipt_root=release_root,
        phase1e_artifact_root=phase1e_root,
        now_provider=lambda: datetime(2026, 7, 16, tzinfo=UTC),
    )
    inventory._schema_guard = _SchemaGuard(
        Phase1GSchemaGuardEvidence(
            release_receipt_hash=receipt.receipt_content_hash,
            catalog_fingerprint=receipt.post_catalog_fingerprint,
            database_identity=receipt.database_identity,
        )
    )

    result = inventory.run()

    assert [item.classification for item in result.l3_source_candidates] == [
        L3SourceClassification.INCOMPLETE
    ]
    assert result.l3_source_candidates[0].source_resolution_receipt_hash is None
    assert {item.classification for item in result.l4_target_candidates} == {
        L4TargetClassification.DIAGNOSTIC,
        L4TargetClassification.STALE,
    }
    assert all(not item.executable for item in result.l4_target_candidates)


def test_inventory_unknown_exception_is_not_downgraded_to_pending() -> None:
    with pytest.raises(Phase1GDevEvidenceError) as caught:
        _inventory_reason(
            RuntimeError("unexpected implementation failure"),
            default=REASON_REAL_INPUT_PENDING,
            operation="plan_l4_target",
        )
    assert caught.value.reason_code == REASON_UNEXPECTED_ERROR
    Phase1GDevEvidenceError,
