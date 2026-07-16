from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.phase1g_command_factory import (
    require_existing_phase1g_result_root,
)
from backend.services.advisory_phase1.phase1g_dev_evidence import (
    Phase1GDevEvidenceService,
    _build_persistent_target_outcomes,
    _persistent_reason_set,
    _persistent_summary_status,
    verify_g5_reference_closure,
)
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1G_RESULT_STORE_LAYOUT_POLICY,
    Phase1GOutputArtifactKind,
    Phase1GOutputArtifactRef,
    REASON_PLAN_STALE,
)
from backend.services.advisory_phase1.phase1g_dev_evidence_contract import (
    AlphaMode,
    ExecutionMode,
    InventoryStatus,
    Phase1GDevEvidenceError,
    Phase1GDevExecutionManifest,
    Phase1GDevInputInventoryReceipt,
    Phase1GDevPersistentTargetOutcome,
    Phase1GDevResidueCheck,
    Phase1GDevRollbackReceipt,
    PersistentStatus,
    REASON_L3_SOURCE_PENDING,
    REASON_L4_PLAN_STALE,
    REASON_MULTI_TRACK_MISSING,
    REASON_MANIFEST_INVALID,
    REASON_REAL_INPUT_PENDING,
    REASON_REFERENCED_READBACK_FAILED,
    REASON_SINGLE_TRACK_MISSING,
    RollbackStatus,
    SummaryStatus,
)
from backend.services.advisory_phase1.phase1g_dev_evidence_store import (
    Phase1GDevEvidenceStore,
)
from backend.tests.advisory_phase1.test_phase1g_dev_evidence_contract import (
    _inventory,
)


def _bare_service(store: Phase1GDevEvidenceStore) -> Phase1GDevEvidenceService:
    service = Phase1GDevEvidenceService.__new__(Phase1GDevEvidenceService)
    service._store = store
    service._now = lambda: datetime(2026, 7, 16, 2, 0, tzinfo=UTC)
    return service


def test_g4_plan_stale_is_preserved_and_mapped_to_g5_reason() -> None:
    assert _persistent_reason_set((REASON_PLAN_STALE,)) == {
        REASON_PLAN_STALE,
        REASON_L4_PLAN_STALE,
    }


def test_persistent_summary_does_not_downgrade_total_failure() -> None:
    assert (
        _persistent_summary_status(PersistentStatus.COMPLETE_DUAL_TRACK)
        is SummaryStatus.COMPLETE
    )
    assert (
        _persistent_summary_status(PersistentStatus.PARTIAL_FAILURE)
        is SummaryStatus.PARTIAL_FAILURE
    )
    assert _persistent_summary_status(PersistentStatus.FAILED) is SummaryStatus.FAILED


def test_shared_command_factory_preserves_existing_result_root_contract(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    existing = tmp_path / "results"
    existing.mkdir()
    assert require_existing_phase1g_result_root(existing) == existing.resolve()
    with pytest.raises(FileNotFoundError):
        require_existing_phase1g_result_root(tmp_path / "missing")


def _pending_inventory() -> Phase1GDevInputInventoryReceipt:
    payload = _inventory().model_dump(mode="json")
    payload.update(
        l3_source_candidates=[],
        l4_target_candidates=[],
        l3_source_set_hash=canonical_json_sha256([]),
        l4_target_set_hash=canonical_json_sha256([]),
        l3_source_eligible_count=0,
        l4_single_executable_count=0,
        l4_native_multi_executable_count=0,
        inventory_status=InventoryStatus.L3_SOURCE_PENDING.value,
        reason_codes=[REASON_L3_SOURCE_PENDING, REASON_REAL_INPUT_PENDING],
        inventory_receipt_hash=None,
    )
    return Phase1GDevInputInventoryReceipt.model_validate(payload)


def _l4_pending_inventory() -> Phase1GDevInputInventoryReceipt:
    payload = _inventory().model_dump(mode="json")
    payload.update(
        inventory_invocation_id="l4-pending-inventory",
        l4_target_candidates=[],
        l4_target_set_hash=canonical_json_sha256([]),
        l4_single_executable_count=0,
        l4_native_multi_executable_count=0,
        inventory_status=InventoryStatus.L3_READY_L4_PENDING.value,
        reason_codes=[
            REASON_REAL_INPUT_PENDING,
            REASON_SINGLE_TRACK_MISSING,
            REASON_MULTI_TRACK_MISSING,
        ],
        inventory_receipt_hash=None,
    )
    return Phase1GDevInputInventoryReceipt.model_validate(payload)


def _complete_rollback(
    *,
    inventory: Phase1GDevInputInventoryReceipt,
    manifest: Phase1GDevExecutionManifest,
    invocation_id: str,
) -> Phase1GDevRollbackReceipt:
    return Phase1GDevRollbackReceipt(
        rollback_invocation_id=invocation_id,
        database_identity=inventory.database_identity,
        catalog_fingerprint=inventory.catalog_fingerprint,
        input_manifest_hash=str(manifest.manifest_hash),
        batch_plan_hash="b" * 64,
        observed_transactional_dml=True,
        physical_rollback_count=1,
        read_query_count=1,
        write_query_count=1,
        normalized_query_set_hash="c" * 64,
        write_relation_set=("app.advisory_capture_batch",),
        in_transaction_outcome_hash="d" * 64,
        ephemeral_result_hashes=("e" * 64,),
        ephemeral_artifacts_disposed=True,
        fresh_connection_residue_checks=(
            Phase1GDevResidueCheck(
                relation_name="app.advisory_capture_batch",
                identity_set_hash="f" * 64,
                checked_identity_count=1,
                residue_count=0,
            ),
        ),
        concurrency_probe_hash="1" * 64,
        rollback_status=RollbackStatus.COMPLETE_ZERO_RESIDUE,
        started_at=datetime(2026, 7, 16, tzinfo=UTC),
        finished_at=datetime(2026, 7, 16, 0, 1, tzinfo=UTC),
    )


def test_empty_rollback_manifest_publishes_truthful_pending_receipts(tmp_path) -> None:
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    inventory_ref = store.publish(_pending_inventory()).ref
    manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=inventory_ref,
        execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
    )
    receipt_stored, summary_stored = _bare_service(store).validate_rollback(
        inventory_ref=inventory_ref,
        manifest=manifest,
    )
    receipt = store.load(receipt_stored.ref)
    summary = store.load(summary_stored.ref)
    assert isinstance(receipt, Phase1GDevRollbackReceipt)
    assert receipt.rollback_status is RollbackStatus.NOT_RUN_SOURCE_EVIDENCE_PENDING
    assert receipt.observed_transactional_dml is False
    assert receipt.physical_rollback_count == 0
    assert summary.rollback_receipt_ref == receipt_stored.ref


def test_empty_rollback_manifest_is_rejected_when_inventory_has_sources(tmp_path) -> None:
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    inventory_ref = store.publish(_inventory()).ref
    manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=inventory_ref,
        execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
    )
    with pytest.raises(Phase1GDevEvidenceError) as caught:
        _bare_service(store).validate_rollback(
            inventory_ref=inventory_ref,
            manifest=manifest,
        )
    assert caught.value.reason_code == REASON_MANIFEST_INVALID


def test_empty_persistent_manifest_publishes_input_pending_after_l3(tmp_path) -> None:
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    inventory = _l4_pending_inventory()
    inventory_ref = store.publish(inventory).ref
    rollback_manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=inventory_ref,
        execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
        source_candidate_hashes=(
            str(inventory.l3_source_candidates[0].source_candidate_hash),
        ),
    )
    store.publish(rollback_manifest)
    rollback = _complete_rollback(
        inventory=inventory,
        manifest=rollback_manifest,
        invocation_id="rollback-complete",
    )
    rollback_ref = store.publish(rollback).ref
    manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=inventory_ref,
        execution_mode=ExecutionMode.PERSISTENT_DUAL_TRACK,
    )
    persistent_stored, _summary = _bare_service(store).capture_persistent(
        inventory_ref=inventory_ref,
        rollback_ref=rollback_ref,
        manifest=manifest,
    )
    persistent = store.load(persistent_stored.ref)
    assert persistent.persistent_status is PersistentStatus.NOT_RUN_INPUT_PENDING
    assert persistent.target_outcomes == ()


def test_persistent_rejects_rollback_manifest_from_another_inventory(tmp_path) -> None:
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    inventory = _inventory()
    inventory_ref = store.publish(inventory).ref
    other_payload = inventory.model_dump(mode="json")
    other_payload.update(
        inventory_invocation_id="other-inventory",
        inventory_receipt_hash=None,
    )
    other_ref = store.publish(
        Phase1GDevInputInventoryReceipt.model_validate(other_payload)
    ).ref
    rollback_manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=other_ref,
        execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
        source_candidate_hashes=(
            str(inventory.l3_source_candidates[0].source_candidate_hash),
        ),
    )
    store.publish(rollback_manifest)
    rollback = _complete_rollback(
        inventory=inventory,
        manifest=rollback_manifest,
        invocation_id="rollback-other-inventory",
    )
    rollback_ref = store.publish(rollback).ref
    manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=inventory_ref,
        execution_mode=ExecutionMode.PERSISTENT_DUAL_TRACK,
    )
    with pytest.raises(Phase1GDevEvidenceError) as caught:
        _bare_service(store).capture_persistent(
            inventory_ref=inventory_ref,
            rollback_ref=rollback_ref,
            manifest=manifest,
        )
    assert caught.value.reason_code == REASON_MANIFEST_INVALID


def test_complete_rollback_cannot_omit_zero_residue_evidence(tmp_path) -> None:  # type: ignore[no-untyped-def]
    inventory = _inventory()
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    inventory_ref = store.publish(inventory).ref
    manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=inventory_ref,
        execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
        source_candidate_hashes=(
            str(inventory.l3_source_candidates[0].source_candidate_hash),
        ),
    )
    payload = _complete_rollback(
        inventory=inventory,
        manifest=manifest,
        invocation_id="rollback-with-evidence",
    ).model_dump(mode="json")
    payload.update(fresh_connection_residue_checks=[], rollback_receipt_hash=None)
    with pytest.raises(ValueError, match="zero-residue evidence"):
        Phase1GDevRollbackReceipt.model_validate(payload)


def test_manifest_verification_fails_when_inventory_reference_is_dangling(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    inventory_ref = store.publish(_inventory()).ref
    manifest_ref = store.publish(
        Phase1GDevExecutionManifest(
            inventory_receipt_ref=inventory_ref,
            execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
        )
    ).ref
    (store.root / inventory_ref.relative_path).unlink()
    with pytest.raises(Phase1GDevEvidenceError) as caught:
        verify_g5_reference_closure(store=store, ref=manifest_ref)
    assert caught.value.reason_code == REASON_REFERENCED_READBACK_FAILED


def test_rollback_verification_rejects_persistent_manifest_identity(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    inventory = _inventory()
    inventory_ref = store.publish(inventory).ref
    wrong_manifest = Phase1GDevExecutionManifest(
        inventory_receipt_ref=inventory_ref,
        execution_mode=ExecutionMode.PERSISTENT_DUAL_TRACK,
        target_request_hashes=tuple(
            str(item.target_request.request_hash)
            for item in inventory.l4_target_candidates
            if item.target_request is not None
        ),
        single_target_count=1,
        native_multi_target_count=1,
    )
    store.publish(wrong_manifest)
    rollback_ref = store.publish(
        _complete_rollback(
            inventory=inventory,
            manifest=wrong_manifest,
            invocation_id="rollback-wrong-manifest-mode",
        )
    ).ref
    with pytest.raises(Phase1GDevEvidenceError) as caught:
        verify_g5_reference_closure(store=store, ref=rollback_ref)
    assert caught.value.reason_code == REASON_REFERENCED_READBACK_FAILED


def test_partial_persistent_outcome_preserves_first_durable_attempt() -> None:
    candidate = _inventory().l4_target_candidates[0]
    assert candidate.target_request is not None
    attempt_ref = Phase1GOutputArtifactRef(
        artifact_kind=Phase1GOutputArtifactKind.ATTEMPT_RECEIPT,
        store_policy_hash=str(PHASE1G_RESULT_STORE_LAYOUT_POLICY.layout_policy_hash),
        relative_path=f"attempts/{'a' * 64}.json",
        semantic_content_hash="a" * 64,
        file_sha256="b" * 64,
    )
    first_target = SimpleNamespace(
        target_request_hash=str(candidate.target_request.request_hash),
        operation_status=SimpleNamespace(value="SUCCESS"),
        reason_codes=(),
        dml_executed=True,
        committed_phases=("BATCH_CREATED",),
        capture_result_hash="c" * 64,
        attempt_receipt_ref=attempt_ref,
    )
    outcomes = _build_persistent_target_outcomes(
        candidates=(candidate,),
        first=SimpleNamespace(target_outcomes=(first_target,)),
        rerun=None,
        global_reasons={"ADVISORY_PHASE1G_G5_REFERENCED_READBACK_FAILED"},
    )
    assert len(outcomes) == 1
    assert outcomes[0].alpha_mode is candidate.alpha_mode
    assert outcomes[0].first_attempt_ref == attempt_ref
    assert outcomes[0].first_operation_status == "SUCCESS"
    assert outcomes[0].rerun_operation_status is None
    assert outcomes[0].exact_rerun_verified is False


def test_exact_rerun_rejects_reused_attempt_receipt() -> None:
    attempt_ref = Phase1GOutputArtifactRef(
        artifact_kind=Phase1GOutputArtifactKind.ATTEMPT_RECEIPT,
        store_policy_hash=str(PHASE1G_RESULT_STORE_LAYOUT_POLICY.layout_policy_hash),
        relative_path=f"attempts/{'a' * 64}.json",
        semantic_content_hash="a" * 64,
        file_sha256="b" * 64,
    )
    with pytest.raises(ValueError, match="exact rerun evidence"):
        Phase1GDevPersistentTargetOutcome(
            target_request_hash="c" * 64,
            alpha_mode=AlphaMode.SINGLE,
            first_operation_status="SUCCESS",
            rerun_operation_status="SUCCESS",
            first_dml_executed=True,
            rerun_dml_executed=False,
            first_committed_phases=("BATCH_CREATED",),
            rerun_committed_phases=(),
            stable_result_hash="d" * 64,
            first_attempt_ref=attempt_ref,
            rerun_attempt_ref=attempt_ref,
            exact_rerun_verified=True,
        )
