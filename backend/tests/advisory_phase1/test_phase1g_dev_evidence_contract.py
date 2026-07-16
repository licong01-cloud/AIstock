from __future__ import annotations

from datetime import UTC, date, datetime
import json

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.phase1g_dev_evidence_contract import (
    AlphaMode,
    EvidenceKind,
    ExecutionMode,
    InventoryStatus,
    L3SourceClassification,
    L4TargetClassification,
    Phase1GDevEvidenceRef,
    Phase1GDevExecutionManifest,
    Phase1GDevIdentityHashRef,
    Phase1GDevInputInventoryReceipt,
    Phase1GDevL3SourceCandidate,
    Phase1GDevL4TargetCandidate,
)
from backend.services.advisory_phase1.phase1g_dev_evidence_store import (
    Phase1GDevEvidenceStore,
)
from backend.services.advisory_phase1.phase1g_contract import Phase1GInputArtifactKind
from backend.tests.advisory_phase1.phase1g_test_support import (
    database_identity,
    h,
    input_ref,
    target_request,
)


def _l3(*, multi: bool = False) -> Phase1GDevL3SourceCandidate:
    request = target_request()
    return Phase1GDevL3SourceCandidate(
        source_phase1e_plan_ref=request.phase1e_plan_ref,
        release_receipt_ref=request.release_schema_receipt_ref,
        alpha_mode=AlphaMode.MULTI if multi else AlphaMode.SINGLE,
        component_package_ids=("leg-a", "leg-b") if multi else (),
        decision_trade_date=date(2026, 7, 1),
        package_id="pkg-multi" if multi else "pkg-single",
        manifest_sha256=h("a"),
        selection_evidence=Phase1GDevIdentityHashRef(
            identity="dse-a", content_hash=h("b")
        ),
        selection_artifact=Phase1GDevIdentityHashRef(
            identity="artifact-a", content_hash=h("c")
        ),
        source_resolution_receipt_hash=h("d"),
        source_event_refs=(
            Phase1GDevIdentityHashRef(identity="event-a", content_hash=h("e")),
        ),
        classification=(
            L3SourceClassification.ELIGIBLE_NATIVE_MULTI
            if multi
            else L3SourceClassification.ELIGIBLE_SINGLE
        ),
    )


def _l4(*, multi: bool = False) -> Phase1GDevL4TargetCandidate:
    request = target_request()
    return Phase1GDevL4TargetCandidate(
        target_request=request,
        alpha_mode=AlphaMode.MULTI if multi else AlphaMode.SINGLE,
        component_package_ids=("leg-a", "leg-b") if multi else (),
        decision_trade_date=request.decision_trade_date,
        program_id=request.program_id,
        package_id="pkg-multi" if multi else "pkg-single",
        manifest_sha256=h("a"),
        admission_scope_id=request.admission_scope_id,
        admission_scope_hash=request.admission_scope_hash,
        phase1e_plan_ref=request.phase1e_plan_ref,
        dse=Phase1GDevIdentityHashRef(identity="dse-a", content_hash=h("b")),
        selection_artifact=Phase1GDevIdentityHashRef(
            identity="artifact-a", content_hash=h("c")
        ),
        source_event_refs=(
            Phase1GDevIdentityHashRef(identity="event-a", content_hash=h("e")),
        ),
        classification=(
            L4TargetClassification.EXECUTABLE_NATIVE_MULTI
            if multi
            else L4TargetClassification.EXECUTABLE_SINGLE
        ),
    )


def _inventory() -> Phase1GDevInputInventoryReceipt:
    l3 = (_l3(), _l3(multi=True))
    single = _l4()
    multi = _l4(multi=True)
    # Make the otherwise identical target request identity distinct.
    assert multi.target_request is not None
    multi_request_payload = multi.target_request.model_dump(mode="json")
    multi_request_payload.update(program_id="program-multi", request_hash=None)
    multi_request = type(multi.target_request).model_validate(multi_request_payload)
    multi_payload = multi.model_dump(mode="json")
    multi_payload.update(
        program_id="program-multi",
        target_request=multi_request.model_dump(mode="json"),
        target_candidate_hash=None,
    )
    multi = Phase1GDevL4TargetCandidate.model_validate(multi_payload)
    l4 = (single, multi)
    release = input_ref(
        kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
        semantic_hash=h("1"),
        file_sha256=h("2"),
    )
    return Phase1GDevInputInventoryReceipt(
        inventory_invocation_id="inventory-a",
        database_identity=database_identity(),
        release_receipt_refs=(release,),
        catalog_fingerprint=h("f"),
        artifact_root_policy_hashes=(h("a"), h("b")),
        l3_source_candidates=l3,
        l4_target_candidates=l4,
        l3_source_set_hash=canonical_json_sha256(
            sorted(item.source_candidate_hash for item in l3)
        ),
        l4_target_set_hash=canonical_json_sha256(
            sorted(item.target_candidate_hash for item in l4)
        ),
        l3_source_eligible_count=2,
        l4_single_executable_count=1,
        l4_native_multi_executable_count=1,
        inventory_status=InventoryStatus.L4_DUAL_TRACK_READY,
        observed_at=datetime(2026, 7, 16, tzinfo=UTC),
    )


def test_inventory_hash_and_dual_track_counts_close() -> None:
    receipt = _inventory()
    assert receipt.inventory_receipt_hash == canonical_json_sha256(
        receipt.canonical_payload()
    )
    assert receipt.inventory_status is InventoryStatus.L4_DUAL_TRACK_READY


def test_manifest_modes_cannot_mix_source_and_persistent_targets(tmp_path) -> None:
    inventory = _inventory()
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    ref = store.publish(inventory).ref
    rollback = Phase1GDevExecutionManifest(
        inventory_receipt_ref=ref,
        execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
        source_candidate_hashes=(
            str(inventory.l3_source_candidates[0].source_candidate_hash),
        ),
    )
    assert rollback.manifest_hash
    pending = Phase1GDevExecutionManifest(
        inventory_receipt_ref=ref,
        execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
    )
    assert pending.source_candidate_hashes == ()
    with pytest.raises(ValueError, match="can only carry"):
        Phase1GDevExecutionManifest(
            inventory_receipt_ref=ref,
            execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
            source_candidate_hashes=(
                str(inventory.l3_source_candidates[0].source_candidate_hash),
            ),
            target_request_hashes=(h("1"),),
        )


def test_g5_store_round_trip_and_tamper_detection(tmp_path) -> None:
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    inventory = _inventory()
    stored = store.publish(inventory)
    assert store.load(stored.ref) == inventory
    path = store.root / stored.ref.relative_path
    document = json.loads(path.read_text(encoding="utf-8"))
    document["inventory_invocation_id"] = "tampered"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(Exception, match="raw hash differs"):
        store.load(stored.ref)
    with pytest.raises(Exception, match="identity collision"):
        store.publish(inventory)


def test_evidence_ref_rejects_wrong_kind_for_manifest(tmp_path) -> None:
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    inventory_ref = store.publish(_inventory()).ref
    wrong_ref = Phase1GDevEvidenceRef(
        evidence_kind=EvidenceKind.ROLLBACK,
        relative_path="rollback/aa/" + h("a") + ".json",
        semantic_content_hash=h("a"),
        file_sha256=h("b"),
    )
    assert inventory_ref.evidence_kind is EvidenceKind.INVENTORY
    with pytest.raises(ValueError, match="inventory receipt ref"):
        Phase1GDevExecutionManifest(
            inventory_receipt_ref=wrong_ref,
            execution_mode=ExecutionMode.ROLLBACK_VALIDATION,
            source_candidate_hashes=(h("c"),),
        )
