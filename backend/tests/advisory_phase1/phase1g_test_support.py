from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from backend.services.advisory_phase0a.models import HandoffReadiness
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.phase1g_contract import (
    DEFAULT_CAPTURE_POLICY_REGISTRY,
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    PHASE1G_RESULT_STORE_LAYOUT_POLICY,
    Phase1GCaptureResult,
    Phase1GInputArtifactKind,
    Phase1GInputArtifactRef,
    Phase1GSelectedObservationMapping,
    Phase1GTargetExecutionRequest,
    Phase1GTraceOutboxMapping,
)
from backend.services.advisory_phase1.readiness_plan import (
    OperationDisposition,
    Phase1EEvidenceBinding,
    Phase1EExecutionPlan,
    Phase1EPlannedOperation,
    Phase1EWorkloadProjection,
    PlanUnitKind,
    PlannedOperationType,
)
from backend.services.advisory_phase1.release_schema_contract import (
    CATALOG_FINGERPRINT_KINDS,
    CatalogFingerprintEvidence,
    DatabaseIdentity,
    ManagedSchemaStatus,
    OperationStatus,
    Phase1F1LegacyMonthInventory,
    PrerequisiteStatus,
    RECEIPT_SCHEMA_VERSION_V2,
    ReleaseSchemaReceipt,
    RequestedOperation,
    TargetLabel,
    load_release_schema_contract,
)
from backend.services.advisory_phase1.source_capacity import CAPACITY_LOGICAL_ROLES, CapacityStatus
from backend.services.advisory_phase1.source_resolution import ResearchReadiness


def h(character: str) -> str:
    return character * 64


def database_identity(
    *, target_label: TargetLabel = TargetLabel.DEV, environment_contract_hash: str | None = None
) -> DatabaseIdentity:
    return DatabaseIdentity(
        target_label=target_label,
        current_database="aistock_dev" if target_label is TargetLabel.DEV else "aistock",
        server_address="127.0.0.1",
        server_port=5432,
        server_version_num=170005,
        current_user_hash=h("a"),
        environment_contract_hash=environment_contract_hash or h("b"),
    )


def catalog_evidence(*, fingerprint: str = h("c")) -> CatalogFingerprintEvidence:
    return CatalogFingerprintEvidence(
        normalizer_version="advisory_phase1f_catalog_normalizer_v1",
        total_sha256=fingerprint,
        object_count=0,
        per_kind_counts={kind: 0 for kind in CATALOG_FINGERPRINT_KINDS},
        per_kind_hashes={kind: canonical_json_sha256([]) for kind in CATALOG_FINGERPRINT_KINDS},
    )


def release_receipt(
    *,
    target_label: TargetLabel = TargetLabel.DEV,
    environment_contract_hash: str | None = None,
    fingerprint: str = h("c"),
) -> ReleaseSchemaReceipt:
    months = (date(2026, 7, 1),)
    inventory_payload = {
        "schema_version": "advisory_phase1f1_legacy_month_inventory_v1",
        "predecessor_layout": "ABSENT",
        "lineage_row_count": 0,
        "candidate_row_count": 0,
        "legacy_months": (),
        "target_months": months,
        "legacy_months_hash": canonical_json_sha256(()),
        "target_months_hash": canonical_json_sha256(months),
    }
    inventory = Phase1F1LegacyMonthInventory(
        **inventory_payload,
        legacy_inventory_hash=canonical_json_sha256(inventory_payload),
    )
    identity = database_identity(
        target_label=target_label,
        environment_contract_hash=environment_contract_hash,
    )
    evidence = catalog_evidence(fingerprint=fingerprint)
    payload: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA_VERSION_V2,
        "operation": RequestedOperation.VERIFY,
        "requested_operation": RequestedOperation.VERIFY,
        "database_identity": identity,
        "request_content_hash": h("d"),
        "plan_content_hash": None,
        "contract_content_hash": load_release_schema_contract().contract_content_hash,
        "pre_catalog_fingerprint": None,
        "pre_catalog_evidence": None,
        "executed_migration_hashes": (),
        "per_migration_results": (),
        "executed_partitions": (),
        "post_catalog_fingerprint": fingerprint,
        "post_catalog_evidence": evidence,
        "operation_status": OperationStatus.SUCCESS,
        "managed_schema_status": ManagedSchemaStatus.COMPATIBLE,
        "prerequisite_status": PrerequisiteStatus.COMPATIBLE,
        "downstream_ready": True,
        "managed_differences": (),
        "prerequisite_differences": (),
        "legacy_inventory": inventory,
        "diagnostics": (),
        "errors": (),
        "started_at": datetime(2026, 7, 15, 1, 0, tzinfo=UTC),
        "finished_at": datetime(2026, 7, 15, 1, 1, tzinfo=UTC),
        "ddl_executed": False,
        "dml_executed": False,
        "runtime_activated": False,
    }
    return ReleaseSchemaReceipt(
        **payload,
        receipt_content_hash=canonical_json_sha256(payload),
    )


def phase1e_plan(*, artifact_store_policy_hash: str = h("e")) -> Phase1EExecutionPlan:
    scope_hash = h("1")
    scope_context = {
        "program_id": "program-a",
        "decision_trade_date": "2026-07-01",
        "admission_scope_id": "scope-a",
        "admission_scope_hash": h("2"),
        "batch_contract": {"artifact_store_policy_hash": artifact_store_policy_hash},
    }
    source_payload = {
        "schema_version": "advisory_phase1e_source_resolution_operation_v1",
        "scope_context": scope_context,
        "source_requirement_set": {},
        "source_requirement_set_id": "srs-a",
        "source_requirement_set_hash": h("3"),
        "source_resolution_receipt": {},
    }
    observation_payload = {
        "schema_version": "advisory_phase1e_request_template_v1",
        "operation": "observation_capture",
        "scope_context": scope_context,
        "source_resolution": {"source_resolution_receipt_hash": h("4")},
        "capture_plan": {"plan_hash": h("5")},
        "required_inputs": [],
    }
    observation_slots = (
        {
            "slot": "control_binding_event_hash",
            "source_type": "versioned_control_binding_event",
            "slot_schema_version": "advisory_phase1e_output_slot_v1",
            "producer_operation": "phase1g_control_binding",
            "hash_validation": "exact_event",
        },
    )
    operations = (
        Phase1EPlannedOperation(
            operation_type=PlannedOperationType.SOURCE_RESOLUTION,
            operation_disposition=OperationDisposition.COMPLETE_REQUEST,
            contract_schema_version="advisory_phase1e_source_resolution_operation_v1",
            complete_request_payload=source_payload,
            complete_request_hash=canonical_json_sha256(source_payload),
        ),
        Phase1EPlannedOperation(
            operation_type=PlannedOperationType.OBSERVATION_CAPTURE,
            operation_disposition=OperationDisposition.SEMANTIC_TEMPLATE,
            contract_schema_version="advisory_phase1e_request_template_v1",
            request_template_payload=observation_payload,
            request_template_hash=canonical_json_sha256(observation_payload),
            required_output_slots=observation_slots,
            unresolved_input_refs=observation_slots,
        ),
    )
    binding = Phase1EEvidenceBinding(
        historical_batch_id="batch-a",
        historical_batch_key="batch-key-a",
        historical_receipt_hash=h("6"),
        historical_program_run_id="run-a",
        program_payload_sha256=h("7"),
        binding_version_id="binding-a",
        binding_payload_hash=h("8"),
        package_id="package-a",
        manifest_sha256=h("9"),
        alpha_mode="single_alpha",
        resolved_style_family="mean_reversion",
        style_assignment_policy_hash=h("a"),
        selection_evidence_id="dse-a",
        selection_evidence_hash=h("b"),
        selection_artifact_id="artifact-a",
        selection_artifact_payload_hash=h("c"),
        source_watermark_hash=h("d"),
        phase0a_audit_id="audit-a",
        phase0a_audit_manifest_hash=h("e"),
        phase0a_request_hash=h("f"),
        handoff_readiness_report_hash=h("0"),
        phase1_handoff_bundle_hash=h("1"),
        admission_scope_set_hash=h("2"),
        admission_scope_id="scope-a",
        admission_scope_hash=h("2"),
        target_scope_hash=h("3"),
        oos_interval_hash=h("4"),
    )
    role_rows = {role: 1 for role in CAPACITY_LOGICAL_ROLES}
    workload = Phase1EWorkloadProjection(
        scope_plan_request_hash=scope_hash,
        style_family="mean_reversion",
        decision_trade_date=date(2026, 7, 1),
        candidate_depth=1,
        horizons=(1,),
        projection_count=1,
        stage_projection_factor=1,
        universe_size_p50=1,
        universe_size_p95=1,
        universe_size_max=1,
        role_rows_p50=role_rows,
        role_rows_p95=role_rows,
        role_rows_max=role_rows,
    )
    return Phase1EExecutionPlan(
        evidence_request_hash=h("5"),
        scope_plan_request_hash=scope_hash,
        compiler_version="phase1e-test-compiler-v1",
        serializer_version="phase1e-test-serializer-v1",
        compiler_source_hash=h("6"),
        plan_unit_kind=PlanUnitKind.ADMISSION_SCOPE,
        scope_key={
            "program_id": "program-a",
            "decision_trade_date": date(2026, 7, 1),
            "package_id": "package-a",
            "manifest_sha256": h("9"),
            "admission_scope_id": "scope-a",
            "evidence_scope": "RETROSPECTIVE_RESEARCH_ONLY",
        },
        evidence_binding=binding,
        handoff_readiness=HandoffReadiness.READY,
        source_readiness=ResearchReadiness.RESEARCH_READY,
        capacity_status=CapacityStatus.MEASURED,
        planned_operations=operations,
        workload_projection=workload,
        resource_budget_by_role={},
        memory_budget={},
        temporary_store_budget={},
        durable_store_budget={},
        capacity_request_hash=h("7"),
        capacity_receipt_hash=h("8"),
        capacity_workload_covered=True,
        resource_values_frozen=True,
    )


def write_phase1e_plan_artifact(
    *, root: Path, plan: Phase1EExecutionPlan, store_policy_hash: str
) -> tuple[Path, bytes]:
    semantic = {
        "schema_version": "advisory_phase1e_artifact_envelope_v1",
        "kind": "plan",
        "identity": plan.plan_hash,
        "semantic_hash": plan.plan_hash,
        "store_policy_hash": store_policy_hash,
        "payload": plan.model_dump(mode="json"),
    }
    document = {
        **semantic,
        "file_sha256": canonical_json_sha256(semantic),
        "materialization": {"producer_code_commit": "test", "first_written_at": "2026-07-15T01:00:00Z"},
    }
    raw = json.dumps(canonicalize(document), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    path = root / "advisory" / "phase1e" / "plans" / str(plan.plan_hash)[:2] / f"{plan.plan_hash}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return path, raw


def input_ref(
    *,
    kind: Phase1GInputArtifactKind,
    semantic_hash: str,
    file_sha256: str,
    store_policy_hash: str | None = None,
    relative_path: str | None = None,
) -> Phase1GInputArtifactRef:
    if kind is Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT:
        path = relative_path or f"receipts/{semantic_hash}.json"
        policy_hash = store_policy_hash or str(PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash)
    else:
        path = relative_path or f"advisory/phase1e/plans/{semantic_hash[:2]}/{semantic_hash}.json"
        policy_hash = store_policy_hash or h("e")
    return Phase1GInputArtifactRef(
        artifact_kind=kind,
        store_policy_hash=policy_hash,
        relative_path=path,
        semantic_content_hash=semantic_hash,
        file_sha256=file_sha256,
    )


def target_request(
    *,
    requested_at: datetime | None = None,
    receipt_relative_path: str | None = None,
    plan_relative_path: str | None = None,
) -> Phase1GTargetExecutionRequest:
    receipt = input_ref(
        kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
        semantic_hash=h("1"),
        file_sha256=h("2"),
        relative_path=receipt_relative_path,
    )
    plan = input_ref(
        kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
        semantic_hash=h("3"),
        file_sha256=h("4"),
        relative_path=plan_relative_path,
    )
    return Phase1GTargetExecutionRequest(
        target_label=TargetLabel.DEV,
        release_schema_receipt_ref=receipt,
        phase1e_plan_ref=plan,
        phase1e_plan_id="p1ep-test",
        phase1e_plan_hash=h("3"),
        source_operation_hash=h("5"),
        observation_template_hash=h("6"),
        program_id="program-a",
        decision_trade_date=date(2026, 7, 1),
        admission_scope_id="scope-a",
        admission_scope_hash=h("7"),
        capture_policy_registry_id=DEFAULT_CAPTURE_POLICY_REGISTRY.registry_id,
        capture_policy_registry_version=DEFAULT_CAPTURE_POLICY_REGISTRY.registry_version,
        capture_policy_registry_hash=str(DEFAULT_CAPTURE_POLICY_REGISTRY.registry_hash),
        result_store_policy_hash=str(PHASE1G_RESULT_STORE_LAYOUT_POLICY.layout_policy_hash),
        requested_at=requested_at or datetime(2026, 7, 15, 1, 0, tzinfo=UTC),
    )


def capture_result() -> Phase1GCaptureResult:
    selected = tuple(
        Phase1GSelectedObservationMapping(
            capture_plan_hash=h(character),
            canonical_signal_id=f"signal-{index}",
            observation_version_id=f"observation-{index}",
            observation_content_hash=h(str((index + 2) % 10)),
            lineage_id=f"lineage-{index}",
            lineage_content_hash=h(chr(ord("a") + index)),
            stage_evidence_bundle_hash=h(chr(ord("c") + index)),
            source_revision_set_id="source-set-a",
            source_revision_set_hash=h("e"),
            trace_outbox_id=f"outbox-{index}",
            trace_content_hash=h(chr(ord("f") - index)),
        )
        for index, character in enumerate(("2", "1"))
    )
    traces = tuple(
        Phase1GTraceOutboxMapping(
            capture_plan_hash=h(character),
            trace_outbox_id=f"outbox-{index}",
            trace_content_hash=h(chr(ord("f") - index)),
        )
        for index, character in enumerate(("2", "1"))
    )
    return Phase1GCaptureResult(
        target_request_hash=h("0"),
        phase1f_receipt_hash=h("1"),
        phase1f_catalog_fingerprint=h("2"),
        phase1e_plan_id="p1ep-test",
        phase1e_plan_hash=h("3"),
        source_resolution_receipt_hash=h("4"),
        source_revision_set_id="source-set-a",
        source_revision_set_hash=h("5"),
        control_binding_event_hash=h("6"),
        capture_batch_id="capture-a",
        capture_request_hash=h("7"),
        capture_attempt_no=1,
        capture_receipt_hash=h("8"),
        membership_count=2,
        membership_hash=h("9"),
        capture_plan_set_count=2,
        capture_plan_set_hash=h("a"),
        selected_observation_mappings=selected,
        trace_outbox_mappings=traces,
    )


def raw_sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()
