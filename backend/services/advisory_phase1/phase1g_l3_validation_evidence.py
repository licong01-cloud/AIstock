"""Compose exact typed Phase 1E input for G5 rollback validation."""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

from backend.services.advisory_phase1.phase1g_artifact_ref import (
    Phase1GImmutableArtifactResolver,
    build_phase1g_target_execution_request,
)
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY,
    Phase1GInputArtifactKind,
    Phase1GInputArtifactRef,
    Phase1GTargetExecutionRequest,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EExecutionPlanProjection,
    Phase1EOperationDisposition,
    Phase1EPlannedOperationType,
    Phase1EPlanUnitKind,
)
from backend.services.advisory_phase1.readiness_plan_store import ContentAddressedPlanStore
from backend.services.advisory_phase1.release_schema_contract import TargetLabel
from backend.services.advisory_phase1.source_resolution import SourceResolutionReceipt

from .phase1g_dev_evidence_contract import (
    L3SourceClassification,
    Phase1GDevEvidenceError,
    Phase1GDevL3SourceCandidate,
    REASON_L3_SOURCE_PENDING,
)


class Phase1GL3ValidationEvidenceComposer:
    """Republish one exact eligible source plan into a disposable external root."""

    def __init__(self, *, source_resolver: Phase1GImmutableArtifactResolver) -> None:
        self._source_resolver = source_resolver

    def compose(
        self,
        *,
        candidate: Phase1GDevL3SourceCandidate,
        ephemeral_phase1e_root: Path,
        requested_at: datetime,
    ) -> tuple[Phase1GTargetExecutionRequest, Phase1GInputArtifactRef]:
        if candidate.classification not in {
            L3SourceClassification.ELIGIBLE_SINGLE,
            L3SourceClassification.ELIGIBLE_NATIVE_MULTI,
        }:
            raise Phase1GDevEvidenceError(
                REASON_L3_SOURCE_PENDING,
                "L3 validation source candidate is not eligible",
            )
        resolved = self._source_resolver.resolve(candidate.source_phase1e_plan_ref)
        plan = resolved.payload
        if not isinstance(plan, Phase1EExecutionPlanProjection):
            raise Phase1GDevEvidenceError(
                REASON_L3_SOURCE_PENDING,
                "L3 source artifact is not a Phase 1E execution plan",
            )
        self._verify_source_closure(candidate=candidate, plan=plan)
        store = ContentAddressedPlanStore(
            root=ephemeral_phase1e_root,
            policy_hash=str(PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash),
        )
        store.publish(
            kind="plan",
            identity=plan.plan_hash,
            semantic_hash=plan.plan_hash,
            payload=plan.model_dump(mode="json"),
        )
        relative = Path(
            "advisory",
            "phase1e",
            "plans",
            plan.plan_hash[:2],
            f"{plan.plan_hash}.json",
        )
        path = store.root / relative
        raw = path.read_bytes()
        plan_ref = Phase1GInputArtifactRef(
            artifact_kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
            store_policy_hash=str(PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash),
            relative_path=relative.as_posix(),
            semantic_content_hash=plan.plan_hash,
            file_sha256=hashlib.sha256(raw).hexdigest(),
        )
        request = build_phase1g_target_execution_request(
            target_label=TargetLabel.DEV,
            release_schema_receipt_ref=candidate.release_receipt_ref,
            phase1e_plan_ref=plan_ref,
            phase1e_plan=plan,
            requested_at=requested_at,
        )
        return request, plan_ref

    @staticmethod
    def _verify_source_closure(
        *,
        candidate: Phase1GDevL3SourceCandidate,
        plan: Phase1EExecutionPlanProjection,
    ) -> None:
        if plan.plan_unit_kind is not Phase1EPlanUnitKind.ADMISSION_SCOPE:
            raise Phase1GDevEvidenceError(
                REASON_L3_SOURCE_PENDING,
                "L3 validation requires an ADMISSION_SCOPE source plan",
            )
        binding = plan.evidence_binding
        if (
            binding.package_id != candidate.package_id
            or binding.manifest_sha256 != candidate.manifest_sha256
            or binding.alpha_mode != candidate.alpha_mode.value
            or tuple(binding.manifest_alpha_component_ids)
            != candidate.component_package_ids
            or binding.selection_evidence_id != candidate.selection_evidence.identity
            or binding.selection_evidence_hash
            != candidate.selection_evidence.content_hash
            or binding.selection_artifact_id != candidate.selection_artifact.identity
            or binding.selection_artifact_payload_hash
            != candidate.selection_artifact.content_hash
            or plan.decision_trade_date != candidate.decision_trade_date
        ):
            raise Phase1GDevEvidenceError(
                REASON_L3_SOURCE_PENDING,
                "L3 source candidate differs from its immutable Phase 1E plan",
            )
        operations = {item.operation_type: item for item in plan.planned_operations}
        source = operations.get(Phase1EPlannedOperationType.SOURCE_RESOLUTION)
        observation = operations.get(Phase1EPlannedOperationType.OBSERVATION_CAPTURE)
        if (
            source is None
            or source.operation_disposition
            is not Phase1EOperationDisposition.COMPLETE_REQUEST
            or source.complete_request_payload is None
            or observation is None
            or observation.operation_disposition
            is not Phase1EOperationDisposition.SEMANTIC_TEMPLATE
        ):
            raise Phase1GDevEvidenceError(
                REASON_L3_SOURCE_PENDING,
                "L3 source plan does not contain complete source and observation semantics",
            )
        payload = source.complete_request_payload.get("source_resolution_receipt")
        try:
            receipt = SourceResolutionReceipt.model_validate(payload)
        except ValueError as exc:
            raise Phase1GDevEvidenceError(
                REASON_L3_SOURCE_PENDING,
                "L3 source resolution receipt is invalid",
            ) from exc
        observed_events = tuple(
            sorted(
                str(item.selected_availability_event_hash)
                for item in receipt.requirement_resolutions
                if item.selected_availability_event_hash is not None
            )
        )
        expected_events = tuple(sorted(item.content_hash for item in candidate.source_event_refs))
        if (
            receipt.source_resolution_receipt_hash
            != candidate.source_resolution_receipt_hash
            or observed_events != expected_events
        ):
            raise Phase1GDevEvidenceError(
                REASON_L3_SOURCE_PENDING,
                "L3 source event closure differs from inventory",
            )
