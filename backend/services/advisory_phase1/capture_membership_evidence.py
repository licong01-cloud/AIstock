"""Phase 1C-2 immutable evidence memberships for an existing capture batch."""

from __future__ import annotations

from backend.services.advisory_phase1.capture_foundation import CaptureMembership, CapturePlan
from backend.services.advisory_phase1.observation_capture import canonical_signal_id_for_plan
from backend.services.advisory_phase1.observation_selector import (
    ObservationSelectionStatus,
    SelectedObservationMapping,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.source_resolution import (
    ResearchReadiness,
    SourceRequirementSet,
    SourceResolutionResult,
)


REASON_CAPTURE_SOURCE_RESOLUTION_INVALID = "ADVISORY_PHASE1_CAPTURE_SOURCE_RESOLUTION_INVALID"
REASON_CAPTURE_SELECTED_MAPPING_INVALID = "ADVISORY_PHASE1_CAPTURE_SELECTED_MAPPING_INVALID"


def build_phase1c2_capture_memberships(
    *,
    plan: CapturePlan,
    requirement_set: SourceRequirementSet,
    resolution_result: SourceResolutionResult,
    selected_mapping: SelectedObservationMapping | None = None,
) -> tuple[CaptureMembership, ...]:
    """Return exact evidence memberships without reading a mutable current record.

    The caller still adds its existing trace-outbox membership.  This helper
    adds only the Phase 1C-2 source requirement, resolution and optional
    selected-observation evidence specified by the approved design.
    """

    receipt = resolution_result.receipt
    source_revision_set = resolution_result.source_revision_set
    if (
        receipt.readiness is ResearchReadiness.BLOCKED
        or not receipt.can_create_capture_plan
        or source_revision_set is None
        or plan.package_id != requirement_set.package_id
        or plan.manifest_sha256 != requirement_set.manifest_sha256
        or plan.program_id != requirement_set.program_id
        or plan.binding_version_id != requirement_set.binding_version_id
        or plan.alpha_mode != requirement_set.alpha_mode
        or plan.decision_as_of_trade_date != requirement_set.decision_as_of_trade_date.isoformat()
        or plan.decision_cutoff_ts != requirement_set.requested_source_cutoff
        or plan.handoff_readiness_hash != requirement_set.handoff_readiness_hash
        or plan.admission_scope_id != requirement_set.admission_scope_id
        or plan.admission_scope_hash != requirement_set.admission_scope_hash
        or plan.calendar_hash != requirement_set.calendar_hash
        or plan.universe_policy_hash != requirement_set.universe_policy_hash
        or plan.evidence_scope != requirement_set.evidence_scope
        or receipt.source_requirement_set_id != requirement_set.source_requirement_set_id
        or receipt.source_requirement_set_hash != requirement_set.source_requirement_set_hash
        or receipt.source_revision_set_id != source_revision_set.source_revision_set_id
        or receipt.source_revision_set_hash != source_revision_set.source_revision_set_hash
        or plan.signal_source_revision_set_id != source_revision_set.source_revision_set_id
        or plan.signal_source_revision_set_hash != source_revision_set.source_revision_set_hash
    ):
        raise SourceLedgerError(
            REASON_CAPTURE_SOURCE_RESOLUTION_INVALID,
            "capture plan does not bind one capture-eligible exact source resolution",
        )
    memberships = [
        CaptureMembership(
            evidence_role="source_requirement_set",
            evidence_id=str(requirement_set.source_requirement_set_id),
            evidence_content_hash=str(requirement_set.source_requirement_set_hash),
        ),
        CaptureMembership(
            evidence_role="source_resolution_receipt",
            evidence_id=str(receipt.source_resolution_receipt_id),
            evidence_content_hash=str(receipt.source_resolution_receipt_hash),
        ),
        CaptureMembership(
            evidence_role="source_revision_set",
            evidence_id=source_revision_set.source_revision_set_id,
            evidence_content_hash=source_revision_set.source_revision_set_hash,
        ),
    ]
    if selected_mapping is not None:
        expected_canonical_signal_id = canonical_signal_id_for_plan(plan)
        if (
            selected_mapping.selection_status is not ObservationSelectionStatus.SELECTED
            or selected_mapping.canonical_signal_id != expected_canonical_signal_id
            or selected_mapping.requested_source_cutoff != requirement_set.requested_source_cutoff
            or selected_mapping.requested_source_cutoff != plan.decision_cutoff_ts
            or selected_mapping.required_capability != plan.capability
            or selected_mapping.admission_scope_id != plan.admission_scope_id
            or selected_mapping.admission_scope_hash != plan.admission_scope_hash
            or selected_mapping.handoff_readiness_hash != plan.handoff_readiness_hash
            or selected_mapping.signal_source_revision_set_hash != plan.signal_source_revision_set_hash
        ):
            raise SourceLedgerError(
                REASON_CAPTURE_SELECTED_MAPPING_INVALID,
                "capture membership cannot reference an unavailable or divergent observation mapping",
            )
        memberships.append(
            CaptureMembership(
                evidence_role="selected_observation_mapping",
                evidence_id=str(selected_mapping.selected_mapping_id),
                evidence_content_hash=str(selected_mapping.selected_mapping_hash),
            )
        )
    return tuple(sorted(memberships, key=lambda item: item.content_key))
