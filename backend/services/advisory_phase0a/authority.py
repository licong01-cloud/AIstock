"""Phase 0A.1 handoff, approval, and authorization contracts.

This module is deliberately side-effect free. Database persistence lives in
``approval_repository`` so the chain and hash rules can be exercised with
fixture receipts before any authority migration is applied.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .policy import canonical_json_sha256


HANDOFF_SCHEMA_VERSION = "advisory_phase0a_handoff_bundle_v2"
APPROVAL_DECISION_SCHEMA_VERSION = "advisory_phase0a_approval_decision_v2"
APPROVAL_BUNDLE_SCHEMA_VERSION = "advisory_phase0a_approval_bundle_v2"
OPERATION_AUTHORIZATION_SCHEMA_VERSION = "advisory_phase1_operation_authorization_v1"


class Phase0AAuthorityError(RuntimeError):
    """Raised when a handoff, chain, or action authorization is not admissible."""


class DecisionKind(str, Enum):
    GLOBAL = "GLOBAL"
    ADMISSION_SCOPE = "ADMISSION_SCOPE"


class DecisionEventType(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    REVOKE = "REVOKE"


class EvidenceScope(str, Enum):
    FORMAL_OOS = "FORMAL_OOS"
    RETROSPECTIVE_RESEARCH_ONLY = "RETROSPECTIVE_RESEARCH_ONLY"
    GAP_ONLY = "GAP_ONLY"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class AuthorizationEventType(str, Enum):
    AUTHORIZE = "AUTHORIZE"
    REVOKE = "REVOKE"


class AdmissionScope(BaseModel):
    """One independently approvable Phase 0A context/interval/capability."""

    model_config = ConfigDict(extra="forbid")

    admission_scope_id: str
    admission_scope_hash: str
    audit_target_id: str
    target_handoff_hash: str
    package_id: str
    manifest_sha256: str
    phase0a_signal_context_hash: str
    oos_interval_id: str
    oos_interval_hash: str
    capability: str
    capability_hash: str
    date_start: date
    date_end: date
    allowed_evidence_scope: EvidenceScope
    stable_signal_semantics_payload_v1: dict[str, Any]
    stable_signal_semantics_hash: str

    @model_validator(mode="after")
    def _valid_range(self) -> "AdmissionScope":
        if self.date_end < self.date_start:
            raise ValueError("admission scope date_end must not precede date_start")
        return self


class TargetHandoff(BaseModel):
    model_config = ConfigDict(extra="forbid")

    audit_target_id: str
    target_scope_hash: str
    admission_scopes: list[AdmissionScope]
    target_handoff_hash: str


class HandoffBundle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = HANDOFF_SCHEMA_VERSION
    audit_id: str
    audit_manifest_hash: str
    request_hash: str
    serializer_version: str
    global_handoff_hashes: dict[str, str]
    sorted_target_handoffs: list[TargetHandoff]
    initial_approval_receipt_hash: str
    phase1_handoff_bundle_hash: str
    created_at: datetime


class ApprovalDecisionRequest(BaseModel):
    """CLI/repository input. The scope itself is resolved from the handoff."""

    model_config = ConfigDict(extra="forbid")

    decision_kind: DecisionKind
    event_type: DecisionEventType
    approval_reference: str
    admission_scope_id: str | None = None
    previous_terminal_decision_hash: str | None = None
    revokes_decision_hash: str | None = None


class ApprovalDecisionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = APPROVAL_DECISION_SCHEMA_VERSION
    decision_id: str
    decision_kind: DecisionKind
    event_type: DecisionEventType
    audit_id: str
    audit_manifest_hash: str
    request_hash: str
    initial_approval_receipt_hash: str
    phase1_handoff_bundle_hash: str
    audit_target_id: str | None = None
    target_handoff_hash: str | None = None
    admission_scope_id: str | None = None
    admission_scope_hash: str | None = None
    stable_signal_semantics_hash: str | None = None
    phase0a_signal_context_hash: str | None = None
    oos_interval_id: str | None = None
    capability: str | None = None
    date_start: date | None = None
    date_end: date | None = None
    allowed_evidence_scope: EvidenceScope
    previous_terminal_decision_hash: str | None = None
    revokes_decision_hash: str | None = None
    actor_principal: str
    authority_backend_id: str
    decision_at: datetime
    approval_reference: str
    decision_hash: str


class ApprovalBundleScope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    admission_scope_id: str
    admission_scope_hash: str
    terminal_decision_hash: str
    allowed_evidence_scope: EvidenceScope
    scope_member_content_hash: str


class ApprovalBundle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approval_bundle_id: str
    schema_version: str = APPROVAL_BUNDLE_SCHEMA_VERSION
    audit_id: str
    audit_manifest_hash: str
    request_hash: str
    initial_approval_receipt_hash: str
    phase1_handoff_bundle_hash: str
    global_terminal_decision_hash: str
    admission_scope_set_hash: str
    scope_member_count: int
    authority_backend_id: str
    authority_backend_hash: str
    scopes: list[ApprovalBundleScope]
    approval_bundle_content_hash: str
    created_by: str
    created_at: datetime


class OperationAuthorizationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authorization_id: str
    event_type: AuthorizationEventType
    operation_type: str
    environment: str
    approval_bundle_hash: str | None = None
    admission_scope_set_hash: str | None = None
    governance_scope_hash: str | None = None
    operation_payload_hash: str
    max_rows: int | None = Field(default=None, ge=0)
    max_bytes: int | None = Field(default=None, ge=0)
    valid_from: datetime
    expires_at: datetime
    previous_event_hash: str | None = None
    revokes_event_hash: str | None = None
    approval_reference: str

    @model_validator(mode="after")
    def _valid_window(self) -> "OperationAuthorizationRequest":
        if self.expires_at <= self.valid_from:
            raise ValueError("authorization expires_at must be after valid_from")
        return self


class OperationAuthorizationEvent(OperationAuthorizationRequest):
    schema_version: str = OPERATION_AUTHORIZATION_SCHEMA_VERSION
    actor_principal: str
    event_at: datetime
    authorization_event_hash: str


_HANDOFF_HASH_KEYS = (
    "source_availability_matrix_hash",
    "universe_survivorship_hash",
    "asset_runtime_hmm_ledger_hash",
    "oos_interval_report_hash",
    "candidate_authority_stage_capability_hash",
    "metric_label_policy_hash",
    "prior_registry_hash",
    "multiple_testing_registry_hash",
    "policy_registry_hash",
)

_BUSINESS_OPERATIONS = {
    "TRACE_CAPTURE_ACTIVATE",
    "CAPTURE_DML",
    "CAPTURE_RECOVER",
    "LABEL_DML",
    "BUILD_CREATE",
    "BUILD_TERMINATE",
    "MATERIALIZE",
    "VERIFY_REGISTER",
    "PROMOTE",
    "SEAL",
    "RECOVER",
}
_GOVERNANCE_OPERATIONS = {
    "SOURCE_LEDGER_ACTIVATE",
    "SOURCE_LEDGER_WRITE",
    "STORE_INIT",
    "INVALIDATE",
    "RESERVATION_RELEASE",
    "STAGING_CLEANUP",
    "GC_MARK_QUARANTINE",
    "GC_DELETE",
    "SCHEDULER_ACTIVATE",
}


def build_handoff_bundle(*, receipt_dir: Path, created_at: datetime | None = None) -> HandoffBundle:
    """Build a deterministic handoff from one immutable Phase 0A receipt directory."""

    artifacts = _load_receipt_artifacts(receipt_dir)
    manifest = artifacts["audit_manifest.json"]
    approval_receipt = artifacts["approval_receipt.json"]
    _validate_receipt_integrity(manifest=manifest, approval_receipt=approval_receipt)

    handoff_hashes = manifest.get("phase1_handoff_hashes")
    if not isinstance(handoff_hashes, dict) or set(handoff_hashes) != set(_HANDOFF_HASH_KEYS):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: incomplete phase1_handoff_hashes")
    if any(not _sha256(value) for value in handoff_hashes.values()):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: malformed handoff hash")

    target_registry = artifacts["target_scope_registry.json"]
    runtime_entries = artifacts["runtime_semantics_ledger.json"].get("entries", [])
    capability_entries = artifacts["candidate_authority_stage_capability_report.json"].get("entries", [])
    intervals = artifacts["oos_interval_report.json"].get("intervals", [])
    classifications = artifacts["oos_interval_report.json"].get("classifications", [])
    lineage_groups = artifacts["candidate_authority_stage_capability_report.json"].get("canonical_observation_groups", [])
    targets = target_registry.get("targets", [])
    if not isinstance(targets, list) or not targets:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: target registry is empty")

    runtime_by_package_date = {
        (str(entry.get("package_id")), str(entry.get("decision_date"))): entry
        for entry in runtime_entries
        if isinstance(entry, dict)
    }
    capability_by_context_date = {
        (str(entry.get("audit_target_id")), str(entry.get("signal_context_hash")), str(entry.get("decision_date"))): entry
        for entry in capability_entries
        if isinstance(entry, dict) and entry.get("audit_target_id") and entry.get("signal_context_hash") and entry.get("decision_date")
    }
    classification_by_context_date = {
        (str(entry.get("audit_target_id")), str(entry.get("signal_context_hash")), str(entry.get("decision_date"))): entry
        for entry in classifications
        if isinstance(entry, dict) and entry.get("audit_target_id") and entry.get("signal_context_hash") and entry.get("decision_date")
    }
    intervals_by_context = _intervals_by_context(intervals)
    target_by_id = {str(target.get("audit_target_id")): target for target in targets if isinstance(target, dict)}

    target_scopes: dict[str, list[AdmissionScope]] = defaultdict(list)
    for group in lineage_groups:
        if not isinstance(group, dict):
            continue
        context_hash = str(group.get("signal_context_hash") or "")
        lineage = group.get("lineage")
        if not context_hash or not isinstance(lineage, list):
            raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: malformed canonical lineage")
        for lineage_entry in lineage:
            if not isinstance(lineage_entry, dict):
                raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: malformed lineage member")
            target_id = str(lineage_entry.get("audit_target_id") or "")
            decision_date = str(lineage_entry.get("decision_date") or "")
            target = target_by_id.get(target_id)
            capability = capability_by_context_date.get((target_id, context_hash, decision_date))
            classification = classification_by_context_date.get((target_id, context_hash, decision_date))
            if target is None or capability is None or classification is None:
                raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: lineage cannot be joined")
            interval = _matching_interval(intervals_by_context.get((target_id, context_hash), []), decision_date)
            if interval is None:
                raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: classification has no interval")
            scope = _build_admission_scope(
                target=target,
                capability=capability,
                classification=classification,
                interval=interval,
                runtime_by_package_date=runtime_by_package_date,
            )
            target_scopes[target_id].append(scope)

    target_handoffs: list[TargetHandoff] = []
    for target_id, target in sorted(target_by_id.items()):
        scopes = _deduplicate_and_validate_scopes(target_scopes.get(target_id, []))
        target_scope_hash = canonical_json_sha256(target)
        target_base = {
            "audit_target_id": target_id,
            "target_scope_hash": target_scope_hash,
            "admission_scopes": [scope.model_dump(mode="json") for scope in scopes],
        }
        target_handoffs.append(
            TargetHandoff(
                **target_base,
                target_handoff_hash=canonical_json_sha256(target_base),
            )
        )

    _attach_target_hashes(target_handoffs)
    created = created_at or datetime.now(timezone.utc)
    global_hashes = {
        "target_scope_registry_hash": canonical_json_sha256(target_registry),
        **{key: str(handoff_hashes[key]) for key in _HANDOFF_HASH_KEYS},
    }
    bundle_base = {
        "schema_version": HANDOFF_SCHEMA_VERSION,
        "audit_id": str(manifest["audit_id"]),
        "audit_manifest_hash": str(manifest["audit_manifest_hash"]),
        "request_hash": str(manifest["request_hash"]),
        "serializer_version": str(manifest["serializer_version"]),
        "global_handoff_hashes": global_hashes,
        "sorted_target_handoffs": [item.model_dump(mode="json") for item in target_handoffs],
        "initial_approval_receipt_hash": canonical_json_sha256(approval_receipt),
    }
    return HandoffBundle(
        **bundle_base,
        phase1_handoff_bundle_hash=canonical_json_sha256(bundle_base),
        created_at=created,
    )


def build_approval_decision(
    *,
    handoff: HandoffBundle,
    request: ApprovalDecisionRequest,
    existing_events: Iterable[ApprovalDecisionEvent],
    actor_principal: str,
    authority_backend_id: str,
    decision_at: datetime | None = None,
) -> ApprovalDecisionEvent:
    """Append one valid terminal-chain event without mutating existing evidence."""

    events = list(existing_events)
    validate_decision_chains(handoff=handoff, events=events)
    scope = _resolve_decision_scope(handoff=handoff, request=request)
    chain = [event for event in events if _decision_chain_key(event) == _request_chain_key(request)]
    terminal = _terminal_decision(chain)
    _validate_decision_transition(request=request, terminal=terminal)
    event_at = decision_at or datetime.now(timezone.utc)
    payload = {
        "schema_version": APPROVAL_DECISION_SCHEMA_VERSION,
        "decision_kind": request.decision_kind.value,
        "event_type": request.event_type.value,
        "audit_id": handoff.audit_id,
        "audit_manifest_hash": handoff.audit_manifest_hash,
        "request_hash": handoff.request_hash,
        "initial_approval_receipt_hash": handoff.initial_approval_receipt_hash,
        "phase1_handoff_bundle_hash": handoff.phase1_handoff_bundle_hash,
        "audit_target_id": scope.audit_target_id if scope else None,
        "target_handoff_hash": scope.target_handoff_hash if scope else None,
        "admission_scope_id": scope.admission_scope_id if scope else None,
        "admission_scope_hash": scope.admission_scope_hash if scope else None,
        "stable_signal_semantics_hash": scope.stable_signal_semantics_hash if scope else None,
        "phase0a_signal_context_hash": scope.phase0a_signal_context_hash if scope else None,
        "oos_interval_id": scope.oos_interval_id if scope else None,
        "capability": scope.capability if scope else None,
        "date_start": scope.date_start.isoformat() if scope else None,
        "date_end": scope.date_end.isoformat() if scope else None,
        "allowed_evidence_scope": (scope.allowed_evidence_scope if scope else EvidenceScope.NOT_APPLICABLE).value,
        "previous_terminal_decision_hash": request.previous_terminal_decision_hash,
        "revokes_decision_hash": request.revokes_decision_hash,
        "actor_principal": actor_principal,
        "authority_backend_id": authority_backend_id,
        "decision_at": event_at,
        "approval_reference": request.approval_reference,
    }
    provisional = ApprovalDecisionEvent(**payload, decision_id="pending", decision_hash="pending")
    decision_hash = canonical_json_sha256(_decision_hash_payload(provisional))
    return provisional.model_copy(
        update={
            "decision_id": f"advdec_{decision_hash[:24]}",
            "decision_hash": decision_hash,
        }
    )


def build_approval_bundle(
    *,
    handoff: HandoffBundle,
    events: Iterable[ApprovalDecisionEvent],
    created_by: str,
    authority_backend_id: str,
    created_at: datetime | None = None,
) -> ApprovalBundle:
    """Build an immutable bundle from the only terminal event in each chain."""

    event_list = list(events)
    validate_decision_chains(handoff=handoff, events=event_list)
    global_terminal = _terminal_decision([event for event in event_list if event.decision_kind == DecisionKind.GLOBAL])
    if global_terminal is None or global_terminal.event_type != DecisionEventType.APPROVE:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: global approval is required")

    scope_members: list[ApprovalBundleScope] = []
    for scope in _all_scopes(handoff):
        terminal = _terminal_decision(
            [event for event in event_list if event.admission_scope_id == scope.admission_scope_id]
        )
        if terminal is None or terminal.event_type != DecisionEventType.APPROVE:
            continue
        member_base = {
            "admission_scope_id": scope.admission_scope_id,
            "admission_scope_hash": scope.admission_scope_hash,
            "terminal_decision_hash": terminal.decision_hash,
            "allowed_evidence_scope": terminal.allowed_evidence_scope,
        }
        scope_members.append(
            ApprovalBundleScope(
                **member_base,
                scope_member_content_hash=canonical_json_sha256(member_base),
            )
        )
    scope_members.sort(key=lambda item: item.admission_scope_id)
    scope_set_hash = canonical_json_sha256([member.model_dump(mode="json") for member in scope_members])
    created = created_at or datetime.now(timezone.utc)
    base = {
        "schema_version": APPROVAL_BUNDLE_SCHEMA_VERSION,
        "audit_id": handoff.audit_id,
        "audit_manifest_hash": handoff.audit_manifest_hash,
        "request_hash": handoff.request_hash,
        "initial_approval_receipt_hash": handoff.initial_approval_receipt_hash,
        "phase1_handoff_bundle_hash": handoff.phase1_handoff_bundle_hash,
        "global_terminal_decision_hash": global_terminal.decision_hash,
        "admission_scope_set_hash": scope_set_hash,
        "scope_member_count": len(scope_members),
        "authority_backend_id": authority_backend_id,
        "authority_backend_hash": canonical_json_sha256({"authority_backend_id": authority_backend_id}),
        "scopes": [member.model_dump(mode="json") for member in scope_members],
        "created_by": created_by,
    }
    content_hash = canonical_json_sha256(base)
    return ApprovalBundle(
        **base,
        approval_bundle_id=f"advappr_{content_hash[:24]}",
        approval_bundle_content_hash=content_hash,
        created_at=created,
    )


def build_operation_authorization_event(
    *,
    request: OperationAuthorizationRequest,
    existing_events: Iterable[OperationAuthorizationEvent],
    actor_principal: str,
    event_at: datetime | None = None,
) -> OperationAuthorizationEvent:
    """Validate matrix/chain rules and return an append-only authorization event."""

    _validate_operation_matrix(request)
    existing = list(existing_events)
    validate_operation_authorization_chain(
        authorization_id=request.authorization_id,
        events=existing,
    )
    chain = [event for event in existing if event.authorization_id == request.authorization_id]
    terminal = _terminal_authorization(chain)
    if terminal is None:
        if request.event_type != AuthorizationEventType.AUTHORIZE or request.previous_event_hash or request.revokes_event_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: invalid initial event")
    elif terminal.event_type == AuthorizationEventType.AUTHORIZE:
        if (
            request.event_type != AuthorizationEventType.REVOKE
            or request.previous_event_hash is not None
            or request.revokes_event_hash != terminal.authorization_event_hash
        ):
            raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: active authorization must revoke")
    elif terminal.event_type == AuthorizationEventType.REVOKE:
        if (
            request.event_type != AuthorizationEventType.AUTHORIZE
            or request.revokes_event_hash is not None
            or request.previous_event_hash != terminal.authorization_event_hash
        ):
            raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: reauthorization predecessor mismatch")
    at = event_at or datetime.now(timezone.utc)
    payload = request.model_dump(mode="json")
    payload.update(
        {
            "schema_version": OPERATION_AUTHORIZATION_SCHEMA_VERSION,
            "actor_principal": actor_principal,
            "event_at": at,
        }
    )
    provisional = OperationAuthorizationEvent(**payload, authorization_event_hash="pending")
    return provisional.model_copy(
        update={"authorization_event_hash": canonical_json_sha256(_authorization_hash_payload(provisional))}
    )


def validate_decision_chains(*, handoff: HandoffBundle, events: Iterable[ApprovalDecisionEvent]) -> None:
    """Reject forked, cross-handoff, or tampered decision chains."""

    by_chain: dict[tuple[str, str | None], list[ApprovalDecisionEvent]] = defaultdict(list)
    for event in events:
        if event.audit_id != handoff.audit_id or event.phase1_handoff_bundle_hash != handoff.phase1_handoff_bundle_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: cross-handoff event")
        if canonical_json_sha256(_decision_hash_payload(event)) != event.decision_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: hash mismatch")
        by_chain[_decision_chain_key(event)].append(event)
    for key, chain in by_chain.items():
        hashes = {event.decision_hash for event in chain}
        if len(hashes) != len(chain):
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_FORKED: duplicate hash")
        successors: dict[str, ApprovalDecisionEvent] = {}
        for event in chain:
            predecessor = event.previous_terminal_decision_hash or event.revokes_decision_hash
            if predecessor:
                if predecessor not in hashes:
                    raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: missing predecessor")
                if predecessor in successors:
                    raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_FORKED: predecessor has multiple successors")
                successors[predecessor] = event
        roots = [event for event in chain if not event.previous_terminal_decision_hash and not event.revokes_decision_hash]
        if len(roots) != 1:
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_FORKED: root ambiguity")
        _validate_stored_decision_chain(root=roots[0], successors=successors)
        _ = _terminal_decision(chain)
        if key[0] == DecisionKind.GLOBAL.value and key[1] is not None:
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: global scope key")


def validate_operation_authorization_chain(
    *,
    authorization_id: str,
    events: Iterable[OperationAuthorizationEvent],
) -> None:
    """Reject forked, cross-authorization, or tampered authorization chains."""

    chain = list(events)
    if not chain:
        return
    if any(event.authorization_id != authorization_id for event in chain):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: cross-authorization event")
    hashes = {event.authorization_event_hash for event in chain}
    if len(hashes) != len(chain):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: duplicate authorization hash")
    successors: dict[str, OperationAuthorizationEvent] = {}
    for event in chain:
        if canonical_json_sha256(_authorization_hash_payload(event)) != event.authorization_event_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: authorization hash mismatch")
        predecessor = event.previous_event_hash or event.revokes_event_hash
        if predecessor:
            if predecessor not in hashes:
                raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: missing predecessor")
            if predecessor in successors:
                raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: authorization fork")
            successors[predecessor] = event
    roots = [event for event in chain if not event.previous_event_hash and not event.revokes_event_hash]
    if len(roots) != 1:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: authorization root ambiguity")
    _validate_stored_authorization_chain(root=roots[0], successors=successors)
    _ = _terminal_authorization(chain)


def validate_approval_bundle_active(
    *,
    bundle: ApprovalBundle,
    events: Iterable[ApprovalDecisionEvent],
) -> None:
    """Require a stored bundle to match the current terminal approval state."""

    event_list = list(events)
    by_chain: dict[tuple[str, str | None], list[ApprovalDecisionEvent]] = defaultdict(list)
    for event in event_list:
        if event.audit_id != bundle.audit_id or event.phase1_handoff_bundle_hash != bundle.phase1_handoff_bundle_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: cross-bundle decision")
        if canonical_json_sha256(_decision_hash_payload(event)) != event.decision_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: decision hash mismatch")
        by_chain[_decision_chain_key(event)].append(event)
    for chain in by_chain.values():
        hashes = {event.decision_hash for event in chain}
        if len(hashes) != len(chain):
            raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: duplicate decision hash")
        successors: dict[str, ApprovalDecisionEvent] = {}
        for event in chain:
            predecessor = event.previous_terminal_decision_hash or event.revokes_decision_hash
            if predecessor:
                if predecessor not in hashes or predecessor in successors:
                    raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: invalid decision chain")
                successors[predecessor] = event
        roots = [event for event in chain if not event.previous_terminal_decision_hash and not event.revokes_decision_hash]
        if len(roots) != 1:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: decision root ambiguity")
        _validate_stored_decision_chain(root=roots[0], successors=successors)
    global_terminal = _terminal_decision(by_chain.get((DecisionKind.GLOBAL.value, None), []))
    if global_terminal is None or global_terminal.event_type != DecisionEventType.APPROVE:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: global approval is inactive")
    if global_terminal.decision_hash != bundle.global_terminal_decision_hash:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: global approval has changed")
    for member in bundle.scopes:
        terminal = _terminal_decision(by_chain.get((DecisionKind.ADMISSION_SCOPE.value, member.admission_scope_id), []))
        if terminal is None or terminal.event_type != DecisionEventType.APPROVE:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: admission scope is inactive")
        if terminal.decision_hash != member.terminal_decision_hash or terminal.admission_scope_hash != member.admission_scope_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: admission scope has changed")


def _load_receipt_artifacts(receipt_dir: Path) -> dict[str, dict[str, Any]]:
    required = {
        "audit_manifest.json",
        "approval_receipt.json",
        "target_scope_registry.json",
        "runtime_semantics_ledger.json",
        "oos_interval_report.json",
        "candidate_authority_stage_capability_report.json",
    }
    loaded: dict[str, dict[str, Any]] = {}
    for name in required:
        path = receipt_dir / name
        if not path.is_file():
            raise Phase0AAuthorityError(f"ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: missing {name}")
        try:
            import json

            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise Phase0AAuthorityError(f"ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: unreadable {name}") from exc
        if not isinstance(value, dict):
            raise Phase0AAuthorityError(f"ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: malformed {name}")
        loaded[name] = value
    return loaded


def _validate_receipt_integrity(*, manifest: dict[str, Any], approval_receipt: dict[str, Any]) -> None:
    stored_hash = manifest.get("audit_manifest_hash")
    base = {key: value for key, value in manifest.items() if key != "audit_manifest_hash"}
    if not _sha256(stored_hash) or canonical_json_sha256(base) != stored_hash:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: audit manifest")
    if approval_receipt.get("schema_version") != "advisory_phase0a_approval_receipt_v1":
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: approval receipt schema")
    if approval_receipt.get("approval_status") != "NOT_APPROVED" or approval_receipt.get("automatic_approval") is not False:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING: initial receipt must be NOT_APPROVED")
    if approval_receipt.get("audit_id") != manifest.get("audit_id"):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: audit id")


def _build_admission_scope(
    *,
    target: dict[str, Any],
    capability: dict[str, Any],
    classification: dict[str, Any],
    interval: dict[str, Any],
    runtime_by_package_date: dict[tuple[str, str], dict[str, Any]],
) -> AdmissionScope:
    package_id = str(target.get("package_id") or "")
    manifest_sha256 = str(target.get("manifest_sha256") or "")
    decision_date = str(capability.get("decision_date") or "")
    runtime = runtime_by_package_date.get((package_id, decision_date))
    clock = capability.get("decision_clock") if isinstance(capability.get("decision_clock"), dict) else {}
    if not package_id or not _sha256(manifest_sha256) or runtime is None:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_STABLE_SIGNAL_SEMANTICS_MISMATCH: target/runtime")
    runtime_id = runtime.get("selection_runtime_semantics_id")
    effective_configs = runtime.get("effective_config_hashes")
    calendar_hash = clock.get("calendar_hash")
    if not isinstance(runtime_id, str) or not runtime_id or not isinstance(effective_configs, dict) or not effective_configs or not _sha256(calendar_hash):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_STABLE_SIGNAL_SEMANTICS_MISMATCH: incomplete runtime semantics")
    named_configs = {str(key): str(value) for key, value in effective_configs.items() if value}
    if len(named_configs) != len(effective_configs):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_STABLE_SIGNAL_SEMANTICS_MISMATCH: missing effective config")
    stable_payload = {
        "package_id": package_id,
        "manifest_sha256": manifest_sha256,
        "selection_runtime_semantics_hash": canonical_json_sha256({"selection_runtime_semantics_id": runtime_id}),
        "package_effective_config_hash": canonical_json_sha256(named_configs),
        "calendar_hash": calendar_hash,
    }
    stable_hash = canonical_json_sha256(stable_payload)
    status = str(classification.get("formal_oos_status") or "")
    evidence_scope = {
        "FORMAL_OOS": EvidenceScope.FORMAL_OOS,
        "RETROSPECTIVE_RESEARCH_ONLY": EvidenceScope.RETROSPECTIVE_RESEARCH_ONLY,
        "NONE": EvidenceScope.GAP_ONLY,
    }.get(status)
    if evidence_scope is None:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: unknown OOS status")
    identity = {
        "audit_target_id": target.get("audit_target_id"),
        "phase0a_signal_context_hash": capability.get("signal_context_hash"),
        "oos_interval_id": interval.get("interval_id"),
        "capability": classification.get("signal_capability"),
        "date_start": interval.get("start_date"),
        "date_end": interval.get("end_date"),
    }
    if any(value in (None, "") for value in identity.values()):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: incomplete admission scope")
    scope_hash = canonical_json_sha256(identity)
    return AdmissionScope(
        admission_scope_id=f"admscope_{scope_hash[:24]}",
        admission_scope_hash=scope_hash,
        audit_target_id=str(target["audit_target_id"]),
        target_handoff_hash="PENDING_TARGET_HASH",
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        phase0a_signal_context_hash=str(capability["signal_context_hash"]),
        oos_interval_id=str(interval["interval_id"]),
        oos_interval_hash=canonical_json_sha256(interval),
        capability=str(classification["signal_capability"]),
        capability_hash=canonical_json_sha256({"capability": classification["signal_capability"]}),
        date_start=date.fromisoformat(str(interval["start_date"])),
        date_end=date.fromisoformat(str(interval["end_date"])),
        allowed_evidence_scope=evidence_scope,
        stable_signal_semantics_payload_v1=stable_payload,
        stable_signal_semantics_hash=stable_hash,
    )


def _intervals_by_context(intervals: Any) -> dict[tuple[str, str], list[dict[str, Any]]]:
    result: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    if not isinstance(intervals, list):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: intervals")
    for interval in intervals:
        if isinstance(interval, dict) and interval.get("signal_context_hash") and interval.get("audit_target_id"):
            result[(str(interval["audit_target_id"]), str(interval["signal_context_hash"]))].append(interval)
    return result


def _matching_interval(intervals: list[dict[str, Any]], decision_date: str) -> dict[str, Any] | None:
    matches = [
        interval
        for interval in intervals
        if str(interval.get("start_date")) <= decision_date <= str(interval.get("end_date"))
    ]
    if len(matches) != 1:
        return None
    return matches[0]


def _deduplicate_and_validate_scopes(scopes: list[AdmissionScope]) -> list[AdmissionScope]:
    unique = {scope.admission_scope_hash: scope for scope in scopes}
    ordered = sorted(unique.values(), key=lambda item: item.admission_scope_id)
    by_capability: dict[tuple[str, str], list[AdmissionScope]] = defaultdict(list)
    for scope in ordered:
        by_capability[(scope.phase0a_signal_context_hash, scope.capability)].append(scope)
    for candidate_scopes in by_capability.values():
        candidate_scopes.sort(key=lambda item: (item.date_start, item.date_end, item.admission_scope_id))
        for previous, current in zip(candidate_scopes, candidate_scopes[1:]):
            if current.date_start <= previous.date_end and current.allowed_evidence_scope != previous.allowed_evidence_scope:
                raise Phase0AAuthorityError("ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH: conflicting overlapping intervals")
    return ordered


def _attach_target_hashes(target_handoffs: list[TargetHandoff]) -> None:
    for index, handoff in enumerate(target_handoffs):
        base = {
            "audit_target_id": handoff.audit_target_id,
            "target_scope_hash": handoff.target_scope_hash,
            "admission_scopes": [
                scope.model_dump(mode="json", exclude={"target_handoff_hash"})
                for scope in handoff.admission_scopes
            ],
        }
        target_hash = canonical_json_sha256(base)
        scopes = [scope.model_copy(update={"target_handoff_hash": target_hash}) for scope in handoff.admission_scopes]
        target_handoffs[index] = TargetHandoff(
            audit_target_id=handoff.audit_target_id,
            target_scope_hash=handoff.target_scope_hash,
            admission_scopes=scopes,
            target_handoff_hash=target_hash,
        )


def _resolve_decision_scope(*, handoff: HandoffBundle, request: ApprovalDecisionRequest) -> AdmissionScope | None:
    if request.decision_kind == DecisionKind.GLOBAL:
        if request.admission_scope_id is not None:
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_TARGET_APPROVAL_SCOPE_MISMATCH: global scope")
        return None
    if not request.admission_scope_id:
        raise Phase0AAuthorityError("ADVISORY_PHASE0A_TARGET_APPROVAL_SCOPE_MISMATCH: scope is required")
    for scope in _all_scopes(handoff):
        if scope.admission_scope_id == request.admission_scope_id:
            return scope
    raise Phase0AAuthorityError("ADVISORY_PHASE0A_TARGET_APPROVAL_SCOPE_MISMATCH: unknown scope")


def _validate_decision_transition(*, request: ApprovalDecisionRequest, terminal: ApprovalDecisionEvent | None) -> None:
    if terminal is None:
        if request.event_type not in {DecisionEventType.APPROVE, DecisionEventType.REJECT}:
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: initial event")
        if request.previous_terminal_decision_hash or request.revokes_decision_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: initial predecessor")
        return
    if terminal.event_type == DecisionEventType.APPROVE:
        if (
            request.event_type != DecisionEventType.REVOKE
            or request.previous_terminal_decision_hash is not None
            or request.revokes_decision_hash != terminal.decision_hash
        ):
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: revoke required")
        return
    if (
        request.event_type != DecisionEventType.APPROVE
        or request.revokes_decision_hash is not None
        or request.previous_terminal_decision_hash != terminal.decision_hash
    ):
        raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: reapproval predecessor")


def _validate_operation_matrix(request: OperationAuthorizationRequest) -> None:
    operation = request.operation_type
    business = operation in _BUSINESS_OPERATIONS
    governance = operation in _GOVERNANCE_OPERATIONS
    ddl = operation == "DDL"
    if not (business or governance or ddl):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: unknown operation")
    if business:
        if not request.approval_bundle_hash or not request.admission_scope_set_hash or request.governance_scope_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: business matrix")
    elif governance:
        if request.approval_bundle_hash or request.admission_scope_set_hash or not request.governance_scope_hash:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: governance matrix")
    elif ddl and (not request.approval_bundle_hash or request.admission_scope_set_hash or not request.governance_scope_hash):
        raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: ddl matrix")


def _all_scopes(handoff: HandoffBundle) -> list[AdmissionScope]:
    return [scope for target in handoff.sorted_target_handoffs for scope in target.admission_scopes]


def _decision_chain_key(event: ApprovalDecisionEvent) -> tuple[str, str | None]:
    return (event.decision_kind.value, event.admission_scope_id)


def _request_chain_key(request: ApprovalDecisionRequest) -> tuple[str, str | None]:
    return (request.decision_kind.value, request.admission_scope_id)


def _terminal_decision(events: Iterable[ApprovalDecisionEvent]) -> ApprovalDecisionEvent | None:
    event_list = list(events)
    if not event_list:
        return None
    predecessors = {
        predecessor
        for event in event_list
        for predecessor in (event.previous_terminal_decision_hash, event.revokes_decision_hash)
        if predecessor
    }
    terminals = [event for event in event_list if event.decision_hash not in predecessors]
    if len(terminals) != 1:
        raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_FORKED: terminal ambiguity")
    return terminals[0]


def _terminal_authorization(events: Iterable[OperationAuthorizationEvent]) -> OperationAuthorizationEvent | None:
    event_list = list(events)
    if not event_list:
        return None
    predecessors = {
        predecessor
        for event in event_list
        for predecessor in (event.previous_event_hash, event.revokes_event_hash)
        if predecessor
    }
    terminals = [event for event in event_list if event.authorization_event_hash not in predecessors]
    if len(terminals) != 1:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: terminal ambiguity")
    return terminals[0]


def _decision_hash_payload(event: ApprovalDecisionEvent) -> dict[str, Any]:
    return event.model_dump(mode="json", exclude={"decision_id", "decision_hash"})


def _authorization_hash_payload(event: OperationAuthorizationEvent) -> dict[str, Any]:
    return event.model_dump(mode="json", exclude={"authorization_event_hash"})


def _validate_stored_decision_chain(
    *,
    root: ApprovalDecisionEvent,
    successors: dict[str, ApprovalDecisionEvent],
) -> None:
    current = root
    if current.event_type not in {DecisionEventType.APPROVE, DecisionEventType.REJECT}:
        raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: invalid initial event")
    while (next_event := successors.get(current.decision_hash)) is not None:
        if current.event_type == DecisionEventType.APPROVE:
            valid = (
                next_event.event_type == DecisionEventType.REVOKE
                and next_event.previous_terminal_decision_hash is None
                and next_event.revokes_decision_hash == current.decision_hash
            )
        else:
            valid = (
                next_event.event_type == DecisionEventType.APPROVE
                and next_event.revokes_decision_hash is None
                and next_event.previous_terminal_decision_hash == current.decision_hash
            )
        if not valid:
            raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: invalid transition")
        current = next_event


def _validate_stored_authorization_chain(
    *,
    root: OperationAuthorizationEvent,
    successors: dict[str, OperationAuthorizationEvent],
) -> None:
    current = root
    if current.event_type != AuthorizationEventType.AUTHORIZE:
        raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: invalid initial event")
    while (next_event := successors.get(current.authorization_event_hash)) is not None:
        if current.event_type == AuthorizationEventType.AUTHORIZE:
            valid = (
                next_event.event_type == AuthorizationEventType.REVOKE
                and next_event.previous_event_hash is None
                and next_event.revokes_event_hash == current.authorization_event_hash
            )
        else:
            valid = (
                next_event.event_type == AuthorizationEventType.AUTHORIZE
                and next_event.revokes_event_hash is None
                and next_event.previous_event_hash == current.authorization_event_hash
            )
        if not valid:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: invalid authorization transition")
        current = next_event


def _sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value.lower())


__all__ = [
    "AdmissionScope",
    "ApprovalBundle",
    "ApprovalBundleScope",
    "ApprovalDecisionEvent",
    "ApprovalDecisionRequest",
    "AuthorizationEventType",
    "DecisionEventType",
    "DecisionKind",
    "EvidenceScope",
    "HandoffBundle",
    "OperationAuthorizationEvent",
    "OperationAuthorizationRequest",
    "Phase0AAuthorityError",
    "build_approval_bundle",
    "build_approval_decision",
    "build_handoff_bundle",
    "build_operation_authorization_event",
    "validate_decision_chains",
    "validate_approval_bundle_active",
    "validate_operation_authorization_chain",
]
