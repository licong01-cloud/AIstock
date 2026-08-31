from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from enum import Enum
from typing import Any, Mapping

from .canonical import digest_named_fields, ensure_sha256
from .cas_store import CASRef, CASStore
from .contracts import Component, ComponentAction, RunOutcome, Scope
from .errors import DatasetReleaseError


SIGNOFF_SCHEMA_VERSION = "dataset_release_signoff_v1"


class SignoffError(DatasetReleaseError):
    code = "DATASET_RELEASE_SIGNOFF_INVALID"


class ValidationStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class SafetyCounters:
    database_writes: int = 0
    production_writes: int = 0
    production_deletes: int = 0
    production_pointer_changes: int = 0
    service_process_controls: int = 0
    candidate_writes: int = 0

    def validate(self, *, outcome: RunOutcome) -> None:
        forbidden = {
            "database_writes": self.database_writes,
            "production_writes": self.production_writes,
            "production_deletes": self.production_deletes,
            "production_pointer_changes": self.production_pointer_changes,
            "service_process_controls": self.service_process_controls,
        }
        nonzero = {key: value for key, value in forbidden.items() if value != 0}
        if nonzero:
            raise SignoffError(
                "signoff contains forbidden DB/production/process mutations",
                context=nonzero,
            )
        if min(asdict(self).values()) < 0:
            raise SignoffError("safety counters cannot be negative")
        if outcome in {RunOutcome.NO_OP_VERIFIED, RunOutcome.REATTESTED} and self.candidate_writes:
            raise SignoffError(f"{outcome.value} cannot write candidate bytes")


@dataclass(frozen=True)
class ValidationResult:
    name: str
    status: ValidationStatus
    required: bool = True
    details_ref: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise SignoffError("validation result name must be non-empty")
        if self.details_ref is not None:
            ensure_sha256(self.details_ref, field=f"validation.{self.name}.details_ref")


@dataclass(frozen=True)
class ComponentSignoff:
    component: Component
    action: ComponentAction
    partition_key: str
    status: ValidationStatus
    manifest_root: str
    fingerprint_digest: str
    source_rows: int = 0
    artifact_rows: int = 0

    def __post_init__(self) -> None:
        ensure_sha256(self.manifest_root, field=f"{self.component.value}.manifest_root")
        ensure_sha256(
            self.fingerprint_digest,
            field=f"{self.component.value}.fingerprint_digest",
        )
        if self.source_rows < 0 or self.artifact_rows < 0:
            raise SignoffError("component row counts cannot be negative")

    def as_dict(self) -> dict[str, Any]:
        return {
            "component": self.component.value,
            "action": self.action.value,
            "partition_key": self.partition_key,
            "status": self.status.value,
            "manifest_root": self.manifest_root,
            "fingerprint_digest": self.fingerprint_digest,
            "source_rows": self.source_rows,
            "artifact_rows": self.artifact_rows,
        }


@dataclass(frozen=True)
class SignoffRequest:
    outcome: RunOutcome
    profile: str
    scope: str
    cutoff: str
    resolved_intent_key: str
    source_content_root: str
    source_provenance_root: str
    pit_snapshot_digest: str
    semantic_profile_digest: str
    action_plan_digest: str
    components: tuple[ComponentSignoff, ...]
    validations: tuple[ValidationResult, ...]
    safety: SafetyCounters
    candidate_identity: str | None = None
    release_digest: str | None = None
    attestation_key: str | None = None
    source_probe_key: str | None = None
    failure_code: str | None = None
    performance: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class SignoffReceipt:
    payload: Mapping[str, Any]
    digest: str

    def write_to_cas(self, cas: CASStore) -> CASRef:
        reference = cas.put_json({**self.payload, "signoff_digest": self.digest})
        return cas.verify(reference)


def build_signoff(request: SignoffRequest) -> SignoffReceipt:
    request.safety.validate(outcome=request.outcome)
    if not request.profile.strip():
        raise SignoffError("signoff profile must be non-empty")
    try:
        Scope(request.scope)
    except ValueError as exc:
        raise SignoffError("signoff scope is invalid") from exc
    try:
        date.fromisoformat(request.cutoff)
    except ValueError as exc:
        raise SignoffError("signoff cutoff must be an ISO date") from exc
    for name in (
        "resolved_intent_key",
        "source_content_root",
        "source_provenance_root",
        "pit_snapshot_digest",
        "semantic_profile_digest",
        "action_plan_digest",
    ):
        ensure_sha256(getattr(request, name), field=name)
    for name in ("candidate_identity", "release_digest", "attestation_key", "source_probe_key"):
        value = getattr(request, name)
        if value is not None:
            ensure_sha256(value, field=name)

    covered = {item.component for item in request.components}
    missing = [item.value for item in Component if item not in covered]
    if missing:
        raise SignoffError(f"signoff omits required components: {missing}")
    component_identities = [(item.component, item.partition_key) for item in request.components]
    if len(component_identities) != len(set(component_identities)):
        raise SignoffError("signoff contains duplicate component partitions")
    validation_names = [item.name for item in request.validations]
    if len(validation_names) != len(set(validation_names)):
        raise SignoffError("signoff contains duplicate validation identities")
    failed_components = [
        f"{item.component.value}:{item.partition_key}"
        for item in request.components
        if item.status is not ValidationStatus.PASS
    ]
    failed_validations = [
        item.name for item in request.validations if item.required and item.status is not ValidationStatus.PASS
    ]

    successful = request.outcome in {
        RunOutcome.NO_OP_VERIFIED,
        RunOutcome.REATTESTED,
        RunOutcome.CANDIDATE_VALIDATED,
    }
    if successful and not any(item.required for item in request.validations):
        raise SignoffError("successful signoff requires explicit required validation")
    if successful and (failed_components or failed_validations):
        raise SignoffError(
            "successful signoff contains failed required evidence",
            context={
                "components": failed_components,
                "validations": failed_validations,
            },
        )
    if successful and request.failure_code is not None:
        raise SignoffError("successful signoff cannot contain a failure code")
    if not successful and (request.failure_code is None or not request.failure_code.strip()):
        raise SignoffError("non-success signoff requires a typed failure code")

    actions = {item.action for item in request.components}
    if request.outcome is RunOutcome.NO_OP_VERIFIED:
        if actions != {ComponentAction.NOOP}:
            raise SignoffError("NO_OP_VERIFIED requires every component action to be NOOP")
        if request.candidate_identity is None or request.attestation_key is None or request.source_probe_key is None:
            raise SignoffError("NO_OP_VERIFIED requires candidate, fresh probe and attestation identities")
    elif request.outcome is RunOutcome.REATTESTED:
        if not actions.issubset({ComponentAction.NOOP, ComponentAction.REATTEST}):
            raise SignoffError("REATTESTED cannot contain materialization actions")
        if request.candidate_identity is None or request.attestation_key is None or request.source_probe_key is None:
            raise SignoffError("REATTESTED requires candidate, fresh probe and attestation identities")
    elif request.outcome is RunOutcome.CANDIDATE_VALIDATED:
        materialized = actions.intersection(
            {
                ComponentAction.RESUME,
                ComponentAction.REUSE,
                ComponentAction.INCREMENTAL,
                ComponentAction.SELECTIVE_REBUILD,
                ComponentAction.FULL_REBUILD,
            }
        )
        if not materialized or request.candidate_identity is None or request.release_digest is None:
            raise SignoffError("CANDIDATE_VALIDATED requires materialized actions and release/candidate identities")

    payload = {
        "schema_version": SIGNOFF_SCHEMA_VERSION,
        "outcome": request.outcome.value,
        "identity": {
            "profile": request.profile,
            "scope": request.scope,
            "cutoff": request.cutoff,
            "resolved_intent_key": request.resolved_intent_key,
            "candidate_identity": request.candidate_identity,
            "release_digest": request.release_digest,
            "attestation_key": request.attestation_key,
            "source_probe_key": request.source_probe_key,
        },
        "source": {
            "content_root": request.source_content_root,
            "provenance_root": request.source_provenance_root,
            "pit_snapshot_digest": request.pit_snapshot_digest,
            "semantic_profile_digest": request.semantic_profile_digest,
        },
        "action_plan_digest": request.action_plan_digest,
        "components": [
            item.as_dict()
            for item in sorted(
                request.components,
                key=lambda value: (value.component.value, value.partition_key),
            )
        ],
        "validation": [
            {
                "name": item.name,
                "status": item.status.value,
                "required": item.required,
                "details_ref": item.details_ref,
            }
            for item in request.validations
        ],
        "failure_code": request.failure_code,
        "performance": dict(request.performance or {}),
        "distribution": {"local_candidate": "required", "node1": "not_requested"},
        "activation": "not_requested",
        "hmm": {
            "producer": "validated" if successful else "invalid",
            "consumer": "not_activated",
            "benchmark": "000300.SH_unchanged",
        },
        "safety": asdict(request.safety),
    }
    digest = digest_named_fields(SIGNOFF_SCHEMA_VERSION, payload)
    return SignoffReceipt(payload=payload, digest=digest)
