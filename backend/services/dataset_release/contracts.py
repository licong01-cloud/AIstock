from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Mapping

from .canonical import (
    digest_named_fields,
    ensure_sha256,
    normalize_root_relative_path,
)
from .errors import IdentityConflictError


UNKNOWN_PRODUCER_PROVENANCE = "UNKNOWN_PRODUCER_PROVENANCE_V1"
UNKNOWN_PIT_SNAPSHOT = "UNKNOWN_PIT_SNAPSHOT_V1"
UNKNOWN_LEGACY_RECEIPT = "UNKNOWN_LEGACY_RECEIPT_V1"


class Scope(str, Enum):
    SAMPLE = "sample"
    FULL = "full"


class Component(str, Enum):
    DAILY_BIN = "daily_bin"
    MINUTE_BIN = "minute_bin"
    FACTOR_H5_STATIC = "factor_h5_static"
    DOMESTIC_INDEX_CONTEXT = "domestic_index_context"


REQUIRED_COMPONENTS: tuple[Component, ...] = tuple(Component)


class RunOutcome(str, Enum):
    NO_OP_VERIFIED = "NO_OP_VERIFIED"
    REATTESTED = "REATTESTED"
    CANDIDATE_VALIDATED = "CANDIDATE_VALIDATED"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ComponentAction(str, Enum):
    NOOP = "NOOP"
    REATTEST = "REATTEST"
    RESUME = "RESUME"
    REUSE = "REUSE"
    INCREMENTAL = "INCREMENTAL"
    SELECTIVE_REBUILD = "SELECTIVE_REBUILD"
    FULL_REBUILD = "FULL_REBUILD"


class OperationKind(str, Enum):
    BUILD = "BUILD"
    NO_OP = "NO_OP"
    REATTEST = "REATTEST"
    RESUME_BUILD = "RESUME_BUILD"
    FINALIZER_RECOVERY = "FINALIZER_RECOVERY"


class SourceProbeSubjectKind(str, Enum):
    CATALOG_CANDIDATE = "CATALOG_CANDIDATE"
    NEW_BUILD = "NEW_BUILD"


class ProducerProvenanceState(str, Enum):
    KNOWN = "KNOWN"
    RECONSTRUCTED_SOURCE_ONLY = "RECONSTRUCTED_SOURCE_ONLY"
    UNKNOWN = "UNKNOWN"


class PitProvenanceState(str, Enum):
    KNOWN = "KNOWN"
    UNKNOWN = "UNKNOWN"


class EquivalenceMode(str, Enum):
    CURRENT_SOURCE_EQUIVALENT = "CURRENT_SOURCE_EQUIVALENT"
    CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED = "CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED"
    ARTIFACT_VALID_SOURCE_CHANGED = "ARTIFACT_VALID_SOURCE_CHANGED"
    ARTIFACT_VALID_ONLY = "ARTIFACT_VALID_ONLY"
    INVALID = "INVALID"
    BLOCKED_LEGACY_PROVENANCE = "BLOCKED_LEGACY_PROVENANCE"


class ValidationCompatibility(str, Enum):
    UNCHANGED = "unchanged"
    VALIDATOR_STRENGTHENING_COMPATIBLE = "validator_strengthening_compatible"
    READER_OR_ARTIFACT_INCOMPATIBLE = "reader_or_artifact_incompatible"
    SEMANTIC_CONTRACT_CHANGED = "semantic_contract_changed"


def canonical_request_hash(payload: Mapping[str, Any]) -> str:
    return digest_named_fields("dataset_release_request_v1", payload)


@dataclass(frozen=True)
class SubmissionIdentity:
    principal: str
    route: str
    idempotency_key: str

    @property
    def key(self) -> str:
        if not self.principal.strip() or not self.route.strip() or not self.idempotency_key.strip():
            raise IdentityConflictError("submission identity fields must be non-empty")
        return digest_named_fields(
            "dataset_release_submission_key_v1",
            {
                "principal": self.principal,
                "route": self.route,
                "idempotency_key": self.idempotency_key,
            },
        )


@dataclass(frozen=True)
class LogicalRequestIdentity:
    profile: str
    resolved_cutoff: date
    scope: Scope
    semantic_profile_digest: str

    @property
    def key(self) -> str:
        return digest_named_fields(
            "dataset_release_logical_request_v1",
            {
                "profile": self.profile,
                "resolved_cutoff": self.resolved_cutoff,
                "scope": self.scope,
                "semantic_profile_digest": ensure_sha256(self.semantic_profile_digest, field="semantic_profile_digest"),
            },
        )


@dataclass(frozen=True)
class ResolvedIntentIdentity:
    logical_request_key: str
    source_content_root: str
    frozen_pit_spans_digest: str

    @property
    def key(self) -> str:
        return digest_named_fields(
            "dataset_release_resolved_intent_v1",
            {
                "logical_request_key": ensure_sha256(self.logical_request_key, field="logical_request_key"),
                "source_content_root": ensure_sha256(self.source_content_root, field="source_content_root"),
                "frozen_pit_spans_digest": ensure_sha256(self.frozen_pit_spans_digest, field="frozen_pit_spans_digest"),
            },
        )


def _provenance_digest(state: ProducerProvenanceState, value: str) -> str:
    if state is ProducerProvenanceState.KNOWN:
        return ensure_sha256(value, field="producer_provenance_digest")
    if value != UNKNOWN_PRODUCER_PROVENANCE:
        raise IdentityConflictError(f"{state.value} producer provenance must use {UNKNOWN_PRODUCER_PROVENANCE}")
    return value


def _pit_digest(state: PitProvenanceState, value: str) -> str:
    if state is PitProvenanceState.KNOWN:
        return ensure_sha256(value, field="pit_snapshot_digest")
    if value != UNKNOWN_PIT_SNAPSHOT:
        raise IdentityConflictError(f"unknown PIT provenance must use {UNKNOWN_PIT_SNAPSHOT}")
    return value


@dataclass(frozen=True)
class CandidateIdentity:
    registration_uuid: str
    allowlisted_root_id: str
    volume_serial: str
    root_relative_path: str
    profile: str
    scope: Scope
    cutoff: date
    lineage_anchor: str
    pit_provenance_state: PitProvenanceState
    pit_provenance_digest_or_sentinel: str
    artifact_root: str
    producer_provenance_state: ProducerProvenanceState
    producer_provenance_digest_or_sentinel: str

    @property
    def key(self) -> str:
        try:
            registration_uuid = str(uuid.UUID(self.registration_uuid))
        except ValueError as exc:
            raise IdentityConflictError("candidate registration_uuid is invalid") from exc
        if not self.allowlisted_root_id.strip() or not self.volume_serial.strip():
            raise IdentityConflictError("candidate root and volume identities must be non-empty")
        build_match = re.fullmatch(r"BUILD_RELEASE_DIGEST:([0-9a-f]{64})", self.lineage_anchor)
        legacy_match = re.fullmatch(
            r"LEGACY_RECEIPT:([^:]+):([0-9a-f]{64}|UNKNOWN_LEGACY_RECEIPT_V1)",
            self.lineage_anchor,
        )
        if build_match is None and legacy_match is None:
            raise IdentityConflictError("candidate lineage anchor has an unknown tag")
        return digest_named_fields(
            "candidate_identity_v1",
            {
                "registration_uuid": registration_uuid,
                "allowlisted_root_id": self.allowlisted_root_id,
                "volume_serial": self.volume_serial,
                "root_relative_path": normalize_root_relative_path(self.root_relative_path),
                "profile": self.profile,
                "scope": self.scope,
                "cutoff": self.cutoff,
                "lineage_anchor": self.lineage_anchor,
                "pit_provenance_state": self.pit_provenance_state,
                "pit_provenance_digest_or_sentinel": _pit_digest(
                    self.pit_provenance_state,
                    self.pit_provenance_digest_or_sentinel,
                ),
                "artifact_root": ensure_sha256(self.artifact_root, field="artifact_root"),
                "producer_provenance_state": self.producer_provenance_state,
                "producer_provenance_digest_or_sentinel": _provenance_digest(
                    self.producer_provenance_state,
                    self.producer_provenance_digest_or_sentinel,
                ),
            },
        )


@dataclass(frozen=True)
class SourceProbeIdentity:
    logical_request_key: str
    candidate_identity: str | None
    artifact_root: str | None
    source_content_root: str
    source_provenance_root: str
    pit_digest: str
    probe_policy_version: str
    probe_receipt_digest: str
    subject_kind: SourceProbeSubjectKind = SourceProbeSubjectKind.CATALOG_CANDIDATE
    subject_identity: str | None = None

    @property
    def key(self) -> str:
        logical_request_key = ensure_sha256(self.logical_request_key, field="logical_request_key")
        kind = SourceProbeSubjectKind(self.subject_kind)
        if kind is SourceProbeSubjectKind.CATALOG_CANDIDATE:
            if self.candidate_identity is None or self.artifact_root is None:
                raise IdentityConflictError("catalog-candidate source probe requires candidate/artifact identity")
            candidate_identity = ensure_sha256(self.candidate_identity, field="candidate_identity")
            artifact_root = ensure_sha256(self.artifact_root, field="artifact_root")
            expected_subject = catalog_candidate_probe_subject(candidate_identity, artifact_root)
        else:
            if self.candidate_identity is not None or self.artifact_root is not None:
                raise IdentityConflictError("new-build source probe forbids candidate/artifact identity")
            candidate_identity = None
            artifact_root = None
            expected_subject = new_build_probe_subject(logical_request_key)
        if self.subject_identity is not None:
            actual_subject = ensure_sha256(self.subject_identity, field="subject_identity")
            if actual_subject != expected_subject:
                raise IdentityConflictError("source probe subject identity does not match its canonical subject")
        digest_fields: dict[str, Any] = {
            name: ensure_sha256(value, field=name)
            for name, value in {
                "source_content_root": self.source_content_root,
                "source_provenance_root": self.source_provenance_root,
                "pit_digest": self.pit_digest,
                "probe_receipt_digest": self.probe_receipt_digest,
            }.items()
        }
        digest_fields.update(
            {
                "logical_request_key": logical_request_key,
                "subject_kind": kind.value,
                "subject_identity": expected_subject,
                "candidate_identity": candidate_identity,
                "artifact_root": artifact_root,
            }
        )
        digest_fields["probe_policy_version"] = self.probe_policy_version
        return digest_named_fields("dataset_release_source_probe_key_v2", digest_fields)


def catalog_candidate_probe_subject(
    candidate_identity: str,
    artifact_root: str,
) -> str:
    return digest_named_fields(
        "dataset_release_catalog_candidate_probe_subject_v1",
        {
            "candidate_identity": ensure_sha256(candidate_identity, field="candidate_identity"),
            "artifact_root": ensure_sha256(artifact_root, field="artifact_root"),
        },
    )


def new_build_probe_subject(logical_request_key: str) -> str:
    return digest_named_fields(
        "dataset_release_new_build_probe_subject_v1",
        {
            "logical_request_key": ensure_sha256(logical_request_key, field="logical_request_key"),
            "subject": "NO_BASELINE_CANDIDATE",
        },
    )


@dataclass(frozen=True)
class AttestationIdentity:
    candidate_identity: str
    producer_provenance_state: ProducerProvenanceState
    producer_provenance_digest_or_sentinel: str
    artifact_root: str
    current_source_content_root: str
    pit_digest: str
    semantic_profile_digest: str
    validation_fingerprint: str
    equivalence_mode: EquivalenceMode
    source_probe_key: str

    @property
    def target_key(self) -> str:
        """Stable validation target identity, independent of observation TTL."""

        return digest_named_fields(
            "dataset_release_attestation_key_v1",
            {
                "candidate_identity": ensure_sha256(self.candidate_identity, field="candidate_identity"),
                "producer_provenance_state": self.producer_provenance_state,
                "producer_provenance_digest_or_sentinel": _provenance_digest(
                    self.producer_provenance_state,
                    self.producer_provenance_digest_or_sentinel,
                ),
                "artifact_root": ensure_sha256(self.artifact_root, field="artifact_root"),
                "current_source_content_root": ensure_sha256(
                    self.current_source_content_root, field="current_source_content_root"
                ),
                "pit_digest": ensure_sha256(self.pit_digest, field="pit_digest"),
                "semantic_profile_digest": ensure_sha256(self.semantic_profile_digest, field="semantic_profile_digest"),
                "validation_fingerprint": ensure_sha256(self.validation_fingerprint, field="validation_fingerprint"),
                "equivalence_mode": self.equivalence_mode,
            },
        )

    @property
    def key(self) -> str:
        """Immutable fresh observation identity used by the durable catalog."""

        return attestation_observation_key(
            self.target_key,
            self.source_probe_key,
        )


@dataclass(frozen=True)
class RunGenerationIdentity:
    operation_kind: OperationKind
    decision_schema: str
    producer_fingerprint: str
    artifact_fingerprint: str
    validation_identity: str
    sample_policy: str
    operation_target: str

    @property
    def digest(self) -> str:
        return digest_named_fields(
            "dataset_release_run_generation_v1",
            {
                "operation_kind": self.operation_kind,
                "decision_schema": self.decision_schema,
                "producer_fingerprint": ensure_sha256(self.producer_fingerprint, field="producer_fingerprint"),
                "artifact_fingerprint": ensure_sha256(self.artifact_fingerprint, field="artifact_fingerprint"),
                "validation_identity": ensure_sha256(self.validation_identity, field="validation_identity"),
                "sample_policy": self.sample_policy,
                "operation_target": ensure_sha256(self.operation_target, field="operation_target"),
            },
        )


@dataclass(frozen=True)
class RunIdentity:
    resolved_intent_key: str
    run_generation_digest: str
    lineage_root_key: str
    lineage_ordinal: int

    @property
    def key(self) -> str:
        if self.lineage_ordinal < 0:
            raise IdentityConflictError("run lineage ordinal cannot be negative")
        return digest_named_fields(
            "dataset_release_run_id_v1",
            {
                "resolved_intent_key": ensure_sha256(self.resolved_intent_key, field="resolved_intent_key"),
                "run_generation_digest": ensure_sha256(self.run_generation_digest, field="run_generation_digest"),
                "lineage_root_key": ensure_sha256(self.lineage_root_key, field="lineage_root_key"),
                "lineage_ordinal": self.lineage_ordinal,
            },
        )


@dataclass(frozen=True)
class AttemptIdentity:
    run_id: str
    ordinal: int

    @property
    def key(self) -> str:
        if self.ordinal <= 0:
            raise IdentityConflictError("attempt ordinal must be positive")
        return digest_named_fields(
            "dataset_release_attempt_id_v1",
            {
                "run_id": ensure_sha256(self.run_id, field="run_id"),
                "ordinal": self.ordinal,
            },
        )


@dataclass(frozen=True)
class ReleaseIdentity:
    resolved_intent_key: str
    frozen_pit_spans_digest: str
    scope: Scope
    producer_fingerprint: str
    artifact_fingerprint: str
    cutoff: date
    profile: str

    @property
    def digest(self) -> str:
        return digest_named_fields(
            "dataset_release_release_digest_v1",
            {
                "resolved_intent_key": ensure_sha256(self.resolved_intent_key, field="resolved_intent_key"),
                "frozen_pit_spans_digest": ensure_sha256(self.frozen_pit_spans_digest, field="frozen_pit_spans_digest"),
                "scope": self.scope,
                "producer_fingerprint": ensure_sha256(self.producer_fingerprint, field="producer_fingerprint"),
                "artifact_fingerprint": ensure_sha256(self.artifact_fingerprint, field="artifact_fingerprint"),
            },
        )

    @property
    def release_id(self) -> str:
        safe_profile = re.sub(r"[^a-zA-Z0-9_-]+", "-", self.profile).strip("-")
        return f"{self.cutoff:%Y%m%d}-{safe_profile}-{self.scope.value}-{self.digest[:16]}-candidate"


def build_operation_target(resolved_intent_key: str, action_plan_digest: str) -> str:
    return digest_named_fields(
        "dataset_release_build_target_v1",
        {
            "resolved_intent_key": ensure_sha256(resolved_intent_key, field="resolved_intent_key"),
            "action_plan_digest": ensure_sha256(action_plan_digest, field="action_plan_digest"),
        },
    )


def reattest_operation_target(
    candidate_identity: str,
    artifact_root: str,
    attestation_target_key: str,
) -> str:
    return digest_named_fields(
        "dataset_release_reattest_target_v1",
        {
            "candidate_identity": ensure_sha256(candidate_identity, field="candidate_identity"),
            "artifact_root": ensure_sha256(artifact_root, field="artifact_root"),
            "attestation_target_key": ensure_sha256(attestation_target_key, field="attestation_target_key"),
        },
    )


def attestation_observation_key(
    attestation_target_key: str,
    source_probe_key: str,
) -> str:
    return digest_named_fields(
        "dataset_release_attestation_observation_v1",
        {
            "attestation_target_key": ensure_sha256(
                attestation_target_key,
                field="attestation_target_key",
            ),
            "source_probe_key": ensure_sha256(
                source_probe_key,
                field="source_probe_key",
            ),
        },
    )


def noop_operation_target(
    candidate_identity: str,
    artifact_root: str,
    source_probe_key: str,
    validation_identity: str,
) -> str:
    return digest_named_fields(
        "dataset_release_noop_target_v1",
        {
            "candidate_identity": ensure_sha256(candidate_identity, field="candidate_identity"),
            "artifact_root": ensure_sha256(artifact_root, field="artifact_root"),
            "source_probe_key": ensure_sha256(source_probe_key, field="source_probe_key"),
            "validation_identity": ensure_sha256(validation_identity, field="validation_identity"),
        },
    )


def resume_operation_target(
    resumes_run_id: str,
    original_run_generation: str,
    validated_checkpoint_root: str,
    resume_ordinal: int,
) -> str:
    if resume_ordinal <= 0:
        raise IdentityConflictError("resume ordinal must be positive")
    return digest_named_fields(
        "dataset_release_resume_build_target_v1",
        {
            "resumes_run_id": ensure_sha256(resumes_run_id, field="resumes_run_id"),
            "original_run_generation": ensure_sha256(original_run_generation, field="original_run_generation"),
            "validated_checkpoint_root": ensure_sha256(validated_checkpoint_root, field="validated_checkpoint_root"),
            "resume_ordinal": resume_ordinal,
        },
    )
