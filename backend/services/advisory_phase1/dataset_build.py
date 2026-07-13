"""Frozen Batch C dataset-build identities and in-memory state-machine oracle.

This module has no filesystem writer, PostgreSQL access, scheduler, runtime
registration, or model-training loop.  Batch D owns Parquet/CAS materialization;
Batch C owns the immutable build/attempt contract it must satisfy.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from enum import Enum
import hashlib
import os
from pathlib import Path
from typing import Callable, Iterable, Protocol
from urllib.parse import unquote, urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize


DATASET_BUILD_REQUEST_SCHEMA_VERSION = "advisory_phase1c3_fixture_dataset_build_request_v1"
BATCH_C_FILESET_VERIFICATION_CONTRACT = "PHASE1C3_BATCH_C_FILESET_FOUNDATION_V1"
BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT = "PHASE1C3_BATCH_D_FULL_PARQUET_V1"

REASON_BUILD_REQUEST_CONFLICT = "ADVISORY_PHASE1C3_BUILD_REQUEST_CONFLICT"
REASON_BUILD_ALREADY_RUNNING = "ADVISORY_PHASE1C3_BUILD_ALREADY_RUNNING"
REASON_BUILD_GENERATION_INVALID = "ADVISORY_PHASE1C3_BUILD_GENERATION_INVALID"
REASON_BUILD_TRANSITION_INVALID = "ADVISORY_PHASE1C3_BUILD_TRANSITION_INVALID"
REASON_ATTEMPT_OPERATION_INVALID = "ADVISORY_PHASE1C3_ATTEMPT_OPERATION_INVALID"
REASON_ATTEMPT_LEASE_EXPIRED = "ADVISORY_PHASE1C3_ATTEMPT_LEASE_EXPIRED"
REASON_ATTEMPT_FENCING_STALE = "ADVISORY_PHASE1C3_ATTEMPT_FENCING_STALE"
REASON_ATTEMPT_FILE_CONFLICT = "ADVISORY_PHASE1C3_ATTEMPT_FILE_CONFLICT"
REASON_CHECKPOINT_CONFLICT = "ADVISORY_PHASE1C3_CHECKPOINT_CONFLICT"
REASON_BASE_SNAPSHOT_INVALID = "ADVISORY_PHASE1C3_BASE_SNAPSHOT_INVALID"


class DatasetBuildError(RuntimeError):
    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


def _sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include timezone")
    return value.astimezone(timezone.utc)


class BuildLifecycle(str, Enum):
    ACTIVE = "ACTIVE"
    SEALED = "SEALED"
    FAILED_TERMINAL = "FAILED_TERMINAL"
    ABORTED = "ABORTED"


class BuildCheckpoint(str, Enum):
    REQUESTED = "REQUESTED"
    MATERIALIZED = "MATERIALIZED"
    VERIFIED = "VERIFIED"
    PROMOTED = "PROMOTED"
    SEALED = "SEALED"


class AttemptOperation(str, Enum):
    MATERIALIZE = "MATERIALIZE"
    VERIFY = "VERIFY"
    PROMOTE = "PROMOTE"
    SEAL = "SEAL"
    RECOVER = "RECOVER"


class AttemptState(str, Enum):
    ACTIVE = "ACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    ABORTED = "ABORTED"


class DatasetBuildEventType(str, Enum):
    REQUESTED = "REQUESTED"
    ATTEMPT_STARTED = "ATTEMPT_STARTED"
    MATERIALIZED = "MATERIALIZED"
    VERIFIED = "VERIFIED"
    PROMOTED = "PROMOTED"
    SEALED = "SEALED"
    ATTEMPT_FAILED = "ATTEMPT_FAILED"
    ATTEMPT_EXPIRED = "ATTEMPT_EXPIRED"
    RECOVERY_STARTED = "RECOVERY_STARTED"
    BUILD_TERMINATED = "BUILD_TERMINATED"


class CaptureSetMember(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    capture_batch_id: str = Field(min_length=1, max_length=160)
    capture_request_hash: str = Field(min_length=64, max_length=64)
    capture_receipt_hash: str = Field(min_length=64, max_length=64)
    membership_hash: str = Field(min_length=64, max_length=64)
    capture_purpose: str = Field(pattern="^(OBSERVATION_CAPTURE_V1|LABEL_CAPTURE_V1)$")
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    source_revision_set_id: str = Field(min_length=1, max_length=160)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    date_start: date
    date_end: date

    @field_validator(
        "capture_request_hash",
        "capture_receipt_hash",
        "membership_hash",
        "handoff_readiness_hash",
        "admission_scope_hash",
        "source_revision_set_hash",
    )
    @classmethod
    def _hash(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)

    def canonical_identity(self) -> dict[str, str]:
        return self.model_dump(mode="json")

    @model_validator(mode="after")
    def _date_range(self) -> "CaptureSetMember":
        if self.date_end < self.date_start:
            raise ValueError("capture member date range is invalid")
        return self


class FrozenIdentity(BaseModel):
    """One immutable id/hash pair included in a frozen build request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    identity_id: str = Field(min_length=1, max_length=160)
    identity_hash: str = Field(min_length=64, max_length=64)

    @field_validator("identity_hash")
    @classmethod
    def _identity_hash(cls, value: str) -> str:
        return _sha256(value, field_name="identity_hash")


class LabelTargetIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    horizon_trading_days: int = Field(ge=0)
    projection: str = Field(min_length=1, max_length=160)
    projection_schema_version: str = Field(min_length=1, max_length=160)


class CompositeCapabilityRequirement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    component: str = Field(min_length=1, max_length=160)
    capability: str = Field(min_length=1, max_length=160)
    required: bool = True


class BaseSnapshotIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshot_id: str = Field(min_length=1, max_length=160)
    snapshot_content_hash: str = Field(min_length=64, max_length=64)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    snapshot_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    capture_set_hash: str = Field(min_length=64, max_length=64)
    policy_compatibility_hash: str = Field(min_length=64, max_length=64)

    @field_validator(
        "snapshot_content_hash",
        "manifest_sha256",
        "snapshot_source_revision_set_hash",
        "capture_set_hash",
        "policy_compatibility_hash",
    )
    @classmethod
    def _hash(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)


class FixtureDatasetBuildRequest(BaseModel):
    """One fully frozen build request.  It never carries worker or lease data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = DATASET_BUILD_REQUEST_SCHEMA_VERSION
    phase0a_audit_id: str = Field(min_length=1, max_length=160)
    phase0a_audit_hash: str = Field(min_length=64, max_length=64)
    phase1_handoff_bundle_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    admission_scopes: tuple[FrozenIdentity, ...] = Field(min_length=1)
    admission_scope_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    captures: tuple[CaptureSetMember, ...] = Field(min_length=1)
    capture_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    date_start: date
    date_end: date
    selected_observation_mappings: tuple[FrozenIdentity, ...] = Field(min_length=1)
    selected_observation_mapping_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    selected_label_mappings: tuple[FrozenIdentity, ...] = Field(min_length=1)
    selected_label_mapping_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    label_policy_bundle_id: str = Field(min_length=1, max_length=160)
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_targets: tuple[LabelTargetIdentity, ...] = Field(min_length=1)
    universe_policy_hash: str = Field(min_length=64, max_length=64)
    benchmark_policy_hash: str = Field(min_length=64, max_length=64)
    cost_policy_hash: str = Field(min_length=64, max_length=64)
    calendar_hash: str = Field(min_length=64, max_length=64)
    symbol_normalization_policy_hash: str = Field(min_length=64, max_length=64)
    query_registry_version: str = Field(min_length=1, max_length=160)
    query_registry_hash: str = Field(min_length=64, max_length=64)
    snapshot_source_revision_set_id: str = Field(min_length=1, max_length=160)
    snapshot_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    required_composite_capabilities: tuple[CompositeCapabilityRequirement, ...] = Field(min_length=1)
    composite_capability_hash: str | None = Field(default=None, min_length=64, max_length=64)
    builder_version: str = Field(min_length=1, max_length=160)
    code_commit: str = Field(min_length=1, max_length=160)
    writer_version: str = Field(min_length=1, max_length=160)
    snapshot_schema_version: str = Field(min_length=1, max_length=160)
    schema_fingerprint: str = Field(min_length=64, max_length=64)
    partition_policy_id: str = Field(min_length=1, max_length=160)
    partition_policy_hash: str = Field(min_length=64, max_length=64)
    policy_compatibility_hash: str = Field(min_length=64, max_length=64)
    compression_config: dict[str, object]
    compression_config_hash: str | None = Field(default=None, min_length=64, max_length=64)
    requested_source_cutoff: date
    label_as_of_ts: datetime
    base_snapshot: BaseSnapshotIdentity | None = None
    build_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "phase0a_audit_hash", "phase1_handoff_bundle_hash", "handoff_readiness_hash",
        "admission_scope_set_hash", "capture_set_hash", "selected_observation_mapping_set_hash",
        "selected_label_mapping_set_hash", "label_policy_bundle_hash", "universe_policy_hash",
        "benchmark_policy_hash", "cost_policy_hash", "calendar_hash", "symbol_normalization_policy_hash",
        "query_registry_hash", "snapshot_source_revision_set_hash", "composite_capability_hash",
        "schema_fingerprint", "partition_policy_hash", "compression_config_hash", "build_request_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("label_as_of_ts")
    @classmethod
    def _timestamp(cls, value: datetime) -> datetime:
        return _aware(value, field_name="label_as_of_ts")

    def canonical_payload(self) -> dict[str, object]:
        return canonicalize(self.model_dump(mode="python", exclude={"capture_set_hash", "build_request_hash"}))

    @model_validator(mode="after")
    def _frozen(self) -> "FixtureDatasetBuildRequest":
        if self.schema_version != DATASET_BUILD_REQUEST_SCHEMA_VERSION:
            raise ValueError("unsupported dataset build request schema")
        if self.date_end < self.date_start:
            raise ValueError("dataset build date range is invalid")
        for field_name, values in (
            ("admission_scopes", self.admission_scopes),
            ("selected_observation_mappings", self.selected_observation_mappings),
            ("selected_label_mappings", self.selected_label_mappings),
        ):
            ordered_values = tuple(sorted(values, key=lambda item: item.identity_id))
            if ordered_values != values or len({item.identity_id for item in values}) != len(values):
                raise ValueError(f"{field_name} must be sorted by unique identity id")
        targets = tuple(sorted(self.label_targets, key=lambda item: (item.horizon_trading_days, item.projection)))
        if targets != self.label_targets or len({(item.horizon_trading_days, item.projection) for item in targets}) != len(targets):
            raise ValueError("label targets must be sorted and unique")
        capabilities = tuple(sorted(self.required_composite_capabilities, key=lambda item: item.component))
        if capabilities != self.required_composite_capabilities or len({item.component for item in capabilities}) != len(capabilities):
            raise ValueError("composite capability requirements must be sorted by unique component")
        ordered = tuple(sorted(self.captures, key=lambda item: item.capture_batch_id))
        if ordered != self.captures or len({item.capture_batch_id for item in ordered}) != len(ordered):
            raise ValueError("capture set must be sorted by unique capture batch id")
        if any(item.date_start < self.date_start or item.date_end > self.date_end for item in ordered):
            raise ValueError("capture member date range is outside the frozen build range")
        if {item.capture_purpose for item in ordered} != {"OBSERVATION_CAPTURE_V1", "LABEL_CAPTURE_V1"}:
            raise ValueError("build request requires both COMPLETE observation and label capture descriptors")
        if any(item.handoff_readiness_hash != self.handoff_readiness_hash for item in ordered):
            raise ValueError("capture member handoff readiness does not match build request")
        if {(item.admission_scope_id, item.admission_scope_hash) for item in ordered} != {
            (item.identity_id, item.identity_hash) for item in self.admission_scopes
        }:
            raise ValueError("capture admission scopes do not match frozen admission scope set")
        scope_hash = canonical_json_sha256([item.model_dump(mode="json") for item in self.admission_scopes])
        observation_hash = canonical_json_sha256([item.model_dump(mode="json") for item in self.selected_observation_mappings])
        label_hash = canonical_json_sha256([item.model_dump(mode="json") for item in self.selected_label_mappings])
        capability_hash = canonical_json_sha256([item.model_dump(mode="json") for item in capabilities])
        compression_hash = canonical_json_sha256(self.compression_config)
        for field_name, persisted, expected in (
            ("admission_scope_set_hash", self.admission_scope_set_hash, scope_hash),
            ("selected_observation_mapping_set_hash", self.selected_observation_mapping_set_hash, observation_hash),
            ("selected_label_mapping_set_hash", self.selected_label_mapping_set_hash, label_hash),
            ("composite_capability_hash", self.composite_capability_hash, capability_hash),
            ("compression_config_hash", self.compression_config_hash, compression_hash),
        ):
            if persisted is not None and persisted != expected:
                raise ValueError(f"{field_name} does not match frozen request content")
            object.__setattr__(self, field_name, expected)
        capture_hash = canonical_json_sha256([item.canonical_identity() for item in ordered])
        if self.capture_set_hash is not None and self.capture_set_hash != capture_hash:
            raise ValueError("capture_set_hash does not match sorted capture set")
        object.__setattr__(self, "capture_set_hash", capture_hash)
        request_hash = canonical_json_sha256(self.canonical_payload())
        if self.build_request_hash is not None and self.build_request_hash != request_hash:
            raise ValueError("build_request_hash does not match canonical request")
        object.__setattr__(self, "build_request_hash", request_hash)
        return self


class DatasetAttemptFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    attempt_id: str = Field(min_length=1, max_length=160)
    fencing_token: int = Field(ge=1)
    logical_path: str = Field(min_length=1, max_length=1024)
    logical_role: str = Field(min_length=1, max_length=160)
    partition_key_hash: str = Field(min_length=64, max_length=64)
    ordinal: int = Field(ge=0)
    staging_uri: str = Field(min_length=1, max_length=4096)
    sha256: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(gt=0)
    row_count: int = Field(ge=0)
    schema_fingerprint: str = Field(min_length=64, max_length=64)
    partition_content_hash: str = Field(min_length=64, max_length=64)
    compression: str = Field(min_length=1, max_length=80)
    writer_version: str = Field(min_length=1, max_length=160)

    @field_validator("partition_key_hash", "sha256", "schema_fingerprint", "partition_content_hash")
    @classmethod
    def _hash(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)

    @property
    def ordinal_key(self) -> tuple[str, str, int]:
        return self.logical_role, self.partition_key_hash, self.ordinal

    def canonical_identity(self) -> dict[str, object]:
        return self.model_dump(mode="json")


class DatasetBuildAttempt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    attempt_id: str = Field(min_length=1, max_length=160)
    build_id: str = Field(min_length=1, max_length=160)
    attempt_no: int = Field(ge=1)
    operation: AttemptOperation
    state: AttemptState
    lease_owner_id: str = Field(min_length=1, max_length=160)
    lease_token: str = Field(min_length=1, max_length=160)
    fencing_token: int = Field(ge=1)
    expected_build_row_version: int = Field(ge=1)
    expected_checkpoint: BuildCheckpoint
    acquired_at: datetime
    expires_at: datetime
    operation_request_hash: str = Field(min_length=64, max_length=64)
    predecessor_attempt_id: str | None = Field(default=None, min_length=1, max_length=160)
    heartbeat_at: datetime
    finished_at: datetime | None = None
    error_code: str | None = None
    error_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("acquired_at", "heartbeat_at", "expires_at", "finished_at")
    @classmethod
    def _timestamps(cls, value: datetime | None, info) -> datetime | None:  # type: ignore[no-untyped-def]
        return _aware(value, field_name=info.field_name) if value is not None else None

    @field_validator("operation_request_hash", "error_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _state(self) -> "DatasetBuildAttempt":
        if self.expires_at <= self.acquired_at:
            raise ValueError("attempt lease must expire after acquisition")
        if self.heartbeat_at < self.acquired_at or self.heartbeat_at > self.expires_at:
            raise ValueError("attempt heartbeat is outside the lease interval")
        if self.operation is AttemptOperation.RECOVER and self.predecessor_attempt_id is None:
            raise ValueError("recover attempt requires its exact expired predecessor")
        if self.operation is not AttemptOperation.RECOVER and self.predecessor_attempt_id is not None:
            raise ValueError("only recover attempt may name a predecessor")
        if self.state is AttemptState.ACTIVE and self.finished_at is not None:
            raise ValueError("active attempt cannot have finished_at")
        if self.state is not AttemptState.ACTIVE and self.finished_at is None:
            raise ValueError("terminal attempt requires finished_at")
        return self


class DatasetBuild(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    build_id: str = Field(min_length=1, max_length=160)
    request: FixtureDatasetBuildRequest
    logical_build_key_sha256: str = Field(min_length=64, max_length=64)
    build_generation: int = Field(ge=1)
    predecessor_build_id: str | None = Field(default=None, min_length=1, max_length=160)
    lifecycle: BuildLifecycle = BuildLifecycle.ACTIVE
    checkpoint: BuildCheckpoint = BuildCheckpoint.REQUESTED
    current_fencing_token: int = Field(default=1, ge=1)
    current_attempt_id: str | None = None
    row_version: int = Field(default=1, ge=1)
    materialized_attempt_id: str | None = None
    materialize_receipt_hash: str | None = None
    materialized_file_set_hash: str | None = None
    verified_attempt_id: str | None = None
    verify_receipt_hash: str | None = None
    verified_file_set_hash: str | None = None
    verification_contract_version: str | None = None
    promoted_attempt_id: str | None = None
    promotion_receipt_hash: str | None = None
    promoted_manifest_hash: str | None = None
    sealed_attempt_id: str | None = None
    seal_receipt_hash: str | None = None
    sealed_snapshot_id: str | None = None
    termination_receipt_hash: str | None = None
    terminal_reason_code: str | None = None
    created_at: datetime
    updated_at: datetime

    @field_validator(
        "logical_build_key_sha256", "materialize_receipt_hash", "materialized_file_set_hash",
        "verify_receipt_hash", "verified_file_set_hash", "termination_receipt_hash",
        "seal_receipt_hash", "promotion_receipt_hash", "promoted_manifest_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("created_at", "updated_at")
    @classmethod
    def _times(cls, value: datetime) -> datetime:
        return _aware(value, field_name="build timestamp")

    @model_validator(mode="after")
    def _closure(self) -> "DatasetBuild":
        expected_key = logical_build_key(self.request)
        if self.logical_build_key_sha256 != expected_key:
            raise ValueError("build logical key does not match request")
        expected_id = build_id_for(expected_key, self.build_generation)
        if self.build_id != expected_id:
            raise ValueError("build id does not match logical key and generation")
        if (self.build_generation == 1) != (self.predecessor_build_id is None):
            raise ValueError("build predecessor must be present exactly for rebuilt generations")
        if self.checkpoint in {BuildCheckpoint.MATERIALIZED, BuildCheckpoint.VERIFIED, BuildCheckpoint.PROMOTED, BuildCheckpoint.SEALED}:
            if not all((self.materialized_attempt_id, self.materialize_receipt_hash, self.materialized_file_set_hash)):
                raise ValueError("materialized checkpoint fields are incomplete")
        if self.checkpoint in {BuildCheckpoint.VERIFIED, BuildCheckpoint.PROMOTED, BuildCheckpoint.SEALED}:
            if not all((self.verified_attempt_id, self.verify_receipt_hash, self.verified_file_set_hash, self.verification_contract_version)):
                raise ValueError("verified checkpoint fields are incomplete")
        if self.checkpoint in {BuildCheckpoint.PROMOTED, BuildCheckpoint.SEALED}:
            if not all((self.promoted_attempt_id, self.promotion_receipt_hash, self.promoted_manifest_hash)):
                raise ValueError("promoted checkpoint fields are incomplete")
        elif any((self.promoted_attempt_id, self.promotion_receipt_hash, self.promoted_manifest_hash)):
            raise ValueError("pre-promoted checkpoint cannot carry promotion evidence")
        if self.checkpoint is BuildCheckpoint.SEALED and not all((self.sealed_attempt_id, self.seal_receipt_hash, self.sealed_snapshot_id)):
            raise ValueError("sealed checkpoint fields are incomplete")
        if self.checkpoint is not BuildCheckpoint.SEALED and any((self.sealed_attempt_id, self.seal_receipt_hash, self.sealed_snapshot_id)):
            raise ValueError("pre-sealed checkpoint cannot carry seal evidence")
        if (self.lifecycle is BuildLifecycle.SEALED) != (self.checkpoint is BuildCheckpoint.SEALED):
            raise ValueError("sealed lifecycle and checkpoint must advance together")
        if self.lifecycle is not BuildLifecycle.ACTIVE and self.current_attempt_id is not None:
            raise ValueError("terminal build cannot retain a current attempt")
        return self


class DatasetBuildEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    build_id: str = Field(min_length=1, max_length=160)
    attempt_id: str | None = None
    fencing_token: int | None = Field(default=None, ge=1)
    event_type: DatasetBuildEventType
    event_at: datetime
    actor: str = Field(min_length=1, max_length=160)
    payload_hash: str = Field(min_length=64, max_length=64)
    reason_codes: tuple[str, ...] = ()
    event_id: str | None = None

    @field_validator("event_at")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _aware(value, field_name="event_at")

    @field_validator("payload_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, field_name="payload_hash")

    @model_validator(mode="after")
    def _identity(self) -> "DatasetBuildEvent":
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"event_id"}))
        expected = f"advbuildevt_{digest[:24]}"
        if self.event_id is not None and self.event_id != expected:
            raise ValueError("event id does not match immutable event content")
        object.__setattr__(self, "event_id", expected)
        return self


class DatasetBlobHeader(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    store_backend_hash: str = Field(min_length=64, max_length=64)
    blob_sha256: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(gt=0)

    @field_validator("store_backend_hash", "blob_sha256")
    @classmethod
    def _hash(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)


class DatasetSnapshotFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    logical_path: str = Field(min_length=1, max_length=1024)
    logical_role: str = Field(min_length=1, max_length=160)
    partition_key_hash: str = Field(min_length=64, max_length=64)
    ordinal: int = Field(ge=0)
    content_uri: str = Field(min_length=1, max_length=4096)
    sha256: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(gt=0)
    row_count: int = Field(ge=0)
    schema_fingerprint: str = Field(min_length=64, max_length=64)
    partition_content_hash: str = Field(min_length=64, max_length=64)
    blob: DatasetBlobHeader

    @field_validator("partition_key_hash", "sha256", "schema_fingerprint", "partition_content_hash")
    @classmethod
    def _hash(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _blob_matches_file(self) -> "DatasetSnapshotFile":
        if self.blob.blob_sha256 != self.sha256 or self.blob.size_bytes != self.size_bytes:
            raise ValueError("snapshot file blob identity must equal file sha and size")
        return self

    @property
    def ordinal_key(self) -> tuple[str, str, int]:
        return self.logical_role, self.partition_key_hash, self.ordinal


class DatasetSnapshotObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_signal_id: str = Field(min_length=1, max_length=160)
    observation_version_id: str = Field(min_length=1, max_length=160)
    evidence_scope: str = "RETROSPECTIVE_RESEARCH_ONLY"
    oos_interval_id: str = Field(min_length=1, max_length=160)
    selector_policy_hash: str = Field(min_length=64, max_length=64)

    @field_validator("selector_policy_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, field_name="selector_policy_hash")


class DatasetSnapshotLabel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    label_key_hash: str = Field(min_length=64, max_length=64)
    label_version_id: str = Field(min_length=1, max_length=160)
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    observation_version_id: str = Field(min_length=1, max_length=160)
    candidate_stage_evidence_id: str = Field(min_length=1, max_length=160)
    symbol: str = Field(min_length=1, max_length=32)
    selector_policy_hash: str = Field(min_length=64, max_length=64)

    @field_validator("label_key_hash", "selector_policy_hash")
    @classmethod
    def _hash(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)


class DatasetSnapshotBlobRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    logical_path: str = Field(min_length=1, max_length=1024)
    logical_role: str = Field(min_length=1, max_length=160)
    partition_key_hash: str = Field(min_length=64, max_length=64)
    ordinal: int = Field(ge=0)
    blob: DatasetBlobHeader
    ref_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("partition_key_hash", "ref_content_hash")
    @classmethod
    def _hash(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_identity(self) -> "DatasetSnapshotBlobRef":
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"ref_content_hash"}))
        if self.ref_content_hash is not None and self.ref_content_hash != digest:
            raise ValueError("snapshot blob ref hash does not match canonical identity")
        object.__setattr__(self, "ref_content_hash", digest)
        return self


class SealedDatasetSnapshot(BaseModel):
    """A fully promoted aggregate.  Batch C accepts its schema but cannot create it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    build_id: str = Field(min_length=1, max_length=160)
    seal_attempt_id: str = Field(min_length=1, max_length=160)
    seal_receipt_hash: str = Field(min_length=64, max_length=64)
    verification_contract_version: str
    manifest_core_sha256: str = Field(min_length=64, max_length=64)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    promotion_receipt_uri: str = Field(min_length=1, max_length=4096)
    promotion_receipt_hash: str = Field(min_length=64, max_length=64)
    snapshot_schema_version: str = Field(min_length=1, max_length=160)
    snapshot_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    capture_set_hash: str = Field(min_length=64, max_length=64)
    base_snapshot: BaseSnapshotIdentity | None = None
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    admission_scope_set_hash: str = Field(min_length=64, max_length=64)
    query_registry_hash: str = Field(min_length=64, max_length=64)
    builder_version: str = Field(min_length=1, max_length=160)
    code_commit: str = Field(min_length=1, max_length=160)
    writer_version: str = Field(min_length=1, max_length=160)
    partition_policy_hash: str = Field(min_length=64, max_length=64)
    policy_compatibility_hash: str = Field(min_length=64, max_length=64)
    dataset_capability_manifest: dict[str, object]
    dataset_capability_manifest_hash: str = Field(min_length=64, max_length=64)
    schema_fingerprint: str = Field(min_length=64, max_length=64)
    files: tuple[DatasetSnapshotFile, ...] = Field(min_length=1)
    observations: tuple[DatasetSnapshotObservation, ...]
    labels: tuple[DatasetSnapshotLabel, ...]
    blob_refs: tuple[DatasetSnapshotBlobRef, ...] = Field(min_length=1)
    label_maturity_event_summary: dict[str, object]
    snapshot_content_hash: str | None = Field(default=None, min_length=64, max_length=64)
    snapshot_id: str | None = Field(default=None, min_length=1, max_length=160)

    @field_validator(
        "seal_receipt_hash", "manifest_core_sha256", "manifest_sha256", "promotion_receipt_hash", "snapshot_source_revision_set_hash",
        "capture_set_hash", "handoff_readiness_hash", "admission_scope_set_hash", "query_registry_hash",
        "policy_compatibility_hash",
        "partition_policy_hash", "dataset_capability_manifest_hash", "schema_fingerprint", "snapshot_content_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _sealed_closure(self) -> "SealedDatasetSnapshot":
        if self.verification_contract_version != BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT:
            raise ValueError("only Batch D full-Parquet verification may form a sealed snapshot")
        if len({item.logical_path for item in self.files}) != len(self.files) or len({item.ordinal_key for item in self.files}) != len(self.files):
            raise ValueError("snapshot files are not unique")
        observations = {item.canonical_signal_id: item for item in self.observations}
        if len(observations) != len(self.observations):
            raise ValueError("snapshot observations are not unique")
        if len({item.label_key_hash for item in self.labels}) != len(self.labels) or len({item.label_version_id for item in self.labels}) != len(self.labels):
            raise ValueError("snapshot labels are not unique")
        if any(item.canonical_signal_id not in observations or observations[item.canonical_signal_id].observation_version_id != item.observation_version_id for item in self.labels):
            raise ValueError("snapshot label does not bind to selected observation")
        refs = {item.logical_path: item for item in self.blob_refs}
        if set(refs) != {item.logical_path for item in self.files}:
            raise ValueError("snapshot blob refs must cover exactly the final file set")
        for file in self.files:
            ref = refs[file.logical_path]
            if ref.blob != file.blob or ref.logical_role != file.logical_role or ref.partition_key_hash != file.partition_key_hash or ref.ordinal != file.ordinal:
                raise ValueError("snapshot blob ref does not match final file identity")
        if canonical_json_sha256(self.dataset_capability_manifest) != self.dataset_capability_manifest_hash:
            raise ValueError("snapshot capability manifest hash is invalid")
        if self.manifest_core_sha256 != canonical_json_sha256({
            "files": [item.model_dump(mode="json") for item in sorted(self.files, key=lambda item: item.logical_path)],
            "observations": [item.model_dump(mode="json") for item in sorted(self.observations, key=lambda item: item.canonical_signal_id)],
            "labels": [item.model_dump(mode="json") for item in sorted(self.labels, key=lambda item: item.label_key_hash)],
            "source_revision_set_hash": self.snapshot_source_revision_set_hash,
            "capture_set_hash": self.capture_set_hash,
            "base_snapshot": self.base_snapshot.model_dump(mode="json") if self.base_snapshot else None,
            "handoff_readiness_hash": self.handoff_readiness_hash,
            "admission_scope_set_hash": self.admission_scope_set_hash,
            "query_registry_hash": self.query_registry_hash,
            "capability_hash": self.dataset_capability_manifest_hash,
            "schema_fingerprint": self.schema_fingerprint,
            "builder_version": self.builder_version,
            "code_commit": self.code_commit,
            "writer_version": self.writer_version,
            "partition_policy_hash": self.partition_policy_hash,
            "policy_compatibility_hash": self.policy_compatibility_hash,
        }):
            raise ValueError("manifest core hash is invalid")
        if self.snapshot_content_hash is not None and self.snapshot_content_hash != self.manifest_core_sha256:
            raise ValueError("snapshot content hash must equal manifest core hash")
        object.__setattr__(self, "snapshot_content_hash", self.manifest_core_sha256)
        expected_id = f"advsnap_{self.manifest_core_sha256[:24]}"
        if self.snapshot_id is not None and self.snapshot_id != expected_id:
            raise ValueError("snapshot id does not match manifest content")
        object.__setattr__(self, "snapshot_id", expected_id)
        return self


class DatasetSnapshotInvalidation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshot_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    invalidated_by: str = Field(min_length=1, max_length=160)
    reason_code: str = Field(min_length=1, max_length=160)
    reason_hash: str | None = Field(default=None, min_length=64, max_length=64)
    invalidation_request_hash: str | None = Field(default=None, min_length=64, max_length=64)
    replacement_snapshot_id: str | None = None
    invalidation_content_hash: str | None = Field(default=None, min_length=64, max_length=64)
    invalidation_id: str | None = Field(default=None, min_length=1, max_length=160)

    @field_validator("manifest_sha256", "reason_hash", "invalidation_request_hash", "invalidation_content_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "DatasetSnapshotInvalidation":
        expected_reason_hash = canonical_json_sha256({"reason_code": self.reason_code})
        if self.reason_hash is not None and self.reason_hash != expected_reason_hash:
            raise ValueError("invalidation reason hash is invalid")
        object.__setattr__(self, "reason_hash", expected_reason_hash)
        request_payload = {
            "snapshot_id": self.snapshot_id,
            "manifest_sha256": self.manifest_sha256,
            "reason_code": self.reason_code,
            "reason_hash": expected_reason_hash,
            "replacement_snapshot_id": self.replacement_snapshot_id,
        }
        expected_request_hash = canonical_json_sha256(request_payload)
        if self.invalidation_request_hash is not None and self.invalidation_request_hash != expected_request_hash:
            raise ValueError("invalidation request hash is invalid")
        object.__setattr__(self, "invalidation_request_hash", expected_request_hash)
        digest = canonical_json_sha256(
            {
                **request_payload,
                "invalidation_request_hash": expected_request_hash,
                "invalidated_by": self.invalidated_by,
            }
        )
        if self.invalidation_content_hash is not None and self.invalidation_content_hash != digest:
            raise ValueError("invalidation content hash is invalid")
        object.__setattr__(self, "invalidation_content_hash", digest)
        expected_id = f"advsnapinv_{digest[:24]}"
        if self.invalidation_id is not None and self.invalidation_id != expected_id:
            raise ValueError("invalidation id is invalid")
        object.__setattr__(self, "invalidation_id", expected_id)
        return self


class InMemorySnapshotInvalidationRepository:
    def __init__(self) -> None:
        self._by_request_hash: dict[str, DatasetSnapshotInvalidation] = {}
        self._by_snapshot_id: dict[str, DatasetSnapshotInvalidation] = {}

    def append(self, invalidation: DatasetSnapshotInvalidation) -> DatasetSnapshotInvalidation:
        existing = self._by_request_hash.get(invalidation.invalidation_request_hash)
        if existing is not None:
            if existing != invalidation:
                raise DatasetBuildError(REASON_BASE_SNAPSHOT_INVALID, "same invalidation request has different content")
            return existing
        if invalidation.snapshot_id in self._by_snapshot_id:
            raise DatasetBuildError(REASON_BASE_SNAPSHOT_INVALID, "snapshot invalidation is append-only and cannot be replaced")
        self._by_request_hash[invalidation.invalidation_request_hash] = invalidation
        self._by_snapshot_id[invalidation.snapshot_id] = invalidation
        return invalidation

    def is_invalidated(self, snapshot_id: str) -> bool:
        return snapshot_id in self._by_snapshot_id


def logical_build_key(request: FixtureDatasetBuildRequest) -> str:
    return canonical_json_sha256(
        {
            "build_request_hash": request.build_request_hash,
            "capture_set_hash": request.capture_set_hash,
            "snapshot_source_revision_set_hash": request.snapshot_source_revision_set_hash,
        }
    )


def build_id_for(logical_key: str, generation: int) -> str:
    if generation < 1:
        raise ValueError("build generation must be positive")
    return f"advbuild_{canonical_json_sha256({'logical_build_key': logical_key, 'generation': generation})[:24]}"


def file_set_hash(files: Iterable[DatasetAttemptFile]) -> str:
    ordered = sorted(files, key=lambda item: item.logical_path)
    if len({item.logical_path for item in ordered}) != len(ordered):
        raise ValueError("attempt file paths are duplicated")
    if len({item.ordinal_key for item in ordered}) != len(ordered):
        raise ValueError("attempt file role/partition/ordinal identities are duplicated")
    return canonical_json_sha256([item.canonical_identity() for item in ordered])


def verify_attempt_file_set(files: Iterable[DatasetAttemptFile]) -> str:
    """Full byte/size verification for Batch C real-file foundation receipts."""

    materialized = tuple(files)
    for item in materialized:
        parsed = urlparse(item.staging_uri)
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "attempt file must use an explicit local file URI")
        raw_path = unquote(parsed.path)
        if os.name == "nt" and len(raw_path) >= 3 and raw_path[0] == "/" and raw_path[2] == ":":
            raw_path = raw_path[1:]
        try:
            payload = Path(raw_path).read_bytes()
        except OSError as error:
            raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "attempt file cannot be read") from error
        if len(payload) != item.size_bytes or hashlib.sha256(payload).hexdigest() != item.sha256:
            raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "attempt file bytes do not match immutable descriptor")
    return file_set_hash(materialized)


def _validated_update(model: BaseModel, updates: dict[str, object]) -> BaseModel:
    """Rebuild frozen contracts through validators; never bypass state closure."""

    payload = model.model_dump(mode="python")
    payload.update(updates)
    return type(model).model_validate(payload)


class DatasetBuildRepository(Protocol):
    def create_or_get(
        self,
        request: FixtureDatasetBuildRequest,
        *,
        actor: str,
        rebuild_predecessor_build_id: str | None = None,
        expected_termination_receipt_hash: str | None = None,
    ) -> DatasetBuild: ...
    def start_attempt(self, **kwargs: object) -> DatasetBuildAttempt: ...
    def heartbeat_attempt(self, **kwargs: object) -> DatasetBuildAttempt: ...


class InMemoryDatasetBuildRepository:
    """Deterministic oracle used by Batch C contract tests before PostgreSQL L4."""

    def __init__(
        self,
        *,
        now_provider: Callable[[], datetime] | None = None,
        base_snapshot_validator: Callable[[BaseSnapshotIdentity], None] | None = None,
    ) -> None:
        self._now = now_provider or (lambda: datetime.now(timezone.utc))
        self._base_snapshot_validator = base_snapshot_validator
        self._builds: dict[str, DatasetBuild] = {}
        self._by_key: dict[str, list[str]] = {}
        self._attempts: dict[str, DatasetBuildAttempt] = {}
        self._files: dict[str, dict[str, DatasetAttemptFile]] = {}
        self._events: dict[str, DatasetBuildEvent] = {}

    def create_or_get(
        self,
        request: FixtureDatasetBuildRequest,
        *,
        actor: str,
        rebuild_predecessor_build_id: str | None = None,
        expected_termination_receipt_hash: str | None = None,
    ) -> DatasetBuild:
        request = FixtureDatasetBuildRequest.model_validate(request.model_dump(mode="python"))
        if request.base_snapshot is not None:
            if self._base_snapshot_validator is None:
                raise DatasetBuildError(REASON_BASE_SNAPSHOT_INVALID, "base snapshot requires an explicit invalidation/admission oracle")
            self._base_snapshot_validator(request.base_snapshot)
        key = logical_build_key(request)
        generation_ids = self._by_key.get(key, [])
        if generation_ids:
            latest = self._builds[generation_ids[-1]]
            if latest.lifecycle in {BuildLifecycle.ACTIVE, BuildLifecycle.SEALED}:
                return latest
            if latest.lifecycle is BuildLifecycle.FAILED_TERMINAL:
                raise DatasetBuildError(REASON_BUILD_GENERATION_INVALID, "terminally failed logical key requires new semantics")
            if (
                rebuild_predecessor_build_id != latest.build_id
                or expected_termination_receipt_hash is None
                or expected_termination_receipt_hash != latest.termination_receipt_hash
            ):
                raise DatasetBuildError(REASON_BUILD_GENERATION_INVALID, "aborted generation requires its exact termination receipt for an explicit rebuild")
            generation = latest.build_generation + 1
        else:
            generation = 1
        now = _aware(self._now(), field_name="now_provider")
        build = DatasetBuild(
            build_id=build_id_for(key, generation), request=request, logical_build_key_sha256=key,
            build_generation=generation,
            predecessor_build_id=latest.build_id if generation > 1 else None,
            created_at=now,
            updated_at=now,
        )
        self._builds[build.build_id] = build
        self._by_key.setdefault(key, []).append(build.build_id)
        self._record_event(build=build, attempt=None, event_type=DatasetBuildEventType.REQUESTED, actor=actor, payload={"request_hash": request.build_request_hash})
        return build

    def start_attempt(
        self,
        *,
        build_id: str,
        operation: AttemptOperation,
        expected_build_row_version: int,
        expected_checkpoint: BuildCheckpoint,
        lease_owner_id: str,
        lease_token: str,
        lease_seconds: int,
        operation_request_hash: str,
    ) -> DatasetBuildAttempt:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        build = self._require_build(build_id)
        if build.lifecycle is not BuildLifecycle.ACTIVE or build.row_version != expected_build_row_version:
            raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "build is not at expected active row version")
        if build.checkpoint is not expected_checkpoint:
            raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "build checkpoint is stale")
        expected_operation = {
            BuildCheckpoint.REQUESTED: AttemptOperation.MATERIALIZE,
            BuildCheckpoint.MATERIALIZED: AttemptOperation.VERIFY,
            BuildCheckpoint.VERIFIED: AttemptOperation.PROMOTE,
            BuildCheckpoint.PROMOTED: AttemptOperation.SEAL,
        }.get(build.checkpoint)
        if operation is not expected_operation:
            raise DatasetBuildError(REASON_ATTEMPT_OPERATION_INVALID, "operation is not the legal next checkpoint transition")
        if build.current_attempt_id is not None:
            current = self._attempts[build.current_attempt_id]
            now = _aware(self._now(), field_name="now_provider")
            if current.state is AttemptState.ACTIVE and current.expires_at > now:
                raise DatasetBuildError(REASON_BUILD_ALREADY_RUNNING, "an active unexpired attempt already owns this build")
            raise DatasetBuildError(
                REASON_ATTEMPT_LEASE_EXPIRED,
                "expired current attempt must be explicitly expired and recovered before a new attempt",
            )
        attempt_no = 1 + sum(1 for item in self._attempts.values() if item.build_id == build_id)
        token = build.current_fencing_token + 1
        now = _aware(self._now(), field_name="now_provider")
        attempt_id = f"advbuildatt_{canonical_json_sha256({'build_id': build_id, 'attempt_no': attempt_no, 'operation': operation.value})[:24]}"
        attempt = DatasetBuildAttempt(
            attempt_id=attempt_id, build_id=build_id, attempt_no=attempt_no, operation=operation,
            state=AttemptState.ACTIVE, lease_owner_id=lease_owner_id, lease_token=lease_token,
            fencing_token=token, expected_build_row_version=expected_build_row_version,
            expected_checkpoint=expected_checkpoint, acquired_at=now, expires_at=now + timedelta(seconds=lease_seconds),
            heartbeat_at=now, operation_request_hash=operation_request_hash,
        )
        self._attempts[attempt_id] = attempt
        self._files[attempt_id] = {}
        self._builds[build_id] = _validated_update(build, {"current_fencing_token": token, "current_attempt_id": attempt_id, "row_version": build.row_version + 1, "updated_at": now})  # type: ignore[assignment]
        self._record_event(build=self._builds[build_id], attempt=attempt, event_type=DatasetBuildEventType.ATTEMPT_STARTED, actor=lease_owner_id, payload={"operation": operation.value})
        return attempt

    def append_file(self, *, attempt_id: str, expected_fencing_token: int, file: DatasetAttemptFile) -> DatasetAttemptFile:
        attempt, build = self._require_active_attempt(attempt_id, expected_fencing_token)
        if attempt.operation is not AttemptOperation.MATERIALIZE or file.attempt_id != attempt_id or file.fencing_token != attempt.fencing_token:
            raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "file does not belong to active materialize attempt")
        current = self._files[attempt_id]
        existing = current.get(file.logical_path)
        if existing is not None:
            if existing == file:
                return existing
            raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "same logical file path has different content")
        if any(item.ordinal_key == file.ordinal_key for item in current.values()):
            raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "file role/partition/ordinal is duplicated")
        current[file.logical_path] = file
        return file

    def heartbeat_attempt(
        self,
        *,
        attempt_id: str,
        expected_fencing_token: int,
        lease_seconds: int,
    ) -> DatasetBuildAttempt:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        attempt, _ = self._require_active_attempt(attempt_id, expected_fencing_token)
        now = _aware(self._now(), field_name="now_provider")
        renewed = _validated_update(
            attempt,
            {"heartbeat_at": now, "expires_at": max(attempt.expires_at, now + timedelta(seconds=lease_seconds))},
        )
        self._attempts[attempt_id] = renewed  # type: ignore[assignment]
        return renewed  # type: ignore[return-value]

    def complete_materialize(self, *, attempt_id: str, expected_fencing_token: int, actor: str) -> DatasetBuild:
        attempt, build = self._require_active_attempt(attempt_id, expected_fencing_token)
        if attempt.operation is not AttemptOperation.MATERIALIZE:
            raise DatasetBuildError(REASON_ATTEMPT_OPERATION_INVALID, "only materialize attempt can complete materialization")
        files = tuple(self._files[attempt_id].values())
        if not files:
            raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "materialize attempt has no immutable files")
        set_hash = verify_attempt_file_set(files)
        receipt = canonical_json_sha256({"attempt_id": attempt_id, "file_set_hash": set_hash, "contract": "MATERIALIZE_V1"})
        now = _aware(self._now(), field_name="now_provider")
        self._attempts[attempt_id] = _validated_update(attempt, {"state": AttemptState.SUCCEEDED, "finished_at": now})  # type: ignore[assignment]
        updated = _validated_update(build, {
            "checkpoint": BuildCheckpoint.MATERIALIZED, "current_attempt_id": None,
            "row_version": build.row_version + 1, "materialized_attempt_id": attempt_id,
            "materialize_receipt_hash": receipt, "materialized_file_set_hash": set_hash, "updated_at": now,
        })
        self._builds[build.build_id] = updated
        self._record_event(build=updated, attempt=self._attempts[attempt_id], event_type=DatasetBuildEventType.MATERIALIZED, actor=actor, payload={"receipt": receipt})
        return updated

    def complete_verify(
        self,
        *,
        attempt_id: str,
        expected_fencing_token: int,
        verification_contract_version: str,
        observed_file_set_hash: str,
        actor: str,
    ) -> DatasetBuild:
        attempt, build = self._require_active_attempt(attempt_id, expected_fencing_token)
        if attempt.operation is not AttemptOperation.VERIFY or build.materialized_file_set_hash != observed_file_set_hash:
            raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "verify must consume exactly the frozen materialized file set")
        if verification_contract_version != BATCH_C_FILESET_VERIFICATION_CONTRACT:
            raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "Batch C can only emit the file-set foundation verification contract")
        receipt = canonical_json_sha256({"attempt_id": attempt_id, "file_set_hash": observed_file_set_hash, "verification_contract_version": verification_contract_version})
        now = _aware(self._now(), field_name="now_provider")
        self._attempts[attempt_id] = _validated_update(attempt, {"state": AttemptState.SUCCEEDED, "finished_at": now})  # type: ignore[assignment]
        updated = _validated_update(build, {
            "checkpoint": BuildCheckpoint.VERIFIED, "current_attempt_id": None, "row_version": build.row_version + 1,
            "verified_attempt_id": attempt_id, "verify_receipt_hash": receipt, "verified_file_set_hash": observed_file_set_hash,
            "verification_contract_version": verification_contract_version, "updated_at": now,
        })
        self._builds[build.build_id] = updated
        self._record_event(build=updated, attempt=self._attempts[attempt_id], event_type=DatasetBuildEventType.VERIFIED, actor=actor, payload={"receipt": receipt})
        return updated

    def fail_attempt(self, *, attempt_id: str, expected_fencing_token: int, error_code: str, actor: str) -> DatasetBuildAttempt:
        attempt, build = self._require_active_attempt(attempt_id, expected_fencing_token)
        now = _aware(self._now(), field_name="now_provider")
        failed = _validated_update(attempt, {"state": AttemptState.FAILED, "finished_at": now, "error_code": error_code, "error_hash": canonical_json_sha256({"error_code": error_code})})
        self._attempts[attempt_id] = failed
        self._builds[build.build_id] = _validated_update(build, {"current_attempt_id": None, "row_version": build.row_version + 1, "updated_at": now})  # type: ignore[assignment]
        self._record_event(build=self._builds[build.build_id], attempt=failed, event_type=DatasetBuildEventType.ATTEMPT_FAILED, actor=actor, payload={"error_code": error_code}, reasons=(error_code,))
        return failed

    def expire_attempt(self, *, attempt_id: str, expected_fencing_token: int, actor: str) -> DatasetBuildAttempt:
        attempt, build = self._require_active_attempt(attempt_id, expected_fencing_token, require_unexpired=False)
        now = _aware(self._now(), field_name="now_provider")
        if attempt.expires_at > now:
            raise DatasetBuildError(REASON_ATTEMPT_LEASE_EXPIRED, "attempt lease has not expired")
        expired = _validated_update(attempt, {"state": AttemptState.EXPIRED, "finished_at": now, "error_code": REASON_ATTEMPT_LEASE_EXPIRED, "error_hash": canonical_json_sha256({"reason": REASON_ATTEMPT_LEASE_EXPIRED})})
        self._attempts[attempt_id] = expired
        self._builds[build.build_id] = _validated_update(build, {"current_attempt_id": None, "current_fencing_token": build.current_fencing_token + 1, "row_version": build.row_version + 1, "updated_at": now})  # type: ignore[assignment]
        self._record_event(build=self._builds[build.build_id], attempt=expired, event_type=DatasetBuildEventType.ATTEMPT_EXPIRED, actor=actor, payload={"attempt": attempt_id}, reasons=(REASON_ATTEMPT_LEASE_EXPIRED,))
        return expired

    def recover_expired_attempt(self, *, expired_attempt_id: str, actor: str) -> DatasetBuildAttempt:
        expired = self._attempts.get(expired_attempt_id)
        if expired is None or expired.state is not AttemptState.EXPIRED:
            raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "recovery requires one expired attempt")
        build = self._require_build(expired.build_id)
        if build.current_attempt_id is not None or build.current_fencing_token <= expired.fencing_token:
            raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "expired attempt has not been fenced off")
        existing_recovery = next(
            (item for item in self._attempts.values() if item.predecessor_attempt_id == expired_attempt_id),
            None,
        )
        if existing_recovery is not None:
            if existing_recovery.lease_owner_id != actor:
                raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "expired attempt recovery has different actor")
            return existing_recovery
        attempt_no = 1 + sum(1 for item in self._attempts.values() if item.build_id == build.build_id)
        now = _aware(self._now(), field_name="now_provider")
        recovery = DatasetBuildAttempt(
            attempt_id=f"advbuildatt_{canonical_json_sha256({'build_id': build.build_id, 'attempt_no': attempt_no, 'operation': AttemptOperation.RECOVER.value})[:24]}",
            build_id=build.build_id, attempt_no=attempt_no, operation=AttemptOperation.RECOVER,
            state=AttemptState.SUCCEEDED, lease_owner_id=actor, lease_token="recovery-receipt",
            fencing_token=build.current_fencing_token, expected_build_row_version=build.row_version,
            expected_checkpoint=build.checkpoint, acquired_at=now, expires_at=now + timedelta(seconds=1),
            heartbeat_at=now, predecessor_attempt_id=expired_attempt_id,
            operation_request_hash=canonical_json_sha256({"expired_attempt_id": expired_attempt_id, "build_fencing": build.current_fencing_token}),
            finished_at=now,
        )
        self._attempts[recovery.attempt_id] = recovery
        self._record_event(build=build, attempt=recovery, event_type=DatasetBuildEventType.RECOVERY_STARTED, actor=actor, payload={"expired_attempt_id": expired_attempt_id})
        return recovery

    def terminate_build(self, *, build_id: str, expected_row_version: int, reason_code: str, terminal: BuildLifecycle, actor: str) -> DatasetBuild:
        if terminal not in {BuildLifecycle.ABORTED, BuildLifecycle.FAILED_TERMINAL}:
            raise ValueError("terminal build lifecycle must be ABORTED or FAILED_TERMINAL")
        build = self._require_build(build_id)
        if build.lifecycle is not BuildLifecycle.ACTIVE or build.row_version != expected_row_version:
            raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "build termination has stale state")
        now = _aware(self._now(), field_name="now_provider")
        receipt = canonical_json_sha256({"build_id": build_id, "checkpoint": build.checkpoint.value, "reason_code": reason_code, "terminal": terminal.value})
        updated = _validated_update(build, {"lifecycle": terminal, "current_attempt_id": None, "row_version": build.row_version + 1, "termination_receipt_hash": receipt, "terminal_reason_code": reason_code, "updated_at": now})
        self._builds[build_id] = updated
        self._record_event(build=updated, attempt=None, event_type=DatasetBuildEventType.BUILD_TERMINATED, actor=actor, payload={"receipt": receipt}, reasons=(reason_code,))
        return updated

    def get_build(self, build_id: str) -> DatasetBuild:
        return self._require_build(build_id)

    def events_for(self, build_id: str) -> tuple[DatasetBuildEvent, ...]:
        return tuple(event for event in self._events.values() if event.build_id == build_id)

    def _require_build(self, build_id: str) -> DatasetBuild:
        build = self._builds.get(build_id)
        if build is None:
            raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "dataset build does not exist")
        return build

    def _require_active_attempt(self, attempt_id: str, fencing_token: int, *, require_unexpired: bool = True) -> tuple[DatasetBuildAttempt, DatasetBuild]:
        attempt = self._attempts.get(attempt_id)
        if attempt is None or attempt.state is not AttemptState.ACTIVE:
            raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "attempt is not active")
        build = self._require_build(attempt.build_id)
        if build.current_attempt_id != attempt_id or build.current_fencing_token != fencing_token or attempt.fencing_token != fencing_token:
            raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "attempt fencing token is stale")
        if require_unexpired and attempt.expires_at <= _aware(self._now(), field_name="now_provider"):
            raise DatasetBuildError(REASON_ATTEMPT_LEASE_EXPIRED, "attempt lease has expired")
        return attempt, build

    def _record_event(self, *, build: DatasetBuild, attempt: DatasetBuildAttempt | None, event_type: DatasetBuildEventType, actor: str, payload: dict[str, object], reasons: tuple[str, ...] = ()) -> None:
        payload_hash = canonical_json_sha256(payload)
        event = DatasetBuildEvent(
            build_id=build.build_id, attempt_id=attempt.attempt_id if attempt else None,
            fencing_token=attempt.fencing_token if attempt else None, event_type=event_type,
            event_at=_aware(self._now(), field_name="now_provider"), actor=actor,
            payload_hash=payload_hash, reason_codes=tuple(sorted(set(reasons))),
        )
        if event.event_id in self._events and self._events[event.event_id] != event:
            raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "build event id collides with different content")
        self._events[str(event.event_id)] = event
