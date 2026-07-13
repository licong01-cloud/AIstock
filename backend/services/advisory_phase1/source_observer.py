"""Typed contracts and pure decision logic for the Phase 1D source observer.

The observer treats ``market.dataset_date_refresh_audit`` only as an ingestion
completion signal.  The append-only source availability ledger remains the
only availability authority consumed by later Phase 1 work.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Iterable, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonical_json_text, canonicalize
from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEvent,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
)


SOURCE_OBSERVER_CONFIG_SCHEMA_VERSION = "advisory_phase1_source_observer_config_v1"
SOURCE_OBSERVATION_RECEIPT_SCHEMA_VERSION = "advisory_phase1_source_observation_receipt_v1"
SOURCE_AUDIT_SNAPSHOT_SCHEMA_VERSION = "advisory_phase1_source_audit_snapshot_v1"
SOURCE_PARTITION_DESCRIPTOR_SCHEMA_VERSION = "advisory_phase1_source_partition_descriptor_v1"
CANONICAL_STREAM_HASH_ALGORITHM = "canonical_stream_sha256_v1"

REASON_OBSERVER_CONFIG_INVALID = "ADVISORY_PHASE1_SOURCE_OBSERVER_CONFIG_INVALID"
REASON_OBSERVER_REGISTRY_MISMATCH = "ADVISORY_PHASE1_SOURCE_OBSERVER_REGISTRY_MISMATCH"
REASON_AUDIT_SCHEMA_MISSING = "ADVISORY_PHASE1_SOURCE_OBSERVER_AUDIT_SCHEMA_MISSING"
REASON_AUDIT_ROW_CHANGED = "ADVISORY_PHASE1_SOURCE_OBSERVER_AUDIT_ROW_CHANGED"
REASON_CURSOR_CONFLICT = "ADVISORY_PHASE1_SOURCE_OBSERVER_CURSOR_CONFLICT"
REASON_OBSERVER_UNEXPECTED = "ADVISORY_PHASE1_SOURCE_OBSERVER_UNEXPECTED"
REASON_AUDIT_STATUS_NOT_ELIGIBLE = "ADVISORY_PHASE1_SOURCE_OBSERVER_AUDIT_STATUS_NOT_ELIGIBLE"
REASON_AUDIT_QUALITY_NOT_ELIGIBLE = "ADVISORY_PHASE1_SOURCE_OBSERVER_QUALITY_NOT_ELIGIBLE"
REASON_AUDIT_EMPTY_NOT_ELIGIBLE = "ADVISORY_PHASE1_SOURCE_OBSERVER_EMPTY_NOT_ELIGIBLE"
REASON_AUDIT_COVERAGE_NOT_ELIGIBLE = "ADVISORY_PHASE1_SOURCE_OBSERVER_COVERAGE_NOT_ELIGIBLE"
REASON_ROW_COUNT_MISMATCH = "ADVISORY_PHASE1_SOURCE_OBSERVER_ROW_COUNT_MISMATCH"
REASON_SCHEMA_MISMATCH = "ADVISORY_PHASE1_SOURCE_OBSERVER_SCHEMA_MISMATCH"
REASON_SOURCE_ROW_INVALID = "ADVISORY_PHASE1_SOURCE_OBSERVER_SOURCE_ROW_INVALID"
REASON_RESOURCE_LIMIT = "ADVISORY_PHASE1_SOURCE_OBSERVER_RESOURCE_LIMIT"
REASON_EVENT_CONFLICT = "ADVISORY_PHASE1_SOURCE_OBSERVER_EVENT_CONFLICT"
REASON_RECEIPT_CONFLICT = "ADVISORY_PHASE1_SOURCE_OBSERVER_RECEIPT_CONFLICT"


class SourceObserverError(RuntimeError):
    """A stable error with safe diagnostic context for the standalone worker."""

    def __init__(self, reason_code: str, detail: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.context = dict(context or {})


class ObservationOutcome(str, Enum):
    EVENT_APPENDED = "EVENT_APPENDED"
    UNCHANGED = "UNCHANGED"
    NOT_ELIGIBLE = "NOT_ELIGIBLE"


class RowCountParityPolicy(str, Enum):
    EXACT = "EXACT"


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return value.astimezone(UTC)


def _require_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


class SourceSchemaColumn(BaseModel):
    """One source projection column resolved only from the compiled registry."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=80)
    pg_data_type: str = Field(min_length=1, max_length=80)
    nullable: bool

    def canonical_payload(self) -> dict[str, Any]:
        return {"name": self.name, "pg_data_type": self.pg_data_type, "nullable": self.nullable}


class SourceQueryTemplate(BaseModel):
    """A compiled query template; configuration can reference but not alter it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    template_id: str = Field(min_length=1, max_length=160)
    template_version: str = Field(min_length=1, max_length=80)
    schema_name: str = Field(min_length=1, max_length=80)
    table_name: str = Field(min_length=1, max_length=80)
    sql: str = Field(min_length=1)
    columns: tuple[SourceSchemaColumn, ...] = Field(min_length=1)
    partition_parameter_name: str = Field(default="trade_date", min_length=1, max_length=80)

    @property
    def schema_fingerprint(self) -> str:
        return canonical_json_sha256(
            {
                "template_id": self.template_id,
                "template_version": self.template_version,
                "schema_name": self.schema_name,
                "table_name": self.table_name,
                "columns": [column.canonical_payload() for column in self.columns],
            }
        )

    @property
    def template_hash(self) -> str:
        return canonical_json_sha256(
            {
                "template_id": self.template_id,
                "template_version": self.template_version,
                "schema_fingerprint": self.schema_fingerprint,
                "sql": self.sql,
            }
        )


class ObservedDatasetSpec(BaseModel):
    """Versioned mapping between one audit dataset and compiled source templates."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_name: str = Field(min_length=1, max_length=160)
    allowed_data_sources: tuple[str, ...] = Field(min_length=1)
    source_roles: tuple[str, ...] = Field(min_length=1)
    query_template_id: str = Field(min_length=1, max_length=160)
    query_template_version: str = Field(min_length=1, max_length=80)
    audit_partition_mapper_id: str = Field(default="trade_date_v1", min_length=1, max_length=80)
    eligible_audit_statuses: tuple[str, ...] = ("success",)
    eligible_quality_statuses: tuple[str, ...] = ("ok",)
    allow_empty_partition: bool = False
    min_coverage_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    row_count_parity_policy: RowCountParityPolicy = RowCountParityPolicy.EXACT

    @model_validator(mode="after")
    def _validate_ordered_sets(self) -> "ObservedDatasetSpec":
        for field_name in ("allowed_data_sources", "source_roles", "eligible_audit_statuses", "eligible_quality_statuses"):
            values = tuple(getattr(self, field_name))
            if tuple(sorted(set(values))) != values:
                raise ValueError(f"{field_name} must be sorted and duplicate-free")
        return self

    def canonical_payload(self, template: SourceQueryTemplate) -> dict[str, Any]:
        if template.template_id != self.query_template_id or template.template_version != self.query_template_version:
            raise SourceObserverError(
                REASON_OBSERVER_REGISTRY_MISMATCH,
                "dataset spec references a different compiled query template",
                context={"dataset_name": self.dataset_name, "template_id": self.query_template_id},
            )
        return {
            "dataset_name": self.dataset_name,
            "allowed_data_sources": list(self.allowed_data_sources),
            "source_roles": list(self.source_roles),
            "query_template_id": self.query_template_id,
            "query_template_version": self.query_template_version,
            "query_template_hash": template.template_hash,
            "audit_partition_mapper_id": self.audit_partition_mapper_id,
            "eligible_audit_statuses": list(self.eligible_audit_statuses),
            "eligible_quality_statuses": list(self.eligible_quality_statuses),
            "allow_empty_partition": self.allow_empty_partition,
            "min_coverage_ratio": self.min_coverage_ratio,
            "row_count_parity_policy": self.row_count_parity_policy.value,
        }


class SourceObserverConfigBundle(BaseModel):
    """Frozen runtime configuration; it contains no approval or role semantics."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    observer_config_id: str = Field(min_length=1, max_length=160)
    observer_config_version: str = Field(min_length=1, max_length=80)
    effective_from_observed_at: datetime
    poll_interval_seconds: int = Field(ge=1, le=86_400)
    audit_scan_batch_size: int = Field(ge=1, le=1_000)
    source_fetch_rows: int = Field(ge=1, le=1_000_000)
    statement_timeout_ms: int = Field(ge=1, le=3_600_000)
    lock_timeout_ms: int = Field(ge=1, le=3_600_000)
    serialization_retry_limit: int = Field(ge=0, le=10)
    max_partition_rows: int = Field(ge=1)
    max_partition_bytes: int = Field(ge=1)
    created_by_service_principal: str = Field(min_length=1, max_length=160)
    dataset_specs: tuple[ObservedDatasetSpec, ...] = Field(min_length=1)

    @field_validator("effective_from_observed_at")
    @classmethod
    def _validate_effective_from(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="effective_from_observed_at")

    @model_validator(mode="after")
    def _validate_specs(self) -> "SourceObserverConfigBundle":
        dataset_names = tuple(spec.dataset_name for spec in self.dataset_specs)
        if tuple(sorted(dataset_names)) != dataset_names or len(set(dataset_names)) != len(dataset_names):
            raise ValueError("dataset_specs must be ordered by unique dataset_name")
        return self

    def query_registry_payload(self, registry: Mapping[str, SourceQueryTemplate]) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_OBSERVER_CONFIG_SCHEMA_VERSION,
            "dataset_specs": [spec.canonical_payload(resolve_query_template(spec, registry)) for spec in self.dataset_specs],
        }

    def query_registry_hash(self, registry: Mapping[str, SourceQueryTemplate]) -> str:
        return canonical_json_sha256(self.query_registry_payload(registry))

    def canonical_payload(self, registry: Mapping[str, SourceQueryTemplate]) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_OBSERVER_CONFIG_SCHEMA_VERSION,
            "observer_config_id": self.observer_config_id,
            "observer_config_version": self.observer_config_version,
            "effective_from_observed_at": self.effective_from_observed_at,
            "query_registry_hash": self.query_registry_hash(registry),
            "poll_interval_seconds": self.poll_interval_seconds,
            "audit_scan_batch_size": self.audit_scan_batch_size,
            "source_fetch_rows": self.source_fetch_rows,
            "statement_timeout_ms": self.statement_timeout_ms,
            "lock_timeout_ms": self.lock_timeout_ms,
            "serialization_retry_limit": self.serialization_retry_limit,
            "max_partition_rows": self.max_partition_rows,
            "max_partition_bytes": self.max_partition_bytes,
            "created_by_service_principal": self.created_by_service_principal,
            "dataset_specs": [spec.canonical_payload(resolve_query_template(spec, registry)) for spec in self.dataset_specs],
        }

    def config_hash(self, registry: Mapping[str, SourceQueryTemplate]) -> str:
        return canonical_json_sha256(self.canonical_payload(registry))


class AuditRowSnapshot(BaseModel):
    """Canonical projection of the mutable readiness row observed by the worker."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_name: str = Field(min_length=1, max_length=160)
    trade_date: date
    data_source: str = Field(min_length=1, max_length=160)
    job_id: str | None = Field(default=None, max_length=160)
    status: str = Field(min_length=1, max_length=32)
    row_count: int = Field(ge=0)
    refreshed_at: datetime
    error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    data_max_at: datetime | None = None
    written_rows: int | None = Field(default=None, ge=0)
    expected_rows: int | None = Field(default=None, ge=0)
    coverage_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    quality_status: str = Field(min_length=1, max_length=32)
    failure_category: str | None = Field(default=None, max_length=160)

    @field_validator("refreshed_at", "data_max_at")
    @classmethod
    def _validate_times(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _require_aware(value, field_name="audit timestamp")

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_AUDIT_SNAPSHOT_SCHEMA_VERSION,
            "dataset_name": self.dataset_name,
            "trade_date": self.trade_date,
            "data_source": self.data_source,
            "job_id": self.job_id,
            "status": self.status,
            "row_count": self.row_count,
            "refreshed_at": self.refreshed_at,
            "error_message": self.error_message,
            "metadata": canonicalize(self.metadata),
            "data_max_at": self.data_max_at,
            "written_rows": self.written_rows,
            "expected_rows": self.expected_rows,
            "coverage_ratio": self.coverage_ratio,
            "quality_status": self.quality_status,
            "failure_category": self.failure_category,
        }

    @property
    def audit_row_hash(self) -> str:
        return canonical_json_sha256(self.canonical_payload())


class SourcePartitionDescriptor(BaseModel):
    """Full deterministic descriptor of one source partition read by the observer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_fingerprint: str = Field(min_length=64, max_length=64)
    row_count: int = Field(ge=0)
    partition_content_hash: str = Field(min_length=64, max_length=64)
    canonical_bytes: int = Field(ge=0)

    @field_validator("schema_fingerprint", "partition_content_hash")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="source descriptor hash")

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_PARTITION_DESCRIPTOR_SCHEMA_VERSION,
            "schema_fingerprint": self.schema_fingerprint,
            "row_count": self.row_count,
            "partition_content_hash": self.partition_content_hash,
            "canonical_bytes": self.canonical_bytes,
        }


class SourceObservationReceipt(BaseModel):
    """Immutable processing evidence; it is not source availability authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    observer_config_id: str = Field(min_length=1, max_length=160)
    observer_config_version: str = Field(min_length=1, max_length=80)
    observer_config_hash: str = Field(min_length=64, max_length=64)
    dataset_name: str = Field(min_length=1, max_length=160)
    data_source: str = Field(min_length=1, max_length=160)
    source_role: str = Field(min_length=1, max_length=80)
    trade_date: date
    partition_key: dict[str, Any] = Field(min_length=1)
    partition_key_hash: str = Field(min_length=64, max_length=64)
    audit_refreshed_at: datetime
    audit_row_hash: str = Field(min_length=64, max_length=64)
    outcome: ObservationOutcome
    availability_event_id: str | None = Field(default=None, max_length=160)
    availability_event_hash: str | None = Field(default=None, min_length=64, max_length=64)
    observed_schema_fingerprint: str | None = Field(default=None, min_length=64, max_length=64)
    observed_row_count: int | None = Field(default=None, ge=0)
    observed_partition_content_hash: str | None = Field(default=None, min_length=64, max_length=64)
    reason_codes: tuple[str, ...] = ()
    observed_at: datetime

    @field_validator(
        "observer_config_hash",
        "partition_key_hash",
        "audit_row_hash",
        "availability_event_hash",
        "observed_schema_fingerprint",
        "observed_partition_content_hash",
    )
    @classmethod
    def _validate_receipt_hashes(cls, value: str | None) -> str | None:
        return None if value is None else _require_sha256(value, field_name="receipt hash field")

    @field_validator("audit_refreshed_at", "observed_at")
    @classmethod
    def _validate_receipt_times(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="receipt timestamp")

    @model_validator(mode="after")
    def _validate_outcome_shape(self) -> "SourceObservationReceipt":
        event_values = (self.availability_event_id, self.availability_event_hash)
        descriptor_values = (
            self.observed_schema_fingerprint,
            self.observed_row_count,
            self.observed_partition_content_hash,
        )
        if self.outcome in {ObservationOutcome.EVENT_APPENDED, ObservationOutcome.UNCHANGED}:
            if any(value is None for value in event_values + descriptor_values) or self.reason_codes:
                raise ValueError("event outcomes require event and descriptor values without reasons")
        else:
            if any(value is not None for value in event_values + descriptor_values) or not self.reason_codes:
                raise ValueError("NOT_ELIGIBLE requires reasons and no event or descriptor")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_OBSERVATION_RECEIPT_SCHEMA_VERSION,
            "observer_config_id": self.observer_config_id,
            "observer_config_version": self.observer_config_version,
            "observer_config_hash": self.observer_config_hash,
            "dataset_name": self.dataset_name,
            "data_source": self.data_source,
            "source_role": self.source_role,
            "trade_date": self.trade_date,
            "partition_key": canonicalize(self.partition_key),
            "partition_key_hash": self.partition_key_hash,
            "audit_refreshed_at": self.audit_refreshed_at,
            "audit_row_hash": self.audit_row_hash,
            "outcome": self.outcome.value,
            "availability_event_id": self.availability_event_id,
            "availability_event_hash": self.availability_event_hash,
            "observed_schema_fingerprint": self.observed_schema_fingerprint,
            "observed_row_count": self.observed_row_count,
            "observed_partition_content_hash": self.observed_partition_content_hash,
            "reason_codes": list(self.reason_codes),
            "observed_at": self.observed_at,
        }

    @property
    def observation_receipt_hash(self) -> str:
        return canonical_json_sha256(self.canonical_payload())

    @property
    def observation_receipt_id(self) -> str:
        return f"asor_{self.observation_receipt_hash[:20]}"


class SourceObserverCursor(BaseModel):
    """Mutable worker checkpoint; it never participates in availability resolution."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    observer_config_hash: str = Field(min_length=64, max_length=64)
    dataset_name: str = Field(min_length=1, max_length=160)
    data_source: str = Field(min_length=1, max_length=160)
    source_role: str = Field(min_length=1, max_length=80)
    last_audit_refreshed_at: datetime
    last_trade_date: date | None = None
    last_audit_row_hash: str | None = Field(default=None, min_length=64, max_length=64)
    row_version: int = Field(ge=1)
    updated_at: datetime

    @field_validator("observer_config_hash", "last_audit_row_hash")
    @classmethod
    def _validate_cursor_hashes(cls, value: str | None) -> str | None:
        return None if value is None else _require_sha256(value, field_name="cursor hash")

    @field_validator("last_audit_refreshed_at", "updated_at")
    @classmethod
    def _validate_cursor_times(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="cursor timestamp")


class ObservationDecision(BaseModel):
    """Pure result before the repository assigns observation time and persists it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: ObservationOutcome
    partition_key: dict[str, Any] = Field(min_length=1)
    reason_codes: tuple[str, ...] = ()
    descriptor: SourcePartitionDescriptor | None = None
    event_request: SourceAvailabilityEventRequest | None = None
    terminal_event: SourceAvailabilityEvent | None = None

    @model_validator(mode="after")
    def _validate_shape(self) -> "ObservationDecision":
        if self.outcome is ObservationOutcome.NOT_ELIGIBLE:
            if not self.reason_codes or self.descriptor is not None or self.event_request is not None:
                raise ValueError("NOT_ELIGIBLE decision requires reasons and no descriptor/event")
        elif self.descriptor is None or self.terminal_event is None and self.outcome is ObservationOutcome.UNCHANGED:
            raise ValueError("event decisions require descriptor; unchanged requires terminal event")
        return self


def resolve_query_template(spec: ObservedDatasetSpec, registry: Mapping[str, SourceQueryTemplate]) -> SourceQueryTemplate:
    template = registry.get(spec.query_template_id)
    if template is None or template.template_version != spec.query_template_version:
        raise SourceObserverError(
            REASON_OBSERVER_REGISTRY_MISMATCH,
            "dataset spec references an unknown query template",
            context={"dataset_name": spec.dataset_name, "template_id": spec.query_template_id},
        )
    return template


def canonical_partition_key(spec: ObservedDatasetSpec, audit: AuditRowSnapshot) -> dict[str, Any]:
    if spec.audit_partition_mapper_id != "trade_date_v1":
        raise SourceObserverError(
            REASON_OBSERVER_REGISTRY_MISMATCH,
            "dataset spec references an unknown partition mapper",
            context={"dataset_name": spec.dataset_name, "mapper": spec.audit_partition_mapper_id},
        )
    if audit.dataset_name != spec.dataset_name:
        raise SourceObserverError(
            REASON_OBSERVER_CONFIG_INVALID,
            "audit row does not match dataset spec",
            context={"audit_dataset": audit.dataset_name, "spec_dataset": spec.dataset_name},
        )
    return {"trade_date": audit.trade_date.isoformat()}


def audit_eligibility_reasons(spec: ObservedDatasetSpec, audit: AuditRowSnapshot) -> tuple[str, ...]:
    reasons: list[str] = []
    if audit.data_source not in spec.allowed_data_sources or audit.status not in spec.eligible_audit_statuses:
        reasons.append(REASON_AUDIT_STATUS_NOT_ELIGIBLE)
    if audit.quality_status not in spec.eligible_quality_statuses:
        reasons.append(REASON_AUDIT_QUALITY_NOT_ELIGIBLE)
    if audit.row_count == 0 and not spec.allow_empty_partition:
        reasons.append(REASON_AUDIT_EMPTY_NOT_ELIGIBLE)
    if spec.min_coverage_ratio is not None and audit.coverage_ratio is not None and audit.coverage_ratio < spec.min_coverage_ratio:
        reasons.append(REASON_AUDIT_COVERAGE_NOT_ELIGIBLE)
    return tuple(sorted(set(reasons)))


def canonical_source_partition_descriptor(
    *,
    template: SourceQueryTemplate,
    rows: Iterable[Mapping[str, Any]],
    max_rows: int,
    max_bytes: int,
) -> SourcePartitionDescriptor:
    """Hash every projected source row in registry column order without sampling."""

    digest = hashlib.sha256()
    header = canonical_json_text(
        {
            "schema_version": SOURCE_PARTITION_DESCRIPTOR_SCHEMA_VERSION,
            "hash_algorithm": CANONICAL_STREAM_HASH_ALGORITHM,
            "schema_fingerprint": template.schema_fingerprint,
            "columns": [column.name for column in template.columns],
        }
    ).encode("utf-8")
    digest.update(header)
    digest.update(b"\n")
    row_count = 0
    canonical_bytes = len(header) + 1
    columns = tuple(column.name for column in template.columns)
    for raw_row in rows:
        try:
            row = {column: raw_row[column] for column in columns}
        except KeyError as exc:
            raise SourceObserverError(
                REASON_SOURCE_ROW_INVALID,
                "source query row misses a registry column",
                context={"template_id": template.template_id, "column": str(exc)},
            ) from exc
        row_bytes = canonical_json_text(canonicalize(row)).encode("utf-8") + b"\n"
        row_count += 1
        canonical_bytes += len(row_bytes)
        if row_count > max_rows or canonical_bytes > max_bytes:
            raise SourceObserverError(
                REASON_RESOURCE_LIMIT,
                "source partition exceeds configured row or byte bound",
                context={
                    "template_id": template.template_id,
                    "row_count": row_count,
                    "canonical_bytes": canonical_bytes,
                    "max_rows": max_rows,
                    "max_bytes": max_bytes,
                },
            )
        digest.update(row_bytes)
    return SourcePartitionDescriptor(
        schema_fingerprint=template.schema_fingerprint,
        row_count=row_count,
        partition_content_hash=digest.hexdigest(),
        canonical_bytes=canonical_bytes,
    )


def _revision_id(
    *,
    spec: ObservedDatasetSpec,
    template: SourceQueryTemplate,
    audit: AuditRowSnapshot,
    source_role: str,
    partition_key: Mapping[str, Any],
    descriptor: SourcePartitionDescriptor,
) -> str:
    return canonical_json_sha256(
        {
            "schema_version": "advisory_phase1_source_revision_identity_v1",
            "dataset_name": spec.dataset_name,
            "data_source": audit.data_source,
            "source_role": source_role,
            "partition_key": canonicalize(dict(partition_key)),
            "query_template_id": template.template_id,
            "query_template_version": template.template_version,
            "query_template_hash": template.template_hash,
            "schema_fingerprint": descriptor.schema_fingerprint,
            "partition_content_hash": descriptor.partition_content_hash,
            "provider_job_descriptor": {
                "provider_job_id": None,
                "refresh_job_id": audit.job_id,
                "provider_published_at": None,
            },
        }
    )


def decide_observation(
    *,
    config: SourceObserverConfigBundle,
    spec: ObservedDatasetSpec,
    template: SourceQueryTemplate,
    audit: AuditRowSnapshot,
    source_role: str,
    descriptor: SourcePartitionDescriptor | None,
    terminal_event: SourceAvailabilityEvent | None,
) -> ObservationDecision:
    """Derive the exact event-or-receipt outcome for one audit/source/role input."""

    if source_role not in spec.source_roles:
        raise SourceObserverError(
            REASON_OBSERVER_CONFIG_INVALID,
            "source role is not registered for dataset",
            context={"dataset_name": spec.dataset_name, "source_role": source_role},
        )
    partition_key = canonical_partition_key(spec, audit)
    reasons = audit_eligibility_reasons(spec, audit)
    if reasons:
        return ObservationDecision(
            outcome=ObservationOutcome.NOT_ELIGIBLE,
            partition_key=partition_key,
            reason_codes=reasons,
        )
    if descriptor is None:
        raise SourceObserverError(
            REASON_SOURCE_ROW_INVALID,
            "eligible audit input requires a full source descriptor",
            context={"dataset_name": audit.dataset_name, "trade_date": audit.trade_date.isoformat()},
        )
    if spec.row_count_parity_policy is RowCountParityPolicy.EXACT and descriptor.row_count != audit.row_count:
        raise SourceObserverError(
            REASON_ROW_COUNT_MISMATCH,
            "audit row count differs from full source partition count",
            context={"audit_row_count": audit.row_count, "source_row_count": descriptor.row_count},
        )
    revision_id = _revision_id(
        spec=spec,
        template=template,
        audit=audit,
        source_role=source_role,
        partition_key=partition_key,
        descriptor=descriptor,
    )
    if terminal_event is not None:
        terminal_descriptor_matches = (
            terminal_event.input.schema_fingerprint == descriptor.schema_fingerprint
            and terminal_event.input.row_count == descriptor.row_count
            and terminal_event.input.partition_content_hash == descriptor.partition_content_hash
        )
        if terminal_descriptor_matches:
            if terminal_event.event_type is SourceAvailabilityEventType.INVALIDATED:
                raise SourceObserverError(
                    REASON_EVENT_CONFLICT,
                    "an invalidated source event cannot become unchanged availability evidence",
                    context={"availability_event_hash": terminal_event.event_content_hash},
                )
            return ObservationDecision(
                outcome=ObservationOutcome.UNCHANGED,
                partition_key=partition_key,
                descriptor=descriptor,
                terminal_event=terminal_event,
            )
        event_type = (
            SourceAvailabilityEventType.REVALIDATED
            if terminal_event.event_type is SourceAvailabilityEventType.INVALIDATED
            else SourceAvailabilityEventType.CORRECTED
        )
        event_revision_no = terminal_event.event_revision_no + 1
        predecessor_event_hash = terminal_event.event_content_hash
    else:
        event_type = SourceAvailabilityEventType.INGESTED
        event_revision_no = 1
        predecessor_event_hash = None
    request = SourceAvailabilityEventRequest(
        dataset_name=spec.dataset_name,
        source_role=source_role,
        partition_key=partition_key,
        revision_id=revision_id,
        event_revision_no=event_revision_no,
        event_type=event_type,
        predecessor_event_hash=predecessor_event_hash,
        provider_job_id=None,
        refresh_job_id=audit.job_id,
        provider_published_at=None,
        schema_fingerprint=descriptor.schema_fingerprint,
        row_count=descriptor.row_count,
        partition_content_hash=descriptor.partition_content_hash,
        quality_status="PASS",
        reason_codes=(),
        created_by_service_principal=config.created_by_service_principal,
    )
    return ObservationDecision(
        outcome=ObservationOutcome.EVENT_APPENDED,
        partition_key=partition_key,
        descriptor=descriptor,
        event_request=request,
        terminal_event=terminal_event,
    )


def build_observation_receipt(
    *,
    config: SourceObserverConfigBundle,
    registry: Mapping[str, SourceQueryTemplate],
    audit: AuditRowSnapshot,
    source_role: str,
    decision: ObservationDecision,
    observed_at: datetime,
    event: SourceAvailabilityEvent | None,
) -> SourceObservationReceipt:
    """Materialize one receipt after the repository has an authoritative DB time."""

    config_hash = config.config_hash(registry)
    if decision.outcome is ObservationOutcome.NOT_ELIGIBLE:
        return SourceObservationReceipt(
            observer_config_id=config.observer_config_id,
            observer_config_version=config.observer_config_version,
            observer_config_hash=config_hash,
            dataset_name=audit.dataset_name,
            data_source=audit.data_source,
            source_role=source_role,
            trade_date=audit.trade_date,
            partition_key=decision.partition_key,
            partition_key_hash=canonical_json_sha256(canonicalize(decision.partition_key)),
            audit_refreshed_at=audit.refreshed_at,
            audit_row_hash=audit.audit_row_hash,
            outcome=ObservationOutcome.NOT_ELIGIBLE,
            reason_codes=decision.reason_codes,
            observed_at=observed_at,
        )
    if decision.descriptor is None or event is None:
        raise SourceObserverError(
            REASON_RECEIPT_CONFLICT,
            "event receipt requires descriptor and persisted availability event",
            context={"outcome": decision.outcome.value},
        )
    return SourceObservationReceipt(
        observer_config_id=config.observer_config_id,
        observer_config_version=config.observer_config_version,
        observer_config_hash=config_hash,
        dataset_name=audit.dataset_name,
        data_source=audit.data_source,
        source_role=source_role,
        trade_date=audit.trade_date,
        partition_key=decision.partition_key,
        partition_key_hash=canonical_json_sha256(canonicalize(decision.partition_key)),
        audit_refreshed_at=audit.refreshed_at,
        audit_row_hash=audit.audit_row_hash,
        outcome=decision.outcome,
        availability_event_id=event.availability_event_id,
        availability_event_hash=event.event_content_hash,
        observed_schema_fingerprint=decision.descriptor.schema_fingerprint,
        observed_row_count=decision.descriptor.row_count,
        observed_partition_content_hash=decision.descriptor.partition_content_hash,
        reason_codes=(),
        observed_at=observed_at,
    )


def _template(*, template_id: str, version: str, table_name: str, sql: str, columns: tuple[tuple[str, str, bool], ...]) -> SourceQueryTemplate:
    return SourceQueryTemplate(
        template_id=template_id,
        template_version=version,
        schema_name="market",
        table_name=table_name,
        sql=sql,
        columns=tuple(SourceSchemaColumn(name=name, pg_data_type=data_type, nullable=nullable) for name, data_type, nullable in columns),
    )


SOURCE_QUERY_TEMPLATES: dict[str, SourceQueryTemplate] = {
    "market_daily_basic_trade_date_v1": _template(
        template_id="market_daily_basic_trade_date_v1",
        version="v1",
        table_name="daily_basic",
        sql=(
            "SELECT trade_date, ts_code, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, "
            "dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv "
            "FROM market.daily_basic WHERE trade_date = %s ORDER BY ts_code"
        ),
        columns=(
            ("trade_date", "date", False), ("ts_code", "text", False), ("close", "numeric", True),
            ("turnover_rate", "numeric", True), ("turnover_rate_f", "numeric", True), ("volume_ratio", "numeric", True),
            ("pe", "numeric", True), ("pe_ttm", "numeric", True), ("pb", "numeric", True), ("ps", "numeric", True),
            ("ps_ttm", "numeric", True), ("dv_ratio", "numeric", True), ("dv_ttm", "numeric", True),
            ("total_share", "numeric", True), ("float_share", "numeric", True), ("free_share", "numeric", True),
            ("total_mv", "numeric", True), ("circ_mv", "numeric", True),
        ),
    ),
    "market_adj_factor_trade_date_v1": _template(
        template_id="market_adj_factor_trade_date_v1",
        version="v1",
        table_name="adj_factor",
        sql="SELECT trade_date, ts_code, adj_factor FROM market.adj_factor WHERE trade_date = %s ORDER BY ts_code",
        columns=(("trade_date", "date", False), ("ts_code", "text", False), ("adj_factor", "double precision", False)),
    ),
    "market_stk_limit_trade_date_v1": _template(
        template_id="market_stk_limit_trade_date_v1",
        version="v1",
        table_name="stk_limit",
        sql="SELECT trade_date, ts_code, pre_close, up_limit, down_limit FROM market.stk_limit WHERE trade_date = %s ORDER BY ts_code",
        columns=(
            ("trade_date", "date", False), ("ts_code", "text", False), ("pre_close", "numeric", True),
            ("up_limit", "numeric", True), ("down_limit", "numeric", True),
        ),
    ),
    "market_suspend_d_trade_date_v1": _template(
        template_id="market_suspend_d_trade_date_v1",
        version="v1",
        table_name="suspend_d",
        sql="SELECT trade_date, ts_code, suspend_type, suspend_timing FROM market.suspend_d WHERE trade_date = %s ORDER BY ts_code, suspend_type",
        columns=(
            ("trade_date", "date", False), ("ts_code", "text", False), ("suspend_type", "text", False),
            ("suspend_timing", "text", True),
        ),
    ),
    "market_index_daily_trade_date_v1": _template(
        template_id="market_index_daily_trade_date_v1",
        version="v1",
        table_name="index_daily",
        sql=(
            "SELECT ts_code, trade_date, close, open, high, low, pre_close, change, pct_chg, vol, amount "
            "FROM market.index_daily WHERE trade_date = %s ORDER BY ts_code"
        ),
        columns=(
            ("ts_code", "character varying", False), ("trade_date", "date", False), ("close", "numeric", True),
            ("open", "numeric", True), ("high", "numeric", True), ("low", "numeric", True),
            ("pre_close", "numeric", True), ("change", "numeric", True), ("pct_chg", "numeric", True),
            ("vol", "numeric", True), ("amount", "numeric", True),
        ),
    ),
}


def default_source_observer_config() -> SourceObserverConfigBundle:
    """Return the only compiled DEV fixture configuration; production is not registered."""

    return SourceObserverConfigBundle(
        observer_config_id="phase1d_market_daily_dev_v1",
        observer_config_version="v1",
        effective_from_observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        poll_interval_seconds=300,
        audit_scan_batch_size=100,
        source_fetch_rows=10_000,
        statement_timeout_ms=30_000,
        lock_timeout_ms=5_000,
        serialization_retry_limit=3,
        max_partition_rows=1_000_000,
        max_partition_bytes=512 * 1024 * 1024,
        created_by_service_principal="advisory-phase1-source-observer",
        dataset_specs=(
            ObservedDatasetSpec(
                dataset_name="adj_factor",
                allowed_data_sources=("tushare",),
                source_roles=("CORPORATE_ACTION",),
                query_template_id="market_adj_factor_trade_date_v1",
                query_template_version="v1",
                eligible_quality_statuses=("ok",),
            ),
            ObservedDatasetSpec(
                dataset_name="daily_basic",
                allowed_data_sources=("tushare",),
                source_roles=("FEATURE_T",),
                query_template_id="market_daily_basic_trade_date_v1",
                query_template_version="v1",
                eligible_quality_statuses=("ok",),
            ),
            ObservedDatasetSpec(
                dataset_name="index_daily",
                allowed_data_sources=("tushare",),
                source_roles=("BENCHMARK",),
                query_template_id="market_index_daily_trade_date_v1",
                query_template_version="v1",
                eligible_quality_statuses=("ok",),
            ),
            ObservedDatasetSpec(
                dataset_name="stk_limit",
                allowed_data_sources=("tushare",),
                source_roles=("TRADABILITY",),
                query_template_id="market_stk_limit_trade_date_v1",
                query_template_version="v1",
                eligible_quality_statuses=("ok",),
            ),
            ObservedDatasetSpec(
                dataset_name="suspend_d",
                allowed_data_sources=("tushare",),
                source_roles=("TRADABILITY",),
                query_template_id="market_suspend_d_trade_date_v1",
                query_template_version="v1",
                eligible_quality_statuses=("empty_valid", "ok"),
                allow_empty_partition=True,
            ),
        ),
    )


def registered_source_observer_configs() -> dict[tuple[str, str], SourceObserverConfigBundle]:
    config = default_source_observer_config()
    return {(config.observer_config_id, config.observer_config_version): config}
