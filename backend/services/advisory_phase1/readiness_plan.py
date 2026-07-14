"""Phase 1E deterministic advisory readiness-plan contracts and compiler.

Phase 1E compiles research-only plans from immutable historical evidence.  It
does not run selection, package validation, inference, paper/simulation flows,
model training, or any form of trading execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
import logging
import math
from typing import Any, Iterable, Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.audit_service import AdvisoryPhase0AAuditService
from backend.services.advisory_phase0a.handoff import Phase0AHandoffNormalizer
from backend.services.advisory_phase0a.historical_research import (
    HISTORICAL_RESEARCH_DATA_SOURCE,
    HISTORICAL_RESEARCH_SCOPE,
    HistoricalResearchBatch,
    HistoricalResearchBatchReceipt,
    HistoricalResearchProgramContext,
    HistoricalResearchProgramRun,
    HistoricalResearchRunStatus,
    HistoricalSelectionEvidence,
    _batch_receipt_payload,
    _program_payload_hash,
)
from backend.services.advisory_phase0a.models import (
    AuditDateRange,
    AuditReceipt,
    AuditRequest,
    AuditTarget,
    ExpectedAlphaMode,
    FormalOOSStatus,
    HandoffAdmissionScope,
    HandoffReadiness,
    HandoffReadinessReport,
    Phase0APolicyRegistry,
    Phase1HandoffBundle,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase0a.resolvers import AuditReaders
from backend.services.advisory_phase1.capture_foundation import CapturePlan
from backend.services.advisory_phase1.source_capacity import (
    CAPACITY_LOGICAL_ROLES,
    CapacityPlanningReceipt,
    CapacityPlanningRequest,
    CapacityStatus,
)
from backend.services.advisory_phase1.source_ledger import SourceAvailabilityEvent
from backend.services.advisory_phase1.source_resolution import (
    FixtureSourceRevisionResolver,
    ResearchReadiness,
    SourceRequirement,
    SourceRequirementSet,
    SourceResolutionResult,
    build_source_requirement_common_pit_identity_hash,
)
from backend.services.advisory_phase1.source_revision import AvailabilityRequirement, SourceRevisionKind


PHASE1E_PLAN_SCHEMA_VERSION = "advisory_phase1e_execution_plan_v1"
PHASE1E_BATCH_RECEIPT_SCHEMA_VERSION = "advisory_phase1e_plan_batch_receipt_v1"
PHASE1E_TEMPLATE_SCHEMA_VERSION = "advisory_phase1e_request_template_v1"
PHASE1E_SOURCE_RESOLUTION_OPERATION_SCHEMA_VERSION = "advisory_phase1e_source_resolution_operation_v1"
PHASE1E_WORKLOAD_SCHEMA_VERSION = "advisory_phase1e_workload_projection_v1"
PHASE1E_OUTPUT_SLOT_SCHEMA_VERSION = "advisory_phase1e_output_slot_v1"

LOGGER = logging.getLogger("aistock.advisory.phase1e")

REASON_HISTORICAL_RECEIPT_MISSING = "ADVISORY_PHASE1E_HISTORICAL_RECEIPT_MISSING"
REASON_HISTORICAL_RECEIPT_CONFLICT = "ADVISORY_PHASE1E_HISTORICAL_RECEIPT_CONFLICT"
REASON_PROGRAM_RUN_NOT_COMPLETE = "ADVISORY_PHASE1E_PROGRAM_RUN_NOT_COMPLETE"
REASON_HISTORICAL_DATE_REQUIRED = "ADVISORY_PHASE1E_HISTORICAL_DATE_REQUIRED"
REASON_DATED_BINDING_MISSING = "ADVISORY_PHASE1E_DATED_BINDING_MISSING"
REASON_BINDING_IDENTITY_MISMATCH = "ADVISORY_PHASE1E_BINDING_IDENTITY_MISMATCH"
REASON_PACKAGE_TYPE_UNSUPPORTED = "ADVISORY_PHASE1E_PACKAGE_TYPE_UNSUPPORTED"
REASON_PACKAGE_LINEAGE_HASH_MISMATCH = "ADVISORY_PHASE1E_PACKAGE_LINEAGE_HASH_MISMATCH"
REASON_FORBIDDEN_SHARED_RUNTIME_DEPENDENCY = "ADVISORY_PHASE1E_FORBIDDEN_SHARED_RUNTIME_DEPENDENCY"
REASON_TARGET_HAS_NO_ADMISSION_SCOPE = "ADVISORY_PHASE1E_TARGET_HAS_NO_ADMISSION_SCOPE"
REASON_FORMAL_SCOPE_NOT_HISTORICAL_INPUT = "ADVISORY_PHASE1E_FORMAL_SCOPE_NOT_HISTORICAL_INPUT"
REASON_AUDIT_HANDOFF_MISMATCH = "ADVISORY_PHASE1E_AUDIT_HANDOFF_MISMATCH"
REASON_AUDIT_ARTIFACT_CONFLICT = "ADVISORY_PHASE1E_AUDIT_ARTIFACT_CONFLICT"
REASON_POLICY_REGISTRY_HASH_MISMATCH = "ADVISORY_PHASE1E_POLICY_REGISTRY_HASH_MISMATCH"
REASON_SOURCE_RESOLUTION_BLOCKED = "ADVISORY_PHASE1E_SOURCE_RESOLUTION_BLOCKED"
REASON_SOURCE_RESOLUTION_CONFLICT = "ADVISORY_PHASE1E_SOURCE_RESOLUTION_CONFLICT"
REASON_CAPACITY_MEASUREMENT_PARTIAL = "ADVISORY_PHASE1E_CAPACITY_MEASUREMENT_PARTIAL"
REASON_CAPACITY_INSUFFICIENT = "ADVISORY_PHASE1E_CAPACITY_INSUFFICIENT"
REASON_CAPACITY_REFERENCE_MISMATCH = "ADVISORY_PHASE1E_CAPACITY_REFERENCE_MISMATCH"
REASON_CAPACITY_WORKLOAD_NOT_COVERED = "ADVISORY_PHASE1E_CAPACITY_WORKLOAD_NOT_COVERED"
REASON_REQUEST_TEMPLATE_INCOMPLETE = "ADVISORY_PHASE1E_REQUEST_TEMPLATE_INCOMPLETE"
REASON_PLAN_ARTIFACT_CONFLICT = "ADVISORY_PHASE1E_PLAN_ARTIFACT_CONFLICT"
REASON_UNEXPECTED_ERROR = "ADVISORY_PHASE1E_UNEXPECTED_ERROR"


class Phase1EError(RuntimeError):
    """A stable, diagnosable Phase 1E compile failure."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = context or {}


class EvidenceOrigin(str, Enum):
    MANUAL_HISTORICAL_RESEARCH = "MANUAL_HISTORICAL_RESEARCH"


class PlanUnitKind(str, Enum):
    ADMISSION_SCOPE = "ADMISSION_SCOPE"
    TARGET_DIAGNOSTIC = "TARGET_DIAGNOSTIC"


class PlannedOperationType(str, Enum):
    SOURCE_RESOLUTION = "SOURCE_RESOLUTION"
    OBSERVATION_CAPTURE = "OBSERVATION_CAPTURE"
    LABEL_CAPTURE = "LABEL_CAPTURE"
    DATASET_BUILD = "DATASET_BUILD"
    DURABLE_STORE_PUBLISH = "DURABLE_STORE_PUBLISH"


class OperationDisposition(str, Enum):
    COMPLETE_REQUEST = "COMPLETE_REQUEST"
    SEMANTIC_TEMPLATE = "SEMANTIC_TEMPLATE"
    DEFERRED = "DEFERRED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


def _sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field_name} must be a lowercase SHA256 hex digest")
    return normalized


def _text(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return value.astimezone(UTC)


def _sorted_strings(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in values if str(value).strip()}))


def _model_payload(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="python")
    return value


def _normalized_dicts(values: Iterable[dict[str, Any]], *, field_name: str) -> tuple[dict[str, Any], ...]:
    """Canonicalize and de-duplicate unordered metadata rows before persistence."""

    by_hash: dict[str, dict[str, Any]] = {}
    for value in values:
        if not isinstance(value, dict):
            raise ValueError(f"{field_name} entries must be objects")
        normalized = canonicalize(value)
        if not isinstance(normalized, dict):
            raise ValueError(f"{field_name} entries must canonicalize to objects")
        by_hash[canonical_json_sha256(normalized)] = normalized
    return tuple(by_hash[key] for key in sorted(by_hash))


def _output_slot(
    *,
    slot: str,
    source_type: str,
    producer_operation: str,
    hash_validation: str,
) -> dict[str, str]:
    """Describe a future output without inventing its value or final request hash."""

    return {
        "slot": _text(slot, field_name="output slot"),
        "source_type": _text(source_type, field_name="output slot source_type"),
        "slot_schema_version": PHASE1E_OUTPUT_SLOT_SCHEMA_VERSION,
        "producer_operation": _text(producer_operation, field_name="output slot producer_operation"),
        "hash_validation": _text(hash_validation, field_name="output slot hash_validation"),
    }


def _normalized_output_slots(values: Iterable[dict[str, Any]], *, field_name: str) -> tuple[dict[str, Any], ...]:
    """Validate one explicit metadata contract for every unresolved output slot."""

    by_slot: dict[str, dict[str, Any]] = {}
    required_fields = {
        "slot",
        "source_type",
        "slot_schema_version",
        "producer_operation",
        "hash_validation",
    }
    for value in values:
        if not isinstance(value, dict):
            raise ValueError(f"{field_name} entries must be objects")
        normalized = canonicalize(value)
        if not isinstance(normalized, dict):
            raise ValueError(f"{field_name} entries must canonicalize to objects")
        missing = required_fields - set(normalized)
        if missing:
            raise ValueError(f"{field_name} entry is missing required metadata: {sorted(missing)}")
        if normalized["slot_schema_version"] != PHASE1E_OUTPUT_SLOT_SCHEMA_VERSION:
            raise ValueError(f"{field_name} entry has an unsupported slot schema")
        for name in required_fields:
            if not isinstance(normalized[name], str) or not normalized[name].strip():
                raise ValueError(f"{field_name} entry field {name} must be non-empty text")
        if normalized["hash_validation"].strip().lower() in {"none", "null", "placeholder"}:
            raise ValueError(f"{field_name} entry must declare a concrete hash validation")
        slot = normalized["slot"]
        existing = by_slot.get(slot)
        if existing is not None and existing != normalized:
            raise ValueError(f"{field_name} has conflicting declarations for output slot {slot}")
        by_slot[slot] = normalized
    return tuple(by_slot[slot] for slot in sorted(by_slot))


class Phase1EProgramDateRequest(BaseModel):
    """One explicit Program + completed historical trading-date input."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    evidence_origin: EvidenceOrigin = EvidenceOrigin.MANUAL_HISTORICAL_RESEARCH
    expected_package_id: str | None = Field(default=None, max_length=160)
    expected_manifest_sha256: str | None = Field(default=None, min_length=64, max_length=64)
    expected_alpha_mode: str | None = Field(default=None, pattern="^(single_alpha|multi_alpha)$")
    expected_style_family: str | None = Field(default=None, max_length=160)
    historical_batch_receipt_ref: str = Field(min_length=1, max_length=160)
    label_as_of_ts: datetime
    program_date_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("program_id", "historical_batch_receipt_ref")
    @classmethod
    def _required_text(cls, value: str) -> str:
        return _text(value, field_name="program-date request text")

    @field_validator("expected_manifest_sha256", "program_date_request_hash")
    @classmethod
    def _optional_hash(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("label_as_of_ts")
    @classmethod
    def _label_clock(cls, value: datetime) -> datetime:
        return _aware(value, field_name="label_as_of_ts")

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "advisory_phase1e_program_date_request_v1",
            "program_id": self.program_id,
            "decision_trade_date": self.decision_trade_date,
            "evidence_origin": self.evidence_origin.value,
            "expected_package_id": self.expected_package_id,
            "expected_manifest_sha256": self.expected_manifest_sha256,
            "expected_alpha_mode": self.expected_alpha_mode,
            "expected_style_family": self.expected_style_family,
            "historical_batch_receipt_ref": self.historical_batch_receipt_ref,
            "label_as_of_ts": self.label_as_of_ts,
        }

    @model_validator(mode="after")
    def _derive_hash(self) -> "Phase1EProgramDateRequest":
        digest = canonical_json_sha256(self.canonical_payload())
        if self.program_date_request_hash is not None and self.program_date_request_hash != digest:
            raise ValueError("program_date_request_hash does not match canonical request")
        object.__setattr__(self, "program_date_request_hash", digest)
        return self


class Phase1ERevalidationBatchRequest(BaseModel):
    """Explicit, batch-level compiler references; scope plans remain independent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    program_dates: tuple[Phase1EProgramDateRequest, ...] = Field(min_length=1)
    phase0a_policy_hash: str = Field(min_length=64, max_length=64)
    source_requirement_registry_hash: str = Field(min_length=64, max_length=64)
    query_registry_hash: str = Field(min_length=64, max_length=64)
    calendar_hash: str = Field(min_length=64, max_length=64)
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    dataset_schema_fingerprint: str = Field(min_length=1, max_length=160)
    partition_policy_hash: str = Field(min_length=64, max_length=64)
    store_backend_config_hash: str = Field(min_length=64, max_length=64)
    capacity_request_ref: str = Field(min_length=1, max_length=400)
    capacity_receipt_ref: str = Field(min_length=1, max_length=400)
    compiler_version: str = Field(min_length=1, max_length=160)
    serializer_version: str = Field(min_length=1, max_length=160)
    compiler_source_hash: str = Field(min_length=64, max_length=64)
    artifact_store_policy_hash: str = Field(min_length=64, max_length=64)
    invocation_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "phase0a_policy_hash",
        "source_requirement_registry_hash",
        "query_registry_hash",
        "calendar_hash",
        "label_policy_bundle_hash",
        "partition_policy_hash",
        "store_backend_config_hash",
        "compiler_source_hash",
        "artifact_store_policy_hash",
        "invocation_request_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "advisory_phase1e_revalidation_batch_request_v1",
            "program_date_request_hashes": [
                item.program_date_request_hash
                for item in sorted(self.program_dates, key=lambda value: (value.program_id, value.decision_trade_date))
            ],
            "phase0a_policy_hash": self.phase0a_policy_hash,
            "source_requirement_registry_hash": self.source_requirement_registry_hash,
            "query_registry_hash": self.query_registry_hash,
            "calendar_hash": self.calendar_hash,
            "label_policy_bundle_hash": self.label_policy_bundle_hash,
            "dataset_schema_fingerprint": self.dataset_schema_fingerprint,
            "partition_policy_hash": self.partition_policy_hash,
            "store_backend_config_hash": self.store_backend_config_hash,
            "capacity_request_ref": self.capacity_request_ref,
            "capacity_receipt_ref": self.capacity_receipt_ref,
            "compiler_version": self.compiler_version,
            "serializer_version": self.serializer_version,
            "compiler_source_hash": self.compiler_source_hash,
            "artifact_store_policy_hash": self.artifact_store_policy_hash,
        }

    @model_validator(mode="after")
    def _unique_and_hashed(self) -> "Phase1ERevalidationBatchRequest":
        keys = [(item.program_id, item.decision_trade_date) for item in self.program_dates]
        if len(keys) != len(set(keys)):
            raise ValueError("program_dates must be unique by (program_id, decision_trade_date)")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.invocation_request_hash is not None and self.invocation_request_hash != digest:
            raise ValueError("invocation_request_hash does not match canonical batch request")
        object.__setattr__(self, "program_dates", tuple(sorted(self.program_dates, key=lambda value: (value.program_id, value.decision_trade_date))))
        object.__setattr__(self, "invocation_request_hash", digest)
        return self


class Phase1EEvidenceBinding(BaseModel):
    """Immutable program/date evidence readback consumed by exactly one plan unit."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    historical_batch_id: str = Field(min_length=1, max_length=160)
    historical_batch_key: str = Field(min_length=1, max_length=160)
    historical_receipt_hash: str = Field(min_length=64, max_length=64)
    historical_program_run_id: str = Field(min_length=1, max_length=160)
    program_payload_sha256: str = Field(min_length=64, max_length=64)
    binding_version_id: str = Field(min_length=1, max_length=160)
    binding_payload_hash: str = Field(min_length=64, max_length=64)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: str = Field(pattern="^(single_alpha|multi_alpha)$")
    manifest_alpha_component_ids: tuple[str, ...] = ()
    resolved_style_family: str = Field(min_length=1, max_length=160)
    style_assignment_policy_hash: str = Field(min_length=64, max_length=64)
    selection_evidence_id: str = Field(min_length=1, max_length=160)
    selection_evidence_hash: str = Field(min_length=64, max_length=64)
    selection_artifact_id: str = Field(min_length=1, max_length=160)
    selection_artifact_payload_hash: str = Field(min_length=64, max_length=64)
    source_watermark_hash: str = Field(min_length=64, max_length=64)
    package_asset_closure_hash: str | None = Field(default=None, min_length=64, max_length=64)
    package_lineage_hash: str | None = Field(default=None, min_length=64, max_length=64)
    phase0a_audit_id: str = Field(min_length=1, max_length=160)
    phase0a_audit_manifest_hash: str = Field(min_length=64, max_length=64)
    phase0a_request_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_report_hash: str = Field(min_length=64, max_length=64)
    phase1_handoff_bundle_hash: str | None = Field(default=None, min_length=64, max_length=64)
    admission_scope_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    admission_scope_id: str | None = Field(default=None, min_length=1, max_length=160)
    admission_scope_hash: str | None = Field(default=None, min_length=64, max_length=64)
    target_scope_hash: str = Field(min_length=64, max_length=64)
    oos_interval_hash: str | None = Field(default=None, min_length=64, max_length=64)
    evidence_scope: str | None = None
    formal_oos_status: str | None = None
    signal_evidence_level: str | None = None
    stable_signal_semantics_hash: str | None = Field(default=None, min_length=64, max_length=64)
    decision_clock_hash: str | None = Field(default=None, min_length=64, max_length=64)
    evidence_binding_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "historical_receipt_hash",
        "program_payload_sha256",
        "binding_payload_hash",
        "manifest_sha256",
        "style_assignment_policy_hash",
        "selection_evidence_hash",
        "selection_artifact_payload_hash",
        "source_watermark_hash",
        "package_asset_closure_hash",
        "package_lineage_hash",
        "phase0a_audit_manifest_hash",
        "phase0a_request_hash",
        "handoff_readiness_report_hash",
        "phase1_handoff_bundle_hash",
        "admission_scope_set_hash",
        "admission_scope_hash",
        "target_scope_hash",
        "oos_interval_hash",
        "stable_signal_semantics_hash",
        "decision_clock_hash",
        "evidence_binding_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="python", exclude={"evidence_binding_hash"})

    @model_validator(mode="after")
    def _validate_scope_shape(self) -> "Phase1EEvidenceBinding":
        component_ids = tuple(sorted(str(value).strip() for value in self.manifest_alpha_component_ids))
        if any(not value for value in component_ids) or len(component_ids) != len(set(component_ids)):
            raise ValueError("manifest alpha component identities must be non-empty and unique")
        if self.alpha_mode == "single_alpha" and component_ids:
            raise ValueError("single Alpha evidence binding cannot carry parent-leg identities")
        object.__setattr__(self, "manifest_alpha_component_ids", component_ids)
        scope_values = (self.admission_scope_id, self.admission_scope_hash)
        if any(value is not None for value in scope_values) and not all(value is not None for value in scope_values):
            raise ValueError("admission scope id/hash must be both present or both null")
        if self.admission_scope_id is None and any(
            value is not None for value in (self.phase1_handoff_bundle_hash, self.admission_scope_set_hash, self.oos_interval_hash)
        ):
            raise ValueError("target diagnostic cannot fabricate admission/bundle identity")
        if self.admission_scope_id is not None and self.oos_interval_hash is None:
            raise ValueError("admission scope requires its immutable interval identity")
        if (self.phase1_handoff_bundle_hash is None) != (self.admission_scope_set_hash is None):
            raise ValueError("handoff bundle and admission-scope set identities must be both present or both null")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.evidence_binding_hash is not None and self.evidence_binding_hash != digest:
            raise ValueError("evidence_binding_hash does not match evidence binding")
        object.__setattr__(self, "evidence_binding_hash", digest)
        return self


class Phase1EPlannedOperation(BaseModel):
    """A complete typed request or a truthful template with no fabricated slots."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_type: PlannedOperationType
    operation_disposition: OperationDisposition
    contract_schema_version: str = Field(min_length=1, max_length=160)
    complete_request_payload: dict[str, Any] | None = None
    complete_request_hash: str | None = Field(default=None, min_length=64, max_length=64)
    request_template_payload: dict[str, Any] | None = None
    request_template_hash: str | None = Field(default=None, min_length=64, max_length=64)
    expected_final_request_hash: str | None = Field(default=None, min_length=64, max_length=64)
    required_output_slots: tuple[dict[str, Any], ...] = ()
    resolved_input_refs: tuple[dict[str, Any], ...] = ()
    unresolved_input_refs: tuple[dict[str, Any], ...] = ()
    resource_budget_ref: str | None = Field(default=None, max_length=400)

    @field_validator("complete_request_hash", "request_template_hash", "expected_final_request_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "operation_type": self.operation_type.value,
            "operation_disposition": self.operation_disposition.value,
            "contract_schema_version": self.contract_schema_version,
            "complete_request_payload": canonicalize(self.complete_request_payload) if self.complete_request_payload is not None else None,
            "complete_request_hash": self.complete_request_hash,
            "request_template_payload": canonicalize(self.request_template_payload) if self.request_template_payload is not None else None,
            "request_template_hash": self.request_template_hash,
            "expected_final_request_hash": self.expected_final_request_hash,
            "required_output_slots": sorted((canonicalize(item) for item in self.required_output_slots), key=canonical_json_sha256),
            "resolved_input_refs": sorted((canonicalize(item) for item in self.resolved_input_refs), key=canonical_json_sha256),
            "unresolved_input_refs": sorted((canonicalize(item) for item in self.unresolved_input_refs), key=canonical_json_sha256),
            "resource_budget_ref": self.resource_budget_ref,
        }

    @model_validator(mode="after")
    def _payload_shape(self) -> "Phase1EPlannedOperation":
        if self.complete_request_payload is not None:
            object.__setattr__(self, "complete_request_payload", canonicalize(self.complete_request_payload))
        if self.request_template_payload is not None:
            object.__setattr__(self, "request_template_payload", canonicalize(self.request_template_payload))
        object.__setattr__(
            self,
            "required_output_slots",
            _normalized_output_slots(self.required_output_slots, field_name="required_output_slots"),
        )
        object.__setattr__(
            self,
            "resolved_input_refs",
            _normalized_dicts(self.resolved_input_refs, field_name="resolved_input_refs"),
        )
        object.__setattr__(
            self,
            "unresolved_input_refs",
            _normalized_output_slots(self.unresolved_input_refs, field_name="unresolved_input_refs"),
        )
        has_complete = self.complete_request_payload is not None or self.complete_request_hash is not None
        has_template = self.request_template_payload is not None or self.request_template_hash is not None
        if self.operation_disposition is OperationDisposition.COMPLETE_REQUEST:
            if not (self.complete_request_payload is not None and self.complete_request_hash is not None) or has_template:
                raise ValueError("complete request requires only a complete payload/hash")
            if canonical_json_sha256(self.complete_request_payload) != self.complete_request_hash:
                raise ValueError("complete_request_hash does not match complete payload")
            if self.required_output_slots or self.unresolved_input_refs:
                raise ValueError("complete request cannot retain unresolved output slots")
        elif self.operation_disposition in {OperationDisposition.SEMANTIC_TEMPLATE, OperationDisposition.DEFERRED}:
            if not (self.request_template_payload is not None and self.request_template_hash is not None) or has_complete:
                raise ValueError("template/deferred operation requires only a template payload/hash")
            if canonical_json_sha256(self.request_template_payload) != self.request_template_hash:
                raise ValueError("request_template_hash does not match template payload")
            if not self.required_output_slots and self.operation_disposition is OperationDisposition.SEMANTIC_TEMPLATE:
                raise ValueError("semantic template must enumerate output slots")
        elif self.operation_disposition is OperationDisposition.NOT_APPLICABLE:
            if has_complete or has_template or self.required_output_slots or self.unresolved_input_refs:
                raise ValueError("not-applicable operation cannot carry request/template data")
        return self


class Phase1EWorkloadProjection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = PHASE1E_WORKLOAD_SCHEMA_VERSION
    scope_plan_request_hash: str = Field(min_length=64, max_length=64)
    style_family: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    program_count: int = Field(default=1, ge=1)
    trading_day_count: int = Field(default=1, ge=1)
    candidate_depth: int = Field(ge=0)
    horizons: tuple[int, ...] = Field(min_length=1)
    projection_count: int = Field(ge=1)
    stage_projection_factor: int = Field(ge=1)
    universe_size_p50: int = Field(ge=0)
    universe_size_p95: int = Field(ge=0)
    universe_size_max: int = Field(ge=0)
    source_role_counts: dict[str, int] = Field(default_factory=dict)
    role_rows_p50: dict[str, int]
    role_rows_p95: dict[str, int]
    role_rows_max: dict[str, int]
    role_logical_bytes_p50: dict[str, int | None] = Field(default_factory=lambda: {role: None for role in CAPACITY_LOGICAL_ROLES})
    role_logical_bytes_p95: dict[str, int | None] = Field(default_factory=lambda: {role: None for role in CAPACITY_LOGICAL_ROLES})
    role_logical_bytes_max: dict[str, int | None] = Field(default_factory=lambda: {role: None for role in CAPACITY_LOGICAL_ROLES})
    role_parquet_bytes_p50: dict[str, int | None] = Field(default_factory=lambda: {role: None for role in CAPACITY_LOGICAL_ROLES})
    role_parquet_bytes_p95: dict[str, int | None] = Field(default_factory=lambda: {role: None for role in CAPACITY_LOGICAL_ROLES})
    role_parquet_bytes_max: dict[str, int | None] = Field(default_factory=lambda: {role: None for role in CAPACITY_LOGICAL_ROLES})
    workload_projection_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("scope_plan_request_hash", "workload_projection_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="python", exclude={"workload_projection_hash"})

    @model_validator(mode="after")
    def _validate_and_hash(self) -> "Phase1EWorkloadProjection":
        if self.schema_version != PHASE1E_WORKLOAD_SCHEMA_VERSION:
            raise ValueError("unsupported Phase 1E workload projection schema")
        if any(horizon <= 0 for horizon in self.horizons):
            raise ValueError("workload horizons must be positive")
        if not (self.universe_size_p50 <= self.universe_size_p95 <= self.universe_size_max):
            raise ValueError("workload universe tiers must be monotonic")
        if any(value < 0 for value in self.source_role_counts.values()):
            raise ValueError("workload source role counts cannot be negative")
        for mapping in (self.role_rows_p50, self.role_rows_p95, self.role_rows_max):
            if set(mapping) != set(CAPACITY_LOGICAL_ROLES) or any(value < 0 for value in mapping.values()):
                raise ValueError("workload must provide non-negative rows for every capacity role")
        for role in CAPACITY_LOGICAL_ROLES:
            if not (self.role_rows_p50[role] <= self.role_rows_p95[role] <= self.role_rows_max[role]):
                raise ValueError("workload role rows must be monotonic")
        for p50, p95, maximum in (
            (self.role_logical_bytes_p50, self.role_logical_bytes_p95, self.role_logical_bytes_max),
            (self.role_parquet_bytes_p50, self.role_parquet_bytes_p95, self.role_parquet_bytes_max),
        ):
            if any(set(mapping) != set(CAPACITY_LOGICAL_ROLES) for mapping in (p50, p95, maximum)):
                raise ValueError("workload byte projections must enumerate every capacity role")
            for role in CAPACITY_LOGICAL_ROLES:
                values = (p50[role], p95[role], maximum[role])
                if any(value is not None and value < 0 for value in values):
                    raise ValueError("workload byte projections cannot be negative")
                if any(value is None for value in values):
                    if not all(value is None for value in values):
                        raise ValueError("workload byte projections must be fully known or fully missing per role")
                elif not (p50[role] <= p95[role] <= maximum[role]):
                    raise ValueError("workload byte projections must be monotonic")
        object.__setattr__(self, "horizons", tuple(sorted(set(self.horizons))))
        object.__setattr__(self, "source_role_counts", dict(sorted(self.source_role_counts.items())))
        for field_name in (
            "role_rows_p50",
            "role_rows_p95",
            "role_rows_max",
            "role_logical_bytes_p50",
            "role_logical_bytes_p95",
            "role_logical_bytes_max",
            "role_parquet_bytes_p50",
            "role_parquet_bytes_p95",
            "role_parquet_bytes_max",
        ):
            mapping = getattr(self, field_name)
            object.__setattr__(self, field_name, {role: mapping[role] for role in CAPACITY_LOGICAL_ROLES})
        digest = canonical_json_sha256(self.canonical_payload())
        if self.workload_projection_hash is not None and self.workload_projection_hash != digest:
            raise ValueError("workload_projection_hash does not match projection")
        object.__setattr__(self, "workload_projection_hash", digest)
        return self


class Phase1EExecutionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = PHASE1E_PLAN_SCHEMA_VERSION
    evidence_request_hash: str = Field(min_length=64, max_length=64)
    scope_plan_request_hash: str = Field(min_length=64, max_length=64)
    compiler_version: str = Field(min_length=1, max_length=160)
    serializer_version: str = Field(min_length=1, max_length=160)
    compiler_source_hash: str = Field(min_length=64, max_length=64)
    plan_unit_kind: PlanUnitKind
    scope_key: dict[str, Any] | None = None
    target_key: dict[str, Any] | None = None
    evidence_binding: Phase1EEvidenceBinding
    handoff_readiness: HandoffReadiness
    source_readiness: ResearchReadiness | None = None
    capacity_status: CapacityStatus | None = None
    reason_codes: tuple[str, ...] = ()
    missing_evidence: tuple[dict[str, Any], ...] = ()
    planned_operations: tuple[Phase1EPlannedOperation, ...]
    workload_projection: Phase1EWorkloadProjection | None = None
    resource_budget_by_role: dict[str, Any] | None = None
    memory_budget: dict[str, Any] | None = None
    temporary_store_budget: dict[str, Any] | None = None
    durable_store_budget: dict[str, Any] | None = None
    missing_capacity_measurements: tuple[str, ...] = ()
    capacity_request_hash: str | None = Field(default=None, min_length=64, max_length=64)
    capacity_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)
    capacity_workload_covered: bool | None = None
    resource_values_frozen: bool | None = None
    research_only: bool = True
    execution_prohibited: bool = True
    plan_hash: str | None = Field(default=None, min_length=64, max_length=64)
    plan_id: str | None = Field(default=None, min_length=1, max_length=160)

    @field_validator(
        "evidence_request_hash",
        "scope_plan_request_hash",
        "compiler_source_hash",
        "capacity_request_hash",
        "capacity_receipt_hash",
        "plan_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "evidence_request_hash": self.evidence_request_hash,
            "scope_plan_request_hash": self.scope_plan_request_hash,
            "compiler_version": self.compiler_version,
            "serializer_version": self.serializer_version,
            "compiler_source_hash": self.compiler_source_hash,
            "plan_unit_kind": self.plan_unit_kind.value,
            "scope_key": canonicalize(self.scope_key) if self.scope_key is not None else None,
            "target_key": canonicalize(self.target_key) if self.target_key is not None else None,
            "evidence_binding": self.evidence_binding.canonical_payload(),
            "handoff_readiness": self.handoff_readiness.value,
            "source_readiness": self.source_readiness.value if self.source_readiness is not None else None,
            "capacity_status": self.capacity_status.value if self.capacity_status is not None else None,
            "reason_codes": list(_sorted_strings(self.reason_codes)),
            "missing_evidence": sorted((canonicalize(item) for item in self.missing_evidence), key=canonical_json_sha256),
            "planned_operations": [
                item.canonical_payload() for item in sorted(self.planned_operations, key=lambda item: item.operation_type.value)
            ],
            "workload_projection": self.workload_projection.canonical_payload() if self.workload_projection is not None else None,
            "resource_budget_by_role": canonicalize(self.resource_budget_by_role) if self.resource_budget_by_role is not None else None,
            "memory_budget": canonicalize(self.memory_budget) if self.memory_budget is not None else None,
            "temporary_store_budget": canonicalize(self.temporary_store_budget) if self.temporary_store_budget is not None else None,
            "durable_store_budget": canonicalize(self.durable_store_budget) if self.durable_store_budget is not None else None,
            "missing_capacity_measurements": list(_sorted_strings(self.missing_capacity_measurements)),
            "capacity_request_hash": self.capacity_request_hash,
            "capacity_receipt_hash": self.capacity_receipt_hash,
            "capacity_workload_covered": self.capacity_workload_covered,
            "resource_values_frozen": self.resource_values_frozen,
            "research_only": self.research_only,
            "execution_prohibited": self.execution_prohibited,
        }

    @model_validator(mode="after")
    def _plan_invariants(self) -> "Phase1EExecutionPlan":
        if self.scope_key is not None:
            object.__setattr__(self, "scope_key", canonicalize(self.scope_key))
        if self.target_key is not None:
            object.__setattr__(self, "target_key", canonicalize(self.target_key))
        object.__setattr__(self, "reason_codes", _sorted_strings(self.reason_codes))
        object.__setattr__(self, "missing_evidence", _normalized_dicts(self.missing_evidence, field_name="missing_evidence"))
        object.__setattr__(
            self,
            "planned_operations",
            tuple(sorted(self.planned_operations, key=lambda item: item.operation_type.value)),
        )
        object.__setattr__(self, "missing_capacity_measurements", _sorted_strings(self.missing_capacity_measurements))
        for field_name in (
            "resource_budget_by_role",
            "memory_budget",
            "temporary_store_budget",
            "durable_store_budget",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, canonicalize(value))
        if self.schema_version != PHASE1E_PLAN_SCHEMA_VERSION or not self.research_only or not self.execution_prohibited:
            raise ValueError("Phase 1E plans are research-only and execution-prohibited")
        operation_types = [item.operation_type for item in self.planned_operations]
        if len(operation_types) != len(set(operation_types)):
            raise ValueError("plan operations must have unique types")
        if self.plan_unit_kind is PlanUnitKind.TARGET_DIAGNOSTIC:
            if self.scope_key is not None or self.target_key is None:
                raise ValueError("target diagnostic requires only a target key")
            if any(value is not None for value in (self.source_readiness, self.capacity_status, self.workload_projection, self.capacity_workload_covered, self.resource_values_frozen)):
                raise ValueError("target diagnostic cannot carry source/capacity workload values")
            if any(item.operation_disposition is not OperationDisposition.NOT_APPLICABLE for item in self.planned_operations):
                raise ValueError("target diagnostic may only contain not-applicable operations")
        else:
            if self.scope_key is None or self.target_key is not None or self.evidence_binding.admission_scope_id is None:
                raise ValueError("admission plan requires a real admission scope")
            if self.capacity_status is None or self.capacity_request_hash is None or self.capacity_receipt_hash is None:
                raise ValueError("admission plan requires exact capacity evidence")
            if self.workload_projection is None or self.capacity_workload_covered is None or self.resource_values_frozen is None:
                raise ValueError("admission plan requires workload coverage state")
            if self.capacity_status is CapacityStatus.MEASURED and self.capacity_workload_covered and not self.resource_values_frozen:
                raise ValueError("covered measured capacity must freeze resource values")
            if self.capacity_status is not CapacityStatus.MEASURED and self.resource_values_frozen:
                raise ValueError("partial/insufficient capacity cannot freeze resource values")
        digest = canonical_json_sha256(self.canonical_payload())
        expected_id = f"p1ep_{digest[:20]}"
        if self.plan_hash is not None and self.plan_hash != digest:
            raise ValueError("plan_hash does not match canonical plan")
        if self.plan_id is not None and self.plan_id != expected_id:
            raise ValueError("plan_id does not match plan hash")
        object.__setattr__(self, "plan_hash", digest)
        object.__setattr__(self, "plan_id", expected_id)
        return self


class Phase1EFailedInputScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    program_date_request_hash: str = Field(min_length=64, max_length=64)
    reason_code: str = Field(min_length=1, max_length=160)
    context: dict[str, Any] = Field(default_factory=dict)

    @field_validator("program_date_request_hash")
    @classmethod
    def _program_date_hash(cls, value: str) -> str:
        return _sha256(value, field_name="program_date_request_hash")

    @model_validator(mode="after")
    def _canonical_context(self) -> "Phase1EFailedInputScope":
        object.__setattr__(self, "context", canonicalize(self.context))
        return self


class Phase1EPlanBatchReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = PHASE1E_BATCH_RECEIPT_SCHEMA_VERSION
    batch_request_hash: str = Field(min_length=64, max_length=64)
    sorted_scope_plan_request_hashes: tuple[str, ...] = ()
    sorted_scope_plan_hashes: tuple[str, ...] = ()
    counts_by_plan_unit_kind: dict[str, int]
    all_scope_workloads_covered: bool
    counts_by_handoff_readiness: dict[str, int]
    counts_by_source_readiness: dict[str, int]
    counts_by_capacity_status: dict[str, int]
    failed_input_scopes: tuple[Phase1EFailedInputScope, ...] = ()
    batch_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("batch_request_hash", "batch_receipt_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("sorted_scope_plan_request_hashes", "sorted_scope_plan_hashes")
    @classmethod
    def _scope_hashes(cls, values: tuple[str, ...], info) -> tuple[str, ...]:  # type: ignore[no-untyped-def]
        return tuple(_sha256(value, field_name=info.field_name) for value in values)

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "batch_request_hash": self.batch_request_hash,
            "sorted_scope_plan_request_hashes": sorted(self.sorted_scope_plan_request_hashes),
            "sorted_scope_plan_hashes": sorted(self.sorted_scope_plan_hashes),
            "counts_by_plan_unit_kind": canonicalize(self.counts_by_plan_unit_kind),
            "all_scope_workloads_covered": self.all_scope_workloads_covered,
            "counts_by_handoff_readiness": canonicalize(self.counts_by_handoff_readiness),
            "counts_by_source_readiness": canonicalize(self.counts_by_source_readiness),
            "counts_by_capacity_status": canonicalize(self.counts_by_capacity_status),
            "failed_input_scopes": [
                item.model_dump(mode="python")
                for item in sorted(self.failed_input_scopes, key=lambda item: (item.program_id, item.decision_trade_date, item.reason_code))
            ],
        }

    @model_validator(mode="after")
    def _derive_hash(self) -> "Phase1EPlanBatchReceipt":
        object.__setattr__(self, "sorted_scope_plan_request_hashes", tuple(sorted(set(self.sorted_scope_plan_request_hashes))))
        object.__setattr__(self, "sorted_scope_plan_hashes", tuple(sorted(set(self.sorted_scope_plan_hashes))))
        object.__setattr__(
            self,
            "failed_input_scopes",
            tuple(
                sorted(
                    self.failed_input_scopes,
                    key=lambda item: (item.program_id, item.decision_trade_date, item.reason_code, item.program_date_request_hash),
                )
            ),
        )
        digest = canonical_json_sha256(self.canonical_payload())
        if self.batch_receipt_hash is not None and self.batch_receipt_hash != digest:
            raise ValueError("batch_receipt_hash does not match batch receipt")
        object.__setattr__(self, "batch_receipt_hash", digest)
        return self


class SourceRequirementTemplate(BaseModel):
    """A registry-defined source requirement with all non-scope values explicit."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: str = Field(pattern="^(single_alpha|multi_alpha)$")
    alpha_component_id: str | None = Field(default=None, max_length=160)
    source_role: str = Field(min_length=1, max_length=80)
    dataset_name: str = Field(min_length=1, max_length=160)
    query_template_id: str = Field(min_length=1, max_length=160)
    query_template_version: str = Field(min_length=1, max_length=80)
    query_template_hash: str = Field(min_length=64, max_length=64)
    bound_parameters: dict[str, Any]
    partition_key: dict[str, Any] = Field(min_length=1)
    revision_kind: SourceRevisionKind
    availability_requirement: AvailabilityRequirement
    business_min_date: date
    business_max_date: date
    enforced_cutoff_predicate_hash: str = Field(min_length=64, max_length=64)
    consumer_scope_suffix: str = Field(min_length=1, max_length=160)

    @field_validator("manifest_sha256", "query_template_hash", "enforced_cutoff_predicate_hash")
    @classmethod
    def _hashes(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _range(self) -> "SourceRequirementTemplate":
        if self.business_min_date > self.business_max_date:
            raise ValueError("source template business range is invalid")
        if self.alpha_component_id is not None:
            object.__setattr__(self, "alpha_component_id", _text(self.alpha_component_id, field_name="alpha_component_id"))
        return self


class SourceRequirementRegistry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "advisory_phase1e_source_requirement_registry_v1"
    templates: tuple[SourceRequirementTemplate, ...] = Field(min_length=1)
    registry_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("registry_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="registry_hash") if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "templates": [
                item.model_dump(mode="python")
                for item in sorted(
                    self.templates,
                    key=lambda item: (
                        item.package_id,
                        item.manifest_sha256,
                        item.alpha_component_id or "",
                        item.consumer_scope_suffix,
                        item.source_role,
                    ),
                )
            ],
        }

    @model_validator(mode="after")
    def _derive_hash(self) -> "SourceRequirementRegistry":
        digest = canonical_json_sha256(self.canonical_payload())
        if self.registry_hash is not None and self.registry_hash != digest:
            raise ValueError("registry_hash does not match source requirement registry")
        object.__setattr__(self, "registry_hash", digest)
        return self


@dataclass(frozen=True)
class Phase1EAuditOutcome:
    request: AuditRequest
    receipt: AuditReceipt
    handoff_report: HandoffReadinessReport
    handoff_bundle: Phase1HandoffBundle | None


@dataclass(frozen=True)
class Phase1EProgramDateEvidence:
    """Immutable inputs supplied through an Advisory read-only projection."""

    historical_batch: HistoricalResearchBatch | None
    historical_receipt: HistoricalResearchBatchReceipt | None
    historical_program_run: HistoricalResearchProgramRun | None
    dated_binding: Any | None
    package: Any | None
    selection_evidence: Any | None
    selection_artifact: Any | None
    policy: Phase0APolicyRegistry
    audit_readers: AuditReaders
    postgres_now: datetime


class Phase1EInputProvider(Protocol):
    def resolve_program_date(
        self,
        *,
        request: Phase1EProgramDateRequest,
        batch_request: Phase1ERevalidationBatchRequest,
    ) -> Phase1EProgramDateEvidence: ...

    def list_source_events(self, *, requirements: SourceRequirementSet) -> Iterable[SourceAvailabilityEvent]: ...


class Phase1EArtifactStore(Protocol):
    def publish(self, *, kind: str, identity: str, payload: dict[str, Any], semantic_hash: str) -> dict[str, Any]: ...


class RegistrySourceRequirementCompiler:
    """Compile exact source requirements from a versioned registry, never defaults."""

    def __init__(self, registry: SourceRequirementRegistry) -> None:
        self._registry = registry

    @property
    def registry_hash(self) -> str:
        return str(self._registry.registry_hash)

    def compile(
        self,
        *,
        binding: Phase1EEvidenceBinding,
        scope: HandoffAdmissionScope,
        request: Phase1EProgramDateRequest,
        batch_request: Phase1ERevalidationBatchRequest,
        universe_policy_hash: str,
        requested_source_cutoff: datetime,
    ) -> SourceRequirementSet:
        matching = [
            item
            for item in self._registry.templates
            if item.package_id == binding.package_id
            and item.manifest_sha256 == binding.manifest_sha256
            and item.alpha_mode == binding.alpha_mode
        ]
        if not matching:
            raise Phase1EError(
                REASON_SOURCE_RESOLUTION_BLOCKED,
                "source requirement registry does not cover the frozen package identity",
                context={"package_id": binding.package_id, "manifest_sha256": binding.manifest_sha256},
            )
        component_ids = tuple(binding.manifest_alpha_component_ids)
        if binding.alpha_mode == "multi_alpha":
            component_templates = {item.alpha_component_id for item in matching if item.alpha_component_id is not None}
            unexpected_component_ids = component_templates - set(component_ids)
            missing_component_ids = set(component_ids) - component_templates
            if not component_ids or unexpected_component_ids or missing_component_ids or any(
                item.alpha_component_id is None for item in matching
            ):
                raise Phase1EError(
                    REASON_SOURCE_RESOLUTION_BLOCKED,
                    "native multi Alpha source registry must cover every persisted Alpha leg exactly",
                    context={
                        "package_id": binding.package_id,
                        "manifest_sha256": binding.manifest_sha256,
                        "missing_alpha_component_ids": sorted(missing_component_ids),
                        "unexpected_alpha_component_ids": sorted(unexpected_component_ids),
                        "unscoped_template_count": sum(item.alpha_component_id is None for item in matching),
                    },
                )
        elif any(item.alpha_component_id is not None for item in matching):
            raise Phase1EError(
                REASON_SOURCE_RESOLUTION_BLOCKED,
                "single Alpha source registry cannot declare parent-leg templates",
                context={"package_id": binding.package_id, "manifest_sha256": binding.manifest_sha256},
            )
        if scope.admission_scope_id != binding.admission_scope_id or scope.admission_scope_hash != binding.admission_scope_hash:
            raise Phase1EError(REASON_AUDIT_HANDOFF_MISMATCH, "scope identity does not match evidence binding")
        common_pit_hash = build_source_requirement_common_pit_identity_hash(
            admission_scope_id=scope.admission_scope_id,
            admission_scope_hash=scope.admission_scope_hash,
            handoff_readiness_hash=binding.handoff_readiness_report_hash,
            program_id=request.program_id,
            binding_version_id=binding.binding_version_id,
            package_id=binding.package_id,
            manifest_sha256=binding.manifest_sha256,
            alpha_mode=binding.alpha_mode,
            decision_as_of_trade_date=request.decision_trade_date,
            requested_source_cutoff=requested_source_cutoff,
            query_registry_hash=batch_request.query_registry_hash,
            calendar_hash=batch_request.calendar_hash,
            universe_policy_hash=universe_policy_hash,
            data_source=HISTORICAL_RESEARCH_DATA_SOURCE,
            execution_origin=EvidenceOrigin.MANUAL_HISTORICAL_RESEARCH.value,
            research_scope=HISTORICAL_RESEARCH_SCOPE,
            execution_prohibited=True,
            research_only=True,
        )
        requirements = tuple(
            SourceRequirement(
                consumer_scope_id=(
                    f"{scope.admission_scope_id}:{template.alpha_component_id or 'single'}:{template.consumer_scope_suffix}"
                ),
                source_role=template.source_role,
                dataset_name=template.dataset_name,
                query_template_id=template.query_template_id,
                query_template_version=template.query_template_version,
                query_template_hash=template.query_template_hash,
                bound_parameters=template.bound_parameters,
                bound_parameter_hash=canonical_json_sha256(template.bound_parameters),
                partition_key=template.partition_key,
                revision_kind=template.revision_kind,
                availability_requirement=template.availability_requirement,
                business_min_date=template.business_min_date,
                business_max_date=template.business_max_date,
                requested_cutoff=(request.label_as_of_ts if template.availability_requirement is AvailabilityRequirement.LABEL_AS_OF else requested_source_cutoff),
                enforced_cutoff_predicate_hash=template.enforced_cutoff_predicate_hash,
                common_pit_identity_hash=common_pit_hash,
            )
            for template in sorted(
                matching,
                key=lambda item: (
                    item.alpha_component_id or "",
                    item.consumer_scope_suffix,
                    item.source_role,
                    item.dataset_name,
                ),
            )
        )
        formal_oos_status = scope.formal_oos_status.value
        if formal_oos_status not in {FormalOOSStatus.RETROSPECTIVE_RESEARCH_ONLY.value, FormalOOSStatus.NONE.value}:
            raise Phase1EError(REASON_FORMAL_SCOPE_NOT_HISTORICAL_INPUT, "formal OOS scope is not a v1 historical input")
        return SourceRequirementSet(
            admission_scope_id=scope.admission_scope_id,
            admission_scope_hash=scope.admission_scope_hash,
            handoff_readiness_hash=binding.handoff_readiness_report_hash,
            program_id=request.program_id,
            binding_version_id=binding.binding_version_id,
            package_id=binding.package_id,
            manifest_sha256=binding.manifest_sha256,
            alpha_mode=binding.alpha_mode,
            decision_as_of_trade_date=request.decision_trade_date,
            requested_source_cutoff=requested_source_cutoff,
            label_as_of_ts=request.label_as_of_ts,
            query_registry_hash=batch_request.query_registry_hash,
            calendar_hash=batch_request.calendar_hash,
            universe_policy_hash=universe_policy_hash,
            formal_oos_status=formal_oos_status,
            evidence_scope=scope.evidence_scope.value,
            research_replay_eligible=False,
            requirements=requirements,
        )


class Phase1EReadinessPlanCompiler:
    """Compile independent scope plans; batch membership never enters a plan hash."""

    def __init__(
        self,
        *,
        source_requirement_compiler: RegistrySourceRequirementCompiler,
        capacity_request: CapacityPlanningRequest,
        capacity_receipt: CapacityPlanningReceipt,
        artifact_store: Phase1EArtifactStore | None = None,
    ) -> None:
        self._source_requirement_compiler = source_requirement_compiler
        self._capacity_request = capacity_request
        self._capacity_receipt = capacity_receipt
        self._artifact_store = artifact_store

    def compile_batch(
        self,
        *,
        request: Phase1ERevalidationBatchRequest,
        provider: Phase1EInputProvider,
    ) -> tuple[list[Phase1EExecutionPlan], Phase1EPlanBatchReceipt]:
        self._validate_batch_dependencies(request)
        plans: list[Phase1EExecutionPlan] = []
        failures: list[Phase1EFailedInputScope] = []
        for program_date in request.program_dates:
            scope_failed = False
            scope_plans: list[Phase1EExecutionPlan] = []
            try:
                scope_plans = self._compile_program_date(
                    request=request,
                    program_date=program_date,
                    provider=provider,
                )
            except Phase1EError as exc:
                scope_failed = True
                error_context = {
                    **exc.context,
                    "batch_request_hash": str(request.invocation_request_hash),
                    "program_id": program_date.program_id,
                    "decision_trade_date": program_date.decision_trade_date.isoformat(),
                }
                LOGGER.warning(
                    "phase1e_scope_unavailable stage=compile_program_date batch_request_hash=%s evidence_request_hash=%s scope_plan_request_hash=%s program_id=%s decision_trade_date=%s reason_code=%s",
                    error_context["batch_request_hash"],
                    error_context.get("evidence_request_hash"),
                    error_context.get("scope_plan_request_hash"),
                    program_date.program_id,
                    program_date.decision_trade_date.isoformat(),
                    exc.reason_code,
                )
                failures.append(
                    Phase1EFailedInputScope(
                        program_id=program_date.program_id,
                        decision_trade_date=program_date.decision_trade_date,
                        program_date_request_hash=str(program_date.program_date_request_hash),
                        reason_code=exc.reason_code,
                        context=canonicalize(error_context),
                    )
                )
            except Exception as exc:
                scope_failed = True
                LOGGER.exception(
                    "phase1e_scope_unexpected_error stage=compile_program_date batch_request_hash=%s program_id=%s decision_trade_date=%s",
                    request.invocation_request_hash,
                    program_date.program_id,
                    program_date.decision_trade_date.isoformat(),
                )
                failures.append(
                    Phase1EFailedInputScope(
                        program_id=program_date.program_id,
                        decision_trade_date=program_date.decision_trade_date,
                        program_date_request_hash=str(program_date.program_date_request_hash),
                        reason_code=REASON_UNEXPECTED_ERROR,
                        context={
                            "batch_request_hash": request.invocation_request_hash,
                            "program_id": program_date.program_id,
                            "decision_trade_date": program_date.decision_trade_date.isoformat(),
                            "exception_type": type(exc).__name__,
                            "message": str(exc),
                        },
                    )
                )
            finally:
                close = getattr(provider, "close_program_date", None)
                if callable(close):
                    try:
                        close()
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.exception(
                            "phase1e_snapshot_close_unexpected_error batch_request_hash=%s program_id=%s decision_trade_date=%s",
                            request.invocation_request_hash,
                            program_date.program_id,
                            program_date.decision_trade_date.isoformat(),
                        )
                        if not scope_failed:
                            scope_failed = True
                            scope_plans = []
                            failures.append(
                                Phase1EFailedInputScope(
                                    program_id=program_date.program_id,
                                    decision_trade_date=program_date.decision_trade_date,
                                    program_date_request_hash=str(program_date.program_date_request_hash),
                                    reason_code=REASON_UNEXPECTED_ERROR,
                                    context={
                                        "batch_request_hash": request.invocation_request_hash,
                                        "program_id": program_date.program_id,
                                        "decision_trade_date": program_date.decision_trade_date.isoformat(),
                                        "stage": "close_program_date",
                                        "exception_type": type(exc).__name__,
                                        "message": str(exc),
                                    },
                                )
                            )
            if not scope_failed:
                plans.extend(scope_plans)
        for plan in plans:
            self._publish_plan(plan)
        batch_request_hash = str(request.invocation_request_hash)
        receipt = self._batch_receipt(batch_request_hash=batch_request_hash, plans=plans, failures=failures)
        if self._artifact_store is not None:
            self._artifact_store.publish(
                kind="batch",
                identity=str(receipt.batch_receipt_hash),
                payload=receipt.model_dump(mode="json"),
                semantic_hash=str(receipt.batch_receipt_hash),
            )
        return plans, receipt

    def _validate_batch_dependencies(self, request: Phase1ERevalidationBatchRequest) -> None:
        if request.source_requirement_registry_hash != self._source_requirement_compiler.registry_hash:
            raise Phase1EError(REASON_SOURCE_RESOLUTION_CONFLICT, "source requirement registry hash does not match batch request")
        if self._capacity_request.query_registry_hash != request.query_registry_hash:
            raise Phase1EError(REASON_CAPACITY_WORKLOAD_NOT_COVERED, "capacity request query registry does not match batch")
        if request.capacity_request_ref != self._capacity_request.request_hash:
            raise Phase1EError(
                REASON_CAPACITY_REFERENCE_MISMATCH,
                "batch capacity request reference does not match the loaded canonical request",
            )
        if self._capacity_receipt.request_hash != self._capacity_request.request_hash:
            raise Phase1EError(REASON_CAPACITY_WORKLOAD_NOT_COVERED, "capacity receipt does not match capacity request")
        if request.capacity_receipt_ref != self._capacity_receipt.receipt_hash:
            raise Phase1EError(
                REASON_CAPACITY_REFERENCE_MISMATCH,
                "batch capacity receipt reference does not match the loaded canonical receipt",
            )

    def _compile_program_date(
        self,
        *,
        request: Phase1ERevalidationBatchRequest,
        program_date: Phase1EProgramDateRequest,
        provider: Phase1EInputProvider,
    ) -> list[Phase1EExecutionPlan]:
        evidence = provider.resolve_program_date(request=program_date, batch_request=request)
        self._validate_historical_input(program_date=program_date, evidence=evidence)
        if program_date.label_as_of_ts > _aware(evidence.postgres_now, field_name="postgres_now"):
            raise Phase1EError(REASON_HISTORICAL_DATE_REQUIRED, "label_as_of_ts is later than PostgreSQL snapshot time")
        if evidence.policy.registry_content_hash != request.phase0a_policy_hash:
            raise Phase1EError(
                REASON_POLICY_REGISTRY_HASH_MISMATCH,
                "frozen Phase 0A policy registry does not match the revalidation batch",
                context={
                    "expected_phase0a_policy_hash": request.phase0a_policy_hash,
                    "actual_phase0a_policy_hash": evidence.policy.registry_content_hash,
                },
            )
        binding = evidence.dated_binding
        package = evidence.package
        selection_evidence = evidence.selection_evidence
        selection_artifact = evidence.selection_artifact
        if binding is None:
            raise Phase1EError(REASON_DATED_BINDING_MISSING, "dated Program binding is missing")
        if package is None or selection_evidence is None or selection_artifact is None:
            raise Phase1EError(REASON_HISTORICAL_RECEIPT_CONFLICT, "immutable package/evidence/artifact readback is incomplete")
        self._validate_identity_assertions(program_date=program_date, evidence=evidence)
        evidence_request_hash = self._evidence_request_hash(program_date=program_date, evidence=evidence)
        audit_outcome = self._run_audit(
            program_date=program_date,
            evidence=evidence,
            evidence_request_hash=evidence_request_hash,
        )
        self._publish_audit(audit_outcome=audit_outcome, evidence_request_hash=evidence_request_hash)
        target = self._audit_target(audit_outcome.receipt, audit_target_id=f"p1e_target_{evidence_request_hash[:20]}")
        if target is None:
            raise Phase1EError(REASON_AUDIT_HANDOFF_MISMATCH, "audit did not return the deterministic target")
        target_authority = next(
            (item for item in target.candidate_authority if item.decision_date == program_date.decision_trade_date),
            None,
        )
        self._validate_label_as_of_cutoff(
            program_date=program_date,
            decision_cutoff_ts=(target_authority.decision_clock.decision_cutoff_ts if target_authority is not None else None),
        )
        scopes = self._matching_scopes(audit_outcome.handoff_report, target.audit_target_id)
        if not scopes:
            return [
                self._target_diagnostic_plan(
                    request=request,
                    program_date=program_date,
                    evidence=evidence,
                    evidence_request_hash=evidence_request_hash,
                    audit_outcome=audit_outcome,
                    target=target,
                    reason_codes=(REASON_TARGET_HAS_NO_ADMISSION_SCOPE, *target.phase0a_reason_codes),
                )
            ]
        plans: list[Phase1EExecutionPlan] = []
        for scope in scopes:
            if scope.formal_oos_status is FormalOOSStatus.FORMAL_OOS:
                plans.append(
                    self._target_diagnostic_plan(
                        request=request,
                        program_date=program_date,
                        evidence=evidence,
                        evidence_request_hash=evidence_request_hash,
                        audit_outcome=audit_outcome,
                        target=target,
                        reason_codes=(REASON_FORMAL_SCOPE_NOT_HISTORICAL_INPUT, *scope.blocking_reason_codes),
                        formal_scope=scope,
                    )
                )
                continue
            plans.append(
                self._admission_plan(
                    request=request,
                    program_date=program_date,
                    evidence=evidence,
                    evidence_request_hash=evidence_request_hash,
                    audit_outcome=audit_outcome,
                    target=target,
                    scope=scope,
                    provider=provider,
                )
            )
        return plans

    @staticmethod
    def _validate_historical_input(*, program_date: Phase1EProgramDateRequest, evidence: Phase1EProgramDateEvidence) -> None:
        batch = evidence.historical_batch
        receipt = evidence.historical_receipt
        run = evidence.historical_program_run
        if batch is None or receipt is None:
            raise Phase1EError(REASON_HISTORICAL_RECEIPT_MISSING, "historical batch receipt is missing")
        if (
            batch.data_source != HISTORICAL_RESEARCH_DATA_SOURCE
            or batch.origin != EvidenceOrigin.MANUAL_HISTORICAL_RESEARCH.value
            or batch.research_scope != HISTORICAL_RESEARCH_SCOPE
            or batch.execution_prohibited is not True
        ):
            raise Phase1EError(REASON_HISTORICAL_RECEIPT_CONFLICT, "historical batch does not satisfy research-only contract")
        expected_receipt_hash = canonical_json_sha256(
            _batch_receipt_payload(batch=batch, status=receipt.status, program_runs=receipt.program_runs)
        )
        if receipt.receipt_hash != expected_receipt_hash or receipt.status is not HistoricalResearchRunStatus.COMPLETE:
            raise Phase1EError(REASON_HISTORICAL_RECEIPT_CONFLICT, "historical receipt hash/status readback mismatch")
        if run is None or run not in receipt.program_runs:
            raise Phase1EError(REASON_HISTORICAL_RECEIPT_CONFLICT, "historical receipt does not contain requested Program run")
        if (
            run.program_id != program_date.program_id
            or run.decision_trade_date != program_date.decision_trade_date
            or run.research_scope != HISTORICAL_RESEARCH_SCOPE
            or run.status is not HistoricalResearchRunStatus.COMPLETE
        ):
            raise Phase1EError(REASON_PROGRAM_RUN_NOT_COMPLETE, "historical Program run is not the requested COMPLETE input")
        if batch.decision_trade_date != program_date.decision_trade_date:
            raise Phase1EError(REASON_HISTORICAL_RECEIPT_CONFLICT, "batch date does not match program-date request")
        required = (
            run.program_payload_sha256,
            run.binding_version_id,
            run.binding_payload_hash,
            run.package_id,
            run.manifest_sha256,
            run.policy_hash,
            run.evidence_id,
            run.evidence_hash,
            run.artifact_id,
            run.artifact_payload_hash,
            run.source_watermark_hash,
        )
        if any(not str(value or "").strip() for value in required):
            raise Phase1EError(REASON_HISTORICAL_RECEIPT_CONFLICT, "complete Program run has missing immutable lineage")
        expected_program_hash = _program_payload_hash(
            context=HistoricalResearchProgramContext(
                program_id=run.program_id,
                binding_version_id=str(run.binding_version_id),
                binding_payload_hash=str(run.binding_payload_hash),
                package_id=str(run.package_id),
                manifest_sha256=str(run.manifest_sha256),
                policy_hash=str(run.policy_hash),
                effective_runtime_config_hash=str(run.effective_runtime_config_hash),
            ),
            evidence=HistoricalSelectionEvidence(
                evidence_id=str(run.evidence_id),
                evidence_hash=str(run.evidence_hash),
                artifact_id=str(run.artifact_id),
                artifact_payload_hash=str(run.artifact_payload_hash),
                source_watermark_hash=str(run.source_watermark_hash),
                candidate_outcome=str(run.candidate_outcome),
                candidates=list(run.research_candidates),
            ),
        )
        if run.program_payload_sha256 != expected_program_hash:
            raise Phase1EError(
                REASON_HISTORICAL_RECEIPT_CONFLICT,
                "historical Program payload hash does not match immutable lineage",
                context={
                    "program_id": run.program_id,
                    "stored_program_payload_sha256": run.program_payload_sha256,
                    "expected_program_payload_sha256": expected_program_hash,
                },
            )

    @staticmethod
    def _validate_label_as_of_cutoff(*, program_date: Phase1EProgramDateRequest, decision_cutoff_ts: datetime | None) -> None:
        if decision_cutoff_ts is None:
            return
        try:
            normalized_cutoff = _aware(decision_cutoff_ts, field_name="decision_clock.decision_cutoff_ts")
        except ValueError as exc:
            raise Phase1EError(
                REASON_HISTORICAL_DATE_REQUIRED,
                "immutable decision cutoff is not timezone-aware",
            ) from exc
        if program_date.label_as_of_ts < normalized_cutoff:
            raise Phase1EError(
                REASON_HISTORICAL_DATE_REQUIRED,
                "label_as_of_ts precedes the immutable decision cutoff",
                context={
                    "label_as_of_ts": program_date.label_as_of_ts.isoformat(),
                    "decision_cutoff_ts": normalized_cutoff.isoformat(),
                },
            )

    @staticmethod
    def _validate_identity_assertions(*, program_date: Phase1EProgramDateRequest, evidence: Phase1EProgramDateEvidence) -> None:
        run = evidence.historical_program_run
        binding = evidence.dated_binding
        package = evidence.package
        selection_evidence = evidence.selection_evidence
        artifact = evidence.selection_artifact
        assert run is not None and binding is not None and package is not None and selection_evidence is not None and artifact is not None
        package_mode = str(getattr(package.alpha_mode, "value", package.alpha_mode))
        if package_mode not in {"single_alpha", "multi_alpha"}:
            raise Phase1EError(REASON_PACKAGE_TYPE_UNSUPPORTED, "only single Alpha and native multi Alpha parent packages are supported")
        mismatches: dict[str, Any] = {}
        values = {
            "binding_version_id": (run.binding_version_id, getattr(binding, "binding_version_id", None)),
            "binding_payload_hash": (run.binding_payload_hash, getattr(binding, "binding_payload_hash", None)),
            "package_id": (run.package_id, getattr(package, "package_id", None)),
            "manifest_sha256": (run.manifest_sha256, getattr(package, "manifest_sha256", None)),
            "evidence_id": (run.evidence_id, getattr(selection_evidence, "evidence_id", None)),
            "evidence_hash": (run.evidence_hash, getattr(selection_evidence, "artifact_hash", None)),
            "artifact_id": (run.artifact_id, getattr(artifact, "artifact_id", None)),
            "artifact_payload_hash": (run.artifact_payload_hash, getattr(artifact, "artifact_payload_sha256", None)),
        }
        for name, (expected, actual) in values.items():
            if expected != actual:
                mismatches[name] = {"historical": expected, "readback": actual}
        if mismatches:
            reason = REASON_PACKAGE_LINEAGE_HASH_MISMATCH if "artifact_payload_hash" in mismatches else REASON_BINDING_IDENTITY_MISMATCH
            raise Phase1EError(reason, "immutable program lineage does not match projection readback", context=mismatches)
        if program_date.expected_package_id is not None and program_date.expected_package_id != package.package_id:
            raise Phase1EError(REASON_BINDING_IDENTITY_MISMATCH, "expected package assertion does not match dated binding")
        if program_date.expected_manifest_sha256 is not None and program_date.expected_manifest_sha256 != package.manifest_sha256:
            raise Phase1EError(REASON_BINDING_IDENTITY_MISMATCH, "expected manifest assertion does not match dated binding")
        if program_date.expected_alpha_mode is not None and program_date.expected_alpha_mode != package_mode:
            raise Phase1EError(REASON_PACKAGE_TYPE_UNSUPPORTED, "expected alpha mode does not match dated package")
        resolved_style = Phase1EReadinessPlanCompiler._resolved_style(evidence=evidence)
        if program_date.expected_style_family is not None and program_date.expected_style_family != resolved_style:
            raise Phase1EError(REASON_BINDING_IDENTITY_MISMATCH, "expected style assertion does not match frozen style assignment")

    @staticmethod
    def _resolved_style(*, evidence: Phase1EProgramDateEvidence) -> str:
        package = evidence.package
        assert package is not None
        manifest = getattr(package, "manifest", None)
        declared = str(getattr(manifest, "style_family", "") or "").strip()
        source_evidence = getattr(manifest, "source_evidence", {}) if manifest is not None else {}
        if not declared and isinstance(source_evidence, dict):
            declared = str(source_evidence.get("style_family") or "").strip()
        policy = evidence.policy.style_assignment_policy or {}
        by_package = policy.get("by_package") if isinstance(policy, dict) else None
        by_manifest = policy.get("by_manifest_sha256") if isinstance(policy, dict) else None
        assigned = None
        if isinstance(by_manifest, dict):
            assigned = by_manifest.get(package.manifest_sha256)
        if assigned is None and isinstance(by_package, dict):
            assigned = by_package.get(package.package_id)
        assigned_text = str(assigned or "").strip()
        if declared and assigned_text and declared != assigned_text:
            raise Phase1EError(REASON_BINDING_IDENTITY_MISMATCH, "frozen manifest style conflicts with frozen style policy")
        resolved = declared or assigned_text
        if not resolved:
            raise Phase1EError(REASON_BINDING_IDENTITY_MISMATCH, "frozen dated package has no style assignment")
        return resolved

    @staticmethod
    def _evidence_request_hash(*, program_date: Phase1EProgramDateRequest, evidence: Phase1EProgramDateEvidence) -> str:
        run = evidence.historical_program_run
        binding = evidence.dated_binding
        package = evidence.package
        assert run is not None and binding is not None and package is not None
        style = Phase1EReadinessPlanCompiler._resolved_style(evidence=evidence)
        return canonical_json_sha256(
            {
                "schema_version": "advisory_phase1e_evidence_request_v1",
                "program_id": program_date.program_id,
                "decision_trade_date": program_date.decision_trade_date,
                "evidence_origin": program_date.evidence_origin.value,
                "binding_version_id": binding.binding_version_id,
                "binding_payload_hash": run.binding_payload_hash,
                "package_id": package.package_id,
                "manifest_sha256": package.manifest_sha256,
                "alpha_mode": str(getattr(package.alpha_mode, "value", package.alpha_mode)),
                "resolved_style_family": style,
                "historical_receipt_hash": evidence.historical_receipt.receipt_hash if evidence.historical_receipt is not None else None,
                "historical_program_run_id": run.program_run_id,
                "program_payload_sha256": run.program_payload_sha256,
                "selection_evidence_id": run.evidence_id,
                "selection_evidence_hash": run.evidence_hash,
                "selection_artifact_id": run.artifact_id,
                "selection_artifact_payload_hash": run.artifact_payload_hash,
                "source_watermark_hash": run.source_watermark_hash,
                "phase0a_policy_hash": evidence.policy.registry_content_hash,
            }
        )

    def _run_audit(
        self,
        *,
        program_date: Phase1EProgramDateRequest,
        evidence: Phase1EProgramDateEvidence,
        evidence_request_hash: str,
    ) -> Phase1EAuditOutcome:
        run = evidence.historical_program_run
        package = evidence.package
        assert run is not None and package is not None
        audit_id = f"p1e_audit_{evidence_request_hash[:20]}"
        audit_target_id = f"p1e_target_{evidence_request_hash[:20]}"
        alpha_mode = ExpectedAlphaMode(str(getattr(package.alpha_mode, "value", package.alpha_mode)))
        style = self._resolved_style(evidence=evidence)
        request = AuditRequest(
            audit_id=audit_id,
            policy_registry_id=str(evidence.policy.policy_registry_id),
            audit_policy_version=evidence.policy.policy_version,
            policy_registry_content_hash=str(evidence.policy.registry_content_hash),
            targets=[
                AuditTarget(
                    audit_target_id=audit_target_id,
                    program_id=program_date.program_id,
                    package_id=package.package_id,
                    manifest_sha256=package.manifest_sha256,
                    expected_alpha_mode=alpha_mode,
                    decision_date_range=AuditDateRange(
                        start_date=program_date.decision_trade_date,
                        end_date=program_date.decision_trade_date,
                    ),
                    decision_dates=[program_date.decision_trade_date],
                    selection_evidence_ids_by_decision_date={program_date.decision_trade_date: str(run.evidence_id)},
                    style_family=style,
                    audit_policy_version=evidence.policy.policy_version,
                )
            ],
        )
        audit = AdvisoryPhase0AAuditService(readers=evidence.audit_readers, policy=evidence.policy).audit(request)
        report, bundle = Phase0AHandoffNormalizer(policy=evidence.policy).normalize(receipt=audit, request=request)
        return Phase1EAuditOutcome(request=request, receipt=audit, handoff_report=report, handoff_bundle=bundle)

    def _publish_audit(self, *, audit_outcome: Phase1EAuditOutcome, evidence_request_hash: str) -> None:
        if self._artifact_store is None:
            return
        payload = {
            "schema_version": "advisory_phase1e_audit_materialization_v1",
            "evidence_request_hash": evidence_request_hash,
            "request": audit_outcome.request.model_dump(mode="json"),
            "receipt": audit_outcome.receipt.model_dump(mode="json"),
            "handoff_readiness_report": audit_outcome.handoff_report.model_dump(mode="json"),
            "phase1_handoff_bundle": audit_outcome.handoff_bundle.model_dump(mode="json") if audit_outcome.handoff_bundle is not None else None,
        }
        self._artifact_store.publish(
            kind="audit",
            identity=audit_outcome.receipt.audit_id,
            payload=payload,
            semantic_hash=canonical_json_sha256(payload),
        )

    @staticmethod
    def _audit_target(receipt: AuditReceipt, *, audit_target_id: str) -> Any | None:
        return next((item for item in receipt.results if item.audit_target_id == audit_target_id), None)

    @staticmethod
    def _matching_scopes(report: HandoffReadinessReport, audit_target_id: str) -> list[HandoffAdmissionScope]:
        target = next((item for item in report.sorted_target_handoffs if item.audit_target_id == audit_target_id), None)
        return list(target.admission_scopes) if target is not None else []

    def _target_diagnostic_plan(
        self,
        *,
        request: Phase1ERevalidationBatchRequest,
        program_date: Phase1EProgramDateRequest,
        evidence: Phase1EProgramDateEvidence,
        evidence_request_hash: str,
        audit_outcome: Phase1EAuditOutcome,
        target: Any,
        reason_codes: Iterable[str],
        formal_scope: HandoffAdmissionScope | None = None,
    ) -> Phase1EExecutionPlan:
        binding = self._evidence_binding(
            program_date=program_date,
            evidence=evidence,
            audit_outcome=audit_outcome,
            target=target,
            scope=None,
        )
        scope_plan_hash = self._scope_plan_request_hash(
            request=request,
            program_date=program_date,
            evidence_request_hash=evidence_request_hash,
            scope_id=None,
            target_scope_hash=target_scope_hash(target),
        )
        operations = tuple(
            Phase1EPlannedOperation(
                operation_type=operation_type,
                operation_disposition=OperationDisposition.NOT_APPLICABLE,
                contract_schema_version=PHASE1E_TEMPLATE_SCHEMA_VERSION,
            )
            for operation_type in PlannedOperationType
        )
        reasons = list(reason_codes)
        if formal_scope is not None:
            reasons.extend(formal_scope.blocking_reason_codes)
        return Phase1EExecutionPlan(
            evidence_request_hash=evidence_request_hash,
            scope_plan_request_hash=scope_plan_hash,
            compiler_version=request.compiler_version,
            serializer_version=request.serializer_version,
            compiler_source_hash=request.compiler_source_hash,
            plan_unit_kind=PlanUnitKind.TARGET_DIAGNOSTIC,
            target_key={
                "program_id": program_date.program_id,
                "decision_trade_date": program_date.decision_trade_date,
                "package_id": binding.package_id,
                "manifest_sha256": binding.manifest_sha256,
                "audit_target_id": target.audit_target_id,
                "target_scope_hash": binding.target_scope_hash,
            },
            evidence_binding=binding,
            handoff_readiness=HandoffReadiness.BLOCKED,
            reason_codes=_sorted_strings(reasons),
            missing_evidence=tuple({"reason_code": code} for code in _sorted_strings(reasons)),
            planned_operations=operations,
        )

    def _admission_plan(
        self,
        *,
        request: Phase1ERevalidationBatchRequest,
        program_date: Phase1EProgramDateRequest,
        evidence: Phase1EProgramDateEvidence,
        evidence_request_hash: str,
        audit_outcome: Phase1EAuditOutcome,
        target: Any,
        scope: HandoffAdmissionScope,
        provider: Phase1EInputProvider,
    ) -> Phase1EExecutionPlan:
        binding = self._evidence_binding(
            program_date=program_date,
            evidence=evidence,
            audit_outcome=audit_outcome,
            target=target,
            scope=scope,
        )
        scope_plan_hash = self._scope_plan_request_hash(
            request=request,
            program_date=program_date,
            evidence_request_hash=evidence_request_hash,
            scope_id=scope.admission_scope_id,
            target_scope_hash=scope.target_scope_hash,
        )
        if scope.readiness is HandoffReadiness.BLOCKED:
            return self._blocked_admission_plan(
                request=request,
                binding=binding,
                scope=scope,
                program_date=program_date,
                scope_plan_hash=scope_plan_hash,
                evidence_request_hash=evidence_request_hash,
            )
        target_authority = next(
            (item for item in target.candidate_authority if item.decision_date == program_date.decision_trade_date),
            None,
        )
        clock = target_authority.decision_clock if target_authority is not None else None
        universe_hash = self._universe_policy_hash(target=target, decision_date=program_date.decision_trade_date)
        if clock is None or clock.decision_cutoff_ts is None or universe_hash is None:
            return self._incomplete_source_plan(
                request=request,
                binding=binding,
                scope=scope,
                program_date=program_date,
                scope_plan_hash=scope_plan_hash,
                evidence_request_hash=evidence_request_hash,
                missing=("decision_clock", "universe_policy_hash"),
            )
        self._validate_label_as_of_cutoff(program_date=program_date, decision_cutoff_ts=clock.decision_cutoff_ts)
        try:
            requirements = self._source_requirement_compiler.compile(
                binding=binding,
                scope=scope,
                request=program_date,
                batch_request=request,
                universe_policy_hash=universe_hash,
                requested_source_cutoff=clock.decision_cutoff_ts,
            )
            resolution = FixtureSourceRevisionResolver().resolve(
                requirement_set=requirements,
                availability_events=provider.list_source_events(requirements=requirements),
            )
        except Phase1EError:
            raise
        except Exception as exc:
            LOGGER.exception(
                "phase1e_source_resolution_unexpected_error program_id=%s decision_trade_date=%s admission_scope_id=%s",
                program_date.program_id,
                program_date.decision_trade_date.isoformat(),
                scope.admission_scope_id,
            )
            raise Phase1EError(
                REASON_SOURCE_RESOLUTION_CONFLICT,
                "read-only source resolution failed",
                context={"exception_type": type(exc).__name__, "message": str(exc)},
            ) from exc
        workload = self._workload_projection(
            scope_plan_hash=scope_plan_hash,
            binding=binding,
            scope=scope,
            decision_trade_date=program_date.decision_trade_date,
            candidate_depth=(target_authority.effective_depth if target_authority is not None else 0) or 0,
            requirements=requirements,
            source_result=resolution,
        )
        coverage, coverage_reasons = self._capacity_coverage(workload=workload, style_family=binding.resolved_style_family)
        bounded_staging_capture_allowed = self._bounded_staging_capture_allowed(
            workload=workload,
            style_family=binding.resolved_style_family,
        )
        capture_plan, capture_missing = self._build_capture_plan(
            binding=binding,
            scope=scope,
            program_date=program_date,
            evidence=evidence,
            audit_outcome=audit_outcome,
            target=target,
            source_result=resolution,
            universe_policy_hash=universe_hash,
        )
        operations = self._operations_for_resolution(
            binding=binding,
            scope=scope,
            scope_context=self._operation_scope_context(
                request=request,
                program_date=program_date,
                binding=binding,
                scope=scope,
                evidence_request_hash=evidence_request_hash,
            ),
            requirements=requirements,
            source_result=resolution,
            capture_plan=capture_plan,
            capture_missing=capture_missing,
            workload_covered=coverage,
            bounded_staging_capture_allowed=bounded_staging_capture_allowed,
        )
        reasons = list(scope.blocking_reason_codes)
        reasons.extend(resolution.receipt.reason_codes)
        reasons.extend(coverage_reasons)
        if self._capacity_receipt.status is CapacityStatus.PARTIAL:
            reasons.append(REASON_CAPACITY_MEASUREMENT_PARTIAL)
        elif self._capacity_receipt.status is CapacityStatus.INSUFFICIENT:
            reasons.append(REASON_CAPACITY_INSUFFICIENT)
        return Phase1EExecutionPlan(
            evidence_request_hash=evidence_request_hash,
            scope_plan_request_hash=scope_plan_hash,
            compiler_version=request.compiler_version,
            serializer_version=request.serializer_version,
            compiler_source_hash=request.compiler_source_hash,
            plan_unit_kind=PlanUnitKind.ADMISSION_SCOPE,
            scope_key={
                "program_id": program_date.program_id,
                "decision_trade_date": program_date.decision_trade_date,
                "package_id": binding.package_id,
                "manifest_sha256": binding.manifest_sha256,
                "admission_scope_id": scope.admission_scope_id,
                "evidence_scope": scope.evidence_scope.value,
            },
            evidence_binding=binding,
            handoff_readiness=scope.readiness,
            source_readiness=resolution.receipt.readiness,
            capacity_status=self._capacity_receipt.status,
            reason_codes=_sorted_strings(reasons),
            missing_evidence=tuple(
                {"reason_code": reason}
                for reason in _sorted_strings(
                    [*resolution.receipt.reason_codes, *coverage_reasons, *scope.blocking_reason_codes]
                )
            ),
            planned_operations=operations,
            workload_projection=workload,
            resource_budget_by_role=self._resource_budget_by_role(),
            memory_budget=self._capacity_receipt.memory_budget_summary,
            temporary_store_budget=self._capacity_receipt.staging_store_summary,
            durable_store_budget=self._capacity_receipt.durable_store_summary,
            missing_capacity_measurements=self._capacity_receipt.missing_measurements,
            capacity_request_hash=self._capacity_request.request_hash,
            capacity_receipt_hash=self._capacity_receipt.receipt_hash,
            capacity_workload_covered=coverage,
            resource_values_frozen=self._capacity_receipt.status is CapacityStatus.MEASURED and coverage,
        )

    def _blocked_admission_plan(
        self,
        *,
        request: Phase1ERevalidationBatchRequest,
        binding: Phase1EEvidenceBinding,
        scope: HandoffAdmissionScope,
        program_date: Phase1EProgramDateRequest,
        scope_plan_hash: str,
        evidence_request_hash: str,
    ) -> Phase1EExecutionPlan:
        operations = tuple(
            Phase1EPlannedOperation(
                operation_type=operation_type,
                operation_disposition=OperationDisposition.NOT_APPLICABLE,
                contract_schema_version=PHASE1E_TEMPLATE_SCHEMA_VERSION,
            )
            for operation_type in PlannedOperationType
        )
        return Phase1EExecutionPlan(
            evidence_request_hash=evidence_request_hash,
            scope_plan_request_hash=scope_plan_hash,
            compiler_version=request.compiler_version,
            serializer_version=request.serializer_version,
            compiler_source_hash=request.compiler_source_hash,
            plan_unit_kind=PlanUnitKind.ADMISSION_SCOPE,
            scope_key={
                "program_id": program_date.program_id,
                "decision_trade_date": program_date.decision_trade_date,
                "package_id": binding.package_id,
                "manifest_sha256": binding.manifest_sha256,
                "admission_scope_id": scope.admission_scope_id,
                "evidence_scope": scope.evidence_scope.value,
            },
            evidence_binding=binding,
            handoff_readiness=scope.readiness,
            source_readiness=None,
            capacity_status=self._capacity_receipt.status,
            reason_codes=_sorted_strings(scope.blocking_reason_codes),
            missing_evidence=tuple({"reason_code": code} for code in scope.blocking_reason_codes),
            planned_operations=operations,
            workload_projection=self._zero_workload(
                scope_plan_hash=scope_plan_hash,
                style_family=binding.resolved_style_family,
                decision_trade_date=program_date.decision_trade_date,
            ),
            resource_budget_by_role=self._resource_budget_by_role(),
            memory_budget=self._capacity_receipt.memory_budget_summary,
            temporary_store_budget=self._capacity_receipt.staging_store_summary,
            durable_store_budget=self._capacity_receipt.durable_store_summary,
            missing_capacity_measurements=self._capacity_receipt.missing_measurements,
            capacity_request_hash=self._capacity_request.request_hash,
            capacity_receipt_hash=self._capacity_receipt.receipt_hash,
            capacity_workload_covered=False,
            resource_values_frozen=False,
        )

    def _incomplete_source_plan(
        self,
        *,
        request: Phase1ERevalidationBatchRequest,
        binding: Phase1EEvidenceBinding,
        scope: HandoffAdmissionScope,
        program_date: Phase1EProgramDateRequest,
        scope_plan_hash: str,
        evidence_request_hash: str,
        missing: Sequence[str],
    ) -> Phase1EExecutionPlan:
        scope_context = self._operation_scope_context(
            request=request,
            program_date=program_date,
            binding=binding,
            scope=scope,
            evidence_request_hash=evidence_request_hash,
        )
        operations = self._template_operations(
            source_payload={
                "schema_version": PHASE1E_TEMPLATE_SCHEMA_VERSION,
                "scope_context": scope_context,
                "missing": list(missing),
            },
            source_slots=tuple(
                _output_slot(
                    slot=value,
                    source_type="immutable_audit_evidence",
                    producer_operation="phase0a_audit_projection",
                    hash_validation="exact_immutable_readback",
                )
                for value in missing
            ),
            deferred=True,
        )
        return Phase1EExecutionPlan(
            evidence_request_hash=evidence_request_hash,
            scope_plan_request_hash=scope_plan_hash,
            compiler_version=request.compiler_version,
            serializer_version=request.serializer_version,
            compiler_source_hash=request.compiler_source_hash,
            plan_unit_kind=PlanUnitKind.ADMISSION_SCOPE,
            scope_key={
                "program_id": program_date.program_id,
                "decision_trade_date": program_date.decision_trade_date,
                "package_id": binding.package_id,
                "manifest_sha256": binding.manifest_sha256,
                "admission_scope_id": scope.admission_scope_id,
                "evidence_scope": scope.evidence_scope.value,
            },
            evidence_binding=binding,
            handoff_readiness=scope.readiness,
            source_readiness=ResearchReadiness.BLOCKED,
            capacity_status=self._capacity_receipt.status,
            reason_codes=(REASON_SOURCE_RESOLUTION_BLOCKED, REASON_REQUEST_TEMPLATE_INCOMPLETE),
            missing_evidence=tuple({"slot": value} for value in missing),
            planned_operations=operations,
            workload_projection=self._zero_workload(
                scope_plan_hash=scope_plan_hash,
                style_family=binding.resolved_style_family,
                decision_trade_date=program_date.decision_trade_date,
            ),
            resource_budget_by_role=self._resource_budget_by_role(),
            memory_budget=self._capacity_receipt.memory_budget_summary,
            temporary_store_budget=self._capacity_receipt.staging_store_summary,
            durable_store_budget=self._capacity_receipt.durable_store_summary,
            missing_capacity_measurements=self._capacity_receipt.missing_measurements,
            capacity_request_hash=self._capacity_request.request_hash,
            capacity_receipt_hash=self._capacity_receipt.receipt_hash,
            capacity_workload_covered=False,
            resource_values_frozen=False,
        )

    def _evidence_binding(
        self,
        *,
        program_date: Phase1EProgramDateRequest,
        evidence: Phase1EProgramDateEvidence,
        audit_outcome: Phase1EAuditOutcome,
        target: Any,
        scope: HandoffAdmissionScope | None,
    ) -> Phase1EEvidenceBinding:
        batch = evidence.historical_batch
        receipt = evidence.historical_receipt
        run = evidence.historical_program_run
        binding = evidence.dated_binding
        package = evidence.package
        selection_evidence = evidence.selection_evidence
        artifact = evidence.selection_artifact
        assert all(value is not None for value in (batch, receipt, run, binding, package, selection_evidence, artifact))
        style = self._resolved_style(evidence=evidence)
        alpha_mode = str(getattr(package.alpha_mode, "value", package.alpha_mode))
        manifest_components = getattr(getattr(package, "manifest", None), "alpha_components", ()) or ()
        alpha_component_ids = tuple(
            sorted(str(getattr(component, "alpha_id", "") or "").strip() for component in manifest_components)
        ) if alpha_mode == "multi_alpha" else ()
        if any(not value for value in alpha_component_ids) or len(alpha_component_ids) != len(set(alpha_component_ids)):
            raise Phase1EError(
                REASON_PACKAGE_LINEAGE_HASH_MISMATCH,
                "persisted native multi Alpha parent has invalid Alpha-leg identity lineage",
                context={"package_id": package.package_id, "manifest_sha256": package.manifest_sha256},
            )
        target_scope_hash_value = canonical_json_sha256(
            {
                "audit_target_id": target.audit_target_id,
                "program_id": target.program_id,
                "package_id": target.package_id,
                "manifest_sha256": target.manifest_sha256,
            }
        )
        if scope is not None:
            target_scope_hash_value = scope.target_scope_hash
        return Phase1EEvidenceBinding(
            historical_batch_id=batch.batch_id,
            historical_batch_key=batch.batch_key,
            historical_receipt_hash=receipt.receipt_hash,
            historical_program_run_id=run.program_run_id,
            program_payload_sha256=str(run.program_payload_sha256),
            binding_version_id=str(run.binding_version_id),
            binding_payload_hash=str(run.binding_payload_hash),
            package_id=package.package_id,
            manifest_sha256=package.manifest_sha256,
            alpha_mode=alpha_mode,
            manifest_alpha_component_ids=alpha_component_ids,
            resolved_style_family=style,
            style_assignment_policy_hash=canonical_json_sha256(evidence.policy.style_assignment_policy),
            selection_evidence_id=selection_evidence.evidence_id,
            selection_evidence_hash=selection_evidence.artifact_hash,
            selection_artifact_id=artifact.artifact_id,
            selection_artifact_payload_hash=str(artifact.artifact_payload_sha256),
            source_watermark_hash=str(run.source_watermark_hash),
            package_asset_closure_hash=getattr(artifact, "asset_closure_hash", None),
            package_lineage_hash=getattr(package, "lineage_hash", None),
            phase0a_audit_id=audit_outcome.receipt.audit_id,
            phase0a_audit_manifest_hash=audit_outcome.receipt.audit_manifest_hash,
            phase0a_request_hash=audit_outcome.receipt.request_hash,
            handoff_readiness_report_hash=audit_outcome.handoff_report.handoff_readiness_hash,
            phase1_handoff_bundle_hash=(audit_outcome.handoff_bundle.phase1_handoff_bundle_hash if scope is not None and audit_outcome.handoff_bundle is not None else None),
            admission_scope_set_hash=(audit_outcome.handoff_bundle.admission_scope_set_hash if scope is not None and audit_outcome.handoff_bundle is not None else None),
            admission_scope_id=scope.admission_scope_id if scope is not None else None,
            admission_scope_hash=scope.admission_scope_hash if scope is not None else None,
            target_scope_hash=target_scope_hash_value,
            oos_interval_hash=scope.oos_interval_hash if scope is not None else None,
            evidence_scope=scope.evidence_scope.value if scope is not None else None,
            formal_oos_status=scope.formal_oos_status.value if scope is not None else None,
            signal_evidence_level=scope.signal_evidence_level.value if scope is not None else None,
            stable_signal_semantics_hash=scope.stable_signal_semantics_hash if scope is not None else None,
            decision_clock_hash=scope.decision_clock_hash if scope is not None else None,
        )

    def _scope_plan_request_hash(
        self,
        *,
        request: Phase1ERevalidationBatchRequest,
        program_date: Phase1EProgramDateRequest,
        evidence_request_hash: str,
        scope_id: str | None,
        target_scope_hash: str,
    ) -> str:
        return canonical_json_sha256(
            {
                "schema_version": "advisory_phase1e_scope_plan_request_v1",
                "program_date_request_hash": program_date.program_date_request_hash,
                "evidence_request_hash": evidence_request_hash,
                "scope_id": scope_id,
                "target_scope_hash": target_scope_hash,
                "source_requirement_registry_hash": request.source_requirement_registry_hash,
                "query_registry_hash": request.query_registry_hash,
                "calendar_hash": request.calendar_hash,
                "label_policy_bundle_hash": request.label_policy_bundle_hash,
                "dataset_schema_fingerprint": request.dataset_schema_fingerprint,
                "partition_policy_hash": request.partition_policy_hash,
                "store_backend_config_hash": request.store_backend_config_hash,
                "capacity_request_hash": request.capacity_request_ref,
                "capacity_receipt_hash": request.capacity_receipt_ref,
                "compiler_version": request.compiler_version,
                "serializer_version": request.serializer_version,
                "compiler_source_hash": request.compiler_source_hash,
                "artifact_store_policy_hash": request.artifact_store_policy_hash,
            }
        )

    @staticmethod
    def _universe_policy_hash(*, target: Any, decision_date: date) -> str | None:
        rows = [item for item in target.universe_survivorship if item.decision_date == decision_date]
        if not rows:
            return None
        values = [layer.policy_hash for layer in rows[0].layers if layer.policy_hash]
        return canonical_json_sha256(sorted(values)) if values else None

    def _workload_projection(
        self,
        *,
        scope_plan_hash: str,
        binding: Phase1EEvidenceBinding,
        scope: HandoffAdmissionScope,
        decision_trade_date: date,
        candidate_depth: int,
        requirements: SourceRequirementSet,
        source_result: SourceResolutionResult,
    ) -> Phase1EWorkloadProjection:
        request = self._capacity_request
        horizons = tuple(sorted(set(request.horizons)))
        source_roles: dict[str, int] = {}
        for requirement in requirements.requirements:
            source_roles[requirement.source_role] = source_roles.get(requirement.source_role, 0) + 1
        base = max(candidate_depth, 0)
        signal = base
        stage = signal * request.stage_projection_factor
        labels = signal * len(horizons) * request.projection_count
        def rows(universe: int) -> dict[str, int]:
            return {
                "canonical_signals": signal,
                "stage_candidates": stage,
                "outcome_labels": labels,
                "universe_outcomes": universe * len(horizons) * request.projection_count,
                "source_revisions": len(source_result.source_revision_set.members) if source_result.source_revision_set is not None else 0,
            }
        rows_p50 = rows(request.universe_size_p50)
        rows_p95 = rows(request.universe_size_p95)
        rows_max = rows(request.universe_size_max)
        return Phase1EWorkloadProjection(
            scope_plan_request_hash=scope_plan_hash,
            style_family=binding.resolved_style_family,
            decision_trade_date=decision_trade_date,
            candidate_depth=base,
            horizons=horizons,
            projection_count=request.projection_count,
            stage_projection_factor=request.stage_projection_factor,
            universe_size_p50=request.universe_size_p50,
            universe_size_p95=request.universe_size_p95,
            universe_size_max=request.universe_size_max,
            source_role_counts=source_roles,
            role_rows_p50=rows_p50,
            role_rows_p95=rows_p95,
            role_rows_max=rows_max,
            role_logical_bytes_p50=self._project_role_bytes(rows_p50, measurement_key="logical_bytes_per_row_p95"),
            role_logical_bytes_p95=self._project_role_bytes(rows_p95, measurement_key="logical_bytes_per_row_p95"),
            role_logical_bytes_max=self._project_role_bytes(rows_max, measurement_key="logical_bytes_per_row_p95"),
            role_parquet_bytes_p50=self._project_role_bytes(rows_p50, measurement_key="parquet_bytes_per_row_p95"),
            role_parquet_bytes_p95=self._project_role_bytes(rows_p95, measurement_key="parquet_bytes_per_row_p95"),
            role_parquet_bytes_max=self._project_role_bytes(rows_max, measurement_key="parquet_bytes_per_row_p95"),
        )

    def _zero_workload(
        self,
        *,
        scope_plan_hash: str,
        style_family: str,
        decision_trade_date: date,
    ) -> Phase1EWorkloadProjection:
        request = self._capacity_request
        zero = {role: 0 for role in CAPACITY_LOGICAL_ROLES}
        return Phase1EWorkloadProjection(
            scope_plan_request_hash=scope_plan_hash,
            style_family=style_family,
            decision_trade_date=decision_trade_date,
            candidate_depth=0,
            horizons=tuple(sorted(set(request.horizons))),
            projection_count=request.projection_count,
            stage_projection_factor=request.stage_projection_factor,
            universe_size_p50=0,
            universe_size_p95=0,
            universe_size_max=0,
            role_rows_p50=zero,
            role_rows_p95=zero,
            role_rows_max=zero,
            role_logical_bytes_p50=self._project_role_bytes(zero, measurement_key="logical_bytes_per_row_p95"),
            role_logical_bytes_p95=self._project_role_bytes(zero, measurement_key="logical_bytes_per_row_p95"),
            role_logical_bytes_max=self._project_role_bytes(zero, measurement_key="logical_bytes_per_row_p95"),
            role_parquet_bytes_p50=self._project_role_bytes(zero, measurement_key="parquet_bytes_per_row_p95"),
            role_parquet_bytes_p95=self._project_role_bytes(zero, measurement_key="parquet_bytes_per_row_p95"),
            role_parquet_bytes_max=self._project_role_bytes(zero, measurement_key="parquet_bytes_per_row_p95"),
        )

    def _project_role_bytes(self, role_rows: dict[str, int], *, measurement_key: str) -> dict[str, int | None]:
        summary = self._capacity_receipt.parquet_measurement_summary
        widths = summary.get(measurement_key) if isinstance(summary, dict) else None
        widths = widths if isinstance(widths, dict) else {}
        projected: dict[str, int | None] = {}
        for role in CAPACITY_LOGICAL_ROLES:
            raw_width = widths.get(role)
            try:
                width = float(raw_width)
            except (TypeError, ValueError):
                projected[role] = None
                continue
            if not math.isfinite(width) or width < 0:
                projected[role] = None
                continue
            projected[role] = int(math.ceil(role_rows[role] * width))
        return projected

    def _capacity_structural_coverage(
        self,
        *,
        workload: Phase1EWorkloadProjection,
        style_family: str,
    ) -> tuple[bool, tuple[str, ...]]:
        request = self._capacity_request
        receipt = self._capacity_receipt
        reasons: list[str] = []
        if request.query_registry_hash != receipt.query_registry_hash:
            reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
        if not (request.history_start_trade_date <= workload.decision_trade_date <= request.history_end_trade_date):
            reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
        if request.program_count_by_style.get(style_family, 0) < workload.program_count:
            reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
        if request.candidate_depth_by_program.get(style_family, 0) < workload.candidate_depth:
            reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
        if not set(workload.horizons).issubset(set(request.horizons)):
            reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
        if request.projection_count < workload.projection_count or request.stage_projection_factor < workload.stage_projection_factor:
            reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
        if (
            request.universe_size_p50 < workload.universe_size_p50
            or request.universe_size_p95 < workload.universe_size_p95
            or request.universe_size_max < workload.universe_size_max
        ):
            reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
        tiers = receipt.role_projection_summary.get("tiers") if isinstance(receipt.role_projection_summary, dict) else None
        max_rows = _model_payload(tiers).get("max", {}).get("role_rows", {}) if isinstance(tiers, dict) else {}
        for role in CAPACITY_LOGICAL_ROLES:
            try:
                rows_covered = int(max_rows.get(role, -1)) >= workload.role_rows_max[role]
            except (TypeError, ValueError):
                rows_covered = False
            if not rows_covered:
                reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
                break
        return (not reasons, _sorted_strings(reasons))

    def _capacity_coverage(self, *, workload: Phase1EWorkloadProjection, style_family: str) -> tuple[bool, tuple[str, ...]]:
        """Require fully measured role-row and byte dominance before calling capacity covered."""

        structurally_covered, structural_reasons = self._capacity_structural_coverage(
            workload=workload,
            style_family=style_family,
        )
        reasons = list(structural_reasons)
        receipt = self._capacity_receipt
        if receipt.status is not CapacityStatus.MEASURED:
            reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
        tiers = receipt.role_projection_summary.get("tiers") if isinstance(receipt.role_projection_summary, dict) else None
        max_projection = _model_payload(tiers).get("max", {}) if isinstance(tiers, dict) else {}
        logical_bytes = max_projection.get("logical_uncompressed_bytes", {}) if isinstance(max_projection, dict) else {}
        parquet_bytes = max_projection.get("projected_parquet_bytes_by_role", {}) if isinstance(max_projection, dict) else {}
        for role in CAPACITY_LOGICAL_ROLES:
            required_logical = workload.role_logical_bytes_max[role]
            required_parquet = workload.role_parquet_bytes_max[role]
            if required_logical is None or required_parquet is None:
                reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
                break
            try:
                logical_covered = int(logical_bytes.get(role, -1)) >= required_logical
                parquet_covered = int(parquet_bytes.get(role, -1)) >= required_parquet
            except (TypeError, ValueError):
                logical_covered = parquet_covered = False
            if not logical_covered or not parquet_covered:
                reasons.append(REASON_CAPACITY_WORKLOAD_NOT_COVERED)
                break
        return (structurally_covered and not reasons, _sorted_strings(reasons))

    def _bounded_staging_capture_allowed(
        self,
        *,
        workload: Phase1EWorkloadProjection,
        style_family: str,
    ) -> bool:
        """Permit only the documented Phase 1G/1H staging path for Parquet-store measurement gaps."""

        structurally_covered, _ = self._capacity_structural_coverage(
            workload=workload,
            style_family=style_family,
        )
        if not structurally_covered or self._capacity_receipt.status is not CapacityStatus.PARTIAL:
            return False
        return bool(self._capacity_receipt.missing_measurements) and all(
            self._is_staging_measurement_gap(value) for value in self._capacity_receipt.missing_measurements
        )

    @staticmethod
    def _is_staging_measurement_gap(value: str) -> bool:
        normalized = str(value).strip().lower()
        return normalized.startswith(
            (
                "parquet_bytes_per_row_p95:",
                "parquet_role_measurement:",
                "parquet_metadata:",
                "snapshot_file_measurement:",
                "changed_partition_ratio:",
            )
        )

    def _build_capture_plan(
        self,
        *,
        binding: Phase1EEvidenceBinding,
        scope: HandoffAdmissionScope,
        program_date: Phase1EProgramDateRequest,
        evidence: Phase1EProgramDateEvidence,
        audit_outcome: Phase1EAuditOutcome,
        target: Any,
        source_result: SourceResolutionResult,
        universe_policy_hash: str,
    ) -> tuple[CapturePlan | None, tuple[dict[str, Any], ...]]:
        """Build a real capture plan only when every existing typed field is present."""

        if not source_result.can_create_capture_plan or source_result.source_revision_set is None:
            return None, (
                _output_slot(
                    slot="signal_source_revision_set",
                    source_type="source_resolution_output",
                    producer_operation="phase1e_source_resolution",
                    hash_validation="source_revision_set",
                ),
            )
        authority = next(
            (item for item in target.candidate_authority if item.decision_date == program_date.decision_trade_date),
            None,
        )
        runtime = next(
            (item for item in target.runtime_semantics if item.decision_date == program_date.decision_trade_date),
            None,
        )
        hmm = next(
            (item for item in target.hmm_vintages if item.decision_date == program_date.decision_trade_date),
            None,
        )
        risk = next(
            (item for item in target.risk_policy_evidence if item.decision_date == program_date.decision_trade_date),
            None,
        )
        clock = authority.decision_clock if authority is not None else None
        semantics = scope.stable_signal_semantics_payload_v1 or {}
        dse_payload = getattr(evidence.selection_evidence, "evidence_payload_json", {}) or {}
        bundle = audit_outcome.handoff_bundle
        missing: list[dict[str, Any]] = []

        def required(name: str, value: Any, producer: str) -> Any:
            if value in (None, ""):
                missing.append(
                    _output_slot(
                        slot=name,
                        source_type="immutable_audit_evidence",
                        producer_operation=producer,
                        hash_validation="exact_immutable_readback",
                    )
                )
            return value

        selection_run_id = required("selection_run_id", authority.selection_run_id if authority is not None else None, "candidate_authority")
        selection_run_hash = required("selection_run_content_hash", authority.selection_run_content_hash if authority is not None else None, "candidate_authority")
        artifact_id = required("selection_score_artifact_id", authority.selection_score_artifact_id if authority is not None else None, "candidate_authority")
        artifact_hash = required("selection_score_artifact_hash", authority.selection_score_artifact_sha256 if authority is not None else None, "candidate_authority")
        evidence_id = required("selection_evidence_id", authority.evidence_id if authority is not None else None, "candidate_authority")
        evidence_hash = required("selection_evidence_hash", authority.daily_selection_evidence_hash if authority is not None else None, "candidate_authority")
        decision_cutoff = required("decision_cutoff_ts", clock.decision_cutoff_ts if clock is not None else None, "decision_clock")
        selection_date = required("selection_as_of_trade_date", clock.selection_as_of_trade_date if clock is not None else None, "decision_clock")
        target_date = required("target_trade_date", clock.target_trade_date if clock is not None else None, "decision_clock")
        effective_cutoff = required("effective_cutoff_date", clock.effective_cutoff_date if clock is not None else None, "decision_clock")
        runtime_semantics_hash = required("selection_runtime_semantics_hash", semantics.get("selection_runtime_semantics_hash"), "stable_signal_semantics")
        package_config_hash = required("package_effective_config_hash", semantics.get("package_effective_config_hash"), "stable_signal_semantics")
        calendar_version = required("calendar_version", clock.calendar_version if clock is not None else None, "decision_clock")
        calendar_hash = required("calendar_hash", semantics.get("calendar_hash") or (clock.calendar_hash if clock is not None else None), "stable_signal_semantics")
        stable_hash = required("stable_signal_semantics_hash", scope.stable_signal_semantics_hash, "handoff_scope")
        signal_context_hash = required("phase0a_signal_context_hash", scope.phase0a_signal_context_hash, "handoff_scope")
        evidence_bundle_hash = required("evidence_bundle_hash", bundle.phase1_handoff_bundle_hash if bundle is not None else None, "phase0a_handoff_bundle")
        runtime_profile_id = required("runtime_profile_version_id", runtime.runtime_profile_version_id if runtime is not None else None, "runtime_semantics")
        runtime_profile_hash = required("runtime_profile_version_hash", runtime.runtime_profile_hash if runtime is not None else None, "runtime_semantics")
        risk_hash = required("risk_policy_hash", risk.risk_policy_hash if risk is not None else None, "risk_policy")
        symbol_policy_hash = required("symbol_normalization_policy_hash", dse_payload.get("symbol_normalization_policy_hash"), "daily_selection_evidence")
        available_at = required("evidence_available_at", getattr(evidence.selection_evidence, "created_at", None), "daily_selection_evidence")

        hmm_status = hmm.status if hmm is not None else None
        hmm_id = None
        hmm_hash = None
        if hmm_status is None:
            missing.append(
                _output_slot(
                    slot="hmm_snapshot_status",
                    source_type="immutable_audit_evidence",
                    producer_operation="hmm_vintage",
                    hash_validation="exact_immutable_readback",
                )
            )
        elif hmm_status != "NOT_APPLICABLE":
            hmm_id = required("hmm_snapshot_id", hmm.model_snapshot_id, "hmm_vintage")
            hmm_hash = required("hmm_snapshot_hash", hmm.model_artifact_sha256, "hmm_vintage")
        if missing:
            return None, tuple(missing)
        try:
            plan = CapturePlan(
                selection_run_id=str(selection_run_id),
                package_id=binding.package_id,
                manifest_sha256=binding.manifest_sha256,
                decision_as_of_trade_date=program_date.decision_trade_date.isoformat(),
                selection_as_of_trade_date=selection_date.isoformat(),
                target_trade_date=target_date.isoformat(),
                decision_cutoff_ts=decision_cutoff,
                alpha_mode=binding.alpha_mode,
                selection_runtime_semantics_hash=str(runtime_semantics_hash),
                package_effective_config_hash=str(package_config_hash),
                calendar_version=str(calendar_version),
                calendar_hash=str(calendar_hash),
                stable_signal_semantics_hash=str(stable_hash),
                canonical_signal_scope_hash=str(signal_context_hash),
                phase0a_audit_id=binding.phase0a_audit_id,
                phase0a_audit_manifest_hash=binding.phase0a_audit_manifest_hash,
                handoff_readiness_hash=binding.handoff_readiness_report_hash,
                admission_scope_id=scope.admission_scope_id,
                admission_scope_hash=scope.admission_scope_hash,
                signal_source_revision_set_id=str(source_result.source_revision_set.source_revision_set_id),
                signal_source_revision_set_hash=str(source_result.source_revision_set.source_revision_set_hash),
                phase0a_signal_context_hash=str(signal_context_hash),
                evidence_bundle_hash=str(evidence_bundle_hash),
                selection_evidence_id=str(evidence_id),
                selection_evidence_hash=str(evidence_hash),
                selection_run_content_hash=str(selection_run_hash),
                selection_score_artifact_id=str(artifact_id),
                selection_score_artifact_hash=str(artifact_hash),
                runtime_profile_version_id=str(runtime_profile_id),
                runtime_profile_version_hash=str(runtime_profile_hash),
                hmm_snapshot_id=str(hmm_id) if hmm_id is not None else None,
                hmm_snapshot_hash=str(hmm_hash) if hmm_hash is not None else None,
                hmm_snapshot_status=str(hmm_status),
                risk_policy_hash=str(risk_hash),
                universe_policy_hash=universe_policy_hash,
                symbol_normalization_policy_hash=str(symbol_policy_hash),
                valid_no_candidate=(getattr(evidence.historical_program_run, "candidate_outcome", None) == "VALID_NO_CANDIDATE"),
                evidence_available_at=available_at,
                audit_target_id=target.audit_target_id,
                target_scope_hash=scope.target_scope_hash,
                capability=scope.capability,
                oos_interval_id=scope.oos_interval_id,
                oos_interval_hash=scope.oos_interval_hash,
                evidence_scope=scope.evidence_scope.value,
                signal_evidence_level=scope.signal_evidence_level.value,
                effective_cutoff_date=effective_cutoff.isoformat(),
                program_id=program_date.program_id,
                binding_version_id=binding.binding_version_id,
                source_run_id=binding.historical_program_run_id,
                lineage_source_type="PHASE0A_AUDIT",
            )
        except (TypeError, ValueError) as exc:
            raise Phase1EError(
                REASON_AUDIT_HANDOFF_MISMATCH,
                "authoritative evidence could not satisfy the existing CapturePlan contract",
                context={"message": str(exc)},
            ) from exc
        return plan, ()

    def _operation_scope_context(
        self,
        *,
        request: Phase1ERevalidationBatchRequest,
        program_date: Phase1EProgramDateRequest,
        binding: Phase1EEvidenceBinding,
        scope: HandoffAdmissionScope,
        evidence_request_hash: str,
    ) -> dict[str, Any]:
        """All known scope semantics carried into every downstream request template."""

        return {
            "program_id": program_date.program_id,
            "decision_trade_date": program_date.decision_trade_date,
            "label_as_of_ts": program_date.label_as_of_ts,
            "evidence_request_hash": evidence_request_hash,
            "evidence_binding_hash": binding.evidence_binding_hash,
            "historical_program_run_id": binding.historical_program_run_id,
            "package_id": binding.package_id,
            "manifest_sha256": binding.manifest_sha256,
            "alpha_mode": binding.alpha_mode,
            "phase0a_audit_id": binding.phase0a_audit_id,
            "phase0a_audit_manifest_hash": binding.phase0a_audit_manifest_hash,
            "handoff_readiness_hash": binding.handoff_readiness_report_hash,
            "admission_scope_id": scope.admission_scope_id,
            "admission_scope_hash": scope.admission_scope_hash,
            "target_scope_hash": scope.target_scope_hash,
            "oos_interval_hash": scope.oos_interval_hash,
            "evidence_scope": scope.evidence_scope.value,
            "batch_contract": {
                "phase0a_policy_hash": request.phase0a_policy_hash,
                "query_registry_hash": request.query_registry_hash,
                "calendar_hash": request.calendar_hash,
                "label_policy_bundle_hash": request.label_policy_bundle_hash,
                "dataset_schema_fingerprint": request.dataset_schema_fingerprint,
                "partition_policy_hash": request.partition_policy_hash,
                "store_backend_config_hash": request.store_backend_config_hash,
                "capacity_request_hash": self._capacity_request.request_hash,
                "capacity_receipt_hash": self._capacity_receipt.receipt_hash,
                "compiler_source_hash": request.compiler_source_hash,
                "artifact_store_policy_hash": request.artifact_store_policy_hash,
            },
        }

    def _operations_for_resolution(
        self,
        *,
        binding: Phase1EEvidenceBinding,
        scope: HandoffAdmissionScope,
        scope_context: dict[str, Any],
        requirements: SourceRequirementSet,
        source_result: SourceResolutionResult,
        capture_plan: CapturePlan | None,
        capture_missing: tuple[dict[str, Any], ...],
        workload_covered: bool,
        bounded_staging_capture_allowed: bool,
    ) -> tuple[Phase1EPlannedOperation, ...]:
        source_payload = {
            "schema_version": PHASE1E_SOURCE_RESOLUTION_OPERATION_SCHEMA_VERSION,
            "scope_context": scope_context,
            "source_requirement_set": requirements.model_dump(mode="json"),
            "source_requirement_set_id": source_result.receipt.source_requirement_set_id,
            "source_requirement_set_hash": source_result.receipt.source_requirement_set_hash,
            "source_resolution_receipt": source_result.receipt.model_dump(mode="json"),
        }
        source_operation = Phase1EPlannedOperation(
            operation_type=PlannedOperationType.SOURCE_RESOLUTION,
            operation_disposition=OperationDisposition.COMPLETE_REQUEST,
            contract_schema_version=PHASE1E_SOURCE_RESOLUTION_OPERATION_SCHEMA_VERSION,
            complete_request_payload=source_payload,
            complete_request_hash=canonical_json_sha256(source_payload),
            resolved_input_refs=(
                {"source_resolution_receipt_hash": source_result.receipt.source_resolution_receipt_hash},
            ),
            resource_budget_ref=str(scope_context["batch_contract"]["capacity_receipt_hash"]),
        )
        if not source_result.can_create_capture_plan:
            return (source_operation, *self._template_operations(
                source_payload={
                    "schema_version": PHASE1E_TEMPLATE_SCHEMA_VERSION,
                    "scope_context": scope_context,
                    "source_resolution": {
                        "source_requirement_set_id": source_result.receipt.source_requirement_set_id,
                        "source_requirement_set_hash": source_result.receipt.source_requirement_set_hash,
                        "source_resolution_receipt_hash": source_result.receipt.source_resolution_receipt_hash,
                    },
                    "reason": REASON_REQUEST_TEMPLATE_INCOMPLETE,
                    "required_source_revision_set": True,
                    "capture_plan": None,
                },
                source_slots=(
                    _output_slot(
                        slot="source_revision_set",
                        source_type="source_resolution_output",
                        producer_operation="phase1e_source_resolution",
                        hash_validation="source_revision_set",
                    ),
                ),
                deferred=True,
                omit_source=True,
            ))
        control_slots = (
            _output_slot(
                slot="control_binding_event_hash",
                source_type="versioned_control_binding_event",
                producer_operation="phase1g_control_binding",
                hash_validation="exact_event",
            ),
            _output_slot(
                slot="capture_batch_id",
                source_type="observation_capture_request_identity",
                producer_operation="phase1g_observation_capture",
                hash_validation="typed_request",
            ),
            _output_slot(
                slot="capture_fencing_token",
                source_type="observation_capture_request_identity",
                producer_operation="phase1g_observation_capture",
                hash_validation="typed_request",
            ),
        )
        if capture_plan is None:
            unresolved = tuple([*capture_missing, *control_slots])
            return (source_operation, *self._template_operations(
                source_payload={
                    "schema_version": PHASE1E_TEMPLATE_SCHEMA_VERSION,
                    "scope_context": scope_context,
                    "source_resolution": {
                        "source_requirement_set_id": source_result.receipt.source_requirement_set_id,
                        "source_requirement_set_hash": source_result.receipt.source_requirement_set_hash,
                        "source_resolution_receipt_hash": source_result.receipt.source_resolution_receipt_hash,
                        "signal_source_revision_set_id": source_result.receipt.source_revision_set_id,
                        "signal_source_revision_set_hash": source_result.receipt.source_revision_set_hash,
                    },
                    "capture_plan": None,
                },
                source_slots=unresolved,
                deferred=True,
                omit_source=True,
            ))
        return (source_operation, *self._template_operations(
            source_payload={
                "schema_version": PHASE1E_TEMPLATE_SCHEMA_VERSION,
                "scope_context": scope_context,
                "source_resolution": {
                    "source_requirement_set_id": source_result.receipt.source_requirement_set_id,
                    "source_requirement_set_hash": source_result.receipt.source_requirement_set_hash,
                    "source_resolution_receipt_hash": source_result.receipt.source_resolution_receipt_hash,
                    "signal_source_revision_set_id": source_result.receipt.source_revision_set_id,
                    "signal_source_revision_set_hash": source_result.receipt.source_revision_set_hash,
                },
                "capture_plan": capture_plan.model_dump(mode="json"),
                "capture_plan_hash": capture_plan.plan_hash,
            },
            source_slots=control_slots,
            deferred=not (workload_covered or bounded_staging_capture_allowed),
            omit_source=True,
        ))

    def _template_operations(
        self,
        *,
        source_payload: dict[str, Any],
        source_slots: tuple[dict[str, Any], ...],
        deferred: bool,
        omit_source: bool = False,
    ) -> tuple[Phase1EPlannedOperation, ...]:
        disposition = OperationDisposition.DEFERRED if deferred else OperationDisposition.SEMANTIC_TEMPLATE
        operations: list[Phase1EPlannedOperation] = []
        scope_context = source_payload.get("scope_context")
        scope_context = scope_context if isinstance(scope_context, dict) else {}
        batch_contract = scope_context.get("batch_contract")
        batch_contract = batch_contract if isinstance(batch_contract, dict) else {}
        resource_budget_ref = str(batch_contract.get("capacity_receipt_hash") or "").strip() or None
        source_resolution = source_payload.get("source_resolution")
        source_resolution = source_resolution if isinstance(source_resolution, dict) else {}
        capture_plan = source_payload.get("capture_plan")
        if not omit_source:
            operations.append(
                Phase1EPlannedOperation(
                    operation_type=PlannedOperationType.SOURCE_RESOLUTION,
                    operation_disposition=disposition,
                    contract_schema_version=PHASE1E_TEMPLATE_SCHEMA_VERSION,
                    request_template_payload=source_payload,
                    request_template_hash=canonical_json_sha256(source_payload),
                    required_output_slots=source_slots,
                    unresolved_input_refs=source_slots,
                    resource_budget_ref=resource_budget_ref,
                )
            )
        observation_payload = {
            "schema_version": PHASE1E_TEMPLATE_SCHEMA_VERSION,
            "operation": "observation_capture",
            "scope_context": scope_context,
            "source_resolution": source_resolution,
            "capture_plan": capture_plan,
            "required_inputs": source_slots,
        }
        operations.append(
            Phase1EPlannedOperation(
                operation_type=PlannedOperationType.OBSERVATION_CAPTURE,
                operation_disposition=disposition,
                contract_schema_version=PHASE1E_TEMPLATE_SCHEMA_VERSION,
                request_template_payload=observation_payload,
                request_template_hash=canonical_json_sha256(observation_payload),
                required_output_slots=source_slots,
                unresolved_input_refs=source_slots,
                resource_budget_ref=resource_budget_ref,
            )
        )
        label_slots = (
            _output_slot(slot="source_observation_capture_batch_id", source_type="observation_capture_output", producer_operation="phase1g_observation_capture", hash_validation="capture_batch"),
            _output_slot(slot="source_observation_capture_receipt_hash", source_type="observation_capture_output", producer_operation="phase1g_observation_capture", hash_validation="capture_receipt"),
            _output_slot(slot="source_observation_membership_hash", source_type="observation_capture_output", producer_operation="phase1g_observation_capture", hash_validation="membership_set"),
            _output_slot(slot="source_observation_capture_plan_set_count", source_type="observation_capture_output", producer_operation="phase1g_observation_capture", hash_validation="count"),
            _output_slot(slot="source_observation_capture_plan_set_hash", source_type="observation_capture_output", producer_operation="phase1g_observation_capture", hash_validation="plan_set"),
            _output_slot(slot="selected_observation_mappings", source_type="observation_capture_output", producer_operation="phase1g_observation_capture", hash_validation="mapping_set"),
            _output_slot(slot="label_capture_batch_id", source_type="label_capture_request_identity", producer_operation="phase1h_label_capture", hash_validation="capture_batch"),
            _output_slot(slot="label_capture_fencing_token", source_type="label_capture_request_identity", producer_operation="phase1h_label_capture", hash_validation="typed_request"),
            _output_slot(slot="label_policy_bundle_id", source_type="versioned_label_policy", producer_operation="phase1h_frozen_policy_resolution", hash_validation="policy_registry"),
            _output_slot(slot="label_source_revision_set_id", source_type="label_source_resolution_output", producer_operation="phase1h_label_source_resolution", hash_validation="revision_set"),
            _output_slot(slot="label_source_revision_set_hash", source_type="label_source_resolution_output", producer_operation="phase1h_label_source_resolution", hash_validation="revision_set"),
            _output_slot(slot="planned_label_descriptors", source_type="label_planning_output", producer_operation="phase1h_label_planning", hash_validation="typed_contract"),
            _output_slot(slot="planned_label_hash", source_type="label_planning_output", producer_operation="phase1h_label_planning", hash_validation="typed_contract"),
        )
        label_required_slots = tuple([*source_slots, *label_slots])
        label_payload = {
            "schema_version": PHASE1E_TEMPLATE_SCHEMA_VERSION,
            "operation": "label_capture",
            "scope_context": scope_context,
            "source_resolution": source_resolution,
            "capture_plan": capture_plan,
            "known_label_policy_bundle_hash": batch_contract.get("label_policy_bundle_hash"),
            "required_inputs": label_slots,
        }
        operations.append(
            Phase1EPlannedOperation(
                operation_type=PlannedOperationType.LABEL_CAPTURE,
                operation_disposition=OperationDisposition.DEFERRED,
                contract_schema_version=PHASE1E_TEMPLATE_SCHEMA_VERSION,
                request_template_payload=label_payload,
                request_template_hash=canonical_json_sha256(label_payload),
                required_output_slots=label_required_slots,
                unresolved_input_refs=label_required_slots,
                resource_budget_ref=resource_budget_ref,
            )
        )
        dataset_slots = (
            _output_slot(slot="observation_capture_receipt", source_type="observation_capture_output", producer_operation="phase1g_observation_capture", hash_validation="capture_receipt"),
            _output_slot(slot="label_capture_receipt", source_type="label_capture_output", producer_operation="phase1h_label_capture", hash_validation="capture_receipt"),
            _output_slot(slot="capture_set_members", source_type="capture_set_output", producer_operation="phase1g_1h_capture_receipts", hash_validation="capture_set"),
            _output_slot(slot="selected_observation_mappings", source_type="observation_capture_output", producer_operation="phase1g_observation_capture", hash_validation="mapping_set"),
            _output_slot(slot="selected_label_mappings", source_type="label_capture_output", producer_operation="phase1h_label_capture", hash_validation="mapping_set"),
            _output_slot(slot="label_target_identities", source_type="label_capture_output", producer_operation="phase1h_label_capture", hash_validation="label_targets"),
            _output_slot(slot="composite_capability_requirements", source_type="dataset_contract_output", producer_operation="phase1i_dataset_contract", hash_validation="typed_contract"),
            _output_slot(slot="snapshot_schema_version", source_type="dataset_contract_output", producer_operation="phase1i_dataset_contract", hash_validation="typed_contract"),
            _output_slot(slot="compression_config", source_type="dataset_contract_output", producer_operation="phase1i_dataset_contract", hash_validation="typed_contract"),
            _output_slot(slot="dataset_policy_compatibility_hash", source_type="dataset_contract_output", producer_operation="phase1i_dataset_contract", hash_validation="typed_contract"),
            _output_slot(slot="partition_policy_id", source_type="dataset_contract_output", producer_operation="phase1i_dataset_contract", hash_validation="typed_contract"),
            _output_slot(slot="builder_writer_code_identity", source_type="dataset_contract_output", producer_operation="phase1i_dataset_contract", hash_validation="typed_contract"),
        )
        dataset_required_slots = tuple([*label_required_slots, *dataset_slots])
        dataset_payload = {
            "schema_version": PHASE1E_TEMPLATE_SCHEMA_VERSION,
            "operation": "dataset_build",
            "scope_context": scope_context,
            "source_resolution": source_resolution,
            "capture_plan": capture_plan,
            "required_inputs": dataset_slots,
        }
        operations.append(
            Phase1EPlannedOperation(
                operation_type=PlannedOperationType.DATASET_BUILD,
                operation_disposition=OperationDisposition.DEFERRED,
                contract_schema_version=PHASE1E_TEMPLATE_SCHEMA_VERSION,
                request_template_payload=dataset_payload,
                request_template_hash=canonical_json_sha256(dataset_payload),
                required_output_slots=dataset_required_slots,
                unresolved_input_refs=dataset_required_slots,
                resource_budget_ref=resource_budget_ref,
            )
        )
        store_slots = (
            _output_slot(slot="dataset_build_receipt", source_type="dataset_build_output", producer_operation="phase1i_dataset_build", hash_validation="build_receipt"),
            _output_slot(slot="dataset_snapshot_manifest", source_type="dataset_build_output", producer_operation="phase1i_dataset_build", hash_validation="snapshot_manifest"),
            _output_slot(slot="dataset_snapshot_content_hash", source_type="dataset_build_output", producer_operation="phase1i_dataset_build", hash_validation="snapshot_content"),
            _output_slot(slot="sealed_parquet_measurements", source_type="dataset_build_output", producer_operation="phase1i_dataset_build", hash_validation="measurement_receipt"),
            _output_slot(slot="durable_store_destination", source_type="durable_store_output", producer_operation="phase1i_store_resolution", hash_validation="store_policy"),
            _output_slot(slot="durable_store_publish_receipt", source_type="durable_store_output", producer_operation="phase1i_store_publish", hash_validation="publish_receipt"),
        )
        store_required_slots = tuple([*dataset_required_slots, *store_slots])
        store_payload = {
            "schema_version": PHASE1E_TEMPLATE_SCHEMA_VERSION,
            "operation": "durable_store_publish",
            "scope_context": scope_context,
            "source_resolution": source_resolution,
            "capture_plan": capture_plan,
            "required_inputs": store_slots,
        }
        operations.append(
            Phase1EPlannedOperation(
                operation_type=PlannedOperationType.DURABLE_STORE_PUBLISH,
                operation_disposition=OperationDisposition.DEFERRED,
                contract_schema_version=PHASE1E_TEMPLATE_SCHEMA_VERSION,
                request_template_payload=store_payload,
                request_template_hash=canonical_json_sha256(store_payload),
                required_output_slots=store_required_slots,
                unresolved_input_refs=store_required_slots,
                resource_budget_ref=resource_budget_ref,
            )
        )
        return tuple(operations)

    def _resource_budget_by_role(self) -> dict[str, Any]:
        tiers = self._capacity_receipt.role_projection_summary.get("tiers", {})
        return {
            role: {
                tier: {
                    "rows": (tiers.get(tier, {}).get("role_rows", {}).get(role)),
                    "logical_bytes": (tiers.get(tier, {}).get("logical_uncompressed_bytes", {}).get(role)),
                    "parquet_bytes": (tiers.get(tier, {}).get("projected_parquet_bytes_by_role", {}).get(role)),
                }
                for tier in ("p50", "p95", "max")
            }
            for role in CAPACITY_LOGICAL_ROLES
        }

    def _publish_plan(self, plan: Phase1EExecutionPlan) -> None:
        if self._artifact_store is None:
            return
        self._artifact_store.publish(
            kind="plan",
            identity=str(plan.plan_hash),
            payload=plan.model_dump(mode="json"),
            semantic_hash=str(plan.plan_hash),
        )

    @staticmethod
    def _batch_receipt(
        *,
        batch_request_hash: str,
        plans: list[Phase1EExecutionPlan],
        failures: list[Phase1EFailedInputScope],
    ) -> Phase1EPlanBatchReceipt:
        def count(values: Iterable[str]) -> dict[str, int]:
            result: dict[str, int] = {}
            for value in values:
                result[value] = result.get(value, 0) + 1
            return dict(sorted(result.items()))
        admission = [plan for plan in plans if plan.plan_unit_kind is PlanUnitKind.ADMISSION_SCOPE]
        return Phase1EPlanBatchReceipt(
            batch_request_hash=batch_request_hash,
            sorted_scope_plan_request_hashes=tuple(plan.scope_plan_request_hash for plan in plans),
            sorted_scope_plan_hashes=tuple(str(plan.plan_hash) for plan in plans),
            counts_by_plan_unit_kind=count(plan.plan_unit_kind.value for plan in plans),
            all_scope_workloads_covered=bool(admission) and all(bool(plan.capacity_workload_covered) for plan in admission),
            counts_by_handoff_readiness=count(plan.handoff_readiness.value for plan in plans),
            counts_by_source_readiness=count(plan.source_readiness.value for plan in plans if plan.source_readiness is not None),
            counts_by_capacity_status=count(plan.capacity_status.value for plan in plans if plan.capacity_status is not None),
            failed_input_scopes=tuple(failures),
        )


def target_scope_hash(target: Any) -> str:
    return canonical_json_sha256(
        {
            "audit_target_id": target.audit_target_id,
            "program_id": target.program_id,
            "package_id": target.package_id,
            "manifest_sha256": target.manifest_sha256,
        }
    )
