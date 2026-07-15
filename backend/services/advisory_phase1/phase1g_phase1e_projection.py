"""Pure Phase 1E plan consumer projection for Phase 1G.

The projection validates the immutable Phase 1E envelope and the exact fields
consumed by Phase 1G without importing the Phase 1E compiler or its database
backed capacity services.
"""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize


PHASE1E_PLAN_SCHEMA_VERSION = "advisory_phase1e_execution_plan_v1"


class Phase1EPlanUnitKind(str, Enum):
    TARGET_DIAGNOSTIC = "TARGET_DIAGNOSTIC"
    ADMISSION_SCOPE = "ADMISSION_SCOPE"


class Phase1EPlannedOperationType(str, Enum):
    SOURCE_RESOLUTION = "SOURCE_RESOLUTION"
    OBSERVATION_CAPTURE = "OBSERVATION_CAPTURE"
    LABEL_CAPTURE = "LABEL_CAPTURE"
    DATASET_BUILD = "DATASET_BUILD"
    DURABLE_STORE_PUBLISH = "DURABLE_STORE_PUBLISH"


class Phase1EOperationDisposition(str, Enum):
    COMPLETE_REQUEST = "COMPLETE_REQUEST"
    SEMANTIC_TEMPLATE = "SEMANTIC_TEMPLATE"
    DEFERRED = "DEFERRED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class _StrictProjection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field_name} must be lowercase sha256")
    return normalized


def _canonical_dicts(values: tuple[dict[str, Any], ...], *, field_name: str) -> tuple[dict[str, Any], ...]:
    normalized = tuple(sorted((canonicalize(value) for value in values), key=canonical_json_sha256))
    if len({canonical_json_sha256(value) for value in normalized}) != len(normalized):
        raise ValueError(f"{field_name} must not contain duplicates")
    return normalized


class Phase1EEvidenceBindingProjection(_StrictProjection):
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
    evidence_binding_hash: str = Field(min_length=64, max_length=64)

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

    @model_validator(mode="after")
    def _validate_binding(self) -> "Phase1EEvidenceBindingProjection":
        component_ids = tuple(sorted(str(value).strip() for value in self.manifest_alpha_component_ids))
        if any(not value for value in component_ids) or len(component_ids) != len(set(component_ids)):
            raise ValueError("manifest alpha component identities must be non-empty and unique")
        if self.alpha_mode == "single_alpha" and component_ids:
            raise ValueError("single Alpha evidence binding cannot carry parent-leg identities")
        object.__setattr__(self, "manifest_alpha_component_ids", component_ids)
        payload = self.model_dump(mode="python", exclude={"evidence_binding_hash"})
        if canonical_json_sha256(payload) != self.evidence_binding_hash:
            raise ValueError("evidence_binding_hash does not match evidence binding")
        return self


class Phase1EPlannedOperationProjection(_StrictProjection):
    operation_type: Phase1EPlannedOperationType
    operation_disposition: Phase1EOperationDisposition
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

    @model_validator(mode="after")
    def _validate_operation(self) -> "Phase1EPlannedOperationProjection":
        complete = self.complete_request_payload
        template = self.request_template_payload
        if complete is not None:
            complete = canonicalize(complete)
            object.__setattr__(self, "complete_request_payload", complete)
        if template is not None:
            template = canonicalize(template)
            object.__setattr__(self, "request_template_payload", template)
        object.__setattr__(
            self,
            "required_output_slots",
            _canonical_dicts(self.required_output_slots, field_name="required_output_slots"),
        )
        object.__setattr__(
            self, "resolved_input_refs", _canonical_dicts(self.resolved_input_refs, field_name="resolved_input_refs")
        )
        object.__setattr__(
            self,
            "unresolved_input_refs",
            _canonical_dicts(self.unresolved_input_refs, field_name="unresolved_input_refs"),
        )
        if self.operation_disposition is Phase1EOperationDisposition.COMPLETE_REQUEST:
            if (
                complete is None
                or self.complete_request_hash is None
                or template is not None
                or self.request_template_hash is not None
            ):
                raise ValueError("complete operation must carry only one complete payload/hash")
            if canonical_json_sha256(complete) != self.complete_request_hash:
                raise ValueError("complete_request_hash does not match complete payload")
            if self.required_output_slots or self.unresolved_input_refs:
                raise ValueError("complete operation cannot retain unresolved output slots")
        elif self.operation_disposition in {
            Phase1EOperationDisposition.SEMANTIC_TEMPLATE,
            Phase1EOperationDisposition.DEFERRED,
        }:
            if (
                template is None
                or self.request_template_hash is None
                or complete is not None
                or self.complete_request_hash is not None
            ):
                raise ValueError("template operation must carry only one template payload/hash")
            if canonical_json_sha256(template) != self.request_template_hash:
                raise ValueError("request_template_hash does not match template payload")
            if (
                self.operation_disposition is Phase1EOperationDisposition.SEMANTIC_TEMPLATE
                and not self.required_output_slots
            ):
                raise ValueError("semantic template must enumerate output slots")
        elif (
            any(
                value is not None
                for value in (complete, self.complete_request_hash, template, self.request_template_hash)
            )
            or self.required_output_slots
            or self.unresolved_input_refs
        ):
            raise ValueError("not-applicable operation cannot carry request data")
        return self


class Phase1EExecutionPlanProjection(_StrictProjection):
    schema_version: str
    evidence_request_hash: str
    scope_plan_request_hash: str
    compiler_version: str
    serializer_version: str
    compiler_source_hash: str
    plan_unit_kind: Phase1EPlanUnitKind
    scope_key: dict[str, Any] | None
    target_key: dict[str, Any] | None
    evidence_binding: Phase1EEvidenceBindingProjection
    handoff_readiness: str
    source_readiness: str | None
    capacity_status: str | None
    reason_codes: tuple[str, ...]
    missing_evidence: tuple[dict[str, Any], ...]
    planned_operations: tuple[Phase1EPlannedOperationProjection, ...]
    workload_projection: dict[str, Any] | None
    resource_budget_by_role: dict[str, Any] | None
    memory_budget: dict[str, Any] | None
    temporary_store_budget: dict[str, Any] | None
    durable_store_budget: dict[str, Any] | None
    missing_capacity_measurements: tuple[str, ...]
    capacity_request_hash: str | None
    capacity_receipt_hash: str | None
    capacity_workload_covered: bool | None
    resource_values_frozen: bool | None
    research_only: bool
    execution_prohibited: bool
    plan_hash: str
    plan_id: str

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

    @model_validator(mode="after")
    def _validate_plan(self) -> "Phase1EExecutionPlanProjection":
        if (
            self.schema_version != PHASE1E_PLAN_SCHEMA_VERSION
            or not self.research_only
            or not self.execution_prohibited
        ):
            raise ValueError("Phase 1E projection requires the research-only execution-prohibited contract")
        if self.plan_unit_kind is Phase1EPlanUnitKind.ADMISSION_SCOPE:
            if (
                self.scope_key is None
                or self.target_key is not None
                or self.evidence_binding.admission_scope_id is None
                or self.evidence_binding.admission_scope_hash is None
            ):
                raise ValueError(
                    "Phase 1G admission-scope projection requires one exact scope binding"
                )
        elif self.scope_key is not None or self.target_key is None:
            raise ValueError(
                "Phase 1G target-diagnostic projection requires one exact target key"
            )
        operations = tuple(sorted(self.planned_operations, key=lambda item: item.operation_type.value))
        operation_types = tuple(item.operation_type for item in operations)
        if len(operation_types) != len(set(operation_types)):
            raise ValueError("Phase 1E projection operations must have unique types")
        object.__setattr__(self, "planned_operations", operations)
        if self.scope_key is not None:
            object.__setattr__(self, "scope_key", canonicalize(self.scope_key))
        if self.target_key is not None:
            object.__setattr__(self, "target_key", canonicalize(self.target_key))
        for field_name in (
            "workload_projection",
            "resource_budget_by_role",
            "memory_budget",
            "temporary_store_budget",
            "durable_store_budget",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, canonicalize(value))
        payload = self.model_dump(mode="json", exclude={"plan_hash", "plan_id"})
        payload["evidence_binding"].pop("evidence_binding_hash", None)
        workload = payload.get("workload_projection")
        if isinstance(workload, dict):
            workload.pop("workload_projection_hash", None)
        payload["reason_codes"] = sorted(self.reason_codes)
        payload["missing_evidence"] = sorted(
            (canonicalize(item) for item in self.missing_evidence),
            key=canonical_json_sha256,
        )
        payload["missing_capacity_measurements"] = sorted(self.missing_capacity_measurements)
        digest = canonical_json_sha256(payload)
        if self.plan_hash != digest or self.plan_id != f"p1ep_{digest[:20]}":
            raise ValueError("Phase 1E plan identity does not match its canonical projection")
        return self

    @property
    def decision_trade_date(self) -> date:
        identity = self.scope_key if self.scope_key is not None else self.target_key
        raw = identity.get("decision_trade_date") if identity else None
        return raw if isinstance(raw, date) else date.fromisoformat(str(raw))
