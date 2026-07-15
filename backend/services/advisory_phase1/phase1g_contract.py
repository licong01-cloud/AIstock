"""Typed Phase 1G execution, result, and receipt contracts.

The module is deliberately isolated from Selection, Paper, simulation, live
inference, and database writers.  It defines deterministic identities only.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel


REASON_SCHEMA_RECEIPT_INVALID = "ADVISORY_PHASE1G_SCHEMA_RECEIPT_INVALID"
REASON_SCHEMA_NOT_READY = "ADVISORY_PHASE1G_SCHEMA_NOT_READY"
REASON_INPUT_REF_INVALID = "ADVISORY_PHASE1G_INPUT_REF_INVALID"
REASON_PLAN_INVALID = "ADVISORY_PHASE1G_PLAN_INVALID"
REASON_RESULT_STORE_FAILED = "ADVISORY_PHASE1G_RESULT_STORE_FAILED"
REASON_ATTEMPT_RECEIPT_STORE_FAILED = "ADVISORY_PHASE1G_ATTEMPT_RECEIPT_STORE_FAILED"
REASON_UNEXPECTED_ERROR = "ADVISORY_PHASE1G_UNEXPECTED_ERROR"

TARGET_REQUEST_SCHEMA_VERSION = "advisory_phase1g_target_execution_request_v1"
BATCH_REQUEST_SCHEMA_VERSION = "advisory_phase1g_execution_batch_request_v1"
TARGET_PLAN_SCHEMA_VERSION = "advisory_phase1g_target_execution_plan_v1"
BATCH_PLAN_SCHEMA_VERSION = "advisory_phase1g_execution_batch_plan_v1"
CAPTURE_RESULT_SCHEMA_VERSION = "advisory_phase1g_capture_result_v1"
ATTEMPT_RECEIPT_SCHEMA_VERSION = "advisory_phase1g_execution_attempt_receipt_v1"
BATCH_RECEIPT_SCHEMA_VERSION = "advisory_phase1g_batch_attempt_receipt_v1"
INPUT_ARTIFACT_REF_SCHEMA_VERSION = "advisory_phase1g_input_artifact_ref_v1"
OUTPUT_ARTIFACT_REF_SCHEMA_VERSION = "advisory_phase1g_output_artifact_ref_v1"
STORE_LAYOUT_POLICY_SCHEMA_VERSION = "advisory_phase1g_store_layout_policy_v1"
CAPTURE_POLICY_REGISTRY_SCHEMA_VERSION = "advisory_phase1g_capture_policy_registry_v1"


class Phase1GContractError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = context or {}


class _StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field_name} must be lowercase sha256")
    return normalized


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return value.astimezone(timezone.utc)


def _sorted_unique(values: tuple[str, ...], *, field_name: str, sha256: bool = False) -> tuple[str, ...]:
    normalized = tuple(sorted(str(value).strip() for value in values))
    if any(not value for value in normalized) or len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} must be non-empty, sorted, and duplicate-free")
    if sha256:
        for value in normalized:
            _sha256(value, field_name=field_name)
    return normalized


class Phase1GInputArtifactKind(str, Enum):
    PHASE1F2_RELEASE_RECEIPT = "PHASE1F2_RELEASE_RECEIPT"
    PHASE1E_EXECUTION_PLAN = "PHASE1E_EXECUTION_PLAN"


class Phase1GOutputArtifactKind(str, Enum):
    CAPTURE_RESULT = "CAPTURE_RESULT"
    ATTEMPT_RECEIPT = "ATTEMPT_RECEIPT"
    BATCH_RECEIPT = "BATCH_RECEIPT"


class Phase1GAttemptStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Phase1GBatchStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    FAILED = "FAILED"


class Phase1GStoreLayoutPolicy(_StrictContract):
    schema_version: Literal[STORE_LAYOUT_POLICY_SCHEMA_VERSION] = STORE_LAYOUT_POLICY_SCHEMA_VERSION
    policy_id: str = Field(min_length=1, max_length=160)
    policy_version: str = Field(min_length=1, max_length=80)
    artifact_kinds: tuple[str, ...] = Field(min_length=1)
    layout_version: str = Field(min_length=1, max_length=160)
    envelope_schema_version: str | None = Field(default=None, max_length=160)
    identity_fields: tuple[str, ...] = Field(min_length=1)
    layout_policy_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("layout_policy_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="layout_policy_hash") if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"layout_policy_hash"})

    @model_validator(mode="after")
    def _validate_policy(self) -> "Phase1GStoreLayoutPolicy":
        object.__setattr__(self, "artifact_kinds", _sorted_unique(self.artifact_kinds, field_name="artifact_kinds"))
        object.__setattr__(self, "identity_fields", _sorted_unique(self.identity_fields, field_name="identity_fields"))
        digest = canonical_json_sha256(self.canonical_payload())
        if self.layout_policy_hash is not None and self.layout_policy_hash != digest:
            raise ValueError("layout_policy_hash does not match store layout policy")
        object.__setattr__(self, "layout_policy_hash", digest)
        return self


PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY = Phase1GStoreLayoutPolicy(
    policy_id="ADVISORY_PHASE1F2_RELEASE_RECEIPT_STORE",
    policy_version="1",
    artifact_kinds=(Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT.value,),
    layout_version="receipts_by_receipt_content_hash_v1",
    identity_fields=("receipt_content_hash",),
)

PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY = Phase1GStoreLayoutPolicy(
    policy_id="ADVISORY_PHASE1E_EXECUTION_PLAN_STORE",
    policy_version="1",
    artifact_kinds=(Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN.value,),
    layout_version="phase1e_plan_hash_prefix_v1",
    envelope_schema_version="advisory_phase1e_artifact_envelope_v1",
    identity_fields=("payload.plan_hash", "semantic_hash"),
)

PHASE1G_RESULT_STORE_LAYOUT_POLICY = Phase1GStoreLayoutPolicy(
    policy_id="ADVISORY_PHASE1G_RESULT_STORE",
    policy_version="1",
    artifact_kinds=tuple(item.value for item in Phase1GOutputArtifactKind),
    layout_version="phase1g_semantic_hash_prefix_v1",
    identity_fields=("attempt_receipt_hash", "batch_attempt_receipt_hash", "capture_result_hash"),
)


class Phase1GComponentContract(_StrictContract):
    component_name: str = Field(min_length=1, max_length=160)
    contract_version: str = Field(min_length=1, max_length=160)
    contract_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("contract_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="contract_hash") if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"contract_hash"})

    @model_validator(mode="after")
    def _validate_contract(self) -> "Phase1GComponentContract":
        digest = canonical_json_sha256(self.canonical_payload())
        if self.contract_hash is not None and self.contract_hash != digest:
            raise ValueError("contract_hash does not match component contract")
        object.__setattr__(self, "contract_hash", digest)
        return self


class Phase1GCapturePolicyRegistry(_StrictContract):
    schema_version: Literal[CAPTURE_POLICY_REGISTRY_SCHEMA_VERSION] = CAPTURE_POLICY_REGISTRY_SCHEMA_VERSION
    registry_id: str = Field(min_length=1, max_length=160)
    registry_version: str = Field(min_length=1, max_length=80)
    absolute_max_candidates: int = Field(ge=1)
    absolute_max_bytes: int = Field(ge=1)
    absolute_max_capture_ms: int = Field(ge=1)
    lease_seconds: int = Field(ge=1)
    statement_timeout_ms: int = Field(ge=1)
    lock_timeout_ms: int = Field(ge=1)
    source_resolver: Phase1GComponentContract
    dse_projection: Phase1GComponentContract
    observation_writer: Phase1GComponentContract
    registry_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("registry_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="registry_hash") if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"registry_hash"})

    @model_validator(mode="after")
    def _validate_registry(self) -> "Phase1GCapturePolicyRegistry":
        if self.lock_timeout_ms > self.statement_timeout_ms:
            raise ValueError("lock timeout cannot exceed statement timeout")
        if self.statement_timeout_ms > self.absolute_max_capture_ms:
            raise ValueError("statement timeout cannot exceed the absolute capture duration")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.registry_hash is not None and self.registry_hash != digest:
            raise ValueError("registry_hash does not match capture policy registry")
        object.__setattr__(self, "registry_hash", digest)
        return self

    def assert_within_bounds(self, *, planned_candidates: int, planned_bytes: int) -> None:
        if planned_candidates < 0 or planned_candidates > self.absolute_max_candidates:
            raise Phase1GContractError(
                REASON_PLAN_INVALID,
                "planned candidates exceed the registered Phase 1G bound",
                context={"planned_candidates": planned_candidates, "maximum": self.absolute_max_candidates},
            )
        if planned_bytes < 0 or planned_bytes > self.absolute_max_bytes:
            raise Phase1GContractError(
                REASON_PLAN_INVALID,
                "planned bytes exceed the registered Phase 1G bound",
                context={"planned_bytes": planned_bytes, "maximum": self.absolute_max_bytes},
            )


DEFAULT_CAPTURE_POLICY_REGISTRY = Phase1GCapturePolicyRegistry(
    registry_id="ADVISORY_PHASE1G_HISTORICAL_OBSERVATION_CAPTURE",
    registry_version="1",
    absolute_max_candidates=1_000_000,
    absolute_max_bytes=2_147_483_648,
    absolute_max_capture_ms=1_800_000,
    lease_seconds=3_600,
    statement_timeout_ms=1_800_000,
    lock_timeout_ms=30_000,
    source_resolver=Phase1GComponentContract(
        component_name="source_resolver",
        contract_version="advisory_phase1g_source_resolver_v1",
    ),
    dse_projection=Phase1GComponentContract(
        component_name="dse_projection",
        contract_version="advisory_phase1g_dse_projection_v1",
    ),
    observation_writer=Phase1GComponentContract(
        component_name="observation_writer",
        contract_version="advisory_phase1g_observation_writer_v1",
    ),
)


def resolve_capture_policy_registry(*, registry_id: str, registry_version: str) -> Phase1GCapturePolicyRegistry:
    if (
        registry_id != DEFAULT_CAPTURE_POLICY_REGISTRY.registry_id
        or registry_version != DEFAULT_CAPTURE_POLICY_REGISTRY.registry_version
    ):
        raise Phase1GContractError(
            REASON_PLAN_INVALID,
            "capture policy registry id/version is not registered",
            context={"registry_id": registry_id, "registry_version": registry_version},
        )
    return DEFAULT_CAPTURE_POLICY_REGISTRY


class Phase1GInputArtifactRef(_StrictContract):
    schema_version: Literal[INPUT_ARTIFACT_REF_SCHEMA_VERSION] = INPUT_ARTIFACT_REF_SCHEMA_VERSION
    artifact_kind: Phase1GInputArtifactKind
    store_policy_hash: str = Field(min_length=64, max_length=64)
    relative_path: str = Field(min_length=1, max_length=800)
    semantic_content_hash: str = Field(min_length=64, max_length=64)
    file_sha256: str = Field(min_length=64, max_length=64)

    @field_validator("store_policy_hash", "semantic_content_hash", "file_sha256")
    @classmethod
    def _hashes(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)

    def semantic_binding_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "artifact_kind": self.artifact_kind.value,
            "store_policy_hash": self.store_policy_hash,
            "semantic_content_hash": self.semantic_content_hash,
            "file_sha256": self.file_sha256,
        }


class Phase1GOutputArtifactRef(_StrictContract):
    schema_version: Literal[OUTPUT_ARTIFACT_REF_SCHEMA_VERSION] = OUTPUT_ARTIFACT_REF_SCHEMA_VERSION
    artifact_kind: Phase1GOutputArtifactKind
    store_policy_hash: str = Field(min_length=64, max_length=64)
    relative_path: str = Field(min_length=1, max_length=800)
    semantic_content_hash: str = Field(min_length=64, max_length=64)
    file_sha256: str = Field(min_length=64, max_length=64)

    @field_validator("store_policy_hash", "semantic_content_hash", "file_sha256")
    @classmethod
    def _hashes(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)

    def semantic_binding_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "artifact_kind": self.artifact_kind.value,
            "store_policy_hash": self.store_policy_hash,
            "semantic_content_hash": self.semantic_content_hash,
            "file_sha256": self.file_sha256,
        }


class Phase1GIdentityHashRef(_StrictContract):
    identity: str = Field(min_length=1, max_length=240)
    content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("content_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, field_name="content_hash")

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class Phase1GTargetExecutionRequest(_StrictContract):
    schema_version: Literal[TARGET_REQUEST_SCHEMA_VERSION] = TARGET_REQUEST_SCHEMA_VERSION
    target_label: TargetLabel
    release_schema_receipt_ref: Phase1GInputArtifactRef
    phase1e_plan_ref: Phase1GInputArtifactRef
    phase1e_plan_id: str = Field(min_length=1, max_length=160)
    phase1e_plan_hash: str = Field(min_length=64, max_length=64)
    source_operation_hash: str = Field(min_length=64, max_length=64)
    observation_template_hash: str = Field(min_length=64, max_length=64)
    program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    capture_policy_registry_id: str = Field(min_length=1, max_length=160)
    capture_policy_registry_version: str = Field(min_length=1, max_length=80)
    capture_policy_registry_hash: str = Field(min_length=64, max_length=64)
    result_store_policy_hash: str = Field(min_length=64, max_length=64)
    requested_at: datetime
    request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "phase1e_plan_hash",
        "source_operation_hash",
        "observation_template_hash",
        "admission_scope_hash",
        "capture_policy_registry_hash",
        "result_store_policy_hash",
        "request_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("requested_at")
    @classmethod
    def _requested_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="requested_at")

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "target_label": self.target_label.value,
            "release_schema_receipt_ref": self.release_schema_receipt_ref.semantic_binding_payload(),
            "phase1e_plan_ref": self.phase1e_plan_ref.semantic_binding_payload(),
            "phase1e_plan_id": self.phase1e_plan_id,
            "phase1e_plan_hash": self.phase1e_plan_hash,
            "source_operation_hash": self.source_operation_hash,
            "observation_template_hash": self.observation_template_hash,
            "program_id": self.program_id,
            "decision_trade_date": self.decision_trade_date,
            "admission_scope_id": self.admission_scope_id,
            "admission_scope_hash": self.admission_scope_hash,
            "capture_policy_registry_id": self.capture_policy_registry_id,
            "capture_policy_registry_version": self.capture_policy_registry_version,
            "capture_policy_registry_hash": self.capture_policy_registry_hash,
            "result_store_policy_hash": self.result_store_policy_hash,
        }

    @model_validator(mode="after")
    def _validate_request(self) -> "Phase1GTargetExecutionRequest":
        if self.release_schema_receipt_ref.artifact_kind is not Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT:
            raise ValueError("release_schema_receipt_ref has the wrong artifact kind")
        if (
            self.release_schema_receipt_ref.store_policy_hash
            != PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash
        ):
            raise ValueError("release_schema_receipt_ref has an unregistered store policy")
        if self.phase1e_plan_ref.artifact_kind is not Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN:
            raise ValueError("phase1e_plan_ref has the wrong artifact kind")
        if self.phase1e_plan_ref.semantic_content_hash != self.phase1e_plan_hash:
            raise ValueError("Phase 1E plan ref does not match phase1e_plan_hash")
        policy = resolve_capture_policy_registry(
            registry_id=self.capture_policy_registry_id,
            registry_version=self.capture_policy_registry_version,
        )
        if policy.registry_hash != self.capture_policy_registry_hash:
            raise ValueError("capture policy registry hash does not match registered policy")
        if self.result_store_policy_hash != PHASE1G_RESULT_STORE_LAYOUT_POLICY.layout_policy_hash:
            raise ValueError("result store policy hash does not match the registered Phase 1G result store")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.request_hash is not None and self.request_hash != digest:
            raise ValueError("request_hash does not match target request")
        object.__setattr__(self, "request_hash", digest)
        return self


class Phase1GExecutionBatchRequest(_StrictContract):
    schema_version: Literal[BATCH_REQUEST_SCHEMA_VERSION] = BATCH_REQUEST_SCHEMA_VERSION
    targets: tuple[Phase1GTargetExecutionRequest, ...] = Field(min_length=1)
    continue_on_target_failure: Literal[True] = True
    execution_prohibited: Literal[True] = True
    batch_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("batch_request_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="batch_request_hash") if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "targets": [
                {"request_hash": item.request_hash, "semantic_request": item.canonical_payload()}
                for item in self.targets
            ],
            "continue_on_target_failure": self.continue_on_target_failure,
            "execution_prohibited": self.execution_prohibited,
        }

    @model_validator(mode="after")
    def _validate_batch(self) -> "Phase1GExecutionBatchRequest":
        targets = tuple(sorted(self.targets, key=lambda item: str(item.request_hash)))
        hashes = tuple(str(item.request_hash) for item in targets)
        if len(hashes) != len(set(hashes)):
            raise ValueError("batch targets must have unique request hashes")
        object.__setattr__(self, "targets", targets)
        digest = canonical_json_sha256(self.canonical_payload())
        if self.batch_request_hash is not None and self.batch_request_hash != digest:
            raise ValueError("batch_request_hash does not match batch request")
        object.__setattr__(self, "batch_request_hash", digest)
        return self


class Phase1GTargetExecutionPlan(_StrictContract):
    schema_version: Literal[TARGET_PLAN_SCHEMA_VERSION] = TARGET_PLAN_SCHEMA_VERSION
    target_request: Phase1GTargetExecutionRequest
    release_receipt_hash: str = Field(min_length=64, max_length=64)
    release_catalog_fingerprint: str = Field(min_length=64, max_length=64)
    database_identity: DatabaseIdentity
    phase1e_plan_id: str = Field(min_length=1, max_length=160)
    phase1e_plan_hash: str = Field(min_length=64, max_length=64)
    source_resolution_expected_hash: str = Field(min_length=64, max_length=64)
    expected_source_events: tuple[Phase1GIdentityHashRef, ...]
    expected_dse: Phase1GIdentityHashRef
    expected_selection_artifact: Phase1GIdentityHashRef
    expected_package: Phase1GIdentityHashRef
    expected_capture_plan_set_hash: str = Field(min_length=64, max_length=64)
    expected_capture_plan_set_count: int = Field(ge=0)
    expected_rows: int = Field(ge=0)
    expected_bytes: int = Field(ge=0)
    capture_policy_registry_hash: str = Field(min_length=64, max_length=64)
    observed_current_binding_head_hash: str = Field(min_length=64, max_length=64)
    observed_capture_batch_state_hash: str = Field(min_length=64, max_length=64)
    observed_outbox_identity_hashes: tuple[str, ...] = ()
    observed_at: datetime
    target_plan_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "release_receipt_hash",
        "release_catalog_fingerprint",
        "phase1e_plan_hash",
        "source_resolution_expected_hash",
        "expected_capture_plan_set_hash",
        "capture_policy_registry_hash",
        "observed_current_binding_head_hash",
        "observed_capture_batch_state_hash",
        "target_plan_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("observed_at")
    @classmethod
    def _observed_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="observed_at")

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"target_plan_hash"})

    @model_validator(mode="after")
    def _validate_plan(self) -> "Phase1GTargetExecutionPlan":
        source_events = tuple(sorted(self.expected_source_events, key=lambda item: (item.identity, item.content_hash)))
        if len({(item.identity, item.content_hash) for item in source_events}) != len(source_events):
            raise ValueError("expected source events must be unique")
        object.__setattr__(self, "expected_source_events", source_events)
        object.__setattr__(
            self,
            "observed_outbox_identity_hashes",
            _sorted_unique(
                self.observed_outbox_identity_hashes,
                field_name="observed_outbox_identity_hashes",
                sha256=True,
            )
            if self.observed_outbox_identity_hashes
            else (),
        )
        if self.release_receipt_hash != self.target_request.release_schema_receipt_ref.semantic_content_hash:
            raise ValueError("plan release receipt hash does not match target request")
        if (
            self.phase1e_plan_id != self.target_request.phase1e_plan_id
            or self.phase1e_plan_hash != self.target_request.phase1e_plan_hash
        ):
            raise ValueError("plan Phase 1E identity does not match target request")
        if self.database_identity.target_label is not self.target_request.target_label:
            raise ValueError("plan database identity does not match target label")
        if self.capture_policy_registry_hash != self.target_request.capture_policy_registry_hash:
            raise ValueError("plan capture policy does not match target request")
        policy = resolve_capture_policy_registry(
            registry_id=self.target_request.capture_policy_registry_id,
            registry_version=self.target_request.capture_policy_registry_version,
        )
        policy.assert_within_bounds(planned_candidates=self.expected_rows, planned_bytes=self.expected_bytes)
        digest = canonical_json_sha256(self.canonical_payload())
        if self.target_plan_hash is not None and self.target_plan_hash != digest:
            raise ValueError("target_plan_hash does not match target plan")
        object.__setattr__(self, "target_plan_hash", digest)
        return self


class Phase1GExecutionBatchPlan(_StrictContract):
    schema_version: Literal[BATCH_PLAN_SCHEMA_VERSION] = BATCH_PLAN_SCHEMA_VERSION
    target_plans: tuple[Phase1GTargetExecutionPlan, ...] = Field(min_length=1)
    target_count: int = Field(ge=1)
    batch_request_hash: str = Field(min_length=64, max_length=64)
    batch_plan_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("batch_request_hash", "batch_plan_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"batch_plan_hash"})

    @model_validator(mode="after")
    def _validate_plan(self) -> "Phase1GExecutionBatchPlan":
        plans = tuple(sorted(self.target_plans, key=lambda item: str(item.target_plan_hash)))
        hashes = tuple(str(item.target_plan_hash) for item in plans)
        if len(hashes) != len(set(hashes)) or self.target_count != len(plans):
            raise ValueError("batch plan target count/hashes are inconsistent")
        object.__setattr__(self, "target_plans", plans)
        digest = canonical_json_sha256(self.canonical_payload())
        if self.batch_plan_hash is not None and self.batch_plan_hash != digest:
            raise ValueError("batch_plan_hash does not match batch plan")
        object.__setattr__(self, "batch_plan_hash", digest)
        return self


def build_phase1g_execution_batch_plan(
    *,
    batch_request: Phase1GExecutionBatchRequest,
    target_plans: tuple[Phase1GTargetExecutionPlan, ...],
) -> Phase1GExecutionBatchPlan:
    request_hashes = tuple(sorted(str(item.request_hash) for item in batch_request.targets))
    plan_request_hashes = tuple(sorted(str(item.target_request.request_hash) for item in target_plans))
    if request_hashes != plan_request_hashes:
        raise Phase1GContractError(
            REASON_PLAN_INVALID,
            "batch execution plan does not close over the exact batch request targets",
            context={
                "requested_target_count": len(request_hashes),
                "planned_target_count": len(plan_request_hashes),
            },
        )
    return Phase1GExecutionBatchPlan(
        target_plans=target_plans,
        target_count=len(target_plans),
        batch_request_hash=str(batch_request.batch_request_hash),
    )


class Phase1GSelectedObservationMapping(_StrictContract):
    capture_plan_hash: str = Field(min_length=64, max_length=64)
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    observation_version_id: str = Field(min_length=1, max_length=160)
    observation_content_hash: str = Field(min_length=64, max_length=64)
    lineage_id: str = Field(min_length=1, max_length=160)
    lineage_content_hash: str = Field(min_length=64, max_length=64)
    stage_evidence_bundle_hash: str = Field(min_length=64, max_length=64)
    source_revision_set_id: str = Field(min_length=1, max_length=160)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    trace_outbox_id: str = Field(min_length=1, max_length=160)
    trace_content_hash: str = Field(min_length=64, max_length=64)

    @field_validator(
        "capture_plan_hash",
        "observation_content_hash",
        "lineage_content_hash",
        "stage_evidence_bundle_hash",
        "source_revision_set_hash",
        "trace_content_hash",
    )
    @classmethod
    def _hashes(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)


class Phase1GTraceOutboxMapping(_StrictContract):
    capture_plan_hash: str = Field(min_length=64, max_length=64)
    trace_outbox_id: str = Field(min_length=1, max_length=160)
    trace_content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("capture_plan_hash", "trace_content_hash")
    @classmethod
    def _hashes(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)


class Phase1GCaptureResult(_StrictContract):
    schema_version: Literal[CAPTURE_RESULT_SCHEMA_VERSION] = CAPTURE_RESULT_SCHEMA_VERSION
    target_request_hash: str = Field(min_length=64, max_length=64)
    phase1f_receipt_hash: str = Field(min_length=64, max_length=64)
    phase1f_catalog_fingerprint: str = Field(min_length=64, max_length=64)
    phase1e_plan_id: str = Field(min_length=1, max_length=160)
    phase1e_plan_hash: str = Field(min_length=64, max_length=64)
    source_resolution_receipt_hash: str = Field(min_length=64, max_length=64)
    source_revision_set_id: str = Field(min_length=1, max_length=160)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    control_binding_event_hash: str = Field(min_length=64, max_length=64)
    capture_batch_id: str = Field(min_length=1, max_length=160)
    capture_request_hash: str = Field(min_length=64, max_length=64)
    capture_attempt_no: int = Field(ge=1)
    capture_status: Literal["COMPLETE"] = "COMPLETE"
    capture_receipt_hash: str = Field(min_length=64, max_length=64)
    membership_count: int = Field(ge=0)
    membership_hash: str = Field(min_length=64, max_length=64)
    capture_plan_set_count: int = Field(ge=0)
    capture_plan_set_hash: str = Field(min_length=64, max_length=64)
    selected_observation_mappings: tuple[Phase1GSelectedObservationMapping, ...]
    trace_outbox_mappings: tuple[Phase1GTraceOutboxMapping, ...]
    runtime_activated: Literal[False] = False
    capture_result_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "target_request_hash",
        "phase1f_receipt_hash",
        "phase1f_catalog_fingerprint",
        "phase1e_plan_hash",
        "source_resolution_receipt_hash",
        "source_revision_set_hash",
        "control_binding_event_hash",
        "capture_request_hash",
        "capture_receipt_hash",
        "membership_hash",
        "capture_plan_set_hash",
        "capture_result_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"capture_result_hash"})

    @model_validator(mode="after")
    def _validate_result(self) -> "Phase1GCaptureResult":
        selected = tuple(
            sorted(
                self.selected_observation_mappings, key=lambda item: (item.capture_plan_hash, item.canonical_signal_id)
            )
        )
        traces = tuple(
            sorted(self.trace_outbox_mappings, key=lambda item: (item.capture_plan_hash, item.trace_outbox_id))
        )
        selected_plan_hashes = tuple(item.capture_plan_hash for item in selected)
        trace_plan_hashes = tuple(item.capture_plan_hash for item in traces)
        if len(selected_plan_hashes) != len(set(selected_plan_hashes)):
            raise ValueError("selected observation mappings must have unique capture plans")
        if len(trace_plan_hashes) != len(set(trace_plan_hashes)):
            raise ValueError("trace outbox mappings must have unique capture plans")
        if selected_plan_hashes != trace_plan_hashes:
            raise ValueError("selected observation and trace mappings must close over the same capture plans")
        trace_by_plan = {item.capture_plan_hash: item for item in traces}
        if any(
            item.trace_outbox_id != trace_by_plan[item.capture_plan_hash].trace_outbox_id
            or item.trace_content_hash != trace_by_plan[item.capture_plan_hash].trace_content_hash
            for item in selected
        ):
            raise ValueError("selected observation mappings do not match trace outbox mappings")
        if self.capture_plan_set_count != len(selected) or self.membership_count < len(selected):
            raise ValueError("capture result counts do not close over selected observations")
        object.__setattr__(self, "selected_observation_mappings", selected)
        object.__setattr__(self, "trace_outbox_mappings", traces)
        digest = canonical_json_sha256(self.canonical_payload())
        if self.capture_result_hash is not None and self.capture_result_hash != digest:
            raise ValueError("capture_result_hash does not match stable result")
        object.__setattr__(self, "capture_result_hash", digest)
        return self


class Phase1GAttemptReceipt(_StrictContract):
    schema_version: Literal[ATTEMPT_RECEIPT_SCHEMA_VERSION] = ATTEMPT_RECEIPT_SCHEMA_VERSION
    target_plan_hash: str = Field(min_length=64, max_length=64)
    target_request_hash: str = Field(min_length=64, max_length=64)
    attempt_invocation_id: str = Field(min_length=1, max_length=160)
    started_at: datetime
    finished_at: datetime | None = None
    operation_status: Phase1GAttemptStatus
    reason_codes: tuple[str, ...] = ()
    dml_executed: bool
    committed_phases: tuple[str, ...] = ()
    capture_batch_id: str | None = Field(default=None, max_length=160)
    capture_attempt_no: int | None = Field(default=None, ge=1)
    capture_batch_status: str | None = Field(default=None, max_length=80)
    capture_result_ref: Phase1GOutputArtifactRef | None = None
    capture_result_hash: str | None = Field(default=None, min_length=64, max_length=64)
    error_context: dict[str, Any] | None = None
    runtime_activated: Literal[False] = False
    attempt_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("target_plan_hash", "target_request_hash", "capture_result_hash", "attempt_receipt_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("started_at", "finished_at")
    @classmethod
    def _timestamps(cls, value: datetime | None, info) -> datetime | None:  # type: ignore[no-untyped-def]
        return _aware_utc(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json", exclude={"attempt_receipt_hash", "capture_result_ref"})
        payload["capture_result_ref"] = (
            self.capture_result_ref.semantic_binding_payload() if self.capture_result_ref is not None else None
        )
        return payload

    @model_validator(mode="after")
    def _validate_receipt(self) -> "Phase1GAttemptReceipt":
        object.__setattr__(
            self,
            "reason_codes",
            _sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else (),
        )
        object.__setattr__(
            self,
            "committed_phases",
            _sorted_unique(self.committed_phases, field_name="committed_phases") if self.committed_phases else (),
        )
        if self.error_context is not None:
            object.__setattr__(self, "error_context", canonicalize(self.error_context))
        batch_values = (self.capture_batch_id, self.capture_attempt_no, self.capture_batch_status)
        if any(value is not None for value in batch_values) and not all(value is not None for value in batch_values):
            raise ValueError("capture batch attempt fields must be all present or all absent")
        if self.operation_status is Phase1GAttemptStatus.IN_PROGRESS:
            if (
                self.finished_at is not None
                or self.capture_result_ref is not None
                or self.capture_result_hash is not None
            ):
                raise ValueError("in-progress attempt cannot be finished or expose a result")
        else:
            if self.finished_at is None or self.finished_at < self.started_at:
                raise ValueError("finished attempt requires an ordered finished_at timestamp")
        if self.operation_status is Phase1GAttemptStatus.SUCCESS:
            if (
                self.capture_result_ref is None
                or self.capture_result_hash is None
                or self.capture_result_ref.semantic_content_hash != self.capture_result_hash
            ):
                raise ValueError("successful attempt requires one matching stable result reference")
            if self.capture_result_ref.artifact_kind is not Phase1GOutputArtifactKind.CAPTURE_RESULT:
                raise ValueError("successful attempt result reference has the wrong artifact kind")
        elif self.capture_result_ref is not None or self.capture_result_hash is not None:
            raise ValueError("non-successful attempt cannot expose a stable result")
        if self.operation_status is Phase1GAttemptStatus.FAILED and not self.reason_codes:
            raise ValueError("failed attempt requires at least one reason code")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.attempt_receipt_hash is not None and self.attempt_receipt_hash != digest:
            raise ValueError("attempt_receipt_hash does not match attempt receipt")
        object.__setattr__(self, "attempt_receipt_hash", digest)
        return self


class Phase1GBatchAttemptReceipt(_StrictContract):
    schema_version: Literal[BATCH_RECEIPT_SCHEMA_VERSION] = BATCH_RECEIPT_SCHEMA_VERSION
    batch_request_hash: str = Field(min_length=64, max_length=64)
    batch_plan_hash: str = Field(min_length=64, max_length=64)
    target_count: int = Field(ge=1)
    succeeded_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    target_attempt_receipt_hashes: tuple[str, ...] = Field(min_length=1)
    successful_capture_result_hashes: tuple[str, ...] = ()
    batch_status: Phase1GBatchStatus
    batch_attempt_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("batch_request_hash", "batch_plan_hash", "batch_attempt_receipt_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"batch_attempt_receipt_hash"})

    @model_validator(mode="after")
    def _validate_receipt(self) -> "Phase1GBatchAttemptReceipt":
        attempt_hashes = _sorted_unique(
            self.target_attempt_receipt_hashes,
            field_name="target_attempt_receipt_hashes",
            sha256=True,
        )
        result_hashes = (
            _sorted_unique(
                self.successful_capture_result_hashes,
                field_name="successful_capture_result_hashes",
                sha256=True,
            )
            if self.successful_capture_result_hashes
            else ()
        )
        object.__setattr__(self, "target_attempt_receipt_hashes", attempt_hashes)
        object.__setattr__(self, "successful_capture_result_hashes", result_hashes)
        if self.target_count != self.succeeded_count + self.failed_count or self.target_count != len(attempt_hashes):
            raise ValueError("batch receipt target counts do not match attempt receipts")
        if self.succeeded_count != len(result_hashes):
            raise ValueError("batch receipt success count does not match stable results")
        expected_status = (
            Phase1GBatchStatus.SUCCESS
            if self.failed_count == 0
            else Phase1GBatchStatus.FAILED
            if self.succeeded_count == 0
            else Phase1GBatchStatus.PARTIAL_FAILURE
        )
        if self.batch_status is not expected_status:
            raise ValueError("batch receipt status does not match target outcomes")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.batch_attempt_receipt_hash is not None and self.batch_attempt_receipt_hash != digest:
            raise ValueError("batch_attempt_receipt_hash does not match batch receipt")
        object.__setattr__(self, "batch_attempt_receipt_hash", digest)
        return self
