"""Typed contracts for Phase 1G G5 DEV evidence."""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.phase1g_contract import (
    Phase1GExecutionBatchPlan,
    Phase1GInputArtifactRef,
    Phase1GOutputArtifactKind,
    Phase1GOutputArtifactRef,
    Phase1GTargetExecutionRequest,
)
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity


G5_CONTRACT_SCHEMA_VERSION = "advisory_phase1g_g5_dev_evidence_v1"
G5_STORE_POLICY_PAYLOAD = {
    "schema_version": "advisory_phase1g_g5_store_policy_v1",
    "layout": {
        "inventory": "inventories/<prefix>/<hash>.json",
        "manifest": "manifests/<prefix>/<hash>.json",
        "plan": "plans/<prefix>/<hash>.json",
        "rollback": "rollback/<prefix>/<hash>.json",
        "persistent": "persistent/<prefix>/<hash>.json",
        "summary": "summaries/<prefix>/<hash>.json",
    },
    "canonical_json": True,
    "atomic_no_replace": True,
    "latest_pointer": False,
}
G5_STORE_POLICY_HASH = canonical_json_sha256(G5_STORE_POLICY_PAYLOAD)

REASON_ENV_INVALID = "ADVISORY_PHASE1G_G5_ENV_INVALID"
REASON_SCHEMA_INVALID = "ADVISORY_PHASE1G_G5_SCHEMA_INVALID"
REASON_ARTIFACT_ROOT_INVALID = "ADVISORY_PHASE1G_G5_ARTIFACT_ROOT_INVALID"
REASON_INVENTORY_INVALID = "ADVISORY_PHASE1G_G5_INVENTORY_INVALID"
REASON_L3_SOURCE_PENDING = "ADVISORY_PHASE1G_G5_L3_SOURCE_EVIDENCE_PENDING"
REASON_REAL_INPUT_PENDING = "ADVISORY_PHASE1G_G5_REAL_INPUT_PENDING"
REASON_SINGLE_TRACK_MISSING = "ADVISORY_PHASE1G_G5_SINGLE_TRACK_MISSING"
REASON_MULTI_TRACK_MISSING = "ADVISORY_PHASE1G_G5_MULTI_TRACK_MISSING"
REASON_MANIFEST_INVALID = "ADVISORY_PHASE1G_G5_MANIFEST_INVALID"
REASON_L3_COORDINATOR_INVALID = "ADVISORY_PHASE1G_G5_L3_COORDINATOR_INVALID"
REASON_L3_FORBIDDEN_SQL = "ADVISORY_PHASE1G_G5_L3_FORBIDDEN_SQL"
REASON_L3_ROLLBACK_FAILED = "ADVISORY_PHASE1G_G5_L3_ROLLBACK_FAILED"
REASON_L3_RESIDUE_DETECTED = "ADVISORY_PHASE1G_G5_L3_RESIDUE_DETECTED"
REASON_L3_CONCURRENCY_FAILED = "ADVISORY_PHASE1G_G5_L3_CONCURRENCY_FAILED"
REASON_L4_PLAN_STALE = "ADVISORY_PHASE1G_G5_L4_PLAN_STALE"
REASON_L4_PARTIAL_FAILURE = "ADVISORY_PHASE1G_G5_L4_PARTIAL_FAILURE"
REASON_REFERENCED_READBACK_FAILED = "ADVISORY_PHASE1G_G5_REFERENCED_READBACK_FAILED"
REASON_EVIDENCE_STORE_FAILED = "ADVISORY_PHASE1G_G5_EVIDENCE_STORE_FAILED"
REASON_UNEXPECTED_ERROR = "ADVISORY_PHASE1G_G5_UNEXPECTED_ERROR"
G5_DATABASE_WRITE_PHASES = frozenset(
    {
        "BATCH_ACQUIRED",
        "BATCH_COMPLETED",
        "BATCH_CREATED",
        "BATCH_EXPIRED",
        "BATCH_FAILED",
        "BATCH_RECOVERED",
        "CONTROL_BINDING",
        "TARGET_EVIDENCE",
    }
)


class Phase1GDevEvidenceError(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.reason_code = reason_code
        self.context = canonicalize(context) if context is not None else None
        super().__init__(message)


class _StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ValueError(f"{field_name} must be lowercase sha256")
    return normalized


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sorted_unique(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    normalized = tuple(sorted(str(value).strip() for value in values))
    if any(not value for value in normalized) or len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} must contain unique non-empty values")
    return normalized


class _HashedContract(_StrictContract):
    hash_field: ClassVar[str]

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={self.hash_field})

    def _close_hash(self) -> None:
        supplied = getattr(self, self.hash_field)
        digest = canonical_json_sha256(self.canonical_payload())
        if supplied is not None and supplied != digest:
            raise ValueError(f"{self.hash_field} does not match canonical payload")
        object.__setattr__(self, self.hash_field, digest)


class AlphaMode(str, Enum):
    SINGLE = "single_alpha"
    MULTI = "multi_alpha"


class L3SourceClassification(str, Enum):
    ELIGIBLE_SINGLE = "L3_SOURCE_ELIGIBLE_SINGLE"
    ELIGIBLE_NATIVE_MULTI = "L3_SOURCE_ELIGIBLE_NATIVE_MULTI"
    INCOMPLETE = "L3_SOURCE_INCOMPLETE"


class L4TargetClassification(str, Enum):
    EXECUTABLE_SINGLE = "L4_EXECUTABLE_SINGLE"
    EXECUTABLE_NATIVE_MULTI = "L4_EXECUTABLE_NATIVE_MULTI"
    DEFERRED = "L4_DEFERRED"
    DIAGNOSTIC = "L4_DIAGNOSTIC"
    STALE = "L4_STALE"
    INCOMPLETE = "L4_INCOMPLETE"


class InventoryStatus(str, Enum):
    L4_DUAL_TRACK_READY = "L4_DUAL_TRACK_READY"
    L3_READY_L4_PENDING = "L3_READY_L4_PENDING"
    L3_SOURCE_PENDING = "L3_SOURCE_PENDING"
    INVALID = "INVALID"


class ExecutionMode(str, Enum):
    ROLLBACK_VALIDATION = "ROLLBACK_VALIDATION"
    PERSISTENT_DUAL_TRACK = "PERSISTENT_DUAL_TRACK"


class RollbackStatus(str, Enum):
    COMPLETE_ZERO_RESIDUE = "COMPLETE_ZERO_RESIDUE"
    NOT_RUN_SOURCE_EVIDENCE_PENDING = "NOT_RUN_SOURCE_EVIDENCE_PENDING"
    FAILED = "FAILED"
    STATE_UNKNOWN = "STATE_UNKNOWN"


class PersistentStatus(str, Enum):
    COMPLETE_DUAL_TRACK = "COMPLETE_DUAL_TRACK"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    FAILED = "FAILED"
    NOT_RUN_INPUT_PENDING = "NOT_RUN_INPUT_PENDING"


class SummaryStatus(str, Enum):
    COMPLETE = "COMPLETE"
    PENDING_L3_SOURCE = "PENDING_L3_SOURCE"
    PENDING_L4 = "PENDING_L4"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    FAILED = "FAILED"


class EvidenceKind(str, Enum):
    INVENTORY = "inventory"
    MANIFEST = "manifest"
    PLAN = "plan"
    ROLLBACK = "rollback"
    PERSISTENT = "persistent"
    SUMMARY = "summary"


class Phase1GDevEvidenceRef(_StrictContract):
    schema_version: Literal[G5_CONTRACT_SCHEMA_VERSION] = G5_CONTRACT_SCHEMA_VERSION
    evidence_kind: EvidenceKind
    store_policy_hash: Literal[G5_STORE_POLICY_HASH] = G5_STORE_POLICY_HASH
    relative_path: str = Field(min_length=1, max_length=800)
    semantic_content_hash: str = Field(min_length=64, max_length=64)
    file_sha256: str = Field(min_length=64, max_length=64)

    @field_validator("semantic_content_hash", "file_sha256")
    @classmethod
    def _hashes(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name)

    def semantic_binding_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"relative_path"})


class Phase1GDevIdentityHashRef(_StrictContract):
    identity: str = Field(min_length=1, max_length=240)
    content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("content_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, field_name="content_hash")


class Phase1GDevL3SourceCandidate(_HashedContract):
    hash_field: ClassVar[str] = "source_candidate_hash"
    schema_version: Literal[G5_CONTRACT_SCHEMA_VERSION] = G5_CONTRACT_SCHEMA_VERSION
    source_phase1e_plan_ref: Phase1GInputArtifactRef
    release_receipt_ref: Phase1GInputArtifactRef
    alpha_mode: AlphaMode
    component_package_ids: tuple[str, ...] = ()
    decision_trade_date: date
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    selection_evidence: Phase1GDevIdentityHashRef
    selection_artifact: Phase1GDevIdentityHashRef
    source_resolution_receipt_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    source_event_refs: tuple[Phase1GDevIdentityHashRef, ...]
    classification: L3SourceClassification
    reason_codes: tuple[str, ...] = ()
    source_candidate_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_sha256", "source_resolution_receipt_hash", "source_candidate_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "Phase1GDevL3SourceCandidate":
        components = _sorted_unique(self.component_package_ids, field_name="component_package_ids") if self.component_package_ids else ()
        reasons = _sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        events = tuple(sorted(self.source_event_refs, key=lambda item: (item.identity, item.content_hash)))
        if len({item.identity for item in events}) != len(events):
            raise ValueError("source_event_refs must have unique identities")
        if self.alpha_mode is AlphaMode.SINGLE and components:
            raise ValueError("single Alpha source cannot carry component package ids")
        if self.alpha_mode is AlphaMode.MULTI and len(components) < 2:
            raise ValueError("native multi Alpha source requires at least two components")
        expected = (
            L3SourceClassification.ELIGIBLE_SINGLE
            if self.alpha_mode is AlphaMode.SINGLE
            else L3SourceClassification.ELIGIBLE_NATIVE_MULTI
        )
        if self.classification is not L3SourceClassification.INCOMPLETE and self.classification is not expected:
            raise ValueError("L3 source classification differs from alpha mode")
        if self.classification is L3SourceClassification.INCOMPLETE:
            if not reasons:
                raise ValueError("incomplete L3 source requires reason codes")
        elif self.source_resolution_receipt_hash is None or not events:
            raise ValueError("eligible L3 source requires exact receipt and source events")
        object.__setattr__(self, "component_package_ids", components)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "source_event_refs", events)
        self._close_hash()
        return self


class Phase1GDevL4TargetCandidate(_HashedContract):
    hash_field: ClassVar[str] = "target_candidate_hash"
    schema_version: Literal[G5_CONTRACT_SCHEMA_VERSION] = G5_CONTRACT_SCHEMA_VERSION
    target_request: Phase1GTargetExecutionRequest | None = None
    alpha_mode: AlphaMode
    component_package_ids: tuple[str, ...] = ()
    decision_trade_date: date
    program_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    admission_scope_id: str | None = Field(default=None, min_length=1, max_length=160)
    admission_scope_hash: str | None = Field(default=None, min_length=64, max_length=64)
    phase1e_plan_ref: Phase1GInputArtifactRef
    dse: Phase1GDevIdentityHashRef
    selection_artifact: Phase1GDevIdentityHashRef
    source_event_refs: tuple[Phase1GDevIdentityHashRef, ...]
    classification: L4TargetClassification
    reason_codes: tuple[str, ...] = ()
    target_candidate_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_sha256", "admission_scope_hash", "target_candidate_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @property
    def executable(self) -> bool:
        return self.classification in {
            L4TargetClassification.EXECUTABLE_SINGLE,
            L4TargetClassification.EXECUTABLE_NATIVE_MULTI,
        }

    def canonical_payload(self) -> dict[str, Any]:
        payload = self.model_dump(
            mode="json", exclude={"target_candidate_hash", "target_request"}
        )
        payload["target_request"] = (
            {
                "request_hash": self.target_request.request_hash,
                "semantic_request": self.target_request.canonical_payload(),
            }
            if self.target_request is not None
            else None
        )
        return payload

    @model_validator(mode="after")
    def _close(self) -> "Phase1GDevL4TargetCandidate":
        components = _sorted_unique(self.component_package_ids, field_name="component_package_ids") if self.component_package_ids else ()
        reasons = _sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        events = tuple(sorted(self.source_event_refs, key=lambda item: (item.identity, item.content_hash)))
        if len({item.identity for item in events}) != len(events):
            raise ValueError("source_event_refs must have unique identities")
        if self.alpha_mode is AlphaMode.SINGLE and components:
            raise ValueError("single Alpha target cannot carry component package ids")
        if self.alpha_mode is AlphaMode.MULTI and len(components) < 2:
            raise ValueError("native multi Alpha target requires at least two components")
        expected = (
            L4TargetClassification.EXECUTABLE_SINGLE
            if self.alpha_mode is AlphaMode.SINGLE
            else L4TargetClassification.EXECUTABLE_NATIVE_MULTI
        )
        if self.classification is expected:
            if (
                self.target_request is None
                or self.admission_scope_id is None
                or self.admission_scope_hash is None
            ):
                raise ValueError(
                    "executable L4 target requires request and admission scope"
                )
        elif not reasons:
            raise ValueError("non-executable L4 target requires reason codes")
        object.__setattr__(self, "component_package_ids", components)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "source_event_refs", events)
        self._close_hash()
        return self


class Phase1GDevInputInventoryReceipt(_HashedContract):
    hash_field: ClassVar[str] = "inventory_receipt_hash"
    schema_version: Literal[G5_CONTRACT_SCHEMA_VERSION] = G5_CONTRACT_SCHEMA_VERSION
    inventory_invocation_id: str = Field(min_length=1, max_length=160)
    target_label: Literal["DEV"] = "DEV"
    database_identity: DatabaseIdentity
    release_receipt_refs: tuple[Phase1GInputArtifactRef, ...]
    catalog_fingerprint: str = Field(min_length=64, max_length=64)
    artifact_root_policy_hashes: tuple[str, ...]
    l3_source_candidates: tuple[Phase1GDevL3SourceCandidate, ...] = ()
    l4_target_candidates: tuple[Phase1GDevL4TargetCandidate, ...] = ()
    l3_source_set_hash: str = Field(min_length=64, max_length=64)
    l4_target_set_hash: str = Field(min_length=64, max_length=64)
    l3_source_eligible_count: int = Field(ge=0)
    l4_single_executable_count: int = Field(ge=0)
    l4_native_multi_executable_count: int = Field(ge=0)
    inventory_status: InventoryStatus
    reason_codes: tuple[str, ...] = ()
    observed_at: datetime
    inventory_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "catalog_fingerprint",
        "l3_source_set_hash",
        "l4_target_set_hash",
        "inventory_receipt_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("artifact_root_policy_hashes")
    @classmethod
    def _policy_hashes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted({_sha256(value, field_name="artifact_root_policy_hashes") for value in values}))

    @field_validator("observed_at")
    @classmethod
    def _observed_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="observed_at")

    @model_validator(mode="after")
    def _close(self) -> "Phase1GDevInputInventoryReceipt":
        l3 = tuple(sorted(self.l3_source_candidates, key=lambda item: str(item.source_candidate_hash)))
        l4 = tuple(sorted(self.l4_target_candidates, key=lambda item: str(item.target_candidate_hash)))
        releases = tuple(sorted(self.release_receipt_refs, key=lambda item: item.semantic_content_hash))
        reasons = _sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        if len({item.source_candidate_hash for item in l3}) != len(l3):
            raise ValueError("L3 source candidate hashes must be unique")
        if len({item.target_candidate_hash for item in l4}) != len(l4):
            raise ValueError("L4 target candidate hashes must be unique")
        l3_count = sum(item.classification is not L3SourceClassification.INCOMPLETE for item in l3)
        single_count = sum(item.classification is L4TargetClassification.EXECUTABLE_SINGLE for item in l4)
        multi_count = sum(item.classification is L4TargetClassification.EXECUTABLE_NATIVE_MULTI for item in l4)
        if (
            self.l3_source_set_hash != canonical_json_sha256([item.source_candidate_hash for item in l3])
            or self.l4_target_set_hash != canonical_json_sha256([item.target_candidate_hash for item in l4])
            or self.l3_source_eligible_count != l3_count
            or self.l4_single_executable_count != single_count
            or self.l4_native_multi_executable_count != multi_count
        ):
            raise ValueError("inventory candidate counts or set hashes do not close")
        expected = (
            InventoryStatus.L4_DUAL_TRACK_READY
            if single_count and multi_count
            else InventoryStatus.L3_READY_L4_PENDING
            if l3_count
            else InventoryStatus.L3_SOURCE_PENDING
        )
        if self.inventory_status is not InventoryStatus.INVALID and self.inventory_status is not expected:
            raise ValueError("inventory status does not match candidate readiness")
        if self.inventory_status is InventoryStatus.INVALID and not reasons:
            raise ValueError("invalid inventory requires reason codes")
        object.__setattr__(self, "l3_source_candidates", l3)
        object.__setattr__(self, "l4_target_candidates", l4)
        object.__setattr__(self, "release_receipt_refs", releases)
        object.__setattr__(self, "reason_codes", reasons)
        self._close_hash()
        return self


class Phase1GDevExecutionManifest(_HashedContract):
    hash_field: ClassVar[str] = "manifest_hash"
    schema_version: Literal[G5_CONTRACT_SCHEMA_VERSION] = G5_CONTRACT_SCHEMA_VERSION
    inventory_receipt_ref: Phase1GDevEvidenceRef
    execution_mode: ExecutionMode
    source_candidate_hashes: tuple[str, ...] = ()
    target_request_hashes: tuple[str, ...] = ()
    single_target_count: int = Field(default=0, ge=0)
    native_multi_target_count: int = Field(default=0, ge=0)
    manifest_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("source_candidate_hashes", "target_request_hashes")
    @classmethod
    def _hash_sets(cls, values: tuple[str, ...], info) -> tuple[str, ...]:  # type: ignore[no-untyped-def]
        normalized = tuple(sorted(_sha256(value, field_name=info.field_name) for value in values))
        if len(normalized) != len(set(normalized)):
            raise ValueError(f"{info.field_name} must be unique")
        return normalized

    @field_validator("manifest_hash")
    @classmethod
    def _manifest_hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="manifest_hash") if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "Phase1GDevExecutionManifest":
        if self.inventory_receipt_ref.evidence_kind is not EvidenceKind.INVENTORY:
            raise ValueError("execution manifest requires an inventory receipt ref")
        if self.execution_mode is ExecutionMode.ROLLBACK_VALIDATION:
            if self.target_request_hashes:
                raise ValueError("rollback manifest can only carry L3 source candidates")
            if self.single_target_count or self.native_multi_target_count:
                raise ValueError("rollback manifest cannot carry persistent target counts")
        else:
            if self.source_candidate_hashes:
                raise ValueError("persistent manifest can only carry target requests")
            if not self.target_request_hashes:
                if self.single_target_count or self.native_multi_target_count:
                    raise ValueError("empty persistent manifest cannot carry target counts")
            else:
                if self.single_target_count < 1 or self.native_multi_target_count < 1:
                    raise ValueError("persistent manifest requires single and native multi targets")
                if self.single_target_count + self.native_multi_target_count != len(self.target_request_hashes):
                    raise ValueError("persistent manifest target counts do not close")
        self._close_hash()
        return self


class Phase1GDevQueryEvidence(_StrictContract):
    statement_type: str = Field(min_length=1, max_length=40)
    relation_names: tuple[str, ...] = ()
    normalized_sql_hash: str = Field(min_length=64, max_length=64)
    facade_mode: Literal["read", "write", "owner"]

    @field_validator("normalized_sql_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, field_name="normalized_sql_hash")


class Phase1GDevResidueCheck(_StrictContract):
    relation_name: str = Field(min_length=1, max_length=160)
    identity_set_hash: str = Field(min_length=64, max_length=64)
    checked_identity_count: int = Field(ge=0)
    residue_count: int = Field(ge=0)

    @field_validator("identity_set_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, field_name="identity_set_hash")


class Phase1GDevRollbackReceipt(_HashedContract):
    hash_field: ClassVar[str] = "rollback_receipt_hash"
    schema_version: Literal[G5_CONTRACT_SCHEMA_VERSION] = G5_CONTRACT_SCHEMA_VERSION
    rollback_invocation_id: str = Field(min_length=1, max_length=160)
    database_identity: DatabaseIdentity
    catalog_fingerprint: str = Field(min_length=64, max_length=64)
    input_manifest_hash: str = Field(min_length=64, max_length=64)
    batch_plan_hash: str | None = Field(default=None, min_length=64, max_length=64)
    observed_transactional_dml: bool
    physical_commit_count: Literal[0] = 0
    physical_rollback_count: int = Field(ge=0, le=1)
    read_query_count: int = Field(ge=0)
    write_query_count: int = Field(ge=0)
    normalized_query_set_hash: str = Field(min_length=64, max_length=64)
    write_relation_set: tuple[str, ...] = ()
    in_transaction_outcome_hash: str | None = Field(default=None, min_length=64, max_length=64)
    ephemeral_result_hashes: tuple[str, ...] = ()
    ephemeral_artifacts_disposed: bool
    fresh_connection_residue_checks: tuple[Phase1GDevResidueCheck, ...] = ()
    concurrency_probe_hash: str | None = Field(default=None, min_length=64, max_length=64)
    rollback_status: RollbackStatus
    reason_codes: tuple[str, ...] = ()
    started_at: datetime
    finished_at: datetime
    rollback_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "catalog_fingerprint",
        "input_manifest_hash",
        "batch_plan_hash",
        "normalized_query_set_hash",
        "in_transaction_outcome_hash",
        "concurrency_probe_hash",
        "rollback_receipt_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("ephemeral_result_hashes")
    @classmethod
    def _result_hashes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted({_sha256(value, field_name="ephemeral_result_hashes") for value in values}))

    @field_validator("started_at", "finished_at")
    @classmethod
    def _timestamps(cls, value: datetime, info) -> datetime:  # type: ignore[no-untyped-def]
        return _aware_utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _close(self) -> "Phase1GDevRollbackReceipt":
        reasons = _sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        relations = _sorted_unique(self.write_relation_set, field_name="write_relation_set") if self.write_relation_set else ()
        checks = tuple(sorted(self.fresh_connection_residue_checks, key=lambda item: item.relation_name))
        if self.finished_at < self.started_at:
            raise ValueError("rollback receipt timestamps are reversed")
        if self.rollback_status is RollbackStatus.COMPLETE_ZERO_RESIDUE:
            checked_relations = {item.relation_name for item in checks}
            if (
                reasons
                or not self.observed_transactional_dml
                or self.physical_rollback_count != 1
                or not self.ephemeral_artifacts_disposed
                or self.read_query_count < 1
                or self.write_query_count < 1
                or not relations
                or not checks
                or not set(relations).issubset(checked_relations)
                or sum(item.checked_identity_count for item in checks) < 1
                or any(item.residue_count for item in checks)
                or self.batch_plan_hash is None
                or self.in_transaction_outcome_hash is None
                or not self.ephemeral_result_hashes
                or self.concurrency_probe_hash is None
            ):
                raise ValueError("complete rollback receipt lacks zero-residue evidence")
        elif self.rollback_status is RollbackStatus.NOT_RUN_SOURCE_EVIDENCE_PENDING:
            if self.observed_transactional_dml or self.physical_rollback_count or self.batch_plan_hash is not None:
                raise ValueError("pending rollback receipt cannot claim transactional execution")
            if REASON_L3_SOURCE_PENDING not in reasons:
                raise ValueError("pending rollback receipt requires source pending reason")
        elif not reasons:
            raise ValueError("failed or unknown rollback receipt requires reason codes")
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "write_relation_set", relations)
        object.__setattr__(self, "fresh_connection_residue_checks", checks)
        self._close_hash()
        return self


class Phase1GDevPersistentTargetOutcome(_StrictContract):
    target_request_hash: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    first_operation_status: str = Field(min_length=1, max_length=80)
    rerun_operation_status: str | None = Field(default=None, max_length=80)
    first_dml_executed: bool
    rerun_dml_executed: bool | None = None
    first_committed_phases: tuple[str, ...] = ()
    rerun_committed_phases: tuple[str, ...] = ()
    stable_result_hash: str | None = Field(default=None, min_length=64, max_length=64)
    first_attempt_ref: Phase1GOutputArtifactRef | None = None
    rerun_attempt_ref: Phase1GOutputArtifactRef | None = None
    exact_rerun_verified: bool = False
    reason_codes: tuple[str, ...] = ()

    @field_validator("target_request_hash", "stable_result_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "Phase1GDevPersistentTargetOutcome":
        reasons = _sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        first_phases = (
            _sorted_unique(
                self.first_committed_phases,
                field_name="first_committed_phases",
            )
            if self.first_committed_phases
            else ()
        )
        rerun_phases = (
            _sorted_unique(
                self.rerun_committed_phases,
                field_name="rerun_committed_phases",
            )
            if self.rerun_committed_phases
            else ()
        )
        underlying_success = (
            self.first_operation_status == "SUCCESS"
            and self.rerun_operation_status == "SUCCESS"
        )
        if self.exact_rerun_verified:
            if (
                reasons
                or not underlying_success
                or self.stable_result_hash is None
                or self.first_attempt_ref is None
                or self.rerun_attempt_ref is None
                or self.rerun_dml_executed is not False
                or self.first_attempt_ref.artifact_kind
                is not Phase1GOutputArtifactKind.ATTEMPT_RECEIPT
                or self.rerun_attempt_ref.artifact_kind
                is not Phase1GOutputArtifactKind.ATTEMPT_RECEIPT
                or self.first_attempt_ref.semantic_content_hash
                == self.rerun_attempt_ref.semantic_content_hash
                or bool(set(rerun_phases) & G5_DATABASE_WRITE_PHASES)
            ):
                raise ValueError("successful persistent target lacks exact rerun evidence")
        elif not reasons:
            raise ValueError("non-exact persistent target requires reason codes")
        object.__setattr__(self, "first_committed_phases", first_phases)
        object.__setattr__(self, "rerun_committed_phases", rerun_phases)
        object.__setattr__(self, "reason_codes", reasons)
        return self


class Phase1GDevPersistentReceipt(_HashedContract):
    hash_field: ClassVar[str] = "persistent_receipt_hash"
    schema_version: Literal[G5_CONTRACT_SCHEMA_VERSION] = G5_CONTRACT_SCHEMA_VERSION
    persistent_invocation_id: str = Field(min_length=1, max_length=160)
    database_identity: DatabaseIdentity
    catalog_fingerprint: str = Field(min_length=64, max_length=64)
    inventory_receipt_ref: Phase1GDevEvidenceRef
    execution_manifest_hash: str = Field(min_length=64, max_length=64)
    batch_plan_ref: Phase1GDevEvidenceRef | None = None
    batch_plan_hash: str | None = Field(default=None, min_length=64, max_length=64)
    first_batch_outcome_hash: str | None = Field(default=None, min_length=64, max_length=64)
    rerun_batch_outcome_hash: str | None = Field(default=None, min_length=64, max_length=64)
    target_outcomes: tuple[Phase1GDevPersistentTargetOutcome, ...] = ()
    batch_attempt_refs: tuple[Phase1GOutputArtifactRef, ...] = ()
    single_target_count: int = Field(ge=0)
    native_multi_target_count: int = Field(ge=0)
    first_dml_target_count: int = Field(ge=0)
    rerun_dml_target_count: int = Field(ge=0)
    stable_result_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    referenced_readback_hash: str | None = Field(default=None, min_length=64, max_length=64)
    persistent_status: PersistentStatus
    reason_codes: tuple[str, ...] = ()
    started_at: datetime
    finished_at: datetime
    persistent_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "catalog_fingerprint",
        "execution_manifest_hash",
        "batch_plan_hash",
        "first_batch_outcome_hash",
        "rerun_batch_outcome_hash",
        "stable_result_set_hash",
        "referenced_readback_hash",
        "persistent_receipt_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("started_at", "finished_at")
    @classmethod
    def _timestamps(cls, value: datetime, info) -> datetime:  # type: ignore[no-untyped-def]
        return _aware_utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _close(self) -> "Phase1GDevPersistentReceipt":
        outcomes = tuple(sorted(self.target_outcomes, key=lambda item: item.target_request_hash))
        reasons = _sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        if len({item.target_request_hash for item in outcomes}) != len(outcomes):
            raise ValueError("persistent target outcomes must have unique request hashes")
        single_count = sum(item.alpha_mode is AlphaMode.SINGLE for item in outcomes)
        multi_count = sum(item.alpha_mode is AlphaMode.MULTI for item in outcomes)
        first_dml_count = sum(item.first_dml_executed for item in outcomes)
        rerun_dml_count = sum(item.rerun_dml_executed is True for item in outcomes)
        stable_hashes = sorted(
            str(item.stable_result_hash)
            for item in outcomes
            if item.stable_result_hash is not None
        )
        expected_stable_set_hash = (
            canonical_json_sha256(stable_hashes) if stable_hashes else None
        )
        if (
            self.single_target_count != single_count
            or self.native_multi_target_count != multi_count
            or self.first_dml_target_count != first_dml_count
            or self.rerun_dml_target_count != rerun_dml_count
            or self.stable_result_set_hash != expected_stable_set_hash
        ):
            raise ValueError("persistent receipt counts or stable result set do not close")
        if self.finished_at < self.started_at:
            raise ValueError("persistent receipt timestamps are reversed")
        if self.inventory_receipt_ref.evidence_kind is not EvidenceKind.INVENTORY:
            raise ValueError("persistent receipt inventory ref has wrong kind")
        if self.batch_plan_ref is not None and (
            self.batch_plan_ref.evidence_kind is not EvidenceKind.PLAN
            or self.batch_plan_ref.semantic_content_hash != self.batch_plan_hash
        ):
            raise ValueError("persistent receipt plan ref does not match batch plan hash")
        if self.persistent_status is PersistentStatus.COMPLETE_DUAL_TRACK:
            if (
                reasons
                or self.single_target_count < 1
                or self.native_multi_target_count < 1
                or self.rerun_dml_target_count != 0
                or self.batch_plan_ref is None
                or self.batch_plan_hash is None
                or self.first_batch_outcome_hash is None
                or self.rerun_batch_outcome_hash is None
                or self.stable_result_set_hash is None
                or self.referenced_readback_hash is None
                or len(self.batch_attempt_refs) != 2
                or any(
                    ref.artifact_kind
                    is not Phase1GOutputArtifactKind.BATCH_RECEIPT
                    for ref in self.batch_attempt_refs
                )
                or len(
                    {ref.semantic_content_hash for ref in self.batch_attempt_refs}
                )
                != 2
                or self.first_batch_outcome_hash
                == self.rerun_batch_outcome_hash
                or len(outcomes) != self.single_target_count + self.native_multi_target_count
                or any(
                    item.first_operation_status != "SUCCESS"
                    or item.rerun_operation_status != "SUCCESS"
                    or not item.exact_rerun_verified
                    or item.stable_result_hash is None
                    or item.rerun_dml_executed
                    or item.reason_codes
                    for item in outcomes
                )
            ):
                raise ValueError("complete persistent receipt lacks dual-track evidence")
        elif self.persistent_status is PersistentStatus.NOT_RUN_INPUT_PENDING:
            if (
                outcomes
                or self.batch_plan_ref is not None
                or self.batch_plan_hash is not None
                or self.first_batch_outcome_hash is not None
                or self.rerun_batch_outcome_hash is not None
                or self.batch_attempt_refs
                or self.referenced_readback_hash is not None
                or REASON_REAL_INPUT_PENDING not in reasons
            ):
                raise ValueError("pending persistent receipt cannot claim execution")
        elif not reasons:
            raise ValueError("failed persistent receipt requires reason codes")
        object.__setattr__(self, "target_outcomes", outcomes)
        object.__setattr__(self, "reason_codes", reasons)
        self._close_hash()
        return self


class Phase1GDevEvidenceSummary(_HashedContract):
    hash_field: ClassVar[str] = "summary_hash"
    schema_version: Literal[G5_CONTRACT_SCHEMA_VERSION] = G5_CONTRACT_SCHEMA_VERSION
    inventory_receipt_ref: Phase1GDevEvidenceRef
    rollback_receipt_ref: Phase1GDevEvidenceRef | None = None
    persistent_receipt_ref: Phase1GDevEvidenceRef | None = None
    summary_status: SummaryStatus
    reason_codes: tuple[str, ...] = ()
    created_at: datetime
    summary_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("created_at")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="created_at")

    @field_validator("summary_hash")
    @classmethod
    def _summary_hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="summary_hash") if value is not None else None

    @model_validator(mode="after")
    def _close(self) -> "Phase1GDevEvidenceSummary":
        if self.inventory_receipt_ref.evidence_kind is not EvidenceKind.INVENTORY:
            raise ValueError("summary inventory ref has wrong kind")
        if self.rollback_receipt_ref is not None and self.rollback_receipt_ref.evidence_kind is not EvidenceKind.ROLLBACK:
            raise ValueError("summary rollback ref has wrong kind")
        if self.persistent_receipt_ref is not None and self.persistent_receipt_ref.evidence_kind is not EvidenceKind.PERSISTENT:
            raise ValueError("summary persistent ref has wrong kind")
        reasons = _sorted_unique(self.reason_codes, field_name="reason_codes") if self.reason_codes else ()
        if self.summary_status is SummaryStatus.COMPLETE:
            if (
                reasons
                or self.rollback_receipt_ref is None
                or self.persistent_receipt_ref is None
            ):
                raise ValueError("complete summary requires exact L3 and L4 receipts")
        if self.summary_status is SummaryStatus.PENDING_L3_SOURCE:
            if (
                self.rollback_receipt_ref is None
                or self.persistent_receipt_ref is not None
                or REASON_L3_SOURCE_PENDING not in reasons
            ):
                raise ValueError("L3 pending summary lacks exact pending evidence")
        if self.summary_status is SummaryStatus.PENDING_L4 and (
            self.rollback_receipt_ref is None
            or REASON_REAL_INPUT_PENDING not in reasons
        ):
            raise ValueError("L4 pending summary lacks exact pending evidence")
        if self.summary_status in {SummaryStatus.PARTIAL_FAILURE, SummaryStatus.FAILED} and not reasons:
            raise ValueError("failed summary requires reason codes")
        object.__setattr__(self, "reason_codes", reasons)
        self._close_hash()
        return self


G5StoredModel = (
    Phase1GDevInputInventoryReceipt
    | Phase1GDevExecutionManifest
    | Phase1GExecutionBatchPlan
    | Phase1GDevRollbackReceipt
    | Phase1GDevPersistentReceipt
    | Phase1GDevEvidenceSummary
)
