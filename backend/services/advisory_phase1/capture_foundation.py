"""Phase 1C capture-batch admission and durable trace-gap primitives.

This module is an optional historical-research sidecar.  It intentionally does
not import Selection, Advisory lifecycle, Paper, simulation, QMT, broker, or
market-data services.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Iterator, Mapping, Protocol

import psycopg2
import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.db.pg_pool import get_conn
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.label_capture import (
    LABEL_CAPTURE_BATCH_SCHEMA_VERSION,
    LABEL_CAPTURE_PURPOSE,
    LabelCaptureBatchRequestV2,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.stage_trace import StageTraceEnvelope, TraceCaptureBinding
from backend.services.advisory_phase1.trace_outbox import ExpectedTraceIdentity


CAPTURE_BATCH_SCHEMA_VERSION = "advisory_phase1_capture_batch_v1"
OBSERVATION_CAPTURE_PURPOSE = "OBSERVATION_CAPTURE_V1"
REASON_CAPTURE_BATCH_CONFLICT = "ADVISORY_PHASE1_CAPTURE_BATCH_CONFLICT"
REASON_CAPTURE_BATCH_STATE_INVALID = "ADVISORY_PHASE1_CAPTURE_BATCH_STATE_INVALID"
REASON_CAPTURE_BATCH_LEASE_EXPIRED = "ADVISORY_PHASE1_CAPTURE_BATCH_LEASE_EXPIRED"
REASON_CAPTURE_BATCH_FENCING_INVALID = "ADVISORY_PHASE1_CAPTURE_BATCH_FENCING_INVALID"
REASON_CAPTURE_BATCH_MEMBERSHIP_INVALID = "ADVISORY_PHASE1_CAPTURE_BATCH_MEMBERSHIP_INVALID"
REASON_TRACE_ADMISSION_BINDING_INVALID = "ADVISORY_PHASE1_TRACE_ADMISSION_BINDING_INVALID"
REASON_TRACE_ADMISSION_BATCH_INVALID = "ADVISORY_PHASE1_TRACE_ADMISSION_BATCH_INVALID"
REASON_TRACE_GAP_CONFLICT = "ADVISORY_PHASE1_TRACE_GAP_CONFLICT"


def _require_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return value.astimezone(timezone.utc)


class CaptureBatchStatus(str, Enum):
    PLANNED = "PLANNED"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    ABORTED = "ABORTED"


class CapturePlan(BaseModel):
    """Frozen Phase 0A identity required before a trace can become an observation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    selection_run_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    decision_as_of_trade_date: str = Field(min_length=10, max_length=10)
    selection_as_of_trade_date: str = Field(min_length=10, max_length=10)
    target_trade_date: str = Field(min_length=10, max_length=10)
    decision_cutoff_ts: datetime
    alpha_mode: str = Field(pattern="^(single_alpha|multi_alpha)$")
    selection_runtime_semantics_hash: str = Field(min_length=64, max_length=64)
    package_effective_config_hash: str = Field(min_length=64, max_length=64)
    calendar_version: str = Field(min_length=1, max_length=160)
    calendar_hash: str = Field(min_length=64, max_length=64)
    stable_signal_semantics_hash: str = Field(min_length=64, max_length=64)
    canonical_signal_scope_hash: str = Field(min_length=64, max_length=64)
    phase0a_audit_id: str = Field(min_length=1, max_length=160)
    phase0a_audit_manifest_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    signal_source_revision_set_id: str = Field(min_length=1, max_length=160)
    signal_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    phase0a_signal_context_hash: str = Field(min_length=64, max_length=64)
    evidence_bundle_hash: str = Field(min_length=64, max_length=64)
    selection_evidence_id: str = Field(min_length=1, max_length=160)
    selection_evidence_hash: str = Field(min_length=64, max_length=64)
    selection_run_content_hash: str = Field(min_length=64, max_length=64)
    selection_score_artifact_id: str = Field(min_length=1, max_length=160)
    selection_score_artifact_hash: str = Field(min_length=64, max_length=64)
    runtime_profile_version_id: str = Field(min_length=1, max_length=160)
    runtime_profile_version_hash: str = Field(min_length=64, max_length=64)
    hmm_snapshot_id: str | None = Field(default=None, max_length=160)
    hmm_snapshot_hash: str | None = Field(default=None, min_length=64, max_length=64)
    hmm_snapshot_status: str = Field(min_length=1, max_length=80)
    risk_policy_hash: str = Field(min_length=64, max_length=64)
    universe_policy_hash: str = Field(min_length=64, max_length=64)
    symbol_normalization_policy_hash: str = Field(min_length=64, max_length=64)
    valid_no_candidate: bool
    evidence_available_at: datetime
    audit_target_id: str = Field(min_length=1, max_length=160)
    target_scope_hash: str = Field(min_length=64, max_length=64)
    capability: str = Field(min_length=1, max_length=160)
    oos_interval_id: str = Field(min_length=1, max_length=160)
    oos_interval_hash: str = Field(min_length=64, max_length=64)
    evidence_scope: str = Field(pattern="^(RETROSPECTIVE_RESEARCH_ONLY|GAP_ONLY)$")
    signal_evidence_level: str = Field(min_length=1, max_length=160)
    effective_cutoff_date: str = Field(min_length=10, max_length=10)
    program_id: str = Field(min_length=1, max_length=160)
    binding_version_id: str = Field(min_length=1, max_length=160)
    source_run_id: str = Field(min_length=1, max_length=160)
    lineage_source_type: str = Field(pattern="^(PHASE0A_AUDIT|ONLINE_REVIEW|ONLINE_LIST|HISTORICAL_REPLAY)$")
    review_run_id: str | None = Field(default=None, max_length=160)
    list_version_id: str | None = Field(default=None, max_length=160)
    plan_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "manifest_sha256",
        "selection_runtime_semantics_hash",
        "package_effective_config_hash",
        "calendar_hash",
        "stable_signal_semantics_hash",
        "canonical_signal_scope_hash",
        "phase0a_audit_manifest_hash",
        "handoff_readiness_hash",
        "admission_scope_hash",
        "signal_source_revision_set_hash",
        "phase0a_signal_context_hash",
        "evidence_bundle_hash",
        "selection_evidence_hash",
        "selection_run_content_hash",
        "selection_score_artifact_hash",
        "runtime_profile_version_hash",
        "hmm_snapshot_hash",
        "risk_policy_hash",
        "universe_policy_hash",
        "symbol_normalization_policy_hash",
        "target_scope_hash",
        "oos_interval_hash",
        "plan_hash",
    )
    @classmethod
    def _sha256(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        if value is None:
            return None
        return _require_sha256(value, field_name=info.field_name)

    @field_validator("decision_cutoff_ts", "evidence_available_at")
    @classmethod
    def _aware(cls, value: datetime, info) -> datetime:  # type: ignore[no-untyped-def]
        return _require_aware(value, field_name=info.field_name)

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"plan_hash"})

    @model_validator(mode="after")
    def _validate_plan_hash(self) -> "CapturePlan":
        try:
            decision_date = date.fromisoformat(self.decision_as_of_trade_date)
            selection_date = date.fromisoformat(self.selection_as_of_trade_date)
            target_date = date.fromisoformat(self.target_trade_date)
            effective_cutoff_date = date.fromisoformat(self.effective_cutoff_date)
        except ValueError as exc:
            raise ValueError("capture plan trade dates must be ISO calendar dates") from exc
        if selection_date != decision_date or target_date <= decision_date or effective_cutoff_date != decision_date:
            raise ValueError("capture plan dates do not satisfy the frozen decision/selection/target contract")
        if self.hmm_snapshot_status == "NOT_APPLICABLE":
            if self.hmm_snapshot_id is not None or self.hmm_snapshot_hash is not None:
                raise ValueError("NOT_APPLICABLE HMM state cannot carry a snapshot reference")
        elif not self.hmm_snapshot_id or not self.hmm_snapshot_hash:
            raise ValueError("applicable HMM state requires an immutable snapshot reference")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.plan_hash is not None and self.plan_hash != digest:
            raise ValueError("plan_hash does not match capture plan")
        object.__setattr__(self, "plan_hash", digest)
        return self


class CaptureBatchRequest(BaseModel):
    """One explicit historical-research capture request; no latest-state lookup."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    capture_batch_id: str = Field(min_length=1, max_length=160)
    binding: TraceCaptureBinding
    plans: tuple[CapturePlan, ...] = Field(min_length=1)
    data_source: str = "DB_HISTORICAL"
    execution_origin: str = "ADVISORY_RUN"
    research_scope: str = "HISTORICAL_RESEARCH_ONLY"
    execution_prohibited: bool = True
    capture_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("capture_request_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="capture_request_hash") if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        binding_payload = self.binding.model_dump(
            mode="json",
            exclude={"control_binding_event_hash", "capture_batch_id", "capture_fencing_token", "binding_hash"},
        )
        return {
            "schema_version": CAPTURE_BATCH_SCHEMA_VERSION,
            "binding": binding_payload,
            "plans": [plan.model_dump(mode="json") for plan in sorted(self.plans, key=lambda item: str(item.plan_hash))],
            "data_source": self.data_source,
            "execution_origin": self.execution_origin,
            "research_scope": self.research_scope,
            "execution_prohibited": self.execution_prohibited,
        }

    @model_validator(mode="after")
    def _validate_request(self) -> "CaptureBatchRequest":
        if (
            self.data_source != "DB_HISTORICAL"
            or self.execution_origin != "ADVISORY_RUN"
            or self.research_scope != "HISTORICAL_RESEARCH_ONLY"
            or self.execution_prohibited is not True
        ):
            raise ValueError("Phase 1 capture requests are restricted to historical advisory research")
        if self.binding.capture_batch_id != self.capture_batch_id or self.binding.capture_fencing_token != 1:
            raise ValueError("new capture batch binding must reference this batch with fencing token one")
        plan_keys = {(plan.selection_run_id, plan.package_id, plan.manifest_sha256) for plan in self.plans}
        if len(plan_keys) != len(self.plans):
            raise ValueError("capture batch plans must have unique selection/package/manifest identities")
        for plan in self.plans:
            if (
                plan.handoff_readiness_hash != self.binding.handoff_readiness_hash
                or plan.admission_scope_id != self.binding.admission_scope_id
                or plan.admission_scope_hash != self.binding.admission_scope_hash
            ):
                raise ValueError("capture plan does not match capture binding scope")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.capture_request_hash is not None and self.capture_request_hash != digest:
            raise ValueError("capture_request_hash does not match capture request")
        object.__setattr__(self, "capture_request_hash", digest)
        return self


CaptureBatchRequestLike = CaptureBatchRequest | LabelCaptureBatchRequestV2


def capture_request_schema(request: CaptureBatchRequestLike) -> str:
    """Return the one supported schema for an already typed request."""

    if isinstance(request, CaptureBatchRequest):
        return CAPTURE_BATCH_SCHEMA_VERSION
    if isinstance(request, LabelCaptureBatchRequestV2):
        return LABEL_CAPTURE_BATCH_SCHEMA_VERSION
    raise TypeError(f"unsupported capture request type: {type(request)!r}")


def capture_request_purpose(request: CaptureBatchRequestLike) -> str:
    if isinstance(request, CaptureBatchRequest):
        return OBSERVATION_CAPTURE_PURPOSE
    if isinstance(request, LabelCaptureBatchRequestV2):
        return LABEL_CAPTURE_PURPOSE
    raise TypeError(f"unsupported capture request type: {type(request)!r}")


def capture_request_hash(request: CaptureBatchRequestLike) -> str:
    if request.capture_request_hash is None:
        raise ValueError("capture request must expose a canonical request hash")
    return str(request.capture_request_hash)


def _capture_binding_payload(request: CaptureBatchRequestLike) -> dict[str, Any]:
    """Return the persisted binding payload for one explicitly typed request."""

    return request.binding.model_dump(mode="json")


def _capture_control_binding_event_hash(request: CaptureBatchRequestLike) -> str:
    """Persist historical control provenance without reading current control state."""

    if isinstance(request, CaptureBatchRequest):
        return request.binding.control_binding_event_hash
    if isinstance(request, LabelCaptureBatchRequestV2):
        return request.binding.source_control_binding_event_hash
    raise TypeError(f"unsupported capture request type: {type(request)!r}")


def _capture_handoff_readiness_hash(request: CaptureBatchRequestLike) -> str:
    return request.binding.handoff_readiness_hash


def _capture_admission_scope_id(request: CaptureBatchRequestLike) -> str:
    return request.binding.admission_scope_id


def _capture_admission_scope_hash(request: CaptureBatchRequestLike) -> str:
    return request.binding.admission_scope_hash


def parse_capture_batch_request_payload(payload: Mapping[str, Any]) -> CaptureBatchRequestLike:
    """Parse one explicitly tagged raw payload without a schema fallback path.

    v1 intentionally has no serialized purpose field, because adding one would
    change its frozen canonical bytes.  Its schema alone therefore determines
    the historical observation-capture purpose.  v2 requires both tags.
    """

    schema_version = payload.get("schema_version")
    purpose = payload.get("capture_purpose")
    if schema_version == CAPTURE_BATCH_SCHEMA_VERSION:
        if purpose not in (None, OBSERVATION_CAPTURE_PURPOSE):
            raise ValueError("v1 capture request has an invalid capture purpose")
        model_payload = dict(payload)
        model_payload.pop("schema_version", None)
        model_payload.pop("capture_purpose", None)
        return CaptureBatchRequest.model_validate(model_payload)
    if schema_version == LABEL_CAPTURE_BATCH_SCHEMA_VERSION:
        if purpose != LABEL_CAPTURE_PURPOSE:
            raise ValueError("v2 label capture request requires LABEL_CAPTURE_V1 purpose")
        return LabelCaptureBatchRequestV2.model_validate(payload)
    raise ValueError("unsupported capture request schema or purpose")


class CaptureMembership(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_role: str = Field(min_length=1, max_length=80)
    evidence_id: str = Field(min_length=1, max_length=160)
    evidence_content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("evidence_content_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="evidence_content_hash")

    @property
    def content_key(self) -> tuple[str, str]:
        return self.evidence_role, self.evidence_id


class CaptureBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request: CaptureBatchRequestLike
    status: CaptureBatchStatus
    row_version: int = Field(ge=1)
    fencing_token: int = Field(ge=1)
    lease_expires_at: datetime | None = None
    capture_attempt_no: int = Field(ge=1)
    predecessor_capture_batch_id: str | None = None
    membership_count: int | None = Field(default=None, ge=0)
    membership_hash: str | None = Field(default=None, min_length=64, max_length=64)
    capture_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)
    reason_codes: tuple[str, ...] = ()
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at", "lease_expires_at")
    @classmethod
    def _aware(cls, value: datetime | None, info) -> datetime | None:  # type: ignore[no-untyped-def]
        return _require_aware(value, field_name=info.field_name) if value is not None else None

    @field_validator("membership_hash", "capture_receipt_hash")
    @classmethod
    def _hash(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _validate_state(self) -> "CaptureBatch":
        if self.status is CaptureBatchStatus.RUNNING:
            if self.lease_expires_at is None:
                raise ValueError("RUNNING capture batch requires a lease")
        elif self.lease_expires_at is not None:
            raise ValueError("non-RUNNING capture batch cannot retain a lease")
        if self.status is CaptureBatchStatus.COMPLETE:
            if self.membership_count is None or self.membership_hash is None or self.capture_receipt_hash is None:
                raise ValueError("COMPLETE capture batch requires sealed membership and receipt")
        elif any(value is not None for value in (self.membership_count, self.membership_hash, self.capture_receipt_hash)):
            raise ValueError("only COMPLETE capture batch can expose a sealed receipt")
        return self


class TraceCaptureGap(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity: ExpectedTraceIdentity
    reason_code: str = Field(min_length=1, max_length=160)
    gap_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("gap_content_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="gap_content_hash") if value is not None else None

    @model_validator(mode="after")
    def _validate_gap(self) -> "TraceCaptureGap":
        digest = canonical_json_sha256(
            {"identity": self.identity.model_dump(mode="json"), "reason_code": self.reason_code}
        )
        if self.gap_content_hash is not None and self.gap_content_hash != digest:
            raise ValueError("gap_content_hash does not match trace gap")
        object.__setattr__(self, "gap_content_hash", digest)
        return self


class CaptureBatchRepository(Protocol):
    def create(self, request: CaptureBatchRequestLike) -> CaptureBatch: ...

    def acquire(self, *, capture_batch_id: str, expected_row_version: int, lease_seconds: int) -> CaptureBatch: ...

    def add_membership(
        self,
        *,
        capture_batch_id: str,
        expected_row_version: int,
        fencing_token: int,
        membership: CaptureMembership,
    ) -> CaptureBatch: ...

    def complete(self, *, capture_batch_id: str, expected_row_version: int, fencing_token: int) -> CaptureBatch: ...

    def expire(self, *, capture_batch_id: str, expected_row_version: int, fencing_token: int) -> CaptureBatch: ...

    def fail(
        self,
        *,
        capture_batch_id: str,
        expected_row_version: int,
        fencing_token: int,
        reason_codes: tuple[str, ...],
    ) -> CaptureBatch: ...

    def recover(
        self,
        *,
        request: CaptureBatchRequestLike,
        predecessor_capture_batch_id: str,
        expected_predecessor_row_version: int,
        predecessor_fencing_token: int,
    ) -> CaptureBatch: ...

    def get(self, capture_batch_id: str) -> CaptureBatch: ...


class InMemoryCaptureBatchRepository:
    """Deterministic state-machine oracle for Phase 1C contract tests."""

    def __init__(self, *, now_provider: Callable[[], datetime] | None = None) -> None:
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self._batches: dict[str, CaptureBatch] = {}
        self._by_request_hash: dict[str, list[str]] = {}
        self._memberships: dict[str, dict[tuple[str, str], CaptureMembership]] = {}

    def create(self, request: CaptureBatchRequestLike) -> CaptureBatch:
        request_hash = capture_request_hash(request)
        existing_by_id = self._batches.get(request.capture_batch_id)
        if existing_by_id is not None:
            if existing_by_id.request == request:
                return existing_by_id
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "same capture batch id has different content")
        existing_ids = self._by_request_hash.get(request_hash, [])
        if existing_ids:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture retry requires explicit recovery from its predecessor")
        now = _require_aware(self._now_provider(), field_name="now_provider")
        batch = CaptureBatch(
            request=request,
            status=CaptureBatchStatus.PLANNED,
            row_version=1,
            fencing_token=1,
            capture_attempt_no=1,
            created_at=now,
            updated_at=now,
        )
        self._batches[request.capture_batch_id] = batch
        self._by_request_hash.setdefault(request_hash, []).append(request.capture_batch_id)
        self._memberships[request.capture_batch_id] = {}
        return batch

    def recover(
        self,
        *,
        request: CaptureBatchRequestLike,
        predecessor_capture_batch_id: str,
        expected_predecessor_row_version: int,
        predecessor_fencing_token: int,
    ) -> CaptureBatch:
        predecessor = self._get(predecessor_capture_batch_id)
        if predecessor.row_version != expected_predecessor_row_version:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture predecessor row version is stale")
        if predecessor.fencing_token != predecessor_fencing_token:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_FENCING_INVALID, "capture predecessor fencing token is stale")
        if predecessor.status not in {
            CaptureBatchStatus.FAILED,
            CaptureBatchStatus.EXPIRED,
            CaptureBatchStatus.ABORTED,
        }:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "capture recovery requires a terminal predecessor")
        if capture_request_hash(predecessor.request) != capture_request_hash(request):
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture recovery request does not match predecessor semantics")
        if (
            capture_request_schema(predecessor.request) != capture_request_schema(request)
            or capture_request_purpose(predecessor.request) != capture_request_purpose(request)
        ):
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture recovery request does not match predecessor schema")
        if request.capture_batch_id in self._batches:
            existing = self._batches[request.capture_batch_id]
            if existing.predecessor_capture_batch_id == predecessor_capture_batch_id and existing.request == request:
                return existing
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "recovery capture batch id already exists")
        if any(
            self._batches[batch_id].status in {CaptureBatchStatus.PLANNED, CaptureBatchStatus.RUNNING}
            for batch_id in self._by_request_hash.get(capture_request_hash(request), [])
        ):
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "same capture request already has an active batch")
        if any(
            self._batches[batch_id].predecessor_capture_batch_id == predecessor_capture_batch_id
            for batch_id in self._by_request_hash.get(capture_request_hash(request), [])
        ):
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture predecessor already has a recovery successor")
        now = _require_aware(self._now_provider(), field_name="now_provider")
        batch = CaptureBatch(
            request=request,
            status=CaptureBatchStatus.PLANNED,
            row_version=1,
            fencing_token=1,
            capture_attempt_no=predecessor.capture_attempt_no + 1,
            predecessor_capture_batch_id=predecessor_capture_batch_id,
            created_at=now,
            updated_at=now,
        )
        self._batches[request.capture_batch_id] = batch
        self._by_request_hash.setdefault(capture_request_hash(request), []).append(request.capture_batch_id)
        self._memberships[request.capture_batch_id] = {}
        return batch

    def acquire(self, *, capture_batch_id: str, expected_row_version: int, lease_seconds: int) -> CaptureBatch:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        batch = self._get(capture_batch_id)
        now = _require_aware(self._now_provider(), field_name="now_provider")
        if batch.row_version != expected_row_version:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture batch row version is stale")
        if batch.status is not CaptureBatchStatus.PLANNED:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "only PLANNED capture batch can be acquired")
        updated = batch.model_copy(
            update={
                "status": CaptureBatchStatus.RUNNING,
                "row_version": batch.row_version + 1,
                "lease_expires_at": now + timedelta(seconds=lease_seconds),
                "updated_at": now,
            }
        )
        self._batches[capture_batch_id] = updated
        return updated

    def add_membership(
        self,
        *,
        capture_batch_id: str,
        expected_row_version: int,
        fencing_token: int,
        membership: CaptureMembership,
    ) -> CaptureBatch:
        batch = self._require_active(
            capture_batch_id=capture_batch_id,
            expected_row_version=expected_row_version,
            fencing_token=fencing_token,
        )
        members = self._memberships[capture_batch_id]
        existing = members.get(membership.content_key)
        if existing is not None:
            if existing == membership:
                return batch
            raise SourceLedgerError(REASON_CAPTURE_BATCH_MEMBERSHIP_INVALID, "membership identity has conflicting content")
        members[membership.content_key] = membership
        now = _require_aware(self._now_provider(), field_name="now_provider")
        updated = batch.model_copy(update={"row_version": batch.row_version + 1, "updated_at": now})
        self._batches[capture_batch_id] = updated
        return updated

    def complete(self, *, capture_batch_id: str, expected_row_version: int, fencing_token: int) -> CaptureBatch:
        batch = self._require_active(
            capture_batch_id=capture_batch_id,
            expected_row_version=expected_row_version,
            fencing_token=fencing_token,
        )
        members = self._memberships[capture_batch_id]
        ordered = [
            member.model_dump(mode="json")
            for _, member in sorted(members.items(), key=lambda item: item[0])
        ]
        membership_hash = canonical_json_sha256(ordered)
        receipt_hash = canonical_json_sha256(
            {
                "capture_request_hash": batch.request.capture_request_hash,
                "capture_batch_id": capture_batch_id,
                "membership_count": len(ordered),
                "membership_hash": membership_hash,
            }
        )
        now = _require_aware(self._now_provider(), field_name="now_provider")
        updated = batch.model_copy(
            update={
                "status": CaptureBatchStatus.COMPLETE,
                "row_version": batch.row_version + 1,
                "lease_expires_at": None,
                "membership_count": len(ordered),
                "membership_hash": membership_hash,
                "capture_receipt_hash": receipt_hash,
                "updated_at": now,
            }
        )
        self._batches[capture_batch_id] = updated
        return updated

    def fail(
        self,
        *,
        capture_batch_id: str,
        expected_row_version: int,
        fencing_token: int,
        reason_codes: tuple[str, ...],
    ) -> CaptureBatch:
        if not reason_codes:
            raise ValueError("failed capture batch requires reason codes")
        batch = self._require_active(
            capture_batch_id=capture_batch_id,
            expected_row_version=expected_row_version,
            fencing_token=fencing_token,
        )
        now = _require_aware(self._now_provider(), field_name="now_provider")
        updated = batch.model_copy(
            update={
                "status": CaptureBatchStatus.FAILED,
                "row_version": batch.row_version + 1,
                "lease_expires_at": None,
                "reason_codes": tuple(sorted(set(reason_codes))),
                "updated_at": now,
            }
        )
        self._batches[capture_batch_id] = updated
        return updated

    def expire(self, *, capture_batch_id: str, expected_row_version: int, fencing_token: int) -> CaptureBatch:
        batch = self._get(capture_batch_id)
        now = _require_aware(self._now_provider(), field_name="now_provider")
        if batch.row_version != expected_row_version:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture batch row version is stale")
        if batch.status is not CaptureBatchStatus.RUNNING:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "only RUNNING capture batch can expire")
        if batch.fencing_token != fencing_token:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_FENCING_INVALID, "capture batch fencing token is stale")
        if batch.lease_expires_at is None or batch.lease_expires_at > now:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "capture batch lease is not expired")
        updated = batch.model_copy(
            update={
                "status": CaptureBatchStatus.EXPIRED,
                "row_version": batch.row_version + 1,
                "lease_expires_at": None,
                "reason_codes": (REASON_CAPTURE_BATCH_LEASE_EXPIRED,),
                "updated_at": now,
            }
        )
        self._batches[capture_batch_id] = updated
        return updated

    def get(self, capture_batch_id: str) -> CaptureBatch:
        return self._get(capture_batch_id)

    def memberships_for(self, capture_batch_id: str) -> tuple[CaptureMembership, ...]:
        self._get(capture_batch_id)
        return tuple(
            sorted(
                self._memberships[capture_batch_id].values(),
                key=lambda item: item.content_key,
            )
        )

    def _get(self, capture_batch_id: str) -> CaptureBatch:
        try:
            return self._batches[capture_batch_id]
        except KeyError as exc:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "capture batch does not exist") from exc

    def _require_active(self, *, capture_batch_id: str, expected_row_version: int, fencing_token: int) -> CaptureBatch:
        batch = self._get(capture_batch_id)
        now = _require_aware(self._now_provider(), field_name="now_provider")
        if batch.row_version != expected_row_version:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture batch row version is stale")
        if batch.status is not CaptureBatchStatus.RUNNING:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "capture batch is not RUNNING")
        if batch.fencing_token != fencing_token:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_FENCING_INVALID, "capture batch fencing token is stale")
        if batch.lease_expires_at is None or batch.lease_expires_at <= now:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_LEASE_EXPIRED, "capture batch lease has expired")
        return batch


class InMemoryTraceAdmissionValidator:
    """Real contract validator for deterministic tests, not a permissive fixture."""

    def __init__(self, *, batches: InMemoryCaptureBatchRepository) -> None:
        self._batches = batches

    def validate(self, *, envelope: StageTraceEnvelope, binding: TraceCaptureBinding, conn: Any | None = None) -> None:
        del conn
        _require_historical_trace_identity(envelope)
        identity = ExpectedTraceIdentity.from_envelope(envelope, binding=binding)
        batch = self._batches.get(binding.capture_batch_id)
        if batch.request.binding != binding:
            raise SourceLedgerError(REASON_TRACE_ADMISSION_BINDING_INVALID, "capture binding does not match batch request")
        if batch.status is not CaptureBatchStatus.RUNNING:
            raise SourceLedgerError(REASON_TRACE_ADMISSION_BATCH_INVALID, "capture batch is not RUNNING")
        if batch.fencing_token != binding.capture_fencing_token:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_FENCING_INVALID, "capture binding fencing token is stale")
        matching = [
            plan
            for plan in batch.request.plans
            if (plan.selection_run_id, plan.package_id, plan.manifest_sha256, plan.decision_as_of_trade_date)
            == (*identity.natural_key[:4],)
        ]
        if len(matching) != 1:
            raise SourceLedgerError(REASON_TRACE_ADMISSION_BATCH_INVALID, "capture batch has no matching frozen plan")
        self._batches._require_active(
            capture_batch_id=binding.capture_batch_id,
            expected_row_version=batch.row_version,
            fencing_token=binding.capture_fencing_token,
        )


class InMemoryTraceCaptureGapRepository:
    def __init__(self) -> None:
        self._gaps: dict[str, TraceCaptureGap] = {}

    def record(self, *, identity: ExpectedTraceIdentity, reason_code: str) -> TraceCaptureGap:
        gap = TraceCaptureGap(identity=identity, reason_code=reason_code)
        existing = self._gaps.get(str(gap.gap_content_hash))
        if existing is not None:
            if existing == gap:
                return existing
            raise SourceLedgerError(REASON_TRACE_GAP_CONFLICT, "same trace gap hash has different content")
        self._gaps[str(gap.gap_content_hash)] = gap
        return gap

    def __call__(self, *, identity: ExpectedTraceIdentity, reason_code: str) -> None:
        self.record(identity=identity, reason_code=reason_code)

    def list(self) -> tuple[TraceCaptureGap, ...]:
        return tuple(self._gaps[key] for key in sorted(self._gaps))


ConnFactory = Callable[[], Iterator[Any]]


def _transactional_conn_factory() -> Iterator[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class PostgresCaptureBatchRepository:
    """Transactional capture-batch persistence with no latest-state reconstruction."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def create(self, request: CaptureBatchRequestLike) -> CaptureBatch:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM app.advisory_capture_batch WHERE capture_batch_id = %s FOR UPDATE",
                    (request.capture_batch_id,),
                )
                existing = cur.fetchone()
                if existing is not None:
                    batch = self._load_locked(cur, dict(existing))
                    if batch.request == request:
                        return batch
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "same capture batch id has different content")
                cur.execute(
                    "SELECT 1 FROM app.advisory_capture_batch WHERE capture_request_hash = %s FOR UPDATE",
                    (capture_request_hash(request),),
                )
                if cur.fetchone() is not None:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture retry requires explicit recovery from its predecessor")
                try:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_capture_batch (
                            capture_batch_id, capture_request_hash, request_payload_jsonb, binding_jsonb,
                            control_binding_event_hash, handoff_readiness_hash, admission_scope_id,
                            admission_scope_hash, capture_request_schema_version, capture_purpose,
                            capture_status, row_version, fencing_token, capture_attempt_no
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PLANNED', 1, 1, 1)
                        RETURNING *
                        """,
                        (
                            request.capture_batch_id,
                            capture_request_hash(request),
                            psycopg2.extras.Json(canonicalize(request.canonical_payload())),
                            psycopg2.extras.Json(canonicalize(_capture_binding_payload(request))),
                            _capture_control_binding_event_hash(request),
                            _capture_handoff_readiness_hash(request),
                            _capture_admission_scope_id(request),
                            _capture_admission_scope_hash(request),
                            capture_request_schema(request),
                            capture_request_purpose(request),
                        ),
                    )
                    row = dict(cur.fetchone())
                    if isinstance(request, CaptureBatchRequest):
                        self._insert_plans(cur, request)
                except psycopg2.IntegrityError as exc:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "database rejected capture batch identity") from exc
                return self._load_locked(cur, row)

    def acquire(self, *, capture_batch_id: str, expected_row_version: int, lease_seconds: int) -> CaptureBatch:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                row = self._select_batch_locked(cur, capture_batch_id)
                self._require_row_version(row, expected_row_version)
                if str(row["capture_status"]) != CaptureBatchStatus.PLANNED.value:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "only PLANNED capture batch can be acquired")
                cur.execute(
                    """
                    UPDATE app.advisory_capture_batch
                    SET capture_status = 'RUNNING', row_version = row_version + 1,
                        lease_expires_at = clock_timestamp() + make_interval(secs => %s)
                    WHERE capture_batch_id = %s
                    RETURNING *
                    """,
                    (lease_seconds, capture_batch_id),
                )
                return self._load_locked(cur, dict(cur.fetchone()))

    def add_membership(
        self,
        *,
        capture_batch_id: str,
        expected_row_version: int,
        fencing_token: int,
        membership: CaptureMembership,
    ) -> CaptureBatch:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                row = self._select_batch_locked(cur, capture_batch_id)
                self._require_active_row(row, expected_row_version=expected_row_version, fencing_token=fencing_token, cur=cur)
                cur.execute(
                    """
                    SELECT evidence_content_hash FROM app.advisory_capture_batch_evidence_membership
                    WHERE capture_batch_id = %s AND evidence_role = %s AND evidence_id = %s
                    FOR UPDATE
                    """,
                    (capture_batch_id, membership.evidence_role, membership.evidence_id),
                )
                existing = cur.fetchone()
                if existing is not None:
                    if str(existing["evidence_content_hash"]) == membership.evidence_content_hash:
                        return self._load_locked(cur, row)
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_MEMBERSHIP_INVALID, "membership identity has conflicting content")
                try:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_capture_batch_evidence_membership (
                            capture_batch_id, evidence_role, evidence_id, evidence_content_hash, fencing_token
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            capture_batch_id,
                            membership.evidence_role,
                            membership.evidence_id,
                            membership.evidence_content_hash,
                            fencing_token,
                        ),
                    )
                    cur.execute(
                        """
                        UPDATE app.advisory_capture_batch SET row_version = row_version + 1
                        WHERE capture_batch_id = %s RETURNING *
                        """,
                        (capture_batch_id,),
                    )
                except psycopg2.IntegrityError as exc:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_MEMBERSHIP_INVALID, "database rejected capture membership") from exc
                return self._load_locked(cur, dict(cur.fetchone()))

    def complete(self, *, capture_batch_id: str, expected_row_version: int, fencing_token: int) -> CaptureBatch:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                row = self._select_batch_locked(cur, capture_batch_id)
                self._require_active_row(row, expected_row_version=expected_row_version, fencing_token=fencing_token, cur=cur)
                cur.execute(
                    """
                    SELECT evidence_role, evidence_id, evidence_content_hash
                    FROM app.advisory_capture_batch_evidence_membership
                    WHERE capture_batch_id = %s ORDER BY evidence_role, evidence_id
                    """,
                    (capture_batch_id,),
                )
                memberships = [
                    {
                        "evidence_role": str(item["evidence_role"]),
                        "evidence_id": str(item["evidence_id"]),
                        "evidence_content_hash": str(item["evidence_content_hash"]),
                    }
                    for item in cur.fetchall()
                ]
                membership_hash = canonical_json_sha256(memberships)
                receipt_hash = canonical_json_sha256(
                    {
                        "capture_request_hash": str(row["capture_request_hash"]),
                        "capture_batch_id": capture_batch_id,
                        "membership_count": len(memberships),
                        "membership_hash": membership_hash,
                    }
                )
                cur.execute(
                    """
                    UPDATE app.advisory_capture_batch
                    SET capture_status = 'COMPLETE', row_version = row_version + 1,
                        lease_expires_at = NULL, membership_count = %s, membership_hash = %s,
                        capture_receipt_hash = %s
                    WHERE capture_batch_id = %s
                    RETURNING *
                    """,
                    (len(memberships), membership_hash, receipt_hash, capture_batch_id),
                )
                return self._load_locked(cur, dict(cur.fetchone()))

    def fail(
        self,
        *,
        capture_batch_id: str,
        expected_row_version: int,
        fencing_token: int,
        reason_codes: tuple[str, ...],
    ) -> CaptureBatch:
        if not reason_codes:
            raise ValueError("failed capture batch requires reason codes")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                row = self._select_batch_locked(cur, capture_batch_id)
                self._require_active_row(row, expected_row_version=expected_row_version, fencing_token=fencing_token, cur=cur)
                cur.execute(
                    """
                    UPDATE app.advisory_capture_batch
                    SET capture_status = 'FAILED', row_version = row_version + 1,
                        lease_expires_at = NULL, reason_codes = %s
                    WHERE capture_batch_id = %s
                    RETURNING *
                    """,
                    (psycopg2.extras.Json(sorted(set(reason_codes))), capture_batch_id),
                )
                return self._load_locked(cur, dict(cur.fetchone()))

    def expire(self, *, capture_batch_id: str, expected_row_version: int, fencing_token: int) -> CaptureBatch:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                row = self._select_batch_locked(cur, capture_batch_id)
                self._require_row_version(row, expected_row_version)
                if str(row["capture_status"]) != CaptureBatchStatus.RUNNING.value:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "only RUNNING capture batch can expire")
                if int(row["fencing_token"]) != fencing_token:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_FENCING_INVALID, "capture batch fencing token is stale")
                cur.execute("SELECT clock_timestamp() AS database_now")
                if row["lease_expires_at"] is None or row["lease_expires_at"] > cur.fetchone()["database_now"]:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "capture batch lease is not expired")
                cur.execute(
                    """
                    UPDATE app.advisory_capture_batch
                    SET capture_status = 'EXPIRED', row_version = row_version + 1,
                        lease_expires_at = NULL, reason_codes = %s
                    WHERE capture_batch_id = %s
                    RETURNING *
                    """,
                    (psycopg2.extras.Json([REASON_CAPTURE_BATCH_LEASE_EXPIRED]), capture_batch_id),
                )
                return self._load_locked(cur, dict(cur.fetchone()))

    def recover(
        self,
        *,
        request: CaptureBatchRequestLike,
        predecessor_capture_batch_id: str,
        expected_predecessor_row_version: int,
        predecessor_fencing_token: int,
    ) -> CaptureBatch:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                predecessor = self._select_batch_locked(cur, predecessor_capture_batch_id)
                self._require_row_version(predecessor, expected_predecessor_row_version)
                if int(predecessor["fencing_token"]) != predecessor_fencing_token:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_FENCING_INVALID, "capture predecessor fencing token is stale")
                if str(predecessor["capture_status"]) not in {
                    CaptureBatchStatus.FAILED.value,
                    CaptureBatchStatus.EXPIRED.value,
                    CaptureBatchStatus.ABORTED.value,
                }:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "capture recovery requires a terminal predecessor")
                if str(predecessor["capture_request_hash"]) != capture_request_hash(request):
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture recovery request does not match predecessor semantics")
                if (
                    str(predecessor["capture_request_schema_version"]) != capture_request_schema(request)
                    or str(predecessor["capture_purpose"]) != capture_request_purpose(request)
                ):
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture recovery request does not match predecessor schema")
                cur.execute(
                    """
                    SELECT 1 FROM app.advisory_capture_batch
                    WHERE capture_request_hash = %s AND capture_status IN ('PLANNED', 'RUNNING') FOR UPDATE
                    """,
                    (capture_request_hash(request),),
                )
                if cur.fetchone() is not None:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "same capture request already has an active batch")
                cur.execute(
                    "SELECT 1 FROM app.advisory_capture_batch WHERE predecessor_capture_batch_id = %s FOR UPDATE",
                    (predecessor_capture_batch_id,),
                )
                if cur.fetchone() is not None:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture predecessor already has a recovery successor")
                next_attempt = int(predecessor["capture_attempt_no"]) + 1
                try:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_capture_batch (
                            capture_batch_id, capture_request_hash, request_payload_jsonb, binding_jsonb,
                            control_binding_event_hash, handoff_readiness_hash, admission_scope_id,
                            admission_scope_hash, capture_request_schema_version, capture_purpose,
                            capture_status, row_version, fencing_token, capture_attempt_no,
                            predecessor_capture_batch_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PLANNED', 1, 1, %s, %s)
                        RETURNING *
                        """,
                        (
                            request.capture_batch_id,
                            capture_request_hash(request),
                            psycopg2.extras.Json(canonicalize(request.canonical_payload())),
                            psycopg2.extras.Json(canonicalize(_capture_binding_payload(request))),
                            _capture_control_binding_event_hash(request),
                            _capture_handoff_readiness_hash(request),
                            _capture_admission_scope_id(request),
                            _capture_admission_scope_hash(request),
                            capture_request_schema(request),
                            capture_request_purpose(request),
                            next_attempt,
                            predecessor_capture_batch_id,
                        ),
                    )
                    row = dict(cur.fetchone())
                    if isinstance(request, CaptureBatchRequest):
                        self._insert_plans(cur, request)
                except psycopg2.IntegrityError as exc:
                    raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "database rejected capture recovery") from exc
                return self._load_locked(cur, row)

    def get(self, capture_batch_id: str) -> CaptureBatch:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                row = self._select_batch_locked(cur, capture_batch_id)
                return self._load_locked(cur, row)

    @staticmethod
    def _insert_plans(cur: Any, request: CaptureBatchRequest) -> None:
        for plan in request.plans:
            cur.execute(
                """
                INSERT INTO app.advisory_capture_plan (
                    capture_batch_id, plan_hash, plan_payload_jsonb, selection_run_id, package_id, manifest_sha256,
                    decision_as_of_trade_date, stable_signal_semantics_hash, canonical_signal_scope_hash,
                    phase0a_audit_id, phase0a_audit_manifest_hash, handoff_readiness_hash,
                    admission_scope_id, admission_scope_hash, signal_source_revision_set_id,
                    signal_source_revision_set_hash, program_id, binding_version_id, source_run_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    request.capture_batch_id,
                    plan.plan_hash,
                    psycopg2.extras.Json(canonicalize(plan.model_dump(mode="json"))),
                    plan.selection_run_id,
                    plan.package_id,
                    plan.manifest_sha256,
                    plan.decision_as_of_trade_date,
                    plan.stable_signal_semantics_hash,
                    plan.canonical_signal_scope_hash,
                    plan.phase0a_audit_id,
                    plan.phase0a_audit_manifest_hash,
                    plan.handoff_readiness_hash,
                    plan.admission_scope_id,
                    plan.admission_scope_hash,
                    plan.signal_source_revision_set_id,
                    plan.signal_source_revision_set_hash,
                    plan.program_id,
                    plan.binding_version_id,
                    plan.source_run_id,
                ),
            )

    @staticmethod
    def _require_row_version(row: Mapping[str, Any], expected_row_version: int) -> None:
        if int(row["row_version"]) != expected_row_version:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "capture batch row version is stale")

    def _require_active_row(
        self,
        row: Mapping[str, Any],
        *,
        expected_row_version: int,
        fencing_token: int,
        cur: Any,
    ) -> None:
        self._require_row_version(row, expected_row_version)
        if str(row["capture_status"]) != CaptureBatchStatus.RUNNING.value:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "capture batch is not RUNNING")
        if int(row["fencing_token"]) != fencing_token:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_FENCING_INVALID, "capture batch fencing token is stale")
        cur.execute("SELECT clock_timestamp() AS database_now")
        if row["lease_expires_at"] is None or row["lease_expires_at"] <= cur.fetchone()["database_now"]:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_LEASE_EXPIRED, "capture batch lease has expired")

    @staticmethod
    def _select_batch_locked(cur: Any, capture_batch_id: str) -> dict[str, Any]:
        cur.execute("SELECT * FROM app.advisory_capture_batch WHERE capture_batch_id = %s FOR UPDATE", (capture_batch_id,))
        row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_STATE_INVALID, "capture batch does not exist")
        return dict(row)

    @staticmethod
    def _load_locked(cur: Any, row: Mapping[str, Any]) -> CaptureBatch:
        schema_version = str(row["capture_request_schema_version"])
        purpose = str(row["capture_purpose"])
        payload = canonicalize(dict(row["request_payload_jsonb"]))
        binding_payload = canonicalize(dict(row["binding_jsonb"]))
        if schema_version == CAPTURE_BATCH_SCHEMA_VERSION and purpose == OBSERVATION_CAPTURE_PURPOSE:
            cur.execute(
                "SELECT * FROM app.advisory_capture_plan WHERE capture_batch_id = %s ORDER BY plan_hash FOR KEY SHARE",
                (row["capture_batch_id"],),
            )
            plans = tuple(
                CapturePlan.model_validate(canonicalize(dict(item["plan_payload_jsonb"])))
                for item in cur.fetchall()
            )
            if not plans:
                raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "v1 capture batch is missing capture plans")
            request = CaptureBatchRequest(
                capture_batch_id=str(row["capture_batch_id"]),
                binding=TraceCaptureBinding.model_validate(binding_payload),
                plans=plans,
                capture_request_hash=str(row["capture_request_hash"]),
            )
        elif schema_version == LABEL_CAPTURE_BATCH_SCHEMA_VERSION and purpose == LABEL_CAPTURE_PURPOSE:
            cur.execute(
                "SELECT 1 FROM app.advisory_capture_plan WHERE capture_batch_id = %s FOR KEY SHARE",
                (row["capture_batch_id"],),
            )
            if cur.fetchone() is not None:
                raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "v2 label capture batch cannot contain capture plans")
            payload["binding"] = binding_payload
            payload["capture_batch_id"] = str(row["capture_batch_id"])
            payload["capture_request_hash"] = str(row["capture_request_hash"])
            request = LabelCaptureBatchRequestV2.model_validate(payload)
        else:
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "unsupported persisted capture schema or purpose")
        if capture_request_hash(request) != str(row["capture_request_hash"]):
            raise SourceLedgerError(REASON_CAPTURE_BATCH_CONFLICT, "persisted capture request hash does not match payload")
        return CaptureBatch(
            request=request,
            status=CaptureBatchStatus(str(row["capture_status"])),
            row_version=int(row["row_version"]),
            fencing_token=int(row["fencing_token"]),
            lease_expires_at=row["lease_expires_at"],
            capture_attempt_no=int(row["capture_attempt_no"]),
            predecessor_capture_batch_id=(
                str(row["predecessor_capture_batch_id"]) if row["predecessor_capture_batch_id"] else None
            ),
            membership_count=int(row["membership_count"]) if row["membership_count"] is not None else None,
            membership_hash=str(row["membership_hash"]) if row["membership_hash"] else None,
            capture_receipt_hash=str(row["capture_receipt_hash"]) if row["capture_receipt_hash"] else None,
            reason_codes=tuple(str(item) for item in (row["reason_codes"] or [])),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


class PostgresTraceAdmissionValidator:
    """Validate persisted binding/batch state inside the trace INSERT transaction."""

    def validate(self, *, envelope: StageTraceEnvelope, binding: TraceCaptureBinding, conn: Any | None = None) -> None:
        if conn is None:
            raise SourceLedgerError(REASON_TRACE_ADMISSION_BATCH_INVALID, "PostgreSQL admission requires an active transaction")
        _require_historical_trace_identity(envelope)
        identity = ExpectedTraceIdentity.from_envelope(envelope, binding=binding)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM app.advisory_phase1_control_binding_event WHERE binding_event_hash = %s FOR KEY SHARE",
                (binding.control_binding_event_hash,),
            )
            control = cur.fetchone()
            if control is None:
                raise SourceLedgerError(REASON_TRACE_ADMISSION_BINDING_INVALID, "trace control binding does not exist")
            expected_config = binding.model_dump(mode="json", exclude={"control_binding_event_hash"})
            if (
                str(control["control_type"]) != "TRACE_CAPTURE"
                or not bool(control["enabled"])
                or str(control["admission_scope_set_hash"] or "") != binding.admission_scope_hash
                or canonicalize(dict(control["config_payload_jsonb"])) != canonicalize(expected_config)
            ):
                raise SourceLedgerError(REASON_TRACE_ADMISSION_BINDING_INVALID, "trace control binding is disabled or divergent")
            cur.execute(
                "SELECT *, clock_timestamp() AS database_now FROM app.advisory_capture_batch WHERE capture_batch_id = %s FOR UPDATE",
                (binding.capture_batch_id,),
            )
            batch = cur.fetchone()
            if batch is None:
                raise SourceLedgerError(REASON_TRACE_ADMISSION_BATCH_INVALID, "capture batch does not exist")
            if (
                str(batch["capture_status"]) != CaptureBatchStatus.RUNNING.value
                or int(batch["fencing_token"]) != binding.capture_fencing_token
                or str(batch["control_binding_event_hash"]) != binding.control_binding_event_hash
                or str(batch["handoff_readiness_hash"]) != binding.handoff_readiness_hash
                or str(batch["admission_scope_id"]) != binding.admission_scope_id
                or str(batch["admission_scope_hash"]) != binding.admission_scope_hash
            ):
                raise SourceLedgerError(REASON_TRACE_ADMISSION_BATCH_INVALID, "capture batch identity does not match trace binding")
            if batch["lease_expires_at"] is None or batch["lease_expires_at"] <= batch["database_now"]:
                raise SourceLedgerError(REASON_CAPTURE_BATCH_LEASE_EXPIRED, "capture batch lease has expired")
            cur.execute(
                """
                SELECT 1 FROM app.advisory_capture_plan
                WHERE capture_batch_id = %s AND selection_run_id = %s AND package_id = %s
                  AND manifest_sha256 = %s AND decision_as_of_trade_date = %s
                FOR KEY SHARE
                """,
                (binding.capture_batch_id, *identity.natural_key[:4]),
            )
            if cur.fetchone() is None:
                raise SourceLedgerError(REASON_TRACE_ADMISSION_BATCH_INVALID, "capture batch has no matching frozen plan")


class PostgresTraceCaptureGapRepository:
    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def record(self, *, identity: ExpectedTraceIdentity, reason_code: str) -> TraceCaptureGap:
        gap = TraceCaptureGap(identity=identity, reason_code=reason_code)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM app.advisory_capture_gap WHERE gap_content_hash = %s FOR UPDATE",
                    (gap.gap_content_hash,),
                )
                existing = cur.fetchone()
                if existing is not None:
                    persisted = _gap_from_row(dict(existing))
                    if persisted == gap:
                        return persisted
                    raise SourceLedgerError(REASON_TRACE_GAP_CONFLICT, "same trace gap hash has different content")
                try:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_capture_gap (
                            capture_gap_id, selection_run_id, package_id, manifest_sha256,
                            decision_as_of_trade_date, capture_policy_hash, reason_code, gap_content_hash
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                        """,
                        (
                            f"acg_{str(gap.gap_content_hash)[:20]}",
                            identity.selection_run_id,
                            identity.package_id,
                            identity.manifest_sha256,
                            identity.decision_as_of_trade_date,
                            identity.capture_policy_hash,
                            reason_code,
                            gap.gap_content_hash,
                        ),
                    )
                except psycopg2.IntegrityError as exc:
                    raise SourceLedgerError(REASON_TRACE_GAP_CONFLICT, "database rejected trace gap identity") from exc
                return _gap_from_row(dict(cur.fetchone()))

    def __call__(self, *, identity: ExpectedTraceIdentity, reason_code: str) -> None:
        self.record(identity=identity, reason_code=reason_code)


def _gap_from_row(row: Mapping[str, Any]) -> TraceCaptureGap:
    return TraceCaptureGap(
        identity=ExpectedTraceIdentity(
            selection_run_id=str(row["selection_run_id"]),
            package_id=str(row["package_id"]),
            manifest_sha256=str(row["manifest_sha256"]),
            decision_as_of_trade_date=row["decision_as_of_trade_date"],
            capture_policy_hash=str(row["capture_policy_hash"]),
        ),
        reason_code=str(row["reason_code"]),
        gap_content_hash=str(row["gap_content_hash"]),
    )


def _require_historical_trace_identity(envelope: StageTraceEnvelope) -> None:
    identity = envelope.trace_content.get("selection_identity")
    if not isinstance(identity, Mapping):
        raise SourceLedgerError(REASON_TRACE_ADMISSION_BATCH_INVALID, "trace selection identity is missing")
    if (
        identity.get("data_source") != "DB_HISTORICAL"
        or identity.get("execution_origin") != "ADVISORY_RUN"
        or identity.get("research_scope") != "HISTORICAL_RESEARCH_ONLY"
        or identity.get("execution_prohibited") is not True
    ):
        raise SourceLedgerError(REASON_TRACE_ADMISSION_BATCH_INVALID, "trace is outside the historical research boundary")
