"""Batch B pure label revision, selection, and frozen-input orchestration.

No object in this module queries mutable current state.  Callers must supply
complete historical source slices as ``OutcomeCalculationRequest`` instances.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
from enum import Enum
import logging
from math import isfinite
from threading import RLock
from typing import Any, Callable, Iterable, Mapping, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.calculation_evidence import LocalCalculationEvidenceStore, StoredCalculationEvidence
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatch,
    CaptureBatchStatus,
    CaptureMembership,
    InMemoryCaptureBatchRepository,
)
from backend.services.advisory_phase1.label_capture import (
    CandidateCoverageSummary,
    LabelCaptureAdmissionContext,
    LabelCaptureBatchRequestV2,
    LabelCaptureGap,
    PlannedLabelDescriptor,
    UniverseCoverageSummary,
)
from backend.services.advisory_phase1.label_policy import LabelPolicyBundle, Projection
from backend.services.advisory_phase1.observation_selector import FixtureObservationVersion
from backend.services.advisory_phase1.outcome_engine import (
    MaturityStatus,
    OutcomeCalculationRequest,
    OutcomeCalculationResult,
    OutcomeEngine,
    OutcomeEventStatus,
    OutcomeOwner,
    OwnerType,
    SourceMemberBinding,
    OUTCOME_CALCULATION_SCHEMA_VERSION,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.stage_trace import MULTI_ALPHA_COMPONENT_EVIDENCE_SCHEMA_VERSION


LABEL_APPEND_REQUEST_SCHEMA_VERSION = "advisory_phase1_label_append_request_v1"
LABEL_SELECTION_SCHEMA_VERSION = "advisory_phase1_label_selection_v1"
LABEL_MAPPING_SCHEMA_VERSION = "advisory_phase1_selected_label_mapping_v1"
UNIVERSE_OUTCOME_PLAN_SCHEMA_VERSION = "advisory_phase1_universe_outcome_plan_v1"
UNIVERSE_RAW_OUTCOME_SCHEMA_VERSION = "advisory_phase1_universe_raw_outcome_v1"

REASON_LABEL_APPEND_REQUEST_CONFLICT = "ADVISORY_PHASE1C3_LABEL_APPEND_REQUEST_CONFLICT"
REASON_LABEL_PREDECESSOR_INVALID = "ADVISORY_PHASE1C3_LABEL_PREDECESSOR_INVALID"
REASON_LABEL_REVISION_CHAIN_INVALID = "ADVISORY_PHASE1C3_LABEL_REVISION_CHAIN_INVALID"
REASON_LABEL_STATE_TRANSITION_INVALID = "ADVISORY_PHASE1C3_LABEL_STATE_TRANSITION_INVALID"
REASON_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID = "ADVISORY_PHASE1C3_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID"
REASON_LABEL_SELECTOR_TERMINAL_CONFLICT = "ADVISORY_PHASE1C3_LABEL_SELECTOR_TERMINAL_CONFLICT"
REASON_LABEL_SELECTOR_CAPABILITY_UNAVAILABLE = "ADVISORY_PHASE1C3_LABEL_SELECTOR_CAPABILITY_UNAVAILABLE"
REASON_LABEL_MAPPING_CONFLICT = "ADVISORY_PHASE1C3_LABEL_MAPPING_CONFLICT"
REASON_LABEL_EVIDENCE_IDENTITY_INVALID = "ADVISORY_PHASE1C3_LABEL_EVIDENCE_IDENTITY_INVALID"
REASON_LABEL_ALPHA_RAW_STAGE_INVALID = "ADVISORY_PHASE1C3_LABEL_ALPHA_RAW_STAGE_INVALID"
REASON_LABEL_CANDIDATE_SET_INVALID = "ADVISORY_PHASE1C3_LABEL_CANDIDATE_SET_INVALID"
REASON_LABEL_UNIVERSE_SET_INVALID = "ADVISORY_PHASE1C3_LABEL_UNIVERSE_SET_INVALID"

logger = logging.getLogger(__name__)


class LabelBuilderError(SourceLedgerError):
    """Stable Batch B failure with an explicit reason code."""


def _require_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return value.astimezone(timezone.utc)


def _normalized_reasons(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(str(value) for value in values if str(value))))


def _canonical_revalidate(model: BaseModel, *, reason_code: str, label: str) -> BaseModel:
    """Reject Pydantic instances whose stored identity no longer matches their fields."""

    try:
        rebuilt = type(model).model_validate(model.model_dump(mode="python"))
    except (TypeError, ValueError) as error:
        raise LabelBuilderError(reason_code, f"{label} failed canonical revalidation") from error
    if rebuilt != model:
        raise LabelBuilderError(reason_code, f"{label} differs from canonical content")
    return rebuilt


def label_key_hash(
    *,
    canonical_signal_id: str,
    symbol: str,
    label_policy_hash: str,
    horizon_trading_days: int,
    projection: Projection | str,
) -> str:
    """The one candidate key formula frozen by the parent design."""

    if horizon_trading_days < 0:
        raise ValueError("label horizon cannot be negative")
    projection_value = projection.value if isinstance(projection, Projection) else str(projection)
    return canonical_json_sha256(
        {
            "canonical_signal_id": canonical_signal_id,
            "symbol": symbol,
            "label_policy_hash": _require_sha256(label_policy_hash, field_name="label_policy_hash"),
            "horizon_trading_days": horizon_trading_days,
            "projection": projection_value,
        }
    )


class LabelAppendRequest(BaseModel):
    """Immutable semantic request to append one label revision."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = LABEL_APPEND_REQUEST_SCHEMA_VERSION
    label_key_hash: str = Field(min_length=64, max_length=64)
    expected_predecessor_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    expected_predecessor_version_hash: str | None = Field(default=None, min_length=64, max_length=64)
    expected_predecessor_revision_no: int | None = Field(default=None, ge=1)
    label_policy_bundle_id: str = Field(min_length=1, max_length=160)
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    label_source_revision_set_id: str = Field(min_length=1, max_length=160)
    label_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    owner: OutcomeOwner
    horizon_trading_days: int = Field(ge=0)
    projection: Projection
    projection_schema_version: str = OUTCOME_CALCULATION_SCHEMA_VERSION
    outcome_result: OutcomeCalculationResult
    projection_payload_hash: str = Field(min_length=64, max_length=64)
    calculation_evidence_sha256: str = Field(min_length=64, max_length=64)
    calculation_evidence_size_bytes: int = Field(ge=1)
    calculation_evidence_store_backend_hash: str = Field(min_length=64, max_length=64)
    calculation_evidence_uri: str = Field(min_length=1, max_length=4096)
    label_append_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "label_key_hash",
        "expected_predecessor_version_hash",
        "label_policy_bundle_hash",
        "label_policy_hash",
        "label_source_revision_set_hash",
        "projection_payload_hash",
        "calculation_evidence_sha256",
        "calculation_evidence_store_backend_hash",
        "label_append_request_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return canonicalize(
            self.model_dump(
                mode="python",
                exclude={
                    "label_append_request_hash",
                    "calculation_evidence_uri",
                },
            )
        )

    @model_validator(mode="after")
    def _frozen_semantics(self) -> "LabelAppendRequest":
        if self.schema_version != LABEL_APPEND_REQUEST_SCHEMA_VERSION:
            raise ValueError("unsupported label append request schema version")
        predecessor = (
            self.expected_predecessor_version_id,
            self.expected_predecessor_version_hash,
            self.expected_predecessor_revision_no,
        )
        if any(value is None for value in predecessor) and any(value is not None for value in predecessor):
            raise ValueError("expected predecessor values must be nullable together")
        if self.projection_schema_version != OUTCOME_CALCULATION_SCHEMA_VERSION:
            raise ValueError("unsupported projection schema version")
        if self.owner != self.outcome_result.owner:
            raise ValueError("label append owner does not match outcome result")
        if (
            self.horizon_trading_days != self.outcome_result.horizon_trading_days
            or self.projection is not self.outcome_result.projection
            or self.projection_payload_hash != self.outcome_result.projection_payload_hash
        ):
            raise ValueError("label append projection identity does not match outcome result")
        expected_key = label_key_hash(
            canonical_signal_id=self.owner.canonical_signal_id,
            symbol=self.owner.symbol,
            label_policy_hash=self.label_policy_hash,
            horizon_trading_days=self.horizon_trading_days,
            projection=self.projection,
        )
        if self.owner.owner_type is OwnerType.CANDIDATE and self.label_key_hash != expected_key:
            raise ValueError("candidate label key does not match policy identity")
        if self.calculation_evidence_sha256 != self.outcome_result.calculation_evidence.evidence_hash:
            raise ValueError("calculation evidence sha does not match outcome result evidence")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.label_append_request_hash is not None and self.label_append_request_hash != digest:
            raise ValueError("label append request hash does not match canonical payload")
        object.__setattr__(self, "label_append_request_hash", digest)
        return self


class OutcomeLabelVersion(BaseModel):
    """One append-only label revision and its first authoritative locator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    label_key_hash: str = Field(min_length=64, max_length=64)
    label_revision_no: int = Field(ge=1)
    supersedes_label_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    supersedes_label_version_hash: str | None = Field(default=None, min_length=64, max_length=64)
    label_append_request_hash: str = Field(min_length=64, max_length=64)
    label_policy_bundle_id: str = Field(min_length=1, max_length=160)
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    label_source_revision_set_id: str = Field(min_length=1, max_length=160)
    label_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    owner: OutcomeOwner
    horizon_trading_days: int = Field(ge=0)
    projection: Projection
    projection_schema_version: str = OUTCOME_CALCULATION_SCHEMA_VERSION
    outcome_result: OutcomeCalculationResult
    calculation_evidence_sha256: str = Field(min_length=64, max_length=64)
    calculation_evidence_size_bytes: int = Field(ge=1)
    calculation_evidence_store_backend_hash: str = Field(min_length=64, max_length=64)
    calculation_evidence_uri: str = Field(min_length=1, max_length=4096)
    created_by_capture_batch_id: str = Field(min_length=1, max_length=160)
    computed_at: datetime
    label_content_hash: str | None = Field(default=None, min_length=64, max_length=64)
    label_version_id: str | None = Field(default=None, min_length=1, max_length=160)

    @field_validator(
        "label_key_hash",
        "supersedes_label_version_hash",
        "label_append_request_hash",
        "label_policy_bundle_hash",
        "label_policy_hash",
        "label_source_revision_set_hash",
        "calculation_evidence_sha256",
        "calculation_evidence_store_backend_hash",
        "label_content_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("computed_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="computed_at")

    def canonical_payload(self) -> dict[str, Any]:
        return canonicalize(self.model_dump(mode="python", exclude={"label_content_hash", "label_version_id"}))

    @model_validator(mode="after")
    def _derive_identity(self) -> "OutcomeLabelVersion":
        predecessor = (self.supersedes_label_version_id, self.supersedes_label_version_hash)
        if any(value is None for value in predecessor) and any(value is not None for value in predecessor):
            raise ValueError("label version predecessor id/hash must be nullable together")
        if self.label_revision_no == 1 and any(value is not None for value in predecessor):
            raise ValueError("first label revision cannot have a predecessor")
        if self.label_revision_no > 1 and any(value is None for value in predecessor):
            raise ValueError("non-first label revision requires predecessor")
        if self.owner != self.outcome_result.owner:
            raise ValueError("label version owner does not match outcome result")
        if (
            self.horizon_trading_days != self.outcome_result.horizon_trading_days
            or self.projection is not self.outcome_result.projection
            or self.calculation_evidence_sha256 != self.outcome_result.calculation_evidence.evidence_hash
        ):
            raise ValueError("label version does not match outcome result identity")
        digest = canonical_json_sha256(self.canonical_payload())
        expected_id = f"advlabel_{digest[:24]}"
        if self.label_content_hash is not None and self.label_content_hash != digest:
            raise ValueError("label_content_hash does not match label version")
        if self.label_version_id is not None and self.label_version_id != expected_id:
            raise ValueError("label_version_id does not match label version")
        object.__setattr__(self, "label_content_hash", digest)
        object.__setattr__(self, "label_version_id", expected_id)
        return self

    @classmethod
    def from_append(
        cls,
        request: LabelAppendRequest,
        *,
        label_revision_no: int,
        predecessor: "OutcomeLabelVersion | None",
        created_by_capture_batch_id: str,
        computed_at: datetime,
    ) -> "OutcomeLabelVersion":
        return cls(
            label_key_hash=request.label_key_hash,
            label_revision_no=label_revision_no,
            supersedes_label_version_id=predecessor.label_version_id if predecessor else None,
            supersedes_label_version_hash=predecessor.label_content_hash if predecessor else None,
            label_append_request_hash=str(request.label_append_request_hash),
            label_policy_bundle_id=request.label_policy_bundle_id,
            label_policy_bundle_hash=request.label_policy_bundle_hash,
            label_policy_hash=request.label_policy_hash,
            label_source_revision_set_id=request.label_source_revision_set_id,
            label_source_revision_set_hash=request.label_source_revision_set_hash,
            owner=request.owner,
            horizon_trading_days=request.horizon_trading_days,
            projection=request.projection,
            projection_schema_version=request.projection_schema_version,
            outcome_result=request.outcome_result,
            calculation_evidence_sha256=request.calculation_evidence_sha256,
            calculation_evidence_size_bytes=request.calculation_evidence_size_bytes,
            calculation_evidence_store_backend_hash=request.calculation_evidence_store_backend_hash,
            calculation_evidence_uri=request.calculation_evidence_uri,
            created_by_capture_batch_id=created_by_capture_batch_id,
            computed_at=computed_at,
        )


class OutcomeLabelAuthorityHeader(BaseModel):
    """Logical authority header; Batch C owns its physical table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    label_version_id: str = Field(min_length=1, max_length=160)
    label_content_hash: str = Field(min_length=64, max_length=64)
    label_key_hash: str = Field(min_length=64, max_length=64)
    label_revision_no: int = Field(ge=1)
    supersedes_label_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    supersedes_label_version_hash: str | None = Field(default=None, min_length=64, max_length=64)
    label_append_request_hash: str = Field(min_length=64, max_length=64)
    label_policy_bundle_id: str = Field(min_length=1, max_length=160)
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    label_source_revision_set_id: str = Field(min_length=1, max_length=160)
    label_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    owner: OutcomeOwner
    horizon_trading_days: int = Field(ge=0)
    projection: Projection
    projection_schema_version: str = OUTCOME_CALCULATION_SCHEMA_VERSION
    maturity_status: MaturityStatus
    outcome_event_status: OutcomeEventStatus
    calculation_evidence_sha256: str = Field(min_length=64, max_length=64)
    calculation_evidence_size_bytes: int = Field(ge=1)
    calculation_evidence_store_backend_hash: str = Field(min_length=64, max_length=64)
    created_by_capture_batch_id: str = Field(min_length=1, max_length=160)
    computed_at: datetime

    @field_validator(
        "label_content_hash",
        "label_key_hash",
        "supersedes_label_version_hash",
        "label_append_request_hash",
        "label_policy_bundle_hash",
        "label_policy_hash",
        "label_source_revision_set_hash",
        "calculation_evidence_sha256",
        "calculation_evidence_store_backend_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("computed_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="computed_at")

    @classmethod
    def from_version(cls, version: OutcomeLabelVersion) -> "OutcomeLabelAuthorityHeader":
        return cls(
            label_version_id=str(version.label_version_id),
            label_content_hash=str(version.label_content_hash),
            label_key_hash=version.label_key_hash,
            label_revision_no=version.label_revision_no,
            supersedes_label_version_id=version.supersedes_label_version_id,
            supersedes_label_version_hash=version.supersedes_label_version_hash,
            label_append_request_hash=version.label_append_request_hash,
            label_policy_bundle_id=version.label_policy_bundle_id,
            label_policy_bundle_hash=version.label_policy_bundle_hash,
            label_policy_hash=version.label_policy_hash,
            label_source_revision_set_id=version.label_source_revision_set_id,
            label_source_revision_set_hash=version.label_source_revision_set_hash,
            owner=version.owner,
            horizon_trading_days=version.horizon_trading_days,
            projection=version.projection,
            projection_schema_version=version.projection_schema_version,
            maturity_status=version.outcome_result.maturity_status,
            outcome_event_status=version.outcome_result.outcome_event_status,
            calculation_evidence_sha256=version.calculation_evidence_sha256,
            calculation_evidence_size_bytes=version.calculation_evidence_size_bytes,
            calculation_evidence_store_backend_hash=version.calculation_evidence_store_backend_hash,
            created_by_capture_batch_id=version.created_by_capture_batch_id,
            computed_at=version.computed_at,
        )


class OutcomeLabelPayload(BaseModel):
    """Logical payload paired exactly once with an authority header."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    label_version_id: str = Field(min_length=1, max_length=160)
    label_content_hash: str = Field(min_length=64, max_length=64)
    outcome_result: OutcomeCalculationResult
    calculation_evidence_uri: str = Field(min_length=1, max_length=4096)

    @field_validator("label_content_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="label_content_hash")

    @classmethod
    def from_version(cls, version: OutcomeLabelVersion) -> "OutcomeLabelPayload":
        return cls(
            label_version_id=str(version.label_version_id),
            label_content_hash=str(version.label_content_hash),
            outcome_result=version.outcome_result,
            calculation_evidence_uri=version.calculation_evidence_uri,
        )


def _validate_header_payload(
    *,
    version: OutcomeLabelVersion,
    header: OutcomeLabelAuthorityHeader,
    payload: OutcomeLabelPayload,
) -> None:
    if (
        header.label_version_id != version.label_version_id
        or payload.label_version_id != version.label_version_id
        or header.label_content_hash != version.label_content_hash
        or payload.label_content_hash != version.label_content_hash
        or header.owner != version.owner
        or header.label_policy_hash != version.label_policy_hash
        or payload.outcome_result != version.outcome_result
        or header.maturity_status is not version.outcome_result.maturity_status
        or header.outcome_event_status is not version.outcome_result.outcome_event_status
        or header.calculation_evidence_sha256 != version.calculation_evidence_sha256
        or payload.calculation_evidence_uri != version.calculation_evidence_uri
    ):
        raise LabelBuilderError(
            REASON_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID,
            "label authority header and payload do not close over one immutable version",
        )


class OutcomeLabelRepository(Protocol):
    """One logical authority contract shared by in-memory and PostgreSQL repositories."""

    def append(self, *, request: LabelAppendRequest, created_by_capture_batch_id: str) -> OutcomeLabelVersion: ...
    def get(self, label_version_id: str) -> OutcomeLabelVersion | None: ...
    def chain_for(self, label_key: str) -> tuple[OutcomeLabelVersion, ...]: ...
    def header_for(self, label_version_id: str) -> OutcomeLabelAuthorityHeader | None: ...
    def payload_for(self, label_version_id: str) -> OutcomeLabelPayload | None: ...


_ALLOWED_TRANSITIONS: dict[MaturityStatus, frozenset[MaturityStatus]] = {
    MaturityStatus.PENDING: frozenset(
        {MaturityStatus.PENDING, MaturityStatus.MATURED, MaturityStatus.RIGHT_CENSORED, MaturityStatus.UNAVAILABLE}
    ),
    MaturityStatus.MATURED: frozenset({MaturityStatus.MATURED, MaturityStatus.UNAVAILABLE}),
    MaturityStatus.RIGHT_CENSORED: frozenset(
        {MaturityStatus.RIGHT_CENSORED, MaturityStatus.MATURED, MaturityStatus.UNAVAILABLE}
    ),
    MaturityStatus.UNAVAILABLE: frozenset(
        {MaturityStatus.UNAVAILABLE, MaturityStatus.MATURED, MaturityStatus.RIGHT_CENSORED}
    ),
}


class InMemoryOutcomeLabelRepository:
    """Thread-safe append-only oracle for logical label header/payload authority."""

    def __init__(self, *, now_provider: Callable[[], datetime] | None = None) -> None:
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self._lock = RLock()
        self._by_request_hash: dict[str, tuple[LabelAppendRequest, OutcomeLabelVersion]] = {}
        self._by_label_key: dict[str, list[OutcomeLabelVersion]] = {}
        self._by_version_id: dict[str, OutcomeLabelVersion] = {}
        self._by_content_hash: dict[str, OutcomeLabelVersion] = {}
        self._headers: dict[str, OutcomeLabelAuthorityHeader] = {}
        self._payloads: dict[str, OutcomeLabelPayload] = {}

    def append(self, *, request: LabelAppendRequest, created_by_capture_batch_id: str) -> OutcomeLabelVersion:
        _canonical_revalidate(
            request,
            reason_code=REASON_LABEL_APPEND_REQUEST_CONFLICT,
            label="label append request",
        )
        request_hash = str(request.label_append_request_hash)
        with self._lock:
            previous_by_request = self._by_request_hash.get(request_hash)
            if previous_by_request is not None:
                previous_request, previous_version = previous_by_request
                if previous_request.canonical_payload() != request.canonical_payload() or previous_request.label_key_hash != request.label_key_hash:
                    raise LabelBuilderError(
                        REASON_LABEL_APPEND_REQUEST_CONFLICT,
                        "same label append request hash has different semantic payload",
                    )
                return previous_version
            chain = self._by_label_key.setdefault(request.label_key_hash, [])
            predecessor = self._terminal(chain)
            self._validate_predecessor(request=request, predecessor=predecessor)
            revision = 1 if predecessor is None else predecessor.label_revision_no + 1
            if predecessor is not None:
                allowed = _ALLOWED_TRANSITIONS[predecessor.outcome_result.maturity_status]
                if request.outcome_result.maturity_status not in allowed:
                    raise LabelBuilderError(
                        REASON_LABEL_STATE_TRANSITION_INVALID,
                        "label maturity transition is not allowed by the frozen matrix",
                    )
                if (
                    request.label_source_revision_set_hash == predecessor.label_source_revision_set_hash
                    and request.calculation_evidence_sha256 == predecessor.calculation_evidence_sha256
                ):
                    raise LabelBuilderError(
                        REASON_LABEL_APPEND_REQUEST_CONFLICT,
                        "a non-idempotent label revision requires new source or calculation evidence",
                    )
            computed_at = _require_aware(self._now_provider(), field_name="now_provider")
            version = OutcomeLabelVersion.from_append(
                request,
                label_revision_no=revision,
                predecessor=predecessor,
                created_by_capture_batch_id=created_by_capture_batch_id,
                computed_at=computed_at,
            )
            version_id = str(version.label_version_id)
            content_hash = str(version.label_content_hash)
            existing_id = self._by_version_id.get(version_id)
            existing_hash = self._by_content_hash.get(content_hash)
            if existing_id is not None and existing_id.label_content_hash != content_hash:
                raise LabelBuilderError(REASON_LABEL_APPEND_REQUEST_CONFLICT, "label version id collides with different content")
            if existing_hash is not None and existing_hash.label_version_id != version_id:
                raise LabelBuilderError(REASON_LABEL_APPEND_REQUEST_CONFLICT, "label content hash collides with different id")
            if existing_id is not None or existing_hash is not None:
                raise LabelBuilderError(REASON_LABEL_APPEND_REQUEST_CONFLICT, "new label append collided with an existing version")
            header = OutcomeLabelAuthorityHeader.from_version(version)
            payload = OutcomeLabelPayload.from_version(version)
            _validate_header_payload(version=version, header=header, payload=payload)
            chain.append(version)
            self._by_request_hash[request_hash] = (request, version)
            self._by_version_id[version_id] = version
            self._by_content_hash[content_hash] = version
            self._headers[version_id] = header
            self._payloads[version_id] = payload
            return self._readback(version_id)

    def chain_for(self, label_key: str) -> tuple[OutcomeLabelVersion, ...]:
        with self._lock:
            return tuple(self._by_label_key.get(label_key, ()))

    def get(self, label_version_id: str) -> OutcomeLabelVersion | None:
        with self._lock:
            return self._by_version_id.get(label_version_id)

    def header_for(self, label_version_id: str) -> OutcomeLabelAuthorityHeader | None:
        with self._lock:
            return self._headers.get(label_version_id)

    def payload_for(self, label_version_id: str) -> OutcomeLabelPayload | None:
        with self._lock:
            return self._payloads.get(label_version_id)

    @staticmethod
    def _terminal(chain: list[OutcomeLabelVersion]) -> OutcomeLabelVersion | None:
        if not chain:
            return None
        InMemoryOutcomeLabelRepository._validate_chain(tuple(chain))
        return chain[-1]

    @staticmethod
    def _validate_predecessor(*, request: LabelAppendRequest, predecessor: OutcomeLabelVersion | None) -> None:
        expected = (
            request.expected_predecessor_version_id,
            request.expected_predecessor_version_hash,
            request.expected_predecessor_revision_no,
        )
        if predecessor is None:
            if any(value is not None for value in expected):
                raise LabelBuilderError(REASON_LABEL_PREDECESSOR_INVALID, "first label revision cannot name a predecessor")
            return
        if any(value is None for value in expected):
            raise LabelBuilderError(REASON_LABEL_PREDECESSOR_INVALID, "next label revision requires the terminal predecessor")
        if (
            request.expected_predecessor_version_id != predecessor.label_version_id
            or request.expected_predecessor_version_hash != predecessor.label_content_hash
            or request.expected_predecessor_revision_no != predecessor.label_revision_no
        ):
            raise LabelBuilderError(REASON_LABEL_PREDECESSOR_INVALID, "label append predecessor is stale or not terminal")

    @staticmethod
    def _validate_chain(versions: tuple[OutcomeLabelVersion, ...]) -> None:
        if not versions:
            return
        for version in versions:
            _canonical_revalidate(
                version,
                reason_code=REASON_LABEL_REVISION_CHAIN_INVALID,
                label="outcome label version",
            )
        ordered = tuple(sorted(versions, key=lambda item: item.label_revision_no))
        if ordered != versions or len({item.label_revision_no for item in ordered}) != len(ordered):
            raise LabelBuilderError(REASON_LABEL_REVISION_CHAIN_INVALID, "label revision order is not continuous")
        by_id = {str(item.label_version_id): item for item in ordered}
        if len(by_id) != len(ordered) or len({str(item.label_content_hash) for item in ordered}) != len(ordered):
            raise LabelBuilderError(REASON_LABEL_REVISION_CHAIN_INVALID, "label chain has duplicate version identity")
        key = ordered[0].label_key_hash
        for expected_revision, version in enumerate(ordered, start=1):
            if version.label_key_hash != key or version.label_revision_no != expected_revision:
                raise LabelBuilderError(REASON_LABEL_REVISION_CHAIN_INVALID, "label revisions are not a single continuous key chain")
            if expected_revision == 1:
                if version.supersedes_label_version_id is not None:
                    raise LabelBuilderError(REASON_LABEL_REVISION_CHAIN_INVALID, "first label revision has a predecessor")
                continue
            predecessor = ordered[expected_revision - 2]
            if (
                version.supersedes_label_version_id != predecessor.label_version_id
                or version.supersedes_label_version_hash != predecessor.label_content_hash
                or version.supersedes_label_version_id not in by_id
            ):
                raise LabelBuilderError(REASON_LABEL_REVISION_CHAIN_INVALID, "label chain predecessor does not match prior terminal")

    def _readback(self, label_version_id: str) -> OutcomeLabelVersion:
        version = self._by_version_id.get(label_version_id)
        header = self._headers.get(label_version_id)
        payload = self._payloads.get(label_version_id)
        if version is None or header is None or payload is None:
            raise LabelBuilderError(REASON_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID, "label append did not persist complete logical authority")
        _validate_header_payload(version=version, header=header, payload=payload)
        return version


class LabelSelectionPolicy(str, Enum):
    EXACT_REVISION_V1 = "EXACT_REVISION_V1"
    LATEST_ELIGIBLE_REVISION_V1 = "LATEST_ELIGIBLE_REVISION_V1"


class LabelSelectionStatus(str, Enum):
    SELECTED = "SELECTED"
    UNAVAILABLE = "UNAVAILABLE"
    CONFLICT = "CONFLICT"


class LabelSelectionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    selection_policy: LabelSelectionPolicy
    selection_policy_hash: str | None = Field(default=None, min_length=64, max_length=64)
    label_key_hash: str = Field(min_length=64, max_length=64)
    requested_label_as_of_ts: datetime
    required_maturity_statuses: tuple[MaturityStatus, ...] = Field(min_length=1)
    required_outcome_event_statuses: tuple[OutcomeEventStatus, ...] = Field(min_length=1)
    required_projection_schema_version: str = OUTCOME_CALCULATION_SCHEMA_VERSION
    expected_observation_version_id: str = Field(min_length=1, max_length=160)
    expected_candidate_stage_evidence_id: str = Field(min_length=1, max_length=160)
    expected_label_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    explicit_label_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    selector_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("label_key_hash", "selection_policy_hash", "expected_label_source_revision_set_hash", "selector_request_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("requested_label_as_of_ts")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="requested_label_as_of_ts")

    def canonical_payload(self) -> dict[str, Any]:
        return canonicalize(
            self.model_dump(
                mode="python",
                exclude={"selection_policy_hash", "selector_request_hash"},
            )
        )

    @model_validator(mode="after")
    def _derive_identity(self) -> "LabelSelectionRequest":
        if self.selection_policy is LabelSelectionPolicy.EXACT_REVISION_V1 and self.explicit_label_version_id is None:
            raise ValueError("exact label selection requires an explicit label version")
        if self.selection_policy is LabelSelectionPolicy.LATEST_ELIGIBLE_REVISION_V1 and self.explicit_label_version_id is not None:
            raise ValueError("latest label selection cannot name an explicit label version")
        if len(set(self.required_maturity_statuses)) != len(self.required_maturity_statuses):
            raise ValueError("required maturity statuses must be unique")
        if len(set(self.required_outcome_event_statuses)) != len(self.required_outcome_event_statuses):
            raise ValueError("required outcome event statuses must be unique")
        policy_hash = canonical_json_sha256({"schema_version": LABEL_SELECTION_SCHEMA_VERSION, "selection_policy": self.selection_policy.value})
        if self.selection_policy_hash is not None and self.selection_policy_hash != policy_hash:
            raise ValueError("label selection policy hash does not match policy")
        request_hash = canonical_json_sha256(self.canonical_payload())
        if self.selector_request_hash is not None and self.selector_request_hash != request_hash:
            raise ValueError("selector request hash does not match canonical request")
        object.__setattr__(self, "selection_policy_hash", policy_hash)
        object.__setattr__(self, "selector_request_hash", request_hash)
        return self


class SelectedLabelMapping(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    selector_request_hash: str = Field(min_length=64, max_length=64)
    selection_policy: LabelSelectionPolicy
    selection_policy_hash: str = Field(min_length=64, max_length=64)
    label_key_hash: str = Field(min_length=64, max_length=64)
    requested_label_as_of_ts: datetime
    terminal_label_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    terminal_label_content_hash: str | None = Field(default=None, min_length=64, max_length=64)
    terminal_label_revision_no: int | None = Field(default=None, ge=1)
    terminal_maturity_status: MaturityStatus | None = None
    terminal_outcome_event_status: OutcomeEventStatus | None = None
    terminal_reason_codes: tuple[str, ...] = ()
    selection_status: LabelSelectionStatus
    reason_codes: tuple[str, ...] = ()
    selected_label_mapping_id: str | None = Field(default=None, min_length=1, max_length=160)
    selected_label_mapping_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "selector_request_hash",
        "selection_policy_hash",
        "label_key_hash",
        "terminal_label_content_hash",
        "selected_label_mapping_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("requested_label_as_of_ts")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="requested_label_as_of_ts")

    def canonical_payload(self) -> dict[str, Any]:
        return canonicalize(
            self.model_dump(
                mode="python",
                exclude={"selected_label_mapping_id", "selected_label_mapping_hash"},
            )
        )

    @model_validator(mode="after")
    def _derive_identity(self) -> "SelectedLabelMapping":
        normalized_reasons = _normalized_reasons(self.reason_codes)
        normalized_terminal_reasons = _normalized_reasons(self.terminal_reason_codes)
        object.__setattr__(self, "reason_codes", normalized_reasons)
        object.__setattr__(self, "terminal_reason_codes", normalized_terminal_reasons)
        terminal = (
            self.terminal_label_version_id,
            self.terminal_label_content_hash,
            self.terminal_label_revision_no,
            self.terminal_maturity_status,
            self.terminal_outcome_event_status,
        )
        if any(value is None for value in terminal) and any(value is not None for value in terminal):
            raise ValueError("terminal label values must be nullable together")
        if all(value is None for value in terminal) and self.terminal_reason_codes:
            raise ValueError("unresolved label mapping cannot carry terminal reason codes")
        if self.selection_status is LabelSelectionStatus.SELECTED:
            if any(value is None for value in terminal) or self.reason_codes:
                raise ValueError("selected label mapping requires one terminal and no reason")
        elif not self.reason_codes:
            raise ValueError("unavailable/conflict label mapping requires a stable reason")
        digest = canonical_json_sha256(self.canonical_payload())
        expected_id = f"slm_{digest[:20]}"
        if self.selected_label_mapping_hash is not None and self.selected_label_mapping_hash != digest:
            raise ValueError("selected label mapping hash does not match content")
        if self.selected_label_mapping_id is not None and self.selected_label_mapping_id != expected_id:
            raise ValueError("selected label mapping id does not match content")
        object.__setattr__(self, "selected_label_mapping_hash", digest)
        object.__setattr__(self, "selected_label_mapping_id", expected_id)
        return self


class TerminalFirstLabelSelector:
    """Resolve the as-of terminal first; never fall back to an older MATURED revision."""

    def select(
        self,
        *,
        request: LabelSelectionRequest,
        label_versions: Iterable[OutcomeLabelVersion],
    ) -> SelectedLabelMapping:
        _canonical_revalidate(
            request,
            reason_code=REASON_LABEL_SELECTOR_TERMINAL_CONFLICT,
            label="label selection request",
        )
        versions = tuple(label_versions)
        if not versions:
            return self._mapping(request=request, terminal=None, status=LabelSelectionStatus.UNAVAILABLE, reasons=(REASON_LABEL_SELECTOR_CAPABILITY_UNAVAILABLE,))
        try:
            InMemoryOutcomeLabelRepository._validate_chain(versions)
        except LabelBuilderError:
            return self._mapping(request=request, terminal=None, status=LabelSelectionStatus.CONFLICT, reasons=(REASON_LABEL_SELECTOR_TERMINAL_CONFLICT,))
        as_of = tuple(version for version in versions if version.computed_at <= request.requested_label_as_of_ts)
        if not as_of:
            return self._mapping(request=request, terminal=None, status=LabelSelectionStatus.UNAVAILABLE, reasons=(REASON_LABEL_SELECTOR_CAPABILITY_UNAVAILABLE,))
        terminal = as_of[-1]
        if terminal.label_key_hash != request.label_key_hash:
            return self._mapping(request=request, terminal=terminal, status=LabelSelectionStatus.CONFLICT, reasons=(REASON_LABEL_SELECTOR_TERMINAL_CONFLICT,))
        if request.selection_policy is LabelSelectionPolicy.EXACT_REVISION_V1 and request.explicit_label_version_id != terminal.label_version_id:
            return self._mapping(request=request, terminal=terminal, status=LabelSelectionStatus.CONFLICT, reasons=(REASON_LABEL_SELECTOR_TERMINAL_CONFLICT,))
        if not self._matches_capability(request=request, version=terminal):
            return self._mapping(request=request, terminal=terminal, status=LabelSelectionStatus.UNAVAILABLE, reasons=(REASON_LABEL_SELECTOR_CAPABILITY_UNAVAILABLE,))
        return self._mapping(request=request, terminal=terminal, status=LabelSelectionStatus.SELECTED, reasons=())

    @staticmethod
    def _matches_capability(*, request: LabelSelectionRequest, version: OutcomeLabelVersion) -> bool:
        outcome = version.outcome_result
        return (
            version.projection_schema_version == request.required_projection_schema_version
            and version.label_source_revision_set_hash == request.expected_label_source_revision_set_hash
            and version.owner.owner_type is OwnerType.CANDIDATE
            and version.owner.observation_version_id == request.expected_observation_version_id
            and version.owner.candidate_stage_evidence_id == request.expected_candidate_stage_evidence_id
            and outcome.maturity_status in request.required_maturity_statuses
            and outcome.outcome_event_status in request.required_outcome_event_statuses
        )

    @staticmethod
    def _mapping(
        *,
        request: LabelSelectionRequest,
        terminal: OutcomeLabelVersion | None,
        status: LabelSelectionStatus,
        reasons: tuple[str, ...],
    ) -> SelectedLabelMapping:
        return SelectedLabelMapping(
            selector_request_hash=str(request.selector_request_hash),
            selection_policy=request.selection_policy,
            selection_policy_hash=str(request.selection_policy_hash),
            label_key_hash=request.label_key_hash,
            requested_label_as_of_ts=request.requested_label_as_of_ts,
            terminal_label_version_id=terminal.label_version_id if terminal else None,
            terminal_label_content_hash=terminal.label_content_hash if terminal else None,
            terminal_label_revision_no=terminal.label_revision_no if terminal else None,
            terminal_maturity_status=terminal.outcome_result.maturity_status if terminal else None,
            terminal_outcome_event_status=terminal.outcome_result.outcome_event_status if terminal else None,
            terminal_reason_codes=terminal.outcome_result.reason_codes if terminal else (),
            selection_status=status,
            reason_codes=reasons,
        )


class InMemorySelectedLabelMappingRepository:
    def __init__(self) -> None:
        self._by_id: dict[str, SelectedLabelMapping] = {}
        self._by_hash: dict[str, SelectedLabelMapping] = {}
        self._lock = RLock()

    def save(self, mapping: SelectedLabelMapping) -> SelectedLabelMapping:
        mapping_id = str(mapping.selected_label_mapping_id)
        mapping_hash = str(mapping.selected_label_mapping_hash)
        with self._lock:
            existing_by_id = self._by_id.get(mapping_id)
            existing_by_hash = self._by_hash.get(mapping_hash)
            if existing_by_id is not None and existing_by_id.selected_label_mapping_hash != mapping_hash:
                raise LabelBuilderError(REASON_LABEL_MAPPING_CONFLICT, "selected label mapping id already has different content")
            if existing_by_hash is not None and existing_by_hash.selected_label_mapping_id != mapping_id:
                raise LabelBuilderError(REASON_LABEL_MAPPING_CONFLICT, "selected label mapping hash already has different id")
            if existing_by_id is not None:
                return existing_by_id
            self._by_id[mapping_id] = mapping
            self._by_hash[mapping_hash] = mapping
            return mapping

    def get(self, mapping_id: str) -> SelectedLabelMapping | None:
        with self._lock:
            return self._by_id.get(mapping_id)


class StageEvidenceReference(BaseModel):
    """Stable logical stage identity until Batch C adds its physical authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    observation_version_id: str = Field(min_length=1, max_length=160)
    stage: str = Field(min_length=1, max_length=80)
    stage_content_hash: str = Field(min_length=64, max_length=64)
    stage_evidence_key_hash: str | None = Field(default=None, min_length=64, max_length=64)
    stage_evidence_id: str | None = Field(default=None, min_length=1, max_length=160)

    @field_validator("stage_content_hash", "stage_evidence_key_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_identity(self) -> "StageEvidenceReference":
        key_hash = canonical_json_sha256(
            {
                "observation_version_id": self.observation_version_id,
                "stage": self.stage,
                "stage_content_hash": self.stage_content_hash,
            }
        )
        expected_id = f"advstage_{key_hash[:24]}"
        if self.stage_evidence_key_hash is not None and self.stage_evidence_key_hash != key_hash:
            raise ValueError("stage evidence key hash does not match immutable stage")
        if self.stage_evidence_id is not None and self.stage_evidence_id != expected_id:
            raise ValueError("stage evidence id does not match immutable stage")
        object.__setattr__(self, "stage_evidence_key_hash", key_hash)
        object.__setattr__(self, "stage_evidence_id", expected_id)
        return self


class CandidateEnumerationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    descriptors: tuple[PlannedLabelDescriptor, ...]
    stage_references: tuple[StageEvidenceReference, ...]
    component_evidence_hashes: tuple[str, ...]
    coverage: CandidateCoverageSummary


def _alpha_raw_stage(observation: FixtureObservationVersion) -> Mapping[str, Any]:
    stages = observation.observation_payload.get("stages")
    if not isinstance(stages, list):
        raise LabelBuilderError(REASON_LABEL_ALPHA_RAW_STAGE_INVALID, "immutable observation has no stage list")
    matching = [item for item in stages if isinstance(item, Mapping) and item.get("stage") == "alpha_raw"]
    if len(matching) != 1:
        raise LabelBuilderError(REASON_LABEL_ALPHA_RAW_STAGE_INVALID, "immutable observation must contain exactly one alpha_raw stage")
    stage = matching[0]
    stage_hash = str(stage.get("content_hash") or "")
    if stage_hash not in observation.stage_content_hashes:
        raise LabelBuilderError(REASON_LABEL_ALPHA_RAW_STAGE_INVALID, "alpha_raw stage is absent from observation stage bundle")
    if str(stage.get("capability_status") or "") != "FULL":
        raise LabelBuilderError(REASON_LABEL_ALPHA_RAW_STAGE_INVALID, "alpha_raw stage is not complete for label enumeration")
    return stage


def enumerate_candidate_labels(
    *,
    context: LabelCaptureAdmissionContext,
    label_policy_bundle: LabelPolicyBundle,
) -> CandidateEnumerationResult:
    """Enumerate every alpha_raw INCLUDED candidate without downstream re-ranking."""

    context.validate()
    if (
        label_policy_bundle.label_policy_bundle_id != context.label_policy_bundle.label_policy_bundle_id
        or label_policy_bundle.label_policy_bundle_hash != context.label_policy_bundle.label_policy_bundle_hash
    ):
        raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "candidate policy bundle differs from admission context")
    mappings = context.mapping_refs()
    observations = {item.observation_version_id: item for item in context.selected_observations}
    descriptors: list[PlannedLabelDescriptor] = []
    references: list[StageEvidenceReference] = []
    component_evidence_hashes: set[str] = set()
    observation_count = 0
    included_count = 0
    excluded_count = 0
    empty_observation_count = 0
    for mapping in mappings:
        observation = observations.get(mapping.terminal_observation_version_id)
        if observation is None or observation.observation_content_hash != mapping.terminal_observation_content_hash:
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "mapping terminal observation is not present in immutable input")
        if observation.observation_revision_no != mapping.terminal_revision_no:
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "mapping terminal revision differs from immutable observation")
        stage = _alpha_raw_stage(observation)
        reference = StageEvidenceReference(
            observation_version_id=observation.observation_version_id,
            stage="alpha_raw",
            stage_content_hash=str(stage["content_hash"]),
        )
        references.append(reference)
        raw_candidates = stage.get("candidates")
        if not isinstance(raw_candidates, list):
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "alpha_raw candidates are not a list")
        stage_input = int(stage.get("input_count", -1))
        stage_output = int(stage.get("output_count", -1))
        stage_excluded = int(stage.get("excluded_count", -1))
        if min(stage_input, stage_output, stage_excluded) < 0 or stage_input != stage_output + stage_excluded:
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "alpha_raw stage counts do not close")
        plan_payload = observation.observation_payload.get("plan")
        if not isinstance(plan_payload, Mapping):
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "observation plan is not an object")
        alpha_mode = str(plan_payload.get("alpha_mode") or "")
        if alpha_mode not in {"single_alpha", "multi_alpha"}:
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "observation alpha mode is invalid")
        included: list[Mapping[str, Any]] = []
        excluded: list[Mapping[str, Any]] = []
        for candidate in raw_candidates:
            if not isinstance(candidate, Mapping):
                raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "alpha_raw candidate is not an object")
            membership_status = str(candidate.get("membership_status") or "")
            if membership_status == "INCLUDED":
                included.append(candidate)
            elif membership_status == "EXCLUDED":
                excluded.append(candidate)
            else:
                raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "alpha_raw candidate membership status is invalid")
            if alpha_mode == "multi_alpha":
                component_evidence = candidate.get("component_evidence")
                component_hash = candidate.get("component_evidence_hash")
                component_reasons = candidate.get("component_reason_codes") or []
                if (
                    candidate.get("component_capability") != "FULL"
                    or candidate.get("component_evidence_schema_version")
                    != MULTI_ALPHA_COMPONENT_EVIDENCE_SCHEMA_VERSION
                    or not isinstance(component_evidence, Mapping)
                    or not isinstance(component_hash, str)
                    or component_reasons
                    or canonical_json_sha256(canonicalize(component_evidence)) != component_hash
                ):
                    raise LabelBuilderError(
                        REASON_LABEL_CANDIDATE_SET_INVALID,
                        "multi-alpha candidate component provenance is incomplete",
                    )
                component_evidence_hashes.add(
                    _require_sha256(component_hash, field_name="component_evidence_hash")
                )
        if len(included) != stage_output or len(excluded) != stage_excluded:
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "alpha_raw candidate rows do not reconcile stage counts")
        symbols = [str(candidate.get("symbol") or "").strip() for candidate in raw_candidates]
        if not all(symbols) or len(set(symbols)) != len(symbols):
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "alpha_raw symbols must be non-empty and unique")
        ranks: list[int] = []
        for candidate in included:
            try:
                rank = int(candidate["rank"])
                score = float(candidate["score_decimal"])
            except (KeyError, TypeError, ValueError) as error:
                raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "included alpha_raw candidate rank or score is invalid") from error
            if rank < 1 or not isfinite(score):
                raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "included alpha_raw candidate rank or score is invalid")
            ranks.append(rank)
        if sorted(ranks) != list(range(1, len(included) + 1)):
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "alpha_raw included ranks are not continuous")
        observation_count += 1
        included_count += len(included)
        excluded_count += len(excluded)
        if not included:
            if not all(bool(getattr(plan, "valid_no_candidate", False)) for plan in context.source_plans):
                raise LabelBuilderError(
                    REASON_LABEL_CANDIDATE_SET_INVALID,
                    "empty alpha_raw output requires frozen valid_no_candidate provenance",
                )
            empty_observation_count += 1
        for candidate in sorted(included, key=lambda item: int(item["rank"])):
            symbol = str(candidate["symbol"])
            for horizon in label_policy_bundle.horizons:
                for projection in label_policy_bundle.projections_by_horizon[horizon]:
                    descriptors.append(
                        PlannedLabelDescriptor(
                            canonical_signal_id=observation.canonical_signal_id,
                            observation_version_id=observation.observation_version_id,
                            candidate_stage_evidence_id=str(reference.stage_evidence_id),
                            symbol=symbol,
                            decision_as_of_trade_date=date.fromisoformat(
                                str(observation.observation_payload["plan"]["decision_as_of_trade_date"])
                            ),
                            horizon_trading_days=horizon,
                            projection=projection.value,
                            label_key_hash=label_key_hash(
                                canonical_signal_id=observation.canonical_signal_id,
                                symbol=symbol,
                                label_policy_hash=label_policy_bundle.label_policy_hash,
                                horizon_trading_days=horizon,
                                projection=projection,
                            ),
                        )
                    )
    descriptor_keys = [(item.canonical_signal_id, item.symbol, item.horizon_trading_days, item.projection) for item in descriptors]
    if len(set(descriptor_keys)) != len(descriptor_keys):
        raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "candidate descriptors are duplicated")
    return CandidateEnumerationResult(
        descriptors=tuple(sorted(descriptors, key=lambda item: (item.canonical_signal_id, item.symbol, item.horizon_trading_days, item.projection))),
        stage_references=tuple(sorted(references, key=lambda item: item.observation_version_id)),
        component_evidence_hashes=tuple(sorted(component_evidence_hashes)),
        coverage=CandidateCoverageSummary(
            observation_count=observation_count,
            included_count=included_count,
            excluded_count=excluded_count,
            empty_observation_count=empty_observation_count,
            planned_label_count=len(descriptors),
        ),
    )


class UniverseConstituent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str = Field(min_length=1, max_length=32)
    universe_layer: str = Field(min_length=1, max_length=160)
    universe_policy_hash: str = Field(min_length=64, max_length=64)
    source_member_bindings: tuple[SourceMemberBinding, ...] = Field(min_length=1)
    constituent_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("universe_policy_hash", "constituent_content_hash")
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_identity(self) -> "UniverseConstituent":
        if len({item.source_member_key for item in self.source_member_bindings}) != len(self.source_member_bindings):
            raise ValueError("universe constituent source members are duplicated")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"constituent_content_hash"}))
        if self.constituent_content_hash is not None and self.constituent_content_hash != digest:
            raise ValueError("universe constituent content hash does not match content")
        object.__setattr__(self, "constituent_content_hash", digest)
        return self


class UniverseOutcomePlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = UNIVERSE_OUTCOME_PLAN_SCHEMA_VERSION
    owner: OutcomeOwner
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    decision_as_of_trade_date: date
    universe_layer: str = Field(min_length=1, max_length=160)
    horizon_trading_days: int = Field(ge=0)
    projection: Projection
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    constituent_content_hash: str = Field(min_length=64, max_length=64)
    plan_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "label_policy_bundle_hash",
        "label_source_revision_set_hash",
        "constituent_content_hash",
        "plan_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_hash(self) -> "UniverseOutcomePlan":
        if self.schema_version != UNIVERSE_OUTCOME_PLAN_SCHEMA_VERSION:
            raise ValueError("unsupported universe outcome plan schema")
        if (
            self.owner.owner_type is not OwnerType.UNIVERSE
            or self.owner.canonical_signal_id != self.canonical_signal_id
            or self.owner.decision_as_of_trade_date != self.decision_as_of_trade_date
            or self.owner.universe_layer != self.universe_layer
        ):
            raise ValueError("universe outcome plan owner identity is invalid")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"plan_hash"}))
        if self.plan_hash is not None and self.plan_hash != digest:
            raise ValueError("universe outcome plan hash does not match content")
        object.__setattr__(self, "plan_hash", digest)
        return self


class UniverseRawOutcomeRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = UNIVERSE_RAW_OUTCOME_SCHEMA_VERSION
    plan: UniverseOutcomePlan
    outcome_result: OutcomeCalculationResult
    calculation_evidence_sha256: str = Field(min_length=64, max_length=64)
    calculation_evidence_size_bytes: int = Field(ge=1)
    calculation_evidence_store_backend_hash: str = Field(min_length=64, max_length=64)
    calculation_evidence_uri: str = Field(min_length=1, max_length=4096)
    raw_outcome_id: str | None = Field(default=None, min_length=1, max_length=160)
    raw_outcome_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "calculation_evidence_sha256",
        "calculation_evidence_store_backend_hash",
        "raw_outcome_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _derive_identity(self) -> "UniverseRawOutcomeRow":
        if self.schema_version != UNIVERSE_RAW_OUTCOME_SCHEMA_VERSION:
            raise ValueError("unsupported universe raw outcome schema")
        if self.outcome_result.owner != self.plan.owner:
            raise ValueError("universe raw outcome owner differs from plan")
        if (
            self.outcome_result.horizon_trading_days != self.plan.horizon_trading_days
            or self.outcome_result.projection is not self.plan.projection
            or self.calculation_evidence_sha256 != self.outcome_result.calculation_evidence.evidence_hash
        ):
            raise ValueError("universe raw outcome does not match plan/result evidence")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"raw_outcome_id", "raw_outcome_hash"}))
        expected_id = f"uor_{digest[:20]}"
        if self.raw_outcome_hash is not None and self.raw_outcome_hash != digest:
            raise ValueError("universe raw outcome hash does not match content")
        if self.raw_outcome_id is not None and self.raw_outcome_id != expected_id:
            raise ValueError("universe raw outcome id does not match content")
        object.__setattr__(self, "raw_outcome_hash", digest)
        object.__setattr__(self, "raw_outcome_id", expected_id)
        return self


def enumerate_universe_outcome_plans(
    *,
    context: LabelCaptureAdmissionContext,
    constituents: Iterable[UniverseConstituent],
    label_policy_bundle: LabelPolicyBundle,
) -> tuple[UniverseOutcomePlan, ...]:
    """Generate exact T-cutoff universe plans without any current-universe query."""

    context.validate()
    if (
        label_policy_bundle.label_policy_bundle_id != context.label_policy_bundle.label_policy_bundle_id
        or label_policy_bundle.label_policy_bundle_hash != context.label_policy_bundle.label_policy_bundle_hash
    ):
        raise LabelBuilderError(REASON_LABEL_UNIVERSE_SET_INVALID, "universe policy bundle differs from admission context")
    supplied = tuple(constituents)
    for constituent in supplied:
        _canonical_revalidate(
            constituent,
            reason_code=REASON_LABEL_UNIVERSE_SET_INVALID,
            label="universe constituent",
        )
    frozen = tuple(sorted(supplied, key=lambda item: item.symbol))
    if len({item.symbol for item in frozen}) != len(frozen):
        raise LabelBuilderError(REASON_LABEL_UNIVERSE_SET_INVALID, "frozen universe contains duplicate symbols")
    universe_policy_hashes = {str(plan.universe_policy_hash) for plan in context.source_plans}
    if len(universe_policy_hashes) != 1:
        raise LabelBuilderError(
            REASON_LABEL_UNIVERSE_SET_INVALID,
            "source capture plans do not share one frozen universe policy",
        )
    expected_universe_policy_hash = next(iter(universe_policy_hashes))
    mappings = context.mapping_refs()
    observations_by_id = {item.observation_version_id: item for item in context.selected_observations}
    plans: list[UniverseOutcomePlan] = []
    source_members = {member.member_key: member for member in context.label_source_revision_set.members}
    for constituent in frozen:
        if constituent.universe_policy_hash != expected_universe_policy_hash:
            raise LabelBuilderError(REASON_LABEL_UNIVERSE_SET_INVALID, "universe constituent policy differs from frozen capture plan")
        for binding in constituent.source_member_bindings:
            member = source_members.get(binding.source_member_key)
            if member is None or member.source_role != binding.source_role or member.partition_content_hash != binding.partition_content_hash:
                raise LabelBuilderError(REASON_LABEL_UNIVERSE_SET_INVALID, "universe constituent source binding is absent from label source revision set")
        for mapping in mappings:
            observation = observations_by_id.get(mapping.terminal_observation_version_id)
            if observation is None:
                raise LabelBuilderError(REASON_LABEL_UNIVERSE_SET_INVALID, "universe plan mapping has no immutable observation")
            decision_as_of_trade_date = date.fromisoformat(
                str(observation.observation_payload["plan"]["decision_as_of_trade_date"])
            )
            for horizon in label_policy_bundle.horizons:
                for projection in label_policy_bundle.projections_by_horizon[horizon]:
                    owner = OutcomeOwner(
                        owner_type=OwnerType.UNIVERSE,
                        owner_key=canonical_json_sha256(
                            {
                                "canonical_signal_id": mapping.canonical_signal_id,
                                "symbol": constituent.symbol,
                                "universe_layer": constituent.universe_layer,
                            }
                        ),
                        canonical_signal_id=mapping.canonical_signal_id,
                        symbol=constituent.symbol,
                        decision_as_of_trade_date=decision_as_of_trade_date,
                        universe_layer=constituent.universe_layer,
                    )
                    plans.append(
                        UniverseOutcomePlan(
                            owner=owner,
                            canonical_signal_id=mapping.canonical_signal_id,
                            decision_as_of_trade_date=owner.decision_as_of_trade_date,
                            universe_layer=constituent.universe_layer,
                            horizon_trading_days=horizon,
                            projection=projection,
                            label_policy_bundle_hash=str(label_policy_bundle.label_policy_bundle_hash),
                            label_source_revision_set_hash=context.label_source_revision_set.source_revision_set_hash,
                            constituent_content_hash=str(constituent.constituent_content_hash),
                        )
                    )
    if len({str(plan.plan_hash) for plan in plans}) != len(plans):
        raise LabelBuilderError(REASON_LABEL_UNIVERSE_SET_INVALID, "universe outcome plans are duplicated")
    return tuple(sorted(plans, key=lambda item: str(item.plan_hash)))


class LabelBuilderRun(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    capture_batch: CaptureBatch
    label_versions: tuple[OutcomeLabelVersion, ...]
    selected_label_mappings: tuple[SelectedLabelMapping, ...]
    universe_raw_rows: tuple[UniverseRawOutcomeRow, ...]
    candidate_coverage: CandidateCoverageSummary
    universe_coverage: UniverseCoverageSummary
    gaps: tuple[LabelCaptureGap, ...]


class LabelBuilder:
    """Execute one fully supplied frozen label-capture request in memory."""

    def __init__(
        self,
        *,
        outcome_engine: OutcomeEngine,
        capture_repository: InMemoryCaptureBatchRepository,
        label_repository: InMemoryOutcomeLabelRepository,
        mapping_repository: InMemorySelectedLabelMappingRepository,
        evidence_store: LocalCalculationEvidenceStore,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._outcome_engine = outcome_engine
        self._capture_repository = capture_repository
        self._label_repository = label_repository
        self._mapping_repository = mapping_repository
        self._evidence_store = evidence_store
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self._completed_runs: dict[str, LabelBuilderRun] = {}
        self._run_lock = RLock()

    def run(
        self,
        *,
        context: LabelCaptureAdmissionContext,
        capture_request: LabelCaptureBatchRequestV2,
        candidate_outcome_requests: Mapping[str, OutcomeCalculationRequest],
        label_selection_requests: Mapping[str, LabelSelectionRequest],
        universe_constituents: Iterable[UniverseConstituent],
        universe_outcome_requests: Mapping[str, OutcomeCalculationRequest],
    ) -> LabelBuilderRun:
        try:
            return self._run_impl(
                context=context,
                capture_request=capture_request,
                candidate_outcome_requests=candidate_outcome_requests,
                label_selection_requests=label_selection_requests,
                universe_constituents=universe_constituents,
                universe_outcome_requests=universe_outcome_requests,
            )
        except Exception as error:
            reason_code = str(getattr(error, "reason_code", REASON_LABEL_EVIDENCE_IDENTITY_INVALID))
            logger.error(
                "advisory_label_builder_failed capture_batch_id=%s reason_code=%s error_type=%s",
                capture_request.capture_batch_id,
                reason_code,
                type(error).__name__,
                exc_info=True,
            )
            raise

    def _run_impl(
        self,
        *,
        context: LabelCaptureAdmissionContext,
        capture_request: LabelCaptureBatchRequestV2,
        candidate_outcome_requests: Mapping[str, OutcomeCalculationRequest],
        label_selection_requests: Mapping[str, LabelSelectionRequest],
        universe_constituents: Iterable[UniverseConstituent],
        universe_outcome_requests: Mapping[str, OutcomeCalculationRequest],
    ) -> LabelBuilderRun:
        with self._run_lock:
            existing_run = self._completed_runs.get(capture_request.capture_batch_id)
            if existing_run is not None:
                return existing_run
        context.validate()
        expected_binding = context.build_binding(capture_batch_id=capture_request.capture_batch_id)
        if expected_binding != capture_request.binding:
            raise LabelBuilderError(REASON_LABEL_EVIDENCE_IDENTITY_INVALID, "label capture request binding cannot be reconstructed from admission context")
        enumeration = enumerate_candidate_labels(context=context, label_policy_bundle=context.label_policy_bundle)
        if enumeration.descriptors != capture_request.planned_labels:
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "capture request planned labels differ from alpha_raw authority")
        constituents = tuple(universe_constituents)
        plans = enumerate_universe_outcome_plans(
            context=context,
            constituents=constituents,
            label_policy_bundle=context.label_policy_bundle,
        )
        batch = self._capture_repository.create(capture_request)
        if batch.status is CaptureBatchStatus.COMPLETE:
            raise LabelBuilderError(
                REASON_LABEL_EVIDENCE_IDENTITY_INVALID,
                "complete label capture batch has no in-memory run receipt for this builder instance",
            )
        if batch.status is CaptureBatchStatus.PLANNED:
            batch = self._capture_repository.acquire(
                capture_batch_id=capture_request.capture_batch_id,
                expected_row_version=batch.row_version,
                lease_seconds=300,
            )
        if batch.status is not CaptureBatchStatus.RUNNING:
            raise LabelBuilderError(REASON_LABEL_EVIDENCE_IDENTITY_INVALID, "label capture batch is not active")
        try:
            batch = self._register_admission_memberships(batch=batch, context=context)
            batch = self._register_stage_memberships(batch=batch, enumeration=enumeration)
            versions: list[OutcomeLabelVersion] = []
            mappings: list[SelectedLabelMapping] = []
            gaps: list[LabelCaptureGap] = []
            for descriptor in enumeration.descriptors:
                request = candidate_outcome_requests.get(descriptor.label_key_hash)
                if request is None:
                    gaps.append(self._gap(descriptor.canonical_identity(), "ADVISORY_PHASE1C3_LABEL_CANDIDATE_REQUEST_MISSING"))
                    continue
                self._validate_candidate_request(descriptor=descriptor, request=request, context=context)
                result = self._outcome_engine.calculate(request)
                stored = self._store_result(result)
                version = self._append_or_reuse_candidate(
                    descriptor=descriptor,
                    context=context,
                    result=result,
                    stored=stored,
                    created_by_capture_batch_id=capture_request.capture_batch_id,
                )
                selection_request = label_selection_requests.get(descriptor.label_key_hash)
                if selection_request is None:
                    raise LabelBuilderError(REASON_LABEL_SELECTOR_TERMINAL_CONFLICT, "every materialized candidate label requires a selection request")
                mapping = self._mapping_repository.save(
                    TerminalFirstLabelSelector().select(
                        request=selection_request,
                        label_versions=self._label_repository.chain_for(descriptor.label_key_hash),
                    )
                )
                versions.append(version)
                mappings.append(mapping)
                batch = self._register_output_memberships(batch=batch, version=version, mapping=mapping, stored=stored)
            universe_rows: list[UniverseRawOutcomeRow] = []
            for plan in plans:
                plan_hash = str(plan.plan_hash)
                request = universe_outcome_requests.get(plan_hash)
                if request is None:
                    gaps.append(self._gap({"plan_hash": plan_hash}, "ADVISORY_PHASE1C3_LABEL_UNIVERSE_REQUEST_MISSING"))
                    continue
                self._validate_universe_request(plan=plan, request=request, context=context)
                result = self._outcome_engine.calculate(request)
                stored = self._store_result(result)
                row = UniverseRawOutcomeRow(
                    plan=plan,
                    outcome_result=result,
                    calculation_evidence_sha256=stored.sha256,
                    calculation_evidence_size_bytes=stored.size_bytes,
                    calculation_evidence_store_backend_hash=stored.store_backend_hash,
                    calculation_evidence_uri=stored.uri,
                )
                universe_rows.append(row)
                batch = self._add_membership(
                    batch=batch,
                    role="universe_raw_outcome",
                    evidence_id=str(row.raw_outcome_id),
                    evidence_hash=str(row.raw_outcome_hash),
                )
                batch = self._add_membership(
                    batch=batch,
                    role="calculation_evidence",
                    evidence_id=stored.sha256,
                    evidence_hash=stored.sha256,
                )
            self._validate_closure(descriptors=enumeration.descriptors, versions=versions, gaps=gaps, plans=plans, rows=universe_rows)
            candidate_coverage = self._candidate_coverage(base=enumeration.coverage, versions=versions)
            universe_coverage = self._universe_coverage(
                frozen_constituent_count=len(constituents),
                plans=plans,
                rows=universe_rows,
            )
            batch = self._add_membership(
                batch=batch,
                role="candidate_coverage",
                evidence_id=f"ccs_{str(candidate_coverage.content_hash)[:20]}",
                evidence_hash=str(candidate_coverage.content_hash),
            )
            batch = self._add_membership(
                batch=batch,
                role="universe_coverage",
                evidence_id=f"ucs_{str(universe_coverage.content_hash)[:20]}",
                evidence_hash=str(universe_coverage.content_hash),
            )
            for gap in gaps:
                batch = self._add_membership(
                    batch=batch,
                    role="label_capture_gap",
                    evidence_id=f"lcg_{str(gap.gap_hash)[:20]}",
                    evidence_hash=str(gap.gap_hash),
                )
            complete = self._capture_repository.complete(
                capture_batch_id=batch.request.capture_batch_id,
                expected_row_version=batch.row_version,
                fencing_token=batch.fencing_token,
            )
            run = LabelBuilderRun(
                capture_batch=complete,
                label_versions=tuple(versions),
                selected_label_mappings=tuple(mappings),
                universe_raw_rows=tuple(universe_rows),
                candidate_coverage=candidate_coverage,
                universe_coverage=universe_coverage,
                gaps=tuple(gaps),
            )
            with self._run_lock:
                self._completed_runs[capture_request.capture_batch_id] = run
            return run
        except Exception as error:
            reason_code = str(getattr(error, "reason_code", REASON_LABEL_EVIDENCE_IDENTITY_INVALID))
            try:
                self._fail_active_batch(batch=batch, reason_code=reason_code)
            except Exception:
                logger.exception(
                    "advisory_label_builder_fail_transition_failed capture_batch_id=%s original_reason_code=%s",
                    capture_request.capture_batch_id,
                    reason_code,
                )
                raise
            raise

    def _register_admission_memberships(self, *, batch: CaptureBatch, context: LabelCaptureAdmissionContext) -> CaptureBatch:
        source = context.source_batch
        batch = self._add_membership(
            batch=batch,
            role="source_observation_capture",
            evidence_id=source.request.capture_batch_id,
            evidence_hash=source.capture_receipt_hash,
        )
        for mapping in context.mapping_refs():
            batch = self._add_membership(
                batch=batch,
                role="selected_observation_mapping",
                evidence_id=mapping.selected_mapping_id,
                evidence_hash=mapping.selected_mapping_hash,
            )
        for observation in context.selected_observations:
            batch = self._add_membership(
                batch=batch,
                role="selected_observation",
                evidence_id=observation.observation_version_id,
                evidence_hash=observation.observation_content_hash,
            )
        policy = context.label_policy_bundle
        batch = self._add_membership(
            batch=batch,
            role="label_policy_bundle",
            evidence_id=str(policy.label_policy_bundle_id),
            evidence_hash=str(policy.label_policy_bundle_hash),
        )
        source_set = context.label_source_revision_set
        return self._add_membership(
            batch=batch,
            role="label_source_revision_set",
            evidence_id=source_set.source_revision_set_id,
            evidence_hash=source_set.source_revision_set_hash,
        )

    def _register_stage_memberships(
        self,
        *,
        batch: CaptureBatch,
        enumeration: CandidateEnumerationResult,
    ) -> CaptureBatch:
        for reference in enumeration.stage_references:
            batch = self._add_membership(
                batch=batch,
                role="candidate_stage_evidence",
                evidence_id=str(reference.stage_evidence_id),
                evidence_hash=str(reference.stage_evidence_key_hash),
            )
        for component_hash in enumeration.component_evidence_hashes:
            batch = self._add_membership(
                batch=batch,
                role="multi_alpha_component_evidence",
                evidence_id=component_hash,
                evidence_hash=component_hash,
            )
        return batch

    def _register_output_memberships(
        self,
        *,
        batch: CaptureBatch,
        version: OutcomeLabelVersion,
        mapping: SelectedLabelMapping,
        stored: StoredCalculationEvidence,
    ) -> CaptureBatch:
        batch = self._add_membership(
            batch=batch,
            role="outcome_label_version",
            evidence_id=str(version.label_version_id),
            evidence_hash=str(version.label_content_hash),
        )
        batch = self._add_membership(
            batch=batch,
            role="selected_label_mapping",
            evidence_id=str(mapping.selected_label_mapping_id),
            evidence_hash=str(mapping.selected_label_mapping_hash),
        )
        return self._add_membership(
            batch=batch,
            role="calculation_evidence",
            evidence_id=stored.sha256,
            evidence_hash=stored.sha256,
        )

    def _add_membership(self, *, batch: CaptureBatch, role: str, evidence_id: str, evidence_hash: str) -> CaptureBatch:
        return self._capture_repository.add_membership(
            capture_batch_id=batch.request.capture_batch_id,
            expected_row_version=batch.row_version,
            fencing_token=batch.fencing_token,
            membership=CaptureMembership(
                evidence_role=role,
                evidence_id=evidence_id,
                evidence_content_hash=evidence_hash,
            ),
        )

    def _store_result(self, result: OutcomeCalculationResult) -> StoredCalculationEvidence:
        stored = self._evidence_store.put(result.calculation_evidence)
        if stored.sha256 != result.calculation_evidence.evidence_hash or stored.size_bytes < 1:
            raise LabelBuilderError(REASON_LABEL_EVIDENCE_IDENTITY_INVALID, "local evidence store readback differs from calculation evidence")
        return stored

    @staticmethod
    def _validate_candidate_request(
        *,
        descriptor: PlannedLabelDescriptor,
        request: OutcomeCalculationRequest,
        context: LabelCaptureAdmissionContext,
    ) -> None:
        owner = request.owner
        if (
            owner.owner_type is not OwnerType.CANDIDATE
            or owner.canonical_signal_id != descriptor.canonical_signal_id
            or owner.observation_version_id != descriptor.observation_version_id
            or owner.candidate_stage_evidence_id != descriptor.candidate_stage_evidence_id
            or owner.symbol != descriptor.symbol
            or owner.decision_as_of_trade_date != descriptor.decision_as_of_trade_date
            or request.horizon_trading_days != descriptor.horizon_trading_days
            or request.projection.value != descriptor.projection
            or request.policies.bundle.label_policy_bundle_hash != context.label_policy_bundle.label_policy_bundle_hash
            or request.label_source_revision_set.source_revision_set_hash != context.label_source_revision_set.source_revision_set_hash
        ):
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "candidate outcome request differs from frozen descriptor/context")

    @staticmethod
    def _validate_universe_request(
        *,
        plan: UniverseOutcomePlan,
        request: OutcomeCalculationRequest,
        context: LabelCaptureAdmissionContext,
    ) -> None:
        if (
            request.owner != plan.owner
            or request.horizon_trading_days != plan.horizon_trading_days
            or request.projection is not plan.projection
            or request.policies.bundle.label_policy_bundle_hash != plan.label_policy_bundle_hash
            or request.label_source_revision_set.source_revision_set_hash != plan.label_source_revision_set_hash
            or request.label_source_revision_set.source_revision_set_hash != context.label_source_revision_set.source_revision_set_hash
        ):
            raise LabelBuilderError(REASON_LABEL_UNIVERSE_SET_INVALID, "universe outcome request differs from frozen plan/context")

    def _append_or_reuse_candidate(
        self,
        *,
        descriptor: PlannedLabelDescriptor,
        context: LabelCaptureAdmissionContext,
        result: OutcomeCalculationResult,
        stored: StoredCalculationEvidence,
        created_by_capture_batch_id: str,
    ) -> OutcomeLabelVersion:
        chain = self._label_repository.chain_for(descriptor.label_key_hash)
        InMemoryOutcomeLabelRepository._validate_chain(chain)
        terminal = chain[-1] if chain else None
        if terminal is not None:
            prior_predecessor = chain[-2] if len(chain) > 1 else None
            retry_request = self._append_request(
                descriptor=descriptor,
                context=context,
                result=result,
                stored=stored,
                predecessor=prior_predecessor,
            )
            if retry_request.label_append_request_hash == terminal.label_append_request_hash:
                return self._label_repository.append(
                    request=retry_request,
                    created_by_capture_batch_id=created_by_capture_batch_id,
                )
        append_request = self._append_request(
            descriptor=descriptor,
            context=context,
            result=result,
            stored=stored,
            predecessor=terminal,
        )
        return self._label_repository.append(
            request=append_request,
            created_by_capture_batch_id=created_by_capture_batch_id,
        )

    @staticmethod
    def _append_request(
        *,
        descriptor: PlannedLabelDescriptor,
        context: LabelCaptureAdmissionContext,
        result: OutcomeCalculationResult,
        stored: StoredCalculationEvidence,
        predecessor: OutcomeLabelVersion | None,
    ) -> LabelAppendRequest:
        policy = context.label_policy_bundle
        return LabelAppendRequest(
            label_key_hash=descriptor.label_key_hash,
            expected_predecessor_version_id=predecessor.label_version_id if predecessor else None,
            expected_predecessor_version_hash=predecessor.label_content_hash if predecessor else None,
            expected_predecessor_revision_no=predecessor.label_revision_no if predecessor else None,
            label_policy_bundle_id=str(policy.label_policy_bundle_id),
            label_policy_bundle_hash=str(policy.label_policy_bundle_hash),
            label_policy_hash=policy.label_policy_hash,
            label_source_revision_set_id=context.label_source_revision_set.source_revision_set_id,
            label_source_revision_set_hash=context.label_source_revision_set.source_revision_set_hash,
            owner=result.owner,
            horizon_trading_days=result.horizon_trading_days,
            projection=result.projection,
            outcome_result=result,
            projection_payload_hash=str(result.projection_payload_hash),
            calculation_evidence_sha256=stored.sha256,
            calculation_evidence_size_bytes=stored.size_bytes,
            calculation_evidence_store_backend_hash=stored.store_backend_hash,
            calculation_evidence_uri=stored.uri,
        )

    def _gap(self, identity: dict[str, Any], reason_code: str) -> LabelCaptureGap:
        return LabelCaptureGap(
            planned_identity=identity,
            reason_code=reason_code,
            observed_at=_require_aware(self._now_provider(), field_name="now_provider"),
        )

    @staticmethod
    def _validate_closure(
        *,
        descriptors: tuple[PlannedLabelDescriptor, ...],
        versions: list[OutcomeLabelVersion],
        gaps: list[LabelCaptureGap],
        plans: tuple[UniverseOutcomePlan, ...],
        rows: list[UniverseRawOutcomeRow],
    ) -> None:
        output_keys = {item.label_key_hash for item in versions}
        gap_keys = {
            str(item.planned_identity.get("label_key_hash"))
            for item in gaps
            if item.planned_identity.get("label_key_hash") is not None
        }
        expected_keys = {item.label_key_hash for item in descriptors}
        if output_keys | gap_keys != expected_keys or output_keys & gap_keys:
            raise LabelBuilderError(REASON_LABEL_CANDIDATE_SET_INVALID, "candidate planned/output/gap closure is incomplete")
        row_hashes = {str(item.plan.plan_hash) for item in rows}
        universe_gap_hashes = {
            str(item.planned_identity.get("plan_hash"))
            for item in gaps
            if item.planned_identity.get("plan_hash") is not None
        }
        expected_plan_hashes = {str(item.plan_hash) for item in plans}
        if row_hashes | universe_gap_hashes != expected_plan_hashes or row_hashes & universe_gap_hashes:
            raise LabelBuilderError(REASON_LABEL_UNIVERSE_SET_INVALID, "universe planned/output/gap closure is incomplete")

    @staticmethod
    def _candidate_coverage(
        *,
        base: CandidateCoverageSummary,
        versions: list[OutcomeLabelVersion],
    ) -> CandidateCoverageSummary:
        maturity = Counter(version.outcome_result.maturity_status.value for version in versions)
        event = Counter(version.outcome_result.outcome_event_status.value for version in versions)
        projection = Counter(version.projection.value for version in versions)
        return CandidateCoverageSummary(
            observation_count=base.observation_count,
            included_count=base.included_count,
            excluded_count=base.excluded_count,
            empty_observation_count=base.empty_observation_count,
            planned_label_count=base.planned_label_count,
            maturity_counts=dict(sorted(maturity.items())),
            outcome_event_counts=dict(sorted(event.items())),
            projection_counts=dict(sorted(projection.items())),
        )

    @staticmethod
    def _universe_coverage(
        *,
        frozen_constituent_count: int,
        plans: tuple[UniverseOutcomePlan, ...],
        rows: list[UniverseRawOutcomeRow],
    ) -> UniverseCoverageSummary:
        maturity = Counter(row.outcome_result.maturity_status.value for row in rows)
        event = Counter(row.outcome_result.outcome_event_status.value for row in rows)
        projection = Counter(row.plan.projection.value for row in rows)
        denominator = Counter(
            row.plan.projection.value
            for row in rows
            if row.outcome_result.maturity_status is MaturityStatus.MATURED
        )
        return UniverseCoverageSummary(
            frozen_constituent_count=frozen_constituent_count,
            planned_row_count=len(plans),
            raw_row_count=len(rows),
            maturity_counts=dict(sorted(maturity.items())),
            outcome_event_counts=dict(sorted(event.items())),
            projection_counts=dict(sorted(projection.items())),
            denominator_count_by_projection=dict(sorted(denominator.items())),
        )

    def _fail_active_batch(self, *, batch: CaptureBatch, reason_code: str) -> None:
        latest = self._capture_repository.get(batch.request.capture_batch_id)
        if latest.status is not CaptureBatchStatus.RUNNING:
            return
        try:
            self._capture_repository.fail(
                capture_batch_id=latest.request.capture_batch_id,
                expected_row_version=latest.row_version,
                fencing_token=latest.fencing_token,
                reason_codes=(reason_code,),
            )
        except SourceLedgerError as failure:
            raise LabelBuilderError(
                REASON_LABEL_EVIDENCE_IDENTITY_INVALID,
                "label capture failed and the in-memory batch could not be moved to FAILED",
            ) from failure
