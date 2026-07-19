"""Typed contracts for Phase 1R historical-range Advisory research.

This module is deliberately free of Selection, Paper, simulation, QMT, QE,
Qlib, package validation, and database imports.  It closes deterministic
request identities, immutable artifact envelopes, persistence facts, and the
approved state machines only.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Literal, TypeAlias
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import canonical_json_sha256


BATCH_REQUEST_SCHEMA_VERSION = "advisory_historical_range_batch_request_v1"
RESOLVED_REQUEST_SCHEMA_VERSION = "advisory_historical_range_resolved_request_v1"
FROZEN_PROGRAM_SCHEMA_VERSION = "advisory_historical_range_frozen_program_v1"
DATE_PLAN_SCHEMA_VERSION = "advisory_historical_range_date_plan_v1"
ARTIFACT_ENVELOPE_SCHEMA_VERSION = "advisory_historical_range_artifact_envelope_v1"
ARTIFACT_REF_SCHEMA_VERSION = "advisory_historical_range_artifact_ref_v1"

HISTORICAL_RANGE_DATA_SOURCE = "DB_HISTORICAL"
HISTORICAL_RANGE_ORIGIN = "HISTORICAL_RANGE_RESEARCH"
HISTORICAL_RANGE_RESEARCH_SCOPE = "HISTORICAL_RESEARCH_ONLY"
HISTORICAL_RANGE_EVIDENCE_LEVEL = "RETROSPECTIVE_RESEARCH_ONLY"
HISTORICAL_RANGE_PRICE_TIMING_POLICY = "PIT_DECISION_THEN_MATURE"

REASON_CONTRACT_INVALID = "ADVISORY_HISTORICAL_RANGE_CONTRACT_INVALID"
REASON_HASH_INVALID = "ADVISORY_HISTORICAL_RANGE_HASH_INVALID"
REASON_STATE_TRANSITION_INVALID = "ADVISORY_HISTORICAL_RANGE_STATE_TRANSITION_INVALID"
REASON_IDENTITY_CONFLICT = "ADVISORY_HISTORICAL_RANGE_IDENTITY_CONFLICT"
REASON_IDEMPOTENCY_CONFLICT = "ADVISORY_HISTORICAL_RANGE_IDEMPOTENCY_CONFLICT"
REASON_ROW_VERSION_CONFLICT = "ADVISORY_HISTORICAL_RANGE_ROW_VERSION_CONFLICT"
REASON_DAY_PLAN_CONFLICT = "ADVISORY_HISTORICAL_RANGE_DAY_PLAN_CONFLICT"
REASON_REPOSITORY_CONFLICT = "ADVISORY_HISTORICAL_RANGE_REPOSITORY_CONFLICT"
REASON_ARTIFACT_ROOT_INVALID = "ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT_INVALID"
REASON_ARTIFACT_NOT_FOUND = "ADVISORY_HISTORICAL_RANGE_ARTIFACT_NOT_FOUND"
REASON_ARTIFACT_COLLISION = "ADVISORY_HISTORICAL_RANGE_ARTIFACT_COLLISION"
REASON_ARTIFACT_TAMPERED = "ADVISORY_HISTORICAL_RANGE_ARTIFACT_TAMPERED"


class HistoricalRangeContractError(RuntimeError):
    """Stable reason-coded contract failure."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = context or {}


class _StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def require_sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field_name} must be a 64-character lowercase sha256")
    return normalized


def _nonblank(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def derive_prefixed_id(prefix: str, payload: Any, *, digest_chars: int = 32) -> str:
    if digest_chars < 12 or digest_chars > 64:
        raise ValueError("digest_chars must be between 12 and 64")
    return f"{_nonblank(prefix, field_name='prefix')}_{canonical_json_sha256(payload)[:digest_chars]}"


class HistoricalRangeProgramSourceKind(str, Enum):
    EXISTING_PROGRAM = "EXISTING_PROGRAM"
    RESEARCH_PROGRAM_SPEC = "RESEARCH_PROGRAM_SPEC"


class HistoricalRangeAlphaMode(str, Enum):
    SINGLE_ALPHA = "single_alpha"
    MULTI_ALPHA = "multi_alpha"


class HistoricalRangeBatchStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PARTIAL = "PARTIAL"
    WAITING_INPUT = "WAITING_INPUT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


class HistoricalRangeProgramStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING_INPUT = "WAITING_INPUT"
    RETRYABLE_FAILED = "RETRYABLE_FAILED"
    PARTIAL = "PARTIAL"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class HistoricalRangeDayStatus(str, Enum):
    PENDING = "PENDING"
    WAITING_PREVIOUS_DAY = "WAITING_PREVIOUS_DAY"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    VALID_NO_CANDIDATE = "VALID_NO_CANDIDATE"
    WAITING_INPUT = "WAITING_INPUT"
    RETRYABLE_FAILED = "RETRYABLE_FAILED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class HistoricalRangeOutcomeStatus(str, Enum):
    NOT_DUE = "NOT_DUE"
    MATURING = "MATURING"
    COMPLETE = "COMPLETE"
    CENSORED = "CENSORED"
    TERMINAL = "TERMINAL"
    FAILED = "FAILED"


class HistoricalRangeOperationStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    RETRYABLE_FAILED = "RETRYABLE_FAILED"
    FAILED = "FAILED"


class HistoricalRangeOperationType(str, Enum):
    CREATE = "CREATE"
    RESUME = "RESUME"
    CANCEL = "CANCEL"
    REFRESH_OUTCOMES = "REFRESH_OUTCOMES"
    BUILD_DATASET_BRIDGE = "BUILD_DATASET_BRIDGE"


class HistoricalRangeListAction(str, Enum):
    ENTER = "ENTER"
    HOLD = "HOLD"
    EXIT = "EXIT"
    WATCH = "WATCH"


class HistoricalRangeOutcomeSubjectType(str, Enum):
    CANDIDATE = "CANDIDATE"
    EPISODE = "EPISODE"
    LIST_VERSION = "LIST_VERSION"
    RANGE = "RANGE"


class HistoricalRangeOutcomeProjection(str, Enum):
    RECOMMENDATION = "RECOMMENDATION"
    EXECUTABLE = "EXECUTABLE"


class HistoricalRangeArtifactKind(str, Enum):
    REQUEST = "REQUEST"
    DATE_PLAN = "DATE_PLAN"
    FROZEN_PROGRAM = "FROZEN_PROGRAM"
    CANDIDATE_ARTIFACT = "CANDIDATE_ARTIFACT"
    DAY_RECEIPT = "DAY_RECEIPT"
    RANGE_RECEIPT = "RANGE_RECEIPT"
    OUTCOME = "OUTCOME"
    SUMMARY = "SUMMARY"
    DATASET_BRIDGE = "DATASET_BRIDGE"


class HistoricalRangeSourceRevisionRefV1(_StrictContract):
    revision_id: str = Field(min_length=1, max_length=240)
    revision_hash: str = Field(min_length=64, max_length=64)

    @field_validator("revision_id")
    @classmethod
    def _revision_id(cls, value: str) -> str:
        return _nonblank(value, field_name="revision_id")

    @field_validator("revision_hash")
    @classmethod
    def _revision_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="revision_hash")


class ExistingProgramSpecV1(_StrictContract):
    source_kind: Literal[HistoricalRangeProgramSourceKind.EXISTING_PROGRAM] = (
        HistoricalRangeProgramSourceKind.EXISTING_PROGRAM
    )
    program_id: str = Field(min_length=1, max_length=160)
    expected_program_version: int = Field(ge=1)
    expected_binding_version_id: str = Field(min_length=1, max_length=160)

    @field_validator("program_id", "expected_binding_version_id")
    @classmethod
    def _required_text(cls, value: str, info: Any) -> str:
        return _nonblank(value, field_name=info.field_name)

    @property
    def research_program_id(self) -> str:
        return self.program_id

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ResearchProgramSpecV1(_StrictContract):
    source_kind: Literal[HistoricalRangeProgramSourceKind.RESEARCH_PROGRAM_SPEC] = (
        HistoricalRangeProgramSourceKind.RESEARCH_PROGRAM_SPEC
    )
    program_name: str = Field(min_length=1, max_length=200)
    package_id: str = Field(min_length=1, max_length=160)
    target_count: int = Field(ge=1)
    review_policy: dict[str, Any]
    runtime_config: dict[str, Any]
    entry_price_basis: str = Field(min_length=1, max_length=80)
    exit_price_basis: str = Field(min_length=1, max_length=80)
    style_profile_ref: str | None = Field(default=None, min_length=1, max_length=500)
    style_profile_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("program_name", "package_id", "entry_price_basis", "exit_price_basis")
    @classmethod
    def _required_text(cls, value: str, info: Any) -> str:
        return _nonblank(value, field_name=info.field_name)

    @field_validator("style_profile_hash")
    @classmethod
    def _style_hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="style_profile_hash") if value is not None else None

    @model_validator(mode="after")
    def _style_ref_is_closed(self) -> "ResearchProgramSpecV1":
        if (self.style_profile_ref is None) != (self.style_profile_hash is None):
            raise ValueError("style_profile_ref and style_profile_hash must be supplied together")
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"program_name"})

    @property
    def config_sha256(self) -> str:
        return canonical_json_sha256(self.semantic_payload())

    @property
    def research_program_id(self) -> str:
        return f"hrp_{self.config_sha256[:32]}"


HistoricalRangeProgramSpecV1: TypeAlias = Annotated[
    ExistingProgramSpecV1 | ResearchProgramSpecV1,
    Field(discriminator="source_kind"),
]


class HistoricalRangeResearchBatchRequestV1(_StrictContract):
    """Client-facing request. Package-derived identities are intentionally absent."""

    schema_version: Literal[BATCH_REQUEST_SCHEMA_VERSION] = BATCH_REQUEST_SCHEMA_VERSION
    request_id: str = Field(default_factory=lambda: f"ahrq_{uuid4().hex}", min_length=1, max_length=160)
    client_idempotency_key: str = Field(min_length=1, max_length=200)
    program_specs: tuple[HistoricalRangeProgramSpecV1, ...] = Field(min_length=1)
    start_trade_date: date
    end_trade_date: date
    data_source: Literal[HISTORICAL_RANGE_DATA_SOURCE] = HISTORICAL_RANGE_DATA_SOURCE
    origin: Literal[HISTORICAL_RANGE_ORIGIN] = HISTORICAL_RANGE_ORIGIN
    research_scope: Literal[HISTORICAL_RANGE_RESEARCH_SCOPE] = HISTORICAL_RANGE_RESEARCH_SCOPE
    evidence_level: Literal[HISTORICAL_RANGE_EVIDENCE_LEVEL] = HISTORICAL_RANGE_EVIDENCE_LEVEL
    execution_prohibited: Literal[True] = True
    requested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    requested_by: str = Field(default="local-user", min_length=1, max_length=160)
    user_request_semantic_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("request_id", "client_idempotency_key", "requested_by")
    @classmethod
    def _required_text(cls, value: str, info: Any) -> str:
        return _nonblank(value, field_name=info.field_name)

    @field_validator("requested_at")
    @classmethod
    def _requested_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="requested_at")

    @field_validator("user_request_semantic_hash")
    @classmethod
    def _semantic_hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="user_request_semantic_hash") if value is not None else None

    @model_validator(mode="after")
    def _close_request(self) -> "HistoricalRangeResearchBatchRequestV1":
        if self.start_trade_date > self.end_trade_date:
            raise ValueError("start_trade_date must not be after end_trade_date")
        ordered = tuple(sorted(self.program_specs, key=lambda item: item.research_program_id))
        identities = tuple(item.research_program_id for item in ordered)
        if len(identities) != len(set(identities)):
            raise ValueError("program_specs resolve to duplicate research_program_id values")
        object.__setattr__(self, "program_specs", ordered)
        digest = canonical_json_sha256(self.semantic_payload())
        if self.user_request_semantic_hash is not None and self.user_request_semantic_hash != digest:
            raise ValueError("user_request_semantic_hash does not match request business semantics")
        object.__setattr__(self, "user_request_semantic_hash", digest)
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "program_specs": [item.semantic_payload() for item in self.program_specs],
            "start_trade_date": self.start_trade_date,
            "end_trade_date": self.end_trade_date,
            "data_source": self.data_source,
            "origin": self.origin,
            "research_scope": self.research_scope,
            "evidence_level": self.evidence_level,
            "execution_prohibited": self.execution_prohibited,
        }


class HistoricalRangeAdmittedComponentV1(_StrictContract):
    component_id: str = Field(min_length=1, max_length=160)
    weight: Decimal = Field(gt=0, le=1)
    factor_order: tuple[str, ...] = Field(min_length=1)
    runtime_input_identity_hash: str = Field(min_length=64, max_length=64)
    lookback_contract_hash: str = Field(min_length=64, max_length=64)

    @field_validator("component_id")
    @classmethod
    def _component_id(cls, value: str) -> str:
        return _nonblank(value, field_name="component_id")

    @field_validator("factor_order")
    @classmethod
    def _factor_order(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(_nonblank(item, field_name="factor_order") for item in value)
        if len(normalized) != len(set(normalized)):
            raise ValueError("factor_order must be duplicate-free")
        return normalized

    @field_validator("runtime_input_identity_hash", "lookback_contract_hash")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)


class HistoricalRangeAdmittedPackageProjectionV1(_StrictContract):
    package_id: str = Field(min_length=1, max_length=160)
    package_version: int = Field(ge=1)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: HistoricalRangeAlphaMode
    components: tuple[HistoricalRangeAdmittedComponentV1, ...] = Field(min_length=1)

    @field_validator("package_id")
    @classmethod
    def _package_id(cls, value: str) -> str:
        return _nonblank(value, field_name="package_id")

    @field_validator("manifest_sha256")
    @classmethod
    def _manifest_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="manifest_sha256")

    @model_validator(mode="after")
    def _close_projection(self) -> "HistoricalRangeAdmittedPackageProjectionV1":
        ordered = tuple(sorted(self.components, key=lambda item: item.component_id))
        if len({item.component_id for item in ordered}) != len(ordered):
            raise ValueError("admitted components must have unique component_id values")
        if self.alpha_mode is HistoricalRangeAlphaMode.SINGLE_ALPHA and len(ordered) != 1:
            raise ValueError("single_alpha projection requires exactly one component")
        if self.alpha_mode is HistoricalRangeAlphaMode.MULTI_ALPHA and len(ordered) < 2:
            raise ValueError("multi_alpha projection requires at least two components")
        if sum((item.weight for item in ordered), start=Decimal("0")) != Decimal("1"):
            raise ValueError("admitted component weights must sum exactly to 1")
        object.__setattr__(self, "components", ordered)
        return self


class HistoricalRangeProgramWarmupComponentV1(_StrictContract):
    component_id: str = Field(min_length=1, max_length=160)
    warmup_start_trade_date: date
    range_start_trade_date: date
    lookback_contract_hash: str = Field(min_length=64, max_length=64)

    @field_validator("component_id")
    @classmethod
    def _component_id(cls, value: str) -> str:
        return _nonblank(value, field_name="component_id")

    @field_validator("lookback_contract_hash")
    @classmethod
    def _lookback_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="lookback_contract_hash")

    @model_validator(mode="after")
    def _range(self) -> "HistoricalRangeProgramWarmupComponentV1":
        if self.warmup_start_trade_date > self.range_start_trade_date:
            raise ValueError("warmup_start_trade_date cannot follow range_start_trade_date")
        return self


class HistoricalRangeProgramWarmupRangeV1(_StrictContract):
    research_program_id: str = Field(min_length=1, max_length=160)
    components: tuple[HistoricalRangeProgramWarmupComponentV1, ...] = Field(min_length=1)

    @field_validator("research_program_id")
    @classmethod
    def _program_id(cls, value: str) -> str:
        return _nonblank(value, field_name="research_program_id")

    @model_validator(mode="after")
    def _components(self) -> "HistoricalRangeProgramWarmupRangeV1":
        ordered = tuple(sorted(self.components, key=lambda item: item.component_id))
        if len({item.component_id for item in ordered}) != len(ordered):
            raise ValueError("warmup components must have unique component_id values")
        object.__setattr__(self, "components", ordered)
        return self


class HistoricalRangeFrozenProgramV1(_StrictContract):
    schema_version: Literal[FROZEN_PROGRAM_SCHEMA_VERSION] = FROZEN_PROGRAM_SCHEMA_VERSION
    research_program_id: str = Field(min_length=1, max_length=160)
    source_program_id: str | None = Field(default=None, min_length=1, max_length=160)
    source_program_version: int | None = Field(default=None, ge=1)
    source_binding_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    package_version: int = Field(ge=1)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: HistoricalRangeAlphaMode
    program_config_hash: str = Field(min_length=64, max_length=64)
    runtime_config_hash: str = Field(min_length=64, max_length=64)
    review_policy_hash: str = Field(min_length=64, max_length=64)
    style_profile_ref: str | None = Field(default=None, min_length=1, max_length=500)
    style_profile_hash: str | None = Field(default=None, min_length=64, max_length=64)
    code_release_id: str = Field(min_length=1, max_length=160)
    code_release_hash: str = Field(min_length=64, max_length=64)
    selection_semantics_version: str = Field(min_length=1, max_length=160)
    selection_semantics_hash: str = Field(min_length=64, max_length=64)
    list_semantics_version: str = Field(min_length=1, max_length=160)
    list_semantics_hash: str = Field(min_length=64, max_length=64)
    target_package_asset_root_hash: str = Field(min_length=64, max_length=64)
    input_warmup_contract_hash: str = Field(min_length=64, max_length=64)
    admitted_package_projection_hash: str = Field(min_length=64, max_length=64)
    admitted_package_projection: HistoricalRangeAdmittedPackageProjectionV1
    frozen_program_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "manifest_sha256",
        "program_config_hash",
        "runtime_config_hash",
        "review_policy_hash",
        "style_profile_hash",
        "code_release_hash",
        "selection_semantics_hash",
        "list_semantics_hash",
        "target_package_asset_root_hash",
        "input_warmup_contract_hash",
        "admitted_package_projection_hash",
        "frozen_program_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_program(self) -> "HistoricalRangeFrozenProgramV1":
        source_values = (
            self.source_program_id,
            self.source_program_version,
            self.source_binding_version_id,
        )
        if any(value is None for value in source_values) and not all(value is None for value in source_values):
            raise ValueError("source Program identity fields must be all present or all absent")
        if self.source_program_id is not None:
            if self.research_program_id != self.source_program_id:
                raise ValueError("existing Program research_program_id must equal source_program_id")
        elif not self.research_program_id.startswith("hrp_"):
            raise ValueError("research-only Program identity must start with hrp_")
        if (self.style_profile_ref is None) != (self.style_profile_hash is None):
            raise ValueError("style profile ref/hash must be supplied together")
        projection = self.admitted_package_projection
        if (
            projection.package_id != self.package_id
            or projection.package_version != self.package_version
            or projection.manifest_sha256 != self.manifest_sha256
            or projection.alpha_mode is not self.alpha_mode
        ):
            raise ValueError("admitted package projection differs from frozen package identity")
        projection_hash = canonical_json_sha256(projection.model_dump(mode="json"))
        if projection_hash != self.admitted_package_projection_hash:
            raise ValueError("admitted_package_projection_hash does not match the frozen projection")
        digest = canonical_json_sha256(self.semantic_payload())
        if self.frozen_program_hash is not None and self.frozen_program_hash != digest:
            raise ValueError("frozen_program_hash does not match frozen Program semantics")
        object.__setattr__(self, "frozen_program_hash", digest)
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"frozen_program_hash"})


class HistoricalRangeDatePlanV1(_StrictContract):
    schema_version: Literal[DATE_PLAN_SCHEMA_VERSION] = DATE_PLAN_SCHEMA_VERSION
    calendar_id: str = Field(min_length=1, max_length=160)
    calendar_version: str = Field(min_length=1, max_length=160)
    start_trade_date: date
    end_trade_date: date
    ordered_trade_dates: tuple[date, ...] = Field(min_length=1)
    ordered_trade_dates_hash: str | None = Field(default=None, min_length=64, max_length=64)
    completed_trade_date_watermark: date
    per_program_input_warmup_ranges: dict[str, HistoricalRangeProgramWarmupRangeV1]
    per_program_input_warmup_ranges_hash: str | None = Field(default=None, min_length=64, max_length=64)
    date_plan_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("ordered_trade_dates_hash", "per_program_input_warmup_ranges_hash", "date_plan_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_date_plan(self) -> "HistoricalRangeDatePlanV1":
        ordered = tuple(self.ordered_trade_dates)
        if ordered != tuple(sorted(ordered)) or len(ordered) != len(set(ordered)):
            raise ValueError("ordered_trade_dates must be strictly ordered and duplicate-free")
        if ordered[0] != self.start_trade_date or ordered[-1] != self.end_trade_date:
            raise ValueError("start/end must equal the first/last frozen trading dates")
        if self.end_trade_date > self.completed_trade_date_watermark:
            raise ValueError("end_trade_date exceeds completed_trade_date_watermark")
        dates_hash = canonical_json_sha256([value.isoformat() for value in ordered])
        for program_id, warmup in self.per_program_input_warmup_ranges.items():
            if warmup.research_program_id != program_id:
                raise ValueError("warmup range key differs from research_program_id")
            if any(item.range_start_trade_date != self.start_trade_date for item in warmup.components):
                raise ValueError("warmup range_start_trade_date must equal the frozen range start")
        warmup_hash = canonical_json_sha256(
            {key: value.model_dump(mode="json") for key, value in sorted(self.per_program_input_warmup_ranges.items())}
        )
        if self.ordered_trade_dates_hash is not None and self.ordered_trade_dates_hash != dates_hash:
            raise ValueError("ordered_trade_dates_hash does not match ordered_trade_dates")
        if (
            self.per_program_input_warmup_ranges_hash is not None
            and self.per_program_input_warmup_ranges_hash != warmup_hash
        ):
            raise ValueError("per_program_input_warmup_ranges_hash does not match warmup ranges")
        object.__setattr__(self, "ordered_trade_dates_hash", dates_hash)
        object.__setattr__(self, "per_program_input_warmup_ranges_hash", warmup_hash)
        digest = canonical_json_sha256(self.semantic_payload())
        if self.date_plan_hash is not None and self.date_plan_hash != digest:
            raise ValueError("date_plan_hash does not match date plan semantics")
        object.__setattr__(self, "date_plan_hash", digest)
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"date_plan_hash"})


class ResolvedHistoricalRangeRequestV1(_StrictContract):
    schema_version: Literal[RESOLVED_REQUEST_SCHEMA_VERSION] = RESOLVED_REQUEST_SCHEMA_VERSION
    request: HistoricalRangeResearchBatchRequestV1
    frozen_programs: tuple[HistoricalRangeFrozenProgramV1, ...] = Field(min_length=1)
    date_plan: HistoricalRangeDatePlanV1
    source_revision_catalog_hash: str = Field(min_length=64, max_length=64)
    selection_semantics_version: str = Field(min_length=1, max_length=160)
    selection_semantics_hash: str = Field(min_length=64, max_length=64)
    list_semantics_version: str = Field(min_length=1, max_length=160)
    list_semantics_hash: str = Field(min_length=64, max_length=64)
    resolved_program_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    request_payload_sha256: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "source_revision_catalog_hash",
        "selection_semantics_hash",
        "list_semantics_hash",
        "resolved_program_set_hash",
        "request_payload_sha256",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_resolved_request(self) -> "ResolvedHistoricalRangeRequestV1":
        ordered = tuple(sorted(self.frozen_programs, key=lambda item: item.research_program_id))
        if len(ordered) != len(self.request.program_specs):
            raise ValueError("resolved Program count differs from requested Program count")
        if len({item.research_program_id for item in ordered}) != len(ordered):
            raise ValueError("frozen_programs must have unique research_program_id values")
        requested_ids = {item.research_program_id for item in self.request.program_specs}
        if {item.research_program_id for item in ordered} != requested_ids:
            raise ValueError("frozen Program identities do not match requested Program specs")
        if set(self.date_plan.per_program_input_warmup_ranges) != requested_ids:
            raise ValueError("per_program_input_warmup_ranges must contain exactly every resolved Program")
        if (
            self.request.start_trade_date != self.date_plan.start_trade_date
            or self.request.end_trade_date != self.date_plan.end_trade_date
        ):
            raise ValueError("date plan range differs from the user request")
        for program in ordered:
            if (
                program.selection_semantics_version != self.selection_semantics_version
                or program.selection_semantics_hash != self.selection_semantics_hash
                or program.list_semantics_version != self.list_semantics_version
                or program.list_semantics_hash != self.list_semantics_hash
            ):
                raise ValueError("frozen Program semantics differ from resolved request semantics")
            warmup = self.date_plan.per_program_input_warmup_ranges[program.research_program_id]
            projection_components = {item.component_id: item for item in program.admitted_package_projection.components}
            warmup_components = {item.component_id: item for item in warmup.components}
            if set(projection_components) != set(warmup_components):
                raise ValueError("warmup component identities differ from the admitted package projection")
            if any(
                warmup_components[key].lookback_contract_hash != projection_components[key].lookback_contract_hash
                for key in projection_components
            ):
                raise ValueError("warmup lookback contract differs from the admitted package projection")
        object.__setattr__(self, "frozen_programs", ordered)
        program_set_hash = canonical_json_sha256(
            [
                {
                    "research_program_id": item.research_program_id,
                    "frozen_program_hash": item.frozen_program_hash,
                }
                for item in ordered
            ]
        )
        if self.resolved_program_set_hash is not None and self.resolved_program_set_hash != program_set_hash:
            raise ValueError("resolved_program_set_hash does not match frozen programs")
        object.__setattr__(self, "resolved_program_set_hash", program_set_hash)
        digest = canonical_json_sha256(self.semantic_payload())
        if self.request_payload_sha256 is not None and self.request_payload_sha256 != digest:
            raise ValueError("request_payload_sha256 does not match resolved business identity")
        object.__setattr__(self, "request_payload_sha256", digest)
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "user_request_semantic_hash": self.request.user_request_semantic_hash,
            "date_plan_hash": self.date_plan.date_plan_hash,
            "ordered_trade_dates_hash": self.date_plan.ordered_trade_dates_hash,
            "source_revision_catalog_hash": self.source_revision_catalog_hash,
            "selection_semantics_version": self.selection_semantics_version,
            "selection_semantics_hash": self.selection_semantics_hash,
            "list_semantics_version": self.list_semantics_version,
            "list_semantics_hash": self.list_semantics_hash,
            "resolved_program_set_hash": self.resolved_program_set_hash,
            "per_program_input_warmup_ranges_hash": self.date_plan.per_program_input_warmup_ranges_hash,
            "frozen_programs": [
                {
                    "research_program_id": item.research_program_id,
                    "frozen_program_hash": item.frozen_program_hash,
                }
                for item in self.frozen_programs
            ],
        }

    @property
    def batch_id(self) -> str:
        return f"ahrb_{self.request_payload_sha256[:32]}"

    def range_run_id(self, research_program_id: str) -> str:
        return derive_prefixed_id(
            "ahrr",
            {
                "batch_id": self.batch_id,
                "research_program_id": _nonblank(research_program_id, field_name="research_program_id"),
            },
        )


class HistoricalRangeArtifactRefV1(_StrictContract):
    schema_version: Literal[ARTIFACT_REF_SCHEMA_VERSION] = ARTIFACT_REF_SCHEMA_VERSION
    artifact_kind: HistoricalRangeArtifactKind
    relative_path: str = Field(min_length=1, max_length=600)
    producer_contract_version: str = Field(min_length=1, max_length=160)
    payload_schema_version: str = Field(min_length=1, max_length=160)
    semantic_content_hash: str = Field(min_length=64, max_length=64)
    payload_sha256: str = Field(min_length=64, max_length=64)
    file_sha256: str = Field(min_length=64, max_length=64)

    @field_validator("semantic_content_hash", "payload_sha256", "file_sha256")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @field_validator("relative_path")
    @classmethod
    def _relative_path(cls, value: str) -> str:
        normalized = value.replace("\\", "/").strip()
        if not normalized or normalized.startswith("/") or ".." in normalized.split("/"):
            raise ValueError("relative_path must be a contained relative path")
        return normalized


class HistoricalRangeArtifactEnvelopeV1(_StrictContract):
    schema_version: Literal[ARTIFACT_ENVELOPE_SCHEMA_VERSION] = ARTIFACT_ENVELOPE_SCHEMA_VERSION
    artifact_kind: HistoricalRangeArtifactKind
    producer_contract_version: str = Field(min_length=1, max_length=160)
    payload_schema_version: str = Field(min_length=1, max_length=160)
    resolved_request_hash: str = Field(min_length=64, max_length=64)
    range_run_id: str | None = Field(default=None, min_length=1, max_length=160)
    day_run_id: str | None = Field(default=None, min_length=1, max_length=160)
    source_revision_refs: tuple[HistoricalRangeSourceRevisionRefV1, ...] = ()
    upstream_refs: tuple[HistoricalRangeArtifactRefV1, ...] = ()
    payload: dict[str, Any]
    payload_sha256: str | None = Field(default=None, min_length=64, max_length=64)
    semantic_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("resolved_request_hash", "payload_sha256", "semantic_content_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_envelope(self) -> "HistoricalRangeArtifactEnvelopeV1":
        source_refs = tuple(sorted(self.source_revision_refs, key=lambda item: (item.revision_id, item.revision_hash)))
        source_identities = tuple((item.revision_id, item.revision_hash) for item in source_refs)
        if len(source_identities) != len(set(source_identities)) or len(source_refs) != len(
            {item.revision_id for item in source_refs}
        ):
            raise ValueError("source_revision_refs must have unique revision_id values")
        upstream = tuple(
            sorted(
                self.upstream_refs,
                key=lambda item: (item.artifact_kind.value, item.semantic_content_hash, item.relative_path),
            )
        )
        upstream_identities = tuple(
            (item.artifact_kind, item.semantic_content_hash, item.relative_path) for item in upstream
        )
        if len(upstream_identities) != len(set(upstream_identities)):
            raise ValueError("upstream_refs must be duplicate-free")
        object.__setattr__(self, "source_revision_refs", source_refs)
        object.__setattr__(self, "upstream_refs", upstream)
        range_only_kinds = {
            HistoricalRangeArtifactKind.FROZEN_PROGRAM,
            HistoricalRangeArtifactKind.OUTCOME,
            HistoricalRangeArtifactKind.SUMMARY,
            HistoricalRangeArtifactKind.DATASET_BRIDGE,
        }
        day_kinds = {
            HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
            HistoricalRangeArtifactKind.DAY_RECEIPT,
        }
        if self.artifact_kind in {HistoricalRangeArtifactKind.REQUEST, HistoricalRangeArtifactKind.DATE_PLAN}:
            if self.range_run_id is not None or self.day_run_id is not None:
                raise ValueError("request/date-plan artifacts cannot carry range/day identity")
        elif self.artifact_kind in range_only_kinds:
            if self.range_run_id is None or self.day_run_id is not None:
                raise ValueError("range artifact requires range_run_id and forbids day_run_id")
        elif self.artifact_kind is HistoricalRangeArtifactKind.RANGE_RECEIPT:
            if self.day_run_id is not None:
                raise ValueError("range receipt cannot carry day_run_id")
        elif self.artifact_kind in day_kinds and (self.range_run_id is None or self.day_run_id is None):
            raise ValueError("day artifact requires range_run_id and day_run_id")
        if self.artifact_kind is HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT and not source_refs:
            raise ValueError("candidate artifacts require explicit source revision lineage")
        payload_hash = canonical_json_sha256(self.payload)
        if self.payload_sha256 is not None and self.payload_sha256 != payload_hash:
            raise ValueError("payload_sha256 does not match payload")
        object.__setattr__(self, "payload_sha256", payload_hash)
        digest = canonical_json_sha256(self.semantic_payload())
        if self.semantic_content_hash is not None and self.semantic_content_hash != digest:
            raise ValueError("semantic_content_hash does not match artifact envelope")
        object.__setattr__(self, "semantic_content_hash", digest)
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"semantic_content_hash"})


class HistoricalRangeArtifactBindingsV1(_StrictContract):
    request_ref: HistoricalRangeArtifactRefV1
    date_plan_ref: HistoricalRangeArtifactRefV1
    frozen_program_refs: dict[str, HistoricalRangeArtifactRefV1]
    artifact_root_identity_hash: str = Field(min_length=64, max_length=64)

    @field_validator("artifact_root_identity_hash")
    @classmethod
    def _root_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="artifact_root_identity_hash")

    @model_validator(mode="after")
    def _kinds(self) -> "HistoricalRangeArtifactBindingsV1":
        if self.request_ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST:
            raise ValueError("request_ref must reference a REQUEST artifact")
        if self.date_plan_ref.artifact_kind is not HistoricalRangeArtifactKind.DATE_PLAN:
            raise ValueError("date_plan_ref must reference a DATE_PLAN artifact")
        if not self.frozen_program_refs:
            raise ValueError("frozen_program_refs cannot be empty")
        if any(
            ref.artifact_kind is not HistoricalRangeArtifactKind.FROZEN_PROGRAM
            for ref in self.frozen_program_refs.values()
        ):
            raise ValueError("frozen_program_refs must reference FROZEN_PROGRAM artifacts")
        return self


class HistoricalRangeDayPlanEntryV1(_StrictContract):
    range_run_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    ordinal: int = Field(ge=1)
    day_run_id: str | None = Field(default=None, min_length=1, max_length=160)
    previous_day_run_id: str | None = Field(default=None, min_length=1, max_length=160)

    @model_validator(mode="after")
    def _derive_ids(self) -> "HistoricalRangeDayPlanEntryV1":
        expected = derive_day_run_id(self.range_run_id, self.decision_trade_date, self.ordinal)
        if self.day_run_id is not None and self.day_run_id != expected:
            raise ValueError("day_run_id does not match range/date/ordinal")
        object.__setattr__(self, "day_run_id", expected)
        if self.ordinal == 1 and self.previous_day_run_id is not None:
            raise ValueError("first day cannot reference a predecessor")
        return self


class HistoricalRangeDayAttemptV1(_StrictContract):
    attempt_id: str = Field(min_length=1, max_length=160)
    day_run_id: str = Field(min_length=1, max_length=160)
    attempt_no: int = Field(ge=1)
    worker_id: str = Field(min_length=1, max_length=160)
    lease_token: str = Field(min_length=1, max_length=200)
    fencing_token: int = Field(ge=1)
    status: Literal[
        "RUNNING", "COMPLETE", "VALID_NO_CANDIDATE", "WAITING_INPUT", "RETRYABLE_FAILED", "FAILED", "CANCELLED"
    ]
    input_hash: str = Field(min_length=64, max_length=64)
    result_hash: str | None = Field(default=None, min_length=64, max_length=64)
    candidate_artifact_ref: HistoricalRangeArtifactRefV1 | None = None
    attempt_receipt_ref: HistoricalRangeArtifactRefV1 | None = None
    reason_codes: tuple[str, ...] = ()
    error_json: dict[str, Any] | None = None
    started_at: datetime
    finished_at: datetime | None = None

    @field_validator(
        "input_hash",
        "result_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("started_at", "finished_at")
    @classmethod
    def _timestamps(cls, value: datetime | None, info: Any) -> datetime | None:
        return _aware_utc(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _attempt_evidence(self) -> "HistoricalRangeDayAttemptV1":
        if (
            self.candidate_artifact_ref is not None
            and self.candidate_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT
        ):
            raise ValueError("candidate_artifact_ref must reference CANDIDATE_ARTIFACT")
        if (
            self.attempt_receipt_ref is not None
            and self.attempt_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT
        ):
            raise ValueError("attempt_receipt_ref must reference DAY_RECEIPT")
        if self.status != "RUNNING" and self.attempt_receipt_ref is None:
            raise ValueError("non-running day attempts require an immutable attempt receipt")
        if (self.status == "RUNNING") != (self.finished_at is None):
            raise ValueError("only RUNNING day attempts may omit finished_at")
        normalized_reasons = tuple(sorted(_nonblank(value, field_name="reason_code") for value in self.reason_codes))
        if len(normalized_reasons) != len(set(normalized_reasons)):
            raise ValueError("reason_codes must be duplicate-free")
        object.__setattr__(self, "reason_codes", normalized_reasons)
        return self


class HistoricalRangeOperationRequestV1(_StrictContract):
    operation_id: str = Field(min_length=1, max_length=160)
    batch_id: str = Field(min_length=1, max_length=160)
    operation_type: HistoricalRangeOperationType
    operation_idempotency_key: str = Field(min_length=1, max_length=200)
    request_payload_sha256: str = Field(min_length=64, max_length=64)
    expected_row_version: int | None = Field(default=None, ge=1)

    @field_validator("request_payload_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return require_sha256(value, field_name="request_payload_sha256")


class HistoricalRangeOperationAttemptV1(_StrictContract):
    attempt_id: str = Field(min_length=1, max_length=160)
    operation_id: str = Field(min_length=1, max_length=160)
    attempt_no: int = Field(ge=1)
    worker_id: str = Field(min_length=1, max_length=160)
    lease_token: str = Field(min_length=1, max_length=200)
    fencing_token: int = Field(ge=1)
    status: Literal["RUNNING", "COMPLETED", "RETRYABLE_FAILED", "FAILED"]
    input_cursor_json: dict[str, Any] | None = None
    result_cursor_json: dict[str, Any] | None = None
    input_hash: str = Field(min_length=64, max_length=64)
    result_hash: str | None = Field(default=None, min_length=64, max_length=64)
    attempt_receipt_ref: HistoricalRangeArtifactRefV1 | None = None
    reason_codes: tuple[str, ...] = ()
    error_json: dict[str, Any] | None = None
    started_at: datetime
    finished_at: datetime | None = None

    @field_validator("input_hash", "result_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("started_at", "finished_at")
    @classmethod
    def _timestamps(cls, value: datetime | None, info: Any) -> datetime | None:
        return _aware_utc(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _receipt_pair(self) -> "HistoricalRangeOperationAttemptV1":
        if (
            self.attempt_receipt_ref is not None
            and self.attempt_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.RANGE_RECEIPT
        ):
            raise ValueError("operation attempt receipt must reference RANGE_RECEIPT")
        if self.status != "RUNNING" and self.attempt_receipt_ref is None:
            raise ValueError("non-running operation attempts require an immutable receipt")
        if (self.status == "RUNNING") != (self.finished_at is None):
            raise ValueError("only RUNNING operation attempts may omit finished_at")
        normalized_reasons = tuple(sorted(_nonblank(value, field_name="reason_code") for value in self.reason_codes))
        if len(normalized_reasons) != len(set(normalized_reasons)):
            raise ValueError("reason_codes must be duplicate-free")
        object.__setattr__(self, "reason_codes", normalized_reasons)
        return self


class HistoricalRangeCandidateFactV1(_StrictContract):
    candidate_id: str = Field(min_length=1, max_length=160)
    day_run_id: str = Field(min_length=1, max_length=160)
    symbol: str = Field(min_length=1, max_length=32)
    membership_status: Literal["INCLUDED", "EXCLUDED"]
    alpha_raw_rank: int | None = Field(default=None, ge=1)
    alpha_raw_score: Decimal | None = None
    hmm_adjusted_rank: int | None = Field(default=None, ge=1)
    hmm_adjusted_score: Decimal | None = None
    risk_policy_adjusted_rank: int | None = Field(default=None, ge=1)
    risk_policy_adjusted_score: Decimal | None = None
    selection_effective_rank: int | None = Field(default=None, ge=1)
    selection_effective_score: Decimal | None = None
    advisory_model_rank: int | None = Field(default=None, ge=1)
    advisory_model_score: Decimal | None = None
    component_lineage_json: dict[str, Any]
    component_lineage_hash: str = Field(min_length=64, max_length=64)
    candidate_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return _nonblank(value, field_name="symbol").upper()

    @field_validator("component_lineage_hash", "candidate_content_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _lineage_hash(self) -> "HistoricalRangeCandidateFactV1":
        if canonical_json_sha256(self.component_lineage_json) != self.component_lineage_hash:
            raise ValueError("component_lineage_hash does not match component_lineage_json")
        if self.membership_status == "INCLUDED" and (
            self.selection_effective_rank is None or self.selection_effective_score is None
        ):
            raise ValueError("included candidates require selection_effective_rank and selection_effective_score")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"candidate_content_hash"}))
        if self.candidate_content_hash is not None and self.candidate_content_hash != digest:
            raise ValueError("candidate_content_hash does not match candidate facts")
        object.__setattr__(self, "candidate_content_hash", digest)
        return self


class HistoricalRangeListVersionFactV1(_StrictContract):
    list_version_id: str = Field(min_length=1, max_length=160)
    day_run_id: str = Field(min_length=1, max_length=160)
    range_run_id: str = Field(min_length=1, max_length=160)
    previous_list_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    previous_list_hash: str | None = Field(default=None, min_length=64, max_length=64)
    previous_day_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)
    target_count: int = Field(ge=1)
    active_count: int = Field(ge=0)
    enter_count: int = Field(ge=0)
    hold_count: int = Field(ge=0)
    exit_count: int = Field(ge=0)
    watch_count: int = Field(ge=0)
    price_timing_policy: Literal[HISTORICAL_RANGE_PRICE_TIMING_POLICY] = HISTORICAL_RANGE_PRICE_TIMING_POLICY
    summary_json: dict[str, Any]
    list_content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("previous_list_hash", "previous_day_receipt_hash", "list_content_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _counts(self) -> "HistoricalRangeListVersionFactV1":
        if self.active_count != self.enter_count + self.hold_count:
            raise ValueError("active_count must equal enter_count + hold_count")
        if self.active_count > self.target_count:
            raise ValueError("active_count cannot exceed target_count")
        return self


class HistoricalRangeListItemFactV1(_StrictContract):
    list_item_id: str = Field(min_length=1, max_length=160)
    list_version_id: str = Field(min_length=1, max_length=160)
    symbol: str = Field(min_length=1, max_length=32)
    action: HistoricalRangeListAction
    rank: int | None = Field(default=None, ge=1)
    score: Decimal | None = None
    reason_codes: tuple[str, ...] = ()
    episode_id: str | None = Field(default=None, min_length=1, max_length=160)
    rule_guidance_json: dict[str, Any]
    intended_execution_trade_date: date | None = None
    intended_execution_basis: str | None = Field(default=None, min_length=1, max_length=80)
    execution_status: str = Field(min_length=1, max_length=80)
    evidence_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return _nonblank(value, field_name="symbol").upper()

    @field_validator("evidence_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="evidence_hash") if value is not None else None

    @model_validator(mode="after")
    def _episode_contract(self) -> "HistoricalRangeListItemFactV1":
        if self.action is HistoricalRangeListAction.WATCH and self.episode_id is not None:
            raise ValueError("WATCH items cannot reference an episode")
        if self.action is not HistoricalRangeListAction.WATCH and self.episode_id is None:
            raise ValueError("ENTER/HOLD/EXIT items require an episode")
        if (self.intended_execution_trade_date is None) != (self.intended_execution_basis is None):
            raise ValueError("intended execution date/basis must be supplied together")
        normalized_reasons = tuple(sorted(_nonblank(value, field_name="reason_code") for value in self.reason_codes))
        if len(normalized_reasons) != len(set(normalized_reasons)):
            raise ValueError("reason_codes must be duplicate-free")
        object.__setattr__(self, "reason_codes", normalized_reasons)
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"evidence_hash"}))
        if self.evidence_hash is not None and self.evidence_hash != digest:
            raise ValueError("evidence_hash does not match list item facts")
        object.__setattr__(self, "evidence_hash", digest)
        return self


class HistoricalRangeEpisodeSnapshotFactV1(_StrictContract):
    episode_snapshot_id: str = Field(min_length=1, max_length=160)
    range_run_id: str = Field(min_length=1, max_length=160)
    list_version_id: str = Field(min_length=1, max_length=160)
    episode_id: str = Field(min_length=1, max_length=160)
    symbol: str = Field(min_length=1, max_length=32)
    decision_trade_date: date
    entry_sequence: int = Field(ge=1)
    enter_decision_trade_date: date
    exit_decision_trade_date: date | None = None
    recommendation_state: Literal["ACTIVE", "EXITED", "ACTIVE_AT_RANGE_END"]
    action: Literal["ENTER", "HOLD", "EXIT"]
    execution_status: str = Field(min_length=1, max_length=80)
    price_quality: str = Field(min_length=1, max_length=80)
    weak_rank_confirmation_count: int = Field(ge=0)
    mark_json: dict[str, Any]
    evidence_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return _nonblank(value, field_name="symbol").upper()

    @field_validator("evidence_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="evidence_hash") if value is not None else None

    @model_validator(mode="after")
    def _state(self) -> "HistoricalRangeEpisodeSnapshotFactV1":
        expected = derive_episode_id(
            self.range_run_id,
            self.symbol,
            self.enter_decision_trade_date,
            self.entry_sequence,
        )
        if self.episode_id != expected:
            raise ValueError("episode_id does not match its deterministic identity")
        if self.action == "EXIT" and self.recommendation_state != "EXITED":
            raise ValueError("EXIT action requires EXITED recommendation state")
        if self.action != "EXIT" and self.recommendation_state == "EXITED":
            raise ValueError("active action cannot use EXITED recommendation state")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"evidence_hash"}))
        if self.evidence_hash is not None and self.evidence_hash != digest:
            raise ValueError("evidence_hash does not match episode snapshot facts")
        object.__setattr__(self, "evidence_hash", digest)
        return self


class HistoricalRangeOutcomeFactV1(_StrictContract):
    outcome_version_id: str = Field(min_length=1, max_length=160)
    outcome_logical_id: str = Field(min_length=1, max_length=160)
    outcome_version: int = Field(ge=1)
    subject_type: HistoricalRangeOutcomeSubjectType
    subject_id: str = Field(min_length=1, max_length=160)
    projection: HistoricalRangeOutcomeProjection
    horizon_trade_days: int = Field(ge=1)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    predecessor_outcome_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    predecessor_outcome_hash: str | None = Field(default=None, min_length=64, max_length=64)
    maturity_status: HistoricalRangeOutcomeStatus
    label_as_of_trade_date: date | None = None
    next_refresh_trade_date: date | None = None
    entry_execution_evidence_json: dict[str, Any] | None = None
    exit_execution_evidence_json: dict[str, Any] | None = None
    benchmark_hash: str | None = Field(default=None, min_length=64, max_length=64)
    cost_policy_hash: str | None = Field(default=None, min_length=64, max_length=64)
    corporate_action_hash: str | None = Field(default=None, min_length=64, max_length=64)
    calculation_evidence_ref: HistoricalRangeArtifactRefV1 | None = None
    outcome_artifact_ref: HistoricalRangeArtifactRefV1
    outcome_json: dict[str, Any]
    outcome_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "label_policy_hash",
        "source_revision_set_hash",
        "predecessor_outcome_hash",
        "benchmark_hash",
        "cost_policy_hash",
        "corporate_action_hash",
        "outcome_content_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeOutcomeFactV1":
        expected = derive_outcome_logical_id(
            self.subject_type,
            self.subject_id,
            self.projection,
            self.horizon_trade_days,
            self.label_policy_hash,
        )
        if self.outcome_logical_id != expected:
            raise ValueError("outcome_logical_id does not match outcome identity")
        if self.outcome_version == 1:
            if self.predecessor_outcome_version_id is not None or self.predecessor_outcome_hash is not None:
                raise ValueError("first outcome version cannot have a predecessor")
        elif self.predecessor_outcome_version_id is None or self.predecessor_outcome_hash is None:
            raise ValueError("later outcome versions require predecessor identity/hash")
        if self.outcome_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME:
            raise ValueError("outcome_artifact_ref must reference OUTCOME")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"outcome_content_hash"}))
        if self.outcome_content_hash is not None and self.outcome_content_hash != digest:
            raise ValueError("outcome_content_hash does not match outcome facts")
        object.__setattr__(self, "outcome_content_hash", digest)
        return self


class HistoricalRangeSummaryFactV1(_StrictContract):
    summary_id: str = Field(min_length=1, max_length=160)
    range_run_id: str = Field(min_length=1, max_length=160)
    summary_version: int = Field(ge=1)
    covered_outcome_set_hash: str = Field(min_length=64, max_length=64)
    predecessor_summary_id: str | None = Field(default=None, min_length=1, max_length=160)
    predecessor_summary_hash: str | None = Field(default=None, min_length=64, max_length=64)
    summary_artifact_ref: HistoricalRangeArtifactRefV1
    summary_json: dict[str, Any]
    summary_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "covered_outcome_set_hash",
        "predecessor_summary_hash",
        "summary_content_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _predecessor(self) -> "HistoricalRangeSummaryFactV1":
        if self.summary_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.SUMMARY:
            raise ValueError("summary_artifact_ref must reference SUMMARY")
        if self.summary_version == 1:
            if self.predecessor_summary_id is not None or self.predecessor_summary_hash is not None:
                raise ValueError("first summary version cannot have a predecessor")
        elif self.predecessor_summary_id is None or self.predecessor_summary_hash is None:
            raise ValueError("later summary versions require predecessor identity/hash")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"summary_content_hash"}))
        if self.summary_content_hash is not None and self.summary_content_hash != digest:
            raise ValueError("summary_content_hash does not match summary facts")
        object.__setattr__(self, "summary_content_hash", digest)
        return self


def derive_list_content_hash(
    list_version: HistoricalRangeListVersionFactV1,
    items: Sequence[HistoricalRangeListItemFactV1],
    episodes: Sequence[HistoricalRangeEpisodeSnapshotFactV1],
) -> str:
    ordered_items = sorted(
        (item.model_dump(mode="json") for item in items),
        key=lambda item: (item["symbol"], item["action"]),
    )
    ordered_episodes = sorted(
        (episode.model_dump(mode="json") for episode in episodes),
        key=lambda item: (item["symbol"], item["episode_id"]),
    )
    return canonical_json_sha256(
        {
            "schema_version": "advisory_historical_range_list_content_v1",
            "list_header": list_version.model_dump(mode="json", exclude={"list_content_hash"}),
            "items": ordered_items,
            "episode_snapshots": ordered_episodes,
        }
    )


def build_candidate_artifact_payload(
    *,
    range_run_id: str,
    day_run_id: str,
    candidates: Sequence[HistoricalRangeCandidateFactV1],
    source_revision_refs: Sequence[HistoricalRangeSourceRevisionRefV1],
) -> dict[str, Any]:
    if not source_revision_refs:
        raise ValueError("candidate artifact payload requires source_revision_refs")
    return {
        "schema_version": "advisory_historical_range_candidate_artifact_payload_v1",
        "range_run_id": _nonblank(range_run_id, field_name="range_run_id"),
        "day_run_id": _nonblank(day_run_id, field_name="day_run_id"),
        "source_revision_refs": [
            item.model_dump(mode="json")
            for item in sorted(source_revision_refs, key=lambda item: (item.revision_id, item.revision_hash))
        ],
        "candidates": sorted(
            (item.model_dump(mode="json") for item in candidates),
            key=lambda item: (item["symbol"], item["candidate_id"]),
        ),
    }


def build_day_receipt_payload(
    *,
    range_run_id: str,
    day_run_id: str,
    terminal_status: HistoricalRangeDayStatus,
    day_input_hash: str,
    candidate_artifact_ref: HistoricalRangeArtifactRefV1,
    list_version: HistoricalRangeListVersionFactV1,
    items: Sequence[HistoricalRangeListItemFactV1],
    episodes: Sequence[HistoricalRangeEpisodeSnapshotFactV1],
    reason_codes: Sequence[str] = (),
) -> dict[str, Any]:
    if terminal_status not in {
        HistoricalRangeDayStatus.COMPLETE,
        HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
    }:
        raise ValueError("day receipt terminal_status must be a successful day state")
    return {
        "schema_version": "advisory_historical_range_day_receipt_payload_v1",
        "range_run_id": _nonblank(range_run_id, field_name="range_run_id"),
        "day_run_id": _nonblank(day_run_id, field_name="day_run_id"),
        "terminal_status": terminal_status.value,
        "day_input_hash": require_sha256(day_input_hash, field_name="day_input_hash"),
        "candidate_artifact_ref": candidate_artifact_ref.model_dump(mode="json"),
        "list_version": list_version.model_dump(mode="json"),
        "items": sorted(
            (item.model_dump(mode="json") for item in items),
            key=lambda item: (item["symbol"], item["action"], item["list_item_id"]),
        ),
        "episode_snapshots": sorted(
            (item.model_dump(mode="json") for item in episodes),
            key=lambda item: (item["symbol"], item["episode_id"], item["episode_snapshot_id"]),
        ),
        "reason_codes": sorted(_nonblank(item, field_name="reason_code") for item in reason_codes),
    }


BATCH_TRANSITIONS: dict[HistoricalRangeBatchStatus, frozenset[HistoricalRangeBatchStatus]] = {
    HistoricalRangeBatchStatus.QUEUED: frozenset(
        {HistoricalRangeBatchStatus.RUNNING, HistoricalRangeBatchStatus.CANCELLED}
    ),
    HistoricalRangeBatchStatus.RUNNING: frozenset(
        {
            HistoricalRangeBatchStatus.PARTIAL,
            HistoricalRangeBatchStatus.WAITING_INPUT,
            HistoricalRangeBatchStatus.COMPLETED,
            HistoricalRangeBatchStatus.FAILED,
            HistoricalRangeBatchStatus.CANCELLING,
        }
    ),
    HistoricalRangeBatchStatus.PARTIAL: frozenset(
        {
            HistoricalRangeBatchStatus.RUNNING,
            HistoricalRangeBatchStatus.COMPLETED,
            HistoricalRangeBatchStatus.FAILED,
            HistoricalRangeBatchStatus.CANCELLED,
        }
    ),
    HistoricalRangeBatchStatus.WAITING_INPUT: frozenset(
        {HistoricalRangeBatchStatus.RUNNING, HistoricalRangeBatchStatus.FAILED, HistoricalRangeBatchStatus.CANCELLED}
    ),
    HistoricalRangeBatchStatus.CANCELLING: frozenset({HistoricalRangeBatchStatus.CANCELLED}),
    HistoricalRangeBatchStatus.COMPLETED: frozenset(),
    HistoricalRangeBatchStatus.FAILED: frozenset(),
    HistoricalRangeBatchStatus.CANCELLED: frozenset(),
}

PROGRAM_TRANSITIONS: dict[HistoricalRangeProgramStatus, frozenset[HistoricalRangeProgramStatus]] = {
    HistoricalRangeProgramStatus.QUEUED: frozenset(
        {HistoricalRangeProgramStatus.RUNNING, HistoricalRangeProgramStatus.CANCELLED}
    ),
    HistoricalRangeProgramStatus.RUNNING: frozenset(
        {
            HistoricalRangeProgramStatus.WAITING_INPUT,
            HistoricalRangeProgramStatus.RETRYABLE_FAILED,
            HistoricalRangeProgramStatus.PARTIAL,
            HistoricalRangeProgramStatus.COMPLETED,
            HistoricalRangeProgramStatus.FAILED,
            HistoricalRangeProgramStatus.CANCELLED,
        }
    ),
    HistoricalRangeProgramStatus.WAITING_INPUT: frozenset(
        {HistoricalRangeProgramStatus.RUNNING, HistoricalRangeProgramStatus.CANCELLED}
    ),
    HistoricalRangeProgramStatus.RETRYABLE_FAILED: frozenset(
        {HistoricalRangeProgramStatus.RUNNING, HistoricalRangeProgramStatus.CANCELLED}
    ),
    HistoricalRangeProgramStatus.PARTIAL: frozenset(
        {HistoricalRangeProgramStatus.RUNNING, HistoricalRangeProgramStatus.CANCELLED}
    ),
    HistoricalRangeProgramStatus.COMPLETED: frozenset(),
    HistoricalRangeProgramStatus.FAILED: frozenset(),
    HistoricalRangeProgramStatus.CANCELLED: frozenset(),
}

DAY_TRANSITIONS: dict[HistoricalRangeDayStatus, frozenset[HistoricalRangeDayStatus]] = {
    HistoricalRangeDayStatus.PENDING: frozenset(
        {HistoricalRangeDayStatus.WAITING_PREVIOUS_DAY, HistoricalRangeDayStatus.CANCELLED}
    ),
    HistoricalRangeDayStatus.WAITING_PREVIOUS_DAY: frozenset(
        {HistoricalRangeDayStatus.RUNNING, HistoricalRangeDayStatus.CANCELLED}
    ),
    HistoricalRangeDayStatus.RUNNING: frozenset(
        {
            HistoricalRangeDayStatus.COMPLETE,
            HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
            HistoricalRangeDayStatus.WAITING_INPUT,
            HistoricalRangeDayStatus.RETRYABLE_FAILED,
            HistoricalRangeDayStatus.FAILED,
            HistoricalRangeDayStatus.CANCELLED,
        }
    ),
    HistoricalRangeDayStatus.WAITING_INPUT: frozenset(
        {HistoricalRangeDayStatus.WAITING_PREVIOUS_DAY, HistoricalRangeDayStatus.CANCELLED}
    ),
    HistoricalRangeDayStatus.RETRYABLE_FAILED: frozenset(
        {HistoricalRangeDayStatus.WAITING_PREVIOUS_DAY, HistoricalRangeDayStatus.CANCELLED}
    ),
    HistoricalRangeDayStatus.COMPLETE: frozenset(),
    HistoricalRangeDayStatus.VALID_NO_CANDIDATE: frozenset(),
    HistoricalRangeDayStatus.FAILED: frozenset(),
    HistoricalRangeDayStatus.CANCELLED: frozenset(),
}

OUTCOME_TRANSITIONS: dict[HistoricalRangeOutcomeStatus, frozenset[HistoricalRangeOutcomeStatus]] = {
    HistoricalRangeOutcomeStatus.NOT_DUE: frozenset(
        {
            HistoricalRangeOutcomeStatus.MATURING,
            HistoricalRangeOutcomeStatus.COMPLETE,
            HistoricalRangeOutcomeStatus.CENSORED,
            HistoricalRangeOutcomeStatus.TERMINAL,
            HistoricalRangeOutcomeStatus.FAILED,
        }
    ),
    HistoricalRangeOutcomeStatus.MATURING: frozenset(
        {
            HistoricalRangeOutcomeStatus.COMPLETE,
            HistoricalRangeOutcomeStatus.CENSORED,
            HistoricalRangeOutcomeStatus.TERMINAL,
            HistoricalRangeOutcomeStatus.FAILED,
        }
    ),
    HistoricalRangeOutcomeStatus.COMPLETE: frozenset(),
    HistoricalRangeOutcomeStatus.CENSORED: frozenset(),
    HistoricalRangeOutcomeStatus.TERMINAL: frozenset(),
    HistoricalRangeOutcomeStatus.FAILED: frozenset(),
}

OPERATION_TRANSITIONS: dict[HistoricalRangeOperationStatus, frozenset[HistoricalRangeOperationStatus]] = {
    HistoricalRangeOperationStatus.QUEUED: frozenset({HistoricalRangeOperationStatus.RUNNING}),
    HistoricalRangeOperationStatus.RUNNING: frozenset(
        {
            HistoricalRangeOperationStatus.COMPLETED,
            HistoricalRangeOperationStatus.RETRYABLE_FAILED,
            HistoricalRangeOperationStatus.FAILED,
        }
    ),
    HistoricalRangeOperationStatus.RETRYABLE_FAILED: frozenset({HistoricalRangeOperationStatus.RUNNING}),
    HistoricalRangeOperationStatus.COMPLETED: frozenset(),
    HistoricalRangeOperationStatus.FAILED: frozenset(),
}


def require_state_transition(
    current: Enum,
    target: Enum,
    transitions: dict[Any, frozenset[Any]],
    *,
    entity: str,
) -> None:
    if target not in transitions[current]:
        raise HistoricalRangeContractError(
            REASON_STATE_TRANSITION_INVALID,
            f"illegal {entity} state transition",
            context={"entity": entity, "current": current.value, "target": target.value},
        )


def require_batch_transition(
    current: HistoricalRangeBatchStatus,
    target: HistoricalRangeBatchStatus,
    *,
    successful_day_count: int,
    program_count: int,
    failed_program_count: int,
    recoverable_program_count: int,
) -> None:
    require_state_transition(current, target, BATCH_TRANSITIONS, entity="batch")
    if target is HistoricalRangeBatchStatus.FAILED and (
        successful_day_count != 0 or failed_program_count != program_count or recoverable_program_count != 0
    ):
        raise HistoricalRangeContractError(
            REASON_STATE_TRANSITION_INVALID,
            "FAILED requires zero successful days and all Programs terminally failed",
            context={
                "successful_day_count": successful_day_count,
                "program_count": program_count,
                "failed_program_count": failed_program_count,
                "recoverable_program_count": recoverable_program_count,
            },
        )


def derive_day_run_id(range_run_id: str, decision_trade_date: date, ordinal: int) -> str:
    if ordinal < 1:
        raise ValueError("ordinal must be positive")
    return derive_prefixed_id(
        "ahrd",
        {
            "range_run_id": _nonblank(range_run_id, field_name="range_run_id"),
            "decision_trade_date": decision_trade_date,
            "ordinal": ordinal,
        },
    )


def derive_episode_id(
    range_run_id: str,
    symbol: str,
    enter_decision_trade_date: date,
    entry_sequence: int,
) -> str:
    if entry_sequence < 1:
        raise ValueError("entry_sequence must be positive")
    return derive_prefixed_id(
        "ahre",
        {
            "range_run_id": _nonblank(range_run_id, field_name="range_run_id"),
            "symbol": _nonblank(symbol, field_name="symbol").upper(),
            "enter_decision_trade_date": enter_decision_trade_date,
            "entry_sequence": entry_sequence,
        },
    )


def derive_outcome_logical_id(
    subject_type: HistoricalRangeOutcomeSubjectType,
    subject_id: str,
    projection: HistoricalRangeOutcomeProjection,
    horizon_trade_days: int,
    label_policy_hash: str,
) -> str:
    if horizon_trade_days < 1:
        raise ValueError("horizon_trade_days must be positive")
    return derive_prefixed_id(
        "ahro",
        {
            "subject_type": subject_type.value,
            "subject_id": _nonblank(subject_id, field_name="subject_id"),
            "projection": projection.value,
            "horizon_trade_days": horizon_trade_days,
            "label_policy_hash": require_sha256(label_policy_hash, field_name="label_policy_hash"),
        },
    )
