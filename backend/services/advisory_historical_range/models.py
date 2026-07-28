"""Typed contracts for Phase 1R historical-range Advisory research.

This module is deliberately free of Selection, Paper, simulation, QMT, QE,
Qlib, package validation, and database imports.  It closes deterministic
request identities, immutable artifact envelopes, persistence facts, and the
approved state machines only.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal, ROUND_HALF_UP
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
PLANNING_ARTIFACT_ENVELOPE_SCHEMA_VERSION = "advisory_historical_range_planning_artifact_envelope_v1"
ARTIFACT_REF_SCHEMA_VERSION = "advisory_historical_range_artifact_ref_v1"
SOURCE_REQUIREMENT_SCHEMA_VERSION = "advisory_historical_range_source_requirement_v1"
SOURCE_REQUIREMENT_PLAN_SCHEMA_VERSION = "advisory_historical_range_source_requirement_plan_v1"
SOURCE_REVISION_MEMBER_SCHEMA_VERSION = "advisory_historical_range_source_revision_member_v1"
SOURCE_REVISION_CATALOG_SCHEMA_VERSION = "advisory_historical_range_source_revision_catalog_v1"
SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION = "advisory_historical_range_source_catalog_checkpoint_v1"
HMM_BINDING_SET_SCHEMA_VERSION = "advisory_historical_range_hmm_binding_set_v1"
CANDIDATE_ARTIFACT_PAYLOAD_SCHEMA_VERSION = "advisory_historical_range_candidate_artifact_payload_v2"
DECISION_MARK_SET_PAYLOAD_SCHEMA_VERSION = "advisory_historical_range_decision_mark_set_v1"
DAY_ATTEMPT_RECEIPT_PAYLOAD_SCHEMA_VERSION = "advisory_historical_range_day_attempt_receipt_v1"
DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2 = "advisory_historical_range_day_receipt_payload_v2"
RUN_EXECUTION_RECEIPT_SCHEMA_VERSION = "advisory_historical_range_run_execution_receipt_v1"
EXECUTION_OPERATION_RECEIPT_SCHEMA_VERSION = "advisory_historical_range_execution_operation_receipt_v1"
EXECUTION_OPERATION_ATTEMPT_RECEIPT_SCHEMA_VERSION = (
    "advisory_historical_range_execution_operation_attempt_receipt_v1"
)
LIST_SUMMARY_SCHEMA_VERSION_V2 = "advisory_historical_range_list_summary_v2"
RULE_GUIDANCE_SCHEMA_VERSION_V2 = "advisory_historical_range_rule_guidance_v2"
EPISODE_MARK_SCHEMA_VERSION_V2 = "advisory_historical_range_episode_mark_v2"
OUTCOME_POLICY_BUNDLE_SCHEMA_VERSION = "advisory_historical_range_outcome_policy_bundle_v1"
OUTCOME_REFRESH_REQUEST_SCHEMA_VERSION = "advisory_historical_range_outcome_refresh_request_v1"
OUTCOME_WORK_ITEM_SCHEMA_VERSION = "advisory_historical_range_outcome_work_item_v1"
OUTCOME_ARTIFACT_SCHEMA_VERSION_V2 = "advisory_historical_range_outcome_artifact_v2"
SUMMARY_POLICY_SCHEMA_VERSION = "advisory_historical_range_summary_policy_v1"
SUMMARY_ARTIFACT_SCHEMA_VERSION_V2 = "advisory_historical_range_summary_artifact_v2"
OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION = "advisory_historical_range_outcome_refresh_receipt_v1"
DATASET_BRIDGE_REQUEST_SCHEMA_VERSION = "advisory_historical_range_dataset_bridge_request_v1"
DATASET_BRIDGE_RECEIPT_SCHEMA_VERSION = "advisory_historical_range_dataset_bridge_receipt_v1"
BACKGROUND_DISPATCH_FAILURE_SCHEMA_VERSION = "advisory_historical_range_background_dispatch_failure_v1"
PHASE0A_LINEAGE_IDENTITY_SCHEMA_VERSION = "advisory_phase1_phase0a_lineage_identity_v1"
HISTORICAL_RANGE_LINEAGE_IDENTITY_SCHEMA_VERSION = "advisory_phase1_historical_range_lineage_identity_v1"

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
REASON_SOURCE_REVISION_MISMATCH = "ADVISORY_HR_SOURCE_REVISION_MISMATCH"
REASON_OUTCOME_NOT_DUE = "ADVISORY_HR_OUTCOME_NOT_DUE"
REASON_OUTCOME_SOURCE_UNAVAILABLE = "ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE"
REASON_OUTCOME_SOURCE_REVISION_CONFLICT = "ADVISORY_HR_OUTCOME_SOURCE_REVISION_CONFLICT"
REASON_OUTCOME_INPUT_CONFLICT = "ADVISORY_HR_OUTCOME_INPUT_CONFLICT"
REASON_OUTCOME_CALCULATION_FAILED = "ADVISORY_HR_OUTCOME_CALCULATION_FAILED"
REASON_OUTCOME_REVISION_INVALID = "ADVISORY_HR_OUTCOME_REVISION_INVALID"
REASON_SUMMARY_OUTCOME_SET_CONFLICT = "ADVISORY_HR_SUMMARY_OUTCOME_SET_CONFLICT"
REASON_SUMMARY_CALCULATION_FAILED = "ADVISORY_HR_SUMMARY_CALCULATION_FAILED"
REASON_DATASET_BRIDGE_VALID_EMPTY = "ADVISORY_HR_DATASET_BRIDGE_VALID_EMPTY"
REASON_DATASET_BRIDGE_LINEAGE_CONFLICT = "ADVISORY_HR_DATASET_BRIDGE_LINEAGE_CONFLICT"
REASON_DATASET_BRIDGE_FORMAL_FALLBACK_FORBIDDEN = "ADVISORY_HR_DATASET_BRIDGE_FORMAL_FALLBACK_FORBIDDEN"
REASON_DATASET_BRIDGE_FAILED = "ADVISORY_HR_DATASET_BRIDGE_FAILED"
REASON_DATABASE_CAPACITY_EXHAUSTED = "ADVISORY_HR_DATABASE_CAPACITY_EXHAUSTED"
REASON_DATABASE_UNAVAILABLE = "ADVISORY_HR_DATABASE_UNAVAILABLE"


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


def _hmm_evidence_date(value: Any, *, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    try:
        return date.fromisoformat(normalized[:10])
    except ValueError as exc:
        raise ValueError(f"{field_name} must contain an ISO date") from exc


def normalize_hmm_binding_metadata(metadata: dict[str, Any], *, decision_trade_date: date) -> dict[str, Any]:
    normalized = dict(metadata)
    required = (
        "model_snapshot_id",
        "signal_preset",
        "model_artifact_sha256",
        "coefficient_sha256",
        "snapshot_trained_at",
        "available_at",
        "training_information_cutoff",
        "as_of_trade_date",
        "effective_trade_date",
        "generation_mode",
        "input_data_max_dates",
    )
    missing = [key for key in required if normalized.get(key) is None or normalized.get(key) == ""]
    if missing:
        raise ValueError(f"HMM binding evidence is incomplete: {missing}")
    for key in ("model_snapshot_id", "signal_preset"):
        normalized[key] = _nonblank(str(normalized[key]), field_name=key)
    for key in ("model_artifact_sha256", "coefficient_sha256"):
        normalized[key] = require_sha256(str(normalized[key]), field_name=key)
    if str(normalized["generation_mode"]).strip() != "EXACT_SNAPSHOT":
        raise ValueError("HMM binding generation_mode must be EXACT_SNAPSHOT")
    normalized["generation_mode"] = "EXACT_SNAPSHOT"
    for key in ("as_of_trade_date", "effective_trade_date"):
        if _hmm_evidence_date(normalized[key], field_name=key) != decision_trade_date:
            raise ValueError(f"HMM binding {key} must equal decision_trade_date")
        normalized[key] = decision_trade_date.isoformat()
    input_dates = normalized["input_data_max_dates"]
    if not isinstance(input_dates, dict) or not input_dates:
        raise ValueError("HMM binding input_data_max_dates must be a non-empty mapping")
    cutoff_dates = [
        _hmm_evidence_date(normalized["snapshot_trained_at"], field_name="snapshot_trained_at"),
        _hmm_evidence_date(normalized["available_at"], field_name="available_at"),
        _hmm_evidence_date(
            normalized["training_information_cutoff"],
            field_name="training_information_cutoff",
        ),
        *(
            _hmm_evidence_date(value, field_name=f"input_data_max_dates.{key}")
            for key, value in input_dates.items()
        ),
    ]
    if any(value > decision_trade_date for value in cutoff_dates):
        raise ValueError("HMM binding contains evidence after decision_trade_date")
    input_hash = canonical_json_sha256(input_dates)
    supplied_input_hash = normalized.get("input_data_max_dates_hash")
    if supplied_input_hash is not None and require_sha256(
        str(supplied_input_hash), field_name="input_data_max_dates_hash"
    ) != input_hash:
        raise ValueError("HMM binding input_data_max_dates_hash differs from its evidence")
    normalized["input_data_max_dates_hash"] = input_hash
    return normalized


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
    PLANNING = "PLANNING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PARTIAL = "PARTIAL"
    WAITING_INPUT = "WAITING_INPUT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    DEDUPLICATED = "DEDUPLICATED"


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
    WAITING_INPUT = "WAITING_INPUT"
    COMPLETED = "COMPLETED"
    RETRYABLE_FAILED = "RETRYABLE_FAILED"
    FAILED = "FAILED"


class HistoricalRangeOperationType(str, Enum):
    CREATE = "CREATE"
    BUILD_SOURCE_CATALOG = "BUILD_SOURCE_CATALOG"
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


class HistoricalRangeEvaluationWindowType(str, Enum):
    FIXED_HORIZON = "FIXED_HORIZON"
    EPISODE_LIFECYCLE = "EPISODE_LIFECYCLE"


class HistoricalRangeOutcomeRevisionReason(str, Enum):
    INITIAL = "INITIAL"
    MATURITY_ADVANCE = "MATURITY_ADVANCE"
    SOURCE_CORRECTION = "SOURCE_CORRECTION"
    CALCULATION_CORRECTION = "CALCULATION_CORRECTION"


class HistoricalRangeBridgeResultStatus(str, Enum):
    SEALED = "SEALED"
    VALID_EMPTY = "VALID_EMPTY"
    RETRYABLE_FAILED = "RETRYABLE_FAILED"
    FAILED = "FAILED"


class HistoricalRangeArtifactKind(str, Enum):
    SOURCE_REQUIREMENT_PLAN = "SOURCE_REQUIREMENT_PLAN"
    SOURCE_CATALOG_CHECKPOINT = "SOURCE_CATALOG_CHECKPOINT"
    HMM_BINDING_SET = "HMM_BINDING_SET"
    REQUEST = "REQUEST"
    DATE_PLAN = "DATE_PLAN"
    FROZEN_PROGRAM = "FROZEN_PROGRAM"
    CANDIDATE_ARTIFACT = "CANDIDATE_ARTIFACT"
    DECISION_MARK_SET = "DECISION_MARK_SET"
    DAY_RECEIPT = "DAY_RECEIPT"
    RANGE_RECEIPT = "RANGE_RECEIPT"
    OUTCOME_REFRESH_RECEIPT = "OUTCOME_REFRESH_RECEIPT"
    DATASET_BRIDGE_RECEIPT = "DATASET_BRIDGE_RECEIPT"
    OUTCOME = "OUTCOME"
    SUMMARY = "SUMMARY"
    DATASET_BRIDGE = "DATASET_BRIDGE"


class HistoricalRangeRequirementPurpose(str, Enum):
    REQUEST_SEAL = "REQUEST_SEAL"
    DAY_EXECUTION = "DAY_EXECUTION"


class HistoricalRangeRevisionAdmissibility(str, Enum):
    FORMAL_EVENT = "FORMAL_EVENT"
    RETROSPECTIVE_DB_CONTENT_HASH = "RETROSPECTIVE_DB_CONTENT_HASH"
    FROZEN_ARTIFACT = "FROZEN_ARTIFACT"


class HistoricalRangeCatalogPhase(str, Enum):
    DISCOVER = "DISCOVER"
    VERIFY = "VERIFY"


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
    required_window: int = Field(ge=1)
    buffer_trading_days: int = Field(ge=0)
    window_resolution: Literal["trading_calendar"] = "trading_calendar"
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
    package_version: str = Field(min_length=1, max_length=80)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: HistoricalRangeAlphaMode
    components: tuple[HistoricalRangeAdmittedComponentV1, ...] = Field(min_length=1)

    @field_validator("package_id", "package_version")
    @classmethod
    def _required_text(cls, value: str, info: Any) -> str:
        return _nonblank(value, field_name=info.field_name)

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


class HistoricalRangeProgramDayWindowV1(_StrictContract):
    decision_trade_date: date
    window_start_trade_date: date

    @model_validator(mode="after")
    def _window_order(self) -> "HistoricalRangeProgramDayWindowV1":
        if self.window_start_trade_date > self.decision_trade_date:
            raise ValueError("window_start_trade_date cannot follow decision_trade_date")
        return self


class HistoricalRangeProgramWarmupComponentV1(_StrictContract):
    component_id: str = Field(min_length=1, max_length=160)
    warmup_start_trade_date: date
    range_start_trade_date: date
    lookback_contract_hash: str = Field(min_length=64, max_length=64)
    day_windows: tuple[HistoricalRangeProgramDayWindowV1, ...] = Field(min_length=1)

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
        ordered = tuple(sorted(self.day_windows, key=lambda item: item.decision_trade_date))
        if len({item.decision_trade_date for item in ordered}) != len(ordered):
            raise ValueError("day_windows must have unique decision_trade_date values")
        if ordered[0].decision_trade_date != self.range_start_trade_date:
            raise ValueError("day_windows must begin on range_start_trade_date")
        if ordered[0].window_start_trade_date != self.warmup_start_trade_date:
            raise ValueError("warmup_start_trade_date must equal the first exact day window")
        object.__setattr__(self, "day_windows", ordered)
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


class HistoricalRangeHMMBindingV1(_StrictContract):
    decision_trade_date: date
    phase0a_hmm_metadata: dict[str, Any]
    source_revision_ref: HistoricalRangeSourceRevisionRefV1
    binding_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("binding_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="binding_hash") if value is not None else None

    @model_validator(mode="after")
    def _close_binding(self) -> "HistoricalRangeHMMBindingV1":
        metadata = normalize_hmm_binding_metadata(
            self.phase0a_hmm_metadata,
            decision_trade_date=self.decision_trade_date,
        )
        object.__setattr__(self, "phase0a_hmm_metadata", metadata)
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"binding_hash"}))
        if self.binding_hash is not None and self.binding_hash != digest:
            raise ValueError("binding_hash does not match HMM binding semantics")
        object.__setattr__(self, "binding_hash", digest)
        return self


class HistoricalRangeHMMBindingSetV1(_StrictContract):
    schema_version: Literal[HMM_BINDING_SET_SCHEMA_VERSION] = HMM_BINDING_SET_SCHEMA_VERSION
    research_program_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    base_runtime_config_hash: str = Field(min_length=64, max_length=64)
    bindings: tuple[HistoricalRangeHMMBindingV1, ...] = Field(min_length=1)
    binding_set_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("research_program_id", "package_id")
    @classmethod
    def _text(cls, value: str, info: Any) -> str:
        return _nonblank(value, field_name=info.field_name)

    @field_validator("base_runtime_config_hash", "binding_set_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_binding_set(self) -> "HistoricalRangeHMMBindingSetV1":
        ordered = tuple(sorted(self.bindings, key=lambda item: item.decision_trade_date))
        if len({item.decision_trade_date for item in ordered}) != len(ordered):
            raise ValueError("HMM binding set must contain one binding per decision day")
        object.__setattr__(self, "bindings", ordered)
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"binding_set_hash"}))
        if self.binding_set_hash is not None and self.binding_set_hash != digest:
            raise ValueError("binding_set_hash does not match HMM binding set semantics")
        object.__setattr__(self, "binding_set_hash", digest)
        return self

    def binding_for_day(self, decision_trade_date: date) -> HistoricalRangeHMMBindingV1:
        matching = [item for item in self.bindings if item.decision_trade_date == decision_trade_date]
        if len(matching) != 1:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
                "sealed HMM binding set does not contain the requested decision day",
                context={
                    "research_program_id": self.research_program_id,
                    "decision_trade_date": decision_trade_date.isoformat(),
                },
            )
        return matching[0]


class HistoricalRangeFrozenProgramV1(_StrictContract):
    schema_version: Literal[FROZEN_PROGRAM_SCHEMA_VERSION] = FROZEN_PROGRAM_SCHEMA_VERSION
    research_program_id: str = Field(min_length=1, max_length=160)
    source_program_id: str | None = Field(default=None, min_length=1, max_length=160)
    source_program_version: int | None = Field(default=None, ge=1)
    source_binding_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    package_version: str = Field(min_length=1, max_length=80)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: HistoricalRangeAlphaMode
    program_config: dict[str, Any]
    program_config_hash: str = Field(min_length=64, max_length=64)
    runtime_config: dict[str, Any]
    runtime_config_hash: str = Field(min_length=64, max_length=64)
    review_policy: dict[str, Any]
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
    resolved_hmm_binding_set_ref: HistoricalRangeArtifactRefV1 | None = None
    resolved_hmm_binding_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    frozen_program_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("research_program_id", "package_id", "package_version", "code_release_id")
    @classmethod
    def _required_text(cls, value: str, info: Any) -> str:
        return _nonblank(value, field_name=info.field_name)

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
        "resolved_hmm_binding_set_hash",
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
        if (self.resolved_hmm_binding_set_ref is None) != (self.resolved_hmm_binding_set_hash is None):
            raise ValueError("resolved HMM binding set ref/hash must be supplied together")
        if (
            self.resolved_hmm_binding_set_ref is not None
            and self.resolved_hmm_binding_set_ref.artifact_kind is not HistoricalRangeArtifactKind.HMM_BINDING_SET
        ):
            raise ValueError("resolved_hmm_binding_set_ref must reference HMM_BINDING_SET")
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
        config_hashes = {
            "program_config_hash": canonical_json_sha256(self.program_config),
            "runtime_config_hash": canonical_json_sha256(self.runtime_config),
            "review_policy_hash": canonical_json_sha256(self.review_policy),
        }
        for field_name, expected_hash in config_hashes.items():
            if getattr(self, field_name) != expected_hash:
                raise ValueError(f"{field_name} does not match its frozen configuration")
        digest = canonical_json_sha256(self.semantic_payload())
        if self.frozen_program_hash is not None and self.frozen_program_hash != digest:
            raise ValueError("frozen_program_hash does not match frozen Program semantics")
        object.__setattr__(self, "frozen_program_hash", digest)
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"frozen_program_hash"})

    def without_resolved_hmm_binding(self) -> "HistoricalRangeFrozenProgramV1":
        payload = self.model_dump(mode="json")
        payload["resolved_hmm_binding_set_ref"] = None
        payload["resolved_hmm_binding_set_hash"] = None
        payload["frozen_program_hash"] = None
        return HistoricalRangeFrozenProgramV1.model_validate(payload)


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
            expected_dates = set(ordered)
            if any(
                {window.decision_trade_date for window in component.day_windows} != expected_dates
                for component in warmup.components
            ):
                raise ValueError("every warmup component must close an exact window for every planned trade date")
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


class HistoricalRangeSourceRequirementV1(_StrictContract):
    schema_version: Literal[SOURCE_REQUIREMENT_SCHEMA_VERSION] = SOURCE_REQUIREMENT_SCHEMA_VERSION
    requirement_id: str = Field(min_length=1, max_length=200)
    source_role: str = Field(min_length=1, max_length=120)
    dataset_id: str = Field(min_length=1, max_length=240)
    query_template_id: str = Field(min_length=1, max_length=240)
    query_template_version: str = Field(min_length=1, max_length=80)
    query_template_hash: str = Field(min_length=64, max_length=64)
    parameter_template: dict[str, Any]
    parameter_template_hash: str | None = Field(default=None, min_length=64, max_length=64)
    partition_ref_template: str = Field(min_length=1, max_length=500)
    depends_on_requirement_ids: tuple[str, ...] = ()
    package_id: str | None = Field(default=None, min_length=1, max_length=160)
    component_id: str | None = Field(default=None, min_length=1, max_length=160)
    decision_trade_date: date | None = None
    required_for: HistoricalRangeRequirementPurpose
    missing_reason_code: str = Field(min_length=1, max_length=160)
    requirement_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "requirement_id",
        "source_role",
        "dataset_id",
        "query_template_id",
        "query_template_version",
        "partition_ref_template",
        "package_id",
        "component_id",
        "missing_reason_code",
    )
    @classmethod
    def _text(cls, value: str | None, info: Any) -> str | None:
        return _nonblank(value, field_name=info.field_name) if value is not None else None

    @field_validator("query_template_hash", "parameter_template_hash", "requirement_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_requirement(self) -> "HistoricalRangeSourceRequirementV1":
        dependencies = tuple(
            sorted(_nonblank(item, field_name="depends_on_requirement_id") for item in self.depends_on_requirement_ids)
        )
        if len(dependencies) != len(set(dependencies)):
            raise ValueError("depends_on_requirement_ids must be duplicate-free")
        if self.requirement_id in dependencies:
            raise ValueError("source requirement cannot depend on itself")
        object.__setattr__(self, "depends_on_requirement_ids", dependencies)
        parameter_hash = canonical_json_sha256(self.parameter_template)
        if self.parameter_template_hash is not None and self.parameter_template_hash != parameter_hash:
            raise ValueError("parameter_template_hash does not match parameter_template")
        object.__setattr__(self, "parameter_template_hash", parameter_hash)
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"requirement_hash"}))
        if self.requirement_hash is not None and self.requirement_hash != digest:
            raise ValueError("requirement_hash does not match source requirement semantics")
        object.__setattr__(self, "requirement_hash", digest)
        return self


class HistoricalRangeSourceRequirementPlanV1(_StrictContract):
    schema_version: Literal[SOURCE_REQUIREMENT_PLAN_SCHEMA_VERSION] = SOURCE_REQUIREMENT_PLAN_SCHEMA_VERSION
    request: HistoricalRangeResearchBatchRequestV1
    date_plan: HistoricalRangeDatePlanV1
    frozen_programs: tuple[HistoricalRangeFrozenProgramV1, ...] = Field(min_length=1)
    query_contract_hash: str = Field(min_length=64, max_length=64)
    calendar_identity_hash: str = Field(min_length=64, max_length=64)
    code_release_hash: str = Field(min_length=64, max_length=64)
    requirements: tuple[HistoricalRangeSourceRequirementV1, ...] = Field(min_length=1)
    requirement_plan_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("query_contract_hash", "calendar_identity_hash", "code_release_hash", "requirement_plan_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_plan(self) -> "HistoricalRangeSourceRequirementPlanV1":
        frozen_programs = tuple(sorted(self.frozen_programs, key=lambda item: item.research_program_id))
        if len(frozen_programs) != len(self.request.program_specs):
            raise ValueError("requirement plan frozen Program count differs from request")
        requested_ids = {item.research_program_id for item in self.request.program_specs}
        if {item.research_program_id for item in frozen_programs} != requested_ids:
            raise ValueError("requirement plan frozen Program identities differ from request")
        if set(self.date_plan.per_program_input_warmup_ranges) != requested_ids:
            raise ValueError("requirement plan date plan does not cover every Program")
        if (
            self.request.start_trade_date != self.date_plan.start_trade_date
            or self.request.end_trade_date != self.date_plan.end_trade_date
        ):
            raise ValueError("requirement plan date range differs from request")
        if any(item.code_release_hash != self.code_release_hash for item in frozen_programs):
            raise ValueError("requirement plan Programs do not share the frozen code release")
        selection_identities = {
            (item.selection_semantics_version, item.selection_semantics_hash) for item in frozen_programs
        }
        list_identities = {(item.list_semantics_version, item.list_semantics_hash) for item in frozen_programs}
        if len(selection_identities) != 1 or len(list_identities) != 1:
            raise ValueError("requirement plan Programs must share selection/list semantics")
        ordered_requirements = _topological_requirement_order(self.requirements)
        object.__setattr__(self, "frozen_programs", frozen_programs)
        object.__setattr__(self, "requirements", ordered_requirements)
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"requirement_plan_hash"}))
        if self.requirement_plan_hash is not None and self.requirement_plan_hash != digest:
            raise ValueError("requirement_plan_hash does not match requirement plan semantics")
        object.__setattr__(self, "requirement_plan_hash", digest)
        return self

    @property
    def batch_id(self) -> str:
        return derive_prefixed_id(
            "ahrb",
            {
                "request_id": self.request.request_id,
                "client_idempotency_key": self.request.client_idempotency_key,
                "user_request_semantic_hash": self.request.user_request_semantic_hash,
                "requirement_plan_hash": self.requirement_plan_hash,
            },
        )

    @property
    def planning_identity_hash(self) -> str:
        return canonical_json_sha256(
            {
                "batch_id": self.batch_id,
                "requirement_plan_hash": self.requirement_plan_hash,
            }
        )


class HistoricalRangeSourceRevisionMemberV1(_StrictContract):
    schema_version: Literal[SOURCE_REVISION_MEMBER_SCHEMA_VERSION] = SOURCE_REVISION_MEMBER_SCHEMA_VERSION
    revision_id: str | None = Field(default=None, min_length=1, max_length=240)
    requirement_id: str = Field(min_length=1, max_length=200)
    source_role: str = Field(min_length=1, max_length=120)
    dataset_id: str = Field(min_length=1, max_length=240)
    partition_ref: str = Field(min_length=1, max_length=500)
    package_id: str | None = Field(default=None, min_length=1, max_length=160)
    component_id: str | None = Field(default=None, min_length=1, max_length=160)
    decision_trade_date: date | None = None
    query_template_id: str = Field(min_length=1, max_length=240)
    query_template_version: str = Field(min_length=1, max_length=80)
    query_template_hash: str = Field(min_length=64, max_length=64)
    bound_parameters: dict[str, Any] | None = None
    parameter_hash: str = Field(min_length=64, max_length=64)
    schema_fingerprint: str | None = Field(default=None, min_length=64, max_length=64)
    row_count: int = Field(ge=0)
    content_hash: str = Field(min_length=64, max_length=64)
    availability_event_hash: str | None = Field(default=None, min_length=64, max_length=64)
    admissibility: HistoricalRangeRevisionAdmissibility
    research_only: Literal[True] = True
    observed_at: datetime
    revision_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "revision_id",
        "requirement_id",
        "source_role",
        "dataset_id",
        "partition_ref",
        "package_id",
        "component_id",
        "query_template_id",
        "query_template_version",
    )
    @classmethod
    def _text(cls, value: str | None, info: Any) -> str | None:
        return _nonblank(value, field_name=info.field_name) if value is not None else None

    @field_validator(
        "query_template_hash",
        "parameter_hash",
        "schema_fingerprint",
        "content_hash",
        "availability_event_hash",
        "revision_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("observed_at")
    @classmethod
    def _observed_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="observed_at")

    @model_validator(mode="after")
    def _close_member(self) -> "HistoricalRangeSourceRevisionMemberV1":
        if self.bound_parameters is not None and canonical_json_sha256(self.bound_parameters) != self.parameter_hash:
            raise ValueError("parameter_hash does not close bound_parameters")
        if (
            self.admissibility is HistoricalRangeRevisionAdmissibility.FORMAL_EVENT
            and self.availability_event_hash is None
        ):
            raise ValueError("FORMAL_EVENT source revisions require availability_event_hash")
        if (
            self.admissibility is HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH
            and self.availability_event_hash is not None
        ):
            raise ValueError("retrospective source revisions cannot claim a formal availability event")
        identity_payload = {
            "requirement_id": self.requirement_id,
            "source_role": self.source_role,
            "dataset_id": self.dataset_id,
            "partition_ref": self.partition_ref,
            "package_id": self.package_id,
            "component_id": self.component_id,
            "decision_trade_date": self.decision_trade_date,
            "query_template_id": self.query_template_id,
            "query_template_version": self.query_template_version,
            "parameter_hash": self.parameter_hash,
        }
        revision_id = derive_prefixed_id("ahrsr", identity_payload, digest_chars=48)
        if self.revision_id is not None and self.revision_id != revision_id:
            raise ValueError("revision_id does not match bound source identity")
        object.__setattr__(self, "revision_id", revision_id)
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"observed_at", "revision_hash"}))
        if self.revision_hash is not None and self.revision_hash != digest:
            raise ValueError("revision_hash does not match source revision semantics")
        object.__setattr__(self, "revision_hash", digest)
        return self


class HistoricalRangeSourceRevisionCatalogV1(_StrictContract):
    schema_version: Literal[SOURCE_REVISION_CATALOG_SCHEMA_VERSION] = SOURCE_REVISION_CATALOG_SCHEMA_VERSION
    requirement_plan_hash: str = Field(min_length=64, max_length=64)
    catalog_generation: int = Field(ge=1)
    query_contract_hash: str = Field(min_length=64, max_length=64)
    calendar_identity_hash: str = Field(min_length=64, max_length=64)
    members: tuple[HistoricalRangeSourceRevisionMemberV1, ...] = Field(min_length=1)
    catalog_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("requirement_plan_hash", "query_contract_hash", "calendar_identity_hash", "catalog_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_catalog(self) -> "HistoricalRangeSourceRevisionCatalogV1":
        members = tuple(sorted(self.members, key=lambda item: str(item.revision_id)))
        revision_ids = tuple(str(item.revision_id) for item in members)
        requirement_ids = tuple(item.requirement_id for item in members)
        if len(revision_ids) != len(set(revision_ids)):
            raise ValueError("catalog revision_id values must be unique")
        if len(requirement_ids) != len(set(requirement_ids)):
            raise ValueError("catalog must resolve each requirement exactly once")
        object.__setattr__(self, "members", members)
        digest = canonical_json_sha256(self.semantic_payload())
        if self.catalog_hash is not None and self.catalog_hash != digest:
            raise ValueError("catalog_hash does not match source revision catalog")
        object.__setattr__(self, "catalog_hash", digest)
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "requirement_plan_hash": self.requirement_plan_hash,
            "catalog_generation": self.catalog_generation,
            "query_contract_hash": self.query_contract_hash,
            "calendar_identity_hash": self.calendar_identity_hash,
            "members": [
                {
                    "requirement_id": member.requirement_id,
                    "revision_id": member.revision_id,
                    "revision_hash": member.revision_hash,
                }
                for member in self.members
            ],
        }

    def source_revision_refs(self) -> tuple[HistoricalRangeSourceRevisionRefV1, ...]:
        return tuple(
            HistoricalRangeSourceRevisionRefV1(
                revision_id=str(member.revision_id),
                revision_hash=str(member.revision_hash),
            )
            for member in self.members
        )


class ResolvedHistoricalRangeRequestV1(_StrictContract):
    schema_version: Literal[RESOLVED_REQUEST_SCHEMA_VERSION] = RESOLVED_REQUEST_SCHEMA_VERSION
    batch_id: str = Field(default="ahrb_artifact", min_length=1, max_length=160, exclude=True)
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
        if self.batch_id != "ahrb_artifact" and not self.batch_id.startswith("ahrb_"):
            raise ValueError("batch_id must be a stable historical-range planning identity")
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

    def range_run_id(self, research_program_id: str) -> str:
        return derive_prefixed_id(
            "ahrr",
            {
                "resolved_request_hash": self.request_payload_sha256,
                "research_program_id": _nonblank(research_program_id, field_name="research_program_id"),
            },
        )


class HistoricalRangeResolvedRequestSnapshotV1(_StrictContract):
    schema_version: Literal["advisory_historical_range_resolved_request_snapshot_v1"] = (
        "advisory_historical_range_resolved_request_snapshot_v1"
    )
    user_request_semantic_hash: str = Field(min_length=64, max_length=64)
    frozen_programs: tuple[HistoricalRangeFrozenProgramV1, ...] = Field(min_length=1)
    date_plan: HistoricalRangeDatePlanV1
    source_revision_catalog_hash: str = Field(min_length=64, max_length=64)
    selection_semantics_version: str = Field(min_length=1, max_length=160)
    selection_semantics_hash: str = Field(min_length=64, max_length=64)
    list_semantics_version: str = Field(min_length=1, max_length=160)
    list_semantics_hash: str = Field(min_length=64, max_length=64)
    resolved_program_set_hash: str = Field(min_length=64, max_length=64)
    request_payload_sha256: str = Field(min_length=64, max_length=64)

    @field_validator(
        "user_request_semantic_hash",
        "source_revision_catalog_hash",
        "selection_semantics_hash",
        "list_semantics_hash",
        "resolved_program_set_hash",
        "request_payload_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _close_snapshot(self) -> "HistoricalRangeResolvedRequestSnapshotV1":
        programs = tuple(sorted(self.frozen_programs, key=lambda item: item.research_program_id))
        program_ids = {item.research_program_id for item in programs}
        if len(program_ids) != len(programs):
            raise ValueError("resolved request snapshot Program identities must be unique")
        if set(self.date_plan.per_program_input_warmup_ranges) != program_ids:
            raise ValueError("resolved request snapshot date plan does not cover every Program")
        for program in programs:
            if (
                program.selection_semantics_version != self.selection_semantics_version
                or program.selection_semantics_hash != self.selection_semantics_hash
                or program.list_semantics_version != self.list_semantics_version
                or program.list_semantics_hash != self.list_semantics_hash
            ):
                raise ValueError("resolved request snapshot Program semantics differ")
        object.__setattr__(self, "frozen_programs", programs)
        expected_program_set_hash = canonical_json_sha256(
            [
                {
                    "research_program_id": item.research_program_id,
                    "frozen_program_hash": item.frozen_program_hash,
                }
                for item in programs
            ]
        )
        if self.resolved_program_set_hash != expected_program_set_hash:
            raise ValueError("resolved request snapshot Program set hash differs")
        if self.request_payload_sha256 != canonical_json_sha256(self.semantic_payload()):
            raise ValueError("resolved request snapshot hash differs from resolved semantics")
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return {
            "schema_version": RESOLVED_REQUEST_SCHEMA_VERSION,
            "user_request_semantic_hash": self.user_request_semantic_hash,
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

    @classmethod
    def from_resolved(
        cls,
        resolved: ResolvedHistoricalRangeRequestV1,
    ) -> "HistoricalRangeResolvedRequestSnapshotV1":
        return cls(
            user_request_semantic_hash=str(resolved.request.user_request_semantic_hash),
            frozen_programs=resolved.frozen_programs,
            date_plan=resolved.date_plan,
            source_revision_catalog_hash=resolved.source_revision_catalog_hash,
            selection_semantics_version=resolved.selection_semantics_version,
            selection_semantics_hash=resolved.selection_semantics_hash,
            list_semantics_version=resolved.list_semantics_version,
            list_semantics_hash=resolved.list_semantics_hash,
            resolved_program_set_hash=str(resolved.resolved_program_set_hash),
            request_payload_sha256=str(resolved.request_payload_sha256),
        )


class HistoricalRangeResolvedRequestArtifactPayloadV1(_StrictContract):
    schema_version: Literal["advisory_historical_range_resolved_request_artifact_payload_v1"] = (
        "advisory_historical_range_resolved_request_artifact_payload_v1"
    )
    resolved_request: HistoricalRangeResolvedRequestSnapshotV1
    source_revision_catalog: HistoricalRangeSourceRevisionCatalogV1

    @field_validator("resolved_request", mode="before")
    @classmethod
    def _snapshot(cls, value: Any) -> Any:
        if isinstance(value, ResolvedHistoricalRangeRequestV1):
            return HistoricalRangeResolvedRequestSnapshotV1.from_resolved(value)
        return value

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeResolvedRequestArtifactPayloadV1":
        if self.source_revision_catalog.catalog_hash != self.resolved_request.source_revision_catalog_hash:
            raise ValueError("source revision catalog hash differs from resolved request")
        return self


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


class HistoricalRangeUnresolvedRequirementV1(_StrictContract):
    ordinal: int = Field(ge=1)
    requirement_id: str = Field(min_length=1, max_length=200)
    reason_code: str = Field(min_length=1, max_length=160)
    blocked_by_requirement_ids: tuple[str, ...] = ()
    context: dict[str, Any] = Field(default_factory=dict)

    @field_validator("requirement_id", "reason_code")
    @classmethod
    def _text(cls, value: str, info: Any) -> str:
        return _nonblank(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _dependencies(self) -> "HistoricalRangeUnresolvedRequirementV1":
        dependencies = tuple(
            sorted(_nonblank(item, field_name="blocked_by_requirement_id") for item in self.blocked_by_requirement_ids)
        )
        if len(dependencies) != len(set(dependencies)):
            raise ValueError("blocked_by_requirement_ids must be duplicate-free")
        object.__setattr__(self, "blocked_by_requirement_ids", dependencies)
        return self


class HistoricalRangeCatalogMemberDeltaV1(_StrictContract):
    ordinal: int = Field(ge=1)
    member: HistoricalRangeSourceRevisionMemberV1


class HistoricalRangeSourceCatalogCheckpointV1(_StrictContract):
    schema_version: Literal[SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION] = SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION
    requirement_plan_hash: str = Field(min_length=64, max_length=64)
    catalog_generation: int = Field(ge=1)
    phase: HistoricalRangeCatalogPhase
    ordinal_start: int = Field(ge=1)
    ordinal_end: int = Field(ge=1)
    next_requirement_ordinal: int = Field(ge=1)
    previous_checkpoint_ref: HistoricalRangeArtifactRefV1 | None = None
    previous_checkpoint_hash: str | None = Field(default=None, min_length=64, max_length=64)
    member_delta: tuple[HistoricalRangeCatalogMemberDeltaV1, ...] = ()
    unresolved_requirement_delta: tuple[HistoricalRangeUnresolvedRequirementV1, ...] = ()
    cumulative_resolved_count: int = Field(ge=0)
    cumulative_member_chain_hash: str = Field(min_length=64, max_length=64)
    checkpoint_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "requirement_plan_hash", "previous_checkpoint_hash", "cumulative_member_chain_hash", "checkpoint_hash"
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_checkpoint(self) -> "HistoricalRangeSourceCatalogCheckpointV1":
        if self.ordinal_end < self.ordinal_start:
            raise ValueError("checkpoint ordinal_end cannot precede ordinal_start")
        if (self.previous_checkpoint_ref is None) != (self.previous_checkpoint_hash is None):
            raise ValueError("previous checkpoint ref/hash must be supplied together")
        if self.previous_checkpoint_ref is not None:
            if self.previous_checkpoint_ref.artifact_kind is not HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT:
                raise ValueError("previous_checkpoint_ref must reference SOURCE_CATALOG_CHECKPOINT")
            if self.previous_checkpoint_ref.semantic_content_hash != self.previous_checkpoint_hash:
                raise ValueError("previous checkpoint ref/hash identity differs")
        members = tuple(sorted(self.member_delta, key=lambda item: item.ordinal))
        unresolved = tuple(sorted(self.unresolved_requirement_delta, key=lambda item: item.ordinal))
        member_requirement_ids = {item.member.requirement_id for item in members}
        unresolved_requirement_ids = {item.requirement_id for item in unresolved}
        if len(member_requirement_ids) != len(members):
            raise ValueError("checkpoint member_delta contains duplicate requirements")
        if len(unresolved_requirement_ids) != len(unresolved):
            raise ValueError("checkpoint unresolved delta contains duplicate requirements")
        if member_requirement_ids & unresolved_requirement_ids:
            raise ValueError("checkpoint requirement cannot be both resolved and unresolved")
        ordinals = [item.ordinal for item in members] + [item.ordinal for item in unresolved]
        if len(ordinals) != len(set(ordinals)):
            raise ValueError("checkpoint delta ordinals must be unique")
        if any(ordinal < self.ordinal_start or ordinal > self.ordinal_end for ordinal in ordinals):
            raise ValueError("checkpoint delta ordinal is outside its declared range")
        if len(members) + len(unresolved) != self.ordinal_end - self.ordinal_start + 1:
            raise ValueError("checkpoint delta count must exactly cover its ordinal range")
        expected_next = min((item.ordinal for item in unresolved), default=self.ordinal_end + 1)
        if self.next_requirement_ordinal != expected_next:
            raise ValueError("next_requirement_ordinal must stop at the first unresolved requirement")
        object.__setattr__(self, "member_delta", members)
        object.__setattr__(self, "unresolved_requirement_delta", unresolved)
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"checkpoint_hash"}))
        if self.checkpoint_hash is not None and self.checkpoint_hash != digest:
            raise ValueError("checkpoint_hash does not match checkpoint delta")
        object.__setattr__(self, "checkpoint_hash", digest)
        return self


class HistoricalRangePlanningArtifactEnvelopeV1(_StrictContract):
    schema_version: Literal[PLANNING_ARTIFACT_ENVELOPE_SCHEMA_VERSION] = PLANNING_ARTIFACT_ENVELOPE_SCHEMA_VERSION
    artifact_kind: Literal[
        HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN,
        HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
        HistoricalRangeArtifactKind.HMM_BINDING_SET,
    ]
    planning_identity_hash: str = Field(min_length=64, max_length=64)
    batch_id: str = Field(min_length=1, max_length=160)
    catalog_generation: int = Field(ge=1)
    producer_contract_version: str = Field(min_length=1, max_length=160)
    payload_schema_version: str = Field(min_length=1, max_length=160)
    payload: dict[str, Any]
    payload_sha256: str | None = Field(default=None, min_length=64, max_length=64)
    semantic_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("planning_identity_hash", "payload_sha256", "semantic_content_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_envelope(self) -> "HistoricalRangePlanningArtifactEnvelopeV1":
        if not self.batch_id.startswith("ahrb_"):
            raise ValueError("planning artifact batch_id must use the historical-range identity")
        expected_schema = {
            HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN: SOURCE_REQUIREMENT_PLAN_SCHEMA_VERSION,
            HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT: SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION,
            HistoricalRangeArtifactKind.HMM_BINDING_SET: HMM_BINDING_SET_SCHEMA_VERSION,
        }[self.artifact_kind]
        if self.payload_schema_version != expected_schema:
            raise ValueError("planning artifact payload schema does not match artifact kind")
        payload_hash = canonical_json_sha256(self.payload)
        if self.payload_sha256 is not None and self.payload_sha256 != payload_hash:
            raise ValueError("payload_sha256 does not match planning payload")
        object.__setattr__(self, "payload_sha256", payload_hash)
        digest = canonical_json_sha256(self.semantic_payload())
        if self.semantic_content_hash is not None and self.semantic_content_hash != digest:
            raise ValueError("semantic_content_hash does not match planning artifact envelope")
        object.__setattr__(self, "semantic_content_hash", digest)
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"semantic_content_hash"})


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
        if self.artifact_kind in {
            HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN,
            HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
            HistoricalRangeArtifactKind.HMM_BINDING_SET,
        }:
            raise ValueError("planning artifacts require HistoricalRangePlanningArtifactEnvelopeV1")
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
        }
        day_kinds = {
            HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
            HistoricalRangeArtifactKind.DECISION_MARK_SET,
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
        elif self.artifact_kind in {
            HistoricalRangeArtifactKind.DATASET_BRIDGE,
            HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
            HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT,
        }:
            if self.day_run_id is not None:
                raise ValueError("batch-derived artifact cannot carry day_run_id")
        elif self.artifact_kind in day_kinds and (self.range_run_id is None or self.day_run_id is None):
            raise ValueError("day artifact requires range_run_id and day_run_id")
        if self.artifact_kind is HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT and not source_refs:
            raise ValueError("candidate artifacts require explicit source revision lineage")
        if (
            self.artifact_kind is HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT
            and self.payload_schema_version != CANDIDATE_ARTIFACT_PAYLOAD_SCHEMA_VERSION
        ):
            raise ValueError("candidate artifacts require the v2 evidence payload")
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


class HistoricalRangeClaimedDayV1(_StrictContract):
    """Durable ownership of one ordered R3 day attempt."""

    batch_id: str = Field(min_length=1, max_length=160)
    range_run_id: str = Field(min_length=1, max_length=160)
    research_program_id: str = Field(min_length=1, max_length=160)
    day_run_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    ordinal: int = Field(ge=1)
    row_version: int = Field(ge=1)
    attempt_no: int = Field(ge=1)
    worker_id: str = Field(min_length=1, max_length=160)
    lease_token: str = Field(min_length=1, max_length=200)
    fencing_token: int = Field(ge=1)
    lease_expires_at: datetime
    resolved_request_hash: str = Field(min_length=64, max_length=64)
    request_ref: HistoricalRangeArtifactRefV1
    list_semantics_version: str = Field(min_length=1, max_length=160)
    list_semantics_hash: str = Field(min_length=64, max_length=64)
    previous_day_run_id: str | None = Field(default=None, min_length=1, max_length=160)
    previous_day_receipt_ref: HistoricalRangeArtifactRefV1 | None = None
    previous_list_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    previous_list_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("resolved_request_hash", "list_semantics_hash", "previous_list_hash")
    @classmethod
    def _claim_hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("lease_expires_at")
    @classmethod
    def _lease_expiry(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="lease_expires_at")

    @model_validator(mode="after")
    def _claim_predecessor(self) -> "HistoricalRangeClaimedDayV1":
        if self.request_ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST:
            raise ValueError("claimed day request_ref must be REQUEST")
        first = self.ordinal == 1
        present = (
            self.previous_day_run_id,
            self.previous_day_receipt_ref,
            self.previous_list_version_id,
            self.previous_list_hash,
        )
        if first and any(item is not None for item in present):
            raise ValueError("first claimed day cannot carry predecessor state")
        if not first and any(item is None for item in present):
            raise ValueError("non-first claimed day requires exact predecessor state")
        if (
            self.previous_day_receipt_ref is not None
            and self.previous_day_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT
        ):
            raise ValueError("claimed day predecessor receipt must be DAY_RECEIPT")
        return self


class HistoricalRangeExecutionBatchV1(_StrictContract):
    batch_id: str = Field(min_length=1, max_length=160)
    status: HistoricalRangeBatchStatus
    row_version: int = Field(ge=1)
    resolved_request_hash: str = Field(min_length=64, max_length=64)
    request_ref: HistoricalRangeArtifactRefV1
    date_plan_ref: HistoricalRangeArtifactRefV1
    artifact_root_identity_hash: str = Field(min_length=64, max_length=64)

    @field_validator("resolved_request_hash", "artifact_root_identity_hash")
    @classmethod
    def _execution_batch_hashes(cls, value: str) -> str:
        return require_sha256(value, field_name="execution_batch_hash")

    @model_validator(mode="after")
    def _execution_batch_refs(self) -> "HistoricalRangeExecutionBatchV1":
        if self.request_ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST:
            raise ValueError("execution batch request_ref must be REQUEST")
        if self.date_plan_ref.artifact_kind is not HistoricalRangeArtifactKind.DATE_PLAN:
            raise ValueError("execution batch date_plan_ref must be DATE_PLAN")
        return self


class HistoricalRangeExecutionRunV1(_StrictContract):
    batch_id: str = Field(min_length=1, max_length=160)
    range_run_id: str = Field(min_length=1, max_length=160)
    research_program_id: str = Field(min_length=1, max_length=160)
    status: HistoricalRangeProgramStatus
    row_version: int = Field(ge=1)
    materialized_day_count: int = Field(ge=0)
    day_plan_cursor_ordinal: int = Field(ge=0)
    final_receipt_ref: HistoricalRangeArtifactRefV1 | None = None
    final_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("final_receipt_hash")
    @classmethod
    def _execution_run_hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="final_receipt_hash") if value is not None else None

    @model_validator(mode="after")
    def _execution_run_receipt(self) -> "HistoricalRangeExecutionRunV1":
        if (self.final_receipt_ref is None) != (self.final_receipt_hash is None):
            raise ValueError("execution run final receipt ref/hash must be supplied together")
        if self.final_receipt_ref is not None:
            if self.final_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.RANGE_RECEIPT:
                raise ValueError("execution run final receipt must be RANGE_RECEIPT")
            if self.final_receipt_ref.semantic_content_hash != self.final_receipt_hash:
                raise ValueError("execution run final receipt hash differs from the ref")
        return self


class HistoricalRangePredecessorStateV1(_StrictContract):
    day_run_id: str = Field(min_length=1, max_length=160)
    list_version: HistoricalRangeListVersionFactV1 | None = None
    active_episodes: tuple[HistoricalRangeEpisodeSnapshotFactV1, ...] = ()
    day_receipt_ref: HistoricalRangeArtifactRefV1 | None = None

    @model_validator(mode="after")
    def _predecessor_state(self) -> "HistoricalRangePredecessorStateV1":
        if self.list_version is None:
            if self.active_episodes or self.day_receipt_ref is not None:
                raise ValueError("empty predecessor state cannot carry list evidence")
            return self
        if self.day_receipt_ref is None or self.day_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT:
            raise ValueError("predecessor list state requires a DAY_RECEIPT ref")
        if any(item.list_version_id != self.list_version.list_version_id for item in self.active_episodes):
            raise ValueError("predecessor active episode belongs to a different list version")
        if any(item.recommendation_state not in {"ACTIVE", "ACTIVE_AT_RANGE_END"} for item in self.active_episodes):
            raise ValueError("predecessor state can expose only active episode snapshots")
        return self


class HistoricalRangeOperationRequestV1(_StrictContract):
    operation_id: str = Field(min_length=1, max_length=160)
    batch_id: str = Field(min_length=1, max_length=160)
    operation_type: HistoricalRangeOperationType
    operation_idempotency_key: str = Field(min_length=1, max_length=200)
    request_payload_sha256: str | None = Field(default=None, min_length=64, max_length=64)
    planning_identity_hash: str | None = Field(default=None, min_length=64, max_length=64)
    expected_row_version: int | None = Field(default=None, ge=1)

    @field_validator("request_payload_sha256", "planning_identity_hash")
    @classmethod
    def _hash(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeOperationRequestV1":
        planning_types = {
            HistoricalRangeOperationType.CREATE,
            HistoricalRangeOperationType.BUILD_SOURCE_CATALOG,
        }
        if self.operation_type in planning_types:
            if self.planning_identity_hash is None or self.request_payload_sha256 is not None:
                raise ValueError("planning operations require planning_identity_hash and forbid sealed request hash")
        elif self.request_payload_sha256 is None:
            raise ValueError("sealed operations require request_payload_sha256")
        return self


class HistoricalRangeBackgroundDispatchFailureV1(_StrictContract):
    """Durable evidence for a background command that failed before domain claim."""

    schema_version: Literal["advisory_historical_range_background_dispatch_failure_v1"] = (
        BACKGROUND_DISPATCH_FAILURE_SCHEMA_VERSION
    )
    operation_id: str = Field(min_length=1, max_length=160)
    batch_id: str = Field(min_length=1, max_length=160)
    command: str = Field(min_length=1, max_length=80)
    stage: Literal["RUNTIME_RECONSTRUCTION", "REQUEST_RECONSTRUCTION", "CLAIM_AND_EXECUTION"]
    reason_code: str = Field(min_length=1, max_length=200)
    error_type: str = Field(min_length=1, max_length=200)
    retryable: Literal[True] = True
    recorded_at: datetime

    @field_validator("recorded_at")
    @classmethod
    def _recorded_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="recorded_at")


class HistoricalRangePlanningArtifactBindingsV1(_StrictContract):
    requirement_plan_ref: HistoricalRangeArtifactRefV1
    artifact_root_identity_hash: str = Field(min_length=64, max_length=64)

    @field_validator("artifact_root_identity_hash")
    @classmethod
    def _root_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="artifact_root_identity_hash")

    @model_validator(mode="after")
    def _kind(self) -> "HistoricalRangePlanningArtifactBindingsV1":
        if self.requirement_plan_ref.artifact_kind is not HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN:
            raise ValueError("requirement_plan_ref must reference SOURCE_REQUIREMENT_PLAN")
        return self


class HistoricalRangeOperationAttemptV1(_StrictContract):
    attempt_id: str = Field(min_length=1, max_length=160)
    operation_id: str = Field(min_length=1, max_length=160)
    attempt_no: int = Field(ge=1)
    worker_id: str = Field(min_length=1, max_length=160)
    lease_token: str = Field(min_length=1, max_length=200)
    fencing_token: int = Field(ge=1)
    status: Literal["RUNNING", "WAITING_INPUT", "COMPLETED", "RETRYABLE_FAILED", "FAILED"]
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
        if self.attempt_receipt_ref is not None and self.attempt_receipt_ref.artifact_kind not in {
            HistoricalRangeArtifactKind.RANGE_RECEIPT,
            HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN,
            HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
            HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
            HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT,
        }:
            raise ValueError(
                "operation attempt receipt kind is not valid for a historical-range operation"
            )
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

    @field_validator(
        "alpha_raw_score",
        "hmm_adjusted_score",
        "risk_policy_adjusted_score",
        "selection_effective_score",
        "advisory_model_score",
    )
    @classmethod
    def _persisted_score_precision(cls, value: Decimal | None) -> Decimal | None:
        if value is None:
            return None
        return value.quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP)

    @field_validator("component_lineage_hash", "candidate_content_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _lineage_hash(self) -> "HistoricalRangeCandidateFactV1":
        expected_candidate_id = derive_prefixed_id(
            "ahc",
            {"day_run_id": self.day_run_id, "symbol": self.symbol},
        )
        if self.candidate_id != expected_candidate_id:
            raise ValueError("candidate_id does not match day/symbol identity")
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


class HistoricalRangeCandidateArtifactPayloadV2(_StrictContract):
    schema_version: Literal[CANDIDATE_ARTIFACT_PAYLOAD_SCHEMA_VERSION] = CANDIDATE_ARTIFACT_PAYLOAD_SCHEMA_VERSION
    range_run_id: str = Field(min_length=1, max_length=160)
    day_run_id: str = Field(min_length=1, max_length=160)
    research_program_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    candidate_input_hash: str = Field(min_length=64, max_length=64)
    package_id: str = Field(min_length=1, max_length=160)
    package_version: str = Field(min_length=1, max_length=80)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: HistoricalRangeAlphaMode
    runtime_profile_hash: str = Field(min_length=64, max_length=64)
    selection_semantics_hash: str = Field(min_length=64, max_length=64)
    code_release_hash: str = Field(min_length=64, max_length=64)
    calendar_identity_hash: str = Field(min_length=64, max_length=64)
    universe_identity_hash: str = Field(min_length=64, max_length=64)
    universe_count: int = Field(ge=1)
    raw_signal_identity_hash: str = Field(min_length=64, max_length=64)
    raw_signal_semantic_header: dict[str, Any]
    raw_inference_receipt: dict[str, Any]
    source_read_receipt_hashes: tuple[str, ...] = Field(min_length=1)
    stage_trace: dict[str, Any]
    stage_closure_hash: str | None = Field(default=None, min_length=64, max_length=64)
    candidate_outcome: Literal["CANDIDATES_AVAILABLE", "VALID_NO_CANDIDATE"]
    no_candidate_reason_codes: tuple[str, ...] = ()
    source_revision_refs: tuple[HistoricalRangeSourceRevisionRefV1, ...] = Field(min_length=1)
    candidates: tuple[HistoricalRangeCandidateFactV1, ...] = ()

    @field_validator("range_run_id", "day_run_id", "research_program_id", "package_id", "package_version")
    @classmethod
    def _text(cls, value: str, info: Any) -> str:
        return _nonblank(value, field_name=info.field_name)

    @field_validator(
        "candidate_input_hash",
        "manifest_sha256",
        "runtime_profile_hash",
        "selection_semantics_hash",
        "code_release_hash",
        "calendar_identity_hash",
        "universe_identity_hash",
        "raw_signal_identity_hash",
        "stage_closure_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_payload(self) -> "HistoricalRangeCandidateArtifactPayloadV2":
        if not self.raw_signal_semantic_header:
            raise ValueError("candidate artifact requires raw_signal_semantic_header")
        if not self.raw_inference_receipt:
            raise ValueError("candidate artifact requires raw_inference_receipt")
        expected_header = {
            "runtime_profile_hash": self.runtime_profile_hash,
            "selection_semantics_hash": self.selection_semantics_hash,
            "code_release_hash": self.code_release_hash,
            "calendar_identity_hash": self.calendar_identity_hash,
            "universe_identity_hash": self.universe_identity_hash,
        }
        header_mismatches = {
            key: {"expected": value, "actual": self.raw_signal_semantic_header.get(key)}
            for key, value in expected_header.items()
            if self.raw_signal_semantic_header.get(key) != value
        }
        if header_mismatches:
            raise ValueError(f"raw signal semantic header differs from candidate identity: {header_mismatches}")
        if canonical_json_sha256(self.raw_signal_semantic_header) != self.raw_signal_identity_hash:
            raise ValueError("raw_signal_identity_hash does not close raw_signal_semantic_header")
        raw_status = self.raw_inference_receipt.get("status")
        raw_score_count = self.raw_inference_receipt.get("score_count")
        if raw_status != "COMPLETE" or not isinstance(raw_score_count, int) or isinstance(raw_score_count, bool):
            raise ValueError("candidate artifact requires a complete raw inference score receipt")
        if raw_score_count < 0:
            raise ValueError("raw inference score_count cannot be negative")
        if not self.stage_trace:
            raise ValueError("candidate artifact requires a complete stage_trace")
        required_stages = {
            "alpha_raw",
            "hmm_adjusted",
            "risk_policy_adjusted",
            "selection_effective",
        }
        missing_stages = sorted(required_stages - set(self.stage_trace))
        if missing_stages:
            raise ValueError(f"candidate artifact stage_trace is incomplete: {missing_stages}")
        for stage_name in required_stages:
            receipt = self.stage_trace[stage_name]
            if not isinstance(receipt, dict) or receipt.get("stage") != stage_name:
                raise ValueError(f"candidate artifact {stage_name} receipt identity is invalid")
            for count_field in ("input_count", "output_count", "excluded_count"):
                count = receipt.get(count_field)
                if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                    raise ValueError(f"candidate artifact {stage_name}.{count_field} is invalid")
            status = receipt.get("status")
            if status == "COMPLETE" and receipt["input_count"] != receipt["output_count"] + receipt["excluded_count"]:
                raise ValueError(f"candidate artifact {stage_name} counts do not close")
            if status == "NOT_APPLICABLE" and (receipt["output_count"] or receipt["excluded_count"]):
                raise ValueError(f"candidate artifact {stage_name} not-applicable receipt has output")
            if status not in {"COMPLETE", "NOT_APPLICABLE"}:
                raise ValueError(f"candidate artifact {stage_name} is not successful")
        ordered_stages = ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")
        if self.stage_trace["alpha_raw"]["input_count"] != raw_score_count:
            raise ValueError("alpha_raw input_count differs from the raw inference score_count")
        for previous, current in zip(ordered_stages, ordered_stages[1:]):
            previous_receipt = self.stage_trace[previous]
            effective_output_count = (
                previous_receipt["input_count"]
                if previous_receipt["status"] == "NOT_APPLICABLE"
                else previous_receipt["output_count"]
            )
            if effective_output_count != self.stage_trace[current]["input_count"]:
                raise ValueError(f"candidate stage count chain breaks between {previous} and {current}")
        receipt_hashes = tuple(
            sorted(
                require_sha256(item, field_name="source_read_receipt_hash") for item in self.source_read_receipt_hashes
            )
        )
        if len(receipt_hashes) != len(set(receipt_hashes)):
            raise ValueError("source_read_receipt_hashes must be duplicate-free")
        source_refs = tuple(sorted(self.source_revision_refs, key=lambda item: (item.revision_id, item.revision_hash)))
        if len(source_refs) != len({item.revision_id for item in source_refs}):
            raise ValueError("source_revision_refs must have unique revision_id values")
        candidates = tuple(sorted(self.candidates, key=lambda item: (item.symbol, item.candidate_id)))
        if len(candidates) != len({item.symbol for item in candidates}):
            raise ValueError("candidate artifact symbols must be unique")
        if any(item.day_run_id != self.day_run_id for item in candidates):
            raise ValueError("candidate artifact contains facts for another day")
        if any(item.advisory_model_rank is not None or item.advisory_model_score is not None for item in candidates):
            raise ValueError("R2-B candidate artifact cannot invent advisory model scores")
        included_count = sum(item.membership_status == "INCLUDED" for item in candidates)
        if len(candidates) != raw_score_count:
            raise ValueError("candidate facts do not cover every raw inference score")
        if self.stage_trace["selection_effective"]["output_count"] != included_count:
            raise ValueError("selection_effective output_count differs from included candidate facts")
        reasons = tuple(
            sorted(_nonblank(item, field_name="no_candidate_reason_code") for item in self.no_candidate_reason_codes)
        )
        if len(reasons) != len(set(reasons)):
            raise ValueError("no_candidate_reason_codes must be duplicate-free")
        if self.candidate_outcome == "CANDIDATES_AVAILABLE":
            if included_count == 0 or reasons:
                raise ValueError("CANDIDATES_AVAILABLE requires included facts and no empty-result reasons")
        elif included_count != 0 or not reasons:
            raise ValueError("VALID_NO_CANDIDATE requires zero included facts and explicit reasons")
        elif raw_score_count > 0 and any(item.membership_status != "EXCLUDED" for item in candidates):
            raise ValueError("filtered VALID_NO_CANDIDATE requires every raw candidate to be excluded")
        stage_hash = canonical_json_sha256(self.stage_trace)
        if self.stage_closure_hash is not None and self.stage_closure_hash != stage_hash:
            raise ValueError("stage_closure_hash does not match stage_trace")
        object.__setattr__(self, "source_read_receipt_hashes", receipt_hashes)
        object.__setattr__(self, "source_revision_refs", source_refs)
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "no_candidate_reason_codes", reasons)
        object.__setattr__(self, "stage_closure_hash", stage_hash)
        return self


class HistoricalRangeCandidateProductionResultV1(_StrictContract):
    schema_version: Literal["advisory_historical_range_candidate_production_result_v1"] = (
        "advisory_historical_range_candidate_production_result_v1"
    )
    research_program_id: str = Field(min_length=1, max_length=160)
    range_run_id: str = Field(min_length=1, max_length=160)
    day_run_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    candidate_input_hash: str = Field(min_length=64, max_length=64)
    candidate_outcome: Literal["CANDIDATES_AVAILABLE", "VALID_NO_CANDIDATE"]
    no_candidate_reason_codes: tuple[str, ...] = ()
    candidates: tuple[HistoricalRangeCandidateFactV1, ...] = ()
    candidate_artifact_ref: HistoricalRangeArtifactRefV1
    stage_trace: dict[str, Any]
    source_revision_refs: tuple[HistoricalRangeSourceRevisionRefV1, ...] = Field(min_length=1)
    raw_signal_identity_hash: str = Field(min_length=64, max_length=64)

    @field_validator("candidate_input_hash", "raw_signal_identity_hash")
    @classmethod
    def _result_hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _close_result(self) -> "HistoricalRangeCandidateProductionResultV1":
        if self.candidate_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT:
            raise ValueError("candidate production result requires a CANDIDATE_ARTIFACT ref")
        candidates = tuple(sorted(self.candidates, key=lambda item: (item.symbol, item.candidate_id)))
        if len(candidates) != len({item.symbol for item in candidates}):
            raise ValueError("candidate production result symbols must be unique")
        refs = tuple(sorted(self.source_revision_refs, key=lambda item: (item.revision_id, item.revision_hash)))
        if len(refs) != len({item.revision_id for item in refs}):
            raise ValueError("candidate production result source refs must be unique")
        reasons = tuple(sorted(_nonblank(item, field_name="reason_code") for item in self.no_candidate_reason_codes))
        if len(reasons) != len(set(reasons)):
            raise ValueError("candidate production result reasons must be unique")
        included_count = sum(item.membership_status == "INCLUDED" for item in candidates)
        if self.candidate_outcome == "CANDIDATES_AVAILABLE" and (included_count == 0 or reasons):
            raise ValueError("CANDIDATES_AVAILABLE result requires included facts and no empty reasons")
        if self.candidate_outcome == "VALID_NO_CANDIDATE" and (included_count != 0 or not reasons):
            raise ValueError("VALID_NO_CANDIDATE result requires reasons and no included facts")
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "source_revision_refs", refs)
        object.__setattr__(self, "no_candidate_reason_codes", reasons)
        return self


class HistoricalRangeExecutionOperationV1(_StrictContract):
    operation_id: str = Field(min_length=1, max_length=160)
    batch_id: str = Field(min_length=1, max_length=160)
    operation_type: Literal["RESUME", "CANCEL"]
    operation_idempotency_key: str = Field(min_length=1, max_length=200)
    idempotency_payload_hash: str = Field(min_length=64, max_length=64)
    resolved_request_hash: str = Field(min_length=64, max_length=64)
    expected_row_version: int = Field(ge=1)
    status: HistoricalRangeOperationStatus
    row_version: int = Field(ge=1)
    attempt_no: int = Field(ge=0)
    worker_id: str | None = Field(default=None, min_length=1, max_length=160)
    lease_token: str | None = Field(default=None, min_length=1, max_length=200)
    lease_expires_at: datetime | None = None
    lease_expired: bool = False
    fencing_token: int | None = Field(default=None, ge=1)
    stable_keyset_cursor_json: dict[str, Any] | None = None
    result_row_version: int | None = Field(default=None, ge=1)
    result_status: str | None = None
    result_ref: HistoricalRangeArtifactRefV1 | None = None

    @field_validator("idempotency_payload_hash", "resolved_request_hash")
    @classmethod
    def _request_hash(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @field_validator("lease_expires_at")
    @classmethod
    def _operation_lease(cls, value: datetime | None) -> datetime | None:
        return _aware_utc(value, field_name="lease_expires_at") if value is not None else None

    @model_validator(mode="after")
    def _running_identity(self) -> "HistoricalRangeExecutionOperationV1":
        lease_fields = (self.worker_id, self.lease_token, self.lease_expires_at)
        if self.status is HistoricalRangeOperationStatus.RUNNING:
            if any(item is None for item in lease_fields) or self.fencing_token is None or self.attempt_no < 1:
                raise ValueError("RUNNING execution operation requires complete lease identity")
            if self.lease_expired and self.lease_expires_at is None:
                raise ValueError("expired execution operation requires lease_expires_at")
        elif any(item is not None for item in lease_fields) or self.lease_expired:
            raise ValueError("non-running execution operation cannot retain lease identity")
        if self.result_ref is not None and self.result_ref.artifact_kind is not HistoricalRangeArtifactKind.RANGE_RECEIPT:
            raise ValueError("execution operation result_ref must be RANGE_RECEIPT")
        return self


class HistoricalRangeExecutionOperationAttemptReceiptV1(_StrictContract):
    schema_version: Literal[EXECUTION_OPERATION_ATTEMPT_RECEIPT_SCHEMA_VERSION] = (
        EXECUTION_OPERATION_ATTEMPT_RECEIPT_SCHEMA_VERSION
    )
    operation_id: str = Field(min_length=1, max_length=160)
    operation_type: Literal["RESUME", "CANCEL"]
    attempt_no: int = Field(ge=1)
    fencing_token: int = Field(ge=1)
    worker_id: str = Field(min_length=1, max_length=160)
    lease_token_hash: str = Field(min_length=64, max_length=64)
    status: Literal["RETRYABLE_FAILED"] = "RETRYABLE_FAILED"
    input_hash: str = Field(min_length=64, max_length=64)
    starting_batch_row_version: int = Field(ge=1)
    stable_cursor: dict[str, Any]
    reason_codes: tuple[str, ...]
    sanitized_error: dict[str, Any]
    lease_expired_at: datetime | None = None

    @field_validator("lease_token_hash", "input_hash")
    @classmethod
    def _attempt_hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @field_validator("lease_expired_at")
    @classmethod
    def _expired_at(cls, value: datetime | None) -> datetime | None:
        return _aware_utc(value, field_name="lease_expired_at") if value is not None else None


class HistoricalRangeDecisionMarkV2(_StrictContract):
    """One T-cutoff recommendation mark, never an execution price."""

    schema_version: Literal["advisory_historical_range_decision_mark_v2"] = "advisory_historical_range_decision_mark_v2"
    symbol: str = Field(min_length=1, max_length=32)
    decision_trade_date: date
    availability: Literal["AVAILABLE", "MARKET_STATE_NO_QUOTE", "DATA_UNAVAILABLE"]
    raw_reference_yuan: Decimal | None = Field(default=None, gt=0)
    adjustment_factor_as_of_t: Decimal | None = Field(default=None, gt=0)
    normalized_reference_mark: Decimal | None = Field(default=None, gt=0)
    mark_quality: Literal["T_CLOSE", "SUSPENDED_CARRY_FORWARD", "TERMINAL_CARRY_FORWARD", "UNAVAILABLE"]
    tradability_status: str = Field(min_length=1, max_length=120)
    source_revision_refs: tuple[HistoricalRangeSourceRevisionRefV1, ...] = Field(min_length=1)
    source_evidence_hash: str = Field(min_length=64, max_length=64)
    fact_effective_at: datetime
    decision_cutoff: datetime
    source_observed_at: datetime
    revision_admissibility: HistoricalRangeRevisionAdmissibility

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return _nonblank(value, field_name="symbol").upper()

    @field_validator("source_evidence_hash")
    @classmethod
    def _source_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="source_evidence_hash")

    @field_validator("fact_effective_at", "decision_cutoff", "source_observed_at")
    @classmethod
    def _timestamps(cls, value: datetime, info: Any) -> datetime:
        return _aware_utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _close_mark(self) -> "HistoricalRangeDecisionMarkV2":
        refs = tuple(sorted(self.source_revision_refs, key=lambda item: (item.revision_id, item.revision_hash)))
        if len(refs) != len({item.revision_id for item in refs}):
            raise ValueError("decision mark source_revision_refs must be unique")
        values = (self.raw_reference_yuan, self.adjustment_factor_as_of_t, self.normalized_reference_mark)
        if self.availability == "AVAILABLE":
            if any(value is None for value in values) or self.mark_quality != "T_CLOSE":
                raise ValueError("available decision marks require a complete T_CLOSE mark")
        elif self.availability == "MARKET_STATE_NO_QUOTE":
            if self.mark_quality not in {"SUSPENDED_CARRY_FORWARD", "TERMINAL_CARRY_FORWARD"}:
                raise ValueError("market-state no-quote marks require a carry-forward quality")
            if any(value is None for value in values):
                raise ValueError("carry-forward decision marks require raw, adjustment, and normalized values")
        elif any(value is not None for value in values) or self.mark_quality != "UNAVAILABLE":
            raise ValueError("unavailable decision marks cannot contain a price")
        if self.raw_reference_yuan is not None and self.adjustment_factor_as_of_t is not None:
            expected = self.raw_reference_yuan * self.adjustment_factor_as_of_t
            if self.normalized_reference_mark != expected:
                raise ValueError("normalized_reference_mark must equal raw_reference_yuan * adjustment_factor_as_of_t")
        if self.fact_effective_at > self.decision_cutoff:
            raise ValueError("decision mark fact_effective_at cannot exceed decision_cutoff")
        object.__setattr__(self, "source_revision_refs", refs)
        return self


class HistoricalRangeDecisionMarkSetV1(_StrictContract):
    schema_version: Literal[DECISION_MARK_SET_PAYLOAD_SCHEMA_VERSION] = DECISION_MARK_SET_PAYLOAD_SCHEMA_VERSION
    range_run_id: str = Field(min_length=1, max_length=160)
    day_run_id: str = Field(min_length=1, max_length=160)
    decision_trade_date: date
    subject_set_hash: str = Field(min_length=64, max_length=64)
    mark_policy_version: str = Field(min_length=1, max_length=160)
    mark_policy_hash: str = Field(min_length=64, max_length=64)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    source_revision_refs: tuple[HistoricalRangeSourceRevisionRefV1, ...] = Field(min_length=1)
    upstream_request_ref: HistoricalRangeArtifactRefV1
    predecessor_day_receipt_ref: HistoricalRangeArtifactRefV1 | None = None
    marks: tuple[HistoricalRangeDecisionMarkV2, ...] = ()
    mark_set_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("subject_set_hash", "mark_policy_hash", "source_revision_set_hash", "mark_set_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _close_set(self) -> "HistoricalRangeDecisionMarkSetV1":
        if self.upstream_request_ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST:
            raise ValueError("decision mark set upstream_request_ref must be REQUEST")
        if (
            self.predecessor_day_receipt_ref is not None
            and self.predecessor_day_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT
        ):
            raise ValueError("decision mark set predecessor must be DAY_RECEIPT")
        refs = tuple(sorted(self.source_revision_refs, key=lambda item: (item.revision_id, item.revision_hash)))
        if len(refs) != len({item.revision_id for item in refs}):
            raise ValueError("decision mark set source refs must be unique")
        marks = tuple(sorted(self.marks, key=lambda item: item.symbol))
        if len(marks) != len({item.symbol for item in marks}):
            raise ValueError("decision mark set symbols must be unique")
        if any(item.decision_trade_date != self.decision_trade_date for item in marks):
            raise ValueError("decision mark set contains a mark for another trade date")
        expected_subject_hash = canonical_json_sha256([item.symbol for item in marks])
        if self.subject_set_hash != expected_subject_hash:
            raise ValueError("decision mark subject_set_hash does not match ordered mark symbols")
        payload = self.model_dump(mode="json", exclude={"mark_set_hash"})
        digest = canonical_json_sha256(payload)
        if self.mark_set_hash is not None and self.mark_set_hash != digest:
            raise ValueError("decision mark set hash does not match typed payload")
        object.__setattr__(self, "source_revision_refs", refs)
        object.__setattr__(self, "marks", marks)
        object.__setattr__(self, "mark_set_hash", digest)
        return self


class HistoricalRangeActiveRankObservationV2(_StrictContract):
    schema_version: Literal["advisory_historical_range_active_rank_observation_v2"] = (
        "advisory_historical_range_active_rank_observation_v2"
    )
    symbol: str = Field(min_length=1, max_length=32)
    classification: Literal[
        "INCLUDED_SELECTION_RANK",
        "EXCLUDED_BY_STAGE",
        "ABSENT_FROM_RAW_SIGNAL",
        "OUTSIDE_PIT_UNIVERSE",
        "VALID_EMPTY_NO_SIGNAL",
    ]
    review_rank: int | None = Field(default=None, ge=1)
    review_score: Decimal | None = None
    increments_weak_confirmation: bool
    evidence_hash: str = Field(min_length=64, max_length=64)
    reason_codes: tuple[str, ...] = ()

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return _nonblank(value, field_name="symbol").upper()

    @field_validator("evidence_hash")
    @classmethod
    def _evidence_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="evidence_hash")

    @model_validator(mode="after")
    def _rank_contract(self) -> "HistoricalRangeActiveRankObservationV2":
        if self.classification == "VALID_EMPTY_NO_SIGNAL":
            if self.review_rank is not None or self.review_score is not None or self.increments_weak_confirmation:
                raise ValueError("valid empty rank observation cannot supply or increment a review rank")
        elif self.review_rank is None:
            raise ValueError("non-empty active rank observation requires review_rank")
        reasons = tuple(sorted(_nonblank(item, field_name="reason_code") for item in self.reason_codes))
        if len(reasons) != len(set(reasons)):
            raise ValueError("rank observation reason_codes must be unique")
        object.__setattr__(self, "reason_codes", reasons)
        return self


class HistoricalRangeRankObservationV2(_StrictContract):
    schema_version: Literal["advisory_historical_range_rank_observation_v2"] = "advisory_historical_range_rank_observation_v2"
    status: Literal["COMPLETE", "VALID_EMPTY_NO_SIGNAL", "DATA_UNAVAILABLE"]
    observed_max_selection_rank: int = Field(ge=0)
    rank_exit_threshold: int = Field(ge=1)
    active_observations: tuple[HistoricalRangeActiveRankObservationV2, ...] = ()
    source_stage_closure_hash: str = Field(min_length=64, max_length=64)
    universe_evidence_hash: str = Field(min_length=64, max_length=64)

    @field_validator("source_stage_closure_hash", "universe_evidence_hash")
    @classmethod
    def _hashes(cls, value: str) -> str:
        return require_sha256(value, field_name="rank observation hash")

    @property
    def synthetic_missing_rank(self) -> int:
        return self.observed_max_selection_rank + 1

    @model_validator(mode="after")
    def _close_observation(self) -> "HistoricalRangeRankObservationV2":
        observations = tuple(sorted(self.active_observations, key=lambda item: item.symbol))
        if len(observations) != len({item.symbol for item in observations}):
            raise ValueError("active rank observations must be unique by symbol")
        if self.status == "VALID_EMPTY_NO_SIGNAL" and self.observed_max_selection_rank != 0:
            raise ValueError("valid empty rank observation requires observed_max_selection_rank=0")
        if self.status == "DATA_UNAVAILABLE" and observations:
            raise ValueError("data unavailable rank observation cannot manufacture active ranks")
        object.__setattr__(self, "active_observations", observations)
        return self


class HistoricalRangeListSummaryV2(_StrictContract):
    schema_version: Literal[LIST_SUMMARY_SCHEMA_VERSION_V2] = LIST_SUMMARY_SCHEMA_VERSION_V2
    candidate_outcome: Literal["CANDIDATES_AVAILABLE", "VALID_NO_CANDIDATE"]
    stage_closure_hash: str = Field(min_length=64, max_length=64)
    enter_count: int = Field(ge=0)
    hold_count: int = Field(ge=0)
    exit_count: int = Field(ge=0)
    watch_count: int = Field(ge=0)
    active_count: int = Field(ge=0)
    overlap_rate: Decimal | None = Field(default=None, ge=0, le=1)
    turnover_rate: Decimal = Field(ge=0)
    replacement_budget_used: int = Field(ge=0)
    replacement_budget_remaining: int = Field(ge=0)
    rank_observation_status: Literal["COMPLETE", "VALID_EMPTY_NO_SIGNAL", "DATA_UNAVAILABLE"]
    observed_max_selection_rank: int = Field(ge=0)
    price_timing_policy: Literal[HISTORICAL_RANGE_PRICE_TIMING_POLICY] = HISTORICAL_RANGE_PRICE_TIMING_POLICY
    mark_policy_version: str = Field(min_length=1, max_length=160)
    mark_policy_hash: str = Field(min_length=64, max_length=64)
    decision_mark_set_ref: HistoricalRangeArtifactRefV1
    previous_list_hash: str | None = Field(default=None, min_length=64, max_length=64)
    previous_day_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)
    guidance_capability: Literal["RULE_DEFAULT"] = "RULE_DEFAULT"

    @model_validator(mode="after")
    def _summary_contract(self) -> "HistoricalRangeListSummaryV2":
        if self.active_count != self.enter_count + self.hold_count:
            raise ValueError("list summary active_count must equal enter_count + hold_count")
        if (self.previous_list_hash is None) != (self.previous_day_receipt_hash is None):
            raise ValueError("summary predecessor list/day hashes must be supplied together")
        if self.decision_mark_set_ref.artifact_kind is not HistoricalRangeArtifactKind.DECISION_MARK_SET:
            raise ValueError("list summary requires a DECISION_MARK_SET ref")
        return self


class HistoricalRangeRuleGuidanceV2(_StrictContract):
    schema_version: Literal[RULE_GUIDANCE_SCHEMA_VERSION_V2] = RULE_GUIDANCE_SCHEMA_VERSION_V2
    action: HistoricalRangeListAction
    intended_execution_trade_date: date | None = None
    intended_execution_basis: str | None = Field(default=None, min_length=1, max_length=80)
    execution_status: Literal["NOT_DUE", "NOT_APPLICABLE"]
    market_state_reason: str | None = Field(default=None, min_length=1, max_length=160)
    requested_execution_basis: str | None = Field(default=None, min_length=1, max_length=80)
    range_end_reason: Literal["NEXT_SESSION_OUTSIDE_FROZEN_DATE_PLAN"] | None = None

    @model_validator(mode="after")
    def _guidance_contract(self) -> "HistoricalRangeRuleGuidanceV2":
        pair = (self.intended_execution_trade_date is None, self.intended_execution_basis is None)
        if pair[0] != pair[1]:
            raise ValueError("guidance intended date/basis must be supplied together")
        if self.action in {HistoricalRangeListAction.HOLD, HistoricalRangeListAction.WATCH}:
            if self.execution_status != "NOT_APPLICABLE" or self.intended_execution_basis is not None:
                raise ValueError("HOLD/WATCH guidance must be NOT_APPLICABLE without intended execution")
        elif self.execution_status != "NOT_DUE":
            raise ValueError("ENTER/EXIT guidance must be NOT_DUE")
        if self.range_end_reason is not None:
            if self.intended_execution_basis is not None or self.requested_execution_basis is None:
                raise ValueError("outside-plan guidance keeps requested basis only")
        return self


class HistoricalRangeEpisodeMarkV2(_StrictContract):
    schema_version: Literal[EPISODE_MARK_SCHEMA_VERSION_V2] = EPISODE_MARK_SCHEMA_VERSION_V2
    recommendation_anchor: Decimal = Field(gt=0)
    current_raw_reference_yuan: Decimal | None = Field(default=None, gt=0)
    current_adjustment_factor: Decimal | None = Field(default=None, gt=0)
    current_normalized_mark: Decimal | None = Field(default=None, gt=0)
    holding_trading_days: int = Field(ge=0)
    runup_bps: Decimal | None = None
    drawdown_bps: Decimal | None = None
    rank_classification: str = Field(min_length=1, max_length=80)
    review_rank: int | None = Field(default=None, ge=1)
    review_score: Decimal | None = None
    weak_rank_confirmation_count: int = Field(ge=0)
    decision_cutoff: datetime
    tradability_status: str = Field(min_length=1, max_length=120)
    mark_quality: Literal["T_CLOSE", "SUSPENDED_CARRY_FORWARD", "TERMINAL_CARRY_FORWARD", "UNAVAILABLE"]
    source_evidence_hash: str = Field(min_length=64, max_length=64)

    @field_validator("decision_cutoff")
    @classmethod
    def _cutoff(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="decision_cutoff")

    @model_validator(mode="after")
    def _mark_contract(self) -> "HistoricalRangeEpisodeMarkV2":
        values = (self.current_raw_reference_yuan, self.current_adjustment_factor, self.current_normalized_mark)
        if self.mark_quality == "UNAVAILABLE":
            if any(value is not None for value in values):
                raise ValueError("unavailable episode mark cannot include price values")
        elif any(value is None for value in values):
            raise ValueError("available episode mark requires raw, adjustment, and normalized values")
        elif self.current_normalized_mark != self.current_raw_reference_yuan * self.current_adjustment_factor:
            raise ValueError("episode normalized mark does not close raw * adjustment")
        return self


class HistoricalRangeDayAttemptReceiptPayloadV1(_StrictContract):
    schema_version: Literal[DAY_ATTEMPT_RECEIPT_PAYLOAD_SCHEMA_VERSION] = DAY_ATTEMPT_RECEIPT_PAYLOAD_SCHEMA_VERSION
    day_run_id: str = Field(min_length=1, max_length=160)
    attempt_no: int = Field(ge=1)
    fencing_token: int = Field(ge=1)
    worker_id: str = Field(min_length=1, max_length=160)
    lease_token_hash: str = Field(min_length=64, max_length=64)
    status: Literal["WAITING_INPUT", "RETRYABLE_FAILED", "FAILED", "CANCELLED"]
    attempt_input_hash: str = Field(min_length=64, max_length=64)
    input_hash_kind: Literal["CLAIM_INPUT", "CANDIDATE_BOUND_INPUT", "DAY_INPUT"]
    candidate_artifact_ref: HistoricalRangeArtifactRefV1 | None = None
    decision_mark_set_ref: HistoricalRangeArtifactRefV1 | None = None
    previous_list_hash: str | None = Field(default=None, min_length=64, max_length=64)
    previous_day_receipt_ref: HistoricalRangeArtifactRefV1 | None = None
    stage: str = Field(min_length=1, max_length=120)
    reason_codes: tuple[str, ...] = ()
    sanitized_error: dict[str, Any] | None = None
    lease_expired_at: datetime | None = None

    @model_validator(mode="after")
    def _receipt_contract(self) -> "HistoricalRangeDayAttemptReceiptPayloadV1":
        candidate_present = self.candidate_artifact_ref is not None
        mark_present = self.decision_mark_set_ref is not None
        if candidate_present and self.candidate_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT:
            raise ValueError("attempt receipt candidate ref must be CANDIDATE_ARTIFACT")
        if mark_present and self.decision_mark_set_ref.artifact_kind is not HistoricalRangeArtifactKind.DECISION_MARK_SET:
            raise ValueError("attempt receipt mark ref must be DECISION_MARK_SET")
        if self.input_hash_kind == "CLAIM_INPUT" and (candidate_present or mark_present):
            raise ValueError("claim-input receipt cannot contain downstream refs")
        if self.input_hash_kind == "CANDIDATE_BOUND_INPUT" and (not candidate_present or mark_present):
            raise ValueError("candidate-bound receipt requires candidate only")
        if self.input_hash_kind == "DAY_INPUT" and (not candidate_present or not mark_present):
            raise ValueError("day-input receipt requires candidate and mark refs")
        if (self.previous_list_hash is None) != (self.previous_day_receipt_ref is None):
            raise ValueError("attempt predecessor list hash/ref must be supplied together")
        if (
            self.previous_day_receipt_ref is not None
            and self.previous_day_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT
        ):
            raise ValueError("attempt predecessor must be DAY_RECEIPT")
        reasons = tuple(sorted(_nonblank(item, field_name="reason_code") for item in self.reason_codes))
        if len(reasons) != len(set(reasons)):
            raise ValueError("attempt receipt reason_codes must be unique")
        object.__setattr__(self, "reason_codes", reasons)
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

    @field_validator("score")
    @classmethod
    def _persisted_score_precision(cls, value: Decimal | None) -> Decimal | None:
        if value is None:
            return None
        return value.quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP)

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


class HistoricalRangePolicyComponentV1(_StrictContract):
    component_role: Literal[
        "CALENDAR",
        "MARKET_DATA",
        "EXECUTION",
        "COST",
        "BENCHMARK",
        "CASH_RETURN",
        "TERMINAL",
        "BARRIER",
        "CORPORATE_ACTION",
    ]
    component_ref: str = Field(min_length=1, max_length=320)
    component_hash: str = Field(min_length=64, max_length=64)

    @field_validator("component_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return require_sha256(value, field_name="component_hash")


class HistoricalRangeOutcomePolicyBundleV1(_StrictContract):
    """Range-native valuation policy with no Phase 0A admission identity."""

    schema_version: Literal[OUTCOME_POLICY_BUNDLE_SCHEMA_VERSION] = OUTCOME_POLICY_BUNDLE_SCHEMA_VERSION
    policy_bundle_id: str | None = Field(default=None, min_length=1, max_length=160)
    policy_bundle_hash: str | None = Field(default=None, min_length=64, max_length=64)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: Literal["single_alpha", "multi_alpha"]
    style_family: str = Field(min_length=1, max_length=160)
    style_resolution_reason: str = Field(min_length=1, max_length=160)
    calendar_version: str = Field(min_length=1, max_length=160)
    calendar_hash: str = Field(min_length=64, max_length=64)
    components: tuple[HistoricalRangePolicyComponentV1, ...] = Field(min_length=9)
    horizons: tuple[int, ...] = Field(min_length=1)
    projections_by_horizon: dict[int, tuple[str, ...]]
    candidate_reference_notional: Decimal = Field(gt=Decimal("0"))
    benchmark_portfolio_notional: Decimal = Field(gt=Decimal("0"))
    gap_1d_enabled: bool = False
    research_scope: Literal["RETROSPECTIVE_RESEARCH_ONLY"] = "RETROSPECTIVE_RESEARCH_ONLY"
    execution_prohibited: Literal[True] = True

    @field_validator("manifest_sha256", "calendar_hash", "policy_bundle_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeOutcomePolicyBundleV1":
        roles = tuple(item.component_role for item in self.components)
        if roles != tuple(sorted(roles)) or len(set(roles)) != len(roles):
            raise ValueError("policy components must be sorted by unique role")
        required_roles = {
            "CALENDAR", "MARKET_DATA", "EXECUTION", "COST", "BENCHMARK",
            "CASH_RETURN", "TERMINAL", "BARRIER", "CORPORATE_ACTION",
        }
        if set(roles) != required_roles:
            raise ValueError("range policy bundle requires the complete Phase 1 component set")
        if self.horizons != tuple(sorted(set(self.horizons))) or any(item < 1 for item in self.horizons):
            raise ValueError("policy horizons must be sorted, unique, and positive")
        if set(self.projections_by_horizon) != set(self.horizons):
            raise ValueError("projections_by_horizon must exactly cover horizons")
        allowed = {
            "RETURN_GROSS", "RETURN_NET_ABSOLUTE", "RETURN_NET_EXCESS",
            "PATH_MFE", "PATH_MAE", "EXECUTABLE_MFE", "EXECUTABLE_MAE",
        }
        for horizon, projections in self.projections_by_horizon.items():
            if not projections or len(set(projections)) != len(projections) or not set(projections) <= allowed:
                raise ValueError(f"horizon {horizon} has invalid or duplicate projections")
        payload = self.model_dump(mode="json", exclude={"policy_bundle_id", "policy_bundle_hash"})
        digest = canonical_json_sha256(payload)
        expected_id = f"ahrpb_{digest[:20]}"
        if self.policy_bundle_hash is not None and self.policy_bundle_hash != digest:
            raise ValueError("policy_bundle_hash does not match canonical range policy")
        if self.policy_bundle_id is not None and self.policy_bundle_id != expected_id:
            raise ValueError("policy_bundle_id does not match canonical range policy")
        object.__setattr__(self, "policy_bundle_hash", digest)
        object.__setattr__(self, "policy_bundle_id", expected_id)
        return self


class HistoricalRangeOutcomeRefreshRequestV1(_StrictContract):
    schema_version: Literal[OUTCOME_REFRESH_REQUEST_SCHEMA_VERSION] = OUTCOME_REFRESH_REQUEST_SCHEMA_VERSION
    batch_id: str = Field(min_length=1, max_length=160)
    range_run_ids: tuple[str, ...] = ()
    label_as_of_trade_date: date
    policy_bundle_ref: HistoricalRangeArtifactRefV1
    policy_bundle_hash: str = Field(min_length=64, max_length=64)
    requested_subject_types: tuple[HistoricalRangeOutcomeSubjectType, ...] = (
        HistoricalRangeOutcomeSubjectType.CANDIDATE,
        HistoricalRangeOutcomeSubjectType.EPISODE,
        HistoricalRangeOutcomeSubjectType.LIST_VERSION,
        HistoricalRangeOutcomeSubjectType.RANGE,
    )
    requested_outcome_logical_ids: tuple[str, ...] = ()
    requested_projections: tuple[HistoricalRangeOutcomeProjection, ...] = (
        HistoricalRangeOutcomeProjection.EXECUTABLE,
        HistoricalRangeOutcomeProjection.RECOMMENDATION,
    )
    horizons: tuple[int, ...] = Field(min_length=1)
    producer_code_hash: str = Field(min_length=64, max_length=64)
    outcome_contract_version: str = Field(min_length=1, max_length=160)
    correction_reason: HistoricalRangeOutcomeRevisionReason | None = None
    correction_evidence_ref: HistoricalRangeArtifactRefV1 | None = None
    operation_idempotency_key: str = Field(min_length=1, max_length=200)
    expected_batch_row_version: int = Field(ge=1)
    max_items_per_slice: int = Field(default=500, ge=1, le=10000)
    max_parallel_runs: int = Field(default=2, ge=1, le=64)
    lease_seconds: int = Field(default=300, ge=30, le=3600)
    request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("policy_bundle_hash", "producer_code_hash", "request_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _request_identity(self) -> "HistoricalRangeOutcomeRefreshRequestV1":
        if (
            self.policy_bundle_ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST
            or self.policy_bundle_ref.payload_sha256 != self.policy_bundle_hash
        ):
            raise ValueError("policy bundle ref/hash pair does not match")
        if self.range_run_ids != tuple(sorted(set(self.range_run_ids))):
            raise ValueError("range_run_ids must be sorted and unique")
        if self.requested_subject_types != tuple(sorted(set(self.requested_subject_types), key=lambda item: item.value)):
            raise ValueError("requested_subject_types must be sorted and unique")
        if self.requested_outcome_logical_ids != tuple(sorted(set(self.requested_outcome_logical_ids))):
            raise ValueError("requested_outcome_logical_ids must be sorted and unique")
        if any(
            not item.strip() or item != item.strip() or len(item) > 160
            for item in self.requested_outcome_logical_ids
        ):
            raise ValueError("requested_outcome_logical_ids contain an invalid identity")
        if self.requested_projections != tuple(sorted(set(self.requested_projections), key=lambda item: item.value)):
            raise ValueError("requested_projections must be sorted and unique")
        if self.horizons != tuple(sorted(set(self.horizons))) or any(item < 1 for item in self.horizons):
            raise ValueError("refresh horizons must be sorted, unique, and positive")
        if (self.correction_reason is None) != (self.correction_evidence_ref is None):
            raise ValueError("outcome correction reason/evidence must be supplied together")
        if self.correction_reason is not None and self.correction_reason not in {
            HistoricalRangeOutcomeRevisionReason.SOURCE_CORRECTION,
            HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
        }:
            raise ValueError("refresh correction reason must be SOURCE or CALCULATION correction")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"request_hash"}))
        if self.request_hash is not None and self.request_hash != digest:
            raise ValueError("request_hash does not match outcome refresh request")
        object.__setattr__(self, "request_hash", digest)
        return self


class HistoricalRangeOutcomeWorkItemV1(_StrictContract):
    schema_version: Literal[OUTCOME_WORK_ITEM_SCHEMA_VERSION] = OUTCOME_WORK_ITEM_SCHEMA_VERSION
    range_run_id: str = Field(min_length=1, max_length=160)
    subject_type: HistoricalRangeOutcomeSubjectType
    subject_id: str = Field(min_length=1, max_length=160)
    subject_ref: HistoricalRangeArtifactRefV1
    policy_bundle_ref: HistoricalRangeArtifactRefV1
    projection: HistoricalRangeOutcomeProjection
    evaluation_window_type: HistoricalRangeEvaluationWindowType
    horizon_trade_days: int = Field(ge=0)
    policy_bundle_hash: str = Field(min_length=64, max_length=64)
    decision_trade_date: date
    intended_entry_trade_date: date | None = None
    earliest_sell_trade_date: date | None = None
    exit_trade_date: date | None = None
    label_as_of_trade_date: date
    source_revision_refs: tuple[HistoricalRangeArtifactRefV1, ...] = Field(min_length=1)
    source_artifact_ref_set_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    producer_code_hash: str = Field(min_length=64, max_length=64)
    outcome_contract_version: str = Field(min_length=1, max_length=160)
    revision_reason: HistoricalRangeOutcomeRevisionReason
    predecessor_outcome_ref: HistoricalRangeArtifactRefV1 | None = None
    revision_evidence_ref: HistoricalRangeArtifactRefV1 | None = None
    outcome_logical_id: str | None = Field(default=None, min_length=1, max_length=160)
    outcome_input_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "policy_bundle_hash", "source_artifact_ref_set_hash", "source_revision_set_hash",
        "producer_code_hash", "outcome_input_hash"
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _window_and_identity(self) -> "HistoricalRangeOutcomeWorkItemV1":
        if (
            self.policy_bundle_ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST
            or self.policy_bundle_ref.payload_sha256 != self.policy_bundle_hash
        ):
            raise ValueError("work item policy artifact ref/hash pair does not match")
        if self.evaluation_window_type is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE:
            if self.subject_type is not HistoricalRangeOutcomeSubjectType.EPISODE or self.horizon_trade_days != 0:
                raise ValueError("episode lifecycle work requires EPISODE subject and horizon sentinel zero")
        elif self.subject_type is HistoricalRangeOutcomeSubjectType.EPISODE or self.horizon_trade_days < 1:
            raise ValueError("fixed-horizon work excludes EPISODE and requires a positive horizon")
        refs = tuple(sorted(self.source_revision_refs, key=lambda item: item.semantic_content_hash))
        if len(refs) != len({item.semantic_content_hash for item in refs}):
            raise ValueError("source revision refs must be unique")
        artifact_ref_set_hash = canonical_json_sha256(
            [item.model_dump(mode="json") for item in refs]
        )
        if (
            self.source_artifact_ref_set_hash is not None
            and self.source_artifact_ref_set_hash != artifact_ref_set_hash
        ):
            raise ValueError("source_artifact_ref_set_hash does not match exact refs")
        if self.revision_reason is HistoricalRangeOutcomeRevisionReason.INITIAL:
            if self.predecessor_outcome_ref is not None or self.revision_evidence_ref is not None:
                raise ValueError("initial outcome work cannot carry predecessor/correction evidence")
        elif self.predecessor_outcome_ref is None:
            raise ValueError("non-initial outcome work requires an exact predecessor")
        if (
            self.predecessor_outcome_ref is not None
            and self.predecessor_outcome_ref.artifact_kind
            is not HistoricalRangeArtifactKind.OUTCOME
        ):
            raise ValueError("outcome predecessor ref must reference OUTCOME")
        if self.revision_reason in {
            HistoricalRangeOutcomeRevisionReason.SOURCE_CORRECTION,
            HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
        } and self.revision_evidence_ref is None:
            raise ValueError("correction work requires immutable revision evidence")
        logical_id = derive_outcome_logical_id(
            self.subject_type,
            self.subject_id,
            self.projection,
            self.evaluation_window_type,
            self.horizon_trade_days,
            self.policy_bundle_hash,
        )
        payload = self.model_dump(
            mode="json",
            exclude={"outcome_logical_id", "outcome_input_hash"},
        )
        payload["source_revision_refs"] = [
            item.model_dump(mode="json") for item in refs
        ]
        payload["source_artifact_ref_set_hash"] = artifact_ref_set_hash
        input_hash = canonical_json_sha256(payload)
        if self.outcome_logical_id is not None and self.outcome_logical_id != logical_id:
            raise ValueError("outcome_logical_id does not match work identity")
        if self.outcome_input_hash is not None and self.outcome_input_hash != input_hash:
            raise ValueError("outcome_input_hash does not match canonical work item")
        object.__setattr__(self, "source_revision_refs", refs)
        object.__setattr__(self, "source_artifact_ref_set_hash", artifact_ref_set_hash)
        object.__setattr__(self, "outcome_logical_id", logical_id)
        object.__setattr__(self, "outcome_input_hash", input_hash)
        return self


class Phase0ALineageIdentity(_StrictContract):
    lineage_type: Literal["PHASE0A"] = "PHASE0A"
    schema_version: Literal[PHASE0A_LINEAGE_IDENTITY_SCHEMA_VERSION] = PHASE0A_LINEAGE_IDENTITY_SCHEMA_VERSION
    phase0a_audit_id: str = Field(min_length=1, max_length=160)
    phase0a_audit_hash: str = Field(min_length=64, max_length=64)
    phase1_handoff_bundle_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    formal_oos_interval_id: str = Field(min_length=1, max_length=160)
    formal_oos_interval_hash: str = Field(min_length=64, max_length=64)

    @field_validator(
        "phase0a_audit_hash", "phase1_handoff_bundle_hash", "handoff_readiness_hash",
        "admission_scope_hash", "formal_oos_interval_hash",
    )
    @classmethod
    def _hash(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)


class HistoricalRangeLineageIdentity(_StrictContract):
    lineage_type: Literal["HISTORICAL_RANGE"] = "HISTORICAL_RANGE"
    schema_version: Literal[HISTORICAL_RANGE_LINEAGE_IDENTITY_SCHEMA_VERSION] = (
        HISTORICAL_RANGE_LINEAGE_IDENTITY_SCHEMA_VERSION
    )
    historical_range_request_ref: HistoricalRangeArtifactRefV1
    historical_range_frozen_program_ref: HistoricalRangeArtifactRefV1
    range_run_id: str = Field(min_length=1, max_length=160)
    range_day_run_id: str = Field(min_length=1, max_length=160)
    candidate_artifact_ref: HistoricalRangeArtifactRefV1
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    code_release_hash: str = Field(min_length=64, max_length=64)
    signal_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    oos_interval_id: Literal["RETROSPECTIVE_RANGE_NO_FORMAL_OOS_V1"] = (
        "RETROSPECTIVE_RANGE_NO_FORMAL_OOS_V1"
    )
    oos_interval_hash: str = Field(min_length=64, max_length=64)
    range_lineage_identity_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "manifest_sha256", "code_release_hash", "signal_source_revision_set_hash",
        "oos_interval_hash", "range_lineage_identity_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeLineageIdentity":
        if self.historical_range_request_ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST:
            raise ValueError("range lineage request ref must be REQUEST")
        if self.historical_range_frozen_program_ref.artifact_kind is not HistoricalRangeArtifactKind.FROZEN_PROGRAM:
            raise ValueError("range lineage program ref must be FROZEN_PROGRAM")
        if self.candidate_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT:
            raise ValueError("range lineage candidate ref must be CANDIDATE_ARTIFACT")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"range_lineage_identity_hash"}))
        if self.range_lineage_identity_hash is not None and self.range_lineage_identity_hash != digest:
            raise ValueError("range_lineage_identity_hash does not match exact lineage")
        object.__setattr__(self, "range_lineage_identity_hash", digest)
        return self


AdvisoryPhase1LineageIdentity: TypeAlias = Annotated[
    Phase0ALineageIdentity | HistoricalRangeLineageIdentity,
    Field(discriminator="lineage_type"),
]


class HistoricalRangeOutcomeFactV1(_StrictContract):
    outcome_version_id: str = Field(min_length=1, max_length=160)
    outcome_logical_id: str = Field(min_length=1, max_length=160)
    outcome_version: int = Field(ge=1)
    subject_type: HistoricalRangeOutcomeSubjectType
    subject_id: str = Field(min_length=1, max_length=160)
    projection: HistoricalRangeOutcomeProjection
    evaluation_window_type: HistoricalRangeEvaluationWindowType
    horizon_trade_days: int = Field(ge=0)
    historical_range_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    outcome_input_hash: str = Field(min_length=64, max_length=64)
    revision_reason: HistoricalRangeOutcomeRevisionReason
    producer_code_hash: str = Field(min_length=64, max_length=64)
    outcome_contract_version: str = Field(min_length=1, max_length=160)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    predecessor_outcome_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    predecessor_outcome_hash: str | None = Field(default=None, min_length=64, max_length=64)
    revision_evidence_ref: HistoricalRangeArtifactRefV1 | None = None
    revision_evidence_hash: str | None = Field(default=None, min_length=64, max_length=64)
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
        "historical_range_policy_bundle_hash",
        "outcome_input_hash",
        "producer_code_hash",
        "source_revision_set_hash",
        "predecessor_outcome_hash",
        "revision_evidence_hash",
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
            self.evaluation_window_type,
            self.horizon_trade_days,
            self.historical_range_policy_bundle_hash,
        )
        if self.outcome_logical_id != expected:
            raise ValueError("outcome_logical_id does not match outcome identity")
        if self.outcome_version == 1:
            if (
                self.predecessor_outcome_version_id is not None
                or self.predecessor_outcome_hash is not None
                or self.revision_reason is not HistoricalRangeOutcomeRevisionReason.INITIAL
            ):
                raise ValueError("first outcome version cannot have a predecessor")
        elif (
            self.predecessor_outcome_version_id is None
            or self.predecessor_outcome_hash is None
            or self.revision_reason is HistoricalRangeOutcomeRevisionReason.INITIAL
        ):
            raise ValueError("later outcome versions require predecessor identity/hash and revision reason")
        if self.evaluation_window_type is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE:
            if self.subject_type is not HistoricalRangeOutcomeSubjectType.EPISODE or self.horizon_trade_days != 0:
                raise ValueError("episode outcome requires EPISODE subject and horizon sentinel zero")
        elif self.subject_type is HistoricalRangeOutcomeSubjectType.EPISODE or self.horizon_trade_days < 1:
            raise ValueError("fixed-horizon outcome excludes EPISODE and requires positive horizon")
        correction = self.revision_reason in {
            HistoricalRangeOutcomeRevisionReason.SOURCE_CORRECTION,
            HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
        }
        if correction != (self.revision_evidence_ref is not None):
            raise ValueError("correction outcomes require exact revision evidence only")
        if (self.revision_evidence_ref is None) != (self.revision_evidence_hash is None):
            raise ValueError("revision evidence ref/hash must be supplied together")
        if (
            self.revision_evidence_ref is not None
            and self.revision_evidence_hash != self.revision_evidence_ref.semantic_content_hash
        ):
            raise ValueError("revision evidence ref/hash pair differs")
        if self.outcome_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME:
            raise ValueError("outcome_artifact_ref must reference OUTCOME")
        artifact = HistoricalRangeOutcomeArtifactV2.model_validate(self.outcome_json)
        if (
            artifact.outcome_logical_id != self.outcome_logical_id
            or artifact.outcome_version_id != self.outcome_version_id
            or artifact.outcome_input_hash != self.outcome_input_hash
            or artifact.projection_group is not self.projection
            or artifact.evaluation_window_type is not self.evaluation_window_type
            or artifact.horizon_trade_days != self.horizon_trade_days
            or artifact.policy_bundle_hash
            != self.historical_range_policy_bundle_hash
            or artifact.label_as_of_trade_date != self.label_as_of_trade_date
            or artifact.source_revision_set_hash != self.source_revision_set_hash
            or artifact.maturity_status is not self.maturity_status
            or artifact.next_refresh_trade_date != self.next_refresh_trade_date
            or artifact.producer_code_hash != self.producer_code_hash
            or (artifact.predecessor_outcome_ref is None)
            != (self.predecessor_outcome_version_id is None)
            or (
                self.revision_evidence_ref is not None
                and self.revision_evidence_ref not in artifact.direct_upstream_refs
            )
        ):
            raise ValueError("outcome fact columns differ from the embedded V2 artifact")
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
    summary_policy_hash: str = Field(min_length=64, max_length=64)
    summary_input_hash: str = Field(min_length=64, max_length=64)
    recall_denominator_set_hash: str = Field(min_length=64, max_length=64)
    recall_denominator_evidence_json: dict[str, Any]
    producer_code_hash: str = Field(min_length=64, max_length=64)
    maturity_coverage_json: dict[str, Any]
    maturity_coverage_hash: str = Field(min_length=64, max_length=64)
    predecessor_summary_id: str | None = Field(default=None, min_length=1, max_length=160)
    predecessor_summary_hash: str | None = Field(default=None, min_length=64, max_length=64)
    summary_artifact_ref: HistoricalRangeArtifactRefV1
    summary_json: dict[str, Any]
    summary_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "covered_outcome_set_hash",
        "summary_policy_hash",
        "summary_input_hash",
        "recall_denominator_set_hash",
        "producer_code_hash",
        "maturity_coverage_hash",
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
            if (
                self.predecessor_summary_id is not None
                or self.predecessor_summary_hash is not None
            ):
                raise ValueError("first summary version cannot have a predecessor")
        elif (
            self.predecessor_summary_id is None
            or self.predecessor_summary_hash is None
        ):
            raise ValueError("later summary versions require predecessor identity/hash")
        if canonical_json_sha256(self.maturity_coverage_json) != self.maturity_coverage_hash:
            raise ValueError("maturity_coverage_hash does not match coverage payload")
        expected_input_hash = canonical_json_sha256(
            {
                "covered_outcome_set_hash": self.covered_outcome_set_hash,
                "summary_policy_hash": self.summary_policy_hash,
                "recall_denominator_set_hash": self.recall_denominator_set_hash,
                "producer_code_hash": self.producer_code_hash,
            }
        )
        if self.summary_input_hash != expected_input_hash:
            raise ValueError("summary_input_hash does not close outcome set, policy, and code")
        artifact = HistoricalRangeSummaryArtifactV2.model_validate(self.summary_json)
        if (
            artifact.range_run_id != self.range_run_id
            or artifact.summary_input_hash != self.summary_input_hash
            or artifact.summary_policy_hash != self.summary_policy_hash
            or artifact.recall_denominator_set_hash
            != self.recall_denominator_set_hash
            or artifact.recall_denominator_evidence
            != self.recall_denominator_evidence_json
            or artifact.covered_outcome_set_hash != self.covered_outcome_set_hash
            or artifact.producer_code_hash != self.producer_code_hash
            or artifact.maturity_coverage != self.maturity_coverage_json
            or artifact.maturity_coverage_hash != self.maturity_coverage_hash
        ):
            raise ValueError("summary fact columns differ from the embedded V2 artifact")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"summary_content_hash"}))
        if self.summary_content_hash is not None and self.summary_content_hash != digest:
            raise ValueError("summary_content_hash does not match summary facts")
        object.__setattr__(self, "summary_content_hash", digest)
        return self


class HistoricalRangeOutcomeArtifactV2(_StrictContract):
    schema_version: Literal[OUTCOME_ARTIFACT_SCHEMA_VERSION_V2] = OUTCOME_ARTIFACT_SCHEMA_VERSION_V2
    outcome_logical_id: str = Field(min_length=1, max_length=160)
    outcome_version_id: str = Field(min_length=1, max_length=160)
    outcome_input_hash: str = Field(min_length=64, max_length=64)
    subject_ref: HistoricalRangeArtifactRefV1
    direct_upstream_refs: tuple[HistoricalRangeArtifactRefV1, ...] = Field(min_length=1)
    projection_group: HistoricalRangeOutcomeProjection
    evaluation_window_type: HistoricalRangeEvaluationWindowType
    horizon_trade_days: int = Field(ge=0)
    policy_bundle_ref: HistoricalRangeArtifactRefV1
    policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_as_of_trade_date: date
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    maturity_status: HistoricalRangeOutcomeStatus
    next_refresh_trade_date: date | None = None
    reason_codes: tuple[str, ...] = ()
    calculation_results: tuple[dict[str, Any], ...] = ()
    calculation_result_set_hash: str = Field(min_length=64, max_length=64)
    predecessor_outcome_ref: HistoricalRangeArtifactRefV1 | None = None
    producer_code_hash: str = Field(min_length=64, max_length=64)

    @field_validator(
        "outcome_input_hash", "policy_bundle_hash", "source_revision_set_hash",
        "calculation_result_set_hash", "producer_code_hash",
    )
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeOutcomeArtifactV2":
        refs = tuple(sorted(self.direct_upstream_refs, key=lambda item: item.semantic_content_hash))
        if len(refs) != len({item.semantic_content_hash for item in refs}) or self.subject_ref not in refs:
            raise ValueError("outcome artifact requires unique exact upstream refs including subject")
        if (
            self.policy_bundle_ref.artifact_kind
            is not HistoricalRangeArtifactKind.REQUEST
            or self.policy_bundle_ref.payload_sha256 != self.policy_bundle_hash
            or self.policy_bundle_ref not in refs
        ):
            raise ValueError(
                "outcome artifact requires the exact frozen policy bundle upstream"
            )
        if self.predecessor_outcome_ref is not None and (
            self.predecessor_outcome_ref.artifact_kind
            is not HistoricalRangeArtifactKind.OUTCOME
            or self.predecessor_outcome_ref not in refs
        ):
            raise ValueError("outcome artifact predecessor must be an exact upstream OUTCOME ref")
        if canonical_json_sha256(list(self.calculation_results)) != self.calculation_result_set_hash:
            raise ValueError("calculation_result_set_hash does not match typed results")
        reasons = tuple(sorted(_nonblank(item, field_name="reason_code") for item in self.reason_codes))
        if len(reasons) != len(set(reasons)):
            raise ValueError("outcome artifact reason codes must be unique")
        if self.maturity_status is HistoricalRangeOutcomeStatus.FAILED and (self.calculation_results or not reasons):
            raise ValueError("FAILED outcome artifact requires reasons and no fabricated calculation result")
        if self.maturity_status is not HistoricalRangeOutcomeStatus.FAILED and not self.calculation_results:
            raise ValueError("non-failed outcome artifact requires calculation results")
        object.__setattr__(self, "direct_upstream_refs", refs)
        object.__setattr__(self, "reason_codes", reasons)
        return self


class HistoricalRangeSummaryPolicyV1(_StrictContract):
    schema_version: Literal[SUMMARY_POLICY_SCHEMA_VERSION] = SUMMARY_POLICY_SCHEMA_VERSION
    subject_types: tuple[HistoricalRangeOutcomeSubjectType, ...]
    projection_groups: tuple[HistoricalRangeOutcomeProjection, ...]
    evaluation_window_types: tuple[HistoricalRangeEvaluationWindowType, ...]
    horizons: tuple[int, ...]
    outcome_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    latest_eligible_resolution: Literal["MAX_VERSION_AS_OF_EXACT_LOGICAL_KEY_V1"] = (
        "MAX_VERSION_AS_OF_EXACT_LOGICAL_KEY_V1"
    )
    incomplete_denominator_policy: Literal["ALL_ELIGIBLE_STATUS_COUNTS_V1"] = "ALL_ELIGIBLE_STATUS_COUNTS_V1"
    equal_weight_policy: Literal["DECISION_DAY_LIST_THEN_RANGE_EQUAL_WEIGHT_V1"] = (
        "DECISION_DAY_LIST_THEN_RANGE_EQUAL_WEIGHT_V1"
    )
    turnover_policy: Literal["HALF_ABSOLUTE_WEIGHT_DELTA_ADJACENT_SUCCESS_V1"] = (
        "HALF_ABSOLUTE_WEIGHT_DELTA_ADJACENT_SUCCESS_V1"
    )
    drawdown_policy: Literal["COMPOUNDED_COHORT_RUNNING_MAX_V1"] = "COMPOUNDED_COHORT_RUNNING_MAX_V1"
    industry_policy: Literal["DECISION_T_FROZEN_HHI_UNKNOWN_BUCKET_V1"] = (
        "DECISION_T_FROZEN_HHI_UNKNOWN_BUCKET_V1"
    )
    recall_policy: Literal["PIT_ELIGIBLE_POSITIVE_TOPK_V1"] = "PIT_ELIGIBLE_POSITIVE_TOPK_V1"
    recall_k_values: tuple[int, ...] = (5,)
    regime_policy: Literal["DECISION_T_FROZEN_REGIME_V1"] = "DECISION_T_FROZEN_REGIME_V1"
    decimal_quantization: str = Field(default="0.000000000001", pattern=r"^0\.0+1$")
    missing_value_policy: Literal["TYPED_UNAVAILABLE_NEVER_ZERO_V1"] = "TYPED_UNAVAILABLE_NEVER_ZERO_V1"
    summary_policy_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("outcome_policy_bundle_hash", "summary_policy_hash")
    @classmethod
    def _hash(cls, value: str | None, info: Any) -> str | None:
        return (
            require_sha256(value, field_name=info.field_name)
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeSummaryPolicyV1":
        for field_name, values, key in (
            ("subject_types", self.subject_types, lambda item: item.value),
            ("projection_groups", self.projection_groups, lambda item: item.value),
            ("evaluation_window_types", self.evaluation_window_types, lambda item: item.value),
        ):
            if not values or values != tuple(sorted(set(values), key=key)):
                raise ValueError(f"{field_name} must be sorted, unique, and non-empty")
        if self.horizons != tuple(sorted(set(self.horizons))) or any(item < 0 for item in self.horizons):
            raise ValueError("summary horizons must be sorted, unique, and non-negative")
        if self.recall_k_values != tuple(sorted(set(self.recall_k_values))) or any(
            item < 1 for item in self.recall_k_values
        ):
            raise ValueError("recall_k_values must be sorted, unique, and positive")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"summary_policy_hash"}))
        if self.summary_policy_hash is not None and self.summary_policy_hash != digest:
            raise ValueError("summary_policy_hash does not match frozen policy")
        object.__setattr__(self, "summary_policy_hash", digest)
        return self


class HistoricalRangeSummaryArtifactV2(_StrictContract):
    schema_version: Literal[SUMMARY_ARTIFACT_SCHEMA_VERSION_V2] = SUMMARY_ARTIFACT_SCHEMA_VERSION_V2
    range_run_id: str = Field(min_length=1, max_length=160)
    summary_input_hash: str = Field(min_length=64, max_length=64)
    summary_policy_hash: str = Field(min_length=64, max_length=64)
    covered_outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...]
    covered_outcome_set_hash: str = Field(min_length=64, max_length=64)
    recall_denominator_evidence: dict[str, Any]
    recall_denominator_set_hash: str = Field(min_length=64, max_length=64)
    maturity_coverage: dict[str, Any]
    maturity_coverage_hash: str = Field(min_length=64, max_length=64)
    metrics: tuple[dict[str, Any], ...]
    metrics_hash: str = Field(min_length=64, max_length=64)
    unavailable_metrics: tuple[dict[str, Any], ...] = ()
    predecessor_summary_ref: HistoricalRangeArtifactRefV1 | None = None
    producer_code_hash: str = Field(min_length=64, max_length=64)

    @field_validator(
        "summary_input_hash", "summary_policy_hash", "covered_outcome_set_hash",
        "recall_denominator_set_hash",
        "maturity_coverage_hash", "metrics_hash", "producer_code_hash",
    )
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeSummaryArtifactV2":
        refs = tuple(sorted(self.covered_outcome_refs, key=lambda item: item.semantic_content_hash))
        if len(refs) != len({item.semantic_content_hash for item in refs}):
            raise ValueError("covered outcome refs must be unique")
        if canonical_json_sha256([item.model_dump(mode="json") for item in refs]) != self.covered_outcome_set_hash:
            raise ValueError("covered_outcome_set_hash does not match exact refs")
        recall_evidence_payload = dict(self.recall_denominator_evidence)
        declared_recall_hash = recall_evidence_payload.pop(
            "denominator_set_hash", None
        )
        if (
            declared_recall_hash != self.recall_denominator_set_hash
            or canonical_json_sha256(recall_evidence_payload)
            != self.recall_denominator_set_hash
        ):
            raise ValueError(
                "recall_denominator_set_hash does not match typed evidence"
            )
        if canonical_json_sha256(self.maturity_coverage) != self.maturity_coverage_hash:
            raise ValueError("maturity_coverage_hash does not match coverage")
        if canonical_json_sha256(list(self.metrics)) != self.metrics_hash:
            raise ValueError("metrics_hash does not match summary metrics")
        if (
            self.predecessor_summary_ref is not None
            and self.predecessor_summary_ref.artifact_kind
            is not HistoricalRangeArtifactKind.SUMMARY
        ):
            raise ValueError("summary predecessor ref must be SUMMARY")
        expected_input = canonical_json_sha256(
            {
                "covered_outcome_set_hash": self.covered_outcome_set_hash,
                "summary_policy_hash": self.summary_policy_hash,
                "recall_denominator_set_hash": self.recall_denominator_set_hash,
                "producer_code_hash": self.producer_code_hash,
            }
        )
        if self.summary_input_hash != expected_input:
            raise ValueError(
                "summary_input_hash does not close outcome, Recall, policy, and code"
            )
        object.__setattr__(self, "covered_outcome_refs", refs)
        return self


class HistoricalRangeOutcomeRefreshReceiptV1(_StrictContract):
    schema_version: Literal[OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION] = OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION
    operation_id: str = Field(min_length=1, max_length=160)
    request_hash: str = Field(min_length=64, max_length=64)
    status: Literal["COMPLETED", "WAITING_INPUT", "RETRYABLE_FAILED", "FAILED"]
    stable_keyset_cursor: dict[str, Any] | None = None
    processed_count: int = Field(ge=0)
    outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...] = ()
    summary_refs: tuple[HistoricalRangeArtifactRefV1, ...] = ()
    reason_codes: tuple[str, ...] = ()
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("request_hash", "receipt_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeOutcomeRefreshReceiptV1":
        if any(item.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME for item in self.outcome_refs):
            raise ValueError("outcome refresh receipt can reference only OUTCOME artifacts")
        if any(item.artifact_kind is not HistoricalRangeArtifactKind.SUMMARY for item in self.summary_refs):
            raise ValueError("outcome refresh receipt can reference only SUMMARY artifacts")
        for field_name, refs in (
            ("outcome_refs", self.outcome_refs),
            ("summary_refs", self.summary_refs),
        ):
            identities = tuple(
                (
                    item.artifact_kind.value,
                    item.semantic_content_hash,
                    item.relative_path,
                )
                for item in refs
            )
            if identities != tuple(sorted(set(identities))):
                raise ValueError(f"refresh {field_name} must be sorted and duplicate-free")
        if self.processed_count != len(self.outcome_refs):
            raise ValueError("refresh processed_count must equal unique outcome refs")
        if self.status == "COMPLETED" and self.stable_keyset_cursor is not None:
            raise ValueError("completed refresh receipt cannot retain a cursor")
        if self.status != "COMPLETED" and not self.reason_codes:
            raise ValueError("non-completed refresh receipt requires reason codes")
        reasons = tuple(sorted(_nonblank(item, field_name="reason_code") for item in self.reason_codes))
        if self.reason_codes != reasons or len(reasons) != len(set(reasons)):
            raise ValueError("refresh reason codes must be sorted and duplicate-free")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"receipt_hash"}))
        if self.receipt_hash is not None and self.receipt_hash != digest:
            raise ValueError("receipt_hash does not match outcome refresh receipt")
        object.__setattr__(self, "receipt_hash", digest)
        return self


class HistoricalRangeDatasetBridgeRequestV1(_StrictContract):
    schema_version: Literal[DATASET_BRIDGE_REQUEST_SCHEMA_VERSION] = DATASET_BRIDGE_REQUEST_SCHEMA_VERSION
    batch_id: str = Field(min_length=1, max_length=160)
    range_run_ids: tuple[str, ...] = Field(min_length=1)
    successful_day_refs: tuple[HistoricalRangeArtifactRefV1, ...]
    candidate_refs: tuple[HistoricalRangeArtifactRefV1, ...]
    outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...]
    summary_refs: tuple[HistoricalRangeArtifactRefV1, ...] = ()
    requested_horizons: tuple[int, ...] = Field(min_length=1)
    requested_maturity_statuses: tuple[HistoricalRangeOutcomeStatus, ...] = Field(
        min_length=1
    )
    evidence_scope: Literal["RETROSPECTIVE_RESEARCH_ONLY"] = "RETROSPECTIVE_RESEARCH_ONLY"
    lineage_source_type: Literal["HISTORICAL_RANGE_RESEARCH"] = "HISTORICAL_RANGE_RESEARCH"
    execution_origin: Literal["HISTORICAL_RANGE_RESEARCH"] = "HISTORICAL_RANGE_RESEARCH"
    research_scope: Literal["RETROSPECTIVE_RESEARCH_ONLY"] = "RETROSPECTIVE_RESEARCH_ONLY"
    policy_bundle_refs: tuple[HistoricalRangeArtifactRefV1, ...] = Field(min_length=1)
    policy_component_hashes: dict[str, dict[str, str]] = Field(min_length=1)
    canonical_signal_dedup_policy_hash: str = Field(min_length=64, max_length=64)
    retrospective_selector_policy_hash: str = Field(min_length=64, max_length=64)
    dataset_schema_hash: str = Field(min_length=64, max_length=64)
    builder_hash: str = Field(min_length=64, max_length=64)
    writer_hash: str = Field(min_length=64, max_length=64)
    partition_policy_hash: str = Field(min_length=64, max_length=64)
    compression_config_hash: str = Field(min_length=64, max_length=64)
    artifact_root_identity_hash: str = Field(min_length=64, max_length=64)
    operation_idempotency_key: str = Field(min_length=1, max_length=200)
    expected_batch_row_version: int = Field(ge=1)
    lease_seconds: int = Field(default=300, ge=1, le=86_400)
    request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "canonical_signal_dedup_policy_hash", "retrospective_selector_policy_hash", "dataset_schema_hash",
        "builder_hash", "writer_hash", "partition_policy_hash", "compression_config_hash",
        "artifact_root_identity_hash", "request_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeDatasetBridgeRequestV1":
        if self.range_run_ids != tuple(sorted(set(self.range_run_ids))):
            raise ValueError("bridge range_run_ids must be sorted and unique")
        for field_name, refs, kind in (
            ("successful_day_refs", self.successful_day_refs, HistoricalRangeArtifactKind.DAY_RECEIPT),
            ("candidate_refs", self.candidate_refs, HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT),
            ("outcome_refs", self.outcome_refs, HistoricalRangeArtifactKind.OUTCOME),
            ("summary_refs", self.summary_refs, HistoricalRangeArtifactKind.SUMMARY),
            ("policy_bundle_refs", self.policy_bundle_refs, HistoricalRangeArtifactKind.REQUEST),
        ):
            if any(item.artifact_kind is not kind for item in refs):
                raise ValueError(f"{field_name} contains an invalid artifact kind")
            identities = tuple(
                (
                    item.artifact_kind.value,
                    item.semantic_content_hash,
                    item.relative_path,
                )
                for item in refs
            )
            if identities != tuple(sorted(set(identities))):
                raise ValueError(f"{field_name} must be sorted and duplicate-free")
        policy_ref_hashes = {item.payload_sha256 for item in self.policy_bundle_refs}
        if len(policy_ref_hashes) != len(self.policy_bundle_refs):
            raise ValueError("bridge policy bundle payload hashes must be unique")
        if set(self.policy_component_hashes) != policy_ref_hashes:
            raise ValueError(
                "bridge policy component hashes must exactly cover policy refs"
            )
        required_component_roles = {
            "CALENDAR",
            "MARKET_DATA",
            "EXECUTION",
            "COST",
            "BENCHMARK",
            "CASH_RETURN",
            "TERMINAL",
            "BARRIER",
            "CORPORATE_ACTION",
        }
        for policy_hash, components in self.policy_component_hashes.items():
            require_sha256(policy_hash, field_name="policy_component_hashes key")
            if set(components) != required_component_roles:
                raise ValueError(
                    "bridge policy component set must contain every Phase 1 role"
                )
            for role, component_hash in components.items():
                require_sha256(
                    component_hash,
                    field_name=f"policy_component_hashes[{role}]",
                )
        if self.requested_horizons != tuple(sorted(set(self.requested_horizons))) or any(
            item < 1 for item in self.requested_horizons
        ):
            raise ValueError("bridge horizons must be sorted, unique, and positive")
        maturity_values = tuple(item.value for item in self.requested_maturity_statuses)
        allowed_maturity = {
            HistoricalRangeOutcomeStatus.COMPLETE.value,
            HistoricalRangeOutcomeStatus.TERMINAL.value,
        }
        if (
            maturity_values != tuple(sorted(set(maturity_values)))
            or not set(maturity_values) <= allowed_maturity
        ):
            raise ValueError(
                "bridge maturity statuses must be sorted, unique, and label-eligible"
            )
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"request_hash"}))
        if self.request_hash is not None and self.request_hash != digest:
            raise ValueError("request_hash does not match dataset bridge request")
        object.__setattr__(self, "request_hash", digest)
        return self


class HistoricalRangeDatasetBridgeReceiptV1(_StrictContract):
    schema_version: Literal[DATASET_BRIDGE_RECEIPT_SCHEMA_VERSION] = DATASET_BRIDGE_RECEIPT_SCHEMA_VERSION
    operation_id: str = Field(min_length=1, max_length=160)
    request_hash: str = Field(min_length=64, max_length=64)
    result_status: HistoricalRangeBridgeResultStatus
    observation_count: int = Field(ge=0)
    label_count: int = Field(ge=0)
    canonical_signal_count: int = Field(ge=0)
    range_lineage_count: int = Field(ge=0)
    retrospective_selector_policy_hash: str = Field(min_length=64, max_length=64)
    dataset_build_id: str | None = Field(default=None, min_length=1, max_length=160)
    sealed_snapshot_id: str | None = Field(default=None, min_length=1, max_length=160)
    bridge_artifact_ref: HistoricalRangeArtifactRefV1
    reason_codes: tuple[str, ...] = ()
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("request_hash", "retrospective_selector_policy_hash", "receipt_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeDatasetBridgeReceiptV1":
        if self.bridge_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.DATASET_BRIDGE:
            raise ValueError("bridge receipt must reference DATASET_BRIDGE artifact")
        nonempty = self.observation_count > 0
        if self.result_status is HistoricalRangeBridgeResultStatus.VALID_EMPTY:
            if (
                nonempty
                or self.label_count
                or self.canonical_signal_count
                or self.range_lineage_count
                or self.dataset_build_id is not None
                or self.sealed_snapshot_id is not None
            ):
                raise ValueError("VALID_EMPTY cannot create capture, build, label, or snapshot facts")
            if REASON_DATASET_BRIDGE_VALID_EMPTY not in self.reason_codes:
                raise ValueError("VALID_EMPTY receipt requires its stable reason code")
        elif self.result_status is HistoricalRangeBridgeResultStatus.SEALED:
            if not nonempty or self.label_count < 1 or self.dataset_build_id is None or self.sealed_snapshot_id is None:
                raise ValueError("SEALED bridge requires non-empty observations, labels, build, and snapshot")
        else:
            if (
                nonempty
                or self.label_count
                or self.canonical_signal_count
                or self.range_lineage_count
                or self.dataset_build_id is not None
                or self.sealed_snapshot_id is not None
            ):
                raise ValueError("failed bridge receipt cannot claim materialized facts")
            if not self.reason_codes:
                raise ValueError("failed bridge receipt requires reason codes")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"receipt_hash"}))
        if self.receipt_hash is not None and self.receipt_hash != digest:
            raise ValueError("receipt_hash does not match dataset bridge receipt")
        object.__setattr__(self, "receipt_hash", digest)
        return self


class HistoricalRangeDayReceiptPayloadV2(_StrictContract):
    """Typed successful-day receipt for the R3 candidate/mark/list closure."""

    schema_version: Literal[DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2] = DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2
    range_run_id: str = Field(min_length=1, max_length=160)
    day_run_id: str = Field(min_length=1, max_length=160)
    terminal_status: Literal["COMPLETE", "VALID_NO_CANDIDATE"]
    day_input_hash: str = Field(min_length=64, max_length=64)
    candidate_artifact_ref: HistoricalRangeArtifactRefV1
    decision_mark_set_ref: HistoricalRangeArtifactRefV1
    previous_day_receipt_ref: HistoricalRangeArtifactRefV1 | None = None
    list_version: HistoricalRangeListVersionFactV1
    items: tuple[HistoricalRangeListItemFactV1, ...]
    episode_snapshots: tuple[HistoricalRangeEpisodeSnapshotFactV1, ...]
    reason_codes: tuple[str, ...] = ()

    @field_validator("day_input_hash")
    @classmethod
    def _day_input_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="day_input_hash")

    @model_validator(mode="after")
    def _receipt_closure(self) -> "HistoricalRangeDayReceiptPayloadV2":
        if self.candidate_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT:
            raise ValueError("R3 day receipt candidate_artifact_ref must be CANDIDATE_ARTIFACT")
        if self.decision_mark_set_ref.artifact_kind is not HistoricalRangeArtifactKind.DECISION_MARK_SET:
            raise ValueError("R3 day receipt decision_mark_set_ref must be DECISION_MARK_SET")
        if (
            self.previous_day_receipt_ref is not None
            and self.previous_day_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT
        ):
            raise ValueError("R3 day receipt predecessor must be DAY_RECEIPT")
        predecessor_present = self.previous_day_receipt_ref is not None
        if predecessor_present != (self.list_version.previous_list_version_id is not None):
            raise ValueError("R3 day receipt predecessor ref/list identity must be supplied together")
        if predecessor_present and (
            self.list_version.previous_day_receipt_hash
            != self.previous_day_receipt_ref.semantic_content_hash
            or self.list_version.previous_list_hash is None
        ):
            raise ValueError("R3 day receipt predecessor hashes differ from the exact predecessor ref")
        if self.list_version.day_run_id != self.day_run_id or self.list_version.range_run_id != self.range_run_id:
            raise ValueError("R3 day receipt list version identity differs from the day")
        if any(item.list_version_id != self.list_version.list_version_id for item in self.items):
            raise ValueError("R3 day receipt item belongs to a different list version")
        if any(item.list_version_id != self.list_version.list_version_id for item in self.episode_snapshots):
            raise ValueError("R3 day receipt episode belongs to a different list version")
        if self.list_version.list_content_hash != derive_list_content_hash(
            self.list_version,
            self.items,
            self.episode_snapshots,
        ):
            raise ValueError("R3 day receipt list content hash does not close the projected facts")
        reasons = tuple(sorted(_nonblank(item, field_name="reason_code") for item in self.reason_codes))
        if len(reasons) != len(set(reasons)):
            raise ValueError("R3 day receipt reason_codes must be duplicate-free")
        object.__setattr__(self, "reason_codes", reasons)
        return self


class HistoricalRangeSuccessfulDayReadbackV1(_StrictContract):
    ordinal: int = Field(ge=1)
    decision_trade_date: date
    receipt_ref: HistoricalRangeArtifactRefV1
    receipt: HistoricalRangeDayReceiptPayloadV2
    candidate_payload: HistoricalRangeCandidateArtifactPayloadV2
    decision_mark_set: HistoricalRangeDecisionMarkSetV1
    attempt: HistoricalRangeDayAttemptV1

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeSuccessfulDayReadbackV1":
        if self.receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT:
            raise ValueError("successful day readback requires a DAY_RECEIPT ref")
        if self.receipt_ref.semantic_content_hash != self.attempt.result_hash:
            raise ValueError("successful day attempt result hash differs from the DAY_RECEIPT ref")
        if self.receipt.day_run_id != self.candidate_payload.day_run_id:
            raise ValueError("successful day candidate identity differs from its receipt")
        if self.receipt.day_run_id != self.decision_mark_set.day_run_id:
            raise ValueError("successful day mark identity differs from its receipt")
        return self


class HistoricalRangeRunExecutionReceiptV1(_StrictContract):
    schema_version: Literal[RUN_EXECUTION_RECEIPT_SCHEMA_VERSION] = RUN_EXECUTION_RECEIPT_SCHEMA_VERSION
    range_run_id: str = Field(min_length=1, max_length=160)
    research_program_id: str = Field(min_length=1, max_length=160)
    status: Literal["COMPLETED", "FAILED", "PARTIAL", "CANCELLED"]
    resolved_request_hash: str = Field(min_length=64, max_length=64)
    ordered_success_day_receipt_refs: tuple[HistoricalRangeArtifactRefV1, ...] = ()
    blocking_attempt_receipt_ref: HistoricalRangeArtifactRefV1 | None = None
    first_list_hash: str | None = Field(default=None, min_length=64, max_length=64)
    latest_list_hash: str | None = Field(default=None, min_length=64, max_length=64)
    successful_day_count: int = Field(ge=0)
    failed_day_count: int = Field(ge=0)
    unexecuted_day_count: int = Field(ge=0)
    blocking_day_run_id: str | None = Field(default=None, min_length=1, max_length=160)
    blocking_ordinal: int | None = Field(default=None, ge=1)

    @field_validator("resolved_request_hash", "first_list_hash", "latest_list_hash")
    @classmethod
    def _receipt_hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeRunExecutionReceiptV1":
        refs = self.ordered_success_day_receipt_refs
        if any(ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT for ref in refs):
            raise ValueError("run receipt success refs must all be DAY_RECEIPT artifacts")
        identities = tuple(ref.semantic_content_hash for ref in refs)
        if len(identities) != len(set(identities)):
            raise ValueError("run receipt success refs must be unique")
        if len(refs) != self.successful_day_count:
            raise ValueError("run receipt successful_day_count differs from its ordered refs")
        if self.blocking_attempt_receipt_ref is not None and (
            self.blocking_attempt_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT
        ):
            raise ValueError("run receipt blocking attempt must be a DAY_RECEIPT artifact")
        if (self.first_list_hash is None) != (self.latest_list_hash is None):
            raise ValueError("run receipt first/latest list hashes must be supplied together")
        if (self.successful_day_count == 0) != (self.first_list_hash is None):
            raise ValueError("run receipt list hashes must match successful day presence")
        blocking = (self.blocking_attempt_receipt_ref, self.blocking_day_run_id, self.blocking_ordinal)
        if any(item is None for item in blocking) and any(item is not None for item in blocking):
            raise ValueError("run receipt blocking attempt/day/ordinal must be supplied together")
        if self.status == "COMPLETED" and (
            self.failed_day_count != 0 or self.unexecuted_day_count != 0 or any(item is not None for item in blocking)
        ):
            raise ValueError("completed run receipt cannot contain failed, unexecuted, or blocking state")
        if self.status in {"FAILED", "PARTIAL"} and (
            self.failed_day_count != 1 or any(item is None for item in blocking)
        ):
            raise ValueError("failed/partial run receipt requires exactly one blocking failed day")
        if self.status == "FAILED" and self.successful_day_count != 0:
            raise ValueError("failed run receipt cannot contain successful days")
        if self.status == "PARTIAL" and self.successful_day_count == 0:
            raise ValueError("partial run receipt requires a successful prefix")
        return self


class HistoricalRangeOperationProgramResultV1(_StrictContract):
    range_run_id: str = Field(min_length=1, max_length=160)
    research_program_id: str = Field(min_length=1, max_length=160)
    status: HistoricalRangeProgramStatus
    row_version: int = Field(ge=1)
    final_receipt_ref: HistoricalRangeArtifactRefV1 | None = None

    @model_validator(mode="after")
    def _result_ref(self) -> "HistoricalRangeOperationProgramResultV1":
        receipt_required = self.status in {
            HistoricalRangeProgramStatus.COMPLETED,
            HistoricalRangeProgramStatus.FAILED,
            HistoricalRangeProgramStatus.CANCELLED,
        }
        if receipt_required and self.final_receipt_ref is None:
            raise ValueError("terminal Program result requires exactly one final range receipt")
        if self.status not in {
            HistoricalRangeProgramStatus.COMPLETED,
            HistoricalRangeProgramStatus.FAILED,
            HistoricalRangeProgramStatus.CANCELLED,
            HistoricalRangeProgramStatus.PARTIAL,
        } and self.final_receipt_ref is not None:
            raise ValueError("nonterminal Program result cannot carry a final range receipt")
        if self.final_receipt_ref is not None and (
            self.final_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.RANGE_RECEIPT
        ):
            raise ValueError("Program final receipt must be RANGE_RECEIPT")
        return self


class HistoricalRangeOperationCancelledDayResultV1(_StrictContract):
    range_run_id: str = Field(min_length=1, max_length=160)
    research_program_id: str = Field(min_length=1, max_length=160)
    day_run_id: str = Field(min_length=1, max_length=160)
    ordinal: int = Field(ge=1)
    row_version: int = Field(ge=1)
    attempt_no: int = Field(ge=1)
    fencing_token: int = Field(ge=1)
    attempt_receipt_ref: HistoricalRangeArtifactRefV1

    @model_validator(mode="after")
    def _cancelled_day_receipt(self) -> "HistoricalRangeOperationCancelledDayResultV1":
        if self.attempt_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT:
            raise ValueError("cancelled day result requires a DAY_RECEIPT attempt ref")
        return self


class HistoricalRangeExecutionOperationReceiptV1(_StrictContract):
    schema_version: Literal[EXECUTION_OPERATION_RECEIPT_SCHEMA_VERSION] = EXECUTION_OPERATION_RECEIPT_SCHEMA_VERSION
    operation_id: str = Field(min_length=1, max_length=160)
    operation_type: Literal["RESUME", "CANCEL"]
    operation_idempotency_key: str = Field(min_length=1, max_length=200)
    idempotency_payload_hash: str = Field(min_length=64, max_length=64)
    attempt_no: int = Field(ge=1)
    fencing_token: int = Field(ge=1)
    starting_batch_row_version: int = Field(ge=1)
    ending_batch_row_version: int = Field(ge=1)
    result_status: HistoricalRangeBatchStatus
    executed_day_count: int = Field(ge=0)
    successful_day_count: int = Field(ge=0)
    waiting_day_count: int = Field(ge=0)
    retryable_day_count: int = Field(ge=0)
    failed_day_count: int = Field(ge=0)
    blocking_day_run_ids: tuple[str, ...] = ()
    program_results: tuple[HistoricalRangeOperationProgramResultV1, ...]
    cancelled_day_results: tuple[HistoricalRangeOperationCancelledDayResultV1, ...] = ()
    prior_nonterminal_attempt_receipt_refs: tuple[HistoricalRangeArtifactRefV1, ...] = ()
    stable_cursor: dict[str, Any]

    @field_validator("idempotency_payload_hash")
    @classmethod
    def _payload_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="idempotency_payload_hash")

    @model_validator(mode="after")
    def _operation_closure(self) -> "HistoricalRangeExecutionOperationReceiptV1":
        program_ids = tuple(item.research_program_id for item in self.program_results)
        if tuple(sorted(program_ids)) != program_ids or len(program_ids) != len(set(program_ids)):
            raise ValueError("operation Program results must be unique and ordered by research_program_id")
        cancelled_day_keys = tuple(
            (item.research_program_id, item.ordinal, item.day_run_id)
            for item in self.cancelled_day_results
        )
        if tuple(sorted(cancelled_day_keys)) != cancelled_day_keys or len(cancelled_day_keys) != len(
            set(cancelled_day_keys)
        ):
            raise ValueError("cancelled day results must be unique and ordered by Program/ordinal/day")
        if self.operation_type != "CANCEL" and self.cancelled_day_results:
            raise ValueError("only CANCEL operation receipts may contain cancelled day results")
        if any(
            ref.artifact_kind is not HistoricalRangeArtifactKind.RANGE_RECEIPT
            for ref in self.prior_nonterminal_attempt_receipt_refs
        ):
            raise ValueError("prior operation attempt receipts must be RANGE_RECEIPT artifacts")
        if self.ending_batch_row_version < self.starting_batch_row_version:
            raise ValueError("operation ending batch row version cannot precede its starting version")
        blocking = tuple(sorted(_nonblank(item, field_name="blocking_day_run_id") for item in self.blocking_day_run_ids))
        if len(blocking) != len(set(blocking)):
            raise ValueError("operation blocking day ids must be unique")
        object.__setattr__(self, "blocking_day_run_ids", blocking)
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
    research_program_id: str,
    decision_trade_date: date,
    candidate_input_hash: str,
    package_id: str,
    package_version: str,
    manifest_sha256: str,
    alpha_mode: HistoricalRangeAlphaMode,
    runtime_profile_hash: str,
    selection_semantics_hash: str,
    code_release_hash: str,
    calendar_identity_hash: str,
    universe_identity_hash: str,
    universe_count: int,
    raw_signal_identity_hash: str,
    raw_signal_semantic_header: dict[str, Any],
    raw_inference_receipt: dict[str, Any],
    source_read_receipt_hashes: Sequence[str],
    stage_trace: dict[str, Any],
    candidate_outcome: Literal["CANDIDATES_AVAILABLE", "VALID_NO_CANDIDATE"],
    no_candidate_reason_codes: Sequence[str],
    candidates: Sequence[HistoricalRangeCandidateFactV1],
    source_revision_refs: Sequence[HistoricalRangeSourceRevisionRefV1],
) -> dict[str, Any]:
    return HistoricalRangeCandidateArtifactPayloadV2(
        range_run_id=range_run_id,
        day_run_id=day_run_id,
        research_program_id=research_program_id,
        decision_trade_date=decision_trade_date,
        candidate_input_hash=candidate_input_hash,
        package_id=package_id,
        package_version=package_version,
        manifest_sha256=manifest_sha256,
        alpha_mode=alpha_mode,
        runtime_profile_hash=runtime_profile_hash,
        selection_semantics_hash=selection_semantics_hash,
        code_release_hash=code_release_hash,
        calendar_identity_hash=calendar_identity_hash,
        universe_identity_hash=universe_identity_hash,
        universe_count=universe_count,
        raw_signal_identity_hash=raw_signal_identity_hash,
        raw_signal_semantic_header=raw_signal_semantic_header,
        raw_inference_receipt=raw_inference_receipt,
        source_read_receipt_hashes=tuple(source_read_receipt_hashes),
        stage_trace=stage_trace,
        candidate_outcome=candidate_outcome,
        no_candidate_reason_codes=tuple(no_candidate_reason_codes),
        source_revision_refs=tuple(source_revision_refs),
        candidates=tuple(candidates),
    ).model_dump(mode="json")


def build_candidate_input_hash(
    *,
    range_run_id: str,
    research_program_id: str,
    decision_trade_date: date,
    frozen_program_hash: str,
    runtime_profile_hash: str,
    code_release_hash: str,
    selection_semantics_hash: str,
    calendar_identity_hash: str,
    universe_identity_hash: str,
    source_revision_catalog_hash: str,
    query_contract_hash: str,
) -> str:
    return canonical_json_sha256(
        {
            "schema_version": "advisory_historical_range_candidate_input_v1",
            "range_run_id": _nonblank(range_run_id, field_name="range_run_id"),
            "research_program_id": _nonblank(research_program_id, field_name="research_program_id"),
            "decision_trade_date": decision_trade_date,
            "frozen_program_hash": require_sha256(frozen_program_hash, field_name="frozen_program_hash"),
            "runtime_profile_hash": require_sha256(runtime_profile_hash, field_name="runtime_profile_hash"),
            "code_release_hash": require_sha256(code_release_hash, field_name="code_release_hash"),
            "selection_semantics_hash": require_sha256(selection_semantics_hash, field_name="selection_semantics_hash"),
            "calendar_identity_hash": require_sha256(calendar_identity_hash, field_name="calendar_identity_hash"),
            "universe_identity_hash": require_sha256(universe_identity_hash, field_name="universe_identity_hash"),
            "source_revision_catalog_hash": require_sha256(
                source_revision_catalog_hash, field_name="source_revision_catalog_hash"
            ),
            "query_contract_hash": require_sha256(query_contract_hash, field_name="query_contract_hash"),
        }
    )


def build_day_input_hash(
    *,
    candidate_input_hash: str,
    candidate_artifact_ref: HistoricalRangeArtifactRefV1,
    previous_list_hash: str | None,
    previous_day_receipt_hash: str | None,
    list_semantics_hash: str,
) -> str:
    if candidate_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT:
        raise ValueError("candidate_artifact_ref must reference CANDIDATE_ARTIFACT")
    if (previous_list_hash is None) != (previous_day_receipt_hash is None):
        raise ValueError("previous list/day receipt hashes must be supplied together")
    return canonical_json_sha256(
        {
            "schema_version": "advisory_historical_range_day_input_v2",
            "candidate_input_hash": require_sha256(candidate_input_hash, field_name="candidate_input_hash"),
            "candidate_artifact_ref": candidate_artifact_ref.model_dump(mode="json"),
            "previous_list_hash": (
                require_sha256(previous_list_hash, field_name="previous_list_hash")
                if previous_list_hash is not None
                else None
            ),
            "previous_day_receipt_hash": (
                require_sha256(previous_day_receipt_hash, field_name="previous_day_receipt_hash")
                if previous_day_receipt_hash is not None
                else None
            ),
            "list_semantics_hash": require_sha256(list_semantics_hash, field_name="list_semantics_hash"),
        }
    )


def build_day_input_hash_v3(
    *,
    candidate_input_hash: str,
    candidate_artifact_ref: HistoricalRangeArtifactRefV1,
    decision_mark_set_ref: HistoricalRangeArtifactRefV1,
    decision_mark_policy_hash: str,
    previous_list_hash: str | None,
    previous_day_receipt_ref: HistoricalRangeArtifactRefV1 | None,
    list_semantics_version: str,
    list_semantics_hash: str,
) -> str:
    """Close every R3 day input without changing the retained R1/R2 hash contract."""
    if candidate_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT:
        raise ValueError("candidate_artifact_ref must reference CANDIDATE_ARTIFACT")
    if decision_mark_set_ref.artifact_kind is not HistoricalRangeArtifactKind.DECISION_MARK_SET:
        raise ValueError("decision_mark_set_ref must reference DECISION_MARK_SET")
    if (previous_list_hash is None) != (previous_day_receipt_ref is None):
        raise ValueError("previous list/day receipt inputs must be supplied together")
    if (
        previous_day_receipt_ref is not None
        and previous_day_receipt_ref.artifact_kind is not HistoricalRangeArtifactKind.DAY_RECEIPT
    ):
        raise ValueError("previous_day_receipt_ref must reference DAY_RECEIPT")
    return canonical_json_sha256(
        {
            "schema_version": "advisory_historical_range_day_input_v3",
            "candidate_input_hash": require_sha256(candidate_input_hash, field_name="candidate_input_hash"),
            "candidate_artifact_ref": candidate_artifact_ref.model_dump(mode="json"),
            "decision_mark_set_ref": decision_mark_set_ref.model_dump(mode="json"),
            "decision_mark_policy_hash": require_sha256(
                decision_mark_policy_hash,
                field_name="decision_mark_policy_hash",
            ),
            "previous_list_hash": (
                require_sha256(previous_list_hash, field_name="previous_list_hash")
                if previous_list_hash is not None
                else None
            ),
            "previous_day_receipt_ref": (
                previous_day_receipt_ref.model_dump(mode="json")
                if previous_day_receipt_ref is not None
                else None
            ),
            "list_semantics_version": _nonblank(
                list_semantics_version,
                field_name="list_semantics_version",
            ),
            "list_semantics_hash": require_sha256(list_semantics_hash, field_name="list_semantics_hash"),
        }
    )


def build_catalog_member_chain_hash(
    *,
    members: Sequence[HistoricalRangeSourceRevisionMemberV1],
    ordered_requirement_ids: Sequence[str],
) -> str:
    by_requirement = {item.requirement_id: item for item in members}
    ordered_ids = tuple(_nonblank(item, field_name="requirement_id") for item in ordered_requirement_ids)
    if len(ordered_ids) != len(set(ordered_ids)):
        raise ValueError("ordered_requirement_ids must be duplicate-free")
    if set(by_requirement) != set(ordered_ids) or len(by_requirement) != len(members):
        raise ValueError("catalog members must resolve every ordered requirement exactly once")
    chain_hash = canonical_json_sha256([])
    for ordinal, requirement_id in enumerate(ordered_ids, start=1):
        member = by_requirement[requirement_id]
        chain_hash = append_catalog_member_chain_hash(
            previous_chain_hash=chain_hash,
            ordinal=ordinal,
            member=member,
        )
    return chain_hash


def append_catalog_member_chain_hash(
    *,
    previous_chain_hash: str,
    ordinal: int,
    member: HistoricalRangeSourceRevisionMemberV1,
) -> str:
    if ordinal < 1:
        raise ValueError("catalog member ordinal must be positive")
    return canonical_json_sha256(
        {
            "schema_version": "advisory_historical_range_catalog_member_chain_v1",
            "previous_chain_hash": require_sha256(previous_chain_hash, field_name="previous_chain_hash"),
            "ordinal": ordinal,
            "requirement_id": member.requirement_id,
            "revision_id": member.revision_id,
            "revision_hash": member.revision_hash,
        }
    )


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


def build_day_receipt_payload_v2(
    *,
    range_run_id: str,
    day_run_id: str,
    terminal_status: HistoricalRangeDayStatus,
    day_input_hash: str,
    candidate_artifact_ref: HistoricalRangeArtifactRefV1,
    decision_mark_set_ref: HistoricalRangeArtifactRefV1,
    previous_day_receipt_ref: HistoricalRangeArtifactRefV1 | None,
    list_version: HistoricalRangeListVersionFactV1,
    items: Sequence[HistoricalRangeListItemFactV1],
    episodes: Sequence[HistoricalRangeEpisodeSnapshotFactV1],
    reason_codes: Sequence[str] = (),
) -> dict[str, Any]:
    if terminal_status not in {
        HistoricalRangeDayStatus.COMPLETE,
        HistoricalRangeDayStatus.VALID_NO_CANDIDATE,
    }:
        raise ValueError("R3 day receipt terminal_status must be a successful day state")
    payload = HistoricalRangeDayReceiptPayloadV2(
        range_run_id=range_run_id,
        day_run_id=day_run_id,
        terminal_status=terminal_status.value,
        day_input_hash=day_input_hash,
        candidate_artifact_ref=candidate_artifact_ref,
        decision_mark_set_ref=decision_mark_set_ref,
        previous_day_receipt_ref=previous_day_receipt_ref,
        list_version=list_version,
        items=tuple(items),
        episode_snapshots=tuple(episodes),
        reason_codes=tuple(reason_codes),
    )
    return payload.model_dump(mode="json")


def _topological_requirement_order(
    requirements: Sequence[HistoricalRangeSourceRequirementV1],
) -> tuple[HistoricalRangeSourceRequirementV1, ...]:
    by_id = {item.requirement_id: item for item in requirements}
    if len(by_id) != len(requirements):
        raise ValueError("source requirement ids must be unique")
    missing_dependencies = sorted(
        {
            dependency
            for item in requirements
            for dependency in item.depends_on_requirement_ids
            if dependency not in by_id
        }
    )
    if missing_dependencies:
        raise ValueError(f"source requirement dependencies do not exist: {missing_dependencies}")
    remaining = {item_id: set(item.depends_on_requirement_ids) for item_id, item in by_id.items()}
    ordered: list[HistoricalRangeSourceRequirementV1] = []
    while remaining:
        ready = sorted(item_id for item_id, dependencies in remaining.items() if not dependencies)
        if not ready:
            raise ValueError("source requirement dependency graph contains a cycle")
        for item_id in ready:
            ordered.append(by_id[item_id])
            remaining.pop(item_id)
        for dependencies in remaining.values():
            dependencies.difference_update(ready)
    return tuple(ordered)


BATCH_TRANSITIONS: dict[HistoricalRangeBatchStatus, frozenset[HistoricalRangeBatchStatus]] = {
    HistoricalRangeBatchStatus.PLANNING: frozenset(
        {
            HistoricalRangeBatchStatus.QUEUED,
            HistoricalRangeBatchStatus.WAITING_INPUT,
            HistoricalRangeBatchStatus.DEDUPLICATED,
            HistoricalRangeBatchStatus.FAILED,
            HistoricalRangeBatchStatus.CANCELLED,
        }
    ),
    HistoricalRangeBatchStatus.QUEUED: frozenset(
        {
            HistoricalRangeBatchStatus.RUNNING,
            HistoricalRangeBatchStatus.CANCELLING,
            HistoricalRangeBatchStatus.CANCELLED,
        }
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
            HistoricalRangeBatchStatus.CANCELLING,
            HistoricalRangeBatchStatus.CANCELLED,
        }
    ),
    HistoricalRangeBatchStatus.WAITING_INPUT: frozenset(
        {
            HistoricalRangeBatchStatus.PLANNING,
            HistoricalRangeBatchStatus.RUNNING,
            HistoricalRangeBatchStatus.FAILED,
            HistoricalRangeBatchStatus.CANCELLING,
            HistoricalRangeBatchStatus.CANCELLED,
        }
    ),
    HistoricalRangeBatchStatus.CANCELLING: frozenset({HistoricalRangeBatchStatus.CANCELLED}),
    HistoricalRangeBatchStatus.COMPLETED: frozenset(),
    HistoricalRangeBatchStatus.FAILED: frozenset(),
    HistoricalRangeBatchStatus.CANCELLED: frozenset(),
    HistoricalRangeBatchStatus.DEDUPLICATED: frozenset(),
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
    HistoricalRangeOperationStatus.QUEUED: frozenset(
        {HistoricalRangeOperationStatus.RUNNING, HistoricalRangeOperationStatus.RETRYABLE_FAILED}
    ),
    HistoricalRangeOperationStatus.RUNNING: frozenset(
        {
            HistoricalRangeOperationStatus.COMPLETED,
            HistoricalRangeOperationStatus.WAITING_INPUT,
            HistoricalRangeOperationStatus.RETRYABLE_FAILED,
            HistoricalRangeOperationStatus.FAILED,
        }
    ),
    HistoricalRangeOperationStatus.RETRYABLE_FAILED: frozenset({HistoricalRangeOperationStatus.RUNNING}),
    HistoricalRangeOperationStatus.WAITING_INPUT: frozenset({HistoricalRangeOperationStatus.RUNNING}),
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
    if (
        target is HistoricalRangeBatchStatus.FAILED
        and current is not HistoricalRangeBatchStatus.PLANNING
        and (successful_day_count != 0 or failed_program_count != program_count or recoverable_program_count != 0)
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
    evaluation_window_type: HistoricalRangeEvaluationWindowType,
    horizon_trade_days: int,
    historical_range_policy_bundle_hash: str,
) -> str:
    if evaluation_window_type is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE:
        if subject_type is not HistoricalRangeOutcomeSubjectType.EPISODE or horizon_trade_days != 0:
            raise ValueError("episode lifecycle identity requires EPISODE and horizon sentinel zero")
    elif subject_type is HistoricalRangeOutcomeSubjectType.EPISODE or horizon_trade_days < 1:
        raise ValueError("fixed-horizon identity excludes EPISODE and requires positive horizon")
    return derive_prefixed_id(
        "ahro",
        {
            "subject_type": subject_type.value,
            "subject_id": _nonblank(subject_id, field_name="subject_id"),
            "projection": projection.value,
            "evaluation_window_type": evaluation_window_type.value,
            "horizon_trade_days": horizon_trade_days,
            "historical_range_policy_bundle_hash": require_sha256(
                historical_range_policy_bundle_hash,
                field_name="historical_range_policy_bundle_hash",
            ),
        },
    )


HistoricalRangeFrozenProgramV1.model_rebuild()
