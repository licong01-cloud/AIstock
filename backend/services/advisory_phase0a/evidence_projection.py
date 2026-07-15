"""Advisory-owned read-only DTOs for immutable selection/package evidence.

This module deliberately contains the parser boundary for records owned by the
Selection, Simulation and StrategyPackage schemas.  It is intentionally
structural: callers receive immutable data values, never another subsystem's
repository, service, validator, asset loader or inference provider.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from copy import deepcopy
import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any, Literal, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from backend.execution_algos.board_lot import board_lot_rule

from .policy import canonicalize


class AdvisoryEvidenceProjectionError(RuntimeError):
    """A persisted evidence row cannot be consumed as immutable Advisory input."""


REASON_PROJECTED_DSE_V2_INVALID = "ADVISORY_PHASE0A_PROJECTED_DSE_V2_INVALID"


class ProjectedHistoricalEvidenceV2ValidationError(AdvisoryEvidenceProjectionError):
    """Strict DSE v2 validation failed without exposing the persisted payload."""

    def __init__(self, *, context: dict[str, Any]) -> None:
        super().__init__(REASON_PROJECTED_DSE_V2_INVALID)
        self.reason_code = REASON_PROJECTED_DSE_V2_INVALID
        self.context = canonicalize(context)


class ProjectedAlphaMode(str, Enum):
    SINGLE_ALPHA = "single_alpha"
    MULTI_ALPHA = "multi_alpha"


class ProjectedPackageAssetType(str, Enum):
    MODEL_WEIGHT = "model_weight"
    FACTOR_CODE = "factor_code"
    FACTOR_SCHEMA = "factor_schema"
    MODEL_CODE = "model_code"
    FEATURE_ORDER = "feature_order"
    TRAIN_CONFIG = "train_config"
    PREPROCESSOR = "preprocessor"
    PREDICTION_SCHEMA = "prediction_schema"
    EXECUTION_CONFIG = "execution_config"
    RISK_POLICY = "risk_policy"
    VALIDATION_REPORT = "validation_report"
    OTHER = "other"


@dataclass(frozen=True)
class ProjectedAdvisoryProgram:
    program_id: str
    target_count: int
    review_policy: dict[str, Any]


@dataclass(frozen=True)
class ProjectedBindingVersion:
    binding_version_id: str
    program_id: str
    package_mode: str
    package_ids: list[str]
    effective_from_trade_date: date | None
    effective_to_trade_date: date | None
    activation_status: str
    binding_payload_hash: str | None = None
    runtime_config_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectedAlphaComponent:
    alpha_id: str
    factor_ids: list[str] = field(default_factory=list)
    model_id: str | None = None
    model_ref: str | None = None
    component_weight: float | None = None
    score_direction: str | None = None
    score_normalization: str | None = None
    holding_period: int | None = None
    rebalance_frequency: str | None = None


@dataclass(frozen=True)
class ProjectedCombinationPolicy:
    method: str
    payload: dict[str, Any] = field(default_factory=dict)

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        _ = mode
        return {"method": self.method, **canonicalize(self.payload)}


@dataclass(frozen=True)
class ProjectedManifestAsset:
    asset_type: str
    asset_ref: str
    asset_sha256: str


@dataclass(frozen=True)
class ProjectedPackageManifest:
    package_id: str
    manifest_sha256: str
    alpha_mode: ProjectedAlphaMode
    style_family: str | None = None
    source_evidence: dict[str, Any] = field(default_factory=dict)
    alpha_components: tuple[ProjectedAlphaComponent, ...] = ()
    alpha_combination_policy: ProjectedCombinationPolicy | None = None
    backtest_context: dict[str, Any] = field(default_factory=dict)
    declared_runtime_assets: tuple[ProjectedManifestAsset, ...] = ()


@dataclass(frozen=True)
class ProjectedPackage:
    package_id: str
    manifest_sha256: str
    alpha_mode: ProjectedAlphaMode
    source_id: str
    manifest: ProjectedPackageManifest
    data_vintage: date | None = None
    asset_closure_hash: str | None = None
    lineage_hash: str | None = None


@dataclass(frozen=True)
class ProjectedPackageAsset:
    package_id: str
    asset_type: ProjectedPackageAssetType
    asset_ref: str
    asset_sha256: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
    asset_role: str = "governed_asset"
    asset_id: int | None = None
    created_at: datetime | None = None


class ProjectedSelectionMode(str, Enum):
    SINGLE_PACKAGE = "single_package"
    INTERSECTION = "intersection"
    UNION = "union"
    WEIGHTED_FUSION = "weighted_fusion"


class ProjectedSelectionRunStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    VALID_NO_CANDIDATE = "VALID_NO_CANDIDATE"


class ProjectedSelectionCandidate(BaseModel):
    """Advisory-owned DTO kept field-for-field compatible with SelectionCandidate."""

    model_config = ConfigDict(extra="forbid")

    symbol: str
    score: float
    rank: int = Field(gt=0)
    target_weight: float | None = Field(default=None, gt=0)
    target_quantity: int | None = Field(default=None, ge=0)
    reference_price: float | None = Field(default=None, gt=0)
    stock_name: str | None = None
    selection_entry_price: float | None = Field(default=None, gt=0)
    selection_entry_price_source: str | None = None
    selection_entry_price_time: str | None = None
    signal_ref_price: float | None = Field(default=None, gt=0)
    previous_close: float | None = Field(default=None, gt=0)
    volume: float | None = Field(default=None, ge=0)
    current_price: float | None = Field(default=None, gt=0)
    current_price_source: str | None = None
    current_price_time: str | None = None
    suggested_entry_price_band: dict[str, Any] | None = None
    suggested_stop_loss_zone: dict[str, Any] | None = None
    guidance_status: str | None = None
    price_guard_policy_sha256: str | None = None
    component_scores: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = None

    @field_validator("symbol")
    @classmethod
    def _symbol_required(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized

    @model_validator(mode="after")
    def _target_quantity_board_lot(self) -> "ProjectedSelectionCandidate":
        if self.target_quantity is not None:
            _validate_projected_target_quantity(self.symbol, self.target_quantity)
        return self


class ProjectedSelectionExclusion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    score: float
    rank: int = Field(gt=0)
    reason: str
    source: str
    context: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def _symbol_required(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized


class ProjectedSelectionRun(BaseModel):
    """Read-only SelectionRun parity DTO without importing shared runtime modules."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    mode: ProjectedSelectionMode
    trade_date: date
    data_source: str
    package_ids: list[str]
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    status: ProjectedSelectionRunStatus = ProjectedSelectionRunStatus.RUNNING
    package_results: dict[str, list[ProjectedSelectionCandidate]] = Field(
        default_factory=dict
    )
    aggregate_results: list[ProjectedSelectionCandidate] = Field(default_factory=list)
    excluded_results: dict[str, list[ProjectedSelectionExclusion]] = Field(
        default_factory=dict
    )
    manifest_sha256_by_package: dict[str, str] = Field(default_factory=dict)
    valid_no_candidate: bool = False
    no_candidate_reason: str | None = None
    error: dict[str, Any] | None = None
    created_at: datetime
    completed_at: datetime | None = None


def _validate_projected_target_quantity(symbol: str, quantity: int) -> None:
    if quantity == 0:
        return
    minimum, increment = board_lot_rule(symbol)
    if quantity >= minimum and quantity % increment == 0:
        return
    raise ValueError(
        f"target_quantity must follow board-lot rules for {symbol}: "
        f"min_qty={minimum}, increment={increment}"
    )


@dataclass(frozen=True)
class ProjectedDailySelectionEvidence:
    evidence_id: str
    target_trade_date: date
    cutoff_date: date | None
    package_id: str
    manifest_sha256: str
    runtime_profile_version_id: str
    runtime_profile_hash: str
    source_type: str
    data_source: str
    candidate_count: int
    artifact_hash: str
    evidence_payload_json: dict[str, Any]
    created_at: datetime | None = None


@dataclass(frozen=True)
class ProjectedSelectionScoreArtifact:
    artifact_id: str
    package_id: str
    manifest_sha256: str
    trade_date: date
    data_source: str
    runtime_config_hash: str
    scores_json: list[dict[str, Any]]
    artifact_sha256: str | None
    score_count: int
    universe_count: int
    top_score_symbol: str | None
    status: str
    metadata: dict[str, Any] = field(default_factory=dict)
    artifact_contract_version: str | None = None
    artifact_payload_sha256: str | None = None
    artifact_input_context_hash: str | None = None
    source_revision_set_hash: str | None = None
    asset_closure_hash: str | None = None
    created_at: datetime | None = None

    def canonical_v2_header(self, *, score_hash: str | None = None) -> dict[str, Any]:
        if self.artifact_contract_version != "selection_score_artifact_v2":
            raise AdvisoryEvidenceProjectionError("selection score artifact is not v2")
        return {
            "schema_version": "selection_score_artifact_v2",
            "package_id": self.package_id,
            "manifest_sha256": self.manifest_sha256,
            "trade_date": self.trade_date,
            "data_source": self.data_source,
            "runtime_config_hash": self.runtime_config_hash,
            "artifact_sha256": score_hash
            or self.artifact_sha256
            or canonical_evidence_json_sha256(self.scores_json),
            "score_count": self.score_count,
            "universe_count": self.universe_count,
            "top_score_symbol": self.top_score_symbol,
            "status": self.status,
            "authority_scope": self.metadata.get("authority_scope"),
            "candidate_outcome": self.metadata.get("candidate_outcome"),
            "artifact_input_context_hash": self.artifact_input_context_hash,
            "source_revision_set_hash": self.source_revision_set_hash,
            "asset_closure_hash": self.asset_closure_hash,
            "provider_semantics_id": self.metadata.get("provider_semantics_id"),
            "provider_semantics_hash": self.metadata.get("provider_semantics_hash"),
            "multi_alpha_parent_parity_hash": self.metadata.get(
                "multi_alpha_parent_parity_hash"
            ),
        }


@dataclass(frozen=True)
class ProjectedProspectiveEvidenceContract:
    prospective_eligible: bool


@dataclass(frozen=True)
class ProjectedDailyEvidenceV2:
    phase0a_candidate_lineage: dict[str, Any]
    evidence_contract: ProjectedProspectiveEvidenceContract


@dataclass(frozen=True)
class ProjectedHistoricalEvidenceV2:
    evidence_contract: dict[str, Any]
    phase0a_candidate_lineage: dict[str, Any]
    phase0a_package_lineage: dict[str, Any]
    phase0a_effective_config_chain: dict[str, Any]
    decision_clock: dict[str, Any]
    phase0a_source_evidence: list[dict[str, Any]]
    selected_candidates: list[dict[str, Any]]
    candidate_outcome: str


class AdvisoryProgramProjectionReader(Protocol):
    def get_program(self, program_id: str) -> ProjectedAdvisoryProgram: ...

    def list_binding_versions(
        self, program_id: str
    ) -> list[ProjectedBindingVersion]: ...


class StrategyPackageProjectionReader(Protocol):
    def get(self, package_id: str) -> ProjectedPackage: ...

    def list_package_assets(
        self, package_id: str, *, protected_only: bool = False
    ) -> list[ProjectedPackageAsset]: ...


class DailySelectionEvidenceProjectionReader(Protocol):
    def get_daily_selection_evidence(
        self, evidence_id: str
    ) -> ProjectedDailySelectionEvidence: ...


class SelectionScoreArtifactProjectionReader(Protocol):
    def list(
        self,
        *,
        package_id: str,
        manifest_sha256: str | None = None,
        limit: int = 100,
    ) -> list[ProjectedSelectionScoreArtifact]: ...


class SelectionRunProjectionReader(Protocol):
    def get_run(self, run_id: str) -> ProjectedSelectionRun: ...


def canonical_evidence_json_sha256(payload: Any) -> str:
    """Canonical hash used for persisted evidence payload/readback comparison."""

    def normalize(value: Any) -> Any:
        if isinstance(value, BaseModel):
            return normalize(value.model_dump(mode="python"))
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise AdvisoryEvidenceProjectionError(
                    "evidence timestamps must be timezone-aware"
                )
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Mapping):
            return {
                str(key): normalize(item)
                for key, item in sorted(value.items(), key=lambda item: str(item[0]))
            }
        if isinstance(value, tuple | list):
            return [normalize(item) for item in value]
        if isinstance(value, float):
            if not math.isfinite(value):
                raise AdvisoryEvidenceProjectionError(
                    "evidence payload cannot contain non-finite floats"
                )
            return value
        return value

    encoded = json.dumps(
        normalize(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def projected_manifest_json_sha256(manifest_json: Mapping[str, Any]) -> str:
    """Hash persisted manifest JSON without importing StrategyPackage runtime modules."""

    payload = deepcopy(dict(manifest_json))
    payload["manifest_sha256"] = None
    payload["package_status"] = None
    payload = _drop_projected_empty_asset_fields(payload)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _drop_projected_empty_asset_fields(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    cleaned: dict[str, Any] = {}
    for key, item in value.items():
        if key == "factor_set" and isinstance(item, list):
            cleaned[key] = [
                _drop_projected_empty_asset_defaults(asset) for asset in item
            ]
        elif key == "model_asset" and isinstance(item, list):
            cleaned[key] = [
                _drop_projected_empty_asset_defaults(asset) for asset in item
            ]
        elif key == "model_asset" and isinstance(item, dict):
            cleaned[key] = _drop_projected_empty_asset_defaults(item)
        elif key == "runtime_assets" and item in (None, {}, []):
            continue
        else:
            cleaned[key] = item
    return cleaned


def _drop_projected_empty_asset_defaults(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    empty_asset_keys = {"asset_ref", "sha256", "size_bytes", "source_uri"}
    cleaned: dict[str, Any] = {}
    for key, item in value.items():
        if key in empty_asset_keys and item in (None, "", [], {}):
            continue
        if key == "model_code_assets" and item in (None, [], {}):
            continue
        if key == "model_code_required" and item is False:
            continue
        cleaned[key] = item
    return cleaned


def _require_sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a lowercase 64-character sha256 digest")
    return normalized


class _EvidenceCaptureMode(str, Enum):
    DISABLED = "DISABLED"
    PROSPECTIVE = "PROSPECTIVE"


class _ProspectiveExecutionOrigin(str, Enum):
    SELECTION_CENTER = "SELECTION_CENTER"
    ADVISORY_RUN = "ADVISORY_RUN"
    PAPER = "PAPER"
    SIMULATION = "SIMULATION"
    REPLAY = "REPLAY"
    PREVIEW = "PREVIEW"


class _CandidateStageName(str, Enum):
    ALPHA_RAW = "alpha_raw"
    HMM_ADJUSTED = "hmm_adjusted"
    RISK_POLICY_ADJUSTED = "risk_policy_adjusted"
    SELECTION_EFFECTIVE = "selection_effective"
    ADVISORY_MODEL = "advisory_model"


class _StageReceiptStatus(str, Enum):
    COMPLETE = "COMPLETE"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    FAILED = "FAILED"


class _EvidenceContractV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["daily_selection_evidence_v2"] = (
        "daily_selection_evidence_v2"
    )
    capture_mode: _EvidenceCaptureMode
    capture_status: Literal["COMPLETE"] = "COMPLETE"
    execution_origin: _ProspectiveExecutionOrigin
    prospective_eligible: bool
    research_scope: Literal["HISTORICAL_RESEARCH_ONLY"]
    execution_prohibited: Literal[True]
    market_data_scope: Literal["DB_HISTORICAL"]
    serializer_version: str
    producer_code_release_id: str
    producer_code_release_hash: str
    captured_at: datetime
    reason_codes: list[str] = Field(default_factory=list)

    @field_validator("serializer_version", "producer_code_release_id")
    @classmethod
    def _required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("field is required")
        return normalized

    @field_validator("producer_code_release_hash")
    @classmethod
    def _code_hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="producer_code_release_hash")

    @field_validator("captured_at")
    @classmethod
    def _aware_capture_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("captured_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def _complete_capture_is_consistent(self) -> "_EvidenceContractV2":
        if self.capture_mode != _EvidenceCaptureMode.PROSPECTIVE:
            raise ValueError("v2 evidence contract requires prospective capture mode")
        if self.reason_codes:
            raise ValueError(
                "complete v2 evidence contract cannot contain reason_codes"
            )
        expected_eligibility = (
            self.execution_origin == _ProspectiveExecutionOrigin.ADVISORY_RUN
        )
        if self.prospective_eligible != expected_eligibility:
            raise ValueError("prospective_eligible must match execution_origin")
        if self.execution_origin != _ProspectiveExecutionOrigin.ADVISORY_RUN:
            raise ValueError(
                "v2 prospective evidence is restricted to historical ADVISORY_RUN research"
            )
        return self


class _DecisionClockEvidenceV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_as_of_trade_date: date
    selection_as_of_trade_date: date
    target_trade_date: date
    effective_entry_trade_date: date
    score_trade_date: date
    reference_price_trade_date: date
    requested_selection_as_of_trade_date: date
    requested_cutoff_date: date
    effective_cutoff_date: date
    decision_cutoff_ts: datetime
    data_available_at: datetime
    decision_generated_at: datetime
    timezone: Literal["Asia/Shanghai"]
    calendar_version: str
    calendar_hash: str
    calendar_source: str
    is_immediately_previous_trade_date: bool
    immediate_after_data_refresh: bool
    decision_clock_hash: str | None = None

    @field_validator("calendar_version", "calendar_source")
    @classmethod
    def _clock_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("field is required")
        return normalized

    @field_validator("calendar_hash", "decision_clock_hash")
    @classmethod
    def _clock_hashes(cls, value: str | None, info: Any) -> str | None:
        if value is None and info.field_name == "decision_clock_hash":
            return None
        return _require_sha256(str(value or ""), field_name=info.field_name)

    @field_validator("decision_cutoff_ts", "data_available_at", "decision_generated_at")
    @classmethod
    def _clock_timestamps_are_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("decision clock timestamps must be timezone-aware")
        return value

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"decision_clock_hash"})

    @model_validator(mode="after")
    def _clock_is_coherent(self) -> "_DecisionClockEvidenceV2":
        if self.decision_as_of_trade_date != self.selection_as_of_trade_date:
            raise ValueError("decision and selection as-of trade dates must match")
        if self.score_trade_date != self.decision_as_of_trade_date:
            raise ValueError("score_trade_date must match decision_as_of_trade_date")
        if self.reference_price_trade_date != self.decision_as_of_trade_date:
            raise ValueError(
                "reference_price_trade_date must match decision_as_of_trade_date"
            )
        if self.effective_cutoff_date != self.decision_as_of_trade_date:
            raise ValueError(
                "effective_cutoff_date must match decision_as_of_trade_date"
            )
        if self.requested_cutoff_date > self.effective_cutoff_date:
            raise ValueError(
                "requested_cutoff_date cannot be after effective_cutoff_date"
            )
        if self.target_trade_date <= self.decision_as_of_trade_date:
            raise ValueError(
                "target_trade_date must be after decision_as_of_trade_date"
            )
        if self.effective_entry_trade_date != self.target_trade_date:
            raise ValueError("effective_entry_trade_date must match target_trade_date")
        if not self.is_immediately_previous_trade_date:
            raise ValueError(
                "decision clock requires immediate next-trading-day relation"
            )
        if self.data_available_at > self.decision_cutoff_ts:
            raise ValueError("data_available_at cannot be after decision_cutoff_ts")
        if self.decision_generated_at < self.decision_cutoff_ts:
            raise ValueError(
                "decision_generated_at cannot be before decision_cutoff_ts"
            )
        digest = canonical_evidence_json_sha256(self.canonical_payload())
        if self.decision_clock_hash is not None and self.decision_clock_hash != digest:
            raise ValueError(
                "decision_clock_hash does not match canonical decision clock"
            )
        object.__setattr__(self, "decision_clock_hash", digest)
        return self


class _EffectiveConfigChainV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    binding_base_config: dict[str, Any]
    binding_base_config_hash: str
    binding_base_source_id: str
    binding_base_source_version: str
    binding_base_source_hash: str
    binding_base_available_at: datetime
    binding_base_effective_from_trade_date: date
    binding_base_effective_to_trade_date: date | None = None
    request_override_config: dict[str, Any]
    request_override_hash: str
    date_enforced_config: dict[str, Any]
    date_enforced_version: str
    date_enforced_hash: str
    selection_normalized_config: dict[str, Any]
    selection_normalized_config_hash: str
    package_effective_config: dict[str, Any]
    package_effective_config_hash: str
    runtime_variant_id: str | None = None
    runtime_profile_version_id: str
    runtime_profile_hash: str
    selection_adapter_version: str
    query_template_version: str
    provider_version: str
    code_release_id: str
    code_release_hash: str
    overridden_field_paths_by_layer: dict[str, list[str]]
    final_effective_config_hash: str
    chain_hash: str | None = None

    @field_validator(
        "binding_base_config_hash",
        "binding_base_source_hash",
        "request_override_hash",
        "date_enforced_hash",
        "selection_normalized_config_hash",
        "package_effective_config_hash",
        "runtime_profile_hash",
        "code_release_hash",
        "final_effective_config_hash",
        "chain_hash",
    )
    @classmethod
    def _config_hashes(cls, value: str | None, info: Any) -> str | None:
        if value is None and info.field_name == "chain_hash":
            return None
        return _require_sha256(str(value or ""), field_name=info.field_name)

    @field_validator(
        "binding_base_source_id",
        "binding_base_source_version",
        "date_enforced_version",
        "runtime_profile_version_id",
        "selection_adapter_version",
        "query_template_version",
        "provider_version",
        "code_release_id",
    )
    @classmethod
    def _config_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("field is required")
        return normalized

    @field_validator("binding_base_available_at")
    @classmethod
    def _binding_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("binding_base_available_at must be timezone-aware")
        return value

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"chain_hash"})

    @model_validator(mode="after")
    def _config_chain_is_consistent(self) -> "_EffectiveConfigChainV2":
        for config_field, hash_field in (
            ("binding_base_config", "binding_base_config_hash"),
            ("request_override_config", "request_override_hash"),
            ("date_enforced_config", "date_enforced_hash"),
            ("selection_normalized_config", "selection_normalized_config_hash"),
            ("package_effective_config", "package_effective_config_hash"),
        ):
            if canonical_evidence_json_sha256(getattr(self, config_field)) != getattr(
                self, hash_field
            ):
                raise ValueError(f"{hash_field} does not match {config_field}")
        if (
            canonical_evidence_json_sha256(self.package_effective_config)
            != self.final_effective_config_hash
        ):
            raise ValueError(
                "final_effective_config_hash does not match package_effective_config"
            )
        digest = canonical_evidence_json_sha256(self.canonical_payload())
        if self.chain_hash is not None and self.chain_hash != digest:
            raise ValueError("chain_hash does not match effective config chain")
        object.__setattr__(self, "chain_hash", digest)
        return self


_UNIVERSE_LAYER_NAMES = (
    "listed_universe",
    "seasoned_universe",
    "pit_st_delist_risk_universe",
    "package_eligible_universe",
    "risk_can_buy_universe",
    "tradability_industry_universe",
)


class _UniverseLayerReceiptV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    layer: str
    status: Literal["FORMAL_READY", "PARTIAL", "RESEARCH_ONLY", "NOT_APPLICABLE"]
    policy_id: str | None = None
    policy_version: str | None = None
    policy_hash: str | None = None
    policy_available_at: datetime | None = None
    policy_effective_from_trade_date: date | None = None
    policy_effective_to_trade_date: date | None = None
    input_count: int = Field(ge=0)
    output_count: int = Field(ge=0)
    excluded_count: int = Field(ge=0)
    exclusion_reason_counts: dict[str, int] = Field(default_factory=dict)
    input_symbol_set_hash: str | None = None
    output_symbol_set_hash: str | None = None
    source_revision_refs: list[dict[str, Any]] = Field(default_factory=list)
    source_revision_set_hash: str | None = None
    available_at: datetime | None = None
    reason_codes: list[str] = Field(default_factory=list)

    @field_validator("layer")
    @classmethod
    def _known_layer(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if normalized not in _UNIVERSE_LAYER_NAMES:
            raise ValueError("unsupported universe layer")
        return normalized

    @field_validator(
        "policy_hash",
        "input_symbol_set_hash",
        "output_symbol_set_hash",
        "source_revision_set_hash",
    )
    @classmethod
    def _optional_hash(cls, value: str | None, info: Any) -> str | None:
        if value is None:
            return None
        return _require_sha256(value, field_name=info.field_name)

    @field_validator("policy_available_at", "available_at")
    @classmethod
    def _universe_timestamps(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("universe timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def _universe_counts_reconcile(self) -> "_UniverseLayerReceiptV2":
        if self.input_count != self.output_count + self.excluded_count:
            raise ValueError("universe layer counts must reconcile")
        if any(value < 0 for value in self.exclusion_reason_counts.values()):
            raise ValueError("universe exclusion reason counts must be non-negative")
        if sum(self.exclusion_reason_counts.values()) != self.excluded_count:
            raise ValueError(
                "universe exclusion reason counts must match excluded_count"
            )
        if self.status != "NOT_APPLICABLE":
            required = (
                self.policy_id,
                self.policy_version,
                self.policy_hash,
                self.policy_available_at,
                self.input_symbol_set_hash,
                self.output_symbol_set_hash,
                self.source_revision_set_hash,
                self.available_at,
            )
            if not all(value not in (None, "") for value in required):
                raise ValueError(
                    "executed universe layer is missing mandatory provenance"
                )
        return self


class _UniverseEvidenceV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    layers: list[_UniverseLayerReceiptV2]
    package_cohort: dict[str, Any]

    @model_validator(mode="after")
    def _all_layers_are_present(self) -> "_UniverseEvidenceV2":
        if tuple(item.layer for item in self.layers) != _UNIVERSE_LAYER_NAMES:
            raise ValueError("universe layers must be present once in canonical order")
        if not self.package_cohort:
            raise ValueError("package_cohort provenance is required")
        return self


class _SourceReadReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_role: str
    dataset_id: str
    partition_ref: str | None = None
    query_template_id: str | None = None
    query_template_version: str | None = None
    query_hash: str | None = None
    parameter_hash: str | None = None
    schema_fingerprint: str | None = None
    row_count: int = Field(ge=0)
    content_hash: str | None = None
    available_at: datetime | None = None
    first_observed_at: datetime | None = None
    refresh_job_ref: str | None = None
    phase1_availability_event_ref: str | None = None
    admissibility: str = "PROSPECTIVE_FIRST_OBSERVED"
    leg_id: str | None = None

    @field_validator("source_role", "dataset_id")
    @classmethod
    def _required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("field is required")
        return normalized

    @model_validator(mode="after")
    def _availability_is_explicit(self) -> "_SourceReadReceipt":
        if self.available_at is None and self.first_observed_at is None:
            raise ValueError(
                "source receipt requires available_at or first_observed_at"
            )
        for field_name, timestamp in (
            ("available_at", self.available_at),
            ("first_observed_at", self.first_observed_at),
        ):
            if timestamp is not None and timestamp.tzinfo is None:
                raise ValueError(f"source receipt {field_name} must be timezone-aware")
        if not str(self.admissibility or "").strip():
            raise ValueError("source receipt admissibility is required")
        return self


class _StageEvidenceReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage: _CandidateStageName
    status: _StageReceiptStatus
    input_count: int = Field(ge=0)
    output_count: int = Field(ge=0)
    excluded_count: int = Field(ge=0)
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    exclusions: list[dict[str, Any]] = Field(default_factory=list)
    semantic_payload: dict[str, Any] = Field(default_factory=dict)
    reason_codes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _counts_match_rows(self) -> "_StageEvidenceReceipt":
        if self.status == _StageReceiptStatus.COMPLETE and self.output_count != len(
            self.candidates
        ):
            raise ValueError("complete stage output_count must match candidate rows")
        if (
            self.status == _StageReceiptStatus.COMPLETE
            and self.input_count != self.output_count + self.excluded_count
        ):
            raise ValueError(
                "complete stage input_count must reconcile output_count and excluded_count"
            )
        if self.status == _StageReceiptStatus.NOT_APPLICABLE and self.candidates:
            raise ValueError("not-applicable stage cannot contain candidate rows")
        if self.status == _StageReceiptStatus.NOT_APPLICABLE and (
            self.output_count or self.excluded_count or self.exclusions
        ):
            raise ValueError("not-applicable stage cannot contain output or exclusions")
        return self


def _canonical_candidate_rows(
    candidates: list[ProjectedSelectionCandidate],
) -> list[dict[str, Any]]:
    rows = [
        {
            "symbol": candidate.symbol,
            "score": candidate.score,
            "rank": candidate.rank,
            "reason": candidate.reason,
            "component_scores": candidate.component_scores,
        }
        for candidate in candidates
    ]
    return sorted(rows, key=lambda item: (int(item["rank"]), str(item["symbol"])))


class _DailySelectionEvidenceV2Payload(BaseModel):
    """Advisory-owned strict parity contract for persisted DSE v2 payloads."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["daily_selection_evidence_v2"] = (
        "daily_selection_evidence_v2"
    )
    evidence_contract: _EvidenceContractV2
    decision_clock: _DecisionClockEvidenceV2
    point_in_time_context: dict[str, Any]
    runtime_profile: dict[str, Any]
    runtime_profile_binding: dict[str, Any]
    selection_artifact_config: dict[str, Any]
    phase0a_effective_config_chain: _EffectiveConfigChainV2
    phase0a_hmm_metadata: dict[str, Any]
    phase0a_risk_policy_metadata: dict[str, Any]
    phase0a_universe_evidence: _UniverseEvidenceV2
    phase0a_package_lineage: dict[str, Any]
    phase0a_asset_closure: list[dict[str, Any]]
    phase0a_source_evidence: list[_SourceReadReceipt]
    phase0a_candidate_lineage: dict[str, Any]
    phase0a_stage_evidence: dict[str, _StageEvidenceReceipt]
    candidate_outcome: Literal["CANDIDATES_PRESENT", "VALID_NO_CANDIDATE"]
    selected_candidates: list[dict[str, Any]]
    excluded_candidates: list[dict[str, Any]]

    @model_validator(mode="after")
    def _payload_is_consistent(self) -> "_DailySelectionEvidenceV2Payload":
        required_stages = {item.value for item in _CandidateStageName}
        if set(self.phase0a_stage_evidence) != required_stages:
            raise ValueError(
                "phase0a_stage_evidence must contain exactly five canonical stages"
            )
        for stage_name, receipt in self.phase0a_stage_evidence.items():
            if receipt.stage.value != stage_name:
                raise ValueError("stage receipt key does not match stage value")
        advisory = self.phase0a_stage_evidence[_CandidateStageName.ADVISORY_MODEL.value]
        if advisory.status != _StageReceiptStatus.NOT_APPLICABLE:
            raise ValueError("advisory_model must be not applicable in Phase 0A.2C")
        effective = self.phase0a_stage_evidence[
            _CandidateStageName.SELECTION_EFFECTIVE.value
        ]
        selected_models = [
            ProjectedSelectionCandidate.model_validate(item)
            for item in self.selected_candidates
        ]
        if effective.status != _StageReceiptStatus.COMPLETE:
            raise ValueError("selection_effective stage must be complete")
        if effective.candidates != _canonical_candidate_rows(selected_models):
            raise ValueError(
                "selection_effective candidates do not match selected_candidates"
            )
        all_exclusions: list[dict[str, Any]] = []
        for stage_name in (
            _CandidateStageName.HMM_ADJUSTED.value,
            _CandidateStageName.RISK_POLICY_ADJUSTED.value,
            _CandidateStageName.SELECTION_EFFECTIVE.value,
        ):
            all_exclusions.extend(self.phase0a_stage_evidence[stage_name].exclusions)
        canonical_excluded = sorted(
            self.excluded_candidates,
            key=lambda item: (
                str(item.get("source") or ""),
                str(item.get("reason") or ""),
                int(item.get("rank") or 0),
                str(item.get("symbol") or ""),
            ),
        )
        if all_exclusions != canonical_excluded:
            raise ValueError("stage exclusions do not match excluded_candidates")
        if (
            self.candidate_outcome == "CANDIDATES_PRESENT"
            and not self.selected_candidates
        ):
            raise ValueError("CANDIDATES_PRESENT requires selected candidates")
        if self.candidate_outcome == "VALID_NO_CANDIDATE" and self.selected_candidates:
            raise ValueError("VALID_NO_CANDIDATE cannot contain selected candidates")
        return self


class ProjectedHistoricalEvidenceV2Strict(_DailySelectionEvidenceV2Payload):
    """Public immutable form of the complete persisted DSE v2 contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)


def _parse_projected_dse_v2(
    payload: dict[str, Any],
) -> _DailySelectionEvidenceV2Payload | None:
    try:
        return _DailySelectionEvidenceV2Payload.model_validate(payload)
    except Exception:
        return None


def parse_projected_historical_evidence_v2_strict(
    payload: dict[str, Any],
) -> ProjectedHistoricalEvidenceV2Strict:
    """Validate the complete DSE v2 payload and preserve structured diagnostics."""

    try:
        return ProjectedHistoricalEvidenceV2Strict.model_validate(payload)
    except ValidationError as exc:
        errors = []
        for item in exc.errors(
            include_url=False, include_context=False, include_input=False
        ):
            errors.append(
                {
                    "field_path": list(map(str, item.get("loc", ()))),
                    "validation_type": str(item.get("type") or "validation_error"),
                }
            )
            if len(errors) == 20:
                break
        raise ProjectedHistoricalEvidenceV2ValidationError(
            context={
                "contract_version": (
                    str(payload.get("schema_version") or "missing")
                    if isinstance(payload, dict)
                    else "invalid_type"
                ),
                "error_count": exc.error_count(),
                "errors": errors,
            }
        ) from exc


def validate_projected_daily_evidence_v2(
    payload: dict[str, Any],
) -> ProjectedDailyEvidenceV2 | None:
    """Validate the complete DSE v2 contract, then expose the Advisory projection."""

    parsed = _parse_projected_dse_v2(payload)
    if parsed is None:
        return None
    lineage = parsed.phase0a_candidate_lineage
    required_lineage = (
        "selection_score_artifact_id",
        "selection_score_artifact_sha256",
        "selection_score_artifact_payload_sha256",
    )
    if any(not str(lineage.get(name) or "").strip() for name in required_lineage):
        return None
    return ProjectedDailyEvidenceV2(
        phase0a_candidate_lineage=canonicalize(lineage),
        evidence_contract=ProjectedProspectiveEvidenceContract(
            prospective_eligible=parsed.evidence_contract.prospective_eligible
        ),
    )


def validate_projected_historical_evidence_v2(
    payload: dict[str, Any],
) -> ProjectedHistoricalEvidenceV2 | None:
    """Validate the complete DSE v2 contract and expose historical research fields."""

    parsed = _parse_projected_dse_v2(payload)
    base = validate_projected_daily_evidence_v2(payload)
    if parsed is None or base is None or not parsed.phase0a_package_lineage:
        return None
    return ProjectedHistoricalEvidenceV2(
        evidence_contract=canonicalize(
            parsed.evidence_contract.model_dump(mode="json")
        ),
        phase0a_candidate_lineage=canonicalize(base.phase0a_candidate_lineage),
        phase0a_package_lineage=canonicalize(parsed.phase0a_package_lineage),
        phase0a_effective_config_chain=canonicalize(
            parsed.phase0a_effective_config_chain.model_dump(mode="json")
        ),
        decision_clock=canonicalize(parsed.decision_clock.model_dump(mode="json")),
        phase0a_source_evidence=canonicalize(
            [item.model_dump(mode="json") for item in parsed.phase0a_source_evidence]
        ),
        selected_candidates=canonicalize(parsed.selected_candidates),
        candidate_outcome=parsed.candidate_outcome,
    )


def projected_manifest_asset_keys(manifest: Any) -> set[tuple[str, str, str]]:
    """Read declared immutable runtime closure members without invoking a validator.

    The function only projects refs and hashes already frozen in a manifest. It
    does not load assets, calculate a new closure hash, or inspect a model.
    """

    declared = getattr(manifest, "declared_runtime_assets", ())
    if declared:
        return {
            (str(item.asset_type), str(item.asset_ref), str(item.asset_sha256))
            for item in declared
            if str(item.asset_ref or "").strip()
            and str(item.asset_sha256 or "").strip()
        }

    expected: set[tuple[str, str, str]] = set()
    for factor in list(getattr(manifest, "factor_set", []) or []):
        asset_ref = str(getattr(factor, "asset_ref", "") or "")
        sha256 = str(getattr(factor, "sha256", "") or "")
        if not asset_ref or not sha256:
            raise AdvisoryEvidenceProjectionError(
                "manifest factor closure member is missing"
            )
        expected.add((ProjectedPackageAssetType.FACTOR_CODE.value, asset_ref, sha256))
    model_asset = getattr(manifest, "model_asset", None)
    model_assets = model_asset if isinstance(model_asset, list) else [model_asset]
    for model in [item for item in model_assets if item is not None]:
        asset_ref = str(getattr(model, "asset_ref", "") or "")
        sha256 = str(getattr(model, "sha256", "") or "")
        if not asset_ref or not sha256:
            raise AdvisoryEvidenceProjectionError(
                "manifest model closure member is missing"
            )
        expected.add((ProjectedPackageAssetType.MODEL_WEIGHT.value, asset_ref, sha256))
        for code_asset in list(getattr(model, "model_code_assets", []) or []):
            code_ref = str(getattr(code_asset, "asset_ref", "") or "")
            code_sha = str(getattr(code_asset, "sha256", "") or "")
            if not code_ref or not code_sha:
                raise AdvisoryEvidenceProjectionError(
                    "manifest model-code closure member is missing"
                )
            expected.add(
                (ProjectedPackageAssetType.MODEL_CODE.value, code_ref, code_sha)
            )
    runtime_assets = getattr(manifest, "runtime_assets", None)
    alpha158 = (
        getattr(runtime_assets, "alpha158", None)
        if runtime_assets is not None
        else None
    )
    if alpha158 is not None and bool(getattr(alpha158, "enabled", False)):
        asset_ref = str(getattr(alpha158, "asset_ref", "") or "")
        sha256 = str(getattr(alpha158, "sha256", "") or "")
        if not asset_ref or not sha256:
            raise AdvisoryEvidenceProjectionError(
                "manifest alpha158 closure member is missing"
            )
        expected.add((ProjectedPackageAssetType.FACTOR_SCHEMA.value, asset_ref, sha256))
    if not expected:
        raise AdvisoryEvidenceProjectionError(
            "manifest does not expose a persisted runtime closure"
        )
    return expected
