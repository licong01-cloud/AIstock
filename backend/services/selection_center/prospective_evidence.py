"""Typed prospective-selection evidence contracts.

The contracts in this module carry evidence beside the shared Selection path.
They must not alter model scores, ranks, filters, or target construction.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from enum import Enum
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.trading_core.errors import RuntimeConfigInvalidError

from .models import SelectionCandidate, SelectionExclusion


class EvidenceCaptureMode(str, Enum):
    DISABLED = "DISABLED"
    PROSPECTIVE = "PROSPECTIVE"


class EvidenceCaptureStatus(str, Enum):
    COMPLETE = "COMPLETE"
    NOT_REQUESTED = "NOT_REQUESTED"
    FAILED = "FAILED"


class ProspectiveExecutionOrigin(str, Enum):
    SELECTION_CENTER = "SELECTION_CENTER"
    ADVISORY_RUN = "ADVISORY_RUN"
    PAPER = "PAPER"
    SIMULATION = "SIMULATION"
    REPLAY = "REPLAY"
    PREVIEW = "PREVIEW"


class CandidateStageName(str, Enum):
    ALPHA_RAW = "alpha_raw"
    HMM_ADJUSTED = "hmm_adjusted"
    RISK_POLICY_ADJUSTED = "risk_policy_adjusted"
    SELECTION_EFFECTIVE = "selection_effective"
    ADVISORY_MODEL = "advisory_model"


class StageReceiptStatus(str, Enum):
    COMPLETE = "COMPLETE"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    FAILED = "FAILED"


REASON_CONTEXT_MISSING = "ADVISORY_PHASE0A2C_CONTEXT_MISSING"
REASON_DECISION_CLOCK_INVALID = "ADVISORY_PHASE0A2C_DECISION_CLOCK_INVALID"
REASON_CONFIG_CHAIN_INCOMPLETE = "ADVISORY_PHASE0A2C_CONFIG_CHAIN_INCOMPLETE"
REASON_ARTIFACT_V2_REQUIRED = "ADVISORY_PHASE0A2C_ARTIFACT_V2_REQUIRED"
REASON_ARTIFACT_IDEMPOTENCY_CONFLICT = "ADVISORY_PHASE0A2C_ARTIFACT_IDEMPOTENCY_CONFLICT"
REASON_SOURCE_RECEIPT_INCOMPLETE = "ADVISORY_PHASE0A2C_SOURCE_RECEIPT_INCOMPLETE"
REASON_ASSET_CLOSURE_INCOMPLETE = "ADVISORY_PHASE0A2C_ASSET_CLOSURE_INCOMPLETE"
REASON_HMM_RECEIPT_INCOMPLETE = "ADVISORY_PHASE0A2C_HMM_RECEIPT_INCOMPLETE"
REASON_UNIVERSE_RECEIPT_INCOMPLETE = "ADVISORY_PHASE0A2C_UNIVERSE_RECEIPT_INCOMPLETE"
REASON_STAGE_RECEIPT_INCOMPLETE = "ADVISORY_PHASE0A2C_STAGE_RECEIPT_INCOMPLETE"
REASON_LINEAGE_MISMATCH = "ADVISORY_PHASE0A2C_LINEAGE_MISMATCH"
REASON_CAPTURE_FAILED = "ADVISORY_PHASE0A2C_CAPTURE_FAILED"
REASON_VALID_NO_CANDIDATE = "ADVISORY_PHASE0A_VALID_NO_CANDIDATE"
REASON_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE = "ADVISORY_PHASE0A_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE"
REASON_VALID_NO_CANDIDATE_DECLARATION_FORBIDDEN = "ADVISORY_PHASE0A_VALID_NO_CANDIDATE_DECLARATION_FORBIDDEN"
REASON_HISTORICAL_RESEARCH_ONLY = "ADVISORY_PHASE0A2C_HISTORICAL_RESEARCH_ONLY"
HISTORICAL_RESEARCH_DATA_SOURCE = "DB_HISTORICAL"
HISTORICAL_RESEARCH_SCOPE = "HISTORICAL_RESEARCH_ONLY"


def _normalize_for_hash(value: Any) -> Any:
    """Convert supported evidence values to a deterministic JSON representation."""

    if isinstance(value, BaseModel):
        return _normalize_for_hash(value.model_dump(mode="python"))
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("evidence timestamps must be timezone-aware")
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _normalize_for_hash(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, tuple | list):
        return [_normalize_for_hash(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("evidence payload cannot contain non-finite floats")
        return value
    return value


def canonical_evidence_json_sha256(payload: Any) -> str:
    normalized = _normalize_for_hash(payload)
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_candidate_rows(candidates: list[SelectionCandidate]) -> list[dict[str, Any]]:
    """Return the stable semantic projection used by stage and parity hashes."""

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


def _require_sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field_name} must be a lowercase 64-character sha256 digest")
    return normalized


class EvidenceContractV2(BaseModel):
    """Top-level prospective producer identity, excluding DSE self references."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["daily_selection_evidence_v2"] = "daily_selection_evidence_v2"
    capture_mode: EvidenceCaptureMode
    capture_status: Literal["COMPLETE"] = "COMPLETE"
    execution_origin: ProspectiveExecutionOrigin
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
    def _complete_capture_is_consistent(self) -> "EvidenceContractV2":
        if self.capture_mode != EvidenceCaptureMode.PROSPECTIVE:
            raise ValueError("v2 evidence contract requires prospective capture mode")
        if self.reason_codes:
            raise ValueError("complete v2 evidence contract cannot contain reason_codes")
        expected_eligibility = self.execution_origin == ProspectiveExecutionOrigin.ADVISORY_RUN
        if self.prospective_eligible != expected_eligibility:
            raise ValueError("prospective_eligible must match execution_origin")
        if self.execution_origin != ProspectiveExecutionOrigin.ADVISORY_RUN:
            raise ValueError("v2 prospective evidence is restricted to historical ADVISORY_RUN research")
        return self


class DecisionClockEvidenceV2(BaseModel):
    """Canonical selection decision clock supplied by the prospective caller."""

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
    def _clock_is_coherent(self) -> "DecisionClockEvidenceV2":
        if self.decision_as_of_trade_date != self.selection_as_of_trade_date:
            raise ValueError("decision and selection as-of trade dates must match")
        if self.score_trade_date != self.decision_as_of_trade_date:
            raise ValueError("score_trade_date must match decision_as_of_trade_date")
        if self.reference_price_trade_date != self.decision_as_of_trade_date:
            raise ValueError("reference_price_trade_date must match decision_as_of_trade_date")
        if self.effective_cutoff_date != self.decision_as_of_trade_date:
            raise ValueError("effective_cutoff_date must match decision_as_of_trade_date")
        if self.requested_cutoff_date > self.effective_cutoff_date:
            raise ValueError("requested_cutoff_date cannot be after effective_cutoff_date")
        if self.target_trade_date <= self.decision_as_of_trade_date:
            raise ValueError("target_trade_date must be after decision_as_of_trade_date")
        if self.effective_entry_trade_date != self.target_trade_date:
            raise ValueError("effective_entry_trade_date must match target_trade_date")
        if not self.is_immediately_previous_trade_date:
            raise ValueError("decision clock requires immediate next-trading-day relation")
        if self.data_available_at > self.decision_cutoff_ts:
            raise ValueError("data_available_at cannot be after decision_cutoff_ts")
        if self.decision_generated_at < self.decision_cutoff_ts:
            raise ValueError("decision_generated_at cannot be before decision_cutoff_ts")
        digest = canonical_evidence_json_sha256(self.canonical_payload())
        if self.decision_clock_hash is not None and self.decision_clock_hash != digest:
            raise ValueError("decision_clock_hash does not match canonical decision clock")
        object.__setattr__(self, "decision_clock_hash", digest)
        return self


class EffectiveConfigChainV2(BaseModel):
    """Typed effective-config provenance; the assembler never invents a layer."""

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
    def _config_chain_is_consistent(self) -> "EffectiveConfigChainV2":
        for config_field, hash_field in (
            ("binding_base_config", "binding_base_config_hash"),
            ("request_override_config", "request_override_hash"),
            ("date_enforced_config", "date_enforced_hash"),
            ("selection_normalized_config", "selection_normalized_config_hash"),
            ("package_effective_config", "package_effective_config_hash"),
        ):
            if canonical_evidence_json_sha256(getattr(self, config_field)) != getattr(self, hash_field):
                raise ValueError(f"{hash_field} does not match {config_field}")
        if canonical_evidence_json_sha256(self.package_effective_config) != self.final_effective_config_hash:
            raise ValueError("final_effective_config_hash does not match package_effective_config")
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


class UniverseLayerReceiptV2(BaseModel):
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
    def _universe_counts_reconcile(self) -> "UniverseLayerReceiptV2":
        if self.input_count != self.output_count + self.excluded_count:
            raise ValueError("universe layer counts must reconcile")
        if any(value < 0 for value in self.exclusion_reason_counts.values()):
            raise ValueError("universe exclusion reason counts must be non-negative")
        if sum(self.exclusion_reason_counts.values()) != self.excluded_count:
            raise ValueError("universe exclusion reason counts must match excluded_count")
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
                raise ValueError("executed universe layer is missing mandatory provenance")
        return self


class UniverseEvidenceV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    layers: list[UniverseLayerReceiptV2]
    package_cohort: dict[str, Any]

    @model_validator(mode="after")
    def _all_layers_are_present(self) -> "UniverseEvidenceV2":
        names = [item.layer for item in self.layers]
        if tuple(names) != _UNIVERSE_LAYER_NAMES:
            raise ValueError("universe layers must be present once in canonical order")
        if not self.package_cohort:
            raise ValueError("package_cohort provenance is required")
        return self


class DailySelectionEvidenceV2Payload(BaseModel):
    """Strict persisted DSE v2 payload. It intentionally has no evidence id/hash."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["daily_selection_evidence_v2"] = "daily_selection_evidence_v2"
    evidence_contract: EvidenceContractV2
    decision_clock: DecisionClockEvidenceV2
    point_in_time_context: dict[str, Any]
    runtime_profile: dict[str, Any]
    runtime_profile_binding: dict[str, Any]
    selection_artifact_config: dict[str, Any]
    phase0a_effective_config_chain: EffectiveConfigChainV2
    phase0a_hmm_metadata: dict[str, Any]
    phase0a_risk_policy_metadata: dict[str, Any]
    phase0a_universe_evidence: UniverseEvidenceV2
    phase0a_package_lineage: dict[str, Any]
    phase0a_asset_closure: list[dict[str, Any]]
    phase0a_source_evidence: list[SourceReadReceipt]
    phase0a_candidate_lineage: dict[str, Any]
    phase0a_stage_evidence: dict[str, StageEvidenceReceipt]
    candidate_outcome: Literal["CANDIDATES_PRESENT", "VALID_NO_CANDIDATE"]
    selected_candidates: list[dict[str, Any]]
    excluded_candidates: list[dict[str, Any]]

    @model_validator(mode="after")
    def _payload_is_consistent(self) -> "DailySelectionEvidenceV2Payload":
        required_stages = {item.value for item in CandidateStageName}
        if set(self.phase0a_stage_evidence) != required_stages:
            raise ValueError("phase0a_stage_evidence must contain exactly five canonical stages")
        for stage_name, receipt in self.phase0a_stage_evidence.items():
            if receipt.stage.value != stage_name:
                raise ValueError("stage receipt key does not match stage value")
        advisory = self.phase0a_stage_evidence[CandidateStageName.ADVISORY_MODEL.value]
        if advisory.status != StageReceiptStatus.NOT_APPLICABLE:
            raise ValueError("advisory_model must be not applicable in Phase 0A.2C")
        effective = self.phase0a_stage_evidence[CandidateStageName.SELECTION_EFFECTIVE.value]
        selected_models = [SelectionCandidate.model_validate(item) for item in self.selected_candidates]
        if effective.status != StageReceiptStatus.COMPLETE:
            raise ValueError("selection_effective stage must be complete")
        if effective.candidates != canonical_candidate_rows(selected_models):
            raise ValueError("selection_effective candidates do not match selected_candidates")
        all_exclusions: list[dict[str, Any]] = []
        for stage_name in (
            CandidateStageName.HMM_ADJUSTED.value,
            CandidateStageName.RISK_POLICY_ADJUSTED.value,
            CandidateStageName.SELECTION_EFFECTIVE.value,
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
        if self.candidate_outcome == "CANDIDATES_PRESENT" and not self.selected_candidates:
            raise ValueError("CANDIDATES_PRESENT requires selected candidates")
        if self.candidate_outcome == "VALID_NO_CANDIDATE" and self.selected_candidates:
            raise ValueError("VALID_NO_CANDIDATE cannot contain selected candidates")
        return self


class SourceReadReceipt(BaseModel):
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
    def _availability_is_explicit(self) -> "SourceReadReceipt":
        if self.available_at is None and self.first_observed_at is None:
            raise ValueError("source receipt requires available_at or first_observed_at")
        for field_name, timestamp in (
            ("available_at", self.available_at),
            ("first_observed_at", self.first_observed_at),
        ):
            if timestamp is not None and timestamp.tzinfo is None:
                raise ValueError(f"source receipt {field_name} must be timezone-aware")
        if not str(self.admissibility or "").strip():
            raise ValueError("source receipt admissibility is required")
        return self


class StageEvidenceReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage: CandidateStageName
    status: StageReceiptStatus
    input_count: int = Field(ge=0)
    output_count: int = Field(ge=0)
    excluded_count: int = Field(ge=0)
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    exclusions: list[dict[str, Any]] = Field(default_factory=list)
    semantic_payload: dict[str, Any] = Field(default_factory=dict)
    reason_codes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _counts_match_rows(self) -> "StageEvidenceReceipt":
        if self.status == StageReceiptStatus.COMPLETE and self.output_count != len(self.candidates):
            raise ValueError("complete stage output_count must match candidate rows")
        if self.status == StageReceiptStatus.COMPLETE and self.input_count != self.output_count + self.excluded_count:
            raise ValueError("complete stage input_count must reconcile output_count and excluded_count")
        if self.status == StageReceiptStatus.NOT_APPLICABLE and self.candidates:
            raise ValueError("not-applicable stage cannot contain candidate rows")
        if self.status == StageReceiptStatus.NOT_APPLICABLE and (self.output_count or self.excluded_count or self.exclusions):
            raise ValueError("not-applicable stage cannot contain output or exclusions")
        return self

    @property
    def content_hash(self) -> str:
        return canonical_evidence_json_sha256(self.candidates)

    @property
    def semantic_hash(self) -> str:
        return canonical_evidence_json_sha256(self.semantic_payload)

    @property
    def receipt_hash(self) -> str:
        return canonical_evidence_json_sha256(
            {
                "stage": self.stage.value,
                "status": self.status.value,
                "input_count": self.input_count,
                "output_count": self.output_count,
                "excluded_count": self.excluded_count,
                "content_hash": self.content_hash,
                "semantic_hash": self.semantic_hash,
                "exclusions": self.exclusions,
                "reason_codes": sorted(self.reason_codes),
            }
        )


class HMMAdjustmentResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    candidates: list[SelectionCandidate] = Field(default_factory=list)
    receipt: StageEvidenceReceipt
    hmm_metadata: dict[str, Any] = Field(default_factory=dict)


class RiskAdjustmentResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    candidates: list[SelectionCandidate] = Field(default_factory=list)
    exclusions: list[SelectionExclusion] = Field(default_factory=list)
    receipt: StageEvidenceReceipt
    risk_metadata: dict[str, Any] = Field(default_factory=dict)


class TradabilityResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    candidates: list[SelectionCandidate] = Field(default_factory=list)
    exclusions: list[SelectionExclusion] = Field(default_factory=list)
    receipt: StageEvidenceReceipt
    universe_metadata: dict[str, Any] = Field(default_factory=dict)


class SelectionStageTrace(BaseModel):
    """One in-memory, single-execution candidate lineage for one package."""

    model_config = ConfigDict(extra="forbid")

    alpha_raw: StageEvidenceReceipt
    hmm_adjusted: StageEvidenceReceipt
    risk_policy_adjusted: StageEvidenceReceipt
    selection_effective: StageEvidenceReceipt
    hmm_metadata: dict[str, Any] = Field(default_factory=dict)
    risk_metadata: dict[str, Any] = Field(default_factory=dict)
    universe_metadata: dict[str, Any] = Field(default_factory=dict)


class ProspectiveSelectionContext(BaseModel):
    """Advisory provenance supplied outside candidate-scoring runtime config."""

    model_config = ConfigDict(extra="forbid")

    capture_mode: EvidenceCaptureMode = EvidenceCaptureMode.DISABLED
    selection_run_id: str | None = None
    execution_origin: ProspectiveExecutionOrigin = ProspectiveExecutionOrigin.SELECTION_CENTER
    research_scope: Literal["HISTORICAL_RESEARCH_ONLY"] = HISTORICAL_RESEARCH_SCOPE
    decision_clock_seed: dict[str, Any] = Field(default_factory=dict)
    effective_config_seed: dict[str, Any] = Field(default_factory=dict)
    policy_registry_ref: dict[str, Any] = Field(default_factory=dict)
    binding_ref: dict[str, Any] = Field(default_factory=dict)
    source_watermark_seed: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None

    @model_validator(mode="after")
    def _prospective_context_is_identified(self) -> "ProspectiveSelectionContext":
        # Selection Center creates the SelectionRun before candidate computation
        # and binds its generated id immediately before invoking the assembler.
        # The assembler, not an unbound caller, enforces this mandatory lineage.
        if self.selection_run_id is not None and not str(self.selection_run_id).strip():
            raise ValueError("selection_run_id cannot be blank")
        if (
            self.capture_mode == EvidenceCaptureMode.PROSPECTIVE
            and self.execution_origin != ProspectiveExecutionOrigin.ADVISORY_RUN
        ):
            raise ValueError("prospective evidence capture is restricted to historical ADVISORY_RUN research")
        return self


def require_historical_research_data_source(
    *,
    context: ProspectiveSelectionContext | None,
    data_source: str,
) -> None:
    """Fail before inference when formal advisory capture leaves the historical DB boundary."""

    if context is None or context.capture_mode != EvidenceCaptureMode.PROSPECTIVE:
        return
    if str(data_source or "").strip() != HISTORICAL_RESEARCH_DATA_SOURCE:
        raise RuntimeConfigInvalidError(
            "prospective advisory research requires DB_HISTORICAL data",
            context={
                "reason_code": REASON_HISTORICAL_RESEARCH_ONLY,
                "data_source": data_source,
                "required_data_source": HISTORICAL_RESEARCH_DATA_SOURCE,
                "execution_origin": context.execution_origin.value,
            },
        )


class EvidenceCaptureReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requested: bool
    schema_version: str
    status: EvidenceCaptureStatus
    evidence_ids_by_package: dict[str, str] = Field(default_factory=dict)
    reason_codes: list[str] = Field(default_factory=list)
    detail_hash: str


def build_stage_receipt(
    *,
    stage: CandidateStageName,
    status: StageReceiptStatus,
    input_count: int,
    candidates: list[SelectionCandidate],
    exclusions: list[SelectionExclusion] | None = None,
    semantic_payload: dict[str, Any] | None = None,
    reason_codes: list[str] | None = None,
) -> StageEvidenceReceipt:
    ranks = [candidate.rank for candidate in candidates]
    symbols = [candidate.symbol for candidate in candidates]
    if len(ranks) != len(set(ranks)) or len(symbols) != len(set(symbols)):
        raise ValueError("stage candidates must have unique ranks and symbols")
    excluded = sorted(
        list(exclusions or []),
        key=lambda item: (item.source, item.reason, item.rank, item.symbol),
    )
    return StageEvidenceReceipt(
        stage=stage,
        status=status,
        input_count=input_count,
        output_count=len(candidates),
        excluded_count=len(excluded),
        candidates=canonical_candidate_rows(candidates) if status == StageReceiptStatus.COMPLETE else [],
        exclusions=[item.model_dump(mode="json") for item in excluded],
        semantic_payload=dict(semantic_payload or {}),
        reason_codes=list(reason_codes or []),
    )
