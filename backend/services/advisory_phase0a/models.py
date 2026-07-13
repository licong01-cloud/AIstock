"""Stable data contracts for the read-only Advisory Phase 0A audit."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Phase0AAuditError(RuntimeError):
    """Raised when an audit request violates a fail-closed Phase 0A contract."""


class BindingResolutionMode(str, Enum):
    AS_OF_HISTORICAL = "as_of_historical"


class ExpectedAlphaMode(str, Enum):
    SINGLE_ALPHA = "single_alpha"
    MULTI_ALPHA = "multi_alpha"


class FormalOOSStatus(str, Enum):
    FORMAL_OOS = "FORMAL_OOS"
    RETROSPECTIVE_RESEARCH_ONLY = "RETROSPECTIVE_RESEARCH_ONLY"
    NONE = "NONE"


class AvailabilityStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"


class LabelMaturityStatus(str, Enum):
    PENDING = "PENDING"
    MATURED = "MATURED"
    CENSORED = "CENSORED"
    UNAVAILABLE = "UNAVAILABLE"


class CandidateAuthorityStatus(str, Enum):
    FORMAL = "FORMAL"
    RETROSPECTIVE = "RETROSPECTIVE"
    NONE = "NONE"


class CandidateStage(str, Enum):
    ALPHA_RAW = "alpha_raw"
    HMM_ADJUSTED = "hmm_adjusted"
    RISK_POLICY_ADJUSTED = "risk_policy_adjusted"
    SELECTION_EFFECTIVE = "selection_effective"
    ADVISORY_MODEL = "advisory_model"


class StageCapabilityStatus(str, Enum):
    FULL = "FULL"
    PARTIAL = "PARTIAL"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class SourceAvailabilityStatus(str, Enum):
    FORMAL_READY = "FORMAL_READY"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    PARTIAL = "PARTIAL"
    MISSING = "MISSING"
    FORBIDDEN = "FORBIDDEN"


class HandoffReadiness(str, Enum):
    READY = "READY"
    PARTIAL = "PARTIAL"
    BLOCKED = "BLOCKED"


class HandoffEvidenceScope(str, Enum):
    FORMAL_OOS = "FORMAL_OOS"
    RETROSPECTIVE_RESEARCH_ONLY = "RETROSPECTIVE_RESEARCH_ONLY"
    GAP_ONLY = "GAP_ONLY"


class AuditDateRange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start_date: date
    end_date: date

    @model_validator(mode="after")
    def _end_not_before_start(self) -> "AuditDateRange":
        if self.end_date < self.start_date:
            raise ValueError("end_date must not be before start_date")
        return self


class AuditTarget(BaseModel):
    """One independently audited Advisory Program and native package binding."""

    model_config = ConfigDict(extra="forbid")

    audit_target_id: str
    program_id: str
    package_id: str
    manifest_sha256: str
    binding_resolution_mode: BindingResolutionMode = BindingResolutionMode.AS_OF_HISTORICAL
    expected_alpha_mode: ExpectedAlphaMode
    decision_date_range: AuditDateRange
    decision_dates: list[date] = Field(default_factory=list)
    selection_evidence_ids_by_decision_date: dict[date, str] = Field(default_factory=dict)
    style_family: str
    requested_capabilities: list[str] = Field(
        default_factory=lambda: [
            "candidate_authority",
            "runtime_semantics",
            "hmm_vintage",
            "source_availability",
            "oos_classification",
        ]
    )
    audit_policy_version: str

    @field_validator("audit_target_id", "program_id", "package_id", "manifest_sha256", "style_family", "audit_policy_version")
    @classmethod
    def _required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("field is required")
        return normalized

    @field_validator("manifest_sha256")
    @classmethod
    def _manifest_sha256_format(cls, value: str) -> str:
        normalized = value.lower()
        if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
            raise ValueError("manifest_sha256 must be a lowercase 64-character sha256 digest")
        return normalized

    @field_validator("requested_capabilities")
    @classmethod
    def _unique_capabilities(cls, value: list[str]) -> list[str]:
        normalized = sorted({str(item or "").strip() for item in value if str(item or "").strip()})
        if not normalized:
            raise ValueError("requested_capabilities must not be empty")
        return normalized

    @model_validator(mode="after")
    def _declared_dates_are_in_scope(self) -> "AuditTarget":
        dates = self.decision_dates or sorted(self.selection_evidence_ids_by_decision_date)
        if not dates:
            raise ValueError("decision_dates or selection_evidence_ids_by_decision_date is required")
        for decision_date in dates:
            if not self.decision_date_range.start_date <= decision_date <= self.decision_date_range.end_date:
                raise ValueError("decision date is outside decision_date_range")
        for decision_date, evidence_id in self.selection_evidence_ids_by_decision_date.items():
            if decision_date not in dates:
                raise ValueError("selection evidence date must be declared in decision_dates")
            if not str(evidence_id or "").strip():
                raise ValueError("selection evidence id must not be empty")
        object.__setattr__(self, "decision_dates", sorted(set(dates)))
        return self


class AuditRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    audit_id: str
    policy_registry_id: str
    audit_policy_version: str
    policy_registry_content_hash: str
    targets: list[AuditTarget]

    @field_validator("audit_id", "policy_registry_id", "audit_policy_version")
    @classmethod
    def _required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("field is required")
        return normalized

    @field_validator("policy_registry_content_hash")
    @classmethod
    def _policy_registry_hash_format(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
            raise ValueError("policy_registry_content_hash must be a lowercase 64-character sha256 digest")
        return normalized

    @model_validator(mode="after")
    def _targets_are_unique_and_use_the_request_policy(self) -> "AuditRequest":
        target_ids = [target.audit_target_id for target in self.targets]
        if not target_ids:
            raise ValueError("targets must not be empty")
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("audit_target_id values must be unique")
        mismatch = [target.audit_target_id for target in self.targets if target.audit_policy_version != self.audit_policy_version]
        if mismatch:
            raise ValueError(f"target policy version mismatch: {sorted(mismatch)}")
        return self


class Phase0APolicyRegistry(BaseModel):
    """Pre-registered policy identities used by an audit, never inferred from results."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "advisory_phase0a_policy_registry_v1"
    policy_registry_id: str | None = None
    policy_version: str
    serializer_version: str = "advisory_phase0a_canonical_v1"
    frozen_at: datetime | None = None
    effective_from_trade_date: date | None = None
    effective_to_trade_date: date | None = None
    registry_content_hash: str | None = None
    formal_start_date: date | None = None
    minimum_trading_day_gap: int = Field(default=20, ge=0)
    benchmark_policy: dict[str, Any] = Field(default_factory=dict)
    cost_policy: dict[str, Any] = Field(default_factory=dict)
    label_policy: dict[str, Any] = Field(default_factory=dict)
    prior_policy: dict[str, Any] = Field(default_factory=dict)
    multiple_testing_policy: dict[str, Any] = Field(default_factory=dict)
    universe_policy: dict[str, Any] = Field(default_factory=dict)
    embargo_policy: dict[str, Any] = Field(default_factory=dict)
    style_assignment_policy: dict[str, Any] = Field(default_factory=dict)
    embargo_policy_id: str | None = None
    embargo_policy_version: str | None = None
    embargo_policy_hash: str | None = None
    cutoff_timestamp_normalization: str | None = None
    training_label_information_end_rule: str | None = None
    calendar_version: str | None = None
    calendar_hash: str | None = None

    @field_validator("schema_version", "policy_version", "serializer_version")
    @classmethod
    def _policy_text_required(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("policy identifier is required")
        return normalized

    @model_validator(mode="after")
    def _normalize_embargo_and_effective_range(self) -> "Phase0APolicyRegistry":
        if (
            self.effective_from_trade_date is not None
            and self.effective_to_trade_date is not None
            and self.effective_to_trade_date < self.effective_from_trade_date
        ):
            raise ValueError("effective_to_trade_date must not be before effective_from_trade_date")
        if not self.embargo_policy:
            return self

        mapped_values = {
            "embargo_policy_id": self.embargo_policy.get("policy_id"),
            "embargo_policy_version": self.embargo_policy.get("policy_version"),
            "embargo_policy_hash": self.embargo_policy.get("policy_hash"),
            "cutoff_timestamp_normalization": self.embargo_policy.get("cutoff_timestamp_normalization"),
            "training_label_information_end_rule": self.embargo_policy.get("training_label_information_end_rule"),
            "calendar_version": self.embargo_policy.get("calendar_version"),
            "calendar_hash": self.embargo_policy.get("calendar_hash"),
        }
        for attribute, value in mapped_values.items():
            if getattr(self, attribute) is None and value not in (None, ""):
                object.__setattr__(self, attribute, value)
        gap = self.embargo_policy.get("minimum_trading_day_gap")
        if gap is not None:
            object.__setattr__(self, "minimum_trading_day_gap", gap)
        return self

    @property
    def is_frozen(self) -> bool:
        return bool(
            self.policy_registry_id
            and self.policy_version
            and self.registry_content_hash
            and self.frozen_at is not None
            and self.effective_from_trade_date is not None
        )


class AssetLedgerEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    package_id: str
    asset_type: str
    asset_ref: str
    asset_id: str | None = None
    asset_sha256: str | None = None
    asset_role: str
    parent_or_lineage_ids: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    available_at: datetime | None = None
    data_cutoff: date | None = None
    information_cutoff_ts: datetime | None = None
    training_data_end_ts: datetime | None = None
    model_selection_decision_ts: datetime | None = None
    research_decision_ts: datetime | None = None
    frozen_at: datetime | None = None
    promoted_or_activated_at: datetime | None = None
    evidence_source_type: str | None = None
    evidence_ref: str | None = None
    evidence_hash: str | None = None
    admissibility: str
    reason_codes: list[str] = Field(default_factory=list)


class RuntimeSemanticsEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_date: date
    package_id: str
    evidence_id: str | None = None
    runtime_profile_version_id: str | None = None
    runtime_profile_hash: str | None = None
    runtime_binding_source: str | None = None
    selection_runtime_semantics_id: str | None = None
    effective_config_hashes: dict[str, str | None] = Field(default_factory=dict)
    effective_config_chain_complete: bool = False
    historical_available_at: datetime | None = None
    is_historical_binding: bool = False
    source_payload_hash: str | None = None
    reason_codes: list[str] = Field(default_factory=list)


class HMMVintageEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_date: date
    package_id: str
    enabled: bool
    status: str
    model_snapshot_id: str | None = None
    model_config_id: str | None = None
    signal_preset: str | None = None
    model_artifact_sha256: str | None = None
    coefficient_sha256: str | None = None
    snapshot_trained_at: datetime | None = None
    available_at: datetime | None = None
    training_information_cutoff: date | None = None
    as_of_trade_date: date | None = None
    effective_trade_date: date | None = None
    generation_mode: str | None = None
    input_data_max_dates_hash: str | None = None
    freshness_lag: int | None = Field(default=None, ge=0)
    reason_codes: list[str] = Field(default_factory=list)


class SourceAvailability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: str
    capability: str
    decision_date: date
    status: SourceAvailabilityStatus
    owner: str | None = None
    authoritative_for: list[str] = Field(default_factory=list)
    schema_or_artifact: str | None = None
    field_name: str | None = None
    data_type: str | None = None
    event_time_field: str | None = None
    available_at_field: str | None = None
    revision_rule: str | None = None
    pit_join_predicate: str | None = None
    watermark_date: date | None = None
    available_at: datetime | None = None
    data_cutoff: date | None = None
    coverage_start: date | None = None
    coverage_end: date | None = None
    coverage_intervals: list[dict[str, Any]] = Field(default_factory=list)
    missing_intervals: list[dict[str, Any]] = Field(default_factory=list)
    partition_watermarks: dict[str, Any] = Field(default_factory=dict)
    revision_epoch: str | None = None
    universe_hash: str | None = None
    source_content_hash: str | None = None
    source_query_id: str | None = None
    query_template_version: str | None = None
    query_hash: str | None = None
    parameter_hash: str | None = None
    row_count: int | None = Field(default=None, ge=0)
    is_point_in_time: bool = False
    reason_codes: list[str] = Field(default_factory=list)


class StageCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage: CandidateStage
    status: StageCapabilityStatus
    input_count: int | None = Field(default=None, ge=0)
    output_count: int | None = Field(default=None, ge=0)
    excluded_count: int | None = Field(default=None, ge=0)
    candidate_count: int | None = Field(default=None, ge=0)
    artifact_hash: str | None = None
    content_hash: str | None = None
    semantic_hash: str | None = None
    reason_codes: list[str] = Field(default_factory=list)


class CandidateDepthEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requested_top_k: int | None = Field(default=None, ge=0)
    requested_observation_depth: int | None = Field(default=None, ge=0)
    display_top_n: int | None = Field(default=None, ge=0)
    manifest_top_k: int | None = Field(default=None, ge=0)
    allowed_top_k_variants: list[int] = Field(default_factory=list)
    runtime_variant_id: str | None = None
    contract_top_k: int | None = Field(default=None, ge=0)
    artifact_top_k: int | None = Field(default=None, ge=0)
    effective_artifact_top_k: int | None = Field(default=None, ge=0)
    alpha_artifact_row_count: int | None = Field(default=None, ge=0)
    hmm_input_depth: int | None = Field(default=None, ge=0)
    effective_selection_top_k: int | None = Field(default=None, ge=0)
    selection_effective_depth: int | None = Field(default=None, ge=0)
    artifact_score_count: int | None = Field(default=None, ge=0)
    artifact_universe_count: int | None = Field(default=None, ge=0)
    observed_max_rank: int | None = Field(default=None, ge=0)
    depth_satisfied: bool | None = None
    contract_hash: str | None = None
    reason_codes: list[str] = Field(default_factory=list)


class DecisionClockEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_as_of_trade_date: date
    selection_as_of_trade_date: date | None = None
    target_trade_date: date | None = None
    selection_run_trade_date: date | None = None
    score_trade_date: date | None = None
    reference_price_trade_date: date | None = None
    effective_entry_trade_date: date | None = None
    requested_selection_as_of_trade_date: date | None = None
    effective_cutoff_date: date | None = None
    decision_cutoff_ts: datetime | None = None
    data_available_at: datetime | None = None
    decision_generated_at: datetime | None = None
    timezone: str = "Asia/Shanghai"
    calendar_version: str | None = None
    calendar_hash: str | None = None
    is_immediately_previous_trade_date: bool | None = None
    is_formal_canonical_clock: bool = False
    reason_codes: list[str] = Field(default_factory=list)


class UniverseLayerEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    layer: str
    status: SourceAvailabilityStatus
    policy_hash: str | None = None
    input_count: int | None = Field(default=None, ge=0)
    output_count: int | None = Field(default=None, ge=0)
    excluded_count: int | None = Field(default=None, ge=0)
    exclusion_reason_counts: dict[str, int] = Field(default_factory=dict)
    symbol_set_hash: str | None = None
    available_at: datetime | None = None
    policy_available_at: datetime | None = None
    reason_codes: list[str] = Field(default_factory=list)


class UniverseSurvivorshipEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_date: date
    package_id: str
    layers: list[UniverseLayerEvidence] = Field(default_factory=list)
    package_cohort_status: SourceAvailabilityStatus
    package_cohort_reason_codes: list[str] = Field(default_factory=list)
    reason_codes: list[str] = Field(default_factory=list)


class RiskPolicyEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_date: date
    package_id: str
    risk_policy_hash: str | None = None
    risk_policy_enabled: bool | None = None
    industry_blacklist_hash: str | None = None
    tradability_policy_hash: str | None = None
    policy_available_at: datetime | None = None
    status: StageCapabilityStatus
    reason_codes: list[str] = Field(default_factory=list)


class EmbargoEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy_id: str | None = None
    policy_version: str | None = None
    policy_hash: str | None = None
    minimum_trading_day_gap: int | None = Field(default=None, ge=0)
    cutoff_timestamp_normalization: str | None = None
    training_label_information_end_rule: str | None = None
    calendar_version: str | None = None
    calendar_hash: str | None = None
    effective_cutoff: date | None = None
    formal_start_date: date | None = None
    status: SourceAvailabilityStatus
    reason_codes: list[str] = Field(default_factory=list)


class CandidateAuthorityReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_date: date
    package_id: str
    manifest_sha256: str | None = None
    signal_context_hash: str | None = None
    canonical_signal_observation_id: str | None = None
    label_context_hash: str | None = None
    status: CandidateAuthorityStatus
    evidence_id: str | None = None
    selection_run_id: str | None = None
    selection_run_content_hash: str | None = None
    selection_score_artifact_id: str | None = None
    selection_score_artifact_sha256: str | None = None
    daily_selection_evidence_hash: str | None = None
    hmm_evidence_hash: str | None = None
    source_type: str | None = None
    requested_top_k: int | None = Field(default=None, ge=0)
    display_top_k: int | None = Field(default=None, ge=0)
    artifact_depth: int | None = Field(default=None, ge=0)
    effective_depth: int | None = Field(default=None, ge=0)
    depth_evidence: CandidateDepthEvidence | None = None
    decision_clock: DecisionClockEvidence | None = None
    risk_policy: RiskPolicyEvidence | None = None
    stage_capabilities: list[StageCapability] = Field(default_factory=list)
    phase0a_reason_codes: list[str] = Field(default_factory=list)
    upstream_reason_codes: list[str] = Field(default_factory=list)


class OOSClassificationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_date: date
    formal_start_date: date | None = None
    effective_cutoff: date | None = None
    mandatory_closure_complete: bool = False
    historical_semantics_available: bool = False
    point_in_time_source_available: bool = False
    candidate_authority_formal: bool = False
    research_replay_eligible: bool = False
    reason_codes: list[str] = Field(default_factory=list)
    upstream_reason_codes: list[str] = Field(default_factory=list)


class OOSClassification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_date: date
    signal_context_hash: str | None = None
    signal_capability: str = "candidate_signal"
    formal_oos_status: FormalOOSStatus
    availability_status: AvailabilityStatus
    effective_cutoff: date | None = None
    research_replay_eligible: bool = False
    label_maturity_status: LabelMaturityStatus = LabelMaturityStatus.UNAVAILABLE
    phase0a_reason_codes: list[str] = Field(default_factory=list)
    upstream_reason_codes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _formal_availability_invariant(self) -> "OOSClassification":
        expected = (
            AvailabilityStatus.AVAILABLE
            if self.formal_oos_status == FormalOOSStatus.FORMAL_OOS
            else AvailabilityStatus.UNAVAILABLE
        )
        if self.availability_status != expected:
            raise ValueError("formal_oos_status and availability_status violate Phase 0A invariant")
        return self


class OOSInterval(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interval_id: str
    start_date: date
    end_date: date
    signal_context_hash: str | None = None
    signal_capability: str = "candidate_signal"
    formal_oos_status: FormalOOSStatus
    availability_status: AvailabilityStatus
    effective_cutoff: date | None = None
    research_replay_eligible: bool = False
    phase0a_reason_codes: list[str] = Field(default_factory=list)
    upstream_reason_codes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _interval_is_valid(self) -> "OOSInterval":
        if self.end_date < self.start_date:
            raise ValueError("end_date must not be before start_date")
        return self


class TargetAuditResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    audit_target_id: str
    program_id: str
    package_id: str
    manifest_sha256: str | None = None
    binding_version_ids: dict[date, str | None] = Field(default_factory=dict)
    asset_ledger: list[AssetLedgerEntry] = Field(default_factory=list)
    runtime_semantics: list[RuntimeSemanticsEvidence] = Field(default_factory=list)
    hmm_vintages: list[HMMVintageEvidence] = Field(default_factory=list)
    source_availability: list[SourceAvailability] = Field(default_factory=list)
    universe_survivorship: list[UniverseSurvivorshipEvidence] = Field(default_factory=list)
    risk_policy_evidence: list[RiskPolicyEvidence] = Field(default_factory=list)
    embargo_evidence: list[EmbargoEvidence] = Field(default_factory=list)
    candidate_authority: list[CandidateAuthorityReport] = Field(default_factory=list)
    oos_classifications: list[OOSClassification] = Field(default_factory=list)
    oos_intervals: list[OOSInterval] = Field(default_factory=list)
    phase0a_reason_codes: list[str] = Field(default_factory=list)
    upstream_reason_codes: list[str] = Field(default_factory=list)


class AuditReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    audit_id: str
    audit_policy_version: str
    request_hash: str
    audit_manifest_hash: str
    result_hash: str
    results: list[TargetAuditResult]
    phase0a_reason_codes: list[str] = Field(default_factory=list)
    upstream_reason_codes: list[str] = Field(default_factory=list)


class HandoffAdmissionScope(BaseModel):
    """One deterministic Phase 1 admission decision derived from Phase 0A evidence."""

    model_config = ConfigDict(extra="forbid")

    admission_scope_id: str
    admission_scope_hash: str
    audit_target_id: str
    target_scope_hash: str
    phase0a_signal_context_hash: str | None = None
    oos_interval_id: str
    oos_interval_hash: str
    capability: str
    capability_hash: str
    date_start: date
    date_end: date
    formal_oos_status: FormalOOSStatus
    signal_evidence_level: CandidateAuthorityStatus
    evidence_scope: HandoffEvidenceScope
    readiness: HandoffReadiness
    stable_signal_semantics_payload_v1: dict[str, Any] | None = None
    stable_signal_semantics_hash: str | None = None
    decision_clock_hash: str | None = None
    blocking_reason_codes: list[str] = Field(default_factory=list)


class HandoffTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    audit_target_id: str
    target_scope_hash: str
    admission_scopes: list[HandoffAdmissionScope] = Field(default_factory=list)
    target_handoff_hash: str


class HandoffReadinessReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = "advisory_phase0a_handoff_readiness_v1"
    audit_id: str
    audit_manifest_hash: str
    request_hash: str
    readiness: HandoffReadiness
    sorted_target_handoffs: list[HandoffTarget] = Field(default_factory=list)
    global_handoff_hashes: dict[str, str] = Field(default_factory=dict)
    blocking_reason_codes: list[str] = Field(default_factory=list)
    handoff_readiness_hash: str


class Phase1HandoffBundle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = "advisory_phase0a_handoff_bundle_v2"
    audit_id: str
    audit_manifest_hash: str
    request_hash: str
    serializer_version: str
    global_handoff_hashes: dict[str, str] = Field(default_factory=dict)
    sorted_target_handoffs: list[HandoffTarget] = Field(default_factory=list)
    admission_scope_set_hash: str
    handoff_readiness_report_hash: str
    phase1_handoff_bundle_hash: str
    created_at: datetime
