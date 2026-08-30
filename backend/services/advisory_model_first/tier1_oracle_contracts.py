from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.feature_schema_v2 import feature_schema_hash
from backend.services.advisory_model_first.policy_contracts import (
    AdvisoryPolicyCostV1,
    AdvisoryPolicySplitV1,
)
from backend.services.advisory_model_first.policy_utility_contracts import FrozenDataIdentityV1
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
N1_DATASET_IDENTITY = "81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd"
N1_WINDOW_ID = "P0C_DEVELOPMENT_V1"
N1_DECISION_START = date(2024, 7, 4)
N1_DECISION_END = date(2026, 2, 2)
N1_DATA_CUTOFF = date(2026, 3, 10)
N1_RESOURCE_LIMIT_BYTES = 8 * 1024**3


class Tier1EvidenceState(str, Enum):
    HIGH = "HIGH"
    LOW = "LOW"
    INCONCLUSIVE = "INCONCLUSIVE"


class Tier1Quadrant(str, Enum):
    THEORETICAL_LOW_LEARNABILITY_LOW = "THEORETICAL_LOW__LEARNABILITY_LOW"
    THEORETICAL_HIGH_LEARNABILITY_LOW = "THEORETICAL_HIGH__LEARNABILITY_LOW"
    THEORETICAL_HIGH_LEARNABILITY_HIGH = "THEORETICAL_HIGH__LEARNABILITY_HIGH"
    THEORETICAL_LOW_LEARNABILITY_HIGH_ANOMALY = "THEORETICAL_LOW__LEARNABILITY_HIGH_ANOMALY"


class Tier1PitSnapshotIdentityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_ref: EvidenceReferenceV1
    spans_sha256: str = Field(pattern=SHA256_PATTERN)
    source_fingerprint_sha256: str = Field(pattern=SHA256_PATTERN)
    parameter_hash: str = Field(pattern=SHA256_PATTERN)
    universe_key: Literal["aistock_equity_pit_canonical_v2"] = "aistock_equity_pit_canonical_v2"
    rule_version: Literal["shsz_a_252td_st_delist_asof_v2"] = "shsz_a_252td_st_delist_asof_v2"
    scope_start: date
    cutoff: date
    span_count: int = Field(gt=0)
    instrument_count: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_scope(self) -> "Tier1PitSnapshotIdentityV1":
        if self.scope_start != N1_DECISION_START or self.cutoff != N1_DATA_CUTOFF:
            raise ValueError("N1 PIT snapshot must exactly cover the development window")
        if self.instrument_count > self.span_count:
            raise ValueError("PIT instrument_count cannot exceed span_count")
        return self


class Tier1OutcomePolicyV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_tier1_outcome_policy_v1"] = "advisory_tier1_outcome_policy_v1"
    label_semantics: Literal["ADVISORY_TIER1_H20_OPEN_TO_OPEN_V1"] = "ADVISORY_TIER1_H20_OPEN_TO_OPEN_V1"
    entry_offset_trading_days: Literal[1] = 1
    holding_period_trading_days: Literal[20] = 20
    max_exit_defer_trading_days: Literal[5] = 5
    candidate_depths: tuple[Literal[20, 40, 50], ...] = (20, 40, 50)
    winner_count: Literal[5] = 5
    target_slot_count: Literal[5] = 5
    capacity_haircut_bps: Literal[5.0] = 5.0
    minimum_economic_benefit_bps: Literal[5.0] = 5.0
    unavailable_entry_slot_return_bps: Literal[0.0] = 0.0
    minimum_full_universe_known_fraction: Literal[0.95] = 0.95
    benchmark_instrument: Literal["000300.SH"] = "000300.SH"

    @model_validator(mode="after")
    def validate_depths(self) -> "Tier1OutcomePolicyV1":
        if self.candidate_depths != (20, 40, 50):
            raise ValueError("N1 candidate depths must be exactly 20/40/50")
        return self


class Tier1InferencePolicyV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_tier1_inference_policy_v1"] = "advisory_tier1_inference_policy_v1"
    confidence_level: Literal[0.95] = 0.95
    block_length_trading_days: Literal[20] = 20
    bootstrap_repetitions: Literal[2000] = 2000
    random_seed: Literal[20260831] = 20260831
    two_sided_alpha: Literal[0.05] = 0.05
    target_power: Literal[0.8] = 0.8
    min_intervention_days: Literal[60] = 60
    min_intervention_fraction: Literal[0.25] = 0.25
    min_intervention_days_per_observed_regime: Literal[20] = 20
    regime_semantics: Literal["CSI300_TRAILING20_CLOSE_RETURN_SIGN_AT_T_V1"] = (
        "CSI300_TRAILING20_CLOSE_RETURN_SIGN_AT_T_V1"
    )


class Tier1LearnabilitySpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_tier1_learnability_spec_v1"] = "advisory_tier1_learnability_spec_v1"
    feature_schema_version: Literal["advisory_feature_schema_v2_suspension_aware"] = (
        "advisory_feature_schema_v2_suspension_aware"
    )
    estimator_family: Literal["SKLEARN_RIDGE_V1"] = "SKLEARN_RIDGE_V1"
    alpha: Literal[100.0] = 100.0
    solver: Literal["svd"] = "svd"
    fit_intercept: Literal[True] = True
    numeric_imputer: Literal["TRAIN_FOLD_MEDIAN"] = "TRAIN_FOLD_MEDIAN"
    numeric_scaler: Literal["TRAIN_FOLD_STANDARD_SCALER"] = "TRAIN_FOLD_STANDARD_SCALER"
    categorical_encoder: Literal["TRAIN_FOLD_ONE_HOT_UNKNOWN_IGNORE_DENSE"] = "TRAIN_FOLD_ONE_HOT_UNKNOWN_IGNORE_DENSE"
    oof_aggregation: Literal["MEAN_ACROSS_VALIDATION_PATHS"] = "MEAN_ACROSS_VALIDATION_PATHS"
    expected_ready_path_count: Literal[28] = 28
    expected_oof_predictions_per_row: Literal[7] = 7
    model_trial_count: Literal[1] = 1


class AdvisoryN1Tier1RequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_n1_tier1_request_v1"] = "frozen_advisory_n1_tier1_request_v1"
    request_id: str = Field(pattern=r"^advn1req_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: Literal[ObjectiveContract.ALPHA_RANKING] = ObjectiveContract.ALPHA_RANKING
    n0_completion_ref: EvidenceReferenceV1
    n0_completion_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    research_window_contract_ref: EvidenceReferenceV1
    research_window_contract_sha256: str = Field(pattern=SHA256_PATTERN)
    research_window_contract_path: str = Field(min_length=1)
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    policy_dataset_bundle_root: str = Field(min_length=1)
    policy_dataset_bundle_id: Literal[N1_DATASET_IDENTITY] = N1_DATASET_IDENTITY
    policy_dataset_manifest_file_sha256: str = Field(pattern=SHA256_PATTERN)
    policy_dataset_request_sha256: str = Field(pattern=SHA256_PATTERN)
    program_id: str = Field(min_length=1)
    binding_version_id: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    selection_runtime_semantics_hash: str = Field(pattern=SHA256_PATTERN)
    style_profile_id: str = Field(min_length=1)
    style_profile_hash: str = Field(pattern=SHA256_PATTERN)
    baseline_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    shadow_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy: AdvisoryPolicyCostV1
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    split_policy: AdvisoryPolicySplitV1
    split_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    representative_seed_run_ids: dict[str, str]
    prediction_artifacts: dict[str, PredictionArtifactDescriptor]
    terminal_weights: dict[str, float]
    pit_snapshot: Tier1PitSnapshotIdentityV1
    qlib_daily_root: str = Field(min_length=1)
    factor_data_root: str = Field(min_length=1)
    factor_data_cutoff: date
    suspend_data_root: str = Field(min_length=1)
    prediction_store_root: str = Field(min_length=1)
    market_calendar_identity: FrozenDataIdentityV1
    suspend_sidecar_identity: FrozenDataIdentityV1
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    decision_date_start: date = N1_DECISION_START
    decision_date_end: date = N1_DECISION_END
    data_cutoff: date = N1_DATA_CUTOFF
    window_id: Literal[N1_WINDOW_ID] = N1_WINDOW_ID
    dataset_identity: Literal[N1_DATASET_IDENTITY] = N1_DATASET_IDENTITY
    policy_rank_semantics_version: Literal["advisory_exact_weighted_pit_top50_v1"] = (
        "advisory_exact_weighted_pit_top50_v1"
    )
    outcome_policy: Tier1OutcomePolicyV1 = Field(default_factory=Tier1OutcomePolicyV1)
    inference_policy: Tier1InferencePolicyV1 = Field(default_factory=Tier1InferencePolicyV1)
    learnability_spec: Tier1LearnabilitySpecV1 = Field(default_factory=Tier1LearnabilitySpecV1)
    resource_max_rss_bytes: Literal[N1_RESOURCE_LIMIT_BYTES] = N1_RESOURCE_LIMIT_BYTES

    @model_validator(mode="after")
    def validate_identity(self) -> "AdvisoryN1Tier1RequestV1":
        if (
            self.decision_date_start != N1_DECISION_START
            or self.decision_date_end != N1_DECISION_END
            or self.data_cutoff != N1_DATA_CUTOFF
        ):
            raise ValueError("N1 must use the exact frozen development dates")
        if self.factor_data_cutoff < self.data_cutoff:
            raise ValueError("factor data cutoff does not cover N1")
        if self.market_calendar_identity.identity_kind != "MARKET_CALENDAR":
            raise ValueError("market calendar identity kind is invalid")
        if self.suspend_sidecar_identity.identity_kind != "SUSPEND_SIDECAR":
            raise ValueError("suspend sidecar identity kind is invalid")
        for identity in (self.market_calendar_identity, self.suspend_sidecar_identity):
            if date.fromisoformat(identity.cutoff_trade_date) < self.data_cutoff:
                raise ValueError("calendar/suspend identity does not cover N1 cutoff")
        expected_feature_hash = feature_schema_hash(
            market_calendar_identity=self.market_calendar_identity.model_dump(mode="json"),
            suspend_sidecar_identity=self.suspend_sidecar_identity.model_dump(mode="json"),
        )
        if self.feature_schema_hash != expected_feature_hash:
            raise ValueError("N1 feature schema hash differs from frozen data identities")
        if self.cost_policy_sha256 != self.cost_policy.policy_sha256:
            raise ValueError("N1 cost policy hash mismatch")
        if self.split_policy_sha256 != self.split_policy.policy_sha256:
            raise ValueError("N1 split policy hash mismatch")
        if self.split_policy.group_count != 8 or self.split_policy.validation_group_count != 2:
            raise ValueError("N1 requires the exact 8-group/2-validation-group CPCV")
        if self.split_policy.embargo_trading_days != 20:
            raise ValueError("N1 requires the exact 20-trading-day embargo")
        if set(self.representative_seed_run_ids) != set(self.terminal_weights):
            raise ValueError("N1 representative leg roster differs from terminal weights")
        if set(self.prediction_artifacts) != set(self.representative_seed_run_ids.values()):
            raise ValueError("N1 prediction descriptors differ from representative runs")
        weight_sum = sum(float(value) for value in self.terminal_weights.values())
        if abs(weight_sum - 1.0) > 1e-10 or any(float(value) <= 0 for value in self.terminal_weights.values()):
            raise ValueError("N1 terminal weights must be positive and sum to one")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected:
            raise ValueError("N1 request_sha256 mismatch")
        if self.request_id != f"advn1req_{expected[:24]}":
            raise ValueError("N1 request_id mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at"},
        )


def build_n1_tier1_request(**values: Any) -> AdvisoryN1Tier1RequestV1:
    payload = dict(values)
    payload.setdefault("schema_version", "frozen_advisory_n1_tier1_request_v1")
    payload.setdefault("created_at", datetime.now(timezone.utc))
    draft = {key: value for key, value in payload.items() if key not in {"request_id", "request_sha256", "created_at"}}
    normalized = AdvisoryN1Tier1RequestV1.model_construct(
        request_id="advn1req_" + "0" * 24,
        request_sha256="0" * 64,
        created_at=payload["created_at"],
        **draft,
    ).model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})
    digest = canonical_json_sha256(normalized)
    payload["request_sha256"] = digest
    payload["request_id"] = f"advn1req_{digest[:24]}"
    return AdvisoryN1Tier1RequestV1.model_validate(payload)


class Tier1MetricInferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    point_estimate_bps: float
    confidence_lower_bps: float
    confidence_upper_bps: float
    bootstrap_standard_error_bps: float = Field(ge=0)
    mde_bps: float = Field(ge=0)
    economic_threshold_bps: float = Field(ge=0)
    evidence_state: Tier1EvidenceState
    evaluated_day_count: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_interval(self) -> "Tier1MetricInferenceV1":
        if not (self.confidence_lower_bps <= self.point_estimate_bps <= self.confidence_upper_bps):
            raise ValueError("point estimate must lie inside the confidence interval")
        expected = (
            Tier1EvidenceState.HIGH
            if self.confidence_lower_bps > self.economic_threshold_bps
            else Tier1EvidenceState.LOW
            if self.confidence_upper_bps <= self.economic_threshold_bps
            else Tier1EvidenceState.INCONCLUSIVE
        )
        if self.evidence_state != expected:
            raise ValueError("evidence_state differs from the frozen threshold rule")
        return self


class Tier1InterventionSupportV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    evaluated_day_count: int = Field(gt=0)
    intervention_day_count: int = Field(ge=0)
    intervention_fraction: float = Field(ge=0, le=1)
    intervention_days_by_regime: dict[str, int]
    minimum_day_count: int = Field(ge=0)
    minimum_fraction: float = Field(ge=0, le=1)
    minimum_days_per_observed_regime: int = Field(ge=0)
    support_sufficient: bool
    reason_codes: tuple[str, ...]


class AdvisoryTier1OracleReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_tier1_oracle_receipt_v1"] = "advisory_tier1_oracle_receipt_v1"
    receipt_id: str = Field(pattern=r"^advn1oracle_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    deployable: Literal[False] = False
    decision_date_count: int = Field(gt=0)
    universe_summary: dict[str, Any]
    recall_summary: dict[str, Any]
    rank_bucket_summary: tuple[dict[str, Any], ...]
    perfect_top5_lift: Tier1MetricInferenceV1
    intervention_support: Tier1InterventionSupportV1
    evidence_sufficient: bool
    evidence_reason_codes: tuple[str, ...]
    result_class: ResearchResultClass
    decision_use: DecisionUse
    sealed_holdout_accessed: Literal[False] = False
    created_at: datetime
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryTier1OracleReceiptV1":
        if self.decision_use == DecisionUse.ACTIVATION_EVIDENCE:
            raise ValueError("oracle receipt cannot be activation evidence")
        if self.evidence_sufficient != (
            self.intervention_support.support_sufficient
            and self.perfect_top5_lift.mde_bps
            <= max(
                self.perfect_top5_lift.point_estimate_bps,
                self.perfect_top5_lift.economic_threshold_bps,
            )
        ):
            raise ValueError("oracle evidence_sufficient differs from frozen rules")
        return _validate_hashed_receipt(self, "advn1oracle_")

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "created_at", "receipt_sha256"})


class AdvisoryTier1LearnabilityReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_tier1_learnability_receipt_v1"] = "advisory_tier1_learnability_receipt_v1"
    receipt_id: str = Field(pattern=r"^advn1learn_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    deployable: Literal[False] = False
    model_family: Literal["SKLEARN_RIDGE_V1"] = "SKLEARN_RIDGE_V1"
    model_trial_count: Literal[1] = 1
    ready_path_count: Literal[28] = 28
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    oof_row_count: int = Field(gt=0)
    oof_predictions_per_row: Literal[7] = 7
    learnability_lift: Tier1MetricInferenceV1
    intervention_support: Tier1InterventionSupportV1
    evidence_sufficient: bool
    evidence_reason_codes: tuple[str, ...]
    result_class: ResearchResultClass
    decision_use: DecisionUse
    sealed_holdout_accessed: Literal[False] = False
    created_at: datetime
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryTier1LearnabilityReceiptV1":
        if self.decision_use == DecisionUse.ACTIVATION_EVIDENCE:
            raise ValueError("learnability receipt cannot be activation evidence")
        if self.evidence_sufficient != (
            self.intervention_support.support_sufficient
            and self.learnability_lift.mde_bps
            <= max(
                self.learnability_lift.point_estimate_bps,
                self.learnability_lift.economic_threshold_bps,
            )
        ):
            raise ValueError("learnability evidence_sufficient differs from frozen rules")
        return _validate_hashed_receipt(self, "advn1learn_")

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "created_at", "receipt_sha256"})


class AdvisoryTier1QuadrantReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_tier1_quadrant_receipt_v1"] = "advisory_tier1_quadrant_receipt_v1"
    receipt_id: str = Field(pattern=r"^advn1quadrant_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    oracle_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    learnability_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    point_quadrant: Tier1Quadrant
    typed_result: str = Field(min_length=1)
    direction_ready: bool
    reason_codes: tuple[str, ...]
    next_task: Literal["N2_ENTRY_EXIT_QE_PREPARATION"] = "N2_ENTRY_EXIT_QE_PREPARATION"
    sealed_holdout_accessed: Literal[False] = False
    created_at: datetime
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryTier1QuadrantReceiptV1":
        return _validate_hashed_receipt(self, "advn1quadrant_")

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "created_at", "receipt_sha256"})


def build_oracle_receipt(**values: Any) -> AdvisoryTier1OracleReceiptV1:
    return _build_receipt(AdvisoryTier1OracleReceiptV1, "advn1oracle_", values)


def build_learnability_receipt(**values: Any) -> AdvisoryTier1LearnabilityReceiptV1:
    return _build_receipt(AdvisoryTier1LearnabilityReceiptV1, "advn1learn_", values)


def build_quadrant_receipt(**values: Any) -> AdvisoryTier1QuadrantReceiptV1:
    return _build_receipt(AdvisoryTier1QuadrantReceiptV1, "advn1quadrant_", values)


def _build_receipt(model_type: type[BaseModel], prefix: str, values: dict[str, Any]) -> Any:
    payload = dict(values)
    payload.setdefault("created_at", datetime.now(timezone.utc))
    functional_fields = set(model_type.model_fields) - {
        "receipt_id",
        "created_at",
        "receipt_sha256",
    }
    normalized = model_type.model_construct(
        receipt_id=prefix + "0" * 24,
        receipt_sha256="0" * 64,
        created_at=payload["created_at"],
        **{key: value for key, value in payload.items() if key in functional_fields},
    ).model_dump(mode="json", exclude={"receipt_id", "created_at", "receipt_sha256"})
    digest = canonical_json_sha256(normalized)
    payload["receipt_sha256"] = digest
    payload["receipt_id"] = prefix + digest[:24]
    return model_type.model_validate(payload)


def _validate_hashed_receipt(receipt: Any, prefix: str) -> Any:
    expected = canonical_json_sha256(receipt.functional_payload())
    if receipt.receipt_sha256 != expected:
        raise ValueError("receipt_sha256 mismatch")
    if receipt.receipt_id != prefix + expected[:24]:
        raise ValueError("receipt_id mismatch")
    return receipt


__all__ = [
    "AdvisoryN1Tier1RequestV1",
    "AdvisoryTier1LearnabilityReceiptV1",
    "AdvisoryTier1OracleReceiptV1",
    "AdvisoryTier1QuadrantReceiptV1",
    "N1_DATASET_IDENTITY",
    "N1_DATA_CUTOFF",
    "N1_DECISION_END",
    "N1_DECISION_START",
    "N1_RESOURCE_LIMIT_BYTES",
    "N1_WINDOW_ID",
    "Tier1EvidenceState",
    "Tier1InferencePolicyV1",
    "Tier1InterventionSupportV1",
    "Tier1LearnabilitySpecV1",
    "Tier1MetricInferenceV1",
    "Tier1OutcomePolicyV1",
    "Tier1PitSnapshotIdentityV1",
    "Tier1Quadrant",
    "build_learnability_receipt",
    "build_n1_tier1_request",
    "build_oracle_receipt",
    "build_quadrant_receipt",
]
