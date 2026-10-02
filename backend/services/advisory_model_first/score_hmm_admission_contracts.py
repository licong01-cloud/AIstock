from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
SCORE_HMM_EXPERIMENT_ID = "ADVISORY-N3-AUX-SCORE-HMM-ADMISSION-V1"
SCORE_HMM_HYPOTHESIS_FAMILY_ID = "ADVISORY-N3-AUX-SCORE-HMM-V1"
SCORE_HMM_RESEARCH_STAGE = "N3_AUX_SCORE_HMM_CONDITIONED_ADMISSION"
SCORE_HMM_DECISION_START = date(2024, 7, 4)
SCORE_HMM_DECISION_END = date(2026, 2, 2)
SCORE_HMM_DATA_CUTOFF = date(2026, 3, 10)
SCORE_HMM_MARKET_HISTORY_START = date(2023, 9, 1)
SCORE_HMM_EXPECTED_DECISION_DAYS = 386
SCORE_HMM_EXPECTED_TOP50_ROWS = 19_300
SCORE_HMM_EXPECTED_TOP20_ROWS = 7_720
SCORE_HMM_EXPECTED_TOP5_ROWS = 1_930
SCORE_HMM_EXPECTED_PATHS = 28
SCORE_HMM_EXPECTED_OOF_PER_ROW = 7
SCORE_HMM_HMM_WARMUP_DAYS = 60
SCORE_HMM_MODEL_TRIAL_COUNT = 5
SCORE_HMM_EXECUTABLE_TRIAL_COUNT = 3
SCORE_HMM_BLOCK_LENGTH = 20
SCORE_HMM_BOOTSTRAP_REPETITIONS = 2_000
SCORE_HMM_BOOTSTRAP_SEED = 20260905
SCORE_HMM_MAX_RSS_BYTES = 8 * 1024**3
SCORE_HMM_MAX_TEMP_BYTES = 8 * 1024**3
SCORE_HMM_PRIMARY_TARGET_ID = "POLICY_EPISODE_NET_RETURN_BPS_MAX20_V1"
SCORE_HMM_SECONDARY_HORIZONS = (1, 5, 10, 20)

PACKAGE_SCORE_CALIBRATION_ONLY = "PACKAGE_SCORE_CALIBRATION_ONLY"
SCORE_PLUS_RAW_MARKET_SHAPE = "SCORE_PLUS_RAW_MARKET_SHAPE"
SCORE_PLUS_MARKET_HMM = "SCORE_PLUS_MARKET_HMM"
SCORE_PLUS_SECTOR_HMM = "SCORE_PLUS_SECTOR_HMM"
SCORE_PLUS_MARKET_AND_SECTOR_HMM = "SCORE_PLUS_MARKET_AND_SECTOR_HMM"

SCORE_HMM_ARM_IDS = (
    PACKAGE_SCORE_CALIBRATION_ONLY,
    SCORE_PLUS_RAW_MARKET_SHAPE,
    SCORE_PLUS_MARKET_HMM,
    SCORE_PLUS_SECTOR_HMM,
    SCORE_PLUS_MARKET_AND_SECTOR_HMM,
)
SCORE_HMM_EXECUTABLE_ARM_IDS = SCORE_HMM_ARM_IDS[:3]
SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS = SCORE_HMM_ARM_IDS[3:]

SCORE_FEATURE_COLUMNS = (
    "parent_rank_pct_top20",
    "parent_score_percentile_top50",
    "parent_score_robust_z_top50",
    "parent_score_gap_to_rank6_iqr",
    "lstm_rank_pct_top50",
    "fund_rank_pct_top50",
    "leg_rank_gap_pct",
    "leg_norm_gap_abs_robust",
    "day_top5_vs_rank6_gap_iqr",
    "day_top20_iqr_over_top50_iqr",
    "day_top20_score_range_over_iqr",
    "day_top5_minus_top20_mean_over_iqr",
)
RAW_MARKET_FEATURE_COLUMNS = (
    "csi300_ret_1",
    "csi300_ret_5",
    "csi300_ret_20",
    "csi300_drawdown_20",
    "csi300_drawdown_60",
    "market_up_ratio",
    "market_limit_up_ratio",
    "market_cross_section_vol",
)
MARKET_HMM_FEATURE_COLUMNS = (
    "market_risk_on_posterior",
    "market_state",
    "market_state_duration",
    "market_hmm_observation_completeness",
)
SECTOR_HMM_FEATURE_COLUMNS = (
    "sector_rotation_score",
    "sector_forecast_state",
    "sector_prediction_availability",
)
COMBINED_INTERACTION_COLUMNS = (
    "parent_rank_pct_x_market_risk_on_posterior",
    "parent_rank_pct_x_sector_rotation_score",
    "market_risk_on_posterior_x_sector_rotation_score",
)

SCORE_HMM_FEATURES_BY_ARM = {
    PACKAGE_SCORE_CALIBRATION_ONLY: SCORE_FEATURE_COLUMNS,
    SCORE_PLUS_RAW_MARKET_SHAPE: (*SCORE_FEATURE_COLUMNS, *RAW_MARKET_FEATURE_COLUMNS),
    SCORE_PLUS_MARKET_HMM: (*SCORE_FEATURE_COLUMNS, *RAW_MARKET_FEATURE_COLUMNS, *MARKET_HMM_FEATURE_COLUMNS),
    SCORE_PLUS_SECTOR_HMM: (*SCORE_FEATURE_COLUMNS, *RAW_MARKET_FEATURE_COLUMNS, *SECTOR_HMM_FEATURE_COLUMNS),
    SCORE_PLUS_MARKET_AND_SECTOR_HMM: (
        *SCORE_FEATURE_COLUMNS,
        *RAW_MARKET_FEATURE_COLUMNS,
        *MARKET_HMM_FEATURE_COLUMNS,
        *SECTOR_HMM_FEATURE_COLUMNS,
        *COMBINED_INTERACTION_COLUMNS,
    ),
}
SCORE_HMM_PREDECESSOR_BY_ARM: dict[str, tuple[str, ...]] = {
    PACKAGE_SCORE_CALIBRATION_ONLY: (),
    SCORE_PLUS_RAW_MARKET_SHAPE: (PACKAGE_SCORE_CALIBRATION_ONLY,),
    SCORE_PLUS_MARKET_HMM: (SCORE_PLUS_RAW_MARKET_SHAPE,),
    SCORE_PLUS_SECTOR_HMM: (SCORE_PLUS_RAW_MARKET_SHAPE,),
    SCORE_PLUS_MARKET_AND_SECTOR_HMM: (SCORE_PLUS_MARKET_HMM, SCORE_PLUS_SECTOR_HMM),
}
SCORE_HMM_FEATURE_SCHEMA_VERSION = "advisory_score_hmm_admission_feature_schema_v1"
SCORE_HMM_ARM_SCHEMA_HASHES = {
    arm_id: canonical_json_sha256(
        {
            "schema_version": SCORE_HMM_FEATURE_SCHEMA_VERSION,
            "arm_id": arm_id,
            "feature_columns": list(SCORE_HMM_FEATURES_BY_ARM[arm_id]),
            "parent_candidate_depth": 20,
            "score_distribution_depth": 50,
            "action_depth": 5,
            "score_transform": "SAME_DAY_AFFINE_INVARIANT_V1",
            "source_clock": "T_CLOSE_VISIBLE_ONLY",
        }
    )
    for arm_id in SCORE_HMM_ARM_IDS
}

SCORE_HMM_EVIDENCE_ROLES = (
    "score_hmm_n1_manifest",
    "score_hmm_n1_request",
    "score_hmm_n1_rankings",
    "score_hmm_n1_cpcv",
    "score_hmm_n1_regime_daily",
    "score_hmm_policy_manifest",
    "score_hmm_policy_request",
    "score_hmm_policy_labels",
    "score_hmm_policy_rankings",
    "score_hmm_policy_shadow_daily",
    "score_hmm_policy_shadow_episodes",
    "score_hmm_policy_baseline",
    "score_hmm_policy_shadow",
    "score_hmm_policy_cost",
    "score_hmm_pit_snapshot",
    "score_hmm_market_warmup_pit_snapshot",
    "score_hmm_trial_registry",
    "score_hmm_main_route",
)


class ScoreHMMAdmissionArmSpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_score_hmm_admission_arm_spec_v1"] = (
        "advisory_score_hmm_admission_arm_spec_v1"
    )
    arm_id: Literal[
        "PACKAGE_SCORE_CALIBRATION_ONLY",
        "SCORE_PLUS_RAW_MARKET_SHAPE",
        "SCORE_PLUS_MARKET_HMM",
        "SCORE_PLUS_SECTOR_HMM",
        "SCORE_PLUS_MARKET_AND_SECTOR_HMM",
    ]
    trial_candidate_index: int = Field(gt=0)
    feature_columns: tuple[str, ...]
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    predecessor_arm_ids: tuple[str, ...]
    run_status: Literal["RUN", "NOT_RUN_SOURCE_UNAVAILABLE"]
    source_requirement: Literal["PACKAGE_SCORE", "RAW_MARKET", "MARKET_HMM", "SECTOR_HMM", "MARKET_AND_SECTOR_HMM"]

    @model_validator(mode="after")
    def validate_arm(self) -> "ScoreHMMAdmissionArmSpecV1":
        expected_run = "RUN" if self.arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS else "NOT_RUN_SOURCE_UNAVAILABLE"
        expected_source = {
            PACKAGE_SCORE_CALIBRATION_ONLY: "PACKAGE_SCORE",
            SCORE_PLUS_RAW_MARKET_SHAPE: "RAW_MARKET",
            SCORE_PLUS_MARKET_HMM: "MARKET_HMM",
            SCORE_PLUS_SECTOR_HMM: "SECTOR_HMM",
            SCORE_PLUS_MARKET_AND_SECTOR_HMM: "MARKET_AND_SECTOR_HMM",
        }[self.arm_id]
        if (
            self.feature_columns != SCORE_HMM_FEATURES_BY_ARM[self.arm_id]
            or self.feature_schema_hash != SCORE_HMM_ARM_SCHEMA_HASHES[self.arm_id]
            or self.predecessor_arm_ids != SCORE_HMM_PREDECESSOR_BY_ARM[self.arm_id]
            or self.run_status != expected_run
            or self.source_requirement != expected_source
        ):
            raise ValueError("score/HMM arm identity drift")
        return self


def build_default_score_hmm_arms(
    reserved_candidate_indices: tuple[int, int, int, int, int],
) -> tuple[ScoreHMMAdmissionArmSpecV1, ...]:
    if len(reserved_candidate_indices) != SCORE_HMM_MODEL_TRIAL_COUNT:
        raise ValueError("score/HMM trial reservation must contain five indices")
    source_by_arm = {
        PACKAGE_SCORE_CALIBRATION_ONLY: "PACKAGE_SCORE",
        SCORE_PLUS_RAW_MARKET_SHAPE: "RAW_MARKET",
        SCORE_PLUS_MARKET_HMM: "MARKET_HMM",
        SCORE_PLUS_SECTOR_HMM: "SECTOR_HMM",
        SCORE_PLUS_MARKET_AND_SECTOR_HMM: "MARKET_AND_SECTOR_HMM",
    }
    return tuple(
        ScoreHMMAdmissionArmSpecV1(
            arm_id=arm_id,
            trial_candidate_index=reserved_candidate_indices[index],
            feature_columns=SCORE_HMM_FEATURES_BY_ARM[arm_id],
            feature_schema_hash=SCORE_HMM_ARM_SCHEMA_HASHES[arm_id],
            predecessor_arm_ids=SCORE_HMM_PREDECESSOR_BY_ARM[arm_id],
            run_status="RUN" if arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS else "NOT_RUN_SOURCE_UNAVAILABLE",
            source_requirement=source_by_arm[arm_id],
        )
        for index, arm_id in enumerate(SCORE_HMM_ARM_IDS)
    )


class AdvisoryAdmissionDecisionV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_admission_decision_v1"] = "advisory_admission_decision_v1"
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    arm_id: str = Field(min_length=1)
    decision_as_of_trade_date: date
    target_trade_date: date
    instrument: str = Field(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")
    parent_rank: int = Field(ge=1, le=5)
    action: Literal["TAKE", "SKIP", "UNAVAILABLE"]
    reason_code: Literal[
        "TAKE_POSITIVE_VALUE",
        "SKIP_NONPOSITIVE_LOWER_BOUND",
        "SKIP_NONPOSITIVE_PROBABILITY",
        "MODEL_INPUT_UNAVAILABLE",
        "NOT_RUN_SOURCE_UNAVAILABLE",
    ]
    arm_available: bool
    label_evaluable: bool
    evaluation_reason_code: Literal["LABEL_NOT_EVALUABLE"] | None
    primary_expected_net_return_bps: float | None
    primary_expected_net_return_lcb80_bps: float | None
    primary_positive_probability: float | None

    @model_validator(mode="after")
    def validate_decision(self) -> "AdvisoryAdmissionDecisionV1":
        values = (
            self.primary_expected_net_return_bps,
            self.primary_expected_net_return_lcb80_bps,
            self.primary_positive_probability,
        )
        if self.action == "TAKE":
            if (
                not self.arm_available
                or self.reason_code != "TAKE_POSITIVE_VALUE"
                or any(value is None for value in values)
                or float(self.primary_expected_net_return_lcb80_bps) <= 0.0
                or float(self.primary_positive_probability) < 0.5
            ):
                raise ValueError("TAKE decision violates the frozen admission rule")
        elif self.action == "SKIP":
            if not self.arm_available or self.reason_code not in {
                "SKIP_NONPOSITIVE_LOWER_BOUND",
                "SKIP_NONPOSITIVE_PROBABILITY",
            }:
                raise ValueError("SKIP decision must be a valid model decision")
        elif self.arm_available or self.reason_code not in {
            "MODEL_INPUT_UNAVAILABLE",
            "NOT_RUN_SOURCE_UNAVAILABLE",
        }:
            raise ValueError("UNAVAILABLE decision must expose a typed source/model reason")
        if self.primary_positive_probability is not None and not 0.0 <= self.primary_positive_probability <= 1.0:
            raise ValueError("admission probability is outside [0, 1]")
        if self.evaluation_reason_code != (None if self.label_evaluable else "LABEL_NOT_EVALUABLE"):
            raise ValueError("admission evaluation status is inconsistent")
        return self


class FrozenAdvisoryScoreHMMAdmissionRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_score_hmm_admission_request_v1"] = (
        "frozen_advisory_score_hmm_admission_request_v1"
    )
    request_id: str = Field(pattern=r"^advscorehmm_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: ObjectiveContract = ObjectiveContract.RISK_MANAGED_ADVISORY
    study_type: ResearchStudyType = ResearchStudyType.LEARNABILITY_AUDIT
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    experiment_id: Literal[SCORE_HMM_EXPERIMENT_ID] = SCORE_HMM_EXPERIMENT_ID
    research_stage: Literal[SCORE_HMM_RESEARCH_STAGE] = SCORE_HMM_RESEARCH_STAGE
    planned_trial_count: Literal[5] = 5
    generated_trial_count: Literal[0] = 0
    evaluated_trial_count: Literal[0] = 0
    selected_trial_count: Literal[0] = 0
    executable_trial_count: Literal[3] = 3
    arm_specs: tuple[ScoreHMMAdmissionArmSpecV1, ...]

    program_id: str = Field(min_length=1)
    binding_version_id: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    style_profile_id: str = Field(min_length=1)
    style_profile_hash: str = Field(pattern=SHA256_PATTERN)
    package_asset_closure_hash: str = Field(pattern=SHA256_PATTERN)
    selection_runtime_semantics_id: str = Field(min_length=1)
    selection_runtime_semantics_hash: str = Field(pattern=SHA256_PATTERN)
    terminal_weights: dict[str, float]
    representative_model_asset_sha256: dict[str, str]

    n1_bundle_path: str = Field(min_length=1)
    n1_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n1_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_manifest_file_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_rankings_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_cpcv_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_regime_daily_sha256: str = Field(pattern=SHA256_PATTERN)
    policy_bundle_path: str = Field(min_length=1)
    policy_bundle_id: str = Field(pattern=SHA256_PATTERN)
    policy_request_sha256: str = Field(pattern=SHA256_PATTERN)
    policy_manifest_file_sha256: str = Field(pattern=SHA256_PATTERN)
    policy_labels_sha256: str = Field(pattern=SHA256_PATTERN)
    policy_rankings_sha256: str = Field(pattern=SHA256_PATTERN)
    baseline_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    shadow_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    split_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    policy_identity: str = Field(pattern=SHA256_PATTERN)

    pit_snapshot_path: str = Field(min_length=1)
    pit_snapshot_file_sha256: str = Field(pattern=SHA256_PATTERN)
    pit_spans_sha256: str = Field(pattern=SHA256_PATTERN)
    market_warmup_pit_snapshot_path: str = Field(min_length=1)
    market_warmup_pit_snapshot_file_sha256: str = Field(pattern=SHA256_PATTERN)
    market_warmup_pit_spans_sha256: str = Field(pattern=SHA256_PATTERN)
    qlib_daily_root: str = Field(min_length=1)
    factor_data_root: str = Field(min_length=1)
    suspend_data_root: str = Field(min_length=1)
    market_calendar_sha256: str = Field(pattern=SHA256_PATTERN)
    market_calendar_row_count: int = Field(gt=0)
    suspend_sidecar_sha256: str = Field(pattern=SHA256_PATTERN)
    suspend_sidecar_row_count: int = Field(gt=0)
    factor_schema_identity: str = Field(pattern=SHA256_PATTERN)
    trading_calendar: tuple[date, ...] = Field(min_length=606)
    trading_calendar_sha256: str = Field(pattern=SHA256_PATTERN)
    decision_start: date = SCORE_HMM_DECISION_START
    decision_end: date = SCORE_HMM_DECISION_END
    data_cutoff: date = SCORE_HMM_DATA_CUTOFF
    expected_decision_day_count: Literal[386] = 386
    expected_top50_row_count: Literal[19300] = 19_300
    expected_top20_row_count: Literal[7720] = 7_720
    expected_top5_row_count: Literal[1930] = 1_930

    primary_target_id: Literal[SCORE_HMM_PRIMARY_TARGET_ID] = SCORE_HMM_PRIMARY_TARGET_ID
    secondary_horizons: tuple[int, ...] = SCORE_HMM_SECONDARY_HORIZONS
    score_distribution_depth: Literal[50] = 50
    model_candidate_depth: Literal[20] = 20
    admission_action_depth: Literal[5] = 5
    ridge_alpha: Literal[100.0] = 100.0
    ridge_solver: Literal["lsqr"] = "lsqr"
    logistic_c: Literal[1.0] = 1.0
    logistic_solver: Literal["lbfgs"] = "lbfgs"
    logistic_max_iter: Literal[1000] = 1000
    model_random_state: Literal[20260905] = 20260905
    conformal_lower_quantile: Literal[0.20] = 0.20
    conformal_upper_quantile: Literal[0.80] = 0.80
    hmm_components: Literal[2] = 2
    hmm_covariance_type: Literal["full"] = "full"
    hmm_n_iter: Literal[200] = 200
    hmm_tol: Literal[0.0001] = 0.0001
    hmm_random_state: Literal[42] = 42
    hmm_min_covar: Literal[0.00001] = 0.00001
    hmm_warmup_trading_days: Literal[60] = 60
    expected_ready_path_count: Literal[28] = 28
    expected_oof_predictions_per_row: Literal[7] = 7

    minimum_paired_days: Literal[300] = 300
    minimum_intervention_days: Literal[60] = 60
    minimum_intervention_fraction: Literal[0.25] = 0.25
    minimum_take_days: Literal[60] = 60
    minimum_skip_days: Literal[60] = 60
    minimum_intervention_days_per_regime: Literal[20] = 20
    minimum_economic_lift_bps: Literal[5.0] = 5.0
    familywise_alpha: Literal[0.005] = 0.005
    pre_run_effective_sample_size: float = Field(gt=0.0)
    pre_run_mde_bps: float = Field(gt=0.0)
    pre_run_power_sufficient_for_5bps: bool
    block_length_trading_days: Literal[20] = 20
    bootstrap_repetitions: Literal[2000] = 2_000
    bootstrap_seed: Literal[20260905] = 20260905

    registry_path: str = Field(min_length=1)
    registry_sha256_at_request: str = Field(pattern=SHA256_PATTERN)
    registry_record_count_at_request: int = Field(ge=1)
    cumulative_evaluated_trial_count_prior: int = Field(ge=0)
    current_route_path: str = Field(min_length=1)
    current_route_sha256: str = Field(pattern=SHA256_PATTERN)
    current_route_next_task: Literal["N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION"] = (
        "N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION"
    )
    cumulative_candidate_index_prior: int = Field(ge=1)
    reserved_candidate_indices: tuple[int, int, int, int, int]
    auxiliary_route_path: str = Field(min_length=1)
    evidence_refs: tuple[EvidenceReferenceV1, ...]
    dataset_identity: str = Field(pattern=SHA256_PATTERN)

    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: Literal[SCORE_HMM_MAX_RSS_BYTES] = SCORE_HMM_MAX_RSS_BYTES
    resource_max_temp_bytes: Literal[SCORE_HMM_MAX_TEMP_BYTES] = SCORE_HMM_MAX_TEMP_BYTES
    resource_max_wall_seconds: Literal[None] = None
    database_read_allowed: Literal[False] = False
    database_write_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    tushare_read_allowed: Literal[False] = False
    qlib_read_allowed: Literal[True] = True
    sealed_holdout_accessed: Literal[False] = False
    factor_catalog_write_allowed: Literal[False] = False
    strategy_package_write_allowed: Literal[False] = False
    runtime_activation_allowed: Literal[False] = False
    selection_rank_change_allowed: Literal[False] = False
    position_weight_output_allowed: Literal[False] = False
    order_write_allowed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenAdvisoryScoreHMMAdmissionRequestV1":
        expected_indices = tuple(
            range(self.cumulative_candidate_index_prior + 1, self.cumulative_candidate_index_prior + 6)
        )
        roles = tuple(item.role for item in self.evidence_refs)
        if (
            self.objective_contract != ObjectiveContract.RISK_MANAGED_ADVISORY
            or self.study_type != ResearchStudyType.LEARNABILITY_AUDIT
            or self.decision_use != DecisionUse.NAVIGATION_ONLY
            or self.secondary_horizons != SCORE_HMM_SECONDARY_HORIZONS
            or self.reserved_candidate_indices != expected_indices
            or self.arm_specs != build_default_score_hmm_arms(self.reserved_candidate_indices)
            or roles != SCORE_HMM_EVIDENCE_ROLES
            or len(set(roles)) != len(roles)
        ):
            raise ValueError("score/HMM request contract drift")
        if (
            tuple(sorted(set(self.trading_calendar))) != self.trading_calendar
            or self.decision_start not in self.trading_calendar
            or self.decision_end not in self.trading_calendar
            or self.data_cutoff != self.trading_calendar[-1]
            or canonical_json_sha256({"market_sessions": [item.isoformat() for item in self.trading_calendar]})
            != self.trading_calendar_sha256
        ):
            raise ValueError("score/HMM trading calendar identity drift")
        for path, bundle_id in (
            (self.n1_bundle_path, self.n1_bundle_id),
            (self.policy_bundle_path, self.policy_bundle_id),
        ):
            if path.replace("\\", "/").rstrip("/").split("/")[-1] != bundle_id:
                raise ValueError("score/HMM bundle path/id drift")
        if set(self.terminal_weights) != set(self.representative_model_asset_sha256):
            raise ValueError("score/HMM parent component identity drift")
        if any(value <= 0.0 for value in self.terminal_weights.values()) or abs(sum(self.terminal_weights.values()) - 1.0) > 1e-9:
            raise ValueError("score/HMM terminal weights are invalid")
        expected_dataset = canonical_json_sha256(
            {
                "n1_bundle_id": self.n1_bundle_id,
                "policy_bundle_id": self.policy_bundle_id,
                "pit_spans_sha256": self.pit_spans_sha256,
                "market_warmup_pit_spans_sha256": self.market_warmup_pit_spans_sha256,
                "market_calendar_sha256": self.market_calendar_sha256,
                "suspend_sidecar_sha256": self.suspend_sidecar_sha256,
                "factor_schema_identity": self.factor_schema_identity,
                "arm_schema_hashes": SCORE_HMM_ARM_SCHEMA_HASHES,
                "policy_identity": self.policy_identity,
                "evidence_refs": [item.model_dump(mode="json") for item in self.evidence_refs],
            }
        )
        if self.dataset_identity != expected_dataset:
            raise ValueError("score/HMM composite dataset identity drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advscorehmm_{digest[:24]}":
            raise ValueError("score/HMM request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at", "output_root"},
        )


class ScoreHMMAdmissionFrontierReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_score_hmm_admission_frontier_receipt_v1"] = (
        "advisory_score_hmm_admission_frontier_receipt_v1"
    )
    receipt_id: str = Field(pattern=r"^advscorehmmrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    planned_trial_count: Literal[5] = 5
    generated_trial_count: Literal[3] = 3
    evaluated_trial_count: int = Field(ge=1, le=3)
    selected_trial_count: int = Field(ge=0, le=1)
    selected_arm_id: Literal[
        "PACKAGE_SCORE_CALIBRATION_ONLY",
        "SCORE_PLUS_RAW_MARKET_SHAPE",
        "SCORE_PLUS_MARKET_HMM",
    ] | None
    eligible_arm_ids: tuple[str, ...]
    arm_statuses: dict[str, str]
    source_unavailable_arm_ids: tuple[str, str] = SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS
    evidence_class: Literal[
        "AUX_CANDIDATE_SELECTED_NAVIGATION_ONLY",
        "AUX_EXECUTED_FRONTIER_SELECTED_ZERO",
        "AUX_EXECUTED_FRONTIER_INSUFFICIENT_SUPPORT",
        "AUX_PARTIAL_SOURCE_UNAVAILABLE",
    ]
    result_class: ResearchResultClass = ResearchResultClass.EXPLORATORY
    objective_contract: ObjectiveContract = ObjectiveContract.RISK_MANAGED_ADVISORY
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    next_task: Literal[
        "N3_AUX_SCORE_HMM_ADMISSION_CONFIRMATION_DESIGN",
        "N3_AUX_SCORE_HMM_EXECUTED_FRONTIER_CLOSED",
        "N3_AUX_SCORE_HMM_SOURCE_READINESS_REVIEW",
    ]
    result_files_sha256: str = Field(pattern=SHA256_PATTERN)
    resource_report_sha256: str = Field(pattern=SHA256_PATTERN)
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False
    runtime_eligible: Literal[False] = False
    runtime_activation_written: Literal[False] = False
    selection_rank_changed: Literal[False] = False
    position_weight_output: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "ScoreHMMAdmissionFrontierReceiptV1":
        selected = self.selected_arm_id is not None
        executable_statuses = {"EVALUATED", "SOURCE_UNAVAILABLE_NO_POLICY_EVALUATION"}
        evaluated_count = sum(
            self.arm_statuses.get(arm_id) == "EVALUATED" for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS
        )
        partial = evaluated_count < len(SCORE_HMM_EXECUTABLE_ARM_IDS)
        status_contract_valid = (
            set(self.arm_statuses) == set(SCORE_HMM_ARM_IDS)
            and all(self.arm_statuses[arm_id] in executable_statuses for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS)
            and all(
                self.arm_statuses[arm_id] == "NOT_RUN_SOURCE_UNAVAILABLE"
                for arm_id in SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS
            )
        )
        eligible_contract_valid = (
            len(set(self.eligible_arm_ids)) == len(self.eligible_arm_ids)
            and set(self.eligible_arm_ids).issubset(SCORE_HMM_EXECUTABLE_ARM_IDS)
            and all(self.arm_statuses.get(arm_id) == "EVALUATED" for arm_id in self.eligible_arm_ids)
        )
        expected_next = (
            "N3_AUX_SCORE_HMM_ADMISSION_CONFIRMATION_DESIGN"
            if selected
            else "N3_AUX_SCORE_HMM_SOURCE_READINESS_REVIEW"
            if partial
            else "N3_AUX_SCORE_HMM_EXECUTED_FRONTIER_CLOSED"
        )
        evidence_relation_valid = (
            self.evidence_class == "AUX_CANDIDATE_SELECTED_NAVIGATION_ONLY"
            if selected
            else self.evidence_class == "AUX_PARTIAL_SOURCE_UNAVAILABLE"
            if partial
            else self.evidence_class
            in {
                "AUX_EXECUTED_FRONTIER_SELECTED_ZERO",
                "AUX_EXECUTED_FRONTIER_INSUFFICIENT_SUPPORT",
            }
        )
        if (
            self.result_class != ResearchResultClass.EXPLORATORY
            or self.objective_contract != ObjectiveContract.RISK_MANAGED_ADVISORY
            or self.decision_use != DecisionUse.NAVIGATION_ONLY
            or self.selected_trial_count != int(selected)
            or self.evaluated_trial_count != evaluated_count
            or self.next_task != expected_next
            or not evidence_relation_valid
            or not status_contract_valid
            or not eligible_contract_valid
            or self.source_unavailable_arm_ids != SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS
            or (selected and partial)
            or (selected and self.selected_arm_id not in self.eligible_arm_ids)
            or (not selected and self.eligible_arm_ids)
        ):
            raise ValueError("score/HMM frontier selection relation drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advscorehmmrcpt_{digest[:24]}":
            raise ValueError("score/HMM frontier receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def build_score_hmm_request(**values: Any) -> FrozenAdvisoryScoreHMMAdmissionRequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    reserved = tuple(values["reserved_candidate_indices"])
    payload = {
        "schema_version": "frozen_advisory_score_hmm_admission_request_v1",
        "created_at": created_at,
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "experiment_id": SCORE_HMM_EXPERIMENT_ID,
        "research_stage": SCORE_HMM_RESEARCH_STAGE,
        "planned_trial_count": 5,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "executable_trial_count": 3,
        "arm_specs": build_default_score_hmm_arms(reserved),
        "primary_target_id": SCORE_HMM_PRIMARY_TARGET_ID,
        "secondary_horizons": SCORE_HMM_SECONDARY_HORIZONS,
        "score_distribution_depth": 50,
        "model_candidate_depth": 20,
        "admission_action_depth": 5,
        "ridge_alpha": 100.0,
        "ridge_solver": "lsqr",
        "logistic_c": 1.0,
        "logistic_solver": "lbfgs",
        "logistic_max_iter": 1000,
        "model_random_state": 20260905,
        "conformal_lower_quantile": 0.20,
        "conformal_upper_quantile": 0.80,
        "hmm_components": 2,
        "hmm_covariance_type": "full",
        "hmm_n_iter": 200,
        "hmm_tol": 1e-4,
        "hmm_random_state": 42,
        "hmm_min_covar": 1e-5,
        "hmm_warmup_trading_days": 60,
        "expected_ready_path_count": 28,
        "expected_oof_predictions_per_row": 7,
        "minimum_paired_days": 300,
        "minimum_intervention_days": 60,
        "minimum_intervention_fraction": 0.25,
        "minimum_take_days": 60,
        "minimum_skip_days": 60,
        "minimum_intervention_days_per_regime": 20,
        "minimum_economic_lift_bps": 5.0,
        "familywise_alpha": 0.005,
        "block_length_trading_days": 20,
        "bootstrap_repetitions": 2_000,
        "bootstrap_seed": 20260905,
        "resource_max_rss_bytes": SCORE_HMM_MAX_RSS_BYTES,
        "resource_max_temp_bytes": SCORE_HMM_MAX_TEMP_BYTES,
        "resource_max_wall_seconds": None,
        "database_read_allowed": False,
        "database_write_allowed": False,
        "network_read_allowed": False,
        "tushare_read_allowed": False,
        "qlib_read_allowed": True,
        "sealed_holdout_accessed": False,
        "factor_catalog_write_allowed": False,
        "strategy_package_write_allowed": False,
        "runtime_activation_allowed": False,
        "selection_rank_change_allowed": False,
        "position_weight_output_allowed": False,
        "order_write_allowed": False,
        "deployable": False,
        **values,
    }
    draft = FrozenAdvisoryScoreHMMAdmissionRequestV1.model_construct(
        request_id="advscorehmm_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenAdvisoryScoreHMMAdmissionRequestV1(
        request_id=f"advscorehmm_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_score_hmm_frontier_receipt(**values: Any) -> ScoreHMMAdmissionFrontierReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    arm_statuses = values.get("arm_statuses")
    if isinstance(arm_statuses, dict):
        values.setdefault(
            "evaluated_trial_count",
            sum(arm_statuses.get(arm_id) == "EVALUATED" for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS),
        )
    payload = {
        "schema_version": "advisory_score_hmm_admission_frontier_receipt_v1",
        "status": "COMPLETE",
        "planned_trial_count": 5,
        "generated_trial_count": 3,
        "result_class": ResearchResultClass.EXPLORATORY,
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "source_unavailable_arm_ids": SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "runtime_activation_written": False,
        "selection_rank_changed": False,
        "position_weight_output": False,
        "created_at": created_at,
        **values,
    }
    draft = ScoreHMMAdmissionFrontierReceiptV1.model_construct(
        receipt_id="advscorehmmrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return ScoreHMMAdmissionFrontierReceiptV1(
        receipt_id=f"advscorehmmrcpt_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )


__all__ = [
    "AdvisoryAdmissionDecisionV1",
    "COMBINED_INTERACTION_COLUMNS",
    "FrozenAdvisoryScoreHMMAdmissionRequestV1",
    "MARKET_HMM_FEATURE_COLUMNS",
    "PACKAGE_SCORE_CALIBRATION_ONLY",
    "RAW_MARKET_FEATURE_COLUMNS",
    "SCORE_FEATURE_COLUMNS",
    "SCORE_HMM_ARM_IDS",
    "SCORE_HMM_ARM_SCHEMA_HASHES",
    "SCORE_HMM_DECISION_END",
    "SCORE_HMM_DECISION_START",
    "SCORE_HMM_EVIDENCE_ROLES",
    "SCORE_HMM_EXECUTABLE_ARM_IDS",
    "SCORE_HMM_EXPERIMENT_ID",
    "SCORE_HMM_FEATURES_BY_ARM",
    "SCORE_HMM_HYPOTHESIS_FAMILY_ID",
    "SCORE_HMM_MARKET_HISTORY_START",
    "SCORE_HMM_PRIMARY_TARGET_ID",
    "SCORE_HMM_SECONDARY_HORIZONS",
    "SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS",
    "SCORE_PLUS_MARKET_AND_SECTOR_HMM",
    "SCORE_PLUS_MARKET_HMM",
    "SCORE_PLUS_RAW_MARKET_SHAPE",
    "SCORE_PLUS_SECTOR_HMM",
    "ScoreHMMAdmissionArmSpecV1",
    "ScoreHMMAdmissionFrontierReceiptV1",
    "build_default_score_hmm_arms",
    "build_score_hmm_frontier_receipt",
    "build_score_hmm_request",
]
