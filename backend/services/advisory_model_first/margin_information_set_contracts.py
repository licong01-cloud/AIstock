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
MARGIN_MVE_EXPERIMENT_ID = "ADVISORY-N3-MARGIN-INFORMATION-SET-MVE-V1"
MARGIN_MVE_HYPOTHESIS_FAMILY_ID = "ADVISORY-N3-UPSTREAM-NEW-SOURCE-V1"
MARGIN_MVE_SIGNAL_START = date(2024, 7, 4)
MARGIN_MVE_SIGNAL_END = date(2026, 2, 2)
MARGIN_MVE_SOURCE_START = date(2024, 7, 3)
MARGIN_MVE_SOURCE_END = date(2026, 1, 30)
MARGIN_MVE_SOURCE_ROW_COUNT = 1_710_301
MARGIN_MVE_KNOWN_ROW_COUNT = 1_709_387
MARGIN_MVE_EVALUABLE_ROW_COUNT = 1_705_332
MARGIN_MVE_NONFINITE_KNOWN_ROW_COUNT = 4_055
MARGIN_MVE_UNKNOWN_ROW_COUNT = 914
MARGIN_MVE_DECISION_DATE_COUNT = 386
MARGIN_MVE_MODEL_TRIAL_COUNT = 3
MARGIN_MVE_CURRENT_FAMILYWISE_HYPOTHESIS_COUNT = 4
MARGIN_MVE_CUMULATIVE_CANDIDATE_INDEX = 80
MARGIN_MVE_CUMULATIVE_PRIMARY_COMPARISON_COUNT = 160
MARGIN_MVE_EVALUATED_MODEL_TRIAL_COUNT_PRIOR = 75
MARGIN_MVE_SELECTABLE_HYPOTHESIS_COUNT_PRIOR = 73
MARGIN_MVE_EXTERNAL_VISIBLE_MARGIN_HYPOTHESIS_COUNT = 6
MARGIN_MVE_TARGET_FREE_PRIOR_PROPOSAL_COUNT = 3
MARGIN_MVE_EXPECTED_PATH_COUNT = 28
MARGIN_MVE_EXPECTED_OOF_PER_ROW = 7
MARGIN_MVE_BLOCK_LENGTH = 20
MARGIN_MVE_BOOTSTRAP_REPETITIONS = 2_000
MARGIN_MVE_BOOTSTRAP_SEED = 20260904
MARGIN_MVE_MAX_RSS_BYTES = 8 * 1024**3
MARGIN_MVE_MAX_TEMP_BYTES = 8 * 1024**3
MARGIN_MVE_SOURCE_CHUNK_ROWS = 250_000
MARGIN_MVE_MIN_SOURCE_FRACTION = 0.65
MARGIN_MVE_MIN_TOP20_SOURCE_FRACTION = 0.50
MARGIN_MVE_MIN_TOP50_SOURCE_FRACTION = 0.50
MARGIN_MVE_MIN_TOP20_SUPPORTED_DAYS = 380
MARGIN_MVE_MIN_TOP20_SUPPORTED_COUNT = 5
MARGIN_MVE_MIN_RAW_FIELD_FINITE_FRACTION = 0.99
MARGIN_MVE_MIN_DYNAMICS_FINITE_FRACTION = 0.70
MARGIN_MVE_MIN_DYNAMICS_FINITE_PER_DAY = 1_000
MARGIN_MVE_CURRENT_MARGIN_SIZE = 269_535_742
MARGIN_MVE_CURRENT_MARGIN_SHA256 = "dece5f75039cfd42b8f9546758cf91179b00cc38e64c4edf9a9a62d0a5a67cd1"
MARGIN_MVE_SECONDARY_MARGIN_SIZE = 155_706_768
MARGIN_MVE_SECONDARY_MARGIN_SHA256 = "da008542c05e778e9eb294e2039413535786e8863111ee980dcf5872544511b3"
MARGIN_MVE_CALENDAR_SIZE = 21_571
MARGIN_MVE_CALENDAR_SHA256 = "ce017cfbf1d9dde630c0d7f39e33b767e95293acd5258104f80491239826207a"
MARGIN_MVE_SOURCE_QUALITY = "REPLAY_PIT_T_MINUS_1_CROSS_SNAPSHOT_STABLE_NOT_VINTAGE"

MARGIN_MVE_SOURCE_FIELDS = (
    "md_rzye",
    "md_rqye",
    "md_rzmre",
    "md_rqyl",
    "md_rzche",
    "md_rqchl",
    "md_rqmcl",
    "md_rzrqye",
)
MARGIN_MVE_RAW_DYNAMICS_FEATURES = (
    "rzye_log_delta_1d",
    "rzye_log_delta_5d",
    "rqye_log_delta_1d",
    "rqye_log_delta_5d",
    "rqyl_log_delta_1d",
    "rqyl_log_delta_5d",
    "rzrqye_log_delta_1d",
    "rzrqye_log_delta_5d",
    "rz_buy_to_prev_balance",
    "rz_repay_to_prev_balance",
    "rq_sell_to_prev_balance",
    "rq_repay_to_prev_balance",
)
MARGIN_MVE_RANKED_DYNAMICS_FEATURES = tuple(f"{value}_rank_pct" for value in MARGIN_MVE_RAW_DYNAMICS_FEATURES)
MARGIN_MVE_PARENT_FEATURES = ("parent_rank_pct",)
MARGIN_MVE_MEMBERSHIP_FEATURES = (
    "parent_rank_pct",
    "margin_row_available",
    "margin_history_coverage_fraction",
)
MARGIN_MVE_EXPANDED_FEATURES = (
    *MARGIN_MVE_MEMBERSHIP_FEATURES,
    *MARGIN_MVE_RANKED_DYNAMICS_FEATURES,
)
MARGIN_MVE_FEATURE_SCHEMA_VERSION = "advisory_n3_margin_information_set_feature_schema_v1"
MARGIN_MVE_FEATURE_SCHEMA_HASH = canonical_json_sha256(
    {
        "schema_version": MARGIN_MVE_FEATURE_SCHEMA_VERSION,
        "decision_clock": "T_CONSUMES_PREVIOUS_TRADING_DAY_D_WITH_EXACT_D_MINUS_1_D_MINUS_5",
        "source_quality": MARGIN_MVE_SOURCE_QUALITY,
        "source_fields": list(MARGIN_MVE_SOURCE_FIELDS),
        "raw_dynamics_features": list(MARGIN_MVE_RAW_DYNAMICS_FEATURES),
        "parent_features": list(MARGIN_MVE_PARENT_FEATURES),
        "membership_features": list(MARGIN_MVE_MEMBERSHIP_FEATURES),
        "expanded_features": list(MARGIN_MVE_EXPANDED_FEATURES),
        "rank_semantics": "SAME_DATE_FINITE_CANONICAL_PARENT_AVERAGE_PCT_ASCENDING",
        "normal_missing_policy": "KEEP_ALL_PARENT_KEYS_TRAIN_FOLD_MEDIAN_NO_ZERO_FILL",
    }
)


class MarginInformationSetModelTrialV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_margin_information_set_model_trial_v1"] = (
        "advisory_n3_margin_information_set_model_trial_v1"
    )
    trial_id: Literal[
        "N3_MARGIN_PARENT_RIDGE_COMPARATOR_V1",
        "N3_MARGIN_MEMBERSHIP_CONTROL_V1",
        "N3_MARGIN_DYNAMICS_EXPANDED_V1",
    ]
    role: Literal["COMPARATOR", "CONTROL", "CANDIDATE"]
    feature_columns: tuple[str, ...]
    estimator_family: Literal["SKLEARN_RIDGE_V1"] = "SKLEARN_RIDGE_V1"
    alpha: Literal[100.0] = 100.0
    solver: Literal["lsqr"] = "lsqr"
    fit_intercept: Literal[True] = True
    numeric_imputer: Literal["TRAIN_FOLD_MEDIAN"] = "TRAIN_FOLD_MEDIAN"
    numeric_scaler: Literal["TRAIN_FOLD_STANDARD_SCALER"] = "TRAIN_FOLD_STANDARD_SCALER"
    direction_frozen: Literal[True] = True
    selectable: bool

    @model_validator(mode="after")
    def validate_trial(self) -> "MarginInformationSetModelTrialV1":
        expected = {
            "N3_MARGIN_PARENT_RIDGE_COMPARATOR_V1": (
                "COMPARATOR",
                MARGIN_MVE_PARENT_FEATURES,
                False,
            ),
            "N3_MARGIN_MEMBERSHIP_CONTROL_V1": (
                "CONTROL",
                MARGIN_MVE_MEMBERSHIP_FEATURES,
                False,
            ),
            "N3_MARGIN_DYNAMICS_EXPANDED_V1": (
                "CANDIDATE",
                MARGIN_MVE_EXPANDED_FEATURES,
                True,
            ),
        }[self.trial_id]
        if (self.role, self.feature_columns, self.selectable) != expected:
            raise ValueError("margin information-set model trial identity drift")
        return self


def build_default_margin_model_trials() -> tuple[MarginInformationSetModelTrialV1, ...]:
    return (
        MarginInformationSetModelTrialV1(
            trial_id="N3_MARGIN_PARENT_RIDGE_COMPARATOR_V1",
            role="COMPARATOR",
            feature_columns=MARGIN_MVE_PARENT_FEATURES,
            selectable=False,
        ),
        MarginInformationSetModelTrialV1(
            trial_id="N3_MARGIN_MEMBERSHIP_CONTROL_V1",
            role="CONTROL",
            feature_columns=MARGIN_MVE_MEMBERSHIP_FEATURES,
            selectable=False,
        ),
        MarginInformationSetModelTrialV1(
            trial_id="N3_MARGIN_DYNAMICS_EXPANDED_V1",
            role="CANDIDATE",
            feature_columns=MARGIN_MVE_EXPANDED_FEATURES,
            selectable=True,
        ),
    )


class FrozenMarginSourceRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_n3_margin_source_request_v1"] = (
        "frozen_advisory_n3_margin_source_request_v1"
    )
    source_request_id: str = Field(pattern=r"^advn3margsrcreq_[0-9a-f]{24}$")
    source_request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    n2b_bundle_path: str = Field(min_length=1)
    n2b_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n2b_outcomes_sha256: str = Field(pattern=SHA256_PATTERN)
    candidate_root: str = Field(min_length=1)
    candidate_state_path: str = Field(min_length=1)
    candidate_state_sha256: str = Field(pattern=SHA256_PATTERN)
    candidate_state_size: int = Field(gt=0)
    candidate_state_updated_at: str = Field(min_length=1)
    current_margin_path: str = Field(min_length=1)
    current_margin_sha256: Literal[MARGIN_MVE_CURRENT_MARGIN_SHA256]
    current_margin_size: Literal[MARGIN_MVE_CURRENT_MARGIN_SIZE]
    secondary_margin_path: str = Field(min_length=1)
    secondary_margin_sha256: Literal[MARGIN_MVE_SECONDARY_MARGIN_SHA256]
    secondary_margin_size: Literal[MARGIN_MVE_SECONDARY_MARGIN_SIZE]
    calendar_path: str = Field(min_length=1)
    calendar_sha256: Literal[MARGIN_MVE_CALENDAR_SHA256]
    calendar_size: Literal[MARGIN_MVE_CALENDAR_SIZE]
    source_fields: tuple[str, ...] = MARGIN_MVE_SOURCE_FIELDS
    signal_start: date = MARGIN_MVE_SIGNAL_START
    signal_end: date = MARGIN_MVE_SIGNAL_END
    source_start: date = MARGIN_MVE_SOURCE_START
    source_end: date = MARGIN_MVE_SOURCE_END
    expected_parent_row_count: Literal[MARGIN_MVE_SOURCE_ROW_COUNT] = MARGIN_MVE_SOURCE_ROW_COUNT
    expected_decision_date_count: Literal[MARGIN_MVE_DECISION_DATE_COUNT] = MARGIN_MVE_DECISION_DATE_COUNT
    chunk_rows: Literal[MARGIN_MVE_SOURCE_CHUNK_ROWS] = MARGIN_MVE_SOURCE_CHUNK_ROWS
    source_quality: Literal[MARGIN_MVE_SOURCE_QUALITY] = MARGIN_MVE_SOURCE_QUALITY
    target_columns_read: Literal[False] = False
    database_read_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenMarginSourceRequestV1":
        if self.source_fields != MARGIN_MVE_SOURCE_FIELDS:
            raise ValueError("margin source field roster drift")
        if self.n2b_bundle_path.replace("\\", "/").rstrip("/").split("/")[-1] != self.n2b_bundle_id:
            raise ValueError("margin source N2-B bundle path/id drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.source_request_sha256 != digest or self.source_request_id != f"advn3margsrcreq_{digest[:24]}":
            raise ValueError("margin source request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"source_request_id", "source_request_sha256", "created_at"},
        )


class MarginSourceIdentityReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_margin_source_identity_receipt_v1"] = (
        "advisory_n3_margin_source_identity_receipt_v1"
    )
    source_receipt_id: str = Field(pattern=r"^advn3margsrcrcpt_[0-9a-f]{24}$")
    source_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    source_request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    projection_sha256: str = Field(pattern=SHA256_PATTERN)
    projection_row_count: int = Field(gt=0)
    common_key_count: int = Field(gt=0)
    current_only_key_count: int = Field(ge=0)
    secondary_only_key_count: int = Field(ge=0)
    value_drift_row_count: Literal[0] = 0
    parent_row_count: Literal[MARGIN_MVE_SOURCE_ROW_COUNT]
    decision_date_count: Literal[MARGIN_MVE_DECISION_DATE_COUNT]
    source_row_fraction: float = Field(ge=0.0, le=1.0)
    top20_source_row_fraction: float = Field(ge=0.0, le=1.0)
    top50_source_row_fraction: float = Field(ge=0.0, le=1.0)
    top20_supported_day_count: int = Field(ge=0)
    raw_field_finite_fraction: dict[str, float]
    candidate_state_before_sha256: str = Field(pattern=SHA256_PATTERN)
    candidate_state_after_sha256: str = Field(pattern=SHA256_PATTERN)
    current_source_row_count_read: int = Field(gt=0)
    secondary_source_row_count_read: int = Field(gt=0)
    source_unique_file_count: Literal[4] = 4
    source_bytes_read: int = Field(gt=0)
    elapsed_seconds: float = Field(ge=0.0)
    peak_rss_bytes: int = Field(gt=0)
    temporary_bytes: int = Field(ge=0)
    source_quality: Literal[MARGIN_MVE_SOURCE_QUALITY] = MARGIN_MVE_SOURCE_QUALITY
    target_columns_read: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    database_reads: Literal[0] = 0
    database_writes: Literal[0] = 0
    network_reads: Literal[0] = 0
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "MarginSourceIdentityReceiptV1":
        if self.candidate_state_before_sha256 != self.candidate_state_after_sha256:
            raise ValueError("candidate state changed during margin source prepare")
        if (
            self.source_row_fraction < MARGIN_MVE_MIN_SOURCE_FRACTION
            or self.top20_source_row_fraction < MARGIN_MVE_MIN_TOP20_SOURCE_FRACTION
            or self.top50_source_row_fraction < MARGIN_MVE_MIN_TOP50_SOURCE_FRACTION
            or self.top20_supported_day_count < MARGIN_MVE_MIN_TOP20_SUPPORTED_DAYS
        ):
            raise ValueError("margin source coverage contract failed")
        if set(self.raw_field_finite_fraction) != set(MARGIN_MVE_SOURCE_FIELDS) or any(
            value < MARGIN_MVE_MIN_RAW_FIELD_FINITE_FRACTION for value in self.raw_field_finite_fraction.values()
        ):
            raise ValueError("margin source raw-field quality contract failed")
        if self.peak_rss_bytes > MARGIN_MVE_MAX_RSS_BYTES or self.temporary_bytes > MARGIN_MVE_MAX_TEMP_BYTES:
            raise ValueError("margin source resource contract failed")
        digest = canonical_json_sha256(self.functional_payload())
        if self.source_receipt_sha256 != digest or self.source_receipt_id != f"advn3margsrcrcpt_{digest[:24]}":
            raise ValueError("margin source receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"source_receipt_id", "source_receipt_sha256", "created_at"},
        )


class FrozenMarginInformationSetRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_n3_margin_information_set_request_v1"] = (
        "frozen_advisory_n3_margin_information_set_request_v1"
    )
    request_id: str = Field(pattern=r"^advn3margreq_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: ObjectiveContract = ObjectiveContract.ALPHA_RANKING
    study_type: ResearchStudyType = ResearchStudyType.LEARNABILITY_AUDIT
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    planned_trial_count: Literal[3] = 3
    generated_trial_count: Literal[0] = 0
    evaluated_trial_count: Literal[0] = 0
    selected_trial_count: Literal[0] = 0
    selectable_trial_count: Literal[1] = 1
    model_trials: tuple[MarginInformationSetModelTrialV1, ...]
    feature_schema_version: Literal[MARGIN_MVE_FEATURE_SCHEMA_VERSION] = MARGIN_MVE_FEATURE_SCHEMA_VERSION
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    evidence_refs: tuple[EvidenceReferenceV1, ...]
    generator_bundle_path: str = Field(min_length=1)
    generator_bundle_id: str = Field(pattern=SHA256_PATTERN)
    generator_request_sha256: str = Field(pattern=SHA256_PATTERN)
    generator_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    n2b_bundle_path: str = Field(min_length=1)
    n2b_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n2b_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n2b_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_bundle_path: str = Field(min_length=1)
    n1_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n1_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_split_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    source_bundle_path: str = Field(min_length=1)
    source_bundle_id: str = Field(pattern=SHA256_PATTERN)
    source_request_sha256: str = Field(pattern=SHA256_PATTERN)
    source_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    source_dataset_identity: str = Field(pattern=SHA256_PATTERN)
    route_dataset_identity: str = Field(pattern=SHA256_PATTERN)
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    signal_start: date = MARGIN_MVE_SIGNAL_START
    signal_end: date = MARGIN_MVE_SIGNAL_END
    expected_source_row_count: Literal[MARGIN_MVE_SOURCE_ROW_COUNT] = MARGIN_MVE_SOURCE_ROW_COUNT
    expected_known_row_count: Literal[MARGIN_MVE_KNOWN_ROW_COUNT] = MARGIN_MVE_KNOWN_ROW_COUNT
    expected_evaluable_row_count: Literal[MARGIN_MVE_EVALUABLE_ROW_COUNT] = MARGIN_MVE_EVALUABLE_ROW_COUNT
    expected_nonfinite_known_row_count: Literal[MARGIN_MVE_NONFINITE_KNOWN_ROW_COUNT] = (
        MARGIN_MVE_NONFINITE_KNOWN_ROW_COUNT
    )
    expected_unknown_row_count: Literal[MARGIN_MVE_UNKNOWN_ROW_COUNT] = MARGIN_MVE_UNKNOWN_ROW_COUNT
    expected_decision_date_count: Literal[MARGIN_MVE_DECISION_DATE_COUNT] = MARGIN_MVE_DECISION_DATE_COUNT
    minimum_evaluable_days: Literal[382] = 382
    minimum_intervention_days: Literal[60] = 60
    minimum_intervention_fraction: Literal[0.25] = 0.25
    minimum_intervention_days_per_regime: Literal[20] = 20
    minimum_parent_lift_bps: Literal[5.0] = 5.0
    expected_ready_path_count: Literal[MARGIN_MVE_EXPECTED_PATH_COUNT] = MARGIN_MVE_EXPECTED_PATH_COUNT
    expected_oof_predictions_per_row: Literal[MARGIN_MVE_EXPECTED_OOF_PER_ROW] = MARGIN_MVE_EXPECTED_OOF_PER_ROW
    current_familywise_hypothesis_count: Literal[MARGIN_MVE_CURRENT_FAMILYWISE_HYPOTHESIS_COUNT] = (
        MARGIN_MVE_CURRENT_FAMILYWISE_HYPOTHESIS_COUNT
    )
    cumulative_candidate_index: Literal[MARGIN_MVE_CUMULATIVE_CANDIDATE_INDEX] = MARGIN_MVE_CUMULATIVE_CANDIDATE_INDEX
    cumulative_primary_comparison_count: Literal[MARGIN_MVE_CUMULATIVE_PRIMARY_COMPARISON_COUNT] = (
        MARGIN_MVE_CUMULATIVE_PRIMARY_COMPARISON_COUNT
    )
    evaluated_model_trial_count_prior: Literal[MARGIN_MVE_EVALUATED_MODEL_TRIAL_COUNT_PRIOR] = (
        MARGIN_MVE_EVALUATED_MODEL_TRIAL_COUNT_PRIOR
    )
    selectable_hypothesis_count_prior: Literal[MARGIN_MVE_SELECTABLE_HYPOTHESIS_COUNT_PRIOR] = (
        MARGIN_MVE_SELECTABLE_HYPOTHESIS_COUNT_PRIOR
    )
    external_visible_margin_hypothesis_count: Literal[MARGIN_MVE_EXTERNAL_VISIBLE_MARGIN_HYPOTHESIS_COUNT] = (
        MARGIN_MVE_EXTERNAL_VISIBLE_MARGIN_HYPOTHESIS_COUNT
    )
    target_free_prior_proposal_count: Literal[MARGIN_MVE_TARGET_FREE_PRIOR_PROPOSAL_COUNT] = (
        MARGIN_MVE_TARGET_FREE_PRIOR_PROPOSAL_COUNT
    )
    block_length_trading_days: Literal[MARGIN_MVE_BLOCK_LENGTH] = MARGIN_MVE_BLOCK_LENGTH
    bootstrap_repetitions: Literal[MARGIN_MVE_BOOTSTRAP_REPETITIONS] = MARGIN_MVE_BOOTSTRAP_REPETITIONS
    bootstrap_seed: Literal[MARGIN_MVE_BOOTSTRAP_SEED] = MARGIN_MVE_BOOTSTRAP_SEED
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: Literal[MARGIN_MVE_MAX_RSS_BYTES] = MARGIN_MVE_MAX_RSS_BYTES
    resource_max_temp_bytes: Literal[MARGIN_MVE_MAX_TEMP_BYTES] = MARGIN_MVE_MAX_TEMP_BYTES
    resource_max_wall_seconds: Literal[None] = None
    database_read_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    qlib_read_allowed: Literal[False] = False
    minute_data_read_allowed: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    factor_catalog_write_allowed: Literal[False] = False
    strategy_package_write_allowed: Literal[False] = False
    runtime_activation_allowed: Literal[False] = False
    position_weight_output_allowed: Literal[False] = False
    final_model_output_allowed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenMarginInformationSetRequestV1":
        if (
            self.objective_contract != ObjectiveContract.ALPHA_RANKING
            or self.study_type != ResearchStudyType.LEARNABILITY_AUDIT
            or self.decision_use != DecisionUse.NAVIGATION_ONLY
            or self.model_trials != build_default_margin_model_trials()
            or self.feature_schema_hash != MARGIN_MVE_FEATURE_SCHEMA_HASH
        ):
            raise ValueError("margin information-set research contract drift")
        roles = [item.role for item in self.evidence_refs]
        required_roles = {
            "n3_margin_generator_manifest",
            "n3_margin_generator_receipt",
            "n3_margin_n2b_manifest",
            "n3_margin_n2b_request",
            "n3_margin_n2b_outcomes",
            "n3_margin_n1_manifest",
            "n3_margin_n1_cpcv",
            "n3_margin_n1_regime_daily",
            "n3_margin_source_manifest",
            "n3_margin_source_receipt",
            "n3_margin_source_projection",
            "n3_margin_source_coverage",
            "n3_margin_cross_snapshot_parity",
            "n3_margin_candidate_state_snapshot",
        }
        if len(roles) != len(set(roles)) or set(roles) != required_roles:
            raise ValueError("margin information-set evidence role roster drift")
        for bundle_path, bundle_id in (
            (self.generator_bundle_path, self.generator_bundle_id),
            (self.n2b_bundle_path, self.n2b_bundle_id),
            (self.n1_bundle_path, self.n1_bundle_id),
            (self.source_bundle_path, self.source_bundle_id),
        ):
            if bundle_path.replace("\\", "/").rstrip("/").split("/")[-1] != bundle_id:
                raise ValueError("margin information-set bundle path/id drift")
        expected_dataset = canonical_json_sha256(
            {
                "source_dataset_identity": self.source_dataset_identity,
                "route_dataset_identity": self.route_dataset_identity,
                "n1_split_policy_sha256": self.n1_split_policy_sha256,
                "source_identity_sha256": self.source_identity_sha256,
                "feature_schema_hash": self.feature_schema_hash,
                "policy_identity": self.policy_identity,
                "evidence_refs": [item.model_dump(mode="json") for item in self.evidence_refs],
            }
        )
        if self.dataset_identity != expected_dataset:
            raise ValueError("margin information-set composite dataset identity drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advn3margreq_{digest[:24]}":
            raise ValueError("margin information-set request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


class MarginInformationSetReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_margin_information_set_receipt_v1"] = (
        "advisory_n3_margin_information_set_receipt_v1"
    )
    receipt_id: str = Field(pattern=r"^advn3margrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    planned_trial_count: Literal[3] = 3
    generated_trial_count: Literal[3] = 3
    evaluated_trial_count: Literal[3] = 3
    selected_trial_count: int = Field(ge=0, le=1)
    selected_trial_id: Literal["N3_MARGIN_DYNAMICS_EXPANDED_V1"] | None
    eligible_trial_ids: tuple[str, ...]
    result_class: ResearchResultClass = ResearchResultClass.EXPLORATORY
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    next_task: Literal[
        "N3_MARGIN_INFORMATION_SET_CONFIRMATION_DESIGN",
        "N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN",
    ]
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    result_files_sha256: str = Field(pattern=SHA256_PATTERN)
    resource_report_sha256: str = Field(pattern=SHA256_PATTERN)
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False
    runtime_eligible: Literal[False] = False
    final_model_written: Literal[False] = False
    factor_catalog_written: Literal[False] = False
    strategy_package_written: Literal[False] = False
    position_weight_output: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "MarginInformationSetReceiptV1":
        if self.result_class != ResearchResultClass.EXPLORATORY or self.decision_use != DecisionUse.NAVIGATION_ONLY:
            raise ValueError("margin information-set receipt research contract drift")
        selected_count = 1 if self.selected_trial_id else 0
        expected_next = (
            "N3_MARGIN_INFORMATION_SET_CONFIRMATION_DESIGN"
            if selected_count
            else "N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN"
        )
        if self.selected_trial_count != selected_count or self.next_task != expected_next:
            raise ValueError("margin information-set selection/next-task relation drift")
        expected_eligible = ("N3_MARGIN_DYNAMICS_EXPANDED_V1",) if selected_count else ()
        if self.eligible_trial_ids != expected_eligible:
            raise ValueError("margin information-set eligible trial roster drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advn3margrcpt_{digest[:24]}":
            raise ValueError("margin information-set receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def build_margin_source_request(**values: Any) -> FrozenMarginSourceRequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "frozen_advisory_n3_margin_source_request_v1",
        "created_at": created_at,
        "source_fields": MARGIN_MVE_SOURCE_FIELDS,
        "signal_start": MARGIN_MVE_SIGNAL_START,
        "signal_end": MARGIN_MVE_SIGNAL_END,
        "source_start": MARGIN_MVE_SOURCE_START,
        "source_end": MARGIN_MVE_SOURCE_END,
        "expected_parent_row_count": MARGIN_MVE_SOURCE_ROW_COUNT,
        "expected_decision_date_count": MARGIN_MVE_DECISION_DATE_COUNT,
        "chunk_rows": MARGIN_MVE_SOURCE_CHUNK_ROWS,
        "source_quality": MARGIN_MVE_SOURCE_QUALITY,
        "target_columns_read": False,
        "database_read_allowed": False,
        "network_read_allowed": False,
        "sealed_holdout_accessed": False,
        **values,
    }
    draft = FrozenMarginSourceRequestV1.model_construct(
        source_request_id="advn3margsrcreq_" + "0" * 24,
        source_request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenMarginSourceRequestV1(
        source_request_id=f"advn3margsrcreq_{digest[:24]}",
        source_request_sha256=digest,
        **payload,
    )


def build_margin_source_receipt(**values: Any) -> MarginSourceIdentityReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_n3_margin_source_identity_receipt_v1",
        "status": "COMPLETE",
        "value_drift_row_count": 0,
        "source_quality": MARGIN_MVE_SOURCE_QUALITY,
        "target_columns_read": False,
        "sealed_holdout_accessed": False,
        "database_reads": 0,
        "database_writes": 0,
        "network_reads": 0,
        "created_at": created_at,
        **values,
    }
    draft = MarginSourceIdentityReceiptV1.model_construct(
        source_receipt_id="advn3margsrcrcpt_" + "0" * 24,
        source_receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return MarginSourceIdentityReceiptV1(
        source_receipt_id=f"advn3margsrcrcpt_{digest[:24]}",
        source_receipt_sha256=digest,
        **payload,
    )


def build_margin_information_set_request(**values: Any) -> FrozenMarginInformationSetRequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "frozen_advisory_n3_margin_information_set_request_v1",
        "created_at": created_at,
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "planned_trial_count": 3,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "selectable_trial_count": 1,
        "model_trials": build_default_margin_model_trials(),
        "feature_schema_version": MARGIN_MVE_FEATURE_SCHEMA_VERSION,
        "feature_schema_hash": MARGIN_MVE_FEATURE_SCHEMA_HASH,
        "resource_max_rss_bytes": MARGIN_MVE_MAX_RSS_BYTES,
        "resource_max_temp_bytes": MARGIN_MVE_MAX_TEMP_BYTES,
        "resource_max_wall_seconds": None,
        "database_read_allowed": False,
        "network_read_allowed": False,
        "qlib_read_allowed": False,
        "minute_data_read_allowed": False,
        "sealed_holdout_accessed": False,
        "factor_catalog_write_allowed": False,
        "strategy_package_write_allowed": False,
        "runtime_activation_allowed": False,
        "position_weight_output_allowed": False,
        "final_model_output_allowed": False,
        "deployable": False,
        **values,
    }
    draft = FrozenMarginInformationSetRequestV1.model_construct(
        request_id="advn3margreq_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenMarginInformationSetRequestV1(
        request_id=f"advn3margreq_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_margin_information_set_receipt(**values: Any) -> MarginInformationSetReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_n3_margin_information_set_receipt_v1",
        "status": "COMPLETE",
        "planned_trial_count": 3,
        "generated_trial_count": 3,
        "evaluated_trial_count": 3,
        "result_class": ResearchResultClass.EXPLORATORY,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "final_model_written": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
        "created_at": created_at,
        **values,
    }
    draft = MarginInformationSetReceiptV1.model_construct(
        receipt_id="advn3margrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return MarginInformationSetReceiptV1(
        receipt_id=f"advn3margrcpt_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )


__all__ = [
    "FrozenMarginInformationSetRequestV1",
    "FrozenMarginSourceRequestV1",
    "MARGIN_MVE_CUMULATIVE_CANDIDATE_INDEX",
    "MARGIN_MVE_CURRENT_FAMILYWISE_HYPOTHESIS_COUNT",
    "MARGIN_MVE_EXPANDED_FEATURES",
    "MARGIN_MVE_EXPERIMENT_ID",
    "MARGIN_MVE_FEATURE_SCHEMA_HASH",
    "MARGIN_MVE_FEATURE_SCHEMA_VERSION",
    "MARGIN_MVE_HYPOTHESIS_FAMILY_ID",
    "MARGIN_MVE_MEMBERSHIP_FEATURES",
    "MARGIN_MVE_PARENT_FEATURES",
    "MARGIN_MVE_RANKED_DYNAMICS_FEATURES",
    "MARGIN_MVE_RAW_DYNAMICS_FEATURES",
    "MARGIN_MVE_SOURCE_FIELDS",
    "MARGIN_MVE_SOURCE_QUALITY",
    "MarginInformationSetModelTrialV1",
    "MarginInformationSetReceiptV1",
    "MarginSourceIdentityReceiptV1",
    "build_default_margin_model_trials",
    "build_margin_information_set_receipt",
    "build_margin_information_set_request",
    "build_margin_source_receipt",
    "build_margin_source_request",
]
