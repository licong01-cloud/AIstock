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
MINUTE_MVE_EXPERIMENT_ID = "ADVISORY-N3-MINUTE-INFORMATION-LEARNABILITY-V1"
MINUTE_MVE_HYPOTHESIS_FAMILY_ID = "ADVISORY-N3-MINUTE-INFORMATION-LEARNABILITY-V1"
MINUTE_MVE_SIGNAL_START = date(2024, 7, 4)
MINUTE_MVE_SIGNAL_END = date(2026, 2, 2)
MINUTE_MVE_SOURCE_ROW_COUNT = 1_710_301
MINUTE_MVE_KNOWN_ROW_COUNT = 1_709_387
MINUTE_MVE_EVALUABLE_ROW_COUNT = 1_705_332
MINUTE_MVE_NONFINITE_KNOWN_ROW_COUNT = 4_055
MINUTE_MVE_UNKNOWN_ROW_COUNT = 914
MINUTE_MVE_DECISION_DATE_COUNT = 386
MINUTE_MVE_MODEL_TRIAL_COUNT = 2
MINUTE_MVE_FAMILYWISE_HYPOTHESIS_COUNT = 4
MINUTE_MVE_EXPECTED_PATH_COUNT = 28
MINUTE_MVE_EXPECTED_OOF_PER_ROW = 7
MINUTE_MVE_BLOCK_LENGTH = 20
MINUTE_MVE_BOOTSTRAP_REPETITIONS = 2_000
MINUTE_MVE_BOOTSTRAP_SEED = 20260903
MINUTE_MVE_MAX_RSS_BYTES = 8 * 1024**3
MINUTE_MVE_MAX_TEMP_BYTES = 16 * 1024**3
MINUTE_MVE_MINIMUM_FEATURE_COVERAGE = 0.80
MINUTE_MVE_PROVIDER_URI = "/home/lc999/data/qlib_minute_bin"
MINUTE_MVE_SNAPSHOT_ID = "qlib_minute_authoritative_full_candidate_20240102_20260630"
MINUTE_MVE_QLIB_VERSION = "0.9.6.99"
MINUTE_MVE_SESSION_WIDE_SINGLE_BAR_DEFICIT_DATES = (
    "2025-11-27",
    "2025-12-08",
    "2025-12-12",
)

MINUTE_MVE_SOURCE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "limit_up",
    "limit_down",
)
MINUTE_MVE_RAW_ECONOMIC_FEATURES = (
    "opening_30m_return_bps",
    "closing_30m_return_bps",
    "realized_volatility_bps",
    "directional_efficiency",
    "close_to_vwap_bps",
    "opening_30m_amount_share",
    "closing_30m_amount_share",
    "limit_pressure",
)
MINUTE_MVE_RANKED_ECONOMIC_FEATURES = tuple(f"{value}_rank_pct" for value in MINUTE_MVE_RAW_ECONOMIC_FEATURES)
MINUTE_MVE_COMPARATOR_FEATURES = ("parent_rank_pct",)
MINUTE_MVE_EXPANDED_FEATURES = (
    "parent_rank_pct",
    "minute_available",
    "minute_coverage_fraction",
    *MINUTE_MVE_RANKED_ECONOMIC_FEATURES,
)
MINUTE_MVE_FEATURE_SCHEMA_VERSION = "advisory_n3_minute_information_set_feature_schema_v1"
MINUTE_MVE_FEATURE_SCHEMA_HASH = canonical_json_sha256(
    {
        "schema_version": MINUTE_MVE_FEATURE_SCHEMA_VERSION,
        "decision_clock": "T_DAY_ONLY_THROUGH_15_00_AFTER_CLOSE_RANKING",
        "source_fields": list(MINUTE_MVE_SOURCE_FIELDS),
        "raw_economic_features": list(MINUTE_MVE_RAW_ECONOMIC_FEATURES),
        "comparator_features": list(MINUTE_MVE_COMPARATOR_FEATURES),
        "expanded_features": list(MINUTE_MVE_EXPANDED_FEATURES),
        "minimum_feature_coverage": MINUTE_MVE_MINIMUM_FEATURE_COVERAGE,
        "market_wide_empty_slot_policy": "NO_VALID_OHLC_BAR_EXCLUDE_FROM_EFFECTIVE_DENOMINATOR_AND_RECORD",
        "session_wide_single_bar_deficit_policy": "KEEP_RAW_240_OF_241_COVERAGE_AND_REPORT_NORMALIZED_SESSION_CLASS",
        "normal_missing_policy": "KEEP_ALL_KEYS_TRAIN_FOLD_MEDIAN_WITH_AVAILABILITY_COVERAGE",
        "rank_semantics": "SAME_DATE_FINITE_AVERAGE_PCT_ASCENDING",
    }
)


class MinuteInformationSetModelTrialV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_minute_information_set_model_trial_v1"] = (
        "advisory_n3_minute_information_set_model_trial_v1"
    )
    trial_id: Literal[
        "N3_MINUTE_PARENT_RIDGE_COMPARATOR_V1",
        "N3_MINUTE_INFORMATION_EXPANDED_V1",
    ]
    role: Literal["COMPARATOR", "CANDIDATE"]
    feature_columns: tuple[str, ...]
    estimator_family: Literal["SKLEARN_RIDGE_V1"] = "SKLEARN_RIDGE_V1"
    alpha: float = Field(default=100.0, ge=0.0)
    solver: Literal["lsqr"] = "lsqr"
    fit_intercept: Literal[True] = True
    numeric_imputer: Literal["TRAIN_FOLD_MEDIAN"] = "TRAIN_FOLD_MEDIAN"
    numeric_scaler: Literal["TRAIN_FOLD_STANDARD_SCALER"] = "TRAIN_FOLD_STANDARD_SCALER"
    direction_frozen: Literal[True] = True

    @model_validator(mode="after")
    def validate_trial(self) -> "MinuteInformationSetModelTrialV1":
        expected = {
            "N3_MINUTE_PARENT_RIDGE_COMPARATOR_V1": ("COMPARATOR", MINUTE_MVE_COMPARATOR_FEATURES),
            "N3_MINUTE_INFORMATION_EXPANDED_V1": ("CANDIDATE", MINUTE_MVE_EXPANDED_FEATURES),
        }[self.trial_id]
        if self.role != expected[0] or self.feature_columns != expected[1] or self.alpha != 100.0:
            raise ValueError("minute information-set model trial identity drift")
        return self


def build_default_minute_model_trials() -> tuple[MinuteInformationSetModelTrialV1, ...]:
    return (
        MinuteInformationSetModelTrialV1(
            trial_id="N3_MINUTE_PARENT_RIDGE_COMPARATOR_V1",
            role="COMPARATOR",
            feature_columns=MINUTE_MVE_COMPARATOR_FEATURES,
        ),
        MinuteInformationSetModelTrialV1(
            trial_id="N3_MINUTE_INFORMATION_EXPANDED_V1",
            role="CANDIDATE",
            feature_columns=MINUTE_MVE_EXPANDED_FEATURES,
        ),
    )


class FrozenMinuteInformationSetRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_n3_minute_information_set_request_v1"] = (
        "frozen_advisory_n3_minute_information_set_request_v1"
    )
    request_id: str = Field(pattern=r"^advn3minreq_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: ObjectiveContract = ObjectiveContract.ALPHA_RANKING
    study_type: ResearchStudyType = ResearchStudyType.LEARNABILITY_AUDIT
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    planned_trial_count: Literal[2] = 2
    generated_trial_count: Literal[0] = 0
    evaluated_trial_count: Literal[0] = 0
    selected_trial_count: Literal[0] = 0
    model_trials: tuple[MinuteInformationSetModelTrialV1, ...]
    feature_schema_version: Literal["advisory_n3_minute_information_set_feature_schema_v1"] = (
        "advisory_n3_minute_information_set_feature_schema_v1"
    )
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    evidence_refs: tuple[EvidenceReferenceV1, ...]
    leg_bundle_path: str = Field(min_length=1)
    leg_bundle_id: str = Field(pattern=SHA256_PATTERN)
    leg_request_sha256: str = Field(pattern=SHA256_PATTERN)
    leg_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    n2a_bundle_path: str = Field(min_length=1)
    n2a_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n2a_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n2a_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_bundle_path: str = Field(min_length=1)
    n1_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n1_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_split_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    source_spike_receipt_path: str = Field(min_length=1)
    source_spike_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    source_dataset_identity: str = Field(pattern=SHA256_PATTERN)
    route_dataset_identity: str = Field(pattern=SHA256_PATTERN)
    minute_source_content_sha256: str = Field(pattern=SHA256_PATTERN)
    minute_source_file_count: int = Field(gt=0)
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    minute_provider_uri: Literal["/home/lc999/data/qlib_minute_bin"] = MINUTE_MVE_PROVIDER_URI
    minute_snapshot_id: Literal["qlib_minute_authoritative_full_candidate_20240102_20260630"] = MINUTE_MVE_SNAPSHOT_ID
    qlib_version: Literal["0.9.6.99"] = MINUTE_MVE_QLIB_VERSION
    minute_source_fields: tuple[str, ...] = MINUTE_MVE_SOURCE_FIELDS
    expected_session_wide_single_bar_deficit_dates: tuple[str, ...] = MINUTE_MVE_SESSION_WIDE_SINGLE_BAR_DEFICIT_DATES
    signal_start: date = MINUTE_MVE_SIGNAL_START
    signal_end: date = MINUTE_MVE_SIGNAL_END
    expected_source_row_count: int = Field(default=MINUTE_MVE_SOURCE_ROW_COUNT, gt=0)
    expected_known_row_count: int = Field(default=MINUTE_MVE_KNOWN_ROW_COUNT, gt=0)
    expected_evaluable_row_count: int = Field(default=MINUTE_MVE_EVALUABLE_ROW_COUNT, gt=0)
    expected_nonfinite_known_row_count: int = Field(default=MINUTE_MVE_NONFINITE_KNOWN_ROW_COUNT, ge=0)
    expected_unknown_row_count: int = Field(default=MINUTE_MVE_UNKNOWN_ROW_COUNT, ge=0)
    expected_decision_date_count: int = Field(default=MINUTE_MVE_DECISION_DATE_COUNT, gt=0)
    minimum_feature_coverage: float = Field(default=MINUTE_MVE_MINIMUM_FEATURE_COVERAGE, gt=0.0, le=1.0)
    minimum_evaluable_days: int = Field(default=382, gt=0)
    minimum_intervention_days: int = Field(default=60, ge=0)
    minimum_intervention_fraction: float = Field(default=0.25, ge=0.0, le=1.0)
    minimum_intervention_days_per_regime: int = Field(default=20, ge=0)
    minimum_parent_lift_bps: float = 5.0
    expected_ready_path_count: int = Field(default=MINUTE_MVE_EXPECTED_PATH_COUNT, gt=0)
    expected_oof_predictions_per_row: int = Field(default=MINUTE_MVE_EXPECTED_OOF_PER_ROW, gt=0)
    familywise_hypothesis_count: int = Field(default=MINUTE_MVE_FAMILYWISE_HYPOTHESIS_COUNT, gt=0)
    block_length_trading_days: int = Field(default=MINUTE_MVE_BLOCK_LENGTH, gt=0)
    bootstrap_repetitions: int = Field(default=MINUTE_MVE_BOOTSTRAP_REPETITIONS, gt=0)
    bootstrap_seed: int = MINUTE_MVE_BOOTSTRAP_SEED
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: int = Field(default=MINUTE_MVE_MAX_RSS_BYTES, gt=0)
    resource_max_temp_bytes: int = Field(default=MINUTE_MVE_MAX_TEMP_BYTES, gt=0)
    resource_max_wall_seconds: Literal[None] = None
    database_read_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    qlib_read_allowed: Literal[True] = True
    qlib_daily_read_allowed: Literal[False] = False
    minute_data_read_allowed: Literal[True] = True
    sealed_holdout_accessed: Literal[False] = False
    factor_catalog_write_allowed: Literal[False] = False
    strategy_package_write_allowed: Literal[False] = False
    runtime_activation_allowed: Literal[False] = False
    position_weight_output_allowed: Literal[False] = False
    final_model_output_allowed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenMinuteInformationSetRequestV1":
        if (
            self.objective_contract != ObjectiveContract.ALPHA_RANKING
            or self.study_type != ResearchStudyType.LEARNABILITY_AUDIT
            or self.decision_use != DecisionUse.NAVIGATION_ONLY
            or self.model_trials != build_default_minute_model_trials()
            or self.feature_schema_hash != MINUTE_MVE_FEATURE_SCHEMA_HASH
            or self.minute_source_fields != MINUTE_MVE_SOURCE_FIELDS
            or self.expected_session_wide_single_bar_deficit_dates != MINUTE_MVE_SESSION_WIDE_SINGLE_BAR_DEFICIT_DATES
            or self.signal_start != MINUTE_MVE_SIGNAL_START
            or self.signal_end != MINUTE_MVE_SIGNAL_END
            or self.resource_max_rss_bytes != MINUTE_MVE_MAX_RSS_BYTES
            or self.resource_max_temp_bytes != MINUTE_MVE_MAX_TEMP_BYTES
        ):
            raise ValueError("minute information-set research contract drift")
        frozen_numeric = {
            "expected_source_row_count": MINUTE_MVE_SOURCE_ROW_COUNT,
            "expected_known_row_count": MINUTE_MVE_KNOWN_ROW_COUNT,
            "expected_evaluable_row_count": MINUTE_MVE_EVALUABLE_ROW_COUNT,
            "expected_nonfinite_known_row_count": MINUTE_MVE_NONFINITE_KNOWN_ROW_COUNT,
            "expected_unknown_row_count": MINUTE_MVE_UNKNOWN_ROW_COUNT,
            "expected_decision_date_count": MINUTE_MVE_DECISION_DATE_COUNT,
            "minimum_feature_coverage": MINUTE_MVE_MINIMUM_FEATURE_COVERAGE,
            "minimum_evaluable_days": 382,
            "minimum_intervention_days": 60,
            "minimum_intervention_fraction": 0.25,
            "minimum_intervention_days_per_regime": 20,
            "minimum_parent_lift_bps": 5.0,
            "expected_ready_path_count": MINUTE_MVE_EXPECTED_PATH_COUNT,
            "expected_oof_predictions_per_row": MINUTE_MVE_EXPECTED_OOF_PER_ROW,
            "familywise_hypothesis_count": MINUTE_MVE_FAMILYWISE_HYPOTHESIS_COUNT,
            "block_length_trading_days": MINUTE_MVE_BLOCK_LENGTH,
            "bootstrap_repetitions": MINUTE_MVE_BOOTSTRAP_REPETITIONS,
            "bootstrap_seed": MINUTE_MVE_BOOTSTRAP_SEED,
        }
        if any(getattr(self, name) != value for name, value in frozen_numeric.items()):
            raise ValueError("minute information-set frozen threshold/count drift")
        required_roles = {
            "n3_minute_leg_manifest",
            "n3_minute_leg_receipt",
            "n3_minute_n2a_manifest",
            "n3_minute_n2a_request",
            "n3_minute_n2a_full_universe",
            "n3_minute_n1_manifest",
            "n3_minute_n1_cpcv",
            "n3_minute_n1_regime_daily",
            "n3_minute_source_spike_receipt",
            "n3_minute_source_meta",
            "n3_minute_source_calendar",
            "n3_minute_source_instruments",
        }
        roles = [item.role for item in self.evidence_refs]
        if len(roles) != len(set(roles)) or set(roles) != required_roles:
            raise ValueError("minute information-set evidence role roster drift")
        for bundle_path, bundle_id in (
            (self.leg_bundle_path, self.leg_bundle_id),
            (self.n2a_bundle_path, self.n2a_bundle_id),
            (self.n1_bundle_path, self.n1_bundle_id),
        ):
            if bundle_path.replace("\\", "/").rstrip("/").split("/")[-1] != bundle_id:
                raise ValueError("minute information-set bundle path/id drift")
        expected_dataset = canonical_json_sha256(
            {
                "source_dataset_identity": self.source_dataset_identity,
                "route_dataset_identity": self.route_dataset_identity,
                "n1_split_policy_sha256": self.n1_split_policy_sha256,
                "minute_source_content_sha256": self.minute_source_content_sha256,
                "evidence_refs": [item.model_dump(mode="json") for item in self.evidence_refs],
            }
        )
        if self.dataset_identity != expected_dataset:
            raise ValueError("minute information-set composite dataset identity drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advn3minreq_{digest[:24]}":
            raise ValueError("minute information-set request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


class MinuteInformationSetReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_minute_information_set_receipt_v1"] = (
        "advisory_n3_minute_information_set_receipt_v1"
    )
    receipt_id: str = Field(pattern=r"^advn3minrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    planned_trial_count: Literal[2] = 2
    generated_trial_count: Literal[2] = 2
    evaluated_trial_count: Literal[2] = 2
    selected_trial_count: int = Field(ge=0, le=1)
    selected_trial_id: Literal["N3_MINUTE_INFORMATION_EXPANDED_V1"] | None
    eligible_trial_ids: tuple[str, ...]
    result_class: ResearchResultClass = ResearchResultClass.EXPLORATORY
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    next_task: Literal[
        "N3_MINUTE_INFORMATION_SET_CONFIRMATION_DESIGN",
        "N3_QE_ALPHA_GENERATOR_MVE_DESIGN",
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
    def validate_receipt(self) -> "MinuteInformationSetReceiptV1":
        if self.result_class != ResearchResultClass.EXPLORATORY or self.decision_use != DecisionUse.NAVIGATION_ONLY:
            raise ValueError("minute information-set receipt research contract drift")
        selected_count = 1 if self.selected_trial_id else 0
        expected_next = (
            "N3_MINUTE_INFORMATION_SET_CONFIRMATION_DESIGN" if selected_count else "N3_QE_ALPHA_GENERATOR_MVE_DESIGN"
        )
        if self.selected_trial_count != selected_count or self.next_task != expected_next:
            raise ValueError("minute information-set selection/next-task relation drift")
        expected_eligible = ("N3_MINUTE_INFORMATION_EXPANDED_V1",) if selected_count else ()
        if self.eligible_trial_ids != expected_eligible:
            raise ValueError("minute information-set eligible trial roster drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advn3minrcpt_{digest[:24]}":
            raise ValueError("minute information-set receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def build_minute_information_set_request(**values: Any) -> FrozenMinuteInformationSetRequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "frozen_advisory_n3_minute_information_set_request_v1",
        "created_at": created_at,
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "planned_trial_count": 2,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "model_trials": build_default_minute_model_trials(),
        "feature_schema_version": MINUTE_MVE_FEATURE_SCHEMA_VERSION,
        "feature_schema_hash": MINUTE_MVE_FEATURE_SCHEMA_HASH,
        "resource_max_rss_bytes": MINUTE_MVE_MAX_RSS_BYTES,
        "resource_max_temp_bytes": MINUTE_MVE_MAX_TEMP_BYTES,
        "resource_max_wall_seconds": None,
        "database_read_allowed": False,
        "network_read_allowed": False,
        "qlib_read_allowed": True,
        "qlib_daily_read_allowed": False,
        "minute_data_read_allowed": True,
        "sealed_holdout_accessed": False,
        "factor_catalog_write_allowed": False,
        "strategy_package_write_allowed": False,
        "runtime_activation_allowed": False,
        "position_weight_output_allowed": False,
        "final_model_output_allowed": False,
        "deployable": False,
        **values,
    }
    draft = FrozenMinuteInformationSetRequestV1.model_construct(
        request_id="advn3minreq_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenMinuteInformationSetRequestV1(
        request_id=f"advn3minreq_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_minute_information_set_receipt(**values: Any) -> MinuteInformationSetReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_n3_minute_information_set_receipt_v1",
        "status": "COMPLETE",
        "planned_trial_count": 2,
        "generated_trial_count": 2,
        "evaluated_trial_count": 2,
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
    draft = MinuteInformationSetReceiptV1.model_construct(
        receipt_id="advn3minrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return MinuteInformationSetReceiptV1(
        receipt_id=f"advn3minrcpt_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )


__all__ = [
    "FrozenMinuteInformationSetRequestV1",
    "MINUTE_MVE_COMPARATOR_FEATURES",
    "MINUTE_MVE_EVALUABLE_ROW_COUNT",
    "MINUTE_MVE_EXPANDED_FEATURES",
    "MINUTE_MVE_EXPERIMENT_ID",
    "MINUTE_MVE_FEATURE_SCHEMA_HASH",
    "MINUTE_MVE_FEATURE_SCHEMA_VERSION",
    "MINUTE_MVE_HYPOTHESIS_FAMILY_ID",
    "MINUTE_MVE_KNOWN_ROW_COUNT",
    "MINUTE_MVE_PROVIDER_URI",
    "MINUTE_MVE_RAW_ECONOMIC_FEATURES",
    "MINUTE_MVE_RANKED_ECONOMIC_FEATURES",
    "MINUTE_MVE_SOURCE_FIELDS",
    "MINUTE_MVE_SESSION_WIDE_SINGLE_BAR_DEFICIT_DATES",
    "MINUTE_MVE_SOURCE_ROW_COUNT",
    "MinuteInformationSetModelTrialV1",
    "MinuteInformationSetReceiptV1",
    "build_default_minute_model_trials",
    "build_minute_information_set_receipt",
    "build_minute_information_set_request",
]
