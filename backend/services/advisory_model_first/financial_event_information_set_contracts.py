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
EVENT_MVE_EXPERIMENT_ID = "ADVISORY-N3-FINANCIAL-EVENT-INFORMATION-SET-MVE-V1"
EVENT_MVE_HYPOTHESIS_FAMILY_ID = "ADVISORY-N3-UPSTREAM-NEW-SOURCE-V1"
EVENT_MVE_SIGNAL_START = date(2024, 7, 4)
EVENT_MVE_SIGNAL_END = date(2026, 2, 2)
EVENT_MVE_PARENT_ROW_COUNT = 1_710_301
EVENT_MVE_KNOWN_ROW_COUNT = 1_709_387
EVENT_MVE_EVALUABLE_ROW_COUNT = 1_705_332
EVENT_MVE_NONFINITE_KNOWN_ROW_COUNT = 4_055
EVENT_MVE_UNKNOWN_ROW_COUNT = 914
EVENT_MVE_DECISION_DATE_COUNT = 386
EVENT_MVE_SOURCE_ROW_COUNT = 84_272
EVENT_MVE_SOURCE_PROJECTION_SHA256 = "d9bda2d23335354bb99f04c5a11643ee56347ae2e8ac871f7bae77e39030bded"
EVENT_MVE_SOURCE_QUALITY = "DATE_ONLY_BACKFILLED_NON_VINTAGE"
EVENT_MVE_MODEL_TRIAL_COUNT = 3
EVENT_MVE_SELECTABLE_TRIAL_COUNT = 1
EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX_PRIOR = 80
EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX = 83
EVENT_MVE_CUMULATIVE_PRIMARY_COMPARISON_COUNT = 166
EVENT_MVE_CURRENT_FAMILYWISE_HYPOTHESIS_COUNT = 4
EVENT_MVE_EXPECTED_PATH_COUNT = 28
EVENT_MVE_EXPECTED_OOF_PER_ROW = 7
EVENT_MVE_BLOCK_LENGTH = 20
EVENT_MVE_BOOTSTRAP_REPETITIONS = 2_000
EVENT_MVE_BOOTSTRAP_SEED = 20260905
EVENT_MVE_MAX_RSS_BYTES = 8 * 1024**3
EVENT_MVE_MAX_TEMP_BYTES = 8 * 1024**3

EVENT_DIRECTION_BY_TYPE = {
    "financial_forecast_large_growth": 1,
    "financial_forecast_turnaround": 1,
    "financial_express_large_growth": 1,
    "financial_indicator_large_growth": 1,
    "financial_forecast_loss": -1,
    "financial_forecast_large_decline": -1,
    "financial_express_loss": -1,
    "financial_express_large_decline": -1,
    "financial_indicator_large_decline": -1,
    "financial_forecast_neutral": 0,
    "financial_express_neutral": 0,
    "financial_indicator_neutral": 0,
}
EVENT_SOURCE_TYPES = ("tushare_express", "tushare_fina_indicator", "tushare_forecast")
EVENT_PARENT_FEATURES = ("parent_rank_pct",)
EVENT_DISCLOSURE_FEATURES = (
    "event_disclosure_seen_120",
    "event_qualifying_seen_120",
    "event_source_type_count_120",
    "event_disclosure_count_120_log1p",
    "event_neutral_count_120_log1p",
    "event_latest_disclosure_age_120",
)
EVENT_SIGNED_FEATURES = (
    "event_signed_value_sum_20",
    "event_signed_value_sum_60",
    "event_signed_value_sum_120",
    "event_signed_value_sum_252",
    "event_positive_count_20_log1p",
    "event_negative_count_20_log1p",
    "event_positive_count_120_log1p",
    "event_negative_count_120_log1p",
    "event_latest_qualifying_signed_value_120",
    "event_forecast_signed_value_sum_120",
    "event_express_signed_value_sum_120",
    "event_fina_indicator_signed_value_sum_120",
)
EVENT_DISCLOSURE_SCHEMA_FEATURES = (*EVENT_PARENT_FEATURES, *EVENT_DISCLOSURE_FEATURES)
EVENT_SIGNED_SCHEMA_FEATURES = (*EVENT_DISCLOSURE_SCHEMA_FEATURES, *EVENT_SIGNED_FEATURES)
EVENT_MVE_FEATURE_SCHEMA_VERSION = "advisory_n3_financial_event_information_set_feature_schema_v1"
EVENT_MVE_FEATURE_SCHEMA_HASH = canonical_json_sha256(
    {
        "schema_version": EVENT_MVE_FEATURE_SCHEMA_VERSION,
        "source_quality": EVENT_MVE_SOURCE_QUALITY,
        "direction_by_type": EVENT_DIRECTION_BY_TYPE,
        "source_types": list(EVENT_SOURCE_TYPES),
        "parent_features": list(EVENT_PARENT_FEATURES),
        "disclosure_features": list(EVENT_DISCLOSURE_FEATURES),
        "signed_features": list(EVENT_SIGNED_FEATURES),
        "clock": "EFFECTIVE_TRADE_DATE_LTE_DECISION_T_TRADING_WINDOWS_20_60_120_252",
        "missing": "KEEP_ALL_PARENT_KEYS_ZERO_COUNTS_AND_SUMS_AGE_121_WITH_SEEN_FLAGS",
        "latest_tie_break": "EFFECTIVE_DESC_SOURCE_TYPE_ASC_EVENT_TYPE_ASC_SOURCE_RECORD_KEY_ASC",
    }
)


class FinancialEventModelTrialV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_financial_event_model_trial_v1"] = "advisory_n3_financial_event_model_trial_v1"
    trial_id: Literal[
        "EVENT_PARENT_COMPARATOR_V1",
        "EVENT_DISCLOSURE_CONTROL_V1",
        "EVENT_SIGNED_CONTENT_V1",
    ]
    role: Literal["COMPARATOR", "CONTROL", "CANDIDATE"]
    feature_columns: tuple[str, ...]
    estimator_family: Literal["SKLEARN_RIDGE_V1"] = "SKLEARN_RIDGE_V1"
    alpha: Literal[100.0] = 100.0
    solver: Literal["lsqr"] = "lsqr"
    fit_intercept: Literal[True] = True
    numeric_imputer: Literal["TRAIN_FOLD_MEDIAN"] = "TRAIN_FOLD_MEDIAN"
    numeric_scaler: Literal["TRAIN_FOLD_STANDARD_SCALER"] = "TRAIN_FOLD_STANDARD_SCALER"
    selectable: bool

    @model_validator(mode="after")
    def validate_trial(self) -> "FinancialEventModelTrialV1":
        expected = {
            "EVENT_PARENT_COMPARATOR_V1": ("COMPARATOR", EVENT_PARENT_FEATURES, False),
            "EVENT_DISCLOSURE_CONTROL_V1": ("CONTROL", EVENT_DISCLOSURE_SCHEMA_FEATURES, False),
            "EVENT_SIGNED_CONTENT_V1": ("CANDIDATE", EVENT_SIGNED_SCHEMA_FEATURES, True),
        }[self.trial_id]
        if (self.role, self.feature_columns, self.selectable) != expected:
            raise ValueError("financial-event trial identity drift")
        return self


def build_default_event_model_trials() -> tuple[FinancialEventModelTrialV1, ...]:
    return (
        FinancialEventModelTrialV1(
            trial_id="EVENT_PARENT_COMPARATOR_V1",
            role="COMPARATOR",
            feature_columns=EVENT_PARENT_FEATURES,
            selectable=False,
        ),
        FinancialEventModelTrialV1(
            trial_id="EVENT_DISCLOSURE_CONTROL_V1",
            role="CONTROL",
            feature_columns=EVENT_DISCLOSURE_SCHEMA_FEATURES,
            selectable=False,
        ),
        FinancialEventModelTrialV1(
            trial_id="EVENT_SIGNED_CONTENT_V1",
            role="CANDIDATE",
            feature_columns=EVENT_SIGNED_SCHEMA_FEATURES,
            selectable=True,
        ),
    )


class FrozenFinancialEventInformationSetRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_n3_financial_event_information_set_request_v1"] = (
        "frozen_advisory_n3_financial_event_information_set_request_v1"
    )
    request_id: str = Field(pattern=r"^advn3fevreq_[0-9a-f]{24}$")
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
    model_trials: tuple[FinancialEventModelTrialV1, ...]
    feature_schema_version: Literal[EVENT_MVE_FEATURE_SCHEMA_VERSION] = EVENT_MVE_FEATURE_SCHEMA_VERSION
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    evidence_refs: tuple[EvidenceReferenceV1, ...]
    source_bundle_path: str = Field(min_length=1)
    source_bundle_id: str = Field(pattern=SHA256_PATTERN)
    source_request_sha256: str = Field(pattern=SHA256_PATTERN)
    source_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    source_projection_sha256: Literal[EVENT_MVE_SOURCE_PROJECTION_SHA256]
    n2b_bundle_path: str = Field(min_length=1)
    n2b_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n2b_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n2b_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_bundle_path: str = Field(min_length=1)
    n1_bundle_id: str = Field(pattern=SHA256_PATTERN)
    n1_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_split_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    qlib_daily_root: str = Field(min_length=1)
    n1_market_calendar_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_market_calendar_row_count: Literal[606] = 606
    n1_market_calendar_cutoff: date = date(2026, 6, 30)
    n1_calendar_data_cutoff: date = date(2026, 3, 10)
    trading_calendar: tuple[date, ...] = Field(min_length=606)
    trading_calendar_sha256: str = Field(pattern=SHA256_PATTERN)
    source_dataset_identity: str = Field(pattern=SHA256_PATTERN)
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    signal_start: date = EVENT_MVE_SIGNAL_START
    signal_end: date = EVENT_MVE_SIGNAL_END
    expected_parent_row_count: Literal[EVENT_MVE_PARENT_ROW_COUNT] = EVENT_MVE_PARENT_ROW_COUNT
    expected_source_row_count: Literal[EVENT_MVE_SOURCE_ROW_COUNT] = EVENT_MVE_SOURCE_ROW_COUNT
    expected_known_row_count: Literal[EVENT_MVE_KNOWN_ROW_COUNT] = EVENT_MVE_KNOWN_ROW_COUNT
    expected_evaluable_row_count: Literal[EVENT_MVE_EVALUABLE_ROW_COUNT] = EVENT_MVE_EVALUABLE_ROW_COUNT
    expected_nonfinite_known_row_count: Literal[EVENT_MVE_NONFINITE_KNOWN_ROW_COUNT] = (
        EVENT_MVE_NONFINITE_KNOWN_ROW_COUNT
    )
    expected_unknown_row_count: Literal[EVENT_MVE_UNKNOWN_ROW_COUNT] = EVENT_MVE_UNKNOWN_ROW_COUNT
    expected_decision_date_count: Literal[EVENT_MVE_DECISION_DATE_COUNT] = EVENT_MVE_DECISION_DATE_COUNT
    expected_ready_path_count: Literal[EVENT_MVE_EXPECTED_PATH_COUNT] = EVENT_MVE_EXPECTED_PATH_COUNT
    expected_oof_predictions_per_row: Literal[EVENT_MVE_EXPECTED_OOF_PER_ROW] = EVENT_MVE_EXPECTED_OOF_PER_ROW
    minimum_evaluable_days: Literal[382] = 382
    minimum_intervention_days: Literal[60] = 60
    minimum_intervention_fraction: Literal[0.25] = 0.25
    minimum_intervention_days_per_regime: Literal[20] = 20
    minimum_parent_lift_bps: Literal[5.0] = 5.0
    minimum_top20_disclosure_fraction_120: Literal[0.70] = 0.70
    minimum_top20_qualifying_fraction_120: Literal[0.70] = 0.70
    minimum_top20_supported_days: Literal[380] = 380
    minimum_top20_disclosure_count: Literal[5] = 5
    minimum_top50_mixed_qualifying_days: Literal[300] = 300
    cumulative_candidate_index_prior: Literal[EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX_PRIOR] = (
        EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX_PRIOR
    )
    cumulative_candidate_index: Literal[EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX] = EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX
    cumulative_primary_comparison_count: Literal[EVENT_MVE_CUMULATIVE_PRIMARY_COMPARISON_COUNT] = (
        EVENT_MVE_CUMULATIVE_PRIMARY_COMPARISON_COUNT
    )
    current_familywise_hypothesis_count: Literal[EVENT_MVE_CURRENT_FAMILYWISE_HYPOTHESIS_COUNT] = (
        EVENT_MVE_CURRENT_FAMILYWISE_HYPOTHESIS_COUNT
    )
    block_length_trading_days: Literal[EVENT_MVE_BLOCK_LENGTH] = EVENT_MVE_BLOCK_LENGTH
    bootstrap_repetitions: Literal[EVENT_MVE_BOOTSTRAP_REPETITIONS] = EVENT_MVE_BOOTSTRAP_REPETITIONS
    bootstrap_seed: Literal[EVENT_MVE_BOOTSTRAP_SEED] = EVENT_MVE_BOOTSTRAP_SEED
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: Literal[EVENT_MVE_MAX_RSS_BYTES] = EVENT_MVE_MAX_RSS_BYTES
    resource_max_temp_bytes: Literal[EVENT_MVE_MAX_TEMP_BYTES] = EVENT_MVE_MAX_TEMP_BYTES
    resource_max_wall_seconds: Literal[None] = None
    database_read_allowed: Literal[False] = False
    database_write_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    tushare_read_allowed: Literal[False] = False
    qlib_calendar_read_allowed: Literal[True] = True
    qlib_feature_read_allowed: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    factor_catalog_write_allowed: Literal[False] = False
    strategy_package_write_allowed: Literal[False] = False
    runtime_activation_allowed: Literal[False] = False
    position_weight_output_allowed: Literal[False] = False
    final_model_output_allowed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenFinancialEventInformationSetRequestV1":
        if (
            self.objective_contract != ObjectiveContract.ALPHA_RANKING
            or self.study_type != ResearchStudyType.LEARNABILITY_AUDIT
            or self.decision_use != DecisionUse.NAVIGATION_ONLY
            or self.model_trials != build_default_event_model_trials()
            or self.feature_schema_hash != EVENT_MVE_FEATURE_SCHEMA_HASH
        ):
            raise ValueError("financial-event research contract drift")
        required_roles = {
            "n3_event_source_manifest",
            "n3_event_source_request",
            "n3_event_source_receipt",
            "n3_event_source_projection",
            "n3_event_source_support",
            "n3_event_n2b_manifest",
            "n3_event_n2b_request",
            "n3_event_n2b_receipt",
            "n3_event_n2b_outcomes",
            "n3_event_n2b_top5",
            "n3_event_n2b_signal_daily",
            "n3_event_n1_manifest",
            "n3_event_n1_request",
            "n3_event_n1_cpcv",
            "n3_event_n1_regime_daily",
        }
        roles = [item.role for item in self.evidence_refs]
        if len(roles) != len(set(roles)) or set(roles) != required_roles:
            raise ValueError("financial-event evidence role roster drift")
        if (
            tuple(sorted(set(self.trading_calendar))) != self.trading_calendar
            or self.signal_start not in self.trading_calendar
            or self.signal_end not in self.trading_calendar
            or self.trading_calendar[-1] != self.n1_calendar_data_cutoff
            or self.n1_market_calendar_cutoff != date(2026, 6, 30)
            or self.n1_calendar_data_cutoff != date(2026, 3, 10)
            or canonical_json_sha256({"market_sessions": [item.isoformat() for item in self.trading_calendar]})
            != self.trading_calendar_sha256
        ):
            raise ValueError("financial-event frozen trading calendar drift")
        for path, bundle_id in (
            (self.source_bundle_path, self.source_bundle_id),
            (self.n2b_bundle_path, self.n2b_bundle_id),
            (self.n1_bundle_path, self.n1_bundle_id),
        ):
            if path.replace("\\", "/").rstrip("/").split("/")[-1] != bundle_id:
                raise ValueError("financial-event bundle path/id drift")
        expected_dataset = canonical_json_sha256(
            {
                "source_dataset_identity": self.source_dataset_identity,
                "source_bundle_id": self.source_bundle_id,
                "source_projection_sha256": self.source_projection_sha256,
                "n1_split_policy_sha256": self.n1_split_policy_sha256,
                "trading_calendar_sha256": self.trading_calendar_sha256,
                "feature_schema_hash": self.feature_schema_hash,
                "policy_identity": self.policy_identity,
                "evidence_refs": [item.model_dump(mode="json") for item in self.evidence_refs],
            }
        )
        if self.dataset_identity != expected_dataset:
            raise ValueError("financial-event composite dataset identity drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advn3fevreq_{digest[:24]}":
            raise ValueError("financial-event request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


class FinancialEventInformationSetReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_financial_event_information_set_receipt_v1"] = (
        "advisory_n3_financial_event_information_set_receipt_v1"
    )
    receipt_id: str = Field(pattern=r"^advn3fevrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    planned_trial_count: Literal[3] = 3
    generated_trial_count: Literal[3] = 3
    evaluated_trial_count: Literal[3] = 3
    selected_trial_count: int = Field(ge=0, le=1)
    selected_trial_id: Literal["EVENT_SIGNED_CONTENT_V1"] | None
    eligible_trial_ids: tuple[str, ...]
    evidence_class: Literal[
        "EXPLORATORY_CANDIDATE_SELECTED_NON_VINTAGE",
        "EXPLORATORY_NOT_SELECTED_NON_VINTAGE",
        "EXPLORATORY_INSUFFICIENT_SUPPORT_NON_VINTAGE",
    ]
    result_class: ResearchResultClass = ResearchResultClass.EXPLORATORY
    decision_use: DecisionUse = DecisionUse.NAVIGATION_ONLY
    next_task: Literal[
        "N3_FINANCIAL_EVENT_VINTAGE_SOURCE_DECISION",
        "N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION",
    ]
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
    def validate_receipt(self) -> "FinancialEventInformationSetReceiptV1":
        selected = self.selected_trial_id is not None
        expected_next = (
            "N3_FINANCIAL_EVENT_VINTAGE_SOURCE_DECISION" if selected else "N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION"
        )
        expected_eligible = ("EVENT_SIGNED_CONTENT_V1",) if selected else ()
        if (
            self.result_class != ResearchResultClass.EXPLORATORY
            or self.decision_use != DecisionUse.NAVIGATION_ONLY
            or self.selected_trial_count != int(selected)
            or self.next_task != expected_next
            or self.eligible_trial_ids != expected_eligible
        ):
            raise ValueError("financial-event receipt selection relation drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advn3fevrcpt_{digest[:24]}":
            raise ValueError("financial-event receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def build_financial_event_request(**values: Any) -> FrozenFinancialEventInformationSetRequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "frozen_advisory_n3_financial_event_information_set_request_v1",
        "created_at": created_at,
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "planned_trial_count": 3,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "selectable_trial_count": 1,
        "model_trials": build_default_event_model_trials(),
        "feature_schema_version": EVENT_MVE_FEATURE_SCHEMA_VERSION,
        "feature_schema_hash": EVENT_MVE_FEATURE_SCHEMA_HASH,
        "resource_max_rss_bytes": EVENT_MVE_MAX_RSS_BYTES,
        "resource_max_temp_bytes": EVENT_MVE_MAX_TEMP_BYTES,
        "resource_max_wall_seconds": None,
        "database_read_allowed": False,
        "database_write_allowed": False,
        "network_read_allowed": False,
        "tushare_read_allowed": False,
        "qlib_calendar_read_allowed": True,
        "qlib_feature_read_allowed": False,
        "sealed_holdout_accessed": False,
        "factor_catalog_write_allowed": False,
        "strategy_package_write_allowed": False,
        "runtime_activation_allowed": False,
        "position_weight_output_allowed": False,
        "final_model_output_allowed": False,
        "deployable": False,
        **values,
    }
    draft = FrozenFinancialEventInformationSetRequestV1.model_construct(
        request_id="advn3fevreq_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenFinancialEventInformationSetRequestV1(
        request_id=f"advn3fevreq_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_financial_event_receipt(**values: Any) -> FinancialEventInformationSetReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_n3_financial_event_information_set_receipt_v1",
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
    draft = FinancialEventInformationSetReceiptV1.model_construct(
        receipt_id="advn3fevrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FinancialEventInformationSetReceiptV1(
        receipt_id=f"advn3fevrcpt_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )


__all__ = [
    "EVENT_DIRECTION_BY_TYPE",
    "EVENT_DISCLOSURE_FEATURES",
    "EVENT_DISCLOSURE_SCHEMA_FEATURES",
    "EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX",
    "EVENT_MVE_EXPERIMENT_ID",
    "EVENT_MVE_FEATURE_SCHEMA_HASH",
    "EVENT_MVE_FEATURE_SCHEMA_VERSION",
    "EVENT_MVE_HYPOTHESIS_FAMILY_ID",
    "EVENT_MVE_SOURCE_PROJECTION_SHA256",
    "EVENT_MVE_SOURCE_QUALITY",
    "EVENT_PARENT_FEATURES",
    "EVENT_SIGNED_FEATURES",
    "EVENT_SIGNED_SCHEMA_FEATURES",
    "EVENT_SOURCE_TYPES",
    "FinancialEventInformationSetReceiptV1",
    "FinancialEventModelTrialV1",
    "FrozenFinancialEventInformationSetRequestV1",
    "build_default_event_model_trials",
    "build_financial_event_receipt",
    "build_financial_event_request",
]
