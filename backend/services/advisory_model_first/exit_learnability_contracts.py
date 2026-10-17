from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.entry_exit_formal_contracts import (
    RESOURCE_MAX_RSS_BYTES,
    ActionSupportSpecV1,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    Tier1EvidenceState,
    Tier1MetricInferenceV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
EXIT_LEARNABILITY_EXPERIMENT_ID = "ADVISORY-N2-EXIT-LEARNABILITY-V1"
EXIT_LEARNABILITY_FAMILY_ID = "ADVISORY-N2-EXIT-LEARNABILITY-FIXED-V1"
EXIT_FEATURE_SCHEMA_VERSION = "advisory_n2_exit_feature_schema_v1"
EXIT_FEATURE_COLUMNS = (
    "selection_rank",
    "selection_score",
    "holding_trading_days_elapsed",
    "holding_fraction_of_time_stop",
    "unrealized_close_return_bps",
    "relative_return_since_entry_bps",
    "return_1d_bps",
    "return_3d_bps",
    "return_5d_bps",
    "return_10d_bps",
    "realized_vol_5d_bps",
    "realized_vol_10d_bps",
    "realized_vol_20d_bps",
    "drawdown_from_peak_since_entry_bps",
    "runup_from_entry_peak_bps",
    "distance_to_stop_bps",
    "distance_to_take_profit_bps",
    "distance_to_trailing_stop_bps",
    "intraday_range_bps",
    "close_location_in_day",
    "volume_ratio_5d_to_20d",
    "market_regime",
)
EXIT_CATEGORICAL_FEATURE_COLUMNS = ("market_regime",)
EXIT_ECONOMIC_THRESHOLD_BPS = 5.0
EXIT_BOOTSTRAP_REPETITIONS = 2000
EXIT_BOOTSTRAP_SEED = 20260902
EXPECTED_READY_PATH_COUNT = 28
EXPECTED_OOF_PREDICTIONS_PER_ROW = 7


class ExitLearnabilityModelSpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n2_exit_learnability_model_spec_v1"] = (
        "advisory_n2_exit_learnability_model_spec_v1"
    )
    estimator_family: Literal["SKLEARN_RIDGE_V1"] = "SKLEARN_RIDGE_V1"
    alpha: Literal[100.0] = 100.0
    solver: Literal["svd"] = "svd"
    fit_intercept: Literal[True] = True
    numeric_imputer: Literal["TRAIN_FOLD_MEDIAN"] = "TRAIN_FOLD_MEDIAN"
    numeric_scaler: Literal["TRAIN_FOLD_STANDARD_SCALER"] = "TRAIN_FOLD_STANDARD_SCALER"
    categorical_encoder: Literal["TRAIN_FOLD_ONE_HOT_UNKNOWN_IGNORE_DENSE"] = "TRAIN_FOLD_ONE_HOT_UNKNOWN_IGNORE_DENSE"
    oof_aggregation: Literal["MEAN_ACROSS_VALIDATION_PATHS"] = "MEAN_ACROSS_VALIDATION_PATHS"
    expected_ready_path_count: Literal[28] = EXPECTED_READY_PATH_COUNT
    expected_oof_predictions_per_row: Literal[7] = EXPECTED_OOF_PREDICTIONS_PER_ROW
    model_trial_count: Literal[1] = 1


class ExitLearnabilityInferenceSpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n2_exit_learnability_inference_spec_v1"] = (
        "advisory_n2_exit_learnability_inference_spec_v1"
    )
    exit_threshold_bps: Literal[5.0] = EXIT_ECONOMIC_THRESHOLD_BPS
    economic_threshold_bps: Literal[5.0] = EXIT_ECONOMIC_THRESHOLD_BPS
    block_length_trading_days: Literal[20] = 20
    bootstrap_repetitions: Literal[2000] = EXIT_BOOTSTRAP_REPETITIONS
    bootstrap_seed: Literal[20260902] = EXIT_BOOTSTRAP_SEED
    target_power: Literal[0.8] = 0.8
    confidence_level: Literal[0.95] = 0.95
    policy: Literal["FIRST_PREDICTED_ADVANTAGE_GT_THRESHOLD_V1"] = "FIRST_PREDICTED_ADVANTAGE_GT_THRESHOLD_V1"
    position_semantics: Literal["FIVE_FIXED_EQUAL_SLOTS"] = "FIVE_FIXED_EQUAL_SLOTS"


class FrozenAdvisoryN2ExitLearnabilityRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_n2_exit_learnability_request_v1"] = (
        "frozen_advisory_n2_exit_learnability_request_v1"
    )
    request_id: str = Field(pattern=r"^advexlearnreq_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: Literal[ObjectiveContract.RISK_MANAGED_ADVISORY] = ObjectiveContract.RISK_MANAGED_ADVISORY
    study_type: Literal[ResearchStudyType.LEARNABILITY_AUDIT] = ResearchStudyType.LEARNABILITY_AUDIT
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    planned_trial_count: Literal[1] = 1
    generated_trial_count: Literal[0] = 0
    evaluated_trial_count: Literal[0] = 0
    selected_trial_count: Literal[0] = 0
    sealed_holdout_accessed: Literal[False] = False
    n2_action_request_path: str = Field(min_length=1)
    n2_action_request_ref: EvidenceReferenceV1
    n2_action_bundle_path: str = Field(min_length=1)
    n2_action_manifest_ref: EvidenceReferenceV1
    n2_action_receipt_ref: EvidenceReferenceV1
    exit_labels_ref: EvidenceReferenceV1
    exit_decisions_ref: EvidenceReferenceV1
    exit_episode_best_ref: EvidenceReferenceV1
    n1_request_path: str = Field(min_length=1)
    n1_request_ref: EvidenceReferenceV1
    n1_bundle_path: str = Field(min_length=1)
    n1_manifest_ref: EvidenceReferenceV1
    policy_dataset_root: str = Field(min_length=1)
    policy_dataset_manifest_ref: EvidenceReferenceV1
    candidate_episode_labels_ref: EvidenceReferenceV1
    cpcv_paths_ref: EvidenceReferenceV1
    parent_spike_path: str = Field(min_length=1)
    parent_spike_ref: EvidenceReferenceV1
    research_window_contract_ref: EvidenceReferenceV1
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    parent_feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    feature_schema_version: Literal["advisory_n2_exit_feature_schema_v1"] = EXIT_FEATURE_SCHEMA_VERSION
    feature_columns: tuple[str, ...] = EXIT_FEATURE_COLUMNS
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    baseline_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    shadow_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    intervention_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    model_spec: ExitLearnabilityModelSpecV1
    inference_spec: ExitLearnabilityInferenceSpecV1
    support_spec: ActionSupportSpecV1
    decision_start: date
    decision_end: date
    outcome_cutoff: date
    qlib_daily_root: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: Literal[RESOURCE_MAX_RSS_BYTES] = RESOURCE_MAX_RSS_BYTES

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenAdvisoryN2ExitLearnabilityRequestV1":
        if self.feature_columns != EXIT_FEATURE_COLUMNS:
            raise ValueError("Exit learnability feature roster drift")
        expected_schema_hash = canonical_json_sha256(
            {
                "feature_schema_version": self.feature_schema_version,
                "feature_columns": list(EXIT_FEATURE_COLUMNS),
                "categorical_columns": list(EXIT_CATEGORICAL_FEATURE_COLUMNS),
            }
        )
        if self.feature_schema_hash != expected_schema_hash:
            raise ValueError("Exit learnability feature schema hash drift")
        if not (self.decision_start <= self.decision_end <= self.outcome_cutoff):
            raise ValueError("Exit learnability date window is invalid")
        expected_roles = {
            "n2_action_request_ref": "exit_learnability_n2_action_request",
            "n2_action_manifest_ref": "exit_learnability_n2_action_manifest",
            "n2_action_receipt_ref": "exit_learnability_n2_action_receipt",
            "exit_labels_ref": "exit_learnability_exit_labels",
            "exit_decisions_ref": "exit_learnability_exit_decisions",
            "exit_episode_best_ref": "exit_learnability_exit_episode_best",
            "n1_request_ref": "exit_learnability_n1_request",
            "n1_manifest_ref": "exit_learnability_n1_manifest",
            "policy_dataset_manifest_ref": "exit_learnability_policy_dataset_manifest",
            "candidate_episode_labels_ref": "exit_learnability_candidate_episode_labels",
            "cpcv_paths_ref": "exit_learnability_cpcv_paths",
            "parent_spike_ref": "exit_learnability_parent_spike",
            "research_window_contract_ref": "n0_window_contract",
        }
        for field_name, role in expected_roles.items():
            if getattr(self, field_name).role != role:
                raise ValueError(f"Exit learnability evidence role drift: {field_name}")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advexlearnreq_{digest[:24]}":
            raise ValueError("Exit learnability request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at"},
        )


class ExitLearnabilitySupportV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    evaluated_episode_count: int = Field(gt=0)
    evaluated_entry_day_count: int = Field(gt=0)
    evaluated_action_day_count: int = Field(gt=0)
    intervention_episode_count: int = Field(ge=0)
    intervention_action_day_count: int = Field(ge=0)
    intervention_action_day_fraction: float = Field(ge=0, le=1)
    intervention_days_by_regime: dict[str, int]
    effective_intervention_block_count: int = Field(ge=0)
    support_sufficient: bool
    reason_codes: tuple[str, ...]


class AdvisoryN2ExitLearnabilityReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n2_exit_learnability_receipt_v1"] = "advisory_n2_exit_learnability_receipt_v1"
    receipt_id: str = Field(pattern=r"^advexlearnrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    objective_contract: Literal[ObjectiveContract.RISK_MANAGED_ADVISORY] = ObjectiveContract.RISK_MANAGED_ADVISORY
    study_type: Literal[ResearchStudyType.LEARNABILITY_AUDIT] = ResearchStudyType.LEARNABILITY_AUDIT
    model_family: Literal["SKLEARN_RIDGE_V1"] = "SKLEARN_RIDGE_V1"
    planned_trial_count: Literal[1] = 1
    generated_trial_count: Literal[1] = 1
    evaluated_trial_count: Literal[1] = 1
    selected_trial_count: Literal[0] = 0
    ready_path_count: Literal[28] = EXPECTED_READY_PATH_COUNT
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    feature_row_count: int = Field(gt=0)
    oof_row_count: int = Field(gt=0)
    oof_predictions_per_row: Literal[7] = EXPECTED_OOF_PREDICTIONS_PER_ROW
    evaluated_episode_count: int = Field(gt=0)
    evaluated_entry_day_count: int = Field(gt=0)
    row_diagnostics: dict[str, Any]
    episode_diagnostics: dict[str, Any]
    policy_lift: Tier1MetricInferenceV1
    intervention_support: ExitLearnabilitySupportV1
    oracle_mean_lift_bps: float = Field(ge=0)
    oracle_capture_ratio: float | None
    evidence_sufficient: bool
    evidence_reason_codes: tuple[str, ...]
    result_class: ResearchResultClass
    decision_use: DecisionUse
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    result_files_sha256: str = Field(pattern=SHA256_PATTERN)
    resource_report_sha256: str = Field(pattern=SHA256_PATTERN)
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False
    final_refit_performed: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryN2ExitLearnabilityReceiptV1":
        powered = self.policy_lift.mde_bps <= max(
            self.policy_lift.point_estimate_bps,
            self.policy_lift.economic_threshold_bps,
        )
        expected_sufficient = self.intervention_support.support_sufficient and powered
        if self.evidence_sufficient != expected_sufficient:
            raise ValueError("Exit learnability evidence_sufficient differs from frozen rules")
        decisive = expected_sufficient and self.policy_lift.evidence_state in {
            Tier1EvidenceState.HIGH,
            Tier1EvidenceState.LOW,
        }
        expected_use = DecisionUse.DIRECTION_GATE if decisive else DecisionUse.NAVIGATION_ONLY
        expected_class = (
            ResearchResultClass.CONTROL_READY
            if decisive and self.policy_lift.evidence_state == Tier1EvidenceState.HIGH
            else ResearchResultClass.NEGATIVE
            if decisive
            else ResearchResultClass.EXPLORATORY
        )
        if self.decision_use != expected_use or self.result_class != expected_class:
            raise ValueError("Exit learnability result classification differs from frozen rules")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advexlearnrcpt_{digest[:24]}":
            raise ValueError("Exit learnability receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"receipt_id", "receipt_sha256", "created_at"},
        )


def build_exit_learnability_request(**values: Any) -> FrozenAdvisoryN2ExitLearnabilityRequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "frozen_advisory_n2_exit_learnability_request_v1",
        "created_at": created_at,
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "planned_trial_count": 1,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "sealed_holdout_accessed": False,
        "feature_schema_version": EXIT_FEATURE_SCHEMA_VERSION,
        "feature_columns": EXIT_FEATURE_COLUMNS,
        "model_spec": ExitLearnabilityModelSpecV1(),
        "inference_spec": ExitLearnabilityInferenceSpecV1(),
        "support_spec": ActionSupportSpecV1(),
        "resource_max_rss_bytes": RESOURCE_MAX_RSS_BYTES,
        **values,
    }
    draft = FrozenAdvisoryN2ExitLearnabilityRequestV1.model_construct(
        request_id="advexlearnreq_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenAdvisoryN2ExitLearnabilityRequestV1(
        request_id=f"advexlearnreq_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_exit_learnability_receipt(**values: Any) -> AdvisoryN2ExitLearnabilityReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_n2_exit_learnability_receipt_v1",
        "status": "COMPLETE",
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT,
        "model_family": "SKLEARN_RIDGE_V1",
        "planned_trial_count": 1,
        "generated_trial_count": 1,
        "evaluated_trial_count": 1,
        "selected_trial_count": 0,
        "ready_path_count": EXPECTED_READY_PATH_COUNT,
        "oof_predictions_per_row": EXPECTED_OOF_PREDICTIONS_PER_ROW,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "final_refit_performed": False,
        "created_at": created_at,
        **values,
    }
    draft = AdvisoryN2ExitLearnabilityReceiptV1.model_construct(
        receipt_id="advexlearnrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return AdvisoryN2ExitLearnabilityReceiptV1(
        receipt_id=f"advexlearnrcpt_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )


__all__ = [
    "AdvisoryN2ExitLearnabilityReceiptV1",
    "EXIT_CATEGORICAL_FEATURE_COLUMNS",
    "EXIT_ECONOMIC_THRESHOLD_BPS",
    "EXIT_FEATURE_COLUMNS",
    "EXIT_FEATURE_SCHEMA_VERSION",
    "EXIT_LEARNABILITY_EXPERIMENT_ID",
    "EXIT_LEARNABILITY_FAMILY_ID",
    "ExitLearnabilityInferenceSpecV1",
    "ExitLearnabilityModelSpecV1",
    "ExitLearnabilitySupportV1",
    "FrozenAdvisoryN2ExitLearnabilityRequestV1",
    "build_exit_learnability_receipt",
    "build_exit_learnability_request",
]
