from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
)
from backend.services.advisory_model_first.score_hmm_admission_contracts import (
    RAW_MARKET_FEATURE_COLUMNS,
    SCORE_FEATURE_COLUMNS,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256

SHA256_PATTERN = r"^[0-9a-f]{64}$"
CAUSAL_ADMISSION_EXPERIMENT_ID = "ADVISORY-N3-AUX-CAUSAL-ADMISSION-V2-1-R1"
CAUSAL_ADMISSION_FAMILY_ID = "ADVISORY-N3-AUX-CAUSAL-ADMISSION-V2-1"
CAUSAL_ADMISSION_STAGE = "N3_AUX_CAUSAL_ADMISSION_V2_1_R1"
CAUSAL_ADMISSION_TARGET_ID = "POLICY_EPISODE_NET_RETURN_BPS_MAX20_V1"
CAUSAL_ADMISSION_FEATURE_COLUMNS = (*SCORE_FEATURE_COLUMNS, *RAW_MARKET_FEATURE_COLUMNS)
CAUSAL_ADMISSION_FEATURE_SCHEMA = "advisory_causal_admission_v2_1_score_raw_v1"
CAUSAL_ADMISSION_FEATURE_SCHEMA_HASH = canonical_json_sha256(
    {
        "schema_version": CAUSAL_ADMISSION_FEATURE_SCHEMA,
        "feature_columns": list(CAUSAL_ADMISSION_FEATURE_COLUMNS),
        "candidate_depth": 20,
        "action_depth": 5,
        "clock": "CHRONOLOGICAL_AS_OF_V2_1",
    }
)

STATIC_ARM_ID = "R1_STATIC_RIDGE_V1"
EXPANDING_ARM_ID = "R1_EXPANDING_20D_RIDGE_V1"
CAUSAL_ADMISSION_ARM_IDS = (STATIC_ARM_ID, EXPANDING_ARM_ID)
CAUSAL_ADMISSION_NEXT_TASK_BY_EVIDENCE = {
    "CAUSAL_ADMISSION_V2_1_SELECTED_ZERO": "N3_AUX_CAUSAL_ADMISSION_V2_1_FRONTIER_CLOSED",
    "CAUSAL_ADMISSION_V2_1_EXPLORATORY_INSUFFICIENT_SUPPORT": (
        "N3_AUX_CAUSAL_ADMISSION_V2_1_INSUFFICIENT_SUPPORT_REVIEW"
    ),
    "CAUSAL_ADMISSION_V2_1_CANDIDATE_SELECTED_NAVIGATION_ONLY": ("N3_AUX_CAUSAL_ADMISSION_V2_1_R3_CONFIRMATION_DESIGN"),
}

INITIAL_START = date(2024, 7, 4)
INITIAL_END = date(2024, 11, 27)
INNER_START = date(2024, 11, 28)
INNER_END = date(2025, 2, 12)
OUTER_START = date(2025, 2, 13)
OUTER_END = date(2026, 2, 2)


class CausalAdmissionArmSpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_causal_admission_arm_v1"] = "advisory_causal_admission_arm_v1"
    arm_id: Literal["R1_STATIC_RIDGE_V1", "R1_EXPANDING_20D_RIDGE_V1"]
    candidate_index: int = Field(gt=0)
    update_kind: Literal["STATIC", "EXPANDING_20D"]
    update_interval_trading_days: Literal[0, 20]
    feature_columns: tuple[str, ...] = CAUSAL_ADMISSION_FEATURE_COLUMNS
    feature_schema_hash: Literal[CAUSAL_ADMISSION_FEATURE_SCHEMA_HASH] = CAUSAL_ADMISSION_FEATURE_SCHEMA_HASH

    @model_validator(mode="after")
    def validate_arm(self) -> "CausalAdmissionArmSpecV1":
        expected = {
            STATIC_ARM_ID: ("STATIC", 0),
            EXPANDING_ARM_ID: ("EXPANDING_20D", 20),
        }[self.arm_id]
        if (self.update_kind, self.update_interval_trading_days) != expected:
            raise ValueError("causal Admission arm update contract differs from R0")
        if self.feature_columns != CAUSAL_ADMISSION_FEATURE_COLUMNS:
            raise ValueError("causal Admission feature columns differ from R0")
        return self


def default_causal_admission_arms(first_candidate_index: int) -> tuple[CausalAdmissionArmSpecV1, ...]:
    return (
        CausalAdmissionArmSpecV1(
            arm_id=STATIC_ARM_ID,
            candidate_index=first_candidate_index,
            update_kind="STATIC",
            update_interval_trading_days=0,
        ),
        CausalAdmissionArmSpecV1(
            arm_id=EXPANDING_ARM_ID,
            candidate_index=first_candidate_index + 1,
            update_kind="EXPANDING_20D",
            update_interval_trading_days=20,
        ),
    )


class FrozenAdvisoryCausalAdmissionRequestV2(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_causal_admission_request_v2"] = (
        "frozen_advisory_causal_admission_request_v2"
    )
    request_id: str = Field(pattern=r"^advcausal_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    contract_revision: Literal["V2_1"] = "V2_1"
    experiment_id: Literal[CAUSAL_ADMISSION_EXPERIMENT_ID] = CAUSAL_ADMISSION_EXPERIMENT_ID
    hypothesis_family_id: Literal[CAUSAL_ADMISSION_FAMILY_ID] = CAUSAL_ADMISSION_FAMILY_ID
    research_stage: Literal[CAUSAL_ADMISSION_STAGE] = CAUSAL_ADMISSION_STAGE
    objective_contract: Literal[ObjectiveContract.RISK_MANAGED_ADVISORY] = ObjectiveContract.RISK_MANAGED_ADVISORY
    study_type: Literal[ResearchStudyType.LEARNABILITY_AUDIT] = ResearchStudyType.LEARNABILITY_AUDIT
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY

    parent_v1_bundle_path: str = Field(min_length=1)
    parent_v1_bundle_id: str = Field(pattern=SHA256_PATTERN)
    parent_v1_request_sha256: str = Field(pattern=SHA256_PATTERN)
    parent_v1_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    program_id: str = Field(min_length=1)
    binding_version_id: str = Field(min_length=1)
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    shadow_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    primary_target_id: Literal[CAUSAL_ADMISSION_TARGET_ID] = CAUSAL_ADMISSION_TARGET_ID

    # These stay ordinary date fields so the frozen request remains JSON
    # round-trippable.  The validator below pins their exact R0 values.
    initial_start: date = INITIAL_START
    initial_end: date = INITIAL_END
    inner_start: date = INNER_START
    inner_end: date = INNER_END
    outer_start: date = OUTER_START
    outer_end: date = OUTER_END
    expected_initial_days: Literal[98] = 98
    expected_inner_days: Literal[48] = 48
    expected_outer_days: Literal[240] = 240
    minimum_mature_train_days: Literal[60] = 60
    minimum_mature_train_rows: Literal[1000] = 1000
    maturity_rule: Literal["LABEL_INFORMATION_END_LT_FIT_CUTOFF"] = "LABEL_INFORMATION_END_LT_FIT_CUTOFF"

    arms: tuple[CausalAdmissionArmSpecV1, CausalAdmissionArmSpecV1]
    planned_trial_count: Literal[2] = 2
    max_estimator_fit_count: Literal[32] = 32
    cumulative_evaluated_trial_count_prior: int = Field(ge=1284)
    cumulative_candidate_index_prior: int = Field(ge=88)

    ridge_alpha: Literal[100.0] = 100.0
    ridge_solver: Literal["lsqr"] = "lsqr"
    logistic_c: Literal[1.0] = 1.0
    logistic_solver: Literal["lbfgs"] = "lbfgs"
    logistic_max_iter: Literal[1000] = 1000
    model_random_state: Literal[20260907] = 20260907
    action_utility_buffer_bps: Literal[5.0] = 5.0
    risk_loss_ratio_limit: Literal[1.10] = 1.10

    inner_minimum_paired_days: Literal[40] = 40
    inner_minimum_intervention_days: Literal[12] = 12
    inner_minimum_intervention_fraction: Literal[0.25] = 0.25
    inner_minimum_take_days: Literal[5] = 5
    inner_minimum_skip_days: Literal[5] = 5
    outer_minimum_paired_days: Literal[200] = 200
    outer_minimum_intervention_days: Literal[60] = 60
    outer_minimum_intervention_fraction: Literal[0.25] = 0.25
    outer_minimum_take_days: Literal[60] = 60
    outer_minimum_skip_days: Literal[60] = 60
    outer_minimum_intervention_days_per_regime: Literal[20] = 20
    block_length_trading_days: Literal[20] = 20
    bootstrap_repetitions: Literal[2000] = 2000
    bootstrap_seed: Literal[20260907] = 20260907
    familywise_method: Literal["HOLM_BONFERRONI"] = "HOLM_BONFERRONI"
    familywise_alpha: Literal[0.05] = 0.05
    minimum_economic_lift_bps: Literal[5.0] = 5.0
    pre_run_mde_daily_rows: Literal[240] = 240
    pre_run_mde_standard_deviation_bps: Literal[168.4145168202] = 168.4145168202
    pre_run_mde_effective_sample_size: Literal[113.7021727054] = 113.7021727054
    pre_run_mde_bps: Literal[44.2485434246] = 44.2485434246
    confirmatory_capable: Literal[False] = False

    registry_path: str = Field(min_length=1)
    registry_sha256_at_request: str = Field(pattern=SHA256_PATTERN)
    registry_record_count_at_request: int = Field(ge=1)
    auxiliary_route_path: str = Field(min_length=1)
    auxiliary_route_sha256_at_request: str = Field(pattern=SHA256_PATTERN)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: Literal[8589934592] = 8589934592
    resource_max_temp_bytes: Literal[8589934592] = 8589934592
    resource_max_wall_seconds: Literal[None] = None
    database_read_allowed: Literal[False] = False
    database_write_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    tushare_read_allowed: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    runtime_activation_allowed: Literal[False] = False
    position_weight_output_allowed: Literal[False] = False
    selection_rank_change_allowed: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenAdvisoryCausalAdmissionRequestV2":
        if (
            self.initial_start,
            self.initial_end,
            self.inner_start,
            self.inner_end,
            self.outer_start,
            self.outer_end,
        ) != (INITIAL_START, INITIAL_END, INNER_START, INNER_END, OUTER_START, OUTER_END):
            raise ValueError("causal Admission R1 dates differ from R0")
        if tuple(arm.arm_id for arm in self.arms) != CAUSAL_ADMISSION_ARM_IDS:
            raise ValueError("causal Admission R1 arm order differs from R0")
        expected_indices = (
            self.cumulative_candidate_index_prior + 1,
            self.cumulative_candidate_index_prior + 2,
        )
        if tuple(arm.candidate_index for arm in self.arms) != expected_indices:
            raise ValueError("causal Admission candidate indices are not consecutive")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected or self.request_id != f"advcausal_{expected[:24]}":
            raise ValueError("causal Admission request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at", "output_root"},
        )


def build_causal_admission_request(**values: Any) -> FrozenAdvisoryCausalAdmissionRequestV2:
    payload = dict(values)
    payload.setdefault("schema_version", "frozen_advisory_causal_admission_request_v2")
    payload.setdefault("created_at", datetime.now(timezone.utc))
    prior = int(payload["cumulative_candidate_index_prior"])
    payload.setdefault("arms", default_causal_admission_arms(prior + 1))
    functional_fields = set(FrozenAdvisoryCausalAdmissionRequestV2.model_fields) - {
        "request_id",
        "request_sha256",
        "created_at",
        "output_root",
    }
    normalized = FrozenAdvisoryCausalAdmissionRequestV2.model_construct(
        request_id="advcausal_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    ).model_dump(mode="json", include=functional_fields)
    digest = canonical_json_sha256(normalized)
    payload["request_sha256"] = digest
    payload["request_id"] = f"advcausal_{digest[:24]}"
    return FrozenAdvisoryCausalAdmissionRequestV2.model_validate(payload)


class CausalAdmissionFrontierReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_causal_admission_frontier_receipt_v1"] = (
        "advisory_causal_admission_frontier_receipt_v1"
    )
    receipt_id: str = Field(pattern=r"^advcausalrcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    bundle_id: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    objective_contract: Literal[ObjectiveContract.RISK_MANAGED_ADVISORY] = ObjectiveContract.RISK_MANAGED_ADVISORY
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    result_class: Literal[ResearchResultClass.EXPLORATORY] = ResearchResultClass.EXPLORATORY
    evidence_class: Literal[
        "CAUSAL_ADMISSION_V2_1_SELECTED_ZERO",
        "CAUSAL_ADMISSION_V2_1_EXPLORATORY_INSUFFICIENT_SUPPORT",
        "CAUSAL_ADMISSION_V2_1_CANDIDATE_SELECTED_NAVIGATION_ONLY",
    ]
    next_task: Literal[
        "N3_AUX_CAUSAL_ADMISSION_V2_1_FRONTIER_CLOSED",
        "N3_AUX_CAUSAL_ADMISSION_V2_1_INSUFFICIENT_SUPPORT_REVIEW",
        "N3_AUX_CAUSAL_ADMISSION_V2_1_R3_CONFIRMATION_DESIGN",
    ]
    planned_trial_count: Literal[2] = 2
    evaluated_trial_count: int = Field(ge=1, le=2)
    selected_trial_count: int = Field(ge=0, le=1)
    selected_arm_id: Literal["R1_STATIC_RIDGE_V1", "R1_EXPANDING_20D_RIDGE_V1"] | None
    sealed_holdout_accessed: Literal[False] = False
    runtime_eligible: Literal[False] = False
    runtime_activation_written: Literal[False] = False
    position_weight_output: Literal[False] = False
    selection_rank_changed: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "CausalAdmissionFrontierReceiptV1":
        if self.selected_trial_count != int(self.selected_arm_id is not None):
            raise ValueError("selected count and selected arm disagree")
        if self.next_task != CAUSAL_ADMISSION_NEXT_TASK_BY_EVIDENCE[self.evidence_class]:
            raise ValueError("causal Admission next task disagrees with evidence class")
        expected = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != expected or self.receipt_id != f"advcausalrcpt_{expected[:24]}":
            raise ValueError("causal Admission receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def build_causal_admission_receipt(**values: Any) -> CausalAdmissionFrontierReceiptV1:
    payload = dict(values)
    payload.setdefault("schema_version", "advisory_causal_admission_frontier_receipt_v1")
    payload.setdefault("created_at", datetime.now(timezone.utc))
    payload.setdefault("next_task", CAUSAL_ADMISSION_NEXT_TASK_BY_EVIDENCE[payload["evidence_class"]])
    functional_fields = set(CausalAdmissionFrontierReceiptV1.model_fields) - {
        "receipt_id",
        "receipt_sha256",
        "created_at",
    }
    normalized = CausalAdmissionFrontierReceiptV1.model_construct(
        receipt_id="advcausalrcpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    ).model_dump(mode="json", include=functional_fields)
    digest = canonical_json_sha256(normalized)
    payload["receipt_sha256"] = digest
    payload["receipt_id"] = f"advcausalrcpt_{digest[:24]}"
    return CausalAdmissionFrontierReceiptV1.model_validate(payload)
