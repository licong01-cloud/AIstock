from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    N1_DATASET_IDENTITY,
    N1_DATA_CUTOFF,
    N1_DECISION_END,
    N1_DECISION_START,
    N1_RESOURCE_LIMIT_BYTES,
    N1_WINDOW_ID,
    Tier1InferencePolicyV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
ALPHA_AUDIT_SCHEMA_VERSION = "frozen_advisory_three_arm_alpha_audit_request_v1"
ALPHA_AUDIT_BUNDLE_SCHEMA = "advisory_three_arm_alpha_audit_bundle_v1"
ALPHA_AUDIT_EXPERIMENT_ID = "ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT"
ALPHA_AUDIT_PARENT_LINEAGE = ("ADVISORY-N1-TIER1-ORACLE",)
LSTM_LEG_ID = "a1_plus3_LSTM_h20"
FUNDGROWTH_LEG_ID = "new_FUNDGROWTH_h20"
LSTM_ARM_ID = "LSTM_ONLY"
FUNDGROWTH_ARM_ID = "FUNDGROWTH_ONLY"
PARENT_ARM_ID = "IC_WEIGHTED_PARENT"
ARM_IDS = (LSTM_ARM_ID, FUNDGROWTH_ARM_ID, PARENT_ARM_ID)
PARENT_TERMINAL_WEIGHTS = {
    LSTM_LEG_ID: 0.6966591521,
    FUNDGROWTH_LEG_ID: 0.3033408479,
}


class AlphaAuditArmV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    arm_id: Literal["LSTM_ONLY", "FUNDGROWTH_ONLY", "IC_WEIGHTED_PARENT"]
    terminal_weights: dict[str, float]

    @model_validator(mode="after")
    def validate_frozen_arm(self) -> "AlphaAuditArmV1":
        expected = frozen_alpha_audit_arm_weights()[self.arm_id]
        if set(self.terminal_weights) != set(expected):
            raise ValueError(f"{self.arm_id} leg roster differs from the frozen arm")
        if any(abs(float(self.terminal_weights[key]) - value) > 1e-12 for key, value in expected.items()):
            raise ValueError(f"{self.arm_id} weights differ from the frozen arm")
        return self


class AdvisoryThreeArmAlphaAuditRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal[ALPHA_AUDIT_SCHEMA_VERSION] = ALPHA_AUDIT_SCHEMA_VERSION
    request_id: str = Field(pattern=r"^advalpha3req_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: Literal[ObjectiveContract.ALPHA_RANKING] = ObjectiveContract.ALPHA_RANKING
    study_type: Literal[ResearchStudyType.ORACLE_DIAGNOSTIC] = ResearchStudyType.ORACLE_DIAGNOSTIC
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    n0_completion_ref: EvidenceReferenceV1
    n0_completion_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    research_window_contract_ref: EvidenceReferenceV1
    research_window_contract_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_request_ref: EvidenceReferenceV1
    n1_request_sha256: str = Field(pattern=SHA256_PATTERN)
    n1_bundle_path: str = Field(min_length=1)
    n1_bundle_manifest_ref: EvidenceReferenceV1
    n1_bundle_id: str = Field(pattern=SHA256_PATTERN)
    registry_path: str = Field(min_length=1)
    program_id: str = Field(min_length=1)
    binding_version_id: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    selection_runtime_semantics_hash: str = Field(pattern=SHA256_PATTERN)
    baseline_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    shadow_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    cost_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    split_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    policy_dataset_bundle_id: Literal[N1_DATASET_IDENTITY] = N1_DATASET_IDENTITY
    pit_spans_sha256: str = Field(pattern=SHA256_PATTERN)
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    representative_seed_run_ids: dict[str, str]
    prediction_artifacts: dict[str, PredictionArtifactDescriptor]
    parent_terminal_weights: dict[str, float]
    arms: tuple[AlphaAuditArmV1, AlphaAuditArmV1, AlphaAuditArmV1]
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    decision_date_start: date = N1_DECISION_START
    decision_date_end: date = N1_DECISION_END
    data_cutoff: date = N1_DATA_CUTOFF
    window_id: Literal[N1_WINDOW_ID] = N1_WINDOW_ID
    dataset_identity: Literal[N1_DATASET_IDENTITY] = N1_DATASET_IDENTITY
    inference_policy: Tier1InferencePolicyV1 = Field(default_factory=Tier1InferencePolicyV1)
    resource_max_rss_bytes: Literal[N1_RESOURCE_LIMIT_BYTES] = N1_RESOURCE_LIMIT_BYTES

    @model_validator(mode="after")
    def validate_frozen_identity(self) -> "AdvisoryThreeArmAlphaAuditRequestV1":
        if (
            self.decision_date_start != N1_DECISION_START
            or self.decision_date_end != N1_DECISION_END
            or self.data_cutoff != N1_DATA_CUTOFF
        ):
            raise ValueError("alpha audit must use the exact N1 development dates")
        expected_leg_ids = {LSTM_LEG_ID, FUNDGROWTH_LEG_ID}
        if set(self.representative_seed_run_ids) != expected_leg_ids:
            raise ValueError("alpha audit requires the exact two parent legs")
        if set(self.prediction_artifacts) != set(self.representative_seed_run_ids.values()):
            raise ValueError("prediction descriptors differ from representative runs")
        if set(self.parent_terminal_weights) != expected_leg_ids or any(
            abs(float(self.parent_terminal_weights[key]) - value) > 1e-12
            for key, value in PARENT_TERMINAL_WEIGHTS.items()
        ):
            raise ValueError("parent terminal weights differ from the frozen package")
        if tuple(item.arm_id for item in self.arms) != ARM_IDS:
            raise ValueError("alpha audit arm roster/order must be the frozen three arms")
        if self.n1_request_ref.role != "n1_frozen_request":
            raise ValueError("N1 request evidence role is invalid")
        if self.n1_bundle_manifest_ref.role != "n1_formal_bundle_manifest":
            raise ValueError("N1 bundle evidence role is invalid")
        expected_manifest_uri = self.n1_bundle_path.rstrip("/") + "/manifest.json"
        if self.n1_bundle_manifest_ref.artifact_uri.replace("\\", "/") != expected_manifest_uri.replace("\\", "/"):
            raise ValueError("N1 bundle manifest evidence path differs from its bundle")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected:
            raise ValueError("alpha audit request_sha256 mismatch")
        if self.request_id != f"advalpha3req_{expected[:24]}":
            raise ValueError("alpha audit request_id mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at"},
        )


class AdvisoryThreeArmAlphaAuditReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_three_arm_alpha_audit_receipt_v1"] = "advisory_three_arm_alpha_audit_receipt_v1"
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    result_files_sha256: str = Field(pattern=SHA256_PATTERN)
    arm_ids: tuple[Literal["LSTM_ONLY", "FUNDGROWTH_ONLY", "IC_WEIGHTED_PARENT"], ...]
    decision_date_count: int = Field(gt=0)
    common_signal_row_count: int = Field(gt=0)
    evaluable_recall_day_count_by_arm: dict[str, int]
    evaluable_top5_day_count_by_arm: dict[str, int]
    planned_trial_count: Literal[0] = 0
    generated_trial_count: Literal[0] = 0
    evaluated_trial_count: Literal[0] = 0
    selected_trial_count: Literal[0] = 0
    result_class: Literal[ResearchResultClass.EXPLORATORY] = ResearchResultClass.EXPLORATORY
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    sealed_holdout_accessed: Literal[False] = False
    runtime_eligible: Literal[False] = False
    activated: Literal[False] = False
    created_at: datetime
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryThreeArmAlphaAuditReceiptV1":
        if self.arm_ids != ARM_IDS:
            raise ValueError("alpha audit receipt arm roster drifted")
        for field_name in (
            "evaluable_recall_day_count_by_arm",
            "evaluable_top5_day_count_by_arm",
        ):
            counts = getattr(self, field_name)
            if set(counts) != set(ARM_IDS) or any(value < 0 for value in counts.values()):
                raise ValueError(f"{field_name} must contain non-negative counts for the frozen arms")
        expected = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != expected:
            raise ValueError("alpha audit receipt_sha256 mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_sha256", "created_at"})


def frozen_alpha_audit_arm_weights() -> dict[str, dict[str, float]]:
    return {
        LSTM_ARM_ID: {LSTM_LEG_ID: 1.0},
        FUNDGROWTH_ARM_ID: {FUNDGROWTH_LEG_ID: 1.0},
        PARENT_ARM_ID: dict(PARENT_TERMINAL_WEIGHTS),
    }


def frozen_alpha_audit_arms() -> tuple[AlphaAuditArmV1, AlphaAuditArmV1, AlphaAuditArmV1]:
    weights = frozen_alpha_audit_arm_weights()
    return tuple(  # type: ignore[return-value]
        AlphaAuditArmV1(arm_id=arm_id, terminal_weights=weights[arm_id]) for arm_id in ARM_IDS
    )


def build_three_arm_alpha_audit_request(**values: Any) -> AdvisoryThreeArmAlphaAuditRequestV1:
    payload = dict(values)
    payload.setdefault("schema_version", ALPHA_AUDIT_SCHEMA_VERSION)
    payload.setdefault("created_at", datetime.now(timezone.utc))
    payload.setdefault("arms", frozen_alpha_audit_arms())
    if len(payload["arms"]) != len(ARM_IDS):
        raise ValueError("alpha audit requires exactly three frozen arms")
    functional_fields = set(AdvisoryThreeArmAlphaAuditRequestV1.model_fields) - {
        "request_id",
        "request_sha256",
        "created_at",
    }
    functional = {key: value for key, value in payload.items() if key in functional_fields}
    normalized = AdvisoryThreeArmAlphaAuditRequestV1.model_construct(
        request_id="advalpha3req_" + "0" * 24,
        request_sha256="0" * 64,
        created_at=payload["created_at"],
        **functional,
    ).model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})
    digest = canonical_json_sha256(normalized)
    payload["request_sha256"] = digest
    payload["request_id"] = f"advalpha3req_{digest[:24]}"
    return AdvisoryThreeArmAlphaAuditRequestV1.model_validate(payload)


def build_three_arm_alpha_audit_receipt(**values: Any) -> AdvisoryThreeArmAlphaAuditReceiptV1:
    payload = dict(values)
    payload.setdefault("schema_version", "advisory_three_arm_alpha_audit_receipt_v1")
    payload.setdefault("created_at", datetime.now(timezone.utc))
    functional_fields = set(AdvisoryThreeArmAlphaAuditReceiptV1.model_fields) - {
        "receipt_sha256",
        "created_at",
    }
    functional = {key: value for key, value in payload.items() if key in functional_fields}
    normalized = AdvisoryThreeArmAlphaAuditReceiptV1.model_construct(
        receipt_sha256="0" * 64,
        created_at=payload["created_at"],
        **functional,
    ).model_dump(mode="json", exclude={"receipt_sha256", "created_at"})
    payload["receipt_sha256"] = canonical_json_sha256(normalized)
    return AdvisoryThreeArmAlphaAuditReceiptV1.model_validate(payload)


__all__ = [
    "ALPHA_AUDIT_BUNDLE_SCHEMA",
    "ALPHA_AUDIT_EXPERIMENT_ID",
    "ALPHA_AUDIT_PARENT_LINEAGE",
    "ARM_IDS",
    "FUNDGROWTH_ARM_ID",
    "FUNDGROWTH_LEG_ID",
    "LSTM_ARM_ID",
    "LSTM_LEG_ID",
    "PARENT_ARM_ID",
    "PARENT_TERMINAL_WEIGHTS",
    "AdvisoryThreeArmAlphaAuditReceiptV1",
    "AdvisoryThreeArmAlphaAuditRequestV1",
    "AlphaAuditArmV1",
    "build_three_arm_alpha_audit_receipt",
    "build_three_arm_alpha_audit_request",
    "frozen_alpha_audit_arm_weights",
    "frozen_alpha_audit_arms",
]
