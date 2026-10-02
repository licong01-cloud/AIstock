"""New fit identity for a common economically valid supervision cohort."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryTrainingRequestV1, SHA256
from backend.services.advisory_model_first.economic_risk_alignment_contracts import FrozenContract
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


class AlignedEntryTrainingRequestV3(FrozenContract):
    schema_version: Literal["aligned_entry_training_request_v3"] = "aligned_entry_training_request_v3"
    source_request: EconomicEntryTrainingRequestV1
    risk_labels_content_sha256: str = Field(pattern=SHA256)
    common_fit_rows_sha256: str = Field(pattern=SHA256)
    implementation_sha256: str = Field(pattern=SHA256)
    lightgbm_version: str = Field(min_length=3)
    return_training_mode: Literal["REFIT_COMMON_ELIGIBLE"] = "REFIT_COMMON_ELIGIBLE"
    risk_metric: Literal["entry_net_max_loss_open_close_bps"] = "entry_net_max_loss_open_close_bps"
    risk_quantile: Literal[0.9] = 0.9
    risk_reference_bps: Literal[800] = 800
    intended_model_trial_count: Literal[1] = 1
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_source_budget(self):
        if self.source_request.downside_budget_bps != 800:
            raise ValueError("v3 preserves the fixed original research reference")
        return self

    @property
    def training_input_sha256(self) -> str:
        return canonical_json_sha256({
            "original_input_identity_sha256": self.source_request.input_identity_sha256,
            "feature_source_sha256": self.source_request.feature_source_sha256,
            "risk_labels_content_sha256": self.risk_labels_content_sha256,
            "common_fit_rows_sha256": self.common_fit_rows_sha256, "risk_metric": self.risk_metric,
        })

    @property
    def request_sha256(self) -> str:
        return canonical_json_sha256({**self.model_dump(mode="json"), "parameters": self.source_request.effective_parameters})


class AlignedEntryStudyPlanV3(FrozenContract):
    schema_version: Literal["aligned_entry_study_plan_v3"] = "aligned_entry_study_plan_v3"
    v2_plan_ref: EvidenceReferenceV1
    v2_prepared_manifest_ref: EvidenceReferenceV1
    training_request: AlignedEntryTrainingRequestV3
    simulator_sha256: str = Field(pattern=SHA256)
    parent_lineage: tuple[str, ...] = Field(min_length=2)
    expected_train_rows: int = Field(gt=0)
    expected_validation_rows: int = Field(gt=0)
    bootstrap_block_days: Literal[5] = 5
    bootstrap_repetitions: Literal[2000] = 2000
    bootstrap_seed: Literal[20261002] = 20261002
    rule_minimum_gap_bps: Literal[-300] = -300
    rule_maximum_gap_bps: Literal[300] = 300
    unknown_policy: Literal["RESEARCH_BASELINE_CONTROL_NOT_MODEL_TAKE"] = "RESEARCH_BASELINE_CONTROL_NOT_MODEL_TAKE"
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False

    @property
    def plan_sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))

    @property
    def experiment_id(self) -> str:
        return f"advaligned_{self.plan_sha256[:24]}"
