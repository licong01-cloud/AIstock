"""Distinct same-core 13/15-column H-TIMING contracts, never legacy weights."""
from typing import Literal

from pydantic import Field, model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_timing_features_v1 import TIMING_FEATURES
from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryTrainingRequestV3
from backend.services.advisory_model_first.economic_entry_contracts import SHA256
from backend.services.advisory_model_first.economic_risk_alignment_contracts import FrozenContract
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

CONTROL_NAMES = (*D_FEATURES, "query_gap_bps")
CANDIDATE_NAMES = (*D_FEATURES, *TIMING_FEATURES, "query_gap_bps")
ARMS = {"CORE_THIRTEEN": CONTROL_NAMES, "TIMING_FIFTEEN": CANDIDATE_NAMES}


class TimingTrainingRequestV1(FrozenContract):
    schema_version: Literal["economic_entry_timing_training_request_v1"] = "economic_entry_timing_training_request_v1"
    # Frozen parent supplies labels, original identity and split, not new features.
    parent_request: AlignedEntryTrainingRequestV3
    input_ref: EvidenceReferenceV1
    input_rows_sha256: str = Field(pattern=SHA256)
    recipe_sha256: str = Field(pattern=SHA256)
    common_fit_rows_sha256: str = Field(pattern=SHA256)
    implementation_sha256: str = Field(pattern=SHA256)
    control_names: tuple[str, ...] = CONTROL_NAMES
    candidate_names: tuple[str, ...] = CANDIDATE_NAMES
    model_configuration_count: Literal[2] = 2
    fitted_head_count: Literal[4] = 4
    economic_candidate_count: Literal[1] = 1
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def fixed_schema(self):
        if self.control_names != CONTROL_NAMES or self.candidate_names != CANDIDATE_NAMES:
            raise ValueError("timing requires new 13/15 orders with query last")
        if self.input_ref.role != "timing_input_manifest_v1":
            raise ValueError("timing requires its exact input manifest")
        return self

    @property
    def request_sha256(self):
        return sha(self.model_dump(mode="json"))


class TimingStudyPlanV1(FrozenContract):
    schema_version: Literal["economic_entry_timing_study_plan_v1"] = "economic_entry_timing_study_plan_v1"
    model_family: Literal["ECONOMIC_ENTRY_TIMING_V1"] = "ECONOMIC_ENTRY_TIMING_V1"
    parent_plan_ref: EvidenceReferenceV1
    training_request: TimingTrainingRequestV1
    scope: dict
    simulator_sha256: str = Field(pattern=SHA256)
    parent_lineage: tuple[str, ...] = Field(min_length=2)
    expected_train_rows: int = Field(ge=30)
    expected_validation_rows: int = Field(gt=0)
    bootstrap_block_days: Literal[5] = 5
    bootstrap_repetitions: Literal[2000] = 2000
    bootstrap_seed: Literal[20261002] = 20261002
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False

    @property
    def plan_sha256(self):
        return sha(self.model_dump(mode="json"))

    @property
    def experiment_id(self):
        return "advtiming_" + self.plan_sha256[:24]
