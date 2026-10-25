"""Independent thirteen-field identity, never a relaxed legacy nine-field scope."""
from typing import Literal

from pydantic import Field, model_validator

from backend.services.advisory_model_first.economic_daily_information_v1 import FEATURES, SEMANTICS
from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryTrainingRequestV3
from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES, SHA256
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EconomicModelScopeV2, FrozenContract
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

INFORMATION_FEATURE_NAMES = ECONOMIC_FEATURE_NAMES + FEATURES
INFORMATION_SEMANTICS_SHA256 = sha(SEMANTICS)


class EconomicEntryInformationScopeV4(FrozenContract):
    schema_version: Literal["economic_entry_information_scope_v4"] = "economic_entry_information_scope_v4"
    model_family: Literal["ALIGNED_ENTRY_INFORMATION_V4"] = "ALIGNED_ENTRY_INFORMATION_V4"
    parent_scope: EconomicModelScopeV2
    feature_names: tuple[str, ...] = INFORMATION_FEATURE_NAMES
    information_semantics_sha256: str = INFORMATION_SEMANTICS_SHA256
    training_information_sha256: str = Field(pattern=SHA256)
    feature_semantics_qualification: Literal["DEVELOPMENT_ONLY_TEMPORAL_PARITY_UNPROVEN"] = "DEVELOPMENT_ONLY_TEMPORAL_PARITY_UNPROVEN"
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_information(self):
        if self.feature_names != INFORMATION_FEATURE_NAMES or self.information_semantics_sha256 != INFORMATION_SEMANTICS_SHA256:
            raise ValueError("v4 has one fixed feature order and computation identity")
        return self

    @property
    def scope_sha256(self):
        return sha(self.model_dump(mode="json"))


class EconomicEntryInformationTrainingRequestV4(FrozenContract):
    schema_version: Literal["economic_entry_information_training_request_v4"] = "economic_entry_information_training_request_v4"
    parent_request: AlignedEntryTrainingRequestV3
    information_ref: EvidenceReferenceV1
    information_rows_sha256: str = Field(pattern=SHA256)
    common_fit_rows_sha256: str = Field(pattern=SHA256)
    implementation_sha256: str = Field(pattern=SHA256)
    information_semantics_sha256: str = INFORMATION_SEMANTICS_SHA256
    feature_names: tuple[str, ...] = INFORMATION_FEATURE_NAMES
    model_configuration_count: Literal[2] = 2
    fitted_head_count: Literal[4] = 4
    economic_candidate_count: Literal[1] = 1
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_fixed_information(self):
        if self.feature_names != INFORMATION_FEATURE_NAMES or self.information_semantics_sha256 != INFORMATION_SEMANTICS_SHA256:
            raise ValueError("v4 cannot search information schema or reorder features")
        if self.information_ref.role != "entry_information_v4_manifest":
            raise ValueError("v4 requires its exact information manifest evidence role")
        return self

    @property
    def request_sha256(self):
        return sha(self.model_dump(mode="json"))


class EconomicEntryInformationStudyPlanV4(FrozenContract):
    schema_version: Literal["economic_entry_information_study_plan_v4"] = "economic_entry_information_study_plan_v4"
    parent_plan_ref: EvidenceReferenceV1
    parent_trained_manifest_ref: EvidenceReferenceV1
    training_request: EconomicEntryInformationTrainingRequestV4
    model_scope: EconomicEntryInformationScopeV4
    simulator_sha256: str = Field(pattern=SHA256)
    parent_lineage: tuple[str, ...] = Field(min_length=2)
    expected_train_rows: int = Field(ge=30)
    expected_validation_rows: int = Field(gt=0)
    development_power: dict
    bootstrap_block_days: Literal[5] = 5
    bootstrap_repetitions: Literal[2000] = 2000
    bootstrap_seed: Literal[20261002] = 20261002
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_scope(self):
        if self.model_scope.training_information_sha256!=self.training_request.information_rows_sha256:
            raise ValueError("v4 model scope must bind its exact training information")
        return self

    @property
    def plan_sha256(self):
        return sha(self.model_dump(mode="json"))

    @property
    def experiment_id(self):
        return f"advinfo_{self.plan_sha256[:24]}"
