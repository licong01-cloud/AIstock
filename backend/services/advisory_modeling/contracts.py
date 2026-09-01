from __future__ import annotations

from decimal import Decimal
from enum import Enum
from statistics import median
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_modeling.identity import (
    FrozenModel,
    quantize_12,
    set_computed_hash,
    validated_hash,
)
from backend.services.advisory_modeling.errors import (
    AdvisoryModelingError,
    REASON_EXPERIMENT_REGISTRY_MISMATCH,
    REASON_SELECTION_NOT_UNIQUE,
)


TRAINING_CONFIG_SCHEMA_VERSION = "advisory_short_rebound_training_config_v1"
EXPERIMENT_REGISTRY_SCHEMA_VERSION = "advisory_short_rebound_experiment_registry_v1"
EXPERIMENT_RESULT_SCHEMA_VERSION = "advisory_short_rebound_experiment_result_v1"
MODEL_SELECTION_RECEIPT_SCHEMA_VERSION = "advisory_short_rebound_model_selection_receipt_v1"
PRIMARY_SEED = 20260710
DIAGNOSTIC_SEEDS = (20260711, 20260712)


class FeatureSet(str, Enum):
    CORE = "CORE"
    CORE_PLUS_HMM = "CORE_PLUS_HMM"
    CORE_PLUS_RISK = "CORE_PLUS_RISK"
    CORE_PLUS_HMM_PLUS_RISK = "CORE_PLUS_HMM_PLUS_RISK"


FEATURE_SET_ORDER = {
    FeatureSet.CORE: 0,
    FeatureSet.CORE_PLUS_HMM: 1,
    FeatureSet.CORE_PLUS_RISK: 2,
    FeatureSet.CORE_PLUS_HMM_PLUS_RISK: 3,
}


class CapabilityStatus(str, Enum):
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"
    RESEARCH_CANDIDATE_ELIGIBLE = "RESEARCH_CANDIDATE_ELIGIBLE"
    RESEARCH_BUNDLE_COMPLETE = "RESEARCH_BUNDLE_COMPLETE"
    RERANK_READY = "RERANK_READY"


class LightGbmTrainingConfigV1(FrozenModel):
    schema_version: Literal[TRAINING_CONFIG_SCHEMA_VERSION] = TRAINING_CONFIG_SCHEMA_VERSION
    objective: Literal["lambdarank"] = "lambdarank"
    metric: Literal["ndcg@5"] = "ndcg@5"
    num_leaves: Literal[31] = 31
    learning_rate: Decimal = Decimal("0.03")
    n_estimators: Literal[600] = 600
    min_data_in_leaf: Literal[80] = 80
    feature_fraction: Decimal = Decimal("0.8")
    bagging_fraction: Decimal = Decimal("0.8")
    bagging_freq: Literal[1] = 1
    lambda_l1: Decimal = Decimal("0.1")
    lambda_l2: Decimal = Decimal("1.0")
    early_stopping_rounds: Literal[80] = 80
    deterministic: Literal[True] = True
    force_col_wise: Literal[True] = True
    num_threads: Literal[1] = 1
    label_gain: tuple[int, ...] = (0, 1, 3, 7, 15)
    primary_seed: Literal[PRIMARY_SEED] = PRIMARY_SEED
    diagnostic_seeds: tuple[int, ...] = DIAGNOSTIC_SEEDS
    seed: Literal[PRIMARY_SEED] = PRIMARY_SEED
    feature_fraction_seed: Literal[PRIMARY_SEED] = PRIMARY_SEED
    bagging_seed: Literal[PRIMARY_SEED] = PRIMARY_SEED
    data_random_seed: Literal[PRIMARY_SEED] = PRIMARY_SEED
    training_config_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "LightGbmTrainingConfigV1":
        if self.diagnostic_seeds != DIAGNOSTIC_SEEDS or self.label_gain != (0, 1, 3, 7, 15):
            raise ValueError("training seeds and label gains are frozen")
        set_computed_hash(
            self,
            field_name="training_config_hash",
            exclude={"training_config_hash"},
        )
        return self


class ExperimentDefinitionV1(FrozenModel):
    candidate_experiment_id: str = Field(min_length=1, max_length=160)
    training_window_years: Literal[2, 3, 5]
    feature_set: FeatureSet
    training_config_hash: str = Field(min_length=64, max_length=64)
    experiment_definition_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("training_config_hash", "experiment_definition_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "ExperimentDefinitionV1":
        expected_id = f"short-rebound-{self.training_window_years}y-{self.feature_set.value.lower()}"
        if self.candidate_experiment_id != expected_id:
            raise ValueError("candidate_experiment_id must use the canonical deterministic identity")
        set_computed_hash(
            self,
            field_name="experiment_definition_hash",
            exclude={"experiment_definition_hash"},
        )
        return self


class ExperimentRegistryV1(FrozenModel):
    schema_version: Literal[EXPERIMENT_REGISTRY_SCHEMA_VERSION] = (
        EXPERIMENT_REGISTRY_SCHEMA_VERSION
    )
    training_config: LightGbmTrainingConfigV1
    candidates: tuple[ExperimentDefinitionV1, ...]
    registry_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "ExperimentRegistryV1":
        expected = _experiment_candidates(str(self.training_config.training_config_hash))
        if self.candidates != expected:
            raise ValueError("experiment registry must contain the frozen 12 candidates")
        set_computed_hash(self, field_name="registry_hash", exclude={"registry_hash"})
        return self


def _experiment_candidates(training_config_hash: str) -> tuple[ExperimentDefinitionV1, ...]:
    return tuple(
        ExperimentDefinitionV1(
            candidate_experiment_id=f"short-rebound-{years}y-{feature_set.value.lower()}",
            training_window_years=years,
            feature_set=feature_set,
            training_config_hash=training_config_hash,
        )
        for years in (2, 3, 5)
        for feature_set in FeatureSet
    )


def frozen_experiment_registry_v1() -> ExperimentRegistryV1:
    config = LightGbmTrainingConfigV1()
    return ExperimentRegistryV1(
        training_config=config,
        candidates=_experiment_candidates(str(config.training_config_hash)),
    )


class ExperimentResultV1(FrozenModel):
    schema_version: Literal[EXPERIMENT_RESULT_SCHEMA_VERSION] = EXPERIMENT_RESULT_SCHEMA_VERSION
    candidate_experiment_id: str
    training_window_years: Literal[2, 3, 5]
    feature_set: FeatureSet
    completed_fold_count: int = Field(ge=0, le=5)
    modelable_coverage: Decimal = Field(ge=0, le=1)
    contract_error_count: int = Field(ge=0)
    completed_seed_set: tuple[int, ...]
    ndcg_at_5_uplift_lower_bound_95: Decimal
    mean_net_excess_return_5_uplift: Decimal
    executable_mae_loss_ratio: Decimal
    turnover_uplift: Decimal
    primary_fold_best_iterations: tuple[int, ...]
    reason_codes: tuple[str, ...] = ()
    result_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("result_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validated_hash(value, field_name="result_hash")

    @model_validator(mode="after")
    def _identity(self) -> "ExperimentResultV1":
        expected_id = f"short-rebound-{self.training_window_years}y-{self.feature_set.value.lower()}"
        if self.candidate_experiment_id != expected_id:
            raise ValueError("experiment result identity differs from its registered configuration")
        canonical_seeds = (PRIMARY_SEED, *DIAGNOSTIC_SEEDS)
        if self.completed_seed_set != tuple(
            seed for seed in canonical_seeds if seed in self.completed_seed_set
        ) or any(seed not in canonical_seeds for seed in self.completed_seed_set):
            raise ValueError("completed_seed_set must be a canonical ordered subset of registered seeds")
        if any(value < 1 or value > 600 for value in self.primary_fold_best_iterations):
            raise ValueError("best iterations must be within [1, 600]")
        if len(self.primary_fold_best_iterations) > 5:
            raise ValueError("primary fold best iterations cannot exceed five folds")
        if PRIMARY_SEED in self.completed_seed_set:
            if len(self.primary_fold_best_iterations) != self.completed_fold_count:
                raise ValueError("primary fold count differs from best-iteration evidence")
        elif self.completed_fold_count or self.primary_fold_best_iterations:
            raise ValueError("primary fold evidence cannot exist before the primary seed completes")
        incomplete = (
            self.completed_fold_count < 5
            or self.modelable_coverage < Decimal("0.95")
            or self.contract_error_count > 0
            or self.completed_seed_set != canonical_seeds
            or len(self.primary_fold_best_iterations) < 5
        )
        if incomplete and not self.reason_codes:
            raise ValueError("incomplete experiment result requires explicit reason codes")
        set_computed_hash(self, field_name="result_hash", exclude={"result_hash"})
        return self

    @property
    def research_candidate_eligible(self) -> bool:
        return (
            self.completed_fold_count == 5
            and self.modelable_coverage >= Decimal("0.95")
            and self.contract_error_count == 0
            and self.completed_seed_set == (PRIMARY_SEED, *DIAGNOSTIC_SEEDS)
            and len(self.primary_fold_best_iterations) == 5
        )


class ModelSelectionReceiptV1(FrozenModel):
    schema_version: Literal[MODEL_SELECTION_RECEIPT_SCHEMA_VERSION] = (
        MODEL_SELECTION_RECEIPT_SCHEMA_VERSION
    )
    experiment_registry_hash: str = Field(min_length=64, max_length=64)
    eligible_result_set_hash: str = Field(min_length=64, max_length=64)
    selected_experiment_id: str
    selected_result_hash: str = Field(min_length=64, max_length=64)
    selection_key: tuple[str | int, ...]
    final_n_estimators: int = Field(ge=1, le=600)
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "experiment_registry_hash", "eligible_result_set_hash", "selected_result_hash", "receipt_hash"
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "ModelSelectionReceiptV1":
        set_computed_hash(self, field_name="receipt_hash", exclude={"receipt_hash"})
        return self


def _selection_key(result: ExperimentResultV1) -> tuple[Decimal | int | str, ...]:
    return (
        -quantize_12(result.ndcg_at_5_uplift_lower_bound_95),
        -quantize_12(result.mean_net_excess_return_5_uplift),
        quantize_12(result.executable_mae_loss_ratio),
        quantize_12(result.turnover_uplift),
        FEATURE_SET_ORDER[result.feature_set],
        result.training_window_years,
        result.candidate_experiment_id,
    )


def select_research_configuration(
    *,
    registry: ExperimentRegistryV1,
    results: tuple[ExperimentResultV1, ...],
) -> ModelSelectionReceiptV1:
    registered = {item.candidate_experiment_id: item for item in registry.candidates}
    if len({result.candidate_experiment_id for result in results}) != len(results):
        raise AdvisoryModelingError(
            REASON_EXPERIMENT_REGISTRY_MISMATCH,
            "experiment results contain duplicate candidate identities",
        )
    for result in results:
        definition = registered.get(result.candidate_experiment_id)
        if definition is None:
            raise AdvisoryModelingError(
                REASON_EXPERIMENT_REGISTRY_MISMATCH,
                "experiment result is not preregistered",
            )
        if (
            definition.training_window_years != result.training_window_years
            or definition.feature_set != result.feature_set
        ):
            raise AdvisoryModelingError(
                REASON_EXPERIMENT_REGISTRY_MISMATCH,
                "experiment result semantics differ from registry",
            )
    if set(registered) != {result.candidate_experiment_id for result in results}:
        raise AdvisoryModelingError(
            REASON_EXPERIMENT_REGISTRY_MISMATCH,
            "experiment results must cover all 12 preregistered candidates",
        )
    eligible = tuple(result for result in results if result.research_candidate_eligible)
    if not eligible:
        raise AdvisoryModelingError(
            REASON_SELECTION_NOT_UNIQUE,
            "no research-eligible model configuration",
        )
    selected = min(eligible, key=_selection_key)
    selected_key = _selection_key(selected)
    final_n_estimators = int(median(sorted(selected.primary_fold_best_iterations)))
    serialized_key = tuple(str(item) if isinstance(item, Decimal) else item for item in selected_key)
    return ModelSelectionReceiptV1(
        experiment_registry_hash=str(registry.registry_hash),
        eligible_result_set_hash=canonical_json_sha256(
            tuple(sorted(str(item.result_hash) for item in eligible))
        ),
        selected_experiment_id=selected.candidate_experiment_id,
        selected_result_hash=str(selected.result_hash),
        selection_key=serialized_key,
        final_n_estimators=final_n_estimators,
    )
