from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.meta_label_contracts import (
    MetaLabelFamilySpecV1,
    MetaLabelOutcomeWeightingV1,
)

HMM_FEATURES = {
    "hmm_bull_posterior",
    "hmm_state",
    "hmm_state_duration",
    "hmm_observation_completeness",
}
HMM_FEATURES |= {f"{column}__missing" for column in tuple(HMM_FEATURES)}


def meta_label_feature_names(family: MetaLabelFamilySpecV1) -> tuple[str, ...]:
    return tuple(column for column in MODEL_FEATURE_COLUMNS if family.include_hmm or column not in HMM_FEATURES)


@dataclass(frozen=True)
class MetaLabelTrainingResult:
    booster: Any
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    best_iteration: int
    evaluation_history: dict[str, Any]
    validation_predictions: pd.DataFrame
    metrics: dict[str, Any]
    outcome_weighting_receipt: dict[str, Any] | None


@dataclass(frozen=True)
class FinalMetaLabelTrainingResult:
    booster: Any
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    boost_rounds: int
    outcome_weighting_receipt: dict[str, Any] | None


@dataclass(frozen=True)
class MetaLabelOutcomeWeightFit:
    scale_bps: float
    normalization_divisor: float


def fit_meta_label_outcome_weights(
    returns: pd.Series | np.ndarray,
    *,
    specification: MetaLabelOutcomeWeightingV1,
) -> tuple[np.ndarray, MetaLabelOutcomeWeightFit]:
    values = _finite_outcome_values(returns)
    scale_bps = float(np.median(np.abs(values)))
    if not np.isfinite(scale_bps) or scale_bps <= 0.0:
        raise AdvisoryModelFirstError(
            "meta-label outcome weighting scale is invalid",
            reason_code="ADVISORY_META_LABEL_OUTCOME_WEIGHTING_INVALID",
            context={"scale_bps": scale_bps},
        )
    raw = _raw_outcome_weights(values, specification=specification, scale_bps=scale_bps)
    normalization_divisor = float(raw.mean())
    if not np.isfinite(normalization_divisor) or normalization_divisor <= 0.0:
        raise AdvisoryModelFirstError(
            "meta-label outcome weighting normalization is invalid",
            reason_code="ADVISORY_META_LABEL_OUTCOME_WEIGHTING_INVALID",
        )
    fit = MetaLabelOutcomeWeightFit(
        scale_bps=scale_bps,
        normalization_divisor=normalization_divisor,
    )
    return raw / normalization_divisor, fit


def apply_meta_label_outcome_weights(
    returns: pd.Series | np.ndarray,
    *,
    specification: MetaLabelOutcomeWeightingV1,
    fit: MetaLabelOutcomeWeightFit,
) -> np.ndarray:
    if (
        not np.isfinite(fit.scale_bps)
        or fit.scale_bps <= 0.0
        or not np.isfinite(fit.normalization_divisor)
        or fit.normalization_divisor <= 0.0
    ):
        raise AdvisoryModelFirstError(
            "meta-label outcome weighting fit is invalid",
            reason_code="ADVISORY_META_LABEL_OUTCOME_WEIGHTING_INVALID",
        )
    values = _finite_outcome_values(returns)
    weights = (
        _raw_outcome_weights(
            values,
            specification=specification,
            scale_bps=fit.scale_bps,
        )
        / fit.normalization_divisor
    )
    if not np.isfinite(weights).all() or (weights <= 0.0).any():
        raise AdvisoryModelFirstError(
            "meta-label outcome weights are invalid",
            reason_code="ADVISORY_META_LABEL_OUTCOME_WEIGHTING_INVALID",
        )
    return weights


def train_meta_label_trial(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    validation_dates: Sequence[pd.Timestamp],
    family: MetaLabelFamilySpecV1,
    seed: int,
    outcome_weighting: MetaLabelOutcomeWeightingV1 | None = None,
) -> MetaLabelTrainingResult:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    merged = features.merge(labels, on=keys, how="inner", validate="one_to_one", suffixes=("", "_label"))
    merged["decision_as_of_trade_date"] = pd.to_datetime(merged["decision_as_of_trade_date"]).dt.normalize()
    train_set = set(pd.DatetimeIndex(pd.to_datetime(list(train_dates))).normalize())
    validation_set = set(pd.DatetimeIndex(pd.to_datetime(list(validation_dates))).normalize())
    train_mask = merged["decision_as_of_trade_date"].isin(train_set) & (merged["label_status"] == "MATURED")
    validation_mask = merged["decision_as_of_trade_date"].isin(validation_set) & (merged["label_status"] == "MATURED")
    if not train_mask.any() or not validation_mask.any():
        raise AdvisoryModelFirstError(
            "meta-label path has no train or validation rows",
            reason_code="ADVISORY_META_LABEL_PATH_NOT_COMPUTABLE",
        )
    validation_group_sizes = merged.loc[validation_mask].groupby("decision_as_of_trade_date").size()
    if validation_group_sizes.empty or int(validation_group_sizes.min()) < 5:
        raise AdvisoryModelFirstError(
            "meta-label validation date has fewer than five modelable candidates",
            reason_code="ADVISORY_META_LABEL_PATH_NOT_COMPUTABLE",
            context={
                "minimum_modelable_candidates": int(validation_group_sizes.min()) if len(validation_group_sizes) else 0
            },
        )
    if merged.loc[train_mask, "take_label"].nunique() < 2 or merged.loc[validation_mask, "take_label"].nunique() < 2:
        raise AdvisoryModelFirstError(
            "meta-label path contains a single class",
            reason_code="ADVISORY_META_LABEL_PATH_NOT_COMPUTABLE",
        )
    feature_names = meta_label_feature_names(family)
    missing = sorted(set(feature_names) - set(merged))
    if missing:
        raise AdvisoryModelFirstError(
            "meta-label feature matrix is incomplete",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": missing},
        )
    matrix = merged.loc[:, feature_names].copy()
    vocabulary: dict[str, tuple[int, ...]] = {}
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    required = [column for column in feature_names if not column.endswith("__missing")]
    all_null = [column for column in required if matrix.loc[train_mask, column].isna().all()]
    if all_null:
        raise AdvisoryModelFirstError(
            "meta-label train features are entirely missing",
            reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
            context={"features": all_null},
        )
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in matrix:
            continue
        categories = tuple(sorted(matrix.loc[train_mask, column].dropna().astype(int).unique()))
        if not categories:
            raise AdvisoryModelFirstError(
                "meta-label categorical feature has no train vocabulary",
                reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
                context={"feature": column},
            )
        vocabulary[column] = categories
        matrix[column] = pd.Categorical(matrix[column], categories=categories)
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable in WSL",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        ) from exc
    train_weights: np.ndarray | None = None
    validation_weights: np.ndarray | None = None
    weighting_fit: MetaLabelOutcomeWeightFit | None = None
    weighting_receipt: dict[str, Any] | None = None
    if outcome_weighting is not None:
        train_weights, weighting_fit = fit_meta_label_outcome_weights(
            merged.loc[train_mask, "net_excess_return_bps"],
            specification=outcome_weighting,
        )
        validation_weights = apply_meta_label_outcome_weights(
            merged.loc[validation_mask, "net_excess_return_bps"],
            specification=outcome_weighting,
            fit=weighting_fit,
        )
        weighting_receipt = _outcome_weighting_receipt(
            outcome_weighting,
            weighting_fit,
            train_weights=train_weights,
            validation_weights=validation_weights,
        )
    train_data = lgb.Dataset(
        matrix.loc[train_mask],
        label=merged.loc[train_mask, "take_label"].astype(int),
        weight=train_weights,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        free_raw_data=False,
    )
    validation_data = lgb.Dataset(
        matrix.loc[validation_mask],
        label=merged.loc[validation_mask, "take_label"].astype(int),
        weight=validation_weights,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        reference=train_data,
        free_raw_data=False,
    )
    params = {
        "objective": "binary",
        "metric": ["binary_logloss", "auc"],
        "num_leaves": family.num_leaves,
        "learning_rate": family.learning_rate,
        "min_data_in_leaf": family.min_data_in_leaf,
        "feature_fraction": family.feature_fraction,
        "bagging_fraction": family.bagging_fraction,
        "bagging_freq": family.bagging_freq,
        "lambda_l1": family.lambda_l1,
        "lambda_l2": family.lambda_l2,
        "deterministic": True,
        "force_col_wise": True,
        "num_threads": family.num_threads,
        "seed": seed,
        "feature_fraction_seed": seed,
        "bagging_seed": seed,
        "data_random_seed": seed,
        "verbosity": -1,
    }
    history: dict[str, Any] = {}
    booster = lgb.train(
        params,
        train_data,
        num_boost_round=family.max_boost_rounds,
        valid_sets=[validation_data],
        valid_names=["validation"],
        callbacks=[
            lgb.early_stopping(family.early_stopping_rounds, verbose=False),
            lgb.record_evaluation(history),
        ],
    )
    validation = merged.loc[
        validation_mask, [*keys, "selection_effective_rank", "take_label", "net_excess_return_bps"]
    ].copy()
    probabilities = booster.predict(matrix.loc[validation_mask], num_iteration=booster.best_iteration)
    if not np.isfinite(probabilities).all() or ((probabilities < 0) | (probabilities > 1)).any():
        raise AdvisoryModelFirstError(
            "meta-label model returned invalid probabilities",
            reason_code="ADVISORY_MODEL_TRAINING_FAILED",
        )
    validation["take_probability"] = probabilities
    validation["skip_probability"] = 1.0 - probabilities
    validation["advisory_model_confidence"] = np.abs(probabilities - 0.5) * 2.0
    validation = validation.sort_values(
        ["decision_as_of_trade_date", "take_probability", "selection_effective_rank", "instrument"],
        ascending=[True, False, True, True],
    )
    validation["entry_priority_rank"] = validation.groupby("decision_as_of_trade_date").cumcount().add(1)
    truth = validation["take_label"].astype(int).to_numpy()
    predicted = (probabilities >= 0.5).astype(int)
    metrics = {
        "roc_auc": float(roc_auc_score(truth, probabilities)),
        "pr_auc": float(average_precision_score(truth, probabilities)),
        "brier": float(brier_score_loss(truth, probabilities)),
        "log_loss": float(log_loss(truth, probabilities, labels=[0, 1])),
        "accuracy": float(accuracy_score(truth, predicted)),
        "precision": float(precision_score(truth, predicted, zero_division=0)),
        "recall": float(recall_score(truth, predicted, zero_division=0)),
        "train_row_count": int(train_mask.sum()),
        "validation_row_count": int(validation_mask.sum()),
        "best_iteration": int(booster.best_iteration),
    }
    if weighting_receipt is not None:
        metrics.update(
            {
                "outcome_weighting_mode": weighting_receipt["mode"],
                "outcome_weight_scale_bps": weighting_receipt["scale_bps"],
                "outcome_weight_normalization_divisor": weighting_receipt["normalization_divisor"],
                "train_weight_min": weighting_receipt["train_weight_min"],
                "train_weight_max": weighting_receipt["train_weight_max"],
                "train_weight_mean": weighting_receipt["train_weight_mean"],
                "validation_weight_min": weighting_receipt["validation_weight_min"],
                "validation_weight_max": weighting_receipt["validation_weight_max"],
                "validation_weight_mean": weighting_receipt["validation_weight_mean"],
            }
        )
    return MetaLabelTrainingResult(
        booster=booster,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        best_iteration=int(booster.best_iteration),
        evaluation_history=history,
        validation_predictions=validation.reset_index(drop=True),
        metrics=metrics,
        outcome_weighting_receipt=weighting_receipt,
    )


def train_final_meta_label(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    family: MetaLabelFamilySpecV1,
    seed: int,
    boost_rounds: int,
    outcome_weighting: MetaLabelOutcomeWeightingV1 | None = None,
) -> FinalMetaLabelTrainingResult:
    if boost_rounds < 1 or boost_rounds > family.max_boost_rounds:
        raise ValueError("final boost_rounds is outside the approved family range")
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    merged = features.merge(labels, on=keys, how="inner", validate="one_to_one", suffixes=("", "_label"))
    merged = merged[merged["label_status"] == "MATURED"].copy()
    if merged.empty or merged["take_label"].nunique() < 2:
        raise AdvisoryModelFirstError(
            "final meta-label training has no two-class mature rows",
            reason_code="ADVISORY_MODEL_TRAINING_FAILED",
        )
    feature_names = meta_label_feature_names(family)
    matrix = merged.loc[:, feature_names].copy()
    vocabulary: dict[str, tuple[int, ...]] = {}
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in matrix:
            continue
        categories = tuple(sorted(matrix[column].dropna().astype(int).unique()))
        if not categories:
            raise AdvisoryModelFirstError(
                "final categorical vocabulary is empty",
                reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
                context={"feature": column},
            )
        vocabulary[column] = categories
        matrix[column] = pd.Categorical(matrix[column], categories=categories)
    import lightgbm as lgb

    weights: np.ndarray | None = None
    weighting_receipt: dict[str, Any] | None = None
    if outcome_weighting is not None:
        weights, fit = fit_meta_label_outcome_weights(
            merged["net_excess_return_bps"],
            specification=outcome_weighting,
        )
        weighting_receipt = _outcome_weighting_receipt(
            outcome_weighting,
            fit,
            train_weights=weights,
            validation_weights=None,
        )
    data = lgb.Dataset(
        matrix,
        label=merged["take_label"].astype(int),
        weight=weights,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        free_raw_data=False,
    )
    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "num_leaves": family.num_leaves,
        "learning_rate": family.learning_rate,
        "min_data_in_leaf": family.min_data_in_leaf,
        "feature_fraction": family.feature_fraction,
        "bagging_fraction": family.bagging_fraction,
        "bagging_freq": family.bagging_freq,
        "lambda_l1": family.lambda_l1,
        "lambda_l2": family.lambda_l2,
        "deterministic": True,
        "force_col_wise": True,
        "num_threads": family.num_threads,
        "seed": seed,
        "feature_fraction_seed": seed,
        "bagging_seed": seed,
        "data_random_seed": seed,
        "verbosity": -1,
    }
    booster = lgb.train(params, data, num_boost_round=boost_rounds)
    return FinalMetaLabelTrainingResult(
        booster=booster,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        boost_rounds=boost_rounds,
        outcome_weighting_receipt=weighting_receipt,
    )


def _finite_outcome_values(returns: pd.Series | np.ndarray) -> np.ndarray:
    values = np.asarray(pd.to_numeric(returns, errors="coerce"), dtype=float)
    if values.ndim != 1 or values.size == 0 or not np.isfinite(values).all():
        raise AdvisoryModelFirstError(
            "meta-label outcome weighting requires finite non-empty returns",
            reason_code="ADVISORY_META_LABEL_OUTCOME_WEIGHTING_INVALID",
        )
    return values


def _raw_outcome_weights(
    values: np.ndarray,
    *,
    specification: MetaLabelOutcomeWeightingV1,
    scale_bps: float,
) -> np.ndarray:
    relative = np.clip(
        np.abs(values) / scale_bps,
        0.0,
        specification.relative_cap,
    )
    raw = specification.base_weight + relative
    if not np.isfinite(raw).all() or (raw <= 0.0).any():
        raise AdvisoryModelFirstError(
            "meta-label outcome weighting produced invalid raw weights",
            reason_code="ADVISORY_META_LABEL_OUTCOME_WEIGHTING_INVALID",
        )
    return raw


def _outcome_weighting_receipt(
    specification: MetaLabelOutcomeWeightingV1,
    fit: MetaLabelOutcomeWeightFit,
    *,
    train_weights: np.ndarray,
    validation_weights: np.ndarray | None,
) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "schema_version": specification.schema_version,
        "mode": specification.mode,
        "scale_statistic": specification.scale_statistic,
        "scale_bps": fit.scale_bps,
        "normalization": specification.normalization,
        "normalization_divisor": fit.normalization_divisor,
        "train_weight_min": float(train_weights.min()),
        "train_weight_max": float(train_weights.max()),
        "train_weight_mean": float(train_weights.mean()),
    }
    if validation_weights is not None:
        receipt.update(
            {
                "validation_weight_min": float(validation_weights.min()),
                "validation_weight_max": float(validation_weights.max()),
                "validation_weight_mean": float(validation_weights.mean()),
            }
        )
    return receipt
