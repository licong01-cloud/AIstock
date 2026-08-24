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
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    recall_score,
    roc_auc_score,
)

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v2 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
    REQUIRED_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.meta_label_contracts import (
    approved_meta_label_outcome_weighting,
)
from backend.services.advisory_model_first.meta_label_training import (
    HMM_FEATURES,
    apply_meta_label_outcome_weights,
    fit_meta_label_outcome_weights,
)
from backend.services.advisory_model_first.policy_utility_contracts import (
    PolicyUtilityArmSpecV2,
    PolicyUtilityFamilySpecV1,
    approved_policy_utility_arms,
)


@dataclass(frozen=True)
class PolicyUtilityTransformFit:
    location_bps: float
    scale_bps: float


@dataclass(frozen=True)
class PolicyUtilityTrainingResult:
    booster: Any
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    best_iteration: int
    evaluation_history: dict[str, Any]
    validation_predictions: pd.DataFrame
    metrics: dict[str, Any]
    transform: PolicyUtilityTransformFit | None
    outcome_weighting_receipt: dict[str, float] | None


@dataclass(frozen=True)
class FinalPolicyUtilityTrainingResult:
    booster: Any
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    boost_rounds: int
    transform: PolicyUtilityTransformFit | None
    arm_id: str


def policy_utility_feature_names(family: PolicyUtilityFamilySpecV1) -> tuple[str, ...]:
    return tuple(column for column in MODEL_FEATURE_COLUMNS if family.include_hmm or column not in HMM_FEATURES)


def fit_policy_utility_transform(values: pd.Series | np.ndarray) -> PolicyUtilityTransformFit:
    target = _finite_vector(values, reason="policy utility transform input is invalid")
    if not len(target):
        raise _error("policy utility transform input is empty")
    location = float(np.median(target))
    scale = float(np.median(np.abs(target - location)))
    if not np.isfinite(location) or not np.isfinite(scale) or scale <= 0.0:
        raise _error("policy utility transform scale is invalid", scale_bps=scale)
    return PolicyUtilityTransformFit(location_bps=location, scale_bps=scale)


def apply_policy_utility_transform(values: pd.Series | np.ndarray, fit: PolicyUtilityTransformFit) -> np.ndarray:
    target = _finite_vector(values, reason="policy utility label contains non-finite values")
    _validate_fit(fit)
    transformed = (target - fit.location_bps) / fit.scale_bps
    if not np.isfinite(transformed).all():
        raise _error("policy utility standardized target is invalid")
    return transformed


def inverse_policy_utility_transform(values: pd.Series | np.ndarray, fit: PolicyUtilityTransformFit) -> np.ndarray:
    standardized = _finite_vector(values, reason="policy utility prediction contains non-finite values")
    _validate_fit(fit)
    restored = standardized * fit.scale_bps + fit.location_bps
    if not np.isfinite(restored).all():
        raise _error("policy utility inverse transform failed")
    return restored


def rank_policy_utility_predictions(frame: pd.DataFrame) -> pd.DataFrame:
    return rank_policy_predictions(frame, score_column="predicted_policy_net_excess_return_bps")


def rank_policy_predictions(frame: pd.DataFrame, *, score_column: str) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        score_column,
    }
    if not required.issubset(frame):
        raise _error("policy utility rank input is incomplete", missing_columns=sorted(required - set(frame)))
    result = frame.copy()
    result["decision_as_of_trade_date"] = pd.to_datetime(result["decision_as_of_trade_date"]).dt.normalize()
    counts = result.groupby("decision_as_of_trade_date").size()
    if counts.empty or not counts.eq(20).all():
        raise AdvisoryModelFirstError(
            "policy utility rank requires exact Selection Top20 for every date",
            reason_code="ADVISORY_POLICY_UTILITY_TOP20_INVALID",
            context={"counts": {str(key.date()): int(value) for key, value in counts.items()}},
        )
    selection_ranks = result.groupby("decision_as_of_trade_date")["selection_effective_rank"].apply(
        lambda values: tuple(sorted(pd.to_numeric(values, errors="coerce").tolist()))
    )
    expected_ranks = tuple(range(1, 21))
    if not selection_ranks.map(lambda values: values == expected_ranks).all():
        raise AdvisoryModelFirstError(
            "policy utility candidates are not exact Selection ranks 1..20",
            reason_code="ADVISORY_POLICY_UTILITY_TOP20_INVALID",
        )
    result[score_column] = pd.to_numeric(result[score_column], errors="coerce")
    result["selection_effective_rank"] = pd.to_numeric(result["selection_effective_rank"], errors="coerce")
    scores = result[score_column].to_numpy(float)
    if not np.isfinite(scores).all():
        raise _error("policy utility rank score is invalid")
    if result.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise _error("policy utility rank contains duplicate candidates")
    result = result.sort_values(
        [
            "decision_as_of_trade_date",
            score_column,
            "selection_effective_rank",
            "instrument",
        ],
        ascending=[True, False, True, True],
    )
    result["entry_priority_rank"] = result.groupby("decision_as_of_trade_date").cumcount().add(1)
    result["selection_exit_rank"] = result["selection_effective_rank"]
    return result.reset_index(drop=True)


def train_policy_utility_trial(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    validation_dates: Sequence[pd.Timestamp],
    family: PolicyUtilityFamilySpecV1,
    seed: int,
    arm: PolicyUtilityArmSpecV2 | None = None,
) -> PolicyUtilityTrainingResult:
    arm = arm or approved_policy_utility_arms()[-1]
    merged, matrix, vocabulary, feature_names, train_mask, validation_mask, validation_scoring_mask = (
        _prepare_training_rows(
            features=features,
            labels=labels,
            train_dates=train_dates,
            validation_dates=validation_dates,
            family=family,
        )
    )
    transform: PolicyUtilityTransformFit | None = None
    outcome_weighting_receipt: dict[str, float] | None = None
    train_weights = None
    validation_weights = None
    if arm.arm_id == "ARM_P0F_V2_HUBER_UTILITY":
        transform = fit_policy_utility_transform(merged.loc[train_mask, "net_excess_return_bps"])
        y_train = apply_policy_utility_transform(merged.loc[train_mask, "net_excess_return_bps"], transform)
        y_validation = apply_policy_utility_transform(merged.loc[validation_mask, "net_excess_return_bps"], transform)
    else:
        if (
            merged.loc[train_mask, "take_label"].nunique() < 2
            or merged.loc[validation_mask, "take_label"].nunique() < 2
        ):
            raise _error("policy utility binary arm contains a single class")
        y_train = merged.loc[train_mask, "take_label"].astype(int).to_numpy()
        y_validation = merged.loc[validation_mask, "take_label"].astype(int).to_numpy()
        if arm.outcome_weighted:
            specification = approved_meta_label_outcome_weighting()
            train_weights, fit = fit_meta_label_outcome_weights(
                merged.loc[train_mask, "net_excess_return_bps"], specification=specification
            )
            validation_weights = apply_meta_label_outcome_weights(
                merged.loc[validation_mask, "net_excess_return_bps"],
                specification=specification,
                fit=fit,
            )
            outcome_weighting_receipt = {
                "scale_bps": fit.scale_bps,
                "normalization_divisor": fit.normalization_divisor,
            }
    lgb = _lightgbm()
    train_data = lgb.Dataset(
        matrix.loc[train_mask],
        label=y_train,
        weight=train_weights,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        free_raw_data=False,
    )
    validation_data = lgb.Dataset(
        matrix.loc[validation_mask],
        label=y_validation,
        weight=validation_weights,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        reference=train_data,
        free_raw_data=False,
    )
    history: dict[str, Any] = {}
    booster = lgb.train(
        _training_params(family, seed, arm=arm),
        train_data,
        num_boost_round=family.max_boost_rounds,
        valid_sets=[validation_data],
        valid_names=["validation"],
        callbacks=[
            lgb.early_stopping(family.early_stopping_rounds, verbose=False),
            lgb.record_evaluation(history),
        ],
    )
    standardized = booster.predict(matrix.loc[validation_scoring_mask], num_iteration=booster.best_iteration)
    predictions = (
        inverse_policy_utility_transform(standardized, transform)
        if transform is not None
        else np.asarray(standardized, dtype=float)
    )
    validation_columns = [
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        "label_status",
        "net_excess_return_bps",
    ]
    if "take_label" in merged:
        validation_columns.append("take_label")
    validation = merged.loc[validation_scoring_mask, validation_columns].copy()
    validation[arm.prediction_column] = predictions
    validation = rank_policy_predictions(validation, score_column=arm.prediction_column)
    metrics = (
        policy_utility_candidate_metrics(validation)
        if transform is not None
        else policy_binary_candidate_metrics(validation)
    )
    return PolicyUtilityTrainingResult(
        booster=booster,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        best_iteration=int(booster.best_iteration),
        evaluation_history=history,
        validation_predictions=validation,
        metrics=metrics,
        transform=transform,
        outcome_weighting_receipt=outcome_weighting_receipt,
    )


def train_final_policy_utility(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    family: PolicyUtilityFamilySpecV1,
    seed: int,
    boost_rounds: int,
    arm: PolicyUtilityArmSpecV2 | None = None,
) -> FinalPolicyUtilityTrainingResult:
    arm = arm or approved_policy_utility_arms()[-1]
    dates = pd.to_datetime(labels.loc[labels["label_status"] == "MATURED", "decision_as_of_trade_date"])
    merged, matrix, vocabulary, feature_names, train_mask, _, _ = _prepare_training_rows(
        features=features,
        labels=labels,
        train_dates=dates,
        validation_dates=dates,
        family=family,
    )
    transform: PolicyUtilityTransformFit | None = None
    weights = None
    if arm.arm_id == "ARM_P0F_V2_HUBER_UTILITY":
        transform = fit_policy_utility_transform(merged.loc[train_mask, "net_excess_return_bps"])
        target = apply_policy_utility_transform(merged.loc[train_mask, "net_excess_return_bps"], transform)
    else:
        if merged.loc[train_mask, "take_label"].nunique() < 2:
            raise _error("final policy utility binary arm contains a single class")
        target = merged.loc[train_mask, "take_label"].astype(int).to_numpy()
        if arm.outcome_weighted:
            weights, _ = fit_meta_label_outcome_weights(
                merged.loc[train_mask, "net_excess_return_bps"],
                specification=approved_meta_label_outcome_weighting(),
            )
    lgb = _lightgbm()
    dataset = lgb.Dataset(
        matrix.loc[train_mask],
        label=target,
        weight=weights,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        free_raw_data=False,
    )
    booster = lgb.train(_training_params(family, seed, arm=arm), dataset, num_boost_round=boost_rounds)
    return FinalPolicyUtilityTrainingResult(
        booster=booster,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        boost_rounds=boost_rounds,
        transform=transform,
        arm_id=arm.arm_id,
    )


def policy_binary_candidate_metrics(predictions: pd.DataFrame) -> dict[str, Any]:
    scored = predictions[predictions["label_status"] == "MATURED"].copy()
    truth = pd.to_numeric(scored["take_label"], errors="coerce").astype(int).to_numpy()
    probability = pd.to_numeric(scored["take_probability"], errors="coerce").to_numpy(float)
    if not len(truth) or not np.isfinite(probability).all() or ((probability < 0) | (probability > 1)).any():
        raise _error("policy utility binary arm predictions are invalid")
    predicted = (probability >= 0.5).astype(int)
    return {
        "roc_auc": float(roc_auc_score(truth, probability)),
        "pr_auc": float(average_precision_score(truth, probability)),
        "brier": float(brier_score_loss(truth, probability)),
        "log_loss": float(log_loss(truth, probability, labels=[0, 1])),
        "accuracy": float(accuracy_score(truth, predicted)),
        "precision": float(precision_score(truth, predicted, zero_division=0)),
        "recall": float(recall_score(truth, predicted, zero_division=0)),
    }


def policy_utility_candidate_metrics(predictions: pd.DataFrame) -> dict[str, Any]:
    scored = predictions[predictions["label_status"] == "MATURED"].copy()
    actual = scored["net_excess_return_bps"].astype(float)
    predicted = scored["predicted_policy_net_excess_return_bps"].astype(float)
    daily_spearman = scored.groupby("decision_as_of_trade_date", sort=True).apply(
        lambda group: group["predicted_policy_net_excess_return_bps"].corr(
            group["net_excess_return_bps"], method="spearman"
        ),
        include_groups=False,
    )
    finite_spearman = daily_spearman[np.isfinite(daily_spearman)]
    daily_spread = scored.groupby("decision_as_of_trade_date", sort=True).apply(
        lambda group: (
            group.loc[group["entry_priority_rank"] <= 5, "net_excess_return_bps"].mean()
            - group.loc[group["entry_priority_rank"] > 5, "net_excess_return_bps"].mean()
        ),
        include_groups=False,
    )
    prediction_distribution = predictions["predicted_policy_net_excess_return_bps"].astype(float)
    quantiles = prediction_distribution.quantile([0.0, 0.25, 0.5, 0.75, 1.0]).to_dict()
    buckets = (
        scored.assign(
            selection_rank_bucket=pd.cut(
                predictions["selection_effective_rank"],
                bins=[0, 5, 10, 15, 20],
                labels=["1-5", "6-10", "11-15", "16-20"],
            )
        )
        .groupby("selection_rank_bucket", observed=True)
        .agg(
            row_count=("instrument", "size"),
            mean_actual_bps=("net_excess_return_bps", "mean"),
            mean_predicted_bps=("predicted_policy_net_excess_return_bps", "mean"),
            mean_entry_priority_rank=("entry_priority_rank", "mean"),
        )
    )
    return {
        "candidate_mae_bps": float(mean_absolute_error(actual, predicted)),
        "candidate_rmse_bps": float(mean_squared_error(actual, predicted) ** 0.5),
        "daily_spearman_mean": float(finite_spearman.mean()) if len(finite_spearman) else None,
        "daily_spearman_computable_dates": int(len(finite_spearman)),
        "top5_vs_rest_realized_utility_spread_bps": float(daily_spread.mean()),
        "prediction_distribution_bps": {str(key): float(value) for key, value in quantiles.items()},
        "selection_rank_bucket_attribution": buckets.reset_index().to_dict("records"),
    }


def _prepare_training_rows(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    validation_dates: Sequence[pd.Timestamp],
    family: PolicyUtilityFamilySpecV1,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    dict[str, tuple[int, ...]],
    tuple[str, ...],
    pd.Series,
    pd.Series,
    pd.Series,
]:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    merged = features.merge(labels, on=keys, how="inner", validate="one_to_one", suffixes=("", "_label"))
    merged["decision_as_of_trade_date"] = pd.to_datetime(merged["decision_as_of_trade_date"]).dt.normalize()
    train_set = set(pd.DatetimeIndex(pd.to_datetime(list(train_dates))).normalize())
    validation_set = set(pd.DatetimeIndex(pd.to_datetime(list(validation_dates))).normalize())
    matured = merged["label_status"] == "MATURED"
    train_mask = merged["decision_as_of_trade_date"].isin(train_set) & matured
    validation_mask = merged["decision_as_of_trade_date"].isin(validation_set) & matured
    validation_scoring_mask = merged["decision_as_of_trade_date"].isin(validation_set)
    if not train_mask.any() or not validation_mask.any():
        raise _error("policy utility path has no train or validation rows")
    validation_counts = merged.loc[validation_scoring_mask].groupby("decision_as_of_trade_date").size()
    if validation_counts.empty or not validation_counts.eq(20).all():
        raise AdvisoryModelFirstError(
            "policy utility validation does not contain exact Selection Top20",
            reason_code="ADVISORY_POLICY_UTILITY_TOP20_INVALID",
        )
    feature_names = policy_utility_feature_names(family)
    missing = sorted(set(feature_names) - set(merged))
    if missing:
        raise _error("policy utility feature matrix is incomplete", missing_columns=missing)
    matrix = merged.loc[:, feature_names].copy()
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    required = [column for column in feature_names if column in REQUIRED_FEATURE_COLUMNS]
    all_null = [column for column in required if matrix.loc[train_mask, column].isna().all()]
    if all_null:
        raise _error("policy utility train features are entirely missing", features=all_null)
    vocabulary: dict[str, tuple[int, ...]] = {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in matrix:
            continue
        categories = tuple(sorted(matrix.loc[train_mask, column].dropna().astype(int).unique()))
        if not categories:
            raise _error("policy utility categorical feature has no train vocabulary", feature=column)
        vocabulary[column] = categories
        matrix[column] = pd.Categorical(matrix[column], categories=categories)
    _finite_vector(merged.loc[train_mask, "net_excess_return_bps"], reason="policy utility train label is invalid")
    _finite_vector(
        merged.loc[validation_mask, "net_excess_return_bps"], reason="policy utility validation label is invalid"
    )
    return merged, matrix, vocabulary, feature_names, train_mask, validation_mask, validation_scoring_mask


def _training_params(
    family: PolicyUtilityFamilySpecV1,
    seed: int,
    *,
    arm: PolicyUtilityArmSpecV2 | None = None,
) -> dict[str, Any]:
    arm = arm or approved_policy_utility_arms()[-1]
    objective = "huber" if arm.arm_id == "ARM_P0F_V2_HUBER_UTILITY" else "binary"
    params = {
        "objective": objective,
        "metric": "l1" if objective == "huber" else ["binary_logloss", "auc"],
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
    if objective == "huber":
        params["alpha"] = 0.9
    return params


def _lightgbm() -> Any:
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable in WSL",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        ) from exc
    return lgb


def _finite_vector(values: pd.Series | np.ndarray, *, reason: str) -> np.ndarray:
    vector = np.asarray(values, dtype=float)
    if vector.ndim != 1 or not np.isfinite(vector).all():
        raise _error(reason)
    return vector


def _validate_fit(fit: PolicyUtilityTransformFit) -> None:
    if not np.isfinite(fit.location_bps) or not np.isfinite(fit.scale_bps) or fit.scale_bps <= 0.0:
        raise _error("policy utility transform fit is invalid")


def _error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_POLICY_UTILITY_PATH_NOT_COMPUTABLE",
        context=context or None,
    )
