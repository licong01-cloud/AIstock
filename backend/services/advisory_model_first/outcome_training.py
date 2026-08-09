from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, log_loss, mean_pinball_loss, roc_auc_score

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.outcome_contracts import (
    OUTCOME_HORIZONS,
    OUTCOME_QUANTILES,
)


@dataclass(frozen=True)
class OutcomeTrainingResult:
    models: dict[str, Any]
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    metrics: dict[str, Any]
    test_predictions: pd.DataFrame
    training_log: dict[str, Any]


def train_outcome_models(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    parent_test_predictions: pd.DataFrame,
    seed: int,
) -> OutcomeTrainingResult:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    merged = features.merge(labels, on=keys, how="inner", validate="one_to_one")
    if merged.empty:
        raise AdvisoryModelFirstError(
            "outcome feature and label matrices have no common rows",
            reason_code="ADVISORY_OUTCOME_LABEL_NOT_MATURE",
        )
    feature_names = tuple(MODEL_FEATURE_COLUMNS)
    missing_features = sorted(set(feature_names) - set(merged.columns))
    if missing_features:
        raise AdvisoryModelFirstError(
            "outcome matrix is missing frozen model features",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_features": missing_features},
        )
    split_values = set(merged["split"].astype(str))
    if not {"train", "validation", "test"}.issubset(split_values):
        raise AdvisoryModelFirstError(
            "outcome matrix is missing a required split",
            reason_code="ADVISORY_OUTCOME_REQUEST_INVALID",
            context={"splits": sorted(split_values)},
        )
    matrix, vocabulary = _prepare_matrix(merged, feature_names=feature_names)
    models: dict[str, Any] = {}
    metrics: dict[str, Any] = {"heads": {}}
    histories: dict[str, Any] = {}
    test_rows = merged[merged["split"] == "test"].copy()

    for horizon in OUTCOME_HORIZONS:
        eligible = merged[f"modelable_{horizon}"].astype(bool)
        train_mask = eligible & (merged["split"] == "train")
        validation_mask = eligible & (merged["split"] == "validation")
        test_mask = eligible & (merged["split"] == "test")
        _require_head_splits(train_mask, validation_mask, test_mask, head=f"horizon_{horizon}")

        raw_quantile_predictions: dict[float, np.ndarray] = {}
        for quantile in OUTCOME_QUANTILES:
            name = f"excess_return_h{horizon}_q{int(quantile * 100):02d}"
            model, history = _train_booster(
                matrix=matrix,
                target=pd.to_numeric(merged[f"excess_return_{horizon}"], errors="coerce"),
                train_mask=train_mask,
                validation_mask=validation_mask,
                objective="quantile",
                seed=seed,
                alpha=quantile,
                head=name,
            )
            predictions = _predict_finite(model, matrix.loc[test_mask], head=name)
            _require_probabilities(predictions, head=name)
            raw_quantile_predictions[quantile] = predictions
            models[name] = model
            histories[name] = history
            metrics["heads"][name] = {
                "pinball_loss": float(
                    mean_pinball_loss(
                        merged.loc[test_mask, f"excess_return_{horizon}"],
                        predictions,
                        alpha=quantile,
                    )
                ),
                "row_count": int(test_mask.sum()),
                "best_iteration": int(model.best_iteration),
            }
        quantile_stack = np.column_stack([raw_quantile_predictions[value] for value in OUTCOME_QUANTILES])
        crossing_count = int(((quantile_stack[:, 0] > quantile_stack[:, 1]) | (quantile_stack[:, 1] > quantile_stack[:, 2])).sum())
        monotonic = np.sort(quantile_stack, axis=1)
        test_positions = test_mask[test_mask].index
        for position, quantile in enumerate(OUTCOME_QUANTILES):
            test_rows.loc[test_positions, f"excess_return_q{int(quantile * 100):02d}_{horizon}"] = monotonic[:, position]
        actual = pd.to_numeric(merged.loc[test_mask, f"excess_return_{horizon}"], errors="raise").to_numpy()
        metrics[f"excess_return_h{horizon}"] = {
            "quantile_crossing_count": crossing_count,
            "q10_q90_empirical_coverage": float(((actual >= monotonic[:, 0]) & (actual <= monotonic[:, 2])).mean()),
        }

        for target_prefix, output_prefix in (
            ("positive_excess", "positive_probability"),
            ("signal_survival", "signal_survival_probability"),
        ):
            name = f"{target_prefix}_h{horizon}"
            target = pd.to_numeric(merged[f"{target_prefix}_{horizon}"], errors="coerce")
            _require_binary_variation(target.loc[train_mask], head=name)
            model, history = _train_booster(
                matrix=matrix,
                target=target,
                train_mask=train_mask,
                validation_mask=validation_mask,
                objective="binary",
                seed=seed,
                head=name,
            )
            predictions = _predict_finite(model, matrix.loc[test_mask], head=name)
            actual_binary = target.loc[test_mask].astype(int).to_numpy()
            test_rows.loc[test_positions, f"{output_prefix}_{horizon}"] = predictions
            models[name] = model
            histories[name] = history
            metrics["heads"][name] = {
                "binary_logloss": float(log_loss(actual_binary, predictions, labels=[0, 1])),
                "brier_score": float(np.mean((predictions - actual_binary) ** 2)),
                "roc_auc": _roc_auc(actual_binary, predictions),
                "positive_rate": float(actual_binary.mean()),
                "row_count": int(test_mask.sum()),
                "best_iteration": int(model.best_iteration),
            }

        for target_prefix in ("path_mfe", "path_mae_loss"):
            raw_path_predictions: dict[float, np.ndarray] = {}
            for quantile in (0.5, 0.9):
                name = f"{target_prefix}_h{horizon}_q{int(quantile * 100):02d}"
                target = pd.to_numeric(merged[f"{target_prefix}_{horizon}"], errors="coerce")
                model, history = _train_booster(
                    matrix=matrix,
                    target=target,
                    train_mask=train_mask,
                    validation_mask=validation_mask,
                    objective="quantile",
                    seed=seed,
                    alpha=quantile,
                    head=name,
                )
                raw = _predict_finite(model, matrix.loc[test_mask], head=name)
                clipped = np.clip(raw, 0.0, None)
                raw_path_predictions[quantile] = clipped
                models[name] = model
                histories[name] = history
                metrics["heads"][name] = {
                    "pinball_loss": float(
                        mean_pinball_loss(target.loc[test_mask], clipped, alpha=quantile)
                    ),
                    "negative_prediction_count_before_clip": int((raw < 0).sum()),
                    "row_count": int(test_mask.sum()),
                    "best_iteration": int(model.best_iteration),
                }
            path_stack = np.column_stack(
                [raw_path_predictions[quantile] for quantile in (0.5, 0.9)]
            )
            path_crossing_count = int((path_stack[:, 0] > path_stack[:, 1]).sum())
            monotonic_path = np.sort(path_stack, axis=1)
            for position, quantile in enumerate((0.5, 0.9)):
                test_rows.loc[
                    test_positions,
                    f"{target_prefix}_q{int(quantile * 100):02d}_{horizon}",
                ] = monotonic_path[:, position]
            metrics[f"{target_prefix}_h{horizon}"] = {
                "quantile_crossing_count": path_crossing_count,
            }

    holding_mask = merged["holding_modelable"].astype(bool)
    holding_train = holding_mask & (merged["split"] == "train")
    holding_validation = holding_mask & (merged["split"] == "validation")
    holding_test = holding_mask & (merged["split"] == "test")
    _require_head_splits(holding_train, holding_validation, holding_test, head="holding_bucket")
    bucket_to_class = {bucket: position for position, bucket in enumerate(OUTCOME_HORIZONS)}
    holding_target = pd.to_numeric(merged["optimal_holding_bucket"], errors="coerce").map(bucket_to_class)
    if holding_target.loc[holding_train].nunique() < 2:
        raise AdvisoryModelFirstError(
            "holding-period training labels contain fewer than two classes",
            reason_code="ADVISORY_OUTCOME_CLASS_VARIATION_MISSING",
            context={"head": "holding_bucket"},
        )
    holding_model, holding_history = _train_booster(
        matrix=matrix,
        target=holding_target,
        train_mask=holding_train,
        validation_mask=holding_validation,
        objective="multiclass",
        seed=seed,
        head="holding_bucket",
    )
    probabilities = np.asarray(holding_model.predict(matrix.loc[holding_test]), dtype=float)
    if (
        probabilities.shape != (int(holding_test.sum()), len(OUTCOME_HORIZONS))
        or not np.isfinite(probabilities).all()
        or (probabilities < 0.0).any()
        or (probabilities > 1.0).any()
        or not np.allclose(probabilities.sum(axis=1), 1.0, atol=1e-6)
    ):
        raise AdvisoryModelFirstError(
            "holding-period model returned invalid probabilities",
            reason_code="ADVISORY_OUTCOME_TRAINING_FAILED",
            context={"shape": list(probabilities.shape)},
        )
    holding_positions = holding_test[holding_test].index
    for class_index, horizon in enumerate(OUTCOME_HORIZONS):
        test_rows.loc[holding_positions, f"holding_probability_{horizon}"] = probabilities[:, class_index]
    modes = np.asarray(OUTCOME_HORIZONS)[probabilities.argmax(axis=1)]
    lows, highs = _holding_ranges(probabilities)
    test_rows.loc[holding_positions, "holding_mode_days"] = modes
    test_rows.loc[holding_positions, "holding_range_low_days"] = lows
    test_rows.loc[holding_positions, "holding_range_high_days"] = highs
    actual_classes = holding_target.loc[holding_test].astype(int).to_numpy()
    actual_days = np.asarray(OUTCOME_HORIZONS)[actual_classes]
    models["holding_bucket"] = holding_model
    histories["holding_bucket"] = holding_history
    metrics["heads"]["holding_bucket"] = {
        "multiclass_logloss": float(log_loss(actual_classes, probabilities, labels=list(range(5)))),
        "accuracy": float(accuracy_score(actual_classes, probabilities.argmax(axis=1))),
        "bucket_day_mae": float(np.mean(np.abs(actual_days - modes))),
        "range_coverage": float(((actual_days >= lows) & (actual_days <= highs)).mean()),
        "row_count": int(holding_test.sum()),
        "best_iteration": int(holding_model.best_iteration),
    }
    metrics.update(
        {
            "model_count": len(models),
            "test_row_count": len(test_rows),
            "test_date_count": int(test_rows["decision_as_of_trade_date"].nunique()),
            "calibration_state": "UNCALIBRATED",
        }
    )
    prediction_columns = [
        *[
            f"excess_return_q{int(quantile * 100):02d}_{horizon}"
            for horizon in OUTCOME_HORIZONS
            for quantile in OUTCOME_QUANTILES
        ],
        *[f"positive_probability_{horizon}" for horizon in OUTCOME_HORIZONS],
        *[f"signal_survival_probability_{horizon}" for horizon in OUTCOME_HORIZONS],
        *[
            f"{prefix}_q{int(quantile * 100):02d}_{horizon}"
            for horizon in OUTCOME_HORIZONS
            for prefix in ("path_mfe", "path_mae_loss")
            for quantile in (0.5, 0.9)
        ],
        *[f"holding_probability_{horizon}" for horizon in OUTCOME_HORIZONS],
        "holding_mode_days",
        "holding_range_low_days",
        "holding_range_high_days",
    ]
    output_columns = [
        *keys,
        "selection_effective_rank",
        "parent_combined_score",
        *prediction_columns,
    ]
    test_predictions = (
        test_rows.loc[:, list(dict.fromkeys(output_columns))]
        .sort_values(keys)
        .reset_index(drop=True)
    )
    metrics["group_summaries"] = _group_summaries(
        test_predictions=test_predictions,
        labels=merged.loc[merged["split"] == "test"],
        parent_test_predictions=parent_test_predictions,
    )
    return OutcomeTrainingResult(
        models=models,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        metrics=metrics,
        test_predictions=test_predictions,
        training_log={"evaluation_history": histories},
    )


def _prepare_matrix(
    merged: pd.DataFrame,
    *,
    feature_names: tuple[str, ...],
) -> tuple[pd.DataFrame, dict[str, tuple[int, ...]]]:
    matrix = merged.loc[:, feature_names].copy()
    for column in matrix.columns:
        if column not in CATEGORICAL_FEATURE_COLUMNS:
            try:
                matrix[column] = pd.to_numeric(matrix[column], errors="raise")
            except (TypeError, ValueError) as exc:
                raise AdvisoryModelFirstError(
                    "outcome feature contains a non-numeric value",
                    reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
                    context={"feature": column, "error_type": type(exc).__name__},
                ) from exc
    train_rows = merged["split"] == "train"
    all_null = [column for column in feature_names if matrix.loc[train_rows, column].isna().all()]
    if all_null:
        raise AdvisoryModelFirstError(
            "outcome frozen feature is entirely missing in train",
            reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
            context={"features": all_null},
        )
    vocabulary: dict[str, tuple[int, ...]] = {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        values = pd.to_numeric(matrix.loc[train_rows, column], errors="coerce").dropna().astype(int)
        categories = tuple(sorted(values.unique().tolist()))
        if not categories:
            raise AdvisoryModelFirstError(
                "outcome categorical feature has no train vocabulary",
                reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
                context={"feature": column},
            )
        vocabulary[column] = categories
        matrix[column] = pd.Categorical(pd.to_numeric(matrix[column], errors="coerce"), categories=categories)
    return matrix, vocabulary


def _train_booster(
    *,
    matrix: pd.DataFrame,
    target: pd.Series,
    train_mask: pd.Series,
    validation_mask: pd.Series,
    objective: str,
    seed: int,
    head: str,
    alpha: float | None = None,
) -> tuple[Any, dict[str, Any]]:
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable in the WSL outcome training environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"error_type": type(exc).__name__},
        ) from exc
    train_target = pd.to_numeric(target.loc[train_mask], errors="coerce")
    validation_target = pd.to_numeric(target.loc[validation_mask], errors="coerce")
    if train_target.isna().any() or validation_target.isna().any():
        raise AdvisoryModelFirstError(
            "outcome training target contains missing values",
            reason_code="ADVISORY_OUTCOME_LABEL_NOT_MATURE",
            context={"head": head},
        )
    train_set = lgb.Dataset(
        matrix.loc[train_mask],
        label=train_target,
        categorical_feature=list(CATEGORICAL_FEATURE_COLUMNS),
        free_raw_data=False,
    )
    validation_set = lgb.Dataset(
        matrix.loc[validation_mask],
        label=validation_target,
        categorical_feature=list(CATEGORICAL_FEATURE_COLUMNS),
        reference=train_set,
        free_raw_data=False,
    )
    parameters: dict[str, Any] = {
        "objective": objective,
        "metric": "quantile" if objective == "quantile" else ("binary_logloss" if objective == "binary" else "multi_logloss"),
        "num_leaves": 31,
        "learning_rate": 0.03,
        "min_data_in_leaf": 40,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 1,
        "lambda_l1": 0.1,
        "lambda_l2": 1.0,
        "deterministic": True,
        "force_col_wise": True,
        "num_threads": 4,
        "seed": seed,
        "feature_fraction_seed": seed,
        "bagging_seed": seed,
        "data_random_seed": seed,
        "verbosity": -1,
    }
    if objective == "quantile":
        parameters["alpha"] = alpha
    if objective == "multiclass":
        parameters["num_class"] = len(OUTCOME_HORIZONS)
    history: dict[str, Any] = {}
    try:
        booster = lgb.train(
            parameters,
            train_set,
            num_boost_round=400,
            valid_sets=[validation_set],
            valid_names=["validation"],
            callbacks=[
                lgb.early_stopping(stopping_rounds=40, verbose=False),
                lgb.record_evaluation(history),
                lgb.log_evaluation(period=0),
            ],
        )
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM outcome head training failed",
            reason_code="ADVISORY_OUTCOME_TRAINING_FAILED",
            context={"head": head, "error_type": type(exc).__name__, "error_message": str(exc)},
        ) from exc
    return booster, history


def _require_head_splits(
    train_mask: pd.Series,
    validation_mask: pd.Series,
    test_mask: pd.Series,
    *,
    head: str,
) -> None:
    counts = {
        "train": int(train_mask.sum()),
        "validation": int(validation_mask.sum()),
        "test": int(test_mask.sum()),
    }
    if min(counts.values()) <= 0:
        raise AdvisoryModelFirstError(
            "outcome head has an empty model split",
            reason_code="ADVISORY_OUTCOME_LABEL_NOT_MATURE",
            context={"head": head, "split_counts": counts},
        )


def _require_binary_variation(target: pd.Series, *, head: str) -> None:
    values = set(pd.to_numeric(target, errors="coerce").dropna().astype(int).unique().tolist())
    if values != {0, 1}:
        raise AdvisoryModelFirstError(
            "outcome binary head requires both classes in train",
            reason_code="ADVISORY_OUTCOME_CLASS_VARIATION_MISSING",
            context={"head": head, "classes": sorted(values)},
        )


def _predict_finite(model: Any, matrix: pd.DataFrame, *, head: str) -> np.ndarray:
    predictions = np.asarray(model.predict(matrix), dtype=float)
    if predictions.shape != (len(matrix),) or not np.isfinite(predictions).all():
        raise AdvisoryModelFirstError(
            "outcome head returned invalid predictions",
            reason_code="ADVISORY_OUTCOME_TRAINING_FAILED",
            context={"head": head, "shape": list(predictions.shape)},
        )
    return predictions


def _require_probabilities(predictions: np.ndarray, *, head: str) -> None:
    if (predictions < 0.0).any() or (predictions > 1.0).any():
        raise AdvisoryModelFirstError(
            "outcome binary head returned values outside the probability range",
            reason_code="ADVISORY_OUTCOME_TRAINING_FAILED",
            context={"head": head},
        )


def _roc_auc(actual: np.ndarray, predictions: np.ndarray) -> float | None:
    return float(roc_auc_score(actual, predictions)) if len(np.unique(actual)) == 2 else None


def _holding_ranges(probabilities: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    cumulative = np.cumsum(probabilities, axis=1)
    low_indices = (cumulative < 0.2).sum(axis=1).clip(max=len(OUTCOME_HORIZONS) - 1)
    high_indices = (cumulative < 0.8).sum(axis=1).clip(max=len(OUTCOME_HORIZONS) - 1)
    buckets = np.asarray(OUTCOME_HORIZONS)
    return buckets[low_indices], buckets[high_indices]


def _group_summaries(
    *,
    test_predictions: pd.DataFrame,
    labels: pd.DataFrame,
    parent_test_predictions: pd.DataFrame,
) -> dict[str, Any]:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    required_parent = {*keys, "advisory_model_rank"}
    missing_parent = sorted(required_parent - set(parent_test_predictions.columns))
    if missing_parent:
        raise AdvisoryModelFirstError(
            "parent model test predictions are missing ranking identity",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
            context={"missing_columns": missing_parent},
        )
    label_columns = [*keys, *[f"excess_return_{horizon}" for horizon in OUTCOME_HORIZONS]]
    try:
        summary_rows = test_predictions.merge(
            labels.loc[:, label_columns],
            on=keys,
            how="left",
            validate="one_to_one",
        ).merge(
            parent_test_predictions.loc[:, [*keys, "advisory_model_rank"]],
            on=keys,
            how="left",
            validate="one_to_one",
        )
    except pd.errors.MergeError as exc:
        raise AdvisoryModelFirstError(
            "outcome summary inputs do not have one-to-one candidate identity",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
            context={"error_type": type(exc).__name__},
        ) from exc
    if summary_rows["advisory_model_rank"].isna().any() or summary_rows[
        [f"excess_return_{horizon}" for horizon in OUTCOME_HORIZONS]
    ].isna().any().any():
        raise AdvisoryModelFirstError(
            "outcome summary inputs do not cover the complete test candidate set",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
            context={
                "test_row_count": len(summary_rows),
                "missing_parent_rank_count": int(summary_rows["advisory_model_rank"].isna().sum()),
            },
        )
    groups = {
        "selection_top5": summary_rows["selection_effective_rank"] <= 5,
        "m2_model_top5": summary_rows["advisory_model_rank"] <= 5,
    }
    output: dict[str, Any] = {}
    for group_name, mask in groups.items():
        group = summary_rows.loc[mask]
        if group.empty:
            raise AdvisoryModelFirstError(
                "outcome summary group is empty",
                reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
                context={"group": group_name},
            )
        by_horizon: dict[str, Any] = {}
        for horizon in OUTCOME_HORIZONS:
            by_horizon[str(horizon)] = {
                "actual_mean_excess_return": float(group[f"excess_return_{horizon}"].mean()),
                "predicted_mean_excess_q50": float(group[f"excess_return_q50_{horizon}"].mean()),
                "predicted_mean_positive_probability": float(
                    group[f"positive_probability_{horizon}"].mean()
                ),
                "predicted_mean_signal_survival_probability": float(
                    group[f"signal_survival_probability_{horizon}"].mean()
                ),
            }
        output[group_name] = {
            "row_count": len(group),
            "decision_date_count": int(group["decision_as_of_trade_date"].nunique()),
            "holding_mode_days_median": float(group["holding_mode_days"].median()),
            "horizons": by_horizon,
        }
    return output
