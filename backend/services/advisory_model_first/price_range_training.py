from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, mean_pinball_loss, roc_auc_score

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.price_range_contracts import (
    ENTRY_GAP_CONDITION,
    PRICE_RANGE_MODEL_NAMES,
    PRICE_RANGE_QUANTILES,
)


@dataclass(frozen=True)
class PriceRangeTrainingResult:
    models: dict[str, Any]
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    metrics: dict[str, Any]
    test_predictions: pd.DataFrame
    training_log: dict[str, Any]


def train_price_range_models(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    seed: int,
) -> PriceRangeTrainingResult:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    if features.duplicated(keys).any() or labels.duplicated(keys).any():
        raise AdvisoryModelFirstError(
            "price-range features or labels contain duplicate identities",
            reason_code="ADVISORY_PRICE_RANGE_LABEL_INPUT_UNAVAILABLE",
        )
    merged = features.merge(labels, on=keys, how="left", validate="one_to_one", indicator=True)
    if not merged["_merge"].eq("both").all():
        raise AdvisoryModelFirstError(
            "price-range frozen feature row has no candidate label",
            reason_code="ADVISORY_PRICE_RANGE_LABEL_INPUT_UNAVAILABLE",
            context={"missing_label_rows": int(merged["_merge"].ne("both").sum())},
        )
    merged = merged.drop(columns="_merge")
    missing_feature_rows = labels.merge(
        features.loc[:, keys], on=keys, how="left", validate="one_to_one", indicator=True
    )
    missing_feature_rows = missing_feature_rows.loc[missing_feature_rows["_merge"].eq("left_only")]
    missing_by_split = {
        name: int(missing_feature_rows["split"].eq(name).sum())
        for name in ("train", "validation", "test", "purged")
    }
    if missing_by_split["test"]:
        raise AdvisoryModelFirstError(
            "price-range test candidate is missing its frozen M1 feature row",
            reason_code="ADVISORY_PRICE_RANGE_SAMPLE_INSUFFICIENT",
            context={"missing_feature_rows_by_split": missing_by_split},
        )
    feature_names = tuple(MODEL_FEATURE_COLUMNS)
    missing_features = sorted(set(feature_names) - set(merged.columns))
    if missing_features:
        raise AdvisoryModelFirstError(
            "price-range matrix is missing frozen model features",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_features": missing_features},
        )
    matrix, vocabulary = _prepare_matrix(merged, feature_names=feature_names)
    all_test_mask = merged["split"].eq("test")
    if not all_test_mask.any():
        raise AdvisoryModelFirstError(
            "price-range matrix has no test candidates",
            reason_code="ADVISORY_PRICE_RANGE_SAMPLE_INSUFFICIENT",
        )
    test_positions = all_test_mask[all_test_mask].index
    test_rows = merged.loc[all_test_mask].copy()
    models: dict[str, Any] = {}
    histories: dict[str, Any] = {}
    metrics: dict[str, Any] = {"heads": {}}

    binary_eligible = merged["binary_modelable"].astype(bool)
    binary_masks = _split_masks(merged, binary_eligible, head="entry_executable_probability")
    binary_target = pd.to_numeric(merged["entry_executable"], errors="coerce")
    _require_binary_variation(binary_target.loc[binary_masks[0]])
    binary_model, history = _train_booster(
        matrix=matrix,
        target=binary_target,
        train_mask=binary_masks[0],
        validation_mask=binary_masks[1],
        objective="binary",
        seed=seed,
        head="entry_executable_probability",
    )
    binary_predictions = _predict_finite(
        binary_model, matrix.loc[all_test_mask], head="entry_executable_probability"
    )
    _require_probabilities(binary_predictions, head="entry_executable_probability")
    test_rows.loc[test_positions, "entry_executable_probability"] = binary_predictions
    binary_locations = np.flatnonzero(binary_masks[2].loc[test_positions].to_numpy())
    binary_actual = binary_target.loc[binary_masks[2]].astype(int).to_numpy()
    binary_scored = binary_predictions[binary_locations]
    models["entry_executable_probability"] = binary_model
    histories["entry_executable_probability"] = history
    metrics["heads"]["entry_executable_probability"] = {
        "binary_logloss": float(log_loss(binary_actual, binary_scored, labels=[0, 1])),
        "brier_score": float(np.mean((binary_scored - binary_actual) ** 2)),
        "roc_auc": _roc_auc(binary_actual, binary_scored),
        "positive_rate": float(binary_actual.mean()),
        "row_count": int(binary_masks[2].sum()),
        "best_iteration": int(binary_model.best_iteration),
    }

    gap_eligible = merged["gap_modelable"].astype(bool)
    gap_masks = _split_masks(merged, gap_eligible, head="entry_gap")
    gap_target = pd.to_numeric(merged["entry_gap_return"], errors="coerce")
    raw_gap_predictions: dict[float, np.ndarray] = {}
    for quantile in PRICE_RANGE_QUANTILES:
        name = f"entry_gap_q{int(quantile * 100):02d}"
        model, history = _train_booster(
            matrix=matrix,
            target=gap_target,
            train_mask=gap_masks[0],
            validation_mask=gap_masks[1],
            objective="quantile",
            seed=seed,
            alpha=quantile,
            head=name,
        )
        raw = _predict_finite(model, matrix.loc[all_test_mask], head=name)
        eligible_predictions = raw[np.flatnonzero(gap_masks[2].loc[test_positions].to_numpy())]
        raw_gap_predictions[quantile] = raw
        models[name] = model
        histories[name] = history
        metrics["heads"][name] = {
            "pinball_loss": float(
                mean_pinball_loss(
                    gap_target.loc[gap_masks[2]],
                    eligible_predictions,
                    alpha=quantile,
                )
            ),
            "row_count": int(gap_masks[2].sum()),
            "best_iteration": int(model.best_iteration),
            "condition": ENTRY_GAP_CONDITION,
        }
    raw_stack = np.column_stack(
        [raw_gap_predictions[quantile] for quantile in PRICE_RANGE_QUANTILES]
    )
    crossing = (raw_stack[:, 0] > raw_stack[:, 1]) | (raw_stack[:, 1] > raw_stack[:, 2])
    monotonic = np.sort(raw_stack, axis=1)
    for position, quantile in enumerate(PRICE_RANGE_QUANTILES):
        test_rows.loc[test_positions, f"entry_gap_q{int(quantile * 100):02d}"] = monotonic[
            :, position
        ]
    gap_locations = np.flatnonzero(gap_masks[2].loc[test_positions].to_numpy())
    gap_actual = gap_target.loc[gap_masks[2]].to_numpy(dtype=float)
    metrics["entry_gap_distribution"] = {
        "condition": ENTRY_GAP_CONDITION,
        "quantile_crossing_count": int(crossing.sum()),
        "quantile_crossing_rate": float(crossing.mean()),
        "q10_q90_empirical_coverage": float(
            (
                (gap_actual >= monotonic[gap_locations, 0])
                & (gap_actual <= monotonic[gap_locations, 2])
            ).mean()
        ),
    }
    if tuple(sorted(models)) != tuple(sorted(PRICE_RANGE_MODEL_NAMES)):
        raise AdvisoryModelFirstError(
            "price-range trainer did not produce the exact four-head contract",
            reason_code="ADVISORY_PRICE_RANGE_TRAINING_FAILED",
            context={"model_names": sorted(models)},
        )
    metrics.update(
        {
            "model_count": len(models),
            "test_row_count": len(test_rows),
            "test_date_count": int(test_rows["decision_as_of_trade_date"].nunique()),
            "binary_test_row_count": int(binary_masks[2].sum()),
            "executable_gap_test_row_count": int(gap_masks[2].sum()),
            "feature_available_row_count": len(merged),
            "feature_unavailable_row_count": len(missing_feature_rows),
            "feature_unavailable_date_count": int(
                missing_feature_rows["decision_as_of_trade_date"].nunique()
            ),
            "feature_unavailable_rows_by_split": missing_by_split,
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "UNCALIBRATED",
        }
    )
    output_columns = [
        *keys,
        "selection_effective_rank",
        "parent_combined_score",
        "entry_label_status",
        "entry_label_reason",
        "entry_executable",
        "entry_gap_return",
        "entry_executable_probability",
        "entry_gap_q10",
        "entry_gap_q50",
        "entry_gap_q90",
    ]
    test_rows["entry_gap_condition"] = ENTRY_GAP_CONDITION
    output_columns.append("entry_gap_condition")
    return PriceRangeTrainingResult(
        models=models,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        metrics=metrics,
        test_predictions=test_rows.loc[:, output_columns].sort_values(keys).reset_index(drop=True),
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
                    "price-range feature contains a non-numeric value",
                    reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
                    context={"feature": column, "error_type": type(exc).__name__},
                ) from exc
    train_rows = merged["split"].eq("train")
    all_null = [name for name in feature_names if matrix.loc[train_rows, name].isna().all()]
    if all_null:
        raise AdvisoryModelFirstError(
            "price-range frozen feature is entirely missing in train",
            reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
            context={"features": all_null},
        )
    vocabulary: dict[str, tuple[int, ...]] = {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        values = pd.to_numeric(matrix.loc[train_rows, column], errors="coerce").dropna().astype(int)
        categories = tuple(sorted(values.unique().tolist()))
        if not categories:
            raise AdvisoryModelFirstError(
                "price-range categorical feature has no train vocabulary",
                reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
                context={"feature": column},
            )
        vocabulary[column] = categories
        matrix[column] = pd.Categorical(
            pd.to_numeric(matrix[column], errors="coerce"), categories=categories
        )
    return matrix, vocabulary


def _split_masks(
    merged: pd.DataFrame,
    eligible: pd.Series,
    *,
    head: str,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    masks = tuple(eligible & merged["split"].eq(name) for name in ("train", "validation", "test"))
    counts = dict(zip(("train", "validation", "test"), (int(mask.sum()) for mask in masks)))
    if any(value <= 0 for value in counts.values()):
        raise AdvisoryModelFirstError(
            "price-range head has no eligible rows in a required split",
            reason_code="ADVISORY_PRICE_RANGE_SAMPLE_INSUFFICIENT",
            context={"head": head, "counts": counts},
        )
    return masks  # type: ignore[return-value]


def _require_binary_variation(target: pd.Series) -> None:
    values = pd.to_numeric(target, errors="coerce").dropna().astype(int)
    if set(values.unique()) != {0, 1}:
        raise AdvisoryModelFirstError(
            "price-range executable training labels do not contain both classes",
            reason_code="ADVISORY_PRICE_RANGE_LABEL_VARIATION_MISSING",
            context={"classes": sorted(values.unique().tolist())},
        )


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
            "LightGBM is unavailable in the WSL price-range training environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"error_type": type(exc).__name__},
        ) from exc
    train_target = pd.to_numeric(target.loc[train_mask], errors="coerce")
    validation_target = pd.to_numeric(target.loc[validation_mask], errors="coerce")
    if train_target.isna().any() or validation_target.isna().any():
        raise AdvisoryModelFirstError(
            "price-range training target contains missing values",
            reason_code="ADVISORY_PRICE_RANGE_LABEL_INPUT_UNAVAILABLE",
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
        "metric": "quantile" if objective == "quantile" else "binary_logloss",
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
            "LightGBM price-range head training failed",
            reason_code="ADVISORY_PRICE_RANGE_TRAINING_FAILED",
            context={"head": head, "error_type": type(exc).__name__, "error_message": str(exc)},
        ) from exc
    return booster, history


def _predict_finite(model: Any, matrix: pd.DataFrame, *, head: str) -> np.ndarray:
    predictions = np.asarray(model.predict(matrix), dtype=float)
    if predictions.shape != (len(matrix),) or not np.isfinite(predictions).all():
        raise AdvisoryModelFirstError(
            "price-range model returned invalid predictions",
            reason_code="ADVISORY_PRICE_RANGE_TRAINING_FAILED",
            context={"head": head, "shape": list(predictions.shape)},
        )
    return predictions


def _require_probabilities(predictions: np.ndarray, *, head: str) -> None:
    if (predictions < 0.0).any() or (predictions > 1.0).any():
        raise AdvisoryModelFirstError(
            "price-range binary head returned values outside [0, 1]",
            reason_code="ADVISORY_PRICE_RANGE_TRAINING_FAILED",
            context={"head": head},
        )


def _roc_auc(actual: np.ndarray, predictions: np.ndarray) -> float | None:
    return float(roc_auc_score(actual, predictions)) if len(np.unique(actual)) == 2 else None
