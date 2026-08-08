from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.time_split import PurgedDateSplit


@dataclass(frozen=True)
class RerankerTrainingResult:
    booster: Any
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    evaluation_history: dict[str, Any]
    metrics: dict[str, Any]
    test_predictions: pd.DataFrame
    baseline_comparison: dict[str, Any]


def train_lambdarank(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    split: PurgedDateSplit,
    seed: int = 20260808,
) -> RerankerTrainingResult:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    merged = features.merge(
        labels,
        on=keys,
        how="inner",
        validate="one_to_one",
        suffixes=("", "_label"),
    )
    merged["decision_as_of_trade_date"] = pd.to_datetime(merged["decision_as_of_trade_date"]).dt.normalize()
    expected_split_by_date = {
        **{date: "train" for date in split.train},
        **{date: "validation" for date in split.validation},
        **{date: "test" for date in split.test},
    }
    declared = merged["decision_as_of_trade_date"].map(expected_split_by_date).fillna("purged")
    split_mismatch = merged["split"].astype(str) != declared
    if split_mismatch.any():
        sample = merged.loc[split_mismatch, ["decision_as_of_trade_date", "split"]].head(10)
        raise AdvisoryModelFirstError(
            "label split does not match the frozen purged date split",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"examples": sample.astype(str).to_dict("records")},
        )
    merged = merged[
        (merged["group_label_status"] == "AVAILABLE")
        & merged["relevance"].notna()
        & merged["split"].isin(["train", "validation", "test"])
    ].copy()
    if merged.empty:
        raise AdvisoryModelFirstError(
            "no modelable feature-label rows remain",
            reason_code="ADVISORY_MODEL_LABEL_NOT_MATURE",
        )
    merged = merged.sort_values(
        ["decision_as_of_trade_date", "selection_effective_rank", "instrument"]
    ).reset_index(drop=True)
    feature_names = tuple(MODEL_FEATURE_COLUMNS)
    missing_columns = sorted(set(feature_names) - set(merged.columns))
    if missing_columns:
        raise AdvisoryModelFirstError(
            "training matrix is missing frozen feature columns",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": missing_columns},
        )
    train_mask = merged["split"] == "train"
    validation_mask = merged["split"] == "validation"
    test_mask = merged["split"] == "test"
    for name, mask in (("train", train_mask), ("validation", validation_mask), ("test", test_mask)):
        date_count = int(merged.loc[mask, "decision_as_of_trade_date"].nunique())
        if date_count == 0:
            raise AdvisoryModelFirstError(
                "training matrix split has no modelable dates",
                reason_code="ADVISORY_MODEL_LABEL_NOT_MATURE",
                context={"split": name},
            )

    all_null_train = [column for column in feature_names if merged.loc[train_mask, column].isna().all()]
    if all_null_train:
        raise AdvisoryModelFirstError(
            "frozen feature columns are entirely missing in train",
            reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
            context={"all_null_train_features": all_null_train},
        )
    matrix = merged.loc[:, feature_names].copy()
    vocabulary: dict[str, tuple[int, ...]] = {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        values = pd.to_numeric(matrix.loc[train_mask, column], errors="coerce").dropna().astype(int)
        categories = tuple(sorted(values.unique().tolist()))
        if not categories:
            raise AdvisoryModelFirstError(
                "categorical feature has no train vocabulary",
                reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
                context={"feature": column},
            )
        vocabulary[column] = categories
        matrix[column] = pd.Categorical(
            pd.to_numeric(matrix[column], errors="coerce"),
            categories=categories,
        )

    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable in the WSL training environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"error_type": type(exc).__name__},
        ) from exc
    train_set = lgb.Dataset(
        matrix.loc[train_mask],
        label=merged.loc[train_mask, "relevance"].astype(int),
        group=_group_sizes(merged.loc[train_mask]),
        categorical_feature=list(CATEGORICAL_FEATURE_COLUMNS),
        free_raw_data=False,
    )
    validation_set = lgb.Dataset(
        matrix.loc[validation_mask],
        label=merged.loc[validation_mask, "relevance"].astype(int),
        group=_group_sizes(merged.loc[validation_mask]),
        categorical_feature=list(CATEGORICAL_FEATURE_COLUMNS),
        reference=train_set,
        free_raw_data=False,
    )
    parameters = {
        "objective": "lambdarank",
        "metric": "ndcg",
        "eval_at": [5],
        "label_gain": [0, 1, 3, 7, 15],
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
    evaluation_history: dict[str, Any] = {}
    try:
        booster = lgb.train(
            parameters,
            train_set,
            num_boost_round=600,
            valid_sets=[validation_set],
            valid_names=["validation"],
            callbacks=[
                lgb.early_stopping(stopping_rounds=60, verbose=False),
                lgb.record_evaluation(evaluation_history),
                lgb.log_evaluation(period=25),
            ],
        )
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM LambdaRank training failed",
            reason_code="ADVISORY_MODEL_TRAINING_FAILED",
            context={
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "train_row_count": int(train_mask.sum()),
                "validation_row_count": int(validation_mask.sum()),
            },
        ) from exc
    test = merged.loc[test_mask].copy()
    test_matrix = matrix.loc[test_mask]
    test["advisory_model_score"] = booster.predict(test_matrix, num_iteration=booster.best_iteration)
    test = test.sort_values(
        ["decision_as_of_trade_date", "advisory_model_score", "instrument"],
        ascending=[True, False, True],
    )
    test["advisory_model_rank"] = test.groupby("decision_as_of_trade_date").cumcount().add(1)
    test["is_top5"] = test["advisory_model_rank"] <= 5
    metrics, baseline_comparison = _evaluate_test(
        test,
        seed=seed,
        expected_test_date_count=len(split.test),
    )
    metrics.update(
        {
            "best_iteration": int(booster.best_iteration),
            "train_date_count": int(merged.loc[train_mask, "decision_as_of_trade_date"].nunique()),
            "validation_date_count": int(merged.loc[validation_mask, "decision_as_of_trade_date"].nunique()),
            "test_date_count": int(test["decision_as_of_trade_date"].nunique()),
            "train_row_count": int(train_mask.sum()),
            "validation_row_count": int(validation_mask.sum()),
            "test_row_count": int(test_mask.sum()),
        }
    )
    keep = [
        *keys,
        "selection_effective_rank",
        "parent_combined_score",
        "hmm_bull_posterior",
        "advisory_model_score",
        "advisory_model_rank",
        "is_top5",
        "relevance",
        "stock_net_return_5",
        "excess_return_5",
        "path_mfe_5",
        "path_mae_loss_5",
    ]
    return RerankerTrainingResult(
        booster=booster,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        evaluation_history=evaluation_history,
        metrics=metrics,
        test_predictions=test[keep].sort_values(
            ["decision_as_of_trade_date", "advisory_model_rank"]
        ).reset_index(drop=True),
        baseline_comparison=baseline_comparison,
    )


def _group_sizes(frame: pd.DataFrame) -> list[int]:
    return frame.groupby("decision_as_of_trade_date", sort=False).size().astype(int).tolist()


def _evaluate_test(
    test: pd.DataFrame,
    *,
    seed: int,
    expected_test_date_count: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    model = test[test["advisory_model_rank"] <= 5]
    metrics = _selection_metrics(model, expected_date_count=expected_test_date_count)
    ndcg_values = []
    for _, group in test.groupby("decision_as_of_trade_date", sort=True):
        ordered = group.sort_values(["advisory_model_score", "instrument"], ascending=[False, True])
        ideal = group.sort_values(["relevance", "instrument"], ascending=[False, True])
        actual_dcg = _dcg(ordered["relevance"].to_numpy(dtype=int)[:5])
        ideal_dcg = _dcg(ideal["relevance"].to_numpy(dtype=int)[:5])
        ndcg_values.append(actual_dcg / ideal_dcg if ideal_dcg > 0 else 0.0)
    metrics["date_level_ndcg_at_5"] = float(np.mean(ndcg_values))

    selections: dict[str, pd.DataFrame] = {
        "model_top5": model,
        "selection_rank_top5": test[test["selection_effective_rank"] <= 5],
        "hmm_top5": _top_by(test, "hmm_bull_posterior", 5),
        "random_top5": _random_top5(test, seed=seed),
        "candidate_top20_equal": test,
    }
    comparison = {
        name: _selection_metrics(frame, expected_date_count=expected_test_date_count)
        for name, frame in selections.items()
    }
    return metrics, comparison


def _selection_metrics(selected: pd.DataFrame, *, expected_date_count: int) -> dict[str, Any]:
    if selected.empty:
        return {"row_count": 0, "date_count": 0, "status": "unavailable"}
    turnover = _shortlist_turnover(selected)
    return {
        "status": "available",
        "row_count": int(len(selected)),
        "date_count": int(selected["decision_as_of_trade_date"].nunique()),
        "mean_stock_net_return_5": float(selected["stock_net_return_5"].mean()),
        "mean_excess_return_5": float(selected["excess_return_5"].mean()),
        "absolute_hit_rate": float((selected["stock_net_return_5"] > 0).mean()),
        "excess_hit_rate": float((selected["excess_return_5"] > 0).mean()),
        "mean_path_mfe_5": float(selected["path_mfe_5"].mean()),
        "mean_path_mae_loss_5": float(selected["path_mae_loss_5"].mean()),
        "shortlist_turnover": turnover,
        "modelable_date_coverage": float(
            selected["decision_as_of_trade_date"].nunique()
            / max(expected_date_count, 1)
        ),
    }


def _top_by(frame: pd.DataFrame, column: str, count: int) -> pd.DataFrame:
    available = frame[frame[column].notna()].sort_values(
        ["decision_as_of_trade_date", column, "instrument"],
        ascending=[True, False, True],
    )
    return available.groupby("decision_as_of_trade_date", sort=False).head(count)


def _random_top5(frame: pd.DataFrame, *, seed: int) -> pd.DataFrame:
    groups = []
    for decision, group in frame.groupby("decision_as_of_trade_date", sort=True):
        digest = hashlib.sha256(f"{seed}:{pd.Timestamp(decision).date().isoformat()}".encode("ascii")).digest()
        rng = np.random.default_rng(int.from_bytes(digest[:8], "big"))
        positions = rng.choice(len(group), size=min(5, len(group)), replace=False)
        groups.append(group.iloc[np.sort(positions)])
    return pd.concat(groups, ignore_index=False)


def _shortlist_turnover(frame: pd.DataFrame) -> float:
    sets = [set(group["instrument"].astype(str)) for _, group in frame.groupby("decision_as_of_trade_date", sort=True)]
    if len(sets) <= 1:
        return 0.0
    values = []
    for previous, current in zip(sets[:-1], sets[1:], strict=True):
        denominator = max(len(previous | current), 1)
        values.append(1.0 - len(previous & current) / denominator)
    return float(np.mean(values))


def _dcg(relevance: Sequence[int]) -> float:
    values = np.asarray(relevance, dtype=float)
    if values.size == 0:
        return 0.0
    return float(np.sum((np.power(2.0, values) - 1.0) / np.log2(np.arange(2, len(values) + 2))))
