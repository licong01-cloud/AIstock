from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.quality_contracts import (
    QUALITY_FAMILIES,
    QUALITY_MODEL_WEIGHTS,
    QUALITY_SEEDS,
    QUALITY_WINDOWS,
)


@dataclass(frozen=True)
class TrainedFamilyCandidate:
    window_id: str
    family_id: str
    seeds: tuple[int, ...]
    boosters: tuple[Any, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    validation_predictions: pd.DataFrame
    evaluation_history: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class TournamentResult:
    winner_row: dict[str, Any]
    report: dict[str, Any]
    winning_family: TrainedFamilyCandidate | None


def run_quality_tournament(frame: pd.DataFrame) -> TournamentResult:
    prepared = validate_quality_projection(frame, allowed_splits=("train", "validation"))
    trained: list[TrainedFamilyCandidate] = []
    rows: list[dict[str, Any]] = []
    for window_id in QUALITY_WINDOWS:
        for family_id in QUALITY_FAMILIES:
            candidate = train_family_candidate(
                prepared,
                window_id=window_id,
                family_id=family_id,
            )
            trained.append(candidate)
            for model_weight in QUALITY_MODEL_WEIGHTS:
                scored = apply_ensemble_scores(
                    candidate.validation_predictions,
                    score_columns=tuple(f"raw_score_{seed}" for seed in QUALITY_SEEDS),
                    model_weight=model_weight,
                )
                metrics = evaluate_shortlist(scored)
                rows.append(
                    {
                        "candidate_id": _candidate_id(window_id, family_id, model_weight),
                        "window_id": window_id,
                        "family_id": family_id,
                        "model_weight": model_weight,
                        "seeds": list(QUALITY_SEEDS),
                        **metrics,
                    }
                )

    validation = prepared[prepared["split"] == "validation"].copy()
    prior = apply_ensemble_scores(
        validation,
        score_columns=(),
        model_weight=0.0,
    )
    prior_row = {
        "candidate_id": "SELECTION_PRIOR_ONLY",
        "window_id": "SELECTION_PRIOR_ONLY",
        "family_id": "SELECTION_PRIOR_ONLY",
        "model_weight": 0.0,
        "seeds": [],
        **evaluate_shortlist(prior),
    }
    all_rows = [*rows, prior_row]
    winner = sorted(all_rows, key=_winner_sort_key)[0]
    winning_family = next(
        (item for item in trained if item.window_id == winner["window_id"] and item.family_id == winner["family_id"]),
        None,
    )
    return TournamentResult(
        winner_row=winner,
        winning_family=winning_family,
        report={
            "schema_version": "advisory_reranker_quality_tournament_report_v1",
            "status": (
                "NO_VALIDATION_MODEL_LIFT_OBSERVED" if winner["model_weight"] == 0.0 else "MODEL_WINNER_SELECTED"
            ),
            "trial_count": len(QUALITY_WINDOWS) * len(QUALITY_FAMILIES) * len(QUALITY_SEEDS),
            "family_candidate_count": len(QUALITY_WINDOWS) * len(QUALITY_FAMILIES),
            "weighted_candidate_count": len(rows),
            "winner": winner,
            "candidates": sorted(all_rows, key=lambda item: item["candidate_id"]),
            "training_trials": [
                {
                    "window_id": candidate.window_id,
                    "family_id": candidate.family_id,
                    **history,
                }
                for candidate in trained
                for history in candidate.evaluation_history
            ],
        },
    )


def train_family_candidate(
    frame: pd.DataFrame,
    *,
    window_id: str,
    family_id: str,
) -> TrainedFamilyCandidate:
    if window_id not in QUALITY_WINDOWS or family_id not in QUALITY_FAMILIES:
        raise AdvisoryModelFirstError(
            "M5A trial identity is outside the frozen matrix",
            reason_code="ADVISORY_M5_TRIAL_FAILED",
            context={"window_id": window_id, "family_id": family_id},
        )
    prepared = validate_quality_projection(frame, allowed_splits=("train", "validation"))
    train_dates = tuple(
        sorted(
            pd.Timestamp(value)
            for value in prepared.loc[prepared["split"] == "train", "decision_as_of_trade_date"].unique()
        )
    )
    window_count = {"EXPANDING_ALL": len(train_dates), "ROLLING_160": 160, "ROLLING_120": 120}[window_id]
    if len(train_dates) < window_count:
        raise AdvisoryModelFirstError(
            "M5A training window does not contain the required modelable dates",
            reason_code="ADVISORY_M5_WINDOW_NOT_AVAILABLE",
            context={"window_id": window_id, "available_dates": len(train_dates), "required_dates": window_count},
        )
    selected_dates = set(train_dates[-window_count:])
    train_mask = (prepared["split"] == "train") & prepared["decision_as_of_trade_date"].isin(selected_dates)
    validation_mask = prepared["split"] == "validation"
    matrix, vocabulary = prepare_model_matrix(prepared, train_mask=train_mask)
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable in the WSL M5A training environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"error_type": type(exc).__name__},
        ) from exc

    target_column = "utility_5" if family_id == "REGRESSION_L1_UTILITY5" else "relevance"
    ranking = family_id != "REGRESSION_L1_UTILITY5"
    boosters: list[Any] = []
    histories: list[dict[str, Any]] = []
    predictions = prepared.loc[validation_mask].copy().reset_index(drop=True)
    validation_matrix = matrix.loc[validation_mask].reset_index(drop=True)
    for seed in QUALITY_SEEDS:
        history: dict[str, Any] = {}
        parameters = _lightgbm_parameters(family_id=family_id, seed=seed)
        try:
            train_set = lgb.Dataset(
                matrix.loc[train_mask],
                label=prepared.loc[train_mask, target_column].astype(float if not ranking else int),
                group=_group_sizes(prepared.loc[train_mask]) if ranking else None,
                categorical_feature=list(CATEGORICAL_FEATURE_COLUMNS),
                free_raw_data=False,
                params={"data_random_seed": seed},
            )
            validation_set = lgb.Dataset(
                matrix.loc[validation_mask],
                label=prepared.loc[validation_mask, target_column].astype(float if not ranking else int),
                group=_group_sizes(prepared.loc[validation_mask]) if ranking else None,
                categorical_feature=list(CATEGORICAL_FEATURE_COLUMNS),
                reference=train_set,
                free_raw_data=False,
            )
            booster = lgb.train(
                parameters,
                train_set,
                num_boost_round=600,
                valid_sets=[validation_set],
                valid_names=["validation"],
                callbacks=[
                    lgb.early_stopping(stopping_rounds=60, verbose=False),
                    lgb.record_evaluation(history),
                    lgb.log_evaluation(period=0),
                ],
            )
            raw = np.asarray(
                booster.predict(validation_matrix, num_iteration=booster.best_iteration),
                dtype=float,
            )
        except Exception as exc:
            raise AdvisoryModelFirstError(
                "M5A LightGBM trial failed",
                reason_code="ADVISORY_M5_TRIAL_FAILED",
                context={
                    "window_id": window_id,
                    "family_id": family_id,
                    "seed": seed,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            ) from exc
        if len(raw) != len(predictions) or not np.isfinite(raw).all():
            raise AdvisoryModelFirstError(
                "M5A trial prediction is incomplete or non-finite",
                reason_code="ADVISORY_M5_ENSEMBLE_INCOMPLETE",
                context={"window_id": window_id, "family_id": family_id, "seed": seed},
            )
        boosters.append(booster)
        histories.append({"seed": seed, "best_iteration": int(booster.best_iteration), "history": history})
        predictions[f"raw_score_{seed}"] = raw
    if len(boosters) != len(QUALITY_SEEDS):
        raise AdvisoryModelFirstError(
            "M5A family candidate does not contain all five boosters",
            reason_code="ADVISORY_M5_ENSEMBLE_INCOMPLETE",
        )
    return TrainedFamilyCandidate(
        window_id=window_id,
        family_id=family_id,
        seeds=QUALITY_SEEDS,
        boosters=tuple(boosters),
        categorical_vocabulary=vocabulary,
        validation_predictions=predictions,
        evaluation_history=tuple(histories),
    )


def validate_quality_projection(
    frame: pd.DataFrame,
    *,
    allowed_splits: tuple[str, ...],
) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "split",
        "selection_effective_rank",
        "parent_combined_score",
        "relevance",
        "utility_5",
        "stock_net_return_5",
        "excess_return_5",
        "path_mfe_5",
        "path_mae_loss_5",
        *MODEL_FEATURE_COLUMNS,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise AdvisoryModelFirstError(
            "M5A projection omits required frozen columns",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            context={"missing_columns": missing},
        )
    result = frame.copy()
    result["decision_as_of_trade_date"] = pd.to_datetime(result["decision_as_of_trade_date"]).dt.normalize()
    result["target_trade_date"] = pd.to_datetime(result["target_trade_date"]).dt.normalize()
    actual_splits = tuple(sorted(result["split"].astype(str).unique().tolist()))
    if set(actual_splits) != set(allowed_splits):
        raise AdvisoryModelFirstError(
            "M5A projection contains a forbidden split",
            reason_code=(
                "ADVISORY_M5_TEST_ACCESSED_BEFORE_WINNER_FREEZE"
                if "test" in actual_splits and "test" not in allowed_splits
                else "ADVISORY_M5_INPUT_IDENTITY_MISMATCH"
            ),
            context={"actual_splits": actual_splits, "allowed_splits": allowed_splits},
        )
    result = result.sort_values(["decision_as_of_trade_date", "selection_effective_rank", "instrument"]).reset_index(
        drop=True
    )
    duplicates = result.duplicated(["decision_as_of_trade_date", "instrument"], keep=False)
    if duplicates.any():
        raise AdvisoryModelFirstError(
            "M5A projection contains duplicate candidate identities",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    for split_name in allowed_splits:
        if not (result["split"] == split_name).any():
            raise AdvisoryModelFirstError(
                "M5A projection split has no modelable rows",
                reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
                context={"split": split_name},
            )
    return result


def prepare_model_matrix(
    frame: pd.DataFrame,
    *,
    train_mask: pd.Series,
    categorical_vocabulary: dict[str, tuple[int, ...]] | None = None,
    validate_all_null_train: bool = True,
) -> tuple[pd.DataFrame, dict[str, tuple[int, ...]]]:
    matrix = frame.loc[:, MODEL_FEATURE_COLUMNS].copy()
    for column in MODEL_FEATURE_COLUMNS:
        if column not in CATEGORICAL_FEATURE_COLUMNS:
            try:
                matrix[column] = pd.to_numeric(matrix[column], errors="raise")
            except (TypeError, ValueError) as exc:
                raise AdvisoryModelFirstError(
                    "M5A frozen feature contains a non-numeric value",
                    reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
                    context={"feature": column, "error_type": type(exc).__name__},
                ) from exc
    all_null = (
        [column for column in MODEL_FEATURE_COLUMNS if matrix.loc[train_mask, column].isna().all()]
        if validate_all_null_train
        else []
    )
    if all_null:
        raise AdvisoryModelFirstError(
            "M5A frozen feature is entirely missing in the selected train window",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            context={"features": all_null},
        )
    vocabulary = dict(categorical_vocabulary or {})
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in vocabulary:
            values = pd.to_numeric(matrix.loc[train_mask, column], errors="coerce").dropna().astype(int)
            vocabulary[column] = tuple(sorted(values.unique().tolist()))
        if not vocabulary[column]:
            raise AdvisoryModelFirstError(
                "M5A categorical feature has no training vocabulary",
                reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
                context={"feature": column},
            )
        matrix[column] = pd.Categorical(
            pd.to_numeric(matrix[column], errors="coerce"),
            categories=vocabulary[column],
        )
    return matrix, vocabulary


def apply_ensemble_scores(
    frame: pd.DataFrame,
    *,
    score_columns: Sequence[str],
    model_weight: float,
) -> pd.DataFrame:
    if model_weight not in (*QUALITY_MODEL_WEIGHTS, 0.0):
        raise AdvisoryModelFirstError(
            "M5A model weight differs from the frozen policy",
            reason_code="ADVISORY_M5_RUNTIME_POLICY_MISMATCH",
        )
    result = frame.copy().reset_index(drop=True)
    group_size = result.groupby("decision_as_of_trade_date")["instrument"].transform("size").astype(float)
    result["selection_prior"] = (
        group_size - pd.to_numeric(result["selection_effective_rank"], errors="raise")
    ) / np.maximum(group_size - 1.0, 1.0)
    if model_weight > 0.0:
        if len(score_columns) != len(QUALITY_SEEDS) or any(column not in result for column in score_columns):
            raise AdvisoryModelFirstError(
                "M5A ensemble is missing one or more seed predictions",
                reason_code="ADVISORY_M5_ENSEMBLE_INCOMPLETE",
                context={"score_columns": list(score_columns)},
            )
        percentile_columns = []
        for column in score_columns:
            pct_column = f"pct__{column}"
            result[pct_column] = _group_percentile_rank(result, score_column=column)
            percentile_columns.append(pct_column)
        result["ensemble_score"] = result[percentile_columns].mean(axis=1)
    else:
        result["ensemble_score"] = 0.0
    result["advisory_model_score"] = (
        model_weight * result["ensemble_score"] + (1.0 - model_weight) * result["selection_prior"]
    )
    result = result.sort_values(
        ["decision_as_of_trade_date", "advisory_model_score", "instrument"],
        ascending=[True, False, True],
    )
    result["advisory_model_rank"] = result.groupby("decision_as_of_trade_date").cumcount().add(1)
    return result.reset_index(drop=True)


def evaluate_shortlist(
    scored: pd.DataFrame,
    *,
    selection_reference: pd.DataFrame | None = None,
) -> dict[str, Any]:
    top5 = scored[scored["advisory_model_rank"] <= 5].copy()
    if top5.empty:
        raise AdvisoryModelFirstError(
            "M5A validation shortlist is empty",
            reason_code="ADVISORY_M5_TRIAL_FAILED",
        )
    daily = top5.groupby("decision_as_of_trade_date", sort=True)["excess_return_5"].mean()
    selection_source = scored if selection_reference is None else selection_reference
    selection_daily = (
        selection_source[selection_source["selection_effective_rank"] <= 5]
        .groupby("decision_as_of_trade_date", sort=True)["excess_return_5"]
        .mean()
    )
    aligned = daily.to_frame("model").join(selection_daily.rename("selection"), how="inner")
    ndcg_values = []
    for _, group in scored.groupby("decision_as_of_trade_date", sort=True):
        actual = group.sort_values(["advisory_model_score", "instrument"], ascending=[False, True])
        ideal = group.sort_values(["relevance", "instrument"], ascending=[False, True])
        ideal_dcg = _dcg(ideal["relevance"].to_numpy(dtype=int)[:5])
        ndcg_values.append(_dcg(actual["relevance"].to_numpy(dtype=int)[:5]) / ideal_dcg if ideal_dcg else 0.0)
    raw_columns = [column for column in scored if column.startswith("raw_score_")]
    spearman = _ranking_stability(scored, raw_columns)
    return {
        "mean_daily_top5_excess_return_5": float(daily.mean()),
        "median_daily_top5_excess_return_5": float(daily.median()),
        "mean_stock_net_return_5": float(top5["stock_net_return_5"].mean()),
        "absolute_hit_rate": float((top5["stock_net_return_5"] > 0).mean()),
        "excess_hit_rate": float((top5["excess_return_5"] > 0).mean()),
        "date_level_ndcg_at_5": float(np.mean(ndcg_values)),
        "shortlist_turnover": _shortlist_turnover(top5),
        "seed_spearman_mean": spearman[0],
        "seed_spearman_worst": spearman[1],
        "mean_daily_lift_vs_selection_rank": float((aligned["model"] - aligned["selection"]).mean()),
        "median_daily_lift_vs_selection_rank": float((aligned["model"] - aligned["selection"]).median()),
        "date_count": int(daily.size),
        "row_count": int(len(top5)),
    }


def _lightgbm_parameters(*, family_id: str, seed: int) -> dict[str, Any]:
    objective_metric = {
        "LAMBDARANK_NDCG5": ("lambdarank", "ndcg"),
        "RANK_XENDCG5": ("rank_xendcg", "ndcg"),
        "REGRESSION_L1_UTILITY5": ("regression_l1", "l1"),
    }
    objective, metric = objective_metric[family_id]
    params: dict[str, Any] = {
        "objective": objective,
        "metric": metric,
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
    if objective in {"lambdarank", "rank_xendcg"}:
        params.update({"eval_at": [5], "label_gain": [0, 1, 3, 7, 15]})
    return params


def _group_percentile_rank(frame: pd.DataFrame, *, score_column: str) -> pd.Series:
    output = pd.Series(index=frame.index, dtype=float)
    for _, group in frame.groupby("decision_as_of_trade_date", sort=False):
        ordered = group.sort_values([score_column, "instrument"], ascending=[False, True])
        denominator = max(len(ordered) - 1, 1)
        output.loc[ordered.index] = 1.0 - np.arange(len(ordered), dtype=float) / denominator
    if output.isna().any() or not np.isfinite(output.to_numpy(dtype=float)).all():
        raise AdvisoryModelFirstError(
            "M5A percentile ensemble produced an invalid score",
            reason_code="ADVISORY_M5_ENSEMBLE_INCOMPLETE",
        )
    return output


def _winner_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -float(row["mean_daily_top5_excess_return_5"]),
        -float(row["median_daily_top5_excess_return_5"]),
        -float(row["excess_hit_rate"]),
        float(row["shortlist_turnover"]),
        str(row["window_id"]),
        str(row["family_id"]),
        float(row["model_weight"]),
    )


def _candidate_id(window_id: str, family_id: str, model_weight: float) -> str:
    return f"{window_id}__{family_id}__MW_{model_weight:.2f}"


def _group_sizes(frame: pd.DataFrame) -> list[int]:
    return frame.groupby("decision_as_of_trade_date", sort=False).size().astype(int).tolist()


def _dcg(relevance: Sequence[int]) -> float:
    values = np.asarray(relevance, dtype=float)
    return float(np.sum((np.power(2.0, values) - 1.0) / np.log2(np.arange(2, len(values) + 2))))


def _ranking_stability(frame: pd.DataFrame, columns: list[str]) -> tuple[float | None, float | None]:
    if len(columns) < 2:
        return None, None
    correlations: list[float] = []
    for _, group in frame.groupby("decision_as_of_trade_date", sort=True):
        ranks: dict[str, np.ndarray] = {}
        for column in columns:
            ordered = group.sort_values([column, "instrument"], ascending=[False, True])
            rank_by_index = {index: rank for rank, index in enumerate(ordered.index, start=1)}
            ranks[column] = np.asarray([rank_by_index[index] for index in group.index], dtype=float)
        for first, second in itertools.combinations(columns, 2):
            value = np.corrcoef(ranks[first], ranks[second])[0, 1]
            if np.isfinite(value):
                correlations.append(float(value))
    if not correlations:
        return None, None
    return float(np.mean(correlations)), float(np.min(correlations))


def _shortlist_turnover(frame: pd.DataFrame) -> float:
    groups = [
        set(group["instrument"].astype(str)) for _, group in frame.groupby("decision_as_of_trade_date", sort=True)
    ]
    if len(groups) <= 1:
        return 0.0
    values = [
        1.0 - len(previous & current) / max(len(previous | current), 1)
        for previous, current in zip(groups[:-1], groups[1:], strict=True)
    ]
    return float(np.mean(values))
