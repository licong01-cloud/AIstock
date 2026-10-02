from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v2 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
    REQUIRED_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.meta_label_training import HMM_FEATURES
from backend.services.advisory_model_first.policy_utility_contracts import PolicyUtilityFamilySpecV1
from backend.services.advisory_model_first.policy_utility_training import (
    PolicyUtilityTransformFit,
    apply_policy_utility_transform,
    fit_policy_utility_transform,
    inverse_policy_utility_transform,
    rank_policy_predictions,
)
from backend.services.advisory_model_first.turnover_constrained_utility_contracts import (
    TURNOVER_SHADOW_PRICE_MULTIPLIERS,
    TurnoverConstrainedUtilityFamilySpecV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SCORE_COLUMN = "predicted_turnover_constrained_utility_bps"
TARGET_COLUMN = "turnover_constrained_policy_utility_bps"
LIABILITY_COLUMN = "turnover_liability_fraction_per_day"


@dataclass(frozen=True)
class ShadowPriceScaleFit:
    utility_location_bps: float
    utility_scale_bps: float
    liability_location: float
    liability_scale: float
    shadow_price_base_bps_per_fraction: float
    candidates_bps_per_fraction: tuple[float, ...]


@dataclass(frozen=True)
class ShadowPriceSelection:
    shadow_price_bps_per_fraction: float
    p0d_train_turnover_budget: float
    oracle_train_turnover: float
    constraint_slack: float
    candidate_turnover_by_price: dict[str, float]


@dataclass(frozen=True)
class TurnoverConstrainedUtilityTrainingResult:
    booster: Any
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    best_iteration: int
    evaluation_history: dict[str, Any]
    validation_predictions: pd.DataFrame
    metrics: dict[str, Any]
    transform: PolicyUtilityTransformFit
    shadow_price_bps_per_fraction: float


@dataclass(frozen=True)
class FinalTurnoverConstrainedUtilityTrainingResult:
    booster: Any
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    boost_rounds: int
    transform: PolicyUtilityTransformFit
    shadow_price_bps_per_fraction: float


def turnover_utility_feature_names(
    family: TurnoverConstrainedUtilityFamilySpecV1,
) -> tuple[str, ...]:
    return tuple(column for column in MODEL_FEATURE_COLUMNS if family.include_hmm or column not in HMM_FEATURES)


def complete_matured_decision_dates(
    labels: pd.DataFrame, *, expected_candidates_per_date: int = 20
) -> tuple[pd.DatetimeIndex, dict[str, Any]]:
    required = {"decision_as_of_trade_date", "instrument", "label_status"}
    if not required.issubset(labels):
        raise _error("turnover utility label coverage columns are missing", missing=sorted(required - set(labels)))
    rows = labels.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    if rows.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise _coverage_error("turnover utility label coverage contains duplicate candidates")
    grouped = rows.groupby("decision_as_of_trade_date", sort=True)
    counts = grouped.size()
    matured = grouped["label_status"].apply(lambda values: bool((values == "MATURED").all()))
    complete = counts.eq(expected_candidates_per_date) & matured
    dates = pd.DatetimeIndex(complete[complete].index).normalize()
    excluded = pd.DatetimeIndex(complete[~complete].index).normalize()
    receipt = {
        "schema_version": "advisory_turnover_utility_calibration_coverage_v1",
        "expected_candidates_per_date": expected_candidates_per_date,
        "complete_matured_decision_count": int(len(dates)),
        "excluded_decision_count": int(len(excluded)),
        "excluded_decision_dates_sha256": canonical_json_sha256(
            [value.date().isoformat() for value in excluded]
        ),
        "label_status_counts": {
            str(key): int(value) for key, value in rows["label_status"].value_counts().sort_index().items()
        },
    }
    if dates.empty:
        raise _coverage_error("turnover utility has no exact-20 matured calibration date")
    return dates, receipt


def add_turnover_constrained_targets(
    labels: pd.DataFrame,
    *,
    target_count: int,
    shadow_price_bps_per_fraction: float,
    turnover_action_count: int = 2,
) -> pd.DataFrame:
    required = {
        "label_status",
        "holding_trading_days",
        "net_excess_return_bps",
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
    }
    if not required.issubset(labels):
        raise _error("turnover utility label input is incomplete", missing=sorted(required - set(labels)))
    if target_count <= 0 or turnover_action_count != 2:
        raise _error("turnover utility policy units are invalid")
    if not np.isfinite(shadow_price_bps_per_fraction) or shadow_price_bps_per_fraction < 0.0:
        raise _error("turnover shadow price is invalid")
    result = labels.copy()
    result[LIABILITY_COLUMN] = np.nan
    result[TARGET_COLUMN] = np.nan
    matured = result["label_status"] == "MATURED"
    holding = pd.to_numeric(result.loc[matured, "holding_trading_days"], errors="coerce").to_numpy(float)
    utility = pd.to_numeric(result.loc[matured, "net_excess_return_bps"], errors="coerce").to_numpy(float)
    if not len(holding) or not np.isfinite(holding).all() or (holding < 1.0).any():
        raise _error("turnover utility matured holding labels are invalid")
    if not np.isfinite(utility).all():
        raise _error("turnover utility matured return labels are invalid")
    liability = turnover_action_count / (float(target_count) * holding)
    adjusted = utility - shadow_price_bps_per_fraction * liability
    if not np.isfinite(liability).all() or not np.isfinite(adjusted).all():
        raise _error("turnover utility adjusted labels are invalid")
    result.loc[matured, LIABILITY_COLUMN] = liability
    result.loc[matured, TARGET_COLUMN] = adjusted
    return result


def fit_shadow_price_scale(
    labels: pd.DataFrame,
    *,
    target_count: int,
    multipliers: Sequence[float] = TURNOVER_SHADOW_PRICE_MULTIPLIERS,
) -> ShadowPriceScaleFit:
    prepared = add_turnover_constrained_targets(
        labels,
        target_count=target_count,
        shadow_price_bps_per_fraction=0.0,
    )
    matured = prepared["label_status"] == "MATURED"
    utility = prepared.loc[matured, "net_excess_return_bps"].to_numpy(float)
    liability = prepared.loc[matured, LIABILITY_COLUMN].to_numpy(float)
    utility_location = float(np.median(utility))
    utility_scale = float(np.median(np.abs(utility - utility_location)))
    liability_location = float(np.median(liability))
    liability_scale = float(np.median(np.abs(liability - liability_location)))
    if (
        not np.isfinite([utility_location, utility_scale, liability_location, liability_scale]).all()
        or utility_scale <= 0.0
        or liability_scale <= 0.0
    ):
        raise _scale_error(
            "turnover utility shadow-price scale is invalid",
            utility_scale_bps=utility_scale,
            liability_scale=liability_scale,
        )
    normalized_multipliers = tuple(float(value) for value in multipliers)
    if normalized_multipliers != TURNOVER_SHADOW_PRICE_MULTIPLIERS:
        raise _scale_error("turnover utility shadow-price multiplier roster is invalid")
    base = utility_scale / liability_scale
    candidates = tuple(base * value for value in normalized_multipliers)
    if not np.isfinite(candidates).all() or tuple(sorted(candidates)) != candidates:
        raise _scale_error("turnover utility shadow-price candidates are invalid")
    return ShadowPriceScaleFit(
        utility_location_bps=utility_location,
        utility_scale_bps=utility_scale,
        liability_location=liability_location,
        liability_scale=liability_scale,
        shadow_price_base_bps_per_fraction=base,
        candidates_bps_per_fraction=candidates,
    )


def select_minimum_feasible_shadow_price(
    *,
    scale_fit: ShadowPriceScaleFit,
    p0d_train_turnover_budget: float,
    evaluate_oracle_turnover: Callable[[float], float],
) -> ShadowPriceSelection:
    if not np.isfinite(p0d_train_turnover_budget) or p0d_train_turnover_budget < 0.0:
        raise _constraint_error("exact P0-D train turnover budget is invalid")
    observed: dict[str, float] = {}
    for price in scale_fit.candidates_bps_per_fraction:
        turnover = float(evaluate_oracle_turnover(price))
        if not np.isfinite(turnover) or turnover < 0.0:
            raise _constraint_error("oracle train turnover is invalid", shadow_price=price)
        observed[format(price, ".17g")] = turnover
        if turnover <= p0d_train_turnover_budget + 1e-15:
            return ShadowPriceSelection(
                shadow_price_bps_per_fraction=price,
                p0d_train_turnover_budget=p0d_train_turnover_budget,
                oracle_train_turnover=turnover,
                constraint_slack=p0d_train_turnover_budget - turnover,
                candidate_turnover_by_price=observed,
            )
    raise _constraint_error(
        "approved shadow-price roster cannot satisfy exact P0-D train turnover budget",
        p0d_train_turnover_budget=p0d_train_turnover_budget,
        candidate_turnover_by_price=observed,
    )


def rank_turnover_utility_predictions(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        SCORE_COLUMN,
    }
    if not required.issubset(frame):
        raise _priority_error("turnover utility rank input is incomplete", missing=sorted(required - set(frame)))
    result = frame.copy()
    result["decision_as_of_trade_date"] = pd.to_datetime(result["decision_as_of_trade_date"]).dt.normalize()
    result["selection_effective_rank"] = pd.to_numeric(result["selection_effective_rank"], errors="coerce")
    result[SCORE_COLUMN] = pd.to_numeric(result[SCORE_COLUMN], errors="coerce")
    counts = result.groupby("decision_as_of_trade_date").size()
    expected = tuple(range(1, 21))
    ranks = result.groupby("decision_as_of_trade_date")["selection_effective_rank"].apply(
        lambda values: tuple(sorted(values.tolist()))
    )
    if counts.empty or not counts.eq(20).all() or not ranks.map(lambda values: values == expected).all():
        raise _priority_error("turnover utility ranking requires exact Selection ranks 1..20")
    if result.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise _priority_error("turnover utility ranking contains duplicate candidates")
    if not np.isfinite(result[SCORE_COLUMN].to_numpy(float)).all():
        raise _priority_error("turnover utility ranking contains a non-finite score")
    result = result.sort_values(
        ["decision_as_of_trade_date", SCORE_COLUMN, "selection_effective_rank", "instrument"],
        ascending=[True, False, True, True],
    )
    result["entry_priority_rank"] = result.groupby("decision_as_of_trade_date").cumcount().add(1)
    result["selection_exit_rank"] = result["selection_effective_rank"]
    result["entry_priority_score_kind"] = "TURNOVER_CONSTRAINED_POLICY_UTILITY_BPS"
    return result.reset_index(drop=True)


def train_turnover_constrained_utility_trial(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    validation_dates: Sequence[pd.Timestamp],
    family: TurnoverConstrainedUtilityFamilySpecV1,
    seed: int,
    target_count: int,
    shadow_price_bps_per_fraction: float,
) -> TurnoverConstrainedUtilityTrainingResult:
    adjusted = add_turnover_constrained_targets(
        labels,
        target_count=target_count,
        shadow_price_bps_per_fraction=shadow_price_bps_per_fraction,
    )
    merged, matrix, vocabulary, feature_names, train_mask, validation_mask, scoring_mask = _prepare_rows(
        features=features,
        labels=adjusted,
        train_dates=train_dates,
        validation_dates=validation_dates,
        family=family,
    )
    transform = fit_policy_utility_transform(merged.loc[train_mask, TARGET_COLUMN])
    y_train = apply_policy_utility_transform(merged.loc[train_mask, TARGET_COLUMN], transform)
    y_validation = apply_policy_utility_transform(merged.loc[validation_mask, TARGET_COLUMN], transform)
    lgb = _lightgbm()
    train_data = lgb.Dataset(
        matrix.loc[train_mask],
        label=y_train,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        free_raw_data=False,
    )
    validation_data = lgb.Dataset(
        matrix.loc[validation_mask],
        label=y_validation,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        reference=train_data,
        free_raw_data=False,
    )
    history: dict[str, Any] = {}
    booster = lgb.train(
        _training_params(family, seed),
        train_data,
        num_boost_round=family.max_boost_rounds,
        valid_sets=[validation_data],
        valid_names=["validation"],
        callbacks=[
            lgb.early_stopping(family.early_stopping_rounds, verbose=False),
            lgb.record_evaluation(history),
        ],
    )
    standardized = booster.predict(matrix.loc[scoring_mask], num_iteration=booster.best_iteration)
    predictions = inverse_policy_utility_transform(standardized, transform)
    columns = [
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        "label_status",
        "holding_trading_days",
        "net_excess_return_bps",
        TARGET_COLUMN,
        LIABILITY_COLUMN,
    ]
    validation = merged.loc[scoring_mask, columns].copy()
    validation[SCORE_COLUMN] = predictions
    validation["turnover_shadow_price_bps_per_fraction"] = shadow_price_bps_per_fraction
    validation = rank_turnover_utility_predictions(validation)
    return TurnoverConstrainedUtilityTrainingResult(
        booster=booster,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        best_iteration=int(booster.best_iteration),
        evaluation_history=history,
        validation_predictions=validation,
        metrics=turnover_utility_candidate_metrics(validation),
        transform=transform,
        shadow_price_bps_per_fraction=shadow_price_bps_per_fraction,
    )


def train_final_turnover_constrained_utility(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    family: TurnoverConstrainedUtilityFamilySpecV1,
    seed: int,
    boost_rounds: int,
    target_count: int,
    shadow_price_bps_per_fraction: float,
) -> FinalTurnoverConstrainedUtilityTrainingResult:
    adjusted = add_turnover_constrained_targets(
        labels,
        target_count=target_count,
        shadow_price_bps_per_fraction=shadow_price_bps_per_fraction,
    )
    dates = pd.to_datetime(adjusted.loc[adjusted["label_status"] == "MATURED", "decision_as_of_trade_date"])
    merged, matrix, vocabulary, feature_names, train_mask, _, _ = _prepare_rows(
        features=features,
        labels=adjusted,
        train_dates=dates,
        validation_dates=dates,
        family=family,
    )
    transform = fit_policy_utility_transform(merged.loc[train_mask, TARGET_COLUMN])
    target = apply_policy_utility_transform(merged.loc[train_mask, TARGET_COLUMN], transform)
    lgb = _lightgbm()
    dataset = lgb.Dataset(
        matrix.loc[train_mask],
        label=target,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        free_raw_data=False,
    )
    booster = lgb.train(_training_params(family, seed), dataset, num_boost_round=boost_rounds)
    return FinalTurnoverConstrainedUtilityTrainingResult(
        booster=booster,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        boost_rounds=boost_rounds,
        transform=transform,
        shadow_price_bps_per_fraction=shadow_price_bps_per_fraction,
    )


def score_final_turnover_constrained_utility(
    *,
    features: pd.DataFrame,
    model: FinalTurnoverConstrainedUtilityTrainingResult,
    score_dates: Sequence[pd.Timestamp],
) -> pd.DataFrame:
    """Score a frozen P0-G refit without fitting or outer-validation early stopping."""
    dates = set(pd.DatetimeIndex(pd.to_datetime(list(score_dates))).normalize())
    rows = features.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(
        rows["decision_as_of_trade_date"]
    ).dt.normalize()
    rows["target_trade_date"] = pd.to_datetime(rows["target_trade_date"]).dt.normalize()
    rows["instrument"] = rows["instrument"].astype(str).str.upper()
    rows = rows[rows["decision_as_of_trade_date"].isin(dates)].copy()
    counts = rows.groupby("decision_as_of_trade_date").size()
    if counts.empty or set(counts.index) != dates or not counts.eq(20).all():
        raise _reference_error("exact P0-G final scoring is not exact Top20")
    names = tuple(str(value) for value in model.feature_names)
    if tuple(model.booster.feature_name()) != names or not set(names).issubset(rows):
        raise _reference_error("exact P0-G final scoring feature identity is invalid")
    matrix = rows.loc[:, names].copy()
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in matrix:
            continue
        categories = tuple(int(value) for value in model.categorical_vocabulary.get(column, ()))
        if not categories:
            raise _reference_error(
                "exact P0-G final scoring categorical vocabulary is empty",
                feature=column,
            )
        numeric = pd.to_numeric(matrix[column], errors="coerce")
        unseen = numeric.notna() & ~numeric.isin(categories)
        if unseen.any():
            missing_indicator = f"{column}__missing"
            if missing_indicator not in matrix:
                raise _reference_error(
                    "exact P0-G final scoring missing indicator is absent",
                    feature=column,
                )
            matrix.loc[unseen, missing_indicator] = 1
            numeric = numeric.mask(unseen)
        matrix[column] = pd.Categorical(numeric, categories=categories)
    standardized = np.asarray(model.booster.predict(matrix), dtype=float)
    predictions = inverse_policy_utility_transform(standardized, model.transform)
    if not np.isfinite(predictions).all():
        raise _reference_error("exact P0-G final predictions are invalid")
    result = rows.loc[
        :, ["decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank"]
    ].copy()
    result[SCORE_COLUMN] = predictions
    result["turnover_shadow_price_bps_per_fraction"] = (
        model.shadow_price_bps_per_fraction
    )
    return rank_turnover_utility_predictions(result)


def train_fixed_p0d_reference_predictions(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    score_dates: Sequence[pd.Timestamp],
    family: PolicyUtilityFamilySpecV1,
    seed: int,
    boost_rounds: int,
) -> pd.DataFrame:
    if boost_rounds < 1:
        raise _reference_error("exact P0-D reference boost rounds are invalid")
    merged, matrix, _, feature_names, train_mask, scoring_mask = _prepare_binary_reference_rows(
        features=features,
        labels=labels,
        train_dates=train_dates,
        score_dates=score_dates,
        family=family,
    )
    target = merged.loc[train_mask, "take_label"].astype(int).to_numpy()
    if len(np.unique(target)) < 2:
        raise _reference_error("exact P0-D reference train labels contain a single class")
    lgb = _lightgbm()
    dataset = lgb.Dataset(
        matrix.loc[train_mask],
        label=target,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        free_raw_data=False,
    )
    booster = lgb.train(
        _binary_reference_training_params(family, seed),
        dataset,
        num_boost_round=boost_rounds,
    )
    probability = np.asarray(booster.predict(matrix.loc[scoring_mask]), dtype=float)
    if not np.isfinite(probability).all() or ((probability < 0.0) | (probability > 1.0)).any():
        raise _reference_error("exact P0-D reference predictions are invalid")
    predictions = merged.loc[
        scoring_mask,
        ["decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank"],
    ].copy()
    predictions["take_probability"] = probability
    ranked = rank_policy_predictions(predictions, score_column="take_probability")
    if tuple(booster.feature_name()) != feature_names:
        raise _reference_error("exact P0-D reference feature order drifted")
    return ranked


def score_exact_p0d_reference_booster(
    *,
    features: pd.DataFrame,
    booster: Any,
    feature_names: Sequence[str],
    categorical_vocabulary: Mapping[str, Sequence[int]],
    score_dates: Sequence[pd.Timestamp],
) -> pd.DataFrame:
    dates = set(pd.DatetimeIndex(pd.to_datetime(list(score_dates))).normalize())
    rows = features.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    rows = rows[rows["decision_as_of_trade_date"].isin(dates)].copy()
    counts = rows.groupby("decision_as_of_trade_date").size()
    if counts.empty or not counts.eq(20).all():
        raise _reference_error("exact P0-D final reference scoring is not exact Top20")
    names = tuple(str(value) for value in feature_names)
    if tuple(booster.feature_name()) != names or not set(names).issubset(rows):
        raise _reference_error("exact P0-D final reference feature identity is invalid")
    matrix = rows.loc[:, names].copy()
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in matrix:
            continue
        categories = tuple(int(value) for value in categorical_vocabulary.get(column, ()))
        if not categories:
            raise _reference_error("exact P0-D final reference categorical vocabulary is empty", feature=column)
        numeric = pd.to_numeric(matrix[column], errors="coerce")
        unseen = numeric.notna() & ~numeric.isin(categories)
        if unseen.any():
            missing_indicator = f"{column}__missing"
            if missing_indicator not in matrix:
                raise _reference_error("exact P0-D final reference missing indicator is absent", feature=column)
            matrix.loc[unseen, missing_indicator] = 1
            numeric = numeric.mask(unseen)
        matrix[column] = pd.Categorical(numeric, categories=categories)
    probability = np.asarray(booster.predict(matrix), dtype=float)
    if not np.isfinite(probability).all() or ((probability < 0.0) | (probability > 1.0)).any():
        raise _reference_error("exact P0-D final reference predictions are invalid")
    predictions = rows.loc[
        :, ["decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank"]
    ].copy()
    predictions["take_probability"] = probability
    return rank_policy_predictions(predictions, score_column="take_probability")


def turnover_utility_candidate_metrics(predictions: pd.DataFrame) -> dict[str, Any]:
    scored = predictions[predictions["label_status"] == "MATURED"].copy()
    if scored.empty:
        raise _error("turnover utility candidate metrics have no matured rows")
    actual_adjusted = scored[TARGET_COLUMN].to_numpy(float)
    actual_raw = scored["net_excess_return_bps"].to_numpy(float)
    predicted = scored[SCORE_COLUMN].to_numpy(float)
    if not np.isfinite(actual_adjusted).all() or not np.isfinite(actual_raw).all() or not np.isfinite(predicted).all():
        raise _error("turnover utility candidate metrics contain non-finite values")
    daily_adjusted_spearman = scored.groupby("decision_as_of_trade_date", sort=True).apply(
        lambda group: group[SCORE_COLUMN].corr(group[TARGET_COLUMN], method="spearman"),
        include_groups=False,
    )
    daily_raw_spearman = scored.groupby("decision_as_of_trade_date", sort=True).apply(
        lambda group: group[SCORE_COLUMN].corr(group["net_excess_return_bps"], method="spearman"),
        include_groups=False,
    )
    spread = scored.groupby("decision_as_of_trade_date", sort=True).apply(
        lambda group: group.loc[group["entry_priority_rank"] <= 5, "net_excess_return_bps"].mean()
        - group.loc[group["entry_priority_rank"] > 5, "net_excess_return_bps"].mean(),
        include_groups=False,
    )
    holding_buckets = (
        scored.assign(
            holding_bucket=pd.cut(
                scored["holding_trading_days"],
                bins=[0, 3, 5, 10, 20],
                labels=["1-3", "4-5", "6-10", "11-20"],
            )
        )
        .groupby("holding_bucket", observed=True)
        .agg(
            row_count=("instrument", "size"),
            mean_raw_utility_bps=("net_excess_return_bps", "mean"),
            mean_adjusted_utility_bps=(TARGET_COLUMN, "mean"),
            mean_entry_priority_rank=("entry_priority_rank", "mean"),
        )
        .reset_index()
        .to_dict("records")
    )
    return {
        "candidate_adjusted_mae_bps": float(mean_absolute_error(actual_adjusted, predicted)),
        "candidate_adjusted_rmse_bps": float(mean_squared_error(actual_adjusted, predicted) ** 0.5),
        "daily_adjusted_spearman_mean": _finite_mean(daily_adjusted_spearman),
        "daily_raw_utility_spearman_mean": _finite_mean(daily_raw_spearman),
        "top5_vs_rest_raw_utility_spread_bps": float(spread.mean()),
        "holding_bucket_attribution": holding_buckets,
    }


def _prepare_rows(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    validation_dates: Sequence[pd.Timestamp],
    family: TurnoverConstrainedUtilityFamilySpecV1,
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
    scoring_mask = merged["decision_as_of_trade_date"].isin(validation_set)
    if not train_mask.any() or not validation_mask.any():
        raise _error("turnover utility path has no matured train or validation rows")
    counts = merged.loc[scoring_mask].groupby("decision_as_of_trade_date").size()
    if counts.empty or not counts.eq(20).all():
        raise _priority_error("turnover utility validation does not contain exact Selection Top20")
    feature_names = turnover_utility_feature_names(family)
    missing = sorted(set(feature_names) - set(merged))
    if missing:
        raise _error("turnover utility feature matrix is incomplete", missing_columns=missing)
    matrix = merged.loc[:, feature_names].copy()
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    required = [column for column in feature_names if column in REQUIRED_FEATURE_COLUMNS]
    all_null = [column for column in required if matrix.loc[train_mask, column].isna().all()]
    if all_null:
        raise _error("turnover utility train features are entirely missing", features=all_null)
    vocabulary: dict[str, tuple[int, ...]] = {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in matrix:
            continue
        categories = tuple(sorted(matrix.loc[train_mask, column].dropna().astype(int).unique()))
        if not categories:
            raise _error("turnover utility categorical feature has no train vocabulary", feature=column)
        vocabulary[column] = categories
        matrix[column] = pd.Categorical(matrix[column], categories=categories)
    for mask, label in ((train_mask, "train"), (validation_mask, "validation")):
        values = merged.loc[mask, TARGET_COLUMN].to_numpy(float)
        if not np.isfinite(values).all():
            raise _error(f"turnover utility {label} adjusted target is invalid")
    return merged, matrix, vocabulary, feature_names, train_mask, validation_mask, scoring_mask


def _prepare_binary_reference_rows(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    score_dates: Sequence[pd.Timestamp],
    family: PolicyUtilityFamilySpecV1,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, tuple[int, ...]], tuple[str, ...], pd.Series, pd.Series]:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    merged = features.merge(labels, on=keys, how="inner", validate="one_to_one", suffixes=("", "_label"))
    merged["decision_as_of_trade_date"] = pd.to_datetime(merged["decision_as_of_trade_date"]).dt.normalize()
    train_set = set(pd.DatetimeIndex(pd.to_datetime(list(train_dates))).normalize())
    score_set = set(pd.DatetimeIndex(pd.to_datetime(list(score_dates))).normalize())
    train_mask = merged["decision_as_of_trade_date"].isin(train_set) & (merged["label_status"] == "MATURED")
    scoring_mask = merged["decision_as_of_trade_date"].isin(score_set)
    if not train_mask.any() or not scoring_mask.any():
        raise _reference_error("exact P0-D reference has no train or score rows")
    counts = merged.loc[scoring_mask].groupby("decision_as_of_trade_date").size()
    if counts.empty or not counts.eq(20).all():
        raise _reference_error("exact P0-D reference score dates are not exact Top20")
    feature_names = tuple(column for column in MODEL_FEATURE_COLUMNS if family.include_hmm or column not in HMM_FEATURES)
    missing = sorted(set(feature_names) - set(merged))
    if missing:
        raise _reference_error("exact P0-D reference feature matrix is incomplete", missing=missing)
    matrix = merged.loc[:, feature_names].copy()
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    vocabulary: dict[str, tuple[int, ...]] = {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in matrix:
            continue
        categories = tuple(sorted(matrix.loc[train_mask, column].dropna().astype(int).unique()))
        if not categories:
            raise _reference_error("exact P0-D reference categorical vocabulary is empty", feature=column)
        vocabulary[column] = categories
        matrix[column] = pd.Categorical(matrix[column], categories=categories)
    return merged, matrix, vocabulary, feature_names, train_mask, scoring_mask


def _training_params(family: TurnoverConstrainedUtilityFamilySpecV1, seed: int) -> dict[str, Any]:
    return {
        "objective": "huber",
        "metric": "l1",
        "alpha": 0.9,
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


def _binary_reference_training_params(family: PolicyUtilityFamilySpecV1, seed: int) -> dict[str, Any]:
    return {
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


def _lightgbm() -> Any:
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable in WSL",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        ) from exc
    return lgb


def _finite_mean(values: pd.Series) -> float | None:
    finite = pd.to_numeric(values, errors="coerce")
    finite = finite[np.isfinite(finite)]
    return float(finite.mean()) if len(finite) else None


def _error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_TURNOVER_UTILITY_MODEL_FAILED",
        context=context or None,
    )


def _scale_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_TURNOVER_UTILITY_SCALE_INVALID",
        context=context or None,
    )


def _constraint_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_TURNOVER_UTILITY_CONSTRAINT_INFEASIBLE",
        context=context or None,
    )


def _coverage_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_TURNOVER_UTILITY_CALIBRATION_COVERAGE_INVALID",
        context=context or None,
    )


def _priority_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_TURNOVER_UTILITY_PRIORITY_INVALID",
        context=context or None,
    )


def _reference_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH",
        context=context or None,
    )
