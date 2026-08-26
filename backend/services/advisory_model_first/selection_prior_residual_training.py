from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

from backend.services.advisory_model_first.selection_prior_residual_contracts import (
    SELECTION_PRIOR_RANK_COUNT,
    SELECTION_PRIOR_RELIABILITY_DENOMINATOR_EPSILON,
    SELECTION_PRIOR_RESIDUAL_SHADOW_PRICE_MULTIPLIERS,
    SelectionPriorResidualFamilySpecV1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v2 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
    REQUIRED_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.meta_label_training import HMM_FEATURES
from backend.services.advisory_model_first.policy_cpcv import _embargo_dates, _information_overlap
from backend.services.advisory_model_first.policy_utility_training import (
    PolicyUtilityTransformFit,
    apply_policy_utility_transform,
    fit_policy_utility_transform,
    inverse_policy_utility_transform,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


RETURN_TARGET_COLUMN = "net_excess_return_bps"
RESIDUAL_TARGET_COLUMN = "selection_prior_residual_bps"
LIABILITY_TARGET_COLUMN = "turnover_liability_fraction_per_day"
SELECTION_PRIOR_SCORE_COLUMN = "selection_prior_bps"
RESIDUAL_SCORE_COLUMN = "predicted_selection_prior_residual_bps"
RESIDUAL_ALPHA_COLUMN = "selection_prior_residual_alpha"
RETURN_SCORE_COLUMN = "predicted_selection_prior_anchored_return_bps"
LIABILITY_SCORE_COLUMN = "predicted_turnover_liability_fraction_per_day"
COMBINED_SCORE_COLUMN = "predicted_selection_prior_residual_output_constrained_utility_bps"


@dataclass(frozen=True)
class InnerFoldSpec:
    block_id: int
    train_dates: tuple[pd.Timestamp, ...]
    validation_dates: tuple[pd.Timestamp, ...]
    score_dates: tuple[pd.Timestamp, ...]
    purged_dates: tuple[pd.Timestamp, ...]
    embargo_dates: tuple[pd.Timestamp, ...]


@dataclass(frozen=True)
class HeadFoldResult:
    predictions: pd.DataFrame
    best_iteration: int
    transform: PolicyUtilityTransformFit
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    metrics: dict[str, float | None]


@dataclass(frozen=True)
class SelectionRankPriorFit:
    ranks: tuple[int, ...]
    rank_locations_bps: tuple[float, ...]
    rank_weights: tuple[int, ...]
    prior_values_bps: tuple[float, ...]
    prior_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "advisory_selection_rank_prior_v1",
            "method": "RANK_MEDIAN_COUNT_WEIGHTED_DECREASING_ISOTONIC_V1",
            "ranks": list(self.ranks),
            "rank_locations_bps": list(self.rank_locations_bps),
            "rank_weights": list(self.rank_weights),
            "prior_values_bps": list(self.prior_values_bps),
            "prior_sha256": self.prior_sha256,
        }


@dataclass(frozen=True)
class ResidualReliabilityFit:
    numerator: float
    denominator: float
    alpha: float
    status: str
    matured_oof_row_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "advisory_selection_prior_residual_reliability_v1",
            "method": "ZERO_INTERCEPT_OOF_CLIPPED_0_1_V1",
            "denominator_epsilon": SELECTION_PRIOR_RELIABILITY_DENOMINATOR_EPSILON,
            "numerator": self.numerator,
            "denominator": self.denominator,
            "alpha": self.alpha,
            "status": self.status,
            "matured_oof_row_count": self.matured_oof_row_count,
        }


@dataclass(frozen=True)
class SelectionPriorResidualOOFResult:
    predictions: pd.DataFrame
    residual_best_iterations: tuple[int, ...]
    liability_best_iterations: tuple[int, ...]
    fold_receipts: tuple[dict[str, Any], ...]
    reliability: ResidualReliabilityFit


@dataclass(frozen=True)
class SelectionPriorResidualFinalModels:
    residual_booster: Any
    liability_booster: Any
    residual_transform: PolicyUtilityTransformFit
    liability_transform: PolicyUtilityTransformFit
    feature_names: tuple[str, ...]
    categorical_vocabulary: dict[str, tuple[int, ...]]
    residual_boost_rounds: int
    liability_boost_rounds: int
    selection_prior: SelectionRankPriorFit
    reliability: ResidualReliabilityFit


@dataclass(frozen=True)
class OOFPriceScale:
    return_location_bps: float
    return_scale_bps: float
    liability_location: float
    liability_scale: float
    base_price_bps_per_fraction: float
    candidates_bps_per_fraction: tuple[float, ...]


@dataclass(frozen=True)
class OOFPriceSelection:
    shadow_price_bps_per_fraction: float
    p0d_oof_turnover_budget: float
    p0j_oof_turnover: float
    constraint_slack: float
    candidate_turnover_by_price: dict[str, float]


def selection_prior_residual_feature_names(family: SelectionPriorResidualFamilySpecV1) -> tuple[str, ...]:
    return tuple(column for column in MODEL_FEATURE_COLUMNS if family.include_hmm or column not in HMM_FEATURES)


def fit_selection_rank_prior(
    labels: pd.DataFrame,
    *,
    train_dates: Sequence[pd.Timestamp],
) -> SelectionRankPriorFit:
    required = {
        "decision_as_of_trade_date",
        "instrument",
        "selection_effective_rank",
        "label_status",
        RETURN_TARGET_COLUMN,
    }
    if not required.issubset(labels):
        raise _prior_error(
            "selection prior label input is incomplete",
            missing=sorted(required - set(labels)),
        )
    rows = labels.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(
        rows["decision_as_of_trade_date"]
    ).dt.normalize()
    if rows.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise _prior_error("selection prior labels contain duplicate candidates")
    train = set(pd.DatetimeIndex(pd.to_datetime(list(train_dates))).normalize())
    rows = rows[
        rows["decision_as_of_trade_date"].isin(train)
        & (rows["label_status"] == "MATURED")
    ].copy()
    if rows.empty:
        raise _prior_error("selection prior has no matured training rows")
    rows["selection_effective_rank"] = pd.to_numeric(
        rows["selection_effective_rank"], errors="coerce"
    )
    rows[RETURN_TARGET_COLUMN] = pd.to_numeric(rows[RETURN_TARGET_COLUMN], errors="coerce")
    if (
        not np.isfinite(rows["selection_effective_rank"].to_numpy(float)).all()
        or not np.isfinite(rows[RETURN_TARGET_COLUMN].to_numpy(float)).all()
        or not np.equal(
            rows["selection_effective_rank"].to_numpy(float),
            np.floor(rows["selection_effective_rank"].to_numpy(float)),
        ).all()
    ):
        raise _prior_error("selection prior rank or return is non-finite/non-integral")
    rows["selection_effective_rank"] = rows["selection_effective_rank"].astype(int)
    expected_ranks = tuple(range(1, SELECTION_PRIOR_RANK_COUNT + 1))
    observed_ranks = tuple(sorted(rows["selection_effective_rank"].unique()))
    if observed_ranks != expected_ranks:
        raise _prior_error(
            "selection prior requires complete ranks 1..20",
            observed_ranks=observed_ranks,
        )
    grouped = rows.groupby("selection_effective_rank", sort=True)[RETURN_TARGET_COLUMN]
    locations = grouped.median().reindex(expected_ranks)
    weights = grouped.size().reindex(expected_ranks)
    if (
        locations.isna().any()
        or weights.isna().any()
        or (weights <= 0).any()
        or not np.isfinite(locations.to_numpy(float)).all()
    ):
        raise _prior_error("selection prior rank statistics are invalid")
    fitted = IsotonicRegression(
        increasing=False,
        out_of_bounds="raise",
    ).fit_transform(
        np.asarray(expected_ranks, dtype=float),
        locations.to_numpy(float),
        sample_weight=weights.to_numpy(float),
    )
    if (
        not np.isfinite(fitted).all()
        or np.any(np.diff(fitted) > 1e-12)
        or float(np.max(fitted) - np.min(fitted)) <= 0.0
    ):
        raise _prior_error("selection prior isotonic curve is degenerate")
    payload = {
        "schema_version": "advisory_selection_rank_prior_v1",
        "method": "RANK_MEDIAN_COUNT_WEIGHTED_DECREASING_ISOTONIC_V1",
        "ranks": list(expected_ranks),
        "rank_locations_bps": [float(value) for value in locations],
        "rank_weights": [int(value) for value in weights],
        "prior_values_bps": [float(value) for value in fitted],
    }
    return SelectionRankPriorFit(
        ranks=expected_ranks,
        rank_locations_bps=tuple(payload["rank_locations_bps"]),
        rank_weights=tuple(payload["rank_weights"]),
        prior_values_bps=tuple(payload["prior_values_bps"]),
        prior_sha256=canonical_json_sha256(payload),
    )


def score_selection_rank_prior(
    ranks: Sequence[float | int],
    prior: SelectionRankPriorFit,
) -> np.ndarray:
    values = pd.to_numeric(pd.Series(list(ranks)), errors="coerce").to_numpy(float)
    if (
        not len(values)
        or not np.isfinite(values).all()
        or not np.equal(values, np.floor(values)).all()
        or (values < 1).any()
        or (values > SELECTION_PRIOR_RANK_COUNT).any()
    ):
        raise _prior_error("selection prior scoring ranks are invalid")
    prior_values = np.asarray(prior.prior_values_bps, dtype=float)
    result = prior_values[values.astype(int) - 1]
    if not np.isfinite(result).all():
        raise _prior_error("selection prior scores are non-finite")
    return result


def add_selection_prior_residual_target(
    labels: pd.DataFrame,
    *,
    prior: SelectionRankPriorFit,
) -> pd.DataFrame:
    required = {"selection_effective_rank", "label_status", RETURN_TARGET_COLUMN}
    if not required.issubset(labels):
        raise _prior_error(
            "selection prior residual label input is incomplete",
            missing=sorted(required - set(labels)),
        )
    result = labels.copy()
    result[SELECTION_PRIOR_SCORE_COLUMN] = score_selection_rank_prior(
        result["selection_effective_rank"], prior
    )
    result[RESIDUAL_TARGET_COLUMN] = np.nan
    matured = result["label_status"] == "MATURED"
    actual = pd.to_numeric(result.loc[matured, RETURN_TARGET_COLUMN], errors="coerce").to_numpy(float)
    prior_values = result.loc[matured, SELECTION_PRIOR_SCORE_COLUMN].to_numpy(float)
    residual = actual - prior_values
    if not np.isfinite(actual).all() or not np.isfinite(residual).all():
        raise _prior_error("selection prior residual targets are non-finite")
    result.loc[matured, RESIDUAL_TARGET_COLUMN] = residual
    return result


def fit_oof_residual_reliability(
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
) -> ResidualReliabilityFit:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    required_predictions = set(keys) | {SELECTION_PRIOR_SCORE_COLUMN, RESIDUAL_SCORE_COLUMN}
    required_labels = set(keys) | {"label_status", RETURN_TARGET_COLUMN}
    if not required_predictions.issubset(predictions) or not required_labels.issubset(labels):
        raise _reliability_error("OOF reliability input columns are incomplete")
    if predictions.duplicated(keys).any() or labels.duplicated(keys).any():
        raise _reliability_error("OOF reliability identities are duplicated")
    attached = predictions.merge(
        labels[keys + ["label_status", RETURN_TARGET_COLUMN]],
        on=keys,
        how="left",
        validate="one_to_one",
    )
    if attached["label_status"].isna().any():
        raise _reliability_error("OOF reliability predictions are missing label identity")
    matured = attached[attached["label_status"] == "MATURED"].copy()
    if matured.empty:
        raise _reliability_error("OOF reliability has no matured rows")
    actual = (
        pd.to_numeric(matured[RETURN_TARGET_COLUMN], errors="coerce").to_numpy(float)
        - matured[SELECTION_PRIOR_SCORE_COLUMN].to_numpy(float)
    )
    predicted = matured[RESIDUAL_SCORE_COLUMN].to_numpy(float)
    if not np.isfinite(actual).all() or not np.isfinite(predicted).all():
        raise _reliability_error("OOF reliability values are non-finite")
    denominator = float(np.dot(predicted, predicted))
    numerator = float(np.dot(predicted, actual))
    if not np.isfinite([numerator, denominator]).all() or denominator < 0.0:
        raise _reliability_error("OOF reliability moments are invalid")
    if denominator <= SELECTION_PRIOR_RELIABILITY_DENOMINATOR_EPSILON:
        alpha = 0.0
        status = "OOF_ZERO_RESIDUAL_VARIANCE_ALPHA_ZERO"
    elif numerator <= 0.0:
        alpha = 0.0
        status = "OOF_NON_POSITIVE_RELIABILITY_ALPHA_ZERO"
    elif numerator >= denominator:
        alpha = 1.0
        status = "OOF_RELIABILITY_CLIPPED_ONE"
    else:
        alpha = numerator / denominator
        status = "OOF_RELIABILITY_INTERIOR"
    if not np.isfinite(alpha) or not 0.0 <= alpha <= 1.0:
        raise _reliability_error("OOF reliability alpha is invalid")
    return ResidualReliabilityFit(
        numerator=numerator,
        denominator=denominator,
        alpha=float(alpha),
        status=status,
        matured_oof_row_count=int(len(matured)),
    )


def apply_residual_reliability(
    predictions: pd.DataFrame,
    reliability: ResidualReliabilityFit,
) -> pd.DataFrame:
    required = {SELECTION_PRIOR_SCORE_COLUMN, RESIDUAL_SCORE_COLUMN}
    if not required.issubset(predictions):
        raise _reliability_error("anchored return input columns are incomplete")
    result = predictions.copy()
    result[RESIDUAL_ALPHA_COLUMN] = reliability.alpha
    result[RETURN_SCORE_COLUMN] = (
        result[SELECTION_PRIOR_SCORE_COLUMN]
        + reliability.alpha * result[RESIDUAL_SCORE_COLUMN]
    )
    if not np.isfinite(result[RETURN_SCORE_COLUMN].to_numpy(float)).all():
        raise _reliability_error("anchored return predictions are non-finite")
    return result


def add_liability_target(
    labels: pd.DataFrame,
    *,
    target_count: int = 5,
    turnover_action_count: int = 2,
) -> pd.DataFrame:
    required = {"label_status", "holding_trading_days"}
    if not required.issubset(labels):
        raise _error("selection-prior-residual liability label input is incomplete", missing=sorted(required - set(labels)))
    if target_count != 5 or turnover_action_count != 2:
        raise _error("selection-prior-residual liability policy units are invalid")
    result = labels.copy()
    result[LIABILITY_TARGET_COLUMN] = np.nan
    matured = result["label_status"] == "MATURED"
    holding = pd.to_numeric(result.loc[matured, "holding_trading_days"], errors="coerce").to_numpy(float)
    if not len(holding) or not np.isfinite(holding).all() or (holding < 1.0).any() or (holding > 20.0).any():
        raise _error("selection-prior-residual matured holding labels are invalid")
    liability = turnover_action_count / (float(target_count) * holding)
    if not np.isfinite(liability).all() or (liability < 0.02).any() or (liability > 0.4).any():
        raise _error("selection-prior-residual liability targets exceed frozen physical bounds")
    result.loc[matured, LIABILITY_TARGET_COLUMN] = liability
    return result


def eligible_constraint_dates(
    labels: pd.DataFrame,
    *,
    expected_decision_date_count: int = 386,
    expected_constraint_decision_date_count: int = 385,
) -> tuple[pd.DatetimeIndex, dict[str, Any]]:
    required = {"decision_as_of_trade_date", "instrument", "label_status"}
    if not required.issubset(labels):
        raise _coverage_error("selection-prior-residual eligibility columns are missing", missing=sorted(required - set(labels)))
    rows = labels.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    if rows.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise _coverage_error("selection-prior-residual labels contain duplicate candidates")
    counts = rows.groupby("decision_as_of_trade_date").size()
    if len(counts) != expected_decision_date_count or not counts.eq(20).all():
        raise _coverage_error("selection-prior-residual labels are not exact Top20 on 386 dates")
    censored = rows.loc[
        rows["label_status"] == "CENSORED_RIGHT_BOUNDARY", "decision_as_of_trade_date"
    ].unique()
    if len(censored) != 1:
        raise _coverage_error("selection-prior-residual constraint requires exactly one right-boundary date")
    eligible = pd.DatetimeIndex(sorted(set(counts.index) - {pd.Timestamp(censored[0])})).normalize()
    if len(eligible) != expected_constraint_decision_date_count:
        raise _coverage_error("selection-prior-residual constraint date count differs from frozen identity")
    excluded = pd.Timestamp(censored[0]).date().isoformat()
    return eligible, {
        "schema_version": "advisory_selection_prior_residual_constraint_coverage_v1",
        "decision_date_count": int(len(counts)),
        "eligible_constraint_decision_date_count": int(len(eligible)),
        "excluded_right_boundary_date": excluded,
        "eligible_dates_sha256": canonical_json_sha256([value.date().isoformat() for value in eligible]),
        "label_status_counts": {
            str(key): int(value) for key, value in rows["label_status"].value_counts().sort_index().items()
        },
    }


def build_inner_fold_specs(
    *,
    labels: pd.DataFrame,
    outer_train_dates: Sequence[pd.Timestamp],
    eligible_dates: Sequence[pd.Timestamp],
    block_by_date: Mapping[str, int],
    trading_calendar: Sequence[pd.Timestamp],
    embargo_trading_days: int = 20,
) -> tuple[InnerFoldSpec, ...]:
    prepared = labels.loc[labels["label_status"] == "MATURED"].copy()
    required = {
        "decision_as_of_trade_date",
        "label_information_start",
        "label_information_end",
        RETURN_TARGET_COLUMN,
        "holding_trading_days",
    }
    if not required.issubset(prepared):
        raise _split_error("selection-prior-residual inner split label columns are missing", missing=sorted(required - set(prepared)))
    for column in ("decision_as_of_trade_date", "label_information_start", "label_information_end"):
        prepared[column] = pd.to_datetime(prepared[column]).dt.normalize()
    outer_dates = set(pd.DatetimeIndex(pd.to_datetime(list(outer_train_dates))).normalize())
    eligible = set(pd.DatetimeIndex(pd.to_datetime(list(eligible_dates))).normalize()) & outer_dates
    prepared = prepared[prepared["decision_as_of_trade_date"].isin(outer_dates)].copy()
    if prepared.empty or not eligible:
        raise _split_error("selection-prior-residual outer train has no matured labels or eligible score dates")
    invalid_interval = (
        prepared["label_information_start"].isna()
        | prepared["label_information_end"].isna()
        | (prepared["label_information_end"] < prepared["label_information_start"])
    )
    if invalid_interval.any():
        raise _split_error("selection-prior-residual outer train contains invalid label intervals")
    block_dates: dict[int, set[pd.Timestamp]] = {}
    for value in sorted(outer_dates):
        block = block_by_date.get(value.date().isoformat())
        if block is None:
            raise _split_error("selection-prior-residual outer train date has no CPCV block", trade_date=value.date().isoformat())
        block_dates.setdefault(int(block), set()).add(value)
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    calendar_pos = {value: index for index, value in enumerate(calendar)}
    folds: list[InnerFoldSpec] = []
    scored: set[pd.Timestamp] = set()
    for block, dates in sorted(block_dates.items()):
        validation_dates = pd.DatetimeIndex(sorted(dates)).normalize()
        validation = prepared[prepared["decision_as_of_trade_date"].isin(validation_dates)].copy()
        train = prepared[~prepared["decision_as_of_trade_date"].isin(validation_dates)].copy()
        if validation.empty or train.empty:
            raise _split_error("selection-prior-residual inner fold has empty train or validation", block_id=block)
        purge_mask = _information_overlap(train, validation)
        embargo = _embargo_dates(
            validation_dates,
            calendar=calendar,
            calendar_pos=calendar_pos,
            days=embargo_trading_days,
        )
        embargo_mask = train["decision_as_of_trade_date"].isin(embargo)
        retained = train.loc[~purge_mask & ~embargo_mask]
        retained_dates = tuple(sorted(retained["decision_as_of_trade_date"].unique()))
        if len(retained_dates) < 2:
            raise _split_error("selection-prior-residual inner fold retains insufficient train dates", block_id=block)
        score_dates = tuple(sorted(dates & eligible))
        if score_dates:
            scored.update(score_dates)
        folds.append(
            InnerFoldSpec(
                block_id=block,
                train_dates=tuple(pd.Timestamp(value) for value in retained_dates),
                validation_dates=tuple(pd.Timestamp(value) for value in validation_dates),
                score_dates=tuple(pd.Timestamp(value) for value in score_dates),
                purged_dates=tuple(
                    pd.Timestamp(value)
                    for value in sorted(train.loc[purge_mask, "decision_as_of_trade_date"].unique())
                ),
                embargo_dates=tuple(
                    pd.Timestamp(value)
                    for value in sorted(train.loc[embargo_mask, "decision_as_of_trade_date"].unique())
                ),
            )
        )
    if scored != eligible:
        raise _split_error("selection-prior-residual inner folds do not cover exact eligible outer-train dates")
    return tuple(folds)


def train_selection_prior_residual_oof(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    folds: Sequence[InnerFoldSpec],
    family: SelectionPriorResidualFamilySpecV1,
    seed: int,
    liability_clip_min: float = 0.02,
    liability_clip_max: float = 0.4,
) -> SelectionPriorResidualOOFResult:
    prepared_labels = add_liability_target(labels)
    parts: list[pd.DataFrame] = []
    residual_rounds: list[int] = []
    liability_rounds: list[int] = []
    receipts: list[dict[str, Any]] = []
    for fold in folds:
        if not fold.score_dates:
            continue
        prior = fit_selection_rank_prior(
            prepared_labels,
            train_dates=fold.train_dates,
        )
        residual_labels = add_selection_prior_residual_target(
            prepared_labels,
            prior=prior,
        )
        residual_result = _train_head_fold(
            features=features,
            labels=residual_labels,
            train_dates=fold.train_dates,
            validation_dates=fold.validation_dates,
            score_dates=fold.score_dates,
            family=family,
            seed=seed,
            target_column=RESIDUAL_TARGET_COLUMN,
            prediction_column=RESIDUAL_SCORE_COLUMN,
            clip_bounds=None,
        )
        residual_predictions = residual_result.predictions.copy()
        residual_predictions[SELECTION_PRIOR_SCORE_COLUMN] = score_selection_rank_prior(
            residual_predictions["selection_effective_rank"],
            prior,
        )
        liability_result = _train_head_fold(
            features=features,
            labels=prepared_labels,
            train_dates=fold.train_dates,
            validation_dates=fold.validation_dates,
            score_dates=fold.score_dates,
            family=family,
            seed=seed,
            target_column=LIABILITY_TARGET_COLUMN,
            prediction_column=LIABILITY_SCORE_COLUMN,
            clip_bounds=(liability_clip_min, liability_clip_max),
        )
        merged = residual_predictions.merge(
            liability_result.predictions[
                ["decision_as_of_trade_date", "target_trade_date", "instrument", LIABILITY_SCORE_COLUMN]
            ],
            on=["decision_as_of_trade_date", "target_trade_date", "instrument"],
            how="inner",
            validate="one_to_one",
        )
        merged["inner_block_id"] = fold.block_id
        parts.append(merged)
        residual_rounds.append(residual_result.best_iteration)
        liability_rounds.append(liability_result.best_iteration)
        receipts.append(
            {
                "block_id": fold.block_id,
                "train_decision_count": len(fold.train_dates),
                "validation_decision_count": len(fold.validation_dates),
                "score_decision_count": len(fold.score_dates),
                "purged_decision_count": len(fold.purged_dates),
                "embargo_decision_count": len(fold.embargo_dates),
                "residual_best_iteration": residual_result.best_iteration,
                "liability_best_iteration": liability_result.best_iteration,
                "residual_head_metrics": residual_result.metrics,
                "liability_metrics": liability_result.metrics,
                "selection_prior": prior.as_dict(),
            }
        )
    if not parts:
        raise _oof_error("selection-prior-residual OOF produced no scored fold")
    predictions = pd.concat(parts, ignore_index=True)
    reliability = fit_oof_residual_reliability(predictions, prepared_labels)
    predictions = apply_residual_reliability(predictions, reliability)
    _verify_exact_predictions(predictions)
    return SelectionPriorResidualOOFResult(
        predictions=predictions,
        residual_best_iterations=tuple(residual_rounds),
        liability_best_iterations=tuple(liability_rounds),
        fold_receipts=tuple(receipts),
        reliability=reliability,
    )


def fit_oof_price_scale(
    predictions: pd.DataFrame,
    *,
    multipliers: Sequence[float] = SELECTION_PRIOR_RESIDUAL_SHADOW_PRICE_MULTIPLIERS,
) -> OOFPriceScale:
    required = {RETURN_SCORE_COLUMN, LIABILITY_SCORE_COLUMN}
    if not required.issubset(predictions):
        raise _constraint_error("selection-prior-residual OOF scale columns are missing")
    return_values = predictions[RETURN_SCORE_COLUMN].to_numpy(float)
    liability_values = predictions[LIABILITY_SCORE_COLUMN].to_numpy(float)
    return_location = float(np.median(return_values))
    liability_location = float(np.median(liability_values))
    return_scale = float(np.median(np.abs(return_values - return_location)))
    liability_scale = float(np.median(np.abs(liability_values - liability_location)))
    if (
        not np.isfinite([return_location, liability_location, return_scale, liability_scale]).all()
        or return_scale <= 0.0
        or liability_scale <= 0.0
    ):
        raise _constraint_error(
            "selection-prior-residual OOF price scale is invalid",
            return_scale_bps=return_scale,
            liability_scale=liability_scale,
        )
    normalized = tuple(float(value) for value in multipliers)
    if normalized != SELECTION_PRIOR_RESIDUAL_SHADOW_PRICE_MULTIPLIERS:
        raise _constraint_error("selection-prior-residual shadow-price multiplier roster is invalid")
    base = return_scale / liability_scale
    candidates = tuple(base * value for value in normalized)
    if not np.isfinite(candidates).all() or tuple(sorted(candidates)) != candidates:
        raise _constraint_error("selection-prior-residual shadow-price candidates are invalid")
    return OOFPriceScale(
        return_location_bps=return_location,
        return_scale_bps=return_scale,
        liability_location=liability_location,
        liability_scale=liability_scale,
        base_price_bps_per_fraction=base,
        candidates_bps_per_fraction=candidates,
    )


def combine_selection_prior_residual_predictions(predictions: pd.DataFrame, *, shadow_price: float) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        SELECTION_PRIOR_SCORE_COLUMN,
        RESIDUAL_SCORE_COLUMN,
        RESIDUAL_ALPHA_COLUMN,
        RETURN_SCORE_COLUMN,
        LIABILITY_SCORE_COLUMN,
    }
    if not required.issubset(predictions):
        raise _priority_error("selection-prior-residual prediction columns are missing", missing=sorted(required - set(predictions)))
    if not np.isfinite(shadow_price) or shadow_price < 0.0:
        raise _priority_error("selection-prior-residual shadow price is invalid")
    result = predictions.copy()
    result[COMBINED_SCORE_COLUMN] = (
        result[RETURN_SCORE_COLUMN] - shadow_price * result[LIABILITY_SCORE_COLUMN]
    )
    result["decision_as_of_trade_date"] = pd.to_datetime(result["decision_as_of_trade_date"]).dt.normalize()
    counts = result.groupby("decision_as_of_trade_date").size()
    ranks = result.groupby("decision_as_of_trade_date")["selection_effective_rank"].apply(
        lambda values: tuple(sorted(pd.to_numeric(values, errors="coerce").tolist()))
    )
    if counts.empty or not counts.eq(20).all() or not ranks.map(lambda values: values == tuple(range(1, 21))).all():
        raise _priority_error("selection-prior-residual ranking requires exact Selection ranks 1..20")
    if result.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise _priority_error("selection-prior-residual ranking contains duplicate candidates")
    if not np.isfinite(result[COMBINED_SCORE_COLUMN].to_numpy(float)).all():
        raise _priority_error("selection-prior-residual combined score is non-finite")
    result = result.sort_values(
        ["decision_as_of_trade_date", COMBINED_SCORE_COLUMN, "selection_effective_rank", "instrument"],
        ascending=[True, False, True, True],
    )
    result["entry_priority_rank"] = result.groupby("decision_as_of_trade_date").cumcount().add(1)
    result["selection_exit_rank"] = result["selection_effective_rank"]
    result["entry_priority_score_kind"] = "SELECTION_PRIOR_RESIDUAL_OUTPUT_CONSTRAINED_UTILITY_BPS"
    return result.reset_index(drop=True)


def select_minimum_feasible_oof_price(
    *,
    scale: OOFPriceScale,
    p0d_oof_turnover_budget: float,
    evaluate_turnover: Callable[[float], float],
) -> OOFPriceSelection:
    if not np.isfinite(p0d_oof_turnover_budget) or p0d_oof_turnover_budget < 0.0:
        raise _constraint_error("exact P0-D OOF turnover budget is invalid")
    observed: dict[str, float] = {}
    for price in scale.candidates_bps_per_fraction:
        turnover = float(evaluate_turnover(price))
        if not np.isfinite(turnover) or turnover < 0.0:
            raise _constraint_error("selection-prior-residual OOF turnover is invalid", shadow_price=price)
        observed[format(price, ".17g")] = turnover
        if turnover <= p0d_oof_turnover_budget + 1e-15:
            return OOFPriceSelection(
                shadow_price_bps_per_fraction=price,
                p0d_oof_turnover_budget=p0d_oof_turnover_budget,
                p0j_oof_turnover=turnover,
                constraint_slack=p0d_oof_turnover_budget - turnover,
                candidate_turnover_by_price=observed,
            )
    raise _constraint_error(
        "approved selection-prior-residual shadow-price roster cannot satisfy exact P0-D OOF budget",
        p0d_oof_turnover_budget=p0d_oof_turnover_budget,
        candidate_turnover_by_price=observed,
    )


def fit_final_selection_prior_residual_models(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    family: SelectionPriorResidualFamilySpecV1,
    seed: int,
    residual_boost_rounds: int,
    liability_boost_rounds: int,
    reliability: ResidualReliabilityFit,
) -> SelectionPriorResidualFinalModels:
    prepared = add_liability_target(labels)
    prior = fit_selection_rank_prior(prepared, train_dates=train_dates)
    residual_labels = add_selection_prior_residual_target(prepared, prior=prior)
    residual_model = _fit_final_head(
        features=features,
        labels=residual_labels,
        train_dates=train_dates,
        family=family,
        seed=seed,
        target_column=RESIDUAL_TARGET_COLUMN,
        boost_rounds=residual_boost_rounds,
    )
    liability_model = _fit_final_head(
        features=features,
        labels=prepared,
        train_dates=train_dates,
        family=family,
        seed=seed,
        target_column=LIABILITY_TARGET_COLUMN,
        boost_rounds=liability_boost_rounds,
    )
    if residual_model[2] != liability_model[2] or residual_model[3] != liability_model[3]:
        raise _error("selection-prior-residual final feature identity differs between heads")
    return SelectionPriorResidualFinalModels(
        residual_booster=residual_model[0],
        liability_booster=liability_model[0],
        residual_transform=residual_model[1],
        liability_transform=liability_model[1],
        feature_names=residual_model[2],
        categorical_vocabulary=residual_model[3],
        residual_boost_rounds=residual_boost_rounds,
        liability_boost_rounds=liability_boost_rounds,
        selection_prior=prior,
        reliability=reliability,
    )


def score_final_selection_prior_residual_models(
    *,
    features: pd.DataFrame,
    models: SelectionPriorResidualFinalModels,
    score_dates: Sequence[pd.Timestamp],
    liability_clip_min: float = 0.02,
    liability_clip_max: float = 0.4,
) -> pd.DataFrame:
    rows, matrix = _score_matrix(
        features=features,
        score_dates=score_dates,
        feature_names=models.feature_names,
        categorical_vocabulary=models.categorical_vocabulary,
    )
    residual_standardized = models.residual_booster.predict(matrix)
    liability_standardized = models.liability_booster.predict(matrix)
    residual_predictions = inverse_policy_utility_transform(
        residual_standardized,
        models.residual_transform,
    )
    liability_predictions = inverse_policy_utility_transform(
        liability_standardized, models.liability_transform
    )
    result = rows[
        ["decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank"]
    ].copy()
    result[SELECTION_PRIOR_SCORE_COLUMN] = score_selection_rank_prior(
        result["selection_effective_rank"],
        models.selection_prior,
    )
    result[RESIDUAL_SCORE_COLUMN] = residual_predictions
    result = apply_residual_reliability(result, models.reliability)
    result[LIABILITY_SCORE_COLUMN] = np.clip(
        liability_predictions,
        liability_clip_min,
        liability_clip_max,
    )
    _verify_exact_predictions(result)
    return result


def selection_prior_residual_candidate_metrics(predictions: pd.DataFrame) -> dict[str, Any]:
    required = {
        RETURN_TARGET_COLUMN,
        SELECTION_PRIOR_SCORE_COLUMN,
        RESIDUAL_SCORE_COLUMN,
        RESIDUAL_ALPHA_COLUMN,
        LIABILITY_TARGET_COLUMN,
        RETURN_SCORE_COLUMN,
        LIABILITY_SCORE_COLUMN,
        COMBINED_SCORE_COLUMN,
        "label_status",
        "decision_as_of_trade_date",
        "entry_priority_rank",
        "selection_effective_rank",
    }
    if not required.issubset(predictions):
        raise _error("selection-prior-residual candidate diagnostic columns are missing")
    all_rows = predictions.copy()
    matured = all_rows[all_rows["label_status"] == "MATURED"].copy()
    if matured.empty:
        raise _error("selection-prior-residual candidate diagnostics have no matured rows")
    metrics: dict[str, Any] = {}
    matured[RESIDUAL_TARGET_COLUMN] = (
        pd.to_numeric(matured[RETURN_TARGET_COLUMN], errors="coerce")
        - matured[SELECTION_PRIOR_SCORE_COLUMN]
    )
    for name, actual, predicted in (
        ("return", RETURN_TARGET_COLUMN, RETURN_SCORE_COLUMN),
        ("residual", RESIDUAL_TARGET_COLUMN, RESIDUAL_SCORE_COLUMN),
        ("liability", LIABILITY_TARGET_COLUMN, LIABILITY_SCORE_COLUMN),
    ):
        y = matured[actual].to_numpy(float)
        score = matured[predicted].to_numpy(float)
        if not np.isfinite(y).all() or not np.isfinite(score).all():
            raise _error("selection-prior-residual candidate diagnostics contain non-finite values", head=name)
        metrics[f"{name}_mae"] = float(mean_absolute_error(y, score))
        metrics[f"{name}_rmse"] = float(mean_squared_error(y, score) ** 0.5)
        correlation = matured.groupby("decision_as_of_trade_date", sort=True).apply(
            lambda group: group[predicted].corr(group[actual], method="spearman"),
            include_groups=False,
        )
        metrics[f"{name}_daily_spearman_mean"] = _finite_mean(correlation)
        metrics[f"{name}_daily_spearman_null_count"] = int((~np.isfinite(correlation)).sum())
    spread = matured.groupby("decision_as_of_trade_date", sort=True).apply(
        lambda group: group.loc[group["entry_priority_rank"] <= 5, RETURN_TARGET_COLUMN].mean()
        - group.loc[group["entry_priority_rank"] > 5, RETURN_TARGET_COLUMN].mean(),
        include_groups=False,
    )
    metrics["top5_vs_rest_raw_return_spread_bps"] = float(spread.mean())
    alpha_values = all_rows[RESIDUAL_ALPHA_COLUMN].drop_duplicates().to_numpy(float)
    if len(alpha_values) != 1 or not np.isfinite(alpha_values).all():
        raise _error("selection-prior-residual diagnostics have inconsistent alpha")
    metrics["residual_alpha"] = float(alpha_values[0])
    metrics["selection_top5_overlap_fraction"] = float(
        (
            (all_rows["entry_priority_rank"] <= 5)
            & (all_rows["selection_effective_rank"] <= 5)
        ).sum()
        / max(1, int((all_rows["entry_priority_rank"] <= 5).sum()))
    )
    metrics["mean_absolute_rank_displacement"] = float(
        (
            pd.to_numeric(all_rows["entry_priority_rank"], errors="coerce")
            - pd.to_numeric(all_rows["selection_effective_rank"], errors="coerce")
        )
        .abs()
        .mean()
    )
    metrics["liability_clip_low_count"] = int((matured[LIABILITY_SCORE_COLUMN] <= 0.02).sum())
    metrics["liability_clip_high_count"] = int((matured[LIABILITY_SCORE_COLUMN] >= 0.4).sum())
    return metrics


def _train_head_fold(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    validation_dates: Sequence[pd.Timestamp],
    score_dates: Sequence[pd.Timestamp],
    family: SelectionPriorResidualFamilySpecV1,
    seed: int,
    target_column: str,
    prediction_column: str,
    clip_bounds: tuple[float, float] | None,
) -> HeadFoldResult:
    merged, matrix, vocabulary, feature_names = _training_matrix(
        features,
        labels,
        family,
        train_dates=train_dates,
    )
    normalized = merged["decision_as_of_trade_date"]
    train_set = set(pd.DatetimeIndex(pd.to_datetime(list(train_dates))).normalize())
    validation_set = set(pd.DatetimeIndex(pd.to_datetime(list(validation_dates))).normalize())
    matured = merged["label_status"] == "MATURED"
    train_mask = normalized.isin(train_set) & matured
    validation_mask = normalized.isin(validation_set) & matured
    if not train_mask.any() or not validation_mask.any():
        raise _oof_error("selection-prior-residual inner fold has no matured train or validation rows")
    transform = fit_policy_utility_transform(merged.loc[train_mask, target_column])
    y_train = apply_policy_utility_transform(merged.loc[train_mask, target_column], transform)
    y_validation = apply_policy_utility_transform(merged.loc[validation_mask, target_column], transform)
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
    booster = lgb.train(
        _training_params(family, seed),
        train_data,
        num_boost_round=family.max_boost_rounds,
        valid_sets=[validation_data],
        valid_names=["inner_validation"],
        callbacks=[lgb.early_stopping(family.early_stopping_rounds, verbose=False)],
    )
    score_rows, score_matrix = _score_matrix(
        features=features,
        score_dates=score_dates,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
    )
    standardized = booster.predict(score_matrix, num_iteration=booster.best_iteration)
    predictions = inverse_policy_utility_transform(standardized, transform)
    if clip_bounds is not None:
        predictions = np.clip(predictions, clip_bounds[0], clip_bounds[1])
    if not np.isfinite(predictions).all():
        raise _oof_error("selection-prior-residual inner fold prediction is non-finite", head=target_column)
    result = score_rows[
        ["decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank"]
    ].copy()
    result[prediction_column] = predictions
    validation_standardized = booster.predict(matrix.loc[validation_mask], num_iteration=booster.best_iteration)
    validation_prediction = inverse_policy_utility_transform(validation_standardized, transform)
    if clip_bounds is not None:
        validation_prediction = np.clip(validation_prediction, clip_bounds[0], clip_bounds[1])
    actual = merged.loc[validation_mask, target_column].to_numpy(float)
    metrics = {
        "mae": float(mean_absolute_error(actual, validation_prediction)),
        "rmse": float(mean_squared_error(actual, validation_prediction) ** 0.5),
    }
    return HeadFoldResult(
        predictions=result,
        best_iteration=int(booster.best_iteration),
        transform=transform,
        feature_names=feature_names,
        categorical_vocabulary=vocabulary,
        metrics=metrics,
    )


def _fit_final_head(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    family: SelectionPriorResidualFamilySpecV1,
    seed: int,
    target_column: str,
    boost_rounds: int,
) -> tuple[Any, PolicyUtilityTransformFit, tuple[str, ...], dict[str, tuple[int, ...]]]:
    if boost_rounds < 1:
        raise _error("selection-prior-residual final boost rounds are invalid", head=target_column)
    merged, matrix, vocabulary, feature_names = _training_matrix(
        features,
        labels,
        family,
        train_dates=train_dates,
    )
    train_set = set(pd.DatetimeIndex(pd.to_datetime(list(train_dates))).normalize())
    train_mask = merged["decision_as_of_trade_date"].isin(train_set) & (merged["label_status"] == "MATURED")
    if not train_mask.any():
        raise _error("selection-prior-residual final head has no matured train rows", head=target_column)
    transform = fit_policy_utility_transform(merged.loc[train_mask, target_column])
    target = apply_policy_utility_transform(merged.loc[train_mask, target_column], transform)
    lgb = _lightgbm()
    dataset = lgb.Dataset(
        matrix.loc[train_mask],
        label=target,
        categorical_feature=[column for column in CATEGORICAL_FEATURE_COLUMNS if column in matrix],
        free_raw_data=False,
    )
    booster = lgb.train(_training_params(family, seed), dataset, num_boost_round=boost_rounds)
    return booster, transform, feature_names, vocabulary


def _training_matrix(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    family: SelectionPriorResidualFamilySpecV1,
    *,
    train_dates: Sequence[pd.Timestamp],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, tuple[int, ...]], tuple[str, ...]]:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    merged = features.merge(labels, on=keys, how="inner", validate="one_to_one", suffixes=("", "_label"))
    merged["decision_as_of_trade_date"] = pd.to_datetime(merged["decision_as_of_trade_date"]).dt.normalize()
    feature_names = selection_prior_residual_feature_names(family)
    missing = sorted(set(feature_names) - set(merged))
    if missing:
        raise _error("selection-prior-residual feature matrix is incomplete", missing_columns=missing)
    matrix = merged.loc[:, feature_names].copy()
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    train_set = set(pd.DatetimeIndex(pd.to_datetime(list(train_dates))).normalize())
    train_mask = merged["decision_as_of_trade_date"].isin(train_set) & (merged["label_status"] == "MATURED")
    if not train_mask.any():
        raise _error("selection-prior-residual feature matrix has no matured train rows")
    required = [column for column in feature_names if column in REQUIRED_FEATURE_COLUMNS]
    all_null = [column for column in required if matrix.loc[train_mask, column].isna().all()]
    if all_null:
        raise _error("selection-prior-residual required features are entirely missing", features=all_null)
    vocabulary: dict[str, tuple[int, ...]] = {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in matrix:
            continue
        categories = tuple(sorted(matrix.loc[train_mask, column].dropna().astype(int).unique()))
        if not categories:
            raise _error("selection-prior-residual categorical feature has no vocabulary", feature=column)
        vocabulary[column] = categories
        numeric = pd.to_numeric(matrix[column], errors="coerce")
        unseen = numeric.notna() & ~numeric.isin(categories)
        if unseen.any():
            missing_indicator = f"{column}__missing"
            if missing_indicator not in matrix:
                raise _error("selection-prior-residual categorical missing indicator is absent", feature=column)
            matrix.loc[unseen, missing_indicator] = 1
            numeric = numeric.mask(unseen)
        matrix[column] = pd.Categorical(numeric, categories=categories)
    return merged, matrix, vocabulary, feature_names


def _score_matrix(
    *,
    features: pd.DataFrame,
    score_dates: Sequence[pd.Timestamp],
    feature_names: Sequence[str],
    categorical_vocabulary: Mapping[str, Sequence[int]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = set(pd.DatetimeIndex(pd.to_datetime(list(score_dates))).normalize())
    rows = features.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    rows = rows[rows["decision_as_of_trade_date"].isin(dates)].copy()
    counts = rows.groupby("decision_as_of_trade_date").size()
    if len(counts) != len(dates) or counts.empty or not counts.eq(20).all():
        raise _oof_error("selection-prior-residual score dates are not exact Top20")
    names = tuple(feature_names)
    if not set(names).issubset(rows):
        raise _oof_error("selection-prior-residual score feature identity is invalid")
    matrix = rows.loc[:, names].copy()
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    for column in CATEGORICAL_FEATURE_COLUMNS:
        if column not in matrix:
            continue
        categories = tuple(int(value) for value in categorical_vocabulary.get(column, ()))
        if not categories:
            raise _oof_error("selection-prior-residual score categorical vocabulary is empty", feature=column)
        numeric = pd.to_numeric(matrix[column], errors="coerce")
        unseen = numeric.notna() & ~numeric.isin(categories)
        if unseen.any():
            missing_indicator = f"{column}__missing"
            if missing_indicator not in matrix:
                raise _oof_error("selection-prior-residual score missing indicator is absent", feature=column)
            matrix.loc[unseen, missing_indicator] = 1
            numeric = numeric.mask(unseen)
        matrix[column] = pd.Categorical(numeric, categories=categories)
    return rows, matrix


def _verify_exact_predictions(predictions: pd.DataFrame) -> None:
    required = {
        "decision_as_of_trade_date",
        "instrument",
        "selection_effective_rank",
        RETURN_SCORE_COLUMN,
        LIABILITY_SCORE_COLUMN,
    }
    if not required.issubset(predictions):
        raise _oof_error("selection-prior-residual OOF prediction columns are incomplete")
    rows = predictions.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    counts = rows.groupby("decision_as_of_trade_date").size()
    if counts.empty or not counts.eq(20).all():
        raise _oof_error("selection-prior-residual OOF predictions are not exact Top20")
    ranks = rows.groupby("decision_as_of_trade_date")["selection_effective_rank"].apply(
        lambda values: tuple(sorted(pd.to_numeric(values, errors="coerce").tolist()))
    )
    if not ranks.map(lambda values: values == tuple(range(1, 21))).all():
        raise _oof_error("selection-prior-residual OOF ranks are not exact 1..20")
    if rows.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise _oof_error("selection-prior-residual OOF predictions contain duplicates")
    for column in (
        SELECTION_PRIOR_SCORE_COLUMN,
        RESIDUAL_SCORE_COLUMN,
        RESIDUAL_ALPHA_COLUMN,
        RETURN_SCORE_COLUMN,
        LIABILITY_SCORE_COLUMN,
    ):
        if not np.isfinite(rows[column].to_numpy(float)).all():
            raise _oof_error("selection-prior-residual OOF predictions contain non-finite values", column=column)
    alpha_values = rows[RESIDUAL_ALPHA_COLUMN].drop_duplicates().to_numpy(float)
    if len(alpha_values) != 1 or not 0.0 <= float(alpha_values[0]) <= 1.0:
        raise _oof_error("selection-prior-residual OOF alpha is inconsistent")


def _training_params(family: SelectionPriorResidualFamilySpecV1, seed: int) -> dict[str, Any]:
    return {
        "objective": "huber",
        "metric": "l1",
        "verbosity": -1,
        "num_leaves": family.num_leaves,
        "learning_rate": family.learning_rate,
        "min_data_in_leaf": family.min_data_in_leaf,
        "feature_fraction": family.feature_fraction,
        "bagging_fraction": family.bagging_fraction,
        "bagging_freq": family.bagging_freq,
        "lambda_l1": family.lambda_l1,
        "lambda_l2": family.lambda_l2,
        "num_threads": family.num_threads,
        "seed": seed,
        "feature_fraction_seed": seed,
        "bagging_seed": seed,
        "data_random_seed": seed,
        "deterministic": True,
        "force_col_wise": True,
    }


def _lightgbm() -> Any:
    try:
        import lightgbm as lgb
    except ImportError as exc:
        raise _error("LightGBM is required for selection-prior-residual training") from exc
    return lgb


def _finite_mean(values: pd.Series) -> float | None:
    finite = pd.to_numeric(values, errors="coerce")
    finite = finite[np.isfinite(finite)]
    return float(finite.mean()) if len(finite) else None


def _typed_error(message: str, reason_code: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(message, reason_code=reason_code, context=context or None)


def _error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return _typed_error(message, "ADVISORY_SELECTION_PRIOR_RESIDUAL_TRAINING_INVALID", **context)


def _coverage_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return _typed_error(message, "ADVISORY_SELECTION_PRIOR_RESIDUAL_CALIBRATION_COVERAGE_INVALID", **context)


def _split_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return _typed_error(message, "ADVISORY_SELECTION_PRIOR_RESIDUAL_INNER_SPLIT_INVALID", **context)


def _oof_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return _typed_error(message, "ADVISORY_SELECTION_PRIOR_RESIDUAL_OOF_INVALID", **context)


def _constraint_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return _typed_error(message, "ADVISORY_SELECTION_PRIOR_RESIDUAL_CONSTRAINT_INFEASIBLE", **context)


def _priority_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return _typed_error(message, "ADVISORY_SELECTION_PRIOR_RESIDUAL_PRIORITY_INVALID", **context)


def _prior_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return _typed_error(message, "ADVISORY_P0J_SELECTION_PRIOR_DEGENERATE", **context)


def _reliability_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return _typed_error(message, "ADVISORY_P0J_RESIDUAL_RELIABILITY_INVALID", **context)
