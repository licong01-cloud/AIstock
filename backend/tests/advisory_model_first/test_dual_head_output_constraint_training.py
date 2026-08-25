from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.dual_head_output_constraint_contracts import (
    approved_dual_head_families,
)
from backend.services.advisory_model_first.dual_head_output_constraint_training import (
    COMBINED_SCORE_COLUMN,
    LIABILITY_SCORE_COLUMN,
    LIABILITY_TARGET_COLUMN,
    RETURN_SCORE_COLUMN,
    add_liability_target,
    build_inner_fold_specs,
    combine_dual_head_predictions,
    dual_head_feature_names,
    eligible_constraint_dates,
    fit_oof_price_scale,
    select_minimum_feasible_oof_price,
    train_dual_head_oof,
    _training_matrix,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v2 import MODEL_FEATURE_COLUMNS


def _labels(periods: int = 8) -> pd.DataFrame:
    rows = []
    dates = pd.bdate_range("2026-01-05", periods=periods)
    for date_index, decision in enumerate(dates):
        target = decision + pd.offsets.BDay(1)
        for rank in range(1, 21):
            status = "MATURED"
            holding = float((rank % 10) + 1)
            information_end = target
            if date_index == 1 and rank == 20:
                status = "NOT_ENTERED_LIMIT_UP"
                holding = np.nan
                information_end = pd.NaT
            if date_index == periods - 1 and rank == 20:
                status = "CENSORED_RIGHT_BOUNDARY"
                holding = np.nan
                information_end = pd.NaT
            rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": target,
                    "instrument": f"S{rank:02d}",
                    "selection_rank": rank,
                    "label_status": status,
                    "holding_trading_days": holding,
                    "net_excess_return_bps": float(100 - rank + date_index),
                    "label_information_start": decision,
                    "label_information_end": information_end,
                }
            )
    return pd.DataFrame(rows)


def _features(periods: int = 8) -> pd.DataFrame:
    rng = np.random.default_rng(20260825)
    rows = []
    for date_index, decision in enumerate(pd.bdate_range("2026-01-05", periods=periods)):
        target = decision + pd.offsets.BDay(1)
        for rank in range(1, 21):
            row = {
                "decision_as_of_trade_date": decision,
                "target_trade_date": target,
                "instrument": f"S{rank:02d}",
                "selection_effective_rank": rank,
            }
            for column in MODEL_FEATURE_COLUMNS:
                row[column] = 0 if column.endswith("__missing") else float(rng.normal())
            row["l2_code_id"] = rank % 4
            row["parent_combined_score"] = float(100 - rank + date_index)
            rows.append(row)
    return pd.DataFrame(rows)


def test_liability_units_and_non_matured_rows_are_never_filled() -> None:
    adjusted = add_liability_target(_labels())
    first = adjusted.iloc[0]
    assert first[LIABILITY_TARGET_COLUMN] == pytest.approx(2 / (5 * 2))
    non_matured = adjusted[adjusted["label_status"] != "MATURED"]
    assert non_matured[LIABILITY_TARGET_COLUMN].isna().all()
    assert adjusted.loc[adjusted["label_status"] == "MATURED", LIABILITY_TARGET_COLUMN].between(
        0.02, 0.4
    ).all()


def test_constraint_eligibility_keeps_limit_up_date_and_excludes_only_right_boundary() -> None:
    labels = _labels()
    dates, receipt = eligible_constraint_dates(
        labels,
        expected_decision_date_count=8,
        expected_constraint_decision_date_count=7,
    )
    assert pd.Timestamp("2026-01-06") in dates
    assert pd.Timestamp(labels["decision_as_of_trade_date"].max()) not in dates
    assert receipt["eligible_constraint_decision_date_count"] == 7
    assert receipt["label_status_counts"]["NOT_ENTERED_LIMIT_UP"] == 1


def test_inner_folds_cover_each_eligible_date_once_and_never_train_on_holdout() -> None:
    labels = _labels()
    all_dates = pd.DatetimeIndex(labels["decision_as_of_trade_date"].unique()).normalize()
    eligible, _ = eligible_constraint_dates(
        labels,
        expected_decision_date_count=8,
        expected_constraint_decision_date_count=7,
    )
    block_by_date = {value.date().isoformat(): index for index, value in enumerate(all_dates)}
    calendar = pd.bdate_range("2025-12-01", "2026-03-31")
    folds = build_inner_fold_specs(
        labels=labels,
        outer_train_dates=all_dates,
        eligible_dates=eligible,
        block_by_date=block_by_date,
        trading_calendar=calendar,
        embargo_trading_days=0,
    )
    scored = [value for fold in folds for value in fold.score_dates]
    assert len(folds) == 8
    assert sorted(scored) == sorted(eligible)
    assert len(scored) == len(set(scored))
    for fold in folds:
        assert set(fold.train_dates).isdisjoint(fold.validation_dates)


def test_oof_price_scale_selection_and_exact_top20_priority_are_deterministic() -> None:
    rows = []
    for decision in pd.to_datetime(["2026-01-05", "2026-01-06"]):
        for rank in range(1, 21):
            rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": decision + pd.offsets.BDay(1),
                    "instrument": f"S{rank:02d}",
                    "selection_effective_rank": rank,
                    RETURN_SCORE_COLUMN: float(21 - rank),
                    LIABILITY_SCORE_COLUMN: 0.02 + rank * 0.01,
                }
            )
    predictions = pd.DataFrame(rows)
    scale = fit_oof_price_scale(predictions)
    second = scale.candidates_bps_per_fraction[1]
    selected = select_minimum_feasible_oof_price(
        scale=scale,
        p0d_oof_turnover_budget=0.25,
        evaluate_turnover=lambda price: 0.30 if price < second else 0.24,
    )
    assert selected.shadow_price_bps_per_fraction == second
    ranked = combine_dual_head_predictions(predictions, shadow_price=second)
    assert ranked.groupby("decision_as_of_trade_date").size().eq(20).all()
    assert ranked[COMBINED_SCORE_COLUMN].notna().all()
    assert set(ranked["entry_priority_score_kind"]) == {
        "DUAL_HEAD_OUTPUT_CONSTRAINED_UTILITY_BPS"
    }
    with pytest.raises(AdvisoryModelFirstError):
        combine_dual_head_predictions(predictions.iloc[:-1], shadow_price=second)


def test_dual_head_features_never_include_future_labels() -> None:
    forbidden = {"holding_trading_days", "net_excess_return_bps", LIABILITY_TARGET_COLUMN}
    for family in approved_dual_head_families():
        assert forbidden.isdisjoint(dual_head_feature_names(family))


def test_categorical_vocabulary_is_train_only_and_unseen_sets_missing_indicator() -> None:
    labels = add_liability_target(_labels())
    features = _features()
    validation_date = pd.Timestamp(features["decision_as_of_trade_date"].max())
    unseen = features["decision_as_of_trade_date"] == validation_date
    features.loc[unseen, "l2_code_id"] = 99
    features.loc[unseen, "l2_code_id__missing"] = 0
    train_dates = pd.to_datetime(
        features.loc[features["decision_as_of_trade_date"] != validation_date, "decision_as_of_trade_date"].unique()
    )
    merged, matrix, vocabulary, _ = _training_matrix(
        features,
        labels,
        approved_dual_head_families()[0],
        train_dates=train_dates,
    )
    validation_mask = merged["decision_as_of_trade_date"] == validation_date
    assert 99 not in vocabulary["l2_code_id"]
    assert matrix.loc[validation_mask, "l2_code_id"].isna().all()
    assert matrix.loc[validation_mask, "l2_code_id__missing"].eq(1).all()


def test_lightgbm_inner_oof_scores_all_eligible_top20() -> None:
    pytest.importorskip("lightgbm")
    periods = 16
    labels = _labels(periods)
    features = _features(periods)
    all_dates = pd.DatetimeIndex(labels["decision_as_of_trade_date"].unique()).normalize()
    eligible, _ = eligible_constraint_dates(
        labels,
        expected_decision_date_count=periods,
        expected_constraint_decision_date_count=periods - 1,
    )
    block_by_date = {
        value.date().isoformat(): index % 4 for index, value in enumerate(all_dates)
    }
    folds = build_inner_fold_specs(
        labels=labels,
        outer_train_dates=all_dates,
        eligible_dates=eligible,
        block_by_date=block_by_date,
        trading_calendar=pd.bdate_range("2025-12-01", "2026-04-30"),
        embargo_trading_days=0,
    )
    result = train_dual_head_oof(
        features=features,
        labels=labels,
        folds=folds,
        family=approved_dual_head_families()[0],
        seed=20260813,
    )
    assert len(result.predictions) == (periods - 1) * 20
    assert result.predictions.groupby("decision_as_of_trade_date").size().eq(20).all()
    assert result.predictions[LIABILITY_SCORE_COLUMN].between(0.02, 0.4).all()
