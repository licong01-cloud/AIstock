from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v2 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.turnover_constrained_utility_contracts import (
    approved_turnover_constrained_utility_families,
)
from backend.services.advisory_model_first.turnover_constrained_utility_training import (
    LIABILITY_COLUMN,
    SCORE_COLUMN,
    TARGET_COLUMN,
    add_turnover_constrained_targets,
    complete_matured_decision_dates,
    fit_shadow_price_scale,
    rank_turnover_utility_predictions,
    select_minimum_feasible_shadow_price,
    train_turnover_constrained_utility_trial,
    turnover_utility_feature_names,
)


def _labels() -> pd.DataFrame:
    rows = []
    for date_index, decision in enumerate(pd.bdate_range("2026-01-05", periods=3)):
        target = decision + pd.offsets.BDay(1)
        for rank in range(1, 21):
            status = "MATURED"
            holding = float((rank % 10) + 1)
            if date_index == 1 and rank == 20:
                status = "NOT_ENTERED_LIMIT_UP"
                holding = np.nan
            rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": target,
                    "instrument": f"S{rank:02d}",
                    "label_status": status,
                    "holding_trading_days": holding,
                    "net_excess_return_bps": float(100 - rank),
                }
            )
    return pd.DataFrame(rows)


def test_turnover_liability_units_and_non_matured_rows_are_not_filled() -> None:
    labels = _labels()
    adjusted = add_turnover_constrained_targets(
        labels,
        target_count=5,
        shadow_price_bps_per_fraction=100.0,
    )
    first = adjusted.iloc[0]
    assert first[LIABILITY_COLUMN] == pytest.approx(2.0 / (5.0 * 2.0))
    assert first[TARGET_COLUMN] == pytest.approx(first["net_excess_return_bps"] - 20.0)
    non_matured = adjusted[adjusted["label_status"] != "MATURED"]
    assert non_matured[LIABILITY_COLUMN].isna().all()
    assert non_matured[TARGET_COLUMN].isna().all()


def test_constraint_calibration_dates_require_exact_twenty_matured_rows() -> None:
    dates, receipt = complete_matured_decision_dates(_labels())
    assert len(dates) == 2
    assert pd.Timestamp("2026-01-06") not in dates
    assert receipt["excluded_decision_count"] == 1
    assert receipt["label_status_counts"]["NOT_ENTERED_LIMIT_UP"] == 1


def test_shadow_price_scale_and_minimum_feasible_selection_are_frozen() -> None:
    labels = _labels()
    matured = labels[labels["label_status"] == "MATURED"]
    fit = fit_shadow_price_scale(matured, target_count=5)
    assert fit.candidates_bps_per_fraction[0] == 0.0
    assert len(fit.candidates_bps_per_fraction) == 8
    second = fit.candidates_bps_per_fraction[1]
    selected = select_minimum_feasible_shadow_price(
        scale_fit=fit,
        p0d_train_turnover_budget=0.25,
        evaluate_oracle_turnover=lambda price: 0.30 if price < second else 0.24,
    )
    assert selected.shadow_price_bps_per_fraction == second
    assert selected.oracle_train_turnover == 0.24
    assert selected.constraint_slack == pytest.approx(0.01)


def test_shadow_price_constraint_fails_closed_without_feasible_candidate() -> None:
    fit = fit_shadow_price_scale(_labels().query("label_status == 'MATURED'"), target_count=5)
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        select_minimum_feasible_shadow_price(
            scale_fit=fit,
            p0d_train_turnover_budget=0.10,
            evaluate_oracle_turnover=lambda _: 0.20,
        )
    assert excinfo.value.reason_code == "ADVISORY_TURNOVER_UTILITY_CONSTRAINT_INFEASIBLE"


def test_turnover_utility_rank_is_exact_top20_and_score_kind_is_explicit() -> None:
    frame = pd.DataFrame(
        {
            "decision_as_of_trade_date": ["2026-02-02"] * 20,
            "target_trade_date": ["2026-02-03"] * 20,
            "instrument": [f"S{i:02d}" for i in range(20)],
            "selection_effective_rank": list(range(20, 0, -1)),
            SCORE_COLUMN: [1.0] * 20,
        }
    )
    ranked = rank_turnover_utility_predictions(frame)
    assert ranked["entry_priority_rank"].tolist() == list(range(1, 21))
    assert ranked["selection_effective_rank"].tolist() == list(range(1, 21))
    assert ranked["selection_exit_rank"].equals(ranked["selection_effective_rank"])
    assert set(ranked["entry_priority_score_kind"]) == {"TURNOVER_CONSTRAINED_POLICY_UTILITY_BPS"}
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        rank_turnover_utility_predictions(frame.iloc[:-1])
    assert excinfo.value.reason_code == "ADVISORY_TURNOVER_UTILITY_PRIORITY_INVALID"


def test_turnover_utility_features_never_include_future_holding_label() -> None:
    for family in approved_turnover_constrained_utility_families():
        names = turnover_utility_feature_names(family)
        assert "holding_trading_days" not in names
        assert "net_excess_return_bps" not in names


def test_turnover_utility_lightgbm_trial_scores_all_top20() -> None:
    pytest.importorskip("lightgbm")
    rng = np.random.default_rng(20260825)
    dates = pd.bdate_range("2026-01-05", periods=16)
    feature_rows = []
    label_rows = []
    for date_index, decision in enumerate(dates):
        target = decision + pd.offsets.BDay(1)
        for rank in range(1, 21):
            feature = {
                "decision_as_of_trade_date": decision,
                "target_trade_date": target,
                "instrument": f"S{rank:02d}",
                "selection_effective_rank": rank,
            }
            for column in MODEL_FEATURE_COLUMNS:
                feature[column] = 0 if column.endswith("__missing") else float(rng.normal())
            feature["l2_code_id"] = rank % 4
            feature_rows.append(feature)
            label_rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": target,
                    "instrument": f"S{rank:02d}",
                    "label_status": (
                        "CENSORED_RIGHT_BOUNDARY" if date_index == len(dates) - 1 and rank == 20 else "MATURED"
                    ),
                    "holding_trading_days": float((rank % 10) + 1),
                    "net_excess_return_bps": feature["parent_combined_score"] * 100.0 + float(rng.normal()),
                }
            )
    result = train_turnover_constrained_utility_trial(
        features=pd.DataFrame(feature_rows),
        labels=pd.DataFrame(label_rows),
        train_dates=dates[:12],
        validation_dates=dates[12:],
        family=approved_turnover_constrained_utility_families()[0],
        seed=20260813,
        target_count=5,
        shadow_price_bps_per_fraction=100.0,
    )
    assert len(result.validation_predictions) == 80
    assert result.validation_predictions.groupby("decision_as_of_trade_date").size().eq(20).all()
    assert (result.validation_predictions["label_status"] == "CENSORED_RIGHT_BOUNDARY").sum() == 1
    assert result.metrics["candidate_adjusted_mae_bps"] >= 0.0
