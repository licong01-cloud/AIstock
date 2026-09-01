from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v2 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.policy_utility_contracts import (
    approved_policy_utility_arms,
    approved_policy_utility_families,
)
from backend.services.advisory_model_first.policy_utility_training import (
    _training_params,
    apply_policy_utility_transform,
    fit_policy_utility_transform,
    inverse_policy_utility_transform,
    rank_policy_utility_predictions,
    train_policy_utility_trial,
)


def test_policy_utility_train_only_median_mad_and_inverse() -> None:
    train = np.array([-10.0, 0.0, 10.0, 20.0, 1000.0])
    fit = fit_policy_utility_transform(train)
    assert fit.location_bps == 10.0
    assert fit.scale_bps == 10.0
    transformed = apply_policy_utility_transform(np.array([-10.0, 20.0]), fit)
    assert transformed.tolist() == [-2.0, 1.0]
    assert inverse_policy_utility_transform(transformed, fit).tolist() == [-10.0, 20.0]
    poisoned_validation = np.array([-1e12, 1e12])
    assert fit_policy_utility_transform(train) == fit
    assert np.isfinite(apply_policy_utility_transform(poisoned_validation, fit)).all()


@pytest.mark.parametrize("values", [[1.0, 1.0], [1.0, np.nan], [1.0, np.inf]])
def test_policy_utility_transform_fails_closed(values) -> None:
    with pytest.raises(AdvisoryModelFirstError):
        fit_policy_utility_transform(np.asarray(values))


def test_policy_utility_rank_is_exact_top20_and_deterministic() -> None:
    frame = pd.DataFrame(
        {
            "decision_as_of_trade_date": ["2026-02-02"] * 20,
            "target_trade_date": ["2026-02-03"] * 20,
            "instrument": [f"S{i:02d}" for i in range(20)],
            "selection_effective_rank": list(range(20, 0, -1)),
            "predicted_policy_net_excess_return_bps": [1.0] * 20,
        }
    )
    ranked = rank_policy_utility_predictions(frame)
    assert ranked["entry_priority_rank"].tolist() == list(range(1, 21))
    assert ranked["selection_effective_rank"].tolist() == list(range(1, 21))
    assert ranked["selection_exit_rank"].equals(ranked["selection_effective_rank"])
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        rank_policy_utility_predictions(frame.iloc[:-1])
    assert excinfo.value.reason_code == "ADVISORY_POLICY_UTILITY_TOP20_INVALID"


def test_policy_utility_huber_parameters_are_frozen() -> None:
    family = approved_policy_utility_families()[0]
    params = _training_params(family, 20260813)
    assert params["objective"] == "huber"
    assert params["alpha"] == 0.9
    assert params["metric"] == "l1"
    assert params["num_leaves"] == 15
    assert params["num_threads"] == 4
    assert params["seed"] == 20260813


def test_policy_utility_lightgbm_trial_scores_all_top20_but_trains_only_matured() -> None:
    pytest.importorskip("lightgbm")
    rng = np.random.default_rng(20260824)
    dates = pd.bdate_range("2026-01-05", periods=16)
    feature_rows = []
    label_rows = []
    for date_index, decision in enumerate(dates):
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
            feature_rows.append(row)
            label_rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": target,
                    "instrument": f"S{rank:02d}",
                    "label_status": ("CENSORED" if date_index == len(dates) - 1 and rank == 20 else "MATURED"),
                    "net_excess_return_bps": row["parent_combined_score"] * 100.0 + float(rng.normal()),
                    "take_label": rank <= 5,
                }
            )
    result = train_policy_utility_trial(
        features=pd.DataFrame(feature_rows),
        labels=pd.DataFrame(label_rows),
        train_dates=dates[:12],
        validation_dates=dates[12:],
        family=approved_policy_utility_families()[0],
        seed=20260813,
    )
    assert len(result.validation_predictions) == 80
    assert result.validation_predictions.groupby("decision_as_of_trade_date").size().eq(20).all()
    assert (result.validation_predictions["label_status"] == "CENSORED").sum() == 1
    assert result.metrics["candidate_mae_bps"] >= 0.0


@pytest.mark.parametrize("arm_index", [0, 1])
def test_policy_utility_binary_arms_score_all_top20(arm_index: int) -> None:
    pytest.importorskip("lightgbm")
    rng = np.random.default_rng(20260824 + arm_index)
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
                    "label_status": "CENSORED" if date_index == 15 and rank == 20 else "MATURED",
                    "take_label": int(rank <= 5),
                    "net_excess_return_bps": float(25 - rank),
                }
            )
    result = train_policy_utility_trial(
        features=pd.DataFrame(feature_rows),
        labels=pd.DataFrame(label_rows),
        train_dates=dates[:12],
        validation_dates=dates[12:],
        family=approved_policy_utility_families()[0],
        seed=20260813,
        arm=approved_policy_utility_arms()[arm_index],
    )
    assert len(result.validation_predictions) == 80
    assert result.validation_predictions.groupby("decision_as_of_trade_date").size().eq(20).all()
    assert result.validation_predictions["take_probability"].between(0, 1).all()
    assert result.transform is None
    assert (result.outcome_weighting_receipt is not None) is bool(arm_index)
