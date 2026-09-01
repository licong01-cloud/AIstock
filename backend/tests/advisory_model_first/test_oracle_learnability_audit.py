from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.feature_schema_v2 import (
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.tier1_oracle_pipeline import (
    _intervention_support,
    build_n1_cpcv_payload,
    run_fixed_learnability_audit,
)
from backend.tests.advisory_model_first.test_oracle_mini_contract import _request


def _audit_inputs():
    dates = pd.bdate_range("2024-07-04", periods=385).append(pd.DatetimeIndex([pd.Timestamp("2026-02-02")]))
    calendar = pd.bdate_range("2024-07-04", "2026-03-10")
    positions = {date_: index for index, date_ in enumerate(calendar)}
    rows = []
    labels = []
    for date_index, decision in enumerate(dates):
        for rank in range(1, 21):
            symbol = f"{rank:06d}.SZ"
            row = {
                "decision_as_of_trade_date": decision,
                "instrument": symbol,
                "selection_effective_rank": rank,
            }
            for column in MODEL_FEATURE_COLUMNS:
                row[column] = (
                    "sector-a"
                    if column in CATEGORICAL_FEATURE_COLUMNS
                    else float(rank)
                    if column == "parent_combined_score"
                    else float(date_index % 5) / 10.0
                )
            rows.append(row)
            labels.append(
                {
                    "decision_as_of_trade_date": decision,
                    "instrument": symbol,
                    "selection_effective_rank": rank,
                    "target_trade_date": calendar[positions[decision] + 1],
                    "effective_exit_trade_date": calendar[positions[decision] + 20],
                    "slot_return_bps": float((rank - 10) * 10),
                    "outcome_known": True,
                }
            )
    labels_frame = pd.DataFrame(labels)
    paths = build_n1_cpcv_payload(
        candidate_labels=labels_frame,
        trading_calendar=calendar,
        request=_request(),
    )
    benchmark_dates = pd.bdate_range("2024-06-03", "2026-02-02")
    benchmark = pd.DataFrame(
        {
            "datetime": benchmark_dates,
            "instrument": "000300.SH",
            "open": 100.0,
            "close": np.linspace(90.0, 110.0, len(benchmark_dates)),
        }
    ).set_index(["datetime", "instrument"])
    return pd.DataFrame(rows), labels_frame, paths, benchmark


def test_fixed_crossfit_produces_exact_seven_oof_predictions_and_support() -> None:
    features, labels, paths, benchmark = _audit_inputs()

    result = run_fixed_learnability_audit(
        features=features,
        candidate_labels=labels,
        cpcv_payload=paths,
        benchmark_daily=benchmark,
        request=_request(),
    )

    assert len(result.oof_predictions) == 386 * 20
    assert set(result.oof_predictions["oof_prediction_count"]) == {7}
    assert len(result.daily) == 386
    assert result.daily["intervened"].all()
    assert result.intervention_support.support_sufficient
    assert result.lift.point_estimate_bps > 0.0
    validation_multiplicity: dict[str, int] = {}
    for path in paths["paths"]:
        assert set(path["train_dates"]).isdisjoint(path["validation_dates"])
        for decision_date in path["validation_dates"]:
            validation_multiplicity[decision_date] = validation_multiplicity.get(decision_date, 0) + 1
    assert len(paths["paths"]) == 28
    assert set(validation_multiplicity.values()) == {7}


def test_validation_label_poison_does_not_change_its_cross_fitted_prediction() -> None:
    features, labels, paths, benchmark = _audit_inputs()
    baseline = run_fixed_learnability_audit(
        features=features,
        candidate_labels=labels,
        cpcv_payload=paths,
        benchmark_daily=benchmark,
        request=_request(),
    )
    poisoned_labels = labels.copy()
    poisoned_labels.loc[0, "slot_return_bps"] = 1_000_000.0
    poisoned = run_fixed_learnability_audit(
        features=features,
        candidate_labels=poisoned_labels,
        cpcv_payload=paths,
        benchmark_daily=benchmark,
        request=_request(),
    )

    key = ["decision_as_of_trade_date", "instrument", "selection_effective_rank"]
    target = labels.loc[0, key].to_dict()
    first = baseline.oof_predictions
    second = poisoned.oof_predictions
    for column, value in target.items():
        first = first[first[column] == value]
        second = second[second[column] == value]
    assert len(first) == len(second) == 1
    assert first.iloc[0]["predicted_slot_return_bps"] == second.iloc[0]["predicted_slot_return_bps"]


def test_intervention_support_counts_zero_for_an_observed_regime() -> None:
    daily = pd.DataFrame(
        {
            "intervened": [True] * 60 + [False] * 60,
            "regime": ["UP_OR_FLAT"] * 60 + ["DOWN"] * 60,
        }
    )

    support = _intervention_support(daily, request=_request())

    assert support.intervention_days_by_regime == {"DOWN": 0, "UP_OR_FLAT": 60}
    assert not support.support_sufficient
    assert "EXPLORATORY_INSUFFICIENT_REGIME_SUPPORT" in support.reason_codes
