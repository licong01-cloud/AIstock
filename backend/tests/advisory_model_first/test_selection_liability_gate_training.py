from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.dual_head_output_constraint_training import (
    LIABILITY_SCORE_COLUMN,
    LIABILITY_TARGET_COLUMN,
    build_inner_fold_specs,
    eligible_constraint_dates,
    train_liability_head_oof,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.selection_liability_gate_training import (
    assert_widest_gate_metrics_match_selection,
    assert_widest_gate_matches_selection,
    build_selection_preserving_gate_priorities,
    selection_liability_gate_candidate_metrics,
    select_widest_feasible_liability_threshold,
)
from backend.services.advisory_model_first.selection_liability_gate_contracts import (
    approved_selection_liability_gate_families,
)
from backend.tests.advisory_model_first.test_dual_head_output_constraint_training import (
    _features,
    _labels,
)


def _predictions() -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-05", periods=2)
    return pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": day,
                "target_trade_date": day + pd.offsets.BDay(1),
                "instrument": f"{rank:06d}.SZ",
                "selection_effective_rank": rank,
                LIABILITY_SCORE_COLUMN: rank * 0.02,
            }
            for day in dates
            for rank in range(1, 21)
        ]
    )


def _selection(predictions: pd.DataFrame) -> pd.DataFrame:
    return predictions[
        ["decision_as_of_trade_date", "instrument", "selection_effective_rank"]
    ].rename(columns={"selection_effective_rank": "entry_priority_rank"})


def test_gate_filters_only_by_liability_and_preserves_dense_selection_order() -> None:
    predictions = _predictions()
    gate = build_selection_preserving_gate_priorities(
        predictions,
        maximum_liability_threshold=0.2,
    )
    assert set(gate.eligible_count_by_date.values()) == {10}
    assert gate.rejected_count == 20
    for _, rows in gate.priorities.groupby("decision_as_of_trade_date"):
        assert rows["selection_effective_rank"].tolist() == list(range(1, 11))
        assert rows["entry_priority_rank"].tolist() == list(range(1, 11))


def test_gate_rejects_prediction_without_target_clock_as_typed_error() -> None:
    with pytest.raises(AdvisoryModelFirstError) as exc:
        build_selection_preserving_gate_priorities(
            _predictions().drop(columns="target_trade_date"),
            maximum_liability_threshold=0.2,
        )
    assert exc.value.reason_code == "ADVISORY_DUAL_HEAD_OOF_INVALID"


def test_widest_threshold_exactly_matches_selection_and_detects_rank_drift() -> None:
    predictions = _predictions()
    widest = build_selection_preserving_gate_priorities(
        predictions,
        maximum_liability_threshold=0.4,
    )
    selection = _selection(predictions)
    assert_widest_gate_matches_selection(widest.priorities, selection)
    selection.loc[selection.index[0], "entry_priority_rank"] = 2
    with pytest.raises(AdvisoryModelFirstError) as exc:
        assert_widest_gate_matches_selection(widest.priorities, selection)
    assert exc.value.reason_code == "ADVISORY_P0K_SELECTION_EQUIVALENCE_FAILED"


def test_widest_threshold_requires_exact_daily_policy_equivalence() -> None:
    selection = {
        "mean_turnover_fraction": 0.1,
        "active_slot_coverage": 1.0,
        "cash_day_count": 0,
        "day_count": 1,
        "daily_completeness": [
            {
                "decision_as_of_trade_date": "2026-01-05",
                "active_count": 5,
                "cash_slot_count": 0,
                "turnover_fraction": 0.1,
            }
        ],
    }
    assert_widest_gate_metrics_match_selection(dict(selection), selection)
    drifted = dict(selection)
    drifted["daily_completeness"] = [
        {
            "decision_as_of_trade_date": "2026-01-05",
            "active_count": 4,
            "cash_slot_count": 1,
            "turnover_fraction": 0.1,
        }
    ]
    with pytest.raises(AdvisoryModelFirstError) as exc:
        assert_widest_gate_metrics_match_selection(drifted, selection)
    assert exc.value.reason_code == "ADVISORY_P0K_SELECTION_EQUIVALENCE_FAILED"


def test_threshold_selector_chooses_first_widest_complete_budget_feasible_value() -> None:
    predictions = _predictions()

    def evaluate(priorities: pd.DataFrame) -> dict[str, object]:
        count = len(priorities) // 2
        return {
            "mean_turnover_fraction": 0.2 if count == 20 else 0.09,
            "active_slot_coverage": 1.0,
            "cash_day_count": 0,
            "complete": True,
        }

    selected = select_widest_feasible_liability_threshold(
        predictions=predictions,
        thresholds=(0.4, 0.2, 0.13333333333333333, 0.08, 0.04, 0.02),
        p0d_oof_turnover_budget=0.1,
        evaluate=evaluate,
    )
    assert selected.maximum_liability_threshold == 0.2
    assert selected.p0k_oof_turnover == 0.09
    assert [item["maximum_liability_threshold"] for item in selected.evaluations] == [0.4, 0.2]


def test_threshold_selector_fails_closed_instead_of_filling_rejected_candidates() -> None:
    with pytest.raises(AdvisoryModelFirstError) as exc:
        select_widest_feasible_liability_threshold(
            predictions=_predictions(),
            thresholds=(0.4, 0.2, 0.13333333333333333, 0.08, 0.04, 0.02),
            p0d_oof_turnover_budget=0.1,
            evaluate=lambda _: {"mean_turnover_fraction": 0.2, "complete": False},
        )
    assert exc.value.reason_code == "ADVISORY_P0K_LIABILITY_GATE_INFEASIBLE"


def test_candidate_return_is_diagnostic_only_and_cannot_change_gate_priority() -> None:
    predictions = _predictions()
    diagnostic = predictions.copy()
    diagnostic["label_status"] = "MATURED"
    diagnostic["net_excess_return_bps"] = range(len(diagnostic))
    diagnostic[LIABILITY_TARGET_COLUMN] = diagnostic[LIABILITY_SCORE_COLUMN]
    before = build_selection_preserving_gate_priorities(
        predictions,
        maximum_liability_threshold=0.2,
    ).priorities
    metrics = selection_liability_gate_candidate_metrics(
        diagnostic,
        maximum_liability_threshold=0.2,
    )
    diagnostic["net_excess_return_bps"] = diagnostic["net_excess_return_bps"] * -1000
    after = build_selection_preserving_gate_priorities(
        predictions,
        maximum_liability_threshold=0.2,
    ).priorities
    assert before.equals(after)
    assert metrics["liability_mae"] == 0.0
    assert "accepted_candidate_mean_return_bps_diagnostic_only" in metrics


def test_public_liability_head_oof_scores_all_top20_without_return_output() -> None:
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
    folds = build_inner_fold_specs(
        labels=labels,
        outer_train_dates=all_dates,
        eligible_dates=eligible,
        block_by_date={
            value.date().isoformat(): index % 4 for index, value in enumerate(all_dates)
        },
        trading_calendar=pd.bdate_range("2025-12-01", "2026-04-30"),
        embargo_trading_days=0,
    )
    result = train_liability_head_oof(
        features=features,
        labels=labels,
        folds=folds,
        family=approved_selection_liability_gate_families()[0],
        seed=20260813,
    )
    assert len(result.predictions) == (periods - 1) * 20
    assert result.predictions[LIABILITY_SCORE_COLUMN].between(0.02, 0.4).all()
    assert "predicted_policy_net_excess_return_bps" not in result.predictions


def test_future_return_poison_cannot_change_liability_split_identity() -> None:
    labels = _labels(12)
    dates = pd.DatetimeIndex(labels["decision_as_of_trade_date"].unique()).normalize()
    block_by_date = {
        value.date().isoformat(): index % 4 for index, value in enumerate(dates)
    }
    kwargs = {
        "outer_train_dates": dates,
        "eligible_dates": dates[:-1],
        "block_by_date": block_by_date,
        "trading_calendar": pd.bdate_range("2025-12-01", "2026-04-30"),
        "embargo_trading_days": 0,
    }
    clean = build_inner_fold_specs(labels=labels, **kwargs)
    poisoned = labels.copy()
    poisoned["net_excess_return_bps"] = range(len(poisoned), 0, -1)
    assert build_inner_fold_specs(labels=poisoned, **kwargs) == clean
