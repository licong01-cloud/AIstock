from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.p0g_anchored_liability_local_reranker_training import (
    ENTRY_PRIORITY_COLUMN,
    build_local_rerank_priorities,
    compare_policy_entries_and_completeness,
    select_minimum_feasible_gain,
)
from backend.services.advisory_model_first.turnover_constrained_utility_training import SCORE_COLUMN


def _predictions() -> tuple[pd.DataFrame, pd.DataFrame]:
    date = pd.Timestamp("2025-01-02")
    rows = [
        {
            "decision_as_of_trade_date": date,
            "target_trade_date": date + pd.Timedelta(days=1),
            "instrument": f"{rank:06d}.SZ",
            "selection_effective_rank": rank,
            ENTRY_PRIORITY_COLUMN: rank,
            SCORE_COLUMN: float(21 - rank),
        }
        for rank in range(1, 21)
    ]
    anchor = pd.DataFrame(rows)
    liability = anchor[
        [
            "decision_as_of_trade_date",
            "target_trade_date",
            "instrument",
            "selection_effective_rank",
        ]
    ].copy()
    liability["predicted_turnover_liability_fraction_per_day"] = [
        0.30 if rank == 5 else (0.02 if rank == 6 else 0.05 + rank / 1000)
        for rank in range(1, 21)
    ]
    return anchor, liability


def test_local_reranker_is_identity_control_or_one_top5_frontier_swap() -> None:
    anchor, liability = _predictions()
    identity = build_local_rerank_priorities(
        anchor,
        liability,
        liability_rank_gain_required=None,
    )
    assert identity.changed_decision_count == 0
    assert identity.priorities[ENTRY_PRIORITY_COLUMN].tolist() == list(range(1, 21))

    changed = build_local_rerank_priorities(
        anchor,
        liability,
        liability_rank_gain_required=12,
    )
    by_symbol = changed.priorities.set_index("instrument")[ENTRY_PRIORITY_COLUMN]
    assert by_symbol["000005.SZ"] == 6
    assert by_symbol["000006.SZ"] == 5
    assert changed.changed_decision_count == 1
    assert changed.changed_candidate_row_count == 2
    assert changed.top5_boundary_change_count == 1
    assert len(changed.selected_swaps) == 1


def test_local_reranker_never_moves_tail_pairs_or_more_than_one_position() -> None:
    anchor, liability = _predictions()
    liability.loc[liability["selection_effective_rank"] == 15, "predicted_turnover_liability_fraction_per_day"] = 0.3
    liability.loc[liability["selection_effective_rank"] == 16, "predicted_turnover_liability_fraction_per_day"] = 0.02
    changed = build_local_rerank_priorities(
        anchor,
        liability,
        liability_rank_gain_required=12,
    )
    by_symbol = changed.priorities.set_index("instrument")[ENTRY_PRIORITY_COLUMN]
    assert by_symbol["000015.SZ"] == 15
    assert by_symbol["000016.SZ"] == 16
    displacement = (
        changed.priorities[ENTRY_PRIORITY_COLUMN]
        - changed.priorities["anchor_entry_priority_rank"]
    ).abs()
    assert displacement.max() == 1


def test_selector_requires_real_entry_change_and_p0d_turnover() -> None:
    anchor, liability = _predictions()
    anchor_metrics = {
        "mean_turnover_fraction": 0.25,
        "active_slot_coverage": 1.0,
        "cash_day_count": 0,
        "day_count": 1,
    }

    def evaluate(priorities: pd.DataFrame) -> dict[str, object]:
        changed = not priorities.loc[
            priorities["instrument"] == "000006.SZ", ENTRY_PRIORITY_COLUMN
        ].eq(6).all()
        return {
            **anchor_metrics,
            "mean_turnover_fraction": 0.20 if changed else 0.25,
            "actual_entry_change_count": 1 if changed else 0,
            "complete": True,
        }

    selected = select_minimum_feasible_gain(
        anchor_predictions=anchor,
        liability_predictions=liability,
        gain_roster=(12, 8, 4, 1),
        p0d_oof_turnover_budget=0.22,
        anchor_metrics=anchor_metrics,
        evaluate=evaluate,
    )
    assert selected.liability_rank_gain_required == 12
    assert selected.actual_entry_change_count == 1
    assert selected.p0l_oof_turnover == 0.20


def test_selector_rejects_priority_only_noop() -> None:
    anchor, liability = _predictions()
    metrics = {
        "mean_turnover_fraction": 0.20,
        "active_slot_coverage": 1.0,
        "cash_day_count": 0,
        "day_count": 1,
    }

    def evaluate(_: pd.DataFrame) -> dict[str, object]:
        return {**metrics, "actual_entry_change_count": 0, "complete": True}

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        select_minimum_feasible_gain(
            anchor_predictions=anchor,
            liability_predictions=liability,
            gain_roster=(12, 8, 4, 1),
            p0d_oof_turnover_budget=0.22,
            anchor_metrics=metrics,
            evaluate=evaluate,
        )
    assert exc_info.value.reason_code == "ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE"


def test_policy_comparison_counts_real_enter_changes_and_checks_completeness() -> None:
    dates = pd.bdate_range("2026-01-05", periods=2)
    anchor_daily = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "turnover_fraction": [0.2, 0.0],
            "active_count": [5, 5],
            "cash_slot_count": [0, 0],
        }
    )
    anchor_episodes = pd.DataFrame(
        {"entry_signal_date": [dates[0]], "instrument": ["000001.SZ"]}
    )
    candidate_episodes = pd.DataFrame(
        {"entry_signal_date": [dates[0]], "instrument": ["000002.SZ"]}
    )
    metrics = compare_policy_entries_and_completeness(
        candidate_daily=anchor_daily.copy(),
        candidate_episodes=candidate_episodes,
        anchor_daily=anchor_daily,
        anchor_episodes=anchor_episodes,
        expected_dates=dates,
    )
    assert metrics["actual_entry_change_count"] == 2
    assert metrics["actual_entry_changed_decision_count"] == 1
    assert metrics["complete"] is True


def test_policy_comparison_rejects_daily_completeness_regression_even_if_aggregate_matches() -> None:
    dates = pd.bdate_range("2026-01-05", periods=2)
    anchor = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "turnover_fraction": [0.2, 0.2],
            "active_count": [5, 4],
            "cash_slot_count": [0, 1],
        }
    )
    candidate = anchor.copy()
    candidate["active_count"] = [4, 5]
    candidate["cash_slot_count"] = [1, 0]
    metrics = compare_policy_entries_and_completeness(
        candidate_daily=candidate,
        candidate_episodes=pd.DataFrame(),
        anchor_daily=anchor,
        anchor_episodes=pd.DataFrame(),
        expected_dates=dates,
    )
    assert metrics["active_slot_coverage"] == metrics["anchor_active_slot_coverage"]
    assert metrics["cash_day_count"] == metrics["anchor_cash_day_count"]
    assert metrics["complete"] is False
