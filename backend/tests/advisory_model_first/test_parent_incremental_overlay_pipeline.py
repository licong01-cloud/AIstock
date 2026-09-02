from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.parent_incremental_overlay_contracts import (
    PARENT_OVERLAY_CANDIDATES,
)
from backend.services.advisory_model_first.parent_incremental_overlay_pipeline import (
    build_overlay_scores,
    evaluate_overlay_trials,
)
from backend.tests.advisory_model_first.test_parent_incremental_overlay_contracts import (
    make_parent_overlay_request,
)


def _source_panel() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.date_range("2025-01-02", periods=4, freq="B")
    for date_index, decision_date in enumerate(dates):
        for stock_index in range(12):
            candidate = float(stock_index)
            if stock_index == 6:
                candidate = 100.0
            elif stock_index == 7:
                candidate = -100.0
            row: dict[str, object] = {
                "decision_as_of_trade_date": decision_date,
                "instrument": f"{stock_index + 1:06d}.SZ",
                "score": float(stock_index),
                "economic_net_excess_bps": float(100 - abs(stock_index - 6) * 10),
                "outcome_known": True,
            }
            for candidate_id in PARENT_OVERLAY_CANDIDATES:
                row[candidate_id] = candidate
            if date_index == 0:
                row["N3_REGIME_CONDITIONED_02"] = 0.0
            if date_index == 1 and stock_index == 6:
                row["N3_CROWDING_DISPERSION_04"] = np.nan
            rows.append(row)
    return pd.DataFrame(rows)


def test_overlay_missing_and_inactive_regime_fall_back_exactly(tmp_path) -> None:
    request = make_parent_overlay_request(tmp_path)
    source = _source_panel()

    scores, activity = build_overlay_scores(source, request=request)

    for trial in request.trials:
        assert np.array_equal(np.isfinite(scores[trial.trial_id]), np.isfinite(scores["parent_rank"]))
    regime_trial = next(item for item in request.trials if item.candidate_id == "N3_REGIME_CONDITIONED_02")
    first_day = scores["decision_as_of_trade_date"] == pd.Timestamp("2025-01-02")
    assert np.allclose(scores.loc[first_day, regime_trial.trial_id], scores.loc[first_day, "parent_rank"])
    assert not bool(
        activity.loc[
            (activity["candidate_id"] == "N3_REGIME_CONDITIONED_02")
            & (activity["decision_as_of_trade_date"] == pd.Timestamp("2025-01-02")),
            "candidate_active",
        ].iloc[0]
    )
    missing_trial = next(item for item in request.trials if item.candidate_id == "N3_CROWDING_DISPERSION_04")
    missing_row = (scores["decision_as_of_trade_date"] == pd.Timestamp("2025-01-03")) & (
        scores["instrument"] == "000007.SZ"
    )
    assert scores.loc[missing_row, missing_trial.trial_id].iloc[0] == scores.loc[missing_row, "parent_rank"].iloc[0]


def test_outcome_poison_does_not_change_overlay_scores(tmp_path) -> None:
    request = make_parent_overlay_request(tmp_path)
    source = _source_panel()
    poisoned = source.copy()
    poisoned["economic_net_excess_bps"] = poisoned["economic_net_excess_bps"] * -999.0

    clean_scores, _ = build_overlay_scores(source, request=request)
    poison_scores, _ = build_overlay_scores(poisoned, request=request)

    pd.testing.assert_frame_equal(clean_scores, poison_scores)


def test_later_date_candidate_poison_does_not_change_earlier_scores(tmp_path) -> None:
    request = make_parent_overlay_request(tmp_path)
    source = _source_panel()
    poisoned = source.copy()
    last_day = poisoned["decision_as_of_trade_date"] == poisoned["decision_as_of_trade_date"].max()
    poisoned.loc[last_day, "N3_CROWDING_DISPERSION_01"] *= -10_000.0

    clean_scores, _ = build_overlay_scores(source, request=request)
    poison_scores, _ = build_overlay_scores(poisoned, request=request)
    earlier = clean_scores["decision_as_of_trade_date"] < clean_scores["decision_as_of_trade_date"].max()

    pd.testing.assert_frame_equal(clean_scores.loc[earlier], poison_scores.loc[earlier])


def test_duplicate_pit_key_fails_closed(tmp_path) -> None:
    request = make_parent_overlay_request(tmp_path)
    source = _source_panel()
    duplicated = pd.concat([source, source.iloc[[0]]], ignore_index=True)

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        build_overlay_scores(duplicated, request=request)
    assert exc_info.value.reason_code == "ADVISORY_N3_PARENT_OVERLAY_PIT_LEAKAGE"


def test_evaluation_reports_intervention_and_selects_once(monkeypatch, tmp_path) -> None:
    request = make_parent_overlay_request(tmp_path).model_copy(
        update={
            "minimum_evaluable_days": 1,
            "minimum_intervention_days": 1,
            "minimum_intervention_fraction": 0.0,
            "minimum_intervention_quarters": 1,
            "bootstrap_repetitions": 10,
        }
    )
    source = _source_panel()
    scores, activity = build_overlay_scores(source, request=request)
    monkeypatch.setattr(
        "backend.services.advisory_model_first.parent_incremental_overlay_pipeline._moving_block_interval",
        lambda *args, **kwargs: (0.01, 0.02),
    )

    result_panel, daily, summary, frontier = evaluate_overlay_trials(
        source_panel=source,
        overlay_scores=scores,
        activity=activity,
        request=request,
    )

    assert len(summary["trials"]) == 24
    assert len(daily) == 24 * 4
    assert len(result_panel) == len(source)
    assert any(item["intervention_day_count"] > 0 for item in summary["trials"])
    assert frontier["selected_trial_count"] == 1
    assert frontier["candidate_reselection_allowed"] is False
    assert frontier["selected_trial_id"] in frontier["eligible_trial_ids"]
