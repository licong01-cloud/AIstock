from __future__ import annotations

from itertools import combinations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.leg_disagreement_contracts import (
    LEG_MVE_EXPANDED_FEATURES,
    FrozenLegDisagreementRequestV1,
    build_default_leg_model_trials,
)
from backend.services.advisory_model_first.leg_disagreement_pipeline import (
    build_leg_feature_panel,
    evaluate_leg_models,
    run_leg_crossfit,
)


def _source() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day_index, day in enumerate(pd.bdate_range("2025-01-02", periods=8)):
        for instrument_index in range(10):
            lstm = float(instrument_index + day_index / 100)
            fund = float((instrument_index * 3 + day_index) % 10)
            rows.append(
                {
                    "decision_as_of_trade_date": day,
                    "instrument": f"{instrument_index:06d}.SZ",
                    "score__LSTM_ONLY": lstm,
                    "score__FUNDGROWTH_ONLY": fund,
                    "score__IC_WEIGHTED_PARENT": 0.7 * lstm + 0.3 * fund,
                    "economic_net_excess_bps": 20.0 * lstm + 5.0 * fund,
                    "outcome_known": True,
                    "future_only": 999.0,
                }
            )
    return pd.DataFrame(rows)


def _request() -> FrozenLegDisagreementRequestV1:
    return FrozenLegDisagreementRequestV1.model_construct(
        request_id="advn3legreq_" + "1" * 24,
        request_sha256="1" * 64,
        model_trials=build_default_leg_model_trials(),
        expected_known_row_count=80,
        expected_ready_path_count=28,
        expected_oof_predictions_per_row=7,
        minimum_evaluable_days=8,
        minimum_intervention_days=1,
        minimum_intervention_fraction=0.0,
        minimum_intervention_days_per_regime=0,
        minimum_parent_lift_bps=5.0,
        familywise_hypothesis_count=4,
        block_length_trading_days=2,
        bootstrap_repetitions=100,
        bootstrap_seed=20260902,
    )


def _paths(dates: list[str]) -> list[dict[str, object]]:
    paths: list[dict[str, object]] = []
    for index, validation in enumerate(combinations(range(8), 2)):
        paths.append(
            {
                "path_id": f"path-{index:02d}",
                "status": "READY",
                "train_dates": [day for offset, day in enumerate(dates) if offset not in validation],
                "validation_dates": [day for offset, day in enumerate(dates) if offset in validation],
            }
        )
    return paths


def test_feature_builder_uses_exact_same_date_formulas_and_ignores_future_columns() -> None:
    source = _source()
    baseline = build_leg_feature_panel(source)
    poisoned = source.copy()
    poisoned["future_only"] = -999.0
    poisoned["economic_net_excess_bps"] = 1_000_000.0
    rebuilt = build_leg_feature_panel(poisoned)
    pd.testing.assert_frame_equal(
        baseline[["decision_as_of_trade_date", "instrument", *LEG_MVE_EXPANDED_FEATURES]],
        rebuilt[["decision_as_of_trade_date", "instrument", *LEG_MVE_EXPANDED_FEATURES]],
    )
    assert np.allclose(baseline["leg_rank_signed_gap"], baseline["lstm_rank_pct"] - baseline["fund_rank_pct"])
    assert np.allclose(baseline["leg_rank_abs_gap"], baseline["leg_rank_signed_gap"].abs())
    assert np.allclose(
        baseline["parent_rank_x_agreement"],
        baseline["parent_rank_pct"] * (1.0 - baseline["leg_rank_abs_gap"]),
    )


def test_feature_builder_rejects_duplicate_pit_keys() -> None:
    source = _source()
    duplicated = pd.concat([source, source.iloc[[0]]], ignore_index=True)
    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_leg_feature_panel(duplicated)
    assert caught.value.reason_code == "ADVISORY_N3_LEG_MVE_PIT_LEAKAGE"


def test_crossfit_uses_28_paths_and_exactly_seven_predictions_per_row() -> None:
    features = build_leg_feature_panel(_source())
    dates = [value.date().isoformat() for value in sorted(features["decision_as_of_trade_date"].unique())]
    oof, diagnostics = run_leg_crossfit(features=features, paths=_paths(dates), request=_request())
    assert len(oof) == 80
    assert len(diagnostics) == 56
    assert oof["linear_oof_score_count"].eq(7).all()
    assert oof["expanded_oof_score_count"].eq(7).all()
    assert np.isfinite(oof[["linear_oof_score", "expanded_oof_score"]].to_numpy()).all()


def test_crossfit_scores_typed_missing_rows_without_using_them_as_labels() -> None:
    source = _source()
    source.loc[0, "outcome_known"] = False
    source.loc[0, "economic_net_excess_bps"] = float("nan")
    features = build_leg_feature_panel(source)
    dates = [value.date().isoformat() for value in sorted(features["decision_as_of_trade_date"].unique())]
    oof, _ = run_leg_crossfit(features=features, paths=_paths(dates), request=_request())
    assert len(oof) == len(features)
    assert oof["linear_oof_score_count"].eq(7).all()
    assert oof["expanded_oof_score_count"].eq(7).all()
    missing = oof.loc[~oof["outcome_known"].astype(bool)]
    assert len(missing) == 1
    assert missing["economic_net_excess_bps"].isna().all()
    assert np.isfinite(missing[["linear_oof_score", "expanded_oof_score"]].to_numpy()).all()


def _evaluation_panel(*, expanded_wins: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    regimes: list[dict[str, object]] = []
    for day_index, day in enumerate(pd.bdate_range("2025-02-03", periods=8)):
        regimes.append({"decision_as_of_trade_date": day, "regime": "DOWN" if day_index % 2 else "UP_OR_FLAT"})
        for instrument_index in range(10):
            parent = float(instrument_index)
            label = 100.0 if instrument_index < 5 else 0.0
            expanded = float(10 - instrument_index) if expanded_wins else parent
            rows.append(
                {
                    "decision_as_of_trade_date": day,
                    "instrument": f"{instrument_index:06d}.SZ",
                    "parent_rank_pct": parent,
                    "economic_net_excess_bps": label,
                    "outcome_known": True,
                    "linear_oof_score": parent,
                    "expanded_oof_score": expanded,
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(regimes)


def test_evaluator_selects_expanded_only_when_all_paired_gates_pass() -> None:
    scores, regimes = _evaluation_panel(expanded_wins=True)
    daily, summary, frontier = evaluate_leg_models(oof_scores=scores, regime_daily=regimes, request=_request())
    assert len(daily) == 8
    assert summary["support"]["support_sufficient"] is True
    assert summary["eligible"] is True
    assert frontier["selected_trial_id"] == "N3_LEG_DISAGREEMENT_EXPANDED_V1"
    assert summary["inference"]["expanded_parent_top5_lift_bps"]["familywise_confidence_lower"] > 5.0


def test_evaluator_routes_zero_when_expanded_does_not_intervene() -> None:
    scores, regimes = _evaluation_panel(expanded_wins=False)
    _, summary, frontier = evaluate_leg_models(oof_scores=scores, regime_daily=regimes, request=_request())
    assert summary["eligible"] is False
    assert "INTERVENTION_DAY_COUNT_BELOW_MINIMUM" in summary["reason_codes"]
    assert frontier["selected_trial_id"] is None


def test_evaluator_preserves_typed_missing_top5_day_without_partial_portfolio_mean() -> None:
    scores, regimes = _evaluation_panel(expanded_wins=True)
    first_day = scores["decision_as_of_trade_date"].min()
    missing = (scores["decision_as_of_trade_date"] == first_day) & (scores["instrument"] == "000000.SZ")
    scores.loc[missing, "outcome_known"] = False
    scores.loc[missing, "economic_net_excess_bps"] = float("nan")
    daily, summary, frontier = evaluate_leg_models(oof_scores=scores, regime_daily=regimes, request=_request())
    first = daily.loc[daily["decision_as_of_trade_date"] == first_day].iloc[0]
    assert bool(first["expanded_top5_evaluable"]) is False
    assert np.isnan(first["expanded_top5_net_excess_bps"])
    assert bool(first["evaluable"]) is False
    assert summary["support"]["evaluable_day_count"] == 7
    assert "EVALUABLE_DAY_COUNT_BELOW_MINIMUM" in summary["reason_codes"]
    assert frontier["selected_trial_id"] is None
