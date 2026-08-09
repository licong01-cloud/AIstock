from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.fresh_hmm import continue_sector_hmm, fit_fresh_sector_hmm


def test_fresh_hmm_fits_file_observations_and_saves_continuation_state() -> None:
    dates = pd.bdate_range("2024-01-02", periods=170)
    members = [f"{index:06d}.SZ" for index in range(1, 13)]
    index = pd.MultiIndex.from_product([dates, members], names=["datetime", "instrument"])
    static = pd.DataFrame(index=index)
    static["l2_code_id"] = np.tile([1] * 6 + [2] * 6, len(dates))
    regime = np.concatenate([np.linspace(100, 90, 85), np.linspace(90, 120, 85)])
    other_regime = np.concatenate([np.linspace(100, 115, 85), np.linspace(115, 95, 85)])
    static["sw2_close"] = np.concatenate(
        [np.repeat([first] * 6 + [second] * 6, 1) for first, second in zip(regime, other_regime, strict=True)]
    )
    first_amount = np.linspace(1_000_000, 2_000_000, len(dates))
    second_amount = np.linspace(2_200_000, 1_200_000, len(dates))
    static["sw2_amount"] = np.concatenate(
        [np.repeat([first] * 6 + [second] * 6, 1) for first, second in zip(first_amount, second_amount, strict=True)]
    )
    market = pd.DataFrame({"limit_up": 0.0}, index=index)
    market.loc[(slice(dates[100], dates[110]), members[:6]), "limit_up"] = 1.0
    benchmark = pd.DataFrame(
        {"close": np.linspace(100.0, 105.0, len(dates))},
        index=pd.MultiIndex.from_product([dates, ["000300.SH"]], names=["datetime", "instrument"]),
    )
    result = fit_fresh_sector_hmm(
        static_all=static,
        market_daily=market,
        benchmark_daily=benchmark,
        trading_calendar=dates,
        train_dates=dates[20:150],
        continuation_cutoff=dates[-1].date().isoformat(),
    )
    model = result.models["models"]["1"]
    assert model["canonical_state_labels"] == {"0": "BEAR", "1": "BULL"}
    assert model["continuation_last_observation_date"] == dates[-1].date().isoformat()
    assert result.states["hmm_bull_posterior"].between(0, 1).all()
    assert not any(item["l2_code_id"] == 1 for item in result.unavailable)


def test_fresh_hmm_continuation_starts_from_frozen_posterior() -> None:
    dates = pd.bdate_range("2026-02-02", periods=35)
    symbols = [f"{index:06d}.SZ" for index in range(1, 7)]
    index = pd.MultiIndex.from_product([dates, symbols], names=["datetime", "instrument"])
    day = np.repeat(np.arange(len(dates), dtype=float), len(symbols))
    static = pd.DataFrame(index=index)
    static["l2_code_id"] = 7
    static["sw2_close"] = 100.0 + day
    static["sw2_amount"] = 1_000_000.0 + day * 100.0
    market = pd.DataFrame({"limit_up": 0.0}, index=index)
    benchmark = pd.DataFrame(
        {"close": 3000.0 + np.arange(len(dates), dtype=float)},
        index=pd.MultiIndex.from_product([dates, ["000300.SH"]], names=["datetime", "instrument"]),
    )
    cutoff = dates[-6]
    observation_order = [
        "sector_return_1",
        "sector_excess_20",
        "sector_amount_share",
        "sector_limit_up_ratio",
    ]
    model = {
        "schema_version": "fresh_sector_hmm_v1",
        "l2_code_id": 7,
        "observation_order": observation_order,
        "transform_mean": [0.0, 0.0, 1.0, 0.0],
        "transform_std": [1.0, 1.0, 1.0, 1.0],
        "transmat": [[0.95, 0.05], [0.05, 0.95]],
        "means": [[-1.0, -1.0, 0.0, 0.0], [1.0, 1.0, 0.0, 0.0]],
        "covariances": [np.eye(4).tolist(), np.eye(4).tolist()],
        "canonical_state_by_raw": {"0": 0, "1": 1},
        "continuation_cutoff": cutoff.date().isoformat(),
        "continuation_last_posterior": [0.8, 0.2],
        "continuation_state": 0,
        "continuation_state_duration": 4,
        "continuation_last_observation_date": cutoff.date().isoformat(),
    }
    result = continue_sector_hmm(
        static_all=static,
        market_daily=market,
        benchmark_daily=benchmark,
        trading_calendar=dates,
        hmm_bundle={
            "schema_version": "fresh_sector_hmm_bundle_v1",
            "observation_order": observation_order,
            "models": {"7": model},
        },
        continuation_cutoff=cutoff.date().isoformat(),
        required_l2_code_ids=[7],
    )
    assert result.states["decision_as_of_trade_date"].tolist() == list(dates[-5:])
    assert result.states["hmm_observation_completeness"].tolist() == [1.0] * 5
    assert np.isfinite(result.states["hmm_bull_posterior"]).all()
    assert result.unavailable == ()


def test_fresh_hmm_continuation_marks_absent_sector_without_blocking_group() -> None:
    dates = pd.bdate_range("2026-03-09", periods=5)
    cutoff = dates[1]
    observation_order = [
        "sector_return_1",
        "sector_excess_20",
        "sector_amount_share",
        "sector_limit_up_ratio",
    ]
    observations = pd.DataFrame(
        columns=observation_order,
        index=pd.MultiIndex.from_arrays([[], []], names=["datetime", "l2_code_id"]),
    )
    model = {
        "schema_version": "fresh_sector_hmm_v1",
        "l2_code_id": 7,
        "observation_order": observation_order,
        "transform_mean": [0.0] * 4,
        "transform_std": [1.0] * 4,
        "transmat": [[0.95, 0.05], [0.05, 0.95]],
        "means": [[-1.0] * 4, [1.0] * 4],
        "covariances": [np.eye(4).tolist(), np.eye(4).tolist()],
        "canonical_state_by_raw": {"0": 0, "1": 1},
        "continuation_cutoff": cutoff.date().isoformat(),
        "continuation_last_posterior": [0.8, 0.2],
        "continuation_state": 0,
        "continuation_state_duration": 4,
        "continuation_last_observation_date": cutoff.date().isoformat(),
    }
    result = continue_sector_hmm(
        static_all=pd.DataFrame(),
        market_daily=pd.DataFrame(),
        benchmark_daily=pd.DataFrame(),
        trading_calendar=dates,
        hmm_bundle={
            "schema_version": "fresh_sector_hmm_bundle_v1",
            "observation_order": observation_order,
            "models": {"7": model},
        },
        continuation_cutoff=cutoff.date().isoformat(),
        required_l2_code_ids=[7],
        precomputed_observations=observations,
    )

    assert result.states.empty
    assert result.unavailable == (
        {
            "l2_code_id": 7,
            "reason_code": "ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            "reason": "continuation_observations_absent",
        },
    )
