from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.fresh_hmm import fit_fresh_sector_hmm


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
