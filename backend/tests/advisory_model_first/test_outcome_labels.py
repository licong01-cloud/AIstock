from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.labels import CLOSE_COST, OPEN_COST
from backend.services.advisory_model_first.outcome_contracts import OUTCOME_HORIZONS
from backend.services.advisory_model_first.outcome_labels import (
    apply_outcome_split,
    build_multi_horizon_outcome_labels,
)
from backend.services.advisory_model_first.outcome_split import fixed_406_outcome_split


def _inputs(periods: int = 40):
    calendar = pd.bdate_range("2024-01-01", periods=periods)
    symbols = ["000001.SZ", "000002.SZ"]
    index = pd.MultiIndex.from_product([calendar, symbols], names=["datetime", "instrument"])
    daily = pd.DataFrame(index=index)
    daily["open"] = 10.0
    daily["close"] = 10.0 + daily.groupby(level="instrument").cumcount() * 0.05
    daily["high"] = daily["close"] + 0.2
    daily["low"] = daily["close"] - 0.2
    daily["factor"] = 1.0
    daily["limit_up"] = 0.0
    daily["limit_down"] = 0.0
    daily["up_limit_price"] = daily["close"] * 1.1
    daily["down_limit_price"] = daily["close"] * 0.9
    benchmark = pd.DataFrame(
        {"open": 100.0, "close": 100.0},
        index=pd.MultiIndex.from_product([calendar, ["000300.SH"]], names=["datetime", "instrument"]),
    )
    candidates = pd.DataFrame(
        {
            "decision_as_of_trade_date": [calendar[0], calendar[0]],
            "target_trade_date": [calendar[1], calendar[1]],
            "instrument": symbols,
        }
    )
    suspend = pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"])
    return calendar, daily, benchmark, candidates, suspend


def test_multi_horizon_labels_share_executable_entry_and_exact_costs() -> None:
    calendar, daily, benchmark, candidates, suspend = _inputs()
    result = build_multi_horizon_outcome_labels(
        candidates=candidates,
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        trading_calendar=calendar,
    )

    first = result.labels.iloc[0]
    for horizon in OUTCOME_HORIZONS:
        assert first[f"label_status_{horizon}"] == "MATURE_EXECUTABLE"
        expected_exit = calendar[horizon]
        assert first[f"actual_exit_date_{horizon}"] == expected_exit
        expected_return = (
            daily.loc[(expected_exit, "000001.SZ"), "close"]
            * (1 - CLOSE_COST)
            / (10.0 * (1 + OPEN_COST))
            - 1
        )
        assert first[f"stock_net_return_{horizon}"] == pytest.approx(expected_return)
        assert first[f"actual_holding_trading_days_{horizon}"] == horizon
    assert first["holding_label_status"] == "AVAILABLE"
    assert first["optimal_holding_bucket"] in OUTCOME_HORIZONS


def test_outcome_label_delays_limit_down_exit_and_uses_same_benchmark_date() -> None:
    calendar, daily, benchmark, candidates, suspend = _inputs()
    nominal = calendar[5]
    daily.loc[(nominal, "000001.SZ"), ["high", "down_limit_price", "limit_down"]] = [8.0, 8.0, 1.0]
    benchmark.loc[(calendar[6], "000300.SH"), "close"] = 102.0

    result = build_multi_horizon_outcome_labels(
        candidates=candidates.iloc[[0]],
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        trading_calendar=calendar,
    )

    row = result.labels.iloc[0]
    assert row["actual_exit_date_5"] == calendar[6]
    assert row["benchmark_return_5"] == pytest.approx(0.02)


def test_one_price_limit_up_entry_marks_every_horizon_unavailable() -> None:
    calendar, daily, benchmark, candidates, suspend = _inputs()
    target = calendar[1]
    daily.loc[(target, "000001.SZ"), ["low", "up_limit_price", "limit_up"]] = [11.0, 11.0, 1.0]

    result = build_multi_horizon_outcome_labels(
        candidates=candidates.iloc[[0]],
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        trading_calendar=calendar,
    )

    row = result.labels.iloc[0]
    assert all(row[f"label_status_{horizon}"] == "NO_EXECUTABLE_ENTRY" for horizon in OUTCOME_HORIZONS)
    assert row["holding_label_status"] == "UNAVAILABLE"


def test_split_modelability_rejects_actual_exit_beyond_boundary() -> None:
    dates = pd.bdate_range("2024-01-01", periods=406)
    split = fixed_406_outcome_split(dates)
    labels = pd.DataFrame(
        {
            "decision_as_of_trade_date": [split.train[-1]],
            "holding_label_status": ["AVAILABLE"],
            **{f"label_status_{horizon}": ["MATURE_EXECUTABLE"] for horizon in OUTCOME_HORIZONS},
            **{f"actual_exit_date_{horizon}": [split.purge_1[-1]] for horizon in OUTCOME_HORIZONS},
        }
    )
    labels["actual_exit_date_20"] = split.validation[0]

    result = apply_outcome_split(labels, split, data_cutoff="2026-06-30")

    assert bool(result.loc[0, "modelable_10"]) is True
    assert bool(result.loc[0, "modelable_20"]) is False
    assert bool(result.loc[0, "holding_modelable"]) is False
