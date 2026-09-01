from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.labels import CLOSE_COST, OPEN_COST, build_five_day_labels


def test_label_uses_executable_prices_costs_and_same_actual_exit_benchmark() -> None:
    calendar = pd.bdate_range("2024-07-01", periods=12)
    symbols = [f"{index:06d}.SZ" for index in range(1, 6)]
    index = pd.MultiIndex.from_product([calendar, symbols], names=["datetime", "instrument"])
    daily = pd.DataFrame(index=index)
    daily["open"] = 10.0
    daily["close"] = 11.0
    daily["high"] = 12.0
    daily["low"] = 9.0
    daily["factor"] = 1.0
    daily["limit_up"] = 0.0
    daily["limit_down"] = 0.0
    daily["up_limit_price"] = 11.0
    daily["down_limit_price"] = 9.0
    benchmark = pd.DataFrame(
        {"open": 100.0, "close": 105.0},
        index=pd.MultiIndex.from_product([calendar, ["000300.SH"]], names=["datetime", "instrument"]),
    )
    candidates = pd.DataFrame(
        {
            "decision_as_of_trade_date": calendar[0],
            "target_trade_date": calendar[1],
            "instrument": symbols,
        }
    )
    result = build_five_day_labels(
        candidates=candidates,
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"]),
        trading_calendar=calendar,
    )
    expected = 11.0 * (1 - CLOSE_COST) / (10.0 * (1 + OPEN_COST)) - 1
    assert result.labels["stock_net_return_5"].iloc[0] == pytest.approx(expected)
    assert result.labels["actual_exit_date"].nunique() == 1
    assert result.labels["group_label_status"].eq("NO_LABEL_VARIATION").all()


def test_one_price_limit_up_entry_is_not_silently_labeled() -> None:
    calendar = pd.bdate_range("2024-07-01", periods=12)
    index = pd.MultiIndex.from_product([calendar, ["000001.SZ"]], names=["datetime", "instrument"])
    daily = pd.DataFrame(
        {
            "open": 10.0,
            "close": 10.0,
            "high": 10.0,
            "low": 10.0,
            "factor": 1.0,
            "limit_up": 0.0,
            "limit_down": 0.0,
            "up_limit_price": 10.0,
            "down_limit_price": 9.0,
        },
        index=index,
    )
    daily.loc[(calendar[1], "000001.SZ"), "limit_up"] = 1.0
    benchmark = pd.DataFrame(
        {"open": 100.0, "close": 100.0},
        index=pd.MultiIndex.from_product([calendar, ["000300.SH"]], names=["datetime", "instrument"]),
    )
    result = build_five_day_labels(
        candidates=pd.DataFrame(
            {
                "decision_as_of_trade_date": [calendar[0]],
                "target_trade_date": [calendar[1]],
                "instrument": ["000001.SZ"],
            }
        ),
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"]),
        trading_calendar=calendar,
    )
    assert result.labels.loc[0, "label_status"] == "NO_EXECUTABLE_ENTRY"
    assert pd.isna(result.labels.loc[0, "utility_5"])
