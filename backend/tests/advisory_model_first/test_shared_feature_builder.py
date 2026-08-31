from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.shared_feature_builder import build_advisory_feature_matrix
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID


def _feature_inputs() -> dict[str, object]:
    dates = pd.bdate_range("2024-03-01", periods=90)
    symbols = ["000001.SZ", "000002.SZ"]
    candidate_index = pd.MultiIndex.from_product([dates, symbols], names=["datetime", "instrument"])
    daily = pd.DataFrame(index=candidate_index)
    day_number = np.tile(np.arange(len(dates), dtype=float), len(symbols)).reshape(len(symbols), -1).T.ravel()
    daily["close"] = 10.0 + day_number * 0.01
    daily["open"] = daily["close"] - 0.01
    daily["high"] = daily["close"] + 0.1
    daily["low"] = daily["close"] - 0.1
    daily["volume"] = 1000.0 + day_number
    daily["amount"] = daily["volume"] * daily["close"]
    daily["factor"] = 1.0
    daily["up_limit_price"] = daily["close"] * 1.1
    daily["down_limit_price"] = daily["close"] * 0.9
    daily["prev_close"] = daily.groupby(level="instrument")["close"].shift(1)
    daily["limit_up"] = 0.0
    daily["limit_down"] = 0.0

    static = pd.DataFrame(index=candidate_index)
    static["db_turnover_rate"] = 1.0
    static["db_volume_ratio"] = 1.0
    static["db_pe_ttm"] = 10.0
    static["db_pb"] = 1.5
    static["db_circ_mv"] = 1000.0
    for column in ("mf_lg_buy_amt", "mf_elg_buy_amt"):
        static[column] = 60.0
    for column in ("mf_lg_sell_amt", "mf_elg_sell_amt"):
        static[column] = 40.0
    static["bb_rev_yoy"] = 5.0
    static["bb_profit_yoy"] = 4.0
    static["bb_gpr"] = 20.0
    static["bb_npr"] = 10.0
    static["cp_cost_5pct"] = 8.0
    static["cp_cost_50pct"] = 10.0
    static["cp_cost_95pct"] = 12.0
    static["cp_winner_rate"] = 0.5
    static["md_rzye"] = 100.0 + day_number
    static["l2_code_id"] = np.tile([1, 2], len(dates))
    static["sw2_close"] = 100.0 + day_number * 0.02
    static["sw2_amount"] = 1_000_000.0 + day_number
    static["sw2_mf_net_amt"] = 1000.0

    market_symbols = [f"{index:06d}.SZ" for index in range(100, 205)]
    market_index = pd.MultiIndex.from_product([dates, market_symbols], names=["datetime", "instrument"])
    market = pd.DataFrame(index=market_index)
    market["close"] = (
        np.tile(np.linspace(10.0, 11.0, len(dates)), len(market_symbols)).reshape(len(market_symbols), -1).T.ravel()
    )
    market["limit_up"] = 0.0
    benchmark = pd.DataFrame(
        {"open": np.linspace(100.0, 110.0, len(dates)), "close": np.linspace(100.0, 111.0, len(dates))},
        index=pd.MultiIndex.from_product([dates, ["000300.SH"]], names=["datetime", "instrument"]),
    )
    decision = dates[-1]
    candidates = pd.DataFrame(
        {
            "trade_date": [decision, decision],
            "decision_as_of_trade_date": [decision, decision],
            "target_trade_date": [decision + pd.offsets.BDay(), decision + pd.offsets.BDay()],
            "instrument": symbols,
            "program_id": "program",
            "binding_version_id": "binding",
            "package_id": "package",
            "manifest_sha256": "a" * 64,
            "selection_runtime_semantics_hash": "b" * 64,
            "selection_source_rank": [1, 2],
            "selection_effective_rank": [1, 2],
            "candidate_group_size": [2, 2],
            "combined_score": [1.0, 0.5],
            f"raw__{LSTM_LEG_ID}": [1.0, 0.0],
            f"norm__{LSTM_LEG_ID}": [1.0, -1.0],
            f"rank__{LSTM_LEG_ID}": [1, 2],
            f"weight__{LSTM_LEG_ID}": [0.7, 0.7],
            f"raw__{FUND_LEG_ID}": [0.5, 0.1],
            f"norm__{FUND_LEG_ID}": [1.0, -1.0],
            f"rank__{FUND_LEG_ID}": [1, 2],
            f"weight__{FUND_LEG_ID}": [0.3, 0.3],
        }
    )
    return {
        "candidates": candidates,
        "candidate_daily": daily,
        "candidate_static": static,
        "market_daily": market,
        "benchmark_daily": benchmark,
        "suspend_rows": pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"]),
        "hmm_states": pd.DataFrame(),
    }


def test_feature_builder_produces_fixed_schema_and_explicit_optional_missing() -> None:
    result = build_advisory_feature_matrix(**_feature_inputs())
    assert len(result.features) == 2
    assert result.coverage["status"].tolist() == ["available"]
    assert result.features["hmm_bull_posterior__missing"].tolist() == [1, 1]
    assert result.features["leg_direction_agreement"].tolist() == [1, 1]


@pytest.mark.parametrize("suspend_count", [1, 10, 59])
def test_feature_builder_v2_preserves_candidate_after_historical_suspension(
    suspend_count: int,
) -> None:
    inputs = _feature_inputs()
    daily = inputs["candidate_daily"].copy()
    all_dates = daily.index.get_level_values("datetime").unique()
    suspended_dates = all_dates[-(suspend_count + 1) : -1]
    suspended_keys = [(date, "000001.SZ") for date in suspended_dates]
    daily = daily.drop(index=suspended_keys)
    inputs["candidate_daily"] = daily
    inputs["suspend_rows"] = pd.DataFrame(
        {
            "trade_date": suspended_dates,
            "instrument": ["000001.SZ"] * suspend_count,
            "suspend_type": ["S"] * suspend_count,
        }
    )
    inputs["feature_schema_version"] = "advisory_feature_schema_v2_suspension_aware"
    inputs["trading_calendar"] = daily.index.get_level_values("datetime").unique()
    inputs["incomplete_candidate_policy"] = "preserve_exact"

    result = build_advisory_feature_matrix(**inputs)

    assert len(result.features) == 2
    assert result.coverage["required_missing_row_count"].tolist() == [0]
    recovered = result.features.loc[result.features["instrument"] == "000001.SZ"].iloc[0]
    assert recovered["suspend_session_count_60"] == float(suspend_count)
    assert recovered["current_bar_synthetic"] == 0


def test_feature_builder_v2_keeps_current_suspension_but_nulls_execution_quote() -> None:
    inputs = _feature_inputs()
    daily = inputs["candidate_daily"].copy()
    decision = daily.index.get_level_values("datetime").unique()[-1]
    suspended_key = (decision, "000001.SZ")
    daily.loc[suspended_key, ["volume", "amount"]] = 0.0
    inputs["candidate_daily"] = daily
    inputs["suspend_rows"] = pd.DataFrame(
        {
            "trade_date": [decision],
            "instrument": ["000001.SZ"],
            "suspend_type": ["S"],
        }
    )
    inputs["feature_schema_version"] = "advisory_feature_schema_v2_suspension_aware"
    inputs["trading_calendar"] = daily.index.get_level_values("datetime").unique()
    inputs["incomplete_candidate_policy"] = "preserve_exact"

    result = build_advisory_feature_matrix(**inputs)

    assert len(result.features) == 2
    suspended = result.features.loc[result.features["instrument"] == "000001.SZ"].iloc[0]
    assert suspended["current_bar_synthetic"] == 1
    assert suspended["decision_is_suspended"] == 1
    assert pd.isna(suspended["decision_limit_up"])
    assert suspended["decision_limit_up__missing"] == 1
    assert pd.isna(suspended["distance_to_limit_up"])
    assert suspended["distance_to_limit_up__missing"] == 1
