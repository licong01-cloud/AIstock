"""One reused input fixture; pure daily contracts, not business return evidence."""
from copy import deepcopy
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES, build_economic_daily_feature_core_v1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.shared_feature_builder import _build_market_features, build_advisory_feature_matrix
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID
from backend.tests.advisory_model_first.test_shared_feature_builder import _feature_inputs


@pytest.fixture
def packet():
    inputs = _feature_inputs()
    sessions = inputs["candidate_daily"].index.get_level_values("datetime").unique()[-20:]
    roles = {"lstm": LSTM_LEG_ID, "fund": FUND_LEG_ID}
    weights = {LSTM_LEG_ID: .7, FUND_LEG_ID: .3}
    columns = ["decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank",
               "candidate_group_size", "combined_score", f"norm__{LSTM_LEG_ID}", f"norm__{FUND_LEG_ID}"]
    candidates = inputs["candidates"].loc[:, columns].copy()
    candidates["combined_score"] = sum(candidates[f"norm__{leg}"] * weight for leg, weight in weights.items())
    daily = inputs["candidate_daily"].loc[inputs["candidate_daily"].index.get_level_values("datetime").isin(sessions)].reset_index()
    raw = daily.rename(columns={"datetime": "trade_date"})[["trade_date", "instrument"]].copy()
    for field in ("open", "high", "low", "close"):
        raw[field + "_li"] = daily[field] * 1000
    raw["volume_hand"] = daily.volume / 100
    raw["amount_li"] = daily.amount * 1000
    raw["adj_factor"] = 1.
    raw["pre_close"] = daily.prev_close
    raw["up_limit"] = daily.up_limit_price
    raw["down_limit"] = daily.down_limit_price
    market = inputs["market_daily"].reset_index().rename(columns={"datetime": "trade_date"})
    market = market.loc[market.trade_date.isin(sessions[-2:]), ["trade_date", "instrument", "close"]].copy()
    benchmark = inputs["benchmark_daily"].reset_index().rename(columns={"datetime": "trade_date"})
    benchmark = benchmark.loc[benchmark.trade_date.isin(sessions), ["trade_date", "instrument", "close"]].copy()
    return dict(candidates=candidates, raw_daily=raw, market_daily=market, benchmark_daily=benchmark,
        suspend_rows=inputs["suspend_rows"].copy(), calendar=tuple(sessions.date) + (candidates.target_trade_date.iloc[0].date(),),
        component_roles=roles, terminal_weights=weights)


def test_complete_D_values_reuse_shared_formulas_and_hand_calculated_information(packet):
    frame, receipt = build_economic_daily_feature_core_v1(**packet)
    assert frame.instrument.tolist() == packet["candidates"].instrument.tolist()
    assert list(frame.columns[3:]) == list(D_FEATURES) and "query_gap_bps" not in frame
    inputs = _feature_inputs()
    inputs["candidates"]["combined_score"] = packet["candidates"].combined_score
    shared = build_advisory_feature_matrix(**inputs, incomplete_candidate_policy="preserve_exact",
        feature_schema_version="advisory_feature_schema_v2_suspension_aware",
        trading_calendar=inputs["candidate_daily"].index.get_level_values("datetime").unique()).features
    for name in D_FEATURES[:8]:
        np.testing.assert_allclose(frame[name].to_numpy(dtype=float), shared[name].to_numpy(dtype=float))
    raw = packet["raw_daily"].loc[packet["raw_daily"].instrument.eq("000001.SZ")]
    assert frame.ret_10.iloc[0] == pytest.approx(raw.close_li.iloc[-1]/raw.close_li.iloc[-11]-1)
    assert frame.close_location_in_day.iloc[0] == pytest.approx(.5)
    assert frame.volume_ratio_5_to_20.iloc[0] == pytest.approx(raw.volume_hand.iloc[-5:].mean()/raw.volume_hand.mean())
    assert not receipt["deployable"] and not receipt["outcomes_read"] and not receipt["new_native_receipt"]
    assert receipt["old_training_parity"] == "UNPROVEN"
    assert all(not row["fields"] for row in receipt["unknown_fields"])


def test_missing_market_previous_session_never_uses_earlier_stale_quote(packet):
    sessions = pd.DatetimeIndex(packet["calendar"][:-1])
    rows = [(day, f"{i:06d}.SZ", 100.+j) for j,day in enumerate(sessions[-3:]) for i in range(100)]
    rows += [(sessions[-3], "999999.SZ", 100.), (sessions[-1], "999999.SZ", 90.)]
    long = pd.DataFrame(rows, columns=["trade_date", "instrument", "close"])
    long_panel = long.set_index(["trade_date", "instrument"]).rename_axis(["datetime", "instrument"]).assign(limit_up=0.)
    assert _build_market_features(long_panel).market_up_ratio.iloc[-1] == pytest.approx(100/101)
    with pytest.raises(AdvisoryModelFirstError, match="foreign or non-session"):
        build_economic_daily_feature_core_v1(**{**packet, "market_daily": long})
    day_pack = {**packet, "market_daily": long.loc[long.trade_date.isin(sessions[-2:])]}
    daily, receipt = build_economic_daily_feature_core_v1(**day_pack)
    batch_day, repeated = build_economic_daily_feature_core_v1(**deepcopy(day_pack))
    pd.testing.assert_frame_equal(daily, batch_day)
    assert daily.market_up_ratio.tolist() == [1., 1.] and receipt == repeated


@pytest.mark.parametrize("field", ["raw_daily", "market_daily", "benchmark_daily", "suspend_rows"])
def test_future_source_rows_fail_closed_before_feature_calculation(packet, field):
    value = packet[field].copy()
    if value.empty:
        value = pd.DataFrame({"trade_date": [pd.Timestamp(packet["calendar"][-1])], "instrument": ["000001.SZ"], "suspend_type": ["S"]})
    value.loc[value.index[0], "trade_date"] = pd.Timestamp(packet["calendar"][-1])
    with pytest.raises(AdvisoryModelFirstError, match="foreign or non-session"):
        build_economic_daily_feature_core_v1(**{**packet, field: value})


@pytest.mark.parametrize("mode", ["filtered", "duplicate", "wrong_score", "bool_weight", "unknown_leg"])
def test_original_roster_scores_and_weights_cannot_be_silently_changed(packet, mode):
    changed = deepcopy(packet)
    if mode == "filtered":
        changed["candidates"] = changed["candidates"].iloc[:1]
    elif mode == "duplicate":
        changed["candidates"].loc[1, "instrument"] = changed["candidates"].instrument.iloc[0]
    elif mode == "wrong_score":
        changed["candidates"].loc[0, "combined_score"] = .2
    elif mode == "bool_weight":
        changed["terminal_weights"][LSTM_LEG_ID] = True
    else:
        changed["component_roles"]["fund"] = "foreign"
    with pytest.raises(AdvisoryModelFirstError):
        build_economic_daily_feature_core_v1(**changed)


def test_normal_missing_and_unknown_benchmark_keep_original_candidates(packet):
    changed = deepcopy(packet)
    changed["raw_daily"] = changed["raw_daily"].loc[~changed["raw_daily"].instrument.eq("000001.SZ")]
    changed["benchmark_daily"] = changed["benchmark_daily"].iloc[:-1]
    frame, receipt = build_economic_daily_feature_core_v1(**changed)
    assert frame.instrument.tolist() == ["000001.SZ", "000002.SZ"]
    assert frame.loc[0, ["ret_1", "ret_5", "atr14_close", "ret_10", "volume_ratio_5_to_20"]].isna().all()
    assert frame.csi300_ret_5.isna().all() and frame.relative_ret_5_vs_csi300.isna().all()
    assert frame.parent_combined_score.tolist() == [1., -1.]
    assert "ret_1" in receipt["unknown_fields"][0]["fields"]


def test_interior_benchmark_gap_keeps_relative_return_unknown(packet):
    changed = deepcopy(packet)
    missing = pd.Timestamp(packet["calendar"][-4])
    changed["benchmark_daily"] = changed["benchmark_daily"].loc[~changed["benchmark_daily"].trade_date.eq(missing)]
    frame, _ = build_economic_daily_feature_core_v1(**changed)
    assert frame.csi300_ret_5.isna().all() and frame.relative_ret_5_vs_csi300.isna().all()
    assert frame.ret_5.notna().all() and len(frame) == len(packet["candidates"])


def test_contradictory_open_is_not_a_normal_unknown(packet):
    changed = deepcopy(packet)
    changed["raw_daily"].loc[0, "open_li"] = changed["raw_daily"].high_li.iloc[0] * 2
    with pytest.raises(AdvisoryModelFirstError, match="OHLC"):
        build_economic_daily_feature_core_v1(**changed)


def test_suspension_is_normalized_only_for_old_bar_features_not_raw_information(packet):
    changed = deepcopy(packet)
    D = pd.Timestamp(packet["calendar"][-2])
    changed["raw_daily"] = changed["raw_daily"].loc[~(changed["raw_daily"].trade_date.eq(D) & changed["raw_daily"].instrument.eq("000001.SZ"))]
    changed["suspend_rows"] = pd.DataFrame({"trade_date": [D], "instrument": ["000001.SZ"], "suspend_type": ["S"]})
    frame, receipt = build_economic_daily_feature_core_v1(**changed)
    assert frame.ret_1.iloc[0] == 0. and frame.atr14_close.notna().all()
    assert pd.isna(frame.ret_10.iloc[0]) and pd.isna(frame.volume_ratio_5_to_20.iloc[0])
    assert len(frame) == 2 and receipt["candidate_count"] == 2


def test_unknown_adjustment_and_conflicting_suspend_have_different_outcomes(packet):
    changed = deepcopy(packet)
    D = pd.Timestamp(packet["calendar"][-2])
    changed["raw_daily"].loc[changed["raw_daily"].trade_date.eq(D), "adj_factor"] = np.nan
    frame, _ = build_economic_daily_feature_core_v1(**changed)
    assert frame.ret_10.isna().all() and len(frame) == len(packet["candidates"])
    changed = {**packet, "suspend_rows": pd.DataFrame({"trade_date": [D], "instrument": ["000001.SZ"], "suspend_type": ["S"]})}
    with pytest.raises(AdvisoryModelFirstError):
        build_economic_daily_feature_core_v1(**changed)


@pytest.mark.parametrize("partial", ["same_day_resume", "intraday_timing", "resume_after_full_suspend"])
def test_partial_suspension_keeps_observed_bars_but_never_invents_missing_bars(packet, partial):
    changed = deepcopy(packet)
    D = pd.Timestamp(packet["calendar"][-2])
    types = ["S", "R"] if partial == "same_day_resume" else ["S"]
    changed["suspend_rows"] = pd.DataFrame({"trade_date": [D] * len(types), "instrument": ["000001.SZ"] * len(types),
        "suspend_type": types, "suspend_timing": ["09:30-10:00"] * len(types)})
    if partial == "resume_after_full_suspend":
        previous = pd.Timestamp(packet["calendar"][-3])
        changed["suspend_rows"] = pd.DataFrame({"trade_date": [previous, D], "instrument": ["000001.SZ"] * 2,
            "suspend_type": ["S", "R"], "suspend_timing": ["", ""]})
        changed["raw_daily"] = changed["raw_daily"].loc[~(changed["raw_daily"].trade_date.eq(previous) & changed["raw_daily"].instrument.eq("000001.SZ"))]
    observed, _ = build_economic_daily_feature_core_v1(**changed)
    assert observed.ret_1.notna().all() and len(observed) == len(packet["candidates"])
    changed["raw_daily"] = changed["raw_daily"].loc[~(changed["raw_daily"].trade_date.eq(D) & changed["raw_daily"].instrument.eq("000001.SZ"))]
    missing, _ = build_economic_daily_feature_core_v1(**changed)
    assert pd.isna(missing.ret_1.iloc[0]) and len(missing) == len(packet["candidates"])


def test_split_adjusted_prices_do_not_adjust_raw_volume(packet):
    changed = deepcopy(packet)
    raw = changed["raw_daily"]
    split = raw.trade_date.ge(pd.Timestamp(packet["calendar"][-11]))
    for column in ("open_li", "high_li", "low_li", "close_li"):
        raw.loc[split, column] /= 2
    raw.loc[split, "adj_factor"] = 2.
    frame, _ = build_economic_daily_feature_core_v1(**changed)
    original, _ = build_economic_daily_feature_core_v1(**packet)
    np.testing.assert_allclose(frame.ret_10, original.ret_10)
    np.testing.assert_allclose(frame.volume_ratio_5_to_20, original.volume_ratio_5_to_20)


def test_row_column_order_decimal_and_original_inputs_are_stable(packet):
    before = deepcopy(packet)
    frame, receipt = build_economic_daily_feature_core_v1(**packet)
    changed = deepcopy(packet)
    changed["raw_daily"] = changed["raw_daily"].iloc[::-1, ::-1].copy()
    changed["raw_daily"]["close_li"] = changed["raw_daily"].close_li.map(lambda value: Decimal(str(value)))
    repeated, again = build_economic_daily_feature_core_v1(**changed)
    pd.testing.assert_frame_equal(frame, repeated)
    assert receipt == again
    for key in ("candidates", "raw_daily", "market_daily", "benchmark_daily", "suspend_rows"):
        pd.testing.assert_frame_equal(packet[key], before[key])


@pytest.mark.parametrize("mode", ["infinite", "string", "duplicate_key", "label_column", "budget"])
def test_malformed_numeric_schema_keys_and_budgets_are_not_unknown_success(packet, mode):
    changed = deepcopy(packet)
    raw = changed["raw_daily"]
    if mode in ("infinite", "string"):
        raw["close_li"] = raw.close_li.astype(object)
        raw.loc[raw.index[0], "close_li"] = float("inf") if mode == "infinite" else "100"
    elif mode == "duplicate_key":
        changed["raw_daily"] = pd.concat([raw, raw.iloc[:1]])
    elif mode == "label_column":
        raw["future_return"] = 100
    else:
        changed["raw_daily"] = pd.concat([raw] * 11, ignore_index=True)
    with pytest.raises(AdvisoryModelFirstError):
        build_economic_daily_feature_core_v1(**changed)


def test_empty_original_roster_is_complete_without_source_or_model_success(packet):
    empty = {**packet}
    for key in ("candidates", "raw_daily", "market_daily", "benchmark_daily", "suspend_rows"):
        empty[key] = empty[key].iloc[:0]
    frame, receipt = build_economic_daily_feature_core_v1(**empty)
    assert frame.empty and list(frame.columns[3:]) == list(D_FEATURES)
    assert receipt["status"] == "NO_CANDIDATES" and receipt["source_evidence"] == "COMPUTATION_ONLY"
    assert not receipt["deployable"]
