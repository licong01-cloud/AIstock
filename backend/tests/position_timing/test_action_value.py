from datetime import date, datetime
from decimal import Decimal as D

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import (
    ActionPlan, ActionValueError, Fill, PositionState, action_candidates, apply_fill,
    choose_action, cutoff_on, daily_fill, leg_fee, market_features,
    normalize_export_bars, quote_fill, require_causal, risk_exit_plan, state_features,
)


def bar(**overrides):
    return {"open": 10, "high": 10.1, "low": 9.9, "close": 10,
            "up_limit": 11, "down_limit": 9, "is_suspended": False, **overrides}


def test_export_raw_conversion_does_not_adjust_limits_twice():
    source = pd.DataFrame({"open": [5.], "high": [5.5], "low": [4.5], "close": [5.],
                           "volume": [2000.], "factor": [.5], "up_limit_price": [11.]})
    result = normalize_export_bars(source)
    assert result.open.iloc[0] == 10
    assert result.volume.iloc[0] == 1000
    assert result.up_limit_price.iloc[0] == 11
    assert source.open.iloc[0] == 5  # no source mutation
    with pytest.raises(ActionValueError, match="FACTOR"):
        normalize_export_bars(source.assign(factor=0))
    assert normalize_export_bars(source, benchmark=True).close.iloc[0] == 5


def test_features_are_trailing_and_scale_invariant():
    dates = pd.bdate_range("2024-01-01", periods=70)
    close = np.linspace(10, 15, len(dates))
    bars = pd.DataFrame({"open": close, "high": close + 1, "low": close - 1,
                         "close": close, "volume": 1000., "factor": .8}, index=dates)
    benchmark = pd.Series(np.linspace(3000, 3400, len(dates)), index=dates)
    full = market_features(bars, benchmark)
    prefix = market_features(bars.iloc[:50], benchmark.iloc[:50])
    pd.testing.assert_frame_equal(full.iloc[:50], prefix)
    pd.testing.assert_frame_equal(full, market_features(bars.assign(factor=.4), benchmark))
    broken = bars.copy()
    broken.loc[dates[45], "close"] = np.nan
    assert np.isnan(market_features(broken, benchmark).loc[dates[50], "realized_vol_20d_bps"])


def test_clock_requires_actual_available_time():
    cutoff = cutoff_on(date(2026, 9, 7))
    assert cutoff.hour == 20
    require_causal(cutoff, cutoff, field="bar")
    for bad in (cutoff.replace(hour=21), datetime(2026, 9, 7, 15)):
        with pytest.raises(ActionValueError, match="NOT_AVAILABLE"):
            require_causal(bad, cutoff, field="event")


def test_candidate_budget_includes_max_price_fee_and_odd_sell_exit():
    cash = PositionState(0, 0, D(10000), D(10000))
    plans = action_candidates("000001.SZ", cash, D(10))
    for plan in plans:
        assert plan.delta % 100 == 0
        assert D("10.1") * plan.delta + leg_fee(plan.symbol, plan.delta, D("10.1")) <= cash.cash
    held = PositionState(251, 251, D(0), D(2510), D(10))
    plans = action_candidates("000001.SZ", held, D(10))
    assert -251 in [p.delta for p in plans]
    assert all(p.delta == -251 or p.delta % 100 == 0 for p in plans)


def test_partial_t1_lock_cannot_sell_odd_tail():
    state = PositionState(251, 151, D(0), D(2510), D(10))
    plans = action_candidates("000001.SZ", state, D(10))
    assert min(p.delta for p in plans) == -100


def test_rule_risk_override_does_not_fabricate_unknown_entry_cost():
    state = PositionState(100, 100, D(0), D(1000), D(10))
    assert risk_exit_plan("000001.SZ", state, D(9)).risk_exit
    unknown = PositionState(100, 100, D(0), D(1000))
    assert risk_exit_plan("000001.SZ", unknown, D(9)) is None
    features = state_features(unknown, ActionPlan("000001.SZ", -100, D(9)))
    assert features["entry_cost_missing"] == 1
    assert np.isnan(features["unrealized_return_bps"])


def test_unique_action_no_trade_and_stable_tie_break():
    plans = tuple(ActionPlan("000001.SZ", d, D(10)) for d in (-200, 0, 100, 200))
    assert choose_action(plans, [-2, 100, -1, 0]).delta == 0
    assert choose_action(plans, [2, 0, 2, 2]).delta == 100
    with pytest.raises(ActionValueError, match="NON_FINITE"):
        choose_action(plans, [np.nan, 0, 1, 2])


def test_daily_guard_rejects_open_gap_even_if_low_later_touches():
    plan = ActionPlan("000001.SZ", 1000, D(10))
    fill = daily_fill(plan, bar(open=10.5, high=10.6, low=9.9, close=10), sellable=0)
    assert fill.status == "NO_FILL"
    assert fill.reason == "SKIP_OPEN_GAP_EXCEEDED"


def test_no_fill_and_unknown_are_different_and_cash_conserved():
    plan = ActionPlan("000001.SZ", 100, D(10))
    state = PositionState(0, 0, D(10000), D(10000))
    nofill = daily_fill(plan, bar(is_suspended=True), sellable=0)
    assert nofill.status == "NO_FILL" and apply_fill(state, nofill) == state
    unknown = daily_fill(plan, None, sellable=0)
    assert unknown.status == "UNKNOWN"
    with pytest.raises(ActionValueError, match="PATH_VALUATION_UNKNOWN"):
        apply_fill(state, unknown)
    fill = daily_fill(plan, bar(), sellable=0)
    changed = apply_fill(state, fill)
    assert changed.quantity == 100 and changed.sellable == 0
    assert changed.cash == D(10000) - D(1000) - D("5.0641")
    assert changed.cash + changed.quantity * D(10) == state.cash - fill.fee


def test_buy_sell_limit_and_t1_are_directional():
    buy = ActionPlan("000001.SZ", 100, D(10))
    sell = ActionPlan("000001.SZ", -100, D(10), True)
    args = {"open_price": D(11), "price": D(11), "up_limit": D(11), "down_limit": D(9), "sellable": 100}
    assert quote_fill(buy, **args).status == "NO_FILL"
    assert quote_fill(sell, **args).status == "FILLED"
    assert quote_fill(sell, **{**args, "sellable": 0}).reason == "T1_SELLABLE_INSUFFICIENT"


def test_component_fee_keeps_minimum_separate_from_regulatory_costs():
    assert leg_fee("000001.SZ", 500, D(10)) == D("5.3205")
    assert leg_fee("000001.SZ", -500, D(10)) == D("7.8205")
    assert leg_fee("000001.SZ", 1000, D(10), parent_count=2) == D("10.641")


def test_yellow_branch_matches_shared_guard_size():
    fill = daily_fill(ActionPlan("000001.SZ", 1000, D(10)), bar(open=10.08, high=10.1, low=10.07, close=10.09), sellable=0)
    assert fill.status == "FILLED"
    assert fill.delta == 500


def test_slippage_rechecks_guard_once_and_preserves_price_bounds():
    plan = ActionPlan("000001.SZ", 1000, D(10))
    filled = daily_fill(plan, bar(open=10.04, high=10.15, low=10, close=10.1), sellable=0, slippage_bps=D(20))
    assert filled.status == "FILLED" and filled.delta == 500
    assert filled.price == D("10.07")
    skipped = daily_fill(plan, bar(open=10.09, high=10.15, low=10.05, close=10.1), sellable=0, slippage_bps=D(20))
    assert skipped.status == "NO_FILL" and skipped.reason == "SKIP_ABOVE_MAX_BUY_PRICE"
    out_of_range = daily_fill(plan, bar(open=10, high=10, low=10, close=10), sellable=0, slippage_bps=D(1))
    assert out_of_range.reason == "SLIPPAGE_EXCEEDS_OBSERVED_RANGE"
    with pytest.raises(ActionValueError, match="SLIPPAGE_SCENARIO_INVALID"):
        daily_fill(plan, bar(), sellable=0, slippage_bps=D(-1))


def test_large_star_budget_uses_legal_affordable_quantity():
    state = PositionState(0, 0, D(10000000), D(10000000))
    plans = action_candidates("688001.SH", state, D(1))
    biggest = max(plan.delta for plan in plans)
    assert D("1.01") * biggest + leg_fee("688001.SH", biggest, D("1.01")) <= state.cash
    assert D("1.01") * (biggest+1) + leg_fee("688001.SH", biggest+1, D("1.01")) > state.cash


def test_invalid_fractional_quantities_negative_fee_and_stale_cost_state_rejected():
    with pytest.raises(ActionValueError, match="QUANTITY_INVALID"):
        PositionState(100.5, 100, D(0), D(1000))
    with pytest.raises(ActionValueError, match="ACTION_QUANTITY"):
        ActionPlan("000001.SZ", True, D(10))
    with pytest.raises(ActionValueError, match="FILL_FEE"):
        Fill("FILLED", 100, D(10), D(-1))
    with pytest.raises(ActionValueError, match="NON_FILL"):
        Fill("NO_FILL", 100, D(10))
    state = PositionState(100, 100, D(0), D(1000), D(10), 30)
    fill = quote_fill(ActionPlan("000001.SZ", -100, D(10), True), open_price=D(10), price=D(10),
                      up_limit=D(11), down_limit=D(9), sellable=100, full_exit=True)
    exited = apply_fill(state, fill)
    assert exited.quantity == 0 and exited.holding_age is None and exited.entry_cost is None
