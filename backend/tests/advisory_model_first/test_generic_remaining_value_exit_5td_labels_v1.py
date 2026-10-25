"""Hand cash/clock cases only; no research fit, production or data mutation."""
from datetime import date, timedelta

import pandas as pd
import pytest

from backend.services.advisory_model_first import generic_remaining_value_exit_5td_labels_v1 as m
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_contracts_v1 import QUOTE_FIELDS, ROSTER


@pytest.fixture
def episode():
    days = [date(2025, 1, 2)+timedelta(days=i) for i in range(8)]  # Explicit fixture sessions, not civil-day inference.
    roster = pd.DataFrame([dict(zip(ROSTER, (days[0], days[1], "000001.SZ", 1, 1), strict=True))])
    geometry, receipt = m.exit_geometry_v1(candidates=roster, decision_dates=days[:1], calendar=days, development_cutoff=days[-1])
    prices = pd.DataFrame([dict(trade_date=day, instrument="000001.SZ", raw_open_cny=100.+i,
        raw_close_cny=100.+i, policy_price_per_raw_cny=1., suspended=False, tradability_unknown=False,
        up_limit=200., down_limit=50.) for i, day in enumerate(days)], columns=QUOTE_FIELDS)
    return dict(geometry=geometry, calendar=days, prices=prices, development_cutoff=days[-1]), receipt


def test_fixed_endpoint_t_plus_one_and_remaining_four_to_one(episode):
    inputs, receipt = episode
    geometry = inputs["geometry"]
    assert geometry.remaining_sessions.tolist() == [4, 3, 2, 1]
    assert geometry.s_date.tolist() == inputs["calendar"][1:5] and geometry.u_date.tolist() == inputs["calendar"][2:6]
    assert geometry.endpoint_date.eq(inputs["calendar"][5]).all() and receipt["episodes"] == 1
    labels, _ = m.build_exit_remaining_value_labels_v1(**inputs)
    assert labels.held.eq(True).all() and labels.label_status.eq("AVAILABLE").all()


def test_common_reference_sunk_buy_once_and_date_specific_sell_fees(episode):
    inputs, _ = episode
    days = inputs["calendar"]
    fees = {days[1]: 3., days[2]: 8., days[5]: 20.}
    labels, receipt = m.build_exit_remaining_value_labels_v1(**inputs, fee_schedule=fees)
    row = labels.iloc[0]
    quantity = 1/(101.*(1+m.POLICY["buy_bps"]/10000))
    ref, sell, end = quantity*101.*(1-3/10000), quantity*102.*(1-8/10000), quantity*105.*(1-20/10000)
    assert row.reference_net_cny == pytest.approx(ref) and row.sell_net_cny == pytest.approx(sell)
    assert row.continue_net_cny == pytest.approx(end) and row.advantage_sell_bps == pytest.approx(10000*(sell-end)/ref)
    assert row.y_hold_bps == pytest.approx(10000*(end/ref-1))
    assert not receipt["economic_confirmation"] and receipt["fit_count"] == 0


def test_same_policy_quantity_equivalent_split_uses_common_basis(episode):
    inputs, _ = episode
    prices = inputs["prices"]
    prices.loc[prices.trade_date.ge(inputs["calendar"][2]), ["raw_open_cny", "raw_close_cny"]] /= 2
    prices.loc[prices.trade_date.ge(inputs["calendar"][2]), "policy_price_per_raw_cny"] = 2.
    labels, _ = m.build_exit_remaining_value_labels_v1(**inputs)
    assert labels.iloc[0].y_hold_bps == pytest.approx(10000*(105./101.-1))


def test_missing_s_mark_does_not_hide_known_endpoint_baseline(episode):
    inputs, _ = episode
    inputs["prices"].loc[inputs["prices"].trade_date.eq(inputs["calendar"][1]), "raw_close_cny"] = None
    labels, _ = m.build_exit_remaining_value_labels_v1(**inputs)
    assert labels.baseline_return_bps.notna().all() and pd.isna(labels.iloc[0].y_hold_bps)
    assert labels.iloc[0].baseline_return_bps == labels.iloc[1].baseline_return_bps


@pytest.mark.parametrize("case", ["missing_sell", "missing_limit", "entry_suspended", "endpoint_suspended", "boundary"])
def test_normal_missing_not_held_unsettled_retains_all_decisions(episode, case):
    inputs, _ = episode
    prices, days = inputs["prices"], inputs["calendar"]
    if case == "missing_sell":
        inputs["prices"] = prices.loc[prices.trade_date.ne(days[2])].copy()
    elif case == "missing_limit":
        prices.loc[prices.trade_date.eq(days[1]), "up_limit"] = None
    elif case == "entry_suspended":
        prices.loc[prices.trade_date.eq(days[1]), "suspended"] = True
    elif case == "endpoint_suspended":
        prices.loc[prices.trade_date.eq(days[5]), "suspended"] = True
    else:
        inputs["development_cutoff"] = days[4]
        inputs["geometry"]["geometry_status"] = "UNSETTLED"
        inputs["prices"] = prices.loc[prices.trade_date.le(days[4])].copy()
    labels, _ = m.build_exit_remaining_value_labels_v1(**inputs)
    assert len(labels) == 4
    if case == "entry_suspended":
        assert labels.label_status.eq("NOT_HELD").all() and labels.baseline_return_bps.eq(0).all()
    elif case == "missing_limit":
        assert labels.held.isna().all() and labels.baseline_return_bps.isna().all()
    elif case == "missing_sell":
        assert labels.iloc[0].label_status == "CONTINUE_ONLY" and pd.isna(labels.iloc[0].advantage_sell_bps)
    elif case == "boundary":
        assert labels.label_status.eq("UNSETTLED").all() and labels.y_hold_bps.isna().all()
    else:
        assert labels.hold_executable.eq(False).all() and labels.y_hold_bps.isna().all()


@pytest.mark.parametrize("case", ["same_day_sale", "reset_endpoint", "policy", "future_quote"])
def test_clock_policy_or_test_values_fail_closed(episode, case):
    inputs, _ = episode
    if case == "same_day_sale":
        inputs["geometry"].loc[0, "u_date"] = inputs["geometry"].loc[0, "s_date"]
    elif case == "reset_endpoint":
        inputs["geometry"]["endpoint_date"] = inputs["calendar"][6]
    elif case == "policy":
        inputs["geometry"]["policy_sha256"] = "f"*64
    else:
        inputs["prices"].loc[0, "trade_date"] = date(2030, 1, 1)
        inputs["prices"].loc[0, "raw_open_cny"] = "FORBIDDEN_TEST_PAYLOAD"
    with pytest.raises(ValueError):
        m.build_exit_remaining_value_labels_v1(**inputs)
