"""Hand held-state prices, original-session missingness and S-only poisoning."""
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import generic_exit_held_path_5td_features_v1 as m
from backend.services.advisory_model_first.generic_exit_held_path_5td_contracts_v1 import NEW_FEATURES, PRICE_FIELDS


@pytest.fixture
def held():
    days = [date(2025, 1, 1)+timedelta(days=i) for i in range(6)]
    rows = pd.DataFrame([dict(episode_id="original", s_date=days[i], entry_date=days[0], instrument="000001.SZ",
        remaining_sessions=4-i) for i in range(4)])
    prices = pd.DataFrame([dict(trade_date=day, instrument="000001.SZ", raw_open_cny=100., raw_high_cny=110.,
        raw_low_cny=90., raw_close_cny=105., adj_factor=1.) for day in days], columns=PRICE_FIELDS)
    return dict(rows=rows, prices=prices, calendar=days, development_cutoff=days[-1])


def test_t_mark_hand_units_and_no_actual_t_sale(held):
    frame, receipt = m.build_exit_held_path_features_v1(**held)
    row = frame.iloc[0]
    assert row.held_mark_return_bps == pytest.approx(10000*(105*(1-m.POLICY["sell_bps"]/10000)/(100*(1+m.POLICY["buy_bps"]/10000))-1))
    assert row.held_peak_drawdown_fraction == pytest.approx(105/110-1)
    assert row.held_range_fraction == pytest.approx(110/90-1)
    assert len(frame) == 4 and receipt["physical_fits"] == 0 and not receipt["future_features_read"]


def test_split_normalization_and_later_poison_do_not_change_s_features(held):
    before, _ = m.build_exit_held_path_features_v1(**held)
    fields = ["raw_open_cny", "raw_high_cny", "raw_low_cny", "raw_close_cny"]
    later = held["prices"].trade_date.gt(held["calendar"][0])
    held["prices"].loc[later, fields] /= 2
    held["prices"].loc[later, "adj_factor"] = 2
    split, _ = m.build_exit_held_path_features_v1(**held)
    pd.testing.assert_frame_equal(before, split)
    held["prices"].loc[held["prices"].trade_date.gt(held["calendar"][3]), [*fields, "adj_factor"]] = np.inf
    poisoned, _ = m.build_exit_held_path_features_v1(**held)
    pd.testing.assert_frame_equal(before, poisoned)


@pytest.mark.parametrize("missing", ["middle", "all", "factor", "boundary"])
def test_missing_original_sessions_unknown_without_shortening_or_deleting(held, missing):
    if missing == "middle":
        held["prices"] = held["prices"].loc[held["prices"].trade_date.ne(held["calendar"][1])]
    elif missing == "all":
        held["prices"].loc[:, PRICE_FIELDS[2:]] = np.nan
    elif missing == "factor":
        held["prices"].loc[held["prices"].trade_date.eq(held["calendar"][1]), "adj_factor"] = np.nan
    else:
        held["development_cutoff"] = held["calendar"][1]
        held["prices"] = held["prices"].loc[held["prices"].trade_date.le(held["calendar"][1])]
    frame, receipt = m.build_exit_held_path_features_v1(**held)
    assert len(frame) == 4 and frame.episode_id.eq("original").all()
    if missing in {"middle", "factor"}:
        assert pd.isna(frame.iloc[2].held_peak_drawdown_fraction) and pd.notna(frame.iloc[2].held_mark_return_bps)
    elif missing == "all":
        assert frame.loc[:, NEW_FEATURES].isna().all().all()
    else:
        assert frame.loc[2:, NEW_FEATURES].isna().all().all() and receipt["unknown"][2]["reason"] == "S_AFTER_DEVELOPMENT_CUTOFF"


@pytest.mark.parametrize("invalid", ["future_cutoff", "duplicate", "clock"])
def test_input_contradictions_not_silently_repaired(held, invalid):
    if invalid == "future_cutoff":
        held["development_cutoff"] = held["calendar"][3]
    elif invalid == "duplicate":
        held["prices"] = pd.concat([held["prices"], held["prices"].iloc[:1]], ignore_index=True)
    else:
        held["rows"].loc[0, "remaining_sessions"] = 1
    with pytest.raises(ValueError):
        m.build_exit_held_path_features_v1(**held)
