"""Fixed arithmetic, real-bar and D-clock contracts, not profit evidence."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_timing_features_v1 import (
    ROSTER_FIELDS, TIMING_FEATURES, build_economic_entry_timing_features_v1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_daily_feature_core_v1 import packet as core_packet


@pytest.fixture
def packet():
    original = core_packet.__wrapped__()
    return {"candidates": original["candidates"].loc[:, ROSTER_FIELDS].copy(),
        "raw_daily": original["raw_daily"].copy(), "suspend_rows": original["suspend_rows"].copy(),
        "calendar": original["calendar"]}


def test_same_closes_different_overnight_intraday_composition_and_fixed_19_pairs(packet):
    bars = packet["raw_daily"]
    bars["high_li"], bars["low_li"], bars["close_li"] = 11000., 9000., 10000.
    bars["open_li"] = np.where(bars.instrument.eq("000001.SZ"), 10100., 9900.)
    result, receipt = build_economic_entry_timing_features_v1(**packet)
    np.testing.assert_allclose(result[TIMING_FEATURES[0]], 2*np.log([1.01, .99]), atol=1e-14)
    np.testing.assert_allclose(result[TIMING_FEATURES[1]], 0., atol=1e-14)
    # An explicit varying path also proves ddof=1 and all nineteen pair positions.
    first = bars.instrument.eq("000001.SZ")
    bars.loc[first, "open_li"] = np.linspace(9500., 10500., 20)
    value, _ = build_economic_entry_timing_features_v1(**packet)
    opens = bars.loc[first, "open_li"].to_numpy()[1:]
    on, intraday = np.log(opens/10000.), np.log(10000./opens)
    np.testing.assert_allclose(on+intraday, 0., atol=1e-14)
    assert value[TIMING_FEATURES[1]].iloc[0] == pytest.approx(np.std(on, ddof=1))
    assert value[TIMING_FEATURES[0]].iloc[0] == pytest.approx(np.mean(on-intraday))
    assert receipt["source_evidence"] == "COMPUTATION_ONLY" and not receipt["deployable"]
    assert not receipt["outcomes_read"] and not receipt["new_native_receipt"]


def test_corporate_action_adjustment_does_not_create_false_overnight_gap(packet):
    expected, _ = build_economic_entry_timing_features_v1(**packet)
    split = packet["raw_daily"].trade_date.ge(pd.Timestamp(packet["calendar"][10]))
    packet["raw_daily"].loc[split, ["open_li", "high_li", "low_li", "close_li"]] /= 2.
    packet["raw_daily"].loc[split, "adj_factor"] *= 2.
    actual, _ = build_economic_entry_timing_features_v1(**packet)
    np.testing.assert_allclose(actual.loc[:, TIMING_FEATURES], expected.loc[:, TIMING_FEATURES], atol=1e-14)


@pytest.mark.parametrize("missing", ["D_close", "earlier_close", "open", "factor", "session"])
def test_needed_values_propagate_independent_unknown_without_compressing_or_deleting(packet, missing):
    bars = packet["raw_daily"]
    day = packet["calendar"][-2] if missing == "D_close" else packet["calendar"][10]
    mask = bars.instrument.eq("000001.SZ") & bars.trade_date.eq(pd.Timestamp(day))
    if missing == "session":
        packet["raw_daily"] = bars.loc[~mask]
    else:
        field = {"D_close": "close_li", "earlier_close": "close_li", "open": "open_li", "factor": "adj_factor"}[missing]
        bars.loc[mask, field] = np.nan
    result, receipt = build_economic_entry_timing_features_v1(**packet)
    assert result.instrument.tolist() == ["000001.SZ", "000002.SZ"]
    assert pd.isna(result[TIMING_FEATURES[0]].iloc[0])
    assert pd.isna(result[TIMING_FEATURES[1]].iloc[0]) == (missing != "D_close")
    assert result.loc[1, list(TIMING_FEATURES)].notna().all()
    assert set(receipt["unknown_fields"][0]["fields"]) == ({TIMING_FEATURES[0]} if missing == "D_close" else set(TIMING_FEATURES))


@pytest.mark.parametrize("state", ["zero", "unknown_volume", "synthetic", "full", "sr", "endpoint", "partial"])
def test_only_real_endpoint_proven_bars_qualify_normal_suspension_is_preserved(packet, state):
    bars = packet["raw_daily"]
    day = pd.Timestamp(packet["calendar"][10])
    mask = bars.instrument.eq("000001.SZ") & bars.trade_date.eq(day)
    if state in ("zero", "unknown_volume"):
        bars.loc[mask, "volume_hand"] = 0. if state == "zero" else np.nan
    elif state == "synthetic":
        bars["synthetic_bar"] = False
        bars.loc[mask, "synthetic_bar"] = True
    else:
        timing = {"full": None, "sr": "10:00-10:30", "endpoint": "09:30-10:30", "partial": "10:00-10:30"}[state]
        rows = [(day, "000001.SZ", "S", timing)]
        if state == "sr":
            rows.append((day, "000001.SZ", "R", "10:30"))
        packet["suspend_rows"] = pd.DataFrame(rows, columns=["trade_date", "instrument", "suspend_type", "suspend_timing"])
    result, receipt = build_economic_entry_timing_features_v1(**packet)
    assert len(result) == 2 and result.loc[1, list(TIMING_FEATURES)].notna().all()
    assert result.loc[0, list(TIMING_FEATURES)].isna().all() == (state != "partial")
    assert bool(receipt["unknown_fields"][0]["fields"]) == (state != "partial")


@pytest.mark.parametrize("poison", ["future", "duplicate", "rank", "numeric", "outcome"])
def test_future_keys_identity_and_invalid_schema_fail_closed(packet, poison):
    if poison == "future":
        packet["raw_daily"].loc[0, "trade_date"] = pd.Timestamp(packet["calendar"][-1])
    elif poison == "duplicate":
        packet["raw_daily"] = pd.concat([packet["raw_daily"], packet["raw_daily"].iloc[:1]], ignore_index=True)
    elif poison == "rank":
        packet["candidates"].loc[0, "selection_effective_rank"] = 2
    elif poison == "numeric":
        packet["raw_daily"].loc[0, "adj_factor"] = -1.
    else:
        packet["raw_daily"]["future_return"] = 100.
    with pytest.raises(AdvisoryModelFirstError):
        build_economic_entry_timing_features_v1(**packet)


def test_hashes_are_stable_and_empty_roster_is_not_fake_model_success(packet):
    one, first = build_economic_entry_timing_features_v1(**packet)
    two, second = build_economic_entry_timing_features_v1(**deepcopy(packet))
    pd.testing.assert_frame_equal(one, two)
    assert first == second
    packet["candidates"] = packet["candidates"].iloc[:0]
    packet["raw_daily"] = packet["raw_daily"].iloc[:0]
    packet["suspend_rows"] = packet["suspend_rows"].iloc[:0]
    result, receipt = build_economic_entry_timing_features_v1(**packet)
    assert result.empty and receipt["status"] == "NO_CANDIDATES" and not receipt["deployable"]
