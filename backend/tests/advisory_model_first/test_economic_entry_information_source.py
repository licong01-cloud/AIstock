from datetime import timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_information_source import build_information_feature_rows_v4
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


@pytest.fixture
def inputs():
    days = pd.bdate_range("2025-01-02", periods=21)
    value = np.arange(1., 21.)
    return dict(candidates=pd.DataFrame({"decision_as_of_trade_date": [days[19]], "target_trade_date": [days[20]],
        "instrument": ["000001.SZ"], "selection_effective_rank": [1]}), calendar=tuple(days[:20].date),
        raw_daily=pd.DataFrame({"trade_date": days[:20], "instrument": "000001.SZ", "high_li": value*1000+500,
            "low_li": value*1000-500, "close_li": value*1000, "volume_hand": value,
            "adj_factor": [1.]*19+[2.]}),
        benchmark_daily=pd.DataFrame({"trade_date": days[:20], "instrument": "000300.SH", "close": 100.+value}))


def test_D_adjustment_raw_volume_and_no_future_or_caller_mutation(inputs):
    saved = inputs["raw_daily"].copy(deep=True)
    result, receipt = build_information_feature_rows_v4(**inputs)
    row = result.iloc[0]
    assert row.ret_10 == 3. and row.close_location_in_day == .5
    assert row.volume_ratio_5_to_20 == pytest.approx(18/10.5)
    assert row.relative_ret_5_vs_csi300 == pytest.approx(20/7.5-1-(120/115-1))
    assert receipt["candidate_count"] == 1 and not receipt["new_native_receipt"] and not receipt["deployable"]
    pd.testing.assert_frame_equal(saved, inputs["raw_daily"])
    shuffled = {**inputs, "raw_daily": saved.iloc[::-1]}
    other, same = build_information_feature_rows_v4(**shuffled)
    pd.testing.assert_frame_equal(result, other)
    assert receipt == same


def test_normal_missing_preserves_keys_and_per_field_unknown(inputs):
    args = {**inputs, "raw_daily": inputs["raw_daily"].iloc[1:]}
    row = build_information_feature_rows_v4(**args)[0].iloc[0]
    assert row.ret_10 == 3. and row.volume_ratio_5_to_20 is None
    assert row.unknown_reasons == {"volume_ratio_5_to_20": "VOLUME_WINDOW_INCOMPLETE"}
    args["raw_daily"] = inputs["raw_daily"].iloc[:0]
    empty = build_information_feature_rows_v4(**args)[0]
    assert len(empty) == 1 and all(empty.iloc[0][field] is None for field in ("ret_10", "relative_ret_5_vs_csi300", "close_location_in_day", "volume_ratio_5_to_20"))


@pytest.mark.parametrize("corrupt", ["future", "duplicate", "bool_rank", "string_number", "foreign", "warmup", "target"])
def test_invalid_source_identity_or_clock_fail_closed(inputs, corrupt):
    args = {**inputs, "candidates": inputs["candidates"].copy(), "raw_daily": inputs["raw_daily"].copy()}
    if corrupt == "future":
        args["raw_daily"].loc[0, "trade_date"] = pd.Timestamp(inputs["calendar"][-1] + timedelta(days=1))
    elif corrupt == "duplicate":
        args["raw_daily"] = pd.concat([args["raw_daily"], args["raw_daily"].iloc[:1]])
    elif corrupt == "bool_rank":
        args["candidates"]["selection_effective_rank"] = True
    elif corrupt == "string_number":
        args["raw_daily"]["close_li"] = "1000"
    elif corrupt == "foreign":
        args["raw_daily"].loc[0, "instrument"] = "000002.SZ"
    elif corrupt == "warmup":
        args["calendar"] = inputs["calendar"][1:]
        args["raw_daily"] = inputs["raw_daily"].iloc[1:]
        args["benchmark_daily"] = inputs["benchmark_daily"].iloc[1:]
    else:
        args["candidates"]["target_trade_date"] = args["candidates"].decision_as_of_trade_date
    with pytest.raises(AdvisoryModelFirstError):
        build_information_feature_rows_v4(**args)


def test_raw_identity_binds_same_adjusted_values_and_original_T(inputs):
    original = build_information_feature_rows_v4(**inputs)
    scaled = {**inputs, "raw_daily": inputs["raw_daily"].copy()}
    scaled["raw_daily"]["adj_factor"] *= 2
    changed = build_information_feature_rows_v4(**scaled)
    pd.testing.assert_frame_equal(original[0], changed[0])
    assert original[1]["raw_source_sha256"] != changed[1]["raw_source_sha256"]
    assert original[1]["information_content_sha256"] != changed[1]["information_content_sha256"]


def test_database_decimal_values_are_valid_and_raw_identity_keeps_precision(inputs):
    args={**inputs,"raw_daily":inputs["raw_daily"].copy()}
    args["raw_daily"]["adj_factor"]=args["raw_daily"].adj_factor.map(lambda value:Decimal(str(value)))
    first=build_information_feature_rows_v4(**args)
    pd.testing.assert_frame_equal(first[0],build_information_feature_rows_v4(**inputs)[0])
    args["raw_daily"].loc[0,"adj_factor"]=Decimal("1.0000000000000000000001")
    second=build_information_feature_rows_v4(**args)
    pd.testing.assert_frame_equal(first[0],second[0])
    assert first[1]["raw_source_sha256"]!=second[1]["raw_source_sha256"]
