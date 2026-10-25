from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.generic_daily_price_input_v1 import (
    FEATURES, KEY, ROSTER, build_generic_daily_price_input_v1 as compute,
)


def inputs(count=2):
    calendar = [date(2025, 1, 1) + timedelta(days=i) for i in range(21)]
    decision, target = calendar[-2:]
    symbols = [f"{600000+i}.SH" for i in range(count)]
    candidates = pd.DataFrame([{KEY[0]: decision, KEY[1]: target, "instrument": symbol,
                                "selection_effective_rank": j+1, "candidate_group_size": count}
                               for j, symbol in enumerate(symbols)], columns=ROSTER)
    panel = pd.DataFrame([{"trade_date": day, "instrument": symbol, "open": 100+i+j-.2,
                           "high": 102+i+j, "low": 99+i+j, "close": 100+i+j, "volume": 100+i}
                          for j, symbol in enumerate(symbols) for i, day in enumerate(calendar[:-1])],
                         columns=("trade_date", "instrument", "open", "high", "low", "close", "volume"))
    benchmark = pd.DataFrame([{"trade_date": day, "instrument": "000300.SH", "close": 200+i}
                              for i, day in enumerate(calendar[:-1])])
    return dict(candidates=candidates, calendar=calendar, panel=panel, benchmark_daily=benchmark,
                market_state={"trade_date": decision, "market_up_ratio": .6, "market_definition_id": "declared-breadth-v1",
                              "visible_through": decision},
                source_context={"package_id": "package-a", "run_id": "run-a", "list_version_id": "list-a",
                                "universe_identity": {"type": "index_union", "indices": ["000300.SH", "000905.SH"]},
                                "source_evidence": "CURRENT_DB_NON_VINTAGE", "price_basis": "D_ADJUSTED_CNY",
                                "volume_basis": "RAW_SHARES", "source_visible_through": decision,
                                "benchmark_visible_through": decision})


def test_nine_hand_computed_values_hashes_and_input_immutability():
    data = inputs()
    before = deepcopy(data)
    result, receipt = compute(**data)
    expected = [119/118-1, 119/114-1, 119/109-1, 3/119, 219/214-1,
                119/114-219/214, 1/3, 117/109.5, .6]
    np.testing.assert_allclose(result.loc[0, list(FEATURES)].to_numpy(dtype=float), expected, atol=1e-14, rtol=0)
    pd.testing.assert_frame_equal(result.loc[:, list(ROSTER)].astype({KEY[0]: "datetime64[ns]", KEY[1]: "datetime64[ns]"}),
                                  data["candidates"].astype({KEY[0]: "datetime64[ns]", KEY[1]: "datetime64[ns]"}))
    for field in ("candidates", "panel", "benchmark_daily"):
        pd.testing.assert_frame_equal(data[field], before[field])
    assert data["source_context"] == before["source_context"]
    assert receipt["known_counts"] == dict.fromkeys(FEATURES, 2)
    assert receipt["fit_count"] == 0 and not receipt["outcomes_read"] and not receipt["new_native_receipt"]
    assert receipt == compute(**data)[1]


@pytest.mark.parametrize("missing_volume", [False, True])
def test_all_unknown_price_columns_preserve_candidates_and_independent_volume(missing_volume):
    data = inputs()
    for field in ("open", "high", "low", "close") + (("volume",) if missing_volume else ()):
        data["panel"][field] = np.nan
    original = data["panel"].copy(deep=True)
    result, receipt = compute(**data)
    assert len(result) == 2 and result[["ret_1", "ret_5", "ret_10", "atr14_close", "close_location_in_day"]].isna().all().all()
    if missing_volume:
        assert result.volume_ratio_5_to_20.isna().all()
    else:
        np.testing.assert_allclose(result.volume_ratio_5_to_20, [117/109.5]*2)
    assert result.csi300_ret_5.notna().all() and result.market_up_ratio.eq(.6).all()
    assert receipt["candidate_count"] == 2 and receipt["fit_count"] == 0
    pd.testing.assert_frame_equal(data["panel"], original)


def test_package_identity_and_original_rank_are_metadata_not_predictors():
    data = inputs()
    first, receipt = compute(**data)
    data["source_context"].update(package_id=None, run_id="other-run", universe_identity="other-index-pool")
    same, changed = compute(**data)
    pd.testing.assert_frame_equal(first, same)
    assert receipt["feature_sha256"] == changed["feature_sha256"]
    assert receipt["source_sha256"] != changed["source_sha256"]
    data["candidates"] = data["candidates"].iloc[::-1].reset_index(drop=True)
    data["candidates"]["selection_effective_rank"] = [1, 2]
    swapped, _ = compute(**data)
    pd.testing.assert_frame_equal(first.set_index("instrument").loc[:, list(FEATURES)].sort_index(),
                                  swapped.set_index("instrument").loc[:, list(FEATURES)].sort_index())
    assert not set(FEATURES) & {"parent_combined_score", "parent_rank_pct", "leg_norm_score_gap", "package_id"}


@pytest.mark.parametrize("count", [0, 50, 51])
def test_original_roster_budget_and_real_empty_float_schema(count):
    data = inputs(count)
    if count > 50:
        with pytest.raises(AdvisoryModelFirstError, match="candidate projection"):
            compute(**data)
    else:
        result, receipt = compute(**data)
        assert len(result) == count and tuple(result.columns) == (*ROSTER, *FEATURES)
        assert all(pd.api.types.is_float_dtype(result[field]) for field in FEATURES)
        assert receipt["status"] == ("NO_CANDIDATES" if count == 0 else "COMPUTED")


def test_normal_missing_bar_preserves_all_candidates_and_original_sessions():
    data = inputs()
    symbol = data["candidates"].instrument.iloc[0]
    data["panel"] = data["panel"].loc[~((data["panel"].instrument == symbol) & (data["panel"].trade_date == data["calendar"][-4]))]
    result, receipt = compute(**data)
    assert result.instrument.tolist() == data["candidates"].instrument.tolist()
    assert result.loc[0, ["ret_5", "ret_10", "atr14_close", "volume_ratio_5_to_20"]].isna().all()
    assert result.loc[0, ["ret_1", "close_location_in_day", "market_up_ratio"]].notna().all()
    assert result.loc[1, list(FEATURES)].notna().all()
    assert receipt["unknown_fields"][0]["fields"] and receipt["candidate_count"] == 2


@pytest.mark.parametrize("source", ["price", "benchmark", "market_clock", "market_definition"])
def test_unknown_source_is_per_field_not_a_global_qualification_gate(source):
    data = inputs()
    if source == "price":
        data["source_context"]["source_visible_through"] = None
        unknown = ["ret_1", "ret_5", "ret_10", "atr14_close", "relative_ret_5_vs_csi300",
                   "close_location_in_day", "volume_ratio_5_to_20"]
    elif source == "benchmark":
        data["source_context"]["benchmark_visible_through"] = None
        unknown = ["csi300_ret_5", "relative_ret_5_vs_csi300"]
    else:
        data["market_state"]["visible_through" if source == "market_clock" else "market_definition_id"] = None
        unknown = ["market_up_ratio"]
    result, _ = compute(**data)
    assert result.loc[:, unknown].isna().all().all()
    assert result.loc[:, [field for field in FEATURES if field not in unknown]].notna().all().all()
    assert len(result) == 2


def test_flat_day_zero_volume_and_missing_benchmark_are_unknown_not_zero_fill():
    data = inputs()
    data["panel"]["volume"] = 0
    selected = data["panel"].trade_date == data["calendar"][-2]
    for field in ("open", "high", "low"):
        data["panel"].loc[selected, field] = data["panel"].loc[selected, "close"]
    data["benchmark_daily"] = data["benchmark_daily"].iloc[:-2]
    result, _ = compute(**data)
    assert result.loc[:, ["close_location_in_day", "volume_ratio_5_to_20", "csi300_ret_5", "relative_ret_5_vs_csi300"]].isna().all().all()
    assert result.ret_5.notna().all()


def test_atr_only_consumes_previous_close_not_unused_first_high_low():
    data = inputs()
    selected = data["panel"].trade_date == data["calendar"][-16]
    data["panel"].loc[selected, ["high", "low"]] = np.nan
    result, receipt = compute(**data)
    np.testing.assert_allclose(result.atr14_close, [3/119, 3/120], atol=1e-14, rtol=0)
    assert not receipt["source_identity_rechecked"] and not receipt["qualification_rechecked"]


@pytest.mark.parametrize("bad", ["duplicate", "foreign", "future", "intraday", "bool", "inf", "bad_range", "bad_rank", "price_clock", "old_clock", "market_clock", "old_market_clock", "nonfinite_identity", "signaling_nan"])
def test_contradictions_fail_closed_without_mutating_original_inputs(bad):
    data = inputs()
    if bad == "duplicate":
        data["panel"] = pd.concat([data["panel"], data["panel"].iloc[:1]])
    elif bad in ("foreign", "future", "intraday"):
        if bad == "foreign":
            data["panel"].loc[0, "instrument"] = "000001.SZ"
        else:
            data["panel"].loc[0, "trade_date"] = data["calendar"][-1] if bad == "future" else pd.Timestamp(data["calendar"][0]) + pd.Timedelta(hours=1)
    elif bad in ("bool", "inf", "bad_range", "signaling_nan"):
        field = "high" if bad == "bad_range" else "volume"
        data["panel"][field] = data["panel"][field].astype(object)
        data["panel"].loc[0, field] = True if bad == "bool" else np.inf if bad == "inf" else Decimal("sNaN") if bad == "signaling_nan" else 1
    elif bad == "bad_rank":
        data["candidates"].loc[0, "selection_effective_rank"] = 2
    elif bad in ("price_clock", "old_clock"):
        data["source_context"]["source_visible_through"] = data["calendar"][-1] if bad == "price_clock" else data["calendar"][-3]
    elif bad in ("market_clock", "old_market_clock"):
        data["market_state"]["visible_through"] = data["calendar"][-1] if bad == "market_clock" else data["calendar"][-3]
    else:
        data["source_context"]["universe_identity"] = {"bad": np.nan}
    with pytest.raises(AdvisoryModelFirstError) as error:
        compute(**data)
    assert error.value.reason_code == "ADVISORY_GENERIC_PRICE_INPUT_INVALID"
