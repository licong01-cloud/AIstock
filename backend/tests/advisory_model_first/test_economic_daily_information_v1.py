from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_information_v1 import build_economic_daily_information_v1, FEATURES
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@pytest.fixture
def inputs():
    days = pd.bdate_range("2025-09-01", periods=20)
    values = np.arange(1., 21.)
    panel = pd.DataFrame({"close": values, "high": values + .5, "low": values - .5, "volume": values},
        index=pd.MultiIndex.from_product([days, ["000001.SZ"]], names=["datetime", "instrument"]))
    return dict(decision_date=days[-1].date(), candidates=[{"instrument": "000001.SZ", "selection_rank": 1}],
        calendar=list(days.date), panel=panel, benchmark_return_5d=.05, benchmark_as_of=days[-1].date(),
        price_basis="D_ADJUSTED", volume_basis="RAW_SAME_UNIT")


@pytest.mark.parametrize("index_kind", ["timestamp", "date", "iso_text"])
def test_hand_calculated_values_identity_and_missing_candidate(inputs, index_kind):
    if index_kind != "timestamp":
        days = list(inputs["panel"].index.get_level_values("datetime").date)
        inputs["panel"].index = pd.MultiIndex.from_arrays([days if index_kind == "date" else [day.isoformat() for day in days],
            inputs["panel"].index.get_level_values("instrument")], names=inputs["panel"].index.names)
    inputs["candidates"].append({"instrument": "000002.SZ", "selection_rank": 2})
    panel_copy = inputs["panel"].copy(deep=True)
    result = build_economic_daily_information_v1(**inputs)
    assert list(result["rows"][0]["values"]) == list(FEATURES)
    assert list(result["rows"][0]["values"].values()) == pytest.approx([1., 20 / 15 - 1 - .05, .5, 18 / 10.5])
    assert not result["rows"][0]["unknown_reasons"]
    missing = result["rows"][1]
    assert missing["instrument"] == "000002.SZ" and all(value is None for value in missing["values"].values())
    assert set(missing["unknown_reasons"]) == set(FEATURES)
    assert result["source_evidence"] == "COMPUTATION_ONLY" and not result["deployable"] and not result["outcomes_read"]
    digest = result.pop("information_sha256")
    assert sha(result) == digest
    pd.testing.assert_frame_equal(panel_copy, inputs["panel"])


@pytest.mark.parametrize("kind,unknown", [("missing_D", FEATURES), ("old_gap", (FEATURES[3],)),
    ("flat", (FEATURES[2],)), ("zero_volume", (FEATURES[3],)), ("missing_column", (FEATURES[3],)),
    ("missing_benchmark", (FEATURES[1],)), ("nullable_volume", (FEATURES[3],)), ("suspended_D", (FEATURES[2],))])
def test_normal_missing_and_suspension_never_drop_or_manufacture(inputs, kind, unknown):
    panel = inputs["panel"]
    if kind == "missing_D":
        inputs["panel"] = panel.iloc[:-1]
    elif kind == "old_gap":
        inputs["panel"] = panel.iloc[1:]
    elif kind == "flat":
        panel.loc[panel.index[-1], ["high", "low"]] = 20.
    elif kind == "zero_volume":
        panel["volume"] = 0.
    elif kind == "missing_column":
        inputs["panel"] = panel.drop(columns="volume")
    elif kind == "missing_benchmark":
        inputs["benchmark_return_5d"] = None
    elif kind == "nullable_volume":
        panel["volume"] = panel["volume"].astype("Float64")
        panel.loc[panel.index[-1], "volume"] = pd.NA
    elif kind == "suspended_D":
        # Caller-provided suspension-normalized bar; calculator cannot fill it.
        panel.loc[panel.index[-1], ["close", "high", "low", "volume"]] = [19., 19., 19., 0.]
    result = build_economic_daily_information_v1(**inputs)
    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert {name for name, value in row["values"].items() if value is None} == set(unknown)
    assert set(row["unknown_reasons"]) == set(unknown)
    if kind == "suspended_D":
        assert row["values"][FEATURES[3]] == pytest.approx((16 + 17 + 18 + 19) / 5 / (sum(range(1, 20)) / 20))


@pytest.mark.parametrize("kind", ["future", "intraday", "foreign", "duplicate", "calendar", "rank_bool", "rank_duplicate",
    "symbol_duplicate", "benchmark_future", "benchmark_impossible", "infinite", "bool", "string", "negative_volume", "bad_ohlc",
    "price_basis", "invalid_date", "timezone", "row_budget", "candidate_budget", "column_duplicate", "aliased_duplicate"])
def test_foreign_time_identity_and_malformed_values_fail_closed(inputs, kind):
    panel = inputs["panel"]
    if kind in {"future", "intraday", "foreign"}:
        days = list(panel.index.get_level_values("datetime"))
        symbols = list(panel.index.get_level_values("instrument"))
        if kind == "future":
            days[-1] += pd.Timedelta(days=1)
        elif kind == "intraday":
            days[-1] += pd.Timedelta(hours=1)
        else:
            symbols[-1] = "000002.SZ"
        panel.index = pd.MultiIndex.from_arrays([days, symbols], names=panel.index.names)
    elif kind == "duplicate":
        inputs["panel"] = pd.concat([panel.iloc[:1], panel])
    elif kind == "calendar":
        inputs["calendar"][-2] = inputs["calendar"][-1]
    elif kind == "rank_bool":
        inputs["candidates"][0]["selection_rank"] = True
    elif kind in {"rank_duplicate", "symbol_duplicate"}:
        inputs["candidates"].append({"instrument": "000002.SZ" if kind == "rank_duplicate" else "000001.SZ",
                                     "selection_rank": 1 if kind == "rank_duplicate" else 2})
    elif kind == "benchmark_future":
        inputs["benchmark_as_of"] += timedelta(days=1)
    elif kind == "benchmark_impossible":
        inputs["benchmark_return_5d"] = -1.
    elif kind == "invalid_date":
        keys = list(panel.index)
        keys[-1] = ("not-a-date", "000001.SZ")
        panel.index = pd.MultiIndex.from_tuples(keys, names=panel.index.names)
    elif kind == "timezone":
        panel.index = pd.MultiIndex.from_arrays([panel.index.get_level_values("datetime").tz_localize("UTC"),
                                                panel.index.get_level_values("instrument")], names=panel.index.names)
    elif kind == "row_budget":
        inputs["panel"] = pd.concat([panel] * 21)
    elif kind == "candidate_budget":
        inputs["candidates"] = [{"instrument": f"{value:06d}.SZ", "selection_rank": value} for value in range(1, 22)]
    elif kind == "column_duplicate":
        inputs["panel"] = pd.concat([panel, panel[["close"]]], axis=1)
    elif kind == "aliased_duplicate":
        alias = panel.iloc[:1].copy()
        alias.index = pd.MultiIndex.from_tuples([(inputs["calendar"][0].isoformat(), "000001.SZ")], names=panel.index.names)
        inputs["panel"] = pd.concat([panel, alias])
    elif kind in {"infinite", "bool", "string"}:
        panel["volume"] = panel["volume"].astype(object)
        panel.loc[panel.index[-1], "volume"] = {"infinite": np.inf, "bool": True, "string": "20"}[kind]
    elif kind == "negative_volume":
        panel.loc[panel.index[-1], "volume"] = -1.
    elif kind == "bad_ohlc":
        panel.loc[panel.index[-1], "high"] = 19.
    else:
        inputs["price_basis"] = "raw_unproven"
    with pytest.raises(AdvisoryModelFirstError):
        build_economic_daily_information_v1(**inputs)


def test_source_digest_binds_unused_values_and_is_row_order_invariant(inputs):
    first = build_economic_daily_information_v1(**inputs)
    inputs["panel"] = inputs["panel"].iloc[::-1]
    assert build_economic_daily_information_v1(**inputs) == first
    panel = inputs["panel"].copy()
    panel.loc[panel.index[-1], "high"] += 1.
    inputs["panel"] = panel
    changed = build_economic_daily_information_v1(**inputs)
    assert changed["rows"] == first["rows"]
    assert changed["source_sha256"] != first["source_sha256"] and changed["information_sha256"] != first["information_sha256"]
    panel["volume"] = np.nan
    inputs["panel"] = panel
    null_column = build_economic_daily_information_v1(**inputs)
    inputs["panel"] = panel.drop(columns="volume")
    absent_column = build_economic_daily_information_v1(**inputs)
    assert absent_column["rows"] == null_column["rows"]
    assert absent_column["source_sha256"] != null_column["source_sha256"]
    inputs["panel"] = panel.assign(volume=1e308)
    assert build_economic_daily_information_v1(**inputs)["rows"][0]["values"][FEATURES[3]] == pytest.approx(1.)
    inputs["candidates"] = []
    inputs["panel"] = inputs["panel"].iloc[:0]
    assert build_economic_daily_information_v1(**inputs)["rows"] == []
