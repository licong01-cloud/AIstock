from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.inference_engine import InferenceEngine
from backend.services.advisory_historical_range.alpha158_batch import FEATURE_ORDER, supports_batch


@pytest.mark.parametrize("dtype", [np.float32, np.float64])
def test_indexed_sequence_matches_existing_formulas_and_population(dtype, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dates = pd.bdate_range("2025-09-01", periods=80)
    index = pd.MultiIndex.from_product([dates, ["SH600003", "SZ000002", "SH600001"]], names=["datetime", "instrument"])
    rng = np.random.default_rng(41)
    history = pd.DataFrame(rng.uniform(3, 30, (len(index), 5)).astype(dtype), index=index, columns=["open", "high", "low", "close", "volume"])
    # Late entry, suspension, constant windows and long missing runs retain the
    # exact legacy population/order/missing behavior; no dates are removed.
    days = index.get_level_values("datetime")
    stocks = index.get_level_values("instrument")
    history = history.loc[~((stocks == "SH600001") & (days < dates[25]))]
    history = history.drop((dates[-2], "SZ000002"))
    history.loc[(slice(dates[-65], dates[-10]), "SH600003"), "close"] = np.nan
    history.loc[(slice(dates[-10], dates[-1]), "SZ000002"), "volume"] = 0
    engine = object.__new__(InferenceEngine)
    columns = list(FEATURE_ORDER)
    expected = pd.concat([
        engine._compute_alpha158_last_day_only(history.loc[history.index.get_level_values("datetime") <= day], columns)
        for day in dates[-3:]
    ])
    actual = engine._compute_alpha158_sequence(history, columns, 3)
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)


def test_sequence_future_poison_cannot_change_earlier_day(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dates = pd.bdate_range("2025-09-01", periods=70)
    index = pd.MultiIndex.from_product([dates, ["SH600001", "SZ000002"]], names=["datetime", "instrument"])
    history = pd.DataFrame(np.random.default_rng(7).uniform(1, 20, (len(index), 5)), index=index, columns=["open", "high", "low", "close", "volume"])
    engine = object.__new__(InferenceEngine)
    columns = ["CORR5", "CORD10", "WVMA60", "RSQR20", "RESI5"]
    original = engine._compute_alpha158_sequence(history, columns, 3)
    poisoned = history.copy()
    poisoned.loc[(dates[-1], slice(None)), :] *= 10000
    changed = engine._compute_alpha158_sequence(poisoned, columns, 3)
    pd.testing.assert_frame_equal(changed.loc[dates[-3]], original.loc[dates[-3]], check_exact=True)


def test_unknown_or_unordered_features_use_existing_explicit_day_views():
    dates = pd.to_datetime(["2025-09-03", "2025-09-01"])
    index = pd.MultiIndex.from_product([dates, ["SH600001"]], names=["datetime", "instrument"])
    history = pd.DataFrame({"close": [3.0, 1.0]}, index=index)
    assert not supports_batch(history, ["CORR5"])
    assert not supports_batch(history.sort_index(), ["custom_static_field"])
    assert not supports_batch(history.sort_index(), ["CORR5"])  # Legacy OHLCV validation is not bypassed.
    zoned = history.sort_index().tz_localize("Asia/Shanghai", level="datetime")
    assert not supports_batch(zoned, ["CORR5"])
    engine = object.__new__(InferenceEngine)
    seen = []

    def original(view, columns):
        seen.append(view.index.get_level_values("datetime").max())
        return view.iloc[-1:].rename(columns={"close": columns[0]})

    engine._compute_alpha158_subset = original
    result = engine._compute_alpha158_sequence(history, ["custom_static_field"], 2)
    assert seen == sorted(dates)
    assert len(result) == 2
