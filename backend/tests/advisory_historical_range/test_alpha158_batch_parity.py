from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.inference_engine import InferenceEngine
from backend.services.advisory_historical_range.alpha158_batch import Alpha158BatchEvaluator, FEATURE_ORDER, supports_batch


@pytest.mark.parametrize("dtype", [np.float32, np.float64])
def test_indexed_sequence_matches_existing_formulas_and_population(dtype, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dates = pd.bdate_range("2025-09-01", periods=80)
    index = pd.MultiIndex.from_product([dates, ["SH600003", "SZ000002", "SH600001"]], names=["datetime", "instrument"])
    rng = np.random.default_rng(41)
    history = pd.DataFrame(rng.uniform(3, 30, (len(index), 5)).astype(dtype), index=index, columns=["open", "high", "low", "close", "volume"])
    # Both native entry points use the trained Qlib formulas; changing the
    # execution path must not change population, order or unavailable values.
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


def test_sample_deviation_and_statistics_are_bound_to_instrument_labels():
    dates = pd.bdate_range("2025-01-01", periods=70)
    rows = []
    for number, day in enumerate(dates):
        rows.append((day, "Z", 20. + number / 4, 100. + number * 3))
        if number >= 2:  # A first appears after Z, yet sorts before it.
            rows.append((day, "A", 70. + number * 2, 900. + number ** 2))
    history = pd.DataFrame(rows, columns=["datetime", "instrument", "close", "volume"]).set_index(["datetime", "instrument"])
    history["open"] = history["close"]
    history["high"] = history["close"] + 1
    history["low"] = history["close"] - 1
    actual = Alpha158BatchEvaluator(history).for_dates([dates[-1]], ["STD5", "VSTD5", "ROC60", "WVMA5"])
    for instrument in ("A", "Z"):
        source = history.xs(instrument, level="instrument")
        row = actual.loc[(dates[-1], instrument)]
        assert row.STD5 == pytest.approx(source.close.iloc[-5:].std(ddof=1) / source.close.iloc[-1])
        assert row.VSTD5 == pytest.approx(source.volume.iloc[-5:].std(ddof=1) / (source.volume.iloc[-1] + 1e-12))
        assert row.ROC60 == pytest.approx(source.close.iloc[-61] / source.close.iloc[-1])
        moves = (source.close / source.close.shift(1) - 1).abs() * source.volume
        assert row.WVMA5 == pytest.approx(moves.iloc[-5:].std(ddof=1) / (moves.iloc[-5:].mean() + 1e-12))


def test_qlib_partial_windows_and_missing_values_do_not_become_defaults():
    dates = pd.bdate_range("2025-01-01", periods=7)
    index = pd.MultiIndex.from_product([dates, ["A"]], names=["datetime", "instrument"])
    history = pd.DataFrame({"close": [2., 3., np.nan, 5., 6., 6., 6.], "volume": [5., 8., 7., 3., 6., 6., 6.]}, index=index)
    history["open"] = 2.
    history["high"] = 7.
    history["low"] = 1.
    result = Alpha158BatchEvaluator(history).for_dates([dates[1], dates[-1]], ["STD5", "WVMA5", "CORR5", "RSQR5", "RESI5", "ROC60"])
    assert result.loc[(dates[1], "A"), "STD5"] == pytest.approx(pd.Series([2., 3.]).std() / 3.)
    assert pd.isna(result.loc[(dates[1], "A"), "WVMA5"])  # only one valid return
    assert result["ROC60"].isna().all()
    close = history.close.droplevel("instrument")
    volume = history.volume.droplevel("instrument")
    expected_corr = close.rolling(5, min_periods=1).corr(np.log(volume + 1)).iloc[-1]
    assert result.loc[(dates[-1], "A"), "CORR5"] == pytest.approx(expected_corr)
    assert result.loc[(dates[-1], "A"), "STD5"] == pytest.approx(close.iloc[-5:].std() / close.iloc[-1])


def test_constant_qlib_correlation_and_rsquare_remain_unavailable():
    dates = pd.bdate_range("2025-01-01", periods=65)
    index = pd.MultiIndex.from_product([dates, ["A"]], names=["datetime", "instrument"])
    history = pd.DataFrame({"open": 4., "high": 4., "low": 4., "close": 4., "volume": 100.}, index=index)
    result = Alpha158BatchEvaluator(history).for_dates([dates[-1]], ["CORR5", "RSQR5", "RESI5", "STD5"])
    assert result[["CORR5", "RSQR5"]].isna().all().all()
    assert result.iloc[0].RESI5 == 0
    assert result.iloc[0].STD5 == 0


def test_frozen_calendar_preserves_missing_bar_positions_without_filling():
    from types import SimpleNamespace
    dates = pd.bdate_range("2026-07-01", periods=75)
    index = pd.MultiIndex.from_product([dates, ["A"]], names=["datetime", "instrument"])
    rng = np.random.default_rng(18)
    history = pd.DataFrame(rng.uniform(2, 40, (len(index), 5)), index=index,
                           columns=["open", "high", "low", "close", "volume"])
    history = history.drop((dates[-8], "A"))
    original = history.copy(deep=True)
    engine = object.__new__(InferenceEngine)
    engine.input_session = SimpleNamespace(file_dataset=SimpleNamespace(calendar=tuple(dates)))
    result = engine._compute_alpha158_sequence(history, ["ROC60", "CORR60", "CORD60"], 1).iloc[0]
    dense = history.xs("A", level="instrument").reindex(dates)
    assert result.ROC60 == pytest.approx(dense.close.iloc[-61] / dense.close.iloc[-1])
    assert result.ROC60 != pytest.approx(history.close.iloc[-61] / history.close.iloc[-1])
    assert result.CORR60 == pytest.approx(dense.close.rolling(60, min_periods=1).corr(np.log(dense.volume + 1)).iloc[-1])
    returns = dense.close / dense.close.shift(1)
    volume_returns = np.log(dense.volume / dense.volume.shift(1) + 1)
    assert result.CORD60 == pytest.approx(returns.rolling(60, min_periods=1).corr(volume_returns).iloc[-1])
    assert dense.loc[dates[-8]].isna().all()
    pd.testing.assert_frame_equal(history, original)
