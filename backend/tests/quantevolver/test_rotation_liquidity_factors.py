"""Causality, missing-data, standalone and algebra checks for six candidates."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.qe_alpha_candidates.rotation_liquidity_factors import FACTOR_NAMES, compute


@pytest.fixture
def pv():
    rng = np.random.default_rng(314)
    dates = pd.bdate_range("2025-01-01", periods=130)
    index = pd.MultiIndex.from_product([dates, ["000001.SZ", "600000.SH", "300001.SZ"]], names=["datetime", "instrument"])
    close = 10 * np.exp(np.cumsum(rng.normal(0, .025, (130, 3)), axis=0))
    opening = close * np.exp(rng.normal(0, .008, close.shape))
    return pd.DataFrame({
        "close": close.ravel(), "open": opening.ravel(),
        "high": (np.maximum(close, opening) * 1.015).ravel(),
        "low": (np.minimum(close, opening) * .985).ravel(),
        "amount": np.exp(rng.normal(12, .6, close.shape)).ravel(),
    }, index=index)


@pytest.mark.parametrize("name", FACTOR_NAMES)
def test_future_mutation_and_input_order_cannot_change_history(pv, name):
    baseline = compute(pv, name)
    assert len(baseline) > 10
    cutoff = pv.index.levels[0][99]
    changed = pv.copy()
    changed.loc[changed.index.get_level_values("datetime") > cutoff, :] *= 11
    expected = baseline.loc[:cutoff]
    pd.testing.assert_frame_equal(compute(changed, name).loc[:cutoff], expected)
    pd.testing.assert_frame_equal(compute(pv.loc[:cutoff], name), expected)
    pd.testing.assert_frame_equal(compute(pv.sample(frac=1, random_state=7), name), baseline)
    assert baseline.index.names == ["datetime", "instrument"]
    assert baseline.columns.tolist() == [name]
    assert np.isfinite(baseline.to_numpy()).all()


@pytest.mark.parametrize("name", FACTOR_NAMES)
def test_no_cross_stock_contamination(pv, name):
    full = compute(pv, name).xs("000001.SZ", level="instrument", drop_level=False)
    only = pv.loc[pv.index.get_level_values("instrument") == "000001.SZ"]
    pd.testing.assert_frame_equal(compute(only, name), full)


@pytest.mark.parametrize("name", FACTOR_NAMES)
def test_missing_current_close_never_creates_signal(pv, name):
    row = pv.index[-4]
    pv.loc[row, "close"] = np.nan
    assert row not in compute(pv, name).index


@pytest.mark.parametrize("name", FACTOR_NAMES)
def test_price_and_amount_units_do_not_change_factors(pv, name):
    changed = pv.copy()
    changed[["close", "open", "high", "low"]] *= 100
    changed["amount"] *= 1000
    pd.testing.assert_frame_equal(compute(changed, name), compute(pv, name), atol=1e-10, rtol=1e-8)


def test_duplicate_rows_rejected(pv):
    with pytest.raises(ValueError, match="duplicate"):
        compute(pd.concat([pv, pv.iloc[:1]]), FACTOR_NAMES[0])


def test_equal_amount_has_zero_excess_participation(pv):
    pv["amount"] = 1.0
    assert np.allclose(compute(pv, FACTOR_NAMES[0]).to_numpy(), 0)


def test_coupling_matches_independent_pearson_windows(pv):
    name = FACTOR_NAMES[3]
    stock = pv.xs("000001.SZ", level="instrument")
    ret = stock.close.pct_change(fill_method=None)
    volume_change = np.log(stock.amount).diff()
    expected = ret.iloc[-10:].corr(volume_change.iloc[-10:]) - ret.iloc[-60:].corr(volume_change.iloc[-60:])
    actual = compute(pv, name).loc[(stock.index[-1], "000001.SZ"), name]
    assert actual == pytest.approx(expected)


def test_no_down_days_is_unavailable_not_zero(pv):
    wide = pv.close.unstack()
    wide.iloc[:, :] = np.arange(1, len(wide) + 1)[:, None]
    pv["close"] = wide.stack()
    assert compute(pv, FACTOR_NAMES[1]).empty


def test_missing_observation_does_not_bridge_trade_calendar(pv):
    row = (pv.index.levels[0][90], "000001.SZ")
    dropped = pv.drop(index=row)
    # The other securities preserve the shared date. No implicit pct_change fill.
    result = compute(dropped, FACTOR_NAMES[3])
    assert (pv.index.levels[0][91], "000001.SZ") not in result.index


@pytest.mark.parametrize("name", [FACTOR_NAMES[0], FACTOR_NAMES[1], FACTOR_NAMES[2], FACTOR_NAMES[5]])
def test_invalid_current_amount_is_not_replaced_by_zero(pv, name):
    row = pv.index[-4]
    pv.loc[row, "amount"] = np.inf
    assert row not in compute(pv, name).index


def test_divergence_matches_independent_formula(pv):
    stock = pv.xs("600000.SH", level="instrument")
    daily = np.log(stock.close / stock.close.shift())
    delta = np.log(stock.close / stock.open) - np.log(stock.open / stock.close.shift())
    expected = delta.iloc[-20:].mean() / daily.iloc[-20:].std()
    name = FACTOR_NAMES[4]
    assert compute(pv, name).loc[(stock.index[-1], "600000.SH"), name] == pytest.approx(expected)


def test_conditional_impact_matches_independent_formula(pv):
    stock = pv.xs("600000.SH", level="instrument")
    ret = stock.close.pct_change(fill_method=None)
    impact = ret.abs() / stock.amount
    prior = impact.iloc[-25:-5].loc[ret.iloc[-25:-5] < 0].mean()
    recent = impact.iloc[-5:].loc[ret.iloc[-5:] < 0].mean()
    name = FACTOR_NAMES[1]
    assert compute(pv, name).loc[(stock.index[-1], "600000.SH"), name] == pytest.approx(np.log(prior / recent))


@pytest.mark.parametrize("name", FACTOR_NAMES)
def test_catalog_source_executes_with_official_pandas_proxy(pv, name, tmp_path):
    from backend.services.quantevolver.backtest_base_data_memory_cache import BacktestBaseDataMemoryCache
    from backend.services.quantevolver.offline_code_text_factor_executor import OfflineCodeTextFactorExecutor
    from scripts.qe_alpha_candidates.run_rotation_ten_research import sources

    path = tmp_path / "daily_pv.h5"
    pv.to_hdf(path, key="data")
    cache = BacktestBaseDataMemoryCache.load_once(tmp_path, "2025-01-01", "2026-01-01", allowed_files=(path.name,))
    code = sources()[name]
    assert "FACTOR_NAMES[" not in code
    actual = OfflineCodeTextFactorExecutor(cache).compute_factor(name, code)
    assert actual.success, actual.error
    # The official cache contract renames the one factor column to 'value'.
    assert actual.dataframe.columns.tolist() == ["value"]
    expected = compute(pv, name).rename(columns={name: "value"})
    pd.testing.assert_frame_equal(actual.dataframe, expected, check_dtype=False, rtol=1e-6)
