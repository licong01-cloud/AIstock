"""PIT membership boundaries and actual supplemental loader behavior."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.qe_alpha_candidates.prepare_rotation_index_context import build_context
from scripts.qe_alpha_candidates.rotation_index_factors import FACTOR_NAMES, compute


@pytest.fixture
def inputs():
    days = pd.bdate_range("2024-01-01", periods=120)
    rng = np.random.default_rng(19)
    index = pd.concat([pd.DataFrame({"trade_date": days, "ts_code": code,
                                    "close": 1000 * np.exp(np.cumsum(rng.normal(.001, .01, len(days))))})
                       for code in ["000300.SH", "000905.SH", "000852.SH"]], ignore_index=True)
    members = pd.DataFrame([
        ["csi500", "000905.SH", "000001.SZ", days[0], days[80]],
        ["csi1000", "000852.SH", "000001.SZ", days[80], None],
        ["csi300", "000300.SH", "600000.SH", days[0], None],
    ], columns=["pool_id", "index_code", "ts_code", "effective_from", "effective_to_exclusive"])
    pv = pd.DataFrame({"close": np.exp(rng.normal(3, .02, 240))},
                      index=pd.MultiIndex.from_product([days, ["000001.SZ", "600000.SH"]], names=["datetime", "instrument"]))
    return days, index, members, pv


def test_half_open_and_order_invariance(inputs):
    days, prices, members, _ = inputs
    c = build_context(prices, members, days)
    assert c.loc[(days[79], "000001.SZ"), "pool_id"] == 500
    assert c.loc[(days[80], "000001.SZ"), "pool_id"] == 1000
    pd.testing.assert_frame_equal(c, build_context(prices.sample(frac=1), members.iloc[::-1], days))


def test_no_membership_or_future_price_backfill(inputs):
    days, prices, members, _ = inputs
    c = build_context(prices, members, days)
    changed = prices.copy()
    changed.loc[changed.trade_date > days[70], "close"] *= 11
    m = members.copy()
    m.loc[1, "ts_code"] = "300001.SZ"
    actual = build_context(changed, m, days)
    pd.testing.assert_frame_equal(actual.loc[:days[70]], c.loc[:days[70]])


def test_overlap_and_source_mismatch_rejected(inputs):
    days, prices, members, _ = inputs
    with pytest.raises(ValueError, match="overlapping"):
        build_context(prices, pd.concat([members, members.iloc[:1]]), days)
    members.loc[0, "index_code"] = "000300.SH"
    with pytest.raises(ValueError, match="mismatch"):
        build_context(prices, members, days)


@pytest.mark.parametrize("name", FACTOR_NAMES)
def test_prefix_stock_isolation_and_no_future_signal(inputs, name):
    days, prices, members, pv = inputs
    context = build_context(prices, members, days)
    baseline = compute(pv, context, name)
    assert not baseline.empty
    expected = baseline.loc[:days[75]]
    pd.testing.assert_frame_equal(compute(pv.loc[:days[75]], context.loc[:days[75]], name), expected)
    pd.testing.assert_frame_equal(compute(pv.sample(frac=1), context.iloc[::-1], name), baseline)
    assert np.isfinite(baseline.to_numpy()).all()


def test_beta_does_not_mix_buckets(inputs):
    days, prices, members, pv = inputs
    c = build_context(prices, members, days)
    result = compute(pv, c, FACTOR_NAMES[2])
    assert (days[75], "000001.SZ") in result.index
    assert (days[81], "000001.SZ") not in result.index


def test_index_returns_computed_before_membership_mapping(inputs):
    days, prices, members, pv = inputs
    c = build_context(prices, members, days)
    index = prices.loc[prices.ts_code == "000852.SH"].set_index("trade_date").close
    assert c.loc[(days[80], "000001.SZ"), "index_return_1d"] == pytest.approx(index.loc[days[80]] / index.loc[days[79]] - 1)
    assert c.xs("600000.SH", level="instrument").relative_strength_20d.dropna().eq(0).all()
    nonmember = pv.copy()
    nonmember.index = pd.MultiIndex.from_arrays([pv.index.get_level_values(0), ["000099.SZ"] * len(pv)], names=["datetime", "instrument"])
    nonmember = nonmember.loc[~nonmember.index.duplicated()]
    assert compute(nonmember, c, FACTOR_NAMES[0]).empty


def test_explicit_context_does_not_shadow_existing_inputs(inputs, tmp_path):
    from backend.services.quantevolver.backtest_base_data_memory_cache import BacktestBaseDataMemoryCache
    days, prices, members, pv = inputs
    base, extra = tmp_path / "base", tmp_path / "extra"
    base.mkdir()
    extra.mkdir()
    pv.to_hdf(base / "daily_pv.h5", key="data")
    c = build_context(prices, members, days)
    c.to_hdf(extra / "index_factor_context.h5", key="data")
    (pv * 10).to_hdf(extra / "daily_pv.h5", key="data")
    loaded = BacktestBaseDataMemoryCache.load_once(base, str(days[0]), str(days[-1]), supplemental_data_dir=extra)
    pd.testing.assert_frame_equal(loaded.get("daily_pv.h5"), pv)
    pd.testing.assert_frame_equal(loaded.get("index_factor_context.h5"), c)
    with pytest.raises(FileNotFoundError, match="not loaded"):
        BacktestBaseDataMemoryCache.load_once(base, str(days[0]), str(days[-1])).get("index_factor_context.h5")


@pytest.mark.parametrize("name", FACTOR_NAMES)
def test_specialized_catalog_source_equals_reference(inputs, tmp_path, name):
    from backend.services.quantevolver.backtest_base_data_memory_cache import BacktestBaseDataMemoryCache
    from backend.services.quantevolver.offline_code_text_factor_executor import OfflineCodeTextFactorExecutor
    from scripts.qe_alpha_candidates.run_rotation_ten_research import sources
    days, prices, members, pv = inputs
    context = build_context(prices, members, days)
    extra = tmp_path / "extra"
    extra.mkdir()
    pv.to_hdf(tmp_path / "daily_pv.h5", key="data")
    context.to_hdf(extra / "index_factor_context.h5", key="data")
    cache = BacktestBaseDataMemoryCache.load_once(tmp_path, str(days[0]), str(days[-1]), supplemental_data_dir=extra)
    code = sources()[name]
    assert "FACTOR_NAMES[" not in code
    result = OfflineCodeTextFactorExecutor(cache).compute_factor(name, code)
    assert result.success, result.error
    pd.testing.assert_frame_equal(result.dataframe, compute(pv, context, name).rename(columns={name: "value"}))


def test_nan_diagnostic_is_not_fabricated_zero():
    from scripts.qe_alpha_candidates.run_rotation_ten_research import json_safe
    assert json_safe({"value": [float("nan"), float("inf"), .2]}) == {"value": [None, None, .2]}


def test_catalog_keeps_evaluated_bytes_across_python_unparse_versions(tmp_path):
    from scripts.qe_alpha_candidates.validate_rotation_ten_library_dev import evaluated_sources
    evaluated = "(close, amount) = (1, 2)\n"
    (tmp_path / "factor.py").write_text(evaluated, encoding="utf-8")
    assert evaluated_sources(tmp_path, {"factor": "close, amount = (1, 2)\n"}) == {"factor": evaluated}
    with pytest.raises(ValueError, match="evaluated source differs"):
        evaluated_sources(tmp_path, {"factor": "close, amount = (2, 1)\n"})


@pytest.mark.parametrize("explicit", [None, "deepseek/deepseek-chat"])
def test_classification_model_override_is_per_call_only(monkeypatch, explicit):
    from types import SimpleNamespace
    from backend.services.quantevolver import factor_analyst, llm_client, prompt_manager
    configured = {"model": "deepseek/deepseek-v4-pro"}
    calls = []

    def completion(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='{"category":"LIQ"}'))])

    monkeypatch.setattr(factor_analyst, "_get_llm_client", lambda: SimpleNamespace(completion=completion))
    monkeypatch.setattr(llm_client, "get_llm_kwargs", lambda _: configured.copy())
    monkeypatch.setattr(prompt_manager.PromptManager, "get_active_prompt_text", lambda *args: None)
    result = factor_analyst._analyze_factor_v2("liquidity", "manual", None, "x=1", {}, None, llm_model=explicit)
    assert result["category"] == "LIQ"
    assert calls[0]["model"] == (explicit or configured["model"])
    assert configured == {"model": "deepseek/deepseek-v4-pro"}


def test_new_pair_dev_selection_is_complete_and_never_writes_old_pairs():
    from scripts.qe_alpha_candidates.validate_rotation_ten_library_dev import new_pair_records
    payload = {"method": "spearman_ewma", "snapshot_date": "2026-08-31", "pairs": [
        {"factor_a": "a", "factor_b": "b", "correlation": None},
        {"factor_a": "a", "factor_b": "old", "correlation": .9},
    ]}
    result = new_pair_records(payload, {"a", "b"})
    assert len(result) == 1 and result[0]["correlation"] is None
    with pytest.raises(ValueError, match="incomplete"):
        new_pair_records(payload, {"a", "b", "c"})
    payload["pairs"].append(payload["pairs"][0])
    with pytest.raises(ValueError, match="duplicate"):
        new_pair_records(payload, {"a", "b"})


@pytest.mark.parametrize("method", ["_get_independent_metrics", "_get_experiment_track_summary", "_get_multi_window_metrics"])
def test_optional_classifier_statistics_report_unavailable(monkeypatch, caplog, method):
    from backend.services.quantevolver import factor_analyst

    def unavailable():
        raise RuntimeError("sensitive connection details must not be logged")

    monkeypatch.setattr(factor_analyst, "get_conn", unavailable)
    assert getattr(factor_analyst.FactorAnalyst(), method)("sample_factor") is None
    assert "unavailable" in caplog.text and "RuntimeError" in caplog.text
    assert "sensitive connection" not in caplog.text
