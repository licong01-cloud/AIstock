from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.quantevolver import qe_eval_v2_metric_engine as metric_engine
from backend.services.quantevolver import qe_eval_v2_qlib_reader as qlib_reader


@pytest.fixture
def multihorizon_bin(tmp_path):
    """Small real Bin input; all market reads and artifacts remain in tmp_path."""
    dates = pd.bdate_range("2023-01-03", periods=271).delete(7)
    symbols = [f"{index:06d}.SZ" for index in range(12)]
    rng = np.random.default_rng(41060)
    prices = (100 * np.exp(np.cumsum(rng.normal(0.0003, 0.01, (len(dates), 12)), axis=0))).astype("<f4")
    (tmp_path / "calendars").mkdir()
    (tmp_path / "instruments").mkdir()
    (tmp_path / "calendars" / "day.txt").write_text(
        "\n".join(str(date.date()) for date in dates), encoding="utf-8",
    )
    (tmp_path / "instruments" / "all.txt").write_text(
        "\n".join(f"{symbol}\t{dates[0].date()}\t{dates[-1].date()}" for symbol in symbols), encoding="utf-8",
    )
    for index, symbol in enumerate(symbols):
        feature_dir = tmp_path / "features" / symbol.lower()
        feature_dir.mkdir(parents=True)
        np.concatenate((np.array([0], dtype="<f4"), prices[:, index])).tofile(feature_dir / "close.day.bin")
    values = pd.DataFrame(rng.normal(size=prices.shape), index=dates, columns=symbols)
    factors = values.rename_axis(index="datetime", columns="instrument").stack().to_frame("factor")
    return tmp_path, dates, symbols, prices, factors


def _research_context(inputs, holding_periods=None):
    root, dates, symbols, _prices, _factors = inputs
    return metric_engine.prepare_shared_context(
        root, str(dates[0].date()), str(dates[-1].date()), set(symbols),
        load_suspend_d=False, load_st_pit_mask=False, holding_periods=holding_periods,
    )


def _stable_metric_output(result):
    result = {key: value for key, value in result.items() if key != "duration"}
    result["reports"] = [
        {key: value for key, value in row.items() if key != "calculated_at"}
        for row in result["reports"]
    ]
    return json.dumps(result, sort_keys=True)


@pytest.mark.parametrize("holding_days", [40, 60, 120, 240])
def test_optional_long_horizons_use_public_api_and_exact_exit_date(multihorizon_bin, holding_days):
    _root, dates, symbols, prices, factors = multihorizon_bin
    context = _research_context(multihorizon_bin, [holding_days])
    name = f"{holding_days}d"
    assert context["holding_periods"] == {name: holding_days + 1}
    assert set(context["computed_holding_periods"]) == set(metric_engine.HOLDING_PERIODS) | {name}
    expected = prices[holding_days + 1, 0] / prices[1, 0] - 1
    assert context["fwd_ret_mats"][name].loc[dates[0], symbols[0]] == pytest.approx(expected)
    result = metric_engine.compute_single_factor_metrics(
        "factor", factors, context, include_horizon_metrics=True,
        evaluation_windows={"evaluation": {"start": str(dates[0].date()), "end": str(dates[-1].date())}},
    )
    assert result["reports"][0]["status"] == "ok"
    metrics = result["metrics"]["evaluation"]
    assert set(metrics["horizon_metrics"]) == {name}
    assert metrics["horizon_metrics"][name]["n_effective_days"] == len(dates) - holding_days - 1
    assert metrics["horizon_support"][name]["exit_offset"] == holding_days + 1
    assert metrics["horizon_support"][name]["mature_end"] == str(dates[-holding_days - 2].date())
    assert metrics["return_horizon"] == "1d" and metrics["h20_return_horizon"] == "T21T1"
    assert f"rank_ic_{name}" not in metrics
    assert all(f"rank_ic_{period}" in metrics for period in metric_engine.HOLDING_PERIODS)
    assert not any("sharpe" in field or "annual" in field for field in metrics["horizon_metrics"][name])


def test_optional_horizons_preserve_default_outputs_and_concurrent_requests(multihorizon_bin):
    _root, _dates, _symbols, _prices, factors = multihorizon_bin
    baseline_context = _research_context(multihorizon_bin)
    original_periods = dict(metric_engine.HOLDING_PERIODS)
    baseline = metric_engine.compute_single_factor_metrics("factor", factors, baseline_context)
    explicit = metric_engine.compute_single_factor_metrics(
        "factor", factors, _research_context(multihorizon_bin, [1, 5, 10, 20]),
    )
    assert _stable_metric_output(baseline) == _stable_metric_output(explicit)
    assert set(baseline_context["fwd_ret_mats"]) == set(original_periods)
    assert "holding_periods" not in baseline_context
    contexts = [_research_context(multihorizon_bin, [60]), _research_context(multihorizon_bin, [240])]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(
            lambda ctx: metric_engine.compute_single_factor_metrics("factor", factors, ctx, include_horizon_metrics=True),
            contexts,
        ))
    assert [set(result["metrics"]["full"]["horizon_metrics"]) for result in results] == [{"60d"}, {"240d"}]
    assert set(contexts[0]["fwd_ret_mats"]) == set(original_periods) | {"60d"}
    assert set(contexts[1]["fwd_ret_mats"]) == set(original_periods) | {"240d"}
    repeated = metric_engine.compute_single_factor_metrics("factor", factors, baseline_context)
    assert _stable_metric_output(baseline) == _stable_metric_output(repeated)
    assert metric_engine.HOLDING_PERIODS == original_periods
    assert _stable_metric_output(baseline) == _stable_metric_output(
        metric_engine.compute_single_factor_metrics("factor", factors, _research_context(multihorizon_bin))
    )


def test_long_horizon_maturity_does_not_crop_short_horizons_or_sliced_context(multihorizon_bin):
    _root, dates, _symbols, _prices, factors = multihorizon_bin
    context = _research_context(multihorizon_bin, [1, 20, 60, 240])
    start = dates[len(dates) - 62]
    # The research consumer may slice signal rows after labels were built.
    view = dict(context)
    view["dates"] = context["dates"][context["dates"] >= start]
    for key in ("close_unstacked", "st_pit_eligible_mask"):
        view[key] = context[key].loc[start:]
    view["fwd_ret_mats"] = {key: frame.loc[start:] for key, frame in context["fwd_ret_mats"].items()}
    view.update(data_start=str(start.date()))
    result = metric_engine.compute_single_factor_metrics(
        "factor", factors, view, include_horizon_metrics=True,
        evaluation_windows={"tail": {"start": str(start.date()), "end": str(dates[-1].date())}},
    )["metrics"]["tail"]
    assert result["horizon_metrics"]["1d"]["n_effective_days"] == 60
    assert result["horizon_metrics"]["60d"]["n_effective_days"] == 1
    assert result["horizon_support"]["60d"]["mature_end"] == str(start.date())
    assert result["horizon_support"]["60d"]["n_immature_days"] == 61
    assert result["horizon_support"]["240d"]["status"] == "unmatured"
    assert result["horizon_metrics"]["240d"]["ic_mean"] is None
    assert result["common_horizon_support"]["n_mature_days"] == 0
    assert result["n_trading_days"] == 62


def test_long_horizon_calendar_gaps_and_per_stock_missing_prices_remain_nan(multihorizon_bin, monkeypatch):
    root, dates, symbols, prices, factors = multihorizon_bin
    frame = metric_engine.read_close_prices(root).copy()
    frame = frame.loc[frame.index.get_level_values("datetime") != dates[61]]
    frame.loc[(dates[30], symbols[0]), "close"] = np.nan
    calls = []
    monkeypatch.setattr(metric_engine, "read_close_prices", lambda *args, **kwargs: calls.append(args) or frame)
    context = _research_context(multihorizon_bin, [60])
    assert len(calls) == 1
    assert context["dates"].equals(dates)
    assert context["close_unstacked"].loc[dates[61]].isna().all()
    labels = context["fwd_ret_mats"]["60d"]
    assert labels.loc[dates[0]].isna().all()
    assert np.isnan(labels.loc[dates[29], symbols[0]])
    assert labels.loc[dates[29], symbols[1]] == pytest.approx(prices[90, 1] / prices[30, 1] - 1)
    metrics = metric_engine.compute_single_factor_metrics(
        "factor", factors, context, include_horizon_metrics=True,
        evaluation_windows={"entry_gap": {"start": str(dates[0].date()), "end": str(dates[0].date())}},
    )["metrics"]["entry_gap"]
    assert metrics["horizon_support"]["60d"]["status"] == "price_missing"
    assert metrics["horizon_support"]["60d"]["n_mature_days"] == 1
    assert metrics["horizon_support"]["60d"]["n_missing_price_pairs"] == 12
    assert metrics["horizon_metrics"]["60d"]["rank_ic_mean"] is None


@pytest.mark.parametrize(
    ("pit_excluded", "suspended", "missing_factors", "status", "valid_pairs"),
    [(2, 1, 0, "ok", 9), (6, 1, 0, "insufficient_cross_section", 5),
     (12, 0, 0, "no_eligible_samples", 0), (0, 0, 12, "factor_unavailable", 0)],
)
def test_long_horizon_preserves_pit_and_suspend_masks_without_compressing_dates(
    multihorizon_bin, monkeypatch, pit_excluded, suspended, missing_factors, status, valid_pairs,
):
    root, dates, symbols, _prices, factors = multihorizon_bin
    eligibility = np.ones((len(dates), len(symbols)), dtype=bool)
    eligibility[0, :pit_excluded] = False
    pairs = {(dates[0], symbol) for symbol in symbols[pit_excluded:pit_excluded + suspended]}

    class FrozenUniverse:
        def build_eligible_mask(self, actual_dates, actual_symbols, **_kwargs):
            assert actual_dates.equals(dates) and actual_symbols == symbols
            return eligibility

        def metadata(self, **_kwargs):
            return {"universe_key": "test_frozen_pit", "universe_fingerprint_sha256": "fixture"}

    monkeypatch.setattr(metric_engine, "FactorUniverseMaskService", FrozenUniverse)
    monkeypatch.setattr(metric_engine, "_load_suspend_pairs", lambda *_args: pairs)
    context = metric_engine.prepare_shared_context(
        root, str(dates[0].date()), str(dates[-1].date()), set(symbols), holding_periods=[1, 60],
    )
    factor_view = factors.copy()
    if missing_factors:
        factor_view.loc[(dates[0], symbols[:missing_factors]), "factor"] = np.nan
    metrics = metric_engine.compute_single_factor_metrics(
        "factor", factor_view, context, include_horizon_metrics=True,
        evaluation_windows={"one_day": {"start": str(dates[0].date()), "end": str(dates[0].date())}},
    )["metrics"]["one_day"]
    assert context["dates"].equals(dates)
    assert metrics["universe"] == "test_frozen_pit"
    for name in ("1d", "60d"):
        support = metrics["horizon_support"][name]
        assert support["status"] == status
        assert support["n_mature_days"] == 1 and support["n_immature_days"] == 0
        assert support["n_valid_pairs"] == valid_pairs
        assert support["n_missing_price_pairs"] == 0
        assert support["n_factor_missing_pairs"] == missing_factors
        assert metrics["horizon_metrics"][name]["n_effective_days"] == (1 if valid_pairs >= 6 else 0)
    assert metrics["common_horizon_support"]["n_valid_pairs"] == valid_pairs


def test_optional_horizon_mapping_is_detached_sorted_and_keeps_internal_dependencies(multihorizon_bin):
    requested = [240, 1, 60, 60]
    context = _research_context(multihorizon_bin, requested)
    requested.append(120)
    assert list(context["holding_periods"].items()) == [("1d", 2), ("60d", 61), ("240d", 241)]
    assert "120d" not in context["fwd_ret_mats"]
    short_context = _research_context(multihorizon_bin, [5])
    assert short_context["computed_holding_periods"] == metric_engine.HOLDING_PERIODS
    output = metric_engine.compute_single_factor_metrics(
        "factor", multihorizon_bin[-1], short_context, include_horizon_metrics=True,
    )["metrics"]["full"]
    assert set(output["horizon_metrics"]) == {"5d"}
    assert output["return_horizon"] == "1d" and output["h20_return_horizon"] == "T21T1"
    assert "horizon_support" not in output


@pytest.mark.parametrize("invalid", [[], [True], [60.0], [0], [241], "60d", 60])
def test_optional_horizons_reject_invalid_request_before_market_reads(monkeypatch, invalid):
    def no_reads(*_args, **_kwargs):
        raise AssertionError("invalid request must not read market data")
    monkeypatch.setattr(metric_engine, "read_close_prices", no_reads)
    with pytest.raises(ValueError, match="holding_periods"):
        metric_engine.prepare_shared_context(holding_periods=invalid)


def test_research_calendar_preserves_missing_price_dates_and_explicit_bounds(multihorizon_bin):
    root, dates, _symbols, _prices, _factors = multihorizon_bin
    assert qlib_reader.read_trading_calendar(root).equals(dates)
    assert qlib_reader.read_trading_calendar(
        root, start_date=str(dates[3].date()), end_date=str(dates[8].date()),
    ).equals(dates[3:9])


@pytest.mark.parametrize("calendar", [
    "2026-01-02\n2026-01-01\n", "2026-01-01\n2026-01-01\n",
    "2026-01-01 12:00:00\n2026-01-02 12:00:00\n", "\n",
])
def test_research_calendar_rejects_invalid_market_dates(tmp_path, calendar):
    (tmp_path / "calendars").mkdir()
    (tmp_path / "calendars" / "day.txt").write_text(calendar, encoding="utf-8")
    with pytest.raises((ValueError, pd.errors.EmptyDataError)):
        qlib_reader.read_trading_calendar(tmp_path)
