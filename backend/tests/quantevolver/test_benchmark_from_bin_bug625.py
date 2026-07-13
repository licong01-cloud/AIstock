"""BUG-625: QE benchmark sourced from the Qlib bin (000300.SH), not a stale parquet.

The daily/minute runners' ``load_benchmark_series`` must:
  * read 000300.SH daily returns from the backtest Qlib bin,
  * fail-fast (raise) on empty / end-short / interior-gap / missing-window,
  * never silently disable / fill 0 / truncate,
  * behave identically across qrun_limit.py and qrun_limit_minute.py.

Runner scripts carry heavy top-level qlib imports, so (matching the existing
read_exp_res test) we slice the two functions out of source and exec them with a
faked ``qlib.data.D`` — no real qlib / bin needed.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
RUNNERS = ["scripts/qrun_limit.py", "scripts/qrun_limit_minute.py"]
FULL_WINDOW = {"task": {"backtest": {"start_time": "2024-07-01", "end_time": "2026-06-30"}}}


def _load_benchmark_fn(runner_rel: str):
    source = (REPO_ROOT / runner_rel).read_text(encoding="utf-8")
    start = source.index("def load_benchmark_series")
    end = source.index("def inject_benchmark")
    ns: dict = {"Path": Path, "pd": pd}
    exec(source[start:end], ns)  # noqa: S102 - trusted first-party source slice
    return ns["load_benchmark_series"]


def _install_fake_D(features_result):
    mod = types.ModuleType("qlib.data")

    class _D:
        @staticmethod
        def features(instruments, fields, start_time=None, end_time=None, freq="day"):
            assert instruments == ["000300.SH"], instruments
            return features_result

    mod.D = _D
    sys.modules["qlib"] = sys.modules.get("qlib") or types.ModuleType("qlib")
    sys.modules["qlib.data"] = mod


def _bench_df(dates, vals):
    idx = pd.MultiIndex.from_product(
        [["000300.SH"], pd.to_datetime(dates)], names=["instrument", "datetime"]
    )
    return pd.DataFrame({"bench": list(vals)}, index=idx)


@pytest.mark.parametrize("runner", RUNNERS)
def test_load_benchmark_from_bin_success(runner):
    _install_fake_D(_bench_df(["2024-07-01", "2025-01-02", "2026-06-30"], [0.01, 0.02, 0.03]))
    fn = _load_benchmark_fn(runner)
    sr = fn(FULL_WINDOW)
    assert list(sr.index.strftime("%Y-%m-%d")) == ["2024-07-01", "2025-01-02", "2026-06-30"]
    assert sr.index.name == "datetime"
    assert str(sr.index.max().date()) == "2026-06-30"
    assert not sr.isna().any()


@pytest.mark.parametrize("runner", RUNNERS)
def test_load_benchmark_empty_bin_fail_fast(runner):
    _install_fake_D(pd.DataFrame())
    fn = _load_benchmark_fn(runner)
    with pytest.raises(RuntimeError, match="bench_empty"):
        fn(FULL_WINDOW)


@pytest.mark.parametrize("runner", RUNNERS)
def test_load_benchmark_end_short_fail_fast(runner):
    # ends 2026-03-10 while required_end=2026-06-30 -> refuse partial benchmark
    _install_fake_D(_bench_df(["2024-07-01", "2026-03-10"], [0.01, 0.02]))
    fn = _load_benchmark_fn(runner)
    with pytest.raises(RuntimeError, match="bench_end_short"):
        fn(FULL_WINDOW)


@pytest.mark.parametrize("runner", RUNNERS)
def test_load_benchmark_no_window_fail_fast(runner):
    _install_fake_D(_bench_df(["2024-07-01", "2026-06-30"], [0.01, 0.02]))
    fn = _load_benchmark_fn(runner)
    with pytest.raises(RuntimeError, match="bench_no_window"):
        fn({})


@pytest.mark.parametrize("runner", RUNNERS)
def test_load_benchmark_interior_gap_fail_fast(runner):
    _install_fake_D(_bench_df(["2024-07-01", "2025-01-02", "2026-06-30"], [0.01, np.nan, 0.03]))
    fn = _load_benchmark_fn(runner)
    with pytest.raises(RuntimeError, match="bench_internal_gap"):
        fn(FULL_WINDOW)


@pytest.mark.parametrize("runner", RUNNERS)
def test_load_benchmark_leading_nan_tolerated(runner):
    # first row NaN (no prior close at window start) is dropped, not an error
    _install_fake_D(_bench_df(["2024-07-01", "2025-01-02", "2026-06-30"], [np.nan, 0.02, 0.03]))
    fn = _load_benchmark_fn(runner)
    sr = fn(FULL_WINDOW)
    assert list(sr.index.strftime("%Y-%m-%d")) == ["2025-01-02", "2026-06-30"]
    assert not sr.isna().any()


@pytest.mark.parametrize("runner", RUNNERS)
def test_load_benchmark_does_not_read_parquet(runner):
    src = (REPO_ROOT / runner).read_text(encoding="utf-8")
    fn_src = src[src.index("def load_benchmark_series"): src.index("def inject_benchmark")]
    assert "benchmark_sh000300.parquet" not in fn_src
    assert "benchmark disabled" not in fn_src  # no silent-disable path


def test_read_exp_res_excess_no_silent_parquet_fill():
    src = (REPO_ROOT / "backend/services/quantevolver/templates/read_exp_res.py").read_text(encoding="utf-8")
    assert "reindex(_report_dates).fillna(0)" not in src  # old silent-fill fallback removed
    assert "excess_unavailable_reason" in src              # partial benchmark -> reason, not fabrication
    assert 'D.features(["000300.SH"], ["$close/Ref($close,1)-1"]' in src  # bin-sourced
