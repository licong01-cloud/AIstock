from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.quantevolver.factor_cache_coverage import factor_cache_covers_window
from backend.services.quantevolver.factor_universe_mask_service import FactorUniverseMaskService
from backend.services.quantevolver import qe_eval_v2_metric_engine as engine


def test_factor_universe_mask_service_builds_mask_from_spans(monkeypatch: pytest.MonkeyPatch) -> None:
    service = FactorUniverseMaskService()

    def fake_load_spans(**_kwargs):
        return [
            {"ts_code": "000001.SZ", "eligible_start": "2026-01-02", "eligible_end": "2026-01-05"},
            {"ts_code": "000002.SZ", "eligible_start": "2026-01-04", "eligible_end": "2026-01-06"},
        ]

    monkeypatch.setattr(service, "load_spans", fake_load_spans)
    dates = pd.date_range("2026-01-01", periods=6, freq="D")
    mask = service.build_eligible_mask(
        dates,
        ["000001.SZ", "000002.SZ", "000003.SZ"],
        start_date="2026-01-01",
        end_date="2026-01-06",
        ensure=False,
    )

    assert mask.shape == (6, 3)
    assert mask[:, 0].tolist() == [False, True, True, True, True, False]
    assert mask[:, 1].tolist() == [False, False, False, True, True, True]
    assert not mask[:, 2].any()


def test_pit_coverage_denominator_uses_st_pit_eligible_mask() -> None:
    f_arr = np.array([[1.0, 2.0], [3.0, 4.0], [np.nan, 6.0]])
    market_valid = np.ones_like(f_arr, dtype=bool)
    suspended = np.zeros_like(f_arr, dtype=bool)
    non_warmup = np.ones_like(f_arr, dtype=bool)
    eligible = np.array([[True, False], [True, True], [False, True]])
    suspended[1, 1] = True

    stats = engine._pit_coverage_stats_from_masks(
        f_arr, market_valid, suspended, non_warmup, eligible
    )

    assert stats["coverage_denominator"] == 3
    assert stats["coverage_numerator"] == 3
    assert stats["suspended_excluded_count"] == 1
    assert stats["st_pit_excluded_count"] == 2
    assert engine._pit_coverage_from_masks(
        f_arr, market_valid, suspended, non_warmup, eligible
    ) == pytest.approx(1.0)


def test_metric_impl_records_st_pit_universe_metadata_and_counts() -> None:
    dates = pd.date_range("2026-01-01", periods=8, freq="D")
    instruments = ["000001.SZ", "000002.SZ", "000003.SZ"]
    close = pd.DataFrame(10.0, index=dates, columns=instruments)
    f_arr = np.arange(24, dtype=float).reshape(8, 3)
    fwd = np.ones_like(f_arr) * 0.01
    suspended = np.zeros_like(f_arr, dtype=bool)
    suspended[2, 1] = True
    eligible = np.ones_like(f_arr, dtype=bool)
    eligible[0, 2] = False
    eligible[1, 2] = False

    metrics, reports = engine._compute_factor_metrics_impl(
        fname="demo_factor",
        f_arr_full=f_arr,
        dates=dates,
        fwd_arr=fwd,
        fwd_arrs={"1d": fwd, "5d": fwd, "10d": fwd, "20d": fwd},
        close_unstacked=close,
        data_start="2026-01-01",
        data_end="2026-01-08",
        calc_batch_id="batch",
        suspended_mask=suspended,
        eligible_mask=eligible,
        universe_metadata={
            "universe_key": "shsz_st_pit_active_v1",
            "universe_rule_version": "rule_v1",
            "universe_fingerprint_sha256": "abc",
            "index_policy": "st_pit_buy_eligible_reindexed_v1",
            "coverage_semantics": "st_pit_buy_eligible_suspend_excluded_non_warmup_v1",
        },
    )

    full = next(row for row in metrics if row["eval_window"] == "full")
    assert full["universe"] == "shsz_st_pit_active_v1"
    assert full["universe_rule_version"] == "rule_v1"
    assert full["universe_fingerprint_sha256"] == "abc"
    assert full["coverage_denominator"] == 21
    assert full["coverage_numerator"] == 21
    assert full["suspended_excluded_count"] == 1
    assert full["st_pit_excluded_count"] == 2
    assert any(r["status"] == "ok" for r in reports)


def test_factor_cache_coverage_rejects_universe_mismatch() -> None:
    covered, reason = factor_cache_covers_window(
        cache_start_date="2026-01-01",
        cache_end_date="2026-01-31",
        target_start="2026-01-01",
        target_end="2026-01-31",
        entry={
            "universe_key": "old",
            "universe_fingerprint_sha256": "oldfp",
            "index_policy": "old_policy",
        },
        expected_universe_key="shsz_st_pit_active_v1",
        expected_universe_fingerprint_sha256="fp",
        expected_index_policy="st_pit_buy_eligible_reindexed_v1",
    )

    assert not covered
    assert reason == "universe_mismatch"


def test_factor_value_loader_rejects_merged_cache_universe_mismatch(tmp_path) -> None:
    from backend.services.quantevolver.factor_value_loader import FactorValueLoader

    single = tmp_path / "single"
    single.mkdir()
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp("2026-01-01")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    merged = single / "_merged_panel.parquet"
    pd.DataFrame({"f1": [1.0]}, index=idx).to_parquet(merged)
    (single / "_merged_panel.parquet.meta.json").write_text(
        '{"as_of_date":"2026-01-31","universe_key":"old","factor_names":["f1"],"date_range":"2026-01-01~2026-01-01"}',
        encoding="utf-8",
    )

    loader = FactorValueLoader(source="single", pipeline_dir=str(tmp_path))
    panel = loader._try_read_merged_cache(
        str(merged),
        ["f1"],
        "2026-01-01",
        "2026-01-01",
        expected_as_of_date="2026-01-31",
        expected_universe_key="shsz_st_pit_active_v1",
    )

    assert panel is None
    assert not merged.exists()
