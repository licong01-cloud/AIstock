from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from backend.services.quantevolver.factor_value_loader import FactorValueLoader
from backend.services.quantevolver.official_factor_batch_compute_service import BatchComputeConfig
from backend.services.quantevolver.official_factor_batch_compute_service import RESOURCE_GATE_FAILED
from backend.services.quantevolver.official_factor_batch_compute_service import OfficialFactorBatchComputeService
from backend.services.quantevolver.correlation_compute_service import _build_correlation_runtime_validation


def test_official_factor_runtime_validation_reports_smoke_gate() -> None:
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    cfg = BatchComputeConfig(
        factor_names=["factor_a", "factor_b"],
        factor_data_dir="/mnt/f/factor_data",
        start_date="2018-08-01",
        end_date="2026-04-30",
    )

    report = service._build_runtime_validation_report(
        cfg=cfg,
        task_id="task-smoke",
        requested=["factor_a", "factor_b"],
        eligible_names=["factor_a", "factor_b"],
        skipped=[],
        results=[{"name": "factor_a", "success": True}, {"name": "factor_b", "success": True}],
        success_count=2,
        fail_count=0,
        db_result={
            "inserted": 4,
            "skipped": 0,
            "errors": [],
            "save_failures": [],
            "metric_precomputed": 2,
            "metric_parent_computed": 0,
            "metric_precompute_failures": [],
        },
        metrics_error=None,
        batch_count=1,
        memory_samples=[
            {"event": "batch_started", "requested_workers": 4, "effective_workers": 2, "rss_mb": 100.0},
            {"event": "batch_released", "single_cache_entries": 0, "rss_mb": 100.0, "swap_mb": 0.0},
        ],
        resource_failures=[],
        universe_meta={"universe_key": "shsz_st_pit_active_v1", "index_policy": "st_pit_buy_eligible_reindexed_v1"},
        start_date="2018-08-01",
        end_date="2026-04-30",
    )

    assert report["schema_version"] == "official_factor_runtime_validation_v1"
    assert report["mode"] == "smoke_2"
    assert report["gate_status"] == "passed"
    assert report["checks"]["single_cache_released"] is True
    assert report["checks"]["timeout_gate_available"] is True
    assert report["checks"]["resource_gate_ok"] is True
    assert report["timeout_per_factor_sec"] == 1800
    assert report["optimization_profile"]["requested_worker_values"] == [4]
    assert report["optimization_profile"]["effective_worker_values"] == [2]
    assert report["optimization_profile"]["metric_precomputed"] == 2
    assert report["optimization_profile"]["metric_parent_computed"] == 0
    assert report["next_gates"]["correlation_full"] == "run_correlation_compute_wsl_against_same_official_cache"


def test_official_factor_runtime_validation_classifies_failures() -> None:
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    cfg = BatchComputeConfig(
        factor_names=["factor_a", "factor_bad"],
        factor_data_dir="/mnt/f/factor_data",
        start_date="2018-08-01",
        end_date="2026-04-30",
    )

    report = service._build_runtime_validation_report(
        cfg=cfg,
        task_id="task-failed",
        requested=["factor_a", "factor_bad"],
        eligible_names=["factor_a", "factor_bad"],
        skipped=[],
        results=[
            {"name": "factor_a", "success": True},
            {"name": "factor_bad", "success": False, "error_type": "schema_invalid", "error": "bad index"},
        ],
        success_count=1,
        fail_count=1,
        db_result={"inserted": 2, "skipped": 0, "errors": [], "save_failures": []},
        metrics_error=None,
        batch_count=1,
        memory_samples=[{"event": "batch_released", "single_cache_entries": 0, "rss_mb": 100.0}],
        resource_failures=[],
        universe_meta={"universe_key": "shsz_st_pit_active_v1", "index_policy": "st_pit_buy_eligible_reindexed_v1"},
        start_date="2018-08-01",
        end_date="2026-04-30",
    )

    assert report["gate_status"] == "failed"
    assert report["failure_summary"] == {"schema_invalid": 1}
    assert report["failed_factors"][0]["name"] == "factor_bad"


def test_official_factor_runtime_validation_reports_resource_gate_failure() -> None:
    service = OfficialFactorBatchComputeService.__new__(OfficialFactorBatchComputeService)
    cfg = BatchComputeConfig(
        factor_names=["factor_a"],
        factor_data_dir="/mnt/f/factor_data",
        start_date="2018-08-01",
        end_date="2026-04-30",
        timeout_per_factor=60,
    )
    resource_failure = {
        "phase": "during_batch",
        "reason": "swap_growth_hard_stop_exceeded",
        "rss_mb": 1024.0,
        "swap_growth_mb": 1200.0,
    }

    report = service._build_runtime_validation_report(
        cfg=cfg,
        task_id="task-resource-failed",
        requested=["factor_a"],
        eligible_names=["factor_a"],
        skipped=[],
        results=[
            {
                "name": "factor_a",
                "success": False,
                "error_type": RESOURCE_GATE_FAILED,
                "error": "memory_gate_failed: swap_growth_hard_stop_exceeded",
            }
        ],
        success_count=0,
        fail_count=1,
        db_result={"inserted": 0, "skipped": 0, "errors": [], "save_failures": []},
        metrics_error=None,
        batch_count=1,
        memory_samples=[{"event": "batch_released", "single_cache_entries": 0, "rss_mb": 1024.0, "swap_mb": 1200.0}],
        resource_failures=[resource_failure],
        universe_meta={"universe_key": "shsz_st_pit_active_v1", "index_policy": "st_pit_buy_eligible_reindexed_v1"},
        start_date="2018-08-01",
        end_date="2026-04-30",
    )

    assert report["gate_status"] == "failed"
    assert report["checks"]["resource_gate_ok"] is False
    assert report["failure_summary"] == {RESOURCE_GATE_FAILED: 1}
    assert report["resource_failures"] == [resource_failure]
    assert report["timeout_per_factor_sec"] == 60


def test_factor_value_loader_validates_qe_subwindow_official_cache_hit(tmp_path: Path) -> None:
    cache_root = tmp_path / "factor_values"
    single_dir = cache_root / "single"
    single_dir.mkdir(parents=True)
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp("2018-08-01"), pd.Timestamp("2026-04-30")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    pd.DataFrame({"value": [1.0, 2.0]}, index=idx).to_parquet(single_dir / "factor_a.parquet")
    (cache_root / "_meta.json").write_text(
        json.dumps(
            {
                "source_system": "official_offline_backtest_factor_data",
                "as_of_date": "2026-04-30",
                "universe_key": "shsz_st_pit_active_v1",
                "index_policy": "st_pit_buy_eligible_reindexed_v1",
                "factors": {
                    "factor_a": {
                        "as_of_date": "2026-04-30",
                        "date_range": "2018-08-01~2026-04-30",
                        "universe_key": "shsz_st_pit_active_v1",
                        "index_policy": "st_pit_buy_eligible_reindexed_v1",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    loader = FactorValueLoader(source="single", pipeline_dir=str(cache_root))
    result = loader.validate_official_cache_window_hit(
        ["factor_a"],
        "2020-01-01",
        "2021-01-01",
        expected_as_of_date="2026-04-30",
        expected_universe_key="shsz_st_pit_active_v1",
        expected_index_policy="st_pit_buy_eligible_reindexed_v1",
    )

    assert result["gate_status"] == "passed"
    assert result["official_cache_hit"] is True
    assert result["hit_factors"] == ["factor_a"]
    assert result["cache_root"].endswith("factor_values")


def test_factor_value_loader_reports_qe_cache_miss_reasons(tmp_path: Path) -> None:
    cache_root = tmp_path / "factor_values"
    (cache_root / "single").mkdir(parents=True)
    (cache_root / "_meta.json").write_text(json.dumps({"factors": {}}), encoding="utf-8")

    loader = FactorValueLoader(source="single", pipeline_dir=str(cache_root))
    result = loader.validate_official_cache_window_hit(["factor_missing"], "2020-01-01", "2021-01-01")

    assert result["gate_status"] == "failed"
    assert result["official_cache_hit"] is False
    assert result["miss_reasons"]["missing_from_cache"] == ["factor_missing"]


def test_correlation_runtime_validation_classifies_exclusions(tmp_path: Path) -> None:
    report = _build_correlation_runtime_validation(
        requested_count=3,
        success_count=2,
        failed_count=1,
        missing_factors=["factor_missing"],
        degenerate_factors=[],
        record_count=1,
        as_of_date="2026-04-30",
        cache_root=tmp_path / "factor_values",
        integrity={"ok": True, "factor_count": 2},
        universe_metadata={"universe_key": "shsz_st_pit_active_v1"},
    )

    assert report["schema_version"] == "official_factor_correlation_runtime_validation_v1"
    assert report["gate_status"] == "passed"
    assert report["excluded_summary"] == {"missing_from_cache": 1, "degenerate_nan": 0}
    assert report["checks"]["official_cache_only"] is True


def test_factor_value_loader_classifies_hash_mismatch(tmp_path: Path) -> None:
    cache_root = tmp_path / "factor_values"
    single_dir = cache_root / "single"
    single_dir.mkdir(parents=True)
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp("2018-08-01"), pd.Timestamp("2026-04-30")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    pd.DataFrame({"value": [1.0, 2.0]}, index=idx).to_parquet(single_dir / "factor_a.parquet")
    (cache_root / "_meta.json").write_text(
        json.dumps(
            {
                "source_system": "official_offline_backtest_factor_data",
                "as_of_date": "2026-04-30",
                "factors": {
                    "factor_a": {
                        "source_hash_raw": "old-hash",
                        "as_of_date": "2026-04-30",
                        "date_range": "2018-08-01~2026-04-30",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    loader = FactorValueLoader(source="single", pipeline_dir=str(cache_root))
    result = loader.validate_official_cache_window_hit(
        ["factor_a"],
        "2020-01-01",
        "2021-01-01",
        expected_code_hashes={"factor_a": "new-hash"},
    )

    assert result["gate_status"] == "failed"
    assert result["miss_reasons"]["hash_mismatch"] == ["factor_a"]
