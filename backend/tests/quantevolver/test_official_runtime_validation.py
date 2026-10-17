from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION
from backend.services.quantevolver.factor_value_loader import FactorValueLoader
from backend.services.quantevolver.official_factor_batch_compute_service import BatchComputeConfig
from backend.services.quantevolver.official_factor_batch_compute_service import RESOURCE_GATE_FAILED
from backend.services.quantevolver.official_factor_batch_compute_service import OfficialFactorBatchComputeService
from backend.services.quantevolver.correlation_compute_service import _build_correlation_runtime_validation


@pytest.mark.parametrize("code,source,category", [
    ('frame["md_rzche"] - frame["md_rzmre"]', "money_flow", "MF"),
    ('$md_rqye / $md_rzrqye + $md_rqyl', "money_flow", "MF"),
    ('raw.md_rqmcl + raw.md_rqchl', "money_flow", "MF"),
    ('$md_rzye + $db_pe', "cross_dataset", "MF"),
    ('$md_unknown + $md_rzye_fake', "unknown", None),
])
def test_financing_margin_fields_use_shared_classification(code, source, category):
    from backend.services.quantevolver import factor_analyst as module
    assert module.classify_data_source(code) == source
    actual, reason = module._classify_by_rules("unclassified_input", code_text=code)
    assert actual == category
    if category == "MF":
        assert ("复合因子" if source == "cross_dataset" else "融资融券") in reason
        description = module._generate_description_by_rules("unclassified_input", category, code_text=code)
        assert "融资融券" in description and "主力资金" not in description


def test_financing_rule_only_analysis_uses_existing_classification_writer(monkeypatch):
    from types import SimpleNamespace
    from backend.services.quantevolver import factor_analyst as module
    root = Path(__file__).resolve().parents[3]
    code = (root / "backend/services/quantevolver/net_repayment_tminus1_factor.py").read_text(encoding="utf-8")
    monkeypatch.setattr(module, "_get_official_grade", lambda _: None)
    monkeypatch.setattr(module, "_official_factor_value_loader", lambda: SimpleNamespace(load_single_factor=lambda _: None))
    monkeypatch.setattr(module.FactorAnalyst, "_get_factor_info", lambda *_: {"code_text": code})
    monkeypatch.setattr(module.FactorAnalyst, "_get_independent_metrics", lambda *_: {})
    monkeypatch.setattr(module.FactorAnalyst, "_get_multi_window_metrics", lambda *_: {})
    writes = []
    monkeypatch.setattr(module.FactorAnalyst, "_upsert_classification", lambda _self, **kwargs: writes.append(kwargs))
    result = module.FactorAnalyst().analyze_single_factor("neutral_test_name", "manual", use_llm=False)
    assert result["ok"] and result["category"] == "MF"
    assert len(writes) == 1 and writes[0]["data_source_group"] == "money_flow"
    assert writes[0]["factor_name"] == "neutral_test_name"


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
        resource_actions=[],
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
    assert report["resource_actions"] == []
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
        resource_actions=[],
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
    resource_action = {
        "action": "cancel_pending",
        "reason": "swap_growth_hard_stop_exceeded",
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
        resource_actions=[resource_action],
        universe_meta={"universe_key": "shsz_st_pit_active_v1", "index_policy": "st_pit_buy_eligible_reindexed_v1"},
        start_date="2018-08-01",
        end_date="2026-04-30",
    )

    assert report["gate_status"] == "failed"
    assert report["checks"]["resource_gate_ok"] is False
    assert report["failure_summary"] == {RESOURCE_GATE_FAILED: 1}
    assert report["resource_failures"] == [resource_failure]
    assert report["resource_actions"] == [resource_action]
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
                "moneyflow_unit_contract_version": MONEYFLOW_UNIT_CONTRACT_VERSION,
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


@pytest.mark.parametrize("entry,helper_first,guard", [
    ("compute_factor", True, True), ("main", False, True), ("compute_factor", False, False),
    ("wrapper", True, True),
    ("qlib", True, True),
])
def test_live_transform_preserves_helpers_and_entry_result(entry, helper_first, guard, monkeypatch):
    from backend.services.quantevolver.factor_code_transformer import (
        FactorCodeTransformer, NON_OFFICIAL_LIVE_TRANSFORMATION_CONTEXT,
    )

    helper = "def helper(frame):\n    return frame * SCALE\n"
    implementation = "compute_factor" if entry in {"wrapper", "qlib"} else entry
    body = f'''def {implementation}():
    df = pd.read_hdf('daily_pv.h5', key='data')
    result = helper(df[['close']])
    result.to_hdf(
        path_or_buf='result.h5', key='data'
    )
'''
    original = "import pandas as pd\nSCALE = 2\n" + (helper + body if helper_first else body + helper)
    if entry == "wrapper":
        original += "def main():\n    compute_factor()\n"
        entry = "main"
    elif entry == "qlib":
        original = "from qlib.data import D\n" + original
        entry = "compute_factor"
    if guard:
        original += f"if __name__ == '__main__':\n    {entry}()\n"

    def forbid_write(*_args, **_kwargs):
        raise AssertionError("live transformation must not write H5")

    monkeypatch.setattr(pd.DataFrame, "to_hdf", forbid_write)
    transformed = FactorCodeTransformer(NON_OFFICIAL_LIVE_TRANSFORMATION_CONTEXT).transform(original, "probe")
    assert transformed.success, transformed.error
    frame = pd.DataFrame({"close": [1., 2.]})

    class Loader:
        def load(self, **_kwargs):
            return frame.copy()

    namespace = {"pd": pd, "_REALTIME_LOADER": Loader()}
    exec(transformed.transformed_code, namespace)
    actual = namespace["calculate_probe"](["000001.SZ"], "2026-04-09", "2026-04-10")
    pd.testing.assert_frame_equal(actual, frame * 2)


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
