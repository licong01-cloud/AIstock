import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from backend.data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "backend" / "scripts" / "run_correlation_compute_wsl.py"
SERVICE = ROOT / "backend" / "services" / "quantevolver" / "correlation_compute_service.py"


class _FakeUniverseMaskService:
    def metadata(self, **_kwargs):
        return {
            "universe_key": "shsz_st_pit_active_v1",
            "universe_rule_version": "test",
            "universe_fingerprint_sha256": "fp-test",
            "index_policy": "st_pit_buy_eligible_reindexed_v1",
        }


def test_correlation_wsl_runner_does_not_import_qe_router_or_stub() -> None:
    source = RUNNER.read_text(encoding="utf-8")

    assert "backend.routers.quantevolver_evolution" not in source
    assert "qe_evolution_service" not in source
    assert "correlation_compute_service" in source


def test_correlation_compute_service_has_no_qe_evolution_dependency() -> None:
    source = SERVICE.read_text(encoding="utf-8")

    assert "qe_evolution_service" not in source
    assert "AutoEvolutionScheduler" not in source
    assert "APIRouter" not in source


def test_correlation_wsl_runner_no_args_returns_structured_usage() -> None:
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"

    completed = subprocess.run(
        [sys.executable, str(RUNNER)],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=30,
        env=env,
    )

    assert completed.returncode == 1
    assert "ImportError" not in completed.stderr
    payload = json.loads(completed.stdout.strip())
    assert payload["type"] == "result"
    assert payload["data"]["success"] is False
    assert "usage:" in payload["data"]["error"]



def test_correlation_factor_cache_uses_offline_backtest_dir() -> None:
    from backend.services.quantevolver import correlation_compute_service as svc
    from backend.services.quantevolver.factor_value_loader import _DEFAULT_PIPELINE_DIR

    cache_dir = svc.get_correlation_factor_cache_dir()

    assert cache_dir.name == "factor_values"
    assert str(cache_dir).replace("\\", "/").endswith("rdagent_assets/factor_values")
    assert str(cache_dir) == os.path.normpath(_DEFAULT_PIPELINE_DIR)


def test_correlation_result_classifies_no_valid_pair_factors() -> None:
    from backend.services.quantevolver.correlation_engine import CorrelationResult

    result = CorrelationResult(
        matrix=np.array(
            [
                [1.0, np.nan, 0.42],
                [np.nan, 1.0, np.nan],
                [0.42, np.nan, 1.0],
            ],
            dtype=float,
        ),
        factor_names=["factor_a", "quality_structure_composite", "factor_b"],
        as_of_date="2026-04-30",
        effective_window=252,
        computation_time_sec=0.01,
    )

    assert result.to_db_records(threshold=0) == [
        {
            "factor_a": "factor_a",
            "factor_b": "factor_b",
            "correlation": 0.42,
            "method": "spearman_ewma",
            "data_period": "252d_as_of_2026-04-30",
        }
    ]
    assert result.get_no_valid_pair_factors() == ["quality_structure_composite"]


def test_correlation_cache_status_reports_offline_orphan_parquets(monkeypatch, tmp_path) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc

    cache_root = tmp_path / "factor_values"
    single = cache_root / "single"
    single.mkdir(parents=True)
    (cache_root / "_meta.json").write_text(
        json.dumps(
            {
                "factors": {
                    "factor_a": {
                        "date_range": "2018-08-01~2026-04-30",
                        "as_of_date": "2026-04-30",
                        "data_source_mode": "backtest_factor_data_dir",
                        "window_train_start": "2018-08-01",
                        "window_backtest_end": "2026-04-30",
                    }
                },
                "data_freshness_profile": "qe_backtest_coverage",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (single / "factor_a.parquet").write_bytes(b"PAR1")
    (single / "factor_orphan.parquet").write_bytes(b"PAR1")

    class FakePipeline:
        def get_cached_singles(self):
            return [
                {"factor_name": "factor_a", "size_mb": 1.25},
                {"factor_name": "factor_orphan", "size_mb": 2.0},
            ]

        def validate_meta_integrity(self):
            return {"ok": False, "orphan_parquets": ["factor_orphan"], "factor_count": 1}

        def get_computable_factors(self):
            return [{"factor_name": "factor_a"}]

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(svc, "CORRELATION_FACTOR_VALUE_CACHE_DIR", cache_root)
    monkeypatch.setattr(svc, "get_correlation_factor_value_pipeline", lambda: FakePipeline())

    status = svc.get_correlation_factor_cache_status()

    assert status["cache_source"] == "offline_research_backtest_factor_values"
    assert status["cache_root"].endswith("factor_values")
    assert status["cache_root"].endswith("factor_values")
    assert status["cached_count"] == 2
    assert status["disk_factor_count"] == 2
    assert status["meta_factor_count"] == 1
    assert status["orphan_parquet_count"] == 1
    assert status["data_source_mode"] == "backtest_factor_data_dir"
    assert status["window_train_start"] == "2018-08-01"
    assert status["window_backtest_end"] == "2026-04-30"


def test_correlation_infers_missing_meta_from_offline_parquet(monkeypatch, tmp_path) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc
    from backend.services.quantevolver.correlation_engine import CorrelationResult

    cache_root = tmp_path / "factor_values"
    single = cache_root / "single"
    single.mkdir(parents=True)
    (cache_root / "_meta.json").write_text(
        json.dumps(
            {
                "moneyflow_unit_contract_version": MONEYFLOW_UNIT_CONTRACT_VERSION,
                "factors": {
                    "factor_a": {"as_of_date": "2026-04-10"}
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp("2026-04-09"), pd.Timestamp("2026-04-10")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    pd.DataFrame({"value": [1.0, 2.0]}, index=idx).to_parquet(single / "factor_b.parquet")

    class FakePipeline:
        _output_dir = str(cache_root)

        def validate_meta_integrity(self):
            return {
                "ok": False,
                "factor_count": 1,
                "top_level_as_of_date": "2026-04-10",
                "orphan_parquets": ["factor_b"],
            }

        def get_cached_singles(self):
            return [{"factor_name": "factor_a"}, {"factor_name": "factor_b"}]

    class FakeCursor:
        rowcount = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            return None

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    class FakeCorrelationEngine:
        def __init__(self, loader):
            self.loader = loader

        def compute_full_matrix(self, factor_names, **kwargs):
            assert factor_names == ["factor_a", "factor_b"]
            assert kwargs["expected_as_of_date"] == "2026-04-10"
            assert str(self.loader._pipeline_dir).endswith("factor_values")
            assert str(self.loader._pipeline_dir).replace("\\", "/").endswith("factor_values")
            return CorrelationResult(
                matrix=np.array([[1.0, 0.1], [0.1, 1.0]], dtype=float),
                factor_names=list(factor_names),
                as_of_date="2026-04-10",
                effective_window=252,
                computation_time_sec=0.01,
                metadata={"num_high_corr_07": 0, "avg_correlation": 0.1, "hdf5_path": str(tmp_path / "corr.h5")},
            )

    monkeypatch.setattr(svc, "CORRELATION_FACTOR_VALUE_CACHE_DIR", cache_root)
    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(svc, "get_correlation_factor_value_pipeline", lambda: FakePipeline())
    monkeypatch.setattr(svc, "get_conn", lambda: FakeConn())
    monkeypatch.setattr(svc, "FactorUniverseMaskService", lambda: _FakeUniverseMaskService())
    monkeypatch.setattr(svc, "_reconcile_correlation_state", lambda reset_all=False: {
        "eligible_factors": 2,
        "deleted_pairs": 0,
        "reset_ineligible_catalog": 0,
        "reset_orphan_catalog": 0,
        "reset_all_catalog": 0,
    })
    monkeypatch.setattr(svc, "_update_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(svc, "_persist_correlations_batch", lambda records, **kwargs: len(records))
    monkeypatch.setattr(svc, "_persist_correlation_metadata", lambda result: None)
    monkeypatch.setattr(svc, "CorrelationEngine", FakeCorrelationEngine)
    monkeypatch.setattr(svc.FactorValueLoader, "invalidate_single_cache", lambda factor_name=None: None)
    monkeypatch.setattr(svc.FactorValueLoader, "invalidate_merged_cache", lambda pipeline_dir=None: None)
    monkeypatch.setattr("glob.glob", lambda pattern: [])

    result = svc.run_correlation_compute_local(["factor_a", "factor_b"], data_date="20260410")

    assert result["success"] is True
    assert result["cache_source"] == "offline_research_backtest_factor_values"
    assert result["cache_root"].endswith("factor_values")


def test_local_correlation_compute_path_is_service_owned_and_db_safe(monkeypatch, tmp_path) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc
    from backend.services.quantevolver.correlation_engine import CorrelationResult

    (tmp_path / "_meta.json").write_text(
        json.dumps(
            {
                "moneyflow_unit_contract_version": MONEYFLOW_UNIT_CONTRACT_VERSION,
                "factors": {
                    "factor_a": {"as_of_date": "2026-04-10"},
                    "factor_b": {"as_of_date": "2026-04-10"},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    class FakePipeline:
        _output_dir = str(tmp_path)

        def validate_meta_integrity(self):
            return {
                "ok": True,
                "factor_count": 2,
                "top_level_as_of_date": "2026-04-10",
            }

        def get_cached_singles(self):
            return [{"factor_name": "factor_a"}, {"factor_name": "factor_b"}]

    class FakeCursor:
        rowcount = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            return None

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    class FakeCorrelationEngine:
        def __init__(self, loader):
            self.loader = loader

        def compute_full_matrix(self, factor_names, **kwargs):
            assert factor_names == ["factor_a", "factor_b"]
            assert kwargs["expected_as_of_date"] == "2026-04-10"
            return CorrelationResult(
                matrix=np.array([[1.0, 0.42], [0.42, 1.0]], dtype=float),
                factor_names=list(factor_names),
                as_of_date="2026-04-10",
                effective_window=252,
                computation_time_sec=0.01,
                metadata={
                    "num_high_corr_07": 0,
                    "avg_correlation": 0.42,
                    "hdf5_path": str(tmp_path / "corr_20260410.h5"),
                },
            )

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(svc, "get_correlation_factor_value_pipeline", lambda: FakePipeline())
    monkeypatch.setattr(svc, "CORRELATION_FACTOR_VALUE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(svc, "get_conn", lambda: FakeConn())
    monkeypatch.setattr(svc, "FactorUniverseMaskService", lambda: _FakeUniverseMaskService())
    monkeypatch.setattr(svc, "_reconcile_correlation_state", lambda reset_all=False: {
        "eligible_factors": 2,
        "deleted_pairs": 0,
        "reset_ineligible_catalog": 0,
        "reset_orphan_catalog": 0,
        "reset_all_catalog": 0,
    })
    monkeypatch.setattr(svc, "_update_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(svc, "_persist_correlations_batch", lambda records, **kwargs: len(records))
    monkeypatch.setattr(svc, "_persist_correlation_metadata", lambda result: None)
    monkeypatch.setattr(svc, "CorrelationEngine", FakeCorrelationEngine)
    monkeypatch.setattr(svc.FactorValueLoader, "invalidate_single_cache", lambda factor_name=None: None)
    monkeypatch.setattr(svc.FactorValueLoader, "invalidate_merged_cache", lambda pipeline_dir=None: None)
    monkeypatch.setattr("glob.glob", lambda pattern: [])

    result = svc.run_correlation_compute_local(["factor_a", "factor_b"])

    assert result["success"] is True
    assert result["requested_factor_count"] == 2
    assert result["success_factor_count"] == 2
    assert result["record_count"] == 1
    assert result["as_of_date"] is None
    assert result["cache_source"] == "offline_research_backtest_factor_values"
    assert result["cache_root"] == str(tmp_path)



def test_local_correlation_compute_classifies_matrix_factor_with_no_valid_pairs(monkeypatch, tmp_path) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc
    from backend.services.quantevolver.correlation_engine import CorrelationResult

    factors = ["factor_a", "quality_structure_composite", "factor_b"]
    (tmp_path / "_meta.json").write_text(
        json.dumps(
            {
                "moneyflow_unit_contract_version": MONEYFLOW_UNIT_CONTRACT_VERSION,
                "factors": {
                    name: {"as_of_date": "2026-04-30"}
                    for name in factors
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    class FakePipeline:
        _output_dir = str(tmp_path)

        def validate_meta_integrity(self):
            return {
                "ok": True,
                "factor_count": 3,
                "top_level_as_of_date": "2026-04-30",
            }

        def get_cached_singles(self):
            return [{"factor_name": name} for name in factors]

    class FakeCursor:
        rowcount = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            return None

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    class FakeCorrelationEngine:
        def __init__(self, loader):
            self.loader = loader

        def compute_full_matrix(self, factor_names, **kwargs):
            assert factor_names == factors
            assert kwargs["expected_as_of_date"] == "2026-04-30"
            return CorrelationResult(
                matrix=np.array(
                    [
                        [1.0, np.nan, 0.42],
                        [np.nan, 1.0, np.nan],
                        [0.42, np.nan, 1.0],
                    ],
                    dtype=float,
                ),
                factor_names=list(factor_names),
                as_of_date="2026-04-30",
                effective_window=252,
                computation_time_sec=0.01,
                metadata={
                    "num_high_corr_07": 0,
                    "avg_correlation": 0.42,
                    "hdf5_path": str(tmp_path / "corr_20260430.h5"),
                },
            )

    persisted_records = []
    persisted_metadata = []

    def fake_persist_records(records, **_kwargs):
        persisted_records.extend(records)
        return len(records)

    def fake_persist_metadata(result):
        persisted_metadata.append(dict(result.metadata))

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(svc, "get_correlation_factor_value_pipeline", lambda: FakePipeline())
    monkeypatch.setattr(svc, "CORRELATION_FACTOR_VALUE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(svc, "get_conn", lambda: FakeConn())
    monkeypatch.setattr(svc, "FactorUniverseMaskService", lambda: _FakeUniverseMaskService())
    monkeypatch.setattr(svc, "_reconcile_correlation_state", lambda reset_all=False: {
        "eligible_factors": 3,
        "deleted_pairs": 0,
        "reset_ineligible_catalog": 0,
        "reset_orphan_catalog": 0,
        "reset_all_catalog": 0,
    })
    monkeypatch.setattr(svc, "_update_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(svc, "_persist_correlations_batch", fake_persist_records)
    monkeypatch.setattr(svc, "_persist_correlation_metadata", fake_persist_metadata)
    monkeypatch.setattr(svc, "CorrelationEngine", FakeCorrelationEngine)
    monkeypatch.setattr(svc.FactorValueLoader, "invalidate_single_cache", lambda factor_name=None: None)
    monkeypatch.setattr(svc.FactorValueLoader, "invalidate_merged_cache", lambda pipeline_dir=None: None)
    monkeypatch.setattr("glob.glob", lambda pattern: [])

    result = svc.run_correlation_compute_local(factors)

    assert result["success"] is True
    assert result["requested_factor_count"] == 3
    assert result["success_factor_count"] == 2
    assert result["failed_factor_count"] == 1
    assert result["record_count"] == 1
    assert result["success_factors"] == ["factor_a", "factor_b"]
    assert result["excluded_factors"]["missing_from_cache"] == []
    assert result["excluded_factors"]["degenerate_nan"] == []
    assert result["excluded_factors"]["no_valid_pairs"] == ["quality_structure_composite"]
    assert result["runtime_validation"]["excluded_summary"] == {
        "missing_from_cache": 0,
        "degenerate_nan": 0,
        "no_valid_pairs": 1,
    }
    assert result["runtime_validation"]["checks"]["excluded_factors_classified"] is True
    assert persisted_records == [
        {
            "factor_a": "factor_a",
            "factor_b": "factor_b",
            "correlation": 0.42,
            "method": "spearman_ewma",
            "data_period": "252d_as_of_2026-04-30",
        }
    ]
    assert persisted_metadata[0]["num_pair_valid_factors"] == 2
    assert persisted_metadata[0]["no_valid_pair_factors"] == ["quality_structure_composite"]
