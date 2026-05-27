import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "backend" / "scripts" / "run_correlation_compute_wsl.py"
SERVICE = ROOT / "backend" / "services" / "quantevolver" / "correlation_compute_service.py"


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


def test_local_correlation_compute_path_is_service_owned_and_db_safe(monkeypatch, tmp_path) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc
    from backend.services.quantevolver.correlation_engine import CorrelationResult

    (tmp_path / "_meta.json").write_text(
        json.dumps(
            {
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

        def clear_snapshot(self):
            raise AssertionError("clear_snapshot should not run without data_date")

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

    import backend.services.quantevolver.factor_value_pipeline as pipeline_mod

    monkeypatch.setattr(pipeline_mod, "FactorValuePipeline", FakePipeline)
    monkeypatch.setattr(svc, "get_conn", lambda: FakeConn())
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
