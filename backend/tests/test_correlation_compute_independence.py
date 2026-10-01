import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

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


def test_correlation_wsl_runner_routes_target_only_mode(monkeypatch, tmp_path, capsys) -> None:
    from backend.scripts import run_correlation_compute_wsl as runner
    from backend.services.quantevolver import correlation_compute_service as svc

    payload_path = tmp_path / "payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "mode": "target_only",
                "target_factor_name": "factor_target",
                "as_of_date": "2026-08-31",
            }
        ),
        encoding="utf-8",
    )
    captured = {}
    monkeypatch.setattr(runner, "assert_wsl_runtime", lambda _operation: None)
    monkeypatch.setattr(
        svc,
        "run_target_correlation_refresh_local",
        lambda **kwargs: captured.update(kwargs) or {"success": True},
    )
    monkeypatch.setattr(
        svc,
        "run_correlation_compute_local",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("full reset must not run")),
    )
    monkeypatch.setattr(sys, "argv", [str(RUNNER), str(payload_path)])

    assert runner.main() == 0
    assert captured["target_factor_name"] == "factor_target"
    assert json.loads(capsys.readouterr().out.strip())["data"]["success"] is True


def test_target_correlation_persistence_replaces_only_target_pairs(monkeypatch) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc

    statements = []
    inserted = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            statements.append((" ".join(sql.split()), params))

        def fetchall(self):
            return [(1, 2)]

    class Conn:
        committed = False
        rolled_back = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

    conn = Conn()

    class Eligibility:
        def list_eligible_factors(self, **_kwargs):
            return [
                {"id": 1, "factor_name": "target"},
                {"id": 2, "factor_name": "reference"},
                {"id": 3, "factor_name": "unrelated"},
            ]

    monkeypatch.setattr(svc, "FactorEligibilityService", Eligibility)
    monkeypatch.setattr(svc, "get_conn", lambda: conn)
    monkeypatch.setattr(
        svc,
        "execute_values",
        lambda _cur, _sql, values, **_kwargs: inserted.extend(values),
    )

    written = svc._persist_target_correlations(
        target_factor_name="target",
        records=[
            {
                "factor_a": "target",
                "factor_b": "reference",
                "correlation": 0.25,
                "method": "spearman_ewma",
            }
        ],
        as_of_date="2026-08-31",
        universe_metadata={"universe_key": "aistock_equity_pit_canonical_v2"},
    )

    assert written == 1
    assert conn.committed is True
    assert conn.rolled_back is False
    assert inserted[0][:2] == (1, 2)
    assert statements[0][1] == (1, 1)
    assert all("TRUNCATE" not in sql for sql, _params in statements)
    assert statements[1][1] == ([2],)
    assert statements[-1][1] == (1, 1)


def test_target_correlation_persistence_rolls_back_before_replacing_state(
    monkeypatch,
) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _sql, _params=None):
            return None

        def fetchall(self):
            return [(1, 2)]

    class Conn:
        committed = False
        rolled_back = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

    conn = Conn()

    class Eligibility:
        def list_eligible_factors(self, **_kwargs):
            return [
                {"id": 1, "factor_name": "target"},
                {"id": 2, "factor_name": "reference"},
            ]

    monkeypatch.setattr(svc, "FactorEligibilityService", Eligibility)
    monkeypatch.setattr(svc, "get_conn", lambda: conn)
    monkeypatch.setattr(
        svc,
        "execute_values",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("insert failed")),
    )

    with pytest.raises(RuntimeError, match="insert failed"):
        svc._persist_target_correlations(
            target_factor_name="target",
            records=[
                {
                    "factor_a": "target",
                    "factor_b": "reference",
                    "correlation": 0.25,
                }
            ],
            as_of_date="2026-08-31",
            universe_metadata={},
        )

    assert conn.committed is False
    assert conn.rolled_back is True


def test_target_correlation_route_submits_explicit_profile_without_full_reset(
    monkeypatch,
) -> None:
    from backend.routers import quantevolver_evolution as router

    submitted = {}

    class Lock:
        def locked(self):
            return False

    class Eligibility:
        def get_eligible_factor_names(self, **_kwargs):
            return ["target"]

    class Executor:
        def submit(self, fn, **kwargs):
            submitted["fn"] = fn
            submitted.update(kwargs)
            return object()

    monkeypatch.setattr(router, "_computing_lock", Lock())
    monkeypatch.setattr(router, "FactorEligibilityService", Eligibility)
    monkeypatch.setattr(router, "_compute_executor", Executor())
    monkeypatch.setattr(
        router._correlation_compute_service,
        "get_correlation_factor_cache_status",
        lambda: {"as_of_date": "2026-08-31"},
    )

    result = router.refresh_target_correlations(
        router.CorrelationTargetRefreshRequest(
            target_factor_name="target",
            as_of_date="2026-08-31",
            dataset_profile_path="X:/profiles/r8.json",
        )
    )

    assert result["status"] == "accepted"
    assert result["unrelated_rows_reset"] is False
    assert submitted["target_factor_name"] == "target"
    assert submitted["dataset_profile_path"] == "X:/profiles/r8.json"


def test_target_correlation_refresh_fails_closed_when_reference_cache_is_missing(
    monkeypatch,
) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc

    class Eligibility:
        def list_eligible_factors(self, **_kwargs):
            return [
                {"id": 1, "factor_name": "target"},
                {"id": 2, "factor_name": "missing_reference"},
            ]

    class Pipeline:
        def get_cached_singles(self):
            return [{"factor_name": "target"}]

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda _operation: None)
    monkeypatch.setattr(svc, "FactorEligibilityService", Eligibility)
    monkeypatch.setattr(svc, "get_correlation_factor_value_pipeline", Pipeline)
    monkeypatch.setattr(svc, "_update_job_status", lambda *_args, **_kwargs: None)

    with pytest.raises(ValueError, match="missing from the cache"):
        svc.run_target_correlation_refresh_local(target_factor_name="target")


def test_correlation_factor_cache_uses_offline_backtest_dir() -> None:
    from backend.services.quantevolver import correlation_compute_service as svc
    from backend.services.quantevolver.factor_value_loader import _DEFAULT_PIPELINE_DIR

    cache_dir = svc.get_correlation_factor_cache_dir()

    assert cache_dir.name == "factor_values"
    assert str(cache_dir).replace("\\", "/").endswith("rdagent_assets/factor_values")
    assert str(cache_dir) == os.path.normpath(_DEFAULT_PIPELINE_DIR)


def test_correlation_uses_promoted_cache_universe_key(monkeypatch, tmp_path) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc

    (tmp_path / "_meta.json").write_text(
        json.dumps({"universe_key": "aistock_equity_pit_canonical_v2"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(svc, "CORRELATION_FACTOR_VALUE_CACHE_DIR", tmp_path)

    assert svc._official_cache_universe_key() == "aistock_equity_pit_canonical_v2"

    (tmp_path / "_meta.json").write_text(
        json.dumps({"universe_key": "shsz_st_pit_active_v1"}),
        encoding="utf-8",
    )
    assert svc._official_cache_universe_key() == svc.OFFICIAL_FACTOR_UNIVERSE_KEY


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


def test_selected_correlation_submatrix_is_rectangular_and_order_invariant() -> None:
    from backend.services.quantevolver.correlation_engine import CorrelationEngine

    dates = pd.bdate_range("2026-01-05", periods=4)
    instruments = ["000001.SZ", "000002.SZ", "000003.SZ"]
    index = pd.MultiIndex.from_product([dates, instruments], names=["datetime", "instrument"])
    base = np.tile(np.asarray([1.0, 2.0, 3.0]), len(dates))
    candidates = pd.DataFrame({"candidate_b": -base, "candidate_a": base}, index=index)
    references = pd.DataFrame({"reference_b": -base, "reference_a": base}, index=index)
    engine = CorrelationEngine(object(), window=4, half_life=2, min_stocks=3, min_days=2)

    first = engine.compute_selected_submatrix(candidates, references, as_of_date="2026-01-08")
    second = engine.compute_selected_submatrix(
        candidates.sample(frac=1.0, random_state=7)[["candidate_a", "candidate_b"]],
        references.sample(frac=1.0, random_state=8)[["reference_a", "reference_b"]],
        as_of_date="2026-01-08",
    )

    assert first.candidate_names == ["candidate_a", "candidate_b"]
    assert first.reference_names == ["reference_a", "reference_b"]
    assert first.matrix.shape == (2, 2)
    assert np.allclose(first.matrix, second.matrix)
    assert np.all(first.effective_days == 4)
    assert first.metadata["computed_pairs"] == 4
    assert first.metadata["reference_reference_pairs_computed"] == 0


def test_selected_correlation_submatrix_reports_insufficient_support_as_null() -> None:
    from backend.services.quantevolver.correlation_engine import CorrelationEngine

    index = pd.MultiIndex.from_product(
        [[pd.Timestamp("2026-01-05")], ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )
    candidate = pd.DataFrame({"candidate": [1.0, 2.0]}, index=index)
    reference = pd.DataFrame({"reference": [2.0, 1.0]}, index=index)
    engine = CorrelationEngine(object(), window=4, half_life=2, min_stocks=2, min_days=2)

    result = engine.compute_selected_submatrix(candidate, reference, as_of_date="2026-01-05")

    assert np.isnan(result.matrix[0, 0])
    assert result.effective_days[0, 0] == 1
    assert result.records()[0]["correlation"] is None
    assert result.records()[0]["reason"] == "insufficient_effective_days"


def test_selected_daily_submatrix_preserves_pairwise_mask_semantics() -> None:
    from scipy import stats

    from backend.services.quantevolver.correlation_engine import CorrelationEngine

    dates = pd.bdate_range("2026-01-05", periods=3)
    instruments = [f"{code:06d}.SZ" for code in range(1, 7)]
    index = pd.MultiIndex.from_product([dates, instruments], names=["datetime", "instrument"])
    candidates = pd.DataFrame(
        {
            "candidate_a": [
                1,
                2,
                3,
                4,
                5,
                6,
                2,
                3,
                np.nan,
                5,
                6,
                7,
                3,
                4,
                5,
                6,
                np.nan,
                8,
            ],
            "candidate_b": [
                6,
                5,
                4,
                3,
                2,
                1,
                7,
                np.nan,
                5,
                4,
                3,
                2,
                8,
                7,
                6,
                np.nan,
                4,
                3,
            ],
        },
        index=index,
    )
    references = pd.DataFrame(
        {
            "reference_a": [
                1,
                4,
                2,
                6,
                3,
                5,
                2,
                5,
                3,
                np.nan,
                4,
                6,
                3,
                np.nan,
                4,
                8,
                5,
                7,
            ],
            "reference_b": [
                5,
                2,
                6,
                1,
                4,
                3,
                6,
                3,
                np.nan,
                2,
                5,
                4,
                7,
                4,
                8,
                3,
                6,
                np.nan,
            ],
        },
        index=index,
    )
    engine = CorrelationEngine(
        object(),
        window=3,
        half_life=2,
        min_stocks=3,
        min_days=1,
        winsorize_quantile=0.2,
    )

    daily = engine.compute_selected_daily_submatrix(candidates, references, as_of_date="2026-01-07")

    expected = np.full((3, 2, 2), np.nan)
    for day_index, date in enumerate(dates):
        left_day = candidates.loc[date]
        right_day = references.loc[date]
        for i, candidate in enumerate(sorted(candidates.columns)):
            for j, reference in enumerate(sorted(references.columns)):
                left = left_day[candidate].to_numpy(dtype=float)
                right = right_day[reference].to_numpy(dtype=float)
                valid = np.isfinite(left) & np.isfinite(right)
                if int(valid.sum()) < 3:
                    continue
                corr, _ = stats.spearmanr(
                    engine._winsorize_array(left[valid]),
                    engine._winsorize_array(right[valid]),
                )
                if np.isfinite(corr):
                    expected[day_index, i, j] = corr

    assert np.allclose(daily.correlations, expected, equal_nan=True, atol=1e-12)
    assert daily.metadata["mask_groups"] < 3 * 2 * 2


def test_selected_daily_submatrix_reuses_dates_without_changing_window_result() -> None:
    from backend.services.quantevolver.correlation_engine import CorrelationEngine

    dates = pd.bdate_range("2026-01-05", periods=5)
    instruments = [f"{code:06d}.SZ" for code in range(1, 7)]
    index = pd.MultiIndex.from_product([dates, instruments], names=["datetime", "instrument"])
    base = np.tile(np.asarray([1.0, 3.0, 2.0, 6.0, 4.0, 5.0]), len(dates))
    candidates = pd.DataFrame({"candidate": base}, index=index)
    references = pd.DataFrame({"reference": np.roll(base, 1)}, index=index)
    engine = CorrelationEngine(object(), window=5, half_life=2, min_stocks=3, min_days=2)
    daily = engine.compute_selected_daily_submatrix(candidates, references, as_of_date="2026-01-09")
    reused = engine.aggregate_selected_daily_submatrix(
        daily,
        start_date="2026-01-07",
        end_date="2026-01-09",
        as_of_date="2026-01-09",
    )
    sliced = engine.compute_selected_submatrix(
        candidates.loc[(slice(pd.Timestamp("2026-01-07"), None), slice(None)), :],
        references.loc[(slice(pd.Timestamp("2026-01-07"), None), slice(None)), :],
        as_of_date="2026-01-09",
    )

    assert np.allclose(reused.matrix, sliced.matrix, equal_nan=True, atol=1e-12)
    assert np.array_equal(reused.effective_days, sliced.effective_days)
    assert np.allclose(reused.avg_stocks_per_day, sliced.avg_stocks_per_day)


def test_selected_correlation_submatrix_rejects_non_daily_or_invalid_identity() -> None:
    from backend.services.quantevolver.correlation_engine import CorrelationEngine

    engine = CorrelationEngine(object(), window=4, half_life=2, min_stocks=2, min_days=2)
    intraday_index = pd.MultiIndex.from_product(
        [[pd.Timestamp("2026-01-05 09:30:00")], ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )
    valid_index = pd.MultiIndex.from_product(
        [[pd.Timestamp("2026-01-05")], ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )

    with pytest.raises(ValueError, match="timezone-naive daily dates"):
        engine.compute_selected_submatrix(
            pd.DataFrame({"candidate": [1.0, 2.0]}, index=intraday_index),
            pd.DataFrame({"reference": [2.0, 1.0]}, index=valid_index),
            as_of_date="2026-01-05",
        )

    invalid_identity = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-01-05"), ""), (pd.Timestamp("2026-01-05"), "000002.SZ")],
        names=["datetime", "instrument"],
    )
    with pytest.raises(ValueError, match="non-empty instrument identities"):
        engine.compute_selected_submatrix(
            pd.DataFrame({"candidate": [1.0, 2.0]}, index=invalid_identity),
            pd.DataFrame({"reference": [2.0, 1.0]}, index=valid_index),
            as_of_date="2026-01-05",
        )


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
                "factors": {"factor_a": {"as_of_date": "2026-04-10"}},
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
    monkeypatch.setattr(
        svc,
        "_reconcile_correlation_state",
        lambda reset_all=False: {
            "eligible_factors": 2,
            "deleted_pairs": 0,
            "reset_ineligible_catalog": 0,
            "reset_orphan_catalog": 0,
            "reset_all_catalog": 0,
        },
    )
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


@pytest.mark.parametrize("failure_stage", [None, "preflight", "matrix"])
def test_local_correlation_compute_path_is_service_owned_and_db_safe(monkeypatch, tmp_path, failure_stage) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc
    from backend.services.quantevolver.correlation_engine import CorrelationResult

    (tmp_path / "_meta.json").write_text(
        json.dumps(
            {
                "moneyflow_unit_contract_version": MONEYFLOW_UNIT_CONTRACT_VERSION,
                "factors": {
                    "factor_a": {"as_of_date": "2026-04-10"},
                    "factor_b": {"as_of_date": "2026-04-10"},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    statements = []
    old_snapshot = tmp_path / "data" / "correlation_matrices" / "corr_20260410.h5"
    old_snapshot.parent.mkdir(parents=True)
    old_snapshot.write_bytes(b"existing published snapshot")

    class FakePipeline:
        _output_dir = str(tmp_path)

        def validate_meta_integrity(self):
            if failure_stage == "preflight":
                raise RuntimeError("preflight failure")
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

        def execute(self, sql, *_args, **_kwargs):
            statements.append(str(sql))
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
            if failure_stage == "matrix":
                raise RuntimeError("matrix failure")
            assert kwargs["save_hdf5"] is False
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
    monkeypatch.setattr(svc, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(svc, "get_correlation_factor_value_pipeline", lambda: FakePipeline())
    monkeypatch.setattr(svc, "CORRELATION_FACTOR_VALUE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(svc, "get_conn", lambda: FakeConn())
    monkeypatch.setattr(svc, "FactorUniverseMaskService", lambda: _FakeUniverseMaskService())
    monkeypatch.setattr(
        svc,
        "_reconcile_correlation_state",
        lambda reset_all=False: {
            "eligible_factors": 2,
            "deleted_pairs": 0,
            "reset_ineligible_catalog": 0,
            "reset_orphan_catalog": 0,
            "reset_all_catalog": 0,
        },
    )
    monkeypatch.setattr(svc, "_update_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(svc, "_persist_correlations_batch", lambda records, **kwargs: len(records))
    monkeypatch.setattr(svc, "_persist_correlation_metadata", lambda result: None)
    monkeypatch.setattr(svc, "CorrelationEngine", FakeCorrelationEngine)
    monkeypatch.setattr(svc.FactorValueLoader, "invalidate_single_cache", lambda factor_name=None: None)
    monkeypatch.setattr(svc.FactorValueLoader, "invalidate_merged_cache", lambda pipeline_dir=None: None)

    result = svc.run_correlation_compute_local(["factor_a", "factor_b"])

    assert not statements, "Preflight/computation must not mutate published DB state"
    assert old_snapshot.read_bytes() == b"existing published snapshot"
    if failure_stage:
        assert result["success"] is False
        assert result["error"] == f"{failure_stage} failure"
        return
    assert result["success"] is True
    assert result["requested_factor_count"] == 2
    assert result["success_factor_count"] == 2
    assert result["record_count"] == 1
    assert result["as_of_date"] is None
    assert result["cache_source"] == "offline_research_backtest_factor_values"
    assert result["cache_root"] == str(tmp_path)


@pytest.mark.parametrize("failure_stage", [None, "insert", "catalog", "metadata", "h5", "replace", "commit"])
def test_full_correlation_publication_preserves_previous_state_on_failure(monkeypatch, tmp_path, failure_stage):
    from backend.services.quantevolver import correlation_compute_service as svc
    from backend.services.quantevolver.correlation_engine import CorrelationResult

    target = tmp_path / "corr_20260410.h5"
    target.write_bytes(b"previous H5")
    other = tmp_path / "corr_20260409.h5"
    other.write_bytes(b"other date")
    result = CorrelationResult(np.array([[1.0, 0.42], [0.42, 1.0]]),
                               ["factor_a", "factor_b"], "2026-04-10", 252, 0.01,
                               {"hdf5_path": str(target)})
    events = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, *_args):
            stage = "metadata" if "INSERT INTO qe_correlation_metadata" in sql else "catalog"
            events.append(sql)
            if failure_stage == stage:
                raise RuntimeError(stage)

    class Conn:
        def cursor(self):
            return Cursor()

    @contextmanager
    def connect(**options):
        assert options == {"autocommit": False, "manage_transaction": True}
        try:
            yield Conn()
            if failure_stage == "commit":
                raise RuntimeError("commit")
            events.append("COMMIT")
        except Exception:
            events.append("ROLLBACK")
            raise

    def insert(*_args, **_kwargs):
        events.append("INSERT PAIRS")
        if failure_stage == "insert":
            raise RuntimeError("insert")

    original_replace = os.replace

    def replace(source, destination):
        if failure_stage == "replace" and Path(source).name.startswith(".corr-stage-"):
            raise RuntimeError("replace")
        original_replace(source, destination)

    class Eligibility:
        def list_eligible_factors(self, **_kwargs):
            return [{"id": 1, "factor_name": "factor_a"}, {"id": 2, "factor_name": "factor_b"}]

    monkeypatch.setattr(svc, "get_conn", connect)
    monkeypatch.setattr(svc, "execute_values", insert)
    monkeypatch.setattr(svc, "FactorEligibilityService", Eligibility)
    monkeypatch.setattr(svc.os, "replace", replace)
    if failure_stage == "h5":
        monkeypatch.setattr(result, "to_hdf5", lambda _path: (_ for _ in ()).throw(RuntimeError("h5")))
    records = [{"factor_a": "factor_a", "factor_b": "factor_b", "correlation": 0.42,
                "method": "spearman_ewma", "data_period": "252d_as_of_2026-04-10"}]
    if failure_stage:
        with pytest.raises(RuntimeError, match=failure_stage):
            svc._persist_correlations_batch(records, replace_all=True, correlation_result=result)
        assert target.read_bytes() == b"previous H5"
        assert "COMMIT" not in events
        if failure_stage != "h5":
            assert events[-1] == "ROLLBACK"
    else:
        assert svc._persist_correlations_batch(records, replace_all=True, correlation_result=result) == 1
        assert events[0] == "TRUNCATE TABLE qe_factor_correlations"
        assert events[-1] == "COMMIT"
        assert any("INSERT INTO qe_correlation_metadata" in sql for sql in events)
        assert np.array_equal(CorrelationResult.from_hdf5(str(target)).matrix, result.matrix)
    assert other.read_bytes() == b"other date"
    assert sorted(path.name for path in tmp_path.iterdir()) == [other.name, target.name]


def test_local_correlation_compute_classifies_matrix_factor_with_no_valid_pairs(monkeypatch, tmp_path) -> None:
    from backend.services.quantevolver import correlation_compute_service as svc
    from backend.services.quantevolver.correlation_engine import CorrelationResult

    factors = ["factor_a", "quality_structure_composite", "factor_b"]
    (tmp_path / "_meta.json").write_text(
        json.dumps(
            {
                "moneyflow_unit_contract_version": MONEYFLOW_UNIT_CONTRACT_VERSION,
                "factors": {name: {"as_of_date": "2026-04-30"} for name in factors},
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
        assert _kwargs["replace_all"] is True
        persisted_metadata.append(dict(_kwargs["correlation_result"].metadata))
        return len(records)

    def fake_persist_metadata(result):
        persisted_metadata.append(dict(result.metadata))

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(svc, "get_correlation_factor_value_pipeline", lambda: FakePipeline())
    monkeypatch.setattr(svc, "CORRELATION_FACTOR_VALUE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(svc, "get_conn", lambda: FakeConn())
    monkeypatch.setattr(svc, "FactorUniverseMaskService", lambda: _FakeUniverseMaskService())
    monkeypatch.setattr(
        svc,
        "_reconcile_correlation_state",
        lambda reset_all=False: {
            "eligible_factors": 3,
            "deleted_pairs": 0,
            "reset_ineligible_catalog": 0,
            "reset_orphan_catalog": 0,
            "reset_all_catalog": 0,
        },
    )
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


def test_correlation_engine_excludes_all_nan_factor_without_aborting_matrix(tmp_path) -> None:
    from backend.services.quantevolver.correlation_engine import CorrelationEngine

    dates = ["2026-08-28", "2026-08-31"]
    instruments = [f"{code:06d}.SZ" for code in range(1, 41)]
    index = pd.MultiIndex.from_product(
        [pd.to_datetime(dates), instruments],
        names=["datetime", "instrument"],
    )
    panel = pd.DataFrame(
        {
            "factor_a": list(range(40)) + list(range(1, 41)),
            "quality_structure_composite": [np.nan] * 80,
            "factor_b": list(reversed(range(40))) + list(reversed(range(1, 41))),
        },
        index=index,
    )

    class FakeLoader:
        def get_date_range(self):
            return dates[0], dates[-1]

        def get_trading_dates(self, _start_date, _end_date):
            return dates

        def load_factor_panel(self, factor_names, **_kwargs):
            return panel.loc[:, factor_names]

    result = CorrelationEngine(
        FakeLoader(),
        window=2,
        half_life=1,
        min_stocks=30,
        min_days=2,
        winsorize_quantile=0.0,
        hdf5_dir=str(tmp_path),
    ).compute_full_matrix(
        ["factor_a", "quality_structure_composite", "factor_b"],
        as_of_date=dates[-1],
        save_hdf5=False,
    )

    assert result.factor_names == ["factor_a", "factor_b"]
    assert result.matrix.shape == (2, 2)
    assert np.isfinite(result.matrix[0, 1])


def test_correlation_engine_fails_closed_when_all_nan_exclusion_leaves_one_factor(tmp_path) -> None:
    from backend.services.quantevolver.correlation_engine import CorrelationEngine

    dates = ["2026-08-28", "2026-08-31"]
    instruments = [f"{code:06d}.SZ" for code in range(1, 41)]
    index = pd.MultiIndex.from_product(
        [pd.to_datetime(dates), instruments],
        names=["datetime", "instrument"],
    )
    panel = pd.DataFrame(
        {
            "factor_a": list(range(40)) + list(range(1, 41)),
            "all_nan_factor": [np.nan] * 80,
        },
        index=index,
    )

    class FakeLoader:
        def get_trading_dates(self, _start_date, _end_date):
            return dates

        def load_factor_panel(self, factor_names, **_kwargs):
            return panel.loc[:, factor_names]

    engine = CorrelationEngine(
        FakeLoader(),
        window=2,
        half_life=1,
        min_stocks=30,
        min_days=2,
        winsorize_quantile=0.0,
        hdf5_dir=str(tmp_path),
    )

    with pytest.raises(ValueError, match="remaining=1"):
        engine.compute_full_matrix(
            ["factor_a", "all_nan_factor"],
            as_of_date=dates[-1],
            save_hdf5=False,
        )
