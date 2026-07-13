from __future__ import annotations

import builtins
from pathlib import Path

import pandas as pd

from backend.services.quantevolver import factor_official_evaluation_service as svc
from backend.services.quantevolver.factor_official_evaluation_service import (
    FactorOfficialEvaluationService,
)
from backend.services.quantevolver.factor_value_loader import FactorValueLoader


class _DummyEligibility:
    def __init__(self, names: list[str]):
        self._names = names

    def list_eligible_factors(self, factor_names=None, include_disabled=True, source_mode="official_offline"):
        names = factor_names or self._names
        return [{"factor_name": name} for name in names]


class _DummyUniverse:
    def metadata(self, start_date: str, end_date: str):
        return {
            "universe_key": "shsz_st_pit_active_v1",
            "start_date": start_date,
            "end_date": end_date,
        }

    def get_window_union_instruments(self, start_date: str, end_date: str, ensure: bool = False):
        return ["000001.SZ"]


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return [("Alpha_Test", 101)]


class _Conn:
    def cursor(self):
        return _Cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _factor_df(value: float = 1.0, dates: list[str] | None = None) -> pd.DataFrame:
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp(day) for day in (dates or ["2018-08-01", "2026-04-30"])], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame({"Alpha_Test": [value + i for i in range(len(idx))]}, index=idx)


def test_official_constructor_uses_offline_single_loader(monkeypatch):
    captured = {}

    class _FakeLoader:
        def __init__(self, source: str, pipeline_dir: str):
            captured["source"] = source
            captured["pipeline_dir"] = pipeline_dir

    monkeypatch.setattr(svc, "FactorValueLoader", _FakeLoader)
    monkeypatch.setattr(svc, "FactorEligibilityService", lambda: _DummyEligibility(["Alpha_Test"]))
    monkeypatch.setattr(svc, "FactorUniverseMaskService", lambda: _DummyUniverse())

    service = FactorOfficialEvaluationService()

    assert captured["source"] == "single"
    assert Path(captured["pipeline_dir"]).parts[-2:] == ("rdagent_assets", "factor_values")
    assert captured["pipeline_dir"].replace("\\", "/").endswith("rdagent_assets/factor_values")
    assert not hasattr(service, "_pipeline")


def test_compute_forwards_to_official_full_compute_dispatch(monkeypatch):
    captured = {}

    class _FakeComposer:
        def _fetch_workspace_config(self, node_id=None):
            captured["config_node_id"] = node_id
            return {"factor_data_dir": "/mnt/f/factor_data", "qlib_data_path": "/mnt/f/qlib_bin"}

    class _FakeFullComputeDispatch:
        def __init__(self, dispatch_service=None):
            captured["dispatch_service"] = dispatch_service

        def submit(self, **kwargs):
            captured["submit"] = kwargs
            return {
                "ok": True,
                "status": "running",
                "task_id": "official_factor_full_test",
                "dispatch_task_id": "official_factor_full_test",
                "node_id": kwargs.get("node_id"),
                "payload": {
                    "cache_source": "official_offline_backtest_factor_data",
                    "code_source": "code_text",
                },
                "cache_root": "rdagent_assets/factor_values",
            }

    monkeypatch.setattr(
        "backend.services.quantevolver.config_composer.ConfigComposer",
        lambda: _FakeComposer(),
    )
    monkeypatch.setattr(
        "backend.services.quantevolver.official_factor_full_compute_dispatch_service.OfficialFactorFullComputeDispatchService",
        _FakeFullComputeDispatch,
    )

    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    service._dispatch_service = object()

    result = service.compute(
        factor_names=["Alpha_Test"],
        data_date="20260430",
        include_disabled=True,
        max_workers=4,
        timeout_per_factor=900,
    )

    assert result["success"] is True
    assert result["task_type"] == "official_factor_full_compute"
    assert result["legacy_compatibility"] == "official_evaluation_compute_forwarded_to_official_factor_full_compute"
    assert result["cache_source"] == "official_offline_backtest_factor_data"
    assert captured["submit"]["factor_names"] == ["Alpha_Test"]
    assert captured["submit"]["factor_data_dir"] == "/mnt/f/factor_data"
    assert captured["submit"]["qlib_bin_path"] == "/mnt/f/qlib_bin"
    assert captured["submit"]["start_date"] == "2018-08-01"
    assert captured["submit"]["end_date"] == "2026-04-30"
    assert captured["submit"]["workers"] == 4
    assert captured["submit"]["batch_size"] == 16



def test_compute_local_reads_backtest_cache_without_snapshot_or_pipeline(monkeypatch):
    calls = {"loader": [], "prepare": None, "metrics_factor": None, "saved": None}

    class _FakeLoader:
        def get_available_factors(self):
            return ["Alpha_Test"]

        def load_single_factor(self, factor_name, start_date=None, end_date=None):
            calls["loader"].append((factor_name, start_date, end_date))
            return _factor_df()

    def _guard_import(name, *args, **kwargs):
        if "factor_value_pipeline" in name:
            raise AssertionError("official evaluation must not import FactorValuePipeline")
        return real_import(name, *args, **kwargs)

    from backend.services.quantevolver import qe_eval_v2_metric_engine as engine

    def _prepare_shared_context(qlib_bin_path, start_date, end_date):
        calls["prepare"] = (str(qlib_bin_path), start_date, end_date)
        return {"ctx": True}

    def _compute_single_factor_metrics(factor_name, factor_df, metrics_ctx):
        calls["metrics_factor"] = (factor_name, len(factor_df), metrics_ctx)
        return {"metrics": {"full": {"data_start": "2018-08-01", "data_end": "2026-04-30"}}}

    monkeypatch.setattr(engine, "prepare_shared_context", _prepare_shared_context)
    monkeypatch.setattr(engine, "compute_single_factor_metrics", _compute_single_factor_metrics)
    monkeypatch.setattr(svc, "get_conn", lambda: _Conn())
    real_import = builtins.__import__
    monkeypatch.setattr(builtins, "__import__", _guard_import)

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)


    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    service._eligibility_service = _DummyEligibility(["Alpha_Test"])
    service._universe_service = _DummyUniverse()
    service._factor_value_loader = _FakeLoader()
    service._resolve_qlib_bin_path = lambda: Path("qlib_bin/qlib_bin_20260311")

    def _save_metrics(engine_data, snapshot_date, factor_ids):
        calls["saved"] = (engine_data, snapshot_date, factor_ids)
        return {"inserted": 1, "skipped": 0, "errors": []}

    service._save_metrics = _save_metrics
    service._save_monthly_ic = lambda *args, **kwargs: None

    result = service._compute_local(
        factor_names=["Alpha_Test"],
        data_date="20260430",
        include_disabled=True,
    )

    assert result["success"] is True
    assert calls["loader"] == [("Alpha_Test", "2018-08-01", "2026-04-30")]
    assert calls["prepare"][1:] == ("2018-08-01", "2026-04-30")
    assert calls["metrics_factor"][0] == "Alpha_Test"
    assert calls["saved"][1] == "2026-04-30"
    assert result["pipeline_summary"]["output_path"].endswith("rdagent_assets\\factor_values\\single") or (
        result["pipeline_summary"]["output_path"].endswith("rdagent_assets/factor_values/single")
    )


def test_compute_local_fails_fast_when_backtest_cache_window_is_incomplete(monkeypatch):
    class _FakeLoader:
        def get_available_factors(self):
            return ["Alpha_Test"]

        def load_single_factor(self, factor_name, start_date=None, end_date=None):
            return _factor_df(dates=["2019-01-01", "2026-04-30"])

    from backend.services.quantevolver import qe_eval_v2_metric_engine as engine

    monkeypatch.setattr(engine, "prepare_shared_context", lambda **kwargs: {"ctx": True})
    monkeypatch.setattr(engine, "compute_single_factor_metrics", lambda *args, **kwargs: {"metrics": {}})
    monkeypatch.setattr(svc, "get_conn", lambda: _Conn())

    monkeypatch.setattr(svc, "assert_wsl_runtime", lambda operation: None)
    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    service._eligibility_service = _DummyEligibility(["Alpha_Test"])
    service._universe_service = _DummyUniverse()
    service._factor_value_loader = _FakeLoader()
    service._resolve_qlib_bin_path = lambda: Path("qlib_bin/qlib_bin_20260311")
    service._save_metrics = lambda *args, **kwargs: {"inserted": 1, "skipped": 0, "errors": []}
    service._save_monthly_ic = lambda *args, **kwargs: None

    result = service._compute_local(
        factor_names=["Alpha_Test"],
        data_date="20260430",
        include_disabled=True,
    )

    assert result["success"] is False
    assert result["success_count"] == 0
    assert result["db_result"]["cache_failures"] == ["Alpha_Test"]
    assert "cache_start=2019-01-01" in result["error"]


def test_factor_value_loader_single_cache_is_scoped_by_pipeline_dir(tmp_path):
    FactorValueLoader.invalidate_single_cache()
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp("2026-04-30")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    dir_a = tmp_path / "cache_a" / "single"
    dir_b = tmp_path / "cache_b" / "single"
    dir_a.mkdir(parents=True)
    dir_b.mkdir(parents=True)
    pd.DataFrame({"value": [1.0]}, index=idx).to_parquet(dir_a / "Alpha_Test.parquet")
    pd.DataFrame({"value": [9.0]}, index=idx).to_parquet(dir_b / "Alpha_Test.parquet")

    loader_a = FactorValueLoader(source="single", pipeline_dir=str(tmp_path / "cache_a"))
    loader_b = FactorValueLoader(source="single", pipeline_dir=str(tmp_path / "cache_b"))

    first = loader_a.load_single_factor("Alpha_Test")
    second = loader_b.load_single_factor("Alpha_Test")

    assert float(first.iloc[0, 0]) == 1.0
    assert float(second.iloc[0, 0]) == 9.0
