from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError
from starlette.background import BackgroundTasks

from backend.routers import quantevolver as router


class _FakeComposer:
    def _get_experiment_record(self, experiment_id):
        return {"node_id": "node-a"}

    def _fetch_workspace_config(self, node_id=None):
        return {"factor_data_dir": "/mnt/f/factor_data", "qlib_data_path": "/mnt/f/qlib_bin"}


def test_factor_cache_compute_submits_official_dispatch(monkeypatch):
    captured = {}

    class _FakeDispatchService:
        def submit(self, **kwargs):
            captured.update(kwargs)
            return {
                "ok": True,
                "status": "running",
                "task_id": "official_factor_full_test",
                "dispatch_task_id": "official_factor_full_test",
                "remote_task_id": "remote-1",
                "node_id": kwargs.get("node_id"),
                "payload": {
                    "batch_size": kwargs.get("batch_size"),
                    "cache_source": "official_offline_backtest_factor_data",
                    "code_source": "code_text",
                },
                "cache_root": "rdagent_assets/factor_values",
            }

    monkeypatch.setattr(router, "_invalidate_cache_meta", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.services.quantevolver.config_composer.ConfigComposer", _FakeComposer)
    monkeypatch.setattr(
        "backend.services.quantevolver.official_factor_full_compute_dispatch_service.OfficialFactorFullComputeDispatchService",
        lambda: _FakeDispatchService(),
    )

    req = router.FactorCacheComputeRequest(
        factor_names=["factor_a", "factor_b"],
        experiment_id="exp-1",
        start_date="2018-08-01",
        end_date="2026-04-30",
        workers=4,
        force=True,
    )
    result = router.factor_cache_compute(req, BackgroundTasks())

    assert result["ok"] is True
    assert result["dispatch_task_id"] == "official_factor_full_test"
    assert result["cache_source"] == "official_offline_backtest_factor_data"
    assert result["code_source"] == "code_text"
    assert captured["factor_names"] == ["factor_a", "factor_b"]
    assert captured["factor_data_dir"] == "/mnt/f/factor_data"
    assert captured["qlib_bin_path"] == "/mnt/f/qlib_bin"
    assert captured["start_date"] == "2018-08-01"
    assert captured["end_date"] == "2026-04-30"
    assert captured["node_id"] == "node-a"
    assert captured["include_disabled"] is False
    assert captured["batch_size"] == 16
    assert "official_factor_full_test" in router._active_cache_tasks


def test_factor_cache_compute_explicit_factors_can_include_disabled(monkeypatch):
    captured = {}

    class _FakeDispatchService:
        def submit(self, **kwargs):
            captured.update(kwargs)
            return {
                "ok": True,
                "status": "running",
                "task_id": "official_factor_full_disabled",
                "dispatch_task_id": "official_factor_full_disabled",
                "payload": {
                    "batch_size": kwargs.get("batch_size"),
                    "include_disabled": kwargs.get("include_disabled"),
                },
                "cache_root": "rdagent_assets/factor_values",
            }

    monkeypatch.setattr(router, "_invalidate_cache_meta", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.services.quantevolver.config_composer.ConfigComposer", _FakeComposer)
    monkeypatch.setattr(
        "backend.services.quantevolver.official_factor_full_compute_dispatch_service.OfficialFactorFullComputeDispatchService",
        lambda: _FakeDispatchService(),
    )

    req = router.FactorCacheComputeRequest(
        factor_names=["disabled_factor"],
        include_disabled=True,
        start_date="2018-08-01",
        end_date="2026-04-30",
    )
    result = router.factor_cache_compute(req, BackgroundTasks())

    assert result["ok"] is True
    assert result["include_disabled"] is True
    assert captured["include_disabled"] is True
    assert router._active_cache_tasks["official_factor_full_disabled"]["include_disabled"] is True


def test_factor_cache_compute_blocks_legacy_resume(monkeypatch):
    monkeypatch.setattr("backend.services.quantevolver.config_composer.ConfigComposer", _FakeComposer)

    with pytest.raises(ValidationError) as exc:
        router.FactorCacheComputeRequest(
            start_date="2018-08-01",
            end_date="2026-04-30",
            resume_task_id="legacy",
        )

    assert "resume_task_id" in str(exc.value)
    assert "Extra inputs are not permitted" in str(exc.value)


def test_official_evaluation_compute_forwards_to_full_compute_without_data_snapshot(monkeypatch):
    import asyncio

    captured = {}

    class _FakeOfficialEvaluationService:
        def compute(self, **kwargs):
            captured.update(kwargs)
            return {
                "ok": True,
                "success": True,
                "task_type": "official_factor_full_compute",
                "task_id": "official_factor_full_route",
                "cache_source": "official_offline_backtest_factor_data",
            }

    monkeypatch.setattr(
        "backend.services.quantevolver.factor_official_evaluation_service.FactorOfficialEvaluationService",
        lambda: _FakeOfficialEvaluationService(),
    )

    req = router.OfficialEvaluationComputeRequest(
        factor_names=["factor_a"],
        start_date="2018-08-01",
        end_date="2026-04-30",
        max_workers=2,
        force=True,
    )
    result = asyncio.run(router.official_evaluation_compute(req))

    assert result["task_type"] == "official_factor_full_compute"
    assert captured["factor_names"] == ["factor_a"]
    assert captured["data_date"] == "2026-04-30"
    assert captured["start_date"] == "2018-08-01"
    assert captured["end_date"] == "2026-04-30"
    assert captured["max_workers"] == 2
    assert captured["force"] is True


def test_factor_list_metric_buttons_submit_official_cache_compute():
    source = Path("frontend/src/app/quantevolver/components/FactorList.tsx").read_text(encoding="utf-8")
    batch_metrics = source[source.index("async function batchFetchMetrics"):source.index("async function batchAnalyze")]
    task_metrics = source[source.index("async function computeSelectedTasksMetrics"):source.index("async function analyzeSelectedTasksFactors")]

    assert "/factor-cache/compute" in source
    assert "submitOfficialFullCompute" in batch_metrics
    assert "submitOfficialFullCompute" in task_metrics
    assert "include_disabled: includeDisabled" in source
    assert "official-evaluation/compute" not in batch_metrics
    assert "official-evaluation/compute" not in task_metrics
    assert "activeSnapshot" not in batch_metrics
    assert "activeSnapshot" not in task_metrics
