from __future__ import annotations

import pytest
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
    assert captured["batch_size"] == 16
    assert "official_factor_full_test" in router._active_cache_tasks


def test_factor_cache_compute_blocks_legacy_resume(monkeypatch):
    monkeypatch.setattr("backend.services.quantevolver.config_composer.ConfigComposer", _FakeComposer)
    req = router.FactorCacheComputeRequest(
        start_date="2018-08-01",
        end_date="2026-04-30",
        resume_task_id="legacy",
    )

    with pytest.raises(router.HTTPException) as exc:
        router.factor_cache_compute(req, BackgroundTasks())

    assert exc.value.status_code == 400
    assert "legacy backfill" in str(exc.value.detail)
