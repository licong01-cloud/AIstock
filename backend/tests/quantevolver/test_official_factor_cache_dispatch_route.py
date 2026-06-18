from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError
from starlette.background import BackgroundTasks

from backend.routers import quantevolver as router


class _FactorCodeConn:
    def __init__(self, row):
        self.row = row
        self.cur = type("Cur", (), {})()
        self.cur.description = [
            ("factor_name",),
            ("source",),
            ("transformation_status",),
            ("last_transformation_at",),
            ("qe_code_path",),
            ("asset_path",),
        ]
        self.cur.fetchone = lambda: self.row
        self.cur.execute = lambda *args, **kwargs: None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        conn = self

        class _CursorCM:
            def __enter__(self):
                return conn.cur

            def __exit__(self, exc_type, exc, tb):
                return False

        return _CursorCM()


class _FakeComposer:
    def _get_experiment_record(self, experiment_id):
        return {"node_id": "node-a"}

    def _fetch_workspace_config(self, node_id=None):
        return {"factor_data_dir": "/mnt/f/factor_data", "qlib_data_path": "/mnt/f/qlib_bin"}


class _NoopCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        return None


class _NoopConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _NoopCursor()


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


def test_official_full_compute_custom_dispatch_uses_legacy_wsl_runner(monkeypatch):
    from backend.services import dispatch_service as dispatch_mod

    captured = {}

    class _FakeClient:
        def __init__(self, base_url):
            captured["base_url"] = base_url

        async def create_task(self, payload):
            captured["scheduler_payload"] = payload
            return {"task": {"id": "remote-1"}}

    svc = dispatch_mod.DispatchService()
    monkeypatch.setattr(dispatch_mod, "ComputeNodeClient", _FakeClient)
    monkeypatch.setattr(dispatch_mod, "get_conn", lambda: _NoopConn())
    def _fake_insert_task(data):
        captured["insert_task"] = data
        return {"task_id": "local-1"}

    monkeypatch.setattr(svc, "_insert_task", _fake_insert_task)
    monkeypatch.setattr(svc, "_add_event", lambda *args, **kwargs: captured.setdefault("events", []).append((args, kwargs)))
    monkeypatch.setattr(svc, "_update_task_fields", lambda *args, **kwargs: captured.setdefault("updates", []).append((args, kwargs)))
    monkeypatch.setattr(svc, "_start_collector", lambda *args, **kwargs: captured.setdefault("collector", args))

    result = asyncio.run(
        svc._create_custom_task(
            {
                "task_name": "official-factor-full-smoke",
                "task_type": "official_factor_full_compute",
                "payload": {
                    "factor_names": ["Alpha_Test"],
                    "start_date": "2018-08-01",
                    "end_date": "2026-04-30",
                    "cache_source": "official_offline_backtest_factor_data",
                },
            },
            {"node_id": "wsl2-5080", "api_base_url": "http://127.0.0.1:9000"},
        )
    )

    assert result["status"] == "running"
    assert captured["insert_task"]["task_type"] == "official_factor_full_compute"
    scheduler_payload = captured["scheduler_payload"]
    assert scheduler_payload["task_type"] == "official_evaluation"
    assert scheduler_payload["payload"]["handler_task_type"] == "official_factor_full_compute"
    assert scheduler_payload["payload"]["task_type"] == "official_factor_full_compute"
    assert scheduler_payload["payload"]["cache_source"] == "official_offline_backtest_factor_data"


def test_official_evaluation_wsl_runner_delegates_full_compute_payload(monkeypatch, tmp_path, capsys):
    from backend.scripts import run_official_evaluation_wsl as runner

    payload = {
        "handler_task_type": "official_factor_full_compute",
        "factor_names": ["Alpha_Test"],
        "start_date": "2018-08-01",
        "end_date": "2026-04-30",
    }
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(payload), encoding="utf-8")
    captured = {}

    class _FakeFullCompute:
        def compute(self, data):
            captured["payload"] = data
            return {"success": True, "status": "success", "cache_source": "official_offline_backtest_factor_data"}

    class _LegacyEvaluationShouldNotRun:
        def __init__(self):
            raise AssertionError("official_factor_full_compute payload must not use legacy local evaluation")

    monkeypatch.setattr(runner, "assert_wsl_runtime", lambda operation: None)
    monkeypatch.setattr(runner, "OfficialFactorBatchComputeService", lambda: _FakeFullCompute())
    monkeypatch.setattr(runner, "FactorOfficialEvaluationService", _LegacyEvaluationShouldNotRun)
    monkeypatch.setattr(sys, "argv", [str(Path(runner.__file__)), str(payload_path)])

    assert runner.main() == 0
    emitted = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert emitted["type"] == "result"
    assert emitted["data"]["success"] is True
    assert captured["payload"] == payload


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


def test_factor_transformation_status_exposes_non_official_fields(monkeypatch):
    class _FakeTransformationService:
        def get_factor_transformation_status(self, **kwargs):
            return {
                "ok": True,
                "total": 1,
                "items": [
                    {
                        "factor_name": "factor_a",
                        "source": "rdagent_task_sync",
                        "has_realtime_code": 1,
                        "qe_code_path": "rdagent_assets/qe_factors/factor_a.py",
                        "has_original_code": True,
                    }
                ],
            }

    monkeypatch.setattr(
        "backend.services.quantevolver.factor_transformation_service.FactorTransformationService",
        lambda: _FakeTransformationService(),
    )

    result = router.get_factor_transformation_status()
    item = result["items"][0]

    assert item["has_non_official_code"] is True
    assert item["non_official_code_path"] == "rdagent_assets/qe_factors/factor_a.py"
    assert "has_realtime_code" not in item
    assert "qe_code_path" not in item


def test_factor_transformation_code_endpoint_exposes_non_official_fields(monkeypatch, tmp_path):
    transformed = tmp_path / "factor_a_live.py"
    original = tmp_path / "factor_a_original.py"
    transformed.write_text("LIVE_CODE = 1", encoding="utf-8")
    original.write_text("OFFICIAL_CODE = 1", encoding="utf-8")
    row = (
        "factor_a",
        "rdagent_task_sync",
        "SUCCESS",
        None,
        str(transformed),
        str(original),
    )
    monkeypatch.setattr(router, "get_conn", lambda: _FactorCodeConn(row))

    result = router.get_factor_non_official_code("factor_a")
    factor = result["factor"]

    assert result["ok"] is True
    assert factor["non_official_code_path"] == str(transformed)
    assert factor["transformed_code_text"] == "LIVE_CODE = 1"
    assert factor["code_text"] == "OFFICIAL_CODE = 1"
    assert "qe_code_path" not in factor
    assert "realtime_code_text" not in factor

