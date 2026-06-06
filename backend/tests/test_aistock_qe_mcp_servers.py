"""Unit tests for QE MCP thin HTTP wrappers."""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest


def _mock_transport(handler):
    def adapter(request: httpx.Request) -> httpx.Response:
        result = handler(request)
        if isinstance(result, httpx.Response):
            return result
        if isinstance(result, tuple):
            status, payload = result
            return httpx.Response(status, json=payload)
        return httpx.Response(200, json=result)
    return httpx.MockTransport(adapter)


@pytest.fixture
def experiment_mcp(monkeypatch):
    monkeypatch.setenv("AISTOCK_QE_EXPERIMENT_BASE_URL", "http://127.0.0.1/api/v1")
    sys.modules.pop("scripts.aistock_qe_experiment_mcp_server", None)
    module = importlib.import_module("scripts.aistock_qe_experiment_mcp_server")
    yield module
    sys.modules.pop("scripts.aistock_qe_experiment_mcp_server", None)


@pytest.fixture
def archive_mcp(monkeypatch):
    monkeypatch.setenv("AISTOCK_QE_ARCHIVE_BASE_URL", "http://127.0.0.1/api/v1/qe-archive")
    sys.modules.pop("scripts.aistock_qe_archive_mcp_server", None)
    module = importlib.import_module("scripts.aistock_qe_archive_mcp_server")
    yield module
    sys.modules.pop("scripts.aistock_qe_archive_mcp_server", None)


def _swap(module: Any, client: Any) -> None:
    module._default_client = client


def test_qe_mcp_default_base_urls_use_production_loopback_port(monkeypatch):
    monkeypatch.delenv("AISTOCK_QE_EXPERIMENT_BASE_URL", raising=False)
    monkeypatch.delenv("AISTOCK_QE_ARCHIVE_BASE_URL", raising=False)
    sys.modules.pop("scripts.aistock_qe_experiment_mcp_server", None)
    sys.modules.pop("scripts.aistock_qe_archive_mcp_server", None)

    experiment = importlib.import_module("scripts.aistock_qe_experiment_mcp_server")
    archive = importlib.import_module("scripts.aistock_qe_archive_mcp_server")

    assert experiment.DEFAULT_BASE_URL == "http://127.0.0.1:8001/api/v1"
    assert archive.DEFAULT_BASE_URL == "http://127.0.0.1:8001/api/v1/qe-archive"
    assert experiment._default_client.base_url == "http://127.0.0.1:8001/api/v1"
    assert archive._default_client.base_url == "http://127.0.0.1:8001/api/v1/qe-archive"
    assert ":8011" not in experiment.DEFAULT_BASE_URL
    assert ":8011" not in archive.DEFAULT_BASE_URL

    sys.modules.pop("scripts.aistock_qe_experiment_mcp_server", None)
    sys.modules.pop("scripts.aistock_qe_archive_mcp_server", None)


def test_qe_experiment_mcp_requires_loopback(monkeypatch):
    monkeypatch.setenv("AISTOCK_QE_EXPERIMENT_BASE_URL", "http://example.com/api/v1")
    sys.modules.pop("scripts.aistock_qe_experiment_mcp_server", None)
    with pytest.raises(ValueError, match="loopback"):
        importlib.import_module("scripts.aistock_qe_experiment_mcp_server")


def test_qe_experiment_run_requires_confirm_before_http(experiment_mcp):
    called = False
    def handler(request: httpx.Request):
        nonlocal called
        called = True
        return {"ok": True}
    _swap(experiment_mcp, experiment_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1", env_name="test", transport=_mock_transport(handler)))
    with pytest.raises(ValueError, match="confirm_run"):
        experiment_mcp.qe_experiment_run_confirmed("qe_1")
    assert called is False


def test_qe_experiment_run_posts_existing_backend_endpoint(experiment_mcp):
    captured = {}
    def handler(request: httpx.Request):
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["query"] = dict(request.url.params)
        return {"status": "success"}
    _swap(experiment_mcp, experiment_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1", env_name="test", transport=_mock_transport(handler)))
    result = experiment_mcp.qe_experiment_run_confirmed("qe_1", node_id="wsl2-5080", confirm_run="QE_EXPERIMENT_RUN")
    assert result["status"] == "success"
    assert captured["method"] == "POST"
    assert captured["path"].endswith("/quantevolver/experiments/qe_1/run")
    assert captured["query"]["engine_mode"] == "unified"


@pytest.mark.parametrize("bad_id", ["../x", "a/b", "x%2Fy", "id?force=1", "white space", ""])
def test_qe_experiment_mcp_rejects_bad_ids(experiment_mcp, bad_id: str):
    with pytest.raises(ValueError):
        experiment_mcp.qe_experiment_get(bad_id)


def test_qe_experiment_list_defaults_to_summary_detail(experiment_mcp):
    captured = {}

    def handler(request: httpx.Request):
        captured["path"] = request.url.path
        captured["query"] = dict(request.url.params)
        return {"ok": True, "items": []}

    _swap(experiment_mcp, experiment_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1", env_name="test", transport=_mock_transport(handler)))
    result = experiment_mcp.qe_experiment_list(limit=7, include_children=True)

    assert result["ok"] is True
    assert captured["path"].endswith("/quantevolver/experiments")
    assert captured["query"]["detail"] == "summary"
    assert captured["query"]["limit"] == "7"
    assert captured["query"]["include_children"] == "true"


def test_qe_experiment_get_defaults_to_summary_detail(experiment_mcp):
    captured = {}

    def handler(request: httpx.Request):
        captured["path"] = request.url.path
        captured["query"] = dict(request.url.params)
        return {"ok": True, "experiment": {"experiment_id": "qe_1"}}

    _swap(experiment_mcp, experiment_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1", env_name="test", transport=_mock_transport(handler)))
    result = experiment_mcp.qe_experiment_get("qe_1")

    assert result["ok"] is True
    assert captured["path"].endswith("/quantevolver/experiments/qe_1")
    assert captured["query"] == {"detail": "summary"}


def test_qe_custom_evo_task_tools_use_summary_and_loop_payload_paths(experiment_mcp):
    captured = []

    def handler(request: httpx.Request):
        captured.append((request.method, request.url.path, dict(request.url.params)))
        return {"status": "success", "data": {}}

    _swap(experiment_mcp, experiment_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1", env_name="test", transport=_mock_transport(handler)))

    experiment_mcp.qe_custom_evo_list_tasks(limit=3)
    experiment_mcp.qe_custom_evo_get_task("task_1")
    experiment_mcp.qe_custom_evo_loop_comparison("task_1")
    experiment_mcp.qe_custom_evo_get_loop_config("task_1", 2)
    experiment_mcp.qe_custom_evo_get_loop_metrics("task_1", 2)
    experiment_mcp.qe_custom_evo_get_loop_analysis("task_1", 2)

    assert captured[0] == ("GET", "/api/v1/quantevolver/evolution/tasks", {"limit": "3", "detail": "summary"})
    assert captured[1] == ("GET", "/api/v1/quantevolver/evolution/tasks/task_1", {"detail": "summary"})
    assert captured[2] == ("GET", "/api/v1/quantevolver/evolution/tasks/task_1/loops/comparison", {})
    assert captured[3] == ("GET", "/api/v1/quantevolver/evolution/tasks/task_1/loops/2/config", {})
    assert captured[4] == ("GET", "/api/v1/quantevolver/evolution/tasks/task_1/loops/2/metrics", {})
    assert captured[5] == ("GET", "/api/v1/quantevolver/evolution/tasks/task_1/loops/2/analysis", {})


def test_qe_template_create_rejects_future_stock_pool_before_http(experiment_mcp):
    called = False

    def handler(request: httpx.Request):
        nonlocal called
        called = True
        return {"status": "success"}

    _swap(
        experiment_mcp,
        experiment_mcp.LoopbackApiClient(
            base_url="http://127.0.0.1/api/v1",
            env_name="test",
            transport=_mock_transport(handler),
        ),
    )

    with pytest.raises(ValueError, match="QE_STOCK_POOL_DATE_OUT_OF_WINDOW"):
        experiment_mcp.qe_template_create(
            "custom_evo",
            "future pool",
            {
                "loops": [
                    {
                        "factor_keys": ["alpha_factor||catalog"],
                        "model_id": "xgboost_v1",
                        "stock_pool": "filtered_pool_20260519",
                        "runtime_flags": {"random_seed": 20260529},
                    }
                ]
            },
        )

    assert called is False


def test_qe_template_delete_requires_confirm_before_http(experiment_mcp):
    called = False

    def handler(request: httpx.Request):
        nonlocal called
        called = True
        return {"status": "success"}

    _swap(experiment_mcp, experiment_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1", env_name="test", transport=_mock_transport(handler)))
    with pytest.raises(ValueError, match="confirm_delete"):
        experiment_mcp.qe_template_delete_confirmed("qet_1")
    assert called is False


def test_qe_template_delete_posts_confirmed_delete_path(experiment_mcp):
    captured = {}

    def handler(request: httpx.Request):
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["payload"] = json.loads(request.content.decode("utf-8"))
        return {"status": "success", "data": {"deleted_template": {"template_id": "qet_1"}}}

    _swap(experiment_mcp, experiment_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1", env_name="test", transport=_mock_transport(handler)))
    result = experiment_mcp.qe_template_delete_confirmed("qet_1", confirm_delete="QE_TEMPLATE_DELETE")

    assert result["status"] == "success"
    assert captured["method"] == "DELETE"
    assert captured["path"].endswith("/qe-templates/qet_1")
    assert captured["payload"] == {"confirm_delete": "QE_TEMPLATE_DELETE"}


def test_loopback_client_requires_refinement_for_large_success_response(experiment_mcp):
    payload = {"data": "x" * 200}

    def handler(request: httpx.Request):
        return httpx.Response(200, json=payload)

    client = experiment_mcp.LoopbackApiClient(
        base_url="http://127.0.0.1/api/v1",
        env_name="test",
        transport=_mock_transport(handler),
        max_response_bytes=64,
    )
    result = client.get("/quantevolver/experiments")

    assert result["status"] == "requires_refinement"
    assert result["mcp_response_too_large"] is True
    assert result["mcp_response_refinement_required"] is True
    assert result["partial_payload_returned"] is False
    assert result["method"] == "GET"
    assert result["path"] == "/quantevolver/experiments"
    assert result["status_code"] == 200
    assert result["original_bytes"] > result["max_bytes"]
    assert "preview" not in result
    assert result["omitted_sections"] == ["response_payload"]
    assert result["retry_with"]["params"]["limit"] == 20
    assert result["retry_with"]["params"]["detail"] == "summary"


def test_qe_archive_execute_requires_confirm_before_http(archive_mcp):
    called = False
    def handler(request: httpx.Request):
        nonlocal called
        called = True
        return {"status": "success"}
    _swap(archive_mcp, archive_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1/qe-archive", env_name="test", transport=_mock_transport(handler)))
    with pytest.raises(ValueError, match="confirm_backfill"):
        archive_mcp.qe_archive_backfill_execute_confirmed()
    assert called is False


def test_qe_archive_selection_preview_posts_explicit_ids(archive_mcp):
    captured = {}

    def handler(request: httpx.Request):
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["payload"] = json.loads(request.content.decode("utf-8"))
        return {"status": "success", "data": {"dry_run": True}}

    _swap(archive_mcp, archive_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1/qe-archive", env_name="test", transport=_mock_transport(handler)))
    result = archive_mcp.qe_archive_backfill_selection_preview(
        experiment_ids=["qe_exp_1"],
        task_ids=["task_1"],
        loop_ids=["task_1_Loop1"],
        task_id="task_1",
        loop_indices=[1, 3, 3],
    )

    assert result["status"] == "success"
    assert captured["method"] == "POST"
    assert captured["path"].endswith("/qe-archive/backfill/preview")
    assert captured["payload"] == {
        "source_mode": "specific_ids",
        "experiment_ids": ["qe_exp_1"],
        "task_ids": ["task_1"],
        "loop_ids": ["task_1_Loop1"],
        "task_id": "task_1",
        "loop_indices": [1, 3],
        "status": "completed",
        "include_archived": False,
        "requested_by": "qe_archive_mcp",
    }


def test_qe_archive_selection_execute_requires_confirm_before_http(archive_mcp):
    called = False

    def handler(request: httpx.Request):
        nonlocal called
        called = True
        return {"status": "success"}

    _swap(archive_mcp, archive_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1/qe-archive", env_name="test", transport=_mock_transport(handler)))
    with pytest.raises(ValueError, match="confirm_write"):
        archive_mcp.qe_archive_backfill_selection_execute_confirmed(task_id="task_1", loop_indices=[1])
    assert called is False


def test_qe_archive_selection_execute_posts_audited_execute_path(archive_mcp):
    captured = {}

    def handler(request: httpx.Request):
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["payload"] = json.loads(request.content.decode("utf-8"))
        return {"status": "success", "data": {"backfill_run_id": "qear_bf_1"}}

    _swap(archive_mcp, archive_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1/qe-archive", env_name="test", transport=_mock_transport(handler)))
    result = archive_mcp.qe_archive_backfill_selection_execute_confirmed(
        task_id="task_1",
        loop_indices=[1],
        confirm_write="QE_ARCHIVE_WRITE",
    )

    assert result["status"] == "success"
    assert captured["method"] == "POST"
    assert captured["path"].endswith("/qe-archive/backfill/execute")
    assert captured["payload"] == {
        "source_mode": "specific_ids",
        "experiment_ids": [],
        "task_ids": [],
        "loop_ids": [],
        "task_id": "task_1",
        "loop_indices": [1],
        "status": "completed",
        "include_archived": False,
        "requested_by": "qe_archive_mcp",
        "confirm_backfill": "QE_ARCHIVE_BACKFILL",
    }


def test_qe_archive_get_source_status_posts_selection(archive_mcp):
    captured = {}

    def handler(request: httpx.Request):
        captured["path"] = request.url.path
        captured["payload"] = json.loads(request.content.decode("utf-8"))
        return {"status": "success", "data": {"loops": {}}}

    _swap(archive_mcp, archive_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1/qe-archive", env_name="test", transport=_mock_transport(handler)))
    result = archive_mcp.qe_archive_get_source_status(experiment_ids=["qe_exp_1"], task_ids=["task_1"], loop_ids=["task_1_Loop1"])

    assert result["status"] == "success"
    assert captured["path"].endswith("/qe-archive/source-status")
    assert captured["payload"] == {
        "experiment_ids": ["qe_exp_1"],
        "task_ids": ["task_1"],
        "loop_ids": ["task_1_Loop1"],
        "include_recommendation": True,
    }


def test_qe_archive_query_factor_usage_path(archive_mcp):
    captured = {}
    def handler(request: httpx.Request):
        captured["path"] = request.url.path
        captured["query"] = dict(request.url.params)
        return {"status": "success", "data": []}
    _swap(archive_mcp, archive_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1/qe-archive", env_name="test", transport=_mock_transport(handler)))
    result = archive_mcp.qe_archive_query_factor_usage(limit=12, min_runs=2)
    assert result["status"] == "success"
    assert captured["path"].endswith("/qe-archive/query/factor-usage")
    assert captured["query"] == {"limit": "12", "min_runs": "2"}


def test_qe_archive_analytics_view_tools_use_compact_paths(archive_mcp):
    captured = []

    def handler(request: httpx.Request):
        captured.append((request.url.path, dict(request.url.params)))
        return {"status": "success", "data": []}

    _swap(archive_mcp, archive_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1/qe-archive", env_name="test", transport=_mock_transport(handler)))

    archive_mcp.qe_archive_query_analytics_view_status()
    archive_mcp.qe_archive_query_run_leaderboard(model_type="LSTM", min_icir=0.5, limit=6, order_by="icir")
    archive_mcp.qe_archive_query_seed_robustness(min_seed_count=3, stable_only=True, limit=7)
    archive_mcp.qe_archive_query_factor_performance(factor_name="alpha_001", min_runs=2, limit=8)
    archive_mcp.qe_archive_query_model_hyperparam_seed_perf(model_type="XGBoost", hyperparam_hash="abc", limit=9)
    archive_mcp.qe_archive_query_overfit_flags(suspicious_only=True, limit=10)
    archive_mcp.qe_archive_query_promotion_candidates(min_seed_count=5, limit=11, order_by="ir_mean")
    archive_mcp.qe_archive_query_evolution_lineage(task_id="task_1", limit=12)

    assert captured[0] == ("/api/v1/qe-archive/analytics/views", {})
    assert captured[1] == (
        "/api/v1/qe-archive/analytics/run-leaderboard",
        {"model_type": "LSTM", "min_icir": "0.5", "limit": "6", "order_by": "icir"},
    )
    assert captured[2] == (
        "/api/v1/qe-archive/analytics/seed-robustness",
        {"min_seed_count": "3", "stable_only": "true", "limit": "7", "order_by": "cagr_mean"},
    )
    assert captured[3] == (
        "/api/v1/qe-archive/analytics/factor-performance",
        {"factor_name": "alpha_001", "min_runs": "2", "limit": "8", "order_by": "best_cagr"},
    )
    assert captured[4] == (
        "/api/v1/qe-archive/analytics/model-hyperparam-seed-perf",
        {"model_type": "XGBoost", "hyperparam_hash": "abc", "limit": "9", "order_by": "cagr"},
    )
    assert captured[5] == (
        "/api/v1/qe-archive/analytics/overfit-flags",
        {"suspicious_only": "true", "limit": "10"},
    )
    assert captured[6] == (
        "/api/v1/qe-archive/analytics/promotion-candidates",
        {"min_seed_count": "5", "limit": "11", "order_by": "ir_mean"},
    )
    assert captured[7] == (
        "/api/v1/qe-archive/analytics/evolution-lineage",
        {"task_id": "task_1", "limit": "12"},
    )


def test_qe_archive_mcp_uses_compact_default_limits(archive_mcp):
    captured = []

    def handler(request: httpx.Request):
        captured.append((request.url.path, dict(request.url.params)))
        return {"status": "success", "data": []}

    _swap(archive_mcp, archive_mcp.LoopbackApiClient(base_url="http://127.0.0.1/api/v1/qe-archive", env_name="test", transport=_mock_transport(handler)))

    archive_mcp.qe_archive_list_runs()
    archive_mcp.qe_archive_list_skips()
    archive_mcp.qe_archive_query_factor_importance()
    archive_mcp.qe_archive_query_factor_importance_stability()
    archive_mcp.qe_archive_query_seed_trials()
    archive_mcp.qe_archive_query_run_leaderboard()
    archive_mcp.qe_archive_query_factor_performance()

    assert captured[0][1]["limit"] == "20"
    assert captured[1][1]["limit"] == "20"
    assert captured[2][1]["limit"] == "10"
    assert captured[3][1]["limit"] == "10"
    assert captured[4][1]["limit"] == "20"
    assert captured[5][1]["limit"] == "20"
    assert captured[6][1]["limit"] == "20"


def test_qe_mcp_scripts_do_not_import_runtime_execution_paths() -> None:
    banned = ("AutoEvolutionScheduler", "backend.db", "get_conn", "RDAgent", "workspace_path", "QE_WORKSPACE_WIN")
    for rel in ("scripts/aistock_qe_experiment_mcp_server.py", "scripts/aistock_qe_archive_mcp_server.py"):
        text = Path(rel).read_text(encoding="utf-8")
        for token in banned:
            assert token not in text, f"{rel} must not contain {token}"


def test_qe_mcp_direct_script_entrypoints_start_without_import_error() -> None:
    env = os.environ.copy()
    env.update(
        {
            "AISTOCK_QE_EXPERIMENT_BASE_URL": "http://127.0.0.1/api/v1",
            "AISTOCK_QE_ARCHIVE_BASE_URL": "http://127.0.0.1/api/v1/qe-archive",
        }
    )
    for rel in ("scripts/aistock_qe_experiment_mcp_server.py", "scripts/aistock_qe_archive_mcp_server.py"):
        completed = subprocess.run(
            [sys.executable, rel],
            input=b"",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            timeout=10,
            check=False,
        )
        stderr = completed.stderr.decode("utf-8", errors="replace")
        assert completed.returncode == 0, stderr
        assert "ModuleNotFoundError" not in stderr
