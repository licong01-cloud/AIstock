"""Unit tests for QE MCP thin HTTP wrappers."""

from __future__ import annotations

import importlib
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
