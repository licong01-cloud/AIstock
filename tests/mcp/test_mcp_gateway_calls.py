from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from mcp.server.fastmcp.exceptions import ToolError

from backend.mcp.gateway import create_gateway


def _run(coro):
    return asyncio.run(coro)


def test_validation_gateway_health_calls_loopback_validation_api() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url)))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="validation", transport=httpx.MockTransport(handler))
        await mcp.call_tool("health", {})

    _run(exercise())
    assert seen == [("GET", "http://127.0.0.1:8001/api/v1/validation/health")]


def test_qe_gateway_confirmed_run_requires_confirmation_and_uses_backend_path() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url)))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="qe", transport=httpx.MockTransport(handler))
        with pytest.raises(ToolError, match="confirm_run must equal"):
            await mcp.call_tool("qe_experiment_run_confirmed", {"experiment_id": "exp-1"})
        await mcp.call_tool(
            "qe_experiment_run_confirmed",
            {"experiment_id": "exp-1", "node_id": "node-1", "confirm_run": "QE_EXPERIMENT_RUN"},
        )

    _run(exercise())
    assert seen == [("POST", "http://127.0.0.1:8001/api/v1/quantevolver/experiments/exp-1/run?engine_mode=unified&node_id=node-1")]


def test_qe_gateway_create_and_run_requires_confirmation_and_posts_direct_endpoint() -> None:
    seen: list[tuple[str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url), json.loads(request.content.decode("utf-8"))))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="qe", transport=httpx.MockTransport(handler))
        payload = {
            "template_kind": "single_experiment",
            "title": "direct smoke",
            "config_json": {
                "factor_names": ["Alpha001"],
                "model_id": "model_lgbm_v1",
                "custom_params": {"random_seed": 42},
            },
        }
        with pytest.raises(ToolError, match="confirm_direct_run must equal"):
            await mcp.call_tool("qe_template_create_and_run_confirmed", payload)
        await mcp.call_tool(
            "qe_template_create_and_run_confirmed",
            {
                **payload,
                "node_id": "node-1",
                "confirm_direct_run": "QE_TEMPLATE_CREATE_AND_RUN",
                "approval_note": "unit direct run",
            },
        )

    _run(exercise())
    assert seen == [
        (
            "POST",
            "http://127.0.0.1:8001/api/v1/qe-templates/create-and-run",
            {
                "template_kind": "single_experiment",
                "title": "direct smoke",
                "description": None,
                "config_json": {
                    "factor_names": ["Alpha001"],
                    "model_id": "model_lgbm_v1",
                    "custom_params": {"random_seed": 42},
                },
                "archive_policy": "AUTO",
                "confirm_direct_run": "QE_TEMPLATE_CREATE_AND_RUN",
                "node_id": "node-1",
                "force_full_train": False,
                "approved_by": "mcp_gateway",
                "approval_note": "unit direct run",
            },
        )
    ]


def test_qe_archive_confirmed_backfill_requires_confirmation_and_uses_backend_path() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url)))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="qe", transport=httpx.MockTransport(handler))
        with pytest.raises(ToolError, match="confirm_backfill must equal"):
            await mcp.call_tool("qe_archive_backfill_execute_confirmed", {})
        await mcp.call_tool("qe_archive_backfill_execute_confirmed", {"confirm_backfill": "QE_ARCHIVE_BACKFILL"})

    _run(exercise())
    assert seen == [("POST", "http://127.0.0.1:8001/api/v1/qe-archive/backfill/execute")]


def test_paper_v2_stable_profile_calls_stable_backend_facades() -> None:
    seen: list[tuple[str, str, dict | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8")) if request.content else None
        seen.append((request.method, str(request.url), body))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="paper_v2_stable", transport=httpx.MockTransport(handler))
        await mcp.call_tool("strategy_packages_list", {"payload": {"status": "ACTIVE", "limit": 10}})
        await mcp.call_tool("selection_center_list_runs", {"payload": {"limit": 5}})
        await mcp.call_tool("advisory_list_programs", {"payload": {"include_archived": False}})
        await mcp.call_tool("paper_v2_monitoring_running_summary", {"payload": {"limit": 3, "position_limit": 2}})
        await mcp.call_tool("qmt_broker_monitoring_get_snapshot", {})

    _run(exercise())
    assert seen == [
        ("GET", "http://127.0.0.1:8001/api/v1/strategy-packages/?status=ACTIVE&limit=10", None),
        ("GET", "http://127.0.0.1:8001/api/v1/selection-center/runs?limit=5", None),
        ("GET", "http://127.0.0.1:8001/api/v1/advisory/programs?include_archived=false", None),
        (
            "GET",
            "http://127.0.0.1:8001/api/v1/paper-v2/running-summary?limit=3&page=1&snapshot_limit=30&position_limit=2&sort_by=latest_run_time&sort_dir=desc",
            None,
        ),
        ("GET", "http://127.0.0.1:8001/api/v1/qmt/snapshot", None),
    ]


def test_paper_v2_confirmed_tools_require_confirmation_before_http() -> None:
    seen: list[tuple[str, str, dict | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8")) if request.content else None
        seen.append((request.method, str(request.url), body))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="paper_v2_stable", transport=httpx.MockTransport(handler))
        with pytest.raises(ToolError, match="confirm must equal"):
            await mcp.call_tool("strategy_packages_create_from_qe_experiment_confirmed", {"payload": {"experiment_id": "exp-1"}})
        with pytest.raises(ToolError, match="confirm must equal"):
            await mcp.call_tool("selection_center_run_confirmed", {"payload": {"package_ids": ["pkg-1"]}})
        with pytest.raises(ToolError, match="confirm must equal"):
            await mcp.call_tool("advisory_create_program_confirmed", {"payload": {"program_name": "p1"}})
        assert seen == []
        await mcp.call_tool(
            "strategy_packages_create_from_qe_experiment_confirmed",
            {"payload": {"experiment_id": "exp-1", "resolve_runtime_assets": False, "confirm": "CREATE_STRATEGY_PACKAGE"}},
        )

    _run(exercise())
    assert seen == [
        (
            "POST",
            "http://127.0.0.1:8001/api/v1/strategy-packages/from-qe-experiment",
            {"experiment_id": "exp-1", "resolve_runtime_assets": False},
        )
    ]
