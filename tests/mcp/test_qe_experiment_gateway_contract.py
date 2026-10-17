from __future__ import annotations

import asyncio
import json

import httpx

from backend.mcp.gateway import create_gateway
from backend.mcp.tool_manifest import MODULE_TOOL_NAMES


MIGRATED_TOOLS = {
    "qe_dataset_profile_get",
    "qe_single_experiment_template_create",
    "qe_universe_comparison_task_create",
}


def _run(coro):
    return asyncio.run(coro)


def test_qe_gateway_manifest_contains_three_migrated_tools() -> None:
    assert MIGRATED_TOOLS <= set(MODULE_TOOL_NAMES["qe_experiment"])


def test_qe_dataset_profile_get_uses_canonical_endpoint() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url)))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="qe", transport=httpx.MockTransport(handler))
        await mcp.call_tool("qe_dataset_profile_get", {})

    _run(exercise())
    assert seen == [("GET", "http://127.0.0.1:8001/api/v1/quantevolver/dataset-profile")]


def test_qe_single_experiment_template_create_preserves_human_contract() -> None:
    seen: list[tuple[str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url), json.loads(request.content)))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="qe", transport=httpx.MockTransport(handler))
        await mcp.call_tool(
            "qe_single_experiment_template_create",
            {
                "title": "pool comparison baseline",
                "factor_names": ["Alpha001"],
                "model_id": "model_lgbm_v1",
                "strategy_id": "TopkDropoutStrategy",
                "node_id": "wsl2-5080",
                "universe_mode": "index_pool",
                "pool_ids": ["000300.SH"],
                "test_start": "2026-01-05",
                "test_end": "2026-08-31",
                "random_seed": 314,
            },
        )

    _run(exercise())
    assert seen == [
        (
            "POST",
            "http://127.0.0.1:8001/api/v1/qe-templates",
            {
                "template_kind": "single_experiment",
                "title": "pool comparison baseline",
                "description": None,
                "config_json": {
                    "factor_names": ["Alpha001"],
                    "model_id": "model_lgbm_v1",
                    "strategy_id": "TopkDropoutStrategy",
                    "node_id": "wsl2-5080",
                    "universe_selection": {
                        "mode": "index_pool",
                        "pool_ids": ["000300.SH"],
                    },
                    "custom_params": {"random_seed": 314},
                    "data_split": {
                        "test_start": "2026-01-05",
                        "test_end": "2026-08-31",
                    },
                },
                "archive_policy": "AUTO",
            },
        )
    ]


def test_qe_universe_comparison_task_create_preserves_arm_contract() -> None:
    seen: list[tuple[str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url), json.loads(request.content)))
        return httpx.Response(200, json={"ok": True})

    async def exercise() -> None:
        mcp, _registry = create_gateway(profile="qe", transport=httpx.MockTransport(handler))
        await mcp.call_tool(
            "qe_universe_comparison_task_create",
            {
                "task_name": "same strategy pools",
                "pool_ids": ["000300.SH", "000905.SH"],
                "factor_keys": ["Alpha001"],
                "model_id": "model_lgbm_v1",
                "node_id": "rdagent-node1",
                "random_seed": 2718,
                "topk": 20,
                "n_drop": 2,
                "label_horizon": 20,
                "execution_algo": "TWAP",
                "test_end": "2026-08-31",
            },
        )

    _run(exercise())
    assert seen == [
        (
            "POST",
            "http://127.0.0.1:8001/api/v1/quantevolver/evolution/universe-comparison-tasks",
            {
                "task_name": "same strategy pools",
                "pool_ids": ["000300.SH", "000905.SH"],
                "topk_by_pool": {"000300.SH": 20, "000905.SH": 20},
                "base_loop": {
                    "factor_keys": ["Alpha001"],
                    "model_id": "model_lgbm_v1",
                    "strategy_id": None,
                    "strategy_params": {"topk": 20, "n_drop": 2},
                    "runtime_flags": {"random_seed": 2718},
                    "label_horizon": 20,
                    "execution_algo": "TWAP",
                    "node_id": "rdagent-node1",
                    "data_split": {"test_end": "2026-08-31"},
                },
                "node_id": "rdagent-node1",
                "auto_start": False,
            },
        )
    ]
