from __future__ import annotations

import asyncio

import pytest
from fastapi import BackgroundTasks, HTTPException

from backend.routers import quantevolver_evolution as evolution_router
from backend.services.quantevolver import qe_active_dataset_profile as profile_module


def _request(pool_ids: list[str]) -> evolution_router.UniverseComparisonCreateRequest:
    return evolution_router.UniverseComparisonCreateRequest(
        task_name="universe comparison",
        pool_ids=pool_ids,
        node_id="wsl2-5080",
        base_loop=evolution_router.CustomEvoLoopConfig(
            factor_keys=["demo||catalog"],
            model_id="model_lgbm_v1",
            strategy_id="strategy_topk",
            strategy_params={"topk": 20, "n_drop": 5},
            runtime_flags={"random_seed": 123},
            execution_algo="TWAP",
            label_horizon=20,
        ),
    )


def test_universe_comparison_expands_only_the_universe(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    async def fake_create(req, _background_tasks):
        captured["request"] = req
        return {"status": "success", "task_id": "qe_cmp_task", "total_loops": len(req.loops)}

    monkeypatch.setattr(evolution_router, "create_custom_evolution_task", fake_create)
    result = asyncio.run(
        evolution_router.create_universe_comparison_task(
            _request(["csi500", "csi300"]),
            BackgroundTasks(),
        )
    )

    assert result["comparison_mode"] == "separate_runs"
    assert result["comparison_task_id"] == "qe_cmp_task"
    loops = captured["request"].loops
    assert [loop.label for loop in loops] == ["universe:csi300", "universe:csi500"]
    assert [loop.universe_selection for loop in loops] == [
        {"mode": "single_index", "pool_ids": ["csi300"]},
        {"mode": "single_index", "pool_ids": ["csi500"]},
    ]
    comparable = []
    for loop in loops:
        value = loop.model_dump() if hasattr(loop, "model_dump") else loop.dict()
        value.pop("label", None)
        value.pop("universe_selection", None)
        flags = dict(value.pop("runtime_flags") or {})
        flags.pop("qe_universe_comparison", None)
        value["runtime_flags"] = flags
        comparable.append(value)
    assert comparable[0] == comparable[1]
    group_ids = {
        loop.runtime_flags["qe_universe_comparison"]["comparison_group_id"]
        for loop in loops
    }
    assert group_ids == {result["comparison_group_id"]}


def test_universe_comparison_rejects_duplicate_or_preselected_pools() -> None:
    with pytest.raises(HTTPException, match="unique"):
        asyncio.run(
            evolution_router.create_universe_comparison_task(
                _request(["csi300", "csi300"]),
                BackgroundTasks(),
            )
        )


def test_universe_comparison_records_explicit_star50_top20_arm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    async def fake_create(req, _background_tasks):
        captured["request"] = req
        return {"status": "success", "task_id": "qe_cmp_task", "total_loops": len(req.loops)}

    monkeypatch.setattr(evolution_router, "create_custom_evolution_task", fake_create)
    request = _request(["star50", "csi300"])
    request.topk_by_pool = {"csi300": 50, "star50": 20}

    result = asyncio.run(
        evolution_router.create_universe_comparison_task(request, BackgroundTasks())
    )

    loops = captured["request"].loops
    by_pool = {loop.universe_selection["pool_ids"][0]: loop for loop in loops}
    assert by_pool["star50"].strategy_params["topk"] == 20
    assert by_pool["csi300"].strategy_params["topk"] == 50
    assert {arm["pool_id"]: arm["topk"] for arm in result["arms"]} == {
        "csi300": 50,
        "star50": 20,
    }


def test_universe_comparison_rejects_unknown_topk_override() -> None:
    request = _request(["csi300", "csi500"])
    request.topk_by_pool = {"star50": 20}
    with pytest.raises(HTTPException, match="outside pool_ids"):
        asyncio.run(
            evolution_router.create_universe_comparison_task(request, BackgroundTasks())
        )

    request = _request(["csi300", "csi500"])
    request.base_loop.universe_selection = {"mode": "single_index", "pool_ids": ["csi300"]}
    with pytest.raises(HTTPException, match="must omit"):
        asyncio.run(
            evolution_router.create_universe_comparison_task(
                request,
                BackgroundTasks(),
            )
        )


def test_universe_comparison_rejects_star50_inheriting_explicit_top50() -> None:
    request = _request(["star50", "csi300"])
    request.base_loop.strategy_params["topk"] = 50

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            evolution_router.create_universe_comparison_task(
                request,
                BackgroundTasks(),
            )
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["reason_code"] == "qe_star50_topk_required"
    assert exc_info.value.detail["context"]["pool_id"] == "star50"


def test_strategy_fork_rejects_star50_non_top20_before_scheduler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def fake_strategy_fork_task(**_kwargs):
        nonlocal called
        called = True
        return "qe_fork"

    monkeypatch.setattr(evolution_router, "ensure_qe_label_horizon_schema", lambda: None)
    monkeypatch.setattr(evolution_router.scheduler, "strategy_fork_task", fake_strategy_fork_task)
    request = evolution_router.StrategyEvolutionForkRequest(
        from_loop_index=1,
        loops=[
            evolution_router.StrategyLoopConfig(
                strategy_params={"topk": 50},
                universe_selection={"mode": "single_index", "pool_ids": ["star50"]},
                execution_algo="TWAP",
            )
        ],
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(evolution_router.strategy_fork_task("qe_source", request))

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["reason_code"] == "qe_star50_topk_required"
    assert called is False


def test_custom_evo_rerun_reuses_persisted_binding_without_reading_active_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    split = {
        "train_start": "2018-08-01",
        "train_end": "2022-12-31",
        "valid_start": "2023-01-01",
        "valid_end": "2024-06-30",
        "test_start": "2024-07-01",
        "test_end": "2026-08-31",
        "backtest_end": "2026-08-28",
    }
    binding = {
        "schema_version": "qe_direct_v2_dataset_binding_v3",
        "selection_pins": {
            "mode": "single_index",
            "pool_ids": ["csi300"],
            "instrument_name": "index_pool__csi300",
        },
    }
    persisted = {
        "loop_index": 1,
        "node_id": "wsl2-5080",
        "data_split": split,
        "stock_pool": "index_pool__csi300",
        "custom_params": {
            "_qe_direct_v2_dataset_binding": binding,
            "_qe_active_dataset_summary": {
                "generation": "generation-1",
                "release_id": "release-1",
                "cutoff": "2026-08-31",
            },
        },
    }

    monkeypatch.setattr(
        evolution_router,
        "resolve_custom_loop_nodes",
        lambda loops, _node: (
            [{**loop, "node_id": "wsl2-5080"} for loop in loops],
            "wsl2-5080",
            {"wsl2-5080"},
        ),
    )

    async def fake_preflight(_node_ids):
        return []

    monkeypatch.setattr(evolution_router, "preflight_qe_nodes", fake_preflight)
    monkeypatch.setattr(
        profile_module,
        "load_active_qe_profile",
        lambda: (_ for _ in ()).throw(AssertionError("rerun must not read the active profile")),
    )

    loops, _node_id, _parallelism = asyncio.run(
        evolution_router._prepare_custom_evo_loop_configs(
            [_request(["csi300", "csi500"]).base_loop],
            request_node_id="wsl2-5080",
            node_parallelism_payload=None,
            assigned_loop_indexes=[1],
            persisted_loop_configs={1: persisted},
        )
    )

    assert loops[0]["data_split"] == split
    assert loops[0]["custom_params"]["_qe_direct_v2_dataset_binding"] == binding
    public = evolution_router._public_custom_evo_config({"loops": [loops[0]]})
    serialized = repr(public)
    assert "_qe_direct_v2_dataset_binding" not in serialized
    assert "/client/path" not in serialized
    assert public["loops"][0]["universe_selection"] == {
        "mode": "single_index",
        "pool_ids": ["csi300"],
    }
    assert "stock_pool" not in public["loops"][0]
    round_trip = evolution_router.CustomEvoLoopConfig.model_validate(public["loops"][0])
    assert round_trip.stock_pool is None
    assert round_trip.universe_selection == {
        "mode": "single_index",
        "pool_ids": ["csi300"],
    }


def test_custom_evo_persisted_star50_binding_defaults_and_rejects_top50(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted = {
        "loop_index": 1,
        "node_id": "wsl2-5080",
        "data_split": {"test_end": "2026-08-31", "backtest_end": "2026-08-28"},
        "stock_pool": "index_pool__star50",
        "custom_params": {
            "_qe_direct_v2_dataset_binding": {
                "schema_version": "qe_direct_v2_dataset_binding_v3",
                "selection_pins": {
                    "mode": "single_index",
                    "pool_ids": ["star50"],
                    "instrument_name": "index_pool__star50",
                },
            }
        },
    }
    monkeypatch.setattr(
        evolution_router,
        "resolve_custom_loop_nodes",
        lambda loops, _node: (
            [{**loop, "node_id": "wsl2-5080"} for loop in loops],
            "wsl2-5080",
            {"wsl2-5080"},
        ),
    )

    async def fake_preflight(_node_ids):
        return {}

    monkeypatch.setattr(evolution_router, "preflight_qe_nodes", fake_preflight)
    base = evolution_router.CustomEvoLoopConfig(
        factor_keys=["demo||catalog"],
        model_id="model_lgbm_v1",
        strategy_params={},
        runtime_flags={"random_seed": 123},
        label_horizon=20,
    )
    loops, _node_id, _parallelism = asyncio.run(
        evolution_router._prepare_custom_evo_loop_configs(
            [base],
            request_node_id="wsl2-5080",
            node_parallelism_payload=None,
            assigned_loop_indexes=[1],
            persisted_loop_configs={1: persisted},
        )
    )
    assert loops[0]["strategy_params"]["topk"] == 20

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            evolution_router._prepare_custom_evo_loop_configs(
                [base.model_copy(update={"strategy_params": {"topk": 50}})],
                request_node_id="wsl2-5080",
                node_parallelism_payload=None,
                assigned_loop_indexes=[1],
                persisted_loop_configs={1: persisted},
            )
        )
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["reason_code"] == "qe_star50_topk_required"


def test_public_custom_evo_config_preserves_legacy_stock_pool_without_binding() -> None:
    public = evolution_router._public_custom_evo_config(
        {
            "loops": [
                {
                    "loop_index": 1,
                    "stock_pool": "filtered_pool_20260630",
                    "custom_params": {"stock_pool": "filtered_pool_20260630"},
                }
            ]
        }
    )

    assert public["loops"][0]["stock_pool"] == "filtered_pool_20260630"
    assert "universe_selection" not in public["loops"][0]
    assert "custom_params" not in public["loops"][0]


def test_new_custom_evo_still_rejects_conflicting_universe_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = _request(["csi300", "csi500"]).base_loop.model_copy(
        update={
            "stock_pool": "stock_universe",
            "universe_selection": {"mode": "single_index", "pool_ids": ["csi300"]},
        }
    )
    monkeypatch.setattr(
        evolution_router,
        "resolve_custom_loop_nodes",
        lambda loops, _node: (
            [{**item, "node_id": "wsl2-5080"} for item in loops],
            "wsl2-5080",
            {"wsl2-5080"},
        ),
    )

    async def fake_preflight(_node_ids):
        return {}

    monkeypatch.setattr(evolution_router, "preflight_qe_nodes", fake_preflight)
    monkeypatch.setattr(profile_module, "load_active_qe_profile", lambda: object())

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            evolution_router._prepare_custom_evo_loop_configs(
                [loop],
                request_node_id="wsl2-5080",
                node_parallelism_payload=None,
            )
        )

    assert exc_info.value.status_code == 400
    assert "stock_pool and universe_selection cannot be supplied together" in str(
        exc_info.value.detail
    )
