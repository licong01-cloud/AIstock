import asyncio

from starlette.background import BackgroundTasks

import pytest
from fastapi import HTTPException

from backend.routers import quantevolver_evolution as qe


class DummyScheduler:
    def __init__(self, editable_config=None):
        self.calls = []
        self.editable_config = editable_config or {"task_id": "task-a", "node_id": "existing-node", "loops": []}

    async def get_custom_evo_editable_config(self, task_id):
        self.calls.append(("config", task_id))
        return {**self.editable_config, "task_id": task_id}

    async def create_custom_evo_task(self, **kwargs):
        self.calls.append(("create", kwargs))
        return "new-custom-task"

    async def rerun_custom_evo_loop(self, **kwargs):
        self.calls.append(("rerun", kwargs))
        return {
            "task_id": kwargs["task_id"],
            "loop_index": kwargs["loop_index"],
            "loop_id": f"{kwargs['task_id']}_Loop{kwargs['loop_index']}",
        }

    async def append_custom_evo_loops(self, **kwargs):
        self.calls.append(("append", kwargs))
        return {
            "task_id": kwargs["task_id"],
            "new_loop_indexes": [4, 5],
            "total_loops": 5,
        }

    async def submit_custom_evo_selected_loops(self, *args, **kwargs):
        self.calls.append(("selected", args, kwargs))
        return {"submitted_loop_ids": []}


def _loop(label="Loop A", node_id=None, stock_pool=None, random_seed=20260522):
    runtime_flags = {"random_seed": random_seed} if random_seed is not None else None
    return qe.CustomEvoLoopConfig(
        label=label,
        factor_keys=["alpha_factor||catalog"],
        model_id="xgboost_v1",
        execution_algo="CLOSE_PRICE",
        label_horizon=5,
        node_id=node_id,
        stock_pool=stock_pool,
        runtime_flags=runtime_flags,
    )


def _patch_non_qe_dependencies(monkeypatch):
    def fake_resolve_custom_loop_nodes(loops_config, request_node_id):
        default_node = request_node_id or "local-node"
        resolved = []
        selected = set()
        for loop_cfg in loops_config:
            next_cfg = dict(loop_cfg)
            next_cfg["node_id"] = next_cfg.get("node_id") or default_node
            selected.add(next_cfg["node_id"])
            resolved.append(next_cfg)
        return resolved, resolved[0]["node_id"], selected

    async def fake_preflight_qe_nodes(node_ids):
        return {
            node_id: {"node_id": node_id, "api_base_url": "http://127.0.0.1:59999"}
            for node_id in node_ids
        }

    monkeypatch.setattr(qe, "ensure_qe_label_horizon_schema", lambda: None)
    monkeypatch.setattr(qe, "resolve_custom_loop_nodes", fake_resolve_custom_loop_nodes)
    def fake_normalize_node_parallelism(node_ids, payload):
        selected = {str(node_id) for node_id in node_ids}
        payload = payload or {}
        unknown = sorted(set(payload) - selected)
        if unknown:
            raise qe.QENodePreflightError(
                "QE_NODE_PARALLELISM_UNKNOWN_NODE",
                "node_parallelism contains nodes that are not selected by any loop.",
                {"unknown_node_ids": unknown, "selected_node_ids": sorted(selected)},
            )
        return {node_id: int(payload.get(node_id, 1)) for node_id in selected}

    monkeypatch.setattr(qe, "normalize_node_parallelism", fake_normalize_node_parallelism)
    monkeypatch.setattr(qe, "preflight_qe_nodes", fake_preflight_qe_nodes)
    monkeypatch.setattr(qe, "_sync_stock_pool_to_remote", lambda stock_pool, node: None)


def test_custom_evo_rerun_route_requires_explicit_delete_confirmation(monkeypatch):
    _patch_non_qe_dependencies(monkeypatch)
    dummy = DummyScheduler()
    monkeypatch.setattr(qe, "scheduler", dummy)

    req = qe.CustomEvoLoopRerunRequest(loop=_loop(), confirm_delete_old_result=False)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(qe.rerun_custom_evo_loop("task-a", 2, req, BackgroundTasks()))

    assert exc.value.status_code == 400
    assert dummy.calls == []


def test_custom_evo_rerun_route_schedules_only_target_loop(monkeypatch):
    _patch_non_qe_dependencies(monkeypatch)
    dummy = DummyScheduler()
    monkeypatch.setattr(qe, "scheduler", dummy)
    background_tasks = BackgroundTasks()

    req = qe.CustomEvoLoopRerunRequest(
        loop=_loop("replacement"),
        execution_mode="parallel_2",
        node_id="node-a",
        node_parallelism={"node-a": 1},
        confirm_delete_old_result=True,
    )
    result = asyncio.run(qe.rerun_custom_evo_loop("task-a", 2, req, background_tasks))

    assert result["status"] == "success"
    assert dummy.calls[0][0] == "config"
    assert dummy.calls[1][0] == "rerun"
    assert dummy.calls[1][1]["loop_index"] == 2
    assert dummy.calls[1][1]["loop_config"]["loop_index"] == 2
    assert dummy.calls[1][1]["loop_config"]["node_id"] == "node-a"
    assert len(background_tasks.tasks) == 1
    assert background_tasks.tasks[0].args == ("task-a", [2])


def test_custom_evo_rerun_route_rejects_seedless_trainable_loop(monkeypatch):
    _patch_non_qe_dependencies(monkeypatch)
    dummy = DummyScheduler()
    monkeypatch.setattr(qe, "scheduler", dummy)

    req = qe.CustomEvoLoopRerunRequest(
        loop=_loop("seedless", random_seed=None),
        confirm_delete_old_result=True,
    )
    with pytest.raises(HTTPException) as exc:
        asyncio.run(qe.rerun_custom_evo_loop("task-a", 2, req, BackgroundTasks()))

    assert exc.value.status_code == 400
    assert "runtime_flags.random_seed" in str(exc.value.detail)
    assert [call[0] for call in dummy.calls] == ["config"]


def test_custom_evo_rerun_keeps_full_distributed_parallelism(monkeypatch):
    _patch_non_qe_dependencies(monkeypatch)
    dummy = DummyScheduler(
        {
            "task_id": "task-a",
            "node_id": "node-a",
            "node_parallelism": {"node-a": 2, "node-b": 3},
            "loops": [
                {"loop_index": 1, "node_id": "node-a"},
                {"loop_index": 2, "node_id": "node-b"},
            ],
        }
    )
    monkeypatch.setattr(qe, "scheduler", dummy)
    background_tasks = BackgroundTasks()

    req = qe.CustomEvoLoopRerunRequest(
        loop=_loop("replacement", node_id="node-b"),
        execution_mode="parallel_2",
        node_parallelism={"node-a": 2, "node-b": 3},
        confirm_delete_old_result=True,
    )
    result = asyncio.run(qe.rerun_custom_evo_loop("task-a", 2, req, background_tasks))

    assert result["status"] == "success"
    assert dummy.calls[1][0] == "rerun"
    assert dummy.calls[1][1]["node_parallelism"] == {"node-a": 2, "node-b": 3}
    assert dummy.calls[1][1]["loop_config"]["node_id"] == "node-b"
    assert background_tasks.tasks[0].args == ("task-a", [2])


def test_custom_evo_append_route_schedules_only_new_loop_indexes(monkeypatch):
    _patch_non_qe_dependencies(monkeypatch)
    dummy = DummyScheduler()
    monkeypatch.setattr(qe, "scheduler", dummy)
    background_tasks = BackgroundTasks()

    req = qe.CustomEvoAppendRequest(
        loops=[_loop("append-1"), _loop("append-2")],
        execution_mode="parallel_2",
        ack_failed_loop_warning=True,
    )
    result = asyncio.run(qe.append_custom_evo_loops("task-a", req, background_tasks))

    assert result["status"] == "success"
    assert result["new_loop_indexes"] == [4, 5]
    assert dummy.calls[0][0] == "config"
    assert dummy.calls[1][0] == "append"
    assert [loop["loop_index"] for loop in dummy.calls[1][1]["loops_config"]] == [1, 2]
    assert {loop["node_id"] for loop in dummy.calls[1][1]["loops_config"]} == {"existing-node"}
    assert len(background_tasks.tasks) == 1
    assert background_tasks.tasks[0].args == ("task-a", [4, 5])


def test_custom_evo_append_keeps_existing_distributed_parallelism(monkeypatch):
    _patch_non_qe_dependencies(monkeypatch)
    dummy = DummyScheduler(
        {
            "task_id": "task-a",
            "node_id": "node-a",
            "node_parallelism": {"node-a": 2, "node-b": 3},
            "loops": [
                {"loop_index": 1, "node_id": "node-a"},
                {"loop_index": 2, "node_id": "node-b"},
            ],
        }
    )
    monkeypatch.setattr(qe, "scheduler", dummy)
    background_tasks = BackgroundTasks()

    req = qe.CustomEvoAppendRequest(
        loops=[_loop("append-on-b", node_id="node-b")],
        execution_mode="parallel_2",
        node_parallelism={"node-a": 2, "node-b": 3},
        ack_failed_loop_warning=True,
    )
    result = asyncio.run(qe.append_custom_evo_loops("task-a", req, background_tasks))

    assert result["status"] == "success"
    assert dummy.calls[1][0] == "append"
    assert dummy.calls[1][1]["node_parallelism"] == {"node-a": 2, "node-b": 3}
    assert dummy.calls[1][1]["loops_config"][0]["node_id"] == "node-b"
    assert background_tasks.tasks[0].args == ("task-a", [4, 5])


def test_prepare_custom_evo_loop_configs_syncs_each_stock_pool_once_per_node(monkeypatch):
    _patch_non_qe_dependencies(monkeypatch)
    sync_calls = []
    monkeypatch.setattr(
        qe,
        "_sync_stock_pool_to_remote",
        lambda stock_pool, node: sync_calls.append((stock_pool, node["node_id"])),
    )

    loops_config, loop1_node_id, node_parallelism = asyncio.run(
        qe._prepare_custom_evo_loop_configs(
            [
                _loop("a", node_id="node-a", stock_pool="filtered_pool_20260428"),
                _loop("b", node_id="node-a", stock_pool="filtered_pool_20260428"),
                _loop("c", node_id="node-b", stock_pool="filtered_pool_20260428"),
            ],
            request_node_id="node-a",
            node_parallelism_payload={"node-a": 2, "node-b": 1},
        )
    )

    assert loop1_node_id == "node-a"
    assert node_parallelism == {"node-a": 2, "node-b": 1}
    assert [loop["node_id"] for loop in loops_config] == ["node-a", "node-a", "node-b"]
    assert sync_calls == [
        ("filtered_pool_20260428", "node-a"),
        ("filtered_pool_20260428", "node-b"),
    ]


def test_prepare_custom_evo_loop_configs_rejects_future_stock_pool_before_sync(monkeypatch):
    _patch_non_qe_dependencies(monkeypatch)
    sync_calls = []
    monkeypatch.setattr(
        qe,
        "_sync_stock_pool_to_remote",
        lambda stock_pool, node: sync_calls.append((stock_pool, node["node_id"])),
    )

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            qe._prepare_custom_evo_loop_configs(
                [
                    _loop("future-pool", node_id="node-a", stock_pool="filtered_pool_20260519"),
                ],
                request_node_id="node-a",
                node_parallelism_payload={"node-a": 1},
            )
        )

    assert exc.value.status_code == 400
    assert "QE_STOCK_POOL_DATE_OUT_OF_WINDOW" in str(exc.value.detail)
    assert sync_calls == []


def test_custom_evo_clone_create_keeps_loop_nodes_and_parallelism(monkeypatch):
    _patch_non_qe_dependencies(monkeypatch)
    dummy = DummyScheduler()
    monkeypatch.setattr(qe, "scheduler", dummy)

    req = qe.CustomEvolutionCreateRequest(
        task_name="clone task",
        target_desc="clone",
        loops=[_loop("clone-a", node_id="node-a"), _loop("clone-b", node_id="node-b")],
        execution_mode="parallel_2",
        node_parallelism={"node-a": 2, "node-b": 3},
        clone_from_task_id="source-task",
    )
    result = asyncio.run(qe.create_custom_evolution_task(req, BackgroundTasks()))

    assert result["status"] == "success"
    assert result["task_id"] == "new-custom-task"
    assert result["node_assignments"] == [
        {"loop_index": 1, "node_id": "node-a"},
        {"loop_index": 2, "node_id": "node-b"},
    ]
    assert result["node_parallelism"] == {"node-a": 2, "node-b": 3}
    assert dummy.calls[0][0] == "create"
    assert dummy.calls[0][1]["clone_from_task_id"] == "source-task"
    loops_config = dummy.calls[0][1]["loops_config"]
    assert all(loop["strategy_params"]["risk_policy"]["enabled"] is True for loop in loops_config)
    assert all("force_exit" in loop["strategy_params"]["risk_policy"]["hard_actions"] for loop in loops_config)
