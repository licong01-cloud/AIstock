import asyncio

from starlette.background import BackgroundTasks

import pytest
from fastapi import HTTPException

from backend.routers import quantevolver_evolution as qe


class DummyScheduler:
    def __init__(self):
        self.calls = []

    async def get_custom_evo_editable_config(self, task_id):
        self.calls.append(("config", task_id))
        return {"task_id": task_id, "node_id": "existing-node", "loops": []}

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


def _loop(label="Loop A"):
    return qe.CustomEvoLoopConfig(
        label=label,
        factor_keys=["alpha_factor||catalog"],
        model_id="xgboost_v1",
        execution_algo="CLOSE_PRICE",
        label_horizon=5,
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
    monkeypatch.setattr(
        qe,
        "normalize_node_parallelism",
        lambda node_ids, payload: {node_id: int((payload or {}).get(node_id, 1)) for node_id in node_ids},
    )
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
