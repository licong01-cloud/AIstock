from __future__ import annotations

import asyncio
import inspect
from collections import OrderedDict

import pytest

from backend.schedulers import node_health_scheduler as scheduler_module
from backend.routers import dispatch as dispatch_router
from backend.services import dispatch_service as dispatch_module
from backend.services.dispatch_service import DispatchService


@pytest.fixture(autouse=True)
def _reset_observation_bus() -> None:
    with DispatchService._observation_condition:
        DispatchService._observation_generation = 0
        DispatchService._task_observations = OrderedDict()
        DispatchService._node_observations = OrderedDict()
    DispatchService._sync_fail_counts.clear()


def _task(**overrides):
    value = {
        "task_id": "dispatch-1",
        "task_name": "quiet",
        "task_type": "correlation_compute",
        "node_id": "node-1",
        "status": "running",
        "remote_task_id": "remote-1",
        "current_loop": 2,
        "total_loops": 10,
        "progress_pct": 20.0,
        "best_ic": None,
        "best_sharpe": None,
        "best_ann_return": None,
        "best_max_dd": None,
        "log_tail": "steady",
    }
    value.update(overrides)
    return value


def _node(**overrides):
    value = {
        "node_id": "node-1",
        "api_base_url": "http://node.invalid",
        "status": "busy",
        "created_at": "2026-08-12T00:00:00Z",
    }
    value.update(overrides)
    return value


class _Cursor:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, _params=None):
        self.calls.append(" ".join(str(sql).split()))


class _Connection:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return _Cursor(self.calls)


def test_observation_bus_deduplicates_100_identical_publications() -> None:
    task = _task()
    first_generation = DispatchService.publish_task_observation(task)

    for _ in range(100):
        assert DispatchService.publish_task_observation(dict(task)) == first_generation

    generation, observed = DispatchService.wait_for_task_observation(
        task["task_id"],
        after_generation=first_generation,
        timeout=0,
    )
    assert generation == first_generation
    assert observed == task


def test_observation_cache_is_bounded_and_node_overlay_is_memory_only(monkeypatch) -> None:
    monkeypatch.setattr(DispatchService, "_OBSERVATION_CACHE_LIMIT", 3)
    for index in range(5):
        DispatchService.publish_task_observation(_task(task_id=f"dispatch-{index}"))
    assert list(DispatchService._task_observations) == ["dispatch-2", "dispatch-3", "dispatch-4"]

    DispatchService.publish_node_observation({
        "node_id": "node-1",
        "status": "online",
        "online": True,
        "busy": False,
        "error": None,
        "observed_at": "2026-08-12T01:00:00Z",
    })
    overlaid = DispatchService._overlay_node_observation(_node(status="offline"))
    assert overlaid["status"] == "online"
    assert overlaid["observed_at"] == "2026-08-12T01:00:00Z"


def test_unchanged_remote_progress_performs_no_dml_or_event(monkeypatch) -> None:
    task = _task()
    snapshot = {"nodes": [_node()], "tasks": [task]}
    calls = {"progress": 0, "update": 0, "event": 0}

    class Client:
        def __init__(self, _base_url: str):
            pass

        async def get_task_progress(self, _remote_task_id: str):
            calls["progress"] += 1
            return {
                "status": "running",
                "current_loop": 2,
                "total_loops": 10,
                "progress_pct": 20.0,
                "log_tail": "steady",
            }

    monkeypatch.setattr(dispatch_module, "ComputeNodeClient", Client)
    svc = DispatchService()
    monkeypatch.setattr(
        svc,
        "_update_observed_task_fields",
        lambda *_args, **_kwargs: calls.__setitem__("update", calls["update"] + 1),
    )
    monkeypatch.setattr(
        svc,
        "_add_event",
        lambda *_args, **_kwargs: calls.__setitem__("event", calls["event"] + 1),
    )

    assert asyncio.run(svc.sync_running_tasks(snapshot)) == 0
    assert calls == {"progress": 1, "update": 0, "event": 0}


def test_background_health_uses_health_only_and_unchanged_is_memory_only(monkeypatch) -> None:
    calls = {"health": 0, "metrics": 0, "update": 0}

    class Client:
        def __init__(self, _base_url: str):
            pass

        async def probe_health(self):
            calls["health"] += 1
            return {"online": True}

        async def get_system_metrics(self):
            calls["metrics"] += 1
            raise AssertionError("background observer must not request metrics")

    monkeypatch.setattr(dispatch_module, "ComputeNodeClient", Client)
    svc = DispatchService()
    monkeypatch.setattr(
        svc,
        "update_node_observed_status",
        lambda *_args, **_kwargs: calls.__setitem__("update", calls["update"] + 1),
    )

    assert asyncio.run(svc.observe_node_health(_node(status="busy"), busy=True)) is False
    assert calls == {"health": 1, "metrics": 0, "update": 0}


def test_background_health_persists_one_real_transition(monkeypatch) -> None:
    calls = {"update": 0}

    class Client:
        def __init__(self, _base_url: str):
            pass

        async def probe_health(self):
            return {"online": False, "error": "unreachable"}

    monkeypatch.setattr(dispatch_module, "ComputeNodeClient", Client)
    svc = DispatchService()

    def update(*_args, **_kwargs):
        calls["update"] += 1
        return True

    monkeypatch.setattr(svc, "update_node_observed_status", update)

    assert asyncio.run(svc.observe_node_health(_node(status="online"), busy=False)) is True
    assert calls["update"] == 1


def test_remote_terminal_transition_and_node_release_happen_once(monkeypatch) -> None:
    task = _task()
    snapshot = {"nodes": [_node()], "tasks": [task]}
    sql_calls: list[str] = []
    updates: list[dict] = []

    class Client:
        def __init__(self, _base_url: str):
            pass

        async def get_task_progress(self, _remote_task_id: str):
            return {"status": "failed", "current_loop": 2, "total_loops": 10}

    monkeypatch.setattr(dispatch_module, "ComputeNodeClient", Client)
    monkeypatch.setattr(dispatch_module, "get_conn", lambda: _Connection(sql_calls))
    svc = DispatchService()

    def update(_task, **fields):
        updates.append(fields)
        return {**task, **fields}

    monkeypatch.setattr(svc, "_update_observed_task_fields", update)

    assert asyncio.run(svc.sync_running_tasks(snapshot)) == 1
    assert len(updates) == 1
    assert updates[0]["status"] == "failed"
    assert len([sql for sql in sql_calls if "current_task_id = NULL" in sql]) == 1


def test_remote_errors_stay_in_memory_until_threshold(monkeypatch) -> None:
    task = _task()
    snapshot = {"nodes": [_node()], "tasks": [task]}
    sql_calls: list[str] = []
    updates: list[dict] = []
    events: list[str] = []

    class Client:
        def __init__(self, _base_url: str):
            pass

        async def get_task_progress(self, _remote_task_id: str):
            raise RuntimeError("remote unavailable")

    monkeypatch.setattr(dispatch_module, "ComputeNodeClient", Client)
    monkeypatch.setattr(dispatch_module, "get_conn", lambda: _Connection(sql_calls))
    monkeypatch.setattr(DispatchService, "_CUSTOM_SYNC_FAIL_THRESHOLD", 3)
    svc = DispatchService()

    def update(_task, **fields):
        updates.append(fields)
        return {**task, **fields}

    monkeypatch.setattr(svc, "_update_observed_task_fields", update)
    monkeypatch.setattr(svc, "_add_event", lambda _task_id, event, *_args: events.append(event))

    assert asyncio.run(svc.sync_running_tasks(snapshot)) == 0
    assert asyncio.run(svc.sync_running_tasks(snapshot)) == 0
    assert updates == []
    assert events == []
    assert sql_calls == []

    assert asyncio.run(svc.sync_running_tasks(snapshot)) == 0
    assert len(updates) == 1
    assert updates[0]["status"] == "failed"
    assert events == ["auto_failed"]
    assert len([sql for sql in sql_calls if "current_task_id = NULL" in sql]) == 1


def test_single_scheduler_loop_recovers_collectors_once(monkeypatch) -> None:
    calls = {"resume": 0, "observe": 0}
    scheduler = scheduler_module.NodeHealthScheduler()
    monkeypatch.setattr(scheduler_module, "_OBSERVER_INTERVAL", 0.001)

    class Service:
        async def resume_log_collectors(self):
            calls["resume"] += 1
            return 0

        async def observe_dispatch_state(self):
            calls["observe"] += 1
            if calls["observe"] == 3:
                scheduler._ensure_stop_event().set()
            return {"node_changes": 0, "task_changes": 0}

    monkeypatch.setattr(dispatch_module, "DispatchService", Service)

    asyncio.run(scheduler._observer_loop())

    assert calls == {"resume": 1, "observe": 3}


def test_automatic_scheduler_has_no_metrics_or_discovery_side_effects() -> None:
    source = scheduler_module.__file__
    text = open(source, encoding="utf-8").read()
    assert "get_system_metrics" not in text
    assert "discover_manual_tasks" not in text
    assert "_heartbeat_loop" not in text
    assert "_task_sync_loop" not in text


def test_manual_discovery_is_explicit_post_only(monkeypatch) -> None:
    calls = {"discover": 0}

    class Service:
        async def discover_manual_tasks(self):
            calls["discover"] += 1
            return 2

    monkeypatch.setattr(dispatch_router, "_svc", Service())
    assert asyncio.run(dispatch_router.discover_manual_tasks()) == {"discovered": 2}
    assert calls["discover"] == 1
    routes = {
        (route.path, tuple(sorted(route.methods or [])))
        for route in dispatch_router.router.routes
    }
    assert ("/dispatch/nodes/discover-manual", ("POST",)) in routes


def test_observer_uses_one_aggregate_read_and_change_only_fencing() -> None:
    snapshot_source = inspect.getsource(DispatchService.load_observer_snapshot)
    sync_source = inspect.getsource(DispatchService.sync_running_tasks)
    update_source = inspect.getsource(DispatchService._update_observed_task_fields)
    assert snapshot_source.count("cur.execute") == 1
    assert "jsonb_agg" in snapshot_source
    assert "jsonb_build_object" in snapshot_source
    assert "to_jsonb" not in snapshot_source
    assert "env_overrides" not in snapshot_source
    assert "config" not in snapshot_source
    assert "list_tasks" not in sync_source
    assert "IS DISTINCT FROM" in update_source
    assert "status IS NOT DISTINCT FROM" in update_source
    assert "remote_task_id IS NOT DISTINCT FROM" in update_source
