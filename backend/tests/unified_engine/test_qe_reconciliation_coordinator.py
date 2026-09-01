from __future__ import annotations

import asyncio
import threading
from collections import Counter

import pytest

from backend.services.quantevolver import qe_reconciliation_coordinator as module
from backend.services.quantevolver.qe_reconciliation_coordinator import (
    QEReconciliationConfig,
    QEReconciliationCoordinator,
    QEReconciliationDue,
    QEReconciliationScope,
    QEReconciliationWakeBus,
)


def test_work_notifications_coalesce_without_becoming_snapshot_requests() -> None:
    bus = QEReconciliationWakeBus()

    for _ in range(100):
        bus.notify(QEReconciliationScope.EXPERIMENT, key="exp-1")

    assert bus.drain_pending_work() == {
        QEReconciliationScope.EXPERIMENT: {"exp-1": False}
    }
    assert bus.drain_snapshot_requests() == {}


def test_wait_registration_requests_snapshot_not_business_work() -> None:
    async def scenario() -> None:
        bus = QEReconciliationWakeBus()
        loop = asyncio.get_running_loop()
        coordinator_event = asyncio.Event()
        bus.register_coordinator(loop=loop, event=coordinator_event)
        waiter = asyncio.create_task(
            bus.wait(
                QEReconciliationScope.RESOURCE_SESSION,
                key="session-1",
                timeout_seconds=60,
            )
        )
        await asyncio.wait_for(coordinator_event.wait(), timeout=1)
        assert bus.drain_pending_work() == {}
        assert bus.drain_snapshot_requests() == {
            QEReconciliationScope.RESOURCE_SESSION: ("session-1",)
        }
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)

    asyncio.run(scenario())


def test_same_state_does_not_wake_but_change_and_missing_do() -> None:
    async def scenario() -> None:
        bus = QEReconciliationWakeBus()
        first = QEReconciliationDue(
            resource_states={
                "session-1": {
                    "current_phase": "gpu_phase",
                    "session_status": "running",
                }
            }
        )
        bus.publish_states(first, resource_session_ids=("session-1",))
        generation = bus.state_generation(
            QEReconciliationScope.RESOURCE_SESSION,
            key="session-1",
        )
        assert generation == 0

        waiter = asyncio.create_task(
            bus.wait(
                QEReconciliationScope.RESOURCE_SESSION,
                key="session-1",
                observed_generation=generation,
                timeout_seconds=60,
            )
        )
        await asyncio.sleep(0)
        bus.publish_states(first, resource_session_ids=("session-1",))
        await asyncio.sleep(0)
        assert not waiter.done()

        changed = QEReconciliationDue(
            resource_states={
                "session-1": {
                    "current_phase": "gpu_phase_released",
                    "session_status": "running",
                }
            }
        )
        bus.publish_states(changed, resource_session_ids=("session-1",))
        assert await asyncio.wait_for(waiter, timeout=1) == 1

        missing_waiter = asyncio.create_task(
            bus.wait(
                QEReconciliationScope.RESOURCE_SESSION,
                key="session-1",
                observed_generation=1,
                timeout_seconds=60,
            )
        )
        await asyncio.sleep(0)
        bus.publish_states(
            QEReconciliationDue(),
            resource_session_ids=("session-1",),
        )
        assert await asyncio.wait_for(missing_waiter, timeout=1) == 2
        assert bus.resource_state("session-1") is None

    asyncio.run(scenario())


def test_snapshot_only_cycle_never_runs_business_scanners() -> None:
    async def scenario() -> None:
        calls: Counter[str] = Counter()

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            calls["aggregate_select"] += 1
            return QEReconciliationDue(
                reservation=True,
                experiment=True,
                evolution=True,
                long_trend=True,
                terminal_resource_session=True,
                loop_states={"loop-1": "running"},
            )

        async def reservation() -> None:
            calls["reservation"] += 1

        async def experiment() -> None:
            calls["experiment"] += 1

        async def evolution() -> None:
            calls["evolution"] += 1

        async def long_trend() -> None:
            calls["long_trend"] += 1

        def resource() -> None:
            calls["resource"] += 1

        coordinator = QEReconciliationCoordinator(
            due_probe=due_probe,
            reservation_scanner=reservation,
            experiment_scanner=experiment,
            evolution_scanner=evolution,
            long_trend_scanner=long_trend,
            resource_scanner=resource,
            wake_bus=QEReconciliationWakeBus(),
        )
        await coordinator.reconcile_once(
            snapshot_requests={QEReconciliationScope.EVOLUTION: ("loop-1",)},
        )
        assert calls == Counter({"aggregate_select": 1})

    asyncio.run(scenario())


def test_event_runs_only_its_family_and_repeated_key_is_throttled() -> None:
    async def scenario() -> None:
        calls: Counter[str] = Counter()

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            calls["aggregate_select"] += 1
            return QEReconciliationDue(
                reservation=True,
                experiment=True,
                evolution=True,
                long_trend=True,
                terminal_resource_session=True,
            )

        async def counted(name: str) -> None:
            calls[name] += 1

        coordinator = QEReconciliationCoordinator(
            due_probe=due_probe,
            reservation_scanner=lambda: counted("reservation"),
            experiment_scanner=lambda: counted("experiment"),
            evolution_scanner=lambda: counted("evolution"),
            long_trend_scanner=lambda: counted("long_trend"),
            resource_scanner=lambda: calls.update(["resource"]),
            wake_bus=QEReconciliationWakeBus(),
        )
        event = {QEReconciliationScope.EXPERIMENT: {"exp-1": False}}
        await coordinator.reconcile_once(pending=event)
        await coordinator.reconcile_once(pending=event)
        await coordinator.reconcile_once(
            pending={QEReconciliationScope.RESOURCE_SESSION: {"session-1": False}}
        )
        assert calls == Counter(
            {"aggregate_select": 2, "experiment": 1, "resource": 1}
        )

    asyncio.run(scenario())


def test_repeated_idle_safety_wake_is_throttled_before_select() -> None:
    async def scenario() -> None:
        calls: Counter[str] = Counter()

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            calls["aggregate_select"] += 1
            return QEReconciliationDue()

        async def unexpected() -> None:
            calls["subscanner"] += 1

        coordinator = QEReconciliationCoordinator(
            due_probe=due_probe,
            reservation_scanner=unexpected,
            experiment_scanner=unexpected,
            evolution_scanner=unexpected,
            long_trend_scanner=unexpected,
            resource_scanner=lambda: calls.update(["subscanner"]),
            wake_bus=QEReconciliationWakeBus(),
        )
        await coordinator.reconcile_once(is_safety_sweep=True)
        await coordinator.reconcile_once(is_safety_sweep=True)
        assert calls == Counter({"aggregate_select": 1})

    asyncio.run(scenario())


def test_event_or_snapshot_probe_never_postpones_startup_safety_recovery() -> None:
    async def scenario() -> None:
        calls: Counter[str] = Counter()

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            calls["select"] += 1
            return QEReconciliationDue()

        coordinator = QEReconciliationCoordinator(
            due_probe=due_probe,
            wake_bus=QEReconciliationWakeBus(),
        )
        await coordinator.reconcile_once(
            pending={QEReconciliationScope.EXPERIMENT: {"exp-1": False}}
        )
        await coordinator.reconcile_once(
            snapshot_requests={QEReconciliationScope.EVOLUTION: ("loop-1",)}
        )
        await coordinator.reconcile_once(is_safety_sweep=True)

        assert calls == Counter({"select": 3})

    asyncio.run(scenario())


def test_environment_contract_disables_families_clamps_intervals_and_keeps_batch(
    monkeypatch,
) -> None:
    monkeypatch.setenv("DISABLE_EVOLUTION_SCANNER", "true")
    monkeypatch.setenv("DISABLE_QE_EXPERIMENT_SCANNER", "1")
    monkeypatch.setenv("QE_EXECUTION_RESERVATION_SCAN_INTERVAL_SEC", "15")
    monkeypatch.setenv("QE_EXPERIMENT_SCAN_INTERVAL_SEC", "300")
    monkeypatch.setenv("QE_EVOLUTION_SCAN_INTERVAL_SEC", "120")
    monkeypatch.setenv("QE_EXPERIMENT_SCAN_BATCH_SIZE", "17")

    config = QEReconciliationConfig.from_environment()

    assert QEReconciliationScope.EXPERIMENT not in config.enabled_scopes
    assert QEReconciliationScope.EVOLUTION not in config.enabled_scopes
    assert config.family_intervals[QEReconciliationScope.RESERVATION] == 60
    assert config.family_intervals[QEReconciliationScope.EXPERIMENT] == 300
    assert config.family_intervals[QEReconciliationScope.EVOLUTION] == 120
    assert config.experiment_batch_size == 17


def test_default_experiment_scanner_receives_legacy_batch_size(monkeypatch) -> None:
    from backend.services.quantevolver import qe_experiment_status_scanner as scanner_module

    seen: list[int] = []

    class Scanner:
        def __init__(self, *, batch_size: int):
            seen.append(batch_size)

        async def scan_once(self):
            return {}

    config = QEReconciliationConfig(
        enabled_scopes=frozenset(QEReconciliationScope),
        family_intervals={scope: 60 for scope in QEReconciliationScope},
        experiment_batch_size=23,
    )
    monkeypatch.setattr(scanner_module, "QEExperimentStatusScanner", Scanner)
    coordinator = QEReconciliationCoordinator(
        config=config,
        reservation_scanner=lambda: _async_count(Counter(), "reservation"),
        evolution_scanner=lambda: _async_count(Counter(), "evolution"),
        long_trend_scanner=lambda: _async_count(Counter(), "long_trend"),
        resource_scanner=lambda: None,
        wake_bus=QEReconciliationWakeBus(),
    )

    coordinator._initialize_default_scanners()

    assert seen == [23]


def test_disabled_families_have_no_due_subquery_scanner_or_dml(monkeypatch) -> None:
    captured: dict[str, str] = {}

    class Cursor:
        description = [
            ("reservation",),
            ("experiment",),
            ("evolution",),
            ("long_trend",),
            ("terminal_resource_session",),
            ("resource_states",),
            ("loop_states",),
        ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, _params):  # type: ignore[no-untyped-def]
            captured["sql"] = " ".join(sql.split())
            assert not any(
                verb in captured["sql"].upper()
                for verb in ("UPDATE ", "INSERT ", "DELETE ")
            )

        def fetchone(self):
            return {
                "reservation": False,
                "experiment": False,
                "evolution": False,
                "long_trend": False,
                "terminal_resource_session": False,
                "resource_states": {},
                "loop_states": {},
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return Cursor()

    disabled = frozenset(
        set(QEReconciliationScope)
        - {QEReconciliationScope.EXPERIMENT, QEReconciliationScope.EVOLUTION}
    )
    config = QEReconciliationConfig(
        enabled_scopes=disabled,
        family_intervals={scope: 60 for scope in QEReconciliationScope},
        experiment_batch_size=11,
    )
    calls: Counter[str] = Counter()
    coordinator = QEReconciliationCoordinator(
        config=config,
        experiment_scanner=lambda: _async_count(calls, "experiment"),
        evolution_scanner=lambda: _async_count(calls, "evolution"),
        wake_bus=QEReconciliationWakeBus(),
    )
    monkeypatch.setattr(module, "get_conn", lambda: Connection())

    asyncio.run(coordinator.reconcile_once(is_safety_sweep=True))

    assert "FROM qe_experiments e" not in captured["sql"]
    assert "JOIN qe_evolution_tasks" not in captured["sql"]
    assert calls == Counter()


def test_due_probe_escapes_like_percent_for_psycopg2_binding(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):  # type: ignore[no-untyped-def]
            captured["sql"] = sql
            captured["params"] = params
            # psycopg2 uses pyformat parameter binding. This catches any bare
            # literal percent before a query can reach PostgreSQL.
            captured["rendered"] = sql % tuple("ARRAY[]" for _ in params)

        def fetchone(self):
            return {
                "reservation": False,
                "experiment": False,
                "evolution": False,
                "long_trend": False,
                "terminal_resource_session": False,
                "resource_states": {},
                "loop_states": {},
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return Cursor()

    config = QEReconciliationConfig(
        enabled_scopes=frozenset(QEReconciliationScope),
        family_intervals={scope: 60 for scope in QEReconciliationScope},
        experiment_batch_size=11,
    )
    coordinator = QEReconciliationCoordinator(
        config=config,
        wake_bus=QEReconciliationWakeBus(),
    )
    monkeypatch.setattr(module, "get_conn", lambda: Connection())

    due = coordinator._load_due_work(
        resource_session_ids=("session-1",),
        loop_ids=("loop-1",),
    )

    assert "NOT LIKE 'qelt:%%'" in str(captured["sql"])
    assert "NOT LIKE 'qelt:%'" in str(captured["rendered"])
    assert str(captured["sql"]).count("%s") == 2
    assert captured["params"] == (["session-1"], ["loop-1"])
    assert not due.any_due


def test_evolution_due_probe_includes_running_task_without_active_loop(monkeypatch) -> None:
    captured: dict[str, str] = {}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, _params):  # type: ignore[no-untyped-def]
            captured["sql"] = " ".join(sql.split())

        def fetchone(self):
            return {
                "reservation": False,
                "experiment": False,
                "evolution": True,
                "long_trend": False,
                "terminal_resource_session": False,
                "resource_states": {},
                "loop_states": {},
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return Cursor()

    config = QEReconciliationConfig(
        enabled_scopes=frozenset({QEReconciliationScope.EVOLUTION}),
        family_intervals={scope: 60 for scope in QEReconciliationScope},
        experiment_batch_size=1,
    )
    coordinator = QEReconciliationCoordinator(
        config=config,
        wake_bus=QEReconciliationWakeBus(),
    )
    monkeypatch.setattr(module, "get_conn", lambda: Connection())

    due = coordinator._load_due_work()

    assert due.evolution is True
    assert "FROM qe_evolution_tasks t WHERE t.status = 'running'" in captured["sql"]


async def _async_count(calls: Counter[str], key: str) -> None:
    calls[key] += 1


def test_100_same_key_wakes_and_forces_throttle_before_due_probe() -> None:
    async def scenario() -> None:
        calls: Counter[str] = Counter()

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            calls["select"] += 1
            return QEReconciliationDue(experiment=True)

        coordinator = QEReconciliationCoordinator(
            due_probe=due_probe,
            experiment_scanner=lambda: _async_count(calls, "scanner"),
            wake_bus=QEReconciliationWakeBus(),
        )
        pending = {QEReconciliationScope.EXPERIMENT: {"exp-1": False}}
        for _ in range(100):
            await coordinator.reconcile_once(pending=pending)
        assert calls == Counter({"select": 1, "scanner": 1})

        forced = {QEReconciliationScope.EXPERIMENT: {"exp-1": True}}
        await coordinator.reconcile_once(pending=forced)
        await coordinator.reconcile_once(pending=forced)
        assert calls == Counter({"select": 2, "scanner": 2})

    asyncio.run(scenario())


def test_different_first_seen_keys_in_same_family_submit_immediately() -> None:
    async def scenario() -> None:
        calls: Counter[str] = Counter()

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            calls["select"] += 1
            return QEReconciliationDue(experiment=True)

        coordinator = QEReconciliationCoordinator(
            due_probe=due_probe,
            experiment_scanner=lambda: _async_count(calls, "experiment"),
            wake_bus=QEReconciliationWakeBus(),
        )
        await coordinator.reconcile_once(
            pending={QEReconciliationScope.EXPERIMENT: {"exp-1": False}}
        )
        await coordinator.reconcile_once(
            pending={QEReconciliationScope.EXPERIMENT: {"exp-2": False}}
        )

        assert calls == Counter({"select": 2, "experiment": 2})

    asyncio.run(scenario())


def test_different_first_seen_keys_across_families_submit_immediately() -> None:
    async def scenario() -> None:
        calls: Counter[str] = Counter()

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            calls["select"] += 1
            return QEReconciliationDue(experiment=True, evolution=True)

        coordinator = QEReconciliationCoordinator(
            due_probe=due_probe,
            experiment_scanner=lambda: _async_count(calls, "experiment"),
            evolution_scanner=lambda: _async_count(calls, "evolution"),
            wake_bus=QEReconciliationWakeBus(),
        )
        await coordinator.reconcile_once(
            pending={QEReconciliationScope.EXPERIMENT: {"exp-1": False}}
        )
        await coordinator.reconcile_once(
            pending={QEReconciliationScope.EVOLUTION: {"task-1": False}}
        )

        assert calls == Counter({"select": 2, "experiment": 1, "evolution": 1})

    asyncio.run(scenario())


def test_multiple_new_keys_coalesced_in_one_wake_use_one_probe() -> None:
    async def scenario() -> None:
        calls: Counter[str] = Counter()

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            calls["select"] += 1
            return QEReconciliationDue(experiment=True)

        coordinator = QEReconciliationCoordinator(
            due_probe=due_probe,
            experiment_scanner=lambda: _async_count(calls, "experiment"),
            wake_bus=QEReconciliationWakeBus(),
        )
        await coordinator.reconcile_once(
            pending={
                QEReconciliationScope.EXPERIMENT: {
                    "exp-1": False,
                    "exp-2": False,
                }
            }
        )

        assert calls == Counter({"select": 1, "experiment": 1})

    asyncio.run(scenario())


def test_slower_family_interval_is_enforced_after_due_probe() -> None:
    async def scenario() -> None:
        calls: Counter[str] = Counter()
        config = QEReconciliationConfig(
            enabled_scopes=frozenset(QEReconciliationScope),
            family_intervals={
                **{scope: 60 for scope in QEReconciliationScope},
                QEReconciliationScope.EXPERIMENT: 300,
            },
            experiment_batch_size=50,
        )

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            calls["select"] += 1
            return QEReconciliationDue(experiment=True)

        coordinator = QEReconciliationCoordinator(
            config=config,
            due_probe=due_probe,
            experiment_scanner=lambda: _async_count(calls, "scanner"),
            wake_bus=QEReconciliationWakeBus(),
        )
        await coordinator.reconcile_once(is_safety_sweep=True)
        coordinator._last_safety_probe_at = None
        await coordinator.reconcile_once(is_safety_sweep=True)
        assert calls == Counter({"select": 2, "scanner": 1})

    asyncio.run(scenario())


def test_pending_and_throttle_maps_are_bounded() -> None:
    bus = QEReconciliationWakeBus()
    bus._PENDING_WORK_LIMIT = 3
    for index in range(10):
        bus.notify(QEReconciliationScope.EXPERIMENT, key=f"exp-{index}")
    pending = bus.drain_pending_work()[QEReconciliationScope.EXPERIMENT]
    assert tuple(pending) == ("exp-7", "exp-8", "exp-9")

    coordinator = QEReconciliationCoordinator(
        due_probe=lambda **_kwargs: QEReconciliationDue(),
        wake_bus=QEReconciliationWakeBus(),
    )
    coordinator._THROTTLE_CACHE_LIMIT = 3
    for index in range(10):
        coordinator._filter_pending_before_probe(
            {QEReconciliationScope.EXPERIMENT: {f"exp-{index}": False}},
            now=float(index),
            replay_scopes=frozenset(),
        )
    assert len(coordinator._last_business_scan_at) == 3


def test_state_generation_remains_monotonic_after_cache_eviction() -> None:
    bus = QEReconciliationWakeBus()
    bus._STATE_CACHE_LIMIT = 1
    bus.publish_states(
        QEReconciliationDue(loop_states={"loop-a": "running"}),
        loop_ids=("loop-a",),
    )
    first = bus.state_generation(QEReconciliationScope.EVOLUTION, key="loop-a")
    bus.publish_states(
        QEReconciliationDue(loop_states={"loop-b": "running"}),
        loop_ids=("loop-b",),
    )
    bus.publish_states(
        QEReconciliationDue(loop_states={"loop-a": "running"}),
        loop_ids=("loop-a",),
    )
    assert bus.state_generation(QEReconciliationScope.EVOLUTION, key="loop-a") > first


def test_inflight_dirty_wake_is_replayed_after_scanner_finishes() -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        calls: Counter[str] = Counter()

        async def scanner() -> None:
            calls["scanner"] += 1
            if calls["scanner"] == 1:
                started.set()
                await release.wait()

        coordinator = QEReconciliationCoordinator(
            due_probe=lambda **_kwargs: QEReconciliationDue(experiment=True),
            experiment_scanner=scanner,
            wake_bus=QEReconciliationWakeBus(),
        )
        await coordinator.reconcile_once(
            pending={QEReconciliationScope.EXPERIMENT: {"exp-1": False}},
            dispatch_background=True,
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        await coordinator.reconcile_once(
            pending={QEReconciliationScope.EXPERIMENT: {"exp-1": True}},
            dispatch_background=True,
        )
        release.set()
        await asyncio.gather(*tuple(coordinator._inflight.values()))
        await asyncio.sleep(0)
        replay = frozenset(coordinator._replay_ready)
        coordinator._replay_ready.clear()
        assert replay == frozenset({QEReconciliationScope.EXPERIMENT})
        await coordinator.reconcile_once(replay_scopes=replay)
        assert calls["scanner"] == 2

    asyncio.run(scenario())


def test_cancellation_waits_for_to_thread_probe_before_pool_can_close() -> None:
    async def scenario() -> None:
        started = threading.Event()
        release = threading.Event()

        def due_probe(**_kwargs):  # type: ignore[no-untyped-def]
            started.set()
            release.wait(timeout=5)
            return QEReconciliationDue()

        coordinator = QEReconciliationCoordinator(
            due_probe=due_probe,
            wake_bus=QEReconciliationWakeBus(),
        )
        task = asyncio.create_task(coordinator.reconcile_once(is_safety_sweep=True))
        while not started.is_set():
            await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
