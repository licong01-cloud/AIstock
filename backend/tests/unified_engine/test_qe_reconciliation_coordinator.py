from __future__ import annotations

import asyncio
from collections import Counter

from backend.services.quantevolver.qe_reconciliation_coordinator import (
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
            {"aggregate_select": 3, "experiment": 1, "resource": 1}
        )

    asyncio.run(scenario())


def test_two_idle_safety_sweeps_are_two_selects_and_zero_subscanner_work() -> None:
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
        assert calls == Counter({"aggregate_select": 2})

    asyncio.run(scenario())
