"""Local wait duration must never invent a remote terminal result."""

import asyncio
import pytest

from backend.services.quantevolver import qe_evolution_service as qes
from backend.services.quantevolver import qe_reconciliation_coordinator as coordinator


@pytest.mark.parametrize("terminal", ["completed", "failed", "cancelled", "canceled"])
@pytest.mark.parametrize("initial_unknown", [False, True])
def test_custom_evo_wait_early_wakeups_and_long_queue_do_not_fail(monkeypatch, terminal, initial_unknown):
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    state = {"status": None, "calls": 0, "clock": 0.0, "processed": []}

    async def wait(scope, *, key, timeout_seconds, observed_generation):
        assert scope == coordinator.QEReconciliationScope.EVOLUTION
        assert key == "task_Loop21"
        assert timeout_seconds == 60
        assert observed_generation == (None if state["calls"] == 0 else state["calls"])
        state["calls"] += 1
        # 250 immediate wakeups would previously exhaust the synthetic 4h
        # counter. Afterwards queue + execution exceed 4h of real time too.
        state["clock"] += 0.1 if state["calls"] < 251 else 15000
        state["status"] = None if initial_unknown and state["calls"] < 3 else "running"
        if state["calls"] == 252:
            state["status"] = terminal
        return state["calls"]

    async def process(task_id, loop_id):
        state["processed"].append((task_id, loop_id))

    def forbidden_db():
        raise AssertionError("A local waiter cannot write failure or poll DB")

    monkeypatch.setattr(coordinator, "wait_for_qe_reconciliation", wait)
    monkeypatch.setattr(coordinator.qe_reconciliation_wakeup, "loop_state", lambda key: state["status"])
    monkeypatch.setattr(qes, "get_conn", forbidden_db)
    monkeypatch.setattr(scheduler, "_safe_process_completed_loop", process)

    async def run():
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "time", lambda: state["clock"])
        await scheduler._wait_and_process_custom_evo_loop("task", 21, "task_Loop21")

    asyncio.run(run())
    assert state["calls"] == 252
    assert state["processed"] == ([("task", "task_Loop21")] if terminal == "completed" else [])


def test_custom_evo_wait_cancellation_does_not_change_persisted_state(monkeypatch):
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)

    async def cancelled(*args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(coordinator, "wait_for_qe_reconciliation", cancelled)
    monkeypatch.setattr(qes, "get_conn", lambda: pytest.fail("wait cancellation must not mark failed"))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(scheduler._wait_and_process_custom_evo_loop("task", 21, "task_Loop21"))
