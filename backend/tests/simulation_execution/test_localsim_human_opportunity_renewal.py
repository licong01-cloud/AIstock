"""Native decision renewal is consumed without resetting the human delay."""

from datetime import timedelta
from decimal import Decimal

import pytest

from backend.services.position_timing.intraday.contracts import ExecutionView, digest
from backend.services.simulation_execution.localsim.human_operation import HumanOperationPolicy, HumanOperationQueue
from backend.services.simulation_execution.localsim.timing import CLOSED_LOOP_MODE, TimingFacade
from backend.tests.simulation_execution.test_localsim_human_operation import human_day
from backend.tests.simulation_execution.test_localsim_position_timing import at, inputs


def native_decisions(*clocks):
    plan, market = inputs()
    facade = TimingFacade(plan, 0, mode=CLOSED_LOOP_MODE)
    decisions = []
    for clock in clocks:
        now = at(clock)
        feedback = ExecutionView(consumer_kind="SIMULATION", symbol=plan.symbol,
            trade_date=plan.target_trade_date, available_at=now, confirmed_quantity=0,
            reserved_quantity=0, cash_cny=Decimal("10000000"), sellable_quantity=0,
            source_ref="renewal-test", feedback_id=digest(clock))
        decisions.append(facade.evaluate(market, feedback, now))
    return plan, decisions


def queued(decision, *, delay=180):
    queue = HumanOperationQueue(account_id="renewal-test", policy=HumanOperationPolicy(reaction_delay_seconds=delay))
    action = queue.enqueue(plan_id=decision.plan_id, revision=decision.revision,
        opportunity_id=decision.opportunity_id, symbol="000001.SZ", side="BUY",
        visible_at=decision.advice_available_at, valid_until=decision.valid_until)
    return queue, action


@pytest.mark.parametrize("rule", ["FIRST_OBSERVABLE_V1", "PRICE_TOUCH_V1", "BOUNDED_REVERSION_V1"])
@pytest.mark.parametrize("delay", [0, 60, 180, 300])
def test_continuously_valid_native_opportunity_reaches_real_fill_after_each_delay(rule, delay):
    plan, market = inputs(rule)
    backend, result = human_day(plan, market, delay=delay)
    account = result["account"]
    assert account["parents"][0]["status"] == "FILLED"
    assert sum(fill.quantity for fill in backend._ledger.fills) == plan.target_quantity
    actions = account["human_operation_state"]["actions"]
    assert len(actions) == 1
    action = actions[0]
    from backend.services.position_timing.intraday.contracts import utc
    visible, ready, dispatched = [utc(action[k]) for k in ("visible_at", "ready_at", "dispatch_at")]
    assert ready == visible + timedelta(seconds=delay)
    assert dispatched == ready
    assert all(fill.trade_time > dispatched for fill in backend._ledger.fills)
    assert all(session.state.executed_quantity == session.order.filled_quantity for session in result["sessions"])


def test_verified_renewal_preserves_identity_first_clock_fifo_and_restore():
    plan, decisions = native_decisions("09:31:10", "09:32:10", "09:33:10", "09:34:10")
    queue, original = queued(decisions[0])
    for decision in decisions[1:]:
        queue.revalidate_opportunity(original.action_id, decision=decision, as_of=decision.as_of,
                                     plan_valid_until=plan.valid_until)
        if decision.as_of < original.ready_at:
            assert queue.claim(decision.as_of, in_trading_session=True) is None
    current = queue.actions[original.action_id]
    assert current.action_id == original.action_id
    assert (current.visible_at, current.ready_at) == (original.visible_at, original.ready_at)
    assert len(queue.actions) == 1
    assert current.valid_until == decisions[-1].valid_until
    restored = HumanOperationQueue.restore(queue.snapshot())
    assert restored.snapshot() == queue.snapshot()
    assert restored.claim(original.ready_at, in_trading_session=True).action_id == original.action_id


@pytest.mark.parametrize("next_clock", ["09:32:40", "09:33:10"])
def test_gap_expired_and_withdrawn_actions_cannot_be_revived(next_clock):
    plan, decisions = native_decisions("09:31:10", next_clock)
    for withdrawn in (False, True):
        queue, action = queued(decisions[0])
        if withdrawn:
            queue.invalidate(action.action_id, "OPPORTUNITY_WAIT")
        queue.revalidate_opportunity(action.action_id, decision=decisions[1], as_of=decisions[1].as_of,
                                     plan_valid_until=plan.valid_until)
        assert queue.actions[action.action_id].status == "INVALIDATED"
        assert queue.actions[action.action_id].valid_until == action.valid_until
        assert queue.claim(action.ready_at, in_trading_session=True) is None


@pytest.mark.parametrize("change", ["clock", "plan_id", "revision", "opportunity_id", "status", "advice_available_at", "data_as_of"])
def test_renewal_rejects_stale_or_mismatched_native_evidence(change):
    plan, decisions = native_decisions("09:31:10", "09:32:10")
    queue, action = queued(decisions[0])
    decision = decisions[1]
    now = decision.as_of
    if change == "clock":
        now += timedelta(seconds=1)
    else:
        value = {"plan_id": "other", "revision": 2, "opportunity_id": "f" * 64,
                 "status": "WAIT", "advice_available_at": now, "data_as_of": now + timedelta(seconds=1)}[change]
        decision = decision.model_copy(update={change: value})
    with pytest.raises(ValueError, match="REVALIDATION"):
        queue.revalidate_opportunity(action.action_id, decision=decision, as_of=now,
                                     plan_valid_until=plan.valid_until)
    assert queue.actions[action.action_id] == action


def test_renewal_clock_cannot_move_backwards_before_any_dispatch():
    plan, decisions = native_decisions("09:31:10", "09:32:10", "09:33:10")
    queue, action = queued(decisions[0])
    for decision in decisions[1:]:
        queue.revalidate_opportunity(action.action_id, decision=decision, as_of=decision.as_of,
                                     plan_valid_until=plan.valid_until)
    before = queue.snapshot()
    with pytest.raises(ValueError, match="REVALIDATION"):
        queue.revalidate_opportunity(action.action_id, decision=decisions[1], as_of=decisions[1].as_of,
                                     plan_valid_until=plan.valid_until)
    assert queue.snapshot() == before


def test_invalid_hard_deadline_rejects_without_mutating_the_queue():
    plan, decisions = native_decisions("09:31:10", "09:32:10")
    queue, action = queued(decisions[0])
    before = queue.snapshot()
    with pytest.raises(ValueError, match="REVALIDATION_WINDOW_INVALID"):
        queue.revalidate_opportunity(action.action_id, decision=decisions[1], as_of=decisions[1].as_of,
                                     plan_valid_until=decisions[1].as_of)
    assert queue.snapshot() == before


def test_renewal_cannot_extend_plan_deadline_or_reexecute_dispatched_action():
    plan, decisions = native_decisions("09:31:10", "09:32:10", "09:33:10")
    queue, action = queued(decisions[0], delay=60)
    deadline = at("09:32:30")
    queue.revalidate_opportunity(action.action_id, decision=decisions[1], as_of=decisions[1].as_of,
                                 plan_valid_until=deadline)
    assert queue.actions[action.action_id].valid_until == deadline
    dispatched = queue.claim(action.ready_at, in_trading_session=True)
    queue.acknowledge(dispatched.action_id, {"status": "SUBMITTED"})
    queue.revalidate_opportunity(action.action_id, decision=decisions[2], as_of=decisions[2].as_of,
                                 plan_valid_until=plan.valid_until)
    assert queue.actions[action.action_id].status == "COMPLETED"
    assert queue.actions[action.action_id].valid_until == deadline
    assert queue.claim(at("09:34:10"), in_trading_session=True) is None
