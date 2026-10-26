"""Human-capacity and native-account oracles, without market or DB I/O."""

from datetime import timedelta
from decimal import Decimal

import pytest

from backend.services.position_timing.intraday.contracts import digest, utc
from backend.services.position_timing.intraday.replay import PlanEvent
from backend.services.simulation_execution.localsim.human_operation import (
    HumanOperationPolicy, HumanOperationQueue, operation_snapshot, validate_operation_snapshot,
    validate_operation_configuration,
)
from backend.services.simulation_execution.localsim.human_portfolio import run_human_portfolio_day
from backend.tests.simulation_execution.test_localsim_position_timing import at, inputs


def queued(queue, name, *, visible="09:31:10", side="BUY", kind="SUBMIT", until="14:57:00"):
    return queue.enqueue(plan_id=name, revision=1, opportunity_id=name, symbol=name,
                         side=side, kind=kind, visible_at=at(visible), valid_until=at(until))


def test_account_capacity_rejections_cancellation_ready_and_exact_restore():
    queue = HumanOperationQueue(account_id="account", policy=HumanOperationPolicy(reaction_delay_seconds=60))
    future = queued(queue, "cancel", visible="09:35:10", kind="CANCEL")
    buy, sell = queued(queue, "buy"), queued(queue, "sell", side="SELL")
    assert queue.claim(at("09:31:10"), in_trading_session=True) is None
    first = queue.claim(at("09:32:10"), in_trading_session=True)
    assert first.action_id == sell.action_id  # Future cancellation cannot block.
    restored = HumanOperationQueue.restore(queue.snapshot())
    assert restored.claim(at("09:32:20"), in_trading_session=True) == first
    restored.acknowledge(first.action_id, {"status": "REJECTED", "reason": "INSUFFICIENT_CASH"})
    assert restored.claim(at("09:32:59"), in_trading_session=True) is None
    second = restored.claim(at("09:33:10"), in_trading_session=True)
    assert second.action_id == buy.action_id
    restored.acknowledge(second.action_id, {"status": "SUBMITTED"})
    assert restored.claim(at("09:36:10"), in_trading_session=True).action_id == future.action_id
    assert restored.snapshot() == HumanOperationQueue.restore(restored.snapshot()).snapshot()


def test_persistent_opportunity_does_not_reset_clock_or_extend_window():
    queue = HumanOperationQueue(account_id="account", policy=HumanOperationPolicy(reaction_delay_seconds=300))
    original = queued(queue, "stock", until="09:35:00")
    assert queued(queue, "stock", visible="09:32:10", until="09:50:00") == original
    assert queue.claim(at("09:36:10"), in_trading_session=True) is None
    assert queue.actions[original.action_id].outcome == {"reason": "OPPORTUNITY_EXPIRED_IN_QUEUE"}
    assert queue.last_action_at is None


def test_legacy_shape_and_unknown_modes_fail_closed():
    assert validate_operation_snapshot(None, legacy=True) == operation_snapshot()
    for bad in [None, {}, {**operation_snapshot(), "operation_mode": "HUMNA_SIM"},
                {**operation_snapshot(), "operation_mode": "HUMAN_SIM"}]:
        with pytest.raises(ValueError):
            validate_operation_snapshot(bad)
    policy = HumanOperationPolicy(reaction_delay_seconds=60)
    assert validate_operation_snapshot(operation_snapshot(policy)) == operation_snapshot(policy)
    assert validate_operation_configuration(None, legacy=True, algo_code="TWAP") == operation_snapshot()
    with pytest.raises(ValueError, match="COMBINATION_UNSUPPORTED"):
        validate_operation_configuration(operation_snapshot(policy), algo_code="TWAP")
    with pytest.raises(ValueError):
        HumanOperationPolicy(reaction_delay_seconds=True)
    snapshot = operation_snapshot(policy)
    snapshot["human_policy"].pop("minimum_action_interval_seconds")
    with pytest.raises(ValueError):
        validate_operation_snapshot(snapshot)
    for field, value in [("allow_split", 0), ("automatic_reprice", 0), ("new_order_requires_known_account", 1)]:
        with pytest.raises(ValueError):
            HumanOperationPolicy(reaction_delay_seconds=60, **{field: value})


def test_repeated_plan_revision_keeps_pending_cancel_and_restore_rejects_impossible_clock():
    queue = HumanOperationQueue(account_id="account", policy=HumanOperationPolicy(reaction_delay_seconds=60))
    cancel = queued(queue, "old", kind="CANCEL")
    queued(queue, "new")
    queue.invalidate_plan("old", reason="PLAN_REVISED")
    assert queue.actions[cancel.action_id].status == "QUEUED"
    claimed = queue.claim(at("09:32:10"), in_trading_session=True)
    queue.acknowledge(claimed.action_id, {"status": "CANCELLED"})
    snapshot = queue.snapshot()
    for changed in [dict(snapshot, last_action_at=at("09:33:10").isoformat()),
                    dict(snapshot, maximum_queue=0)]:
        changed["state_sha256"] = digest({k:v for k,v in changed.items() if k != "state_sha256"})
        with pytest.raises(ValueError, match="RESTORE_.*CONFLICT"):
            HumanOperationQueue.restore(changed)
    changed = queue.snapshot()
    changed["actions"][0]["ready_at"] = at("09:31:10").isoformat()
    changed["state_sha256"] = digest({k:v for k,v in changed.items() if k != "state_sha256"})
    with pytest.raises(ValueError, match="RESTORE_ACTION_STATE_CONFLICT"):
        HumanOperationQueue.restore(changed)


def full_markets():
    plan, market = inputs()
    bars = []
    for n in range(330):
        if 120 <= n < 210:
            continue
        start = at("09:30:00") + timedelta(minutes=n)
        bars.append(market.bars[0].model_copy(update={"start": start, "end": start + timedelta(minutes=1),
                                                    "received_at": start + timedelta(seconds=65)}))
    market = market.model_copy(update={"bars": tuple(bars)})
    other = market.model_copy(update={"symbol": "600000.SH", "day_facts": market.day_facts.model_copy(update={"symbol": "600000.SH"})})
    plan2 = plan.model_copy(update={"plan_id": "second", "symbol": "600000.SH"})
    return [plan, plan2], {market.symbol: market, other.symbol: other}


@pytest.mark.parametrize("delay,count", [(0, 2), (60, 1)])
def test_two_stocks_share_one_operator_and_no_double_delay(delay, count):
    plans, markets = full_markets()
    from backend.services.simulation_execution.localsim.timing_research import backend_for
    from backend.services.position_timing.intraday.research import CostContract
    cost = CostContract(open_rate="0.000095", close_rate="0.000595", minimum_cny="5")
    backend = backend_for({"portfolio_id": "account", "package_id": "fixture",
                           "origin_initial_cash_cny": "10000000", "cash_cny": "10000000", "positions": {}},
                          markets[plans[0].symbol], cost)
    result = run_human_portfolio_day(backend=backend, markets=markets,
        events=[PlanEvent(available_at=p.valid_from, plan=p) for p in plans],
        initial_marks={s: Decimal("10") for s in markets},
        policy=HumanOperationPolicy(reaction_delay_seconds=delay), run_id="human-test")
    actions = result["account"]["human_operation_state"]["actions"]
    submits = [a for a in actions if a["kind"] == "SUBMIT" and a["dispatch_at"]]
    assert [utc(a["dispatch_at"]) for a in submits] == [at("09:31:10") + timedelta(seconds=delay + 60*n) for n in range(count)]
    fills = sorted(backend._ledger.fills, key=lambda f: f.trade_time)
    assert [f.trade_time for f in fills] == [at("09:32:00") + timedelta(seconds=delay + 60*n) for n in range(count)]
    assert len(backend._records) == count
    assert all(s.order.created_at == s.submitted_at for s in result["sessions"])
    assert all(s.feedback_history[0].available_at == s.submitted_at for s in result["sessions"])
    assert backend._ledger.cash_decimal == Decimal("10000000") - Decimal("10005") * count
    assert all(p.available_quantity == 0 for p in backend._ledger.positions.values())
    assert result["account"]["human_operation_metrics"]["action_count"] == count
    assert len(result["account"]["parents"]) == 2  # Queued expiry stays in the population.
    if delay:
        assert result["account"]["human_operation_metrics"]["expired_actions"] == 1
    assert digest(result["account"]["human_operation_state"])


def human_day(plan, market, *, events=None, cash="10000000", delay=60):
    from backend.services.simulation_execution.localsim.timing_research import backend_for
    from backend.services.position_timing.intraday.research import CostContract
    cost = CostContract(open_rate="0.000095", close_rate="0.000595", minimum_cny="5")
    # The single-order fixture has 30 minutes; portfolio validation requires
    # the complete frozen day. Extend this synthetic fixture explicitly.
    market = market.model_copy(update={"bars":market.bars + tuple(
        market.bars[-1].model_copy(update={"start":at("09:30:00")+timedelta(minutes=n),
                                          "end":at("09:30:00")+timedelta(minutes=n+1),
                                          "received_at":at("09:30:00")+timedelta(minutes=n, seconds=65)})
        for n in range(30,327) if n < 120 or n >= 210)})
    backend = backend_for({"portfolio_id": "account", "package_id": "fixture", "origin_initial_cash_cny": cash,
                           "cash_cny": cash, "positions": {}}, market, cost)
    result = run_human_portfolio_day(backend=backend, markets={plan.symbol:market},
        events=events or [PlanEvent(available_at=plan.valid_from, plan=plan)],
        initial_marks={plan.symbol:Decimal("10")}, policy=HumanOperationPolicy(reaction_delay_seconds=delay), run_id="test")
    return backend, result


def test_partial_fill_multiple_revisions_cancel_once_then_only_confirmed_remaining():
    plan, market = inputs()
    market = market.model_copy(update={"bars":tuple(b.model_copy(update={"volume_shares":Decimal("300")}) for b in market.bars)})
    revision2 = plan.model_copy(update={"revision":2, "source_revision":"2"})
    revision3 = plan.model_copy(update={"revision":3, "source_revision":"3"})
    backend, result = human_day(plan, market, delay=0, events=[
        PlanEvent(available_at=plan.valid_from, plan=plan),
        PlanEvent(available_at=at("09:32:30"), plan=revision2),
        PlanEvent(available_at=at("09:32:40"), plan=revision3)])
    actions = result["account"]["human_operation_state"]["actions"]
    cancels = [a for a in actions if a["kind"] == "CANCEL" and a["dispatch_at"]]
    assert len(cancels) == 1
    assert len(backend._records) == 2
    assert sum(f.quantity for f in backend._ledger.fills) == 1000
    assert len(result["account"]["parents"]) == 1
    assert result["account"]["parents"][0]["status"] == "FILLED"
    assert all(s.state.executed_quantity == s.order.filled_quantity for s in result["sessions"])
    assert backend._ledger.cash_decimal == Decimal("9989990")  # Two real parents, cumulative minimum per parent.


@pytest.mark.parametrize("delay", [0,60,180,300])
def test_future_suffix_never_changes_earlier_human_dispatch(delay):
    plan, market = inputs()
    _, first = human_day(plan, market, delay=delay)
    cutoff = at("09:33:00")
    changed = market.model_copy(update={"bars":tuple(b if b.received_at < cutoff else b.model_copy(
        update={"open":Decimal("10.9"), "high":Decimal("11"), "low":Decimal("10.8"), "close":Decimal("10.9"),
                "volume_shares":Decimal("100")}) for b in market.bars)})
    _, second = human_day(plan, changed, delay=delay)
    def prefix(result):
        return [(a["kind"], a["visible_at"], a["dispatch_at"]) for a in result["account"]["human_operation_state"]["actions"]
                if a["dispatch_at"] and utc(a["dispatch_at"]) < cutoff]
    assert prefix(first) == prefix(second)


def test_cash_and_sellable_limits_do_not_turn_opportunity_into_a_fill():
    plan, market = inputs(side="SELL")
    backend, result = human_day(plan, market)
    assert not backend._ledger.fills and not backend._ledger.positions
    assert result["account"]["parents"][0]["status"] in {"NO_FILL", "UNAVAILABLE"}
    plan, market = inputs()
    backend, result = human_day(plan, market, cash="100")
    assert not backend._ledger.fills and backend._ledger.cash_decimal == 100
    assert result["account"]["parents"][0]["status"] in {"NO_FILL", "UNAVAILABLE"}


def test_unexpected_core_input_conflict_fails_day_instead_of_publishing_unavailable_success():
    plan, market = inputs()
    market = market.model_copy(update={"day_facts":market.day_facts.model_copy(update={"reference_price":None})})
    with pytest.raises(ValueError, match="LOCALSIM_HUMAN_CORE_UNAVAILABLE:TRADABILITY_UNKNOWN"):
        human_day(plan, market)


@pytest.mark.parametrize("delay,partial", [(0,False),(60,True)])
def test_cancel_race_keeps_partial_fill_and_does_not_use_invisible_filled_status_to_save_action(delay,partial):
    plan, market = inputs()
    if partial:
        market = market.model_copy(update={"bars":tuple(b.model_copy(update={"volume_shares":Decimal("300")}) for b in market.bars)})
    revision = plan.model_copy(update={"revision":2,"source_revision":"2"})
    backend, result = human_day(plan,market,delay=delay,events=[
        PlanEvent(available_at=plan.valid_from,plan=plan),PlanEvent(available_at=at("09:32:30"),plan=revision)])
    cancels = [a for a in result["account"]["human_operation_state"]["actions"] if a["kind"] == "CANCEL"]
    assert len(cancels) == 1 and cancels[0]["status"] == "COMPLETED"
    assert cancels[0]["outcome"]["cancellation_race"] is True
    assert result["account"]["human_operation_metrics"]["cancellation_races"] == 1
    assert sum(f.quantity for f in backend._ledger.fills) == 1000
    assert len(backend._records) == (2 if partial else 1)
    if partial:
        assert cancels[0]["outcome"]["filled_quantity_at_request"] == 0
        assert cancels[0]["outcome"]["filled_quantity_at_cancel"] == 300


def test_concurrent_partial_fills_follow_native_cash_fact_replay_order(tmp_path):
    from backend.services.simulation_execution.localsim.timing_research import backend_for
    from backend.services.simulation_execution.localsim.timing_portfolio import commit_portfolio_day, validate_day_ledger
    from backend.services.simulation_execution.localsim.timing_store import TimingFileRepository
    from backend.services.position_timing.intraday.research import CostContract
    plans, markets = full_markets()
    first = plans[0].model_copy(update={"plan_id":"zebra"})
    second = plans[1].model_copy(update={"plan_id":"middle"})
    third = first.model_copy(update={"plan_id":"alpha", "symbol":"600001.SH", "valid_from":at("09:32:00")})
    markets[third.symbol] = markets[first.symbol].model_copy(update={"symbol":third.symbol,
        "day_facts":markets[first.symbol].day_facts.model_copy(update={"symbol":third.symbol})})
    markets = {s:m.model_copy(update={"bars":tuple(b.model_copy(update={"volume_shares":Decimal("300")}) for b in m.bars)}) for s,m in markets.items()}
    initial = {"account_id":"account", "portfolio_id":"account", "package_id":"fixture",
               "origin_initial_cash_cny":"10000000", "cash_cny":"10000000", "positions":{}}
    cost = CostContract(open_rate="0.000095",close_rate="0.000595",minimum_cny="5")
    backend = backend_for(initial, markets[first.symbol], cost)
    result = run_human_portfolio_day(backend=backend, markets=markets,
        events=[PlanEvent(available_at=p.valid_from,plan=p) for p in (first,second,third)],
        initial_marks={s:Decimal("10") for s in markets}, policy=HumanOperationPolicy(reaction_delay_seconds=0), run_id="concurrent")
    assert sum(f.quantity for f in backend._ledger.fills) == 3000
    assert len({f.symbol for f in backend._ledger.fills if f.trade_time == at("09:34:00")}) == 2
    store = TimingFileRepository(tmp_path, "lsdm_"+"a"*48)
    commit_portfolio_day(store=store,result=result,input_identity="b"*64,day_index=0)
    validate_day_ledger(store.latest(), initial=initial,cost=cost)
