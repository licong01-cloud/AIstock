"""Public W15 envelope on the real human queue, OMS, fees and confirmed fills."""

from datetime import timedelta
from decimal import Decimal

import pytest

from backend.services.position_timing.intraday.contracts import ExecutionView, PriceConstraint, PriceInterval, utc
from backend.services.position_timing.intraday.replay import PlanEvent
from backend.services.position_timing.intraday.research import CostContract
from backend.services.simulation_execution.localsim import wait_budget_execution as wait
from backend.services.simulation_execution.localsim.human_operation import HumanOperationPolicy
from backend.services.simulation_execution.localsim.human_portfolio import run_human_portfolio_day
from backend.services.simulation_execution.localsim.timing_research import backend_for
from backend.tests.simulation_execution.test_localsim_human_operation import full_markets
from backend.tests.simulation_execution.test_localsim_position_timing import at, inputs


def run_day(plans, markets, *, control=wait.CONTROL_ID, events=None, account_id="account"):
    cost = CostContract(open_rate="0.000095", close_rate="0.000595", minimum_cny="5")
    backend = backend_for({"portfolio_id": account_id, "package_id": "fixture", "origin_initial_cash_cny": "10000000",
        "cash_cny": "10000000", "positions": {}}, next(iter(markets.values())), cost)
    result = run_human_portfolio_day(backend=backend, markets=markets,
        events=events or [PlanEvent(available_at=p.valid_from, plan=p) for p in plans],
        initial_marks={s: Decimal("10") for s in markets}, policy=HumanOperationPolicy(reaction_delay_seconds=60),
        run_id="w15-functional", optimized=True, research_control=control, research_audit=True)
    return backend, result


def test_outer_wait_never_creates_pending_and_json_control_restore_keeps_release():
    plan, market = inputs()
    facade = wait.WaitBudgetFacade(plan)
    feedback = ExecutionView(consumer_kind="SIMULATION", symbol=plan.symbol, trade_date=plan.target_trade_date,
        available_at=at("09:30:00"), confirmed_quantity=0, reserved_quantity=0, cash_cny="10000000", sellable_quantity=0,
        source_ref="account", feedback_id="opening")
    first = facade.evaluate(market, feedback, at("09:31:10"))
    assert first.status == "WAIT" and facade.pending is None
    assert facade.control_decision.underlying_decision.status == "OPPORTUNITY"
    assert first.opportunity_id != facade.control_decision.underlying_decision.opportunity_id
    state = facade.control_state.model_dump(mode="json")
    restored = wait.WaitBudgetFacade(plan, control_state=state)
    def market_at(clock):
        end = at(clock).replace(second=0)
        bar = market.bars[-1].model_copy(update={"start": end-timedelta(minutes=1), "end": end,
                                                "received_at": end+timedelta(seconds=5)})
        return market.model_copy(update={"bars": (bar,)})
    before = restored.evaluate(market_at("09:46:09"), feedback, at("09:46:09"))
    assert before.status == "WAIT" and restored.pending is None
    released = restored.evaluate(market_at("09:46:10"), feedback, at("09:46:10"))
    assert released.status == "OPPORTUNITY" and released.advice_available_at == at("09:46:10")
    assert restored.pending["advice_available_at"] == at("09:46:10")
    assert released.decision_id == restored.control_decision.decision_id
    revised = plan.model_copy(update={"revision": 2})
    restored.replace_plan(revised)
    assert restored.control_state is None and restored.pending is None


def test_real_w15_dispatch_uses_outer_clock_and_confirmed_future_bar(monkeypatch):
    plans, markets = full_markets()
    original, observed = wait.evaluate_wait_control, []
    def checked(plan, market, execution, policy, state, clock):
        assert all(max(b.end, b.received_at, b.source_available_at or b.end) <= clock for b in market.bars)
        value = original(plan, market, execution, policy, state, clock)
        observed.append(value[0])
        return value
    monkeypatch.setattr(wait, "evaluate_wait_control", checked)
    backend, result = run_day(plans, markets)
    assert observed and any(d.status == "WAIT" for d in observed) and any(d.status == "OPPORTUNITY" for d in observed)
    submits = [a for a in result["account"]["human_operation_state"]["actions"] if a["kind"] == "SUBMIT"]
    assert submits and all(utc(a["visible_at"]) >= at("09:46:10") for a in submits)
    dispatched = {a["plan_id"]: a for a in submits if a["dispatch_at"] is not None}
    assert backend._ledger.fills
    for session in result["sessions"]:
        action = dispatched[session.facade.plan.plan_id]
        assert utc(action["ready_at"]) == utc(action["visible_at"]) + timedelta(seconds=60)
        assert all(f.trade_time > utc(action["dispatch_at"]) for f in session.fills)
        assert session.state.executed_quantity == session.order.filled_quantity
        assert session.feedback_history[-1].confirmed_quantity == session.order.filled_quantity
    assert len(backend._ledger.cash_entries) == len(backend._ledger.fills)
    assert all(p.available_quantity == 0 for p in backend._ledger.positions.values())
    for parent in result["account"]["parents"]:
        audit = parent["research_timing"]
        assert audit["signal_revisions"][0]["batch_started_at"] == at("09:31:10").isoformat()
        assert audit["first_fill_at"] is None or utc(audit["first_fill_at"]) > utc(audit["first_dispatch_at"])


@pytest.mark.parametrize("cancel", [False, True])
def test_revised_or_cancelled_plan_cannot_reuse_the_old_wait_budget(cancel):
    plans, markets = full_markets()
    plan = plans[0]
    revised = plan.model_copy(update={"revision": 2})
    events = [PlanEvent(available_at=plan.valid_from, plan=plan), PlanEvent(available_at=at("09:40:00"), plan=revised)]
    if cancel:
        events.append(PlanEvent(available_at=at("09:54:00"), plan=revised.model_copy(update={"revision": 3, "lifecycle": "CANCELLED"})))
    backend, result = run_day([plan], {plan.symbol: markets[plan.symbol]}, events=events)
    if cancel:
        assert not backend._ledger.fills
    else:
        assert backend._ledger.fills and min(f.trade_time for f in backend._ledger.fills) >= at("09:57:00")
    revisions = result["account"]["parents"][0]["research_timing"]["signal_revisions"]
    assert revisions[0]["first_outer_opportunity_at"] is None


def test_hard_limit_and_unknown_control_cannot_be_waived():
    plans, markets = full_markets()
    plan = plans[0].model_copy(update={"price_constraint": PriceConstraint(mode="HARD",
        intervals=(PriceInterval(lower="9.5", upper="9.9"),), source_ref="hard", source_sha256="b"*64,
        valid_from=at("09:30:00"), valid_until=at("14:57:00"))})
    backend, result = run_day([plan], {plan.symbol: markets[plan.symbol]})
    assert not backend._ledger.fills and result["account"]["parents"][0]["status"] == "NO_FILL"
    assert not result["account"]["human_operation_state"]["actions"]
    with pytest.raises(ValueError, match="CONTROL_UNKNOWN"):
        run_day([plan], {plan.symbol: markets[plan.symbol]}, control="W15-typo")


def test_partial_fills_expire_with_native_confirmation_and_parent_minimum_fee():
    plans, markets = full_markets()
    plan = plans[0].model_copy(update={"valid_until": at("09:51:00")})
    market = markets[plan.symbol]
    market = market.model_copy(update={"bars": tuple(b.model_copy(update={"volume_shares": Decimal("100")}) for b in market.bars)})
    backend, result = run_day([plan], {plan.symbol: market})
    parent = result["account"]["parents"][0]
    assert parent["status"] == "PARTIAL" and 0 < parent["filled_quantity"] < plan.target_quantity
    assert sum(f.quantity for f in backend._ledger.fills) == parent["filled_quantity"]
    assert sum(e.fee for e in backend._ledger.cash_entries) == Decimal("5")
    assert result["sessions"][0].execution_state.is_terminal
    assert not result["account"]["human_operation_state"]["inflight"]


def test_account_and_plan_ids_do_not_change_symbol_priority_or_economics():
    plans, markets = full_markets()
    first, _ = run_day(plans, markets, account_id="original")
    renamed = [p.model_copy(update={"plan_id": str(100-i)}) for i, p in enumerate(plans)]
    second, _ = run_day(renamed, markets, account_id="clone")
    def trades(backend):
        return [(f.symbol,f.side,f.quantity,f.price,f.trade_time) for f in backend._ledger.fills]
    assert trades(first) == trades(second) and first._ledger.cash_decimal == second._ledger.cash_decimal
