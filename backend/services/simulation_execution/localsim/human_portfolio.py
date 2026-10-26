"""Virtual human response before the existing LocalSIM order/fill boundary."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from decimal import Decimal
import heapq

from backend.services.position_timing.intraday.contracts import (
    CHINA, ExecutionView, Plan, digest, payload, sessions as trading_sessions, utc,
)
from backend.services.trading_core.models import OrderStatus, PositionLot

from .human_operation import HumanOperationPolicy, HumanOperationQueue, operation_snapshot
from .timing import CLOSED_LOOP_MODE, TimingFacade
from .timing_portfolio import TERMINAL, _account, _validate_markets, portfolio_day_result


@dataclass
class _Proposal:
    plan: Plan
    facade: TimingFacade
    executions: list = field(default_factory=list)
    reasons: set[str] = field(default_factory=set)

    @property
    def active(self):
        return self.executions[-1] if self.executions else None

    @property
    def filled_quantity(self):
        return sum(e.order.filled_quantity for e in self.executions)


def run_human_portfolio_day(*, backend, markets, events, initial_marks,
                            policy: HumanOperationPolicy, run_id: str) -> dict:
    """A single account, one operator, native feedback, and an atomic day result.

    Full bars are only supplied to native post-interval matching. Core evaluate
    sees visible_prefix, and human dispatch never inspects its execution bar.
    """
    if not markets:
        raise ValueError("LOCALSIM_HUMAN_MARKETS_REQUIRED")
    day = next(iter(markets.values())).trade_date
    start, terminal = (utc(datetime.combine(day, clock, CHINA)) for clock in (time(9, 30), time(14, 57)))
    _validate_markets(markets, day, start)
    if backend._records or backend._ledger.fills or backend._ledger.cash_entries:
        raise ValueError("LOCALSIM_HUMAN_FRESH_DAY_REQUIRED")
    backend._ledger.settle_trade_date(day)
    required = set(backend._ledger.positions) | {e.plan.symbol for e in events}
    if not required.issubset(markets) or any(s not in initial_marks or initial_marks[s] <= 0 for s in required):
        raise ValueError("LOCALSIM_HUMAN_MARK_OR_MARKET_MISSING")
    queue = HumanOperationQueue(account_id=backend.portfolio_id, policy=policy)
    initial, marks = _account(backend), dict(initial_marks)
    initial_nav = Decimal(str(backend._ledger.account_snapshot(
        prices={s: float(p) for s, p in marks.items()}, snapshot_time=start).nav))
    views, navs = [(start, initial)], [{"as_of": start.isoformat(), "nav_cny": str(initial_nav)}]
    changes, bars, observations = {}, {}, {}
    for event in sorted(events, key=lambda e: (e.available_at, e.plan.plan_id, e.plan.revision)):
        plan = event.plan
        if (plan.target_trade_date != day or plan.valid_until > terminal or plan.target_quantity is None
                or plan.allow_split or event.available_at > plan.valid_until or plan.symbol not in markets):
            raise ValueError("LOCALSIM_HUMAN_PLAN_CONTRACT_INVALID")
        changes.setdefault(max(start, event.available_at), []).append(event)
    for market in markets.values():
        for bar in market.bars:
            visible_at = max(bar.end, bar.received_at, bar.source_available_at or bar.end)
            if start <= bar.start < terminal and visible_at <= terminal:
                bars.setdefault(bar.start, {})[market.symbol] = bar
                observations.setdefault(visible_at, {})[market.symbol] = bar
    polls = {start + timedelta(minutes=n, seconds=10) for n in range(327)}
    clocks = list({start, terminal} | polls | set(changes) | set(bars) | set(observations)
                  | {e.plan.valid_until for e in events})
    heapq.heapify(clocks)
    scheduled, proposals = set(clocks), {}
    capacity_debt, cancellation_races = 0, 0

    def schedule(clock):
        if start <= clock <= terminal and clock not in scheduled:
            scheduled.add(clock)
            heapq.heappush(clocks, clock)

    def visible(clock):
        return max((v for v in views if v[0] <= clock), key=lambda v: v[0])

    def evaluate_proposal(proposal, clock):
        """Recheck with the same causal core at poll and actual dispatch time."""
        plan, execution = proposal.plan, proposal.active
        visible_at, account_view = visible(clock)
        if execution is not None and execution.facade is proposal.facade:
            feedback = execution.feedback(clock)
        else:
            feedback = ExecutionView(consumer_kind="SIMULATION", symbol=plan.symbol, trade_date=day,
                                     available_at=visible_at, confirmed_quantity=0, reserved_quantity=0,
                                     cash_cny=Decimal(account_view["cash_cny"]),
                                     sellable_quantity=account_view["positions"].get(plan.symbol, {}).get("available_quantity", 0),
                                     source_ref=run_id, feedback_id=digest([run_id, account_view, plan.revision]))
        lot = account_view["positions"].get(plan.symbol)
        feedback = feedback.model_copy(update={"cash_cny": Decimal(account_view["cash_cny"]),
                                               "sellable_quantity": lot["available_quantity"] if lot else 0,
                                               "available_at": max(feedback.available_at, visible_at),
                                               "feedback_id": digest([feedback.feedback_id, account_view])})
        return proposal.facade.evaluate(markets[plan.symbol], feedback, clock)

    def add_cancel(proposal, clock, reason):
        execution = proposal.active
        if execution is None or execution.order.status in TERMINAL:
            return
        action = queue.enqueue(plan_id=execution.facade.plan.plan_id, revision=execution.facade.plan.revision,
                               opportunity_id="cancel:" + execution.order.order_id,
                               symbol=execution.order.symbol, side=execution.order.side.value, kind="CANCEL",
                               visible_at=clock, valid_until=terminal)
        proposal.reasons.add(reason)
        schedule(action.ready_at)

    while clocks:
        clock = heapq.heappop(clocks)
        if clock > terminal:
            break
        for event in changes.get(clock, []):
            plan = event.plan
            if plan.plan_id not in proposals:
                if plan.revision != 1 or plan.lifecycle != "ACTIVE" or event.available_at > plan.valid_from:
                    raise ValueError("LOCALSIM_HUMAN_INITIAL_PLAN_INVALID")
                proposals[plan.plan_id] = _Proposal(plan, TimingFacade(plan, 0, mode=CLOSED_LOOP_MODE))
            else:
                proposal = proposals[plan.plan_id]
                old = proposal.plan
                if (plan.revision != old.revision + 1 or (plan.symbol, plan.side) != (old.symbol, old.side)):
                    raise ValueError("LOCALSIM_HUMAN_PLAN_REVISION_CONFLICT")
                queue.invalidate_plan(plan.plan_id, reason="PLAN_REVISED")
                add_cancel(proposal, clock, "PLAN_REVISED")
                proposal.plan, proposal.facade = plan, TimingFacade(plan, 0, mode=CLOSED_LOOP_MODE)
        for proposal in proposals.values():
            plan, execution = proposal.plan, proposal.active
            if clock >= plan.valid_until or plan.lifecycle != "ACTIVE":
                queue.invalidate_plan(plan.plan_id, reason="PLAN_EXPIRED_OR_CANCELLED")
                if execution is not None and clock >= plan.valid_until and execution.order.status not in TERMINAL:
                    capacity_debt += 1
                    execution.cancel("PLAN_HARD_EXPIRY")
                continue
            if clock not in polls and clock not in changes:
                continue
            if execution is not None and execution.facade.last_feedback_at is not None and clock < execution.facade.last_feedback_at:
                continue
            if execution is not None and execution.facade.plan.revision != plan.revision:
                if execution.order.status not in TERMINAL:
                    continue  # The predecessor must be confirmed before replacement.
                remaining = max(0, plan.target_quantity - proposal.filled_quantity)
                if remaining == 0:
                    continue
                if proposal.facade.plan.target_quantity != remaining:
                    replacement = plan.model_copy(update={"target_quantity": remaining,
                                                          "quantity_source_ref": digest([plan.quantity_source_ref, proposal.filled_quantity])})
                    proposal.facade = TimingFacade(replacement, 0, mode=CLOSED_LOOP_MODE)
            decision = evaluate_proposal(proposal, clock)
            if decision.status != "OPPORTUNITY":
                queue.invalidate_plan(plan.plan_id, revision=plan.revision, reason="OPPORTUNITY_" + decision.status)
                proposal.reasons.add("OPPORTUNITY_" + decision.status)
                if execution is not None and execution.order.status not in TERMINAL:
                    add_cancel(proposal, clock, "OPPORTUNITY_WITHDRAWN")
                continue
            if execution is None or execution.facade.plan.revision < plan.revision and execution.order.status in TERMINAL:
                action = queue.enqueue(plan_id=plan.plan_id, revision=plan.revision, opportunity_id=decision.opportunity_id,
                                       symbol=plan.symbol, side=plan.side,
                                       visible_at=decision.advice_available_at or decision.as_of,
                                       valid_until=min(plan.valid_until, decision.valid_until))
                schedule(action.ready_at)
        # Existing orders match independently; new human orders cannot use this bar.
        any_fill = False
        for proposal in proposals.values():
            for execution in proposal.executions:
                bar = bars.get(clock, {}).get(execution.order.symbol)
                if (bar is not None and execution.order.status not in TERMINAL
                        and (execution.facade.last_feedback_at is None or execution.facade.last_feedback_at <= clock)):
                    any_fill = bool(execution.step(bar)) or any_fill
        if any_fill:
            views.append((clock + timedelta(seconds=65), _account(backend)))
        in_session = any(left <= clock < right for left, right in trading_sessions(day)) and clock < terminal
        action = queue.claim(clock, in_trading_session=in_session)
        if action is not None:
            proposal = proposals[action.plan_id]
            if action.kind == "CANCEL":
                execution = next((e for e in proposal.executions if e.facade.plan.revision == action.revision), None)
                if execution is None:
                    raise ValueError("LOCALSIM_HUMAN_CANCEL_ORDER_MISSING")
                if execution.order.status == OrderStatus.FILLED:
                    cancellation_races += 1
                execution.cancel("HUMAN_CANCEL_CONFIRMED")
                outcome = {"status": execution.order.status.value, "order_id": execution.order.order_id}
            else:
                plan, facade = proposal.plan, proposal.facade
                decision = facade.decision
                if (plan.revision == action.revision and plan.lifecycle == "ACTIVE"
                        and plan.valid_from <= clock < min(plan.valid_until, action.valid_until)
                        and (decision is None or decision.as_of < clock)):
                    decision = evaluate_proposal(proposal, clock)
                if (plan.revision != action.revision or plan.lifecycle != "ACTIVE" or decision is None
                        or decision.status != "OPPORTUNITY" or facade.pending is None
                        or facade.pending["opportunity_id"] != action.opportunity_id
                        or not plan.valid_from <= clock < min(plan.valid_until, action.valid_until)):
                    outcome = {"status": "NO_ORDER", "reason": "STALE_OPPORTUNITY_AT_DISPATCH"}
                elif not markets[plan.symbol].day_facts.is_suspended:
                    execution = backend.open_timing_portfolio_execution(
                        plan=facade.plan, market=markets[plan.symbol], delay_seconds=0, run_id=run_id,
                        binding_id=backend.portfolio_id, submitted_at=clock, facade=facade)
                    proposal.executions.append(execution)
                    outcome = {"status": "SUBMITTED", "order_id": execution.order.order_id}
                else:
                    outcome = {"status": "NO_ORDER", "reason": "SUSPENDED_BY_SUSPEND_D"}
            queue.acknowledge(action.action_id, outcome)
            if queue.pending_count:
                schedule(clock + timedelta(seconds=policy.minimum_action_interval_seconds))
        if clock in observations:
            for symbol, bar in observations[clock].items():
                marks[symbol] = bar.close
        if clock in observations or clock == terminal:
            _, account_view = visible(clock)
            valuation = deepcopy(backend._ledger)
            valuation.cash = Decimal(account_view["cash_cny"])
            valuation.positions = {s: PositionLot.model_validate(p) for s, p in account_view["positions"].items()}
            navs.append({"as_of": clock.isoformat(), "nav_cny": str(valuation.account_snapshot(
                prices={s: float(p) for s, p in marks.items()}, snapshot_time=clock).nav)})
    executions = [e for p in proposals.values() for e in p.executions]
    for proposal in proposals.values():
        queue.invalidate_plan(proposal.plan.plan_id, kind=None, reason="COMMON_TERMINAL_1457")
        for execution in proposal.executions:
            if execution.order.status not in TERMINAL:
                capacity_debt += 1
            execution.cancel("COMMON_TERMINAL_1457")
    result = portfolio_day_result(backend=backend, sessions=executions, initial=initial, marks=marks,
                                  navs=navs, day=day, terminal=terminal, initial_nav=initial_nav)
    parents = []
    for proposal in proposals.values():
        fills = [f for e in proposal.executions for f in e.fills]
        qty = sum(f.quantity for f in fills)
        plan = proposal.plan
        reasons = proposal.reasons | {r for e in proposal.executions for r in e.reasons}
        actions = [a for a in queue.actions.values() if a.plan_id == plan.plan_id]
        reasons.update(a.outcome["reason"] for a in actions if a.outcome and "reason" in a.outcome)
        unavailable = not proposal.executions and "OPPORTUNITY_UNAVAILABLE" in reasons
        parents.append({"plan_id": plan.plan_id, "symbol": plan.symbol, "side": plan.side,
                        "target_quantity": plan.target_quantity, "filled_quantity": qty,
                        "status": "UNAVAILABLE" if unavailable else "FILLED" if qty == plan.target_quantity else "PARTIAL" if qty else "NO_FILL",
                        "order_created": bool(proposal.executions), "reason_codes": sorted(reasons),
                        "average_price_raw": str(sum(Decimal(str(f.price)) * f.quantity for f in fills) / qty) if qty else None,
                        "weighted_wait_seconds": str(sum(q * wait for e in proposal.executions for q, wait in e.waits)),
                        "human_actions": [a.model_dump(mode="json") for a in actions],
                        "sessions": [e.snapshot(include_account=False) for e in proposal.executions]})
    account = result["account"]
    account.update(parents=payload(parents), operation=operation_snapshot(policy),
                   human_operation_state=queue.snapshot(),
                   human_operation_metrics={"action_count": sum(a.dispatch_at is not None for a in queue.actions.values()),
                                            "maximum_queue": queue.maximum_queue,
                                            "pending_cancellation_at_deadline": capacity_debt,
                                            "cancellation_races": cancellation_races,
                                            "expired_actions": sum(a.status == "INVALIDATED" for a in queue.actions.values())})
    return result
