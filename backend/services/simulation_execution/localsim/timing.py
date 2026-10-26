"""LocalSIM-owned intent/confirmation bridge to the unique intraday timing SDK.

No rule, ledger, fee or matching implementation lives here. The open projection
is an execution-only interval proxy; it is never supplied to signal features.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from backend.execution_algos.base_algo import BaseExecutionAlgo, OrderState, StepResult
from backend.execution_algos.board_lot import board_lot_rule, round_to_board_lot
from backend.services.position_timing.intraday import evaluate
from backend.services.position_timing.intraday.contracts import (
    CHINA, Decision, ExecutionView, MarketView, MinuteBar as ObservedBar, Plan,
    State, digest, payload, sessions, utc,
)
from backend.services.position_timing.intraday.inputs import visible_prefix
from backend.services.strategy_package.execution_policy import (
    compute_execution_policy_sha256, normalize_execution_policy_json,
)
from backend.services.trading_core.execution_algo_adapter import ExecutionAlgoAdapter
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import (
    Fill, MinuteBar, Order, OrderEvent, OrderIntent, OrderSide, OrderStatus, PositionLot,
)
from backend.services.trading_core.ledger import CashLedgerEntry

from .models import LocalSimExecutionRuntimeStatus, LocalSimExecutionStateV1
from .economic import local_sim_fact_payload


ALGO_CODE = "POSITION_TIMING_INTRADAY_V1"
POLICY_ID = "localsim_position_timing_v1"
FROZEN_MODE = "FROZEN_OPPORTUNITY_DELAY_DIAGNOSTIC"
CLOSED_LOOP_MODE = "CONFIRMED_FEEDBACK_CLOSED_LOOP"


def policy_snapshot() -> dict:
    policy = normalize_execution_policy_json({
        "algo_code": ALGO_CODE, "bar_freq": "1m", "fallback_algo_code": None,
        "algo_config": {"proxy": "NEXT_ELIGIBLE_BAR_OPEN", "strict_after": True,
                        "allow_partial_fill": True},
    })
    return {"policy_version_id": POLICY_ID, "policy_sha256": compute_execution_policy_sha256(policy),
            "policy_json": policy}


def require_timing_policy(snapshot: dict) -> None:
    expected = policy_snapshot()
    if (snapshot.get("policy_version_id", snapshot.get("validated_execution_policy_id")) != POLICY_ID
            or snapshot.get("policy_sha256") != expected["policy_sha256"]
            or snapshot.get("policy_json") != expected["policy_json"]):
        raise ValueError("LOCALSIM_TIMING_POLICY_IDENTITY_MISMATCH")


class TimingFacade(BaseExecutionAlgo):
    ALGO_CODE = ALGO_CODE
    HANDLES_LIMIT_STATE = True
    HANDLES_ZERO_VOLUME = True

    def __init__(self, plan: Plan, delay_seconds: int, *, mode: str):
        super().__init__()
        if delay_seconds not in (0, 60, 180, 300) or mode not in (FROZEN_MODE, CLOSED_LOOP_MODE):
            raise ValueError("LOCALSIM_TIMING_UNSUPPORTED_CONTRACT")
        if plan.target_quantity is None:
            raise ValueError("LOCALSIM_TIMING_QUANTITY_REQUIRED")
        self.plan, self.delay_seconds, self.mode = plan, delay_seconds, mode
        self.core_state: State | None = None
        self.decision: Decision | None = None
        self.pending: dict | None = None
        self.closed_opportunities: set[str] = set()
        self.confirmed_fill_ids: set[str] = set()
        self.last_batch = 0
        self.last_confirmed_at: datetime | None = None
        self.last_feedback_at: datetime | None = None
        self._last_no_fill_reason: str | None = None
        self._last_no_fill_context: dict = {}

    def evaluate(self, market: MarketView, execution: ExecutionView, as_of: datetime) -> Decision:
        if self.mode != CLOSED_LOOP_MODE or execution.consumer_kind != "SIMULATION":
            raise ValueError("LOCALSIM_TIMING_COMPLETE_FEEDBACK_REQUIRED")
        if self.last_feedback_at is not None and utc(as_of) < self.last_feedback_at:
            raise ValueError("LOCALSIM_TIMING_EVALUATION_PRECEDES_CONFIRMED_ACCOUNT_CLOCK")
        prefix = visible_prefix(market, as_of)
        decision, self.core_state = evaluate(
            self.plan, prefix, execution, self.plan.policy, self.core_state, as_of,
        )
        self.observe(decision)
        return decision

    def observe(self, decision: Decision) -> None:
        if (decision.plan_id, decision.revision, decision.policy_sha256) != (
                self.plan.plan_id, self.plan.revision, self.plan.policy_sha256):
            raise ValueError("LOCALSIM_TIMING_DECISION_IDENTITY_MISMATCH")
        if (decision.price_constraint != self.plan.price_constraint
                or not 1 <= decision.target_fraction_numerator <= decision.target_fraction_denominator <= 3
                or decision.advice_available_at is not None and decision.advice_available_at > decision.as_of):
            raise ValueError("LOCALSIM_TIMING_DECISION_CONTRACT_INVALID")
        if self.decision and decision.as_of < self.decision.as_of:
            raise ValueError("LOCALSIM_TIMING_DECISION_CLOCK_REVERSED")
        self.decision = decision
        if decision.status != "OPPORTUNITY":
            self.invalidate("OPPORTUNITY_" + decision.status)
        elif decision.opportunity_id not in self.closed_opportunities:
            if self.pending is None:
                self.pending = {"opportunity_id": decision.opportunity_id,
                                "advice_available_at": decision.advice_available_at or decision.as_of,
                                "batch_id": decision.batch_id}
            elif self.pending["opportunity_id"] != decision.opportunity_id:
                raise ValueError("LOCALSIM_TIMING_PENDING_BATCH_NOT_CONFIRMED")

    def invalidate(self, reason: str) -> None:
        if self.pending:
            self.closed_opportunities.add(self.pending["opportunity_id"])
        self.pending = None
        self._last_no_fill_reason = reason

    def replace_plan(self, plan: Plan) -> None:
        if (plan.plan_id, plan.symbol, plan.side, plan.target_trade_date) != (
                self.plan.plan_id, self.plan.symbol, self.plan.side, self.plan.target_trade_date):
            raise ValueError("LOCALSIM_TIMING_PLAN_IDENTITY_MISMATCH")
        if plan.revision <= self.plan.revision:
            if plan != self.plan:
                raise ValueError("LOCALSIM_TIMING_REVISION_CONFLICT")
            return
        self.invalidate("PLAN_REVISED")
        self.plan, self.core_state, self.decision = plan, None, None

    def compute_step(self, state: OrderState, bar_data: dict, market_context: dict) -> StepResult | None:
        self._last_no_fill_reason = "NO_EXECUTABLE_INTENT"
        self._last_no_fill_context = {}
        decision, pending, plan = self.decision, self.pending, self.plan
        start = utc(market_context["execution_start"])
        if plan.target_quantity != state.total_quantity:
            raise ValueError("LOCALSIM_TIMING_REVISED_QUANTITY_REQUIRES_NEW_ORDER")
        if not pending or not decision or self.is_complete(state):
            return None
        if (plan.lifecycle != "ACTIVE" or not plan.valid_from <= start < plan.valid_until
                or not any(left <= start < right for left, right in sessions(plan.target_trade_date))):
            self.invalidate("PLAN_EXPIRED_OR_CANCELLED")
            return None
        if decision.as_of > start:
            raise ValueError("LOCALSIM_TIMING_FUTURE_DECISION")
        if start >= decision.valid_until:
            self.invalidate("OPPORTUNITY_EXPIRED")
            return None
        if start <= utc(pending["advice_available_at"]) + timedelta(seconds=self.delay_seconds):
            self._last_no_fill_reason = "WAITING_FOR_DELAY"
            return None
        price = Decimal(str(bar_data["open"]))
        constraint = plan.price_constraint
        if constraint.mode == "HARD" and (
                not constraint.intervals
                or constraint.valid_from is not None and start < constraint.valid_from
                or constraint.valid_until is not None and start >= constraint.valid_until
                or not any(b.lower <= price <= b.upper for b in constraint.intervals)):
            self._last_no_fill_reason = "HARD_PRICE_CONSTRAINT"
            return None
        cumulative = state.total_quantity * decision.target_fraction_numerator // decision.target_fraction_denominator
        cumulative = round_to_board_lot(cumulative, state.symbol, side=state.side,
                                       allow_sell_residual=cumulative == state.total_quantity)
        remaining = min(state.total_quantity, cumulative) - state.executed_quantity
        quantity = min(remaining, int(bar_data["volume"]), market_context["quantity_capacity"])
        residual_exit = state.side == "SELL" and remaining == market_context["position_quantity"]
        quantity = round_to_board_lot(quantity, state.symbol, side=state.side,
                                     allow_sell_residual=residual_exit and quantity == remaining)
        if not residual_exit and 0 < remaining - quantity < board_lot_rule(state.symbol)[0]:
            quantity = round_to_board_lot(remaining - board_lot_rule(state.symbol)[0], state.symbol)
        if quantity <= 0:
            self._last_no_fill_reason = "QUANTITY_OR_LIQUIDITY_LIMIT"
            return None
        # No executed/reserved mutation. Only the subsequent account confirmation
        # may acknowledge this proposal, including any capital-fit reduction.
        self._last_no_fill_reason = None
        return StepResult(state.symbol, state.side, quantity, float(price), "NEXT_ELIGIBLE_BAR_OPEN")

    def handle_limit_state_no_fill(self, state: OrderState, *, reason: str, context: dict) -> None:
        self._last_no_fill_reason, self._last_no_fill_context = reason, context

    def confirm(self, state: OrderState, order: Any, fills: list[Fill], *, available_at: datetime) -> None:
        unseen = [f for f in fills if f.fill_id not in self.confirmed_fill_ids]
        if order.filled_quantity != state.executed_quantity + sum(f.quantity for f in unseen):
            raise ValueError("LOCALSIM_TIMING_CONFIRMATION_QUANTITY_CONFLICT")
        self.confirmed_fill_ids.update(f.fill_id for f in unseen)
        if unseen:
            self.last_feedback_at = available_at
        state.executed_quantity = order.filled_quantity
        state.is_complete = order.status == OrderStatus.FILLED
        if unseen and self.decision:
            target = state.total_quantity * self.decision.target_fraction_numerator // self.decision.target_fraction_denominator
            target = round_to_board_lot(target, state.symbol, side=state.side,
                                       allow_sell_residual=target == state.total_quantity)
            if state.executed_quantity >= target:
                self.last_batch, self.last_confirmed_at = self.decision.batch_id, available_at
                self.invalidate("BATCH_CONFIRMED")


class _TimingAlgoAdapter(ExecutionAlgoAdapter):
    """Explicit LocalSIM dependency injection; no global algorithm registration."""

    def __init__(self, facade: TimingFacade, state: OrderState):
        self.facade, self.state = facade, state

    def create_state(self, order, algo_code, config):
        if algo_code != ALGO_CODE or order.quantity != self.state.total_quantity:
            raise ValueError("LOCALSIM_TIMING_ADAPTER_IDENTITY_MISMATCH")
        return self.facade, self.state


class TimingExecution:
    """One LocalSIM order using its backend's existing OMS, ledger and engine."""

    def __init__(self, backend, *, plan: Plan, market: MarketView, delay_seconds: int,
                 mode: str, run_id: str, binding_id: str,
                 submitted_at: datetime | None = None, facade: TimingFacade | None = None,
                 order_intent: OrderIntent | None = None):
        if (market.symbol, market.trade_date) != (plan.symbol, plan.target_trade_date):
            raise ValueError("LOCALSIM_TIMING_MARKET_IDENTITY_MISMATCH")
        if (market.day_facts.symbol, market.day_facts.trade_date, market.calendar.trade_date) != (
                plan.symbol, plan.target_trade_date, plan.target_trade_date):
            raise ValueError("LOCALSIM_TIMING_DAY_FACT_IDENTITY_MISMATCH")
        self.backend, self.market = backend, market
        if facade is not None and (facade.plan != plan or facade.delay_seconds != delay_seconds or facade.mode != mode):
            raise ValueError("LOCALSIM_TIMING_FACADE_IDENTITY_MISMATCH")
        self.facade = facade if facade is not None else TimingFacade(plan, delay_seconds, mode=mode)
        self.submitted_at = utc(submitted_at) if submitted_at is not None else None
        if self.submitted_at is not None and not plan.valid_from <= self.submitted_at < plan.valid_until:
            raise ValueError("LOCALSIM_TIMING_SUBMISSION_WINDOW_INVALID")
        self.state = self.facade.init_order(plan.symbol, plan.side, plan.target_quantity)
        self.run_id, self.binding_id = run_id, binding_id
        intent = order_intent if order_intent is not None else OrderIntent(intent_id="lst_" + digest([run_id, plan.plan_id, plan.revision]),
                             package_id=backend.package_id, portfolio_id=backend.portfolio_id,
                             symbol=plan.symbol, side=plan.side, quantity=plan.target_quantity,
                             target_trade_date=plan.target_trade_date,
                             metadata={"execution_policy": POLICY_ID, "plan_sha256": digest(plan)})
        if (intent.package_id, intent.portfolio_id, intent.symbol, intent.side.value, intent.quantity,
                intent.target_trade_date) != (backend.package_id, backend.portfolio_id, plan.symbol, plan.side,
                                              plan.target_quantity, plan.target_trade_date):
            raise ValueError("LOCALSIM_RESEARCH_ORDER_INTENT_CONFLICT")
        self.order = backend._oms.create_order(intent)
        order_clock = self.submitted_at if self.submitted_at is not None else plan.valid_from
        self.order = self.order.model_copy(update={"order_id": "lsto_" + digest([run_id, plan.plan_id, plan.revision]),
                                                  "created_at": order_clock, "updated_at": order_clock})
        self.engine = MinuteExecutionEngine(adapter=_TimingAlgoAdapter(self.facade, self.state), oms=backend._oms)
        self.execution_state = LocalSimExecutionStateV1(
            run_id=run_id, binding_id=binding_id, trade_date=plan.target_trade_date, plan_id=plan.plan_id,
            intent_id=intent.intent_id, algo_instance_id=run_id, portfolio_id=backend.portfolio_id,
            order_id=self.order.order_id, symbol=plan.symbol, side=plan.side,
            total_quantity=plan.target_quantity, filled_quantity=0, remaining_quantity=plan.target_quantity,
            algo_code=ALGO_CODE, order_status="SUBMITTED", runtime_status="ACTIVE",
            schedule_version=POLICY_ID, causality_cursor=order_clock, idempotency_key=digest(intent),
        )
        self.fills: list[Fill] = []
        self.events: list[Any] = []
        self.bar_hashes: dict[str, str] = {}
        self.waits: list[tuple[int, Decimal]] = []
        self.reasons: list[str] = []
        self.feedback_history = [self._account_feedback(plan.valid_from)]

    def _account_feedback(self, available_at: datetime) -> ExecutionView:
        lot = self.backend._ledger.positions.get(self.order.symbol)
        return ExecutionView(consumer_kind="SIMULATION", symbol=self.order.symbol,
                             trade_date=self.facade.plan.target_trade_date, available_at=available_at,
                             confirmed_quantity=self.order.filled_quantity, reserved_quantity=0,
                             cash_cny=str(self.backend._ledger.cash_decimal),
                             sellable_quantity=lot.available_quantity if lot else 0,
                             source_ref=self.run_id, feedback_id=digest([self.run_id, [f.fill_id for f in self.fills]]),
                             last_confirmed_batch=self.facade.last_batch,
                             last_confirmed_at=self.facade.last_confirmed_at)

    def feedback(self, as_of: datetime) -> ExecutionView:
        """Never relabel a later acknowledgement as an earlier account view."""
        available = [view for view in self.feedback_history if view.available_at <= utc(as_of)]
        if not available:
            raise ValueError("LOCALSIM_TIMING_FEEDBACK_NOT_YET_AVAILABLE")
        return available[-1]

    def execution_bar(self, bar: ObservedBar, facts) -> MinuteBar:
        """Timing's frozen next-open proxy; native research overrides only this input adapter."""
        return MinuteBar(symbol=self.order.symbol, bar_time=bar.start.astimezone(CHINA),
                         open=float(bar.open), high=float(bar.open), low=float(bar.open), close=float(bar.open),
                         volume=int(bar.volume_shares), is_suspended=False,
                         limit_up=float(facts.limit_up) if facts.has_daily_limit else None,
                         limit_down=float(facts.limit_down) if facts.has_daily_limit else None)

    def confirmed_algo_state(self, engine_state):
        return self.engine._dump_algo_state(self.state)

    def step(self, bar: ObservedBar) -> list[Fill]:
        if self.submitted_at is not None and utc(bar.start) <= self.submitted_at:
            return []  # Human dispatch cannot fill at that clock or an earlier bar.
        key, identity = bar.start.isoformat(), bar.content_sha256()
        if key in self.bar_hashes:
            if self.bar_hashes[key] != identity:
                raise ValueError("LOCALSIM_TIMING_BAR_REVISION_CONFLICT")
            return []
        if self.bar_hashes and bar.start <= max(utc(k) for k in self.bar_hashes):
            raise ValueError("LOCALSIM_TIMING_BAR_CLOCK_REVERSED")
        if self.order.status in (OrderStatus.CANCELLED, OrderStatus.FILLED, OrderStatus.REJECTED):
            return []
        facts = self.market.day_facts
        if (facts.available_at > bar.start or self.market.calendar.available_at > bar.start
                or facts.is_suspended is None
                or not facts.is_suspended and facts.has_daily_limit is None
                or facts.has_daily_limit and (facts.limit_up is None or facts.limit_down is None)):
            raise ValueError("LOCALSIM_TIMING_TRADABILITY_UNKNOWN")
        self.bar_hashes[key] = identity
        if facts.is_suspended or not self.market.calendar.is_trading_day:
            self.reasons.append("SUSPENDED_OR_NON_TRADING_DAY")
            return []
        lot = self.backend._ledger.positions.get(self.order.symbol)
        capacity = self.order.remaining_quantity if self.order.side == OrderSide.BUY else min(
            self.order.remaining_quantity, lot.available_quantity if lot else 0)
        # The full interval's volume is a disclosed ex-post liquidity proxy,
        # not proof of opening-auction queue access or a signal feature.
        proxy = self.execution_bar(bar, facts)
        before = self.backend._snapshot_ledger_state()
        state_before = deepcopy(self.state)
        facade_before = deepcopy(self.facade.__dict__)
        advice = utc(self.facade.pending["advice_available_at"]) if self.facade.pending else None
        try:
            order, engine_state, fills, events = self.engine.execute_order_incremental(
                order=self.order, execution_state=self.execution_state, new_bars=[proxy],
                algo_code=self.execution_state.algo_code,
                algo_config=getattr(self, "native_algo_config", None),
                market_context={"execution_start": bar.start, "quantity_capacity": capacity,
                                "position_quantity": lot.quantity if lot else 0},
            )
            order, engine_state, fills, events, capital = self.backend._fit_buy_fills_to_available_cash(
                order_before=self.order, engine_state=engine_state, final_order=order, fills=fills, events=events,
            )
            for fill in fills:
                self.backend._ledger.apply_fill(fill)
            available_at = max(bar.end, bar.received_at, bar.source_available_at or bar.end)
            self.facade.confirm(self.state, order, fills, available_at=available_at)
        except Exception:
            self.backend._restore_ledger_state(before)
            self.state.__dict__.update(state_before.__dict__)
            self.facade.__dict__.clear()
            self.facade.__dict__.update(facade_before)
            self.bar_hashes.pop(key)
            raise
        # First advice time survives opportunity renewal and confirmation.
        if advice is not None:
            self.waits.extend((f.quantity, Decimal(str((bar.start - advice).total_seconds()))) for f in fills)
        sequence = self.execution_state.sequence + 1
        engine_state = engine_state.model_copy(update={"algo_state": self.confirmed_algo_state(engine_state),
                                                       "order_status": order.status.value,
                                                       "runtime_status": (LocalSimExecutionRuntimeStatus.FILLED
                                                                          if order.status == OrderStatus.FILLED
                                                                          else LocalSimExecutionRuntimeStatus.ACTIVE),
                                                       "sequence": sequence,
                                                       "idempotency_key": digest([self.run_id, sequence, identity]),
                                                       "state_hash": ""})
        self.order, self.execution_state = order, LocalSimExecutionStateV1.model_validate(engine_state.model_dump())
        self.fills.extend(fills)
        self.events.extend(e for e in events if e.event_type.value != "NO_FILL")
        if fills:
            self.feedback_history.append(self._account_feedback(available_at))
        if capital:
            self.reasons.append("INSUFFICIENT_CASH")
        elif self.facade._last_no_fill_reason:
            self.reasons.append(self.facade._last_no_fill_reason)
        # Publish only after the account and facade transition has committed.
        # A subscriber failure must not undo a fill already observed by others.
        self.backend._sync_timing_execution(self, fills)
        return fills

    def cancel(self, reason: str = "PLAN_EXPIRED") -> None:
        if self.order.status == OrderStatus.CANCELLED:
            return  # The confirmed cancellation is already the durable outcome.
        self.facade.invalidate(reason)
        if self.order.status not in (OrderStatus.CANCELLED, OrderStatus.FILLED, OrderStatus.REJECTED):
            self.order, event = self.backend._oms.cancel_order(self.order, reason)
            self.events.append(event)
        if self.order.status == OrderStatus.CANCELLED:
            state = self.execution_state.model_dump(mode="python")
            state.update(order_status="CANCELLED", runtime_status=LocalSimExecutionRuntimeStatus.CANCELLED,
                         terminal_reason=reason, sequence=self.execution_state.sequence + 1,
                         idempotency_key=digest([self.run_id, "cancel", reason]), state_hash="")
            self.execution_state = LocalSimExecutionStateV1.model_validate(state)
        self.reasons.append(reason)
        self.backend._sync_timing_execution(self, [])

    def update_plan(self, plan: Plan) -> dict:
        """A quantity revision cancels the old parent; the planner owns replacement."""
        previous = self.facade.plan
        self.facade.replace_plan(plan)
        if plan.lifecycle != "ACTIVE" or plan.target_quantity != previous.target_quantity:
            self.cancel("PLAN_CANCELLED" if plan.lifecycle != "ACTIVE" else "PLAN_REVISED_REQUIRES_NEW_INTENT")
            return {"status": "CANCELLED_REQUIRES_NEW_INTENT", "confirmed_quantity": self.order.filled_quantity,
                    "remaining_quantity": max(0, plan.target_quantity - self.order.filled_quantity)}
        return {"status": "AWAITING_FRESH_OPPORTUNITY", "confirmed_quantity": self.order.filled_quantity}

    def snapshot(self, *, include_account: bool = True) -> dict:
        ledger = self.backend._ledger
        receipt = {"schema_version": "localsim_timing_account_receipt_v1", "run_id": self.run_id,
                "portfolio_id": self.backend.portfolio_id, "package_id": self.backend.package_id,
                "market_sha256": digest(self.market), "execution_policy": policy_snapshot(),
                "binding_id": self.binding_id, "plan": payload(self.facade.plan),
                "mode": self.facade.mode, "delay_seconds": self.facade.delay_seconds,
                "order": self.order.model_dump(mode="json"),
                "fills": [f.model_dump(mode="json") for f in self.fills],
                "events": [e.model_dump(mode="json") for e in self.events],
                "execution_state": self.execution_state.model_dump(mode="json"),
                "core_state": payload(self.facade.core_state) if self.facade.core_state else None,
                "decision": payload(self.facade.decision) if self.facade.decision else None,
                "pending": payload(self.facade.pending),
                "closed_opportunities": sorted(self.facade.closed_opportunities),
                "confirmed_fill_ids": sorted(self.facade.confirmed_fill_ids),
                "last_batch": self.facade.last_batch,
                "last_confirmed_at": payload(self.facade.last_confirmed_at),
                "last_feedback_at": payload(self.facade.last_feedback_at),
                "feedback_history": payload(self.feedback_history),
                "bar_hashes": self.bar_hashes, "waits": payload(self.waits), "reasons": self.reasons,
                "liquidity_proxy": "EX_POST_INTERVAL_VOLUME_NOT_OPEN_QUEUE_PROOF"}
        if self.submitted_at is not None:
            receipt["human_submitted_at"] = payload(self.submitted_at)
        if include_account:
            receipt.update(cash_cny=str(ledger.cash_decimal),
                           positions={s: p.model_dump(mode="json") for s, p in ledger.positions.items()},
                           cash_entries=[local_sim_fact_payload(e, fact_type="cash_entry") for e in ledger.cash_entries])
        else:
            # The portfolio day owns one account and one economic journal. A
            # parent audit must not duplicate every other parent's cash facts,
            # nor be accepted by single-account restore as a complete receipt.
            receipt["schema_version"] = "localsim_timing_parent_audit_v1"
            receipt["account_scope"] = "PORTFOLIO_DAY_RECEIPT"
        return receipt

    @classmethod
    def restore(cls, backend, market: MarketView, receipt: dict):
        """Restore a verified committed receipt without reapplying any fill."""
        if (receipt["schema_version"] != "localsim_timing_account_receipt_v1"
                or receipt["portfolio_id"] != backend.portfolio_id
                or receipt["package_id"] != backend.package_id or digest(market) != receipt["market_sha256"]
                or receipt["execution_policy"] != policy_snapshot()):
            raise ValueError("LOCALSIM_TIMING_RESTORE_IDENTITY_MISMATCH")
        plan = Plan.model_validate(receipt["plan"])
        session = backend.open_timing_execution(plan=plan, delay_seconds=receipt["delay_seconds"],
                                               mode=receipt["mode"], run_id=receipt["run_id"],
                                               binding_id=receipt["binding_id"],
                                               submitted_at=receipt.get("human_submitted_at"))
        backend._records.pop(session.handle.handle_id)
        backend._intent_index.pop(session.handle.intent_id)
        session.order = Order.model_validate(receipt["order"])
        session.execution_state = LocalSimExecutionStateV1.model_validate(receipt["execution_state"])
        if (session.execution_state.order_id != session.order.order_id
                or session.execution_state.filled_quantity != session.order.filled_quantity
                or session.order.quantity != plan.target_quantity and session.order.status != OrderStatus.CANCELLED):
            raise ValueError("LOCALSIM_TIMING_RESTORE_ORDER_CONFLICT")
        session.fills = [Fill.model_validate(row) for row in receipt["fills"]]
        session.events = [OrderEvent.model_validate(row) for row in receipt["events"]]
        if (len({f.fill_id for f in session.fills}) != len(session.fills)
                or sum(f.quantity for f in session.fills) != session.order.filled_quantity
                or any(f.order_id != session.order.order_id for f in session.fills)):
            raise ValueError("LOCALSIM_TIMING_RESTORE_FILL_CONFLICT")
        ledger = backend._ledger
        ledger.cash = Decimal(receipt["cash_cny"])
        ledger.positions = {s: PositionLot.model_validate(p) for s, p in receipt["positions"].items()}
        if any(p.portfolio_id != backend.portfolio_id or s != p.symbol for s, p in ledger.positions.items()):
            raise ValueError("LOCALSIM_TIMING_RESTORE_POSITION_CONFLICT")
        ledger.fills = list(session.fills)
        ledger.cash_entries = [CashLedgerEntry(
            fill_id=e["fill_id"], portfolio_id=e["portfolio_id"], trade_date=date.fromisoformat(e["trade_date"]),
            symbol=e["symbol"], side=OrderSide(e["side"]), notional=Decimal(e["notional"]),
            fee=Decimal(e["fee"]), cash_delta=Decimal(e["cash_delta"]), cash_after=Decimal(e["cash_after"]),
        ) for e in receipt["cash_entries"]]
        if {e.fill_id for e in ledger.cash_entries} != {f.fill_id for f in session.fills}:
            raise ValueError("LOCALSIM_TIMING_RESTORE_CASH_FACT_CONFLICT")
        if session.fills:
            ledger.restore_order_fee_state(order_id=session.order.order_id, symbol=session.order.symbol,
                                           side=session.order.side,
                                           cumulative_notional=sum(e.notional for e in ledger.cash_entries),
                                           charged_fee=sum(e.fee for e in ledger.cash_entries))
        facade = session.facade
        facade.core_state = State.model_validate(receipt["core_state"]) if receipt["core_state"] else None
        facade.decision = Decision.model_validate(receipt["decision"]) if receipt["decision"] else None
        facade.pending = deepcopy(receipt["pending"])
        facade.closed_opportunities = set(receipt["closed_opportunities"])
        facade.confirmed_fill_ids = set(receipt["confirmed_fill_ids"])
        if facade.confirmed_fill_ids != {f.fill_id for f in session.fills}:
            raise ValueError("LOCALSIM_TIMING_RESTORE_CONFIRMATION_CONFLICT")
        facade.last_batch = receipt["last_batch"]
        facade.last_confirmed_at = utc(receipt["last_confirmed_at"]) if receipt["last_confirmed_at"] else None
        facade.last_feedback_at = utc(receipt["last_feedback_at"]) if receipt["last_feedback_at"] else None
        session.bar_hashes = dict(receipt["bar_hashes"])
        session.waits = [(qty, Decimal(wait)) for qty, wait in receipt["waits"]]
        session.reasons = list(receipt["reasons"])
        session.feedback_history = [ExecutionView.model_validate(row) for row in receipt["feedback_history"]]
        session.state = session.facade.init_order(session.order.symbol, session.order.side.value, session.order.quantity)
        session.engine.adapter.state = session.state
        session.engine._restore_algo_state(session.state, session.execution_state)
        backend._register_timing_execution(session)
        return session
