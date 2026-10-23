"""Causal minute decisions. This module has no I/O or consumer-side fills."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from .contracts import Decision, ExecutionView, MarketView, Plan, Policy, State, digest, sessions, trading_minutes, utc


def evaluate(
    plan: Plan,
    observed_prefix: MarketView,
    execution_view: ExecutionView,
    policy: Policy,
    previous_state: State | None,
    as_of: datetime,
) -> tuple[Decision, State]:
    """Evaluate only observed data; return explicit decisions and immutable next state."""
    as_of = utc(as_of)
    market, execution = observed_prefix, execution_view
    identity = digest(plan)
    state = previous_state or State(plan_sha256=identity)
    before_hash = digest(state)
    input_hash = digest({"plan": plan, "market": market, "execution": execution, "policy": policy})
    updates: dict = {"last_as_of": as_of}
    data_as_of = None
    expiry = min(plan.valid_until, as_of + timedelta(seconds=policy.decision_ttl_seconds))
    reasons: list[str] = []
    source_status = "VERIFIED_INPUT"

    def finish(status: str, reason: str) -> tuple[Decision, State]:
        next_state = state.model_copy(update={**updates, "last_condition": status == "OPPORTUNITY"})
        batch = next_state.batch_id
        denominator = policy.max_batches if policy.allow_split else 1
        numerator = batch if policy.allow_split else 1
        opportunity_id = digest({"plan_id": plan.plan_id, "revision": plan.revision, "policy": plan.policy_sha256, "batch_id": batch})
        body = dict(
            opportunity_id=opportunity_id, plan_id=plan.plan_id, revision=plan.revision,
            policy_sha256=plan.policy_sha256, input_sha256=input_hash,
            previous_state_sha256=before_hash, as_of=as_of, status=status, batch_id=batch,
            target_fraction_numerator=numerator, target_fraction_denominator=denominator,
            target_quantity=None if plan.target_quantity is None else plan.target_quantity * numerator // denominator,
            reason_codes=tuple([*reasons, reason]), data_as_of=data_as_of, valid_until=expiry,
            advice_available_at=next_state.first_opportunity_at, source_status=source_status,
            execution_status=execution.consumer_kind, price_constraint=plan.price_constraint,
        )
        decision = Decision(decision_id=digest(body), **body)
        return decision, next_state.model_copy(update={"last_decision_ref": decision.decision_id})

    if state.plan_sha256 != identity or digest(policy) != plan.policy_sha256:
        return finish("UNAVAILABLE", "PLAN_POLICY_STATE_IDENTITY_CONFLICT")
    if state.last_as_of is not None and as_of < state.last_as_of:
        updates["last_as_of"] = state.last_as_of
        return finish("UNAVAILABLE", "CLOCK_REGRESSION")
    if state.input_conflict:
        return finish("UNAVAILABLE", "INPUT_REVISION_CONFLICT")
    if plan.lifecycle != "ACTIVE" or as_of >= plan.valid_until:
        return finish("EXPIRED", "PLAN_" + (plan.lifecycle if plan.lifecycle != "ACTIVE" else "EXPIRED"))
    if as_of < plan.valid_from:
        return finish("WAIT", "PLAN_NOT_STARTED")
    if (market.symbol != plan.symbol or market.trade_date != plan.target_trade_date
            or market.calendar.trade_date != plan.target_trade_date
            or market.day_facts.trade_date != plan.target_trade_date or market.day_facts.symbol != plan.symbol):
        return finish("UNAVAILABLE", "MARKET_IDENTITY_MISMATCH")
    if max(market.calendar.available_at, market.day_facts.available_at) > as_of:
        return finish("UNAVAILABLE", "CONTEXT_NOT_YET_AVAILABLE")
    if not market.calendar.is_trading_day:
        return finish("WAIT", "NON_TRADING_DAY")
    session = next(((left, right) for left, right in sessions(plan.target_trade_date) if left <= as_of < right), None)
    if session is None:
        return finish("WAIT", "SESSION_BREAK")
    expiry = min(expiry, session[1])
    facts = market.day_facts
    if facts.is_suspended is True:
        return finish("WAIT", "SUSPENDED")
    if facts.is_suspended is None or facts.has_daily_limit is None or facts.reference_price is None:
        return finish("UNAVAILABLE", "TRADABILITY_UNKNOWN")
    if facts.has_daily_limit and (facts.limit_up is None or facts.limit_down is None
                                  or not facts.limit_down < facts.reference_price < facts.limit_up):
        return finish("UNAVAILABLE", "TRADABILITY_UNKNOWN")
    context_hash = digest({"source_ref": market.source_ref, "source_version": market.source_version,
                           "calendar": market.calendar.model_dump(exclude={"available_at"}),
                           "facts": facts.model_dump(exclude={"available_at"})})
    if state.source_context_sha256 is not None and state.source_context_sha256 != context_hash:
        updates["input_conflict"] = True
        return finish("UNAVAILABLE", "INPUT_REVISION_CONFLICT")
    updates["source_context_sha256"] = context_hash
    if execution.consumer_kind == "SIMULATION":
        if plan.target_quantity is None:
            return finish("UNAVAILABLE", "SIMULATION_TARGET_QUANTITY_REQUIRED")
        if execution.symbol != plan.symbol or execution.trade_date != plan.target_trade_date or execution.available_at > as_of:
            return finish("UNAVAILABLE", "EXECUTION_IDENTITY_OR_AVAILABILITY_CONFLICT")
        if execution.last_confirmed_at is not None and execution.last_confirmed_at > as_of:
            return finish("UNAVAILABLE", "FUTURE_EXECUTION_FEEDBACK")
        total_batches = policy.max_batches if policy.allow_split else 1
        if execution.last_confirmed_batch >= total_batches or execution.confirmed_quantity >= plan.target_quantity:
            return finish("EXPIRED", "PLAN_COMPLETED")
        if execution.last_confirmed_batch < state.batch_id - 1:
            return finish("UNAVAILABLE", "EXECUTION_FEEDBACK_REGRESSION")
        if execution.last_confirmed_batch > state.batch_id:
            return finish("UNAVAILABLE", "EXECUTION_FEEDBACK_BATCH_JUMP")
        if execution.last_confirmed_batch >= state.batch_id:
            if trading_minutes(execution.last_confirmed_at, as_of, plan.target_trade_date) < policy.cooldown_trading_minutes:
                return finish("WAIT", "BATCH_COOLDOWN")
            updates.update(batch_id=execution.last_confirmed_batch + 1, batch_started_at=None,
                           budget_released=False, first_opportunity_at=None)
        if plan.side == "BUY" and execution.cash_cny == 0:
            return finish("WAIT", "NO_AVAILABLE_CASH")
        if plan.side == "SELL" and execution.sellable_quantity == 0:
            return finish("WAIT", "NO_SELLABLE_QUANTITY")

    # Validate the supplied prefix itself. An offline caller cannot smuggle future
    # observations in and rely on an adapter silently filtering them here.
    bars = sorted(market.bars, key=lambda b: b.end)
    consumed = dict(state.consumed_bars)
    seen: dict = {}
    accepted = []
    for bar in bars:
        if not any(left <= bar.start < bar.end <= right for left, right in sessions(plan.target_trade_date)):
            return finish("UNAVAILABLE", "BAR_OUTSIDE_SESSION")
        available = max(bar.end, bar.received_at, bar.source_available_at or bar.end)
        if available > as_of or bar.end > bar.received_at - timedelta(seconds=policy.closure_delay_seconds):
            return finish("UNAVAILABLE", "UNOBSERVED_OR_UNCLOSED_BAR")
        key, content = bar.end.isoformat(), bar.content_sha256()
        if (key in seen and seen[key] != content) or (key in consumed and consumed[key] != content):
            updates["input_conflict"] = True
            return finish("UNAVAILABLE", "INPUT_REVISION_CONFLICT")
        if key not in seen:
            accepted.append(bar)
        seen[key] = content
        consumed[key] = content
    updates["consumed_bars"] = consumed
    current = [bar for bar in accepted if session[0] <= bar.start and bar.end <= session[1]]
    if not current:
        return finish("WAIT", "WAITING_FOR_BAR")
    last = current[-1]
    data_as_of = last.end
    updates["last_processed_bar"] = last.end
    expected = session[0] + timedelta(minutes=max(0, int((as_of - timedelta(seconds=policy.closure_delay_seconds) - session[0]).total_seconds() // 60)))
    if expected - last.end > timedelta(minutes=policy.max_lag_slots):
        return finish("UNAVAILABLE", "MINUTE_STALE")
    if last.volume_shares == 0:
        return finish("WAIT", "NO_OBSERVED_TRADE")
    if facts.has_daily_limit and not facts.limit_down <= last.close <= facts.limit_up:
        return finish("UNAVAILABLE", "PRICE_OUTSIDE_DAILY_LIMITS")
    if facts.has_daily_limit and ((plan.side == "BUY" and last.close >= facts.limit_up)
                                  or (plan.side == "SELL" and last.close <= facts.limit_down)):
        return finish("WAIT", "LIMIT_UP_BUY_BLOCKED" if plan.side == "BUY" else "LIMIT_DOWN_SELL_BLOCKED")
    price = plan.price_constraint
    dependent = price.mode in {"PREFERRED", "HARD"}
    if dependent:
        if (not price.intervals or not price.source_ref or price.source_sha256 is None
                or price.valid_from is None or price.valid_until is None):
            return finish("UNAVAILABLE", "PRICE_CONSTRAINT_UNAVAILABLE")
        if not price.valid_from <= as_of < price.valid_until:
            return finish("UNAVAILABLE", "PRICE_CONSTRAINT_EXPIRED_OR_NOT_STARTED")
        expiry = min(expiry, price.valid_until)
    inside = any(interval.lower <= last.close <= interval.upper for interval in price.intervals)
    started = updates.get("batch_started_at", state.batch_started_at)
    if started is None:
        started = as_of
        updates["batch_started_at"] = started
    if price.mode == "HARD" and not inside:
        return finish("WAIT", "OUTSIDE_HARD_PRICE_CONSTRAINT")
    if policy.policy_id == "FIRST_OBSERVABLE_V1":
        ready, reason = True, "FIRST_OBSERVABLE"
        if price.mode in {"PREFERRED", "REFERENCE"}:
            reasons.append("PREFERENCE_NOT_USED_BY_BASELINE")
    elif policy.policy_id == "PRICE_TOUCH_V1":
        ready, reason = inside, "PRICE_CLOSE_IN_CONSTRAINT" if inside else "WAITING_FOR_PRICE"
    else:
        released = updates.get("budget_released", state.budget_released) or trading_minutes(started, as_of, plan.target_trade_date) >= policy.wait_trading_minutes
        updates["budget_released"] = released
        if released:
            ready, reason = True, "TIME_BUDGET_RELEASE"
            if price.mode == "PREFERRED" and not inside:
                reasons.append("PREFERRED_PRICE_WAIVED_AT_BUDGET")
        else:
            window = current[-policy.mean_bars:]
            continuous = len(window) == policy.mean_bars and all(b.end - a.end == timedelta(minutes=1) for a, b in zip(window, window[1:]))
            if not continuous:
                return finish("WAIT", "WAITING_FOR_CONTIGUOUS_WARMUP")
            mean = sum((b.close for b in window), Decimal(0)) / policy.mean_bars
            ready = (last.close <= mean and last.close > window[-2].close) if plan.side == "BUY" else (last.close >= mean and last.close < window[-2].close)
            ready = ready and (price.mode != "PREFERRED" or inside)
            reason = "BOUNDED_REVERSION" if ready else "WAITING_FOR_REVERSION"
    if ready and updates.get("first_opportunity_at", state.first_opportunity_at) is None:
        updates["first_opportunity_at"] = as_of
    return finish("OPPORTUNITY" if ready else "WAIT", reason)
