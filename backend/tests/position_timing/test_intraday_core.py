"""Business oracles for minute advice, independent of runtime services."""

from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from backend.services.position_timing.intraday import evaluate
from backend.services.position_timing.intraday.contracts import (
    Calendar, DayFacts, ExecutionView, MarketView, MinuteBar, Plan, Policy,
    PriceConstraint, PriceInterval, digest, utc,
)
from backend.services.position_timing.intraday.inputs import normalize_tdx_bars, visible_prefix


def at(clock="09:35:10"):
    return utc("2026-10-22T" + clock + "+08:00")


def case(policy_id="FIRST_OBSERVABLE_V1", *, prices=("10",) * 5, side="BUY", split=False, price=None, source="MANUAL"):
    policy = Policy(policy_id=policy_id, allow_split=split, source_commit="1" * 40)
    plan = Plan(plan_id="minute_test", revision=1, source_kind=source, source_ref="frozen-test", source_revision="1",
                symbol="000001.SZ", side=side, target_trade_date=at().date(), valid_from=at("09:30:00"),
                valid_until=at("14:57:00"), policy_id=policy_id, allow_split=split, policy=policy,
                policy_sha256=digest(policy), price_constraint=price or PriceConstraint())
    bars = []
    for index, value in enumerate(prices, 1):
        end = at("09:30:00") + timedelta(minutes=index)
        close = Decimal(value)
        bars.append(MinuteBar(start=end - timedelta(minutes=1), end=end, open=close, close=close,
                              high=close + Decimal("0.01"), low=close - Decimal("0.01"),
                              volume_shares="10000", received_at=end + timedelta(seconds=5)))
    facts = DayFacts(trade_date=plan.target_trade_date, symbol=plan.symbol, is_suspended=False, has_daily_limit=True,
                     reference_price="10", limit_up="11", limit_down="9", available_at=at("09:00:00"),
                     source_ref="frozen-day", source_sha256="a" * 64)
    market = MarketView(symbol=plan.symbol, trade_date=plan.target_trade_date, source_ref="frozen-bars", source_version="v1",
                        calendar=Calendar(trade_date=plan.target_trade_date, is_trading_day=True, source_ref="calendar",
                                          source_sha256="b" * 64, available_at=at("09:00:00")),
                        day_facts=facts, bars=tuple(bars), visibility_basis="ASSUMED_VISIBILITY")
    return plan, market


@pytest.mark.parametrize("policy_id,side,prices,reason", [
    ("FIRST_OBSERVABLE_V1", "BUY", ("10",) * 5, "FIRST_OBSERVABLE"),
    ("BOUNDED_REVERSION_V1", "BUY", ("10.1", "10.08", "10.06", "9.90", "9.95"), "BOUNDED_REVERSION"),
    ("BOUNDED_REVERSION_V1", "SELL", ("9.90", "9.92", "9.94", "10.10", "10.05"), "BOUNDED_REVERSION"),
])
def test_fixed_rules_have_hand_checked_opportunities(policy_id, side, prices, reason):
    plan, market = case(policy_id, side=side, prices=prices)
    decision, state = evaluate(plan, market, ExecutionView(), plan.policy, None, at())
    assert decision.status == "OPPORTUNITY" and reason in decision.reason_codes
    assert decision.target_quantity is None and decision.orders_created is False
    again, again_state = evaluate(plan, market, ExecutionView(), plan.policy, None, at())
    assert decision == again and state == again_state


def test_price_touch_uses_close_and_preserves_disjoint_holes():
    price = PriceConstraint(mode="HARD", intervals=(PriceInterval(lower="9.80", upper="9.90"), PriceInterval(lower="10.10", upper="10.20")),
                            source_ref="user", source_sha256="c" * 64, valid_from=at("09:30:00"), valid_until=at("14:57:00"))
    plan, market = case("PRICE_TOUCH_V1", price=price)
    last = market.bars[-1].model_copy(update={"low": Decimal("9.85"), "high": Decimal("10.15")})
    decision, _ = evaluate(plan, market.model_copy(update={"bars": (*market.bars[:-1], last)}), ExecutionView(), plan.policy, None, at())
    assert decision.status == "WAIT" and "OUTSIDE_HARD_PRICE_CONSTRAINT" in decision.reason_codes
    plan, market = case("PRICE_TOUCH_V1", price=price, prices=("9.85",) * 5)
    assert evaluate(plan, market, ExecutionView(), plan.policy, None, at())[0].status == "OPPORTUNITY"
    with pytest.raises(ValidationError):
        case("PRICE_TOUCH_V1")


@pytest.mark.parametrize("side,last,reason", [("BUY", "11", "LIMIT_UP_BUY_BLOCKED"), ("SELL", "9", "LIMIT_DOWN_SELL_BLOCKED")])
def test_directional_limits_and_unknown_facts_do_not_fake_trade(side, last, reason):
    plan, market = case(side=side, prices=(last,) * 5)
    decision, _ = evaluate(plan, market, ExecutionView(), plan.policy, None, at())
    assert decision.status == "WAIT" and reason in decision.reason_codes
    missing = market.model_copy(update={"day_facts": market.day_facts.model_copy(update={"is_suspended": None})})
    assert evaluate(plan, missing, ExecutionView(), plan.policy, None, at())[0].status == "UNAVAILABLE"
    suspended = missing.model_copy(update={"day_facts": missing.day_facts.model_copy(update={"is_suspended": True})})
    assert "SUSPENDED" in evaluate(plan, suspended, ExecutionView(), plan.policy, None, at())[0].reason_codes


def test_future_prefix_revision_staleness_and_session_boundaries():
    plan, full = case(prices=("10",) * 6)
    visible = visible_prefix(full, at())
    decision, state = evaluate(plan, visible, ExecutionView(), plan.policy, None, at())
    assert decision.status == "OPPORTUNITY"
    assert "UNOBSERVED_OR_UNCLOSED_BAR" in evaluate(plan, full, ExecutionView(), plan.policy, None, at())[0].reason_codes
    changed_future = full.model_copy(update={"bars": (*full.bars[:-1], full.bars[-1].model_copy(update={"close": Decimal("10.01")}))})
    assert evaluate(plan, visible_prefix(changed_future, at()), ExecutionView(), plan.policy, None, at())[0] == decision
    revised = visible.model_copy(update={"bars": (*visible.bars[:-1], visible.bars[-1].model_copy(update={"close": Decimal("10.01")}))})
    conflict, conflict_state = evaluate(plan, revised, ExecutionView(), plan.policy, state, at("09:36:10"))
    assert "INPUT_REVISION_CONFLICT" in conflict.reason_codes
    assert "INPUT_REVISION_CONFLICT" in evaluate(plan, visible, ExecutionView(), plan.policy, conflict_state, at("09:36:11"))[0].reason_codes
    assert "MINUTE_STALE" in evaluate(plan, visible, ExecutionView(), plan.policy, None, at("09:37:10"))[0].reason_codes
    assert "SESSION_BREAK" in evaluate(plan, visible, ExecutionView(), plan.policy, None, at("11:30:00"))[0].reason_codes
    assert evaluate(plan, visible, ExecutionView(), plan.policy, None, at("14:57:00"))[0].status == "EXPIRED"


def test_wait_budget_and_confirmed_split_progress_are_not_alert_counts():
    plan, market = case("BOUNDED_REVERSION_V1", split=True)
    plan = plan.model_copy(update={"target_quantity": 3000, "quantity_source_ref": "frozen-target"})
    first, state = evaluate(plan, market, ExecutionView(), plan.policy, None, at())
    assert first.status == "WAIT"
    _, later = case("BOUNDED_REVERSION_V1", prices=("10",) * 20)
    released, state = evaluate(plan, later, ExecutionView(), plan.policy, state, at("09:50:10"))
    assert released.status == "OPPORTUNITY" and "TIME_BUDGET_RELEASE" in released.reason_codes
    assert released.target_quantity == 1000 and released.target_fraction_denominator == 3
    feedback = ExecutionView(consumer_kind="SIMULATION", symbol=plan.symbol, trade_date=plan.target_trade_date,
                             available_at=at("09:50:30"), confirmed_quantity=1000, reserved_quantity=0, cash_cny="100000",
                             sellable_quantity=0, source_ref="sim-test", feedback_id="fill1", last_confirmed_batch=1,
                             last_confirmed_at=at("09:50:30"))
    assert "BATCH_COOLDOWN" in evaluate(plan, later, feedback, plan.policy, state, at("09:54:10"))[0].reason_codes
    _, current = case("BOUNDED_REVERSION_V1", prices=("10",) * 26)
    next_batch, _ = evaluate(plan, current, feedback, plan.policy, state, at("09:56:10"))
    assert next_batch.batch_id == 2 and next_batch.target_quantity == 2000
    with pytest.raises(ValidationError):
        ExecutionView(confirmed_quantity=1000)


def test_tdx_normalization_converts_once_and_excludes_unclosed_bar():
    row = {"time": at("09:35:00"), "open": 10, "high": 10, "low": 10, "close": 10, "volume": 123, "volume_shares": 12345}
    bars = normalize_tdx_bars([row], trade_date=at().date(), received_at=at())
    assert bars[0].volume_shares == 12345 and bars[0].close == 10
    assert normalize_tdx_bars([row], trade_date=at().date(), received_at=at("09:35:04")) == ()
    with pytest.raises(ValueError, match="VOLUME_UNITS"):
        normalize_tdx_bars([{**row, "volume_shares": 12400}], trade_date=at().date(), received_at=at())
