from __future__ import annotations

import inspect

import pytest

from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.services.trading_core.exit_guard import ExitGuardContext, ExitGuardPolicy, evaluate
from backend.services.trading_core.price_guard import (
    ALPHA_RANK_DROP_EXIT,
    HOLD,
    PRICE_BASIS_MISMATCH_ERROR,
    STOP_LOSS_DEFERRED_T1,
    STOP_LOSS_TRIGGERED,
    TAKE_PROFIT_TARGET_REACHED,
    WAITING_FOR_PRICE_GUARD_INPUT,
)


def _ctx(**updates) -> ExitGuardContext:
    base = {
        "actual_entry_cost": 10.0,
        "current_price": 10.0,
        "latest_rank": 5,
        "latest_rank_pct": 0.05,
        "alpha_decay_confirm_days": 0,
        "t1_eligible": True,
        "price_basis": "raw",
    }
    base.update(updates)
    return ExitGuardContext(**base)


def test_s1_1_exit_guard_is_pure_and_deterministic() -> None:
    source = inspect.getsource(evaluate)

    assert "get_conn" not in source
    assert "requests" not in source
    assert "open(" not in source
    policy = ExitGuardPolicy(policy_sha256="exit-sha")

    assert evaluate(_ctx(), policy) == evaluate(_ctx(), policy)


def test_s1_8_hard_stop_deferred_t1_and_later_exits() -> None:
    policy = ExitGuardPolicy(policy_sha256="exit-sha")

    deferred = evaluate(_ctx(current_price=9.3, t1_eligible=False), policy)
    assert deferred.reason_code == STOP_LOSS_DEFERRED_T1
    assert deferred.should_exit is False

    triggered = evaluate(_ctx(current_price=9.3, t1_eligible=True), policy)
    assert triggered.reason_code == STOP_LOSS_TRIGGERED
    assert triggered.should_exit is True


def test_s1_3_exit_rule_v1_take_profit_default_off_and_alpha_decay_on() -> None:
    default_policy = ExitGuardPolicy(policy_sha256="exit-sha")
    assert evaluate(_ctx(current_price=11.5), default_policy).reason_code == HOLD

    take_policy = ExitGuardPolicy.from_dict(
        {"policy_sha256": "exit-sha", "take_profit": {"enabled": True, "take_profit_bps": 1200}}
    )
    assert evaluate(_ctx(current_price=11.5), take_policy).reason_code == TAKE_PROFIT_TARGET_REACHED

    rank_drop = evaluate(
        _ctx(latest_rank=80, latest_rank_pct=0.8, alpha_decay_confirm_days=2),
        default_policy,
    )
    assert rank_drop.reason_code == ALPHA_RANK_DROP_EXIT
    assert rank_drop.should_exit is True


def test_s1_9_exit_guard_suspend_waiting_and_fail_fast_inputs() -> None:
    policy = ExitGuardPolicy(policy_sha256="exit-sha")

    suspended = evaluate(_ctx(current_price=None, suspend_status="SUSPENDED"), policy)
    assert suspended.action == "WAITING"
    assert suspended.reason_code == WAITING_FOR_PRICE_GUARD_INPUT

    with pytest.raises(DataUnavailableError):
        evaluate(_ctx(current_price=None), policy)

    with pytest.raises(RuntimeConfigInvalidError) as basis:
        evaluate(_ctx(price_basis="adjusted"), policy)
    assert basis.value.context["reason_code"] == PRICE_BASIS_MISMATCH_ERROR
