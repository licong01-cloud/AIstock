"""Pure ExitGuard evaluator for advisory lifecycle decisions."""

from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from typing import Any, Mapping

from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.services.trading_core.price_guard import (
    ALPHA_RANK_DROP_EXIT,
    HOLD,
    PRICE_BASIS_MISMATCH_ERROR,
    STOP_LOSS_DEFERRED_T1,
    STOP_LOSS_TRIGGERED,
    TAKE_PROFIT_TARGET_REACHED,
    TIME_STOP_TRIGGERED,
    TRAILING_STOP_TRIGGERED,
    UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR,
    WAITING_FOR_PRICE_GUARD_INPUT,
    WATCHLIST_EXPIRED,
)


SUPPORTED_EXIT_GUARD_MODES = {"rule_v1", "bucket_calibrated", "ml_exit_v1"}


def _default_stop_loss() -> dict[str, Any]:
    return {
        "enabled": True,
        "max_loss_bps": 600.0,
        "soft_loss_bps": 400.0,
        "volatility_multiple": 2.5,
        "reference": "actual_entry_cost",
    }


def _default_take_profit() -> dict[str, Any]:
    return {"enabled": False, "take_profit_bps": 1200.0, "trailing_stop_bps": 500.0}


def _default_alpha_decay_exit() -> dict[str, Any]:
    return {"enabled": True, "rank_drop_below": "top40%", "confirm_days": 2}


def _default_time_stop() -> dict[str, Any]:
    return {"enabled": False, "max_holding_days": 10}


@dataclass(frozen=True)
class ExitGuardPolicy:
    contract: str = "exit_guard_v1"
    enabled: bool = True
    mode: str = "rule_v1"
    price_basis: str = "raw"
    stop_loss: Mapping[str, Any] = field(default_factory=_default_stop_loss)
    take_profit: Mapping[str, Any] = field(default_factory=_default_take_profit)
    alpha_decay_exit: Mapping[str, Any] = field(default_factory=_default_alpha_decay_exit)
    time_stop: Mapping[str, Any] = field(default_factory=_default_time_stop)
    t1_handling: str = "defer_to_next_tradable_day"
    guidance_status: str = "rule_default"
    policy_sha256: str | None = None

    def __post_init__(self) -> None:
        if str(self.mode or "").strip() not in SUPPORTED_EXIT_GUARD_MODES:
            raise RuntimeConfigInvalidError(
                "unsupported exit_guard mode",
                context={"reason_code": UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR, "mode": self.mode},
            )
        if str(self.price_basis or "").strip() != "raw":
            raise RuntimeConfigInvalidError(
                "exit_guard Stage 1 supports raw price_basis only",
                context={"reason_code": PRICE_BASIS_MISMATCH_ERROR, "price_basis": self.price_basis},
            )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "ExitGuardPolicy":
        payload = dict(data or {})
        return cls(
            contract=str(payload.get("contract") or "exit_guard_v1"),
            enabled=bool(payload.get("enabled", True)),
            mode=str(payload.get("mode") or "rule_v1"),
            price_basis=str(payload.get("price_basis") or "raw"),
            stop_loss=_merge_dict(_default_stop_loss(), payload.get("stop_loss")),
            take_profit=_merge_dict(_default_take_profit(), payload.get("take_profit")),
            alpha_decay_exit=_merge_dict(_default_alpha_decay_exit(), payload.get("alpha_decay_exit")),
            time_stop=_merge_dict(_default_time_stop(), payload.get("time_stop")),
            t1_handling=str(payload.get("t1_handling") or "defer_to_next_tradable_day"),
            guidance_status=str(payload.get("guidance_status") or "rule_default"),
            policy_sha256=str(payload["policy_sha256"]) if payload.get("policy_sha256") else None,
        )


@dataclass(frozen=True)
class ExitGuardContext:
    actual_entry_cost: float | None
    current_price: float | None
    high_since_entry: float | None = None
    latest_rank: int | None = None
    latest_rank_pct: float | None = None
    initial_rank: int | None = None
    atr: float | None = None
    vol: float | None = None
    days_since_entry: int | None = None
    t1_eligible: bool = True
    soft_stop_price: float | None = None
    hard_stop_price: float | None = None
    take_profit_price: float | None = None
    trailing_stop_price: float | None = None
    alpha_decay_confirm_days: int = 0
    lifecycle_status: str | None = None
    suspend_status: str | None = None
    st_flag: bool | None = None
    delist_flag: bool | None = None
    score: float | None = None
    rank: int | None = None
    evidence_id: str | None = None
    price_basis: str = "raw"
    feature_availability_ts: str | None = None
    factor_adjustment_ratio: float | None = None


@dataclass(frozen=True)
class ExitGuardDecision:
    action: str
    reason_code: str
    should_exit: bool = False
    stop_price: float | None = None
    take_price: float | None = None
    t1_note: str | None = None
    guidance_status: str = "rule_default"
    policy_sha256: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


def evaluate(ctx: ExitGuardContext, policy: ExitGuardPolicy) -> ExitGuardDecision:
    """Evaluate an advisory exit decision with no side effects."""

    if str(ctx.price_basis or "").strip() != str(policy.price_basis or "").strip():
        raise RuntimeConfigInvalidError(
            "exit_guard context price_basis does not match policy",
            context={"reason_code": PRICE_BASIS_MISMATCH_ERROR, "context_basis": ctx.price_basis, "policy_basis": policy.price_basis},
        )
    if not policy.enabled:
        return _hold(policy, details={"enabled": False})
    if policy.mode == "bucket_calibrated":
        raise NotImplementedError(UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR)
    if policy.mode == "ml_exit_v1":
        raise NotImplementedError(UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR)
    if policy.mode != "rule_v1":
        raise RuntimeConfigInvalidError(
            "unsupported exit_guard mode",
            context={"reason_code": UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR, "mode": policy.mode},
        )
    if _is_suspended(ctx.suspend_status):
        return ExitGuardDecision(
            action="WAITING",
            reason_code=WAITING_FOR_PRICE_GUARD_INPUT,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"suspend_status": ctx.suspend_status, "carry": True},
        )
    entry_cost = _positive(ctx.actual_entry_cost)
    current_price = _positive(ctx.current_price)
    if entry_cost is None or current_price is None:
        raise DataUnavailableError(
            "exit_guard requires actual_entry_cost and current_price",
            context={"actual_entry_cost": ctx.actual_entry_cost, "current_price": ctx.current_price},
        )

    stop_loss = dict(policy.stop_loss or {})
    hard_stop = ctx.hard_stop_price or entry_cost * (1.0 - _bps_config(stop_loss, "max_loss_bps") / 10000.0)
    soft_stop = ctx.soft_stop_price or entry_cost * (1.0 - float(stop_loss.get("soft_loss_bps", 400.0)) / 10000.0)
    if bool(stop_loss.get("enabled", True)) and current_price <= hard_stop:
        if not bool(ctx.t1_eligible):
            return ExitGuardDecision(
                action="HOLD",
                reason_code=STOP_LOSS_DEFERRED_T1,
                should_exit=False,
                stop_price=hard_stop,
                t1_note="T+1 defer_to_next_tradable_day",
                guidance_status=policy.guidance_status,
                policy_sha256=policy.policy_sha256,
                details={"current_price": current_price, "hard_stop_price": hard_stop},
            )
        return ExitGuardDecision(
            action="STOP_LOSS",
            reason_code=STOP_LOSS_TRIGGERED,
            should_exit=True,
            stop_price=hard_stop,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"current_price": current_price, "hard_stop_price": hard_stop},
        )

    take_profit = dict(policy.take_profit or {})
    take_price = ctx.take_profit_price or entry_cost * (1.0 + float(take_profit.get("take_profit_bps", 1200.0)) / 10000.0)
    if bool(take_profit.get("enabled", False)) and current_price >= take_price:
        return ExitGuardDecision(
            action="TAKE_PROFIT",
            reason_code=TAKE_PROFIT_TARGET_REACHED,
            should_exit=True,
            take_price=take_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"current_price": current_price, "take_price": take_price},
        )
    trailing_stop = ctx.trailing_stop_price
    if bool(take_profit.get("enabled", False)) and trailing_stop is not None and current_price <= trailing_stop:
        return ExitGuardDecision(
            action="TRAILING_STOP",
            reason_code=TRAILING_STOP_TRIGGERED,
            should_exit=True,
            stop_price=trailing_stop,
            take_price=take_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"current_price": current_price, "trailing_stop_price": trailing_stop},
        )

    alpha_decay = dict(policy.alpha_decay_exit or {})
    if bool(alpha_decay.get("enabled", True)) and _rank_drop_triggered(ctx, alpha_decay):
        return ExitGuardDecision(
            action="ALPHA_RANK_DROP_EXIT",
            reason_code=ALPHA_RANK_DROP_EXIT,
            should_exit=True,
            stop_price=hard_stop,
            take_price=take_price if bool(take_profit.get("enabled", False)) else None,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"latest_rank": ctx.latest_rank, "latest_rank_pct": ctx.latest_rank_pct},
        )

    time_stop = dict(policy.time_stop or {})
    if bool(time_stop.get("enabled", False)) and int(ctx.days_since_entry or 0) >= int(time_stop.get("max_holding_days", 10)):
        return ExitGuardDecision(
            action="TIME_STOP",
            reason_code=TIME_STOP_TRIGGERED,
            should_exit=True,
            stop_price=hard_stop,
            take_price=take_price if bool(take_profit.get("enabled", False)) else None,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"days_since_entry": ctx.days_since_entry, "max_holding_days": time_stop.get("max_holding_days")},
        )
    if bool(ctx.delist_flag):
        return ExitGuardDecision(
            action="EXIT",
            reason_code=WATCHLIST_EXPIRED,
            should_exit=True,
            stop_price=hard_stop,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"delist_flag": True},
        )
    return _hold(
        policy,
        stop_price=hard_stop,
        take_price=take_price if bool(take_profit.get("enabled", False)) else None,
        details={"soft_stop_price": soft_stop, "hard_stop_price": hard_stop},
    )


def _hold(
    policy: ExitGuardPolicy,
    *,
    stop_price: float | None = None,
    take_price: float | None = None,
    details: dict[str, Any] | None = None,
) -> ExitGuardDecision:
    return ExitGuardDecision(
        action="HOLD",
        reason_code=HOLD,
        stop_price=stop_price,
        take_price=take_price,
        guidance_status=policy.guidance_status,
        policy_sha256=policy.policy_sha256,
        details=details or {},
    )


def _rank_drop_triggered(ctx: ExitGuardContext, config: Mapping[str, Any]) -> bool:
    confirm_days = int(config.get("confirm_days", 2))
    if int(ctx.alpha_decay_confirm_days or 0) < confirm_days:
        return False
    threshold = str(config.get("rank_drop_below") or "top40%").strip().lower()
    if threshold.endswith("%"):
        try:
            pct_threshold = float(threshold.removeprefix("top").removesuffix("%")) / 100.0
        except ValueError:
            return False
        return ctx.latest_rank_pct is not None and float(ctx.latest_rank_pct) > pct_threshold
    if threshold.startswith("top"):
        try:
            rank_threshold = int(threshold.removeprefix("top"))
        except ValueError:
            return False
        return ctx.latest_rank is not None and int(ctx.latest_rank) > rank_threshold
    try:
        rank_threshold = int(threshold)
    except ValueError:
        return False
    return ctx.latest_rank is not None and int(ctx.latest_rank) > rank_threshold


def _positive(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _bps_config(config: Mapping[str, Any], key: str) -> float:
    try:
        parsed = float(config.get(key))
    except (TypeError, ValueError) as exc:
        raise RuntimeConfigInvalidError(
            "exit_guard bps config must be numeric",
            context={"reason_code": UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR, "key": key, "value": config.get(key)},
        ) from exc
    if not isfinite(parsed) or parsed < 0:
        raise RuntimeConfigInvalidError(
            "exit_guard bps config must be non-negative finite",
            context={"reason_code": UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR, "key": key, "value": config.get(key)},
        )
    return parsed


def _is_suspended(status: str | None) -> bool:
    text = str(status or "").strip().upper()
    return text in {"S", "SUSPENDED", "HALT", "停牌"}


def _merge_dict(base: dict[str, Any], override: Any) -> dict[str, Any]:
    merged = dict(base)
    if isinstance(override, Mapping):
        merged.update(dict(override))
    return merged
