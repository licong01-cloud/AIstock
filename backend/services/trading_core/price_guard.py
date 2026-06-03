"""Pure PriceGuard evaluator for advisory, QE, and Paper v2 adapters.

The evaluator intentionally has no database, network, clock, or broker I/O.
Adapters must construct a complete context and pass a frozen policy snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from typing import Any, Mapping

from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


ACCEPT_WITHIN_GREEN_ZONE = "ACCEPT_WITHIN_GREEN_ZONE"
REDUCE_YELLOW_OPEN_GAP = "REDUCE_YELLOW_OPEN_GAP"
REDUCE_YELLOW_CHASE_BAND = "REDUCE_YELLOW_CHASE_BAND"
SKIP_OPEN_GAP_EXCEEDED = "SKIP_OPEN_GAP_EXCEEDED"
SKIP_ABOVE_MAX_BUY_PRICE = "SKIP_ABOVE_MAX_BUY_PRICE"
SKIP_NEAR_LIMIT_UP = "SKIP_NEAR_LIMIT_UP"
SKIP_BELOW_MIN_SELL_PRICE_REBALANCE = "SKIP_BELOW_MIN_SELL_PRICE_REBALANCE"
EXECUTE_RISK_EXIT_WITH_WIDER_LIMIT = "EXECUTE_RISK_EXIT_WITH_WIDER_LIMIT"
WAITING_FOR_PRICE_GUARD_INPUT = "WAITING_FOR_PRICE_GUARD_INPUT"
ADD_BREAKOUT_NEAR_LIMIT = "ADD_BREAKOUT_NEAR_LIMIT"
SKIP_BREAKOUT_LOW_FILL_PROBABILITY = "SKIP_BREAKOUT_LOW_FILL_PROBABILITY"
HOLD = "HOLD"
TAKE_PROFIT_TARGET_REACHED = "TAKE_PROFIT_TARGET_REACHED"
TRAILING_STOP_TRIGGERED = "TRAILING_STOP_TRIGGERED"
STOP_LOSS_TRIGGERED = "STOP_LOSS_TRIGGERED"
STOP_LOSS_DEFERRED_T1 = "STOP_LOSS_DEFERRED_T1"
TIME_STOP_TRIGGERED = "TIME_STOP_TRIGGERED"
ALPHA_RANK_DROP_EXIT = "ALPHA_RANK_DROP_EXIT"
WATCHLIST_EXPIRED = "WATCHLIST_EXPIRED"
PRE_FILTER_LIMIT_UP = "PRE_FILTER_LIMIT_UP"
PG_SKIP_NEAR_LIMIT_UP = "PG_SKIP_NEAR_LIMIT_UP"
SIGNAL_REF_PRICE_MISSING_DATA_ERROR = "SIGNAL_REF_PRICE_MISSING_DATA_ERROR"
PRICE_BASIS_MISMATCH_ERROR = "PRICE_BASIS_MISMATCH_ERROR"
LIMIT_PRICE_MISSING_DATA_ERROR = "LIMIT_PRICE_MISSING_DATA_ERROR"
UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR = "UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR"
UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR = "UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR"

REASON_CODES: frozenset[str] = frozenset(
    {
        ACCEPT_WITHIN_GREEN_ZONE,
        REDUCE_YELLOW_OPEN_GAP,
        REDUCE_YELLOW_CHASE_BAND,
        SKIP_OPEN_GAP_EXCEEDED,
        SKIP_ABOVE_MAX_BUY_PRICE,
        SKIP_NEAR_LIMIT_UP,
        SKIP_BELOW_MIN_SELL_PRICE_REBALANCE,
        EXECUTE_RISK_EXIT_WITH_WIDER_LIMIT,
        WAITING_FOR_PRICE_GUARD_INPUT,
        ADD_BREAKOUT_NEAR_LIMIT,
        SKIP_BREAKOUT_LOW_FILL_PROBABILITY,
        HOLD,
        TAKE_PROFIT_TARGET_REACHED,
        TRAILING_STOP_TRIGGERED,
        STOP_LOSS_TRIGGERED,
        STOP_LOSS_DEFERRED_T1,
        TIME_STOP_TRIGGERED,
        ALPHA_RANK_DROP_EXIT,
        WATCHLIST_EXPIRED,
        PRE_FILTER_LIMIT_UP,
        PG_SKIP_NEAR_LIMIT_UP,
        SIGNAL_REF_PRICE_MISSING_DATA_ERROR,
        PRICE_BASIS_MISMATCH_ERROR,
        LIMIT_PRICE_MISSING_DATA_ERROR,
        UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR,
        UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR,
    }
)

SUPPORTED_PRICE_GUARD_MODES = {"rule_v1", "bucket_calibrated", "ml_residual_alpha_v1"}


def _default_signal_ref_price_policy() -> dict[str, str]:
    return {"buy": "signal_close", "sell": "signal_close", "intraday": "arrival_price"}


def _default_buy_policy() -> dict[str, Any]:
    return {
        "max_open_gap_bps": 300.0,
        "yellow_open_gap_bps": 150.0,
        "yellow_size_multiplier": 0.5,
        "max_chase_bps": 100.0,
        "yellow_chase_bps": 50.0,
        "near_limit_up_skip_bps": 80.0,
        "allow_partial": True,
        "breakout_addon": {
            "enabled": False,
            "require_momentum_regime": True,
            "min_score_bucket": "top5",
            "dist_to_limit_up_lt_bps": 200.0,
            "min_volume_ratio_open": 1.5,
            "add_size_multiplier": 0.5,
            "min_fill_probability": 0.6,
        },
    }


def _default_sell_policy() -> dict[str, Any]:
    return {
        "rebalance_max_slippage_bps": 150.0,
        "risk_exit_max_slippage_bps": 500.0,
        "near_limit_down_rebalance_skip_bps": 80.0,
        "allow_partial": True,
    }


@dataclass(frozen=True)
class PriceGuardPolicy:
    """Frozen PriceGuard policy schema with all Stage-1 and future mode slots."""

    contract: str = "execution_price_guard_v1"
    enabled: bool = True
    mode: str = "rule_v1"
    price_basis: str = "raw"
    signal_ref_price: Mapping[str, str] = field(default_factory=_default_signal_ref_price_policy)
    buy: Mapping[str, Any] = field(default_factory=_default_buy_policy)
    sell: Mapping[str, Any] = field(default_factory=_default_sell_policy)
    guidance_status: str = "rule_default"
    policy_sha256: str | None = None

    def __post_init__(self) -> None:
        mode = str(self.mode or "").strip()
        if mode not in SUPPORTED_PRICE_GUARD_MODES:
            raise RuntimeConfigInvalidError(
                "unsupported price_guard mode",
                context={"reason_code": UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR, "mode": self.mode},
            )
        if str(self.price_basis or "").strip() != "raw":
            raise RuntimeConfigInvalidError(
                "price_guard Stage 1 supports raw price_basis only",
                context={"reason_code": PRICE_BASIS_MISMATCH_ERROR, "price_basis": self.price_basis},
            )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "PriceGuardPolicy":
        payload = dict(data or {})
        return cls(
            contract=str(payload.get("contract") or "execution_price_guard_v1"),
            enabled=bool(payload.get("enabled", True)),
            mode=str(payload.get("mode") or "rule_v1"),
            price_basis=str(payload.get("price_basis") or "raw"),
            signal_ref_price=dict(payload.get("signal_ref_price") or _default_signal_ref_price_policy()),
            buy=_merge_dict(_default_buy_policy(), payload.get("buy")),
            sell=_merge_dict(_default_sell_policy(), payload.get("sell")),
            guidance_status=str(payload.get("guidance_status") or "rule_default"),
            policy_sha256=str(payload["policy_sha256"]) if payload.get("policy_sha256") else None,
        )


@dataclass(frozen=True)
class PriceGuardContext:
    """Point-in-time input context for a single buy/sell acceptance decision."""

    signal_ref_price: float | None
    prev_close: float | None
    open_price: float | None = None
    current_price: float | None = None
    limit_up: float | None = None
    limit_down: float | None = None
    open_gap_bps: float | None = None
    current_gap_bps: float | None = None
    dist_to_limit_up_bps: float | None = None
    dist_to_limit_down_bps: float | None = None
    volume_ratio_open: float | None = None
    amount_20d: float | None = None
    board_type: str | None = None
    st_flag: bool | None = None
    suspend_status: str | None = None
    score: float | None = None
    rank: int | None = None
    score_bucket: str | None = None
    expected_alpha_bps: float | None = None
    alpha_family: str | None = None
    target_weight: float | None = None
    market_regime: str | None = None
    momentum_regime: str | bool | None = None
    momentum_regime_flag: bool | None = None
    event_flag: str | bool | None = None
    side: str = "buy"
    sell_reason: str | None = None
    holding_days: int | None = None
    prev_position: float | None = None
    price_basis: str = "raw"
    feature_availability_ts: str | None = None
    fill_probability: float | None = None


@dataclass(frozen=True)
class PriceGuardDecision:
    action: str
    reason_code: str
    size_multiplier: float = 1.0
    max_buy_price: float | None = None
    min_sell_price: float | None = None
    guard_price: float | None = None
    guidance_status: str = "rule_default"
    policy_sha256: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


def evaluate(ctx: PriceGuardContext, policy: PriceGuardPolicy) -> PriceGuardDecision:
    """Evaluate a single PriceGuard decision with no side effects."""

    if not policy.enabled:
        return PriceGuardDecision(
            action="ACCEPT",
            reason_code=ACCEPT_WITHIN_GREEN_ZONE,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"enabled": False},
        )
    if policy.mode == "bucket_calibrated":
        raise NotImplementedError(UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR)
    if policy.mode == "ml_residual_alpha_v1":
        raise NotImplementedError(UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR)
    if policy.mode != "rule_v1":
        raise RuntimeConfigInvalidError(
            "unsupported price_guard mode",
            context={"reason_code": UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR, "mode": policy.mode},
        )

    _validate_common_context(ctx, policy)
    side = str(ctx.side or "buy").strip().lower()
    if side == "buy":
        return _evaluate_buy(ctx, policy)
    if side == "sell":
        return _evaluate_sell(ctx, policy)
    raise RuntimeConfigInvalidError(
        "price_guard side must be buy or sell",
        context={"reason_code": UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR, "side": ctx.side},
    )


def _evaluate_buy(ctx: PriceGuardContext, policy: PriceGuardPolicy) -> PriceGuardDecision:
    signal = _positive(ctx.signal_ref_price)
    limit_up = _positive(ctx.limit_up)
    limit_down = _positive(ctx.limit_down)
    if limit_up is None or limit_down is None:
        raise DataUnavailableError(
            "price_guard buy evaluation requires limit_up and limit_down",
            context={"reason_code": LIMIT_PRICE_MISSING_DATA_ERROR, "limit_up": ctx.limit_up, "limit_down": ctx.limit_down},
        )
    execution_price = _positive(ctx.open_price) or _positive(ctx.current_price)
    if execution_price is None:
        return PriceGuardDecision(
            action="WAITING",
            reason_code=WAITING_FOR_PRICE_GUARD_INPUT,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"missing": "open_price_or_current_price"},
        )

    buy = dict(policy.buy or {})
    max_open_gap_bps = _required_bps(buy, "max_open_gap_bps")
    yellow_open_gap_bps = _required_bps(buy, "yellow_open_gap_bps")
    yellow_size_multiplier = float(buy.get("yellow_size_multiplier", 0.5))
    max_chase_bps = _required_bps(buy, "max_chase_bps")
    yellow_chase_bps = float(buy.get("yellow_chase_bps", max_chase_bps / 2.0))
    near_limit_up_skip_bps = _required_bps(buy, "near_limit_up_skip_bps")
    open_gap_bps = ctx.open_gap_bps if ctx.open_gap_bps is not None else _bps(execution_price, signal)
    chase_bps = ctx.current_gap_bps if ctx.current_gap_bps is not None else _bps(execution_price, signal)
    dist_to_limit_up = ctx.dist_to_limit_up_bps
    if dist_to_limit_up is None:
        dist_to_limit_up = max(0.0, (limit_up - execution_price) / execution_price * 10000.0)
    max_buy_price = min(signal * (1.0 + max_chase_bps / 10000.0), limit_up)

    breakout = _evaluate_breakout_addon(ctx, policy, execution_price=execution_price, max_buy_price=max_buy_price)
    if breakout is not None:
        return breakout

    if dist_to_limit_up <= near_limit_up_skip_bps:
        return PriceGuardDecision(
            action="SKIP",
            reason_code=SKIP_NEAR_LIMIT_UP,
            size_multiplier=0.0,
            max_buy_price=max_buy_price,
            guard_price=max_buy_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"dist_to_limit_up_bps": dist_to_limit_up, "near_limit_up_skip_bps": near_limit_up_skip_bps},
        )
    if open_gap_bps > max_open_gap_bps:
        return PriceGuardDecision(
            action="SKIP",
            reason_code=SKIP_OPEN_GAP_EXCEEDED,
            size_multiplier=0.0,
            max_buy_price=max_buy_price,
            guard_price=max_buy_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"open_gap_bps": open_gap_bps, "max_open_gap_bps": max_open_gap_bps},
        )
    if execution_price > max_buy_price:
        return PriceGuardDecision(
            action="SKIP",
            reason_code=SKIP_ABOVE_MAX_BUY_PRICE,
            size_multiplier=0.0,
            max_buy_price=max_buy_price,
            guard_price=max_buy_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"execution_price": execution_price, "max_buy_price": max_buy_price, "chase_bps": chase_bps},
        )
    if open_gap_bps > yellow_open_gap_bps:
        return PriceGuardDecision(
            action="REDUCE",
            reason_code=REDUCE_YELLOW_OPEN_GAP,
            size_multiplier=yellow_size_multiplier,
            max_buy_price=max_buy_price,
            guard_price=max_buy_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"open_gap_bps": open_gap_bps, "yellow_open_gap_bps": yellow_open_gap_bps},
        )
    if chase_bps > yellow_chase_bps:
        return PriceGuardDecision(
            action="REDUCE",
            reason_code=REDUCE_YELLOW_CHASE_BAND,
            size_multiplier=yellow_size_multiplier,
            max_buy_price=max_buy_price,
            guard_price=max_buy_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"chase_bps": chase_bps, "yellow_chase_bps": yellow_chase_bps},
        )
    return PriceGuardDecision(
        action="ACCEPT",
        reason_code=ACCEPT_WITHIN_GREEN_ZONE,
        max_buy_price=max_buy_price,
        guard_price=max_buy_price,
        guidance_status=policy.guidance_status,
        policy_sha256=policy.policy_sha256,
        details={"open_gap_bps": open_gap_bps, "chase_bps": chase_bps},
    )


def _evaluate_sell(ctx: PriceGuardContext, policy: PriceGuardPolicy) -> PriceGuardDecision:
    signal = _positive(ctx.signal_ref_price)
    execution_price = _positive(ctx.open_price) or _positive(ctx.current_price)
    if execution_price is None:
        return PriceGuardDecision(
            action="WAITING",
            reason_code=WAITING_FOR_PRICE_GUARD_INPUT,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"missing": "open_price_or_current_price"},
        )
    sell = dict(policy.sell or {})
    risk_exit = str(ctx.sell_reason or "").strip().lower() in {"risk_exit", "stop_loss", "hard_stop"}
    slippage_bps = float(
        sell.get("risk_exit_max_slippage_bps" if risk_exit else "rebalance_max_slippage_bps", 500.0 if risk_exit else 150.0)
    )
    min_sell_price = signal * (1.0 - slippage_bps / 10000.0)
    if risk_exit:
        return PriceGuardDecision(
            action="SELL",
            reason_code=EXECUTE_RISK_EXIT_WITH_WIDER_LIMIT,
            min_sell_price=min_sell_price,
            guard_price=min_sell_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"sell_reason": ctx.sell_reason, "slippage_bps": slippage_bps},
        )
    if execution_price < min_sell_price:
        return PriceGuardDecision(
            action="SKIP",
            reason_code=SKIP_BELOW_MIN_SELL_PRICE_REBALANCE,
            size_multiplier=0.0,
            min_sell_price=min_sell_price,
            guard_price=min_sell_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"execution_price": execution_price, "min_sell_price": min_sell_price},
        )
    return PriceGuardDecision(
        action="SELL",
        reason_code=ACCEPT_WITHIN_GREEN_ZONE,
        min_sell_price=min_sell_price,
        guard_price=min_sell_price,
        guidance_status=policy.guidance_status,
        policy_sha256=policy.policy_sha256,
    )


def _evaluate_breakout_addon(
    ctx: PriceGuardContext,
    policy: PriceGuardPolicy,
    *,
    execution_price: float,
    max_buy_price: float,
) -> PriceGuardDecision | None:
    buy = dict(policy.buy or {})
    addon = buy.get("breakout_addon")
    if not isinstance(addon, Mapping) or not bool(addon.get("enabled")):
        return None
    require_momentum = bool(addon.get("require_momentum_regime", True))
    momentum_ok = bool(ctx.momentum_regime_flag) or str(ctx.momentum_regime).lower() in {"true", "1", "momentum", "strong", "up"}
    if require_momentum and not momentum_ok:
        return None
    if not _score_bucket_at_least(ctx.score_bucket, str(addon.get("min_score_bucket") or "top5")):
        return None
    dist_limit = ctx.dist_to_limit_up_bps
    if dist_limit is None:
        limit_up = _positive(ctx.limit_up)
        if limit_up is None:
            return None
        dist_limit = max(0.0, (limit_up - execution_price) / execution_price * 10000.0)
    if dist_limit >= float(addon.get("dist_to_limit_up_lt_bps", 200.0)):
        return None
    volume_ratio = float(ctx.volume_ratio_open or 0.0)
    if volume_ratio < float(addon.get("min_volume_ratio_open", 1.5)):
        return None
    fill_probability = float(ctx.fill_probability if ctx.fill_probability is not None else 1.0)
    min_fill_probability = float(addon.get("min_fill_probability", 0.6))
    if fill_probability < min_fill_probability:
        return PriceGuardDecision(
            action="SKIP",
            reason_code=SKIP_BREAKOUT_LOW_FILL_PROBABILITY,
            size_multiplier=0.0,
            max_buy_price=max_buy_price,
            guard_price=max_buy_price,
            guidance_status=policy.guidance_status,
            policy_sha256=policy.policy_sha256,
            details={"fill_probability": fill_probability, "min_fill_probability": min_fill_probability},
        )
    multiplier = float(addon.get("add_size_multiplier", 0.5))
    return PriceGuardDecision(
        action="ADD",
        reason_code=ADD_BREAKOUT_NEAR_LIMIT,
        size_multiplier=multiplier,
        max_buy_price=max_buy_price,
        guard_price=max_buy_price,
        guidance_status=policy.guidance_status,
        policy_sha256=policy.policy_sha256,
        details={"dist_to_limit_up_bps": dist_limit, "volume_ratio_open": volume_ratio, "fill_probability": fill_probability},
    )


def _validate_common_context(ctx: PriceGuardContext, policy: PriceGuardPolicy) -> None:
    if str(ctx.price_basis or "").strip() != str(policy.price_basis or "").strip():
        raise RuntimeConfigInvalidError(
            "price_guard context price_basis does not match policy",
            context={"reason_code": PRICE_BASIS_MISMATCH_ERROR, "context_basis": ctx.price_basis, "policy_basis": policy.price_basis},
        )
    if _positive(ctx.signal_ref_price) is None:
        raise DataUnavailableError(
            "price_guard requires signal_ref_price",
            context={"reason_code": SIGNAL_REF_PRICE_MISSING_DATA_ERROR, "signal_ref_price": ctx.signal_ref_price},
        )


def _required_bps(config: Mapping[str, Any], key: str) -> float:
    value = config.get(key)
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeConfigInvalidError(
            "price_guard bps config must be numeric",
            context={"reason_code": UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR, "key": key, "value": value},
        ) from exc
    if not isfinite(parsed) or parsed < 0:
        raise RuntimeConfigInvalidError(
            "price_guard bps config must be non-negative finite",
            context={"reason_code": UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR, "key": key, "value": value},
        )
    return parsed


def _positive(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _bps(price: float, reference: float) -> float:
    return (price - reference) / reference * 10000.0


def _merge_dict(base: dict[str, Any], override: Any) -> dict[str, Any]:
    merged = dict(base)
    if isinstance(override, Mapping):
        for key, value in override.items():
            if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
                child = dict(merged[key])
                child.update(dict(value))
                merged[key] = child
            else:
                merged[key] = value
    return merged


def _score_bucket_at_least(actual: str | None, required: str) -> bool:
    order = {"top1": 1, "top3": 3, "top5": 5, "top10": 10, "top20": 20, "top40": 40, "top40%": 40}
    actual_value = order.get(str(actual or "").lower())
    required_value = order.get(str(required or "").lower())
    if actual_value is None or required_value is None:
        return False
    return actual_value <= required_value
