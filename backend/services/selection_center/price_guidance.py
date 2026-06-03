"""Selection Center advisory entry/stop guidance.

This module is display/advisory only. It must not create orders, write ledgers,
or claim QE-validated PnL.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from math import isfinite
from typing import Any, Iterable, Mapping

from backend.services.selection_center.models import SelectionCandidate
from backend.services.strategy_package.execution_policy import compute_execution_policy_sha256
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.price_guard import PriceGuardContext, PriceGuardPolicy, evaluate as evaluate_price_guard


PRICE_GUIDANCE_COMPONENT_KEY = "selection_price_guidance"
GUIDANCE_STATUS_RULE_DEFAULT = "rule_default"
PRICE_BASIS_RAW = "raw"
TICK_SIZE = Decimal("0.01")
DISCLAIMER = "投顾区间仅用于选股展示；最终委托价需由后续 PriceGuard/执行层按当时行情重新确认。"

DEFAULT_ADVISORY_POLICY_JSON: dict[str, Any] = {
    "algo_code": "ADVISORY_PRICE_GUARD_RULE_V1",
    "algo_config": {},
    "price_guard": {
        "contract": "execution_price_guard_v1",
        "enabled": True,
        "mode": "rule_v1",
        "price_basis": PRICE_BASIS_RAW,
        "signal_ref_price": {"buy": "signal_close", "sell": "signal_close", "intraday": "arrival_price"},
        "buy": {
            "max_open_gap_bps": 300,
            "yellow_open_gap_bps": 150,
            "yellow_size_multiplier": 0.5,
            "max_chase_bps": 100,
            "near_limit_up_skip_bps": 80,
            "allow_partial": True,
            "breakout_addon": {
                "enabled": False,
                "require_momentum_regime": True,
                "min_score_bucket": "top5",
                "dist_to_limit_up_lt_bps": 200,
                "min_volume_ratio_open": 1.5,
                "add_size_multiplier": 0.5,
                "min_fill_probability": 0.6,
            },
        },
        "sell": {
            "rebalance_max_slippage_bps": 150,
            "risk_exit_max_slippage_bps": 500,
            "near_limit_down_rebalance_skip_bps": 80,
            "allow_partial": True,
        },
        "guidance_status": GUIDANCE_STATUS_RULE_DEFAULT,
    },
    "exit_guard": {
        "contract": "exit_guard_v1",
        "enabled": True,
        "mode": "rule_v1",
        "price_basis": PRICE_BASIS_RAW,
        "stop_loss": {"enabled": True, "max_loss_bps": 600, "soft_loss_bps": 400, "volatility_multiple": 2.5, "reference": "actual_entry_cost"},
        "take_profit": {"enabled": False, "take_profit_bps": 1200, "trailing_stop_bps": 500},
        "alpha_decay_exit": {"enabled": True, "rank_drop_below": "top40%", "confirm_days": 2},
        "time_stop": {"enabled": False, "max_holding_days": 10},
        "t1_handling": "defer_to_next_tradable_day",
    },
}


def attach_price_guidance(
    candidates: Iterable[SelectionCandidate],
    *,
    trade_date: Any,
    runtime_config: Mapping[str, Any] | None = None,
) -> list[SelectionCandidate]:
    return [build_price_guidance(candidate, trade_date=trade_date, runtime_config=runtime_config) for candidate in candidates]


def build_price_guidance(
    candidate: SelectionCandidate,
    *,
    trade_date: Any,
    runtime_config: Mapping[str, Any] | None = None,
) -> SelectionCandidate:
    config = dict((runtime_config or {}).get("price_guidance") or {})
    if config.get("enabled") is False:
        return candidate
    price_basis = str(config.get("price_basis") or PRICE_BASIS_RAW)
    if price_basis != PRICE_BASIS_RAW:
        raise RuntimeConfigInvalidError(
            "selection price guidance requires raw price_basis",
            context={"price_basis": price_basis, "reason_code": "PRICE_BASIS_MISMATCH_ERROR"},
        )

    signal_ref_price = _positive(candidate.selection_entry_price)
    if signal_ref_price is None:
        return _mark_guidance_unavailable(candidate, reason="signal_ref_price_missing")
    policy_json = _policy_json_from_config(config)
    policy_sha256 = str(config.get("policy_sha256") or compute_execution_policy_sha256(policy_json))
    if not policy_sha256:
        return _mark_guidance_unavailable(candidate, reason="policy_sha256_missing", signal_ref_price=signal_ref_price)

    price_guard_payload = dict(policy_json["price_guard"])
    price_guard_payload["policy_sha256"] = policy_sha256
    policy = PriceGuardPolicy.from_dict(price_guard_payload)
    previous_close = _positive(candidate.previous_close) or signal_ref_price
    limit_down, limit_up, limit_pct = _limit_prices(candidate.symbol, previous_close)
    score_bucket = _score_bucket(candidate.rank)
    expected_alpha_bps = _expected_alpha_bps(candidate, config)
    buy_policy = dict(policy.buy)
    alpha_budget_bps = min(
        float(buy_policy.get("max_open_gap_bps", 300.0)),
        float(buy_policy.get("max_chase_bps", 100.0)) + max(1.0, expected_alpha_bps * float(config.get("alpha_budget_fraction", 0.25))),
    )
    policy_for_eval = replace(
        policy,
        buy={
            **buy_policy,
            "max_chase_bps": alpha_budget_bps,
            "yellow_chase_bps": min(float(buy_policy.get("yellow_open_gap_bps", 150.0)), alpha_budget_bps / 2.0),
        },
    )
    max_buy_price = min(signal_ref_price * (1.0 + alpha_budget_bps / 10000.0), limit_up)
    ctx = PriceGuardContext(
        signal_ref_price=signal_ref_price,
        prev_close=previous_close,
        open_price=signal_ref_price,
        current_price=signal_ref_price,
        limit_up=limit_up,
        limit_down=limit_down,
        open_gap_bps=0.0,
        current_gap_bps=0.0,
        dist_to_limit_up_bps=max(0.0, (limit_up - signal_ref_price) / signal_ref_price * 10000.0),
        dist_to_limit_down_bps=max(0.0, (signal_ref_price - limit_down) / signal_ref_price * 10000.0),
        volume_ratio_open=None,
        amount_20d=None,
        board_type=_board_type(candidate.symbol),
        st_flag=False,
        suspend_status=None,
        score=candidate.score,
        rank=candidate.rank,
        score_bucket=score_bucket,
        expected_alpha_bps=expected_alpha_bps,
        alpha_family=str(config.get("alpha_family") or "multi_factor"),
        target_weight=candidate.target_weight,
        market_regime=None,
        momentum_regime=None,
        event_flag=None,
        side="buy",
        price_basis=PRICE_BASIS_RAW,
        feature_availability_ts=str(candidate.selection_entry_price_time or trade_date),
    )
    initial_decision = evaluate_price_guard(ctx, policy_for_eval)
    entry_band = _entry_band(
        signal_ref_price=signal_ref_price,
        limit_down=limit_down,
        limit_up=limit_up,
        limit_pct=limit_pct,
        max_buy_price=max_buy_price,
        policy=policy_for_eval,
        policy_sha256=policy_sha256,
        reference_source=str(candidate.selection_entry_price_source or "selection_entry_price"),
        score_bucket=score_bucket,
        expected_alpha_bps=expected_alpha_bps,
        initial_reason=initial_decision.reason_code,
    )
    stop_zone = _stop_zone(
        signal_ref_price=signal_ref_price,
        limit_down=limit_down,
        policy_json=policy_json,
        policy_sha256=policy_sha256,
        reference_source=str(candidate.selection_entry_price_source or "selection_entry_price"),
    )
    payload = {
        "schema_version": "selection_price_guidance_v1",
        "signal_ref_price": signal_ref_price,
        "entry_band": entry_band,
        "stop_loss_zone": stop_zone,
        "guidance_status": GUIDANCE_STATUS_RULE_DEFAULT,
        "price_guard_policy_sha256": policy_sha256,
        "generated_for_trade_date": str(trade_date),
    }
    component_scores = dict(candidate.component_scores or {})
    component_scores[PRICE_GUIDANCE_COMPONENT_KEY] = payload
    return candidate.model_copy(
        update={
            "signal_ref_price": signal_ref_price,
            "suggested_entry_price_band": entry_band,
            "suggested_stop_loss_zone": stop_zone,
            "guidance_status": GUIDANCE_STATUS_RULE_DEFAULT,
            "price_guard_policy_sha256": policy_sha256,
            "component_scores": component_scores,
        }
    )


def guidance_fields_from_component_scores(component_scores: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = (component_scores or {}).get(PRICE_GUIDANCE_COMPONENT_KEY)
    if not isinstance(payload, dict):
        return {}
    return {
        "signal_ref_price": payload.get("signal_ref_price"),
        "suggested_entry_price_band": payload.get("entry_band"),
        "suggested_stop_loss_zone": payload.get("stop_loss_zone"),
        "guidance_status": payload.get("guidance_status"),
        "price_guard_policy_sha256": payload.get("price_guard_policy_sha256"),
    }


def _policy_json_from_config(config: Mapping[str, Any]) -> dict[str, Any]:
    policy_json = dict(config.get("policy_json") or DEFAULT_ADVISORY_POLICY_JSON)
    if "price_guard" not in policy_json or "exit_guard" not in policy_json:
        raise RuntimeConfigInvalidError(
            "selection price guidance policy_json requires price_guard and exit_guard",
            context={"missing": [key for key in ("price_guard", "exit_guard") if key not in policy_json]},
        )
    return policy_json


def _entry_band(
    *,
    signal_ref_price: float,
    limit_down: float,
    limit_up: float,
    limit_pct: float,
    max_buy_price: float,
    policy: PriceGuardPolicy,
    policy_sha256: str,
    reference_source: str,
    score_bucket: str,
    expected_alpha_bps: float,
    initial_reason: str,
) -> dict[str, Any]:
    buy = dict(policy.buy)
    yellow_open_gap_bps = float(buy.get("yellow_open_gap_bps", 150.0))
    yellow_size_multiplier = float(buy.get("yellow_size_multiplier", 0.5))
    green_max = min(max_buy_price, signal_ref_price * (1.0 + yellow_open_gap_bps / 10000.0))
    max_buy = min(max_buy_price, limit_up)
    green_min = max(limit_down, signal_ref_price * 0.995)
    red_min = min(limit_up, max_buy + float(TICK_SIZE))
    return {
        "range_source": "alpha_budget_based",
        "reference_source": reference_source,
        "price_basis": PRICE_BASIS_RAW,
        "policy_sha256": policy_sha256,
        "guidance_status": GUIDANCE_STATUS_RULE_DEFAULT,
        "signal_ref_price": _round_nearest(signal_ref_price),
        "score_bucket": score_bucket,
        "expected_alpha_bps": expected_alpha_bps,
        "tick_size": float(TICK_SIZE),
        "limit_up": _round_floor(limit_up),
        "limit_down": _round_ceiling(limit_down),
        "limit_pct": limit_pct,
        "limit_source": "estimated_from_signal_ref_price_or_previous_close",
        "green": {
            "min_price": _round_ceiling(green_min),
            "max_price": _round_floor(green_max),
            "reason_code": "ACCEPT_WITHIN_GREEN_ZONE",
        },
        "yellow": {
            "min_price": _round_ceiling(green_max + float(TICK_SIZE)),
            "max_price": _round_floor(max_buy),
            "size_multiplier": yellow_size_multiplier,
            "reason_code": "REDUCE_YELLOW_OPEN_GAP",
        },
        "red": {
            "min_price": _round_ceiling(red_min),
            "max_price": _round_floor(limit_up),
            "reason_code": "SKIP_OPEN_GAP_EXCEEDED",
        },
        "max_buy_price": _round_floor(max_buy),
        "initial_decision_reason_code": initial_reason,
        "breakout_addon_enabled": bool((buy.get("breakout_addon") or {}).get("enabled")) if isinstance(buy.get("breakout_addon"), dict) else False,
        "disclaimer": DISCLAIMER,
    }


def _stop_zone(
    *,
    signal_ref_price: float,
    limit_down: float,
    policy_json: Mapping[str, Any],
    policy_sha256: str,
    reference_source: str,
) -> dict[str, Any]:
    exit_guard = dict(policy_json["exit_guard"])
    stop_loss = dict(exit_guard.get("stop_loss") or {})
    hard_loss_bps = float(stop_loss.get("max_loss_bps", 600.0))
    soft_loss_bps = float(stop_loss.get("soft_loss_bps", 400.0))
    soft_stop = max(limit_down, signal_ref_price * (1.0 - soft_loss_bps / 10000.0))
    hard_stop = max(limit_down, signal_ref_price * (1.0 - hard_loss_bps / 10000.0))
    take_profit = dict(exit_guard.get("take_profit") or {})
    take_profit_enabled = bool(take_profit.get("enabled", False))
    return {
        "range_source": "alpha_budget_based",
        "reference_source": reference_source,
        "price_basis": PRICE_BASIS_RAW,
        "policy_sha256": policy_sha256,
        "guidance_status": GUIDANCE_STATUS_RULE_DEFAULT,
        "soft_stop_price": _round_floor(soft_stop),
        "hard_stop_price": _round_floor(hard_stop),
        "soft_loss_bps": soft_loss_bps,
        "hard_loss_bps": hard_loss_bps,
        "limit_down": _round_ceiling(limit_down),
        "take_profit_enabled": take_profit_enabled,
        "take_profit_price": _round_floor(signal_ref_price * (1.0 + float(take_profit.get("take_profit_bps", 1200.0)) / 10000.0))
        if take_profit_enabled
        else None,
        "disclaimer": DISCLAIMER,
    }


def _mark_guidance_unavailable(
    candidate: SelectionCandidate,
    *,
    reason: str,
    signal_ref_price: float | None = None,
) -> SelectionCandidate:
    payload = {
        "schema_version": "selection_price_guidance_v1",
        "guidance_unavailable_reason": reason,
        "signal_ref_price": signal_ref_price,
        "guidance_status": None,
    }
    component_scores = dict(candidate.component_scores or {})
    component_scores[PRICE_GUIDANCE_COMPONENT_KEY] = payload
    return candidate.model_copy(update={"signal_ref_price": signal_ref_price, "component_scores": component_scores})


def _expected_alpha_bps(candidate: SelectionCandidate, config: Mapping[str, Any]) -> float:
    configured = _positive(config.get("expected_alpha_bps"))
    if configured is not None:
        return configured
    scores = candidate.component_scores or {}
    for key in ("expected_alpha_bps", "alpha_budget_bps"):
        parsed = _positive(scores.get(key))
        if parsed is not None:
            return parsed
    rank = int(candidate.rank)
    if rank <= 5:
        return 400.0
    if rank <= 20:
        return 300.0
    return 200.0


def _score_bucket(rank: int) -> str:
    if rank <= 5:
        return "top5"
    if rank <= 10:
        return "top10"
    if rank <= 20:
        return "top20"
    return "top40"


def _limit_prices(symbol: str, previous_close: float) -> tuple[float, float, float]:
    pct = _limit_pct(symbol)
    return previous_close * (1.0 - pct), previous_close * (1.0 + pct), pct


def _limit_pct(symbol: str) -> float:
    code = str(symbol or "").split(".")[0]
    suffix = str(symbol or "").split(".")[-1].upper() if "." in str(symbol or "") else ""
    if suffix in {"BJ", "BSE"} or code.startswith(("4", "8")):
        return 0.30
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10


def _board_type(symbol: str) -> str:
    code = str(symbol or "").split(".")[0]
    suffix = str(symbol or "").split(".")[-1].upper() if "." in str(symbol or "") else ""
    if suffix in {"BJ", "BSE"}:
        return "BJ"
    if code.startswith(("688", "689")):
        return "STAR"
    if code.startswith(("300", "301")):
        return "CHINEXT"
    return "MAIN"


def _round_floor(value: float) -> float:
    adjusted = round(float(value) + 1e-9, 10)
    return float((Decimal(str(adjusted)) / TICK_SIZE).to_integral_value(rounding=ROUND_FLOOR) * TICK_SIZE)


def _round_ceiling(value: float) -> float:
    return float((Decimal(str(value)) / TICK_SIZE).to_integral_value(rounding=ROUND_CEILING) * TICK_SIZE)


def _round_nearest(value: float) -> float:
    return float(Decimal(str(value)).quantize(TICK_SIZE))


def _positive(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed <= 0:
        return None
    return parsed
