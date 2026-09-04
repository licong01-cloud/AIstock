"""Frozen guard snapshots, board-lot sizing, and componentized costs."""

from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal, ROUND_CEILING
from typing import Any, Iterable

from backend.execution_algos.board_lot import board_lot_rule, round_to_board_lot
from backend.services.trading_core.exit_guard import ExitGuardPolicy
from backend.services.trading_core.price_guard import PriceGuardPolicy

from .contracts import (
    LegCostEstimateV1,
    ParentOrderCostScenarioV1,
    TriggerSide,
    TypedStatus,
    canonical_sha256,
)


POLICY_SOURCE_REPOSITORY_COMMIT = "f870debe3b963d9d3d41ce9663db9722af921e80"
POLICY_SOURCE_CAPTURED_AT = "2026-09-03T14:33:32+08:00"

PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1: dict[str, Any] = {
    "contract": "execution_price_guard_v1",
    "enabled": True,
    "mode": "rule_v1",
    "price_basis": "raw",
    "signal_ref_price": {"buy": "signal_close", "sell": "signal_close", "intraday": "arrival_price"},
    "buy": {
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
    },
    "sell": {
        "rebalance_max_slippage_bps": 150.0,
        "risk_exit_max_slippage_bps": 500.0,
        "near_limit_down_rebalance_skip_bps": 80.0,
        "allow_partial": True,
    },
    "guidance_status": "rule_default",
}

EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1: dict[str, Any] = {
    "contract": "exit_guard_v1",
    "enabled": True,
    "mode": "rule_v1",
    "price_basis": "raw",
    "stop_loss": {
        "enabled": True,
        "max_loss_bps": 600.0,
        "soft_loss_bps": 400.0,
        "volatility_multiple": 2.5,
        "reference": "actual_entry_cost",
    },
    "take_profit": {"enabled": False, "take_profit_bps": 1200.0, "trailing_stop_bps": 500.0},
    "alpha_decay_exit": {"enabled": True, "rank_drop_below": "top40%", "confirm_days": 2},
    "time_stop": {"enabled": False, "max_holding_days": 10},
    "t1_handling": "defer_to_next_tradable_day",
    "guidance_status": "rule_default",
}

PERSONAL_MANUAL_COMPONENT_COST_V1: dict[str, Any] = {
    "cost_policy_version": "PERSONAL_MANUAL_COMPONENT_COST_V1",
    "fee_schedule_as_of": "2026-09-03",
    "fee_source_refs": [
        "SSE_CHARGE_SCHEDULE_2026_01",
        "SZSE_FEE_AND_TAX_SCHEDULE_2026_01",
        "CHINACLEAR_SH_SZ_SECURITIES_FEE_SCHEDULE_2025_06",
        "SAT_STAMP_DUTY_HALVING_2023_08",
        "USER_BROKER_NET_COMMISSION_QUOTE_2026_09_03",
    ],
    "net_commission_rate": "0.000085",
    "minimum_commission_cny": "5",
    "transfer_fee_rate": "0.000010",
    "regulatory_fee_rate": "0.000020",
    "handling_fee_rate": "0.0000341",
    "stamp_duty_sell_rate": "0.000500",
    "commission_quote_basis": "NET_EX_REGULATORY_FEES",
    "min_commission_scope": "PER_PARENT_ORDER",
    "min_commission_scope_verification": "BROKER_UNVERIFIED",
    "assumed_parent_order_count": 1,
    "calculation_precision": "UNROUNDED_COMPONENT_ESTIMATE",
}


def _without_policy_hash(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("policy_sha256", None)
    return result


def guard_snapshot_envelope(*, kind: str) -> dict[str, Any]:
    if kind == "price_guard":
        source_module = "backend.services.trading_core.price_guard"
        source_symbol = "PriceGuardPolicy"
        policy = PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1
        snapshot_version = "PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1"
    elif kind == "exit_guard":
        source_module = "backend.services.trading_core.exit_guard"
        source_symbol = "ExitGuardPolicy"
        policy = EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1
        snapshot_version = "EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1"
    else:
        raise ValueError(f"unsupported guard snapshot kind: {kind}")
    source_defaults_sha256 = canonical_sha256(policy)
    timing_policy_sha256 = canonical_sha256(
        {"snapshot_version": snapshot_version, "policy": policy, "source_defaults_sha256": source_defaults_sha256}
    )
    return {
        "schema_version": "position_timing_guard_snapshot_v1",
        "snapshot_version": snapshot_version,
        "policy": policy,
        "provenance": {
            "source_module": source_module,
            "source_symbol": source_symbol,
            "source_repository_commit": POLICY_SOURCE_REPOSITORY_COMMIT,
            "source_captured_at": POLICY_SOURCE_CAPTURED_AT,
            "source_defaults_sha256": source_defaults_sha256,
            "timing_policy_sha256": timing_policy_sha256,
        },
    }


PRICE_GUARD_SNAPSHOT_ENVELOPE_V1 = guard_snapshot_envelope(kind="price_guard")
EXIT_GUARD_SNAPSHOT_ENVELOPE_V1 = guard_snapshot_envelope(kind="exit_guard")
PRICE_GUARD_SNAPSHOT_ARTIFACT_SHA256 = canonical_sha256(PRICE_GUARD_SNAPSHOT_ENVELOPE_V1)
EXIT_GUARD_SNAPSHOT_ARTIFACT_SHA256 = canonical_sha256(EXIT_GUARD_SNAPSHOT_ENVELOPE_V1)
COST_POLICY_SHA256 = canonical_sha256(PERSONAL_MANUAL_COMPONENT_COST_V1)


def assert_shared_guard_defaults_unmodified() -> None:
    """Fail loudly if shared default factories drift from the audited snapshot."""

    current_price = _without_policy_hash(asdict(PriceGuardPolicy()))
    current_exit = _without_policy_hash(asdict(ExitGuardPolicy()))
    if canonical_sha256(current_price) != canonical_sha256(PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1):
        raise RuntimeError("PRICE_GUARD_SHARED_DEFAULT_DRIFT")
    if canonical_sha256(current_exit) != canonical_sha256(EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1):
        raise RuntimeError("EXIT_GUARD_SHARED_DEFAULT_DRIFT")


def frozen_price_guard_policy() -> PriceGuardPolicy:
    payload = dict(PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1)
    payload["policy_sha256"] = PRICE_GUARD_SNAPSHOT_ENVELOPE_V1["provenance"]["timing_policy_sha256"]
    return PriceGuardPolicy.from_dict(payload)


def frozen_exit_guard_policy() -> ExitGuardPolicy:
    payload = dict(EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1)
    payload["policy_sha256"] = EXIT_GUARD_SNAPSHOT_ENVELOPE_V1["provenance"]["timing_policy_sha256"]
    return ExitGuardPolicy.from_dict(payload)


def planned_full_notional_threshold(exposure: Decimal | str | float) -> int:
    fraction = Decimal(str(exposure))
    if fraction <= 0:
        raise ValueError("exposure must be positive")
    minimum = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["minimum_commission_cny"])
    rate = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["net_commission_rate"])
    return int((minimum / (rate * fraction)).to_integral_value(rounding=ROUND_CEILING))


def legal_target_quantity(*, notional_cny: Decimal, price_raw: Decimal, symbol: str) -> int:
    if notional_cny <= 0 or price_raw <= 0:
        return 0
    requested = int(notional_cny / price_raw)
    return round_to_board_lot(requested, symbol, side="BUY")


def _split_quantities(
    *, quantity: int, symbol: str, side: TriggerSide, requested_count: int, full_exit: bool
) -> tuple[int, ...] | None:
    if requested_count < 1 or requested_count > 3 or quantity <= 0:
        return None
    min_qty, increment = board_lot_rule(symbol)
    if requested_count == 1:
        if side is TriggerSide.SELL and full_exit:
            return (quantity,)
        legal = round_to_board_lot(quantity, symbol, side=side.value, allow_sell_residual=False)
        return (legal,) if legal == quantity and legal > 0 else None
    minimum_required = (
        min_qty * (requested_count - 1) + 1
        if side is TriggerSide.SELL and full_exit
        else min_qty * requested_count
    )
    if quantity < minimum_required:
        return None
    if side is TriggerSide.BUY and quantity % increment != 0:
        return None

    # Allocate approximately equal legal children.  For a full SELL, only the
    # final parent order may carry the odd residual.
    remaining = quantity
    parts: list[int] = []
    for index in range(requested_count - 1):
        slots = requested_count - index
        ideal = remaining // slots
        part = (ideal // increment) * increment
        part = max(min_qty, part)
        remaining_slots = slots - 1
        reserve = (
            min_qty * (remaining_slots - 1) + 1
            if side is TriggerSide.SELL and full_exit
            else min_qty * remaining_slots
        )
        if remaining - part < reserve:
            part = remaining - reserve
            part = (part // increment) * increment
        if part < min_qty or part % increment != 0:
            return None
        parts.append(part)
        remaining -= part
    if remaining <= 0:
        return None
    if side is TriggerSide.SELL and full_exit:
        parts.append(remaining)
    elif remaining >= min_qty and remaining % increment == 0:
        parts.append(remaining)
    else:
        return None
    return tuple(parts) if sum(parts) == quantity else None


def _cost_for_notionals(*, side: TriggerSide, notionals: Iterable[Decimal]) -> dict[str, Decimal]:
    values = tuple(notionals)
    commission_rate = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["net_commission_rate"])
    minimum = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["minimum_commission_cny"])
    transfer_rate = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["transfer_fee_rate"])
    regulatory_rate = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["regulatory_fee_rate"])
    handling_rate = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["handling_fee_rate"])
    stamp_rate = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["stamp_duty_sell_rate"])
    commission = sum((max(minimum, value * commission_rate) for value in values), Decimal("0"))
    total_notional = sum(values, Decimal("0"))
    transfer = total_notional * transfer_rate
    regulatory = total_notional * regulatory_rate
    handling = total_notional * handling_rate
    stamp = total_notional * stamp_rate if side is TriggerSide.SELL else Decimal("0")
    total = commission + transfer + regulatory + handling + stamp
    bps = total / total_notional * Decimal("10000") if total_notional > 0 else Decimal("0")
    return {
        "commission": commission,
        "transfer": transfer,
        "regulatory": regulatory,
        "handling": handling,
        "stamp": stamp,
        "total": total,
        "bps": bps,
    }


def estimate_leg_cost(
    *,
    side: TriggerSide,
    quantity: int,
    reference_price_raw: Decimal,
    symbol: str,
    full_exit: bool = False,
) -> LegCostEstimateV1:
    if side not in {TriggerSide.BUY, TriggerSide.SELL}:
        raise ValueError("cost estimate side must be BUY or SELL")
    if quantity <= 0 or reference_price_raw <= 0:
        raise ValueError("cost estimate requires positive quantity and price")
    total_notional = reference_price_raw * quantity
    scenarios: list[ParentOrderCostScenarioV1] = []
    scenario_names = {
        1: "ONE_PARENT_ORDER_BASE",
        2: "TWO_PARENT_ORDERS_NEAR_EQUAL_LEGAL_QTY",
        3: "THREE_PARENT_ORDERS_NEAR_EQUAL_LEGAL_QTY",
    }
    for count in (1, 2, 3):
        quantities = _split_quantities(
            quantity=quantity, symbol=symbol, side=side, requested_count=count, full_exit=full_exit
        )
        if quantities is None:
            scenarios.append(
                ParentOrderCostScenarioV1(
                    scenario=scenario_names[count],
                    requested_parent_order_count=count,
                    effective_parent_order_count=0,
                    status=TypedStatus.UNAVAILABLE,
                    parent_order_quantities=(),
                    parent_order_notionals_cny=(),
                    commission_cny=Decimal("0"),
                    transfer_fee_cny=Decimal("0"),
                    regulatory_fee_cny=Decimal("0"),
                    handling_fee_cny=Decimal("0"),
                    stamp_duty_cny=Decimal("0"),
                    total_cost_cny=Decimal("0"),
                    total_cost_bps=Decimal("0"),
                    reason_code="LEGAL_PARENT_ORDER_SPLIT_UNAVAILABLE",
                )
            )
            continue
        notionals = tuple(reference_price_raw * part for part in quantities)
        costs = _cost_for_notionals(side=side, notionals=notionals)
        scenarios.append(
            ParentOrderCostScenarioV1(
                scenario=scenario_names[count],
                requested_parent_order_count=count,
                effective_parent_order_count=len(quantities),
                status=TypedStatus.AVAILABLE,
                parent_order_quantities=quantities,
                parent_order_notionals_cny=notionals,
                commission_cny=costs["commission"],
                transfer_fee_cny=costs["transfer"],
                regulatory_fee_cny=costs["regulatory"],
                handling_fee_cny=costs["handling"],
                stamp_duty_cny=costs["stamp"],
                total_cost_cny=costs["total"],
                total_cost_bps=costs["bps"],
            )
        )
    commission_rate = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["net_commission_rate"])
    minimum = Decimal(PERSONAL_MANUAL_COMPONENT_COST_V1["minimum_commission_cny"])
    return LegCostEstimateV1(
        side=side,
        quantity=quantity,
        reference_price_raw=reference_price_raw,
        notional_cny=total_notional,
        small_trade_cost_heavy=total_notional * commission_rate < minimum,
        cost_policy_sha256=COST_POLICY_SHA256,
        scenarios=tuple(scenarios),
    )


def board_lot_identity(symbol: str) -> dict[str, Any]:
    minimum, increment = board_lot_rule(symbol)
    payload = {
        "source_module": "backend.execution_algos.board_lot",
        "source_symbol": "board_lot_rule/round_to_board_lot",
        "symbol": symbol,
        "minimum_quantity": minimum,
        "increment": increment,
        "buy_rounding": "FLOOR_TO_LEGAL_INCREMENT",
        "full_sell_residual": "FLUSH_ON_FINAL_PARENT_ORDER",
    }
    return {**payload, "identity_sha256": canonical_sha256(payload)}


__all__ = [
    "COST_POLICY_SHA256",
    "EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1",
    "EXIT_GUARD_SNAPSHOT_ENVELOPE_V1",
    "EXIT_GUARD_SNAPSHOT_ARTIFACT_SHA256",
    "PERSONAL_MANUAL_COMPONENT_COST_V1",
    "POLICY_SOURCE_REPOSITORY_COMMIT",
    "PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1",
    "PRICE_GUARD_SNAPSHOT_ENVELOPE_V1",
    "PRICE_GUARD_SNAPSHOT_ARTIFACT_SHA256",
    "assert_shared_guard_defaults_unmodified",
    "board_lot_identity",
    "estimate_leg_cost",
    "frozen_exit_guard_policy",
    "frozen_price_guard_policy",
    "legal_target_quantity",
    "planned_full_notional_threshold",
]
