"""Frozen core/tactical cash-account policies for PT-NEXT-023."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd

from backend.execution_algos.board_lot import round_to_board_lot
from backend.services.trading_core.exit_guard import ExitGuardContext, evaluate as evaluate_exit

from .action_value import ActionValueError
from .contracts import canonical_sha256
from .fundamental_timing import (
    _terminal,
    _trade_profitable,
    _trend_on,
    build_research_features,
)
from .pattern_close_cash_replay import Account, CAPITAL, ZERO, dec, execute, positive
from .pattern_strategy import acceleration_volume_exit
from .pattern_strategy_evolution import NO_OPEN_GAP_POLICY
from .policy import frozen_exit_guard_policy


S1 = "CORE_80_TACTICAL_20_V1"
S2 = "CORE_70_STAGED_15X2_V1"
POLICY_IDS = (S1, S2)
CORE_RATIOS = {S1: Decimal("0.80"), S2: Decimal("0.70")}
STAGE_FRACTION = Decimal("0.15")
S1_TACTICAL_FRACTION = Decimal("0.20")
SECOND_STAGE_ATR_MULTIPLE = Decimal("1.0")

POLICY_CONTRACT = {
    "schema": "position_timing_core_tactical_policy_set_v1",
    "policies": list(POLICY_IDS),
    "decision": "T_CLOSE",
    "execution": "T_PLUS_1_CLOSE",
    "initial_entry": "SAME_UNGUARDED_FULL_CASH_PLAN_AS_BUY_AND_HOLD",
    "core_ratios": {key: str(value) for key, value in CORE_RATIOS.items()},
    "s1_tactical_fraction": str(S1_TACTICAL_FRACTION),
    "s2_stage_fraction": str(STAGE_FRACTION),
    "s2_second_stage": "LATER_R6_EXHAUSTION_AND_PLUS_1_ATR_FROM_FIRST_TRIM_REFERENCE",
    "trend": "C_GT_SMA60_AND_SMA60_GT_SMA60_LAG5",
    "trend_exit": "TWO_CONSECUTIVE_GLOBAL_SESSIONS_C_LT_SMA60_MINUS_ATR20_TO_CORE_FLOOR",
    "risk": "FROZEN_RULE_DEFAULT_TO_CORE_FLOOR",
    "recovery": "RISK_CLEAR_AND_TREND_TRUE_AND_C_GT_SMA20_AND_R0_FALSE",
    "recovery_buy_guard": "NO_OPEN_GAP_POLICY",
    "account": "INDEPENDENT_10M_CASH_NATURAL_REINVESTMENT",
    "ordinary_full_exit": False,
}
POLICY_CONTRACT_SHA256 = canonical_sha256(POLICY_CONTRACT)


@dataclass
class CorePolicyState:
    account: Account
    hold: Account
    pending_risk: bool = False
    pending_trend: bool = False
    below_count: int = 0
    r0_edge: bool = False
    r6_edge: bool = False
    full_units_anchor: Decimal = ZERO
    core_floor_units: Decimal = ZERO
    trim_stage: int = 0
    first_trim_reference: Decimal | None = None
    first_trim_atr: Decimal | None = None
    last_trim_execution: int | None = None
    min_floor_gap: Decimal | None = None


def _reset_anchor(state: CorePolicyState, policy_id: str) -> None:
    state.full_units_anchor = state.account.units
    state.core_floor_units = state.account.units * CORE_RATIOS[policy_id]
    state.trim_stage = 0
    state.first_trim_reference = None
    state.first_trim_atr = None
    state.last_trim_execution = None
    state.pending_risk = False
    state.pending_trend = False
    state.r0_edge = False
    state.r6_edge = False


def _removable_units(state: CorePolicyState) -> Decimal:
    return max(ZERO, state.account.units - state.core_floor_units)


def _sell_fraction(
    state: CorePolicyState,
    desired_units: Decimal,
    *,
    symbol: str,
    execution_factor: Decimal,
) -> Decimal | None:
    removable = _removable_units(state)
    desired = min(removable, max(ZERO, desired_units))
    if desired <= ZERO or state.account.units <= ZERO or execution_factor <= ZERO:
        return None
    value = desired / state.account.units
    fraction = min(Decimal(1), value)
    raw_available = int(state.account.units * execution_factor)
    requested = int(Decimal(raw_available) * fraction)
    legal = round_to_board_lot(
        requested,
        symbol,
        side="SELL",
        allow_sell_residual=False,
    )
    return fraction if legal else None


def _to_floor_fraction(
    state: CorePolicyState,
    *,
    symbol: str,
    execution_factor: Decimal,
) -> Decimal | None:
    return _sell_fraction(
        state,
        _removable_units(state),
        symbol=symbol,
        execution_factor=execution_factor,
    )


def _guard_active(state: CorePolicyState, reference: Decimal, decision: int) -> bool:
    guard = evaluate_exit(
        ExitGuardContext(
            actual_entry_cost=float(state.account.entry),
            current_price=float(reference),
            days_since_entry=decision - state.account.bought_on,
            t1_eligible=True,
            suspend_status="ACTIVE",
            price_basis="raw",
        ),
        frozen_exit_guard_policy(),
    )
    return bool(guard.should_exit)


def _event(features: pd.DataFrame, ordinal: int, template: str) -> tuple[bool | None, dict[str, Any]]:
    matched, reason = acceleration_volume_exit(features, ordinal, template)
    if not reason.get("available"):
        return None, reason
    return bool(matched), reason


def _update_floor_gap(state: CorePolicyState) -> None:
    if state.full_units_anchor <= ZERO:
        return
    gap = state.account.units - state.core_floor_units
    if gap < ZERO:
        raise ActionValueError(
            "CORE_TACTICAL_FLOOR_VIOLATION",
            units=str(state.account.units),
            floor=str(state.core_floor_units),
        )
    state.min_floor_gap = gap if state.min_floor_gap is None else min(state.min_floor_gap, gap)


def _replay_one(
    symbol: str,
    bars: pd.DataFrame,
    *,
    policy_id: str,
    enrollment_ordinal: int,
    adjusted: pd.DataFrame,
    pattern: pd.DataFrame,
    trend: pd.DataFrame,
    ready: np.ndarray,
) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    if policy_id not in POLICY_IDS:
        raise ActionValueError("CORE_TACTICAL_POLICY_UNKNOWN", policy_id=policy_id)
    if enrollment_ordinal < 0 or enrollment_ordinal >= len(bars) - 1:
        raise ActionValueError("CORE_TACTICAL_ENROLLMENT_ORDINAL_INVALID")
    raw_rows = bars.to_dict("records")
    pit = bars.pit_active.to_numpy(bool)
    state = CorePolicyState(Account(), Account())
    rows: list[dict[str, Any]] = [{
        "ordinal": enrollment_ordinal,
        "timing_nav": float(CAPITAL),
        "hold_nav": float(CAPITAL),
        "timing_exposure": 0.0,
        "hold_exposure": 0.0,
        "valuation_status": "CASH_ANCHOR",
        "stale_mark": False,
        "trim_stage": 0,
        "floor_gap_units": 0.0,
    }]
    fills: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    exposure_states: Counter[str] = Counter()
    last_mark: Decimal | None = None

    for decision in range(enrollment_ordinal, len(bars) - 1):
        target = decision + 1
        bar = raw_rows[target]
        value = adjusted.close.iloc[decision]
        reference = dec(value) if positive(value) else last_mark
        plan: str | None = None
        authority = "HOLD" if state.account.units else "WAIT"
        sell_fraction: Decimal | None = None
        r0_value: bool | None = None
        r6_value: bool | None = None
        attempted_r0 = attempted_r6 = False
        guard_now = False
        execution_factor = dec(bar["factor"])

        if state.account.units:
            trend_observed = bool(ready[decision])
            trend_on = trend_observed and _trend_on(trend, decision)
            below = trend_observed and bool(
                trend.iloc[decision].close < trend.iloc[decision].sma60 - trend.iloc[decision].atr20
            )
            state.below_count = state.below_count + 1 if below else 0
            if not trend_observed:
                state.below_count = 0
            state.pending_trend |= state.below_count >= 2
            if reference is not None and positive(value):
                guard_now = _guard_active(state, reference, decision)
                state.pending_risk |= guard_now
            if trend_observed:
                r0_value, _ = _event(pattern, decision, "R0")
                r6_value, _ = _event(pattern, decision, "R6")

            floor_fraction = _to_floor_fraction(
                state,
                symbol=symbol,
                execution_factor=execution_factor,
            )
            if state.pending_risk and floor_fraction is not None:
                plan, authority = "SELL", "FROZEN_RISK_TACTICAL_REDUCTION"
                sell_fraction = floor_fraction
            elif state.pending_trend and floor_fraction is not None:
                plan, authority = "SELL", "TWO_CLOSE_TREND_TACTICAL_REDUCTION"
                sell_fraction = floor_fraction
            elif policy_id == S1:
                eligible = bool(
                    r0_value is True
                    and reference is not None
                    and _trade_profitable(state.account, reference)
                )
                if eligible and not state.r0_edge and floor_fraction is not None:
                    plan, authority = "SELL", "R0_TACTICAL_20_TRIM"
                    sell_fraction = floor_fraction
                    attempted_r0 = True
            else:
                first_eligible = bool(
                    state.trim_stage == 0
                    and r0_value is True
                    and reference is not None
                    and _trade_profitable(state.account, reference)
                )
                if first_eligible and not state.r0_edge:
                    sell_fraction = _sell_fraction(
                        state,
                        state.full_units_anchor * STAGE_FRACTION,
                        symbol=symbol,
                        execution_factor=execution_factor,
                    )
                    if sell_fraction is not None:
                        plan, authority = "SELL", "R0_STAGE1_15_TRIM"
                        attempted_r0 = True
                elif (
                    state.trim_stage == 1
                    and state.last_trim_execution is not None
                    and decision >= state.last_trim_execution
                    and r6_value is True
                    and reference is not None
                    and state.first_trim_reference is not None
                    and state.first_trim_atr is not None
                    and reference >= state.first_trim_reference + SECOND_STAGE_ATR_MULTIPLE * state.first_trim_atr
                    and _trade_profitable(state.account, reference)
                ):
                    sell_fraction = _sell_fraction(
                        state,
                        state.full_units_anchor * STAGE_FRACTION,
                        symbol=symbol,
                        execution_factor=execution_factor,
                    )
                    if sell_fraction is not None:
                        plan, authority = "SELL", "R6_STAGE2_15_TRIM"
                        attempted_r6 = True

            if plan is None and state.trim_stage > 0:
                recovery_ready = bool(
                    reference is not None
                    and not state.pending_risk
                    and not state.pending_trend
                    and not guard_now
                    and trend_on
                    and trend.iloc[decision].close > trend.iloc[decision].sma20
                    and r0_value is False
                )
                if recovery_ready and state.account.cash > ZERO:
                    plan, authority = "BUY", "TACTICAL_RECOVERY"
        else:
            state.below_count = 0
            if not state.account.bought_once and pit[decision]:
                plan, authority = "BUY", "COMMON_INITIAL_ENTRY"

        actions = {
            "timing": plan,
            "hold": "BUY" if not state.hold.bought_once and pit[decision] else None,
        }
        timing_fill: dict[str, Any] | None = None
        for role, side in actions.items():
            if side is None:
                continue
            account = state.account if role == "timing" else state.hold
            if reference is None:
                fill = {"side": side, "status": "DECISION_REFERENCE_UNKNOWN"}
            else:
                is_initial = role == "hold" or authority == "COMMON_INITIAL_ENTRY"
                fill = execute(
                    account,
                    symbol=symbol,
                    bar=bar,
                    ordinal=target,
                    side=side,
                    reference=reference,
                    guarded=role == "timing" and not is_initial,
                    risk=role == "timing" and authority == "FROZEN_RISK_TACTICAL_REDUCTION",
                    sell_fraction=sell_fraction or Decimal(1),
                    price_guard_policy=NO_OPEN_GAP_POLICY if role == "timing" and not is_initial else None,
                )
            counts[f"{role}:{side}:{fill['status']}"] += 1
            before_stage = state.trim_stage
            event = {
                "symbol": symbol,
                "policy_id": policy_id,
                "role": role,
                "decision_ordinal": decision,
                "execution_ordinal": target,
                "authority": authority if role == "timing" else "BUY_AND_HOLD",
                "decision_reference": float(reference) if reference is not None else None,
                "planned_sell_fraction": float(sell_fraction) if sell_fraction is not None else None,
                "trim_stage_before": before_stage if role == "timing" else None,
                **fill,
            }
            if role == "timing":
                timing_fill = fill
                if fill["status"] == "FILLED":
                    if authority == "COMMON_INITIAL_ENTRY":
                        _reset_anchor(state, policy_id)
                    elif authority == "TACTICAL_RECOVERY":
                        _reset_anchor(state, policy_id)
                    elif side == "SELL":
                        if authority == "R0_STAGE1_15_TRIM":
                            state.trim_stage = 1
                            state.first_trim_reference = reference
                            atr = pattern.iloc[decision].atr14
                            state.first_trim_atr = dec(atr) if positive(atr) else None
                            state.last_trim_execution = target
                        else:
                            state.trim_stage = 1 if policy_id == S1 else 2
                            state.last_trim_execution = target
                        if authority == "FROZEN_RISK_TACTICAL_REDUCTION":
                            state.pending_risk = False
                        if authority == "TWO_CLOSE_TREND_TACTICAL_REDUCTION":
                            state.pending_trend = False
                _update_floor_gap(state)
                event.update({
                    "trim_stage_after": state.trim_stage,
                    "full_units_anchor": float(state.full_units_anchor),
                    "core_floor_units": float(state.core_floor_units),
                    "floor_gap_units": float(state.account.units - state.core_floor_units),
                })
            fills.append(event)

        if r0_value is not None:
            if not attempted_r0 or (timing_fill is not None and timing_fill.get("status") == "FILLED"):
                state.r0_edge = r0_value
        if r6_value is not None:
            if not attempted_r6 or (timing_fill is not None and timing_fill.get("status") == "FILLED"):
                state.r6_edge = r6_value
        effective_floor = _to_floor_fraction(
            state,
            symbol=symbol,
            execution_factor=execution_factor,
        ) is None
        if not guard_now and effective_floor:
            state.pending_risk = False
        if state.below_count == 0 and effective_floor:
            state.pending_trend = False

        mark_value = adjusted.close.iloc[target]
        stale = bool(bar["is_suspended"] and not positive(mark_value))
        if positive(mark_value):
            last_mark = dec(mark_value)
        mark = last_mark if positive(mark_value) or stale else None
        values = {"timing": state.account.wealth(mark), "hold": state.hold.wealth(mark)}
        row: dict[str, Any] = {
            "ordinal": target,
            "stale_mark": stale,
            "valuation_status": "KNOWN" if all(item is not None for item in values.values()) else "UNKNOWN",
            "trim_stage": state.trim_stage,
            "floor_gap_units": float(state.account.units - state.core_floor_units),
        }
        for role, account in (("timing", state.account), ("hold", state.hold)):
            nav = values[role]
            row[f"{role}_nav"] = float(nav) if nav is not None else np.nan
            row[f"{role}_exposure"] = (
                float(account.units * mark / nav)
                if account.units and mark is not None and nav
                else (0.0 if nav is not None else np.nan)
            )
        if not state.account.bought_once:
            exposure_states["PRE_ENTRY"] += 1
        elif state.trim_stage == 0:
            exposure_states["FULL"] += 1
        elif _removable_units(state) <= ZERO:
            exposure_states["CORE_FLOOR"] += 1
        else:
            exposure_states["PARTIAL_TRIM"] += 1
        rows.append(row)

    detail = {
        "symbol": symbol,
        "policy_id": policy_id,
        "status": "REPLAYED",
        "enrollment_ordinal": enrollment_ordinal,
        "counts": dict(counts),
        "exposure_states": dict(exposure_states),
        "core_ratio": float(CORE_RATIOS[policy_id]),
        "floor_violation_count": 0,
        "min_floor_gap_units": float(state.min_floor_gap) if state.min_floor_gap is not None else None,
        "terminal": {
            "timing": _terminal(state.account, final_bar=raw_rows[-1], mark=last_mark),
            "hold": _terminal(state.hold, final_bar=raw_rows[-1], mark=last_mark),
        },
    }
    return pd.DataFrame(rows), fills, detail


def replay_core_tactical_policies(
    symbol: str,
    bars: pd.DataFrame,
    *,
    enrollment_ordinal: int,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    adjusted, pattern, trend, ready = build_research_features(symbol, bars)
    return replay_core_tactical_policies_from_features(
        symbol,
        bars,
        enrollment_ordinal=enrollment_ordinal,
        adjusted=adjusted,
        pattern=pattern,
        trend=trend,
        ready=ready,
    )


def replay_core_tactical_policies_from_features(
    symbol: str,
    bars: pd.DataFrame,
    *,
    enrollment_ordinal: int,
    adjusted: pd.DataFrame,
    pattern: pd.DataFrame,
    trend: pd.DataFrame,
    ready: np.ndarray,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    day_frames: list[pd.DataFrame] = []
    fills: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    for policy_id in POLICY_IDS:
        frame, events, detail = _replay_one(
            symbol,
            bars,
            policy_id=policy_id,
            enrollment_ordinal=enrollment_ordinal,
            adjusted=adjusted,
            pattern=pattern,
            trend=trend,
            ready=ready,
        )
        day_frames.append(frame.assign(symbol=symbol, policy_id=policy_id))
        fills.extend(events)
        details.append(detail)
    return pd.concat(day_frames, ignore_index=True), pd.DataFrame(fills), details


__all__ = [
    "CORE_RATIOS",
    "POLICY_CONTRACT",
    "POLICY_CONTRACT_SHA256",
    "POLICY_IDS",
    "S1",
    "S2",
    "replay_core_tactical_policies",
    "replay_core_tactical_policies_from_features",
]
