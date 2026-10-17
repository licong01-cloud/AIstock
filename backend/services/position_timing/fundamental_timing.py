"""Frozen T1/T2 policies for the PT-NEXT-022 independent-account replay."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd

from backend.services.trading_core.exit_guard import ExitGuardContext, evaluate as evaluate_exit

from .action_value import ActionValueError
from .contracts import canonical_sha256
from .pattern_close_cash_replay import Account, CAPITAL, dec, execute, execution_status, fee, positive
from .pattern_strategy import PATTERN_FEATURE_COLUMNS, acceleration_volume_exit, pattern_feature_frame
from .pattern_strategy_evolution import NO_OPEN_GAP_POLICY, _trend_features
from .pattern_universe_benchmark import NO_ACCOUNT_ACTIONS, _qlib_adjusted_bars
from .policy import frozen_exit_guard_policy


T1 = "TREND_CONTINUATION_REENTRY_V1"
T2 = "CORE_70_TACTICAL_30_RECOVERY_V1"
POLICY_IDS = (T1, T2)
TACTICAL_SELL_FRACTION = Decimal("0.30")

POLICY_CONTRACT = {
    "schema": "position_timing_fundamental_policy_set_v1",
    "policies": list(POLICY_IDS),
    "decision": "T_CLOSE",
    "execution": "T_PLUS_1_CLOSE",
    "trend": "C_GT_SMA60_AND_SMA60_GT_SMA60_LAG5",
    "trend_exit": "TWO_CONSECUTIVE_GLOBAL_SESSIONS_C_LT_SMA60_MINUS_ATR20",
    "tactical_sell_fraction": str(TACTICAL_SELL_FRACTION),
    "tactical_event": "R0_ACCELERATION_VOLUME_FALSE_TO_TRUE_AND_CURRENT_TRADE_NET_PROFIT",
    "recovery": "EVENT_FALSE_AND_TREND_TRUE_AND_C_GT_SMA20",
    "buy_guard": "NO_OPEN_GAP_POLICY",
    "risk_guard": "FROZEN_RULE_DEFAULT",
    "account": "INDEPENDENT_10M_CASH_NATURAL_REINVESTMENT",
}
POLICY_CONTRACT_SHA256 = canonical_sha256(POLICY_CONTRACT)


@dataclass
class PolicyState:
    account: Account
    hold: Account
    pending_risk: bool = False
    below_count: int = 0
    tactical_state: str = "ARMED"
    eligible_edge: bool = False
    last_trim_execution: int | None = None


def common_feature_ready(pattern: pd.DataFrame, trend: pd.DataFrame) -> np.ndarray:
    pattern_ready = pattern.loc[:, PATTERN_FEATURE_COLUMNS].notna().all(axis=1)
    trend_ready = trend[["close", "sma20", "sma60", "sma60_lag5", "atr20"]].notna().all(axis=1)
    return (pattern_ready & trend_ready).to_numpy(bool)


def build_research_features(symbol: str, bars: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, np.ndarray]:
    adjusted = _qlib_adjusted_bars(bars, symbol=symbol)
    pattern = pattern_feature_frame(adjusted, symbol=symbol, corporate_actions=NO_ACCOUNT_ACTIONS)
    trend = _trend_features(pattern)
    return adjusted, pattern, trend, common_feature_ready(pattern, trend)


def _trade_profitable(account: Account, reference: Decimal) -> bool:
    if not account.units or account.entry is None:
        return False
    gross = account.units * reference
    return gross - fee(gross, "SELL") > account.units * account.entry


def _trend_on(trend: pd.DataFrame, ordinal: int) -> bool:
    row = trend.iloc[ordinal]
    return bool(row.close > row.sma60 and row.sma60 > row.sma60_lag5)


def _terminal(account: Account, *, final_bar: dict[str, Any], mark: Decimal | None) -> dict[str, Any]:
    mtm = account.wealth(mark)
    if not account.units:
        liquidatable = account.cash
        liquidation_status = "CASH"
    elif mark is None:
        liquidatable = None
        liquidation_status = "VALUATION_UNKNOWN"
    else:
        status = execution_status(final_bar, "SELL")
        if status == "EXECUTABLE":
            notional = account.units * mark
            liquidatable = account.cash + notional - fee(notional, "SELL")
            liquidation_status = "LIQUIDATABLE_AT_FINAL_CLOSE"
        else:
            liquidatable = None
            liquidation_status = status
    return {
        "status": "CASH" if not account.units else "HELD_AT_END",
        "mtm_nav_cny": float(mtm) if mtm is not None else None,
        "liquidatable_nav_cny": float(liquidatable) if liquidatable is not None else None,
        "liquidation_status": liquidation_status,
        "fees_cny": float(account.fees),
    }


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
        raise ActionValueError("FUNDAMENTAL_POLICY_UNKNOWN", policy_id=policy_id)
    if enrollment_ordinal < 0 or enrollment_ordinal >= len(bars) - 1:
        raise ActionValueError("FUNDAMENTAL_ENROLLMENT_ORDINAL_INVALID")
    raw_rows = bars.to_dict("records")
    pit = bars.pit_active.to_numpy(bool)
    state = PolicyState(Account(), Account())
    rows: list[dict[str, Any]] = [{
        "ordinal": enrollment_ordinal,
        "timing_nav": float(CAPITAL),
        "hold_nav": float(CAPITAL),
        "timing_exposure": 0.0,
        "hold_exposure": 0.0,
        "valuation_status": "CASH_ANCHOR",
        "stale_mark": False,
    }]
    fills: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    last_mark: Decimal | None = None

    for decision in range(enrollment_ordinal, len(bars) - 1):
        target = decision + 1
        bar = raw_rows[target]
        value = adjusted.close.iloc[decision]
        reference = dec(value) if positive(value) else last_mark
        plan: str | None = None
        authority = "HOLD" if state.account.units else "WAIT"
        sell_fraction = Decimal(1)

        if state.account.units:
            if reference is not None and positive(value):
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
                state.pending_risk |= guard.should_exit
            trend_observed = bool(ready[decision])
            trend_on = trend_observed and _trend_on(trend, decision)
            below = trend_observed and bool(
                trend.iloc[decision].close < trend.iloc[decision].sma60 - trend.iloc[decision].atr20
            )
            state.below_count = state.below_count + 1 if below else 0
            if not trend_observed:
                state.below_count = 0
            if state.pending_risk:
                plan, authority = "SELL", "FROZEN_RISK_EXIT"
            elif state.below_count >= 2:
                plan, authority = "SELL", "TWO_CLOSE_TREND_EXIT"
            elif policy_id == T2:
                eligible: bool | None = None
                if trend_observed:
                    matched, _ = acceleration_volume_exit(pattern, decision, "R0")
                    eligible = bool(matched and reference is not None and _trade_profitable(state.account, reference))
                if state.tactical_state == "ARMED":
                    if eligible is True and not state.eligible_edge:
                        plan, authority = "SELL", "R0_ACCELERATION_TACTICAL_TRIM"
                        sell_fraction = TACTICAL_SELL_FRACTION
                    if eligible is not None:
                        state.eligible_edge = eligible
                elif state.tactical_state == "TRIMMED":
                    if eligible is False and trend_on and bool(trend.iloc[decision].close > trend.iloc[decision].sma20):
                        if state.last_trim_execution is not None and decision >= state.last_trim_execution:
                            plan, authority = "BUY", "TACTICAL_RECOVERY"
                    if eligible is not None:
                        state.eligible_edge = eligible
        else:
            state.below_count = 0
            state.eligible_edge = False
            state.tactical_state = "ARMED"
            if ready[decision] and pit[decision] and _trend_on(trend, decision):
                plan, authority = "BUY", "TREND_CONTINUATION_ENTRY"

        actions = {
            "timing": plan,
            "hold": "BUY" if not state.hold.bought_once and pit[decision] else None,
        }
        for role, side in actions.items():
            if side is None:
                continue
            account = state.account if role == "timing" else state.hold
            if reference is None:
                fill = {"side": side, "status": "DECISION_REFERENCE_UNKNOWN"}
            else:
                fill = execute(
                    account,
                    symbol=symbol,
                    bar=bar,
                    ordinal=target,
                    side=side,
                    reference=reference,
                    guarded=role == "timing",
                    risk=role == "timing" and state.pending_risk,
                    sell_fraction=sell_fraction if role == "timing" else Decimal(1),
                    price_guard_policy=NO_OPEN_GAP_POLICY if role == "timing" else None,
                )
            counts[f"{role}:{side}:{fill['status']}"] += 1
            fills.append({
                "symbol": symbol,
                "policy_id": policy_id,
                "role": role,
                "decision_ordinal": decision,
                "execution_ordinal": target,
                "authority": authority if role == "timing" else "BUY_AND_HOLD",
                "decision_reference": float(reference) if reference is not None else None,
                **fill,
            })
            if role == "timing" and fill["status"] == "FILLED":
                if side == "SELL" and state.account.units:
                    state.tactical_state = "TRIMMED"
                    state.last_trim_execution = target
                elif side == "SELL":
                    state.pending_risk = False
                    state.below_count = 0
                    state.tactical_state = "ARMED"
                    state.eligible_edge = False
                    state.last_trim_execution = None
                elif authority == "TACTICAL_RECOVERY":
                    state.tactical_state = "ARMED"
                    state.eligible_edge = False

        mark_value = adjusted.close.iloc[target]
        stale = bool(bar["is_suspended"] and not positive(mark_value))
        if positive(mark_value):
            last_mark = dec(mark_value)
        mark = last_mark if positive(mark_value) or stale else None
        values = {"timing": state.account.wealth(mark), "hold": state.hold.wealth(mark)}
        row: dict[str, Any] = {
            "ordinal": target,
            "stale_mark": stale,
            "valuation_status": "KNOWN" if all(v is not None for v in values.values()) else "UNKNOWN",
        }
        for role, account in (("timing", state.account), ("hold", state.hold)):
            nav = values[role]
            row[f"{role}_nav"] = float(nav) if nav is not None else np.nan
            row[f"{role}_exposure"] = (
                float(account.units * mark / nav) if account.units and mark is not None and nav else
                (0.0 if nav is not None else np.nan)
            )
        rows.append(row)

    detail = {
        "symbol": symbol,
        "policy_id": policy_id,
        "status": "REPLAYED",
        "enrollment_ordinal": enrollment_ordinal,
        "counts": dict(counts),
        "terminal": {
            "timing": _terminal(state.account, final_bar=raw_rows[-1], mark=last_mark),
            "hold": _terminal(state.hold, final_bar=raw_rows[-1], mark=last_mark),
        },
    }
    return pd.DataFrame(rows), fills, detail


def replay_fundamental_policies(
    symbol: str,
    bars: pd.DataFrame,
    *,
    enrollment_ordinal: int,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    adjusted, pattern, trend, ready = build_research_features(symbol, bars)
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
