"""Independent Qlib virtual-unit cash accounts; no broker/account action model."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd

from backend.execution_algos.board_lot import round_to_board_lot
from backend.services.trading_core.exit_guard import ExitGuardContext, evaluate as evaluate_exit
from backend.services.trading_core.price_guard import PriceGuardContext, evaluate as evaluate_price

from .action_value import ActionValueError
from .contracts import TriggerSide
from .pattern_strategy import (
    BreakoutEvent, PATTERN_FEATURE_COLUMNS, acceleration_volume_exit,
    advance_entry_event, breakout_observed, pattern_feature_frame,
)
from .pattern_universe_benchmark import NO_ACCOUNT_ACTIONS, _qlib_adjusted_bars
from .policy import component_cost_for_parent_notionals, frozen_exit_guard_policy, frozen_price_guard_policy

CAPITAL = Decimal("10000000")
ZERO = Decimal(0)


def dec(value: Any) -> Decimal:
    return Decimal(str(value))


def positive(value: Any) -> bool:
    return bool(np.isfinite(value) and value > 0)


def fee(notional: Decimal, side: str) -> Decimal:
    return component_cost_for_parent_notionals(
        side=TriggerSide(side), notionals=(notional,),
    )["total"]


def affordable_quantity(symbol: str, cash: Decimal, price: Decimal) -> int:
    """Monotone search in raw legal quantities, including commission floor."""
    if price <= 0 or cash < 0:
        raise ActionValueError("CLOSE_CASH_BUDGET_INVALID")
    low, high = 0, int(cash / price)
    while low < high:
        mid = (low + high + 1) // 2
        qty = round_to_board_lot(mid, symbol, side="BUY")
        cost = price * qty
        if not qty or cost + fee(cost, "BUY") <= cash:
            low = mid
        else:
            high = mid - 1
    return round_to_board_lot(low, symbol, side="BUY")


def execution_status(bar: dict[str, Any], side: str) -> str:
    if bar["is_suspended"]:
        return "SUSPENDED"
    if not all(positive(bar[k]) for k in ("open", "close", "factor")):
        return "MARKET_DATA_UNKNOWN"
    up, down, price = bar["up_limit"], bar["down_limit"], bar["close"]
    # The frozen r5 adapter has no authoritative no-limit flag. Missing bounds
    # therefore cannot be silently interpreted as an IPO no-limit session.
    if not positive(up) or not positive(down) or down >= up:
        return "LIMIT_DATA_UNKNOWN"
    if price > up + .005 or price < down - .005:
        return "LIMIT_PRICE_INCONSISTENT"
    if (side == "BUY" and price >= up - .005) or (side == "SELL" and price <= down + .005):
        return "DIRECTIONAL_LIMIT_BLOCKED"
    return "EXECUTABLE"


@dataclass
class Account:
    cash: Decimal = CAPITAL
    units: Decimal = ZERO
    entry: Decimal | None = None
    bought_on: int = -1
    fees: Decimal = ZERO
    bought_once: bool = False

    def wealth(self, adjusted_close: Decimal | None) -> Decimal | None:
        if not self.units:
            return self.cash
        return None if adjusted_close is None else self.cash + self.units * adjusted_close


def execute(
    account: Account, *, symbol: str, bar: dict[str, Any], ordinal: int,
    side: str, reference: Decimal, guarded: bool, risk: bool = False,
) -> dict[str, Any]:
    result: dict[str, Any] = {"side": side, "status": execution_status(bar, side)}
    if result["status"] != "EXECUTABLE":
        return result
    if side == "SELL" and (not account.units or ordinal <= account.bought_on):
        return {**result, "status": "T1_NOT_SELLABLE"}
    factor, raw = dec(bar["factor"]), dec(bar["close"])
    scale = Decimal(1)
    if guarded and not risk:
        # Map the fixed adjusted decision reference to execution-day raw basis.
        ref = reference / factor
        decision = evaluate_price(PriceGuardContext(
            signal_ref_price=float(ref), prev_close=float(ref), current_price=float(raw),
            open_gap_bps=float((dec(bar["open"]) / ref - 1) * 10000),
            current_gap_bps=float((raw / ref - 1) * 10000),
            limit_up=float(bar["up_limit"]), limit_down=float(bar["down_limit"]),
            side=side.lower(), sell_reason="rebalance" if side == "SELL" else None,
            price_basis="raw",
        ), frozen_price_guard_policy())
        result["guard_reason"] = decision.reason_code
        if decision.action not in {"ACCEPT", "REDUCE", "SELL"}:
            return {**result, "status": "PRICE_GUARD_BLOCKED"}
        if decision.action == "REDUCE":
            scale = dec(decision.size_multiplier)
    if side == "BUY":
        qty = affordable_quantity(symbol, account.cash, raw)
        qty = round_to_board_lot(int(qty * scale), symbol, side="BUY")
        if not qty:
            return {**result, "status": "CASH_BELOW_MINIMUM_LOT"}
        notional = raw * qty
        charged = fee(notional, side)
        if notional + charged > account.cash:
            raise ActionValueError("CLOSE_CASH_OVERSPEND")
        units = dec(qty) / factor
        account.entry = (account.units * (account.entry or ZERO) + notional + charged) / (account.units + units)
        account.units += units
        account.cash -= notional + charged
        account.bought_on = ordinal
        account.bought_once = True
        result["raw_quantity"] = qty
    else:
        # Complete virtual-unit liquidation is intentional, not a claim of a
        # legal broker share entitlement inferred from adjustment factors.
        units = account.units
        notional = units * raw * factor
        charged = fee(notional, side)
        if account.cash + notional < charged:
            return {**result, "status": "CASH_BELOW_SELL_FEE"}
        account.cash += notional - charged
        account.units, account.entry = ZERO, None
    account.fees += charged
    return {**result, "status": "FILLED", "notional": float(notional),
            "fee": float(charged), "virtual_units": float(units)}


def replay(symbol: str, bars: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    adjusted = _qlib_adjusted_bars(bars, symbol=symbol)
    features = pattern_feature_frame(adjusted, symbol=symbol, corporate_actions=NO_ACCOUNT_ACTIONS)
    # Necessary-condition prefilters only. The existing pure functions retain
    # final signal authority; this avoids millions of irrelevant Series reads.
    ceiling = features[["sma5", "sma10"]].max(axis=1)
    crosses = ((features.adjusted_close > ceiling) & (features.adjusted_close.shift(1) <= ceiling.shift(1))).to_numpy(bool)
    volume_possible = features.volume_ratio.ge(2.).to_numpy(bool)
    ready = features.loc[:, PATTERN_FEATURE_COLUMNS].notna().all(axis=1).to_numpy()
    candidates = np.flatnonzero(ready[:-1] & bars.pit_active.to_numpy(bool)[:-1])
    if not len(candidates):
        return pd.DataFrame(), [], {"symbol": symbol, "status": "NO_FEATURE_READY_PIT_SESSION"}
    start = int(candidates[0])
    raw_rows = bars.to_dict("records")
    policy, hold = Account(), Account()
    accounts = {"timing": policy, "hold": hold}
    event = None
    event_reference = None
    blocked_until = -1
    edge = False
    pending_risk = False
    last_mark = None
    rows, fills = [], []
    counts: Counter[str] = Counter()
    # Signal-day starting cash anchor: the first return includes entry costs.
    rows.append({"ordinal": start, "timing_nav": float(CAPITAL), "hold_nav": float(CAPITAL),
                 "timing_exposure": 0., "hold_exposure": 0., "stale_mark": False,
                 "valuation_status": "CASH_ANCHOR"})
    for decision in range(start, len(bars) - 1):
        target = decision + 1
        before, bar = raw_rows[decision], raw_rows[target]
        decision_price = adjusted.close.iloc[decision]
        reference = dec(decision_price) if positive(decision_price) else last_mark
        plan = None
        plan_reference = reference
        authority = "HOLD" if policy.units else "WAIT"
        pit = bool(before["pit_active"])
        if policy.units:
            event = None
            event_reference = None
            if reference is not None and positive(decision_price):
                # Evaluate ratio in one consistent basis through the shared
                # frozen guard; no new guard thresholds or account actions.
                guard = evaluate_exit(ExitGuardContext(
                    actual_entry_cost=float(policy.entry), current_price=float(reference),
                    days_since_entry=decision - policy.bought_on,
                    t1_eligible=True, suspend_status="ACTIVE", price_basis="raw",
                ), frozen_exit_guard_policy())
                pending_risk |= guard.should_exit
            if pending_risk:
                plan, authority = "SELL", "FROZEN_RISK_EXIT"
            elif ready[decision]:
                matched = False
                if volume_possible[decision]:
                    matched, _ = acceleration_volume_exit(features, decision, "R0")
                wealth = policy.wealth(reference)
                eligible = matched and wealth is not None and wealth > CAPITAL
                if eligible and not edge:
                    plan, authority = "SELL", "ACCELERATION_VOLUME_EXIT"
                edge = eligible
        else:
            edge = False
            if event is not None:
                transition = advance_entry_event(features, event, decision)
                counts[transition.state] += 1
                if transition.state == "PULLBACK_CONFIRMED" and pit:
                    plan, authority = "BUY", "PULLBACK_CONFIRMED"
                    plan_reference = event_reference
                if transition.event is None:
                    blocked_until = max(blocked_until, event.breakout_ordinal + 5)
                event = transition.event
            if event is None and plan is None and decision >= blocked_until and pit and crosses[decision] and breakout_observed(features, decision):
                event = BreakoutEvent(decision, "R0")
                event_reference = reference
                counts["BREAKOUT_OBSERVED"] += 1
        plans = {"timing": plan, "hold": "BUY" if not hold.bought_once and pit else None}
        for role, side in plans.items():
            if side is None:
                continue
            execution_reference = plan_reference if role == "timing" else reference
            if execution_reference is None:
                fill = {"side": side, "status": "DECISION_REFERENCE_UNKNOWN"}
            else:
                fill = execute(accounts[role], symbol=symbol, bar=bar, ordinal=target,
                               side=side, reference=execution_reference, guarded=role == "timing",
                               risk=role == "timing" and pending_risk)
            counts[f"{role}:{side}:{fill['status']}"] += 1
            fills.append({"symbol": symbol, "role": role, "decision_ordinal": decision,
                          "execution_ordinal": target, "authority": authority if role == "timing" else "BUY_AND_HOLD",
                          **fill})
            if role == "timing" and side == "SELL" and fill["status"] == "FILLED":
                pending_risk, edge = False, False
        price = adjusted.close.iloc[target]
        stale = bool(bar["is_suspended"] and not positive(price))
        if positive(price):
            last_mark = dec(price)
            mark = last_mark
        elif stale:
            mark = last_mark
        else:
            mark = None
        values = {role: account.wealth(mark) for role, account in accounts.items()}
        row = {"ordinal": target, "stale_mark": stale,
               "valuation_status": "UNKNOWN" if any(v is None for v in values.values()) else "KNOWN"}
        for role, account in accounts.items():
            value = values[role]
            row[f"{role}_nav"] = float(value) if value is not None else np.nan
            row[f"{role}_exposure"] = float(account.units * mark / value) if account.units and value else (0. if value is not None else np.nan)
        rows.append(row)
    terminal = {}
    for role, account in accounts.items():
        state = execution_status(raw_rows[-1], "SELL") if account.units else "CASH"
        nav = rows[-1][f"{role}_nav"]
        sale = account.units * dec(adjusted.close.iloc[-1]) if account.units and positive(adjusted.close.iloc[-1]) else ZERO
        terminal[role] = {"status": state, "fees_cny": float(account.fees),
                          "liquidatable_nav_cny": nav - float(fee(sale, "SELL")) if state == "EXECUTABLE" and account.bought_on < len(bars)-1 else (nav if state == "CASH" else None)}
    return pd.DataFrame(rows), fills, {"symbol": symbol, "status": "REPLAYED", "start_ordinal": start,
                                     "counts": dict(counts), "terminal": terminal}
