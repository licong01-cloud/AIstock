"""Frozen PT-NEXT-021 strategy set and causal independent-account replay."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd

from backend.services.trading_core.exit_guard import (
    ExitGuardContext,
    evaluate as evaluate_exit,
)
from backend.services.trading_core.price_guard import PriceGuardPolicy

from .contracts import canonical_sha256
from .pattern_close_cash_replay import Account, CAPITAL, dec, execute, fee, positive
from .pattern_strategy import (
    BreakoutEvent,
    PATTERN_FEATURE_COLUMNS,
    acceleration_volume_exit,
    advance_entry_event,
    breakout_observed,
    pattern_feature_frame,
)
from .pattern_universe_benchmark import NO_ACCOUNT_ACTIONS, _qlib_adjusted_bars
from .policy import PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1, frozen_exit_guard_policy


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    family: str
    entry_reference: str
    omit_open_gap_veto: bool
    entry_rule: str
    exit_rule: str
    profit_basis: str
    acceleration_sell_fraction: str

    @property
    def identity(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "family": self.family,
            "entry_reference": self.entry_reference,
            "omit_open_gap_veto": self.omit_open_gap_veto,
            "entry_rule": self.entry_rule,
            "exit_rule": self.exit_rule,
            "profit_basis": self.profit_basis,
            "acceleration_sell_fraction": self.acceleration_sell_fraction,
        }


STRATEGIES = (
    StrategySpec("A0", "ENTRY_POLICY_2X2", "BREAKOUT_CLOSE", False, "R0_PULLBACK", "R0_ACCELERATION", "ACCOUNT_ABOVE_INITIAL_CAPITAL", "1"),
    StrategySpec("A1", "ENTRY_POLICY_2X2", "CONFIRMATION_CLOSE", False, "R0_PULLBACK", "R0_ACCELERATION", "ACCOUNT_ABOVE_INITIAL_CAPITAL", "1"),
    StrategySpec("A2", "ENTRY_POLICY_2X2", "BREAKOUT_CLOSE", True, "R0_PULLBACK", "R0_ACCELERATION", "ACCOUNT_ABOVE_INITIAL_CAPITAL", "1"),
    StrategySpec("A3", "ENTRY_POLICY_2X2", "CONFIRMATION_CLOSE", True, "R0_PULLBACK", "R0_ACCELERATION", "ACCOUNT_ABOVE_INITIAL_CAPITAL", "1"),
    StrategySpec("B0", "TREND", "DECISION_CLOSE", True, "RISING_SMA60", "TWO_CLOSES_BELOW_SMA60_MINUS_ATR20", "NONE", "0"),
    StrategySpec("B1", "TREND", "DECISION_CLOSE", True, "PRIOR_60D_HIGH_BREAKOUT", "NON_DECREASING_3ATR20_TRAIL", "NONE", "0"),
    StrategySpec("B2", "TREND", "DECISION_CLOSE", True, "SMA20_PULLBACK_RECOVERY", "TWO_CLOSES_BELOW_SMA60_MINUS_ATR20", "NONE", "0"),
    StrategySpec("C0", "PROFIT_EXIT", "CONFIRMATION_CLOSE", True, "R0_PULLBACK", "R0_ACCELERATION", "CURRENT_TRADE_NET_PROFIT", "1"),
    StrategySpec("C1", "PROFIT_EXIT", "CONFIRMATION_CLOSE", True, "R0_PULLBACK", "R0_ACCELERATION", "CURRENT_TRADE_NET_PROFIT", "0.5"),
    StrategySpec("C2", "PROFIT_EXIT", "CONFIRMATION_CLOSE", True, "R0_PULLBACK", "RISK_ONLY", "NONE", "0"),
)
STRATEGY_BY_ID = {item.strategy_id: item for item in STRATEGIES}
STRATEGY_SET_SHA256 = canonical_sha256([item.identity for item in STRATEGIES])
START_MODES = ("OWN_START", "COMMON_START")


def _no_open_gap_policy() -> PriceGuardPolicy:
    payload = dict(PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1)
    buy = dict(payload["buy"])
    # The real open gap remains in the context and audit.  Only this explicit
    # research policy makes both opening-gap thresholds non-binding for a
    # T+1 CLOSE fill; chase and near-limit rules are unchanged.
    buy["max_open_gap_bps"] = 1_000_000_000.0
    buy["yellow_open_gap_bps"] = 1_000_000_000.0
    payload["buy"] = buy
    payload["guidance_status"] = "research_close_fill_open_gap_not_binding"
    payload["policy_sha256"] = canonical_sha256(payload)
    return PriceGuardPolicy.from_dict(payload)


NO_OPEN_GAP_POLICY = _no_open_gap_policy()


def _trend_features(pattern: pd.DataFrame) -> pd.DataFrame:
    close = pattern.adjusted_close
    high = pattern.adjusted_high
    low = pattern.adjusted_low
    prior = close.shift(1)
    true_range = pd.concat(
        [(high - low).abs(), (high - prior).abs(), (low - prior).abs()],
        axis=1,
    ).max(axis=1, skipna=False)
    return pd.DataFrame(
        {
            "close": close,
            "sma20": close.rolling(20, min_periods=20).mean(),
            "sma60": close.rolling(60, min_periods=60).mean(),
            "sma60_lag5": close.rolling(60, min_periods=60).mean().shift(5),
            "atr20": true_range.rolling(20, min_periods=20).mean(),
            "prior_high60": high.shift(1).rolling(60, min_periods=60).max(),
        },
        index=pattern.index,
    ).replace([np.inf, -np.inf], np.nan)


def _ready_mask(spec: StrategySpec, pattern: pd.DataFrame, trend: pd.DataFrame) -> np.ndarray:
    if spec.family != "TREND":
        return pattern.loc[:, PATTERN_FEATURE_COLUMNS].notna().all(axis=1).to_numpy(bool)
    names = ["close", "sma60", "sma60_lag5", "atr20"]
    if spec.strategy_id == "B1":
        names.append("prior_high60")
    current = trend.loc[:, names].notna().all(axis=1)
    if spec.strategy_id == "B2":
        current &= trend.close.shift(1).notna() & trend.sma20.notna() & trend.sma20.shift(1).notna()
    return current.to_numpy(bool)


def _first_start(ready: np.ndarray, pit: np.ndarray) -> int | None:
    candidates = np.flatnonzero(ready[:-1] & pit[:-1])
    return int(candidates[0]) if len(candidates) else None


@dataclass
class PathState:
    policy: Account = field(default_factory=Account)
    hold: Account = field(default_factory=Account)
    event: BreakoutEvent | None = None
    event_reference: Decimal | None = None
    blocked_until: int = -1
    acceleration_edge: bool = False
    pending_risk: bool = False
    below_count: int = 0
    highest_close: Decimal | None = None
    trailing_floor: Decimal | None = None
    partial_done: bool = False
    last_exit_ordinal: int | None = None


def _trade_profitable(account: Account, reference: Decimal) -> bool:
    if not account.units or account.entry is None:
        return False
    gross = account.units * reference
    return gross - fee(gross, "SELL") > account.units * account.entry


def _trend_entry(spec: StrategySpec, trend: pd.DataFrame, ordinal: int) -> bool:
    row = trend.iloc[ordinal]
    rising = bool(row.close > row.sma60 and row.sma60 > row.sma60_lag5)
    if spec.strategy_id == "B0":
        return rising
    if spec.strategy_id == "B1":
        return bool(rising and row.close > row.prior_high60)
    previous = trend.iloc[ordinal - 1]
    return bool(
        rising
        and previous.close <= previous.sma20
        and row.close > row.sma20
        and row.close > previous.close
    )


def _trend_exit(state: PathState, spec: StrategySpec, trend: pd.DataFrame, ordinal: int) -> bool:
    row = trend.iloc[ordinal]
    if spec.strategy_id == "B1":
        close = dec(row.close)
        atr = dec(row.atr20)
        state.highest_close = close if state.highest_close is None else max(state.highest_close, close)
        candidate = state.highest_close - Decimal(3) * atr
        state.trailing_floor = candidate if state.trailing_floor is None else max(state.trailing_floor, candidate)
        return close < state.trailing_floor
    below = bool(row.close < row.sma60 - row.atr20)
    state.below_count = state.below_count + 1 if below else 0
    return state.below_count >= 2


def _post_exit_returns(adjusted: pd.DataFrame, execution: int) -> dict[str, float | None]:
    base = adjusted.close.iloc[execution]
    result: dict[str, float | None] = {}
    for horizon in (5, 20, 60):
        terminal = execution + horizon
        value = adjusted.close.iloc[terminal] if terminal < len(adjusted) else np.nan
        result[f"post_exit_return_{horizon}d"] = (
            float(value / base - 1) if positive(base) and positive(value) else None
        )
    return result


def _replay_path(
    *,
    symbol: str,
    bars: pd.DataFrame,
    adjusted: pd.DataFrame,
    pattern: pd.DataFrame,
    trend: pd.DataFrame,
    ready: np.ndarray,
    spec: StrategySpec,
    start: int,
    start_mode: str,
) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    raw_rows = bars.to_dict("records")
    pit = bars.pit_active.to_numpy(bool)
    state = PathState()
    counts: Counter[str] = Counter()
    rows: list[dict[str, Any]] = [
        {
            "ordinal": start,
            "timing_nav": float(CAPITAL),
            "hold_nav": float(CAPITAL),
            "timing_exposure": 0.0,
            "hold_exposure": 0.0,
            "stale_mark": False,
            "valuation_status": "CASH_ANCHOR",
        }
    ]
    fills: list[dict[str, Any]] = []
    last_mark: Decimal | None = None
    for decision in range(start, len(bars) - 1):
        target = decision + 1
        bar = raw_rows[target]
        price_value = adjusted.close.iloc[decision]
        reference = dec(price_value) if positive(price_value) else last_mark
        plan: str | None = None
        plan_reference = reference
        sell_fraction = Decimal(1)
        authority = "HOLD" if state.policy.units else "WAIT"
        if state.policy.units:
            state.event = None
            state.event_reference = None
            if reference is not None and positive(price_value):
                risk = evaluate_exit(
                    ExitGuardContext(
                        actual_entry_cost=float(state.policy.entry),
                        current_price=float(reference),
                        days_since_entry=decision - state.policy.bought_on,
                        t1_eligible=True,
                        suspend_status="ACTIVE",
                        price_basis="raw",
                    ),
                    frozen_exit_guard_policy(),
                )
                state.pending_risk |= risk.should_exit
            if state.pending_risk:
                plan, authority = "SELL", "FROZEN_RISK_EXIT"
            elif ready[decision]:
                if spec.family == "TREND":
                    if _trend_exit(state, spec, trend, decision):
                        plan, authority = "SELL", spec.exit_rule
                elif spec.exit_rule != "RISK_ONLY":
                    matched, _ = acceleration_volume_exit(pattern, decision, "R0")
                    if spec.profit_basis == "ACCOUNT_ABOVE_INITIAL_CAPITAL":
                        wealth = state.policy.wealth(reference)
                        eligible = matched and wealth is not None and wealth > CAPITAL
                    else:
                        eligible = matched and reference is not None and _trade_profitable(state.policy, reference)
                    if eligible and not state.acceleration_edge:
                        plan, authority = "SELL", "ACCELERATION_VOLUME_EXIT"
                        sell_fraction = Decimal(spec.acceleration_sell_fraction)
                    state.acceleration_edge = eligible
            elif spec.strategy_id in {"B0", "B2"}:
                # A missing/suspended global session breaks a consecutive-day
                # observation.  Keeping the old count would silently compress
                # two separated breaches into the frozen two-close rule.
                state.below_count = 0
        else:
            state.acceleration_edge = False
            state.below_count = 0
            state.highest_close = None
            state.trailing_floor = None
            state.partial_done = False
            if spec.family == "TREND":
                if ready[decision] and pit[decision] and _trend_entry(spec, trend, decision):
                    plan, authority = "BUY", spec.entry_rule
            else:
                if state.event is not None:
                    transition = advance_entry_event(pattern, state.event, decision)
                    counts[transition.state] += 1
                    if transition.state == "PULLBACK_CONFIRMED" and pit[decision]:
                        plan, authority = "BUY", "PULLBACK_CONFIRMED"
                        if spec.entry_reference == "BREAKOUT_CLOSE":
                            plan_reference = state.event_reference
                    if transition.event is None:
                        state.blocked_until = max(
                            state.blocked_until,
                            state.event.breakout_ordinal + 5,
                        )
                    state.event = transition.event
                if (
                    state.event is None
                    and plan is None
                    and decision >= state.blocked_until
                    and pit[decision]
                    and ready[decision]
                    and breakout_observed(pattern, decision)
                ):
                    state.event = BreakoutEvent(decision, "R0")
                    state.event_reference = reference
                    counts["BREAKOUT_OBSERVED"] += 1

        planned = {
            "timing": plan,
            "hold": "BUY" if not state.hold.bought_once and pit[decision] else None,
        }
        for role, side in planned.items():
            if side is None:
                continue
            account = state.policy if role == "timing" else state.hold
            execution_reference = plan_reference if role == "timing" else reference
            if execution_reference is None:
                fill = {"side": side, "status": "DECISION_REFERENCE_UNKNOWN"}
            else:
                fraction = sell_fraction
                if spec.strategy_id == "C1" and side == "SELL" and authority == "ACCELERATION_VOLUME_EXIT":
                    if state.partial_done:
                        continue
                    fraction = Decimal("0.5")
                fill = execute(
                    account,
                    symbol=symbol,
                    bar=bar,
                    ordinal=target,
                    side=side,
                    reference=execution_reference,
                    guarded=role == "timing",
                    risk=role == "timing" and state.pending_risk,
                    sell_fraction=fraction,
                    price_guard_policy=(
                        NO_OPEN_GAP_POLICY
                        if role == "timing" and spec.omit_open_gap_veto
                        else None
                    ),
                )
            counts[f"{role}:{side}:{fill['status']}"] += 1
            event = {
                "symbol": symbol,
                "strategy_id": spec.strategy_id,
                "start_mode": start_mode,
                "role": role,
                "decision_ordinal": decision,
                "execution_ordinal": target,
                "authority": authority if role == "timing" else "BUY_AND_HOLD",
                "decision_reference": float(execution_reference) if execution_reference is not None else None,
                "observed_open_gap_bps": (
                    float((dec(bar["open"]) / (execution_reference / dec(bar["factor"])) - 1) * 10000)
                    if execution_reference is not None and positive(bar["open"]) and positive(bar["factor"])
                    else None
                ),
                **fill,
            }
            if side == "SELL" and fill["status"] == "FILLED":
                event.update(_post_exit_returns(adjusted, target))
            if side == "BUY" and fill["status"] == "FILLED":
                event["sessions_since_last_exit"] = (
                    target - state.last_exit_ordinal
                    if state.last_exit_ordinal is not None
                    else None
                )
            fills.append(event)
            if role == "timing" and fill["status"] == "FILLED":
                if side == "BUY":
                    state.highest_close = dec(adjusted.close.iloc[target]) if positive(adjusted.close.iloc[target]) else None
                elif account.units:
                    state.partial_done = True
                else:
                    state.pending_risk = False
                    state.acceleration_edge = False
                    state.last_exit_ordinal = target

        mark_value = adjusted.close.iloc[target]
        stale = bool(bar["is_suspended"] and not positive(mark_value))
        if positive(mark_value):
            last_mark = dec(mark_value)
            mark = last_mark
        elif stale:
            mark = last_mark
        else:
            mark = None
        accounts = {"timing": state.policy, "hold": state.hold}
        values = {name: account.wealth(mark) for name, account in accounts.items()}
        row: dict[str, Any] = {
            "ordinal": target,
            "stale_mark": stale,
            "valuation_status": (
                "UNKNOWN" if any(value is None for value in values.values()) else "KNOWN"
            ),
        }
        for role, account in accounts.items():
            value = values[role]
            row[f"{role}_nav"] = float(value) if value is not None else np.nan
            row[f"{role}_exposure"] = (
                float(account.units * mark / value)
                if account.units and value
                else (0.0 if value is not None else np.nan)
            )
        rows.append(row)

    terminal: dict[str, Any] = {}
    for role, account in {"timing": state.policy, "hold": state.hold}.items():
        status = "CASH" if not account.units else "HELD_AT_END"
        terminal_nav = rows[-1][f"{role}_nav"]
        terminal[role] = {
            "status": status,
            "fees_cny": float(account.fees),
            # Diagnostics are immutable canonical JSON.  Unknown valuation is
            # a typed null, never a non-standard NaN token.
            "mtm_nav_cny": float(terminal_nav) if np.isfinite(terminal_nav) else None,
        }
    detail = {
        "symbol": symbol,
        "strategy_id": spec.strategy_id,
        "start_mode": start_mode,
        "status": "REPLAYED",
        "start_ordinal": start,
        "counts": dict(counts),
        "terminal": terminal,
    }
    return pd.DataFrame(rows), fills, detail


def replay_strategy_set(
    symbol: str,
    bars: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    adjusted = _qlib_adjusted_bars(bars, symbol=symbol)
    pattern = pattern_feature_frame(
        adjusted,
        symbol=symbol,
        corporate_actions=NO_ACCOUNT_ACTIONS,
    )
    trend = _trend_features(pattern)
    pit = bars.pit_active.to_numpy(bool)
    ready = {spec.strategy_id: _ready_mask(spec, pattern, trend) for spec in STRATEGIES}
    starts = {
        spec.strategy_id: _first_start(ready[spec.strategy_id], pit)
        for spec in STRATEGIES
    }
    available = [value for value in starts.values() if value is not None]
    common = max(available) if len(available) == len(STRATEGIES) else None
    day_frames: list[pd.DataFrame] = []
    fills: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    for spec in STRATEGIES:
        for mode, start in (("OWN_START", starts[spec.strategy_id]), ("COMMON_START", common)):
            if start is None:
                details.append(
                    {
                        "symbol": symbol,
                        "strategy_id": spec.strategy_id,
                        "start_mode": mode,
                        "status": "NO_FEATURE_READY_PIT_SESSION",
                        "start_ordinal": None,
                        "counts": {},
                    }
                )
                continue
            frame, events, detail = _replay_path(
                symbol=symbol,
                bars=bars,
                adjusted=adjusted,
                pattern=pattern,
                trend=trend,
                ready=ready[spec.strategy_id],
                spec=spec,
                start=start,
                start_mode=mode,
            )
            day_frames.append(
                frame.assign(
                    symbol=symbol,
                    strategy_id=spec.strategy_id,
                    start_mode=mode,
                )
            )
            fills.extend(events)
            details.append(detail)
    days = pd.concat(day_frames, ignore_index=True) if day_frames else pd.DataFrame()
    return days, pd.DataFrame(fills), details
