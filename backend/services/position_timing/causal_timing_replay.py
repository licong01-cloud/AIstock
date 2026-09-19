"""Continuous independent-account replay for PT-NEXT-024."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping
import warnings

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .causal_timing_contracts import (
    BH, CYCLE5, CYCLE5_GBDT, CYCLE5_RIDGE, LEGACY_S1, LEGACY_S2,
    PASSIVE70, PASSIVE80, POLICY_IDS, RECOVERY_DELAY_SESSIONS,
    TEST_LAST_DECISION, TEST_LAST_EXECUTION,
)
from .causal_timing_execution import MinuteExecutionSource, execute_quote, quote_for_view
from .causal_timing_model import predict
from .core_tactical_timing import (
    SECOND_STAGE_ATR_MULTIPLE, S1, S2, STAGE_FRACTION,
    CorePolicyState, _event, _floor_gap, _guard_active, _reset_anchor,
    _sell_fraction, _to_floor_fraction, _trade_profitable, _trend_on,
)
from .pattern_close_cash_replay import Account, CAPITAL, ZERO, dec, positive


PASSIVE_BUDGET = {BH: Decimal(1), PASSIVE80: Decimal("0.80"), PASSIVE70: Decimal("0.70")}
MODEL_BY_POLICY = {CYCLE5_RIDGE: "ridge", CYCLE5_GBDT: "gbdt"}


@dataclass
class ReplayState:
    core: CorePolicyState
    recovery_due: int | None = None
    last_mark: Decimal | None = None
    model_rejected: int = 0
    model_unavailable: int = 0


def _clone_account(value: Account) -> Account:
    return Account(value.cash, value.units, value.entry, value.bought_on, value.fees, value.bought_once)


def _prediction_arrays(
    features: pd.DataFrame, models: Mapping[str, Mapping[str, Any]],
) -> dict[str, np.ndarray]:
    finite = np.isfinite(features.to_numpy(float)).all(axis=1)
    result: dict[str, np.ndarray] = {}
    for model_id in set(MODEL_BY_POLICY.values()):
        model = models.get(model_id)
        if model is None:
            continue
        values = np.full(len(features), np.nan, dtype=float)
        if finite.any():
            values[finite] = predict(model, features.loc[finite])
        result[model_id] = values
    return result


def _valuation(account: Account, adjusted_mark: Any, state: ReplayState) -> tuple[Decimal | None, bool]:
    if positive(adjusted_mark):
        state.last_mark = dec(adjusted_mark)
        return account.wealth(state.last_mark), False
    if state.last_mark is not None:
        return account.wealth(state.last_mark), True
    return account.wealth(None), False


def _legacy_plan(
    state: ReplayState, *, symbol: str, policy_id: str, decision: int,
    execution_factor: Decimal, reference: Decimal | None,
    pattern: pd.DataFrame, trend: pd.DataFrame, ready: np.ndarray,
) -> tuple[str | None, str, Decimal | None, bool, bool, bool | None, bool | None, bool]:
    core = state.core
    authority = "HOLD"
    sell_fraction: Decimal | None = None
    attempted_r0 = attempted_r6 = False
    r0_value: bool | None = None
    r6_value: bool | None = None
    guard_now = False
    trend_observed = bool(ready[decision])
    trend_on = trend_observed and _trend_on(trend, decision)
    below = trend_observed and bool(
        trend.iloc[decision].close < trend.iloc[decision].sma60 - trend.iloc[decision].atr20
    )
    core.below_count = core.below_count + 1 if below else 0
    if not trend_observed:
        core.below_count = 0
    core.pending_trend |= core.below_count >= 2
    if reference is not None:
        guard_now = _guard_active(core, reference, decision)
        core.pending_risk |= guard_now
    if trend_observed:
        r0_value, _ = _event(pattern, decision, "R0")
        r6_value, _ = _event(pattern, decision, "R6")
    floor_fraction = _to_floor_fraction(core, symbol=symbol, execution_factor=execution_factor)
    plan: str | None = None
    if core.pending_risk and floor_fraction is not None:
        plan, authority, sell_fraction = "SELL", "FROZEN_RISK_TACTICAL_REDUCTION", floor_fraction
    elif core.pending_trend and floor_fraction is not None:
        plan, authority, sell_fraction = "SELL", "TWO_CLOSE_TREND_TACTICAL_REDUCTION", floor_fraction
    elif policy_id == LEGACY_S1:
        eligible = bool(r0_value is True and reference is not None and _trade_profitable(core.account, reference))
        if eligible and not core.r0_edge and floor_fraction is not None:
            plan, authority, sell_fraction, attempted_r0 = "SELL", "R0_TACTICAL_20_TRIM", floor_fraction, True
    else:
        first = bool(core.trim_stage == 0 and r0_value is True and reference is not None
                     and _trade_profitable(core.account, reference))
        if first and not core.r0_edge:
            fraction = _sell_fraction(core, core.full_units_anchor * STAGE_FRACTION,
                                      symbol=symbol, execution_factor=execution_factor)
            if fraction is not None:
                plan, authority, sell_fraction, attempted_r0 = "SELL", "R0_STAGE1_15_TRIM", fraction, True
        elif (
            core.trim_stage == 1 and core.last_trim_execution is not None
            and decision >= core.last_trim_execution and r6_value is True and reference is not None
            and core.first_trim_reference is not None and core.first_trim_atr is not None
            and reference >= core.first_trim_reference + SECOND_STAGE_ATR_MULTIPLE * core.first_trim_atr
            and _trade_profitable(core.account, reference)
        ):
            fraction = _sell_fraction(core, core.full_units_anchor * STAGE_FRACTION,
                                      symbol=symbol, execution_factor=execution_factor)
            if fraction is not None:
                plan, authority, sell_fraction, attempted_r6 = "SELL", "R6_STAGE2_15_TRIM", fraction, True
    if plan is None and core.trim_stage > 0:
        recovery = bool(
            reference is not None and not core.pending_risk and not core.pending_trend and not guard_now
            and trend_on and trend.iloc[decision].close > trend.iloc[decision].sma20 and r0_value is False
        )
        if recovery and core.account.cash > ZERO:
            plan, authority = "BUY", "TACTICAL_RECOVERY"
    return plan, authority, sell_fraction, attempted_r0, attempted_r6, r0_value, r6_value, guard_now


def _cycle_candidate(
    state: ReplayState, *, symbol: str, decision: int, execution_factor: Decimal,
    reference: Decimal | None, pattern: pd.DataFrame, trend: pd.DataFrame, ready: np.ndarray,
) -> tuple[str | None, Decimal | None, bool | None, bool]:
    core = state.core
    trend_observed = bool(ready[decision])
    below = trend_observed and bool(
        trend.iloc[decision].close < trend.iloc[decision].sma60 - trend.iloc[decision].atr20
    )
    core.below_count = core.below_count + 1 if below else 0
    if not trend_observed:
        core.below_count = 0
    core.pending_trend |= core.below_count >= 2
    guard_now = False
    if reference is not None:
        guard_now = _guard_active(core, reference, decision)
        core.pending_risk |= guard_now
    r0_value: bool | None = None
    if trend_observed:
        r0_value, _ = _event(pattern, decision, "R0")
    floor_fraction = _to_floor_fraction(core, symbol=symbol, execution_factor=execution_factor)
    reason: str | None = None
    if core.pending_risk and floor_fraction is not None:
        reason = "FROZEN_RISK_TACTICAL_REDUCTION"
    elif core.pending_trend and floor_fraction is not None:
        reason = "TWO_CLOSE_TREND_TACTICAL_REDUCTION"
    elif (r0_value is True and not core.r0_edge and floor_fraction is not None
          and reference is not None and _trade_profitable(core.account, reference)):
        reason = "R0_TACTICAL_20_TRIM"
    return reason, floor_fraction, r0_value, guard_now


def replay_policy(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame, pattern: pd.DataFrame,
    trend: pd.DataFrame, ready: np.ndarray, model_features: pd.DataFrame,
    policy_id: str, execution_view: str, enrollment_ordinal: int,
    terminal_ordinal: int, minute_source: MinuteExecutionSource | None,
    models: Mapping[str, Mapping[str, Any]],
    model_predictions: Mapping[str, np.ndarray] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if policy_id not in POLICY_IDS:
        raise ActionValueError("CAUSAL_REPLAY_POLICY_UNKNOWN", policy_id=policy_id)
    raw_rows = bars.to_dict("records")
    if not (0 <= enrollment_ordinal < terminal_ordinal < len(bars)):
        raise ActionValueError("CAUSAL_REPLAY_BOUNDARY_INVALID")
    account = Account()
    core_policy = S2 if policy_id == LEGACY_S2 else S1
    state = ReplayState(CorePolicyState(account, Account()))
    rows: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    start = enrollment_ordinal
    predictions = (
        dict(model_predictions) if model_predictions is not None
        else _prediction_arrays(model_features, models)
    )

    for decision in range(start, terminal_ordinal):
        target = decision + 1
        if target > terminal_ordinal:
            break
        reference_value = adjusted.close.iloc[decision]
        reference = dec(reference_value) if positive(reference_value) else state.last_mark
        bar = raw_rows[target]
        factor = dec(bar["factor"]) if positive(bar.get("factor")) else ZERO
        plan: str | None = None
        authority = "HOLD"
        sell_fraction: Decimal | None = None
        r0_value: bool | None = None
        r6_value: bool | None = None
        attempted_r0 = attempted_r6 = False
        guard_now = False
        model_value: float | None = None
        model_status = "NOT_APPLICABLE"

        ordinary_execution_allowed = bars.index[target].date() <= TEST_LAST_EXECUTION
        if not account.bought_once:
            if ordinary_execution_allowed and decision >= start and bool(bars.pit_active.iloc[decision]):
                plan, authority = "BUY", "COMMON_INITIAL_ENTRY"
        elif ordinary_execution_allowed and policy_id in (LEGACY_S1, LEGACY_S2):
            plan, authority, sell_fraction, attempted_r0, attempted_r6, r0_value, r6_value, guard_now = _legacy_plan(
                state, symbol=symbol, policy_id=policy_id, decision=decision,
                execution_factor=factor, reference=reference, pattern=pattern, trend=trend, ready=ready,
            )
        elif ordinary_execution_allowed and policy_id in (CYCLE5, CYCLE5_RIDGE, CYCLE5_GBDT):
            if (state.recovery_due is not None and target >= state.recovery_due
                    and bars.index[target].date() <= TEST_LAST_EXECUTION and account.cash > ZERO):
                plan, authority = "BUY", "FIXED_5_SESSION_RECOVERY"
            elif state.recovery_due is None and bars.index[decision].date() <= TEST_LAST_DECISION:
                reason, sell_fraction, r0_value, _ = _cycle_candidate(
                    state, symbol=symbol, decision=decision, execution_factor=factor,
                    reference=reference, pattern=pattern, trend=trend, ready=ready,
                )
                if reason is not None and target + RECOVERY_DELAY_SESSIONS <= terminal_ordinal:
                    allowed = True
                    if policy_id in MODEL_BY_POLICY:
                        values = predictions.get(MODEL_BY_POLICY[policy_id])
                        if values is None or not np.isfinite(values[decision]):
                            allowed, model_status = False, "MODEL_UNAVAILABLE"
                            state.model_unavailable += 1
                        else:
                            model_value = float(values[decision])
                            allowed = model_value > 0.0
                            model_status = "ACCEPT" if allowed else "REJECT"
                            state.model_rejected += int(not allowed)
                    if allowed:
                        plan, authority = "SELL", reason
                    else:
                        # One causal decision per candidate episode.  Execution failures retry;
                        # model rejections consume the observed episode rather than polling it daily.
                        state.core.pending_risk = False
                        state.core.pending_trend = False
                        if r0_value is not None:
                            state.core.r0_edge = r0_value

        # Baselines buy only at the common entry. Passive budgets remain cash thereafter.
        if policy_id in PASSIVE_BUDGET and account.bought_once:
            plan = None
        if target > terminal_ordinal:
            plan = None
        if plan is not None:
            quote = quote_for_view(
                view_id=execution_view, symbol=symbol, trade_date=bars.index[target].date(),
                daily_bar=bar, minute_source=minute_source,
            )
            if authority == "COMMON_INITIAL_ENTRY":
                ratio = PASSIVE_BUDGET.get(policy_id, Decimal(1))
                budget = CAPITAL * ratio
            else:
                budget = None
            fill = execute_quote(
                account, symbol=symbol, quote=quote, ordinal=target, side=plan,
                budget=budget, sell_fraction=sell_fraction or Decimal(1),
            )
            counts[f"{plan}:{fill['status']}"] += 1
            event = {
                "symbol": symbol, "policy_id": policy_id, "execution_view": execution_view,
                "decision_ordinal": decision, "execution_ordinal": target,
                "decision_date": str(bars.index[decision].date()),
                "execution_date": str(bars.index[target].date()), "authority": authority,
                "decision_reference": float(reference) if reference is not None else None,
                "model_prediction_bps": model_value, "model_status": model_status,
                "planned_sell_fraction": float(sell_fraction) if sell_fraction is not None else None,
                **fill,
            }
            fills.append(event)
            if fill["status"] == "FILLED":
                if authority == "COMMON_INITIAL_ENTRY":
                    _reset_anchor(state.core, core_policy)
                elif plan == "SELL":
                    if policy_id in (CYCLE5, CYCLE5_RIDGE, CYCLE5_GBDT):
                        state.core.trim_stage = 1
                        state.core.last_trim_execution = target
                        state.recovery_due = target + RECOVERY_DELAY_SESSIONS
                        state.core.pending_risk = False
                        state.core.pending_trend = False
                    elif authority == "R0_STAGE1_15_TRIM":
                        state.core.trim_stage = 1
                        state.core.first_trim_reference = reference
                        atr = pattern.iloc[decision].atr14
                        state.core.first_trim_atr = dec(atr) if positive(atr) else None
                        state.core.last_trim_execution = target
                    else:
                        state.core.trim_stage = 1 if policy_id == LEGACY_S1 else 2
                        state.core.last_trim_execution = target
                        if authority == "FROZEN_RISK_TACTICAL_REDUCTION":
                            state.core.pending_risk = False
                        if authority == "TWO_CLOSE_TREND_TACTICAL_REDUCTION":
                            state.core.pending_trend = False
                elif authority in {"TACTICAL_RECOVERY", "FIXED_5_SESSION_RECOVERY"}:
                    _reset_anchor(state.core, core_policy)
                    state.recovery_due = None
            if r0_value is not None and (not attempted_r0 or fill["status"] == "FILLED"):
                state.core.r0_edge = r0_value
            if r6_value is not None and (not attempted_r6 or fill["status"] == "FILLED"):
                state.core.r6_edge = r6_value

        # Legacy edges advance on an observed non-attempt day; an execution
        # failure deliberately keeps the old edge so the same episode retries.
        if policy_id in (LEGACY_S1, LEGACY_S2):
            if r0_value is not None and not attempted_r0:
                state.core.r0_edge = r0_value
            if r6_value is not None and not attempted_r6:
                state.core.r6_edge = r6_value
            floor_reached = _to_floor_fraction(state.core, symbol=symbol, execution_factor=factor) is None
            if floor_reached and not guard_now:
                state.core.pending_risk = False
            if floor_reached and state.core.below_count == 0:
                state.core.pending_trend = False
        elif policy_id in (CYCLE5, CYCLE5_RIDGE, CYCLE5_GBDT) and r0_value is not None:
            # A filled/failed SELL keeps its edge semantics above; otherwise
            # ordinary false observations release the next rising edge.
            if plan != "SELL" or (fills and fills[-1].get("status") == "FILLED"):
                state.core.r0_edge = r0_value

        mark_value = adjusted.close.iloc[target]
        wealth, stale = _valuation(account, mark_value, state)
        rows.append({
            "symbol": symbol, "policy_id": policy_id, "execution_view": execution_view,
            "ordinal": target, "valuation_date": str(bars.index[target].date()),
            "nav": float(wealth) if wealth is not None else np.nan,
            "cash": float(account.cash), "virtual_units": float(account.units),
            "exposure": (float(account.units * state.last_mark / wealth)
                         if account.units and state.last_mark is not None and wealth else 0.0),
            "stale_mark": stale, "valuation_status": "KNOWN" if wealth is not None else "UNKNOWN",
            "trim_stage": state.core.trim_stage, "recovery_due_ordinal": state.recovery_due,
            "floor_gap_units": float(_floor_gap(state.core)) if state.core.full_units_anchor else 0.0,
        })

    # A terminal sell is submitted for every residual position under the same execution view.
    terminal_fill: dict[str, Any] | None = None
    if account.units > ZERO:
        bar = raw_rows[terminal_ordinal]
        quote = quote_for_view(
            view_id=execution_view, symbol=symbol, trade_date=bars.index[terminal_ordinal].date(),
            daily_bar=bar, minute_source=minute_source,
        )
        terminal_fill = execute_quote(
            account, symbol=symbol, quote=quote, ordinal=terminal_ordinal,
            side="SELL", sell_fraction=Decimal(1),
        )
        fills.append({
            "symbol": symbol, "policy_id": policy_id, "execution_view": execution_view,
            "decision_ordinal": terminal_ordinal - 1, "execution_ordinal": terminal_ordinal,
            "decision_date": str(bars.index[terminal_ordinal - 1].date()),
            "execution_date": str(bars.index[terminal_ordinal].date()),
            "authority": "TERMINAL_LIQUIDATION", **terminal_fill,
        })
        counts[f"TERMINAL_SELL:{terminal_fill['status']}"] += 1
        mark = state.last_mark
        wealth = account.wealth(mark)
        if rows:
            rows[-1].update({
                "nav": float(wealth) if wealth is not None else np.nan,
                "cash": float(account.cash), "virtual_units": float(account.units),
                "exposure": (float(account.units * mark / wealth)
                             if account.units and mark is not None and wealth else 0.0),
                "valuation_status": "KNOWN" if wealth is not None else "UNKNOWN",
            })
    terminal_wealth = account.wealth(state.last_mark)
    detail = {
        "symbol": symbol, "policy_id": policy_id, "execution_view": execution_view,
        "status": "REPLAYED", "enrollment_ordinal": enrollment_ordinal,
        "counts": dict(counts), "fees_cny": float(account.fees),
        "terminal_nav_cny": float(terminal_wealth) if terminal_wealth is not None else None,
        "terminal_cash_cny": float(account.cash), "terminal_virtual_units": float(account.units),
        "terminal_status": (
            "CASH" if account.units == ZERO else
            "RESIDUAL_POSITION_NOT_CLEARED" if terminal_wealth is not None else "VALUATION_UNAVAILABLE"
        ),
        "terminal_fill_status": terminal_fill.get("status") if terminal_fill else "NO_POSITION",
        "model_rejected_count": state.model_rejected,
        "model_unavailable_count": state.model_unavailable,
    }
    return pd.DataFrame(rows), pd.DataFrame(fills), detail


def replay_symbol(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame, pattern: pd.DataFrame,
    trend: pd.DataFrame, ready: np.ndarray, model_features: pd.DataFrame,
    enrollment_ordinal: int, terminal_ordinal: int,
    minute_source: MinuteExecutionSource | None, models: Mapping[str, Mapping[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    days: list[pd.DataFrame] = []
    fills: list[pd.DataFrame] = []
    details: list[dict[str, Any]] = []
    model_predictions = _prediction_arrays(model_features, models)
    for view_id in ("DAILY_CLOSE", "MINUTE_CLOSE_PROXY", "SCHEDULED_1000_PROXY"):
        for policy_id in POLICY_IDS:
            frame, events, detail = replay_policy(
                symbol=symbol, bars=bars, adjusted=adjusted, pattern=pattern, trend=trend,
                ready=ready, model_features=model_features, policy_id=policy_id,
                execution_view=view_id, enrollment_ordinal=enrollment_ordinal,
                terminal_ordinal=terminal_ordinal, minute_source=minute_source, models=models,
                model_predictions=model_predictions,
            )
            days.append(frame)
            if not events.empty:
                fills.append(events)
            details.append(detail)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        day_frame = pd.concat(days, ignore_index=True)
        fill_frame = pd.concat(fills, ignore_index=True) if fills else pd.DataFrame()
    return day_frame, fill_frame, details
