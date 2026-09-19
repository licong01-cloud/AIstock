"""Hindsight-only diagnostics and mature labels for PT-NEXT-024.

Policy modules must never import this file.  Its outputs are evidence and
supervised labels with explicit availability times, not decision features.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .causal_timing_contracts import (
    FEATURE_ORDER, LABEL_HORIZON_SESSIONS, ORACLE_BUYBACK_MAX_SESSIONS,
    ORACLE_GRID_SESSIONS,
)
from .causal_timing_execution import affordable_for_budget, daily_quote, execute_quote
from .contracts import canonical_sha256
from .pattern_close_cash_replay import Account, CAPITAL, ZERO, dec, fee, positive


def _clone_account(value: Account) -> Account:
    return Account(
        value.cash, value.units, value.entry, value.bought_on,
        value.fees, value.bought_once,
    )


def _action_value(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame,
    decision_ordinal: int, sell_execution_ordinal: int, sold_units: Decimal,
    sell_notional: Decimal, sell_fee: Decimal, recovery_delay: int,
) -> dict[str, Any]:
    horizon = decision_ordinal + LABEL_HORIZON_SESSIONS
    if horizon >= len(bars):
        return {"status": "LABEL_IMMATURE"}
    if not positive(adjusted.close.iloc[horizon]):
        return {"status": "LABEL_TERMINAL_UNAVAILABLE"}
    cash = sell_notional - sell_fee
    if cash < ZERO or sold_units <= ZERO:
        raise ActionValueError("CAUSAL_LABEL_SELL_ECONOMICS_INVALID")
    account = Account(cash=cash, units=ZERO, entry=None, bought_on=sell_execution_ordinal, bought_once=True)
    buy_status = "NOT_ATTEMPTED"
    buy_execution: int | None = None
    due = sell_execution_ordinal + recovery_delay
    for ordinal in range(due, horizon + 1):
        bar = bars.iloc[ordinal].to_dict()
        quote = daily_quote(bar, trade_date=bars.index[ordinal].date())
        fill = execute_quote(account, symbol=symbol, quote=quote, ordinal=ordinal, side="BUY")
        buy_status = fill["status"]
        if fill["status"] == "FILLED":
            buy_execution = ordinal
            break
    mark = dec(adjusted.close.iloc[horizon])
    action_wealth = account.wealth(mark)
    hold_wealth = sold_units * mark
    if action_wealth is None:
        return {"status": "LABEL_TERMINAL_UNAVAILABLE"}
    delta = action_wealth - hold_wealth
    return {
        "status": "MATURED",
        "buy_status": buy_status,
        "buy_execution_ordinal": buy_execution,
        "buy_execution_date": str(bars.index[buy_execution].date()) if buy_execution is not None else None,
        "label_window_end": str(bars.index[horizon].date()),
        "label_available_at": str(bars.index[horizon].date()),
        "action_wealth_cny": float(action_wealth),
        "hold_wealth_cny": float(hold_wealth),
        "label_net_action_value_bps": float(delta / CAPITAL * Decimal(10000)),
    }


def _standardized_label(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame,
    decision_ordinal: int, sell_execution_ordinal: int,
) -> dict[str, Any]:
    horizon = decision_ordinal + LABEL_HORIZON_SESSIONS
    if horizon >= len(bars) or not positive(adjusted.close.iloc[horizon]):
        return {"status": "LABEL_IMMATURE"}
    decision_bar = bars.iloc[decision_ordinal]
    if not positive(decision_bar.close) or not positive(decision_bar.factor):
        return {"status": "LABEL_INITIAL_STATE_UNAVAILABLE"}
    raw, factor = dec(decision_bar.close), dec(decision_bar.factor)
    quantity = affordable_for_budget(symbol, CAPITAL, raw, CAPITAL)
    if quantity <= 0:
        return {"status": "LABEL_INITIAL_QUANTITY_UNAVAILABLE"}
    units = Decimal(quantity) / factor
    initial_notional = raw * quantity
    initial_fee = fee(initial_notional, "BUY")
    residual = CAPITAL - initial_notional - initial_fee
    adjusted_entry = (initial_notional + initial_fee) / units
    action = Account(
        cash=residual, units=units, entry=adjusted_entry,
        bought_on=decision_ordinal - 1, fees=initial_fee, bought_once=True,
    )
    hold = Account(
        cash=residual, units=units, entry=adjusted_entry,
        bought_on=decision_ordinal - 1, fees=initial_fee, bought_once=True,
    )
    sell = execute_quote(
        action, symbol=symbol,
        quote=daily_quote(bars.iloc[sell_execution_ordinal].to_dict(),
                          trade_date=bars.index[sell_execution_ordinal].date()),
        ordinal=sell_execution_ordinal, side="SELL", sell_fraction=Decimal("0.20"),
    )
    buy_status = "NOT_APPLICABLE"
    buy_execution: int | None = None
    if sell["status"] == "FILLED":
        due = sell_execution_ordinal + 5
        for ordinal in range(due, horizon + 1):
            fill = execute_quote(
                action, symbol=symbol,
                quote=daily_quote(bars.iloc[ordinal].to_dict(), trade_date=bars.index[ordinal].date()),
                ordinal=ordinal, side="BUY",
            )
            buy_status = fill["status"]
            if fill["status"] == "FILLED":
                buy_execution = ordinal
                break
    mark = dec(adjusted.close.iloc[horizon])
    action_wealth, hold_wealth = action.wealth(mark), hold.wealth(mark)
    if action_wealth is None or hold_wealth is None:
        return {"status": "LABEL_TERMINAL_UNAVAILABLE"}
    return {
        "status": "MATURED", "sell_status": sell["status"], "buy_status": buy_status,
        "buy_execution_ordinal": buy_execution,
        "buy_execution_date": str(bars.index[buy_execution].date()) if buy_execution is not None else None,
        "label_window_end": str(bars.index[horizon].date()),
        "label_available_at": str(bars.index[horizon].date()),
        "action_wealth_cny": float(action_wealth), "hold_wealth_cny": float(hold_wealth),
        "label_net_action_value_bps": float((action_wealth - hold_wealth) / CAPITAL * Decimal(10000)),
        "standardized_initial_quantity": quantity,
        "standardized_initial_buy_fee": float(initial_fee),
        "standardized_initial_residual_cash": float(residual),
    }


def labels_from_legacy_fills(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame,
    market_features: pd.DataFrame, fills: pd.DataFrame,
) -> pd.DataFrame:
    """Build fixed-five-session labels from actually filled legacy S1 sells."""
    if fills.empty:
        return pd.DataFrame(columns=(
            "symbol", "decision_date", "label_available_at", "label_net_action_value_bps", *FEATURE_ORDER,
        ))
    selected = fills.loc[
        fills.role.eq("timing") & fills.side.eq("SELL")
        & ~fills.authority.eq("TERMINAL_LIQUIDATION")
    ].sort_values(["decision_ordinal", "execution_ordinal"], kind="stable")
    rows: list[dict[str, Any]] = []
    for item in selected.itertuples():
        decision = int(item.decision_ordinal)
        features = market_features.iloc[decision]
        if not np.isfinite(features.loc[list(FEATURE_ORDER)].to_numpy(float)).all():
            continue
        label = _standardized_label(
            symbol=symbol, bars=bars, adjusted=adjusted, decision_ordinal=decision,
            sell_execution_ordinal=int(item.execution_ordinal),
        )
        if label["status"] != "MATURED":
            continue
        rows.append({
            "symbol": symbol, "decision_ordinal": decision,
            "decision_date": str(bars.index[decision].date()),
            "reference_authority": item.authority,
            "reference_fill_status": item.status,
            "reference_sell_execution_ordinal": int(item.execution_ordinal),
            "reference_fill_sha256": canonical_sha256({
                "symbol": symbol, "policy_id": item.policy_id,
                "authority": item.authority, "status": item.status,
                "decision_ordinal": decision,
                "execution_ordinal": int(item.execution_ordinal),
            }),
            **{name: float(features[name]) for name in FEATURE_ORDER}, **label,
        })
    return pd.DataFrame(rows)


def event_oracle_from_legacy_fills(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame, fills: pd.DataFrame,
) -> pd.DataFrame:
    """Enumerate one-to-twenty-session buybacks without stitching events."""
    if fills.empty:
        return pd.DataFrame()
    selected = fills.loc[
        fills.role.eq("timing") & fills.side.eq("SELL") & fills.status.eq("FILLED")
    ].sort_values(["decision_ordinal", "execution_ordinal"], kind="stable")
    rows: list[dict[str, Any]] = []
    for event_number, item in enumerate(selected.itertuples()):
        alternatives: list[dict[str, Any]] = []
        for delay in range(1, ORACLE_BUYBACK_MAX_SESSIONS + 1):
            value = _action_value(
                symbol=symbol, bars=bars, adjusted=adjusted,
                decision_ordinal=int(item.decision_ordinal),
                sell_execution_ordinal=int(item.execution_ordinal),
                sold_units=dec(item.virtual_units), sell_notional=dec(item.notional),
                sell_fee=dec(item.fee), recovery_delay=delay,
            )
            if value.get("status") == "MATURED":
                alternatives.append({"delay": delay, **value})
        if not alternatives:
            rows.append({
                "symbol": symbol, "event_id": f"{symbol}:{event_number}",
                "decision_ordinal": int(item.decision_ordinal), "status": "NO_MATURED_ALTERNATIVE",
            })
            continue
        # No action has zero incremental bps and is always an explicit alternative.
        best = max([{"delay": None, "label_net_action_value_bps": 0.0}] + alternatives,
                   key=lambda value: float(value["label_net_action_value_bps"]))
        fixed = next((value for value in alternatives if value["delay"] == 5), None)
        rows.append({
            "symbol": symbol, "event_id": f"{symbol}:{event_number}",
            "decision_ordinal": int(item.decision_ordinal),
            "decision_date": str(bars.index[int(item.decision_ordinal)].date()),
            "status": "MATURED", "best_delay_sessions": best["delay"],
            "oracle_net_action_value_bps": float(best["label_net_action_value_bps"]),
            "fixed5_net_action_value_bps": (
                float(fixed["label_net_action_value_bps"]) if fixed is not None else None
            ),
            "alternative_count": len(alternatives) + 1,
        })
    return pd.DataFrame(rows)


def restricted_account_oracle(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame,
    enrollment_ordinal: int, terminal_ordinal: int,
) -> dict[str, Any]:
    """Exact best path inside one frozen sell/one frozen buyback grid."""
    if enrollment_ordinal + 1 >= terminal_ordinal:
        return {"status": "NO_FEASIBLE_PATH", "path_count": 0}
    initial = Account()
    entry_ordinal = enrollment_ordinal + 1
    entry = execute_quote(
        initial, symbol=symbol,
        quote=daily_quote(bars.iloc[entry_ordinal].to_dict(), trade_date=bars.index[entry_ordinal].date()),
        ordinal=entry_ordinal, side="BUY",
    )
    if entry["status"] != "FILLED":
        return {"status": "INITIAL_ENTRY_UNAVAILABLE", "path_count": 0, "entry_status": entry["status"]}
    marks = pd.to_numeric(adjusted.close.iloc[entry_ordinal:terminal_ordinal + 1], errors="coerce").to_numpy(float)
    if not np.isfinite(marks).all() or np.any(marks <= 0):
        return {"status": "VALUATION_UNAVAILABLE", "path_count": 0}

    def evaluate(account: Account, sell_at: int | None, buy_at: int | None) -> dict[str, Any] | None:
        current = Account(account.cash, account.units, account.entry, account.bought_on, account.fees, True)
        # Each path has at most two ordinary state transitions.  Materialize
        # those states once and value whole segments with NumPy; repeatedly
        # scanning every session for every grid pair is exactly equivalent but
        # prohibitively expensive for the full PIT universe.
        states: list[tuple[int, Account]] = [(0, Account(
            current.cash, current.units, current.entry, current.bought_on,
            current.fees, current.bought_once,
        ))]
        sell_status = buy_status = "NOT_APPLICABLE"
        if sell_at is not None:
            fill = execute_quote(
                current, symbol=symbol,
                quote=daily_quote(bars.iloc[sell_at].to_dict(), trade_date=bars.index[sell_at].date()),
                ordinal=sell_at, side="SELL", sell_fraction=Decimal("0.20"),
            )
            sell_status = fill["status"]
            if sell_status != "FILLED":
                return None
            states.append((sell_at - entry_ordinal, _clone_account(current)))
        if buy_at is not None:
            fill = execute_quote(
                current, symbol=symbol,
                quote=daily_quote(bars.iloc[buy_at].to_dict(), trade_date=bars.index[buy_at].date()),
                ordinal=buy_at, side="BUY",
            )
            buy_status = fill["status"]
            if buy_status != "FILLED":
                return None
            states.append((buy_at - entry_ordinal, _clone_account(current)))
        path = np.empty(len(marks), dtype=float)
        for index, (start, state) in enumerate(states):
            stop = states[index + 1][0] if index + 1 < len(states) else len(marks)
            path[start:stop] = float(state.cash) + float(state.units) * marks[start:stop]
        if current.units > ZERO:
            execute_quote(
                current, symbol=symbol,
                quote=daily_quote(
                    bars.iloc[terminal_ordinal].to_dict(),
                    trade_date=bars.index[terminal_ordinal].date(),
                ),
                ordinal=terminal_ordinal, side="SELL", sell_fraction=Decimal(1),
            )
        terminal_wealth = current.wealth(dec(marks[-1]))
        if terminal_wealth is None:
            return None
        path[-1] = float(terminal_wealth)
        # The 10m cash grant is part of the frozen account path.  Starting the
        # drawdown series after the entry fee would erase that first loss.
        array = np.r_[float(CAPITAL), path]
        drawdown = float(np.min(array / np.maximum.accumulate(array) - 1.0))
        return {
            "terminal_nav_cny": float(array[-1]), "max_drawdown": drawdown,
            "sell_execution_ordinal": sell_at, "buy_execution_ordinal": buy_at,
            "sell_status": sell_status, "buy_status": buy_status,
        }

    paths: list[dict[str, Any]] = []
    bh = evaluate(initial, None, None)
    if bh is not None:
        paths.append({"path_id": "BH", **bh})
    grid = list(range(entry_ordinal + ORACLE_GRID_SESSIONS, terminal_ordinal, ORACLE_GRID_SESSIONS))
    for sell_at in grid:
        sell_only = evaluate(initial, sell_at, None)
        if sell_only is not None:
            paths.append({"path_id": f"S{sell_at}", **sell_only})
        for buy_at in grid:
            if buy_at <= sell_at:
                continue
            value = evaluate(initial, sell_at, buy_at)
            if value is not None:
                paths.append({"path_id": f"S{sell_at}B{buy_at}", **value})
    if not paths:
        return {"status": "NO_FEASIBLE_PATH", "path_count": 0}
    best = max(paths, key=lambda item: item["terminal_nav_cny"])
    constrained: dict[str, Any] = {}
    for limit in (0.20, 0.30):
        eligible = [item for item in paths if abs(min(0.0, item["max_drawdown"])) <= limit]
        constrained[f"mdd_{int(limit * 100)}"] = (
            max(eligible, key=lambda item: item["terminal_nav_cny"]) if eligible else "NO_FEASIBLE_PATH"
        )
    result = {
        "status": "COMPLETE", "path_count": len(paths), "grid_ordinals": grid,
        "best_terminal": best, "constrained": constrained,
        "bh": next((item for item in paths if item["path_id"] == "BH"), None),
    }
    return {**result, "oracle_sha256": canonical_sha256(result)}
