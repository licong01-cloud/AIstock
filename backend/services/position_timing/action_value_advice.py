"""One pure stock/day decision for offline replay and local inference.

This module neither publishes official cards nor produces orders. Experimental
classification and serving publication belong to the existing service boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd

from .action_value import (
    ActionPlan, ActionValueError, FEATURE_ORDER, MARKET_FEATURES, POLICY_SHA256,
    PositionState, action_candidates, choose_action, cutoff_on, market_features,
    money, risk_exit_plan, state_features,
)
from .action_value_model import HEADS, LocalActionModel


DIRECTION_REFERENCE_NOTIONAL_CNY = Decimal(100000)


@dataclass(frozen=True)
class DailyActionDecision:
    symbol: str
    decision_as_of: datetime
    action: str
    plan: ActionPlan
    authority: str
    model_sha256: str | None
    policy_sha256: str
    candidates: tuple[dict[str, Any], ...]
    reason_codes: tuple[str, ...]


def action_name(state: PositionState, delta: int) -> str:
    if delta > 0:
        return "OPEN" if state.quantity == 0 else "ADD"
    if delta < 0:
        return "EXIT" if -delta == state.quantity else "REDUCE"
    return "HOLD" if state.quantity else "WAIT"


def decide_stock_day(*, symbol: str, state: PositionState, bars: pd.DataFrame,
                     benchmark: pd.Series, decision_as_of: datetime,
                     model: LocalActionModel | None, max_exposure: Decimal = Decimal(1),
                     delist_risk: bool = False) -> DailyActionDecision:
    """Do not substitute zero predictions, invent entry cost, or read future bars.

    Caller validates snapshot source/PIT identities. Current required core gaps
    raise a typed error; the service can then expose explicit L1 fallback. Risk
    exits retain the frozen rule priority and need no model/core imputation.
    """
    if (decision_as_of.tzinfo is None or decision_as_of != cutoff_on(decision_as_of.date())
            or bars.empty or bars.index[-1].date() != decision_as_of.date()
            or bars.index.max().date() > decision_as_of.date()):
        raise ActionValueError("DECISION_CLOCK_OR_CURRENT_BAR_INVALID")
    price = money(bars.iloc[-1]["close"])
    if price <= 0:
        raise ActionValueError("CURRENT_RAW_PRICE_INVALID")
    if not Decimal(0) <= max_exposure <= 1:
        raise ActionValueError("ACTION_BUDGET_INVALID")
    risk = risk_exit_plan(symbol, state, price, delisted=delist_risk)
    if risk is not None:
        return DailyActionDecision(symbol, decision_as_of, action_name(state, risk.delta), risk,
                                   "FROZEN_RULE_RISK_OVERRIDE", None, POLICY_SHA256, (), ("RISK_EXIT_OVERRIDE",))
    if model is None:
        raise ActionValueError("MODEL_UNAVAILABLE_RULE_FALLBACK")
    current = market_features(bars, benchmark).iloc[-1]
    if current.isna().any():
        raise ActionValueError("CURRENT_CORE_FEATURE_UNAVAILABLE",
                               features=[name for name in MARKET_FEATURES if pd.isna(current[name])])
    plans = action_candidates(symbol, state, price, max_exposure=max_exposure)
    actionable = [plan for plan in plans if plan.delta]
    values = {0: 0.0}
    if actionable:
        frame = pd.DataFrame([{**current.to_dict(), **state_features(state, plan)} for plan in actionable],
                             columns=FEATURE_ORDER)
        objectives = [HEADS[0] if plan.delta > 0 else HEADS[1] for plan in actionable]
        predictions = model.predict(frame, objectives, decision_as_of=decision_as_of)
        values.update({plan.delta: float(value) for plan, value in zip(actionable, predictions)})
    else:
        # Validate model identity/time even when no executable quantity exists.
        model.predict(pd.DataFrame(columns=FEATURE_ORDER), [], decision_as_of=decision_as_of)
    selected = choose_action(plans, [values[plan.delta] for plan in plans])
    candidates = tuple({"action": action_name(state, plan.delta),
                        "planned_delta_qty": plan.delta, "estimated_net_action_value_bps": values[plan.delta],
                        "objective": HEADS[0] if plan.delta > 0 else HEADS[1] if plan.delta < 0 else "NO_ACTION"}
                       for plan in plans)
    return DailyActionDecision(symbol, decision_as_of, action_name(state, selected.delta), selected,
                               "LOCAL_MODEL_ESTIMATE", model.metadata["model_sha256"], POLICY_SHA256, candidates,
                               ("MODEL_ESTIMATE_NOT_STOCK_CONFIDENCE",))


def public_advice(decision: DailyActionDecision, *, direction_only: bool) -> dict[str, Any]:
    """Reference scenario quantities never escape as personalized instructions."""
    return {
        "symbol": decision.symbol, "decision_as_of": decision.decision_as_of.isoformat(),
        "action": decision.action, "authority": decision.authority,
        "model_sha256": decision.model_sha256, "policy_sha256": decision.policy_sha256,
        "sizing_status": "DIRECTION_ONLY" if direction_only else "PERSONALIZED_QUANTITY_ESTIMATE",
        "planned_delta_qty": None if direction_only else decision.plan.delta,
        "reference_notional_cny": str(DIRECTION_REFERENCE_NOTIONAL_CNY) if direction_only else None,
        "reason_codes": list(decision.reason_codes) + (["SIZING_INPUT_UNAVAILABLE", "REFERENCE_NOT_USER_BUDGET"] if direction_only else []),
        "candidate_action_values": [
            {
                "action": item["action"],
                "objective": item["objective"],
                "estimated_net_action_value_bps": item["estimated_net_action_value_bps"],
                "planned_delta_qty": None if direction_only else item["planned_delta_qty"],
            }
            for item in decision.candidates
        ],
        # This is research advice, not an independently executable alert/card.
        "executable_alert": False,
    }
