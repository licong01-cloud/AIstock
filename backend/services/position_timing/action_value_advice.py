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
    ActionPlan, ActionValueError, CORE_INFORMATION_BLOCK,
    PositionState, action_candidates, choose_action, cutoff_on, feature_contract,
    market_features, money, policy_sha256_for, risk_exit_plan, state_features,
)
from .action_value_model import HEADS, LocalActionModel
from .contracts import canonical_sha256


DIRECTION_REFERENCE_NOTIONAL_CNY = Decimal(100000)
FULL_MODEL_ACTION_AUTHORITY = "FULL_ACTION_VALUE_V4"
ENTRY_ONLY_MODEL_ACTION_AUTHORITY = "ENTRY_ONLY_MODEL_WITH_FROZEN_RISK_EXIT_V1"
OPEN_ONLY_MODEL_ACTION_AUTHORITY = "OPEN_ONLY_MODEL_WITH_FROZEN_RISK_EXIT_V1"
ENTRY_ONLY_MODEL_ACTION_CONTRACT = {
    "policy_id": ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    "risk_exit_priority": "FROZEN_RULE_RISK_OVERRIDE",
    "model_allowed_directions": ("OPEN", "ADD"),
    "model_minimum_net_action_value_bps": 0.0,
    "existing_holding_without_risk_exit_or_positive_entry": "HOLD",
    "cash_without_positive_entry": "WAIT",
}
OPEN_ONLY_MODEL_ACTION_CONTRACT = {
    "policy_id": OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    "risk_exit_priority": "FROZEN_RULE_RISK_OVERRIDE",
    "model_allowed_directions": ("OPEN",),
    "model_state_support": "CASH_ONLY_ENTRY_HEAD",
    "model_minimum_net_action_value_bps": 0.0,
    "existing_holding_without_risk_exit": "HOLD",
    "cash_without_positive_open": "WAIT",
}


def _restricted_action_contract(model_action_authority: str) -> dict[str, Any] | None:
    if model_action_authority == ENTRY_ONLY_MODEL_ACTION_AUTHORITY:
        return ENTRY_ONLY_MODEL_ACTION_CONTRACT
    if model_action_authority == OPEN_ONLY_MODEL_ACTION_AUTHORITY:
        return OPEN_ONLY_MODEL_ACTION_CONTRACT
    return None


def action_authority_policy_sha256(
    information_block: str, model_action_authority: str
) -> str:
    base = policy_sha256_for(information_block)
    if model_action_authority == FULL_MODEL_ACTION_AUTHORITY:
        return base
    contract = _restricted_action_contract(model_action_authority)
    if contract is None:
        raise ActionValueError("MODEL_ACTION_AUTHORITY_UNSUPPORTED")
    return canonical_sha256(
        {
            "base_policy_sha256": base,
            "model_action_contract": contract,
        }
    )


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
                      delist_risk: bool = False,
                      target_state: PositionState | None = None,
                      target_reference: Decimal | None = None,
                      current_market: pd.Series | None = None,
                      information_block: str = CORE_INFORMATION_BLOCK,
                      model_action_authority: str = FULL_MODEL_ACTION_AUTHORITY,
                      ) -> DailyActionDecision:
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
    planning_state = target_state or state
    planning_reference = target_reference if target_reference is not None else price
    if planning_reference <= 0:
        raise ActionValueError("TARGET_RAW_REFERENCE_INVALID")
    if not Decimal(0) <= max_exposure <= 1:
        raise ActionValueError("ACTION_BUDGET_INVALID")
    market_feature_names, feature_order, _ = feature_contract(information_block)
    policy_sha256 = action_authority_policy_sha256(
        information_block, model_action_authority
    )
    if delist_risk and not planning_state.quantity:
        plan = ActionPlan(symbol, 0, planning_reference)
        return DailyActionDecision(
            symbol,
            decision_as_of,
            "WAIT",
            plan,
            "FROZEN_RULE_RISK_OVERRIDE",
            None,
            policy_sha256,
            (),
            ("TERMINAL_LISTING_BUY_BLOCKED",),
        )
    risk = risk_exit_plan(symbol, state, price, delisted=delist_risk)
    if risk is not None:
        target_delta = -planning_state.sellable
        translated = (
            ActionPlan(symbol, target_delta, planning_reference, True)
            if target_delta
            else ActionPlan(symbol, 0, planning_reference)
        )
        return DailyActionDecision(symbol, decision_as_of, action_name(planning_state, translated.delta), translated,
                                   "FROZEN_RULE_RISK_OVERRIDE", None, policy_sha256, (), ("RISK_EXIT_OVERRIDE",))
    if model is None:
        raise ActionValueError("MODEL_UNAVAILABLE_RULE_FALLBACK")
    if model.metadata.get("information_block", CORE_INFORMATION_BLOCK) != information_block:
        raise ActionValueError("MODEL_INFORMATION_BLOCK_MISMATCH")
    if current_market is not None:
        try:
            current_market_date = pd.Timestamp(current_market.name).date()
        except (TypeError, ValueError, OverflowError) as exc:
            raise ActionValueError("CURRENT_CORE_FEATURE_DATE_INVALID") from exc
        if current_market_date != decision_as_of.date():
            raise ActionValueError("CURRENT_CORE_FEATURE_DATE_MISMATCH")
    current = (
        current_market
        if current_market is not None
        else market_features(bars, benchmark, information_block=information_block).iloc[-1]
    )
    if not set(market_feature_names).issubset(current.index):
        code = (
            "CURRENT_CORE_FEATURE_SCHEMA_INVALID"
            if information_block == CORE_INFORMATION_BLOCK
            else "CURRENT_OPTIONAL_FEATURE_SCHEMA_INVALID"
        )
        raise ActionValueError(code, information_block=information_block)
    current = current.loc[list(market_feature_names)]
    if current.isna().any():
        code = (
            "CURRENT_CORE_FEATURE_UNAVAILABLE"
            if information_block == CORE_INFORMATION_BLOCK
            else "CURRENT_OPTIONAL_FEATURE_UNAVAILABLE"
        )
        raise ActionValueError(
            code,
            information_block=information_block,
            features=[name for name in market_feature_names if pd.isna(current[name])],
        )
    plans = action_candidates(symbol, planning_state, planning_reference, max_exposure=max_exposure)
    open_only_has_cash_state = (
        model_action_authority != OPEN_ONLY_MODEL_ACTION_AUTHORITY
        or planning_state.quantity == 0
    )
    actionable = [
        plan
        for plan in plans
        if plan.delta
        and (
            model_action_authority == FULL_MODEL_ACTION_AUTHORITY
            or (plan.delta > 0 and open_only_has_cash_state)
        )
    ]
    values = {0: 0.0}
    if actionable:
        frame = pd.DataFrame([{**current.to_dict(), **state_features(planning_state, plan)} for plan in actionable],
                             columns=feature_order)
        objectives = [HEADS[0] if plan.delta > 0 else HEADS[1] for plan in actionable]
        predictions = model.predict(frame, objectives, decision_as_of=decision_as_of)
        values.update({plan.delta: float(value) for plan, value in zip(actionable, predictions)})
    else:
        # Validate model identity/time even when no executable quantity exists.
        model.predict(pd.DataFrame(columns=feature_order), [], decision_as_of=decision_as_of)
    eligible_plans = [
        plan
        for plan in plans
        if (
            model_action_authority == FULL_MODEL_ACTION_AUTHORITY
            or (
                plan.delta >= 0
                and (
                    model_action_authority != OPEN_ONLY_MODEL_ACTION_AUTHORITY
                    or planning_state.quantity == 0
                    or plan.delta == 0
                )
            )
        )
    ]
    selected = choose_action(
        eligible_plans, [values[plan.delta] for plan in eligible_plans]
    )
    candidates = tuple({"action": action_name(planning_state, plan.delta),
                        "planned_delta_qty": plan.delta, "estimated_net_action_value_bps": values[plan.delta],
                        "objective": HEADS[0] if plan.delta > 0 else HEADS[1] if plan.delta < 0 else "NO_ACTION"}
                       for plan in eligible_plans)
    reason_codes = ["MODEL_ESTIMATE_NOT_STOCK_CONFIDENCE"]
    if model_action_authority == ENTRY_ONLY_MODEL_ACTION_AUTHORITY:
        reason_codes.append("MODEL_EXIT_AUTHORITY_REMOVED")
    elif model_action_authority == OPEN_ONLY_MODEL_ACTION_AUTHORITY:
        reason_codes.extend(("MODEL_EXIT_AUTHORITY_REMOVED", "MODEL_ADD_AUTHORITY_REMOVED"))
    return DailyActionDecision(symbol, decision_as_of, action_name(planning_state, selected.delta), selected,
                               "LOCAL_MODEL_ESTIMATE", model.metadata["model_sha256"], policy_sha256, candidates,
                               tuple(reason_codes))


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
