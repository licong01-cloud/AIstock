"""Immutable historical replay for PT-NEXT-018 pattern timing research.

This is an offline, timing-owned research pipeline.  It deliberately has no
API, scheduler, database write, registry/current pointer, card, alert, order,
or serving integration.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import (
    ActionPlan,
    ActionValueError,
    Fill,
    PositionState,
    TZ,
    action_candidates,
    apply_fill,
    cutoff_on,
    daily_fill,
    money,
    risk_exit_plan,
    state_features,
)
from .action_value_corporate_actions import (
    CorporateAction,
    CorporateActionBook,
    apply_corporate_action_with_audit,
)
from .action_value_data import DailyCandidate, file_reference
from .action_value_pipeline import _clean_repository_commit
from .action_value_research import (
    REFERENCE_CAPITAL_CNY,
    _apply_actions_until,
    _available_raw_close,
    _has_unbound_material_factor_change,
    _mark_to_market,
    _project_target_state,
    _roll_state_to_decision,
)
from .action_value_suspensions import SuspensionSnapshotBook
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256
from .pattern_strategy import (
    BreakoutEvent,
    PATTERN_TEMPLATE_SET_SHA256,
    TEMPLATE_BY_ID,
    acceleration_volume_exit,
    advance_entry_event,
    breakout_observed,
    pattern_feature_frame,
)
from .policy import COST_POLICY_SHA256, round_to_board_lot


PIPELINE_ID = "POSITION_TIMING_PATTERN_STRATEGY_V1"
ARTIFACT_FOLDER = "pattern_strategy_v1"
REQUEST_SCHEMA = "position_timing_pattern_strategy_request_v1"
RECEIPT_SCHEMA = "position_timing_pattern_strategy_receipt_v1"
BUNDLE_SCHEMA = "position_timing_pattern_strategy_bundle_v1"
POPULATION_SEED_TEXT = "20260911"
EVALUATION_SYMBOL_LIMIT = 64
INITIAL_TRAINING_SESSIONS = 756
PRIMARY_HORIZON = 20
TERMINAL_MAX_DEFER = 5
BOOTSTRAP_SAMPLES = 5000
BLOCK_SESSIONS = 25
INFERENCE_SEED = 20260911
PROTOTYPE_FAMILY_SIZE = 4
PROTOTYPE_CONFIDENCE_LEVEL = 1.0 - 0.05 / PROTOTYPE_FAMILY_SIZE
EVOLUTION_FAMILY_SIZE = 5
PREREGISTERED_FAMILY_COUNT = 2
TOTAL_FORMAL_COMPARISON_COUNT = PROTOTYPE_FAMILY_SIZE + EVOLUTION_FAMILY_SIZE
RESULT_CLASS = "EXPLORATORY_USER_PROPOSED_HYPOTHESIS_CROSS_SYMBOL_NOT_TEMPORAL_HOLDOUT"

PROTOTYPE_CONTRACT: Mapping[str, Any] = {
    "schema_version": "position_timing_pattern_prototype_contract_v1",
    "template_id": "R0",
    "comparisons": ("E1_MINUS_E0", "X1_MINUS_X0", "P_MINUS_BUY_AND_HOLD", "P_MINUS_FROZEN_L1"),
    "familywise_hypothesis_count": PROTOTYPE_FAMILY_SIZE,
    "economic_threshold_bps": 0.0,
    "horizon_trading_days": PRIMARY_HORIZON,
    "terminal_max_defer_trading_days": TERMINAL_MAX_DEFER,
    "initial_training_sessions": INITIAL_TRAINING_SESSIONS,
    "event_aggregation": "ANCHOR_DATE_EQUAL_WEIGHT_AFTER_SYMBOL_EVENT_MEAN",
    "policy_aggregation": "TRADING_DATE_FIXED_POPULATION_MEAN",
    "bootstrap": {
        "method": "CIRCULAR_MOVING_BLOCK",
        "block_sessions": BLOCK_SESSIONS,
        "samples": BOOTSTRAP_SAMPLES,
        "seed": INFERENCE_SEED,
        "simultaneous_confidence_level": PROTOTYPE_CONFIDENCE_LEVEL,
    },
    "cost_policy_sha256": COST_POLICY_SHA256,
    "cost_sensitivity_scenarios": (
        {"scenario_id": "PARENT_ORDERS_1", "parent_count": 1, "additional_friction_bps_per_leg": "0"},
        {"scenario_id": "PARENT_ORDERS_2", "parent_count": 2, "additional_friction_bps_per_leg": "0"},
        {"scenario_id": "PARENT_ORDERS_3", "parent_count": 3, "additional_friction_bps_per_leg": "0"},
        {"scenario_id": "FRICTION_5_BPS_PER_LEG", "parent_count": 1, "additional_friction_bps_per_leg": "5"},
        {"scenario_id": "FRICTION_10_BPS_PER_LEG", "parent_count": 1, "additional_friction_bps_per_leg": "10"},
    ),
    "pattern_template_set_sha256": PATTERN_TEMPLATE_SET_SHA256,
    "serving": "FORBIDDEN",
}
PROTOTYPE_CONTRACT_SHA256 = canonical_sha256(PROTOTYPE_CONTRACT)


@dataclass(frozen=True)
class PathReplay:
    state: PositionState
    nominal_ordinal: int
    fills: tuple[dict[str, Any], ...]
    first_exit_anchor: Mapping[str, Any] | None = None
    unknown_reason: str | None = None
    realized_fees: Decimal = Decimal(0)


@dataclass(frozen=True)
class PrototypeReplayResult:
    events: pd.DataFrame
    sleeve_days: pd.DataFrame
    fills: pd.DataFrame
    coverage: Mapping[str, Any]
    receipt: Mapping[str, Any]


class PatternCandidateCache:
    """Task-local immutable read cache; it never writes or changes the source."""

    def __init__(self, candidate: DailyCandidate) -> None:
        self._candidate = candidate
        self.root = candidate.root
        self.calendar = candidate.calendar
        self.spans = candidate.spans
        self.suspension_keys = candidate.suspension_keys
        self.references = candidate.references
        self._bars: dict[str, pd.DataFrame] = {}

    @property
    def symbols(self) -> tuple[str, ...]:
        return self._candidate.symbols

    def bars(self, symbol: str) -> pd.DataFrame:
        if symbol not in self._bars:
            self._bars[symbol] = self._candidate.bars(symbol)
        return self._bars[symbol]


def _snapshot_scope(path: Path, *, expected_symbols: Sequence[str], start: date, end: date) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        scope = payload["scope"]
        symbols = tuple(str(item).upper() for item in scope["symbols"])
        scope_start = date.fromisoformat(scope["start"])
        scope_end = date.fromisoformat(scope["end"])
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise ActionValueError("PATTERN_SNAPSHOT_SCOPE_INVALID", path=path.as_posix()) from exc
    missing = sorted(set(expected_symbols).difference(symbols))
    if missing or scope_start > start or scope_end < end:
        raise ActionValueError(
            "PATTERN_SNAPSHOT_SCOPE_MISMATCH",
            path=path.as_posix(),
            missing_symbol_count=len(missing),
            missing_symbol_examples=missing[:10],
        )
    return {"start": scope_start.isoformat(), "end": scope_end.isoformat(), "symbol_count": len(symbols)}


def _request_symbols(request: Mapping[str, Any]) -> tuple[str, ...]:
    raw: list[str] = []
    for key in ("selected_symbols", "training_symbols", "evaluation_symbols", "snapshot_symbols"):
        values = request.get(key)
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
            raw.extend(str(item).upper() for item in values)
    population = request.get("population_spec")
    if isinstance(population, Mapping):
        values = population.get("selected_symbols")
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
            raw.extend(str(item).upper() for item in values)
    return tuple(sorted(set(raw)))


def prior_timing_request_population(
    research_root: Path,
    *,
    exclude_request_sha256: str | None = None,
) -> Mapping[str, Any]:
    """Bind prior request populations without opening any outcome or receipt."""

    root = research_root.resolve()
    records: list[dict[str, Any]] = []
    forbidden: set[str] = set()
    for path in sorted(root.glob("*/requests/*.json")):
        reference = file_reference(path)
        try:
            request = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ActionValueError("PATTERN_PRIOR_REQUEST_UNREADABLE", path=path.as_posix()) from exc
        digest = request.get("request_sha256")
        identity = {key: value for key, value in request.items() if key != "request_sha256"}
        if not isinstance(digest, str) or digest != canonical_sha256(identity):
            raise ActionValueError("PATTERN_PRIOR_REQUEST_IDENTITY_MISMATCH", path=path.as_posix())
        if digest == exclude_request_sha256:
            continue
        symbols = _request_symbols(request)
        forbidden.update(symbols)
        records.append(
            {
                "relative_path": path.relative_to(root).as_posix(),
                "schema_version": request.get("schema_version"),
                "request_sha256": digest,
                "file_sha256": reference["sha256"],
                "symbols_sha256": canonical_sha256(symbols),
                "symbol_count": len(symbols),
            }
        )
    if not records:
        raise ActionValueError("PATTERN_PRIOR_REQUEST_SET_UNAVAILABLE")
    payload = {
        "schema_version": "position_timing_pattern_prior_requests_v1",
        "request_count": len(records),
        "requests": records,
        "forbidden_symbols": tuple(sorted(forbidden)),
        "forbidden_symbol_count": len(forbidden),
        "outcomes_read": False,
    }
    return {**payload, "aggregate_sha256": canonical_sha256(payload)}


def select_pattern_evaluation_symbols(
    candidate_symbols: Sequence[str], *, forbidden_symbols: Sequence[str], limit: int = EVALUATION_SYMBOL_LIMIT
) -> tuple[str, ...]:
    forbidden = {str(item).upper() for item in forbidden_symbols}
    ranked = sorted(
        {str(item).upper() for item in candidate_symbols},
        key=lambda symbol: (hashlib.sha256(f"{POPULATION_SEED_TEXT}|{symbol}".encode("ascii")).hexdigest(), symbol),
    )
    selected = tuple(symbol for symbol in ranked if symbol not in forbidden)[:limit]
    if not selected or len(selected) < min(limit, len(set(ranked).difference(forbidden))):
        raise ActionValueError("PATTERN_EVALUATION_POPULATION_UNAVAILABLE")
    return selected


def plan_pattern_population(*, timing_root: Path, parent_request_path: Path) -> Mapping[str, Any]:
    parent_ref = file_reference(parent_request_path)
    try:
        parent = json.loads(parent_request_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_PARENT_REQUEST_UNAVAILABLE") from exc
    identity = {key: value for key, value in parent.items() if key != "request_sha256"}
    if parent.get("request_sha256") != canonical_sha256(identity):
        raise ActionValueError("PATTERN_PARENT_REQUEST_IDENTITY_MISMATCH")
    population = parent.get("population_spec") or {}
    label_contract = parent.get("label_contract") or {}
    if (
        parent.get("schema_version") != "position_timing_deferred_entry_request_v1"
        or parent.get("pipeline_id") != "POSITION_TIMING_DEFERRED_ENTRY_HELDOUT_V1"
        or label_contract.get("horizon_trading_days") != PRIMARY_HORIZON
        or label_contract.get("terminal_max_defer_trading_days") != TERMINAL_MAX_DEFER
        or not parent.get("training_symbols")
        or population.get("start") is None
        or population.get("end") is None
        or parent.get("database_write") is not False
        or parent.get("runtime_write") is not False
    ):
        raise ActionValueError("PATTERN_PARENT_REQUEST_CONTRACT_MISMATCH")
    candidate = DailyCandidate.open(Path(parent["candidate_root"]))
    prior = prior_timing_request_population(timing_root.resolve() / "research")
    training = tuple(str(item).upper() for item in parent["training_symbols"])
    forbidden = tuple(sorted(set(prior["forbidden_symbols"]).union(training)))
    evaluation = select_pattern_evaluation_symbols(candidate.symbols, forbidden_symbols=forbidden)
    return {
        "schema_version": "position_timing_pattern_population_plan_v1",
        "parent_request": parent_ref,
        "candidate_root": candidate.root.as_posix(),
        "training_symbols": training,
        "evaluation_symbols": evaluation,
        "snapshot_symbols": tuple(sorted(set(training).union(evaluation))),
        "population_spec": {
            "start": population["start"],
            "end": population["end"],
            "selection": "SHA256_20260911_PIPE_SYMBOL_AFTER_ALL_PRIOR_REQUESTS",
            "initial_training_sessions": INITIAL_TRAINING_SESSIONS,
        },
        "prior_request_identity": prior,
        "outcomes_read": False,
    }


def _max_budgeted_buy(symbol: str, state: PositionState, reference: Decimal) -> ActionPlan:
    plans = action_candidates(symbol, state, reference)
    return max(plans, key=lambda item: item.delta)


def _pattern_raw_close(bar: Mapping[str, Any]) -> Decimal | None:
    if not bool(bar.get("pit_active")):
        return None
    return _available_raw_close(bar)


def _inventory_raw_close(bar: Mapping[str, Any]) -> Decimal | None:
    """Price existing inventory without treating PIT buy eligibility as tradability."""

    return _available_raw_close(bar)


def _effective_inventory_terminal_ordinal(
    bars: pd.DataFrame,
    nominal: int,
    *,
    max_defer: int,
    symbol: str,
    corporate_actions: CorporateActionBook,
    calendar_dates: Sequence[date],
) -> int | None:
    """Find a common liquidation day for inventory, including post-PIT holdings."""

    for ordinal in range(nominal, min(len(bars), nominal + max_defer + 1)):
        row = bars.iloc[ordinal]
        if bool(row.get("is_suspended")):
            continue
        try:
            close = money(row["close"])
            low = money(row["low"])
            high = money(row["high"])
            down = money(row["down_limit"])
        except (KeyError, ValueError, ArithmeticError):
            continue
        if min(close, low, high, down) <= 0 or low == high == down:
            continue
        action = corporate_actions.on(symbol, calendar_dates[ordinal])
        if (
            action is not None
            and action.quantity_multiplier > 1
            and action.share_listing_date is not None
            and action.share_listing_date > calendar_dates[ordinal]
        ):
            continue
        return ordinal
    return None


def _mapped_anchor_plan(
    *,
    symbol: str,
    quantity: int,
    anchor_reference: Decimal,
    anchor_ordinal: int,
    decision_ordinal: int,
    bars: pd.DataFrame,
    corporate_actions: CorporateActionBook,
) -> ActionPlan:
    if quantity <= 0:
        return ActionPlan(symbol, 0, anchor_reference)
    mapped = Decimal(quantity)
    for action in corporate_actions.between(
        symbol, bars.index[anchor_ordinal].date(), bars.index[decision_ordinal].date()
    ):
        mapped *= action.quantity_multiplier
    mapped_quantity = round_to_board_lot(int(mapped.to_integral_value(rounding=ROUND_FLOOR)), symbol, side="BUY")
    anchor_factor = money(bars.iloc[anchor_ordinal]["factor"])
    decision_factor = money(bars.iloc[decision_ordinal]["factor"])
    if min(anchor_factor, decision_factor) <= 0:
        raise ActionValueError("PATTERN_PLAN_FACTOR_INVALID", symbol=symbol)
    reference = anchor_reference * anchor_factor / decision_factor
    return ActionPlan(symbol, mapped_quantity, reference)


def _translated_target_plan(
    plan: ActionPlan,
    *,
    state: PositionState,
    target_state: PositionState,
    target_reference: Decimal,
    target_action: CorporateAction | None,
) -> ActionPlan:
    delta = plan.delta
    if target_action is not None:
        if delta > 0:
            mapped = int((Decimal(delta) * target_action.quantity_multiplier).to_integral_value(rounding=ROUND_FLOOR))
            delta = round_to_board_lot(mapped, plan.symbol, side="BUY")
        elif -delta == state.quantity:
            delta = -target_state.sellable
        elif delta < 0:
            mapped = int((Decimal(-delta) * target_action.quantity_multiplier).to_integral_value(rounding=ROUND_FLOOR))
            delta = -round_to_board_lot(mapped, plan.symbol, side="SELL", allow_sell_residual=False)
    return ActionPlan(plan.symbol, delta, target_reference, plan.risk_exit)


def _execute_next_session(
    *,
    state: PositionState,
    plan: ActionPlan,
    symbol: str,
    bars: pd.DataFrame,
    decision_ordinal: int,
    calendar_dates: Sequence[date],
    corporate_actions: CorporateActionBook,
    parent_count: int,
    additional_friction_bps: Decimal,
    path_role: str,
) -> tuple[PositionState, Fill, Decimal, CorporateAction | None, Decimal]:
    target_state, target_reference, target_action, fractional = _project_target_state(
        state=state,
        symbol=symbol,
        bars=bars,
        decision_ordinal=decision_ordinal,
        corporate_actions=corporate_actions,
        calendar_dates=calendar_dates,
    )
    translated = _translated_target_plan(
        plan,
        state=state,
        target_state=target_state,
        target_reference=target_reference,
        target_action=target_action,
    )
    target_bar = bars.iloc[decision_ordinal + 1]
    if translated.delta > 0 and not bool(target_bar.get("pit_active")):
        fill = Fill("NO_FILL", reason="TARGET_OUTSIDE_PIT")
    else:
        fill = daily_fill(
            translated,
            target_bar,
            sellable=target_state.sellable,
            parent_count=parent_count,
            full_exit=(-translated.delta == target_state.quantity),
            slippage_bps=Decimal(0),
        )
    if fill.status == "FILLED" and additional_friction_bps:
        if not additional_friction_bps.is_finite() or additional_friction_bps < 0:
            raise ActionValueError("PATTERN_FRICTION_SCENARIO_INVALID")
        fill = Fill(
            "FILLED",
            fill.delta,
            fill.price,
            fill.fee + Decimal(abs(fill.delta)) * fill.price * additional_friction_bps / Decimal(10000),
            "ADDITIONAL_FRICTION_SCENARIO",
        )
    if (
        fill.status == "FILLED"
        and fill.delta > 0
        and fill.price is not None
        and Decimal(fill.delta) * fill.price + fill.fee > target_state.cash
    ):
        fill = Fill("NO_FILL", reason="NO_FILL_BUDGET")
    if fill.status == "UNKNOWN":
        raise ActionValueError(
            "PATH_VALUATION_UNKNOWN",
            symbol=symbol,
            path_role=path_role,
            reason=fill.reason,
            target_trade_date=calendar_dates[decision_ordinal + 1].isoformat(),
        )
    return apply_fill(target_state, fill), fill, target_reference, target_action, fractional


def _carry_to_target(
    state: PositionState,
    *,
    symbol: str,
    decision_ordinal: int,
    calendar_dates: Sequence[date],
    corporate_actions: CorporateActionBook,
) -> PositionState:
    """Advance a no-decision day without inventing a tradable reference."""

    target_ordinal = decision_ordinal + 1
    action = corporate_actions.on(symbol, calendar_dates[target_ordinal])
    if action is None:
        return state
    if action.source_available_at > cutoff_on(calendar_dates[decision_ordinal]):
        raise ActionValueError(
            "CORPORATE_ACTION_NOT_VISIBLE_AT_DECISION",
            symbol=symbol,
            effective_trade_date=calendar_dates[target_ordinal].isoformat(),
        )
    return apply_corporate_action_with_audit(
        state,
        action,
        next_trade_date=(calendar_dates[target_ordinal + 1] if target_ordinal + 1 < len(calendar_dates) else None),
    ).state


def _mapped_reference_to_target(
    reference: Decimal | None,
    *,
    bars: pd.DataFrame,
    decision_ordinal: int,
    action: CorporateAction | None,
) -> Decimal | None:
    """Map the last known raw price across an ex-date when target price is absent."""

    if reference is None or action is None:
        return reference
    current_factor = money(bars.iloc[decision_ordinal]["factor"])
    target_factor = money(bars.iloc[decision_ordinal + 1]["factor"])
    if min(current_factor, target_factor) <= 0:
        raise ActionValueError("PATTERN_REFERENCE_FACTOR_INVALID")
    return reference * current_factor / target_factor


def _fill_record(
    *,
    symbol: str,
    path_role: str,
    decision_ordinal: int,
    calendar_dates: Sequence[date],
    plan: ActionPlan,
    fill: Fill,
    anchor_ordinal: int,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "path_role": path_role,
        "anchor_date": calendar_dates[anchor_ordinal],
        "decision_date": calendar_dates[decision_ordinal],
        "decision_as_of": cutoff_on(calendar_dates[decision_ordinal]),
        "feature_available_at": cutoff_on(calendar_dates[decision_ordinal]),
        "target_date": calendar_dates[decision_ordinal + 1],
        "planned_delta_qty": plan.delta,
        "fill_status": fill.status,
        "fill_reason": fill.reason,
        "fill_delta_qty": fill.delta,
        "fill_price_raw": float(fill.price) if fill.price is not None else None,
        "fill_fee_cny": float(fill.fee),
    }


def _economic_profit(state: PositionState, *, symbol: str, reference: Decimal) -> bool:
    return (
        state.quantity > 0
        and state.entry_cost is not None
        and _mark_to_market(state, symbol, reference) > state.capital
    )


def _simulate_entry_path(
    *,
    symbol: str,
    bars: pd.DataFrame,
    features: pd.DataFrame,
    calendar_dates: Sequence[date],
    corporate_actions: CorporateActionBook,
    anchor_ordinal: int,
    nominal_ordinal: int,
    initial_plan: ActionPlan | None,
    initial_decision_ordinal: int | None,
    path_role: str,
    parent_count: int,
    additional_friction_bps: Decimal,
    detect_exit_anchor: bool,
) -> PathReplay:
    state = PositionState(0, 0, REFERENCE_CAPITAL_CNY, REFERENCE_CAPITAL_CNY)
    fills: list[dict[str, Any]] = []
    realized_fees = Decimal(0)
    first_exit_anchor: Mapping[str, Any] | None = None
    next_decision = anchor_ordinal
    if initial_plan is not None and initial_decision_ordinal is not None:
        # Cash is unchanged by actions before the delayed first decision; the
        # plan itself is already mapped from the breakout share/price basis.
        state, fill, _, _, _ = _execute_next_session(
            state=state,
            plan=initial_plan,
            symbol=symbol,
            bars=bars,
            decision_ordinal=initial_decision_ordinal,
            calendar_dates=calendar_dates,
            corporate_actions=corporate_actions,
            parent_count=parent_count,
            additional_friction_bps=additional_friction_bps,
            path_role=path_role,
        )
        realized_fees += fill.fee
        fills.append(
            _fill_record(
                symbol=symbol,
                path_role=path_role,
                decision_ordinal=initial_decision_ordinal,
                calendar_dates=calendar_dates,
                plan=initial_plan,
                fill=fill,
                anchor_ordinal=anchor_ordinal,
            )
        )
        next_decision = initial_decision_ordinal + 1

    exit_edge_active = False
    for decision_ordinal in range(next_decision, nominal_ordinal):
        state = _roll_state_to_decision(state)
        reference = (
            _inventory_raw_close(bars.iloc[decision_ordinal])
            if state.quantity
            else _pattern_raw_close(bars.iloc[decision_ordinal])
        )
        if reference is None:
            state = _carry_to_target(
                state,
                symbol=symbol,
                decision_ordinal=decision_ordinal,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
            )
            continue
        risk = risk_exit_plan(symbol, state, reference)
        matched, reason_values = acceleration_volume_exit(features, decision_ordinal, "R0")
        eligible_exit = matched and _economic_profit(state, symbol=symbol, reference=reference)
        if detect_exit_anchor and first_exit_anchor is None and risk is None and eligible_exit and not exit_edge_active:
            first_exit_anchor = {
                "decision_ordinal": decision_ordinal,
                "state": state,
                "reason_values": reason_values,
            }
        exit_edge_active = eligible_exit
        plan = risk or ActionPlan(symbol, 0, reference)
        state, fill, _, _, _ = _execute_next_session(
            state=state,
            plan=plan,
            symbol=symbol,
            bars=bars,
            decision_ordinal=decision_ordinal,
            calendar_dates=calendar_dates,
            corporate_actions=corporate_actions,
            parent_count=parent_count,
            additional_friction_bps=additional_friction_bps,
            path_role=path_role,
        )
        realized_fees += fill.fee
        if plan.delta:
            fills.append(
                _fill_record(
                    symbol=symbol,
                    path_role=path_role,
                    decision_ordinal=decision_ordinal,
                    calendar_dates=calendar_dates,
                    plan=plan,
                    fill=fill,
                    anchor_ordinal=anchor_ordinal,
                )
            )
    return PathReplay(
        state,
        nominal_ordinal,
        tuple(fills),
        first_exit_anchor,
        realized_fees=realized_fees,
    )


def _common_terminal_values(
    paths: Sequence[PathReplay],
    *,
    symbol: str,
    bars: pd.DataFrame,
    nominal_ordinal: int,
    corporate_actions: CorporateActionBook,
    calendar_dates: Sequence[date],
) -> tuple[list[Decimal], list[Decimal], int] | None:
    if any(item.state.quantity for item in paths):
        effective = _effective_inventory_terminal_ordinal(
            bars,
            nominal_ordinal,
            max_defer=TERMINAL_MAX_DEFER,
            symbol=symbol,
            corporate_actions=corporate_actions,
            calendar_dates=calendar_dates,
        )
        if effective is None:
            return None
    else:
        effective = nominal_ordinal
    values: list[Decimal] = []
    gross_values: list[Decimal] = []
    for item in paths:
        state = item.state
        if effective > nominal_ordinal:
            state, _ = _apply_actions_until(
                state,
                symbol=symbol,
                start_exclusive=calendar_dates[nominal_ordinal],
                end_inclusive=calendar_dates[effective],
                corporate_actions=corporate_actions,
                calendar_dates=calendar_dates,
            )
        if state.quantity:
            reference = _inventory_raw_close(bars.iloc[effective])
            if reference is None:
                return None
            values.append(_mark_to_market(state, symbol, reference))
            gross_values.append(state.cash + item.realized_fees + Decimal(state.quantity) * reference)
        else:
            values.append(state.cash)
            gross_values.append(state.cash + item.realized_fees)
    return values, gross_values, effective


def _exit_quantity(symbol: str, state: PositionState, template_id: str) -> int:
    template = TEMPLATE_BY_ID[template_id]
    if template.exit_fraction == 1.0:
        return state.sellable
    requested = int(Decimal(state.sellable) * Decimal(str(template.exit_fraction)))
    return round_to_board_lot(requested, symbol, side="SELL", allow_sell_residual=False)


def _simulate_exit_pair(
    *,
    symbol: str,
    bars: pd.DataFrame,
    features: pd.DataFrame,
    calendar_dates: Sequence[date],
    corporate_actions: CorporateActionBook,
    exit_anchor: Mapping[str, Any],
    template_id: str,
    parent_count: int,
    additional_friction_bps: Decimal,
    comparison: str = "X1_MINUS_X0",
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    anchor = int(exit_anchor["decision_ordinal"])
    nominal = anchor + PRIMARY_HORIZON
    if nominal + TERMINAL_MAX_DEFER >= len(calendar_dates):
        return None, []
    initial = exit_anchor["state"]
    if not isinstance(initial, PositionState):
        raise ActionValueError("PATTERN_EXIT_ANCHOR_STATE_INVALID")
    reference = _pattern_raw_close(bars.iloc[anchor])
    if reference is None:
        return None, []
    quantity = _exit_quantity(symbol, initial, template_id)
    if quantity <= 0:
        return None, []

    paths: list[PathReplay] = []
    path_wealths: list[list[Decimal]] = []
    all_fills: list[dict[str, Any]] = []
    for role, do_exit in (("X1_ACCELERATION_EXIT", True), ("X0_RISK_OR_HOLD", False)):
        state = initial
        fills: list[dict[str, Any]] = []
        realized_fees = Decimal(0)
        plan = ActionPlan(symbol, -quantity, reference) if do_exit else ActionPlan(symbol, 0, reference)
        state, fill, target_reference, _, _ = _execute_next_session(
            state=state,
            plan=plan,
            symbol=symbol,
            bars=bars,
            decision_ordinal=anchor,
            calendar_dates=calendar_dates,
            corporate_actions=corporate_actions,
            parent_count=parent_count,
            additional_friction_bps=additional_friction_bps,
            path_role=role,
        )
        realized_fees += fill.fee
        if plan.delta:
            fills.append(
                _fill_record(
                    symbol=symbol,
                    path_role=role,
                    decision_ordinal=anchor,
                    calendar_dates=calendar_dates,
                    plan=plan,
                    fill=fill,
                    anchor_ordinal=anchor,
                )
            )
        first_price = _inventory_raw_close(bars.iloc[anchor + 1]) or target_reference
        wealths = [_mark_to_market(state, symbol, first_price) if state.quantity else state.cash]
        last_price = first_price
        for decision_ordinal in range(anchor + 1, nominal):
            state = _roll_state_to_decision(state)
            current = (
                _inventory_raw_close(bars.iloc[decision_ordinal])
                if state.quantity
                else _pattern_raw_close(bars.iloc[decision_ordinal])
            )
            if current is None:
                target_action = corporate_actions.on(symbol, calendar_dates[decision_ordinal + 1])
                last_price = _mapped_reference_to_target(
                    last_price,
                    bars=bars,
                    decision_ordinal=decision_ordinal,
                    action=target_action,
                ) or last_price
                state = _carry_to_target(
                    state,
                    symbol=symbol,
                    decision_ordinal=decision_ordinal,
                    calendar_dates=calendar_dates,
                    corporate_actions=corporate_actions,
                )
                target_price = _inventory_raw_close(bars.iloc[decision_ordinal + 1])
                if target_price is not None:
                    last_price = target_price
                wealths.append(_mark_to_market(state, symbol, last_price) if state.quantity else state.cash)
                continue
            risk = risk_exit_plan(symbol, state, current)
            next_plan = risk or ActionPlan(symbol, 0, current)
            state, next_fill, _, _, _ = _execute_next_session(
                state=state,
                plan=next_plan,
                symbol=symbol,
                bars=bars,
                decision_ordinal=decision_ordinal,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                parent_count=parent_count,
                additional_friction_bps=additional_friction_bps,
                path_role=role,
            )
            realized_fees += next_fill.fee
            if next_plan.delta:
                fills.append(
                    _fill_record(
                        symbol=symbol,
                        path_role=role,
                        decision_ordinal=decision_ordinal,
                        calendar_dates=calendar_dates,
                        plan=next_plan,
                        fill=next_fill,
                        anchor_ordinal=anchor,
                    )
                )
            target_price = _inventory_raw_close(bars.iloc[decision_ordinal + 1])
            if target_price is not None:
                last_price = target_price
            wealths.append(_mark_to_market(state, symbol, last_price) if state.quantity else state.cash)
        paths.append(PathReplay(state, nominal, tuple(fills), realized_fees=realized_fees))
        path_wealths.append(wealths)
        all_fills.extend(fills)
    terminal = _common_terminal_values(
        paths,
        symbol=symbol,
        bars=bars,
        nominal_ordinal=nominal,
        corporate_actions=corporate_actions,
        calendar_dates=calendar_dates,
    )
    if terminal is None:
        return None, all_fills
    values, gross_values, effective = terminal
    for index, value in enumerate(values):
        path_wealths[index].append(value)
    anchor_wealth = _mark_to_market(initial, symbol, reference)
    candidate_fees = paths[0].realized_fees
    baseline_fees = paths[1].realized_fees
    net_bps = (values[0] - values[1]) / anchor_wealth * Decimal(10000)
    gross_bps = (gross_values[0] - gross_values[1]) / anchor_wealth * Decimal(10000)
    drawdowns = []
    for wealths in path_wealths:
        numeric = np.asarray([float(value) for value in wealths], dtype=float)
        peaks = np.maximum.accumulate(numeric)
        drawdowns.append(float(np.max((1.0 - numeric / peaks) * 10000.0)))
    row = {
        "comparison": comparison,
        "symbol": symbol,
        "anchor_date": calendar_dates[anchor],
        "decision_as_of": cutoff_on(calendar_dates[anchor]),
        "feature_available_at": cutoff_on(calendar_dates[anchor]),
        "nominal_terminal_date": calendar_dates[nominal],
        "effective_terminal_date": calendar_dates[effective],
        "net_incremental_bps": float(net_bps),
        "gross_incremental_bps": float(gross_bps),
        "candidate_fee_cny": float(candidate_fees),
        "baseline_fee_cny": float(baseline_fees),
        "candidate_max_drawdown_bps": drawdowns[0],
        "baseline_max_drawdown_bps": drawdowns[1],
        "candidate_minus_baseline_drawdown_bps": drawdowns[0] - drawdowns[1],
        "candidate_terminal_wealth_cny": float(values[0]),
        "baseline_terminal_wealth_cny": float(values[1]),
        "candidate_fill_status": all_fills[0]["fill_status"] if all_fills else "NO_ACTION",
        "state_sha256": canonical_sha256(
            {
                "quantity": initial.quantity,
                "sellable": initial.sellable,
                "cash": str(initial.cash),
                "capital": str(initial.capital),
                "entry_cost": str(initial.entry_cost) if initial.entry_cost is not None else None,
                "holding_age": initial.holding_age,
            }
        ),
        **state_features(initial, ActionPlan(symbol, -quantity, reference)),
        **{f"reason_{key}": value for key, value in exit_anchor["reason_values"].items()},
    }
    return row, all_fills


def evaluate_entry_and_exit_mechanisms(
    *,
    symbol: str,
    bars: pd.DataFrame,
    features: pd.DataFrame,
    calendar_dates: Sequence[date],
    corporate_actions: CorporateActionBook,
    start_ordinal: int,
    end_ordinal: int,
    template_id: str = "R0",
    parent_count: int = 1,
    additional_friction_bps: Decimal = Decimal(0),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Counter[str]]:
    events: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    ordinal = max(start_ordinal, 20)
    last_anchor = min(end_ordinal - PRIMARY_HORIZON - TERMINAL_MAX_DEFER, len(calendar_dates) - 1)
    while ordinal <= last_anchor:
        if not breakout_observed(features, ordinal):
            ordinal += 1
            continue
        counts["BREAKOUT_OBSERVED"] += 1
        reference = _pattern_raw_close(bars.iloc[ordinal])
        if reference is None:
            counts["PATTERN_SOURCE_UNAVAILABLE"] += 1
            ordinal += 1
            continue
        cash = PositionState(0, 0, REFERENCE_CAPITAL_CNY, REFERENCE_CAPITAL_CNY)
        planned = _max_budgeted_buy(symbol, cash, reference)
        event = BreakoutEvent(ordinal, template_id, planned.delta)
        transition = None
        confirmation_ordinal: int | None = None
        template = TEMPLATE_BY_ID[template_id]
        for probe in range(ordinal + 1, min(len(features), ordinal + template.pullback_wait_sessions + 1)):
            transition = advance_entry_event(features, event, probe)
            if transition.state == "PULLBACK_CONFIRMED":
                confirmation_ordinal = probe
                break
            if transition.event is None:
                break
        outcome = transition.state if transition is not None else "PULLBACK_WINDOW_EXPIRED"
        counts[outcome] += 1

        baseline_plan = _mapped_anchor_plan(
            symbol=symbol,
            quantity=planned.delta,
            anchor_reference=reference,
            anchor_ordinal=ordinal,
            decision_ordinal=ordinal,
            bars=bars,
            corporate_actions=corporate_actions,
        )
        candidate_plan = (
            _mapped_anchor_plan(
                symbol=symbol,
                quantity=planned.delta,
                anchor_reference=reference,
                anchor_ordinal=ordinal,
                decision_ordinal=confirmation_ordinal,
                bars=bars,
                corporate_actions=corporate_actions,
            )
            if confirmation_ordinal is not None
            else None
        )
        nominal = ordinal + PRIMARY_HORIZON
        try:
            e0 = _simulate_entry_path(
                symbol=symbol,
                bars=bars,
                features=features,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                anchor_ordinal=ordinal,
                nominal_ordinal=nominal,
                initial_plan=baseline_plan,
                initial_decision_ordinal=ordinal,
                path_role="E0_IMMEDIATE",
                parent_count=parent_count,
                additional_friction_bps=additional_friction_bps,
                detect_exit_anchor=True,
            )
            e1 = _simulate_entry_path(
                symbol=symbol,
                bars=bars,
                features=features,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                anchor_ordinal=ordinal,
                nominal_ordinal=nominal,
                initial_plan=candidate_plan,
                initial_decision_ordinal=confirmation_ordinal,
                path_role="E1_PULLBACK",
                parent_count=parent_count,
                additional_friction_bps=additional_friction_bps,
                detect_exit_anchor=True,
            )
            fills.extend(e0.fills)
            fills.extend(e1.fills)
            terminal = _common_terminal_values(
                [e1, e0],
                symbol=symbol,
                bars=bars,
                nominal_ordinal=nominal,
                corporate_actions=corporate_actions,
                calendar_dates=calendar_dates,
            )
            if terminal is None:
                counts["UNAVAILABLE_AT_HORIZON"] += 1
            else:
                values, gross_values, effective = terminal
                candidate_fees = sum(
                    (Decimal(str(item["fill_fee_cny"])) for item in e1.fills),
                    Decimal(0),
                )
                baseline_fees = sum(
                    (Decimal(str(item["fill_fee_cny"])) for item in e0.fills),
                    Decimal(0),
                )
                net_bps = (values[0] - values[1]) / REFERENCE_CAPITAL_CNY * Decimal(10000)
                gross_bps = (gross_values[0] - gross_values[1]) / REFERENCE_CAPITAL_CNY * Decimal(10000)
                events.append(
                    {
                        "comparison": "E1_MINUS_E0",
                        "symbol": symbol,
                        "anchor_date": calendar_dates[ordinal],
                        "decision_as_of": cutoff_on(calendar_dates[ordinal]),
                        "feature_available_at": cutoff_on(calendar_dates[ordinal]),
                        "nominal_terminal_date": calendar_dates[nominal],
                        "effective_terminal_date": calendar_dates[effective],
                        "event_outcome": outcome,
                        "confirmation_date": calendar_dates[confirmation_ordinal]
                        if confirmation_ordinal is not None
                        else None,
                        "planned_quantity_at_breakout": planned.delta,
                        "net_incremental_bps": float(net_bps),
                        "gross_incremental_bps": float(gross_bps),
                        "candidate_fee_cny": float(candidate_fees),
                        "baseline_fee_cny": float(baseline_fees),
                        "candidate_terminal_wealth_cny": float(values[0]),
                        "baseline_terminal_wealth_cny": float(values[1]),
                        "candidate_fill_status": e1.fills[0]["fill_status"] if e1.fills else "NO_ACTION",
                        "baseline_fill_status": e0.fills[0]["fill_status"] if e0.fills else "NO_ACTION",
                    }
                )
                counts["EVALUATED_ENTRY_EVENTS"] += 1
            if e0.first_exit_anchor is not None:
                exit_row, exit_fills = _simulate_exit_pair(
                    symbol=symbol,
                    bars=bars,
                    features=features,
                    calendar_dates=calendar_dates,
                    corporate_actions=corporate_actions,
                    exit_anchor=e0.first_exit_anchor,
                    template_id=template_id,
                    parent_count=parent_count,
                    additional_friction_bps=additional_friction_bps,
                )
                fills.extend(exit_fills)
                if exit_row is None:
                    counts["RIGHT_CENSORED_EXIT_EVENTS"] += 1
                else:
                    events.append(exit_row)
                    counts["EVALUATED_EXIT_EVENTS"] += 1
            if e1.first_exit_anchor is not None:
                model_exit_row, model_exit_fills = _simulate_exit_pair(
                    symbol=symbol,
                    bars=bars,
                    features=features,
                    calendar_dates=calendar_dates,
                    corporate_actions=corporate_actions,
                    exit_anchor=e1.first_exit_anchor,
                    template_id=template_id,
                    parent_count=parent_count,
                    additional_friction_bps=additional_friction_bps,
                    comparison="X1_MINUS_X0_R0_ENTRY_DIAGNOSTIC",
                )
                fills.extend(model_exit_fills)
                if model_exit_row is None:
                    counts["RIGHT_CENSORED_R0_EXIT_EVENTS"] += 1
                else:
                    events.append(model_exit_row)
                    counts["EVALUATED_R0_EXIT_EVENTS"] += 1
        except ActionValueError as exc:
            counts[f"UNKNOWN_{exc.code}"] += 1
        ordinal += PRIMARY_HORIZON + TERMINAL_MAX_DEFER + 1
    return events, fills, counts


def _policy_plan(
    *,
    symbol: str,
    state: PositionState,
    reference: Decimal,
    features: pd.DataFrame,
    ordinal: int,
    template_id: str,
    exit_edge_active: bool,
    exit_selector: Callable[..., Mapping[str, Any]] | None = None,
) -> tuple[ActionPlan | None, bool, Mapping[str, Any]]:
    risk = risk_exit_plan(symbol, state, reference)
    if risk is not None:
        return risk, False, {"authority": "FROZEN_RISK_EXIT"}
    matched, reasons = acceleration_volume_exit(features, ordinal, template_id)
    eligible = matched and _economic_profit(state, symbol=symbol, reference=reference)
    if eligible and not exit_edge_active:
        selection = (
            exit_selector(
                symbol=symbol,
                ordinal=ordinal,
                state=state,
                reference=reference,
                features=features,
            )
            if exit_selector is not None
            else {"execute": True, "authority": "ACCELERATION_VOLUME_EXIT"}
        )
        if selection.get("execute") is not True:
            return None, True, {"authority": str(selection.get("authority", "MODEL_X0_HOLD")), **reasons, **selection}
        quantity = _exit_quantity(symbol, state, template_id)
        if quantity:
            return (
                ActionPlan(symbol, -quantity, reference),
                True,
                {
                    "authority": str(selection.get("authority", "ACCELERATION_VOLUME_EXIT")),
                    **reasons,
                    **selection,
                },
            )
    return None, eligible, {"authority": "HOLD", **reasons}


def replay_full_policy_symbol(
    *,
    symbol: str,
    bars: pd.DataFrame,
    features: pd.DataFrame,
    calendar_dates: Sequence[date],
    corporate_actions: CorporateActionBook,
    start_ordinal: int,
    terminal_ordinal: int,
    template_id: str = "R0",
    template_schedule: Mapping[str, str] | None = None,
    entry_selector: Callable[..., Mapping[str, Any]] | None = None,
    exit_selector: Callable[..., Mapping[str, Any]] | None = None,
    parent_count: int = 1,
    additional_friction_bps: Decimal = Decimal(0),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Counter[str]]:
    states = {
        "POLICY": PositionState(0, 0, REFERENCE_CAPITAL_CNY, REFERENCE_CAPITAL_CNY),
        "BUY_AND_HOLD": PositionState(0, 0, REFERENCE_CAPITAL_CNY, REFERENCE_CAPITAL_CNY),
        "FROZEN_L1": PositionState(0, 0, REFERENCE_CAPITAL_CNY, REFERENCE_CAPITAL_CNY),
    }
    buy_hold_complete = False
    active_event: BreakoutEvent | None = None
    event_reference: Decimal | None = None
    blocked_until = start_ordinal
    exit_edge_active = False
    exit_edge_template_id = template_id
    previous_difference = {"BUY_AND_HOLD": Decimal(0), "FROZEN_L1": Decimal(0)}
    previous_gross_difference = {"BUY_AND_HOLD": Decimal(0), "FROZEN_L1": Decimal(0)}
    cumulative_fees = {key: Decimal(0) for key in states}
    last_prices: dict[str, Decimal | None] = {key: None for key in states}
    rows: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for ordinal in range(start_ordinal, terminal_ordinal):
        month = calendar_dates[ordinal].strftime("%Y-%m")
        current_template_id = (
            str(template_schedule.get(month, template_id)) if template_schedule is not None else template_id
        )
        if current_template_id not in TEMPLATE_BY_ID:
            raise ActionValueError("PATTERN_TEMPLATE_SCHEDULE_INVALID", month=month)
        if current_template_id != exit_edge_template_id:
            exit_edge_active = False
            exit_edge_template_id = current_template_id
        has_inventory = any(state.quantity for state in states.values())
        if has_inventory and not bool(bars.iloc[ordinal].get("pit_active")):
            counts["HELD_INVENTORY_OUTSIDE_PIT_BUY_ELIGIBILITY"] += 1
        reference = (
            _inventory_raw_close(bars.iloc[ordinal]) if has_inventory else _pattern_raw_close(bars.iloc[ordinal])
        )
        plans: dict[str, ActionPlan] = {}
        for role in tuple(states):
            states[role] = _roll_state_to_decision(states[role])

        if reference is None:
            counts["DECISION_PRICE_UNAVAILABLE"] += 1
            if active_event is not None:
                counts["PATTERN_SOURCE_UNAVAILABLE"] += 1
                blocked_until = max(
                    blocked_until,
                    active_event.breakout_ordinal + TEMPLATE_BY_ID[active_event.template_id].pullback_wait_sessions,
                )
                active_event = None
                event_reference = None
            for role in tuple(states):
                target_action = corporate_actions.on(symbol, calendar_dates[ordinal + 1])
                last_prices[role] = _mapped_reference_to_target(
                    last_prices[role],
                    bars=bars,
                    decision_ordinal=ordinal,
                    action=target_action,
                )
                states[role] = _carry_to_target(
                    states[role],
                    symbol=symbol,
                    decision_ordinal=ordinal,
                    calendar_dates=calendar_dates,
                    corporate_actions=corporate_actions,
                )
            target_ordinal = ordinal + 1
            price = _inventory_raw_close(bars.iloc[target_ordinal])
            for role in states:
                if price is not None:
                    last_prices[role] = price
                if last_prices[role] is None and states[role].quantity:
                    raise ActionValueError("PATTERN_VALUATION_PRICE_UNAVAILABLE", symbol=symbol)
            wealth = {
                role: (_mark_to_market(state, symbol, last_prices[role]) if state.quantity else state.cash)
                for role, state in states.items()
            }
            gross_wealth = {
                role: (state.cash + cumulative_fees[role] + Decimal(state.quantity) * (last_prices[role] or Decimal(0)))
                for role, state in states.items()
            }
            for baseline in ("BUY_AND_HOLD", "FROZEN_L1"):
                difference = wealth["POLICY"] - wealth[baseline]
                increment = difference - previous_difference[baseline]
                previous_difference[baseline] = difference
                gross_difference = gross_wealth["POLICY"] - gross_wealth[baseline]
                gross_increment = gross_difference - previous_gross_difference[baseline]
                previous_gross_difference[baseline] = gross_difference
                rows.append(
                    {
                        "symbol": symbol,
                        "valuation_date": calendar_dates[target_ordinal],
                        "decision_as_of": cutoff_on(calendar_dates[ordinal]),
                        "feature_available_at": cutoff_on(calendar_dates[ordinal]),
                        "comparison": f"P_MINUS_{baseline}",
                        "incremental_net_value_bps": float(increment / REFERENCE_CAPITAL_CNY * Decimal(10000)),
                        "incremental_gross_value_bps": float(gross_increment / REFERENCE_CAPITAL_CNY * Decimal(10000)),
                        "policy_wealth_cny": float(wealth["POLICY"]),
                        "baseline_wealth_cny": float(wealth[baseline]),
                        "policy_quantity": states["POLICY"].quantity,
                        "baseline_quantity": states[baseline].quantity,
                        "policy_exposure": float(
                            Decimal(states["POLICY"].quantity)
                            * (last_prices["POLICY"] or Decimal(0))
                            / wealth["POLICY"]
                        ),
                        "policy_authority": "SOURCE_UNAVAILABLE_NO_ACTION",
                        "template_id": current_template_id,
                    }
                )
            continue

        policy_state = states["POLICY"]
        policy_plan: ActionPlan | None = None
        policy_authority = "WAIT" if not policy_state.quantity else "HOLD"
        if policy_state.quantity:
            policy_plan, exit_edge_active, metadata = _policy_plan(
                symbol=symbol,
                state=policy_state,
                reference=reference,
                features=features,
                ordinal=ordinal,
                template_id=current_template_id,
                exit_edge_active=exit_edge_active,
                exit_selector=exit_selector,
            )
            policy_authority = str(metadata["authority"])
            active_event = None
            event_reference = None
        else:
            exit_edge_active = False
            if active_event is not None:
                transition = advance_entry_event(features, active_event, ordinal)
                counts[transition.state] += 1
                if transition.state == "PULLBACK_CONFIRMED":
                    if event_reference is None:
                        raise ActionValueError("PATTERN_EVENT_REFERENCE_MISSING")
                    policy_plan = _mapped_anchor_plan(
                        symbol=symbol,
                        quantity=active_event.planned_quantity,
                        anchor_reference=event_reference,
                        anchor_ordinal=active_event.breakout_ordinal,
                        decision_ordinal=ordinal,
                        bars=bars,
                        corporate_actions=corporate_actions,
                    )
                    policy_authority = "PULLBACK_CONFIRMED_OPEN"
                if transition.event is None:
                    blocked_until = max(
                        blocked_until,
                        active_event.breakout_ordinal + TEMPLATE_BY_ID[active_event.template_id].pullback_wait_sessions,
                    )
                    active_event = None
                    event_reference = None
            if (
                active_event is None
                and policy_plan is None
                and ordinal >= blocked_until
                and breakout_observed(features, ordinal)
            ):
                candidate = _max_budgeted_buy(symbol, policy_state, reference)
                selection = (
                    entry_selector(
                        symbol=symbol,
                        ordinal=ordinal,
                        state=policy_state,
                        reference=reference,
                        features=features,
                        plan=candidate,
                    )
                    if entry_selector is not None
                    else {"choice": "E1", "authority": "RULE_R0_WAIT_PULLBACK"}
                )
                choice = selection.get("choice")
                if choice == "E0":
                    policy_plan = candidate
                    policy_authority = str(selection.get("authority", "MODEL_E0_IMMEDIATE"))
                    blocked_until = ordinal + TEMPLATE_BY_ID[current_template_id].pullback_wait_sessions
                elif choice == "E1":
                    active_event = BreakoutEvent(ordinal, current_template_id, candidate.delta)
                    event_reference = reference
                    policy_authority = str(selection.get("authority", "MODEL_E1_WAIT_PULLBACK"))
                else:
                    raise ActionValueError("PATTERN_ENTRY_SELECTOR_RESULT_INVALID")
                counts["BREAKOUT_OBSERVED"] += 1

        plans["POLICY"] = policy_plan or ActionPlan(symbol, 0, reference)
        if not buy_hold_complete and bool(bars.iloc[ordinal].get("pit_active")):
            plans["BUY_AND_HOLD"] = _max_budgeted_buy(symbol, states["BUY_AND_HOLD"], reference)
        else:
            plans["BUY_AND_HOLD"] = ActionPlan(symbol, 0, reference)
        l1_risk = risk_exit_plan(symbol, states["FROZEN_L1"], reference)
        plans["FROZEN_L1"] = l1_risk or ActionPlan(symbol, 0, reference)

        target_references: dict[str, Decimal] = {}
        for role, plan in plans.items():
            pre = states[role]
            state, fill, target_reference, _, _ = _execute_next_session(
                state=pre,
                plan=plan,
                symbol=symbol,
                bars=bars,
                decision_ordinal=ordinal,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                parent_count=parent_count,
                additional_friction_bps=additional_friction_bps,
                path_role=f"FULL_{role}",
            )
            states[role] = state
            target_references[role] = target_reference
            cumulative_fees[role] += fill.fee
            if plan.delta:
                fills.append(
                    _fill_record(
                        symbol=symbol,
                        path_role=f"FULL_{role}",
                        decision_ordinal=ordinal,
                        calendar_dates=calendar_dates,
                        plan=plan,
                        fill=fill,
                        anchor_ordinal=start_ordinal,
                    )
                )
                counts[f"{role}_{fill.status}"] += 1
            if role == "BUY_AND_HOLD" and state.quantity:
                buy_hold_complete = True

        target_ordinal = ordinal + 1
        price = _inventory_raw_close(bars.iloc[target_ordinal])
        for role in states:
            if price is not None:
                last_prices[role] = price
            elif states[role].quantity:
                last_prices[role] = target_references[role]
            if last_prices[role] is None and states[role].quantity:
                raise ActionValueError("PATTERN_VALUATION_PRICE_UNAVAILABLE", symbol=symbol)
        wealth = {
            role: (_mark_to_market(state, symbol, last_prices[role]) if state.quantity else state.cash)
            for role, state in states.items()
        }
        gross_wealth = {
            role: (state.cash + cumulative_fees[role] + Decimal(state.quantity) * (last_prices[role] or Decimal(0)))
            for role, state in states.items()
        }
        for baseline in ("BUY_AND_HOLD", "FROZEN_L1"):
            difference = wealth["POLICY"] - wealth[baseline]
            increment = difference - previous_difference[baseline]
            previous_difference[baseline] = difference
            gross_difference = gross_wealth["POLICY"] - gross_wealth[baseline]
            gross_increment = gross_difference - previous_gross_difference[baseline]
            previous_gross_difference[baseline] = gross_difference
            rows.append(
                {
                    "symbol": symbol,
                    "valuation_date": calendar_dates[target_ordinal],
                    "decision_as_of": cutoff_on(calendar_dates[ordinal]),
                    "feature_available_at": cutoff_on(calendar_dates[ordinal]),
                    "comparison": f"P_MINUS_{baseline}",
                    "incremental_net_value_bps": float(increment / REFERENCE_CAPITAL_CNY * Decimal(10000)),
                    "incremental_gross_value_bps": float(gross_increment / REFERENCE_CAPITAL_CNY * Decimal(10000)),
                    "policy_wealth_cny": float(wealth["POLICY"]),
                    "baseline_wealth_cny": float(wealth[baseline]),
                    "policy_quantity": states["POLICY"].quantity,
                    "baseline_quantity": states[baseline].quantity,
                    "policy_exposure": float(
                        Decimal(states["POLICY"].quantity) * (last_prices["POLICY"] or Decimal(0)) / wealth["POLICY"]
                    ),
                    "policy_authority": policy_authority,
                    "template_id": current_template_id,
                }
            )
    counts["L1_BASELINE_INFORMATION_LIMITATION_CASH_WITHOUT_HISTORICAL_INTENT"] += 1
    return rows, fills, counts


def sparse_event_interval(
    daily_numerator: np.ndarray,
    daily_indicator: np.ndarray,
    *,
    block_sessions: int = BLOCK_SESSIONS,
    samples: int = BOOTSTRAP_SAMPLES,
    seed: int = INFERENCE_SEED,
    confidence_level: float = PROTOTYPE_CONFIDENCE_LEVEL,
) -> Mapping[str, Any]:
    numerator = np.asarray(daily_numerator, dtype=float)
    indicator = np.asarray(daily_indicator, dtype=float)
    if (
        numerator.ndim != 1
        or indicator.ndim != 1
        or len(numerator) != len(indicator)
        or not len(numerator)
        or not np.isfinite(numerator).all()
        or not np.isin(indicator, [0.0, 1.0]).all()
        or not 0 < confidence_level < 1
        or samples <= 0
        or block_sessions <= 0
        or indicator.sum() <= 0
    ):
        raise ActionValueError("PATTERN_SPARSE_INFERENCE_INPUT_INVALID")
    rng = np.random.default_rng(seed)
    block = min(block_sessions, len(numerator))
    count = int(np.ceil(len(numerator) / block))
    draws: list[float] = []
    for _ in range(samples):
        starts = rng.integers(0, len(numerator), size=count)
        indexes = np.concatenate([(np.arange(start, start + block) % len(numerator)) for start in starts])[
            : len(numerator)
        ]
        denominator = indicator[indexes].sum()
        if denominator > 0:
            draws.append(float(numerator[indexes].sum() / denominator))
    valid_fraction = len(draws) / samples
    if valid_fraction < 0.95:
        return {
            "point_bps": float(numerator.sum() / indicator.sum()),
            "lower_bps": None,
            "upper_bps": None,
            "valid_resample_fraction": valid_fraction,
            "reason_code": "SPARSE_BOOTSTRAP_VALID_FRACTION_INSUFFICIENT",
        }
    alpha = 1.0 - confidence_level
    return {
        "point_bps": float(numerator.sum() / indicator.sum()),
        "lower_bps": float(np.quantile(draws, alpha / 2.0)),
        "upper_bps": float(np.quantile(draws, 1.0 - alpha / 2.0)),
        "valid_resample_fraction": valid_fraction,
        "reason_code": None,
    }


def mean_interval(
    values: np.ndarray,
    *,
    block_sessions: int = BLOCK_SESSIONS,
    samples: int = BOOTSTRAP_SAMPLES,
    seed: int = INFERENCE_SEED,
    confidence_level: float = PROTOTYPE_CONFIDENCE_LEVEL,
) -> Mapping[str, Any]:
    values = np.asarray(values, dtype=float)
    if values.ndim != 1 or not len(values) or not np.isfinite(values).all():
        raise ActionValueError("PATTERN_MEAN_INFERENCE_INPUT_INVALID")
    indicator = np.ones(len(values), dtype=float)
    return sparse_event_interval(
        values,
        indicator,
        block_sessions=block_sessions,
        samples=samples,
        seed=seed,
        confidence_level=confidence_level,
    )


def _effect_evidence(inference: Mapping[str, Any], *, coverage_complete: bool) -> str:
    lower, upper = inference.get("lower_bps"), inference.get("upper_bps")
    if lower is None or upper is None or not coverage_complete:
        return "INCONCLUSIVE"
    if float(lower) > 0:
        return "SUPPORTED"
    if float(upper) < 0:
        return "NEGATIVE"
    return "INCONCLUSIVE"


def _comparison_receipts(
    events: pd.DataFrame,
    sleeve_days: pd.DataFrame,
    *,
    calendar_dates: Sequence[date],
    coverage_complete: bool,
    bootstrap_samples: int,
) -> Mapping[str, Any]:
    comparisons: dict[str, Any] = {}
    for offset, comparison in enumerate(("E1_MINUS_E0", "X1_MINUS_X0")):
        subset = events.loc[events["comparison"].eq(comparison)] if not events.empty else pd.DataFrame()
        if subset.empty:
            inference = {"point_bps": None, "lower_bps": None, "upper_bps": None, "reason_code": "NO_EVALUABLE_EVENTS"}
            nominal_inference = dict(inference)
        else:
            by_day = subset.groupby("anchor_date", as_index=True)["net_incremental_bps"].mean()
            aligned = by_day.reindex(pd.Index(calendar_dates), fill_value=0.0)
            indicator = pd.Series(0.0, index=pd.Index(calendar_dates))
            indicator.loc[by_day.index] = 1.0
            inference = sparse_event_interval(
                aligned.to_numpy(float),
                indicator.to_numpy(float),
                samples=bootstrap_samples,
                seed=INFERENCE_SEED + offset,
            )
            nominal_inference = sparse_event_interval(
                aligned.to_numpy(float),
                indicator.to_numpy(float),
                samples=bootstrap_samples,
                seed=INFERENCE_SEED + offset,
                confidence_level=0.95,
            )
        mde = (
            max(
                inference["point_bps"] - inference["lower_bps"],
                inference["upper_bps"] - inference["point_bps"],
            )
            if inference.get("lower_bps") is not None and inference.get("upper_bps") is not None
            else None
        )
        comparisons[comparison] = {
            "unit": "EVENT_BPS_PER_ACTIVE_ANCHOR_DATE",
            "event_count": int(len(subset)),
            "active_date_count": int(subset["anchor_date"].nunique()) if not subset.empty else 0,
            "gross_point_bps": (
                float(subset.groupby("anchor_date")["gross_incremental_bps"].mean().mean())
                if not subset.empty
                else None
            ),
            "inference": inference,
            "nominal_inference": nominal_inference,
            "mde_bps": mde,
            "power_status": "NOT_COMPUTABLE",
            "power_reason_code": "NO_PREREGISTERED_SAME_ESTIMAND_EFFECT_SCALE",
            "effect_evidence": _effect_evidence(inference, coverage_complete=coverage_complete),
        }
    for offset, comparison in enumerate(("P_MINUS_BUY_AND_HOLD", "P_MINUS_FROZEN_L1"), start=2):
        subset = sleeve_days.loc[sleeve_days["comparison"].eq(comparison)] if not sleeve_days.empty else pd.DataFrame()
        if subset.empty:
            inference = {
                "point_bps": None,
                "lower_bps": None,
                "upper_bps": None,
                "reason_code": "NO_EVALUABLE_POLICY_DAYS",
            }
            nominal_inference = dict(inference)
            daily_count = 0
        else:
            daily = subset.groupby("valuation_date")["incremental_net_value_bps"].mean()
            inference = mean_interval(daily.to_numpy(float), samples=bootstrap_samples, seed=INFERENCE_SEED + offset)
            nominal_inference = mean_interval(
                daily.to_numpy(float),
                samples=bootstrap_samples,
                seed=INFERENCE_SEED + offset,
                confidence_level=0.95,
            )
            daily_count = len(daily)
        mde = (
            max(
                inference["point_bps"] - inference["lower_bps"],
                inference["upper_bps"] - inference["point_bps"],
            )
            if inference.get("lower_bps") is not None and inference.get("upper_bps") is not None
            else None
        )
        comparisons[comparison] = {
            "unit": "BPS_PER_TRADING_DAY",
            "trading_day_count": daily_count,
            "gross_point_bps": (
                float(subset.groupby("valuation_date")["incremental_gross_value_bps"].mean().mean())
                if not subset.empty
                else None
            ),
            "inference": inference,
            "nominal_inference": nominal_inference,
            "mde_bps": mde,
            "power_status": "NOT_COMPUTABLE",
            "power_reason_code": "NO_PREREGISTERED_SAME_ESTIMAND_EFFECT_SCALE",
            "effect_evidence": _effect_evidence(inference, coverage_complete=coverage_complete),
        }
    return comparisons


def _max_drawdown_bps(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(float)
    if not len(numeric) or not np.isfinite(numeric).all() or np.any(numeric <= 0):
        return None
    return float(np.max((1.0 - numeric / np.maximum.accumulate(numeric)) * 10000.0))


def _research_diagnostics(events: pd.DataFrame, sleeve_days: pd.DataFrame, fills: pd.DataFrame) -> Mapping[str, Any]:
    entry = events.loc[events["comparison"].eq("E1_MINUS_E0")] if not events.empty else pd.DataFrame()
    exit_rows = events.loc[events["comparison"].eq("X1_MINUS_X0")] if not events.empty else pd.DataFrame()
    policy = sleeve_days.drop_duplicates(["symbol", "valuation_date"]) if not sleeve_days.empty else pd.DataFrame()
    drawdowns: dict[str, Any] = {}
    if not sleeve_days.empty:
        for role, column in (
            ("POLICY", "policy_wealth_cny"),
            ("BUY_AND_HOLD", "baseline_wealth_cny"),
            ("FROZEN_L1", "baseline_wealth_cny"),
        ):
            if role == "POLICY":
                source = policy
            else:
                source = sleeve_days.loc[
                    sleeve_days["comparison"].eq(
                        "P_MINUS_BUY_AND_HOLD" if role == "BUY_AND_HOLD" else "P_MINUS_FROZEN_L1"
                    )
                ]
            per_symbol = {
                symbol: _max_drawdown_bps(group.sort_values("valuation_date")[column])
                for symbol, group in source.groupby("symbol")
            }
            finite = [value for value in per_symbol.values() if value is not None]
            drawdowns[role] = {
                "mean_symbol_max_drawdown_bps": float(np.mean(finite)) if finite else None,
                "max_symbol_drawdown_bps": float(np.max(finite)) if finite else None,
            }
    full_policy_fills = (
        fills.loc[fills["path_role"].eq("FULL_POLICY") & fills["fill_status"].eq("FILLED")]
        if not fills.empty
        else pd.DataFrame()
    )
    turnover = (
        float(
            (
                pd.to_numeric(full_policy_fills["fill_delta_qty"]).abs()
                * pd.to_numeric(full_policy_fills["fill_price_raw"])
            ).sum()
            / (max(1, policy["symbol"].nunique()) * float(REFERENCE_CAPITAL_CNY))
        )
        if not full_policy_fills.empty
        else 0.0
    )
    return {
        "mean_policy_exposure": (float(pd.to_numeric(policy["policy_exposure"]).mean()) if not policy.empty else None),
        "turnover_notional_over_initial_capital_per_symbol": turnover,
        "drawdown": drawdowns,
        "waiting_missed_upside": {
            "negative_event_count": int((entry["net_incremental_bps"] < 0).sum()) if not entry.empty else 0,
            "mean_negative_bps": (
                float(entry.loc[entry["net_incremental_bps"] < 0, "net_incremental_bps"].mean())
                if not entry.empty and (entry["net_incremental_bps"] < 0).any()
                else None
            ),
        },
        "take_profit_continued_upside": {
            "negative_event_count": int((exit_rows["net_incremental_bps"] < 0).sum()) if not exit_rows.empty else 0,
            "mean_negative_bps": (
                float(exit_rows.loc[exit_rows["net_incremental_bps"] < 0, "net_incremental_bps"].mean())
                if not exit_rows.empty and (exit_rows["net_incremental_bps"] < 0).any()
                else None
            ),
            "mean_candidate_minus_baseline_drawdown_bps": (
                float(exit_rows["candidate_minus_baseline_drawdown_bps"].mean()) if not exit_rows.empty else None
            ),
        },
    }


def replay_prototype(
    candidate: DailyCandidate,
    *,
    symbols: Sequence[str],
    corporate_actions: CorporateActionBook,
    start: date,
    end: date,
    template_id: str = "R0",
    parent_count: int = 1,
    additional_friction_bps: Decimal = Decimal(0),
    bootstrap_samples: int = BOOTSTRAP_SAMPLES,
) -> PrototypeReplayResult:
    if template_id not in TEMPLATE_BY_ID or not symbols or len(set(symbols)) != len(symbols):
        raise ActionValueError("PATTERN_REPLAY_SPEC_INVALID")
    calendar_dates = tuple(timestamp.date() for timestamp in candidate.calendar)
    date_to_ordinal = {day: ordinal for ordinal, day in enumerate(calendar_dates)}
    if start not in date_to_ordinal or end not in date_to_ordinal:
        raise ActionValueError("PATTERN_REPLAY_CALENDAR_MISMATCH")
    source_start = date_to_ordinal[start]
    start_ordinal = source_start + INITIAL_TRAINING_SESSIONS
    end_ordinal = date_to_ordinal[end]
    terminal_ordinal = end_ordinal - TERMINAL_MAX_DEFER
    if start_ordinal >= terminal_ordinal:
        raise ActionValueError("PATTERN_REPLAY_RANGE_EMPTY")

    all_events: list[dict[str, Any]] = []
    all_sleeves: list[dict[str, Any]] = []
    all_fills: list[dict[str, Any]] = []
    symbol_counts: dict[str, Any] = {}
    excluded: Counter[str] = Counter()
    for symbol in symbols:
        try:
            bars = candidate.bars(symbol)
            if _has_unbound_material_factor_change(
                symbol=symbol,
                bars=bars,
                start_ordinal=start_ordinal,
                end_ordinal=end_ordinal,
                corporate_actions=corporate_actions,
            ):
                raise ActionValueError("UNBOUND_MATERIAL_FACTOR_CHANGE", symbol=symbol)
            features = pattern_feature_frame(bars, symbol=symbol, corporate_actions=corporate_actions)
            events, mechanism_fills, mechanism_counts = evaluate_entry_and_exit_mechanisms(
                symbol=symbol,
                bars=bars,
                features=features,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                start_ordinal=start_ordinal,
                end_ordinal=end_ordinal,
                template_id=template_id,
                parent_count=parent_count,
                additional_friction_bps=additional_friction_bps,
            )
            sleeves, policy_fills, policy_counts = replay_full_policy_symbol(
                symbol=symbol,
                bars=bars,
                features=features,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                start_ordinal=start_ordinal,
                terminal_ordinal=terminal_ordinal,
                template_id=template_id,
                parent_count=parent_count,
                additional_friction_bps=additional_friction_bps,
            )
            all_events.extend(events)
            all_sleeves.extend(sleeves)
            all_fills.extend(mechanism_fills)
            all_fills.extend(policy_fills)
            symbol_counts[symbol] = {
                "mechanism": dict(sorted(mechanism_counts.items())),
                "policy": dict(sorted(policy_counts.items())),
                "event_rows": len(events),
                "sleeve_rows": len(sleeves),
            }
        except ActionValueError as exc:
            excluded[exc.code] += 1
            symbol_counts[symbol] = {"excluded": True, "reason_code": exc.code, "details": exc.details}

    events_frame = pd.DataFrame(all_events)
    sleeves_frame = pd.DataFrame(all_sleeves)
    fills_frame = pd.DataFrame(all_fills)
    mechanism_unknown_event_count = sum(
        int(value)
        for counts in symbol_counts.values()
        for key, value in counts.get("mechanism", {}).items()
        if key.startswith("UNKNOWN_")
    )
    coverage_complete = (
        not excluded
        and mechanism_unknown_event_count == 0
        and len(symbol_counts) == len(symbols)
    )
    comparisons = _comparison_receipts(
        events_frame,
        sleeves_frame,
        calendar_dates=calendar_dates[start_ordinal : terminal_ordinal + 1],
        coverage_complete=coverage_complete,
        bootstrap_samples=bootstrap_samples,
    )
    coverage = {
        "schema_version": "position_timing_pattern_coverage_v1",
        "requested_symbol_count": len(symbols),
        "completed_symbol_count": len(symbols) - sum(excluded.values()),
        "coverage_complete": coverage_complete,
        "mechanism_unknown_event_count": mechanism_unknown_event_count,
        "excluded": dict(sorted(excluded.items())),
        "symbols": symbol_counts,
        "evaluation_start": calendar_dates[start_ordinal].isoformat(),
        "evaluation_end": calendar_dates[terminal_ordinal].isoformat(),
        "source_end": end.isoformat(),
    }
    coverage["coverage_sha256"] = canonical_sha256(coverage)
    result_class = RESULT_CLASS
    joint = (
        "SUPPORTED"
        if comparisons["P_MINUS_BUY_AND_HOLD"]["effect_evidence"] == "SUPPORTED"
        and comparisons["P_MINUS_FROZEN_L1"]["effect_evidence"] == "SUPPORTED"
        else "INCONCLUSIVE"
    )
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "result_class": result_class,
        "template_id": template_id,
        "template_sha256": TEMPLATE_BY_ID[template_id].sha256,
        "prototype_contract_sha256": PROTOTYPE_CONTRACT_SHA256,
        "familywise_hypothesis_count": PROTOTYPE_FAMILY_SIZE,
        "comparisons": comparisons,
        "joint_policy_evidence": joint,
        "selected_trial_count": 0,
        "serving_status": "NOT_SERVING_SEPARATE_RUNTIME_DECISION_REQUIRED",
        "coverage_sha256": coverage["coverage_sha256"],
        "event_row_count": len(events_frame),
        "sleeve_day_row_count": len(sleeves_frame),
        "fill_row_count": len(fills_frame),
        "total_realized_fee_cny": (float(fills_frame["fill_fee_cny"].sum()) if not fills_frame.empty else 0.0),
        "parent_order_count": parent_count,
        "additional_friction_bps_per_leg": str(additional_friction_bps),
        "gross_net_note": "NET_INCLUDES_COMPONENTIZED_FEES; GROSS_AND_SENSITIVITY_ARE_MATERIALIZED_BY_EXPLICIT_REPLAY_SCENARIOS",
        "diagnostics": _research_diagnostics(events_frame, sleeves_frame, fills_frame),
        "registry_written": False,
        "current_written": False,
        "research_model_outputs_written": False,
        "serving_model_artifact_written": False,
        "card_written": False,
        "alert_written": False,
        "order_written": False,
        "database_written": False,
        "runtime_written": False,
    }
    return PrototypeReplayResult(events_frame, sleeves_frame, fills_frame, coverage, receipt)


def _prototype_population_identities(result: PrototypeReplayResult) -> Mapping[str, Any]:
    identities: dict[str, Any] = {}
    for comparison in ("E1_MINUS_E0", "X1_MINUS_X0"):
        subset = (
            result.events.loc[result.events["comparison"].eq(comparison)] if not result.events.empty else pd.DataFrame()
        )
        keys = (
            tuple(
                sorted(
                    (str(row.symbol), row.anchor_date.isoformat())
                    for row in subset.loc[:, ["symbol", "anchor_date"]].itertuples(index=False)
                )
            )
            if not subset.empty
            else ()
        )
        identities[comparison] = {"row_count": len(keys), "keys_sha256": canonical_sha256(keys)}
    for comparison in ("P_MINUS_BUY_AND_HOLD", "P_MINUS_FROZEN_L1"):
        subset = (
            result.sleeve_days.loc[result.sleeve_days["comparison"].eq(comparison)]
            if not result.sleeve_days.empty
            else pd.DataFrame()
        )
        keys = (
            tuple(
                sorted(
                    (str(row.symbol), row.valuation_date.isoformat())
                    for row in subset.loc[:, ["symbol", "valuation_date"]].itertuples(index=False)
                )
            )
            if not subset.empty
            else ()
        )
        identities[comparison] = {"row_count": len(keys), "keys_sha256": canonical_sha256(keys)}
    return identities


def _outer_population_identity(frame: pd.DataFrame) -> Mapping[str, Any]:
    if frame.empty or not {"symbol", "valuation_date"}.issubset(frame):
        keys: tuple[tuple[str, str], ...] = ()
    else:
        keys = tuple(
            sorted(
                (str(row.symbol), row.valuation_date.isoformat())
                for row in frame.loc[:, ["symbol", "valuation_date"]].drop_duplicates().itertuples(index=False)
            )
        )
    return {"row_count": len(keys), "keys_sha256": canonical_sha256(keys)}


def _optimizer_contract_sha256() -> str:
    from .pattern_optimizer import OPTIMIZER_CONTRACT_SHA256

    return OPTIMIZER_CONTRACT_SHA256


def _optimizer_contract() -> Mapping[str, Any]:
    from .pattern_optimizer import OPTIMIZER_CONTRACT

    return OPTIMIZER_CONTRACT


def _model_contract_sha256() -> str:
    from .pattern_model import MODEL_CONTRACT_SHA256

    return MODEL_CONTRACT_SHA256


def _model_contract() -> Mapping[str, Any]:
    from .pattern_model import MODEL_CONTRACT

    return MODEL_CONTRACT


def prepare_pattern_request(
    *,
    timing_root: Path,
    repository_root: Path,
    parent_request_path: Path,
    corporate_action_snapshot: Path,
    suspension_snapshot: Path,
) -> Path:
    repository_root = repository_root.resolve()
    source_commit = _clean_repository_commit(repository_root)
    timing_root = timing_root.resolve()
    plan = plan_pattern_population(timing_root=timing_root, parent_request_path=parent_request_path.resolve())
    symbols = tuple(plan["snapshot_symbols"])
    start = date.fromisoformat(plan["population_spec"]["start"])
    end = date.fromisoformat(plan["population_spec"]["end"])
    corporate_scope = _snapshot_scope(
        corporate_action_snapshot.resolve(), expected_symbols=symbols, start=start, end=end
    )
    suspension_scope = _snapshot_scope(suspension_snapshot.resolve(), expected_symbols=symbols, start=start, end=end)
    candidate = DailyCandidate.open(Path(plan["candidate_root"]))
    # Both development and outer populations are consumed by this request;
    # binding only evaluation files would leave model training source mutable.
    coverage = candidate.coverage(symbols)
    corporate_book = CorporateActionBook.open(corporate_action_snapshot.resolve())
    suspension_book = SuspensionSnapshotBook.open(suspension_snapshot.resolve())
    source_code_paths = {
        "pattern_strategy_source": Path(__file__).with_name("pattern_strategy.py"),
        "pattern_research_source": Path(__file__),
        "pattern_optimizer_source": Path(__file__).with_name("pattern_optimizer.py"),
        "pattern_model_source": Path(__file__).with_name("pattern_model.py"),
        "action_value_source": Path(__file__).with_name("action_value.py"),
        "policy_source": Path(__file__).with_name("policy.py"),
    }
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "created_at": datetime.now(TZ).isoformat(),
        "repository_root": repository_root.as_posix(),
        "repository_commit": source_commit,
        "timing_root": timing_root.as_posix(),
        "parent_request": file_reference(parent_request_path.resolve()),
        "candidate_root": candidate.root.as_posix(),
        "training_symbols": plan["training_symbols"],
        "evaluation_symbols": plan["evaluation_symbols"],
        "snapshot_symbols": symbols,
        "population_spec": plan["population_spec"],
        "prior_request_identity": plan["prior_request_identity"],
        "candidate_source_identity": coverage,
        "corporate_action_snapshot": file_reference(corporate_action_snapshot.resolve()),
        "corporate_action_snapshot_sha256": corporate_book.snapshot_sha256,
        "corporate_action_scope": corporate_scope,
        "suspension_snapshot": file_reference(suspension_snapshot.resolve()),
        "suspension_snapshot_sha256": suspension_book.snapshot_sha256,
        "suspension_scope": suspension_scope,
        "source_code": {key: file_reference(path) for key, path in source_code_paths.items()},
        "prototype_contract": PROTOTYPE_CONTRACT,
        "prototype_contract_sha256": PROTOTYPE_CONTRACT_SHA256,
        "optimizer_contract": _optimizer_contract(),
        "optimizer_contract_sha256": _optimizer_contract_sha256(),
        "model_contract": _model_contract(),
        "model_contract_sha256": _model_contract_sha256(),
        "preregistered_family_count": PREREGISTERED_FAMILY_COUNT,
        "total_formal_comparison_count": TOTAL_FORMAL_COMPARISON_COUNT,
        "result_class": RESULT_CLASS,
        "registry_write": False,
        "current_write": False,
        "research_model_outputs_write": True,
        "serving_model_artifact_write": False,
        "card_write": False,
        "alert_write": False,
        "order_write": False,
        "database_write": False,
        "runtime_write": False,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = timing_root / "research" / ARTIFACT_FOLDER / "requests" / f"{request['request_sha256']}.json"
    PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(request))
    return path


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_REQUEST_UNAVAILABLE") from exc
    identity = {key: value for key, value in request.items() if key != "request_sha256"}
    false_flags = (
        "registry_write",
        "current_write",
        "serving_model_artifact_write",
        "card_write",
        "alert_write",
        "order_write",
        "database_write",
        "runtime_write",
    )
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("request_sha256") != canonical_sha256(identity)
        or request.get("prototype_contract_sha256") != PROTOTYPE_CONTRACT_SHA256
        or canonical_sha256(request.get("prototype_contract")) != PROTOTYPE_CONTRACT_SHA256
        or request.get("optimizer_contract_sha256") != _optimizer_contract_sha256()
        or canonical_sha256(request.get("optimizer_contract")) != _optimizer_contract_sha256()
        or request.get("model_contract_sha256") != _model_contract_sha256()
        or canonical_sha256(request.get("model_contract")) != _model_contract_sha256()
        or request.get("preregistered_family_count") != PREREGISTERED_FAMILY_COUNT
        or request.get("total_formal_comparison_count") != TOTAL_FORMAL_COMPARISON_COUNT
        or request.get("result_class") != RESULT_CLASS
        or request.get("research_model_outputs_write") is not True
        or len(str(request.get("corporate_action_snapshot_sha256", ""))) != 64
        or len(str(request.get("suspension_snapshot_sha256", ""))) != 64
        or any(request.get(flag) is not False for flag in false_flags)
    ):
        raise ActionValueError("PATTERN_REQUEST_IDENTITY_MISMATCH")
    return request


def _without_path(reference: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in reference.items() if key != "path"}


def _manifest(root: Path, receipt: Mapping[str, Any]) -> Mapping[str, Any]:
    names = tuple(
        path.relative_to(root).as_posix()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    )
    payload = {
        "schema_version": BUNDLE_SCHEMA,
        "request_sha256": receipt["request_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "files": {name: _without_path(file_reference(root / name)) for name in names},
    }
    return {**payload, "manifest_sha256": canonical_sha256(payload)}


def _publish_bundle(
    bundle: Path,
    *,
    request: Mapping[str, Any],
    result: PrototypeReplayResult,
    receipt: Mapping[str, Any],
    additional_frames: Mapping[str, pd.DataFrame] | None = None,
    additional_json: Mapping[str, Mapping[str, Any]] | None = None,
    research_models: Mapping[str, Sequence[Any]] | None = None,
) -> None:
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        PositionTimingArtifactStore._publish_immutable(staging / "request.json", canonical_json_bytes(request))
        PositionTimingArtifactStore._publish_immutable(staging / "coverage.json", canonical_json_bytes(result.coverage))
        result.events.to_parquet(staging / "events.parquet", index=False)
        result.sleeve_days.to_parquet(staging / "sleeve_days.parquet", index=False)
        result.fills.to_parquet(staging / "fills.parquet", index=False)
        for name, frame in (additional_frames or {}).items():
            if Path(name).name != name or not name.endswith(".parquet"):
                raise ActionValueError("PATTERN_ADDITIONAL_FRAME_NAME_INVALID", name=name)
            frame.to_parquet(staging / name, index=False)
        for name, payload in (additional_json or {}).items():
            if Path(name).name != name or not name.endswith(".json"):
                raise ActionValueError("PATTERN_ADDITIONAL_JSON_NAME_INVALID", name=name)
            PositionTimingArtifactStore._publish_immutable(staging / name, canonical_json_bytes(payload))
        for feature_set, models in (research_models or {}).items():
            for model in models:
                model_root = staging / "models" / feature_set / model.metadata["model_sha256"]
                PositionTimingArtifactStore._publish_immutable(
                    model_root / "manifest.json", canonical_json_bytes(model.metadata)
                )
                for head, booster in model.boosters.items():
                    content = booster.model_to_string().encode("utf-8")
                    if hashlib.sha256(content).hexdigest() != model.metadata["heads"][head]["text_sha256"]:
                        raise ActionValueError("PATTERN_MODEL_TEXT_IDENTITY_MISMATCH")
                    PositionTimingArtifactStore._publish_immutable(model_root / f"{head}.txt", content)
        PositionTimingArtifactStore._publish_immutable(staging / "receipt.json", canonical_json_bytes(receipt))
        PositionTimingArtifactStore._publish_immutable(
            staging / "manifest.json", canonical_json_bytes(_manifest(staging, receipt))
        )
        lock = bundle.parents[3] / "locks" / f"pattern-{bundle.name}.lock"
        with _exclusive_file_lock(lock):
            if bundle.exists():
                inspect_pattern_bundle(bundle)
            else:
                os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def inspect_pattern_bundle(bundle: Path) -> Mapping[str, Any]:
    bundle = bundle.resolve()
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_BUNDLE_UNAVAILABLE") from exc
    request = _load_request(bundle / "request.json")
    manifest_identity = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    receipt_identity = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    evolution = receipt.get("evolution") or {}
    false_flags = (
        "registry_written",
        "current_written",
        "serving_model_artifact_written",
        "card_written",
        "alert_written",
        "order_written",
        "database_written",
        "runtime_written",
    )
    if (
        manifest.get("schema_version") != BUNDLE_SCHEMA
        or manifest.get("manifest_sha256") != canonical_sha256(manifest_identity)
        or receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("pipeline_id") != PIPELINE_ID
        or receipt.get("receipt_sha256") != canonical_sha256(receipt_identity)
        or receipt.get("request_sha256") != request["request_sha256"]
        or receipt.get("result_class") != RESULT_CLASS
        or receipt.get("familywise_hypothesis_count") != PROTOTYPE_FAMILY_SIZE
        or receipt.get("preregistered_family_count") != PREREGISTERED_FAMILY_COUNT
        or receipt.get("total_formal_comparison_count") != TOTAL_FORMAL_COMPARISON_COUNT
        or receipt.get("selected_trial_count") != 0
        or receipt.get("research_model_outputs_written") is not True
        or evolution.get("familywise_hypothesis_count") != EVOLUTION_FAMILY_SIZE
        or evolution.get("formal_comparison_count") != EVOLUTION_FAMILY_SIZE
        or evolution.get("internal_template_candidate_count") != 8
        or evolution.get("model_feature_set_count") != 2
        or evolution.get("model_head_count_per_feature_set") != 2
        or set((evolution.get("comparisons") or {}))
        != {
            "Q_MINUS_P",
            "Q_MINUS_BUY_AND_HOLD",
            "ENHANCED_MINUS_CORE",
            "ENHANCED_MINUS_P",
            "ENHANCED_MINUS_BUY_AND_HOLD",
        }
        or evolution.get("receipt_sha256")
        != canonical_sha256({key: value for key, value in evolution.items() if key != "receipt_sha256"})
        or any(receipt.get(flag) is not False for flag in false_flags)
        or manifest.get("request_sha256") != request["request_sha256"]
        or manifest.get("receipt_sha256") != receipt["receipt_sha256"]
        or bundle.name != request["request_sha256"]
    ):
        raise ActionValueError("PATTERN_BUNDLE_IDENTITY_MISMATCH")
    expected_names = set(manifest.get("files", {})) | {"manifest.json"}
    actual_names = {
        path.relative_to(bundle).as_posix()
        for path in bundle.rglob("*")
        if path.is_file()
    }
    if actual_names != expected_names:
        raise ActionValueError(
            "PATTERN_BUNDLE_FILE_SET_MISMATCH",
            missing=sorted(expected_names - actual_names),
            unexpected=sorted(actual_names - expected_names),
        )
    for name, expected in manifest.get("files", {}).items():
        if _without_path(file_reference(bundle / name)) != expected:
            raise ActionValueError("PATTERN_BUNDLE_FILE_IDENTITY_MISMATCH", file=name)
    return {"manifest": manifest, "request": request, "receipt": receipt}


def run_pattern_request(request_path: Path) -> Mapping[str, Any]:
    from .pattern_model import (
        build_pattern_model_rows,
        historical_model_event_predictions,
        model_comparisons,
        replay_model_outer,
        walk_forward_pattern_models,
    )
    from .pattern_optimizer import (
        build_template_development_paths,
        monthly_template_schedule,
        optimizer_comparisons,
        replay_optimizer_outer,
    )

    request = _load_request(request_path.resolve())
    repository = Path(request["repository_root"])
    if _clean_repository_commit(repository) != request["repository_commit"]:
        raise ActionValueError("PATTERN_CODE_IDENTITY_MISMATCH")
    timing_root = Path(request["timing_root"]).resolve()
    bundle = timing_root / "research" / ARTIFACT_FOLDER / "bundles" / request["request_sha256"]
    if bundle.exists():
        return {"status": "ALREADY_MATERIALIZED", "bundle": bundle.as_posix(), **inspect_pattern_bundle(bundle)}
    for reference in (
        request["parent_request"],
        request["corporate_action_snapshot"],
        request["suspension_snapshot"],
        *request["source_code"].values(),
    ):
        if file_reference(Path(reference["path"])) != reference:
            raise ActionValueError("PATTERN_BOUND_SOURCE_CHANGED", path=reference["path"])
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    observed_source = candidate.coverage(tuple(request["snapshot_symbols"]))
    if observed_source["source_sha256"] != request["candidate_source_identity"]["source_sha256"]:
        raise ActionValueError("PATTERN_CANDIDATE_SOURCE_CHANGED")
    observed_prior = prior_timing_request_population(
        timing_root / "research",
        exclude_request_sha256=request["request_sha256"],
    )
    if observed_prior["aggregate_sha256"] != request["prior_request_identity"]["aggregate_sha256"]:
        raise ActionValueError("PATTERN_PRIOR_REQUEST_SET_CHANGED")
    evaluation_symbols = tuple(request["evaluation_symbols"])
    expected = select_pattern_evaluation_symbols(
        candidate.symbols,
        forbidden_symbols=tuple(request["prior_request_identity"]["forbidden_symbols"])
        + tuple(request["training_symbols"]),
    )
    if evaluation_symbols != expected:
        raise ActionValueError("PATTERN_EVALUATION_POPULATION_DRIFT")
    corporate_actions = CorporateActionBook.open(Path(request["corporate_action_snapshot"]["path"]))
    suspension_book = SuspensionSnapshotBook.open(Path(request["suspension_snapshot"]["path"]))
    if (
        corporate_actions.snapshot_sha256 != request["corporate_action_snapshot_sha256"]
        or suspension_book.snapshot_sha256 != request["suspension_snapshot_sha256"]
    ):
        raise ActionValueError("PATTERN_SNAPSHOT_IDENTITY_DRIFT")
    candidate = suspension_book.apply(candidate, snapshot_path=Path(request["suspension_snapshot"]["path"]))
    cached = PatternCandidateCache(candidate)
    start = date.fromisoformat(request["population_spec"]["start"])
    end = date.fromisoformat(request["population_spec"]["end"])
    result = replay_prototype(
        cached,
        symbols=evaluation_symbols,
        corporate_actions=corporate_actions,
        start=start,
        end=end,
    )
    primary_populations = _prototype_population_identities(result)

    sensitivity: dict[str, Any] = {
        "PARENT_ORDERS_1": {
            "parent_count": 1,
            "additional_friction_bps_per_leg": "0",
            "comparisons": {
                name: {
                    "net_point_bps": values["inference"]["point_bps"],
                    "gross_point_bps": values["gross_point_bps"],
                    "adjusted_lower_bps": values["inference"]["lower_bps"],
                    "adjusted_upper_bps": values["inference"]["upper_bps"],
                    "effect_evidence": values["effect_evidence"],
                }
                for name, values in result.receipt["comparisons"].items()
            },
            "total_realized_fee_cny": result.receipt["total_realized_fee_cny"],
            "coverage_complete": result.coverage["coverage_complete"],
            "comparison_populations": primary_populations,
        }
    }
    for scenario_id, parent_count, friction in (
        ("PARENT_ORDERS_2", 2, Decimal(0)),
        ("PARENT_ORDERS_3", 3, Decimal(0)),
        ("FRICTION_5_BPS_PER_LEG", 1, Decimal(5)),
        ("FRICTION_10_BPS_PER_LEG", 1, Decimal(10)),
    ):
        scenario = replay_prototype(
            cached,
            symbols=evaluation_symbols,
            corporate_actions=corporate_actions,
            start=start,
            end=end,
            parent_count=parent_count,
            additional_friction_bps=friction,
        )
        sensitivity[scenario_id] = {
            "parent_count": parent_count,
            "additional_friction_bps_per_leg": str(friction),
            "comparisons": {
                name: {
                    "net_point_bps": values["inference"]["point_bps"],
                    "gross_point_bps": values["gross_point_bps"],
                    "adjusted_lower_bps": values["inference"]["lower_bps"],
                    "adjusted_upper_bps": values["inference"]["upper_bps"],
                    "effect_evidence": values["effect_evidence"],
                }
                for name, values in scenario.receipt["comparisons"].items()
            },
            "total_realized_fee_cny": scenario.receipt["total_realized_fee_cny"],
            "coverage_sha256": scenario.coverage["coverage_sha256"],
            "coverage_complete": scenario.coverage["coverage_complete"],
            "excluded": scenario.coverage["excluded"],
            "comparison_populations": _prototype_population_identities(scenario),
        }
    cost_assumption_sensitive = {}
    for name, primary in result.receipt["comparisons"].items():
        sensitive_scenarios = []
        unavailable_scenarios = []
        if primary["effect_evidence"] == "SUPPORTED":
            for scenario_id in ("PARENT_ORDERS_2", "PARENT_ORDERS_3"):
                same_population = (
                    sensitivity[scenario_id]["coverage_complete"]
                    and sensitivity[scenario_id]["comparison_populations"][name] == primary_populations[name]
                )
                if not same_population:
                    unavailable_scenarios.append(scenario_id)
                    continue
                lower = sensitivity[scenario_id]["comparisons"][name]["adjusted_lower_bps"]
                if lower is None or lower <= 0:
                    sensitive_scenarios.append(scenario_id)
        cost_assumption_sensitive[name] = {
            "cost_assumption_sensitive": bool(sensitive_scenarios),
            "sensitive_scenarios": sensitive_scenarios,
            "unavailable_not_same_population_scenarios": unavailable_scenarios,
            "classification_unchanged": True,
        }

    training_symbols = tuple(request["training_symbols"])
    development_days, optimizer_development_coverage = build_template_development_paths(
        cached,
        training_symbols=training_symbols,
        corporate_actions=corporate_actions,
        start=start,
        end=end,
    )
    calendar_dates = tuple(timestamp.date() for timestamp in cached.calendar)
    evaluation_start = calendar_dates[calendar_dates.index(start) + INITIAL_TRAINING_SESSIONS]
    evaluation_end = calendar_dates[calendar_dates.index(end) - TERMINAL_MAX_DEFER]
    schedule = monthly_template_schedule(
        development_days,
        calendar_dates=calendar_dates,
        evaluation_start=evaluation_start,
        evaluation_end=evaluation_end,
        development_coverage_complete=bool(optimizer_development_coverage["coverage_complete"]),
    )
    optimizer_outer, optimizer_outer_coverage = replay_optimizer_outer(
        cached,
        evaluation_symbols=evaluation_symbols,
        corporate_actions=corporate_actions,
        start=start,
        end=end,
        schedule=schedule,
    )
    optimizer_evidence_coverage_complete = bool(
        optimizer_development_coverage["coverage_complete"]
        and optimizer_outer_coverage["coverage_complete"]
    )
    optimizer_evidence = (
        optimizer_comparisons(
            optimizer_outer,
            coverage_complete=optimizer_evidence_coverage_complete,
        )
        if not optimizer_outer.empty
        else {
            name: {
                "unit": "BPS_PER_TRADING_DAY",
                "inference": {"point_bps": None, "lower_bps": None, "upper_bps": None, "reason_code": "NO_OUTER_ROWS"},
                "effect_evidence": "INCONCLUSIVE",
            }
            for name in ("Q_MINUS_P", "Q_MINUS_BUY_AND_HOLD")
        }
    )

    model_rows, model_training_coverage = build_pattern_model_rows(
        cached,
        symbols=training_symbols,
        corporate_actions=corporate_actions,
        start=start,
        end=end,
    )
    walk = walk_forward_pattern_models(
        model_rows,
        calendar_dates=calendar_dates,
        source_sha256=request["candidate_source_identity"]["source_sha256"],
        request_sha256=request["request_sha256"],
        source_commit=request["repository_commit"],
    )
    evaluation_model_rows, model_evaluation_coverage = build_pattern_model_rows(
        cached,
        symbols=evaluation_symbols,
        corporate_actions=corporate_actions,
        start=start,
        end=end,
    )
    model_outer = pd.DataFrame()
    historical_predictions = pd.DataFrame()
    model_outer_coverage: Mapping[str, Any] = {
        "coverage_complete": False,
        "reason_code": "MODEL_NOT_ESTIMABLE",
    }
    model_evidence = {
        name: {
            "unit": "BPS_PER_TRADING_DAY",
            "inference": {
                "point_bps": None,
                "lower_bps": None,
                "upper_bps": None,
                "reason_code": "MODEL_NOT_ESTIMABLE",
            },
            "effect_evidence": "INCONCLUSIVE",
        }
        for name in ("ENHANCED_MINUS_CORE", "ENHANCED_MINUS_P", "ENHANCED_MINUS_BUY_AND_HOLD")
    }
    model_input_coverage_complete = bool(
        model_training_coverage["coverage_complete"]
        and model_evaluation_coverage["coverage_complete"]
    )
    if all(walk.models.get(key) for key in ("CORE_ONLY", "CORE_PLUS_PATTERN_V1")):
        model_outer, model_outer_coverage = replay_model_outer(
            cached,
            evaluation_symbols=evaluation_symbols,
            corporate_actions=corporate_actions,
            start=start,
            end=end,
            models=walk.models,
        )
        if not model_outer.empty:
            model_evidence = model_comparisons(
                model_outer,
                coverage_complete=bool(
                    model_input_coverage_complete and model_outer_coverage["coverage_complete"]
                ),
            )
        historical_predictions = historical_model_event_predictions(
            evaluation_model_rows,
            models=walk.models,
        )

    if not historical_predictions.empty:
        historical_predictions = historical_predictions.assign(
            _case_order=[
                hashlib.sha256(
                    f"{row.symbol}|{row.decision_as_of.isoformat()}|{row.objective}".encode("utf-8")
                ).hexdigest()
                for row in historical_predictions.itertuples(index=False)
            ]
        )
        cases = (
            historical_predictions.sort_values("_case_order")
            .groupby("objective", group_keys=False)
            .head(20)
            .drop(columns="_case_order")
            .reset_index(drop=True)
        )
    else:
        cases = historical_predictions.copy()

    evolution_comparisons = {**optimizer_evidence, **model_evidence}
    base_evolution_populations = {
        "Q_MINUS_P": _outer_population_identity(optimizer_outer),
        "Q_MINUS_BUY_AND_HOLD": _outer_population_identity(optimizer_outer),
        "ENHANCED_MINUS_CORE": _outer_population_identity(model_outer),
        "ENHANCED_MINUS_P": _outer_population_identity(model_outer),
        "ENHANCED_MINUS_BUY_AND_HOLD": _outer_population_identity(model_outer),
    }
    evolution_cost_sensitivity: dict[str, Any] = {}
    evolution_sensitivity_frames: dict[str, pd.DataFrame] = {}
    for parent_count in (2, 3):
        scenario_id = f"PARENT_ORDERS_{parent_count}"
        optimizer_scenario_outer, optimizer_scenario_coverage = replay_optimizer_outer(
            cached,
            evaluation_symbols=evaluation_symbols,
            corporate_actions=corporate_actions,
            start=start,
            end=end,
            schedule=schedule,
            parent_count=parent_count,
        )
        optimizer_scenario_evidence_coverage_complete = bool(
            optimizer_development_coverage["coverage_complete"]
            and optimizer_scenario_coverage["coverage_complete"]
        )
        optimizer_scenario_evidence = (
            optimizer_comparisons(
                optimizer_scenario_outer,
                coverage_complete=optimizer_scenario_evidence_coverage_complete,
            )
            if not optimizer_scenario_outer.empty
            else {
                name: {
                    "unit": "BPS_PER_TRADING_DAY",
                    "inference": {
                        "point_bps": None,
                        "lower_bps": None,
                        "upper_bps": None,
                        "reason_code": "NO_OUTER_ROWS",
                    },
                    "effect_evidence": "INCONCLUSIVE",
                }
                for name in ("Q_MINUS_P", "Q_MINUS_BUY_AND_HOLD")
            }
        )
        model_scenario_outer = pd.DataFrame()
        model_scenario_coverage: Mapping[str, Any] = {
            "coverage_complete": False,
            "reason_code": "MODEL_NOT_ESTIMABLE",
        }
        model_scenario_evidence = {
            name: {
                "unit": "BPS_PER_TRADING_DAY",
                "inference": {
                    "point_bps": None,
                    "lower_bps": None,
                    "upper_bps": None,
                    "reason_code": "MODEL_NOT_ESTIMABLE",
                },
                "effect_evidence": "INCONCLUSIVE",
            }
            for name in ("ENHANCED_MINUS_CORE", "ENHANCED_MINUS_P", "ENHANCED_MINUS_BUY_AND_HOLD")
        }
        if all(walk.models.get(key) for key in ("CORE_ONLY", "CORE_PLUS_PATTERN_V1")):
            model_scenario_outer, model_scenario_coverage = replay_model_outer(
                cached,
                evaluation_symbols=evaluation_symbols,
                corporate_actions=corporate_actions,
                start=start,
                end=end,
                models=walk.models,
                parent_count=parent_count,
            )
            if not model_scenario_outer.empty:
                model_scenario_evidence = model_comparisons(
                    model_scenario_outer,
                    coverage_complete=bool(
                        model_input_coverage_complete and model_scenario_coverage["coverage_complete"]
                    ),
                )
        scenario_comparisons = {**optimizer_scenario_evidence, **model_scenario_evidence}
        optimizer_population = _outer_population_identity(optimizer_scenario_outer)
        model_population = _outer_population_identity(model_scenario_outer)
        scenario_populations = {
            "Q_MINUS_P": optimizer_population,
            "Q_MINUS_BUY_AND_HOLD": optimizer_population,
            "ENHANCED_MINUS_CORE": model_population,
            "ENHANCED_MINUS_P": model_population,
            "ENHANCED_MINUS_BUY_AND_HOLD": model_population,
        }
        same_population = {
            name: scenario_populations[name] == base_evolution_populations[name]
            and (
                bool(optimizer_scenario_evidence_coverage_complete)
                if name.startswith("Q_")
                else bool(model_input_coverage_complete and model_scenario_coverage["coverage_complete"])
            )
            for name in evolution_comparisons
        }
        evolution_cost_sensitivity[scenario_id] = {
            "parent_count": parent_count,
            "policy_and_model_identity": "BASE_FROZEN_NO_RESELECTION_NO_RETRAINING",
            "comparisons": scenario_comparisons,
            "comparison_populations": scenario_populations,
            "same_population_as_base": same_population,
            "optimizer_outer_coverage": optimizer_scenario_coverage,
            "model_outer_coverage": model_scenario_coverage,
        }
        evolution_sensitivity_frames[f"optimizer_outer_parent_orders_{parent_count}.parquet"] = optimizer_scenario_outer
        evolution_sensitivity_frames[f"model_outer_parent_orders_{parent_count}.parquet"] = model_scenario_outer

    evolution_cost_flags: dict[str, Any] = {}
    for name, primary in evolution_comparisons.items():
        sensitive_scenarios: list[str] = []
        unavailable_scenarios: list[str] = []
        for scenario_id, scenario in evolution_cost_sensitivity.items():
            if not scenario["same_population_as_base"][name]:
                unavailable_scenarios.append(scenario_id)
                continue
            if primary["effect_evidence"] == "SUPPORTED":
                lower = scenario["comparisons"][name]["inference"]["lower_bps"]
                if lower is None or lower <= 0:
                    sensitive_scenarios.append(scenario_id)
        evolution_cost_flags[name] = {
            "cost_assumption_sensitive": bool(sensitive_scenarios),
            "sensitive_scenarios": sensitive_scenarios,
            "unavailable_not_same_population_scenarios": unavailable_scenarios,
            "classification_unchanged": True,
        }
    evolution_receipt = {
        "schema_version": "position_timing_pattern_evolution_receipt_v1",
        "optimizer_contract_sha256": request["optimizer_contract_sha256"],
        "model_contract_sha256": request["model_contract_sha256"],
        "familywise_hypothesis_count": EVOLUTION_FAMILY_SIZE,
        "formal_comparison_count": EVOLUTION_FAMILY_SIZE,
        "internal_template_candidate_count": 8,
        "optimizer_schedule_month_count": len(schedule),
        "optimizer_selected_from_history_month_count": int(
            schedule["status"].eq("SELECTED_FROM_PRIOR_COMPLETED_12_MONTHS").sum()
        ),
        "model_feature_set_count": 2,
        "model_head_count_per_feature_set": 2,
        "model_fit_attempt_count": walk.receipt["fit_attempt_count"],
        "model_fit_success_count": walk.receipt["fit_success_count"],
        "comparisons": evolution_comparisons,
        "cost_sensitivity": evolution_cost_sensitivity,
        "cost_assumption_sensitivity_flags": evolution_cost_flags,
        "optimizer_development_coverage_sha256": optimizer_development_coverage["coverage_sha256"],
        "optimizer_outer_coverage": optimizer_outer_coverage,
        "optimizer_evidence_coverage_complete": optimizer_evidence_coverage_complete,
        "model_training_coverage_sha256": model_training_coverage["coverage_sha256"],
        "model_evaluation_coverage_sha256": model_evaluation_coverage["coverage_sha256"],
        "model_outer_coverage": model_outer_coverage,
        "model_input_coverage_complete": model_input_coverage_complete,
        "model_evidence_coverage_complete": bool(
            model_input_coverage_complete and model_outer_coverage["coverage_complete"]
        ),
        "model_fit_receipt_sha256": walk.receipt["receipt_sha256"],
        "selected_trial_count": 0,
        "interpretation": "BOUNDED_EXPLORATORY_COMPARISONS_NOT_GLOBAL_OPTIMUM_NOT_SERVING",
    }
    evolution_receipt["receipt_sha256"] = canonical_sha256(evolution_receipt)
    receipt = {
        **result.receipt,
        "request_sha256": request["request_sha256"],
        "preregistered_family_count": PREREGISTERED_FAMILY_COUNT,
        "total_formal_comparison_count": TOTAL_FORMAL_COMPARISON_COUNT,
        "repository_commit": request["repository_commit"],
        "created_at": datetime.now(TZ).isoformat(),
        "source_sha256": request["candidate_source_identity"]["source_sha256"],
        "corporate_action_snapshot_sha256": corporate_actions.snapshot_sha256,
        "suspension_snapshot_sha256": suspension_book.snapshot_sha256,
        "research_model_outputs_written": True,
        "cost_sensitivity": sensitivity,
        "cost_assumption_sensitivity_flags": cost_assumption_sensitive,
        "evolution": evolution_receipt,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    _publish_bundle(
        bundle,
        request=request,
        result=result,
        receipt=receipt,
        additional_frames={
            "optimizer_development_days.parquet": development_days,
            "optimizer_schedule.parquet": schedule.drop(columns="all_scores"),
            "optimizer_outer_days.parquet": optimizer_outer,
            "model_training_rows.parquet": model_rows,
            "model_fit_log.parquet": walk.fit_log,
            "model_evaluation_rows.parquet": evaluation_model_rows,
            "model_outer_days.parquet": model_outer,
            "historical_predictions.parquet": historical_predictions.drop(columns=["_case_order"], errors="ignore"),
            "historical_cases.parquet": cases,
            **evolution_sensitivity_frames,
        },
        additional_json={
            "optimizer_development_coverage.json": optimizer_development_coverage,
            "model_training_coverage.json": model_training_coverage,
            "model_evaluation_coverage.json": model_evaluation_coverage,
            "evolution_receipt.json": evolution_receipt,
        },
        research_models=walk.models,
    )
    return {"status": "MATERIALIZED", "bundle": bundle.as_posix(), **inspect_pattern_bundle(bundle)}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    population = sub.add_parser("population")
    population.add_argument("--timing-root", type=Path, required=True)
    population.add_argument("--parent-request", type=Path, required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--timing-root", type=Path, required=True)
    prepare.add_argument("--repository-root", type=Path, required=True)
    prepare.add_argument("--parent-request", type=Path, required=True)
    prepare.add_argument("--corporate-action-snapshot", type=Path, required=True)
    prepare.add_argument("--suspension-snapshot", type=Path, required=True)
    run = sub.add_parser("run")
    run.add_argument("--request", type=Path, required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--bundle", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "population":
        payload = plan_pattern_population(timing_root=args.timing_root, parent_request_path=args.parent_request)
    elif args.command == "prepare":
        payload = {
            "request": prepare_pattern_request(
                timing_root=args.timing_root,
                repository_root=args.repository_root,
                parent_request_path=args.parent_request,
                corporate_action_snapshot=args.corporate_action_snapshot,
                suspension_snapshot=args.suspension_snapshot,
            ).as_posix()
        }
    elif args.command == "run":
        payload = run_pattern_request(args.request)
    else:
        payload = inspect_pattern_bundle(args.bundle)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
