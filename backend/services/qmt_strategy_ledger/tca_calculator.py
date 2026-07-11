"""Pure Decimal calculator for MiniQMT Phase 0A execution TCA.

This module has no database, broker, clock, or market-data side effects.  It
calculates only from an explicit immutable evidence set supplied by the rebuild
service.  Missing evidence remains ``None`` with a loud validity reason; it is
never converted to a synthetic zero.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_EVEN, ROUND_HALF_UP, localcontext
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .tca_models import canonical_tca_manifest_sha256 as canonical_json_sha256


CALCULATOR_VERSION = "miniqmt_execution_tca_calculator_v1"
FORMULA_VERSION = "miniqmt_execution_tca_formula_v1"
MARK_POLICY_VERSION = "miniqmt_execution_tca_mark_selector_v1"
FEE_ALLOCATION_VERSION = "miniqmt_execution_tca_fee_allocation_v1"
_EIGHT_PLACES = Decimal("0.00000001")
_TWELVE_PLACES = Decimal("0.000000000001")
_BPS = Decimal("10000")
_FEE_COMPONENTS = frozenset({"commission", "exchange_handling", "transfer", "stamp_tax", "other"})
_CHINA_TZ = ZoneInfo("Asia/Shanghai")


class TcaCalculationError(ValueError):
    """A stable loud domain failure suitable for a FAILED rebuild receipt."""

    def __init__(self, reason_code: str, stage: str, **context: Any) -> None:
        self.reason_code = reason_code
        self.stage = stage
        self.context = context
        super().__init__(
            f"reason_code={reason_code}, stage={stage}, context={context}"
        )


@dataclass(frozen=True, slots=True)
class TcaFill:
    trade_id: str
    order_id: str
    price: Decimal
    quantity: int
    trade_time: datetime | None
    canonical_fact_sha256: str
    observation_ids: Mapping[str, str] = field(default_factory=dict)
    observation_hashes: Mapping[str, str] = field(default_factory=dict)
    actual_fee_cny: Decimal | None = None
    fee_provenance: str = "MISSING"
    actual_fee_scope: str = "MISSING"
    child_receipt_mid: Decimal | None = None
    markout_mid_by_horizon_ms: Mapping[int, Decimal | None] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.trade_id or not self.order_id:
            raise TcaCalculationError("ADAPTIVE_IS_TCA_TRADE_IDENTITY_MISSING", "tca_calculator_input")
        if self.price <= 0 or self.quantity <= 0:
            raise TcaCalculationError(
                "ADAPTIVE_IS_TCA_TRADE_UNIT_INVALID",
                "tca_calculator_input",
                trade_id=self.trade_id,
            )
        if self.fee_provenance not in {"ACTUAL", "MISSING", "UNKNOWN_LEGACY"}:
            raise TcaCalculationError(
                "ADAPTIVE_IS_TCA_FEE_PROVENANCE_INVALID",
                "tca_calculator_input",
                trade_id=self.trade_id,
                fee_provenance=self.fee_provenance,
            )
        if self.actual_fee_scope not in {"TRADE_LEVEL", "ORDER_LEVEL", "MISSING"}:
            raise TcaCalculationError(
                "ADAPTIVE_IS_TCA_FEE_SCOPE_INVALID",
                "tca_calculator_input",
                trade_id=self.trade_id,
                actual_fee_scope=self.actual_fee_scope,
            )

    @property
    def amount(self) -> Decimal:
        return self.price * Decimal(self.quantity)


@dataclass(frozen=True, slots=True)
class QuoteCandidate:
    evidence_id: str
    symbol: str
    market_time: datetime | None
    received_at: datetime | None
    bid_price_1: Decimal | None
    ask_price_1: Decimal | None
    last_price: Decimal | None = None
    quote_source: str | None = None
    raw_quote_sha256: str | None = None
    market_phase: str | None = None
    stock_status: str | None = None


@dataclass(frozen=True, slots=True)
class SelectedMark:
    mark_type: str
    target_time: datetime
    quality: str
    candidate: QuoteCandidate | None
    mid_price: Decimal | None
    age_or_lag_ms: int | None
    evidence_sha256: str

    def as_manifest(self) -> dict[str, Any]:
        candidate = self.candidate
        return {
            "mark_type": self.mark_type,
            "target_time": self.target_time,
            "quality": self.quality,
            "evidence_id": candidate.evidence_id if candidate else None,
            "market_time": candidate.market_time if candidate else None,
            "received_at": candidate.received_at if candidate else None,
            "bid_price_1": candidate.bid_price_1 if candidate else None,
            "ask_price_1": candidate.ask_price_1 if candidate else None,
            "last_price": candidate.last_price if candidate else None,
            "mid_price": self.mid_price,
            "age_or_lag_ms": self.age_or_lag_ms,
            "quote_source": candidate.quote_source if candidate else None,
            "raw_quote_sha256": candidate.raw_quote_sha256 if candidate else None,
            "market_phase": candidate.market_phase if candidate else None,
            "stock_status": candidate.stock_status if candidate else None,
            "mark_policy_version": MARK_POLICY_VERSION,
        }


@dataclass(frozen=True, slots=True)
class FeeAllocation:
    total_cny: Decimal | None
    quality: str
    breakdown: Mapping[str, Decimal]
    by_trade: Mapping[str, Mapping[str, Decimal]]
    policy_sha256: str | None
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class TcaCalculationInput:
    parent_intent_id: str
    trade_date: date
    side: str
    eligible_quantity: int | None
    decision_price: Decimal | None
    arrival_price: Decimal | None
    deadline: datetime | None
    as_of_time: datetime
    snapshot_kind: str
    fills: tuple[TcaFill, ...]
    deadline_mark: SelectedMark | None = None
    estimated_fee_policy: Mapping[str, Any] | None = None
    terminal_as_of: datetime | None = None
    reconciliation_run_id: str | None = None
    finality_satisfied: bool = False
    residual_reason: str = "UNKNOWN"
    residual_executability_class: str = "UNKNOWN"
    decimal_tolerance: Decimal = _EIGHT_PLACES


@dataclass(frozen=True, slots=True)
class TcaCalculationResult:
    values: Mapping[str, Any]
    canonical_input_sha256: str
    canonical_output_sha256: str


def select_mark(
    *,
    candidates: Sequence[QuoteCandidate],
    symbol: str,
    mark_type: str,
    target_time: datetime,
    max_distance_ms: int,
    trade_date: date,
    session_ended: bool = False,
    clock_skew_tolerance_ms: int = 1_000,
) -> SelectedMark:
    """Select a deadline/backward or markout/forward BBO from durable evidence."""

    if mark_type not in {"DEADLINE", "CHILD_RECEIPT", "FILL_MARKOUT_60S", "FILL_MARKOUT_300S", "FILL_MARKOUT_900S"}:
        raise TcaCalculationError("ADAPTIVE_IS_TCA_MARK_TYPE_INVALID", "tca_mark_select", mark_type=mark_type)
    if max_distance_ms < 0:
        raise TcaCalculationError("ADAPTIVE_IS_TCA_MARK_DISTANCE_INVALID", "tca_mark_select")
    scoped = [item for item in candidates if item.symbol == symbol]
    timed = [item for item in scoped if item.market_time is not None]
    if not scoped:
        return _missing_mark(mark_type, target_time, "MARKET_SESSION_ENDED" if session_ended else "MISSING")
    if not timed:
        return _missing_mark(mark_type, target_time, "MISSING_TIME")

    if mark_type in {"DEADLINE", "CHILD_RECEIPT"}:
        eligible = [item for item in timed if item.market_time <= target_time]
        if not eligible:
            return _missing_mark(mark_type, target_time, "FUTURE_SKEW")
        candidate = max(eligible, key=lambda item: (item.market_time, item.evidence_id))
        distance_ms = int((target_time - candidate.market_time).total_seconds() * 1000)
    else:
        eligible = [
            item
            for item in timed
            if item.market_time.astimezone(_CHINA_TZ).date() == trade_date and item.market_time >= target_time
        ]
        if not eligible:
            return _missing_mark(mark_type, target_time, "MARKET_SESSION_ENDED" if session_ended else "MISSING")
        candidate = min(eligible, key=lambda item: (item.market_time, item.evidence_id))
        distance_ms = int((candidate.market_time - target_time).total_seconds() * 1000)

    bid, ask = candidate.bid_price_1, candidate.ask_price_1
    clock_skewed = bool(
        candidate.received_at is not None
        and candidate.market_time is not None
        and (candidate.market_time - candidate.received_at).total_seconds() * 1000 > clock_skew_tolerance_ms
    )
    if clock_skewed:
        quality, mid = "CLOCK_SKEW", None
    elif bid is None or ask is None or bid <= 0 or ask <= 0:
        quality, mid = "ONE_SIDED", None
    elif bid > ask:
        quality, mid = "CROSSED", None
    elif distance_ms > max_distance_ms:
        quality, mid = "STALE", None
    else:
        quality, mid = "VALID", (bid + ask) / Decimal(2)
    manifest = {
        "mark_type": mark_type,
        "target_time": target_time,
        "candidate": _quote_manifest(candidate),
        "quality": quality,
        "mid_price": mid,
        "age_or_lag_ms": distance_ms,
        "mark_policy_version": MARK_POLICY_VERSION,
    }
    return SelectedMark(mark_type, target_time, quality, candidate, mid, distance_ms, canonical_json_sha256(manifest))


def calculate_parent_tca(input_: TcaCalculationInput) -> TcaCalculationResult:
    """Calculate one immutable parent snapshot using signed CNY atomics."""

    with localcontext() as context:
        context.prec = max(context.prec, 28)
        context.rounding = ROUND_HALF_EVEN
        return _calculate_parent_tca(input_)


def _calculate_parent_tca(input_: TcaCalculationInput) -> TcaCalculationResult:

    if input_.side not in {"BUY", "SELL"}:
        raise TcaCalculationError("ADAPTIVE_IS_TCA_SIDE_INVALID", "tca_calculator_input", side=input_.side)
    if input_.snapshot_kind not in {"DEADLINE", "RECONCILED_FINAL"}:
        raise TcaCalculationError("ADAPTIVE_IS_TCA_SNAPSHOT_KIND_INVALID", "tca_calculator_input")
    if input_.snapshot_kind == "DEADLINE" and input_.deadline and input_.as_of_time < input_.deadline:
        raise TcaCalculationError("ADAPTIVE_IS_TCA_DEADLINE_NOT_REACHED", "tca_calculator_finality")
    if input_.snapshot_kind == "RECONCILED_FINAL" and not input_.terminal_as_of:
        raise TcaCalculationError("ADAPTIVE_IS_TCA_TERMINAL_AS_OF_MISSING", "tca_calculator_finality")

    fills = _canonical_fills(input_.fills)
    missing_trade_time = [fill.trade_id for fill in fills if fill.trade_time is None]
    cross_trade_date = [
        fill.trade_id
        for fill in fills
        if fill.trade_time is not None and fill.trade_time.astimezone(_CHINA_TZ).date() != input_.trade_date
    ]
    deadline_fills = tuple(
        fill
        for fill in fills
        if fill.trade_time is not None
        and fill.trade_time.astimezone(_CHINA_TZ).date() == input_.trade_date
        and input_.deadline is not None
        and fill.trade_time <= input_.deadline
    )
    terminal_fills = tuple(
        fill
        for fill in fills
        if fill.trade_time is not None
        and fill.trade_time.astimezone(_CHINA_TZ).date() == input_.trade_date
        and input_.terminal_as_of is not None
        and fill.trade_time <= input_.terminal_as_of
    ) if input_.snapshot_kind == "RECONCILED_FINAL" else deadline_fills
    post_fills = tuple(fill for fill in terminal_fills if fill not in deadline_fills)

    q_e = input_.eligible_quantity
    q_deadline = sum(fill.quantity for fill in deadline_fills)
    q_terminal = sum(fill.quantity for fill in terminal_fills)
    invariant_results: dict[str, Any] = {
        "missing_authoritative_trade_time": missing_trade_time,
        "cross_trade_date_trade_ids": cross_trade_date,
        "deadline_resolved": input_.deadline is not None,
        "eligible_quantity_valid": q_e is not None and q_e > 0,
        "deadline_overfill": q_e is not None and q_deadline > q_e,
        "terminal_overfill": q_e is not None and q_terminal > q_e,
    }
    missing_canonical = [fill.trade_id for fill in fills if not _is_sha256(fill.canonical_fact_sha256)]
    invariant_results["missing_canonical_trade_fact"] = missing_canonical
    invalid = (
        q_e is None
        or q_e <= 0
        or input_.deadline is None
        or bool(missing_trade_time)
        or bool(cross_trade_date)
        or bool(missing_canonical)
        or bool(invariant_results["deadline_overfill"])
        or bool(invariant_results["terminal_overfill"])
    )
    residual_deadline = q_e - q_deadline if q_e is not None and not invalid else None
    residual_terminal = q_e - q_terminal if q_e is not None and not invalid else None
    sign = Decimal(1 if input_.side == "BUY" else -1)

    fee_universe = terminal_fills if input_.snapshot_kind == "RECONCILED_FINAL" else deadline_fills
    estimated_fee = estimate_fee_allocations(fee_universe, input_.side, input_.estimated_fee_policy)
    actual_fee = _actual_fee_allocations(fee_universe)
    deadline_actual_total, deadline_actual_quality = _actual_fee_subset(actual_fee, deadline_fills)
    post_actual_total, post_actual_quality = _actual_fee_subset(actual_fee, post_fills)
    if input_.snapshot_kind == "DEADLINE" and _has_order_actual_fee(fee_universe):
        deadline_actual_quality = "PROVISIONAL_ORDER_FEE_ALLOCATION"
    estimated_deadline_total = _sum_allocations(estimated_fee.by_trade, deadline_fills) if estimated_fee.total_cny is not None else None
    estimated_post_total = _sum_allocations(estimated_fee.by_trade, post_fills) if estimated_fee.total_cny is not None else None
    estimated_quality = estimated_fee.quality
    if input_.snapshot_kind == "DEADLINE" and estimated_fee.total_cny is not None and _has_order_scope(input_.estimated_fee_policy):
        estimated_quality = "PROVISIONAL_ORDER_FEE_ALLOCATION"

    decision_valid = input_.decision_price is not None and input_.decision_price > 0
    arrival_valid = input_.arrival_price is not None and input_.arrival_price > 0
    mark_valid = input_.deadline_mark is not None and input_.deadline_mark.quality == "VALID" and input_.deadline_mark.mid_price is not None
    mark_required = residual_deadline is not None and residual_deadline > 0
    base_valid = not invalid and q_e is not None and q_e > 0 and input_.deadline is not None

    delay = execution = opportunity = None
    decision_direct = decision_gross = arrival_gross = None
    decision_mode = None
    if base_valid and arrival_valid:
        delay = sign * Decimal(q_e) * (input_.arrival_price - input_.decision_price) if decision_valid else None
        execution = sign * sum(
            (Decimal(fill.quantity) * (fill.price - input_.arrival_price) for fill in deadline_fills),
            Decimal(0),
        )
        opportunity = (
            sign * Decimal(residual_deadline) * (input_.deadline_mark.mid_price - input_.arrival_price)
            if not mark_required or mark_valid
            else None
        )
        if not mark_required:
            opportunity = Decimal(0)
        arrival_gross = execution + opportunity if opportunity is not None else None
    if base_valid and decision_valid and (not mark_required or mark_valid):
        decision_direct = sign * (
            sum((Decimal(fill.quantity) * (fill.price - input_.decision_price) for fill in deadline_fills), Decimal(0))
            + Decimal(residual_deadline) * ((input_.deadline_mark.mid_price if mark_required else input_.decision_price) - input_.decision_price)
        )
        if arrival_valid and delay is not None and execution is not None and opportunity is not None:
            decision_gross = delay + execution + opportunity
            decision_mode = "DECOMPOSED"
            if abs(decision_direct - decision_gross) > input_.decimal_tolerance:
                invalid = True
                invariant_results["direct_decomposed_equality"] = False
            else:
                invariant_results["direct_decomposed_equality"] = True
        else:
            decision_gross = decision_direct
            decision_mode = "DIRECT"
            invariant_results["direct_decomposed_equality"] = None

    decision_denominator = Decimal(q_e) * input_.decision_price if base_valid and decision_valid else None
    arrival_denominator = Decimal(q_e) * input_.arrival_price if base_valid and arrival_valid else None
    result_status = "INVALID" if invalid else (
        "FINAL" if input_.snapshot_kind == "RECONCILED_FINAL" and input_.finality_satisfied else "PROVISIONAL"
    )
    deadline_notional = sum((fill.amount for fill in deadline_fills), Decimal(0))
    terminal_notional = sum((fill.amount for fill in terminal_fills), Decimal(0))
    deadline_vwap = deadline_notional / Decimal(q_deadline) if q_deadline else None
    terminal_vwap = terminal_notional / Decimal(q_terminal) if q_terminal else None

    effective = _effective_spread(deadline_fills, sign)
    markouts = {horizon: _markout(deadline_fills, sign, horizon) for horizon in (60000, 300000, 900000)}
    post_cost = None
    if post_fills and mark_valid:
        post_cost = sign * sum(
            (Decimal(fill.quantity) * (fill.price - input_.deadline_mark.mid_price) for fill in post_fills), Decimal(0)
        )

    values: dict[str, Any] = {
        "result_status": result_status,
        "eligible_quantity": q_e,
        "deadline_filled_quantity": q_deadline,
        "terminal_filled_quantity": q_terminal,
        "post_deadline_filled_quantity": q_terminal - q_deadline,
        "deadline_residual_quantity": residual_deadline,
        "terminal_residual_quantity": residual_terminal,
        "deadline_fill_count": len(deadline_fills),
        "deadline_fill_notional_cny": _q8(deadline_notional),
        "deadline_fill_vwap": _q8(deadline_vwap),
        "terminal_fill_count": len(terminal_fills),
        "terminal_fill_notional_cny": _q8(terminal_notional),
        "terminal_fill_vwap": _q8(terminal_vwap),
        "delay_cost_cny": _q8(delay),
        "execution_cost_cny": _q8(execution),
        "opportunity_cost_cny": _q8(opportunity),
        "decision_calculation_mode": decision_mode,
        "decision_is_direct_check_gross_cny": _q8(decision_direct),
        "decision_is_gross_cny": _q8(decision_gross),
        "decision_is_net_actual_cny": _net(decision_gross, deadline_actual_total),
        "decision_is_net_estimated_cny": _net(decision_gross, estimated_deadline_total),
        "decision_is_gross_bps": _ratio_bps(decision_gross, decision_denominator),
        "decision_is_net_actual_bps": _ratio_bps(_net_raw(decision_gross, deadline_actual_total), decision_denominator),
        "decision_is_net_estimated_bps": _ratio_bps(_net_raw(decision_gross, estimated_deadline_total), decision_denominator),
        "arrival_is_gross_cny": _q8(arrival_gross),
        "arrival_is_net_actual_cny": _net(arrival_gross, deadline_actual_total),
        "arrival_is_net_estimated_cny": _net(arrival_gross, estimated_deadline_total),
        "arrival_is_gross_bps": _ratio_bps(arrival_gross, arrival_denominator),
        "arrival_is_net_actual_bps": _ratio_bps(_net_raw(arrival_gross, deadline_actual_total), arrival_denominator),
        "arrival_is_net_estimated_bps": _ratio_bps(_net_raw(arrival_gross, estimated_deadline_total), arrival_denominator),
        "deadline_fee_actual_cny": _q8(deadline_actual_total),
        "deadline_fee_estimated_cny": _q8(estimated_deadline_total),
        "post_deadline_fee_actual_cny": _q8(post_actual_total),
        "post_deadline_fee_estimated_cny": _q8(estimated_post_total),
        "deadline_fee_quality": deadline_actual_quality if deadline_actual_quality in {"ACTUAL_PARTIAL", "PROVISIONAL_ORDER_FEE_ALLOCATION"} or deadline_actual_total is not None else estimated_quality,
        "post_deadline_fee_quality": post_actual_quality if post_actual_quality == "ACTUAL_PARTIAL" or post_actual_total is not None else estimated_quality,
        "fee_breakdown": {
            "actual_allocated": actual_fee.breakdown,
            "estimated_allocation": estimated_fee.breakdown,
        },
        "fee_schedule_version": _policy_value(input_.estimated_fee_policy, "fee_schedule_id"),
        "account_fee_profile_version": _policy_value(input_.estimated_fee_policy, "account_fee_profile_version"),
        "fee_allocation_version": _policy_value(input_.estimated_fee_policy, "fee_allocation_version") or FEE_ALLOCATION_VERSION,
        "completion_by_deadline_quantity": _completion(q_deadline, q_e),
        "terminal_completion_quantity": _completion(q_terminal, q_e),
        "completion_by_deadline_notional": _completion(q_deadline, q_e) if arrival_valid else None,
        "effective_spread_bps": effective[0],
        "effective_spread_partial_bps": effective[1],
        "effective_spread_coverage_notional_ratio": effective[2],
        "cost_markout_60s_bps": markouts[60000][0],
        "cost_markout_300s_bps": markouts[300000][0],
        "cost_markout_900s_bps": markouts[900000][0],
        "markout_partial_metrics": {str(h): values[1] for h, values in markouts.items()},
        "markout_coverage": {str(h): values[2] for h, values in markouts.items()},
        "post_deadline_execution_cost_cny": _q8(post_cost),
        "residual_reason": input_.residual_reason,
        "residual_executability_class": "INVALID" if invalid else input_.residual_executability_class,
        "metric_validity": {
            "decision": _validity(decision_gross, "DECISION_BENCHMARK_OR_MARK_MISSING"),
            "arrival": _validity(arrival_gross, "ARRIVAL_BENCHMARK_OR_MARK_MISSING"),
            "actual_fee": _validity(deadline_actual_total, "ACTUAL_FEE_INCOMPLETE"),
            "estimated_fee": _validity(estimated_deadline_total, estimated_fee.reason_code or "ESTIMATED_FEE_INCOMPLETE"),
        },
        "benchmark_coverage": {"decision_valid": decision_valid, "arrival_valid": arrival_valid},
        "mark_coverage": {
            "deadline_quality": input_.deadline_mark.quality if input_.deadline_mark else "MISSING",
            "deadline_required": mark_required,
        },
        "fee_coverage": {
            "deadline_actual_quality": deadline_actual_quality,
            "post_deadline_actual_quality": post_actual_quality,
            "estimated_quality": estimated_quality,
        },
        "invariant_results": invariant_results,
        "formula_version": FORMULA_VERSION,
        "calculator_version": CALCULATOR_VERSION,
    }
    input_manifest = _input_manifest(input_, fills, estimated_fee.policy_sha256)
    output_manifest = dict(values)
    return TcaCalculationResult(values, canonical_json_sha256(input_manifest), canonical_json_sha256(output_manifest))


def estimate_fee_allocations(
    fills: Sequence[TcaFill], side: str, policy: Mapping[str, Any] | None
) -> FeeAllocation:
    """Apply a frozen component policy and stable largest-remainder allocation."""

    with localcontext() as context:
        context.prec = max(context.prec, 28)
        context.rounding = ROUND_HALF_EVEN
        return _estimate_fee_allocations(fills, side, policy)


def _estimate_fee_allocations(
    fills: Sequence[TcaFill], side: str, policy: Mapping[str, Any] | None
) -> FeeAllocation:

    if not fills:
        return FeeAllocation(Decimal(0), "ESTIMATED", {}, {}, canonical_json_sha256(policy) if policy else None)
    if not policy:
        return FeeAllocation(None, "MISSING", {}, {}, None, "ADAPTIVE_IS_TCA_FEE_POLICY_MISSING")
    required_top = {
        "fee_schedule_id",
        "effective_from",
        "market",
        "account_fee_profile_version",
        "account_fee_profile_sha256",
        "fee_allocation_version",
        "settlement_rounding",
        "components",
    }
    missing_top = sorted(required_top.difference(policy))
    if missing_top or policy.get("settlement_rounding") != "ROUND_HALF_UP":
        return FeeAllocation(None, "MISSING", {}, {}, canonical_json_sha256(policy), "ADAPTIVE_IS_TCA_FEE_POLICY_INCOMPLETE")
    components = policy.get("components")
    if not isinstance(components, Mapping) or set(components) != _FEE_COMPONENTS:
        return FeeAllocation(None, "MISSING", {}, {}, canonical_json_sha256(policy), "ADAPTIVE_IS_TCA_FEE_COMPONENT_RULE_MISSING")

    by_trade: dict[str, dict[str, Decimal]] = {fill.trade_id: {} for fill in fills}
    totals: dict[str, Decimal] = {}
    for component_name in sorted(components):
        rule = components[component_name]
        if not isinstance(rule, Mapping) or not _valid_fee_rule(rule):
            return FeeAllocation(None, "MISSING", {}, {}, canonical_json_sha256(policy), "ADAPTIVE_IS_TCA_FEE_COMPONENT_RULE_INCOMPLETE")
        applicable = [fill for fill in fills if side in set(rule["applies_to_sides"])]
        if not applicable:
            totals[component_name] = Decimal(0)
            continue
        if rule["calculation_scope"] == "TRADE":
            for fill in applicable:
                value = _component_total((fill,), rule)
                by_trade[fill.trade_id][component_name] = value
            totals[component_name] = sum((by_trade[fill.trade_id][component_name] for fill in applicable), Decimal(0))
        else:
            component_total = Decimal(0)
            order_ids = sorted({fill.order_id for fill in applicable})
            for order_id in order_ids:
                order_fills = tuple(fill for fill in applicable if fill.order_id == order_id)
                order_total = _component_total(order_fills, rule)
                allocations = _largest_remainder(order_fills, order_total, Decimal(str(rule["rounding_unit"])))
                for trade_id, amount in allocations.items():
                    by_trade[trade_id][component_name] = amount
                component_total += order_total
            totals[component_name] = component_total
    total = sum(totals.values(), Decimal(0))
    return FeeAllocation(total, "ESTIMATED", totals, by_trade, canonical_json_sha256(policy))


def _valid_fee_rule(rule: Mapping[str, Any]) -> bool:
    required = {
        "rate",
        "calculation_scope",
        "rate_base",
        "minimum_rule",
        "minimum_amount",
        "rounding_stage",
        "rounding_unit",
        "rounding_mode",
        "applies_to_sides",
    }
    if required.difference(rule):
        return False
    return (
        rule["calculation_scope"] in {"TRADE", "ORDER"}
        and rule["rate_base"] == "NOTIONAL"
        and rule["minimum_rule"] in {"NONE", "MINIMUM"}
        and rule["rounding_stage"] == "COMPONENT_TOTAL"
        and rule["rounding_mode"] == "ROUND_HALF_UP"
        and Decimal(str(rule["rate"])) >= 0
        and Decimal(str(rule["rounding_unit"])) > 0
    )


def _component_total(fills: Sequence[TcaFill], rule: Mapping[str, Any]) -> Decimal:
    unrounded = sum((fill.amount for fill in fills), Decimal(0)) * Decimal(str(rule["rate"]))
    if rule["minimum_rule"] == "MINIMUM":
        unrounded = max(unrounded, Decimal(str(rule["minimum_amount"])))
    return unrounded.quantize(Decimal(str(rule["rounding_unit"])), rounding=ROUND_HALF_UP)


def _largest_remainder(fills: Sequence[TcaFill], total: Decimal, unit: Decimal) -> dict[str, Decimal]:
    notional = sum((fill.amount for fill in fills), Decimal(0))
    if notional <= 0 or any(not fill.trade_id for fill in fills):
        raise TcaCalculationError("ADAPTIVE_IS_TCA_FEE_ALLOCATION_IDENTITY_MISSING", "tca_fee_allocation")
    exact_units = {fill.trade_id: total / unit * fill.amount / notional for fill in fills}
    allocated_units = {key: int(value) for key, value in exact_units.items()}
    remaining = int(total / unit) - sum(allocated_units.values())
    ordered = sorted(
        fills,
        key=lambda fill: (
            -(exact_units[fill.trade_id] - Decimal(allocated_units[fill.trade_id])),
            fill.trade_time.isoformat() if fill.trade_time is not None else "",
            fill.trade_id,
        ),
    )
    for fill in ordered[:remaining]:
        allocated_units[fill.trade_id] += 1
    result = {trade_id: Decimal(units) * unit for trade_id, units in allocated_units.items()}
    if sum(result.values(), Decimal(0)) != total:
        raise TcaCalculationError("ADAPTIVE_IS_TCA_FEE_ALLOCATION_INVARIANT", "tca_fee_allocation")
    return result


def _actual_fee_allocations(fills: Sequence[TcaFill]) -> FeeAllocation:
    if not fills:
        return FeeAllocation(Decimal(0), "ACTUAL_COMPLETE", {"commission": Decimal(0)}, {}, None)
    by_trade: dict[str, dict[str, Decimal]] = {}
    for order_id in sorted({fill.order_id for fill in fills}):
        order_fills = tuple(fill for fill in fills if fill.order_id == order_id)
        order_evidence = [
            fill.actual_fee_cny
            for fill in order_fills
            if fill.fee_provenance == "ACTUAL"
            and fill.actual_fee_scope == "ORDER_LEVEL"
            and fill.actual_fee_cny is not None
        ]
        trade_evidence = [
            fill
            for fill in order_fills
            if fill.fee_provenance == "ACTUAL"
            and fill.actual_fee_scope == "TRADE_LEVEL"
            and fill.actual_fee_cny is not None
        ]
        if order_evidence and trade_evidence:
            raise TcaCalculationError(
                "ADAPTIVE_IS_TCA_ACTUAL_FEE_SCOPE_CONFLICT", "tca_fee_actual", order_id=order_id
            )
        if order_evidence:
            totals = set(order_evidence)
            if len(totals) != 1:
                raise TcaCalculationError(
                    "ADAPTIVE_IS_TCA_ORDER_FEE_CONFLICT", "tca_fee_actual", order_id=order_id
                )
            allocations = _largest_remainder(order_fills, totals.pop(), Decimal("0.01"))
            for trade_id, value in allocations.items():
                by_trade[trade_id] = {"commission": value}
        else:
            for fill in trade_evidence:
                by_trade[fill.trade_id] = {"commission": fill.actual_fee_cny}
    allocated = sum((sum(parts.values(), Decimal(0)) for parts in by_trade.values()), Decimal(0))
    if len(by_trade) == len(fills):
        return FeeAllocation(allocated, "ACTUAL_COMPLETE", {"commission": allocated}, by_trade, None)
    if by_trade:
        return FeeAllocation(None, "ACTUAL_PARTIAL", {"observed_commission": allocated}, by_trade, None)
    quality = "UNKNOWN_LEGACY" if any(fill.fee_provenance == "UNKNOWN_LEGACY" for fill in fills) else "MISSING"
    return FeeAllocation(None, quality, {}, {}, None)


def _actual_fee_subset(allocation: FeeAllocation, fills: Sequence[TcaFill]) -> tuple[Decimal | None, str]:
    if not fills:
        return Decimal(0), "ACTUAL_COMPLETE"
    covered = [fill for fill in fills if fill.trade_id in allocation.by_trade]
    if len(covered) == len(fills):
        return _sum_allocations(allocation.by_trade, fills), "ACTUAL_COMPLETE"
    if covered:
        return None, "ACTUAL_PARTIAL"
    return None, allocation.quality


def _has_order_actual_fee(fills: Sequence[TcaFill]) -> bool:
    return any(fill.fee_provenance == "ACTUAL" and fill.actual_fee_scope == "ORDER_LEVEL" for fill in fills)


def _canonical_fills(fills: Iterable[TcaFill]) -> tuple[TcaFill, ...]:
    by_key: dict[str, TcaFill] = {}
    for fill in fills:
        existing = by_key.get(fill.trade_id)
        if existing is None:
            by_key[fill.trade_id] = fill
        elif existing.canonical_fact_sha256 != fill.canonical_fact_sha256:
            raise TcaCalculationError(
                "ADAPTIVE_IS_TCA_CANONICAL_TRADE_CONFLICT",
                "tca_calculator_dedup",
                trade_id=fill.trade_id,
            )
    return tuple(
        sorted(
            by_key.values(),
            key=lambda fill: (
                fill.trade_time is None,
                fill.trade_time.isoformat() if fill.trade_time is not None else "",
                fill.trade_id,
            ),
        )
    )


def _effective_spread(fills: Sequence[TcaFill], sign: Decimal) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    if not fills:
        return Decimal(0), Decimal(0), Decimal(1)
    covered = [fill for fill in fills if fill.child_receipt_mid is not None and fill.child_receipt_mid > 0]
    total_notional = sum((fill.amount for fill in fills), Decimal(0))
    covered_execution = sum((fill.amount for fill in covered), Decimal(0))
    coverage = covered_execution / total_notional if total_notional else None
    if not covered:
        return None, None, _q12(coverage)
    numerator = sign * sum(
        (Decimal(fill.quantity) * (fill.price - fill.child_receipt_mid) for fill in covered), Decimal(0)
    )
    denominator = sum((Decimal(fill.quantity) * fill.child_receipt_mid for fill in covered), Decimal(0))
    partial = Decimal(2) * numerator / denominator * _BPS if denominator else None
    headline = partial if len(covered) == len(fills) else None
    return _q8(headline), _q8(partial), _q12(coverage)


def _markout(fills: Sequence[TcaFill], sign: Decimal, horizon: int) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    if not fills:
        return Decimal(0), Decimal(0), Decimal(1)
    covered = [fill for fill in fills if fill.markout_mid_by_horizon_ms.get(horizon) is not None]
    total = sum((fill.amount for fill in fills), Decimal(0))
    covered_notional = sum((fill.amount for fill in covered), Decimal(0))
    coverage = covered_notional / total if total else None
    if not covered:
        return None, None, _q12(coverage)
    numerator = sign * sum(
        (Decimal(fill.quantity) * (fill.price - fill.markout_mid_by_horizon_ms[horizon]) for fill in covered), Decimal(0)
    )
    partial = numerator / covered_notional * _BPS if covered_notional else None
    return _q8(partial if len(covered) == len(fills) else None), _q8(partial), _q12(coverage)


def _input_manifest(input_: TcaCalculationInput, fills: Sequence[TcaFill], fee_policy_sha256: str | None) -> dict[str, Any]:
    return {
        "parent_intent_id": input_.parent_intent_id,
        "trade_date": input_.trade_date,
        "side": input_.side,
        "eligible_quantity": input_.eligible_quantity,
        "decision_price": input_.decision_price,
        "arrival_price": input_.arrival_price,
        "deadline": input_.deadline,
        "as_of_time": input_.deadline if input_.snapshot_kind == "DEADLINE" else input_.terminal_as_of,
        "snapshot_kind": input_.snapshot_kind,
        "terminal_as_of": input_.terminal_as_of,
        "reconciliation_run_id": input_.reconciliation_run_id,
        "finality_satisfied": input_.finality_satisfied,
        "fills": [_fill_manifest(fill) for fill in fills],
        "deadline_mark": input_.deadline_mark.as_manifest() if input_.deadline_mark else None,
        "fee_policy_sha256": fee_policy_sha256,
        "calculator_version": CALCULATOR_VERSION,
        "formula_version": FORMULA_VERSION,
    }


def _missing_mark(mark_type: str, target_time: datetime, quality: str) -> SelectedMark:
    manifest = {"mark_type": mark_type, "target_time": target_time, "quality": quality, "mark_policy_version": MARK_POLICY_VERSION}
    return SelectedMark(mark_type, target_time, quality, None, None, None, canonical_json_sha256(manifest))


def _quote_manifest(candidate: QuoteCandidate) -> dict[str, Any]:
    return {name: getattr(candidate, name) for name in candidate.__dataclass_fields__}


def _fill_manifest(fill: TcaFill) -> dict[str, Any]:
    return {name: getattr(fill, name) for name in fill.__dataclass_fields__}


def _sum_allocations(by_trade: Mapping[str, Mapping[str, Decimal]], fills: Sequence[TcaFill]) -> Decimal:
    return sum((sum(by_trade.get(fill.trade_id, {}).values(), Decimal(0)) for fill in fills), Decimal(0))


def _has_order_scope(policy: Mapping[str, Any] | None) -> bool:
    components = policy.get("components") if policy else None
    return isinstance(components, Mapping) and any(
        isinstance(rule, Mapping) and rule.get("calculation_scope") == "ORDER" for rule in components.values()
    )


def _policy_value(policy: Mapping[str, Any] | None, key: str) -> Any:
    return policy.get(key) if policy else None


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _completion(filled: int, eligible: int | None) -> Decimal | None:
    return _q12(Decimal(filled) / Decimal(eligible)) if eligible else None


def _ratio_bps(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return _q8(numerator / denominator * _BPS)


def _net_raw(gross: Decimal | None, fee: Decimal | None) -> Decimal | None:
    return gross + fee if gross is not None and fee is not None else None


def _net(gross: Decimal | None, fee: Decimal | None) -> Decimal | None:
    return _q8(_net_raw(gross, fee))


def _validity(value: Any, reason: str) -> dict[str, Any]:
    return {"valid": value is not None, "reason_code": None if value is not None else reason}


def _q8(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 28
        context.rounding = ROUND_HALF_EVEN
        return value.quantize(_EIGHT_PLACES)


def _q12(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 28
        context.rounding = ROUND_HALF_EVEN
        return value.quantize(_TWELVE_PLACES)
