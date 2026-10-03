"""D-only tick sets; the numerical decision kernel remains aligned v3."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_aligned_inference import predict_aligned_entry_nodes_v3
from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryDailyInputV1, EconomicEntryPriceNodeV1
from backend.services.advisory_model_first.economic_entry_labels import _day, _fail, _number
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EconomicModelScopeV2, EntryRiskBudgetV2
from backend.services.advisory_model_first.economic_risk_alignment_inference import price_context_sha256
from backend.services.advisory_model_first.price_range_regulatory import resolve_regulatory_price_range
from backend.services.advisory_model_first.realtime_feature_source import PriceRangeRealtimeContext
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def daily_decision_feature_values_v1(features, names):
    required = tuple(name for name in names if name != "query_gap_bps")
    if not set(required).issubset(features):
        _fail("economic daily input is missing its D feature schema")
    return {name: None if isinstance(features[name], (bool, np.bool_)) else _number(features[name]) for name in required}


def _intervals(nodes, tick):
    intervals, group = [], []
    for node in [*nodes, None]:
        if node is not None and node.status == "ACCEPTABLE":
            group.append(node)
        elif group:
            intervals.append({"minimum_cny": group[0].price_cny, "maximum_cny": group[-1].price_cny,
                              "grid_step_cny": tick, "node_count": len(group),
                              "expected_net_return_min_bps": min(value.expected_net_return_bps for value in group),
                              "expected_net_return_max_bps": max(value.expected_net_return_bps for value in group),
                              "entry_net_max_loss_q90_max_bps": max(value.entry_net_max_loss_q90_bps for value in group)})
            group = []
    return intervals


def _publish(common, status, reason, nodes, intervals):
    counts = pd.Series([value.status for value in nodes], dtype=str).value_counts().astype(int).to_dict()
    known = sum(counts.get(name, 0) for name in ("ACCEPTABLE", "REJECTED"))
    payload = {**common, "recommendation_status": status, "reason_code": reason,
               "query_nodes": [value.model_dump(mode="json") for value in nodes], "node_status_counts": counts,
               "acceptable_price_intervals": intervals,
               "availability": "COMPLETE" if nodes and known == len(nodes) else "PARTIAL" if known else "UNAVAILABLE"}
    return {**payload, "advice_sha256": canonical_json_sha256(payload)}


def build_economic_daily_price_set_v1(*, fitted, model_scope, model_bundle_sha256, prediction_input, context,
                                     decision_features, trading_calendar, risk_budget, maximum_grid_nodes=5000,
                                     qualified_bundle=None, role_binding=None):
    """A serving loader must verify the real model/scope before calling this kernel."""
    prediction_input = EconomicEntryDailyInputV1.model_validate(prediction_input.model_dump())
    model_scope = EconomicModelScopeV2.model_validate(model_scope.model_dump())
    source = fitted.request.source_request
    if (prediction_input.scope != model_scope or source.feature_names != model_scope.feature_names
            or prediction_input.decision_date <= source.validation_end):
        _fail("economic daily model scope, schema or mature cutoff differs")
    if (not isinstance(model_bundle_sha256, str) or len(model_bundle_sha256) != 64
            or any(value not in "0123456789abcdef" for value in model_bundle_sha256)):
        _fail("economic daily model bundle hash malformed")
    if isinstance(maximum_grid_nodes, bool) or not isinstance(maximum_grid_nodes, int) or not 1 <= maximum_grid_nodes <= 5000:
        _fail("economic daily grid budget must be an explicit bounded integer")
    calendar = pd.DatetimeIndex([_day(value) for value in trading_calendar])
    decision, target = pd.Timestamp(prediction_input.decision_date), pd.Timestamp(prediction_input.target_date)
    if (calendar.empty or not calendar.is_unique or not calendar.is_monotonic_increasing or decision not in calendar
            or target not in calendar or calendar.get_loc(target) != calendar.get_loc(decision) + 1):
        _fail("economic daily target must be the next bound trading day")
    features = daily_decision_feature_values_v1(decision_features, source.feature_names)
    if canonical_json_sha256(features) != prediction_input.feature_values_sha256:
        _fail("economic D feature contents differ from input receipt")
    formal = qualified_bundle is not None or role_binding is not None
    if formal:
        from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryValueRoleV1
        from backend.services.advisory_model_first.economic_entry_serving_bundle import LoadedEconomicEntryConfirmedBundleV1
        if not isinstance(qualified_bundle, LoadedEconomicEntryConfirmedBundleV1) or not isinstance(role_binding, EconomicEntryValueRoleV1):
            _fail("formal price advice requires both the verified qualified bundle and independent role")
        if (qualified_bundle.fitted is not fitted or qualified_bundle.manifest.scope != model_scope
                or qualified_bundle.manifest.manifest_sha256 != model_bundle_sha256 or role_binding.scope != model_scope
                or role_binding.bundle_id != qualified_bundle.manifest.bundle_id
                or role_binding.confirmation_request_sha256 != qualified_bundle.confirmation_request.request_sha256
                or role_binding.program_id != prediction_input.program_id or role_binding.binding_version_id != prediction_input.binding_version_id
                or prediction_input.target_date < role_binding.effective_from_target_date
                or prediction_input.captured_at < role_binding.created_at or prediction_input.source_evidence != "NATIVE_COMPLETE"
                or prediction_input.evidence_level != "PROSPECTIVE_INPUT"
                or qualified_bundle.confirmation_result.get("status") != "ECONOMIC_GATES_PASSED"):
            _fail("formal price advice model, role, input or actual qualification differs")
    if risk_budget is not None:
        risk_budget = EntryRiskBudgetV2.model_validate(risk_budget.model_dump())
        if (risk_budget.maximum_loss_bps != fitted.request.risk_reference_bps
                or (formal and risk_budget != qualified_bundle.confirmation_request.business_risk)
                or (not formal and risk_budget.reference_use != "FIXED_RESEARCH_STOP_REFERENCE")):
            _fail("first aligned consumer cannot search a new risk budget or masquerade it as business configuration")
    if formal and risk_budget is None:
        _fail("formal price advice cannot omit its independently confirmed business risk")
    common = {"schema_version": "economic_entry_daily_advice_v1", "role": "ENTRY_VALUE",
              "objective_contract": "RISK_MANAGED_ADVISORY", "model_bundle_sha256": model_bundle_sha256,
              "model_scope_sha256": model_scope.scope_sha256, "prediction_input_sha256": prediction_input.identity_sha256,
              "prediction_input": prediction_input.model_dump(mode="json"), "price_basis": "raw_cny",
              "risk_metric": fitted.request.risk_metric, "risk_quantile": .9,
              "risk_budget": risk_budget.model_dump(mode="json") if risk_budget else None,
              "reference_raw_cny": None, "regulatory_price_range": None, "query_grid_step_cny": None, "maximum_grid_nodes": maximum_grid_nodes,
              "decision_use": "ADVISORY_ONLY" if formal else "NAVIGATION_ONLY", "deployable": formal,
              "evidence_state": "CONFIRMED_ENTRY_VALUE" if formal else "RESEARCH_NAVIGATION",
              "role_binding_sha256": role_binding.role_sha256 if formal else None,
              "inference_semantics": "observed_price_condition_estimate_not_causal_optimal_execution",
              "uncertainty_semantics": fitted.diagnostics["uncertainty_semantics"],
              "validation_return_abs_error_p90_bps": fitted.diagnostics["validation_return_abs_error_p90_bps"],
              "validity": "exact_D_frozen_tick_nodes_only_actual_entry_requires_separate_execution_evidence"}
    if context is None:
        if prediction_input.price_context_sha256 is not None:
            _fail("economic price context missing despite a bound receipt")
        return _publish(common, "UNAVAILABLE", "PRICE_CONTEXT_UNAVAILABLE", [], [])
    if not isinstance(context, PriceRangeRealtimeContext):
        _fail("economic price context must follow the verified source contract")
    if context.symbol != prediction_input.instrument or price_context_sha256(context) != prediction_input.price_context_sha256:
        _fail("economic price context symbol or contents differ from receipt")
    if not isinstance(context.decision_price_trade_date, date) or not isinstance(context.list_date, date):
        _fail("economic price context dates are not explicit dates")
    if context.decision_price_trade_date > prediction_input.decision_date:
        _fail("economic price context exceeds D")
    if context.decision_price_trade_date < prediction_input.decision_date:
        return _publish(common, "UNAVAILABLE", "STALE_PRICE_COORDINATE_UNPROVEN_CANDIDATE_RETAINED", [], [])
    values = (context.decision_raw_close, context.target_raw_price_multiplier, context.tick_size, context.price_unit_divisor)
    if (any(isinstance(value, (bool, np.bool_)) or (number := _number(value)) is None or number <= 0 for value in values)
            or not isinstance(context.target_is_st, bool) or context.board_type not in {"MAIN", "STAR", "CHINEXT", "BSE"}
            or context.list_date > prediction_input.decision_date or isinstance(context.listed_trading_days, bool)
            or not isinstance(context.listed_trading_days, int) or context.listed_trading_days < 1):
        _fail("economic D-visible regulatory attributes are invalid")
    reference = float(context.decision_raw_close) * float(context.target_raw_price_multiplier)
    if not np.isfinite(reference) or reference <= 0:
        _fail("economic D reference is nonfinite or nonpositive")
    regulatory = resolve_regulatory_price_range(context, target_trade_date=prediction_input.target_date)
    common.update(reference_raw_cny=reference, regulatory_price_range=regulatory.as_dict())
    if regulatory.status != "LIMITED":
        return _publish(common, "QUERY_DOMAIN_UNAVAILABLE", "NO_LIMIT_REQUIRES_REGISTERED_FINITE_DOMAIN", [], [])
    low, high, tick = Decimal(str(regulatory.low)), Decimal(str(regulatory.high)), Decimal(str(context.tick_size))
    common["query_grid_step_cny"] = float(tick)
    if not all(value.is_finite() for value in (low, high, tick)) or low <= 0 or low > high or low % tick or high % tick:
        _fail("economic regulatory bounds differ from positive CNY tick lattice")
    count = int((high - low) / tick) + 1
    if count > maximum_grid_nodes:
        return _publish(common, "QUERY_DOMAIN_UNAVAILABLE", "GRID_RESOURCE_BUDGET_EXCEEDED", [], [])
    prices = [float(low + index * tick) for index in range(count)]
    matrix = pd.DataFrame({name: np.nan if value is None else value for name, value in features.items()}, index=range(count))
    matrix["query_gap_bps"] = (np.asarray(prices) / reference - 1) * 10000
    predicted = predict_aligned_entry_nodes_v3(fitted=fitted, matrix=matrix)
    nodes = []
    for price, values in zip(prices, predicted.to_dict("records"), strict=True):
        supported = values["model_action"] != "UNAVAILABLE"
        status = ("OUT_OF_SUPPORT" if not supported else "EXECUTABILITY_UNPROVEN" if price >= float(high) - 1e-9 else
                  "RISK_CONTRACT_UNCONFIGURED" if risk_budget is None else "ACCEPTABLE" if values["model_action"] == "TAKE" else "REJECTED")
        nodes.append(EconomicEntryPriceNodeV1(price_cny=price, status=status,
                    expected_net_return_bps=values["expected_net_return_bps"] if supported else None,
                    entry_net_max_loss_q90_bps=values["entry_net_max_loss_q90_bps"] if supported else None,
                    support_observations=values["support_observations"], support_decision_days=values["support_decision_days"]))
    intervals = _intervals(nodes, float(tick))
    known = sum(value.status in {"ACCEPTABLE", "REJECTED"} for value in nodes)
    estimated = sum(value.expected_net_return_bps is not None for value in nodes)
    status = ("UNAVAILABLE" if not estimated else "RISK_CONTRACT_UNCONFIGURED" if risk_budget is None else
              "ACCEPTABLE_PRICE_SET" if intervals else "NO_ACCEPTABLE_PRICE" if known == len(nodes) else "PARTIAL_UNKNOWN")
    return _publish(common, status, None, nodes, intervals)


def project_economic_daily_advice_v1(advice):
    original = dict(advice)
    digest = original.pop("advice_sha256", None)
    if digest is None or canonical_json_sha256(original) != digest or original.get("schema_version") != "economic_entry_daily_advice_v1":
        _fail("economic advice content/schema differs from its immutable digest")
    source = EconomicEntryDailyInputV1.model_validate(original.get("prediction_input"))
    formal = original.get("decision_use") == "ADVISORY_ONLY"
    risk_payload = original.get("risk_budget")
    parsed_risk = EntryRiskBudgetV2.model_validate(risk_payload) if risk_payload is not None else None
    if (original.get("decision_use") not in {"ADVISORY_ONLY", "NAVIGATION_ONLY"} or original.get("deployable") is not formal
            or original.get("evidence_state") != ("CONFIRMED_ENTRY_VALUE" if formal else "RESEARCH_NAVIGATION")
            or original.get("prediction_input_sha256") != source.identity_sha256 or original.get("model_scope_sha256") != source.scope.scope_sha256
            or (formal and (source.source_evidence != "NATIVE_COMPLETE" or source.evidence_level != "PROSPECTIVE_INPUT"
                or not isinstance(original.get("role_binding_sha256"), str) or len(original["role_binding_sha256"]) != 64
                or any(value not in "0123456789abcdef" for value in original["role_binding_sha256"])
                or parsed_risk is None or parsed_risk.reference_use != "EXPLICIT_BUSINESS_CONFIGURATION"))
            or (not formal and original.get("role_binding_sha256") is not None)):
        _fail("economic advice has contradictory evidence, identity or independent role")
    raw_nodes = original.get("query_nodes")
    if not isinstance(raw_nodes, list) or len(raw_nodes) > 5000:
        _fail("economic advice node list exceeds its upfront contract")
    nodes = [EconomicEntryPriceNodeV1.model_validate(value) for value in raw_nodes]
    counts = {}
    for node in nodes:
        counts[node.status] = counts.get(node.status, 0) + 1
    known = sum(counts.get(name, 0) for name in ("ACCEPTABLE", "REJECTED"))
    expected_availability = "COMPLETE" if nodes and known == len(nodes) else "PARTIAL" if known else "UNAVAILABLE"
    expected_intervals = []
    if nodes:
        step = original.get("query_grid_step_cny")
        number = None if isinstance(step, bool) else _number(step)
        regulatory = original.get("regulatory_price_range") or {}
        if (number is None or number <= 0 or regulatory.get("status") != "LIMITED"
                or nodes[0].price_cny != regulatory.get("low") or nodes[-1].price_cny != regulatory.get("high")
                or any(Decimal(str(right.price_cny)) - Decimal(str(left.price_cny)) != Decimal(str(step))
                       for left, right in zip(nodes, nodes[1:]))):
            _fail("economic advice dropped, bridged or reordered a frozen price tick")
        expected_intervals = _intervals(nodes, number)
    if (canonical_json_sha256(counts) != canonical_json_sha256(original.get("node_status_counts"))
            or expected_availability != original.get("availability")
            or canonical_json_sha256(expected_intervals) != canonical_json_sha256(original.get("acceptable_price_intervals"))):
        _fail("economic advice projection does not match its complete frozen nodes")
    estimated = sum(value.expected_net_return_bps is not None for value in nodes)
    if nodes:
        expected_status = ("UNAVAILABLE" if not estimated else "RISK_CONTRACT_UNCONFIGURED" if parsed_risk is None else
            "ACCEPTABLE_PRICE_SET" if expected_intervals else "NO_ACCEPTABLE_PRICE" if known == len(nodes) else "PARTIAL_UNKNOWN")
        if original.get("recommendation_status") != expected_status:
            _fail("economic advice recommendation contradicts its actual node states")
    elif original.get("recommendation_status") not in {"UNAVAILABLE", "QUERY_DOMAIN_UNAVAILABLE", "RISK_CONTRACT_UNCONFIGURED"}:
        _fail("economic advice cannot declare rejection or acceptable prices without nodes")
    projection = {name: value for name, value in original.items() if name != "query_nodes"}
    projection.update(schema_version="economic_entry_daily_projection_v1", projection_producer_version="economic_entry_daily_v1",
                      original_advice_sha256=digest, query_node_count=len(original["query_nodes"]))
    return {**projection, "projection_sha256": canonical_json_sha256(projection)}


def select_economic_daily_observed_price_v1(*, advice, expected_advice_sha256, observation_date,
                                          observed_price_cny, trading_status):
    """Read exact immutable D nodes; not an order or an intraday timing algorithm."""
    projection = project_economic_daily_advice_v1(advice)
    if (projection["original_advice_sha256"] != expected_advice_sha256
            or _day(observation_date).date() != EconomicEntryDailyInputV1.model_validate(advice["prediction_input"]).target_date):
        _fail("economic observed price must bind the exact D advice and T date")
    common = {"advice_sha256": expected_advice_sha256, "decision_use": advice["decision_use"],
              "deployable": advice["deployable"], "observation_date": _day(observation_date).date().isoformat()}
    if trading_status in {"SUSPENDED", "LIMIT_UP"}:
        return {**common, "action": "NOT_APPLICABLE", "reason_code": trading_status}
    price = None if isinstance(observed_price_cny, (bool, np.bool_)) else _number(observed_price_cny)
    if trading_status != "EXECUTABLE" or price is None or price <= 0:
        return {**common, "action": "UNAVAILABLE", "reason_code": "OBSERVED_PRICE_OR_EXECUTABILITY_UNPROVEN"}
    node = next((value for value in advice["query_nodes"] if Decimal(str(value["price_cny"])) == Decimal(str(price))), None)
    if node is None:
        return {**common, "action": "UNAVAILABLE", "reason_code": "OBSERVED_PRICE_NOT_AN_EXACT_FROZEN_NODE"}
    node = EconomicEntryPriceNodeV1.model_validate(node)
    action = "TAKE" if node.status == "ACCEPTABLE" else "SKIP" if node.status == "REJECTED" else "UNAVAILABLE"
    return {**common, "action": action, "reason_code": node.status,
            "observed_node": node.model_dump(mode="json")}
