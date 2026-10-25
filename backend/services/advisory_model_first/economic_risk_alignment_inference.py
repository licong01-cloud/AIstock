"""D-frozen economic grids with fresh input identity and explicit risk semantics."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from decimal import Decimal
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import _day, _fail, _number
from backend.services.advisory_model_first.economic_risk_alignment_contracts import (
    EconomicModelScopeV2, EntryRiskBudgetV2, PredictionInputContextV2,
)
from backend.services.advisory_model_first.economic_risk_alignment_training import EntryLossTrainingResultV2
from backend.services.advisory_model_first.price_range_regulatory import resolve_regulatory_price_range
from backend.services.advisory_model_first.realtime_feature_source import PriceRangeRealtimeContext
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def price_context_sha256(context: PriceRangeRealtimeContext) -> str:
    return canonical_json_sha256({name: value.isoformat() if isinstance(value, date) else value
                                 for name, value in asdict(context).items()})


def build_daily_entry_loss_advice_v2(
    *, fitted: EntryLossTrainingResultV2, model_scope: EconomicModelScopeV2,
    model_bundle_sha256: str, prediction_input: PredictionInputContextV2,
    context: PriceRangeRealtimeContext, decision_features: Mapping[str, Any],
    trading_calendar: Sequence[date], risk_budget: EntryRiskBudgetV2 | None,
    grid_step_ticks: int = 1, maximum_grid_nodes: int = 5000,
) -> dict[str, Any]:
    """Consumer-side kernel only. No DB call, binding activation or current ST lookup."""
    prediction_input = PredictionInputContextV2.model_validate(prediction_input.model_dump())
    model_scope = EconomicModelScopeV2.model_validate(model_scope.model_dump())
    if prediction_input.scope.scope_sha256 != model_scope.scope_sha256:
        _fail("daily prediction package/policy/schema/universe scope differs from model")
    if risk_budget is not None:
        risk_budget = EntryRiskBudgetV2.model_validate(risk_budget.model_dump())
    if len(model_bundle_sha256) != 64 or any(value not in "0123456789abcdef" for value in model_bundle_sha256):
        _fail("daily economic bundle hash is malformed")
    if fitted.original_request.feature_names != model_scope.feature_names:
        _fail("daily economic model feature order differs from scope")
    if prediction_input.decision_date <= fitted.original_request.validation_end:
        _fail("daily economic prediction predates fitted/calibrated model cutoff")
    calendar = pd.DatetimeIndex([_day(day) for day in trading_calendar])
    if calendar.empty or not calendar.is_unique or not calendar.is_monotonic_increasing:
        _fail("daily economic calendar is unordered or duplicated")
    decision, target = pd.Timestamp(prediction_input.decision_date), pd.Timestamp(prediction_input.target_date)
    if decision not in calendar or target not in calendar or calendar.get_loc(target) != calendar.get_loc(decision) + 1:
        _fail("daily economic target is not the next bound trading day")
    if context.symbol != prediction_input.instrument or context.decision_price_trade_date != prediction_input.decision_date:
        _fail("daily price context symbol or D reference identity differs")
    if price_context_sha256(context) != prediction_input.price_context_sha256:
        _fail("daily price context hash differs from input receipt")
    if (
        not isinstance(context.target_is_st, bool) or context.board_type not in {"MAIN", "STAR", "CHINEXT", "BSE"}
        or not isinstance(context.list_date, date) or context.list_date > prediction_input.decision_date
        or isinstance(context.listed_trading_days, bool) or not isinstance(context.listed_trading_days, int)
        or context.listed_trading_days < 1
        or not all((value := _number(raw)) is not None and value > 0 for raw in
                   (context.decision_raw_close, context.target_raw_price_multiplier, context.tick_size, context.price_unit_divisor))
    ):
        _fail("daily regulatory PIT attributes invalid or unknown")
    if (isinstance(grid_step_ticks, bool) or not isinstance(grid_step_ticks, int) or grid_step_ticks < 1
            or isinstance(maximum_grid_nodes, bool) or not isinstance(maximum_grid_nodes, int) or maximum_grid_nodes < 1):
        _fail("daily grid requires explicit positive integer resolution and budget")
    reference = context.decision_raw_close * context.target_raw_price_multiplier
    regulatory = resolve_regulatory_price_range(context, target_trade_date=prediction_input.target_date)
    common = {
        "schema_version": "entry_loss_daily_advice_v2", "role": "ENTRY_VALUE",
        "objective_contract": "RISK_MANAGED_ADVISORY", "model_bundle_sha256": model_bundle_sha256,
        "model_scope_sha256": model_scope.scope_sha256,
        "training_input_identity_sha256": fitted.original_request.input_identity_sha256,
        "prediction_input_sha256": prediction_input.identity_sha256,
        "prediction_input": prediction_input.model_dump(mode="json"),
        "risk_metric": "entry_net_max_loss_open_close_bps", "risk_quantile": .9,
        "risk_budget": risk_budget.model_dump(mode="json") if risk_budget else None,
        "price_basis": "raw_cny", "reference_raw_cny": reference,
        "regulatory_price_range": regulatory.as_dict(), "grid_step_ticks": grid_step_ticks,
        "tick_size_cny": context.tick_size, "maximum_grid_nodes": maximum_grid_nodes,
        "uncertainty_semantics": fitted.diagnostics["uncertainty_semantics"],
        "validation_return_abs_error_p90_bps": fitted.diagnostics["validation_return_abs_error_p90_bps"],
        "inference_semantics": "observed_price_condition_prediction_not_causal_optimal_execution",
        "training_universe_definition": model_scope.universe_definition_evidence,
        "production_scope_eligible": False, "decision_use": "NAVIGATION_ONLY", "deployable": False,
    }
    if regulatory.status == "NO_DAILY_LIMIT":
        return _publish_advice(common, "QUERY_DOMAIN_UNAVAILABLE", "NO_DAILY_LIMIT_NEEDS_REGISTERED_DOMAIN", [], [])
    tick = Decimal(str(context.tick_size))
    if not tick.is_finite() or tick <= 0:
        _fail("daily grid tick must be finite positive raw-CNY")
    lower, upper = Decimal(str(regulatory.low)), Decimal(str(regulatory.high))
    if lower % tick or upper % tick:
        _fail("regulatory grid bounds are off the CNY tick lattice")
    step = tick * grid_step_ticks
    count = int((upper - lower) // step) + 1
    if count > maximum_grid_nodes:
        return _publish_advice(common, "QUERY_DOMAIN_UNAVAILABLE", "GRID_RESOURCE_BUDGET_EXCEEDED", [], [])
    prices = np.array([float(lower + index * step) for index in range(count)])
    names = model_scope.feature_names
    required = set(names) - {"query_gap_bps"}
    if not required.issubset(decision_features):
        _fail("daily economic prediction missing D feature schema")
    values = {name: value if (value := _number(decision_features[name])) is not None else np.nan for name in required}
    rows = pd.DataFrame(values, index=range(count))
    rows["query_gap_bps"] = (prices / reference - 1) * 10000
    rows = rows.loc[:, names]
    if set(fitted.feature_bounds) != set(names):
        _fail("daily model statistical support schema differs")
    supported = np.isfinite(rows.to_numpy(dtype=float)).all(axis=1)
    for name, (low, high) in fitted.feature_bounds.items():
        if not np.isfinite([low, high]).all() or low > high:
            _fail("daily model statistical support has invalid bounds")
        supported &= rows[name].between(low, high).to_numpy()
    observations, days = [], []
    for index, gap in enumerate(rows.query_gap_bps):
        support = fitted.price_support.get(int(np.floor(gap / fitted.original_request.gap_bin_width_bps)))
        if support is None:
            supported[index] = False
            observations.append(0)
            days.append(0)
            continue
        number, day_count = support["observation_count"], support["decision_day_count"]
        minimum, maximum = support["observed_min_gap_bps"], support["observed_max_gap_bps"]
        if (not isinstance(number, int) or not isinstance(day_count, int) or isinstance(number, bool)
                or isinstance(day_count, bool) or number < fitted.original_request.minimum_bin_observations
                or day_count < fitted.original_request.minimum_bin_days or day_count > number
                or not np.isfinite([minimum, maximum]).all() or minimum > maximum):
            _fail("daily model price support metadata invalid")
        supported[index] &= minimum <= gap <= maximum
        observations.append(number)
        days.append(day_count)
    expected, risk = np.full(count, np.nan), np.full(count, np.nan)
    if supported.any():
        usable = rows.loc[supported].astype(float)
        expected[supported] = fitted.return_model.predict(usable, num_threads=2)
        risk[supported] = fitted.risk_model.predict(usable, num_threads=2)
        if (not np.isfinite(expected[supported]).all() or not np.isfinite(risk[supported]).all()
                or (risk[supported] < 0).any() or (risk[supported] > 10000).any()):
            _fail("daily economic model returned invalid value/entry-loss estimate")
    accepted = supported & (expected > 0) & (risk <= risk_budget.maximum_loss_bps) if risk_budget else np.zeros(count, dtype=bool)
    nodes = [{"price_cny": float(price), "status": "OUT_OF_SUPPORT" if not supported[index] else
              "RISK_CONTRACT_UNCONFIGURED" if risk_budget is None else "ACCEPTABLE" if accepted[index] else "REJECTED",
              "expected_net_return_bps": float(expected[index]) if supported[index] else None,
              "entry_net_max_loss_q90_bps": float(risk[index]) if supported[index] else None,
              "support_observations": observations[index], "support_decision_days": days[index]}
             for index, price in enumerate(prices)]
    intervals, start = [], None
    for index in range(count + 1):
        if index < count and accepted[index]:
            start = index if start is None else start
        elif start is not None:
            intervals.append({"minimum_cny": float(prices[start]), "maximum_cny": float(prices[index - 1]),
                              "grid_step_cny": float(step)})
            start = None
    common["decision_feature_sha256"] = canonical_json_sha256({name: value if np.isfinite(value) else None
                                                               for name, value in values.items()})
    status = ("UNAVAILABLE" if not supported.any() else "RISK_CONTRACT_UNCONFIGURED" if risk_budget is None else "ACCEPTABLE_PRICE_SET" if intervals
              else "NO_ACCEPTABLE_PRICE" if supported.all() else "UNAVAILABLE")
    return _publish_advice(common, status, "NO_SUPPORTED_PRICE_OR_D_FEATURES" if not supported.any() else None, nodes, intervals)


def _publish_advice(common, status, reason, nodes, intervals):
    result = {**common, "recommendation_status": status, "reason_code": reason,
              "query_nodes": nodes, "acceptable_price_intervals": intervals,
              "validity": "exact_D_frozen_grid_nodes_only_and_actual_entry_must_be_executable"}
    supported = sum(value["status"] != "OUT_OF_SUPPORT" for value in nodes)
    result["availability"] = "COMPLETE" if nodes and supported == len(nodes) else "PARTIAL" if supported else "UNAVAILABLE"
    result["advice_sha256"] = canonical_json_sha256(result)
    return result


def select_daily_entry_loss_advice_v2(
    *, advice: Mapping[str, Any], expected_advice_sha256: str, observation_date: date,
    actual_open_raw_cny: float | None, trading_status: str,
) -> dict[str, Any]:
    payload = dict(advice)
    digest = payload.pop("advice_sha256", None)
    if digest != expected_advice_sha256 or canonical_json_sha256(payload) != digest:
        _fail("v2 opening observation does not match D-frozen advice")
    if advice.get("role") != "ENTRY_VALUE" or advice.get("schema_version") != "entry_loss_daily_advice_v2":
        _fail("v2 selection requires its own economic role/version")
    if observation_date.isoformat() != advice["prediction_input"]["target_date"]:
        _fail("v2 opening observation target mismatch")
    if trading_status in {"SUSPENDED", "LIMIT_UP"}:
        return {"action": "NOT_APPLICABLE", "reason_code": trading_status, "advice_sha256": digest}
    actual = _number(actual_open_raw_cny)
    if trading_status != "EXECUTABLE" or actual is None or actual <= 0:
        return {"action": "UNAVAILABLE", "reason_code": "OPEN_OR_TRADABILITY_UNKNOWN", "advice_sha256": digest}
    regulatory = advice["regulatory_price_range"]
    if regulatory["status"] == "LIMITED" and (actual >= regulatory["high"] - 1e-9 or actual < regulatory["low"] - 1e-9):
        return {"action": "UNAVAILABLE", "reason_code": "OPEN_LIMIT_EXECUTION_UNPROVEN", "advice_sha256": digest}
    node = next((value for value in advice["query_nodes"] if abs(value["price_cny"] - actual) <= 1e-9), None)
    if node is None or node["status"] in {"OUT_OF_SUPPORT", "RISK_CONTRACT_UNCONFIGURED"}:
        return {"action": "UNAVAILABLE", "reason_code": "OPEN_OUTSIDE_FROZEN_ACCEPTABLE_DECISION", "advice_sha256": digest}
    if node["status"] not in {"ACCEPTABLE", "REJECTED"}:
        _fail("v2 node contains an invalid decision state")
    return {"action": "TAKE" if node["status"] == "ACCEPTABLE" else "SKIP",
            "reason_code": node["status"], "advice_sha256": digest, "selected_node": dict(node)}
