"""Conditional economic advice on a frozen, legal raw-price grid."""

from __future__ import annotations

import math
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import _fail, _number
from backend.services.advisory_model_first.economic_entry_contracts import (
    EconomicEntryInputIdentityV1,
    EconomicEntryTrainingRequestV1,
)
from backend.services.advisory_model_first.economic_entry_training import EconomicEntryTrainingResult
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def build_economic_price_advice(
    *, fitted: EconomicEntryTrainingResult, model_bundle_sha256: str,
    input_identity: EconomicEntryInputIdentityV1, instrument: str,
    decision_date: date, target_date: date, feature_visible_through: date,
    decision_features: Mapping[str, float], reference_raw_cny: float,
    legal_price_min_cny: float, legal_price_max_cny: float,
    query_prices_cny: Sequence[float], grid_step_cny: float, tick_size_cny: float,
) -> dict[str, Any]:
    EconomicEntryTrainingRequestV1.model_validate(fitted.request.model_dump())
    input_identity = EconomicEntryInputIdentityV1.model_validate(input_identity.model_dump())
    if input_identity.identity_sha256 != fitted.request.input_identity_sha256:
        _fail("economic inference input/policy identity differs from fitted model")
    if fitted.request.downside_budget_bps != input_identity.shadow_policy["stop_loss_bps"]:
        _fail("economic downside budget differs from the frozen policy reference")
    if len(instrument) != 9 or not instrument[:6].isdigit() or instrument[6:] not in {".SH", ".SZ", ".BJ"}:
        _fail("economic advice instrument is invalid")
    if not feature_visible_through <= decision_date < target_date:
        _fail("economic inference feature clock is not D-frozen", "ADVISORY_ECONOMIC_FEATURE_PIT")
    if decision_date <= fitted.request.validation_end:
        _fail("economic advice predates the fitted/calibrated model cutoff", "ADVISORY_ECONOMIC_FEATURE_PIT")
    if len(model_bundle_sha256) != 64 or any(char not in "0123456789abcdef" for char in model_bundle_sha256):
        _fail("economic model bundle hash is invalid")
    prices = np.asarray(query_prices_cny, dtype=float)
    controls = (reference_raw_cny, legal_price_min_cny, legal_price_max_cny, grid_step_cny, tick_size_cny)
    if not all(math.isfinite(value) and value > 0 for value in controls):
        _fail("economic price grid requires explicit positive raw-CNY reference/bounds/step/tick")
    if prices.ndim != 1 or len(prices) == 0 or len(prices) > fitted.request.resource_max_rows or not np.isfinite(prices).all():
        _fail("economic price grid is empty or non-finite")
    if legal_price_min_cny > legal_price_max_cny or (prices < legal_price_min_cny).any() or (prices > legal_price_max_cny).any():
        _fail("query grid violates the frozen legal price bounds")
    if (np.diff(prices) <= 0).any() or not np.allclose(np.diff(prices), grid_step_cny, rtol=0, atol=1e-9):
        _fail("query grid must be sorted, unique and uniformly spaced without omitted nodes")
    try:
        tick = Decimal(str(tick_size_cny))
        if Decimal(str(grid_step_cny)) % tick or any(Decimal(str(price)) % tick for price in query_prices_cny):
            _fail("query grid and step must be on the raw-CNY tick lattice")
    except InvalidOperation:
        _fail("query price tick arithmetic is invalid")
    feature_names = fitted.request.feature_names
    required = set(feature_names) - {"query_gap_bps"}
    if not required.issubset(decision_features):
        _fail("economic inference misses frozen feature names")
    values = {name: value if (value := _number(decision_features[name])) is not None else float("nan")
              for name in required}
    rows = pd.DataFrame({name: value for name, value in values.items()}, index=range(len(prices)))
    rows["query_gap_bps"] = (prices / reference_raw_cny - 1) * 10000
    rows = rows.loc[:, feature_names]
    matrix = rows.to_numpy(dtype=float)
    finite = np.isfinite(matrix).all(axis=1)
    feature_supported = finite.copy()
    if set(fitted.feature_bounds) != set(feature_names):
        _fail("model support schema differs from frozen feature order")
    for name, (lower, upper) in fitted.feature_bounds.items():
        if not math.isfinite(lower) or not math.isfinite(upper) or lower > upper:
            _fail("model feature support bounds are invalid")
        feature_supported &= rows[name].between(lower, upper).to_numpy()
    bins = np.floor(rows["query_gap_bps"] / fitted.request.gap_bin_width_bps).astype(int)
    query_gaps = rows["query_gap_bps"].to_numpy(dtype=float)
    supported = feature_supported.copy()
    counts, days = [], []
    for index, bin_id in enumerate(bins):
        support = fitted.price_support.get(int(bin_id))
        if support is None:
            supported[index] = False
            counts.append(0)
            days.append(0)
            continue
        count, day_count = int(support["observation_count"]), int(support["decision_day_count"])
        minimum, maximum = float(support["observed_min_gap_bps"]), float(support["observed_max_gap_bps"])
        if (
            not math.isfinite(minimum) or not math.isfinite(maximum) or minimum > maximum
            or day_count > count
        ):
            _fail("price-bin support metadata is invalid")
        if count < fitted.request.minimum_bin_observations or day_count < fitted.request.minimum_bin_days:
            _fail("model advertises a supported bin below its pre-registered support threshold")
        supported[index] &= minimum <= query_gaps[index] <= maximum
        counts.append(count)
        days.append(day_count)
    expected = np.full(len(prices), np.nan)
    risk = np.full(len(prices), np.nan)
    if supported.any():
        usable = rows.loc[supported].astype(float)
        expected[supported] = fitted.return_model.predict(usable, num_threads=2)
        risk[supported] = fitted.risk_model.predict(usable, num_threads=2)
        if (
            not np.isfinite(expected[supported]).all() or not np.isfinite(risk[supported]).all()
            or (risk[supported] < 0).any() or (risk[supported] > 10000).any()
        ):
            _fail("economic model returned invalid mean or downside estimates")
    accepted = supported & (expected > fitted.request.minimum_expected_net_value_bps) & (risk <= fitted.request.downside_budget_bps)
    intervals: list[dict[str, float]] = []
    start: int | None = None
    for index in range(len(prices) + 1):
        if index < len(prices) and accepted[index]:
            if start is None:
                start = index
        elif start is not None:
            intervals.append({"minimum_cny": float(prices[start]), "maximum_cny": float(prices[index - 1]),
                              "grid_step_cny": grid_step_cny})
            start = None
    nodes = []
    for index, price in enumerate(prices):
        node_status = "ACCEPTABLE" if accepted[index] else "REJECTED" if supported[index] else "OUT_OF_SUPPORT"
        nodes.append({
            "price_cny": float(price), "status": node_status,
            "expected_net_return_bps": float(expected[index]) if supported[index] else None,
            "daily_mark_drawdown_q90_bps": float(risk[index]) if supported[index] else None,
            "support_observations": counts[index], "support_decision_days": days[index],
        })
    result = {
        "schema_version": "economic_entry_price_advice_v1", "role": "ENTRY_VALUE",
        "objective_contract": "RISK_MANAGED_ADVISORY", "model_bundle_sha256": model_bundle_sha256,
        "training_request_sha256": fitted.request.request_sha256,
        "input_identity_sha256": fitted.request.input_identity_sha256,
        "instrument": instrument, "package_id": input_identity.package_id,
        "program_id": input_identity.program_id,
        "shadow_policy_sha256": input_identity.shadow_policy_sha256,
        "cost_policy_sha256": input_identity.cost_policy_sha256,
        "candidate_roster_sha256": input_identity.candidate_roster_sha256,
        "source_evidence": input_identity.source_evidence,
        "evidence_limitations": list(input_identity.evidence_limitations),
        "decision_feature_sha256": canonical_json_sha256({
            "values": {key: values[key] if math.isfinite(values[key]) else None for key in sorted(values)},
            "decision": decision_date.isoformat(), "instrument": instrument,
        }),
        "decision_date": decision_date.isoformat(), "target_date": target_date.isoformat(),
        "feature_visible_through": feature_visible_through.isoformat(),
        "price_basis": "raw_cny", "reference_raw_cny": reference_raw_cny,
        "legal_price_min_cny": legal_price_min_cny, "legal_price_max_cny": legal_price_max_cny,
        "tick_size_cny": tick_size_cny, "grid_step_cny": grid_step_cny,
        "recommendation_status": "ACCEPTABLE_PRICE_SET" if intervals else "NO_ACCEPTABLE_PRICE" if supported.all() else "UNAVAILABLE",
        "availability": "COMPLETE" if supported.all() else "PARTIAL" if supported.any() else "UNAVAILABLE",
        "acceptable_price_intervals": intervals, "query_nodes": nodes,
        "validity": "frozen_grid_nodes_only_and_actual_entry_must_be_executable",
        "uncertainty": fitted.diagnostics["uncertainty_semantics"],
        "validation_return_abs_error_p90_bps": fitted.diagnostics["validation_return_abs_error_p90_bps"],
        "evidence_level": "HISTORICAL_REPLAY", "decision_use": "NAVIGATION_ONLY", "deployable": False,
    }
    result["advice_sha256"] = canonical_json_sha256(result)
    return result


def select_frozen_opening_advice(
    *, advice: Mapping[str, Any], expected_advice_sha256: str,
    observation_date: date, actual_open_raw_cny: float | None, trading_status: str,
) -> dict[str, Any]:
    payload = dict(advice)
    digest = payload.pop("advice_sha256", None)
    if digest != expected_advice_sha256 or canonical_json_sha256(payload) != digest:
        _fail("opening selection does not match the D-frozen advice identity")
    if advice.get("role") != "ENTRY_VALUE" or advice.get("schema_version") != "economic_entry_price_advice_v1":
        _fail("opening selection requires the economic entry role, not a price distribution")
    if observation_date.isoformat() != advice["target_date"]:
        _fail("opening observation does not match the frozen target date")
    if trading_status in {"SUSPENDED", "LIMIT_UP"}:
        return {"action": "NOT_APPLICABLE", "reason_code": trading_status, "advice_sha256": digest}
    if trading_status != "EXECUTABLE" or actual_open_raw_cny is None or not math.isfinite(actual_open_raw_cny) or actual_open_raw_cny <= 0:
        return {"action": "UNAVAILABLE", "reason_code": "OPEN_OR_TRADABILITY_UNKNOWN", "advice_sha256": digest}
    node = next((node for node in advice["query_nodes"] if math.isclose(node["price_cny"], actual_open_raw_cny, rel_tol=0, abs_tol=1e-9)), None)
    if node is None or node["status"] == "OUT_OF_SUPPORT":
        return {"action": "UNAVAILABLE", "reason_code": "OPEN_OUTSIDE_FROZEN_SUPPORT", "advice_sha256": digest}
    if node["status"] not in {"ACCEPTABLE", "REJECTED"}:
        _fail("frozen opening node has an unknown action status")
    return {"action": "TAKE" if node["status"] == "ACCEPTABLE" else "SKIP",
            "reason_code": node["status"], "advice_sha256": digest, "selected_node": dict(node)}
