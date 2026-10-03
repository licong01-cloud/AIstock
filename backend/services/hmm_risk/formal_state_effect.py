"""Approved L2 post-calibration prediction/effect contract; never fits or selects.

The original train/semantic artifacts remain immutable.  Prediction receives
observations only; outcomes enter a separate, subsequent evaluation boundary.
"""

from __future__ import annotations

from collections import Counter
from datetime import date
import hashlib
import math
from typing import Any, Mapping, Sequence

import numpy as np

from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_model import (
    FormalStateError,
    causal_filter,
    preprocess_apply,
    project_validation,
    receipt,
    restore_model,
)
from backend.services.hmm_risk.rotation_l2 import _newey_west, _rank_ic, _score_and_states

VERSION = "hmm_risk_l2_postcalibration_effect_v1"
ACCEPTANCE_SCHEMA = "hmm_risk_l2_postcalibration_effect_acceptance_v1"
OBSERVATION_SCHEMA = "hmm_risk_l2_postcalibration_observations_v1"
MODEL_SCHEMA = "hmm_risk_l2_postcalibration_frozen_models_v1"
REQUEST_SHA = "94b35f9c8b5767b1a5cb3ade009fcada1fe5811c2486404a9c0e1796b03084f7"
ACCEPTANCE_SHA = "fa42b6982f2ff5d8b5e7c257ad727ba66eb0ee9ac9a18e045847e573704a01ac"
REPEAT_SHA = "e23d7c36fec13aa6a678b3780475e048ad217d764ccce31000bb21b6d2bd89c4"
RESEARCH_SHA = "7f0d2376dd955069444077a3c7821e2aac761b3a4f139577a69d1ec8450a8f2d"
CALENDAR_SHA = "ce017cfbf1d9dde630c0d7f39e33b767e95293acd5258104f80491239826207a"
START = date(2025, 5, 6)
END = date(2026, 3, 31)
CONTINUATION_START = date(2025, 4, 1)
KEY = "autocycle_all_core:L2"
UNMAPPED = {"801033.SI", "801045.SI", "801204.SI", "801743.SI"}
BASIS = "POST_CALIBRATION_RETROSPECTIVE_DEVELOPMENT"
EFFECT_REACHED = "DEVELOPMENT_EFFECT_REACHED_FORWARD_UNCONFIRMED"
CONTRACT = {
    "version": VERSION,
    "family": "autocycle_all_core",
    "level": "L2",
    "seed": 47,
    "feature_names": list(ALL_CORE_FEATURES),
    "decision_window": [START.isoformat(), END.isoformat()],
    "source_outcome_end": END.isoformat(),
    "utility_horizons": [5, 10, 20],
    "utility_weights": [0.35, 0.35, 0.30],
    "binding_mbe_rank_ic": 0.02,
    "mbe_derivation": "CONVENTIONAL_PRIOR_MAGNITUDE_NOT_VALUE_DERIVED",
    "coverage_minimum": 0.90,
    "hard_margin": 1e-12,
    "hac_lag": 19,
    "hac_is_gate": False,
    "validation_basis": BASIS,
}


def fail(message: str, *, reason: str = "input_invalid") -> FormalStateError:
    return FormalStateError(f"hmm_risk_l2_effect_{reason}", message)


def verify_receipt(body: Mapping[str, Any], expected: str | None = None) -> None:
    actual = canonical_sha256({k: v for k, v in body.items() if k != "receipt_sha256"})
    if body.get("receipt_sha256") != actual or (expected is not None and actual != expected):
        raise fail("canonical receipt identity differs", reason="identity_mismatch")


def calendar_contract(calendar: Sequence[str]) -> dict[str, Any]:
    days = [date.fromisoformat(value) for value in calendar]
    if days != sorted(set(days)) or START not in days or END not in days:
        raise fail("frozen calendar is unordered/incomplete")
    decisions = [d for d in days if START <= d <= END]
    maturity = {
        str(h): [d.isoformat() for d in decisions if days.index(d) + h < len(days) and days[days.index(d) + h] <= END]
        for h in (5, 10, 20)
    }
    expected = {"5": (216, "2026-03-24"), "10": (211, "2026-03-17"), "20": (201, "2026-03-03")}
    if len(decisions) != 221 or any(
        len(maturity[h]) != n or maturity[h][-1] != last for h, (n, last) in expected.items()
    ):
        raise fail("approved decision/maturity calendar differs")
    first = days.index(START)
    if first == 0 or days[first - 1] != date(2025, 4, 30):
        raise fail("first as-of is not the approved watermark")
    return {
        "decisions": [d.isoformat() for d in decisions],
        "maturity": maturity,
        "as_of": {d.isoformat(): days[days.index(d) - 1].isoformat() for d in decisions},
    }


def extract_frozen_models(
    request: Mapping[str, Any],
    acceptance: Mapping[str, Any],
    research: Mapping[str, Any],
    child: Mapping[str, Any],
) -> dict[str, Any]:
    """Authenticate one historical child, extract only selected parameters/prefix.

    No train-global preprocessing is recomputed; no D5 function is called.
    The pinned request authenticates the frozen A5 ledger and old observations.
    """
    for payload, pin in (
        (request, REQUEST_SHA),
        (acceptance, ACCEPTANCE_SHA),
        (research, RESEARCH_SHA),
        (child, REPEAT_SHA),
    ):
        verify_receipt(payload, pin)
    codes = request["sector_codes"]["L2"]
    if (
        codes != sorted(set(codes))
        or len(codes) != 131
        or acceptance["request_sha256"] != REQUEST_SHA
        or acceptance["repeat_sha256"] != REPEAT_SHA
        or acceptance["fresh_process_bitwise_equal"] is not True
        or child["request_sha256"] != REQUEST_SHA
        or research["request_sha256"] != REQUEST_SHA
        or research["original_acceptance_sha256"] != ACCEPTANCE_SHA
        or research["selected_seed"] != 47
        or acceptance["selection"][KEY]["selected_seed"] != 47
        or acceptance["selection"][KEY]["accepted"] is not True
    ):
        raise fail("original source/seed/selected-group closure differs", reason="identity_mismatch")
    group = child["groups"][KEY]
    selected = [candidate for candidate in group["candidates"] if candidate["seed"] == 47]
    if len(selected) != 1 or sorted(selected[0]["entries"]) != codes or sorted(research["semantic"]) != codes:
        raise fail("selected L2 denominator differs")
    models = {}
    null_codes = set()
    for code in codes:
        fitted = selected[0]["entries"][code]
        verify_receipt(fitted)
        result = research["semantic"][code]
        verify_receipt(result)
        model_hash = canonical_sha256(fitted["model"])
        if (
            fitted["accepted"] is not True
            or model_hash != result["selected_model_parameter_sha256"]
            or model_hash != acceptance["semantic"][KEY][code]["selected_model_parameter_sha256"]
            or result["selected_identity"]
            != {"family": "autocycle_all_core", "level": "L2", "sector": code, "seed": 47}
        ):
            raise fail(f"model identity differs: {code}", reason="identity_mismatch")
        carrier = request["series"][KEY][code]["validation"]
        model = restore_model(fitted["model"])
        positions = carrier["observation_available_positions"]
        values = project_validation(
            np.asarray(carrier["observation_values_f64"], dtype=np.float64), group["preprocess"], fitted["projection"]
        )
        posterior = causal_filter(model, positions, values, len(request["validation_calendar"]))
        if not np.array_equal(posterior, np.asarray(acceptance["semantic"][KEY][code]["posterior"])):
            raise fail(f"original causal prefix readback differs: {code}", reason="prefix_mismatch")
        mapping = result["mapping"] if result["semantic_evidence_valid"] else None
        if mapping is None:
            null_codes.add(code)
        elif set(mapping) != {"0", "1", "2"} or set(mapping.values()) != {"fading", "neutral", "trending"}:
            raise fail(f"frozen semantic mapping invalid: {code}")
        utilities = {str(state["state"]): state["utility_mean"] for state in result["states"]}
        if mapping is not None and (
            set(utilities) != {"0", "1", "2"} or not all(math.isfinite(float(v)) for v in utilities.values())
        ):
            raise fail(f"frozen utility means invalid: {code}")
        models[code] = {
            "model": fitted["model"],
            "model_sha256": model_hash,
            "projection": fitted["projection"],
            "prefix_positions": positions,
            "prefix_values": carrier["observation_values_f64"],
            "prefix_posterior": posterior.tolist(),
            "mapping": mapping,
            "utility_means": utilities,
            "semantic_receipt_sha256": result["receipt_sha256"],
        }
    if null_codes != UNMAPPED:
        raise fail("approved four mapping-insufficient sectors differ")
    eligibility = request["policy"]["eligibility_receipt"]
    verify_receipt(eligibility, request["policy"]["eligibility_receipt_sha256"])
    entries = eligibility["entries"]
    eligibility_map = {item["canonical_ts_code"]: item["moneyflow_contributor_eligible"] for item in entries}
    if len(eligibility_map) != len(entries) or any(type(v) is not bool for v in eligibility_map.values()):
        raise fail("frozen train contributor identity is duplicated/invalid")
    names = {
        row["canonical_l2_code"]: row["canonical_l2_name"]
        for row in request["industry_authority"]["l2_projection"]["rows"]
    }
    if set(names) != set(codes):
        raise fail("official text-code name projection differs")
    return receipt(
        {
            "schema_version": MODEL_SCHEMA,
            "contract": CONTRACT,
            "original_request_sha256": REQUEST_SHA,
            "original_acceptance_sha256": ACCEPTANCE_SHA,
            "original_repeat_sha256": REPEAT_SHA,
            "research_sha256": RESEARCH_SHA,
            "catalog": codes,
            "names": names,
            "preprocess": group["preprocess"],
            "models": models,
            "prefix_calendar": request["validation_calendar"],
            "eligibility": eligibility_map,
            "eligibility_receipt_sha256": eligibility["receipt_sha256"],
            "feature_definition": request["policy"]["l2_feature_definition"],
            "industry_authority": request["industry_authority"],
            "source_identity": request["source_identity"],
            "security_identity_sha256": request["policy"]["security_identity_manifest_sha256"],
            "provider_absence_sha256": request["policy"]["provider_absence_manifest_sha256"],
            "fits": 0,
            "selection_performed": False,
        }
    )


def validate_models(frozen: Mapping[str, Any]) -> None:
    verify_receipt(frozen)
    if (
        frozen.get("schema_version") != MODEL_SCHEMA
        or frozen.get("contract") != CONTRACT
        or frozen.get("original_request_sha256") != REQUEST_SHA
        or frozen.get("original_acceptance_sha256") != ACCEPTANCE_SHA
        or frozen.get("original_repeat_sha256") != REPEAT_SHA
        or frozen.get("research_sha256") != RESEARCH_SHA
        or type(frozen.get("fits")) is not int
        or frozen.get("fits") != 0
        or frozen.get("selection_performed") is not False
        or frozen["catalog"] != sorted(set(frozen["catalog"]))
        or len(frozen["catalog"]) != 131
        or set(frozen["models"]) != set(frozen["catalog"])
        or set(frozen["names"]) != set(frozen["catalog"])
    ):
        raise fail("frozen-model contract/lineage differs", reason="identity_mismatch")
    for code, item in frozen["models"].items():
        if canonical_sha256(item["model"]) != item["model_sha256"]:
            raise fail(f"frozen model parameters changed: {code}", reason="identity_mismatch")
        if item["projection"]["sector_code"] != code:
            raise fail("frozen projection sector differs", reason="identity_mismatch")
        verify_receipt(item["projection"])
        mapping, utilities = item["mapping"], item["utility_means"]
        if mapping is not None and (
            set(mapping) != {"0", "1", "2"}
            or set(mapping.values()) != {"trending", "neutral", "fading"}
            or set(utilities) != {"0", "1", "2"}
            or any(type(value) not in (int, float) or not math.isfinite(value) for value in utilities.values())
        ):
            raise fail("frozen semantic/utility projection differs", reason="identity_mismatch")
    if {code for code, item in frozen["models"].items() if item["mapping"] is None} != UNMAPPED:
        raise fail("mapping-insufficient catalog differs", reason="identity_mismatch")


def predict(frozen: Mapping[str, Any], observations: Mapping[str, Any]) -> dict[str, Any]:
    """Pure prediction boundary: no label, fitting, selection or DB API accepted."""
    validate_models(frozen)
    verify_receipt(observations)
    if (
        observations.get("schema_version") != OBSERVATION_SCHEMA
        or observations.get("model_set_sha256") != frozen["receipt_sha256"]
        or observations.get("feature_names") != list(ALL_CORE_FEATURES)
        or observations.get("eligibility_receipt_sha256") != frozen["eligibility_receipt_sha256"]
        or observations.get("feature_definition_sha256") != canonical_sha256(frozen["feature_definition"])
        or observations.get("tail_accessed") is not False
        or set(observations["sectors"]) != set(frozen["catalog"])
        or "outcomes" in observations
        or "targets" in observations
    ):
        raise fail("observation/label boundary or frozen train identity differs")
    dates = observations["calendar"]
    schedule = calendar_contract(observations["source_calendar"])
    prefix = frozen["prefix_calendar"]
    continuation = [
        day
        for day in observations["source_calendar"]
        if CONTINUATION_START.isoformat() <= day <= schedule["as_of"][END.isoformat()]
    ]
    if (
        len(prefix) != 182
        or prefix[0] != "2024-07-01"
        or prefix[-1] != "2025-03-31"
        or dates != prefix + continuation
        or dates != sorted(set(dates))
        or any(
            set(observations["structural_membership"].get(day, {})) != set(frozen["catalog"]) for day in continuation
        )
    ):
        raise fail("causal calendar prefix/end differs")
    positions = {day: i for i, day in enumerate(dates)}
    scores_by_day = {day: {} for day in schedule["decisions"]}
    raw_rows = {}
    inactive_receipts = []
    for code in frozen["catalog"]:
        item = frozen["models"][code]
        obs = observations["sectors"][code]
        available_positions = obs["positions"]
        if any(p < len(prefix) for p in available_positions):
            raise fail("new observations overwrite original prefix")
        all_positions = item["prefix_positions"] + available_positions
        raw = np.asarray(item["prefix_values"] + obs["values"], dtype=np.float64)
        processed = project_validation(raw, frozen["preprocess"], item["projection"])
        inactive = item["projection"]["inactive_feature_indices"]
        full_processed = preprocess_apply(raw, frozen["preprocess"]) if inactive else None
        observation_slots = {p: i for i, p in enumerate(all_positions)}
        entry_hash = canonical_sha256(item) if inactive else None
        posterior = causal_filter(restore_model(item["model"]), all_positions, processed, len(dates))
        if not np.array_equal(posterior[: len(prefix)], np.asarray(item["prefix_posterior"])):
            raise fail(f"prefix changed after observation continuation: {code}", reason="prefix_mismatch")
        valid_observations = set(all_positions)
        for day in schedule["decisions"]:
            as_of = schedule["as_of"][day]
            p = positions[as_of]
            if inactive and p in observation_slots:
                slot = observation_slots[p]
                for index in inactive:
                    raw_value = float(raw[slot, index])
                    transformed_value = float(full_processed[slot, index])
                    inactive_receipts.append(
                        receipt(
                            {
                                "schema_version": "hmm_risk_inactive_dimension_observation_receipt_v1",
                                "model_set_id": frozen["receipt_sha256"],
                                "model_entry_sha256": entry_hash,
                                "projection_sha256": item["projection"]["receipt_sha256"],
                                "family": "autocycle_all_core",
                                "level": "L2",
                                "sector_code": code,
                                "trade_date": day,
                                "as_of_date": as_of,
                                "input_manifest_source_sha256": observations["receipt_sha256"],
                                "feature_index": index,
                                "feature_name": ALL_CORE_FEATURES[index],
                                "raw_value_f64": raw_value,
                                "preprocessed_value_f64": transformed_value,
                                "raw_value_float64_sha256": _float64_sha256(raw_value),
                                "preprocessed_value_float64_sha256": _float64_sha256(transformed_value),
                                "raw_value_is_finite": True,
                                "preprocessed_value_is_finite": True,
                                "inactive_feature_observed_non_zero": raw_value != 0.0,
                            }
                        )
                    )
            structural = observations["structural_membership"][as_of][code]
            if type(structural) is not bool:
                raise fail("structural population is not boolean")
            mapping = item["mapping"]
            reason = None
            raw_score = state = hard_state = None
            if not structural:
                reason = "hmm_risk_l2_effect_no_resolved_pit_members"
            elif mapping is None:
                reason = "hmm_risk_l2_effect_mapping_insufficient"
            elif p not in valid_observations:
                reason = obs["na_reasons"].get(as_of)
                if not isinstance(reason, str) or not reason:
                    raise fail(f"missing typed observation NA: {code}/{as_of}")
            else:
                ordered = np.sort(posterior[p])
                if ordered[-1] - ordered[-2] <= CONTRACT["hard_margin"]:
                    reason = "hmm_risk_model_posterior_tie"
                else:
                    hard_state = int(np.argmax(posterior[p]))
                    state = mapping[str(hard_state)]
                    raw_score = float(item["utility_means"][str(hard_state)])
                    if not math.isfinite(raw_score):
                        raise fail("hard utility is non-finite")
                    scores_by_day[day][code] = raw_score
            raw_rows[(day, code)] = {
                "trade_date": day,
                "as_of_date": as_of,
                "sector_level": "L2",
                "sector_code": code,
                "sector_name": frozen["names"][code],
                "raw_score": raw_score,
                "semantic_state": state,
                "hard_state": hard_state,
                "structural_eligible": structural,
                "feature_eligible": p in valid_observations,
                "reason_code": reason,
                "model_parameter_sha256": item["model_sha256"],
            }
    predictions = []
    for day in schedule["decisions"]:
        raw_scores = scores_by_day[day]
        ranks, groups = _score_and_states(raw_scores) if len(raw_scores) >= 2 else ({}, {})
        for code in frozen["catalog"]:
            row = raw_rows[(day, code)]
            available = code in ranks
            if row["raw_score"] is not None and not available:
                row["reason_code"] = "hmm_risk_l2_effect_cross_section_rank_unavailable"
            predictions.append(
                {
                    **row,
                    "rotation_score": ranks.get(code),
                    "forecast_state": row["semantic_state"] if available else None,
                    "daily_rank_group": groups.get(code),
                    "availability": "available" if available else "unavailable",
                    "feature_contributions": (
                        {
                            "hard_state": row["hard_state"],
                            "frozen_utility_mean": row["raw_score"],
                            "semantic_state": row["semantic_state"],
                            "average_rank_score": ranks[code],
                            "daily_rank_group": groups[code],
                            "model_parameter_sha256": row["model_parameter_sha256"],
                        }
                        if available
                        else None
                    ),
                    "outcome_status": "PENDING_EVALUATION",
                }
            )
    return receipt(
        {
            "schema_version": VERSION + "_predictions",
            "contract": CONTRACT,
            "model_set_sha256": frozen["receipt_sha256"],
            "observation_sha256": observations["receipt_sha256"],
            "predictions": predictions,
            "inactive_dimension_observation_receipts": inactive_receipts,
            "fits": 0,
            "selection_performed": False,
            "target_accessed": False,
            "tail_accessed": False,
        }
    )


def _float64_sha256(value: float) -> str:
    return hashlib.sha256(np.asarray(value, dtype="<f8").tobytes()).hexdigest()


def composite_outcomes(
    calendar: Sequence[str],
    daily_returns: Mapping[str, Mapping[str, float | None]],
    benchmark: Mapping[str, float],
    catalog: Sequence[str],
) -> dict[str, Any]:
    """Called only after predictions are sealed; t+1..t+h, never t or as-of."""
    schedule = calendar_contract(calendar)
    lookup = {day: i for i, day in enumerate(calendar)}
    result = {}
    components = {}
    for day in schedule["decisions"]:
        result[day], components[day] = {}, {}
        for code in catalog:
            per_horizon = {}
            for horizon in (5, 10, 20):
                window = calendar[lookup[day] + 1 : lookup[day] + horizon + 1]
                value = None
                if len(window) == horizon and day in schedule["maturity"][str(horizon)]:
                    returns = [daily_returns[d][code] for d in window]
                    market = [benchmark[d] for d in window]
                    if all(v is not None and math.isfinite(float(v)) for v in returns + market):
                        value = float(np.sum(np.asarray(returns) - np.asarray(market)))
                per_horizon[str(horizon)] = value
            components[day][code] = per_horizon
            values = [per_horizon[str(h)] for h in (5, 10, 20)]
            result[day][code] = (
                float(sum(w * v for w, v in zip((0.35, 0.35, 0.30), values, strict=True)))
                if all(v is not None for v in values)
                else None
            )
    return receipt(
        {
            "schema_version": VERSION + "_outcomes",
            "calendar": list(calendar),
            "outcomes": result,
            "components": components,
            "tail_accessed": False,
        }
    )


def _spread(scores: Mapping[str, float], outcomes: Mapping[str, float]) -> float | None:
    # Freeze extreme groups on the prediction population, not on future label
    # availability. Legal outcome NA may reduce diagnostics, never select ranks.
    codes = sorted(scores, key=lambda c: (scores[c], c))
    q = min(len(codes) // 2, math.ceil(0.2 * len(codes)))
    if not q:
        return None
    low, high = set(codes[:q]), set(codes[-q:])
    for score in set(scores[c] for c in codes):
        tied = {c for c in codes if scores[c] == score}
        if tied & low and tied - low:
            low -= tied
        if tied & high and tied - high:
            high -= tied
    low &= set(outcomes)
    high &= set(outcomes)
    if not low or not high:
        return None
    return math.fsum(outcomes[c] for c in high) / len(high) - math.fsum(outcomes[c] for c in low) / len(low)


def evaluate(
    sealed: Mapping[str, Any],
    labels: Mapping[str, Any],
    baseline: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    verify_receipt(sealed)
    verify_receipt(labels)
    if (
        sealed.get("schema_version") != VERSION + "_predictions"
        or sealed.get("contract") != CONTRACT
        or sealed.get("target_accessed") is not False
        or sealed.get("tail_accessed") is not False
        or type(sealed.get("fits")) is not int
        or sealed.get("fits") != 0
        or sealed.get("selection_performed") is not False
        or labels.get("schema_version") != VERSION + "_outcomes"
        or labels.get("tail_accessed") is not False
    ):
        raise fail("sealed prediction/outcome contract differs")
    schedule = calendar_contract(labels["calendar"])
    mature = schedule["maturity"]["20"]
    predictions = [dict(row) for row in sealed["predictions"]]
    codes = sorted({row["sector_code"] for row in predictions})
    by_day = {}
    baseline_by_day = {}
    for rows, output in ((predictions, by_day), (baseline, baseline_by_day)):
        for row in rows:
            day, code = row["trade_date"], row["sector_code"]
            if day not in schedule["decisions"] or code not in codes or row["as_of_date"] != schedule["as_of"][day]:
                raise fail("comparison dates/catalog/as-of differ")
            if code in output.setdefault(day, {}):
                raise fail("duplicate prediction identity")
            output[day][code] = row
        if set(output) != set(schedule["decisions"]) or any(set(d) != set(codes) for d in output.values()):
            raise fail("comparison must retain the entire 131 x 221 grid")
    if len(codes) != 131:
        raise fail("catalog denominator differs")
    daily = []
    ic, baseline_ic, paired_delta = {}, {}, {}
    unavailable = Counter()
    counts = {code: {"structural": 0, "raw": 0, "prediction": 0, "metric": 0} for code in codes}
    total_s = total_p = total_r = 0
    total_baseline_s = total_baseline_p = 0
    horizon_ic = {str(h): {} for h in (5, 10, 20)}
    for day in schedule["decisions"]:
        source = by_day[day]
        structural = {c for c, r in source.items() if r["structural_eligible"]}
        raw = {c for c, r in source.items() if r["raw_score"] is not None}
        predicted = {c for c, r in source.items() if r["availability"] == "available"}
        if not predicted <= raw <= structural:
            raise fail("population nesting differs")
        y = labels["outcomes"][day]
        if set(y) != set(codes):
            raise fail("outcome catalog differs")
        if any(
            value is not None and (type(value) not in (int, float) or not math.isfinite(value)) for value in y.values()
        ):
            raise fail("non-finite composite outcome cannot be a legal NA")
        if any(
            type(source[c]["raw_score"]) not in (int, float) or not math.isfinite(source[c]["raw_score"]) for c in raw
        ):
            raise fail("raw score is not finite")
        metric = {c for c in predicted if y[c] is not None}
        scores = {c: source[c]["raw_score"] for c in metric}
        outcomes = {c: y[c] for c in metric}
        enough = day in mature and len(metric) >= max(2, math.ceil(0.9 * len(predicted)))
        value = _rank_ic(scores, outcomes) if enough else None
        if value is not None:
            ic[date.fromisoformat(day)] = value
        arm = baseline_by_day[day]
        base_p = {c for c, r in arm.items() if r["availability"] == "available"}
        base_s = {c for c, r in arm.items() if r["structural_eligible"]}
        base_m = {c for c in base_p if y[c] is not None}
        base_scores = {c: arm[c]["rotation_score"] for c in base_m}
        base_value = (
            _rank_ic(base_scores, {c: y[c] for c in base_m})
            if day in mature and len(base_m) >= max(2, math.ceil(0.9 * len(base_p)))
            else None
        )
        if base_value is not None:
            baseline_ic[date.fromisoformat(day)] = base_value
        joint = metric & base_m
        left = _rank_ic({c: scores[c] for c in joint}, {c: y[c] for c in joint}) if day in mature else None
        right = _rank_ic({c: base_scores[c] for c in joint}, {c: y[c] for c in joint}) if day in mature else None
        if left is not None and right is not None:
            paired_delta[date.fromisoformat(day)] = left - right
        total_s += len(structural)
        total_p += len(predicted)
        total_r += len(raw)
        total_baseline_s += len(base_s)
        total_baseline_p += len(base_p)
        for h in (5, 10, 20):
            component = labels["components"][day]
            hm = {c for c in predicted if component[c][str(h)] is not None}
            hi = (
                _rank_ic({c: source[c]["raw_score"] for c in hm}, {c: component[c][str(h)] for c in hm})
                if day in schedule["maturity"][str(h)] and len(hm) >= max(2, math.ceil(0.9 * len(predicted)))
                else None
            )
            if hi is not None:
                horizon_ic[str(h)][date.fromisoformat(day)] = hi
        for c, r in source.items():
            for field, population in (
                ("structural", structural),
                ("raw", raw),
                ("prediction", predicted),
                ("metric", metric),
            ):
                counts[c][field] += int(c in population)
            if r["availability"] != "available":
                unavailable[r["reason_code"]] += 1
            r["outcome_status"] = (
                "prediction_unavailable"
                if c not in predicted
                else "outcome_not_mature"
                if day not in mature
                else "available"
                if y[c] is not None
                else "outcome_legal_na"
            )
        daily.append(
            {
                "trade_date": day,
                "D": len(codes),
                "S": len(structural),
                "R": len(raw),
                "P": len(predicted),
                "M": len(metric),
                "rank_ic": value,
                "spread": _spread({c: source[c]["raw_score"] for c in predicted}, outcomes) if enough else None,
                "baseline_P": len(base_p),
                "baseline_M": len(base_m),
                "baseline_rank_ic": base_value,
                "paired_count": len(joint),
                "paired_prediction_coverage": len(joint) / len(predicted) if predicted else None,
                "paired_baseline_coverage": len(joint) / len(base_p) if base_p else None,
                "paired_hmm_rank_ic": left,
                "paired_baseline_rank_ic": right,
            }
        )
        daily[-1]["population_masks"] = {
            "S": sorted(structural),
            "R": sorted(raw),
            "P": sorted(predicted),
            "M": sorted(metric),
            "baseline_S": sorted(base_s),
            "baseline_P": sorted(base_p),
            "baseline_M": sorted(base_m),
            "paired": sorted(joint),
        }
    mean = math.fsum(ic.values()) / len(ic) if ic else None
    coverage = total_p / total_s if total_s else None
    evidence = coverage is not None and coverage >= 0.9 and len(ic) / len(mature) >= 0.9
    effect = "EVIDENCE_INSUFFICIENT" if not evidence else EFFECT_REACHED if mean >= 0.02 else "BELOW_BINDING_MBE"
    blocks = []
    for start, end in (("2025-05-06", "2025-09-30"), ("2025-10-01", "2026-03-31")):
        rows = [row for row in daily if start <= row["trade_date"] <= end]
        values = [row["rank_ic"] for row in rows if row["rank_ic"] is not None]
        blocks.append(
            {
                "start": start,
                "end": end,
                "decision_count": len(rows),
                "mature_count": sum(r["trade_date"] in mature for r in rows),
                "valid_ic_count": len(values),
                "mean_daily_rank_ic": math.fsum(values) / len(values) if values else None,
            }
        )
    mature_dates = [date.fromisoformat(day) for day in mature]
    return {
        "effect_status": effect,
        "predictions": predictions,
        "metrics": {
            "overall": {
                "mean_daily_rank_ic": mean,
                "prediction_coverage": coverage,
                "catalog_prediction_coverage": total_p / (131 * 221),
                "raw_prediction_coverage": total_r / total_s if total_s else None,
            },
            "mature_day_count": len(mature),
            "valid_ic_day_count": len(ic),
            "valid_ic_day_share": len(ic) / len(mature),
            "evidence_sufficient": evidence,
            "coverage_sufficient": coverage is not None and coverage >= 0.9,
            "hac": _newey_west(mature_dates, ic, lag=19),
            "blocks": blocks,
            "daily": daily,
            "industry_missingness": counts,
            "unavailable_reasons": dict(sorted(unavailable.items())),
            "outcome_status_counts": dict(sorted(Counter(r["outcome_status"] for r in predictions).items())),
            "horizon_diagnostics": {
                str(h): {
                    "planned_mature_days": len(schedule["maturity"][str(h)]),
                    "valid_ic_days": len(horizon_ic[str(h)]),
                    "binding": False,
                    "hac": _newey_west(
                        [date.fromisoformat(d) for d in schedule["maturity"][str(h)]], horizon_ic[str(h)], lag=h - 1
                    ),
                }
                for h in (5, 10, 20)
            },
            "baseline": {
                "version": "hmm_risk_rotation_l2_moneyflow_delta_v1",
                "label": "same_composite_5_10_20",
                "prediction_coverage": total_baseline_p / total_baseline_s if total_baseline_s else None,
                "valid_ic_days": len(baseline_ic),
                "hac": _newey_west(mature_dates, baseline_ic, lag=19),
                "paired_delta_hac": _newey_west(mature_dates, paired_delta, lag=19),
                "promotion_gate": False,
            },
        },
    }


def close_effect_processes(first: Mapping[str, Any], second: Mapping[str, Any]) -> dict[str, Any]:
    for payload in (first, second):
        verify_receipt(payload)
        if (
            payload.get("schema_version") != VERSION + "_repeat"
            or payload.get("contract") != CONTRACT
            or type(payload.get("fits")) is not int
            or payload.get("fits") != 0
            or payload.get("selection_performed") is not False
            or payload.get("tail_accessed") is not False
        ):
            raise fail("fresh-process repeat contract differs")
    if canonical_json_bytes(first) != canonical_json_bytes(second):
        raise fail("fresh-process prediction/effect payloads differ", reason="repeat_mismatch")
    result = first["result"]
    identity = first["evaluation_input_identity"]
    body = {
        "schema_version": ACCEPTANCE_SCHEMA,
        "contract_version": VERSION,
        "run_id": canonical_sha256({"repeat": first["receipt_sha256"], "version": VERSION}),
        "model_hash": identity["model_parameter_set_sha256"],
        "evaluation_contract_hash": canonical_sha256(CONTRACT),
        "input_hash": canonical_sha256(identity),
        "mapping_hash": identity["mapping_sha256"],
        "quote_authority_hash": identity["quote_authority_sha256"],
        "evaluation_input_identity": identity,
        "numeric_environment": first["numeric_environment"],
        "fresh_process_bitwise_equal": True,
        "repeat_sha256": first["receipt_sha256"],
        "planned_fits": 0,
        "completed_fits": 0,
        "selection_performed": False,
        "execution_status": "COMPLETED",
        "effect_status": result["effect_status"],
        "predictions": result["predictions"],
        "metrics": result["metrics"],
        "rotation_l2_capability_status": "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
        if result["effect_status"] == EFFECT_REACHED
        else "NOT_AVAILABLE",
        "research_surface_status": "NOT_AVAILABLE",
        "forward_power_status": "UNAVAILABLE",
        "forward_confirmation": "NOT_STARTED",
        "advisory_status": "NOT_AVAILABLE",
        "validation_basis": BASIS,
        "tail_accessed": False,
        "database_write": False,
        "runtime_action": False,
        "ready": False,
        "phase2_ready": False,
        "limitations": [
            "development_not_untouched_confirmation",
            "stable_taxonomy_backcast_not_as_published",
            "hard_state_utility_scale_may_differ_by_sector",
            "overlapping_outcomes_and_nonrandom_missingness",
            "not_qe_increment_or_risk_precision_evidence",
        ],
    }
    acceptance = {**body, "acceptance_sha256": canonical_sha256(body)}
    validate_acceptance(acceptance)
    return acceptance


def validate_acceptance(acceptance: Mapping[str, Any]) -> None:
    """Strict product-version boundary, including per-sector frozen explanations."""
    body = {k: v for k, v in acceptance.items() if k != "acceptance_sha256"}
    if (
        acceptance.get("schema_version") != ACCEPTANCE_SCHEMA
        or acceptance.get("contract_version") != VERSION
        or acceptance.get("evaluation_contract_hash") != canonical_sha256(CONTRACT)
        or acceptance.get("acceptance_sha256") != canonical_sha256(body)
        or acceptance.get("fresh_process_bitwise_equal") is not True
        or any(
            acceptance.get(k) is not False
            for k in (
                "tail_accessed",
                "selection_performed",
                "database_write",
                "runtime_action",
                "ready",
                "phase2_ready",
            )
        )
        or any(type(acceptance.get(k)) is not int or acceptance[k] != 0 for k in ("planned_fits", "completed_fits"))
    ):
        raise fail("effect acceptance contract differs", reason="identity_mismatch")
    identity = acceptance["evaluation_input_identity"]
    schedule = calendar_contract(identity["source_calendar"])
    params = identity["model_parameter_sha256_by_sector"]
    semantics = identity["semantic_mapping_and_utility_by_sector"]
    if (
        acceptance["input_hash"] != canonical_sha256(identity)
        or acceptance["model_hash"] != canonical_sha256(params)
        or identity["semantic_mapping_sha256"] != canonical_sha256(semantics)
        or identity["original_request_sha256"] != REQUEST_SHA
        or identity["original_acceptance_sha256"] != ACCEPTANCE_SHA
        or identity["research_sha256"] != RESEARCH_SHA
        or identity["model_parameter_set_sha256"] != canonical_sha256(params)
        or acceptance["mapping_hash"] != identity["mapping_sha256"]
        or acceptance["quote_authority_hash"] != identity["quote_authority_sha256"]
        or set(params) != set(semantics)
        or len(params) != 131
        or {code for code, (mapping, _) in semantics.items() if mapping is None} != UNMAPPED
    ):
        raise fail("effect identity/model/mapping differs", reason="identity_mismatch")
    diagnostics = identity["inactive_dimension_observation_receipts"]
    keys = []
    for item in diagnostics:
        verify_receipt(item)
        code, day = item["sector_code"], item["trade_date"]
        raw, transformed = item["raw_value_f64"], item["preprocessed_value_f64"]
        if (
            item["schema_version"] != "hmm_risk_inactive_dimension_observation_receipt_v1"
            or code != "801207.SI"
            or day not in schedule["decisions"]
            or item["as_of_date"] != schedule["as_of"][day]
            or item["family"] != "autocycle_all_core"
            or item["level"] != "L2"
            or type(item["feature_index"]) is not int
            or item["feature_index"] != 19
            or item["feature_name"] != ALL_CORE_FEATURES[19]
            or item["model_set_id"] != identity["frozen_model_set_sha256"]
            or item["model_entry_sha256"] != identity["model_entry_sha256_by_sector"][code]
            or item["projection_sha256"] != identity["projection_sha256_by_sector"][code]
            or item["input_manifest_source_sha256"] != identity["observation_sha256"]
            or any(
                isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v)
                for v in (raw, transformed)
            )
            or item["raw_value_is_finite"] is not True
            or item["preprocessed_value_is_finite"] is not True
            or item["raw_value_float64_sha256"] != _float64_sha256(raw)
            or item["preprocessed_value_float64_sha256"] != _float64_sha256(transformed)
            or item["inactive_feature_observed_non_zero"] is not (raw != 0.0)
        ):
            raise fail("inactive observation diagnostic identity/value differs", reason="identity_mismatch")
        keys.append((code, day))
    expected = sorted(
        (row["sector_code"], row["trade_date"])
        for row in acceptance["predictions"]
        if row["sector_code"] == "801207.SI" and row["feature_eligible"]
    )
    if keys != expected:
        raise fail("inactive observation diagnostic denominator/order differs", reason="identity_mismatch")
    daily = {}
    for row in acceptance["predictions"]:
        if row["trade_date"] not in schedule["decisions"] or row["as_of_date"] != schedule["as_of"].get(
            row["trade_date"]
        ):
            raise fail("product decision/as-of calendar differs")
        code = row["sector_code"]
        if code not in params or row["model_parameter_sha256"] != params[code]:
            raise fail("product selected parameter binding differs", reason="identity_mismatch")
        if code in daily.setdefault(row["trade_date"], {}):
            raise fail("duplicate product sector/date")
        daily[row["trade_date"]][code] = row
        mapping, utilities = semantics[code]
        if row["raw_score"] is not None:
            state = row["hard_state"]
            if (
                type(state) is not int
                or state not in (0, 1, 2)
                or mapping is None
                or row["raw_score"] != utilities[str(state)]
                or row["semantic_state"] != mapping[str(state)]
            ):
                raise fail("hard state/utility explanation differs")
        if row["availability"] == "available":
            expected = {
                "hard_state": row["hard_state"],
                "frozen_utility_mean": row["raw_score"],
                "semantic_state": row["semantic_state"],
                "average_rank_score": row["rotation_score"],
                "daily_rank_group": row["daily_rank_group"],
                "model_parameter_sha256": params[code],
            }
            if row["feature_contributions"] != expected or row["forecast_state"] != row["semantic_state"]:
                raise fail("HMM explanation is not its frozen-state projection")
    if len(daily) != 221 or any(set(rows) != set(params) for rows in daily.values()):
        raise fail("product 131 x 221 catalog differs")
    for rows in daily.values():
        raw = {c: row["raw_score"] for c, row in rows.items() if row["raw_score"] is not None}
        scores, groups = _score_and_states(raw) if len(raw) >= 2 else ({}, {})
        if any(
            row["rotation_score"] != scores.get(c) or row["daily_rank_group"] != groups.get(c)
            for c, row in rows.items()
        ):
            raise fail("product average ranks/daily groups differ")
        for code, row in rows.items():
            expected_available = code in scores
            if (
                type(row["structural_eligible"]) is not bool
                or type(row["feature_eligible"]) is not bool
                or row["availability"] != ("available" if expected_available else "unavailable")
                or (row["raw_score"] is not None and not (row["structural_eligible"] and row["feature_eligible"]))
                or (expected_available and row["reason_code"] is not None)
                or (
                    not expected_available
                    and (
                        not isinstance(row["reason_code"], str)
                        or not row["reason_code"]
                        or row["forecast_state"] is not None
                        or row["feature_contributions"] is not None
                    )
                )
            ):
                raise fail("product availability/population coupling differs")
    metrics = acceptance["metrics"]
    mean, coverage = metrics["overall"]["mean_daily_rank_ic"], metrics["overall"]["prediction_coverage"]
    diagnostic_rows = metrics["daily"]
    if [row["trade_date"] for row in diagnostic_rows] != schedule["decisions"]:
        raise fail("daily metric calendar differs")
    daily_values = []
    total_s = total_p = 0
    for diagnostic in diagnostic_rows:
        rows = daily[diagnostic["trade_date"]]
        masks = diagnostic["population_masks"]
        expected_masks = {
            "S": sorted(code for code, row in rows.items() if row["structural_eligible"]),
            "R": sorted(code for code, row in rows.items() if row["raw_score"] is not None),
            "P": sorted(code for code, row in rows.items() if row["availability"] == "available"),
            "M": sorted(code for code, row in rows.items() if row["outcome_status"] == "available"),
        }
        if diagnostic["D"] != 131 or any(
            masks[key] != values or diagnostic[key] != len(values) for key, values in expected_masks.items()
        ):
            raise fail("metric/product population closure differs")
        value = diagnostic["rank_ic"]
        if value is not None:
            if (
                diagnostic["trade_date"] not in schedule["maturity"]["20"]
                or len(expected_masks["M"]) < max(2, math.ceil(0.9 * len(expected_masks["P"])))
                or type(value) not in (int, float)
                or not math.isfinite(value)
                or not -1 <= value <= 1
            ):
                raise fail("metric IC eligibility differs")
            daily_values.append(value)
        for code, row in rows.items():
            statuses = (
                (
                    {"available", "outcome_legal_na"}
                    if diagnostic["trade_date"] in schedule["maturity"]["20"]
                    else {"outcome_not_mature"}
                )
                if code in expected_masks["P"]
                else {"prediction_unavailable"}
            )
            if row["outcome_status"] not in statuses:
                raise fail("product outcome maturity/status differs")
        total_s += diagnostic["S"]
        total_p += diagnostic["P"]
    computed_mean = math.fsum(daily_values) / len(daily_values) if daily_values else None
    computed_coverage = total_p / total_s if total_s else None
    if mean != computed_mean or coverage != computed_coverage or metrics["valid_ic_day_count"] != len(daily_values):
        raise fail("aggregate metric/product population closure differs")
    evidence = coverage is not None and coverage >= 0.9 and metrics["valid_ic_day_count"] / 201 >= 0.9
    status = (
        "EVIDENCE_INSUFFICIENT"
        if not evidence
        else EFFECT_REACHED
        if mean is not None and mean >= 0.02
        else "BELOW_BINDING_MBE"
    )
    if (
        metrics["mature_day_count"] != 201
        or metrics["evidence_sufficient"] is not evidence
        or acceptance["effect_status"] != status
        or acceptance["rotation_l2_capability_status"]
        != ("RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED" if status == EFFECT_REACHED else "NOT_AVAILABLE")
        or acceptance["execution_status"] != "COMPLETED"
        or acceptance["validation_basis"] != BASIS
        or acceptance["forward_confirmation"] != "NOT_STARTED"
        or acceptance["forward_power_status"] != "UNAVAILABLE"
        or acceptance["advisory_status"] != "NOT_AVAILABLE"
        or acceptance["research_surface_status"] != "NOT_AVAILABLE"
    ):
        raise fail("effect/evidence/product status coupling differs")
