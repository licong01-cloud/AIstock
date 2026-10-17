"""One approved continuous L2 risk-budget study; frozen model, zero training."""

from __future__ import annotations

from datetime import date
import math
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.hmm_risk import frozen_l2_history as history
from backend.services.hmm_risk import frozen_l2_history_input as inputs
from backend.services.hmm_risk import risk_l2_value_replay as value
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_effect import verify_receipt
from backend.services.hmm_risk.formal_state_input import _bounded_l2_stock_facts
from backend.services.hmm_risk.formal_state_model import receipt
from backend.services.hmm_risk.stock_fact_observation import build_c010_feature_domain_panel

VERSION = "hmm_risk_l2_continuous_budget_v1"
BINDING = {
    "generation": "20261015-v18-unified-quote1",
    "release_id": "qe_hmm_full_v2_20260831",
    "revision": "20261015-r8-unified-quote1",
    "cutoff": "2026-08-31",
    "manifest_sha256": "f82670728cd6cde06bbc834481187794d081070689648c06b88786a7782e435b",
    "manifest_file_sha256": "1d3602211a7828cc5696d8eb206e49edf1bdccf447cd2a85c0a7186e02b8bf5c",
}
CONTRACT = {
    "version": VERSION,
    "window": ["2026-04-01", "2026-08-31"],
    "prediction_days": 104,
    "return_days": 103,
    "budget_denominator": 131,
    "continuous_formula": "(1-probability)/131",
    "warning_threshold": 0.20,
    "unavailable_policy": "INPUT_UNAVAILABLE_CASH",
    "cost_bps": list(value.COST_BPS),
    "independence_status": "ALREADY_CONSUMED_HISTORY_DEVELOPMENT_ONLY",
    "model_sha256": value.APPROVED_PINS["model_hash"],
    "release_binding": BINDING,
    "fits": 0,
    "forward_confirmed": False,
    "product_activation": False,
}
ARMS = ("B", "R", "X_R", "C", "X_C")
METRICS = ("cumulative_return", "maximum_drawdown", "worst_daily_return", "downside_squared_loss")
require = value.require


def validate_request(request):
    verify_receipt(request)
    require(
        request.get("schema_version") == VERSION + "_request" and request.get("contract") == CONTRACT,
        "approved continuous contract differs",
        "identity_mismatch",
    )
    for key in ("candidate_root", "security_identity_manifest", "provider_absence_manifest", "work_parent"):
        path = Path(request[key])
        require(
            path.is_absolute() and not any(inputs.reader._is_link(p) for p in (path, *path.parents)),
            f"explicit ordinary {key} required",
        )


def _source(request):
    validate_request(request)
    source, _, parameters = inputs._risk_model_source(request)
    # Only model/feature-definition authority survives from the old request;
    # all numeric stock facts must be read from this explicitly frozen v18.
    source["source"] = {
        **source["source"],
        **{
            k: request[k]
            for k in ("candidate_root", "security_identity_manifest", "provider_absence_manifest", "work_parent")
        },
    }
    root = Path(request["candidate_root"])
    calendar_path = inputs.reader._require_file(
        root, "components/daily_bin_candidate/calendars/day.txt", history.CALENDAR_SHA
    )
    calendar = [d.isoformat() for d in inputs.reader._parse_calendar(calendar_path) if d <= history.END]
    history.schedule(calendar)
    return source, parameters, calendar


def validate_bundle(request, bundle):
    """Parent/child closure against request and original model authority, not self hashes."""
    source, parameters, calendar = _source(request)
    verify_receipt(bundle)
    identity = bundle["source_identity"]["release_identity"]
    require(
        bundle["schema_version"] == VERSION + "_risk_features"
        and bundle["contract"] == CONTRACT
        and bundle["request_sha256"] == request["receipt_sha256"]
        and bundle["calendar"] == calendar
        and bundle["catalog"] == source["frozen"]["catalog"]
        and bundle["feature_names"] == list(ALL_CORE_FEATURES)
        and bundle["parameters"] == parameters
        and identity.get("frozen_release_generation") == BINDING["generation"]
        and identity.get("frozen_release_binding_sha256") == canonical_sha256(BINDING)
        and identity.get("dataset_manifest_sha256") == BINDING["manifest_sha256"]
        and identity.get("cutoff") == BINDING["cutoff"],
        "feature bundle differs from parent authorities",
        "identity_mismatch",
    )


def prepare(request, source_commit):
    source, parameters, calendar = _source(request)
    plan = history.schedule(calendar)
    start = date.fromisoformat(calendar[plan["positions"][plan["days"][0]] - 260])
    assets, window, aggregates, identity = _bounded_l2_stock_facts(
        source["frozen"],
        source["source"],
        start=start,
        end=date.fromisoformat(plan["as_of"][plan["days"][-1]]),
        release_binding=BINDING,
    )
    panel, definition, cross = build_c010_feature_domain_panel(
        aggregates,
        trading_dates=window,
        csi300_returns={d: assets["benchmark"][d] for d in window},
        expected_sector_count=131,
        direct_sector_level="L2",
        canonical_sector_codes=source["frozen"]["catalog"],
    )
    require(
        definition == source["frozen"]["feature_definition"],
        "original C-010/A5 definition changed",
        "identity_mismatch",
    )
    rows = {d: {} for d in plan["days"]}
    for code in source["frozen"]["catalog"]:
        frame = panel.xs(code, level="l1_code")
        for day in rows:
            as_of = plan["as_of"][day]
            vector = frame.reindex(pd.to_datetime([as_of]))[list(ALL_CORE_FEATURES)].to_numpy(dtype=np.float64)[0]
            valid = bool(np.isfinite(vector).all())
            rows[day][code] = {
                "as_of_date": as_of,
                "features": vector.tolist() if valid else None,
                "reason_code": None
                if valid
                else identity["domain_reasons"].get((as_of, code), "hmm_risk_c010_observation_unavailable"),
            }
    return receipt(
        {
            "schema_version": VERSION + "_risk_features",
            "contract": CONTRACT,
            "request_sha256": request["receipt_sha256"],
            "source_commit": source_commit,
            "calendar": calendar,
            "catalog": source["frozen"]["catalog"],
            "feature_names": list(ALL_CORE_FEATURES),
            "rows": rows,
            "parameters": parameters,
            "source_identity": {
                k: v for k, v in identity.items() if k not in ("structural_membership", "domain_reasons")
            },
            "cross_section_sha256": canonical_sha256(cross),
        }
    )


def outcomes(request, bundle):
    source, _, calendar = _source(request)
    verify_receipt(bundle)
    require(
        bundle["request_sha256"] == request["receipt_sha256"] and bundle["calendar"] == calendar,
        "outcome request/calendar differs",
        "identity_mismatch",
    )
    _, _, aggregates, identity = _bounded_l2_stock_facts(
        source["frozen"],
        source["source"],
        start=history.START,
        end=history.END,
        prewarm_price_history=True,
        release_binding=BINDING,
    )
    require(
        identity["release_identity"] == bundle["source_identity"]["release_identity"],
        "feature/outcome release changed",
        "identity_mismatch",
    )
    days = history.schedule(calendar)["days"]
    returns = {d: {c: None for c in bundle["catalog"]} for d in days[1:]}
    seen = set()
    for row in aggregates:
        day = row.trade_date.isoformat()
        if day in returns:
            require(row.l1_code in returns[day], "unknown outcome industry")
            require((day, row.l1_code) not in seen, "duplicate outcome industry/date")
            seen.add((day, row.l1_code))
            returns[day][row.l1_code] = float(row.l1_return)
    return receipt(
        {
            "schema_version": VERSION + "_outcomes",
            "contract": CONTRACT,
            "feature_sha256": bundle["receipt_sha256"],
            "returns": returns,
            "source_identity": {
                k: v for k, v in identity.items() if k not in ("structural_membership", "domain_reasons")
            },
        }
    )


def budgets(catalog, signals):
    old, eligible, warnings = value._budgets(catalog, signals)
    weights = {c: (1 - signals[c]["probability"]) / 131 if c in eligible else 0.0 for c in catalog}
    matched = math.fsum(weights.values()) / len(eligible) if eligible else 0.0
    targets = {
        "B": old["B"],
        "R": old["R"],
        "X_R": old["X"],
        "C": weights,
        "X_C": {c: matched if c in eligible else 0.0 for c in catalog},
    }
    require(
        math.isclose(math.fsum(targets["C"].values()), math.fsum(targets["X_C"].values()), abs_tol=1e-12),
        "continuous matched exposure differs",
        "formula_invalid",
    )
    return targets, eligible, warnings


def compare(catalog, days, signals, returns):
    # Reuse the validated original three-arm engine; never duplicate drift or
    # cash/cost arithmetic. X is named X_R in the new five-arm contract.
    baseline = value.replay(catalog, days, signals, returns)
    previous = {a: {c: 0.0 for c in catalog} for a in ("C", "X_C")}
    depleted = {a: False for a in previous}
    daily, opportunity = [], []
    for s, t, old in zip(days[:-1], days[1:], baseline["daily"], strict=True):
        targets, eligible, _ = budgets(catalog, signals[s])
        paired = not old["missing_held_return_sectors"]
        arms = {"B": old["arms"]["B"], "R": old["arms"]["R"], "X_R": old["arms"]["X"]}
        for a in previous:
            arms[a], previous[a], depleted[a] = value.arm_day(
                catalog, targets[a], returns[t], previous[a], depleted[a], paired
            )
        paired = all(arms[a]["gross_return"] is not None for a in ARMS)
        daily.append(
            {
                **old,
                "arms": arms,
                "paired_return_available": paired,
                "reason_code": "REFERENCE_CAPITAL_DEPLETED" if any(depleted.values()) else old["reason_code"],
            }
        )
        opportunity.append(
            {
                "return_date": t,
                "gross_return_removed_C": math.fsum(
                    (targets["B"][c] - targets["C"][c]) * returns[t][c] for c in eligible
                )
                if paired
                else None,
                "positive_gross_opportunity_C": math.fsum(
                    (targets["B"][c] - targets["C"][c]) * max(0, returns[t][c]) for c in eligible
                )
                if paired
                else None,
                "negative_gross_loss_avoided_C": math.fsum(
                    (targets["B"][c] - targets["C"][c]) * min(0, returns[t][c]) for c in eligible
                )
                if paired
                else None,
            }
        )
    dates = days[1:]
    blocks = {
        a: value._blocks(dates, [r["arms"][a]["gross_return"] if r["paired_return_available"] else None for r in daily])
        for a in ARMS
    }
    complete = all(r["paired_return_available"] for r in daily) and any(r["eligible_count"] for r in daily)
    full = {a: blocks[a][0] if complete else None for a in ARMS}
    comparisons = {
        a + "_minus_" + b: {m: full[a][m] - full[b][m] if complete else None for m in METRICS}
        for a, b in (("C", "B"), ("C", "R"), ("C", "X_C"), ("R", "X_R"))
    }
    return receipt(
        {
            "schema_version": VERSION + "_result",
            "contract": CONTRACT,
            "status": "INSUFFICIENT_REFERENCE_PATH"
            if not complete
            else "REFERENCE_RISK_REDUCTION_OBSERVED"
            if comparisons["C_minus_X_C"]["maximum_drawdown"] > 0
            else "REFERENCE_RISK_REDUCTION_NOT_OBSERVED",
            "reference_basis": "GROSS_SYNTHETIC_L2_REFERENCE",
            "net_value_status": "UNASSESSED",
            "forward_confirmed": False,
            "planned_return_dates": len(dates),
            "sector_count": 131,
            "paired_return_dates": sum(r["paired_return_available"] for r in daily),
            "complete_reference_path": complete,
            "prediction_availability": baseline["prediction_availability"],
            "return_start": dates[0],
            "return_end": dates[-1],
            "legal_na_dates": [r["return_date"] for r in daily if r["missing_held_return_sectors"]],
            "gross_blocks": blocks,
            "gross_full_path": full,
            "comparisons": comparisons,
            "cost_sensitivity": {
                a: {
                    str(bp): value._blocks(
                        dates,
                        [
                            r["arms"][a]["cost_sensitivity_return"][str(bp)] if r["paired_return_available"] else None
                            for r in daily
                        ],
                    )
                    for bp in value.COST_BPS
                }
                for a in ARMS
            },
            "exposure": {
                a: {
                    "mean_risk_budget": math.fsum(r["arms"][a]["risk_budget"] for r in daily) / len(daily),
                    "known_turnover_sum": math.fsum(
                        r["arms"][a]["one_sided_risk_turnover"]
                        for r in daily
                        if r["arms"][a]["one_sided_risk_turnover"] is not None
                    ),
                    "unknown_turnover_dates": sum(r["arms"][a]["one_sided_risk_turnover"] is None for r in daily),
                }
                for a in ARMS
            },
            "opportunity_cost": {"daily_continuous": opportunity, "original_binary": baseline["opportunity_cost"]},
            "daily": daily,
            "fits": 0,
            "database_access": False,
            "new_tail_accessed": False,
            "runtime_action": False,
        }
    )


def infer(bundle):
    return history.infer_risk(bundle, bundle["parameters"], contract=CONTRACT, version=VERSION)


def validate_sealed(bundle, sealed):
    verify_receipt(bundle)
    verify_receipt(sealed)
    require(
        bundle["contract"] == CONTRACT and bundle["schema_version"] == VERSION + "_risk_features",
        "feature authority differs",
        "identity_mismatch",
    )
    require(
        sealed["schema_version"] == VERSION + "_risk_sealed"
        and sealed["contract"] == CONTRACT
        and sealed["input_sha256"] == bundle["receipt_sha256"]
        and sealed["model_sha256"] == CONTRACT["model_sha256"]
        and canonical_sha256(bundle["parameters"]) == CONTRACT["model_sha256"]
        and type(sealed["fits"]) is int
        and sealed["fits"] == 0,
        "sealed authority differs",
        "identity_mismatch",
    )
    plan = history.schedule(bundle["calendar"])
    require(
        len(bundle["catalog"]) == 131
        and bundle["catalog"] == sorted(set(bundle["catalog"]))
        and bundle["feature_names"] == list(ALL_CORE_FEATURES),
        "sealed population authority differs",
    )
    expected = {(d, c) for d in plan["days"] for c in bundle["catalog"]}
    rows = sealed["predictions"]
    require(
        len(rows) == len(expected) and {(r["trade_date"], r["sector_code"]) for r in rows} == expected,
        "sealed grid missing/duplicate",
    )
    signals = {d: {} for d in plan["days"]}
    for row in rows:
        require(row["as_of_date"] == plan["as_of"][row["trade_date"]], "sealed as-of differs")
        original = bundle["rows"][row["trade_date"]][row["sector_code"]]
        available = original["features"] is not None
        require(
            row.get("structural_eligible") is available
            and row["availability"] == ("available" if available else "unavailable")
            and row["reason_code"] == (None if available else original["reason_code"])
            and row.get("volatility_Nd")
            == (original["features"][ALL_CORE_FEATURES.index("volatility_Nd")] if available else None),
            "sealed observation eligibility differs",
            "identity_mismatch",
        )
        signals[row["trade_date"]][row["sector_code"]] = row
    for day in signals:
        budgets(bundle["catalog"], signals[day])
    return signals


def evaluate(bundle, sealed, facts):
    signals = validate_sealed(bundle, sealed)
    verify_receipt(facts)
    require(
        facts["schema_version"] == VERSION + "_outcomes"
        and facts["contract"] == CONTRACT
        and facts["feature_sha256"] == bundle["receipt_sha256"]
        and facts["source_identity"]["release_identity"] == bundle["source_identity"]["release_identity"],
        "outcome authority differs",
        "identity_mismatch",
    )
    result = compare(bundle["catalog"], history.schedule(bundle["calendar"])["days"], signals, facts["returns"])
    return receipt(
        {
            **{k: v for k, v in result.items() if k != "receipt_sha256"},
            "prediction_sha256": sealed["receipt_sha256"],
            "outcome_sha256": facts["receipt_sha256"],
        }
    )
