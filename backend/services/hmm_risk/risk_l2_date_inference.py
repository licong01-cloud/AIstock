"""Label-free, explicit-date consumption of the approved frozen L2 risk model.

Training identity never changes when a compatible inference release changes.
This file consumer is not a legacy preset_A coefficient or a live DB writer.
"""

from __future__ import annotations

from datetime import date
import hashlib
import math
from pathlib import Path
import re

import numpy as np
import pandas as pd

from backend.services.hmm_risk import frozen_l2_history_input as inputs
from backend.services.hmm_risk import risk_l2 as risk
from backend.services.hmm_risk import risk_l2_value_replay as value
from backend.services.hmm_risk import rotation_l1_input_bundle as reader
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_effect import verify_receipt
from backend.services.hmm_risk.formal_state_executor import load_effect_request, validate_output_location
from backend.services.hmm_risk.formal_state_input import _bounded_l2_stock_facts
from backend.services.hmm_risk.formal_state_model import receipt
from backend.services.hmm_risk.stock_fact_observation import build_c010_feature_domain_panel

VERSION = "hmm_risk_l2_date_inference_v1"
# One price predecessor + (14 - 1) ATR predecessors + 250 ranked observations.
CONTEXT_OPEN_DAYS = 264
RELEASE_FIELDS = {"generation", "release_id", "revision", "cutoff", "manifest_sha256", "manifest_file_sha256"}
SOURCE_FIELDS = {
    "candidate_root",
    "release_binding",
    "calendar_sha256",
    "security_identity_manifest",
    "security_identity_sha256",
    "provider_absence_manifest",
    "provider_absence_sha256",
    "industry_authority",
    "work_parent",
}


def require(condition, message, reason="date_inference_invalid"):
    if not condition:
        raise risk.fail(message, reason)


def _day(value):
    require(isinstance(value, str), "canonical ISO date required")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise risk.fail("canonical ISO date required", "date_inference_invalid") from exc
    require(result.isoformat() == value, "canonical ISO date required")
    return result


def validate_request(request):
    verify_receipt(request)
    require(
        set(request)
        == {"schema_version", "model_assets", "source", "decision_dates", "display_limit", "receipt_sha256"}
        and request["schema_version"] == VERSION + "_request",
        "explicit inference request fields/version differ",
    )
    model, source, days = request["model_assets"], request["source"], request["decision_dates"]
    require(
        isinstance(model, dict) and set(model) == {"source_request_path", "sealed_path", "acceptance_path"},
        "model locators differ",
    )
    require(isinstance(source, dict) and set(source) == SOURCE_FIELDS, "source fields differ")
    binding = source["release_binding"]
    require(
        isinstance(binding, dict)
        and set(binding) == RELEASE_FIELDS
        and all(isinstance(v, str) and v and v == v.strip() for v in binding.values()),
        "explicit six-field release binding required",
    )
    cutoff = _day(binding["cutoff"])
    require(isinstance(days, list) and days and days == sorted(set(days)), "dates must be nonempty, sorted and unique")
    require(
        all(_day(risk.TRAIN_END) < _day(day) <= cutoff for day in days),
        "decision date precedes frozen training end or escapes release cutoff",
    )
    require(
        type(request["display_limit"]) is int and 1 <= request["display_limit"] <= 30, "display limit must be 1..30"
    )
    for key in ("calendar_sha256", "security_identity_sha256", "provider_absence_sha256"):
        reader._require_sha256(source[key], key)
    for key in ("manifest_sha256", "manifest_file_sha256"):
        reader._require_sha256(binding[key], key)
    for locator in (
        *model.values(),
        *(source[k] for k in ("candidate_root", "security_identity_manifest", "provider_absence_manifest")),
    ):
        require(isinstance(locator, str) and Path(locator).is_absolute(), "explicit absolute locator required")
        require(
            not any(inputs.reader._is_link(p) for p in (Path(locator), *Path(locator).parents)),
            "indirect input forbidden",
        )
    require(Path(source["candidate_root"]).is_dir(), "candidate root absent")
    validate_output_location(Path(source["work_parent"]), dataset_root=Path(source["candidate_root"]))
    require(isinstance(source["industry_authority"], dict), "explicit industry authority required")


def model_source(request):
    """Authenticate frozen parameters without opening the old feature/label asset."""
    assets, pins = request["model_assets"], value.APPROVED_PINS
    path = Path(assets["source_request_path"])
    before = inputs._stamp(path)
    require(
        hashlib.sha256(path.read_bytes()).hexdigest() == risk.SOURCE_REQUEST_FILE_SHA,
        "original source byte pin differs",
        "identity_mismatch",
    )
    original = load_effect_request(path)
    sealed = inputs._asset(assets["sealed_path"], pins["sealed_hash"])
    acceptance = inputs._asset(assets["acceptance_path"], pins["acceptance_hash"])
    require(
        acceptance.get("model") == {k: v for k, v in sealed.items() if k not in {"predictions", "receipt_sha256"}}
        and acceptance.get("sealed_prediction_sha256") == pins["sealed_hash"]
        and sealed.get("contract") == risk.CONTRACT
        and sealed.get("feature_sha256") == pins["features_hash"]
        and sealed.get("model_sha256") == pins["model_hash"]
        and canonical_sha256(sealed["parameters"]) == pins["model_hash"]
        and canonical_sha256(sealed["input_identity"]) == pins["input_hash"]
        and sealed["input_identity"]["pit_bundle_sha256"]
        == risk.PIT_BUNDLE_SHA
        == original["frozen"]["industry_authority"]["identity"]["bundle_hash"]
        and inputs._stamp(path) == before,
        "original model/acceptance/source linkage differs",
        "identity_mismatch",
    )
    frozen = original["frozen"]
    require(
        sealed["input_identity"]["feature_definition_sha256"] == canonical_sha256(frozen["feature_definition"]),
        "original feature definition differs",
        "identity_mismatch",
    )
    identity = {
        "model_sha256": pins["model_hash"],
        "acceptance_sha256": pins["acceptance_hash"],
        "sealed_sha256": pins["sealed_hash"],
        "source_request_file_sha256": risk.SOURCE_REQUEST_FILE_SHA,
        "training_input_sha256": pins["input_hash"],
        "training_pit_bundle_sha256": risk.PIT_BUNDLE_SHA,
        "feature_definition_sha256": canonical_sha256(frozen["feature_definition"]),
        "eligibility_sha256": canonical_sha256(frozen["eligibility"]),
    }
    return frozen, sealed["parameters"], identity


def date_plan(request):
    source = request["source"]
    path = Path(source["candidate_root"]) / "components/daily_bin_candidate/calendars/day.txt"
    before = inputs._stamp(path)
    calendar_file = inputs.reader._require_file(
        Path(source["candidate_root"]), "components/daily_bin_candidate/calendars/day.txt", source["calendar_sha256"]
    )
    # Study parsers require historical development boundaries. The inference
    # consumer binds the explicit release calendar, not an old study schedule.
    calendar = [_day(line).isoformat() for line in calendar_file.read_text(encoding="utf-8").splitlines()]
    require(
        calendar and calendar == sorted(set(calendar)) and calendar[-1] == source["release_binding"]["cutoff"],
        "release calendar ordering/cutoff differs",
    )
    require(inputs._stamp(path) == before, "calendar changed while read", "identity_mismatch")
    positions = {day: n for n, day in enumerate(calendar)}
    days = request["decision_dates"]
    require(all(day in positions for day in days), "decision date is not an open session")
    require(
        positions[days[0]] >= CONTEXT_OPEN_DAYS,
        "full composed feature context unavailable",
        "source_context_unavailable",
    )
    return {
        "as_of": {day: calendar[positions[day] - 1] for day in days},
        "start": calendar[positions[days[0]] - CONTEXT_OPEN_DAYS],
    }


def prepare(request, source_commit):
    validate_request(request)
    require(isinstance(source_commit, str) and re.fullmatch(r"[0-9a-f]{40}", source_commit), "executor commit invalid")
    frozen, parameters, model_identity = model_source(request)
    source, plan = request["source"], date_plan(request)
    # Input authority may evolve independently, but training eligibility and
    # formulas do not. The shared reader validates official text-code closure.
    data_frozen = {
        **frozen,
        "industry_authority": source["industry_authority"],
        "security_identity_sha256": source["security_identity_sha256"],
        "provider_absence_sha256": source["provider_absence_sha256"],
    }
    assets, window, aggregates, identity = _bounded_l2_stock_facts(
        data_frozen,
        source,
        start=_day(plan["start"]),
        end=_day(plan["as_of"][request["decision_dates"][-1]]),
        prewarm_price_history=True,
        release_binding=source["release_binding"],
        expected_calendar_sha256=source["calendar_sha256"],
    )
    panel, definition, cross = build_c010_feature_domain_panel(
        aggregates,
        trading_dates=window,
        csi300_returns={day: assets["benchmark"][day] for day in window},
        expected_sector_count=131,
        direct_sector_level="L2",
        canonical_sector_codes=frozen["catalog"],
    )
    require(definition == frozen["feature_definition"], "C-010/A5 feature definition changed", "identity_mismatch")
    rows = {day: {} for day in request["decision_dates"]}
    for code in frozen["catalog"]:
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
            "schema_version": VERSION + "_features",
            "request_sha256": request["receipt_sha256"],
            "source_commit": source_commit,
            "model_identity": model_identity,
            "catalog": frozen["catalog"],
            "feature_names": list(ALL_CORE_FEATURES),
            "parameters": parameters,
            "rows": rows,
            "as_of": plan["as_of"],
            "inference_identity": {
                **{k: v for k, v in identity.items() if k not in ("structural_membership", "domain_reasons")},
                "calendar_sha256": source["calendar_sha256"],
                "industry_authority_sha256": canonical_sha256(source["industry_authority"]),
                "security_identity_sha256": source["security_identity_sha256"],
                "provider_absence_sha256": source["provider_absence_sha256"],
                "cross_section_sha256": canonical_sha256(cross),
            },
        }
    )


def validate_bundle(request, bundle, expected_sha256):
    validate_request(request)
    reader._require_sha256(expected_sha256, "parent feature bundle SHA256")
    verify_receipt(bundle, expected_sha256)
    require(
        set(bundle)
        == {
            "schema_version",
            "request_sha256",
            "source_commit",
            "model_identity",
            "catalog",
            "feature_names",
            "parameters",
            "rows",
            "as_of",
            "inference_identity",
            "receipt_sha256",
        },
        "feature bundle fields differ (labels/outcomes forbidden)",
    )
    frozen, parameters, identity = model_source(request)
    plan, source = date_plan(request), request["source"]
    binding = source["release_binding"]
    data = bundle["inference_identity"]
    release = data["release_identity"]
    require(
        bundle["schema_version"] == VERSION + "_features"
        and isinstance(bundle["source_commit"], str)
        and re.fullmatch(r"[0-9a-f]{40}", bundle["source_commit"])
        and bundle["request_sha256"] == request["receipt_sha256"]
        and bundle["model_identity"] == identity
        and bundle["parameters"] == parameters
        and bundle["catalog"] == frozen["catalog"]
        and len(bundle["catalog"]) == 131
        and bundle["catalog"] == sorted(set(bundle["catalog"]))
        and bundle["feature_names"] == list(ALL_CORE_FEATURES)
        and bundle["as_of"] == plan["as_of"]
        and release["frozen_release_generation"] == binding["generation"]
        and release["frozen_release_binding_sha256"] == canonical_sha256(binding)
        and release["dataset_manifest_sha256"] == binding["manifest_sha256"]
        and release["cutoff"] == binding["cutoff"]
        and data["calendar_sha256"] == source["calendar_sha256"]
        and data["industry_authority_sha256"] == canonical_sha256(source["industry_authority"])
        and data["security_identity_sha256"] == source["security_identity_sha256"]
        and data["provider_absence_sha256"] == source["provider_absence_sha256"],
        "feature bundle differs from model/request authorities",
        "identity_mismatch",
    )
    require(set(bundle["rows"]) == set(request["decision_dates"]), "feature date grid differs")
    for day, rows in bundle["rows"].items():
        require(set(rows) == set(bundle["catalog"]), "feature sector grid differs")
        for row in rows.values():
            require(
                set(row) == {"as_of_date", "features", "reason_code"} and row["as_of_date"] == plan["as_of"][day],
                "feature schema/as-of differs",
            )
            values, reason = row["features"], row["reason_code"]
            require(
                (values is None and isinstance(reason, str) and reason.startswith("hmm_risk_"))
                or (
                    isinstance(values, list)
                    and len(values) == 20
                    and reason is None
                    and all(type(v) in (int, float) and math.isfinite(v) for v in values)
                ),
                "feature vector must be twenty finite reals or typed NA",
            )


def infer(request, bundle, *, expected_bundle_sha256):
    validate_bundle(request, bundle, expected_bundle_sha256)
    predictions, changed = risk.predictions_from_parameters(
        bundle["rows"], request["decision_dates"], bundle["catalog"], bundle["parameters"]
    )
    summaries = []
    for day in request["decision_dates"]:
        available = [row for row in predictions if row["trade_date"] == day and row["availability"] == "available"]
        ranked = sorted(available, key=lambda row: (-row["probability"], row["sector_code"]))
        summaries.append(
            {
                "trade_date": day,
                "as_of_date": bundle["as_of"][day],
                "catalog_count": 131,
                "available_count": len(available),
                "unavailable_count": 131 - len(available),
                "display_sector_codes": [row["sector_code"] for row in ranked[: request["display_limit"]]],
            }
        )
    return receipt(
        {
            "schema_version": VERSION + "_prediction",
            "request_sha256": request["receipt_sha256"],
            "features_sha256": bundle["receipt_sha256"],
            "source_commit": bundle["source_commit"],
            "model_identity": bundle["model_identity"],
            "inference_identity": bundle["inference_identity"],
            "capability_state": "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
            if any(row["availability"] == "available" for row in predictions)
            else "NOT_AVAILABLE",
            "predictions": predictions,
            "dates": summaries,
            "inactive_changed_rows": changed,
            "warning_threshold": 0.20,
            "fits": 0,
            "labels_read": False,
            "outcomes_read": False,
            "forward_confirmed": False,
            "policy_applied": False,
        }
    )


def validate_prediction(request, bundle, result):
    """Parent readback checks business fields against its own prepared authority."""
    verify_receipt(result)
    require(
        result["schema_version"] == VERSION + "_prediction"
        and result["request_sha256"] == request["receipt_sha256"]
        and result["features_sha256"] == bundle["receipt_sha256"]
        and result["source_commit"] == bundle["source_commit"]
        and result["model_identity"] == bundle["model_identity"]
        and result["inference_identity"] == bundle["inference_identity"]
        and type(result["fits"]) is int
        and result["fits"] == 0
        and all(result[k] is False for k in ("labels_read", "outcomes_read", "forward_confirmed", "policy_applied"))
        and result["warning_threshold"] == 0.20,
        "prediction identity/counter differs",
        "identity_mismatch",
    )
    rows = result["predictions"]
    grid = [(day, code) for day in request["decision_dates"] for code in bundle["catalog"]]
    require([(row["trade_date"], row["sector_code"]) for row in rows] == grid, "prediction grid differs")
    summaries = []
    for day in request["decision_dates"]:
        available = []
        for row in (item for item in rows if item["trade_date"] == day):
            original = bundle["rows"][day][row["sector_code"]]
            valid = original["features"] is not None
            probability = row["probability"]
            require(
                row["as_of_date"] == bundle["as_of"][day]
                and row["availability"] == ("available" if valid else "unavailable")
                and row["structural_eligible"] is valid
                and row["reason_code"] == (None if valid else original["reason_code"])
                and row["volatility_Nd"]
                == (original["features"][ALL_CORE_FEATURES.index("volatility_Nd")] if valid else None)
                and (
                    (
                        type(probability) in (int, float)
                        and math.isfinite(probability)
                        and 0 <= probability <= 1
                        and row["warning"] is (probability >= 0.20)
                    )
                    if valid
                    else probability is None and row["warning"] is None
                ),
                "prediction value/NA/as-of differs",
                "prediction_failed",
            )
            if valid:
                available.append(row)
        summaries.append(
            {
                "trade_date": day,
                "as_of_date": bundle["as_of"][day],
                "catalog_count": 131,
                "available_count": len(available),
                "unavailable_count": 131 - len(available),
                "display_sector_codes": [
                    row["sector_code"]
                    for row in sorted(available, key=lambda row: (-row["probability"], row["sector_code"]))[
                        : request["display_limit"]
                    ]
                ],
            }
        )
    require(result["dates"] == summaries, "display/coverage readback differs")
    require(
        result["capability_state"]
        == (
            "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
            if any(d["available_count"] for d in summaries)
            else "NOT_AVAILABLE"
        ),
        "availability summary differs",
    )
