"""Current D6-NA finite compact carrier and its shared write/read validator.

Missing observations remain calendar positions, never synthetic numeric rows.
This module neither fits models nor introduces availability acceptance gates.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

import numpy as np

from backend.services.hmm_risk.contracts import canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_model import FormalStateError

CARRIER_VERSION = "hmm_risk_d6_validation_calendar_series_v1"
MANIFEST_VERSION = "hmm_risk_d6_frozen_input_manifest_v2"
BASE_VERSION = "hmm_risk_c008_b3_d6_01_b_v1"
AVAILABILITY_VERSION = "hmm_risk_c008_b3_d6_na_a_v1"
COMPONENT_WEIGHTS = {"excess_return_5d": 0.35, "excess_return_10d": 0.35, "excess_return_20d": 0.30}
OBSERVATION_UNAVAILABLE = "hmm_risk_semantic_validation_observation_unavailable"
UTILITY_UNAVAILABLE = "hmm_risk_semantic_validation_utility_unavailable"
MISMATCH = "hmm_risk_semantic_validation_availability_receipt_mismatch"


def _fail(message: str) -> None:
    raise FormalStateError(MISMATCH, message, evidence={"assignment_status": "failed"})


def _sha(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _finite(value: Any, shape: tuple[int, ...]) -> np.ndarray:
    def numeric(v: Any) -> bool:
        return all(numeric(item) for item in v) if isinstance(v, list) else type(v) in (int, float)

    if not isinstance(value, list) or not numeric(value):
        _fail("compact payload contains a non-numeric value or placeholder")
    result = np.asarray(value, dtype=np.float64)
    if shape[0] == 0 and result.shape == (0,):
        result = result.reshape(shape)
    if result.shape != shape or not np.isfinite(result).all():
        _fail("compact payload shape/finite contract differs")
    return result


def _positions(mask: Any, positions: Any, total: int) -> None:
    if not isinstance(mask, list) or len(mask) != total or any(type(item) is not bool for item in mask):
        _fail("availability mask must be a full boolean calendar vector")
    if (
        not isinstance(positions, list)
        or any(type(p) is not int or not 0 <= p < total for p in positions)
        or positions != [i for i, available in enumerate(mask) if available]
    ):
        _fail("mask and strictly increasing integer positions differ")


def _manifest(body: Mapping[str, Any]) -> dict[str, Any]:
    groups = {
        "calendar": [body["calendar_dates"], body["calendar_positions"]],
        "full_features": body["feature_names"],
        "observation_mask": body["observation_available_mask"],
        "observation_positions": body["observation_available_positions"],
        "observation_payload": body["observation_values_f64"],
        "components": body["components"],
        "utility_mask": body["utility_available_mask"],
        "utility_positions": body["utility_available_positions"],
        "utility_payload": body["combined_values_f64"],
        "source_receipts": [body["source_cutoff"], body["source_identity_sha256"], body["daily_sources"]],
    }
    manifest = {
        "schema_version": MANIFEST_VERSION,
        "carrier_schema_version": CARRIER_VERSION,
        "hashes": {name: canonical_sha256(value) for name, value in groups.items()},
    }
    return {**manifest, "aggregate_sha256": canonical_sha256(manifest)}


def build_calendar_carrier(
    *,
    dates: Sequence[str],
    feature_names: Sequence[str],
    observations: np.ndarray,
    components: Mapping[str, Mapping[str, Any]],
    source_identity_sha256: str,
    source_receipt_sha256: str,
) -> dict[str, Any]:
    """Freeze raw full-dimensional availability before any preprocess/projection."""
    raw = np.asarray(observations, dtype=np.float64)
    if raw.shape != (len(dates), len(feature_names)):
        _fail("raw full-feature/calendar dimensions differ")
    observation_mask = np.isfinite(raw).all(axis=1).tolist()
    observation_positions = np.flatnonzero(observation_mask).tolist()
    compact_components = {}
    for name in COMPONENT_WEIGHTS:
        if name not in components or set(components[name]) != {"positions", "values"}:
            _fail("all three independently sourced utility components required")
        part = components[name]
        positions = part["positions"]
        if not isinstance(positions, list) or any(type(p) is not int or not 0 <= p < len(dates) for p in positions):
            _fail("component positions invalid")
        mask = [p in positions for p in range(len(dates))]
        _positions(mask, positions, len(dates))
        _finite(part["values"], (len(positions),))
        compact_components[name] = {
            "component_available_mask": mask,
            "component_available_positions": list(positions),
            "values_f64": _finite(part["values"], (len(positions),)).tolist(),
        }
    if set(components) != set(COMPONENT_WEIGHTS):
        _fail("unknown utility component")
    utility_mask = [
        all(part["component_available_mask"][p] for part in compact_components.values()) for p in range(len(dates))
    ]
    utility_positions = [p for p, available in enumerate(utility_mask) if available]
    lookup = {name: dict(zip(part["positions"], part["values"])) for name, part in components.items()}
    combined = [sum(weight * lookup[name][p] for name, weight in COMPONENT_WEIGHTS.items()) for p in utility_positions]
    daily_sources = []
    for p, day in enumerate(dates):
        missing_features = [name for j, name in enumerate(feature_names) if not np.isfinite(raw[p, j])]
        missing_components = [
            name for name, part in compact_components.items() if not part["component_available_mask"][p]
        ]
        item = {
            "date": day,
            "position": p,
            "missing_feature_names": missing_features,
            "missing_component_names": missing_components,
            "observation_reasons": [OBSERVATION_UNAVAILABLE] if missing_features else [],
            "utility_reasons": [UTILITY_UNAVAILABLE] if missing_components else [],
            "source_identity_sha256": source_identity_sha256,
            "source_receipt_sha256": source_receipt_sha256,
        }
        daily_sources.append({**item, "receipt_sha256": canonical_sha256(item)})
    body = {
        "schema_version": CARRIER_VERSION,
        "calendar_dates": list(dates),
        "calendar_positions": list(range(len(dates))),
        "feature_names": list(feature_names),
        "observation_available_mask": observation_mask,
        "observation_available_positions": observation_positions,
        "observation_values_f64": raw[observation_mask].tolist(),
        "components": compact_components,
        "utility_available_mask": utility_mask,
        "utility_available_positions": utility_positions,
        "combined_values_f64": combined,
        "source_cutoff": "2025-04-30",
        "source_identity_sha256": source_identity_sha256,
        "daily_sources": daily_sources,
    }
    value = {**body, "manifest": _manifest(body)}
    validate_calendar_carrier(
        value,
        dates=dates,
        feature_names=feature_names,
        source_identity_sha256=source_identity_sha256,
        source_receipt_sha256=source_receipt_sha256,
    )
    return value


def validate_calendar_carrier(
    value: Mapping[str, Any],
    *,
    dates: Sequence[str],
    feature_names: Sequence[str],
    source_identity_sha256: str,
    source_receipt_sha256: str,
) -> None:
    fields = {
        "schema_version",
        "calendar_dates",
        "calendar_positions",
        "feature_names",
        "observation_available_mask",
        "observation_available_positions",
        "observation_values_f64",
        "components",
        "utility_available_mask",
        "utility_available_positions",
        "combined_values_f64",
        "source_cutoff",
        "source_identity_sha256",
        "daily_sources",
        "manifest",
    }
    if not isinstance(value, Mapping) or set(value) != fields or value["schema_version"] != CARRIER_VERSION:
        _fail("current D6 carrier schema/fields required")
    if (
        len(dates) != 182
        or list(dates) != sorted(set(dates))
        or dates[0] != "2024-07-01"
        or dates[-1] != "2025-03-31"
        or value["calendar_dates"] != list(dates)
        or value["calendar_positions"] != list(range(182))
        or value["feature_names"] != list(feature_names)
        or len(set(feature_names)) != len(feature_names)
        or value["source_cutoff"] != "2025-04-30"
        or value["source_identity_sha256"] != source_identity_sha256
        or not _sha(source_identity_sha256)
    ):
        _fail("frozen calendar/full-feature/source identity differs")
    for day in dates:
        date.fromisoformat(day)
    _positions(value["observation_available_mask"], value["observation_available_positions"], 182)
    _finite(value["observation_values_f64"], (len(value["observation_available_positions"]), len(feature_names)))
    components = value["components"]
    if not isinstance(components, dict) or set(components) != set(COMPONENT_WEIGHTS):
        _fail("utility component set differs")
    lookup = {}
    for name, part in components.items():
        if not isinstance(part, dict) or set(part) != {
            "component_available_mask",
            "component_available_positions",
            "values_f64",
        }:
            _fail("utility component fields differ")
        _positions(part["component_available_mask"], part["component_available_positions"], 182)
        _finite(part["values_f64"], (len(part["component_available_positions"]),))
        lookup[name] = dict(zip(part["component_available_positions"], part["values_f64"]))
    expected_utility_mask = [
        all(part["component_available_mask"][p] for part in components.values()) for p in range(182)
    ]
    if value["utility_available_mask"] != expected_utility_mask:
        _fail("combined utility is not the full component intersection")
    _positions(value["utility_available_mask"], value["utility_available_positions"], 182)
    combined = _finite(value["combined_values_f64"], (len(value["utility_available_positions"]),))
    expected = [
        sum(weight * lookup[name][p] for name, weight in COMPONENT_WEIGHTS.items())
        for p in value["utility_available_positions"]
    ]
    if canonical_json_bytes(combined.tolist()) != canonical_json_bytes(expected):
        _fail("combined utility does not match frozen weights/components")
    sources = value["daily_sources"]
    if not isinstance(sources, list) or len(sources) != 182:
        _fail("full daily source ledger required")
    for p, item in enumerate(sources):
        if not isinstance(item, dict) or set(item) != {
            "date",
            "position",
            "missing_feature_names",
            "missing_component_names",
            "observation_reasons",
            "utility_reasons",
            "source_identity_sha256",
            "source_receipt_sha256",
            "receipt_sha256",
        }:
            _fail("daily source ledger fields differ")
        missing = item["missing_feature_names"]
        if (
            not isinstance(missing, list)
            or missing != [name for name in feature_names if name in missing]
            or not set(missing) <= set(feature_names)
            or bool(missing) == value["observation_available_mask"][p]
            or item["missing_component_names"] != [name for name in COMPONENT_WEIGHTS if p not in lookup[name]]
            or item["observation_reasons"] != ([OBSERVATION_UNAVAILABLE] if missing else [])
            or item["utility_reasons"] != ([UTILITY_UNAVAILABLE] if not expected_utility_mask[p] else [])
            or item["date"] != dates[p]
            or type(item["position"]) is not int
            or item["position"] != p
            or item["source_identity_sha256"] != source_identity_sha256
            or item["source_receipt_sha256"] != source_receipt_sha256
            or not _sha(source_receipt_sha256)
            or item["receipt_sha256"] != canonical_sha256({k: v for k, v in item.items() if k != "receipt_sha256"})
        ):
            _fail("daily source reasons/identity/masks disagree")
    body = {k: v for k, v in value.items() if k != "manifest"}
    if value["manifest"] != _manifest(body):
        _fail("independent hashes or manifest aggregate differ")


def semantic_components(carrier: Mapping[str, Any]) -> dict[str, Any]:
    return {
        name: {"positions": part["component_available_positions"], "values": part["values_f64"]}
        for name, part in carrier["components"].items()
    }


def calendar_ledger(carrier: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            **source,
            "observation_available": carrier["observation_available_mask"][p],
            "utility_available": carrier["utility_available_mask"][p],
            "mode": "emission_update" if carrier["observation_available_mask"][p] else "transition_only",
            "evidence_included": carrier["observation_available_mask"][p] and carrier["utility_available_mask"][p],
        }
        for p, source in enumerate(carrier["daily_sources"])
    ]


def evaluate_calendar_evidence(
    model: Any,
    *,
    carrier: Mapping[str, Any],
    processed_values: np.ndarray,
    dates: Sequence[str],
    feature_names: Sequence[str],
    source_identity_sha256: str,
    source_receipt_sha256: str,
    selected_identity: Mapping[str, Any],
) -> dict[str, Any]:
    """One current-contract evaluator, used by both writer and zero-refit readback."""
    from backend.services.hmm_risk.formal_state_model import FAMILIES, SEEDS, model_payload, receipt, semantic_evidence

    if (
        not isinstance(selected_identity, Mapping)
        or set(selected_identity) != {"family", "level", "sector", "seed"}
        or selected_identity["family"] not in FAMILIES
        or selected_identity["level"] not in {"L1", "L2"}
        or type(selected_identity["seed"]) is not int
        or selected_identity["seed"] not in SEEDS
        or not isinstance(selected_identity["sector"], str)
        or not selected_identity["sector"].endswith(".SI")
    ):
        _fail("selected family/level/sector/seed identity required")

    validate_calendar_carrier(
        carrier,
        dates=dates,
        feature_names=feature_names,
        source_identity_sha256=source_identity_sha256,
        source_receipt_sha256=source_receipt_sha256,
    )
    ledger = calendar_ledger(carrier)
    try:
        result = semantic_evidence(
            model,
            dates=dates,
            positions=carrier["observation_available_positions"],
            values=processed_values,
            components=semantic_components(carrier),
        )
    except FormalStateError as exc:
        # A real assignment failure still retains the immutable calendar and
        # source ledger; unknown numerical results are not fabricated.
        result = receipt(
            {
                "contract_version": "hmm_risk_c008_b3_d6_01_b_na_a_v1",
                "base_contract_version": BASE_VERSION,
                "availability_contract_version": AVAILABILITY_VERSION,
                "assignment_status": "failed",
                "evidence_status": "insufficient_evidence",
                "semantic_assignment_valid": False,
                "semantic_evidence_valid": False,
                "reasons": [exc.reason_code],
                "primary_reason": exc.reason_code,
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "mapping": None,
                "evidence_positions": [p for p, row in enumerate(ledger) if row["evidence_included"]],
                **{
                    key: value
                    for key, value in (exc.evidence.items() if isinstance(exc.evidence, dict) else [])
                    if key
                    in {
                        "posterior",
                        "diagnostic_hard_assignment_positions",
                        "diagnostic_hard_assignment_values",
                        "diagnostic_tie_positions",
                    }
                },
            }
        )
    events = [
        {"date": item["date"], "position": item["position"], "reason_code": reason}
        for item in ledger
        for reason in item["observation_reasons"] + item["utility_reasons"]
    ]
    body = {k: v for k, v in result.items() if k != "receipt_sha256"}
    body.update(
        selected_identity=dict(selected_identity),
        selected_model_parameter_sha256=canonical_sha256(model_payload(model)),
        processed_observation_payload_sha256=canonical_sha256(np.asarray(processed_values, dtype=np.float64).tolist()),
        input_manifest=carrier["manifest"],
        observation_available_mask=carrier["observation_available_mask"],
        utility_available_mask=carrier["utility_available_mask"],
        ledger=ledger,
        availability_events=events,
        evidence_dates=[dates[p] for p in result["evidence_positions"]],
    )
    body["canonical_hashes"] = {
        name: canonical_sha256(body[name])
        for name in (
            "observation_available_mask",
            "utility_available_mask",
            "ledger",
            "posterior",
            "evidence_dates",
            "evidence_hard_assignments",
            "utility_on_evidence",
            "states",
            "gaps",
        )
        if name in body
    }
    return receipt(body)


def validate_calendar_evidence(value: Mapping[str, Any], model: Any, **source: Any) -> None:
    expected = evaluate_calendar_evidence(model, **source)
    if canonical_json_bytes(value) != canonical_json_bytes(expected):
        _fail("semantic write/readback differs from frozen carrier and model")
