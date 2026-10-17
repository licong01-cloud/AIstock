"""Read-only fixed-5TD model projection; no training, data source, API or activation."""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first import generic_price_5td_models_v1 as daily
from backend.services.advisory_model_first import generic_minute_price_5td_models_v1 as minute
from backend.services.advisory_model_first import generic_volume_path_price_5td_model_v1 as volume
from backend.services.advisory_model_first import generic_joint_distribution_price_5td_model_v1 as joint
from backend.services.advisory_model_first.generic_daily_price_input_v1 import KEY, ROSTER, _day, _number
from backend.services.advisory_model_first.generic_price_5td_inference_v1 import generic_price_set_5td_v1
from backend.services.advisory_model_first.generic_minute_price_5td_inference_v1 import minute_price_set_5td_v1
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY, POLICY_SHA256
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

FAMILIES = {
    "DAILY_5TD": (daily, daily.GenericPrice5TDFitV1, generic_price_set_5td_v1),
    "MINUTE_5TD": (minute, minute.GenericMinutePrice5TDFitV1, minute_price_set_5td_v1),
    "VOLUME_PATH_5TD": (volume, volume.GenericVolumePathPrice5TDFitV1, volume.volume_path_price_set_5td_v1),
    "JOINT_DISTRIBUTION_5TD": (joint, joint.GenericJointDistributionPrice5TDFitV1, joint.joint_price_set_5td_v1),
}
SOURCE_KEYS = {"package_id", "run_id", "list_version_id", "universe_identity", "source_evidence", "feature_visible_through"}
PRICE_KEYS = {"reference_cny", "legal_low_cny", "legal_high_cny", "tick_cny", "visible_through", "price_basis"}
PRICES = ("reference_cny", "legal_low_cny", "legal_high_cny", "tick_cny")
SHA = re.compile(r"[0-9a-f]{64}")


def _fail(message, code="ADVISORY_GENERIC_PRICE_CONSUMER_INVALID"):
    raise AdvisoryModelFirstError(message, reason_code=code)


def _object(body):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                _fail("generic model JSON has duplicate keys")
            result[key] = value
        return result
    try:
        value = json.loads(body, object_pairs_hook=unique)
        if not isinstance(value, dict):
            _fail("generic model JSON is not an object")
        json.dumps(value, allow_nan=False)  # Shared identity hash intentionally has broader legacy JSON semantics.
    except (ValueError, TypeError, UnicodeError) as error:
        _fail(f"generic model JSON is invalid: {type(error).__name__}")
    return value


def _read(path, maximum):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path.absolute() or path.drive.upper() == "C:":
        _fail("generic model path is not an explicit non-C nonredirected path")
    if not path.is_file():
        _fail("generic trained model is not present", "ADVISORY_GENERIC_MODEL_NOT_TRAINED")
    if not 0 < path.stat().st_size <= maximum:
        _fail("generic model file exceeds its upfront read budget")
    with path.open("rb") as stream:
        body = stream.read(maximum + 1)
    if not 0 < len(body) <= maximum:
        _fail("generic model changed beyond its bounded read")
    return body


def _fitted(family, body):
    if family not in FAMILIES:
        _fail("generic model family is not supported")
    module, constructor, _ = FAMILIES[family]
    value = _object(body)
    try:
        if family != "JOINT_DISTRIBUTION_5TD":
            value["intervals_bps"] = tuple(tuple(interval) for interval in value["intervals_bps"])
        else:
            value["forest"] = tuple(value["forest"])
        fitted = constructor(**value)
        (module.validate_fit if family == "DAILY_5TD" else module.validate_fit_v1)(fitted)
    except (ValueError, TypeError, KeyError) as error:
        _fail(f"generic trained model identity differs: {type(error).__name__}")
    return fitted


@dataclass(frozen=True)
class LoadedGenericPriceSetModelV1:
    """Immutable snapshots, not externally mutable fitted dictionaries."""
    model_family: str
    trained_manifest_sha256: str
    manifest_bytes: bytes
    model_bytes: bytes


def load_generic_price_set_model_v1(*, model_family, trained_manifest_ref):
    if model_family not in FAMILIES:
        _fail("generic model family is not supported")
    try:
        reference = EvidenceReferenceV1.model_validate(trained_manifest_ref)
    except (TypeError, ValueError):
        _fail("generic trained reference is malformed")
    path = Path(reference.artifact_uri)
    if path.name != "manifest.json" or path.parent.name != "trained":
        _fail("generic consumer needs the original trained manifest, not another stage")
    body = _read(path, 1024**2)
    if hashlib.sha256(body).hexdigest() != reference.sha256 or len(body) != reference.size_bytes:
        _fail("generic trained manifest differs from its explicit reference")
    manifest = _object(body)
    if (set(manifest) != {"schema_version", "stage", "plan_sha256", "parent_sha256", "files", "stage_sha256"}
            or manifest["schema_version"] != "economic_entry_stage_v1" or manifest["stage"] != "trained"
            or not all(isinstance(manifest.get(name), str) and SHA.fullmatch(manifest[name])
                       for name in ("plan_sha256", "parent_sha256", "stage_sha256"))
            or sha({name: value for name, value in manifest.items() if name != "stage_sha256"}) != manifest["stage_sha256"]
            or not isinstance(manifest["files"], dict) or set(manifest["files"]) != {"model.json"}):
        _fail("generic trained stage content identity differs")
    descriptor = manifest["files"]["model.json"]
    if (not isinstance(descriptor, dict) or set(descriptor) != {"sha256", "size_bytes"}
            or type(descriptor["size_bytes"]) is not int or not 0 < descriptor["size_bytes"] <= 64 * 1024**2
            or not isinstance(descriptor["sha256"], str) or not SHA.fullmatch(descriptor["sha256"])):
        _fail("generic model descriptor contradicts the bounded trained file")
    model = _read(path.parent / "model.json", 64 * 1024**2)
    if len(model) != descriptor["size_bytes"] or hashlib.sha256(model).hexdigest() != descriptor["sha256"]:
        _fail("generic model differs from its trained descriptor")
    _fitted(model_family, model)
    return LoadedGenericPriceSetModelV1(model_family, reference.sha256, body, model)


def _context(value):
    if not isinstance(value, dict) or set(value) != SOURCE_KEYS:
        _fail("generic source metadata schema differs")
    for key in ("package_id", "run_id", "list_version_id", "source_evidence"):
        if value[key] is not None and (not isinstance(value[key], str) or not value[key].strip()):
            _fail("generic source identity is neither text nor explicitly unknown")
    if value["universe_identity"] is not None and not isinstance(value["universe_identity"], (dict, str)):
        _fail("generic pool metadata is not a declared identity or unknown")
    result = dict(value)
    clock = _day(value["feature_visible_through"], nullable=True)
    result["feature_visible_through"] = clock.isoformat() if clock else None
    try:
        encoded = json.dumps(result, allow_nan=False, ensure_ascii=False, sort_keys=True)
    except (ValueError, TypeError):
        _fail("generic metadata must be finite JSON")
    if len(encoded.encode("utf-8")) > 65536:
        _fail("generic metadata exceeds its budget")
    return json.loads(encoded), clock


def project_generic_price_sets_v1(*, loaded, candidate_rows, price_contexts, source_context,
                                  budget_seconds=30., monotonic=time.monotonic):
    """No future quotes or Y; preserve all original candidates and UNKNOWN holes."""
    if not isinstance(loaded, LoadedGenericPriceSetModelV1):
        _fail("generic price projection needs an explicitly loaded model")
    manifest = _object(loaded.manifest_bytes)
    descriptor = manifest.get("files", {}).get("model.json", {})
    if (hashlib.sha256(loaded.manifest_bytes).hexdigest() != loaded.trained_manifest_sha256
            or hashlib.sha256(loaded.model_bytes).hexdigest() != descriptor.get("sha256")
            or len(loaded.model_bytes) != descriptor.get("size_bytes")):
        _fail("generic immutable model snapshot was replaced inconsistently")
    if isinstance(budget_seconds, bool) or not isinstance(budget_seconds, (int, float)) or not 0 < budget_seconds <= 30:
        _fail("generic projection deadline must be positive and at most thirty seconds")
    deadline = monotonic() + budget_seconds
    fitted = _fitted(loaded.model_family, loaded.model_bytes)
    module, _, price_set = FAMILIES[loaded.model_family]
    names = tuple(daily.FEATURES) + (() if loaded.model_family == "DAILY_5TD" else tuple(module.MINUTE_FEATURES))
    context, feature_clock = _context(source_context)
    if (not isinstance(candidate_rows, pd.DataFrame) or not candidate_rows.columns.is_unique
            or set(candidate_rows.columns) != set((*ROSTER, *names)) or len(candidate_rows) > 50
            or not isinstance(price_contexts, dict)):
        _fail("generic candidate projection schema or bounded population differs")
    rows, coordinates, days = [], [], set()
    for row in candidate_rows.to_dict("records"):
        d, t = (_day(row[key]) for key in KEY[:2])
        rank, group = row["selection_effective_rank"], row["candidate_group_size"]
        if (d >= t or feature_clock is not None and feature_clock > d
                or not isinstance(row["instrument"], str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", row["instrument"])
                or any(isinstance(v, (bool, np.bool_)) or not isinstance(v, (int, np.integer)) for v in (rank, group))
                or not 1 <= rank <= group):
            _fail("generic original D/T, feature clock, symbol or rank/group contradicts input")
        days.add((d, t))
        normalized = {**row, KEY[0]: d.isoformat(), KEY[1]: t.isoformat(),
                      "selection_effective_rank": int(rank), "candidate_group_size": int(group)}
        normalized.update({name: _number(row[name]) for name in names})
        rows.append(normalized)
        coordinate = price_contexts.get(row["instrument"])
        if coordinate is None:
            coordinates.append(None)
            continue
        if not isinstance(coordinate, dict) or set(coordinate) != PRICE_KEYS:
            _fail("generic price context schema differs")
        clock = _day(coordinate["visible_through"], nullable=True)
        numbers = {name: _number(coordinate[name], positive=True) for name in PRICES}
        if (clock is not None and clock > d or coordinate["price_basis"] not in (None, "D_ANCHORED_CNY")
                or numbers["legal_low_cny"] is not None and numbers["legal_high_cny"] is not None
                and numbers["legal_low_cny"] > numbers["legal_high_cny"]):
            _fail("generic price context has future or contradictory D coordinates")
        coordinates.append({**numbers, "visible_through": clock.isoformat() if clock else None,
                            "price_basis": coordinate["price_basis"]})
    symbols = [row["instrument"] for row in rows]
    ranks = [row["selection_effective_rank"] for row in rows]
    if (len(days) > 1 or len(set(symbols)) != len(symbols) or len(set(ranks)) != len(ranks) or ranks != sorted(ranks)
            or len({row["candidate_group_size"] for row in rows}) > 1
            or rows and rows[0]["candidate_group_size"] < len(rows) or set(price_contexts) - set(symbols)):
        _fail("generic original candidate uniqueness, order, group or price partition differs")
    advice = []
    for row, coordinate in zip(rows, coordinates, strict=True):
        if monotonic() > deadline:
            _fail("generic projection exceeded its deadline without publishing a partial batch")
        d = row[KEY[0]]
        if feature_clock is None:
            result = dict(status="UNKNOWN_FEATURE_CLOCK", intervals_cny=(), legal_node_count=0, unknown_node_count=0)
        elif (coordinate is None or coordinate["visible_through"] != d or coordinate["price_basis"] is None
              or any(coordinate[name] is None for name in PRICES)):
            result = dict(status="UNKNOWN_PRICE_CONTEXT", intervals_cny=(), legal_node_count=0, unknown_node_count=0)
        else:
            arguments = dict(fitted=fitted, d_features={name: row[name] for name in names},
                             **{name: coordinate[name] for name in PRICES})
            if loaded.model_family != "JOINT_DISTRIBUTION_5TD":
                arguments["arm"] = "candidate"
            result = price_set(**arguments)
        advice.append({**{name: row[name] for name in ROSTER}, **result,
            "input_unknown_fields": [name for name in names if row[name] is None],
            "model_sha256": fitted.model_sha256, "policy_sha256": POLICY_SHA256,
            "model_family": loaded.model_family, "label_contract": POLICY["label_contract"], "holding_sessions": 5,
            "evidence_use": "NAVIGATION_ONLY", "deployable": False, "economic_confirmation": False,
            "price_basis": "D_ANCHORED_CNY", "hypothetical_price_not_order": True})
        if monotonic() > deadline:
            _fail("generic projection exceeded its deadline without publishing a partial batch")
    return dict(schema_version="generic_price_set_consumer_v1", status="COMPUTED" if rows else "NO_CANDIDATES",
        source_context=context, model_family=loaded.model_family, model_sha256=fitted.model_sha256,
        trained_manifest_sha256=loaded.trained_manifest_sha256, model_stage_sha256=_object(loaded.manifest_bytes)["stage_sha256"],
        input_sha256=sha(dict(rows=rows, coordinates=coordinates, context=context)), advice=advice,
        candidate_count=len(rows), decision_clock="D", holding_sessions=5, policy_sha256=POLICY_SHA256,
        acceptance_criterion=dict(expected_net_bps=">0", downside_q90_bps="<=800", buy_bps=.95, sell_bps=5.95),
        fit_count=0, outcomes_read=False, database_read=False, database_write=False, model_activation=False,
        qualification_rechecked=False, evidence_use="NAVIGATION_ONLY", deployable=False, economic_confirmation=False,
        target_calendar_verified=False, hypothetical_price_not_order=True)
