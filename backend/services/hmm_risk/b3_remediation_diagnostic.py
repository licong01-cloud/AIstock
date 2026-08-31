from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from typing import Any

import numpy as np

from backend.services.hmm_risk.b3_blocker_diagnostic import FORMAL_AUTHORITY, validate_formal_report
from backend.services.hmm_risk.b3_training import B3TrainOnlySeries
from backend.services.hmm_risk.state_model_set import (
    StateModelSetError,
    _apply_preprocess,
    canonical_json_bytes,
    canonical_sha256,
)


SCHEMA_VERSION = "hmm_risk_c008_b3_remediation_diag02_v1"
DIAGNOSTIC_CONTRACT = "C-008-B3-REMEDIATION-DIAG-02"
BLOCKER_SCHEMA_VERSION = "hmm_risk_c008_b3_formal_blocker_diag01_v1"
BLOCKER_REPORT_SHA256 = "10287e845f07bf3d9c15a68e5d09ad14e54613348824ac2af568f0244a1cffe8"
BLOCKER_PRODUCER_COMMIT = "ac3687c2e56d000a1fae6d196a8334e46060b07b"
EXPECTED_PROFILE_COUNTS = {
    "autocycle_all_core:L1": 31,
    "autocycle_all_core:L2": 131,
    "legacy_covfix:L1": 31,
    "legacy_covfix:L2": 131,
}
EXPECTED_FEATURE_COUNTS = {"autocycle_all_core": 20, "legacy_covfix": 7}
QUANTILES = (0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0)
LIKELIHOOD_ROWS = ("accepted", "accepted_with_warning", "failed", "insufficient_evidence")
COVARIANCE_COLUMNS = (
    "accepted",
    "failed_bounds_only",
    "failed_mstep_only",
    "failed_bounds_and_mstep",
    "invalid",
    "insufficient_evidence",
)
Y_METRICS = (
    "mstep_max_abs_relative_residual",
    "covariance_min_lower_slack",
    "covariance_min_upper_slack",
    "total_anomaly_cell_count",
)
EXPECTED_SINGLETON_FAILURES = {
    ("autocycle_all_core", "L2", "801129.SI", 43): [1, 284, 188],
    ("legacy_covfix", "L2", "801113.SI", 42): [300, 298, 1],
    ("legacy_covfix", "L2", "801769.SI", 49): [308, 290, 1],
}


def _receipt(value: Mapping[str, Any], field: str, label: str) -> None:
    identity = str(value.get(field) or "")
    body = {key: item for key, item in value.items() if key != field}
    if len(identity) != 64 or canonical_sha256(body) != identity:
        raise StateModelSetError(f"{label} canonical receipt mismatch")


def validate_authorities(formal_report: Mapping[str, Any], blocker_report: Mapping[str, Any]) -> dict[str, str]:
    validate_formal_report(formal_report)
    formal_sha = canonical_sha256(dict(formal_report))
    blocker_sha = canonical_sha256(dict(blocker_report))
    if formal_sha != FORMAL_AUTHORITY["report_sha256"]:
        raise StateModelSetError("remediation diagnostic formal authority mismatch")
    if blocker_sha != BLOCKER_REPORT_SHA256:
        raise StateModelSetError("remediation diagnostic blocker authority mismatch")
    if (
        blocker_report.get("schema_version") != BLOCKER_SCHEMA_VERSION
        or blocker_report.get("diagnostic_contract") != "C-008-B3-FORMAL-BLOCKER-DIAG-01"
        or blocker_report.get("diagnostic_producer_commit") != BLOCKER_PRODUCER_COMMIT
        or blocker_report.get("status") != "diagnostic_complete"
    ):
        raise StateModelSetError("remediation diagnostic blocker authority metadata mismatch")
    _receipt(blocker_report, "receipt_sha256", "remediation blocker report")
    if blocker_report.get("formal_authority") != FORMAL_AUTHORITY:
        raise StateModelSetError("remediation diagnostic blocker formal authority mismatch")
    expected_flags = {
        "selection_performed": False,
        "selection_reexecuted": False,
        "acceptance_decision_reexecuted": False,
        "formal_thresholds_changed": False,
        "hard_semantic_authority_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
        "canonical_payload_bitwise_equal": True,
    }
    for field, expected in expected_flags.items():
        if blocker_report.get(field) != expected:
            raise StateModelSetError(f"remediation diagnostic blocker {field} boundary mismatch")
    if (
        blocker_report.get("observed_total_fit_count") != 348
        or blocker_report.get("observed_fits_per_process") != 174
        or blocker_report.get("d6_replay_count") != 3
    ):
        raise StateModelSetError("remediation diagnostic blocker count closure mismatch")
    return {"formal_report_sha256": formal_sha, "blocker_report_sha256": blocker_sha}


def _project_entry(value: Mapping[str, Any]) -> dict[str, Any]:
    _receipt(value, "diagnostic_entry_sha256", "train projection entry")
    for field in ("validation_accessed", "future_utility_accessed", "selection_performed", "model_write_performed"):
        if value.get(field) is not False:
            raise StateModelSetError(f"train projection {field} must be false")
    status = str(value.get("status") or "")
    if status not in {"fit_completed", "fit_failed"}:
        raise StateModelSetError("train projection entry status is invalid")
    identity_fields = (
        "diagnostic_entry_sha256",
        "role",
        "family",
        "level",
        "seed",
        "sector_code",
        "source_entry_receipt_sha256",
        "train_input_manifest_sha256",
        "formal_failed_stages",
        "status",
        "validation_accessed",
        "future_utility_accessed",
        "selection_performed",
        "model_write_performed",
    )
    body = {field: value.get(field) for field in identity_fields}
    if status == "fit_completed":
        training = value.get("training_receipt")
        signed = value.get("signed_distances")
        hard = value.get("hard_sequence_detail")
        fitted = value.get("fitted_model_payload")
        if not all(isinstance(item, Mapping) for item in (training, signed, hard, fitted)):
            raise StateModelSetError("train projection completed-entry evidence is incomplete")
        for field in ("validation_accessed", "future_utility_accessed"):
            if training.get(field) is not False:
                raise StateModelSetError(f"train projection training receipt {field} must be false")
        preprocess = fitted.get("preprocess")
        if not isinstance(preprocess, Mapping):
            raise StateModelSetError("train projection preprocess identity is missing")
        body.update(
            {
                "training_receipt": dict(training),
                "signed_distances": dict(signed),
                "hard_sequence_detail": dict(hard),
                "preprocess_identity": dict(preprocess),
                "preprocess_identity_sha256": canonical_sha256(dict(preprocess)),
            }
        )
    else:
        failure_fields = (
            "diagnostic_failure_stage",
            "diagnostic_failure_reason_code",
            "diagnostic_failure_type",
            "diagnostic_failure_evidence",
        )
        if value.get("diagnostic_failure_stage") != "initialization" or not isinstance(
            value.get("diagnostic_failure_evidence"), Mapping
        ):
            raise StateModelSetError("train projection initialization failure evidence is incomplete")
        body.update({field: value.get(field) for field in failure_fields})
    return {**body, "projection_entry_sha256": canonical_sha256(body)}


def build_train_only_projection(blocker_report: Mapping[str, Any]) -> dict[str, Any]:
    numeric_environment = blocker_report.get("numeric_environment")
    if not isinstance(numeric_environment, Mapping) or blocker_report.get(
        "numeric_environment_sha256"
    ) != canonical_sha256(dict(numeric_environment)):
        raise StateModelSetError("train projection numeric environment identity is invalid")
    raw_entries = blocker_report.get("targeted_evidence")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise StateModelSetError("train projection targeted evidence is missing")
    entries = [_project_entry(value) for value in raw_entries if isinstance(value, Mapping)]
    if len(entries) != len(raw_entries) or len({value["projection_entry_sha256"] for value in entries}) != len(entries):
        raise StateModelSetError("train projection targeted evidence identity is invalid")
    roles = {role: sum(value.get("role") == role for value in entries) for role in ("rejected", "control")}
    statuses = {
        status: sum(value.get("status") == status for value in entries) for status in ("fit_completed", "fit_failed")
    }
    body = {
        "schema_version": "hmm_risk_c008_b3_remediation_diag02_train_projection_v1",
        "source_schema_version": blocker_report.get("schema_version"),
        "source_diagnostic_contract": blocker_report.get("diagnostic_contract"),
        "source_diagnostic_producer_commit": blocker_report.get("diagnostic_producer_commit"),
        "formal_authority": dict(blocker_report.get("formal_authority") or {}),
        "numeric_environment": dict(numeric_environment),
        "numeric_environment_sha256": blocker_report.get("numeric_environment_sha256"),
        "targeted_evidence": entries,
        "targeted_evidence_count": len(entries),
        "role_counts": roles,
        "status_counts": statuses,
        "validation_accessed": False,
        "future_utility_accessed": False,
    }
    return {**body, "projection_sha256": canonical_sha256(body)}


def preprocess_identities(
    projection: Mapping[str, Any],
    *,
    approved_fallback: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for entry in projection.get("targeted_evidence") or ():
        if entry.get("status") != "fit_completed":
            continue
        key = f"{entry['family']}:{entry['level']}"
        identity = str(entry.get("preprocess_identity_sha256") or "")
        preprocess = entry.get("preprocess_identity")
        if not isinstance(preprocess, Mapping) or identity != canonical_sha256(dict(preprocess)):
            raise StateModelSetError("train projection preprocess receipt is invalid")
        grouped[key][identity] = dict(preprocess)
    for key, preprocess in (approved_fallback or {}).items():
        value = dict(preprocess)
        if key != "legacy_covfix:L1" or value != {
            "family": "identity",
            "winsor_low": None,
            "winsor_high": None,
            "center": None,
            "scale": None,
        }:
            raise StateModelSetError("train projection preprocess fallback is not the approved legacy L1 identity")
        grouped[key][canonical_sha256(value)] = value
    expected = set(EXPECTED_PROFILE_COUNTS)
    if set(grouped) != expected or any(len(values) != 1 for values in grouped.values()):
        raise StateModelSetError("train projection preprocess identity is not unique for every family/level")
    return {key: next(iter(values.values())) for key, values in sorted(grouped.items())}


def _little_endian_matrix(value: Any) -> np.ndarray:
    matrix = np.array(value, dtype="<f8", order="C", copy=True)
    if matrix.ndim != 2 or matrix.shape[0] == 0 or matrix.shape[1] == 0:
        raise StateModelSetError("remediation profile matrix shape is invalid")
    if not np.isfinite(matrix).all():
        raise StateModelSetError("remediation profile matrix contains non-finite values")
    return matrix


def _vector_stats(value: np.ndarray) -> dict[str, Any]:
    vector = np.array(value, dtype="<f8", order="C", copy=True).reshape(-1)
    if not np.isfinite(vector).all() or vector.size == 0:
        raise StateModelSetError("remediation variance vector is empty or non-finite")
    mean = math.fsum(float(item) for item in vector) / int(vector.size)
    variance = math.fsum((float(item) - mean) ** 2 for item in vector) / int(vector.size)
    normalized = vector.copy()
    normalized[normalized == 0.0] = 0.0
    unique_bits = {int(item) for item in normalized.view("<u8")}
    return {
        "min": float(vector.min()),
        "max": float(vector.max()),
        "mean": float(mean),
        "var_ddof0": float(variance),
        "zero_variance": variance == 0.0,
        "non_positive_variance": variance <= 0.0,
        "all_values_exact_zero": bool(np.all(vector == 0.0)),
        "unique_finite_value_count": len(unique_bits),
        "raw_float64_sha256": hashlib.sha256(vector.tobytes(order="C")).hexdigest(),
    }


def build_profile_variance_evidence(
    series: B3TrainOnlySeries,
    *,
    family: str,
    level: str,
    feature_names: Sequence[str],
    preprocess: Mapping[str, Any],
    feature_definition: Mapping[str, Any],
    source_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    features = tuple(str(value) for value in feature_names)
    series.validate(len(features))
    matrix = _little_endian_matrix(series.train_observations)
    if matrix.shape[1] != len(features):
        raise StateModelSetError("remediation profile feature shape is invalid")
    dates = tuple(series.train_dates)
    if tuple(sorted(dates)) != dates or len(set(dates)) != len(dates):
        raise StateModelSetError("remediation profile dates must be strictly increasing")
    if dates[-1] > date.fromisoformat("2024-06-30"):
        raise StateModelSetError("remediation profile temporal boundary is invalid")
    preprocessed = _little_endian_matrix(_apply_preprocess(matrix, preprocess))
    feature_definition_sha256 = canonical_sha256(dict(feature_definition))
    feature_evidence = []
    for index, feature_name in enumerate(features):
        raw = _vector_stats(matrix[:, index])
        processed = _vector_stats(preprocessed[:, index])
        formula_component = {
            "feature_name": feature_name,
            "feature_definition_sha256": feature_definition_sha256,
            "formula_diff": dict(feature_definition.get("formula_diff_by_feature", {})).get(feature_name),
            "cross_section_operator": dict(feature_definition.get("cross_section_operator_by_feature", {})).get(
                feature_name
            ),
            "moneyflow_denominator": dict(feature_definition.get("moneyflow_denominator_by_feature", {})).get(
                feature_name
            ),
        }
        feature_evidence.append(
            {
                "feature_index": index,
                "feature_name": feature_name,
                "unique_finite_value_count": raw["unique_finite_value_count"],
                "raw_float64_sha256": raw["raw_float64_sha256"],
                "raw": raw,
                "preprocessed": processed,
                "formula_component": formula_component,
                "formula_component_sha256": canonical_sha256(formula_component),
            }
        )
    body = {
        "schema_version": "hmm_risk_c008_b3_remediation_diag02_profile_v1",
        "family": family,
        "level": level,
        "sector_code": series.sector_code,
        "feature_names": list(features),
        "row_count": int(matrix.shape[0]),
        "ordered_train_dates_sha256": canonical_sha256([value.isoformat() for value in dates]),
        "train_start": dates[0].isoformat(),
        "train_end": dates[-1].isoformat(),
        "raw_observation_dtype": "<f8",
        "raw_observation_order": "C",
        "raw_observation_shape": list(matrix.shape),
        "raw_observation_sha256": hashlib.sha256(matrix.tobytes(order="C")).hexdigest(),
        "train_input_manifest": dict(series.train_input_manifest),
        "train_input_manifest_sha256": canonical_sha256(dict(series.train_input_manifest)),
        "preprocess_identity": dict(preprocess),
        "preprocess_identity_sha256": canonical_sha256(dict(preprocess)),
        "feature_definition_sha256": feature_definition_sha256,
        "source_provenance": dict(source_provenance),
        "source_provenance_sha256": canonical_sha256(dict(source_provenance)),
        "features": feature_evidence,
    }
    return {**body, "profile_receipt_sha256": canonical_sha256(body)}


def _linear_quantile(sorted_values: Sequence[float], q: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    h = (len(sorted_values) - 1) * q
    i = math.floor(h)
    j = math.ceil(h)
    return float(sorted_values[i] + (h - i) * (sorted_values[j] - sorted_values[i]))


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    if any(not math.isfinite(float(value)) for value in values):
        raise StateModelSetError("remediation variance distribution contains non-finite values")
    positive = sorted(float(value) for value in values if float(value) > 0.0)
    status = "complete" if positive else "insufficient_evidence"
    return {
        "status": status,
        "observed_count": len(values),
        "positive_count": len(positive),
        "zero_count": sum(float(value) == 0.0 for value in values),
        "negative_count": sum(float(value) < 0.0 for value in values),
        "min_positive": positive[0] if positive else None,
        "quantiles": {str(q): _linear_quantile(positive, q) for q in QUANTILES},
    }


def aggregate_variance_distributions(profiles: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, dict[str, list[float]]] = defaultdict(lambda: {"raw": [], "preprocessed": []})
    for profile in profiles:
        for feature in profile.get("features") or ():
            key = f"{profile['family']}:{profile['level']}:{feature['feature_name']}"
            grouped[key]["raw"].append(float(feature["raw"]["var_ddof0"]))
            grouped[key]["preprocessed"].append(float(feature["preprocessed"]["var_ddof0"]))
    return {
        key: {domain: _distribution(values) for domain, values in domains.items()}
        for key, domains in sorted(grouped.items())
    }


def build_initialization_source_evidence(projection: Mapping[str, Any]) -> dict[str, Any]:
    failures = [entry for entry in projection.get("targeted_evidence") or () if entry.get("status") == "fit_failed"]
    if len(failures) != 11:
        raise StateModelSetError("remediation initialization source count must be 11")
    persistent = []
    singleton = []
    for entry in failures:
        evidence = entry.get("diagnostic_failure_evidence")
        if not isinstance(evidence, Mapping):
            raise StateModelSetError("remediation initialization source evidence is invalid")
        row = {
            "family": entry["family"],
            "level": entry["level"],
            "sector_code": entry["sector_code"],
            "seed": entry["seed"],
            "diagnostic_entry_sha256": entry["diagnostic_entry_sha256"],
            "source_entry_receipt_sha256": entry["source_entry_receipt_sha256"],
            "projection_entry_sha256": entry["projection_entry_sha256"],
            "failure_evidence": dict(evidence),
        }
        if "reference_variance" in evidence:
            reference = [float(value) for value in evidence.get("reference_variance") or ()]
            if (
                entry.get("family") != "autocycle_all_core"
                or entry.get("level") != "L2"
                or entry.get("sector_code") != "801207.SI"
                or len(reference) != 20
                or reference[-1] != 0.0
                or any(not math.isfinite(value) or value <= 0.0 for value in reference[:-1])
            ):
                raise StateModelSetError("remediation persistent zero-variance source mismatch")
            persistent.append(row)
        elif "cluster_counts" in evidence:
            counts = [int(value) for value in evidence.get("cluster_counts") or ()]
            identity = (str(entry["family"]), str(entry["level"]), str(entry["sector_code"]), int(entry["seed"]))
            if len(counts) != 3 or counts != EXPECTED_SINGLETON_FAILURES.get(identity):
                raise StateModelSetError("remediation singleton initialization source mismatch")
            singleton.append(row)
        else:
            raise StateModelSetError("remediation initialization source class is unsupported")
    observed_singletons = {(row["family"], row["level"], row["sector_code"], row["seed"]) for row in singleton}
    if (
        len(persistent) != 8
        or {row["seed"] for row in persistent} != set(range(42, 50))
        or observed_singletons != set(EXPECTED_SINGLETON_FAILURES)
    ):
        raise StateModelSetError("remediation initialization source classification mismatch")
    body = {
        "persistent_zero_variance": persistent,
        "persistent_zero_variance_count": len(persistent),
        "singleton_cluster": singleton,
        "singleton_cluster_count": len(singleton),
        "entry_count": len(failures),
    }
    return {**body, "evidence_sha256": canonical_sha256(body)}


def _entry_identity(entry: Mapping[str, Any]) -> str:
    return f"{entry['family']}:{entry['level']}:{entry['seed']}:{entry['sector_code']}"


def _covariance_column(entry: Mapping[str, Any]) -> str:
    distances = entry.get("signed_distances")
    training = entry.get("training_receipt")
    if not isinstance(distances, Mapping) or not isinstance(training, Mapping):
        return "insufficient_evidence"
    try:
        lower = float(distances["covariance_min_lower_slack"])
        upper = float(distances["covariance_min_upper_slack"])
        mstep = float(distances["covariance_min_mstep_slack"])
    except (KeyError, TypeError, ValueError):
        return "invalid"
    if not all(math.isfinite(value) for value in (lower, upper, mstep)):
        raise StateModelSetError("remediation covariance evidence contains non-finite values")
    bounds_failed = lower < 0.0 or upper < 0.0
    mstep_failed = mstep < 0.0
    if bounds_failed and mstep_failed:
        return "failed_bounds_and_mstep"
    if bounds_failed:
        return "failed_bounds_only"
    if mstep_failed:
        return "failed_mstep_only"
    return "accepted"


def _likelihood_row(entry: Mapping[str, Any]) -> str:
    try:
        status = str(entry["training_receipt"]["likelihood"]["likelihood_status"])
    except (KeyError, TypeError):
        return "insufficient_evidence"
    if status not in LIKELIHOOD_ROWS:
        raise StateModelSetError("remediation likelihood status is unsupported")
    return status


def _xy(entry: Mapping[str, Any]) -> tuple[float, dict[str, float]]:
    deltas = list(entry.get("signed_distances", {}).get("likelihood_delta_distances") or ())
    if not deltas:
        raise StateModelSetError("remediation likelihood delta evidence is missing")
    x = float(deltas[-1]["relative"])
    evidence = entry.get("training_receipt", {}).get("covariance", {}).get("evidence", {})
    y = {
        "mstep_max_abs_relative_residual": float(evidence["mstep_max_abs_relative_residual"]),
        "covariance_min_lower_slack": float(entry["signed_distances"]["covariance_min_lower_slack"]),
        "covariance_min_upper_slack": float(entry["signed_distances"]["covariance_min_upper_slack"]),
        "total_anomaly_cell_count": float(int(evidence.get("below_count", 0)) + int(evidence.get("above_count", 0))),
    }
    if not math.isfinite(x) or any(not math.isfinite(value) for value in y.values()):
        raise StateModelSetError("remediation statistic evidence contains non-finite values")
    return x, y


def _average_ranks(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: (values[index], index))
    ranks = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        average = ((start + 1) + end) / 2.0
        for position in range(start, end):
            ranks[order[position]] = average
        start = end
    return ranks


def _pearson(x: Sequence[float], y: Sequence[float]) -> float | None:
    if len(x) < 3 or len(x) != len(y):
        return None
    mx = math.fsum(x) / len(x)
    my = math.fsum(y) / len(y)
    dx = [value - mx for value in x]
    dy = [value - my for value in y]
    sx = math.fsum(value * value for value in dx)
    sy = math.fsum(value * value for value in dy)
    if sx == 0.0 or sy == 0.0:
        return None
    return math.fsum(a * b for a, b in zip(dx, dy, strict=True)) / math.sqrt(sx * sy)


def _correlation_group(rows: Sequence[tuple[str, float, float]], composition: Mapping[str, int]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda item: item[0])
    x = [item[1] for item in ordered]
    y = [item[2] for item in ordered]
    pearson = _pearson(x, y)
    spearman = _pearson(_average_ranks(x), _average_ranks(y)) if pearson is not None else None
    status = "complete" if pearson is not None and spearman is not None else "insufficient_evidence"
    body = {
        "status": status,
        "pair_count": len(ordered),
        "role_composition": dict(sorted(composition.items())),
        "ordered_raw_pairs": [
            {"entry_identity": identity, "x": x_value, "y": y_value} for identity, x_value, y_value in ordered
        ],
        "pearson": pearson,
        "spearman": spearman,
    }
    return {**body, "raw_pairs_sha256": canonical_sha256(body["ordered_raw_pairs"])}


def _structure_analysis(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    failure_rows = []
    minimum_slacks: dict[str, float] = {}
    persistence: dict[str, dict[str, Any]] = {}
    boundary_failure_counts: dict[str, int] = defaultdict(int)
    failure_combination_counts: dict[str, int] = defaultdict(int)
    for entry in entries:
        identity = _entry_identity(entry)
        states = entry.get("signed_distances", {}).get("state_distances")
        if not isinstance(states, Mapping):
            raise StateModelSetError("remediation train structure evidence is missing")
        entry_failures = set()
        for state, evidence in sorted(states.items()):
            if not isinstance(evidence, Mapping):
                raise StateModelSetError("remediation train state evidence is invalid")
            slack_map = {
                "count": evidence.get("count_slack"),
                "occupancy": evidence.get("occupancy_slack"),
                "month": evidence.get("month_slack"),
                "run": evidence.get("run_slack"),
                "incoming_transition": evidence.get("incoming_transition_slack"),
                "outgoing_transition": evidence.get("outgoing_transition_slack"),
                "run_concentration": evidence.get("run_concentration_slack"),
            }
            for name, raw in slack_map.items():
                if raw is None:
                    continue
                value = float(raw)
                if not math.isfinite(value):
                    raise StateModelSetError("remediation train structure evidence contains non-finite values")
                key = f"{entry['family']}:{entry['level']}:{state}:{name}"
                minimum_slacks[key] = min(minimum_slacks.get(key, value), value)
                if value < 0.0:
                    entry_failures.add(name)
                    boundary_failure_counts[f"{entry['family']}:{entry['level']}:{state}:{name}"] += 1
        if entry_failures:
            combination = "+".join(sorted(entry_failures))
            failure_combination_counts[combination] += 1
            failure_rows.append(
                {
                    "entry_identity": identity,
                    "family": entry["family"],
                    "level": entry["level"],
                    "seed": entry["seed"],
                    "sector_code": entry["sector_code"],
                    "failed_boundaries": sorted(entry_failures),
                }
            )
        sector_key = f"{entry['family']}:{entry['level']}:{entry['sector_code']}"
        aggregate = persistence.setdefault(sector_key, {"seeds": [], "failed_seeds": [], "failed_boundaries": set()})
        aggregate["seeds"].append(int(entry["seed"]))
        if entry_failures:
            aggregate["failed_seeds"].append(int(entry["seed"]))
            aggregate["failed_boundaries"].update(entry_failures)
    normalized_persistence = {
        key: {
            "seeds": sorted(set(value["seeds"])),
            "failed_seeds": sorted(set(value["failed_seeds"])),
            "failed_boundaries": sorted(value["failed_boundaries"]),
        }
        for key, value in sorted(persistence.items())
    }
    ordered = sorted(failure_rows, key=lambda value: value["entry_identity"])
    return {
        "failure_intersections": {
            "count": len(ordered),
            "entries": ordered,
            "entries_sha256": canonical_sha256(ordered),
        },
        "minimum_slacks": dict(sorted(minimum_slacks.items())),
        "boundary_failure_counts": dict(sorted(boundary_failure_counts.items())),
        "failure_combination_counts": dict(sorted(failure_combination_counts.items())),
        "sector_seed_persistence": normalized_persistence,
        "sector_seed_persistence_sha256": canonical_sha256(normalized_persistence),
    }


def analyze_completed_entries(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    completed = [entry for entry in entries if entry.get("status") == "fit_completed"]
    matrix = {
        row: {
            column: {"count": 0, "entry_identities": [], "entry_identities_sha256": canonical_sha256([])}
            for column in COVARIANCE_COLUMNS
        }
        for row in LIKELIHOOD_ROWS
    }
    raw_by_group: dict[str, dict[str, list[tuple[str, float, float]]]] = defaultdict(
        lambda: {metric: [] for metric in Y_METRICS}
    )
    composition: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for entry in completed:
        identity = _entry_identity(entry)
        row = _likelihood_row(entry)
        column = _covariance_column(entry)
        matrix[row][column]["count"] += 1
        matrix[row][column]["entry_identities"].append(identity)
        x, y = _xy(entry)
        family_level = f"family_level={entry['family']}:{entry['level']}"
        role = f"role={entry['role']}"
        groups = ("overall", family_level, role, f"{family_level}|{role}")
        for group in groups:
            composition[group][str(entry["role"])] += 1
            for metric, y_value in y.items():
                raw_by_group[group][metric].append((identity, x, y_value))
    for row in LIKELIHOOD_ROWS:
        for column in COVARIANCE_COLUMNS:
            identities = sorted(matrix[row][column]["entry_identities"])
            matrix[row][column]["entry_identities"] = identities
            matrix[row][column]["entry_identities_sha256"] = canonical_sha256(identities)
    correlations = {
        group: {metric: _correlation_group(rows, composition[group]) for metric, rows in sorted(metrics.items())}
        for group, metrics in sorted(raw_by_group.items())
    }
    return {
        "entry_count": len(completed),
        "cross_matrix": matrix,
        "cross_matrix_sha256": canonical_sha256(matrix),
        "correlations": correlations,
        "correlations_sha256": canonical_sha256(correlations),
        "train_structure": _structure_analysis(completed),
    }


def build_report(
    formal_report: Mapping[str, Any],
    blocker_report: Mapping[str, Any],
    profiles: Sequence[Mapping[str, Any]],
    *,
    producer_commit: str,
    numeric_environment: Mapping[str, Any],
) -> dict[str, Any]:
    authority = validate_authorities(formal_report, blocker_report)
    projection = build_train_only_projection(blocker_report)
    counts = defaultdict(int)
    identities = set()
    for profile in profiles:
        _receipt(profile, "profile_receipt_sha256", "remediation profile")
        key = f"{profile['family']}:{profile['level']}"
        counts[key] += 1
        identities.add(f"{key}:{profile['sector_code']}")
        if len(profile.get("feature_names") or ()) != EXPECTED_FEATURE_COUNTS.get(str(profile.get("family"))):
            raise StateModelSetError("remediation profile feature count is invalid")
    if dict(counts) != EXPECTED_PROFILE_COUNTS or len(identities) != 324:
        raise StateModelSetError("remediation profile manifest is incomplete")
    profile_manifest = {
        "profile_count": len(profiles),
        "counts_by_family_level": dict(sorted(counts.items())),
        "profile_receipt_sha256": [value["profile_receipt_sha256"] for value in profiles],
    }
    profile_manifest["manifest_sha256"] = canonical_sha256(profile_manifest)
    initialization = build_initialization_source_evidence(projection)
    analysis = analyze_completed_entries(projection["targeted_evidence"])
    if (
        projection.get("role_counts") != {"rejected": 150, "control": 24}
        or projection.get("status_counts") != {"fit_completed": 163, "fit_failed": 11}
        or analysis["entry_count"] != 163
    ):
        raise StateModelSetError("remediation projected evidence count is invalid")
    variance = aggregate_variance_distributions(profiles)
    if len(variance) != 54:
        raise StateModelSetError("remediation variance distribution manifest is incomplete")
    current_numeric_environment = dict(numeric_environment)
    current_numeric_environment_sha256 = canonical_sha256(current_numeric_environment)
    insufficient_groups = [
        f"{group}:{metric}"
        for group, metrics in analysis["correlations"].items()
        for metric, value in metrics.items()
        if value["status"] == "insufficient_evidence"
    ]
    body = {
        "schema_version": SCHEMA_VERSION,
        "status": "diagnostic_complete",
        "diagnostic_contract": DIAGNOSTIC_CONTRACT,
        "producer_commit": producer_commit,
        "source_commit": producer_commit,
        "formal_source_commit": FORMAL_AUTHORITY["producer_commit"],
        "blocker_source_commit": BLOCKER_PRODUCER_COMMIT,
        "formal_authority": authority,
        "numeric_environment": current_numeric_environment,
        "numeric_environment_sha256": current_numeric_environment_sha256,
        "blocker_numeric_environment_sha256": projection["numeric_environment_sha256"],
        "train_projection": projection,
        "train_projection_sha256": projection["projection_sha256"],
        "profile_manifest": profile_manifest,
        "profiles": list(profiles),
        "variance_distributions": variance,
        "initialization_source_evidence": initialization,
        "completed_entry_analysis": analysis,
        "section_statuses": {
            "authority": "complete",
            "train_projection": "complete",
            "profile_variance": "complete",
            "initialization_source": "complete",
            "likelihood_covariance_association": "complete",
            "train_structure": "complete",
        },
        "reason_codes": (["hmm_risk_remediation_diag_statistic_insufficient"] if insufficient_groups else []),
        "statistic_insufficient_groups": insufficient_groups,
        "hmm_refit_performed": False,
        "selection_performed": False,
        "validation_accessed": False,
        "formal_acceptance_reexecuted": False,
        "threshold_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def reason_code_for_error(error: BaseException) -> str:
    message = str(error).lower()
    if "collision" in message:
        return "hmm_risk_remediation_diag_artifact_collision"
    if "readback" in message:
        return "hmm_risk_remediation_diag_readback_mismatch"
    if "authority" in message or "canonical receipt" in message:
        return "hmm_risk_remediation_diag_authority_mismatch"
    if "train projection" in message or "projection" in message:
        return "hmm_risk_remediation_diag_train_projection_invalid"
    if "temporal" in message or "strictly increasing" in message:
        return "hmm_risk_remediation_diag_profile_temporal_boundary_invalid"
    if "profile manifest" in message or "profile feature count" in message:
        return "hmm_risk_remediation_diag_profile_manifest_incomplete"
    if "variance" in message:
        return "hmm_risk_remediation_diag_variance_evidence_invalid"
    if "initialization" in message or "singleton" in message:
        return "hmm_risk_remediation_diag_initialization_source_mismatch"
    if "non-finite" in message:
        return "hmm_risk_remediation_diag_numeric_non_finite"
    if "statistic" in message or "correlation" in message or "structure" in message:
        return "hmm_risk_remediation_diag_statistic_evidence_invalid"
    return "hmm_risk_remediation_diag_artifact_write_failed"


def failure_report(*, producer_commit: str, reason_code: str, error: BaseException) -> dict[str, Any]:
    body = {
        "schema_version": SCHEMA_VERSION,
        "status": "failed",
        "diagnostic_contract": DIAGNOSTIC_CONTRACT,
        "producer_commit": producer_commit,
        "reason_codes": [reason_code],
        "error_type": type(error).__name__[:256],
        "error": str(error)[-4000:],
        "hmm_refit_performed": False,
        "selection_performed": False,
        "validation_accessed": False,
        "formal_acceptance_reexecuted": False,
        "threshold_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def write_diagnostic_artifact(path: Path, report: Mapping[str, Any]) -> str:
    target = path.resolve()
    payload = canonical_json_bytes(dict(report)) + b"\n"
    identity = canonical_sha256(dict(report))
    if target.exists():
        if target.read_bytes() != payload:
            raise StateModelSetError(f"remediation diagnostic artifact collision: {target}")
        return identity
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if temporary.read_bytes() != payload:
            raise StateModelSetError("remediation diagnostic temporary artifact readback mismatch")
        try:
            os.link(temporary, target)
        except FileExistsError:
            if target.read_bytes() != payload:
                raise StateModelSetError(f"remediation diagnostic artifact collision: {target}") from None
        if target.read_bytes() != payload:
            raise StateModelSetError("remediation diagnostic artifact readback mismatch")
        value = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or canonical_sha256(value) != identity:
            raise StateModelSetError("remediation diagnostic canonical readback mismatch")
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    temporary.unlink(missing_ok=True)
    return identity
