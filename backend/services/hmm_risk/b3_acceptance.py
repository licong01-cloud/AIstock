from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date
from numbers import Real
from typing import Any

import numpy as np

from backend.services.hmm_risk.b3_mixed_dimension import (
    INACTIVE_DIMENSION_REASON_CODE,
    MIXED_DIMENSION_CONTRACT_VERSION,
    MIXED_REPEAT_SCHEMA_VERSION,
    MIXED_TRAINING_ENTRY_SCHEMA_VERSION,
    uses_mixed_dimension_level,
    validate_projection_receipt,
)
from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_sha256


D3_CONTRACT_VERSION = "hmm_risk_c008_b3_d3_03_a_v1"
D4_LIKELIHOOD_VERSION = "hmm_risk_c008_b3_d4_01_map_a_v1"
D4_COVARIANCE_VERSION = "hmm_risk_c008_b3_d4_02_a_v1"
D4_OCCUPANCY_VERSION = "hmm_risk_c008_b3_d4_03_persistent_a_v1"
D5_SELECTION_VERSION = "hmm_risk_c008_b3_d5_01_b_v1"
D6_SEMANTIC_VERSION = "hmm_risk_c008_b3_d6_01_b_v1"
L2_RETRAIN_VERSION = "hmm_risk_c008_b3_l2_retrain_a_v1"
RESTART_SCHEDULE = tuple(range(42, 50))


def _valid_sha256(value: Any) -> bool:
    identity = str(value or "")
    return len(identity) == 64 and all(character in "0123456789abcdef" for character in identity.lower())


def _finite_array(value: Any, *, shape: tuple[int, ...] | None = None) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    if shape is not None and result.shape != shape:
        raise ValueError(f"array shape mismatch expected={shape} actual={result.shape}")
    if not np.isfinite(result).all():
        raise ValueError("array contains non-finite values")
    return result


def _strict_real(value: Any) -> float:
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, Real):
        raise TypeError("numeric evidence must be a real number")
    return float(value)


def _strict_real_vector(value: Any) -> np.ndarray:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("numeric history must be a sequence")
    return np.asarray([_strict_real(item) for item in value], dtype=np.float64)


def _ordered_unique_dates(values: Sequence[date]) -> tuple[date, ...]:
    result = tuple(values)
    if not result or any(not isinstance(value, date) for value in result):
        raise ValueError("ordered dates are missing or invalid")
    if tuple(sorted(result)) != result or len(set(result)) != len(result):
        raise ValueError("ordered dates must be strictly increasing and unique")
    return result


def _status_receipt(
    *,
    contract_version: str,
    failures: Sequence[str] = (),
    blockers: Sequence[str] = (),
    warnings: Sequence[str] = (),
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    failure_codes = list(dict.fromkeys(str(value) for value in failures))
    blocking_codes = list(dict.fromkeys(str(value) for value in blockers))
    warning_codes = list(dict.fromkeys(str(value) for value in warnings))
    if failure_codes:
        status = "failed"
        valid = False
        primary = failure_codes[0]
    elif blocking_codes:
        status = "insufficient_evidence"
        valid = False
        primary = blocking_codes[0]
    elif warning_codes:
        status = "accepted_with_warning"
        valid = True
        primary = warning_codes[0]
    else:
        status = "accepted"
        valid = True
        primary = None
    body = {
        "contract_version": contract_version,
        "status": status,
        "valid": valid,
        "failure_reason_codes": failure_codes,
        "blocking_reason_codes": blocking_codes,
        "warning_reason_codes": warning_codes,
        "primary_reason_code": primary,
        "evidence": dict(evidence),
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _status_value(
    *,
    failures: Sequence[str] = (),
    blockers: Sequence[str] = (),
    warnings: Sequence[str] = (),
) -> tuple[str, bool]:
    if failures:
        return "failed", False
    if blockers:
        return "insufficient_evidence", False
    if warnings:
        return "accepted_with_warning", True
    return "accepted", True


def _named_status_receipt(
    prefix: str,
    *,
    contract_version: str,
    failures: Sequence[str] = (),
    blockers: Sequence[str] = (),
    warnings: Sequence[str] = (),
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    receipt = _status_receipt(
        contract_version=contract_version,
        failures=failures,
        blockers=blockers,
        warnings=warnings,
        evidence=evidence,
    )
    receipt.pop("receipt_sha256")
    receipt[f"{prefix}_status"] = receipt.pop("status")
    receipt[f"{prefix}_valid"] = receipt.pop("valid")
    return {**receipt, "receipt_sha256": canonical_sha256(receipt)}


def map_covariance_prior_objective(
    raw_log_likelihood: Any,
    raw_covars: Any,
    covars_prior: Any,
    covars_weight: Any,
) -> dict[str, Any]:
    """Return the exact D4-01-MAP-A objective components for one parameter state."""

    likelihood = float(raw_log_likelihood)
    covars = np.asarray(raw_covars, dtype=np.float64)
    prior = np.asarray(covars_prior, dtype=np.float64)
    weight = float(covars_weight)
    if covars.ndim != 2 or prior.shape != covars.shape:
        raise ValueError("MAP covariance/prior shape is invalid")
    if not np.isfinite(likelihood) or not np.isfinite(covars).all() or not np.isfinite(prior).all():
        raise ValueError("MAP objective input contains non-finite value")
    if not np.isfinite(weight) or weight <= 1.0 or np.any(covars <= 0.0) or np.any(prior <= 0.0):
        raise ValueError("MAP covariance/prior/weight domain is invalid")
    log_component = float(np.sum((weight - 1.0) * np.log(covars), dtype=np.float64))
    inverse_component = float(np.sum(prior / covars, dtype=np.float64))
    prior_adjustment = -0.5 * (log_component + inverse_component)
    objective = likelihood + prior_adjustment
    if not all(np.isfinite(value) for value in (log_component, inverse_component, prior_adjustment, objective)):
        raise ValueError("MAP objective is non-finite")
    return {
        "raw_log_likelihood": likelihood,
        "prior_log_covariance_component": log_component,
        "prior_inverse_covariance_component": inverse_component,
        "prior_adjustment": prior_adjustment,
        "map_objective": objective,
    }


def map_numeric_envelope(previous: float) -> float:
    return max(1e-8, math.sqrt(np.finfo(np.float64).eps) * max(1.0, abs(previous)))


def evaluate_likelihood_acceptance(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    """Apply D4-01-MAP-A and recompute all convergence comparisons fail-closed."""

    missing = "hmm_risk_model_likelihood_evidence_missing"
    if evidence is None:
        body = {
            "contract_version": D4_LIKELIHOOD_VERSION,
            "monitor_status": "insufficient_evidence",
            "convergence_valid": False,
            "likelihood_status": "insufficient_evidence",
            "likelihood_valid": False,
            "failure_reason_codes": [],
            "blocking_reason_codes": [missing],
            "warning_reason_codes": [],
            "primary_reason_code": missing,
            "evidence": {},
        }
        return {**body, "receipt_sha256": canonical_sha256(body)}

    required = {
        "authority",
        "maximum_iterations",
        "raw_likelihood_history",
        "map_objective_history",
        "map_prior_adjustment_history",
        "objective_component_history",
        "covariance_valid_history",
        "covariance_receipt_sha256_history",
        "joint_stop_iteration",
    }
    if not required.issubset(evidence):
        body = {
            "contract_version": D4_LIKELIHOOD_VERSION,
            "monitor_status": "insufficient_evidence",
            "convergence_valid": False,
            "likelihood_status": "insufficient_evidence",
            "likelihood_valid": False,
            "failure_reason_codes": [],
            "blocking_reason_codes": [missing],
            "warning_reason_codes": [],
            "primary_reason_code": missing,
            "evidence": {"evidence_fields_present": sorted(str(key) for key in evidence)},
        }
        return {**body, "receipt_sha256": canonical_sha256(body)}

    failures: list[str] = []
    blockers: list[str] = []
    warnings: list[str] = []
    try:
        if evidence["authority"] != "covariance_prior_map_objective":
            raise ValueError("MAP authority is invalid")
        if isinstance(evidence["maximum_iterations"], (bool, np.bool_)) or not isinstance(
            evidence["maximum_iterations"], (int, np.integer)
        ):
            raise TypeError("maximum_iterations must be an integer")
        maximum_iterations = int(evidence["maximum_iterations"])
        raw_history = _strict_real_vector(evidence["raw_likelihood_history"])
        map_history = _strict_real_vector(evidence["map_objective_history"])
        prior_history = _strict_real_vector(evidence["map_prior_adjustment_history"])
        component_history = tuple(evidence["objective_component_history"])
        covariance_valid_history = tuple(evidence["covariance_valid_history"])
        covariance_hashes = tuple(str(value) for value in evidence["covariance_receipt_sha256_history"])
        joint_stop = evidence["joint_stop_iteration"]
        if joint_stop is not None and (
            isinstance(joint_stop, (bool, np.bool_)) or not isinstance(joint_stop, (int, np.integer))
        ):
            raise TypeError("joint stop iteration is invalid")
        joint_stop = None if joint_stop is None else int(joint_stop)
    except (TypeError, ValueError):
        failures.append("hmm_risk_model_monitor_history_invalid")
        raw_history = map_history = prior_history = np.asarray([], dtype=np.float64)
        component_history = ()
        covariance_valid_history = ()
        covariance_hashes = ()
        maximum_iterations = 0
        joint_stop = None

    lengths = {
        int(raw_history.size),
        int(map_history.size),
        int(prior_history.size),
        len(covariance_valid_history),
        len(covariance_hashes),
        len(component_history),
    }
    history_shape_valid = (
        raw_history.ndim == map_history.ndim == prior_history.ndim == 1
        and len(lengths) == 1
        and 2 <= raw_history.size <= maximum_iterations
        and maximum_iterations == 300
        and all(isinstance(value, bool) for value in covariance_valid_history)
        and all(_valid_sha256(value) for value in covariance_hashes)
        and all(isinstance(value, Mapping) for value in component_history)
    )
    histories_finite = bool(
        np.isfinite(raw_history).all() and np.isfinite(map_history).all() and np.isfinite(prior_history).all()
    )
    if not history_shape_valid:
        failures.append("hmm_risk_model_monitor_history_invalid")
    if not histories_finite:
        failures.extend(("hmm_risk_model_likelihood_non_finite", "hmm_risk_model_map_objective_non_finite"))
    if (
        evidence.get("raw_likelihood_is_diagnostic_only") is not True
        or evidence.get("postfit_projection_performed") is not False
    ):
        failures.append("hmm_risk_model_contract_unsupported")

    map_deltas: list[dict[str, Any]] = []
    raw_deltas: list[dict[str, Any]] = []
    expected_joint_stop: int | None = None
    if history_shape_valid and histories_finite:
        if not np.array_equal(map_history, raw_history + prior_history):
            failures.append("hmm_risk_model_map_objective_non_finite")
        for index, component in enumerate(component_history):
            try:
                if isinstance(component["iteration"], (bool, np.bool_)) or not isinstance(
                    component["iteration"], (int, np.integer)
                ):
                    raise TypeError("component iteration must be an integer")
                component_iteration = int(component["iteration"])
                component_raw = _strict_real(component["raw_log_likelihood"])
                component_prior = _strict_real(component["prior_adjustment"])
                component_map = _strict_real(component["map_objective"])
                component_log = _strict_real(component["prior_log_covariance_component"])
                component_inverse = _strict_real(component["prior_inverse_covariance_component"])
            except (KeyError, TypeError, ValueError):
                failures.append("hmm_risk_model_map_objective_non_finite")
                break
            if (
                component_iteration != index + 1
                or not all(
                    np.isfinite(value)
                    for value in (component_raw, component_prior, component_map, component_log, component_inverse)
                )
                or component_raw != float(raw_history[index])
                or component_prior != float(prior_history[index])
                or component_map != float(map_history[index])
                or component_map != component_raw + component_prior
                or component_prior != -0.5 * (component_log + component_inverse)
            ):
                failures.append("hmm_risk_model_map_objective_non_finite")
                break
        for index in range(1, int(map_history.size)):
            previous_map = float(map_history[index - 1])
            current_map = float(map_history[index])
            map_delta = current_map - previous_map
            envelope = map_numeric_envelope(previous_map)
            covariance_valid = covariance_valid_history[index]
            within_envelope = abs(map_delta) <= envelope
            map_deltas.append(
                {
                    "index": index,
                    "iteration": index + 1,
                    "previous": previous_map,
                    "current": current_map,
                    "absolute": map_delta,
                    "numeric_envelope": envelope,
                    "covariance_valid": covariance_valid,
                    "joint_stop_eligible": within_envelope and covariance_valid,
                }
            )
            raw_previous = float(raw_history[index - 1])
            raw_current = float(raw_history[index])
            raw_delta = raw_current - raw_previous
            raw_deltas.append(
                {
                    "index": index,
                    "iteration": index + 1,
                    "previous": raw_previous,
                    "current": raw_current,
                    "absolute": raw_delta,
                    "relative": raw_delta / max(1.0, abs(raw_previous)),
                }
            )
            if map_delta < -envelope:
                failures.append("hmm_risk_model_map_objective_decrease")
            elif map_delta < 0.0:
                warnings.append("hmm_risk_model_map_numeric_envelope_warning")
            if raw_delta < 0.0:
                warnings.append("hmm_risk_model_raw_likelihood_decrease_diagnostic")
            if expected_joint_stop is None and within_envelope and covariance_valid:
                expected_joint_stop = index + 1
        if joint_stop != expected_joint_stop:
            failures.append("hmm_risk_model_map_joint_convergence_unavailable")
        if joint_stop is None:
            failures.append("hmm_risk_model_map_joint_convergence_unavailable")
        elif joint_stop != int(raw_history.size):
            failures.append("hmm_risk_model_map_joint_convergence_unavailable")

    failures = list(dict.fromkeys(failures))
    blockers = list(dict.fromkeys(blockers))
    warnings = list(dict.fromkeys(warnings))
    convergence_failures = [code for code in failures if code != "hmm_risk_model_likelihood_non_finite"]
    monitor_status, convergence_valid = _status_value(failures=convergence_failures, blockers=blockers)
    likelihood_failures = [code for code in failures if code == "hmm_risk_model_likelihood_non_finite"]
    likelihood_status, likelihood_valid = _status_value(
        failures=likelihood_failures,
        blockers=blockers,
        warnings=warnings,
    )
    normalized_evidence = {
        "authority": evidence.get("authority"),
        "maximum_iterations": maximum_iterations,
        "raw_likelihood_history": raw_history.tolist() if histories_finite else None,
        "map_objective_history": map_history.tolist() if histories_finite else None,
        "map_prior_adjustment_history": prior_history.tolist() if histories_finite else None,
        "objective_component_history": [dict(value) for value in component_history] if histories_finite else None,
        "covariance_valid_history": list(covariance_valid_history),
        "covariance_receipt_sha256_history": list(covariance_hashes),
        "joint_stop_iteration": joint_stop,
        "raw_likelihood_is_diagnostic_only": evidence.get("raw_likelihood_is_diagnostic_only") is True,
        "postfit_projection_performed": evidence.get("postfit_projection_performed") is True,
        "iterations": int(raw_history.size),
        "raw_likelihood_history_sha256": canonical_sha256(raw_history.tolist()) if histories_finite else None,
        "map_objective_history_sha256": canonical_sha256(map_history.tolist()) if histories_finite else None,
        "map_deltas": map_deltas,
        "raw_likelihood_deltas": raw_deltas,
        "numeric_envelope_formula": "max(1e-8,sqrt(eps_float64)*max(1,abs(previous_map_objective)))",
    }
    primary = failures[0] if failures else blockers[0] if blockers else warnings[0] if warnings else None
    body = {
        "contract_version": D4_LIKELIHOOD_VERSION,
        "monitor_status": monitor_status,
        "convergence_valid": convergence_valid,
        "likelihood_status": likelihood_status,
        "likelihood_valid": likelihood_valid,
        "failure_reason_codes": failures,
        "blocking_reason_codes": blockers,
        "warning_reason_codes": warnings,
        "primary_reason_code": primary,
        "evidence": normalized_evidence,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def evaluate_covariance_acceptance(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    """Apply D4-02-A to raw fitted covariance; no clipping or projection is performed."""

    if evidence is None:
        return _named_status_receipt(
            "covariance",
            contract_version=D4_COVARIANCE_VERSION,
            blockers=("hmm_risk_model_covariance_evidence_missing",),
            evidence={},
        )
    failures: list[str] = []
    required_fields = {
        "raw_covars",
        "sector_local_reference_variance_R_sj",
        "state_posterior_mass",
        "posterior_second_moment_about_fitted_mean",
        "train_rows",
        "nu",
    }
    if not required_fields.issubset(evidence):
        return _named_status_receipt(
            "covariance",
            contract_version=D4_COVARIANCE_VERSION,
            blockers=("hmm_risk_model_covariance_evidence_missing",),
            evidence={"available_fields": sorted(str(key) for key in evidence)},
        )
    try:
        raw = np.asarray(evidence["raw_covars"], dtype=np.float64)
        reference = np.asarray(evidence["sector_local_reference_variance_R_sj"], dtype=np.float64)
        masses = np.asarray(evidence["state_posterior_mass"], dtype=np.float64)
        second_moment = np.asarray(evidence["posterior_second_moment_about_fitted_mean"], dtype=np.float64)
        train_rows = int(evidence["train_rows"])
        nu = float(evidence["nu"])
    except (TypeError, ValueError):
        return _named_status_receipt(
            "covariance",
            contract_version=D4_COVARIANCE_VERSION,
            failures=("hmm_risk_model_covariance_invalid",),
            evidence={"covariance_parseable": False},
        )
    expected_shape = (3, int(reference.size))
    all_finite = math.isfinite(nu) and all(
        np.isfinite(value).all() for value in (raw, reference, masses, second_moment)
    )
    if not all_finite:
        failures.append("hmm_risk_model_covariance_invalid")
    if raw.shape != expected_shape or second_moment.shape != expected_shape or masses.shape != (3,):
        failures.append("hmm_risk_model_covariance_invalid")
    if np.any(raw <= 0.0) or np.any(reference <= 0.0) or np.any(masses <= 0.0) or train_rows <= 0 or nu != 1.0:
        failures.append("hmm_risk_model_covariance_invalid")

    if not failures:
        denominator = nu + masses[:, None]
        expected_covariance = (nu * reference[None, :] + masses[:, None] * second_moment) / denominator
        lower = nu * reference[None, :] / denominator
        upper = (nu + train_rows) * reference[None, :] / denominator
        below = raw < (1.0 - 0.005) * lower
        above = raw > (1.0 + 0.005) * upper
        anomaly = below | above
        relative_residual = np.abs(raw - expected_covariance) / np.maximum(
            np.abs(expected_covariance), np.finfo(np.float64).tiny
        )
        if anomaly.any():
            failures.extend(
                (
                    "hmm_risk_model_covariance_bounds_failed",
                    "hmm_risk_model_covariance_anomaly_budget_exceeded",
                )
            )
        if float(relative_residual.max()) > 0.02:
            failures.append("hmm_risk_model_covariance_acceptance_failed")
        computed = {
            "mstep_expected_covariance": expected_covariance.tolist(),
            "dynamic_lower_reference": lower.tolist(),
            "dynamic_upper_reference": upper.tolist(),
            "mstep_relative_residual": relative_residual.tolist(),
            "mstep_max_abs_relative_residual": float(relative_residual.max()),
            "below_count": int(below.sum()),
            "above_count": int(above.sum()),
            "per_state_anomaly_count": anomaly.sum(axis=1).astype(int).tolist(),
            "per_feature_anomaly_count": anomaly.sum(axis=0).astype(int).tolist(),
            "anomaly_mask_sha256": canonical_sha256(anomaly.astype(np.uint8).tolist()),
        }
    else:
        computed = {}
    receipt_evidence = {
        "raw_covars": raw.tolist() if all_finite else None,
        "sector_local_reference_variance_R_sj": reference.tolist() if all_finite else None,
        "state_posterior_mass": masses.tolist() if all_finite else None,
        "posterior_second_moment_about_fitted_mean": second_moment.tolist() if all_finite else None,
        "train_rows": train_rows,
        "nu": nu if math.isfinite(nu) else None,
        "bound_tolerance": 0.005,
        "mstep_tolerance": 0.02,
        "postfit_projection_performed": bool(evidence.get("postfit_projection_performed", False)),
        **computed,
    }
    if receipt_evidence["postfit_projection_performed"]:
        failures.append("hmm_risk_model_covariance_acceptance_failed")
    return _named_status_receipt(
        "covariance",
        contract_version=D4_COVARIANCE_VERSION,
        failures=failures,
        evidence=receipt_evidence,
    )


def _hard_sequence_metrics(posteriors: np.ndarray, dates: tuple[date, ...]) -> tuple[np.ndarray, dict[str, Any]]:
    if posteriors.shape != (len(dates), 3) or not posteriors.size:
        raise ValueError("posterior shape does not match ordered dates")
    if np.any(posteriors < 0.0):
        raise ValueError("posterior contains negative probability")
    row_error = float(np.max(np.abs(posteriors.sum(axis=1) - 1.0)))
    ordered = np.sort(posteriors, axis=1)
    margin = ordered[:, -1] - ordered[:, -2]
    hard = np.argmax(posteriors, axis=1).astype(np.int64)
    transitions = np.zeros((3, 3), dtype=np.int64)
    if hard.size > 1:
        np.add.at(transitions, (hard[:-1], hard[1:]), 1)
    runs: dict[int, list[int]] = {state: [] for state in range(3)}
    current = int(hard[0])
    length = 1
    for raw in hard[1:]:
        state = int(raw)
        if state == current:
            length += 1
        else:
            runs[current].append(length)
            current = state
            length = 1
    runs[current].append(length)
    states: dict[str, Any] = {}
    for state in range(3):
        mask = hard == state
        count = int(mask.sum())
        lengths = runs[state]
        months = sorted({dates[index].strftime("%Y-%m") for index in np.flatnonzero(mask)})
        states[str(state)] = {
            "hard_count": count,
            "normalized_occupancy": count / len(dates),
            "calendar_month_count": len(months),
            "calendar_months": months,
            "contiguous_run_count": len(lengths),
            "incoming_transition_count": int(transitions[:, state].sum() - transitions[state, state]),
            "outgoing_transition_count": int(transitions[state, :].sum() - transitions[state, state]),
            "maximum_single_run_share": max(lengths) / count if count and lengths else None,
        }
    return hard, {
        "row_sum_max_abs_error": row_error,
        "top1_top2_min_margin": float(margin.min()),
        "hard_assignment_sha256": canonical_sha256(hard.tolist()),
        "transition_counts": transitions.tolist(),
        "states": states,
    }


def _train_occupancy_receipt_from_metrics(
    metrics: Mapping[str, Any],
    *,
    evidence_identity: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply the single D4-03 authority to normalized hard-sequence metrics."""

    normalized_metrics = {key: value for key, value in metrics.items() if key != "states"}
    normalized_metrics["states"] = {str(key): dict(value) for key, value in metrics["states"].items()}
    train_rows = int(evidence_identity["train_rows"])
    failures: list[str] = []
    if normalized_metrics["row_sum_max_abs_error"] > 1e-12:
        failures.append("hmm_risk_model_posterior_normalization_failed")
    if normalized_metrics["top1_top2_min_margin"] <= 1e-12:
        failures.append("hmm_risk_model_posterior_tie")
    posterior_common_valid = (
        normalized_metrics["row_sum_max_abs_error"] <= 1e-12 and normalized_metrics["top1_top2_min_margin"] > 1e-12
    )
    count_threshold = max(5, math.ceil(0.01 * train_rows))
    persistent_count_threshold = max(30, math.ceil(0.10 * train_rows))
    for state in normalized_metrics["states"].values():
        common_valid = posterior_common_valid
        if state["hard_count"] < count_threshold:
            failures.append("hmm_risk_model_train_state_count_insufficient")
            common_valid = False
        if state["normalized_occupancy"] < 0.01:
            failures.append("hmm_risk_model_train_occupancy_insufficient")
            common_valid = False
        if state["calendar_month_count"] < 3:
            failures.append("hmm_risk_model_train_month_coverage_insufficient")
            common_valid = False
        if state["incoming_transition_count"] < 2 or state["outgoing_transition_count"] < 2:
            failures.append("hmm_risk_model_train_transition_coverage_insufficient")
            common_valid = False
        share = state["maximum_single_run_share"]
        recurrent_valid = bool(
            common_valid and share is not None and share <= 0.8 and state["contiguous_run_count"] >= 3
        )
        persistent_valid = bool(
            common_valid
            and share is not None
            and share > 0.8
            and state["hard_count"] >= persistent_count_threshold
            and state["normalized_occupancy"] >= 0.10
            and state["calendar_month_count"] >= 6
            and state["contiguous_run_count"] >= 2
            and state["incoming_transition_count"] >= 2
            and state["outgoing_transition_count"] >= 2
        )
        if common_valid and not (recurrent_valid or persistent_valid):
            failures.append("hmm_risk_model_train_regime_path_unsatisfied")
        state.update(
            {
                "common_gate_valid": common_valid,
                "evidence_path": "recurrent" if recurrent_valid else "persistent" if persistent_valid else "none",
                "recurrent_path": {
                    "eligible_by_run_share": share is not None and share <= 0.8,
                    "run_count_valid": state["contiguous_run_count"] >= 3,
                    "valid": recurrent_valid,
                },
                "persistent_path": {
                    "eligible_by_run_share": share is not None and share > 0.8,
                    "count_threshold": persistent_count_threshold,
                    "count_valid": state["hard_count"] >= persistent_count_threshold,
                    "occupancy_threshold": 0.10,
                    "occupancy_valid": state["normalized_occupancy"] >= 0.10,
                    "month_threshold": 6,
                    "month_valid": state["calendar_month_count"] >= 6,
                    "run_threshold": 2,
                    "run_valid": state["contiguous_run_count"] >= 2,
                    "transition_threshold": 2,
                    "incoming_transition_valid": state["incoming_transition_count"] >= 2,
                    "outgoing_transition_valid": state["outgoing_transition_count"] >= 2,
                    "valid": persistent_valid,
                },
            }
        )
    evidence = {
        **dict(evidence_identity),
        "count_threshold": count_threshold,
        "occupancy_threshold": 0.01,
        "month_threshold": 3,
        "run_threshold": 3,
        "transition_threshold": 2,
        "maximum_run_share_threshold": 0.8,
        "persistent_count_threshold": persistent_count_threshold,
        "persistent_occupancy_threshold": 0.10,
        "persistent_month_threshold": 6,
        "persistent_run_threshold": 2,
        "path_partition": {"recurrent_max_run_share": "<=0.8", "persistent_min_run_share": ">0.8"},
        **normalized_metrics,
    }
    return _named_status_receipt(
        "train_occupancy",
        contract_version=D4_OCCUPANCY_VERSION,
        failures=failures,
        evidence=evidence,
    )


def evaluate_train_occupancy(
    posteriors: Any,
    dates: Sequence[date] | None,
    *,
    frozen_input_manifest: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Apply D4-03-PERSISTENT-A to causal train hard assignments only."""

    if dates is None or not isinstance(frozen_input_manifest, Mapping):
        return _named_status_receipt(
            "train_occupancy",
            contract_version=D4_OCCUPANCY_VERSION,
            blockers=("hmm_risk_model_train_occupancy_evidence_missing",),
            evidence={},
        )
    try:
        ordered_dates = _ordered_unique_dates(dates)
    except (TypeError, ValueError):
        return _named_status_receipt(
            "train_occupancy",
            contract_version=D4_OCCUPANCY_VERSION,
            failures=("hmm_risk_model_train_date_sequence_invalid",),
            evidence={},
        )
    ordered_date_strings = [value.isoformat() for value in ordered_dates]
    required_hashes = (
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "calendar_manifest_hash",
        "feature_domain_policy_sha256",
    )
    if (
        frozen_input_manifest.get("schema_version") != "hmm_risk_d4_train_frozen_input_manifest_v1"
        or frozen_input_manifest.get("direct_sector_level") not in {"L1", "L2"}
        or not str(frozen_input_manifest.get("sector_code") or "").strip()
        or not _valid_sha256(frozen_input_manifest.get("train_observation_sha256"))
        or frozen_input_manifest.get("train_dates") != ordered_date_strings
        or frozen_input_manifest.get("train_dates_sha256") != canonical_sha256(ordered_date_strings)
        or any(not _valid_sha256(frozen_input_manifest.get(field)) for field in required_hashes)
    ):
        return _named_status_receipt(
            "train_occupancy",
            contract_version=D4_OCCUPANCY_VERSION,
            blockers=("hmm_risk_model_train_occupancy_evidence_missing",),
            evidence={"frozen_input_manifest_sha256": canonical_sha256(dict(frozen_input_manifest))},
        )
    try:
        probabilities = _finite_array(posteriors, shape=(len(ordered_dates), 3))
        hard, metrics = _hard_sequence_metrics(probabilities, ordered_dates)
    except (TypeError, ValueError):
        return _named_status_receipt(
            "train_occupancy",
            contract_version=D4_OCCUPANCY_VERSION,
            failures=("hmm_risk_model_posterior_invalid",),
            evidence={},
        )
    evidence_identity = {
        "direct_sector_level": frozen_input_manifest.get("direct_sector_level"),
        "sector_code": frozen_input_manifest.get("sector_code"),
        "train_observation_sha256": frozen_input_manifest.get("train_observation_sha256"),
        **{field: frozen_input_manifest.get(field) for field in required_hashes},
        "train_rows": len(ordered_dates),
        "ordered_date_sha256": canonical_sha256(ordered_date_strings),
        "posterior_sha256": canonical_sha256(probabilities.tolist()),
        "frozen_input_manifest_sha256": canonical_sha256(dict(frozen_input_manifest)),
        "validation_accessed": False,
        "future_utility_accessed": False,
    }
    return _train_occupancy_receipt_from_metrics(metrics, evidence_identity=evidence_identity)


def _strict_nonnegative_integer(value: Any, *, field: str) -> int:
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, np.integer)):
        raise TypeError(f"{field} must be an integer")
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{field} must be nonnegative")
    return normalized


def _train_occupancy_receipt_semantics_valid(receipt: Mapping[str, Any]) -> bool:
    """Recompute D4-03 status/path comparisons from durable normalized evidence."""

    try:
        hash_valid = _canonical_receipt_hash_valid(receipt)
    except (TypeError, ValueError, OverflowError):
        return False
    if (
        receipt.get("contract_version") != D4_OCCUPANCY_VERSION
        or not isinstance(receipt.get("receipt_sha256"), str)
        or not hash_valid
    ):
        return False
    evidence = receipt.get("evidence")
    if not isinstance(evidence, Mapping):
        return False
    try:
        train_rows = _strict_nonnegative_integer(evidence["train_rows"], field="train_rows")
        if train_rows <= 0:
            return False
        if (
            evidence.get("validation_accessed") is not False
            or evidence.get("future_utility_accessed") is not False
            or not all(
                isinstance(evidence.get(field), str) and _valid_sha256(evidence.get(field))
                for field in (
                    "ordered_date_sha256",
                    "posterior_sha256",
                    "hard_assignment_sha256",
                    "frozen_input_manifest_sha256",
                )
            )
        ):
            return False
        for field in (
            "train_observation_sha256",
            "dataset_manifest_hash",
            "mapping_manifest_hash",
            "calendar_manifest_hash",
            "feature_domain_policy_sha256",
        ):
            if not isinstance(evidence.get(field), str) or not _valid_sha256(evidence.get(field)):
                return False

        row_error = _strict_real(evidence["row_sum_max_abs_error"])
        margin = _strict_real(evidence["top1_top2_min_margin"])
        if not math.isfinite(row_error) or row_error < 0.0 or not math.isfinite(margin):
            return False
        transition_rows = evidence.get("transition_counts")
        if (
            not isinstance(transition_rows, Sequence)
            or isinstance(transition_rows, (str, bytes))
            or len(transition_rows) != 3
        ):
            return False
        transitions = np.asarray(
            [
                [_strict_nonnegative_integer(value, field="transition_count") for value in row]
                for row in transition_rows
                if isinstance(row, Sequence) and not isinstance(row, (str, bytes)) and len(row) == 3
            ],
            dtype=np.int64,
        )
        if transitions.shape != (3, 3) or int(transitions.sum()) != train_rows - 1:
            return False

        states = evidence.get("states")
        if not isinstance(states, Mapping) or set(states) != {"0", "1", "2"}:
            return False
        total_count = 0
        base_states: dict[str, dict[str, Any]] = {}
        for state_index in range(3):
            state = states[str(state_index)]
            if not isinstance(state, Mapping):
                return False
            count = _strict_nonnegative_integer(state["hard_count"], field="hard_count")
            total_count += count
            occupancy = _strict_real(state["normalized_occupancy"])
            month_count = _strict_nonnegative_integer(state["calendar_month_count"], field="calendar_month_count")
            months = state.get("calendar_months")
            run_count = _strict_nonnegative_integer(state["contiguous_run_count"], field="contiguous_run_count")
            incoming = _strict_nonnegative_integer(
                state["incoming_transition_count"], field="incoming_transition_count"
            )
            outgoing = _strict_nonnegative_integer(
                state["outgoing_transition_count"], field="outgoing_transition_count"
            )
            share_value = state.get("maximum_single_run_share")
            share = None if share_value is None else _strict_real(share_value)
            if (
                not math.isfinite(occupancy)
                or occupancy != count / train_rows
                or not isinstance(months, Sequence)
                or isinstance(months, (str, bytes))
                or any(not isinstance(value, str) for value in months)
                or list(months) != sorted(set(months))
                or month_count != len(months)
                or incoming != int(transitions[:, state_index].sum() - transitions[state_index, state_index])
                or outgoing != int(transitions[state_index, :].sum() - transitions[state_index, state_index])
                or (count == 0 and share is not None)
                or (count > 0 and (share is None or not math.isfinite(share) or not 0.0 < share <= 1.0))
            ):
                return False
            base_states[str(state_index)] = {
                "hard_count": count,
                "normalized_occupancy": occupancy,
                "calendar_month_count": month_count,
                "calendar_months": list(months),
                "contiguous_run_count": run_count,
                "incoming_transition_count": incoming,
                "outgoing_transition_count": outgoing,
                "maximum_single_run_share": share,
            }
        if total_count != train_rows:
            return False
        base_metrics = {
            "row_sum_max_abs_error": row_error,
            "top1_top2_min_margin": margin,
            "hard_assignment_sha256": evidence["hard_assignment_sha256"],
            "transition_counts": transitions.tolist(),
            "states": base_states,
        }
        evidence_identity = {
            field: evidence[field]
            for field in (
                "direct_sector_level",
                "sector_code",
                "train_observation_sha256",
                "dataset_manifest_hash",
                "mapping_manifest_hash",
                "calendar_manifest_hash",
                "feature_domain_policy_sha256",
                "train_rows",
                "ordered_date_sha256",
                "posterior_sha256",
                "frozen_input_manifest_sha256",
                "validation_accessed",
                "future_utility_accessed",
            )
        }
    except (KeyError, TypeError, ValueError, OverflowError):
        return False

    expected = _train_occupancy_receipt_from_metrics(base_metrics, evidence_identity=evidence_identity)
    return dict(receipt) == expected


def d4_training_receipt_readback_failures(entry: Mapping[str, Any]) -> list[str]:
    """Return typed failures after replaying durable D4 receipt authorities."""

    failures: list[str] = []
    likelihood = entry.get("likelihood")
    covariance = entry.get("covariance")
    occupancy = entry.get("train_occupancy")
    if not all(isinstance(receipt, Mapping) for receipt in (likelihood, covariance, occupancy)):
        return ["hmm_risk_model_selection_contract_unsatisfied"]
    likelihood_evidence = likelihood.get("evidence")
    covariance_evidence = covariance.get("evidence")
    if not isinstance(likelihood_evidence, Mapping) or not isinstance(covariance_evidence, Mapping):
        return ["hmm_risk_model_selection_contract_unsatisfied"]
    try:
        recomputed_likelihood = evaluate_likelihood_acceptance(likelihood_evidence)
        recomputed_covariance = evaluate_covariance_acceptance(covariance_evidence)
    except (TypeError, ValueError, OverflowError, FloatingPointError):
        return ["hmm_risk_model_selection_contract_unsatisfied"]
    covariance_hashes = recomputed_likelihood.get("evidence", {}).get("covariance_receipt_sha256_history", ())
    if dict(likelihood) != recomputed_likelihood:
        failures.extend(recomputed_likelihood.get("failure_reason_codes") or ())
        failures.extend(recomputed_likelihood.get("blocking_reason_codes") or ())
        if not failures:
            failures.append("hmm_risk_model_selection_contract_unsatisfied")
    if dict(covariance) != recomputed_covariance:
        covariance_failures = list(recomputed_covariance.get("failure_reason_codes") or ())
        covariance_failures.extend(recomputed_covariance.get("blocking_reason_codes") or ())
        failures.extend(covariance_failures or ["hmm_risk_model_selection_contract_unsatisfied"])
    if not _train_occupancy_receipt_semantics_valid(occupancy):
        failures.append("hmm_risk_model_selection_contract_unsatisfied")
    if (
        not isinstance(covariance_hashes, Sequence)
        or isinstance(covariance_hashes, (str, bytes))
        or not covariance_hashes
        or covariance_hashes[-1] != recomputed_covariance.get("receipt_sha256")
    ):
        failures.append("hmm_risk_model_selection_contract_unsatisfied")
    return list(dict.fromkeys(str(value) for value in failures))


def validate_d4_training_receipts(entry: Mapping[str, Any]) -> bool:
    """Validate durable D4 receipts with the same authorities used by their writers."""

    return not d4_training_receipt_readback_failures(entry)


def _candidate_status(entry: Mapping[str, Any]) -> bool:
    likelihood = entry.get("likelihood", {})
    covariance = entry.get("covariance", {})
    likelihood_evidence = likelihood.get("evidence", {}) if isinstance(likelihood, Mapping) else {}
    covariance_hashes = likelihood_evidence.get("covariance_receipt_sha256_history", ())
    return (
        entry.get("fit_status") == "accepted"
        and entry.get("model_entry_status") == "accepted"
        and entry.get("model_entry_valid") is True
        and likelihood.get("convergence_valid") is True
        and likelihood.get("likelihood_valid") is True
        and covariance.get("covariance_valid") is True
        and isinstance(covariance_hashes, Sequence)
        and not isinstance(covariance_hashes, (str, bytes))
        and bool(covariance_hashes)
        and covariance_hashes[-1] == covariance.get("receipt_sha256")
        and entry.get("train_occupancy", {}).get("train_occupancy_valid") is True
        and validate_d4_training_receipts(entry)
    )


def _rejected_stage(
    *,
    stage: str,
    status: str,
    valid: bool,
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    failures = list(receipt.get("failure_reason_codes") or ())
    blockers = list(receipt.get("blocking_reason_codes") or ())
    if not failures and not blockers:
        failures.append("hmm_risk_model_selection_contract_unsatisfied")
    primary = receipt.get("primary_reason_code") or (failures[0] if failures else blockers[0])
    return {
        "stage": stage,
        "status": status,
        "valid": valid,
        "failure_reason_codes": failures,
        "blocking_reason_codes": blockers,
        "primary_reason_code": primary,
    }


def _candidate_rejection_summary(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Retain compact train-only D4 rejection evidence for every ineligible sector entry."""

    rejected: list[dict[str, Any]] = []
    for entry in entries:
        stages: list[dict[str, Any]] = []
        if entry.get("fit_status") != "accepted":
            failures = list(entry.get("failure_reason_codes") or ())
            blockers = list(entry.get("blocking_reason_codes") or ())
            if not failures and not blockers:
                failures.append("hmm_risk_model_fit_failed")
            stages.append(
                {
                    "stage": str(entry.get("failure_stage") or "fit"),
                    "status": str(entry.get("model_entry_status") or "failed"),
                    "valid": False,
                    "failure_reason_codes": failures,
                    "blocking_reason_codes": blockers,
                    "primary_reason_code": (
                        failures[0] if failures else blockers[0] if blockers else "hmm_risk_model_fit_failed"
                    ),
                }
            )
        else:
            likelihood = entry.get("likelihood")
            if isinstance(likelihood, Mapping) and not (
                likelihood.get("convergence_valid") is True and likelihood.get("likelihood_valid") is True
            ):
                failures = list(likelihood.get("failure_reason_codes") or ())
                blockers = list(likelihood.get("blocking_reason_codes") or ())
                status = "failed" if failures else "insufficient_evidence" if blockers else "failed"
                stages.append(_rejected_stage(stage="likelihood", status=status, valid=False, receipt=likelihood))
            covariance = entry.get("covariance")
            if isinstance(covariance, Mapping) and covariance.get("covariance_valid") is not True:
                stages.append(
                    _rejected_stage(
                        stage="covariance",
                        status=str(covariance.get("covariance_status") or "failed"),
                        valid=False,
                        receipt=covariance,
                    )
                )
            occupancy = entry.get("train_occupancy")
            if isinstance(occupancy, Mapping) and occupancy.get("train_occupancy_valid") is not True:
                stages.append(
                    _rejected_stage(
                        stage="train_occupancy",
                        status=str(occupancy.get("train_occupancy_status") or "failed"),
                        valid=False,
                        receipt=occupancy,
                    )
                )
            if entry.get("model_entry_valid") is not True and not stages:
                stages.append(
                    {
                        "stage": "model_entry",
                        "status": str(entry.get("model_entry_status") or "failed"),
                        "valid": False,
                        "failure_reason_codes": ["hmm_risk_model_selection_contract_unsatisfied"],
                        "blocking_reason_codes": [],
                        "primary_reason_code": "hmm_risk_model_selection_contract_unsatisfied",
                    }
                )
            readback_failures = d4_training_receipt_readback_failures(entry)
            if readback_failures and not stages:
                stages.append(
                    {
                        "stage": "d4_receipt_readback",
                        "status": "failed",
                        "valid": False,
                        "failure_reason_codes": readback_failures,
                        "blocking_reason_codes": [],
                        "primary_reason_code": readback_failures[0],
                    }
                )
        if stages:
            rejected.append(
                {
                    "sector_code": str(entry.get("sector_code") or ""),
                    "entry_receipt_sha256": str(entry.get("entry_receipt_sha256") or ""),
                    "failed_stages": stages,
                }
            )
    return rejected


def _canonical_receipt_hash_valid(receipt: Mapping[str, Any], field: str = "receipt_sha256") -> bool:
    expected = str(receipt.get(field) or "")
    body = {key: value for key, value in receipt.items() if key != field}
    return len(expected) == 64 and canonical_sha256(body) == expected


def select_level_restart(
    first_repeat: Mapping[str, Any],
    second_repeat: Mapping[str, Any],
    *,
    family: str,
    level: str,
    expected_sector_codes: Sequence[str],
    feature_count: int,
    feature_domain_policy_sha256: str,
) -> dict[str, Any]:
    """Apply D5-01-B after D5-02 bitwise repeat equality, without reading validation evidence."""

    codes = tuple(sorted(str(value) for value in expected_sector_codes))
    policy_identity_valid = _valid_sha256(feature_domain_policy_sha256)
    expected_count = 31 if level == "L1" else 131 if level == "L2" else 0
    mixed_dimension = uses_mixed_dimension_level(family, level)
    blockers: list[str] = []
    failures: list[str] = []
    if len(codes) != expected_count or len(set(codes)) != expected_count:
        failures.append("hmm_risk_model_selection_level_incomplete")
    if not policy_identity_valid:
        blockers.append("hmm_risk_model_selection_contract_unsatisfied")
    for repeat in (first_repeat, second_repeat):
        if tuple(repeat.get("schedule") or ()) != RESTART_SCHEDULE:
            blockers.append("hmm_risk_model_restart_schedule_incomplete")
        if repeat.get("family") != family or repeat.get("level") != level:
            failures.append("hmm_risk_model_selection_contract_unsatisfied")
        if tuple(repeat.get("canonical_sector_codes") or ()) != codes:
            failures.append("hmm_risk_model_selection_level_incomplete")
        if len(tuple(repeat.get("feature_names") or ())) != feature_count:
            failures.append("hmm_risk_model_selection_contract_unsatisfied")
        if mixed_dimension and (
            repeat.get("schema_version") != MIXED_REPEAT_SCHEMA_VERSION
            or repeat.get("dimension_contract_version") != MIXED_DIMENSION_CONTRACT_VERSION
            or repeat.get("feature_count") != feature_count
        ):
            failures.append(INACTIVE_DIMENSION_REASON_CODE)
        if repeat.get("validation_accessed") is not False or repeat.get("future_utility_accessed") is not False:
            failures.append("hmm_risk_model_selection_contract_unsatisfied")
        if repeat.get("semantic_labelability_accessed") is not False or repeat.get("d6_status_accessed") is not False:
            failures.append("hmm_risk_model_selection_contract_unsatisfied")
    first_entries = list(first_repeat.get("entries") or ())
    second_entries = list(second_repeat.get("entries") or ())
    model_maps: list[dict[tuple[int, str], Mapping[str, Any]]] = []
    for repeat, entries in ((first_repeat, first_entries), (second_repeat, second_entries)):
        models = list(repeat.get("models") or ())
        if len(entries) != len(RESTART_SCHEDULE) * expected_count:
            failures.append("hmm_risk_model_selection_level_incomplete")
        entry_keys: set[tuple[int, str]] = set()
        for entry in entries:
            if not isinstance(entry, Mapping) or not _canonical_receipt_hash_valid(entry, "entry_receipt_sha256"):
                failures.append("hmm_risk_model_selection_repeat_mismatch")
                continue
            key = (int(entry.get("seed", -1)), str(entry.get("sector_code") or ""))
            if key in entry_keys or key[0] not in RESTART_SCHEDULE or key[1] not in codes:
                failures.append("hmm_risk_model_selection_level_incomplete")
            entry_keys.add(key)
            if entry.get("fit_status") == "accepted":
                if mixed_dimension:
                    projection = entry.get("projection_receipt")
                    try:
                        if not isinstance(projection, Mapping):
                            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
                        effective_count = validate_projection_receipt(
                            projection,
                            family=family,
                            level=level,
                            sector_code=str(entry.get("sector_code") or ""),
                            full_feature_names=repeat.get("feature_names") or (),
                            preprocess=repeat.get("preprocess") or {},
                        )
                        if (
                            entry.get("schema_version") != MIXED_TRAINING_ENTRY_SCHEMA_VERSION
                            or entry.get("dimension_contract_version") != MIXED_DIMENSION_CONTRACT_VERSION
                            or entry.get("feature_count") != feature_count
                            or entry.get("likelihood_feature_count") != effective_count
                            or entry.get("projection_sha256") != projection["projection_sha256"]
                            or projection.get("projected_matrix_shape", [None])[0] != entry.get("training_rows")
                            or projection.get("source_identities", {}).get("feature_domain_policy_sha256")
                            != feature_domain_policy_sha256
                        ):
                            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
                    except (StateModelSetError, TypeError, ValueError):
                        failures.append(INACTIVE_DIMENSION_REASON_CODE)
                for field, contract in (
                    ("likelihood", D4_LIKELIHOOD_VERSION),
                    ("covariance", D4_COVARIANCE_VERSION),
                    ("train_occupancy", D4_OCCUPANCY_VERSION),
                ):
                    receipt = entry.get(field)
                    if (
                        not isinstance(receipt, Mapping)
                        or receipt.get("contract_version") != contract
                        or not _canonical_receipt_hash_valid(receipt)
                    ):
                        failures.append("hmm_risk_model_selection_repeat_mismatch")
        model_map: dict[tuple[int, str], Mapping[str, Any]] = {}
        for model in models:
            if not isinstance(model, Mapping):
                failures.append("hmm_risk_model_selection_repeat_mismatch")
                continue
            expected_model_hash = str(model.get("model_payload_sha256") or "")
            model_body = {key: value for key, value in model.items() if key != "model_payload_sha256"}
            key = (int(model.get("seed", -1)), str(model.get("sector_code") or ""))
            if (
                len(expected_model_hash) != 64
                or canonical_sha256(model_body) != expected_model_hash
                or key in model_map
                or key[0] not in RESTART_SCHEDULE
                or key[1] not in codes
                or model.get("family") != family
                or model.get("level") != level
            ):
                failures.append("hmm_risk_model_selection_repeat_mismatch")
                continue
            if mixed_dimension:
                projection = model.get("projection_receipt")
                try:
                    if not isinstance(projection, Mapping):
                        raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
                    effective_count = validate_projection_receipt(
                        projection,
                        family=family,
                        level=level,
                        sector_code=key[1],
                        full_feature_names=model.get("feature_names") or (),
                        preprocess=model.get("preprocess") or {},
                        means_shape=np.asarray(model.get("means"), dtype=np.float64).shape,
                        covariance_shape=np.asarray(model.get("covars"), dtype=np.float64).shape,
                    )
                    if (
                        model.get("schema_version") != "hmm_risk_b3_inactive_dimension_model_entry_v1"
                        or model.get("dimension_contract_version") != MIXED_DIMENSION_CONTRACT_VERSION
                        or model.get("feature_count") != feature_count
                        or model.get("likelihood_feature_count") != effective_count
                        or model.get("projection_sha256") != projection["projection_sha256"]
                        or projection.get("source_identities", {}).get("feature_domain_policy_sha256")
                        != feature_domain_policy_sha256
                    ):
                        raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
                except (StateModelSetError, TypeError, ValueError):
                    failures.append(INACTIVE_DIMENSION_REASON_CODE)
                    continue
            model_map[key] = model
        for entry in entries:
            if isinstance(entry, Mapping) and entry.get("fit_status") == "accepted":
                key = (int(entry.get("seed", -1)), str(entry.get("sector_code") or ""))
                model = model_map.get(key)
                if model is None or entry.get("model_payload_sha256") != model.get("model_payload_sha256"):
                    failures.append("hmm_risk_model_selection_repeat_mismatch")
                elif mixed_dimension and (
                    entry.get("projection_sha256") != model.get("projection_sha256")
                    or entry.get("likelihood_feature_count") != model.get("likelihood_feature_count")
                ):
                    failures.append(INACTIVE_DIMENSION_REASON_CODE)
        model_maps.append(model_map)
        if canonical_sha256(models) != repeat.get("model_payload_sha256"):
            failures.append("hmm_risk_model_selection_repeat_mismatch")
        candidate_payload = {
            "family": repeat.get("family"),
            "level": repeat.get("level"),
            "schedule": list(repeat.get("schedule") or ()),
            "canonical_sector_codes": list(repeat.get("canonical_sector_codes") or ()),
            "feature_names": list(repeat.get("feature_names") or ()),
            "preprocess": repeat.get("preprocess"),
            "numeric_environment": repeat.get("numeric_environment"),
            "entries": entries,
            "models": models,
        }
        if mixed_dimension:
            candidate_payload["dimension_contract_version"] = repeat.get("dimension_contract_version")
        if canonical_sha256(candidate_payload) != repeat.get("candidate_payload_sha256"):
            failures.append("hmm_risk_model_selection_repeat_mismatch")
    if canonical_sha256(first_entries) != canonical_sha256(second_entries):
        failures.append("hmm_risk_model_selection_repeat_mismatch")
    if first_repeat.get("candidate_payload_sha256") != second_repeat.get("candidate_payload_sha256"):
        failures.append("hmm_risk_model_selection_repeat_mismatch")
    if first_repeat.get("model_payload_sha256") != second_repeat.get("model_payload_sha256"):
        failures.append("hmm_risk_model_selection_repeat_mismatch")

    candidates: list[dict[str, Any]] = []
    for schedule_index, seed in enumerate(RESTART_SCHEDULE):
        entries = sorted(
            (
                entry
                for entry in first_entries
                if entry.get("family") == family and entry.get("level") == level and entry.get("seed") == seed
            ),
            key=lambda entry: str(entry.get("sector_code") or ""),
        )
        entry_codes = tuple(str(entry.get("sector_code") or "") for entry in entries)
        rejection_summary = _candidate_rejection_summary(entries)
        candidate_failures = [
            str(code)
            for item in rejection_summary
            for stage in item["failed_stages"]
            for code in stage["failure_reason_codes"]
        ]
        candidate_blockers = [
            str(code)
            for item in rejection_summary
            for stage in item["failed_stages"]
            for code in stage["blocking_reason_codes"]
        ]
        if entry_codes != codes:
            candidate_failures.append("hmm_risk_model_selection_level_incomplete")
        if any(not _candidate_status(entry) for entry in entries) and not rejection_summary:
            candidate_failures.append("hmm_risk_model_selection_contract_unsatisfied")
        accepted_entry_codes = {
            str(entry.get("sector_code") or "") for entry in entries if entry.get("fit_status") == "accepted"
        }
        if any((seed, code) not in model_maps[0] for code in accepted_entry_codes):
            candidate_failures.append("hmm_risk_model_selection_repeat_mismatch")
        eligible = (
            entry_codes == codes
            and all(_candidate_status(entry) for entry in entries)
            and all((seed, code) in model_maps[0] for code in codes)
        )
        scores: list[float] = []
        score_dimensions: list[int] = []
        if eligible:
            for entry in entries:
                try:
                    final = float(entry["final_train_log_likelihood"])
                    rows = int(entry["training_rows"])
                    dimensions = int(entry["likelihood_feature_count"] if mixed_dimension else entry["feature_count"])
                    score = final / (rows * dimensions)
                except (KeyError, TypeError, ValueError, ZeroDivisionError):
                    eligible = False
                    candidate_failures.append("hmm_risk_model_selection_contract_unsatisfied")
                    break
                expected_dimension = feature_count
                if mixed_dimension:
                    projection = entry.get("projection_receipt")
                    try:
                        if not isinstance(projection, Mapping):
                            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
                        expected_dimension = validate_projection_receipt(
                            projection,
                            family=family,
                            level=level,
                            sector_code=str(entry.get("sector_code") or ""),
                            full_feature_names=first_repeat.get("feature_names") or (),
                            preprocess=first_repeat.get("preprocess") or {},
                        )
                        if projection.get("projected_matrix_shape", [None])[0] != rows:
                            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
                    except StateModelSetError:
                        eligible = False
                        candidate_failures.append(INACTIVE_DIMENSION_REASON_CODE)
                        break
                if rows <= 0 or dimensions != expected_dimension:
                    eligible = False
                    candidate_failures.append(
                        INACTIVE_DIMENSION_REASON_CODE
                        if mixed_dimension
                        else "hmm_risk_model_selection_contract_unsatisfied"
                    )
                    break
                if not math.isfinite(score):
                    eligible = False
                    candidate_failures.append("hmm_risk_model_selection_score_non_finite")
                    break
                scores.append(score)
                score_dimensions.append(dimensions)
        aggregate = None
        if eligible:
            ordered_scores = sorted(scores)
            ordered_sector_scores = []
            for code, entry, score, dimension in zip(codes, entries, scores, score_dimensions, strict=True):
                score_body = {
                    "sector_code": code,
                    "score": score,
                    "training_rows": int(entry["training_rows"]),
                    "final_train_log_likelihood": float(entry["final_train_log_likelihood"]),
                    "effective_dimension": dimension,
                    "denominator": int(entry["training_rows"]) * dimension,
                    "formula": "final_train_log_likelihood/(training_rows*effective_dimension)",
                }
                if mixed_dimension:
                    score_body.update(
                        {
                            "schema_version": "hmm_risk_b3_d5_effective_dimension_score_receipt_v1",
                            "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                            "projection_sha256": entry["projection_sha256"],
                        }
                    )
                ordered_sector_scores.append(
                    {
                        **score_body,
                        **({"score_sha256": canonical_sha256(score_body)} if mixed_dimension else {}),
                    }
                )
            aggregate = {
                "minimum": ordered_scores[0],
                "median": ordered_scores[len(ordered_scores) // 2],
                "mean": math.fsum(scores) / len(scores),
                "ordered_sector_scores": ordered_sector_scores,
            }
        candidates.append(
            {
                "seed": seed,
                "schedule_index": schedule_index,
                "eligible": eligible,
                "aggregate": aggregate,
                "entry_receipt_hashes": [str(entry.get("entry_receipt_sha256") or "") for entry in entries],
                "failure_reason_codes": list(dict.fromkeys(candidate_failures)),
                "blocking_reason_codes": list(dict.fromkeys(candidate_blockers)),
                "primary_reason_code": (
                    candidate_failures[0]
                    if candidate_failures
                    else candidate_blockers[0]
                    if candidate_blockers
                    else None
                ),
                "rejection_summary": rejection_summary,
                "rejection_summary_sha256": canonical_sha256(rejection_summary),
                "warning_reason_codes": sorted(
                    {
                        str(code)
                        for entry in entries
                        for code in entry.get("likelihood", {}).get("warning_reason_codes", ())
                    }
                ),
            }
        )
    pool = [candidate for candidate in candidates if candidate["eligible"]]
    filter_receipts: list[dict[str, Any]] = []
    for component in ("minimum", "median", "mean"):
        if not pool:
            break
        best = max(float(candidate["aggregate"][component]) for candidate in pool)
        survivors = []
        for candidate in pool:
            value = float(candidate["aggregate"][component])
            tolerance = 1e-12 + 1e-12 * max(abs(best), abs(value))
            if best - value <= tolerance:
                survivors.append(candidate)
        filter_receipts.append(
            {
                "component": component,
                "best": best,
                "survivor_seeds": [candidate["seed"] for candidate in survivors],
            }
        )
        pool = survivors
    selected = min(pool, key=lambda candidate: candidate["schedule_index"]) if pool else None
    if not blockers and not failures and selected is None:
        failures.append("hmm_risk_model_selection_unavailable")
    evidence = {
        "family": family,
        "level": level,
        "feature_domain_policy_sha256": feature_domain_policy_sha256,
        "canonical_sector_codes": list(codes),
        "canonical_sector_set_sha256": canonical_sha256(list(codes)),
        "schedule": list(RESTART_SCHEDULE),
        "feature_count": feature_count,
        "repeat_entries_sha256": canonical_sha256(first_entries),
        "candidates": candidates,
        "lexicographic_filters": filter_receipts,
        "selected_seed": None if selected is None else selected["seed"],
        "selected_schedule_index": None if selected is None else selected["schedule_index"],
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "selection_followed_by_refit": False,
    }
    if mixed_dimension:
        evidence["dimension_contract_version"] = MIXED_DIMENSION_CONTRACT_VERSION
    receipt = _status_receipt(
        contract_version=D5_SELECTION_VERSION,
        failures=failures,
        blockers=blockers,
        evidence=evidence,
    )
    receipt.pop("receipt_sha256")
    receipt["level_selection_status"] = receipt.pop("status")
    receipt["level_selection_valid"] = receipt.pop("valid")
    return {**receipt, "receipt_sha256": canonical_sha256(receipt)}


def evaluate_semantic_validation(
    posteriors: Any,
    dates: Sequence[date] | None,
    utility_components: Mapping[str, Any] | None,
    *,
    frozen_input_manifest: Mapping[str, Any] | None,
    selected_model_payload_sha256: str,
) -> dict[str, Any]:
    """Apply D6-01-B after selection; hard argmax remains the sole semantic authority."""

    assignment_failures: list[str] = []
    assignment_blockers: list[str] = []
    evidence_failures: list[str] = []
    evidence_blockers: list[str] = []
    manifest_valid = isinstance(frozen_input_manifest, Mapping)
    if not manifest_valid:
        assignment_blockers.append("hmm_risk_semantic_validation_evidence_missing")
        evidence_blockers.append("hmm_risk_semantic_validation_evidence_missing")
        frozen_input_manifest = {}
    required_manifest_hashes = (
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "calendar_manifest_hash",
        "l2_stock_fact_manifest_hash",
        "feature_domain_policy_sha256",
    )
    if manifest_valid and (
        frozen_input_manifest.get("schema_version") != "hmm_risk_d6_frozen_input_manifest_v1"
        or frozen_input_manifest.get("benchmark_identity") != "000300.SH"
        or frozen_input_manifest.get("direct_sector_level") not in {"L1", "L2"}
        or not str(frozen_input_manifest.get("sector_code") or "").strip()
        or not _valid_sha256(frozen_input_manifest.get("validation_observation_sha256"))
        or any(not _valid_sha256(frozen_input_manifest.get(field)) for field in required_manifest_hashes)
        or not _valid_sha256(selected_model_payload_sha256)
    ):
        assignment_blockers.append("hmm_risk_semantic_validation_evidence_missing")
        evidence_blockers.append("hmm_risk_semantic_validation_evidence_missing")
    if dates is None:
        assignment_blockers.append("hmm_risk_semantic_validation_evidence_missing")
        ordered_dates: tuple[date, ...] = ()
        probabilities = np.empty((0, 3), dtype=np.float64)
        hard = np.empty((0,), dtype=np.int64)
        metrics: dict[str, Any] = {}
    else:
        try:
            ordered_dates = _ordered_unique_dates(dates)
            probabilities = _finite_array(posteriors, shape=(len(ordered_dates), 3))
            hard, metrics = _hard_sequence_metrics(probabilities, ordered_dates)
        except (TypeError, ValueError):
            assignment_failures.append("hmm_risk_semantic_validation_posterior_invalid")
            ordered_dates = ()
            probabilities = np.empty((0, 3), dtype=np.float64)
            hard = np.empty((0,), dtype=np.int64)
            metrics = {}
    if ordered_dates and (
        len(ordered_dates) != 182 or ordered_dates[0] != date(2024, 7, 1) or ordered_dates[-1] != date(2025, 3, 31)
    ):
        assignment_failures.append("hmm_risk_semantic_validation_date_sequence_invalid")
    ordered_date_strings = [value.isoformat() for value in ordered_dates]
    if ordered_dates and (
        frozen_input_manifest.get("validation_dates") != ordered_date_strings
        or frozen_input_manifest.get("validation_dates_sha256") != canonical_sha256(ordered_date_strings)
    ):
        assignment_blockers.append("hmm_risk_semantic_validation_evidence_missing")
    if metrics:
        if metrics["row_sum_max_abs_error"] > 1e-12:
            assignment_failures.append("hmm_risk_semantic_validation_posterior_normalization_failed")
        if metrics["top1_top2_min_margin"] <= 1e-12:
            assignment_failures.append("hmm_risk_semantic_validation_posterior_tie")
    assignment_evidence = {
        "direct_sector_level": frozen_input_manifest.get("direct_sector_level"),
        "sector_code": frozen_input_manifest.get("sector_code"),
        "validation_observation_sha256": frozen_input_manifest.get("validation_observation_sha256"),
        **{field: frozen_input_manifest.get(field) for field in required_manifest_hashes},
        "validation_rows": len(ordered_dates),
        "ordered_date_sha256": canonical_sha256(ordered_date_strings),
        "posterior_sha256": canonical_sha256(probabilities.tolist()),
        "frozen_input_manifest_sha256": canonical_sha256(dict(frozen_input_manifest)),
        "selected_model_payload_sha256": selected_model_payload_sha256,
        **metrics,
    }
    assignment = _status_receipt(
        contract_version=D6_SEMANTIC_VERSION,
        failures=assignment_failures,
        blockers=assignment_blockers,
        evidence=assignment_evidence,
    )
    assignment.pop("receipt_sha256")
    assignment["semantic_assignment_status"] = assignment.pop("status")
    assignment["semantic_assignment_valid"] = assignment.pop("valid")
    assignment["receipt_sha256"] = canonical_sha256(assignment)

    combined = np.empty((0,), dtype=np.float64)
    components: dict[str, np.ndarray] = {}
    if utility_components is None:
        evidence_blockers.append("hmm_risk_semantic_validation_evidence_missing")
    else:
        if utility_components.get("source_cutoff") != "2025-04-30":
            evidence_blockers.append("hmm_risk_semantic_validation_evidence_missing")
        if utility_components.get("formula_version") != "hmm_risk_hard_future_excess_035_035_030_v1":
            evidence_blockers.append("hmm_risk_semantic_validation_evidence_missing")
        try:
            for key in ("excess_return_5d", "excess_return_10d", "excess_return_20d"):
                components[key] = _finite_array(utility_components[key], shape=(len(ordered_dates),))
            combined = (
                0.35 * components["excess_return_5d"]
                + 0.35 * components["excess_return_10d"]
                + 0.30 * components["excess_return_20d"]
            )
        except (KeyError, TypeError, ValueError):
            evidence_failures.append("hmm_risk_semantic_utility_non_finite")
    if components:
        component_hashes = {key: canonical_sha256(value.tolist()) for key, value in sorted(components.items())}
        if frozen_input_manifest.get("utility_component_sha256") != component_hashes:
            evidence_blockers.append("hmm_risk_semantic_validation_evidence_missing")
        if (
            frozen_input_manifest.get("source_cutoff") != "2025-04-30"
            or frozen_input_manifest.get("formula_version") != "hmm_risk_hard_future_excess_035_035_030_v1"
        ):
            evidence_blockers.append("hmm_risk_semantic_validation_evidence_missing")
    semantic_mapping: dict[str, str] | None = None
    state_utility: dict[str, Any] = {}
    if assignment["semantic_assignment_valid"] and combined.shape == (len(ordered_dates),):
        count_threshold = max(5, math.ceil(0.02 * len(ordered_dates)))
        means: dict[int, float] = {}
        for state in range(3):
            state_metrics = metrics["states"][str(state)]
            mask = hard == state
            values = combined[mask]
            variance = float(np.var(values, ddof=1)) if values.size >= 2 else float("nan")
            mean = float(math.fsum(float(value) for value in values) / values.size) if values.size else float("nan")
            state_utility[str(state)] = {
                "count": int(values.size),
                "mean": mean if math.isfinite(mean) else None,
                "sample_variance_ddof_1": variance if math.isfinite(variance) else None,
            }
            if state_metrics["hard_count"] == 0:
                evidence_failures.append("hmm_risk_semantic_hard_state_missing")
            if state_metrics["hard_count"] < count_threshold:
                evidence_failures.append("hmm_risk_semantic_validation_state_count_insufficient")
            if state_metrics["normalized_occupancy"] < 0.02:
                evidence_failures.append("hmm_risk_semantic_validation_occupancy_insufficient")
            if state_metrics["calendar_month_count"] < 2:
                evidence_failures.append("hmm_risk_semantic_validation_month_coverage_insufficient")
            if state_metrics["contiguous_run_count"] < 2:
                evidence_failures.append("hmm_risk_semantic_validation_run_coverage_insufficient")
            if state_metrics["incoming_transition_count"] < 2 or state_metrics["outgoing_transition_count"] < 2:
                evidence_failures.append("hmm_risk_semantic_validation_transition_coverage_insufficient")
            if state_metrics["maximum_single_run_share"] is None or state_metrics["maximum_single_run_share"] > 0.9:
                evidence_failures.append("hmm_risk_semantic_validation_run_concentration_exceeded")
            if not math.isfinite(mean):
                evidence_failures.append("hmm_risk_semantic_utility_non_finite")
            elif not math.isfinite(variance):
                evidence_failures.append("hmm_risk_semantic_utility_variance_non_finite")
            else:
                means[state] = mean
        if len(means) == 3:
            ordered_states = sorted(means, key=means.get)
            for left, right in zip(ordered_states, ordered_states[1:]):
                gap = means[right] - means[left]
                tolerance = max(1e-12, 32 * np.finfo(np.float64).eps * max(1.0, abs(means[left]), abs(means[right])))
                if not gap > tolerance:
                    evidence_failures.append("hmm_risk_semantic_validation_utility_gap_insufficient")
            if not evidence_failures and not evidence_blockers:
                semantic_mapping = {
                    str(ordered_states[0]): "fading",
                    str(ordered_states[1]): "neutral",
                    str(ordered_states[2]): "trending",
                }
    elif not assignment["semantic_assignment_valid"]:
        evidence_blockers.append("hmm_risk_semantic_evidence_insufficient")
    if combined.size and frozen_input_manifest.get("combined_utility_sha256") != canonical_sha256(combined.tolist()):
        evidence_blockers.append("hmm_risk_semantic_validation_evidence_missing")
    semantic_evidence = {
        "direct_sector_level": frozen_input_manifest.get("direct_sector_level"),
        "sector_code": frozen_input_manifest.get("sector_code"),
        "validation_observation_sha256": frozen_input_manifest.get("validation_observation_sha256"),
        **{field: frozen_input_manifest.get(field) for field in required_manifest_hashes},
        "utility_formula": "0.35*excess_return_5d+0.35*excess_return_10d+0.30*excess_return_20d",
        "utility_component_sha256": {
            key: canonical_sha256(value.tolist()) for key, value in sorted(components.items())
        },
        "combined_utility_sha256": canonical_sha256(combined.tolist()),
        "frozen_input_manifest_sha256": canonical_sha256(dict(frozen_input_manifest)),
        "selected_model_payload_sha256": selected_model_payload_sha256,
        "state_utility": state_utility,
        "semantic_mapping": semantic_mapping,
        "hard_semantic_authority": True,
        "soft_evidence_used_for_acceptance": False,
        "selection_reexecuted": False,
    }
    evidence_receipt = _status_receipt(
        contract_version=D6_SEMANTIC_VERSION,
        failures=evidence_failures,
        blockers=evidence_blockers,
        evidence=semantic_evidence,
    )
    evidence_receipt.pop("receipt_sha256")
    evidence_receipt["semantic_evidence_status"] = evidence_receipt.pop("status")
    evidence_receipt["semantic_evidence_valid"] = evidence_receipt.pop("valid")
    evidence_receipt["receipt_sha256"] = canonical_sha256(evidence_receipt)
    return {
        "contract_version": D6_SEMANTIC_VERSION,
        "assignment": assignment,
        "semantic_evidence": evidence_receipt,
        "semantic_mapping": semantic_mapping if evidence_receipt["semantic_evidence_valid"] else None,
    }
