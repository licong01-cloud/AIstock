from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

import numpy as np

from backend.services.hmm_risk.state_model_set import canonical_sha256


D3_CONTRACT_VERSION = "hmm_risk_c008_b3_d3_03_a_v1"
D4_LIKELIHOOD_VERSION = "hmm_risk_c008_b3_d4_01_a_v1"
D4_COVARIANCE_VERSION = "hmm_risk_c008_b3_d4_02_a_v1"
D4_OCCUPANCY_VERSION = "hmm_risk_c008_b3_d4_03_b_v1"
D5_SELECTION_VERSION = "hmm_risk_c008_b3_d5_01_b_v1"
D6_SEMANTIC_VERSION = "hmm_risk_c008_b3_d6_01_b_v1"
L2_RETRAIN_VERSION = "hmm_risk_c008_b3_l2_retrain_a_v1"
RESTART_SCHEDULE = tuple(range(42, 50))


def _finite_array(value: Any, *, shape: tuple[int, ...] | None = None) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    if shape is not None and result.shape != shape:
        raise ValueError(f"array shape mismatch expected={shape} actual={result.shape}")
    if not np.isfinite(result).all():
        raise ValueError("array contains non-finite values")
    return result


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


def evaluate_likelihood_acceptance(monitor: Mapping[str, Any] | None) -> dict[str, Any]:
    """Apply D4-01-A without treating hmmlearn monitor convergence as likelihood acceptance."""

    if monitor is None:
        return _status_receipt(
            contract_version=D4_LIKELIHOOD_VERSION,
            blockers=("hmm_risk_model_likelihood_evidence_missing",),
            evidence={},
        )
    failures: list[str] = []
    blockers: list[str] = []
    warnings: list[str] = []
    required_fields = {"converged", "iterations", "maximum_iterations", "history"}
    if not required_fields.issubset(monitor):
        return _status_receipt(
            contract_version=D4_LIKELIHOOD_VERSION,
            blockers=("hmm_risk_model_likelihood_evidence_missing",),
            evidence={"monitor_fields_present": sorted(str(key) for key in monitor)},
        )
    try:
        converged = bool(monitor["converged"])
        iterations = int(monitor["iterations"])
        maximum_iterations = int(monitor["maximum_iterations"])
        history = np.asarray(monitor["history"], dtype=np.float64)
    except (TypeError, ValueError):
        return _status_receipt(
            contract_version=D4_LIKELIHOOD_VERSION,
            failures=("hmm_risk_model_likelihood_history_invalid",),
            evidence={"history_parseable": False},
        )
    history_is_finite = bool(np.isfinite(history).all())
    if not history_is_finite:
        failures.append("hmm_risk_model_likelihood_non_finite")
    if history.ndim != 1 or history.size < 2 or history.size != iterations:
        failures.append("hmm_risk_model_likelihood_history_invalid")
    if not converged:
        failures.append("hmm_risk_model_monitor_not_converged")
    if not 2 <= iterations < maximum_iterations or maximum_iterations != 300:
        failures.append("hmm_risk_model_likelihood_iteration_contract_failed")

    deltas: list[dict[str, Any]] = []
    if history.ndim == 1 and history.size >= 2 and history_is_finite:
        for index in range(1, int(history.size)):
            previous = float(history[index - 1])
            current = float(history[index])
            absolute = current - previous
            relative = absolute / max(1.0, abs(previous))
            terminal = index == history.size - 1
            deltas.append(
                {
                    "index": index,
                    "terminal": terminal,
                    "previous": previous,
                    "current": current,
                    "absolute": absolute,
                    "relative": relative,
                }
            )
            if not terminal and absolute < 0.0:
                failures.append("hmm_risk_model_likelihood_nonterminal_decrease")
            if terminal and absolute >= 0.0 and not absolute < 0.01:
                failures.append("hmm_risk_model_likelihood_tolerance_failed")
            if terminal and absolute < 0.0:
                if relative < -2e-5:
                    failures.append("hmm_risk_model_likelihood_tolerance_failed")
                else:
                    warnings.append("hmm_risk_model_likelihood_terminal_decrease_warning")
    evidence = {
        "monitor_converged": converged,
        "iterations": iterations,
        "maximum_iterations": maximum_iterations,
        "history": history.tolist() if history_is_finite else None,
        "history_sha256": canonical_sha256(history.tolist()) if history_is_finite else None,
        "history_non_finite_count": int((~np.isfinite(history)).sum()),
        "deltas": deltas,
        "fit_tolerance": 0.01,
        "terminal_relative_tolerance": -2e-5,
    }
    return _status_receipt(
        contract_version=D4_LIKELIHOOD_VERSION,
        failures=failures,
        blockers=blockers,
        warnings=warnings,
        evidence=evidence,
    )


def evaluate_covariance_acceptance(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    """Apply D4-02-A to raw fitted covariance; no clipping or projection is performed."""

    if evidence is None:
        return _status_receipt(
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
        return _status_receipt(
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
        return _status_receipt(
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
    return _status_receipt(
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


def evaluate_train_occupancy(posteriors: Any, dates: Sequence[date] | None) -> dict[str, Any]:
    """Apply D4-03-B to causal train hard assignments only."""

    if dates is None:
        return _status_receipt(
            contract_version=D4_OCCUPANCY_VERSION,
            blockers=("hmm_risk_model_train_evidence_missing",),
            evidence={},
        )
    try:
        ordered_dates = _ordered_unique_dates(dates)
        probabilities = _finite_array(posteriors, shape=(len(ordered_dates), 3))
        hard, metrics = _hard_sequence_metrics(probabilities, ordered_dates)
    except (TypeError, ValueError):
        return _status_receipt(
            contract_version=D4_OCCUPANCY_VERSION,
            failures=("hmm_risk_model_train_posterior_invalid",),
            evidence={},
        )
    failures: list[str] = []
    if metrics["row_sum_max_abs_error"] > 1e-12:
        failures.append("hmm_risk_model_train_posterior_normalization_failed")
    if metrics["top1_top2_min_margin"] <= 1e-12:
        failures.append("hmm_risk_model_posterior_tie")
    count_threshold = max(5, math.ceil(0.01 * len(ordered_dates)))
    for state in metrics["states"].values():
        if state["hard_count"] < count_threshold:
            failures.append("hmm_risk_model_train_state_count_insufficient")
        if state["normalized_occupancy"] < 0.01:
            failures.append("hmm_risk_model_train_occupancy_insufficient")
        if state["calendar_month_count"] < 3:
            failures.append("hmm_risk_model_train_month_coverage_insufficient")
        if state["contiguous_run_count"] < 3:
            failures.append("hmm_risk_model_train_run_coverage_insufficient")
        if state["incoming_transition_count"] < 2 or state["outgoing_transition_count"] < 2:
            failures.append("hmm_risk_model_train_transition_coverage_insufficient")
        if state["maximum_single_run_share"] is None or state["maximum_single_run_share"] > 0.8:
            failures.append("hmm_risk_model_train_run_concentration_failed")
    evidence = {
        "train_rows": len(ordered_dates),
        "ordered_date_sha256": canonical_sha256([value.isoformat() for value in ordered_dates]),
        "posterior_sha256": canonical_sha256(probabilities.tolist()),
        "count_threshold": count_threshold,
        "occupancy_threshold": 0.01,
        "month_threshold": 3,
        "run_threshold": 3,
        "transition_threshold": 2,
        "maximum_run_share_threshold": 0.8,
        "validation_accessed": False,
        "future_utility_accessed": False,
        **metrics,
    }
    return _status_receipt(
        contract_version=D4_OCCUPANCY_VERSION,
        failures=failures,
        evidence=evidence,
    )


def _candidate_status(entry: Mapping[str, Any]) -> bool:
    return (
        entry.get("fit_status") == "accepted"
        and bool(entry.get("likelihood", {}).get("valid"))
        and bool(entry.get("covariance", {}).get("valid"))
        and bool(entry.get("train_occupancy", {}).get("valid"))
    )


def select_level_restart(
    first_repeat: Mapping[str, Any],
    second_repeat: Mapping[str, Any],
    *,
    family: str,
    level: str,
    expected_sector_codes: Sequence[str],
    feature_count: int,
) -> dict[str, Any]:
    """Apply D5-01-B after D5-02 bitwise repeat equality, without reading validation evidence."""

    codes = tuple(sorted(str(value) for value in expected_sector_codes))
    expected_count = 31 if level == "L1" else 131 if level == "L2" else 0
    blockers: list[str] = []
    failures: list[str] = []
    if len(codes) != expected_count or len(set(codes)) != expected_count:
        failures.append("hmm_risk_model_restart_level_incomplete")
    for repeat in (first_repeat, second_repeat):
        if tuple(repeat.get("schedule") or ()) != RESTART_SCHEDULE:
            blockers.append("hmm_risk_model_restart_schedule_incomplete")
        if repeat.get("family") != family or repeat.get("level") != level:
            failures.append("hmm_risk_model_selection_contract_unsatisfied")
        if tuple(repeat.get("canonical_sector_codes") or ()) != codes:
            failures.append("hmm_risk_model_restart_level_incomplete")
        if len(tuple(repeat.get("feature_names") or ())) != feature_count:
            failures.append("hmm_risk_model_selection_contract_unsatisfied")
        if repeat.get("validation_accessed") is not False or repeat.get("future_utility_accessed") is not False:
            failures.append("hmm_risk_model_selection_contract_unsatisfied")
        if repeat.get("semantic_labelability_accessed") is not False or repeat.get("d6_status_accessed") is not False:
            failures.append("hmm_risk_model_selection_contract_unsatisfied")
    first_entries = list(first_repeat.get("entries") or ())
    second_entries = list(second_repeat.get("entries") or ())
    for repeat, entries in ((first_repeat, first_entries), (second_repeat, second_entries)):
        models = list(repeat.get("models") or ())
        if canonical_sha256(models) != repeat.get("model_payload_sha256"):
            failures.append("hmm_risk_model_repeat_hash_mismatch")
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
        if canonical_sha256(candidate_payload) != repeat.get("candidate_payload_sha256"):
            failures.append("hmm_risk_model_repeat_hash_mismatch")
    if canonical_sha256(first_entries) != canonical_sha256(second_entries):
        failures.append("hmm_risk_model_repeat_hash_mismatch")
    if first_repeat.get("candidate_payload_sha256") != second_repeat.get("candidate_payload_sha256"):
        failures.append("hmm_risk_model_repeat_hash_mismatch")
    if first_repeat.get("model_payload_sha256") != second_repeat.get("model_payload_sha256"):
        failures.append("hmm_risk_model_repeat_hash_mismatch")

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
        eligible = entry_codes == codes and all(_candidate_status(entry) for entry in entries)
        scores: list[float] = []
        if eligible:
            for entry in entries:
                try:
                    final = float(entry["final_train_log_likelihood"])
                    rows = int(entry["training_rows"])
                    dimensions = int(entry["feature_count"])
                    score = final / (rows * dimensions)
                except (KeyError, TypeError, ValueError, ZeroDivisionError):
                    eligible = False
                    break
                if rows <= 0 or dimensions != feature_count or not math.isfinite(score):
                    eligible = False
                    break
                scores.append(score)
        aggregate = None
        if eligible:
            ordered_scores = sorted(scores)
            aggregate = {
                "minimum": ordered_scores[0],
                "median": ordered_scores[len(ordered_scores) // 2],
                "mean": math.fsum(scores) / len(scores),
                "ordered_sector_scores": [
                    {"sector_code": code, "score": score} for code, score in zip(codes, scores, strict=True)
                ],
            }
        candidates.append(
            {
                "seed": seed,
                "schedule_index": schedule_index,
                "eligible": eligible,
                "aggregate": aggregate,
                "entry_receipt_hashes": [str(entry.get("entry_receipt_sha256") or "") for entry in entries],
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
) -> dict[str, Any]:
    """Apply D6-01-B after selection; hard argmax remains the sole semantic authority."""

    assignment_failures: list[str] = []
    assignment_blockers: list[str] = []
    evidence_failures: list[str] = []
    evidence_blockers: list[str] = []
    if dates is None:
        assignment_blockers.append("hmm_risk_semantic_evidence_missing")
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
            assignment_failures.append("hmm_risk_semantic_posterior_invalid")
            ordered_dates = ()
            probabilities = np.empty((0, 3), dtype=np.float64)
            hard = np.empty((0,), dtype=np.int64)
            metrics = {}
    if ordered_dates and (
        len(ordered_dates) != 182 or ordered_dates[0] != date(2024, 7, 1) or ordered_dates[-1] != date(2025, 3, 31)
    ):
        assignment_failures.append("hmm_risk_semantic_validation_date_sequence_invalid")
    if metrics:
        if metrics["row_sum_max_abs_error"] > 1e-12:
            assignment_failures.append("hmm_risk_semantic_posterior_normalization_failed")
        if metrics["top1_top2_min_margin"] <= 1e-12:
            assignment_failures.append("hmm_risk_semantic_posterior_tie")
    assignment_evidence = {
        "validation_rows": len(ordered_dates),
        "ordered_date_sha256": canonical_sha256([value.isoformat() for value in ordered_dates]),
        "posterior_sha256": canonical_sha256(probabilities.tolist()),
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
        evidence_blockers.append("hmm_risk_semantic_evidence_missing")
    else:
        if utility_components.get("source_cutoff") != "2025-04-30":
            evidence_blockers.append("hmm_risk_semantic_evidence_missing")
        if utility_components.get("formula_version") != "hmm_risk_hard_future_excess_035_035_030_v1":
            evidence_blockers.append("hmm_risk_semantic_evidence_missing")
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
                evidence_failures.append("hmm_risk_semantic_validation_run_concentration_failed")
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
        evidence_blockers.append("hmm_risk_semantic_assignment_not_accepted")
    semantic_evidence = {
        "utility_formula": "0.35*excess_return_5d+0.35*excess_return_10d+0.30*excess_return_20d",
        "utility_component_sha256": {
            key: canonical_sha256(value.tolist()) for key, value in sorted(components.items())
        },
        "combined_utility_sha256": canonical_sha256(combined.tolist()),
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
