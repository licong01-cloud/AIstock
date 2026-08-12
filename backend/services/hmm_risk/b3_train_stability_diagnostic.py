"""Zero-refit train-window stability evidence for the frozen P6 L2 models."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date
import math
from typing import Any

import numpy as np

from backend.services.hmm_risk.b3_acceptance import RESTART_SCHEDULE
from backend.services.hmm_risk.b3_mixed_dimension import build_projection_receipt
from backend.services.hmm_risk.b3_training import B3FittedModel, B3TrainOnlySeries
from backend.services.hmm_risk.state_model_set import (
    StateModelSetError,
    _apply_preprocess,
    canonical_sha256,
    causal_forward_posteriors,
)


CONTRACT_VERSION = "C-008-B3-TRAIN-STABILITY-DIAG-01"
REPORT_SCHEMA_VERSION = "hmm_risk_c008_b3_train_stability_diag01_v1"
TARGET_FAMILY = "autocycle_all_core"
TARGET_LEVEL = "L2"
EXPECTED_SECTOR_COUNT = 131
WINDOW_ROWS = 182
TRAILING_ROWS = WINDOW_ROWS * 2
MIN_TRAIN_ROWS = 420

ZERO_REFIT_REPORT_SHA256 = "dcf4c69ec7ba817d8d19f8cca27f6a855f25b2e7d147a5b754549d431d8c26a1"
TRAINING_AUTHORITY_RECEIPT_SHA256 = "012f5f93b0d47a8a6e084486fcb47869c7f9b489a7e038fdf764e8c6a3d7d650"
FRESH_PROCESS_RECEIPT_HASHES = (
    "8488d2e4c83fc016304ed29b5d06a1d37d0b02aea2df37e6418a4f88f5e5c40a",
    "672e3aed63cc3e7e0cf1d938af5174391dfa54c9932ac70be085005d70424fcc",
)
D5_SELECTION_RECEIPT_SHA256 = "8ec3967bb775329bcd277c440a8cfc11f1b15888777e677c4612820d34085cbc"
SELECTED_MODEL_HASHES_SHA256 = "f226650b4a85f5722bdae96b4e8dc09d0a07c8e9dce3983685a1687f38c7bb27"
TRAIN_SOURCE_IDENTITIES = {
    "dataset_manifest_hash": "75bd5d221272f7a0d5d21e26113c9af6e1fc6c7b865e81781ebfca59007ca8c6",
    "mapping_manifest_hash": "acb38f303e5b9c7447fcae8e65ea23fe58615da67da762f51f03caa862682ab9",
    "calendar_manifest_hash": "af4a60cd23a079c015b3b1bca097de42c2da9948992a188e74a6640595b2f445",
    "l2_stock_fact_manifest_hash": "6a0aa51bfa48928678665229492e5dcb3ced8a32c7edc5776af689a0fa4b4144",
    "feature_domain_policy_sha256": "7ca5ef417094e18368af556f564274b2a609cde79a02e75a1e18d81944e595d3",
}

REASON_POSTERIOR_INVALID = "hmm_risk_train_stability_posterior_invalid"
REASON_POSTERIOR_NORMALIZATION = "hmm_risk_train_stability_posterior_normalization_failed"
REASON_POSTERIOR_TIE = "hmm_risk_train_stability_posterior_tie"
REASON_COUNT = "hmm_risk_train_stability_state_count_insufficient"
REASON_OCCUPANCY = "hmm_risk_train_stability_occupancy_insufficient"
REASON_MONTH = "hmm_risk_train_stability_month_coverage_insufficient"
REASON_RUN = "hmm_risk_train_stability_run_coverage_insufficient"
REASON_TRANSITION = "hmm_risk_train_stability_transition_coverage_insufficient"
REASON_RUN_CONCENTRATION = "hmm_risk_train_stability_run_concentration_exceeded"


def _validate_authority(
    training_authority: Mapping[str, Any],
    zero_refit_report: Mapping[str, Any],
    first_models: Mapping[tuple[int, str], B3FittedModel],
    second_models: Mapping[tuple[int, str], B3FittedModel],
) -> tuple[str, ...]:
    if training_authority.get("receipt_sha256") != TRAINING_AUTHORITY_RECEIPT_SHA256:
        raise StateModelSetError("train-stability training authority receipt differs")
    if tuple(training_authority.get("fresh_process_receipt_hashes") or ()) != FRESH_PROCESS_RECEIPT_HASHES:
        raise StateModelSetError("train-stability fresh-process authority differs")
    if any(training_authority.get(key) != value for key, value in TRAIN_SOURCE_IDENTITIES.items()):
        raise StateModelSetError("train-stability source authority differs")
    selection = training_authority.get("selection")
    if not isinstance(selection, Mapping) or selection.get("receipt_sha256") != D5_SELECTION_RECEIPT_SHA256:
        raise StateModelSetError("train-stability D5 authority differs")
    if canonical_sha256(dict(zero_refit_report)) != ZERO_REFIT_REPORT_SHA256:
        raise StateModelSetError("train-stability zero-refit report authority differs")
    if (
        zero_refit_report.get("training_authority_receipt_sha256") != TRAINING_AUTHORITY_RECEIPT_SHA256
        or zero_refit_report.get("d5_selection_receipt_sha256") != D5_SELECTION_RECEIPT_SHA256
        or zero_refit_report.get("selected_model_payload_hashes_sha256") != SELECTED_MODEL_HASHES_SHA256
        or zero_refit_report.get("family") != TARGET_FAMILY
        or zero_refit_report.get("level") != TARGET_LEVEL
        or zero_refit_report.get("selected_seed") != 43
        or zero_refit_report.get("fit_performed") is not False
        or zero_refit_report.get("selection_reexecuted") is not False
        or zero_refit_report.get("ready_artifact_write_performed") is not False
    ):
        raise StateModelSetError("train-stability zero-refit lineage is invalid")

    selected_hashes = tuple(str(value) for value in zero_refit_report.get("selected_model_payload_hashes") or ())
    if len(selected_hashes) != EXPECTED_SECTOR_COUNT or canonical_sha256(list(selected_hashes)) != (
        SELECTED_MODEL_HASHES_SHA256
    ):
        raise StateModelSetError("train-stability selected model hash closure is invalid")
    keys = set(first_models)
    expected_codes = tuple(sorted({code for _, code in keys}))
    expected_keys = {(seed, code) for seed in RESTART_SCHEDULE for code in expected_codes}
    if len(expected_codes) != EXPECTED_SECTOR_COUNT or keys != expected_keys or set(second_models) != expected_keys:
        raise StateModelSetError("train-stability frozen 8x131 model closure is invalid")
    for key in sorted(expected_keys):
        first = first_models[key]
        second = second_models[key]
        if first.model_payload_sha256 != second.model_payload_sha256:
            raise StateModelSetError(f"train-stability fresh-process model differs: seed={key[0]} sector={key[1]}")
    if tuple(first_models[(43, code)].model_payload_sha256 for code in expected_codes) != selected_hashes:
        raise StateModelSetError("train-stability selected model ordering differs from zero-refit authority")
    return expected_codes


def _source_closure(
    item: B3TrainOnlySeries,
    models: Sequence[B3FittedModel],
) -> tuple[np.ndarray, dict[str, Any]]:
    item.validate(len(models[0].feature_names))
    if len(item.train_dates) < MIN_TRAIN_ROWS:
        raise StateModelSetError(
            f"train-stability requires at least {MIN_TRAIN_ROWS} observations: sector={item.sector_code}"
        )
    expected_preprocess = dict(models[0].preprocess)
    expected_projection = dict(models[0].projection_receipt or {})
    if not expected_projection:
        raise StateModelSetError(f"train-stability projection receipt is missing: sector={item.sector_code}")
    for model in models:
        if (
            model.family != TARGET_FAMILY
            or model.level != TARGET_LEVEL
            or model.sector_code != item.sector_code
            or model.observation_manifest_hash != item.observation_manifest_hash
            or dict(model.preprocess) != expected_preprocess
            or dict(model.projection_receipt or {}) != expected_projection
        ):
            raise StateModelSetError(f"train-stability frozen source identity drifted: sector={item.sector_code}")

    full = _apply_preprocess(item.train_observations, expected_preprocess)
    recomputed_projection, projected = build_projection_receipt(
        family=TARGET_FAMILY,
        level=TARGET_LEVEL,
        sector_code=item.sector_code,
        full_feature_names=models[0].feature_names,
        preprocess=expected_preprocess,
        raw_observations=item.train_observations,
        preprocessed_observations=full,
        train_input_manifest=item.train_input_manifest,
    )
    if recomputed_projection != expected_projection:
        raise StateModelSetError(f"train-stability projected train matrix drifted: sector={item.sector_code}")
    dates = [value.isoformat() for value in item.train_dates]
    comparison = {
        "sector_code": item.sector_code,
        "row_count": len(dates),
        "train_dates_sha256": canonical_sha256(dates),
        "observation_manifest_hash": item.observation_manifest_hash,
        "train_input_manifest_sha256": canonical_sha256(dict(item.train_input_manifest)),
        "preprocess_sha256": canonical_sha256(expected_preprocess),
        "projection_sha256": recomputed_projection["projection_sha256"],
        "projected_matrix_sha256": recomputed_projection["projected_matrix_sha256"],
        "projected_matrix_shape": recomputed_projection["projected_matrix_shape"],
        "frozen_hashes_matched": True,
    }
    return projected, {**comparison, "comparison_sha256": canonical_sha256(comparison)}


def _window_evidence(
    observations: np.ndarray,
    dates: Sequence[date],
    calendar_positions: Mapping[date, int],
    model: B3FittedModel,
    *,
    window: str,
) -> dict[str, Any]:
    values = np.asarray(observations, dtype=np.float64)
    day_values = tuple(dates)
    if values.shape[0] != WINDOW_ROWS or len(day_values) != WINDOW_ROWS:
        raise StateModelSetError(f"train-stability {window} window must contain exactly {WINDOW_ROWS} rows")
    try:
        positions = tuple(calendar_positions[value] for value in day_values)
    except KeyError as exc:
        raise StateModelSetError(f"train-stability date is absent from frozen calendar: {exc.args[0]}") from exc
    if any(right <= left for left, right in zip(positions, positions[1:], strict=False)):
        raise StateModelSetError("train-stability calendar positions must be strictly increasing")

    try:
        posterior = causal_forward_posteriors(
            values,
            startprob=model.startprob,
            transmat=model.transmat,
            means=model.means,
            covars=model.covars,
        )
    except StateModelSetError as exc:
        if "posterior normalization failed" not in str(exc):
            raise
        body = {
            "window": window,
            "row_count": WINDOW_ROWS,
            "date_start": day_values[0].isoformat(),
            "date_end": day_values[-1].isoformat(),
            "date_sha256": canonical_sha256([value.isoformat() for value in day_values]),
            "calendar_position_sha256": canonical_sha256(list(positions)),
            "calendar_gap_count": sum(right != left + 1 for left, right in zip(positions, positions[1:], strict=False)),
            "observation_window_sha256": canonical_sha256(values.tolist()),
            "posterior_finite": False,
            "posterior_nonnegative": False,
            "posterior_row_sum_max_abs_error": None,
            "top1_top2_min_margin": None,
            "hard_assignment_sha256": None,
            "transition_counts": None,
            "transition_counts_sha256": None,
            "states": None,
            "status": "train_window_structurally_unobserved",
            "reason_codes": [REASON_POSTERIOR_NORMALIZATION],
            "diagnostic_only": True,
            "formal_d5_gate_applied": False,
        }
        return {**body, "window_evidence_sha256": canonical_sha256(body)}
    finite = bool(np.isfinite(posterior).all())
    nonnegative = bool(np.all(posterior >= 0.0))
    row_sum_error = float(np.max(np.abs(posterior.sum(axis=1) - 1.0))) if posterior.size else math.inf
    ordered = np.sort(posterior, axis=1)
    minimum_margin = float(np.min(ordered[:, -1] - ordered[:, -2])) if posterior.size else -math.inf
    hard = posterior.argmax(axis=1).astype(np.int64)
    transitions = np.zeros((3, 3), dtype=np.int64)
    runs: dict[int, list[int]] = {state: [] for state in range(3)}
    current = int(hard[0])
    run_length = 1
    for index in range(1, len(hard)):
        state = int(hard[index])
        adjacent = positions[index] == positions[index - 1] + 1
        if adjacent:
            transitions[int(hard[index - 1]), state] += 1
        if adjacent and state == current:
            run_length += 1
        else:
            runs[current].append(run_length)
            current, run_length = state, 1
    runs[current].append(run_length)

    reasons: list[str] = []
    if not finite or not nonnegative:
        reasons.append(REASON_POSTERIOR_INVALID)
    if not math.isfinite(row_sum_error) or row_sum_error > 1e-12:
        reasons.append(REASON_POSTERIOR_NORMALIZATION)
    if not math.isfinite(minimum_margin) or minimum_margin <= 1e-12:
        reasons.append(REASON_POSTERIOR_TIE)
    count_threshold = max(5, math.ceil(0.02 * WINDOW_ROWS))
    states: dict[str, Any] = {}
    for state in range(3):
        mask = hard == state
        count = int(mask.sum())
        lengths = runs[state]
        months = sorted({day_values[index].strftime("%Y-%m") for index in np.flatnonzero(mask)})
        maximum_share = max(lengths) / count if count and lengths else None
        incoming = int(transitions[:, state].sum() - transitions[state, state])
        outgoing = int(transitions[state, :].sum() - transitions[state, state])
        state_reasons: list[str] = []
        if count < count_threshold:
            state_reasons.append(REASON_COUNT)
        if count / WINDOW_ROWS < 0.02:
            state_reasons.append(REASON_OCCUPANCY)
        if len(months) < 2:
            state_reasons.append(REASON_MONTH)
        if len(lengths) < 2:
            state_reasons.append(REASON_RUN)
        if incoming < 2 or outgoing < 2:
            state_reasons.append(REASON_TRANSITION)
        if maximum_share is None or maximum_share > 0.9:
            state_reasons.append(REASON_RUN_CONCENTRATION)
        reasons.extend(state_reasons)
        states[str(state)] = {
            "hard_count": count,
            "normalized_occupancy": count / WINDOW_ROWS,
            "calendar_month_count": len(months),
            "contiguous_run_count": len(lengths),
            "incoming_transition_count": incoming,
            "outgoing_transition_count": outgoing,
            "maximum_single_run_share": maximum_share,
            "reason_codes": state_reasons,
        }
    reason_codes = sorted(set(reasons))
    body = {
        "window": window,
        "row_count": WINDOW_ROWS,
        "date_start": day_values[0].isoformat(),
        "date_end": day_values[-1].isoformat(),
        "date_sha256": canonical_sha256([value.isoformat() for value in day_values]),
        "calendar_position_sha256": canonical_sha256(list(positions)),
        "calendar_gap_count": sum(right != left + 1 for left, right in zip(positions, positions[1:], strict=False)),
        "observation_window_sha256": canonical_sha256(values.tolist()),
        "posterior_finite": finite,
        "posterior_nonnegative": nonnegative,
        "posterior_row_sum_max_abs_error": row_sum_error,
        "top1_top2_min_margin": minimum_margin,
        "hard_assignment_sha256": canonical_sha256(hard.tolist()),
        "transition_counts": transitions.tolist(),
        "transition_counts_sha256": canonical_sha256(transitions.tolist()),
        "states": states,
        "status": (
            "train_window_structurally_observed" if not reason_codes else "train_window_structurally_unobserved"
        ),
        "reason_codes": reason_codes,
        "diagnostic_only": True,
        "formal_d5_gate_applied": False,
    }
    return {**body, "window_evidence_sha256": canonical_sha256(body)}


def evaluate_window(
    observations: np.ndarray,
    dates: Sequence[date],
    trading_dates: Sequence[date],
    model: B3FittedModel,
    *,
    window: str,
) -> dict[str, Any]:
    """Public testable window boundary; each call resets filtering from fitted startprob."""

    calendar = tuple(trading_dates)
    if tuple(sorted(calendar)) != calendar or len(set(calendar)) != len(calendar):
        raise StateModelSetError("train-stability frozen calendar is not strictly increasing")
    return _window_evidence(
        observations,
        dates,
        {value: index for index, value in enumerate(calendar)},
        model,
        window=window,
    )


def _blocked_sector_codes(zero_refit_report: Mapping[str, Any]) -> dict[str, list[str]]:
    artifact = zero_refit_report.get("selected_artifact")
    if not isinstance(artifact, Mapping):
        raise StateModelSetError("train-stability selected D6 artifact is missing")
    output: dict[str, list[str]] = {}
    entries = list(artifact.get("entries") or ())
    if len(entries) != EXPECTED_SECTOR_COUNT:
        raise StateModelSetError("train-stability selected D6 artifact sector closure is invalid")
    for entry in entries:
        code = str(entry.get("sector_code") or "")
        semantic = entry.get("semantic")
        evidence = semantic.get("semantic_evidence") if isinstance(semantic, Mapping) else None
        if not isinstance(evidence, Mapping):
            raise StateModelSetError(f"train-stability D6 evidence is missing: sector={code}")
        if evidence.get("semantic_evidence_valid") is False:
            output[code] = sorted(str(value) for value in evidence.get("failure_reason_codes") or ())
    if len(output) != 11:
        raise StateModelSetError(f"train-stability requires exactly 11 frozen D6 blockers; actual={len(output)}")
    return output


def build_report(
    *,
    training_authority: Mapping[str, Any],
    zero_refit_report: Mapping[str, Any],
    first_models: Mapping[tuple[int, str], B3FittedModel],
    second_models: Mapping[tuple[int, str], B3FittedModel],
    series: Mapping[str, B3TrainOnlySeries],
    trading_dates: Sequence[date],
    diagnostic_producer_commit: str,
) -> dict[str, Any]:
    """Build the compact 1048-profile report without fit, selection, D6, or model writes."""

    expected_codes = _validate_authority(training_authority, zero_refit_report, first_models, second_models)
    blockers = _blocked_sector_codes(zero_refit_report)
    calendar = tuple(trading_dates)
    if tuple(sorted(calendar)) != calendar or len(set(calendar)) != len(calendar):
        raise StateModelSetError("train-stability frozen calendar is not strictly increasing")
    positions = {value: index for index, value in enumerate(calendar)}
    if tuple(sorted(series)) != expected_codes:
        raise StateModelSetError("train-stability rebuilt L2 sector closure differs")

    source_comparisons: list[dict[str, Any]] = []
    projected_by_code: dict[str, np.ndarray] = {}
    for code in expected_codes:
        models = [first_models[(seed, code)] for seed in RESTART_SCHEDULE]
        projected, comparison = _source_closure(series[code], models)
        projected_by_code[code] = projected
        source_comparisons.append(comparison)

    profiles: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    seed_complete: dict[int, bool] = {seed: True for seed in RESTART_SCHEDULE}
    blocker_summary: list[dict[str, Any]] = []
    comparison_by_code = {value["sector_code"]: value for value in source_comparisons}
    for code in expected_codes:
        item = series[code]
        projected = projected_by_code[code]
        n_rows = projected.shape[0]
        early_slice = slice(n_rows - TRAILING_ROWS, n_rows - WINDOW_ROWS)
        late_slice = slice(n_rows - WINDOW_ROWS, n_rows)
        for seed in RESTART_SCHEDULE:
            model = first_models[(seed, code)]
            early = _window_evidence(
                projected[early_slice],
                item.train_dates[early_slice],
                positions,
                model,
                window="early",
            )
            late = _window_evidence(
                projected[late_slice],
                item.train_dates[late_slice],
                positions,
                model,
                window="late",
            )
            observed = all(value["status"] == "train_window_structurally_observed" for value in (early, late))
            seed_complete[seed] = seed_complete[seed] and observed
            reason_counts.update(early["reason_codes"])
            reason_counts.update(late["reason_codes"])
            profile_body = {
                "family": TARGET_FAMILY,
                "level": TARGET_LEVEL,
                "seed": seed,
                "sector_code": code,
                "model_payload_sha256": model.model_payload_sha256,
                "source_comparison_sha256": comparison_by_code[code]["comparison_sha256"],
                "early": early,
                "late": late,
                "both_windows_structurally_observed": observed,
            }
            profiles.append({**profile_body, "profile_sha256": canonical_sha256(profile_body)})
            if seed == 43 and code in blockers:
                blocker_summary.append(
                    {
                        "sector_code": code,
                        "d6_reason_codes": blockers[code],
                        "early_status": early["status"],
                        "late_status": late["status"],
                        "classification": (
                            "validation_only_structure_collapse" if observed else "train_structure_instability_observed"
                        ),
                    }
                )

    if len(profiles) != EXPECTED_SECTOR_COUNT * len(RESTART_SCHEDULE) or len(blocker_summary) != 11:
        raise StateModelSetError("train-stability profile or D6 blocker closure is incomplete")
    per_seed = [
        {
            "seed": seed,
            "profile_count": EXPECTED_SECTOR_COUNT,
            "all_131_both_windows_structurally_observed": seed_complete[seed],
        }
        for seed in RESTART_SCHEDULE
    ]
    body = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "status": "diagnostic_complete",
        "diagnostic_producer_commit": diagnostic_producer_commit,
        "authority": _authority_summary(training_authority),
        "source_comparisons": source_comparisons,
        "source_comparison_count": len(source_comparisons),
        "profile_count": len(profiles),
        "profiles": profiles,
        "per_seed": per_seed,
        "complete_seed_count": sum(value["all_131_both_windows_structurally_observed"] for value in per_seed),
        "d6_blocker_summary": blocker_summary,
        "d6_blocker_count": len(blocker_summary),
        "reason_counts": dict(sorted(reason_counts.items())),
        "diagnostic_only": True,
        "formal_d5_gate_applied": False,
        **_no_action_flags(),
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _authority_summary(training_authority: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "zero_refit_report_sha256": ZERO_REFIT_REPORT_SHA256,
        "training_authority_receipt_sha256": TRAINING_AUTHORITY_RECEIPT_SHA256,
        "fresh_process_receipt_hashes": list(FRESH_PROCESS_RECEIPT_HASHES),
        "d5_selection_receipt_sha256": D5_SELECTION_RECEIPT_SHA256,
        "selected_model_payload_hashes_sha256": SELECTED_MODEL_HASHES_SHA256,
        **{key: training_authority.get(key) for key in TRAIN_SOURCE_IDENTITIES},
        "family": TARGET_FAMILY,
        "level": TARGET_LEVEL,
        "schedule": list(RESTART_SCHEDULE),
    }


def _expected_authority() -> dict[str, Any]:
    return {
        "zero_refit_report_sha256": ZERO_REFIT_REPORT_SHA256,
        "training_authority_receipt_sha256": TRAINING_AUTHORITY_RECEIPT_SHA256,
        "fresh_process_receipt_hashes": list(FRESH_PROCESS_RECEIPT_HASHES),
        "d5_selection_receipt_sha256": D5_SELECTION_RECEIPT_SHA256,
        "selected_model_payload_hashes_sha256": SELECTED_MODEL_HASHES_SHA256,
        **TRAIN_SOURCE_IDENTITIES,
        "family": TARGET_FAMILY,
        "level": TARGET_LEVEL,
        "schedule": list(RESTART_SCHEDULE),
    }


def _no_action_flags() -> dict[str, bool | int]:
    return {
        "fit_performed": False,
        "refit_count": 0,
        "selection_performed": False,
        "d6_executed": False,
        "formal_acceptance_thresholds_applied": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }


def source_drift_report(*, error: Exception, diagnostic_producer_commit: str) -> dict[str, Any]:
    body = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "status": "insufficient_evidence",
        "primary_reason_code": "hmm_risk_train_stability_source_drift",
        "error_type": type(error).__name__,
        "error": str(error),
        "diagnostic_producer_commit": diagnostic_producer_commit,
        "authority": _expected_authority(),
        "source_comparisons": [],
        "profile_count": 0,
        "profiles": [],
        **_no_action_flags(),
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _reject_embedded_bulk_values(value: Any, *, path: str = "report") -> None:
    forbidden_keys = {
        "observations",
        "posteriors",
        "hard_assignments",
        "training_matrix",
        "startprob",
        "transmat",
        "means",
        "covars",
    }
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key in forbidden_keys:
                raise StateModelSetError(f"train-stability compact report embeds forbidden field: {path}.{key}")
            _reject_embedded_bulk_values(child, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _reject_embedded_bulk_values(child, path=f"{path}[{index}]")


def validate_report(report: Mapping[str, Any]) -> None:
    _reject_embedded_bulk_values(report)
    body = {key: value for key, value in report.items() if key != "receipt_sha256"}
    if (
        report.get("schema_version") != REPORT_SCHEMA_VERSION
        or report.get("contract_version") != CONTRACT_VERSION
        or report.get("receipt_sha256") != canonical_sha256(body)
        or report.get("authority") != _expected_authority()
        or len(str(report.get("diagnostic_producer_commit") or "")) != 40
        or any(
            character not in "0123456789abcdef"
            for character in str(report.get("diagnostic_producer_commit") or "").lower()
        )
        or any(report.get(key) != value for key, value in _no_action_flags().items())
    ):
        raise StateModelSetError("train-stability report contract is invalid")
    if report.get("status") == "diagnostic_complete":
        if (
            report.get("profile_count") != EXPECTED_SECTOR_COUNT * len(RESTART_SCHEDULE)
            or len(report.get("profiles") or ()) != EXPECTED_SECTOR_COUNT * len(RESTART_SCHEDULE)
            or report.get("source_comparison_count") != EXPECTED_SECTOR_COUNT
            or report.get("d6_blocker_count") != 11
        ):
            raise StateModelSetError("train-stability complete report closure is invalid")
        identities = set()
        for profile in report["profiles"]:
            profile_body = {key: value for key, value in profile.items() if key != "profile_sha256"}
            identity = (profile.get("seed"), profile.get("sector_code"))
            windows = (profile.get("early"), profile.get("late"))
            if (
                profile.get("profile_sha256") != canonical_sha256(profile_body)
                or identity in identities
                or identity[0] not in RESTART_SCHEDULE
                or not identity[1]
                or any(not isinstance(window, Mapping) for window in windows)
            ):
                raise StateModelSetError("train-stability profile hash or identity is invalid")
            for expected_window, window in zip(("early", "late"), windows, strict=True):
                window_body = {key: value for key, value in window.items() if key != "window_evidence_sha256"}
                if (
                    window.get("window") != expected_window
                    or window.get("row_count") != WINDOW_ROWS
                    or window.get("window_evidence_sha256") != canonical_sha256(window_body)
                    or window.get("diagnostic_only") is not True
                    or window.get("formal_d5_gate_applied") is not False
                    or window.get("status")
                    not in {"train_window_structurally_observed", "train_window_structurally_unobserved"}
                ):
                    raise StateModelSetError("train-stability window evidence is invalid")
            identities.add(identity)
        expected_identities = {(seed, code) for seed in RESTART_SCHEDULE for code in {value[1] for value in identities}}
        if len({value[1] for value in identities}) != EXPECTED_SECTOR_COUNT or identities != expected_identities:
            raise StateModelSetError("train-stability complete profile identity grid is invalid")
        comparisons = list(report.get("source_comparisons") or ())
        comparison_by_code = {}
        for comparison in comparisons:
            comparison_body = {key: value for key, value in comparison.items() if key != "comparison_sha256"}
            code = str(comparison.get("sector_code") or "")
            if (
                comparison.get("comparison_sha256") != canonical_sha256(comparison_body)
                or comparison.get("frozen_hashes_matched") is not True
                or not code
                or code in comparison_by_code
            ):
                raise StateModelSetError("train-stability source comparison is invalid")
            comparison_by_code[code] = comparison
        if set(comparison_by_code) != {value[1] for value in identities} or any(
            profile.get("source_comparison_sha256")
            != comparison_by_code[str(profile.get("sector_code"))]["comparison_sha256"]
            for profile in report["profiles"]
        ):
            raise StateModelSetError("train-stability profile/source comparison closure is invalid")
        per_seed = list(report.get("per_seed") or ())
        if (
            [value.get("seed") for value in per_seed] != list(RESTART_SCHEDULE)
            or any(value.get("profile_count") != EXPECTED_SECTOR_COUNT for value in per_seed)
            or report.get("complete_seed_count")
            != sum(value.get("all_131_both_windows_structurally_observed") is True for value in per_seed)
        ):
            raise StateModelSetError("train-stability per-seed aggregate is invalid")
    elif report.get("status") == "insufficient_evidence":
        if (
            report.get("primary_reason_code") != "hmm_risk_train_stability_source_drift"
            or report.get("profile_count") != 0
            or report.get("profiles") != []
        ):
            raise StateModelSetError("train-stability source-drift report evaluated profiles")
    else:
        raise StateModelSetError("train-stability report status is invalid")
