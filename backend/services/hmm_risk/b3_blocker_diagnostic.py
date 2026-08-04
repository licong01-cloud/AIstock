from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from backend.services.hmm_risk.b3_acceptance import (
    D4_COVARIANCE_VERSION,
    D4_LIKELIHOOD_VERSION,
    D4_OCCUPANCY_VERSION,
    D6_SEMANTIC_VERSION,
    L2_RETRAIN_VERSION,
    RESTART_SCHEDULE,
    evaluate_semantic_validation,
)
from backend.services.hmm_risk.b3_training import (
    B3FittedModel,
    B3TrainingStageError,
    B3TrainOnlySeries,
    fit_b3_target_entry,
    formal_b3_parameter_profile,
)
from backend.services.hmm_risk.state_model_set import (
    L1TrainingSeries,
    StateModelSetError,
    _apply_preprocess,
    _fit_preprocess,
    canonical_sha256,
    causal_forward_posteriors,
    c008_b3_diag04_fixed_numeric_environment,
)


DIAGNOSTIC_VERSION = "hmm_risk_c008_b3_formal_blocker_diag01_v1"
TARGET_VERSION = "hmm_risk_c008_b3_formal_blocker_diag01_target_v1"
AFFECTED_LEVELS = (
    "autocycle_all_core:L1",
    "autocycle_all_core:L2",
    "legacy_covfix:L2",
)
EXPECTED_REJECTED_COUNTS = {
    "autocycle_all_core:L1": 9,
    "autocycle_all_core:L2": 74,
    "legacy_covfix:L2": 67,
}
FORMAL_AUTHORITY = {
    "producer_commit": "e2c01bae156281d551b084156fec4a09ed5a84ee",
    "report_sha256": "e7992f87fb555eb26d6c2ef1ad9d45863954edd83fbfcc39f5ae01765cf3939f",
    "receipt_sha256": "684b20471f54f17ada374b824b8d0703a770dcf9be9699cf9d15c46598f80362",
    "dataset_manifest_hash": "6afa5d35b350d3c58704e1da6308d3fff7f4e0fa06a9fe3050464026471665f3",
    "mapping_manifest_hash": "2bc1c87a328758dc690e712ea2395972d0eb28f27412e0fc24633e8b04853560",
    "calendar_manifest_hash": "af4a60cd23a079c015b3b1bca097de42c2da9948992a188e74a6640595b2f445",
    "l2_stock_fact_manifest_hash": "1a7f50f6d6782bfe36ff3638f8e0ddf06fbdb83328cb5cf126f5f1bdc66ef320",
    "semantic_dataset_manifest_hash": "5aa3778be68f081065c31e648c5781da68119e95b3ed1d9585769fb91de613dc",
    "semantic_mapping_manifest_hash": "b80d1ee0c7628176c85053eeedd8a648c6384cdf7981a2bd54a22ea87e0fc864",
    "semantic_calendar_manifest_hash": "f26f1a74aa80e42eddb0fa4de1f978dd605279f2340032b738704e4bccdeec08",
    "semantic_l2_stock_fact_manifest_hash": "a454985b3bab2692a09e5ca27b2d2552f4543baa81415f5672567f78f39f6a84",
    "feature_domain_policy_sha256": "ae8eda5bba1992965bcc8e17be6db1c6d9019d87417d7632d2b33a7728c220d9",
    "formula_version": "hmm_risk_l1_sector_factor_formula_v2_c010",
}


def _hex_sha(value: Any, label: str) -> str:
    result = str(value or "").lower()
    if len(result) != 64 or any(character not in "0123456789abcdef" for character in result):
        raise StateModelSetError(f"{label} is not a canonical SHA-256 identity")
    return result


def _canonical_receipt(value: Mapping[str, Any], field: str, label: str) -> None:
    expected = _hex_sha(value.get(field), f"{label}.{field}")
    body = {key: item for key, item in value.items() if key != field}
    if canonical_sha256(body) != expected:
        raise StateModelSetError(f"{label} canonical receipt hash mismatch")


def validate_formal_report(
    report: Mapping[str, Any],
    *,
    authority: Mapping[str, str] = FORMAL_AUTHORITY,
) -> None:
    if report.get("schema_version") != "hmm_risk_b3_repeated_preparation_receipt_v1":
        raise StateModelSetError("formal blocker diagnostic report schema is invalid")
    if canonical_sha256(dict(report)) != authority["report_sha256"]:
        raise StateModelSetError("formal blocker diagnostic report authority hash mismatch")
    _canonical_receipt(report, "receipt_sha256", "formal B3 report")
    for field in (
        "producer_commit",
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "calendar_manifest_hash",
        "l2_stock_fact_manifest_hash",
        "semantic_dataset_manifest_hash",
        "semantic_mapping_manifest_hash",
        "semantic_calendar_manifest_hash",
        "semantic_l2_stock_fact_manifest_hash",
        "feature_domain_policy_sha256",
        "formula_version",
    ):
        if str(report.get(field) or "") != authority[field]:
            raise StateModelSetError(f"formal blocker diagnostic {field} mismatch")
    expected_flags = {
        "status": "blocked",
        "selection_performed": True,
        "selection_used_validation": False,
        "selection_used_future_utility": False,
        "selection_followed_by_refit": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    for field, expected in expected_flags.items():
        if report.get(field) != expected:
            raise StateModelSetError(f"formal blocker diagnostic {field} is not the approved blocked boundary")
    if report.get("family_model_set_statuses") != {
        "autocycle_all_core": "blocked",
        "legacy_covfix": "blocked",
    }:
        raise StateModelSetError("formal blocker diagnostic family status closure is invalid")


def _validate_candidate(
    candidate: Mapping[str, Any],
    *,
    canonical_codes: Sequence[str],
    expected_seed: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    if candidate.get("seed") != expected_seed or candidate.get("schedule_index") != RESTART_SCHEDULE.index(
        expected_seed
    ):
        raise StateModelSetError("formal candidate restart identity is invalid")
    hashes = list(candidate.get("entry_receipt_hashes") or ())
    if len(hashes) != len(canonical_codes) or any(len(_hex_sha(value, "entry receipt")) != 64 for value in hashes):
        raise StateModelSetError("formal candidate entry receipt closure is incomplete")
    rejection = list(candidate.get("rejection_summary") or ())
    if candidate.get("eligible") is not False or not rejection:
        raise StateModelSetError("formal affected candidate must be ineligible with a rejection summary")
    if candidate.get("rejection_summary_sha256") != canonical_sha256(rejection):
        raise StateModelSetError("formal candidate rejection summary hash mismatch")
    seen: set[str] = set()
    for row in rejection:
        code = str(row.get("sector_code") or "")
        if code not in canonical_codes or code in seen:
            raise StateModelSetError("formal candidate rejection sector identity is invalid or duplicated")
        seen.add(code)
        if row.get("entry_receipt_sha256") != hashes[canonical_codes.index(code)]:
            raise StateModelSetError("formal candidate rejection entry hash differs from the level receipt")
        stages = list(row.get("failed_stages") or ())
        if not stages or any(not stage.get("stage") or stage.get("valid") is not False for stage in stages):
            raise StateModelSetError("formal candidate rejection stage closure is incomplete")
    return rejection, hashes


def _derive_d6_targets(report: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str]:
    artifacts = report.get("selected_artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != {"legacy_covfix:L1"}:
        raise StateModelSetError("formal selected artifact closure differs from the approved blocked outcome")
    artifact = artifacts["legacy_covfix:L1"]
    _canonical_receipt(artifact, "artifact_sha256", "legacy L1 selected artifact")
    if artifact.get("selected_seed") != 43 or artifact.get("entry_count") != 31:
        raise StateModelSetError("formal selected legacy L1 identity is invalid")
    entries = list(artifact.get("entries") or ())
    if len(entries) != 31:
        raise StateModelSetError("formal selected legacy L1 entries are incomplete")
    statuses: dict[str, str] = {}
    hashes: dict[str, str] = {}
    parameter_hashes: set[str] = set()
    codes: list[str] = []
    for entry in entries:
        _canonical_receipt(entry, "selected_entry_sha256", "selected legacy L1 entry")
        code = str(entry.get("sector_code") or "")
        if not code or code in statuses or entry.get("seed") != 43:
            raise StateModelSetError("formal selected legacy L1 entry identity is invalid")
        semantic = entry.get("semantic")
        if not isinstance(semantic, Mapping) or not isinstance(semantic.get("semantic_evidence"), Mapping):
            raise StateModelSetError("formal selected legacy L1 semantic evidence is missing")
        statuses[code] = str(semantic["semantic_evidence"].get("semantic_evidence_status") or "")
        hashes[code] = str(entry["selected_entry_sha256"])
        parameter_hashes.add(_hex_sha(entry.get("parameter_profile_sha256"), "parameter profile"))
        codes.append(code)
    if codes != sorted(codes) or len(parameter_hashes) != 1:
        raise StateModelSetError("formal selected legacy L1 order/profile identity is invalid")
    failed = [code for code in codes if statuses[code] == "failed"]
    if failed != ["801980.SI"] or any(status not in {"accepted", "failed"} for status in statuses.values()):
        raise StateModelSetError("formal selected legacy L1 D6 failure closure differs from authority")
    index = codes.index(failed[0])
    before = next(
        codes[(index - offset) % len(codes)]
        for offset in range(1, len(codes))
        if statuses[codes[(index - offset) % len(codes)]] == "accepted"
    )
    after = next(
        codes[(index + offset) % len(codes)]
        for offset in range(1, len(codes))
        if statuses[codes[(index + offset) % len(codes)]] == "accepted"
    )
    if (before, after) != ("801970.SI", "801010.SI"):
        raise StateModelSetError("formal selected legacy L1 deterministic controls differ from approved design")
    targets = [
        {
            "role": "rejected",
            "family": "legacy_covfix",
            "level": "L1",
            "seed": 43,
            "sector_code": failed[0],
            "selected_entry_sha256": hashes[failed[0]],
        },
        {
            "role": "control_before",
            "family": "legacy_covfix",
            "level": "L1",
            "seed": 43,
            "sector_code": before,
            "selected_entry_sha256": hashes[before],
        },
        {
            "role": "control_after",
            "family": "legacy_covfix",
            "level": "L1",
            "seed": 43,
            "sector_code": after,
            "selected_entry_sha256": hashes[after],
        },
    ]
    return targets, next(iter(parameter_hashes))


def derive_target_manifest(
    report: Mapping[str, Any],
    *,
    authority: Mapping[str, str] = FORMAL_AUTHORITY,
    expected_rejected_counts: Mapping[str, int] = EXPECTED_REJECTED_COUNTS,
) -> dict[str, Any]:
    validate_formal_report(report, authority=authority)
    selections = report.get("selections")
    if not isinstance(selections, Mapping) or set(selections) != {
        "autocycle_all_core:L1",
        "autocycle_all_core:L2",
        "legacy_covfix:L1",
        "legacy_covfix:L2",
    }:
        raise StateModelSetError("formal blocker diagnostic selection closure is incomplete")
    targets: list[dict[str, Any]] = []
    for key in AFFECTED_LEVELS:
        selection = selections[key]
        _canonical_receipt(selection, "receipt_sha256", f"{key} selection")
        evidence = selection.get("evidence")
        if not isinstance(evidence, Mapping):
            raise StateModelSetError(f"{key} selection evidence is missing")
        family, level = key.split(":", 1)
        codes = [str(value) for value in evidence.get("canonical_sector_codes") or ()]
        expected_count = 31 if level == "L1" else 131
        if len(codes) != expected_count or codes != sorted(set(codes)):
            raise StateModelSetError(f"{key} canonical sector closure is invalid")
        candidates = list(evidence.get("candidates") or ())
        if len(candidates) != len(RESTART_SCHEDULE):
            raise StateModelSetError(f"{key} restart candidate closure is invalid")
        rejected_count = 0
        for seed, candidate in zip(RESTART_SCHEDULE, candidates, strict=True):
            rejection, hashes = _validate_candidate(candidate, canonical_codes=codes, expected_seed=seed)
            bad_codes = {str(row["sector_code"]) for row in rejection}
            for row in rejection:
                code = str(row["sector_code"])
                targets.append(
                    {
                        "role": "rejected",
                        "family": family,
                        "level": level,
                        "seed": seed,
                        "sector_code": code,
                        "source_entry_receipt_sha256": row["entry_receipt_sha256"],
                        "formal_failed_stages": row["failed_stages"],
                    }
                )
                rejected_count += 1
            control = next((code for code in codes if code not in bad_codes), None)
            if control is None:
                raise StateModelSetError(f"{key} seed {seed} has no deterministic accepted control")
            targets.append(
                {
                    "role": "control",
                    "family": family,
                    "level": level,
                    "seed": seed,
                    "sector_code": control,
                    "source_entry_receipt_sha256": hashes[codes.index(control)],
                    "formal_failed_stages": [],
                }
            )
        if rejected_count != expected_rejected_counts[key]:
            raise StateModelSetError(f"{key} rejected pair count differs from the approved diagnostic design")
    identities = [(row["family"], row["level"], row["seed"], row["sector_code"]) for row in targets]
    if len(targets) != 174 or len(set(identities)) != 174:
        raise StateModelSetError("formal blocker diagnostic target set must contain 174 unique pair identities")
    rejected = sum(row["role"] == "rejected" for row in targets)
    controls = sum(row["role"] == "control" for row in targets)
    if rejected != 150 or controls != 24:
        raise StateModelSetError("formal blocker diagnostic rejected/control closure is invalid")
    d6_targets, parameter_profile_sha256 = _derive_d6_targets(report)
    body = {
        "schema_version": TARGET_VERSION,
        "formal_report_sha256": authority["report_sha256"],
        "formal_receipt_sha256": authority["receipt_sha256"],
        "formal_producer_commit": authority["producer_commit"],
        "parameter_profile_sha256": parameter_profile_sha256,
        "targets": targets,
        "target_pair_count": len(targets),
        "rejected_pair_count": rejected,
        "control_pair_count": controls,
        "fits_per_process": len(targets),
        "fresh_process_count": 2,
        "total_fit_budget": len(targets) * 2,
        "d6_replay_targets": d6_targets,
        "d6_replay_count": len(d6_targets),
        "selection_performed": False,
        "validation_accessed_for_d4": False,
        "future_utility_accessed_for_d4": False,
    }
    return {**body, "target_manifest_sha256": canonical_sha256(body)}


def _hard_sequence_detail(posteriors: np.ndarray, dates: Sequence[Any]) -> dict[str, Any]:
    probabilities = np.asarray(posteriors, dtype=np.float64)
    if probabilities.shape != (len(dates), 3) or not np.isfinite(probabilities).all() or np.any(probabilities < 0.0):
        raise StateModelSetError("blocker diagnostic hard sequence posterior is invalid")
    hard = probabilities.argmax(axis=1).astype(np.int64)
    transitions = np.zeros((3, 3), dtype=np.int64)
    if hard.size > 1:
        np.add.at(transitions, (hard[:-1], hard[1:]), 1)
    runs: dict[int, list[int]] = {state: [] for state in range(3)}
    current, length = int(hard[0]), 1
    for raw in hard[1:]:
        state = int(raw)
        if state == current:
            length += 1
        else:
            runs[current].append(length)
            current, length = state, 1
    runs[current].append(length)
    return {
        "hard_assignment_sha256": canonical_sha256(hard.tolist()),
        "transition_counts": transitions.tolist(),
        "run_lengths_by_state": {str(state): runs[state] for state in range(3)},
        "run_lengths_sha256": canonical_sha256({str(state): runs[state] for state in range(3)}),
    }


def _signed_distances(entry: Mapping[str, Any], hard_detail: Mapping[str, Any]) -> dict[str, Any]:
    likelihood = entry.get("likelihood", {})
    likelihood_evidence = likelihood.get("evidence", {})
    delta_distances = []
    for delta in likelihood_evidence.get("deltas") or ():
        relative = float(delta["relative"])
        delta_distances.append(
            {
                **dict(delta),
                "terminal_negative_distance_to_minus_2e_5": relative - (-2e-5)
                if delta.get("terminal") and float(delta["absolute"]) < 0.0
                else None,
            }
        )
    covariance = entry.get("covariance", {})
    covariance_evidence = covariance.get("evidence", {})
    raw = np.asarray(covariance_evidence.get("raw_covars"), dtype=np.float64)
    lower = np.asarray(covariance_evidence.get("dynamic_lower_reference"), dtype=np.float64)
    upper = np.asarray(covariance_evidence.get("dynamic_upper_reference"), dtype=np.float64)
    residual = np.asarray(covariance_evidence.get("mstep_relative_residual"), dtype=np.float64)
    if not (raw.shape == lower.shape == upper.shape == residual.shape) or raw.ndim != 2:
        raise StateModelSetError("blocker diagnostic covariance distance evidence is incomplete")
    lower_slack = raw - (1.0 - 0.005) * lower
    upper_slack = (1.0 + 0.005) * upper - raw
    mstep_slack = 0.02 - residual
    occupancy_evidence = entry.get("train_occupancy", {}).get("evidence", {})
    states = occupancy_evidence.get("states")
    if not isinstance(states, Mapping) or set(states) != {"0", "1", "2"}:
        raise StateModelSetError("blocker diagnostic train hard-state evidence is incomplete")
    count_threshold = int(occupancy_evidence["count_threshold"])
    state_distances = {}
    for state, metrics in states.items():
        share = metrics.get("maximum_single_run_share")
        state_distances[state] = {
            "count_slack": int(metrics["hard_count"]) - count_threshold,
            "occupancy_slack": float(metrics["normalized_occupancy"]) - 0.01,
            "month_slack": int(metrics["calendar_month_count"]) - 3,
            "run_slack": int(metrics["contiguous_run_count"]) - 3,
            "incoming_transition_slack": int(metrics["incoming_transition_count"]) - 2,
            "outgoing_transition_slack": int(metrics["outgoing_transition_count"]) - 2,
            "run_concentration_slack": None if share is None else 0.8 - float(share),
            "run_lengths": hard_detail["run_lengths_by_state"][state],
        }
    return {
        "likelihood_contract_version": D4_LIKELIHOOD_VERSION,
        "likelihood_delta_distances": delta_distances,
        "covariance_contract_version": D4_COVARIANCE_VERSION,
        "covariance_lower_slack": lower_slack.tolist(),
        "covariance_upper_slack": upper_slack.tolist(),
        "covariance_mstep_slack": mstep_slack.tolist(),
        "covariance_min_lower_slack": float(lower_slack.min()),
        "covariance_min_upper_slack": float(upper_slack.min()),
        "covariance_min_mstep_slack": float(mstep_slack.min()),
        "train_occupancy_contract_version": D4_OCCUPANCY_VERSION,
        "posterior_normalization_slack": 1e-12 - float(occupancy_evidence["row_sum_max_abs_error"]),
        "posterior_margin_slack": float(occupancy_evidence["top1_top2_min_margin"]) - 1e-12,
        "state_distances": state_distances,
    }


def run_targeted_level(
    series: Mapping[str, B3TrainOnlySeries],
    targets: Sequence[Mapping[str, Any]],
    *,
    family: str,
    level: str,
    feature_names: Sequence[str],
    preprocess_family: str,
) -> list[dict[str, Any]]:
    preprocess = _fit_preprocess(series, preprocess_family=preprocess_family)
    environment = c008_b3_diag04_fixed_numeric_environment()
    if str(environment.get("packages", {}).get("hmmlearn") or "") != "0.3.3":
        raise StateModelSetError("blocker diagnostic requires hmmlearn==0.3.3")
    output: list[dict[str, Any]] = []
    for target in targets:
        code = str(target.get("sector_code") or "")
        seed = int(target.get("seed"))
        if target.get("family") != family or target.get("level") != level or code not in series:
            raise StateModelSetError("blocker diagnostic target differs from its grouped train series")
        item = series[code]
        train_input_manifest_sha256 = canonical_sha256(dict(item.train_input_manifest))
        try:
            entry, fitted = fit_b3_target_entry(
                item,
                family=family,
                level=level,
                feature_names=feature_names,
                preprocess=preprocess,
                seed=seed,
                numeric_environment=environment,
            )
        except B3TrainingStageError as exc:
            formal_failure = {
                "schema_version": "hmm_risk_b3_training_entry_receipt_v1",
                "contract_version": formal_b3_parameter_profile()["contract"],
                "retrain_contract_version": L2_RETRAIN_VERSION if level == "L2" else None,
                "family": family,
                "level": level,
                "seed": seed,
                "sector_code": code,
                "feature_count": len(tuple(feature_names)),
                "training_rows": int(item.train_observations.shape[0]),
                "fit_status": "failed",
                "model_entry_status": "failed",
                "model_entry_valid": False,
                "failure_stage": exc.stage,
                "failure_reason_codes": [exc.reason_code],
                "failure_type": exc.cause_type,
                "failure_message": str(exc),
                "validation_accessed": False,
                "future_utility_accessed": False,
                "semantic_labelability_accessed": False,
                "d6_status_accessed": False,
                "artifact_write_performed": False,
            }
            formal_hash = canonical_sha256(formal_failure)
            if formal_hash != target.get("source_entry_receipt_sha256"):
                raise StateModelSetError("blocker diagnostic failed-entry replay differs from formal receipt")
            body = {
                **dict(target),
                "status": "fit_failed",
                "formal_entry_receipt_reproduced": True,
                "diagnostic_failure_stage": exc.stage,
                "diagnostic_failure_reason_code": exc.reason_code,
                "diagnostic_failure_type": exc.cause_type,
                "diagnostic_failure_evidence": exc.cause_evidence,
                "train_input_manifest_sha256": train_input_manifest_sha256,
                "fitted_model_payload": None,
                "missing_evidence": [
                    "fitted_model_payload",
                    "likelihood",
                    "covariance",
                    "train_occupancy",
                ],
                "validation_accessed": False,
                "future_utility_accessed": False,
                "selection_performed": False,
                "model_write_performed": False,
            }
            if target.get("role") != "rejected":
                raise StateModelSetError("blocker diagnostic deterministic control replayed as a fit failure")
        else:
            if entry.get("entry_receipt_sha256") != target.get("source_entry_receipt_sha256"):
                raise StateModelSetError("blocker diagnostic refit entry differs from formal receipt")
            train = _apply_preprocess(item.train_observations, fitted.preprocess)
            posteriors = causal_forward_posteriors(
                train,
                startprob=fitted.startprob,
                transmat=fitted.transmat,
                means=fitted.means,
                covars=fitted.covars,
            )
            hard_detail = _hard_sequence_detail(posteriors, item.train_dates)
            occupancy_evidence = entry.get("train_occupancy", {}).get("evidence", {})
            if hard_detail["hard_assignment_sha256"] != occupancy_evidence.get("hard_assignment_sha256") or hard_detail[
                "transition_counts"
            ] != occupancy_evidence.get("transition_counts"):
                raise StateModelSetError("blocker diagnostic train hard-sequence replay differs from formal receipt")
            body = {
                **dict(target),
                "status": "fit_completed",
                "formal_entry_receipt_reproduced": True,
                "training_receipt": entry,
                "train_input_manifest_sha256": train_input_manifest_sha256,
                "fitted_model_payload": fitted.payload(),
                "initialization_centers_sha256": canonical_sha256(entry["initialization_evidence"].get("means")),
                "hard_sequence_detail": hard_detail,
                "signed_distances": _signed_distances(entry, hard_detail),
                "missing_evidence": [],
                "validation_accessed": False,
                "future_utility_accessed": False,
                "selection_performed": False,
                "model_write_performed": False,
            }
            if target.get("role") == "control" and entry.get("model_entry_valid") is not True:
                raise StateModelSetError("blocker diagnostic deterministic control is no longer D4 accepted")
            if target.get("role") == "rejected" and entry.get("model_entry_valid") is not False:
                raise StateModelSetError("blocker diagnostic rejected entry no longer reproduces its D4 failure")
        output.append({**body, "diagnostic_entry_sha256": canonical_sha256(body)})
    return output


def _numeric_leaves(value: Any, *, path: str = "") -> dict[str, float]:
    output: dict[str, float] = {}
    if isinstance(value, Mapping):
        for key in sorted(value):
            child = f"{path}.{key}" if path else str(key)
            output.update(_numeric_leaves(value[key], path=child))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            output.update(_numeric_leaves(item, path=f"{path}[{index}]"))
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number):
            raise StateModelSetError(f"blocker diagnostic numeric comparison is non-finite: {path}")
        output[path] = number
    return output


def _failure_classification(target: Mapping[str, Any]) -> tuple[str, list[str], list[str]]:
    stages = list(target.get("formal_failed_stages") or ())
    stage_names = [str(stage.get("stage") or "") for stage in stages]
    reasons = [
        str(reason)
        for stage in stages
        for reason in (list(stage.get("failure_reason_codes") or ()) + list(stage.get("blocking_reason_codes") or ()))
    ]
    if len(stages) > 1:
        classification = "multi_stage_failure"
    elif any("initialization" in value for value in (*stage_names, *reasons)):
        classification = "initialization_failure"
    elif stage_names == ["likelihood"]:
        classification = "likelihood_failure"
    elif stage_names == ["covariance"]:
        classification = "covariance_failure"
    elif stage_names == ["train_occupancy"]:
        classification = "hard_structure_failure"
    else:
        classification = "insufficient_evidence"
    return classification, stage_names, sorted(set(reasons))


def build_matched_comparisons(evidence: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    controls: dict[tuple[str, str, int], Mapping[str, Any]] = {}
    rejected: list[Mapping[str, Any]] = []
    for entry in evidence:
        key = (str(entry.get("family") or ""), str(entry.get("level") or ""), int(entry.get("seed")))
        if entry.get("role") == "control":
            if key in controls:
                raise StateModelSetError("blocker diagnostic contains duplicate matched controls")
            controls[key] = entry
        elif entry.get("role") == "rejected":
            rejected.append(entry)
        else:
            raise StateModelSetError("blocker diagnostic contains an unsupported target role")
    if len(controls) != 24 or len(rejected) != 150:
        raise StateModelSetError("blocker diagnostic comparison input closure is invalid")
    comparisons: list[dict[str, Any]] = []
    stage_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    classification_counts: dict[str, int] = {}
    missing_evidence_map: list[dict[str, Any]] = []
    for entry in rejected:
        key = (str(entry["family"]), str(entry["level"]), int(entry["seed"]))
        control = controls.get(key)
        if control is None:
            raise StateModelSetError("blocker diagnostic rejected entry lacks its deterministic control")
        classification, stages, reasons = _failure_classification(entry)
        classification_counts[classification] = classification_counts.get(classification, 0) + 1
        for stage in stages:
            stage_counts[stage] = stage_counts.get(stage, 0) + 1
        for reason in reasons:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        rejected_values = _numeric_leaves(entry.get("signed_distances") or {})
        control_values = _numeric_leaves(control.get("signed_distances") or {})
        common = sorted(set(rejected_values) & set(control_values))
        differences = [
            {
                "path": path,
                "rejected_value": rejected_values[path],
                "control_value": control_values[path],
                "rejected_minus_control": rejected_values[path] - control_values[path],
            }
            for path in common
        ]
        missing = sorted(set(rejected_values) ^ set(control_values))
        missing.extend(str(value) for value in entry.get("missing_evidence") or ())
        missing = sorted(set(missing))
        if missing:
            missing_evidence_map.append(
                {
                    "family": key[0],
                    "level": key[1],
                    "seed": key[2],
                    "sector_code": entry["sector_code"],
                    "missing_evidence": missing,
                }
            )
        body = {
            "family": key[0],
            "level": key[1],
            "seed": key[2],
            "rejected_sector_code": entry["sector_code"],
            "control_sector_code": control["sector_code"],
            "classification": classification,
            "failed_stages": stages,
            "failure_reason_codes": reasons,
            "rejected_source_entry_receipt_sha256": entry["source_entry_receipt_sha256"],
            "control_source_entry_receipt_sha256": control["source_entry_receipt_sha256"],
            "rejected_diagnostic_entry_sha256": entry["diagnostic_entry_sha256"],
            "control_diagnostic_entry_sha256": control["diagnostic_entry_sha256"],
            "rejected_train_input_manifest_sha256": entry["train_input_manifest_sha256"],
            "control_train_input_manifest_sha256": control["train_input_manifest_sha256"],
            "rejected_model_payload_sha256": None
            if entry.get("fitted_model_payload") is None
            else entry["fitted_model_payload"]["model_payload_sha256"],
            "control_model_payload_sha256": None
            if control.get("fitted_model_payload") is None
            else control["fitted_model_payload"]["model_payload_sha256"],
            "matched_numeric_differences": differences,
            "matched_numeric_difference_count": len(differences),
            "missing_evidence": missing,
        }
        comparisons.append({**body, "comparison_sha256": canonical_sha256(body)})
    aggregate = {
        "comparison_count": len(comparisons),
        "classification_counts": dict(sorted(classification_counts.items())),
        "stage_counts": dict(sorted(stage_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "missing_evidence_entry_count": len(missing_evidence_map),
    }
    body = {
        "schema_version": "hmm_risk_c008_b3_formal_blocker_matched_comparison_v1",
        "comparisons": comparisons,
        "aggregate": aggregate,
        "missing_evidence_map": missing_evidence_map,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _selected_model(entry: Mapping[str, Any]) -> B3FittedModel:
    feature_names = tuple(str(value) for value in entry.get("feature_names") or ())
    model_body = {
        "schema_version": entry.get("schema_version"),
        "contract_version": entry.get("contract_version"),
        "family": entry.get("family"),
        "level": entry.get("level"),
        "seed": entry.get("seed"),
        "sector_code": entry.get("sector_code"),
        "feature_names": list(feature_names),
        "preprocess": entry.get("preprocess"),
        "startprob": entry.get("startprob"),
        "transmat": entry.get("transmat"),
        "means": entry.get("means"),
        "covariance_type": entry.get("covariance_type"),
        "covars": entry.get("covars"),
        "parameter_profile_sha256": entry.get("parameter_profile_sha256"),
        "numeric_environment_sha256": entry.get("numeric_environment_sha256"),
        "observation_manifest_hash": entry.get("observation_manifest_hash"),
        "pit_constituent_manifest_hash": entry.get("pit_constituent_manifest_hash"),
    }
    model_hash = _hex_sha(entry.get("model_payload_sha256"), "selected model payload")
    if canonical_sha256(model_body) != model_hash:
        raise StateModelSetError("selected model payload hash mismatch")
    return B3FittedModel(
        family=str(entry["family"]),
        level=str(entry["level"]),
        seed=int(entry["seed"]),
        sector_code=str(entry["sector_code"]),
        feature_names=feature_names,
        preprocess=dict(entry["preprocess"]),
        startprob=np.asarray(entry["startprob"], dtype=np.float64),
        transmat=np.asarray(entry["transmat"], dtype=np.float64),
        means=np.asarray(entry["means"], dtype=np.float64),
        covars=np.asarray(entry["covars"], dtype=np.float64),
        parameter_profile_sha256=str(entry["parameter_profile_sha256"]),
        numeric_environment_sha256=str(entry["numeric_environment_sha256"]),
        observation_manifest_hash=str(entry["observation_manifest_hash"]),
        pit_constituent_manifest_hash=str(entry["pit_constituent_manifest_hash"]),
        model_payload_sha256=model_hash,
    )


def replay_selected_d6(
    report: Mapping[str, Any],
    series: Mapping[str, L1TrainingSeries],
    target_manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    artifact = report["selected_artifacts"]["legacy_covfix:L1"]
    entries = {str(entry["sector_code"]): entry for entry in artifact["entries"]}
    results: list[dict[str, Any]] = []
    for target in target_manifest.get("d6_replay_targets") or ():
        code = str(target.get("sector_code") or "")
        if code not in entries or code not in series:
            raise StateModelSetError("D6 blocker replay target is missing")
        original = entries[code]
        if original.get("selected_entry_sha256") != target.get("selected_entry_sha256"):
            raise StateModelSetError("D6 blocker replay selected entry hash mismatch")
        model = _selected_model(original)
        item = series[code]
        item.validate(len(model.feature_names))
        validation = _apply_preprocess(item.validation_observations, model.preprocess)
        posteriors = causal_forward_posteriors(
            validation,
            startprob=model.startprob,
            transmat=model.transmat,
            means=model.means,
            covars=model.covars,
        )
        semantic = evaluate_semantic_validation(
            posteriors,
            item.validation_dates,
            {
                "source_cutoff": item.validation_utility_source_cutoff.isoformat()
                if item.validation_utility_source_cutoff
                else None,
                "formula_version": item.validation_utility_formula_version,
                **{name: values for name, values in item.validation_future_components.items()},
            },
            frozen_input_manifest=item.validation_input_manifest,
            selected_model_payload_sha256=model.model_payload_sha256,
        )
        if semantic != original.get("semantic"):
            raise StateModelSetError("D6 blocker replay differs from the formal selected semantic receipt")
        assignment = semantic["assignment"]["evidence"]
        states = assignment["states"]
        utility_evidence = semantic["semantic_evidence"].get("evidence")
        if not isinstance(utility_evidence, Mapping) or not isinstance(utility_evidence.get("state_utility"), Mapping):
            raise StateModelSetError("D6 blocker replay utility evidence is incomplete")
        utility_rows = []
        for state, metrics in utility_evidence["state_utility"].items():
            mean = float(metrics["mean"])
            variance = float(metrics["sample_variance_ddof_1"])
            if not math.isfinite(mean) or not math.isfinite(variance) or int(metrics["count"]) <= 1:
                raise StateModelSetError("D6 blocker replay utility evidence is non-finite or insufficient")
            utility_rows.append(
                {
                    "state": str(state),
                    "count": int(metrics["count"]),
                    "mean": mean,
                    "sample_variance_ddof_1": variance,
                }
            )
        utility_rows.sort(key=lambda value: (value["mean"], value["state"]))
        adjacent_gaps = []
        for lower, upper in zip(utility_rows, utility_rows[1:], strict=False):
            threshold = max(
                1e-12,
                32.0 * np.finfo(np.float64).eps * max(1.0, abs(lower["mean"]), abs(upper["mean"])),
            )
            gap = upper["mean"] - lower["mean"]
            adjacent_gaps.append(
                {
                    "lower_state": lower["state"],
                    "upper_state": upper["state"],
                    "numeric_gap": gap,
                    "approved_numeric_gap_threshold": threshold,
                    "signed_gap_slack": gap - threshold,
                }
            )
        hard_detail = _hard_sequence_detail(posteriors, item.validation_dates)
        if hard_detail["hard_assignment_sha256"] != assignment.get("hard_assignment_sha256") or hard_detail[
            "transition_counts"
        ] != assignment.get("transition_counts"):
            raise StateModelSetError("D6 blocker hard-sequence replay differs from the formal semantic receipt")
        state_distances = {}
        count_threshold = max(5, math.ceil(0.02 * len(item.validation_dates)))
        for state, metrics in states.items():
            state_distances[state] = {
                "count_slack": int(metrics["hard_count"]) - count_threshold,
                "occupancy_slack": float(metrics["normalized_occupancy"]) - 0.02,
                "month_slack": int(metrics["calendar_month_count"]) - 2,
                "run_slack": int(metrics["contiguous_run_count"]) - 2,
                "incoming_transition_slack": int(metrics["incoming_transition_count"]) - 2,
                "outgoing_transition_slack": int(metrics["outgoing_transition_count"]) - 2,
                "run_concentration_slack": 0.9 - float(metrics["maximum_single_run_share"]),
                "run_lengths": hard_detail["run_lengths_by_state"][state],
            }
        body = {
            **dict(target),
            "contract_version": D6_SEMANTIC_VERSION,
            "semantic_receipt": semantic,
            "hard_sequence_detail": hard_detail,
            "state_distances": state_distances,
            "hard_utility_by_state": utility_rows,
            "adjacent_utility_gap_distances": adjacent_gaps,
            "posterior_normalization_slack": 1e-12 - float(assignment["row_sum_max_abs_error"]),
            "posterior_margin_slack": float(assignment["top1_top2_min_margin"]) - 1e-12,
            "selection_reexecuted": False,
            "refit_performed": False,
            "soft_evidence_used_for_acceptance": False,
        }
        results.append({**body, "d6_replay_sha256": canonical_sha256(body)})
    if len(results) != 3:
        raise StateModelSetError("D6 blocker replay must contain exactly three entries")
    return results
