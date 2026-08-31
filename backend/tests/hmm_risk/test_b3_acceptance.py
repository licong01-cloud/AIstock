from __future__ import annotations

import math
from copy import deepcopy
from datetime import date, timedelta

import numpy as np
import pytest

from backend.services.hmm_risk import b3_acceptance as subject
from backend.services.hmm_risk.b3_mixed_dimension import (
    INACTIVE_DIMENSION_REASON_CODE,
    MIXED_DIMENSION_CONTRACT_VERSION,
    MIXED_MODEL_SCHEMA_VERSION,
    MIXED_REPEAT_SCHEMA_VERSION,
    MIXED_TRAINING_ENTRY_SCHEMA_VERSION,
    TARGET_SECTOR,
    build_projection_receipt,
)
from backend.services.hmm_risk.state_model_set import ALL_CORE_FEATURES


def _monitor(
    history: list[float],
    *,
    converged: bool = True,
    map_history: list[float] | None = None,
    covariance_valid: list[bool] | None = None,
    covariance_receipt_sha256: str = "a" * 64,
) -> dict:
    if map_history is None:
        map_history = [-100.0 + index for index in range(len(history))]
        if len(map_history) >= 2:
            map_history[-1] = map_history[-2] + 1e-7
    if covariance_valid is None:
        covariance_valid = [True] * len(history)
    prior_history = [map_value - raw for map_value, raw in zip(map_history, history, strict=True)]
    map_history = [raw + prior for raw, prior in zip(history, prior_history, strict=True)]
    return {
        "authority": "covariance_prior_map_objective",
        "maximum_iterations": 300,
        "raw_likelihood_history": history,
        "map_objective_history": map_history,
        "map_prior_adjustment_history": prior_history,
        "objective_component_history": [
            {
                "iteration": index + 1,
                "raw_log_likelihood": raw,
                "prior_log_covariance_component": -2.0 * prior - 1.0,
                "prior_inverse_covariance_component": 1.0,
                "prior_adjustment": prior,
                "map_objective": map_value,
            }
            for index, (raw, prior, map_value) in enumerate(zip(history, prior_history, map_history, strict=True))
        ],
        "covariance_valid_history": covariance_valid,
        "covariance_receipt_sha256_history": [covariance_receipt_sha256] * len(history),
        "joint_stop_iteration": len(history) if converged else None,
        "raw_likelihood_is_diagnostic_only": True,
        "postfit_projection_performed": False,
    }


def test_d4_01_keeps_raw_likelihood_warning_distinct_and_rejects_map_decrease() -> None:
    warning = subject.evaluate_likelihood_acceptance(_monitor([-100.0, -90.0, -90.001]))
    assert warning["monitor_status"] == "accepted"
    assert warning["convergence_valid"] is True
    assert warning["likelihood_status"] == "accepted_with_warning"
    assert warning["likelihood_valid"] is True
    assert warning["warning_reason_codes"] == ["hmm_risk_model_raw_likelihood_decrease_diagnostic"]

    failure = subject.evaluate_likelihood_acceptance(
        _monitor([-100.0, -99.0], converged=False, map_history=[-100.0, -100.001])
    )
    assert failure["monitor_status"] == "failed"
    assert failure["convergence_valid"] is False
    assert "hmm_risk_model_map_objective_decrease" in failure["failure_reason_codes"]


def test_d4_01_exact_map_envelope_and_missing_evidence_fail_closed() -> None:
    previous = -100.0
    envelope = subject.map_numeric_envelope(previous)
    accepted = subject.evaluate_likelihood_acceptance(
        _monitor([-2.0, -1.0], map_history=[previous, previous + envelope])
    )
    boundary_warning = subject.evaluate_likelihood_acceptance(
        _monitor([-2.0, -1.0], map_history=[previous, previous - envelope])
    )
    rejected = subject.evaluate_likelihood_acceptance(
        _monitor(
            [-2.0, -1.0],
            converged=False,
            map_history=[previous, previous - 2.0 * envelope],
        )
    )
    missing = subject.evaluate_likelihood_acceptance(None)

    assert accepted["likelihood_status"] == "accepted"
    assert boundary_warning["likelihood_status"] == "accepted_with_warning"
    assert "hmm_risk_model_map_numeric_envelope_warning" in boundary_warning["warning_reason_codes"]
    assert "hmm_risk_model_map_objective_decrease" in rejected["failure_reason_codes"]
    assert missing["likelihood_status"] == "insufficient_evidence"


def test_d4_01_rejects_unknown_authority_instead_of_coercing_it() -> None:
    receipt = subject.evaluate_likelihood_acceptance(_monitor([-2.0, -1.5]) | {"authority": "raw_likelihood_monitor"})
    assert receipt["monitor_status"] == "failed"
    assert receipt["convergence_valid"] is False
    assert "hmm_risk_model_monitor_history_invalid" in receipt["failure_reason_codes"]


def test_d4_numeric_evidence_non_finite_is_failed_not_missing_or_serialized_as_null_success() -> None:
    likelihood = subject.evaluate_likelihood_acceptance(_monitor([-1.0, float("nan")]))
    covariance_input = _covariance_evidence()
    covariance_input["raw_covars"][0][0] = float("inf")
    covariance = subject.evaluate_covariance_acceptance(covariance_input)

    assert likelihood["likelihood_status"] == "failed"
    assert "hmm_risk_model_likelihood_non_finite" in likelihood["failure_reason_codes"]
    assert covariance["covariance_status"] == "failed"
    assert "hmm_risk_model_covariance_invalid" in covariance["failure_reason_codes"]


def test_d4_01_map_objective_matches_covariance_prior_formula_exactly() -> None:
    raw_covars = np.asarray([[0.5, 2.0], [1.0, 4.0], [0.25, 8.0]], dtype=np.float64)
    prior = np.asarray([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], dtype=np.float64)
    receipt = subject.map_covariance_prior_objective(-123.0, raw_covars, prior, 2.0)
    expected_adjustment = -0.5 * float(np.sum(np.log(raw_covars) + prior / raw_covars))

    assert receipt["prior_adjustment"] == expected_adjustment
    assert receipt["map_objective"] == -123.0 + expected_adjustment


@pytest.mark.parametrize("invalid", [0.0, -1.0, float("nan"), float("inf")])
def test_d4_01_map_objective_rejects_invalid_raw_covariance(invalid: float) -> None:
    raw_covars = np.ones((3, 2), dtype=np.float64)
    raw_covars[0, 0] = invalid
    with pytest.raises(ValueError):
        subject.map_covariance_prior_objective(-1.0, raw_covars, np.ones((3, 2)), 2.0)


def test_d4_01_map_receipt_rejects_component_drift_projection_and_steps_after_joint_stop() -> None:
    component_drift = _monitor([-2.0, -1.0])
    component_drift["objective_component_history"][1]["prior_inverse_covariance_component"] += 1.0
    projected = _monitor([-2.0, -1.0]) | {"postfit_projection_performed": True}
    trailing = _monitor(
        [-3.0, -2.0, -1.0],
        map_history=[-100.0, -100.0 + 1e-7, -100.0 + 2e-7],
    )
    trailing["joint_stop_iteration"] = 2

    drift_receipt = subject.evaluate_likelihood_acceptance(component_drift)
    projected_receipt = subject.evaluate_likelihood_acceptance(projected)
    trailing_receipt = subject.evaluate_likelihood_acceptance(trailing)

    assert "hmm_risk_model_map_objective_non_finite" in drift_receipt["failure_reason_codes"]
    assert "hmm_risk_model_contract_unsupported" in projected_receipt["failure_reason_codes"]
    assert "hmm_risk_model_map_joint_convergence_unavailable" in trailing_receipt["failure_reason_codes"]


def test_d4_01_map_receipt_rejects_string_numeric_coercion() -> None:
    receipt = _monitor([-2.0, -1.0])
    receipt["joint_stop_iteration"] = "2"
    result = subject.evaluate_likelihood_acceptance(receipt)

    assert result["convergence_valid"] is False
    assert "hmm_risk_model_monitor_history_invalid" in result["failure_reason_codes"]


def _covariance_evidence(*, raw: float = 1.0, residual_reference: float = 1.0) -> dict:
    return {
        "raw_covars": np.full((3, 2), raw).tolist(),
        "sector_local_reference_variance_R_sj": [1.0, 1.0],
        "state_posterior_mass": [10.0, 10.0, 10.0],
        "posterior_second_moment_about_fitted_mean": np.full((3, 2), residual_reference).tolist(),
        "train_rows": 30,
        "nu": 1.0,
        "postfit_projection_performed": False,
    }


def test_d4_02_accepts_raw_consistent_covariance_and_rejects_bounds_projection_and_residual() -> None:
    accepted = subject.evaluate_covariance_acceptance(_covariance_evidence())
    bounded = subject.evaluate_covariance_acceptance(_covariance_evidence(raw=3.0))
    residual = subject.evaluate_covariance_acceptance(_covariance_evidence(raw=1.03))
    projected = subject.evaluate_covariance_acceptance({**_covariance_evidence(), "postfit_projection_performed": True})

    assert accepted["covariance_status"] == "accepted"
    assert accepted["covariance_valid"] is True
    assert "hmm_risk_model_covariance_bounds_failed" in bounded["failure_reason_codes"]
    assert "hmm_risk_model_covariance_acceptance_failed" in residual["failure_reason_codes"]
    assert "hmm_risk_model_covariance_acceptance_failed" in projected["failure_reason_codes"]


def _train_dates(count: int) -> tuple[date, ...]:
    return tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(count))


def _posterior(states: list[int]) -> np.ndarray:
    result = np.zeros((len(states), 3), dtype=np.float64)
    result[np.arange(len(states)), states] = 1.0
    return result


def _train_manifest(dates: tuple[date, ...]) -> dict:
    encoded = [value.isoformat() for value in dates]
    return {
        "schema_version": "hmm_risk_d4_train_frozen_input_manifest_v1",
        "direct_sector_level": "L1",
        "sector_code": "S001",
        "train_observation_sha256": "f" * 64,
        "train_dates": encoded,
        "train_dates_sha256": subject.canonical_sha256(encoded),
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "feature_domain_policy_sha256": "e" * 64,
    }


def test_d4_03_anti_singleton_train_contract() -> None:
    accepted_states = [index % 3 for index in range(30)]
    dates = _train_dates(30)
    accepted = subject.evaluate_train_occupancy(
        _posterior(accepted_states), dates, frozen_input_manifest=_train_manifest(dates)
    )
    singleton_states = [0 if index % 2 == 0 else 1 for index in range(30)]
    singleton_states[-1] = 2
    rejected = subject.evaluate_train_occupancy(
        _posterior(singleton_states), dates, frozen_input_manifest=_train_manifest(dates)
    )

    assert accepted["train_occupancy_status"] == "accepted"
    assert accepted["evidence"]["validation_accessed"] is False
    assert accepted["evidence"]["future_utility_accessed"] is False
    assert "hmm_risk_model_train_state_count_insufficient" in rejected["failure_reason_codes"]
    assert "hmm_risk_model_train_month_coverage_insufficient" in rejected["failure_reason_codes"]


def test_d4_03_persistent_path_accepts_exact_count_threshold_without_weakening_common_gate() -> None:
    states = [1, 2] * 30 + [0] * 29 + [1, 2] * 30 + [0] + [1, 2] * 75
    dates = _train_dates(len(states))
    receipt = subject.evaluate_train_occupancy(_posterior(states), dates, frozen_input_manifest=_train_manifest(dates))

    state_zero = receipt["evidence"]["states"]["0"]
    assert receipt["train_occupancy_status"] == "accepted"
    assert state_zero["hard_count"] == 30
    assert state_zero["maximum_single_run_share"] > 0.8
    assert state_zero["evidence_path"] == "persistent"
    assert state_zero["persistent_path"]["valid"] is True


def test_d4_03_two_run_low_share_cannot_use_persistent_path() -> None:
    states = [1, 2] * 25 + [0] * 15 + [1, 2] * 25 + [0] * 15 + [1, 2] * 85
    dates = _train_dates(len(states))
    receipt = subject.evaluate_train_occupancy(_posterior(states), dates, frozen_input_manifest=_train_manifest(dates))

    state_zero = receipt["evidence"]["states"]["0"]
    assert state_zero["maximum_single_run_share"] <= 0.8
    assert state_zero["evidence_path"] == "none"
    assert "hmm_risk_model_train_regime_path_unsatisfied" in receipt["failure_reason_codes"]


def _selection_repeat(*, level: str, preferred_seed: int) -> dict:
    count = 31 if level == "L1" else 131
    codes = [f"S{index:03d}" for index in range(count)]
    entries = []
    models = []
    covariance = subject.evaluate_covariance_acceptance(_covariance_evidence())
    likelihood = subject.evaluate_likelihood_acceptance(
        _monitor([-2.0, -1.995], covariance_receipt_sha256=covariance["receipt_sha256"])
    )
    train_dates = _train_dates(30)
    train_occupancy = subject.evaluate_train_occupancy(
        _posterior([index % 3 for index in range(30)]),
        train_dates,
        frozen_input_manifest=_train_manifest(train_dates),
    )
    for seed in subject.RESTART_SCHEDULE:
        score = -1.0 if seed == preferred_seed else -2.0 - 0.01 * (seed - 42)
        for code in codes:
            model_body = {
                "family": "legacy_covfix",
                "level": level,
                "seed": seed,
                "sector_code": code,
            }
            model = {**model_body, "model_payload_sha256": subject.canonical_sha256(model_body)}
            models.append(model)
            entry_body = {
                "family": "legacy_covfix",
                "level": level,
                "seed": seed,
                "sector_code": code,
                "fit_status": "accepted",
                "likelihood": likelihood,
                "covariance": covariance,
                "train_occupancy": train_occupancy,
                "model_entry_status": "accepted",
                "model_entry_valid": True,
                "model_payload_sha256": model["model_payload_sha256"],
                "final_train_log_likelihood": score * 100 * 7,
                "training_rows": 100,
                "feature_count": 7,
            }
            entries.append({**entry_body, "entry_receipt_sha256": subject.canonical_sha256(entry_body)})
    repeat = {
        "family": "legacy_covfix",
        "level": level,
        "schedule": list(subject.RESTART_SCHEDULE),
        "canonical_sector_codes": codes,
        "feature_names": [f"f{index}" for index in range(7)],
        "preprocess": {"family": "identity"},
        "numeric_environment": {"scope": "test"},
        "entries": entries,
        "models": models,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
    }
    candidate_payload = {
        key: repeat[key]
        for key in (
            "family",
            "level",
            "schedule",
            "canonical_sector_codes",
            "feature_names",
            "preprocess",
            "numeric_environment",
            "entries",
            "models",
        )
    }
    repeat["model_payload_sha256"] = subject.canonical_sha256(models)
    repeat["candidate_payload_sha256"] = subject.canonical_sha256(candidate_payload)
    return repeat


def test_candidate_rejects_rehashed_map_joint_covariance_lineage_drift() -> None:
    entry = deepcopy(_selection_repeat(level="L1", preferred_seed=46)["entries"][0])
    assert subject._candidate_status(entry) is True

    entry["likelihood"]["evidence"]["covariance_receipt_sha256_history"][-1] = "0" * 64
    likelihood_body = {key: value for key, value in entry["likelihood"].items() if key != "receipt_sha256"}
    entry["likelihood"] = {**likelihood_body, "receipt_sha256": subject.canonical_sha256(likelihood_body)}

    assert subject._candidate_status(entry) is False


def _rehash_selection_repeat(repeat: dict) -> None:
    candidate_fields = (
        "family",
        "level",
        "schedule",
        "canonical_sector_codes",
        "feature_names",
        "preprocess",
        "numeric_environment",
        "entries",
        "models",
    )
    payload = {field: repeat[field] for field in candidate_fields}
    if "dimension_contract_version" in repeat:
        payload["dimension_contract_version"] = repeat["dimension_contract_version"]
    repeat["candidate_payload_sha256"] = subject.canonical_sha256(payload)


def test_d5_recomputes_fully_rehashed_map_receipt_before_candidate_acceptance() -> None:
    repeat = _selection_repeat(level="L1", preferred_seed=46)
    for entry in (value for value in repeat["entries"] if value["sector_code"] == "S000"):
        likelihood = deepcopy(entry["likelihood"])
        evidence = likelihood["evidence"]
        evidence["map_objective_history"] = [-100.0, -101.0]
        evidence["map_prior_adjustment_history"] = [-98.0, -99.005]
        evidence["objective_component_history"] = [
            {
                "iteration": 1,
                "raw_log_likelihood": -2.0,
                "prior_log_covariance_component": 195.0,
                "prior_inverse_covariance_component": 1.0,
                "prior_adjustment": -98.0,
                "map_objective": -100.0,
            },
            {
                "iteration": 2,
                "raw_log_likelihood": -1.995,
                "prior_log_covariance_component": 197.01,
                "prior_inverse_covariance_component": 1.0,
                "prior_adjustment": -99.005,
                "map_objective": -101.0,
            },
        ]
        recomputed = subject.evaluate_likelihood_acceptance(evidence)
        assert recomputed["convergence_valid"] is False
        likelihood_body = {key: value for key, value in likelihood.items() if key != "receipt_sha256"}
        entry["likelihood"] = {
            **likelihood_body,
            "receipt_sha256": subject.canonical_sha256(likelihood_body),
        }
        entry_body = {key: value for key, value in entry.items() if key != "entry_receipt_sha256"}
        entry["entry_receipt_sha256"] = subject.canonical_sha256(entry_body)
    _rehash_selection_repeat(repeat)

    selection = subject.select_level_restart(
        deepcopy(repeat),
        deepcopy(repeat),
        family="legacy_covfix",
        level="L1",
        expected_sector_codes=repeat["canonical_sector_codes"],
        feature_count=7,
        feature_domain_policy_sha256="e" * 64,
    )

    assert selection["level_selection_status"] == "failed"
    assert selection["level_selection_valid"] is False
    assert selection["failure_reason_codes"] == ["hmm_risk_model_selection_unavailable"]
    assert all(
        "hmm_risk_model_map_objective_decrease" in candidate["failure_reason_codes"]
        for candidate in selection["evidence"]["candidates"]
    )


def test_candidate_recomputes_rehashed_covariance_and_occupancy_receipts() -> None:
    covariance_entry = deepcopy(_selection_repeat(level="L1", preferred_seed=46)["entries"][0])
    covariance = deepcopy(covariance_entry["covariance"])
    covariance["evidence"]["raw_covars"][0][0] = 3.0
    covariance_body = {key: value for key, value in covariance.items() if key != "receipt_sha256"}
    covariance_entry["covariance"] = {
        **covariance_body,
        "receipt_sha256": subject.canonical_sha256(covariance_body),
    }
    likelihood = deepcopy(covariance_entry["likelihood"])
    likelihood["evidence"]["covariance_receipt_sha256_history"][-1] = covariance_entry["covariance"]["receipt_sha256"]
    likelihood_body = {key: value for key, value in likelihood.items() if key != "receipt_sha256"}
    covariance_entry["likelihood"] = {
        **likelihood_body,
        "receipt_sha256": subject.canonical_sha256(likelihood_body),
    }
    assert subject._candidate_status(covariance_entry) is False

    occupancy_entry = deepcopy(_selection_repeat(level="L1", preferred_seed=46)["entries"][0])
    occupancy = deepcopy(occupancy_entry["train_occupancy"])
    occupancy["evidence"]["states"]["0"]["hard_count"] = 1
    occupancy["evidence"]["states"]["0"]["normalized_occupancy"] = 1 / occupancy["evidence"]["train_rows"]
    occupancy_body = {key: value for key, value in occupancy.items() if key != "receipt_sha256"}
    occupancy_entry["train_occupancy"] = {
        **occupancy_body,
        "receipt_sha256": subject.canonical_sha256(occupancy_body),
    }
    assert subject._candidate_status(occupancy_entry) is False

    nonfinite_entry = deepcopy(_selection_repeat(level="L1", preferred_seed=46)["entries"][0])
    nonfinite_entry["train_occupancy"]["evidence"]["row_sum_max_abs_error"] = float("nan")
    assert subject._candidate_status(nonfinite_entry) is False


def _mixed_dimension_selection_repeat(*, preferred_seed: int) -> dict:
    codes = sorted([f"S{index:03d}" for index in range(130)] + [TARGET_SECTOR])
    entries = []
    models = []
    covariance = subject.evaluate_covariance_acceptance(_covariance_evidence())
    likelihood = subject.evaluate_likelihood_acceptance(
        _monitor([-2.0, -1.995], covariance_receipt_sha256=covariance["receipt_sha256"])
    )
    train_dates = _train_dates(30)
    train_occupancy = subject.evaluate_train_occupancy(
        _posterior([index % 3 for index in range(30)]),
        train_dates,
        frozen_input_manifest=_train_manifest(train_dates),
    )
    preprocess = {
        "family": "winsor_zscore_1_99_train_global_v1",
        "winsor_low": [-3.0] * 20,
        "winsor_high": [3.0] * 20,
        "center": [0.0] * 20,
        "scale": [1.0] * 20,
    }
    manifest = {
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "l2_stock_fact_manifest_hash": "e" * 64,
        "feature_domain_policy_sha256": "e" * 64,
        "formula_version": "hmm_risk_c010_feature_formula_v1",
    }
    projections = {}
    for code in codes:
        observations = np.ones((100, 20), dtype=np.float64)
        if code == TARGET_SECTOR:
            observations[:, 19] = 0.0
        projection, _ = build_projection_receipt(
            family="autocycle_all_core",
            level="L2",
            sector_code=code,
            full_feature_names=ALL_CORE_FEATURES,
            preprocess=preprocess,
            raw_observations=observations,
            preprocessed_observations=observations,
            train_input_manifest=manifest,
        )
        projections[code] = projection
    for seed in subject.RESTART_SCHEDULE:
        normalized_score = -1.0 if seed == preferred_seed else -2.0 - 0.01 * (seed - 42)
        for code in codes:
            projection = projections[code]
            dimension = int(projection["likelihood_feature_count"])
            model_body = {
                "schema_version": MIXED_MODEL_SCHEMA_VERSION,
                "contract_version": subject.D3_CONTRACT_VERSION,
                "family": "autocycle_all_core",
                "level": "L2",
                "seed": seed,
                "sector_code": code,
                "feature_names": list(ALL_CORE_FEATURES),
                "preprocess": preprocess,
                "startprob": [1 / 3, 1 / 3, 1 / 3],
                "transmat": [[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]],
                "means": [[0.0] * dimension for _ in range(3)],
                "covariance_type": "diag",
                "covars": [[1.0] * dimension for _ in range(3)],
                "parameter_profile_sha256": "a" * 64,
                "numeric_environment_sha256": "b" * 64,
                "observation_manifest_hash": "c" * 64,
                "pit_constituent_manifest_hash": "d" * 64,
                "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                "feature_count": 20,
                "likelihood_feature_names": projection["active_feature_names"],
                "likelihood_feature_count": dimension,
                "projection_receipt": projection,
                "projection_sha256": projection["projection_sha256"],
            }
            model = {**model_body, "model_payload_sha256": subject.canonical_sha256(model_body)}
            models.append(model)
            entry_body = {
                "schema_version": MIXED_TRAINING_ENTRY_SCHEMA_VERSION,
                "family": "autocycle_all_core",
                "level": "L2",
                "seed": seed,
                "sector_code": code,
                "fit_status": "accepted",
                "likelihood": likelihood,
                "covariance": covariance,
                "train_occupancy": train_occupancy,
                "model_entry_status": "accepted",
                "model_entry_valid": True,
                "model_payload_sha256": model["model_payload_sha256"],
                "final_train_log_likelihood": normalized_score * 100 * dimension,
                "training_rows": 100,
                "feature_count": 20,
                "likelihood_feature_count": dimension,
                "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                "projection_receipt": projection,
                "projection_sha256": projection["projection_sha256"],
            }
            entries.append({**entry_body, "entry_receipt_sha256": subject.canonical_sha256(entry_body)})
    repeat = {
        "schema_version": MIXED_REPEAT_SCHEMA_VERSION,
        "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
        "feature_count": 20,
        "family": "autocycle_all_core",
        "level": "L2",
        "schedule": list(subject.RESTART_SCHEDULE),
        "canonical_sector_codes": codes,
        "feature_names": list(ALL_CORE_FEATURES),
        "preprocess": preprocess,
        "numeric_environment": {"scope": "test"},
        "entries": entries,
        "models": models,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
    }
    candidate_payload = {
        key: repeat[key]
        for key in (
            "family",
            "level",
            "schedule",
            "canonical_sector_codes",
            "feature_names",
            "preprocess",
            "numeric_environment",
            "entries",
            "models",
            "dimension_contract_version",
        )
    }
    repeat["model_payload_sha256"] = subject.canonical_sha256(models)
    repeat["candidate_payload_sha256"] = subject.canonical_sha256(candidate_payload)
    return repeat


def test_d5_mixed_dimension_uses_effective_entry_dimension_without_reducing_completeness() -> None:
    first = _mixed_dimension_selection_repeat(preferred_seed=46)
    second = _mixed_dimension_selection_repeat(preferred_seed=46)
    codes = first["canonical_sector_codes"]
    receipt = subject.select_level_restart(
        first,
        second,
        family="autocycle_all_core",
        level="L2",
        expected_sector_codes=codes,
        feature_count=20,
        feature_domain_policy_sha256="e" * 64,
    )

    assert receipt["level_selection_valid"] is True
    assert receipt["evidence"]["selected_seed"] == 46
    selected = next(candidate for candidate in receipt["evidence"]["candidates"] if candidate["seed"] == 46)
    assert len(selected["aggregate"]["ordered_sector_scores"]) == 131
    target = next(
        item for item in selected["aggregate"]["ordered_sector_scores"] if item["sector_code"] == TARGET_SECTOR
    )
    assert target["effective_dimension"] == 19
    assert target["denominator"] == 1900
    assert target["score"] == pytest.approx(-1.0)
    target_body = {key: value for key, value in target.items() if key != "score_sha256"}
    assert target["score_sha256"] == subject.canonical_sha256(target_body)
    assert {item["effective_dimension"] for item in selected["aggregate"]["ordered_sector_scores"]} == {19, 20}


def test_d5_mixed_dimension_rejects_rehashed_dimension_drift() -> None:
    first = _mixed_dimension_selection_repeat(preferred_seed=46)
    second = _mixed_dimension_selection_repeat(preferred_seed=46)
    for repeat in (first, second):
        entry = next(item for item in repeat["entries"] if item["sector_code"] == TARGET_SECTOR)
        entry["likelihood_feature_count"] = 20
        entry_body = {key: value for key, value in entry.items() if key != "entry_receipt_sha256"}
        entry["entry_receipt_sha256"] = subject.canonical_sha256(entry_body)
        candidate_payload = {
            key: repeat[key]
            for key in (
                "family",
                "level",
                "schedule",
                "canonical_sector_codes",
                "feature_names",
                "preprocess",
                "numeric_environment",
                "entries",
                "models",
                "dimension_contract_version",
            )
        }
        repeat["candidate_payload_sha256"] = subject.canonical_sha256(candidate_payload)
    receipt = subject.select_level_restart(
        first,
        second,
        family="autocycle_all_core",
        level="L2",
        expected_sector_codes=first["canonical_sector_codes"],
        feature_count=20,
        feature_domain_policy_sha256="e" * 64,
    )
    assert receipt["level_selection_valid"] is False
    assert INACTIVE_DIMENSION_REASON_CODE in receipt["failure_reason_codes"]


def test_d5_mixed_dimension_rejects_projected_row_count_drift() -> None:
    first = _mixed_dimension_selection_repeat(preferred_seed=46)
    second = _mixed_dimension_selection_repeat(preferred_seed=46)
    for repeat in (first, second):
        entry = next(item for item in repeat["entries"] if item["sector_code"] == TARGET_SECTOR)
        entry["training_rows"] = 99
        entry_body = {key: value for key, value in entry.items() if key != "entry_receipt_sha256"}
        entry["entry_receipt_sha256"] = subject.canonical_sha256(entry_body)
        candidate_payload = {
            key: repeat[key]
            for key in (
                "family",
                "level",
                "schedule",
                "canonical_sector_codes",
                "feature_names",
                "preprocess",
                "numeric_environment",
                "entries",
                "models",
                "dimension_contract_version",
            )
        }
        repeat["candidate_payload_sha256"] = subject.canonical_sha256(candidate_payload)
    receipt = subject.select_level_restart(
        first,
        second,
        family="autocycle_all_core",
        level="L2",
        expected_sector_codes=first["canonical_sector_codes"],
        feature_count=20,
        feature_domain_policy_sha256="e" * 64,
    )
    assert receipt["level_selection_valid"] is False
    assert INACTIVE_DIMENSION_REASON_CODE in receipt["failure_reason_codes"]


def test_d5_mixed_dimension_keeps_nonfinite_score_as_typed_candidate_failure() -> None:
    first = _mixed_dimension_selection_repeat(preferred_seed=46)
    second = _mixed_dimension_selection_repeat(preferred_seed=46)
    for repeat in (first, second):
        entry = next(item for item in repeat["entries"] if item["seed"] == 46 and item["sector_code"] == TARGET_SECTOR)
        entry["final_train_log_likelihood"] = "nan"
        entry_body = {key: value for key, value in entry.items() if key != "entry_receipt_sha256"}
        entry["entry_receipt_sha256"] = subject.canonical_sha256(entry_body)
        candidate_payload = {
            key: repeat[key]
            for key in (
                "family",
                "level",
                "schedule",
                "canonical_sector_codes",
                "feature_names",
                "preprocess",
                "numeric_environment",
                "entries",
                "models",
                "dimension_contract_version",
            )
        }
        repeat["candidate_payload_sha256"] = subject.canonical_sha256(candidate_payload)
    receipt = subject.select_level_restart(
        first,
        second,
        family="autocycle_all_core",
        level="L2",
        expected_sector_codes=first["canonical_sector_codes"],
        feature_count=20,
        feature_domain_policy_sha256="e" * 64,
    )
    rejected = next(candidate for candidate in receipt["evidence"]["candidates"] if candidate["seed"] == 46)
    assert rejected["eligible"] is False
    assert rejected["failure_reason_codes"] == ["hmm_risk_model_selection_score_non_finite"]
    assert receipt["level_selection_valid"] is True
    assert receipt["evidence"]["selected_seed"] != 46


@pytest.mark.parametrize(("level", "count", "preferred_seed"), [("L1", 31, 46), ("L2", 131, 48)])
def test_d5_selects_one_level_global_seed_for_complete_l1_and_l2(
    level: str,
    count: int,
    preferred_seed: int,
) -> None:
    first = _selection_repeat(level=level, preferred_seed=preferred_seed)
    second = _selection_repeat(level=level, preferred_seed=preferred_seed)
    receipt = subject.select_level_restart(
        first,
        second,
        family="legacy_covfix",
        level=level,
        expected_sector_codes=[f"S{index:03d}" for index in range(count)],
        feature_count=7,
        feature_domain_policy_sha256="e" * 64,
    )

    assert receipt["level_selection_status"] == "accepted"
    assert receipt["level_selection_valid"] is True
    assert receipt["evidence"]["selected_seed"] == preferred_seed
    assert receipt["evidence"]["feature_domain_policy_sha256"] == "e" * 64
    assert receipt["evidence"]["selection_followed_by_refit"] is False


def test_d5_rejects_missing_feature_domain_policy_identity() -> None:
    first = _selection_repeat(level="L1", preferred_seed=46)
    second = _selection_repeat(level="L1", preferred_seed=46)
    receipt = subject.select_level_restart(
        first,
        second,
        family="legacy_covfix",
        level="L1",
        expected_sector_codes=[f"S{index:03d}" for index in range(31)],
        feature_count=7,
        feature_domain_policy_sha256="invalid",
    )

    assert receipt["level_selection_valid"] is False
    assert "hmm_risk_model_selection_contract_unsatisfied" in receipt["blocking_reason_codes"]


def test_d5_repeat_mismatch_and_validation_access_fail_closed() -> None:
    first = _selection_repeat(level="L1", preferred_seed=46)
    second = _selection_repeat(level="L1", preferred_seed=46)
    second["model_payload_sha256"] = "f" * 64
    first["validation_accessed"] = True
    receipt = subject.select_level_restart(
        first,
        second,
        family="legacy_covfix",
        level="L1",
        expected_sector_codes=[f"S{index:03d}" for index in range(31)],
        feature_count=7,
        feature_domain_policy_sha256="e" * 64,
    )
    assert receipt["level_selection_status"] == "failed"
    assert "hmm_risk_model_selection_repeat_mismatch" in receipt["failure_reason_codes"]
    assert "hmm_risk_model_selection_contract_unsatisfied" in receipt["failure_reason_codes"]


def test_d5_persists_compact_rejection_reasons_for_each_ineligible_candidate() -> None:
    first = _selection_repeat(level="L1", preferred_seed=46)
    second = _selection_repeat(level="L1", preferred_seed=46)
    rejected_dates = _train_dates(30)
    rejected_states = [0 if index % 2 == 0 else 1 for index in range(30)]
    rejected_states[-1] = 2
    rejected_occupancy = subject.evaluate_train_occupancy(
        _posterior(rejected_states),
        rejected_dates,
        frozen_input_manifest=_train_manifest(rejected_dates),
    )

    for repeat in (first, second):
        entry_index = next(
            index
            for index, entry in enumerate(repeat["entries"])
            if entry["seed"] == 42 and entry["sector_code"] == "S000"
        )
        entry_body = {
            key: value for key, value in repeat["entries"][entry_index].items() if key != "entry_receipt_sha256"
        }
        entry_body.update(
            train_occupancy=rejected_occupancy,
            model_entry_status="failed",
            model_entry_valid=False,
        )
        repeat["entries"][entry_index] = {
            **entry_body,
            "entry_receipt_sha256": subject.canonical_sha256(entry_body),
        }
        candidate_payload = {
            key: repeat[key]
            for key in (
                "family",
                "level",
                "schedule",
                "canonical_sector_codes",
                "feature_names",
                "preprocess",
                "numeric_environment",
                "entries",
                "models",
            )
        }
        repeat["candidate_payload_sha256"] = subject.canonical_sha256(candidate_payload)

    receipt = subject.select_level_restart(
        first,
        second,
        family="legacy_covfix",
        level="L1",
        expected_sector_codes=[f"S{index:03d}" for index in range(31)],
        feature_count=7,
        feature_domain_policy_sha256="e" * 64,
    )

    rejected_candidate = next(candidate for candidate in receipt["evidence"]["candidates"] if candidate["seed"] == 42)
    assert rejected_candidate["eligible"] is False
    assert rejected_occupancy["failure_reason_codes"]
    assert rejected_candidate["failure_reason_codes"] == rejected_occupancy["failure_reason_codes"]
    assert rejected_candidate["blocking_reason_codes"] == []
    assert rejected_candidate["primary_reason_code"] == rejected_occupancy["primary_reason_code"]
    assert rejected_candidate["rejection_summary"] == [
        {
            "sector_code": "S000",
            "entry_receipt_sha256": first["entries"][0]["entry_receipt_sha256"],
            "failed_stages": [
                {
                    "stage": "train_occupancy",
                    "status": "failed",
                    "valid": False,
                    "failure_reason_codes": rejected_candidate["failure_reason_codes"],
                    "blocking_reason_codes": [],
                    "primary_reason_code": rejected_occupancy["primary_reason_code"],
                }
            ],
        }
    ]
    assert rejected_candidate["rejection_summary_sha256"] == subject.canonical_sha256(
        rejected_candidate["rejection_summary"]
    )


def test_d5_rejection_summary_keeps_fit_failure_stage_without_inventing_model_mismatch() -> None:
    failure_body = {
        "family": "legacy_covfix",
        "level": "L1",
        "seed": 42,
        "sector_code": "S000",
        "fit_status": "failed",
        "model_entry_status": "failed",
        "model_entry_valid": False,
        "failure_stage": "fit",
        "failure_reason_codes": ["hmm_risk_model_fit_failed"],
        "blocking_reason_codes": [],
    }
    entry = {**failure_body, "entry_receipt_sha256": subject.canonical_sha256(failure_body)}

    assert subject._candidate_rejection_summary([entry]) == [
        {
            "sector_code": "S000",
            "entry_receipt_sha256": entry["entry_receipt_sha256"],
            "failed_stages": [
                {
                    "stage": "fit",
                    "status": "failed",
                    "valid": False,
                    "failure_reason_codes": ["hmm_risk_model_fit_failed"],
                    "blocking_reason_codes": [],
                    "primary_reason_code": "hmm_risk_model_fit_failed",
                }
            ],
        }
    ]


def _validation_dates() -> tuple[date, ...]:
    start = date(2024, 7, 1)
    end = date(2025, 3, 31)
    available = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            available.append(current)
        current += timedelta(days=1)
    indexes = np.linspace(0, len(available) - 1, 182, dtype=int)
    return tuple(available[index] for index in indexes)


def _utility(states: list[int]) -> dict:
    values = np.asarray([(-0.02, 0.0, 0.02)[state] for state in states], dtype=np.float64)
    return {
        "excess_return_5d": values,
        "excess_return_10d": values,
        "excess_return_20d": values,
        "source_cutoff": "2025-04-30",
        "formula_version": "hmm_risk_hard_future_excess_035_035_030_v1",
    }


def _validation_manifest(dates: tuple[date, ...], utility: dict) -> dict:
    encoded = [value.isoformat() for value in dates]
    components = {
        key: np.asarray(utility[key], dtype=np.float64)
        for key in ("excess_return_5d", "excess_return_10d", "excess_return_20d")
    }
    combined = (
        0.35 * components["excess_return_5d"]
        + 0.35 * components["excess_return_10d"]
        + 0.30 * components["excess_return_20d"]
    )
    return {
        "schema_version": "hmm_risk_d6_frozen_input_manifest_v1",
        "direct_sector_level": "L1",
        "sector_code": "S001",
        "benchmark_identity": "000300.SH",
        "validation_observation_sha256": "f" * 64,
        "validation_dates": encoded,
        "validation_dates_sha256": subject.canonical_sha256(encoded),
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "l2_stock_fact_manifest_hash": "d" * 64,
        "feature_domain_policy_sha256": "e" * 64,
        "source_cutoff": utility["source_cutoff"],
        "formula_version": utility["formula_version"],
        "utility_component_sha256": {
            key: subject.canonical_sha256(value.tolist()) for key, value in sorted(components.items())
        },
        "combined_utility_sha256": subject.canonical_sha256(combined.tolist()),
    }


def _calendar_carrier_and_manifest(
    dates: tuple[date, ...],
    utility: dict,
    *,
    observation_missing: tuple[int, ...] = (),
    utility_missing: tuple[int, ...] = (),
) -> tuple[subject.D6ValidationCalendarSeries, dict]:
    observation_mask = tuple(index not in observation_missing for index in range(len(dates)))
    observation_positions = tuple(index for index, available in enumerate(observation_mask) if available)
    observation_values = np.asarray([[float(index)] for index in observation_positions], dtype=np.float64)
    component_masks = {
        name: tuple(index not in utility_missing for index in range(len(dates)))
        for name in ("excess_return_5d", "excess_return_10d", "excess_return_20d")
    }
    component_positions = {
        name: tuple(index for index, available in enumerate(mask) if available)
        for name, mask in component_masks.items()
    }
    component_values = {
        name: np.asarray(utility[name], dtype=np.float64)[np.asarray(mask, dtype=bool)]
        for name, mask in component_masks.items()
    }
    utility_mask = tuple(index not in utility_missing for index in range(len(dates)))
    utility_positions = tuple(index for index, available in enumerate(utility_mask) if available)
    combined_dense = (
        0.35 * np.asarray(utility["excess_return_5d"], dtype=np.float64)
        + 0.35 * np.asarray(utility["excess_return_10d"], dtype=np.float64)
        + 0.30 * np.asarray(utility["excess_return_20d"], dtype=np.float64)
    )
    source_identities = {
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "l2_stock_fact_manifest_hash": "d" * 64,
        "feature_domain_policy_sha256": "e" * 64,
    }
    ledger = tuple(
        {
            "date": day.isoformat(),
            "position": index,
            "observation_available": observation_mask[index],
            "utility_available": utility_mask[index],
            "mode": "emission_update" if observation_mask[index] else "transition_only",
            "evidence_included": bool(observation_mask[index] and utility_mask[index]),
            "missing_feature_names": [] if observation_mask[index] else ["feature_1"],
            "missing_component_names": (
                [] if utility_mask[index] else ["excess_return_10d", "excess_return_20d", "excess_return_5d"]
            ),
            "observation_unavailable_reason_codes": (
                [] if observation_mask[index] else ["hmm_risk_semantic_validation_observation_unavailable"]
            ),
            "utility_unavailable_reason_codes": (
                [] if utility_mask[index] else ["hmm_risk_semantic_validation_utility_unavailable"]
            ),
            "observation_source_receipt": {
                "sector_code": "S001",
                "date": dates[index].isoformat(),
                "feature_names": ["feature_1"],
                "missing_feature_names": [] if observation_mask[index] else ["feature_1"],
                "available": observation_mask[index],
                "source_identities": source_identities,
            },
            "observation_source_receipt_sha256": subject.canonical_sha256(
                {
                    "sector_code": "S001",
                    "date": dates[index].isoformat(),
                    "feature_names": ["feature_1"],
                    "missing_feature_names": [] if observation_mask[index] else ["feature_1"],
                    "available": observation_mask[index],
                    "source_identities": source_identities,
                }
            ),
            "utility_source_receipt": {
                "sector_code": "S001",
                "date": dates[index].isoformat(),
                "component_names": ["excess_return_10d", "excess_return_20d", "excess_return_5d"],
                "missing_component_names": (
                    [] if utility_mask[index] else ["excess_return_10d", "excess_return_20d", "excess_return_5d"]
                ),
                "available": utility_mask[index],
                "source_identities": source_identities,
            },
            "utility_source_receipt_sha256": subject.canonical_sha256(
                {
                    "sector_code": "S001",
                    "date": dates[index].isoformat(),
                    "component_names": ["excess_return_10d", "excess_return_20d", "excess_return_5d"],
                    "missing_component_names": (
                        [] if utility_mask[index] else ["excess_return_10d", "excess_return_20d", "excess_return_5d"]
                    ),
                    "available": utility_mask[index],
                    "source_identities": source_identities,
                }
            ),
        }
        for index, day in enumerate(dates)
    )
    carrier = subject.D6ValidationCalendarSeries(
        calendar_dates=dates,
        feature_names=("feature_1",),
        observation_available_mask=observation_mask,
        observation_available_positions=observation_positions,
        observation_values_f64=observation_values,
        component_available_masks=component_masks,
        component_available_positions=component_positions,
        component_values_f64=component_values,
        utility_available_mask=utility_mask,
        utility_available_positions=utility_positions,
        combined_utility_values_f64=combined_dense[np.asarray(utility_mask, dtype=bool)],
        availability_ledger=ledger,
        source_identities=source_identities,
    )
    payload = carrier.payload()
    manifest = {
        **source_identities,
        "schema_version": "hmm_risk_d6_frozen_input_manifest_v2",
        "direct_sector_level": "L1",
        "sector_code": "S001",
        "benchmark_identity": "000300.SH",
        "calendar_carrier_schema_version": carrier.schema_version,
        "calendar_carrier_payload": payload,
        "calendar_carrier_sha256": carrier.carrier_sha256,
        "validation_calendar_sha256": subject.canonical_sha256(payload["calendar_dates"]),
        "feature_names_sha256": subject.canonical_sha256(payload["feature_names"]),
        "observation_available_mask_sha256": subject.canonical_sha256(payload["observation_available_mask"]),
        "observation_available_positions_sha256": subject.canonical_sha256(payload["observation_available_positions"]),
        "observation_values_sha256": subject.canonical_sha256(payload["observation_values_f64"]),
        "utility_component_sha256": {
            name: subject.canonical_sha256(values) for name, values in payload["component_values_f64"].items()
        },
        "component_available_mask_sha256": {
            name: subject.canonical_sha256(values) for name, values in payload["component_available_masks"].items()
        },
        "component_available_positions_sha256": {
            name: subject.canonical_sha256(values) for name, values in payload["component_available_positions"].items()
        },
        "utility_available_mask_sha256": subject.canonical_sha256(payload["utility_available_mask"]),
        "utility_available_positions_sha256": subject.canonical_sha256(payload["utility_available_positions"]),
        "combined_utility_sha256": subject.canonical_sha256(payload["combined_utility_values_f64"]),
        "availability_ledger_sha256": subject.canonical_sha256(payload["availability_ledger"]),
        "source_cutoff": utility["source_cutoff"],
        "formula_version": utility["formula_version"],
    }
    return carrier, manifest


def test_d6_na_calendar_excludes_missing_days_without_turning_events_into_failures() -> None:
    states = [(index // 3) % 3 for index in range(182)]
    dates = _validation_dates()
    utility = _utility(states)
    carrier, manifest = _calendar_carrier_and_manifest(
        dates,
        utility,
        observation_missing=(4,),
        utility_missing=(10,),
    )
    posterior = _posterior(states)
    posterior[4] = np.asarray([0.5, 0.5, 0.0])

    receipt = subject.evaluate_semantic_validation_calendar(
        posterior,
        carrier,
        frozen_input_manifest=manifest,
        selected_model_payload_sha256="f" * 64,
    )

    assert receipt["contract_version"] == subject.D6_NA_SEMANTIC_VERSION
    assert receipt["assignment"]["semantic_assignment_valid"] is True
    assert receipt["semantic_evidence"]["semantic_evidence_valid"] is True
    assert receipt["assignment"]["evidence"]["evidence_rows"] == 180
    assert 4 in receipt["assignment"]["evidence"]["diagnostic_tie_positions"]
    assert receipt["assignment"]["failure_reason_codes"] == []
    assert receipt["assignment"]["blocking_reason_codes"] == []
    assert receipt["semantic_evidence"]["failure_reason_codes"] == []
    assert len(receipt["assignment"]["evidence"]["availability_events"]) == 2


def test_d6_na_calendar_fails_evidence_at_29_rows_and_rejects_manifest_drift() -> None:
    states = [(index // 3) % 3 for index in range(182)]
    dates = _validation_dates()
    utility = _utility(states)
    missing = tuple(range(29, 182))
    carrier, manifest = _calendar_carrier_and_manifest(dates, utility, observation_missing=missing)
    receipt = subject.evaluate_semantic_validation_calendar(
        _posterior(states),
        carrier,
        frozen_input_manifest=manifest,
        selected_model_payload_sha256="f" * 64,
    )
    assert (
        "hmm_risk_semantic_validation_evidence_rows_insufficient"
        in receipt["semantic_evidence"]["failure_reason_codes"]
    )
    assert receipt["semantic_mapping"] is None

    drifted = dict(manifest)
    drifted["calendar_carrier_sha256"] = "0" * 64
    mismatch = subject.evaluate_semantic_validation_calendar(
        _posterior(states),
        carrier,
        frozen_input_manifest=drifted,
        selected_model_payload_sha256="f" * 64,
    )
    assert (
        "hmm_risk_semantic_validation_availability_receipt_mismatch" in mismatch["assignment"]["failure_reason_codes"]
    )

    payload = deepcopy(carrier.payload())
    source_receipt = payload["availability_ledger"][0]["observation_source_receipt"]
    source_receipt["available"] = False
    payload["availability_ledger"][0]["observation_source_receipt_sha256"] = subject.canonical_sha256(source_receipt)
    with pytest.raises(subject.StateModelSetError, match="source receipt content differs"):
        subject.D6ValidationCalendarSeries.from_payload(payload)


def test_d6_hard_semantic_mapping_is_post_selection_and_rejects_singleton_without_fallback() -> None:
    states = [(index // 3) % 3 for index in range(182)]
    dates = _validation_dates()
    utility = _utility(states)
    accepted = subject.evaluate_semantic_validation(
        _posterior(states),
        dates,
        utility,
        frozen_input_manifest=_validation_manifest(dates, utility),
        selected_model_payload_sha256="e" * 64,
    )
    singleton = [index % 2 for index in range(182)]
    singleton[-1] = 2
    rejected = subject.evaluate_semantic_validation(
        _posterior(singleton),
        dates,
        _utility(singleton),
        frozen_input_manifest=_validation_manifest(dates, _utility(singleton)),
        selected_model_payload_sha256="e" * 64,
    )

    assert accepted["assignment"]["semantic_assignment_valid"] is True
    assert accepted["semantic_evidence"]["semantic_evidence_valid"] is True
    assert accepted["semantic_mapping"] == {"0": "fading", "1": "neutral", "2": "trending"}
    assert rejected["semantic_mapping"] is None
    assert (
        "hmm_risk_semantic_validation_state_count_insufficient" in rejected["semantic_evidence"]["failure_reason_codes"]
    )


def test_d6_preserves_valid_assignment_when_utility_evidence_is_missing() -> None:
    states = [(index // 3) % 3 for index in range(182)]
    dates = _validation_dates()
    utility = _utility(states)
    receipt = subject.evaluate_semantic_validation(
        _posterior(states),
        dates,
        None,
        frozen_input_manifest=_validation_manifest(dates, utility),
        selected_model_payload_sha256="e" * 64,
    )
    assert receipt["assignment"]["semantic_assignment_valid"] is True
    assert receipt["semantic_evidence"]["semantic_evidence_status"] == "insufficient_evidence"
    assert receipt["semantic_mapping"] is None


def test_d5_score_uses_actual_terminal_likelihood_not_history_maximum() -> None:
    receipt = subject.evaluate_likelihood_acceptance(_monitor([-10.0, -9.0, -9.00001]))
    assert receipt["evidence"]["raw_likelihood_history"][-1] == -9.00001
    assert math.isfinite(receipt["evidence"]["raw_likelihood_deltas"][-1]["relative"])
