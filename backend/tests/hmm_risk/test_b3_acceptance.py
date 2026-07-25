from __future__ import annotations

import math
from datetime import date, timedelta

import numpy as np
import pytest

from backend.services.hmm_risk import b3_acceptance as subject


def _monitor(history: list[float], *, converged: bool = True) -> dict:
    return {
        "converged": converged,
        "iterations": len(history),
        "maximum_iterations": 300,
        "history": history,
    }


def test_d4_01_keeps_terminal_warning_distinct_and_rejects_nonterminal_decrease() -> None:
    warning = subject.evaluate_likelihood_acceptance(_monitor([-100.0, -90.0, -90.001]))
    assert warning["status"] == "accepted_with_warning"
    assert warning["valid"] is True
    assert warning["warning_reason_codes"] == ["hmm_risk_model_likelihood_terminal_decrease_warning"]

    failure = subject.evaluate_likelihood_acceptance(_monitor([-100.0, -100.0001, -99.0]))
    assert failure["status"] == "failed"
    assert failure["valid"] is False
    assert "hmm_risk_model_likelihood_nonterminal_decrease" in failure["failure_reason_codes"]


def test_d4_01_exact_terminal_boundaries_and_missing_evidence_fail_closed() -> None:
    accepted = subject.evaluate_likelihood_acceptance(_monitor([0.0, float(np.nextafter(0.01, 0.0))]))
    rejected = subject.evaluate_likelihood_acceptance(_monitor([0.0, 0.01]))
    boundary_warning = subject.evaluate_likelihood_acceptance(_monitor([0.0, -2e-5]))
    below_warning = subject.evaluate_likelihood_acceptance(_monitor([0.0, float(np.nextafter(-2e-5, -1.0))]))
    missing = subject.evaluate_likelihood_acceptance(None)

    assert accepted["status"] == "accepted"
    assert "hmm_risk_model_likelihood_tolerance_failed" in rejected["failure_reason_codes"]
    assert boundary_warning["status"] == "accepted_with_warning"
    assert "hmm_risk_model_likelihood_tolerance_failed" in below_warning["failure_reason_codes"]
    assert missing["status"] == "insufficient_evidence"


def test_d4_numeric_evidence_non_finite_is_failed_not_missing_or_serialized_as_null_success() -> None:
    likelihood = subject.evaluate_likelihood_acceptance(_monitor([-1.0, float("nan")]))
    covariance_input = _covariance_evidence()
    covariance_input["raw_covars"][0][0] = float("inf")
    covariance = subject.evaluate_covariance_acceptance(covariance_input)

    assert likelihood["status"] == "failed"
    assert "hmm_risk_model_likelihood_non_finite" in likelihood["failure_reason_codes"]
    assert covariance["status"] == "failed"
    assert "hmm_risk_model_covariance_invalid" in covariance["failure_reason_codes"]


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

    assert accepted["status"] == "accepted"
    assert accepted["valid"] is True
    assert "hmm_risk_model_covariance_bounds_failed" in bounded["failure_reason_codes"]
    assert "hmm_risk_model_covariance_acceptance_failed" in residual["failure_reason_codes"]
    assert "hmm_risk_model_covariance_acceptance_failed" in projected["failure_reason_codes"]


def _train_dates(count: int) -> tuple[date, ...]:
    return tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(count))


def _posterior(states: list[int]) -> np.ndarray:
    result = np.zeros((len(states), 3), dtype=np.float64)
    result[np.arange(len(states)), states] = 1.0
    return result


def test_d4_03_anti_singleton_train_contract() -> None:
    accepted_states = [index % 3 for index in range(30)]
    accepted = subject.evaluate_train_occupancy(_posterior(accepted_states), _train_dates(30))
    singleton_states = [0 if index % 2 == 0 else 1 for index in range(30)]
    singleton_states[-1] = 2
    rejected = subject.evaluate_train_occupancy(_posterior(singleton_states), _train_dates(30))

    assert accepted["status"] == "accepted"
    assert accepted["evidence"]["validation_accessed"] is False
    assert accepted["evidence"]["future_utility_accessed"] is False
    assert "hmm_risk_model_train_state_count_insufficient" in rejected["failure_reason_codes"]
    assert "hmm_risk_model_train_month_coverage_insufficient" in rejected["failure_reason_codes"]


def _selection_repeat(*, level: str, preferred_seed: int) -> dict:
    count = 31 if level == "L1" else 131
    codes = [f"S{index:03d}" for index in range(count)]
    entries = []
    for seed in subject.RESTART_SCHEDULE:
        score = -1.0 if seed == preferred_seed else -2.0 - 0.01 * (seed - 42)
        for code in codes:
            entries.append(
                {
                    "family": "legacy_covfix",
                    "level": level,
                    "seed": seed,
                    "sector_code": code,
                    "fit_status": "accepted",
                    "likelihood": {"valid": True, "status": "accepted_with_warning" if seed == 42 else "accepted"},
                    "covariance": {"valid": True},
                    "train_occupancy": {"valid": True},
                    "final_train_log_likelihood": score * 100 * 7,
                    "training_rows": 100,
                    "feature_count": 7,
                    "entry_receipt_sha256": f"{seed:02d}{code}".ljust(64, "0")[:64],
                }
            )
    repeat = {
        "family": "legacy_covfix",
        "level": level,
        "schedule": list(subject.RESTART_SCHEDULE),
        "canonical_sector_codes": codes,
        "feature_names": [f"f{index}" for index in range(7)],
        "preprocess": {"family": "identity"},
        "numeric_environment": {"scope": "test"},
        "entries": entries,
        "models": [],
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
    repeat["model_payload_sha256"] = subject.canonical_sha256([])
    repeat["candidate_payload_sha256"] = subject.canonical_sha256(candidate_payload)
    return repeat


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
    )

    assert receipt["level_selection_status"] == "accepted"
    assert receipt["level_selection_valid"] is True
    assert receipt["evidence"]["selected_seed"] == preferred_seed
    assert receipt["evidence"]["selection_followed_by_refit"] is False


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
    )
    assert receipt["level_selection_status"] == "failed"
    assert "hmm_risk_model_repeat_hash_mismatch" in receipt["failure_reason_codes"]
    assert "hmm_risk_model_selection_contract_unsatisfied" in receipt["failure_reason_codes"]


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


def test_d6_hard_semantic_mapping_is_post_selection_and_rejects_singleton_without_fallback() -> None:
    states = [(index // 3) % 3 for index in range(182)]
    accepted = subject.evaluate_semantic_validation(_posterior(states), _validation_dates(), _utility(states))
    singleton = [index % 2 for index in range(182)]
    singleton[-1] = 2
    rejected = subject.evaluate_semantic_validation(
        _posterior(singleton),
        _validation_dates(),
        _utility(singleton),
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
    receipt = subject.evaluate_semantic_validation(_posterior(states), _validation_dates(), None)
    assert receipt["assignment"]["semantic_assignment_valid"] is True
    assert receipt["semantic_evidence"]["semantic_evidence_status"] == "insufficient_evidence"
    assert receipt["semantic_mapping"] is None


def test_d5_score_uses_actual_terminal_likelihood_not_history_maximum() -> None:
    receipt = subject.evaluate_likelihood_acceptance(_monitor([-10.0, -9.0, -9.00001]))
    assert receipt["evidence"]["history"][-1] == -9.00001
    assert math.isfinite(receipt["evidence"]["deltas"][-1]["relative"])
