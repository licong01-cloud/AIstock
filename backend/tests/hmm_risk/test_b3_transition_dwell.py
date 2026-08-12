from __future__ import annotations

import numpy as np
import pytest

from backend.services.hmm_risk.b3_acceptance import evaluate_likelihood_acceptance
from backend.services.hmm_risk.b3_transition_dwell import (
    CONTRACT_VERSION,
    REASON_MATRIX_INVALID,
    REASON_PRIOR_INVALID,
    TransitionDwellContractError,
    assert_target_scope,
    build_transition_prior,
    transition_map_objective,
)
from backend.services.hmm_risk.state_model_set import canonical_sha256
from backend.services.hmm_risk import b3_training as training_subject


def test_transition_prior_uses_exact_clipped_self_center_and_preserves_off_diagonal_ratios() -> None:
    counts = np.asarray([[99, 1, 0], [1, 1, 18], [7, 3, 0]], dtype=np.int64)

    receipt = build_transition_prior(counts)

    center = np.asarray(receipt["transition_prior_center"], dtype=np.float64)
    prior = np.asarray(receipt["transmat_prior"], dtype=np.float64)
    assert receipt["contract_version"] == CONTRACT_VERSION
    assert center[0, 0] == pytest.approx(0.9)
    assert center[1, 1] == pytest.approx(0.5)
    assert center[2, 2] == pytest.approx(0.5)
    assert center[0, 1] / center[0, 2] == pytest.approx((1.0 + 0.1) / (0.0 + 0.1))
    assert np.allclose(center.sum(axis=1), 1.0, atol=1e-12, rtol=0.0)
    assert np.array_equal(prior, 1.0 + 8.0 * center)
    body = {key: value for key, value in receipt.items() if key != "transition_prior_sha256"}
    assert receipt["transition_prior_sha256"] == canonical_sha256(body)
    assert receipt["validation_accessed"] is False
    assert receipt["future_utility_accessed"] is False


def test_likelihood_readback_rejects_tampered_transition_prior_even_with_self_consistent_outer_values() -> None:
    receipt = build_transition_prior([[90, 9, 1], [2, 90, 8], [5, 5, 90]])
    matrix = np.asarray([[0.8, 0.15, 0.05], [0.1, 0.8, 0.1], [0.05, 0.15, 0.8]])
    component = transition_map_objective(matrix, receipt["transmat_prior"])
    component["transmat_prior"][0][0] += 1.0
    transition_adjustment = component["transition_prior_adjustment"]
    prior_adjustment = -10.0 + transition_adjustment
    evidence = {
        "authority": "covariance_and_transition_prior_map_objective",
        "maximum_iterations": 300,
        "raw_likelihood_history": [-100.0, -99.0],
        "map_objective_history": [-100.0 + prior_adjustment, -99.0 + prior_adjustment - 1.0],
        "map_prior_adjustment_history": [prior_adjustment, prior_adjustment - 1.0],
        "objective_component_history": [
            {
                **component,
                "iteration": index + 1,
                "raw_log_likelihood": (-100.0, -99.0)[index],
                "prior_log_covariance_component": (20.0, 22.0)[index],
                "prior_inverse_covariance_component": 0.0,
                "covariance_prior_adjustment": (-10.0, -11.0)[index],
                "prior_adjustment": (prior_adjustment, prior_adjustment - 1.0)[index],
                "map_objective": (-100.0 + prior_adjustment, -99.0 + prior_adjustment - 1.0)[index],
            }
            for index in range(2)
        ],
        "covariance_valid_history": [True, True],
        "covariance_receipt_sha256_history": ["a" * 64, "b" * 64],
        "joint_stop_iteration": 2,
        "raw_likelihood_is_diagnostic_only": True,
        "postfit_projection_performed": False,
    }
    result = evaluate_likelihood_acceptance(evidence)
    assert result["convergence_valid"] is False
    assert "hmm_risk_model_map_objective_non_finite" in result["failure_reason_codes"]


@pytest.mark.parametrize(
    "counts",
    [
        [[1, 2], [3, 4]],
        [[1, 0, 0], [0, -1, 0], [0, 0, 1]],
        [[1, 0, 0], [0, float("nan"), 0], [0, 0, 1]],
        [[1.5, 0, 0], [0, 1, 0], [0, 0, 1]],
    ],
)
def test_transition_prior_fails_closed_on_invalid_counts(counts) -> None:
    with pytest.raises(TransitionDwellContractError) as exc_info:
        build_transition_prior(counts)
    assert exc_info.value.reason_code == REASON_PRIOR_INVALID


def test_transition_map_component_extends_map_authority_without_changing_raw_likelihood() -> None:
    prior_receipt = build_transition_prior([[90, 9, 1], [2, 90, 8], [5, 5, 90]])
    matrix = np.asarray([[0.8, 0.15, 0.05], [0.1, 0.8, 0.1], [0.05, 0.15, 0.8]])
    component = transition_map_objective(matrix, prior_receipt["transmat_prior"])
    transition_adjustment = component["transition_prior_adjustment"]
    raw = [-100.0, -99.0]
    covariance_adjustments = [-10.0, -11.0]
    total = [value + transition_adjustment for value in covariance_adjustments]
    maps = [value + adjustment for value, adjustment in zip(raw, total, strict=True)]
    components = [
        {
            **component,
            "iteration": index + 1,
            "raw_log_likelihood": raw[index],
            "prior_log_covariance_component": 20.0 + 2.0 * index,
            "prior_inverse_covariance_component": 0.0,
            "covariance_prior_adjustment": covariance_adjustments[index],
            "transition_prior_adjustment": transition_adjustment,
            "prior_adjustment": total[index],
            "map_objective": maps[index],
        }
        for index in range(2)
    ]
    evidence = {
        "authority": "covariance_and_transition_prior_map_objective",
        "maximum_iterations": 300,
        "raw_likelihood_history": raw,
        "map_objective_history": maps,
        "map_prior_adjustment_history": total,
        "objective_component_history": components,
        "covariance_valid_history": [True, True],
        "covariance_receipt_sha256_history": ["a" * 64, "b" * 64],
        "joint_stop_iteration": 2,
        "raw_likelihood_is_diagnostic_only": True,
        "postfit_projection_performed": False,
    }

    accepted = evaluate_likelihood_acceptance(evidence)

    assert accepted["convergence_valid"] is True
    assert accepted["likelihood_valid"] is True
    assert accepted["evidence"]["raw_likelihood_history"] == raw


def test_transition_matrix_and_dwell_fail_closed_without_fallback() -> None:
    prior = build_transition_prior(np.eye(3, dtype=np.int64))["transmat_prior"]
    with pytest.raises(TransitionDwellContractError) as zero_info:
        transition_map_objective([[1.0, 0.0, 0.0], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]], prior)
    assert zero_info.value.reason_code == REASON_MATRIX_INVALID

    almost_absorbing = np.asarray(
        [[1.0, np.finfo(np.float64).tiny, np.finfo(np.float64).tiny], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]
    )
    almost_absorbing[0] /= almost_absorbing[0].sum()
    with pytest.raises(TransitionDwellContractError) as dwell_info:
        transition_map_objective(almost_absorbing, prior)
    assert dwell_info.value.reason_code == REASON_MATRIX_INVALID


def test_transition_dwell_scope_is_level_local_and_never_per_sector() -> None:
    assert_target_scope(family="autocycle_all_core", level="L2")
    for family, level in (("autocycle_all_core", "L1"), ("legacy_covfix", "L2")):
        with pytest.raises(TransitionDwellContractError) as exc_info:
            assert_target_scope(family=family, level=level)
        assert exc_info.value.reason_code == REASON_PRIOR_INVALID


def test_level_repeat_passes_transition_contract_to_every_fit_and_never_selects(monkeypatch) -> None:
    monkeypatch.setattr(
        training_subject,
        "c008_b3_diag04_fixed_numeric_environment",
        lambda: {"packages": {"hmmlearn": "0.3.3"}},
    )
    monkeypatch.setattr(
        training_subject,
        "_fit_preprocess",
        lambda series, preprocess_family: {"family": preprocess_family},
    )
    calls = []

    class _Item:
        train_observations = np.ones((120, 20))

        def validate(self, feature_count):
            assert feature_count == 20

    def fake_fit(item, **kwargs):
        del item
        calls.append(kwargs)
        raise training_subject.B3TrainingStageError(
            "covariance",
            "hmm_risk_model_covariance_invalid",
            ValueError("expected candidate failure"),
        )

    monkeypatch.setattr(training_subject, "_fit_b3_train_only", fake_fit)
    series = {f"L2-{index:03d}": _Item() for index in range(131)}

    repeat, models = training_subject.run_level_repeat(
        series,
        family="autocycle_all_core",
        level="L2",
        feature_names=training_subject.ALL_CORE_FEATURES,
        preprocess_family="winsor_zscore_1_99_train_global_v1",
        process_identity="fresh_process_1",
        transition_dwell_b=True,
    )

    assert len(calls) == 1048
    assert all(call["transition_dwell_b"] is True for call in calls)
    assert repeat["transition_dwell_contract"] == CONTRACT_VERSION
    assert repeat["selection_performed"] is False
    assert repeat["d6_status_accessed"] is False
    assert models == {}


def test_default_parameter_profile_and_repeat_do_not_adopt_transition_contract(monkeypatch) -> None:
    profile = training_subject.formal_b3_parameter_profile()
    assert profile["convergence_authority"] == "covariance_prior_map_objective_with_d4_02_joint_stop"
    assert "transition_contract" not in profile

    monkeypatch.setattr(
        training_subject,
        "c008_b3_diag04_fixed_numeric_environment",
        lambda: {"packages": {"hmmlearn": "0.3.3"}},
    )
    monkeypatch.setattr(training_subject, "_fit_preprocess", lambda *args, **kwargs: {})

    class _Item:
        train_observations = np.ones((120, 7))

        def validate(self, feature_count):
            assert feature_count == 7

    calls = []

    def fake_fit(item, **kwargs):
        del item
        calls.append(kwargs)
        raise training_subject.B3TrainingStageError(
            "covariance", "hmm_risk_model_covariance_invalid", ValueError("expected")
        )

    monkeypatch.setattr(training_subject, "_fit_b3_train_only", fake_fit)
    repeat, _ = training_subject.run_level_repeat(
        {f"L1-{index:02d}": _Item() for index in range(31)},
        family="legacy_covfix",
        level="L1",
        feature_names=training_subject.BASE_FEATURES,
        preprocess_family="identity",
        process_identity="fresh_process_1",
    )
    assert all(call["transition_dwell_b"] is False for call in calls)
    assert "transition_dwell_contract" not in repeat


def test_transition_model_identity_closes_prior_hash_into_model_payload() -> None:
    prior = build_transition_prior([[90, 9, 1], [2, 90, 8], [5, 5, 90]])
    fitted = training_subject.B3FittedModel(
        family="autocycle_all_core",
        level="L2",
        seed=42,
        sector_code="S001",
        feature_names=training_subject.ALL_CORE_FEATURES,
        preprocess={},
        startprob=np.full(3, 1 / 3),
        transmat=np.asarray([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
        means=np.zeros((3, 19)),
        covars=np.ones((3, 19)),
        parameter_profile_sha256="a" * 64,
        numeric_environment_sha256="b" * 64,
        observation_manifest_hash="c" * 64,
        pit_constituent_manifest_hash="d" * 64,
        model_payload_sha256="e" * 64,
        transition_dwell_contract=CONTRACT_VERSION,
        transition_prior_sha256=prior["transition_prior_sha256"],
    )
    payload = fitted.payload()
    assert payload["transition_dwell_contract"] == CONTRACT_VERSION
    assert payload["transition_prior_sha256"] == prior["transition_prior_sha256"]
