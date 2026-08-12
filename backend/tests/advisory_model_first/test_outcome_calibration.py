from __future__ import annotations

import numpy as np
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.outcome_calibration import (
    apply_path_upper_adjustment,
    apply_platt_calibrator,
    apply_return_interval_adjustment,
    expected_calibration_error,
    fit_path_upper_adjustment,
    fit_platt_calibrator,
    fit_return_interval_adjustment,
)


def test_platt_calibration_preserves_raw_identity_and_produces_finite_parameters() -> None:
    margin = np.asarray([-2.0, -1.0, -0.2, 0.3, 1.0, 2.0])
    raw = 1.0 / (1.0 + np.exp(-margin))
    truth = np.asarray([0, 0, 1, 0, 1, 1])

    result = fit_platt_calibrator(
        head="positive_excess_h5",
        raw_margin=margin,
        raw_probability=raw,
        truth=truth,
    )

    calibrated = apply_platt_calibrator(
        raw_margin=margin,
        coefficient=result.coefficient or 0.0,
        intercept=result.intercept or 0.0,
    )
    assert result.state == "CALIBRATED"
    assert result.reason_code is None
    assert result.positive_count == 3
    assert result.solver["library"] == "scikit-learn"
    assert result.solver["estimator"] == "LogisticRegression"
    assert result.solver["solver"] == "lbfgs"
    assert result.solver["library_version"]
    assert result.iteration_count > 0
    assert result.convergence_state == "CONVERGED"
    assert np.array_equal(raw, 1.0 / (1.0 + np.exp(-margin)))
    assert calibrated.shape == raw.shape
    assert np.isfinite(calibrated).all()
    assert (calibrated >= 0.0).all() and (calibrated <= 1.0).all()
    assert result.validation_metrics["raw"]["row_count"] == 6
    assert result.validation_metrics["calibrated"]["ece_10_bin"]["bin_count"] == 10


def test_platt_uses_the_frozen_unregularized_solver_contract(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Estimator:
        coef_ = np.asarray([[1.0]])
        intercept_ = np.asarray([0.0])
        n_iter_ = np.asarray([1])

        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def fit(self, _matrix, _truth) -> None:
            return None

    monkeypatch.setattr(
        "backend.services.advisory_model_first.outcome_calibration.LogisticRegression",
        _Estimator,
    )
    margin = np.asarray([-1.0, 0.0, 1.0])
    fit_platt_calibrator(
        head="positive_excess_h1",
        raw_margin=margin,
        raw_probability=1.0 / (1.0 + np.exp(-margin)),
        truth=np.asarray([0, 0, 1]),
    )

    assert captured == {
        "penalty": None,
        "solver": "lbfgs",
        "fit_intercept": True,
        "max_iter": 1000,
        "random_state": 20260812,
    }


def test_platt_single_class_is_explicitly_uncalibrated_without_constant_substitute() -> None:
    result = fit_platt_calibrator(
        head="signal_survival_h1",
        raw_margin=np.asarray([-1.0, 0.0, 1.0]),
        raw_probability=np.asarray([0.2, 0.5, 0.8]),
        truth=np.asarray([1, 1, 1]),
    )

    assert result.state == "UNCALIBRATED"
    assert result.coefficient is None
    assert result.intercept is None
    assert result.reason_code == "ADVISORY_OUTCOME_CALIBRATION_CLASS_VARIATION_MISSING"
    assert result.iteration_count == 0
    assert result.convergence_state == "NOT_FITTED_CLASS_VARIATION_MISSING"
    assert result.validation_metrics["calibrated"] is None


def test_platt_negative_slope_is_uncalibrated_instead_of_reversing_probability_order() -> None:
    margin = np.asarray([-2.0, -1.0, 0.0, 1.0, 2.0])
    raw = 1.0 / (1.0 + np.exp(-margin))

    result = fit_platt_calibrator(
        head="positive_excess_h5",
        raw_margin=margin,
        raw_probability=raw,
        truth=np.asarray([1, 1, 1, 0, 0]),
    )

    assert result.state == "UNCALIBRATED"
    assert result.coefficient is None
    assert result.intercept is None
    assert result.reason_code == "ADVISORY_OUTCOME_CALIBRATION_ORDER_REVERSAL"
    assert result.iteration_count > 0
    assert result.convergence_state == "CONVERGED_ORDER_REVERSAL"
    assert result.validation_metrics["raw"]["row_count"] == 5
    assert result.validation_metrics["calibrated"] is None


def test_apply_platt_rejects_non_positive_slope() -> None:
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        apply_platt_calibrator(
            raw_margin=np.asarray([-1.0, 1.0]),
            coefficient=0.0,
            intercept=0.0,
        )

    assert exc_info.value.reason_code == "ADVISORY_OUTCOME_CALIBRATION_FAILED"


def test_ece_uses_fixed_ten_bins_and_includes_probability_one_in_last_bin() -> None:
    result = expected_calibration_error(
        np.asarray([0, 1, 1]),
        np.asarray([0.0, 0.55, 1.0]),
        bin_count=10,
    )

    assert len(result["bins"]) == 10
    assert result["bins"][0]["count"] == 1
    assert result["bins"][5]["count"] == 1
    assert result["bins"][9]["count"] == 1


def test_return_conformal_adjustment_is_finite_sample_nonnegative_and_monotonic() -> None:
    q10 = np.asarray([0.0, 0.3, -0.1, 0.2])
    q50 = np.asarray([0.1, 0.2, 0.0, 0.4])
    q90 = np.asarray([0.2, 0.1, 0.1, 0.3])
    truth = np.asarray([0.15, 0.5, -0.2, 0.35])

    spec = fit_return_interval_adjustment(q10=q10, q50=q50, q90=q90, truth=truth)
    lower, middle, upper = apply_return_interval_adjustment(
        q10=q10,
        q50=q50,
        q90=q90,
        delta=spec["delta"],
    )

    assert spec["delta"] == pytest.approx(0.2)
    assert (lower <= middle).all() and (middle <= upper).all()
    assert spec["validation_metrics"]["calibrated_coverage"] >= spec["validation_metrics"]["raw_coverage"]


def test_path_calibration_only_expands_existing_q90_upper_bound() -> None:
    q50 = np.asarray([0.2, -0.1, 0.4, 0.3])
    q90 = np.asarray([0.1, 0.3, 0.5, 0.2])
    truth = np.asarray([0.25, 0.4, 0.8, 0.35])

    spec = fit_path_upper_adjustment(q50=q50, q90=q90, truth=truth)
    median, upper = apply_path_upper_adjustment(q50=q50, q90=q90, delta=spec["delta"])

    assert spec["method"] == "CONFORMAL_UPPER_90_NONNEGATIVE_EXPANSION"
    assert spec["nominal_coverage"] == 0.9
    assert (median >= 0.0).all()
    assert (upper >= median).all()


def test_nonfinite_calibration_input_fails_loudly() -> None:
    with pytest.raises(AdvisoryModelFirstError) as error:
        fit_return_interval_adjustment(
            q10=np.asarray([0.0, np.nan]),
            q50=np.asarray([0.1, 0.2]),
            q90=np.asarray([0.2, 0.3]),
            truth=np.asarray([0.1, 0.2]),
        )
    assert error.value.reason_code == "ADVISORY_OUTCOME_CALIBRATION_FAILED"
