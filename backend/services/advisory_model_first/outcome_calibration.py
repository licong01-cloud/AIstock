from __future__ import annotations

import math
from importlib.metadata import version
from dataclasses import dataclass
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

PLATT_SOLVER_CONTRACT = {
    "library": "scikit-learn",
    "estimator": "LogisticRegression",
    "penalty": None,
    "solver": "lbfgs",
    "fit_intercept": True,
    "max_iter": 1000,
    "random_state": 20260812,
}


@dataclass(frozen=True)
class PlattCalibrationResult:
    state: str
    head: str
    row_count: int
    positive_count: int
    negative_count: int
    coefficient: float | None
    intercept: float | None
    reason_code: str | None
    solver: dict[str, Any]
    iteration_count: int
    convergence_state: str
    validation_metrics: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "head": self.head,
            "row_count": self.row_count,
            "positive_count": self.positive_count,
            "negative_count": self.negative_count,
            "coefficient": self.coefficient,
            "intercept": self.intercept,
            "reason_code": self.reason_code,
            "solver": self.solver,
            "iteration_count": self.iteration_count,
            "convergence_state": self.convergence_state,
            "validation_metrics": self.validation_metrics,
        }


def fit_platt_calibrator(
    *,
    head: str,
    raw_margin: np.ndarray,
    raw_probability: np.ndarray,
    truth: np.ndarray,
) -> PlattCalibrationResult:
    margin = _finite_vector(raw_margin, name=f"{head}.raw_margin")
    probability = _probability_vector(raw_probability, name=f"{head}.raw_probability")
    actual = _binary_vector(truth, name=f"{head}.truth")
    if not (len(margin) == len(probability) == len(actual)) or len(actual) == 0:
        raise _calibration_error(
            "binary calibration vectors have inconsistent or empty shape",
            head=head,
        )
    positive_count = int(actual.sum())
    negative_count = int(len(actual) - positive_count)
    raw_metrics = binary_metrics(actual, probability)
    solver_identity = {
        **PLATT_SOLVER_CONTRACT,
        "library_version": version("scikit-learn"),
    }
    if positive_count == 0 or negative_count == 0:
        return PlattCalibrationResult(
            state="UNCALIBRATED",
            head=head,
            row_count=len(actual),
            positive_count=positive_count,
            negative_count=negative_count,
            coefficient=None,
            intercept=None,
            reason_code="ADVISORY_OUTCOME_CALIBRATION_CLASS_VARIATION_MISSING",
            solver=solver_identity,
            iteration_count=0,
            convergence_state="NOT_FITTED_CLASS_VARIATION_MISSING",
            validation_metrics={"raw": raw_metrics, "calibrated": None},
        )
    try:
        estimator = LogisticRegression(
            **{
                key: value
                for key, value in PLATT_SOLVER_CONTRACT.items()
                if key not in {"library", "estimator"}
            }
        )
        estimator.fit(margin.reshape(-1, 1), actual)
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "Platt probability calibration failed",
            reason_code="ADVISORY_OUTCOME_CALIBRATION_FAILED",
            context={"head": head, "error_type": type(exc).__name__, "error_message": str(exc)},
        ) from exc
    coefficient = float(estimator.coef_[0, 0])
    intercept = float(estimator.intercept_[0])
    iteration_count = int(estimator.n_iter_[0])
    if (
        not math.isfinite(coefficient)
        or not math.isfinite(intercept)
        or iteration_count >= PLATT_SOLVER_CONTRACT["max_iter"]
    ):
        raise _calibration_error("Platt probability calibration did not converge", head=head)
    if coefficient <= 0.0:
        return PlattCalibrationResult(
            state="UNCALIBRATED",
            head=head,
            row_count=len(actual),
            positive_count=positive_count,
            negative_count=negative_count,
            coefficient=None,
            intercept=None,
            reason_code="ADVISORY_OUTCOME_CALIBRATION_ORDER_REVERSAL",
            solver=solver_identity,
            iteration_count=iteration_count,
            convergence_state="CONVERGED_ORDER_REVERSAL",
            validation_metrics={"raw": raw_metrics, "calibrated": None},
        )
    calibrated = apply_platt_calibrator(
        raw_margin=margin,
        coefficient=coefficient,
        intercept=intercept,
    )
    return PlattCalibrationResult(
        state="CALIBRATED",
        head=head,
        row_count=len(actual),
        positive_count=positive_count,
        negative_count=negative_count,
        coefficient=coefficient,
        intercept=intercept,
        reason_code=None,
        solver=solver_identity,
        iteration_count=iteration_count,
        convergence_state="CONVERGED",
        validation_metrics={"raw": raw_metrics, "calibrated": binary_metrics(actual, calibrated)},
    )


def apply_platt_calibrator(
    *, raw_margin: np.ndarray, coefficient: float, intercept: float
) -> np.ndarray:
    margin = _finite_vector(raw_margin, name="raw_margin")
    if not math.isfinite(coefficient) or not math.isfinite(intercept) or coefficient <= 0.0:
        raise _calibration_error("Platt calibration parameters are invalid")
    linear = coefficient * margin + intercept
    output = np.empty_like(linear, dtype=float)
    positive = linear >= 0
    output[positive] = 1.0 / (1.0 + np.exp(-linear[positive]))
    exponent = np.exp(linear[~positive])
    output[~positive] = exponent / (1.0 + exponent)
    return _probability_vector(output, name="calibrated_probability")


def binary_metrics(actual: np.ndarray, probability: np.ndarray) -> dict[str, Any]:
    truth = _binary_vector(actual, name="actual")
    predicted = _probability_vector(probability, name="probability")
    if len(truth) != len(predicted) or len(truth) == 0:
        raise _calibration_error("binary metric vectors have inconsistent or empty shape")
    clipped = np.clip(predicted, np.finfo(float).eps, 1.0 - np.finfo(float).eps)
    return {
        "roc_auc": float(roc_auc_score(truth, predicted)) if len(np.unique(truth)) == 2 else None,
        "brier_score": float(np.mean((predicted - truth) ** 2)),
        "binary_logloss": float(log_loss(truth, clipped, labels=[0, 1])),
        "ece_10_bin": expected_calibration_error(truth, predicted, bin_count=10),
        "positive_rate": float(truth.mean()),
        "row_count": len(truth),
    }


def expected_calibration_error(
    actual: np.ndarray, probability: np.ndarray, *, bin_count: int
) -> dict[str, Any]:
    truth = _binary_vector(actual, name="actual")
    predicted = _probability_vector(probability, name="probability")
    if len(truth) != len(predicted) or len(truth) == 0 or bin_count <= 0:
        raise _calibration_error("ECE inputs are inconsistent")
    indices = np.minimum((predicted * bin_count).astype(int), bin_count - 1)
    bins: list[dict[str, Any]] = []
    ece = 0.0
    for index in range(bin_count):
        mask = indices == index
        count = int(mask.sum())
        lower = index / bin_count
        upper = (index + 1) / bin_count
        if count:
            mean_probability = float(predicted[mask].mean())
            event_rate = float(truth[mask].mean())
            contribution = count / len(truth) * abs(mean_probability - event_rate)
            ece += contribution
        else:
            mean_probability = None
            event_rate = None
            contribution = 0.0
        bins.append(
            {
                "index": index,
                "lower": lower,
                "upper": upper,
                "count": count,
                "mean_probability": mean_probability,
                "event_rate": event_rate,
                "contribution": float(contribution),
            }
        )
    return {"value": float(ece), "bin_count": bin_count, "bins": bins}


def fit_return_interval_adjustment(
    *, q10: np.ndarray, q50: np.ndarray, q90: np.ndarray, truth: np.ndarray
) -> dict[str, Any]:
    lower, middle, upper = _monotonic_triplet(q10, q50, q90)
    actual = _finite_vector(truth, name="return_truth")
    if len(actual) != len(lower) or len(actual) == 0:
        raise _calibration_error("return interval vectors have inconsistent or empty shape")
    scores = np.maximum.reduce((lower - actual, actual - upper, np.zeros(len(actual))))
    delta = _finite_sample_quantile(scores, coverage=0.8)
    return {
        "state": "CALIBRATED",
        "method": "CQR_CENTRAL_80_NONNEGATIVE_EXPANSION",
        "nominal_coverage": 0.8,
        "row_count": len(actual),
        "delta": delta,
        "validation_metrics": interval_metrics(actual, lower, upper, delta=delta),
    }


def fit_path_upper_adjustment(
    *, q50: np.ndarray, q90: np.ndarray, truth: np.ndarray
) -> dict[str, Any]:
    median, upper = _monotonic_nonnegative_pair(q50, q90)
    actual = _finite_vector(truth, name="path_truth")
    if len(actual) != len(upper) or len(actual) == 0:
        raise _calibration_error("path interval vectors have inconsistent or empty shape")
    scores = np.maximum(actual - upper, 0.0)
    delta = _finite_sample_quantile(scores, coverage=0.9)
    calibrated_upper = upper + delta
    return {
        "state": "CALIBRATED",
        "method": "CONFORMAL_UPPER_90_NONNEGATIVE_EXPANSION",
        "nominal_coverage": 0.9,
        "row_count": len(actual),
        "delta": delta,
        "validation_metrics": {
            "raw_upper_coverage": float((actual <= upper).mean()),
            "calibrated_upper_coverage": float((actual <= calibrated_upper).mean()),
            "raw_mean_upper": float(upper.mean()),
            "calibrated_mean_upper": float(calibrated_upper.mean()),
            "q50_mean": float(median.mean()),
        },
    }


def apply_return_interval_adjustment(
    *, q10: np.ndarray, q50: np.ndarray, q90: np.ndarray, delta: float
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    lower, middle, upper = _monotonic_triplet(q10, q50, q90)
    _require_nonnegative_delta(delta)
    return lower - delta, middle, upper + delta


def apply_path_upper_adjustment(
    *, q50: np.ndarray, q90: np.ndarray, delta: float
) -> tuple[np.ndarray, np.ndarray]:
    median, upper = _monotonic_nonnegative_pair(q50, q90)
    _require_nonnegative_delta(delta)
    return median, upper + delta


def interval_metrics(
    actual: np.ndarray, lower: np.ndarray, upper: np.ndarray, *, delta: float
) -> dict[str, Any]:
    _require_nonnegative_delta(delta)
    calibrated_lower = lower - delta
    calibrated_upper = upper + delta
    return {
        "raw_coverage": float(((actual >= lower) & (actual <= upper)).mean()),
        "calibrated_coverage": float(
            ((actual >= calibrated_lower) & (actual <= calibrated_upper)).mean()
        ),
        "raw_mean_width": float((upper - lower).mean()),
        "calibrated_mean_width": float((calibrated_upper - calibrated_lower).mean()),
    }


def _finite_sample_quantile(scores: np.ndarray, *, coverage: float) -> float:
    values = _finite_vector(scores, name="conformal_scores")
    if len(values) == 0 or not 0.0 < coverage < 1.0 or (values < 0.0).any():
        raise _calibration_error("conformal score contract is invalid")
    rank = min(math.ceil((len(values) + 1) * coverage), len(values))
    return float(np.sort(values)[rank - 1])


def _monotonic_triplet(
    first: np.ndarray, second: np.ndarray, third: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    vectors = [_finite_vector(value, name="quantile") for value in (first, second, third)]
    if len({len(value) for value in vectors}) != 1:
        raise _calibration_error("quantile vectors have inconsistent shape")
    ordered = np.sort(np.column_stack(vectors), axis=1)
    return ordered[:, 0], ordered[:, 1], ordered[:, 2]


def _monotonic_nonnegative_pair(
    first: np.ndarray, second: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    vectors = [_finite_vector(value, name="path_quantile") for value in (first, second)]
    if len(vectors[0]) != len(vectors[1]):
        raise _calibration_error("path quantile vectors have inconsistent shape")
    ordered = np.sort(np.maximum(np.column_stack(vectors), 0.0), axis=1)
    return ordered[:, 0], ordered[:, 1]


def _finite_vector(value: np.ndarray, *, name: str) -> np.ndarray:
    vector = np.asarray(value, dtype=float)
    if vector.ndim != 1 or not np.isfinite(vector).all():
        raise _calibration_error("calibration vector is not finite and one-dimensional", name=name)
    return vector


def _probability_vector(value: np.ndarray, *, name: str) -> np.ndarray:
    vector = _finite_vector(value, name=name)
    if (vector < 0.0).any() or (vector > 1.0).any():
        raise _calibration_error("calibration probability is outside [0, 1]", name=name)
    return vector


def _binary_vector(value: np.ndarray, *, name: str) -> np.ndarray:
    vector = _finite_vector(value, name=name)
    if not set(np.unique(vector)).issubset({0.0, 1.0}):
        raise _calibration_error("calibration truth is not binary", name=name)
    return vector.astype(int)


def _require_nonnegative_delta(delta: float) -> None:
    if not math.isfinite(delta) or delta < 0.0:
        raise _calibration_error("calibration adjustment is negative or non-finite")


def _calibration_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_OUTCOME_CALIBRATION_FAILED",
        context=context,
    )
