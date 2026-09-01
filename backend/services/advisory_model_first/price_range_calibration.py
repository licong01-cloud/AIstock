from __future__ import annotations

import math
from typing import Any

import numpy as np

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def fit_entry_gap_interval_adjustment(
    *,
    split: str,
    q10: np.ndarray,
    q50: np.ndarray,
    q90: np.ndarray,
    truth: np.ndarray,
) -> dict[str, Any]:
    if split != "validation":
        raise calibration_error("entry-gap calibration fit accepts validation split only")
    lower, middle, upper = monotonic_triplet(q10, q50, q90)
    actual = finite_vector(truth, name="entry_gap_truth")
    if len(actual) != len(lower) or len(actual) == 0:
        raise calibration_error("entry-gap interval vectors have inconsistent or empty shape")
    scores = np.maximum.reduce((lower - actual, actual - upper, np.zeros(len(actual))))
    rank = min(math.ceil((len(scores) + 1) * 0.8), len(scores))
    delta = float(np.sort(scores)[rank - 1])
    return {
        "state": "CALIBRATED",
        "method": "CQR_CENTRAL_80_NONNEGATIVE_EXPANSION",
        "nominal_coverage": 0.8,
        "fit_split": split,
        "row_count": len(actual),
        "finite_sample_rank": rank,
        "delta": delta,
        "validation_metrics": interval_metrics(actual, lower, upper, delta=delta),
    }


def apply_entry_gap_interval_adjustment(
    *, q10: np.ndarray, q50: np.ndarray, q90: np.ndarray, delta: float
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    lower, middle, upper = monotonic_triplet(q10, q50, q90)
    if not math.isfinite(delta) or delta < 0.0:
        raise calibration_error("entry-gap calibration delta must be finite and nonnegative")
    return lower - delta, middle, upper + delta


def interval_metrics(
    actual: np.ndarray, lower: np.ndarray, upper: np.ndarray, *, delta: float
) -> dict[str, Any]:
    truth = finite_vector(actual, name="entry_gap_truth")
    calibrated_lower = lower - delta
    calibrated_upper = upper + delta
    raw_inside = (truth >= lower) & (truth <= upper)
    calibrated_inside = (truth >= calibrated_lower) & (truth <= calibrated_upper)
    return {
        "row_count": len(truth),
        "raw_coverage": float(raw_inside.mean()),
        "calibrated_coverage": float(calibrated_inside.mean()),
        "raw_coverage_absolute_error": float(abs(raw_inside.mean() - 0.8)),
        "calibrated_coverage_absolute_error": float(abs(calibrated_inside.mean() - 0.8)),
        "raw_mean_width": float((upper - lower).mean()),
        "calibrated_mean_width": float((calibrated_upper - calibrated_lower).mean()),
        "raw_median_width": float(np.median(upper - lower)),
        "calibrated_median_width": float(np.median(calibrated_upper - calibrated_lower)),
        "raw_lower_miss_rate": float((truth < lower).mean()),
        "raw_upper_miss_rate": float((truth > upper).mean()),
        "calibrated_lower_miss_rate": float((truth < calibrated_lower).mean()),
        "calibrated_upper_miss_rate": float((truth > calibrated_upper).mean()),
    }


def monotonic_triplet(
    q10: np.ndarray, q50: np.ndarray, q90: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    vectors = (
        finite_vector(q10, name="q10"),
        finite_vector(q50, name="q50"),
        finite_vector(q90, name="q90"),
    )
    if len({len(vector) for vector in vectors}) != 1:
        raise calibration_error("entry-gap quantile vectors have inconsistent shape")
    values = np.column_stack(vectors)
    ordered = np.sort(values, axis=1)
    return ordered[:, 0], ordered[:, 1], ordered[:, 2]


def finite_vector(values: np.ndarray, *, name: str) -> np.ndarray:
    output = np.asarray(values, dtype=float)
    if output.ndim != 1 or not np.isfinite(output).all():
        raise calibration_error(f"{name} must be a finite vector")
    return output


def calibration_error(message: str) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_PRICE_RANGE_CALIBRATION_FAILED",
    )
