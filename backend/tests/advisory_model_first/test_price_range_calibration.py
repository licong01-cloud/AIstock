from __future__ import annotations

import numpy as np
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.price_range_calibration import (
    apply_entry_gap_interval_adjustment,
    fit_entry_gap_interval_adjustment,
)


def test_entry_gap_cqr_is_finite_sample_nonnegative_symmetric_and_keeps_q50() -> None:
    q10 = np.asarray([0.00, 0.03, -0.01, 0.02])
    q50 = np.asarray([0.01, 0.02, 0.00, 0.04])
    q90 = np.asarray([0.02, 0.01, 0.01, 0.03])
    truth = np.asarray([0.015, 0.05, -0.02, 0.035])

    spec = fit_entry_gap_interval_adjustment(
        split="validation", q10=q10, q50=q50, q90=q90, truth=truth
    )
    lower, middle, upper = apply_entry_gap_interval_adjustment(
        q10=q10,
        q50=q50,
        q90=q90,
        delta=spec["delta"],
    )
    raw = np.sort(np.column_stack((q10, q50, q90)), axis=1)

    assert spec["finite_sample_rank"] == 4
    assert spec["delta"] == pytest.approx(0.02)
    assert np.array_equal(middle, raw[:, 1])
    assert np.allclose(raw[:, 0] - lower, spec["delta"])
    assert np.allclose(upper - raw[:, 2], spec["delta"])
    assert (lower <= middle).all() and (middle <= upper).all()
    metrics = spec["validation_metrics"]
    assert metrics["calibrated_coverage"] >= metrics["raw_coverage"]
    assert metrics["calibrated_mean_width"] >= metrics["raw_mean_width"]
    assert metrics["calibrated_lower_miss_rate"] <= metrics["raw_lower_miss_rate"]
    assert metrics["calibrated_upper_miss_rate"] <= metrics["raw_upper_miss_rate"]


@pytest.mark.parametrize("delta", [-0.01, float("nan"), float("inf")])
def test_apply_entry_gap_cqr_rejects_invalid_delta(delta: float) -> None:
    with pytest.raises(AdvisoryModelFirstError) as error:
        apply_entry_gap_interval_adjustment(
            q10=np.asarray([0.0]),
            q50=np.asarray([0.1]),
            q90=np.asarray([0.2]),
            delta=delta,
        )
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_CALIBRATION_FAILED"


def test_entry_gap_cqr_rejects_nonfinite_or_shape_mismatched_inputs() -> None:
    with pytest.raises(AdvisoryModelFirstError) as nonfinite:
        fit_entry_gap_interval_adjustment(
            split="validation",
            q10=np.asarray([0.0, np.nan]),
            q50=np.asarray([0.1, 0.2]),
            q90=np.asarray([0.2, 0.3]),
            truth=np.asarray([0.1, 0.2]),
        )
    assert nonfinite.value.reason_code == "ADVISORY_PRICE_RANGE_CALIBRATION_FAILED"

    with pytest.raises(AdvisoryModelFirstError) as mismatch:
        fit_entry_gap_interval_adjustment(
            split="validation",
            q10=np.asarray([0.0]),
            q50=np.asarray([0.1, 0.2]),
            q90=np.asarray([0.2, 0.3]),
            truth=np.asarray([0.1, 0.2]),
        )
    assert mismatch.value.reason_code == "ADVISORY_PRICE_RANGE_CALIBRATION_FAILED"


@pytest.mark.parametrize("split", ["train", "purged", "test"])
def test_entry_gap_cqr_fit_rejects_non_validation_split(split: str) -> None:
    values = np.asarray([0.0])
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        fit_entry_gap_interval_adjustment(
            split=split,
            q10=values,
            q50=values,
            q90=values,
            truth=values,
        )
    assert exc_info.value.reason_code == "ADVISORY_PRICE_RANGE_CALIBRATION_FAILED"
