from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.outcome_inference import score_outcome_bundle
from backend.services.advisory_model_first.outcome_runtime_bundle import (
    LoadedAdvisoryOutcomeBundle,
    expected_outcome_model_names,
)


class _Model:
    def __init__(self, name: str, *, invalid_probability: bool = False) -> None:
        self.name = name
        self.invalid_probability = invalid_probability

    def feature_name(self) -> list[str]:
        return list(MODEL_FEATURE_COLUMNS)

    def predict(self, matrix: pd.DataFrame, raw_score: bool = False):
        if self.name == "holding_bucket":
            return np.tile(np.asarray([0.1, 0.2, 0.4, 0.2, 0.1]), (len(matrix), 1))
        if self.name.startswith(("positive_excess", "signal_survival")):
            if raw_score:
                return np.full(len(matrix), 0.4)
            return np.full(len(matrix), 1.1 if self.invalid_probability else 0.6)
        if "q10" in self.name:
            return np.full(len(matrix), 0.03)
        if "q50" in self.name:
            return np.full(len(matrix), -0.01)
        return np.full(len(matrix), 0.01)


def _bundle(*, invalid_probability: bool = False) -> LoadedAdvisoryOutcomeBundle:
    return LoadedAdvisoryOutcomeBundle(
        outcome_bundle_id="a" * 64,
        bundle_path=Path("/model/outcome"),
        manifest={"request_id": "advoutreq_runtime", "horizons": [1, 3, 5, 10, 20]},
        feature_schema={"categorical_vocabulary": {"l2_code_id": [1, 2]}},
        models={
            name: _Model(name, invalid_probability=invalid_probability)
            for name in expected_outcome_model_names()
        },
    )


def _calibrated_bundle() -> LoadedAdvisoryOutcomeBundle:
    bundle = _bundle()
    manifest = {
            **bundle.manifest,
            "schema_version": "advisory_outcome_bundle_v2",
            "calibration_state": "PARTIAL",
    }
    calibration = {
        "binary_heads": {
            f"{family}_h{horizon}": {
                "state": "CALIBRATED",
                "coefficient": 2.0,
                "intercept": -0.5,
            }
            for horizon in (1, 3, 5, 10, 20)
            for family in ("positive_excess", "signal_survival")
        },
        "return_intervals": {
            f"excess_return_h{horizon}": {"delta": 0.02}
            for horizon in (1, 3, 5, 10, 20)
        },
        "path_upper": {
            f"{family}_h{horizon}": {"delta": 0.03}
            for horizon in (1, 3, 5, 10, 20)
            for family in ("path_mfe", "path_mae_loss")
        },
    }
    return replace(bundle, manifest=manifest, calibration=calibration)


def _features() -> pd.DataFrame:
    frame = pd.DataFrame(0.0, index=range(2), columns=MODEL_FEATURE_COLUMNS)
    frame["instrument"] = ["000001.SZ", "000002.SZ"]
    frame["l2_code_id"] = [1, 2]
    return frame


def test_outcome_inference_returns_five_horizons_and_holding_range() -> None:
    result = score_outcome_bundle(_bundle(), _features())

    assert len(result) == 2
    assert [row["horizon_days"] for row in result[0]["horizons"]] == [1, 3, 5, 10, 20]
    horizon_5 = result[0]["horizons"][2]
    assert horizon_5["excess_return_q10"] == -0.01
    assert horizon_5["excess_return_q50"] == 0.01
    assert horizon_5["excess_return_q90"] == 0.03
    assert horizon_5["path_mfe_q50"] == 0.0
    assert horizon_5["path_mfe_q90"] == 0.01
    assert result[0]["holding_period"] == {
        "probabilities": {"1": 0.1, "3": 0.2, "5": 0.4, "10": 0.2, "20": 0.1},
        "mode_days": 5,
        "range_low_days": 3,
        "range_high_days": 10,
    }


def test_outcome_inference_rejects_invalid_binary_probability() -> None:
    with pytest.raises(AdvisoryModelFirstError) as error:
        score_outcome_bundle(_bundle(invalid_probability=True), _features())
    assert error.value.reason_code == "ADVISORY_OUTCOME_INFERENCE_FAILED"


def test_outcome_inference_rejects_missing_head_without_keyerror() -> None:
    bundle = _bundle()
    bundle.models.pop("positive_excess_h1")
    with pytest.raises(AdvisoryModelFirstError) as error:
        score_outcome_bundle(bundle, _features())
    assert error.value.reason_code == "ADVISORY_OUTCOME_BUNDLE_INVALID"


def test_v2_outcome_inference_keeps_raw_and_adds_calibrated_values() -> None:
    result = score_outcome_bundle(_calibrated_bundle(), _features())

    horizon = result[0]["horizons"][2]
    assert horizon["positive_probability"] == 0.6
    assert horizon["positive_probability_calibration_state"] == "CALIBRATED"
    assert horizon["positive_probability_calibrated"] != horizon["positive_probability"]
    assert horizon["excess_return_calibrated_q10"] == pytest.approx(-0.03)
    assert horizon["excess_return_calibrated_q90"] == pytest.approx(0.05)
    assert horizon["path_mfe_calibrated_q50"] == 0.0
    assert horizon["path_mfe_calibrated_q90"] == pytest.approx(0.04)
    assert result[0]["holding_period"]["calibration_state"] == "UNCALIBRATED"


def test_v2_partial_binary_head_keeps_raw_and_returns_null_calibrated_value() -> None:
    bundle = _calibrated_bundle()
    calibration = {
        **(bundle.calibration or {}),
        "binary_heads": {
            **(bundle.calibration or {})["binary_heads"],
            "positive_excess_h5": {
                "state": "UNCALIBRATED",
                "coefficient": None,
                "intercept": None,
                "reason_code": "ADVISORY_OUTCOME_CALIBRATION_CLASS_VARIATION_MISSING",
            },
        },
    }

    result = score_outcome_bundle(replace(bundle, calibration=calibration), _features())

    horizon = result[0]["horizons"][2]
    assert horizon["positive_probability"] == 0.6
    assert horizon["positive_probability_calibration_state"] == "UNCALIBRATED"
    assert horizon["positive_probability_calibrated"] is None
    assert horizon["signal_survival_probability_calibration_state"] == "CALIBRATED"
    assert horizon["signal_survival_probability_calibrated"] is not None
