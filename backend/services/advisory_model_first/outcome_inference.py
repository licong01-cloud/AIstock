from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.outcome_calibration import (
    apply_path_upper_adjustment,
    apply_platt_calibrator,
    apply_return_interval_adjustment,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.outcome_contracts import OUTCOME_HORIZONS
from backend.services.advisory_model_first.outcome_runtime_bundle import (
    LoadedAdvisoryOutcomeBundle,
    expected_outcome_model_names,
)


def score_outcome_bundle(
    bundle: LoadedAdvisoryOutcomeBundle,
    features: pd.DataFrame,
) -> list[dict[str, Any]]:
    expected_models = set(expected_outcome_model_names())
    if set(bundle.models) != expected_models:
        raise AdvisoryModelFirstError(
            "outcome inference bundle does not contain the exact model set",
            reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
            context={
                "missing_models": sorted(expected_models - set(bundle.models)),
                "unexpected_models": sorted(set(bundle.models) - expected_models),
            },
        )
    matrix = _prepare_outcome_matrix(bundle, features)
    predictions = {
        name: _predict_head(model, matrix, head=name)
        for name, model in bundle.models.items()
        if name != "holding_bucket"
    }
    raw_margins: dict[str, np.ndarray] = {}
    if bundle.calibration is not None:
        for name in bundle.models:
            if name.startswith(("positive_excess", "signal_survival")):
                raw_margins[name] = _predict_head(
                    bundle.models[name], matrix, head=name, raw_score=True
                )
    try:
        holding = np.asarray(bundle.models["holding_bucket"].predict(matrix), dtype=float)
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "holding-period outcome head inference failed",
            reason_code="ADVISORY_OUTCOME_INFERENCE_FAILED",
            context={"error_type": type(exc).__name__},
        ) from exc
    if (
        holding.shape != (len(matrix), len(OUTCOME_HORIZONS))
        or not np.isfinite(holding).all()
        or (holding < 0.0).any()
        or (holding > 1.0).any()
        or not np.allclose(holding.sum(axis=1), 1.0, atol=1e-6)
    ):
        raise AdvisoryModelFirstError(
            "holding-period outcome head returned invalid probabilities",
            reason_code="ADVISORY_OUTCOME_INFERENCE_FAILED",
            context={"shape": list(holding.shape)},
        )
    output: list[dict[str, Any]] = []
    buckets = np.asarray(OUTCOME_HORIZONS)
    cumulative = np.cumsum(holding, axis=1)
    low_indices = (cumulative < 0.2).sum(axis=1).clip(max=len(buckets) - 1)
    high_indices = (cumulative < 0.8).sum(axis=1).clip(max=len(buckets) - 1)
    for row_index, row in features.reset_index(drop=True).iterrows():
        horizons: list[dict[str, Any]] = []
        for horizon in OUTCOME_HORIZONS:
            excess = sorted(
                float(predictions[f"excess_return_h{horizon}_q{value}"][row_index])
                for value in (10, 50, 90)
            )
            mfe = sorted(
                max(0.0, float(predictions[f"path_mfe_h{horizon}_q{value}"][row_index]))
                for value in (50, 90)
            )
            mae = sorted(
                max(0.0, float(predictions[f"path_mae_loss_h{horizon}_q{value}"][row_index]))
                for value in (50, 90)
            )
            positive_probability = float(predictions[f"positive_excess_h{horizon}"][row_index])
            survival_probability = float(predictions[f"signal_survival_h{horizon}"][row_index])
            _require_probability(positive_probability, head=f"positive_excess_h{horizon}")
            _require_probability(survival_probability, head=f"signal_survival_h{horizon}")
            horizon_output: dict[str, Any] = {
                    "horizon_days": horizon,
                    "excess_return_q10": excess[0],
                    "excess_return_q50": excess[1],
                    "excess_return_q90": excess[2],
                    "positive_probability": positive_probability,
                    "signal_survival_probability": survival_probability,
                    "path_mfe_q50": mfe[0],
                    "path_mfe_q90": mfe[1],
                    "path_mae_loss_q50": mae[0],
                    "path_mae_loss_q90": mae[1],
            }
            if bundle.calibration is not None:
                return_spec = bundle.calibration["return_intervals"][f"excess_return_h{horizon}"]
                calibrated_return = apply_return_interval_adjustment(
                    q10=np.asarray([excess[0]]),
                    q50=np.asarray([excess[1]]),
                    q90=np.asarray([excess[2]]),
                    delta=float(return_spec["delta"]),
                )
                horizon_output.update(
                    {
                        "excess_return_calibrated_q10": float(calibrated_return[0][0]),
                        "excess_return_calibrated_q50": float(calibrated_return[1][0]),
                        "excess_return_calibrated_q90": float(calibrated_return[2][0]),
                        "return_interval_calibration_state": "CALIBRATED",
                    }
                )
                for family, output_name in (
                    ("positive_excess", "positive_probability"),
                    ("signal_survival", "signal_survival_probability"),
                ):
                    head = f"{family}_h{horizon}"
                    spec = bundle.calibration["binary_heads"][head]
                    horizon_output[f"{output_name}_calibration_state"] = spec["state"]
                    horizon_output[f"{output_name}_calibrated"] = (
                        float(
                            apply_platt_calibrator(
                                raw_margin=np.asarray([raw_margins[head][row_index]]),
                                coefficient=float(spec["coefficient"]),
                                intercept=float(spec["intercept"]),
                            )[0]
                        )
                        if spec["state"] == "CALIBRATED"
                        else None
                    )
                for family, raw_values in (
                    ("path_mfe", mfe),
                    ("path_mae_loss", mae),
                ):
                    spec = bundle.calibration["path_upper"][f"{family}_h{horizon}"]
                    calibrated_path = apply_path_upper_adjustment(
                        q50=np.asarray([raw_values[0]]),
                        q90=np.asarray([raw_values[1]]),
                        delta=float(spec["delta"]),
                    )
                    horizon_output[f"{family}_calibrated_q50"] = float(calibrated_path[0][0])
                    horizon_output[f"{family}_calibrated_q90"] = float(calibrated_path[1][0])
                    horizon_output[f"{family}_calibration_state"] = "CALIBRATED"
            horizons.append(horizon_output)
        holding_period: dict[str, Any] = {
                    "probabilities": {
                        str(horizon): float(holding[row_index, position])
                        for position, horizon in enumerate(OUTCOME_HORIZONS)
                    },
                    "mode_days": int(buckets[int(holding[row_index].argmax())]),
                    "range_low_days": int(buckets[int(low_indices[row_index])]),
                    "range_high_days": int(buckets[int(high_indices[row_index])]),
        }
        if bundle.calibration is not None:
            holding_period["calibration_state"] = "UNCALIBRATED"
        output.append(
            {
                "symbol": str(row["instrument"]),
                "horizons": horizons,
                "holding_period": holding_period,
            }
        )
    return output


def unavailable_outcome_envelope(
    *,
    reason_code: str,
    message: str,
) -> dict[str, Any]:
    return {
        "status": "OUTCOME_UNAVAILABLE",
        "calibration_state": "UNCALIBRATED",
        "calibration_policy_version": None,
        "parent_outcome_bundle_id": None,
        "binary_calibration_state": "UNCALIBRATED",
        "return_interval_calibration_state": "UNCALIBRATED",
        "path_upper_calibration_state": "UNCALIBRATED",
        "holding_calibration_state": "UNCALIBRATED",
        "outcome_bundle_id": None,
        "parent_bundle_id": None,
        "model_version": None,
        "horizons": list(OUTCOME_HORIZONS),
        "candidates": [],
        "reason_code": reason_code,
        "message": message,
    }


def _prepare_outcome_matrix(
    bundle: LoadedAdvisoryOutcomeBundle,
    features: pd.DataFrame,
) -> pd.DataFrame:
    missing = sorted(set(MODEL_FEATURE_COLUMNS) - set(features.columns))
    if missing:
        raise AdvisoryModelFirstError(
            "outcome inference is missing frozen model features",
            reason_code="ADVISORY_OUTCOME_INFERENCE_FAILED",
            context={"missing_features": missing},
        )
    matrix = features.loc[:, MODEL_FEATURE_COLUMNS].copy()
    for column in matrix.columns:
        if column not in CATEGORICAL_FEATURE_COLUMNS:
            try:
                matrix[column] = pd.to_numeric(matrix[column], errors="raise")
            except (TypeError, ValueError) as exc:
                raise AdvisoryModelFirstError(
                    "outcome inference feature contains a non-numeric value",
                    reason_code="ADVISORY_OUTCOME_INFERENCE_FAILED",
                    context={"feature": column, "error_type": type(exc).__name__},
                ) from exc
    vocabulary = bundle.feature_schema.get("categorical_vocabulary") or {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        categories = tuple(int(value) for value in vocabulary.get(column) or ())
        if not categories:
            raise AdvisoryModelFirstError(
                "outcome bundle categorical vocabulary is empty",
                reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
                context={"feature": column},
            )
        numeric = pd.to_numeric(matrix[column], errors="coerce")
        unseen = numeric.notna() & ~numeric.isin(categories)
        if unseen.any():
            matrix.loc[unseen, f"{column}__missing"] = 1
            numeric = numeric.mask(unseen)
        matrix[column] = pd.Categorical(numeric, categories=categories)
    for name, model in bundle.models.items():
        try:
            feature_names = tuple(model.feature_name())
        except Exception as exc:
            raise AdvisoryModelFirstError(
                "outcome head feature identity cannot be read",
                reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
                context={"head": name, "error_type": type(exc).__name__},
            ) from exc
        if feature_names != tuple(MODEL_FEATURE_COLUMNS):
            raise AdvisoryModelFirstError(
                "outcome head feature order differs from the frozen schema",
                reason_code="ADVISORY_OUTCOME_BUNDLE_INVALID",
                context={"head": name},
            )
    return matrix


def _predict_head(
    model: Any, matrix: pd.DataFrame, *, head: str, raw_score: bool = False
) -> np.ndarray:
    try:
        values = np.asarray(
            model.predict(matrix, raw_score=True) if raw_score else model.predict(matrix),
            dtype=float,
        )
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "outcome head inference failed",
            reason_code="ADVISORY_OUTCOME_INFERENCE_FAILED",
            context={"head": head, "error_type": type(exc).__name__},
        ) from exc
    if values.shape != (len(matrix),) or not np.isfinite(values).all():
        raise AdvisoryModelFirstError(
            "outcome head returned invalid predictions",
            reason_code="ADVISORY_OUTCOME_INFERENCE_FAILED",
            context={"head": head, "shape": list(values.shape)},
        )
    return values


def _require_probability(value: float, *, head: str) -> None:
    if not 0.0 <= value <= 1.0:
        raise AdvisoryModelFirstError(
            "outcome probability is outside the valid range",
            reason_code="ADVISORY_OUTCOME_INFERENCE_FAILED",
            context={"head": head, "value": value},
        )
