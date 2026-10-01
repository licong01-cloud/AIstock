"""Minimal single-head action-value models for PT-NEXT-024."""
from __future__ import annotations

import importlib.metadata
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .causal_timing_contracts import (
    FEATURE_INPUT_BASIS_SHA256,
    FEATURE_ORDER,
    FEATURE_SPEC_SHA256,
    GBDT_SPEC,
    RIDGE_SPEC,
    TRAIN_END,
    TRAIN_START,
    VALID_END,
    VALID_START,
)
from .contracts import canonical_sha256


LABEL = "label_net_action_value_bps"
REQUIRED_COLUMNS = ("symbol", "decision_date", "label_available_at", LABEL, *FEATURE_ORDER)


def _matrix(rows: pd.DataFrame) -> np.ndarray:
    missing = sorted(set(REQUIRED_COLUMNS) - set(rows))
    if missing:
        raise ActionValueError("CAUSAL_MODEL_COLUMNS_MISSING", missing=missing)
    values = rows.loc[:, list(FEATURE_ORDER)].to_numpy(dtype=np.float64)
    if not np.isfinite(values).all():
        raise ActionValueError("CAUSAL_MODEL_FEATURE_NONFINITE")
    return values


def training_split(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    decisions = pd.to_datetime(rows.decision_date).dt.date
    available = pd.to_datetime(rows.label_available_at).dt.date
    train_mask = (
        (decisions >= TRAIN_START) & (decisions <= TRAIN_END) & (available <= TRAIN_END)
    )
    valid_mask = (
        (decisions >= VALID_START) & (decisions <= VALID_END) & (available <= VALID_END)
    )
    if bool((train_mask & valid_mask).any()):
        raise ActionValueError("CAUSAL_MODEL_SPLIT_OVERLAP")
    audit = {
        "input_rows": len(rows),
        "train_rows": int(train_mask.sum()),
        "validation_rows": int(valid_mask.sum()),
        "immature_or_outside_rows": int((~(train_mask | valid_mask)).sum()),
    }
    train, valid = rows.loc[train_mask].copy(), rows.loc[valid_mask].copy()
    if train.empty or valid.empty:
        raise ActionValueError("CAUSAL_MODEL_SPLIT_EMPTY", **audit)
    _matrix(train)
    _matrix(valid)
    if not np.isfinite(train[LABEL].to_numpy(float)).all() or not np.isfinite(valid[LABEL].to_numpy(float)).all():
        raise ActionValueError("CAUSAL_MODEL_LABEL_NONFINITE")
    return train, valid, audit


def _metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float | int]:
    if len(actual) != len(predicted) or not len(actual):
        raise ActionValueError("CAUSAL_MODEL_METRIC_INPUT_INVALID")
    residual = predicted - actual
    return {
        "rows": len(actual),
        "mse_bps2": float(np.mean(residual ** 2)),
        "mae_bps": float(np.mean(np.abs(residual))),
        "prediction_mean_bps": float(np.mean(predicted)),
        "label_mean_bps": float(np.mean(actual)),
        "selected_fraction": float(np.mean(predicted > 0.0)),
    }


def fit_ridge(rows: pd.DataFrame, *, source_sha256: str, request_sha256: str) -> dict[str, Any]:
    train, valid, audit = training_split(rows)
    x = _matrix(train)
    y = train[LABEL].to_numpy(np.float64)
    mean = x.mean(axis=0)
    scale = x.std(axis=0, ddof=0)
    scale[scale == 0] = 1.0
    standardized = (x - mean) / scale
    design = np.column_stack((np.ones(len(x)), standardized))
    penalty = np.eye(design.shape[1], dtype=np.float64) * float(RIDGE_SPEC["l2_alpha"]) * len(x)
    penalty[0, 0] = 0.0
    coefficients = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    payload: dict[str, Any] = {
        "schema_version": "position_timing_causal_ridge_model_v1",
        "model_family": RIDGE_SPEC["model_family"],
        "spec": RIDGE_SPEC,
        "feature_order": list(FEATURE_ORDER),
        "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "feature_input_basis_sha256": FEATURE_INPUT_BASIS_SHA256,
        "training_start": TRAIN_START.isoformat(), "training_end": TRAIN_END.isoformat(),
        "label_cutoff": TRAIN_END.isoformat(), "model_effective_at": "2024-07-01T09:00:00+08:00",
        "source_sha256": source_sha256, "request_sha256": request_sha256,
        "training_rows_sha256": canonical_sha256(train.loc[:, list(REQUIRED_COLUMNS)].to_dict("records")),
        "mean": mean.tolist(), "scale": scale.tolist(),
        "intercept": float(coefficients[0]), "coefficients": coefficients[1:].tolist(),
        "split_audit": audit,
    }
    payload["validation"] = _metrics(valid[LABEL].to_numpy(float), predict(payload, valid))
    payload["model_sha256"] = canonical_sha256(payload)
    return payload


def _lightgbm() -> Any:
    try:
        version = importlib.metadata.version("lightgbm")
        import lightgbm as lgb
    except (ImportError, importlib.metadata.PackageNotFoundError) as exc:
        raise ActionValueError("CAUSAL_MODEL_LIGHTGBM_UNAVAILABLE") from exc
    if version != GBDT_SPEC["library_version"]:
        raise ActionValueError("CAUSAL_MODEL_LIGHTGBM_VERSION_DRIFT", observed=version)
    return lgb


def fit_gbdt(rows: pd.DataFrame, *, source_sha256: str, request_sha256: str) -> dict[str, Any]:
    train, valid, audit = training_split(rows)
    lgb = _lightgbm()
    params = {key: value for key, value in GBDT_SPEC.items() if key not in {"model_family", "library_version", "num_boost_round", "decision_threshold_bps"}}
    booster = lgb.train(
        params,
        lgb.Dataset(_matrix(train), label=train[LABEL].to_numpy(float), feature_name=list(FEATURE_ORDER), free_raw_data=False),
        num_boost_round=int(GBDT_SPEC["num_boost_round"]),
    )
    payload: dict[str, Any] = {
        "schema_version": "position_timing_causal_gbdt_model_v1",
        "model_family": GBDT_SPEC["model_family"], "spec": GBDT_SPEC,
        "feature_order": list(FEATURE_ORDER), "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "feature_input_basis_sha256": FEATURE_INPUT_BASIS_SHA256,
        "training_start": TRAIN_START.isoformat(), "training_end": TRAIN_END.isoformat(),
        "label_cutoff": TRAIN_END.isoformat(), "model_effective_at": "2024-07-01T09:00:00+08:00",
        "source_sha256": source_sha256, "request_sha256": request_sha256,
        "training_rows_sha256": canonical_sha256(train.loc[:, list(REQUIRED_COLUMNS)].to_dict("records")),
        "booster_text": booster.model_to_string(num_iteration=booster.current_iteration()),
        "split_audit": audit,
    }
    payload["validation"] = _metrics(valid[LABEL].to_numpy(float), predict(payload, valid))
    payload["model_sha256"] = canonical_sha256(payload)
    return payload


def validate_model(model: Mapping[str, Any]) -> None:
    expected = canonical_sha256({key: value for key, value in model.items() if key != "model_sha256"})
    if model.get("model_sha256") != expected:
        raise ActionValueError("CAUSAL_MODEL_IDENTITY_DRIFT")
    if tuple(model.get("feature_order") or ()) != tuple(FEATURE_ORDER):
        raise ActionValueError("CAUSAL_MODEL_FEATURE_ORDER_DRIFT")
    if model.get("feature_spec_sha256") != FEATURE_SPEC_SHA256:
        raise ActionValueError("CAUSAL_MODEL_FEATURE_SPEC_DRIFT")
    if model.get("feature_input_basis_sha256") != FEATURE_INPUT_BASIS_SHA256:
        raise ActionValueError("CAUSAL_MODEL_FEATURE_BASIS_DRIFT")


def predict(model: Mapping[str, Any], rows: pd.DataFrame | Mapping[str, float]) -> np.ndarray:
    if "model_sha256" in model:
        validate_model(model)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame([rows])
    missing = sorted(set(FEATURE_ORDER) - set(frame))
    if missing:
        raise ActionValueError("CAUSAL_MODEL_PREDICTION_COLUMNS_MISSING", missing=missing)
    x = frame.loc[:, list(FEATURE_ORDER)].to_numpy(np.float64)
    if not np.isfinite(x).all():
        raise ActionValueError("CAUSAL_MODEL_PREDICTION_NONFINITE")
    family = model.get("model_family")
    if family == RIDGE_SPEC["model_family"]:
        mean = np.asarray(model["mean"], dtype=np.float64)
        scale = np.asarray(model["scale"], dtype=np.float64)
        coef = np.asarray(model["coefficients"], dtype=np.float64)
        return float(model["intercept"]) + ((x - mean) / scale) @ coef
    if family == GBDT_SPEC["model_family"]:
        booster = _lightgbm().Booster(model_str=str(model["booster_text"]))
        return np.asarray(booster.predict(x, num_iteration=booster.current_iteration()), dtype=np.float64)
    raise ActionValueError("CAUSAL_MODEL_FAMILY_UNKNOWN", model_family=family)
