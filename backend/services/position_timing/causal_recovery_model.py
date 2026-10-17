"""Small-cap recover-now action-value models for PT-NEXT-026."""
from __future__ import annotations

import importlib.metadata
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .causal_timing_contracts import FEATURE_INPUT_BASIS_SHA256, FEATURE_ORDER as MARKET_FEATURES
from .contracts import canonical_sha256


LABEL = "label_recover_now_advantage_bps"
STATE_FEATURES = (
    "sessions_since_trim",
    "return_since_trim_bps",
    "log1p_total_mv_wanyuan",
)
FEATURE_ORDER = (*MARKET_FEATURES, *STATE_FEATURES)
FEATURE_SPEC = {
    "schema": "position_timing_smallcap_recovery_features_v1",
    "feature_order": list(FEATURE_ORDER),
    "market_feature_input_basis_sha256": FEATURE_INPUT_BASIS_SHA256,
    "state_availability": "DECISION_AS_OF_WITH_T_MINUS_1_MARKET_CAP",
    "imputer": "NONE_REQUIRE_FINITE",
}
FEATURE_SPEC_SHA256 = canonical_sha256(FEATURE_SPEC)
REQUIRED_COLUMNS = (
    "symbol", "event_id", "decision_date", "label_available_at", LABEL, *FEATURE_ORDER,
)

TRAIN_START = "2018-08-01"
TRAIN_END = "2022-12-31"
VALID_START = "2023-01-01"
VALID_END = "2024-06-30"

RIDGE_SPEC = {
    "model_family": "RECOVERY_RIDGE_CLOSED_FORM_V1",
    "objective": "MSE_BPS",
    "l2_alpha": 0.001,
    "fit_intercept": True,
    "standardization": "TRAIN_MEAN_STD_ZERO_VARIANCE_SCALE_ONE",
    "decision_threshold_bps": 0.0,
}
GBDT_SPEC = {
    "model_family": "RECOVERY_LIGHTGBM_REGRESSION_V1",
    "library_version": "4.6.0",
    "objective": "regression",
    "metric": "l2",
    "num_boost_round": 100,
    "learning_rate": 0.05,
    "max_depth": 3,
    "num_leaves": 7,
    "min_data_in_leaf": 200,
    "feature_fraction": 1.0,
    "bagging_fraction": 1.0,
    "bagging_freq": 0,
    "lambda_l2": 1.0,
    "seed": 20260920,
    "deterministic": True,
    "force_col_wise": True,
    "num_threads": 1,
    "decision_threshold_bps": 0.0,
}


def _matrix(rows: pd.DataFrame) -> np.ndarray:
    missing = sorted(set(REQUIRED_COLUMNS) - set(rows))
    if missing:
        raise ActionValueError("RECOVERY_MODEL_COLUMNS_MISSING", missing=missing)
    values = rows.loc[:, list(FEATURE_ORDER)].to_numpy(np.float64)
    if not np.isfinite(values).all():
        raise ActionValueError("RECOVERY_MODEL_FEATURE_NONFINITE")
    return values


def training_split(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    decisions = pd.to_datetime(rows.decision_date)
    available = pd.to_datetime(rows.label_available_at)
    train_mask = (
        decisions.between(TRAIN_START, TRAIN_END)
        & (available <= pd.Timestamp(TRAIN_END))
    )
    valid_mask = (
        decisions.between(VALID_START, VALID_END)
        & (available <= pd.Timestamp(VALID_END))
    )
    if bool((train_mask & valid_mask).any()):
        raise ActionValueError("RECOVERY_MODEL_SPLIT_OVERLAP")
    train, valid = rows.loc[train_mask].copy(), rows.loc[valid_mask].copy()
    audit = {
        "input_rows": len(rows),
        "train_rows": len(train),
        "validation_rows": len(valid),
        "immature_or_outside_rows": int((~(train_mask | valid_mask)).sum()),
    }
    if train.empty or valid.empty:
        raise ActionValueError("RECOVERY_MODEL_SPLIT_EMPTY", **audit)
    _matrix(train)
    _matrix(valid)
    if not np.isfinite(train[LABEL].to_numpy(float)).all() or not np.isfinite(valid[LABEL].to_numpy(float)).all():
        raise ActionValueError("RECOVERY_MODEL_LABEL_NONFINITE")
    return train, valid, audit


def _metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float | int]:
    if len(actual) != len(predicted) or not len(actual):
        raise ActionValueError("RECOVERY_MODEL_METRIC_INPUT_INVALID")
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
    x, y = _matrix(train), train[LABEL].to_numpy(np.float64)
    mean = x.mean(axis=0)
    scale = x.std(axis=0, ddof=0)
    scale[scale == 0] = 1.0
    design = np.column_stack((np.ones(len(x)), (x - mean) / scale))
    penalty = np.eye(design.shape[1]) * float(RIDGE_SPEC["l2_alpha"]) * len(x)
    penalty[0, 0] = 0.0
    coefficients = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    payload: dict[str, Any] = {
        "schema_version": "position_timing_smallcap_recovery_ridge_model_v1",
        "model_family": RIDGE_SPEC["model_family"],
        "spec": RIDGE_SPEC,
        "feature_order": list(FEATURE_ORDER),
        "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "label_name": LABEL,
        "training_start": TRAIN_START,
        "training_end": TRAIN_END,
        "label_cutoff": TRAIN_END,
        "model_effective_at": "2024-07-01T09:00:00+08:00",
        "source_sha256": source_sha256,
        "request_sha256": request_sha256,
        "training_rows_sha256": canonical_sha256(train.loc[:, list(REQUIRED_COLUMNS)].to_dict("records")),
        "mean": mean.tolist(),
        "scale": scale.tolist(),
        "intercept": float(coefficients[0]),
        "coefficients": coefficients[1:].tolist(),
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
        raise ActionValueError("RECOVERY_MODEL_LIGHTGBM_UNAVAILABLE") from exc
    if version != GBDT_SPEC["library_version"]:
        raise ActionValueError("RECOVERY_MODEL_LIGHTGBM_VERSION_DRIFT", observed=version)
    return lgb


def fit_gbdt(rows: pd.DataFrame, *, source_sha256: str, request_sha256: str) -> dict[str, Any]:
    train, valid, audit = training_split(rows)
    lgb = _lightgbm()
    params = {
        key: value for key, value in GBDT_SPEC.items()
        if key not in {"model_family", "library_version", "num_boost_round", "decision_threshold_bps"}
    }
    booster = lgb.train(
        params,
        lgb.Dataset(
            _matrix(train), label=train[LABEL].to_numpy(float),
            feature_name=list(FEATURE_ORDER), free_raw_data=False,
        ),
        num_boost_round=int(GBDT_SPEC["num_boost_round"]),
    )
    payload: dict[str, Any] = {
        "schema_version": "position_timing_smallcap_recovery_gbdt_model_v1",
        "model_family": GBDT_SPEC["model_family"],
        "spec": GBDT_SPEC,
        "feature_order": list(FEATURE_ORDER),
        "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "label_name": LABEL,
        "training_start": TRAIN_START,
        "training_end": TRAIN_END,
        "label_cutoff": TRAIN_END,
        "model_effective_at": "2024-07-01T09:00:00+08:00",
        "source_sha256": source_sha256,
        "request_sha256": request_sha256,
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
        raise ActionValueError("RECOVERY_MODEL_IDENTITY_DRIFT")
    if tuple(model.get("feature_order") or ()) != FEATURE_ORDER:
        raise ActionValueError("RECOVERY_MODEL_FEATURE_ORDER_DRIFT")
    if model.get("feature_spec_sha256") != FEATURE_SPEC_SHA256 or model.get("label_name") != LABEL:
        raise ActionValueError("RECOVERY_MODEL_CONTRACT_DRIFT")


def predict(model: Mapping[str, Any], rows: pd.DataFrame | Mapping[str, float]) -> np.ndarray:
    if "model_sha256" in model:
        validate_model(model)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame([rows])
    missing = sorted(set(FEATURE_ORDER) - set(frame))
    if missing:
        raise ActionValueError("RECOVERY_MODEL_PREDICTION_COLUMNS_MISSING", missing=missing)
    x = frame.loc[:, list(FEATURE_ORDER)].to_numpy(np.float64)
    if not np.isfinite(x).all():
        raise ActionValueError("RECOVERY_MODEL_PREDICTION_NONFINITE")
    family = model.get("model_family")
    if family == RIDGE_SPEC["model_family"]:
        return float(model["intercept"]) + ((x - np.asarray(model["mean"])) / np.asarray(model["scale"])) @ np.asarray(model["coefficients"])
    if family == GBDT_SPEC["model_family"]:
        booster = _lightgbm().Booster(model_str=str(model["booster_text"]))
        return np.asarray(booster.predict(x, num_iteration=booster.current_iteration()), dtype=np.float64)
    raise ActionValueError("RECOVERY_MODEL_FAMILY_UNKNOWN", model_family=family)


__all__ = [
    "FEATURE_ORDER", "FEATURE_SPEC_SHA256", "GBDT_SPEC", "LABEL", "RIDGE_SPEC",
    "fit_gbdt", "fit_ridge", "predict", "training_split", "validate_model",
]
