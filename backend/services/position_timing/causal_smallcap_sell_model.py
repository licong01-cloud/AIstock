"""Frozen small-cap SELL-vs-HOLD model identity for PT-NEXT-027.

The numerical estimators intentionally reuse PT-NEXT-024's frozen Ridge and
shallow-LightGBM implementations.  This module adds a distinct, hash-bound
population/label identity without creating a second estimator implementation.
"""
from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .causal_timing_contracts import FEATURE_ORDER, FEATURE_SPEC_SHA256
from .causal_timing_model import (
    fit_gbdt as _fit_gbdt,
    fit_ridge as _fit_ridge,
    predict as _predict,
    training_split,
)
from .contracts import canonical_sha256


LABEL = "label_net_action_value_bps"
POPULATION_ID = "SMALL_LT_50B_CAUSAL_COHORT"
LABEL_CONTRACT_ID = "SELL_FIXED5_VS_HOLD_T21_5M_V1"
MODEL_SCHEMA = {
    "ridge": "position_timing_smallcap_sell_ridge_model_v1",
    "gbdt": "position_timing_smallcap_sell_gbdt_model_v1",
}


def _bind(payload: Mapping[str, Any], *, family: str) -> dict[str, Any]:
    value = {key: item for key, item in payload.items() if key != "model_sha256"}
    value.update(
        {
            "schema_version": MODEL_SCHEMA[family],
            "training_population_id": POPULATION_ID,
            "label_contract_id": LABEL_CONTRACT_ID,
            "label_column": LABEL,
        }
    )
    value["model_sha256"] = canonical_sha256(value)
    return value


def fit_ridge(
    rows: pd.DataFrame, *, source_sha256: str, request_sha256: str
) -> dict[str, Any]:
    return _bind(
        _fit_ridge(rows, source_sha256=source_sha256, request_sha256=request_sha256),
        family="ridge",
    )


def fit_gbdt(
    rows: pd.DataFrame, *, source_sha256: str, request_sha256: str
) -> dict[str, Any]:
    return _bind(
        _fit_gbdt(rows, source_sha256=source_sha256, request_sha256=request_sha256),
        family="gbdt",
    )


def validate_model(model: Mapping[str, Any]) -> None:
    expected = canonical_sha256(
        {key: value for key, value in model.items() if key != "model_sha256"}
    )
    if model.get("model_sha256") != expected:
        raise ActionValueError("SMALLCAP_SELL_MODEL_IDENTITY_DRIFT")
    if model.get("schema_version") not in MODEL_SCHEMA.values():
        raise ActionValueError("SMALLCAP_SELL_MODEL_SCHEMA_DRIFT")
    if model.get("training_population_id") != POPULATION_ID:
        raise ActionValueError("SMALLCAP_SELL_MODEL_POPULATION_DRIFT")
    if model.get("label_contract_id") != LABEL_CONTRACT_ID:
        raise ActionValueError("SMALLCAP_SELL_MODEL_LABEL_CONTRACT_DRIFT")
    if model.get("label_column") != LABEL:
        raise ActionValueError("SMALLCAP_SELL_MODEL_LABEL_COLUMN_DRIFT")
    if tuple(model.get("feature_order") or ()) != tuple(FEATURE_ORDER):
        raise ActionValueError("SMALLCAP_SELL_MODEL_FEATURE_ORDER_DRIFT")
    if model.get("feature_spec_sha256") != FEATURE_SPEC_SHA256:
        raise ActionValueError("SMALLCAP_SELL_MODEL_FEATURE_SPEC_DRIFT")


def predict(
    model: Mapping[str, Any], rows: pd.DataFrame | Mapping[str, float]
) -> np.ndarray:
    validate_model(model)
    return _predict(model, rows)


__all__ = [
    "FEATURE_ORDER",
    "LABEL",
    "LABEL_CONTRACT_ID",
    "MODEL_SCHEMA",
    "POPULATION_ID",
    "fit_gbdt",
    "fit_ridge",
    "predict",
    "training_split",
    "validate_model",
]
