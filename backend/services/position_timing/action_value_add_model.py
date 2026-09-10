"""Timing-owned single-head ADD action-value model for historical research.

This module deliberately does not extend ``action_value_model.HEADS`` or write
``models_v2``.  It reuses the frozen core feature/preprocessing and estimator
specification while keeping ADD-state supervision in a separate artifact
identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
from typing import Any, Sequence

import numpy as np
import pandas as pd

from .action_value import (
    CORE_INFORMATION_BLOCK,
    ActionValueError,
    feature_contract,
    policy_sha256_for,
    require_causal,
)
from .action_value_model import (
    _lightgbm,
    estimator_parameters,
    monthly_training_windows,
    numeric_matrix,
)
from .contracts import canonical_sha256, validate_sha256


ADD_OBJECTIVE = "ADD_ACTION_VALUE_V1"
ADD_MODEL_SCHEMA = "position_timing_add_action_model_v1"


def add_training_rows_asof(
    rows: pd.DataFrame,
    cutoff: datetime,
    *,
    feature_order: Sequence[str],
) -> pd.DataFrame:
    """Return only matured, state-matched ADD labels available at ``cutoff``."""

    if cutoff.tzinfo is None:
        raise ActionValueError("ADD_TRAINING_CUTOFF_NAIVE")
    required = {
        "decision_as_of",
        "label_available_at",
        "objective",
        "planned_delta_qty",
        "baseline_action",
        "net_action_value_bps",
        *feature_order,
    }
    if not required.issubset(rows):
        raise ActionValueError(
            "ADD_TRAINING_SCHEMA_MISSING", missing=sorted(required - set(rows))
        )
    for column in ("decision_as_of", "label_available_at"):
        if rows[column].isna().any() or any(
            pd.Timestamp(value).tzinfo is None for value in rows[column]
        ):
            raise ActionValueError("ADD_TRAINING_LABEL_TIME_INVALID", column=column)
    decisions = pd.to_datetime(rows["decision_as_of"], utc=True)
    availability = pd.to_datetime(rows["label_available_at"], utc=True)
    if (availability <= decisions).any():
        raise ActionValueError("ADD_TRAINING_LABEL_INTERVAL_INVALID")
    eligible = (decisions < cutoff) & (availability <= cutoff)
    result = rows.loc[eligible].copy()
    if result.empty:
        raise ActionValueError("ADD_TRAINING_OBJECTIVE_UNAVAILABLE")
    if (
        not result["objective"].eq(ADD_OBJECTIVE).all()
        or not result["baseline_action"].eq("HOLD").all()
        or not pd.to_numeric(result["planned_delta_qty"]).gt(0).all()
        or not pd.to_numeric(result["holding_exposure"]).gt(0).all()
        or not pd.to_numeric(result["action_fraction"]).gt(0).all()
        or not pd.to_numeric(result["holding_age_missing"]).eq(0).all()
        or not pd.to_numeric(result["entry_cost_missing"]).eq(0).all()
    ):
        raise ActionValueError("ADD_TRAINING_STATE_SUPPORT_INVALID")
    if not np.isfinite(pd.to_numeric(result["net_action_value_bps"])).all():
        raise ActionValueError("ADD_TRAINING_TARGET_NON_FINITE")
    return result


@dataclass
class AddActionModel:
    metadata: dict[str, Any]
    booster: Any

    def predict(self, frame: pd.DataFrame, *, decision_as_of: datetime) -> np.ndarray:
        require_causal(
            datetime.fromisoformat(self.metadata["available_at"]),
            decision_as_of,
            field="add_model.available_at",
        )
        information_block = self.metadata.get(
            "information_block", CORE_INFORMATION_BLOCK
        )
        market_features, feature_order, feature_spec_sha256 = feature_contract(
            information_block
        )
        if (
            self.metadata.get("schema_version") != ADD_MODEL_SCHEMA
            or self.metadata.get("objective") != ADD_OBJECTIVE
            or tuple(self.metadata.get("feature_order", ())) != feature_order
            or self.metadata.get("feature_spec_sha256") != feature_spec_sha256
            or self.metadata.get("policy_sha256")
            != policy_sha256_for(information_block)
        ):
            raise ActionValueError("ADD_MODEL_FEATURE_OR_POLICY_IDENTITY_MISMATCH")
        if tuple(frame.columns) != feature_order:
            raise ActionValueError("FEATURE_ORDER_MISMATCH")
        if len(frame) and not np.isfinite(
            frame.loc[:, market_features].to_numpy(dtype=float)
        ).all():
            raise ActionValueError(
                "CURRENT_CORE_FEATURE_UNAVAILABLE",
                information_block=information_block,
            )
        if frame.empty:
            return np.empty(0, dtype=float)
        matrix, _ = numeric_matrix(
            frame,
            self.metadata["medians"],
            feature_order=feature_order,
        )
        result = np.asarray(self.booster.predict(matrix, num_threads=1), dtype=float)
        if result.shape != (len(frame),) or not np.isfinite(result).all():
            raise ActionValueError("ADD_MODEL_PREDICTION_NON_FINITE")
        return result


def fit_add_model(
    rows: pd.DataFrame,
    *,
    cutoff: datetime,
    available_at: datetime,
    source_sha256: str,
    request_sha256: str,
    source_commit: str,
    information_block: str = CORE_INFORMATION_BLOCK,
) -> AddActionModel:
    require_causal(cutoff, available_at, field="add_training.cutoff")
    for value in (source_sha256, request_sha256):
        validate_sha256(value, field="add model source identity")
    if len(source_commit) != 40 or any(
        char not in "0123456789abcdef" for char in source_commit
    ):
        raise ActionValueError("ADD_MODEL_CODE_IDENTITY_INVALID")
    market_features, feature_order, feature_spec_sha256 = feature_contract(
        information_block
    )
    lightgbm = _lightgbm()
    training = add_training_rows_asof(
        rows, cutoff, feature_order=feature_order
    )
    matrix, medians = numeric_matrix(
        training.loc[:, feature_order], feature_order=feature_order
    )
    estimator = lightgbm.LGBMRegressor(**estimator_parameters())
    estimator.fit(matrix, training["net_action_value_bps"].to_numpy(dtype=float))
    booster = estimator.booster_
    model_text = booster.model_to_string()
    metadata = {
        "schema_version": ADD_MODEL_SCHEMA,
        "objective": ADD_OBJECTIVE,
        "feature_order": feature_order,
        "feature_spec_sha256": feature_spec_sha256,
        "policy_sha256": policy_sha256_for(information_block),
        "source_sha256": source_sha256,
        "request_sha256": request_sha256,
        "source_commit": source_commit,
        "training_cutoff": cutoff.isoformat(),
        "available_at": available_at.isoformat(),
        "temporal_mode": "HISTORICAL_REPLAY",
        "package_version": lightgbm.__version__,
        "training_rows": len(training),
        "label_available_max": pd.to_datetime(
            training["label_available_at"], utc=True
        ).max().isoformat(),
        "parameters": estimator.get_params(deep=False),
        "medians": medians,
        "text_sha256": hashlib.sha256(model_text.encode("utf-8")).hexdigest(),
        "interpretation": "STATE_MATCHED_ADD_MODEL_ESTIMATE_NOT_STOCK_CONFIDENCE",
    }
    if information_block != CORE_INFORMATION_BLOCK:
        metadata.update(
            {
                "information_block": information_block,
                "required_market_features": market_features,
            }
        )
    metadata["model_sha256"] = canonical_sha256(metadata)
    return AddActionModel(metadata=metadata, booster=booster)


@dataclass(frozen=True)
class AddWalkForwardResult:
    predictions: pd.DataFrame
    models: tuple[AddActionModel, ...]
    diagnostics: dict[str, Any]


def walk_forward_add_action_values(
    rows: pd.DataFrame,
    *,
    calendar: Sequence[date],
    source_sha256: str,
    request_sha256: str,
    source_commit: str,
    information_block: str = CORE_INFORMATION_BLOCK,
) -> AddWalkForwardResult:
    """Fit the single ADD head at the unchanged monthly expanding windows."""

    _, feature_order, feature_spec_sha256 = feature_contract(information_block)
    predictions: list[pd.DataFrame] = []
    models: list[AddActionModel] = []
    windows = monthly_training_windows(calendar)
    decisions = pd.to_datetime(rows["decision_as_of"], utc=True)
    for index, window in enumerate(windows):
        start = pd.Timestamp(window["available_at"])
        end = (
            pd.Timestamp(windows[index + 1]["available_at"])
            if index + 1 < len(windows)
            else None
        )
        mask = decisions >= start
        if end is not None:
            mask &= decisions < end
        validation = rows.loc[mask]
        if validation.empty:
            continue
        try:
            model = fit_add_model(
                rows,
                cutoff=window["cutoff"],
                available_at=window["available_at"],
                source_sha256=source_sha256,
                request_sha256=request_sha256,
                source_commit=source_commit,
                information_block=information_block,
            )
        except ActionValueError as exc:
            if exc.code == "ADD_TRAINING_OBJECTIVE_UNAVAILABLE":
                continue
            raise
        scored = validation.loc[
            :,
            [
                "sleeve_id",
                "initial_state",
                "symbol",
                "decision_as_of",
                "objective",
                "planned_delta_qty",
                "net_action_value_bps",
            ],
        ].copy()
        scored["predicted_action_value_bps"] = model.predict(
            validation.loc[:, feature_order],
            decision_as_of=max(
                window["available_at"],
                pd.Timestamp(validation["decision_as_of"].max()).to_pydatetime(),
            ),
        )
        scored["model_sha256"] = model.metadata["model_sha256"]
        predictions.append(scored)
        models.append(model)
    if not predictions:
        raise ActionValueError("ADD_WALK_FORWARD_PREDICTIONS_EMPTY")
    frame = pd.concat(predictions, ignore_index=True)
    diagnostics = {
        "schema_version": "position_timing_add_walk_forward_diagnostic_v1",
        "objective": ADD_OBJECTIVE,
        "feature_spec_sha256": feature_spec_sha256,
        "model_count": len(models),
        "prediction_rows": len(frame),
        "model_hashes": tuple(model.metadata["model_sha256"] for model in models),
        "warning": "ACTION_CONDITIONED_OOF_DIAGNOSTIC_NOT_CONTINUOUS_POLICY_RETURN",
    }
    diagnostics["diagnostic_sha256"] = canonical_sha256(diagnostics)
    return AddWalkForwardResult(frame, tuple(models), diagnostics)


__all__ = [
    "ADD_MODEL_SCHEMA",
    "ADD_OBJECTIVE",
    "AddActionModel",
    "AddWalkForwardResult",
    "add_training_rows_asof",
    "fit_add_model",
    "walk_forward_add_action_values",
]
