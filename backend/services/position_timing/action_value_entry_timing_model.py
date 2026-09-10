"""Dedicated same-stock immediate-versus-deferred entry timing model.

The model is research-only and deliberately does not extend the legacy
``action_value_model.HEADS`` or write the existing ``models_v2`` namespace.
It reuses only the frozen feature preprocessing, estimator parameters, and
monthly expanding-window schedule.
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


ENTRY_TIMING_OBJECTIVE = "ENTRY_TIMING_VALUE_V1"
ENTRY_TIMING_MODEL_SCHEMA = "position_timing_entry_timing_model_v1"
ENTRY_TIMING_ACTION_AUTHORITY = (
    "DEFERRED_ENTRY_TIMING_MODEL_WITH_FROZEN_RISK_EXIT_V1"
)
ENTRY_TIMING_ACTION_CONTRACT = {
    "policy_id": ENTRY_TIMING_ACTION_AUTHORITY,
    "cash_state_model_head": ENTRY_TIMING_OBJECTIVE,
    "cash_state_actions": ("OPEN", "WAIT"),
    "model_minimum_net_action_value_bps": 0.0,
    "positive_score_action": "OPEN_FIRST_ELIGIBLE",
    "non_positive_score_action": "WAIT_ONE_SESSION_THEN_REEVALUATE",
    "held_state_actions": ("FROZEN_RULE_RISK_EXIT", "HOLD"),
    "model_add_reduce_exit_authority": False,
}
ENTRY_TIMING_POLICY_SHA256 = canonical_sha256(
    {
        "base_policy_sha256": policy_sha256_for(CORE_INFORMATION_BLOCK),
        "model_action_contract": ENTRY_TIMING_ACTION_CONTRACT,
    }
)
ALWAYS_OPEN_ENTRY_COMPARATOR = (
    "ALWAYS_OPEN_FIRST_ELIGIBLE_WITH_FROZEN_RISK_EXIT_V1"
)
ALWAYS_OPEN_ENTRY_CONTRACT = {
    **ENTRY_TIMING_ACTION_CONTRACT,
    "policy_id": ALWAYS_OPEN_ENTRY_COMPARATOR,
    "cash_state_model_head": None,
    "positive_score_action": "NOT_APPLICABLE",
    "non_positive_score_action": "NOT_APPLICABLE",
    "cash_state_action": "OPEN_FIRST_ELIGIBLE_MAXIMUM_LEGAL_QUANTITY",
    "role": "PREREGISTERED_NON_SERVING_COMPARATOR",
}
ALWAYS_OPEN_ENTRY_POLICY_SHA256 = canonical_sha256(
    {
        "base_policy_sha256": policy_sha256_for(CORE_INFORMATION_BLOCK),
        "model_action_contract": ALWAYS_OPEN_ENTRY_CONTRACT,
    }
)


def entry_timing_training_rows_asof(
    rows: pd.DataFrame,
    cutoff: datetime,
    *,
    feature_order: Sequence[str],
) -> pd.DataFrame:
    """Return only causal, matured immediate-versus-deferred labels."""

    if cutoff.tzinfo is None:
        raise ActionValueError("ENTRY_TIMING_TRAINING_CUTOFF_NAIVE")
    required = {
        "decision_as_of",
        "label_available_at",
        "objective",
        "planned_delta_qty",
        "immediate_fill_status",
        "deferred_fill_status",
        "net_entry_timing_value_bps",
        *feature_order,
    }
    if not required.issubset(rows):
        raise ActionValueError(
            "ENTRY_TIMING_TRAINING_SCHEMA_MISSING",
            missing=sorted(required - set(rows)),
        )
    for column in ("decision_as_of", "label_available_at"):
        if rows[column].isna().any() or any(
            pd.Timestamp(value).tzinfo is None for value in rows[column]
        ):
            raise ActionValueError(
                "ENTRY_TIMING_TRAINING_LABEL_TIME_INVALID", column=column
            )
    decisions = pd.to_datetime(rows["decision_as_of"], utc=True)
    availability = pd.to_datetime(rows["label_available_at"], utc=True)
    if (availability <= decisions).any():
        raise ActionValueError("ENTRY_TIMING_TRAINING_LABEL_INTERVAL_INVALID")
    result = rows.loc[(decisions < cutoff) & (availability <= cutoff)].copy()
    if result.empty:
        raise ActionValueError("ENTRY_TIMING_TRAINING_OBJECTIVE_UNAVAILABLE")
    if (
        not result["objective"].eq(ENTRY_TIMING_OBJECTIVE).all()
        or not result["immediate_fill_status"].eq("FILLED").all()
        or not result["deferred_fill_status"].isin(("FILLED", "NO_FILL")).all()
        or not pd.to_numeric(result["planned_delta_qty"]).gt(0).all()
        or not pd.to_numeric(result["holding_exposure"]).eq(0).all()
    ):
        raise ActionValueError("ENTRY_TIMING_TRAINING_STATE_SUPPORT_INVALID")
    if not np.isfinite(
        pd.to_numeric(result["net_entry_timing_value_bps"])
    ).all():
        raise ActionValueError("ENTRY_TIMING_TRAINING_TARGET_NON_FINITE")
    return result


@dataclass
class EntryTimingModel:
    metadata: dict[str, Any]
    booster: Any

    def predict(self, frame: pd.DataFrame, *, decision_as_of: datetime) -> np.ndarray:
        require_causal(
            datetime.fromisoformat(self.metadata["available_at"]),
            decision_as_of,
            field="entry_timing_model.available_at",
        )
        information_block = self.metadata.get(
            "information_block", CORE_INFORMATION_BLOCK
        )
        market_features, feature_order, feature_spec_sha256 = feature_contract(
            information_block
        )
        if (
            self.metadata.get("schema_version") != ENTRY_TIMING_MODEL_SCHEMA
            or self.metadata.get("objective") != ENTRY_TIMING_OBJECTIVE
            or tuple(self.metadata.get("feature_order", ())) != feature_order
            or self.metadata.get("feature_spec_sha256") != feature_spec_sha256
            or self.metadata.get("base_policy_sha256")
            != policy_sha256_for(information_block)
        ):
            raise ActionValueError(
                "ENTRY_TIMING_MODEL_FEATURE_OR_POLICY_IDENTITY_MISMATCH"
            )
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
            frame, self.metadata["medians"], feature_order=feature_order
        )
        result = np.asarray(self.booster.predict(matrix, num_threads=1), dtype=float)
        if result.shape != (len(frame),) or not np.isfinite(result).all():
            raise ActionValueError("ENTRY_TIMING_MODEL_PREDICTION_NON_FINITE")
        return result


def fit_entry_timing_model(
    rows: pd.DataFrame,
    *,
    cutoff: datetime,
    available_at: datetime,
    source_sha256: str,
    request_sha256: str,
    source_commit: str,
    information_block: str = CORE_INFORMATION_BLOCK,
) -> EntryTimingModel:
    require_causal(cutoff, available_at, field="entry_timing_training.cutoff")
    for value in (source_sha256, request_sha256):
        validate_sha256(value, field="entry timing model source identity")
    if len(source_commit) != 40 or any(
        char not in "0123456789abcdef" for char in source_commit
    ):
        raise ActionValueError("ENTRY_TIMING_MODEL_CODE_IDENTITY_INVALID")
    market_features, feature_order, feature_spec_sha256 = feature_contract(
        information_block
    )
    lightgbm = _lightgbm()
    training = entry_timing_training_rows_asof(
        rows, cutoff, feature_order=feature_order
    )
    matrix, medians = numeric_matrix(
        training.loc[:, feature_order], feature_order=feature_order
    )
    estimator = lightgbm.LGBMRegressor(**estimator_parameters())
    estimator.fit(
        matrix, training["net_entry_timing_value_bps"].to_numpy(dtype=float)
    )
    booster = estimator.booster_
    model_text = booster.model_to_string()
    metadata = {
        "schema_version": ENTRY_TIMING_MODEL_SCHEMA,
        "objective": ENTRY_TIMING_OBJECTIVE,
        "feature_order": feature_order,
        "feature_spec_sha256": feature_spec_sha256,
        "base_policy_sha256": policy_sha256_for(information_block),
        "entry_timing_policy_sha256": ENTRY_TIMING_POLICY_SHA256,
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
        "interpretation": (
            "SAME_STOCK_IMMEDIATE_VS_ONE_SESSION_DEFER_ESTIMATE_"
            "NOT_STOCK_CONFIDENCE_OR_GLOBAL_OPTIMAL_STOPPING"
        ),
    }
    if information_block != CORE_INFORMATION_BLOCK:
        metadata.update(
            {
                "information_block": information_block,
                "required_market_features": market_features,
            }
        )
    metadata["model_sha256"] = canonical_sha256(metadata)
    return EntryTimingModel(metadata=metadata, booster=booster)


@dataclass(frozen=True)
class EntryTimingWalkForwardResult:
    predictions: pd.DataFrame
    models: tuple[EntryTimingModel, ...]
    diagnostics: dict[str, Any]


def walk_forward_entry_timing_values(
    rows: pd.DataFrame,
    *,
    calendar: Sequence[date],
    source_sha256: str,
    request_sha256: str,
    source_commit: str,
    information_block: str = CORE_INFORMATION_BLOCK,
) -> EntryTimingWalkForwardResult:
    """Fit the frozen single head at unchanged monthly expanding windows."""

    _, feature_order, feature_spec_sha256 = feature_contract(information_block)
    predictions: list[pd.DataFrame] = []
    models: list[EntryTimingModel] = []
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
            model = fit_entry_timing_model(
                rows,
                cutoff=window["cutoff"],
                available_at=window["available_at"],
                source_sha256=source_sha256,
                request_sha256=request_sha256,
                source_commit=source_commit,
                information_block=information_block,
            )
        except ActionValueError as exc:
            if exc.code == "ENTRY_TIMING_TRAINING_OBJECTIVE_UNAVAILABLE":
                continue
            raise
        scored = validation.loc[
            :,
            [
                "symbol",
                "decision_as_of",
                "objective",
                "planned_delta_qty",
                "net_entry_timing_value_bps",
            ],
        ].copy()
        scored["predicted_entry_timing_value_bps"] = model.predict(
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
        raise ActionValueError("ENTRY_TIMING_WALK_FORWARD_PREDICTIONS_EMPTY")
    frame = pd.concat(predictions, ignore_index=True)
    diagnostics = {
        "schema_version": "position_timing_entry_timing_walk_forward_diagnostic_v1",
        "objective": ENTRY_TIMING_OBJECTIVE,
        "feature_spec_sha256": feature_spec_sha256,
        "model_count": len(models),
        "prediction_rows": len(frame),
        "model_hashes": tuple(model.metadata["model_sha256"] for model in models),
        "warning": (
            "OOF_TARGET_DIAGNOSTIC_NOT_CONTINUOUS_POLICY_RETURN_OR_"
            "STOCK_CONFIDENCE"
        ),
    }
    diagnostics["diagnostic_sha256"] = canonical_sha256(diagnostics)
    return EntryTimingWalkForwardResult(frame, tuple(models), diagnostics)


__all__ = [
    "ALWAYS_OPEN_ENTRY_COMPARATOR",
    "ALWAYS_OPEN_ENTRY_CONTRACT",
    "ALWAYS_OPEN_ENTRY_POLICY_SHA256",
    "ENTRY_TIMING_ACTION_AUTHORITY",
    "ENTRY_TIMING_ACTION_CONTRACT",
    "ENTRY_TIMING_MODEL_SCHEMA",
    "ENTRY_TIMING_OBJECTIVE",
    "ENTRY_TIMING_POLICY_SHA256",
    "EntryTimingModel",
    "EntryTimingWalkForwardResult",
    "entry_timing_training_rows_asof",
    "fit_entry_timing_model",
    "walk_forward_entry_timing_values",
]
