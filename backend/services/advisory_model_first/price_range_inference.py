from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.labels import CLOSE_COST, OPEN_COST
from backend.services.advisory_model_first.price_range_contracts import (
    PRICE_RANGE_MODEL_NAMES,
)
from backend.services.advisory_model_first.price_range_regulatory import (
    resolve_regulatory_price_range,
)
from backend.services.advisory_model_first.price_range_runtime_bundle import (
    LoadedAdvisoryPriceRangeBundle,
)
from backend.services.advisory_model_first.realtime_feature_source import (
    PriceRangeRealtimeContext,
)


def score_price_range_bundle(
    bundle: LoadedAdvisoryPriceRangeBundle,
    features: pd.DataFrame,
    *,
    contexts: Mapping[str, PriceRangeRealtimeContext],
    context_unavailable: Sequence[Mapping[str, Any]],
    outcome_candidates: Sequence[Mapping[str, Any]],
    review_policy: Mapping[str, Any],
    review_policy_sha256: str,
    target_trade_date,
) -> list[dict[str, Any]]:
    if set(bundle.models) != set(PRICE_RANGE_MODEL_NAMES):
        raise AdvisoryModelFirstError(
            "price-range inference bundle does not contain the exact model set",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        )
    matrix = _prepare_matrix(bundle, features)
    predictions = {
        name: _predict_head(model, matrix, head=name)
        for name, model in bundle.models.items()
    }
    outcomes = {str(item.get("symbol")): item for item in outcome_candidates}
    if len(outcomes) != len(outcome_candidates):
        raise AdvisoryModelFirstError(
            "outcome candidates contain duplicate symbols",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
        )
    failures = {
        str(item.get("symbol")): item
        for item in context_unavailable
        if str(item.get("symbol") or "")
    }
    output: list[dict[str, Any]] = []
    for row_index, row in features.reset_index(drop=True).iterrows():
        symbol = str(row["instrument"])
        context = contexts.get(symbol)
        outcome = outcomes.get(symbol)
        if context is None:
            failure = failures.get(symbol) or {
                "reason_code": "ADVISORY_PRICE_RANGE_PIT_ATTRIBUTE_UNAVAILABLE",
                "message": "price-range realtime context is unavailable",
            }
            output.append(
                unavailable_price_range_candidate(
                    symbol=symbol,
                    reason_code=str(failure["reason_code"]),
                    message=str(failure["message"]),
                )
            )
            continue
        if outcome is None:
            output.append(
                unavailable_price_range_candidate(
                    symbol=symbol,
                    reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
                    message="matching M3 outcome prediction is unavailable",
                )
            )
            continue
        try:
            output.append(
                _project_candidate(
                    symbol=symbol,
                    context=context,
                    executable_probability=float(
                        predictions["entry_executable_probability"][row_index]
                    ),
                    entry_gaps=tuple(
                        float(predictions[name][row_index])
                        for name in ("entry_gap_q10", "entry_gap_q50", "entry_gap_q90")
                    ),
                    outcome=outcome,
                    review_policy=review_policy,
                    review_policy_sha256=review_policy_sha256,
                    target_trade_date=target_trade_date,
                )
            )
        except AdvisoryModelFirstError as exc:
            output.append(
                unavailable_price_range_candidate(
                    symbol=symbol,
                    reason_code=exc.reason_code,
                    message=str(exc),
                )
            )
    return output


def unavailable_price_range_envelope(*, reason_code: str, message: str) -> dict[str, Any]:
    return {
        "status": "PRICE_RANGE_UNAVAILABLE",
        "calibration_state": "UNCALIBRATED",
        "price_range_bundle_id": None,
        "parent_bundle_id": None,
        "outcome_bundle_id": None,
        "model_version": None,
        "price_basis": "UNADJUSTED_CNY_DECISION_CLOSE",
        "candidates": [],
        "reason_code": reason_code,
        "message": message,
    }


def unavailable_price_range_candidate(
    *, symbol: str, reason_code: str, message: str
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "status": "PRICE_RANGE_UNAVAILABLE",
        "projection_condition": "ENTRY_EXECUTABLE_AT_PREDICTED_ENTRY_MID",
        "entry_executable_probability": None,
        "decision_reference_price": None,
        "target_raw_price_multiplier": None,
        "entry_price": None,
        "take_profit_price": None,
        "protective_price": None,
        "stop_loss_price": None,
        "tick_size": None,
        "regulatory_price_range": None,
        "review_policy": None,
        "reason_code": reason_code,
        "message": message,
    }


def _project_candidate(
    *,
    symbol: str,
    context: PriceRangeRealtimeContext,
    executable_probability: float,
    entry_gaps: tuple[float, float, float],
    outcome: Mapping[str, Any],
    review_policy: Mapping[str, Any],
    review_policy_sha256: str,
    target_trade_date,
) -> dict[str, Any]:
    if not np.isfinite(executable_probability) or not 0 <= executable_probability <= 1:
        raise AdvisoryModelFirstError(
            "entry-executable probability is invalid",
            reason_code="ADVISORY_PRICE_RANGE_INFERENCE_FAILED",
            context={"symbol": symbol},
        )
    gaps = sorted(entry_gaps)
    if not all(np.isfinite(value) and value > -1 for value in gaps):
        raise AdvisoryModelFirstError(
            "entry-gap quantiles are invalid",
            reason_code="ADVISORY_PRICE_RANGE_PROJECTION_INVALID",
            context={"symbol": symbol},
        )
    regulatory = resolve_regulatory_price_range(
        context,
        target_trade_date=target_trade_date,
    )
    reference = context.decision_raw_close * context.target_raw_price_multiplier
    raw_entry = [reference * (1.0 + value) for value in gaps]
    if regulatory.status == "LIMITED":
        assert regulatory.low is not None and regulatory.high is not None
        raw_entry = [
            min(regulatory.high, max(regulatory.low, value)) for value in raw_entry
        ]
    entry_low = _round_tick(raw_entry[0], context.tick_size, ROUND_FLOOR)
    entry_mid = _round_tick(raw_entry[1], context.tick_size, ROUND_HALF_UP)
    entry_high = _round_tick(raw_entry[2], context.tick_size, ROUND_CEILING)
    if regulatory.status == "LIMITED":
        assert regulatory.low is not None and regulatory.high is not None
        entry_low = max(entry_low, regulatory.low)
        entry_mid = min(regulatory.high, max(regulatory.low, entry_mid))
        entry_high = min(entry_high, regulatory.high)
    if not (0 < entry_low <= entry_mid <= entry_high):
        raise AdvisoryModelFirstError(
            "entry price range is invalid after clipping and tick rounding",
            reason_code="ADVISORY_PRICE_RANGE_PROJECTION_INVALID",
            context={"symbol": symbol},
        )

    holding = outcome.get("holding_period")
    if not isinstance(holding, Mapping):
        raise AdvisoryModelFirstError(
            "M3 holding-period prediction is unavailable",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
            context={"symbol": symbol},
        )
    try:
        horizon = int(holding["mode_days"])
        range_low = int(holding["range_low_days"])
        range_high = int(holding["range_high_days"])
    except (KeyError, TypeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "M3 holding-period prediction is invalid",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
            context={"symbol": symbol},
        ) from exc
    if horizon not in {1, 3, 5, 10, 20} or not range_low <= horizon <= range_high:
        raise AdvisoryModelFirstError(
            "M3 holding-period mode is outside its predicted range",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
            context={"symbol": symbol},
        )
    horizon_rows = [
        item
        for item in outcome.get("horizons") or []
        if int(item.get("horizon_days", -1)) == horizon
    ]
    if len(horizon_rows) != 1:
        raise AdvisoryModelFirstError(
            "M3 target-horizon path prediction is unavailable",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
            context={"symbol": symbol, "horizon_days": horizon},
        )
    horizon_row = horizon_rows[0]
    mfe = sorted(
        max(0.0, float(horizon_row[name]))
        for name in ("path_mfe_q50", "path_mfe_q90")
    )
    mae = sorted(
        max(0.0, float(horizon_row[name]))
        for name in ("path_mae_loss_q50", "path_mae_loss_q90")
    )
    if not all(np.isfinite(value) for value in (*mfe, *mae)):
        raise AdvisoryModelFirstError(
            "M3 MFE or MAE prediction is invalid",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
            context={"symbol": symbol},
        )
    market_mfe = [max(0.0, (1 + OPEN_COST) * (1 + value) / (1 - CLOSE_COST) - 1) for value in mfe]
    market_drawdown = [
        max(0.0, 1 - (1 + OPEN_COST) * (1 - value) / (1 - CLOSE_COST))
        for value in mae
    ]
    stop_loss_bps = _nonnegative_policy_int(review_policy, "stop_loss_bps")
    take_profit_bps = _nonnegative_policy_int(review_policy, "take_profit_bps")
    trailing_stop_bps = _nonnegative_policy_int(review_policy, "trailing_stop_bps")
    take_profit_mode = str(review_policy.get("take_profit_mode") or "").strip().lower()
    if take_profit_mode not in {"fixed", "trailing", "none"}:
        raise AdvisoryModelFirstError(
            "review policy take-profit mode is invalid",
            reason_code="ADVISORY_PRICE_RANGE_POLICY_IDENTITY_MISMATCH",
            context={"symbol": symbol},
        )

    take_profit_low = max(
        entry_mid,
        _round_tick(entry_mid * (1 + market_mfe[0]), context.tick_size, ROUND_FLOOR),
    )
    take_profit_high = max(
        take_profit_low,
        _round_tick(entry_mid * (1 + market_mfe[1]), context.tick_size, ROUND_CEILING),
    )
    hard_drawdown = stop_loss_bps / 10000.0 if stop_loss_bps > 0 else None
    near = min(market_drawdown[0], hard_drawdown) if hard_drawdown is not None else market_drawdown[0]
    far = min(market_drawdown[1], hard_drawdown) if hard_drawdown is not None else market_drawdown[1]
    if near > far:
        near, far = far, near
    hard_stop_price = (
        _round_tick(entry_mid * (1 - hard_drawdown), context.tick_size, ROUND_CEILING)
        if hard_drawdown is not None
        else None
    )
    stop_low = min(
        entry_mid,
        _round_tick(entry_mid * (1 - far), context.tick_size, ROUND_CEILING),
    )
    stop_high = min(
        entry_mid,
        _round_tick(entry_mid * (1 - near), context.tick_size, ROUND_CEILING),
    )
    if hard_stop_price is not None:
        stop_low = max(stop_low, hard_stop_price)
        stop_high = max(stop_high, hard_stop_price)
    if not (0 < stop_low <= stop_high <= entry_mid):
        raise AdvisoryModelFirstError(
            "stop-loss price range violates the hard risk boundary",
            reason_code="ADVISORY_PRICE_RANGE_PROJECTION_INVALID",
            context={"symbol": symbol},
        )
    stop_status = "SINGLE_POINT" if stop_low == stop_high else "AVAILABLE"
    protective = _protective_price(
        entry_mid=entry_mid,
        market_mfe=market_mfe,
        tick_size=context.tick_size,
        hard_stop_price=hard_stop_price,
        take_profit_bps=take_profit_bps,
        trailing_stop_bps=trailing_stop_bps,
        take_profit_mode=take_profit_mode,
    )
    return {
        "symbol": symbol,
        "status": "EXPERIMENTAL_SHADOW",
        "projection_condition": "ENTRY_EXECUTABLE_AT_PREDICTED_ENTRY_MID",
        "entry_executable_probability": executable_probability,
        "decision_reference_price": context.decision_raw_close,
        "target_raw_price_multiplier": context.target_raw_price_multiplier,
        "entry_price": {
            "condition": "ENTRY_EXECUTABLE",
            "low": entry_low,
            "mid": entry_mid,
            "high": entry_high,
        },
        "take_profit_price": {
            "low": take_profit_low,
            "high": take_profit_high,
            "horizon_trade_days": horizon,
        },
        "protective_price": protective,
        "stop_loss_price": {
            "status": stop_status,
            "low": stop_low,
            "high": stop_high,
            "hard_stop_price": hard_stop_price,
        },
        "tick_size": context.tick_size,
        "regulatory_price_range": regulatory.as_dict(),
        "review_policy": {
            "review_policy_sha256": review_policy_sha256,
            "stop_loss_bps": stop_loss_bps,
            "take_profit_bps": take_profit_bps,
            "trailing_stop_bps": trailing_stop_bps,
            "take_profit_mode": take_profit_mode,
        },
        "reason_code": None,
        "message": None,
    }


def _protective_price(
    *,
    entry_mid: float,
    market_mfe: Sequence[float],
    tick_size: float,
    hard_stop_price: float | None,
    take_profit_bps: int,
    trailing_stop_bps: int,
    take_profit_mode: str,
) -> dict[str, Any]:
    if take_profit_mode != "trailing" or take_profit_bps <= 0 or trailing_stop_bps <= 0:
        return {
            "status": "NOT_APPLICABLE",
            "policy_activation_price": None,
            "model_peak_low": None,
            "model_peak_high": None,
            "floor_low": None,
            "floor_high": None,
        }
    activation_return = take_profit_bps / 10000.0
    trailing_drawdown = trailing_stop_bps / 10000.0
    activation_price = _round_tick(
        entry_mid * (1 + activation_return), tick_size, ROUND_CEILING
    )
    if market_mfe[1] < activation_return:
        return {
            "status": "MODEL_BELOW_POLICY_ACTIVATION",
            "policy_activation_price": activation_price,
            "model_peak_low": _round_tick(
                entry_mid * (1 + market_mfe[0]), tick_size, ROUND_FLOOR
            ),
            "model_peak_high": _round_tick(
                entry_mid * (1 + market_mfe[1]), tick_size, ROUND_CEILING
            ),
            "floor_low": None,
            "floor_high": None,
        }
    effective = [max(value, activation_return) for value in market_mfe]
    floors = [
        _round_tick(
            entry_mid * (1 + value - trailing_drawdown),
            tick_size,
            ROUND_CEILING,
        )
        for value in effective
    ]
    if hard_stop_price is not None:
        floors = [max(value, hard_stop_price) for value in floors]
    return {
        "status": "AVAILABLE_CONDITIONAL_ON_POLICY_ACTIVATION",
        "policy_activation_price": activation_price,
        "model_peak_low": _round_tick(
            entry_mid * (1 + market_mfe[0]), tick_size, ROUND_FLOOR
        ),
        "model_peak_high": _round_tick(
            entry_mid * (1 + market_mfe[1]), tick_size, ROUND_CEILING
        ),
        "floor_low": min(floors),
        "floor_high": max(floors),
    }


def _prepare_matrix(
    bundle: LoadedAdvisoryPriceRangeBundle,
    features: pd.DataFrame,
) -> pd.DataFrame:
    missing = sorted(set(MODEL_FEATURE_COLUMNS) - set(features.columns))
    if missing:
        raise AdvisoryModelFirstError(
            "price-range inference is missing frozen model features",
            reason_code="ADVISORY_PRICE_RANGE_INFERENCE_FAILED",
            context={"missing_features": missing},
        )
    matrix = features.loc[:, MODEL_FEATURE_COLUMNS].copy()
    for column in matrix.columns:
        if column not in CATEGORICAL_FEATURE_COLUMNS:
            try:
                matrix[column] = pd.to_numeric(matrix[column], errors="raise")
            except (TypeError, ValueError) as exc:
                raise AdvisoryModelFirstError(
                    "price-range feature contains a non-numeric value",
                    reason_code="ADVISORY_PRICE_RANGE_INFERENCE_FAILED",
                    context={"feature": column},
                ) from exc
    vocabulary = bundle.feature_schema.get("categorical_vocabulary") or {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        categories = tuple(int(value) for value in vocabulary.get(column) or ())
        if not categories:
            raise AdvisoryModelFirstError(
                "price-range categorical vocabulary is empty",
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
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
                "price-range head feature identity cannot be read",
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
                context={"head": name},
            ) from exc
        if feature_names != tuple(MODEL_FEATURE_COLUMNS):
            raise AdvisoryModelFirstError(
                "price-range head feature order differs from the frozen schema",
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
                context={"head": name},
            )
    return matrix


def _predict_head(model: Any, matrix: pd.DataFrame, *, head: str) -> np.ndarray:
    try:
        values = np.asarray(model.predict(matrix), dtype=float)
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "price-range head inference failed",
            reason_code="ADVISORY_PRICE_RANGE_INFERENCE_FAILED",
            context={"head": head, "error_type": type(exc).__name__},
        ) from exc
    if values.shape != (len(matrix),) or not np.isfinite(values).all():
        raise AdvisoryModelFirstError(
            "price-range head returned invalid predictions",
            reason_code="ADVISORY_PRICE_RANGE_INFERENCE_FAILED",
            context={"head": head, "shape": list(values.shape)},
        )
    return values


def _nonnegative_policy_int(policy: Mapping[str, Any], field: str) -> int:
    try:
        value = int(policy[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "review policy price-range field is invalid",
            reason_code="ADVISORY_PRICE_RANGE_POLICY_IDENTITY_MISMATCH",
            context={"field": field},
        ) from exc
    if value < 0:
        raise AdvisoryModelFirstError(
            "review policy price-range field must be nonnegative",
            reason_code="ADVISORY_PRICE_RANGE_POLICY_IDENTITY_MISMATCH",
            context={"field": field},
        )
    return value


def _round_tick(value: float, tick_size: float, rounding: str) -> float:
    if not np.isfinite(value) or value <= 0 or tick_size <= 0:
        raise AdvisoryModelFirstError(
            "price is invalid before tick rounding",
            reason_code="ADVISORY_PRICE_RANGE_PROJECTION_INVALID",
        )
    tick = Decimal(str(tick_size))
    normalized_value = Decimal(str(round(value, 12)))
    units = (normalized_value / tick).quantize(Decimal("1"), rounding=rounding)
    return float(units * tick)
