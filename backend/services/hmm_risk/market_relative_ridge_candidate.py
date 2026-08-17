"""P2-3B direct and P2-3C market-conditioned sector-rotation Ridge candidates.

The module implements the user-approved C-011-P2-3B-D1 through D6 and
C-011-P2-3C-D1 through D6 contracts.  It is offline-only: development folds may
select one alpha per level, while the untouched holdout, production model/READY
state, database, and runtime remain inaccessible.  A successful run writes only
a compact candidate receipt through the explicit repository-external writer.
"""

from __future__ import annotations

import json
import math
import os
import platform
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.hmm_risk.market_relative_jump_spike import (
    DEVELOPMENT_END,
    DEVELOPMENT_START,
    DEVELOPMENT_TRADING_DAYS,
    FOLDS,
    HOLDOUT_END,
    HOLDOUT_START,
    HOLDOUT_TRADING_DAYS,
    MARKET_FEATURES,
    RELATIVE_FEATURES,
    JumpFit,
    JumpSpikeError,
    PreparedComponent,
    SequenceData,
    causal_states,
    fit_jump_model,
    jump_failure_receipt,
    jump_fit_receipt,
    market_planned_fit_count,
    market_development_score,
    market_fold_metrics,
    prepare_component,
    run_market_component,
    semantic_mapping,
    state_rows,
)
from backend.services.hmm_risk.state_model_set import canonical_json_bytes, canonical_sha256, sha256_bytes

CONTRACT_VERSION = "C-011-P2-3B-D1-D6"
ALGORITHM_VERSION = "hmm_risk_market_relative_ridge_candidate_v1"
MODEL_ORIGIN = "market_relative_ridge_v1"
REPORT_SCHEMA_VERSION = "hmm_risk_market_relative_ridge_candidate_report_v1"
REQUEST_SCHEMA_VERSION = "hmm_risk_market_relative_ridge_candidate_request_v1"
COMPONENT_SCHEMA_VERSION = "hmm_risk_market_relative_ridge_component_v1"

P2_3C_CONTRACT_VERSION = "C-011-P2-3C-D1-D6"
P2_3C_ALGORITHM_VERSION = "hmm_risk_market_conditioned_ridge_candidate_v1"
P2_3C_MODEL_ORIGIN = "market_conditioned_ridge_v1"
P2_3C_REPORT_SCHEMA_VERSION = "hmm_risk_market_conditioned_ridge_candidate_report_v1"
P2_3C_REQUEST_SCHEMA_VERSION = "hmm_risk_market_conditioned_ridge_candidate_request_v1"
P2_3C_COMPONENT_SCHEMA_VERSION = "hmm_risk_market_conditioned_ridge_component_v1"
P2_3C_MARKET_COMPONENT_SCHEMA_VERSION = "hmm_risk_market_conditioning_component_v1"
P2_3C_FIXED_JUMP_PENALTY = 4.0
P2_3C_FIXED_SEED = 42
P2_3C_ATTEMPT_INDEX = 3
P2_3A_NOT_AVAILABLE_REPORT_SHA256 = "034fdf3c7a2354bad62bdea0a55b675f2552c42a65d1ccacbc454561e75f12ec"
P2_3B_NOT_AVAILABLE_REPORT_SHA256 = "d3298654ed9f2080f4623c2c50721ebf9951d2034d42cfdfe225f36e4ee0fc45"
P2_3C_INTERACTION_FEATURES = tuple(f"market_sign_x_{name}" for name in RELATIVE_FEATURES)
P2_3C_FEATURES = (*RELATIVE_FEATURES, *P2_3C_INTERACTION_FEATURES)

ALPHA_GRID = (0.01, 0.1, 1.0, 10.0, 100.0)
ALPHA_METRIC_TOLERANCE = 1e-4
STATE_TIE_TOLERANCE = 1e-12
TARGET_HORIZON = 10
STATE_FRACTION = 0.20
MINIMUM_EXTREME_COUNT = 5
LEVEL_SPECS: dict[str, dict[str, Any]] = {
    "L1": {"expected_sector_count": 31, "minimum_daily_count": 28},
    "L2": {"expected_sector_count": 131, "minimum_daily_count": 118},
}

REASON_INPUT_IDENTITY = "hmm_risk_rotation_input_identity_mismatch"
REASON_TARGET_UNAVAILABLE = "hmm_risk_rotation_target_unavailable"
REASON_FIT_FAILED = "hmm_risk_rotation_fit_failed"
REASON_SCORE_NON_FINITE = "hmm_risk_rotation_score_non_finite"
REASON_METRIC_UNAVAILABLE = "hmm_risk_rotation_metric_unavailable"
REASON_SELECTION_UNAVAILABLE = "hmm_risk_rotation_selection_unavailable"
REASON_DEVELOPMENT_NON_POSITIVE = "hmm_risk_rotation_development_effect_non_positive"
REASON_STATE_TIE = "hmm_risk_rotation_state_boundary_tie_insufficient"
REASON_HOLDOUT = "hmm_risk_rotation_holdout_access_forbidden"
REASON_COLLISION = "hmm_risk_rotation_candidate_collision"
REASON_READBACK = "hmm_risk_rotation_candidate_readback_mismatch"
REASON_UNEXPECTED = "hmm_risk_rotation_unexpected_error"
REASON_MARKET_IDENTITY = "hmm_risk_market_conditioning_identity_mismatch"
REASON_MARKET_REGIME = "hmm_risk_market_conditioning_regime_unavailable"
REASON_INTERACTION_NON_FINITE = "hmm_risk_market_conditioning_interaction_non_finite"


class RidgeCandidateError(RuntimeError):
    """Typed fail-closed error for the P2-3B and P2-3C candidates."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        stage: str,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.stage = stage
        self.evidence = dict(evidence or {})


@dataclass(frozen=True)
class TargetRows:
    level: str
    start: date
    end: date
    eligible_dates: tuple[date, ...]
    values: dict[tuple[str, date], float]
    receipt: dict[str, Any]


@dataclass(frozen=True)
class RidgeFit:
    alpha: float
    coefficient: np.ndarray
    intercept: float
    row_count: int
    feature_count: int
    training_identity_sha256: str

    def predict(self, values: np.ndarray) -> np.ndarray:
        matrix = np.asarray(values, dtype="<f8")
        if matrix.ndim != 2 or matrix.shape[1] != self.coefficient.shape[0]:
            raise _fail(REASON_SCORE_NON_FINITE, "Ridge prediction feature shape is invalid", stage="score")
        result = np.asarray(matrix @ self.coefficient + self.intercept, dtype="<f8")
        if result.ndim != 1 or not np.isfinite(result).all():
            raise _fail(REASON_SCORE_NON_FINITE, "Ridge prediction is non-finite", stage="score")
        return result


@dataclass(frozen=True)
class MarketConditioningFold:
    fold: str
    train_states: dict[date, str]
    validation_states: dict[date, str]
    receipt: dict[str, Any]


@dataclass(frozen=True)
class ConditionedFeatureComponent:
    component: str
    level: str
    feature_names: tuple[str, ...]
    canonical_codes: tuple[str, ...]
    sequences: tuple[SequenceData, ...]
    unavailable_items: tuple[dict[str, Any], ...]
    valid_row_count: int
    valid_identity_sha256: str


@dataclass(frozen=True)
class ConditionedComponent:
    component: ConditionedFeatureComponent
    receipt: dict[str, Any]


def _fail(
    reason_code: str,
    message: str,
    *,
    stage: str,
    evidence: Mapping[str, Any] | None = None,
) -> RidgeCandidateError:
    return RidgeCandidateError(reason_code, message, stage=stage, evidence=evidence)


def _as_date(value: date | pd.Timestamp | str) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise _fail(REASON_INPUT_IDENTITY, "date identity is invalid", stage="input") from exc


def _require_sha256(value: Any, field: str) -> str:
    text = str(value or "")
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise _fail(REASON_INPUT_IDENTITY, f"{field} must be lowercase SHA-256", stage="input")
    return text


def _panel_frame(panel: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(panel, pd.DataFrame) or not isinstance(panel.index, pd.MultiIndex):
        raise _fail(REASON_INPUT_IDENTITY, "panel must use a two-level MultiIndex", stage="input")
    if len(panel.index.names) != 2 or panel.index.names[0] != "trade_date":
        raise _fail(REASON_INPUT_IDENTITY, "panel index must begin with trade_date", stage="input")
    frame = panel.reset_index().rename(columns={panel.index.names[1]: "sector_code"})
    frame["trade_date"] = frame["trade_date"].map(_as_date)
    frame["sector_code"] = frame["sector_code"].map(str)
    if frame.duplicated(["trade_date", "sector_code"]).any():
        raise _fail(REASON_INPUT_IDENTITY, "panel contains duplicate sector/date rows", stage="input")
    return frame.sort_values(["trade_date", "sector_code"], kind="mergesort").reset_index(drop=True)


def _calendar_slice(
    calendar: Sequence[date],
    *,
    start: date,
    end: date,
    expected_days: int,
) -> tuple[date, ...]:
    result = tuple(day for day in calendar if start <= day <= end)
    if (
        len(result) != expected_days
        or not result
        or result[0] != start
        or result[-1] != end
        or result != tuple(sorted(set(result)))
    ):
        raise _fail(
            REASON_INPUT_IDENTITY,
            "calendar slice differs from the approved development fold",
            stage="fold_boundary",
            evidence={"start": start.isoformat(), "end": end.isoformat(), "actual_count": len(result)},
        )
    return result


def _future_cumulative(values: Sequence[float]) -> float:
    product = 1.0
    for value in values:
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("future return is non-finite")
        product *= 1.0 + number
    result = product - 1.0
    if not math.isfinite(result):
        raise ValueError("future cumulative return is non-finite")
    return result


def _benchmark_rows(dataset_manifest: Mapping[str, Any]) -> dict[date, float]:
    calendar = dataset_manifest.get("calendar_benchmark")
    rows = calendar.get("rows") if isinstance(calendar, Mapping) else None
    if not isinstance(rows, list):
        raise _fail(REASON_INPUT_IDENTITY, "benchmark manifest rows are missing", stage="input")
    output: dict[date, float] = {}
    for row in rows:
        if not isinstance(row, list) or len(row) != 2:
            raise _fail(REASON_INPUT_IDENTITY, "benchmark manifest row is invalid", stage="input")
        try:
            day = _as_date(row[0])
            value = float(row[1])
        except (TypeError, ValueError) as exc:
            raise _fail(REASON_INPUT_IDENTITY, "benchmark manifest row is invalid", stage="input") from exc
        if day in output or not math.isfinite(value):
            raise _fail(REASON_INPUT_IDENTITY, "benchmark manifest is duplicate or non-finite", stage="input")
        output[day] = value
    return output


def build_target_rows(
    panel: pd.DataFrame,
    benchmark_returns: Mapping[date, float],
    calendar: Sequence[date],
    *,
    level: str,
    start: date,
    end: date,
    expected_days: int,
    expected_sector_count: int,
    minimum_daily_count: int,
    horizon: int = TARGET_HORIZON,
) -> TargetRows:
    """Build the approved daily-centered future excess target without leakage."""

    approved_dates = _calendar_slice(calendar, start=start, end=end, expected_days=expected_days)
    if horizon != TARGET_HORIZON:
        raise _fail(REASON_INPUT_IDENTITY, "target horizon differs from the approved contract", stage="target")
    frame = _panel_frame(panel)
    if "daily_return" not in frame.columns:
        raise _fail(REASON_INPUT_IDENTITY, "panel daily_return is missing", stage="target")
    codes = tuple(sorted(frame["sector_code"].unique().tolist()))
    if len(codes) != expected_sector_count:
        raise _fail(
            REASON_INPUT_IDENTITY,
            f"{level} canonical sector count is invalid",
            stage="target",
            evidence={"expected": expected_sector_count, "actual": len(codes)},
        )
    returns: dict[tuple[str, date], float] = {}
    for row in frame.itertuples(index=False):
        try:
            value = float(getattr(row, "daily_return"))
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            returns[(str(getattr(row, "sector_code")), getattr(row, "trade_date"))] = value

    calendar_positions = {day: index for index, day in enumerate(calendar)}
    eligible_dates = tuple(
        day
        for day in approved_dates
        if calendar_positions[day] + horizon < len(calendar) and calendar[calendar_positions[day] + horizon] <= end
    )
    excluded_tail = tuple(day for day in approved_dates if day not in set(eligible_dates))
    values: dict[tuple[str, date], float] = {}
    target_rows: list[list[Any]] = []
    denominator_rows: list[list[Any]] = []
    unavailable_dates: list[dict[str, Any]] = []
    benchmark_identity: list[list[Any]] = []
    for day in eligible_dates:
        position = calendar_positions[day]
        future_dates = calendar[position + 1 : position + horizon + 1]
        try:
            benchmark = _future_cumulative([benchmark_returns[item] for item in future_dates])
        except (KeyError, ValueError):
            unavailable_dates.append(
                {
                    "trade_date": day.isoformat(),
                    "reason_code": REASON_TARGET_UNAVAILABLE,
                    "failure": "benchmark_future_return_unavailable",
                }
            )
            continue
        benchmark_identity.append([day.isoformat(), benchmark])
        outcomes: list[tuple[str, float]] = []
        for code in codes:
            try:
                sector_return = _future_cumulative([returns[(code, item)] for item in future_dates])
            except (KeyError, ValueError):
                continue
            outcomes.append((code, sector_return - benchmark))
        denominator_rows.append([day.isoformat(), len(outcomes)])
        if len(outcomes) < minimum_daily_count:
            unavailable_dates.append(
                {
                    "trade_date": day.isoformat(),
                    "reason_code": REASON_TARGET_UNAVAILABLE,
                    "available_count": len(outcomes),
                    "required_count": minimum_daily_count,
                }
            )
            continue
        median = float(np.median(np.asarray([item[1] for item in outcomes], dtype=np.float64)))
        if not math.isfinite(median):
            raise _fail(REASON_TARGET_UNAVAILABLE, "daily target median is non-finite", stage="target")
        for code, excess in outcomes:
            target = float(excess - median)
            if not math.isfinite(target):
                raise _fail(REASON_TARGET_UNAVAILABLE, "daily-centered target is non-finite", stage="target")
            values[(code, day)] = target
            target_rows.append([day.isoformat(), code, target])

    body = {
        "schema_version": "hmm_risk_rotation_target_rows_v1",
        "level": level,
        "horizon": horizon,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "expected_sector_count": expected_sector_count,
        "minimum_daily_count": minimum_daily_count,
        "eligible_dates": [day.isoformat() for day in eligible_dates],
        "eligible_date_set_sha256": canonical_sha256([day.isoformat() for day in eligible_dates]),
        "excluded_tail_dates": [day.isoformat() for day in excluded_tail],
        "excluded_tail_date_set_sha256": canonical_sha256([day.isoformat() for day in excluded_tail]),
        "target_row_count": len(target_rows),
        "target_rows_sha256": canonical_sha256(target_rows),
        "target_identity_sha256": canonical_sha256([[item[0], item[1]] for item in target_rows]),
        "denominator_rows": denominator_rows,
        "denominator_sha256": canonical_sha256(denominator_rows),
        "benchmark_identity_sha256": canonical_sha256(benchmark_identity),
        "unavailable_dates": unavailable_dates,
        "unavailable_date_count": len(unavailable_dates),
    }
    return TargetRows(
        level=level,
        start=start,
        end=end,
        eligible_dates=eligible_dates,
        values=values,
        receipt={**body, "receipt_sha256": canonical_sha256(body)},
    )


def _feature_rows(
    component: PreparedComponent | ConditionedFeatureComponent,
) -> dict[tuple[str, date], np.ndarray]:
    rows: dict[tuple[str, date], np.ndarray] = {}
    for sequence in component.sequences:
        for day, values in zip(sequence.dates, sequence.values, strict=True):
            key = (sequence.key, day)
            if key in rows:
                raise _fail(REASON_INPUT_IDENTITY, "feature identity is duplicated", stage="input")
            vector = np.asarray(values, dtype=np.float64)
            if vector.shape != (len(component.feature_names),) or not np.isfinite(vector).all():
                raise _fail(REASON_INPUT_IDENTITY, "feature vector is invalid", stage="input")
            rows[key] = vector
    return rows


def _record_fixed_market_fit(
    component: PreparedComponent,
    *,
    attempt_log: list[dict[str, Any]],
    context: Mapping[str, Any],
) -> JumpFit:
    try:
        fit = fit_jump_model(
            component,
            state_count=2,
            jump_penalty=P2_3C_FIXED_JUMP_PENALTY,
            seed=P2_3C_FIXED_SEED,
        )
        raw_receipt = jump_fit_receipt(fit, component)
    except Exception as exc:
        raw_receipt = jump_failure_receipt(P2_3C_FIXED_SEED, P2_3C_FIXED_JUMP_PENALTY, exc)
        body = {key: value for key, value in raw_receipt.items() if key != "receipt_sha256"}
        body = {**dict(context), **body}
        attempt_log.append({**body, "receipt_sha256": canonical_sha256(body)})
        raise
    body = {key: value for key, value in raw_receipt.items() if key != "receipt_sha256"}
    body = {**dict(context), **body}
    attempt_log.append({**body, "receipt_sha256": canonical_sha256(body)})
    return fit


def _market_state_receipt(
    component: PreparedComponent,
    rows: Mapping[tuple[str, date], str],
) -> tuple[dict[date, str], dict[str, Any]]:
    if len(component.sequences) != 1 or component.sequences[0].key != "market":
        raise _fail(REASON_MARKET_IDENTITY, "market component sequence identity is invalid", stage="market_state")
    sequence = component.sequences[0]
    expected_keys = {("market", day) for day in sequence.dates}
    if set(rows) != expected_keys:
        raise _fail(
            REASON_MARKET_IDENTITY,
            "market state rows do not match the prepared date identity",
            stage="market_state",
            evidence={"expected_count": len(expected_keys), "actual_count": len(rows)},
        )
    by_date: dict[date, str] = {}
    state_rows_payload: list[list[str]] = []
    for day in sequence.dates:
        label = rows[("market", day)]
        if label not in {"risk_on", "risk_off"}:
            raise _fail(REASON_MARKET_IDENTITY, "market semantic state is invalid", stage="market_state")
        by_date[day] = label
        state_rows_payload.append([day.isoformat(), label])
    transition_counts = {
        "risk_off->risk_off": 0,
        "risk_off->risk_on": 0,
        "risk_on->risk_off": 0,
        "risk_on->risk_on": 0,
    }
    for index in range(1, len(sequence.dates)):
        if sequence.ordinals[index] != sequence.ordinals[index - 1] + 1:
            continue
        previous = by_date[sequence.dates[index - 1]]
        current = by_date[sequence.dates[index]]
        transition_counts[f"{previous}->{current}"] += 1
    state_counts = {label: sum(value == label for value in by_date.values()) for label in ("risk_off", "risk_on")}
    body = {
        "arrival_cost_policy": "zero_at_each_segment_start_no_train_carry",
        "state_row_count": len(state_rows_payload),
        "state_rows": state_rows_payload,
        "state_rows_sha256": canonical_sha256(state_rows_payload),
        "date_set_sha256": canonical_sha256([item[0] for item in state_rows_payload]),
        "state_counts": state_counts,
        "transition_counts": transition_counts,
        "transition_counts_used_for_acceptance": False,
    }
    return by_date, {**body, "receipt_sha256": canonical_sha256(body)}


def _market_fold_conditioning(
    inputs: Mapping[str, Any],
    *,
    calendar: tuple[date, ...],
    benchmark: Mapping[date, float],
    fold: Mapping[str, Any],
    attempt_log: list[dict[str, Any]],
) -> MarketConditioningFold:
    panel = _component_panel(inputs, "L2")
    train = prepare_component(
        panel,
        component="market",
        level="L2",
        feature_names=MARKET_FEATURES,
        calendar=calendar,
        start=fold["train_start"],
        end=fold["train_end"],
        expected_days=fold["train_days"],
        expected_sector_count=LEVEL_SPECS["L2"]["expected_sector_count"],
        minimum_daily_count=LEVEL_SPECS["L2"]["minimum_daily_count"],
        relative=False,
    )
    fit = _record_fixed_market_fit(
        train,
        attempt_log=attempt_log,
        context={"component": "market", "fold": fold["fold"], "phase": "development"},
    )
    validation = prepare_component(
        panel,
        component="market",
        level="L2",
        feature_names=MARKET_FEATURES,
        calendar=calendar,
        start=fold["validation_start"],
        end=fold["validation_end"],
        expected_days=fold["validation_days"],
        expected_sector_count=LEVEL_SPECS["L2"]["expected_sector_count"],
        minimum_daily_count=LEVEL_SPECS["L2"]["minimum_daily_count"],
        relative=False,
        preprocessor=train.preprocessor,
    )
    mapping = semantic_mapping("market", MARKET_FEATURES, fit.centers)
    train_states, train_state_receipt = _market_state_receipt(
        train,
        state_rows(train, causal_states(train, fit.centers, P2_3C_FIXED_JUMP_PENALTY), mapping),
    )
    validation_states, validation_state_receipt = _market_state_receipt(
        validation,
        state_rows(
            validation,
            causal_states(validation, fit.centers, P2_3C_FIXED_JUMP_PENALTY),
            mapping,
        ),
    )
    missing = {
        "train": sorted({"risk_off", "risk_on"} - set(train_states.values())),
        "validation": sorted({"risk_off", "risk_on"} - set(validation_states.values())),
    }
    if missing["train"] or missing["validation"]:
        raise _fail(
            REASON_MARKET_REGIME,
            "market conditioning fold does not contain both semantic states",
            stage="market_conditioning",
            evidence={
                "fold": fold["fold"],
                "missing_states": missing,
                "train_state_receipt": train_state_receipt,
                "validation_state_receipt": validation_state_receipt,
            },
        )
    validation_rows = {("market", day): label for day, label in validation_states.items()}
    metrics = market_fold_metrics(
        validation_rows,
        benchmark,
        calendar,
        validation_start=fold["validation_start"],
        validation_end=fold["validation_end"],
    )
    body = {
        "fold": fold["fold"],
        "train_start": fold["train_start"].isoformat(),
        "train_end": fold["train_end"].isoformat(),
        "validation_start": fold["validation_start"].isoformat(),
        "validation_end": fold["validation_end"].isoformat(),
        "jump_penalty": P2_3C_FIXED_JUMP_PENALTY,
        "selected_seed": P2_3C_FIXED_SEED,
        "preprocess": train.preprocessor.payload(),
        "preprocess_sha256": canonical_sha256(train.preprocessor.payload()),
        "fit": jump_fit_receipt(fit, train),
        "centers": np.asarray(fit.centers, dtype="<f8").tolist(),
        "centers_sha256": sha256_bytes(np.asarray(fit.centers, dtype="<f8").tobytes()),
        "semantic_mapping": {str(key): value for key, value in sorted(mapping.items())},
        "semantic_mapping_sha256": canonical_sha256({str(key): value for key, value in sorted(mapping.items())}),
        "train_states": train_state_receipt,
        "validation_states": validation_state_receipt,
        "metrics": metrics,
        "metric_valid": metrics.get("metric_valid") is True,
        "holdout_accessed": False,
    }
    receipt = {**body, "receipt_sha256": canonical_sha256(body)}
    return MarketConditioningFold(
        fold=str(fold["fold"]),
        train_states=train_states,
        validation_states=validation_states,
        receipt=receipt,
    )


def _run_market_conditioning_development(
    inputs: Mapping[str, Any],
    *,
    calendar: tuple[date, ...],
    benchmark: Mapping[date, float],
    attempt_log: list[dict[str, Any]],
) -> tuple[tuple[MarketConditioningFold, ...], dict[str, Any]]:
    folds = tuple(
        _market_fold_conditioning(
            inputs,
            calendar=calendar,
            benchmark=benchmark,
            fold=fold,
            attempt_log=attempt_log,
        )
        for fold in FOLDS
    )
    fold_receipts = [item.receipt for item in folds]
    score = market_development_score(fold_receipts)
    if score.get("lambda_eligible") is not True:
        reason_code = str(score.get("market_selection_reason_code") or REASON_MARKET_IDENTITY)
        raise _fail(
            reason_code,
            "fixed market conditioning component fails development acceptance",
            stage="market_development_acceptance",
            evidence={
                "fixed_jump_penalty": P2_3C_FIXED_JUMP_PENALTY,
                "fixed_seed": P2_3C_FIXED_SEED,
                "fold_receipts": fold_receipts,
                "fold_receipts_sha256": canonical_sha256(fold_receipts),
                "development_score": score,
            },
        )
    body = {
        "schema_version": P2_3C_MARKET_COMPONENT_SCHEMA_VERSION,
        "component": "market",
        "phase": "development",
        "fixed_jump_penalty": P2_3C_FIXED_JUMP_PENALTY,
        "fixed_seed": P2_3C_FIXED_SEED,
        "folds": fold_receipts,
        "fold_count": len(fold_receipts),
        "development_score": score,
        "holdout_accessed": False,
    }
    return folds, {**body, "receipt_sha256": canonical_sha256(body)}


def _condition_component(
    component: PreparedComponent,
    market_states: Mapping[date, str],
    *,
    fold: str,
    phase: str,
) -> ConditionedComponent:
    expected_dates = {day for sequence in component.sequences for day in sequence.dates}
    if set(market_states) != expected_dates:
        raise _fail(
            REASON_MARKET_IDENTITY,
            "market states and sector feature dates do not have the same identity",
            stage="interaction",
            evidence={
                "fold": fold,
                "phase": phase,
                "sector_date_count": len(expected_dates),
                "market_date_count": len(market_states),
            },
        )
    sequences: list[SequenceData] = []
    identity_rows: list[list[str]] = []
    matrices: list[np.ndarray] = []
    for sequence in component.sequences:
        base = np.asarray(sequence.values, dtype="<f8")
        if base.ndim != 2 or base.shape[1] != len(RELATIVE_FEATURES):
            raise _fail(
                REASON_MARKET_IDENTITY,
                "sector feature matrix shape is invalid for market conditioning",
                stage="interaction",
            )
        if not np.isfinite(base).all():
            raise _fail(
                REASON_INTERACTION_NON_FINITE,
                "sector feature matrix is non-finite before market conditioning",
                stage="interaction",
                evidence={"fold": fold, "phase": phase, "sequence_key": sequence.key},
            )
        labels = [market_states[day] for day in sequence.dates]
        if any(label not in {"risk_on", "risk_off"} for label in labels):
            raise _fail(REASON_MARKET_IDENTITY, "market sign identity is invalid", stage="interaction")
        signs = np.asarray([1.0 if label == "risk_on" else -1.0 for label in labels], dtype="<f8")
        conditioned = np.asarray(np.concatenate([base, base * signs[:, None]], axis=1), dtype="<f8")
        if conditioned.shape != (base.shape[0], len(P2_3C_FEATURES)) or not np.isfinite(conditioned).all():
            raise _fail(
                REASON_INTERACTION_NON_FINITE,
                "market-conditioned feature matrix is non-finite or has an invalid shape",
                stage="interaction",
                evidence={"fold": fold, "phase": phase, "sequence_key": sequence.key},
            )
        sequences.append(
            SequenceData(
                key=sequence.key,
                dates=sequence.dates,
                ordinals=sequence.ordinals,
                values=conditioned,
            )
        )
        matrices.append(conditioned)
        identity_rows.extend(
            [[sequence.key, day.isoformat(), label] for day, label in zip(sequence.dates, labels, strict=True)]
        )
    matrix = np.vstack(matrices).astype("<f8", copy=False)
    body = {
        "fold": fold,
        "phase": phase,
        "base_feature_names": list(RELATIVE_FEATURES),
        "conditioned_feature_names": list(P2_3C_FEATURES),
        "feature_count": len(P2_3C_FEATURES),
        "dtype": "float64_le",
        "shape": list(matrix.shape),
        "matrix_sha256": sha256_bytes(matrix.tobytes()),
        "interaction_row_count": len(identity_rows),
        "market_state_identity_sha256": canonical_sha256(identity_rows),
        "market_sign": {"risk_on": 1.0, "risk_off": -1.0},
    }
    conditioned_component = ConditionedFeatureComponent(
        component=f"{component.component}_market_conditioned",
        level=component.level,
        feature_names=P2_3C_FEATURES,
        canonical_codes=component.canonical_codes,
        sequences=tuple(sequences),
        unavailable_items=component.unavailable_items,
        valid_row_count=component.valid_row_count,
        valid_identity_sha256=component.valid_identity_sha256,
    )
    return ConditionedComponent(
        component=conditioned_component,
        receipt={**body, "receipt_sha256": canonical_sha256(body)},
    )


def _conditioned_fit_receipt(fit: RidgeFit) -> dict[str, Any]:
    if fit.coefficient.shape != (len(P2_3C_FEATURES),) or not np.isfinite(fit.coefficient).all():
        raise _fail(REASON_MARKET_IDENTITY, "conditioned Ridge coefficient identity is invalid", stage="fit_receipt")
    beta = np.asarray(fit.coefficient[: len(RELATIVE_FEATURES)], dtype="<f8")
    gamma = np.asarray(fit.coefficient[len(RELATIVE_FEATURES) :], dtype="<f8")
    risk_on = np.asarray(beta + gamma, dtype="<f8")
    risk_off = np.asarray(beta - gamma, dtype="<f8")
    base = _fit_receipt(fit)
    body = {key: value for key, value in base.items() if key != "receipt_sha256"}
    body.update(
        {
            "feature_names": list(P2_3C_FEATURES),
            "base_coefficient": beta.tolist(),
            "interaction_coefficient": gamma.tolist(),
            "risk_on_slope": risk_on.tolist(),
            "risk_off_slope": risk_off.tolist(),
            "risk_on_slope_sha256": sha256_bytes(risk_on.tobytes()),
            "risk_off_slope_sha256": sha256_bytes(risk_off.tobytes()),
        }
    )
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _regime_split_metrics(
    scores: Mapping[tuple[str, date], float],
    targets: TargetRows,
    states: Mapping[tuple[str, date], str],
    market_states: Mapping[date, str],
) -> dict[str, Any]:
    if not set(targets.eligible_dates).issubset(market_states):
        raise _fail(REASON_MARKET_IDENTITY, "target dates are missing market states", stage="diagnostic")
    output: dict[str, Any] = {}
    for regime in ("risk_off", "risk_on"):
        dates = tuple(day for day in targets.eligible_dates if market_states[day] == regime)
        date_set = set(dates)
        subset_targets = TargetRows(
            level=targets.level,
            start=targets.start,
            end=targets.end,
            eligible_dates=dates,
            values={identity: value for identity, value in targets.values.items() if identity[1] in date_set},
            receipt={},
        )
        metrics = fold_metrics(
            {identity: value for identity, value in scores.items() if identity[1] in date_set},
            subset_targets,
            {identity: value for identity, value in states.items() if identity[1] in date_set},
        )
        output[regime] = {
            "date_count": len(dates),
            "date_set_sha256": canonical_sha256([day.isoformat() for day in dates]),
            "metrics": metrics,
        }
    return output


def _fit_ridge(
    component: PreparedComponent | ConditionedFeatureComponent,
    targets: TargetRows,
    *,
    alpha: float,
    attempt_log: list[dict[str, Any]],
    context: Mapping[str, Any],
) -> RidgeFit:
    feature_rows = _feature_rows(component)
    identities = sorted(set(feature_rows) & set(targets.values), key=lambda item: (item[1], item[0]))
    if not identities:
        raise _fail(REASON_TARGET_UNAVAILABLE, "Ridge training has no feature/target rows", stage="fit")
    matrix = np.asarray([feature_rows[item] for item in identities], dtype="<f8")
    target = np.asarray([targets.values[item] for item in identities], dtype="<f8")
    identity_payload = [[day.isoformat(), code] for code, day in identities]
    attempt_base = {
        **dict(context),
        "alpha": float(alpha),
        "row_count": len(identities),
        "training_identity_sha256": canonical_sha256(identity_payload),
    }
    try:
        model = Ridge(
            alpha=float(alpha),
            fit_intercept=True,
            solver="svd",
            positive=False,
            copy_X=True,
            tol=1e-4,
            max_iter=None,
            random_state=None,
        )
        model.fit(matrix, target)
        coefficient = np.asarray(model.coef_, dtype="<f8")
        intercept = float(model.intercept_)
        if (
            coefficient.shape != (matrix.shape[1],)
            or not np.isfinite(coefficient).all()
            or not math.isfinite(intercept)
        ):
            raise ValueError("Ridge parameters are non-finite or have an invalid shape")
    except Exception as exc:
        attempt = {
            **attempt_base,
            "status": "fit_failed",
            "reason_code": REASON_FIT_FAILED,
            "exception_type": type(exc).__name__,
            "error_message": str(exc),
        }
        attempt_log.append({**attempt, "receipt_sha256": canonical_sha256(attempt)})
        raise _fail(
            REASON_FIT_FAILED,
            "Ridge fit failed",
            stage="fit",
            evidence={"exception_type": type(exc).__name__, **attempt_base},
        ) from exc
    fit = RidgeFit(
        alpha=float(alpha),
        coefficient=coefficient,
        intercept=intercept,
        row_count=len(identities),
        feature_count=matrix.shape[1],
        training_identity_sha256=canonical_sha256(identity_payload),
    )
    attempt = {
        **attempt_base,
        "status": "fit_completed",
        "feature_count": fit.feature_count,
        "coefficient_sha256": sha256_bytes(np.asarray(coefficient, dtype="<f8").tobytes()),
        "intercept": intercept,
    }
    attempt_log.append({**attempt, "receipt_sha256": canonical_sha256(attempt)})
    return fit


def _fit_receipt(fit: RidgeFit) -> dict[str, Any]:
    body = {
        "status": "fit_completed",
        "alpha": fit.alpha,
        "fit_intercept": True,
        "solver": "svd",
        "positive": False,
        "copy_X": True,
        "tol": 1e-4,
        "max_iter": None,
        "random_state": None,
        "dtype": "float64_le",
        "row_count": fit.row_count,
        "feature_count": fit.feature_count,
        "training_identity_sha256": fit.training_identity_sha256,
        "coefficient": fit.coefficient.tolist(),
        "coefficient_sha256": sha256_bytes(np.asarray(fit.coefficient, dtype="<f8").tobytes()),
        "intercept": fit.intercept,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def predict_scores(
    component: PreparedComponent | ConditionedFeatureComponent,
    fit: RidgeFit,
) -> tuple[dict[tuple[str, date], float], dict[str, Any]]:
    feature_rows = _feature_rows(component)
    identities = sorted(feature_rows, key=lambda item: (item[1], item[0]))
    if not identities:
        raise _fail(REASON_SCORE_NON_FINITE, "prediction has no feature rows", stage="score")
    matrix = np.asarray([feature_rows[item] for item in identities], dtype="<f8")
    predicted = fit.predict(matrix)
    scores = {identity: float(value) for identity, value in zip(identities, predicted, strict=True)}
    rows = [[day.isoformat(), code, scores[(code, day)]] for code, day in identities]
    body = {
        "score_row_count": len(rows),
        "score_rows_sha256": canonical_sha256(rows),
        "score_identity_sha256": canonical_sha256([[item[0], item[1]] for item in rows]),
    }
    return scores, {**body, "receipt_sha256": canonical_sha256(body)}


def _tie_groups(rows: list[tuple[str, float]]) -> list[tuple[int, int]]:
    groups: list[tuple[int, int]] = []
    start = 0
    for index in range(1, len(rows)):
        if abs(rows[index][1] - rows[index - 1][1]) > STATE_TIE_TOLERANCE:
            groups.append((start, index))
            start = index
    groups.append((start, len(rows)))
    return groups


def project_daily_states(
    scores: Mapping[tuple[str, date], float],
    *,
    level: str,
    minimum_daily_count: int,
) -> tuple[dict[tuple[str, date], str], dict[str, Any]]:
    """Project finite scores to daily fading/neutral/trending buckets."""

    by_date: dict[date, list[tuple[str, float]]] = {}
    for (code, day), raw_value in scores.items():
        value = float(raw_value)
        if not math.isfinite(value):
            raise _fail(REASON_SCORE_NON_FINITE, "rotation score is non-finite", stage="state_projection")
        by_date.setdefault(day, []).append((str(code), value))
    states: dict[tuple[str, date], str] = {}
    date_receipts: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    for day in sorted(by_date):
        rows = sorted(by_date[day], key=lambda item: (item[1], item[0]))
        if len({item[0] for item in rows}) != len(rows):
            raise _fail(REASON_INPUT_IDENTITY, "score identity is duplicated", stage="state_projection")
        count = len(rows)
        if count < minimum_daily_count:
            unavailable.append(
                {
                    "trade_date": day.isoformat(),
                    "reason_code": REASON_METRIC_UNAVAILABLE,
                    "available_count": count,
                    "required_count": minimum_daily_count,
                }
            )
            continue
        q = max(MINIMUM_EXTREME_COUNT, math.ceil(STATE_FRACTION * count))
        labels = ["fading" if index < q else "trending" if index >= count - q else "neutral" for index in range(count)]
        boundary_groups: list[list[int]] = []
        for start, end in _tie_groups(rows):
            crosses_lower = start <= q - 1 and end > q
            crosses_upper = start <= count - q - 1 and end > count - q
            if crosses_lower or crosses_upper:
                labels[start:end] = ["neutral"] * (end - start)
                boundary_groups.append([start, end])
        fading_count = labels.count("fading")
        trending_count = labels.count("trending")
        if fading_count < MINIMUM_EXTREME_COUNT or trending_count < MINIMUM_EXTREME_COUNT:
            unavailable.append(
                {
                    "trade_date": day.isoformat(),
                    "reason_code": REASON_STATE_TIE,
                    "available_count": count,
                    "q": q,
                    "fading_count": fading_count,
                    "trending_count": trending_count,
                    "boundary_groups": boundary_groups,
                }
            )
            continue
        for (code, _), label in zip(rows, labels, strict=True):
            states[(code, day)] = label
        date_receipts.append(
            {
                "trade_date": day.isoformat(),
                "available_count": count,
                "q": q,
                "fading_count": fading_count,
                "neutral_count": labels.count("neutral"),
                "trending_count": trending_count,
                "boundary_groups": boundary_groups,
                "state_rows_sha256": canonical_sha256(
                    [[code, label] for (code, _), label in zip(rows, labels, strict=True)]
                ),
            }
        )
    body = {
        "schema_version": "hmm_risk_rotation_state_projection_v1",
        "level": level,
        "state_fraction": STATE_FRACTION,
        "minimum_extreme_count": MINIMUM_EXTREME_COUNT,
        "tie_tolerance": STATE_TIE_TOLERANCE,
        "date_receipts": date_receipts,
        "date_receipts_sha256": canonical_sha256(date_receipts),
        "unavailable_dates": unavailable,
        "unavailable_date_count": len(unavailable),
        "state_row_count": len(states),
    }
    return states, {**body, "receipt_sha256": canonical_sha256(body)}


def _rank_average(values: Sequence[float]) -> np.ndarray:
    return pd.Series(np.asarray(values, dtype=np.float64)).rank(method="average").to_numpy(dtype=np.float64)


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < MINIMUM_EXTREME_COUNT:
        return None
    left_rank = _rank_average(left)
    right_rank = _rank_average(right)
    if (
        not np.isfinite(left_rank).all()
        or not np.isfinite(right_rank).all()
        or float(np.var(left_rank)) <= 0.0
        or float(np.var(right_rank)) <= 0.0
    ):
        return None
    result = float(np.corrcoef(left_rank, right_rank)[0, 1])
    return result if math.isfinite(result) else None


def fold_metrics(
    scores: Mapping[tuple[str, date], float],
    targets: TargetRows,
    states: Mapping[tuple[str, date], str],
) -> dict[str, Any]:
    daily_ic: list[list[Any]] = []
    daily_spread: list[list[Any]] = []
    ic_unavailable: list[str] = []
    spread_unavailable: list[str] = []
    for day in targets.eligible_dates:
        observations = sorted(
            (
                code,
                float(score),
                float(targets.values[(code, day)]),
                states.get((code, day)),
            )
            for (code, score_day), score in scores.items()
            if score_day == day and (code, day) in targets.values
        )
        ic = _spearman([item[1] for item in observations], [item[2] for item in observations])
        if ic is None:
            ic_unavailable.append(day.isoformat())
        else:
            daily_ic.append([day.isoformat(), ic])
        trending = [item[2] for item in observations if item[3] == "trending"]
        fading = [item[2] for item in observations if item[3] == "fading"]
        if len(trending) < MINIMUM_EXTREME_COUNT or len(fading) < MINIMUM_EXTREME_COUNT:
            spread_unavailable.append(day.isoformat())
        else:
            spread = math.fsum(trending) / len(trending) - math.fsum(fading) / len(fading)
            if math.isfinite(spread):
                daily_spread.append([day.isoformat(), spread])
            else:
                spread_unavailable.append(day.isoformat())
    required = math.ceil(0.80 * len(targets.eligible_dates))
    metric_valid = bool(targets.eligible_dates) and len(daily_ic) >= required and len(daily_spread) >= required
    body = {
        "schema_version": "hmm_risk_rotation_fold_metrics_v1",
        "eligible_date_count": len(targets.eligible_dates),
        "eligible_date_set_sha256": canonical_sha256([day.isoformat() for day in targets.eligible_dates]),
        "required_date_count": required,
        "rank_ic_available_date_count": len(daily_ic),
        "spread_available_date_count": len(daily_spread),
        "rank_ic_unavailable_dates": ic_unavailable,
        "spread_unavailable_dates": spread_unavailable,
        "daily_rank_ic": daily_ic,
        "daily_rank_ic_sha256": canonical_sha256(daily_ic),
        "daily_spread": daily_spread,
        "daily_spread_sha256": canonical_sha256(daily_spread),
        "mean_rank_ic": math.fsum(float(item[1]) for item in daily_ic) / len(daily_ic) if daily_ic else None,
        "mean_spread": (
            math.fsum(float(item[1]) for item in daily_spread) / len(daily_spread) if daily_spread else None
        ),
        "metric_valid": metric_valid,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _select_alpha(alpha_receipts: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    eligible = [item for item in alpha_receipts if item.get("alpha_eligible") is True]
    if not eligible:
        raise _fail(
            REASON_SELECTION_UNAVAILABLE,
            "no alpha has three valid development folds",
            stage="alpha_selection",
            evidence={
                "alpha_receipts": list(alpha_receipts),
                "alpha_receipts_sha256": canonical_sha256(alpha_receipts),
            },
        )

    def better(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
        left_ic = float(left["median_rank_ic"])
        right_ic = float(right["median_rank_ic"])
        if abs(left_ic - right_ic) > ALPHA_METRIC_TOLERANCE:
            return left_ic > right_ic
        left_spread = float(left["median_spread"])
        right_spread = float(right["median_spread"])
        if abs(left_spread - right_spread) > ALPHA_METRIC_TOLERANCE:
            return left_spread > right_spread
        return float(left["alpha"]) > float(right["alpha"])

    selected = eligible[0]
    for candidate in eligible[1:]:
        if better(candidate, selected):
            selected = candidate
    return selected


def _require_positive_development(level: str, selected: Mapping[str, Any]) -> None:
    if float(selected["median_rank_ic"]) > 0.0 and float(selected["median_spread"]) > 0.0:
        return
    raise _fail(
        REASON_DEVELOPMENT_NON_POSITIVE,
        f"{level} selected development effect is not strictly positive",
        stage="development_acceptance",
        evidence={
            "level": level,
            "selected_alpha": selected["alpha"],
            "median_rank_ic": selected["median_rank_ic"],
            "median_spread": selected["median_spread"],
        },
    )


def _component_panel(inputs: Mapping[str, Any], level: str) -> pd.DataFrame:
    panel = inputs.get("panel" if level == "L1" else "l2_panel")
    if not isinstance(panel, pd.DataFrame):
        raise _fail(REASON_INPUT_IDENTITY, f"{level} panel is missing", stage="input")
    return panel


def _prepare_fold(
    panel: pd.DataFrame,
    benchmark: Mapping[date, float],
    calendar: tuple[date, ...],
    *,
    level: str,
    fold: Mapping[str, Any],
) -> tuple[PreparedComponent, TargetRows, PreparedComponent, TargetRows]:
    spec = LEVEL_SPECS[level]
    train = prepare_component(
        panel,
        component=f"{level}_ridge",
        level=level,
        feature_names=RELATIVE_FEATURES,
        calendar=calendar,
        start=fold["train_start"],
        end=fold["train_end"],
        expected_days=fold["train_days"],
        expected_sector_count=spec["expected_sector_count"],
        minimum_daily_count=spec["minimum_daily_count"],
        relative=True,
    )
    train_target = build_target_rows(
        panel,
        benchmark,
        calendar,
        level=level,
        start=fold["train_start"],
        end=fold["train_end"],
        expected_days=fold["train_days"],
        expected_sector_count=spec["expected_sector_count"],
        minimum_daily_count=spec["minimum_daily_count"],
    )
    validation = prepare_component(
        panel,
        component=f"{level}_ridge",
        level=level,
        feature_names=RELATIVE_FEATURES,
        calendar=calendar,
        start=fold["validation_start"],
        end=fold["validation_end"],
        expected_days=fold["validation_days"],
        expected_sector_count=spec["expected_sector_count"],
        minimum_daily_count=spec["minimum_daily_count"],
        relative=True,
        preprocessor=train.preprocessor,
    )
    validation_target = build_target_rows(
        panel,
        benchmark,
        calendar,
        level=level,
        start=fold["validation_start"],
        end=fold["validation_end"],
        expected_days=fold["validation_days"],
        expected_sector_count=spec["expected_sector_count"],
        minimum_daily_count=spec["minimum_daily_count"],
    )
    return train, train_target, validation, validation_target


def _run_level(
    level: str,
    *,
    inputs: Mapping[str, Any],
    calendar: tuple[date, ...],
    benchmark: Mapping[date, float],
    attempt_log: list[dict[str, Any]],
) -> dict[str, Any]:
    if level not in LEVEL_SPECS:
        raise _fail(REASON_INPUT_IDENTITY, f"unknown level: {level}", stage="input")
    panel = _component_panel(inputs, level)
    prepared_folds = [(fold, *_prepare_fold(panel, benchmark, calendar, level=level, fold=fold)) for fold in FOLDS]
    alpha_receipts: list[dict[str, Any]] = []
    for alpha in ALPHA_GRID:
        fold_receipts: list[dict[str, Any]] = []
        for fold, train, train_target, validation, validation_target in prepared_folds:
            fit = _fit_ridge(
                train,
                train_target,
                alpha=alpha,
                attempt_log=attempt_log,
                context={"component": level, "fold": fold["fold"], "phase": "selection"},
            )
            scores, score_receipt = predict_scores(validation, fit)
            states, projection_receipt = project_daily_states(
                scores,
                level=level,
                minimum_daily_count=LEVEL_SPECS[level]["minimum_daily_count"],
            )
            metrics = fold_metrics(scores, validation_target, states)
            body = {
                "fold": fold["fold"],
                "train_start": fold["train_start"].isoformat(),
                "train_end": fold["train_end"].isoformat(),
                "validation_start": fold["validation_start"].isoformat(),
                "validation_end": fold["validation_end"].isoformat(),
                "alpha": alpha,
                "preprocess": train.preprocessor.payload(),
                "preprocess_sha256": canonical_sha256(train.preprocessor.payload()),
                "train_target": train_target.receipt,
                "validation_target": validation_target.receipt,
                "fit": _fit_receipt(fit),
                "scores": score_receipt,
                "state_projection": projection_receipt,
                "metrics": metrics,
                "metric_valid": metrics["metric_valid"],
                "holdout_accessed": False,
            }
            fold_receipts.append({**body, "receipt_sha256": canonical_sha256(body)})
        eligible = all(item["metric_valid"] is True for item in fold_receipts)
        rank_ic = [float(item["metrics"]["mean_rank_ic"]) for item in fold_receipts] if eligible else []
        spread = [float(item["metrics"]["mean_spread"]) for item in fold_receipts] if eligible else []
        body = {
            "alpha": alpha,
            "folds": fold_receipts,
            "fold_count": len(fold_receipts),
            "alpha_eligible": eligible,
            "median_rank_ic": float(np.median(rank_ic)) if rank_ic else None,
            "median_spread": float(np.median(spread)) if spread else None,
        }
        alpha_receipts.append({**body, "receipt_sha256": canonical_sha256(body)})
    selected = _select_alpha(alpha_receipts)
    try:
        _require_positive_development(level, selected)
    except RidgeCandidateError as exc:
        raise RidgeCandidateError(
            exc.reason_code,
            str(exc),
            stage=exc.stage,
            evidence={
                **exc.evidence,
                "alpha_receipts": alpha_receipts,
                "alpha_receipts_sha256": canonical_sha256(alpha_receipts),
            },
        ) from exc

    spec = LEVEL_SPECS[level]
    final = prepare_component(
        panel,
        component=f"{level}_ridge",
        level=level,
        feature_names=RELATIVE_FEATURES,
        calendar=calendar,
        start=DEVELOPMENT_START,
        end=DEVELOPMENT_END,
        expected_days=DEVELOPMENT_TRADING_DAYS,
        expected_sector_count=spec["expected_sector_count"],
        minimum_daily_count=spec["minimum_daily_count"],
        relative=True,
    )
    final_target = build_target_rows(
        panel,
        benchmark,
        calendar,
        level=level,
        start=DEVELOPMENT_START,
        end=DEVELOPMENT_END,
        expected_days=DEVELOPMENT_TRADING_DAYS,
        expected_sector_count=spec["expected_sector_count"],
        minimum_daily_count=spec["minimum_daily_count"],
    )
    final_fit = _fit_ridge(
        final,
        final_target,
        alpha=float(selected["alpha"]),
        attempt_log=attempt_log,
        context={"component": level, "fold": "final-development", "phase": "final"},
    )
    body = {
        "schema_version": COMPONENT_SCHEMA_VERSION,
        "component": level,
        "level": level,
        "feature_names": list(RELATIVE_FEATURES),
        "canonical_sector_count": len(final.canonical_codes),
        "canonical_sector_sha256": canonical_sha256(list(final.canonical_codes)),
        "alpha_receipts": alpha_receipts,
        "selected_alpha": selected["alpha"],
        "selected_median_rank_ic": selected["median_rank_ic"],
        "selected_median_spread": selected["median_spread"],
        "final_preprocess": final.preprocessor.payload(),
        "final_preprocess_sha256": canonical_sha256(final.preprocessor.payload()),
        "final_target": final_target.receipt,
        "final_fit": _fit_receipt(final_fit),
        "valid_row_count": final.valid_row_count,
        "valid_identity_sha256": final.valid_identity_sha256,
        "unavailable_items": list(final.unavailable_items),
        "unavailable_item_count": len(final.unavailable_items),
        "holdout_accessed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _run_conditioned_level_development(
    level: str,
    *,
    inputs: Mapping[str, Any],
    calendar: tuple[date, ...],
    benchmark: Mapping[date, float],
    market_folds: Sequence[MarketConditioningFold],
    attempt_log: list[dict[str, Any]],
) -> dict[str, Any]:
    if level not in LEVEL_SPECS:
        raise _fail(REASON_INPUT_IDENTITY, f"unknown level: {level}", stage="input")
    market_by_fold = {item.fold: item for item in market_folds}
    if set(market_by_fold) != {str(item["fold"]) for item in FOLDS}:
        raise _fail(REASON_MARKET_IDENTITY, "market fold identity is incomplete", stage="interaction")
    panel = _component_panel(inputs, level)
    prepared_folds: list[
        tuple[
            Mapping[str, Any],
            PreparedComponent,
            TargetRows,
            PreparedComponent,
            TargetRows,
            ConditionedComponent,
            ConditionedComponent,
            MarketConditioningFold,
        ]
    ] = []
    for fold in FOLDS:
        train, train_target, validation, validation_target = _prepare_fold(
            panel,
            benchmark,
            calendar,
            level=level,
            fold=fold,
        )
        market = market_by_fold[str(fold["fold"])]
        conditioned_train = _condition_component(
            train,
            market.train_states,
            fold=str(fold["fold"]),
            phase="train",
        )
        conditioned_validation = _condition_component(
            validation,
            market.validation_states,
            fold=str(fold["fold"]),
            phase="validation",
        )
        prepared_folds.append(
            (
                fold,
                train,
                train_target,
                validation,
                validation_target,
                conditioned_train,
                conditioned_validation,
                market,
            )
        )
    alpha_receipts: list[dict[str, Any]] = []
    for alpha in ALPHA_GRID:
        fold_receipts: list[dict[str, Any]] = []
        for (
            fold,
            train,
            train_target,
            _validation,
            validation_target,
            conditioned_train,
            conditioned_validation,
            market,
        ) in prepared_folds:
            fit = _fit_ridge(
                conditioned_train.component,
                train_target,
                alpha=alpha,
                attempt_log=attempt_log,
                context={"component": level, "fold": fold["fold"], "phase": "selection"},
            )
            scores, score_receipt = predict_scores(conditioned_validation.component, fit)
            states, projection_receipt = project_daily_states(
                scores,
                level=level,
                minimum_daily_count=LEVEL_SPECS[level]["minimum_daily_count"],
            )
            metrics = fold_metrics(scores, validation_target, states)
            body = {
                "fold": fold["fold"],
                "train_start": fold["train_start"].isoformat(),
                "train_end": fold["train_end"].isoformat(),
                "validation_start": fold["validation_start"].isoformat(),
                "validation_end": fold["validation_end"].isoformat(),
                "alpha": alpha,
                "preprocess": train.preprocessor.payload(),
                "preprocess_sha256": canonical_sha256(train.preprocessor.payload()),
                "train_target": train_target.receipt,
                "validation_target": validation_target.receipt,
                "train_interaction": conditioned_train.receipt,
                "validation_interaction": conditioned_validation.receipt,
                "market_fold_receipt_sha256": market.receipt["receipt_sha256"],
                "fit": _conditioned_fit_receipt(fit),
                "scores": score_receipt,
                "state_projection": projection_receipt,
                "metrics": metrics,
                "regime_split_diagnostics": _regime_split_metrics(
                    scores,
                    validation_target,
                    states,
                    market.validation_states,
                ),
                "metric_valid": metrics["metric_valid"],
                "regime_split_used_for_selection": False,
                "holdout_accessed": False,
            }
            fold_receipts.append({**body, "receipt_sha256": canonical_sha256(body)})
        eligible = all(item["metric_valid"] is True for item in fold_receipts)
        rank_ic = [float(item["metrics"]["mean_rank_ic"]) for item in fold_receipts] if eligible else []
        spread = [float(item["metrics"]["mean_spread"]) for item in fold_receipts] if eligible else []
        body = {
            "alpha": alpha,
            "folds": fold_receipts,
            "fold_count": len(fold_receipts),
            "alpha_eligible": eligible,
            "median_rank_ic": float(np.median(rank_ic)) if rank_ic else None,
            "median_spread": float(np.median(spread)) if spread else None,
            "regime_split_used_for_selection": False,
        }
        alpha_receipts.append({**body, "receipt_sha256": canonical_sha256(body)})
    try:
        selected = _select_alpha(alpha_receipts)
    except RidgeCandidateError as exc:
        raise RidgeCandidateError(
            exc.reason_code,
            str(exc),
            stage=exc.stage,
            evidence={
                **exc.evidence,
                "alpha_receipts": alpha_receipts,
                "alpha_receipts_sha256": canonical_sha256(alpha_receipts),
            },
        ) from exc
    except Exception as exc:
        raise RidgeCandidateError(
            REASON_UNEXPECTED,
            f"unexpected conditioned {level} alpha selection failure",
            stage="alpha_selection",
            evidence={
                "exception_type": type(exc).__name__,
                "error_message": str(exc),
                "alpha_receipts": alpha_receipts,
                "alpha_receipts_sha256": canonical_sha256(alpha_receipts),
            },
        ) from exc
    try:
        _require_positive_development(level, selected)
    except RidgeCandidateError as exc:
        raise RidgeCandidateError(
            exc.reason_code,
            str(exc),
            stage=exc.stage,
            evidence={
                **exc.evidence,
                "alpha_receipts": alpha_receipts,
                "alpha_receipts_sha256": canonical_sha256(alpha_receipts),
            },
        ) from exc
    body = {
        "schema_version": P2_3C_COMPONENT_SCHEMA_VERSION,
        "component": level,
        "level": level,
        "phase": "development",
        "base_feature_names": list(RELATIVE_FEATURES),
        "conditioned_feature_names": list(P2_3C_FEATURES),
        "alpha_receipts": alpha_receipts,
        "selected_alpha": selected["alpha"],
        "selected_median_rank_ic": selected["median_rank_ic"],
        "selected_median_spread": selected["median_spread"],
        "holdout_accessed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _run_market_conditioning_final(
    inputs: Mapping[str, Any],
    *,
    calendar: tuple[date, ...],
    attempt_log: list[dict[str, Any]],
) -> tuple[dict[date, str], dict[str, Any]]:
    panel = _component_panel(inputs, "L2")
    component = prepare_component(
        panel,
        component="market",
        level="L2",
        feature_names=MARKET_FEATURES,
        calendar=calendar,
        start=DEVELOPMENT_START,
        end=DEVELOPMENT_END,
        expected_days=DEVELOPMENT_TRADING_DAYS,
        expected_sector_count=LEVEL_SPECS["L2"]["expected_sector_count"],
        minimum_daily_count=LEVEL_SPECS["L2"]["minimum_daily_count"],
        relative=False,
    )
    fit = _record_fixed_market_fit(
        component,
        attempt_log=attempt_log,
        context={"component": "market", "fold": "final-development", "phase": "final"},
    )
    mapping = semantic_mapping("market", MARKET_FEATURES, fit.centers)
    states, state_receipt = _market_state_receipt(
        component,
        state_rows(
            component,
            causal_states(component, fit.centers, P2_3C_FIXED_JUMP_PENALTY),
            mapping,
        ),
    )
    missing = sorted({"risk_off", "risk_on"} - set(states.values()))
    if missing:
        raise _fail(
            REASON_MARKET_REGIME,
            "final development market fit does not contain both semantic states",
            stage="market_conditioning",
            evidence={"missing_states": missing, "state_receipt": state_receipt},
        )
    body = {
        "schema_version": P2_3C_MARKET_COMPONENT_SCHEMA_VERSION,
        "component": "market",
        "phase": "final-development",
        "fixed_jump_penalty": P2_3C_FIXED_JUMP_PENALTY,
        "fixed_seed": P2_3C_FIXED_SEED,
        "preprocess": component.preprocessor.payload(),
        "preprocess_sha256": canonical_sha256(component.preprocessor.payload()),
        "fit": jump_fit_receipt(fit, component),
        "centers": np.asarray(fit.centers, dtype="<f8").tolist(),
        "centers_sha256": sha256_bytes(np.asarray(fit.centers, dtype="<f8").tobytes()),
        "semantic_mapping": {str(key): value for key, value in sorted(mapping.items())},
        "semantic_mapping_sha256": canonical_sha256({str(key): value for key, value in sorted(mapping.items())}),
        "states": state_receipt,
        "holdout_accessed": False,
    }
    return states, {**body, "receipt_sha256": canonical_sha256(body)}


def _run_conditioned_level_final(
    level: str,
    *,
    inputs: Mapping[str, Any],
    calendar: tuple[date, ...],
    benchmark: Mapping[date, float],
    market_states: Mapping[date, str],
    selected_alpha: float,
    attempt_log: list[dict[str, Any]],
) -> dict[str, Any]:
    spec = LEVEL_SPECS[level]
    panel = _component_panel(inputs, level)
    component = prepare_component(
        panel,
        component=f"{level}_ridge",
        level=level,
        feature_names=RELATIVE_FEATURES,
        calendar=calendar,
        start=DEVELOPMENT_START,
        end=DEVELOPMENT_END,
        expected_days=DEVELOPMENT_TRADING_DAYS,
        expected_sector_count=spec["expected_sector_count"],
        minimum_daily_count=spec["minimum_daily_count"],
        relative=True,
    )
    targets = build_target_rows(
        panel,
        benchmark,
        calendar,
        level=level,
        start=DEVELOPMENT_START,
        end=DEVELOPMENT_END,
        expected_days=DEVELOPMENT_TRADING_DAYS,
        expected_sector_count=spec["expected_sector_count"],
        minimum_daily_count=spec["minimum_daily_count"],
    )
    conditioned = _condition_component(
        component,
        market_states,
        fold="final-development",
        phase="final",
    )
    fit = _fit_ridge(
        conditioned.component,
        targets,
        alpha=float(selected_alpha),
        attempt_log=attempt_log,
        context={"component": level, "fold": "final-development", "phase": "final"},
    )
    body = {
        "schema_version": P2_3C_COMPONENT_SCHEMA_VERSION,
        "component": level,
        "level": level,
        "phase": "final-development",
        "selected_alpha": float(selected_alpha),
        "canonical_sector_count": len(component.canonical_codes),
        "canonical_sector_sha256": canonical_sha256(list(component.canonical_codes)),
        "preprocess": component.preprocessor.payload(),
        "preprocess_sha256": canonical_sha256(component.preprocessor.payload()),
        "interaction": conditioned.receipt,
        "target": targets.receipt,
        "fit": _conditioned_fit_receipt(fit),
        "valid_row_count": component.valid_row_count,
        "valid_identity_sha256": component.valid_identity_sha256,
        "unavailable_items": list(component.unavailable_items),
        "unavailable_item_count": len(component.unavailable_items),
        "holdout_accessed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def planned_fit_count() -> int:
    return market_planned_fit_count() + 2 * (len(ALPHA_GRID) * len(FOLDS) + 1)


def p2_3c_planned_fit_count() -> int:
    return len(FOLDS) + 2 * len(ALPHA_GRID) * len(FOLDS) + 3


def _runtime_versions() -> dict[str, Any]:
    import scipy
    import sklearn
    from threadpoolctl import threadpool_info

    return {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "scipy": scipy.__version__,
        "scikit_learn": sklearn.__version__,
        "thread_environment": {
            name: os.environ.get(name)
            for name in (
                "OMP_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "MKL_NUM_THREADS",
                "VECLIB_MAXIMUM_THREADS",
                "NUMEXPR_NUM_THREADS",
            )
        },
        "threadpool_info": threadpool_info(),
    }


def _failure_runtime_versions() -> dict[str, Any]:
    """Preserve a typed runtime-version failure without losing the main failure receipt."""

    try:
        return _runtime_versions()
    except Exception as exc:
        return {
            "status": "unavailable",
            "reason_code": REASON_UNEXPECTED,
            "stage": "runtime_version_receipt",
            "exception_type": type(exc).__name__,
            "error_message": str(exc),
        }


def _request_identity(request: Mapping[str, Any], producer_commit: str) -> dict[str, Any]:
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
        raise _fail(REASON_INPUT_IDENTITY, "request schema is invalid", stage="input")
    if request.get("contract_version") != CONTRACT_VERSION:
        raise _fail(REASON_INPUT_IDENTITY, "request contract is invalid", stage="input")
    if len(producer_commit) != 40 or any(char not in "0123456789abcdef" for char in producer_commit):
        raise _fail(REASON_INPUT_IDENTITY, "producer commit is not a full lowercase Git SHA", stage="input")
    if str(request.get("expected_producer_commit") or "") != producer_commit:
        raise _fail(REASON_INPUT_IDENTITY, "producer commit differs from request", stage="input")
    source = request.get("source")
    if not isinstance(source, Mapping):
        raise _fail(REASON_INPUT_IDENTITY, "request source is missing", stage="input")
    forbidden_hash = _require_sha256(
        request.get("forbidden_holdout_date_set_sha256"), "forbidden_holdout_date_set_sha256"
    )
    if (
        request.get("holdout_start") != HOLDOUT_START.isoformat()
        or request.get("holdout_end") != HOLDOUT_END.isoformat()
    ):
        raise _fail(REASON_HOLDOUT, "forbidden holdout boundary is invalid", stage="input")
    return {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "expected_producer_commit": producer_commit,
        "holdout_start": HOLDOUT_START.isoformat(),
        "holdout_end": HOLDOUT_END.isoformat(),
        "holdout_trading_day_count": HOLDOUT_TRADING_DAYS,
        "forbidden_holdout_date_set_sha256": forbidden_hash,
        "source_sha256": canonical_sha256(source),
    }


def validate_p2_3c_static_request(request: Mapping[str, Any]) -> dict[str, Any]:
    """Validate every P2-3C request identity that does not depend on source Git."""

    if request.get("schema_version") != P2_3C_REQUEST_SCHEMA_VERSION:
        raise _fail(REASON_INPUT_IDENTITY, "P2-3C request schema is invalid", stage="input")
    if request.get("contract_version") != P2_3C_CONTRACT_VERSION:
        raise _fail(REASON_INPUT_IDENTITY, "P2-3C request contract is invalid", stage="input")
    if request.get("candidate_attempt_index") != P2_3C_ATTEMPT_INDEX:
        raise _fail(REASON_INPUT_IDENTITY, "P2-3C candidate attempt index is invalid", stage="input")
    prior = request.get("prior_not_available_report_sha256s")
    expected_prior = {
        "P2-3A": P2_3A_NOT_AVAILABLE_REPORT_SHA256,
        "P2-3B": P2_3B_NOT_AVAILABLE_REPORT_SHA256,
    }
    if prior != expected_prior:
        raise _fail(REASON_INPUT_IDENTITY, "prior NOT_AVAILABLE receipt identities are invalid", stage="input")
    fixed_market = request.get("fixed_market_parameters")
    if fixed_market != {"jump_penalty": P2_3C_FIXED_JUMP_PENALTY, "seed": P2_3C_FIXED_SEED}:
        raise _fail(REASON_INPUT_IDENTITY, "fixed market parameter identity is invalid", stage="input")
    source = request.get("source")
    if not isinstance(source, Mapping):
        raise _fail(REASON_INPUT_IDENTITY, "request source is missing", stage="input")
    forbidden_hash = _require_sha256(
        request.get("forbidden_holdout_date_set_sha256"), "forbidden_holdout_date_set_sha256"
    )
    if (
        request.get("holdout_start") != HOLDOUT_START.isoformat()
        or request.get("holdout_end") != HOLDOUT_END.isoformat()
    ):
        raise _fail(REASON_HOLDOUT, "forbidden holdout boundary is invalid", stage="input")
    return {
        "schema_version": P2_3C_REQUEST_SCHEMA_VERSION,
        "contract_version": P2_3C_CONTRACT_VERSION,
        "candidate_attempt_index": P2_3C_ATTEMPT_INDEX,
        "prior_not_available_report_sha256s": expected_prior,
        "fixed_market_parameters": fixed_market,
        "holdout_start": HOLDOUT_START.isoformat(),
        "holdout_end": HOLDOUT_END.isoformat(),
        "holdout_trading_day_count": HOLDOUT_TRADING_DAYS,
        "forbidden_holdout_date_set_sha256": forbidden_hash,
        "source_sha256": canonical_sha256(source),
    }


def _p2_3c_request_identity(request: Mapping[str, Any], producer_commit: str) -> dict[str, Any]:
    static_identity = validate_p2_3c_static_request(request)
    if len(producer_commit) != 40 or any(char not in "0123456789abcdef" for char in producer_commit):
        raise _fail(REASON_INPUT_IDENTITY, "producer commit is not a full lowercase Git SHA", stage="input")
    if str(request.get("expected_producer_commit") or "") != producer_commit:
        raise _fail(REASON_INPUT_IDENTITY, "producer commit differs from request", stage="input")
    return {**static_identity, "expected_producer_commit": producer_commit}


def run_p2_3b_candidate(
    inputs: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    producer_commit: str,
) -> dict[str, Any]:
    """Execute the approved 184-fit development candidate without holdout access."""

    request_identity = _request_identity(request, producer_commit)
    raw_calendar = inputs.get("trading_dates")
    if not isinstance(raw_calendar, (tuple, list)):
        raise _fail(REASON_INPUT_IDENTITY, "trading calendar is missing", stage="input")
    calendar = tuple(_as_date(value) for value in raw_calendar)
    if calendar != tuple(sorted(set(calendar))):
        raise _fail(REASON_INPUT_IDENTITY, "trading calendar is not sorted and unique", stage="input")
    development_dates = _calendar_slice(
        calendar,
        start=DEVELOPMENT_START,
        end=DEVELOPMENT_END,
        expected_days=DEVELOPMENT_TRADING_DAYS,
    )
    if any(day >= HOLDOUT_START for day in calendar):
        raise _fail(
            REASON_HOLDOUT,
            "P2-3B inputs contain forbidden holdout dates",
            stage="input",
            evidence={"max_input_date": calendar[-1].isoformat()},
        )
    dataset_manifest = inputs.get("dataset_manifest")
    mapping_manifest = inputs.get("mapping_manifest")
    if not isinstance(dataset_manifest, Mapping) or not isinstance(mapping_manifest, (Mapping, list)):
        raise _fail(REASON_INPUT_IDENTITY, "dataset or mapping manifest is missing", stage="input")
    benchmark = _benchmark_rows(dataset_manifest)
    attempt_log: list[dict[str, Any]] = []
    components: list[dict[str, Any]] = []
    try:
        market_attempt_start = len(attempt_log)
        components.append(
            run_market_component(
                inputs,
                calendar=calendar,
                benchmark=benchmark,
                attempt_log=attempt_log,
            )
        )
        market_attempt_count = len(attempt_log) - market_attempt_start
        if market_attempt_count != market_planned_fit_count():
            raise _fail(
                REASON_INPUT_IDENTITY,
                "market component fit count differs from the approved plan",
                stage="market_finalization",
                evidence={"planned": market_planned_fit_count(), "actual": market_attempt_count},
            )
        for level in ("L1", "L2"):
            components.append(
                _run_level(
                    level,
                    inputs=inputs,
                    calendar=calendar,
                    benchmark=benchmark,
                    attempt_log=attempt_log,
                )
            )
    except RidgeCandidateError as exc:
        evidence = {
            **exc.evidence,
            "completed_fit_count": len(attempt_log),
            "fit_attempts_sha256": canonical_sha256(attempt_log),
            "fit_attempts": attempt_log,
            "completed_component_count": len(components),
            "completed_component_receipt_sha256s": [str(item["receipt_sha256"]) for item in components],
            "completed_components": components,
        }
        raise RidgeCandidateError(exc.reason_code, str(exc), stage=exc.stage, evidence=evidence) from exc
    except JumpSpikeError as exc:
        evidence = {
            **exc.evidence,
            "completed_fit_count": len(attempt_log),
            "fit_attempts_sha256": canonical_sha256(attempt_log),
            "fit_attempts": attempt_log,
            "completed_component_count": len(components),
            "completed_component_receipt_sha256s": [str(item["receipt_sha256"]) for item in components],
            "completed_components": components,
        }
        raise RidgeCandidateError(exc.reason_code, str(exc), stage=exc.stage, evidence=evidence) from exc
    except Exception as exc:
        evidence = {
            "exception_type": type(exc).__name__,
            "error_message": str(exc),
            "completed_fit_count": len(attempt_log),
            "fit_attempts_sha256": canonical_sha256(attempt_log),
            "fit_attempts": attempt_log,
            "completed_component_count": len(components),
            "completed_component_receipt_sha256s": [str(item["receipt_sha256"]) for item in components],
            "completed_components": components,
        }
        raise RidgeCandidateError(
            REASON_UNEXPECTED,
            "unexpected P2-3B candidate failure",
            stage="unknown",
            evidence=evidence,
        ) from exc
    if len(attempt_log) != planned_fit_count():
        raise _fail(
            REASON_INPUT_IDENTITY,
            "completed fit attempt count differs from the approved plan",
            stage="finalization",
            evidence={
                "planned": planned_fit_count(),
                "actual": len(attempt_log),
                "fit_attempts_sha256": canonical_sha256(attempt_log),
                "fit_attempts": attempt_log,
                "completed_component_count": len(components),
                "completed_component_receipt_sha256s": [str(item["receipt_sha256"]) for item in components],
                "completed_components": components,
            },
        )
    try:
        dataset_hash = canonical_sha256(dataset_manifest)
        mapping_hash = canonical_sha256(mapping_manifest)
        body = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "algorithm_version": ALGORITHM_VERSION,
            "model_origin": MODEL_ORIGIN,
            "status": "P2_3B_CANDIDATE_FROZEN_PENDING_P2_4_HOLDOUT_ACCEPTANCE",
            "producer_commit": producer_commit,
            "runtime_versions": _runtime_versions(),
            "request_identity": request_identity,
            "request_identity_sha256": canonical_sha256(request_identity),
            "dataset_manifest_sha256": dataset_hash,
            "mapping_manifest_sha256": mapping_hash,
            "database_identity": inputs.get("database"),
            "calendar_manifest_sha256": canonical_sha256(dataset_manifest.get("calendar_benchmark")),
            "feature_formula_sha256": canonical_sha256(
                {"L1": inputs.get("feature_definition"), "L2": inputs.get("l2_feature_definition")}
            ),
            "development_start": DEVELOPMENT_START.isoformat(),
            "development_end": DEVELOPMENT_END.isoformat(),
            "development_trading_day_count": len(development_dates),
            "development_date_set_sha256": canonical_sha256([day.isoformat() for day in development_dates]),
            "forbidden_holdout_start": HOLDOUT_START.isoformat(),
            "forbidden_holdout_end": HOLDOUT_END.isoformat(),
            "forbidden_holdout_date_set_sha256": request_identity["forbidden_holdout_date_set_sha256"],
            "planned_fit_count": planned_fit_count(),
            "completed_fit_count": len(attempt_log),
            "fit_attempts_sha256": canonical_sha256(attempt_log),
            "components": components,
            "component_receipt_sha256s": [str(item["receipt_sha256"]) for item in components],
            "component_count": len(components),
            "candidate_status": "development_candidate_frozen",
            "failure_stage": None,
            "failure_reason_code": None,
            "holdout_accessed": False,
            "selection_performed": True,
            "partial_component_selection_performed": False,
            "selection_scope": "development_only",
            "product_acceptance_performed": False,
            "candidate_receipt_write": False,
            "failure_receipt_write": False,
            "model_write": False,
            "ready_write": False,
            "database_write": False,
            "runtime_action": False,
        }
        return {**body, "report_sha256": canonical_sha256(body)}
    except Exception as exc:
        raise RidgeCandidateError(
            REASON_UNEXPECTED,
            "P2-3B candidate finalization failed",
            stage="finalization",
            evidence={
                "exception_type": type(exc).__name__,
                "error_message": str(exc),
                "completed_fit_count": len(attempt_log),
                "fit_attempts_sha256": canonical_sha256(attempt_log),
                "fit_attempts": attempt_log,
                "completed_component_count": len(components),
                "completed_component_receipt_sha256s": [str(item["receipt_sha256"]) for item in components],
                "completed_components": components,
            },
        ) from exc


def run_p2_3c_candidate(
    inputs: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    producer_commit: str,
) -> dict[str, Any]:
    """Execute the approved P2-3C 36-fit development contract without holdout access."""

    request_identity = _p2_3c_request_identity(request, producer_commit)
    raw_calendar = inputs.get("trading_dates")
    if not isinstance(raw_calendar, (tuple, list)):
        raise _fail(REASON_INPUT_IDENTITY, "trading calendar is missing", stage="input")
    calendar = tuple(_as_date(value) for value in raw_calendar)
    if calendar != tuple(sorted(set(calendar))):
        raise _fail(REASON_INPUT_IDENTITY, "trading calendar is not sorted and unique", stage="input")
    development_dates = _calendar_slice(
        calendar,
        start=DEVELOPMENT_START,
        end=DEVELOPMENT_END,
        expected_days=DEVELOPMENT_TRADING_DAYS,
    )
    if any(day >= HOLDOUT_START for day in calendar):
        raise _fail(
            REASON_HOLDOUT,
            "P2-3C inputs contain forbidden holdout dates",
            stage="input",
            evidence={"max_input_date": calendar[-1].isoformat()},
        )
    dataset_manifest = inputs.get("dataset_manifest")
    mapping_manifest = inputs.get("mapping_manifest")
    if not isinstance(dataset_manifest, Mapping) or not isinstance(mapping_manifest, (Mapping, list)):
        raise _fail(REASON_INPUT_IDENTITY, "dataset or mapping manifest is missing", stage="input")
    benchmark = _benchmark_rows(dataset_manifest)
    attempt_log: list[dict[str, Any]] = []
    completed_stages: list[dict[str, Any]] = []

    def require_attempt_count(expected: int, stage: str) -> None:
        if len(attempt_log) != expected:
            raise _fail(
                REASON_INPUT_IDENTITY,
                "completed fit attempt count differs from the approved P2-3C stage boundary",
                stage=stage,
                evidence={"expected": expected, "actual": len(attempt_log)},
            )

    try:
        market_folds, market_development = _run_market_conditioning_development(
            inputs,
            calendar=calendar,
            benchmark=benchmark,
            attempt_log=attempt_log,
        )
        completed_stages.append(market_development)
        require_attempt_count(3, "market_development_finalization")

        l1_development = _run_conditioned_level_development(
            "L1",
            inputs=inputs,
            calendar=calendar,
            benchmark=benchmark,
            market_folds=market_folds,
            attempt_log=attempt_log,
        )
        completed_stages.append(l1_development)
        require_attempt_count(18, "L1_development_finalization")

        l2_development = _run_conditioned_level_development(
            "L2",
            inputs=inputs,
            calendar=calendar,
            benchmark=benchmark,
            market_folds=market_folds,
            attempt_log=attempt_log,
        )
        completed_stages.append(l2_development)
        require_attempt_count(33, "L2_development_finalization")

        final_market_states, market_final = _run_market_conditioning_final(
            inputs,
            calendar=calendar,
            attempt_log=attempt_log,
        )
        completed_stages.append(market_final)
        l1_final = _run_conditioned_level_final(
            "L1",
            inputs=inputs,
            calendar=calendar,
            benchmark=benchmark,
            market_states=final_market_states,
            selected_alpha=float(l1_development["selected_alpha"]),
            attempt_log=attempt_log,
        )
        completed_stages.append(l1_final)
        l2_final = _run_conditioned_level_final(
            "L2",
            inputs=inputs,
            calendar=calendar,
            benchmark=benchmark,
            market_states=final_market_states,
            selected_alpha=float(l2_development["selected_alpha"]),
            attempt_log=attempt_log,
        )
        completed_stages.append(l2_final)
        require_attempt_count(p2_3c_planned_fit_count(), "finalization")
    except (RidgeCandidateError, JumpSpikeError) as exc:
        evidence = {
            **exc.evidence,
            "completed_fit_count": len(attempt_log),
            "fit_attempts_sha256": canonical_sha256(attempt_log),
            "fit_attempts": attempt_log,
            "completed_component_count": len(completed_stages),
            "completed_component_receipt_sha256s": [str(item["receipt_sha256"]) for item in completed_stages],
            "completed_components": completed_stages,
        }
        raise RidgeCandidateError(exc.reason_code, str(exc), stage=exc.stage, evidence=evidence) from exc
    except Exception as exc:
        evidence = {
            "exception_type": type(exc).__name__,
            "error_message": str(exc),
            "completed_fit_count": len(attempt_log),
            "fit_attempts_sha256": canonical_sha256(attempt_log),
            "fit_attempts": attempt_log,
            "completed_component_count": len(completed_stages),
            "completed_component_receipt_sha256s": [str(item["receipt_sha256"]) for item in completed_stages],
            "completed_components": completed_stages,
        }
        raise RidgeCandidateError(
            REASON_UNEXPECTED,
            "unexpected P2-3C candidate failure",
            stage="unknown",
            evidence=evidence,
        ) from exc
    try:
        body = {
            "schema_version": P2_3C_REPORT_SCHEMA_VERSION,
            "contract_version": P2_3C_CONTRACT_VERSION,
            "algorithm_version": P2_3C_ALGORITHM_VERSION,
            "model_origin": P2_3C_MODEL_ORIGIN,
            "status": "P2_3C_CANDIDATE_FROZEN_PENDING_P2_4_HOLDOUT_ACCEPTANCE",
            "producer_commit": producer_commit,
            "runtime_versions": _runtime_versions(),
            "request_identity": request_identity,
            "request_identity_sha256": canonical_sha256(request_identity),
            "candidate_attempt_index": P2_3C_ATTEMPT_INDEX,
            "prior_not_available_report_sha256s": request_identity["prior_not_available_report_sha256s"],
            "fixed_market_parameters": request_identity["fixed_market_parameters"],
            "dataset_manifest_sha256": canonical_sha256(dataset_manifest),
            "mapping_manifest_sha256": canonical_sha256(mapping_manifest),
            "database_identity": inputs.get("database"),
            "calendar_manifest_sha256": canonical_sha256(dataset_manifest.get("calendar_benchmark")),
            "feature_formula_sha256": canonical_sha256(
                {"L1": inputs.get("feature_definition"), "L2": inputs.get("l2_feature_definition")}
            ),
            "development_start": DEVELOPMENT_START.isoformat(),
            "development_end": DEVELOPMENT_END.isoformat(),
            "development_trading_day_count": len(development_dates),
            "development_date_set_sha256": canonical_sha256([day.isoformat() for day in development_dates]),
            "forbidden_holdout_start": HOLDOUT_START.isoformat(),
            "forbidden_holdout_end": HOLDOUT_END.isoformat(),
            "forbidden_holdout_date_set_sha256": request_identity["forbidden_holdout_date_set_sha256"],
            "planned_fit_count": p2_3c_planned_fit_count(),
            "completed_fit_count": len(attempt_log),
            "fit_attempts_sha256": canonical_sha256(attempt_log),
            "components": completed_stages,
            "component_receipt_sha256s": [str(item["receipt_sha256"]) for item in completed_stages],
            "component_count": len(completed_stages),
            "candidate_status": "development_candidate_frozen",
            "failure_stage": None,
            "failure_reason_code": None,
            "holdout_accessed": False,
            "selection_performed": True,
            "partial_component_selection_performed": False,
            "selection_scope": "development_only",
            "regime_split_used_for_selection": False,
            "product_acceptance_performed": False,
            "candidate_receipt_write": False,
            "failure_receipt_write": False,
            "model_write": False,
            "ready_write": False,
            "database_write": False,
            "runtime_action": False,
        }
        return {**body, "report_sha256": canonical_sha256(body)}
    except Exception as exc:
        raise RidgeCandidateError(
            REASON_UNEXPECTED,
            "P2-3C candidate finalization failed",
            stage="finalization",
            evidence={
                "exception_type": type(exc).__name__,
                "error_message": str(exc),
                "completed_fit_count": len(attempt_log),
                "fit_attempts_sha256": canonical_sha256(attempt_log),
                "fit_attempts": attempt_log,
                "completed_component_count": len(completed_stages),
                "completed_component_receipt_sha256s": [str(item["receipt_sha256"]) for item in completed_stages],
                "completed_components": completed_stages,
            },
        ) from exc


def failure_report(
    request: Mapping[str, Any],
    *,
    producer_commit: str,
    error: BaseException,
    completed_fit_count: int = 0,
    candidate_mode: str = "p2-3b",
) -> dict[str, Any]:
    if isinstance(error, (RidgeCandidateError, JumpSpikeError)):
        reason_code = error.reason_code
        stage = error.stage
        evidence = error.evidence
    else:
        reason_code = REASON_UNEXPECTED
        stage = "unknown"
        evidence = {"exception_type": type(error).__name__, "error_message": str(error)}
    if candidate_mode == "p2-3b":
        identity = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "algorithm_version": ALGORITHM_VERSION,
            "model_origin": MODEL_ORIGIN,
            "planned_fit_count": planned_fit_count(),
        }
    elif candidate_mode == "p2-3c":
        identity = {
            "schema_version": P2_3C_REPORT_SCHEMA_VERSION,
            "contract_version": P2_3C_CONTRACT_VERSION,
            "algorithm_version": P2_3C_ALGORITHM_VERSION,
            "model_origin": P2_3C_MODEL_ORIGIN,
            "planned_fit_count": p2_3c_planned_fit_count(),
        }
    else:
        raise _fail(REASON_INPUT_IDENTITY, "unknown candidate mode", stage="failure_receipt")
    partial_component_selection_performed = bool(
        isinstance(evidence, Mapping)
        and (
            int(evidence.get("completed_component_count") or 0) > (1 if candidate_mode == "p2-3c" else 0)
            or "selected_alpha" in evidence
        )
    )
    body = {
        **identity,
        "status": "NOT_AVAILABLE_FOR_PROMOTION",
        "producer_commit": producer_commit,
        "runtime_versions": _failure_runtime_versions(),
        "request_sha256": canonical_sha256(request),
        "completed_fit_count": completed_fit_count,
        "failure_stage": stage,
        "failure_reason_code": reason_code,
        "failure_evidence": evidence,
        "holdout_accessed": False,
        "selection_performed": False,
        "partial_component_selection_performed": partial_component_selection_performed,
        "selection_scope": "development_only",
        "product_acceptance_performed": False,
        "candidate_receipt_write": False,
        "failure_receipt_write": False,
        "model_write": False,
        "ready_write": False,
        "database_write": False,
        "runtime_action": False,
    }
    return {**body, "report_sha256": canonical_sha256(body)}


def report_for_write(report: Mapping[str, Any], *, failure: bool = False) -> dict[str, Any]:
    body = {key: value for key, value in report.items() if key != "report_sha256"}
    body["failure_receipt_write"] = failure
    body["candidate_receipt_write"] = not failure
    return {**body, "report_sha256": canonical_sha256(body)}


def _require_external_output(path: Path, *, repository_root: Path) -> Path:
    if not path.is_absolute():
        raise _fail(REASON_INPUT_IDENTITY, "output must be absolute", stage="output")
    resolved = path.resolve()
    root = repository_root.resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        pass
    else:
        raise _fail(REASON_INPUT_IDENTITY, "output must be repository-external", stage="output")
    if resolved.name in {"", ".", "..", "latest", "latest.json"}:
        raise _fail(REASON_INPUT_IDENTITY, "output identity is invalid", stage="output")
    return resolved


def preflight_output_path(path: Path, *, repository_root: Path) -> Path:
    target = _require_external_output(path, repository_root=repository_root)
    failure = target.with_name(f"{target.stem}.failure.json")
    existing = [item.name for item in (target, failure) if item.exists()]
    if existing:
        raise _fail(
            REASON_COLLISION,
            "output or failure receipt already exists",
            stage="output_preflight",
            evidence={"existing_names": existing},
        )
    return target


def _write_once(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() == payload:
            return
        raise _fail(REASON_COLLISION, f"immutable output collision: {path.name}", stage="output")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise _fail(REASON_COLLISION, f"immutable output collision: {path.name}", stage="output")
        os.unlink(temporary)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def write_report(path: Path, report: Mapping[str, Any], *, repository_root: Path) -> Path:
    target = _require_external_output(path, repository_root=repository_root)
    payload = canonical_json_bytes(dict(report)) + b"\n"
    _write_once(target, payload)
    try:
        parsed = json.loads(target.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _fail(REASON_READBACK, "report JSON readback failed", stage="output") from exc
    if not isinstance(parsed, dict) or canonical_json_bytes(parsed) != canonical_json_bytes(dict(report)):
        raise _fail(REASON_READBACK, "report canonical readback mismatch", stage="output")
    return target
