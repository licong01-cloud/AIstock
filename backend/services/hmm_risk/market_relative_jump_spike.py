"""P2-3 market-regime plus sector-relative jump-model spike.

This module is deliberately offline-only.  It consumes the existing C-010
observation authority, performs the approved development-only selection, and
produces a compact candidate receipt.  It never reads the untouched holdout or
writes a production model set, READY state, database row, or runtime state.
"""

from __future__ import annotations

import math
import json
import os
import platform
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

from backend.services.hmm_risk.state_model_set import (
    canonical_json_bytes,
    canonical_sha256,
    sha256_bytes,
)

CONTRACT_VERSION = "C-011-P2-3-A"
ALGORITHM_VERSION = "hmm_risk_market_relative_jump_v1"
REPORT_SCHEMA_VERSION = "hmm_risk_market_relative_jump_spike_report_v1"
REQUEST_SCHEMA_VERSION = "hmm_risk_market_relative_jump_spike_request_v1"

DEVELOPMENT_START = date(2022, 1, 4)
DEVELOPMENT_END = date(2025, 3, 31)
DEVELOPMENT_TRADING_DAYS = 783
HOLDOUT_START = date(2025, 4, 1)
HOLDOUT_END = date(2026, 3, 31)
HOLDOUT_TRADING_DAYS = 242

RESTART_SEEDS = tuple(range(42, 50))
LAMBDA_GRID = (0.25, 0.5, 1.0, 2.0, 4.0, 8.0)
MAX_JUMP_ITERATIONS = 200
OBJECTIVE_ENVELOPE_SCALE = 1e-10
RESTART_TIE_ATOL = 1e-12
RESTART_TIE_RTOL = 1e-12
LAMBDA_METRIC_TOLERANCE = 1e-4
SEMANTIC_TIE_TOLERANCE = 1e-8
PREPROCESS_STD_FLOOR = 1e-12

MARKET_FEATURES = (
    "daily_return",
    "volatility_Nd",
    "net_mf_ratio",
    "sf_breadth_5d",
    "sf_dispersion_5d_neg",
)
RELATIVE_FEATURES = (
    "excess_return_Nd",
    "net_mf_ratio",
    "elg_net_mf_ratio",
    "sf_excess_breadth_5d",
    "sf_turnover_pctile_120d_neg",
)

FOLDS = (
    {
        "fold": "fold-1",
        "train_start": DEVELOPMENT_START,
        "train_end": date(2023, 9, 1),
        "validation_start": date(2023, 9, 4),
        "validation_end": date(2024, 3, 14),
        "train_days": 405,
        "validation_days": 126,
    },
    {
        "fold": "fold-2",
        "train_start": DEVELOPMENT_START,
        "train_end": date(2024, 3, 14),
        "validation_start": date(2024, 3, 15),
        "validation_end": date(2024, 9, 18),
        "train_days": 531,
        "validation_days": 126,
    },
    {
        "fold": "fold-3",
        "train_start": DEVELOPMENT_START,
        "train_end": date(2024, 9, 18),
        "validation_start": date(2024, 9, 19),
        "validation_end": DEVELOPMENT_END,
        "train_days": 657,
        "validation_days": 126,
    },
)

REASON_INPUT_IDENTITY = "hmm_risk_jump_input_identity_mismatch"
REASON_FOLD_BOUNDARY = "hmm_risk_jump_fold_boundary_invalid"
REASON_PREPROCESS = "hmm_risk_jump_preprocess_invalid"
REASON_OBJECTIVE_NON_FINITE = "hmm_risk_jump_objective_non_finite"
REASON_OBJECTIVE_INCREASED = "hmm_risk_jump_objective_increased"
REASON_STATE_EMPTY = "hmm_risk_jump_state_empty"
REASON_MAX_ITERATIONS = "hmm_risk_jump_max_iterations_reached"
REASON_SEMANTIC_TIE = "hmm_risk_jump_semantic_tie"
REASON_SELECTION_METRIC = "hmm_risk_jump_selection_metric_unavailable"
REASON_SELECTION = "hmm_risk_jump_selection_unavailable"
REASON_HOLDOUT = "hmm_risk_jump_holdout_access_forbidden"
REASON_COLLISION = "hmm_risk_jump_candidate_collision"
REASON_READBACK = "hmm_risk_jump_candidate_readback_mismatch"
REASON_COVERAGE = "hmm_risk_jump_coverage_contract_failed"
REASON_REPRESENTATIVENESS = "hmm_risk_jump_representativeness_failed"
REASON_UNEXPECTED = "hmm_risk_jump_unexpected_error"


class JumpSpikeError(RuntimeError):
    """Typed fail-closed error for the P2-3 spike."""

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
class Preprocessor:
    feature_names: tuple[str, ...]
    lower: tuple[float, ...]
    upper: tuple[float, ...]
    mean: tuple[float, ...]
    std: tuple[float, ...]
    valid_row_count: int
    valid_identity_sha256: str

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": "hmm_risk_jump_level_global_preprocess_v1",
            "feature_names": list(self.feature_names),
            "quantile_method": "linear",
            "lower_quantile": 0.01,
            "upper_quantile": 0.99,
            "mean_algorithm": "math.fsum_over_n",
            "variance_algorithm": "math.fsum_squared_deviation_over_n",
            "ddof": 0,
            "dtype": "float64_le",
            "lower": list(self.lower),
            "upper": list(self.upper),
            "mean": list(self.mean),
            "std": list(self.std),
            "valid_row_count": self.valid_row_count,
            "valid_identity_sha256": self.valid_identity_sha256,
        }


@dataclass(frozen=True)
class SequenceData:
    key: str
    dates: tuple[date, ...]
    ordinals: tuple[int, ...]
    values: np.ndarray


@dataclass(frozen=True)
class PreparedComponent:
    component: str
    level: str
    feature_names: tuple[str, ...]
    expected_sector_count: int
    minimum_daily_count: int
    canonical_codes: tuple[str, ...]
    sequences: tuple[SequenceData, ...]
    preprocessor: Preprocessor
    unavailable_items: tuple[dict[str, Any], ...]
    valid_row_count: int
    valid_identity_sha256: str


@dataclass(frozen=True)
class JumpFit:
    centers: np.ndarray
    paths: tuple[np.ndarray, ...]
    objective: float
    normalized_objective: float
    iterations: int
    seed: int
    jump_penalty: float
    row_count: int
    feature_count: int


def _fail(
    reason_code: str,
    message: str,
    *,
    stage: str,
    evidence: Mapping[str, Any] | None = None,
) -> JumpSpikeError:
    return JumpSpikeError(reason_code, message, stage=stage, evidence=evidence)


def _iso(value: date | pd.Timestamp | str) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


def _as_date(value: date | pd.Timestamp | str) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _require_sha256(value: Any, field: str) -> str:
    text = str(value or "").lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise _fail(REASON_INPUT_IDENTITY, f"{field} must be SHA-256", stage="input")
    return text


def _array_hash(array: np.ndarray) -> str:
    normalized = np.asarray(array, dtype="<f8", order="C")
    header = canonical_json_bytes({"dtype": "float64_le", "shape": list(normalized.shape)})
    return sha256_bytes(header + normalized.tobytes(order="C"))


def _frame(panel: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(panel, pd.DataFrame) or not isinstance(panel.index, pd.MultiIndex):
        raise _fail(REASON_INPUT_IDENTITY, "panel must use a two-level MultiIndex", stage="input")
    if len(panel.index.names) != 2 or panel.index.names[0] != "trade_date":
        raise _fail(REASON_INPUT_IDENTITY, "panel index must begin with trade_date", stage="input")
    result = panel.reset_index().rename(columns={panel.index.names[1]: "sector_code"})
    result["trade_date"] = result["trade_date"].map(_as_date)
    result["sector_code"] = result["sector_code"].map(str)
    if result.duplicated(["trade_date", "sector_code"]).any():
        raise _fail(REASON_INPUT_IDENTITY, "panel contains duplicate sector/date rows", stage="input")
    return result.sort_values(["trade_date", "sector_code"], kind="mergesort").reset_index(drop=True)


def _calendar_slice(calendar: Sequence[date], start: date, end: date, expected: int) -> tuple[date, ...]:
    values = tuple(item for item in calendar if start <= item <= end)
    if (
        len(values) != expected
        or not values
        or values[0] != start
        or values[-1] != end
        or values != tuple(sorted(set(values)))
    ):
        raise _fail(
            REASON_FOLD_BOUNDARY,
            "calendar slice does not match the approved fold",
            stage="fold_boundary",
            evidence={"start": start.isoformat(), "end": end.isoformat(), "actual_count": len(values)},
        )
    return values


def _fit_preprocessor(frame: pd.DataFrame, feature_names: Sequence[str]) -> Preprocessor:
    features = tuple(feature_names)
    missing = [field for field in features if field not in frame.columns]
    if missing:
        raise _fail(REASON_PREPROCESS, f"feature columns are missing: {missing}", stage="preprocess")
    values = frame.loc[:, list(features)].to_numpy(dtype=np.float64)
    valid_mask = np.isfinite(values).all(axis=1)
    valid = values[valid_mask]
    if valid.shape[0] < 2:
        raise _fail(REASON_PREPROCESS, "preprocess has fewer than two valid rows", stage="preprocess")
    identities = [
        [frame.iloc[index]["trade_date"].isoformat(), str(frame.iloc[index]["sector_code"])]
        for index in np.flatnonzero(valid_mask)
    ]
    lower = np.quantile(valid, 0.01, axis=0, method="linear")
    upper = np.quantile(valid, 0.99, axis=0, method="linear")
    clipped = np.clip(valid, lower, upper)
    means = np.asarray([math.fsum(column.tolist()) / len(column) for column in clipped.T], dtype=np.float64)
    variances = np.asarray(
        [
            math.fsum((float(value) - float(mean)) ** 2 for value in column) / len(column)
            for column, mean in zip(clipped.T, means, strict=True)
        ],
        dtype=np.float64,
    )
    std = np.sqrt(variances)
    if (
        not np.isfinite(lower).all()
        or not np.isfinite(upper).all()
        or not np.isfinite(means).all()
        or not np.isfinite(std).all()
        or np.any(std <= PREPROCESS_STD_FLOOR)
    ):
        raise _fail(
            REASON_PREPROCESS,
            "level-global preprocess is non-finite or has non-positive scale",
            stage="preprocess",
            evidence={"std": std.tolist()},
        )
    return Preprocessor(
        feature_names=features,
        lower=tuple(float(value) for value in lower),
        upper=tuple(float(value) for value in upper),
        mean=tuple(float(value) for value in means),
        std=tuple(float(value) for value in std),
        valid_row_count=int(valid.shape[0]),
        valid_identity_sha256=canonical_sha256(identities),
    )


def _transform(frame: pd.DataFrame, preprocessor: Preprocessor) -> tuple[np.ndarray, np.ndarray]:
    values = frame.loc[:, list(preprocessor.feature_names)].to_numpy(dtype=np.float64)
    valid_mask = np.isfinite(values).all(axis=1)
    transformed = np.full(values.shape, np.nan, dtype=np.float64)
    if valid_mask.any():
        clipped = np.clip(
            values[valid_mask],
            np.asarray(preprocessor.lower, dtype=np.float64),
            np.asarray(preprocessor.upper, dtype=np.float64),
        )
        transformed[valid_mask] = (clipped - np.asarray(preprocessor.mean, dtype=np.float64)) / np.asarray(
            preprocessor.std, dtype=np.float64
        )
    return transformed, valid_mask


def prepare_component(
    panel: pd.DataFrame,
    *,
    component: str,
    level: str,
    feature_names: Sequence[str],
    calendar: Sequence[date],
    start: date,
    end: date,
    expected_days: int,
    expected_sector_count: int,
    minimum_daily_count: int,
    relative: bool,
    preprocessor: Preprocessor | None = None,
) -> PreparedComponent:
    """Prepare one train or validation component without per-sector scaling."""

    approved_dates = _calendar_slice(calendar, start, end, expected_days)
    date_ordinals = {day: index for index, day in enumerate(calendar)}
    frame = _frame(panel)
    frame = frame[(frame["trade_date"] >= start) & (frame["trade_date"] <= end)].copy().reset_index(drop=True)
    codes = tuple(sorted(frame["sector_code"].unique().tolist()))
    if len(codes) != expected_sector_count:
        raise _fail(
            REASON_INPUT_IDENTITY,
            f"{level} canonical sector count is invalid",
            stage="input",
            evidence={"expected": expected_sector_count, "actual": len(codes)},
        )
    fitted = preprocessor or _fit_preprocessor(frame, feature_names)
    if fitted.feature_names != tuple(feature_names):
        raise _fail(REASON_PREPROCESS, "preprocess feature order mismatch", stage="preprocess")
    transformed, valid_mask = _transform(frame, fitted)
    unavailable: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for position, row in frame.iterrows():
        if not bool(valid_mask[position]):
            unavailable.append(
                {
                    "level": level,
                    "sector_code": str(row["sector_code"]),
                    "trade_date": row["trade_date"].isoformat(),
                    "reason_code": REASON_PREPROCESS,
                }
            )
            continue
        rows.append(
            {
                "trade_date": row["trade_date"],
                "sector_code": str(row["sector_code"]),
                "values": transformed[position],
            }
        )
    by_date: dict[date, list[dict[str, Any]]] = {}
    for row in rows:
        by_date.setdefault(row["trade_date"], []).append(row)
    sequence_rows: dict[str, list[tuple[date, np.ndarray]]] = {}
    for day in approved_dates:
        items = sorted(by_date.get(day, []), key=lambda item: item["sector_code"])
        if len(items) < minimum_daily_count:
            unavailable.append(
                {
                    "level": level,
                    "sector_code": "*",
                    "trade_date": day.isoformat(),
                    "reason_code": "hmm_risk_jump_cross_section_coverage_insufficient",
                    "available_count": len(items),
                    "required_count": minimum_daily_count,
                }
            )
            continue
        matrix = np.asarray([item["values"] for item in items], dtype=np.float64)
        median = np.median(matrix, axis=0)
        if not np.isfinite(median).all():
            raise _fail(REASON_PREPROCESS, "cross-section median is non-finite", stage="preprocess")
        if relative:
            for item in items:
                sequence_rows.setdefault(item["sector_code"], []).append((day, item["values"] - median))
        else:
            sequence_rows.setdefault("market", []).append((day, median))
    sequences: list[SequenceData] = []
    identities: list[list[str]] = []
    for key in sorted(sequence_rows):
        items = sequence_rows[key]
        dates = tuple(item[0] for item in items)
        values = np.asarray([item[1] for item in items], dtype=np.float64)
        if not np.isfinite(values).all():
            raise _fail(REASON_PREPROCESS, "prepared values are non-finite", stage="preprocess")
        sequences.append(
            SequenceData(
                key=key,
                dates=dates,
                ordinals=tuple(date_ordinals[item] for item in dates),
                values=values,
            )
        )
        identities.extend([[key, item.isoformat()] for item in dates])
    if not sequences:
        raise _fail(REASON_PREPROCESS, "component has no valid sequences", stage="preprocess")
    return PreparedComponent(
        component=component,
        level=level,
        feature_names=tuple(feature_names),
        expected_sector_count=expected_sector_count,
        minimum_daily_count=minimum_daily_count,
        canonical_codes=codes,
        sequences=tuple(sequences),
        preprocessor=fitted,
        unavailable_items=tuple(sorted(unavailable, key=lambda item: canonical_json_bytes(item))),
        valid_row_count=len(identities),
        valid_identity_sha256=canonical_sha256(identities),
    )


def _segment_bounds(ordinals: Sequence[int]) -> tuple[tuple[int, int], ...]:
    if not ordinals:
        return ()
    bounds: list[tuple[int, int]] = []
    start = 0
    for index in range(1, len(ordinals)):
        if ordinals[index] != ordinals[index - 1] + 1:
            bounds.append((start, index))
            start = index
    bounds.append((start, len(ordinals)))
    return tuple(bounds)


def _emission_costs(values: np.ndarray, centers: np.ndarray) -> np.ndarray:
    costs = np.sum((values[:, np.newaxis, :] - centers[np.newaxis, :, :]) ** 2, axis=2)
    if not np.isfinite(costs).all():
        raise _fail(REASON_OBJECTIVE_NON_FINITE, "emission cost is non-finite", stage="fit")
    return costs


def _optimal_segment_path(values: np.ndarray, centers: np.ndarray, jump_penalty: float) -> np.ndarray:
    """Return the global minimum path, with stable lower-index ties."""

    emissions = _emission_costs(values, centers)
    rows, states = emissions.shape
    costs = np.empty((rows, states), dtype=np.float64)
    predecessor = np.zeros((rows, states), dtype=np.int64)
    costs[0] = emissions[0]
    for row in range(1, rows):
        for current in range(states):
            candidates = costs[row - 1] + jump_penalty
            candidates = candidates.copy()
            candidates[current] = costs[row - 1, current]
            previous = int(np.argmin(candidates))
            predecessor[row, current] = previous
            costs[row, current] = emissions[row, current] + candidates[previous]
    path = np.empty(rows, dtype=np.int64)
    path[-1] = int(np.argmin(costs[-1]))
    for row in range(rows - 1, 0, -1):
        path[row - 1] = predecessor[row, path[row]]
    return path


def _optimal_paths(component: PreparedComponent, centers: np.ndarray, jump_penalty: float) -> tuple[np.ndarray, ...]:
    paths: list[np.ndarray] = []
    for sequence in component.sequences:
        path = np.empty(sequence.values.shape[0], dtype=np.int64)
        for start, end in _segment_bounds(sequence.ordinals):
            path[start:end] = _optimal_segment_path(sequence.values[start:end], centers, jump_penalty)
        paths.append(path)
    return tuple(paths)


def _causal_segment_states(values: np.ndarray, centers: np.ndarray, jump_penalty: float) -> np.ndarray:
    """Causal fixed-parameter recursion; never backtracks or smooths."""

    emissions = _emission_costs(values, centers)
    rows, states = emissions.shape
    output = np.empty(rows, dtype=np.int64)
    previous_cost = np.zeros(states, dtype=np.float64)
    for row in range(rows):
        current_cost = np.empty(states, dtype=np.float64)
        for current in range(states):
            candidates = previous_cost + jump_penalty
            candidates = candidates.copy()
            candidates[current] = previous_cost[current]
            previous = int(np.argmin(candidates))
            current_cost[current] = emissions[row, current] + candidates[previous]
        output[row] = int(np.argmin(current_cost))
        previous_cost = current_cost
    return output


def causal_states(component: PreparedComponent, centers: np.ndarray, jump_penalty: float) -> tuple[np.ndarray, ...]:
    paths: list[np.ndarray] = []
    for sequence in component.sequences:
        path = np.empty(sequence.values.shape[0], dtype=np.int64)
        for start, end in _segment_bounds(sequence.ordinals):
            path[start:end] = _causal_segment_states(sequence.values[start:end], centers, jump_penalty)
        paths.append(path)
    return tuple(paths)


def _objective(
    component: PreparedComponent,
    centers: np.ndarray,
    paths: Sequence[np.ndarray],
    jump_penalty: float,
) -> float:
    total = 0.0
    for sequence, path in zip(component.sequences, paths, strict=True):
        if path.shape != (sequence.values.shape[0],):
            raise _fail(REASON_INPUT_IDENTITY, "path shape mismatch", stage="fit")
        residual = sequence.values - centers[path]
        total += float(np.sum(residual * residual, dtype=np.float64))
        for start, end in _segment_bounds(sequence.ordinals):
            if end - start > 1:
                total += jump_penalty * int(np.count_nonzero(path[start + 1 : end] != path[start : end - 1]))
    if not math.isfinite(total):
        raise _fail(REASON_OBJECTIVE_NON_FINITE, "jump objective is non-finite", stage="fit")
    return total


def _updated_centers(
    component: PreparedComponent,
    paths: Sequence[np.ndarray],
    state_count: int,
) -> np.ndarray:
    rows: list[np.ndarray] = []
    labels: list[np.ndarray] = []
    for sequence, path in zip(component.sequences, paths, strict=True):
        rows.append(sequence.values)
        labels.append(path)
    stacked = np.vstack(rows)
    stacked_labels = np.concatenate(labels)
    centers = np.empty((state_count, stacked.shape[1]), dtype=np.float64)
    counts: list[int] = []
    for state in range(state_count):
        selected = stacked[stacked_labels == state]
        counts.append(int(selected.shape[0]))
        if selected.shape[0] == 0:
            raise _fail(
                REASON_STATE_EMPTY,
                "jump state has no assigned training row",
                stage="fit",
                evidence={"state": state, "state_counts": counts},
            )
        centers[state] = np.asarray(
            [math.fsum(column.tolist()) / len(column) for column in selected.T], dtype=np.float64
        )
    if not np.isfinite(centers).all():
        raise _fail(REASON_OBJECTIVE_NON_FINITE, "updated centers are non-finite", stage="fit")
    return centers


def fit_jump_model(
    component: PreparedComponent,
    *,
    state_count: int,
    jump_penalty: float,
    seed: int,
) -> JumpFit:
    """Fit one scheduled restart of the approved Gaussian-centroid jump model."""

    if state_count not in {2, 3} or jump_penalty not in LAMBDA_GRID or seed not in RESTART_SEEDS:
        raise _fail(REASON_INPUT_IDENTITY, "fit parameters are outside the approved grid", stage="fit")
    stacked = np.vstack([sequence.values for sequence in component.sequences])
    if stacked.shape[0] < state_count:
        raise _fail(REASON_STATE_EMPTY, "fewer rows than states", stage="fit")
    kmeans = KMeans(
        n_clusters=state_count,
        init="k-means++",
        n_init=1,
        random_state=seed,
        max_iter=300,
        tol=1e-4,
        algorithm="lloyd",
        copy_x=True,
    )
    labels = kmeans.fit_predict(stacked)
    paths: list[np.ndarray] = []
    offset = 0
    for sequence in component.sequences:
        end = offset + sequence.values.shape[0]
        paths.append(np.asarray(labels[offset:end], dtype=np.int64))
        offset = end
    current_paths = tuple(paths)
    centers = np.asarray(kmeans.cluster_centers_, dtype=np.float64)
    current_objective = _objective(component, centers, current_paths, jump_penalty)
    for iteration in range(1, MAX_JUMP_ITERATIONS + 1):
        centers = _updated_centers(component, current_paths, state_count)
        next_paths = _optimal_paths(component, centers, jump_penalty)
        next_objective = _objective(component, centers, next_paths, jump_penalty)
        envelope = OBJECTIVE_ENVELOPE_SCALE * max(1.0, abs(current_objective))
        if next_objective > current_objective + envelope:
            raise _fail(
                REASON_OBJECTIVE_INCREASED,
                "jump objective increased beyond the numeric envelope",
                stage="fit",
                evidence={
                    "previous": current_objective,
                    "current": next_objective,
                    "envelope": envelope,
                    "iteration": iteration,
                },
            )
        unchanged = all(np.array_equal(left, right) for left, right in zip(current_paths, next_paths, strict=True))
        if unchanged and abs(next_objective - current_objective) <= envelope:
            row_count = int(stacked.shape[0])
            feature_count = int(stacked.shape[1])
            return JumpFit(
                centers=centers,
                paths=next_paths,
                objective=float(next_objective),
                normalized_objective=float(next_objective / (row_count * feature_count)),
                iterations=iteration,
                seed=seed,
                jump_penalty=float(jump_penalty),
                row_count=row_count,
                feature_count=feature_count,
            )
        current_paths = next_paths
        current_objective = next_objective
    raise _fail(
        REASON_MAX_ITERATIONS,
        "jump fit reached the approved maximum iterations",
        stage="fit",
        evidence={"max_iterations": MAX_JUMP_ITERATIONS},
    )


def semantic_mapping(component: str, feature_names: Sequence[str], centers: np.ndarray) -> dict[int, str]:
    if component == "market":
        if centers.shape[0] != 2:
            raise _fail(REASON_INPUT_IDENTITY, "market model must have two states", stage="semantic")
        daily_index = tuple(feature_names).index("daily_return")
        volatility_index = tuple(feature_names).index("volatility_Nd")
        scores = centers[:, daily_index] - centers[:, volatility_index]
        labels = ("risk_off", "risk_on")
    else:
        if centers.shape[0] != 3:
            raise _fail(REASON_INPUT_IDENTITY, "relative model must have three states", stage="semantic")
        excess_index = tuple(feature_names).index("excess_return_Nd")
        scores = centers[:, excess_index]
        labels = ("fading", "neutral", "trending")
    if not np.isfinite(scores).all():
        raise _fail(REASON_SEMANTIC_TIE, "semantic score is non-finite", stage="semantic")
    order = np.argsort(scores, kind="stable")
    ordered = scores[order]
    if np.any(np.diff(ordered) <= SEMANTIC_TIE_TOLERANCE):
        raise _fail(
            REASON_SEMANTIC_TIE,
            "semantic scores are tied or insufficiently separated",
            stage="semantic",
            evidence={"scores": scores.tolist()},
        )
    return {int(state): label for state, label in zip(order, labels, strict=True)}


def state_rows(
    component: PreparedComponent,
    paths: Sequence[np.ndarray],
    mapping: Mapping[int, str],
) -> dict[tuple[str, date], str]:
    output: dict[tuple[str, date], str] = {}
    for sequence, path in zip(component.sequences, paths, strict=True):
        for day, state in zip(sequence.dates, path, strict=True):
            key = (sequence.key, day)
            if key in output or int(state) not in mapping:
                raise _fail(REASON_INPUT_IDENTITY, "state output identity is invalid", stage="semantic")
            output[key] = mapping[int(state)]
    return output


def _benchmark_rows(dataset_manifest: Mapping[str, Any]) -> dict[date, float]:
    calendar = dataset_manifest.get("calendar_benchmark")
    rows = calendar.get("rows") if isinstance(calendar, Mapping) else None
    if not isinstance(rows, list):
        raise _fail(REASON_INPUT_IDENTITY, "benchmark manifest rows are missing", stage="input")
    output: dict[date, float] = {}
    for item in rows:
        if not isinstance(item, list) or len(item) != 2:
            raise _fail(REASON_INPUT_IDENTITY, "benchmark manifest row is invalid", stage="input")
        day = _as_date(item[0])
        value = float(item[1])
        if day in output or not math.isfinite(value):
            raise _fail(REASON_INPUT_IDENTITY, "benchmark manifest is duplicate or non-finite", stage="input")
        output[day] = value
    return output


def _daily_returns(panel: pd.DataFrame) -> dict[tuple[str, date], float]:
    frame = _frame(panel)
    if "daily_return" not in frame:
        raise _fail(REASON_INPUT_IDENTITY, "panel daily_return is missing", stage="metric")
    output: dict[tuple[str, date], float] = {}
    for row in frame.itertuples(index=False):
        value = float(getattr(row, "daily_return"))
        if math.isfinite(value):
            output[(str(getattr(row, "sector_code")), getattr(row, "trade_date"))] = value
    return output


def _future_cumulative(values: Sequence[float]) -> float:
    product = 1.0
    for value in values:
        if not math.isfinite(value):
            raise ValueError("future return is non-finite")
        product *= 1.0 + value
    result = product - 1.0
    if not math.isfinite(result):
        raise ValueError("future cumulative return is non-finite")
    return result


def _rank_average(values: Sequence[float]) -> np.ndarray:
    return pd.Series(np.asarray(values, dtype=np.float64)).rank(method="average").to_numpy(dtype=np.float64)


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 5:
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


def _eligible_decision_dates(calendar: Sequence[date], *, start: date, end: date, horizon: int) -> tuple[date, ...]:
    in_window = [day for day in calendar if start <= day <= end]
    index = {day: position for position, day in enumerate(calendar)}
    return tuple(
        day for day in in_window if index[day] + horizon < len(calendar) and calendar[index[day] + horizon] <= end
    )


def _metric_date_identity(
    calendar: Sequence[date], *, start: date, end: date, horizon: int
) -> tuple[tuple[date, ...], dict[str, Any]]:
    in_window = tuple(day for day in calendar if start <= day <= end)
    eligible = _eligible_decision_dates(calendar, start=start, end=end, horizon=horizon)
    eligible_set = set(eligible)
    excluded_tail = tuple(day for day in in_window if day not in eligible_set)
    body = {
        "eligible_decision_dates": [day.isoformat() for day in eligible],
        "eligible_decision_date_set_sha256": canonical_sha256([day.isoformat() for day in eligible]),
        "excluded_tail_dates": [day.isoformat() for day in excluded_tail],
        "excluded_tail_date_set_sha256": canonical_sha256([day.isoformat() for day in excluded_tail]),
    }
    return eligible, body


def relative_fold_metrics(
    states: Mapping[tuple[str, date], str],
    panel: pd.DataFrame,
    benchmark_returns: Mapping[date, float],
    calendar: Sequence[date],
    *,
    validation_start: date,
    validation_end: date,
    horizon: int = 10,
) -> dict[str, Any]:
    """Compute the development-fold IC/spread metrics used for lambda selection."""

    returns = _daily_returns(panel)
    eligible, date_identity = _metric_date_identity(
        calendar,
        start=validation_start,
        end=validation_end,
        horizon=horizon,
    )
    position = {day: index for index, day in enumerate(calendar)}
    signals = {"fading": -1.0, "neutral": 0.0, "trending": 1.0}
    daily_ic: list[float] = []
    daily_spread: list[float] = []
    ic_unavailable: list[str] = []
    spread_unavailable: list[str] = []
    for day in eligible:
        future_dates = calendar[position[day] + 1 : position[day] + horizon + 1]
        try:
            benchmark = _future_cumulative([float(benchmark_returns[item]) for item in future_dates])
        except (KeyError, ValueError):
            ic_unavailable.append(day.isoformat())
            spread_unavailable.append(day.isoformat())
            continue
        observations: list[tuple[str, str, float, float]] = []
        for (code, state_day), label in states.items():
            if state_day != day or label not in signals:
                continue
            try:
                sector = _future_cumulative([returns[(code, item)] for item in future_dates])
            except (KeyError, ValueError):
                continue
            observations.append((code, label, signals[label], sector - benchmark))
        observations.sort(key=lambda item: item[0])
        ic = _spearman([item[2] for item in observations], [item[3] for item in observations])
        trending = [item[3] for item in observations if item[1] == "trending"]
        fading = [item[3] for item in observations if item[1] == "fading"]
        if ic is None:
            ic_unavailable.append(day.isoformat())
        else:
            daily_ic.append(ic)
        if len(trending) < 5 or len(fading) < 5:
            spread_unavailable.append(day.isoformat())
        else:
            spread = math.fsum(trending) / len(trending) - math.fsum(fading) / len(fading)
            if not math.isfinite(spread):
                spread_unavailable.append(day.isoformat())
            else:
                daily_spread.append(spread)
    required = math.ceil(0.80 * len(eligible))
    ic_available = len(daily_ic)
    spread_available = len(daily_spread)
    valid = len(eligible) > 0 and ic_available >= required and spread_available >= required
    body = {
        "schema_version": "hmm_risk_jump_relative_fold_metrics_v1",
        "horizon": horizon,
        **date_identity,
        "eligible_date_count": len(eligible),
        "required_date_count": required,
        "rank_ic_available_date_count": ic_available,
        "spread_available_date_count": spread_available,
        "rank_ic_unavailable_dates": sorted(set(ic_unavailable)),
        "spread_unavailable_dates": sorted(set(spread_unavailable)),
        "mean_rank_ic": math.fsum(daily_ic) / ic_available if ic_available else None,
        "mean_spread": math.fsum(daily_spread) / spread_available if spread_available else None,
        "metric_valid": valid,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def risk_metrics(outcomes: Sequence[bool], predictions: Sequence[bool]) -> dict[str, Any]:
    if len(outcomes) != len(predictions) or not outcomes:
        return {"metric_valid": False, "reason_code": REASON_SELECTION_METRIC}
    tp = sum(bool(actual and predicted) for actual, predicted in zip(outcomes, predictions, strict=True))
    fp = sum(bool(not actual and predicted) for actual, predicted in zip(outcomes, predictions, strict=True))
    fn = sum(bool(actual and not predicted) for actual, predicted in zip(outcomes, predictions, strict=True))
    tn = len(outcomes) - tp - fp - fn
    if tp + fp <= 0 or tp + fn <= 0:
        return {
            "metric_valid": False,
            "reason_code": REASON_SELECTION_METRIC,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
        }
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)
    if precision + recall <= 0.0:
        return {
            "metric_valid": False,
            "reason_code": REASON_SELECTION_METRIC,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
        }
    f1 = 2.0 * precision * recall / (precision + recall)
    base_rate = (tp + fn) / len(outcomes)
    return {
        "metric_valid": all(math.isfinite(value) for value in (precision, recall, f1, base_rate)),
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "base_rate": base_rate,
        "precision_lift": precision - base_rate,
    }


def market_fold_metrics(
    states: Mapping[tuple[str, date], str],
    benchmark_returns: Mapping[date, float],
    calendar: Sequence[date],
    *,
    validation_start: date,
    validation_end: date,
    horizon: int = 10,
) -> dict[str, Any]:
    eligible, date_identity = _metric_date_identity(
        calendar,
        start=validation_start,
        end=validation_end,
        horizon=horizon,
    )
    position = {day: index for index, day in enumerate(calendar)}
    outcomes: list[bool] = []
    predictions: list[bool] = []
    for day in eligible:
        label = states.get(("market", day))
        if label not in {"risk_on", "risk_off"}:
            return {
                "schema_version": "hmm_risk_jump_market_fold_metrics_v1",
                **date_identity,
                "metric_valid": False,
                "reason_code": REASON_SELECTION_METRIC,
                "eligible_date_count": len(eligible),
                "available_date_count": len(outcomes),
            }
        cumulative = 1.0
        minimum = math.inf
        try:
            for future_day in calendar[position[day] + 1 : position[day] + horizon + 1]:
                cumulative *= 1.0 + float(benchmark_returns[future_day])
                minimum = min(minimum, cumulative - 1.0)
        except (KeyError, ValueError):
            return {
                "schema_version": "hmm_risk_jump_market_fold_metrics_v1",
                **date_identity,
                "metric_valid": False,
                "reason_code": REASON_SELECTION_METRIC,
                "eligible_date_count": len(eligible),
                "available_date_count": len(outcomes),
            }
        outcomes.append(minimum <= -0.05)
        predictions.append(label == "risk_off")
    metrics = risk_metrics(outcomes, predictions)
    body = {
        "schema_version": "hmm_risk_jump_market_fold_metrics_v1",
        **date_identity,
        "eligible_date_count": len(eligible),
        "available_date_count": len(outcomes),
        **metrics,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def newey_west_t(values: Sequence[float], *, lag: int) -> dict[str, Any]:
    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 1 or array.size <= lag + 1 or lag < 0 or not np.isfinite(array).all():
        return {"metric_valid": False, "reason_code": "hmm_risk_jump_product_metric_unavailable"}
    mean = math.fsum(array.tolist()) / array.size
    centered = array - mean
    gamma0 = math.fsum((centered * centered).tolist()) / array.size
    variance_numerator = gamma0
    for offset in range(1, lag + 1):
        gamma = math.fsum((centered[offset:] * centered[:-offset]).tolist()) / array.size
        variance_numerator += 2.0 * (1.0 - offset / (lag + 1.0)) * gamma
    variance_mean = variance_numerator / array.size
    if not math.isfinite(variance_mean) or variance_mean <= 0.0:
        return {"metric_valid": False, "reason_code": "hmm_risk_jump_product_metric_unavailable"}
    statistic = mean / math.sqrt(variance_mean)
    if not math.isfinite(statistic):
        return {"metric_valid": False, "reason_code": "hmm_risk_jump_product_metric_unavailable"}
    return {
        "metric_valid": True,
        "sample_count": int(array.size),
        "lag": lag,
        "mean": mean,
        "variance_mean": variance_mean,
        "t_stat": statistic,
    }


def freeze_quintiles(
    rows: Sequence[Mapping[str, Any]],
    *,
    canonical_codes: Sequence[str],
    development_dates: Sequence[date],
    expected_development_days: int = DEVELOPMENT_TRADING_DAYS,
    expected_sector_count: int = 131,
) -> dict[str, Any]:
    """Freeze development-only L2 size/liquidity quintiles."""

    codes = tuple(sorted(set(str(value) for value in canonical_codes)))
    dates = tuple(development_dates)
    if (
        len(codes) != expected_sector_count
        or len(dates) != expected_development_days
        or dates != tuple(sorted(set(dates)))
        or (expected_development_days == DEVELOPMENT_TRADING_DAYS and dates[0] != DEVELOPMENT_START)
        or (expected_development_days == DEVELOPMENT_TRADING_DAYS and dates[-1] != DEVELOPMENT_END)
    ):
        raise _fail(
            REASON_REPRESENTATIVENESS,
            "development quintile authority is invalid",
            stage="coverage",
        )
    approved_dates = set(dates)
    by_code: dict[str, dict[str, list[float]]] = {code: {"size": [], "liquidity": []} for code in codes}
    seen: set[tuple[str, str]] = set()
    for row in rows:
        code = str(row.get("sector_code") or "")
        try:
            parsed_day = _as_date(row.get("trade_date"))
        except (TypeError, ValueError) as exc:
            raise _fail(
                REASON_REPRESENTATIVENESS,
                "quintile trade date is invalid",
                stage="coverage",
            ) from exc
        day = parsed_day.isoformat()
        if code not in by_code or parsed_day not in approved_dates or (code, day) in seen:
            raise _fail(REASON_REPRESENTATIVENESS, "quintile input identity is invalid", stage="coverage")
        seen.add((code, day))
        try:
            size = float(row.get("price_expected_weight"))
        except (TypeError, ValueError):
            size = math.nan
        try:
            liquidity = float(row.get("moneyflow_contributor_amount"))
        except (TypeError, ValueError):
            liquidity = math.nan
        if math.isfinite(size):
            by_code[code]["size"].append(size)
        if math.isfinite(liquidity):
            by_code[code]["liquidity"].append(liquidity)
    required = math.ceil(0.80 * expected_development_days)
    statistics: dict[str, dict[str, float]] = {}
    for code in codes:
        if len(by_code[code]["size"]) < required or len(by_code[code]["liquidity"]) < required:
            raise _fail(
                REASON_REPRESENTATIVENESS,
                "quintile evidence coverage is insufficient",
                stage="coverage",
                evidence={"sector_code": code, "required": required},
            )
        statistics[code] = {
            "size": float(np.median(np.asarray(by_code[code]["size"], dtype=np.float64))),
            "liquidity": float(np.median(np.asarray(by_code[code]["liquidity"], dtype=np.float64))),
        }
    groups: dict[str, dict[str, int]] = {"size": {}, "liquidity": {}}
    for field in ("size", "liquidity"):
        ordered = sorted(codes, key=lambda code: (statistics[code][field], code))
        for rank, code in enumerate(ordered):
            groups[field][code] = min(4, math.floor(rank * 5 / len(ordered)))
    body = {
        "schema_version": "hmm_risk_jump_development_quintiles_v1",
        "expected_development_days": expected_development_days,
        "minimum_coverage_ratio": 0.80,
        "canonical_codes": list(codes),
        "statistics": statistics,
        "groups": groups,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def classify_coverage(
    *,
    holdout_dates: Sequence[date],
    l1_codes: Sequence[str],
    l2_codes: Sequence[str],
    l1_available: set[tuple[str, date]],
    l2_available: set[tuple[str, date]],
    l2_to_l1: Mapping[str, str],
    size_quintiles: Mapping[str, int],
    liquidity_quintiles: Mapping[str, int],
    product_metrics_passed: bool,
) -> dict[str, Any]:
    """Apply the approved FULL/COVERAGE/NOT_AVAILABLE closure without reading data."""

    dates = tuple(holdout_dates)
    l1 = tuple(sorted(set(str(value) for value in l1_codes)))
    l2 = tuple(sorted(set(str(value) for value in l2_codes)))
    if (
        len(dates) != HOLDOUT_TRADING_DAYS
        or dates != tuple(sorted(set(dates)))
        or dates[0] != HOLDOUT_START
        or dates[-1] != HOLDOUT_END
        or len(l1) != 31
        or len(l2) != 131
        or set(l2_to_l1) != set(l2)
        or any(parent not in set(l1) for parent in l2_to_l1.values())
        or set(size_quintiles) != set(l2)
        or set(liquidity_quintiles) != set(l2)
        or set(size_quintiles.values()) != set(range(5))
        or set(liquidity_quintiles.values()) != set(range(5))
    ):
        return {
            "status": "NOT_AVAILABLE",
            "reason_code": REASON_REPRESENTATIVENESS,
            "product_metrics_passed": product_metrics_passed,
        }
    valid_l1_keys = {(code, day) for code in l1 for day in dates}
    valid_l2_keys = {(code, day) for code in l2 for day in dates}
    if not l1_available <= valid_l1_keys or not l2_available <= valid_l2_keys:
        return {
            "status": "NOT_AVAILABLE",
            "reason_code": REASON_COVERAGE,
            "product_metrics_passed": product_metrics_passed,
        }
    full = l1_available == valid_l1_keys and l2_available == valid_l2_keys
    qualified_dates = sum(
        sum((code, day) in l1_available for code in l1) >= 28 and sum((code, day) in l2_available for code in l2) >= 118
        for day in dates
    )
    sector_minimum = all(sum((code, day) in l1_available for day in dates) / len(dates) >= 0.80 for code in l1) and all(
        sum((code, day) in l2_available for day in dates) / len(dates) >= 0.80 for code in l2
    )

    def group_minimum(groups: Mapping[str, int]) -> bool:
        for group in range(5):
            members = [code for code in l2 if groups[code] == group]
            denominator = len(members) * len(dates)
            if denominator <= 0:
                return False
            numerator = sum((code, day) in l2_available for code in members for day in dates)
            if numerator / denominator < 0.80:
                return False
        return True

    hierarchy_valid = True
    for parent in l1:
        children = [code for code in l2 if l2_to_l1[code] == parent]
        if not children:
            hierarchy_valid = False
            break
        covered = sum(any((child, day) in l2_available for child in children) for day in dates)
        if covered / len(dates) < 0.90:
            hierarchy_valid = False
            break
    coverage_available = (
        qualified_dates / len(dates) >= 0.90
        and sector_minimum
        and group_minimum(size_quintiles)
        and group_minimum(liquidity_quintiles)
        and hierarchy_valid
    )
    status = (
        "FULL_READY"
        if product_metrics_passed and full
        else "COVERAGE_AVAILABLE"
        if product_metrics_passed and coverage_available
        else "NOT_AVAILABLE"
    )
    body = {
        "status": status,
        "reason_code": None if status != "NOT_AVAILABLE" else REASON_COVERAGE,
        "product_metrics_passed": product_metrics_passed,
        "holdout_date_count": len(dates),
        "l1_denominator": len(valid_l1_keys),
        "l1_available_count": len(l1_available),
        "l2_denominator": len(valid_l2_keys),
        "l2_available_count": len(l2_available),
        "qualified_date_count": qualified_dates,
        "full_coverage": full,
        "sector_minimum_passed": sector_minimum,
        "size_quintile_passed": group_minimum(size_quintiles),
        "liquidity_quintile_passed": group_minimum(liquidity_quintiles),
        "hierarchy_passed": hierarchy_valid,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def planned_fit_count() -> int:
    return 3 * len(LAMBDA_GRID) * len(RESTART_SEEDS) * len(FOLDS) + 3 * len(RESTART_SEEDS)


def _fit_summary(fit: JumpFit, component: PreparedComponent | None = None) -> dict[str, Any]:
    path_payload: list[dict[str, Any]] = []
    for index, path in enumerate(fit.paths):
        path_array = np.asarray(path, dtype="<i8")
        item: dict[str, Any] = {
            "sequence_key": component.sequences[index].key if component is not None else str(index),
            "dtype": "int64_le",
            "shape": list(path_array.shape),
            "sha256": sha256_bytes(path_array.tobytes()),
            "state_counts": [int(np.count_nonzero(path_array == state)) for state in range(fit.centers.shape[0])],
        }
        if component is not None:
            sequence = component.sequences[index]
            jumps = 0
            segments = _segment_bounds(sequence.ordinals)
            for start, end in segments:
                jumps += int(np.count_nonzero(path_array[start + 1 : end] != path_array[start : end - 1]))
            item.update(
                {
                    "date_set_sha256": canonical_sha256([day.isoformat() for day in sequence.dates]),
                    "segment_count": len(segments),
                    "jump_count": jumps,
                    "run_count": len(segments) + jumps,
                }
            )
        path_payload.append(item)
    state_counts = [
        int(sum(np.count_nonzero(path == state) for path in fit.paths)) for state in range(fit.centers.shape[0])
    ]
    jump_count: int | None = None
    run_count: int | None = None
    if component is not None:
        jump_count = 0
        run_count = 0
        for sequence, path in zip(component.sequences, fit.paths, strict=True):
            for start, end in _segment_bounds(sequence.ordinals):
                if end > start:
                    segment_jumps = int(np.count_nonzero(path[start + 1 : end] != path[start : end - 1]))
                    jump_count += segment_jumps
                    run_count += 1 + segment_jumps
    body = {
        "status": "fit_completed",
        "seed": fit.seed,
        "jump_penalty": fit.jump_penalty,
        "objective": fit.objective,
        "normalized_objective": fit.normalized_objective,
        "iterations": fit.iterations,
        "row_count": fit.row_count,
        "feature_count": fit.feature_count,
        "centers_sha256": _array_hash(fit.centers),
        "state_counts": state_counts,
        "sequence_count": len(fit.paths),
        "run_count": run_count,
        "jump_count": jump_count,
        "path_receipts": path_payload,
        "path_receipts_sha256": canonical_sha256(path_payload),
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _failed_attempt(seed: int, jump_penalty: float, error: BaseException) -> dict[str, Any]:
    if isinstance(error, JumpSpikeError):
        reason = error.reason_code
        stage = error.stage
        evidence = error.evidence
    else:
        reason = REASON_UNEXPECTED
        stage = "fit"
        evidence = {"exception_type": type(error).__name__, "error_message": str(error)}
    body = {
        "status": "fit_failed",
        "seed": seed,
        "jump_penalty": jump_penalty,
        "reason_code": reason,
        "stage": stage,
        "evidence": evidence,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _run_restarts(
    component: PreparedComponent,
    *,
    state_count: int,
    jump_penalty: float,
    attempt_log: list[dict[str, Any]],
    context: Mapping[str, Any],
) -> tuple[JumpFit, list[dict[str, Any]]]:
    successes: list[JumpFit] = []
    receipts: list[dict[str, Any]] = []
    for seed in RESTART_SEEDS:
        try:
            fit = fit_jump_model(
                component,
                state_count=state_count,
                jump_penalty=jump_penalty,
                seed=seed,
            )
            receipt = _fit_summary(fit, component)
            successes.append(fit)
        except Exception as exc:  # preserve every scheduled candidate failure
            receipt = _failed_attempt(seed, jump_penalty, exc)
        receipt_body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
        receipt_body = {**dict(context), **receipt_body}
        receipt = {**receipt_body, "receipt_sha256": canonical_sha256(receipt_body)}
        receipts.append(receipt)
        attempt_log.append(receipt)
    if not successes:
        raise _fail(
            REASON_SELECTION,
            "all scheduled restarts failed",
            stage="restart_selection",
            evidence={**dict(context), "jump_penalty": jump_penalty},
        )
    best_value = min(fit.normalized_objective for fit in successes)
    eligible = [
        fit
        for fit in successes
        if abs(fit.normalized_objective - best_value)
        <= RESTART_TIE_ATOL + RESTART_TIE_RTOL * max(abs(fit.normalized_objective), abs(best_value))
    ]
    selected = min(eligible, key=lambda fit: RESTART_SEEDS.index(fit.seed))
    return selected, receipts


def _select_lambda(receipts: Sequence[Mapping[str, Any]], *, component: str) -> float:
    eligible = [item for item in receipts if item.get("lambda_eligible") is True]
    if not eligible:
        raise _fail(REASON_SELECTION, "no lambda has three valid development folds", stage="lambda_selection")
    if component == "market":
        score_fields = ("median_f1", "median_precision_lift")
    else:
        score_fields = ("median_rank_ic", "median_spread")
    remaining = eligible
    for field in score_fields:
        values = [float(item[field]) for item in remaining]
        if not values or not all(math.isfinite(value) for value in values):
            raise _fail(REASON_SELECTION_METRIC, f"{field} is unavailable", stage="lambda_selection")
        best = max(values)
        remaining = [item for item in remaining if abs(float(item[field]) - best) <= LAMBDA_METRIC_TOLERANCE]
    return float(min(remaining, key=lambda item: LAMBDA_GRID.index(float(item["jump_penalty"])))["jump_penalty"])


def _component_spec(name: str) -> dict[str, Any]:
    if name == "market":
        return {
            "component": "market",
            "level": "L2",
            "features": MARKET_FEATURES,
            "expected_sector_count": 131,
            "minimum_daily_count": 118,
            "relative": False,
            "state_count": 2,
        }
    if name == "L1_relative":
        return {
            "component": "L1_relative",
            "level": "L1",
            "features": RELATIVE_FEATURES,
            "expected_sector_count": 31,
            "minimum_daily_count": 28,
            "relative": True,
            "state_count": 3,
        }
    if name == "L2_relative":
        return {
            "component": "L2_relative",
            "level": "L2",
            "features": RELATIVE_FEATURES,
            "expected_sector_count": 131,
            "minimum_daily_count": 118,
            "relative": True,
            "state_count": 3,
        }
    raise _fail(REASON_INPUT_IDENTITY, f"unknown component: {name}", stage="input")


def _component_panel(inputs: Mapping[str, Any], level: str) -> pd.DataFrame:
    value = inputs.get("panel" if level == "L1" else "l2_panel")
    if not isinstance(value, pd.DataFrame):
        raise _fail(REASON_INPUT_IDENTITY, f"{level} panel is missing", stage="input")
    return value


def _run_component(
    name: str,
    *,
    inputs: Mapping[str, Any],
    calendar: tuple[date, ...],
    benchmark: Mapping[date, float],
    attempt_log: list[dict[str, Any]],
) -> dict[str, Any]:
    spec = _component_spec(name)
    panel = _component_panel(inputs, spec["level"])
    lambda_receipts: list[dict[str, Any]] = []
    for jump_penalty in LAMBDA_GRID:
        fold_receipts: list[dict[str, Any]] = []
        for fold in FOLDS:
            train = prepare_component(
                panel,
                component=name,
                level=spec["level"],
                feature_names=spec["features"],
                calendar=calendar,
                start=fold["train_start"],
                end=fold["train_end"],
                expected_days=fold["train_days"],
                expected_sector_count=spec["expected_sector_count"],
                minimum_daily_count=spec["minimum_daily_count"],
                relative=spec["relative"],
            )
            attempt_start = len(attempt_log)
            selected: JumpFit | None = None
            selection_error: JumpSpikeError | None = None
            try:
                selected, _ = _run_restarts(
                    train,
                    state_count=spec["state_count"],
                    jump_penalty=jump_penalty,
                    attempt_log=attempt_log,
                    context={"component": name, "fold": fold["fold"], "phase": "development"},
                )
            except JumpSpikeError as exc:
                selection_error = exc
            attempts = attempt_log[attempt_start:]
            metrics: dict[str, Any] = {
                "metric_valid": False,
                "reason_code": selection_error.reason_code if selection_error else REASON_SELECTION_METRIC,
            }
            validation_accessed = False
            if selected is not None:
                try:
                    validation = prepare_component(
                        panel,
                        component=name,
                        level=spec["level"],
                        feature_names=spec["features"],
                        calendar=calendar,
                        start=fold["validation_start"],
                        end=fold["validation_end"],
                        expected_days=fold["validation_days"],
                        expected_sector_count=spec["expected_sector_count"],
                        minimum_daily_count=spec["minimum_daily_count"],
                        relative=spec["relative"],
                        preprocessor=train.preprocessor,
                    )
                    validation_accessed = True
                    mapping = semantic_mapping(name, spec["features"], selected.centers)
                    paths = causal_states(validation, selected.centers, jump_penalty)
                    states = state_rows(validation, paths, mapping)
                    if name == "market":
                        metrics = market_fold_metrics(
                            states,
                            benchmark,
                            calendar,
                            validation_start=fold["validation_start"],
                            validation_end=fold["validation_end"],
                        )
                    else:
                        metrics = relative_fold_metrics(
                            states,
                            panel,
                            benchmark,
                            calendar,
                            validation_start=fold["validation_start"],
                            validation_end=fold["validation_end"],
                        )
                except JumpSpikeError as exc:
                    metrics = {"metric_valid": False, "reason_code": exc.reason_code, "stage": exc.stage}
            fold_body = {
                "fold": fold["fold"],
                "train_start": fold["train_start"].isoformat(),
                "train_end": fold["train_end"].isoformat(),
                "validation_start": fold["validation_start"].isoformat(),
                "validation_end": fold["validation_end"].isoformat(),
                "jump_penalty": jump_penalty,
                "preprocess": train.preprocessor.payload(),
                "preprocess_sha256": canonical_sha256(train.preprocessor.payload()),
                "selected_seed": selected.seed if selected is not None else None,
                "selected_fit": _fit_summary(selected, train) if selected is not None else None,
                "attempts": attempts,
                "metrics": metrics,
                "metric_valid": metrics.get("metric_valid") is True,
                "validation_accessed": validation_accessed,
                "holdout_accessed": False,
            }
            fold_receipts.append({**fold_body, "receipt_sha256": canonical_sha256(fold_body)})
        metric_valid = all(item["metric_valid"] is True for item in fold_receipts)
        if name == "market":
            f1 = [float(item["metrics"]["f1"]) for item in fold_receipts] if metric_valid else []
            lift = [float(item["metrics"]["precision_lift"]) for item in fold_receipts] if metric_valid else []
            score = {
                "median_f1": float(np.median(f1)) if f1 else None,
                "median_precision_lift": float(np.median(lift)) if lift else None,
            }
        else:
            ic = [float(item["metrics"]["mean_rank_ic"]) for item in fold_receipts] if metric_valid else []
            spread = [float(item["metrics"]["mean_spread"]) for item in fold_receipts] if metric_valid else []
            score = {
                "median_rank_ic": float(np.median(ic)) if ic else None,
                "median_spread": float(np.median(spread)) if spread else None,
            }
        lambda_body = {
            "jump_penalty": jump_penalty,
            "folds": fold_receipts,
            "fold_count": len(fold_receipts),
            "lambda_eligible": metric_valid,
            **score,
        }
        lambda_receipts.append({**lambda_body, "receipt_sha256": canonical_sha256(lambda_body)})
    selected_lambda = _select_lambda(lambda_receipts, component=name)
    final = prepare_component(
        panel,
        component=name,
        level=spec["level"],
        feature_names=spec["features"],
        calendar=calendar,
        start=DEVELOPMENT_START,
        end=DEVELOPMENT_END,
        expected_days=DEVELOPMENT_TRADING_DAYS,
        expected_sector_count=spec["expected_sector_count"],
        minimum_daily_count=spec["minimum_daily_count"],
        relative=spec["relative"],
    )
    final_fit, final_attempts = _run_restarts(
        final,
        state_count=spec["state_count"],
        jump_penalty=selected_lambda,
        attempt_log=attempt_log,
        context={"component": name, "fold": "final-development", "phase": "final"},
    )
    mapping = semantic_mapping(name, spec["features"], final_fit.centers)
    body = {
        "schema_version": "hmm_risk_market_relative_jump_component_v1",
        "component": name,
        "level": spec["level"],
        "state_count": spec["state_count"],
        "feature_names": list(spec["features"]),
        "canonical_sector_count": len(final.canonical_codes),
        "canonical_sector_sha256": canonical_sha256(list(final.canonical_codes)),
        "lambda_receipts": lambda_receipts,
        "selected_lambda": selected_lambda,
        "final_selected_seed": final_fit.seed,
        "final_fit": _fit_summary(final_fit, final),
        "final_attempts": final_attempts,
        "preprocess": final.preprocessor.payload(),
        "preprocess_sha256": canonical_sha256(final.preprocessor.payload()),
        "final_centers": final_fit.centers.tolist(),
        "final_centers_sha256": _array_hash(final_fit.centers),
        "semantic_mapping": {str(key): value for key, value in sorted(mapping.items())},
        "arrival_cost_policy": "zero_at_each_segment_start_no_train_carry",
        "valid_row_count": final.valid_row_count,
        "valid_identity_sha256": final.valid_identity_sha256,
        "unavailable_items": list(final.unavailable_items),
        "unavailable_item_count": len(final.unavailable_items),
        "holdout_accessed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _runtime_versions() -> dict[str, Any]:
    import scipy
    import sklearn  # local import keeps the module's version receipt explicit
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


def _request_identity(request: Mapping[str, Any], producer_commit: str) -> dict[str, Any]:
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
        raise _fail(REASON_INPUT_IDENTITY, "request schema is invalid", stage="input")
    if request.get("contract_version") != CONTRACT_VERSION:
        raise _fail(REASON_INPUT_IDENTITY, "request contract is invalid", stage="input")
    if str(request.get("expected_producer_commit") or "") != producer_commit:
        raise _fail(REASON_INPUT_IDENTITY, "producer commit differs from request", stage="input")
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
        "source_sha256": canonical_sha256(request.get("source")),
    }


def run_p2_3_spike(
    inputs: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    producer_commit: str,
) -> dict[str, Any]:
    """Execute the approved 456-fit development spike without touching holdout data."""

    request_identity = _request_identity(request, producer_commit)
    raw_calendar = inputs.get("trading_dates")
    if not isinstance(raw_calendar, (tuple, list)):
        raise _fail(REASON_INPUT_IDENTITY, "trading calendar is missing", stage="input")
    calendar = tuple(_as_date(value) for value in raw_calendar)
    if calendar != tuple(sorted(set(calendar))):
        raise _fail(REASON_INPUT_IDENTITY, "trading calendar is not sorted and unique", stage="input")
    development_dates = _calendar_slice(calendar, DEVELOPMENT_START, DEVELOPMENT_END, DEVELOPMENT_TRADING_DAYS)
    if any(day >= HOLDOUT_START for day in calendar):
        raise _fail(
            REASON_HOLDOUT,
            "P2-3 inputs contain forbidden holdout dates",
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
        for name in ("market", "L1_relative", "L2_relative"):
            components.append(
                _run_component(
                    name,
                    inputs=inputs,
                    calendar=calendar,
                    benchmark=benchmark,
                    attempt_log=attempt_log,
                )
            )
    except JumpSpikeError as exc:
        evidence = {
            **exc.evidence,
            "completed_fit_count": len(attempt_log),
            "fit_attempts_sha256": canonical_sha256(attempt_log),
            "fit_attempts": attempt_log,
        }
        raise JumpSpikeError(exc.reason_code, str(exc), stage=exc.stage, evidence=evidence) from exc
    except Exception as exc:
        evidence = {
            "exception_type": type(exc).__name__,
            "error_message": str(exc),
            "completed_fit_count": len(attempt_log),
            "fit_attempts_sha256": canonical_sha256(attempt_log),
            "fit_attempts": attempt_log,
        }
        raise JumpSpikeError(
            REASON_UNEXPECTED,
            "unexpected P2-3 spike failure",
            stage="unknown",
            evidence=evidence,
        ) from exc
    if len(attempt_log) != planned_fit_count():
        raise _fail(
            REASON_INPUT_IDENTITY,
            "completed fit attempt count differs from the approved plan",
            stage="finalization",
            evidence={"planned": planned_fit_count(), "actual": len(attempt_log)},
        )
    dataset_hash = canonical_sha256(dataset_manifest)
    mapping_hash = canonical_sha256(mapping_manifest)
    component_hashes = [str(item["receipt_sha256"]) for item in components]
    body = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "status": "P2_3_SPIKE_ACCEPTED_PENDING_P2_4_HOLDOUT_ACCEPTANCE",
        "producer_commit": producer_commit,
        "runtime_versions": _runtime_versions(),
        "request_identity": request_identity,
        "request_identity_sha256": canonical_sha256(request_identity),
        "dataset_manifest_sha256": dataset_hash,
        "mapping_manifest_sha256": mapping_hash,
        "database_identity": inputs.get("database"),
        "calendar_manifest_sha256": canonical_sha256(dataset_manifest.get("calendar_benchmark")),
        "feature_formula_sha256": canonical_sha256(
            {
                "L1": inputs.get("feature_definition"),
                "L2": inputs.get("l2_feature_definition"),
            }
        ),
        "hierarchy_sha256": mapping_hash,
        "development_start": DEVELOPMENT_START.isoformat(),
        "development_end": DEVELOPMENT_END.isoformat(),
        "development_trading_day_count": len(development_dates),
        "development_date_set_sha256": canonical_sha256([item.isoformat() for item in development_dates]),
        "forbidden_holdout_start": HOLDOUT_START.isoformat(),
        "forbidden_holdout_end": HOLDOUT_END.isoformat(),
        "forbidden_holdout_date_set_sha256": request_identity["forbidden_holdout_date_set_sha256"],
        "planned_fit_count": planned_fit_count(),
        "completed_fit_count": len(attempt_log),
        "fit_attempts_sha256": canonical_sha256(attempt_log),
        "components": components,
        "component_receipt_sha256s": component_hashes,
        "component_count": len(components),
        "candidate_status": "development_candidate_frozen",
        "failure_stage": None,
        "failure_reason_code": None,
        "holdout_accessed": False,
        "selection_performed": True,
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


def failure_report(
    request: Mapping[str, Any],
    *,
    producer_commit: str,
    error: BaseException,
    completed_fit_count: int = 0,
) -> dict[str, Any]:
    if isinstance(error, JumpSpikeError):
        reason_code = error.reason_code
        stage = error.stage
        evidence = error.evidence
    else:
        reason_code = REASON_UNEXPECTED
        stage = "unknown"
        evidence = {"exception_type": type(error).__name__, "error_message": str(error)}
    body = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "status": "NOT_AVAILABLE_FOR_PROMOTION",
        "producer_commit": producer_commit,
        "runtime_versions": _runtime_versions(),
        "request_sha256": canonical_sha256(request),
        "planned_fit_count": planned_fit_count(),
        "completed_fit_count": completed_fit_count,
        "failure_stage": stage,
        "failure_reason_code": reason_code,
        "failure_evidence": evidence,
        "holdout_accessed": False,
        "selection_performed": False,
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
    if failure:
        body["failure_receipt_write"] = True
        body["candidate_receipt_write"] = False
    else:
        body["candidate_receipt_write"] = True
        body["failure_receipt_write"] = False
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
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
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
