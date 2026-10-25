"""Deterministic jump-model primitives used by the active HMM risk contract.

This module contains only the model-neutral data structures and algorithms
needed by the current rotation inference path.  It owns no experiment,
selection, holdout, artifact, database, or runtime behaviour.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.cluster import KMeans


CURRENT_STATE_COUNT = 2
CURRENT_JUMP_PENALTY = 4.0
CURRENT_SEED = 42
MAX_JUMP_ITERATIONS = 200
OBJECTIVE_ENVELOPE_SCALE = 1e-10

REASON_INPUT_IDENTITY = "hmm_risk_jump_input_identity_mismatch"
REASON_OBJECTIVE_NON_FINITE = "hmm_risk_jump_objective_non_finite"
REASON_OBJECTIVE_INCREASED = "hmm_risk_jump_objective_increased"
REASON_STATE_EMPTY = "hmm_risk_jump_state_empty"
REASON_MAX_ITERATIONS = "hmm_risk_jump_max_iterations_reached"


class JumpModelError(RuntimeError):
    """Typed fail-closed error for the deterministic jump model."""

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


def _fail(
    reason_code: str,
    message: str,
    *,
    stage: str,
    evidence: Mapping[str, Any] | None = None,
) -> JumpModelError:
    return JumpModelError(reason_code, message, stage=stage, evidence=evidence)


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
        # The schema identity is retained because active V16 receipts bind it.
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


def _optimal_paths(
    component: PreparedComponent,
    centers: np.ndarray,
    jump_penalty: float,
) -> tuple[np.ndarray, ...]:
    paths: list[np.ndarray] = []
    for sequence in component.sequences:
        path = np.empty(sequence.values.shape[0], dtype=np.int64)
        for start, end in _segment_bounds(sequence.ordinals):
            path[start:end] = _optimal_segment_path(sequence.values[start:end], centers, jump_penalty)
        paths.append(path)
    return tuple(paths)


def _causal_segment_states(values: np.ndarray, centers: np.ndarray, jump_penalty: float) -> np.ndarray:
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


def causal_states(
    component: PreparedComponent,
    centers: np.ndarray,
    jump_penalty: float,
) -> tuple[np.ndarray, ...]:
    """Run fixed-parameter causal recursion without smoothing or backtracking."""

    if jump_penalty != CURRENT_JUMP_PENALTY:
        raise _fail(REASON_INPUT_IDENTITY, "jump penalty is outside the current contract", stage="inference")
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
    stacked = np.vstack([sequence.values for sequence in component.sequences])
    stacked_labels = np.concatenate(paths)
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
            [math.fsum(column.tolist()) / len(column) for column in selected.T],
            dtype=np.float64,
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
    """Fit the single deterministic jump-model profile used by current rotation."""

    if state_count != CURRENT_STATE_COUNT or jump_penalty != CURRENT_JUMP_PENALTY or seed != CURRENT_SEED:
        raise _fail(REASON_INPUT_IDENTITY, "fit parameters are outside the current contract", stage="fit")
    if not component.sequences:
        raise _fail(REASON_STATE_EMPTY, "component has no sequences", stage="fit")
    stacked = np.vstack([sequence.values for sequence in component.sequences])
    if stacked.shape[0] < state_count or not np.isfinite(stacked).all():
        raise _fail(REASON_STATE_EMPTY, "training values are invalid or fewer than states", stage="fit")
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
        "jump fit reached the current maximum iterations",
        stage="fit",
        evidence={"max_iterations": MAX_JUMP_ITERATIONS},
    )


__all__ = [
    "CURRENT_JUMP_PENALTY",
    "CURRENT_SEED",
    "CURRENT_STATE_COUNT",
    "JumpFit",
    "JumpModelError",
    "PreparedComponent",
    "Preprocessor",
    "SequenceData",
    "causal_states",
    "fit_jump_model",
]
