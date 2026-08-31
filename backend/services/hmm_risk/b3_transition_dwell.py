"""Exact train-only TRANSITION-DWELL-B prior and MAP objective components."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np

from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_sha256


CONTRACT_VERSION = "hmm_risk_c008_b3_transition_dwell_b_v1"
TARGET_FAMILY = "autocycle_all_core"
TARGET_LEVEL = "L2"
TRANSITION_ALPHA = 0.1
SELF_CENTER_MIN = 0.50
SELF_CENTER_MAX = 0.90
PRIOR_CONCENTRATION = 8.0
ROW_SUM_TOLERANCE = 1e-12

REASON_PRIOR_INVALID = "hmm_risk_model_transition_prior_invalid"
REASON_MATRIX_INVALID = "hmm_risk_model_transition_matrix_invalid"
REASON_MAP_NON_FINITE = "hmm_risk_model_transition_map_objective_non_finite"


class TransitionDwellContractError(StateModelSetError):
    """Stable typed failure for the approved level-local transition contract."""

    def __init__(self, reason_code: str, message: str, *, evidence: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.evidence = dict(evidence or {})


def _strict_counts(value: Any) -> np.ndarray:
    counts = np.asarray(value, dtype=np.float64)
    valid = (
        counts.shape == (3, 3)
        and np.isfinite(counts).all()
        and np.all(counts >= 0.0)
        and np.array_equal(counts, np.floor(counts))
    )
    if not valid:
        raise TransitionDwellContractError(
            REASON_PRIOR_INVALID,
            "transition counts must be a finite non-negative 3x3 integer matrix",
            evidence={"shape": list(counts.shape)},
        )
    return counts


def build_transition_prior(transition_counts: Any) -> dict[str, Any]:
    """Build the exact KMeans-sequence prior centre and Dirichlet prior."""

    counts = _strict_counts(transition_counts)
    row_totals = counts.sum(axis=1)
    q = (counts + TRANSITION_ALPHA) / (row_totals[:, None] + 3.0 * TRANSITION_ALPHA)
    center = np.empty((3, 3), dtype=np.float64)
    for state in range(3):
        self_probability = float(np.clip(q[state, state], SELF_CENTER_MIN, SELF_CENTER_MAX))
        center[state, state] = self_probability
        off_states = [index for index in range(3) if index != state]
        denominator = float(sum(counts[state, index] + TRANSITION_ALPHA for index in off_states))
        for index in off_states:
            center[state, index] = (1.0 - self_probability) * ((counts[state, index] + TRANSITION_ALPHA) / denominator)
    prior = 1.0 + PRIOR_CONCENTRATION * center
    row_errors = np.abs(center.sum(axis=1) - 1.0)
    if (
        not np.isfinite(q).all()
        or not np.isfinite(center).all()
        or not np.isfinite(prior).all()
        or np.any(center <= 0.0)
        or np.any(prior <= 1.0)
        or float(row_errors.max()) > ROW_SUM_TOLERANCE
    ):
        raise TransitionDwellContractError(
            REASON_PRIOR_INVALID,
            "transition prior centre is invalid",
            evidence={"maximum_row_sum_error": float(row_errors.max())},
        )
    body = {
        "contract_version": CONTRACT_VERSION,
        "transition_counts": counts.astype(np.int64).tolist(),
        "row_totals": row_totals.astype(np.int64).tolist(),
        "alpha": TRANSITION_ALPHA,
        "q": q.tolist(),
        "self_center_bounds": [SELF_CENTER_MIN, SELF_CENTER_MAX],
        "transition_prior_center": center.tolist(),
        "tau": PRIOR_CONCENTRATION,
        "transmat_prior": prior.tolist(),
        "maximum_center_row_sum_error": float(row_errors.max()),
        "train_only": True,
        "validation_accessed": False,
        "future_utility_accessed": False,
    }
    return {**body, "transition_prior_sha256": canonical_sha256(body)}


def transition_map_objective(transmat: Any, transmat_prior: Any) -> dict[str, Any]:
    """Return the approved transition contribution and expected-dwell diagnostic."""

    matrix = np.asarray(transmat, dtype=np.float64)
    prior = np.asarray(transmat_prior, dtype=np.float64)
    row_errors = np.abs(matrix.sum(axis=1) - 1.0) if matrix.shape == (3, 3) else np.asarray([np.inf])
    if (
        matrix.shape != (3, 3)
        or prior.shape != (3, 3)
        or not np.isfinite(matrix).all()
        or not np.isfinite(prior).all()
        or np.any(matrix <= 0.0)
        or np.any(prior <= 1.0)
        or float(row_errors.max()) > ROW_SUM_TOLERANCE
    ):
        raise TransitionDwellContractError(
            REASON_MATRIX_INVALID,
            "fitted transition matrix or prior is invalid",
            evidence={"maximum_row_sum_error": float(row_errors.max())},
        )
    adjustment = float(np.sum((prior - 1.0) * np.log(matrix), dtype=np.float64))
    denominator = 1.0 - np.diag(matrix)
    if not np.isfinite(denominator).all() or np.any(denominator <= 0.0):
        raise TransitionDwellContractError(
            REASON_MATRIX_INVALID,
            "fitted transition matrix implies invalid expected dwell",
        )
    if not np.isfinite(adjustment):
        raise TransitionDwellContractError(
            REASON_MAP_NON_FINITE,
            "transition MAP objective is non-finite",
        )
    dwell = 1.0 / denominator
    if not np.isfinite(dwell).all():
        raise TransitionDwellContractError(REASON_MAP_NON_FINITE, "expected dwell is non-finite")
    body = {
        "contract_version": CONTRACT_VERSION,
        "raw_transmat": matrix.tolist(),
        "raw_transmat_sha256": canonical_sha256(matrix.tolist()),
        "transmat_prior": prior.tolist(),
        "transmat_prior_sha256": canonical_sha256(prior.tolist()),
        "transition_prior_adjustment": adjustment,
        "maximum_transmat_row_sum_error": float(row_errors.max()),
        "expected_dwell_diagnostic_only": dwell.tolist(),
    }
    return {**body, "transition_component_sha256": canonical_sha256(body)}


def assert_target_scope(*, family: str, level: str) -> None:
    if family != TARGET_FAMILY or level != TARGET_LEVEL:
        raise TransitionDwellContractError(
            REASON_PRIOR_INVALID,
            f"{CONTRACT_VERSION} is restricted to {TARGET_FAMILY}:{TARGET_LEVEL}",
            evidence={"family": family, "level": level},
        )
