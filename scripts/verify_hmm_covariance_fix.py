#!/usr/bin/env python
"""Regression check for RD-Agent HMM covariance clipping.

This script creates synthetic GaussianHMM objects with out-of-bounds
covariances and verifies that train_sector_hmm.validate_and_fix_covariance
persists the clipped values through hmmlearn's covariance setter.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
from hmmlearn.hmm import GaussianHMM


def _rd_agent_root() -> Path:
    raw = os.getenv("RD_AGENT_ROOT")
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "RD-Agent-main"


def _build_hmm(covariance_type: str, covars: np.ndarray) -> GaussianHMM:
    hmm = GaussianHMM(n_components=covars.shape[0], covariance_type=covariance_type)
    n_features = covars.shape[-1] if covars.ndim == 3 else covars.shape[1]
    hmm.n_features = n_features
    hmm.startprob_ = np.full(hmm.n_components, 1.0 / hmm.n_components)
    hmm.transmat_ = np.full((hmm.n_components, hmm.n_components), 1.0 / hmm.n_components)
    hmm.means_ = np.zeros((hmm.n_components, n_features))
    hmm.covars_ = covars
    return hmm


def main() -> None:
    rd_root = _rd_agent_root()
    sys.path.insert(0, str(rd_root))

    from model_training.hmm.train_sector_hmm import (  # noqa: WPS433
        covariance_bound_stats,
        validate_and_fix_covariance,
    )

    diag_hmm = _build_hmm(
        "diag",
        np.array([[1000.0, 0.000001, 2.0], [47.35, 5.0, 0.1]], dtype=np.float64),
    )
    fixed, count = validate_and_fix_covariance(diag_hmm, max_covar=10.0, min_covar=1e-3)
    stats = covariance_bound_stats(diag_hmm, max_covar=10.0, min_covar=1e-3)
    assert fixed is True
    assert count == 3
    assert stats["covariance_max_after"] == 10.0
    assert stats["covariance_min_after"] == 0.001

    full_hmm = _build_hmm(
        "full",
        np.array([[[1000.0, 0.0], [0.0, 0.000001]]], dtype=np.float64),
    )
    fixed, count = validate_and_fix_covariance(full_hmm, max_covar=10.0, min_covar=1e-3)
    stats = covariance_bound_stats(full_hmm, max_covar=10.0, min_covar=1e-3)
    assert fixed is True
    assert count == 2
    assert stats["covariance_max_after"] == 10.0
    assert stats["covariance_min_after"] == 0.001

    print("HMM covariance clipping regression passed")


if __name__ == "__main__":
    main()
