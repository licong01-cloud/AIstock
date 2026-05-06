#!/usr/bin/env python3
"""Stage-3 HMM sector-factor retraining screen.

This wrapper keeps the existing diagnostic engine read-only, but extends its
candidate list with broader sector-rotation scenarios.  Every candidate here is
still a real HMM retrain: the added sector-factor columns enter the observation
matrix before ``GaussianHMM.fit``.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import hmm_sector_factor_retrain_diagnostic_20260504 as base  # noqa: E402


BASE_FIT_PREPROCESS = base.fit_preprocess
BASE_APPLY_PREPROCESS = base.apply_preprocess


STAGE3_CANDIDATES = (
    *base.CANDIDATES,
    base.CandidateSpec(
        name="stage3_flow_breadth_tier",
        description="Flow core + breadth confirmation + large-vs-small flow tier state.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
            "sf_flow_tier_strength_10d",
        ),
    ),
    base.CandidateSpec(
        name="stage3_flow_breadth_vol10",
        description="Flow + breadth with short volatility compression as HMM emission context.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
            "sf_volatility_10d_neg",
            "sf_intraday_range_5d_neg",
        ),
    ),
    base.CandidateSpec(
        name="stage3_flow_breadth_turnover_light",
        description="Flow + breadth with light turnover-crowding context, avoiding full turnover-heavy union.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
            "sf_turnover_pctile_120d_neg",
            "sf_turnover_ma5_ma20_neg",
        ),
    ),
    base.CandidateSpec(
        name="stage3_flow_breadth_mom20",
        description="Flow + breadth with sector momentum and lottery-return reversal context.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
            "sf_amount_weighted_mom_20d",
            "sf_max_ret_20d_neg",
        ),
    ),
    base.CandidateSpec(
        name="stage3_flow_dynamic_breadth",
        description="Flow core + dynamic flow-volatility sentiment + breadth/dispersion confirmation.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_dynamic_flow_vol_sentiment",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
        ),
    ),
    base.CandidateSpec(
        name="stage3_compact_stability_breadth_vol",
        description="Low-dimensional stability/breadth/volatility HMM input to reduce overfit risk.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_excess_breadth_5d",
            "sf_volatility_10d_neg",
        ),
    ),
    base.CandidateSpec(
        name="stage3_breadth_mom_regime",
        description="Non-flow scenario: breadth, dispersion, momentum and volatility as sector-regime emissions.",
        sector_features=(
            "sf_breadth_5d",
            "sf_breadth_10d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
            "sf_amount_weighted_mom_20d",
            "sf_volatility_20d_neg",
        ),
    ),
    base.CandidateSpec(
        name="stage3_flow_breadth_tier_robust",
        description="Robust-zscore version of flow + breadth + flow-tier HMM emissions.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
            "sf_flow_tier_strength_10d",
        ),
        preprocess="robust_zscore",
    ),
    base.CandidateSpec(
        name="stage3_flow_breadth_tier_zscore",
        description="Plain train-only zscore version of flow + breadth + flow-tier HMM emissions.",
        sector_features=(
            "sf_mf_net_ratio_std_5d_neg",
            "sf_small_net_ratio_5d",
            "sf_breadth_5d",
            "sf_excess_breadth_5d",
            "sf_dispersion_5d_neg",
            "sf_flow_tier_strength_10d",
        ),
        preprocess="zscore",
    ),
)

DEFAULT_STAGE3_CANDIDATES = (
    "baseline_legacy7_winsor_zscore",
    "flow_plus_breadth",
    "stage3_flow_breadth_tier",
    "stage3_flow_breadth_vol10",
    "stage3_flow_breadth_turnover_light",
    "stage3_flow_breadth_mom20",
    "stage3_flow_dynamic_breadth",
    "stage3_compact_stability_breadth_vol",
    "stage3_breadth_mom_regime",
    "stage3_flow_breadth_tier_robust",
    "stage3_flow_breadth_tier_zscore",
)


def fit_preprocess_stage3(
    obs_by_sector: dict[str, np.ndarray],
    mode: str,
    winsor_q: float,
) -> dict[str, Any]:
    """Extend the base diagnostic with train-only zscore variants."""
    if mode in {"identity", "winsor_zscore"}:
        return BASE_FIT_PREPROCESS(obs_by_sector, mode, winsor_q)
    all_obs = np.vstack([obs for obs in obs_by_sector.values() if len(obs)])
    if all_obs.size == 0:
        raise RuntimeError("Cannot fit preprocessing on empty observations")
    params: dict[str, Any] = {
        "mode": mode,
        "fit_scope": "train_window_only",
        "feature_count": int(all_obs.shape[1]),
        "train_observation_count": int(all_obs.shape[0]),
    }
    if mode == "zscore":
        mean = all_obs.mean(axis=0)
        std = np.where(all_obs.std(axis=0) < 1e-10, 1.0, all_obs.std(axis=0))
        params.update({"mean": mean.tolist(), "std": std.tolist()})
        return params
    if mode == "robust_zscore":
        median = np.median(all_obs, axis=0)
        q25 = np.quantile(all_obs, 0.25, axis=0)
        q75 = np.quantile(all_obs, 0.75, axis=0)
        scale = (q75 - q25) / 1.349
        fallback = np.where(all_obs.std(axis=0) < 1e-10, 1.0, all_obs.std(axis=0))
        scale = np.where(np.abs(scale) < 1e-10, fallback, scale)
        params.update(
            {
                "median": median.tolist(),
                "q25": q25.tolist(),
                "q75": q75.tolist(),
                "scale": scale.tolist(),
            }
        )
        return params
    raise ValueError(f"Unsupported preprocess mode: {mode}")


def apply_preprocess_stage3(obs: np.ndarray, params: dict[str, Any]) -> np.ndarray:
    mode = params["mode"]
    if mode in {"identity", "winsor_zscore"}:
        return BASE_APPLY_PREPROCESS(obs, params)
    if mode == "zscore":
        mean = np.asarray(params["mean"], dtype=np.float64)
        std = np.asarray(params["std"], dtype=np.float64)
        return (obs - mean) / std
    if mode == "robust_zscore":
        median = np.asarray(params["median"], dtype=np.float64)
        scale = np.asarray(params["scale"], dtype=np.float64)
        return (obs - median) / scale
    raise ValueError(f"Unsupported preprocess mode: {mode}")


def _has_option(name: str) -> bool:
    return name in sys.argv[1:]


def main() -> None:
    base.CANDIDATES = STAGE3_CANDIDATES
    base.fit_preprocess = fit_preprocess_stage3
    base.apply_preprocess = apply_preprocess_stage3
    if not _has_option("--output-dir"):
        sys.argv.extend(["--output-dir", ".codex_tmp/hmm_sector_factor_stage3_20260505"])
    if not _has_option("--candidates"):
        sys.argv.append("--candidates")
        sys.argv.extend(DEFAULT_STAGE3_CANDIDATES)
    base.main()


if __name__ == "__main__":
    main()
