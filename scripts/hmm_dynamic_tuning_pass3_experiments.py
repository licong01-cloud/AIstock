#!/usr/bin/env python3
"""Third-pass narrow offline HMM tuning grid.

The grid is centered on the best second-pass direction:
3-state probability-up signal, 5D/10D/20D validation blend, and moderate
coefficient bounds. It also tests two relative probability-up mappings that
still emit ordinary multiplicative coefficient artifacts.
"""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import hmm_dynamic_offline_experiments as exp  # noqa: E402


exp.HORIZON_PRESETS.update(
    {
        "blend_balanced": {5: 0.33, 10: 0.34, 20: 0.33},
        "blend_5_30_10_40_20_30": {5: 0.30, 10: 0.40, 20: 0.30},
        "blend_5_30_10_35_20_35": {5: 0.30, 10: 0.35, 20: 0.35},
        "blend_5_25_10_35_20_40": {5: 0.25, 10: 0.35, 20: 0.40},
        "blend_5_40_10_30_20_30": {5: 0.40, 10: 0.30, 20: 0.30},
    }
)

V = exp.VariantSpec

exp.DEFAULT_VARIANTS = [
    # Bound shape around the second-pass winner.
    V("p3_pup_clip_0p9825_1p0175", "PUP K3 blend medium-wide bounds", 3, "pup", "blend_5_10_20", 0.060, 0.9825, 1.0175, 0.30, random_state=44),
    V("p3_pup_clip_0p9875_1p0125", "PUP K3 blend medium-tight bounds", 3, "pup", "blend_5_10_20", 0.060, 0.9875, 1.0125, 0.30, random_state=44),
    V("p3_pup_clip_0p9800_1p0150", "PUP K3 blend downside-skew bounds", 3, "pup", "blend_5_10_20", 0.060, 0.9800, 1.0150, 0.30, random_state=44),
    V("p3_pup_clip_0p9850_1p0200", "PUP K3 blend upside-skew bounds", 3, "pup", "blend_5_10_20", 0.060, 0.9850, 1.0200, 0.30, random_state=44),
    V("p3_pup_clip_0p9875_1p0150", "PUP K3 blend narrow downside bound", 3, "pup", "blend_5_10_20", 0.060, 0.9875, 1.0150, 0.30, random_state=44),

    # Lambda and posterior-confidence scale near the winner.
    V("p3_pup_lambda_0p05_clip_0p985_1p015", "PUP K3 lambda 0.05", 3, "pup", "blend_5_10_20", 0.050, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_lambda_0p07_clip_0p985_1p015", "PUP K3 lambda 0.07", 3, "pup", "blend_5_10_20", 0.070, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_confscale_0p20_clip_0p985_1p015", "PUP K3 confidence scale 0.20", 3, "pup", "blend_5_10_20", 0.060, 0.9850, 1.0150, 0.20, random_state=44),
    V("p3_pup_confscale_0p25_clip_0p985_1p015", "PUP K3 confidence scale 0.25", 3, "pup", "blend_5_10_20", 0.060, 0.9850, 1.0150, 0.25, random_state=44),
    V("p3_pup_confscale_0p35_clip_0p985_1p015", "PUP K3 confidence scale 0.35", 3, "pup", "blend_5_10_20", 0.060, 0.9850, 1.0150, 0.35, random_state=44),
    V("p3_pup_confscale_0p40_clip_0p985_1p015", "PUP K3 confidence scale 0.40", 3, "pup", "blend_5_10_20", 0.060, 0.9850, 1.0150, 0.40, random_state=44),

    # Horizon weight micro-tuning.
    V("p3_pup_balanced_clip_0p985_1p015", "PUP K3 balanced horizon weights", 3, "pup", "blend_balanced", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_10heavy_clip_0p985_1p015", "PUP K3 10D-heavy weights", 3, "pup", "blend_5_30_10_40_20_30", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_10_20heavy_clip_0p985_1p015", "PUP K3 10D/20D-heavy weights", 3, "pup", "blend_5_30_10_35_20_35", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_20heavy_clip_0p985_1p015", "PUP K3 20D-heavy weights", 3, "pup", "blend_5_25_10_35_20_40", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_5heavy_clip_0p985_1p015", "PUP K3 5D-heavy weights", 3, "pup", "blend_5_40_10_30_20_30", 0.060, 0.9850, 1.0150, 0.30, random_state=44),

    # Relative PUP mappings: rank/z-score sectors daily before coefficienting.
    V("p3_pup_z_lambda_0p004_clip_0p985_1p015", "PUP cross-sectional z lambda 0.004", 3, "pup_z", "blend_5_10_20", 0.004, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_z_lambda_0p006_clip_0p985_1p015", "PUP cross-sectional z lambda 0.006", 3, "pup_z", "blend_5_10_20", 0.006, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_z_lambda_0p008_clip_0p985_1p015", "PUP cross-sectional z lambda 0.008", 3, "pup_z", "blend_5_10_20", 0.008, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_rank_lambda_0p010_clip_0p985_1p015", "PUP cross-sectional rank lambda 0.010", 3, "pup_rank", "blend_5_10_20", 0.010, 0.9850, 1.0150, 0.30, random_state=44),
    V("p3_pup_rank_lambda_0p015_clip_0p985_1p015", "PUP cross-sectional rank lambda 0.015", 3, "pup_rank", "blend_5_10_20", 0.015, 0.9850, 1.0150, 0.30, random_state=44),
]


if __name__ == "__main__":
    exp.main()
