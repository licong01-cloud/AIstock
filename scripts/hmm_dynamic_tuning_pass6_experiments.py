#!/usr/bin/env python3
"""Sixth-pass final check around the downside-skewed 20D-50% PUP candidate."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import hmm_dynamic_offline_experiments as exp  # noqa: E402


exp.HORIZON_PRESETS.update(
    {
        "w20_50": {5: 0.20, 10: 0.30, 20: 0.50},
        "w20_50_10res": {5: 0.15, 10: 0.35, 20: 0.50},
        "w20_50_5res": {5: 0.25, 10: 0.25, 20: 0.50},
        "w20_45_a": {5: 0.25, 10: 0.30, 20: 0.45},
        "w20_60_a": {5: 0.15, 10: 0.25, 20: 0.60},
    }
)

V = exp.VariantSpec

exp.DEFAULT_VARIANTS = [
    V("p6_pup_w20_50_clip_0p9800_1p0150_base", "PUP K3 20D 50% downside-skew base", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.30, random_state=44),
    V("p6_pup_w20_50_clip_0p9750_1p0150", "PUP K3 20D 50% stronger downside", 3, "pup", "w20_50", 0.060, 0.9750, 1.0150, 0.30, random_state=44),
    V("p6_pup_w20_50_clip_0p9775_1p0150", "PUP K3 20D 50% medium downside", 3, "pup", "w20_50", 0.060, 0.9775, 1.0150, 0.30, random_state=44),
    V("p6_pup_w20_50_clip_0p9825_1p0150", "PUP K3 20D 50% lighter downside", 3, "pup", "w20_50", 0.060, 0.9825, 1.0150, 0.30, random_state=44),
    V("p6_pup_w20_50_clip_0p9800_1p0125", "PUP K3 20D 50% lower upside", 3, "pup", "w20_50", 0.060, 0.9800, 1.0125, 0.30, random_state=44),
    V("p6_pup_w20_50_clip_0p9800_1p0175", "PUP K3 20D 50% higher upside", 3, "pup", "w20_50", 0.060, 0.9800, 1.0175, 0.30, random_state=44),
    V("p6_pup_w20_50_clip_0p9750_1p0125", "PUP K3 20D 50% stronger downside lower upside", 3, "pup", "w20_50", 0.060, 0.9750, 1.0125, 0.30, random_state=44),

    V("p6_pup_w20_50_clip_0p9800_1p0150_conf_0p20", "PUP K3 downside-skew conf 0.20", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.20, random_state=44),
    V("p6_pup_w20_50_clip_0p9800_1p0150_conf_0p25", "PUP K3 downside-skew conf 0.25", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.25, random_state=44),
    V("p6_pup_w20_50_clip_0p9800_1p0150_conf_0p35", "PUP K3 downside-skew conf 0.35", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.35, random_state=44),
    V("p6_pup_w20_50_clip_0p9800_1p0150_lambda_0p05", "PUP K3 downside-skew lambda 0.05", 3, "pup", "w20_50", 0.050, 0.9800, 1.0150, 0.30, random_state=44),
    V("p6_pup_w20_50_clip_0p9800_1p0150_lambda_0p07", "PUP K3 downside-skew lambda 0.07", 3, "pup", "w20_50", 0.070, 0.9800, 1.0150, 0.30, random_state=44),

    V("p6_pup_w20_50_10res_clip_0p9800_1p0150", "PUP K3 20D 50%, more 10D residual", 3, "pup", "w20_50_10res", 0.060, 0.9800, 1.0150, 0.30, random_state=44),
    V("p6_pup_w20_50_5res_clip_0p9800_1p0150", "PUP K3 20D 50%, more 5D residual", 3, "pup", "w20_50_5res", 0.060, 0.9800, 1.0150, 0.30, random_state=44),
    V("p6_pup_w20_45a_clip_0p9800_1p0150", "PUP K3 20D 45% downside-skew", 3, "pup", "w20_45_a", 0.060, 0.9800, 1.0150, 0.30, random_state=44),
    V("p6_pup_w20_60a_clip_0p9800_1p0150", "PUP K3 20D 60% downside-skew", 3, "pup", "w20_60_a", 0.060, 0.9800, 1.0150, 0.30, random_state=44),
]


if __name__ == "__main__":
    exp.main()
