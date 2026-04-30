#!/usr/bin/env python3
"""Fifth-pass final micro grid around the 20D-50% PUP candidate."""
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
        "w20_55_a": {5: 0.15, 10: 0.30, 20: 0.55},
        "w20_55_b": {5: 0.20, 10: 0.25, 20: 0.55},
        "w20_60_a": {5: 0.15, 10: 0.25, 20: 0.60},
        "w20_60_b": {5: 0.10, 10: 0.30, 20: 0.60},
        "w20_70": {5: 0.10, 10: 0.20, 20: 0.70},
    }
)

V = exp.VariantSpec

exp.DEFAULT_VARIANTS = [
    V("p5_pup_w20_50_base", "PUP K3 20D 50% base", 3, "pup", "w20_50", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p5_pup_w20_55a", "PUP K3 20D 55%, 10D preserved", 3, "pup", "w20_55_a", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p5_pup_w20_55b", "PUP K3 20D 55%, 5D preserved", 3, "pup", "w20_55_b", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p5_pup_w20_60a", "PUP K3 20D 60%, balanced residual", 3, "pup", "w20_60_a", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p5_pup_w20_60b", "PUP K3 20D 60%, 10D residual", 3, "pup", "w20_60_b", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p5_pup_w20_70", "PUP K3 20D 70% stress test", 3, "pup", "w20_70", 0.060, 0.9850, 1.0150, 0.30, random_state=44),

    V("p5_pup_w20_50_conf_0p20", "PUP K3 20D 50% conf 0.20", 3, "pup", "w20_50", 0.060, 0.9850, 1.0150, 0.20, random_state=44),
    V("p5_pup_w20_50_conf_0p25", "PUP K3 20D 50% conf 0.25", 3, "pup", "w20_50", 0.060, 0.9850, 1.0150, 0.25, random_state=44),
    V("p5_pup_w20_50_conf_0p35", "PUP K3 20D 50% conf 0.35", 3, "pup", "w20_50", 0.060, 0.9850, 1.0150, 0.35, random_state=44),

    V("p5_pup_w20_50_lambda_0p05", "PUP K3 20D 50% lambda 0.05", 3, "pup", "w20_50", 0.050, 0.9850, 1.0150, 0.30, random_state=44),
    V("p5_pup_w20_50_lambda_0p07", "PUP K3 20D 50% lambda 0.07", 3, "pup", "w20_50", 0.070, 0.9850, 1.0150, 0.30, random_state=44),
    V("p5_pup_w20_50_clip_0p9825_1p0175", "PUP K3 20D 50% medium-wide bounds", 3, "pup", "w20_50", 0.060, 0.9825, 1.0175, 0.30, random_state=44),
    V("p5_pup_w20_50_clip_0p9800_1p0150", "PUP K3 20D 50% downside-skew bounds", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.30, random_state=44),
]


if __name__ == "__main__":
    exp.main()
