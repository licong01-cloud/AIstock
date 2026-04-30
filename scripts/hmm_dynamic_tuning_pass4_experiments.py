#!/usr/bin/env python3
"""Fourth-pass narrow HMM tuning around the 20D-heavy PUP candidate."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import hmm_dynamic_offline_experiments as exp  # noqa: E402


exp.HORIZON_PRESETS.update(
    {
        "w20_40_a": {5: 0.25, 10: 0.35, 20: 0.40},
        "w20_40_b": {5: 0.30, 10: 0.30, 20: 0.40},
        "w20_42": {5: 0.23, 10: 0.35, 20: 0.42},
        "w20_45_a": {5: 0.25, 10: 0.30, 20: 0.45},
        "w20_45_b": {5: 0.20, 10: 0.35, 20: 0.45},
        "w20_50": {5: 0.20, 10: 0.30, 20: 0.50},
    }
)

V = exp.VariantSpec

exp.DEFAULT_VARIANTS = [
    # Reconfirm the pass-3 winner under the pass-4 output root.
    V("p4_pup_w20_40a_base", "PUP K3 20D-heavy base", 3, "pup", "w20_40_a", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p4_pup_w20_40b", "PUP K3 20D 40%, lower 10D", 3, "pup", "w20_40_b", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p4_pup_w20_42", "PUP K3 20D 42%", 3, "pup", "w20_42", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p4_pup_w20_45a", "PUP K3 20D 45%, balanced 5/10", 3, "pup", "w20_45_a", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p4_pup_w20_45b", "PUP K3 20D 45%, 10D preserved", 3, "pup", "w20_45_b", 0.060, 0.9850, 1.0150, 0.30, random_state=44),
    V("p4_pup_w20_50", "PUP K3 20D 50%", 3, "pup", "w20_50", 0.060, 0.9850, 1.0150, 0.30, random_state=44),

    # Confidence-scale and lambda around 20D-heavy.
    V("p4_pup_w20_40a_conf_0p20", "PUP K3 20D-heavy conf 0.20", 3, "pup", "w20_40_a", 0.060, 0.9850, 1.0150, 0.20, random_state=44),
    V("p4_pup_w20_40a_conf_0p25", "PUP K3 20D-heavy conf 0.25", 3, "pup", "w20_40_a", 0.060, 0.9850, 1.0150, 0.25, random_state=44),
    V("p4_pup_w20_40a_conf_0p35", "PUP K3 20D-heavy conf 0.35", 3, "pup", "w20_40_a", 0.060, 0.9850, 1.0150, 0.35, random_state=44),
    V("p4_pup_w20_40a_lambda_0p05", "PUP K3 20D-heavy lambda 0.05", 3, "pup", "w20_40_a", 0.050, 0.9850, 1.0150, 0.30, random_state=44),
    V("p4_pup_w20_40a_lambda_0p07", "PUP K3 20D-heavy lambda 0.07", 3, "pup", "w20_40_a", 0.070, 0.9850, 1.0150, 0.30, random_state=44),

    # Bound shape around 20D-heavy.
    V("p4_pup_w20_40a_clip_0p9825_1p0175", "PUP K3 20D-heavy medium-wide bounds", 3, "pup", "w20_40_a", 0.060, 0.9825, 1.0175, 0.30, random_state=44),
    V("p4_pup_w20_40a_clip_0p9875_1p0125", "PUP K3 20D-heavy medium-tight bounds", 3, "pup", "w20_40_a", 0.060, 0.9875, 1.0125, 0.30, random_state=44),
    V("p4_pup_w20_40a_clip_0p9800_1p0150", "PUP K3 20D-heavy downside-skew bounds", 3, "pup", "w20_40_a", 0.060, 0.9800, 1.0150, 0.30, random_state=44),
    V("p4_pup_w20_40a_clip_0p9850_1p0200", "PUP K3 20D-heavy upside-skew bounds", 3, "pup", "w20_40_a", 0.060, 0.9850, 1.0200, 0.30, random_state=44),
]


if __name__ == "__main__":
    exp.main()
