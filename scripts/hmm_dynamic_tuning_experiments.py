#!/usr/bin/env python3
"""Second-pass offline HMM tuning grid.

This reuses the isolated offline runner and only changes the in-memory variant
list. It does not write DB model_train_* rows.
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
        "blend_5heavy": {5: 0.45, 10: 0.35, 20: 0.20},
        "blend_10heavy_light20": {5: 0.35, 10: 0.45, 20: 0.20},
        "blend_light20": {5: 0.40, 10: 0.40, 20: 0.20},
        "ten_heavy": {10: 0.70, 20: 0.30},
    }
)

V = exp.VariantSpec

exp.DEFAULT_VARIANTS = [
    # Probability-up K3: strongest first-pass direction.
    V("pup_blend_k3_clip_0p99_1p01", "PUP blend K3 tighter clip", 3, "pup", "blend_5_10_20", 0.060, 0.99, 1.01, 0.30, random_state=44),
    V("pup_blend_k3_clip_0p985_1p015", "PUP blend K3 medium-tight clip", 3, "pup", "blend_5_10_20", 0.060, 0.985, 1.015, 0.30, random_state=44),
    V("pup_blend_k3_lambda_0p04", "PUP blend K3 lower lambda", 3, "pup", "blend_5_10_20", 0.040, 0.98, 1.02, 0.30, random_state=44),
    V("pup_blend_k3_lambda_0p08", "PUP blend K3 higher lambda", 3, "pup", "blend_5_10_20", 0.080, 0.98, 1.02, 0.30, random_state=44),
    V("pup_blend_k3_neutral_0p02", "PUP blend K3 weak-signal neutral band 0.02", 3, "pup", "blend_5_10_20", 0.060, 0.98, 1.02, 0.30, random_state=44, neutral_band=0.02),
    V("pup_blend_k3_neutral_0p03", "PUP blend K3 weak-signal neutral band 0.03", 3, "pup", "blend_5_10_20", 0.060, 0.98, 1.02, 0.30, random_state=44, neutral_band=0.03),
    V("pup_blend_k3_conf_floor_0p20", "PUP blend K3 confidence floor 0.20", 3, "pup", "blend_5_10_20", 0.060, 0.98, 1.02, 0.30, random_state=44, confidence_floor=0.20),
    V("pup_blend_k3_conf_floor_0p30", "PUP blend K3 confidence floor 0.30", 3, "pup", "blend_5_10_20", 0.060, 0.98, 1.02, 0.30, random_state=44, confidence_floor=0.30),
    V("pup_blend_k3_neutral_0p03_conf_0p20", "PUP blend K3 neutral band + confidence floor", 3, "pup", "blend_5_10_20", 0.060, 0.98, 1.02, 0.30, random_state=44, neutral_band=0.03, confidence_floor=0.20),
    V("pup_5heavy_k3_clip_0p98_1p02", "PUP K3 5D-heavy blend", 3, "pup", "blend_5heavy", 0.060, 0.98, 1.02, 0.30, random_state=44),
    V("pup_light20_k3_clip_0p98_1p02", "PUP K3 5D/10D with light 20D", 3, "pup", "blend_light20", 0.060, 0.98, 1.02, 0.30, random_state=44),
    V("pup_10heavy_light20_k3_clip_0p98_1p02", "PUP K3 10D-heavy light 20D", 3, "pup", "blend_10heavy_light20", 0.060, 0.98, 1.02, 0.30, random_state=44),
    V("pup_blend_k3_seed77", "PUP blend K3 alternate seed 77", 3, "pup", "blend_5_10_20", 0.060, 0.98, 1.02, 0.30, random_state=77),

    # Additive research variants, script-only.
    V("additive_pup_blend_k3_beta_0p01", "Additive PUP beta 0.01", 3, "additive_pup", "blend_5_10_20", 0.0, 1.0, 1.0, 0.30, additive_beta=0.01, random_state=48),
    V("additive_pup_blend_k3_beta_0p02", "Additive PUP beta 0.02", 3, "additive_pup", "blend_5_10_20", 0.0, 1.0, 1.0, 0.30, additive_beta=0.02, random_state=48),
    V("additive_pup_blend_k3_beta_0p02_neutral_0p03", "Additive PUP beta 0.02 with neutral band", 3, "additive_pup", "blend_5_10_20", 0.0, 1.0, 1.0, 0.30, additive_beta=0.02, random_state=48, neutral_band=0.03),

    # Expected-return conservative repairs.
    V("er_winsor_10_20_k3_lambda_0p006_clip_0p99_1p01", "ER winsor 10/20 conservative", 3, "er_winsor", "blend_10_20", 0.006, 0.99, 1.01, 0.30, random_state=45),
    V("er_median_10_20_k3_lambda_0p006_clip_0p99_1p01", "ER median 10/20 conservative", 3, "er_median", "blend_10_20", 0.006, 0.99, 1.01, 0.30, random_state=45),
    V("er_winsor_blend_k3_neutral_0p25_clip_0p99_1p01", "ER winsor blend with z neutral band", 3, "er_winsor", "blend_5_10_20", 0.006, 0.99, 1.01, 0.30, random_state=43, neutral_band=0.25),

    # Conservative checks for lower-priority first-pass directions.
    V("pup_10heavy_k3_clip_0p99_1p01_neutral_0p05_conf_0p20", "PUP 10-heavy conservative", 3, "pup", "ten_heavy", 0.060, 0.99, 1.01, 0.30, random_state=46, neutral_band=0.05, confidence_floor=0.20),
    V("pup_blend_k4_clip_0p995_1p005_neutral_0p05_conf_0p30", "K4 PUP extremely conservative", 4, "pup", "blend_5_10_20", 0.060, 0.995, 1.005, 0.25, random_state=47, neutral_band=0.05, confidence_floor=0.30),
]


if __name__ == "__main__":
    exp.main()
