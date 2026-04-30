#!/usr/bin/env python3
"""Seventh-pass final confirmation grid around confidence scale 0.20."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import hmm_dynamic_offline_experiments as exp  # noqa: E402


exp.HORIZON_PRESETS.update({"w20_50": {5: 0.20, 10: 0.30, 20: 0.50}})

V = exp.VariantSpec

exp.DEFAULT_VARIANTS = [
    V("p7_pup_w20_50_clip_0p9800_1p0150_conf_0p10", "PUP K3 final conf 0.10", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.10, random_state=44),
    V("p7_pup_w20_50_clip_0p9800_1p0150_conf_0p15", "PUP K3 final conf 0.15", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.15, random_state=44),
    V("p7_pup_w20_50_clip_0p9800_1p0150_conf_0p18", "PUP K3 final conf 0.18", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.18, random_state=44),
    V("p7_pup_w20_50_clip_0p9800_1p0150_conf_0p20", "PUP K3 final conf 0.20", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.20, random_state=44),
    V("p7_pup_w20_50_clip_0p9800_1p0150_conf_0p22", "PUP K3 final conf 0.22", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.22, random_state=44),
    V("p7_pup_w20_50_clip_0p9800_1p0150_conf_0p25", "PUP K3 final conf 0.25", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.25, random_state=44),

    V("p7_pup_w20_50_clip_0p9775_1p0150_conf_0p20", "PUP K3 final stronger downside conf 0.20", 3, "pup", "w20_50", 0.060, 0.9775, 1.0150, 0.20, random_state=44),
    V("p7_pup_w20_50_clip_0p9825_1p0150_conf_0p20", "PUP K3 final lighter downside conf 0.20", 3, "pup", "w20_50", 0.060, 0.9825, 1.0150, 0.20, random_state=44),
    V("p7_pup_w20_50_clip_0p9800_1p0125_conf_0p20", "PUP K3 final lower upside conf 0.20", 3, "pup", "w20_50", 0.060, 0.9800, 1.0125, 0.20, random_state=44),
    V("p7_pup_w20_50_clip_0p9800_1p0175_conf_0p20", "PUP K3 final higher upside conf 0.20", 3, "pup", "w20_50", 0.060, 0.9800, 1.0175, 0.20, random_state=44),
    V("p7_pup_w20_50_clip_0p9800_1p0150_conf_0p20_lambda_0p05", "PUP K3 final lambda 0.05 conf 0.20", 3, "pup", "w20_50", 0.050, 0.9800, 1.0150, 0.20, random_state=44),
    V("p7_pup_w20_50_clip_0p9800_1p0150_conf_0p20_lambda_0p07", "PUP K3 final lambda 0.07 conf 0.20", 3, "pup", "w20_50", 0.070, 0.9800, 1.0150, 0.20, random_state=44),
]


if __name__ == "__main__":
    exp.main()
