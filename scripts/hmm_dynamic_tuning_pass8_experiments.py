#!/usr/bin/env python3
"""Eighth-pass stop grid: check whether confidence scale below 0.10 helps."""
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
    V("p8_pup_w20_50_clip_0p9800_1p0150_conf_0p05", "PUP K3 final conf 0.05", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.05, random_state=44),
    V("p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075", "PUP K3 final conf 0.075", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.075, random_state=44),
    V("p8_pup_w20_50_clip_0p9800_1p0150_conf_0p10", "PUP K3 final conf 0.10", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.10, random_state=44),
    V("p8_pup_w20_50_clip_0p9800_1p0150_conf_0p12", "PUP K3 final conf 0.12", 3, "pup", "w20_50", 0.060, 0.9800, 1.0150, 0.12, random_state=44),
    V("p8_pup_w20_50_clip_0p9775_1p0150_conf_0p10", "PUP K3 final 0.9775/1.015 conf 0.10", 3, "pup", "w20_50", 0.060, 0.9775, 1.0150, 0.10, random_state=44),
    V("p8_pup_w20_50_clip_0p9750_1p0150_conf_0p10", "PUP K3 final 0.975/1.015 conf 0.10", 3, "pup", "w20_50", 0.060, 0.9750, 1.0150, 0.10, random_state=44),
    V("p8_pup_w20_50_clip_0p9800_1p0175_conf_0p10", "PUP K3 final 0.98/1.0175 conf 0.10", 3, "pup", "w20_50", 0.060, 0.9800, 1.0175, 0.10, random_state=44),
    V("p8_pup_w20_50_clip_0p9800_1p0150_conf_0p10_lambda_0p07", "PUP K3 final lambda 0.07 conf 0.10", 3, "pup", "w20_50", 0.070, 0.9800, 1.0150, 0.10, random_state=44),
]


if __name__ == "__main__":
    exp.main()
