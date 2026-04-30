#!/usr/bin/env python3
"""Train strict no-leak dynamic HMM candidates for the QE default window.

This wrapper reuses ``hmm_dynamic_offline_experiments`` but narrows the grid to
the two dynamic PUP variants selected for QE validation.  The default split is
chosen so that validation 20D forward labels end before the QE test window.
"""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import hmm_dynamic_offline_experiments as exp  # noqa: E402


exp.HORIZON_PRESETS.update({"w20_50": {5: 0.20, 10: 0.30, 20: 0.50}})

Variant = exp.VariantSpec

exp.DEFAULT_VARIANTS = [
    Variant(
        "strict_default_pup_w20_50_clip_0p9800_1p0150_conf_0p075",
        "strict no-leak PUP K3 conf 0.075 for QE default window",
        3,
        "pup",
        "w20_50",
        0.060,
        0.9800,
        1.0150,
        0.075,
        random_state=44,
    ),
    Variant(
        "strict_default_pup_w20_50_clip_0p9800_1p0150_conf_0p10",
        "strict no-leak PUP K3 conf 0.10 for QE default window",
        3,
        "pup",
        "w20_50",
        0.060,
        0.9800,
        1.0150,
        0.10,
        random_state=44,
    ),
]


if __name__ == "__main__":
    exp.main()
