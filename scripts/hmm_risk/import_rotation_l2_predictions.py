#!/usr/bin/env python3
"""Explicit database import for an already-closed L2 acceptance artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.services.hmm_risk.rotation_l2_prediction import RotationL2PredictionRepository, rows_from_acceptance


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acceptance", type=Path, required=True)
    args = parser.parse_args()
    value = json.loads(args.acceptance.read_text(encoding="utf-8"))
    result = RotationL2PredictionRepository().write_rows(rows_from_acceptance(value))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
