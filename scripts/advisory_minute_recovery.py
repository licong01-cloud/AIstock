"""Explicit offline minute-recovery feature preparation. No training/activation."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-study", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args()
    # These affect only this explicit offline process, before numeric imports.
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[name] = "2"
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from backend.services.advisory_model_first.minute_recovery_inputs_v1 import prepare
    result = prepare(source_study=args.source_study, output_root=args.output_root,
                     progress=lambda event: print(json.dumps(event), flush=True))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
