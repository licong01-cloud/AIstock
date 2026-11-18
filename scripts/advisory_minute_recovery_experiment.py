"""Execute the authorized single 18-fit recovery experiment; no production actions."""
import argparse
import json
import os
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("feature-root", "source-root", "output-root", "source-commit"):
        parser.add_argument("--"+name, required=True)
    args = parser.parse_args()
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[name] = "2"
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from backend.services.advisory_model_first.minute_recovery_pipeline_v1 import run
    result = run(**vars(args), progress=lambda e:print(json.dumps(e), flush=True))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
