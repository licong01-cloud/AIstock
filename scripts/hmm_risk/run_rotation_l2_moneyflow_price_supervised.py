"""Approved four-feature version, sharing the existing guarded two-child CLI."""

from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# Numerical process settings, never experiment-record or runtime bindings.
for key in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
):
    os.environ[key] = "1"

from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as model  # noqa: E402
from scripts.hmm_risk.run_rotation_l2_moneyflow_supervised import main as guarded_main  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    return guarded_main(argv, engine=model, child_script=Path(__file__).resolve())


if __name__ == "__main__":
    raise SystemExit(main())
