"""Current formal 5184-fit executor; no retired B3/P6/D1 CLI compatibility."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# These are applied before importing any numerical package in each fresh process.
for variable in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[variable] = "1"
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    load_request,
    receipt,
    run_two_processes,
    train_repeat,
    write_once,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("preflight", "run", "child"))
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        if args.mode == "run":
            print(run_two_processes(args.request, args.output, Path(__file__)))
        else:
            request = load_request(args.request)
            result = (
                train_repeat(request)
                if args.mode == "child"
                else receipt(
                    {
                        "status": "preflight_passed",
                        "request_sha256": request["receipt_sha256"],
                        "fits": 0,
                        "database_write": False,
                        "runtime_action": False,
                    }
                )
            )
            write_once(args.output, result)
    except Exception as exc:
        failure = args.output.with_name(args.output.name + ".failure.json")
        write_once(
            failure,
            receipt(
                {
                    "status": "failed",
                    "reason": getattr(exc, "reason_code", "hmm_risk_formal_execution_failed"),
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                    "database_write": False,
                    "runtime_action": False,
                    "ready": False,
                }
            ),
        )
        print(f"failed: {exc}; receipt={failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
