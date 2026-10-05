"""Run two fresh zero-fit L2 risk consumption replays; no runtime activation."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from backend.services.hmm_risk.formal_state_effect import verify_receipt  # noqa: E402
from backend.services.hmm_risk.risk_l2_value_replay import APPROVED_PINS, VERSION, execute, require  # noqa: E402


def source_head() -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "child"))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--executor-commit")
    args = parser.parse_args(argv)
    output = None
    try:
        validated = validate_output_location(args.output)
        require(not validated.exists(), "output already exists; no overwrite", "output_collision")
        output = validated
        head = source_head()
        if args.mode == "child":
            require(
                bool(args.executor_commit) and head == args.executor_commit, "child source differs", "identity_mismatch"
            )
            result = execute(args.request, args.request_sha256)
            write_once(output, result)
            print(json.dumps({"status": result["status"], "receipt_sha256": result["receipt_sha256"], "new_fits": 0}))
        else:
            children = []
            env = {**os.environ, "PYTHONPATH": str(ROOT)}
            for key in (
                "OMP_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "MKL_NUM_THREADS",
                "VECLIB_MAXIMUM_THREADS",
                "NUMEXPR_NUM_THREADS",
            ):
                env[key] = "1"
            for index in (1, 2):
                path = output / f"process_{index}.json"
                subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "child",
                        "--request",
                        str(args.request),
                        "--request-sha256",
                        args.request_sha256,
                        "--executor-commit",
                        head,
                        "--output",
                        str(path),
                    ],
                    check=True,
                    env=env,
                    cwd=ROOT,
                )
                child = read_json(path)
                verify_receipt(child)
                require(
                    child.get("schema_version") == VERSION + "_result"
                    and child.get("request_sha256") == args.request_sha256
                    and child.get("source_pins") == APPROVED_PINS
                    and child.get("planned_return_dates") == 423
                    and child.get("sector_count") == 131
                    and len(child.get("daily", [])) == 423
                    and child.get("status")
                    in {
                        "INSUFFICIENT_REFERENCE_PATH",
                        "REFERENCE_RISK_REDUCTION_OBSERVED",
                        "REFERENCE_RISK_REDUCTION_NOT_OBSERVED",
                    }
                    and all(
                        type(child.get(k)) is int and child[k] == 0
                        for k in ("new_fits", "new_filter_calls", "new_predict_calls")
                    )
                    and all(
                        child.get(k) is False
                        for k in ("database_access", "tail_accessed", "dataset_write", "runtime_action")
                    )
                    and child.get("zero_compute_poison_active") is True,
                    "child envelope differs from the exact zero-compute request",
                    "identity_mismatch",
                )
                children.append(child)
            require(source_head() == head, "parent source changed", "identity_mismatch")
            require(
                canonical_json_bytes(children[0]) == canonical_json_bytes(children[1]),
                "fresh payloads differ",
                "repeat_mismatch",
            )
            report = receipt(
                {
                    "schema_version": VERSION + "_acceptance",
                    "execution_status": "COMPLETED",
                    "executor_commit": head,
                    "request_sha256": args.request_sha256,
                    "fresh_process_bitwise_equal": True,
                    "numeric_tolerance_used": False,
                    "planned_fits": 0,
                    "completed_fits": 0,
                    "database_access": False,
                    "tail_accessed": False,
                    "runtime_action": False,
                    "result": children[0],
                }
            )
            write_once(output / "acceptance.json", report)
            print(
                json.dumps(
                    {
                        "status": children[0]["status"],
                        "receipt_sha256": report["receipt_sha256"],
                        "fresh_process_bitwise_equal": True,
                        "new_fits": 0,
                    }
                )
            )
        return 0
    except Exception as exc:
        # Preserve a typed CLI failure, including subprocess and final write/readback failures.
        failure = receipt(
            {
                "schema_version": VERSION + "_failure",
                "execution_status": "FAILED",
                "reason_code": getattr(exc, "reason_code", "hmm_risk_l2_value_execution_failed"),
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "new_fits": 0,
                "database_access": False,
                "tail_accessed": False,
                "runtime_action": False,
            }
        )
        if output is not None:
            failure_path = output.with_name(output.name + ".failure.json")
            try:
                validate_output_location(failure_path)
                write_once(failure_path, failure)
            except Exception as write_error:
                failure = receipt(
                    {
                        **{k: v for k, v in failure.items() if k != "receipt_sha256"},
                        "failure_receipt_error": type(write_error).__name__,
                    }
                )
        print(json.dumps(failure, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
