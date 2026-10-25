"""One approved holding-reference replay, two fresh processes, no inference."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_holding_reference as study  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from scripts.hmm_risk.validate_frozen_l2_history import _head, _numeric  # noqa: E402


def repeat(request, original, signals, facts, head, environment):
    return receipt(
        {
            "schema_version": study.VERSION + "_repeat",
            "contract": study.CONTRACT,
            "request_sha256": request["receipt_sha256"],
            "original_pins": study.PINS,
            "facts_sha256": facts["receipt_sha256"],
            "executor_commit": head,
            "numeric_environment": environment,
            "fits": 0,
            "new_inference_calls": 0,
            "result": study.evaluate(request, original, signals, facts, facts["receipt_sha256"]),
        }
    )


def run(args, request, original, source, signals, head, environment):
    processes, logs = [], []
    try:
        study.require(not args.output.exists(), "output collision")
        args.output.mkdir(parents=True)
        with study.zero_compute():
            facts = study.build_facts(request, original, source)
        facts_path = args.output / "facts.json"
        write_once(facts_path, facts)
        env = {**os.environ, **{k: "1" for k in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT)}
        for n in (1, 2):
            log = (args.output / f"process_{n}.log").open("xb")
            logs.append(log)
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "child",
                "--request",
                str(args.request),
                "--request-sha256",
                request["receipt_sha256"],
                "--facts",
                str(facts_path),
                "--facts-sha256",
                facts["receipt_sha256"],
                "--executor-commit",
                head,
                "--output",
                str(args.output / f"process_{n}.json"),
            ]
            processes.append(subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT))
        codes = [process.wait() for process in processes]
        completed = sum(code == 0 for code in codes)
        study.require(completed == 2, "fresh-process scoring failed")
        paths = [args.output / f"process_{n}.json" for n in (1, 2)]
        repeats = [read_json(path) for path in paths]
        for item in repeats:
            study.verify(item)
            study.require(
                item["schema_version"] == study.VERSION + "_repeat"
                and item["contract"] == study.CONTRACT
                and item["request_sha256"] == request["receipt_sha256"]
                and item["original_pins"] == study.PINS
                and item["facts_sha256"] == facts["receipt_sha256"]
                and item["executor_commit"] == head
                and item["numeric_environment"] == environment
                and type(item["fits"]) is int
                and item["fits"] == 0
                and type(item["new_inference_calls"]) is int
                and item["new_inference_calls"] == 0,
                "child closure differs from parent",
                "identity_mismatch",
            )
        study.require(paths[0].read_bytes() == paths[1].read_bytes(), "fresh-process payloads differ")
        with study.zero_compute():
            readback = repeat(request, original, signals, facts, head, environment)
        study.require(
            canonical_json_bytes(readback) == canonical_json_bytes(repeats[0]),
            "parent arithmetic readback differs",
            "formula_invalid",
        )
        study.require(
            _head() == head
            and not subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip(),
            "executor changed during replay",
        )
        final = receipt(
            {
                "schema_version": study.VERSION + "_acceptance",
                "contract": study.CONTRACT,
                "execution_status": "COMPLETED",
                "executor_commit": head,
                "request_sha256": request["receipt_sha256"],
                "original_pins": study.PINS,
                "facts_sha256": facts["receipt_sha256"],
                "source_identity": facts["source_identity"],
                "cohort_sha256": facts["cohort_sha256"],
                "two_process_bitwise_equal": True,
                "completed_children": 2,
                "fits": 0,
                "new_inference_calls": 0,
                "result": readback["result"],
                "database_write": False,
                "dataset_write": False,
                "active_profile_write": False,
                "runtime_action": False,
            }
        )
        write_once(args.output / "acceptance.json", final)
        print(
            f"COMPLETED status={final['result']['status']} paired={final['result']['paired_return_dates']}/103 "
            f"fits=0 new_inference_calls=0 sha256={final['receipt_sha256']}"
        )
    except Exception as exc:
        # Also handle a second spawn failure: the first child may have finished.
        # Count actual successful exits, including failures before any spawn.
        exc.completed_children = sum(process.wait() == 0 for process in processes)
        raise
    finally:
        for process in processes:
            process.wait()
        for log in logs:
            log.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "child"))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--facts", type=Path)
    parser.add_argument("--facts-sha256")
    parser.add_argument("--executor-commit")
    args = parser.parse_args(argv)
    validated = False
    try:
        args.output = validate_output_location(args.output)
        validated = True
        head = _head()
        study.require(
            not subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip(),
            "formal replay requires clean committed source",
        )
        environment = _numeric("P2")
        with study.zero_compute():
            request = study.continuous.inputs._asset(args.request, args.request_sha256)
            original, source, signals = study.load_inputs(request)
            if args.mode == "child":
                study.require(
                    args.executor_commit == head and args.facts is not None and bool(args.facts_sha256),
                    "parent facts/source binding missing",
                )
                facts = study.continuous.inputs._asset(args.facts, args.facts_sha256)
                write_once(args.output, repeat(request, original, signals, facts, head, environment))
            else:
                run(args, request, original, source, signals, head, environment)
    except Exception as exc:
        path = args.output.with_name(
            args.output.name + (".parent.failure.json" if args.mode == "run" else ".failure.json")
        )
        if validated and not path.exists():
            write_once(
                path,
                receipt(
                    {
                        "schema_version": study.VERSION + "_failure",
                        "status": "FAILED",
                        "stage": args.mode,
                        "reason_code": getattr(exc, "reason_code", "hmm_risk_holding_reference_execution_failed"),
                        "exception_type": type(exc).__name__,
                        "message": str(exc),
                        "completed_children": getattr(exc, "completed_children", None),
                        "fits": 0,
                        "new_inference_calls": 0,
                        "database_write": False,
                        "dataset_write": False,
                        "runtime_action": False,
                    }
                ),
            )
        print(f"FAILED: {getattr(exc, 'reason_code', type(exc).__name__)}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
