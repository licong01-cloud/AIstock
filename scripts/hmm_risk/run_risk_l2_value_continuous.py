"""Run approved v18 continuous-budget research: two fresh processes, zero fits."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_value_continuous as study  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from scripts.hmm_risk.validate_frozen_l2_history import _head, _numeric  # noqa: E402


def failure(path, exc):
    write_once(
        path,
        receipt(
            {
                "schema_version": study.VERSION + "_failure",
                "status": "FAILED",
                "reason_code": getattr(exc, "reason_code", "hmm_risk_continuous_execution_failed"),
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "fits": 0,
                "database_write": False,
                "dataset_write": False,
                "runtime_action": False,
            }
        ),
    )


def wait_for(paths, failure_path, processes=()):
    deadline = time.monotonic() + 3600
    while not all(p.is_file() for p in paths):
        study.require(not failure_path.is_file(), "parent failed before outcome release")
        study.require(all(p.poll() is None for p in processes), "child failed before seal")
        study.require(time.monotonic() < deadline, "peer did not release required files")
        time.sleep(0.1)


def child(args, request, bundle):
    study.require(_head() == args.executor_commit == bundle["source_commit"], "child executor identity differs")
    environment = _numeric("P2")
    study.require(environment == bundle["numeric_environment"], "numeric environment differs")
    with study.history.no_training_or_external_actions():
        sealed = study.infer(bundle)
        study.validate_sealed(bundle, sealed)
        sealed_path = args.output.with_suffix(".sealed.json")
        write_once(sealed_path, sealed)
        write_once(args.output.with_suffix(".ready.json"), {"sealed_sha256": sealed["receipt_sha256"]})
        wait_for([args.outcomes.with_suffix(".ready.json")], args.outcomes.with_name("parent.failure.json"))
        facts = read_json(args.outcomes)
        study.verify_receipt(facts)
        study.require(
            read_json(args.outcomes.with_suffix(".ready.json")) == {"outcome_sha256": facts["receipt_sha256"]}
            and facts["sealed_prediction_sha256"] == sealed["receipt_sha256"],
            "outcome seal binding differs",
        )
        result = study.evaluate(bundle, sealed, facts)
    write_once(
        args.output,
        receipt(
            {
                "schema_version": study.VERSION + "_repeat",
                "contract": study.CONTRACT,
                "request_sha256": request["receipt_sha256"],
                "features_sha256": bundle["receipt_sha256"],
                "sealed_sha256": sealed["receipt_sha256"],
                "outcome_sha256": facts["receipt_sha256"],
                "executor_commit": args.executor_commit,
                "numeric_environment": environment,
                "fits": 0,
                "result": result,
            }
        ),
    )


def run(args, request, bundle):
    head = _head()
    study.require(head == bundle["source_commit"], "feature/executor source differs")
    study.require(not args.output.exists(), "output collision")
    args.output.mkdir(parents=True)
    env = {**os.environ, **{k: "1" for k in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT)}
    processes, logs = [], []
    facts_path = args.output / "outcomes.json"
    parent_failure = args.output / "parent.failure.json"
    try:
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
                "--features",
                str(args.features),
                "--feature-sha256",
                bundle["receipt_sha256"],
                "--executor-commit",
                head,
                "--outcomes",
                str(facts_path),
                "--output",
                str(args.output / f"process_{n}.json"),
            ]
            processes.append(subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT))
        ready = [args.output / f"process_{n}.ready.json" for n in (1, 2)]
        wait_for(ready, parent_failure, processes)
        sealed = [read_json(args.output / f"process_{n}.sealed.json") for n in (1, 2)]
        for item, path in zip(sealed, ready, strict=True):
            study.validate_sealed(bundle, item)
            study.require(read_json(path) == {"sealed_sha256": item["receipt_sha256"]}, "child ready linkage differs")
        study.require(
            canonical_json_bytes(sealed[0]) == canonical_json_bytes(sealed[1]), "fresh inference payload differs"
        )
        with study.history.no_training_or_external_actions():
            facts = study.outcomes(request, bundle)
        facts = receipt(
            {
                **{k: v for k, v in facts.items() if k != "receipt_sha256"},
                "sealed_prediction_sha256": sealed[0]["receipt_sha256"],
            }
        )
        write_once(facts_path, facts)
        write_once(facts_path.with_suffix(".ready.json"), {"outcome_sha256": facts["receipt_sha256"]})
        study.require(all(p.wait() == 0 for p in processes), "child comparison failed")
        repeats = [read_json(args.output / f"process_{n}.json") for n in (1, 2)]
        for item in repeats:
            study.verify_receipt(item)
            study.require(
                item["schema_version"] == study.VERSION + "_repeat"
                and item["contract"] == study.CONTRACT
                and item["request_sha256"] == request["receipt_sha256"]
                and item["features_sha256"] == bundle["receipt_sha256"]
                and item["sealed_sha256"] == sealed[0]["receipt_sha256"]
                and item["outcome_sha256"] == facts["receipt_sha256"]
                and item["executor_commit"] == head
                and item["numeric_environment"] == bundle["numeric_environment"]
                and type(item["fits"]) is int
                and item["fits"] == 0,
                "child result authority differs",
                "identity_mismatch",
            )
            study.require(
                item["result"]["prediction_sha256"] == sealed[0]["receipt_sha256"]
                and item["result"]["outcome_sha256"] == facts["receipt_sha256"],
                "result source linkage differs",
            )
        study.require(
            canonical_json_bytes(repeats[0]) == canonical_json_bytes(repeats[1]), "fresh business payload differs"
        )
        study.require(
            canonical_json_bytes(repeats[0]["result"])
            == canonical_json_bytes(study.evaluate(bundle, sealed[0], facts)),
            "parent business readback differs",
        )
        final = receipt(
            {
                "schema_version": study.VERSION + "_acceptance",
                "contract": study.CONTRACT,
                "execution_status": "COMPLETED",
                "executor_commit": head,
                "request_sha256": request["receipt_sha256"],
                "feature_sha256": bundle["receipt_sha256"],
                "source_identity": bundle["source_identity"],
                "model_sha256": study.CONTRACT["model_sha256"],
                "two_process_bitwise_equal": True,
                "fits": 0,
                "result": repeats[0]["result"],
                "database_write": False,
                "dataset_write": False,
                "runtime_action": False,
            }
        )
        write_once(args.output / "acceptance.json", final)
        print(
            f"completed; fits=0; two_process_equal=true; status={final['result']['status']}; {args.output / 'acceptance.json'}"
        )
    except Exception as exc:
        if not parent_failure.exists():
            failure(parent_failure, exc)
        raise
    finally:
        # Own children observe parent.failure and exit; no user process control.
        for process in processes:
            process.wait()
        for log in logs:
            log.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run", "child"))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--features", type=Path)
    parser.add_argument("--feature-sha256")
    parser.add_argument("--executor-commit")
    parser.add_argument("--outcomes", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output_validated = False
    try:
        args.output = validate_output_location(args.output)
        output_validated = True
        request = study.inputs._asset(args.request, args.request_sha256)
        study.validate_request(request)
        study.require(
            not subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip(),
            "formal study requires clean committed source",
        )
        if args.mode == "prepare":
            study.require(not args.output.exists(), "prepared output collision")
            environment = _numeric("P2")
            with study.history.no_training_or_external_actions():
                bundle = study.prepare(request, _head())
            bundle = receipt(
                {**{k: v for k, v in bundle.items() if k != "receipt_sha256"}, "numeric_environment": environment}
            )
            write_once(args.output, bundle)
            print(f"v18 prepared; fits=0; hash={bundle['receipt_sha256']}")
        else:
            study.require(args.features is not None and bool(args.feature_sha256), "explicit pinned features required")
            bundle = study.inputs._asset(args.features, args.feature_sha256)
            study.validate_bundle(request, bundle)
            if args.mode == "child":
                study.require(args.outcomes is not None and args.executor_commit is not None, "parent binding missing")
                child(args, request, bundle)
            else:
                run(args, request, bundle)
    except Exception as exc:
        path = args.output.with_name(args.output.name + ".failure.json")
        if output_validated and not path.exists():
            failure(path, exc)
        print(f"FAILED: {getattr(exc, 'reason_code', type(exc).__name__)}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
