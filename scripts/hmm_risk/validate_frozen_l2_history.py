"""Fixed P1→P2 offline history validation; two children, zero fits, no services."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import frozen_l2_history as history  # noqa: E402
from backend.services.hmm_risk import frozen_l2_history_input as source  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_effect import verify_receipt  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    numeric_environment,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402


def _head():
    return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()


def _failure(output, exc):
    result = receipt(
        {
            "schema_version": history.VERSION + "_failure",
            "status": "FAILED",
            "reason_code": getattr(exc, "reason_code", "hmm_risk_frozen_history_execution_failed"),
            "exception_type": type(exc).__name__,
            "message": str(exc),
            "fits": 0,
            "database_write": False,
            "runtime_action": False,
        }
    )
    write_once(output, result)


def _numeric(kind):
    # Match the original fixed numerical libraries; import is not a fit.
    import hmmlearn.hmm  # noqa: F401

    environment = numeric_environment()
    result = {k: environment[k] for k in ("versions", "thread_variables", "thread_pools")}
    result["thread_pools"] = [{k: v for k, v in p.items() if k != "filepath"} for p in result["thread_pools"]]
    history.require(
        sys.version.split()[0] == "3.13.5" and all(p["num_threads"] == 1 for p in result["thread_pools"]),
        "approved single-thread Python environment differs",
    )
    expected = {
        "numpy": "2.3.3",
        "scipy": "1.16.3",
        "scikit-learn": "1.8.0",
        "threadpoolctl": "3.6.0",
        "hmmlearn": "0.3.3",
    }
    history.require(
        all(result["versions"].get(k) == v for k, v in expected.items()), "approved dependency versions differ"
    )
    history.require(all(os.environ.get(k) == "1" for k in THREAD_VARIABLES), "approved thread variables differ")
    return result


def child(args):
    bundle = read_json(args.features)
    verify_receipt(bundle, args.feature_sha256)
    history.require(_head() == args.executor_commit == bundle["source_commit"], "child source commit differs")
    environment = _numeric(args.kind)
    history.require(environment == bundle["numeric_environment"], "inference and feature numeric environments differ")
    with history.no_training_or_external_actions():
        sealed = (
            history.infer_rotation(bundle, bundle["parameters"])
            if args.kind == "P1"
            else history.infer_risk(bundle, bundle["parameters"])
        )
        sealed_path = args.output.with_suffix(".sealed.json")
        write_once(sealed_path, sealed)
        history.require(
            canonical_json_bytes(read_json(sealed_path)) == canonical_json_bytes(sealed), "sealed readback differs"
        )
        write_once(args.output.with_suffix(".ready.json"), {"sealed_sha256": sealed["receipt_sha256"]})
        # Both children seal before the parent constructs a single outcome view.
        deadline = time.monotonic() + 3600
        while not args.outcomes.with_suffix(".ready.json").is_file():
            if args.outcomes.with_name("parent.failure.json").is_file():
                raise history.risk.fail("parent failed before outcome release", "parent_failed")
            if time.monotonic() >= deadline:
                raise history.risk.fail("parent did not release outcome view", "parent_timeout")
            time.sleep(0.1)
        facts = read_json(args.outcomes)
        verify_receipt(facts)
        history.require(
            read_json(args.outcomes.with_suffix(".ready.json")) == {"outcome_sha256": facts["receipt_sha256"]},
            "outcome ready binding differs",
        )
        history.require(
            facts["sealed_prediction_sha256"] == sealed["receipt_sha256"], "parent prediction/outcome closure differs"
        )
        result = (
            history.evaluate_rotation(bundle, sealed, facts)
            if args.kind == "P1"
            else history.evaluate_risk(bundle, sealed, facts)
        )
        write_once(
            args.output,
            receipt(
                {
                    "schema_version": history.VERSION + "_repeat",
                    "kind": args.kind,
                    "numeric_environment": environment,
                    "sealed_sha256": sealed["receipt_sha256"],
                    "result": result,
                    "executor_commit": args.executor_commit,
                    "fits": 0,
                }
            ),
        )


def run(args, request):
    bundle = read_json(args.features)
    verify_receipt(bundle, args.feature_sha256)
    head = _head()
    history.require(head == bundle["source_commit"], "input and executor source differ")
    history.require(
        bundle["request_sha256"] == request["receipt_sha256"] and bundle["contract"] == history.CONTRACT,
        "feature request lineage differs",
    )
    history.require(
        not subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip(),
        "formal history requires clean committed source",
    )
    history.require(not args.output.exists(), "output collision")
    args.output.mkdir(parents=True)
    env = {**os.environ, **{k: "1" for k in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT)}
    processes, logs = [], []
    facts_path = args.output / "outcomes.json"
    try:
        for n in (1, 2):
            path = args.output / f"process_{n}.json"
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
                "--kind",
                request["kind"],
                "--outcomes",
                str(facts_path),
                "--output",
                str(path),
                "--executor-commit",
                head,
            ]
            processes.append(subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT))
        sealed_paths = [args.output / f"process_{n}.sealed.json" for n in (1, 2)]
        ready_paths = [args.output / f"process_{n}.ready.json" for n in (1, 2)]
        while not all(p.is_file() for p in ready_paths):
            history.require(all(p.poll() is None for p in processes), "child failed before prediction seal")
            time.sleep(0.1)
        sealed = [read_json(p) for p in sealed_paths]
        for item, path in zip(sealed, ready_paths, strict=True):
            verify_receipt(item)
            history.require(
                read_json(path) == {"sealed_sha256": item["receipt_sha256"]}, "prediction ready binding differs"
            )
        history.require(
            canonical_json_bytes(sealed[0]) == canonical_json_bytes(sealed[1]), "two inference processes differ"
        )
        with history.no_training_or_external_actions():
            facts = source.outcomes(request, bundle)
        facts = receipt(
            {
                **{k: v for k, v in facts.items() if k != "receipt_sha256"},
                "sealed_prediction_sha256": sealed[0]["receipt_sha256"],
            }
        )
        write_once(facts_path, facts)
        history.require(
            canonical_json_bytes(read_json(facts_path)) == canonical_json_bytes(facts), "outcome readback differs"
        )
        write_once(facts_path.with_suffix(".ready.json"), {"outcome_sha256": facts["receipt_sha256"]})
        history.require(all(p.wait() == 0 for p in processes), "child evaluation failed")
        repeats = [read_json(args.output / f"process_{n}.json") for n in (1, 2)]
        for item in repeats:
            verify_receipt(item)
            history.require(
                item["schema_version"] == history.VERSION + "_repeat"
                and item["kind"] == request["kind"]
                and item["executor_commit"] == head
                and item["numeric_environment"] == bundle["numeric_environment"]
                and type(item["fits"]) is int
                and item["fits"] == 0
                and item["sealed_sha256"] == sealed[0]["receipt_sha256"]
                and item["result"]["prediction_sha256"] == sealed[0]["receipt_sha256"]
                and item["result"]["outcome_sha256"] == facts["receipt_sha256"],
                "child result does not close against parent authorities",
            )
        history.require(
            canonical_json_bytes(repeats[0]) == canonical_json_bytes(repeats[1]), "fresh business payloads differ"
        )
        final = receipt(
            {
                "schema_version": history.VERSION + "_acceptance",
                "kind": request["kind"],
                "contract": history.CONTRACT,
                "request_sha256": request["receipt_sha256"],
                "features_sha256": bundle["receipt_sha256"],
                "executor_commit": head,
                "two_process_bitwise_equal": True,
                "fits": 0,
                "result": repeats[0]["result"],
                "numeric_environment": repeats[0]["numeric_environment"],
                "database_write": False,
                "dataset_write": False,
                "runtime_action": False,
            }
        )
        write_once(args.output / "acceptance.json", final)
        history.require(
            canonical_json_bytes(read_json(args.output / "acceptance.json")) == canonical_json_bytes(final),
            "acceptance readback differs",
        )
        print(f"{request['kind']} history completed; fits=0; two-process equal; {args.output / 'acceptance.json'}")
    except Exception as exc:
        _failure(args.output / "parent.failure.json", exc)
        raise
    finally:
        # Own children see the parent failure receipt and exit; never control user processes.
        for p in processes:
            p.wait()
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run", "child"))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--features", type=Path)
    parser.add_argument("--feature-sha256")
    parser.add_argument("--kind", choices=("P1", "P2"))
    parser.add_argument("--outcomes", type=Path)
    parser.add_argument("--executor-commit")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output_validated = False
    try:
        args.output = validate_output_location(args.output)
        output_validated = True
        request = source._asset(args.request, args.request_sha256)
        history.require(
            request.get("schema_version") == history.VERSION + "_request"
            and request.get("contract") == history.CONTRACT,
            "request contract differs",
        )
        if args.mode == "prepare":
            history.require(not args.output.exists(), "prepared output already exists")
            history.require(
                not subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip(),
                "formal preparation requires clean committed source",
            )
            with history.no_training_or_external_actions():
                environment = _numeric(request["kind"])
                bundle = source.prepare(request, _head())
            bundle = receipt(
                {**{k: v for k, v in bundle.items() if k != "receipt_sha256"}, "numeric_environment": environment}
            )
            write_once(args.output, bundle)
            print(f"{request['kind']} features prepared; fits=0; hash={bundle['receipt_sha256']}")
        else:
            history.require(
                args.features is not None and bool(args.feature_sha256), "explicit pinned features required"
            )
            if args.mode == "child":
                history.require(
                    args.kind == request["kind"] and args.outcomes is not None and args.executor_commit is not None,
                    "child parent binding differs",
                )
                child(args)
            else:
                run(args, request)
    except Exception as exc:
        path = args.output.with_name(args.output.name + ".failure.json")
        if output_validated and not path.exists():
            _failure(path, exc)
        print(f"FAILED: {getattr(exc, 'reason_code', type(exc).__name__)}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
