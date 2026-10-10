"""Bounded monthly L2 research. Explicit request, immutable source, no services."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
for key in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
):
    os.environ[key] = "1"

from backend.services.hmm_risk import rotation_l2_rolling_return as model  # noqa: E402
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_effect import verify_receipt  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from backend.services.hmm_risk.frozen_l2_history import no_training_or_external_actions  # noqa: E402


def _head():
    return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()


def _request(args):
    path = args.request
    model.require(
        path.is_absolute()
        and path.is_file()
        and not any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (path, *path.parents)),
        "request is absent, relative or indirect",
    )
    before = path.stat()
    value = read_json(path)
    verify_receipt(value, args.request_sha256)
    model.require(
        (before.st_size, before.st_mtime_ns) == (path.stat().st_size, path.stat().st_mtime_ns),
        "request changed while read",
    )
    model.require(
        set(value)
        == {
            "schema_version",
            "contract",
            "source_history_request_path",
            "fixed_acceptance_path",
            "synthetic_features_path",
            "synthetic_outcomes_path",
            "synthetic_acceptance_path",
            "receipt_sha256",
        }
        and value["schema_version"] == model.VERSION + "_request"
        and value["contract"] == model.CONTRACT,
        "request contract or field set differs",
    )
    return value


def _source(args):
    model.require(_head() == args.executor_commit, "executor commit changed")
    model.require(
        not subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip(),
        "formal experiment requires clean committed source",
    )


def _input(args, request):
    model.require(args.input.is_absolute() and args.input.is_file(), "input must be an absolute file")
    model.require(
        not any(
            p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (args.input, *args.input.parents)
        ),
        "input has an indirect ancestor",
    )
    before = args.input.stat()
    value = read_json(args.input)
    ridge.verify(value, "input_hash")
    model.require(
        value["input_hash"] == args.input_sha256
        and value["source_commit"] == args.executor_commit
        and value["request_sha256"] == request["receipt_sha256"],
        "input authority differs",
    )
    model.require(
        (before.st_size, before.st_mtime_ns) == (args.input.stat().st_size, args.input.stat().st_mtime_ns),
        "input changed while read",
    )
    model.validate_input(value)
    return value


def _failure(path, exc, counters):
    payload = receipt(
        {
            "schema_version": model.VERSION + "_failure",
            "status": "FAILED",
            "reason_code": getattr(exc, "reason_code", "hmm_risk_rotation_l2_rolling_execution_failed"),
            "exception_type": type(exc).__name__,
            "message": str(exc),
            **counters,
            "failed_fits": (
                counters["started_fits"] - counters["completed_fits"]
                if all(type(v) is int for v in counters.values())
                else None
            ),
            "database_write": False,
            "dataset_write": False,
            "runtime_action": False,
        }
    )
    write_once(path, payload)


def _child(args, request, counters):
    bundle = _input(args, request)

    def progress(stage, number, origin):
        counters[stage + "_fits"] += 1
        print(f"process={args.process_index} month={origin} {stage}={number}/5", flush=True)

    from sklearn.linear_model import Ridge

    # The pure candidate needs Ridge.fit, not any old HMM/database/source action.
    fit = Ridge.fit
    with no_training_or_external_actions(), patch("sklearn.linear_model.Ridge.fit", fit):
        result = model.run_process(bundle, process_index=args.process_index, progress=progress)
    _source(args)
    write_once(args.output, result)


def _run(args, request, counters):
    bundle = _input(args, request)
    model.require(not args.output.exists(), "run output already exists")
    args.output.mkdir(parents=True)
    children, logs = [], []
    try:
        env = {**os.environ, **{k: "1" for k in model.THREADS}, "PYTHONPATH": str(ROOT)}
        for index in (1, 2):
            log = (args.output / f"process_{index}.log").open("xb")
            logs.append(log)
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "child",
                "--request",
                str(args.request),
                "--request-sha256",
                args.request_sha256,
                "--executor-commit",
                args.executor_commit,
                "--input",
                str(args.input),
                "--input-sha256",
                args.input_sha256,
                "--output",
                str(args.output / f"process_{index}.json"),
                "--process-index",
                str(index),
            ]
            children.append(subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT))
        counters.update(started_fits=None, completed_fits=None)
        codes = [child.wait() for child in children]
        model.require(codes == [0, 0], "fresh-process child failed", "child_failed")
        first, second = [read_json(args.output / f"process_{index}.json") for index in (1, 2)]
        model.verify_processes(first, second, input_bundle=bundle)
        counters.update(started_fits=10, completed_fits=10)
        # Only now may the parent read the complete retrospective outcome view.
        with no_training_or_external_actions():
            facts = model.read_evaluation_facts(request, bundle)
            write_once(args.output / "outcomes.json", facts)
            final = model.close_processes(first, second, input_bundle=bundle, facts=facts)
        _source(args)
        write_once(args.output / "acceptance.json", final)
        model.require(
            canonical_json_bytes(read_json(args.output / "acceptance.json")) == canonical_json_bytes(final),
            "parent finalization business readback differs",
        )
        print(f"completed_fits=10 prediction_sha256={final['prediction_sha256']} receipt={final['receipt_sha256']}")
    finally:
        # Only subprocesses created by this exact invocation, never a user service.
        for child in children:
            if child.poll() is None:
                child.terminate()
                child.wait()
        for log in logs:
            log.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "run", "child"))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--executor-commit", required=True)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--input-sha256")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--process-index", type=int, choices=(1, 2))
    args = parser.parse_args(argv)
    # Do not create even a failure receipt inside a source/candidate/linked path.
    args.output = validate_output_location(args.output)
    failure = (
        args.output.with_name(args.output.name + ".parent.failure.json")
        if args.command == "run"
        else args.output.with_name(args.output.name + ".failure.json")
    )
    validate_output_location(failure)
    model.require(not failure.exists(), "failure output collision")
    counters = {"started_fits": 0, "completed_fits": 0}
    try:
        _source(args)
        request = _request(args)
        if args.command == "prepare":
            with no_training_or_external_actions():
                result = model.prepare_inputs(request, source_commit=args.executor_commit)
            _source(args)
            write_once(args.output, result)
            print(f"prepared input_hash={result['input_hash']} months=5 formal_fits=0")
        else:
            model.require(args.input is not None and args.input_sha256 is not None, "explicit input/hash required")
            if args.command == "child":
                model.require(args.process_index is not None, "child process index required")
                _child(args, request, counters)
            else:
                _run(args, request, counters)
        return 0
    except Exception as exc:
        try:
            _failure(failure, exc, counters)
        except Exception as receipt_exc:
            print(f"failure_receipt_error={type(receipt_exc).__name__}: {receipt_exc}", file=sys.stderr)
        print(f"{getattr(exc, 'reason_code', 'hmm_risk_rotation_l2_rolling_execution_failed')}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
