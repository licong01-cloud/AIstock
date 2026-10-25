"""Explicit preparation / two fresh-process candidates; never an API or scheduler."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import l2_economic_candidates as model  # noqa: E402
from backend.services.hmm_risk.frozen_l2_history import no_training_or_external_actions  # noqa: E402
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as hashes  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    numeric_environment,
    read_json,
    validate_output_location,
    write_once,
)


def head() -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()


def clean_head() -> str:
    if subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip():
        model.require(False, "formal execution requires committed clean source")
    return head()


def environment() -> dict:
    value = numeric_environment()
    return {
        "versions": value["versions"],
        "thread_variables": value["thread_variables"],
        "thread_pools": [{k: v for k, v in p.items() if k != "filepath"} for p in value["thread_pools"]],
    }


def close(
    bundle: dict, children: list[dict], source: str, candidate: str, *, evaluator_source: str | None = None
) -> dict:
    model.validate_input(bundle)
    for number, child in enumerate(children, 1):
        hashes.verify(child, "process_sha256")
        model.require(
            child["schema_version"] == model.VERSION + "_process" and child["process_index"] == number,
            "fresh-process child identity differs",
        )
        model.require(
            child["source_commit"] == source and child["sealed"]["candidate"] == candidate,
            "child source/candidate differs",
        )
        model.readback(bundle, child["sealed"])
    model.require(
        len(children) == 2
        and children[0]["sealed"] == children[1]["sealed"]
        and children[0]["numeric_environment"] == children[1]["numeric_environment"],
        "fresh-process bitwise mismatch",
    )
    # Outcomes are opened only after both predictions and parent zero-fit readback.
    result = model.evaluate(bundle, children[0]["sealed"])
    return hashes.seal(
        {
            "schema_version": model.VERSION + "_acceptance",
            "candidate": candidate,
            "contract": model.CONTRACT,
            "source_commit": source,
            "evaluation_source_commit": evaluator_source or source,
            "fits_added_by_closure": 0,
            "input_sha256": bundle["input_sha256"],
            "model_sha256": children[0]["sealed"]["model_sha256"],
            "prediction_sha256": children[0]["sealed"]["prediction_sha256"],
            "child_sha256": [c["process_sha256"] for c in children],
            "completed_fits": 2 if candidate == "risk" else 10,
            "reproducibility": "BITWISE_EQUAL_PARENT_ZERO_FIT_READBACK",
            "result": result,
            "selection_basis": model.CONTRACT["selection_basis"],
            "new_tail_accessed": False,
            "historical_window_previously_consumed": True,
            "advisory_status": "NOT_AVAILABLE",
            "forward_confirmation": "NOT_STARTED",
            "database_write": False,
            "dataset_write": False,
            "runtime_action": False,
        },
        "acceptance_sha256",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run", "child", "close"))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--candidate", choices=("risk", "rotation"))
    parser.add_argument("--process-index", type=int)
    parser.add_argument("--input-sha256")
    parser.add_argument("--source-commit")
    parser.add_argument("--child-one", type=Path)
    parser.add_argument("--child-two", type=Path)
    args = parser.parse_args(argv)
    output = validate_output_location(args.output)
    progress = {"started_fits": 0, "completed_fits": 0}
    try:
        model.require(not output.exists(), "output must be new", "output_collision")
        source = clean_head()
        if args.mode == "prepare":
            write_once(output, model.prepare(read_json(args.request), source))
            print(f"prepared={output}; fits=0; source={source}", flush=True)
        elif args.mode == "child":
            model.require(
                args.candidate is not None
                and args.process_index in (1, 2)
                and args.input_sha256
                and args.source_commit == source,
                "child authority arguments invalid",
            )
            bundle = read_json(args.request)
            model.require(
                bundle["input_sha256"] == args.input_sha256 and bundle["source_commit"] == source,
                "child prepared/source differs",
            )
            env = environment()
            sealed = model.execute(bundle, args.candidate, progress)
            write_once(
                output,
                hashes.seal(
                    {
                        "schema_version": model.VERSION + "_process",
                        "process_index": args.process_index,
                        "source_commit": source,
                        "numeric_environment": env,
                        "sealed": sealed,
                    },
                    "process_sha256",
                ),
            )
            print(f"sealed={output}; completed_fits={progress['completed_fits']}", flush=True)
        elif args.mode == "close":
            model.require(
                args.candidate and args.source_commit and args.child_one and args.child_two,
                "close requires explicit candidate, fitting source and two sealed children",
            )
            subprocess.check_call(["git", "-C", str(ROOT), "merge-base", "--is-ancestor", args.source_commit, source])
            bundle = read_json(args.request)
            model.validate_input(bundle)
            model.require(bundle["source_commit"] == args.source_commit, "closure fitting source differs")
            authority = model.prepare(bundle["request"], args.source_commit)
            model.require(
                authority["input_sha256"] == bundle["input_sha256"], "closure prepared/source authority differs"
            )
            del authority
            children = [model.reference._sealed_file(p) for p in (args.child_one, args.child_two)]
            with (
                no_training_or_external_actions(),
                patch.object(
                    model.GradientBoostingRegressor,
                    "fit",
                    side_effect=RuntimeError("zero-fit closure forbids training"),
                ),
            ):
                result = close(bundle, children, args.source_commit, args.candidate, evaluator_source=source)
            write_once(output, result)
            print(
                f"{result['result']['effect_status']}; additional_fits=0; original_fits={result['completed_fits']}; output={output}",
                flush=True,
            )
        else:
            model.require(args.candidate is not None, "run requires explicit candidate")
            bundle = read_json(args.request)
            model.validate_input(bundle)
            model.require(bundle["source_commit"] == source, "prepared source commit differs")
            authority = model.prepare(bundle["request"], source)
            model.require(
                authority["input_sha256"] == bundle["input_sha256"],
                "prepared input differs from pinned source derivation",
            )
            del authority
            output.mkdir(parents=True)
            env = {**os.environ, **{k: "1" for k in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT)}
            children = []
            for number in (1, 2):
                path = output / f"process_{number}.json"
                with (output / f"process_{number}.log").open("xb") as log:
                    child = subprocess.run(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "child",
                            "--request",
                            str(args.request),
                            "--output",
                            str(path),
                            "--candidate",
                            args.candidate,
                            "--process-index",
                            str(number),
                            "--input-sha256",
                            bundle["input_sha256"],
                            "--source-commit",
                            source,
                        ],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=False,
                    )
                if child.returncode:
                    failed = path.with_name(path.name + ".failure.json")
                    if failed.is_file():
                        receipt = read_json(failed)
                        hashes.verify(receipt, "failure_sha256")
                        progress = {k: progress[k] + receipt["fit_counts"][k] for k in progress}
                    model.require(False, f"child {number} failed; see {path.name}.log", "child_failed")
                value = read_json(path)
                hashes.verify(value, "process_sha256")
                progress = {k: progress[k] + value["sealed"]["fit_counts"][k] for k in progress}
                children.append(value)
                print(f"child={number}; completed_fits={progress['completed_fits']}", flush=True)
            result = close(bundle, children, source, args.candidate)
            write_once(output / "acceptance.json", result)
            print(
                f"{result['result']['effect_status']}; completed_fits={result['completed_fits']}; output={output / 'acceptance.json'}",
                flush=True,
            )
    except Exception as exc:
        failure = hashes.seal(
            {
                "schema_version": model.VERSION + "_failure",
                "candidate": args.candidate,
                "status": "FAILED",
                "reason_code": getattr(exc, "reason_code", "hmm_risk_l2_economic_execution_failed"),
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "fit_counts": progress,
                "database_write": False,
                "dataset_write": False,
                "runtime_action": False,
            },
            "failure_sha256",
        )
        write_once(output.with_name(output.name + ".failure.json"), failure)
        print(f"FAILED: {failure['reason_code']}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
