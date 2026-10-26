"""Approved quarterly risk comparison: file-only prepare, exactly two 2-fit children."""

from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_rolling as rolling  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    numeric_environment,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402


@contextmanager
def file_only(*, allow_fit):
    def forbidden(*args, **kwargs):
        raise rolling.risk.fail("database/network/unauthorized fit forbidden", "forbidden_action")

    targets = [
        "backend.db.pg_pool.get_conn",
        "psycopg2.connect",
        "socket.create_connection",
        "backend.services.hmm_risk.formal_state_model.fit_entry",
        "backend.services.hmm_risk.formal_state_model.causal_filter",
        "backend.services.hmm_risk.rotation_l1_input_bundle.load_active_hmm_dataset_identity",
    ]
    if not allow_fit:
        targets.append("sklearn.linear_model.LogisticRegression.fit")
    with ExitStack() as stack:
        for target in targets:
            stack.enter_context(patch(target, forbidden))
        yield


def clean_head():
    head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    status = subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip()
    rolling.require(not status, "formal execution requires clean committed source")
    return head


def close_processes(first, second, prepared, head):
    for result in (first, second):
        rolling.core.verify_receipt(result)
        rolling.require(
            result["contract"] == rolling.CONTRACT
            and result["fits"] == 2
            and result["source_commit"] == head
            and result["prepared_sha256"] == prepared["receipt_sha256"],
            "child identity differs",
        )
    rolling.require(canonical_json_bytes(first) == canonical_json_bytes(second), "fresh processes differ")
    return receipt(
        dict(
            schema_version=rolling.VERSION + "_acceptance",
            status="COMPLETED",
            contract=rolling.CONTRACT,
            source_commit=head,
            completed_fits=4,
            process_result_sha256=first["receipt_sha256"],
            prepared_sha256=prepared["receipt_sha256"],
            model_bundles=first["model_bundles"],
            evaluation=first["evaluation"],
            numeric_environment=first["numeric_environment"],
            database_access=False,
            database_write=False,
            runtime_action=False,
            automatic_promotion=False,
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run", "child"))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--executor-commit")
    args = parser.parse_args()
    output = validate_output_location(args.output)
    if args.mode == "prepare":
        candidate_root = read_json(args.request)["source"]["candidate_root"]
    else:
        prepared = rolling.core.inputs._asset(str(args.request), args.request_sha256)
        features = rolling.core.inputs._asset(prepared["feature_path"], prepared["feature_sha256"])
        candidate_root = features["source"]["candidate_root"]
        del features
    validate_output_location(output, dataset_root=Path(candidate_root))
    # Never write into an existing artifact, even on an error path.
    if output.exists() or output.with_name(output.name + ".failure.json").exists():
        parser.error("output or failure receipt already exists")
    try:
        head = clean_head()
        if args.mode == "prepare":
            with file_only(allow_fit=False):
                prepared = rolling.prepare(args.request, args.request_sha256, output, head)
            print(f"PREPARED; fits=0; hash={prepared['receipt_sha256']}; output={output}", flush=True)
            return 0
        prepared = rolling.core.inputs._asset(str(args.request), args.request_sha256)
        rolling.require(prepared["source_commit"] == head, "prepared source commit differs")
        if args.mode == "child":
            rolling.require(args.executor_commit == head, "child source commit differs")
            environment = numeric_environment()
            environment = {k: environment[k] for k in ("versions", "thread_variables", "thread_pools")}
            environment["thread_pools"] = [
                {k: v for k, v in pool.items() if k != "filepath"} for pool in environment["thread_pools"]
            ]
            rolling.require(all(v == "1" for v in environment["thread_variables"].values()), "numeric threads differ")
            with file_only(allow_fit=True):
                result = rolling.execute_child(prepared, output, head)
            result = receipt(
                {k: v for k, v in result.items() if k != "receipt_sha256"} | {"numeric_environment": environment}
            )
            write_once(output / "process.json", result)
            return 0
        output.mkdir(parents=True)
        env = {**os.environ, **{k: "1" for k in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT), "PYTHONUTF8": "1"}
        results = []
        for number in (1, 2):
            child_output = output / f"process_{number}"
            print(f"process {number}/2; two approved fits", flush=True)
            with (output / f"process_{number}.log").open("xb") as log:
                child = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "child",
                        "--request",
                        str(args.request),
                        "--request-sha256",
                        args.request_sha256,
                        "--output",
                        str(child_output),
                        "--executor-commit",
                        head,
                    ],
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=False,
                )
            rolling.require(child.returncode == 0, f"process {number} failed; no retry")
            results.append(read_json(child_output / "process.json"))
        rolling.require(clean_head() == head, "source changed during execution")
        accepted = close_processes(*results, prepared, head)
        for model in accepted["model_bundles"]:
            path = output / f"model_{model['vintage']['id']}.json"
            write_once(path, model)
            rolling.require(
                rolling.core.inputs._asset(str(path), model["receipt_sha256"]) == model, "model readback differs"
            )
        for arm in ("M0", "M1"):
            prediction = receipt(
                dict(
                    schema_version=rolling.VERSION + "_predictions",
                    arm=arm,
                    source_commit=head,
                    prepared_sha256=prepared["receipt_sha256"],
                    contract=rolling.CONTRACT,
                    model_bundles=accepted["model_bundles"] if arm == "M1" else [],
                    predictions=accepted["evaluation"][arm + "_predictions"],
                    research_only=True,
                    historical_as_known=False,
                    untouched_holdout=False,
                )
            )
            write_once(output / f"{arm}_predictions.json", prediction)
        write_once(output / "acceptance.json", accepted)
        print(f"COMPLETED; fits=4; automatic_promotion=false; output={output}", flush=True)
        return 0
    except Exception as exc:
        # Attempt/seal files provide durable exact progress, including a failed fit.
        attempts = list(output.glob("*.attempt.json")) + list(output.glob("process_*/*.attempt.json"))
        sealed = list(output.glob("*.sealed.json")) + list(output.glob("process_*/*.sealed.json"))
        reason = getattr(exc, "reason_code", "hmm_risk_rolling_execution_failed")
        failure = receipt(
            dict(
                status="INSUFFICIENT_TRAINING_EVIDENCE"
                if reason.endswith("train_label_evidence_insufficient")
                else "FAILED",
                reason_code=reason,
                exception_type=type(exc).__name__,
                message=str(exc),
                attempted_fits=len(attempts),
                sealed_fits=len(sealed),
                database_write=False,
                runtime_action=False,
            )
        )
        write_once(output.with_name(output.name + ".failure.json"), failure)
        print(f"FAILED: {failure['reason_code']}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
