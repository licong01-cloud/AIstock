"""Explicit-release file preflight/re-inference and at most four Q4 fits."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_member_release as study  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    numeric_environment,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from scripts.hmm_risk.run_risk_l2_loss_severity import guard  # noqa: E402
from scripts.hmm_risk.run_risk_l2_rolling import clean_head  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("source-preflight", "prepare", "preflight", "run", "child", "reinfer"))
    for key in ("request", "prepared", "model", "features"):
        parser.add_argument("--" + key, type=Path)
        parser.add_argument("--" + key + "-sha256")
    parser.add_argument("--source-mode", choices=("frozen_file", "database_daily"), default="frozen_file")
    parser.add_argument("--executor-commit")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = validate_output_location(args.output)
    failure = output.with_name(output.name + ".failure.json")
    if output.exists() or failure.exists():
        parser.error("output or failure exists; no overwrite/retry")
    safe = False
    try:
        head = clean_head()
        if args.mode in {"source-preflight", "prepare", "reinfer"}:
            study.require(args.request and args.request_sha256, "explicit request byte pin required")
            request = study.member.pinned_json(args.request, args.request_sha256)
            if args.source_mode == "frozen_file":
                validate_output_location(output, dataset_root=Path(request["source"]["candidate_root"]))
            safe = True
            with guard(allow_ridge=False):
                if args.mode == "source-preflight":
                    result = study.source_preflight(request)
                elif args.mode == "prepare":
                    result = study.prepare(args.request, args.request_sha256, output, head)
                else:
                    study.require(
                        args.model and args.model_sha256 and args.features and args.features_sha256,
                        "explicit model/features pins required",
                    )
                    result = study.reinfer(
                        read_json(args.model),
                        request,
                        read_json(args.features),
                        model_sha256=args.model_sha256,
                        bundle_sha256=args.features_sha256,
                        source_mode=args.source_mode,
                    )
            study.require(clean_head() == head, "source changed during read")
            if args.mode != "prepare":
                write_once(
                    output,
                    receipt({k: v for k, v in result.items() if k != "receipt_sha256"} | dict(executor_commit=head)),
                )
            print(f"{args.mode}: {result.get('status', 'COMPLETED')}; fits=0")
            return 2 if result.get("status") == "BLOCKED" else 0
        study.require(args.prepared and args.prepared_sha256, "explicit prepared identity required")
        with guard(allow_ridge=False):
            prepared, features, _, rows = study.load_prepared(args.prepared, args.prepared_sha256)
            validate_output_location(output, dataset_root=Path(features["source"]["candidate_root"]))
            safe = True
        if args.mode == "preflight":
            write_once(
                output,
                receipt(
                    dict(
                        status="PASS",
                        source_commit=head,
                        prepared_sha256=args.prepared_sha256,
                        common_rows=len(rows),
                        common_dates=len({r[0] for r in rows}),
                        common_mask_sha256=prepared["common_mask_sha256"],
                        fits=0,
                        evaluation_outcomes_read=False,
                        database_access=False,
                        runtime_action=False,
                    )
                ),
            )
            return 0
        if args.mode == "child":
            study.require(head == args.executor_commit, "child source differs")
            numeric = numeric_environment()
            numeric = {k: numeric[k] for k in ("versions", "thread_variables", "thread_pools")}
            numeric["thread_pools"] = [{k: v for k, v in p.items() if k != "filepath"} for p in numeric["thread_pools"]]
            with guard(allow_ridge=True):
                result = study.execute_child(args.prepared, args.prepared_sha256, output, head)
            study.require(clean_head() == head, "source changed in child")
            write_once(
                output / "process.json",
                receipt({k: v for k, v in result.items() if k != "receipt_sha256"} | dict(numeric_environment=numeric)),
            )
            return 0
        output.mkdir(parents=True)
        environment = {**os.environ, **dict.fromkeys(THREAD_VARIABLES, "1"), "PYTHONPATH": str(ROOT), "PYTHONUTF8": "1"}
        results = []
        for number in (1, 2):
            destination = output / f"process_{number}"
            with (output / f"process_{number}.log").open("xb") as log:
                process = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "child",
                        "--prepared",
                        str(args.prepared),
                        "--prepared-sha256",
                        args.prepared_sha256,
                        "--executor-commit",
                        head,
                        "--output",
                        str(destination),
                    ],
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=False,
                )
            study.require(process.returncode == 0, f"fresh process {number} failed; no retry")
            results.append(read_json(destination / "process.json"))
        study.require(clean_head() == head, "source changed during run")
        acceptance = study.close_processes(*results, head, prepared, features)
        write_once(output / "acceptance.json", acceptance)
        for arm in study.member.ARMS:
            write_once(
                output / f"predictions_{arm}.json",
                receipt(
                    dict(
                        schema_version=study.VERSION + "_prediction",
                        contract=study.CONTRACT,
                        arm=arm,
                        acceptance_sha256=acceptance["receipt_sha256"],
                        predictions=results[0]["sealed"][arm]["predictions"],
                        inference_identity=features["inference_identity"],
                        fits=0,
                        policy_applied=False,
                    )
                ),
            )
        print("COMPLETED; formal fits=4; no evaluation or promotion")
        return 0
    except Exception as exc:
        if safe:
            write_once(
                failure,
                receipt(
                    dict(
                        status="FAILED",
                        reason_code=getattr(exc, "reason_code", None),
                        detail=str(exc),
                        automatic_retry=False,
                        database_write=False,
                        runtime_action=False,
                    )
                ),
            )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
