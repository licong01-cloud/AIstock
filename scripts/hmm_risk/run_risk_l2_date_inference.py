"""Explicit-date frozen risk inference from files or one read-only DB snapshot."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_date_inference as study  # noqa: E402
from backend.services.hmm_risk import risk_l2_database_inference as database  # noqa: E402
from backend.services.hmm_risk.frozen_l2_history import no_training_or_external_actions  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from scripts.hmm_risk.validate_frozen_l2_history import _head, _numeric  # noqa: E402


def _clean(head):
    study.require(
        _head() == head
        and not subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip(),
        "inference requires stable clean committed source",
    )


def run(args, request, head, environment, consumer=study):
    processes, logs = [], []
    try:
        study.require(not args.output.exists(), "output collision")
        args.output.mkdir(parents=True)
        if args.source_mode == "database_daily":
            with consumer.connect(request) as connection, no_training_or_external_actions():
                bundle = consumer.prepare(request, head, connection=connection)
        else:
            with no_training_or_external_actions():
                bundle = consumer.prepare(request, head)
        features_path = args.output / "features.json"
        write_once(features_path, bundle)
        env = {**os.environ, **{key: "1" for key in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT)}
        for n in (1, 2):
            log = (args.output / f"process_{n}.log").open("xb")
            logs.append(log)
            processes.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "child",
                        "--source-mode",
                        args.source_mode,
                        "--request",
                        str(args.request),
                        "--request-sha256",
                        request["receipt_sha256"],
                        "--features",
                        str(features_path),
                        "--features-sha256",
                        bundle["receipt_sha256"],
                        "--executor-commit",
                        head,
                        "--output",
                        str(args.output / f"process_{n}.json"),
                    ],
                    cwd=ROOT,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
            )
        study.require(all(process.wait() == 0 for process in processes), "fresh-process inference failed")
        paths = [args.output / f"process_{n}.json" for n in (1, 2)]
        results = [read_json(path) for path in paths]
        for result in results:
            consumer.validate_prediction(request, bundle, result)
            study.require(
                result["numeric_environment"] == environment, "child numerical environment differs", "identity_mismatch"
            )
        study.require(
            paths[0].read_bytes() == paths[1].read_bytes(), "fresh-process payloads differ", "prediction_failed"
        )
        _clean(head)
        final = receipt(
            {
                "schema_version": consumer.VERSION + "_acceptance",
                "execution_status": "COMPLETED",
                "request_sha256": request["receipt_sha256"],
                "executor_commit": head,
                "features_sha256": bundle["receipt_sha256"],
                "prediction_sha256": results[0]["receipt_sha256"],
                "model_identity": bundle["model_identity"],
                "inference_identity": bundle["inference_identity"],
                "dates": results[0]["dates"],
                "two_process_bitwise_equal": True,
                "completed_children": 2,
                "fits": 0,
                "labels_read": False,
                "outcomes_read": False,
                "database_write": False,
                "dataset_write": False,
                "active_profile_write": False,
                "runtime_action": False,
                "policy_applied": False,
                "forward_confirmed": False,
            }
        )
        write_once(args.output / "acceptance.json", final)
        print(f"COMPLETED dates={len(final['dates'])} catalog=131 fits=0 sha256={final['receipt_sha256']}")
    except Exception as exc:
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
    parser.add_argument("--source-mode", choices=("frozen_file", "database_daily"), default="frozen_file")
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--features", type=Path)
    parser.add_argument("--features-sha256")
    parser.add_argument("--executor-commit")
    args = parser.parse_args(argv)
    consumer = database if args.source_mode == "database_daily" else study
    validated = False
    try:
        args.output = validate_output_location(args.output)
        validated = True
        head = _head()
        _clean(head)
        environment = _numeric("risk")
        with no_training_or_external_actions():
            request = study.inputs._asset(args.request, args.request_sha256)
            consumer.validate_request(request)
            if args.mode == "child":
                study.require(
                    args.executor_commit == head and args.features is not None and bool(args.features_sha256),
                    "parent features/source binding missing",
                )
                bundle = study.inputs._asset(args.features, args.features_sha256)
                study.require(
                    bundle["source_commit"] == head, "parent feature source commit differs", "identity_mismatch"
                )
                result = consumer.infer(request, bundle, expected_bundle_sha256=args.features_sha256)
                _clean(head)
                write_once(
                    args.output,
                    receipt(
                        {
                            **{k: v for k, v in result.items() if k != "receipt_sha256"},
                            "numeric_environment": environment,
                        }
                    ),
                )
        if args.mode == "run":
            run(args, request, head, environment, consumer)
    except Exception as exc:
        path = args.output.with_name(
            args.output.name + (".parent.failure.json" if args.mode == "run" else ".failure.json")
        )
        if validated and not path.exists():
            write_once(
                path,
                receipt(
                    {
                        "schema_version": consumer.VERSION + "_failure",
                        "execution_status": "FAILED",
                        "stage": args.mode,
                        "reason_code": getattr(exc, "reason_code", "hmm_risk_l2_risk_date_inference_failed"),
                        "exception_type": type(exc).__name__,
                        "message": str(exc),
                        "completed_children": getattr(exc, "completed_children", 0),
                        "fits": 0,
                        "database_write": False,
                        "dataset_write": False,
                        "runtime_action": False,
                    }
                ),
            )
        print(f"FAILED stage={args.mode} reason={getattr(exc, 'reason_code', type(exc).__name__)}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
