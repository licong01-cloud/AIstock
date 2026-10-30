"""Single approved loss candidate: two fresh processes, four fits; no policy."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_loss_severity as loss  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    numeric_environment,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from scripts.hmm_risk.run_risk_l2_rolling import clean_head, file_only  # noqa: E402


@contextmanager
def guard(*, allow_ridge):
    def forbidden(*args, **kwargs):
        raise loss.rolling.risk.fail("unauthorized fit/network", "forbidden_action")

    with file_only(allow_fit=False), patch("socket.socket.connect", forbidden):
        if allow_ridge:
            yield
        else:
            with patch("sklearn.linear_model.Ridge.fit", forbidden):
                yield


def close_processes(first, second, head):
    for result in (first, second):
        loss.core.verify_receipt(result)
        loss.require(
            result["contract"] == loss.CONTRACT
            and result["fits"] == 2
            and result["source_commit"] == head
            and result["original_prepared_sha256"] == loss.PREPARED_PIN,
            "child execution identity differs",
        )
    loss.require(canonical_json_bytes(first) == canonical_json_bytes(second), "fresh processes differ")
    return receipt(
        dict(
            schema_version=loss.VERSION + "_acceptance",
            status="COMPLETED",
            contract=loss.CONTRACT,
            source_commit=head,
            completed_fits=4,
            process_sha256=first["receipt_sha256"],
            models=[s["model"] for s in first["sealed"]],
            evaluation=first["evaluation"],
            numeric_environment=first["numeric_environment"],
            database_write=False,
            runtime_action=False,
            automatic_promotion=False,
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("preflight", "run", "child", "infer"))
    parser.add_argument("--study", type=Path)
    parser.add_argument("--m0", type=Path)
    parser.add_argument("--m1", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--executor-commit")
    parser.add_argument("--model", type=Path)
    parser.add_argument("--model-sha256")
    parser.add_argument("--request", type=Path)
    parser.add_argument("--features", type=Path)
    parser.add_argument("--features-sha256")
    parser.add_argument("--source-mode", choices=("frozen_file", "database_daily"))
    args = parser.parse_args()
    output = validate_output_location(args.output)
    if output.exists() or output.with_name(output.name + ".failure.json").exists():
        parser.error("output/failure already exists; no retry or overwrite")
    safe_output = False
    try:
        head = clean_head()
        if args.mode == "infer":
            loss.require(
                all(
                    (args.model, args.model_sha256, args.request, args.features, args.features_sha256, args.source_mode)
                ),
                "explicit model/request/source pins required",
            )
            request = read_json(args.request)
            if args.source_mode == "frozen_file":
                validate_output_location(output, dataset_root=Path(request["source"]["candidate_root"]))
            safe_output = True
            with guard(allow_ridge=False):
                result = loss.infer_prepared(
                    read_json(args.model),
                    request,
                    read_json(args.features),
                    source_mode=args.source_mode,
                    expected_model_sha256=args.model_sha256,
                    expected_bundle_sha256=args.features_sha256,
                )
            result = receipt(
                {k: v for k, v in result.items() if k != "receipt_sha256"} | {"inference_executor_commit": head}
            )
            loss.require(clean_head() == head, "source changed during inference")
            write_once(output, result)
            print(f"INFERRED; fits=0; rows={len(result['predictions'])}; hash={result['receipt_sha256']}")
            return 0
        loss.require(all((args.study, args.m0, args.m1)), "explicit study and baseline paths required")
        with guard(allow_ridge=False):
            original, features = loss.load_study(args.study)
            validate_output_location(output, dataset_root=Path(features["source"]["candidate_root"]))
            safe_output = True
            loss.load_baselines({"M0": args.m0, "M1": args.m1}, features)
        del original, features
        if args.mode == "preflight":
            write_once(
                output,
                receipt(
                    dict(
                        status="PASS",
                        source_commit=head,
                        prepared_sha256=loss.PREPARED_PIN,
                        feature_sha256=loss.FEATURE_PIN,
                        baseline_pins=loss.PREDICTION_PINS,
                        fits=0,
                        training_labels_read=False,
                        evaluation_outcomes_read=False,
                        database_access=False,
                    )
                ),
            )
            return 0
        if args.mode == "child":
            loss.require(args.executor_commit == head, "child source commit differs")
            numeric = numeric_environment()
            numeric = {k: numeric[k] for k in ("versions", "thread_variables", "thread_pools")}
            numeric["thread_pools"] = [{k: v for k, v in p.items() if k != "filepath"} for p in numeric["thread_pools"]]
            with guard(allow_ridge=True):
                result = loss.execute_child(args.study, {"M0": args.m0, "M1": args.m1}, output, head)
            loss.require(clean_head() == head, "source changed in child")
            write_once(
                output / "process.json",
                receipt({k: v for k, v in result.items() if k != "receipt_sha256"} | {"numeric_environment": numeric}),
            )
            return 0
        output.mkdir(parents=True)
        env = {**os.environ, **{k: "1" for k in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT), "PYTHONUTF8": "1"}
        results = []
        for number in (1, 2):
            child_output = output / f"process_{number}"
            print(f"fresh process {number}/2; approved fits=2", flush=True)
            with (output / f"process_{number}.log").open("xb") as log:
                child = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "child",
                        "--study",
                        str(args.study),
                        "--m0",
                        str(args.m0),
                        "--m1",
                        str(args.m1),
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
            loss.require(child.returncode == 0, f"process {number} failed; no retry")
            results.append(read_json(child_output / "process.json"))
        loss.require(clean_head() == head, "source changed during study")
        accepted = close_processes(*results, head)
        for model in accepted["models"]:
            write_once(output / f"model_{model['vintage']['id']}.json", model)
        prediction = receipt(
            dict(
                schema_version=loss.VERSION + "_predictions",
                source_commit=head,
                contract=loss.CONTRACT,
                model_bundles=accepted["models"],
                release_binding=accepted["models"][0]["training_release_binding"],
                input_identity=accepted["models"][0]["training_input_identity"],
                feature_sha256=loss.FEATURE_PIN,
                predictions=[r for s in results[0]["sealed"] for r in s["predictions"]],
                historical_as_known=False,
                untouched_holdout=False,
                policy_applied=False,
                research_only=True,
            )
        )
        write_once(output / "loss_predictions.json", prediction)
        write_once(output / "acceptance.json", accepted)
        print(f"COMPLETED; fits=4; {accepted['evaluation']['research_disposition']}; output={output}", flush=True)
        return 0
    except Exception as exc:
        attempts = list(output.glob("*.attempt.json")) + list(output.glob("process_*/*.attempt.json"))
        sealed = list(output.glob("*.sealed.json")) + list(output.glob("process_*/*.sealed.json"))
        failure = receipt(
            dict(
                status="FAILED",
                reason_code=getattr(exc, "reason_code", "hmm_risk_loss_execution_failed"),
                message=str(exc),
                attempted_fits=len(attempts),
                sealed_fits=len(sealed),
                database_write=False,
                runtime_action=False,
            )
        )
        if safe_output:
            write_once(output.with_name(output.name + ".failure.json"), failure)
        print(f"FAILED: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
