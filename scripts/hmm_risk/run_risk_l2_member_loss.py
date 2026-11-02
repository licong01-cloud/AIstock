"""One member-loss candidate and paired control: two fresh processes, eight fits."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_member_loss as study  # noqa: E402
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
    parser.add_argument("mode", choices=("prepare", "preflight", "run", "child", "infer"))
    for name in ("study", "cohort", "old-a", "prepared", "model", "request", "features", "provider-absence-manifest"):
        parser.add_argument("--" + name, type=Path)
    for name in ("cohort-sha256", "prepared-sha256", "model-sha256", "features-sha256", "executor-commit"):
        parser.add_argument("--" + name)
    parser.add_argument("--source-mode", choices=("frozen_file", "database_daily"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = validate_output_location(args.output)
    if output.exists() or output.with_name(output.name + ".failure.json").exists():
        parser.error("output or failure exists; no overwrite or automatic retry")
    safe_output = False
    try:
        head = clean_head()
        if args.mode == "infer":
            study.require(
                all(
                    (args.model, args.model_sha256, args.request, args.features, args.features_sha256, args.source_mode)
                ),
                "explicit inference identities required",
            )
            request = read_json(args.request)
            if args.source_mode == "frozen_file":
                validate_output_location(output, dataset_root=Path(request["source"]["candidate_root"]))
            safe_output = True
            with guard(allow_ridge=False):
                result = study.infer_prepared(
                    read_json(args.model),
                    request,
                    read_json(args.features),
                    source_mode=args.source_mode,
                    expected_model_sha256=args.model_sha256,
                    expected_bundle_sha256=args.features_sha256,
                )
            study.require(clean_head() == head, "source changed during inference")
            write_once(
                output,
                receipt(
                    {k: v for k, v in result.items() if k != "receipt_sha256"} | {"inference_executor_commit": head}
                ),
            )
            print(f"INFERRED rows={len(result['predictions'])}; fits=0")
            return 0
        if args.mode == "prepare":
            study.require(
                all((args.study, args.cohort, args.cohort_sha256, args.old_a, args.provider_absence_manifest)),
                "explicit study/cohort/A/provider pins required",
            )
            with guard(allow_ridge=False):
                _, features = study.loss.load_study(args.study)
                validate_output_location(output, dataset_root=Path(features["source"]["candidate_root"]))
                safe_output = True
                prepared = study.prepare(
                    args.study,
                    args.cohort,
                    args.cohort_sha256,
                    args.old_a,
                    output,
                    head,
                    provider_absence_manifest=args.provider_absence_manifest,
                )
            study.require(clean_head() == head, "source changed during prepare")
            print(f"PREPARED fits=0; hash={prepared['receipt_sha256']}")
            return 0
        study.require(args.prepared and args.prepared_sha256, "explicit prepared pin required")
        with guard(allow_ridge=False):
            prepared, features = study.load_prepared(args.prepared, args.prepared_sha256)
            validate_output_location(output, dataset_root=Path(features["source"]["candidate_root"]))
            safe_output = True
        if args.mode == "preflight":
            with guard(allow_ridge=False):
                _, samples = study.training_packets(prepared, features)
                study.pinned_json(prepared["old_a_path"], study.OLD_A_PIN)
            write_once(
                output,
                receipt(
                    dict(
                        status="PASS",
                        prepared_sha256=prepared["receipt_sha256"],
                        source_commit=head,
                        common_training_samples=samples,
                        fits=0,
                        evaluation_outcomes_read=False,
                        database_access=False,
                        runtime_action=False,
                    )
                ),
            )
            print("PREFLIGHT PASS; fits=0; evaluation_outcomes_read=false")
            return 0
        del features
        if args.mode == "child":
            study.require(args.executor_commit == head, "child source differs")
            numeric = numeric_environment()
            numeric = {k: numeric[k] for k in ("versions", "thread_variables", "thread_pools")}
            numeric["thread_pools"] = [{k: v for k, v in p.items() if k != "filepath"} for p in numeric["thread_pools"]]
            with guard(allow_ridge=True):
                result = study.execute_child(args.prepared, args.prepared_sha256, output, head)
            study.require(clean_head() == head, "source changed in child")
            write_once(
                output / "process.json",
                receipt({k: v for k, v in result.items() if k != "receipt_sha256"} | {"numeric_environment": numeric}),
            )
            return 0
        output.mkdir(parents=True)
        environment = {**os.environ, **{k: "1" for k in THREAD_VARIABLES}, "PYTHONPATH": str(ROOT), "PYTHONUTF8": "1"}
        results = []
        for number in (1, 2):
            child_output = output / f"process_{number}"
            print(f"fresh process {number}/2; planned fits=4", flush=True)
            with (output / f"process_{number}.log").open("xb") as log:
                child = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "child",
                        "--prepared",
                        str(args.prepared),
                        "--prepared-sha256",
                        args.prepared_sha256,
                        "--output",
                        str(child_output),
                        "--executor-commit",
                        head,
                    ],
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=False,
                )
            study.require(child.returncode == 0, f"process {number} failed; no retry")
            results.append(read_json(child_output / "process.json"))
        study.require(clean_head() == head, "source changed during study")
        accepted = study.close_processes(*results, head, args.prepared_sha256)
        for arm in study.ARMS:
            models = [v[arm]["model"] for v in results[0]["sealed"]]
            for model in models:
                write_once(output / f"model_{model['vintage']['id']}_{arm}.json", model)
            predictions = receipt(
                dict(
                    schema_version=study.ARMS[arm][0] + "_predictions",
                    contract=study.CONTRACT,
                    arm=arm,
                    source_commit=head,
                    prepared_sha256=args.prepared_sha256,
                    model_bundles=models,
                    release_binding=prepared["release_binding"],
                    feature_sha256=study.loss.FEATURE_PIN,
                    predictions=[r for v in results[0]["sealed"] for r in v[arm]["predictions"]],
                    historical_as_known=False,
                    untouched_holdout=False,
                    policy_applied=False,
                    research_only=True,
                )
            )
            write_once(output / f"predictions_{arm}.json", predictions)
        write_once(output / "acceptance.json", accepted)
        print(f"COMPLETED fits=8; {accepted['evaluation']['research_disposition']}", flush=True)
        return 0
    except Exception as exc:
        attempts = list(output.glob("*.attempt.json")) + list(output.glob("process_*/*.attempt.json"))
        sealed = list(output.glob("*.sealed.json")) + list(output.glob("process_*/*.sealed.json"))
        if safe_output:
            write_once(
                output.with_name(output.name + ".failure.json"),
                receipt(
                    dict(
                        status="FAILED",
                        reason_code=getattr(exc, "reason_code", "hmm_risk_member_loss_execution_failed"),
                        message=str(exc),
                        attempted_fits=len(attempts),
                        sealed_fits=len(sealed),
                        database_write=False,
                        dataset_write=False,
                        runtime_action=False,
                    )
                ),
            )
        print(f"FAILED: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
