"""Frozen signed-member study: isolated TRAIN, ten attempts, sealed evaluation."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_member_return as study  # noqa: E402
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


def environment():
    value = numeric_environment()
    return dict(
        versions=value["versions"],
        thread_variables=value["thread_variables"],
        thread_pools=[{k: v for k, v in p.items() if k != "filepath"} for p in value["thread_pools"]],
    )


def asset(args, name):
    path, pin = getattr(args, name), getattr(args, name + "_sha256")
    study.require(path and pin, f"explicit {name} path/pin required")
    return study.core.inputs._asset(str(path), pin)


def child(mode, arguments, output, head, log):
    env = {**os.environ, **dict.fromkeys(THREAD_VARIABLES, "1"), "PYTHONPATH": str(ROOT), "PYTHONUTF8": "1"}
    with log.open("xb") as handle:
        run = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                mode,
                *arguments,
                "--executor-commit",
                head,
                "--output",
                str(output),
            ],
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            check=False,
        )
    study.require(run.returncode == 0, f"{mode} failed; no retry; see {log}")
    study.require(clean_head() == head, "source changed during child")


def fit_all(args, output, head, prepared, packets):
    # One budget claim tied to the immutable prepared carrier, not a new run directory.
    claim = validate_output_location(args.prepared.parent / "formal_fit_attempts")
    study.require(not claim.exists(), "formal fit budget already claimed; no retry")
    write_once(
        claim / "claim.json",
        receipt(dict(prepared_sha256=args.prepared_sha256, source_commit=head, authorized_attempts=10)),
    )
    output.mkdir(parents=True)
    groups, byte_hashes = [], []
    for number in (1, 2):
        group, hashes = {}, {}
        for vintage in study.VINTAGES:
            key = vintage["id"]
            locator = prepared["packets"][key]
            destination = output / f"process_{number}" / key
            arguments = [
                "--packet",
                locator["path"],
                "--packet-sha256",
                locator["sha256"],
                "--attempt-root",
                str(claim / f"process_{number}"),
            ]
            if key == "2026Q4":
                arguments += ["--control", prepared["control_path"], "--control-sha256", prepared["control_sha256"]]
            child("fit-child", arguments, destination, head, output / f"process_{number}_{key}.log")
            path = destination / "models.json"
            group[key], hashes[key] = read_json(path), hashlib.sha256(path.read_bytes()).hexdigest()
        groups.append(group)
        byte_hashes.append(hashes)
    study.require(byte_hashes[0] == byte_hashes[1], "fresh model bytes differ")
    acceptance = study.close_fits(groups, packets, head)
    write_once(output / "acceptance.json", acceptance)
    return acceptance


def predict_all(args, output, head, prepared):
    acceptance = asset(args, "acceptance")
    study.require(acceptance["source_commit"] == head, "fit source differs")
    output.mkdir(parents=True)
    paths = []
    for number in (1, 2):
        path = output / f"process_{number}.json"
        child(
            "predict-child",
            [
                "--acceptance",
                str(args.acceptance),
                "--acceptance-sha256",
                args.acceptance_sha256,
                "--features",
                prepared["prediction_features_path"],
                "--features-sha256",
                prepared["prediction_features_sha256"],
            ],
            path,
            head,
            output / f"process_{number}.log",
        )
        paths.append(path)
    study.require(paths[0].read_bytes() == paths[1].read_bytes(), "fresh prediction bytes differ")
    prediction = read_json(paths[0])
    study.core.verify_receipt(prediction)
    result = receipt(
        dict(
            schema_version=study.VERSION + "_prediction_acceptance",
            status="PASS",
            source_commit=head,
            fresh_processes=2,
            prediction_path=str(paths[0]),
            prediction_sha256=prediction["receipt_sha256"],
            prediction_file_sha256=hashlib.sha256(paths[0].read_bytes()).hexdigest(),
            fit_acceptance_sha256=acceptance["receipt_sha256"],
            fits=0,
            evaluation_outcomes_read=False,
        )
    )
    write_once(output / "acceptance.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode", choices=("prepare", "preflight", "fit", "fit-child", "predict", "predict-child", "evaluate")
    )
    for name in ("request", "prepared", "packet", "control", "features", "acceptance", "prediction_acceptance"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path)
        parser.add_argument("--" + name.replace("_", "-") + "-sha256")
    parser.add_argument("--attempt-root", type=Path)
    parser.add_argument("--executor-commit")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = validate_output_location(args.output)
    failure = output.with_name(output.name + ".failure.json")
    study.require(not output.exists() and not failure.exists(), "output/failure exists; no overwrite")
    head = clean_head()
    safe = False
    try:
        numeric = environment()
        if args.mode.endswith("-child"):
            study.require(args.executor_commit == head, "child source differs")
        if args.mode == "fit-child":
            with guard(allow_ridge=True):
                packet = asset(args, "packet")
                study.require(packet["source_commit"] == head, "TRAIN source differs")
                # Deliberately no prepared request, later-quarter TRAIN or outcomes access here.
                control = asset(args, "control") if packet["vintage"] == study.release.Q4 else None
                study.require(args.attempt_root is not None, "attempt ledger required")
                attempt_root = validate_output_location(args.attempt_root)
                claim = read_json(attempt_root.parent / "claim.json")
                study.core.verify_receipt(claim)
                study.require(
                    claim["source_commit"] == head
                    and claim["authorized_attempts"] == 10
                    and attempt_root.name in {"process_1", "process_2"},
                    "unapproved fit ledger",
                )
                safe = True
                result = study.fit_packet(packet, control, output, attempt_root, head, numeric)
        else:
            with guard(allow_ridge=False):
                safe = True
                if args.mode == "prepare":
                    study.require(args.request and args.request_sha256, "explicit request byte pin required")
                    result = study.prepare(args.request, args.request_sha256, output, head)
                elif args.mode == "predict-child":
                    acceptance, features = asset(args, "acceptance"), asset(args, "features")
                    study.require(acceptance["source_commit"] == head, "prediction source differs")
                    result = study.predict_collection(acceptance, features)
                    write_once(output, result)
                else:
                    study.require(args.prepared and args.prepared_sha256, "explicit prepared pin required")
                    prepared, packets, _, features = study.load_prepared(args.prepared, args.prepared_sha256)
                    study.require(prepared["source_commit"] == head, "prepared source differs")
                    if args.mode == "preflight":
                        result = receipt(
                            dict(
                                status="PASS",
                                source_commit=head,
                                prepared_sha256=args.prepared_sha256,
                                quarters={
                                    k: dict(
                                        rows=len(p["rows"]),
                                        dates=len({r[0] for r in p["rows"]}),
                                        excluded=p["excluded"],
                                        mask_sha256=p["mask_sha256"],
                                    )
                                    for k, p in packets.items()
                                },
                                prediction_dates=len(features["prediction_dates"]),
                                sectors=len(features["catalog"]),
                                fits=0,
                                database_access=False,
                                evaluation_outcomes_read=False,
                                numeric_environment=numeric,
                            )
                        )
                        write_once(output, result)
                    elif args.mode == "fit":
                        result = fit_all(args, output, head, prepared, packets)
                    elif args.mode == "predict":
                        result = predict_all(args, output, head, prepared)
                    else:
                        acceptance, sealed = asset(args, "acceptance"), asset(args, "prediction_acceptance")
                        study.require(
                            acceptance["source_commit"] == sealed["source_commit"] == head, "evaluation source differs"
                        )
                        prediction = study.member.pinned_json(
                            sealed["prediction_path"], sealed["prediction_file_sha256"]
                        )
                        result = study.evaluate(prepared, acceptance, prediction, sealed)
                        write_once(output, result)
        study.require(clean_head() == head, "source changed during execution")
        print(
            f"{args.mode}: {result.get('status', 'COMPLETED')}; fits={result.get('completed_fits', result.get('fits', 0))}"
        )
        return 0
    except Exception as exc:
        if safe:
            write_once(
                failure,
                receipt(
                    dict(
                        status="TECHNICAL_BLOCKED",
                        detail=str(exc),
                        reason_code=getattr(exc, "reason_code", None),
                        automatic_retry=False,
                        database_write=False,
                        dataset_write=False,
                        service_control=False,
                    )
                ),
            )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
