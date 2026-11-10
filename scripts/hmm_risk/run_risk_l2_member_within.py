"""Approved W study: existing carriers, six isolated fits, no trading policy."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_member_return as h  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from scripts.hmm_risk.run_risk_l2_member_return import environment  # noqa: E402
from scripts.hmm_risk.run_risk_l2_loss_severity import guard  # noqa: E402
from scripts.hmm_risk.run_risk_l2_rolling import clean_head  # noqa: E402


def asset(path, pin):
    h.require(path is not None and pin, "explicit asset path and canonical pin required")
    return h.core.inputs._asset(str(path), pin)


def preflight(args, output, head, numeric):
    h.validate_numeric(numeric)
    for name in ("prepared", "controls", "baseline"):
        supplied = getattr(args, name + "_sha256")
        h.require(
            supplied is None or supplied == h.WITHIN_PINS["predictions" if name == "baseline" else name],
            "W explicitly supplied frozen pin differs",
        )
    p, packets, controls, _, features = h.within_inputs(args.prepared, args.controls, args.baseline)
    wrappers, locators = {}, {}
    for q, packet in packets.items():
        wrapper = receipt(
            dict(
                schema_version=h.WITHIN_VERSION + "_train_reference",
                contract=h.WITHIN_CONTRACT,
                packet_path=p["packets"][q]["path"],
                packet_sha256=packet["receipt_sha256"],
                reference_S=controls["models"][q]["S"],
                source_commit=head,
            )
        )
        path = output / (q + ".json")
        write_once(path, wrapper)
        wrappers[q] = dict(
            rows=len(packet["rows"]),
            dates=len({r[0] for r in packet["rows"]}),
            excluded=packet["excluded"],
            mask_sha256=packet["mask_sha256"],
        )
        locators[q] = dict(path=str(path), sha256=wrapper["receipt_sha256"])
    result = receipt(
        dict(
            schema_version=h.WITHIN_VERSION + "_preflight",
            contract=h.WITHIN_CONTRACT,
            status="PASS",
            source_commit=head,
            prepared_path=str(args.prepared),
            controls_path=str(args.controls),
            baseline_path=str(args.baseline),
            wrappers=locators,
            quarters=wrappers,
            prediction_features_path=p["prediction_features_path"],
            features_sha256=features["receipt_sha256"],
            prediction_dates=len(features["prediction_dates"]),
            sectors=len(features["catalog"]),
            original_training_source=p["source_commit"],
            fits=0,
            evaluation_outcomes_read=False,
            database_access=False,
            numeric_environment=numeric,
        )
    )
    write_once(output / "preflight.json", result)
    return result


def load_preflight(path, pin, head):
    p = asset(path, pin)
    h.require(
        p["schema_version"] == h.WITHIN_VERSION + "_preflight"
        and p["contract"] == h.WITHIN_CONTRACT
        and p["source_commit"] == head
        and p["status"] == "PASS"
        and p["fits"] == 0
        and p["evaluation_outcomes_read"] is False
        and set(p["wrappers"]) == {v["id"] for v in h.VINTAGES},
        "W preflight identity differs",
    )
    return p


def child(mode, arguments, output, head, log):
    env = {**os.environ, **dict.fromkeys(THREAD_VARIABLES, "1"), "PYTHONPATH": str(ROOT), "PYTHONUTF8": "1"}
    with log.open("xb") as stream:
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
            stdout=stream,
            stderr=subprocess.STDOUT,
            check=False,
        )
    h.require(run.returncode == 0, f"W {mode} failed; no retry; see {log}")
    h.require(clean_head() == head, "W source changed during child")


def fit_all(args, output, head, p):
    # Changing the fit output directory cannot create a second budget for this preflight.
    claim = validate_output_location(args.preflight.parent / "formal_fit_attempts")
    write_once(
        claim / "claim.json",
        receipt(
            dict(
                contract=h.WITHIN_CONTRACT,
                source_commit=head,
                preflight_sha256=p["receipt_sha256"],
                authorized_attempts=6,
                inputs={q: v["sha256"] for q, v in p["wrappers"].items()},
            )
        ),
    )
    output.mkdir(parents=True)
    groups, hashes = [], []
    wrappers = {q: asset(v["path"], v["sha256"]) for q, v in p["wrappers"].items()}
    for number in (1, 2):
        group, byte_hashes = {}, {}
        for vintage in h.VINTAGES:
            q = vintage["id"]
            locator = p["wrappers"][q]
            target = output / f"process_{number}" / q
            child(
                "fit-child",
                [
                    "--input",
                    locator["path"],
                    "--input-sha256",
                    locator["sha256"],
                    "--attempt-root",
                    str(claim / f"process_{number}"),
                ],
                target,
                head,
                output / f"process_{number}_{q}.log",
            )
            path = target / "model.json"
            group[q], byte_hashes[q] = read_json(path), hashlib.sha256(path.read_bytes()).hexdigest()
        groups.append(group)
        hashes.append(byte_hashes)
    h.require(hashes[0] == hashes[1], "W fresh model bytes differ")
    result = h.within_close(groups, wrappers, head, p["features_sha256"])
    write_once(output / "acceptance.json", result)
    return result


def predict_all(args, output, head, p):
    acceptance = asset(args.acceptance, args.acceptance_sha256)
    h.within_validate_acceptance(acceptance)
    h.require(acceptance["source_commit"] == head, "W prediction source differs")
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
                p["prediction_features_path"],
                "--features-sha256",
                p["features_sha256"],
            ],
            path,
            head,
            output / f"process_{number}.log",
        )
        paths.append(path)
    h.require(paths[0].read_bytes() == paths[1].read_bytes(), "W fresh prediction bytes differ")
    prediction = read_json(paths[0])
    h.core.verify_receipt(prediction)
    result = receipt(
        dict(
            schema_version=h.WITHIN_VERSION + "_prediction_acceptance",
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
    parser.add_argument("mode", choices=("preflight", "fit", "fit-child", "predict", "predict-child", "evaluate"))
    for name in ("prepared", "controls", "baseline", "preflight", "input", "acceptance", "features", "sealed"):
        parser.add_argument("--" + name, type=Path)
        parser.add_argument("--" + name + "-sha256")
    parser.add_argument("--attempt-root", type=Path)
    parser.add_argument("--executor-commit")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = validate_output_location(args.output)
    failure = output.with_name(output.name + ".failure.json")
    h.require(not output.exists() and not failure.exists(), "W output/failure exists; no overwrite or retry")
    head = clean_head()
    safe = False
    try:
        numeric = environment()
        if args.mode.endswith("-child"):
            h.require(args.executor_commit == head, "W child source differs")
        with guard(allow_ridge=args.mode == "fit-child"):
            safe = True
            if args.mode == "preflight":
                h.require(args.prepared and args.controls and args.baseline, "W explicit frozen paths required")
                result = preflight(args, output, head, numeric)
            elif args.mode == "fit-child":
                wrapper = asset(args.input, args.input_sha256)
                h.require(
                    wrapper["schema_version"] == h.WITHIN_VERSION + "_train_reference"
                    and wrapper["contract"] == h.WITHIN_CONTRACT
                    and wrapper["source_commit"] == head,
                    "W TRAIN wrapper differs",
                )
                packet = asset(wrapper["packet_path"], wrapper["packet_sha256"])
                h.require(args.attempt_root is not None, "W attempt root required")
                attempt = validate_output_location(args.attempt_root)
                claim = read_json(attempt.parent / "claim.json")
                h.core.verify_receipt(claim)
                h.require(
                    claim["contract"] == h.WITHIN_CONTRACT
                    and claim["source_commit"] == head
                    and claim["authorized_attempts"] == 6
                    and attempt.name in {"process_1", "process_2"}
                    and claim["inputs"][packet["vintage"]["id"]] == args.input_sha256,
                    "W unapproved fit ledger",
                )
                result = h.within_fit(packet, wrapper["reference_S"], output, attempt, head, numeric)
            elif args.mode == "predict-child":
                acceptance, features = (
                    asset(args.acceptance, args.acceptance_sha256),
                    asset(args.features, args.features_sha256),
                )
                h.require(acceptance["source_commit"] == head, "W prediction source differs")
                result = h.within_predict(acceptance, features)
                write_once(output, result)
            else:
                p = load_preflight(args.preflight, args.preflight_sha256, head)
                if args.mode == "fit":
                    result = fit_all(args, output, head, p)
                elif args.mode == "predict":
                    result = predict_all(args, output, head, p)
                else:
                    prepared, _, controls, baseline, features = h.within_inputs(
                        p["prepared_path"], p["controls_path"], p["baseline_path"]
                    )
                    acceptance, sealed = (
                        asset(args.acceptance, args.acceptance_sha256),
                        asset(args.sealed, args.sealed_sha256),
                    )
                    h.require(
                        acceptance["source_commit"] == sealed["source_commit"] == head, "W evaluation source differs"
                    )
                    prediction = h.member.pinned_json(sealed["prediction_path"], sealed["prediction_file_sha256"])
                    result = h.within_evaluate(prepared, controls, baseline, features, acceptance, prediction, sealed)
                    write_once(output, result)
        h.require(clean_head() == head, "W source changed during execution")
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
