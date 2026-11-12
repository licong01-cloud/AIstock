"""Approved single financing increment: prepare, 12-fit ceiling, seal, evaluate."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_margin_information as h  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    THREAD_VARIABLES,
    read_json,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.formal_state_model import receipt  # noqa: E402
from scripts.hmm_risk.run_risk_l2_loss_severity import guard  # noqa: E402
from scripts.hmm_risk.run_risk_l2_member_return import environment  # noqa: E402
from scripts.hmm_risk.run_risk_l2_rolling import clean_head  # noqa: E402


def asset(path, pin):
    h.require(path is not None and pin, "explicit immutable path/canonical pin required")
    return h.core.inputs._asset(str(path), pin)


def preflight(args, head):
    p = asset(args.preflight, args.preflight_sha256)
    h.require(
        p["schema_version"] == h.VERSION + "_preflight"
        and p["contract"] == h.CONTRACT
        and p["source_commit"] == head
        and p["status"] == "PASS"
        and p["fits"] == 0
        and p["evaluation_outcomes_read"] is False,
        "preflight source/contract differs",
    )
    expected = [
        dict(process=n, quarter=v["id"], arm=arm)
        for v in h.prior.VINTAGES
        for n in (1, 2)
        for arm in (["M"] if p["training"][v["id"]]["reused_control"] else ["C20", "M"])
    ]
    h.require(p["attempts"] == expected and 6 <= len(expected) <= 12, "preflight attempt list differs")
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
    h.require(run.returncode == 0, f"financing {mode} failed; no retry; see {log}")
    h.require(clean_head() == head, "source changed during child")


def claim_budget(p, head):
    ledger = validate_output_location(Path(p["fit_ledger_root"]))
    write_once(
        ledger / "claim.json",
        receipt(
            dict(
                contract=h.CONTRACT,
                source_commit=head,
                request_sha256=p["request_sha256"],
                preflight_sha256=p["receipt_sha256"],
                attempts=p["attempts"],
            )
        ),
    )
    return ledger


def fit_all(args, p, output, head):
    wrappers = {q: asset(v["path"], v["sha256"]) for q, v in p["packets"].items()}
    finance = asset(p["finance_path"], p["finance_sha256"])
    features = asset(p["prediction_features_path"], p["prediction_features_sha256"])
    h.prior.validate_prediction_features(features)
    h.require(finance["contract"] == h.CONTRACT, "financing carrier contract differs")
    for q, wrapper in wrappers.items():
        h.require(
            wrapper["vintage"]["id"] == q
            and wrapper["source_commit"] == head
            and wrapper["finance_sha256"] == finance["receipt_sha256"],
            "formal quarter input differs",
        )
        h.load_packet(wrapper)  # Validate every quarter before claiming or consuming a fit.
    ledger = claim_budget(p, head)  # Request-bound; a different output directory does not reset this budget.
    output.mkdir(parents=True)
    processes, hashes = [], []
    common = ["--preflight", str(args.preflight), "--preflight-sha256", args.preflight_sha256]
    for number in (1, 2):
        group, byte_hashes = {}, {}
        for vintage in h.prior.VINTAGES:
            q = vintage["id"]
            target = output / f"process_{number}" / q
            child(
                "fit-child",
                [*common, "--process", str(number), "--quarter", q],
                target,
                head,
                output / f"process_{number}_{q}.log",
            )
            path = target / "models.json"
            group[q], byte_hashes[q] = read_json(path), hashlib.sha256(path.read_bytes()).hexdigest()
        processes.append(group)
        hashes.append(byte_hashes)
    h.require(hashes[0] == hashes[1], "fresh model file SHA differs")
    h.require(
        all(
            (ledger / f"process_{a['process']}" / (a["quarter"] + "." + a["arm"] + ".json")).is_file()
            for a in p["attempts"]
        ),
        "formal attempt ledger is incomplete",
    )
    accepted = h.close_fits(processes, wrappers, p, head)
    write_once(output / "acceptance.json", accepted)
    paths = []
    for number in (1, 2):
        target = output / f"predictions_{number}.json"
        child(
            "predict-child",
            [
                *common,
                "--acceptance",
                str(output / "acceptance.json"),
                "--acceptance-sha256",
                accepted["receipt_sha256"],
            ],
            target,
            head,
            output / f"predict_{number}.log",
        )
        paths.append(target)
    h.require(paths[0].read_bytes() == paths[1].read_bytes(), "fresh prediction file SHA differs")
    prediction = read_json(paths[0])
    sealed = receipt(
        dict(
            schema_version=h.VERSION + "_prediction_acceptance",
            status="PASS",
            source_commit=head,
            fresh_processes=2,
            prediction_path=str(paths[0]),
            prediction_sha256=prediction["receipt_sha256"],
            prediction_file_sha256=hashlib.sha256(paths[0].read_bytes()).hexdigest(),
            fit_acceptance_sha256=accepted["receipt_sha256"],
            fits=0,
            evaluation_outcomes_read=False,
        )
    )
    write_once(output / "prediction_acceptance.json", sealed)
    return sealed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run", "fit-child", "predict-child", "evaluate"))
    for name in ("request", "preflight", "acceptance", "sealed"):
        parser.add_argument("--" + name, type=Path)
        parser.add_argument("--" + name + "-sha256")
    parser.add_argument("--quarter", choices=tuple(v["id"] for v in h.prior.VINTAGES))
    parser.add_argument("--process", type=int, choices=(1, 2))
    parser.add_argument("--executor-commit")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = validate_output_location(args.output)
    failure = output.with_name(output.name + ".failure.json")
    h.require(not output.exists() and not failure.exists(), "output/failure already exists; no overwrite/retry")
    head = clean_head()
    safe = False
    try:
        numeric = environment()
        h.prior.validate_numeric(numeric)
        if args.mode.endswith("-child"):
            h.require(args.executor_commit == head, "child source differs")
        with guard(allow_ridge=args.mode == "fit-child"):
            safe = True
            if args.mode == "prepare":
                h.require(args.request and args.request_sha256, "explicit approved request required")
                result = h.prepare(args.request, args.request_sha256, output, head)
            else:
                p = preflight(args, head)
                if args.mode == "run":
                    result = fit_all(args, p, output, head)
                elif args.mode == "fit-child":
                    h.require(args.process and args.quarter, "child quarter/process required")
                    ledger = Path(p["fit_ledger_root"])
                    claim = read_json(ledger / "claim.json")
                    h.core.verify_receipt(claim)
                    h.require(
                        claim["contract"] == h.CONTRACT
                        and claim["source_commit"] == head
                        and claim["preflight_sha256"] == p["receipt_sha256"]
                        and claim["attempts"] == p["attempts"],
                        "formal attempt claim differs",
                    )
                    locator = p["packets"][args.quarter]
                    wrapper = asset(locator["path"], locator["sha256"])
                    h.require(wrapper["vintage"]["id"] == args.quarter, "quarter carrier swapped")
                    result = h.fit_packet(wrapper, output, ledger / f"process_{args.process}", head, numeric)
                elif args.mode == "predict-child":
                    accepted = asset(args.acceptance, args.acceptance_sha256)
                    h.require(accepted["preflight_sha256"] == p["receipt_sha256"], "prediction preflight differs")
                    result = h.predict(
                        accepted,
                        asset(p["prediction_features_path"], p["prediction_features_sha256"]),
                        asset(p["finance_path"], p["finance_sha256"]),
                        head,
                    )
                    write_once(output, result)
                else:
                    sealed = asset(args.sealed, args.sealed_sha256)
                    accepted = asset(args.acceptance, args.acceptance_sha256)
                    prediction = asset(sealed["prediction_path"], sealed["prediction_sha256"])
                    h.require(
                        hashlib.sha256(Path(sealed["prediction_path"]).read_bytes()).hexdigest()
                        == sealed["prediction_file_sha256"],
                        "sealed prediction file SHA differs",
                    )
                    result = h.evaluate(p, accepted, prediction, sealed, head)
                    write_once(output, result)
        print(
            h.canonical_json_bytes(
                dict(
                    mode=args.mode,
                    status=result.get("status", "PASS"),
                    receipt_sha256=result["receipt_sha256"],
                    output=str(output),
                    fits=result.get("fits", 0),
                )
            ).decode()
        )
    except Exception as exc:
        if safe:
            write_once(
                failure,
                receipt(
                    dict(
                        mode=args.mode,
                        status="FAILED",
                        source_commit=head,
                        error_type=type(exc).__name__,
                        message=str(exc),
                        retry_allowed=False,
                    )
                ),
            )
        raise


if __name__ == "__main__":
    main()
