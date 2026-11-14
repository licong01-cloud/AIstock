"""Approved frozen earnings increment: complete preflight, 12 fits, seal, value."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import risk_l2_earnings_information as h  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
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
    h.require(path is not None and pin, "explicit immutable path/pin required")
    return h.core.inputs._asset(str(path), pin)


def preflight(args, head):
    p = asset(args.preflight, args.preflight_sha256)
    h.require(
        p["schema_version"] == h.VERSION + "_preflight"
        and p["contract"] == h.CONTRACT
        and p["status"] == "PASS"
        and p["source_commit"] == head
        and p["fits"] == 0
        and p["evaluation_outcomes_read"] is False
        and p["attempts"] == h.attempts()
        and Path(p["fit_ledger_root"]).resolve() == Path(h.FIT_LEDGER_ROOT).resolve(),
        "preflight differs",
    )
    return p


def packets(p, head):
    h.require(set(p["packets"]) == {v["id"] for v in h.prior.VINTAGES}, "quarter packets incomplete")
    wrappers = {q: asset(v["path"], v["sha256"]) for q, v in p["packets"].items()}
    for q, wrapper in wrappers.items():
        h.require(
            wrapper["source_commit"] == head
            and wrapper["vintage"]["id"] == q
            and wrapper["feature_sha256"] == p["feature_sha256"],
            "TRAIN packet swapped",
        )
        h.load_packet(wrapper)
    extra = asset(p["feature_path"], p["feature_sha256"])
    features = asset(p["prediction_features_path"], p["prediction_features_sha256"])
    h.require(extra["contract"] == h.CONTRACT and extra["evaluation_outcomes_read"] is False, "feature carrier differs")
    h.prior.validate_prediction_features(features)
    return wrappers, extra, features


def child(mode, arguments, target, head, log):
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
                str(target),
            ],
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            check=False,
        )
    h.require(run.returncode == 0, f"earnings {mode} failed; no retry; see {log}")
    h.require(clean_head() == head, "source changed during child")


def run(args, p, output, head):
    wrappers, _, _ = packets(p, head)  # Every quarter before the first attempt.
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
    output.mkdir(parents=True)
    common = ["--preflight", str(args.preflight), "--preflight-sha256", args.preflight_sha256]
    groups, paths = [], []
    for number in (1, 2):
        target = output / f"process_{number}.json"
        child("fit-child", [*common, "--process", str(number)], target, head, output / f"fit_{number}.log")
        groups.append(read_json(target)["quarters"])
    for a in p["attempts"]:
        h.require(
            (ledger / f"process_{a['process']}" / (a["quarter"] + "." + a["arm"] + ".json")).is_file(),
            "attempt ledger incomplete",
        )
    accepted = h.close_fits(groups, wrappers, p, head)
    acceptance_path = output / "acceptance.json"
    write_once(acceptance_path, accepted)
    for number in (1, 2):
        target = output / f"predictions_{number}.json"
        child(
            "predict-child",
            [*common, "--acceptance", str(acceptance_path), "--acceptance-sha256", accepted["receipt_sha256"]],
            target,
            head,
            output / f"predict_{number}.log",
        )
        paths.append(target)
    h.require(paths[0].read_bytes() == paths[1].read_bytes(), "fresh prediction bytes differ")
    prediction = read_json(paths[0])
    result = receipt(
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
    write_once(output / "prediction_acceptance.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run", "fit-child", "predict-child", "evaluate"))
    for name in ("request", "preflight", "acceptance", "sealed"):
        parser.add_argument("--" + name, type=Path)
        parser.add_argument("--" + name + "-sha256")
    parser.add_argument("--process", type=int, choices=(1, 2))
    parser.add_argument("--executor-commit")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = validate_output_location(args.output)
    failure = output.with_name(output.name + ".failure.json")
    h.require(not output.exists() and not failure.exists(), "output/failure exists; no overwrite or retry")
    head = clean_head()
    try:
        numeric = environment()
        h.prior.validate_numeric(numeric)
        if args.mode.endswith("-child"):
            h.require(args.executor_commit == head, "child source differs")
        with guard(allow_ridge=args.mode == "fit-child"):
            if args.mode == "prepare":
                h.require(args.request and args.request_sha256, "explicit approved request required")
                result = h.prepare(args.request, args.request_sha256, output, head)
            else:
                p = preflight(args, head)
                if args.mode == "run":
                    result = run(args, p, output, head)
                elif args.mode == "fit-child":
                    h.require(args.process in (1, 2), "formal process number required")
                    wrappers, _, _ = packets(p, head)
                    ledger = Path(p["fit_ledger_root"])
                    claim = read_json(ledger / "claim.json")
                    h.core.verify_receipt(claim)
                    h.require(
                        claim
                        == receipt(
                            dict(
                                contract=h.CONTRACT,
                                source_commit=head,
                                request_sha256=p["request_sha256"],
                                preflight_sha256=p["receipt_sha256"],
                                attempts=p["attempts"],
                            )
                        ),
                        "formal budget claim differs",
                    )
                    result = receipt(
                        dict(
                            quarters={
                                v["id"]: h.fit_packet(
                                    wrappers[v["id"]], ledger / f"process_{args.process}", head, numeric
                                )
                                for v in h.prior.VINTAGES
                            },
                            source_commit=head,
                            fits=6,
                            evaluation_outcomes_read=False,
                        )
                    )
                    write_once(output, result)
                elif args.mode == "predict-child":
                    accepted = asset(args.acceptance, args.acceptance_sha256)
                    h.require(accepted["preflight_sha256"] == p["receipt_sha256"], "prediction preflight differs")
                    result = h.predict(
                        accepted,
                        asset(p["prediction_features_path"], p["prediction_features_sha256"]),
                        asset(p["feature_path"], p["feature_sha256"]),
                        head,
                    )
                    write_once(output, result)
                else:
                    sealed, accepted = (
                        asset(args.sealed, args.sealed_sha256),
                        asset(args.acceptance, args.acceptance_sha256),
                    )
                    prediction = asset(sealed["prediction_path"], sealed["prediction_sha256"])
                    h.require(
                        hashlib.sha256(Path(sealed["prediction_path"]).read_bytes()).hexdigest()
                        == sealed["prediction_file_sha256"],
                        "prediction file SHA differs",
                    )
                    result = h.evaluate(p, accepted, prediction, sealed, head)
                    write_once(output, result)
        print(
            canonical_json_bytes(
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
