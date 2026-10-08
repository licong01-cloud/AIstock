"""Approved L2 Ridge preflight / two fresh-process fits; no DB or services."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as model  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import read_json, validate_output_location, write_once  # noqa: E402

THREADS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
)


def parser(*, with_reference: bool = False) -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    modes = result.add_subparsers(dest="mode", required=True)
    prep = modes.add_parser("preflight")
    for flag in ("active-profile", "dataset-root", "security-identity-manifest", "provider-absence-manifest"):
        prep.add_argument("--" + flag, type=Path, required=True)
    for flag in ("security-identity-sha256", "provider-absence-sha256", "approved-manifest-sha256"):
        prep.add_argument("--" + flag, required=True)
    prep.add_argument("--output", type=Path, required=True)
    if with_reference:
        prep.add_argument("--reference-acceptance", type=Path, required=True)
    for mode in ("run", "child"):
        sub = modes.add_parser(mode)
        sub.add_argument("--input-bundle", type=Path, required=True)
        sub.add_argument("--output", type=Path, required=True)
        if mode == "child":
            sub.add_argument("--input-sha256", required=True)
            sub.add_argument("--executor-commit", required=True)
            sub.add_argument("--process-index", type=int, choices=(1, 2), required=True)
    return result


def source_head() -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()


def main(argv: list[str] | None = None, *, engine=None, child_script: Path | None = None) -> int:
    selected_model = engine or model
    args = parser(with_reference=engine is not None).parse_args(argv)
    # Reject unsafe destinations before any write, including a failure receipt.
    try:
        output = validate_output_location(args.output, dataset_root=getattr(args, "dataset_root", None))
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"FAILED output location: {exc}", file=sys.stderr)
        return 1
    completed = 0
    output_authorized = args.mode == "preflight"
    try:
        if args.mode == "preflight":
            profile = read_json(args.active_profile)
            if profile.get("components", {}).get("dataset_manifest_sha256") != args.approved_manifest_sha256:
                raise selected_model.fail("active manifest differs from the explicitly approved input")
            bundle = selected_model.prepare_inputs(
                active_profile_path=args.active_profile,
                dataset_root=args.dataset_root,
                source_commit=source_head(),
                security_identity_manifest_path=args.security_identity_manifest,
                security_identity_sha256=args.security_identity_sha256,
                provider_absence_manifest_path=args.provider_absence_manifest,
                provider_absence_sha256=args.provider_absence_sha256,
                **({"reference_acceptance_path": args.reference_acceptance} if engine is not None else {}),
            )
            write_once(output, bundle)
            print(f"preflight=PASS; input={bundle['input_hash']}; fits=0; tail=false")
        elif args.mode == "child":
            if source_head() != args.executor_commit:
                raise selected_model.fail("fresh child source commit differs")
            bundle = read_json(args.input_bundle)
            if bundle.get("input_hash") != args.input_sha256:
                raise selected_model.fail("fresh child input differs from parent")
            validate_output_location(output, dataset_root=Path(bundle["source"]["evaluation_source_binding"]["root"]))
            output_authorized = True
            report = selected_model.run_process(bundle, process_index=args.process_index)
            completed = 1
            write_once(output, report)
            if read_json(output) != report:
                raise selected_model.fail("child sealed prediction readback differs")
        else:
            bundle = read_json(args.input_bundle)
            selected_model.validate_input(bundle)
            validate_output_location(output, dataset_root=Path(bundle["source"]["evaluation_source_binding"]["root"]))
            output_authorized = True
            head = source_head()
            if (
                head != bundle["source"]["identity"]["source_git_commit"]
                or subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip()
            ):
                raise selected_model.fail("formal run requires clean source pinned to the preflight")
            if output.exists():
                raise selected_model.fail("run output already exists; no automatic retry", "output_collision")
            output.mkdir(parents=True)
            reports = []
            env = {**os.environ, **{key: "1" for key in THREADS}, "PYTHONPATH": str(ROOT)}
            for number in (1, 2):
                child_path = output / f"process_{number}.json"
                with (output / f"process_{number}.log").open("xb") as log:
                    result = subprocess.run(
                        [
                            sys.executable,
                            str(child_script or Path(__file__).resolve()),
                            "child",
                            "--input-bundle",
                            str(args.input_bundle),
                            "--output",
                            str(child_path),
                            "--input-sha256",
                            bundle["input_hash"],
                            "--executor-commit",
                            head,
                            "--process-index",
                            str(number),
                        ],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=False,
                    )
                if result.returncode:
                    raise selected_model.fail(
                        f"fresh child {number} failed with exit {result.returncode}", "child_failed"
                    )
                reports.append(read_json(child_path))
                completed += 1
            # Reject even jointly rehashed child drift before consuming evaluation outcomes.
            selected_model.verify_processes(*reports, input_bundle=bundle)
            # The first evaluation outcome read occurs only after both sealed reports are verified.
            facts = selected_model.read_evaluation_facts(bundle)
            final = selected_model.close_processes(*reports, input_bundle=bundle, facts=facts)
            selected_model.validate_acceptance(final)
            write_once(output / "acceptance.json", final)
            if canonical_json_bytes(read_json(output / "acceptance.json")) != canonical_json_bytes(final):
                raise selected_model.fail("parent acceptance readback differs")
            print(
                f"{final['effect_status']}; fits=2; IC={final['metrics']['overall']['mean_daily_rank_ic']}; output={output}"
            )
    except Exception as exc:
        failure = {
            "status": "FAILED",
            "stage": args.mode,
            "reason_code": getattr(exc, "reason_code", "hmm_risk_rotation_l2_supervised_execution_failed"),
            "cause_type": type(exc).__name__,
            "message": str(exc),
            "completed_fits_known": completed,
            "fit_count_authority": "completed_reports_only",
            "tail_accessed": False,
            "database_write": False,
            "runtime_action": False,
        }
        failure_path = output.with_name(output.name + ".failure.json")
        if output_authorized:
            try:
                write_once(failure_path, failure)
            except (OSError, ValueError) as receipt_error:
                print(f"failure_receipt_unwritten={type(receipt_error).__name__}", file=sys.stderr)
        else:
            print("failure_receipt_unwritten=output_authority_not_validated", file=sys.stderr)
        print(f"FAILED {failure['reason_code']}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
