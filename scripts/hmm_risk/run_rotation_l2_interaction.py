"""File-only paired L2 interaction research: two fresh processes, four fits."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk import rotation_l2_interaction as model  # noqa: E402
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as price  # noqa: E402
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_executor import read_json, validate_output_location, write_once  # noqa: E402
from scripts.hmm_risk.run_rotation_l2_moneyflow_supervised import parser  # noqa: E402


def source_head() -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()


def require_clean_source(expected: str) -> None:
    if (
        source_head() != expected
        or subprocess.check_output(["git", "-C", str(ROOT), "status", "--porcelain"], text=True).strip()
    ):
        raise model.fail("formal execution requires clean source pinned to input")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        output = validate_output_location(args.output, dataset_root=getattr(args, "dataset_root", None))
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"FAILED output location: {exc}", file=sys.stderr)
        return 1
    authority = args.mode == "preflight"
    known = 0
    potentially_started = False

    def completed(count):
        nonlocal known
        known = count

    try:
        if args.mode == "preflight":
            profile = read_json(args.active_profile)
            if (
                args.approved_manifest_sha256 != model.APPROVED_INPUT["manifest_sha256"]
                or profile.get("components", {}).get("dataset_manifest_sha256") != args.approved_manifest_sha256
            ):
                raise model.fail("active manifest is not the approved input")
            bundle = model.prepare_inputs(
                active_profile_path=args.active_profile,
                dataset_root=args.dataset_root,
                source_commit=source_head(),
                security_identity_manifest_path=args.security_identity_manifest,
                security_identity_sha256=args.security_identity_sha256,
                provider_absence_manifest_path=args.provider_absence_manifest,
                provider_absence_sha256=args.provider_absence_sha256,
            )
            write_once(output, bundle)
            if read_json(output) != bundle:
                raise model.fail("input write/readback differs")
            print(f"preflight=PASS input={bundle['input_hash']} fits=0 tail=false")
            return 0
        bundle = read_json(args.input_bundle)
        model.validate_input(bundle)
        validate_output_location(output, dataset_root=Path(bundle["source"]["evaluation_source_binding"]["root"]))
        authority = True
        require_clean_source(bundle["source"]["identity"]["source_git_commit"])
        if args.mode == "child":
            if args.executor_commit != source_head() or args.input_sha256 != bundle["input_hash"]:
                raise model.fail("child source or input differs from parent")
            if output.exists():
                raise model.fail("child destination exists", "output_collision")
            potentially_started = True
            report = model.run_process(bundle, process_index=args.process_index, on_completed=completed)
            write_once(output, report)
            if read_json(output) != report:
                raise model.fail("child write/readback differs")
            return 0
        if output.exists():
            raise model.fail("run destination exists; no retry", "output_collision")
        output.mkdir(parents=True)
        env = {**os.environ, **{k: "1" for k in price.THREADS}, "PYTHONPATH": str(ROOT)}
        reports = []
        for i in (1, 2):
            target = output / f"process_{i}.json"
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "child",
                "--input-bundle",
                str(args.input_bundle),
                "--output",
                str(target),
                "--process-index",
                str(i),
                "--executor-commit",
                source_head(),
                "--input-sha256",
                bundle["input_hash"],
            ]
            potentially_started = True
            with (output / f"process_{i}.log").open("xb") as log:
                result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
            if result.returncode:
                raise model.fail(f"fresh process {i} failed; inspect its typed failure", "child_failed")
            reports.append(read_json(target))
            known += 2
        # This is the only route to an evaluation read. Self-hashed child drift
        # is rejected against parent training/parameters/predictions first.
        model.verify_processes(*reports, input_bundle=bundle)
        facts = model.read_evaluation_facts(bundle)
        final = model.close_processes(*reports, input_bundle=bundle, facts=facts)
        model.validate_acceptance(final)
        target = output / "acceptance.json"
        write_once(target, final)
        if canonical_json_bytes(read_json(target)) != canonical_json_bytes(final):
            raise model.fail("acceptance write/readback differs")
        print(f"{final['effect_status']} fits=4 tail=false acceptance={final['acceptance_sha256']} output={output}")
    except Exception as exc:
        receipt = {
            "status": "FAILED",
            "stage": args.mode,
            "reason_code": getattr(exc, "reason_code", "hmm_risk_rotation_l2_interaction_execution_failed"),
            "cause_type": type(exc).__name__,
            "message": str(exc),
            "completed_fits_known": known,
            "remaining_fit_state": "UNKNOWN_IF_CHILD_OR_FIT_STARTED" if potentially_started else "NOT_STARTED",
            "tail_accessed": False,
            "database_write": False,
            "runtime_action": False,
        }
        if authority:
            try:
                write_once(output.with_name(output.name + ".failure.json"), receipt)
            except (OSError, ValueError) as error:
                print(f"failure_receipt_unwritten={type(error).__name__}", file=sys.stderr)
        print(f"FAILED {receipt['reason_code']}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
