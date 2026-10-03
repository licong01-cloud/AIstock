"""Current formal 5184-fit executor; no retired B3/P6/D1 CLI compatibility."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

# These are applied before importing any numerical package in each fresh process.
for variable in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[variable] = "1"
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk.formal_state_executor import (  # noqa: E402
    load_request,
    load_effect_request,
    effect_repeat,
    l2_research_readback,
    read_json,
    prepare_effect_request,
    receipt,
    run_two_processes,
    train_repeat,
    validate_output_location,
    write_once,
)
from backend.services.hmm_risk.contracts import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.formal_state_model import FormalStateError  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        choices=("prepare", "preflight", "run", "child", "l2-readback", "effect-prepare", "effect-run", "effect-child"),
    )
    parser.add_argument("--request", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--security-identity-manifest", type=Path)
    parser.add_argument("--provider-absence-manifest", type=Path)
    parser.add_argument("--industry-authority", type=Path)
    parser.add_argument("--work-parent", type=Path)
    parser.add_argument("--producer-commit")
    parser.add_argument("--original-acceptance", type=Path)
    parser.add_argument("--research-readback", type=Path)
    parser.add_argument("--original-child", type=Path)
    parser.add_argument("--request-sha256")
    args = parser.parse_args()
    if (args.mode in ("l2-readback", "effect-prepare")) != (args.original_acceptance is not None):
        parser.error("l2-readback/effect-prepare require --original-acceptance")
    if (args.mode == "effect-prepare") != (args.research_readback is not None and args.original_child is not None):
        parser.error("effect-prepare requires --research-readback and --original-child")
    if args.mode != "effect-prepare" and (args.research_readback is not None or args.original_child is not None):
        parser.error("historical model arguments are only allowed for effect-prepare")
    if (args.mode == "effect-child") != (args.request_sha256 is not None):
        parser.error("effect-child requires the parent's --request-sha256")
    source_args = (
        args.candidate_root,
        args.security_identity_manifest,
        args.provider_absence_manifest,
        args.industry_authority,
        args.work_parent,
        args.producer_commit,
    )
    if args.mode == "effect-prepare":
        if (
            args.request is None
            or args.industry_authority is not None
            or any(
                value is None
                for value in (
                    args.candidate_root,
                    args.security_identity_manifest,
                    args.provider_absence_manifest,
                    args.work_parent,
                    args.producer_commit,
                )
            )
        ):
            parser.error(
                "effect-prepare requires original --request and five explicit source arguments; no replacement industry authority"
            )
    elif args.mode == "prepare":
        if args.request is not None or any(value is None for value in source_args):
            parser.error("prepare requires all explicit source arguments and no --request")
    elif args.request is None or any(value is not None for value in source_args):
        parser.error("executor modes require --request and no preparation arguments")
    # An unsafe failure path must never be used to report its own rejection.
    # Location rejection is stderr-only and happens before output/temp creation.
    try:
        args.output = validate_output_location(args.output, dataset_root=args.candidate_root)
        if args.mode in ("prepare", "effect-prepare"):
            args.work_parent = validate_output_location(args.work_parent, dataset_root=args.candidate_root)
    except Exception as exc:
        print(f"unsafe output rejected without writing: {exc}", file=sys.stderr)
        return 1
    try:
        if args.mode == "effect-prepare":
            head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
            if head != args.producer_commit:
                raise FormalStateError(
                    "hmm_risk_l2_effect_identity_mismatch", "source commit differs from the executing worktree"
                )
            result = prepare_effect_request(
                original_request=args.request,
                original_acceptance=args.original_acceptance,
                research=args.research_readback,
                original_child=args.original_child,
                source={
                    "candidate_root": str(args.candidate_root),
                    "security_identity_manifest": str(args.security_identity_manifest),
                    "provider_absence_manifest": str(args.provider_absence_manifest),
                    "work_parent": str(args.work_parent),
                    "producer_commit": args.producer_commit,
                },
            )
            write_once(args.output, result)
            load_effect_request(args.output, expected_sha256=result["receipt_sha256"])
            print(f"L2 effect inputs prepared/read back: {args.output}; fits=0; targets not accessed")
        elif args.mode == "effect-run":
            print(run_two_processes(args.request, args.output, Path(__file__), effect=True))
        elif args.mode == "effect-child":
            request = load_effect_request(args.request, expected_sha256=args.request_sha256)
            head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
            if head != request["source"]["producer_commit"]:
                raise FormalStateError("hmm_risk_l2_effect_identity_mismatch", "fresh child source commit differs")
            write_once(args.output, effect_repeat(request))
        elif args.mode == "prepare":
            from backend.services.hmm_risk.formal_state_input import prepare_file_request

            result = prepare_file_request(
                candidate_root=args.candidate_root,
                security_identity_manifest=args.security_identity_manifest,
                provider_absence_manifest=args.provider_absence_manifest,
                industry_authority=read_json(args.industry_authority),
                work_parent=args.work_parent,
                producer_commit=args.producer_commit,
            )
            write_once(args.output, result)
            load_request(args.output)
            print(f"request prepared and read back: {args.output}; fits=0")
        elif args.mode == "run":
            print(run_two_processes(args.request, args.output, Path(__file__)))
        elif args.mode == "l2-readback":
            request = load_request(args.request)
            original = read_json(args.original_acceptance)
            result = l2_research_readback(request, original)
            write_once(args.output, result)
            if canonical_json_bytes(read_json(args.output)) != canonical_json_bytes(
                l2_research_readback(request, original)
            ):
                raise FormalStateError("hmm_risk_formal_output_readback_failed", "L2 research durable readback differs")
            print(f"L2 research readback: {result['semantic_evidence_valid_count']}/131; fits=0; ready=false")
        else:
            request = load_request(args.request)
            result = (
                train_repeat(request)
                if args.mode == "child"
                else receipt(
                    {
                        "status": "preflight_passed",
                        "request_sha256": request["receipt_sha256"],
                        "fits": 0,
                        "database_write": False,
                        "runtime_action": False,
                    }
                )
            )
            write_once(args.output, result)
    except Exception as exc:
        failure = args.output.with_name(args.output.name + ".failure.json")
        write_once(
            failure,
            receipt(
                {
                    "status": "failed",
                    "reason": getattr(exc, "reason_code", "hmm_risk_formal_execution_failed"),
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                    "database_write": False,
                    "runtime_action": False,
                    "ready": False,
                }
            ),
        )
        print(f"failed: {exc}; receipt={failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
