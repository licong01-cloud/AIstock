#!/usr/bin/env python3
"""Explicit offline CLI for the approved SW L2 zero-fit development run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from backend.services.hmm_risk.contracts import canonical_json_bytes
from backend.services.hmm_risk.rotation_l2 import build_consumer_artifact, close_processes, run_process
from backend.services.hmm_risk.rotation_l2_input import build_rotation_l2_input_bundle


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid UTF-8 JSON input: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON input is not an object: {path}")
    return value


def _write_once(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"refusing to overwrite artifact: {path}")
    temporary = path.with_suffix(path.suffix + ".tmp")
    if temporary.exists():
        raise FileExistsError(f"temporary artifact already exists: {temporary}")
    temporary.write_bytes(canonical_json_bytes(value) + b"\n")
    temporary.replace(path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--active-profile", type=Path, required=True)
    preflight.add_argument("--dataset-root", type=Path, required=True)
    preflight.add_argument("--source-commit", required=True)
    preflight.add_argument("--security-identity-manifest", type=Path, required=True)
    preflight.add_argument("--security-identity-sha256", required=True)
    preflight.add_argument("--provider-absence-manifest", type=Path, required=True)
    preflight.add_argument("--provider-absence-sha256", required=True)
    preflight.add_argument("--output", type=Path, required=True)

    child = subparsers.add_parser("development-child")
    child.add_argument("--input-bundle", type=Path, required=True)
    child.add_argument("--process-index", type=int, choices=(1, 2), required=True)
    child.add_argument("--output", type=Path, required=True)

    close = subparsers.add_parser("development-close")
    close.add_argument("--input-bundle", type=Path, required=True)
    close.add_argument("--first-report", type=Path, required=True)
    close.add_argument("--second-report", type=Path, required=True)
    close.add_argument("--acceptance-output", type=Path, required=True)
    close.add_argument("--consumer-output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "preflight":
            result = build_rotation_l2_input_bundle(
                active_profile_path=args.active_profile,
                dataset_root=args.dataset_root,
                source_commit=args.source_commit,
                security_identity_manifest_path=args.security_identity_manifest,
                security_identity_sha256=args.security_identity_sha256,
                provider_absence_manifest_path=args.provider_absence_manifest,
                provider_absence_sha256=args.provider_absence_sha256,
            )
            _write_once(args.output, result)
            summary = {"status": "PASS", "input_hash": result["input_hash"], "output": str(args.output)}
        elif args.command == "development-child":
            result = run_process(_read(args.input_bundle), process_index=args.process_index)
            _write_once(args.output, result)
            summary = {
                "status": "PASS",
                "process_index": args.process_index,
                "report_sha256": result["report_sha256"],
                "output": str(args.output),
            }
        else:
            bundle = _read(args.input_bundle)
            acceptance = close_processes(_read(args.first_report), _read(args.second_report), input_bundle=bundle)
            consumer = build_consumer_artifact(acceptance)
            _write_once(args.acceptance_output, acceptance)
            _write_once(args.consumer_output, consumer)
            summary = {
                "status": "PASS",
                "run_id": acceptance["run_id"],
                "effect_status": acceptance["effect_status"],
                "rotation_l2_capability_status": acceptance["rotation_l2_capability_status"],
                "planned_fits": acceptance["planned_fits"],
                "tail_accessed": acceptance["tail_accessed"],
                "acceptance_output": str(args.acceptance_output),
                "consumer_output": str(args.consumer_output),
            }
    except Exception as exc:  # CLI boundary emits one explicit failure and non-zero status.
        failure = {"status": "FAILED", "error_type": type(exc).__name__, "message": str(exc)}
        if hasattr(exc, "reason_code"):
            failure["reason_code"] = str(exc.reason_code)
        if hasattr(exc, "context"):
            failure["context"] = dict(exc.context)
        print(json.dumps(failure, ensure_ascii=False, default=str), file=sys.stderr)
        return 1
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
