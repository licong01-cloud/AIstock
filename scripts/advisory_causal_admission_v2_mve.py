#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, NoReturn, Sequence

from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.advisory_model_first.causal_admission_v2_pipeline import (  # noqa: E402
    inspect_causal_admission_bundle,
    prepare_causal_admission_request,
    run_causal_admission_mve,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError  # noqa: E402


class AdvisoryCausalAdmissionArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise AdvisoryCausalAdmissionArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(description="Development-only Advisory causal Admission v2.1 learnability audit")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--parent-v1-bundle", required=True)
    prepare.add_argument("--repository-root", required=True)
    prepare.add_argument("--output-root", required=True)
    prepare.add_argument("--output", required=True)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True)
    return parser


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "prepare":
        request = prepare_causal_admission_request(
            parent_v1_bundle_path=args.parent_v1_bundle,
            repository_root=args.repository_root,
            output_root=args.output_root,
            request_path=args.output,
        )
        return {
            "status": "FROZEN_REQUEST",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "repository_commit": request.repository_commit,
            "parent_v1_bundle_id": request.parent_v1_bundle_id,
            "planned_trial_count": request.planned_trial_count,
            "candidate_indices": [arm.candidate_index for arm in request.arms],
            "pre_run_mde_bps": request.pre_run_mde_bps,
            "confirmatory_capable": request.confirmatory_capable,
            "sealed_holdout_accessed": False,
            "runtime_activation": False,
            "database_write": False,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "run":
        return run_causal_admission_mve(args.request)
    if args.command == "inspect":
        return inspect_causal_admission_bundle(args.bundle)
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (AdvisoryCausalAdmissionArgumentError, ValidationError, ValueError, KeyError) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_CAUSAL_REQUEST_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_CAUSAL_UNEXPECTED_FAILURE",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    else:
        exit_code = 0
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
