#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError  # noqa: E402
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import (  # noqa: E402
    inspect_qe_alpha_mve_bundle,
    prepare_qe_alpha_mve_request,
    run_qe_alpha_mve,
)


class AdvisoryQEAlphaMVEArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise AdvisoryQEAlphaMVEArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(description="Development-only Advisory N3 upstream QE alpha MVE")
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser("prepare")
    prepare.add_argument("--n1-bundle", required=True)
    prepare.add_argument("--n2a-bundle", required=True)
    prepare.add_argument("--n2b-bundle", required=True)
    prepare.add_argument("--n2-action-bundle", required=True)
    prepare.add_argument("--exit-learnability-bundle", required=True)
    prepare.add_argument("--preparation", required=True)
    prepare.add_argument("--factor-root", required=True)
    prepare.add_argument("--qlib-daily-root", required=True)
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
        request = prepare_qe_alpha_mve_request(
            n1_bundle_path=args.n1_bundle,
            n2a_bundle_path=args.n2a_bundle,
            n2b_bundle_path=args.n2b_bundle,
            n2_action_bundle_path=args.n2_action_bundle,
            exit_learnability_bundle_path=args.exit_learnability_bundle,
            preparation_path=args.preparation,
            factor_root=args.factor_root,
            qlib_daily_root=args.qlib_daily_root,
            repository_root=args.repository_root,
            output_root=args.output_root,
            output_path=args.output,
        )
        return {
            "status": "FROZEN_REQUEST",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "route_receipt_id": request.route_receipt.receipt_id,
            "selected_route": request.route_receipt.selected_route,
            "repository_commit": request.repository_commit,
            "planned_trial_count": request.planned_trial_count,
            "generated_trial_count": request.generated_trial_count,
            "evaluated_trial_count": request.evaluated_trial_count,
            "selected_trial_count": request.selected_trial_count,
            "sealed_holdout_accessed": False,
            "deployable": False,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "run":
        return run_qe_alpha_mve(args.request)
    if args.command == "inspect":
        return inspect_qe_alpha_mve_bundle(args.bundle)
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (AdvisoryQEAlphaMVEArgumentError, ValidationError, ValueError, KeyError) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_QE_ALPHA_MVE_UNEXPECTED_FAILURE",
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
