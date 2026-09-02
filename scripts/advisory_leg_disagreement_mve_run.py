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

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError  # noqa: E402
from backend.services.advisory_model_first.leg_disagreement_pipeline import (  # noqa: E402
    inspect_leg_disagreement_bundle,
    prepare_leg_disagreement_request,
    run_leg_disagreement_mve,
)


class AdvisoryLegDisagreementArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise AdvisoryLegDisagreementArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(description="Development-only Advisory N3 leg-disagreement learnability MVE")
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser("prepare")
    prepare.add_argument("--parent-overlay-bundle", required=True)
    prepare.add_argument("--n2a-bundle", required=True)
    prepare.add_argument("--n1-bundle", required=True)
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
        request = prepare_leg_disagreement_request(
            parent_overlay_bundle_path=args.parent_overlay_bundle,
            n2a_bundle_path=args.n2a_bundle,
            n1_bundle_path=args.n1_bundle,
            repository_root=args.repository_root,
            output_root=args.output_root,
            output_path=args.output,
        )
        return {
            "status": "FROZEN_REQUEST",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "repository_commit": request.repository_commit,
            "parent_overlay_bundle_id": request.parent_overlay_bundle_id,
            "n2a_bundle_id": request.n2a_bundle_id,
            "n1_bundle_id": request.n1_bundle_id,
            "planned_trial_count": request.planned_trial_count,
            "generated_trial_count": request.generated_trial_count,
            "evaluated_trial_count": request.evaluated_trial_count,
            "selected_trial_count": request.selected_trial_count,
            "sealed_holdout_accessed": False,
            "deployable": False,
            "final_model_output": False,
            "position_weight_output": False,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "run":
        return run_leg_disagreement_mve(args.request)
    if args.command == "inspect":
        return inspect_leg_disagreement_bundle(args.bundle)
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (AdvisoryLegDisagreementArgumentError, ValidationError, ValueError, KeyError) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_N3_LEG_MVE_REQUEST_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_N3_LEG_MVE_UNEXPECTED_FAILURE",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    else:
        exit_code = 0
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
