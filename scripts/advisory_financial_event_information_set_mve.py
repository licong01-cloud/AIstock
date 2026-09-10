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
from backend.services.advisory_model_first.financial_event_information_set_pipeline import (  # noqa: E402
    inspect_financial_event_information_set_bundle,
    prepare_financial_event_information_set_request,
    run_financial_event_information_set_mve,
)


class AdvisoryFinancialEventArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise AdvisoryFinancialEventArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(description="Development-only Advisory N3 financial-event information-set MVE")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--source-bundle", required=True)
    prepare.add_argument("--n2b-bundle", required=True)
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
        request = prepare_financial_event_information_set_request(
            source_bundle_path=args.source_bundle,
            n2b_bundle_path=args.n2b_bundle,
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
            "source_bundle_id": request.source_bundle_id,
            "n2b_bundle_id": request.n2b_bundle_id,
            "n1_bundle_id": request.n1_bundle_id,
            "planned_trial_count": request.planned_trial_count,
            "generated_trial_count": request.generated_trial_count,
            "evaluated_trial_count": request.evaluated_trial_count,
            "selected_trial_count": request.selected_trial_count,
            "selectable_trial_count": request.selectable_trial_count,
            "cumulative_candidate_index": request.cumulative_candidate_index,
            "source_time_quality": "DATE_ONLY_BACKFILLED_NON_VINTAGE",
            "sealed_holdout_accessed": False,
            "deployable": False,
            "final_model_output": False,
            "position_weight_output": False,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "run":
        return run_financial_event_information_set_mve(args.request)
    if args.command == "inspect":
        return inspect_financial_event_information_set_bundle(args.bundle)
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (AdvisoryFinancialEventArgumentError, ValidationError, ValueError, KeyError) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_N3_EVENT_MVE_UNEXPECTED_FAILURE",
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
