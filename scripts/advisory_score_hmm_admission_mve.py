#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, NoReturn, Sequence

from dotenv import dotenv_values
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError  # noqa: E402
from backend.services.advisory_model_first.score_hmm_admission_pipeline import (  # noqa: E402
    freeze_score_hmm_market_pit_snapshot,
    inspect_score_hmm_admission_bundle,
    prepare_score_hmm_admission_request,
    run_score_hmm_admission_mve,
)


class AdvisoryScoreHMMArgumentError(ValueError):
    pass


_CANONICAL_SOURCE_ENV_KEYS = (
    "TDX_DB_HOST",
    "TDX_DB_PORT",
    "TDX_DB_NAME",
    "TDX_DB_USER",
    "TDX_DB_PASSWORD",
)


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise AdvisoryScoreHMMArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(
        description="Development-only Advisory package-score and causal market-HMM admission MVE"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    pit = commands.add_parser("freeze-market-pit")
    pit.add_argument("--env-file", required=True)
    pit.add_argument("--output", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--n1-bundle", required=True)
    prepare.add_argument("--market-warmup-pit-snapshot", required=True)
    prepare.add_argument("--repository-root", required=True)
    prepare.add_argument("--output-root", required=True)
    prepare.add_argument("--output", required=True)
    prepare.add_argument("--auxiliary-route")
    run = commands.add_parser("run")
    run.add_argument("--request", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True)
    return parser


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "freeze-market-pit":
        env_file = Path(args.env_file)
        if not env_file.is_file():
            raise AdvisoryScoreHMMArgumentError("the declared canonical source env file could not be loaded")
        values = dotenv_values(env_file)
        missing = [key for key in _CANONICAL_SOURCE_ENV_KEYS if not str(values.get(key) or "").strip()]
        if missing:
            raise AdvisoryScoreHMMArgumentError(f"the declared canonical source profile is incomplete: {sorted(missing)}")
        for key in _CANONICAL_SOURCE_ENV_KEYS:
            os.environ[key] = str(values[key])
        result = freeze_score_hmm_market_pit_snapshot(output_path=args.output)
        return {**result, "database_target": "PRIMARY_CANONICAL_READ_ONLY"}
    if args.command == "prepare":
        request = prepare_score_hmm_admission_request(
            n1_bundle_path=args.n1_bundle,
            market_warmup_pit_snapshot_path=args.market_warmup_pit_snapshot,
            repository_root=args.repository_root,
            output_root=args.output_root,
            output_path=args.output,
            auxiliary_route_path=args.auxiliary_route,
        )
        return {
            "status": "FROZEN_REQUEST",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "repository_commit": request.repository_commit,
            "n1_bundle_id": request.n1_bundle_id,
            "policy_bundle_id": request.policy_bundle_id,
            "planned_trial_count": request.planned_trial_count,
            "executable_trial_count": request.executable_trial_count,
            "reserved_candidate_indices": list(request.reserved_candidate_indices),
            "pre_run_mde_bps": request.pre_run_mde_bps,
            "sector_arms": "NOT_RUN_SOURCE_UNAVAILABLE",
            "sealed_holdout_accessed": False,
            "deployable": False,
            "runtime_activation": False,
            "position_weight_output": False,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "run":
        return run_score_hmm_admission_mve(args.request)
    if args.command == "inspect":
        return inspect_score_hmm_admission_bundle(args.bundle)
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (AdvisoryScoreHMMArgumentError, ValidationError, ValueError, KeyError) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_SCORE_HMM_REQUEST_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_SCORE_HMM_UNEXPECTED_FAILURE",
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
