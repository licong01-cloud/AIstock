#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from dotenv import load_dotenv
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError  # noqa: E402
from backend.services.advisory_model_first.tier1_oracle_pipeline import (  # noqa: E402
    freeze_canonical_pit_snapshot,
    inspect_n1_bundle,
    prepare_n1_tier1_request,
    run_n1_tier1_pipeline,
)


class AdvisoryN1ArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise AdvisoryN1ArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(description="Development-only Advisory N1 Tier-1 oracle and learnability audit")
    commands = parser.add_subparsers(dest="command", required=True)

    pit = commands.add_parser("freeze-pit-snapshot")
    pit.add_argument("--env-file", required=True)
    pit.add_argument("--output", required=True)

    prepare = commands.add_parser("prepare-request")
    prepare.add_argument("--n0-completion", required=True)
    prepare.add_argument("--research-window-contract", required=True)
    prepare.add_argument("--registry", required=True)
    prepare.add_argument("--route", required=True)
    prepare.add_argument("--policy-dataset-bundle-root", required=True)
    prepare.add_argument("--feature-reference-request", required=True)
    prepare.add_argument("--pit-snapshot", required=True)
    prepare.add_argument("--repository-root", required=True)
    prepare.add_argument("--output-root", required=True)
    prepare.add_argument("--output", required=True)

    run = commands.add_parser("run")
    run.add_argument("--request", required=True)

    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True)
    return parser


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "freeze-pit-snapshot":
        env_file = Path(args.env_file)
        if not env_file.is_file():
            raise AdvisoryN1ArgumentError("the declared DEV env file could not be loaded")
        load_dotenv(env_file, override=False)
        return freeze_canonical_pit_snapshot(output_path=args.output)
    if args.command == "prepare-request":
        request = prepare_n1_tier1_request(
            n0_completion_path=args.n0_completion,
            research_window_contract_path=args.research_window_contract,
            registry_path=args.registry,
            route_path=args.route,
            policy_dataset_bundle_root=args.policy_dataset_bundle_root,
            feature_reference_request_path=args.feature_reference_request,
            pit_snapshot_path=args.pit_snapshot,
            repository_root=args.repository_root,
            output_root=args.output_root,
            output_path=args.output,
        )
        return {
            "status": "FROZEN_REQUEST",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "repository_commit": request.repository_commit,
            "dataset_identity": request.dataset_identity,
            "window_id": request.window_id,
            "sealed_holdout_accessed": False,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "run":
        return run_n1_tier1_pipeline(args.request)
    if args.command == "inspect":
        return inspect_n1_bundle(args.bundle)
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (AdvisoryN1ArgumentError, ValidationError, ValueError, KeyError) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_N1_REQUEST_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_N1_UNEXPECTED_FAILURE",
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
