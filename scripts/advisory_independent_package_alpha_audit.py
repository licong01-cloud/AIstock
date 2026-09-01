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
from backend.services.advisory_model_first.independent_package_alpha_audit_pipeline import (  # noqa: E402
    inspect_independent_package_alpha_audit_bundle,
    prepare_independent_package_alpha_audit_request,
    run_independent_package_alpha_audit,
)


class AdvisoryIndependentPackageAuditArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise AdvisoryIndependentPackageAuditArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(description="Development-only independent StrategyPackage common-window alpha audit")
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser("prepare")
    prepare.add_argument("--env-file", required=True)
    prepare.add_argument("--n1-request", required=True)
    prepare.add_argument("--n1-bundle", required=True)
    prepare.add_argument("--n2a-request", required=True)
    prepare.add_argument("--n2a-bundle", required=True)
    prepare.add_argument("--roster-exclusion-receipt", required=True)
    prepare.add_argument("--repository-root", required=True)
    prepare.add_argument("--output-root", required=True)
    prepare.add_argument("--output", required=True)

    run = commands.add_parser("run")
    run.add_argument("--env-file", required=True)
    run.add_argument("--request", required=True)

    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True)
    return parser


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "prepare":
        _load_declared_env(args.env_file)
        request = prepare_independent_package_alpha_audit_request(
            n1_request_path=args.n1_request,
            n1_bundle_path=args.n1_bundle,
            n2a_request_path=args.n2a_request,
            n2a_bundle_path=args.n2a_bundle,
            roster_exclusion_receipt_path=args.roster_exclusion_receipt,
            repository_root=args.repository_root,
            output_root=args.output_root,
            output_path=args.output,
        )
        return {
            "status": "FROZEN_REQUEST",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "repository_commit": request.repository_commit,
            "arm_ids": list(request.arm_ids),
            "package_ids": [item.package_id for item in request.packages],
            "factor_group_closures": list(request.factor_group_closures),
            "planned_trial_count": 0,
            "sealed_holdout_accessed": False,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "run":
        _load_declared_env(args.env_file)
        return run_independent_package_alpha_audit(args.request)
    if args.command == "inspect":
        return inspect_independent_package_alpha_audit_bundle(args.bundle)
    raise AssertionError(f"unsupported command: {args.command}")


def _load_declared_env(value: str) -> None:
    path = Path(value)
    if not path.is_file():
        raise AdvisoryIndependentPackageAuditArgumentError("the declared DEV env file could not be loaded")
    load_dotenv(path, override=False)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (
        AdvisoryIndependentPackageAuditArgumentError,
        ValidationError,
        ValueError,
        KeyError,
    ) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_PACKAGE_ALPHA_AUDIT_REQUEST_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_PACKAGE_ALPHA_AUDIT_UNEXPECTED_FAILURE",
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
