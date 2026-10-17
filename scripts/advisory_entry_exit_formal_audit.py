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

from backend.services.advisory_model_first.entry_exit_formal_pipeline import (  # noqa: E402
    inspect_n2_action_audit_bundle,
    prepare_n2_action_audit_request,
    run_n2_action_audit,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError  # noqa: E402


class AdvisoryN2ActionAuditArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise AdvisoryN2ActionAuditArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(description="Development-only Advisory N2 Entry Guard and Exit-label oracle audit")
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser("prepare")
    prepare.add_argument("--env-file", required=True)
    prepare.add_argument("--n1-request", required=True)
    prepare.add_argument("--n1-bundle", required=True)
    prepare.add_argument("--m4-request", required=True)
    prepare.add_argument("--m4-bundle", required=True)
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
        request = prepare_n2_action_audit_request(
            n1_request_path=args.n1_request,
            n1_bundle_path=args.n1_bundle,
            m4_request_path=args.m4_request,
            m4_bundle_path=args.m4_bundle,
            repository_root=args.repository_root,
            output_root=args.output_root,
            output_path=args.output,
        )
        return {
            "status": "FROZEN_REQUEST",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "repository_commit": request.repository_commit,
            "entry_arm_ids": [item.arm_id for item in request.entry_arms],
            "planned_trial_count": request.planned_trial_count,
            "sealed_holdout_accessed": request.sealed_holdout_accessed,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "run":
        _load_declared_env(args.env_file)
        return run_n2_action_audit(args.request)
    if args.command == "inspect":
        return inspect_n2_action_audit_bundle(args.bundle)
    raise AssertionError(f"unsupported command: {args.command}")


def _load_declared_env(value: str) -> None:
    path = Path(value)
    if not path.is_file():
        raise AdvisoryN2ActionAuditArgumentError("the declared DEV env file could not be loaded")
    load_dotenv(path, override=False)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (
        AdvisoryN2ActionAuditArgumentError,
        ValidationError,
        ValueError,
        KeyError,
    ) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_N2_ACTION_REQUEST_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_N2_ACTION_UNEXPECTED_FAILURE",
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
