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
from backend.services.advisory_model_first.qe_advisory_matched_canary import (  # noqa: E402
    build_qe_advisory_matched_canary_report,
    build_qe_exact_retry_preflight,
)


class AdvisoryQECanaryArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise AdvisoryQECanaryArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(
        description="Build read-only Advisory receipts from QE public API responses"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    preflight = commands.add_parser("preflight")
    preflight.add_argument("--task-json", required=True)
    preflight.add_argument("--loop-json", action="append", required=True)
    preflight.add_argument("--profile-status", required=True)

    report = commands.add_parser("report")
    report.add_argument("--observation-json", action="append", required=True)
    return parser


def _read_json_object(path_value: str) -> dict[str, Any]:
    path = Path(path_value).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AdvisoryQECanaryArgumentError(f"JSON root must be an object: {path}")
    return payload


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "preflight":
        receipt = build_qe_exact_retry_preflight(
            _read_json_object(args.task_json),
            [_read_json_object(path) for path in args.loop_json],
            profile_status_marker=args.profile_status,
        )
        return receipt.model_dump(mode="json")
    if args.command == "report":
        report = build_qe_advisory_matched_canary_report(
            [_read_json_object(path) for path in args.observation_json]
        )
        return report.model_dump(mode="json")
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (
        AdvisoryQECanaryArgumentError,
        ValidationError,
        OSError,
        json.JSONDecodeError,
    ) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_QE_CANARY_INPUT_INVALID",
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
