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
from backend.services.advisory_model_first.qe_alpha_mve_preparation import (  # noqa: E402
    build_default_qe_alpha_mve_preparation,
    inspect_qe_alpha_mve_preparation,
    write_qe_alpha_mve_preparation,
)


class AdvisoryQEAlphaPreparationArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise AdvisoryQEAlphaPreparationArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(description="Build or inspect a preparation-only Advisory QE alpha MVE contract")
    commands = parser.add_subparsers(dest="command", required=True)

    build = commands.add_parser("build")
    build.add_argument("--output", required=True)

    inspect = commands.add_parser("inspect")
    inspect.add_argument("--preparation", required=True)
    return parser


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "build":
        preparation = build_default_qe_alpha_mve_preparation()
        return write_qe_alpha_mve_preparation(args.output, preparation)
    if args.command == "inspect":
        return inspect_qe_alpha_mve_preparation(args.preparation)
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = exc.as_dict()
        exit_code = 1
    except (
        AdvisoryQEAlphaPreparationArgumentError,
        ValidationError,
        ValueError,
        KeyError,
    ) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_QE_ALPHA_PREPARATION_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_QE_ALPHA_PREPARATION_UNEXPECTED_FAILURE",
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
