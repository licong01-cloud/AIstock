#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Sequence

from pydantic import ValidationError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_ENV_FILE = os.environ.get("AISTOCK_ENV_FILE")
if _ENV_FILE:
    load_dotenv(_ENV_FILE, override=False)

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError  # noqa: E402
from backend.services.advisory_model_first.qe_alpha_generator_pipeline import (  # noqa: E402
    generate_alpha_candidates,
    inspect_generation_bundle,
    inspect_generator_mve_bundle,
    prepare_generator_request,
    run_generator_mve,
    snapshot_dev_factor_catalog,
)


class AdvisoryQEAlphaGeneratorArgumentError(ValueError):
    pass


class _TypedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise AdvisoryQEAlphaGeneratorArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedParser(description="Development-only Advisory N3 QE alpha generator MVE")
    commands = parser.add_subparsers(dest="command", required=True)

    catalog = commands.add_parser("snapshot-catalog")
    catalog.add_argument("--output", required=True)

    prepare = commands.add_parser("prepare")
    prepare.add_argument("--parent-qe-bundle", required=True)
    prepare.add_argument("--parent-overlay-bundle", required=True)
    prepare.add_argument("--minute-bundle", required=True)
    prepare.add_argument("--catalog-snapshot", required=True)
    prepare.add_argument("--repository-root", required=True)
    prepare.add_argument("--output-root", required=True)
    prepare.add_argument("--output", required=True)

    generate = commands.add_parser("generate")
    generate.add_argument("--request", required=True)

    run = commands.add_parser("run")
    run.add_argument("--request", required=True)
    run.add_argument("--generation-bundle", required=True)

    inspect_generation = commands.add_parser("inspect-generation")
    inspect_generation.add_argument("--bundle", required=True)

    inspect_result = commands.add_parser("inspect")
    inspect_result.add_argument("--bundle", required=True)
    return parser


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "snapshot-catalog":
        payload = snapshot_dev_factor_catalog(args.output)
        return {
            "status": "FROZEN_CATALOG_SNAPSHOT",
            "snapshot_id": payload["snapshot_id"],
            "snapshot_sha256": payload["snapshot_sha256"],
            "row_count": payload["row_count"],
            "read_only_transaction": True,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "prepare":
        request = prepare_generator_request(
            parent_qe_bundle_path=args.parent_qe_bundle,
            parent_overlay_bundle_path=args.parent_overlay_bundle,
            minute_bundle_path=args.minute_bundle,
            catalog_snapshot_path=args.catalog_snapshot,
            repository_root=args.repository_root,
            output_root=args.output_root,
            output_path=args.output,
        )
        return {
            "status": "FROZEN_REQUEST",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "repository_commit": request.repository_commit,
            "model": request.model_identity.model,
            "max_generation_calls": request.max_generation_calls,
            "max_raw_generation_attempts": request.max_raw_generation_attempts,
            "max_evaluated_expressions": request.max_evaluated_expressions,
            "sealed_holdout_accessed": False,
            "deployable": False,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "generate":
        return generate_alpha_candidates(args.request)
    if args.command == "run":
        return run_generator_mve(args.request, args.generation_bundle)
    if args.command == "inspect-generation":
        return inspect_generation_bundle(args.bundle)
    if args.command == "inspect":
        return inspect_generator_mve_bundle(args.bundle)
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = _run(_parser().parse_args(argv))
    except AdvisoryModelFirstError as exc:
        result = _redact(exc.as_dict())
        exit_code = 1
    except (AdvisoryQEAlphaGeneratorArgumentError, ValidationError, ValueError, KeyError) as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID",
            "message": _redact_text(str(exc)),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:
        result = {
            "status": "failed",
            "reason_code": "ADVISORY_QE_ALPHA_GENERATOR_UNEXPECTED_FAILURE",
            "message": _redact_text(str(exc)),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    else:
        exit_code = 0
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return exit_code


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: (
                "<redacted>"
                if re.search(r"(?i)(api[_-]?key|authorization|bearer|password|secret)", key)
                else _redact(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return _redact_text(value) if isinstance(value, str) else value


def _redact_text(value: str) -> str:
    return re.sub(
        r"(?i)(api[_-]?key|authorization|bearer|password|secret)\s*[:=]\s*\S+",
        r"\1=<redacted>",
        value,
    )[:1000]


if __name__ == "__main__":
    raise SystemExit(main())
