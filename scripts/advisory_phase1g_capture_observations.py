#!/usr/bin/env python3
"""Plan, capture and verify Phase 1G historical advisory observations."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
from typing import Any

import psycopg2  # noqa: F401  # Kept for the existing CLI offline-verification contract.

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.advisory_phase0a.policy import (
    canonical_json_text,
)
from backend.services.advisory_phase1.phase1g_command_factory import (
    build_phase1g_command_context,
    verify_phase1g_attempt_database,
    verify_phase1g_target_attempt_database,
)
from backend.services.advisory_phase1.phase1g_contract import (
    ATTEMPT_RECEIPT_SCHEMA_VERSION,
    BATCH_RECEIPT_SCHEMA_VERSION,
    Phase1GAttemptReceipt,
    Phase1GBatchAttemptReceipt,
    Phase1GCaptureResult,
    Phase1GExecutionBatchPlan,
    Phase1GExecutionBatchRequest,
    Phase1GOutputArtifactKind,
)
from backend.services.advisory_phase1.phase1g_result_store import Phase1GResultStore
from backend.services.advisory_phase1.phase1g_schema_guard import (
    Phase1GExactTargetConnectionResolver,
)
from backend.services.advisory_phase1.phase1g_service import (
    Phase1GExitClass,
    Phase1GService,
)
from backend.services.advisory_phase1.release_schema_contract import TargetLabel


LOGGER = logging.getLogger("advisory_phase1g_capture_observations")
EXIT_SUCCESS = 0
EXIT_COMMAND_ERROR = 2
EXIT_TARGET_FAILURE = 3
EXIT_RECEIPT_INCOMPLETE = 4
EXIT_INTERNAL = 70


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Phase 1G historical advisory observation capture"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan")
    plan.add_argument("--batch-request", required=True, type=Path)
    _add_execution_roots(plan)

    capture = subparsers.add_parser("capture")
    capture.add_argument("--plan", required=True, type=Path)
    _add_execution_roots(capture)

    verify_result = subparsers.add_parser("verify-result")
    verify_result.add_argument("--result", required=True, type=Path)

    verify_attempt = subparsers.add_parser("verify-attempt")
    verify_attempt.add_argument("--attempt", required=True, type=Path)
    verify_attempt.add_argument("--db-readback", action="store_true")
    verify_attempt.add_argument("--result-root", type=Path)
    verify_attempt.add_argument("--env-file", type=Path)
    verify_attempt.add_argument("--target-db", choices=("dev", "production"))
    return parser


def _add_execution_roots(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--release-receipt-root", required=True, type=Path)
    parser.add_argument("--phase1e-artifact-root", required=True, type=Path)
    parser.add_argument("--result-root", required=True, type=Path)
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--target-db", required=True, choices=("dev", "production"))


def _read_document(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_file():
        raise ValueError("input JSON path is not a regular file")
    document = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("input JSON must be one object")
    return document


def _target_label(value: str) -> TargetLabel:
    return TargetLabel.DEV if value == "dev" else TargetLabel.PRODUCTION


def _service(args: argparse.Namespace) -> Phase1GService:
    target_label = _target_label(args.target_db)
    return build_phase1g_command_context(
        env_file=args.env_file,
        target_label=target_label,
        release_receipt_root=args.release_receipt_root,
        phase1e_artifact_root=args.phase1e_artifact_root,
        result_root=args.result_root,
    ).service


def _emit(document: Any) -> None:
    if hasattr(document, "model_dump"):
        document = document.model_dump(mode="json")
    print(canonical_json_text(document))


def _plan(args: argparse.Namespace) -> int:
    request = Phase1GExecutionBatchRequest.model_validate(
        _read_document(args.batch_request)
    )
    result = _service(args).plan_batch(request)
    _emit(result)
    return EXIT_SUCCESS


def _capture(args: argparse.Namespace) -> int:
    plan = Phase1GExecutionBatchPlan.model_validate(_read_document(args.plan))
    result = _service(args).capture_batch(plan)
    _emit(result)
    return _capture_exit_code(result)


def _capture_exit_code(result: Any) -> int:
    reasons = {
        reason
        for outcome in result.target_outcomes
        for reason in outcome.reason_codes
    } | set(getattr(result, "reason_codes", ()))
    if any(reason.endswith("UNEXPECTED_ERROR") for reason in reasons):
        return EXIT_INTERNAL
    if result.exit_class is Phase1GExitClass.INFRASTRUCTURE_FAILURE:
        return EXIT_RECEIPT_INCOMPLETE
    if result.exit_class is Phase1GExitClass.SUCCESS:
        return EXIT_SUCCESS
    if result.exit_class is Phase1GExitClass.PARTIAL_FAILURE:
        return EXIT_TARGET_FAILURE
    return EXIT_TARGET_FAILURE


def _verify_result(args: argparse.Namespace) -> int:
    result = Phase1GCaptureResult.model_validate(_read_document(args.result))
    _emit(
        {
            "ok": True,
            "artifact_kind": Phase1GOutputArtifactKind.CAPTURE_RESULT.value,
            "capture_result_hash": result.capture_result_hash,
        }
    )
    return EXIT_SUCCESS


def _verify_attempt(args: argparse.Namespace) -> int:
    document = _read_document(args.attempt)
    schema_version = document.get("schema_version")
    if schema_version == ATTEMPT_RECEIPT_SCHEMA_VERSION:
        receipt: Phase1GAttemptReceipt | Phase1GBatchAttemptReceipt = (
            Phase1GAttemptReceipt.model_validate(document)
        )
        identity = receipt.attempt_receipt_hash
        kind = Phase1GOutputArtifactKind.ATTEMPT_RECEIPT
    elif schema_version == BATCH_RECEIPT_SCHEMA_VERSION:
        receipt = Phase1GBatchAttemptReceipt.model_validate(document)
        identity = receipt.batch_attempt_receipt_hash
        kind = Phase1GOutputArtifactKind.BATCH_RECEIPT
    else:
        raise ValueError("attempt schema_version is not registered")
    db_args = (args.result_root, args.env_file, args.target_db)
    if args.db_readback != all(item is not None for item in db_args):
        raise ValueError(
            "--db-readback requires result-root, env-file and target-db together"
        )
    if not args.db_readback and any(item is not None for item in db_args):
        raise ValueError(
            "result-root, env-file and target-db require --db-readback"
        )
    if args.db_readback:
        _verify_attempt_db(receipt=receipt, args=args)
    _emit(
        {
            "ok": True,
            "artifact_kind": kind.value,
            "identity": identity,
            "db_readback": bool(args.db_readback),
        }
    )
    return EXIT_SUCCESS


def _verify_attempt_db(
    *,
    receipt: Phase1GAttemptReceipt | Phase1GBatchAttemptReceipt,
    args: argparse.Namespace,
) -> None:
    result_store = Phase1GResultStore(root=args.result_root)
    target_label = _target_label(args.target_db)
    config = Phase1GExactTargetConnectionResolver(
        env_file=args.env_file
    ).resolve(target_label=target_label)
    verify_phase1g_attempt_database(
        receipt=receipt,
        result_store=result_store,
        connection_config=config,
    )


def _verify_target_attempt_db(
    receipt: Phase1GAttemptReceipt,
    result_store: Phase1GResultStore,
    connect_kwargs: dict[str, Any],
) -> None:
    verify_phase1g_target_attempt_database(
        receipt=receipt,
        result_store=result_store,
        connect_kwargs=connect_kwargs,
    )


def _error_document(command: str, exc: Exception) -> dict[str, Any]:
    reason = str(getattr(exc, "reason_code", "ADVISORY_PHASE1G_COMMAND_INVALID"))
    context = getattr(exc, "context", None)
    safe_context = None
    if isinstance(context, dict):
        allowed = {
            "artifact_kind",
            "capacity_status",
            "cause_reason_code",
            "conflict_kind",
            "exception_type",
            "field_name",
            "phase1e_reason_codes",
            "source_readiness",
            "target_label",
        }
        safe_context = {
            key: value for key, value in context.items() if key in allowed
        }
        failures = context.get("target_failures")
        if isinstance(failures, (list, tuple)):
            failure_fields = allowed | {"reason_code", "target_request_hash"}
            safe_context["target_failures"] = [
                {
                    key: value
                    for key, value in failure.items()
                    if key in failure_fields
                }
                for failure in failures
                if isinstance(failure, dict)
            ]
        safe_context = safe_context or None
    return {
        "ok": False,
        "command": command,
        "error_code": reason,
        "reason_code": reason,
        "message": str(exc),
        "context": safe_context,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        if args.command == "plan":
            return _plan(args)
        if args.command == "capture":
            return _capture(args)
        if args.command == "verify-result":
            return _verify_result(args)
        return _verify_attempt(args)
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        LOGGER.error(
            "phase1g command rejected command=%s exception_type=%s",
            args.command,
            type(exc).__name__,
        )
        _emit(_error_document(args.command, exc))
        return EXIT_COMMAND_ERROR
    except Exception as exc:
        reason = str(getattr(exc, "reason_code", "ADVISORY_PHASE1G_UNEXPECTED_ERROR"))
        if reason == "ADVISORY_PHASE1G_UNEXPECTED_ERROR":
            LOGGER.exception("phase1g command failed unexpectedly command=%s", args.command)
            exit_code = EXIT_INTERNAL
        else:
            LOGGER.error(
                "phase1g command failed command=%s reason_code=%s exception_type=%s",
                args.command,
                reason,
                type(exc).__name__,
            )
            exit_code = EXIT_COMMAND_ERROR
        _emit(_error_document(args.command, exc))
        return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
