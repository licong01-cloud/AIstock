#!/usr/bin/env python3
"""Inventory and validate Phase 1G G5 DEV evidence."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
import traceback
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.advisory_phase0a.policy import canonical_json_text
from backend.services.advisory_phase1.phase1g_dev_evidence import (
    Phase1GDevEvidenceService,
    verify_g5_reference_closure,
)
from backend.services.advisory_phase1.phase1g_dev_evidence_contract import (
    EvidenceKind,
    InventoryStatus,
    PersistentStatus,
    Phase1GDevEvidenceError,
    Phase1GDevEvidenceRef,
    Phase1GDevExecutionManifest,
    Phase1GDevInputInventoryReceipt,
    Phase1GDevPersistentReceipt,
    Phase1GDevRollbackReceipt,
    REASON_EVIDENCE_STORE_FAILED,
    REASON_L3_CONCURRENCY_FAILED,
    REASON_L3_COORDINATOR_INVALID,
    REASON_L3_FORBIDDEN_SQL,
    REASON_L3_RESIDUE_DETECTED,
    REASON_L3_ROLLBACK_FAILED,
    REASON_L3_SOURCE_PENDING,
    REASON_L4_PLAN_STALE,
    REASON_L4_PARTIAL_FAILURE,
    REASON_REAL_INPUT_PENDING,
    REASON_REFERENCED_READBACK_FAILED,
    REASON_UNEXPECTED_ERROR,
    RollbackStatus,
)
from backend.services.advisory_phase1.phase1g_dev_evidence_store import (
    Phase1GDevEvidenceStore,
)


LOGGER = logging.getLogger("advisory_phase1g_dev_evidence")
EXIT_SUCCESS = 0
EXIT_COMMAND_INVALID = 2
EXIT_INPUT_PENDING = 3
EXIT_L3_FAILED = 4
EXIT_PERSISTENT_FAILED = 5
EXIT_INTERNAL = 70


def _log_sanitized_exception(message: str, exc: Exception, *args: object) -> None:
    frames = " > ".join(
        f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}"
        for frame in traceback.extract_tb(exc.__traceback__)
    )
    LOGGER.error(
        message + " exception_type=%s redacted_traceback=%s",
        *args,
        type(exc).__name__,
        frames,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Phase 1G G5 DEV evidence (historical research only)"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory = subparsers.add_parser("inventory")
    _add_execution_roots(inventory)

    rollback = subparsers.add_parser("validate-rollback")
    rollback.add_argument("--inventory-ref", required=True, type=Path)
    rollback.add_argument("--execution-manifest", required=True, type=Path)
    _add_execution_roots(rollback)

    persistent = subparsers.add_parser("capture-persistent")
    persistent.add_argument("--inventory-ref", required=True, type=Path)
    persistent.add_argument("--rollback-ref", required=True, type=Path)
    persistent.add_argument("--execution-manifest", required=True, type=Path)
    _add_execution_roots(persistent)

    verify = subparsers.add_parser("verify-evidence")
    verify.add_argument("--evidence-ref", required=True, type=Path)
    verify.add_argument("--g5-evidence-root", required=True, type=Path)
    verify.add_argument("--db-readback", action="store_true")
    verify.add_argument("--env-file", type=Path)
    verify.add_argument("--release-receipt-root", type=Path)
    verify.add_argument("--phase1e-artifact-root", type=Path)
    verify.add_argument("--phase1g-result-root", type=Path)
    return parser


def _add_execution_roots(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--release-receipt-root", required=True, type=Path)
    parser.add_argument("--phase1e-artifact-root", required=True, type=Path)
    parser.add_argument("--phase1g-result-root", required=True, type=Path)
    parser.add_argument("--g5-evidence-root", required=True, type=Path)


def _service(args: argparse.Namespace) -> Phase1GDevEvidenceService:
    return Phase1GDevEvidenceService(
        env_file=args.env_file,
        release_receipt_root=args.release_receipt_root,
        phase1e_artifact_root=args.phase1e_artifact_root,
        phase1g_result_root=args.phase1g_result_root,
        evidence_store=Phase1GDevEvidenceStore(root=args.g5_evidence_root),
    )


def _read_json(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_file():
        raise ValueError("input JSON path must be a regular file")
    try:
        document = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("input path is not valid UTF-8 JSON") from exc
    if not isinstance(document, dict):
        raise ValueError("input JSON must contain one object")
    return document


def _read_ref(path: Path, *, expected: EvidenceKind | None = None) -> Phase1GDevEvidenceRef:
    ref = Phase1GDevEvidenceRef.model_validate(_read_json(path))
    if expected is not None and ref.evidence_kind is not expected:
        raise ValueError(f"evidence ref must have kind {expected.value}")
    return ref


def _emit(document: Any) -> None:
    if hasattr(document, "model_dump"):
        document = document.model_dump(mode="json")
    print(canonical_json_text(document))


def _inventory(args: argparse.Namespace) -> int:
    stored = _service(args).inventory()
    receipt = Phase1GDevEvidenceStore(root=args.g5_evidence_root).load(stored.ref)
    if not isinstance(receipt, Phase1GDevInputInventoryReceipt):
        raise ValueError("inventory ref resolves to the wrong model")
    _emit(
        {
            "ok": True,
            "command": args.command,
            "receipt_ref": stored.ref,
            "inventory_status": receipt.inventory_status.value,
            "l3_source_eligible_count": receipt.l3_source_eligible_count,
            "l4_single_executable_count": receipt.l4_single_executable_count,
            "l4_native_multi_executable_count": receipt.l4_native_multi_executable_count,
            "reason_codes": receipt.reason_codes,
        }
    )
    return (
        EXIT_SUCCESS
        if receipt.inventory_status is InventoryStatus.L4_DUAL_TRACK_READY
        else EXIT_INPUT_PENDING
    )


def _rollback(args: argparse.Namespace) -> int:
    inventory_ref = _read_ref(args.inventory_ref, expected=EvidenceKind.INVENTORY)
    manifest = Phase1GDevExecutionManifest.model_validate(
        _read_json(args.execution_manifest)
    )
    stored, summary = _service(args).validate_rollback(
        inventory_ref=inventory_ref,
        manifest=manifest,
    )
    receipt = Phase1GDevEvidenceStore(root=args.g5_evidence_root).load(stored.ref)
    if not isinstance(receipt, Phase1GDevRollbackReceipt):
        raise ValueError("rollback ref resolves to the wrong model")
    _emit(
        {
            "ok": receipt.rollback_status is RollbackStatus.COMPLETE_ZERO_RESIDUE,
            "command": args.command,
            "receipt_ref": stored.ref,
            "summary_ref": summary.ref,
            "rollback_status": receipt.rollback_status.value,
            "reason_codes": receipt.reason_codes,
        }
    )
    if receipt.rollback_status is RollbackStatus.COMPLETE_ZERO_RESIDUE:
        return EXIT_SUCCESS
    if receipt.rollback_status is RollbackStatus.NOT_RUN_SOURCE_EVIDENCE_PENDING:
        return _exit_with_precedence(
            receipt.reason_codes,
            fallback=EXIT_INPUT_PENDING,
        )
    return _exit_with_precedence(receipt.reason_codes, fallback=EXIT_L3_FAILED)


def _persistent(args: argparse.Namespace) -> int:
    inventory_ref = _read_ref(args.inventory_ref, expected=EvidenceKind.INVENTORY)
    rollback_ref = _read_ref(args.rollback_ref, expected=EvidenceKind.ROLLBACK)
    manifest = Phase1GDevExecutionManifest.model_validate(
        _read_json(args.execution_manifest)
    )
    stored, summary = _service(args).capture_persistent(
        inventory_ref=inventory_ref,
        rollback_ref=rollback_ref,
        manifest=manifest,
    )
    receipt = Phase1GDevEvidenceStore(root=args.g5_evidence_root).load(stored.ref)
    if not isinstance(receipt, Phase1GDevPersistentReceipt):
        raise ValueError("persistent ref resolves to the wrong model")
    _emit(
        {
            "ok": receipt.persistent_status is PersistentStatus.COMPLETE_DUAL_TRACK,
            "command": args.command,
            "receipt_ref": stored.ref,
            "summary_ref": summary.ref,
            "persistent_status": receipt.persistent_status.value,
            "reason_codes": receipt.reason_codes,
        }
    )
    if receipt.persistent_status is PersistentStatus.COMPLETE_DUAL_TRACK:
        return EXIT_SUCCESS
    if receipt.persistent_status is PersistentStatus.NOT_RUN_INPUT_PENDING:
        return _exit_with_precedence(
            receipt.reason_codes,
            fallback=EXIT_INPUT_PENDING,
        )
    return _exit_with_precedence(
        receipt.reason_codes,
        fallback=EXIT_PERSISTENT_FAILED,
    )


def _verify(args: argparse.Namespace) -> int:
    ref = _read_ref(args.evidence_ref)
    store = Phase1GDevEvidenceStore(root=args.g5_evidence_root)
    db_args = (
        args.env_file,
        args.release_receipt_root,
        args.phase1e_artifact_root,
        args.phase1g_result_root,
    )
    if args.db_readback != all(value is not None for value in db_args):
        raise ValueError(
            "--db-readback requires env-file, release-receipt-root, phase1e-artifact-root and phase1g-result-root together"
        )
    if not args.db_readback and any(value is not None for value in db_args):
        raise ValueError("DB roots and env-file require --db-readback")
    if args.db_readback:
        result = _service(args).verify_evidence(ref, db_readback=True)
    else:
        model, reference_closure_hash = verify_g5_reference_closure(
            store=store,
            ref=ref,
        )
        result = {
            "ok": True,
            "evidence_kind": ref.evidence_kind.value,
            "semantic_content_hash": ref.semantic_content_hash,
            "model_schema_version": getattr(model, "schema_version", None),
            "db_readback": False,
            "referenced_readback_hash": None,
            "reference_closure_hash": reference_closure_hash,
        }
    _emit({"command": args.command, **result})
    return EXIT_SUCCESS


def _error_document(*, command: str, exc: Exception) -> dict[str, Any]:
    reason = str(getattr(exc, "reason_code", "ADVISORY_PHASE1G_G5_COMMAND_INVALID"))
    context = getattr(exc, "context", None)
    safe_context = None
    if isinstance(context, dict):
        allowed = {
            "exception_type",
            "operation",
            "relations",
            "statement_types",
            "target_label",
        }
        safe_context = {key: value for key, value in context.items() if key in allowed} or None
    return {
        "ok": False,
        "command": command,
        "error_code": reason,
        "reason_code": reason,
        "message": str(exc),
        "context": safe_context,
    }


def _exit_for_reason(reason: str) -> int:
    if reason in {REASON_UNEXPECTED_ERROR, REASON_EVIDENCE_STORE_FAILED}:
        return EXIT_INTERNAL
    if reason in {
        REASON_L3_COORDINATOR_INVALID,
        REASON_L3_FORBIDDEN_SQL,
        REASON_L3_ROLLBACK_FAILED,
        REASON_L3_RESIDUE_DETECTED,
        REASON_L3_CONCURRENCY_FAILED,
    }:
        return EXIT_L3_FAILED
    if reason in {
        REASON_L4_PLAN_STALE,
        REASON_L4_PARTIAL_FAILURE,
        REASON_REFERENCED_READBACK_FAILED,
    }:
        return EXIT_PERSISTENT_FAILED
    if reason in {REASON_L3_SOURCE_PENDING, REASON_REAL_INPUT_PENDING}:
        return EXIT_INPUT_PENDING
    return EXIT_COMMAND_INVALID


def _exit_with_precedence(
    reason_codes: tuple[str, ...], *, fallback: int
) -> int:
    return max(
        (fallback, *(_exit_for_reason(reason) for reason in reason_codes)),
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        if args.command == "inventory":
            return _inventory(args)
        if args.command == "validate-rollback":
            return _rollback(args)
        if args.command == "capture-persistent":
            return _persistent(args)
        return _verify(args)
    except Phase1GDevEvidenceError as exc:
        LOGGER.error(
            "phase1g G5 command failed command=%s reason_code=%s",
            args.command,
            exc.reason_code,
        )
        _emit(_error_document(command=args.command, exc=exc))
        return _exit_for_reason(exc.reason_code)
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        LOGGER.error(
            "phase1g G5 command rejected command=%s exception_type=%s",
            args.command,
            type(exc).__name__,
        )
        _emit(_error_document(command=args.command, exc=exc))
        return EXIT_COMMAND_INVALID
    except Exception as exc:  # noqa: BLE001
        _log_sanitized_exception(
            "phase1g G5 command failed unexpectedly command=%s",
            exc,
            args.command,
        )
        wrapped = Phase1GDevEvidenceError(
            REASON_UNEXPECTED_ERROR,
            "unexpected G5 command failure",
            context={"exception_type": type(exc).__name__},
        )
        _emit(_error_document(command=args.command, exc=wrapped))
        return EXIT_INTERNAL


if __name__ == "__main__":
    raise SystemExit(main())
