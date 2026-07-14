#!/usr/bin/env python
"""Explicit Phase 1F schema release CLI.

This tool only plans, verifies, or applies the frozen Advisory Phase 1 schema
contract. It never starts a worker, writes business DML, or activates a
recommendation/runtime path.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.advisory_phase1.release_schema_apply_postgres import (  # noqa: E402
    ReleaseSchemaApplyError,
    apply_release_schema_plan,
    plan_release_schema,
    verify_release_schema_plan,
)
from backend.services.advisory_phase1.release_schema_contract import (  # noqa: E402
    ManagedSchemaStatus,
    ReleaseSchemaPlan,
    ReleaseSchemaReceipt,
    RequestedOperation,
    TargetLabel,
    canonical_json_text,
    load_release_schema_contract,
    make_release_plan_request,
)
from backend.services.advisory_phase1.release_schema_receipt_store import (  # noqa: E402
    ReleaseSchemaReceiptStore,
    ReleaseSchemaReceiptStoreError,
)
from backend.services.advisory_phase1.release_schema_verify_postgres import (  # noqa: E402
    ReleaseSchemaVerificationError,
    resolve_database_connection,
)
from backend.services.advisory_phase1.source_capacity import CapacityPlanningReceipt, CapacityPlanningRequest  # noqa: E402


LOGGER = logging.getLogger("aistock.advisory.phase1f.release_schema")

EXIT_OK = 0
EXIT_REQUEST_CONTRACT = 2
EXIT_ENVIRONMENT = 3
EXIT_DRIFT = 4
EXIT_DDL = 5
EXIT_POST_VERIFY_STORE = 6
EXIT_INTERNAL = 7


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to read JSON input {path.name}: {type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON input {path.name} must be an object")
    return payload


def _target_label(value: str) -> TargetLabel:
    return TargetLabel.DEV if value == "dev" else TargetLabel.PRODUCTION


def _requested_operation(value: str) -> RequestedOperation:
    return RequestedOperation(value.upper())


def _load_phase1e_hashes(paths: list[Path]) -> tuple[str, ...]:
    hashes: list[str] = []
    for path in paths:
        payload = _read_json(path)
        value = payload.get("plan_content_hash")
        if not isinstance(value, str):
            raise ValueError(f"Phase 1E plan {path.name} has no plan_content_hash")
        hashes.append(value)
    return tuple(sorted(set(hashes)))


def _create_request(args: argparse.Namespace):
    contract = load_release_schema_contract()
    capacity_payload = _read_json(args.capacity_request)
    capacity_request = CapacityPlanningRequest.model_validate(capacity_payload)
    capacity_receipt_hash: str | None = None
    if args.capacity_receipt is not None:
        capacity_receipt = CapacityPlanningReceipt.model_validate(_read_json(args.capacity_receipt))
        if capacity_receipt.request_hash != capacity_request.request_hash:
            raise ValueError("capacity receipt request_hash does not match the supplied capacity request")
        capacity_receipt_hash = capacity_receipt.receipt_hash
    request = make_release_plan_request(
        contract=contract,
        target_label=_target_label(args.db_target),
        history_start_trade_date=capacity_request.history_start_trade_date,
        history_end_trade_date=capacity_request.history_end_trade_date,
        capacity_request_hash=capacity_request.request_hash,
        capacity_receipt_hash=capacity_receipt_hash,
        phase1e_plan_hashes=_load_phase1e_hashes(args.phase1e_plan),
        requested_operation=_requested_operation(args.requested_operation),
    )
    return contract, request


def _emit(payload: dict[str, Any]) -> None:
    print(canonical_json_text(payload))


def _exit_for_status(status: ManagedSchemaStatus) -> int:
    return EXIT_DRIFT if status in {ManagedSchemaStatus.DRIFTED, ManagedSchemaStatus.UNSUPPORTED} else EXIT_OK


def _exit_for_reason(reason_code: str) -> int:
    if reason_code in {
        "PHASE1F_ENV_CONFIG_MISSING",
        "PHASE1F_DATABASE_CONNECTION_FAILED",
        "PHASE1F_DATABASE_IDENTITY_MISMATCH",
    }:
        return EXIT_ENVIRONMENT
    if reason_code in {
        "PHASE1F_SCHEMA_DRIFTED",
        "PHASE1F_POSTGRES_VERSION_UNSUPPORTED",
        "PHASE1F_MIGRATION_HASH_MISMATCH",
        "PHASE1F_PARTITION_BOUND_MISMATCH",
        "ADVISORY_PHASE1F1_PREDECESSOR_SCHEMA_INVALID",
        "ADVISORY_PHASE1F1_PARENT_DATE_UNRESOLVED",
        "ADVISORY_PHASE1F1_CATALOG_DRIFTED",
    }:
        return EXIT_DRIFT
    if reason_code in {
        "PHASE1F_DDL_LOCK_TIMEOUT",
        "PHASE1F_DDL_STATEMENT_TIMEOUT",
        "PHASE1F_DDL_EXECUTION_FAILED",
        "PHASE1F_TRANSACTION_VERIFY_FAILED",
        "PHASE1F_MIGRATION_FILE_MISSING",
        "ADVISORY_PHASE1F1_COPY_MISMATCH",
        "ADVISORY_PHASE1F1_PARTITION_MISSING",
    }:
        return EXIT_DDL
    if reason_code in {
        "PHASE1F_POST_COMMIT_VERIFY_FAILED",
        "PHASE1F_RECEIPT_STORE_FAILED",
        "ADVISORY_PHASE1F1_VIEW_CONTRACT_MISMATCH",
        "ADVISORY_PHASE1F1_POST_COMMIT_VERIFY_FAILED",
        "ADVISORY_PHASE1F1_POST_FAILURE_VERIFY_FAILED",
    }:
        return EXIT_POST_VERIFY_STORE
    return EXIT_REQUEST_CONTRACT


def _plan(args: argparse.Namespace) -> int:
    contract, request = _create_request(args)
    config = resolve_database_connection(target_label=request.target_label, env_file=args.env_file)
    store = ReleaseSchemaReceiptStore(args.receipt_root)
    plan, receipt = plan_release_schema(config=config, contract=contract, request=request)
    plan_artifact = store.write_plan(identity=plan.plan_content_hash, payload=plan.model_dump(mode="json"))
    receipt_artifact = store.write_receipt(
        identity=receipt.receipt_content_hash, payload=receipt.model_dump(mode="json")
    )
    _emit(
        {
            "operation": "PLAN",
            "plan": plan.model_dump(mode="json"),
            "plan_path": str(plan_artifact.path),
            "receipt": receipt.model_dump(mode="json"),
            "receipt_path": str(receipt_artifact.path),
        }
    )
    return _exit_for_status(plan.managed_schema_status)


def _load_plan(args: argparse.Namespace) -> tuple[ReleaseSchemaPlan, ReleaseSchemaReceiptStore]:
    store = ReleaseSchemaReceiptStore(args.receipt_root)
    payload = store.load(kind="plans", path=args.plan)
    return ReleaseSchemaPlan.model_validate(payload), store


def _verify(args: argparse.Namespace) -> int:
    plan, store = _load_plan(args)
    contract = load_release_schema_contract()
    config = resolve_database_connection(target_label=_target_label(args.db_target), env_file=args.env_file)
    receipt = verify_release_schema_plan(plan=plan, config=config, contract=contract)
    artifact = store.write_receipt(identity=receipt.receipt_content_hash, payload=receipt.model_dump(mode="json"))
    _emit({"operation": "VERIFY", "receipt": receipt.model_dump(mode="json"), "receipt_path": str(artifact.path)})
    return _exit_for_status(receipt.managed_schema_status)


def _apply(args: argparse.Namespace) -> int:
    plan, store = _load_plan(args)
    contract = load_release_schema_contract()
    config = resolve_database_connection(target_label=_target_label(args.db_target), env_file=args.env_file)
    receipt = apply_release_schema_plan(plan=plan, config=config, contract=contract)
    artifact = store.write_receipt(identity=receipt.receipt_content_hash, payload=receipt.model_dump(mode="json"))
    _emit({"operation": "APPLY", "receipt": receipt.model_dump(mode="json"), "receipt_path": str(artifact.path)})
    if receipt.operation_status.value == "FAILED":
        reason_code = str(receipt.errors[0].get("reason_code") or "PHASE1F_DDL_EXECUTION_FAILED")
        return _exit_for_reason(reason_code)
    return _exit_for_status(receipt.managed_schema_status)


def _inspect_receipt(args: argparse.Namespace) -> int:
    payload = _read_json(args.receipt)
    receipt = ReleaseSchemaReceipt.model_validate(payload)
    _emit({"operation": "INSPECT_RECEIPT", "receipt": receipt.model_dump(mode="json")})
    return EXIT_OK


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    def release_args(command: argparse.ArgumentParser, *, require_plan: bool = False) -> None:
        command.add_argument("--db-target", choices=("dev", "production"), required=True)
        command.add_argument("--env-file", type=Path, required=True)
        command.add_argument("--receipt-root", type=Path, required=True)
        if require_plan:
            command.add_argument("--plan", type=Path, required=True)

    plan = commands.add_parser("plan")
    release_args(plan)
    plan.add_argument("--capacity-request", type=Path, required=True)
    plan.add_argument("--capacity-receipt", type=Path)
    plan.add_argument("--phase1e-plan", type=Path, action="append", default=[])
    plan.add_argument("--requested-operation", choices=("plan", "verify", "apply"), required=True)

    verify = commands.add_parser("verify")
    release_args(verify, require_plan=True)

    apply = commands.add_parser("apply")
    release_args(apply, require_plan=True)

    inspect = commands.add_parser("inspect-receipt")
    inspect.add_argument("--receipt", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        if args.command == "plan":
            return _plan(args)
        if args.command == "verify":
            return _verify(args)
        if args.command == "apply":
            return _apply(args)
        return _inspect_receipt(args)
    except (ValueError, ReleaseSchemaApplyError) as exc:
        reason_code = getattr(exc, "reason_code", "PHASE1F_REQUEST_INVALID")
        context = exc.receipt_error() if isinstance(exc, ReleaseSchemaApplyError) else None
        LOGGER.error(
            "Phase 1F release schema failure reason=%s exception_type=%s context=%s",
            reason_code,
            type(exc).__name__,
            context,
        )
        _emit({"error_code": reason_code, "reason_code": reason_code, "message": str(exc), "context": context})
        return _exit_for_reason(reason_code)
    except ReleaseSchemaVerificationError as exc:
        LOGGER.error("Phase 1F catalog verification failure reason=%s", exc.reason_code)
        _emit({"error_code": exc.reason_code, "reason_code": exc.reason_code, "message": str(exc)})
        return _exit_for_reason(exc.reason_code)
    except ReleaseSchemaReceiptStoreError as exc:
        LOGGER.error("Phase 1F receipt-store failure reason=%s", exc.reason_code)
        _emit({"error_code": exc.reason_code, "reason_code": exc.reason_code, "message": str(exc)})
        return EXIT_POST_VERIFY_STORE
    except Exception as exc:  # pragma: no cover - top-level diagnostic boundary.
        LOGGER.exception("Phase 1F release schema CLI failed with %s", type(exc).__name__)
        _emit(
            {
                "error_code": "PHASE1F_INTERNAL_ERROR",
                "reason_code": "PHASE1F_INTERNAL_ERROR",
                "message": type(exc).__name__,
            }
        )
        return EXIT_INTERNAL


if __name__ == "__main__":
    raise SystemExit(main())
