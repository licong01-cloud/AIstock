"""Controlled Phase 0A.1 finalizer and authority CLI.

Default commands only validate immutable receipt artifacts. Any database mutation
requires a command-specific acknowledgement and defaults to a local dev DB.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg2

from backend.services.advisory_phase0a.approval_repository import PostgresApprovalAuthorityRepository
from backend.services.advisory_phase0a.authority import (
    ApprovalDecisionRequest,
    HandoffBundle,
    OperationAuthorizationRequest,
    Phase0AAuthorityError,
    build_approval_bundle,
    build_approval_decision,
    build_handoff_bundle,
    build_operation_authorization_event,
    validate_approval_bundle_active,
    validate_decision_chains,
    validate_operation_authorization_chain,
)
from backend.services.advisory_phase0a.policy import canonical_json_text


TARGET_DEV = "dev"
TARGET_PROD = "prod"


class AdvisoryPhase0AFinalizerCommandError(RuntimeError):
    """A command error that must not emit a partial authority record."""


def _load_env_file(path: Path | None) -> None:
    if path is None or not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _db_config(*, target_db: str) -> dict[str, Any]:
    prefix = "TDX_DB_DEV" if target_db == TARGET_DEV else "TDX_DB"
    required = [f"{prefix}_{name}" for name in ("HOST", "PORT", "NAME", "USER", "PASSWORD")]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise AdvisoryPhase0AFinalizerCommandError(f"missing database environment keys: {missing}")
    config = {
        "host": os.environ[f"{prefix}_HOST"],
        "port": int(os.environ[f"{prefix}_PORT"]),
        "dbname": os.environ[f"{prefix}_NAME"],
        "user": os.environ[f"{prefix}_USER"],
        "password": os.environ[f"{prefix}_PASSWORD"],
    }
    if target_db == TARGET_DEV:
        host = str(config["host"]).lower()
        database = str(config["dbname"]).lower()
        if host not in {"127.0.0.1", "localhost"} or not any(marker in database for marker in ("dev", "scratch", "test")):
            raise AdvisoryPhase0AFinalizerCommandError(
                "refusing dev target because it does not look like a local scratch/dev DB"
            )
    return config


@contextmanager
def _write_conn_factory(*, env_file: Path | None, target_db: str, allow_production_authority: bool) -> Iterator[Any]:
    _load_env_file(env_file)
    if target_db == TARGET_PROD and not allow_production_authority:
        raise AdvisoryPhase0AFinalizerCommandError(
            "production authority mutation requires --allow-production-authority and an approved authority DDL gate"
        )
    connection = psycopg2.connect(**_db_config(target_db=target_db))
    try:
        yield connection
    finally:
        connection.close()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryPhase0AFinalizerCommandError(f"unable to read JSON file {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise AdvisoryPhase0AFinalizerCommandError(f"JSON file {path} must contain an object")
    return payload


def _read_handoff(path: Path) -> HandoffBundle:
    try:
        return HandoffBundle.model_validate(_read_object(path))
    except ValueError as exc:
        raise AdvisoryPhase0AFinalizerCommandError(f"invalid handoff bundle: {exc}") from exc


def _write_new_json(path: Path, payload: Any) -> None:
    if path.exists():
        raise AdvisoryPhase0AFinalizerCommandError(f"refusing to overwrite immutable output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json_text(payload) + "\n", encoding="utf-8", newline="\n")


def _repository(args: argparse.Namespace) -> PostgresApprovalAuthorityRepository:
    def factory() -> Iterator[Any]:
        return _write_conn_factory(
            env_file=args.env_file,
            target_db=args.target_db,
            allow_production_authority=bool(args.allow_production_authority),
        )

    return PostgresApprovalAuthorityRepository(conn_factory=factory)


def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--env-file", type=Path, default=Path(os.environ.get("AISTOCK_ENV_FILE", ".env")))
    parser.add_argument("--target-db", choices=(TARGET_DEV, TARGET_PROD), default=TARGET_DEV)
    parser.add_argument("--allow-production-authority", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Finalize immutable Phase 0A handoff and authority evidence.")
    subcommands = parser.add_subparsers(dest="command", required=True)

    validate = subcommands.add_parser("validate-handoff")
    validate.add_argument("--receipt-dir", required=True, type=Path)

    handoff = subcommands.add_parser("build-handoff-bundle")
    handoff.add_argument("--receipt-dir", required=True, type=Path)
    handoff.add_argument("--output", required=True, type=Path)

    for name, execute_flag in (("register-decision", "--execute-finalize"), ("revoke-decision", "--execute-revoke")):
        command = subcommands.add_parser(name)
        command.add_argument("--handoff", required=True, type=Path)
        command.add_argument("--decision", required=True, type=Path)
        command.add_argument("--authority-backend-id", required=True)
        command.add_argument(execute_flag, action="store_true")
        _add_connection_args(command)

    bundle = subcommands.add_parser("build-approval-bundle")
    bundle.add_argument("--handoff", required=True, type=Path)
    bundle.add_argument("--authority-backend-id", required=True)
    bundle.add_argument("--execute-bundle", action="store_true")
    _add_connection_args(bundle)

    verify = subcommands.add_parser("verify-decision-chain")
    verify.add_argument("--handoff", required=True, type=Path)
    _add_connection_args(verify)

    for name, execute_flag in (("authorize-operation", "--execute-authorize"), ("revoke-authorization", "--execute-auth-revoke")):
        command = subcommands.add_parser(name)
        command.add_argument("--authorization", required=True, type=Path)
        command.add_argument(execute_flag, action="store_true")
        _add_connection_args(command)

    verify_authorization = subcommands.add_parser("verify-authorization")
    verify_authorization.add_argument("--authorization-id", required=True)
    _add_connection_args(verify_authorization)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "validate-handoff":
            handoff = build_handoff_bundle(receipt_dir=args.receipt_dir)
            _emit({"ok": True, "mode": "validated_only", "audit_id": handoff.audit_id, "phase1_handoff_bundle_hash": handoff.phase1_handoff_bundle_hash})
            return 0
        if args.command == "build-handoff-bundle":
            handoff = build_handoff_bundle(receipt_dir=args.receipt_dir)
            _write_new_json(args.output, handoff.model_dump(mode="json"))
            _emit({"ok": True, "mode": "handoff_bundle", "output": str(args.output), "phase1_handoff_bundle_hash": handoff.phase1_handoff_bundle_hash})
            return 0
        if args.command in {"register-decision", "revoke-decision"}:
            flag = "execute_finalize" if args.command == "register-decision" else "execute_revoke"
            _require_execute(args, flag)
            handoff = _read_handoff(args.handoff)
            request = ApprovalDecisionRequest.model_validate(_read_object(args.decision))
            if args.command == "register-decision" and request.event_type.value == "REVOKE":
                raise AdvisoryPhase0AFinalizerCommandError("register-decision cannot write event_type=REVOKE")
            if args.command == "revoke-decision" and request.event_type.value != "REVOKE":
                raise AdvisoryPhase0AFinalizerCommandError("revoke-decision requires event_type=REVOKE")
            repository = _repository(args)
            actor = repository.current_actor()
            event = build_approval_decision(
                handoff=handoff,
                request=request,
                existing_events=repository.list_decisions(handoff=handoff),
                actor_principal=actor,
                authority_backend_id=args.authority_backend_id,
            )
            repository.append_decision(event)
            _emit({"ok": True, "command": args.command, "decision_hash": event.decision_hash, "actor_principal": actor})
            return 0
        if args.command == "build-approval-bundle":
            _require_execute(args, "execute_bundle")
            handoff = _read_handoff(args.handoff)
            repository = _repository(args)
            actor = repository.current_actor()
            bundle = build_approval_bundle(
                handoff=handoff,
                events=repository.list_decisions(handoff=handoff),
                created_by=actor,
                authority_backend_id=args.authority_backend_id,
            )
            repository.append_bundle(bundle)
            _emit({"ok": True, "command": args.command, "approval_bundle_content_hash": bundle.approval_bundle_content_hash, "scope_member_count": bundle.scope_member_count})
            return 0
        if args.command == "verify-decision-chain":
            handoff = _read_handoff(args.handoff)
            repository = _repository(args)
            events = repository.list_decisions(handoff=handoff)
            validate_decision_chains(handoff=handoff, events=events)
            _emit({"ok": True, "command": args.command, "event_count": len(events)})
            return 0
        if args.command in {"authorize-operation", "revoke-authorization"}:
            flag = "execute_authorize" if args.command == "authorize-operation" else "execute_auth_revoke"
            _require_execute(args, flag)
            request = OperationAuthorizationRequest.model_validate(_read_object(args.authorization))
            if args.command == "authorize-operation" and request.event_type.value != "AUTHORIZE":
                raise AdvisoryPhase0AFinalizerCommandError("authorize-operation requires event_type=AUTHORIZE")
            if args.command == "revoke-authorization" and request.event_type.value != "REVOKE":
                raise AdvisoryPhase0AFinalizerCommandError("revoke-authorization requires event_type=REVOKE")
            repository = _repository(args)
            actor = repository.current_actor()
            if args.command == "authorize-operation":
                _validate_registered_bundle(repository=repository, request=request)
            event = build_operation_authorization_event(
                request=request,
                existing_events=repository.list_operation_authorizations(authorization_id=request.authorization_id),
                actor_principal=actor,
            )
            repository.append_operation_authorization(event)
            _emit({"ok": True, "command": args.command, "authorization_event_hash": event.authorization_event_hash, "actor_principal": actor})
            return 0
        if args.command == "verify-authorization":
            repository = _repository(args)
            events = repository.list_operation_authorizations(authorization_id=args.authorization_id)
            validate_operation_authorization_chain(authorization_id=args.authorization_id, events=events)
            _emit({"ok": True, "command": args.command, "event_count": len(events)})
            return 0
        raise AdvisoryPhase0AFinalizerCommandError(f"unsupported command: {args.command}")
    except (AdvisoryPhase0AFinalizerCommandError, Phase0AAuthorityError, ValueError) as exc:
        _emit({"ok": False, "error": str(exc)}, stream=sys.stderr)
        return 2


def _require_execute(args: argparse.Namespace, flag: str) -> None:
    if not getattr(args, flag):
        raise AdvisoryPhase0AFinalizerCommandError(f"--{flag.replace('_', '-')} is required for mutation")


def _validate_registered_bundle(
    *,
    repository: PostgresApprovalAuthorityRepository,
    request: OperationAuthorizationRequest,
) -> None:
    if request.approval_bundle_hash is None:
        return
    bundle = repository.get_approval_bundle(approval_bundle_content_hash=request.approval_bundle_hash)
    if bundle is None:
        raise AdvisoryPhase0AFinalizerCommandError(
            "ADVISORY_PHASE1_OPERATION_AUTHORIZATION_MISSING: approval bundle is not registered"
        )
    if request.admission_scope_set_hash and request.admission_scope_set_hash != bundle.admission_scope_set_hash:
        raise AdvisoryPhase0AFinalizerCommandError(
            "ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: approval bundle scope set"
        )
    validate_approval_bundle_active(
        bundle=bundle,
        events=repository.list_decisions_for_bundle(bundle=bundle),
    )


def _emit(payload: dict[str, Any], *, stream: Any | None = None) -> None:
    print(json.dumps(payload, sort_keys=True, default=str), file=stream or sys.stdout)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
