"""Execute a versioned Advisory Phase 0A audit with read-only database sessions."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, Iterator, Mapping

import psycopg2
from dotenv import dotenv_values

if __package__ in {None, ""}:  # Support direct ``python scripts/...`` execution from the repository root.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.services.advisory_phase0a.audit_service import (
    AdvisoryPhase0AAuditService,
    receipt_artifact_payloads,
)
from backend.services.advisory_phase0a.evidence_projection_postgres import AdvisoryPostgresEvidenceProjection
from backend.services.advisory_phase0a.models import AuditRequest, Phase0AAuditError, Phase0APolicyRegistry
from backend.services.advisory_phase0a.policy import (
    POLICY_REGISTRY_ROOT,
    PolicyRegistryValidationError,
    canonical_json_sha256,
    canonical_json_text,
    load_frozen_policy_registry,
)
from backend.services.advisory_phase0a.resolvers import AuditReaders
from backend.services.advisory_dev_input_onboarding.contracts import database_identity_hash, validate_sha256
from backend.services.advisory_dev_input_onboarding.store import RealDevOnboardingEvidenceStore
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel


TARGET_PROD = "prod"
TARGET_DEV = "dev"
READ_ONLY_STATEMENT_TIMEOUT_MS = 5_000


class AdvisoryPhase0ACommandError(RuntimeError):
    """A command-line error that should not emit a partial receipt."""


def _load_env_file(path: Path) -> dict[str, str]:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise AdvisoryPhase0ACommandError("audit env file does not exist") from exc
    if not resolved.is_file():
        raise AdvisoryPhase0ACommandError("audit env path must reference an existing file")
    values = {
        str(key): str(value)
        for key, value in dotenv_values(resolved, interpolate=False).items()
        if key and value is not None
    }
    if not values:
        raise AdvisoryPhase0ACommandError("audit env file contains no database configuration")
    return values


def _db_config(*, target_db: str, env_values: Mapping[str, str]) -> dict[str, Any]:
    if target_db == TARGET_DEV:
        required = [
            "TDX_DB_DEV_HOST",
            "TDX_DB_DEV_PORT",
            "TDX_DB_DEV_NAME",
            "TDX_DB_DEV_USER",
            "TDX_DB_DEV_PASSWORD",
        ]
        missing = [key for key in required if not env_values.get(key)]
        if missing:
            raise AdvisoryPhase0ACommandError(f"missing dev database environment keys: {missing}")
        config = {
            "host": env_values["TDX_DB_DEV_HOST"],
            "port": int(env_values["TDX_DB_DEV_PORT"]),
            "dbname": env_values["TDX_DB_DEV_NAME"],
            "user": env_values["TDX_DB_DEV_USER"],
            "password": env_values["TDX_DB_DEV_PASSWORD"],
        }
        return config
    required = ["TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"]
    missing = [key for key in required if not env_values.get(key)]
    if missing:
        raise AdvisoryPhase0ACommandError(f"missing database environment keys: {missing}")
    return {
        "host": env_values["TDX_DB_HOST"],
        "port": int(env_values["TDX_DB_PORT"]),
        "dbname": env_values["TDX_DB_NAME"],
        "user": env_values["TDX_DB_USER"],
        "password": env_values["TDX_DB_PASSWORD"],
    }


@contextmanager
def _env_conn_factory(
    *,
    env_file: Path,
    target_db: str,
    expected_database_identity_hash: str | None = None,
) -> Iterator[Any]:
    """Open a PostgreSQL connection with a server-enforced read-only default transaction."""

    env_values = _load_env_file(env_file)
    connection = psycopg2.connect(**_db_config(target_db=target_db, env_values=env_values))
    set_session = getattr(connection, "set_session", None)
    if not callable(set_session):
        connection.close()
        raise AdvisoryPhase0ACommandError("database connection does not support read-only sessions")
    set_session(readonly=True, autocommit=False)
    with connection.cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout = %s", (READ_ONLY_STATEMENT_TIMEOUT_MS,))
        if expected_database_identity_hash is not None:
            expected = validate_sha256(expected_database_identity_hash, field_name="expected_database_identity_hash")
            cursor.execute(
                """
                SELECT current_database(), host(inet_server_addr()), inet_server_port(),
                       current_setting('server_version_num')::integer, current_user,
                       current_setting('transaction_read_only')
                """
            )
            row = cursor.fetchone()
            if row is None or str(row[5]).lower() not in {"on", "true"}:
                connection.close()
                raise AdvisoryPhase0ACommandError("audit database identity query did not observe a read-only transaction")
            config = _db_config(target_db=target_db, env_values=env_values)
            environment_contract_hash = canonical_json_sha256(
                {
                    "target_label": (TargetLabel.DEV if target_db == TARGET_DEV else TargetLabel.PRODUCTION).value,
                    "host": str(config["host"]),
                    "port": int(config["port"]),
                    "database": str(config["dbname"]),
                    "user": str(config["user"]),
                }
            )
            identity = DatabaseIdentity(
                target_label=TargetLabel.DEV if target_db == TARGET_DEV else TargetLabel.PRODUCTION,
                current_database=str(row[0]),
                server_address=str(row[1]) if row[1] is not None else None,
                server_port=int(row[2]),
                server_version_num=int(row[3]),
                current_user_hash=hashlib.sha256(str(row[4]).encode("utf-8")).hexdigest(),
                environment_contract_hash=environment_contract_hash,
            )
            if database_identity_hash(identity) != expected:
                connection.close()
                raise AdvisoryPhase0ACommandError("audit database identity differs from the explicit target contract")
    try:
        yield connection
    finally:
        connection.close()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryPhase0ACommandError(f"unable to read JSON file {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AdvisoryPhase0ACommandError(f"JSON file {path} must contain an object")
    return value


def _read_request(path: Path) -> AuditRequest:
    try:
        return AuditRequest.model_validate(_read_json(path))
    except ValueError as exc:
        raise AdvisoryPhase0ACommandError(f"invalid audit request: {exc}") from exc


def _load_policy_for_request(*, request: AuditRequest, registry_root: Path | None) -> Phase0APolicyRegistry:
    policy = load_frozen_policy_registry(
        policy_registry_id=request.policy_registry_id,
        policy_version=request.audit_policy_version,
        registry_root=registry_root,
    )
    if policy.registry_content_hash != request.policy_registry_content_hash:
        raise AdvisoryPhase0ACommandError(
            "ADVISORY_PHASE0A_POLICY_REGISTRY_HASH_MISMATCH: "
            f"request={request.policy_registry_content_hash} policy={policy.registry_content_hash}"
        )
    return policy


def _write_content_addressed_receipt(
    *,
    receipt: Any,
    request: AuditRequest,
    policy: Phase0APolicyRegistry,
    output_root: Path,
) -> tuple[Path, bool]:
    identity = str(receipt.audit_manifest_hash or "").strip().lower()
    validate_sha256(identity, field_name="audit_manifest_hash")
    root = RealDevOnboardingEvidenceStore(root=output_root).root
    bucket = root / "audit-receipts" / identity[:2] / identity
    audit_id = str(receipt.audit_id or "").strip()
    if not audit_id or Path(audit_id).name != audit_id or audit_id in {".", ".."}:
        raise AdvisoryPhase0ACommandError("audit_id must be one safe path segment")
    destination = bucket / audit_id
    try:
        destination.resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise AdvisoryPhase0ACommandError("content-addressed audit receipt escapes its output root") from exc
    expected = receipt_artifact_payloads(receipt=receipt, request=request, policy=policy)
    if any(Path(filename).name != filename for filename in expected):
        raise AdvisoryPhase0ACommandError("audit receipt artifact filename must be one safe path segment")

    def expected_bytes(filename: str, payload: Any) -> bytes:
        text = str(payload) if filename.endswith(".md") else canonical_json_text(payload) + "\n"
        return text.encode("utf-8")

    def verify_existing() -> None:
        if not destination.is_dir():
            raise AdvisoryPhase0ACommandError("content-addressed audit receipt destination is not a directory")
        actual_names = {item.name for item in destination.iterdir() if item.is_file()}
        if actual_names != set(expected):
            raise AdvisoryPhase0ACommandError("content-addressed audit receipt file closure differs")
        if any(item.is_dir() for item in destination.iterdir()):
            raise AdvisoryPhase0ACommandError("content-addressed audit receipt contains an unexpected directory")
        for filename, payload in expected.items():
            if (destination / filename).read_bytes() != expected_bytes(filename, payload):
                raise AdvisoryPhase0ACommandError("content-addressed audit receipt differs from exact readback")

    if destination.exists():
        verify_existing()
        return destination, True
    bucket.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{audit_id}.", suffix=".staging", dir=bucket))
    try:
        for filename, payload in expected.items():
            path = staging / filename
            with path.open("xb") as handle:
                handle.write(expected_bytes(filename, payload))
                handle.flush()
                os.fsync(handle.fileno())
        try:
            os.rename(staging, destination)
        except OSError as exc:
            if not destination.exists():
                raise AdvisoryPhase0ACommandError("unable to publish content-addressed audit receipt") from exc
            verify_existing()
            return destination, True
        verify_existing()
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return destination, False


@contextmanager
def _readers_from_env(
    *,
    env_file: Path,
    target_db: str,
    expected_database_identity_hash: str | None = None,
) -> Iterator[AuditReaders]:
    def factory() -> Iterator[Any]:
        return _env_conn_factory(
            env_file=env_file,
            target_db=target_db,
            expected_database_identity_hash=expected_database_identity_hash,
        )

    projection = AdvisoryPostgresEvidenceProjection(factory)
    with projection.snapshot() as snapshot:
        yield AuditReaders(
            advisory=snapshot,
            package=snapshot,
            evidence=snapshot,
            score_artifact=snapshot,
            selection_run=snapshot,
            source_probe=snapshot,
            calendar=snapshot,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a read-only Advisory Phase 0A evidence audit.")
    parser.add_argument("command", nargs="?", choices=("validate-policy-registry",))
    parser.add_argument("--request", type=Path, help="Versioned audit request JSON")
    parser.add_argument("--policy-registry-id", help="Policy registry id for validation mode")
    parser.add_argument("--policy-version", help="Policy version for validation mode")
    parser.add_argument(
        "--policy-registry-root",
        type=Path,
        default=POLICY_REGISTRY_ROOT,
        help="Read-only root containing repo-tracked frozen policy registry JSON files",
    )
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--target-db", choices=(TARGET_PROD, TARGET_DEV), default=TARGET_PROD)
    parser.add_argument(
        "--expected-database-identity-hash",
        help="Exact database identity hash required for DEV read-only audit execution",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "validate-policy-registry":
            if not args.policy_registry_id or not args.policy_version:
                raise AdvisoryPhase0ACommandError(
                    "validate-policy-registry requires --policy-registry-id and --policy-version"
                )
            policy = load_frozen_policy_registry(
                policy_registry_id=args.policy_registry_id,
                policy_version=args.policy_version,
                registry_root=args.policy_registry_root,
            )
            print(
                json.dumps(
                    {
                        "ok": True,
                        "mode": "policy_registry_validated",
                        "policy_registry_id": policy.policy_registry_id,
                        "policy_version": policy.policy_version,
                        "registry_content_hash": policy.registry_content_hash,
                        "effective_from_trade_date": policy.effective_from_trade_date.isoformat(),
                        "effective_to_trade_date": (
                            policy.effective_to_trade_date.isoformat()
                            if policy.effective_to_trade_date is not None
                            else None
                        ),
                    },
                    sort_keys=True,
                )
            )
            return 0
        if args.request is None:
            raise AdvisoryPhase0ACommandError("--request is required for an audit")
        if args.env_file is None or args.output_root is None:
            raise AdvisoryPhase0ACommandError("audit execution requires explicit --env-file and --output-root")
        request = _read_request(args.request)
        policy = _load_policy_for_request(request=request, registry_root=args.policy_registry_root)
        if policy.policy_version != request.audit_policy_version:
            raise AdvisoryPhase0ACommandError(
                f"policy version mismatch: request={request.audit_policy_version} policy={policy.policy_version}"
            )
        if args.target_db == TARGET_DEV and not args.expected_database_identity_hash:
            raise AdvisoryPhase0ACommandError(
                "--expected-database-identity-hash is required for an exact DEV audit target"
            )
        reader_context = _readers_from_env(
            env_file=args.env_file,
            target_db=args.target_db,
            expected_database_identity_hash=args.expected_database_identity_hash,
        )
        if not hasattr(reader_context, "__enter__"):
            reader_context = nullcontext(reader_context)
        with reader_context as readers:
            service = AdvisoryPhase0AAuditService(readers=readers, policy=policy)
            receipt = service.audit(request)
        destination, idempotent = _write_content_addressed_receipt(
            receipt=receipt,
            request=request,
            policy=policy,
            output_root=args.output_root,
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "mode": "read_only_audit",
                    "audit_id": receipt.audit_id,
                    "audit_manifest_hash": receipt.audit_manifest_hash,
                    "output": str(destination),
                    "idempotent": idempotent,
                },
                sort_keys=True,
            )
        )
        return 0
    except (AdvisoryPhase0ACommandError, Phase0AAuditError, PolicyRegistryValidationError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
