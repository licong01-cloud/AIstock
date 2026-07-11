"""Execute a user-approved Advisory Phase 0A audit with read-only database sessions."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg2

from backend.services.advisory_phase0a.audit_service import AdvisoryPhase0AAuditService, write_receipt_artifacts
from backend.services.advisory_phase0a.models import AuditRequest, Phase0AAuditError, Phase0APolicyRegistry
from backend.services.advisory_phase0a.policy import canonical_json_sha256, default_policy_registry
from backend.services.advisory_phase0a.resolvers import AuditReaders, PostgresReadOnlySourceProbe
from backend.services.advisory_program import AdvisoryProgramPGRepository
from backend.services.selection_center.repository import SelectionCenterRepository
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactRepository


TARGET_PROD = "prod"
TARGET_DEV = "dev"
DEFAULT_OUTPUT_ROOT = Path("tests/aistock_validation/history/advisory_phase0a")
READ_ONLY_STATEMENT_TIMEOUT_MS = 5_000


class AdvisoryPhase0ACommandError(RuntimeError):
    """A command-line error that should not emit a partial receipt."""


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
    if target_db == TARGET_DEV:
        required = [
            "TDX_DB_DEV_HOST",
            "TDX_DB_DEV_PORT",
            "TDX_DB_DEV_NAME",
            "TDX_DB_DEV_USER",
            "TDX_DB_DEV_PASSWORD",
        ]
        missing = [key for key in required if not os.environ.get(key)]
        if missing:
            raise AdvisoryPhase0ACommandError(f"missing dev database environment keys: {missing}")
        config = {
            "host": os.environ["TDX_DB_DEV_HOST"],
            "port": int(os.environ["TDX_DB_DEV_PORT"]),
            "dbname": os.environ["TDX_DB_DEV_NAME"],
            "user": os.environ["TDX_DB_DEV_USER"],
            "password": os.environ["TDX_DB_DEV_PASSWORD"],
        }
        host = str(config["host"]).lower()
        dbname = str(config["dbname"]).lower()
        if host not in {"127.0.0.1", "localhost"} or not any(marker in dbname for marker in ("dev", "scratch", "test")):
            raise AdvisoryPhase0ACommandError(
                "refusing dev target because it does not look like a local scratch/dev DB: "
                f"host={config['host']} dbname={config['dbname']}"
            )
        return config
    required = ["TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise AdvisoryPhase0ACommandError(f"missing database environment keys: {missing}")
    return {
        "host": os.environ["TDX_DB_HOST"],
        "port": int(os.environ["TDX_DB_PORT"]),
        "dbname": os.environ["TDX_DB_NAME"],
        "user": os.environ["TDX_DB_USER"],
        "password": os.environ["TDX_DB_PASSWORD"],
    }


@contextmanager
def _env_conn_factory(*, env_file: Path | None, target_db: str) -> Iterator[Any]:
    """Open a PostgreSQL connection with a server-enforced read-only default transaction."""

    _load_env_file(env_file)
    connection = psycopg2.connect(**_db_config(target_db=target_db))
    set_session = getattr(connection, "set_session", None)
    if not callable(set_session):
        connection.close()
        raise AdvisoryPhase0ACommandError("database connection does not support read-only sessions")
    set_session(readonly=True, autocommit=False)
    with connection.cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout = %s", (READ_ONLY_STATEMENT_TIMEOUT_MS,))
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


def _read_policy(path: Path | None, *, policy_version: str) -> Phase0APolicyRegistry:
    if path is None:
        return default_policy_registry(policy_version=policy_version)
    try:
        return Phase0APolicyRegistry.model_validate(_read_json(path))
    except ValueError as exc:
        raise AdvisoryPhase0ACommandError(f"invalid Phase 0A policy registry: {exc}") from exc


def _readers_from_env(*, env_file: Path | None, target_db: str) -> AuditReaders:
    def factory() -> Iterator[Any]:
        return _env_conn_factory(env_file=env_file, target_db=target_db)

    source_probe = PostgresReadOnlySourceProbe(factory)
    return AuditReaders(
        advisory=AdvisoryProgramPGRepository(conn_factory=factory),
        package=StrategyPackageRepository(conn_factory=factory),
        evidence=SimulationRuntimeRepository(conn_factory=factory),
        score_artifact=StrategyPackageSelectionArtifactRepository(conn_factory=factory),
        selection_run=SelectionCenterRepository(conn_factory=factory),
        source_probe=source_probe,
        calendar=source_probe,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a read-only Advisory Phase 0A evidence audit.")
    parser.add_argument("--request", required=True, type=Path, help="Approved audit request JSON")
    parser.add_argument("--policy", type=Path, help="Pre-registered policy JSON")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--env-file", type=Path, default=Path(os.environ.get("AISTOCK_ENV_FILE", ".env")))
    parser.add_argument("--target-db", choices=(TARGET_PROD, TARGET_DEV), default=TARGET_PROD)
    parser.add_argument(
        "--execute-readonly",
        action="store_true",
        help="Required acknowledgement before opening a read-only DB session and writing a receipt.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        request = _read_request(args.request)
        policy = _read_policy(args.policy, policy_version=request.audit_policy_version)
        if policy.policy_version != request.audit_policy_version:
            raise AdvisoryPhase0ACommandError(
                f"policy version mismatch: request={request.audit_policy_version} policy={policy.policy_version}"
            )
        if not args.execute_readonly:
            print(
                json.dumps(
                    {
                        "ok": True,
                        "mode": "validated_only",
                        "audit_id": request.audit_id,
                        "request_hash": canonical_json_sha256(request),
                    },
                    sort_keys=True,
                )
            )
            return 0
        if args.policy is None:
            raise AdvisoryPhase0ACommandError("--policy is required with --execute-readonly")
        if not request.approved_request_reference:
            raise AdvisoryPhase0ACommandError(
                "approved_request_reference is required with --execute-readonly; no controlled audit may run without scope approval"
            )
        service = AdvisoryPhase0AAuditService(readers=_readers_from_env(env_file=args.env_file, target_db=args.target_db), policy=policy)
        receipt = service.audit(request)
        destination = write_receipt_artifacts(
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
                },
                sort_keys=True,
            )
        )
        return 0
    except (AdvisoryPhase0ACommandError, Phase0AAuditError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
