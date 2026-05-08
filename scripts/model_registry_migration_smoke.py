"""Model registry migration smoke checks.

Default mode is a static dry-run: it validates migration/rollback files and
does not open a DB connection. DB execution is opt-in and guarded so Phase 5
schema checks can be run against a dev database without touching production.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = REPO_ROOT / "backend" / "migrations" / "model_registry_phase5_20260509.sql"
ROLLBACK_PATH = REPO_ROOT / "backend" / "migrations" / "model_registry_phase5_20260509_rollback.sql"

CONFIRM_DB_CHECK = "MODEL_REGISTRY_PHASE5_DEV_ROLLBACK_CHECK"
CONFIRM_APPLY = "APPLY_MODEL_REGISTRY_PHASE5_DEV_ONLY"
ENV_DB_CHECK_ENABLED = "AISTOCK_MODEL_REGISTRY_MIGRATION_DEV_DB"
ENV_APPLY_ENABLED = "AISTOCK_MODEL_REGISTRY_MIGRATION_APPLY_ENABLED"
ENV_ALLOW_SUSPICIOUS_ROLLBACK = "AISTOCK_MODEL_REGISTRY_ALLOW_PRODUCTION_LIKE_ROLLBACK_CHECK"

EXPECTED_TABLES = (
    "model_template",
    "model_spec",
    "model_trial",
    "model_artifact",
    "model_lifecycle_event",
)
EXPECTED_VIEWS = (
    "v_qe_selectable_model_spec",
    "v_model_catalog_compat",
    "v_legacy_aistock_model_catalog_bridge",
)
EXPECTED_INDEXES = (
    "idx_model_spec_template",
    "idx_model_spec_qe_selectable",
    "idx_model_spec_source",
    "idx_model_trial_spec",
    "idx_model_trial_qe_lineage",
    "idx_model_artifact_trial",
    "idx_model_artifact_status",
    "idx_model_lifecycle_event_object",
)
DEV_DB_MARKERS = ("dev", "test", "sandbox", "local", "tmp", "temp")
PRODUCTION_DB_MARKERS = ("prod", "production", "live", "main")
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


class MigrationSmokeError(RuntimeError):
    """Raised when the migration smoke gate fails."""


@dataclass(frozen=True)
class DbTarget:
    host: str
    port: int
    dbname: str
    user: str

    @property
    def label(self) -> str:
        return f"{self.user}@{self.host}:{self.port}/{self.dbname}"

    def as_psycopg2_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": os.getenv("TDX_DB_PASSWORD", ""),
            "application_name": "AIstock-model-registry-migration-smoke",
            "options": "-c client_encoding=utf8",
        }


@dataclass(frozen=True)
class SmokeReport:
    status: str
    mode: str
    migration_file: str
    rollback_file: str
    checks: dict[str, Any]
    db_target: str | None = None


def _env_truthy(key: str) -> bool:
    value = (os.getenv(key) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


def _read_required(path: Path, label: str) -> str:
    if not path.exists():
        raise MigrationSmokeError(f"Missing {label}: {path}")
    return path.read_text(encoding="utf-8")


def _table_columns(sql: str, table: str) -> list[str]:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS model_registry\.{re.escape(table)} \((.*?)\n\);",
        sql,
        flags=re.DOTALL,
    )
    if not match:
        raise MigrationSmokeError(f"Missing CREATE TABLE for model_registry.{table}")
    columns: list[str] = []
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        first = line.split()[0]
        if first.upper() in {"CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK"}:
            continue
        columns.append(first.strip('"'))
    return columns


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise MigrationSmokeError(message)


def _validate_migration_sql(sql: str) -> dict[str, Any]:
    _require("CREATE SCHEMA IF NOT EXISTS model_registry" in sql, "Migration must create model_registry schema")
    _require("CREATE TABLE IF NOT EXISTS public." not in sql, "Migration must not create public.* tables")
    _require("DROP SCHEMA" not in sql.upper(), "Migration file must not drop schemas")

    columns_by_table: dict[str, list[str]] = {}
    for table in EXPECTED_TABLES:
        _require(
            f"CREATE TABLE IF NOT EXISTS model_registry.{table}" in sql,
            f"Missing table model_registry.{table}",
        )
        _require(f"COMMENT ON TABLE model_registry.{table}" in sql, f"Missing table comment for {table}")
        columns = _table_columns(sql, table)
        columns_by_table[table] = columns
        for column in columns:
            _require(
                f"COMMENT ON COLUMN model_registry.{table}.{column}" in sql,
                f"Missing column comment for {table}.{column}",
            )

    for view in EXPECTED_VIEWS:
        _require(
            f"CREATE OR REPLACE VIEW model_registry.{view}" in sql,
            f"Missing view model_registry.{view}",
        )
        _require(f"COMMENT ON VIEW model_registry.{view}" in sql, f"Missing view comment for {view}")

    for index in EXPECTED_INDEXES:
        _require(f"CREATE INDEX IF NOT EXISTS {index}" in sql, f"Missing index {index}")

    _require(
        "FALSE::BOOLEAN AS paper_selectable" in sql,
        "Model registry compatibility views must keep Paper raw-model selection disabled",
    )
    _require(
        "FROM public.aistock_model_catalog" in sql,
        "Legacy bridge must be read-only against public.aistock_model_catalog",
    )
    return {
        "schema": "model_registry",
        "tables": {table: {"column_count": len(columns)} for table, columns in columns_by_table.items()},
        "views": list(EXPECTED_VIEWS),
        "indexes": list(EXPECTED_INDEXES),
    }


def _validate_rollback_sql(sql: str) -> dict[str, Any]:
    _require("DROP SCHEMA IF EXISTS model_registry CASCADE" in sql, "Rollback SQL must state the schema drop plan")
    _require(CONFIRM_APPLY in sql or "DROP_MODEL_REGISTRY_PHASE5_DEV_ONLY" in sql, "Rollback SQL must be guarded by a confirmation token")
    _require("current_setting('aistock.model_registry_rollback_confirm'" in sql, "Rollback SQL must fail without a session confirmation setting")
    _require("IS DISTINCT FROM" in sql or "COALESCE(" in sql, "Rollback confirmation check must be NULL-safe")
    _require("RAISE EXCEPTION" in sql, "Rollback SQL must fail-fast when confirmation is absent")
    return {"guarded": True, "destructive": True}


def run_static_smoke(migration_path: Path = MIGRATION_PATH, rollback_path: Path = ROLLBACK_PATH) -> SmokeReport:
    migration_sql = _read_required(migration_path, "migration SQL")
    rollback_sql = _read_required(rollback_path, "rollback SQL")
    checks = {
        "migration": _validate_migration_sql(migration_sql),
        "rollback": _validate_rollback_sql(rollback_sql),
    }
    return SmokeReport(
        status="passed",
        mode="static_dry_run",
        migration_file=str(migration_path),
        rollback_file=str(rollback_path),
        checks=checks,
    )


def db_target_from_args(args: argparse.Namespace) -> DbTarget:
    return DbTarget(
        host=str(args.db_host or os.getenv("TDX_DB_HOST", "127.0.0.1")).strip(),
        port=int(args.db_port or os.getenv("TDX_DB_PORT", "5432")),
        dbname=str(args.db_name or os.getenv("TDX_DB_NAME", "aistock")).strip(),
        user=str(args.db_user or os.getenv("TDX_DB_USER", "postgres")).strip(),
    )


def production_like_reasons(target: DbTarget) -> list[str]:
    reasons: list[str] = []
    if target.host.lower() not in LOCAL_HOSTS:
        reasons.append("host_is_not_local")
    dbname = target.dbname.lower()
    if any(marker in dbname for marker in PRODUCTION_DB_MARKERS):
        reasons.append("dbname_contains_production_marker")
    if not any(marker in dbname for marker in DEV_DB_MARKERS):
        reasons.append("dbname_has_no_dev_marker")
    if dbname in {"aistock", "aistock_prod", "production", "prod"}:
        reasons.append("dbname_looks_production")
    app_env = (os.getenv("AISTOCK_ENV") or os.getenv("ENV") or "").strip().lower()
    if app_env in {"prod", "production"}:
        reasons.append("environment_is_production")
    return reasons


def _format_reasons(reasons: list[str]) -> str:
    return json.dumps(reasons, ensure_ascii=False)


def _require_db_execution_safety(args: argparse.Namespace, target: DbTarget) -> None:
    reasons = production_like_reasons(target)
    if args.db_transaction_check:
        _require(
            args.confirm_db_check == CONFIRM_DB_CHECK,
            f"--db-transaction-check requires --confirm-db-check {CONFIRM_DB_CHECK}",
        )
        _require(
            _env_truthy(ENV_DB_CHECK_ENABLED),
            f"--db-transaction-check requires {ENV_DB_CHECK_ENABLED}=true",
        )
        if reasons and not args.allow_production_like_rollback_check:
            raise MigrationSmokeError(
                "Refusing DB rollback check against production-like target "
                f"{target.label}: {_format_reasons(reasons)}"
            )
        if reasons and not _env_truthy(ENV_ALLOW_SUSPICIOUS_ROLLBACK):
            raise MigrationSmokeError(
                "Production-like rollback check requires "
                f"{ENV_ALLOW_SUSPICIOUS_ROLLBACK}=true in addition to the explicit flag"
            )
        return

    if args.apply:
        _require(args.confirm_apply == CONFIRM_APPLY, f"--apply requires --confirm-apply {CONFIRM_APPLY}")
        _require(_env_truthy(ENV_APPLY_ENABLED), f"--apply requires {ENV_APPLY_ENABLED}=true")
        if reasons:
            raise MigrationSmokeError(
                "Refusing --apply against production-like target "
                f"{target.label}: {_format_reasons(reasons)}"
            )


def _connect(target: DbTarget):
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise MigrationSmokeError("psycopg2 is required for DB execution checks") from exc
    return psycopg2.connect(**target.as_psycopg2_kwargs())


def _fetch_names(cur: Any, sql: str) -> set[str]:
    cur.execute(sql)
    return {str(row[0]) for row in cur.fetchall()}


def run_db_execution(
    *,
    target: DbTarget,
    migration_path: Path = MIGRATION_PATH,
    rollback_path: Path = ROLLBACK_PATH,
    apply: bool = False,
) -> SmokeReport:
    static_report = run_static_smoke(migration_path=migration_path, rollback_path=rollback_path)
    migration_sql = migration_path.read_text(encoding="utf-8")
    conn = _connect(target)
    committed = False
    transaction_finished = False
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '3s'")
            cur.execute("SET LOCAL statement_timeout = '60s'")
            cur.execute("SELECT to_regclass('public.aistock_model_catalog')")
            if cur.fetchone()[0] is None:
                raise MigrationSmokeError(
                    "public.aistock_model_catalog is required by the legacy bridge; "
                    "use an AIstock dev DB snapshot for DB execution smoke"
                )
            cur.execute(migration_sql)
            tables = _fetch_names(
                cur,
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'model_registry' AND table_type = 'BASE TABLE'
                """,
            )
            views = _fetch_names(
                cur,
                """
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = 'model_registry'
                """,
            )
            missing_tables = sorted(set(EXPECTED_TABLES) - tables)
            missing_views = sorted(set(EXPECTED_VIEWS) - views)
            if missing_tables or missing_views:
                raise MigrationSmokeError(
                    f"DB migration check missing tables={missing_tables} views={missing_views}"
                )
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pg_description d
                JOIN pg_class c ON c.oid = d.objoid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'model_registry'
                  AND d.description IS NOT NULL
                  AND d.description <> ''
                """
            )
            comment_count = int(cur.fetchone()[0])
            if comment_count <= len(EXPECTED_TABLES):
                raise MigrationSmokeError("DB migration check found too few model_registry comments")
        if apply:
            conn.commit()
            committed = True
            transaction_finished = True
        else:
            conn.rollback()
            transaction_finished = True
    finally:
        if not committed and not transaction_finished:
            conn.rollback()
        conn.close()

    checks = dict(static_report.checks)
    checks["db_execution"] = {
        "transaction": "committed" if apply else "rolled_back",
        "tables": list(EXPECTED_TABLES),
        "views": list(EXPECTED_VIEWS),
    }
    return SmokeReport(
        status="passed",
        mode="apply" if apply else "db_transaction_check_rolled_back",
        migration_file=str(migration_path),
        rollback_file=str(rollback_path),
        checks=checks,
        db_target=target.label,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--migration-file", default=str(MIGRATION_PATH), help="Path to Phase 5 migration SQL.")
    parser.add_argument("--rollback-file", default=str(ROLLBACK_PATH), help="Path to guarded rollback SQL.")
    parser.add_argument("--json", action="store_true", help="Emit JSON report.")
    parser.add_argument("--load-dotenv", action="store_true", help="Load .env before reading TDX_DB_* variables.")
    parser.add_argument("--db-transaction-check", action="store_true", help="Execute migration in a transaction and roll it back.")
    parser.add_argument("--confirm-db-check", default="", help=f"Required token: {CONFIRM_DB_CHECK}")
    parser.add_argument("--allow-production-like-rollback-check", action="store_true", help="Allow rollback-only DB check for a production-like target when the matching env guard is also set.")
    parser.add_argument("--apply", action="store_true", help="Apply migration to an explicitly marked dev DB.")
    parser.add_argument("--confirm-apply", default="", help=f"Required token: {CONFIRM_APPLY}")
    parser.add_argument("--db-host", help="DB host; defaults to TDX_DB_HOST or 127.0.0.1.")
    parser.add_argument("--db-port", type=int, help="DB port; defaults to TDX_DB_PORT or 5432.")
    parser.add_argument("--db-name", help="DB name; defaults to TDX_DB_NAME or aistock.")
    parser.add_argument("--db-user", help="DB user; defaults to TDX_DB_USER or postgres.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.load_dotenv:
        _load_dotenv(REPO_ROOT / ".env")
    if args.apply and args.db_transaction_check:
        raise SystemExit("--apply and --db-transaction-check are mutually exclusive")

    migration_path = Path(args.migration_file)
    rollback_path = Path(args.rollback_file)
    try:
        if args.apply or args.db_transaction_check:
            target = db_target_from_args(args)
            _require_db_execution_safety(args, target)
            report = run_db_execution(
                target=target,
                migration_path=migration_path,
                rollback_path=rollback_path,
                apply=bool(args.apply),
            )
        else:
            report = run_static_smoke(migration_path=migration_path, rollback_path=rollback_path)
        payload = asdict(report)
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            db_text = f" db_target={report.db_target}" if report.db_target else ""
            print(f"status={report.status} mode={report.mode}{db_text}")
        return 0
    except MigrationSmokeError as exc:
        payload = {
            "status": "failed",
            "mode": "apply" if args.apply else "db_transaction_check" if args.db_transaction_check else "static_dry_run",
            "error": str(exc),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"status=failed error={exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
