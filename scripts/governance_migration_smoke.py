"""Smoke checks for the full QE governance migration stack.

Default mode is a static dry-run: it validates the expected 2026-05-09
migration files and never opens a DB connection. Optional DB execution is
explicitly guarded and rolls back by default for dev/test targets only.
Production-readonly preflight is SELECT-only and records a live catalog
snapshot without applying DDL or writes.
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
from urllib.parse import parse_qs, urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "backend" / "migrations"

CONFIRM_DB_CHECK = "QE_GOVERNANCE_FULL_STACK_DEV_ROLLBACK_CHECK"
CONFIRM_APPLY = "APPLY_QE_GOVERNANCE_FULL_STACK_DEV_ONLY"
CONFIRM_PRODUCTION_PREFLIGHT = "QE_GOVERNANCE_PROD_READONLY_PREFLIGHT"
ENV_DB_CHECK_ENABLED = "AISTOCK_QE_GOVERNANCE_MIGRATION_DEV_DB"
ENV_APPLY_ENABLED = "AISTOCK_QE_GOVERNANCE_MIGRATION_APPLY_ENABLED"
ENV_PRODUCTION_PREFLIGHT_ENABLED = "AISTOCK_QE_GOVERNANCE_PROD_READONLY_PREFLIGHT"

DEV_DB_MARKERS = ("dev", "test", "sandbox", "local", "tmp", "temp", "ci")
PRODUCTION_DB_MARKERS = ("prod", "production", "live", "main")
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}

DESTRUCTIVE_PATTERNS = (
    r"\bDROP\s+SCHEMA\b",
    r"\bDROP\s+TABLE\b",
    r"\bTRUNCATE\b",
    r"\bDELETE\s+FROM\b",
    r"\bALTER\s+TABLE\b[^;]*\bDROP\s+(?:COLUMN|CONSTRAINT)\b",
)


class GovernanceMigrationSmokeError(RuntimeError):
    """Raised when the governance migration smoke gate fails."""


@dataclass(frozen=True)
class MigrationSpec:
    filename: str
    phase: str
    schema: str
    tables: tuple[str, ...] = ()
    views: tuple[str, ...] = ()
    indexes: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    alter_columns: tuple[tuple[str, str], ...] = ()
    required_markers: tuple[str, ...] = ()
    forbidden_markers: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()

    @property
    def path(self) -> Path:
        return MIGRATIONS_DIR / self.filename


STACK_SPECS: tuple[MigrationSpec, ...] = (
    MigrationSpec(
        filename="model_registry_phase5_20260509.sql",
        phase="phase5_model_registry",
        schema="model_registry",
        tables=(
            "model_template",
            "model_spec",
            "model_trial",
            "model_artifact",
            "model_lifecycle_event",
        ),
        views=(
            "v_qe_selectable_model_spec",
            "v_model_catalog_compat",
            "v_legacy_aistock_model_catalog_bridge",
        ),
        indexes=(
            "idx_model_spec_template",
            "idx_model_spec_qe_selectable",
            "idx_model_spec_source",
            "idx_model_trial_spec",
            "idx_model_trial_qe_lineage",
            "idx_model_artifact_trial",
            "idx_model_artifact_status",
            "idx_model_lifecycle_event_object",
        ),
        constraints=(
            "model_template_seed_capability_check",
            "model_spec_lifecycle_status_check",
            "model_trial_seed_policy_check",
            "model_artifact_protected_retention_check",
            "model_lifecycle_event_object_type_check",
        ),
        required_markers=(
            "CREATE SCHEMA IF NOT EXISTS model_registry",
            "FALSE::BOOLEAN AS paper_selectable",
            "FROM public.aistock_model_catalog",
            "COMMENT ON SCHEMA model_registry",
        ),
        forbidden_markers=("CREATE TABLE IF NOT EXISTS public.",),
    ),
    MigrationSpec(
        filename="strategy_pkg_promotion_review_20260509.sql",
        phase="phase1_promotion_review_standalone",
        schema="strategy_pkg",
        tables=("promotion_review",),
        indexes=("idx_strategy_pkg_promotion_review_status",),
        constraints=("UNIQUE (source_type, source_id)",),
        required_markers=(
            "Draft only: do not execute against production DB",
            "REVIEW_PENDING awaits human review",
            "does not imply approved SOTA or Paper eligibility",
        ),
    ),
    MigrationSpec(
        filename="qe_phase4_master_seed_contract_20260509.sql",
        phase="phase4_master_seed_contract",
        schema="strategy_pkg",
        tables=("seed_fragility_score",),
        indexes=("idx_seed_fragility_score_manifest", "idx_seed_fragility_score_policy"),
        constraints=(
            "package_seed_policy_check",
            "package_master_seed_range_check",
            "seed_fragility_score_policy_check",
            "seed_fragility_score_master_seed_range_check",
            "seed_fragility_score_sensitivity_nonnegative_check",
            "seed_fragility_score_rank_stability_range_check",
        ),
        alter_columns=(
            ("package", "seed_policy"),
            ("package", "master_seed"),
            ("package", "seed_sequence"),
            ("package", "seed_contract"),
            ("package", "seed_contract_sha256"),
            ("package", "reproducibility_level"),
            ("package", "nondeterministic_flags"),
        ),
        required_markers=(
            "All objects live in strategy_pkg schema; no public schema changes.",
            "IF NOT EXISTS (",
            "to_regclass('strategy_pkg.package')",
        ),
        depends_on=("strategy_pkg.package",),
    ),
    MigrationSpec(
        filename="strategy_pkg_runtime_variant_20260509.sql",
        phase="phase6_runtime_variants",
        schema="strategy_pkg",
        tables=("package_runtime_variant",),
        indexes=(
            "idx_package_runtime_variant_hash",
            "idx_package_runtime_variant_status",
            "idx_package_runtime_variant_core",
        ),
        constraints=(
            "package_runtime_variant_kind_check",
            "package_runtime_variant_validation_status_check",
            "package_runtime_variant_paper_candidate_check",
        ),
        required_markers=(
            "REFERENCES strategy_pkg.package(package_id) ON DELETE RESTRICT",
            "paper_candidate = FALSE OR validation_status = 'VALIDATION_PASSED'",
            "must not mutate frozen factors, model assets, or alpha core",
        ),
        depends_on=("strategy_pkg.package",),
    ),
    MigrationSpec(
        filename="strategy_pkg_validation_run_20260509.sql",
        phase="phase7_validation_runs",
        schema="strategy_pkg",
        tables=("package_validation_run",),
        indexes=(
            "idx_package_validation_run_package",
            "idx_package_validation_run_type_status",
            "idx_package_validation_run_variant",
        ),
        constraints=(
            "package_validation_type_check",
            "package_validation_retrain_mode_check",
            "package_validation_status_check",
            "package_validation_reproducibility_check",
            "package_validation_backtest_window_check",
            "package_validation_terminal_completed_check",
            "package_validation_passed_evidence_check",
            "package_validation_runtime_variant_check",
            "package_validation_latest_data_check",
            "package_validation_fixed_weight_check",
            "package_validation_retrain_seed_check",
            "package_validation_walk_forward_check",
        ),
        required_markers=(
            "original_fixed_weight",
            "latest_retrain",
            "walk_forward_rolling",
            "runtime_variant_backtest",
            "never overwrites frozen StrategyPackage manifest assets",
        ),
        depends_on=("strategy_pkg.package", "strategy_pkg.package_runtime_variant"),
    ),
    MigrationSpec(
        filename="strategy_pkg_package_asset_20260509.sql",
        phase="phase2_package_asset_ledger",
        schema="strategy_pkg",
        tables=("package_asset",),
        indexes=(
            "idx_package_asset_package_type",
            "idx_package_asset_protected",
            "idx_package_asset_package_ref",
        ),
        constraints=("package_asset_size_non_negative_check",),
        alter_columns=(
            ("package_asset", "asset_role"),
            ("package_asset", "asset_size_bytes"),
            ("package_asset", "protected_asset"),
            ("package_asset", "source_uri"),
        ),
        required_markers=(
            "protected asset ledger foundation",
            "protected_asset BOOLEAN NOT NULL DEFAULT TRUE",
            "does not authorize cleanup or overwrite",
        ),
        depends_on=("strategy_pkg.package",),
    ),
)

PHASE1A_APPLY_ORDER = (
    "strategy_pkg_package_asset_20260509.sql",
    "qe_phase4_master_seed_contract_20260509.sql",
    "strategy_pkg_runtime_variant_20260509.sql",
    "strategy_pkg_validation_run_20260509.sql",
    "strategy_pkg_promotion_review_20260509.sql",
    "model_registry_phase5_20260509.sql",
)


@dataclass(frozen=True)
class DbTarget:
    host: str
    port: int
    dbname: str
    user: str
    password: str = ""

    @property
    def label(self) -> str:
        return f"{self.user}@{self.host}:{self.port}/{self.dbname}"

    def as_psycopg2_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "application_name": "AIstock-qe-governance-migration-smoke",
            "options": "-c client_encoding=utf8",
        }


@dataclass(frozen=True)
class SmokeReport:
    status: str
    mode: str
    files: list[str]
    order: list[str]
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
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise GovernanceMigrationSmokeError(message)


def _fetch_one(cur: Any, sql: str) -> Any:
    cur.execute(sql)
    row = cur.fetchone()
    if not row:
        return None
    return row[0]


def _split_relation(relation: str) -> tuple[str, str]:
    if "." not in relation:
        raise GovernanceMigrationSmokeError(f"Expected schema-qualified relation name: {relation}")
    schema, name = relation.split(".", 1)
    return schema, name


def _relation_kind(cur: Any, schema: str, relation_name: str) -> str | None:
    cur.execute(
        """
        SELECT c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '%s' AND c.relname = '%s'
        """
        % (schema, relation_name)
    )
    row = cur.fetchone()
    if not row:
        return None
    return str(row[0])


def _relation_exists(cur: Any, relation: str) -> bool:
    schema, relation_name = _split_relation(relation)
    return _relation_kind(cur, schema, relation_name) is not None


def _table_exists(cur: Any, schema: str, table: str) -> bool:
    return _relation_kind(cur, schema, table) in {"r", "p"}


def _view_exists(cur: Any, schema: str, view: str) -> bool:
    return _relation_kind(cur, schema, view) == "v"


def _table_db_columns(cur: Any, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '%s' AND table_name = '%s'
        ORDER BY ordinal_position
        """
        % (schema, table)
    )
    return [str(row[0]) for row in cur.fetchall()]


def _schema_db_indexes(cur: Any, schema: str) -> dict[str, str]:
    cur.execute(
        """
        SELECT tablename, indexname
        FROM pg_indexes
        WHERE schemaname = '%s'
        ORDER BY indexname
        """
        % schema
    )
    return {str(row[1]): str(row[0]) for row in cur.fetchall()}


def _schema_db_named_constraints(cur: Any, schema: str) -> dict[str, str]:
    cur.execute(
        """
        SELECT r.relname, c.conname
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = '%s'
        ORDER BY c.conname
        """
        % schema
    )
    return {str(row[1]): str(row[0]) for row in cur.fetchall()}


def _read_required(path: Path) -> str:
    if not path.exists():
        raise GovernanceMigrationSmokeError(f"Missing migration file: {path}")
    return path.read_text(encoding="utf-8")


def _strip_sql_comments(sql: str) -> str:
    no_block = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return re.sub(r"--.*?$", "", no_block, flags=re.MULTILINE)


def _create_table_body(sql: str, schema: str, table: str) -> str:
    match = re.search(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{re.escape(schema)}\.{re.escape(table)}\s*\((.*?)\n\);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise GovernanceMigrationSmokeError(f"Missing CREATE TABLE for {schema}.{table}")
    return match.group(1)


def _table_columns(sql: str, schema: str, table: str) -> list[str]:
    body = _create_table_body(sql, schema, table)
    columns: list[str] = []
    for raw_line in body.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        raw_first = line.split()[0]
        first = raw_first.strip('"')
        if first.upper() in {"CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK"}:
            break
        if raw_first.startswith("'") or first.upper() in {"OR", "AND"}:
            continue
        columns.append(first)
    return columns


def _expected_columns_for_spec(spec: MigrationSpec) -> dict[str, list[str]]:
    sql = _read_required(spec.path)
    expected = {table: _table_columns(sql, spec.schema, table) for table in spec.tables}
    for table, column in spec.alter_columns:
        expected.setdefault(table, [])
        if column not in expected[table]:
            expected[table].append(column)
    return expected


def _named_constraint_markers(spec: MigrationSpec) -> list[str]:
    return [constraint for constraint in spec.constraints if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", constraint)]


def _expected_index_tables_for_spec(spec: MigrationSpec) -> dict[str, str]:
    sql = _read_required(spec.path)
    return {
        match.group(1): match.group(2)
        for match in re.finditer(
            rf"CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+([A-Za-z_][A-Za-z0-9_]*)\s+ON\s+"
            rf"{re.escape(spec.schema)}\.([A-Za-z_][A-Za-z0-9_]*)",
            sql,
            flags=re.IGNORECASE,
        )
        if match.group(1) in spec.indexes
    }


def _expected_named_constraint_tables_for_spec(spec: MigrationSpec) -> dict[str, str]:
    sql = _read_required(spec.path)
    expected_names = set(_named_constraint_markers(spec))
    tables_by_constraint: dict[str, str] = {}
    for table in spec.tables:
        try:
            body = _create_table_body(sql, spec.schema, table)
        except GovernanceMigrationSmokeError:
            continue
        for match in re.finditer(r"\bCONSTRAINT\s+([A-Za-z_][A-Za-z0-9_]*)\b", body, flags=re.IGNORECASE):
            constraint = match.group(1)
            if constraint in expected_names:
                tables_by_constraint[constraint] = table
    for match in re.finditer(
        rf"ALTER\s+TABLE\s+{re.escape(spec.schema)}\.([A-Za-z_][A-Za-z0-9_]*).*?"
        r"ADD\s+CONSTRAINT\s+([A-Za-z_][A-Za-z0-9_]*)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        table = match.group(1)
        constraint = match.group(2)
        if constraint in expected_names:
            tables_by_constraint[constraint] = table
    return tables_by_constraint


def _has_comment_for_column(sql: str, schema: str, table: str, column: str) -> bool:
    return re.search(
        rf"COMMENT\s+ON\s+COLUMN\s+{re.escape(schema)}\.{re.escape(table)}\.{re.escape(column)}\s+IS\s+'",
        sql,
        flags=re.IGNORECASE,
    ) is not None


def _validate_no_destructive_sql(sql: str, spec: MigrationSpec) -> None:
    uncommented = _strip_sql_comments(sql)
    for pattern in DESTRUCTIVE_PATTERNS:
        if re.search(pattern, uncommented, flags=re.IGNORECASE | re.DOTALL):
            raise GovernanceMigrationSmokeError(f"{spec.filename} contains destructive SQL matching {pattern}")


def _validate_spec(spec: MigrationSpec, sql: str) -> dict[str, Any]:
    _validate_no_destructive_sql(sql, spec)
    _require(
        f"CREATE SCHEMA IF NOT EXISTS {spec.schema}" in sql,
        f"{spec.filename} must create {spec.schema} schema idempotently",
    )
    for marker in spec.required_markers:
        _require(marker in sql, f"{spec.filename} missing safety/semantic marker: {marker}")
    for marker in spec.forbidden_markers:
        _require(marker not in sql, f"{spec.filename} contains forbidden marker: {marker}")

    columns_by_table: dict[str, list[str]] = {}
    for table in spec.tables:
        table_ref = f"{spec.schema}.{table}"
        _require(
            f"CREATE TABLE IF NOT EXISTS {table_ref}" in sql,
            f"{spec.filename} missing table {table_ref}",
        )
        _require(f"COMMENT ON TABLE {table_ref}" in sql, f"{spec.filename} missing table comment for {table_ref}")
        columns = _table_columns(sql, spec.schema, table)
        columns_by_table[table] = columns
        for column in columns:
            _require(
                _has_comment_for_column(sql, spec.schema, table, column),
                f"{spec.filename} missing column comment for {table_ref}.{column}",
            )

    for table, column in spec.alter_columns:
        _require(
            f"ADD COLUMN IF NOT EXISTS {column}" in sql,
            f"{spec.filename} missing idempotent ADD COLUMN for {spec.schema}.{table}.{column}",
        )
        _require(
            _has_comment_for_column(sql, spec.schema, table, column),
            f"{spec.filename} missing altered column comment for {spec.schema}.{table}.{column}",
        )

    for view in spec.views:
        view_ref = f"{spec.schema}.{view}"
        _require(f"CREATE OR REPLACE VIEW {view_ref}" in sql, f"{spec.filename} missing view {view_ref}")
        _require(f"COMMENT ON VIEW {view_ref}" in sql, f"{spec.filename} missing view comment for {view_ref}")

    for index in spec.indexes:
        _require(f"CREATE INDEX IF NOT EXISTS {index}" in sql or f"CREATE UNIQUE INDEX IF NOT EXISTS {index}" in sql,
                 f"{spec.filename} missing idempotent index {index}")

    for constraint in spec.constraints:
        _require(constraint in sql, f"{spec.filename} missing constraint/check marker {constraint}")

    return {
        "phase": spec.phase,
        "schema": spec.schema,
        "tables": {table: {"column_count": len(cols)} for table, cols in columns_by_table.items()},
        "views": list(spec.views),
        "indexes": list(spec.indexes),
        "constraints": list(spec.constraints),
        "alter_columns": [f"{table}.{column}" for table, column in spec.alter_columns],
        "depends_on": list(spec.depends_on),
    }


def _validate_order(specs: tuple[MigrationSpec, ...]) -> None:
    expected = [spec.filename for spec in STACK_SPECS]
    actual = [spec.filename for spec in specs]
    _require(actual == expected, f"Unexpected migration order: expected {expected}, got {actual}")
    seen_tables: set[str] = {"strategy_pkg.package", "public.aistock_model_catalog"}
    for spec in specs:
        missing = [dep for dep in spec.depends_on if dep not in seen_tables]
        _require(not missing, f"{spec.filename} appears before dependencies are available: {missing}")
        seen_tables.update(f"{spec.schema}.{table}" for table in spec.tables)


def run_static_smoke(specs: tuple[MigrationSpec, ...] = STACK_SPECS) -> SmokeReport:
    _validate_order(specs)
    checks: dict[str, Any] = {}
    files: list[str] = []
    for spec in specs:
        sql = _read_required(spec.path)
        checks[spec.filename] = _validate_spec(spec, sql)
        files.append(str(spec.path))
    return SmokeReport(
        status="passed",
        mode="static_dry_run",
        files=files,
        order=[spec.filename for spec in specs],
        checks=checks,
    )


def _catalog_state(cur: Any, spec: MigrationSpec, expected_columns: dict[str, list[str]]) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    indexes: dict[str, Any] = {}
    named_constraints: dict[str, Any] = {}
    views: dict[str, Any] = {}
    missing_tables: list[str] = []
    missing_views: list[str] = []
    missing_indexes: list[str] = []
    missing_named_constraints: list[str] = []
    missing_columns: list[str] = []

    for table, expected in expected_columns.items():
        relation = f"{spec.schema}.{table}"
        exists = _table_exists(cur, spec.schema, table)
        columns = _table_db_columns(cur, spec.schema, table) if exists else []
        table_missing_columns = [column for column in expected if column not in columns]
        if not exists:
            missing_tables.append(relation)
            table_missing_columns = expected[:]
        missing_columns.extend(f"{relation}.{column}" for column in table_missing_columns)
        tables[table] = {
            "exists": exists,
            "columns": columns,
            "expected_columns": expected,
            "missing_columns": table_missing_columns,
        }

    for view in spec.views:
        relation = f"{spec.schema}.{view}"
        exists = _view_exists(cur, spec.schema, view)
        if not exists:
            missing_views.append(relation)
        views[view] = {"exists": exists}

    db_indexes = _schema_db_indexes(cur, spec.schema)
    expected_index_tables = _expected_index_tables_for_spec(spec)
    for index in spec.indexes:
        expected_table = expected_index_tables.get(index)
        actual_table = db_indexes.get(index)
        index_exists = actual_table is not None and (expected_table is None or actual_table == expected_table)
        if not index_exists:
            missing_indexes.append(f"{spec.schema}.{index}")
        indexes[index] = {"exists": index_exists, "table": actual_table, "expected_table": expected_table}

    db_constraints = _schema_db_named_constraints(cur, spec.schema)
    expected_constraint_tables = _expected_named_constraint_tables_for_spec(spec)
    for constraint in _named_constraint_markers(spec):
        expected_table = expected_constraint_tables.get(constraint)
        actual_table = db_constraints.get(constraint)
        constraint_exists = actual_table is not None and (expected_table is None or actual_table == expected_table)
        if not constraint_exists:
            missing_named_constraints.append(f"{spec.schema}.{constraint}")
        named_constraints[constraint] = {"exists": constraint_exists, "table": actual_table, "expected_table": expected_table}

    missing_object_count = (
        len(missing_tables)
        + len(missing_views)
        + len(missing_indexes)
        + len(missing_named_constraints)
        + len(missing_columns)
    )
    return {
        "phase": spec.phase,
        "schema": spec.schema,
        "tables": tables,
        "views": views,
        "indexes": indexes,
        "named_constraints": named_constraints,
        "missing_tables": missing_tables,
        "missing_views": missing_views,
        "missing_indexes": missing_indexes,
        "missing_named_constraints": missing_named_constraints,
        "missing_columns": missing_columns,
        "missing_object_count": missing_object_count,
        "apply_needed": missing_object_count > 0,
    }


def run_production_readonly_preflight(
    *,
    target: DbTarget,
    specs: tuple[MigrationSpec, ...] = STACK_SPECS,
) -> SmokeReport:
    static_report = run_static_smoke(specs)
    conn = _connect(target)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            database_name = _fetch_one(cur, "SELECT current_database()")
            server_addr = _fetch_one(cur, "SELECT COALESCE(inet_server_addr()::text, '')")
            server_port = _fetch_one(cur, "SELECT inet_server_port()")
            base_dependencies = {
                "strategy_pkg.package": {"exists": _relation_exists(cur, "strategy_pkg.package")},
                "public.aistock_model_catalog": {"exists": _relation_exists(cur, "public.aistock_model_catalog")},
            }
            missing_base_dependencies = [name for name, state in base_dependencies.items() if not state["exists"]]
            expected_columns_by_spec = {spec.filename: _expected_columns_for_spec(spec) for spec in specs}
            spec_states = {
                spec.filename: _catalog_state(cur, spec, expected_columns_by_spec[spec.filename]) for spec in specs
            }
            total_missing_object_count = sum(state["missing_object_count"] for state in spec_states.values()) + len(
                missing_base_dependencies
            )
    finally:
        conn.close()

    checks = dict(static_report.checks)
    checks["production_preflight"] = {
        "database_name": database_name,
        "server_address": server_addr,
        "server_port": server_port,
        "base_dependencies": base_dependencies,
        "missing_base_dependencies": missing_base_dependencies,
        "specs": spec_states,
        "apply_needed": total_missing_object_count > 0,
        "total_missing_object_count": total_missing_object_count,
    }
    return SmokeReport(
        status="passed",
        mode="production_readonly_preflight",
        files=static_report.files,
        order=static_report.order,
        checks=checks,
        db_target=target.label,
    )


def _db_target_from_url(url: str) -> DbTarget | None:
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        return None
    query = parse_qs(parsed.query)
    return DbTarget(
        host=parsed.hostname or "127.0.0.1",
        port=parsed.port or 5432,
        dbname=(parsed.path or "/").lstrip("/") or "aistock",
        user=parsed.username or os.getenv("TDX_DB_USER", "postgres"),
        password=parsed.password or query.get("password", [os.getenv("TDX_DB_PASSWORD", "")])[0],
    )


def db_target_from_args(args: argparse.Namespace) -> DbTarget:
    from_url = _db_target_from_url(str(args.database_url or os.getenv("DATABASE_URL", "")))
    if from_url is not None and not any([args.db_host, args.db_port, args.db_name, args.db_user]):
        return from_url
    return DbTarget(
        host=str(args.db_host or os.getenv("TDX_DB_HOST", "127.0.0.1")).strip(),
        port=int(args.db_port or os.getenv("TDX_DB_PORT", "5432")),
        dbname=str(args.db_name or os.getenv("TDX_DB_NAME", "aistock")).strip(),
        user=str(args.db_user or os.getenv("TDX_DB_USER", "postgres")).strip(),
        password=str(args.db_password or os.getenv("TDX_DB_PASSWORD", "")),
    )


def production_like_reasons(target: DbTarget) -> list[str]:
    reasons: list[str] = []
    host = target.host.lower()
    dbname = target.dbname.lower()
    if host not in LOCAL_HOSTS:
        reasons.append("host_is_not_local")
    if any(marker in dbname for marker in PRODUCTION_DB_MARKERS):
        reasons.append("dbname_contains_production_marker")
    if not any(marker in dbname for marker in DEV_DB_MARKERS):
        reasons.append("dbname_has_no_dev_marker")
    if dbname in {"aistock", "aistock_prod", "production", "prod", "main"}:
        reasons.append("dbname_looks_production")
    app_env = (os.getenv("AISTOCK_ENV") or os.getenv("ENV") or "").strip().lower()
    if app_env in {"prod", "production", "live"}:
        reasons.append("environment_is_production")
    return reasons


def _require_db_execution_safety(args: argparse.Namespace, target: DbTarget) -> None:
    reasons = production_like_reasons(target)
    if args.db_transaction_check:
        _require(
            args.confirm_db_check == CONFIRM_DB_CHECK,
            f"--db-transaction-check requires --confirm-db-check {CONFIRM_DB_CHECK}",
        )
        _require(_env_truthy(ENV_DB_CHECK_ENABLED), f"--db-transaction-check requires {ENV_DB_CHECK_ENABLED}=true")
        if reasons:
            raise GovernanceMigrationSmokeError(
                "Refusing DB transaction smoke against production-like target "
                f"{target.label}: {json.dumps(reasons, ensure_ascii=False)}"
            )
        return

    if args.apply:
        _require(args.confirm_apply == CONFIRM_APPLY, f"--apply requires --confirm-apply {CONFIRM_APPLY}")
        _require(_env_truthy(ENV_APPLY_ENABLED), f"--apply requires {ENV_APPLY_ENABLED}=true")
        if reasons:
            raise GovernanceMigrationSmokeError(
                "Refusing --apply against production-like target "
                f"{target.label}: {json.dumps(reasons, ensure_ascii=False)}"
            )


def _connect(target: DbTarget):
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise GovernanceMigrationSmokeError("psycopg2 is required for DB execution checks") from exc
    try:
        return psycopg2.connect(**target.as_psycopg2_kwargs())
    except Exception as exc:  # pragma: no cover - exact driver error depends on local DB setup
        raise GovernanceMigrationSmokeError(f"failed to connect to DB target {target.label}: {exc}") from exc


def _fetch_names(cur: Any, sql: str) -> set[str]:
    cur.execute(sql)
    return {str(row[0]) for row in cur.fetchall()}


def _specs_in_apply_order(specs: tuple[MigrationSpec, ...] = STACK_SPECS) -> tuple[MigrationSpec, ...]:
    by_filename = {spec.filename: spec for spec in specs}
    if set(by_filename) != set(PHASE1A_APPLY_ORDER):
        raise GovernanceMigrationSmokeError("Unexpected Phase 1A apply spec set")
    return tuple(by_filename[filename] for filename in PHASE1A_APPLY_ORDER)


def _verify_catalog_after_apply(cur: Any, specs: tuple[MigrationSpec, ...]) -> None:
    expected_columns_by_spec = {spec.filename: _expected_columns_for_spec(spec) for spec in specs}
    missing: dict[str, dict[str, Any]] = {}
    for spec in specs:
        state = _catalog_state(cur, spec, expected_columns_by_spec[spec.filename])
        if state["missing_object_count"]:
            missing[spec.filename] = {
                "missing_object_count": state["missing_object_count"],
                "missing_tables": state["missing_tables"],
                "missing_views": state["missing_views"],
                "missing_indexes": state["missing_indexes"],
                "missing_named_constraints": state["missing_named_constraints"],
                "missing_columns": state["missing_columns"],
            }
    if missing:
        raise GovernanceMigrationSmokeError(f"DB smoke missing objects after apply: {json.dumps(missing, ensure_ascii=False)}")


def run_db_execution(*, target: DbTarget, apply: bool = False) -> SmokeReport:
    static_report = run_static_smoke()
    conn = _connect(target)
    apply_order = _specs_in_apply_order()
    applied_files: list[str] = []
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('strategy_pkg.package'), to_regclass('public.aistock_model_catalog')")
            package_regclass, catalog_regclass = cur.fetchone()
        if package_regclass is None:
            raise GovernanceMigrationSmokeError("strategy_pkg.package is required before governance stack smoke")
        if catalog_regclass is None:
            raise GovernanceMigrationSmokeError("public.aistock_model_catalog is required by model registry bridge")
        conn.autocommit = False

        if not apply:
            try:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL lock_timeout = '3s'")
                    cur.execute("SET LOCAL statement_timeout = '120s'")
                    for spec in apply_order:
                        cur.execute(spec.path.read_text(encoding="utf-8"))
                    _verify_catalog_after_apply(cur, apply_order)
                conn.rollback()
            except Exception:
                conn.rollback()
                raise
        else:
            for spec in apply_order:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SET LOCAL lock_timeout = '3s'")
                        cur.execute("SET LOCAL statement_timeout = '120s'")
                        cur.execute(spec.path.read_text(encoding="utf-8"))
                    conn.commit()
                    applied_files.append(spec.filename)
                except Exception:
                    conn.rollback()
                    raise

            with conn.cursor() as cur:
                _verify_catalog_after_apply(cur, apply_order)
            conn.rollback()
    finally:
        conn.close()

    checks = dict(static_report.checks)
    checks["db_execution"] = {
        "transaction": "committed_per_file" if apply else "rolled_back",
        "apply_order": [spec.filename for spec in apply_order],
        "applied_files": applied_files,
    }
    return SmokeReport(
        status="passed",
        mode="apply" if apply else "db_transaction_check_rolled_back",
        files=static_report.files,
        order=[spec.filename for spec in apply_order],
        checks=checks,
        db_target=target.label,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit JSON report.")
    parser.add_argument("--load-dotenv", action="store_true", help="Load .env before reading DB variables.")
    parser.add_argument(
        "--production-readonly-preflight",
        action="store_true",
        help="Run a SELECT-only production preflight that snapshots live catalog state without DDL or writes.",
    )
    parser.add_argument(
        "--confirm-production-readonly-preflight",
        default="",
        help=f"Required token: {CONFIRM_PRODUCTION_PREFLIGHT}",
    )
    parser.add_argument("--db-transaction-check", action="store_true", help="Execute full stack in a transaction and roll it back.")
    parser.add_argument("--confirm-db-check", default="", help=f"Required token: {CONFIRM_DB_CHECK}")
    parser.add_argument("--apply", action="store_true", help="Apply migrations to an explicitly marked dev/test DB.")
    parser.add_argument("--confirm-apply", default="", help=f"Required token: {CONFIRM_APPLY}")
    parser.add_argument("--database-url", help="Optional PostgreSQL URL; overridden by explicit DB flags.")
    parser.add_argument("--db-host", help="DB host; defaults to TDX_DB_HOST or 127.0.0.1.")
    parser.add_argument("--db-port", type=int, help="DB port; defaults to TDX_DB_PORT or 5432.")
    parser.add_argument("--db-name", help="DB name; defaults to TDX_DB_NAME or aistock.")
    parser.add_argument("--db-user", help="DB user; defaults to TDX_DB_USER or postgres.")
    parser.add_argument("--db-password", help="DB password; defaults to TDX_DB_PASSWORD.")
    return parser


def _require_production_readonly_preflight_safety(args: argparse.Namespace) -> None:
    _require(
        args.confirm_production_readonly_preflight == CONFIRM_PRODUCTION_PREFLIGHT,
        f"--production-readonly-preflight requires --confirm-production-readonly-preflight {CONFIRM_PRODUCTION_PREFLIGHT}",
    )
    _require(
        _env_truthy(ENV_PRODUCTION_PREFLIGHT_ENABLED),
        f"--production-readonly-preflight requires {ENV_PRODUCTION_PREFLIGHT_ENABLED}=true",
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.load_dotenv:
        _load_dotenv(REPO_ROOT / ".env")
    if sum(bool(flag) for flag in (args.production_readonly_preflight, args.apply, args.db_transaction_check)) > 1:
        raise SystemExit("--production-readonly-preflight, --apply, and --db-transaction-check are mutually exclusive")

    try:
        if args.production_readonly_preflight:
            _require_production_readonly_preflight_safety(args)
            target = db_target_from_args(args)
            report = run_production_readonly_preflight(target=target)
        elif args.apply or args.db_transaction_check:
            target = db_target_from_args(args)
            _require_db_execution_safety(args, target)
            report = run_db_execution(target=target, apply=bool(args.apply))
        else:
            report = run_static_smoke()
        payload = asdict(report)
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            db_text = f" db_target={report.db_target}" if report.db_target else ""
            print(f"status={report.status} mode={report.mode}{db_text}")
        return 0
    except GovernanceMigrationSmokeError as exc:
        payload = {
            "status": "failed",
            "mode": (
                "production_readonly_preflight"
                if args.production_readonly_preflight
                else "apply"
                if args.apply
                else "db_transaction_check"
                if args.db_transaction_check
                else "static_dry_run"
            ),
            "error": str(exc),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"status=failed error={exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
