from __future__ import annotations

import os
from pathlib import Path

import psycopg2


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend/db/migrations/ra_upgrade/003_qe_autonomy.sql"
DOTENV = Path("F:/Dev/AIstock/.env")
EXPECTED_COLUMNS = {
    "auto_run_id": ("text", "NO", None),
    "qe_task_id": ("text", "NO", None),
    "methodology_ref": ("text", "YES", None),
    "stop_conditions_json": ("jsonb", "NO", None),
    "budget_json": ("jsonb", "NO", None),
    "status": ("text", "NO", None),
    "loops_completed": ("integer", "NO", "0"),
    "last_verdict_json": ("jsonb", "YES", None),
    "created_at": ("timestamp with time zone", "NO", "now()"),
    "updated_at": ("timestamp with time zone", "NO", "now()"),
}
EXPECTED_STATUS = {"running", "stopped_target", "stopped_no_improve", "stopped_budget", "failed"}


def _load_dev_db_creds() -> dict[str, object]:
    values: dict[str, str] = {}
    if DOTENV.exists():
        for line in DOTENV.read_text(encoding="utf-8").splitlines():
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"')
    for key in ("TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME", "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD"):
        values.setdefault(key, os.environ.get(key, ""))
    missing = [key for key in ("TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME", "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD") if not values.get(key)]
    if missing:
        raise AssertionError(f"real Postgres DDL gate requires dev DB env, missing: {missing}")
    if values["TDX_DB_DEV_HOST"] not in {"127.0.0.1", "localhost"}:
        raise AssertionError("Phase6 DDL test only allows local validation Postgres, never production")
    db_name = values["TDX_DB_DEV_NAME"].lower()
    if "dev" not in db_name and "validation" not in db_name:
        raise AssertionError("Phase6 DDL test requires a dev/validation database name")
    return {
        "host": values["TDX_DB_DEV_HOST"],
        "port": int(values["TDX_DB_DEV_PORT"]),
        "dbname": values["TDX_DB_DEV_NAME"],
        "user": values["TDX_DB_DEV_USER"],
        "password": values["TDX_DB_DEV_PASSWORD"],
        "connect_timeout": 3,
    }


def _snapshot(cur, schema: str) -> dict[str, object]:
    cur.execute(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name='qe_autonomous_evolution_runs'
        ORDER BY ordinal_position
        """,
        (schema,),
    )
    columns = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname=%s AND tablename='qe_autonomous_evolution_runs'
        ORDER BY indexname
        """,
        (schema,),
    )
    indexes = dict(cur.fetchall())
    cur.execute(
        """
        SELECT conname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class r ON r.oid=c.conrelid
        JOIN pg_namespace n ON n.oid=r.relnamespace
        WHERE n.nspname=%s AND r.relname='qe_autonomous_evolution_runs'
        ORDER BY conname
        """,
        (schema,),
    )
    checks = dict(cur.fetchall())
    cur.execute(
        """
        SELECT objsubid, description
        FROM pg_description d
        JOIN pg_class c ON c.oid=d.objoid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=%s AND c.relname='qe_autonomous_evolution_runs'
        ORDER BY objsubid
        """,
        (schema,),
    )
    comments = dict(cur.fetchall())
    return {"columns": columns, "indexes": indexes, "checks": checks, "comments": comments}


def test_qe_autonomy_migration_is_idempotent_on_real_postgres() -> None:
    ddl = MIGRATION.read_text(encoding="utf-8")
    creds = _load_dev_db_creds()
    schema = "ra_phase6_qe_autonomy_test"
    with psycopg2.connect(**creds) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{schema}"')
            try:
                cur.execute(f'SET search_path TO "{schema}"')
                cur.execute(ddl)
                first = _snapshot(cur, schema)
                cur.execute(ddl)
                second = _snapshot(cur, schema)
            finally:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    assert first == second, "003_qe_autonomy.sql must be idempotent with zero pg_catalog diff"
    assert first["columns"] == EXPECTED_COLUMNS
    indexes = first["indexes"]
    assert "idx_qaer_task_status" in indexes
    assert "qe_task_id" in indexes["idx_qaer_task_status"] and "status" in indexes["idx_qaer_task_status"]
    assert "idx_qaer_updated_at" in indexes
    status_check = " ".join(first["checks"].values())
    for status in EXPECTED_STATUS:
        assert status in status_check
    assert "approval_required" not in status_check
    comments = first["comments"]
    assert comments[0].startswith("QE 自主演进")
    assert set(comments) == set(range(0, len(EXPECTED_COLUMNS) + 1))
