from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend/db/migrations/ra_upgrade/002_agent_teams.sql"
DOTENV = Path("F:/Dev/AIstock/.env")
EXPECTED_COLUMNS = {
    "agent_run_id": ("text", "NO", None),
    "parent_task_id": ("text", "NO", None),
    "agent_key": ("text", "NO", None),
    "role": ("text", "NO", None),
    "status": ("text", "NO", "'queued'::text"),
    "input_json": ("jsonb", "NO", "'{}'::jsonb"),
    "result_json": ("jsonb", "NO", "'{}'::jsonb"),
    "model_profile_id": ("text", "YES", None),
    "trace_id": ("text", "YES", None),
    "created_at": ("timestamp with time zone", "NO", "now()"),
    "updated_at": ("timestamp with time zone", "NO", "now()"),
}


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
    missing = [key for key in values if key.startswith("TDX_DB_DEV_") and not values[key]]
    if missing:
        raise AssertionError(f"real Postgres DDL gate requires dev DB env, missing: {missing}")
    if values["TDX_DB_DEV_HOST"] not in {"127.0.0.1", "localhost"}:
        raise AssertionError("Phase5 DDL test only allows local validation Postgres, never production")
    if "dev" not in values["TDX_DB_DEV_NAME"].lower():
        raise AssertionError("Phase5 DDL test requires a dev/validation database name")
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
        WHERE table_schema=%s AND table_name='assistant_agent_runs'
        ORDER BY ordinal_position
        """,
        (schema,),
    )
    columns = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname=%s AND tablename='assistant_agent_runs'
        ORDER BY indexname
        """,
        (schema,),
    )
    indexes = dict(cur.fetchall())
    cur.execute(
        """
        SELECT objsubid, description
        FROM pg_description d
        JOIN pg_class c ON c.oid=d.objoid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=%s AND c.relname='assistant_agent_runs'
        ORDER BY objsubid
        """,
        (schema,),
    )
    comments = dict(cur.fetchall())
    return {"columns": columns, "indexes": indexes, "comments": comments}


def test_agent_teams_migration_is_idempotent_on_real_postgres() -> None:
    ddl = MIGRATION.read_text(encoding="utf-8")
    creds = _load_dev_db_creds()
    schema = "ra_phase5_agent_teams_test"
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
    assert first == second, "002_agent_teams.sql must be idempotent with zero pg_catalog diff"
    assert first["columns"] == EXPECTED_COLUMNS
    indexes = first["indexes"]
    assert "idx_aar_parent" in indexes
    assert "parent_task_id" in indexes["idx_aar_parent"] and "status" in indexes["idx_aar_parent"]
    comments = first["comments"]
    assert comments[0].startswith("Agent Teams")
    assert set(comments) == set(range(0, len(EXPECTED_COLUMNS) + 1))
