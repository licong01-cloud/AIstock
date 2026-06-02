from __future__ import annotations

import os
from pathlib import Path

import psycopg2


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend/db/migrations/ra_upgrade/004_code_context_refs.sql"
DOTENV = Path("F:/Dev/AIstock/.env")
EXPECTED_COLUMNS = {
    "code_context_ref_id": ("text", "NO", None),
    "context_pack_id": ("text", "NO", None),
    "task_id": ("text", "YES", None),
    "query_text": ("text", "NO", None),
    "file_path": ("text", "NO", None),
    "symbol": ("text", "NO", None),
    "status": ("text", "NO", "'ok'::text"),
    "summary_ref": ("text", "YES", None),
    "detail_ref": ("text", "YES", None),
    "edge_refs_json": ("jsonb", "NO", None),
    "affected_tests_json": ("jsonb", "NO", "'[]'::jsonb"),
    "manifest_json": ("jsonb", "NO", None),
    "provenance_json": ("jsonb", "NO", None),
    "as_of": ("timestamp with time zone", "NO", None),
    "created_at": ("timestamp with time zone", "NO", "now()"),
    "updated_at": ("timestamp with time zone", "NO", "now()"),
}


def _load_dev_db_creds(env: dict[str, str] | None = None, dotenv: Path = DOTENV) -> dict[str, object]:
    values: dict[str, str] = {}
    if dotenv.exists():
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"')
    source_env = env or os.environ
    for key in ("TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME", "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD"):
        values[key] = source_env.get(key, values.get(key, ""))
    missing = [key for key in ("TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME", "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD") if not values.get(key)]
    if missing:
        raise AssertionError(f"real Postgres DDL gate requires dev DB env, missing: {missing}")
    if values["TDX_DB_DEV_HOST"] not in {"127.0.0.1", "localhost"}:
        raise AssertionError("Phase8 DDL test only allows local validation Postgres, never production")
    db_name = values["TDX_DB_DEV_NAME"].lower()
    if "dev" not in db_name and "validation" not in db_name:
        raise AssertionError("Phase8 DDL test requires a dev/validation database name")
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
        WHERE table_schema=%s AND table_name='assistant_code_context_refs'
        ORDER BY ordinal_position
        """,
        (schema,),
    )
    columns = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname=%s AND tablename='assistant_code_context_refs'
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
        WHERE n.nspname=%s AND r.relname='assistant_code_context_refs'
        ORDER BY conname
        """,
        (schema,),
    )
    constraints = dict(cur.fetchall())
    cur.execute(
        """
        SELECT objsubid, description
        FROM pg_description d
        JOIN pg_class c ON c.oid=d.objoid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=%s AND c.relname='assistant_code_context_refs'
        ORDER BY objsubid
        """,
        (schema,),
    )
    comments = dict(cur.fetchall())
    return {"columns": columns, "indexes": indexes, "constraints": constraints, "comments": comments}


def test_code_context_refs_migration_is_idempotent_on_real_postgres() -> None:
    ddl = MIGRATION.read_text(encoding="utf-8")
    creds = _load_dev_db_creds()
    schema = "ra_phase8_code_context_refs_test"
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
    assert first == second, "004_code_context_refs.sql must be idempotent with zero pg_catalog diff"
    assert first["columns"] == EXPECTED_COLUMNS
    indexes = first["indexes"]
    for index_name in ("idx_accr_context_pack", "idx_accr_task_file", "idx_accr_as_of", "idx_accr_manifest_gin", "idx_accr_provenance_gin"):
        assert index_name in indexes
    constraints = " ".join(first["constraints"].values())
    for required in ("ck_accr_status", "ck_accr_edge_refs_array", "ck_accr_provenance_object", "ck_accr_no_test_success_claim"):
        assert required in first["constraints"]
    assert "passed" in constraints and "verified" in constraints and "state" in constraints
    comments = first["comments"]
    assert comments[0].startswith("Research Assistant code intelligence context refs")
    assert set(comments) == set(range(0, len(EXPECTED_COLUMNS) + 1))


def test_code_context_refs_ddl_env_guard_fails_fast_for_missing_or_unsafe_env(tmp_path: Path) -> None:
    safe_base = {
        "TDX_DB_DEV_HOST": "127.0.0.1",
        "TDX_DB_DEV_PORT": "5432",
        "TDX_DB_DEV_NAME": "aistock_dev",
        "TDX_DB_DEV_USER": "user",
        "TDX_DB_DEV_PASSWORD": "pw",
    }
    with pytest_raises_assertion("missing"):
        _load_dev_db_creds(env={**safe_base, "TDX_DB_DEV_PASSWORD": ""}, dotenv=tmp_path / "absent.env")
    with pytest_raises_assertion("local validation"):
        _load_dev_db_creds(env={**safe_base, "TDX_DB_DEV_HOST": "10.0.0.8"}, dotenv=tmp_path / "absent.env")
    with pytest_raises_assertion("dev/validation"):
        _load_dev_db_creds(env={**safe_base, "TDX_DB_DEV_NAME": "aistock_prod"}, dotenv=tmp_path / "absent.env")


class pytest_raises_assertion:
    def __init__(self, match: str) -> None:
        self.match = match

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, _tb) -> bool:
        assert exc_type is AssertionError
        assert self.match in str(exc)
        return True
