from __future__ import annotations

import os
import re
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv
import psycopg2
import pytest


ROOT = Path(__file__).resolve().parents[3]
MIGRATION_ROOT = ROOT / "backend" / "migrations"
PREFLIGHT = MIGRATION_ROOT / "v25_execution_algorithm_retirement_20260816.preflight.sql"
FORWARD = MIGRATION_ROOT / "v25_execution_algorithm_retirement_20260816.sql"
ROLLBACK = MIGRATION_ROOT / "v25_execution_algorithm_retirement_20260816.rollback.sql"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")


def _runtime_repo_root() -> Path:
    git_file = ROOT / ".git"
    if git_file.is_file():
        match = re.search(r"gitdir:\s*(.+)", git_file.read_text(encoding="utf-8"))
        if match:
            git_dir = Path(match.group(1).strip())
            if git_dir.name == "worktrees":
                git_dir = git_dir.parent
            if git_dir.parent.name == "worktrees":
                common_root = git_dir.parents[2]
                if (common_root / ".git").exists():
                    return common_root
    return ROOT


def _dev_dsn() -> dict[str, object]:
    if os.getenv("AISTOCK_RUN_V25_CATALOG_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized V25 catalog DEV PostgreSQL fixture")
    load_dotenv(_runtime_repo_root() / ".env", override=False)
    dsn: dict[str, object] = {
        "host": os.getenv("TDX_DB_DEV_HOST"),
        "port": int(os.getenv("TDX_DB_DEV_PORT", "0")),
        "dbname": os.getenv("TDX_DB_DEV_NAME"),
        "user": os.getenv("TDX_DB_DEV_USER"),
        "password": os.getenv("TDX_DB_DEV_PASSWORD"),
        "connect_timeout": 5,
    }
    if dsn["host"] != "127.0.0.1" or dsn["port"] != 5433 or "dev" not in str(dsn["dbname"]).lower():
        raise AssertionError(f"refusing non-DEV V25 catalog target {dsn['host']}:{dsn['port']}/{dsn['dbname']}")
    if not dsn["user"] or not dsn["password"]:
        pytest.fail("guarded DEV credentials are unavailable")
    return dsn


def test_retirement_migration_is_bounded_idempotent_and_history_preserving() -> None:
    preflight = _text(PREFLIGHT)
    forward = _text(FORWARD)
    rollback = _text(ROLLBACK)

    assert "REPEATABLE READ READ ONLY" in preflight
    assert "UPDATE public.execution_algorithm_catalog" in forward
    assert "algo_code IN ('V25_TWO_STAGE', 'V25_1_SMALL_CAP')" in forward
    assert "SET is_enabled = FALSE" in forward
    assert "DELETE" not in forward.upper()
    assert "INSERT" not in forward.upper()
    assert "default_config" not in forward
    assert "description" not in forward
    assert "Safe no-op" in rollback
    assert "UPDATE " not in rollback.upper()
    assert "DELETE " not in rollback.upper()


def test_retirement_migration_preserves_catalog_identity_on_dev_postgres() -> None:
    schema = "v25ret_" + uuid4().hex
    assert re.fullmatch(r"v25ret_[0-9a-f]{32}", schema)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    preflight = _text(PREFLIGHT).replace("public.execution_algorithm_catalog", f"{schema}.execution_algorithm_catalog")
    forward = _text(FORWARD).replace("public.execution_algorithm_catalog", f"{schema}.execution_algorithm_catalog")
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(
                f"""
                CREATE TABLE {schema}.execution_algorithm_catalog(
                    algo_code TEXT PRIMARY KEY,
                    description TEXT NOT NULL,
                    default_config JSONB NOT NULL,
                    is_enabled BOOLEAN NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_algorithm_catalog(algo_code,description,default_config,is_enabled)
                VALUES
                    ('V25_TWO_STAGE','historical two stage','{{"model":"early-late"}}'::jsonb,TRUE),
                    ('V25_1_SMALL_CAP','historical small cap','{{"model":"small-cap"}}'::jsonb,TRUE),
                    ('TWAP','active twap','{{"split_count":3}}'::jsonb,TRUE)
                """
            )
            cur.execute(preflight)
            cur.execute(forward)
            cur.execute(forward)
            cur.execute(
                f"SELECT algo_code,description,default_config,is_enabled FROM {schema}.execution_algorithm_catalog ORDER BY algo_code"
            )
            rows = cur.fetchall()
            assert rows == [
                ("TWAP", "active twap", {"split_count": 3}, True),
                ("V25_1_SMALL_CAP", "historical small cap", {"model": "small-cap"}, False),
                ("V25_TWO_STAGE", "historical two stage", {"model": "early-late"}, False),
            ]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()
