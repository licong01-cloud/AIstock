"""Shared fixtures for Stage 7.3 deep data-quality assertions.

Connection contract (mirrors backend/tests/qe_archive/conftest.py — the
canonical pattern established by dw-foundation):

- ``dev_db_creds`` returns the dev DB credentials parsed from
  F:/Dev/AIstock/.env (TDX_DB_DEV_*). Refuses anything that isn't port 5433
  with 'dev' in the dbname. Skips the test when credentials are missing.
- ``dev_conn`` yields a live ``psycopg2`` connection. Skips when the DB
  is unreachable.
- ``archive_tables_ready`` skips when ``qe_archive.paper_v2_run`` does not
  exist on the dev DB (i.e. T12 has not been applied or this is a fresh
  CI runner). Tests that need source-only data (paper_v2.*) do NOT depend
  on this; tests that need archive data DO.

These tests are designed to skip cleanly when the dev DB is not present so
the ``data_quality_deep`` nox session is always green even on a fresh CI
runner that hasn't loaded Batch A + Batch C. Real coverage happens locally
or on the self-hosted runner with the full dev DB.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest


ENV_FILE = Path("F:/Dev/AIstock/.env")


def _parse_env() -> dict[str, str]:
    cfg: dict[str, str] = {}
    if not ENV_FILE.exists():
        return cfg
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def _dev_db_creds() -> dict[str, Any] | None:
    cfg = _parse_env()
    required = ("TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME",
                "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD")
    if not all(k in cfg for k in required):
        return None
    creds = {
        "host": cfg["TDX_DB_DEV_HOST"],
        "port": int(cfg["TDX_DB_DEV_PORT"]),
        "dbname": cfg["TDX_DB_DEV_NAME"],
        "user": cfg["TDX_DB_DEV_USER"],
        "password": cfg["TDX_DB_DEV_PASSWORD"],
    }
    # Hard safety: only literal 5433 + dbname containing 'dev'.
    if creds["port"] != 5433:
        return None
    if "dev" not in creds["dbname"].lower():
        return None
    return creds


@pytest.fixture(scope="session")
def dev_db_creds() -> dict[str, Any]:
    creds = _dev_db_creds()
    if creds is None:
        pytest.skip(
            "dev DB credentials missing or unsafe (need TDX_DB_DEV_* env, "
            "port=5433, dbname containing 'dev'); deep data-quality assertions "
            "skip cleanly."
        )
    return creds


@pytest.fixture
def dev_conn(dev_db_creds: dict[str, Any]):
    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed; deep data-quality assertions skip cleanly.")
    try:
        conn = psycopg2.connect(connect_timeout=3, **dev_db_creds)
    except Exception as exc:  # noqa: BLE001 - any connect failure -> skip with reason
        pytest.skip(f"dev DB unreachable at {dev_db_creds['host']}:{dev_db_creds['port']}: {exc}")
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname=%s AND tablename=%s",
            (schema, table),
        )
        return cur.fetchone() is not None


@pytest.fixture
def archive_tables_ready(dev_conn) -> bool:
    """Skip the test if qe_archive.paper_v2_run does not exist on dev DB."""
    if not _table_exists(dev_conn, "qe_archive", "paper_v2_run"):
        pytest.skip(
            "qe_archive.paper_v2_run absent on dev DB; T12 must be applied "
            "(scripts/dev_db/_apply_t12_dev.py on origin/claude/dw-foundation-20260510)."
        )
    return True


@pytest.fixture
def source_tables_ready(dev_conn) -> bool:
    """Skip when paper_v2.run is empty (Batch A not loaded)."""
    if not _table_exists(dev_conn, "paper_v2", "run"):
        pytest.skip("paper_v2.run table absent; baseline schema not present on dev DB.")
    with dev_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM paper_v2.run")
        count = cur.fetchone()[0]
    if count == 0:
        pytest.skip("paper_v2.run is empty; Batch A real-data import has not been loaded.")
    return True
