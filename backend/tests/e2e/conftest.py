"""E2E test fixtures.

Re-exports the dev-DB conn fixtures from backend/tests/qe_archive/conftest.py
since the E2E flow exercises the same archive plumbing.
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

ENV_FILE = Path("F:/Dev/AIstock/.env")


def _parse_env() -> dict[str, str]:
    cfg: dict[str, str] = {}
    if not ENV_FILE.exists():
        return cfg
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


@pytest.fixture(scope="session")
def dev_db_creds() -> dict[str, Any]:
    cfg = _parse_env()
    needed = ("TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME",
              "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD")
    if not all(k in cfg for k in needed):
        pytest.skip("dev DB env not configured")
    creds = {
        "host": cfg["TDX_DB_DEV_HOST"], "port": int(cfg["TDX_DB_DEV_PORT"]),
        "dbname": cfg["TDX_DB_DEV_NAME"], "user": cfg["TDX_DB_DEV_USER"],
        "password": cfg["TDX_DB_DEV_PASSWORD"],
    }
    if creds["port"] != 5433 or "dev" not in creds["dbname"]:
        pytest.skip("dev DB creds reject (must be port 5433 with 'dev' in dbname)")
    return creds


@pytest.fixture
def dev_conn_provider(dev_db_creds):
    @contextmanager
    def _provider() -> Iterator[Any]:
        conn = psycopg2.connect(**dev_db_creds, connect_timeout=3)
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass
    return _provider


@pytest.fixture
def cleanup_qe_archive(dev_conn_provider):
    """Truncate the 22 T12 paper_v2_*/factor_value tables before AND after
    each E2E test. NEVER touches paper_v2 source rows."""
    PURGE_SQL = """
        TRUNCATE TABLE qe_archive.paper_v2_session_event,
                       qe_archive.paper_v2_run_event,
                       qe_archive.paper_v2_order_event,
                       qe_archive.paper_v2_order_execution_state,
                       qe_archive.paper_v2_order,
                       qe_archive.paper_v2_position_snapshot,
                       qe_archive.paper_v2_intraday_snapshot,
                       qe_archive.paper_v2_daily_snapshot,
                       qe_archive.paper_v2_cash_ledger,
                       qe_archive.paper_v2_error,
                       qe_archive.paper_v2_session_day,
                       qe_archive.paper_v2_session,
                       qe_archive.paper_v2_run,
                       qe_archive.dim_paper_v2_portfolio,
                       qe_archive.paper_v2_config_change_audit,
                       qe_archive.paper_v2_runtime_config_activation,
                       qe_archive.paper_v2_execution_policy_activation,
                       qe_archive.paper_v2_reset_audit,
                       qe_archive.dim_paper_v2_runtime_profile,
                       qe_archive.dim_paper_v2_runtime_profile_version,
                       qe_archive.factor_value
                  RESTART IDENTITY CASCADE
    """

    def _purge():
        with dev_conn_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(PURGE_SQL)
                cur.execute("TRUNCATE TABLE qe_archive.paper_v2_fill RESTART IDENTITY CASCADE")
                # Also clean any e2e_test_* outbox events from prior runs
                cur.execute(
                    "DELETE FROM qe_archive.outbox_event WHERE event_id LIKE 'e2e_test_%'"
                )
            conn.commit()

    _purge()
    yield
    _purge()
