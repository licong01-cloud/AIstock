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
def e2e_test_run_id(dev_conn_provider):
    """Pick the Batch A run with the most fills (richest cross-table footprint).

    Returned to tests so they all reference the same run_id, and so cleanup
    can scope DELETEs to JUST that run_id (per Codex Stage 7.2 r1 P2.1).
    """
    with dev_conn_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT r.run_id FROM paper_v2.run r
                   WHERE EXISTS (SELECT 1 FROM paper_v2.fills WHERE run_id = r.run_id)
                   ORDER BY (
                     SELECT COUNT(*) FROM paper_v2.fills f WHERE f.run_id = r.run_id
                   ) DESC
                   LIMIT 1"""
            )
            row = cur.fetchone()
    if not row:
        pytest.skip("Batch A not loaded; run scripts/dev_db/batch_a_import_real_data.py first")
    return row[0]


@pytest.fixture
def cleanup_qe_archive(dev_conn_provider, e2e_test_run_id):
    """P2.1 (Codex Stage 7.2 r1): SCOPED cleanup. Replaces the previous
    blanket TRUNCATE of all 22 T12 tables. Now deletes only:
      - archive rows tied to the e2e_test_run_id (run-scoped)
      - dim/activation/audit rows tied to that run's portfolio_id
      - outbox_event rows whose event_id LIKE 'e2e_test_%'

    Rationale: the TRUNCATE approach wiped concurrent dev-DB testers' data
    in a shared environment. The scoped DELETE leaves other test fixtures
    untouched while still leaving a clean slate for THIS test.

    NEVER touches paper_v2 source rows.
    """
    # Tables with a direct run_id column (handler writes only for our run_id)
    RUN_SCOPED_TABLES = (
        "paper_v2_session_event", "paper_v2_run_event", "paper_v2_order_event",
        "paper_v2_order_execution_state", "paper_v2_order",
        "paper_v2_position_snapshot", "paper_v2_intraday_snapshot",
        "paper_v2_daily_snapshot", "paper_v2_cash_ledger", "paper_v2_error",
        "paper_v2_session_day", "paper_v2_session",
        "paper_v2_fill",  # partitioned but DELETE works against parent
    )
    # Tables scoped by portfolio_id (dim/activation/audit families)
    PORTFOLIO_SCOPED_TABLES = (
        "dim_paper_v2_portfolio",
        "paper_v2_config_change_audit",
        "paper_v2_runtime_config_activation",
        "paper_v2_execution_policy_activation",
        "paper_v2_reset_audit",
    )

    def _purge(conn, portfolio_id: str | None) -> None:
        with conn.cursor() as cur:
            # Run-scoped deletes (always safe — only this run's rows)
            for tbl in RUN_SCOPED_TABLES:
                cur.execute(
                    f"DELETE FROM qe_archive.{tbl} WHERE run_id = %s",
                    (e2e_test_run_id,),
                )
            # paper_v2_run last (FK target for above)
            cur.execute(
                "DELETE FROM qe_archive.paper_v2_run WHERE run_id = %s",
                (e2e_test_run_id,),
            )
            # Portfolio-scoped (only when we know the portfolio)
            if portfolio_id:
                for tbl in PORTFOLIO_SCOPED_TABLES:
                    cur.execute(
                        f"DELETE FROM qe_archive.{tbl} WHERE portfolio_id = %s",
                        (portfolio_id,),
                    )
                # runtime_profile dim is scoped via profile_id JOIN to portfolio
                cur.execute(
                    """DELETE FROM qe_archive.dim_paper_v2_runtime_profile_version
                       WHERE profile_id IN (
                           SELECT profile_id FROM paper_v2.runtime_profile
                           WHERE portfolio_id = %s
                       )""",
                    (portfolio_id,),
                )
                cur.execute(
                    """DELETE FROM qe_archive.dim_paper_v2_runtime_profile
                       WHERE profile_id IN (
                           SELECT profile_id FROM paper_v2.runtime_profile
                           WHERE portfolio_id = %s
                       )""",
                    (portfolio_id,),
                )
            # E2E test outbox events
            cur.execute(
                "DELETE FROM qe_archive.outbox_event WHERE event_id LIKE 'e2e_test_%'"
            )
        conn.commit()

    # Look up portfolio_id for the chosen run (safe SELECT on source)
    portfolio_id: str | None = None
    with dev_conn_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT portfolio_id FROM paper_v2.run WHERE run_id = %s",
                (e2e_test_run_id,),
            )
            r = cur.fetchone()
            if r:
                portfolio_id = r[0]
        # Pre-test purge
        _purge(conn, portfolio_id)

    yield

    with dev_conn_provider() as conn:
        _purge(conn, portfolio_id)
