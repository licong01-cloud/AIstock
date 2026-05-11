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
    """P2.1 r2 (Codex Stage 7.2 r2): TIME-scoped cleanup for dim/audit tables.

    Round 1 introduced portfolio-scoped cleanup which Codex found over-broad
    (multiple test or non-test runs sharing a portfolio_id could be wiped).
    Round 2 splits the cleanup into two narrow scopes:

      Run-scoped DELETE: tables with a direct run_id column. DELETE WHERE
      run_id = e2e_test_run_id is the tightest possible scope; only the
      specific run's archive rows are touched.

      Time-scoped DELETE: dim/audit/activation tables that lack run_id.
      We capture test_started_at at fixture entry and post-test DELETE only
      rows whose captured_at >= test_started_at. Pre-existing rows (from
      prior tests / concurrent fixtures sharing the portfolio) are NOT
      touched. Worst-case leaves harmless duplicate SCD2 dim rows; handler
      is already idempotent on natural keys.

      Outbox: DELETE WHERE event_id LIKE 'e2e_test_%' — already tight scope.

    NEVER touches paper_v2 source rows.
    """
    from datetime import datetime, timezone

    # Tables with a direct run_id column — run-scoped DELETE is precise
    RUN_SCOPED_TABLES = (
        "paper_v2_session_event", "paper_v2_run_event", "paper_v2_order_event",
        "paper_v2_order_execution_state", "paper_v2_order",
        "paper_v2_position_snapshot", "paper_v2_intraday_snapshot",
        "paper_v2_daily_snapshot", "paper_v2_cash_ledger", "paper_v2_error",
        "paper_v2_session_day", "paper_v2_session",
        "paper_v2_fill",  # partitioned but DELETE works via parent
    )

    # Tables without run_id column — time-scoped DELETE
    # (only rows this test wrote post-fixture-start)
    TIME_SCOPED_TABLES = (
        "dim_paper_v2_portfolio",
        "dim_paper_v2_runtime_profile",
        "dim_paper_v2_runtime_profile_version",
        "paper_v2_config_change_audit",
        "paper_v2_runtime_config_activation",
        "paper_v2_execution_policy_activation",
        "paper_v2_reset_audit",
    )

    def _run_scoped_purge(conn) -> None:
        """Always safe pre- AND post-test cleanup of run-scoped rows."""
        with conn.cursor() as cur:
            for tbl in RUN_SCOPED_TABLES:
                cur.execute(
                    f"DELETE FROM qe_archive.{tbl} WHERE run_id = %s",
                    (e2e_test_run_id,),
                )
            cur.execute(
                "DELETE FROM qe_archive.paper_v2_run WHERE run_id = %s",
                (e2e_test_run_id,),
            )
            cur.execute(
                "DELETE FROM qe_archive.outbox_event WHERE event_id LIKE 'e2e_test_%'"
            )
        conn.commit()

    def _time_scoped_purge(conn, threshold) -> None:
        """Post-test only: delete dim/audit rows captured during this test."""
        with conn.cursor() as cur:
            for tbl in TIME_SCOPED_TABLES:
                cur.execute(
                    f"DELETE FROM qe_archive.{tbl} WHERE captured_at >= %s",
                    (threshold,),
                )
        conn.commit()

    # Pre-test: run-scoped purge only (don't time-scope on entry — might be
    # legitimate concurrent rows from other tests that we should not touch)
    with dev_conn_provider() as conn:
        _run_scoped_purge(conn)

    # Capture threshold AFTER pre-purge so post-test cleanup is tight
    test_started_at = datetime.now(timezone.utc)
    yield

    with dev_conn_provider() as conn:
        _run_scoped_purge(conn)
        _time_scoped_purge(conn, test_started_at)
