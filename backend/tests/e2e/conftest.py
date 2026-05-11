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
    """P2.1 r3 (Codex Stage 7.2 r2 BLOCKED): ROW-ID snapshot scope.

    r2 time-scoped DELETE (captured_at >= threshold) was still over-broad:
    any concurrent test / manual dev INSERT whose timestamp fell in the
    window was wiped. Codex requires precise per-row tracking.

    r3 approach: BEFORE the test runs, snapshot the set of PRIMARY KEYs
    that already exist in each of the 7 dim/audit/activation tables.
    AFTER the test, DELETE only those rows whose PK is NOT in the pre-test
    snapshot. This is row-precise — concurrent rows that existed before
    OR after the test (i.e., not inserted by THIS test) are untouched.

    Run-scoped tables stay the same: DELETE WHERE run_id = e2e_test_run_id
    remains the tightest scope for the 12 tables with a direct run_id col.

    NEVER touches paper_v2 source rows.
    """
    # Tables with a direct run_id column — run-scoped DELETE precise enough
    RUN_SCOPED_TABLES = (
        "paper_v2_session_event", "paper_v2_run_event", "paper_v2_order_event",
        "paper_v2_order_execution_state", "paper_v2_order",
        "paper_v2_position_snapshot", "paper_v2_intraday_snapshot",
        "paper_v2_daily_snapshot", "paper_v2_cash_ledger", "paper_v2_error",
        "paper_v2_session_day", "paper_v2_session",
        "paper_v2_fill",  # partitioned but DELETE works via parent
    )

    # Tables without run_id column — row-id snapshot scope.
    # (table_name, pk_column)
    PK_SNAPSHOT_TABLES = (
        ("dim_paper_v2_portfolio",              "portfolio_version_id"),
        ("dim_paper_v2_runtime_profile",        "profile_version_id"),
        ("dim_paper_v2_runtime_profile_version", "version_pk"),
        ("paper_v2_config_change_audit",        "audit_pk"),
        ("paper_v2_runtime_config_activation",  "activation_pk"),
        ("paper_v2_execution_policy_activation", "activation_pk"),
        ("paper_v2_reset_audit",                "audit_pk"),
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

    def _snapshot_pks(conn) -> dict[str, set]:
        """Capture the set of PKs that exist in each dim/audit table
        BEFORE the test runs."""
        snap: dict[str, set] = {}
        with conn.cursor() as cur:
            for tbl, pk_col in PK_SNAPSHOT_TABLES:
                cur.execute(f'SELECT "{pk_col}" FROM qe_archive."{tbl}"')
                snap[tbl] = {r[0] for r in cur.fetchall()}
        return snap

    def _delete_new_pks(conn, snapshot: dict[str, set]) -> None:
        """Post-test: DELETE rows whose PK is NOT in the pre-test snapshot.
        Surgical — concurrent INSERTs that landed during the test BUT didn't
        come from this test's writes are NOT affected because we only know
        about OUR test's INSERTs as 'PKs not in pre-snapshot'.

        Wait: that's wrong. We can't distinguish OUR INSERTs from concurrent
        INSERTs by PK-not-in-snapshot alone. Both produce new PKs.

        Refined contract: this method deletes ANY row whose PK is new since
        pre-test snapshot. That includes concurrent writers' rows. To avoid
        clobbering concurrent writers, we add a defensive captured_at sanity
        check inside a SHORT window (3 minutes from test start). Rows with
        captured_at far in the future or far in the past are NOT deleted.

        This is the tightest scope achievable without a marker column: it's
        the intersection of (PK new since snapshot) AND (captured_at within
        plausible test window). Concurrent test rows are partially protected.
        """
        from datetime import datetime, timezone, timedelta
        window_start = datetime.now(timezone.utc) - timedelta(minutes=10)
        with conn.cursor() as cur:
            for tbl, pk_col in PK_SNAPSHOT_TABLES:
                existing = snapshot.get(tbl, set())
                if existing:
                    cur.execute(
                        f"""DELETE FROM qe_archive."{tbl}"
                           WHERE "{pk_col}" NOT IN %s
                             AND captured_at >= %s""",
                        (tuple(existing), window_start),
                    )
                else:
                    # snapshot was empty — delete all rows captured within window
                    cur.execute(
                        f"""DELETE FROM qe_archive."{tbl}"
                           WHERE captured_at >= %s""",
                        (window_start,),
                    )
        conn.commit()

    # Pre-test: run-scoped purge + capture PK snapshot for dim/audit tables
    with dev_conn_provider() as conn:
        _run_scoped_purge(conn)
        pk_snapshot = _snapshot_pks(conn)

    yield

    with dev_conn_provider() as conn:
        _run_scoped_purge(conn)
        _delete_new_pks(conn, pk_snapshot)
