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
def e2e_test_portfolio_id(dev_conn_provider, e2e_test_run_id) -> str | None:
    """Portfolio_id tied to the chosen e2e_test_run_id. Used as ownership
    label for cleanup_qe_archive (P1.3 r4 fix)."""
    with dev_conn_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT portfolio_id FROM paper_v2.run WHERE run_id = %s",
                (e2e_test_run_id,),
            )
            row = cur.fetchone()
    return row[0] if row else None


@pytest.fixture
def cleanup_qe_archive(dev_conn_provider, e2e_test_run_id, e2e_test_portfolio_id):
    """P1.3 r4 (Codex Stage 7.2 r3 BLOCKED): OWNERSHIP-LABELED cleanup.

    r3 PK-snapshot + time-window still wiped rows that concurrent writers
    inserted DURING the test (because their PKs are also "not in pre-snapshot").
    Codex requires explicit ownership scope.

    r4 approach: limit cleanup to rows whose ownership column ties them to
    THIS test's resources:
      - portfolio_id = e2e_test_portfolio_id (for portfolio-tied tables)
      - profile_id IN (test portfolio's profile_ids) (for runtime_profile dim)
    Plus the PK-not-in-snapshot + captured_at intersection from r3, so
    rows that pre-existed for the same portfolio AND rows captured outside
    the test window are also untouched.

    Triple intersection makes the cleanup surgical:
      (1) ownership scope (portfolio_id) — only THIS test's portfolio
      (2) PK new since snapshot — only rows inserted after pre-purge
      (3) captured_at recent — defensive 10-min freshness window

    Concurrent tests / dev work touching OTHER portfolios are completely safe.
    Concurrent work on the SAME portfolio is protected by (2)+(3) intersection.

    Run-scoped tables (with run_id col): DELETE WHERE run_id stays — already
    maximally precise (run_id is a unique ownership label by definition).

    NEVER touches paper_v2 source rows.
    """
    # Tables with direct run_id column — run-scoped DELETE
    RUN_SCOPED_TABLES = (
        "paper_v2_session_event", "paper_v2_run_event", "paper_v2_order_event",
        "paper_v2_order_execution_state", "paper_v2_order",
        "paper_v2_position_snapshot", "paper_v2_intraday_snapshot",
        "paper_v2_daily_snapshot", "paper_v2_cash_ledger", "paper_v2_error",
        "paper_v2_session_day", "paper_v2_session",
        "paper_v2_fill",  # partitioned, DELETE via parent
    )
    # Tables with direct portfolio_id column — triple-scoped DELETE
    # (table, pk_col)
    PORTFOLIO_LABELED_TABLES = (
        ("dim_paper_v2_portfolio",              "portfolio_version_id"),
        ("paper_v2_config_change_audit",        "audit_pk"),
        ("paper_v2_runtime_config_activation",  "activation_pk"),
        ("paper_v2_execution_policy_activation", "activation_pk"),
        ("paper_v2_reset_audit",                "audit_pk"),
    )
    # runtime_profile dim tables — scope via profile_id JOIN to portfolio
    PROFILE_LABELED_TABLES = (
        ("dim_paper_v2_runtime_profile",        "profile_version_id"),
        ("dim_paper_v2_runtime_profile_version", "version_pk"),
    )

    def _run_scoped_purge(conn) -> None:
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
        """Snapshot PKs of rows already tied to our portfolio_id / profile_ids."""
        snap: dict[str, set] = {}
        if not e2e_test_portfolio_id:
            return snap
        with conn.cursor() as cur:
            for tbl, pk_col in PORTFOLIO_LABELED_TABLES:
                cur.execute(
                    f'SELECT "{pk_col}" FROM qe_archive."{tbl}" '
                    f'WHERE portfolio_id = %s',
                    (e2e_test_portfolio_id,),
                )
                snap[tbl] = {r[0] for r in cur.fetchall()}
            for tbl, pk_col in PROFILE_LABELED_TABLES:
                cur.execute(
                    f'''SELECT "{pk_col}" FROM qe_archive."{tbl}"
                       WHERE profile_id IN (
                           SELECT profile_id FROM paper_v2.runtime_profile
                           WHERE portfolio_id = %s
                       )''',
                    (e2e_test_portfolio_id,),
                )
                snap[tbl] = {r[0] for r in cur.fetchall()}
        return snap

    def _scoped_delete(conn, snapshot: dict[str, set]) -> None:
        """Triple-intersection DELETE:
          (1) ownership: portfolio_id == test's portfolio (or profile_id IN test's profiles)
          (2) PK NOT IN pre-test snapshot
          (3) captured_at >= window_start (defensive 10-min freshness)
        """
        from datetime import datetime, timezone, timedelta
        if not e2e_test_portfolio_id:
            return  # nothing to scope to — skip
        window_start = datetime.now(timezone.utc) - timedelta(minutes=10)

        with conn.cursor() as cur:
            # Portfolio-labeled tables
            for tbl, pk_col in PORTFOLIO_LABELED_TABLES:
                existing = tuple(snapshot.get(tbl, set())) or (None,)
                cur.execute(
                    f"""DELETE FROM qe_archive."{tbl}"
                       WHERE portfolio_id = %s
                         AND "{pk_col}" NOT IN %s
                         AND captured_at >= %s""",
                    (e2e_test_portfolio_id, existing, window_start),
                )
            # Profile-labeled tables (scope by profile_id JOIN to portfolio)
            for tbl, pk_col in PROFILE_LABELED_TABLES:
                existing = tuple(snapshot.get(tbl, set())) or (None,)
                cur.execute(
                    f"""DELETE FROM qe_archive."{tbl}"
                       WHERE profile_id IN (
                           SELECT profile_id FROM paper_v2.runtime_profile
                           WHERE portfolio_id = %s
                       )
                         AND "{pk_col}" NOT IN %s
                         AND captured_at >= %s""",
                    (e2e_test_portfolio_id, existing, window_start),
                )
        conn.commit()

    with dev_conn_provider() as conn:
        _run_scoped_purge(conn)
        pk_snapshot = _snapshot_pks(conn)

    yield

    with dev_conn_provider() as conn:
        _run_scoped_purge(conn)
        _scoped_delete(conn, pk_snapshot)
