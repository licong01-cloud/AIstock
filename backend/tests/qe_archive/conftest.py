"""Test fixtures for qe_archive handler integration tests.

Critical safety: dev DB (5433/aistock_dev) only. Connection providers refuse
prod (5432) by asserting port + dbname before yielding any connection.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor


ENV_FILE = Path("F:/Dev/AIstock/.env")


def _parse_env() -> dict[str, str]:
    cfg = {}
    if not ENV_FILE.exists():
        return cfg
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def _dev_db_creds() -> dict[str, Any] | None:
    cfg = _parse_env()
    keys = ("TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME",
            "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD")
    if not all(k in cfg for k in keys):
        return None
    creds = {
        "host": cfg["TDX_DB_DEV_HOST"],
        "port": int(cfg["TDX_DB_DEV_PORT"]),
        "dbname": cfg["TDX_DB_DEV_NAME"],
        "user": cfg["TDX_DB_DEV_USER"],
        "password": cfg["TDX_DB_DEV_PASSWORD"],
    }
    if creds["port"] != 5433:
        return None  # refuse anything that's not 5433
    if "dev" not in creds["dbname"]:
        return None
    return creds


@pytest.fixture(scope="session")
def dev_db_creds() -> dict[str, Any]:
    creds = _dev_db_creds()
    if creds is None:
        pytest.skip(
            "dev DB credentials not available (need TDX_DB_DEV_* env on port 5433 "
            "with 'dev' in dbname); skipping integration tests"
        )
    return creds


@pytest.fixture(scope="session")
def dev_db_available(dev_db_creds: dict[str, Any]) -> bool:
    """Verify dev DB is reachable AND T12 has been applied (Phase 2)."""
    try:
        conn = psycopg2.connect(**dev_db_creds, connect_timeout=3)
    except Exception as e:
        pytest.skip(f"dev DB unreachable: {e}")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_tables WHERE schemaname='qe_archive' AND tablename='paper_v2_run'"
            )
            if cur.fetchone() is None:
                pytest.skip(
                    "qe_archive.paper_v2_run not present on dev DB — "
                    "T12 must be applied first (see scripts/dev_db/_apply_t12_dev.py)"
                )
    finally:
        conn.close()
    return True


@pytest.fixture
def dev_conn_provider(dev_db_creds: dict[str, Any], dev_db_available: bool):
    """Return a ConnectionProvider that yields a dev DB connection.

    The handler's connection_provider must return a context manager. We yield
    a connection and close it afterwards.
    """
    @contextmanager
    def _provider() -> Iterator[Any]:
        conn = psycopg2.connect(**dev_db_creds)
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
    """Delete all qe_archive paper_v2_*/factor_value rows before AND after the
    test so each test runs against a clean archive but does NOT touch the
    paper_v2 source data.
    """
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
    # paper_v2_fill is partitioned; truncate via parent

    def _purge():
        with dev_conn_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(PURGE_SQL)
                cur.execute("TRUNCATE TABLE qe_archive.paper_v2_fill RESTART IDENTITY CASCADE")
            conn.commit()

    _purge()
    yield
    _purge()


@pytest.fixture
def sample_run_id(dev_conn_provider) -> str:
    """Pick the first run_id from paper_v2.run on dev (Batch A imported 121).
    Skip if Batch A hasn't been run."""
    with dev_conn_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_id FROM paper_v2.run ORDER BY trade_date DESC LIMIT 1"
            )
            row = cur.fetchone()
    if not row:
        pytest.skip("paper_v2.run is empty — run scripts/dev_db/batch_a_import_real_data.py first")
    return row[0]


@pytest.fixture
def run_id_with_runtime_profile(dev_conn_provider) -> str:
    """Pick a run whose portfolio has at least 1 runtime_profile row (T21 P1.4
    test coverage). Different tests need different source coverage; this fixture
    picks the most recent run that satisfies the constraint."""
    with dev_conn_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT r.run_id FROM paper_v2.run r
                   WHERE EXISTS (SELECT 1 FROM paper_v2.runtime_profile
                                 WHERE portfolio_id = r.portfolio_id)
                   ORDER BY r.trade_date DESC LIMIT 1"""
            )
            row = cur.fetchone()
    if not row:
        pytest.skip("no runs with runtime_profile data")
    return row[0]


@pytest.fixture
def run_id_with_runtime_profile_version(dev_conn_provider) -> str:
    with dev_conn_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT r.run_id FROM paper_v2.run r
                   WHERE EXISTS (
                       SELECT 1 FROM paper_v2.runtime_profile_version v
                       JOIN paper_v2.runtime_profile p ON p.profile_id = v.profile_id
                       WHERE p.portfolio_id = r.portfolio_id
                   )
                   ORDER BY r.trade_date DESC LIMIT 1"""
            )
            row = cur.fetchone()
    if not row:
        pytest.skip("no runs with runtime_profile_version data")
    return row[0]


@pytest.fixture
def run_id_with_runtime_config_activation(dev_conn_provider) -> str:
    with dev_conn_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT r.run_id FROM paper_v2.run r
                   WHERE EXISTS (
                       SELECT 1 FROM paper_v2.runtime_config_activation
                       WHERE portfolio_id = r.portfolio_id AND trade_date = r.trade_date
                   ) ORDER BY r.trade_date DESC LIMIT 1"""
            )
            row = cur.fetchone()
    if not row:
        pytest.skip("no runs with runtime_config_activation matching trade_date")
    return row[0]


@pytest.fixture
def run_id_with_execution_policy_activation(dev_conn_provider) -> str:
    with dev_conn_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT r.run_id FROM paper_v2.run r
                   WHERE EXISTS (
                       SELECT 1 FROM paper_v2.execution_policy_activation
                       WHERE portfolio_id = r.portfolio_id AND trade_date = r.trade_date
                   ) ORDER BY r.trade_date DESC LIMIT 1"""
            )
            row = cur.fetchone()
    if not row:
        pytest.skip("no runs with execution_policy_activation matching trade_date")
    return row[0]


@pytest.fixture
def run_id_with_reset_audit(dev_conn_provider) -> str:
    with dev_conn_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT r.run_id FROM paper_v2.run r
                   WHERE EXISTS (SELECT 1 FROM paper_v2.reset_audit
                                 WHERE portfolio_id = r.portfolio_id)
                   ORDER BY r.trade_date DESC LIMIT 1"""
            )
            row = cur.fetchone()
    if not row:
        pytest.skip("no runs with reset_audit data")
    return row[0]


@pytest.fixture
def archive_event_payload():
    """Helper to build a well-formed archive event payload."""
    def _build(
        event_type: str,
        run_id: str | None = None,
        trade_date: str | None = None,
        audit_id: str | None = None,
        extra: dict | None = None,
        schema_version: int = 1,
        routing_class: str = "archive",
    ) -> dict:
        payload = {
            "schema_version": schema_version,
            "routing_class": routing_class,
            "occurred_at": "2026-05-10T15:30:00Z",
        }
        if run_id is not None:
            payload["run_id"] = run_id
        if trade_date is not None:
            payload["trade_date"] = trade_date
        if audit_id is not None:
            payload["audit_id"] = audit_id
        if extra:
            payload.update(extra)
        return payload

    return _build
