"""Dev-DB fixtures for paper-v2 integration tests (Phase 3, T-PAPER-V2-INT).

These fixtures connect to the dev PostgreSQL instance ONLY:

    host=127.0.0.1  port=5433  dbname=aistock_dev  user=postgres

Production aistock@5432 is NEVER acceptable here. ``_dev_dsn`` enforces this
with hard assertions (DevDbTargetMisconfigured) so a misconfigured ``.env``
or stray override cannot accidentally point an integration test at prod.

Tests that INSERT data MUST tag rows with a ``test_int*`` prefix and DELETE
on teardown. Tests that READ existing data (the dominant pattern for INT-2..4)
need no cleanup.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# .env loading + DSN guard
# ---------------------------------------------------------------------------


_AISTOCK_ENV_PATH = "F:/Dev/AIstock/.env"


class DevDbTargetMisconfigured(RuntimeError):
    """Raised when the resolved DSN does not point at the dev DB.

    This is the primary safety guardrail against tests writing to prod
    aistock@5432. Every code path that opens a connection in this module
    must go through ``_dev_dsn`` first.
    """


def _load_env_once() -> None:
    """Load AIstock .env into os.environ if dev keys are missing.

    Idempotent — ``load_dotenv`` is a no-op if values already exist with
    higher precedence. We only load when the dev keys are absent so that an
    exported override (e.g. CI secrets) wins.
    """
    if os.environ.get("TDX_DB_DEV_HOST") and os.environ.get("TDX_DB_DEV_PORT"):
        return
    if os.path.exists(_AISTOCK_ENV_PATH):
        load_dotenv(_AISTOCK_ENV_PATH, override=False)


def _dev_dsn() -> dict[str, Any]:
    """Resolve and validate the dev DB connection params from environment.

    Returns a dict suitable for ``psycopg2.connect(**dsn)``. Raises
    ``DevDbTargetMisconfigured`` if the resolved target does NOT match the
    dev DB invariants:

        * port == 5433
        * 'dev' in dbname.lower()

    The password value is NEVER printed; everything else is included in the
    error context so a debugging human can see the misconfiguration at a
    glance.
    """
    _load_env_once()
    host = os.environ.get("TDX_DB_DEV_HOST")
    port_str = os.environ.get("TDX_DB_DEV_PORT")
    dbname = os.environ.get("TDX_DB_DEV_NAME")
    user = os.environ.get("TDX_DB_DEV_USER")
    password = os.environ.get("TDX_DB_DEV_PASSWORD")

    missing = [
        name
        for name, value in (
            ("TDX_DB_DEV_HOST", host),
            ("TDX_DB_DEV_PORT", port_str),
            ("TDX_DB_DEV_NAME", dbname),
            ("TDX_DB_DEV_USER", user),
            ("TDX_DB_DEV_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise DevDbTargetMisconfigured(
            f"missing dev DB env vars: {missing}; expected in {_AISTOCK_ENV_PATH}"
        )

    try:
        port = int(port_str)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise DevDbTargetMisconfigured(
            f"TDX_DB_DEV_PORT is not an integer: {port_str!r}"
        ) from exc

    if port != 5433:
        raise DevDbTargetMisconfigured(
            f"refusing to connect: TDX_DB_DEV_PORT={port} (expected 5433); "
            f"host={host} dbname={dbname} user={user}"
        )
    if not dbname or "dev" not in dbname.lower():
        raise DevDbTargetMisconfigured(
            f"refusing to connect: dbname={dbname!r} does not contain 'dev'; "
            f"host={host} port={port} user={user}"
        )
    # REV-1 P2.1: runtime hard assertion on host. Must be loopback to prevent
    # accidental remote/prod hits — even a stray TDX_DB_DEV_HOST=<prod-ip>
    # combined with port=5433 + dev-named DB would still be wrong.
    if host != "127.0.0.1":
        raise DevDbTargetMisconfigured(
            f"refusing to connect: dev DB host must be 127.0.0.1, got {host!r}; "
            f"prevents accidental remote/prod hits "
            f"(port={port} dbname={dbname} user={user})"
        )

    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def dev_db_conn() -> Iterator[psycopg2.extensions.connection]:
    """Yield a psycopg2 connection to the dev DB; rollback + close on teardown.

    Function-scoped so a per-test rollback cannot leak across tests. The
    teardown rollback is intentionally unconditional — even read-only tests
    benefit from it as a defensive cleanup.
    """
    dsn = _dev_dsn()
    conn = psycopg2.connect(**dsn)
    try:
        yield conn
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()


@pytest.fixture(scope="function")
def dev_db_cursor(
    dev_db_conn: psycopg2.extensions.connection,
) -> Iterator[psycopg2.extensions.cursor]:
    """Yield a RealDictCursor for the dev DB conn fixture above."""
    cur = dev_db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cur
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Run-data loaders
# ---------------------------------------------------------------------------


def load_dev_paper_v2_run(
    conn: psycopg2.extensions.connection, run_id: str
) -> dict[str, Any]:
    """Return the run row plus all child fills/positions/daily_snapshots.

    Returns a dict shaped ``{"run": {...}, "fills": [...], "positions": [...],
    "snapshots": [...]}``. Each list is a list of RealDictCursor row dicts
    (or empty list if no rows for that table).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM paper_v2.run WHERE run_id = %s", (run_id,))
        run_row = cur.fetchone()
        cur.execute(
            "SELECT * FROM paper_v2.fills WHERE run_id = %s ORDER BY trade_time, fill_id",
            (run_id,),
        )
        fills = list(cur.fetchall())
        cur.execute(
            "SELECT * FROM paper_v2.positions WHERE run_id = %s ORDER BY trade_date, symbol",
            (run_id,),
        )
        positions = list(cur.fetchall())
        cur.execute(
            "SELECT * FROM paper_v2.daily_snapshots WHERE run_id = %s ORDER BY trade_date",
            (run_id,),
        )
        snapshots = list(cur.fetchall())
    return {
        "run": dict(run_row) if run_row else None,
        "fills": fills,
        "positions": positions,
        "snapshots": snapshots,
    }


def find_run_with_capture_fills(
    conn: psycopg2.extensions.connection, n: int = 1
) -> list[str]:
    """Return up to N run_ids that have at least one fill with a populated
    ``intended_price`` (Batch C synthetic / wired path)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT run_id
            FROM paper_v2.fills
            WHERE intended_price IS NOT NULL
            ORDER BY run_id DESC
            LIMIT %s
            """,
            (n,),
        )
        return [row[0] for row in cur.fetchall()]


def find_run_with_market_order_fills(
    conn: psycopg2.extensions.connection, n: int = 1
) -> list[str]:
    """Return up to N run_ids whose fills include at least one with
    ``intended_price IS NULL`` (Batch A real prod-like MARKET path)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT run_id
            FROM paper_v2.fills
            WHERE intended_price IS NULL
            ORDER BY run_id DESC
            LIMIT %s
            """,
            (n,),
        )
        return [row[0] for row in cur.fetchall()]


def find_recent_run_with_full_data(
    conn: psycopg2.extensions.connection,
) -> str | None:
    """Return the most recent run_id that has fills + positions + at least one
    daily_snapshot. Used by INT-2 for the broadest invariant coverage."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.run_id
            FROM paper_v2.run r
            WHERE EXISTS (SELECT 1 FROM paper_v2.fills f WHERE f.run_id = r.run_id)
              AND EXISTS (SELECT 1 FROM paper_v2.positions p WHERE p.run_id = r.run_id)
              AND EXISTS (SELECT 1 FROM paper_v2.daily_snapshots d WHERE d.run_id = r.run_id)
            ORDER BY r.run_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    return row[0] if row else None
