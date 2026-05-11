"""T29 / P1.1 r4 — empirical proof that SELECT FOR UPDATE actually locks.

Codex r3 BLOCKED on the observation that `pg_pool.get_conn()` sets
autocommit=True at pg_pool.py:248, so SELECT FOR UPDATE under that path
releases the lock immediately and provides no real protection. Round 4
switched the E2E test's mutation block to a DIRECT psycopg2 connection
(autocommit=False) — this module proves empirically that the chosen
transactional path actually serializes concurrent writers.

Two scenarios:

  test_for_update_lock_blocks_concurrent_update:
    - Conn A: BEGIN; SELECT ... FOR UPDATE on a target row
    - Conn B (different connection): UPDATE the same row with short
      lock_timeout — MUST fail with LockNotAvailable
    - Conn A: COMMIT
    - Conn B retry: succeeds

  test_pg_pool_autocommit_does_NOT_lock:
    - Conn A via pg_pool.get_conn() (autocommit=True): SELECT FOR UPDATE
    - Conn B (different connection): UPDATE the same row — succeeds
      immediately because autocommit released the lock at SELECT return
    This documents WHY we cannot use pg_pool for transactional work.

Both scenarios use a synthetic test row in a row that doesn't matter for
test correctness — we INSERT a marker row then DELETE in cleanup.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import psycopg2
import pytest
from psycopg2 import errors as pg_errors


# Use a synthetic strategy_pkg.package row whose content doesn't matter; we
# only exercise row-level locking semantics. The row is INSERTed at fixture
# entry and DELETEd at fixture exit so no real data is mutated.

_SYNTHETIC_PKG_ID = "pkg_e2e_concurrency_test"


@pytest.fixture
def synthetic_locked_package(dev_db_creds):
    """INSERT a throwaway strategy_pkg.package row for lock-semantics testing.

    Copies the manifest_json from an existing package so all NOT NULL fields
    are satisfied with valid content; only DB-side columns we control here.
    """
    conn = psycopg2.connect(**dev_db_creds)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Borrow a valid manifest from an existing package; we only
            # care that the INSERT succeeds and the row participates in
            # row-level locking like any real row.
            cur.execute(
                """SELECT manifest_json, manifest_sha256
                   FROM strategy_pkg.package
                   ORDER BY package_id LIMIT 1"""
            )
            row = cur.fetchone()
            if row is None:
                pytest.skip("no package rows to clone manifest from")
            manifest_json, manifest_sha256 = row

            # Clean any leftover from prior test failures
            cur.execute(
                "DELETE FROM strategy_pkg.package WHERE package_id = %s",
                (_SYNTHETIC_PKG_ID,),
            )
            cur.execute(
                """INSERT INTO strategy_pkg.package (
                       package_id, package_name, package_version,
                       source_type, source_id,
                       package_status, manifest_json, manifest_sha256,
                       paper_portfolio_count, created_at, updated_at
                   ) VALUES (
                       %s, 'e2e_test_pkg', '0.0.1',
                       'qe_experiment', 'e2e_test_src',
                       'DRAFT', %s, %s,
                       0, NOW(), NOW()
                   )""",
                (_SYNTHETIC_PKG_ID, json.dumps(manifest_json), manifest_sha256),
            )
        conn.commit()
        yield _SYNTHETIC_PKG_ID
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM strategy_pkg.package WHERE package_id = %s",
                    (_SYNTHETIC_PKG_ID,),
                )
            conn.commit()
        finally:
            conn.close()


class TestForUpdateLockBlocks:
    """Empirical proof: SELECT FOR UPDATE under autocommit=False actually
    blocks concurrent writers. Failing this test would mean the entire P1.1
    round-4 strategy is illusory."""

    def test_for_update_lock_blocks_concurrent_update(
        self, dev_db_creds, synthetic_locked_package,
    ):
        # Connection A: take the row lock
        conn_a = psycopg2.connect(**dev_db_creds)
        conn_a.autocommit = False
        # Connection B: separate session, short lock_timeout so the test
        # doesn't hang if locks don't actually serialize
        conn_b = psycopg2.connect(**dev_db_creds)
        conn_b.autocommit = False
        try:
            with conn_b.cursor() as cur_b:
                cur_b.execute("SET LOCAL lock_timeout = '500ms'")

            # A: BEGIN + SELECT FOR UPDATE
            with conn_a.cursor() as cur_a:
                cur_a.execute(
                    """SELECT package_status FROM strategy_pkg.package
                       WHERE package_id = %s FOR UPDATE""",
                    (synthetic_locked_package,),
                )
                a_locked_status = cur_a.fetchone()[0]
            # ... A holds the lock until commit/rollback

            # B: try to UPDATE the same row — MUST fail with lock_timeout
            with conn_b.cursor() as cur_b:
                with pytest.raises(pg_errors.LockNotAvailable):
                    cur_b.execute(
                        """UPDATE strategy_pkg.package
                           SET package_status = 'PAPER_RUNNING'
                           WHERE package_id = %s""",
                        (synthetic_locked_package,),
                    )
            # B's transaction is now in error state; rollback to clear
            conn_b.rollback()

            # A: commit (release lock)
            conn_a.commit()

            # B: retry UPDATE — should now succeed (lock released)
            with conn_b.cursor() as cur_b:
                cur_b.execute(
                    """UPDATE strategy_pkg.package
                       SET package_status = 'PAPER_RUNNING'
                       WHERE package_id = %s""",
                    (synthetic_locked_package,),
                )
                assert cur_b.rowcount == 1
            conn_b.commit()

            # Verify final state
            with conn_a.cursor() as cur_a:
                cur_a.execute(
                    "SELECT package_status FROM strategy_pkg.package WHERE package_id = %s",
                    (synthetic_locked_package,),
                )
                final_status = cur_a.fetchone()[0]
            assert final_status == "PAPER_RUNNING"
            # And the originally captured status was DRAFT (from fixture)
            assert a_locked_status == "DRAFT"
        finally:
            try:
                conn_a.rollback()
            except Exception:
                pass
            try:
                conn_b.rollback()
            except Exception:
                pass
            conn_a.close()
            conn_b.close()

    def test_autocommit_select_for_update_does_NOT_lock(
        self, dev_db_creds, synthetic_locked_package,
    ):
        """Documents WHY r3 was broken: SELECT FOR UPDATE under autocommit=True
        immediately releases the lock at statement end. A concurrent UPDATE
        from a different connection succeeds without blocking.

        This is the exact failure mode pg_pool.get_conn() exhibits (since
        pg_pool.py:248 sets autocommit=True), and it's why round 4 had to
        switch to direct dev_conn_provider for the mutation work.
        """
        conn_a = psycopg2.connect(**dev_db_creds)
        conn_a.autocommit = True   # mirror pg_pool.py:248 behavior
        conn_b = psycopg2.connect(**dev_db_creds)
        conn_b.autocommit = False
        try:
            with conn_b.cursor() as cur_b:
                cur_b.execute("SET LOCAL lock_timeout = '500ms'")

            # A: SELECT FOR UPDATE under autocommit — lock acquired then
            # IMMEDIATELY released when the statement completes (autocommit
            # commits at end of statement).
            with conn_a.cursor() as cur_a:
                cur_a.execute(
                    """SELECT package_status FROM strategy_pkg.package
                       WHERE package_id = %s FOR UPDATE""",
                    (synthetic_locked_package,),
                )
                _ = cur_a.fetchone()

            # B: UPDATE — should SUCCEED (no real lock held by A)
            with conn_b.cursor() as cur_b:
                cur_b.execute(
                    """UPDATE strategy_pkg.package
                       SET package_status = 'PAPER_RUNNING'
                       WHERE package_id = %s""",
                    (synthetic_locked_package,),
                )
                assert cur_b.rowcount == 1
            conn_b.commit()
        finally:
            conn_a.close()
            try:
                conn_b.rollback()
            except Exception:
                pass
            conn_b.close()
