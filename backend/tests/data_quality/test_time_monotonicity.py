"""Time-series sanity invariants (Stage 7.3 §7.3.5).

Original Stage 7.3 §7.3.5 framed this family as "monotonicity", but
Stage 7.3 r1 review (Agent C P2.1) showed:
  - Pre-sorting by trade_time before checking trade_time non-decreasing
    is tautological.
  - There is no usable "insertion-order" column on paper_v2.fills
    (fill_id is a hex string; Batch A's created_at is identical across
    all 8243 rows from a single bulk-import).

The family was re-scoped to "time-series sanity" with bounded-day and
positive-duration invariants that ARE testable on the real schema:

- ``paper_v2.fills.trade_time::date`` equals its owning run's
  ``paper_v2.run.trade_date`` (bounded-day correctness; catches
  cross-day leakage from clock-skew or replay-window bugs).
- ``qe_archive.paper_v2_run.captured_at`` <=
  ``qe_archive.paper_v2_run.archive_completed_at`` (positive archive
  duration; per Stage 7.3 r1 fix, ``archive_started_at`` was replaced
  with ``captured_at`` because the former does not exist on T12 /
  T14b/c r3 schema).
- ``qe_archive.paper_v2_run.archive_completed_at`` >=
  ``paper_v2.run.completed_at`` (archive cannot finish before source's
  own completion timestamp).
- ``qe_archive.paper_v2_session_day`` is unique per
  ``(trade_session_id, trade_date)``.
"""

from __future__ import annotations

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import RealDictCursor  # noqa: E402  after importorskip

from .conftest import skip_if_missing_columns


def test_module_collected_smoke():
    assert True


def test_fills_trade_time_bounded_by_run_trade_date(
    dev_conn, source_tables_ready,
):
    """Every fill's ``trade_time::date`` must equal its run's
    ``trade_date`` (no cross-day leakage).

    Per Agent C P2.1 review feedback: the original monotonicity check
    was tautological because it pre-sorted by ``trade_time``. Switching
    to "ORDER BY insertion column" is also unreliable on Batch A real
    data because:
      - ``fill_id`` is a hex-prefixed string (e.g. ``fill_05e0e2e0...``),
        NOT a sequence-generated bigint, so its lexicographic order has
        no relationship to insertion order.
      - ``created_at`` is uniform across all 8243 imported fills (single
        bulk-import timestamp), so it gives no ordering signal.

    The meaningful time-series-sanity invariant on this data is
    bounded-day correctness: a fill belonging to ``run_id=R`` whose
    ``trade_date`` is D must have its ``trade_time`` fall on day D.
    Cross-day leakage would indicate a clock-skew / replay-window bug.
    This invariant is non-tautological (a buggy writer can violate it)
    and verifiable on real data (0 violations on Batch A as of
    2026-05-11).
    """
    with dev_conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM paper_v2.fills f
            JOIN paper_v2.run r USING (run_id)
            WHERE r.trade_date IS NOT NULL
              AND f.trade_time IS NOT NULL
              AND f.trade_time::date <> r.trade_date
            """
        )
        violations = cur.fetchone()[0]
    if violations == 0:
        # Find the total row count to surface in the assertion message
        # even on the success path -- gives the reviewer a sense of the
        # test's actual coverage.
        with dev_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM paper_v2.fills f "
                "JOIN paper_v2.run r USING (run_id) "
                "WHERE r.trade_date IS NOT NULL AND f.trade_time IS NOT NULL"
            )
            checked = cur.fetchone()[0]
        if checked == 0:
            pytest.skip("no fills with both trade_date and trade_time joined.")
        return
    # Surface up to 5 violators with context
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT f.fill_id, f.run_id, r.trade_date AS run_date,
                   f.trade_time AS fill_time
            FROM paper_v2.fills f
            JOIN paper_v2.run r USING (run_id)
            WHERE r.trade_date IS NOT NULL
              AND f.trade_time IS NOT NULL
              AND f.trade_time::date <> r.trade_date
            LIMIT 5
            """
        )
        sample = list(cur.fetchall())
    assert False, (
        f"{violations} fill(s) have trade_time on a different date than the "
        f"owning run's trade_date; first 5 (fill_id, run_id, run_date, fill_time): "
        f"{[(r['fill_id'], r['run_id'], r['run_date'], r['fill_time']) for r in sample]}"
    )


def test_archive_completed_at_after_source_run_completed_at(
    dev_conn, source_tables_ready, archive_tables_ready,
):
    """archive_completed_at must be >= source.run.completed_at (the
    archive cannot complete before its source row was finished). The
    canonical source timestamp on paper_v2.run is ``completed_at`` (no
    ``created_at`` column exists on this branch's schema)."""
    with dev_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='qe_archive' AND table_name='paper_v2_run'
              AND column_name='archive_completed_at'
            """
        )
        if cur.fetchone() is None:
            pytest.skip("qe_archive.paper_v2_run.archive_completed_at column not present.")
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='paper_v2' AND table_name='run'
              AND column_name IN ('completed_at', 'created_at', 'started_at')
            """
        )
        source_cols = {r[0] for r in cur.fetchall()}
    source_ts_col = next(
        (c for c in ("completed_at", "created_at", "started_at") if c in source_cols),
        None,
    )
    if source_ts_col is None:
        pytest.skip("paper_v2.run has no completed_at/created_at/started_at column.")
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT a.run_id,
                   s.{source_ts_col} AS source_ts,
                   a.archive_completed_at AS arch_done
            FROM qe_archive.paper_v2_run a
            JOIN paper_v2.run s USING (run_id)
            WHERE a.archive_completed_at IS NOT NULL AND s.{source_ts_col} IS NOT NULL
            LIMIT 200
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("no archive runs with both timestamps populated.")
    violations = [r for r in rows if r["arch_done"] < r["source_ts"]]
    assert not violations, (
        f"{len(violations)} archive runs completed before their source row's "
        f"{source_ts_col}; first 5: "
        f"{[(r['run_id'], r['source_ts'], r['arch_done']) for r in violations[:5]]}"
    )


def test_archive_captured_before_completed(
    dev_conn, archive_tables_ready,
):
    """Archive write-side invariant: ``captured_at <= archive_completed_at``.

    Per Agent C P2.2 review feedback: the original test targeted an
    ``archive_started_at`` column that does not exist on the T12 schema
    nor on dw-foundation T14b/c r3 (verified via column probe on dev DB
    2026-05-11). Replaced with the equivalent invariant using the
    columns that DO exist:
      - ``captured_at`` (the qe_archive write timestamp)
      - ``archive_completed_at`` (when the archive handler reported done)

    captured_at must be <= archive_completed_at for any row that has
    both populated.
    """
    skip_if_missing_columns(
        dev_conn, "qe_archive", "paper_v2_run",
        ("captured_at", "archive_completed_at"),
        "captured_at / archive_completed_at not both present on dev DB; "
        "T12 may not be applied or schema diverged from origin/claude/"
        "dw-foundation-20260510 expectations.",
    )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT run_id, captured_at, archive_completed_at
            FROM qe_archive.paper_v2_run
            WHERE captured_at IS NOT NULL AND archive_completed_at IS NOT NULL
            LIMIT 500
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("no archive runs with both captured_at + archive_completed_at populated.")
    violations = [
        r for r in rows
        if r["archive_completed_at"] < r["captured_at"]
    ]
    assert not violations, (
        f"{len(violations)} runs have archive_completed_at < captured_at "
        f"(handler finished before it started writing); first 5: "
        f"{[(r['run_id'], r['captured_at'], r['archive_completed_at']) for r in violations[:5]]}"
    )


def test_session_day_unique_per_session_trade_date(
    dev_conn, archive_tables_ready,
):
    """qe_archive.paper_v2_session_day must have at most 1 row per
    (trade_session_id, trade_date) pair."""
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname='qe_archive' "
            "AND tablename='paper_v2_session_day'"
        )
        if cur.fetchone() is None:
            pytest.skip("qe_archive.paper_v2_session_day not present.")
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT trade_session_id, trade_date, count(*) AS n
            FROM qe_archive.paper_v2_session_day
            GROUP BY trade_session_id, trade_date
            HAVING count(*) > 1
            LIMIT 50
            """
        )
        rows = list(cur.fetchall())
    assert not rows, (
        f"{len(rows)} (session_id, trade_date) collisions; first 5: {rows[:5]}"
    )
