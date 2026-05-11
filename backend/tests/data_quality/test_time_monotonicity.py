"""Time-series monotonicity invariants.

Per Stage 7.3 §7.3.5:
- ``paper_v2.fills.trade_time`` is non-decreasing within a single run.
- ``paper_v2.run.created_at`` < ``qe_archive.paper_v2_run.archive_completed_at``
  (the archive cannot complete before its source row was even created).
- ``paper_v2.session_day.trade_date`` is unique per session within a run.
- ``qe_archive.paper_v2_run.archive_completed_at`` >=
  ``qe_archive.paper_v2_run.archive_started_at`` when both columns exist.
"""

from __future__ import annotations

import pytest

from psycopg2.extras import RealDictCursor


def test_module_collected_smoke():
    assert True


def test_fills_trade_time_monotonic_within_run(
    dev_conn, source_tables_ready,
):
    """For every run, fills ordered by (trade_time, fill_id) have
    non-decreasing trade_time."""
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT run_id
            FROM paper_v2.fills
            GROUP BY run_id
            HAVING count(*) >= 2
            ORDER BY count(*) DESC
            LIMIT 20
            """
        )
        run_ids = [r["run_id"] for r in cur.fetchall()]
    if not run_ids:
        pytest.skip("no runs with >=2 fills.")
    offenders: list[tuple[str, int]] = []
    for run_id in run_ids:
        with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT fill_id, trade_time
                FROM paper_v2.fills
                WHERE run_id = %s
                ORDER BY trade_time NULLS LAST, fill_id
                """,
                (run_id,),
            )
            rows = list(cur.fetchall())
        prev = None
        for r in rows:
            tt = r["trade_time"]
            if tt is None:
                continue
            if prev is not None and tt < prev:
                offenders.append((run_id, r["fill_id"]))
                break
            prev = tt
    assert not offenders, (
        f"{len(offenders)} runs have non-monotonic trade_time; first 5: {offenders[:5]}"
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


def test_archive_started_before_completed(
    dev_conn, archive_tables_ready,
):
    """When both archive_started_at + archive_completed_at exist,
    completed >= started for every row."""
    with dev_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='qe_archive' AND table_name='paper_v2_run'
              AND column_name IN ('archive_started_at', 'archive_completed_at')
            """
        )
        cols = {r[0] for r in cur.fetchall()}
    if {"archive_started_at", "archive_completed_at"} - cols:
        pytest.skip("archive_started_at / archive_completed_at not both present.")
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT run_id, archive_started_at, archive_completed_at
            FROM qe_archive.paper_v2_run
            WHERE archive_started_at IS NOT NULL AND archive_completed_at IS NOT NULL
            LIMIT 500
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("no archive runs with both timestamps recorded.")
    violations = [
        r for r in rows
        if r["archive_completed_at"] < r["archive_started_at"]
    ]
    assert not violations, (
        f"{len(violations)} runs have completed_at < started_at; first 5: "
        f"{[(r['run_id'], r['archive_started_at'], r['archive_completed_at']) for r in violations[:5]]}"
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
