"""Cross-table cardinality assertions.

Per Stage 7.3 §7.3.4:
- For every run_id in qe_archive.paper_v2_run, the count of source
  ``paper_v2.fills`` matches ``qe_archive.paper_v2_fill`` count.
- ``paper_v2_run.total_fill_count`` (if present) matches the actual
  count.
- ``factor_value`` parquet row count vs archive row count for the same
  (factor_name, code_text_hash, trade_date) tuple.
"""

from __future__ import annotations

import pytest

from psycopg2.extras import RealDictCursor


def test_module_collected_smoke():
    assert True


def test_paper_v2_fill_count_matches_archive_per_run(
    dev_conn, source_tables_ready, archive_tables_ready,
):
    """For every archived run, source fill count == archive fill count."""
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH archived AS (
              SELECT run_id FROM qe_archive.paper_v2_run
            ),
            src AS (
              SELECT f.run_id, count(*) AS src_n
              FROM paper_v2.fills f
              JOIN archived USING (run_id)
              GROUP BY f.run_id
            ),
            arc AS (
              SELECT f.run_id, count(*) AS arc_n
              FROM qe_archive.paper_v2_fill f
              JOIN archived USING (run_id)
              GROUP BY f.run_id
            )
            SELECT a.run_id, COALESCE(s.src_n, 0) AS src_n, COALESCE(a.arc_n, 0) AS arc_n
            FROM arc a
            LEFT JOIN src s USING (run_id)
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("no joined runs between paper_v2.run and qe_archive.paper_v2_run yet.")
    drift = [r for r in rows if r["src_n"] != r["arc_n"]]
    assert not drift, (
        f"{len(drift)} run(s) have fill-count drift; first 5: "
        f"{[(r['run_id'], r['src_n'], r['arc_n']) for r in drift[:5]]}"
    )


def test_archive_run_aggregate_counts_self_consistent(
    dev_conn, archive_tables_ready,
):
    """archive_run.total_fill_count (if column exists) equals the
    paper_v2_fill count for the same run_id."""
    with dev_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='qe_archive' AND table_name='paper_v2_run'
              AND column_name = 'total_fill_count'
            """
        )
        has_col = cur.fetchone() is not None
    if not has_col:
        pytest.skip(
            "qe_archive.paper_v2_run.total_fill_count column not present in T12; "
            "aggregate-self-consistency check deferred."
        )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT r.run_id, r.total_fill_count AS recorded, count(f.fill_id) AS observed
            FROM qe_archive.paper_v2_run r
            LEFT JOIN qe_archive.paper_v2_fill f USING (run_id)
            GROUP BY r.run_id, r.total_fill_count
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("no archive runs to aggregate.")
    drift = [r for r in rows if r["recorded"] is not None and r["recorded"] != r["observed"]]
    assert not drift, (
        f"{len(drift)} run(s) have total_fill_count drift; first 5: "
        f"{[(r['run_id'], r['recorded'], r['observed']) for r in drift[:5]]}"
    )


def test_paper_v2_session_event_count_matches_per_run(
    dev_conn, source_tables_ready, archive_tables_ready,
):
    """session_events on source equals paper_v2_session_event on archive per run."""
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname='qe_archive' "
            "AND tablename='paper_v2_session_event'"
        )
        if cur.fetchone() is None:
            pytest.skip("qe_archive.paper_v2_session_event not present.")
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH archived AS (
              SELECT run_id FROM qe_archive.paper_v2_run
            )
            SELECT a.run_id,
                   (SELECT count(*) FROM paper_v2.session_events WHERE run_id = a.run_id) AS src_n,
                   (SELECT count(*) FROM qe_archive.paper_v2_session_event WHERE run_id = a.run_id) AS arc_n
            FROM archived a
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("no archived runs to compare session_events for.")
    drift = [r for r in rows if r["src_n"] != r["arc_n"]]
    assert not drift, (
        f"{len(drift)} run(s) have session_event count drift; first 5: "
        f"{[(r['run_id'], r['src_n'], r['arc_n']) for r in drift[:5]]}"
    )


def test_factor_value_archive_unique_per_idempotency_key(
    dev_conn, archive_tables_ready,
):
    """Per D5 Q4.c: factor_value rows are idempotent on
    (factor_name, code_text_hash, trade_date, code). The archive must not
    contain duplicate rows on that key."""
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname='qe_archive' "
            "AND tablename='factor_value'"
        )
        if cur.fetchone() is None:
            pytest.skip("qe_archive.factor_value not present.")
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT factor_name, code_text_hash, trade_date, code, count(*) AS n
            FROM qe_archive.factor_value
            GROUP BY factor_name, code_text_hash, trade_date, code
            HAVING count(*) > 1
            LIMIT 50
            """
        )
        rows = list(cur.fetchall())
    assert not rows, (
        f"{len(rows)} factor_value idempotency-key collisions; first 5: {rows[:5]}"
    )
