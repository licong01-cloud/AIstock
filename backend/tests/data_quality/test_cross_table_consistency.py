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

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import RealDictCursor  # noqa: E402  after importorskip

from .conftest import skip_if_missing_columns


def test_module_collected_smoke():
    assert True


def test_paper_v2_fill_count_matches_archive_per_run(
    dev_conn, source_tables_ready, archive_tables_ready,
):
    """For every archived run, source fill count == archive fill count.

    Per Agent C P1.3 review: must FAIL (not skip) when ``paper_v2.run``
    has rows but ``qe_archive.paper_v2_run`` does not — that's the
    canonical 'handler not registered / archive worker not enabled'
    state and surfaces the real regression. Only skip when BOTH source
    and archive are empty (no data to compare).

    Sentinel: query walks every archived run as the driver and a
    LEFT JOIN on paper_v2.fills so a run with 0 archive fills against a
    source with N fills shows up as drift, not vanishes.
    """
    with dev_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM paper_v2.run")
        source_run_n = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM qe_archive.paper_v2_run")
        archive_run_n = cur.fetchone()[0]
    if source_run_n == 0:
        pytest.skip("paper_v2.run is empty; nothing to compare.")
    if archive_run_n == 0:
        pytest.fail(
            f"paper_v2.run has {source_run_n} row(s) but qe_archive.paper_v2_run "
            f"has 0 -- handler is not registered on the archive worker yet, OR the "
            f"worker is not consuming outbox events. This is a regression, not a "
            f"clean-slate condition; the archive backfill is expected to run."
        )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Drive from qe_archive.paper_v2_run (one row per archived run), then
        # LEFT JOIN both source and archive fill counts. This surfaces both
        # archive_with_zero_fills AND source_with_zero_fills as drift rather
        # than letting one side disappear via inner-join semantics.
        cur.execute(
            """
            SELECT r.run_id,
                   COALESCE(s.src_n, 0) AS src_n,
                   COALESCE(a.arc_n, 0) AS arc_n
            FROM qe_archive.paper_v2_run r
            LEFT JOIN (
              SELECT run_id, count(*) AS src_n
              FROM paper_v2.fills
              GROUP BY run_id
            ) s USING (run_id)
            LEFT JOIN (
              SELECT run_id, count(*) AS arc_n
              FROM qe_archive.paper_v2_fill
              GROUP BY run_id
            ) a USING (run_id)
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("qe_archive.paper_v2_run is empty after row-count check (race).")
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
    skip_if_missing_columns(
        dev_conn, "qe_archive", "paper_v2_run", ("total_fill_count",),
        "total_fill_count is not on the current T12 schema; tracked for "
        "addition under the dw-foundation T14b/c r3 enhancement track. When "
        "the column lands, this test activates automatically.",
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
    contain duplicate rows on that key.

    Whole-table aggregate: the GROUP BY/HAVING runs server-side over
    every row. The trailing LIMIT 50 only caps the violator *sample*
    surfaced in the assertion message; the count assertion below is
    against the whole-table total.
    """
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
            SELECT count(*) AS total_violators FROM (
              SELECT factor_name, code_text_hash, trade_date, code
              FROM qe_archive.factor_value
              GROUP BY factor_name, code_text_hash, trade_date, code
              HAVING count(*) > 1
            ) v
            """
        )
        total_violators = cur.fetchone()["total_violators"]
        cur.execute(
            """
            SELECT factor_name, code_text_hash, trade_date, code, count(*) AS n
            FROM qe_archive.factor_value
            GROUP BY factor_name, code_text_hash, trade_date, code
            HAVING count(*) > 1
            LIMIT 50
            """
        )
        sample = list(cur.fetchall())
    assert total_violators == 0, (
        f"{total_violators} factor_value idempotency-key collisions; "
        f"first {len(sample)} sample: {sample[:5]}"
    )
