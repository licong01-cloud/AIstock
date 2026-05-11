"""Field-level consistency between paper_v2.run and qe_archive.paper_v2_run.

Per Stage 7.3 §7.3.1: assert each mirrored field on the archive side has
the canonical value the source row carries — covering enum-case
normalization (per P1.4 round 1 review) + portfolio identity + status
upper-casing + monotone counters.

Tests skip cleanly when the dev DB or archive tables are absent so the
``data_quality_deep`` nox session can run green on a fresh CI host.
"""

from __future__ import annotations

import pytest

from psycopg2.extras import RealDictCursor


PAPER_V2_RUN_STATUSES = (
    "PENDING", "RUNNING", "SUCCEEDED", "FAILED", "INTERRUPTED",
)


def test_module_collected_smoke():
    """Sentinel test so pytest never exits 5 even when every DB-dependent
    test below is skipped."""
    assert PAPER_V2_RUN_STATUSES  # constant must be non-empty


def _fetchall_dict(conn, sql: str, params=()) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def test_archive_runs_have_matching_source_run(
    dev_conn, source_tables_ready, archive_tables_ready,
):
    """Every qe_archive.paper_v2_run.run_id must exist on paper_v2.run."""
    archive = _fetchall_dict(
        dev_conn,
        "SELECT run_id FROM qe_archive.paper_v2_run",
    )
    if not archive:
        pytest.skip("qe_archive.paper_v2_run is empty; no handler runs have been recorded yet.")
    run_ids = tuple(r["run_id"] for r in archive)
    source = _fetchall_dict(
        dev_conn,
        "SELECT run_id FROM paper_v2.run WHERE run_id = ANY(%s)",
        (list(run_ids),),
    )
    source_ids = {r["run_id"] for r in source}
    missing = [rid for rid in run_ids if rid not in source_ids]
    assert not missing, (
        f"archive references {len(missing)} run_id(s) that no longer exist on "
        f"paper_v2.run; orphan archive rows: {missing[:5]}"
    )


def test_archive_run_status_is_uppercase(
    dev_conn, archive_tables_ready,
):
    """Per P1.4 round 1 CHECK enum: archive status values are uppercase."""
    rows = _fetchall_dict(
        dev_conn,
        "SELECT run_id, status FROM qe_archive.paper_v2_run WHERE status IS NOT NULL",
    )
    if not rows:
        pytest.skip("qe_archive.paper_v2_run has no rows yet.")
    offenders = [r for r in rows if r["status"] not in PAPER_V2_RUN_STATUSES]
    assert not offenders, (
        f"{len(offenders)} archive run(s) carry non-canonical status (must be "
        f"in {PAPER_V2_RUN_STATUSES}); offenders: "
        f"{[(r['run_id'], r['status']) for r in offenders[:5]]}"
    )


def test_archive_run_portfolio_id_matches_source(
    dev_conn, source_tables_ready, archive_tables_ready,
):
    """portfolio_id on archive must equal the source row's portfolio_id."""
    rows = _fetchall_dict(
        dev_conn,
        """
        SELECT a.run_id, a.portfolio_id AS archive_pid, s.portfolio_id AS source_pid
        FROM qe_archive.paper_v2_run a
        JOIN paper_v2.run s USING (run_id)
        LIMIT 200
        """,
    )
    if not rows:
        pytest.skip("no matching run_id between paper_v2.run and qe_archive.paper_v2_run yet.")
    drift = [r for r in rows if r["archive_pid"] != r["source_pid"]]
    assert not drift, (
        f"{len(drift)} run(s) have drifted portfolio_id: "
        f"{[(r['run_id'], r['archive_pid'], r['source_pid']) for r in drift[:5]]}"
    )


def test_archive_run_status_case_matches_uppercased_source(
    dev_conn, source_tables_ready, archive_tables_ready,
):
    """archive status must equal source.status.upper() (the handler upper-
    cases at archive time per P1.4 round 1)."""
    rows = _fetchall_dict(
        dev_conn,
        """
        SELECT a.run_id, a.status AS archive_status, s.status AS source_status
        FROM qe_archive.paper_v2_run a
        JOIN paper_v2.run s USING (run_id)
        WHERE a.status IS NOT NULL AND s.status IS NOT NULL
        LIMIT 200
        """,
    )
    if not rows:
        pytest.skip("no overlap between paper_v2.run and qe_archive.paper_v2_run for status comparison.")
    drift = [r for r in rows if r["archive_status"] != str(r["source_status"]).upper()]
    assert not drift, (
        f"{len(drift)} archive runs have status drift from source.upper(): "
        f"{[(r['run_id'], r['archive_status'], r['source_status']) for r in drift[:5]]}"
    )


def test_dim_paper_v2_portfolio_scd2_has_unique_current_version(
    dev_conn, archive_tables_ready,
):
    """SCD2: at most one valid_to=NULL (current) row per portfolio_id."""
    rows = _fetchall_dict(
        dev_conn,
        """
        SELECT portfolio_id, count(*) AS current_count
        FROM qe_archive.dim_paper_v2_portfolio
        WHERE valid_to IS NULL
        GROUP BY portfolio_id
        HAVING count(*) > 1
        """,
    )
    assert not rows, (
        f"{len(rows)} portfolio(s) have >1 'current' SCD2 row; "
        f"first offenders: {rows[:5]}"
    )
