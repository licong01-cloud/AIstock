from __future__ import annotations

import os

import psycopg2
import pytest


@pytest.mark.skipif(
    not os.environ.get("AISTOCK_PHASE1R_DEV_CATALOG_DSN"),
    reason="explicit AISTOCK_PHASE1R_DEV_CATALOG_DSN is required; no database settings are guessed",
)
def test_explicit_dev_catalog_contains_phase1r_r1_tables() -> None:
    """Read-only catalog check after an explicitly authorized DEV migration."""

    dsn = os.environ["AISTOCK_PHASE1R_DEV_CATALOG_DSN"]
    expected = {
        "advisory_historical_range_batch",
        "advisory_historical_range_run",
        "advisory_historical_range_day_run",
        "advisory_historical_range_day_attempt",
        "advisory_historical_range_operation",
        "advisory_historical_range_operation_attempt",
        "advisory_historical_range_candidate",
        "advisory_historical_range_list_version",
        "advisory_historical_range_list_item",
        "advisory_historical_range_episode_snapshot",
        "advisory_historical_range_outcome",
        "advisory_historical_range_summary",
    }
    connection = psycopg2.connect(dsn)
    try:
        connection.set_session(readonly=True, autocommit=False)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'app'
                  AND table_name LIKE 'advisory_historical_range_%'
                """
            )
            actual = {str(row[0]) for row in cursor.fetchall()}
        assert expected <= actual
    finally:
        connection.rollback()
        connection.close()
