from __future__ import annotations

import os

import psycopg2
import pytest

from backend.services.advisory_historical_range.query_repository import PostgresHistoricalRangeQueryRepository


@pytest.mark.skipif(
    not os.getenv("AISTOCK_PHASE1R_R5_DEV_DSN"),
    reason="AISTOCK_PHASE1R_R5_DEV_DSN is required for the explicit DEV PostgreSQL read E2E",
)
def test_real_dev_postgres_batch_and_operation_projection() -> None:
    dsn = os.environ["AISTOCK_PHASE1R_R5_DEV_DSN"]

    def connect():
        return psycopg2.connect(dsn, connect_timeout=5)

    repository = PostgresHistoricalRangeQueryRepository(conn_factory=connect)
    page = repository.list_batches(limit=5)
    assert page["page"]["limit"] == 5
    assert isinstance(page["items"], list)
    if page["items"]:
        batch = repository.get_batch(str(page["items"][0]["batch_id"]))
        operations = repository.list_operations(batch_id=str(batch["batch_id"]), limit=5)
        assert isinstance(operations["items"], list)
