"""Explicitly authorized rollback-only DEV-DB L4 for the source ledger."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import pytest
from dotenv import load_dotenv

from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
)
from backend.services.advisory_phase1.source_ledger_postgres import PostgresSourceAvailabilityLedger
from backend.services.advisory_phase1.source_revision import (
    AvailabilityRequirement,
    SourceRevisionKind,
    SourceRevisionMemberInput,
    build_source_revision_set,
)
from backend.services.advisory_phase1.source_revision_postgres import PostgresSourceRevisionRepository


_ENV_FILE = Path("F:/Dev/AIstock/.env")


def _dev_dsn() -> dict[str, Any]:
    if os.getenv("AISTOCK_DEV_DB_E2E") != "1":
        pytest.skip("set AISTOCK_DEV_DB_E2E=1 to authorize the DEV-DB stateful L4 gate")
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=False)
    dsn = {
        "host": os.getenv("TDX_DB_DEV_HOST"),
        "port": int(os.getenv("TDX_DB_DEV_PORT", "0")),
        "dbname": os.getenv("TDX_DB_DEV_NAME"),
        "user": os.getenv("TDX_DB_DEV_USER"),
        "password": os.getenv("TDX_DB_DEV_PASSWORD"),
    }
    if dsn["host"] != "127.0.0.1" or dsn["port"] != 5433 or "dev" not in str(dsn["dbname"] or "").lower():
        raise AssertionError(f"refusing source-ledger L4 target {dsn['host']}:{dsn['port']}/{dsn['dbname']}")
    if not dsn["user"] or not dsn["password"]:
        pytest.skip("DEV DB credentials are unavailable")
    return dsn


def _event(*, number: int, predecessor: str | None = None, event_type: SourceAvailabilityEventType = SourceAvailabilityEventType.INGESTED) -> SourceAvailabilityEventRequest:
    return SourceAvailabilityEventRequest(
        dataset_name="market.kline_daily_raw",
        source_role="FEATURE_T",
        partition_key={"trade_date": "2026-06-30", "l4": "source-ledger"},
        revision_id=f"l4-r{number}",
        event_revision_no=number,
        event_type=event_type,
        predecessor_event_hash=predecessor,
        provider_job_id="l4-ingestion",
        refresh_job_id="l4-refresh",
        schema_fingerprint="l4-schema-v1",
        row_count=17,
        partition_content_hash=(str(number) * 64)[:64],
        quality_status="PASS",
        created_by_service_principal="dev-db-l4",
    )


def _assert_l4_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'app' AND table_name = 'advisory_source_availability_event'
            """
        )
        assert cur.fetchone() == ("advisory_source_availability_event",)
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'app'
              AND table_name = ANY(%s)
            """,
            (["advisory_source_revision_set", "advisory_source_revision_member"],),
        )
        assert {row[0] for row in cur.fetchall()} == {"advisory_source_revision_set", "advisory_source_revision_member"}


def test_source_ledger_l4_dev_db_is_append_only_and_rolls_back() -> None:
    conn = psycopg2.connect(**_dev_dsn(), connect_timeout=5)
    conn.autocommit = False
    try:
        _assert_l4_schema(conn)

        @contextmanager
        def conn_factory() -> Iterator[Any]:
            yield conn

        ledger = PostgresSourceAvailabilityLedger(conn_factory=conn_factory)
        first = ledger.append(_event(number=1))
        second = ledger.append(
            _event(number=2, predecessor=first.event_content_hash, event_type=SourceAvailabilityEventType.CORRECTED)
        )
        assert ledger.append(_event(number=1)) == first
        assert ledger.select_as_of(
            dataset_name=first.input.dataset_name,
            source_role=first.input.source_role,
            partition_key=first.input.partition_key,
            cutoff=second.formal_available_at,
        ) == second
        member = SourceRevisionMemberInput(
            source_role=second.input.source_role,
            dataset_name=second.input.dataset_name,
            query_template_id="l4-kline-template",
            query_template_version="1",
            query_template_hash="a" * 64,
            bound_parameter_hash="b" * 64,
            partition_key=second.input.partition_key,
            revision_kind=SourceRevisionKind.IMMUTABLE_INGESTION,
            revision_id=second.input.revision_id,
            availability_requirement=AvailabilityRequirement.DECISION_CUTOFF,
            business_min_date=datetime(2026, 6, 30, tzinfo=timezone.utc).date(),
            business_max_date=datetime(2026, 6, 30, tzinfo=timezone.utc).date(),
            available_at_min=second.formal_available_at,
            available_at_max=second.formal_available_at,
            schema_fingerprint=second.input.schema_fingerprint,
            row_count=second.input.row_count,
            partition_content_hash=second.input.partition_content_hash,
            quality_status=second.input.quality_status,
            availability_event=second,
        )
        revision_set = build_source_revision_set(
            query_registry_hash="c" * 64,
            requested_source_cutoff=second.formal_available_at,
            label_as_of_ts=second.formal_available_at,
            research_only=True,
            members=[member],
        )
        revisions = PostgresSourceRevisionRepository(conn_factory=conn_factory)
        assert revisions.freeze(revision_set) == revision_set
        assert revisions.freeze(revision_set) == revision_set
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_revision_member WHERE source_revision_set_id = %s",
                (revision_set.source_revision_set_id,),
            )
            assert cur.fetchone() == (1,)
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1_SOURCE_EVENT_IMMUTABLE"):
                cur.execute(
                    "UPDATE app.advisory_source_availability_event SET quality_status = 'FAILED' WHERE availability_event_id = %s",
                    (first.availability_event_id,),
                )
            conn.rollback()
        # The failed mutation rolled back the test transaction. Reopen a fresh
        # transaction and verify the L4 rows did not survive it.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_availability_event WHERE partition_chain_key = %s",
                (first.partition_chain_key,),
            )
            assert cur.fetchone() == (0,)
    finally:
        conn.rollback()
        conn.close()
