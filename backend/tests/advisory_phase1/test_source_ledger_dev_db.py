"""Explicitly authorized rollback-only DEV-DB L4 for the source ledger."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import psycopg2.extras
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
_MIGRATION = Path("backend/db/migrations/add_advisory_phase1c2_source_revision_cutoff_20260713.sql")
_ROLLBACK = Path("backend/db/migrations/add_advisory_phase1c2_source_revision_cutoff_20260713.rollback.sql")


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
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'app'
              AND (
                    (table_name = 'advisory_source_revision_set' AND column_name = 'schema_version')
                    OR (
                        table_name = 'advisory_source_revision_member'
                        AND column_name = 'enforced_cutoff_predicate_hash'
                    )
                  )
            """
        )
        assert set(cur.fetchall()) == {
            ("advisory_source_revision_set", "schema_version"),
            ("advisory_source_revision_member", "enforced_cutoff_predicate_hash"),
        }


def _apply_sql(conn: Any, path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))


def test_source_ledger_l4_dev_db_is_append_only_and_rolls_back() -> None:
    conn = psycopg2.connect(**_dev_dsn(), connect_timeout=5)
    conn.autocommit = True
    applied = False
    try:
        _apply_sql(conn, _ROLLBACK)
        _apply_sql(conn, _MIGRATION)
        _apply_sql(conn, _MIGRATION)
        applied = True
        conn.autocommit = False
        _assert_l4_schema(conn)

        with conn.cursor() as cur:
            cur.execute("SAVEPOINT source_revision_schema_v2_insert_contract")
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1_SOURCE_REVISION_SET_SCHEMA_INVALID"):
                cur.execute(
                    """
                    INSERT INTO app.advisory_source_revision_set (
                        source_revision_set_id, source_revision_set_hash, query_registry_hash,
                        schema_version, requested_source_cutoff, label_as_of_ts, research_only, member_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, TRUE, 1)
                    """,
                    (
                        "phase1c2-explicit-v1-rejected",
                        "7" * 64,
                        "8" * 64,
                        "advisory_phase1_source_revision_set_v1",
                        datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
                        datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
                    ),
                )
            cur.execute("ROLLBACK TO SAVEPOINT source_revision_schema_v2_insert_contract")
            cur.execute(
                """
                INSERT INTO app.advisory_source_revision_set (
                    source_revision_set_id, source_revision_set_hash, query_registry_hash,
                    requested_source_cutoff, label_as_of_ts, research_only, member_count
                ) VALUES (%s, %s, %s, %s, %s, TRUE, 1)
                RETURNING schema_version
                """,
                (
                    "phase1c2-default-v2",
                    "9" * 64,
                    "0" * 64,
                    datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
                    datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
                ),
            )
            assert cur.fetchone() == ("advisory_phase1_source_revision_set_v2",)
            cur.execute("ROLLBACK TO SAVEPOINT source_revision_schema_v2_insert_contract")

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
            enforced_cutoff_predicate_hash="c" * 64,
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
                """
                SELECT source_revision_set_id, schema_version
                FROM app.advisory_source_revision_set
                WHERE source_revision_set_id = %s
                """,
                (revision_set.source_revision_set_id,),
            )
            assert cur.fetchone() == (revision_set.source_revision_set_id, revision_set.schema_version)
            cur.execute(
                """
                SELECT enforced_cutoff_predicate_hash
                FROM app.advisory_source_revision_member
                WHERE source_revision_set_id = %s
                """,
                (revision_set.source_revision_set_id,),
            )
            assert cur.fetchone() == (member.enforced_cutoff_predicate_hash,)
            cur.execute("SAVEPOINT missing_v2_cutoff_predicate")
            cur.execute(
                """
                INSERT INTO app.advisory_source_revision_set (
                    source_revision_set_id, source_revision_set_hash, query_registry_hash,
                    schema_version, requested_source_cutoff, label_as_of_ts, research_only, member_count
                ) VALUES (%s, %s, %s, %s, %s, %s, TRUE, 1)
                """,
                (
                    "phase1c2-l4-missing-cutoff",
                    "f" * 64,
                    "e" * 64,
                    "advisory_phase1_source_revision_set_v2",
                    second.formal_available_at,
                    second.formal_available_at,
                ),
            )
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1_SOURCE_REVISION_MEMBER_INVALID"):
                cur.execute(
                    """
                    INSERT INTO app.advisory_source_revision_member (
                        source_revision_set_id, member_key, source_role, dataset_name,
                        query_template_id, query_template_version, query_template_hash,
                        bound_parameter_hash, partition_key, partition_key_hash, revision_kind,
                        revision_id, availability_event_hash, availability_requirement,
                        business_min_date, business_max_date, available_at_min, available_at_max,
                        schema_fingerprint, row_count, partition_content_hash, quality_status,
                        research_only
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE
                    )
                    """,
                    (
                        "phase1c2-l4-missing-cutoff",
                        member.member_key,
                        member.source_role,
                        member.dataset_name,
                        member.query_template_id,
                        member.query_template_version,
                        member.query_template_hash,
                        member.bound_parameter_hash,
                        psycopg2.extras.Json(member.partition_key),
                        member.partition_key_hash,
                        member.revision_kind.value,
                        member.revision_id,
                        member.availability_event.event_content_hash if member.availability_event else None,
                        member.availability_requirement.value,
                        member.business_min_date,
                        member.business_max_date,
                        member.available_at_min,
                        member.available_at_max,
                        member.schema_fingerprint,
                        member.row_count,
                        member.partition_content_hash,
                        member.quality_status,
                    ),
                )
            cur.execute("ROLLBACK TO SAVEPOINT missing_v2_cutoff_predicate")
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
        try:
            if applied:
                conn.autocommit = True
                _apply_sql(conn, _ROLLBACK)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT count(*)
                        FROM information_schema.columns
                        WHERE table_schema = 'app'
                          AND (
                                (table_name = 'advisory_source_revision_set' AND column_name = 'schema_version')
                                OR (
                                    table_name = 'advisory_source_revision_member'
                                    AND column_name = 'enforced_cutoff_predicate_hash'
                                )
                              )
                        """
                    )
                    assert cur.fetchone() == (0,)
                _apply_sql(conn, _ROLLBACK)
        finally:
            conn.close()
