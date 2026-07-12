"""PostgreSQL repository for immutable Advisory source availability evidence.

The repository owns only ``app.advisory_source_availability_event``.  It does
not start ingestion, access real-time providers, or modify market source data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterator

import psycopg2
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.source_ledger import (
    REASON_EVENT_CHAIN_INVALID,
    REASON_EVENT_CONFLICT,
    REASON_SOURCE_INVALIDATED,
    REASON_SOURCE_QUALITY_INVALID,
    REASON_SOURCE_UNAVAILABLE,
    SourceAvailabilityEvent,
    SourceAvailabilityEventInput,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
    SourceLedgerError,
)


ConnFactory = Callable[[], Iterator[Any]]


def _transactional_conn_factory() -> Iterator[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class PostgresSourceAvailabilityLedger:
    """Append and select canonical source evidence with per-chain serialization."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def append(self, request: SourceAvailabilityEventRequest) -> SourceAvailabilityEvent:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (request.derived_partition_chain_key,))
                cur.execute(
                    """
                    SELECT * FROM app.advisory_source_availability_event
                    WHERE dataset_name = %s AND source_role = %s
                      AND partition_key_hash = %s AND event_revision_no = %s
                    FOR UPDATE
                    """,
                    (request.dataset_name, request.source_role, request.partition_key_hash, request.event_revision_no),
                )
                existing_row = cur.fetchone()
                if existing_row is not None:
                    existing = _event_from_row(dict(existing_row))
                    if existing.input.append_request_hash == request.derived_append_request_hash:
                        return existing
                    raise SourceLedgerError(
                        REASON_EVENT_CONFLICT,
                        "same natural partition revision has a different append request",
                        context={"availability_event_id": existing.availability_event_id},
                    )
                cur.execute("SELECT clock_timestamp() AS first_observed_at")
                event = SourceAvailabilityEvent.from_request(
                    request,
                    first_observed_at=cur.fetchone()["first_observed_at"],
                )
                try:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_source_availability_event (
                            availability_event_id, append_request_hash, dataset_name, source_role, partition_key,
                            partition_key_hash, partition_chain_key, revision_id, event_revision_no,
                            event_type, predecessor_event_hash, provider_job_id, refresh_job_id,
                            provider_published_at, first_observed_at, formal_available_at,
                            schema_fingerprint, row_count, partition_content_hash, quality_status,
                            reason_codes, event_content_hash, created_by_service_principal
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        RETURNING *
                        """,
                        _event_insert_params(event),
                    )
                except (psycopg2.IntegrityError, psycopg2.errors.RaiseException) as exc:
                    raise SourceLedgerError(
                        REASON_EVENT_CHAIN_INVALID,
                        "database rejected the source availability chain",
                        context={"database_constraint": getattr(exc.diag, "constraint_name", None)},
                    ) from exc
                return _event_from_row(dict(cur.fetchone()))

    def select_as_of(
        self,
        *,
        dataset_name: str,
        source_role: str,
        partition_key: dict[str, Any],
        cutoff: datetime,
    ) -> SourceAvailabilityEvent:
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise SourceLedgerError("ADVISORY_PHASE1_SOURCE_EVENT_TIME_INVALID", "cutoff must include an explicit timezone")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM app.advisory_source_availability_event
                    WHERE dataset_name = %s AND source_role = %s AND partition_key_hash = %s
                      AND formal_available_at <= %s
                    ORDER BY event_revision_no DESC
                    LIMIT 1
                    """,
                    (
                        dataset_name,
                        source_role,
                        canonical_json_sha256(canonicalize(partition_key)),
                        cutoff.astimezone(timezone.utc),
                    ),
                )
                row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(
                REASON_SOURCE_UNAVAILABLE,
                "no source availability event was formally available by cutoff",
                context={"dataset_name": dataset_name, "source_role": source_role, "cutoff": cutoff.isoformat()},
            )
        event = _event_from_row(dict(row))
        if event.event_type is SourceAvailabilityEventType.INVALIDATED:
            raise SourceLedgerError(
                REASON_SOURCE_INVALIDATED,
                "latest source event available by cutoff is invalidated",
                context={"availability_event_hash": event.event_content_hash},
            )
        if event.input.quality_status != "PASS":
            raise SourceLedgerError(
                REASON_SOURCE_QUALITY_INVALID,
                "latest source event available by cutoff does not have PASS quality",
                context={"availability_event_hash": event.event_content_hash, "quality_status": event.input.quality_status},
            )
        return event


def _event_insert_params(event: SourceAvailabilityEvent) -> tuple[Any, ...]:
    item = event.input
    return (
        event.availability_event_id,
        item.append_request_hash,
        item.dataset_name,
        item.source_role,
        psycopg2.extras.Json(canonicalize(item.partition_key)),
        item.partition_key_hash,
        item.partition_chain_key,
        item.revision_id,
        item.event_revision_no,
        item.event_type.value,
        item.predecessor_event_hash,
        item.provider_job_id,
        item.refresh_job_id,
        item.provider_published_at,
        item.first_observed_at,
        item.formal_available_at,
        item.schema_fingerprint,
        item.row_count,
        item.partition_content_hash,
        item.quality_status,
        psycopg2.extras.Json(list(item.reason_codes)),
        event.event_content_hash,
        item.created_by_service_principal,
    )


def _event_from_row(row: dict[str, Any]) -> SourceAvailabilityEvent:
    item = SourceAvailabilityEventInput(
        dataset_name=str(row["dataset_name"]),
        source_role=str(row["source_role"]),
        partition_key=dict(row["partition_key"]),
        partition_chain_key=str(row["partition_chain_key"]),
        append_request_hash=str(row["append_request_hash"]),
        revision_id=str(row["revision_id"]),
        event_revision_no=int(row["event_revision_no"]),
        event_type=SourceAvailabilityEventType(str(row["event_type"])),
        predecessor_event_hash=str(row["predecessor_event_hash"]) if row["predecessor_event_hash"] is not None else None,
        provider_job_id=str(row["provider_job_id"]) if row["provider_job_id"] is not None else None,
        refresh_job_id=str(row["refresh_job_id"]) if row["refresh_job_id"] is not None else None,
        provider_published_at=row["provider_published_at"],
        first_observed_at=row["first_observed_at"],
        schema_fingerprint=str(row["schema_fingerprint"]),
        row_count=int(row["row_count"]),
        partition_content_hash=str(row["partition_content_hash"]),
        quality_status=str(row["quality_status"]),
        reason_codes=tuple(str(code) for code in row["reason_codes"] or []),
        created_by_service_principal=str(row["created_by_service_principal"]),
    )
    event = SourceAvailabilityEvent.from_input(item)
    if event.availability_event_id != str(row["availability_event_id"]) or event.event_content_hash != str(row["event_content_hash"]):
        raise SourceLedgerError(
            "ADVISORY_PHASE1_SOURCE_EVENT_CONFLICT",
            "persisted source availability row does not match its canonical hash",
            context={"availability_event_id": str(row["availability_event_id"])},
        )
    return event
