"""PostgreSQL persistence for immutable Advisory source-revision sets."""

from __future__ import annotations

from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_phase0a.policy import canonicalize
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.source_revision import (
    REASON_REVISION_SET_CONFLICT,
    SourceRevisionMemberInput,
    SourceRevisionSet,
)


ConnFactory = Callable[[], Iterator[Any]]


def _transactional_conn_factory() -> Iterator[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class PostgresSourceRevisionRepository:
    """Persist a complete source set atomically, or return its exact retry."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def freeze(self, revision_set: SourceRevisionSet) -> SourceRevisionSet:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_source_revision_set (
                        source_revision_set_id, source_revision_set_hash, query_registry_hash,
                        requested_source_cutoff, label_as_of_ts, research_only, member_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_revision_set_hash) DO NOTHING
                    RETURNING source_revision_set_id, query_registry_hash, requested_source_cutoff,
                              label_as_of_ts, research_only, member_count
                    """,
                    (
                        revision_set.source_revision_set_id,
                        revision_set.source_revision_set_hash,
                        revision_set.query_registry_hash,
                        revision_set.requested_source_cutoff,
                        revision_set.label_as_of_ts,
                        revision_set.research_only,
                        len(revision_set.members),
                    ),
                )
                inserted = cur.fetchone()
                if inserted is None:
                    cur.execute(
                        """
                        SELECT source_revision_set_id, query_registry_hash, requested_source_cutoff,
                               label_as_of_ts, research_only, member_count
                        FROM app.advisory_source_revision_set
                        WHERE source_revision_set_hash = %s
                        FOR KEY SHARE
                        """,
                        (revision_set.source_revision_set_hash,),
                    )
                    existing = cur.fetchone()
                    if existing is None or not _matches_set_row(existing, revision_set):
                        raise SourceLedgerError(
                            REASON_REVISION_SET_CONFLICT,
                            "same source revision hash has a conflicting persisted header",
                            context={"source_revision_set_hash": revision_set.source_revision_set_hash},
                        )
                    cur.execute(
                        "SELECT * FROM app.advisory_source_revision_member WHERE source_revision_set_id = %s ORDER BY member_key",
                        (existing["source_revision_set_id"],),
                    )
                    persisted_members = list(cur.fetchall())
                    if not _matches_member_rows(persisted_members, revision_set):
                        raise SourceLedgerError(
                            REASON_REVISION_SET_CONFLICT,
                            "same source revision hash has a conflicting persisted member set",
                            context={"source_revision_set_hash": revision_set.source_revision_set_hash},
                        )
                    return revision_set
                for member in revision_set.members:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_source_revision_member (
                            source_revision_set_id, member_key, source_role, dataset_name,
                            query_template_id, query_template_version, query_template_hash,
                            bound_parameter_hash, partition_key, partition_key_hash, revision_kind,
                            revision_id, availability_event_hash, availability_requirement,
                            business_min_date, business_max_date, available_at_min, available_at_max,
                            schema_fingerprint, row_count, partition_content_hash, quality_status,
                            reason_codes, research_only
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        _member_params(revision_set.source_revision_set_id, member),
                    )
        return revision_set


def _matches_set_row(row: Any, revision_set: SourceRevisionSet) -> bool:
    return (
        str(row["source_revision_set_id"]) == revision_set.source_revision_set_id
        and str(row["query_registry_hash"]) == revision_set.query_registry_hash
        and row["requested_source_cutoff"] == revision_set.requested_source_cutoff
        and row["label_as_of_ts"] == revision_set.label_as_of_ts
        and bool(row["research_only"]) is revision_set.research_only
        and int(row["member_count"]) == len(revision_set.members)
    )


def _member_params(source_revision_set_id: str, member: SourceRevisionMemberInput) -> tuple[Any, ...]:
    return (
        source_revision_set_id,
        member.member_key,
        member.source_role,
        member.dataset_name,
        member.query_template_id,
        member.query_template_version,
        member.query_template_hash,
        member.bound_parameter_hash,
        psycopg2.extras.Json(canonicalize(member.partition_key)),
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
        psycopg2.extras.Json(list(member.reason_codes)),
        member.research_only,
    )


def _matches_member_rows(rows: list[Any], revision_set: SourceRevisionSet) -> bool:
    expected = sorted((_member_payload(member) for member in revision_set.members), key=lambda item: item["member_key"])
    persisted = sorted((_row_payload(row) for row in rows), key=lambda item: item["member_key"])
    return persisted == expected


def _member_payload(member: SourceRevisionMemberInput) -> dict[str, Any]:
    return {
        "member_key": member.member_key,
        "source_role": member.source_role,
        "dataset_name": member.dataset_name,
        "query_template_id": member.query_template_id,
        "query_template_version": member.query_template_version,
        "query_template_hash": member.query_template_hash,
        "bound_parameter_hash": member.bound_parameter_hash,
        "partition_key": canonicalize(member.partition_key),
        "partition_key_hash": member.partition_key_hash,
        "revision_kind": member.revision_kind.value,
        "revision_id": member.revision_id,
        "availability_event_hash": member.availability_event.event_content_hash if member.availability_event else None,
        "availability_requirement": member.availability_requirement.value,
        "business_min_date": member.business_min_date,
        "business_max_date": member.business_max_date,
        "available_at_min": member.available_at_min,
        "available_at_max": member.available_at_max,
        "schema_fingerprint": member.schema_fingerprint,
        "row_count": member.row_count,
        "partition_content_hash": member.partition_content_hash,
        "quality_status": member.quality_status,
        "reason_codes": list(member.reason_codes),
        "research_only": member.research_only,
    }


def _row_payload(row: Any) -> dict[str, Any]:
    return {
        "member_key": str(row["member_key"]),
        "source_role": str(row["source_role"]),
        "dataset_name": str(row["dataset_name"]),
        "query_template_id": str(row["query_template_id"]),
        "query_template_version": str(row["query_template_version"]),
        "query_template_hash": str(row["query_template_hash"]),
        "bound_parameter_hash": str(row["bound_parameter_hash"]),
        "partition_key": canonicalize(dict(row["partition_key"])),
        "partition_key_hash": str(row["partition_key_hash"]),
        "revision_kind": str(row["revision_kind"]),
        "revision_id": str(row["revision_id"]),
        "availability_event_hash": str(row["availability_event_hash"]) if row["availability_event_hash"] else None,
        "availability_requirement": str(row["availability_requirement"]),
        "business_min_date": row["business_min_date"],
        "business_max_date": row["business_max_date"],
        "available_at_min": row["available_at_min"],
        "available_at_max": row["available_at_max"],
        "schema_fingerprint": str(row["schema_fingerprint"]),
        "row_count": int(row["row_count"]),
        "partition_content_hash": str(row["partition_content_hash"]),
        "quality_status": str(row["quality_status"]),
        "reason_codes": list(row["reason_codes"] or []),
        "research_only": bool(row["research_only"]),
    }
