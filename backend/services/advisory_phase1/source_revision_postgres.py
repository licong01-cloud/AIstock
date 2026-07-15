"""PostgreSQL persistence for immutable Advisory source-revision sets."""

from __future__ import annotations

from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_phase0a.policy import canonicalize
from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEvent,
    SourceAvailabilityEventInput,
    SourceAvailabilityEventType,
    SourceLedgerError,
)
from backend.services.advisory_phase1.source_revision import (
    AvailabilityRequirement,
    REASON_REVISION_SET_CONFLICT,
    SourceRevisionKind,
    SourceRevisionMemberInput,
    SourceRevisionSet,
    build_source_revision_set,
)


ConnFactory = Callable[[], Iterator[Any]]

REASON_PHASE1G_SOURCE_REVISION_CONFLICT = "ADVISORY_PHASE1G_SOURCE_REVISION_CONFLICT"


SOURCE_REVISION_MEMBER_INSERT_SQL = """
    INSERT INTO app.advisory_source_revision_member (
        source_revision_set_id, member_key, source_role, dataset_name,
        query_template_id, query_template_version, query_template_hash,
        bound_parameter_hash, enforced_cutoff_predicate_hash, partition_key, partition_key_hash, revision_kind,
        revision_id, availability_event_hash, availability_requirement,
        business_min_date, business_max_date, available_at_min, available_at_max,
        schema_fingerprint, row_count, partition_content_hash, quality_status,
        reason_codes, research_only
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
"""

SOURCE_REVISION_SET_SELECT_FOR_KEY_SHARE_SQL = """
    SELECT source_revision_set_id, source_revision_set_hash, query_registry_hash,
           schema_version, requested_source_cutoff, label_as_of_ts, research_only, member_count
    FROM app.advisory_source_revision_set
    WHERE source_revision_set_hash = %s
    FOR KEY SHARE
"""

SOURCE_REVISION_MEMBER_SELECT_EXACT_SQL = """
    SELECT m.member_key, m.source_role, m.dataset_name,
           m.query_template_id, m.query_template_version, m.query_template_hash,
           m.bound_parameter_hash, m.enforced_cutoff_predicate_hash,
           m.partition_key, m.partition_key_hash, m.revision_kind, m.revision_id,
           m.availability_event_hash, m.availability_requirement,
           m.business_min_date, m.business_max_date, m.available_at_min, m.available_at_max,
           m.schema_fingerprint, m.row_count, m.partition_content_hash, m.quality_status,
           m.reason_codes, m.research_only,
           e.availability_event_id AS event_availability_event_id,
           e.append_request_hash AS event_append_request_hash,
           e.dataset_name AS event_dataset_name,
           e.source_role AS event_source_role,
           e.partition_key AS event_partition_key,
           e.partition_key_hash AS event_partition_key_hash,
           e.partition_chain_key AS event_partition_chain_key,
           e.revision_id AS event_revision_id,
           e.event_revision_no AS event_revision_no,
           e.event_type AS event_type,
           e.predecessor_event_hash AS event_predecessor_event_hash,
           e.provider_job_id AS event_provider_job_id,
           e.refresh_job_id AS event_refresh_job_id,
           e.provider_published_at AS event_provider_published_at,
           e.first_observed_at AS event_first_observed_at,
           e.schema_fingerprint AS event_schema_fingerprint,
           e.row_count AS event_row_count,
           e.partition_content_hash AS event_partition_content_hash,
           e.quality_status AS event_quality_status,
           e.reason_codes AS event_reason_codes,
           e.event_content_hash AS event_content_hash,
           e.created_by_service_principal AS event_created_by_service_principal
    FROM app.advisory_source_revision_member AS m
    LEFT JOIN app.advisory_source_availability_event AS e
      ON e.event_content_hash = m.availability_event_hash
    WHERE m.source_revision_set_id = %s
    ORDER BY m.member_key
"""


def _transactional_conn_factory() -> Iterator[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class PostgresSourceRevisionRepository:
    """Persist a complete source set atomically, or return its exact retry."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def freeze(self, revision_set: SourceRevisionSet) -> SourceRevisionSet:
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    return self.freeze_in_transaction(cur, revision_set)
        except SourceLedgerError as exc:
            if exc.reason_code != REASON_PHASE1G_SOURCE_REVISION_CONFLICT:
                raise
            raise SourceLedgerError(
                REASON_REVISION_SET_CONFLICT,
                "source revision set conflicts with persisted content",
                context=exc.context,
            ) from exc

    def freeze_in_transaction(
        self, cur: Any, revision_set: SourceRevisionSet
    ) -> SourceRevisionSet:
        """Freeze a complete set using the caller's transaction and cursor."""

        cur.execute(
            """
                    INSERT INTO app.advisory_source_revision_set (
                        source_revision_set_id, source_revision_set_hash, query_registry_hash,
                        schema_version, requested_source_cutoff, label_as_of_ts, research_only, member_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING source_revision_set_id, query_registry_hash, schema_version, requested_source_cutoff,
                              label_as_of_ts, research_only, member_count
                    """,
            (
                revision_set.source_revision_set_id,
                revision_set.source_revision_set_hash,
                revision_set.query_registry_hash,
                revision_set.schema_version,
                revision_set.requested_source_cutoff,
                revision_set.label_as_of_ts,
                revision_set.research_only,
                len(revision_set.members),
            ),
        )
        inserted = cur.fetchone()
        if inserted is not None:
            for member in revision_set.members:
                cur.execute(
                    SOURCE_REVISION_MEMBER_INSERT_SQL,
                    _member_params(revision_set.source_revision_set_id, member),
                )
        persisted = self.read_exact_in_transaction(
            cur, revision_set.source_revision_set_hash
        )
        if persisted.model_dump(mode="json") != revision_set.model_dump(mode="json"):
            raise SourceLedgerError(
                REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
                "same source revision hash has conflicting persisted content",
                context={
                    "source_revision_set_hash": revision_set.source_revision_set_hash
                },
            )
        return revision_set

    @staticmethod
    def read_exact_in_transaction(
        cur: Any, source_revision_set_hash: str
    ) -> SourceRevisionSet:
        """Read and reconstruct a complete source set under a fixed key-share lock."""

        cur.execute(
            SOURCE_REVISION_SET_SELECT_FOR_KEY_SHARE_SQL, (source_revision_set_hash,)
        )
        header = cur.fetchone()
        if header is None:
            raise SourceLedgerError(
                REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
                "source revision set is missing during exact read",
                context={"source_revision_set_hash": source_revision_set_hash},
            )
        cur.execute(
            SOURCE_REVISION_MEMBER_SELECT_EXACT_SQL, (header["source_revision_set_id"],)
        )
        rows = list(cur.fetchall())
        return _reconstruct_source_revision_set(header, rows)

    @staticmethod
    def read_exact_readonly(
        cur: Any, source_revision_set_hash: str
    ) -> SourceRevisionSet:
        """Read the same complete set without row locks on a read-only connection."""

        cur.execute(
            """
            SELECT source_revision_set_id, source_revision_set_hash, query_registry_hash,
                   schema_version, requested_source_cutoff, label_as_of_ts, research_only, member_count
            FROM app.advisory_source_revision_set
            WHERE source_revision_set_hash = %s
            """,
            (source_revision_set_hash,),
        )
        header = cur.fetchone()
        if header is None:
            raise SourceLedgerError(
                REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
                "source revision set is missing during exact read",
                context={"source_revision_set_hash": source_revision_set_hash},
            )
        cur.execute(
            SOURCE_REVISION_MEMBER_SELECT_EXACT_SQL, (header["source_revision_set_id"],)
        )
        return _reconstruct_source_revision_set(header, list(cur.fetchall()))


def _matches_set_row(row: Any, revision_set: SourceRevisionSet) -> bool:
    return (
        str(row["source_revision_set_id"]) == revision_set.source_revision_set_id
        and str(row["query_registry_hash"]) == revision_set.query_registry_hash
        and str(row["schema_version"]) == revision_set.schema_version
        and row["requested_source_cutoff"] == revision_set.requested_source_cutoff
        and row["label_as_of_ts"] == revision_set.label_as_of_ts
        and bool(row["research_only"]) is revision_set.research_only
        and int(row["member_count"]) == len(revision_set.members)
    )


def _member_params(
    source_revision_set_id: str, member: SourceRevisionMemberInput
) -> tuple[Any, ...]:
    return (
        source_revision_set_id,
        member.member_key,
        member.source_role,
        member.dataset_name,
        member.query_template_id,
        member.query_template_version,
        member.query_template_hash,
        member.bound_parameter_hash,
        member.enforced_cutoff_predicate_hash,
        psycopg2.extras.Json(canonicalize(member.partition_key)),
        member.partition_key_hash,
        member.revision_kind.value,
        member.revision_id,
        (
            member.availability_event.event_content_hash
            if member.availability_event
            else None
        ),
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
    expected = sorted(
        (_member_payload(member) for member in revision_set.members),
        key=lambda item: item["member_key"],
    )
    persisted = sorted(
        (_row_payload(row) for row in rows), key=lambda item: item["member_key"]
    )
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
        "enforced_cutoff_predicate_hash": member.enforced_cutoff_predicate_hash,
        "partition_key": canonicalize(member.partition_key),
        "partition_key_hash": member.partition_key_hash,
        "revision_kind": member.revision_kind.value,
        "revision_id": member.revision_id,
        "availability_event_hash": (
            member.availability_event.event_content_hash
            if member.availability_event
            else None
        ),
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
        "enforced_cutoff_predicate_hash": str(row["enforced_cutoff_predicate_hash"]),
        "partition_key": canonicalize(dict(row["partition_key"])),
        "partition_key_hash": str(row["partition_key_hash"]),
        "revision_kind": str(row["revision_kind"]),
        "revision_id": str(row["revision_id"]),
        "availability_event_hash": (
            str(row["availability_event_hash"])
            if row["availability_event_hash"]
            else None
        ),
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


def _reconstruct_source_revision_set(header: Any, rows: list[Any]) -> SourceRevisionSet:
    members = [_member_from_exact_row(row) for row in rows]
    if int(header["member_count"]) != len(members):
        raise SourceLedgerError(
            REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
            "source revision member count does not match the persisted header",
            context={
                "source_revision_set_hash": str(header["source_revision_set_hash"])
            },
        )
    rebuilt = build_source_revision_set(
        query_registry_hash=str(header["query_registry_hash"]),
        requested_source_cutoff=header["requested_source_cutoff"],
        label_as_of_ts=header["label_as_of_ts"],
        research_only=bool(header["research_only"]),
        members=members,
    )
    if (
        rebuilt.source_revision_set_id != str(header["source_revision_set_id"])
        or rebuilt.source_revision_set_hash != str(header["source_revision_set_hash"])
        or rebuilt.schema_version != str(header["schema_version"])
    ):
        raise SourceLedgerError(
            REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
            "source revision header does not match reconstructed canonical content",
            context={
                "source_revision_set_hash": str(header["source_revision_set_hash"])
            },
        )
    return rebuilt


def _member_from_exact_row(row: Any) -> SourceRevisionMemberInput:
    availability_event = _availability_event_from_exact_row(row)
    try:
        member = SourceRevisionMemberInput(
            source_role=str(row["source_role"]),
            dataset_name=str(row["dataset_name"]),
            query_template_id=str(row["query_template_id"]),
            query_template_version=str(row["query_template_version"]),
            query_template_hash=str(row["query_template_hash"]),
            bound_parameter_hash=str(row["bound_parameter_hash"]),
            enforced_cutoff_predicate_hash=str(row["enforced_cutoff_predicate_hash"]),
            partition_key=canonicalize(dict(row["partition_key"])),
            revision_kind=SourceRevisionKind(str(row["revision_kind"])),
            revision_id=str(row["revision_id"]),
            availability_requirement=AvailabilityRequirement(
                str(row["availability_requirement"])
            ),
            business_min_date=row["business_min_date"],
            business_max_date=row["business_max_date"],
            available_at_min=row["available_at_min"],
            available_at_max=row["available_at_max"],
            schema_fingerprint=str(row["schema_fingerprint"]),
            row_count=int(row["row_count"]),
            partition_content_hash=str(row["partition_content_hash"]),
            quality_status=str(row["quality_status"]),
            reason_codes=tuple(str(item) for item in (row["reason_codes"] or [])),
            availability_event=availability_event,
            research_only=bool(row["research_only"]),
        )
    except (TypeError, ValueError) as exc:
        raise SourceLedgerError(
            REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
            "persisted source revision member is invalid",
            context={"member_key": str(row.get("member_key") or "")},
        ) from exc
    if member.member_key != str(row["member_key"]) or member.partition_key_hash != str(
        row["partition_key_hash"]
    ):
        raise SourceLedgerError(
            REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
            "persisted source revision member identity is invalid",
            context={"member_key": str(row["member_key"])},
        )
    return member


def _availability_event_from_exact_row(row: Any) -> SourceAvailabilityEvent | None:
    expected_hash = (
        str(row["availability_event_hash"]) if row["availability_event_hash"] else None
    )
    if expected_hash is None:
        if row.get("event_content_hash") is not None:
            raise SourceLedgerError(
                REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
                "source member without an event hash joined an availability event",
                context={"member_key": str(row["member_key"])},
            )
        return None
    if row.get("event_content_hash") is None:
        raise SourceLedgerError(
            REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
            "source revision member references a missing availability event",
            context={
                "member_key": str(row["member_key"]),
                "availability_event_hash": expected_hash,
            },
        )
    try:
        item = SourceAvailabilityEventInput(
            dataset_name=str(row["event_dataset_name"]),
            source_role=str(row["event_source_role"]),
            partition_key=canonicalize(dict(row["event_partition_key"])),
            partition_chain_key=str(row["event_partition_chain_key"]),
            append_request_hash=str(row["event_append_request_hash"]),
            revision_id=str(row["event_revision_id"]),
            event_revision_no=int(row["event_revision_no"]),
            event_type=SourceAvailabilityEventType(str(row["event_type"])),
            predecessor_event_hash=(
                str(row["event_predecessor_event_hash"])
                if row["event_predecessor_event_hash"]
                else None
            ),
            provider_job_id=(
                str(row["event_provider_job_id"])
                if row["event_provider_job_id"]
                else None
            ),
            refresh_job_id=(
                str(row["event_refresh_job_id"])
                if row["event_refresh_job_id"]
                else None
            ),
            provider_published_at=row["event_provider_published_at"],
            first_observed_at=row["event_first_observed_at"],
            schema_fingerprint=str(row["event_schema_fingerprint"]),
            row_count=int(row["event_row_count"]),
            partition_content_hash=str(row["event_partition_content_hash"]),
            quality_status=str(row["event_quality_status"]),
            reason_codes=tuple(
                str(value) for value in (row["event_reason_codes"] or [])
            ),
            created_by_service_principal=str(row["event_created_by_service_principal"]),
        )
        materialized = SourceAvailabilityEvent.from_input(item)
    except (TypeError, ValueError) as exc:
        raise SourceLedgerError(
            REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
            "persisted availability event is invalid",
            context={"availability_event_hash": expected_hash},
        ) from exc
    if (
        materialized.event_content_hash != expected_hash
        or materialized.event_content_hash != str(row["event_content_hash"])
        or materialized.availability_event_id != str(row["event_availability_event_id"])
    ):
        raise SourceLedgerError(
            REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
            "persisted availability event identity does not match its canonical payload",
            context={"availability_event_hash": expected_hash},
        )
    return materialized
