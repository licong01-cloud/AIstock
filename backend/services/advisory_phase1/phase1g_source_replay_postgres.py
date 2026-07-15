"""Injected fixed-SQL source-event projection for Phase 1G G2 replay."""

from __future__ import annotations

from typing import Any

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.phase1g_source_replay import (
    Phase1GSourceReplayError,
    REASON_SOURCE_REPLAY_INPUT_INVALID,
)
from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEvent,
    SourceAvailabilityEventInput,
    SourceAvailabilityEventType,
    source_partition_chain_key,
)
from backend.services.advisory_phase1.source_resolution import SourceRequirementSet


SOURCE_EVENT_CHAIN_SELECT_SQL = """
    SELECT availability_event_id, append_request_hash, dataset_name, source_role,
           partition_key, partition_key_hash, partition_chain_key, revision_id,
           event_revision_no, event_type, predecessor_event_hash, provider_job_id,
           refresh_job_id, provider_published_at, first_observed_at,
           formal_available_at, schema_fingerprint, row_count,
           partition_content_hash, quality_status, reason_codes,
           event_content_hash, created_by_service_principal
    FROM app.advisory_source_availability_event
    WHERE partition_chain_key = ANY(%s)
    ORDER BY partition_chain_key ASC, event_revision_no ASC, event_content_hash ASC
"""

PHASE1G_G2_SOURCE_SQL_REGISTRY = {
    "source_event_chains": SOURCE_EVENT_CHAIN_SELECT_SQL,
}
PHASE1G_G2_SOURCE_SQL_REGISTRY_HASH = canonical_json_sha256(
    PHASE1G_G2_SOURCE_SQL_REGISTRY
)


class Phase1GSourceReplayPostgresReader:
    """Read complete immutable source chains with a caller-owned cursor."""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    def load_events(
        self, requirement_set: SourceRequirementSet
    ) -> tuple[SourceAvailabilityEvent, ...]:
        chain_keys = sorted(
            {
                source_partition_chain_key(
                    dataset_name=requirement.dataset_name,
                    source_role=requirement.source_role,
                    partition_key=requirement.partition_key,
                )
                for requirement in requirement_set.requirements
            }
        )
        if not chain_keys:
            raise Phase1GSourceReplayError(
                REASON_SOURCE_REPLAY_INPUT_INVALID,
                "source requirement set contains no partition chains",
                context={
                    "source_requirement_set_id": requirement_set.source_requirement_set_id
                },
            )
        self._cursor.execute(SOURCE_EVENT_CHAIN_SELECT_SQL, (chain_keys,))
        rows = list(self._cursor.fetchall())
        events = tuple(_event_from_row(row) for row in rows)
        unexpected = sorted(
            {item.partition_chain_key for item in events}.difference(chain_keys)
        )
        if unexpected:
            raise Phase1GSourceReplayError(
                REASON_SOURCE_REPLAY_INPUT_INVALID,
                "source event query returned an unrelated partition chain",
                context={"unexpected_chain_count": len(unexpected)},
            )
        return events


def _event_from_row(row: Any) -> SourceAvailabilityEvent:
    try:
        item = SourceAvailabilityEventInput(
            dataset_name=str(row["dataset_name"]),
            source_role=str(row["source_role"]),
            partition_key=canonicalize(dict(row["partition_key"])),
            partition_chain_key=str(row["partition_chain_key"]),
            append_request_hash=str(row["append_request_hash"]),
            revision_id=str(row["revision_id"]),
            event_revision_no=int(row["event_revision_no"]),
            event_type=SourceAvailabilityEventType(str(row["event_type"])),
            predecessor_event_hash=(
                str(row["predecessor_event_hash"])
                if row["predecessor_event_hash"] is not None
                else None
            ),
            provider_job_id=(
                str(row["provider_job_id"])
                if row["provider_job_id"] is not None
                else None
            ),
            refresh_job_id=(
                str(row["refresh_job_id"])
                if row["refresh_job_id"] is not None
                else None
            ),
            provider_published_at=row["provider_published_at"],
            first_observed_at=row["first_observed_at"],
            schema_fingerprint=str(row["schema_fingerprint"]),
            row_count=int(row["row_count"]),
            partition_content_hash=str(row["partition_content_hash"]),
            quality_status=str(row["quality_status"]),
            reason_codes=tuple(str(item) for item in (row["reason_codes"] or [])),
            created_by_service_principal=str(row["created_by_service_principal"]),
        )
        event = SourceAvailabilityEvent.from_input(item)
    except (KeyError, TypeError, ValueError) as exc:
        raise Phase1GSourceReplayError(
            REASON_SOURCE_REPLAY_INPUT_INVALID,
            "persisted source event failed strict reconstruction",
            context={"exception_type": type(exc).__name__},
        ) from exc
    if (
        item.partition_key_hash != str(row["partition_key_hash"])
        or item.formal_available_at != row["formal_available_at"]
        or event.availability_event_id != str(row["availability_event_id"])
        or event.event_content_hash != str(row["event_content_hash"])
    ):
        raise Phase1GSourceReplayError(
            REASON_SOURCE_REPLAY_INPUT_INVALID,
            "persisted source event canonical identity does not match its columns",
            context={"partition_chain_key": item.partition_chain_key},
        )
    return event
