from __future__ import annotations

import pytest

from backend.services.advisory_phase1.phase1g_source_replay import (
    Phase1GSourceReplayError,
    parse_phase1g_source_operation,
    replay_phase1g_source_operation,
)
from backend.services.advisory_phase1.phase1g_source_replay_postgres import (
    PHASE1G_G2_SOURCE_SQL_REGISTRY,
    Phase1GSourceReplayPostgresReader,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.source_revision_postgres import (
    PostgresSourceRevisionRepository,
    REASON_PHASE1G_SOURCE_REVISION_CONFLICT,
)
from backend.tests.advisory_phase1.test_phase1g_source_replay import g2_source_case


def _event_row(event):  # type: ignore[no-untyped-def]
    item = event.input
    return {
        "availability_event_id": event.availability_event_id,
        "append_request_hash": item.append_request_hash,
        "dataset_name": item.dataset_name,
        "source_role": item.source_role,
        "partition_key": item.partition_key,
        "partition_key_hash": item.partition_key_hash,
        "partition_chain_key": item.partition_chain_key,
        "revision_id": item.revision_id,
        "event_revision_no": item.event_revision_no,
        "event_type": item.event_type.value,
        "predecessor_event_hash": item.predecessor_event_hash,
        "provider_job_id": item.provider_job_id,
        "refresh_job_id": item.refresh_job_id,
        "provider_published_at": item.provider_published_at,
        "first_observed_at": item.first_observed_at,
        "formal_available_at": item.formal_available_at,
        "schema_fingerprint": item.schema_fingerprint,
        "row_count": item.row_count,
        "partition_content_hash": item.partition_content_hash,
        "quality_status": item.quality_status,
        "reason_codes": list(item.reason_codes),
        "event_content_hash": event.event_content_hash,
        "created_by_service_principal": item.created_by_service_principal,
    }


class _RowsCursor:
    def __init__(self, rows):  # type: ignore[no-untyped-def]
        self.rows = rows
        self.executed = []

    def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
        self.executed.append((str(sql), params))

    def fetchall(self):  # type: ignore[no-untyped-def]
        return list(self.rows)


def test_source_event_reader_uses_one_fixed_select_and_reconstructs_exact_event() -> (
    None
):
    plan, target, event = g2_source_case()
    operation = parse_phase1g_source_operation(phase1e_plan=plan, target_request=target)
    cursor = _RowsCursor([_event_row(event)])
    events = Phase1GSourceReplayPostgresReader(cursor).load_events(
        operation.requirement_set
    )

    assert events == (event,)
    assert len(cursor.executed) == 1
    assert "partition_chain_key = ANY(%s)" in cursor.executed[0][0]
    assert cursor.executed[0][1][0] == [event.partition_chain_key]
    assert replay_phase1g_source_operation(
        projection=operation, availability_events=events
    ).source_revision_set


def test_source_sql_registry_is_fixed_explicit_read_only_sql() -> None:
    for sql in PHASE1G_G2_SOURCE_SQL_REGISTRY.values():
        normalized = " ".join(sql.upper().split())
        assert normalized.startswith("SELECT ")
        assert "SELECT *" not in normalized
        assert not any(
            token in normalized
            for token in (" INSERT ", " UPDATE ", " DELETE ", " MERGE ")
        )
        assert " LIMIT " not in normalized


def test_source_event_reader_rejects_canonical_column_and_chain_tamper() -> None:
    plan, target, event = g2_source_case()
    operation = parse_phase1g_source_operation(phase1e_plan=plan, target_request=target)
    bad_time = _event_row(event)
    bad_time["formal_available_at"] = event.formal_available_at.replace(year=2025)
    with pytest.raises(Phase1GSourceReplayError) as error:
        Phase1GSourceReplayPostgresReader(_RowsCursor([bad_time])).load_events(
            operation.requirement_set
        )
    assert (
        getattr(error.value, "reason_code", None)
        == "ADVISORY_PHASE1G_SOURCE_REPLAY_INPUT_INVALID"
    )

    unrelated = _event_row(event)
    unrelated["partition_chain_key"] = "f" * 64
    with pytest.raises(Phase1GSourceReplayError) as error:
        Phase1GSourceReplayPostgresReader(_RowsCursor([unrelated])).load_events(
            operation.requirement_set
        )
    assert (
        getattr(error.value, "reason_code", None)
        == "ADVISORY_PHASE1G_SOURCE_REPLAY_INPUT_INVALID"
    )


class _FreezeCursor:
    def __init__(self, header, members):  # type: ignore[no-untyped-def]
        self.header = header
        self.members = members
        self.executed = []
        self._last = ""

    def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
        self._last = str(sql)
        self.executed.append((self._last, params))

    def fetchone(self):  # type: ignore[no-untyped-def]
        if "INSERT INTO app.advisory_source_revision_set" in self._last:
            return {"source_revision_set_id": self.header["source_revision_set_id"]}
        if "FROM app.advisory_source_revision_set" in self._last:
            return self.header
        return None

    def fetchall(self):  # type: ignore[no-untyped-def]
        return self.members


def _member_row(source_set):  # type: ignore[no-untyped-def]
    member = source_set.members[0]
    event = member.availability_event
    assert event is not None
    row = {
        "member_key": member.member_key,
        **member.content_payload(),
        "partition_key_hash": member.partition_key_hash,
        "reason_codes": list(member.reason_codes),
    }
    item = event.input
    row.update(
        {
            "event_availability_event_id": event.availability_event_id,
            "event_append_request_hash": item.append_request_hash,
            "event_dataset_name": item.dataset_name,
            "event_source_role": item.source_role,
            "event_partition_key": item.partition_key,
            "event_partition_key_hash": item.partition_key_hash,
            "event_partition_chain_key": item.partition_chain_key,
            "event_revision_id": item.revision_id,
            "event_revision_no": item.event_revision_no,
            "event_type": item.event_type.value,
            "event_predecessor_event_hash": item.predecessor_event_hash,
            "event_provider_job_id": item.provider_job_id,
            "event_refresh_job_id": item.refresh_job_id,
            "event_provider_published_at": item.provider_published_at,
            "event_first_observed_at": item.first_observed_at,
            "event_schema_fingerprint": item.schema_fingerprint,
            "event_row_count": item.row_count,
            "event_partition_content_hash": item.partition_content_hash,
            "event_quality_status": item.quality_status,
            "event_reason_codes": list(item.reason_codes),
            "event_content_hash": event.event_content_hash,
            "event_created_by_service_principal": item.created_by_service_principal,
        }
    )
    return row


def test_source_revision_transaction_primitives_use_only_caller_cursor_and_exact_readback() -> (
    None
):
    plan, target, event = g2_source_case()
    operation = parse_phase1g_source_operation(phase1e_plan=plan, target_request=target)
    source_set = replay_phase1g_source_operation(
        projection=operation,
        availability_events=(event,),
    ).source_revision_set
    header = {
        "source_revision_set_id": source_set.source_revision_set_id,
        "source_revision_set_hash": source_set.source_revision_set_hash,
        "query_registry_hash": source_set.query_registry_hash,
        "schema_version": source_set.schema_version,
        "requested_source_cutoff": source_set.requested_source_cutoff,
        "label_as_of_ts": source_set.label_as_of_ts,
        "research_only": source_set.research_only,
        "member_count": len(source_set.members),
    }
    cursor = _FreezeCursor(header, [_member_row(source_set)])
    repository = PostgresSourceRevisionRepository(conn_factory=lambda: (_ for _ in ()))

    assert repository.freeze_in_transaction(cursor, source_set) == source_set
    assert (
        repository.read_exact_in_transaction(
            cursor, source_set.source_revision_set_hash
        )
        == source_set
    )
    statements = " ".join(sql.upper() for sql, _ in cursor.executed)
    assert "COMMIT" not in statements
    assert "ROLLBACK" not in statements
    assert "FOR KEY SHARE" in statements


def test_source_revision_exact_readback_rejects_conflicting_header() -> None:
    plan, target, event = g2_source_case()
    operation = parse_phase1g_source_operation(phase1e_plan=plan, target_request=target)
    source_set = replay_phase1g_source_operation(
        projection=operation,
        availability_events=(event,),
    ).source_revision_set
    header = {
        "source_revision_set_id": source_set.source_revision_set_id,
        "source_revision_set_hash": source_set.source_revision_set_hash,
        "query_registry_hash": "f" * 64,
        "schema_version": source_set.schema_version,
        "requested_source_cutoff": source_set.requested_source_cutoff,
        "label_as_of_ts": source_set.label_as_of_ts,
        "research_only": source_set.research_only,
        "member_count": len(source_set.members),
    }
    cursor = _FreezeCursor(header, [_member_row(source_set)])
    with pytest.raises(SourceLedgerError) as error:
        PostgresSourceRevisionRepository.read_exact_in_transaction(
            cursor, source_set.source_revision_set_hash
        )
    assert error.value.reason_code == REASON_PHASE1G_SOURCE_REVISION_CONFLICT
