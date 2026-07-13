"""Exact source-revision set contracts."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.services.advisory_phase1.source_ledger import (
    InMemorySourceAvailabilityLedger,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
    SourceLedgerError,
)
from backend.services.advisory_phase1.source_revision import (
    AvailabilityRequirement,
    SourceRevisionKind,
    SourceRevisionMemberInput,
    build_source_revision_set,
)
from backend.services.advisory_phase1.source_revision_postgres import (
    SOURCE_REVISION_MEMBER_INSERT_SQL,
    _matches_member_rows,
    _member_params,
    _member_payload,
)


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def _event():
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    return ledger.append(
        SourceAvailabilityEventRequest(
            dataset_name="market.kline_daily_raw",
            source_role="FEATURE_T",
            partition_key={"trade_date": "2026-06-30"},
            revision_id="r1",
            event_revision_no=1,
            event_type=SourceAvailabilityEventType.INGESTED,
            schema_fingerprint="schema-v1",
            row_count=9,
            partition_content_hash=HASH_A,
            quality_status="PASS",
            created_by_service_principal="test-observer",
        )
    )


def _member(
    *,
    event=None,
    include_event: bool = True,
    revision_kind: SourceRevisionKind = SourceRevisionKind.IMMUTABLE_INGESTION,
    requirement: AvailabilityRequirement = AvailabilityRequirement.DECISION_CUTOFF,
):
    event = event or _event()
    source = event.input
    return SourceRevisionMemberInput(
        source_role=source.source_role,
        dataset_name=source.dataset_name,
        query_template_id="market-kline-v1",
        query_template_version="1",
        query_template_hash=HASH_B,
        bound_parameter_hash=HASH_C,
        enforced_cutoff_predicate_hash="d" * 64,
        partition_key=source.partition_key,
        revision_kind=revision_kind,
        revision_id=source.revision_id,
        availability_requirement=requirement,
        business_min_date=date(2026, 6, 30),
        business_max_date=date(2026, 6, 30),
        available_at_min=source.formal_available_at,
        available_at_max=source.formal_available_at,
        schema_fingerprint=source.schema_fingerprint,
        row_count=source.row_count,
        partition_content_hash=source.partition_content_hash,
        quality_status=source.quality_status,
        availability_event=event if include_event else None,
        research_only=True,
    )


def test_revision_set_is_deterministic_and_pins_exact_event() -> None:
    event = _event()
    member = _member(event=event)
    revision_set = build_source_revision_set(
        query_registry_hash=HASH_C,
        requested_source_cutoff=OBSERVED_AT,
        label_as_of_ts=OBSERVED_AT,
        research_only=True,
        members=[member],
    )

    assert revision_set.members == (member,)
    assert revision_set.members[0].content_payload()["availability_event_hash"] == event.event_content_hash
    assert revision_set.source_revision_set_id == f"srs_{revision_set.source_revision_set_hash[:20]}"


def test_decision_cutoff_member_cannot_drop_or_mismatch_event_evidence() -> None:
    event = _event()
    member = _member(event=event)
    without_event = member.model_dump()
    without_event["availability_event"] = None
    with pytest.raises(ValidationError, match="decision-cutoff research member requires"):
        SourceRevisionMemberInput.model_validate(without_event)
    mismatched = member.model_dump()
    mismatched["partition_content_hash"] = HASH_B
    with pytest.raises(ValidationError, match="member fields must exactly match"):
        SourceRevisionMemberInput.model_validate(mismatched)


def test_watermark_only_is_research_only_and_not_decision_evidence() -> None:
    event = _event()
    with pytest.raises(ValidationError, match="decision-cutoff research member requires"):
        _member(event=event, include_event=False, revision_kind=SourceRevisionKind.WATERMARK_ONLY)


def test_non_research_member_is_rejected() -> None:
    member = _member().model_dump()
    member["research_only"] = False
    with pytest.raises(ValidationError, match="must remain research-only"):
        SourceRevisionMemberInput.model_validate(member)


def test_formal_member_cannot_reference_invalidated_event() -> None:
    observed_times = iter([OBSERVED_AT, OBSERVED_AT.replace(day=2)])
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: next(observed_times))
    first_request = SourceAvailabilityEventRequest.model_validate(
        _event().input.model_dump(exclude={"partition_chain_key", "append_request_hash", "first_observed_at"})
    )
    first = ledger.append(first_request)
    invalidated = ledger.append(
        SourceAvailabilityEventRequest(
            **{
                **first_request.model_dump(),
                "revision_id": "r2",
                "event_revision_no": 2,
                "event_type": SourceAvailabilityEventType.INVALIDATED,
                "predecessor_event_hash": first.event_content_hash,
            }
        )
    )
    with pytest.raises(ValidationError, match="cannot reference an INVALIDATED"):
        _member(event=invalidated)


def test_decision_member_cannot_use_event_observed_after_cutoff() -> None:
    event = _event()
    with pytest.raises(SourceLedgerError, match="ADVISORY_PHASE1_SOURCE_REVISION_MEMBER_INVALID"):
        build_source_revision_set(
            query_registry_hash=HASH_C,
            requested_source_cutoff=OBSERVED_AT.replace(hour=9),
            label_as_of_ts=OBSERVED_AT,
            research_only=True,
            members=[_member(event=event)],
        )


def test_exact_retry_compares_member_content_not_only_count() -> None:
    member = _member()
    revision_set = build_source_revision_set(
        query_registry_hash=HASH_C,
        requested_source_cutoff=OBSERVED_AT,
        label_as_of_ts=OBSERVED_AT,
        research_only=True,
        members=[member],
    )
    persisted = _member_payload(member)
    assert _matches_member_rows([persisted], revision_set)
    persisted["partition_content_hash"] = HASH_B
    assert not _matches_member_rows([persisted], revision_set)
    assert len(_member_params(revision_set.source_revision_set_id, member)) == 25
    assert SOURCE_REVISION_MEMBER_INSERT_SQL.count("%s") == 25


def test_v2_member_requires_frozen_cutoff_predicate_and_migration_preserves_v1() -> None:
    member = _member().model_dump()
    member.pop("enforced_cutoff_predicate_hash")
    with pytest.raises(ValidationError, match="enforced_cutoff_predicate_hash"):
        SourceRevisionMemberInput.model_validate(member)

    migration = (
        Path(__file__).parents[2]
        / "db"
        / "migrations"
        / "add_advisory_phase1c2_source_revision_cutoff_20260713.sql"
    ).read_text(encoding="utf-8")
    rollback = (
        Path(__file__).parents[2]
        / "db"
        / "migrations"
        / "add_advisory_phase1c2_source_revision_cutoff_20260713.rollback.sql"
    ).read_text(encoding="utf-8")
    assert "schema_version" in migration
    assert "enforced_cutoff_predicate_hash" in migration
    assert "advisory_phase1_source_revision_set_v2" in migration
    assert "NEW.enforced_cutoff_predicate_hash IS NULL" in migration
    assert "UPDATE app.advisory_source_revision" not in migration
    assert "ROLLBACK_REQUIRES_NO_V2_SOURCE_REVISION_EVIDENCE" in rollback


def test_frozen_cutoff_predicate_changes_source_revision_set_hash() -> None:
    member = _member()
    divergent_payload = member.model_dump()
    divergent_payload["enforced_cutoff_predicate_hash"] = "e" * 64
    divergent_member = SourceRevisionMemberInput.model_validate(divergent_payload)

    first = build_source_revision_set(
        query_registry_hash=HASH_C,
        requested_source_cutoff=OBSERVED_AT,
        label_as_of_ts=OBSERVED_AT,
        research_only=True,
        members=[member],
    )
    second = build_source_revision_set(
        query_registry_hash=HASH_C,
        requested_source_cutoff=OBSERVED_AT,
        label_as_of_ts=OBSERVED_AT,
        research_only=True,
        members=[divergent_member],
    )

    assert first.schema_version == "advisory_phase1_source_revision_set_v2"
    assert first.source_revision_set_hash != second.source_revision_set_hash
