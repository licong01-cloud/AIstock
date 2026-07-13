from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEvent,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
)
from backend.services.advisory_phase1.source_observer import (
    AuditRowSnapshot,
    ObservationOutcome,
    REASON_AUDIT_QUALITY_NOT_ELIGIBLE,
    REASON_EVENT_CONFLICT,
    REASON_RESOURCE_LIMIT,
    SOURCE_QUERY_TEMPLATES,
    SourceObserverError,
    SourcePartitionDescriptor,
    audit_eligibility_reasons,
    build_observation_receipt,
    canonical_source_partition_descriptor,
    decide_observation,
    default_source_observer_config,
    resolve_query_template,
)


NOW = datetime(2026, 7, 14, 8, 0, tzinfo=UTC)


def _daily_audit(*, quality_status: str = "ok", row_count: int = 2) -> AuditRowSnapshot:
    return AuditRowSnapshot(
        dataset_name="daily_basic",
        trade_date=date(2026, 7, 13),
        data_source="tushare",
        job_id="job_daily_1",
        status="success",
        row_count=row_count,
        refreshed_at=NOW,
        metadata={"mode": "incremental"},
        quality_status=quality_status,
    )


def _descriptor(content: str = "a") -> SourcePartitionDescriptor:
    config = default_source_observer_config()
    spec = next(item for item in config.dataset_specs if item.dataset_name == "daily_basic")
    template = resolve_query_template(spec, SOURCE_QUERY_TEMPLATES)
    return SourcePartitionDescriptor(
        schema_fingerprint=template.schema_fingerprint,
        row_count=2,
        partition_content_hash=content * 64,
        canonical_bytes=128,
    )


def _daily_spec_and_template():
    config = default_source_observer_config()
    spec = next(item for item in config.dataset_specs if item.dataset_name == "daily_basic")
    return config, spec, resolve_query_template(spec, SOURCE_QUERY_TEMPLATES)


def test_first_eligible_observation_builds_ingested_event() -> None:
    config, spec, template = _daily_spec_and_template()
    decision = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit(),
        source_role="FEATURE_T",
        descriptor=_descriptor(),
        terminal_event=None,
    )

    assert decision.outcome is ObservationOutcome.EVENT_APPENDED
    assert decision.event_request is not None
    assert decision.event_request.event_revision_no == 1
    assert decision.event_request.event_type.value == "INGESTED"
    assert decision.event_request.quality_status == "PASS"


def test_identical_source_descriptor_is_unchanged_and_reuses_terminal() -> None:
    config, spec, template = _daily_spec_and_template()
    first = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit(),
        source_role="FEATURE_T",
        descriptor=_descriptor(),
        terminal_event=None,
    )
    assert first.event_request is not None
    terminal = SourceAvailabilityEvent.from_request(first.event_request, first_observed_at=NOW)

    unchanged = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit(),
        source_role="FEATURE_T",
        descriptor=_descriptor(),
        terminal_event=terminal,
    )

    assert unchanged.outcome is ObservationOutcome.UNCHANGED
    assert unchanged.event_request is None
    assert unchanged.terminal_event == terminal


def test_changed_source_descriptor_builds_exact_corrected_successor() -> None:
    config, spec, template = _daily_spec_and_template()
    first = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit(),
        source_role="FEATURE_T",
        descriptor=_descriptor("a"),
        terminal_event=None,
    )
    assert first.event_request is not None
    terminal = SourceAvailabilityEvent.from_request(first.event_request, first_observed_at=NOW)
    corrected = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit(),
        source_role="FEATURE_T",
        descriptor=_descriptor("b"),
        terminal_event=terminal,
    )

    assert corrected.outcome is ObservationOutcome.EVENT_APPENDED
    assert corrected.event_request is not None
    assert corrected.event_request.event_type.value == "CORRECTED"
    assert corrected.event_request.event_revision_no == 2
    assert corrected.event_request.predecessor_event_hash == terminal.event_content_hash


def test_invalidated_terminal_cannot_become_unchanged() -> None:
    config, spec, template = _daily_spec_and_template()
    first = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit(),
        source_role="FEATURE_T",
        descriptor=_descriptor(),
        terminal_event=None,
    )
    assert first.event_request is not None
    first_event = SourceAvailabilityEvent.from_request(first.event_request, first_observed_at=NOW)
    invalidated_payload = first.event_request.model_dump()
    invalidated_payload.update(
        event_revision_no=2,
        event_type=SourceAvailabilityEventType.INVALIDATED,
        predecessor_event_hash=first_event.event_content_hash,
    )
    invalidated_request = SourceAvailabilityEventRequest(**invalidated_payload)
    invalidated = SourceAvailabilityEvent.from_request(invalidated_request, first_observed_at=NOW)

    with pytest.raises(SourceObserverError, match=REASON_EVENT_CONFLICT):
        decide_observation(
            config=config,
            spec=spec,
            template=template,
            audit=_daily_audit(),
            source_role="FEATURE_T",
            descriptor=_descriptor(),
            terminal_event=invalidated,
        )


def test_invalidated_terminal_with_new_content_builds_revalidated_event() -> None:
    config, spec, template = _daily_spec_and_template()
    first = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit(),
        source_role="FEATURE_T",
        descriptor=_descriptor("a"),
        terminal_event=None,
    )
    assert first.event_request is not None
    first_event = SourceAvailabilityEvent.from_request(first.event_request, first_observed_at=NOW)
    invalidated_request = SourceAvailabilityEventRequest(
        **{
            **first.event_request.model_dump(),
            "event_revision_no": 2,
            "event_type": SourceAvailabilityEventType.INVALIDATED,
            "predecessor_event_hash": first_event.event_content_hash,
        }
    )
    invalidated = SourceAvailabilityEvent.from_request(invalidated_request, first_observed_at=NOW)
    revalidated = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit(),
        source_role="FEATURE_T",
        descriptor=_descriptor("b"),
        terminal_event=invalidated,
    )
    assert revalidated.event_request is not None
    assert revalidated.event_request.event_type is SourceAvailabilityEventType.REVALIDATED
    assert revalidated.event_request.event_revision_no == 3
    assert revalidated.event_request.predecessor_event_hash == invalidated.event_content_hash


def test_not_eligible_audit_has_reason_and_never_builds_event() -> None:
    config, spec, template = _daily_spec_and_template()
    audit = _daily_audit(quality_status="low_coverage")
    assert audit_eligibility_reasons(spec, audit) == (REASON_AUDIT_QUALITY_NOT_ELIGIBLE,)
    decision = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=audit,
        source_role="FEATURE_T",
        descriptor=None,
        terminal_event=None,
    )
    assert decision.outcome is ObservationOutcome.NOT_ELIGIBLE
    assert decision.event_request is None
    receipt = build_observation_receipt(
        config=config,
        registry=SOURCE_QUERY_TEMPLATES,
        audit=audit,
        source_role="FEATURE_T",
        decision=decision,
        observed_at=NOW,
        event=None,
    )
    assert receipt.outcome is ObservationOutcome.NOT_ELIGIBLE
    assert receipt.reason_codes == (REASON_AUDIT_QUALITY_NOT_ELIGIBLE,)


def test_canonical_partition_hashes_all_rows_and_fails_without_truncation() -> None:
    _, spec, template = _daily_spec_and_template()
    rows = []
    for code in ("000001.SZ", "000002.SZ"):
        rows.append({column.name: (date(2026, 7, 13) if column.name == "trade_date" else code if column.name == "ts_code" else None) for column in template.columns})
    descriptor = canonical_source_partition_descriptor(template=template, rows=rows, max_rows=2, max_bytes=10_000)
    assert descriptor.row_count == 2
    assert len(descriptor.partition_content_hash) == 64
    with pytest.raises(SourceObserverError, match=REASON_RESOURCE_LIMIT):
        canonical_source_partition_descriptor(template=template, rows=rows, max_rows=1, max_bytes=10_000)


def test_compiled_registry_matches_existing_market_table_type_contracts() -> None:
    adj_factor = SOURCE_QUERY_TEMPLATES["market_adj_factor_trade_date_v1"]
    index_daily = SOURCE_QUERY_TEMPLATES["market_index_daily_trade_date_v1"]
    assert next(column for column in adj_factor.columns if column.name == "adj_factor").pg_data_type == "double precision"
    assert next(column for column in index_daily.columns if column.name == "ts_code").pg_data_type == "character varying"


def test_revision_identity_binds_current_role_source_and_job_descriptor() -> None:
    config, base_spec, template = _daily_spec_and_template()
    spec = base_spec.model_copy(
        update={
            "source_roles": ("FEATURE_T", "BENCHMARK"),
            "allowed_data_sources": ("tushare", "verified_secondary"),
        }
    )
    base_audit = _daily_audit()

    def revision(*, role: str, audit: AuditRowSnapshot) -> str:
        decision = decide_observation(
            config=config,
            spec=spec,
            template=template,
            audit=audit,
            source_role=role,
            descriptor=_descriptor(),
            terminal_event=None,
        )
        assert decision.event_request is not None
        return decision.event_request.revision_id

    feature_revision = revision(role="FEATURE_T", audit=base_audit)
    assert revision(role="BENCHMARK", audit=base_audit) != feature_revision
    assert revision(
        role="FEATURE_T",
        audit=base_audit.model_copy(update={"data_source": "verified_secondary"}),
    ) != feature_revision
    assert revision(
        role="FEATURE_T",
        audit=base_audit.model_copy(update={"job_id": "job_daily_2"}),
    ) != feature_revision


def test_same_content_with_new_job_reuses_event_and_keeps_new_audit_receipt_identity() -> None:
    config, spec, template = _daily_spec_and_template()
    first = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit(),
        source_role="FEATURE_T",
        descriptor=_descriptor(),
        terminal_event=None,
    )
    assert first.event_request is not None
    terminal = SourceAvailabilityEvent.from_request(first.event_request, first_observed_at=NOW)
    unchanged = decide_observation(
        config=config,
        spec=spec,
        template=template,
        audit=_daily_audit().model_copy(update={"job_id": "job_daily_2"}),
        source_role="FEATURE_T",
        descriptor=_descriptor(),
        terminal_event=terminal,
    )
    assert unchanged.outcome is ObservationOutcome.UNCHANGED
    assert unchanged.event_request is None
    assert unchanged.terminal_event == terminal
