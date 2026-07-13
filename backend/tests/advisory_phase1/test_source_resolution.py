"""Phase 1C-2 fixture source requirement and readiness contracts."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.source_ledger import (
    InMemorySourceAvailabilityLedger,
    SourceAvailabilityEvent,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
    SourceLedgerError,
)
from backend.services.advisory_phase1.source_resolution import (
    REASON_SOURCE_CHAIN_INVALID,
    REASON_SOURCE_QUALITY_INVALID,
    REASON_SOURCE_REPLAY_NOT_ELIGIBLE,
    REASON_SOURCE_REQUIREMENT_SET_CONFLICT,
    REASON_SOURCE_RESOLUTION_RECEIPT_CONFLICT,
    REASON_SOURCE_TERMINAL_INVALIDATED,
    REASON_SOURCE_UNAVAILABLE_AS_OF,
    FixtureSourceRevisionResolver,
    InMemorySourceRequirementSetRepository,
    InMemorySourceResolutionReceiptRepository,
    ResearchReadiness,
    SourceRequirement,
    SourceRequirementSet,
    build_source_requirement_common_pit_identity_hash,
)
from backend.services.advisory_phase1.source_revision import AvailabilityRequirement, SourceRevisionKind


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
CUTOFF = OBSERVED_AT + timedelta(minutes=1)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


def _common_pit_hash(*, alpha_mode: str = "single_alpha") -> str:
    return build_source_requirement_common_pit_identity_hash(
        admission_scope_id="scope-fixture",
        admission_scope_hash=HASH_C,
        handoff_readiness_hash=HASH_D,
        program_id="program-fixture",
        binding_version_id="binding-fixture",
        package_id="package-fixture",
        manifest_sha256=HASH_E,
        alpha_mode=alpha_mode,
        decision_as_of_trade_date=date(2026, 6, 30),
        requested_source_cutoff=CUTOFF,
        query_registry_hash=HASH_F,
        calendar_hash=HASH_A,
        universe_policy_hash=HASH_B,
        data_source="DB_HISTORICAL",
        execution_origin="MANUAL_HISTORICAL_RESEARCH",
        research_scope="HISTORICAL_RESEARCH_ONLY",
        execution_prohibited=True,
        research_only=True,
    )


def _requirement(
    *,
    consumer_scope_id: str,
    source_role: str = "FEATURE_T",
    partition_key: dict[str, str] | None = None,
    lookback: int = 20,
    business_min_date: date = date(2026, 6, 1),
    availability_requirement: AvailabilityRequirement = AvailabilityRequirement.DECISION_CUTOFF,
    requested_cutoff: datetime = CUTOFF,
    common_pit_identity_hash: str | None = None,
) -> SourceRequirement:
    parameters = {"lookback": lookback, "source_role": source_role}
    return SourceRequirement(
        consumer_scope_id=consumer_scope_id,
        source_role=source_role,
        dataset_name="market.kline_daily_raw",
        query_template_id="fixture-kline-v1",
        query_template_version="1",
        query_template_hash=HASH_A,
        bound_parameters=parameters,
        bound_parameter_hash=canonical_json_sha256(parameters),
        partition_key=partition_key or {"trade_date": "2026-06-30"},
        revision_kind=SourceRevisionKind.IMMUTABLE_INGESTION,
        availability_requirement=availability_requirement,
        business_min_date=business_min_date,
        business_max_date=date(2026, 6, 30),
        requested_cutoff=requested_cutoff,
        enforced_cutoff_predicate_hash=HASH_B,
        common_pit_identity_hash=common_pit_identity_hash or _common_pit_hash(),
    )


def _requirement_set(
    *requirements: SourceRequirement,
    alpha_mode: str = "single_alpha",
    formal_oos_status: str = "RETROSPECTIVE_RESEARCH_ONLY",
    evidence_scope: str = "RETROSPECTIVE_RESEARCH_ONLY",
    replay_eligible: bool = False,
    label_as_of_ts: datetime = CUTOFF,
) -> SourceRequirementSet:
    return SourceRequirementSet(
        admission_scope_id="scope-fixture",
        admission_scope_hash=HASH_C,
        handoff_readiness_hash=HASH_D,
        program_id="program-fixture",
        binding_version_id="binding-fixture",
        package_id="package-fixture",
        manifest_sha256=HASH_E,
        alpha_mode=alpha_mode,
        decision_as_of_trade_date=date(2026, 6, 30),
        requested_source_cutoff=CUTOFF,
        label_as_of_ts=label_as_of_ts,
        query_registry_hash=HASH_F,
        calendar_hash=HASH_A,
        universe_policy_hash=HASH_B,
        formal_oos_status=formal_oos_status,
        evidence_scope=evidence_scope,
        research_replay_eligible=replay_eligible,
        requirements=tuple(requirements),
    )


def _event(
    ledger: InMemorySourceAvailabilityLedger,
    *,
    source_role: str = "FEATURE_T",
    partition_key: dict[str, str] | None = None,
    revision_no: int = 1,
    event_type: SourceAvailabilityEventType = SourceAvailabilityEventType.INGESTED,
    predecessor_event_hash: str | None = None,
    revision_id: str | None = None,
    content_hash: str = HASH_A,
    quality_status: str = "PASS",
):
    return ledger.append(
        SourceAvailabilityEventRequest(
            dataset_name="market.kline_daily_raw",
            source_role=source_role,
            partition_key=partition_key or {"trade_date": "2026-06-30"},
            revision_id=revision_id or f"r{revision_no}",
            event_revision_no=revision_no,
            event_type=event_type,
            predecessor_event_hash=predecessor_event_hash,
            schema_fingerprint="fixture-schema-v1",
            row_count=100,
            partition_content_hash=content_hash,
            quality_status=quality_status,
            created_by_service_principal="fixture-observer",
        )
    )


def test_single_alpha_complete_source_is_research_ready_and_capture_eligible() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    event = _event(ledger)
    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(_requirement(consumer_scope_id="single-alpha")),
        availability_events=(event,),
    )

    assert result.receipt.readiness is ResearchReadiness.RESEARCH_READY
    assert result.receipt.resolved_requirement_count == 1
    assert result.receipt.can_create_capture_plan is True
    assert result.can_create_capture_plan is True
    assert result.source_revision_set is not None
    assert result.source_revision_set.members[0].availability_event == event
    assert result.source_revision_set.members[0].enforced_cutoff_predicate_hash == HASH_B


def test_native_multi_alpha_accepts_different_lookbacks_without_cross_leg_equality() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    event = _event(ledger)
    common_pit_identity_hash = _common_pit_hash(alpha_mode="multi_alpha")
    fast = _requirement(
        consumer_scope_id="component-fast",
        lookback=5,
        business_min_date=date(2026, 6, 23),
        common_pit_identity_hash=common_pit_identity_hash,
    )
    slow = _requirement(
        consumer_scope_id="component-slow",
        lookback=60,
        business_min_date=date(2026, 4, 1),
        common_pit_identity_hash=common_pit_identity_hash,
    )

    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(fast, slow, alpha_mode="multi_alpha"),
        availability_events=(event,),
    )

    assert result.receipt.readiness is ResearchReadiness.RESEARCH_READY
    assert {item.consumer_scope_id for item in result.receipt.requirement_resolutions} == {"component-fast", "component-slow"}
    assert {item.selected_source_member_key for item in result.receipt.requirement_resolutions} == {
        item.member_key for item in result.source_revision_set.members
    }
    assert result.source_revision_set is not None
    assert len(result.source_revision_set.members) == 2


def test_multi_alpha_shared_physical_source_keeps_two_requirement_mappings_and_one_member() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    event = _event(ledger)
    common_pit_identity_hash = _common_pit_hash(alpha_mode="multi_alpha")
    first = _requirement(
        consumer_scope_id="component-one",
        lookback=20,
        common_pit_identity_hash=common_pit_identity_hash,
    )
    second = _requirement(
        consumer_scope_id="component-two",
        lookback=20,
        common_pit_identity_hash=common_pit_identity_hash,
    )

    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(first, second, alpha_mode="multi_alpha"),
        availability_events=(event,),
    )

    assert result.receipt.readiness is ResearchReadiness.RESEARCH_READY
    assert len(result.receipt.requirement_resolutions) == 2
    assert len({item.selected_source_member_key for item in result.receipt.requirement_resolutions}) == 1
    assert result.source_revision_set is not None
    assert len(result.source_revision_set.members) == 1


def test_multi_alpha_divergent_common_pit_identity_is_blocked_before_resolution() -> None:
    common_pit_identity_hash = _common_pit_hash(alpha_mode="multi_alpha")
    first = _requirement(consumer_scope_id="component-one", common_pit_identity_hash=common_pit_identity_hash)
    divergent_payload = _requirement(
        consumer_scope_id="component-two",
        common_pit_identity_hash=common_pit_identity_hash,
    ).model_dump()
    divergent_payload.pop("requirement_id")
    divergent_payload.pop("requirement_hash")
    divergent_payload["common_pit_identity_hash"] = "0" * 64
    divergent = SourceRequirement.model_validate(divergent_payload)

    with pytest.raises(ValidationError, match="ADVISORY_PHASE1_SOURCE_REQUIREMENT_CONFLICT"):
        _requirement_set(first, divergent, alpha_mode="multi_alpha")


def test_partial_source_keeps_available_member_and_enumerates_missing_requirement() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    feature = _event(ledger)
    common_pit_identity_hash = _common_pit_hash(alpha_mode="multi_alpha")
    feature_requirement = _requirement(
        consumer_scope_id="feature",
        source_role="FEATURE_T",
        common_pit_identity_hash=common_pit_identity_hash,
    )
    universe_requirement = _requirement(
        consumer_scope_id="universe",
        source_role="UNIVERSE_T",
        common_pit_identity_hash=common_pit_identity_hash,
    )

    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(feature_requirement, universe_requirement, alpha_mode="multi_alpha"),
        availability_events=(feature,),
    )

    assert result.receipt.readiness is ResearchReadiness.PARTIAL
    assert result.receipt.resolved_requirement_count == 1
    assert result.receipt.unavailable_requirement_count == 1
    assert result.receipt.can_create_capture_plan is True
    assert result.source_revision_set is not None
    assert len(result.source_revision_set.members) == 1


def test_all_missing_source_is_partial_gap_only_without_placeholder_revision_set() -> None:
    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(_requirement(consumer_scope_id="single-alpha")),
        availability_events=(),
    )

    assert result.receipt.readiness is ResearchReadiness.PARTIAL
    assert result.receipt.unavailable_requirement_count == 1
    assert result.receipt.source_revision_set_id is None
    assert result.can_create_capture_plan is False


def test_invalidated_and_failed_quality_terminal_are_partial_without_predecessor_fallback() -> None:
    observed_times = iter([OBSERVED_AT, OBSERVED_AT + timedelta(minutes=1)])
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: next(observed_times))
    first = _event(ledger)
    invalidated = _event(
        ledger,
        revision_no=2,
        event_type=SourceAvailabilityEventType.INVALIDATED,
        predecessor_event_hash=first.event_content_hash,
        revision_id="r2",
        content_hash=HASH_A,
    )
    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(_requirement(consumer_scope_id="single-alpha")),
        availability_events=(first, invalidated),
    )
    assert result.receipt.readiness is ResearchReadiness.PARTIAL
    assert result.receipt.reason_codes == (REASON_SOURCE_TERMINAL_INVALIDATED,)
    assert result.source_revision_set is None

    quality_times = iter([OBSERVED_AT, OBSERVED_AT + timedelta(minutes=1)])
    quality_ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: next(quality_times))
    quality_first = _event(quality_ledger)
    failed = _event(
        quality_ledger,
        revision_no=2,
        event_type=SourceAvailabilityEventType.CORRECTED,
        predecessor_event_hash=quality_first.event_content_hash,
        revision_id="r2",
        content_hash=HASH_B,
        quality_status="FAILED",
    )
    quality_result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(_requirement(consumer_scope_id="single-alpha")),
        availability_events=(quality_first, failed),
    )
    assert quality_result.receipt.readiness is ResearchReadiness.PARTIAL
    assert quality_result.receipt.reason_codes == (REASON_SOURCE_QUALITY_INVALID,)
    assert quality_result.source_revision_set is None


def test_chain_conflict_blocks_scope_and_does_not_emit_source_set() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    first = _event(ledger)
    malformed = first.model_copy(update={"event_content_hash": "0" * 64})
    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(_requirement(consumer_scope_id="single-alpha")),
        availability_events=(first, malformed),
    )

    assert result.receipt.readiness is ResearchReadiness.BLOCKED
    assert result.receipt.reason_codes == (REASON_SOURCE_CHAIN_INVALID,)
    assert result.source_revision_set is None
    assert result.can_create_capture_plan is False


def test_source_chain_with_reverse_availability_time_is_blocked() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT + timedelta(days=1))
    first = _event(ledger)
    corrected = SourceAvailabilityEvent.from_request(
        SourceAvailabilityEventRequest(
            dataset_name="market.kline_daily_raw",
            source_role="FEATURE_T",
            partition_key={"trade_date": "2026-06-30"},
            revision_id="r2",
            event_revision_no=2,
            event_type=SourceAvailabilityEventType.CORRECTED,
            predecessor_event_hash=first.event_content_hash,
            schema_fingerprint="fixture-schema-v1",
            row_count=100,
            partition_content_hash=HASH_B,
            quality_status="PASS",
            created_by_service_principal="phase1c2-test",
        ),
        first_observed_at=OBSERVED_AT,
    )
    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(_requirement(consumer_scope_id="single-alpha")),
        availability_events=(first, corrected),
    )

    assert result.receipt.readiness is ResearchReadiness.BLOCKED
    assert result.receipt.reason_codes == (REASON_SOURCE_CHAIN_INVALID,)


def test_future_correction_does_not_pollute_as_of_terminal() -> None:
    observed_times = iter([OBSERVED_AT, OBSERVED_AT + timedelta(days=1)])
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: next(observed_times))
    first = _event(ledger)
    corrected = _event(
        ledger,
        revision_no=2,
        event_type=SourceAvailabilityEventType.CORRECTED,
        predecessor_event_hash=first.event_content_hash,
        revision_id="r2",
        content_hash=HASH_B,
    )
    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(_requirement(consumer_scope_id="single-alpha")),
        availability_events=(first, corrected),
    )

    assert result.receipt.readiness is ResearchReadiness.RESEARCH_READY
    assert result.source_revision_set is not None
    assert result.source_revision_set.members[0].availability_event == first


def test_label_as_of_requirement_uses_label_clock_not_decision_cutoff() -> None:
    label_as_of_ts = OBSERVED_AT + timedelta(days=1)
    observed_times = iter([OBSERVED_AT, label_as_of_ts])
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: next(observed_times))
    first = _event(ledger)
    corrected = _event(
        ledger,
        revision_no=2,
        event_type=SourceAvailabilityEventType.CORRECTED,
        predecessor_event_hash=first.event_content_hash,
        revision_id="r2",
        content_hash=HASH_B,
    )
    requirement = _requirement(
        consumer_scope_id="label-source",
        availability_requirement=AvailabilityRequirement.LABEL_AS_OF,
        requested_cutoff=label_as_of_ts,
    )
    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(requirement, label_as_of_ts=label_as_of_ts),
        availability_events=(first, corrected),
    )

    assert result.receipt.readiness is ResearchReadiness.RESEARCH_READY
    assert result.source_revision_set is not None
    assert result.source_revision_set.members[0].availability_event == corrected


def test_non_replayable_none_scope_is_blocked_with_explicit_reason_not_a_fake_success() -> None:
    result = FixtureSourceRevisionResolver().resolve(
        requirement_set=_requirement_set(
            _requirement(consumer_scope_id="single-alpha"),
            formal_oos_status="NONE",
            evidence_scope="GAP_ONLY",
            replay_eligible=False,
        ),
        availability_events=(),
    )

    assert result.receipt.readiness is ResearchReadiness.BLOCKED
    assert result.receipt.reason_codes == (REASON_SOURCE_REPLAY_NOT_ELIGIBLE, REASON_SOURCE_UNAVAILABLE_AS_OF)


def test_same_explicit_requirement_identity_with_different_payload_is_rejected() -> None:
    requirement = _requirement(consumer_scope_id="single-alpha")
    conflicting = requirement.model_dump()
    conflicting["business_min_date"] = date(2026, 5, 1)
    with pytest.raises(ValidationError, match="requirement_hash does not match"):
        SourceRequirement.model_validate(conflicting)


def test_requirement_set_repository_is_idempotent_and_rejects_identity_conflict() -> None:
    repository = InMemorySourceRequirementSetRepository()
    original = _requirement_set(_requirement(consumer_scope_id="single-alpha"))
    divergent = _requirement_set(_requirement(consumer_scope_id="another-alpha"))

    assert repository.save(original) is original
    assert repository.save(original) is original
    assert repository.get(str(original.source_requirement_set_id)) is original

    conflicting = divergent.model_copy(update={"source_requirement_set_id": original.source_requirement_set_id})
    with pytest.raises(SourceLedgerError, match=REASON_SOURCE_REQUIREMENT_SET_CONFLICT):
        repository.save(conflicting)


def test_resolution_receipt_repository_is_idempotent_and_rejects_identity_conflict() -> None:
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: OBSERVED_AT)
    event = _event(ledger)
    resolver = FixtureSourceRevisionResolver()
    original = resolver.resolve(
        requirement_set=_requirement_set(_requirement(consumer_scope_id="single-alpha")),
        availability_events=(event,),
    ).receipt
    divergent = resolver.resolve(
        requirement_set=_requirement_set(_requirement(consumer_scope_id="another-alpha")),
        availability_events=(event,),
    ).receipt
    repository = InMemorySourceResolutionReceiptRepository()

    assert repository.save(original) is original
    assert repository.save(original) is original
    assert repository.get(str(original.source_resolution_receipt_id)) is original

    conflicting = divergent.model_copy(update={"source_resolution_receipt_id": original.source_resolution_receipt_id})
    with pytest.raises(SourceLedgerError, match=REASON_SOURCE_RESOLUTION_RECEIPT_CONFLICT):
        repository.save(conflicting)
