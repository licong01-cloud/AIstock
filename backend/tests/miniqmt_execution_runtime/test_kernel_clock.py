from __future__ import annotations

from datetime import UTC, date, datetime, time
import json

import pytest

from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    MarketCode,
    SessionSegment,
    canonical_json_bytes,
)
from backend.services.miniqmt_execution_runtime.kernel_clock import (
    CatchUpPolicyV1,
    ExchangeSessionClockV1,
    KernelClockError,
    add_exchange_active_seconds_v1,
    build_eod_event_v1,
    build_session_event_v1,
    build_timer_event_v1,
    effective_timer_due_at_v1,
    project_exchange_session_v1,
    session_epoch_v1,
    session_event_id_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    EventSourceV2,
    EventTypeV2,
    ExchangeSessionAuthorityV1,
    ExecutionAlgoTimerOccurrenceStatusV1,
    ExecutionAlgoTimerOccurrenceV1,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    RuntimeEventIngressReceiptV1,
    SessionPhaseV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    kernel_lease_fence_token_v1,
)


def _authority(
    segments: tuple[SessionSegment, ...] | None = None,
) -> ExchangeSessionAuthorityV1:
    segments = segments or (SessionSegment(time(9, 30), time(11, 30)), SessionSegment(time(13), time(15)))
    effective_at = datetime(2026, 7, 26, 16, tzinfo=UTC)
    snapshots = {
        market: CalendarSnapshot(
            calendar_id=f"calendar_{market.value}_20260727",
            market=market,
            trade_date=date(2026, 7, 27),
            timezone="Asia/Shanghai",
            session_segments=segments,
            effective_at_utc=effective_at,
            source_version="aistock_calendar_v1",
        )
        for market in MarketCode
    }
    snapshot_set = CalendarSnapshotSet(snapshot_set_id="calendar_set_k2c", snapshot_by_market=snapshots)
    snapshot_json = json.loads(canonical_json_bytes(snapshot_set.canonical_payload()).decode("utf-8"))
    snapshot_json["set_sha256"] = snapshot_set.set_sha256
    ordered = (MarketCode.SH, MarketCode.SZ, MarketCode.BJ)
    return ExchangeSessionAuthorityV1.create(
        runtime_id="runtime_k2c",
        exchange_trade_date="2026-07-27",
        calendar_snapshot_set_id=snapshot_set.snapshot_set_id,
        calendar_snapshot_set_json=snapshot_json,
        calendar_snapshot_set_sha256=snapshot_set.set_sha256,
        ordered_market_calendar_sha256s=tuple(
            snapshot_set.snapshot_by_market[market].calendar_sha256 for market in ordered
        ),
        ordered_session_segments=tuple(segment.canonical_payload() for segment in segments),
        source_effective_at_utc=effective_at,
    )


def _schedule(
    *, policy: CatchUpPolicyV1 = CatchUpPolicyV1.APPLY_ONCE, timer_name: str = "twap_slice"
) -> ExecutionAlgoTimerScheduleV1:
    mutation = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id="mqalgo_k2c",
        transition_id="mqtransition_k2c",
        ordinal=0,
        timer_name=timer_name,
        schedule_epoch=session_epoch_v1(_authority()),
        due_at_exchange_utc="2026-07-27T03:45:00Z",
        catch_up_policy=policy.value,
        payload={"slice": 2},
    )
    return ExecutionAlgoTimerScheduleV1.create(
        runtime_id="runtime_k2c",
        mutation=mutation,
        status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
        emitted_event_id=None,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at_utc=None,
        row_version=1,
        created_at_utc="2026-07-27T01:30:00Z",
        updated_at_utc="2026-07-27T01:30:00Z",
        closed_at_utc=None,
    )


def _claimed_pair(
    schedule: ExecutionAlgoTimerScheduleV1,
) -> tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1]:
    owner = "clock_worker:clock_incarnation"
    mutation = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=schedule.algo_instance_id,
        transition_id="mqtransition_k2c_claim",
        ordinal=0,
        timer_name=schedule.timer_name,
        schedule_epoch=schedule.schedule_epoch,
        due_at_exchange_utc=schedule.due_at_exchange_utc,
        catch_up_policy=schedule.catch_up_policy,
        payload=thaw_json_v1(schedule.payload),
    )
    schedule = ExecutionAlgoTimerScheduleV1.create(
        runtime_id=schedule.runtime_id,
        mutation=mutation,
        status=ExecutionAlgoTimerScheduleStatusV1.EMITTING,
        emitted_event_id=None,
        lease_owner=owner,
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="TIMER_SCHEDULE",
            owner_id=schedule.schedule_id,
            lease_epoch=1,
            lease_owner=owner,
        ),
        lease_expires_at_utc="2026-07-27T05:01:00Z",
        row_version=2,
        created_at_utc=schedule.created_at_utc,
        updated_at_utc="2026-07-27T05:00:00Z",
        closed_at_utc=None,
    )
    occurrence = ExecutionAlgoTimerOccurrenceV1.create(
        schedule=schedule,
        exchange_session_authority_sha256=_authority().authority_sha256,
        status=ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED,
        emitted_event_id=None,
        catch_up_receipt_sha256=None,
        lease_owner=owner,
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="TIMER_OCCURRENCE",
            owner_id=schedule.timer_occurrence_id,
            lease_epoch=1,
            lease_owner=owner,
        ),
        lease_expires_at_utc="2026-07-27T05:01:00Z",
        row_version=1,
        created_at_utc="2026-07-27T05:00:00Z",
        closed_at_utc=None,
    )
    return schedule, occurrence


@pytest.mark.parametrize(
    ("observed", "phase", "active_seconds"),
    [
        ("2026-07-27T01:20:00Z", SessionPhaseV1.CLOSED, 0),
        ("2026-07-27T02:30:00Z", SessionPhaseV1.CONTINUOUS_AM, 3600),
        ("2026-07-27T04:00:00Z", SessionPhaseV1.LUNCH_BREAK, 7200),
        ("2026-07-27T06:00:00Z", SessionPhaseV1.CONTINUOUS_PM, 10800),
        ("2026-07-27T07:10:00Z", SessionPhaseV1.CLOSED, 14400),
    ],
)
def test_exchange_session_projection_uses_only_durable_segments(
    observed: str, phase: SessionPhaseV1, active_seconds: int
) -> None:
    projection = project_exchange_session_v1(_authority(), observed)
    assert projection.session_phase is phase
    assert projection.exchange_active_seconds == active_seconds


def test_lunch_due_is_shifted_to_pm_without_counting_natural_seconds() -> None:
    authority = _authority()
    assert effective_timer_due_at_v1(authority, "2026-07-27T03:45:00Z") == "2026-07-27T05:00:00.000000Z"
    assert add_exchange_active_seconds_v1(authority, "2026-07-27T03:15:00Z", 3600) == ("2026-07-27T05:45:00.000000Z")


def test_session_and_eod_identities_are_deterministic_and_authority_bound() -> None:
    authority = _authority()
    epoch = session_epoch_v1(authority)
    event_id = session_event_id_v1(
        authority,
        session_event_type="SESSION_LUNCH_END",
        phase_boundary_at_utc="2026-07-27T05:00:00Z",
    )
    session_event = build_session_event_v1(
        authority=authority,
        sequence=2,
        session_event_type="SESSION_LUNCH_END",
        session_phase=SessionPhaseV1.CONTINUOUS_PM,
        phase_boundary_at_utc="2026-07-27T05:00:00Z",
    )
    eod = build_eod_event_v1(authority=authority, sequence=3, phase_boundary_at_utc="2026-07-27T07:00:00Z")
    assert thaw_json_v1(session_event.source_identity) == {"session_event_id": event_id}
    assert thaw_json_v1(eod.source_identity) == {
        "runtime_id": authority.runtime_id,
        "trade_date": authority.exchange_trade_date,
        "session_epoch": epoch,
    }


def test_timer_event_has_no_quote_and_is_bound_to_occurrence_and_algo() -> None:
    schedule, occurrence = _claimed_pair(_schedule())
    event = build_timer_event_v1(
        authority=_authority(),
        schedule=schedule,
        occurrence=occurrence,
        sequence=7,
        monotonic_ns=123,
    )
    assert event.event_type is EventTypeV2.TIMER
    assert event.source is EventSourceV2.EXCHANGE_SESSION_CLOCK
    assert thaw_json_v1(event.source_identity) == {"timer_occurrence_id": occurrence.timer_occurrence_id}
    assert thaw_json_v1(event.correlation)["algo_instance_id"] == schedule.algo_instance_id
    assert not ({"quote", "market_data_id", "price"} & set(thaw_json_v1(event.payload)))


def test_stale_timer_claim_reuses_occurrence_identity_and_advances_both_fences() -> None:
    previous_schedule, previous_occurrence = _claimed_pair(_schedule())
    owner = "clock_worker:successor_incarnation"
    mutation = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=previous_schedule.algo_instance_id,
        transition_id="mqtransition_k2c_reclaim",
        ordinal=0,
        timer_name=previous_schedule.timer_name,
        schedule_epoch=previous_schedule.schedule_epoch,
        due_at_exchange_utc=previous_schedule.due_at_exchange_utc,
        catch_up_policy=previous_schedule.catch_up_policy,
        payload=thaw_json_v1(previous_schedule.payload),
    )
    reclaimed_schedule = ExecutionAlgoTimerScheduleV1.create(
        runtime_id=previous_schedule.runtime_id,
        mutation=mutation,
        status=ExecutionAlgoTimerScheduleStatusV1.EMITTING,
        emitted_event_id=None,
        lease_owner=owner,
        lease_epoch=2,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="TIMER_SCHEDULE",
            owner_id=previous_schedule.schedule_id,
            lease_epoch=2,
            lease_owner=owner,
        ),
        lease_expires_at_utc="2026-07-27T05:03:00Z",
        row_version=3,
        created_at_utc=previous_schedule.created_at_utc,
        updated_at_utc="2026-07-27T05:02:00Z",
        closed_at_utc=None,
    )
    reclaimed_occurrence = ExecutionAlgoTimerOccurrenceV1.create(
        schedule=reclaimed_schedule,
        exchange_session_authority_sha256=previous_occurrence.exchange_session_authority_sha256,
        status=ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED,
        emitted_event_id=None,
        catch_up_receipt_sha256=None,
        lease_owner=owner,
        lease_epoch=2,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="TIMER_OCCURRENCE",
            owner_id=previous_occurrence.timer_occurrence_id,
            lease_epoch=2,
            lease_owner=owner,
        ),
        lease_expires_at_utc="2026-07-27T05:03:00Z",
        row_version=2,
        created_at_utc=previous_occurrence.created_at_utc,
        closed_at_utc=None,
    )
    assert reclaimed_schedule.validate_successor_v1(previous_schedule) == reclaimed_schedule
    assert reclaimed_occurrence.validate_successor_v1(previous_occurrence) == reclaimed_occurrence
    assert reclaimed_occurrence.timer_occurrence_id == previous_occurrence.timer_occurrence_id


def test_clock_rejects_non_authoritative_session_shape_and_unknown_catch_up_policy() -> None:
    authority = _authority()
    malformed = authority.model_copy(update={"ordered_session_segments": authority.ordered_session_segments[:1]})
    with pytest.raises(KernelClockError, match="strict readback"):
        project_exchange_session_v1(malformed, "2026-07-27T02:00:00Z")
    legacy_mutation = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id="mqalgo_k2c",
        transition_id="mqtransition_k2c_legacy",
        ordinal=0,
        timer_name="twap_slice",
        schedule_epoch=session_epoch_v1(authority),
        due_at_exchange_utc="2026-07-27T03:45:00Z",
        catch_up_policy="LEGACY_BURST",
        payload={"slice": 2},
    )
    legacy_schedule = ExecutionAlgoTimerScheduleV1.create(
        runtime_id="runtime_k2c",
        mutation=legacy_mutation,
        status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
        emitted_event_id=None,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at_utc=None,
        row_version=1,
        created_at_utc="2026-07-27T01:30:00Z",
        updated_at_utc="2026-07-27T01:30:00Z",
        closed_at_utc=None,
    )
    legacy_schedule, legacy_occurrence = _claimed_pair(legacy_schedule)
    with pytest.raises(KernelClockError, match="catch-up policy"):
        build_timer_event_v1(
            authority=authority,
            schedule=legacy_schedule,
            occurrence=legacy_occurrence,
            sequence=1,
            monotonic_ns=1,
        )


def test_clock_fail_loud_matrix_covers_invalid_authority_time_policy_and_boundary() -> None:
    with pytest.raises(KernelClockError, match="strict durable"):
        project_exchange_session_v1(None, "2026-07-27T02:00:00Z")  # type: ignore[arg-type]
    three_segments = (
        SessionSegment(time(9, 15), time(9, 25)),
        SessionSegment(time(9, 30), time(11, 30)),
        SessionSegment(time(13), time(15)),
    )
    with pytest.raises(KernelClockError, match="exact AM/PM"):
        project_exchange_session_v1(_authority(three_segments), "2026-07-27T02:00:00Z")
    with pytest.raises(KernelClockError, match="does not belong"):
        project_exchange_session_v1(_authority(), "2026-07-28T02:00:00Z")
    with pytest.raises(KernelClockError, match="outside the durable exchange session"):
        effective_timer_due_at_v1(_authority(), "2026-07-27T01:00:00Z")
    with pytest.raises(KernelClockError, match="non-negative strict integer"):
        add_exchange_active_seconds_v1(_authority(), "2026-07-27T02:00:00Z", -1)
    with pytest.raises(KernelClockError, match="beyond the durable session close"):
        add_exchange_active_seconds_v1(_authority(), "2026-07-27T06:59:59Z", 2)
    with pytest.raises(KernelClockError, match="unsupported exchange-session event type"):
        session_event_id_v1(
            _authority(),
            session_event_type="SESSION_UNKNOWN",
            phase_boundary_at_utc="2026-07-27T05:00:00Z",
        )
    with pytest.raises(KernelClockError, match="type and phase"):
        build_session_event_v1(
            authority=_authority(),
            sequence=1,
            session_event_type="SESSION_LUNCH_END",
            session_phase=SessionPhaseV1.LUNCH_BREAK,
            phase_boundary_at_utc="2026-07-27T05:00:00Z",
        )
    skipped_schedule, skipped_occurrence = _claimed_pair(
        _schedule(policy=CatchUpPolicyV1.SKIP_WITH_RECEIPT, timer_name="skip_no_event")
    )
    with pytest.raises(KernelClockError, match="only APPLY_ONCE"):
        build_timer_event_v1(
            authority=_authority(),
            schedule=skipped_schedule,
            occurrence=skipped_occurrence,
            sequence=1,
            monotonic_ns=1,
        )


class _ClockRepository:
    def __init__(
        self,
        *,
        authority: ExchangeSessionAuthorityV1,
        claim_batches: list[tuple[tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1], ...]],
        fail_first_finalize: bool = False,
    ) -> None:
        self.authority = authority
        self.claim_batches = claim_batches
        self.fail_first_finalize = fail_first_finalize
        self.last_sequence = 1
        self.events: dict[str, dict[str, object]] = {}
        self.ingested_types: list[EventTypeV2] = []
        self.finalized: list[tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1]] = []
        self.finalize_attempts = 0
        self.claim_calls: list[dict[str, object]] = []

    def read_exchange_session_authority(self, *, runtime_id: str, exchange_trade_date: date):
        assert runtime_id == self.authority.runtime_id
        assert exchange_trade_date.isoformat() == self.authority.exchange_trade_date
        return self.authority

    def read_runtime_last_event_sequence(self, runtime_id: str) -> int:
        assert runtime_id == self.authority.runtime_id
        return self.last_sequence

    def read_event_transaction(self, event_id: str):
        try:
            return self.events[event_id]
        except KeyError:
            raise KeyError(event_id) from None

    def ingest_routed_event_atomic(
        self, *, event, catalog_runtime, correlated_algo_instance_ids, callback_mapping_update=None
    ):
        del catalog_runtime, correlated_algo_instance_ids, callback_mapping_update
        assert event.sequence == self.last_sequence + 1
        receipt = RuntimeEventIngressReceiptV1.create(
            runtime_id=event.runtime_id,
            event_id=event.event_id,
            event_key_sha256=event.event_key_sha256,
            runtime_sequence=event.sequence,
            ordered_target_algo_instance_ids=(),
            ordered_delivery_ids=(),
            transaction_commit_identity=f"mqtx_clock_{event.sequence}",
        )
        self.last_sequence = event.sequence
        self.events[event.event_id] = {"event": event, "receipt": receipt, "deliveries": ()}
        self.ingested_types.append(event.event_type)
        return receipt

    def claim_due_timer_schedules_atomic(self, **kwargs):
        self.claim_calls.append(kwargs)
        return self.claim_batches.pop(0) if self.claim_batches else ()

    def finalize_timer_claim_atomic(self, *, schedule, occurrence):
        self.finalize_attempts += 1
        if self.fail_first_finalize and self.finalize_attempts == 1:
            from backend.services.miniqmt_execution_runtime.kernel_repository import KernelRepositoryConflict

            raise KernelRepositoryConflict("injected crash after event commit")
        self.finalized.append((schedule, occurrence))
        return schedule, occurrence


def test_clock_restart_after_event_commit_reuses_event_and_finishes_claim_once() -> None:
    claimed = _claimed_pair(_schedule())
    repository = _ClockRepository(
        authority=_authority(),
        claim_batches=[(claimed,), (claimed,)],
        fail_first_finalize=True,
    )
    clock = ExchangeSessionClockV1(
        repository=repository,
        catalog_runtime=object(),  # type: ignore[arg-type]
        lease_owner="clock_worker:clock_incarnation",
    )
    with pytest.raises(RuntimeError, match="injected crash"):
        clock.wake(
            runtime_id="runtime_k2c",
            exchange_trade_date=date(2026, 7, 27),
            observed_at_utc="2026-07-27T05:00:00Z",
            monotonic_ns=10,
            lease_expires_at_utc="2026-07-27T05:01:00Z",
        )
    receipt = clock.wake(
        runtime_id="runtime_k2c",
        exchange_trade_date=date(2026, 7, 27),
        observed_at_utc="2026-07-27T05:00:00Z",
        monotonic_ns=20,
        lease_expires_at_utc="2026-07-27T05:01:00Z",
    )
    assert repository.ingested_types.count(EventTypeV2.TIMER) == 1
    assert len(repository.finalized) == 1
    assert receipt.processed_timer_count == 1


def test_clock_catch_up_policies_are_terminal_and_never_emit_timer_or_burst() -> None:
    skipped = _claimed_pair(_schedule(policy=CatchUpPolicyV1.SKIP_WITH_RECEIPT, timer_name="skip_slice"))
    expired = _claimed_pair(_schedule(policy=CatchUpPolicyV1.TERMINAL_EXPIRED, timer_name="expire_slice"))
    repository = _ClockRepository(authority=_authority(), claim_batches=[(skipped, expired)])
    receipt = ExchangeSessionClockV1(
        repository=repository,
        catalog_runtime=object(),  # type: ignore[arg-type]
        lease_owner="clock_worker:clock_incarnation",
    ).wake(
        runtime_id="runtime_k2c",
        exchange_trade_date=date(2026, 7, 27),
        observed_at_utc="2026-07-27T05:00:00Z",
        monotonic_ns=10,
        lease_expires_at_utc="2026-07-27T05:01:00Z",
    )
    assert EventTypeV2.TIMER not in repository.ingested_types
    assert [item[1].status for item in repository.finalized] == [
        ExecutionAlgoTimerOccurrenceStatusV1.SKIPPED,
        ExecutionAlgoTimerOccurrenceStatusV1.EXPIRED,
    ]
    assert all(item[1].catch_up_receipt_sha256 is not None for item in repository.finalized)
    assert receipt.processed_timer_count == 2


def test_clock_does_not_claim_before_open_or_lunch_boundary_and_resumes_in_pm() -> None:
    repository = _ClockRepository(authority=_authority(), claim_batches=[])
    clock = ExchangeSessionClockV1(
        repository=repository,
        catalog_runtime=object(),  # type: ignore[arg-type]
        lease_owner="clock_worker:clock_incarnation",
    )
    clock.wake(
        runtime_id="runtime_k2c",
        exchange_trade_date=date(2026, 7, 27),
        observed_at_utc="2026-07-27T01:20:00Z",
        monotonic_ns=1,
        lease_expires_at_utc="2026-07-27T01:21:00Z",
    )
    assert repository.claim_calls == []
    clock.wake(
        runtime_id="runtime_k2c",
        exchange_trade_date=date(2026, 7, 27),
        observed_at_utc="2026-07-27T04:00:00Z",
        monotonic_ns=2,
        lease_expires_at_utc="2026-07-27T04:01:00Z",
    )
    assert repository.claim_calls[-1]["due_cutoff_at_utc"] == "2026-07-27T03:29:59.999999Z"
    clock.wake(
        runtime_id="runtime_k2c",
        exchange_trade_date=date(2026, 7, 27),
        observed_at_utc="2026-07-27T05:00:00Z",
        monotonic_ns=3,
        lease_expires_at_utc="2026-07-27T05:01:00Z",
    )
    assert repository.claim_calls[-1]["due_cutoff_at_utc"] == "2026-07-27T05:00:00.000000Z"


def test_clock_wake_rejects_unbounded_page_inputs_before_repository_access() -> None:
    repository = _ClockRepository(authority=_authority(), claim_batches=[])
    clock = ExchangeSessionClockV1(
        repository=repository,
        catalog_runtime=object(),  # type: ignore[arg-type]
        lease_owner="clock_worker:clock_incarnation",
    )
    common = {
        "runtime_id": "runtime_k2c",
        "exchange_trade_date": date(2026, 7, 27),
        "observed_at_utc": "2026-07-27T05:00:00Z",
        "monotonic_ns": 1,
        "lease_expires_at_utc": "2026-07-27T05:01:00Z",
    }
    with pytest.raises(ValueError, match="timer_page_limit"):
        clock.wake(**common, timer_page_limit=201)
    with pytest.raises(ValueError, match="max_timer_pages"):
        clock.wake(**common, max_timer_pages=0)
    assert repository.claim_calls == []
