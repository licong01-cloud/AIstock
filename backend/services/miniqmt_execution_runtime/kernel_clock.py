"""Deterministic exchange-session and one-shot timer semantics for K2-C.

This module is shadow-only until the later runtime migration.  It consumes the
durable exchange-session authority and never reads a local calendar, quote
cache, broker, or wall-clock singleton.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

from .plugin_canonical import (
    canonical_utc_datetime_v1,
    hash_hex_v1,
    json_safe_evidence_v1,
    thaw_json_v1,
)
from .plugin_contracts import (
    EventSourceV2,
    EventTypeV2,
    ExchangeSessionAuthorityV1,
    ExecutionAlgoTimerOccurrenceStatusV1,
    ExecutionAlgoTimerOccurrenceV1,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
    SessionPhaseV1,
    TimerMutationTypeV1,
    TimerMutationV1,
)
from .plugin_registry import PluginCatalogRuntimeV2


_EXCHANGE_TZ = ZoneInfo("Asia/Shanghai")
_SESSION_EVENT_PHASES = {
    "SESSION_OPEN": SessionPhaseV1.CONTINUOUS_AM,
    "SESSION_LUNCH_START": SessionPhaseV1.LUNCH_BREAK,
    "SESSION_LUNCH_END": SessionPhaseV1.CONTINUOUS_PM,
    "SESSION_CLOSE": SessionPhaseV1.CLOSED,
}


class CatchUpPolicyV1(StrEnum):
    """The only K2 timer recovery policies; none authorizes burst replay."""

    APPLY_ONCE = "APPLY_ONCE"
    SKIP_WITH_RECEIPT = "SKIP_WITH_RECEIPT"
    TERMINAL_EXPIRED = "TERMINAL_EXPIRED"


class KernelClockError(RuntimeError):
    """Typed fail-loud clock failure with bounded, JSON-safe evidence."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = json_safe_evidence_v1(context)
        super().__init__(message)


@dataclass(frozen=True)
class ExchangeSessionProjectionV1:
    runtime_id: str
    exchange_trade_date: str
    session_epoch: str
    session_phase: SessionPhaseV1
    observed_at_utc: str
    exchange_active_seconds: int
    am_start_utc: str
    am_end_utc: str
    pm_start_utc: str
    pm_end_utc: str


@dataclass(frozen=True)
class KernelClockWakeReceiptV1:
    runtime_id: str
    exchange_trade_date: str
    authority_sha256: str
    observed_at_utc: str
    ordered_session_event_ids: tuple[str, ...]
    ordered_timer_event_ids: tuple[str, ...]
    ordered_terminal_occurrence_ids: tuple[str, ...]
    eod_event_id: str | None
    processed_timer_count: int
    timer_page_count: int
    more_due_timers: bool


class KernelClockRepositoryV1(Protocol):
    def read_exchange_session_authority(
        self, *, runtime_id: str, exchange_trade_date: date
    ) -> ExchangeSessionAuthorityV1: ...

    def read_runtime_last_event_sequence(self, runtime_id: str) -> int: ...

    def read_event_transaction(self, event_id: str) -> dict[str, Any]: ...

    def ingest_routed_event_atomic(
        self,
        *,
        event: RuntimeEventEnvelopeV2,
        catalog_runtime: PluginCatalogRuntimeV2,
        correlated_algo_instance_ids: tuple[str, ...],
        callback_mapping_update: None = None,
    ) -> RuntimeEventIngressReceiptV1: ...

    def claim_due_timer_schedules_atomic(
        self,
        *,
        runtime_id: str,
        exchange_trade_date: date,
        exchange_session_authority_sha256: str,
        due_cutoff_at_utc: Any,
        observed_at_utc: Any,
        lease_owner: str,
        lease_expires_at_utc: Any,
        eligible_algo_statuses: tuple[str, ...],
        limit: int,
    ) -> tuple[tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1], ...]: ...

    def finalize_timer_claim_atomic(
        self,
        *,
        schedule: ExecutionAlgoTimerScheduleV1,
        occurrence: ExecutionAlgoTimerOccurrenceV1,
    ) -> tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1]: ...


def _strict_authority(authority: Any) -> ExchangeSessionAuthorityV1:
    if not isinstance(authority, ExchangeSessionAuthorityV1):
        raise KernelClockError(
            "MINIQMT_EXCHANGE_SESSION_AUTHORITY_INVALID",
            "clock requires a strict durable ExchangeSessionAuthorityV1",
            context={"actual_type": type(authority).__name__},
        )
    try:
        strict = ExchangeSessionAuthorityV1.model_validate_json(authority.model_dump_json())
    except (TypeError, ValueError) as exc:
        raise KernelClockError(
            "MINIQMT_EXCHANGE_SESSION_AUTHORITY_INVALID",
            "exchange-session authority strict readback failed",
            context={"exception": exc, "runtime_id": getattr(authority, "runtime_id", None)},
        ) from exc
    segments = tuple(thaw_json_v1(item) for item in strict.ordered_session_segments)
    if len(segments) != 2:
        raise KernelClockError(
            "MINIQMT_EXCHANGE_SESSION_SEGMENTS_INVALID",
            "ExchangeSessionClock requires the exact AM/PM continuous segment pair",
            context={"runtime_id": strict.runtime_id, "segment_count": len(segments)},
        )
    normalized: list[tuple[time, time]] = []
    for ordinal, segment in enumerate(segments):
        if not isinstance(segment, dict) or set(segment) != {"start_local", "end_local"}:
            raise KernelClockError(
                "MINIQMT_EXCHANGE_SESSION_SEGMENTS_INVALID",
                "session segment does not use exact start_local/end_local fields",
                context={"runtime_id": strict.runtime_id, "ordinal": ordinal, "segment": segment},
            )
        try:
            start = time.fromisoformat(segment["start_local"])
            end = time.fromisoformat(segment["end_local"])
        except (TypeError, ValueError) as exc:
            raise KernelClockError(
                "MINIQMT_EXCHANGE_SESSION_SEGMENTS_INVALID",
                "session segment contains an invalid local time",
                context={"runtime_id": strict.runtime_id, "ordinal": ordinal, "exception": exc},
            ) from exc
        if start.tzinfo is not None or end.tzinfo is not None or start >= end:
            raise KernelClockError(
                "MINIQMT_EXCHANGE_SESSION_SEGMENTS_INVALID",
                "session segment must contain ordered timezone-free local times",
                context={"runtime_id": strict.runtime_id, "ordinal": ordinal, "segment": segment},
            )
        normalized.append((start, end))
    if normalized[0][1] >= normalized[1][0]:
        raise KernelClockError(
            "MINIQMT_EXCHANGE_SESSION_SEGMENTS_INVALID",
            "AM and PM continuous segments must have one positive lunch interval",
            context={"runtime_id": strict.runtime_id, "segments": segments},
        )
    return strict


def _parse_utc(value: Any, *, field_name: str) -> datetime:
    canonical = canonical_utc_datetime_v1(value, field_name=field_name)
    return datetime.fromisoformat(canonical.replace("Z", "+00:00"))


def _session_intervals(authority: ExchangeSessionAuthorityV1) -> tuple[tuple[datetime, datetime], ...]:
    strict = _strict_authority(authority)
    trade_date = date.fromisoformat(strict.exchange_trade_date)
    intervals: list[tuple[datetime, datetime]] = []
    for segment in strict.ordered_session_segments:
        payload = thaw_json_v1(segment)
        start_local = datetime.combine(trade_date, time.fromisoformat(payload["start_local"]), tzinfo=_EXCHANGE_TZ)
        end_local = datetime.combine(trade_date, time.fromisoformat(payload["end_local"]), tzinfo=_EXCHANGE_TZ)
        intervals.append((start_local.astimezone(UTC), end_local.astimezone(UTC)))
    return tuple(intervals)


def session_epoch_v1(authority: ExchangeSessionAuthorityV1) -> str:
    strict = _strict_authority(authority)
    return "mqsessionepoch_" + hash_hex_v1(
        "miniqmt_session_epoch_v1",
        {
            "runtime_id": strict.runtime_id,
            "exchange_trade_date": strict.exchange_trade_date,
            "exchange_session_authority_sha256": strict.authority_sha256,
        },
    )


def session_event_id_v1(
    authority: ExchangeSessionAuthorityV1,
    *,
    session_event_type: str,
    phase_boundary_at_utc: Any,
) -> str:
    strict = _strict_authority(authority)
    phase = _SESSION_EVENT_PHASES.get(session_event_type)
    if phase is None:
        raise KernelClockError(
            "MINIQMT_SESSION_EVENT_TYPE_INVALID",
            "unsupported exchange-session event type",
            context={"session_event_type": session_event_type},
        )
    boundary = canonical_utc_datetime_v1(phase_boundary_at_utc, field_name="phase_boundary_at_utc")
    return "mqsessionevt_" + hash_hex_v1(
        "miniqmt_session_event_identity_v1",
        {
            "runtime_id": strict.runtime_id,
            "session_epoch": session_epoch_v1(strict),
            "session_phase": phase.value,
            "phase_boundary_at_utc": boundary,
        },
    )


def project_exchange_session_v1(
    authority: ExchangeSessionAuthorityV1, observed_at_utc: Any
) -> ExchangeSessionProjectionV1:
    strict = _strict_authority(authority)
    observed = _parse_utc(observed_at_utc, field_name="observed_at_utc")
    if observed.astimezone(_EXCHANGE_TZ).date().isoformat() != strict.exchange_trade_date:
        raise KernelClockError(
            "MINIQMT_EXCHANGE_SESSION_OBSERVED_DATE_CONFLICT",
            "observed clock time does not belong to the durable exchange trade date",
            context={
                "runtime_id": strict.runtime_id,
                "exchange_trade_date": strict.exchange_trade_date,
                "observed_at_utc": canonical_utc_datetime_v1(observed),
            },
        )
    (am_start, am_end), (pm_start, pm_end) = _session_intervals(strict)
    if am_start <= observed < am_end:
        phase = SessionPhaseV1.CONTINUOUS_AM
    elif am_end <= observed < pm_start:
        phase = SessionPhaseV1.LUNCH_BREAK
    elif pm_start <= observed < pm_end:
        phase = SessionPhaseV1.CONTINUOUS_PM
    else:
        phase = SessionPhaseV1.CLOSED
    active = max(0, int((min(observed, am_end) - am_start).total_seconds()))
    active = min(active, int((am_end - am_start).total_seconds()))
    if observed > pm_start:
        active += max(0, int((min(observed, pm_end) - pm_start).total_seconds()))
    return ExchangeSessionProjectionV1(
        runtime_id=strict.runtime_id,
        exchange_trade_date=strict.exchange_trade_date,
        session_epoch=session_epoch_v1(strict),
        session_phase=phase,
        observed_at_utc=canonical_utc_datetime_v1(observed),
        exchange_active_seconds=active,
        am_start_utc=canonical_utc_datetime_v1(am_start),
        am_end_utc=canonical_utc_datetime_v1(am_end),
        pm_start_utc=canonical_utc_datetime_v1(pm_start),
        pm_end_utc=canonical_utc_datetime_v1(pm_end),
    )


def effective_timer_due_at_v1(authority: ExchangeSessionAuthorityV1, due_at_exchange_utc: Any) -> str:
    strict = _strict_authority(authority)
    due = _parse_utc(due_at_exchange_utc, field_name="due_at_exchange_utc")
    (am_start, am_end), (pm_start, pm_end) = _session_intervals(strict)
    if am_end <= due < pm_start:
        due = pm_start
    if due < am_start or due > pm_end:
        raise KernelClockError(
            "MINIQMT_TIMER_DUE_OUTSIDE_EXCHANGE_SESSION",
            "timer due is outside the durable exchange session",
            context={"runtime_id": strict.runtime_id, "due_at_exchange_utc": canonical_utc_datetime_v1(due)},
        )
    return canonical_utc_datetime_v1(due)


def add_exchange_active_seconds_v1(authority: ExchangeSessionAuthorityV1, start_at_utc: Any, seconds: int) -> str:
    strict = _strict_authority(authority)
    if type(seconds) is not int or seconds < 0:
        raise KernelClockError(
            "MINIQMT_EXCHANGE_ACTIVE_SECONDS_INVALID",
            "exchange-active seconds must be a non-negative strict integer",
            context={"seconds": seconds},
        )
    cursor = _parse_utc(start_at_utc, field_name="start_at_utc")
    remaining = seconds
    for segment_start, segment_end in _session_intervals(strict):
        if cursor >= segment_end:
            continue
        cursor = max(cursor, segment_start)
        available = max(0, int((segment_end - cursor).total_seconds()))
        if remaining <= available:
            return canonical_utc_datetime_v1(cursor + timedelta(seconds=remaining))
        remaining -= available
        cursor = segment_end
    raise KernelClockError(
        "MINIQMT_EXCHANGE_ACTIVE_DEADLINE_AFTER_CLOSE",
        "exchange-active duration extends beyond the durable session close",
        context={
            "runtime_id": strict.runtime_id,
            "start_at_utc": canonical_utc_datetime_v1(start_at_utc),
            "seconds": seconds,
            "remaining_seconds": remaining,
        },
    )


def build_session_event_v1(
    *,
    authority: ExchangeSessionAuthorityV1,
    sequence: int,
    session_event_type: str,
    session_phase: SessionPhaseV1,
    phase_boundary_at_utc: Any,
) -> RuntimeEventEnvelopeV2:
    strict = _strict_authority(authority)
    expected_phase = _SESSION_EVENT_PHASES.get(session_event_type)
    if expected_phase is None or session_phase is not expected_phase:
        raise KernelClockError(
            "MINIQMT_SESSION_EVENT_PHASE_CONFLICT",
            "session event type and phase do not match the code-owned boundary map",
            context={"session_event_type": session_event_type, "session_phase": getattr(session_phase, "value", None)},
        )
    boundary = canonical_utc_datetime_v1(phase_boundary_at_utc, field_name="phase_boundary_at_utc")
    session_event_id = session_event_id_v1(
        strict,
        session_event_type=session_event_type,
        phase_boundary_at_utc=boundary,
    )
    return RuntimeEventEnvelopeV2.create(
        runtime_id=strict.runtime_id,
        sequence=sequence,
        event_type=EventTypeV2.SESSION,
        event_time_utc=boundary,
        monotonic_ns=None,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        symbol=None,
        payload_schema_version="miniqmt_session_event_v1",
        payload={
            "session_event_id": session_event_id,
            "session_event_type": session_event_type,
            "session_phase": session_phase.value,
            "runtime_id": strict.runtime_id,
            "trade_date": strict.exchange_trade_date,
            "session_epoch": session_epoch_v1(strict),
            "phase_boundary_at_utc": boundary,
            "exchange_session_authority_sha256": strict.authority_sha256,
        },
        source_identity={"session_event_id": session_event_id},
        correlation={},
    )


def build_eod_event_v1(
    *, authority: ExchangeSessionAuthorityV1, sequence: int, phase_boundary_at_utc: Any
) -> RuntimeEventEnvelopeV2:
    strict = _strict_authority(authority)
    boundary = canonical_utc_datetime_v1(phase_boundary_at_utc, field_name="phase_boundary_at_utc")
    epoch = session_epoch_v1(strict)
    return RuntimeEventEnvelopeV2.create(
        runtime_id=strict.runtime_id,
        sequence=sequence,
        event_type=EventTypeV2.EOD,
        event_time_utc=boundary,
        monotonic_ns=None,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        symbol=None,
        payload_schema_version="miniqmt_eod_event_v1",
        payload={
            "runtime_id": strict.runtime_id,
            "trade_date": strict.exchange_trade_date,
            "session_epoch": epoch,
            "session_phase": SessionPhaseV1.CLOSED.value,
            "phase_boundary_at_utc": boundary,
            "terminal_outcome": "EXPIRED_WITH_RESIDUAL",
            "exchange_session_authority_sha256": strict.authority_sha256,
        },
        source_identity={
            "runtime_id": strict.runtime_id,
            "trade_date": strict.exchange_trade_date,
            "session_epoch": epoch,
        },
        correlation={},
    )


def build_timer_event_v1(
    *,
    authority: ExchangeSessionAuthorityV1,
    schedule: ExecutionAlgoTimerScheduleV1,
    occurrence: ExecutionAlgoTimerOccurrenceV1,
    sequence: int,
    monotonic_ns: int,
) -> RuntimeEventEnvelopeV2:
    strict = _strict_authority(authority)
    if not isinstance(schedule, ExecutionAlgoTimerScheduleV1) or not isinstance(
        occurrence, ExecutionAlgoTimerOccurrenceV1
    ):
        raise KernelClockError(
            "MINIQMT_TIMER_CARRIER_INVALID",
            "timer event requires strict schedule and occurrence carriers",
            context={"schedule_type": type(schedule).__name__, "occurrence_type": type(occurrence).__name__},
        )
    try:
        schedule = ExecutionAlgoTimerScheduleV1.model_validate_json(schedule.model_dump_json())
        occurrence = ExecutionAlgoTimerOccurrenceV1.model_validate_json(occurrence.model_dump_json())
        policy = CatchUpPolicyV1(schedule.catch_up_policy)
    except (TypeError, ValueError) as exc:
        raise KernelClockError(
            "MINIQMT_TIMER_CARRIER_INVALID",
            "timer schedule/occurrence or catch-up policy failed strict readback",
            context={"schedule_id": getattr(schedule, "schedule_id", None), "exception": exc},
        ) from exc
    if policy is not CatchUpPolicyV1.APPLY_ONCE:
        raise KernelClockError(
            "MINIQMT_TIMER_POLICY_DOES_NOT_EMIT",
            "only APPLY_ONCE produces a TIMER event; other policies require a terminal receipt",
            context={"schedule_id": schedule.schedule_id, "catch_up_policy": policy.value},
        )
    if (
        schedule.runtime_id != strict.runtime_id
        or occurrence.runtime_id != strict.runtime_id
        or occurrence.schedule_id != schedule.schedule_id
        or occurrence.algo_instance_id != schedule.algo_instance_id
        or occurrence.exchange_session_authority_sha256 != strict.authority_sha256
        or occurrence.status is not ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED
        or schedule.status is not ExecutionAlgoTimerScheduleStatusV1.EMITTING
        or occurrence.lease_owner != schedule.lease_owner
        or occurrence.lease_epoch != schedule.lease_epoch
    ):
        raise KernelClockError(
            "MINIQMT_TIMER_CARRIER_CLOSURE_CONFLICT",
            "timer schedule, occurrence, lease and session authority do not form one exact claim",
            context={
                "runtime_id": strict.runtime_id,
                "schedule_id": schedule.schedule_id,
                "timer_occurrence_id": occurrence.timer_occurrence_id,
            },
        )
    payload = {
        "timer_occurrence_id": occurrence.timer_occurrence_id,
        "schedule_id": schedule.schedule_id,
        "algo_instance_id": schedule.algo_instance_id,
        "timer_name": schedule.timer_name,
        "schedule_epoch": schedule.schedule_epoch,
        "due_at_exchange_utc": schedule.due_at_exchange_utc,
        "effective_due_at_exchange_utc": effective_timer_due_at_v1(strict, schedule.due_at_exchange_utc),
        "catch_up_policy": policy.value,
        "timer_payload": thaw_json_v1(schedule.payload),
        "timer_payload_sha256": schedule.payload_sha256,
        "exchange_session_authority_sha256": strict.authority_sha256,
    }
    return RuntimeEventEnvelopeV2.create(
        runtime_id=strict.runtime_id,
        sequence=sequence,
        event_type=EventTypeV2.TIMER,
        event_time_utc=occurrence.created_at_utc,
        monotonic_ns=monotonic_ns,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        symbol=None,
        payload_schema_version="miniqmt_timer_due_v1",
        payload=payload,
        source_identity={"timer_occurrence_id": occurrence.timer_occurrence_id},
        correlation={"algo_instance_id": schedule.algo_instance_id, "schedule_id": schedule.schedule_id},
    )


def _terminal_timer_pair_v1(
    *,
    authority: ExchangeSessionAuthorityV1,
    schedule: ExecutionAlgoTimerScheduleV1,
    occurrence: ExecutionAlgoTimerOccurrenceV1,
    closed_at_utc: Any,
    emitted_event_id: str | None,
    occurrence_status: ExecutionAlgoTimerOccurrenceStatusV1,
    catch_up_reason: str | None,
) -> tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1]:
    closed = canonical_utc_datetime_v1(closed_at_utc, field_name="closed_at_utc")
    if schedule.status is not ExecutionAlgoTimerScheduleStatusV1.EMITTING or (
        occurrence.status is not ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED
    ):
        raise KernelClockError(
            "MINIQMT_TIMER_FINALIZATION_PREDECESSOR_INVALID",
            "timer finalization requires one active EMITTING/CLAIMED pair",
            context={"schedule_id": schedule.schedule_id, "timer_occurrence_id": occurrence.timer_occurrence_id},
        )
    if occurrence_status is ExecutionAlgoTimerOccurrenceStatusV1.EVENT_COMMITTED:
        if emitted_event_id is None or catch_up_reason is not None:
            raise KernelClockError(
                "MINIQMT_TIMER_FINALIZATION_OUTCOME_INVALID",
                "EVENT_COMMITTED requires an exact event identity and no catch-up terminal reason",
                context={"schedule_id": schedule.schedule_id},
            )
        schedule_status = ExecutionAlgoTimerScheduleStatusV1.EMITTED
        catch_up_receipt = None
    elif occurrence_status in {
        ExecutionAlgoTimerOccurrenceStatusV1.SKIPPED,
        ExecutionAlgoTimerOccurrenceStatusV1.EXPIRED,
    }:
        if emitted_event_id is not None or catch_up_reason is None:
            raise KernelClockError(
                "MINIQMT_TIMER_FINALIZATION_OUTCOME_INVALID",
                "terminal catch-up outcome requires a reason and cannot fabricate an event identity",
                context={"schedule_id": schedule.schedule_id},
            )
        schedule_status = ExecutionAlgoTimerScheduleStatusV1.EXPIRED
        catch_up_receipt = hash_hex_v1(
            "miniqmt_timer_catch_up_receipt_v1",
            {
                "runtime_id": schedule.runtime_id,
                "algo_instance_id": schedule.algo_instance_id,
                "schedule_id": schedule.schedule_id,
                "timer_occurrence_id": occurrence.timer_occurrence_id,
                "due_at_exchange_utc": schedule.due_at_exchange_utc,
                "effective_due_at_exchange_utc": effective_timer_due_at_v1(authority, schedule.due_at_exchange_utc),
                "catch_up_policy": schedule.catch_up_policy,
                "outcome": occurrence_status.value,
                "reason_code": catch_up_reason,
                "exchange_session_authority_sha256": authority.authority_sha256,
                "closed_at_utc": closed,
            },
        )
    else:
        raise KernelClockError(
            "MINIQMT_TIMER_FINALIZATION_OUTCOME_INVALID",
            "timer occurrence outcome is not terminal",
            context={"status": occurrence_status.value},
        )
    mutation = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=schedule.algo_instance_id,
        transition_id=f"mqtimerfinal_{schedule.lease_epoch}",
        ordinal=0,
        timer_name=schedule.timer_name,
        schedule_epoch=schedule.schedule_epoch,
        due_at_exchange_utc=schedule.due_at_exchange_utc,
        catch_up_policy=schedule.catch_up_policy,
        payload=thaw_json_v1(schedule.payload),
    )
    terminal_schedule = ExecutionAlgoTimerScheduleV1.create(
        runtime_id=schedule.runtime_id,
        mutation=mutation,
        status=schedule_status,
        emitted_event_id=emitted_event_id,
        lease_owner=None,
        lease_epoch=schedule.lease_epoch,
        lease_fence_token=None,
        lease_expires_at_utc=None,
        row_version=schedule.row_version + 1,
        created_at_utc=schedule.created_at_utc,
        updated_at_utc=closed,
        closed_at_utc=closed,
    )
    terminal_occurrence = ExecutionAlgoTimerOccurrenceV1.create(
        schedule=terminal_schedule,
        exchange_session_authority_sha256=authority.authority_sha256,
        status=occurrence_status,
        emitted_event_id=emitted_event_id,
        catch_up_receipt_sha256=catch_up_receipt,
        lease_owner=None,
        lease_epoch=occurrence.lease_epoch,
        lease_fence_token=None,
        lease_expires_at_utc=None,
        row_version=occurrence.row_version + 1,
        created_at_utc=occurrence.created_at_utc,
        closed_at_utc=closed,
    )
    terminal_schedule.validate_successor_v1(schedule)
    terminal_occurrence.validate_successor_v1(occurrence)
    return terminal_schedule, terminal_occurrence


def _same_event_without_process_monotonic_v1(
    expected: RuntimeEventEnvelopeV2, persisted: RuntimeEventEnvelopeV2
) -> bool:
    expected_payload = expected.canonical_payload_v1(exclude={"monotonic_ns"})
    persisted_payload = persisted.canonical_payload_v1(exclude={"monotonic_ns"})
    return expected_payload == persisted_payload


@dataclass(frozen=True)
class ExchangeSessionClockV1:
    """One shadow clock worker over the unique K2 repository and ingress path."""

    repository: KernelClockRepositoryV1
    catalog_runtime: PluginCatalogRuntimeV2
    lease_owner: str

    def _ingest_deterministic_event(
        self,
        *,
        event_id: str,
        builder: Callable[[int], RuntimeEventEnvelopeV2],
    ) -> RuntimeEventIngressReceiptV1:
        from .kernel_ingress import KernelIngressCoordinatorV1
        from .kernel_repository import KernelRepositoryCommitUnknown, KernelRepositoryConflict

        coordinator = KernelIngressCoordinatorV1(
            repository=self.repository,
            catalog_runtime=self.catalog_runtime,
        )
        try:
            existing = self.repository.read_event_transaction(event_id)
        except KeyError:
            existing = None
        if existing is not None:
            persisted_event = existing["event"]
            expected = builder(persisted_event.sequence)
            if not _same_event_without_process_monotonic_v1(expected, persisted_event):
                raise KernelClockError(
                    "MINIQMT_CLOCK_EVENT_DURABLE_DRIFT",
                    "deterministic clock event identity exists with different durable facts",
                    context={"event_id": event_id, "runtime_id": persisted_event.runtime_id},
                )
            return existing["receipt"]
        last_conflict: BaseException | None = None
        for _ in range(3):
            sequence = self.repository.read_runtime_last_event_sequence(builder(1).runtime_id) + 1
            event = builder(sequence)
            try:
                return coordinator.ingest(event=event)
            except (KernelRepositoryConflict, KernelRepositoryCommitUnknown) as exc:
                last_conflict = exc
                try:
                    committed = self.repository.read_event_transaction(event_id)
                except KeyError:
                    if isinstance(exc, KernelRepositoryCommitUnknown):
                        raise exc
                    if str(exc) not in {
                        "event sequence is not the exact runtime successor",
                        "runtime event sequence CAS failed",
                    }:
                        raise exc
                    continue
                persisted_event = committed["event"]
                expected = builder(persisted_event.sequence)
                if not _same_event_without_process_monotonic_v1(expected, persisted_event):
                    raise KernelClockError(
                        "MINIQMT_CLOCK_EVENT_DURABLE_DRIFT",
                        "clock event committed during retry with conflicting facts",
                        context={"event_id": event_id, "exception": exc},
                    ) from exc
                return committed["receipt"]
        raise KernelClockError(
            "MINIQMT_CLOCK_EVENT_SEQUENCE_CONTENTION",
            "clock event could not acquire the exact runtime sequence after bounded retries",
            context={"event_id": event_id, "last_conflict": last_conflict},
        ) from last_conflict

    def wake(
        self,
        *,
        runtime_id: str,
        exchange_trade_date: date,
        observed_at_utc: Any,
        monotonic_ns: int,
        lease_expires_at_utc: Any,
        timer_page_limit: int = 200,
        max_timer_pages: int = 100,
    ) -> KernelClockWakeReceiptV1:
        if type(timer_page_limit) is not int or not 1 <= timer_page_limit <= 200:
            raise ValueError("timer_page_limit must be a strict integer in [1, 200]")
        if type(max_timer_pages) is not int or not 1 <= max_timer_pages <= 1000:
            raise ValueError("max_timer_pages must be a strict integer in [1, 1000]")
        authority = _strict_authority(
            self.repository.read_exchange_session_authority(
                runtime_id=runtime_id,
                exchange_trade_date=exchange_trade_date,
            )
        )
        projection = project_exchange_session_v1(authority, observed_at_utc)
        observed = projection.observed_at_utc
        boundaries = (
            (
                "SESSION_OPEN",
                SessionPhaseV1.CONTINUOUS_AM,
                projection.am_start_utc,
            ),
            (
                "SESSION_LUNCH_START",
                SessionPhaseV1.LUNCH_BREAK,
                projection.am_end_utc,
            ),
            (
                "SESSION_LUNCH_END",
                SessionPhaseV1.CONTINUOUS_PM,
                projection.pm_start_utc,
            ),
        )
        session_event_ids: list[str] = []
        for event_type, phase, boundary in boundaries:
            if boundary > observed:
                continue
            probe = build_session_event_v1(
                authority=authority,
                sequence=1,
                session_event_type=event_type,
                session_phase=phase,
                phase_boundary_at_utc=boundary,
            )
            receipt = self._ingest_deterministic_event(
                event_id=probe.event_id,
                builder=lambda sequence, event_type=event_type, phase=phase, boundary=boundary: build_session_event_v1(
                    authority=authority,
                    sequence=sequence,
                    session_event_type=event_type,
                    session_phase=phase,
                    phase_boundary_at_utc=boundary,
                ),
            )
            session_event_ids.append(receipt.event_id)

        observed_datetime = _parse_utc(observed, field_name="observed_at_utc")
        am_start_datetime = _parse_utc(projection.am_start_utc, field_name="am_start_utc")
        if projection.session_phase is SessionPhaseV1.LUNCH_BREAK:
            due_cutoff = canonical_utc_datetime_v1(
                _parse_utc(projection.am_end_utc, field_name="am_end_utc") - timedelta(microseconds=1)
            )
        elif observed >= projection.pm_end_utc:
            due_cutoff = projection.pm_end_utc
        else:
            due_cutoff = observed
        timer_event_ids: list[str] = []
        terminal_occurrence_ids: list[str] = []
        processed = 0
        pages = 0
        more_due = False
        while observed_datetime >= am_start_datetime and pages < max_timer_pages:
            claimed = self.repository.claim_due_timer_schedules_atomic(
                runtime_id=runtime_id,
                exchange_trade_date=exchange_trade_date,
                exchange_session_authority_sha256=authority.authority_sha256,
                due_cutoff_at_utc=due_cutoff,
                observed_at_utc=observed,
                lease_owner=self.lease_owner,
                lease_expires_at_utc=lease_expires_at_utc,
                eligible_algo_statuses=("ACTIVE", "PAUSED") if observed >= projection.pm_end_utc else ("ACTIVE",),
                limit=timer_page_limit,
            )
            if not claimed:
                more_due = False
                break
            pages += 1
            processed += len(claimed)
            for schedule, occurrence in claimed:
                try:
                    policy = CatchUpPolicyV1(schedule.catch_up_policy)
                except ValueError as exc:
                    raise KernelClockError(
                        "MINIQMT_TIMER_CATCH_UP_POLICY_INVALID",
                        "timer schedule uses an unsupported catch-up policy",
                        context={"schedule_id": schedule.schedule_id, "catch_up_policy": schedule.catch_up_policy},
                    ) from exc
                after_close = observed >= projection.pm_end_utc
                if after_close or policy is CatchUpPolicyV1.TERMINAL_EXPIRED:
                    terminal = _terminal_timer_pair_v1(
                        authority=authority,
                        schedule=schedule,
                        occurrence=occurrence,
                        closed_at_utc=observed,
                        emitted_event_id=None,
                        occurrence_status=ExecutionAlgoTimerOccurrenceStatusV1.EXPIRED,
                        catch_up_reason=(
                            "MINIQMT_TIMER_EOD_TERMINAL_EXPIRED"
                            if after_close
                            else "MINIQMT_TIMER_POLICY_TERMINAL_EXPIRED"
                        ),
                    )
                    self.repository.finalize_timer_claim_atomic(schedule=terminal[0], occurrence=terminal[1])
                    terminal_occurrence_ids.append(occurrence.timer_occurrence_id)
                    continue
                if policy is CatchUpPolicyV1.SKIP_WITH_RECEIPT:
                    terminal = _terminal_timer_pair_v1(
                        authority=authority,
                        schedule=schedule,
                        occurrence=occurrence,
                        closed_at_utc=observed,
                        emitted_event_id=None,
                        occurrence_status=ExecutionAlgoTimerOccurrenceStatusV1.SKIPPED,
                        catch_up_reason="MINIQMT_TIMER_POLICY_SKIP_WITH_RECEIPT",
                    )
                    self.repository.finalize_timer_claim_atomic(schedule=terminal[0], occurrence=terminal[1])
                    terminal_occurrence_ids.append(occurrence.timer_occurrence_id)
                    continue
                probe = build_timer_event_v1(
                    authority=authority,
                    schedule=schedule,
                    occurrence=occurrence,
                    sequence=1,
                    monotonic_ns=monotonic_ns,
                )
                receipt = self._ingest_deterministic_event(
                    event_id=probe.event_id,
                    builder=lambda sequence, schedule=schedule, occurrence=occurrence: build_timer_event_v1(
                        authority=authority,
                        schedule=schedule,
                        occurrence=occurrence,
                        sequence=sequence,
                        monotonic_ns=monotonic_ns,
                    ),
                )
                terminal = _terminal_timer_pair_v1(
                    authority=authority,
                    schedule=schedule,
                    occurrence=occurrence,
                    closed_at_utc=occurrence.created_at_utc,
                    emitted_event_id=receipt.event_id,
                    occurrence_status=ExecutionAlgoTimerOccurrenceStatusV1.EVENT_COMMITTED,
                    catch_up_reason=None,
                )
                self.repository.finalize_timer_claim_atomic(schedule=terminal[0], occurrence=terminal[1])
                timer_event_ids.append(receipt.event_id)
            more_due = len(claimed) == timer_page_limit
            if not more_due:
                break

        close_id: str | None = None
        eod_id: str | None = None
        if observed >= projection.pm_end_utc:
            close_probe = build_session_event_v1(
                authority=authority,
                sequence=1,
                session_event_type="SESSION_CLOSE",
                session_phase=SessionPhaseV1.CLOSED,
                phase_boundary_at_utc=projection.pm_end_utc,
            )
            close_receipt = self._ingest_deterministic_event(
                event_id=close_probe.event_id,
                builder=lambda sequence: build_session_event_v1(
                    authority=authority,
                    sequence=sequence,
                    session_event_type="SESSION_CLOSE",
                    session_phase=SessionPhaseV1.CLOSED,
                    phase_boundary_at_utc=projection.pm_end_utc,
                ),
            )
            close_id = close_receipt.event_id
            session_event_ids.append(close_id)
            eod_probe = build_eod_event_v1(
                authority=authority,
                sequence=1,
                phase_boundary_at_utc=projection.pm_end_utc,
            )
            eod_receipt = self._ingest_deterministic_event(
                event_id=eod_probe.event_id,
                builder=lambda sequence: build_eod_event_v1(
                    authority=authority,
                    sequence=sequence,
                    phase_boundary_at_utc=projection.pm_end_utc,
                ),
            )
            eod_id = eod_receipt.event_id
        return KernelClockWakeReceiptV1(
            runtime_id=runtime_id,
            exchange_trade_date=exchange_trade_date.isoformat(),
            authority_sha256=authority.authority_sha256,
            observed_at_utc=observed,
            ordered_session_event_ids=tuple(session_event_ids),
            ordered_timer_event_ids=tuple(timer_event_ids),
            ordered_terminal_occurrence_ids=tuple(terminal_occurrence_ids),
            eod_event_id=eod_id,
            processed_timer_count=processed,
            timer_page_count=pages,
            more_due_timers=more_due,
        )


__all__ = [
    "CatchUpPolicyV1",
    "ExchangeSessionClockV1",
    "ExchangeSessionProjectionV1",
    "KernelClockRepositoryV1",
    "KernelClockWakeReceiptV1",
    "KernelClockError",
    "add_exchange_active_seconds_v1",
    "build_eod_event_v1",
    "build_session_event_v1",
    "build_timer_event_v1",
    "effective_timer_due_at_v1",
    "project_exchange_session_v1",
    "session_epoch_v1",
    "session_event_id_v1",
]
