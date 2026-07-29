from __future__ import annotations

from backend.services.miniqmt_execution_runtime.kernel_clock import (
    effective_timer_due_at_v1,
    session_epoch_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    EventSourceV2,
    EventTypeV2,
    RuntimeEventEnvelopeV2,
)
from backend.tests.miniqmt_execution_runtime.test_current_three_kernel_plugins import (
    _plugin,
    _services,
    _start_context,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_clock import _authority


def _timer_event(
    *,
    sequence: int,
    event_time_utc: str,
    occurrence_id: str,
    schedule_epoch: str,
    exchange_trade_date: str,
    session_phase: str,
) -> RuntimeEventEnvelopeV2:
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k3a_plugins",
        sequence=sequence,
        event_type=EventTypeV2.TIMER,
        event_time_utc=event_time_utc,
        monotonic_ns=sequence,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        symbol="600000.SH",
        payload_schema_version="miniqmt_timer_due_v1",
        payload={
            "timer_occurrence_id": occurrence_id,
            "timer_name": "TWAP_ACTIVE_SECOND",
            "schedule_epoch": schedule_epoch,
        },
        source_identity={"timer_occurrence_id": occurrence_id},
        correlation={
            "exchange_trade_date": exchange_trade_date,
            "session_epoch": schedule_epoch,
            "session_phase": session_phase,
        },
    )


def test_twap_112959_raw_due_moves_to_pm_without_lunch_occurrence_or_burst() -> None:
    authority = _authority()
    schedule_epoch = session_epoch_v1(authority)
    plugin = _plugin("TWAP_LITE_MINIQMT")
    initialized = plugin.initialize(
        _start_context(
            "TWAP_LITE_MINIQMT",
            logical_time_utc="2026-07-27T03:29:59Z",
            exchange_trade_date="2026-07-27",
            session_epoch=schedule_epoch,
        )
    )
    raw_due = initialized.timer_mutations[0].due_at_exchange_utc

    assert raw_due == "2026-07-27T03:30:00.000000Z"
    assert effective_timer_due_at_v1(authority, raw_due) == "2026-07-27T05:00:00.000000Z"

    pm_event = _timer_event(
        sequence=2,
        event_time_utc="2026-07-27T05:00:00Z",
        occurrence_id="timer_occurrence_k3a_pm_first",
        schedule_epoch=schedule_epoch,
        exchange_trade_date="2026-07-27",
        session_phase="CONTINUOUS_PM",
    )
    applied = plugin.transition(
        state=initialized.next_state,
        event=pm_event,
        services=_services(state=initialized.next_state, event=pm_event, delivery_suffix="pm_first"),
    )

    state = thaw_json_v1(applied.next_state.state)
    assert state["active_elapsed_seconds"] == 1
    assert state["interval_elapsed_seconds"] == 1
    assert len(applied.timer_mutations) == 1
    assert applied.timer_mutations[0].due_at_exchange_utc == "2026-07-27T05:00:01.000000Z"

    replay = _plugin("TWAP_LITE_MINIQMT").transition(
        state=_plugin("TWAP_LITE_MINIQMT").restore_state(applied.next_state),
        event=pm_event,
        services=_services(state=applied.next_state, event=pm_event, delivery_suffix="pm_replay"),
    )
    replay_state = thaw_json_v1(replay.next_state.state)
    assert replay_state["active_elapsed_seconds"] == 1
    assert replay.timer_mutations == ()
    assert replay.diagnostic_observations[0].reason_code == "TWAP_TIMER_DUPLICATE"


def test_twap_restart_counts_each_durable_active_second_once_and_stops_before_final_slice() -> None:
    plugin = _plugin("TWAP_LITE_MINIQMT")
    snapshot = plugin.initialize(_start_context("TWAP_LITE_MINIQMT")).next_state

    for sequence in range(2, 6):
        event = _timer_event(
            sequence=sequence,
            event_time_utc=f"2026-07-28T01:30:0{sequence - 1}Z",
            occurrence_id=f"timer_occurrence_k3a_duration_{sequence - 1}",
            schedule_epoch="session_k3a_am",
            exchange_trade_date="2026-07-28",
            session_phase="CONTINUOUS_AM",
        )
        restored = _plugin("TWAP_LITE_MINIQMT").restore_state(snapshot)
        transition = _plugin("TWAP_LITE_MINIQMT").transition(
            state=restored,
            event=event,
            services=_services(state=restored, event=event, delivery_suffix=f"duration_{sequence}"),
        )
        snapshot = transition.next_state

    state = thaw_json_v1(snapshot.state)
    assert state["active_elapsed_seconds"] == 4
    assert state["status"] == "FINISHED"
    assert transition.terminal_outcome.value == "EXPIRED_WITH_RESIDUAL"
    assert transition.broker_commands == ()
    assert transition.timer_mutations == ()
