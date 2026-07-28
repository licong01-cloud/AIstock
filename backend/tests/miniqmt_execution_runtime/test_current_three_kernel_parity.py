from __future__ import annotations

from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
    build_kernel_command_outcome_event_payload_v1,
    build_kernel_order_event_payload_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2, SideV1
from backend.tests.miniqmt_execution_runtime.test_current_three_kernel_plugins import (
    _event,
    _plugin,
    _services,
    _start_context,
    _tick,
)


def _accepted_submit(plugin, submitted, command, *, sequence: int, suffix: str):  # type: ignore[no-untyped-def]
    common = {
        "runtime_id": command.runtime_id,
        "algo_instance_id": submitted.next_state.algo_instance_id,
        "parent_intent_id": f"parent_{plugin.ALGO_CODE.lower()}",
        "strategy_slot_id": f"slot_{plugin.ALGO_CODE.lower()}",
        "mapping_id": f"mapping_{suffix}",
        "command_id": command.command_id,
        "local_vt_orderid": command.local_vt_orderid,
        "broker_order_id": f"broker_{suffix}",
    }
    payload = build_kernel_command_outcome_event_payload_v1(
        receipt_id=f"mqoutcomercpt_{suffix}",
        receipt_sha256="7" * 64,
        **common,
        command_type="SUBMIT_LIMIT",
        outcome="ACCEPTED",
        outbox_status="ACKED",
        outbox_row_version=3,
        outcome_receipt_sha256="8" * 64,
        outbox_terminal=True,
        order_terminal=False,
    )
    event = _event(
        sequence=sequence,
        event_type=EventTypeV2.COMMAND_OUTCOME,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        schema="miniqmt_command_outcome_v1",
        payload=payload.model_dump(mode="json"),
        source_identity={"receipt_id": payload.receipt_id, "receipt_sha256": payload.receipt_sha256},
    )
    accepted = plugin.transition(
        state=submitted.next_state,
        event=event,
        services=_services(state=submitted.next_state, event=event, delivery_suffix=f"{suffix}_accepted"),
    )
    return accepted, common


def test_sniper_sell_uses_frozen_limit_native_bid_depth_and_cancel_before_requote() -> None:
    plugin = _plugin("SNIPER_MINIQMT")
    initial = plugin.initialize(_start_context("SNIPER_MINIQMT", side=SideV1.SELL)).next_state

    not_crossed = _tick(2, bid="9.99", ask="10.01", volume=200)
    waiting = plugin.transition(
        state=initial,
        event=not_crossed,
        services=_services(state=initial, event=not_crossed, delivery_suffix="sniper_sell_wait"),
    )
    assert waiting.broker_commands == ()

    crossed = _tick(3, bid="10.01", ask="10.02", volume=150)
    submitted = plugin.transition(
        state=waiting.next_state,
        event=crossed,
        services=_services(state=waiting.next_state, event=crossed, delivery_suffix="sniper_sell_submit"),
    )
    command = submitted.broker_commands[0]
    assert command.side is SideV1.SELL
    assert command.price_decimal == "10"
    assert command.quantity == 150

    accepted, _ = _accepted_submit(plugin, submitted, command, sequence=4, suffix="sniper_sell")
    next_tick = _tick(5, bid="10.02", ask="10.03", volume=200)
    cancelling = plugin.transition(
        state=accepted.next_state,
        event=next_tick,
        services=_services(state=accepted.next_state, event=next_tick, delivery_suffix="sniper_sell_cancel"),
    )
    assert len(cancelling.broker_commands) == 1
    assert cancelling.broker_commands[0].command_type.value == "CANCEL_ORDER"
    assert cancelling.broker_commands[0].owned_broker_order_id == "broker_sniper_sell"


def test_best_limit_price_change_cancel_then_restart_reuses_next_persisted_draw_ordinal() -> None:
    plugin = _plugin("BEST_LIMIT_MINIQMT")
    initial = plugin.initialize(_start_context("BEST_LIMIT_MINIQMT")).next_state
    first_tick = _tick(2, bid="9.88", ask="9.89")
    submitted = plugin.transition(
        state=initial,
        event=first_tick,
        services=_services(state=initial, event=first_tick, delivery_suffix="best_first"),
    )
    first_command = submitted.broker_commands[0]
    accepted, common = _accepted_submit(plugin, submitted, first_command, sequence=3, suffix="best_limit")

    same_price = _tick(4, bid="9.88", ask="9.90")
    unchanged = plugin.transition(
        state=accepted.next_state,
        event=same_price,
        services=_services(state=accepted.next_state, event=same_price, delivery_suffix="best_same"),
    )
    assert unchanged.broker_commands == ()

    changed_price = _tick(5, bid="9.87", ask="9.89")
    cancelling = plugin.transition(
        state=unchanged.next_state,
        event=changed_price,
        services=_services(state=unchanged.next_state, event=changed_price, delivery_suffix="best_changed"),
    )
    assert len(cancelling.broker_commands) == 1
    assert cancelling.broker_commands[0].command_type.value == "CANCEL_ORDER"
    assert thaw_json_v1(cancelling.next_state.state)["next_draw_ordinal"] == 1

    terminal_payload = build_kernel_order_event_payload_v1(
        raw_payload={"order_status": 54, "traded_volume": 0},
        order_event_id="order_event_best_cancelled",
        requested_quantity=first_command.quantity,
        symbol="600000.SH",
        side="BUY",
        **common,
    )
    terminal = _event(
        sequence=6,
        event_type=EventTypeV2.ORDER,
        source=EventSourceV2.QMT_GATEWAY_CALLBACK,
        schema="miniqmt_order_event_v1",
        payload=terminal_payload.model_dump(mode="json"),
        source_identity={"order_event_id": terminal_payload.order_event_id},
    )
    cleared = plugin.transition(
        state=cancelling.next_state,
        event=terminal,
        services=_services(state=cancelling.next_state, event=terminal, delivery_suffix="best_terminal"),
    )
    replacement_tick = _tick(7, bid="9.86", ask="9.88")
    services = _services(state=cleared.next_state, event=replacement_tick, delivery_suffix="best_replace")

    first = _plugin("BEST_LIMIT_MINIQMT").transition(
        state=_plugin("BEST_LIMIT_MINIQMT").restore_state(cleared.next_state),
        event=replacement_tick,
        services=services,
    )
    replay = _plugin("BEST_LIMIT_MINIQMT").transition(
        state=_plugin("BEST_LIMIT_MINIQMT").restore_state(cleared.next_state),
        event=replacement_tick,
        services=services,
    )

    assert first == replay
    assert thaw_json_v1(first.broker_commands[0].metadata)["draw_ordinal"] == 1
    assert thaw_json_v1(first.next_state.state)["next_draw_ordinal"] == 2
