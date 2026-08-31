from __future__ import annotations

from typing import Any

import pytest

from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1


def _manifest(algo_code: str):
    from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v3

    return next(item for item in current_three_manifests_v3() if item.algo_code == algo_code)


def _config(algo_code: str) -> dict[str, Any]:
    return {
        "SNIPER_MINIQMT": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        "BEST_LIMIT_MINIQMT": {"min_volume": 100, "max_volume": 500},
        "TWAP_LITE_MINIQMT": {"time": 4, "interval": 2},
    }[algo_code]


def _plugin(algo_code: str):
    from backend.execution_algos.vnpy_style.plugin_factories import current_three_process_bindings_v3

    binding_id = {
        "SNIPER_MINIQMT": "aistock.vnpy.sniper.factory",
        "BEST_LIMIT_MINIQMT": "aistock.vnpy.best_limit.factory",
        "TWAP_LITE_MINIQMT": "aistock.vnpy.twap_lite.factory",
    }[algo_code]
    return current_three_process_bindings_v3().resolve(binding_id)(_config(algo_code))


def _start_context(
    algo_code: str,
    *,
    side: Any = None,
    logical_time_utc: str = "2026-07-28T01:30:00Z",
    exchange_trade_date: str = "2026-07-28",
    session_epoch: str = "session_k3a_am",
    session_phase: Any = None,
):
    from backend.services.miniqmt_execution_runtime.plugin_contracts import (
        AlgoStartContextV1,
        DeterministicExecutionContextV1,
        SessionPhaseV1,
        SideV1,
        _algo_instance_id_v2,
    )

    manifest = _manifest(algo_code)
    selected_side = SideV1.BUY if side is None else SideV1(side)
    selected_phase = SessionPhaseV1.CONTINUOUS_AM if session_phase is None else SessionPhaseV1(session_phase)
    config = _config(algo_code)
    config_sha256 = hash_hex_v1("miniqmt_plugin_config_v2", config)
    parent_intent_id = f"parent_{algo_code.lower()}"
    strategy_slot_id = f"slot_{algo_code.lower()}"
    algo_instance_id = _algo_instance_id_v2(
        runtime_id="runtime_k3a_plugins",
        parent_intent_id=parent_intent_id,
        strategy_slot_id=strategy_slot_id,
        algo_code=algo_code,
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        plugin_manifest_sha256=manifest.manifest_sha256,
        plugin_config_sha256=config_sha256,
    )
    context = DeterministicExecutionContextV1.create(
        runtime_id="runtime_k3a_plugins",
        algo_instance_id=algo_instance_id,
        event_id=f"event_start_{algo_code.lower()}",
        delivery_id=f"delivery_start_{algo_code.lower()}",
        plugin_manifest_sha256=manifest.manifest_sha256,
        transition_sequence=0,
        logical_time_utc=logical_time_utc,
        exchange_trade_date=exchange_trade_date,
        session_epoch=session_epoch,
        session_phase=selected_phase,
        input_projection_sha256="9" * 64,
    )
    contract = {"pricetick_decimal": "0.01", "min_volume": 100}
    account = {"account_group_id": "sim_account"}
    capability = {"route_id": "miniqmt_sim_b0", "capabilities": ["L1_ASK", "L1_BID"]}
    return AlgoStartContextV1(
        schema_version="miniqmt_algo_start_context_v1",
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        parent_intent_id=parent_intent_id,
        strategy_slot_id=strategy_slot_id,
        symbol="600000.SH",
        side=selected_side,
        limit_price_decimal="10",
        parent_quantity=200,
        min_volume=100,
        volume_increment=100,
        plugin_manifest=manifest,
        plugin_config=config,
        plugin_config_sha256=config_sha256,
        start_event_id=context.event_id,
        start_delivery_id=context.delivery_id,
        deterministic_context=context,
        contract_projection=contract,
        contract_projection_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
        account_projection=account,
        account_projection_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
        market_capability_projection=capability,
        market_capability_projection_sha256=hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
        execution_plan_id="plan_k3a_plugins",
        execution_plan_sha256="a" * 64,
        release_id="release_k3a_plugins",
        release_sha256="b" * 64,
        policy_id="policy_k3a_plugins",
        policy_sha256="c" * 64,
    )


def _event(
    *,
    sequence: int,
    event_type: Any,
    source: Any,
    schema: str,
    payload: dict[str, Any],
    source_identity: dict[str, Any],
):
    from backend.services.miniqmt_execution_runtime.plugin_contracts import RuntimeEventEnvelopeV2

    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k3a_plugins",
        sequence=sequence,
        event_type=event_type,
        event_time_utc=f"2026-07-28T01:30:{sequence:02d}Z",
        monotonic_ns=None,
        source=source,
        symbol="600000.SH",
        payload_schema_version=schema,
        payload=payload,
        source_identity=source_identity,
        correlation={
            "exchange_trade_date": "2026-07-28",
            "session_epoch": "session_k3a_am",
            "session_phase": "CONTINUOUS_AM",
        },
    )


def _services(*, state: Any, event: Any, delivery_suffix: str):
    from backend.services.miniqmt_execution_runtime.plugin_contracts import (
        AlgoReadOnlyServicesV1,
        ExecutionProjectionSetV1,
    )

    delivery_id = f"delivery_{delivery_suffix}"
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=state.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        projection_refs=(),
    )
    return AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=state.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id=None,
        market_data_projection=None,
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )


def _services_with_market(*, state: Any, event: Any, delivery_suffix: str, market_id: str, payload: dict[str, Any]):
    from backend.services.miniqmt_execution_runtime.plugin_contracts import (
        AlgoReadOnlyServicesV1,
        ExecutionProjectionRefV1,
        ExecutionProjectionSetV1,
        KernelProjectionTypeV1,
    )

    delivery_id = f"delivery_{delivery_suffix}"
    payload_sha256 = hash_hex_v1("miniqmt_market_data_projection_v2", payload)
    ref = ExecutionProjectionRefV1.create(
        projection_type=KernelProjectionTypeV1.MARKET_DATA,
        projection_id=market_id,
        projection_version="1",
        payload_sha256=payload_sha256,
        source_event_id=f"source_{market_id}",
        logical_at_utc=event.event_time_utc,
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=state.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        projection_refs=(ref,),
    )
    return AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=state.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id=market_id,
        market_data_projection=payload,
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )


def _timer(sequence: int, occurrence_id: str):
    from backend.services.miniqmt_execution_runtime.plugin_contracts import (
        EventSourceV2,
        EventTypeV2,
        RuntimeEventEnvelopeV2,
    )

    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k3a_plugins",
        sequence=sequence,
        event_type=EventTypeV2.TIMER,
        event_time_utc=f"2026-07-28T01:30:{sequence:02d}Z",
        monotonic_ns=sequence,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        symbol="600000.SH",
        payload_schema_version="miniqmt_timer_due_v1",
        payload={
            "timer_occurrence_id": occurrence_id,
            "timer_name": "TWAP_ACTIVE_SECOND",
            "schedule_epoch": "session_k3a_am",
        },
        source_identity={"timer_occurrence_id": occurrence_id},
        correlation={
            "exchange_trade_date": "2026-07-28",
            "session_epoch": "session_k3a_am",
            "session_phase": "CONTINUOUS_AM",
        },
    )


def _tick(sequence: int, *, bid: str = "9.9", ask: str = "10", volume: int = 200):
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    return _event(
        sequence=sequence,
        event_type=EventTypeV2.TICK,
        source=EventSourceV2.B0_QUOTE_V2,
        schema="miniqmt_market_data_view_v2",
        payload={
            "generation": sequence,
            "session_phase": "CONTINUOUS_AM",
            "exchange_time_utc": f"2026-07-28T01:30:{sequence:02d}Z",
            "bid_price_1": bid,
            "ask_price_1": ask,
            "bid_volume_1": volume,
            "ask_volume_1": volume,
        },
        source_identity={"market_data_id": f"market_k3a_{sequence}"},
    )


def test_public_transition_id_helper_is_the_exact_single_authority() -> None:
    from backend.services.miniqmt_execution_runtime.plugin_contracts import algo_transition_id_v1

    values = {
        "delivery_id": "mqdelivery_k3a_1",
        "event_id": "mqrtevt_k3a_1",
        "runtime_id": "runtime_k3a_1",
        "algo_instance_id": "mqalgo_k3a_1",
        "transition_sequence": 2,
    }
    expected = "mqtransition_" + hash_hex_v1("miniqmt_algo_transition_identity_v1", values)
    assert algo_transition_id_v1(**values) == expected

    with pytest.raises((TypeError, ValueError)):
        algo_transition_id_v1(**{**values, "delivery_id": ""})
    with pytest.raises((TypeError, ValueError)):
        algo_transition_id_v1(**{**values, "transition_sequence": True})


def test_v3_manifest_factory_class_binding_closure_is_exact() -> None:
    from backend.execution_algos.vnpy_style.best_limit_plugin import BestLimitMiniQMTPluginV3
    from backend.execution_algos.vnpy_style.plugin_factories import current_three_process_bindings_v3
    from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v3
    from backend.execution_algos.vnpy_style.sniper_plugin import SniperMiniQMTPluginV3
    from backend.execution_algos.vnpy_style.twap_lite_plugin import TwapLiteMiniQMTPluginV3
    from backend.services.miniqmt_execution_runtime.kernel_delivery import ExecutionAlgoPluginV2
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventTypeV2

    expected = {
        "SNIPER_MINIQMT": (
            "backend.execution_algos.vnpy_style.plugin_factories:create_sniper_miniqmt_plugin_v3",
            "aistock.vnpy.sniper.factory",
            SniperMiniQMTPluginV3,
            {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        ),
        "BEST_LIMIT_MINIQMT": (
            "backend.execution_algos.vnpy_style.plugin_factories:create_best_limit_miniqmt_plugin_v3",
            "aistock.vnpy.best_limit.factory",
            BestLimitMiniQMTPluginV3,
            {"min_volume": 100, "max_volume": 500},
        ),
        "TWAP_LITE_MINIQMT": (
            "backend.execution_algos.vnpy_style.plugin_factories:create_twap_lite_miniqmt_plugin_v3",
            "aistock.vnpy.twap_lite.factory",
            TwapLiteMiniQMTPluginV3,
            {"time": 600, "interval": 60},
        ),
    }
    manifests = current_three_manifests_v3()
    bindings = current_three_process_bindings_v3()

    assert {item.algo_code for item in manifests} == set(expected)
    assert {item.plugin_version for item in manifests} == {"3.0.0"}
    for manifest in manifests:
        implementation_ref, binding_id, plugin_class, config = expected[manifest.algo_code]
        assert manifest.implementation_ref == implementation_ref
        assert EventTypeV2.COMMAND_OUTCOME in manifest.subscribed_event_types
        factory = bindings.resolve(binding_id)
        plugin = factory(config)
        assert isinstance(plugin, ExecutionAlgoPluginV2)
        assert type(plugin) is plugin_class
        assert plugin.manifest == manifest


def test_sniper_transition_is_pure_transition_first_and_pending_safe() -> None:
    plugin = _plugin("SNIPER_MINIQMT")
    state = plugin.initialize(_start_context("SNIPER_MINIQMT")).next_state
    event = _tick(2)
    transition = plugin.transition(
        state=state,
        event=event,
        services=_services(state=state, event=event, delivery_suffix="sniper_tick_2"),
    )

    assert len(transition.broker_commands) == 1
    command = transition.broker_commands[0]
    assert command.price_decimal == "10"
    assert command.quantity == 200
    active = thaw_json_v1(transition.next_state.state)["active_orders"][0]
    assert active["status"] == "COMMAND_PENDING"
    assert active["pending_command_id"] == command.command_id
    assert active["broker_order_id"] is None

    next_event = _tick(3)
    repeated = plugin.transition(
        state=transition.next_state,
        event=next_event,
        services=_services(state=transition.next_state, event=next_event, delivery_suffix="sniper_tick_3"),
    )
    assert repeated.broker_commands == ()
    assert repeated.diagnostic_observations[0].reason_code == "K3_COMMAND_LIFECYCLE_WAIT"


def test_best_limit_retry_and_restore_preserve_draw_and_effect_identity() -> None:
    plugin = _plugin("BEST_LIMIT_MINIQMT")
    initial = plugin.initialize(_start_context("BEST_LIMIT_MINIQMT")).next_state
    event = _tick(2, bid="9.88", ask="9.89")
    services = _services(state=initial, event=event, delivery_suffix="best_limit_tick_2")

    first = plugin.transition(state=plugin.restore_state(initial), event=event, services=services)
    retried = _plugin("BEST_LIMIT_MINIQMT").transition(
        state=_plugin("BEST_LIMIT_MINIQMT").restore_state(initial), event=event, services=services
    )

    assert first == retried
    assert first.broker_commands[0].price_decimal == "9.88"
    assert thaw_json_v1(first.next_state.state)["next_draw_ordinal"] == 1


def test_command_outcome_and_terminal_order_wait_for_exact_trade_closure() -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_command_outcome_event_payload_v1,
        build_kernel_order_event_payload_v1,
        build_kernel_trade_event_payload_v1,
    )
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    plugin = _plugin("SNIPER_MINIQMT")
    initial = plugin.initialize(_start_context("SNIPER_MINIQMT")).next_state
    tick = _tick(2, volume=200)
    submitted = plugin.transition(
        state=initial,
        event=tick,
        services=_services(state=initial, event=tick, delivery_suffix="lifecycle_tick"),
    )
    command = submitted.broker_commands[0]
    common = {
        "runtime_id": tick.runtime_id,
        "algo_instance_id": submitted.next_state.algo_instance_id,
        "parent_intent_id": "parent_sniper_miniqmt",
        "strategy_slot_id": "slot_sniper_miniqmt",
        "mapping_id": "mapping_sniper_k3a",
        "command_id": command.command_id,
        "local_vt_orderid": command.local_vt_orderid,
        "broker_order_id": "broker_sniper_k3a",
    }
    outcome_payload = build_kernel_command_outcome_event_payload_v1(
        receipt_id="mqoutcomercpt_sniper_k3a",
        receipt_sha256="d" * 64,
        **common,
        command_type="SUBMIT_LIMIT",
        outcome="ACCEPTED",
        outbox_status="ACKED",
        outbox_row_version=3,
        outcome_receipt_sha256="e" * 64,
        outbox_terminal=True,
        order_terminal=False,
    )
    outcome = _event(
        sequence=3,
        event_type=EventTypeV2.COMMAND_OUTCOME,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        schema="miniqmt_command_outcome_v1",
        payload=outcome_payload.model_dump(mode="json"),
        source_identity={"receipt_id": "mqoutcomercpt_sniper_k3a", "receipt_sha256": "d" * 64},
    )
    accepted = plugin.transition(
        state=submitted.next_state,
        event=outcome,
        services=_services(state=submitted.next_state, event=outcome, delivery_suffix="outcome"),
    )
    assert thaw_json_v1(accepted.next_state.state)["active_orders"][0]["status"] == "SUBMITTED"

    order_payload = build_kernel_order_event_payload_v1(
        raw_payload={"order_status": 56, "traded_volume": command.quantity},
        order_event_id="order_event_sniper_terminal",
        requested_quantity=command.quantity,
        symbol="600000.SH",
        side="BUY",
        **common,
    )
    order = _event(
        sequence=4,
        event_type=EventTypeV2.ORDER,
        source=EventSourceV2.QMT_GATEWAY_CALLBACK,
        schema="miniqmt_order_event_v1",
        payload=order_payload.model_dump(mode="json"),
        source_identity={"order_event_id": "order_event_sniper_terminal"},
    )
    pending_trade = plugin.transition(
        state=accepted.next_state,
        event=order,
        services=_services(state=accepted.next_state, event=order, delivery_suffix="order"),
    )
    assert thaw_json_v1(pending_trade.next_state.state)["active_orders"][0]["status"] == "TERMINAL_TRADE_PENDING"
    assert thaw_json_v1(pending_trade.next_state.state)["traded_quantity"] == 0

    trade_payload = build_kernel_trade_event_payload_v1(
        raw_payload={"trade_id": "broker_trade_sniper_k3a"},
        trade_quantity=command.quantity,
        trade_price_decimal="10",
        symbol="600000.SH",
        side="BUY",
        **common,
    )
    trade = _event(
        sequence=5,
        event_type=EventTypeV2.TRADE,
        source=EventSourceV2.QMT_GATEWAY_CALLBACK,
        schema="miniqmt_trade_fact_v1",
        payload=trade_payload.model_dump(mode="json"),
        source_identity={"trade_id": "broker_trade_sniper_k3a"},
    )
    filled = plugin.transition(
        state=pending_trade.next_state,
        event=trade,
        services=_services(state=pending_trade.next_state, event=trade, delivery_suffix="trade"),
    )
    filled_state = thaw_json_v1(filled.next_state.state)
    assert filled_state["active_orders"] == []
    assert filled_state["traded_quantity"] == 200
    assert filled.terminal_outcome.value == "FILLED"


def test_active_order_hash_and_conditional_identity_are_strict() -> None:
    from backend.services.miniqmt_execution_runtime.plugin_contracts import CurrentThreeActiveOrderStateV3

    plugin = _plugin("SNIPER_MINIQMT")
    state = plugin.initialize(_start_context("SNIPER_MINIQMT")).next_state
    event = _tick(2)
    transition = plugin.transition(
        state=state,
        event=event,
        services=_services(state=state, event=event, delivery_suffix="active_hash"),
    )
    active = thaw_json_v1(transition.next_state.state)["active_orders"][0]

    assert CurrentThreeActiveOrderStateV3.model_validate_json(__import__("json").dumps(active))
    with pytest.raises(ValueError):
        CurrentThreeActiveOrderStateV3.model_validate_json(
            __import__("json").dumps({**active, "broker_order_id": "fabricated_pre_ack"})
        )
    with pytest.raises(ValueError):
        CurrentThreeActiveOrderStateV3.model_validate_json(
            __import__("json").dumps({**active, "active_order_state_sha256": "0" * 64})
        )


def test_terminal_order_missing_cumulative_waits_for_trade_then_strict_oms_reconcile() -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_command_outcome_event_payload_v1,
        build_kernel_order_event_payload_v1,
        build_kernel_order_reconcile_event_payload_v1,
        build_kernel_trade_event_payload_v1,
    )
    from backend.services.miniqmt_execution_runtime.plugin_contracts import (
        EventSourceV2,
        EventTypeV2,
        KernelTradeFactRefV1,
    )

    plugin = _plugin("SNIPER_MINIQMT")
    initial = plugin.initialize(_start_context("SNIPER_MINIQMT")).next_state
    tick = _tick(2)
    submitted = plugin.transition(
        state=initial,
        event=tick,
        services=_services(state=initial, event=tick, delivery_suffix="reconcile_submit"),
    )
    command = submitted.broker_commands[0]
    common = {
        "runtime_id": tick.runtime_id,
        "algo_instance_id": submitted.next_state.algo_instance_id,
        "parent_intent_id": "parent_sniper_miniqmt",
        "strategy_slot_id": "slot_sniper_miniqmt",
        "mapping_id": "mapping_sniper_reconcile",
        "command_id": command.command_id,
        "local_vt_orderid": command.local_vt_orderid,
        "broker_order_id": "broker_sniper_reconcile",
    }
    outcome_payload = build_kernel_command_outcome_event_payload_v1(
        receipt_id="mqoutcomercpt_sniper_reconcile",
        receipt_sha256="3" * 64,
        **common,
        command_type="SUBMIT_LIMIT",
        outcome="ACCEPTED",
        outbox_status="ACKED",
        outbox_row_version=3,
        outcome_receipt_sha256="4" * 64,
        outbox_terminal=True,
        order_terminal=False,
    )
    outcome = _event(
        sequence=3,
        event_type=EventTypeV2.COMMAND_OUTCOME,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        schema="miniqmt_command_outcome_v1",
        payload=outcome_payload.model_dump(mode="json"),
        source_identity={"receipt_id": outcome_payload.receipt_id, "receipt_sha256": outcome_payload.receipt_sha256},
    )
    accepted = plugin.transition(
        state=submitted.next_state,
        event=outcome,
        services=_services(state=submitted.next_state, event=outcome, delivery_suffix="reconcile_outcome"),
    )
    order_payload = build_kernel_order_event_payload_v1(
        raw_payload={"order_status": 56},
        order_event_id="order_event_sniper_missing_cumulative",
        requested_quantity=command.quantity,
        symbol="600000.SH",
        side="BUY",
        **common,
    )
    order = _event(
        sequence=4,
        event_type=EventTypeV2.ORDER,
        source=EventSourceV2.QMT_GATEWAY_CALLBACK,
        schema="miniqmt_order_event_v1",
        payload=order_payload.model_dump(mode="json"),
        source_identity={"order_event_id": order_payload.order_event_id},
    )
    pending = plugin.transition(
        state=accepted.next_state,
        event=order,
        services=_services(state=accepted.next_state, event=order, delivery_suffix="reconcile_order"),
    )
    assert pending.diagnostic_observations[0].reason_code == "K3_ORDER_CUMULATIVE_UNAVAILABLE"

    trade_payload = build_kernel_trade_event_payload_v1(
        raw_payload={"trade_id": "broker_trade_sniper_reconcile"},
        trade_quantity=command.quantity,
        trade_price_decimal="10",
        symbol="600000.SH",
        side="BUY",
        **common,
    )
    trade = _event(
        sequence=5,
        event_type=EventTypeV2.TRADE,
        source=EventSourceV2.QMT_GATEWAY_CALLBACK,
        schema="miniqmt_trade_fact_v1",
        payload=trade_payload.model_dump(mode="json"),
        source_identity={"trade_id": trade_payload.trade_id},
    )
    traded = plugin.transition(
        state=pending.next_state,
        event=trade,
        services=_services(state=pending.next_state, event=trade, delivery_suffix="reconcile_trade"),
    )
    traded_state = thaw_json_v1(traded.next_state.state)
    assert traded.terminal_outcome is None
    assert traded_state["traded_quantity"] == command.quantity
    assert traded_state["active_orders"][0]["status"] == "TERMINAL_TRADE_PENDING"

    trade_ref = KernelTradeFactRefV1(trade_id=trade_payload.trade_id, trade_fact_sha256=trade_payload.fact_sha256)
    reconcile_payload = build_kernel_order_reconcile_event_payload_v1(
        ordered_trade_refs=(trade_ref,),
        requested_quantity=command.quantity,
        receipt_id="mqreconcile_sniper_reconcile",
        receipt_sha256="5" * 64,
        runtime_id=tick.runtime_id,
        algo_instance_id=submitted.next_state.algo_instance_id,
        parent_intent_id="parent_sniper_miniqmt",
        strategy_slot_id="slot_sniper_miniqmt",
        mapping_id="mapping_sniper_reconcile",
        local_vt_orderid=command.local_vt_orderid,
        broker_order_id="broker_sniper_reconcile",
        symbol="600000.SH",
        side="BUY",
        normalized_order_status="FILLED",
        authoritative_cumulative_filled_quantity=command.quantity,
        authoritative_remaining_quantity=0,
        callback_watermark=f"{tick.runtime_id}:5",
        snapshot_sha256="6" * 64,
    )
    reconcile = _event(
        sequence=6,
        event_type=EventTypeV2.RECONCILE,
        source=EventSourceV2.QMT_OMS_RECONCILIATION,
        schema="miniqmt_reconciliation_receipt_v1",
        payload=reconcile_payload.model_dump(mode="json"),
        source_identity={
            "receipt_id": reconcile_payload.receipt_id,
            "receipt_sha256": reconcile_payload.receipt_sha256,
        },
    )
    closed = plugin.transition(
        state=traded.next_state,
        event=reconcile,
        services=_services(state=traded.next_state, event=reconcile, delivery_suffix="reconcile_close"),
    )

    assert closed.terminal_outcome.value == "FILLED"
    assert thaw_json_v1(closed.next_state.state)["active_orders"] == []


def test_twap_uses_raw_plus_one_second_and_durable_market_projection() -> None:
    plugin = _plugin("TWAP_LITE_MINIQMT")
    initialized = plugin.initialize(_start_context("TWAP_LITE_MINIQMT"))
    assert initialized.timer_mutations[0].due_at_exchange_utc == "2026-07-28T01:30:01.000000Z"

    tick = _tick(2, ask="9.99")
    ticked = plugin.transition(
        state=initialized.next_state,
        event=tick,
        services=_services(state=initialized.next_state, event=tick, delivery_suffix="twap_tick"),
    )
    market_id = thaw_json_v1(tick.source_identity)["market_data_id"]
    projection = {"ask_price_1": "9.99", "bid_price_1": "9.98"}

    first_timer = _timer(3, "timer_occurrence_k3a_1")
    first = plugin.transition(
        state=ticked.next_state,
        event=first_timer,
        services=_services_with_market(
            state=ticked.next_state,
            event=first_timer,
            delivery_suffix="twap_timer_1",
            market_id=market_id,
            payload=projection,
        ),
    )
    first_state = thaw_json_v1(first.next_state.state)
    assert first_state["active_elapsed_seconds"] == 1
    assert first.broker_commands == ()
    assert first.timer_mutations[0].due_at_exchange_utc == "2026-07-28T01:30:04.000000Z"

    second_timer = _timer(4, "timer_occurrence_k3a_2")
    second = plugin.transition(
        state=first.next_state,
        event=second_timer,
        services=_services_with_market(
            state=first.next_state,
            event=second_timer,
            delivery_suffix="twap_timer_2",
            market_id=market_id,
            payload=projection,
        ),
    )
    assert len(second.broker_commands) == 1
    assert second.broker_commands[0].price_decimal == "10"
    assert second.broker_commands[0].quantity == 100
    second_state = thaw_json_v1(second.next_state.state)
    assert second_state["active_elapsed_seconds"] == 2
    assert second_state["interval_elapsed_seconds"] == 0

    with pytest.raises(ValueError, match="identity conflicts"):
        plugin.transition(
            state=first.next_state,
            event=second_timer,
            services=_services_with_market(
                state=first.next_state,
                event=second_timer,
                delivery_suffix="twap_wrong_market",
                market_id="wrong_market_identity",
                payload=projection,
            ),
        )


@pytest.mark.parametrize(
    ("algo_code", "payload"),
    [
        (
            "SNIPER_MINIQMT",
            {
                "generation": 2,
                "session_phase": "CONTINUOUS_AM",
                "exchange_time_utc": "2026-07-28T01:30:02Z",
                "ask_volume_1": 200,
            },
        ),
        (
            "BEST_LIMIT_MINIQMT",
            {
                "generation": 2,
                "session_phase": "CONTINUOUS_AM",
                "exchange_time_utc": "2026-07-28T01:30:02Z",
            },
        ),
    ],
)
def test_missing_native_quote_waits_without_fallback_or_raw_key_error(algo_code: str, payload: dict[str, Any]) -> None:
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    plugin = _plugin(algo_code)
    state = plugin.initialize(_start_context(algo_code)).next_state
    event = _event(
        sequence=2,
        event_type=EventTypeV2.TICK,
        source=EventSourceV2.B0_QUOTE_V2,
        schema="miniqmt_market_data_view_v2",
        payload=payload,
        source_identity={"market_data_id": f"market_missing_{algo_code.lower()}"},
    )

    transition = plugin.transition(
        state=state,
        event=event,
        services=_services(state=state, event=event, delivery_suffix=f"missing_{algo_code.lower()}"),
    )

    assert transition.broker_commands == ()
    assert transition.diagnostic_observations[0].reason_code == "WAITING_FOR_MARKET_DATA"
    assert thaw_json_v1(transition.next_state.state)["last_tick_lineage"]["event_id"] == event.event_id


def test_eod_active_child_returns_residual_only_after_terminal_order_callback() -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_command_outcome_event_payload_v1,
        build_kernel_order_event_payload_v1,
    )
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    plugin = _plugin("SNIPER_MINIQMT")
    initial = plugin.initialize(_start_context("SNIPER_MINIQMT")).next_state
    tick = _tick(2)
    submitted = plugin.transition(
        state=initial,
        event=tick,
        services=_services(state=initial, event=tick, delivery_suffix="eod_submit"),
    )
    command = submitted.broker_commands[0]
    common = {
        "runtime_id": tick.runtime_id,
        "algo_instance_id": submitted.next_state.algo_instance_id,
        "parent_intent_id": "parent_sniper_miniqmt",
        "strategy_slot_id": "slot_sniper_miniqmt",
        "mapping_id": "mapping_sniper_eod",
        "command_id": command.command_id,
        "local_vt_orderid": command.local_vt_orderid,
        "broker_order_id": "broker_sniper_eod",
    }
    outcome_payload = build_kernel_command_outcome_event_payload_v1(
        receipt_id="mqoutcomercpt_sniper_eod",
        receipt_sha256="1" * 64,
        **common,
        command_type="SUBMIT_LIMIT",
        outcome="ACCEPTED",
        outbox_status="ACKED",
        outbox_row_version=3,
        outcome_receipt_sha256="2" * 64,
        outbox_terminal=True,
        order_terminal=False,
    )
    outcome = _event(
        sequence=3,
        event_type=EventTypeV2.COMMAND_OUTCOME,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        schema="miniqmt_command_outcome_v1",
        payload=outcome_payload.model_dump(mode="json"),
        source_identity={"receipt_id": "mqoutcomercpt_sniper_eod", "receipt_sha256": "1" * 64},
    )
    accepted = plugin.transition(
        state=submitted.next_state,
        event=outcome,
        services=_services(state=submitted.next_state, event=outcome, delivery_suffix="eod_outcome"),
    )
    eod = _event(
        sequence=4,
        event_type=EventTypeV2.EOD,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        schema="miniqmt_eod_event_v1",
        payload={"trade_date": "2026-07-28"},
        source_identity={
            "runtime_id": tick.runtime_id,
            "trade_date": "2026-07-28",
            "session_epoch": "session_k3a_am",
        },
    )
    stopping = plugin.transition(
        state=accepted.next_state,
        event=eod,
        services=_services(state=accepted.next_state, event=eod, delivery_suffix="eod"),
    )
    assert stopping.terminal_outcome is None
    assert stopping.broker_commands[0].command_type.value == "CANCEL_ORDER"
    assert thaw_json_v1(stopping.next_state.state)["status"] == "STOPPED"

    order_payload = build_kernel_order_event_payload_v1(
        raw_payload={"order_status": 54, "traded_volume": 0},
        order_event_id="order_event_sniper_eod_cancelled",
        requested_quantity=command.quantity,
        symbol="600000.SH",
        side="BUY",
        **common,
    )
    order = _event(
        sequence=5,
        event_type=EventTypeV2.ORDER,
        source=EventSourceV2.QMT_GATEWAY_CALLBACK,
        schema="miniqmt_order_event_v1",
        payload=order_payload.model_dump(mode="json"),
        source_identity={"order_event_id": "order_event_sniper_eod_cancelled"},
    )
    closed = plugin.transition(
        state=stopping.next_state,
        event=order,
        services=_services(state=stopping.next_state, event=order, delivery_suffix="eod_order"),
    )

    assert closed.terminal_outcome.value == "EXPIRED_WITH_RESIDUAL"
    closed_state = thaw_json_v1(closed.next_state.state)
    assert closed_state["status"] == "FINISHED"
    assert closed_state["active_orders"] == []


def test_plugin_transition_rejects_unsubscribed_event_instead_of_noop_success() -> None:
    from backend.execution_algos.vnpy_style.plugin_base import CurrentThreePluginError
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    plugin = _plugin("SNIPER_MINIQMT")
    initialized = plugin.initialize(_start_context("SNIPER_MINIQMT"))
    account = _event(
        sequence=2,
        event_type=EventTypeV2.ACCOUNT,
        source=EventSourceV2.QMT_OMS_PROJECTION,
        schema="miniqmt_account_projection_v1",
        payload={"available_cash": "100000"},
        source_identity={"projection_version": "1", "projection_sha256": "a" * 64},
    )

    with pytest.raises(CurrentThreePluginError, match="does not accept ACCOUNT"):
        plugin.transition(
            state=initialized.next_state,
            event=account,
            services=_services(
                state=initialized.next_state,
                event=account,
                delivery_suffix="unsubscribed_account",
            ),
        )


def test_twap_eod_cancels_durable_timer_before_residual_terminal() -> None:
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    plugin = _plugin("TWAP_LITE_MINIQMT")
    initialized = plugin.initialize(_start_context("TWAP_LITE_MINIQMT"))
    eod = _event(
        sequence=2,
        event_type=EventTypeV2.EOD,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        schema="miniqmt_eod_event_v1",
        payload={"trade_date": "2026-07-28"},
        source_identity={
            "runtime_id": "runtime_k3a_plugins",
            "trade_date": "2026-07-28",
            "session_epoch": "session_k3a_am",
        },
    )

    terminal = plugin.transition(
        state=initialized.next_state,
        event=eod,
        services=_services(state=initialized.next_state, event=eod, delivery_suffix="twap_eod"),
    )

    assert terminal.terminal_outcome.value == "EXPIRED_WITH_RESIDUAL"
    assert len(terminal.timer_mutations) == 1
    assert terminal.timer_mutations[0].mutation_type.value == "CANCEL"
    assert terminal.timer_mutations[0].schedule_id == initialized.timer_mutations[0].schedule_id


def test_eod_late_submit_acceptance_and_cancel_rejection_continue_automatic_closure() -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_command_outcome_event_payload_v1,
    )
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    plugin = _plugin("SNIPER_MINIQMT")
    initial = plugin.initialize(_start_context("SNIPER_MINIQMT")).next_state
    tick = _tick(2)
    submitted = plugin.transition(
        state=initial,
        event=tick,
        services=_services(state=initial, event=tick, delivery_suffix="late_submit"),
    )
    submit = submitted.broker_commands[0]
    eod = _event(
        sequence=3,
        event_type=EventTypeV2.EOD,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        schema="miniqmt_eod_event_v1",
        payload={"trade_date": "2026-07-28"},
        source_identity={
            "runtime_id": tick.runtime_id,
            "trade_date": "2026-07-28",
            "session_epoch": "session_k3a_am",
        },
    )
    stopped = plugin.transition(
        state=submitted.next_state,
        event=eod,
        services=_services(state=submitted.next_state, event=eod, delivery_suffix="late_eod"),
    )
    assert stopped.broker_commands == ()
    assert thaw_json_v1(stopped.next_state.state)["status"] == "STOPPED"

    submit_payload = build_kernel_command_outcome_event_payload_v1(
        receipt_id="mqoutcomercpt_late_submit",
        receipt_sha256="1" * 64,
        runtime_id=tick.runtime_id,
        algo_instance_id=stopped.next_state.algo_instance_id,
        parent_intent_id="parent_sniper_miniqmt",
        strategy_slot_id="slot_sniper_miniqmt",
        mapping_id="mapping_late_submit",
        command_id=submit.command_id,
        command_type="SUBMIT_LIMIT",
        local_vt_orderid=submit.local_vt_orderid,
        broker_order_id="broker_late_submit",
        outcome="ACCEPTED",
        outbox_status="ACKED",
        outbox_row_version=3,
        outcome_receipt_sha256="2" * 64,
        outbox_terminal=True,
        order_terminal=False,
    )
    submit_outcome = _event(
        sequence=4,
        event_type=EventTypeV2.COMMAND_OUTCOME,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        schema="miniqmt_command_outcome_v1",
        payload=submit_payload.model_dump(mode="json"),
        source_identity={"receipt_id": submit_payload.receipt_id, "receipt_sha256": submit_payload.receipt_sha256},
    )
    canceling = plugin.transition(
        state=stopped.next_state,
        event=submit_outcome,
        services=_services(state=stopped.next_state, event=submit_outcome, delivery_suffix="late_accept"),
    )
    assert len(canceling.broker_commands) == 1
    first_cancel = canceling.broker_commands[0]
    assert first_cancel.command_type.value == "CANCEL_ORDER"
    assert first_cancel.owned_broker_order_id == "broker_late_submit"

    cancel_payload = build_kernel_command_outcome_event_payload_v1(
        receipt_id="mqoutcomercpt_late_cancel_rejected",
        receipt_sha256="3" * 64,
        runtime_id=tick.runtime_id,
        algo_instance_id=canceling.next_state.algo_instance_id,
        parent_intent_id="parent_sniper_miniqmt",
        strategy_slot_id="slot_sniper_miniqmt",
        mapping_id="mapping_late_submit",
        command_id=first_cancel.command_id,
        command_type="CANCEL_ORDER",
        local_vt_orderid=first_cancel.local_vt_orderid,
        broker_order_id="broker_late_submit",
        outcome="REJECTED",
        outbox_status="ACKED_REJECTED",
        outbox_row_version=3,
        outcome_receipt_sha256="4" * 64,
        outbox_terminal=True,
        order_terminal=False,
    )
    cancel_outcome = _event(
        sequence=5,
        event_type=EventTypeV2.COMMAND_OUTCOME,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        schema="miniqmt_command_outcome_v1",
        payload=cancel_payload.model_dump(mode="json"),
        source_identity={"receipt_id": cancel_payload.receipt_id, "receipt_sha256": cancel_payload.receipt_sha256},
    )
    retried = plugin.transition(
        state=canceling.next_state,
        event=cancel_outcome,
        services=_services(state=canceling.next_state, event=cancel_outcome, delivery_suffix="late_cancel_reject"),
    )
    assert len(retried.broker_commands) == 1
    assert retried.broker_commands[0].command_type.value == "CANCEL_ORDER"
    assert retried.broker_commands[0].command_id != first_cancel.command_id
    active = thaw_json_v1(retried.next_state.state)["active_orders"][0]
    assert active["status"] == "CANCEL_PENDING"
    assert active["pending_command_id"] == retried.broker_commands[0].command_id


def test_plugin_base_dispatch_and_identity_failures_are_explicit() -> None:
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    plugin = _plugin("SNIPER_MINIQMT")
    initial = plugin.initialize(_start_context("SNIPER_MINIQMT")).next_state
    with pytest.raises(TypeError, match="snapshot"):
        plugin.restore_state(object())

    wrong_manifest_state = initial.model_copy(update={"plugin_manifest_sha256": "0" * 64})
    with pytest.raises(ValueError, match="manifest"):
        plugin.restore_state(wrong_manifest_state)

    wrong_algo_payload = thaw_json_v1(initial.state)
    wrong_algo_payload["algo_code"] = "BEST_LIMIT_MINIQMT"
    wrong_algo_values = initial.model_dump(mode="python")
    wrong_algo_values.update(
        state=wrong_algo_payload,
        state_sha256=hash_hex_v1("execution_algo_state_v2", wrong_algo_payload),
    )
    wrong_algo_state = type(initial).model_validate(wrong_algo_values)
    tick = _tick(2)
    with pytest.raises(ValueError, match="algorithm identity"):
        plugin.transition(
            state=wrong_algo_state,
            event=tick,
            services=_services(state=wrong_algo_state, event=tick, delivery_suffix="wrong_algo"),
        )

    session = _event(
        sequence=2,
        event_type=EventTypeV2.SESSION,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        schema="miniqmt_session_event_v1",
        payload={"session_phase": "CONTINUOUS_AM"},
        source_identity={"session_event_id": "session_event_observed_k3a"},
    )
    observed = plugin.transition(
        state=initial,
        event=session,
        services=_services(state=initial, event=session, delivery_suffix="session_observed"),
    )
    assert observed.diagnostic_observations[0].reason_code == "K3_SESSION_OBSERVED"

    timer = _timer(2, "timer_occurrence_sniper_invalid")
    with pytest.raises(ValueError, match="does not accept TIMER"):
        plugin.transition(
            state=initial,
            event=timer,
            services=_services(state=initial, event=timer, delivery_suffix="timer_invalid"),
        )

    eod = _event(
        sequence=2,
        event_type=EventTypeV2.EOD,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        schema="miniqmt_eod_event_v1",
        payload={"trade_date": "2026-07-28"},
        source_identity={
            "runtime_id": "runtime_k3a_plugins",
            "trade_date": "2026-07-28",
            "session_epoch": "session_k3a_am",
        },
    )
    terminal = plugin.transition(
        state=initial,
        event=eod,
        services=_services(state=initial, event=eod, delivery_suffix="empty_eod"),
    )
    assert terminal.terminal_outcome.value == "EXPIRED_WITH_RESIDUAL"


@pytest.mark.parametrize(
    "payload,match",
    [
        (
            {
                "session_phase": "CONTINUOUS_AM",
                "exchange_time_utc": "2026-07-28T01:30:02Z",
                "ask_price_1": "10",
                "ask_volume_1": 100,
            },
            "generation",
        ),
        (
            {
                "generation": 1,
                "session_phase": "LUNCH_BREAK",
                "exchange_time_utc": "2026-07-28T01:30:02Z",
                "ask_price_1": "10",
                "ask_volume_1": 100,
            },
            "continuous-session",
        ),
    ],
)
def test_tick_lineage_rejects_missing_generation_and_noncontinuous_phase(payload: dict[str, Any], match: str) -> None:
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    plugin = _plugin("SNIPER_MINIQMT")
    initial = plugin.initialize(_start_context("SNIPER_MINIQMT")).next_state
    event = _event(
        sequence=2,
        event_type=EventTypeV2.TICK,
        source=EventSourceV2.B0_QUOTE_V2,
        schema="miniqmt_market_data_view_v2",
        payload=payload,
        source_identity={"market_data_id": "market_invalid_lineage"},
    )
    with pytest.raises(ValueError, match=match):
        plugin.transition(
            state=initial,
            event=event,
            services=_services(state=initial, event=event, delivery_suffix=f"invalid_{match}"),
        )


def test_sell_quote_and_strict_volume_use_native_side_specific_fields() -> None:
    from backend.services.miniqmt_execution_runtime.plugin_contracts import SideV1

    plugin = _plugin("SNIPER_MINIQMT")
    initial = plugin.initialize(_start_context("SNIPER_MINIQMT", side=SideV1.SELL)).next_state
    tick = _tick(2, bid="10.03", ask="10.04", volume=200)
    submitted = plugin.transition(
        state=initial,
        event=tick,
        services=_services(state=initial, event=tick, delivery_suffix="sell_native_bid"),
    )
    assert submitted.broker_commands[0].price_decimal == "10"
    assert submitted.broker_commands[0].side is SideV1.SELL

    malformed = _event(
        sequence=3,
        event_type=tick.event_type,
        source=tick.source,
        schema=tick.payload_schema_version,
        payload={
            **thaw_json_v1(tick.payload),
            "bid_volume_1": "200",
        },
        source_identity={"market_data_id": "market_invalid_sell_volume"},
    )
    with pytest.raises(ValueError, match="nonnegative strict integer"):
        plugin.transition(
            state=initial,
            event=malformed,
            services=_services(state=initial, event=malformed, delivery_suffix="sell_invalid_volume"),
        )


def test_callback_after_active_state_closed_is_visible_and_trade_without_owner_fails() -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_order_event_payload_v1,
        build_kernel_trade_event_payload_v1,
    )
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    plugin = _plugin("SNIPER_MINIQMT")
    initial = plugin.initialize(_start_context("SNIPER_MINIQMT")).next_state
    common = {
        "runtime_id": "runtime_k3a_plugins",
        "algo_instance_id": initial.algo_instance_id,
        "parent_intent_id": "parent_sniper_miniqmt",
        "strategy_slot_id": "slot_sniper_miniqmt",
        "mapping_id": "mapping_callback_after_close",
        "command_id": "command_callback_after_close",
        "local_vt_orderid": "local_callback_after_close",
        "broker_order_id": "broker_callback_after_close",
        "symbol": "600000.SH",
        "side": "BUY",
    }
    order_payload = build_kernel_order_event_payload_v1(
        raw_payload={"order_status": 54, "traded_volume": 0},
        order_event_id="order_callback_after_close",
        requested_quantity=100,
        **common,
    )
    order = _event(
        sequence=2,
        event_type=EventTypeV2.ORDER,
        source=EventSourceV2.QMT_GATEWAY_CALLBACK,
        schema="miniqmt_order_event_v1",
        payload=order_payload.model_dump(mode="json"),
        source_identity={"order_event_id": order_payload.order_event_id},
    )
    observed = plugin.transition(
        state=initial,
        event=order,
        services=_services(state=initial, event=order, delivery_suffix="order_after_close"),
    )
    assert observed.diagnostic_observations[0].reason_code == "K3_ORDER_CALLBACK_PRECEDED"

    trade_payload = build_kernel_trade_event_payload_v1(
        raw_payload={"trade_id": "trade_callback_after_close"},
        trade_quantity=100,
        trade_price_decimal="10",
        **common,
    )
    trade = _event(
        sequence=2,
        event_type=EventTypeV2.TRADE,
        source=EventSourceV2.QMT_GATEWAY_CALLBACK,
        schema="miniqmt_trade_fact_v1",
        payload=trade_payload.model_dump(mode="json"),
        source_identity={"trade_id": trade_payload.trade_id},
    )
    with pytest.raises(ValueError, match="no exact active"):
        plugin.transition(
            state=initial,
            event=trade,
            services=_services(state=initial, event=trade, delivery_suffix="trade_after_close"),
        )
