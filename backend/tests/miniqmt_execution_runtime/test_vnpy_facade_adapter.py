from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.execution_algos.vnpy_compat.facade_adapter import (
    VnpyFacadeBackedPluginAdapterV1,
    state_mapping_set_sha256_v1,
    terminal_mapping_set_sha256_v1,
)
from backend.execution_algos.vnpy_compat.facade_characterization import (
    build_vnpy_facade_state_mappings_v1,
    build_vnpy_facade_terminal_mappings_v1,
    load_pinned_vnpy_algorithm_classes_v1,
)
from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAlgorithmBindingV1,
    VnpyFacadeActiveOrderV1,
    VnpyFacadeStateValueV1,
    VnpyFacadeStateEnvelopeV1,
    VnpyFacadeTransitionInputV1,
)
from backend.execution_algos.vnpy_compat.facade_projection import AlgoStatus, OrderData, Status
from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v2
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ActiveChildClosureStatusV1,
    EventSourceV2,
    EventTypeV2,
    RuntimeEventEnvelopeV2,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1


def _adapter() -> VnpyFacadeBackedPluginAdapterV1:
    algo_code = "SNIPER_MINIQMT"
    algorithm_class = load_pinned_vnpy_algorithm_classes_v1()[algo_code]
    state = tuple(item for item in build_vnpy_facade_state_mappings_v1() if item.algo_code == algo_code)
    terminal = tuple(item for item in build_vnpy_facade_terminal_mappings_v1() if item.algo_code == algo_code)
    binding = VnpyFacadeAlgorithmBindingV1.create(
        algo_code=algo_code,
        source_identity_sha256=state[0].source_identity_sha256,
        class_ref=f"{algorithm_class.__module__}:{algorithm_class.__qualname__}",
        constructor_signature_sha256="a" * 64,
        constructor_body_sha256="b" * 64,
        state_mapping_set_sha256=state_mapping_set_sha256_v1(state),
        terminal_mapping_set_sha256=terminal_mapping_set_sha256_v1(terminal),
        characterization_receipt_sha256="c" * 64,
        adapter_contract_sha256="d" * 64,
    )
    manifest = next(item for item in current_three_manifests_v2() if item.algo_code == algo_code)
    return VnpyFacadeBackedPluginAdapterV1(
        manifest=manifest,
        algorithm_class=algorithm_class,
        algorithm_binding=binding,
        state_mappings=state,
        terminal_mappings=terminal,
    )


def test_adapter_is_sealed_and_direct_spi_bypass_fails_loud() -> None:
    adapter = _adapter()

    with pytest.raises(ValueError, match="initialize_with_facade"):
        adapter.initialize(None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="exact transition input"):
        adapter.restore_state(None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="transition_with_facade"):
        adapter.transition(state=None, event=None, services=None)  # type: ignore[arg-type]
    with pytest.raises(AttributeError, match="immutable"):
        adapter.manifest = current_three_manifests_v2()[0]


def test_adapter_rejects_class_and_mapping_identity_drift() -> None:
    adapter = _adapter()
    state = adapter._state_mappings  # exact read-only test of sealed process binding
    terminal = adapter._terminal_mappings
    binding = adapter._algorithm_binding

    with pytest.raises(ValueError, match="class identity"):
        VnpyFacadeBackedPluginAdapterV1(
            manifest=adapter.manifest,
            algorithm_class=dict,
            algorithm_binding=binding,
            state_mappings=state,
            terminal_mappings=terminal,
        )

    with pytest.raises(ValueError, match="terminal mapping set"):
        VnpyFacadeBackedPluginAdapterV1(
            manifest=adapter.manifest,
            algorithm_class=adapter._algorithm_class,
            algorithm_binding=binding,
            state_mappings=state,
            terminal_mappings=terminal[1:],
        )
    with pytest.raises(ValueError, match="state mapping set"):
        VnpyFacadeBackedPluginAdapterV1(
            manifest=adapter.manifest,
            algorithm_class=adapter._algorithm_class,
            algorithm_binding=binding,
            state_mappings=state[1:],
            terminal_mappings=terminal,
        )


def test_mapping_sets_reject_noncanonical_or_duplicate_input() -> None:
    adapter = _adapter()
    state = adapter._state_mappings
    terminal = adapter._terminal_mappings

    with pytest.raises(ValueError, match="unique and canonically sorted"):
        state_mapping_set_sha256_v1((state[1], state[0], *state[2:]))
    with pytest.raises(ValueError, match="unique and canonically sorted"):
        state_mapping_set_sha256_v1((state[0], state[0], *state[1:]))
    with pytest.raises(ValueError, match="unique and canonically sorted"):
        terminal_mapping_set_sha256_v1((terminal[0], terminal[0], *terminal[1:]))


def test_state_carrier_encoding_and_decoding_is_strict() -> None:
    adapter = _adapter()

    assert adapter._strict_state_value_v1(AlgoStatus.RUNNING)["member"] == "RUNNING"
    assert adapter._strict_state_value_v1("value") == "value"
    assert adapter._strict_state_value_v1(None) is None
    assert adapter._strict_state_value_v1(1.5) == "1.5"
    with pytest.raises(ValueError, match="non-finite"):
        adapter._strict_state_value_v1(float("nan"))
    with pytest.raises(ValueError, match="unsupported"):
        adapter._strict_state_value_v1([])

    status = VnpyFacadeStateValueV1.create(
        name="status",
        value={"enum_owner": "Status", "member": "PARTTRADED", "pinned_value": Status.PARTTRADED.value},
        value_type="Status",
    )
    assert adapter._decode_state_value_v1(status) is Status.PARTTRADED
    unsupported = VnpyFacadeStateValueV1.create(
        name="status",
        value={"enum_owner": "AlgoStatus", "member": "RUNNING", "pinned_value": AlgoStatus.RUNNING.value},
        value_type="AlgoStatus",
    )
    with pytest.raises(ValueError, match="enum owner"):
        adapter._decode_state_value_v1(unsupported)
    assert (
        adapter._decode_state_value_v1(VnpyFacadeStateValueV1.create(name="ratio", value="1.25", value_type="float"))
        == 1.25
    )
    assert adapter._decode_state_value_v1(VnpyFacadeStateValueV1.create(name="count", value=2, value_type="int")) == 2
    with pytest.raises(ValueError, match="wrong carrier type"):
        adapter._decode_state_value_v1(VnpyFacadeStateValueV1.create(name="count", value="2", value_type="int"))


def test_adapter_rejects_a_non_class_binding() -> None:
    adapter = _adapter()
    with pytest.raises(TypeError, match="must be a class"):
        VnpyFacadeBackedPluginAdapterV1(
            manifest=adapter.manifest,
            algorithm_class=None,  # type: ignore[arg-type]
            algorithm_binding=adapter._algorithm_binding,
            state_mappings=adapter._state_mappings,
            terminal_mappings=adapter._terminal_mappings,
        )


def _event(event_type: EventTypeV2, payload: dict[str, object]) -> RuntimeEventEnvelopeV2:
    source = EventSourceV2.EXCHANGE_SESSION_CLOCK
    schema = "miniqmt_session_event_v1"
    identity = {"session_event_id": "session_event_k4_callback"}
    monotonic_ns = None
    if event_type is EventTypeV2.TICK:
        source = EventSourceV2.B0_QUOTE_V2
        schema = "miniqmt_market_data_view_v2"
        identity = {"market_data_id": "market_k4_callback"}
    elif event_type is EventTypeV2.TIMER:
        schema = "miniqmt_timer_due_v1"
        identity = {"timer_occurrence_id": "timer_k4_callback"}
        monotonic_ns = 1
    elif event_type is EventTypeV2.ORDER:
        source = EventSourceV2.QMT_GATEWAY_CALLBACK
        schema = "miniqmt_order_event_v1"
        order_payload = {
            "order_event_id": "order_event_k4_callback",
            "runtime_id": "runtime_k4_callback",
            "algo_instance_id": "algo_k4_callback",
            "parent_intent_id": "parent_k4_callback",
            "strategy_slot_id": "slot_k4_callback",
            "mapping_id": "mapping_k4_callback",
            "command_id": "command_k4_callback",
            "broker_order_id": "broker_k4_callback",
            "symbol": "600000.SH",
            "side": "BUY",
            "observed_remaining_quantity": None,
            "terminal": False,
            "source_payload_sha256": "a" * 64,
            **payload,
        }
        order_payload["fact_sha256"] = hash_hex_v1("miniqmt_kernel_order_event_payload_v1", order_payload)
        payload = order_payload
        identity = {"order_event_id": "order_event_k4_callback"}
    elif event_type is EventTypeV2.TRADE:
        source = EventSourceV2.QMT_GATEWAY_CALLBACK
        schema = "miniqmt_trade_fact_v1"
        trade_payload = {
            "runtime_id": "runtime_k4_callback",
            "algo_instance_id": "algo_k4_callback",
            "parent_intent_id": "parent_k4_callback",
            "strategy_slot_id": "slot_k4_callback",
            "mapping_id": "mapping_k4_callback",
            "command_id": "command_k4_callback",
            "broker_order_id": "broker_k4_callback",
            "symbol": "600000.SH",
            "side": "BUY",
            "source_payload_sha256": "b" * 64,
            **payload,
        }
        trade_payload["fact_sha256"] = hash_hex_v1("miniqmt_kernel_trade_event_payload_v1", trade_payload)
        payload = trade_payload
        identity = {"trade_id": str(payload["trade_id"])}
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k4_callback",
        sequence=1,
        event_type=event_type,
        event_time_utc="2026-07-29T01:30:00Z",
        monotonic_ns=monotonic_ns,
        source=source,
        symbol="600000.SH",
        payload_schema_version=schema,
        payload=payload,
        source_identity=identity,
        correlation={},
    )


def test_callback_router_invokes_each_exact_callback_once_and_rejects_unknown() -> None:
    adapter = _adapter()

    class Algorithm:
        algo_name = "algo"

        def __init__(self) -> None:
            self.calls: list[tuple[str, object]] = []

        def update_tick(self, value: object) -> None:
            self.calls.append(("TICK", value))

        def update_timer(self) -> None:
            self.calls.append(("TIMER", None))

        def update_order(self, value: object) -> None:
            self.calls.append(("ORDER", value))

        def update_trade(self, value: object) -> None:
            self.calls.append(("TRADE", value))

    algorithm = Algorithm()
    facade = SimpleNamespace(get_tick=lambda owner: "tick")
    active = VnpyFacadeActiveOrderV1.create(
        local_vt_orderid="local_k4_callback",
        broker_order_id="broker_k4_callback",
        command_id="command_k4_callback",
        child_order_id="child_k4_callback",
        symbol="600000.SH",
        side="BUY",
        price_decimal="10",
        requested_quantity=100,
        cumulative_quantity=25,
        remaining_quantity=75,
        status="BROKER_ACCEPTED",
        last_order_event_id=None,
        last_trade_event_id=None,
    )
    before = SimpleNamespace(ordered_active_orders=(active,))

    adapter._invoke_callback_once_v1(
        algorithm=algorithm,
        event=_event(EventTypeV2.TICK, {}),
        facade=facade,
        before_envelope=before,
    )
    adapter._invoke_callback_once_v1(
        algorithm=algorithm,
        event=_event(EventTypeV2.TIMER, {}),
        facade=facade,
        before_envelope=before,
    )
    adapter._invoke_callback_once_v1(
        algorithm=algorithm,
        event=_event(
            EventTypeV2.ORDER,
            {
                "local_vt_orderid": active.local_vt_orderid,
                "normalized_order_status": "PARTIALLY_FILLED",
                "observed_cumulative_filled_quantity": None,
            },
        ),
        facade=facade,
        before_envelope=before,
    )
    adapter._invoke_callback_once_v1(
        algorithm=algorithm,
        event=_event(
            EventTypeV2.TRADE,
            {
                "local_vt_orderid": active.local_vt_orderid,
                "trade_id": "trade_k4_callback",
                "trade_price_decimal": "10",
                "trade_quantity": 25,
            },
        ),
        facade=facade,
        before_envelope=before,
    )

    assert [name for name, _ in algorithm.calls] == ["TICK", "TIMER", "ORDER", "TRADE"]
    with pytest.raises(ValueError, match="cumulative quantity authority"):
        adapter._invoke_callback_once_v1(
            algorithm=algorithm,
            event=_event(
                EventTypeV2.ORDER,
                {
                    "local_vt_orderid": "unknown",
                    "normalized_order_status": "ACCEPTED",
                    "observed_cumulative_filled_quantity": None,
                },
            ),
            facade=facade,
            before_envelope=SimpleNamespace(ordered_active_orders=()),
        )
    with pytest.raises(ValueError, match="not mapped"):
        adapter._invoke_callback_once_v1(
            algorithm=algorithm,
            event=_event(EventTypeV2.SESSION, {}),
            facade=facade,
            before_envelope=before,
        )


def test_active_order_state_uses_exact_order_and_trade_callback_facts() -> None:
    adapter = _adapter()
    active = VnpyFacadeActiveOrderV1.create(
        local_vt_orderid="local_k4_callback",
        broker_order_id="broker_k4_callback",
        command_id="command_k4_callback",
        child_order_id="child_k4_callback",
        symbol="600000.SH",
        side="BUY",
        price_decimal="10",
        requested_quantity=100,
        cumulative_quantity=25,
        remaining_quantity=75,
        status="BROKER_ACCEPTED",
        last_order_event_id=None,
        last_trade_event_id=None,
    )
    before = SimpleNamespace(ordered_active_orders=(active,))
    collector = SimpleNamespace(broker_commands=())

    order_event = _event(
        EventTypeV2.ORDER,
        {
            "local_vt_orderid": active.local_vt_orderid,
            "normalized_order_status": "PARTIALLY_FILLED",
            "observed_cumulative_filled_quantity": 30,
            "observed_remaining_quantity": 70,
        },
    )
    order_input = VnpyFacadeTransitionInputV1.model_construct(
        runtime_event=order_event,
        ordered_active_mappings=(),
    )
    order_algorithm = SimpleNamespace(
        active_orders={
            active.local_vt_orderid: OrderData(
                vt_orderid=active.local_vt_orderid,
                status=Status.PARTTRADED,
                traded=30,
                price=10,
            )
        }
    )
    order_state = adapter._active_orders_v1(
        algorithm=order_algorithm,
        invocation_input=order_input,
        collector=collector,
        before_envelope=before,
    )
    assert (order_state[0].cumulative_quantity, order_state[0].remaining_quantity) == (30, 70)
    assert order_state[0].last_order_event_id == order_event.event_id

    trade_event = _event(
        EventTypeV2.TRADE,
        {
            "local_vt_orderid": active.local_vt_orderid,
            "trade_id": "trade_k4_callback",
            "trade_price_decimal": "10",
            "trade_quantity": 10,
        },
    )
    trade_input = VnpyFacadeTransitionInputV1.model_construct(
        runtime_event=trade_event,
        ordered_active_mappings=(),
    )
    trade_algorithm = SimpleNamespace(
        active_orders={
            active.local_vt_orderid: OrderData(
                vt_orderid=active.local_vt_orderid,
                status=Status.PARTTRADED,
                traded=25,
                price=10,
            )
        }
    )
    trade_state = adapter._active_orders_v1(
        algorithm=trade_algorithm,
        invocation_input=trade_input,
        collector=collector,
        before_envelope=before,
    )
    assert (trade_state[0].cumulative_quantity, trade_state[0].remaining_quantity) == (35, 65)
    assert trade_state[0].last_trade_event_id == trade_event.event_id

    regressed_event = _event(
        EventTypeV2.ORDER,
        {
            "local_vt_orderid": active.local_vt_orderid,
            "normalized_order_status": "PARTIALLY_FILLED",
            "observed_cumulative_filled_quantity": 20,
            "observed_remaining_quantity": 80,
        },
    )
    with pytest.raises(ValueError, match="regressed"):
        adapter._active_orders_v1(
            algorithm=order_algorithm,
            invocation_input=VnpyFacadeTransitionInputV1.model_construct(
                runtime_event=regressed_event,
                ordered_active_mappings=(),
            ),
            collector=collector,
            before_envelope=before,
        )


def test_terminal_outcome_requires_exact_clean_active_child_closure() -> None:
    adapter = _adapter()
    envelope = VnpyFacadeStateEnvelopeV1.model_construct(
        ordered_active_orders=(),
        traded_volume_decimal="100",
        target_volume_decimal="100",
        status_member="FINISHED",
    )
    event = _event(
        EventTypeV2.TRADE,
        {
            "local_vt_orderid": "local_k4_callback",
            "trade_id": "trade_k4_terminal",
            "trade_price_decimal": "10",
            "trade_quantity": 100,
        },
    )

    assert adapter._terminal_outcome_v1(envelope, event, ActiveChildClosureStatusV1.CLEAN).value == "FILLED"
    assert (
        adapter._terminal_outcome_v1(
            envelope,
            event,
            ActiveChildClosureStatusV1.OUTCOME_UNKNOWN,
        )
        is None
    )
    with pytest.raises(TypeError, match="ActiveChildClosureStatusV1"):
        adapter._terminal_outcome_v1(envelope, event, "CLEAN")  # type: ignore[arg-type]
