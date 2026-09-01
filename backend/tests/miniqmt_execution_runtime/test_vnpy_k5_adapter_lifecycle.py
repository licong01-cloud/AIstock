"""Real K5 facade-adapter initialization and state-codec coverage."""

from __future__ import annotations

import json

import pytest

from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAuthorityInputV2,
    VnpyFacadeContractError,
    VnpyFacadeInitializationInputV2,
    VnpyFacadeStateEnvelopeV1,
    VnpyFacadeStateValueV1,
    VnpyFacadeTransitionInputV2,
)
from backend.execution_algos.vnpy_compat.k5_plugin_factories import (
    create_iceberg_plugin_v1,
    create_stop_plugin_v1,
)
from backend.execution_algos.vnpy_compat.k5_plugin_manifests import (
    validate_iceberg_state_v1,
    validate_stop_state_v1,
)
from backend.services.miniqmt_execution_runtime.k5_shadow_catalog import (
    build_k5_shadow_catalog_runtime_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
    build_kernel_order_event_payload_v1,
    build_kernel_trade_event_payload_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_creation import KernelAlgoCreationCoordinatorV1
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    AlgoReadOnlyServicesV1,
    AlgoStateSnapshotV2,
    AlgoStartContextV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandTypeV2,
    CommandChildMappingStatusV1,
    DeliveryStatusV1,
    DeterministicExecutionContextV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    EventSourceV2,
    EventTypeV2,
    GatewayCapabilityCatalogV1,
    MarketDataCapabilityV1,
    KernelCommandLifecycleProjectionItemV1,
    KernelCommandLifecycleProjectionV1,
    KernelProjectionTypeV1,
    OrderTypeV1,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
    SideV1,
    MiniQMTPluginContractError,
    MiniQMTPluginReasonCode,
    _algo_instance_id_v2,
    kernel_lease_fence_token_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import PluginRouteCompatibilityReceiptV1
from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    KernelPluginInvocationError,
    invoke_plugin_initialize_v1,
    invoke_plugin_transition_v1,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_creation import _CapturingRepository, _request


def _gateway() -> GatewayCapabilityCatalogV1:
    values = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": "route.k5.lifecycle.test",
        "quote_source": "B0_QUOTE_V2",
        "gateway_backend": "minqmt_sim",
        "order_types": tuple(sorted(OrderTypeV1, key=lambda item: item.value)),
        "market_data_capabilities": tuple(sorted(MarketDataCapabilityV1, key=lambda item: item.value)),
        "session_phases": tuple(sorted(SessionPhaseV1, key=lambda item: item.value)),
        "idempotent_submit_by_client_ref": False,
        "exact_order_id_cancel": True,
    }
    return GatewayCapabilityCatalogV1(
        **values,
        catalog_sha256=hash_hex_v1(
            "miniqmt_gateway_capability_catalog_v1",
            {
                **values,
                "order_types": [item.value for item in values["order_types"]],
                "market_data_capabilities": [item.value for item in values["market_data_capabilities"]],
                "session_phases": [item.value for item in values["session_phases"]],
            },
        ),
    )


def _initialization(
    algo_code: str,
    config: dict[str, object],
    *,
    side: SideV1 = SideV1.BUY,
    limit_price_decimal: str = "10.01",
) -> tuple[object, object]:
    gateway = _gateway()
    candidate = build_k5_shadow_catalog_runtime_v1(gateway_catalog=gateway)
    descriptor = next(
        item
        for item in candidate.catalog_runtime.snapshot.registration_descriptors
        if item.manifest.algo_code == algo_code
    )
    manifest = descriptor.manifest
    receipt = next(
        item
        for item in candidate.catalog_runtime.snapshot.pinned_compatibility_receipts
        if item.plugin_key == descriptor.plugin_key
    )
    route = PluginRouteCompatibilityReceiptV1.create(
        catalog_snapshot=candidate.catalog_runtime.snapshot,
        plugin_key=descriptor.plugin_key,
        gateway_catalog=gateway,
    ).validate_against_authority_v1(
        catalog_snapshot=candidate.catalog_runtime.snapshot,
        gateway_catalog=gateway,
    )
    authority = VnpyFacadeAuthorityInputV2.create(
        conformance_authority=candidate.conformance_authority,
        plugin_catalog_snapshot=candidate.catalog_runtime.snapshot,
        gateway_capability_catalog=gateway,
        plugin_key=descriptor.plugin_key,
        manifest=manifest,
        pinned_compatibility_receipt=receipt,
        route_compatibility_receipt=route,
    )
    config_sha256 = hash_hex_v1("miniqmt_plugin_config_v2", config)
    runtime_id = f"runtime_k5_{algo_code.lower()}"
    parent_intent_id = f"parent_k5_{algo_code.lower()}"
    strategy_slot_id = f"slot_k5_{algo_code.lower()}"
    algo_instance_id = _algo_instance_id_v2(
        runtime_id=runtime_id,
        parent_intent_id=parent_intent_id,
        strategy_slot_id=strategy_slot_id,
        algo_code=algo_code,
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        plugin_manifest_sha256=manifest.manifest_sha256,
        plugin_config_sha256=config_sha256,
    )
    event = RuntimeEventEnvelopeV2.create(
        runtime_id=runtime_id,
        sequence=1,
        event_type=EventTypeV2.ALGO_START,
        event_time_utc="2026-07-31T01:20:00Z",
        monotonic_ns=None,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        symbol="600000.SH",
        payload_schema_version="miniqmt_algo_start_v1",
        payload={
            "parent_intent_id": parent_intent_id,
            "strategy_slot_id": strategy_slot_id,
            "target_quantity": 100,
            "execution_plan_id": "plan_k5",
            "execution_plan_sha256": "a" * 64,
            "release_id": "release_k5",
            "release_sha256": "b" * 64,
            "policy_id": "policy_k5",
            "policy_sha256": "c" * 64,
            "route_receipt_sha256": route.receipt_sha256,
            "route_compatibility_receipt": route.model_dump(mode="json"),
            "gateway_capability_catalog": gateway.model_dump(mode="json"),
            "plugin_catalog_sha256": candidate.catalog_runtime.snapshot.catalog_sha256,
        },
        source_identity={
            "algo_instance_id": algo_instance_id,
            "runtime_id": runtime_id,
            "parent_intent_id": parent_intent_id,
            "strategy_slot_id": strategy_slot_id,
            "algo_code": algo_code,
            "plugin_id": manifest.plugin_id,
            "plugin_version": manifest.plugin_version,
            "plugin_manifest_sha256": manifest.manifest_sha256,
            "plugin_config_sha256": config_sha256,
        },
        correlation={},
    )
    delivery = AlgoEventDeliveryV1.create(
        event=event,
        algo_instance_id=algo_instance_id,
        plugin_manifest_sha256=manifest.manifest_sha256,
        algo_delivery_sequence=1,
        previous_delivery_id=None,
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc=event.event_time_utc,
        updated_at_utc=event.event_time_utc,
    )
    deterministic = DeterministicExecutionContextV1.create(
        runtime_id=runtime_id,
        algo_instance_id=algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        plugin_manifest_sha256=manifest.manifest_sha256,
        transition_sequence=1,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-31",
        session_epoch="session_k5_test",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="9" * 64,
    )
    contract = {
        "symbol": "600000.SH",
        "gateway_name": "minqmt_sim",
        "min_volume": "100",
        "volume_increment": "100",
        "pricetick_decimal": "0.01",
    }
    account = {"account_group_id": "sim_account"}
    start_context = AlgoStartContextV1(
        schema_version="miniqmt_algo_start_context_v1",
        runtime_id=runtime_id,
        algo_instance_id=algo_instance_id,
        parent_intent_id=parent_intent_id,
        strategy_slot_id=strategy_slot_id,
        symbol="600000.SH",
        side=side,
        limit_price_decimal=limit_price_decimal,
        parent_quantity=100,
        min_volume=100,
        volume_increment=100,
        plugin_manifest=manifest,
        plugin_config=config,
        plugin_config_sha256=config_sha256,
        start_event_id=event.event_id,
        start_delivery_id=delivery.delivery_id,
        deterministic_context=deterministic,
        contract_projection=contract,
        contract_projection_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
        account_projection=account,
        account_projection_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
        market_capability_projection=gateway.model_dump(mode="json"),
        market_capability_projection_sha256=hash_hex_v1(
            "miniqmt_market_capability_projection_v1", gateway.model_dump(mode="json")
        ),
        execution_plan_id="plan_k5",
        execution_plan_sha256="a" * 64,
        release_id="release_k5",
        release_sha256="b" * 64,
        policy_id="policy_k5",
        policy_sha256="c" * 64,
    )
    return manifest, VnpyFacadeInitializationInputV2.create(
        start_event=event,
        start_delivery=delivery,
        start_context=start_context,
        authority_input=authority,
    )


def _reserved_mapping(command, *, strategy_slot_id: str, logical_at_utc: str) -> ExecutionCommandChildMappingV1:
    return ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id=strategy_slot_id,
        mapping_status=CommandChildMappingStatusV1.RESERVED,
        mapping_version=1,
        broker_order_id=None,
        broker_identity_source_event_id=None,
        last_order_event_id=None,
        last_trade_event_id=None,
        updated_by_event_id=None,
        created_at_utc=logical_at_utc,
        updated_at_utc=logical_at_utc,
    )


def _mapping_with_broker(
    command,
    *,
    strategy_slot_id: str,
    logical_at_utc: str,
    broker_order_id: str,
    terminal_order_event_id: str | None = None,
) -> ExecutionCommandChildMappingV1:
    return ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id=strategy_slot_id,
        mapping_status=(
            CommandChildMappingStatusV1.TERMINAL
            if terminal_order_event_id is not None
            else CommandChildMappingStatusV1.BROKER_ACCEPTED
        ),
        mapping_version=2 if terminal_order_event_id is None else 3,
        broker_order_id=broker_order_id,
        broker_identity_source_event_id="event_k5_broker_accept",
        last_order_event_id=terminal_order_event_id,
        last_trade_event_id=None,
        updated_by_event_id=terminal_order_event_id or "event_k5_broker_accept",
        created_at_utc=logical_at_utc,
        updated_at_utc=logical_at_utc,
    )


def _transition_input(
    *,
    initialization: VnpyFacadeInitializationInputV2,
    before_state: AlgoStateSnapshotV2,
    event_type: EventTypeV2,
    sequence: int,
    market_overrides: dict[str, object] | None = None,
    mappings: tuple[ExecutionCommandChildMappingV1, ...] = (),
    mapping_commands: dict[str, object] | None = None,
    callback_payload: object | None = None,
    include_market: bool = True,
    session_phase: SessionPhaseV1 = SessionPhaseV1.CONTINUOUS_AM,
) -> VnpyFacadeTransitionInputV2:
    context = initialization.start_context
    mappings = tuple(sorted(mappings, key=lambda item: item.local_vt_orderid))
    event_time = f"2026-07-31T01:{20 + sequence:02d}:00Z"
    timer_payload = {
        "timer_occurrence_id": f"timer_occurrence_k5_{sequence}",
        "schedule_id": "timer_schedule_k5",
        "algo_instance_id": context.algo_instance_id,
        "timer_name": "K5_TIMER",
        "schedule_epoch": 1,
        "due_at_exchange_utc": event_time,
        "effective_due_at_exchange_utc": event_time,
        "catch_up_policy": "APPLY_ONCE",
        "timer_payload": {},
        "timer_payload_sha256": hash_hex_v1("miniqmt_timer_mutation_payload_v1", {}),
        "exchange_session_authority_sha256": "7" * 64,
    }
    if event_type is EventTypeV2.TICK:
        event_source = EventSourceV2.B0_QUOTE_V2
        event_symbol = context.symbol
        event_schema = "miniqmt_market_data_view_v2"
        event_source_identity = {"market_data_id": f"market_k5_{sequence}"}
    elif event_type is EventTypeV2.TIMER:
        event_source = EventSourceV2.EXCHANGE_SESSION_CLOCK
        event_symbol = None
        event_schema = "miniqmt_timer_due_v1"
        event_source_identity = {"timer_occurrence_id": timer_payload["timer_occurrence_id"]}
    elif event_type is EventTypeV2.ORDER:
        event_source = EventSourceV2.QMT_GATEWAY_CALLBACK
        event_symbol = context.symbol
        event_schema = "miniqmt_order_event_v1"
        event_source_identity = {"order_event_id": callback_payload.order_event_id}
    elif event_type is EventTypeV2.TRADE:
        event_source = EventSourceV2.QMT_GATEWAY_CALLBACK
        event_symbol = context.symbol
        event_schema = "miniqmt_trade_fact_v1"
        event_source_identity = {"trade_id": callback_payload.trade_id}
    else:
        raise AssertionError(f"unsupported K5 test event type {event_type.value}")
    initial_event_payload = callback_payload.canonical_payload_v1() if callback_payload is not None else timer_payload
    provisional = RuntimeEventEnvelopeV2.create(
        runtime_id=context.runtime_id,
        sequence=sequence,
        event_type=event_type,
        event_time_utc=event_time,
        monotonic_ns=sequence,
        source=event_source,
        symbol=event_symbol,
        payload_schema_version=event_schema,
        payload=initial_event_payload,
        source_identity=event_source_identity,
        correlation={},
    )
    source_event_id = provisional.event_id if event_type is EventTypeV2.TICK else f"mqrtevt_prior_k5_{sequence}"
    market = {
        "market_data_id": f"market_k5_{sequence}",
        "source_event_id": source_event_id,
        "generation": 1,
        "source_sequence": sequence if event_type is EventTypeV2.TICK else sequence - 1,
        "exchange_time_utc": event_time,
        "symbol": context.symbol,
        "logical_at_utc": event_time,
        "bid_price_1": "10",
        "bid_volume_1": 500,
        "ask_price_1": "10",
        "ask_volume_1": 500,
        "last_price": "10.02",
        "limit_up": "11",
        "limit_down": "9",
        "exchange_trade_date": "2026-07-31",
        "session_epoch": "session_k5_test",
        "session_phase": session_phase.value,
        "quote_source": "B0_QUOTE_V2",
    }
    if market_overrides:
        market.update(market_overrides)
    event = RuntimeEventEnvelopeV2.create(
        runtime_id=context.runtime_id,
        sequence=sequence,
        event_type=event_type,
        event_time_utc=event_time,
        monotonic_ns=sequence,
        source=event_source,
        symbol=event_symbol,
        payload_schema_version=event_schema,
        payload=(
            market
            if event_type is EventTypeV2.TICK
            else callback_payload.canonical_payload_v1()
            if callback_payload is not None
            else timer_payload
        ),
        source_identity=(
            {"market_data_id": market["market_data_id"]} if event_type is EventTypeV2.TICK else event_source_identity
        ),
        correlation={},
    )
    pending = AlgoEventDeliveryV1.create(
        event=event,
        algo_instance_id=context.algo_instance_id,
        plugin_manifest_sha256=context.plugin_manifest.manifest_sha256,
        algo_delivery_sequence=sequence,
        previous_delivery_id=before_state.last_applied_delivery_id,
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc=event_time,
        updated_at_utc=event_time,
    )
    initial_delivery = AlgoDeliveryPersistenceV1.create(
        delivery=pending,
        lease_epoch=0,
        lease_fence_token=None,
        row_version=1,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        closed_at_utc=None,
    )
    lease_owner = "worker_k5:incarnation_k5"
    claimed = AlgoDeliveryPersistenceV1.model_validate(
        {
            **initial_delivery.model_dump(mode="python"),
            "status": DeliveryStatusV1.CLAIMED,
            "attempt_count": 1,
            "lease_owner": lease_owner,
            "lease_epoch": 1,
            "lease_fence_token": kernel_lease_fence_token_v1(
                owner_type="DELIVERY",
                owner_id=initial_delivery.delivery_id,
                lease_epoch=1,
                lease_owner=lease_owner,
            ),
            "lease_expires_at": "2026-07-31T02:00:00Z",
            "row_version": 2,
        },
        strict=True,
    )
    contract = thaw_json_v1(context.contract_projection)
    refs_list = [
        ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.CONTRACT,
            projection_id=f"contract_k5_{sequence}",
            projection_version="contract_v1",
            payload_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
            source_event_id=None,
            logical_at_utc=event_time,
        )
    ]
    if include_market:
        refs_list.append(
            ExecutionProjectionRefV1.create(
                projection_type=KernelProjectionTypeV1.MARKET_DATA,
                projection_id=f"market_k5_{sequence}",
                projection_version="market_v2",
                payload_sha256=hash_hex_v1("miniqmt_market_data_projection_v2", market),
                source_event_id=source_event_id,
                logical_at_utc=event_time,
            )
        )
    refs = tuple(
        sorted(
            refs_list,
            key=lambda item: (item.projection_type.value, item.projection_id),
        )
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        projection_refs=refs,
    )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        contract_projection_id=f"contract_k5_{sequence}",
        contract_projection=contract,
        market_data_projection_id=f"market_k5_{sequence}" if include_market else None,
        market_data_projection=market if include_market else None,
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    deterministic = DeterministicExecutionContextV1.create(
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        plugin_manifest_sha256=context.plugin_manifest.manifest_sha256,
        transition_sequence=sequence,
        logical_time_utc=event_time,
        exchange_trade_date="2026-07-31",
        session_epoch="session_k5_test",
        session_phase=session_phase,
        input_projection_sha256=projection_set.projection_set_sha256,
    )
    envelope = VnpyFacadeStateEnvelopeV1.model_validate_json(
        json.dumps(thaw_json_v1(before_state.state), sort_keys=True, separators=(",", ":")), strict=True
    )
    traded = int(float(envelope.traded_volume_decimal))
    algo = ExecutionAlgoInstancePersistenceV2.create(
        algo_instance_id=context.algo_instance_id,
        runtime_id=context.runtime_id,
        parent_intent_id=context.parent_intent_id,
        strategy_slot_id=context.strategy_slot_id,
        symbol=context.symbol,
        side=context.side,
        target_quantity=context.parent_quantity,
        traded_quantity=traded,
        remaining_quantity=context.parent_quantity - traded,
        algo_code=context.plugin_manifest.algo_code,
        plugin_id=context.plugin_manifest.plugin_id,
        plugin_version=context.plugin_manifest.plugin_version,
        plugin_manifest_sha256=context.plugin_manifest.manifest_sha256,
        plugin_config_json=thaw_json_v1(context.plugin_config),
        plugin_config_sha256=context.plugin_config_sha256,
        compatibility_receipt_sha256=initialization.authority_input.pinned_compatibility_receipt.receipt_sha256,
        state_schema_version=before_state.state_schema_version,
        state_json=before_state.state,
        state_sha256=before_state.state_sha256,
        transition_sequence=sequence - 1,
        last_applied_delivery_sequence=sequence - 1,
        last_applied_delivery_id=before_state.last_applied_delivery_id,
        last_closed_delivery_sequence=sequence - 1,
        terminal_delivery_sequence=None,
        status=ExecutionAlgoPersistenceStatusV2.ACTIVE,
        failure_receipt_id=None,
        active_child_closure_status=ActiveChildClosureStatusV1.NOT_APPLICABLE,
        active_child_count=len(mappings),
        row_version=sequence - 1,
        created_at_utc=initialization.start_event.event_time_utc,
        updated_at_utc=event_time,
        terminal_at_utc=None,
        archived_at_utc=None,
    )
    commands = mapping_commands or {}
    lifecycle_items = []
    for mapping in mappings:
        current_command = commands[mapping.local_vt_orderid]
        lifecycle_items.append(
            KernelCommandLifecycleProjectionItemV1(
                mapping_id=mapping.mapping_id,
                mapping_version=mapping.mapping_version,
                mapping_payload_sha256=mapping.payload_sha256,
                local_vt_orderid=mapping.local_vt_orderid,
                submit_command_id=mapping.command_id,
                broker_order_id=mapping.broker_order_id,
                mapping_status=mapping.mapping_status,
                current_outbox_command_id=current_command.command_id,
                current_outbox_command_type=current_command.command_type,
                current_outbox_status=(
                    BrokerCommandOutboxStatusV1.PENDING
                    if mapping.mapping_status is CommandChildMappingStatusV1.RESERVED
                    or current_command.command_type is BrokerCommandTypeV2.CANCEL_ORDER
                    else BrokerCommandOutboxStatusV1.ACKED
                ),
                current_outbox_row_version=1,
                current_outbox_payload_sha256=current_command.payload_sha256,
                outcome_receipt_sha256=None,
                latest_command_outcome_event_id=None,
                latest_command_outcome_payload_sha256=None,
                command_outcome_delivery_id=None,
                command_outcome_delivery_status=None,
            )
        )
    lifecycle = KernelCommandLifecycleProjectionV1.create(
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        ordered_items=tuple(lifecycle_items),
    )
    return VnpyFacadeTransitionInputV2.create(
        runtime_event=event,
        claimed_delivery=claimed,
        algo_instance=algo,
        manifest=context.plugin_manifest,
        authority_input=initialization.authority_input,
        before_state=before_state,
        read_only_services=services,
        command_lifecycle_projection=lifecycle,
        ordered_active_mappings=mappings,
        deterministic_context=deterministic,
        transition_sequence=sequence,
    )


@pytest.mark.parametrize(
    ("algo_code", "config", "factory", "validator"),
    (
        ("ICEBERG", {"display_volume": 100, "interval": 1}, create_iceberg_plugin_v1, validate_iceberg_state_v1),
        ("STOP", {"price_add": "0.01"}, create_stop_plugin_v1, validate_stop_state_v1),
    ),
)
def test_k5_real_facade_initialize_preserves_exact_source_state_and_codec(
    algo_code: str,
    config: dict[str, object],
    factory,
    validator,
) -> None:
    manifest, invocation = _initialization(algo_code, config)
    adapter = factory(config)
    initialized = adapter.initialize_with_facade_v2(invocation)
    state = thaw_json_v1(initialized.next_state.state)

    assert not initialized.broker_commands
    assert not initialized.timer_mutations
    assert thaw_json_v1(validator(manifest, state)) == state
    values = {item["name"]: item["value"] for item in state["ordered_variables"]}
    if algo_code == "ICEBERG":
        assert values == {"timer_count": 0, "vt_orderid": ""}
    else:
        assert values == {"order_status": "", "vt_orderid": ""}


def test_iceberg_timer_uses_exchange_event_count_and_restarts_deterministically() -> None:
    _manifest, initialization = _initialization("ICEBERG", {"display_volume": 100, "interval": 2})
    initialized = create_iceberg_plugin_v1({"display_volume": 100, "interval": 2}).initialize_with_facade_v2(
        initialization
    )
    first_timer = _transition_input(
        initialization=initialization,
        before_state=initialized.next_state,
        event_type=EventTypeV2.TIMER,
        sequence=2,
    )
    first = create_iceberg_plugin_v1({"display_volume": 100, "interval": 2}).transition_with_facade_v2(first_timer)

    assert first.broker_commands == ()
    second_timer = _transition_input(
        initialization=initialization,
        before_state=first.next_state,
        event_type=EventTypeV2.TIMER,
        sequence=3,
    )
    adapter = create_iceberg_plugin_v1({"display_volume": 100, "interval": 2})
    submitted = adapter.transition_with_facade_v2(second_timer)
    restarted = create_iceberg_plugin_v1({"display_volume": 100, "interval": 2}).transition_with_facade_v2(second_timer)

    assert submitted == restarted
    assert len(submitted.broker_commands) == 1
    assert submitted.broker_commands[0].quantity == 100
    assert submitted.broker_commands[0].price_decimal == "10.01"


def test_iceberg_interval_zero_missing_tick_pm_and_rounded_zero_are_explicit() -> None:
    ready_config = {"display_volume": 100, "interval": 0}
    _manifest, initialization = _initialization("ICEBERG", ready_config)
    initialized = create_iceberg_plugin_v1(ready_config).initialize_with_facade_v2(initialization)
    missing_input = _transition_input(
        initialization=initialization,
        before_state=initialized.next_state,
        event_type=EventTypeV2.TIMER,
        sequence=2,
        include_market=False,
    )
    missing = create_iceberg_plugin_v1(ready_config).transition_with_facade_v2(missing_input)
    assert missing.broker_commands == ()
    assert "MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE" in {item.reason_code for item in missing.diagnostic_observations}

    pm_input = _transition_input(
        initialization=initialization,
        before_state=initialized.next_state,
        event_type=EventTypeV2.TIMER,
        sequence=2,
        session_phase=SessionPhaseV1.CONTINUOUS_PM,
    )
    pm = create_iceberg_plugin_v1(ready_config).transition_with_facade_v2(pm_input)
    assert len(pm.broker_commands) == 1

    rounded_config = {"display_volume": "0.5", "interval": 0}
    _manifest, rounded_initialization = _initialization("ICEBERG", rounded_config)
    rounded_initialized = create_iceberg_plugin_v1(rounded_config).initialize_with_facade_v2(rounded_initialization)
    rounded_input = _transition_input(
        initialization=rounded_initialization,
        before_state=rounded_initialized.next_state,
        event_type=EventTypeV2.TIMER,
        sequence=2,
    )
    rounded = create_iceberg_plugin_v1(rounded_config).transition_with_facade_v2(rounded_input)
    assert rounded.broker_commands == ()
    assert "MINIQMT_VNPY_FACADE_ROUNDED_VOLUME_ZERO" in {item.reason_code for item in rounded.diagnostic_observations}


def test_stop_native_tick_triggers_once_and_restart_does_not_rearm() -> None:
    manifest, initialization = _initialization("STOP", {"price_add": "0.01"})
    initialized = create_stop_plugin_v1({"price_add": "0.01"}).initialize_with_facade_v2(initialization)
    trigger = _transition_input(
        initialization=initialization,
        before_state=initialized.next_state,
        event_type=EventTypeV2.TICK,
        sequence=2,
        market_overrides={"last_price": "10.02"},
    )
    triggered = invoke_plugin_transition_v1(
        plugin=create_stop_plugin_v1({"price_add": "0.01"}),
        expected_manifest=manifest,
        state_codec=validate_stop_state_v1,
        state=trigger.before_state,
        event=trigger.runtime_event,
        services=trigger.read_only_services,
        deterministic_context=trigger.deterministic_context,
        facade_input=trigger,
    )

    assert len(triggered.broker_commands) == 1
    submit = triggered.broker_commands[0]
    mapping = _reserved_mapping(
        submit,
        strategy_slot_id=initialization.start_context.strategy_slot_id,
        logical_at_utc=trigger.deterministic_context.logical_time_utc,
    )
    repeated = _transition_input(
        initialization=initialization,
        before_state=triggered.next_state,
        event_type=EventTypeV2.TICK,
        sequence=3,
        market_overrides={"last_price": "10.50"},
        mappings=(mapping,),
        mapping_commands={mapping.local_vt_orderid: submit},
    )
    result = create_stop_plugin_v1({"price_add": "0.01"}).transition_with_facade_v2(repeated)
    restarted = create_stop_plugin_v1({"price_add": "0.01"}).transition_with_facade_v2(repeated)

    assert result == restarted
    assert result.broker_commands == ()


@pytest.mark.parametrize(
    ("side", "not_triggered_last", "triggered_last", "expected_price"),
    (
        (SideV1.BUY, "10", "12", "11"),
        (SideV1.SELL, "10.02", "8", "9"),
    ),
)
def test_stop_buy_sell_trigger_and_native_limit_bounds(
    side: SideV1,
    not_triggered_last: str,
    triggered_last: str,
    expected_price: str,
) -> None:
    config = {"price_add": "2"}
    _manifest, initialization = _initialization("STOP", config, side=side)
    initialized = create_stop_plugin_v1(config).initialize_with_facade_v2(initialization)
    not_triggered_input = _transition_input(
        initialization=initialization,
        before_state=initialized.next_state,
        event_type=EventTypeV2.TICK,
        sequence=2,
        market_overrides={"last_price": not_triggered_last},
    )
    not_triggered = create_stop_plugin_v1(config).transition_with_facade_v2(not_triggered_input)
    assert not_triggered.broker_commands == ()

    triggered_input = _transition_input(
        initialization=initialization,
        before_state=initialized.next_state,
        event_type=EventTypeV2.TICK,
        sequence=2,
        market_overrides={"last_price": triggered_last},
    )
    triggered = create_stop_plugin_v1(config).transition_with_facade_v2(triggered_input)
    assert len(triggered.broker_commands) == 1
    assert triggered.broker_commands[0].side is side
    assert triggered.broker_commands[0].price_decimal == expected_price


def test_iceberg_cancel_pending_resubmit_and_late_old_terminal_preserve_pinned_pointer_semantics() -> None:
    config = {"display_volume": 100, "interval": 1}
    _manifest, initialization = _initialization("ICEBERG", config)
    initialized = create_iceberg_plugin_v1(config).initialize_with_facade_v2(initialization)
    first_timer = _transition_input(
        initialization=initialization,
        before_state=initialized.next_state,
        event_type=EventTypeV2.TIMER,
        sequence=2,
    )
    submitted = create_iceberg_plugin_v1(config).transition_with_facade_v2(first_timer)
    first_submit = submitted.broker_commands[0]
    broker_order_id = "broker_k5_iceberg_old"
    accepted_mapping = _mapping_with_broker(
        first_submit,
        strategy_slot_id=initialization.start_context.strategy_slot_id,
        logical_at_utc=first_timer.deterministic_context.logical_time_utc,
        broker_order_id=broker_order_id,
    )
    common = {
        "runtime_id": first_submit.runtime_id,
        "algo_instance_id": first_submit.algo_instance_id,
        "parent_intent_id": first_submit.parent_intent_id,
        "strategy_slot_id": initialization.start_context.strategy_slot_id,
        "mapping_id": accepted_mapping.mapping_id,
        "command_id": first_submit.command_id,
        "local_vt_orderid": first_submit.local_vt_orderid,
        "broker_order_id": broker_order_id,
    }
    accepted_payload = build_kernel_order_event_payload_v1(
        raw_payload={"order_status": 48, "traded_volume": 0},
        order_event_id="order_event_k5_iceberg_accepted",
        requested_quantity=first_submit.quantity,
        symbol=first_submit.symbol,
        side=first_submit.side,
        **common,
    )
    accepted_input = _transition_input(
        initialization=initialization,
        before_state=submitted.next_state,
        event_type=EventTypeV2.ORDER,
        sequence=3,
        mappings=(accepted_mapping,),
        mapping_commands={accepted_mapping.local_vt_orderid: first_submit},
        callback_payload=accepted_payload,
    )
    accepted = create_iceberg_plugin_v1(config).transition_with_facade_v2(accepted_input)

    cancel_input = _transition_input(
        initialization=initialization,
        before_state=accepted.next_state,
        event_type=EventTypeV2.TIMER,
        sequence=4,
        market_overrides={"ask_price_1": "10"},
        mappings=(accepted_mapping,),
        mapping_commands={accepted_mapping.local_vt_orderid: first_submit},
    )
    cancelling = create_iceberg_plugin_v1(config).transition_with_facade_v2(cancel_input)
    cancel_command = cancelling.broker_commands[0]
    assert cancel_command.command_type is BrokerCommandTypeV2.CANCEL_ORDER
    cancel_state = VnpyFacadeStateEnvelopeV1.model_validate_json(
        json.dumps(thaw_json_v1(cancelling.next_state.state), sort_keys=True, separators=(",", ":")), strict=True
    )
    assert {item.name: thaw_json_v1(item.value) for item in cancel_state.ordered_variables}["vt_orderid"] == ""
    assert len(cancel_state.ordered_active_orders) == 1

    resubmit_input = _transition_input(
        initialization=initialization,
        before_state=cancelling.next_state,
        event_type=EventTypeV2.TIMER,
        sequence=5,
        market_overrides={"ask_price_1": "10.50"},
        mappings=(accepted_mapping,),
        mapping_commands={accepted_mapping.local_vt_orderid: cancel_command},
    )
    resubmitted = create_iceberg_plugin_v1(config).transition_with_facade_v2(resubmit_input)
    new_submit = resubmitted.broker_commands[0]
    assert new_submit.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
    assert new_submit.local_vt_orderid != first_submit.local_vt_orderid
    new_mapping = _reserved_mapping(
        new_submit,
        strategy_slot_id=initialization.start_context.strategy_slot_id,
        logical_at_utc=resubmit_input.deterministic_context.logical_time_utc,
    )

    old_terminal_mapping = _mapping_with_broker(
        first_submit,
        strategy_slot_id=initialization.start_context.strategy_slot_id,
        logical_at_utc=resubmit_input.deterministic_context.logical_time_utc,
        broker_order_id=broker_order_id,
        terminal_order_event_id="order_event_k5_iceberg_old_cancelled",
    )
    old_terminal_payload = build_kernel_order_event_payload_v1(
        raw_payload={"order_status": 54, "traded_volume": 0},
        order_event_id="order_event_k5_iceberg_old_cancelled",
        requested_quantity=first_submit.quantity,
        symbol=first_submit.symbol,
        side=first_submit.side,
        **common,
    )
    late_input = _transition_input(
        initialization=initialization,
        before_state=resubmitted.next_state,
        event_type=EventTypeV2.ORDER,
        sequence=6,
        mappings=(old_terminal_mapping, new_mapping),
        mapping_commands={
            old_terminal_mapping.local_vt_orderid: cancel_command,
            new_mapping.local_vt_orderid: new_submit,
        },
        callback_payload=old_terminal_payload,
    )
    late = create_iceberg_plugin_v1(config).transition_with_facade_v2(late_input)
    late_state = VnpyFacadeStateEnvelopeV1.model_validate_json(
        json.dumps(thaw_json_v1(late.next_state.state), sort_keys=True, separators=(",", ":")), strict=True
    )
    assert {item.name: thaw_json_v1(item.value) for item in late_state.ordered_variables}["vt_orderid"] == ""
    assert tuple(item.local_vt_orderid for item in late_state.ordered_active_orders) == (new_submit.local_vt_orderid,)


@pytest.mark.parametrize(
    ("algo_code", "config", "factory", "event_type"),
    (
        ("ICEBERG", {"display_volume": 100, "interval": 1}, create_iceberg_plugin_v1, EventTypeV2.TIMER),
        ("STOP", {"price_add": "0.01"}, create_stop_plugin_v1, EventTypeV2.TICK),
    ),
)
def test_k5_order_trade_partial_exact_terminal_overfill_and_restart(
    algo_code: str,
    config: dict[str, object],
    factory,
    event_type: EventTypeV2,
) -> None:
    _manifest, initialization = _initialization(algo_code, config)
    initialized = factory(config).initialize_with_facade_v2(initialization)
    trigger_input = _transition_input(
        initialization=initialization,
        before_state=initialized.next_state,
        event_type=event_type,
        sequence=2,
        market_overrides={"last_price": "10.02"},
    )
    submitted = factory(config).transition_with_facade_v2(trigger_input)
    command = submitted.broker_commands[0]
    broker_order_id = f"broker_k5_{algo_code.lower()}"
    accepted_mapping = _mapping_with_broker(
        command,
        strategy_slot_id=initialization.start_context.strategy_slot_id,
        logical_at_utc=trigger_input.deterministic_context.logical_time_utc,
        broker_order_id=broker_order_id,
    )
    common = {
        "runtime_id": command.runtime_id,
        "algo_instance_id": command.algo_instance_id,
        "parent_intent_id": command.parent_intent_id,
        "strategy_slot_id": initialization.start_context.strategy_slot_id,
        "mapping_id": accepted_mapping.mapping_id,
        "command_id": command.command_id,
        "local_vt_orderid": command.local_vt_orderid,
        "broker_order_id": broker_order_id,
    }
    order_payload = build_kernel_order_event_payload_v1(
        raw_payload={"order_status": 56, "traded_volume": command.quantity},
        order_event_id=f"order_event_k5_{algo_code.lower()}_terminal",
        requested_quantity=command.quantity,
        symbol=command.symbol,
        side=command.side,
        **common,
    )
    order_input = _transition_input(
        initialization=initialization,
        before_state=submitted.next_state,
        event_type=EventTypeV2.ORDER,
        sequence=3,
        mappings=(accepted_mapping,),
        mapping_commands={accepted_mapping.local_vt_orderid: command},
        callback_payload=order_payload,
    )
    order_result = factory(config).transition_with_facade_v2(order_input)
    assert order_result.terminal_outcome is None

    terminal_mapping = _mapping_with_broker(
        command,
        strategy_slot_id=initialization.start_context.strategy_slot_id,
        logical_at_utc=order_input.deterministic_context.logical_time_utc,
        broker_order_id=broker_order_id,
        terminal_order_event_id=order_payload.order_event_id,
    )
    partial_quantity = command.quantity - 1
    partial_payload = build_kernel_trade_event_payload_v1(
        raw_payload={"trade_id": f"trade_k5_{algo_code.lower()}_partial"},
        trade_quantity=partial_quantity,
        trade_price_decimal=command.price_decimal,
        symbol=command.symbol,
        side=command.side,
        **common,
    )
    partial_input = _transition_input(
        initialization=initialization,
        before_state=order_result.next_state,
        event_type=EventTypeV2.TRADE,
        sequence=4,
        mappings=(terminal_mapping,),
        mapping_commands={terminal_mapping.local_vt_orderid: command},
        callback_payload=partial_payload,
    )
    partial = factory(config).transition_with_facade_v2(partial_input)
    assert partial.terminal_outcome is None

    final_payload = build_kernel_trade_event_payload_v1(
        raw_payload={"trade_id": f"trade_k5_{algo_code.lower()}_final"},
        trade_quantity=1,
        trade_price_decimal=command.price_decimal,
        symbol=command.symbol,
        side=command.side,
        **common,
    )
    final_input = _transition_input(
        initialization=initialization,
        before_state=partial.next_state,
        event_type=EventTypeV2.TRADE,
        sequence=5,
        mappings=(terminal_mapping,),
        mapping_commands={terminal_mapping.local_vt_orderid: command},
        callback_payload=final_payload,
    )
    final = factory(config).transition_with_facade_v2(final_input)
    restarted = factory(config).transition_with_facade_v2(final_input)
    assert final == restarted
    assert final.terminal_outcome.value == "FILLED"

    overfill_payload = build_kernel_trade_event_payload_v1(
        raw_payload={"trade_id": f"trade_k5_{algo_code.lower()}_overfill"},
        trade_quantity=command.quantity + 1,
        trade_price_decimal=command.price_decimal,
        symbol=command.symbol,
        side=command.side,
        **common,
    )
    overfill_input = _transition_input(
        initialization=initialization,
        before_state=order_result.next_state,
        event_type=EventTypeV2.TRADE,
        sequence=4,
        mappings=(terminal_mapping,),
        mapping_commands={terminal_mapping.local_vt_orderid: command},
        callback_payload=overfill_payload,
    )
    with pytest.raises(VnpyFacadeContractError) as caught:
        factory(config).transition_with_facade_v2(overfill_input)
    assert caught.value.reason_code == "MINIQMT_VNPY_FACADE_BINDING_INVALID"


def test_kernel_public_initialize_preserves_facade_reason_and_unfrozen_context() -> None:
    _iceberg_manifest, invocation = _initialization("ICEBERG", {"display_volume": 100, "interval": 1})
    stop_manifest = create_stop_plugin_v1({"price_add": "0.01"}).manifest

    with pytest.raises(KernelPluginInvocationError) as caught:
        invoke_plugin_initialize_v1(
            plugin=create_stop_plugin_v1({"price_add": "0.01"}),
            expected_manifest=stop_manifest,
            start_context=invocation.start_context,
            facade_input=invocation,
        )

    assert caught.value.reason_code == "MINIQMT_VNPY_FACADE_BINDING_INVALID"
    assert caught.value.context["stage"] == "PLUGIN_INITIALIZE_FACADE"
    assert caught.value.context["algo_code"] == "STOP"


def _replace_state_value(state: dict[str, object], *, name: str, value: object) -> dict[str, object]:
    envelope = VnpyFacadeStateEnvelopeV1.model_validate_json(
        json.dumps(state, sort_keys=True, separators=(",", ":")), strict=True
    )
    variables = tuple(
        VnpyFacadeStateValueV1.create(name=item.name, value=value, value_type=item.value_type)
        if item.name == name
        else item
        for item in envelope.ordered_variables
    )
    if tuple(item.name for item in variables) == tuple(item.name for item in envelope.ordered_variables):
        fields = (
            "runtime_id",
            "algo_instance_id",
            "plugin_id",
            "plugin_version",
            "plugin_manifest_sha256",
            "algorithm_binding_sha256",
            "algo_name",
            "symbol",
            "direction_member",
            "offset_member",
            "limit_price_decimal",
            "target_volume_decimal",
            "status_member",
            "traded_volume_decimal",
            "traded_price_decimal",
            "contract_view",
            "ordered_active_orders",
            "ordered_parameters",
            "state_mapping_set_sha256",
        )
        return VnpyFacadeStateEnvelopeV1.create(
            **{
                **{field: getattr(envelope, field) for field in fields},
                "ordered_variables": variables,
            }
        ).canonical_payload_v1()
    raise AssertionError(f"state has no variable named {name}")


@pytest.mark.parametrize(
    ("algo_code", "config", "factory", "validator", "variable", "invalid_value"),
    (
        (
            "ICEBERG",
            {"display_volume": 100, "interval": 1},
            create_iceberg_plugin_v1,
            validate_iceberg_state_v1,
            "timer_count",
            1,
        ),
        (
            "STOP",
            {"price_add": "0.01"},
            create_stop_plugin_v1,
            validate_stop_state_v1,
            "vt_orderid",
            "unowned_local_order",
        ),
    ),
)
def test_k5_state_codecs_reject_hash_valid_source_semantic_drift(
    algo_code: str,
    config: dict[str, object],
    factory,
    validator,
    variable: str,
    invalid_value: object,
) -> None:
    manifest, invocation = _initialization(algo_code, config)
    initialized = factory(config).initialize_with_facade_v2(invocation)
    drifted = _replace_state_value(thaw_json_v1(initialized.next_state.state), name=variable, value=invalid_value)

    with pytest.raises(MiniQMTPluginContractError) as caught:
        validator(manifest, drifted)
    assert caught.value.reason_code is MiniQMTPluginReasonCode.STATE_SCHEMA_INVALID


@pytest.mark.parametrize(
    ("algo_code", "config"),
    (
        ("ICEBERG", {"display_volume": 100, "interval": 1}),
        ("STOP", {"price_add": "0.01"}),
    ),
)
def test_k5_uses_real_k2_creation_seam_without_product_catalog_switch(
    algo_code: str,
    config: dict[str, object],
) -> None:
    gateway = _gateway()
    candidate = build_k5_shadow_catalog_runtime_v1(gateway_catalog=gateway)
    request = _request()
    contract = {
        "symbol": request.symbol,
        "gateway_name": gateway.gateway_backend,
        "min_volume": "100",
        "volume_increment": "100",
        "pricetick_decimal": "0.01",
    }
    capability = gateway.model_dump(mode="json")
    refs = tuple(
        sorted(
            (
                ExecutionProjectionRefV1.create(
                    projection_type=item.projection_type,
                    projection_id=item.projection_id,
                    projection_version=item.projection_version,
                    payload_sha256=(
                        hash_hex_v1("miniqmt_contract_projection_v1", contract)
                        if item.projection_type is KernelProjectionTypeV1.CONTRACT
                        else hash_hex_v1("miniqmt_market_capability_projection_v1", capability)
                        if item.projection_type is KernelProjectionTypeV1.MARKET_CAPABILITY
                        else item.payload_sha256
                    ),
                    source_event_id=item.source_event_id,
                    logical_at_utc=item.logical_at_utc,
                )
                for item in request.projection_refs
            ),
            key=lambda item: (item.projection_type.value, item.projection_id),
        )
    )
    request_payload = request.model_dump(mode="python")
    request_payload.update(
        {
            "algo_code": algo_code,
            "plugin_config": config,
            "plugin_config_sha256": hash_hex_v1("miniqmt_plugin_config_v2", config),
            "contract_projection": contract,
            "contract_projection_sha256": hash_hex_v1("miniqmt_contract_projection_v1", contract),
            "market_capability_projection": capability,
            "market_capability_projection_sha256": hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
            "projection_refs": refs,
        }
    )
    result = KernelAlgoCreationCoordinatorV1(
        repository=_CapturingRepository(),
        catalog_runtime=candidate.catalog_runtime,
        gateway_catalog=gateway,
        facade_authority=candidate.conformance_authority,
    ).create(type(request).model_validate(request_payload, strict=True))

    assert result["algo"].algo_code == algo_code
    assert result["algo"].status.value == "ACTIVE"
    assert result["delivery"].status.value == "APPLIED"
    assert result["receipt"].ordered_command_ids == ()
