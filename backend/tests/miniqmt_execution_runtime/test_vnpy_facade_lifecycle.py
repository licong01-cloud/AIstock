from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from backend.execution_algos.vnpy_compat.facade_adapter import (
    VnpyFacadeBackedPluginAdapterV1,
    state_mapping_set_sha256_v1,
    terminal_mapping_set_sha256_v1,
)
from backend.execution_algos.vnpy_compat.facade_characterization import (
    build_vnpy_facade_contract_v1,
    build_vnpy_facade_source_manifest_v1,
    build_vnpy_facade_state_mappings_v1,
    build_vnpy_facade_terminal_mappings_v1,
    load_pinned_vnpy_algorithm_classes_v1,
)
from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeActiveOrderV1,
    VnpyFacadeAlgorithmBindingV1,
    VnpyFacadeAlgorithmCharacterizationReceiptV1,
    VnpyFacadeAuthorityInputV1,
    VnpyFacadeCommandAuthorityDispositionV1,
    VnpyFacadeCompatibilityStatusV1,
    VnpyFacadeConformanceBuildItemV1,
    VnpyFacadeConformanceReceiptV1,
    VnpyFacadeConformanceSetV1,
    VnpyFacadeContractError,
    VnpyFacadeInitializationInputV1,
    VnpyFacadeRuntimeBindingDispositionV1,
    VnpyFacadeStateEnvelopeV1,
    VnpyFacadeTransitionInputV1,
)
from backend.execution_algos.vnpy_compat.receipts import (
    build_current_three_compatibility_receipts_v1,
)
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_descriptors_v2,
    current_three_manifests_v2,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    hash_hex_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    AlgoReadOnlyServicesV1,
    AlgoStateSnapshotV2,
    AlgoStartContextV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CommandChildMappingStatusV1,
    DeliveryStatusV1,
    DeterministicExecutionContextV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionAlgoPluginManifestV2,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    GatewayCapabilityCatalogV1,
    KernelCommandLifecycleProjectionV1,
    KernelCommandLifecycleProjectionItemV1,
    KernelProjectionTypeV1,
    MarketDataCapabilityV1,
    OrderTypeV1,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
    SideV1,
    _algo_instance_id_v2,
    kernel_lease_fence_token_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    CompatibilityStatusV1,
    PluginCatalogSnapshotV1,
    PluginCreationBindingV1,
    PluginRegistrationDescriptorV2,
    PluginRouteCompatibilityReceiptV1,
    VnpyCompatibilityReceiptV2,
)


@dataclass(frozen=True)
class _Fixture:
    adapter: VnpyFacadeBackedPluginAdapterV1
    authority: VnpyFacadeAuthorityInputV1
    manifest: ExecutionAlgoPluginManifestV2
    k1_receipt: VnpyCompatibilityReceiptV2
    gateway: GatewayCapabilityCatalogV1


def _candidate_manifest() -> ExecutionAlgoPluginManifestV2:
    original = next(item for item in current_three_manifests_v2() if item.algo_code == "SNIPER_MINIQMT")
    state_schema = {"type": "object", "additionalProperties": True}
    plain = original.canonical_payload_v1()
    plain.update(
        implementation_ref=("backend.execution_algos.vnpy_compat.facade_adapter:VnpyFacadeBackedPluginAdapterV1"),
        state_schema_version="miniqmt_vnpy_facade_state_envelope_v1",
        state_schema=state_schema,
        state_schema_sha256=hash_hex_v1("miniqmt_plugin_state_schema_v1", state_schema),
        behavior_characterization_sha256="f" * 64,
    )
    behavior_keys = (
        "plugin_id",
        "algo_code",
        "plugin_version",
        "provider",
        "implementation_ref",
        "config_schema_version",
        "config_schema_sha256",
        "state_schema_version",
        "state_schema_sha256",
        "subscribed_event_types",
        "market_data_requirements",
        "required_facade_methods",
        "required_facade_object_fields",
        "supported_sides",
        "supported_order_types",
        "supported_broker_backends",
        "restart_policy",
        "source_attribution",
        "compatibility_requirement",
        "behavior_characterization_sha256",
    )
    plain["behavior_contract_sha256"] = hash_hex_v1(
        "miniqmt_plugin_behavior_contract_v2", {key: plain[key] for key in behavior_keys}
    )
    plain.pop("manifest_sha256")
    plain["manifest_sha256"] = hash_hex_v1("execution_algo_plugin_manifest_v2", plain)
    typed = original.model_dump(mode="python")
    typed.update(
        implementation_ref=plain["implementation_ref"],
        state_schema_version=plain["state_schema_version"],
        state_schema=state_schema,
        state_schema_sha256=plain["state_schema_sha256"],
        behavior_characterization_sha256=plain["behavior_characterization_sha256"],
        behavior_contract_sha256=plain["behavior_contract_sha256"],
        manifest_sha256=plain["manifest_sha256"],
    )
    return ExecutionAlgoPluginManifestV2(**typed)


def _gateway() -> GatewayCapabilityCatalogV1:
    values = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": "route.sim.k4.test-only",
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


def _fixture() -> _Fixture:
    manifest = _candidate_manifest()
    original_descriptor = next(
        item for item in current_three_descriptors_v2() if item.manifest.algo_code == "SNIPER_MINIQMT"
    )
    descriptor = PluginRegistrationDescriptorV2(
        **{
            **original_descriptor.model_dump(mode="python"),
            "manifest": manifest,
            "factory_binding_id": "test.k4.facade.factory",
            "factory_callable_ref": manifest.implementation_ref,
        }
    )
    original_k1 = next(
        item for item in build_current_three_compatibility_receipts_v1() if item.plugin_id == manifest.plugin_id
    )
    k1 = VnpyCompatibilityReceiptV2.create(
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        manifest_sha256=manifest.manifest_sha256,
        requirement_sha256=original_k1.requirement_sha256,
        surface_sha256=original_k1.surface_sha256,
        source_lock_sha256=original_k1.source_lock_sha256,
        method_signature_sha256=original_k1.method_signature_sha256,
        object_field_sha256=original_k1.object_field_sha256,
        characterization_sha256=original_k1.characterization_sha256,
        status=CompatibilityStatusV1.PASSED,
        ordered_failures=(),
    )
    snapshot = PluginCatalogSnapshotV1.create(
        descriptors=(descriptor,),
        receipts=(k1,),
        creation_bindings=(PluginCreationBindingV1(algo_code=manifest.algo_code, plugin_key=descriptor.plugin_key),),
    )
    gateway = _gateway()
    route = PluginRouteCompatibilityReceiptV1.create(
        catalog_snapshot=snapshot,
        plugin_key=descriptor.plugin_key,
        gateway_catalog=gateway,
    )
    requirements = tuple(item.compatibility_requirement for item in current_three_manifests_v2())
    facade_contract = build_vnpy_facade_contract_v1(compatibility_requirements=requirements)
    source_manifest = build_vnpy_facade_source_manifest_v1()
    all_state = build_vnpy_facade_state_mappings_v1()
    all_terminal = build_vnpy_facade_terminal_mappings_v1()
    state = tuple(item for item in all_state if item.algo_code == manifest.algo_code)
    terminal = tuple(item for item in all_terminal if item.algo_code == manifest.algo_code)
    algorithm_class = load_pinned_vnpy_algorithm_classes_v1()[manifest.algo_code]
    probe_config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    characterization = VnpyFacadeAlgorithmCharacterizationReceiptV1.create(
        algo_code=manifest.algo_code,
        source_identity_sha256=state[0].source_identity_sha256,
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        characterization_requirement_sha256="1" * 64,
        canonical_factory_probe_config=probe_config,
        factory_probe_config_sha256=hash_hex_v1("miniqmt_vnpy_facade_factory_probe_config_v1", probe_config),
        facade_contract_sha256=facade_contract.facade_contract_sha256,
        implementation_binding_set_sha256=facade_contract.implementation_binding_set_sha256,
        dto_mapping_set_sha256=facade_contract.dto_mapping_set_sha256,
        state_mapping_set_sha256=state_mapping_set_sha256_v1(state),
        terminal_mapping_set_sha256=terminal_mapping_set_sha256_v1(terminal),
        isolated_module_binding_set_sha256=facade_contract.isolated_module_binding_set_sha256,
        ordered_vector_ids=("test_only_candidate_real_sniper",),
        vector_set_sha256="3" * 64,
        status=VnpyFacadeCompatibilityStatusV1.PASSED,
        ordered_failures=(),
    )
    binding = VnpyFacadeAlgorithmBindingV1.create(
        algo_code=manifest.algo_code,
        source_identity_sha256=state[0].source_identity_sha256,
        class_ref=f"{algorithm_class.__module__}:{algorithm_class.__qualname__}",
        constructor_signature_sha256="4" * 64,
        constructor_body_sha256="5" * 64,
        state_mapping_set_sha256=state_mapping_set_sha256_v1(state),
        terminal_mapping_set_sha256=terminal_mapping_set_sha256_v1(terminal),
        characterization_receipt_sha256=characterization.receipt_sha256,
        adapter_contract_sha256=facade_contract.facade_contract_sha256,
    )
    conformance = VnpyFacadeConformanceReceiptV1.create(
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        algo_code=manifest.algo_code,
        manifest_sha256=manifest.manifest_sha256,
        runtime_binding_disposition=VnpyFacadeRuntimeBindingDispositionV1.FACADE_BACKED_ADAPTER,
        command_authority_disposition=VnpyFacadeCommandAuthorityDispositionV1.SHADOW_ONLY_K2_V1,
        pinned_compatibility_receipt_sha256=k1.receipt_sha256,
        requirement_sha256=k1.requirement_sha256,
        surface_sha256=k1.surface_sha256,
        source_lock_sha256=k1.source_lock_sha256,
        method_signature_sha256=k1.method_signature_sha256,
        object_field_sha256=k1.object_field_sha256,
        characterization_sha256=k1.characterization_sha256,
        facade_contract_sha256=facade_contract.facade_contract_sha256,
        implementation_binding_set_sha256=facade_contract.implementation_binding_set_sha256,
        method_contract_set_sha256=facade_contract.method_contract_set_sha256,
        dto_mapping_set_sha256=facade_contract.dto_mapping_set_sha256,
        state_mapping_set_sha256=facade_contract.state_mapping_set_sha256,
        terminal_mapping_set_sha256=facade_contract.terminal_mapping_set_sha256,
        isolated_module_binding_set_sha256=facade_contract.isolated_module_binding_set_sha256,
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        algorithm_characterization_receipt_sha256=characterization.receipt_sha256,
        algorithm_binding_sha256=binding.binding_sha256,
        status=VnpyFacadeCompatibilityStatusV1.PASSED,
        ordered_failures=(),
    )
    build_item = VnpyFacadeConformanceBuildItemV1.create(
        plugin_key=descriptor.plugin_key.canonical_payload_v1(),
        registration_descriptor_full_payload=descriptor.canonical_payload_v1(),
        pinned_compatibility_receipt_sha256=k1.receipt_sha256,
        algorithm_characterization_receipt_sha256=characterization.receipt_sha256,
        algorithm_binding_sha256=binding.binding_sha256,
        runtime_binding_disposition=VnpyFacadeRuntimeBindingDispositionV1.FACADE_BACKED_ADAPTER,
        command_authority_disposition=VnpyFacadeCommandAuthorityDispositionV1.SHADOW_ONLY_K2_V1,
    )
    conformance_set = VnpyFacadeConformanceSetV1.create(
        plugin_catalog_sha256=snapshot.catalog_sha256,
        facade_contract_sha256=facade_contract.facade_contract_sha256,
        dto_mapping_set_sha256=facade_contract.dto_mapping_set_sha256,
        state_mapping_set_sha256=facade_contract.state_mapping_set_sha256,
        terminal_mapping_set_sha256=facade_contract.terminal_mapping_set_sha256,
        isolated_module_binding_set_sha256=facade_contract.isolated_module_binding_set_sha256,
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        ordered_receipts=(conformance,),
        build_items=(build_item,),
    )
    authority = VnpyFacadeAuthorityInputV1.create(
        plugin_catalog_snapshot=snapshot,
        gateway_capability_catalog=gateway,
        plugin_key=descriptor.plugin_key,
        manifest=manifest,
        pinned_compatibility_receipt=k1,
        route_compatibility_receipt=route,
        facade_conformance_receipt=conformance,
        facade_conformance_set=conformance_set,
    )
    adapter = VnpyFacadeBackedPluginAdapterV1(
        manifest=manifest,
        algorithm_class=algorithm_class,
        algorithm_binding=binding,
        state_mappings=state,
        terminal_mappings=terminal,
    )
    return _Fixture(adapter=adapter, authority=authority, manifest=manifest, k1_receipt=k1, gateway=gateway)


def _initialization(fixture: _Fixture) -> VnpyFacadeInitializationInputV1:
    config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    config_sha = hash_hex_v1("miniqmt_plugin_config_v2", config)
    runtime_id = "runtime_k4_test_only"
    parent_id = "parent_k4_test_only"
    slot_id = "slot_k4_test_only"
    algo_id = _algo_instance_id_v2(
        runtime_id=runtime_id,
        parent_intent_id=parent_id,
        strategy_slot_id=slot_id,
        algo_code=fixture.manifest.algo_code,
        plugin_id=fixture.manifest.plugin_id,
        plugin_version=fixture.manifest.plugin_version,
        plugin_manifest_sha256=fixture.manifest.manifest_sha256,
        plugin_config_sha256=config_sha,
    )
    event = RuntimeEventEnvelopeV2.create(
        runtime_id=runtime_id,
        sequence=1,
        event_type=EventTypeV2.ALGO_START,
        event_time_utc="2026-07-29T01:20:00Z",
        monotonic_ns=None,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        symbol="600000.SH",
        payload_schema_version="miniqmt_algo_start_v1",
        payload={
            "parent_intent_id": parent_id,
            "strategy_slot_id": slot_id,
            "target_quantity": 100,
            "execution_plan_id": "plan_k4",
            "execution_plan_sha256": "a" * 64,
            "release_id": "release_k4",
            "release_sha256": "b" * 64,
            "policy_id": "policy_k4",
            "policy_sha256": "c" * 64,
            "route_receipt_sha256": fixture.authority.route_compatibility_receipt.receipt_sha256,
            "route_compatibility_receipt": fixture.authority.route_compatibility_receipt.model_dump(mode="json"),
            "gateway_capability_catalog": fixture.gateway.model_dump(mode="json"),
            "plugin_catalog_sha256": fixture.authority.plugin_catalog_snapshot.catalog_sha256,
        },
        source_identity={
            "algo_instance_id": algo_id,
            "runtime_id": runtime_id,
            "parent_intent_id": parent_id,
            "strategy_slot_id": slot_id,
            "algo_code": fixture.manifest.algo_code,
            "plugin_id": fixture.manifest.plugin_id,
            "plugin_version": fixture.manifest.plugin_version,
            "plugin_manifest_sha256": fixture.manifest.manifest_sha256,
            "plugin_config_sha256": config_sha,
        },
        correlation={},
    )
    delivery = AlgoEventDeliveryV1.create(
        event=event,
        algo_instance_id=algo_id,
        plugin_manifest_sha256=fixture.manifest.manifest_sha256,
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
        algo_instance_id=algo_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        plugin_manifest_sha256=fixture.manifest.manifest_sha256,
        transition_sequence=1,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-29",
        session_epoch="session_k4_test_only",
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
    capability = fixture.gateway.model_dump(mode="json")
    start_context = AlgoStartContextV1(
        schema_version="miniqmt_algo_start_context_v1",
        runtime_id=runtime_id,
        algo_instance_id=algo_id,
        parent_intent_id=parent_id,
        strategy_slot_id=slot_id,
        symbol="600000.SH",
        side=SideV1.BUY,
        limit_price_decimal="10.01",
        parent_quantity=100,
        min_volume=100,
        volume_increment=100,
        plugin_manifest=fixture.manifest,
        plugin_config=config,
        plugin_config_sha256=config_sha,
        start_event_id=event.event_id,
        start_delivery_id=delivery.delivery_id,
        deterministic_context=deterministic,
        contract_projection=contract,
        contract_projection_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
        account_projection=account,
        account_projection_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
        market_capability_projection=capability,
        market_capability_projection_sha256=hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
        execution_plan_id="plan_k4",
        execution_plan_sha256="a" * 64,
        release_id="release_k4",
        release_sha256="b" * 64,
        policy_id="policy_k4",
        policy_sha256="c" * 64,
    )
    return VnpyFacadeInitializationInputV1.create(
        start_event=event,
        start_delivery=delivery,
        start_context=start_context,
        authority_input=fixture.authority,
        transition_id="transition_k4_init",
    )


def _transition(
    fixture: _Fixture,
    initialization: VnpyFacadeInitializationInputV1,
    state,
    *,
    market_overrides: dict[str, object] | None = None,
    active_mappings: tuple[ExecutionCommandChildMappingV1, ...] = (),
) -> VnpyFacadeTransitionInputV1:
    context = initialization.start_context
    event = RuntimeEventEnvelopeV2.create(
        runtime_id=context.runtime_id,
        sequence=2,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-07-29T01:30:00Z",
        monotonic_ns=None,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol=context.symbol,
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={"last_price_decimal": "10.00"},
        source_identity={"market_data_id": "market_k4_test_only"},
        correlation={},
    )
    pending = AlgoEventDeliveryV1.create(
        event=event,
        algo_instance_id=context.algo_instance_id,
        plugin_manifest_sha256=fixture.manifest.manifest_sha256,
        algo_delivery_sequence=2,
        previous_delivery_id=context.start_delivery_id,
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc=event.event_time_utc,
        updated_at_utc=event.event_time_utc,
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
    lease_owner = "worker_k4:incarnation_k4"
    claimed_payload = initial_delivery.model_dump(mode="python")
    claimed_payload.update(
        status=DeliveryStatusV1.CLAIMED,
        attempt_count=1,
        lease_owner=lease_owner,
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=initial_delivery.delivery_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        ),
        lease_expires_at="2026-07-29T01:31:00Z",
        transition_id="transition_k4_tick",
        row_version=2,
    )
    delivery = AlgoDeliveryPersistenceV1.model_validate(claimed_payload)
    deterministic = DeterministicExecutionContextV1.create(
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        plugin_manifest_sha256=fixture.manifest.manifest_sha256,
        transition_sequence=2,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-29",
        session_epoch="session_k4_test_only",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="8" * 64,
    )
    algo = ExecutionAlgoInstancePersistenceV2.create(
        algo_instance_id=context.algo_instance_id,
        runtime_id=context.runtime_id,
        parent_intent_id=context.parent_intent_id,
        strategy_slot_id=context.strategy_slot_id,
        symbol=context.symbol,
        side=context.side,
        target_quantity=context.parent_quantity,
        traded_quantity=0,
        remaining_quantity=context.parent_quantity,
        algo_code=fixture.manifest.algo_code,
        plugin_id=fixture.manifest.plugin_id,
        plugin_version=fixture.manifest.plugin_version,
        plugin_manifest_sha256=fixture.manifest.manifest_sha256,
        plugin_config_json={"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        plugin_config_sha256=context.plugin_config_sha256,
        compatibility_receipt_sha256=fixture.k1_receipt.receipt_sha256,
        state_schema_version=state.state_schema_version,
        state_json=state.state,
        state_sha256=state.state_sha256,
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id=context.start_delivery_id,
        last_closed_delivery_sequence=1,
        terminal_delivery_sequence=None,
        status=ExecutionAlgoPersistenceStatusV2.ACTIVE,
        failure_receipt_id=None,
        active_child_closure_status=ActiveChildClosureStatusV1.NOT_APPLICABLE,
        active_child_count=len(active_mappings),
        row_version=1,
        created_at_utc="2026-07-29T01:20:00Z",
        updated_at_utc="2026-07-29T01:20:00Z",
        terminal_at_utc=None,
        archived_at_utc=None,
    )
    contract = {
        "symbol": "600000.SH",
        "gateway_name": "minqmt_sim",
        "min_volume": "100",
        "volume_increment": "100",
        "pricetick_decimal": "0.01",
    }
    market = {
        "symbol": "600000.SH",
        "logical_at_utc": "2026-07-29T01:30:00Z",
        "bid_price_1": "10",
        "bid_volume_1": 500,
        "ask_price_1": "10",
        "ask_volume_1": 100,
        "last_price": "10",
        "limit_up": "11",
        "limit_down": "9",
    }
    if market_overrides:
        market.update(market_overrides)
    refs = tuple(
        sorted(
            (
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.CONTRACT,
                    projection_id="contract_k4",
                    projection_version="contract_v1",
                    payload_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
                    source_event_id=None,
                    logical_at_utc=event.event_time_utc,
                ),
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.MARKET_DATA,
                    projection_id="market_k4",
                    projection_version="market_v2",
                    payload_sha256=hash_hex_v1("miniqmt_market_data_projection_v2", market),
                    source_event_id=event.event_id,
                    logical_at_utc=event.event_time_utc,
                ),
            ),
            key=lambda item: (item.projection_type.value, item.projection_id),
        )
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        projection_refs=refs,
    )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        contract_projection_id="contract_k4",
        contract_projection=contract,
        market_data_projection_id="market_k4",
        market_data_projection=market,
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    lifecycle_items = tuple(
        KernelCommandLifecycleProjectionItemV1(
            mapping_id=mapping.mapping_id,
            mapping_version=mapping.mapping_version,
            mapping_payload_sha256=mapping.payload_sha256,
            local_vt_orderid=mapping.local_vt_orderid,
            submit_command_id=mapping.command_id,
            broker_order_id=mapping.broker_order_id,
            mapping_status=mapping.mapping_status,
            current_outbox_command_id=mapping.command_id,
            current_outbox_command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
            current_outbox_status=BrokerCommandOutboxStatusV1.PENDING,
            current_outbox_row_version=1,
            current_outbox_payload_sha256=mapping.payload_sha256,
            outcome_receipt_sha256=None,
            latest_command_outcome_event_id=None,
            latest_command_outcome_payload_sha256=None,
            command_outcome_delivery_id=None,
            command_outcome_delivery_status=None,
        )
        for mapping in active_mappings
    )
    lifecycle = KernelCommandLifecycleProjectionV1.create(
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        ordered_items=lifecycle_items,
    )
    return VnpyFacadeTransitionInputV1.create(
        runtime_event=event,
        delivery=delivery,
        algo_instance=algo,
        manifest=fixture.manifest,
        authority_input=fixture.authority,
        before_state=state,
        read_only_services=services,
        command_lifecycle_projection=lifecycle,
        ordered_active_mappings=active_mappings,
        deterministic_context=deterministic,
        transition_sequence=2,
    )


def test_real_pinned_sniper_initialize_restore_tick_and_freeze() -> None:
    fixture = _fixture()
    initialization_input = _initialization(fixture)
    initialized = fixture.adapter.initialize_with_facade(initialization_input)
    initialized_retry = fixture.adapter.initialize_with_facade(initialization_input)

    assert initialized.next_state.transition_sequence == 1
    assert not initialized.broker_commands
    assert initialized_retry == initialized
    assert tuple(item.reason_code for item in initialized.diagnostic_observations) == (
        "MINIQMT_VNPY_FACADE_ALGO_PROJECTION",
        "MINIQMT_VNPY_FACADE_ALGO_PROJECTION",
        "MINIQMT_VNPY_FACADE_ALGO_LOG",
    )

    transition_input = _transition(fixture, initialization_input, initialized.next_state)
    transitioned = fixture.adapter.transition_with_facade(transition_input)
    transitioned_retry = fixture.adapter.transition_with_facade(transition_input)

    assert transitioned.next_state.transition_sequence == 2
    assert transitioned_retry == transitioned
    assert len(transitioned.broker_commands) == 1
    assert transitioned.broker_commands[0].quantity == 100
    assert transitioned.broker_commands[0].price_decimal == "10.01"
    assert transitioned.terminal_outcome is None


@pytest.mark.parametrize(
    "market_overrides",
    (
        {"ask_volume_1": True},
        {"logical_at_utc": "not-a-time"},
        {"symbol": "000001.SZ"},
    ),
)
def test_public_transition_rejects_malformed_market_projection_without_business_fallback(
    market_overrides: dict[str, object],
) -> None:
    fixture = _fixture()
    initialization = _initialization(fixture)
    initialized = fixture.adapter.initialize_with_facade(initialization)
    transition_input = _transition(
        fixture,
        initialization,
        initialized.next_state,
        market_overrides=market_overrides,
    )

    with pytest.raises(VnpyFacadeContractError) as caught:
        fixture.adapter.transition_with_facade(transition_input)
    assert caught.value.reason_code == "MINIQMT_VNPY_FACADE_MARKET_DATA_INVALID"


def test_transition_input_rejects_sequence_and_predecessor_drift_before_callback() -> None:
    fixture = _fixture()
    initialization = _initialization(fixture)
    initialized = fixture.adapter.initialize_with_facade(initialization)
    transition_input = _transition(fixture, initialization, initialized.next_state)
    base = transition_input.model_dump(mode="python", exclude={"input_sha256"})

    for delivery_update, context_update in (
        ({"algo_delivery_sequence": 1}, {}),
        ({"previous_delivery_id": "wrong_delivery"}, {}),
        ({}, {"transition_sequence": 3}),
    ):
        delivery = transition_input.delivery.model_copy(update=delivery_update)
        deterministic = DeterministicExecutionContextV1.create(
            **{
                **transition_input.deterministic_context.model_dump(
                    mode="python", exclude={"schema_version", "context_sha256"}
                ),
                **context_update,
            }
        )
        with pytest.raises(ValueError, match="sequence|predecessor"):
            VnpyFacadeTransitionInputV1.create(
                **{
                    **base,
                    "delivery": delivery,
                    "deterministic_context": deterministic,
                    "transition_sequence": deterministic.transition_sequence,
                }
            )


def test_transition_input_rejects_hash_closed_mapping_owner_drift() -> None:
    fixture = _fixture()
    initialization = _initialization(fixture)
    initialized = fixture.adapter.initialize_with_facade(initialization)
    transition_input = _transition(fixture, initialization, initialized.next_state)
    submitted = fixture.adapter.transition_with_facade(transition_input).broker_commands[0]
    wrong_parent_command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=submitted.runtime_id,
        algo_instance_id=submitted.algo_instance_id,
        parent_intent_id="wrong_parent_k4",
        transition_id=submitted.transition_id,
        ordinal=submitted.ordinal,
        local_vt_orderid=None,
        symbol=submitted.symbol,
        side=submitted.side,
        order_type=submitted.order_type,
        price_decimal=submitted.price_decimal,
        quantity=submitted.quantity,
        owned_broker_order_id=None,
        reason_code=submitted.reason_code,
        metadata=submitted.canonical_payload_v1()["metadata"],
    )
    mapping = ExecutionCommandChildMappingV1.create(
        command=wrong_parent_command,
        strategy_slot_id=transition_input.algo_instance.strategy_slot_id,
        mapping_status=CommandChildMappingStatusV1.RESERVED,
        mapping_version=1,
        broker_order_id=None,
        broker_identity_source_event_id=None,
        last_order_event_id=None,
        last_trade_event_id=None,
        updated_by_event_id=None,
        created_at_utc=transition_input.deterministic_context.logical_time_utc,
        updated_at_utc=transition_input.deterministic_context.logical_time_utc,
    )
    lifecycle_item = KernelCommandLifecycleProjectionItemV1(
        mapping_id=mapping.mapping_id,
        mapping_version=mapping.mapping_version,
        mapping_payload_sha256=mapping.payload_sha256,
        local_vt_orderid=mapping.local_vt_orderid,
        submit_command_id=mapping.command_id,
        broker_order_id=None,
        mapping_status=mapping.mapping_status,
        current_outbox_command_id=mapping.command_id,
        current_outbox_command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        current_outbox_status=BrokerCommandOutboxStatusV1.PENDING,
        current_outbox_row_version=1,
        current_outbox_payload_sha256=wrong_parent_command.payload_sha256,
        outcome_receipt_sha256=None,
        latest_command_outcome_event_id=None,
        latest_command_outcome_payload_sha256=None,
        command_outcome_delivery_id=None,
        command_outcome_delivery_status=None,
    )
    lifecycle = KernelCommandLifecycleProjectionV1.create(
        runtime_id=transition_input.deterministic_context.runtime_id,
        algo_instance_id=transition_input.deterministic_context.algo_instance_id,
        event_id=transition_input.deterministic_context.event_id,
        delivery_id=transition_input.deterministic_context.delivery_id,
        ordered_items=(lifecycle_item,),
    )
    base = transition_input.model_dump(mode="python", exclude={"input_sha256"})

    with pytest.raises(ValueError, match="active mapping owner"):
        VnpyFacadeTransitionInputV1.create(
            **{
                **base,
                "command_lifecycle_projection": lifecycle,
                "ordered_active_mappings": (mapping,),
            }
        )


def test_transition_input_closes_valid_active_mapping_lifecycle_and_before_state() -> None:
    fixture = _fixture()
    initialization = _initialization(fixture)
    initialized = fixture.adapter.initialize_with_facade(initialization)
    context = initialization.start_context
    command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        parent_intent_id=context.parent_intent_id,
        transition_id="transition_k4_previous_submit",
        ordinal=0,
        local_vt_orderid=None,
        symbol=context.symbol,
        side=context.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="K4_ACTIVE_MAPPING_CLOSURE",
        metadata={},
    )
    mapping = ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id=context.strategy_slot_id,
        mapping_status=CommandChildMappingStatusV1.RESERVED,
        mapping_version=1,
        broker_order_id=None,
        broker_identity_source_event_id=None,
        last_order_event_id=None,
        last_trade_event_id=None,
        updated_by_event_id=None,
        created_at_utc=context.deterministic_context.logical_time_utc,
        updated_at_utc=context.deterministic_context.logical_time_utc,
    )
    before_envelope = VnpyFacadeStateEnvelopeV1.model_validate_json(
        json.dumps(thaw_json_v1(initialized.next_state.state), sort_keys=True, separators=(",", ":")),
        strict=True,
    )
    active = VnpyFacadeActiveOrderV1.create(
        local_vt_orderid=mapping.local_vt_orderid,
        broker_order_id=mapping.broker_order_id,
        command_id=mapping.command_id,
        child_order_id=mapping.child_order_id,
        symbol=mapping.symbol,
        side=mapping.side.value,
        price_decimal=mapping.requested_price_decimal,
        requested_quantity=mapping.requested_quantity,
        cumulative_quantity=0,
        remaining_quantity=mapping.requested_quantity,
        status=mapping.mapping_status.value,
        last_order_event_id=mapping.last_order_event_id,
        last_trade_event_id=mapping.last_trade_event_id,
    )
    envelope_with_active = VnpyFacadeStateEnvelopeV1.create(
        **{
            **before_envelope.canonical_payload_v1(
                exclude={"schema_version", "ordered_active_orders", "state_envelope_sha256"}
            ),
            "contract_view": before_envelope.contract_view,
            "ordered_active_orders": (active,),
            "ordered_parameters": before_envelope.ordered_parameters,
            "ordered_variables": before_envelope.ordered_variables,
        }
    )
    state_with_active = AlgoStateSnapshotV2.create(
        plugin_manifest=fixture.manifest,
        deterministic_context=context.deterministic_context,
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id=context.start_delivery_id,
        last_closed_delivery_sequence=1,
        state=envelope_with_active.canonical_payload_v1(),
        last_applied_event_id=context.start_event_id,
    )

    transition = _transition(
        fixture,
        initialization,
        state_with_active,
        active_mappings=(mapping,),
    )

    assert transition.ordered_active_mappings == (mapping,)
    assert transition.command_lifecycle_projection.ordered_items[0].mapping_payload_sha256 == mapping.payload_sha256

    drifted_item = transition.command_lifecycle_projection.ordered_items[0].model_copy(
        update={"mapping_version": mapping.mapping_version + 1}
    )
    drifted_lifecycle = KernelCommandLifecycleProjectionV1.create(
        runtime_id=context.runtime_id,
        algo_instance_id=context.algo_instance_id,
        event_id=transition.runtime_event.event_id,
        delivery_id=transition.delivery.delivery_id,
        ordered_items=(drifted_item,),
    )
    with pytest.raises(ValueError, match="command lifecycle facts"):
        VnpyFacadeTransitionInputV1.create(
            runtime_event=transition.runtime_event,
            delivery=transition.delivery,
            algo_instance=transition.algo_instance,
            manifest=transition.manifest,
            authority_input=transition.authority_input,
            before_state=transition.before_state,
            read_only_services=transition.read_only_services,
            command_lifecycle_projection=drifted_lifecycle,
            ordered_active_mappings=transition.ordered_active_mappings,
            deterministic_context=transition.deterministic_context,
            transition_sequence=transition.transition_sequence,
        )
