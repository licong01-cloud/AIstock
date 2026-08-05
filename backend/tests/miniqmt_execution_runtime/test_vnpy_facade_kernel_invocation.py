from __future__ import annotations

import json

import pytest

import backend.services.miniqmt_execution_runtime.kernel_creation as kernel_creation_module
import backend.services.miniqmt_execution_runtime.kernel_delivery as kernel_delivery_module
from backend.execution_algos.vnpy_compat.facade_adapter import (
    VnpyFacadeBackedPluginAdapterV1,
    state_mapping_set_sha256_v1,
    terminal_mapping_set_sha256_v1,
)
from backend.execution_algos.vnpy_compat.facade_characterization import (
    build_vnpy_facade_source_manifest_v1,
    build_vnpy_facade_state_mappings_v1,
    build_vnpy_facade_terminal_mappings_v1,
    load_pinned_vnpy_algorithm_classes_v1,
)
from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAlgorithmBindingV2,
    VnpyFacadeAlgorithmCharacterizationReceiptV2,
    VnpyFacadeAuthorityInputV2,
    VnpyFacadeCommandAuthorityDispositionV1,
    VnpyFacadeCompatibilityStatusV1,
    VnpyFacadeConformanceAuthorityValidationReceiptV2,
    VnpyFacadeConformanceBuildItemV2,
    VnpyFacadeConformanceReceiptV2,
    VnpyFacadeConformanceSetV2,
    VnpyFacadeDeterministicInputsV1,
    VnpyFacadeExecutedVectorResultV1,
    VnpyFacadeInitializationInputV2,
    VnpyFacadeRuntimeBindingDispositionV1,
    VnpyFacadeSourceExecutionSetV1,
    VnpyFacadeSourceExecutorBindingV1,
    VnpyFacadeSourceStateEnvelopeV1,
    VnpyFacadeStateEnvelopeV1,
    VnpyFacadeRepositoryEventReadV1,
    VnpyFacadeRepositoryReadKindV1,
    VnpyFacadeRepositoryReadRequestV1,
    VnpyFacadeRepositoryReadSetV1,
    VnpyFacadeTransitionInputV2,
    _seal_vnpy_facade_conformance_authority_v2,
    hash_hex_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    KernelDeliveryExecutionInputV1,
    KernelDeliveryWorkerV1,
    ResolvedKernelPluginV1,
    KernelPluginInvocationError,
    invoke_plugin_initialize_v1,
    invoke_plugin_transition_v1,
    validate_vnpy_facade_k6_product_command_trace_v1,
    validate_vnpy_facade_k2_shadow_command_authority_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_creation import (
    KernelAlgoCreationCoordinatorV1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    AlgoReadOnlyServicesV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    DeliveryStatusV1,
    DeterministicExecutionContextV1,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    KernelProjectionTypeV1,
    OrderTypeV1,
    algo_transition_id_v1,
)
from backend.tests.miniqmt_execution_runtime.test_vnpy_facade_lifecycle import (
    _fixture,
    _initialization,
    _transition,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_creation import (
    _CapturingRepository,
    _request,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_delivery import (
    _command_projection_refs,
    _event_lineage,
)


def _v2_candidate():
    fixture = _fixture()
    v1_input = _initialization(fixture)
    v1_initialized = fixture.adapter.initialize_with_facade(v1_input)
    envelope = VnpyFacadeStateEnvelopeV1.model_validate_json(
        json.dumps(thaw_json_v1(v1_initialized.next_state.state), sort_keys=True, separators=(",", ":")),
        strict=True,
    )
    source_envelope = VnpyFacadeSourceStateEnvelopeV1.create(
        runtime_id=envelope.runtime_id,
        algo_instance_id=envelope.algo_instance_id,
        algo_code=fixture.manifest.algo_code,
        source_identity_sha256="4" * 64,
        manifest_view_sha256="9" * 64,
        algo_name=envelope.algo_name,
        symbol=envelope.symbol,
        direction_member=envelope.direction_member,
        offset_member=envelope.offset_member,
        limit_price_decimal=envelope.limit_price_decimal,
        target_volume_decimal=envelope.target_volume_decimal,
        status_member=envelope.status_member,
        traded_volume_decimal=envelope.traded_volume_decimal,
        traded_price_decimal=envelope.traded_price_decimal,
        contract_projection={
            "gateway_name": envelope.contract_view.gateway_name,
            "min_volume": envelope.contract_view.min_volume,
            "volume_increment": envelope.contract_view.volume_increment,
            "pricetick_decimal": envelope.contract_view.pricetick_decimal,
        },
        ordered_active_orders=envelope.ordered_active_orders,
        ordered_parameters=envelope.ordered_parameters,
        ordered_variables=envelope.ordered_variables,
        state_mapping_set_sha256=envelope.state_mapping_set_sha256,
    )
    source_manifest = build_vnpy_facade_source_manifest_v1()
    v1_receipt = fixture.authority.facade_conformance_receipt
    v1_set = fixture.authority.facade_conformance_set
    executor = VnpyFacadeSourceExecutorBindingV1.create(
        executor_ref="backend.execution_algos.vnpy_compat.facade_source_execution:execute_vnpy_facade_vectors_v1",
        executor_signature_sha256="1" * 64,
        executor_source_sha256="2" * 64,
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        facade_contract_sha256=v1_set.facade_contract_sha256,
        implementation_binding_set_sha256=v1_receipt.implementation_binding_set_sha256,
        isolated_module_binding_set_sha256=v1_set.isolated_module_binding_set_sha256,
        dto_mapping_set_sha256=v1_set.dto_mapping_set_sha256,
        state_mapping_set_sha256=v1_set.state_mapping_set_sha256,
        terminal_mapping_set_sha256=v1_set.terminal_mapping_set_sha256,
        vector_artifact_sha256="7" * 64,
        vector_artifact_file_sha256="8" * 64,
        supported_algo_codes=(
            "BEST_LIMIT_MINIQMT",
            "ICEBERG",
            "SNIPER_MINIQMT",
            "STOP",
            "TWAP_LITE_MINIQMT",
        ),
    )
    result = VnpyFacadeExecutedVectorResultV1.create(
        vector_id="test_only_candidate_vector",
        vector_sha256="3" * 64,
        scenario_id="test_only_candidate_scenario",
        step_ordinal=0,
        source_executor_binding_sha256=executor.binding_sha256,
        source_identity_sha256="4" * 64,
        invocation_status="COMPLETED",
        actual_ordered_facade_calls=(),
        actual_ordered_effects=(),
        actual_after_state_or_null=source_envelope,
        actual_terminal_outcome=None,
        consumed_deterministic_inputs=VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=()),
        ordered_execution_failures=(),
    )
    execution_set = VnpyFacadeSourceExecutionSetV1.create(
        algo_code=fixture.manifest.algo_code,
        characterization_requirement_sha256="5" * 64,
        source_executor_binding_sha256=executor.binding_sha256,
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        facade_contract_sha256=v1_set.facade_contract_sha256,
        vector_set_sha256="6" * 64,
        ordered_results=(result,),
        ordered_failures=(),
        status=VnpyFacadeCompatibilityStatusV1.PASSED,
    )
    probe_config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    characterization = VnpyFacadeAlgorithmCharacterizationReceiptV2.create(
        algo_code=fixture.manifest.algo_code,
        source_identity_sha256="4" * 64,
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        characterization_requirement_sha256=execution_set.characterization_requirement_sha256,
        canonical_factory_probe_config=probe_config,
        factory_probe_config_sha256=hash_hex_v1("miniqmt_vnpy_facade_factory_probe_config_v1", probe_config),
        facade_contract_sha256=v1_set.facade_contract_sha256,
        implementation_binding_set_sha256=v1_receipt.implementation_binding_set_sha256,
        dto_mapping_set_sha256=v1_set.dto_mapping_set_sha256,
        state_mapping_set_sha256=v1_set.state_mapping_set_sha256,
        terminal_mapping_set_sha256=v1_set.terminal_mapping_set_sha256,
        isolated_module_binding_set_sha256=v1_set.isolated_module_binding_set_sha256,
        source_executor_binding_sha256=executor.binding_sha256,
        source_execution_set_sha256=execution_set.execution_set_sha256,
        ordered_vector_ids=(result.vector_id,),
        vector_set_sha256=execution_set.vector_set_sha256,
        status=VnpyFacadeCompatibilityStatusV1.PASSED,
        ordered_failures=(),
    )
    state = tuple(
        item for item in build_vnpy_facade_state_mappings_v1() if item.algo_code == fixture.manifest.algo_code
    )
    terminal = tuple(
        item for item in build_vnpy_facade_terminal_mappings_v1() if item.algo_code == fixture.manifest.algo_code
    )
    algorithm_class = load_pinned_vnpy_algorithm_classes_v1()[fixture.manifest.algo_code]
    binding = VnpyFacadeAlgorithmBindingV2.create(
        algo_code=fixture.manifest.algo_code,
        source_identity_sha256=state[0].source_identity_sha256,
        class_ref=f"{algorithm_class.__module__}:{algorithm_class.__qualname__}",
        constructor_signature_sha256="7" * 64,
        constructor_body_sha256="8" * 64,
        state_mapping_set_sha256=state_mapping_set_sha256_v1(state),
        terminal_mapping_set_sha256=terminal_mapping_set_sha256_v1(terminal),
        characterization_receipt_sha256=characterization.receipt_sha256,
        adapter_contract_sha256=v1_set.facade_contract_sha256,
        source_executor_binding_sha256=executor.binding_sha256,
        source_execution_set_sha256=execution_set.execution_set_sha256,
    )
    v2_receipt = VnpyFacadeConformanceReceiptV2.create(
        **{
            **v1_receipt.model_dump(
                mode="python",
                exclude={
                    "schema_version",
                    "algorithm_characterization_receipt_sha256",
                    "algorithm_binding_sha256",
                    "status",
                    "ordered_failures",
                    "receipt_sha256",
                },
            ),
            "source_executor_binding_sha256": executor.binding_sha256,
            "source_execution_set_sha256": execution_set.execution_set_sha256,
            "algorithm_characterization_receipt_v2_sha256": characterization.receipt_sha256,
            "algorithm_binding_sha256": binding.binding_sha256,
            "status": VnpyFacadeCompatibilityStatusV1.PASSED,
            "ordered_failures": (),
        }
    )
    descriptor = fixture.authority.plugin_catalog_snapshot.registration_descriptors[0]
    build_item = VnpyFacadeConformanceBuildItemV2.create(
        plugin_key=descriptor.plugin_key.canonical_payload_v1(),
        registration_descriptor_full_payload=descriptor.canonical_payload_v1(),
        pinned_compatibility_receipt_sha256=fixture.k1_receipt.receipt_sha256,
        source_executor_binding_sha256=executor.binding_sha256,
        source_execution_set_sha256=execution_set.execution_set_sha256,
        algorithm_characterization_receipt_v2_sha256=characterization.receipt_sha256,
        algorithm_binding_sha256=binding.binding_sha256,
        runtime_binding_disposition=VnpyFacadeRuntimeBindingDispositionV1.FACADE_BACKED_ADAPTER,
        command_authority_disposition=VnpyFacadeCommandAuthorityDispositionV1.SHADOW_ONLY_K2_V1,
    )
    conformance_set = VnpyFacadeConformanceSetV2.create(
        plugin_catalog_sha256=fixture.authority.plugin_catalog_snapshot.catalog_sha256,
        facade_contract_sha256=v1_set.facade_contract_sha256,
        dto_mapping_set_sha256=v1_set.dto_mapping_set_sha256,
        state_mapping_set_sha256=v1_set.state_mapping_set_sha256,
        terminal_mapping_set_sha256=v1_set.terminal_mapping_set_sha256,
        isolated_module_binding_set_sha256=v1_set.isolated_module_binding_set_sha256,
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        source_executor_binding_sha256=executor.binding_sha256,
        ordered_source_execution_set_sha256s=(execution_set.execution_set_sha256,),
        ordered_receipts=(v2_receipt,),
        build_items=(build_item,),
    )
    validation_input_sha = hash_hex_v1(
        "miniqmt_vnpy_facade_conformance_authority_validation_input_v2",
        {
            "conformance_set_v2_sha256": conformance_set.receipt_set_sha256,
            "source_executor_binding_sha256": executor.binding_sha256,
            "ordered_source_execution_set_sha256s": [execution_set.execution_set_sha256],
        },
    )
    validation_receipt = VnpyFacadeConformanceAuthorityValidationReceiptV2.create(
        conformance_set_v2_sha256=conformance_set.receipt_set_sha256,
        source_executor_binding_sha256=executor.binding_sha256,
        ordered_source_execution_set_sha256s=(execution_set.execution_set_sha256,),
        validation_input_sha256=validation_input_sha,
        status=VnpyFacadeCompatibilityStatusV1.PASSED,
        ordered_failures=(),
    )
    sealed = _seal_vnpy_facade_conformance_authority_v2(
        conformance_set=conformance_set,
        source_executor_binding=executor,
        source_execution_sets=(execution_set,),
        characterization_receipts=(characterization,),
        algorithm_bindings=(binding,),
        validation_receipt=validation_receipt,
    )
    authority = VnpyFacadeAuthorityInputV2.create(
        conformance_authority=sealed,
        plugin_catalog_snapshot=fixture.authority.plugin_catalog_snapshot,
        gateway_capability_catalog=fixture.gateway,
        plugin_key=descriptor.plugin_key,
        manifest=fixture.manifest,
        pinned_compatibility_receipt=fixture.k1_receipt,
        route_compatibility_receipt=fixture.authority.route_compatibility_receipt,
    )
    adapter = VnpyFacadeBackedPluginAdapterV1(
        manifest=fixture.manifest,
        algorithm_class=algorithm_class,
        algorithm_binding=binding,
        state_mappings=state,
        terminal_mappings=terminal,
    )
    return fixture, adapter, authority, v1_input, sealed


def test_v2_optional_invocation_uses_precomputed_transition_identity_with_claimed_null_fact() -> None:
    fixture, adapter, authority, v1_input, _sealed = _v2_candidate()
    initialization = VnpyFacadeInitializationInputV2.create(
        start_event=v1_input.start_event,
        start_delivery=v1_input.start_delivery,
        start_context=v1_input.start_context,
        authority_input=authority,
    )
    initialized = invoke_plugin_initialize_v1(
        plugin=adapter,
        expected_manifest=fixture.manifest,
        start_context=initialization.start_context,
        facade_input=initialization,
    )
    v1_transition = _transition(fixture, v1_input, initialized.next_state)
    claimed_payload = v1_transition.delivery.model_dump(mode="python")
    claimed_payload["transition_id"] = None
    claimed_delivery = AlgoDeliveryPersistenceV1.model_validate(claimed_payload)
    deterministic_context = DeterministicExecutionContextV1.create(
        **{
            **v1_transition.deterministic_context.model_dump(
                mode="python",
                exclude={"schema_version", "context_sha256", "input_projection_sha256"},
            ),
            "input_projection_sha256": (
                v1_transition.read_only_services.execution_projection_set.projection_set_sha256
            ),
        }
    )
    transition_input = VnpyFacadeTransitionInputV2.create(
        runtime_event=v1_transition.runtime_event,
        claimed_delivery=claimed_delivery,
        algo_instance=v1_transition.algo_instance,
        manifest=fixture.manifest,
        authority_input=authority,
        before_state=v1_transition.before_state,
        read_only_services=v1_transition.read_only_services,
        command_lifecycle_projection=v1_transition.command_lifecycle_projection,
        ordered_active_mappings=v1_transition.ordered_active_mappings,
        deterministic_context=deterministic_context,
        transition_sequence=v1_transition.transition_sequence,
    )

    result = invoke_plugin_transition_v1(
        plugin=adapter,
        expected_manifest=fixture.manifest,
        state_codec=lambda _manifest, state: state,
        state=transition_input.before_state,
        event=transition_input.runtime_event,
        services=transition_input.read_only_services,
        deterministic_context=transition_input.deterministic_context,
        facade_input=transition_input,
    )

    assert claimed_delivery.transition_id is None
    assert result.broker_commands[0].transition_id == transition_input.transition_id
    assert result.next_state.last_applied_delivery_id == claimed_delivery.delivery_id


def test_optional_invocation_rejects_adapter_without_v2_input_before_callback() -> None:
    fixture, adapter, _authority, v1_input, _sealed = _v2_candidate()
    with pytest.raises(KernelPluginInvocationError) as caught:
        invoke_plugin_initialize_v1(
            plugin=adapter,
            expected_manifest=fixture.manifest,
            start_context=v1_input.start_context,
        )
    assert caught.value.reason_code == "MINIQMT_VNPY_FACADE_BINDING_INVALID"


def test_k2_shadow_materialization_rejects_multi_command_without_truncating_trace() -> None:
    fixture, adapter, authority, v1_input, _sealed = _v2_candidate()
    initialization = VnpyFacadeInitializationInputV2.create(
        start_event=v1_input.start_event,
        start_delivery=v1_input.start_delivery,
        start_context=v1_input.start_context,
        authority_input=authority,
    )
    initialized = adapter.initialize_with_facade_v2(initialization)
    v1_transition = _transition(fixture, v1_input, initialized.next_state)
    claimed = v1_transition.delivery.model_copy(update={"transition_id": None})
    context = DeterministicExecutionContextV1.create(
        **{
            **v1_transition.deterministic_context.model_dump(
                mode="python",
                exclude={"schema_version", "context_sha256", "input_projection_sha256"},
            ),
            "input_projection_sha256": (
                v1_transition.read_only_services.execution_projection_set.projection_set_sha256
            ),
        }
    )
    transition_input = VnpyFacadeTransitionInputV2.create(
        runtime_event=v1_transition.runtime_event,
        claimed_delivery=claimed,
        algo_instance=v1_transition.algo_instance,
        manifest=fixture.manifest,
        authority_input=authority,
        before_state=initialized.next_state,
        read_only_services=v1_transition.read_only_services,
        command_lifecycle_projection=v1_transition.command_lifecycle_projection,
        ordered_active_mappings=(),
        deterministic_context=context,
        transition_sequence=2,
    )
    transition = adapter.transition_with_facade_v2(transition_input)
    first = transition.broker_commands[0]
    second = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=first.runtime_id,
        algo_instance_id=first.algo_instance_id,
        parent_intent_id=first.parent_intent_id,
        transition_id=first.transition_id,
        ordinal=first.ordinal + 1,
        local_vt_orderid=None,
        symbol=first.symbol,
        side=first.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal=first.price_decimal,
        quantity=first.quantity,
        owned_broker_order_id=None,
        reason_code="MINIQMT_VNPY_FACADE_TEST_SECOND_COMMAND",
        metadata={"test_only_candidate": True},
    )
    full_trace = transition.model_copy(update={"broker_commands": (first, second)})

    with pytest.raises(KernelPluginInvocationError) as caught:
        validate_vnpy_facade_k2_shadow_command_authority_v1(full_trace)
    assert caught.value.reason_code == ("MINIQMT_VNPY_FACADE_MULTI_COMMAND_PRODUCT_AUTHORITY_UNAVAILABLE")
    assert caught.value.context["ordered_command_ids"] == [
        first.command_id,
        second.command_id,
    ]
    # K6 does not reinterpret K5's shadow receipt as a product grant.  It
    # accepts the full source trace solely for the later V3 authority/
    # materializer transaction.
    validate_vnpy_facade_k6_product_command_trace_v1(full_trace)


def test_k2_creation_optional_seam_invokes_exact_facade_adapter_without_switching_catalog_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, adapter, _authority_input, _v1_input, sealed = _v2_candidate()
    descriptor = fixture.authority.plugin_catalog_snapshot.registration_descriptors[0]

    class _CatalogRuntime:
        snapshot = fixture.authority.plugin_catalog_snapshot

        @staticmethod
        def plugin_key_for_new_instance(algo_code: str):
            if algo_code != descriptor.manifest.algo_code:
                raise KeyError(algo_code)
            return descriptor.plugin_key

        @staticmethod
        def descriptor_for_restore(plugin_key):
            if plugin_key != descriptor.plugin_key:
                raise KeyError(plugin_key)
            return descriptor

    monkeypatch.setattr(
        kernel_creation_module,
        "resolve_plugin_for_restore_v1",
        lambda **_values: ResolvedKernelPluginV1(
            plugin=adapter,
            descriptor=descriptor,
            state_codec=lambda _manifest, state: state,
        ),
    )
    request = _request()
    contract = {
        "symbol": request.symbol,
        "gateway_name": fixture.gateway.gateway_backend,
        "min_volume": "100",
        "volume_increment": "100",
        "pricetick_decimal": "0.01",
    }
    capability = fixture.gateway.model_dump(mode="json")
    contract_sha = hash_hex_v1("miniqmt_contract_projection_v1", contract)
    capability_sha = hash_hex_v1("miniqmt_market_capability_projection_v1", capability)
    refs = tuple(
        ExecutionProjectionRefV1.create(
            projection_type=item.projection_type,
            projection_id=item.projection_id,
            projection_version=item.projection_version,
            payload_sha256=(
                contract_sha
                if item.projection_type is KernelProjectionTypeV1.CONTRACT
                else capability_sha
                if item.projection_type is KernelProjectionTypeV1.MARKET_CAPABILITY
                else item.payload_sha256
            ),
            source_event_id=item.source_event_id,
            logical_at_utc=item.logical_at_utc,
        )
        for item in request.projection_refs
    )
    request_payload = request.model_dump(mode="python")
    request_payload.update(
        {
            "contract_projection": contract,
            "contract_projection_sha256": contract_sha,
            "market_capability_projection": capability,
            "market_capability_projection_sha256": capability_sha,
            "projection_refs": tuple(sorted(refs, key=lambda item: item.projection_type.value)),
        }
    )
    request = type(request).model_validate(request_payload, strict=True)
    result = KernelAlgoCreationCoordinatorV1(
        repository=_CapturingRepository(),
        catalog_runtime=_CatalogRuntime(),  # type: ignore[arg-type]
        gateway_catalog=fixture.gateway,
        facade_authority=sealed,
    ).create(request)

    assert result["algo"].status.value == "ACTIVE", (
        result["receipt"].stable_reason_code,
        thaw_json_v1(result["receipt"].bounded_context),
    )
    assert result["delivery"].status.value == "APPLIED"
    assert result["receipt"].ordered_command_ids == ()


@pytest.mark.parametrize("trigger_submit", [False, True])
def test_k2_delivery_worker_consumes_same_transaction_facade_readset_without_product_switch(
    monkeypatch: pytest.MonkeyPatch,
    trigger_submit: bool,
) -> None:
    fixture, adapter, _authority_input, v1_input, sealed = _v2_candidate()
    initialization = VnpyFacadeInitializationInputV2.create(
        start_event=v1_input.start_event,
        start_delivery=v1_input.start_delivery,
        start_context=v1_input.start_context,
        authority_input=VnpyFacadeAuthorityInputV2.create(
            conformance_authority=sealed,
            plugin_catalog_snapshot=fixture.authority.plugin_catalog_snapshot,
            gateway_capability_catalog=fixture.gateway,
            plugin_key=fixture.authority.plugin_key,
            manifest=fixture.manifest,
            pinned_compatibility_receipt=fixture.k1_receipt,
            route_compatibility_receipt=fixture.authority.route_compatibility_receipt,
        ),
    )
    initialized = adapter.initialize_with_facade_v2(initialization)
    v1_transition = _transition(
        fixture,
        v1_input,
        initialized.next_state,
        market_overrides=(
            None
            if trigger_submit
            else {
                "bid_price_1": "10.40",
                "ask_price_1": "10.50",
                "last_price": "10.40",
            }
        ),
    )
    market_data_id = thaw_json_v1(v1_transition.runtime_event.source_identity)["market_data_id"]
    projection_refs = tuple(
        ExecutionProjectionRefV1.create(
            projection_type=item.projection_type,
            projection_id=(
                market_data_id if item.projection_type is KernelProjectionTypeV1.MARKET_DATA else item.projection_id
            ),
            projection_version=(
                "miniqmt_market_data_projection_v2"
                if item.projection_type is KernelProjectionTypeV1.MARKET_DATA
                else item.projection_version
            ),
            payload_sha256=item.payload_sha256,
            source_event_id=item.source_event_id,
            logical_at_utc=item.logical_at_utc,
        )
        for item in v1_transition.read_only_services.execution_projection_set.ordered_projection_refs
    )
    if trigger_submit:
        projection_refs = (
            *projection_refs,
            *tuple(
                item
                for item in _command_projection_refs(v1_transition.runtime_event)
                if item.projection_type is not KernelProjectionTypeV1.MARKET_DATA
            ),
        )
    projection_refs = tuple(
        sorted(
            projection_refs,
            key=lambda item: (item.projection_type.value, item.projection_id),
        )
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=v1_transition.runtime_event.runtime_id,
        algo_instance_id=v1_transition.delivery.algo_instance_id,
        event_id=v1_transition.runtime_event.event_id,
        delivery_id=v1_transition.delivery.delivery_id,
        projection_refs=projection_refs,
    )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=v1_transition.runtime_event.runtime_id,
        algo_instance_id=v1_transition.delivery.algo_instance_id,
        event_id=v1_transition.runtime_event.event_id,
        delivery_id=v1_transition.delivery.delivery_id,
        contract_projection_id=v1_transition.read_only_services.contract_projection_id,
        contract_projection=thaw_json_v1(v1_transition.read_only_services.contract_projection),
        market_data_projection_id=market_data_id,
        market_data_projection=thaw_json_v1(v1_transition.read_only_services.market_data_projection),
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    claimed_payload = v1_transition.delivery.model_dump(mode="python")
    claimed_payload["transition_id"] = None
    claimed_delivery = AlgoDeliveryPersistenceV1.model_validate(claimed_payload)
    deterministic_context = DeterministicExecutionContextV1.create(
        **{
            **v1_transition.deterministic_context.model_dump(
                mode="python",
                exclude={"schema_version", "context_sha256", "input_projection_sha256"},
            ),
            "input_projection_sha256": (services.execution_projection_set.projection_set_sha256),
        }
    )
    start_delivery_payload = v1_input.start_delivery.model_dump(mode="python")
    start_delivery_payload.update(
        status=DeliveryStatusV1.APPLIED,
        attempt_count=1,
        transition_id=algo_transition_id_v1(
            delivery_id=v1_input.start_delivery.delivery_id,
            event_id=v1_input.start_event.event_id,
            runtime_id=v1_input.start_event.runtime_id,
            algo_instance_id=v1_input.start_delivery.algo_instance_id,
            transition_sequence=1,
        ),
        updated_at_utc=v1_input.start_event.event_time_utc,
    )
    start_delivery = AlgoDeliveryPersistenceV1.create(
        delivery=AlgoEventDeliveryV1.model_validate(start_delivery_payload),
        lease_epoch=1,
        lease_fence_token=None,
        row_version=2,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        closed_at_utc=v1_input.start_event.event_time_utc,
    )
    start_read = VnpyFacadeRepositoryEventReadV1.create(
        read_kind=VnpyFacadeRepositoryReadKindV1.ALGO_START,
        runtime_id=claimed_delivery.runtime_id,
        algo_instance_id=claimed_delivery.algo_instance_id,
        cutoff_delivery_sequence_or_null=None,
        cutoff_event_sequence_or_null=None,
        event=v1_input.start_event,
        delivery=start_delivery,
    )
    read_request = VnpyFacadeRepositoryReadRequestV1.create(
        runtime_id=claimed_delivery.runtime_id,
        algo_instance_id=claimed_delivery.algo_instance_id,
        current_event_id=v1_transition.runtime_event.event_id,
        current_event_sequence=v1_transition.runtime_event.sequence,
        current_delivery_id=claimed_delivery.delivery_id,
        current_delivery_sequence=claimed_delivery.algo_delivery_sequence,
        exchange_trade_date=deterministic_context.exchange_trade_date,
        session_epoch=deterministic_context.session_epoch,
        session_phase=deterministic_context.session_phase,
    )
    read_set = VnpyFacadeRepositoryReadSetV1.create(
        request=read_request,
        algo_start_read=start_read,
        latest_prior_tick_read_or_null=None,
    )

    class _Repository:
        def __init__(self) -> None:
            self.bundle = None

        def read_delivery(self, _delivery_id):
            return claimed_delivery

        def read_algo_instance(self, _algo_instance_id):
            return v1_transition.algo_instance

        def apply_claimed_delivery_atomic(self, **values):
            assert values["facade_read_request"] == read_request
            self.bundle = values["bundle_builder"](
                v1_transition.runtime_event,
                claimed_delivery,
                v1_transition.algo_instance,
                initialized.next_state,
                (),
                (),
                (),
                read_set,
            )
            return {"bundle": self.bundle}

    descriptor = fixture.authority.plugin_catalog_snapshot.registration_descriptors[0]
    monkeypatch.setattr(
        kernel_delivery_module,
        "resolve_plugin_for_restore_v1",
        lambda **_values: ResolvedKernelPluginV1(
            plugin=adapter,
            descriptor=descriptor,
            state_codec=lambda _manifest, state: state,
        ),
    )
    repository = _Repository()
    worker = KernelDeliveryWorkerV1(
        repository=repository,
        catalog_runtime=type("Catalog", (), {"snapshot": fixture.authority.plugin_catalog_snapshot})(),
        worker_id="worker_k4",
        process_incarnation_id="incarnation_k4",
        facade_authority=sealed,
        gateway_catalog=fixture.gateway,
    )
    result = worker.process_once(
        delivery_id=claimed_delivery.delivery_id,
        lease_expires_at=claimed_delivery.lease_expires_at,
        logical_time_utc=v1_transition.runtime_event.event_time_utc,
        facade_read_request=read_request,
        input_builder=lambda *_args: KernelDeliveryExecutionInputV1(
            services=services,
            deterministic_context=deterministic_context,
            consumed_lineage_refs=_event_lineage(
                v1_transition.runtime_event,
                services.execution_projection_set,
            ),
            command_lifecycle_projection=_args[-2],
        ),
    )

    assert result["bundle"] == repository.bundle
    assert repository.bundle.receipt.schema_version == "miniqmt_algo_transition_receipt_v1", (
        repository.bundle.receipt.stable_reason_code,
        thaw_json_v1(repository.bundle.receipt.bounded_context),
    )
    assert len(repository.bundle.receipt.ordered_command_ids) == int(trigger_submit)
    assert len(repository.bundle.new_child_mappings) == int(trigger_submit)
    assert len(repository.bundle.command_outboxes) == int(trigger_submit)
