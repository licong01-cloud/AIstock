"""K2-B atomic ALGO_START coordinator for the shadow execution kernel."""

from __future__ import annotations

from typing import Any, Protocol

from backend.execution_algos.vnpy_compat.facade_adapter import VnpyFacadeBackedPluginAdapterV1
from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAuthorityInputV2,
    VnpyFacadeConformanceAuthorityV2,
    VnpyFacadeInitializationInputV2,
)

from .kernel_delivery import (
    KernelAlgoCreationRequestV1,
    KernelAlgoCreationRequestV2,
    KernelAlgoStartWriteBundleV1,
    KernelPluginInvocationError,
    invoke_plugin_initialize_v1,
    resolve_plugin_for_restore_v1,
    validate_vnpy_facade_k6_product_command_trace_v1,
    validate_vnpy_facade_k2_shadow_command_authority_v1,
)
from .kernel_materializer import materialize_applied_transition_v1, materialize_failure_transition_v1
from .full_five_catalog_authority import FULL_FIVE_ALGO_CODES_V1, build_hot_full_five_catalog_authority_v1
from .plugin_canonical import thaw_json_v1
from .plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    AlgoStartContextV1,
    AlgoTransitionV1,
    DeliveryStatusV1,
    DeterministicExecutionContextV1,
    ConsumedLineageRefV1,
    ConsumedLineageTypeV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionProjectionSetV1,
    ExecutionProjectionRefV1,
    GatewayCapabilityCatalogV1,
    KernelProjectionTypeV1,
    KernelCommandLifecycleProjectionV1,
    RuntimeEventEnvelopeV2,
    _algo_instance_id_v2,
    stable_exception_reason_code_v1,
)
from .plugin_registry import (
    CompatibilityStatusV1,
    PluginCatalogRuntimeV2,
    PluginCatalogSnapshotV1,
    PluginRouteCompatibilityReceiptV1,
)


class KernelAlgoCreationRepositoryV1(Protocol):
    def initialize_algo_atomic(
        self,
        *,
        runtime_id: str,
        event_key_sha256: str,
        creation_authority: KernelAlgoCreationRequestV1,
        bundle_builder: Any,
    ) -> dict[str, Any]: ...


class KernelAlgoCreationRepositoryV2(Protocol):
    def initialize_product_algo_atomic_v3(
        self,
        *,
        runtime_id: str,
        worker_incarnation_id: str,
        event_key_sha256: str,
        creation_authority: KernelAlgoCreationRequestV2,
        creation_binding: VnpyFacadeAuthorityInputV2,
        bundle_builder: Any,
    ) -> dict[str, Any]: ...


class KernelAlgoCreationCoordinatorV1:
    def __init__(
        self,
        *,
        repository: KernelAlgoCreationRepositoryV1,
        catalog_runtime: PluginCatalogRuntimeV2,
        gateway_catalog: GatewayCapabilityCatalogV1,
        facade_authority: VnpyFacadeConformanceAuthorityV2 | None = None,
    ) -> None:
        self._repository = repository
        self._catalog_runtime = catalog_runtime
        self._catalog_snapshot = PluginCatalogSnapshotV1.model_validate(
            catalog_runtime.snapshot.model_dump(mode="python"), strict=True
        )
        self._gateway_catalog = GatewayCapabilityCatalogV1.model_validate(
            gateway_catalog.model_dump(mode="python"), strict=True
        )
        if facade_authority is not None and not isinstance(facade_authority, VnpyFacadeConformanceAuthorityV2):
            raise TypeError("facade_authority must be VnpyFacadeConformanceAuthorityV2 or None")
        self._facade_authority = facade_authority

    def create(self, request: KernelAlgoCreationRequestV1) -> dict[str, Any]:
        if type(request) is not KernelAlgoCreationRequestV1:
            raise TypeError("request must be KernelAlgoCreationRequestV1")
        return self._create(request, final_product_route=False)

    def _create(
        self,
        request: KernelAlgoCreationRequestV1 | KernelAlgoCreationRequestV2,
        *,
        final_product_route: bool,
    ) -> dict[str, Any]:
        request.validate_hashes_v1()
        if final_product_route:
            if type(request) is not KernelAlgoCreationRequestV2:
                raise TypeError("final product creation requires KernelAlgoCreationRequestV2")
            request.validate_hashes_v2()
        elif type(request) is not KernelAlgoCreationRequestV1:
            raise TypeError("shadow creation requires an exact KernelAlgoCreationRequestV1")
        try:
            plugin_key = self._catalog_runtime.plugin_key_for_new_instance(request.algo_code)
            descriptor = self._catalog_runtime.descriptor_for_restore(plugin_key)
        except KeyError as exc:
            raise KernelPluginInvocationError(
                "MINIQMT_ALGO_PLUGIN_BINDING_INVALID",
                "creation binding does not resolve to one exact frozen plugin",
                context={"algo_code": request.algo_code},
                broker_called=False,
            ) from exc
        route_receipt = PluginRouteCompatibilityReceiptV1.create(
            catalog_snapshot=self._catalog_snapshot,
            plugin_key=plugin_key,
            gateway_catalog=self._gateway_catalog,
        ).validate_against_authority_v1(
            catalog_snapshot=self._catalog_snapshot,
            gateway_catalog=self._gateway_catalog,
        )
        if route_receipt.status is not CompatibilityStatusV1.PASSED:
            raise KernelPluginInvocationError(
                "MINIQMT_ALGO_ROUTE_COMPATIBILITY_FAILED",
                "exact plugin/route capability receipt is not PASSED",
                context={
                    "algo_code": request.algo_code,
                    "plugin_id": descriptor.manifest.plugin_id,
                    "route_id": self._gateway_catalog.route_id,
                    "route_receipt_sha256": route_receipt.receipt_sha256,
                    "failures": [
                        {"field_path": item.field_path, "context_sha256": item.context_sha256}
                        for item in route_receipt.ordered_failures
                    ],
                },
                broker_called=False,
            )
        product_creation_binding: VnpyFacadeAuthorityInputV2 | None = None
        if final_product_route:
            if self._facade_authority is None:
                raise KernelPluginInvocationError(
                    "MINIQMT_K6_PRODUCT_FACADE_AUTHORITY_INVALID",
                    "final product creation requires sealed full-five conformance authority",
                    context={"algo_code": request.algo_code},
                    broker_called=False,
                )
            k1_receipts = tuple(
                item for item in self._catalog_snapshot.pinned_compatibility_receipts if item.plugin_key == plugin_key
            )
            if len(k1_receipts) != 1:
                raise KernelPluginInvocationError(
                    "MINIQMT_K6_PRODUCT_COMPATIBILITY_AUTHORITY_INVALID",
                    "final product creation requires one exact pinned compatibility receipt",
                    context={"plugin_key": plugin_key.canonical_payload_v1()},
                    broker_called=False,
                )
            product_creation_binding = VnpyFacadeAuthorityInputV2.create(
                conformance_authority=self._facade_authority,
                plugin_catalog_snapshot=self._catalog_snapshot,
                gateway_capability_catalog=self._gateway_catalog,
                plugin_key=plugin_key,
                manifest=descriptor.manifest,
                pinned_compatibility_receipt=k1_receipts[0],
                route_compatibility_receipt=route_receipt,
            )
        manifest = descriptor.manifest
        route_projection_ref = ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.ROUTE_COMPATIBILITY,
            projection_id="mqroutecompat_" + route_receipt.receipt_sha256,
            projection_version="plugin_route_compatibility_receipt_v1",
            payload_sha256=route_receipt.receipt_sha256,
            source_event_id=None,
            logical_at_utc=request.logical_time_utc,
        )
        projection_refs = tuple(
            sorted(
                (*request.projection_refs, route_projection_ref),
                key=lambda item: (item.projection_type.value, item.projection_id),
            )
        )
        algo_instance_id = _algo_instance_id_v2(
            runtime_id=request.runtime_id,
            parent_intent_id=request.parent_intent_id,
            strategy_slot_id=request.strategy_slot_id,
            algo_code=manifest.algo_code,
            plugin_id=manifest.plugin_id,
            plugin_version=manifest.plugin_version,
            plugin_manifest_sha256=manifest.manifest_sha256,
            plugin_config_sha256=request.plugin_config_sha256,
        )

        def build_event(sequence: int) -> RuntimeEventEnvelopeV2:
            payload: dict[str, Any] = {
                "parent_intent_id": request.parent_intent_id,
                "strategy_slot_id": request.strategy_slot_id,
                "target_quantity": request.parent_quantity,
                "execution_plan_id": request.execution_plan_id,
                "execution_plan_sha256": request.execution_plan_sha256,
                "release_id": request.release_id,
                "release_sha256": request.release_sha256,
                "policy_id": request.policy_id,
                "policy_sha256": request.policy_sha256,
                "gateway_capability_catalog": self._gateway_catalog.model_dump(mode="json"),
                "plugin_catalog_sha256": self._catalog_snapshot.catalog_sha256,
            }
            if final_product_route:
                assert type(request) is KernelAlgoCreationRequestV2
                payload.update(
                    {
                        "plugin_route_compatibility_receipt_sha256": route_receipt.receipt_sha256,
                        "plugin_route_compatibility_receipt": route_receipt.model_dump(mode="json"),
                        "product_route_cutover_receipt_sha256": request.product_route_cutover_receipt_sha256,
                        "product_route_owner_sha256": request.product_route_owner_sha256,
                        "product_route_epoch": request.product_route_epoch,
                        "effective_new_instance_sequence": request.effective_new_instance_sequence,
                        "binding_id": request.binding_id,
                        "creation_request_sha256": request.creation_request_sha256,
                    }
                )
            else:
                payload.update(
                    {
                        "route_receipt_sha256": route_receipt.receipt_sha256,
                        "route_compatibility_receipt": route_receipt.model_dump(mode="json"),
                    }
                )
            return RuntimeEventEnvelopeV2.create(
                runtime_id=request.runtime_id,
                sequence=sequence,
                event_type=EventTypeV2.ALGO_START,
                event_time_utc=request.logical_time_utc,
                monotonic_ns=None,
                source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
                symbol=request.symbol,
                payload_schema_version="miniqmt_algo_start_v2" if final_product_route else "miniqmt_algo_start_v1",
                payload=payload,
                source_identity={
                    "algo_instance_id": algo_instance_id,
                    "runtime_id": request.runtime_id,
                    "parent_intent_id": request.parent_intent_id,
                    "strategy_slot_id": request.strategy_slot_id,
                    "algo_code": manifest.algo_code,
                    "plugin_id": manifest.plugin_id,
                    "plugin_version": manifest.plugin_version,
                    "plugin_manifest_sha256": manifest.manifest_sha256,
                    "plugin_config_sha256": request.plugin_config_sha256,
                },
                correlation={
                    "execution_plan_id": request.execution_plan_id,
                    "release_id": request.release_id,
                    "policy_id": request.policy_id,
                    "binding_id": request.binding_id if final_product_route else None,
                    "exchange_trade_date": request.exchange_trade_date,
                    "session_epoch": request.session_epoch,
                    "session_phase": request.session_phase.value,
                },
            )

        def build_bundle(sequence: int) -> KernelAlgoStartWriteBundleV1:
            event = build_event(sequence)
            event_delivery = AlgoEventDeliveryV1.create(
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
                created_at_utc=request.logical_time_utc,
                updated_at_utc=request.logical_time_utc,
            )
            initial_delivery = AlgoDeliveryPersistenceV1.create(
                delivery=event_delivery,
                lease_epoch=0,
                lease_fence_token=None,
                row_version=1,
                next_attempt_at_utc=None,
                failure_receipt_id=None,
                skip_receipt_id=None,
                closed_at_utc=None,
            )
            projection_set = ExecutionProjectionSetV1.create(
                runtime_id=request.runtime_id,
                algo_instance_id=algo_instance_id,
                event_id=event.event_id,
                delivery_id=initial_delivery.delivery_id,
                projection_refs=projection_refs,
            )
            deterministic_context = DeterministicExecutionContextV1.create(
                runtime_id=request.runtime_id,
                algo_instance_id=algo_instance_id,
                event_id=event.event_id,
                delivery_id=initial_delivery.delivery_id,
                plugin_manifest_sha256=manifest.manifest_sha256,
                transition_sequence=1,
                logical_time_utc=request.logical_time_utc,
                exchange_trade_date=request.exchange_trade_date,
                session_epoch=request.session_epoch,
                session_phase=request.session_phase,
                input_projection_sha256=projection_set.projection_set_sha256,
            )
            start_context = AlgoStartContextV1(
                schema_version="miniqmt_algo_start_context_v1",
                runtime_id=request.runtime_id,
                algo_instance_id=algo_instance_id,
                parent_intent_id=request.parent_intent_id,
                strategy_slot_id=request.strategy_slot_id,
                symbol=request.symbol,
                side=request.side,
                limit_price_decimal=request.limit_price_decimal,
                parent_quantity=request.parent_quantity,
                min_volume=request.min_volume,
                volume_increment=request.volume_increment,
                plugin_manifest=manifest,
                plugin_config=thaw_json_v1(request.plugin_config),
                plugin_config_sha256=request.plugin_config_sha256,
                start_event_id=event.event_id,
                start_delivery_id=initial_delivery.delivery_id,
                deterministic_context=deterministic_context,
                contract_projection=thaw_json_v1(request.contract_projection),
                contract_projection_sha256=request.contract_projection_sha256,
                account_projection=thaw_json_v1(request.account_projection),
                account_projection_sha256=request.account_projection_sha256,
                market_capability_projection=thaw_json_v1(request.market_capability_projection),
                market_capability_projection_sha256=request.market_capability_projection_sha256,
                execution_plan_id=request.execution_plan_id,
                execution_plan_sha256=request.execution_plan_sha256,
                release_id=request.release_id,
                release_sha256=request.release_sha256,
                policy_id=request.policy_id,
                policy_sha256=request.policy_sha256,
            )
            try:
                resolved = resolve_plugin_for_restore_v1(
                    catalog_runtime=self._catalog_runtime,
                    plugin_id=manifest.plugin_id,
                    plugin_version=manifest.plugin_version,
                    plugin_manifest_sha256=manifest.manifest_sha256,
                    canonical_plugin_config=thaw_json_v1(request.plugin_config),
                    plugin_config_sha256=request.plugin_config_sha256,
                )
                facade_input = None
                if isinstance(resolved.plugin, VnpyFacadeBackedPluginAdapterV1):
                    if self._facade_authority is not None:
                        k1_receipts = tuple(
                            item
                            for item in self._catalog_snapshot.pinned_compatibility_receipts
                            if item.plugin_key == plugin_key
                        )
                        if len(k1_receipts) != 1:
                            raise KernelPluginInvocationError(
                                "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
                                "facade initialization requires one exact K1 compatibility receipt",
                                context={"plugin_key": plugin_key.canonical_payload_v1()},
                                broker_called=False,
                            )
                        authority_input = VnpyFacadeAuthorityInputV2.create(
                            conformance_authority=self._facade_authority,
                            plugin_catalog_snapshot=self._catalog_snapshot,
                            gateway_capability_catalog=self._gateway_catalog,
                            plugin_key=plugin_key,
                            manifest=manifest,
                            pinned_compatibility_receipt=k1_receipts[0],
                            route_compatibility_receipt=route_receipt,
                        )
                        facade_input = VnpyFacadeInitializationInputV2.create(
                            start_event=event,
                            start_delivery=event_delivery,
                            start_context=start_context,
                            authority_input=authority_input,
                        )
                initialization = invoke_plugin_initialize_v1(
                    plugin=resolved.plugin,
                    expected_manifest=manifest,
                    start_context=start_context,
                    facade_input=facade_input,
                )
                if facade_input is not None and final_product_route:
                    validate_vnpy_facade_k6_product_command_trace_v1(initialization)
                elif facade_input is not None:
                    validate_vnpy_facade_k2_shadow_command_authority_v1(initialization)
                if final_product_route and initialization.broker_commands:
                    raise KernelPluginInvocationError(
                        "MINIQMT_K6_PRODUCT_ALGO_START_COMMAND_FORBIDDEN",
                        "final product ALGO_START may initialize state/timers but cannot emit broker commands",
                        context={
                            "runtime_id": request.runtime_id,
                            "parent_intent_id": request.parent_intent_id,
                            "ordered_command_ids": [item.command_id for item in initialization.broker_commands],
                        },
                        broker_called=False,
                    )
                transition = AlgoTransitionV1(
                    schema_version="miniqmt_algo_transition_v1",
                    next_state=initialization.next_state,
                    broker_commands=initialization.broker_commands,
                    timer_mutations=initialization.timer_mutations,
                    diagnostic_observations=initialization.diagnostic_observations,
                    terminal_outcome=initialization.terminal_outcome,
                    effect_set_sha256=initialization.effect_set_sha256,
                )
                transition_bundle = materialize_applied_transition_v1(
                    event=event,
                    predecessor_delivery=initial_delivery,
                    previous_algo=None,
                    transition=transition,
                    projection_set=projection_set,
                    consumed_lineage_refs=(
                        ConsumedLineageRefV1.create(
                            lineage_type=ConsumedLineageTypeV1.EVENT,
                            identity=event.event_id,
                            payload_sha256=event.payload_sha256,
                        ),
                    ),
                    strategy_slot_id=request.strategy_slot_id,
                    parent_intent_id=request.parent_intent_id,
                    compatibility_receipt_sha256=route_receipt.receipt_sha256,
                    plugin_config=thaw_json_v1(request.plugin_config),
                    plugin_config_sha256=request.plugin_config_sha256,
                    target_quantity=request.parent_quantity,
                    algo_code=manifest.algo_code,
                    symbol=request.symbol,
                    side=request.side,
                    command_lifecycle_projection=KernelCommandLifecycleProjectionV1.create(
                        runtime_id=event.runtime_id,
                        algo_instance_id=initialization.next_state.algo_instance_id,
                        event_id=event.event_id,
                        delivery_id=initial_delivery.delivery_id,
                        ordered_items=(),
                    ),
                    existing_mappings_by_local_vt_orderid={},
                    existing_timer_schedules={},
                    initialization=True,
                )
            except Exception as exc:
                transition_bundle = materialize_failure_transition_v1(
                    event=event,
                    predecessor_delivery=initial_delivery,
                    previous_algo=None,
                    algo_code=manifest.algo_code,
                    plugin_id=manifest.plugin_id,
                    plugin_version=manifest.plugin_version,
                    plugin_manifest_sha256=manifest.manifest_sha256,
                    plugin_config=thaw_json_v1(request.plugin_config),
                    plugin_config_sha256=request.plugin_config_sha256,
                    compatibility_receipt_sha256=route_receipt.receipt_sha256,
                    parent_intent_id=request.parent_intent_id,
                    strategy_slot_id=request.strategy_slot_id,
                    symbol=request.symbol,
                    side=request.side,
                    target_quantity=request.parent_quantity,
                    stable_reason_code=stable_exception_reason_code_v1(
                        exc, default="MINIQMT_ALGO_INITIALIZATION_FAILED"
                    ),
                    exception=exc,
                    failure_context=getattr(exc, "context", {"stage": "ALGO_INITIALIZATION"}),
                    projection_set=projection_set,
                    active_mappings=(),
                    active_timer_schedules=(),
                    logical_time_utc=request.logical_time_utc,
                    initialization=True,
                )
            return KernelAlgoStartWriteBundleV1(
                event=event,
                initial_delivery=initial_delivery,
                transition_bundle=transition_bundle,
            )

        probe = build_event(1)
        if final_product_route:
            assert type(request) is KernelAlgoCreationRequestV2
            assert product_creation_binding is not None
            initialize_v3 = getattr(self._repository, "initialize_product_algo_atomic_v3", None)
            if not callable(initialize_v3):
                raise TypeError(
                    "repository must implement initialize_product_algo_atomic_v3 for final product creation"
                )
            return initialize_v3(
                runtime_id=request.runtime_id,
                worker_incarnation_id=self._product_worker_incarnation_id,
                event_key_sha256=probe.event_key_sha256,
                creation_authority=request,
                creation_binding=product_creation_binding,
                bundle_builder=build_bundle,
            )
        return self._repository.initialize_algo_atomic(
            runtime_id=request.runtime_id,
            event_key_sha256=probe.event_key_sha256,
            creation_authority=request,
            bundle_builder=build_bundle,
        )


class KernelAlgoCreationCoordinatorV2(KernelAlgoCreationCoordinatorV1):
    """Final K6-D product creation entry; V1 shadow requests are not accepted."""

    def __init__(
        self,
        *,
        repository: KernelAlgoCreationRepositoryV2,
        catalog_runtime: PluginCatalogRuntimeV2,
        gateway_catalog: GatewayCapabilityCatalogV1,
        worker_incarnation_id: str,
        facade_authority: VnpyFacadeConformanceAuthorityV2,
    ) -> None:
        if not callable(getattr(repository, "initialize_product_algo_atomic_v3", None)):
            raise TypeError("repository must implement initialize_product_algo_atomic_v3")
        if (
            type(worker_incarnation_id) is not str
            or not worker_incarnation_id
            or (worker_incarnation_id != worker_incarnation_id.strip())
        ):
            raise TypeError("worker_incarnation_id must be a non-empty canonical string")
        if not isinstance(facade_authority, VnpyFacadeConformanceAuthorityV2):
            raise TypeError("final product creation requires VnpyFacadeConformanceAuthorityV2")
        strict_gateway = GatewayCapabilityCatalogV1.model_validate(
            gateway_catalog.model_dump(mode="python"), strict=True
        )
        full_authority = build_hot_full_five_catalog_authority_v1(gateway_catalog=strict_gateway)
        supplied_snapshot = PluginCatalogSnapshotV1.model_validate(
            catalog_runtime.snapshot.model_dump(mode="python"), strict=True
        )
        expected_snapshot = PluginCatalogSnapshotV1.model_validate(
            full_authority.catalog_runtime.snapshot.model_dump(mode="python"), strict=True
        )
        actual_algos = tuple(item.manifest.algo_code for item in supplied_snapshot.registration_descriptors)
        if actual_algos != FULL_FIVE_ALGO_CODES_V1 or supplied_snapshot != expected_snapshot:
            raise KernelPluginInvocationError(
                "MINIQMT_K6_PRODUCT_CATALOG_AUTHORITY_INVALID",
                "final product creation requires the independently rebuilt exact full-five plugin catalog",
                context={
                    "expected_algo_codes": list(FULL_FIVE_ALGO_CODES_V1),
                    "actual_algo_codes": list(actual_algos),
                    "expected_catalog_sha256": expected_snapshot.catalog_sha256,
                    "actual_catalog_sha256": supplied_snapshot.catalog_sha256,
                },
                broker_called=False,
            )
        expected_facade_authority = full_authority.conformance_authority
        if (
            facade_authority.conformance_set != expected_facade_authority.conformance_set
            or facade_authority.source_executor_binding != expected_facade_authority.source_executor_binding
            or facade_authority.source_execution_sets != expected_facade_authority.source_execution_sets
            or facade_authority.characterization_receipts != expected_facade_authority.characterization_receipts
            or facade_authority.algorithm_bindings != expected_facade_authority.algorithm_bindings
            or facade_authority.validation_receipt != expected_facade_authority.validation_receipt
        ):
            raise KernelPluginInvocationError(
                "MINIQMT_K6_PRODUCT_FACADE_AUTHORITY_INVALID",
                "final product creation facade authority differs from the independently rebuilt full-five source authority",
                context={
                    "expected_receipt_set_sha256": expected_facade_authority.conformance_set.receipt_set_sha256,
                    "actual_receipt_set_sha256": facade_authority.conformance_set.receipt_set_sha256,
                },
                broker_called=False,
            )
        super().__init__(
            repository=repository,  # type: ignore[arg-type]
            catalog_runtime=catalog_runtime,
            gateway_catalog=strict_gateway,
            facade_authority=facade_authority,
        )
        self._product_worker_incarnation_id = worker_incarnation_id

    def create(self, request: KernelAlgoCreationRequestV2) -> dict[str, Any]:
        if type(request) is not KernelAlgoCreationRequestV2:
            raise TypeError("request must be KernelAlgoCreationRequestV2")
        return self._create(request, final_product_route=True)


__all__ = [
    "KernelAlgoCreationCoordinatorV1",
    "KernelAlgoCreationCoordinatorV2",
    "KernelAlgoCreationRepositoryV1",
    "KernelAlgoCreationRepositoryV2",
]
