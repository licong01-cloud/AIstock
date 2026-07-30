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
    KernelAlgoStartWriteBundleV1,
    KernelPluginInvocationError,
    invoke_plugin_initialize_v1,
    resolve_plugin_for_restore_v1,
    validate_vnpy_facade_k2_shadow_command_authority_v1,
)
from .kernel_materializer import materialize_applied_transition_v1, materialize_failure_transition_v1
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
        if not isinstance(request, KernelAlgoCreationRequestV1):
            raise TypeError("request must be KernelAlgoCreationRequestV1")
        request.validate_hashes_v1()
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
            return RuntimeEventEnvelopeV2.create(
                runtime_id=request.runtime_id,
                sequence=sequence,
                event_type=EventTypeV2.ALGO_START,
                event_time_utc=request.logical_time_utc,
                monotonic_ns=None,
                source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
                symbol=request.symbol,
                payload_schema_version="miniqmt_algo_start_v1",
                payload={
                    "parent_intent_id": request.parent_intent_id,
                    "strategy_slot_id": request.strategy_slot_id,
                    "target_quantity": request.parent_quantity,
                    "execution_plan_id": request.execution_plan_id,
                    "execution_plan_sha256": request.execution_plan_sha256,
                    "release_id": request.release_id,
                    "release_sha256": request.release_sha256,
                    "policy_id": request.policy_id,
                    "policy_sha256": request.policy_sha256,
                    "route_receipt_sha256": route_receipt.receipt_sha256,
                    "route_compatibility_receipt": route_receipt.model_dump(mode="json"),
                    "gateway_capability_catalog": self._gateway_catalog.model_dump(mode="json"),
                    "plugin_catalog_sha256": self._catalog_snapshot.catalog_sha256,
                },
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
                if facade_input is not None:
                    validate_vnpy_facade_k2_shadow_command_authority_v1(initialization)
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
        return self._repository.initialize_algo_atomic(
            runtime_id=request.runtime_id,
            event_key_sha256=probe.event_key_sha256,
            creation_authority=request,
            bundle_builder=build_bundle,
        )


__all__ = ["KernelAlgoCreationCoordinatorV1", "KernelAlgoCreationRepositoryV1"]
