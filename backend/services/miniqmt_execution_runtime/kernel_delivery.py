"""Pure K2-B plugin invocation; durable transaction ownership stays in the repository."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable, Literal, Protocol, Sequence

from pydantic import model_validator

from backend.execution_algos.vnpy_compat.facade_adapter import VnpyFacadeBackedPluginAdapterV1
from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAuthorityInputV2,
    VnpyFacadeConformanceAuthorityV2,
    VnpyFacadeContractError,
    VnpyFacadeInitializationInputV2,
    VnpyFacadeRepositoryReadRequestV1,
    VnpyFacadeRepositoryReadSetV1,
    read_vnpy_facade_lifecycle_items_v1,
    VnpyFacadeTransitionInputV2,
)

from .plugin_canonical import freeze_json_v1, hash_hex_v1, json_safe_evidence_v1, thaw_json_v1
from .plugin_contracts import (
    AlgoInitializationV1,
    AlgoDeliveryPersistenceV1,
    AlgoFailureReceiptV1,
    AlgoReadOnlyServicesV1,
    AlgoSkipReceiptV1,
    AlgoStartContextV1,
    AlgoStateSnapshotV2,
    AlgoTransitionReceiptV1,
    AlgoTransitionV1,
    BrokerCommandOutboxV1,
    CurrentThreeActiveOrderStateV3,
    DeterministicExecutionContextV1,
    DiagnosticObservationV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPluginManifestV2,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    ExecutionProjectionRefV1,
    EventTypeV2,
    FrozenJsonObjectFieldV1,
    FrozenStrictModel,
    GatewayCapabilityCatalogV1,
    IdentityV1,
    KernelErrorEvidenceV1,
    KernelCommandLifecycleProjectionItemV1,
    KernelCommandLifecycleProjectionV1,
    KernelCommandOutcomeEventPayloadV1,
    KernelProjectionTypeV1,
    RuntimeEventEnvelopeV2,
    PositiveIntV1,
    SessionPhaseV1,
    Sha256V1,
    SideV1,
    ConsumedLineageRefV1,
    ConsumedLineageTypeV1,
    kernel_lease_fence_token_v1,
    safe_exception_summary_v1,
    stable_exception_reason_code_v1,
    TimerMutationV1,
)
from .plugin_registry import (
    CompatibilityStatusV1,
    ExecutionAlgoPluginV2,
    PluginCatalogRuntimeV2,
    PluginCatalogSnapshotV1,
    PluginKeyV1,
    PluginRegistrationDescriptorV2,
    PluginRouteCompatibilityReceiptV1,
)
from .vnpy_facade_diagnostics import record_vnpy_facade_runtime_invocation_v1


class KernelPluginInvocationError(RuntimeError):
    """Typed deterministic plugin failure with JSON-safe evidence."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        context: dict[str, Any],
        broker_called: bool | None = False,
    ) -> None:
        self.reason_code = reason_code
        self.context = json_safe_evidence_v1(context)
        self.broker_called = broker_called
        super().__init__(message)


@dataclass(frozen=True)
class ResolvedKernelPluginV1:
    plugin: ExecutionAlgoPluginV2
    descriptor: PluginRegistrationDescriptorV2
    state_codec: Callable[[ExecutionAlgoPluginManifestV2, Any], Any]


class KernelAlgoCreationRequestV1(FrozenStrictModel):
    runtime_id: str
    parent_intent_id: str
    strategy_slot_id: str
    symbol: str
    side: SideV1
    limit_price_decimal: str
    parent_quantity: int
    min_volume: int
    volume_increment: int
    algo_code: str
    plugin_config: FrozenJsonObjectFieldV1
    plugin_config_sha256: Sha256V1
    contract_projection: FrozenJsonObjectFieldV1
    contract_projection_sha256: Sha256V1
    account_projection: FrozenJsonObjectFieldV1
    account_projection_sha256: Sha256V1
    market_capability_projection: FrozenJsonObjectFieldV1
    market_capability_projection_sha256: Sha256V1
    projection_refs: tuple[ExecutionProjectionRefV1, ...]
    execution_plan_id: str
    execution_plan_sha256: Sha256V1
    release_id: str
    release_sha256: Sha256V1
    policy_id: str
    policy_sha256: Sha256V1
    logical_time_utc: str
    exchange_trade_date: str
    session_epoch: str
    session_phase: SessionPhaseV1

    def validate_hashes_v1(self) -> "KernelAlgoCreationRequestV1":
        projections = (
            ("miniqmt_plugin_config_v2", self.plugin_config, self.plugin_config_sha256),
            ("miniqmt_contract_projection_v1", self.contract_projection, self.contract_projection_sha256),
            ("miniqmt_account_projection_v1", self.account_projection, self.account_projection_sha256),
            (
                "miniqmt_market_capability_projection_v1",
                self.market_capability_projection,
                self.market_capability_projection_sha256,
            ),
        )
        for domain, payload, supplied in projections:
            if hash_hex_v1(domain, thaw_json_v1(payload)) != supplied:
                raise ValueError(f"{domain} hash differs from creation request payload")
        ref_keys = tuple((item.projection_type.value, item.projection_id) for item in self.projection_refs)
        if ref_keys != tuple(sorted(ref_keys)):
            raise ValueError("creation projection refs must be in canonical type/identity order")
        by_type = {item.projection_type: item for item in self.projection_refs}
        if len(by_type) != len(self.projection_refs):
            raise ValueError("creation projection refs contain duplicate authority types")
        required_refs = {
            KernelProjectionTypeV1.CONTRACT: self.contract_projection_sha256,
            KernelProjectionTypeV1.ACCOUNT: self.account_projection_sha256,
            KernelProjectionTypeV1.MARKET_CAPABILITY: self.market_capability_projection_sha256,
        }
        for projection_type, expected_hash in required_refs.items():
            ref = by_type.get(projection_type)
            if ref is None or ref.payload_sha256 != expected_hash:
                raise ValueError(f"creation {projection_type.value} ref does not close to its frozen payload")
        if KernelProjectionTypeV1.ROUTE_COMPATIBILITY in by_type:
            raise ValueError("route compatibility ref is coordinator-owned and cannot be caller supplied")
        return self


class KernelAlgoCreationRequestV2(KernelAlgoCreationRequestV1):
    """Final K6-D product-creation authority with immutable route lineage."""

    schema_version: Literal["miniqmt_kernel_algo_creation_request_v2"]
    binding_id: IdentityV1
    product_route_cutover_receipt_sha256: Sha256V1
    product_route_owner_sha256: Sha256V1
    product_route_epoch: PositiveIntV1
    effective_new_instance_sequence: PositiveIntV1
    creation_request_sha256: Sha256V1

    @classmethod
    def from_v1(
        cls,
        request: KernelAlgoCreationRequestV1,
        *,
        binding_id: str,
        product_route_cutover_receipt_sha256: str,
        product_route_owner_sha256: str,
        product_route_epoch: int,
        effective_new_instance_sequence: int,
    ) -> "KernelAlgoCreationRequestV2":
        """Promote a frozen V1 authority only after repository-owned route readback."""
        if type(request) is not KernelAlgoCreationRequestV1:
            raise TypeError("request must be an exact KernelAlgoCreationRequestV1 authority")
        request.validate_hashes_v1()
        route_lineage = {
            "schema_version": "miniqmt_kernel_algo_creation_request_v2",
            "binding_id": binding_id,
            "product_route_cutover_receipt_sha256": product_route_cutover_receipt_sha256,
            "product_route_owner_sha256": product_route_owner_sha256,
            "product_route_epoch": product_route_epoch,
            "effective_new_instance_sequence": effective_new_instance_sequence,
        }
        canonical_payload = {**request.model_dump(mode="json"), **route_lineage}
        creation_request_sha256 = hash_hex_v1("miniqmt_kernel_algo_creation_request_v2", canonical_payload)
        return cls(
            **request.model_dump(mode="python"),
            **route_lineage,
            creation_request_sha256=creation_request_sha256,
        )

    def validate_hashes_v2(self) -> "KernelAlgoCreationRequestV2":
        super().validate_hashes_v1()
        expected = hash_hex_v1(
            "miniqmt_kernel_algo_creation_request_v2",
            self.canonical_payload_v1(exclude={"creation_request_sha256"}),
        )
        if self.creation_request_sha256 != expected:
            raise ValueError("product creation request hash differs from frozen route lineage")
        return self

    @model_validator(mode="after")
    def _validate_product_route_lineage_v2(self) -> "KernelAlgoCreationRequestV2":
        return self.validate_hashes_v2()


@dataclass(frozen=True)
class KernelDeliveryExecutionInputV1:
    services: AlgoReadOnlyServicesV1
    deterministic_context: DeterministicExecutionContextV1
    consumed_lineage_refs: tuple[ConsumedLineageRefV1, ...]
    command_lifecycle_projection: KernelCommandLifecycleProjectionV1


def build_command_lifecycle_projection_v1(
    *,
    event: RuntimeEventEnvelopeV2,
    delivery: AlgoDeliveryPersistenceV1,
    previous_state: AlgoStateSnapshotV2,
    mappings: Sequence[ExecutionCommandChildMappingV1],
    outboxes: Sequence[BrokerCommandOutboxV1],
) -> KernelCommandLifecycleProjectionV1:
    state_payload = thaw_json_v1(previous_state.state)
    if state_payload.get("schema_version") == "miniqmt_vnpy_facade_state_envelope_v1":
        active_items = [item.model_dump(mode="json") for item in read_vnpy_facade_lifecycle_items_v1(state_payload)]
    else:
        active_items = state_payload.get("active_orders")
        if not isinstance(active_items, list):
            raise ValueError("durable state must expose active_orders for lifecycle projection")
    state_by_local: dict[str, CurrentThreeActiveOrderStateV3] = {}
    for item in active_items:
        if not isinstance(item, dict):
            raise ValueError("active-order lifecycle state is not a strict object")
        strict_item = CurrentThreeActiveOrderStateV3.model_validate_json(
            json.dumps(item, sort_keys=True, separators=(",", ":"))
        )
        local_id = strict_item.local_vt_orderid
        if local_id in state_by_local:
            raise ValueError("active-order lifecycle state contains duplicate local identities")
        state_by_local[local_id] = strict_item
    mapping_by_local = {item.local_vt_orderid: item for item in mappings}
    if len(mapping_by_local) != len(tuple(mappings)):
        raise ValueError("durable lifecycle mapping set contains duplicate local identities")
    if any(
        item.runtime_id != event.runtime_id or item.algo_instance_id != previous_state.algo_instance_id
        for item in mappings
    ):
        raise ValueError("durable lifecycle mapping set crosses runtime or algo owner")
    relevant_local_ids = sorted(
        set(state_by_local)
        | {
            item.local_vt_orderid
            for item in mappings
            if item.mapping_status.value in {"RESERVED", "DISPATCHING", "BROKER_ACCEPTED", "OUTCOME_UNKNOWN"}
        }
    )
    outbox_by_command = {item.command_id: item for item in outboxes}
    if len(outbox_by_command) != len(tuple(outboxes)):
        raise ValueError("durable lifecycle outbox set contains duplicate command identities")
    if any(
        item.runtime_id != event.runtime_id or item.algo_instance_id != previous_state.algo_instance_id
        for item in outboxes
    ):
        raise ValueError("durable lifecycle outbox set crosses runtime or algo owner")
    strict_outcome = None
    if event.event_type is EventTypeV2.COMMAND_OUTCOME:
        strict_outcome = KernelCommandOutcomeEventPayloadV1.model_validate_json(
            json.dumps(thaw_json_v1(event.payload), sort_keys=True, separators=(",", ":"))
        )
    projection_items: list[KernelCommandLifecycleProjectionItemV1] = []
    for local_id in relevant_local_ids:
        mapping = mapping_by_local.get(local_id)
        state_item = state_by_local.get(local_id)
        if mapping is None or state_item is None:
            raise ValueError("active state and durable mapping lifecycle sets differ")
        command_id = state_item.pending_command_id or state_item.submit_command_id
        outbox = outbox_by_command.get(command_id)
        if outbox is None or outbox.mapping_id != mapping.mapping_id:
            raise ValueError("active state current command has no exact durable outbox")
        outcome_values: dict[str, Any] = {
            "outcome_receipt_sha256": None,
            "latest_command_outcome_event_id": None,
            "latest_command_outcome_payload_sha256": None,
            "command_outcome_delivery_id": None,
            "command_outcome_delivery_status": None,
        }
        if strict_outcome is not None and strict_outcome.command_id == outbox.command_id:
            if (
                strict_outcome.runtime_id != event.runtime_id
                or strict_outcome.algo_instance_id != previous_state.algo_instance_id
                or strict_outcome.parent_intent_id != mapping.parent_intent_id
                or strict_outcome.strategy_slot_id != mapping.strategy_slot_id
                or strict_outcome.mapping_id != mapping.mapping_id
                or strict_outcome.command_type is not outbox.command_type
                or strict_outcome.local_vt_orderid != local_id
                or strict_outcome.outbox_row_version != outbox.row_version
                or strict_outcome.outbox_status != outbox.status.value
                or strict_outcome.broker_order_id not in {None, outbox.broker_order_id, mapping.broker_order_id}
            ):
                raise ValueError("COMMAND_OUTCOME delivery conflicts with locked outbox readback")
            outcome_values = {
                "outcome_receipt_sha256": strict_outcome.outcome_receipt_sha256,
                "latest_command_outcome_event_id": event.event_id,
                "latest_command_outcome_payload_sha256": event.payload_sha256,
                "command_outcome_delivery_id": delivery.delivery_id,
                "command_outcome_delivery_status": delivery.status,
            }
        projection_items.append(
            KernelCommandLifecycleProjectionItemV1(
                mapping_id=mapping.mapping_id,
                mapping_version=mapping.mapping_version,
                mapping_payload_sha256=mapping.payload_sha256,
                local_vt_orderid=mapping.local_vt_orderid,
                submit_command_id=mapping.command_id,
                broker_order_id=mapping.broker_order_id,
                mapping_status=mapping.mapping_status,
                current_outbox_command_id=outbox.command_id,
                current_outbox_command_type=outbox.command_type,
                current_outbox_status=outbox.status,
                current_outbox_row_version=outbox.row_version,
                current_outbox_payload_sha256=outbox.payload_sha256,
                **outcome_values,
            )
        )
    return KernelCommandLifecycleProjectionV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=previous_state.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        ordered_items=tuple(projection_items),
    )


class KernelDeliveryRepositoryV1(Protocol):
    def read_runtime_event(self, event_id: str) -> RuntimeEventEnvelopeV2: ...

    def read_delivery(self, delivery_id: str) -> AlgoDeliveryPersistenceV1: ...

    def read_algo_instance(self, algo_instance_id: str) -> ExecutionAlgoInstancePersistenceV2: ...

    def claim_delivery(self, **values: Any) -> AlgoDeliveryPersistenceV1: ...

    def apply_claimed_delivery_atomic(self, **values: Any) -> dict[str, Any]: ...

    def apply_claimed_product_delivery_atomic_v3(self, **values: Any) -> dict[str, Any]: ...

    def mark_delivery_retryable(self, **values: Any) -> AlgoDeliveryPersistenceV1: ...

    def reclaim_stale_delivery(self, **values: Any) -> AlgoDeliveryPersistenceV1: ...


class KernelRequiredProviderUnavailable(RuntimeError):
    def __init__(self, message: str, *, context: dict[str, Any]) -> None:
        self.context = json_safe_evidence_v1(context)
        super().__init__(message)


def validate_vnpy_facade_k2_shadow_command_authority_v1(
    transition: AlgoTransitionV1 | AlgoInitializationV1,
) -> None:
    """Keep the existing K2 V1 materializer at its exact single-command boundary."""

    if not isinstance(transition, (AlgoTransitionV1, AlgoInitializationV1)):
        raise TypeError("transition must be AlgoTransitionV1 or AlgoInitializationV1")
    commands = transition.broker_commands
    if len(commands) > 1:
        raise KernelPluginInvocationError(
            "MINIQMT_VNPY_FACADE_MULTI_COMMAND_PRODUCT_AUTHORITY_UNAVAILABLE",
            "K4 shadow preserves the full ordered command trace but K2 V1 cannot materialize it",
            context={
                "command_count": len(commands),
                "ordered_command_ids": [item.command_id for item in commands],
                "effect_set_sha256": transition.effect_set_sha256,
            },
            broker_called=False,
        )


def validate_vnpy_facade_k6_product_command_trace_v1(
    transition: AlgoTransitionV1 | AlgoInitializationV1,
) -> None:
    """Validate the final route's complete 0..N command trace.

    This is intentionally not a product-authority evaluator and does not
    authorize a broker side effect.  It establishes the exact ordered plugin
    trace that the K6 V3 authority/materializer must consume.  The historical
    K2 shadow validator above remains single-command by design and must never
    be repurposed as a final-route compatibility switch.
    """

    if not isinstance(transition, (AlgoTransitionV1, AlgoInitializationV1)):
        raise TypeError("transition must be AlgoTransitionV1 or AlgoInitializationV1")
    commands = transition.broker_commands
    ordinals = tuple(item.ordinal for item in commands)
    command_ids = tuple(item.command_id for item in commands)
    if (
        ordinals != tuple(sorted(ordinals))
        or len(ordinals) != len(set(ordinals))
        or len(command_ids) != len(set(command_ids))
    ):
        raise KernelPluginInvocationError(
            "MINIQMT_K6_PRODUCT_COMMAND_TRACE_INVALID",
            "final K6 product command trace must retain the ordered unique command subset of the complete effect trace",
            context={
                "command_count": len(commands),
                "ordered_ordinals": list(ordinals),
                "ordered_command_ids": list(command_ids),
                "effect_set_sha256": transition.effect_set_sha256,
            },
            broker_called=False,
        )


@dataclass(frozen=True)
class ProductDeliveryProposalV3:
    transition_bundle: Any
    base_services: AlgoReadOnlyServicesV1
    creation_binding: VnpyFacadeAuthorityInputV2
    route_receipt: PluginRouteCompatibilityReceiptV1
    replay_builder: Callable[[AlgoReadOnlyServicesV1], Any]

    def __post_init__(self) -> None:
        if not isinstance(self.base_services, AlgoReadOnlyServicesV1):
            raise TypeError("base_services must be AlgoReadOnlyServicesV1")
        if not isinstance(self.creation_binding, VnpyFacadeAuthorityInputV2):
            raise TypeError("creation_binding must be VnpyFacadeAuthorityInputV2")
        if not isinstance(self.route_receipt, PluginRouteCompatibilityReceiptV1):
            raise TypeError("route_receipt must be PluginRouteCompatibilityReceiptV1")
        if not callable(self.replay_builder):
            raise TypeError("replay_builder must be callable")


class KernelDeliveryWorkerV1:
    def __init__(
        self,
        *,
        repository: KernelDeliveryRepositoryV1,
        catalog_runtime: PluginCatalogRuntimeV2,
        worker_id: str,
        process_incarnation_id: str,
        facade_authority: VnpyFacadeConformanceAuthorityV2 | None = None,
        gateway_catalog: GatewayCapabilityCatalogV1 | None = None,
        product_mode: bool = False,
        product_evidence_provider: Any | None = None,
    ) -> None:
        if not worker_id or not process_incarnation_id:
            raise ValueError("worker and process incarnation identities are required")
        self._repository = repository
        self._catalog_runtime = catalog_runtime
        self._catalog_snapshot = PluginCatalogSnapshotV1.model_validate(
            catalog_runtime.snapshot.model_dump(mode="python"), strict=True
        )
        if facade_authority is not None and not isinstance(facade_authority, VnpyFacadeConformanceAuthorityV2):
            raise TypeError("facade_authority must be VnpyFacadeConformanceAuthorityV2 or None")
        if facade_authority is not None and not isinstance(gateway_catalog, GatewayCapabilityCatalogV1):
            raise TypeError("gateway_catalog is required when facade_authority is supplied")
        if gateway_catalog is not None and not isinstance(gateway_catalog, GatewayCapabilityCatalogV1):
            raise TypeError("gateway_catalog must be GatewayCapabilityCatalogV1 or None")
        self._facade_authority = facade_authority
        self._gateway_catalog = (
            None
            if gateway_catalog is None
            else GatewayCapabilityCatalogV1.model_validate(gateway_catalog.model_dump(mode="python"), strict=True)
        )
        if type(product_mode) is not bool:
            raise TypeError("product_mode must be bool")
        if product_mode and (
            self._facade_authority is None
            or self._gateway_catalog is None
            or not callable(getattr(product_evidence_provider, "build_with_cursor_v1", None))
            or not callable(getattr(repository, "apply_claimed_product_delivery_atomic_v3", None))
        ):
            raise TypeError("product delivery requires sealed authority, gateway, evidence provider and V3 repository")
        if not product_mode and product_evidence_provider is not None:
            raise TypeError("shadow delivery cannot receive a product evidence provider")
        self._product_mode = product_mode
        self._product_evidence_provider = product_evidence_provider
        self._lease_owner = f"{worker_id}:{process_incarnation_id}"

    def process_once(
        self,
        *,
        delivery_id: str,
        lease_expires_at: Any,
        logical_time_utc: Any,
        facade_read_request: VnpyFacadeRepositoryReadRequestV1 | None = None,
        input_builder: Callable[
            [
                RuntimeEventEnvelopeV2,
                AlgoDeliveryPersistenceV1,
                ExecutionAlgoInstancePersistenceV2,
                AlgoStateSnapshotV2 | None,
                tuple[ExecutionCommandChildMappingV1, ...],
                tuple[BrokerCommandOutboxV1, ...],
                tuple[ExecutionAlgoTimerScheduleV1, ...],
                KernelCommandLifecycleProjectionV1,
                VnpyFacadeRepositoryReadSetV1 | None,
            ],
            KernelDeliveryExecutionInputV1,
        ]
        | None = None,
    ) -> dict[str, Any]:
        from .kernel_materializer import (
            materialize_applied_transition_v1,
            materialize_failure_transition_v1,
            materialize_skip_transition_v1,
        )

        current = self._repository.read_delivery(delivery_id)
        if current.status.value not in {"PENDING", "FAILED_RETRYABLE", "CLAIMED"}:
            raise KernelPluginInvocationError(
                "MINIQMT_ALGO_DELIVERY_NOT_CLAIMABLE",
                "delivery worker received a non-claimable durable status",
                context={"delivery_id": delivery_id, "status": current.status.value},
                broker_called=False,
            )
        if current.status.value != "CLAIMED" and current.attempt_count >= 5:
            raise KernelPluginInvocationError(
                "MINIQMT_ALGO_DELIVERY_RETRY_EXHAUSTED",
                "delivery already consumed all five durable attempts",
                context={"delivery_id": delivery_id, "attempt_count": current.attempt_count},
                broker_called=False,
            )
        algo = self._repository.read_algo_instance(current.algo_instance_id)
        if self._product_mode and facade_read_request is None:
            event = self._repository.read_runtime_event(current.event_id)
            correlation = thaw_json_v1(event.correlation)
            facade_read_request = VnpyFacadeRepositoryReadRequestV1.create(
                runtime_id=event.runtime_id,
                algo_instance_id=algo.algo_instance_id,
                current_event_id=event.event_id,
                current_event_sequence=event.sequence,
                current_delivery_id=current.delivery_id,
                current_delivery_sequence=current.algo_delivery_sequence,
                exchange_trade_date=correlation["exchange_trade_date"],
                session_epoch=correlation["session_epoch"],
                session_phase=SessionPhaseV1(correlation["session_phase"]),
            )
        if facade_read_request is not None and not isinstance(facade_read_request, VnpyFacadeRepositoryReadRequestV1):
            raise TypeError("facade_read_request must be VnpyFacadeRepositoryReadRequestV1 or None")
        if facade_read_request is not None and self._facade_authority is None:
            raise KernelPluginInvocationError(
                "MINIQMT_VNPY_FACADE_BINDING_INVALID",
                "facade repository read request requires a sealed conformance authority",
                context={
                    "delivery_id": delivery_id,
                    "algo_instance_id": current.algo_instance_id,
                    "facade_authority_supplied": self._facade_authority is not None,
                    "facade_read_request_supplied": facade_read_request is not None,
                },
                broker_called=False,
            )
        if current.status.value == "CLAIMED":
            if current.lease_owner != self._lease_owner or current.lease_fence_token is None:
                raise KernelPluginInvocationError(
                    "MINIQMT_ALGO_DELIVERY_CLAIM_OWNER_CONFLICT",
                    "delivery worker cannot continue another process incarnation's durable claim",
                    context={
                        "delivery_id": delivery_id,
                        "expected_lease_owner": self._lease_owner,
                        "actual_lease_owner": current.lease_owner,
                    },
                    broker_called=False,
                )
            claimed = current
            lease_epoch = current.lease_epoch
            fence = current.lease_fence_token
        else:
            lease_epoch = current.lease_epoch + 1
            fence = kernel_lease_fence_token_v1(
                owner_type="DELIVERY",
                owner_id=delivery_id,
                lease_epoch=lease_epoch,
                lease_owner=self._lease_owner,
            )
            claimed = self._repository.claim_delivery(
                delivery_id=delivery_id,
                lease_owner=self._lease_owner,
                lease_epoch=lease_epoch,
                lease_fence_token=fence,
                lease_expires_at=lease_expires_at,
                updated_at_utc=logical_time_utc,
                expected_row_version=current.row_version,
            )

        def build(
            event: RuntimeEventEnvelopeV2,
            locked_delivery: AlgoDeliveryPersistenceV1,
            locked_algo: ExecutionAlgoInstancePersistenceV2,
            previous_state: AlgoStateSnapshotV2 | None,
            active_mappings: tuple[ExecutionCommandChildMappingV1, ...],
            active_command_outboxes: tuple[BrokerCommandOutboxV1, ...],
            active_timers: tuple[ExecutionAlgoTimerScheduleV1, ...],
            facade_read_set: VnpyFacadeRepositoryReadSetV1 | None,
            product_base_services: AlgoReadOnlyServicesV1 | None = None,
        ) -> KernelTransitionWriteBundleV1:
            failure_mapping_statuses = {
                "RESERVED",
                "DISPATCHING",
                "BROKER_ACCEPTED",
                "OUTCOME_UNKNOWN",
            }
            failure_outbox_statuses = {
                "PENDING",
                "CLAIMED",
                "DISPATCHING",
                "FAILED_RETRYABLE",
                "OUTCOME_UNKNOWN",
                "RECONCILING",
            }
            failure_mappings = tuple(
                item for item in active_mappings if item.mapping_status.value in failure_mapping_statuses
            )
            failure_command_outboxes = tuple(
                item for item in active_command_outboxes if item.status.value in failure_outbox_statuses
            )

            def terminal_failure(
                exc: Exception,
                *,
                reason_code: str,
                context: dict[str, Any] | None = None,
                projection_set: ExecutionProjectionSetV1 | None = None,
            ) -> KernelTransitionWriteBundleV1:
                return materialize_failure_transition_v1(
                    event=event,
                    predecessor_delivery=locked_delivery,
                    previous_algo=locked_algo,
                    algo_code=locked_algo.algo_code,
                    plugin_id=locked_algo.plugin_id,
                    plugin_version=locked_algo.plugin_version,
                    plugin_manifest_sha256=locked_algo.plugin_manifest_sha256,
                    plugin_config=thaw_json_v1(locked_algo.plugin_config_json),
                    plugin_config_sha256=locked_algo.plugin_config_sha256,
                    compatibility_receipt_sha256=locked_algo.compatibility_receipt_sha256,
                    parent_intent_id=locked_algo.parent_intent_id,
                    strategy_slot_id=locked_algo.strategy_slot_id,
                    symbol=locked_algo.symbol,
                    side=locked_algo.side,
                    target_quantity=locked_algo.target_quantity,
                    stable_reason_code=reason_code,
                    exception=exc,
                    failure_context=(context if context is not None else {"stage": "ALGO_DELIVERY_APPLY"}),
                    projection_set=projection_set,
                    active_mappings=failure_mappings,
                    active_command_outboxes=failure_command_outboxes,
                    active_timer_schedules=active_timers,
                    logical_time_utc=logical_time_utc,
                    initialization=False,
                )

            if locked_algo.status.value == "FAILED":
                return materialize_skip_transition_v1(
                    event=event,
                    predecessor_delivery=locked_delivery,
                    previous_algo=locked_algo,
                    logical_time_utc=logical_time_utc,
                )
            if previous_state is None:
                exc = KernelPluginInvocationError(
                    "MINIQMT_ALGO_STATE_READBACK_MISSING",
                    "non-failed algo delivery has no exact applied state snapshot",
                    context={
                        "algo_instance_id": locked_algo.algo_instance_id,
                        "delivery_id": locked_delivery.delivery_id,
                    },
                    broker_called=False,
                )
                return terminal_failure(
                    exc,
                    reason_code=exc.reason_code,
                    context=exc.context,
                )
            try:
                lifecycle_projection = build_command_lifecycle_projection_v1(
                    event=event,
                    delivery=locked_delivery,
                    previous_state=previous_state,
                    mappings=active_mappings,
                    outboxes=active_command_outboxes,
                )
            except Exception as exc:
                return terminal_failure(
                    exc,
                    reason_code="MINIQMT_ALGO_DELIVERY_LIFECYCLE_PROJECTION_INVALID",
                    context={"stage": "DELIVERY_LIFECYCLE_PROJECTION_READBACK"},
                )
            try:
                if self._product_mode:
                    if not isinstance(product_base_services, AlgoReadOnlyServicesV1):
                        raise TypeError("product repository did not supply same-cursor base services")
                    correlation = thaw_json_v1(event.correlation)
                    deterministic_context = DeterministicExecutionContextV1.create(
                        runtime_id=event.runtime_id,
                        algo_instance_id=locked_algo.algo_instance_id,
                        event_id=event.event_id,
                        delivery_id=locked_delivery.delivery_id,
                        plugin_manifest_sha256=locked_algo.plugin_manifest_sha256,
                        transition_sequence=previous_state.transition_sequence + 1,
                        logical_time_utc=event.event_time_utc,
                        exchange_trade_date=correlation["exchange_trade_date"],
                        session_epoch=correlation["session_epoch"],
                        session_phase=SessionPhaseV1(correlation["session_phase"]),
                        input_projection_sha256=(product_base_services.execution_projection_set.projection_set_sha256),
                    )
                    lineages = [
                        ConsumedLineageRefV1.create(
                            lineage_type=ConsumedLineageTypeV1.EVENT,
                            identity=event.event_id,
                            payload_sha256=event.payload_sha256,
                        )
                    ]
                    market_ref = next(
                        (
                            item
                            for item in product_base_services.execution_projection_set.ordered_projection_refs
                            if item.projection_type is KernelProjectionTypeV1.MARKET_DATA
                        ),
                        None,
                    )
                    if market_ref is not None:
                        lineages.append(
                            ConsumedLineageRefV1.create(
                                lineage_type=ConsumedLineageTypeV1.MARKET_DATA,
                                identity=market_ref.projection_id,
                                payload_sha256=market_ref.payload_sha256,
                            )
                        )
                    inputs = KernelDeliveryExecutionInputV1(
                        services=product_base_services,
                        deterministic_context=deterministic_context,
                        consumed_lineage_refs=tuple(lineages),
                        command_lifecycle_projection=lifecycle_projection,
                    )
                else:
                    if not callable(input_builder):
                        raise TypeError("shadow delivery requires a callable input_builder")
                    inputs = input_builder(
                        event,
                        locked_delivery,
                        locked_algo,
                        previous_state,
                        active_mappings,
                        active_command_outboxes,
                        active_timers,
                        lifecycle_projection,
                        facade_read_set,
                    )
            except KernelRequiredProviderUnavailable as exc:
                if locked_delivery.attempt_count < 5:
                    raise
                return terminal_failure(
                    exc,
                    reason_code="MINIQMT_ALGO_DELIVERY_RETRY_EXHAUSTED",
                    context=exc.context,
                )
            except Exception as exc:
                return terminal_failure(
                    exc,
                    reason_code="MINIQMT_ALGO_DELIVERY_INPUT_INVALID",
                    context={"stage": "DELIVERY_INPUT_BUILDER"},
                )
            if not isinstance(inputs, KernelDeliveryExecutionInputV1):
                exc = TypeError("input_builder must return KernelDeliveryExecutionInputV1")
                return terminal_failure(
                    exc,
                    reason_code="MINIQMT_ALGO_DELIVERY_INPUT_INVALID",
                    context={"stage": "DELIVERY_INPUT_READBACK", "result_type": type(inputs).__name__},
                )
            if inputs.command_lifecycle_projection != lifecycle_projection:
                exc = KernelPluginInvocationError(
                    "MINIQMT_ALGO_DELIVERY_LIFECYCLE_PROJECTION_DRIFT",
                    "delivery input lifecycle projection differs from the locked durable readback",
                    context={
                        "delivery_id": locked_delivery.delivery_id,
                        "expected_projection_sha256": lifecycle_projection.projection_sha256,
                        "actual_projection_sha256": inputs.command_lifecycle_projection.projection_sha256,
                    },
                    broker_called=False,
                )
                return terminal_failure(
                    exc,
                    reason_code=exc.reason_code,
                    context=exc.context,
                    projection_set=inputs.services.execution_projection_set,
                )
            if event.event_type is EventTypeV2.OPERATOR:
                operator_payload = thaw_json_v1(event.payload)
                if operator_payload.get("schema_version") == "miniqmt_hot_market_economic_action_v1":
                    expected_row_version = operator_payload.get("expected_algo_row_version")
                    if type(expected_row_version) is not int or expected_row_version != locked_algo.row_version:
                        exc = KernelPluginInvocationError(
                            "MINIQMT_HOT_MARKET_EFFECT_STALE_ALGO_VERSION",
                            "hot economic effect does not own the exact locked algo generation",
                            context={
                                "runtime_id": event.runtime_id,
                                "algo_instance_id": locked_algo.algo_instance_id,
                                "event_id": event.event_id,
                                "expected_algo_row_version": expected_row_version,
                                "actual_algo_row_version": locked_algo.row_version,
                            },
                            broker_called=False,
                        )
                        return terminal_failure(
                            exc,
                            reason_code=exc.reason_code,
                            context=exc.context,
                            projection_set=inputs.services.execution_projection_set,
                        )
            try:
                resolved = resolve_plugin_for_restore_v1(
                    catalog_runtime=self._catalog_runtime,
                    plugin_id=locked_algo.plugin_id,
                    plugin_version=locked_algo.plugin_version,
                    plugin_manifest_sha256=locked_algo.plugin_manifest_sha256,
                    canonical_plugin_config=thaw_json_v1(locked_algo.plugin_config_json),
                    plugin_config_sha256=locked_algo.plugin_config_sha256,
                )
                plugin_key = resolved.descriptor.plugin_key
                k1_receipts = tuple(
                    item
                    for item in self._catalog_snapshot.pinned_compatibility_receipts
                    if item.plugin_key == plugin_key
                )
                route_receipt = None
                authority_input = None
                if self._product_mode or isinstance(resolved.plugin, VnpyFacadeBackedPluginAdapterV1):
                    if self._facade_authority is None or self._gateway_catalog is None or len(k1_receipts) != 1:
                        raise KernelPluginInvocationError(
                            "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
                            "product/facade transition requires one exact sealed compatibility authority",
                            context={"plugin_key": plugin_key.canonical_payload_v1()},
                            broker_called=False,
                        )
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
                            "transition route compatibility receipt is not PASSED",
                            context={
                                "plugin_key": plugin_key.canonical_payload_v1(),
                                "route_receipt_sha256": route_receipt.receipt_sha256,
                            },
                            broker_called=False,
                        )
                    authority_input = VnpyFacadeAuthorityInputV2.create(
                        conformance_authority=self._facade_authority,
                        plugin_catalog_snapshot=self._catalog_snapshot,
                        gateway_capability_catalog=self._gateway_catalog,
                        plugin_key=plugin_key,
                        manifest=resolved.descriptor.manifest,
                        pinned_compatibility_receipt=k1_receipts[0],
                        route_compatibility_receipt=route_receipt,
                    )

                def evaluate_transition(
                    services: AlgoReadOnlyServicesV1,
                    deterministic_context: DeterministicExecutionContextV1,
                ) -> AlgoTransitionV1:
                    facade_input = None
                    if isinstance(resolved.plugin, VnpyFacadeBackedPluginAdapterV1):
                        if facade_read_request is None or facade_read_set is None or authority_input is None:
                            raise KernelPluginInvocationError(
                                "MINIQMT_VNPY_FACADE_REPOSITORY_READ_INVALID",
                                "facade-backed transition requires its exact same-cursor repository read set",
                                context={
                                    "runtime_id": event.runtime_id,
                                    "algo_instance_id": locked_algo.algo_instance_id,
                                    "delivery_id": locked_delivery.delivery_id,
                                },
                                broker_called=False,
                            )
                        facade_read_set.validate_against_request_v1(facade_read_request)
                        facade_input = VnpyFacadeTransitionInputV2.create(
                            runtime_event=event,
                            claimed_delivery=locked_delivery,
                            algo_instance=locked_algo,
                            manifest=resolved.descriptor.manifest,
                            authority_input=authority_input,
                            before_state=previous_state,
                            read_only_services=services,
                            command_lifecycle_projection=lifecycle_projection,
                            ordered_active_mappings=active_mappings,
                            deterministic_context=deterministic_context,
                            transition_sequence=deterministic_context.transition_sequence,
                        )
                    elif facade_read_set is not None or facade_read_request is not None:
                        raise KernelPluginInvocationError(
                            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
                            "ordinary pure plugin cannot consume facade repository authority",
                            context={
                                "plugin_id": resolved.descriptor.plugin_key.plugin_id,
                                "algo_instance_id": locked_algo.algo_instance_id,
                            },
                            broker_called=False,
                        )
                    result = invoke_plugin_transition_v1(
                        plugin=resolved.plugin,
                        expected_manifest=resolved.descriptor.manifest,
                        state_codec=resolved.state_codec,
                        state=previous_state,
                        event=event,
                        services=services,
                        deterministic_context=deterministic_context,
                        facade_input=facade_input,
                    )
                    if self._product_mode:
                        validate_vnpy_facade_k6_product_command_trace_v1(result)
                    elif facade_input is not None:
                        validate_vnpy_facade_k2_shadow_command_authority_v1(result)
                    return result

                def materialize(
                    transition: AlgoTransitionV1,
                    services: AlgoReadOnlyServicesV1,
                ) -> KernelTransitionWriteBundleV1:
                    return materialize_applied_transition_v1(
                        event=event,
                        predecessor_delivery=locked_delivery,
                        previous_algo=locked_algo,
                        transition=transition,
                        projection_set=services.execution_projection_set,
                        consumed_lineage_refs=inputs.consumed_lineage_refs,
                        strategy_slot_id=locked_algo.strategy_slot_id,
                        parent_intent_id=locked_algo.parent_intent_id,
                        compatibility_receipt_sha256=locked_algo.compatibility_receipt_sha256,
                        plugin_config=thaw_json_v1(locked_algo.plugin_config_json),
                        plugin_config_sha256=locked_algo.plugin_config_sha256,
                        target_quantity=locked_algo.target_quantity,
                        algo_code=locked_algo.algo_code,
                        symbol=locked_algo.symbol,
                        side=locked_algo.side,
                        command_lifecycle_projection=lifecycle_projection,
                        existing_mappings_by_local_vt_orderid={item.local_vt_orderid: item for item in active_mappings},
                        existing_timer_schedules={item.schedule_id: item for item in active_timers},
                        initialization=False,
                    )

                transition = evaluate_transition(inputs.services, inputs.deterministic_context)
                proposal_bundle = materialize(transition, inputs.services)
                if not self._product_mode:
                    return proposal_bundle
                assert authority_input is not None and route_receipt is not None

                def replay_builder(services: AlgoReadOnlyServicesV1) -> KernelTransitionWriteBundleV1:
                    base = inputs.deterministic_context
                    replay_context = DeterministicExecutionContextV1.create(
                        runtime_id=base.runtime_id,
                        algo_instance_id=base.algo_instance_id,
                        event_id=base.event_id,
                        delivery_id=base.delivery_id,
                        plugin_manifest_sha256=base.plugin_manifest_sha256,
                        transition_sequence=base.transition_sequence,
                        logical_time_utc=base.logical_time_utc,
                        exchange_trade_date=base.exchange_trade_date,
                        session_epoch=base.session_epoch,
                        session_phase=base.session_phase,
                        input_projection_sha256=services.execution_projection_set.projection_set_sha256,
                    )
                    return materialize(evaluate_transition(services, replay_context), services)

                return ProductDeliveryProposalV3(
                    transition_bundle=proposal_bundle,
                    base_services=inputs.services,
                    creation_binding=authority_input,
                    route_receipt=route_receipt,
                    replay_builder=replay_builder,
                )
            except Exception as exc:
                return terminal_failure(
                    exc,
                    reason_code=stable_exception_reason_code_v1(exc, default="MINIQMT_ALGO_TRANSITION_FAILED"),
                    context=getattr(exc, "context", {"stage": "ALGO_DELIVERY_APPLY"}),
                    projection_set=inputs.services.execution_projection_set,
                )

        try:
            apply_method = (
                self._repository.apply_claimed_product_delivery_atomic_v3
                if self._product_mode
                else self._repository.apply_claimed_delivery_atomic
            )
            apply_kwargs = dict(
                delivery_id=delivery_id,
                expected_delivery_row_version=claimed.row_version,
                expected_algo_row_version=algo.row_version,
                expected_lease_owner=self._lease_owner,
                expected_lease_epoch=lease_epoch,
                expected_lease_fence_token=fence,
                facade_read_request=facade_read_request,
            )
            if self._product_mode:
                apply_kwargs.update(
                    proposal_builder=build,
                    product_evidence_provider=self._product_evidence_provider,
                )
            else:
                apply_kwargs["bundle_builder"] = build
            return apply_method(**apply_kwargs)
        except KernelRequiredProviderUnavailable as exc:
            evidence = KernelErrorEvidenceV1.create(
                stage="DELIVERY_REQUIRED_PROVIDER",
                stable_reason_code="MINIQMT_ALGO_DELIVERY_REQUIRED_PROVIDER_UNAVAILABLE",
                exception=exc,
                message=str(exc),
                retryable=True,
                terminal=False,
                broker_called=False,
                primary_context={
                    "delivery_id": delivery_id,
                    "algo_instance_id": claimed.algo_instance_id,
                    "attempt_count": claimed.attempt_count,
                    **exc.context,
                },
                secondary_errors=[],
            )
            retryable = self._repository.mark_delivery_retryable(
                delivery_id=delivery_id,
                expected_row_version=claimed.row_version,
                expected_lease_owner=self._lease_owner,
                expected_lease_epoch=lease_epoch,
                expected_lease_fence_token=fence,
                error_evidence=evidence,
                failed_at_utc=logical_time_utc,
            )
            return {"delivery": retryable, "retry_scheduled": True, "error_evidence": evidence}


class KernelProductDeliveryWorkerV3(KernelDeliveryWorkerV1):
    """Final product worker; the K2-only materializer is not reachable."""

    def __init__(
        self,
        *,
        repository: KernelDeliveryRepositoryV1,
        catalog_runtime: PluginCatalogRuntimeV2,
        worker_id: str,
        process_incarnation_id: str,
        facade_authority: VnpyFacadeConformanceAuthorityV2,
        gateway_catalog: GatewayCapabilityCatalogV1,
        product_evidence_provider: Any,
    ) -> None:
        super().__init__(
            repository=repository,
            catalog_runtime=catalog_runtime,
            worker_id=worker_id,
            process_incarnation_id=process_incarnation_id,
            facade_authority=facade_authority,
            gateway_catalog=gateway_catalog,
            product_mode=True,
            product_evidence_provider=product_evidence_provider,
        )

    def process_committed_delivery_v3(
        self,
        *,
        delivery_id: str,
        lease_expires_at: Any,
        logical_time_utc: Any,
    ) -> dict[str, Any]:
        current = self._repository.read_delivery(delivery_id)
        if current.status.value in {"APPLIED", "FAILED_TERMINAL", "SKIPPED_TERMINAL"}:
            return {"delivery": current, "idempotent": True}
        return self.process_once(
            delivery_id=delivery_id,
            lease_expires_at=lease_expires_at,
            logical_time_utc=logical_time_utc,
        )


@dataclass(frozen=True)
class KernelTransitionWriteBundleV1:
    algo_instance: ExecutionAlgoInstancePersistenceV2
    delivery: AlgoDeliveryPersistenceV1
    receipt: AlgoTransitionReceiptV1 | AlgoFailureReceiptV1 | AlgoSkipReceiptV1
    projection_set: ExecutionProjectionSetV1 | None
    after_state: AlgoStateSnapshotV2 | None
    applied_transition: AlgoTransitionV1 | None = None
    new_child_mappings: tuple[ExecutionCommandChildMappingV1, ...] = ()
    command_outboxes: tuple[BrokerCommandOutboxV1, ...] = ()
    updated_child_mappings: tuple[ExecutionCommandChildMappingV1, ...] = ()
    updated_command_outboxes: tuple[BrokerCommandOutboxV1, ...] = ()
    timer_mutations: tuple[TimerMutationV1, ...] = ()
    timer_schedules: tuple[ExecutionAlgoTimerScheduleV1, ...] = ()
    diagnostic_observations: tuple[DiagnosticObservationV1, ...] = ()

    @classmethod
    def create(
        cls,
        *,
        algo_instance: ExecutionAlgoInstancePersistenceV2,
        delivery: AlgoDeliveryPersistenceV1,
        receipt: AlgoTransitionReceiptV1 | AlgoFailureReceiptV1 | AlgoSkipReceiptV1,
        projection_set: ExecutionProjectionSetV1 | None,
        after_state: AlgoStateSnapshotV2 | None,
        applied_transition: AlgoTransitionV1 | None = None,
        new_child_mappings: Sequence[ExecutionCommandChildMappingV1] = (),
        command_outboxes: Sequence[BrokerCommandOutboxV1] = (),
        updated_child_mappings: Sequence[ExecutionCommandChildMappingV1] = (),
        updated_command_outboxes: Sequence[BrokerCommandOutboxV1] = (),
        timer_mutations: Sequence[TimerMutationV1] = (),
        timer_schedules: Sequence[ExecutionAlgoTimerScheduleV1] = (),
        diagnostic_observations: Sequence[DiagnosticObservationV1] = (),
    ) -> "KernelTransitionWriteBundleV1":
        values = {
            "new_child_mappings": tuple(new_child_mappings),
            "command_outboxes": tuple(command_outboxes),
            "updated_child_mappings": tuple(updated_child_mappings),
            "updated_command_outboxes": tuple(updated_command_outboxes),
            "timer_mutations": tuple(timer_mutations),
            "timer_schedules": tuple(timer_schedules),
            "diagnostic_observations": tuple(diagnostic_observations),
        }
        typed = (
            ("new_child_mappings", values["new_child_mappings"], ExecutionCommandChildMappingV1),
            ("command_outboxes", values["command_outboxes"], BrokerCommandOutboxV1),
            ("updated_child_mappings", values["updated_child_mappings"], ExecutionCommandChildMappingV1),
            ("updated_command_outboxes", values["updated_command_outboxes"], BrokerCommandOutboxV1),
            ("timer_mutations", values["timer_mutations"], TimerMutationV1),
            ("timer_schedules", values["timer_schedules"], ExecutionAlgoTimerScheduleV1),
            ("diagnostic_observations", values["diagnostic_observations"], DiagnosticObservationV1),
        )
        for name, items, item_type in typed:
            if any(not isinstance(item, item_type) for item in items):
                raise TypeError(f"{name} contains an invalid carrier")
        if applied_transition is not None and not isinstance(applied_transition, AlgoTransitionV1):
            raise TypeError("applied_transition must be AlgoTransitionV1 or None")
        if applied_transition is not None and not isinstance(receipt, AlgoTransitionReceiptV1):
            raise ValueError("applied transition carrier requires an APPLIED receipt")
        return cls(
            algo_instance=algo_instance,
            delivery=delivery,
            receipt=receipt,
            projection_set=projection_set,
            after_state=after_state,
            applied_transition=applied_transition,
            **values,
        )


@dataclass(frozen=True)
class KernelAlgoStartWriteBundleV1:
    event: RuntimeEventEnvelopeV2
    initial_delivery: AlgoDeliveryPersistenceV1
    transition_bundle: KernelTransitionWriteBundleV1


def _invocation_error(
    reason_code: str,
    message: str,
    *,
    stage: str,
    exception: BaseException | None = None,
    **context: Any,
) -> KernelPluginInvocationError:
    evidence = {"stage": stage, **context}
    if exception is not None:
        evidence.update(safe_exception_summary_v1(exception))
    return KernelPluginInvocationError(reason_code, message, context=evidence, broker_called=False)


def _facade_exception_context_v1(exc: VnpyFacadeContractError) -> dict[str, Any]:
    """Render facade evidence without allowing its carrier type to mask it."""

    try:
        context = thaw_json_v1(exc.context)
    except TypeError:
        context = json_safe_evidence_v1(exc.context)
    return context if type(context) is dict else {"facade_context": context}


def resolve_plugin_for_restore_v1(
    *,
    catalog_runtime: PluginCatalogRuntimeV2,
    plugin_id: str,
    plugin_version: str,
    plugin_manifest_sha256: str,
    canonical_plugin_config: dict[str, Any],
    plugin_config_sha256: str,
) -> ResolvedKernelPluginV1:
    """Resolve one exact frozen plugin key; never scans latest or legacy routes."""

    if not isinstance(catalog_runtime, PluginCatalogRuntimeV2):
        raise TypeError("catalog_runtime must be PluginCatalogRuntimeV2")
    try:
        snapshot = PluginCatalogSnapshotV1.model_validate(
            catalog_runtime.snapshot.model_dump(mode="python"), strict=True
        )
        plugin_key = PluginKeyV1(
            plugin_id=plugin_id,
            plugin_version=plugin_version,
            manifest_sha256=plugin_manifest_sha256,
        )
        descriptor = next(item for item in snapshot.registration_descriptors if item.plugin_key == plugin_key)
    except (StopIteration, TypeError, ValueError) as exc:
        raise _invocation_error(
            "MINIQMT_ALGO_PLUGIN_BINDING_INVALID",
            "exact frozen plugin descriptor is unavailable or invalid",
            stage="PLUGIN_DESCRIPTOR_READBACK",
            exception=exc,
            plugin_id=plugin_id,
            plugin_version=plugin_version,
            plugin_manifest_sha256=plugin_manifest_sha256,
        ) from exc
    expected_config_hash = hash_hex_v1("miniqmt_plugin_config_v2", canonical_plugin_config)
    if plugin_config_sha256 != expected_config_hash:
        raise _invocation_error(
            "MINIQMT_ALGO_PLUGIN_CONFIG_INVALID",
            "plugin config hash differs from the frozen canonical payload",
            stage="PLUGIN_CONFIG_READBACK",
            plugin_id=plugin_id,
            expected_config_sha256=expected_config_hash,
            actual_config_sha256=plugin_config_sha256,
        )
    config_validator = catalog_runtime.process_bindings.resolve(descriptor.config_validator_binding_id)
    factory = catalog_runtime.process_bindings.resolve(descriptor.factory_binding_id)
    state_codec = catalog_runtime.process_bindings.resolve(descriptor.state_codec_binding_id)
    if not callable(config_validator) or not callable(factory) or not callable(state_codec):
        raise _invocation_error(
            "MINIQMT_ALGO_PLUGIN_BINDING_INVALID",
            "one or more exact process-local plugin bindings are unavailable",
            stage="PLUGIN_BINDING_RESOLUTION",
            plugin_id=plugin_id,
            factory_binding_id=descriptor.factory_binding_id,
            config_validator_binding_id=descriptor.config_validator_binding_id,
            state_codec_binding_id=descriptor.state_codec_binding_id,
        )
    try:
        validated_config = config_validator(descriptor.manifest, canonical_plugin_config)
        if hash_hex_v1("miniqmt_plugin_config_v2", thaw_json_v1(validated_config)) != plugin_config_sha256:
            raise ValueError("config validator output does not preserve frozen config identity")
        plugin = factory(thaw_json_v1(validated_config))
    except (TypeError, ValueError, AttributeError, KeyError) as exc:
        raise _invocation_error(
            "MINIQMT_ALGO_PLUGIN_BINDING_INVALID",
            "plugin factory cannot construct the required pure ExecutionAlgoPluginV2",
            stage="PLUGIN_FACTORY",
            exception=exc,
            plugin_id=plugin_id,
            factory_binding_id=descriptor.factory_binding_id,
        ) from exc
    if not isinstance(plugin, ExecutionAlgoPluginV2) or plugin.manifest != descriptor.manifest:
        raise _invocation_error(
            "MINIQMT_ALGO_PLUGIN_BINDING_INVALID",
            "plugin factory result does not expose the exact frozen pure plugin manifest and methods",
            stage="PLUGIN_FACTORY_RESULT",
            plugin_id=plugin_id,
            factory_binding_id=descriptor.factory_binding_id,
            result_type=type(plugin).__name__,
        )
    return ResolvedKernelPluginV1(plugin=plugin, descriptor=descriptor, state_codec=state_codec)


def invoke_plugin_initialize_v1(
    *,
    plugin: ExecutionAlgoPluginV2 | VnpyFacadeBackedPluginAdapterV1,
    expected_manifest: ExecutionAlgoPluginManifestV2,
    start_context: AlgoStartContextV1,
    facade_input: VnpyFacadeInitializationInputV2 | None = None,
) -> AlgoInitializationV1:
    if not isinstance(expected_manifest, ExecutionAlgoPluginManifestV2):
        raise TypeError("expected_manifest must be ExecutionAlgoPluginManifestV2")
    is_facade_adapter = isinstance(plugin, VnpyFacadeBackedPluginAdapterV1)
    if facade_input is not None and not isinstance(facade_input, VnpyFacadeInitializationInputV2):
        raise TypeError("facade_input must be VnpyFacadeInitializationInputV2 or None")
    if is_facade_adapter != (facade_input is not None):
        raise _invocation_error(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "facade-backed adapter and exact V2 initialization input must be supplied together",
            stage="PLUGIN_INITIALIZE_FACADE_BINDING",
            plugin_type=type(plugin).__name__,
            facade_input_type=None if facade_input is None else type(facade_input).__name__,
        )
    if (
        not is_facade_adapter and not isinstance(plugin, ExecutionAlgoPluginV2)
    ) or plugin.manifest != expected_manifest:
        raise _invocation_error(
            "MINIQMT_ALGO_PLUGIN_BINDING_INVALID",
            "initialize plugin manifest or pure method surface is invalid",
            stage="PLUGIN_INITIALIZE_BINDING",
            plugin_type=type(plugin).__name__,
            expected_manifest_sha256=expected_manifest.manifest_sha256,
        )
    if not isinstance(start_context, AlgoStartContextV1):
        raise TypeError("start_context must be AlgoStartContextV1")
    if facade_input is not None and facade_input.start_context != start_context:
        raise _invocation_error(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "facade initialization input does not equal the SPI start context",
            stage="PLUGIN_INITIALIZE_FACADE_INPUT",
            algo_instance_id=start_context.algo_instance_id,
        )
    try:
        result = (
            plugin.initialize_with_facade_v2(facade_input)
            if is_facade_adapter and facade_input is not None
            else plugin.initialize(start_context)
        )
    except VnpyFacadeContractError as exc:
        record_vnpy_facade_runtime_invocation_v1(phase="INITIALIZE", outcome="FAILED", reason_code=exc.reason_code)
        raise _invocation_error(
            exc.reason_code,
            exc.message,
            stage="PLUGIN_INITIALIZE_FACADE",
            **_facade_exception_context_v1(exc),
        ) from exc
    except Exception as exc:
        if is_facade_adapter:
            record_vnpy_facade_runtime_invocation_v1(
                phase="INITIALIZE",
                outcome="FAILED",
                reason_code="MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            )
        raise _invocation_error(
            "MINIQMT_ALGO_INITIALIZATION_PLUGIN_FAILED",
            "pure plugin initialize raised a deterministic failure",
            stage="PLUGIN_INITIALIZE",
            exception=exc,
            runtime_id=start_context.runtime_id,
            algo_instance_id=start_context.algo_instance_id,
            event_id=start_context.start_event_id,
            delivery_id=start_context.start_delivery_id,
        ) from exc
    if not isinstance(result, AlgoInitializationV1):
        if is_facade_adapter:
            record_vnpy_facade_runtime_invocation_v1(
                phase="INITIALIZE",
                outcome="FAILED",
                reason_code="MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            )
        raise _invocation_error(
            "MINIQMT_ALGO_INITIALIZATION_RESULT_INVALID",
            "plugin initialize did not return AlgoInitializationV1",
            stage="PLUGIN_INITIALIZE_RESULT",
            algo_instance_id=start_context.algo_instance_id,
            result_type=type(result).__name__,
        )
    if (
        result.start_event_id != start_context.start_event_id
        or result.start_delivery_id != start_context.start_delivery_id
        or result.next_state.algo_instance_id != start_context.algo_instance_id
        or result.next_state.plugin_manifest_sha256 != expected_manifest.manifest_sha256
        or result.next_state.updated_at_utc != start_context.deterministic_context.logical_time_utc
    ):
        if is_facade_adapter:
            record_vnpy_facade_runtime_invocation_v1(
                phase="INITIALIZE",
                outcome="FAILED",
                reason_code="MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            )
        raise _invocation_error(
            "MINIQMT_ALGO_INITIALIZATION_RESULT_INVALID",
            "plugin initialization result does not close to its deterministic start context",
            stage="PLUGIN_INITIALIZE_RESULT",
            algo_instance_id=start_context.algo_instance_id,
            event_id=start_context.start_event_id,
            delivery_id=start_context.start_delivery_id,
        )
    if is_facade_adapter:
        record_vnpy_facade_runtime_invocation_v1(phase="INITIALIZE", outcome="PASSED", reason_code="NONE")
    return result


def invoke_plugin_transition_v1(
    *,
    plugin: ExecutionAlgoPluginV2 | VnpyFacadeBackedPluginAdapterV1,
    expected_manifest: ExecutionAlgoPluginManifestV2,
    state_codec: Callable[[ExecutionAlgoPluginManifestV2, Any], Any],
    state: AlgoStateSnapshotV2,
    event: RuntimeEventEnvelopeV2,
    services: AlgoReadOnlyServicesV1,
    deterministic_context: DeterministicExecutionContextV1,
    facade_input: VnpyFacadeTransitionInputV2 | None = None,
) -> AlgoTransitionV1:
    if not isinstance(expected_manifest, ExecutionAlgoPluginManifestV2):
        raise TypeError("expected_manifest must be ExecutionAlgoPluginManifestV2")
    is_facade_adapter = isinstance(plugin, VnpyFacadeBackedPluginAdapterV1)
    if facade_input is not None and not isinstance(facade_input, VnpyFacadeTransitionInputV2):
        raise TypeError("facade_input must be VnpyFacadeTransitionInputV2 or None")
    if is_facade_adapter != (facade_input is not None):
        raise _invocation_error(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "facade-backed adapter and exact V2 transition input must be supplied together",
            stage="PLUGIN_TRANSITION_FACADE_BINDING",
            plugin_type=type(plugin).__name__,
            facade_input_type=None if facade_input is None else type(facade_input).__name__,
        )
    if (
        not is_facade_adapter and not isinstance(plugin, ExecutionAlgoPluginV2)
    ) or plugin.manifest != expected_manifest:
        raise _invocation_error(
            "MINIQMT_ALGO_PLUGIN_BINDING_INVALID",
            "transition plugin manifest or pure method surface is invalid",
            stage="PLUGIN_TRANSITION_BINDING",
            plugin_type=type(plugin).__name__,
            expected_manifest_sha256=expected_manifest.manifest_sha256,
        )
    if not callable(state_codec):
        raise TypeError("state_codec must be callable")
    if not isinstance(state, AlgoStateSnapshotV2):
        raise TypeError("state must be AlgoStateSnapshotV2")
    if not isinstance(event, RuntimeEventEnvelopeV2):
        raise TypeError("event must be RuntimeEventEnvelopeV2")
    if not isinstance(services, AlgoReadOnlyServicesV1):
        raise TypeError("services must be AlgoReadOnlyServicesV1")
    if not isinstance(deterministic_context, DeterministicExecutionContextV1):
        raise TypeError("deterministic_context must be DeterministicExecutionContextV1")
    if facade_input is not None and (
        facade_input.before_state != state
        or facade_input.runtime_event != event
        or facade_input.read_only_services != services
        or facade_input.deterministic_context != deterministic_context
    ):
        raise _invocation_error(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "facade transition input does not equal the SPI state/event/services/context",
            stage="PLUGIN_TRANSITION_FACADE_INPUT",
            algo_instance_id=state.algo_instance_id,
            event_id=event.event_id,
        )
    if (
        state.algo_instance_id != deterministic_context.algo_instance_id
        or state.plugin_manifest_sha256 != expected_manifest.manifest_sha256
        or event.runtime_id != deterministic_context.runtime_id
        or event.event_id != deterministic_context.event_id
        or services.runtime_id != deterministic_context.runtime_id
        or services.algo_instance_id != deterministic_context.algo_instance_id
        or services.event_id != deterministic_context.event_id
        or services.delivery_id != deterministic_context.delivery_id
        or services.execution_projection_set.projection_set_sha256 != deterministic_context.input_projection_sha256
    ):
        raise _invocation_error(
            "MINIQMT_ALGO_TRANSITION_OWNER_CONFLICT",
            "state, event, services and deterministic context owners do not close",
            stage="PLUGIN_TRANSITION_INPUT",
            runtime_id=deterministic_context.runtime_id,
            algo_instance_id=deterministic_context.algo_instance_id,
            event_id=deterministic_context.event_id,
            delivery_id=deterministic_context.delivery_id,
        )
    try:
        if is_facade_adapter and facade_input is not None:
            result = plugin.transition_with_facade_v2(facade_input)
        else:
            codec_state = state_codec(expected_manifest, thaw_json_v1(state.state))
            if thaw_json_v1(freeze_json_v1(codec_state)) != thaw_json_v1(state.state):
                raise ValueError("state codec changed durable state during strict readback")
            restored = plugin.restore_state(state)
            if restored != state:
                raise ValueError("restore_state changed the frozen same-version state snapshot")
            result = plugin.transition(state=restored, event=event, services=services)
    except KernelPluginInvocationError:
        raise
    except VnpyFacadeContractError as exc:
        record_vnpy_facade_runtime_invocation_v1(phase="TRANSITION", outcome="FAILED", reason_code=exc.reason_code)
        raise _invocation_error(
            exc.reason_code,
            exc.message,
            stage="PLUGIN_TRANSITION_FACADE",
            **_facade_exception_context_v1(exc),
        ) from exc
    except Exception as exc:
        if is_facade_adapter:
            record_vnpy_facade_runtime_invocation_v1(
                phase="TRANSITION",
                outcome="FAILED",
                reason_code="MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            )
        raise _invocation_error(
            "MINIQMT_ALGO_TRANSITION_PLUGIN_FAILED",
            "pure plugin transition raised a deterministic failure",
            stage="PLUGIN_TRANSITION",
            exception=exc,
            runtime_id=event.runtime_id,
            algo_instance_id=state.algo_instance_id,
            event_id=event.event_id,
            delivery_id=services.delivery_id,
        ) from exc
    if not isinstance(result, AlgoTransitionV1):
        if is_facade_adapter:
            record_vnpy_facade_runtime_invocation_v1(
                phase="TRANSITION",
                outcome="FAILED",
                reason_code="MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            )
        raise _invocation_error(
            "MINIQMT_ALGO_TRANSITION_RESULT_INVALID",
            "plugin transition did not return AlgoTransitionV1",
            stage="PLUGIN_TRANSITION_RESULT",
            result_type=type(result).__name__,
            algo_instance_id=state.algo_instance_id,
        )
    next_state = result.next_state
    if (
        next_state.algo_instance_id != state.algo_instance_id
        or next_state.plugin_id != expected_manifest.plugin_id
        or next_state.plugin_version != expected_manifest.plugin_version
        or next_state.plugin_manifest_sha256 != expected_manifest.manifest_sha256
        or next_state.transition_sequence != deterministic_context.transition_sequence
        or next_state.transition_sequence != state.transition_sequence + 1
        or next_state.last_applied_delivery_sequence != deterministic_context.transition_sequence
        or next_state.last_applied_delivery_id != deterministic_context.delivery_id
        or next_state.last_applied_event_id != event.event_id
        or next_state.updated_at_utc != deterministic_context.logical_time_utc
    ):
        if is_facade_adapter:
            record_vnpy_facade_runtime_invocation_v1(
                phase="TRANSITION",
                outcome="FAILED",
                reason_code="MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            )
        raise _invocation_error(
            "MINIQMT_ALGO_TRANSITION_RESULT_INVALID",
            "plugin transition result does not close to durable predecessor and deterministic context",
            stage="PLUGIN_TRANSITION_RESULT",
            algo_instance_id=state.algo_instance_id,
            event_id=event.event_id,
            delivery_id=deterministic_context.delivery_id,
            before_state_sha256=state.state_sha256,
            after_state_sha256=next_state.state_sha256,
        )
    if is_facade_adapter:
        record_vnpy_facade_runtime_invocation_v1(phase="TRANSITION", outcome="PASSED", reason_code="NONE")
    return result


__all__ = [
    "ExecutionAlgoPluginV2",
    "KernelAlgoStartWriteBundleV1",
    "KernelAlgoCreationRequestV1",
    "KernelAlgoCreationRequestV2",
    "KernelDeliveryExecutionInputV1",
    "KernelDeliveryWorkerV1",
    "KernelProductDeliveryWorkerV3",
    "KernelPluginInvocationError",
    "KernelTransitionWriteBundleV1",
    "ProductDeliveryProposalV3",
    "ResolvedKernelPluginV1",
    "build_command_lifecycle_projection_v1",
    "invoke_plugin_initialize_v1",
    "invoke_plugin_transition_v1",
    "resolve_plugin_for_restore_v1",
    "validate_vnpy_facade_k6_product_command_trace_v1",
    "validate_vnpy_facade_k2_shadow_command_authority_v1",
]
