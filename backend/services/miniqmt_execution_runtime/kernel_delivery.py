"""Pure K2-B plugin invocation; durable transaction ownership stays in the repository."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable, Protocol, Sequence, runtime_checkable

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
    KernelErrorEvidenceV1,
    KernelCommandLifecycleProjectionItemV1,
    KernelCommandLifecycleProjectionV1,
    KernelCommandOutcomeEventPayloadV1,
    KernelProjectionTypeV1,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
    Sha256V1,
    SideV1,
    ConsumedLineageRefV1,
    kernel_lease_fence_token_v1,
    safe_exception_summary_v1,
    stable_exception_reason_code_v1,
    TimerMutationV1,
)
from .plugin_registry import (
    PluginCatalogRuntimeV2,
    PluginCatalogSnapshotV1,
    PluginKeyV1,
    PluginRegistrationDescriptorV2,
)


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


@runtime_checkable
class ExecutionAlgoPluginV2(Protocol):
    manifest: ExecutionAlgoPluginManifestV2

    def initialize(self, context: AlgoStartContextV1) -> AlgoInitializationV1: ...

    def restore_state(self, snapshot: AlgoStateSnapshotV2) -> AlgoStateSnapshotV2: ...

    def transition(
        self,
        *,
        state: AlgoStateSnapshotV2,
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
    ) -> AlgoTransitionV1: ...


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
    def read_delivery(self, delivery_id: str) -> AlgoDeliveryPersistenceV1: ...

    def read_algo_instance(self, algo_instance_id: str) -> ExecutionAlgoInstancePersistenceV2: ...

    def claim_delivery(self, **values: Any) -> AlgoDeliveryPersistenceV1: ...

    def apply_claimed_delivery_atomic(self, **values: Any) -> dict[str, Any]: ...

    def mark_delivery_retryable(self, **values: Any) -> AlgoDeliveryPersistenceV1: ...

    def reclaim_stale_delivery(self, **values: Any) -> AlgoDeliveryPersistenceV1: ...


class KernelRequiredProviderUnavailable(RuntimeError):
    def __init__(self, message: str, *, context: dict[str, Any]) -> None:
        self.context = json_safe_evidence_v1(context)
        super().__init__(message)


class KernelDeliveryWorkerV1:
    def __init__(
        self,
        *,
        repository: KernelDeliveryRepositoryV1,
        catalog_runtime: PluginCatalogRuntimeV2,
        worker_id: str,
        process_incarnation_id: str,
    ) -> None:
        if not worker_id or not process_incarnation_id:
            raise ValueError("worker and process incarnation identities are required")
        self._repository = repository
        self._catalog_runtime = catalog_runtime
        self._lease_owner = f"{worker_id}:{process_incarnation_id}"

    def process_once(
        self,
        *,
        delivery_id: str,
        lease_expires_at: Any,
        logical_time_utc: Any,
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
            ],
            KernelDeliveryExecutionInputV1,
        ],
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
                inputs = input_builder(
                    event,
                    locked_delivery,
                    locked_algo,
                    previous_state,
                    active_mappings,
                    active_command_outboxes,
                    active_timers,
                    lifecycle_projection,
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
            try:
                resolved = resolve_plugin_for_restore_v1(
                    catalog_runtime=self._catalog_runtime,
                    plugin_id=locked_algo.plugin_id,
                    plugin_version=locked_algo.plugin_version,
                    plugin_manifest_sha256=locked_algo.plugin_manifest_sha256,
                    canonical_plugin_config=thaw_json_v1(locked_algo.plugin_config_json),
                    plugin_config_sha256=locked_algo.plugin_config_sha256,
                )
                transition = invoke_plugin_transition_v1(
                    plugin=resolved.plugin,
                    expected_manifest=resolved.descriptor.manifest,
                    state_codec=resolved.state_codec,
                    state=previous_state,
                    event=event,
                    services=inputs.services,
                    deterministic_context=inputs.deterministic_context,
                )
                return materialize_applied_transition_v1(
                    event=event,
                    predecessor_delivery=locked_delivery,
                    previous_algo=locked_algo,
                    transition=transition,
                    projection_set=inputs.services.execution_projection_set,
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
            except Exception as exc:
                return terminal_failure(
                    exc,
                    reason_code=stable_exception_reason_code_v1(exc, default="MINIQMT_ALGO_TRANSITION_FAILED"),
                    context=getattr(exc, "context", {"stage": "ALGO_DELIVERY_APPLY"}),
                    projection_set=inputs.services.execution_projection_set,
                )

        try:
            return self._repository.apply_claimed_delivery_atomic(
                delivery_id=delivery_id,
                expected_delivery_row_version=claimed.row_version,
                expected_algo_row_version=algo.row_version,
                expected_lease_owner=self._lease_owner,
                expected_lease_epoch=lease_epoch,
                expected_lease_fence_token=fence,
                bundle_builder=build,
            )
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


@dataclass(frozen=True)
class KernelTransitionWriteBundleV1:
    algo_instance: ExecutionAlgoInstancePersistenceV2
    delivery: AlgoDeliveryPersistenceV1
    receipt: AlgoTransitionReceiptV1 | AlgoFailureReceiptV1 | AlgoSkipReceiptV1
    projection_set: ExecutionProjectionSetV1 | None
    after_state: AlgoStateSnapshotV2 | None
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
        return cls(
            algo_instance=algo_instance,
            delivery=delivery,
            receipt=receipt,
            projection_set=projection_set,
            after_state=after_state,
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
        plugin = factory(validated_config)
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
    plugin: ExecutionAlgoPluginV2,
    expected_manifest: ExecutionAlgoPluginManifestV2,
    start_context: AlgoStartContextV1,
) -> AlgoInitializationV1:
    if not isinstance(expected_manifest, ExecutionAlgoPluginManifestV2):
        raise TypeError("expected_manifest must be ExecutionAlgoPluginManifestV2")
    if not isinstance(plugin, ExecutionAlgoPluginV2) or plugin.manifest != expected_manifest:
        raise _invocation_error(
            "MINIQMT_ALGO_PLUGIN_BINDING_INVALID",
            "initialize plugin manifest or pure method surface is invalid",
            stage="PLUGIN_INITIALIZE_BINDING",
            plugin_type=type(plugin).__name__,
            expected_manifest_sha256=expected_manifest.manifest_sha256,
        )
    if not isinstance(start_context, AlgoStartContextV1):
        raise TypeError("start_context must be AlgoStartContextV1")
    try:
        result = plugin.initialize(start_context)
    except Exception as exc:
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
        raise _invocation_error(
            "MINIQMT_ALGO_INITIALIZATION_RESULT_INVALID",
            "plugin initialization result does not close to its deterministic start context",
            stage="PLUGIN_INITIALIZE_RESULT",
            algo_instance_id=start_context.algo_instance_id,
            event_id=start_context.start_event_id,
            delivery_id=start_context.start_delivery_id,
        )
    return result


def invoke_plugin_transition_v1(
    *,
    plugin: ExecutionAlgoPluginV2,
    expected_manifest: ExecutionAlgoPluginManifestV2,
    state_codec: Callable[[ExecutionAlgoPluginManifestV2, Any], Any],
    state: AlgoStateSnapshotV2,
    event: RuntimeEventEnvelopeV2,
    services: AlgoReadOnlyServicesV1,
    deterministic_context: DeterministicExecutionContextV1,
) -> AlgoTransitionV1:
    if not isinstance(expected_manifest, ExecutionAlgoPluginManifestV2):
        raise TypeError("expected_manifest must be ExecutionAlgoPluginManifestV2")
    if not isinstance(plugin, ExecutionAlgoPluginV2) or plugin.manifest != expected_manifest:
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
        codec_state = state_codec(expected_manifest, thaw_json_v1(state.state))
        if thaw_json_v1(freeze_json_v1(codec_state)) != thaw_json_v1(state.state):
            raise ValueError("state codec changed durable state during strict readback")
        restored = plugin.restore_state(state)
        if restored != state:
            raise ValueError("restore_state changed the frozen same-version state snapshot")
        result = plugin.transition(state=restored, event=event, services=services)
    except KernelPluginInvocationError:
        raise
    except Exception as exc:
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
    return result


__all__ = [
    "ExecutionAlgoPluginV2",
    "KernelAlgoStartWriteBundleV1",
    "KernelAlgoCreationRequestV1",
    "KernelDeliveryExecutionInputV1",
    "KernelPluginInvocationError",
    "KernelTransitionWriteBundleV1",
    "ResolvedKernelPluginV1",
    "build_command_lifecycle_projection_v1",
    "invoke_plugin_initialize_v1",
    "invoke_plugin_transition_v1",
    "resolve_plugin_for_restore_v1",
]
