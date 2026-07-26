"""Deterministic K2-B plugin effect materialization into strict durable carriers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
from typing import Any

from .kernel_delivery import KernelTransitionWriteBundleV1
from .plugin_canonical import hash_hex_v1, thaw_json_v1
from .plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoFailureReceiptV1,
    AlgoSkipReceiptV1,
    AlgoTransitionReceiptV1,
    AlgoTransitionV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerCommandV2,
    BrokerCommandTypeV2,
    CommandChildMappingStatusV1,
    ConsumedLineageTypeV1,
    ConsumedLineageRefV1,
    DeliveryStatusV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    EventTypeV2,
    KernelProjectionTypeV1,
    KernelErrorEvidenceV1,
    OrderTypeV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
    TerminalOutcomeV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    canonical_utc_datetime_v1,
    transaction_commit_identity_v1,
    safe_exception_summary_v1,
)


class KernelEffectMaterializationError(ValueError):
    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = context
        super().__init__(message)


def _pending_outbox(
    *,
    command: Any,
    mapping_id: str,
    logical_time_utc: Any,
) -> BrokerCommandOutboxV1:
    return BrokerCommandOutboxV1.create(
        command=command,
        mapping_id=mapping_id,
        status=BrokerCommandOutboxStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at=None,
        dispatch_attempt_id=None,
        next_attempt_at_utc=None,
        broker_called=None,
        broker_order_id=None,
        ack_receipt_json=None,
        ack_receipt_sha256=None,
        non_acceptance_receipt=None,
        unknown_outcome_receipt=None,
        reconcile_receipt=None,
        last_error_json=None,
        row_version=1,
        created_at_utc=logical_time_utc,
        updated_at_utc=logical_time_utc,
        closed_at_utc=None,
    )


def _terminalize_reserved_mapping_v1(
    *, previous: ExecutionCommandChildMappingV1, logical_time_utc: Any
) -> ExecutionCommandChildMappingV1:
    payload = previous.model_dump(mode="python")
    payload.update(
        mapping_status=CommandChildMappingStatusV1.TERMINAL,
        mapping_version=previous.mapping_version + 1,
        updated_at_utc=canonical_utc_datetime_v1(logical_time_utc, field_name="updated_at_utc"),
    )
    receipt_payload = previous.canonical_payload_v1(exclude={"mapping_receipt_sha256"})
    receipt_payload.update(
        mapping_status=CommandChildMappingStatusV1.TERMINAL.value,
        mapping_version=previous.mapping_version + 1,
        updated_at_utc=payload["updated_at_utc"],
    )
    payload["mapping_receipt_sha256"] = hash_hex_v1(
        "miniqmt_command_child_mapping_receipt_v1",
        receipt_payload,
    )
    successor = ExecutionCommandChildMappingV1.model_validate(payload)
    successor.validate_successor_v1(previous)
    return successor


def _terminalize_pre_call_outbox_v1(
    *,
    previous: BrokerCommandOutboxV1,
    error_evidence: KernelErrorEvidenceV1,
    logical_time_utc: Any,
) -> BrokerCommandOutboxV1:
    command = BrokerCommandV2.model_validate_json(
        json.dumps(thaw_json_v1(previous.payload_json), sort_keys=True, separators=(",", ":"))
    )
    successor = BrokerCommandOutboxV1.create(
        command=command,
        mapping_id=previous.mapping_id,
        status=BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
        attempt_count=previous.attempt_count,
        lease_owner=None,
        lease_epoch=previous.lease_epoch,
        lease_fence_token=None,
        lease_expires_at=None,
        dispatch_attempt_id=previous.dispatch_attempt_id,
        next_attempt_at_utc=None,
        broker_called=False,
        broker_order_id=None,
        ack_receipt_json=None,
        ack_receipt_sha256=None,
        non_acceptance_receipt=None,
        unknown_outcome_receipt=None,
        reconcile_receipt=None,
        last_error_json=error_evidence.model_dump(mode="json"),
        row_version=previous.row_version + 1,
        created_at_utc=previous.created_at_utc,
        updated_at_utc=logical_time_utc,
        closed_at_utc=logical_time_utc,
    )
    successor.validate_successor_v1(previous)
    return successor


def _materialize_commands(
    *,
    transition: AlgoTransitionV1,
    strategy_slot_id: str,
    logical_time_utc: Any,
    existing_mappings_by_local_vt_orderid: Mapping[str, ExecutionCommandChildMappingV1],
) -> tuple[tuple[ExecutionCommandChildMappingV1, ...], tuple[BrokerCommandOutboxV1, ...]]:
    mappings: list[ExecutionCommandChildMappingV1] = []
    outboxes: list[BrokerCommandOutboxV1] = []
    for command in transition.broker_commands:
        if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            mapping = ExecutionCommandChildMappingV1.create(
                command=command,
                strategy_slot_id=strategy_slot_id,
                mapping_status=CommandChildMappingStatusV1.RESERVED,
                mapping_version=1,
                broker_order_id=None,
                broker_identity_source_event_id=None,
                last_order_event_id=None,
                last_trade_event_id=None,
                updated_by_event_id=None,
                created_at_utc=logical_time_utc,
                updated_at_utc=logical_time_utc,
            )
            mappings.append(mapping)
            mapping_id = mapping.mapping_id
        else:
            mapping = existing_mappings_by_local_vt_orderid.get(command.local_vt_orderid)
            if (
                mapping is None
                or mapping.runtime_id != command.runtime_id
                or mapping.algo_instance_id != command.algo_instance_id
                or mapping.parent_intent_id != command.parent_intent_id
                or mapping.broker_order_id != command.owned_broker_order_id
            ):
                raise KernelEffectMaterializationError(
                    "MINIQMT_ALGO_TRANSITION_CANCEL_OWNER_INVALID",
                    "CANCEL command does not close to one exact durable active child mapping",
                    context={
                        "command_id": command.command_id,
                        "local_vt_orderid": command.local_vt_orderid,
                        "owned_broker_order_id": command.owned_broker_order_id,
                    },
                )
            mapping_id = mapping.mapping_id
        outboxes.append(_pending_outbox(command=command, mapping_id=mapping_id, logical_time_utc=logical_time_utc))
    return tuple(mappings), tuple(outboxes)


def _materialize_timers(
    *,
    runtime_id: str,
    transition: AlgoTransitionV1,
    logical_time_utc: Any,
    existing_timer_schedules: Mapping[str, ExecutionAlgoTimerScheduleV1],
) -> tuple[ExecutionAlgoTimerScheduleV1, ...]:
    schedules: list[ExecutionAlgoTimerScheduleV1] = []
    for mutation in transition.timer_mutations:
        if mutation.mutation_type is TimerMutationTypeV1.UPSERT_ONE_SHOT:
            schedule = ExecutionAlgoTimerScheduleV1.create(
                runtime_id=runtime_id,
                mutation=mutation,
                status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
                emitted_event_id=None,
                lease_owner=None,
                lease_epoch=0,
                lease_fence_token=None,
                lease_expires_at_utc=None,
                row_version=1,
                created_at_utc=logical_time_utc,
                updated_at_utc=logical_time_utc,
                closed_at_utc=None,
            )
        else:
            previous = existing_timer_schedules.get(mutation.schedule_id)
            if previous is None or previous.algo_instance_id != mutation.algo_instance_id:
                raise KernelEffectMaterializationError(
                    "MINIQMT_TIMER_CANCEL_OWNER_INVALID",
                    "CANCEL timer mutation has no exact durable schedule owner",
                    context={"schedule_id": mutation.schedule_id, "algo_instance_id": mutation.algo_instance_id},
                )
            schedule = _cancel_timer_schedule(previous=previous, logical_time_utc=logical_time_utc)
        schedules.append(schedule)
    return tuple(schedules)


def _cancel_timer_schedule(
    *, previous: ExecutionAlgoTimerScheduleV1, logical_time_utc: Any
) -> ExecutionAlgoTimerScheduleV1:
    payload = previous.model_dump(mode="python")
    payload.update(
        status=ExecutionAlgoTimerScheduleStatusV1.CANCELLED,
        row_version=previous.row_version + 1,
        updated_at_utc=logical_time_utc,
        closed_at_utc=logical_time_utc,
    )
    canonical_payload = previous.model_dump(mode="json")
    canonical_payload.update(
        status=ExecutionAlgoTimerScheduleStatusV1.CANCELLED.value,
        row_version=previous.row_version + 1,
        updated_at_utc=canonical_utc_datetime_v1(logical_time_utc, field_name="updated_at_utc"),
        closed_at_utc=canonical_utc_datetime_v1(logical_time_utc, field_name="closed_at_utc"),
    )
    payload["schedule_receipt_sha256"] = hash_hex_v1(
        "miniqmt_timer_schedule_receipt_v1",
        {key: value for key, value in canonical_payload.items() if key != "schedule_receipt_sha256"},
    )
    schedule = ExecutionAlgoTimerScheduleV1.model_validate(payload)
    schedule.validate_successor_v1(previous)
    return schedule


def _terminal_status(
    terminal_outcome: TerminalOutcomeV1 | None,
    *,
    initialization: bool,
    previous_status: ExecutionAlgoPersistenceStatusV2 | None,
) -> ExecutionAlgoPersistenceStatusV2:
    if terminal_outcome is None:
        return (
            ExecutionAlgoPersistenceStatusV2.ACTIVE
            if initialization
            else previous_status or ExecutionAlgoPersistenceStatusV2.ACTIVE
        )
    mapping = {
        TerminalOutcomeV1.FILLED: ExecutionAlgoPersistenceStatusV2.COMPLETED,
        TerminalOutcomeV1.CANCELLED: ExecutionAlgoPersistenceStatusV2.CANCELLED,
        TerminalOutcomeV1.EXPIRED_WITH_RESIDUAL: ExecutionAlgoPersistenceStatusV2.EXPIRED_WITH_RESIDUAL,
    }
    status = mapping.get(terminal_outcome)
    if status is None:
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_TERMINAL_OUTCOME_INVALID",
            "REJECTED/FAILED_TERMINAL plugin outcomes require a failure receipt, not an applied transition",
            context={"terminal_outcome": terminal_outcome.value},
        )
    return status


def _validate_projection_lineage_v1(
    *,
    event: RuntimeEventEnvelopeV2,
    projection_set: ExecutionProjectionSetV1,
    consumed_lineage_refs: Sequence[ConsumedLineageRefV1],
    has_broker_commands: bool,
) -> None:
    lineages = tuple(consumed_lineage_refs)
    if any(not isinstance(item, ConsumedLineageRefV1) for item in lineages):
        raise TypeError("consumed_lineage_refs must contain only ConsumedLineageRefV1")
    identities = tuple((item.lineage_type, item.identity) for item in lineages)
    if len(identities) != len(set(identities)):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_LINEAGE_DUPLICATE",
            "transition consumed lineage contains duplicate authority identity",
            context={"event_id": event.event_id},
        )
    event_lineage = next((item for item in lineages if item.lineage_type is ConsumedLineageTypeV1.EVENT), None)
    if (
        event_lineage is None
        or event_lineage.identity != event.event_id
        or event_lineage.payload_sha256 != event.payload_sha256
    ):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_EVENT_LINEAGE_INVALID",
            "transition must consume the exact durable runtime event envelope",
            context={"event_id": event.event_id, "payload_sha256": event.payload_sha256},
        )
    refs = {item.projection_type: item for item in projection_set.ordered_projection_refs}
    if event.event_type is EventTypeV2.TICK:
        source_identity = thaw_json_v1(event.source_identity)
        market_data_id = source_identity.get("market_data_id")
        market_ref = refs.get(KernelProjectionTypeV1.MARKET_DATA)
        market_lineage = next(
            (item for item in lineages if item.lineage_type is ConsumedLineageTypeV1.MARKET_DATA),
            None,
        )
        if (
            type(market_data_id) is not str
            or market_ref is None
            or market_lineage is None
            or market_ref.projection_id != market_data_id
            or market_lineage.identity != market_data_id
            or market_lineage.payload_sha256 != market_ref.payload_sha256
            or market_ref.source_event_id != event.event_id
        ):
            raise KernelEffectMaterializationError(
                "MINIQMT_ALGO_TRANSITION_MARKET_DATA_LINEAGE_INVALID",
                "TICK transition must close to exact market-data projection and lineage",
                context={"event_id": event.event_id, "market_data_id": market_data_id},
            )
    if has_broker_commands:
        required = {
            KernelProjectionTypeV1.ROUTE_COMPATIBILITY: (
                "plugin_route_compatibility_receipt_v1",
                "mqroutecompat_",
            ),
            KernelProjectionTypeV1.OMS_PREFLIGHT: (
                "miniqmt_oms_preflight_projection_receipt_v1",
                "mqomspreflight_",
            ),
            KernelProjectionTypeV1.RISK_DECISION: (
                "miniqmt_risk_decision_receipt_v1",
                "mqriskdecision_",
            ),
            KernelProjectionTypeV1.KILL_SWITCH_STATE: (
                "miniqmt_kill_switch_state_v1",
                "mqkillswitch_",
            ),
        }
        missing = tuple(sorted(item.value for item in set(required) - set(refs)))
        if missing:
            raise KernelEffectMaterializationError(
                "MINIQMT_ALGO_TRANSITION_COMMAND_AUTHORITY_MISSING",
                "broker command lacks exact route, OMS, risk or kill-switch projection authority",
                context={"event_id": event.event_id, "missing_projection_types": missing},
            )
        invalid = tuple(
            sorted(
                projection_type.value
                for projection_type, (version, identity_prefix) in required.items()
                if refs[projection_type].projection_version != version
                or not refs[projection_type].projection_id.startswith(identity_prefix)
                or refs[projection_type].source_event_id != event.event_id
            )
        )
        if invalid:
            raise KernelEffectMaterializationError(
                "MINIQMT_ALGO_TRANSITION_COMMAND_AUTHORITY_INVALID",
                "broker command projection authority has invalid schema, identity or event owner",
                context={"event_id": event.event_id, "invalid_projection_types": invalid},
            )


def materialize_applied_transition_v1(
    *,
    event: RuntimeEventEnvelopeV2,
    predecessor_delivery: AlgoDeliveryPersistenceV1,
    previous_algo: ExecutionAlgoInstancePersistenceV2 | None,
    transition: AlgoTransitionV1,
    projection_set: ExecutionProjectionSetV1,
    consumed_lineage_refs: Sequence[ConsumedLineageRefV1],
    strategy_slot_id: str,
    parent_intent_id: str,
    compatibility_receipt_sha256: str,
    plugin_config: dict[str, Any],
    plugin_config_sha256: str,
    target_quantity: int,
    algo_code: str,
    symbol: str,
    side: Any,
    existing_mappings_by_local_vt_orderid: Mapping[str, ExecutionCommandChildMappingV1],
    existing_timer_schedules: Mapping[str, ExecutionAlgoTimerScheduleV1],
    initialization: bool,
) -> KernelTransitionWriteBundleV1:
    next_state = transition.next_state
    if (
        event.runtime_id != predecessor_delivery.runtime_id
        or event.event_id != predecessor_delivery.event_id
        or next_state.algo_instance_id != predecessor_delivery.algo_instance_id
        or projection_set.runtime_id != event.runtime_id
        or projection_set.algo_instance_id != next_state.algo_instance_id
        or projection_set.event_id != event.event_id
        or projection_set.delivery_id != predecessor_delivery.delivery_id
    ):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_OWNER_CONFLICT",
            "event, delivery, state and projection owners do not close",
            context={"event_id": event.event_id, "delivery_id": predecessor_delivery.delivery_id},
        )
    _validate_projection_lineage_v1(
        event=event,
        projection_set=projection_set,
        consumed_lineage_refs=consumed_lineage_refs,
        has_broker_commands=bool(transition.broker_commands),
    )
    provisional = AlgoTransitionReceiptV1.create(
        delivery_id=predecessor_delivery.delivery_id,
        event_id=event.event_id,
        runtime_id=event.runtime_id,
        algo_instance_id=next_state.algo_instance_id,
        plugin_id=next_state.plugin_id,
        plugin_version=next_state.plugin_version,
        plugin_manifest_sha256=next_state.plugin_manifest_sha256,
        transition_sequence=next_state.transition_sequence,
        before_state_sha256_or_INIT=("INIT" if previous_algo is None else previous_algo.state_sha256),
        after_state_sha256=next_state.state_sha256,
        ordered_command_ids=tuple(item.command_id for item in transition.broker_commands),
        ordered_timer_mutation_ids=tuple(item.mutation_identity_v1() for item in transition.timer_mutations),
        ordered_diagnostic_observation_ids=tuple(item.observation_id for item in transition.diagnostic_observations),
        ordered_consumed_lineage_refs=tuple(consumed_lineage_refs),
        execution_projection_set_sha256=projection_set.projection_set_sha256,
        effect_set_sha256=transition.effect_set_sha256,
        terminal_outcome=transition.terminal_outcome,
        logical_applied_at_utc=next_state.updated_at_utc,
        transaction_commit_identity="mqtx_pending_materialization",
    )
    if any(item.transition_id != provisional.transition_id for item in transition.broker_commands):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_COMMAND_IDENTITY_INVALID",
            "plugin command transition identity differs from the deterministic receipt identity",
            context={"transition_id": provisional.transition_id},
        )
    if any(item.transition_id != provisional.transition_id for item in transition.timer_mutations):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_TIMER_IDENTITY_INVALID",
            "plugin timer transition identity differs from the deterministic receipt identity",
            context={"transition_id": provisional.transition_id},
        )
    if any(item.transition_id != provisional.transition_id for item in transition.diagnostic_observations):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_DIAGNOSTIC_IDENTITY_INVALID",
            "plugin diagnostic transition identity differs from the deterministic receipt identity",
            context={"transition_id": provisional.transition_id},
        )
    mappings, outboxes = _materialize_commands(
        transition=transition,
        strategy_slot_id=strategy_slot_id,
        logical_time_utc=next_state.updated_at_utc,
        existing_mappings_by_local_vt_orderid=existing_mappings_by_local_vt_orderid,
    )
    schedules = _materialize_timers(
        runtime_id=event.runtime_id,
        transition=transition,
        logical_time_utc=next_state.updated_at_utc,
        existing_timer_schedules=existing_timer_schedules,
    )
    status = _terminal_status(
        transition.terminal_outcome,
        initialization=initialization,
        previous_status=None if previous_algo is None else previous_algo.status,
    )
    active_child_count = (0 if previous_algo is None else previous_algo.active_child_count) + len(mappings)
    if (
        status
        in {
            ExecutionAlgoPersistenceStatusV2.COMPLETED,
            ExecutionAlgoPersistenceStatusV2.CANCELLED,
            ExecutionAlgoPersistenceStatusV2.EXPIRED_WITH_RESIDUAL,
        }
        and active_child_count
    ):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_TERMINAL_ACTIVE_CHILD",
            "non-failure terminal transition cannot retain active durable children",
            context={"active_child_count": active_child_count, "status": status.value},
        )
    closure = (
        ActiveChildClosureStatusV1.CLEAN
        if status
        in {
            ExecutionAlgoPersistenceStatusV2.COMPLETED,
            ExecutionAlgoPersistenceStatusV2.CANCELLED,
            ExecutionAlgoPersistenceStatusV2.EXPIRED_WITH_RESIDUAL,
        }
        else ActiveChildClosureStatusV1.NOT_APPLICABLE
    )
    state_payload = thaw_json_v1(next_state.state)
    state_parent_quantity = state_payload.get("parent_quantity")
    traded_quantity = state_payload.get("traded_quantity")
    if (
        type(state_parent_quantity) is not int
        or state_parent_quantity != target_quantity
        or type(traded_quantity) is not int
        or traded_quantity < 0
        or traded_quantity > target_quantity
    ):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_QUANTITY_AUTHORITY_INVALID",
            "plugin state does not expose the exact parent and traded quantity closure",
            context={
                "target_quantity": target_quantity,
                "state_parent_quantity": state_parent_quantity,
                "state_traded_quantity": traded_quantity,
            },
        )
    if previous_algo is not None and traded_quantity < previous_algo.traded_quantity:
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_QUANTITY_REGRESSION",
            "plugin state regressed the durable cumulative traded quantity",
            context={
                "previous_traded_quantity": previous_algo.traded_quantity,
                "state_traded_quantity": traded_quantity,
            },
        )
    if transition.terminal_outcome is TerminalOutcomeV1.FILLED and traded_quantity != target_quantity:
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_TRANSITION_FILLED_QUANTITY_INVALID",
            "FILLED terminal outcome requires cumulative traded quantity equal to parent quantity",
            context={"target_quantity": target_quantity, "traded_quantity": traded_quantity},
        )
    remaining_quantity = target_quantity - traded_quantity
    terminal = status not in {ExecutionAlgoPersistenceStatusV2.ACTIVE, ExecutionAlgoPersistenceStatusV2.PAUSED}
    algo = ExecutionAlgoInstancePersistenceV2.create(
        algo_instance_id=next_state.algo_instance_id,
        runtime_id=event.runtime_id,
        parent_intent_id=parent_intent_id,
        strategy_slot_id=strategy_slot_id,
        symbol=symbol,
        side=side,
        target_quantity=target_quantity,
        traded_quantity=traded_quantity,
        remaining_quantity=remaining_quantity,
        algo_code=algo_code,
        plugin_id=next_state.plugin_id,
        plugin_version=next_state.plugin_version,
        plugin_manifest_sha256=next_state.plugin_manifest_sha256,
        plugin_config_json=plugin_config,
        plugin_config_sha256=plugin_config_sha256,
        compatibility_receipt_sha256=compatibility_receipt_sha256,
        state_schema_version=next_state.state_schema_version,
        state_json=thaw_json_v1(next_state.state),
        state_sha256=next_state.state_sha256,
        transition_sequence=next_state.transition_sequence,
        last_applied_delivery_sequence=next_state.last_applied_delivery_sequence,
        last_applied_delivery_id=next_state.last_applied_delivery_id,
        last_closed_delivery_sequence=next_state.last_closed_delivery_sequence,
        terminal_delivery_sequence=predecessor_delivery.algo_delivery_sequence if terminal else None,
        status=status,
        failure_receipt_id=None,
        active_child_closure_status=closure,
        active_child_count=active_child_count,
        row_version=1 if previous_algo is None else previous_algo.row_version + 1,
        created_at_utc=next_state.updated_at_utc if previous_algo is None else previous_algo.created_at_utc,
        updated_at_utc=next_state.updated_at_utc,
        terminal_at_utc=next_state.updated_at_utc if terminal else None,
        archived_at_utc=None if previous_algo is None else previous_algo.archived_at_utc,
    )
    input_hashes = (
        projection_set.projection_set_sha256,
        next_state.state_sha256,
        *(item.payload_sha256 for item in mappings),
        *(item.payload_sha256 for item in outboxes),
        *(item.schedule_receipt_sha256 for item in schedules),
        *(item.context_sha256 for item in transition.diagnostic_observations),
    )
    transition_outputs = (
        provisional.transition_id,
        *(item.mapping_id for item in mappings),
        *(item.command_id for item in outboxes),
        *(item.schedule_id for item in schedules),
        *(item.observation_id for item in transition.diagnostic_observations),
    )
    if initialization:
        provisional_ingress = RuntimeEventIngressReceiptV1.create(
            runtime_id=event.runtime_id,
            event_id=event.event_id,
            event_key_sha256=event.event_key_sha256,
            runtime_sequence=event.sequence,
            ordered_target_algo_instance_ids=(next_state.algo_instance_id,),
            ordered_delivery_ids=(predecessor_delivery.delivery_id,),
            transaction_commit_identity="mqtx_pending_materialization",
        )
        operation = "INITIALIZE_ALGO_ATOMIC_APPLIED"
        input_hashes = (event.event_key_sha256, event.payload_sha256, *input_hashes)
        output_identities = (
            event.event_id,
            provisional_ingress.ingress_receipt_id,
            predecessor_delivery.delivery_id,
            *transition_outputs,
        )
    else:
        operation = "APPLY_CLAIMED_DELIVERY_ATOMIC_APPLIED"
        output_identities = transition_outputs
    tx_identity = transaction_commit_identity_v1(
        operation=operation,
        owner_identities=(
            event.runtime_id,
            next_state.algo_instance_id,
            event.event_id,
            predecessor_delivery.delivery_id,
        ),
        input_hashes=input_hashes,
        output_identities=output_identities,
    )
    receipt = AlgoTransitionReceiptV1.create(
        **provisional.canonical_payload_v1(
            exclude={
                "schema_version",
                "transition_id",
                "ordered_consumed_lineage_refs",
                "transaction_commit_identity",
                "receipt_sha256",
            }
        ),
        ordered_consumed_lineage_refs=provisional.ordered_consumed_lineage_refs,
        transaction_commit_identity=tx_identity,
    )
    delivery_payload = predecessor_delivery.model_dump(mode="python")
    delivery_payload.update(
        status=DeliveryStatusV1.APPLIED,
        lease_owner=None,
        lease_expires_at=None,
        lease_fence_token=None,
        transition_id=receipt.transition_id,
        last_error_json=None,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        row_version=predecessor_delivery.row_version + 1,
        updated_at_utc=next_state.updated_at_utc,
        closed_at_utc=next_state.updated_at_utc,
    )
    delivery = AlgoDeliveryPersistenceV1.model_validate(delivery_payload)
    return KernelTransitionWriteBundleV1.create(
        algo_instance=algo,
        delivery=delivery,
        receipt=receipt,
        projection_set=projection_set,
        after_state=next_state,
        new_child_mappings=mappings,
        command_outboxes=outboxes,
        timer_mutations=transition.timer_mutations,
        timer_schedules=schedules,
        diagnostic_observations=transition.diagnostic_observations,
    )


def materialize_failure_transition_v1(
    *,
    event: RuntimeEventEnvelopeV2,
    predecessor_delivery: AlgoDeliveryPersistenceV1,
    previous_algo: ExecutionAlgoInstancePersistenceV2 | None,
    algo_code: str,
    plugin_id: str,
    plugin_version: str,
    plugin_manifest_sha256: str,
    plugin_config: dict[str, Any],
    plugin_config_sha256: str,
    compatibility_receipt_sha256: str,
    parent_intent_id: str,
    strategy_slot_id: str,
    symbol: str,
    side: Any,
    target_quantity: int,
    stable_reason_code: str,
    exception: BaseException,
    failure_context: dict[str, Any],
    projection_set: ExecutionProjectionSetV1 | None = None,
    active_mappings: Sequence[ExecutionCommandChildMappingV1],
    active_command_outboxes: Sequence[BrokerCommandOutboxV1] = (),
    active_timer_schedules: Sequence[ExecutionAlgoTimerScheduleV1],
    logical_time_utc: Any,
    initialization: bool,
) -> KernelTransitionWriteBundleV1:
    if initialization != (previous_algo is None):
        raise ValueError("initialization flag must match previous algo absence")
    ordered_mappings = tuple(sorted(active_mappings, key=lambda item: item.child_order_id))
    if len({item.child_order_id for item in ordered_mappings}) != len(ordered_mappings):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_FAILURE_ACTIVE_CHILD_DUPLICATE",
            "failure materialization received duplicate active child identity",
            context={"algo_instance_id": predecessor_delivery.algo_instance_id},
        )
    transition_sequence = 1 if previous_algo is None else previous_algo.transition_sequence + 1
    exception_summary = safe_exception_summary_v1(exception)
    evidence_context = {**failure_context}
    if "renderer_error_type" in exception_summary:
        evidence_context["renderer_error_type"] = exception_summary["renderer_error_type"]
    error_evidence = KernelErrorEvidenceV1.create(
        stage="ALGO_INITIALIZATION" if initialization else "ALGO_DELIVERY_APPLY",
        stable_reason_code=stable_reason_code,
        exception=exception,
        message=exception_summary["exception_message"],
        retryable=False,
        terminal=True,
        broker_called=False,
        primary_context={
            "runtime_id": event.runtime_id,
            "algo_instance_id": predecessor_delivery.algo_instance_id,
            "event_id": event.event_id,
            "delivery_id": predecessor_delivery.delivery_id,
            **evidence_context,
        },
        secondary_errors=[],
    )
    provisional_failure = AlgoFailureReceiptV1.create(
        delivery_id=predecessor_delivery.delivery_id,
        event_id=event.event_id,
        runtime_id=event.runtime_id,
        algo_instance_id=predecessor_delivery.algo_instance_id,
        plugin_id=plugin_id,
        plugin_version=plugin_version,
        plugin_manifest_sha256=plugin_manifest_sha256,
        transition_sequence=transition_sequence,
        stable_reason_code=stable_reason_code,
        exception_type=exception_summary["exception_type"],
        message=exception_summary["exception_message"],
        context=evidence_context,
        last_good_state_sha256_or_ABSENT_INITIAL_STATE=(
            "ABSENT_INITIAL_STATE" if previous_algo is None else previous_algo.state_sha256
        ),
        ordered_cancel_command_ids=(),
        ordered_active_child_ids=tuple(item.child_order_id for item in ordered_mappings),
        active_child_closure_status=(
            ActiveChildClosureStatusV1.CLEAN.value
            if not ordered_mappings
            else ActiveChildClosureStatusV1.OUTCOME_UNKNOWN.value
        ),
        transaction_commit_identity="mqtx_pending_failure",
    )
    cancel_commands: list[BrokerCommandV2] = []
    outboxes: list[BrokerCommandOutboxV1] = []
    updated_mappings: list[ExecutionCommandChildMappingV1] = []
    updated_outboxes: list[BrokerCommandOutboxV1] = []
    outbox_by_mapping = {item.mapping_id: item for item in active_command_outboxes}
    if len(outbox_by_mapping) != len(tuple(active_command_outboxes)):
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_FAILURE_OUTBOX_DUPLICATE",
            "failure materialization received duplicate active command outbox ownership",
            context={"algo_instance_id": predecessor_delivery.algo_instance_id},
        )
    any_unknown = False
    for mapping in ordered_mappings:
        submit_outbox = outbox_by_mapping.get(mapping.mapping_id)
        if mapping.mapping_status is CommandChildMappingStatusV1.RESERVED:
            if submit_outbox is None or submit_outbox.command_type is not BrokerCommandTypeV2.SUBMIT_LIMIT:
                raise KernelEffectMaterializationError(
                    "MINIQMT_ALGO_FAILURE_SUBMIT_OUTBOX_MISSING",
                    "RESERVED child has no exact durable SUBMIT outbox",
                    context={"mapping_id": mapping.mapping_id},
                )
            if submit_outbox.status not in {
                BrokerCommandOutboxStatusV1.PENDING,
                BrokerCommandOutboxStatusV1.CLAIMED,
                BrokerCommandOutboxStatusV1.FAILED_RETRYABLE,
            }:
                raise KernelEffectMaterializationError(
                    "MINIQMT_ALGO_FAILURE_SUBMIT_OUTBOX_STATE_CONFLICT",
                    "RESERVED child is paired with a post-dispatch SUBMIT outbox",
                    context={"mapping_id": mapping.mapping_id, "outbox_status": submit_outbox.status.value},
                )
            updated_mappings.append(
                _terminalize_reserved_mapping_v1(previous=mapping, logical_time_utc=logical_time_utc)
            )
            updated_outboxes.append(
                _terminalize_pre_call_outbox_v1(
                    previous=submit_outbox,
                    error_evidence=error_evidence,
                    logical_time_utc=logical_time_utc,
                )
            )
            continue
        if mapping.broker_order_id is None:
            any_unknown = True
            continue
        command = BrokerCommandV2.create(
            command_type=BrokerCommandTypeV2.CANCEL_ORDER,
            runtime_id=event.runtime_id,
            algo_instance_id=predecessor_delivery.algo_instance_id,
            parent_intent_id=parent_intent_id,
            transition_id=provisional_failure.failure_receipt_id,
            ordinal=len(cancel_commands),
            local_vt_orderid=mapping.local_vt_orderid,
            symbol=mapping.symbol,
            side=mapping.side,
            order_type=OrderTypeV1.LIMIT,
            price_decimal=mapping.requested_price_decimal,
            quantity=mapping.requested_quantity,
            owned_broker_order_id=mapping.broker_order_id,
            reason_code="MINIQMT_ALGO_FAILURE_CANCEL_ACTIVE_CHILD",
            metadata={"failure_receipt_id": provisional_failure.failure_receipt_id, "mapping_id": mapping.mapping_id},
        )
        cancel_commands.append(command)
        outboxes.append(
            _pending_outbox(command=command, mapping_id=mapping.mapping_id, logical_time_utc=logical_time_utc)
        )
    timer_mutations: list[TimerMutationV1] = []
    cancelled_schedules: list[ExecutionAlgoTimerScheduleV1] = []
    for schedule in sorted(active_timer_schedules, key=lambda item: item.schedule_id):
        mutation = TimerMutationV1.create(
            mutation_type=TimerMutationTypeV1.CANCEL,
            algo_instance_id=schedule.algo_instance_id,
            transition_id=provisional_failure.failure_receipt_id,
            ordinal=len(cancel_commands) + len(timer_mutations),
            timer_name=schedule.timer_name,
            schedule_epoch=schedule.schedule_epoch,
            due_at_exchange_utc=None,
            catch_up_policy=schedule.catch_up_policy,
            payload=thaw_json_v1(schedule.payload),
        )
        if mutation.schedule_id != schedule.schedule_id:
            raise KernelEffectMaterializationError(
                "MINIQMT_TIMER_CANCEL_OWNER_INVALID",
                "failure timer cancellation changed durable schedule identity",
                context={"schedule_id": schedule.schedule_id},
            )
        successor = _cancel_timer_schedule(previous=schedule, logical_time_utc=logical_time_utc)
        timer_mutations.append(mutation)
        cancelled_schedules.append(successor)
    active_child_count = len(ordered_mappings) - len(updated_mappings)
    closure = (
        ActiveChildClosureStatusV1.CLEAN
        if active_child_count == 0
        else ActiveChildClosureStatusV1.OUTCOME_UNKNOWN
        if any_unknown
        else ActiveChildClosureStatusV1.CANCEL_PENDING
    )
    provisional_failure = AlgoFailureReceiptV1.create(
        **provisional_failure.canonical_payload_v1(
            exclude={
                "schema_version",
                "failure_receipt_id",
                "bounded_context",
                "context_sha256",
                "ordered_cancel_command_ids",
                "active_child_closure_status",
                "transaction_commit_identity",
                "failure_receipt_sha256",
            }
        ),
        context=evidence_context,
        ordered_cancel_command_ids=tuple(item.command_id for item in cancel_commands),
        active_child_closure_status=closure.value,
        transaction_commit_identity="mqtx_pending_failure",
    )
    input_hashes = (
        plugin_manifest_sha256,
        provisional_failure.context_sha256,
        *((projection_set.projection_set_sha256,) if projection_set is not None else ()),
        *(item.payload_sha256 for item in outboxes),
        *(item.payload_sha256 for item in updated_mappings),
        *(item.payload_sha256 for item in updated_outboxes),
        *(item.schedule_receipt_sha256 for item in cancelled_schedules),
    )
    transition_outputs = (
        provisional_failure.failure_receipt_id,
        *(item.command_id for item in outboxes),
        *(item.mapping_id for item in updated_mappings),
        *(item.command_id for item in updated_outboxes),
        *(item.schedule_id for item in cancelled_schedules),
    )
    if initialization:
        provisional_ingress = RuntimeEventIngressReceiptV1.create(
            runtime_id=event.runtime_id,
            event_id=event.event_id,
            event_key_sha256=event.event_key_sha256,
            runtime_sequence=event.sequence,
            ordered_target_algo_instance_ids=(predecessor_delivery.algo_instance_id,),
            ordered_delivery_ids=(predecessor_delivery.delivery_id,),
            transaction_commit_identity="mqtx_pending_failure",
        )
        operation = "INITIALIZE_ALGO_ATOMIC_FAILED_TERMINAL"
        input_hashes = (event.event_key_sha256, event.payload_sha256, *input_hashes)
        output_identities = (
            event.event_id,
            provisional_ingress.ingress_receipt_id,
            predecessor_delivery.delivery_id,
            *transition_outputs,
        )
    else:
        operation = "APPLY_CLAIMED_DELIVERY_ATOMIC_FAILED_TERMINAL"
        output_identities = transition_outputs
    tx_identity = transaction_commit_identity_v1(
        operation=operation,
        owner_identities=(
            event.runtime_id,
            predecessor_delivery.algo_instance_id,
            event.event_id,
            predecessor_delivery.delivery_id,
        ),
        input_hashes=input_hashes,
        output_identities=output_identities,
    )
    failure_receipt = AlgoFailureReceiptV1.create(
        **provisional_failure.canonical_payload_v1(
            exclude={
                "schema_version",
                "failure_receipt_id",
                "bounded_context",
                "context_sha256",
                "transaction_commit_identity",
                "failure_receipt_sha256",
            }
        ),
        context=failure_context,
        transaction_commit_identity=tx_identity,
    )
    if previous_algo is None:
        traded_quantity = 0
        state_schema_version = state_json = state_sha256 = None
        last_applied_sequence = 0
        last_applied_id = None
        created_at = logical_time_utc
        row_version = 1
        archived_at = None
    else:
        traded_quantity = previous_algo.traded_quantity
        state_schema_version = previous_algo.state_schema_version
        state_json = None if previous_algo.state_json is None else thaw_json_v1(previous_algo.state_json)
        state_sha256 = previous_algo.state_sha256
        last_applied_sequence = previous_algo.last_applied_delivery_sequence
        last_applied_id = previous_algo.last_applied_delivery_id
        created_at = previous_algo.created_at_utc
        row_version = previous_algo.row_version + 1
        archived_at = previous_algo.archived_at_utc
    algo = ExecutionAlgoInstancePersistenceV2.create(
        algo_instance_id=predecessor_delivery.algo_instance_id,
        runtime_id=event.runtime_id,
        parent_intent_id=parent_intent_id,
        strategy_slot_id=strategy_slot_id,
        symbol=symbol,
        side=side,
        target_quantity=target_quantity,
        traded_quantity=traded_quantity,
        remaining_quantity=target_quantity - traded_quantity,
        algo_code=algo_code,
        plugin_id=plugin_id,
        plugin_version=plugin_version,
        plugin_manifest_sha256=plugin_manifest_sha256,
        plugin_config_json=plugin_config,
        plugin_config_sha256=plugin_config_sha256,
        compatibility_receipt_sha256=compatibility_receipt_sha256,
        state_schema_version=state_schema_version,
        state_json=state_json,
        state_sha256=state_sha256,
        transition_sequence=0 if initialization else transition_sequence,
        last_applied_delivery_sequence=last_applied_sequence,
        last_applied_delivery_id=last_applied_id,
        last_closed_delivery_sequence=predecessor_delivery.algo_delivery_sequence,
        terminal_delivery_sequence=predecessor_delivery.algo_delivery_sequence,
        status=ExecutionAlgoPersistenceStatusV2.FAILED,
        failure_receipt_id=failure_receipt.failure_receipt_id,
        active_child_closure_status=closure,
        active_child_count=active_child_count,
        row_version=row_version,
        created_at_utc=created_at,
        updated_at_utc=logical_time_utc,
        terminal_at_utc=logical_time_utc,
        archived_at_utc=archived_at,
    )
    delivery_payload = predecessor_delivery.model_dump(mode="python")
    delivery_payload.update(
        status=DeliveryStatusV1.FAILED_TERMINAL,
        lease_owner=None,
        lease_expires_at=None,
        lease_fence_token=None,
        transition_id=None,
        last_error_json=error_evidence.model_dump(mode="json"),
        next_attempt_at_utc=None,
        failure_receipt_id=failure_receipt.failure_receipt_id,
        skip_receipt_id=None,
        row_version=predecessor_delivery.row_version + 1,
        updated_at_utc=logical_time_utc,
        closed_at_utc=logical_time_utc,
    )
    delivery = AlgoDeliveryPersistenceV1.model_validate(delivery_payload)
    return KernelTransitionWriteBundleV1.create(
        algo_instance=algo,
        delivery=delivery,
        receipt=failure_receipt,
        projection_set=projection_set,
        after_state=None,
        new_child_mappings=(),
        command_outboxes=tuple(outboxes),
        updated_child_mappings=tuple(updated_mappings),
        updated_command_outboxes=tuple(updated_outboxes),
        timer_mutations=tuple(timer_mutations),
        timer_schedules=tuple(cancelled_schedules),
        diagnostic_observations=(),
    )


def materialize_skip_transition_v1(
    *,
    event: RuntimeEventEnvelopeV2,
    predecessor_delivery: AlgoDeliveryPersistenceV1,
    previous_algo: ExecutionAlgoInstancePersistenceV2,
    logical_time_utc: Any,
) -> KernelTransitionWriteBundleV1:
    if previous_algo.status is not ExecutionAlgoPersistenceStatusV2.FAILED or previous_algo.failure_receipt_id is None:
        raise KernelEffectMaterializationError(
            "MINIQMT_ALGO_SKIP_OWNER_INVALID",
            "only a durably FAILED algo can terminal-skip a raced later delivery",
            context={"algo_instance_id": previous_algo.algo_instance_id, "status": previous_algo.status.value},
        )
    provisional = AlgoSkipReceiptV1.create(
        delivery_id=predecessor_delivery.delivery_id,
        event_id=event.event_id,
        runtime_id=event.runtime_id,
        algo_instance_id=previous_algo.algo_instance_id,
        previous_delivery_id=predecessor_delivery.previous_delivery_id,
        terminal_failure_receipt_id=previous_algo.failure_receipt_id,
        logical_skipped_at_utc=logical_time_utc,
        transaction_commit_identity="mqtx_pending_skip",
    )
    tx_identity = transaction_commit_identity_v1(
        operation="APPLY_CLAIMED_DELIVERY_ATOMIC_SKIPPED_TERMINAL",
        owner_identities=(
            event.runtime_id,
            previous_algo.algo_instance_id,
            event.event_id,
            predecessor_delivery.delivery_id,
        ),
        input_hashes=(),
        output_identities=(provisional.skip_receipt_id,),
    )
    receipt = AlgoSkipReceiptV1.create(
        **provisional.canonical_payload_v1(
            exclude={"schema_version", "skip_receipt_id", "transaction_commit_identity", "skip_receipt_sha256"}
        ),
        transaction_commit_identity=tx_identity,
    )
    algo_payload = previous_algo.model_dump(mode="python")
    algo_payload.update(
        last_closed_delivery_sequence=predecessor_delivery.algo_delivery_sequence,
        row_version=previous_algo.row_version + 1,
        updated_at_utc=logical_time_utc,
    )
    algo = ExecutionAlgoInstancePersistenceV2.model_validate(algo_payload)
    delivery_payload = predecessor_delivery.model_dump(mode="python")
    delivery_payload.update(
        status=DeliveryStatusV1.SKIPPED_TERMINAL,
        lease_owner=None,
        lease_expires_at=None,
        lease_fence_token=None,
        transition_id=None,
        last_error_json=None,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=receipt.skip_receipt_id,
        row_version=predecessor_delivery.row_version + 1,
        updated_at_utc=logical_time_utc,
        closed_at_utc=logical_time_utc,
    )
    delivery = AlgoDeliveryPersistenceV1.model_validate(delivery_payload)
    return KernelTransitionWriteBundleV1.create(
        algo_instance=algo,
        delivery=delivery,
        receipt=receipt,
        projection_set=None,
        after_state=None,
    )


__all__ = [
    "KernelEffectMaterializationError",
    "materialize_applied_transition_v1",
    "materialize_failure_transition_v1",
    "materialize_skip_transition_v1",
]
