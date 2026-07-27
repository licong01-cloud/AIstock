"""Single carrier-to-scalar and immutable-retry projection authority for K2-A."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from .kernel_repository_common import KernelRepositoryConflict
from .plugin_canonical import canonical_utc_datetime_v1
from .plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoFailureReceiptV1,
    AlgoSkipReceiptV1,
    AlgoStateSnapshotV2,
    AlgoTransitionReceiptV1,
    BrokerCommandOutboxV1,
    BrokerDispatchAttemptV1,
    BrokerOutcomeReconciliationReceiptV1,
    ExchangeSessionAuthorityV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoTimerOccurrenceV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    KernelWorkerStartupReceiptV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
)


def _lease_owner_projection(lease_owner: str | None) -> tuple[str | None, str | None]:
    if lease_owner is None:
        return None, None
    worker_id, separator, process_incarnation_id = lease_owner.partition(":")
    if separator != ":" or not worker_id or not process_incarnation_id:
        raise ValueError("lease_owner must be worker_id:process_incarnation_id")
    return worker_id, process_incarnation_id


def _delivery_scalar_projection(delivery: AlgoDeliveryPersistenceV1) -> dict[str, Any]:
    values = delivery.model_dump(mode="json")
    lease_worker, lease_incarnation = _lease_owner_projection(delivery.lease_owner)
    return {
        "delivery_id": delivery.delivery_id,
        "event_id": delivery.event_id,
        "runtime_id": delivery.runtime_id,
        "algo_instance_id": delivery.algo_instance_id,
        "plugin_manifest_sha256": delivery.plugin_manifest_sha256,
        "algo_delivery_sequence": delivery.algo_delivery_sequence,
        "previous_delivery_sequence": None
        if delivery.previous_delivery_id is None
        else delivery.algo_delivery_sequence - 1,
        "previous_delivery_id": delivery.previous_delivery_id,
        "status": delivery.status.value,
        "attempt_count": delivery.attempt_count,
        "lease_owner": delivery.lease_owner,
        "lease_worker_id": lease_worker,
        "lease_process_incarnation_id": lease_incarnation,
        "lease_epoch": delivery.lease_epoch,
        "lease_fence_token": delivery.lease_fence_token,
        "lease_expires_at": delivery.lease_expires_at,
        "transition_id": delivery.transition_id,
        "last_error_json": values["last_error_json"],
        "next_attempt_at_utc": delivery.next_attempt_at_utc,
        "failure_receipt_id": delivery.failure_receipt_id,
        "skip_receipt_id": delivery.skip_receipt_id,
        "row_version": delivery.row_version,
        "created_at_utc": delivery.created_at_utc,
        "updated_at_utc": delivery.updated_at_utc,
        "closed_at_utc": delivery.closed_at_utc,
    }


def _outbox_scalar_projection(outbox: BrokerCommandOutboxV1) -> dict[str, Any]:
    values = outbox.model_dump(mode="json")
    lease_worker, lease_incarnation = _lease_owner_projection(outbox.lease_owner)
    return {
        "command_id": outbox.command_id,
        "transition_id": outbox.transition_id,
        "ordinal": outbox.ordinal,
        "runtime_id": outbox.runtime_id,
        "algo_instance_id": outbox.algo_instance_id,
        "parent_intent_id": outbox.parent_intent_id,
        "mapping_id": outbox.mapping_id,
        "command_type": outbox.command_type.value,
        "local_vt_orderid": outbox.local_vt_orderid,
        "payload_json": values["payload_json"],
        "payload_sha256": outbox.payload_sha256,
        "status": outbox.status.value,
        "attempt_count": outbox.attempt_count,
        "lease_owner": outbox.lease_owner,
        "lease_worker_id": lease_worker,
        "lease_process_incarnation_id": lease_incarnation,
        "lease_epoch": outbox.lease_epoch,
        "lease_fence_token": outbox.lease_fence_token,
        "lease_expires_at": outbox.lease_expires_at,
        "dispatch_attempt_id": outbox.dispatch_attempt_id,
        "callback_watermark_before_call": outbox.callback_watermark_before_call,
        "deterministic_client_order_ref": outbox.deterministic_client_order_ref,
        "next_attempt_at_utc": outbox.next_attempt_at_utc,
        "broker_called": outbox.broker_called,
        "broker_order_id": outbox.broker_order_id,
        "ack_receipt_json": values["ack_receipt_json"],
        "ack_receipt_sha256": outbox.ack_receipt_sha256,
        "non_acceptance_receipt_json": values["non_acceptance_receipt"],
        "unknown_outcome_receipt_json": values["unknown_outcome_receipt"],
        "reconcile_receipt_json": values["reconcile_receipt"],
        "last_error_json": values["last_error_json"],
        "row_version": outbox.row_version,
        "created_at_utc": outbox.created_at_utc,
        "updated_at_utc": outbox.updated_at_utc,
        "closed_at_utc": outbox.closed_at_utc,
        "outbox_row_sha256": outbox.outbox_row_sha256,
    }


def _algo_scalar_projection(algo: ExecutionAlgoInstancePersistenceV2) -> dict[str, Any]:
    values = algo.model_dump(mode="json")
    return {
        "algo_instance_id": algo.algo_instance_id,
        "runtime_id": algo.runtime_id,
        "parent_intent_id": algo.parent_intent_id,
        "strategy_slot_id": algo.strategy_slot_id,
        "symbol": algo.symbol,
        "side": algo.side.value,
        "target_quantity": algo.target_quantity,
        "traded_quantity": algo.traded_quantity,
        "remaining_quantity": algo.remaining_quantity,
        "algo_code": algo.algo_code,
        "status": algo.status.value,
        "archived_at": algo.archived_at_utc,
        "created_at": algo.created_at_utc,
        "updated_at": algo.updated_at_utc,
        "kernel_contract_version": algo.kernel_contract_version,
        "plugin_id": algo.plugin_id,
        "plugin_version": algo.plugin_version,
        "plugin_manifest_sha256": algo.plugin_manifest_sha256,
        "plugin_config_json": values["plugin_config_json"],
        "plugin_config_sha256": algo.plugin_config_sha256,
        "compatibility_receipt_sha256": algo.compatibility_receipt_sha256,
        "state_schema_version": algo.state_schema_version,
        "state_json": values["state_json"],
        "state_sha256": algo.state_sha256,
        "transition_sequence": algo.transition_sequence,
        "last_applied_delivery_sequence": algo.last_applied_delivery_sequence,
        "last_applied_delivery_id": algo.last_applied_delivery_id,
        "last_closed_delivery_sequence": algo.last_closed_delivery_sequence,
        "terminal_delivery_sequence": algo.terminal_delivery_sequence,
        "failure_receipt_id": algo.failure_receipt_id,
        "active_child_closure_status": algo.active_child_closure_status.value,
        "active_child_count": algo.active_child_count,
        "row_version": algo.row_version,
        "terminal_at_utc": algo.terminal_at_utc,
    }


def _worker_startup_scalar_projection(receipt: KernelWorkerStartupReceiptV1) -> dict[str, Any]:
    return {
        "worker_id": receipt.worker_id,
        "process_role": receipt.process_role,
        "incarnation_sequence": receipt.incarnation_sequence,
        "source_revision": receipt.source_revision,
        "process_incarnation_id": receipt.process_incarnation_id,
        "started_at_utc": receipt.started_at_utc,
        "startup_transaction_commit_identity": receipt.startup_transaction_commit_identity,
        "receipt_sha256": receipt.receipt_sha256,
    }


def _event_scalar_projection(event: RuntimeEventEnvelopeV2, receipt: RuntimeEventIngressReceiptV1) -> dict[str, Any]:
    values = event.model_dump(mode="json")
    return {
        "event_id": event.event_id,
        "runtime_id": event.runtime_id,
        "sequence": event.sequence,
        "event_type": event.event_type.value,
        "event_time": event.event_time_utc,
        "source": event.source.value,
        "payload": values,
        "event_contract_version": "KERNEL_V2",
        "event_schema_version": event.schema_version,
        "payload_schema_version": event.payload_schema_version,
        "event_key_sha256": event.event_key_sha256,
        "payload_sha256": event.payload_sha256,
        "observed_at_utc": event.event_time_utc,
        "logical_at_utc": event.event_time_utc,
        "source_identity_json": values["source_identity"],
        "correlation_json": values["correlation"],
        "ingress_receipt_json": receipt.model_dump(mode="json"),
        "ingress_receipt_sha256": receipt.receipt_sha256,
        "routing_rule_version": receipt.routing_rule_version,
        "transaction_commit_identity": receipt.transaction_commit_identity,
    }


def _transition_scalar_projection(
    *,
    receipt: AlgoTransitionReceiptV1 | AlgoFailureReceiptV1 | AlgoSkipReceiptV1,
    kind: str,
    transition_sequence: int,
    projection_set: ExecutionProjectionSetV1 | None,
    after_state: AlgoStateSnapshotV2 | None,
) -> dict[str, Any]:
    receipt_values = receipt.model_dump(mode="json")
    transition_id = (
        receipt.transition_id
        if isinstance(receipt, AlgoTransitionReceiptV1)
        else receipt.failure_receipt_id
        if isinstance(receipt, AlgoFailureReceiptV1)
        else receipt.skip_receipt_id
    )
    receipt_sha256 = (
        receipt.receipt_sha256
        if isinstance(receipt, AlgoTransitionReceiptV1)
        else receipt.failure_receipt_sha256
        if isinstance(receipt, AlgoFailureReceiptV1)
        else receipt.skip_receipt_sha256
    )
    return {
        "transition_id": transition_id,
        "delivery_id": receipt.delivery_id,
        "event_id": receipt.event_id,
        "runtime_id": receipt.runtime_id,
        "algo_instance_id": receipt.algo_instance_id,
        "transition_sequence": transition_sequence,
        "transition_kind": kind,
        "transition_receipt_json": receipt_values if kind == "APPLIED" else None,
        "failure_receipt_json": receipt_values if kind == "FAILED_TERMINAL" else None,
        "skip_receipt_json": receipt_values if kind == "SKIPPED_TERMINAL" else None,
        "receipt_sha256": receipt_sha256,
        "execution_projection_set_json": None if projection_set is None else projection_set.model_dump(mode="json"),
        "execution_projection_set_sha256": None if projection_set is None else projection_set.projection_set_sha256,
        "after_state_json": None if after_state is None else after_state.model_dump(mode="json"),
        "after_state_sha256": None if after_state is None else after_state.state_sha256,
        "transaction_commit_identity": receipt.transaction_commit_identity,
    }


def _mapping_scalar_projection(mapping: ExecutionCommandChildMappingV1, *, child_price_type: int = 2) -> dict[str, Any]:
    return {
        "child_order_id": mapping.child_order_id,
        "runtime_id": mapping.runtime_id,
        "algo_instance_id": mapping.algo_instance_id,
        "parent_intent_id": mapping.parent_intent_id,
        "strategy_slot_id": mapping.strategy_slot_id,
        "symbol": mapping.symbol,
        "side": mapping.side.value,
        "quantity": mapping.requested_quantity,
        "price": mapping.requested_price_decimal,
        "price_type": child_price_type,
        "status": "SUBMITTING",
        "broker_order_id": mapping.broker_order_id,
        "updated_at": mapping.updated_at_utc,
        "kernel_contract_version": "KERNEL_V2",
        "mapping_id": mapping.mapping_id,
        "command_id": mapping.command_id,
        "local_vt_orderid": mapping.local_vt_orderid,
        "deterministic_client_order_ref": mapping.deterministic_client_order_ref,
        "order_remark": mapping.order_remark,
        "mapping_status": mapping.mapping_status.value,
        "mapping_version": mapping.mapping_version,
        "mapping_payload_sha256": mapping.payload_sha256,
        "mapping_receipt_sha256": mapping.mapping_receipt_sha256,
        "broker_identity_source_event_id": mapping.broker_identity_source_event_id,
        "last_order_event_id": mapping.last_order_event_id,
        "last_trade_event_id": mapping.last_trade_event_id,
        "created_transition_id": mapping.created_transition_id,
        "updated_by_event_id": mapping.updated_by_event_id,
        "mapping_created_at_utc": mapping.created_at_utc,
        "mapping_updated_at_utc": mapping.updated_at_utc,
    }


def _dispatch_attempt_scalar_projection(attempt: BrokerDispatchAttemptV1) -> dict[str, Any]:
    return {
        "dispatch_attempt_id": attempt.dispatch_attempt_id,
        "stage": attempt.stage.value,
        "command_id": attempt.command_id,
        "attempt_count": attempt.attempt_count,
        "lease_epoch": attempt.lease_epoch,
        "lease_fence_token": attempt.lease_fence_token,
        "process_incarnation_id": attempt.process_incarnation_id,
        "started_at_utc": attempt.started_at_utc,
        "finished_at_utc": attempt.finished_at_utc,
        "pre_call_complete": attempt.pre_call_complete,
        "broker_called": attempt.broker_called,
        "outcome": attempt.outcome,
        "error_reason_code": attempt.error_reason_code,
        "error_context_sha256": attempt.error_context_sha256,
        "authority_receipt_sha256": attempt.authority_receipt_sha256,
        "attempt_receipt_sha256": attempt.attempt_receipt_sha256,
    }


def _reconciliation_receipt_scalar_projection(
    receipt: BrokerOutcomeReconciliationReceiptV1,
    *,
    runtime_id: str,
) -> dict[str, Any]:
    """One writer/readback scalar authority for immutable reconciliation history."""

    return {
        "receipt_sha256": receipt.receipt_sha256,
        "command_id": receipt.command_id,
        "runtime_id": runtime_id,
        "reconcile_attempt": receipt.reconcile_attempt,
        "callback_watermark": receipt.callback_watermark,
        "outcome": receipt.outcome.value,
        "observed_at_utc": receipt.observed_at_utc,
    }


def _timer_schedule_scalar_projection(schedule: ExecutionAlgoTimerScheduleV1) -> dict[str, Any]:
    lease_worker, lease_incarnation = _lease_owner_projection(schedule.lease_owner)
    return {
        "schedule_id": schedule.schedule_id,
        "runtime_id": schedule.runtime_id,
        "algo_instance_id": schedule.algo_instance_id,
        "timer_name": schedule.timer_name,
        "schedule_epoch": schedule.schedule_epoch,
        "due_at_exchange_utc": schedule.due_at_exchange_utc,
        "catch_up_policy": schedule.catch_up_policy,
        "payload_json": schedule.model_dump(mode="json")["payload"],
        "payload_sha256": schedule.payload_sha256,
        "status": schedule.status.value,
        "timer_occurrence_id": schedule.timer_occurrence_id,
        "emitted_event_id": schedule.emitted_event_id,
        "lease_owner": schedule.lease_owner,
        "lease_worker_id": lease_worker,
        "lease_process_incarnation_id": lease_incarnation,
        "lease_epoch": schedule.lease_epoch,
        "lease_fence_token": schedule.lease_fence_token,
        "lease_expires_at_utc": schedule.lease_expires_at_utc,
        "row_version": schedule.row_version,
        "created_at_utc": schedule.created_at_utc,
        "updated_at_utc": schedule.updated_at_utc,
        "closed_at_utc": schedule.closed_at_utc,
        "schedule_receipt_sha256": schedule.schedule_receipt_sha256,
    }


def _timer_occurrence_scalar_projection(occurrence: ExecutionAlgoTimerOccurrenceV1) -> dict[str, Any]:
    lease_worker, lease_incarnation = _lease_owner_projection(occurrence.lease_owner)
    return {
        "timer_occurrence_id": occurrence.timer_occurrence_id,
        "schedule_id": occurrence.schedule_id,
        "runtime_id": occurrence.runtime_id,
        "algo_instance_id": occurrence.algo_instance_id,
        "due_at_exchange_utc": occurrence.due_at_exchange_utc,
        "exchange_session_authority_sha256": occurrence.exchange_session_authority_sha256,
        "status": occurrence.status.value,
        "emitted_event_id": occurrence.emitted_event_id,
        "catch_up_receipt_sha256": occurrence.catch_up_receipt_sha256,
        "lease_owner": occurrence.lease_owner,
        "lease_worker_id": lease_worker,
        "lease_process_incarnation_id": lease_incarnation,
        "lease_epoch": occurrence.lease_epoch,
        "lease_fence_token": occurrence.lease_fence_token,
        "lease_expires_at_utc": occurrence.lease_expires_at_utc,
        "row_version": occurrence.row_version,
        "created_at_utc": occurrence.created_at_utc,
        "closed_at_utc": occurrence.closed_at_utc,
        "occurrence_receipt_sha256": occurrence.occurrence_receipt_sha256,
    }


def _exchange_session_scalar_projection(authority: ExchangeSessionAuthorityV1) -> dict[str, Any]:
    return {
        "runtime_id": authority.runtime_id,
        "exchange_trade_date": date.fromisoformat(authority.exchange_trade_date),
        "calendar_snapshot_set_id": authority.calendar_snapshot_set_id,
        "calendar_snapshot_set_sha256": authority.calendar_snapshot_set_sha256,
        "session_definition_version": authority.session_definition_version,
        "authority_sha256": authority.authority_sha256,
    }


def _assert_scalar_columns(row: Any, expected: dict[str, Any], *, carrier_name: str) -> None:
    def normalized(value: Any) -> Any:
        if isinstance(value, datetime):
            return canonical_utc_datetime_v1(value, field_name="durable_scalar")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return format(value, "f")
        return value

    def equal(actual: Any, expected_value: Any) -> bool:
        if isinstance(actual, Decimal) or isinstance(expected_value, Decimal):
            try:
                return Decimal(str(actual)) == Decimal(str(expected_value))
            except (InvalidOperation, ValueError):
                return False
        return normalized(actual) == normalized(expected_value)

    mismatches = {
        key: {"expected": normalized(expected_value), "actual": normalized(row[key])}
        for key, expected_value in expected.items()
        if not equal(row[key], expected_value)
    }
    if mismatches:
        raise KernelRepositoryConflict(f"{carrier_name} scalar columns drift from strict carrier: {mismatches}")


def _delivery_creation_matches(
    current: AlgoDeliveryPersistenceV1,
    initial: AlgoDeliveryPersistenceV1,
) -> bool:
    fields = (
        "delivery_id",
        "event_id",
        "runtime_id",
        "algo_instance_id",
        "plugin_manifest_sha256",
        "algo_delivery_sequence",
        "previous_delivery_id",
        "created_at_utc",
    )
    return all(getattr(current, field) == getattr(initial, field) for field in fields)


def _mapping_creation_matches(
    current: ExecutionCommandChildMappingV1,
    initial: ExecutionCommandChildMappingV1,
) -> bool:
    fields = (
        "mapping_id",
        "command_id",
        "runtime_id",
        "algo_instance_id",
        "parent_intent_id",
        "strategy_slot_id",
        "local_vt_orderid",
        "child_order_id",
        "deterministic_client_order_ref",
        "order_remark",
        "payload_sha256",
        "created_transition_id",
        "created_at_utc",
    )
    return all(getattr(current, field) == getattr(initial, field) for field in fields)


def _outbox_creation_matches(current: BrokerCommandOutboxV1, initial: BrokerCommandOutboxV1) -> bool:
    fields = (
        "command_id",
        "transition_id",
        "ordinal",
        "runtime_id",
        "algo_instance_id",
        "parent_intent_id",
        "mapping_id",
        "command_type",
        "local_vt_orderid",
        "payload_sha256",
        "deterministic_client_order_ref",
        "created_at_utc",
    )
    return all(getattr(current, field) == getattr(initial, field) for field in fields)


def _transition_retry_matches(
    readback: dict[str, Any],
    *,
    receipt: AlgoTransitionReceiptV1 | AlgoFailureReceiptV1 | AlgoSkipReceiptV1,
    projection_set: ExecutionProjectionSetV1 | None,
    after_state: AlgoStateSnapshotV2 | None,
    mappings: Sequence[ExecutionCommandChildMappingV1],
    outboxes: Sequence[BrokerCommandOutboxV1],
) -> bool:
    if (
        readback["receipt"] != receipt
        or readback["projection_set"] != projection_set
        or readback["after_state"] != after_state
    ):
        return False
    current_mappings = {item.mapping_id: item for item in readback["new_child_mappings"]}
    initial_mappings = {item.mapping_id: item for item in mappings}
    if current_mappings.keys() != initial_mappings.keys() or any(
        not _mapping_creation_matches(current_mappings[identity], initial)
        for identity, initial in initial_mappings.items()
    ):
        return False
    current_outboxes = {item.command_id: item for item in readback["command_outboxes"]}
    initial_outboxes = {item.command_id: item for item in outboxes}
    return current_outboxes.keys() == initial_outboxes.keys() and all(
        _outbox_creation_matches(current_outboxes[identity], initial) for identity, initial in initial_outboxes.items()
    )
