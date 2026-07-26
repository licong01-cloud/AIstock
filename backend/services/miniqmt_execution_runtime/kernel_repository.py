"""PostgreSQL-only durable repository for MiniQMT Execution Kernel K2-A."""

from __future__ import annotations

import inspect
import json
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterator, Sequence

import psycopg2
import psycopg2.extras

from backend.db.pg_pool import get_conn

from .plugin_canonical import canonical_utc_datetime_v1
from .plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoFailureReceiptV1,
    AlgoSkipReceiptV1,
    AlgoStateSnapshotV2,
    AlgoTransitionReceiptV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerDispatchAttemptV1,
    CommandChildMappingStatusV1,
    DeliveryStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionAlgoTimerOccurrenceStatusV1,
    ExecutionAlgoTimerOccurrenceV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    ExchangeSessionAuthorityV1,
    KernelWorkerStartupReceiptV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
    kernel_lease_fence_token_v1,
    transaction_commit_identity_v1,
)


class KernelRepositoryConflict(RuntimeError):
    """A durable identity or CAS version conflicts with persisted facts."""


class KernelRepositorySchemaError(RuntimeError):
    """The K2 schema is absent or only partially installed."""


class KernelRepositoryCommitUnknown(RuntimeError):
    """The database may have committed, but the transaction return was not observed."""


_K2_SCHEMA_CATALOG_SHA256 = "6e4fc4ae4c6e403d3316c124da6ae5933eb33184129569fd6bf1cf750e27f762"

_K2_CATALOG_QUERY = """
WITH target_tables(relname) AS (
    VALUES
        ('execution_kernel_worker_epoch'),
        ('execution_kernel_worker_incarnation'),
        ('execution_algo_event_delivery'),
        ('execution_algo_transition'),
        ('execution_algo_command_outbox'),
        ('execution_algo_command_dispatch_attempt'),
        ('execution_algo_timer_schedule'),
        ('execution_algo_timer_occurrence'),
        ('execution_exchange_session_authority'),
        ('execution_algo_diagnostic_observation')
), additive_columns(relname,attname) AS (
    VALUES
        ('execution_runtime','runtime_id'),
        ('execution_runtime','trade_date'),
        ('execution_runtime_event','event_contract_version'),
        ('execution_runtime_event','event_schema_version'),
        ('execution_runtime_event','payload_schema_version'),
        ('execution_runtime_event','event_key_sha256'),
        ('execution_runtime_event','payload_sha256'),
        ('execution_runtime_event','observed_at_utc'),
        ('execution_runtime_event','logical_at_utc'),
        ('execution_runtime_event','source_identity_json'),
        ('execution_runtime_event','correlation_json'),
        ('execution_runtime_event','ingress_receipt_json'),
        ('execution_runtime_event','ingress_receipt_sha256'),
        ('execution_runtime_event','routing_rule_version'),
        ('execution_runtime_event','transaction_commit_identity'),
        ('execution_algo_instance','kernel_contract_version'),
        ('execution_algo_instance','traded_quantity'),
        ('execution_algo_instance','plugin_id'),
        ('execution_algo_instance','plugin_version'),
        ('execution_algo_instance','plugin_manifest_sha256'),
        ('execution_algo_instance','plugin_config_json'),
        ('execution_algo_instance','plugin_config_sha256'),
        ('execution_algo_instance','compatibility_receipt_sha256'),
        ('execution_algo_instance','state_schema_version'),
        ('execution_algo_instance','state_json'),
        ('execution_algo_instance','state_sha256'),
        ('execution_algo_instance','transition_sequence'),
        ('execution_algo_instance','last_applied_delivery_sequence'),
        ('execution_algo_instance','last_applied_delivery_id'),
        ('execution_algo_instance','last_closed_delivery_sequence'),
        ('execution_algo_instance','terminal_delivery_sequence'),
        ('execution_algo_instance','failure_receipt_id'),
        ('execution_algo_instance','active_child_closure_status'),
        ('execution_algo_instance','active_child_count'),
        ('execution_algo_instance','row_version'),
        ('execution_algo_instance','terminal_at_utc'),
        ('execution_algo_instance','kernel_carrier_json'),
        ('execution_child_order','kernel_contract_version'),
        ('execution_child_order','mapping_id'),
        ('execution_child_order','command_id'),
        ('execution_child_order','local_vt_orderid'),
        ('execution_child_order','deterministic_client_order_ref'),
        ('execution_child_order','order_remark'),
        ('execution_child_order','mapping_status'),
        ('execution_child_order','mapping_version'),
        ('execution_child_order','mapping_payload_sha256'),
        ('execution_child_order','mapping_receipt_sha256'),
        ('execution_child_order','broker_identity_source_event_id'),
        ('execution_child_order','last_order_event_id'),
        ('execution_child_order','last_trade_event_id'),
        ('execution_child_order','created_transition_id'),
        ('execution_child_order','updated_by_event_id'),
        ('execution_child_order','mapping_created_at_utc'),
        ('execution_child_order','mapping_updated_at_utc'),
        ('execution_child_order','mapping_json')
), catalog_items(sort_key,item) AS (
    SELECT
        format('column:%s:%05s', table_class.relname, attribute.attnum),
        jsonb_build_array(
            'column', table_class.relname, attribute.attname,
            format_type(attribute.atttypid, attribute.atttypmod),
            attribute.attnotnull,
            coalesce(pg_get_expr(attribute_default.adbin, attribute_default.adrelid), '')
        )
    FROM pg_class AS table_class
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    JOIN pg_attribute AS attribute
      ON attribute.attrelid=table_class.oid AND attribute.attnum > 0 AND NOT attribute.attisdropped
    LEFT JOIN pg_attrdef AS attribute_default
      ON attribute_default.adrelid=table_class.oid AND attribute_default.adnum=attribute.attnum
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR (table_class.relname,attribute.attname) IN (SELECT relname,attname FROM additive_columns)
      )

    UNION ALL

    SELECT
        format('constraint:%s:%s', table_class.relname, constraint_record.conname),
        jsonb_build_array(
            'constraint', table_class.relname, constraint_record.conname,
            constraint_record.contype, constraint_record.condeferrable,
            constraint_record.condeferred, constraint_record.convalidated,
            replace(
                pg_get_constraintdef(constraint_record.oid, true),
                table_schema.nspname || '.', '<schema>.'
            )
        )
    FROM pg_constraint AS constraint_record
    JOIN pg_class AS table_class ON table_class.oid=constraint_record.conrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR constraint_record.conname LIKE '%miniqmt_k2%'
      )

    UNION ALL

    SELECT
        format('index:%s:%s', table_class.relname, index_class.relname),
        jsonb_build_array(
            'index', table_class.relname, index_class.relname,
            index_record.indisunique, index_record.indisprimary,
            index_record.indisvalid, index_record.indisready,
            replace(
                pg_get_indexdef(index_record.indexrelid, 0, true),
                table_schema.nspname || '.', '<schema>.'
            ),
            coalesce(
                replace(
                    pg_get_expr(index_record.indpred, index_record.indrelid, true),
                    table_schema.nspname || '.', '<schema>.'
                ),
                ''
            )
        )
    FROM pg_index AS index_record
    JOIN pg_class AS table_class ON table_class.oid=index_record.indrelid
    JOIN pg_class AS index_class ON index_class.oid=index_record.indexrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR index_class.relname LIKE '%miniqmt_k2%'
      )
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key), '[]'::jsonb)::TEXT AS payload
    FROM catalog_items
)
SELECT encode(sha256(convert_to(payload, 'UTF8')), 'hex')
FROM canonical_catalog
""".strip()


def _accepts_keyword(factory: Any, keyword: str) -> bool:
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return False
    return keyword in signature.parameters or any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
    )


def _json(value: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(value)


def _row_json(row: Any, key: str) -> dict[str, Any]:
    value = row[key]
    if not isinstance(value, dict):
        raise KernelRepositoryConflict(f"durable {key} is not a JSON object")
    return value


def _model_from_json(model: Any, value: dict[str, Any]) -> Any:
    return model.model_validate_json(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _bounded_limit(limit: int) -> int:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("limit must be a strict integer in [1, 1000]")
    return limit


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


class PostgresMiniQMTKernelRepository:
    """Strict K2 persistence. It has no in-memory, plugin, OMS, or Gateway fallback."""

    def __init__(self, conn_factory: Callable[..., Any] = get_conn) -> None:
        self._conn_factory = conn_factory
        self._accepts_autocommit = _accepts_keyword(conn_factory, "autocommit")
        self._accepts_manage_transaction = _accepts_keyword(conn_factory, "manage_transaction")

    @contextmanager
    def _connection(self, *, transaction: bool) -> Iterator[Any]:
        kwargs: dict[str, Any] = {}
        if self._accepts_autocommit:
            kwargs["autocommit"] = not transaction
        if self._accepts_manage_transaction:
            kwargs["manage_transaction"] = transaction
        with self._conn_factory(**kwargs) as conn:
            yield conn

    def preflight_schema(self) -> dict[str, bool]:
        required = (
            "execution_algo_event_delivery",
            "execution_algo_transition",
            "execution_algo_command_outbox",
            "execution_algo_command_dispatch_attempt",
            "execution_algo_timer_schedule",
            "execution_algo_timer_occurrence",
            "execution_kernel_worker_epoch",
            "execution_kernel_worker_incarnation",
            "execution_exchange_session_authority",
            "execution_algo_diagnostic_observation",
        )
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT relname, to_regclass('qmt_strategy.' || relname) IS NOT NULL AS present
                    FROM unnest(%s::text[]) AS relname
                    ORDER BY relname
                    """,
                    (list(required),),
                )
                result = {str(row["relname"]): bool(row["present"]) for row in cur.fetchall()}
        if not all(result.values()):
            raise KernelRepositorySchemaError(f"K2 schema is incomplete: {result}")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                try:
                    cur.execute(
                        """
                        SELECT schema_name,language_name,volatility,arguments,result_type,function_body
                        FROM (
                            SELECT namespace.nspname AS schema_name,language.lanname AS language_name,
                                   function_record.provolatile AS volatility,
                                   pg_get_function_arguments(function_record.oid) AS arguments,
                                   pg_get_function_result(function_record.oid) AS result_type,
                                   function_record.prosrc AS function_body
                            FROM pg_proc AS function_record
                            JOIN pg_namespace AS namespace ON namespace.oid=function_record.pronamespace
                            JOIN pg_language AS language ON language.oid=function_record.prolang
                            WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()')
                        ) AS function_authority
                        """
                    )
                    function_row = cur.fetchone()
                    if function_row is None:
                        raise KernelRepositorySchemaError("K2 schema fingerprint authority is unavailable")
                    normalized_body = (
                        str(function_row["function_body"])
                        .replace(str(function_row["schema_name"]), "qmt_strategy")
                        .strip()
                        .rstrip(";")
                    )
                    if (
                        function_row["language_name"] != "sql"
                        or function_row["volatility"] != "s"
                        or function_row["arguments"] != ""
                        or function_row["result_type"] != "text"
                        or normalized_body != _K2_CATALOG_QUERY.strip().rstrip(";")
                    ):
                        raise KernelRepositorySchemaError("K2 catalog function drift")
                    cur.execute(f"SELECT * FROM ({_K2_CATALOG_QUERY}) AS catalog(catalog_sha256)")
                    catalog_sha256 = str(cur.fetchone()["catalog_sha256"])
                except KernelRepositorySchemaError:
                    raise
                except psycopg2.Error as exc:
                    raise KernelRepositorySchemaError("K2 schema fingerprint authority is unavailable") from exc
        if catalog_sha256 != _K2_SCHEMA_CATALOG_SHA256:
            raise KernelRepositorySchemaError(
                f"K2 schema catalog drift: expected {_K2_SCHEMA_CATALOG_SHA256}, got {catalog_sha256}"
            )
        result["schema_catalog_fingerprint"] = True
        return result

    def read_delivery(self, delivery_id: str) -> AlgoDeliveryPersistenceV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT delivery_id,event_id,runtime_id,algo_instance_id,plugin_manifest_sha256,
                           algo_delivery_sequence,previous_delivery_sequence,previous_delivery_id,status,
                           attempt_count,lease_owner,lease_worker_id,lease_process_incarnation_id,
                           lease_epoch,lease_fence_token,lease_expires_at,transition_id,last_error_json,
                           next_attempt_at_utc,failure_receipt_id,skip_receipt_id,row_version,
                           created_at_utc,updated_at_utc,closed_at_utc,carrier_json
                    FROM qmt_strategy.execution_algo_event_delivery WHERE delivery_id=%s
                    """,
                    (delivery_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(delivery_id)
        delivery = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(row, "carrier_json"))
        _assert_scalar_columns(
            row,
            _delivery_scalar_projection(delivery),
            carrier_name="delivery",
        )
        return delivery

    def read_outbox_command(self, command_id: str) -> BrokerCommandOutboxV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT command_id,transition_id,ordinal,runtime_id,algo_instance_id,parent_intent_id,
                           mapping_id,command_type,local_vt_orderid,payload_json,payload_sha256,status,
                           attempt_count,lease_owner,lease_worker_id,lease_process_incarnation_id,
                           lease_epoch,lease_fence_token,lease_expires_at,dispatch_attempt_id,
                           deterministic_client_order_ref,next_attempt_at_utc,broker_called,broker_order_id,
                           ack_receipt_json,ack_receipt_sha256,non_acceptance_receipt_json,
                           unknown_outcome_receipt_json,reconcile_receipt_json,last_error_json,row_version,
                           created_at_utc,updated_at_utc,closed_at_utc,outbox_row_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_command_outbox WHERE command_id=%s
                    """,
                    (command_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(command_id)
        outbox = _model_from_json(BrokerCommandOutboxV1, _row_json(row, "carrier_json"))
        _assert_scalar_columns(
            row,
            _outbox_scalar_projection(outbox),
            carrier_name="outbox",
        )
        return outbox

    def read_algo_instance(self, algo_instance_id: str) -> ExecutionAlgoInstancePersistenceV2:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT algo.algo_instance_id,algo.runtime_id,algo.parent_intent_id,algo.strategy_slot_id,
                           algo.symbol,algo.side,algo.target_quantity,algo.traded_quantity,
                           algo.remaining_quantity,algo.algo_code,algo.status,algo.archived_at,algo.created_at,
                           algo.updated_at,algo.kernel_contract_version,algo.plugin_id,algo.plugin_version,
                           algo.plugin_manifest_sha256,algo.plugin_config_json,algo.plugin_config_sha256,
                           algo.compatibility_receipt_sha256,algo.state_schema_version,algo.state_json,
                           algo.state_sha256,algo.transition_sequence,algo.last_applied_delivery_sequence,
                           algo.last_applied_delivery_id,algo.last_closed_delivery_sequence,
                           algo.terminal_delivery_sequence,algo.failure_receipt_id,
                           algo.active_child_closure_status,algo.active_child_count,algo.row_version,
                           algo.terminal_at_utc,algo.kernel_carrier_json,
                           COUNT(child.child_order_id) FILTER (
                               WHERE child.kernel_contract_version='KERNEL_V2'
                                 AND child.mapping_status IN ('RESERVED','DISPATCHING','BROKER_ACCEPTED','OUTCOME_UNKNOWN')
                           ) AS reconstructed_active_child_count
                    FROM qmt_strategy.execution_algo_instance AS algo
                    LEFT JOIN qmt_strategy.execution_child_order AS child
                      ON child.runtime_id=algo.runtime_id AND child.algo_instance_id=algo.algo_instance_id
                    WHERE algo.algo_instance_id=%s AND algo.kernel_contract_version='KERNEL_V2'
                    GROUP BY algo.algo_instance_id
                    """,
                    (algo_instance_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(algo_instance_id)
        algo = _model_from_json(ExecutionAlgoInstancePersistenceV2, _row_json(row, "kernel_carrier_json"))
        _assert_scalar_columns(
            row,
            _algo_scalar_projection(algo),
            carrier_name="algo",
        )
        if algo.active_child_count != row["reconstructed_active_child_count"]:
            raise KernelRepositoryConflict("algo carrier does not close to durable active-child reconstruction")
        return algo

    def start_worker_incarnation(
        self,
        *,
        worker_id: str,
        process_role: str,
        source_revision: str,
        started_at_utc: Any,
    ) -> KernelWorkerStartupReceiptV1:
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_kernel_worker_epoch(worker_id, process_role)
                    VALUES (%s, %s)
                    ON CONFLICT (worker_id, process_role) DO NOTHING
                    """,
                    (worker_id, process_role),
                )
                cur.execute(
                    """
                    SELECT incarnation_sequence
                    FROM qmt_strategy.execution_kernel_worker_epoch
                    WHERE worker_id = %s AND process_role = %s
                    FOR UPDATE
                    """,
                    (worker_id, process_role),
                )
                row = cur.fetchone()
                if row is None:
                    raise KernelRepositoryConflict("worker epoch row disappeared while locked")
                sequence = int(row["incarnation_sequence"]) + 1
                provisional = KernelWorkerStartupReceiptV1.create(
                    worker_id=worker_id,
                    process_role=process_role,
                    incarnation_sequence=sequence,
                    source_revision=source_revision,
                    started_at_utc=started_at_utc,
                    startup_transaction_commit_identity="mqtx_pending_worker_startup",
                )
                transaction_id = transaction_commit_identity_v1(
                    operation="START_WORKER_INCARNATION",
                    owner_identities=(worker_id, process_role),
                    input_hashes=(),
                    output_identities=(provisional.process_incarnation_id,),
                )
                receipt = KernelWorkerStartupReceiptV1.create(
                    worker_id=worker_id,
                    process_role=process_role,
                    incarnation_sequence=sequence,
                    source_revision=source_revision,
                    started_at_utc=started_at_utc,
                    startup_transaction_commit_identity=transaction_id,
                )
                cur.execute(
                    """
                    UPDATE qmt_strategy.execution_kernel_worker_epoch
                    SET incarnation_sequence = %s, updated_at_utc = %s
                    WHERE worker_id = %s AND process_role = %s AND incarnation_sequence = %s
                    """,
                    (sequence, receipt.started_at_utc, worker_id, process_role, sequence - 1),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("worker epoch CAS failed")
                receipt_projection = _worker_startup_scalar_projection(receipt)
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_kernel_worker_incarnation(
                        worker_id, process_role, incarnation_sequence, source_revision,
                        process_incarnation_id, started_at_utc, startup_transaction_commit_identity,
                        receipt_sha256, startup_receipt_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        receipt_projection["worker_id"],
                        receipt_projection["process_role"],
                        receipt_projection["incarnation_sequence"],
                        receipt_projection["source_revision"],
                        receipt_projection["process_incarnation_id"],
                        receipt_projection["started_at_utc"],
                        receipt_projection["startup_transaction_commit_identity"],
                        receipt_projection["receipt_sha256"],
                        _json(receipt.model_dump(mode="json")),
                    ),
                )
        return self.read_worker_startup_receipt(receipt.process_incarnation_id)

    def read_worker_startup_receipt(self, process_incarnation_id: str) -> KernelWorkerStartupReceiptV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT worker_id,process_role,incarnation_sequence,source_revision,
                           process_incarnation_id,started_at_utc,startup_transaction_commit_identity,
                           receipt_sha256,startup_receipt_json
                    FROM qmt_strategy.execution_kernel_worker_incarnation
                    WHERE process_incarnation_id = %s
                    """,
                    (process_incarnation_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(process_incarnation_id)
        receipt = _model_from_json(KernelWorkerStartupReceiptV1, _row_json(row, "startup_receipt_json"))
        _assert_scalar_columns(
            row,
            _worker_startup_scalar_projection(receipt),
            carrier_name="worker startup receipt",
        )
        return receipt

    def write_event_receipt_deliveries(
        self,
        *,
        event: RuntimeEventEnvelopeV2,
        deliveries: Sequence[AlgoDeliveryPersistenceV1],
    ) -> RuntimeEventIngressReceiptV1:
        if not isinstance(event, RuntimeEventEnvelopeV2):
            raise TypeError("event must be RuntimeEventEnvelopeV2")
        strict_deliveries = tuple(deliveries)
        if any(not isinstance(item, AlgoDeliveryPersistenceV1) for item in strict_deliveries):
            raise TypeError("deliveries must contain only AlgoDeliveryPersistenceV1")
        if any(item.event_id != event.event_id or item.runtime_id != event.runtime_id for item in strict_deliveries):
            raise ValueError("delivery owner conflicts with event")
        for delivery in strict_deliveries:
            try:
                delivery.validate_initial_v1()
            except ValueError as exc:
                raise ValueError("first write requires initial PENDING delivery") from exc
        ordered = tuple(sorted(strict_deliveries, key=lambda item: item.algo_instance_id))
        targets = tuple(item.algo_instance_id for item in ordered)
        delivery_ids = tuple(item.delivery_id for item in ordered)
        provisional = RuntimeEventIngressReceiptV1.create(
            runtime_id=event.runtime_id,
            event_id=event.event_id,
            event_key_sha256=event.event_key_sha256,
            runtime_sequence=event.sequence,
            ordered_target_algo_instance_ids=targets,
            ordered_delivery_ids=delivery_ids,
            transaction_commit_identity="mqtx_pending_event_write",
        )
        transaction_id = transaction_commit_identity_v1(
            operation="WRITE_EVENT_RECEIPT_DELIVERIES",
            owner_identities=(event.runtime_id,),
            input_hashes=(event.event_key_sha256, event.payload_sha256),
            output_identities=(event.event_id, provisional.ingress_receipt_id, *delivery_ids),
        )
        receipt = RuntimeEventIngressReceiptV1.create(
            runtime_id=event.runtime_id,
            event_id=event.event_id,
            event_key_sha256=event.event_key_sha256,
            runtime_sequence=event.sequence,
            ordered_target_algo_instance_ids=targets,
            ordered_delivery_ids=delivery_ids,
            transaction_commit_identity=transaction_id,
        )
        event_projection = _event_scalar_projection(event, receipt)
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_runtime_event(
                        event_id, runtime_id, sequence, event_type, event_time, source, payload,
                        event_contract_version, event_schema_version, payload_schema_version, event_key_sha256, payload_sha256,
                        observed_at_utc, logical_at_utc, source_identity_json, correlation_json,
                        ingress_receipt_json, ingress_receipt_sha256, routing_rule_version,
                        transaction_commit_identity
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,'KERNEL_V2',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        event_projection["event_id"],
                        event_projection["runtime_id"],
                        event_projection["sequence"],
                        event_projection["event_type"],
                        event_projection["event_time"],
                        event_projection["source"],
                        _json(event_projection["payload"]),
                        event_projection["event_schema_version"],
                        event_projection["payload_schema_version"],
                        event_projection["event_key_sha256"],
                        event_projection["payload_sha256"],
                        event_projection["observed_at_utc"],
                        event_projection["logical_at_utc"],
                        _json(event_projection["source_identity_json"]),
                        _json(event_projection["correlation_json"]),
                        _json(event_projection["ingress_receipt_json"]),
                        event_projection["ingress_receipt_sha256"],
                        event_projection["routing_rule_version"],
                        event_projection["transaction_commit_identity"],
                    ),
                )
                event_was_inserted = cur.rowcount == 1
                if event_was_inserted:
                    for delivery in ordered:
                        delivery_projection = _delivery_scalar_projection(delivery)
                        cur.execute(
                            """
                        INSERT INTO qmt_strategy.execution_algo_event_delivery(
                            delivery_id,event_id,runtime_id,algo_instance_id,plugin_manifest_sha256,
                            algo_delivery_sequence,previous_delivery_sequence,previous_delivery_id,status,
                            attempt_count,lease_owner,lease_worker_id,lease_process_incarnation_id,
                            lease_epoch,lease_fence_token,lease_expires_at,
                            transition_id,last_error_json,next_attempt_at_utc,failure_receipt_id,skip_receipt_id,
                            row_version,created_at_utc,updated_at_utc,closed_at_utc,carrier_json
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (delivery_id) DO NOTHING
                        """,
                            (
                                delivery_projection["delivery_id"],
                                delivery_projection["event_id"],
                                delivery_projection["runtime_id"],
                                delivery_projection["algo_instance_id"],
                                delivery_projection["plugin_manifest_sha256"],
                                delivery_projection["algo_delivery_sequence"],
                                delivery_projection["previous_delivery_sequence"],
                                delivery_projection["previous_delivery_id"],
                                delivery_projection["status"],
                                delivery_projection["attempt_count"],
                                delivery_projection["lease_owner"],
                                delivery_projection["lease_worker_id"],
                                delivery_projection["lease_process_incarnation_id"],
                                delivery_projection["lease_epoch"],
                                delivery_projection["lease_fence_token"],
                                delivery_projection["lease_expires_at"],
                                delivery_projection["transition_id"],
                                None
                                if delivery_projection["last_error_json"] is None
                                else _json(delivery_projection["last_error_json"]),
                                delivery_projection["next_attempt_at_utc"],
                                delivery_projection["failure_receipt_id"],
                                delivery_projection["skip_receipt_id"],
                                delivery_projection["row_version"],
                                delivery_projection["created_at_utc"],
                                delivery_projection["updated_at_utc"],
                                delivery_projection["closed_at_utc"],
                                _json(delivery.model_dump(mode="json")),
                            ),
                        )
                cur.execute(
                    "SELECT payload, ingress_receipt_json FROM qmt_strategy.execution_runtime_event WHERE event_id=%s",
                    (event.event_id,),
                )
                event_row = cur.fetchone()
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery WHERE event_id=%s ORDER BY algo_instance_id",
                    (event.event_id,),
                )
                delivery_rows = cur.fetchall()
                in_transaction_event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(event_row, "payload"))
                in_transaction_receipt = _model_from_json(
                    RuntimeEventIngressReceiptV1, _row_json(event_row, "ingress_receipt_json")
                )
                in_transaction_deliveries = tuple(
                    _model_from_json(AlgoDeliveryPersistenceV1, _row_json(row, "carrier_json")) for row in delivery_rows
                )
                if (
                    in_transaction_event != event
                    or in_transaction_receipt != receipt
                    or len(in_transaction_deliveries) != len(ordered)
                    or any(
                        not _delivery_creation_matches(current, initial)
                        for current, initial in zip(in_transaction_deliveries, ordered, strict=True)
                    )
                ):
                    raise KernelRepositoryConflict("event identity exists with different immutable transaction payload")
        readback = self.read_event_transaction(event.event_id)
        if (
            readback["event"] != event
            or readback["receipt"] != receipt
            or len(readback["deliveries"]) != len(ordered)
            or any(
                not _delivery_creation_matches(current, initial)
                for current, initial in zip(readback["deliveries"], ordered, strict=True)
            )
        ):
            raise KernelRepositoryConflict("event transaction readback closure differs from writer payload")
        return receipt

    def read_event_transaction(self, event_id: str) -> dict[str, Any]:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT event_id,runtime_id,sequence,event_type,event_time,source,payload,
                           event_contract_version,event_schema_version,payload_schema_version,
                           event_key_sha256,payload_sha256,observed_at_utc,logical_at_utc,
                           source_identity_json,correlation_json,ingress_receipt_json,
                           ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                    FROM qmt_strategy.execution_runtime_event
                    WHERE event_id = %s AND event_contract_version = 'KERNEL_V2'
                    """,
                    (event_id,),
                )
                event_row = cur.fetchone()
                if event_row is None:
                    raise KeyError(event_id)
                cur.execute(
                    """
                    SELECT delivery_id
                    FROM qmt_strategy.execution_algo_event_delivery
                    WHERE event_id = %s
                    ORDER BY algo_instance_id
                    """,
                    (event_id,),
                )
                delivery_ids = tuple(str(row["delivery_id"]) for row in cur.fetchall())
        event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(event_row, "payload"))
        receipt = _model_from_json(RuntimeEventIngressReceiptV1, _row_json(event_row, "ingress_receipt_json"))
        _assert_scalar_columns(
            event_row,
            _event_scalar_projection(event, receipt),
            carrier_name="event",
        )
        deliveries = tuple(self.read_delivery(delivery_id) for delivery_id in delivery_ids)
        if receipt.ordered_delivery_ids != tuple(item.delivery_id for item in deliveries):
            raise KernelRepositoryConflict("event receipt scalar columns drift from strict carrier delivery order")
        return {"event": event, "receipt": receipt, "deliveries": deliveries}

    def write_transition_bundle(
        self,
        *,
        algo_instance: ExecutionAlgoInstancePersistenceV2,
        delivery: AlgoDeliveryPersistenceV1,
        receipt: AlgoTransitionReceiptV1 | AlgoFailureReceiptV1 | AlgoSkipReceiptV1,
        projection_set: ExecutionProjectionSetV1 | None,
        after_state: AlgoStateSnapshotV2 | None,
        expected_algo_row_version: int,
        expected_delivery_row_version: int,
        new_child_mappings: Sequence[ExecutionCommandChildMappingV1] = (),
        command_outboxes: Sequence[BrokerCommandOutboxV1] = (),
        child_price_type: int = 2,
    ) -> dict[str, Any]:
        if delivery.row_version != expected_delivery_row_version + 1:
            raise KernelRepositoryConflict("delivery successor version does not match CAS expectation")
        mappings = tuple(new_child_mappings)
        outboxes = tuple(command_outboxes)
        if any(not isinstance(item, ExecutionCommandChildMappingV1) for item in mappings):
            raise TypeError("new_child_mappings must contain only ExecutionCommandChildMappingV1")
        if any(not isinstance(item, BrokerCommandOutboxV1) for item in outboxes):
            raise TypeError("command_outboxes must contain only BrokerCommandOutboxV1")
        for mapping in mappings:
            try:
                mapping.validate_initial_v1()
            except ValueError as exc:
                raise ValueError("first write requires initial RESERVED mapping") from exc
        for outbox in outboxes:
            try:
                outbox.validate_initial_v1()
            except ValueError as exc:
                raise ValueError("first write requires initial PENDING outbox") from exc
        if type(child_price_type) is not int:
            raise TypeError("child_price_type must be a strict integer")
        if child_price_type != 2:
            raise ValueError("K2 SUBMIT_LIMIT child_price_type authority must be 2")
        if isinstance(receipt, AlgoTransitionReceiptV1):
            kind = "APPLIED"
            transition_json, failure_json, skip_json = receipt.model_dump(mode="json"), None, None
            transition_id = receipt.transition_id
            expected_command_ids = receipt.ordered_command_ids
            if projection_set is None or after_state is None:
                raise ValueError("APPLIED transition requires projection set and after-state")
        elif isinstance(receipt, AlgoFailureReceiptV1):
            kind = "FAILED_TERMINAL"
            transition_json, failure_json, skip_json = None, receipt.model_dump(mode="json"), None
            transition_id = receipt.failure_receipt_id
            expected_command_ids = receipt.ordered_cancel_command_ids
            if projection_set is not None or after_state is not None or mappings:
                raise ValueError("failure transition cannot create a new child mapping or applied state")
        elif isinstance(receipt, AlgoSkipReceiptV1):
            kind = "SKIPPED_TERMINAL"
            transition_json, failure_json, skip_json = None, None, receipt.model_dump(mode="json")
            transition_id = receipt.skip_receipt_id
            expected_command_ids = ()
            if projection_set is not None or after_state is not None or mappings or outboxes:
                raise ValueError("skip transition cannot persist effects")
        else:
            raise TypeError("receipt must be an exact K2 transition/failure/skip receipt")
        if (
            receipt.delivery_id != delivery.delivery_id
            or receipt.event_id != delivery.event_id
            or receipt.runtime_id != delivery.runtime_id
            or receipt.algo_instance_id != delivery.algo_instance_id
            or algo_instance.runtime_id != delivery.runtime_id
            or algo_instance.algo_instance_id != delivery.algo_instance_id
        ):
            raise ValueError("transition owner identities do not close")
        if tuple(item.command_id for item in outboxes) != expected_command_ids:
            raise ValueError("command outboxes do not match the receipt ordered command set")
        if len({item.mapping_id for item in mappings}) != len(mappings):
            raise ValueError("new child mappings contain duplicate mapping identity")
        outbox_by_command = {item.command_id: item for item in outboxes}
        if len(outbox_by_command) != len(outboxes):
            raise ValueError("command outboxes contain duplicate command identity")
        for mapping in mappings:
            outbox = outbox_by_command.get(mapping.command_id)
            if outbox is None or outbox.mapping_id != mapping.mapping_id:
                raise ValueError("new child mapping is not closed by its SUBMIT outbox")
            if (
                mapping.created_transition_id != transition_id
                or mapping.runtime_id != receipt.runtime_id
                or mapping.algo_instance_id != receipt.algo_instance_id
                or mapping.parent_intent_id != algo_instance.parent_intent_id
            ):
                raise ValueError("new child mapping owner conflicts with transition")
        for outbox in outboxes:
            if (
                outbox.transition_id != transition_id
                or outbox.runtime_id != receipt.runtime_id
                or outbox.algo_instance_id != receipt.algo_instance_id
                or outbox.parent_intent_id != algo_instance.parent_intent_id
            ):
                raise ValueError("outbox owner conflicts with transition")
        if isinstance(receipt, AlgoTransitionReceiptV1):
            input_hashes = (
                projection_set.projection_set_sha256,
                after_state.state_sha256,
                *(item.payload_sha256 for item in mappings),
                *(item.payload_sha256 for item in outboxes),
            )
        elif isinstance(receipt, AlgoFailureReceiptV1):
            input_hashes = (receipt.plugin_manifest_sha256, receipt.context_sha256)
        else:
            input_hashes = ()
        expected_transaction_identity = transaction_commit_identity_v1(
            operation=f"WRITE_{kind}_TRANSITION_BUNDLE",
            owner_identities=(
                receipt.runtime_id,
                receipt.algo_instance_id,
                receipt.event_id,
                receipt.delivery_id,
            ),
            input_hashes=input_hashes,
            output_identities=(
                transition_id,
                *(item.mapping_id for item in mappings),
                *(item.command_id for item in outboxes),
            ),
        )
        if receipt.transaction_commit_identity != expected_transaction_identity:
            raise ValueError("transition receipt does not use repository-owned transaction commit identity")
        transition_sequence = (
            receipt.transition_sequence if hasattr(receipt, "transition_sequence") else delivery.algo_delivery_sequence
        )
        transition_projection = _transition_scalar_projection(
            receipt=receipt,
            kind=kind,
            transition_sequence=transition_sequence,
            projection_set=projection_set,
            after_state=after_state,
        )
        try:
            existing_readback = self.read_transition_bundle(transition_id)
        except KeyError:
            existing_readback = None
        if existing_readback is not None:
            if not _transition_retry_matches(
                existing_readback,
                receipt=receipt,
                projection_set=projection_set,
                after_state=after_state,
                mappings=mappings,
                outboxes=outboxes,
            ):
                raise KernelRepositoryConflict("transition identity exists with different immutable bundle")
            return existing_readback
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_algo_transition(
                        transition_id,delivery_id,event_id,runtime_id,algo_instance_id,transition_sequence,
                        transition_kind,transition_receipt_json,failure_receipt_json,skip_receipt_json,
                        receipt_sha256,execution_projection_set_json,execution_projection_set_sha256,
                        after_state_json,after_state_sha256,transaction_commit_identity
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (transition_id) DO NOTHING
                    """,
                    (
                        transition_projection["transition_id"],
                        transition_projection["delivery_id"],
                        transition_projection["event_id"],
                        transition_projection["runtime_id"],
                        transition_projection["algo_instance_id"],
                        transition_projection["transition_sequence"],
                        transition_projection["transition_kind"],
                        None
                        if transition_projection["transition_receipt_json"] is None
                        else _json(transition_projection["transition_receipt_json"]),
                        None
                        if transition_projection["failure_receipt_json"] is None
                        else _json(transition_projection["failure_receipt_json"]),
                        None
                        if transition_projection["skip_receipt_json"] is None
                        else _json(transition_projection["skip_receipt_json"]),
                        transition_projection["receipt_sha256"],
                        None
                        if transition_projection["execution_projection_set_json"] is None
                        else _json(transition_projection["execution_projection_set_json"]),
                        transition_projection["execution_projection_set_sha256"],
                        None
                        if transition_projection["after_state_json"] is None
                        else _json(transition_projection["after_state_json"]),
                        transition_projection["after_state_sha256"],
                        transition_projection["transaction_commit_identity"],
                    ),
                )
                cur.execute(
                    """
                    SELECT transition_kind, transition_receipt_json, failure_receipt_json, skip_receipt_json,
                           execution_projection_set_json, after_state_json
                    FROM qmt_strategy.execution_algo_transition
                    WHERE transition_id=%s
                    """,
                    (transition_id,),
                )
                persisted = cur.fetchone()
                if persisted is None:
                    raise KernelRepositoryConflict("transition bundle is incomplete inside transaction")
                expected_receipt_json = {
                    "APPLIED": transition_json,
                    "FAILED_TERMINAL": failure_json,
                    "SKIPPED_TERMINAL": skip_json,
                }[kind]
                persisted_receipt_json = persisted[
                    {
                        "APPLIED": "transition_receipt_json",
                        "FAILED_TERMINAL": "failure_receipt_json",
                        "SKIPPED_TERMINAL": "skip_receipt_json",
                    }[kind]
                ]
                if (
                    persisted["transition_kind"] != kind
                    or persisted_receipt_json != expected_receipt_json
                    or persisted["execution_projection_set_json"]
                    != (None if projection_set is None else projection_set.model_dump(mode="json"))
                    or persisted["after_state_json"]
                    != (None if after_state is None else after_state.model_dump(mode="json"))
                ):
                    raise KernelRepositoryConflict("transition identity exists with different immutable bundle")
                self._write_transition_commands_with_cursor(
                    cur,
                    transition_id=transition_id,
                    mappings=mappings,
                    outboxes=outboxes,
                    child_price_type=child_price_type,
                )
                self._cas_algo_with_cursor(
                    cur,
                    algo_instance=algo_instance,
                    expected_row_version=expected_algo_row_version,
                )
                cur.execute(
                    """
                    SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery
                    WHERE delivery_id = %s FOR UPDATE
                    """,
                    (delivery.delivery_id,),
                )
                previous_row = cur.fetchone()
                if previous_row is None:
                    raise KeyError(delivery.delivery_id)
                previous = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(previous_row, "carrier_json"))
                delivery.validate_successor_v1(previous)
                delivery_projection = _delivery_scalar_projection(delivery)
                cur.execute(
                    """
                    UPDATE qmt_strategy.execution_algo_event_delivery
                    SET status=%s, attempt_count=%s, lease_owner=%s,
                        lease_worker_id=%s, lease_process_incarnation_id=%s, lease_epoch=%s,
                        lease_fence_token=%s, lease_expires_at=%s, transition_id=%s,
                        last_error_json=%s, next_attempt_at_utc=%s, failure_receipt_id=%s,
                        skip_receipt_id=%s, row_version=%s, updated_at_utc=%s, closed_at_utc=%s,
                        carrier_json=%s
                    WHERE delivery_id=%s AND row_version=%s
                      AND lease_owner IS NOT DISTINCT FROM %s
                      AND lease_epoch=%s
                      AND lease_fence_token IS NOT DISTINCT FROM %s
                    """,
                    (
                        delivery_projection["status"],
                        delivery_projection["attempt_count"],
                        delivery_projection["lease_owner"],
                        delivery_projection["lease_worker_id"],
                        delivery_projection["lease_process_incarnation_id"],
                        delivery_projection["lease_epoch"],
                        delivery_projection["lease_fence_token"],
                        delivery_projection["lease_expires_at"],
                        delivery_projection["transition_id"],
                        None
                        if delivery_projection["last_error_json"] is None
                        else _json(delivery_projection["last_error_json"]),
                        delivery_projection["next_attempt_at_utc"],
                        delivery_projection["failure_receipt_id"],
                        delivery_projection["skip_receipt_id"],
                        delivery_projection["row_version"],
                        delivery_projection["updated_at_utc"],
                        delivery_projection["closed_at_utc"],
                        _json(delivery.model_dump(mode="json")),
                        delivery.delivery_id,
                        expected_delivery_row_version,
                        previous.lease_owner,
                        previous.lease_epoch,
                        previous.lease_fence_token,
                    ),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("delivery CAS failed")
        readback = self.read_transition_bundle(transition_id)
        if not _transition_retry_matches(
            readback,
            receipt=receipt,
            projection_set=projection_set,
            after_state=after_state,
            mappings=mappings,
            outboxes=outboxes,
        ):
            raise KernelRepositoryConflict("transition command closure differs from writer payload")
        return readback

    def read_transition_bundle(self, transition_id: str) -> dict[str, Any]:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_algo_transition WHERE transition_id=%s", (transition_id,)
                )
                row = cur.fetchone()
                cur.execute(
                    """
                    SELECT outbox.command_id,child.created_transition_id
                    FROM qmt_strategy.execution_algo_command_outbox AS outbox
                    JOIN qmt_strategy.execution_child_order AS child
                      ON child.mapping_id=outbox.mapping_id
                    WHERE outbox.transition_id=%s
                    ORDER BY outbox.ordinal, outbox.command_id
                    """,
                    (transition_id,),
                )
                command_rows = cur.fetchall()
        if row is None:
            raise KeyError(transition_id)
        kind = row["transition_kind"]
        receipt_model: Any = {
            "APPLIED": AlgoTransitionReceiptV1,
            "FAILED_TERMINAL": AlgoFailureReceiptV1,
            "SKIPPED_TERMINAL": AlgoSkipReceiptV1,
        }[kind]
        receipt_key = {
            "APPLIED": "transition_receipt_json",
            "FAILED_TERMINAL": "failure_receipt_json",
            "SKIPPED_TERMINAL": "skip_receipt_json",
        }[kind]
        receipt = _model_from_json(receipt_model, _row_json(row, receipt_key))
        projection_set = (
            None
            if row["execution_projection_set_json"] is None
            else _model_from_json(ExecutionProjectionSetV1, _row_json(row, "execution_projection_set_json"))
        )
        after_state = (
            None
            if row["after_state_json"] is None
            else _model_from_json(AlgoStateSnapshotV2, _row_json(row, "after_state_json"))
        )
        transition_sequence = (
            receipt.transition_sequence
            if hasattr(receipt, "transition_sequence")
            else self.read_delivery(receipt.delivery_id).algo_delivery_sequence
        )
        _assert_scalar_columns(
            row,
            _transition_scalar_projection(
                receipt=receipt,
                kind=kind,
                transition_sequence=transition_sequence,
                projection_set=projection_set,
                after_state=after_state,
            ),
            carrier_name="transition",
        )
        mappings_by_id: dict[str, ExecutionCommandChildMappingV1] = {}
        outboxes: list[BrokerCommandOutboxV1] = []
        for command_row in command_rows:
            chain = self.read_command_identity_chain(str(command_row["command_id"]))
            outbox = chain["outbox"]
            mapping = chain["mapping"]
            outboxes.append(outbox)
            if command_row["created_transition_id"] == transition_id:
                mappings_by_id.setdefault(mapping.mapping_id, mapping)
        return {
            "receipt": receipt,
            "projection_set": projection_set,
            "after_state": after_state,
            "new_child_mappings": tuple(mappings_by_id.values()),
            "command_outboxes": tuple(outboxes),
        }

    def _write_transition_commands_with_cursor(
        self,
        cur: Any,
        *,
        transition_id: str,
        mappings: Sequence[ExecutionCommandChildMappingV1],
        outboxes: Sequence[BrokerCommandOutboxV1],
        child_price_type: int,
    ) -> None:
        for mapping in mappings:
            mapping_projection = _mapping_scalar_projection(mapping, child_price_type=child_price_type)
            cur.execute(
                """
                    INSERT INTO qmt_strategy.execution_child_order(
                        child_order_id,runtime_id,algo_instance_id,parent_intent_id,strategy_slot_id,
                        symbol,side,quantity,price,price_type,status,metadata,updated_at,
                        kernel_contract_version,mapping_id,command_id,local_vt_orderid,
                        deterministic_client_order_ref,order_remark,mapping_status,mapping_version,
                        mapping_payload_sha256,mapping_receipt_sha256,broker_identity_source_event_id,
                        last_order_event_id,last_trade_event_id,created_transition_id,updated_by_event_id,
                        mapping_created_at_utc,mapping_updated_at_utc,mapping_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'SUBMITTING','{}'::jsonb,%s,'KERNEL_V2',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (child_order_id) DO NOTHING
                """,
                (
                    mapping_projection["child_order_id"],
                    mapping_projection["runtime_id"],
                    mapping_projection["algo_instance_id"],
                    mapping_projection["parent_intent_id"],
                    mapping_projection["strategy_slot_id"],
                    mapping_projection["symbol"],
                    mapping_projection["side"],
                    mapping_projection["quantity"],
                    mapping_projection["price"],
                    mapping_projection["price_type"],
                    mapping_projection["updated_at"],
                    mapping_projection["mapping_id"],
                    mapping_projection["command_id"],
                    mapping_projection["local_vt_orderid"],
                    mapping_projection["deterministic_client_order_ref"],
                    mapping_projection["order_remark"],
                    mapping_projection["mapping_status"],
                    mapping_projection["mapping_version"],
                    mapping_projection["mapping_payload_sha256"],
                    mapping_projection["mapping_receipt_sha256"],
                    mapping_projection["broker_identity_source_event_id"],
                    mapping_projection["last_order_event_id"],
                    mapping_projection["last_trade_event_id"],
                    mapping_projection["created_transition_id"],
                    mapping_projection["updated_by_event_id"],
                    mapping_projection["mapping_created_at_utc"],
                    mapping_projection["mapping_updated_at_utc"],
                    _json(mapping.model_dump(mode="json")),
                ),
            )
            cur.execute(
                "SELECT mapping_json FROM qmt_strategy.execution_child_order WHERE mapping_id=%s",
                (mapping.mapping_id,),
            )
            mapping_row = cur.fetchone()
            if (
                mapping_row is None
                or _model_from_json(ExecutionCommandChildMappingV1, _row_json(mapping_row, "mapping_json")) != mapping
            ):
                raise KernelRepositoryConflict("mapping identity exists with different immutable payload")
        for outbox in outboxes:
            cur.execute(
                "SELECT mapping_json FROM qmt_strategy.execution_child_order WHERE mapping_id=%s",
                (outbox.mapping_id,),
            )
            mapping_row = cur.fetchone()
            if mapping_row is None:
                raise KernelRepositoryConflict("outbox references an unknown command-child mapping")
            mapping = _model_from_json(ExecutionCommandChildMappingV1, _row_json(mapping_row, "mapping_json"))
            if (
                mapping.runtime_id != outbox.runtime_id
                or mapping.algo_instance_id != outbox.algo_instance_id
                or mapping.parent_intent_id != outbox.parent_intent_id
                or mapping.local_vt_orderid != outbox.local_vt_orderid
                or outbox.transition_id != transition_id
            ):
                raise KernelRepositoryConflict("outbox mapping owner identity conflicts")
            command = BrokerCommandV2.model_validate_json(
                json.dumps(outbox.model_dump(mode="json")["payload_json"], sort_keys=True, separators=(",", ":"))
            )
            if (
                command.symbol != mapping.symbol
                or command.side is not mapping.side
                or command.price_decimal != mapping.requested_price_decimal
                or command.quantity != mapping.requested_quantity
            ):
                raise ValueError("command business payload conflicts with durable mapping")
            if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
                if mapping.command_id != command.command_id or mapping.created_transition_id != transition_id:
                    raise ValueError("SUBMIT command must create its mapping in the same transition")
            elif mapping.broker_order_id is None or command.owned_broker_order_id != mapping.broker_order_id:
                raise ValueError("CANCEL broker order identity conflicts with durable mapping")
            cur.execute(
                """
                    INSERT INTO qmt_strategy.execution_algo_command_outbox(
                        command_id,transition_id,ordinal,runtime_id,algo_instance_id,parent_intent_id,
                        mapping_id,command_type,local_vt_orderid,payload_json,payload_sha256,
                        status,attempt_count,lease_owner,lease_epoch,lease_fence_token,
                        lease_expires_at,dispatch_attempt_id,deterministic_client_order_ref,next_attempt_at_utc,
                        broker_called,broker_order_id,ack_receipt_json,ack_receipt_sha256,
                        non_acceptance_receipt_json,unknown_outcome_receipt_json,reconcile_receipt_json,
                        last_error_json,row_version,created_at_utc,
                        updated_at_utc,closed_at_utc,carrier_json,outbox_row_sha256
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (command_id) DO NOTHING
                    """,
                self._outbox_sql_values(outbox),
            )
            cur.execute(
                "SELECT carrier_json AS outbox_json FROM qmt_strategy.execution_algo_command_outbox WHERE command_id=%s",
                (outbox.command_id,),
            )
            row = cur.fetchone()
            if row is None or _model_from_json(BrokerCommandOutboxV1, _row_json(row, "outbox_json")) != outbox:
                raise KernelRepositoryConflict("command identity exists with different immutable payload")

    def read_command_identity_chain(self, command_id: str) -> dict[str, Any]:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT outbox.command_id AS requested_command_id,
                           child.child_order_id,child.runtime_id,child.algo_instance_id,
                           child.parent_intent_id,child.strategy_slot_id,child.symbol,child.side,
                           child.quantity,child.price,child.price_type,child.status,child.broker_order_id,
                           child.updated_at,child.kernel_contract_version,child.mapping_id,child.command_id,
                           child.local_vt_orderid,child.deterministic_client_order_ref,child.order_remark,
                           child.mapping_status,child.mapping_version,child.mapping_payload_sha256,
                           child.mapping_receipt_sha256,child.broker_identity_source_event_id,
                           child.last_order_event_id,child.last_trade_event_id,child.created_transition_id,
                           child.updated_by_event_id,child.mapping_created_at_utc,
                           child.mapping_updated_at_utc,child.mapping_json
                    FROM qmt_strategy.execution_algo_command_outbox AS outbox
                    JOIN qmt_strategy.execution_child_order AS child
                      ON child.mapping_id = outbox.mapping_id
                    WHERE outbox.command_id = %s
                    """,
                    (command_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(command_id)
        mapping = _model_from_json(ExecutionCommandChildMappingV1, _row_json(row, "mapping_json"))
        _assert_scalar_columns(
            row,
            _mapping_scalar_projection(mapping),
            carrier_name="mapping",
        )
        return {"outbox": self.read_outbox_command(command_id), "mapping": mapping}

    def compare_and_swap_mapping_outbox(
        self,
        *,
        mapping: ExecutionCommandChildMappingV1,
        outbox: BrokerCommandOutboxV1,
        expected_mapping_version: int,
        expected_outbox_row_version: int,
        expected_lease_owner: str | None,
        expected_lease_epoch: int,
        expected_lease_fence_token: str | None,
    ) -> dict[str, Any]:
        if mapping.mapping_id != outbox.mapping_id:
            raise ValueError("mapping and outbox identities do not close")
        if (
            mapping.runtime_id != outbox.runtime_id
            or mapping.algo_instance_id != outbox.algo_instance_id
            or mapping.parent_intent_id != outbox.parent_intent_id
            or mapping.local_vt_orderid != outbox.local_vt_orderid
        ):
            raise ValueError("mapping and outbox business identities do not close")
        command = BrokerCommandV2.model_validate_json(
            json.dumps(outbox.model_dump(mode="json")["payload_json"], sort_keys=True, separators=(",", ":"))
        )
        if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            if (
                mapping.command_id != outbox.command_id
                or mapping.deterministic_client_order_ref != outbox.deterministic_client_order_ref
                or mapping.broker_order_id != outbox.broker_order_id
            ):
                raise ValueError("SUBMIT mapping/outbox broker identity does not close")
            coupled_states = {
                CommandChildMappingStatusV1.DISPATCHING: {BrokerCommandOutboxStatusV1.DISPATCHING},
                CommandChildMappingStatusV1.BROKER_ACCEPTED: {BrokerCommandOutboxStatusV1.ACKED},
                CommandChildMappingStatusV1.BROKER_REJECTED: {BrokerCommandOutboxStatusV1.ACKED_REJECTED},
                CommandChildMappingStatusV1.OUTCOME_UNKNOWN: {
                    BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
                    BrokerCommandOutboxStatusV1.RECONCILING,
                },
            }
            if outbox.status not in coupled_states.get(mapping.mapping_status, set()):
                raise ValueError("mapping/outbox coupled state conflicts")
        else:
            if command.owned_broker_order_id != mapping.broker_order_id:
                raise ValueError("CANCEL owned broker identity conflicts with durable mapping")
            cancel_nonterminal = {
                BrokerCommandOutboxStatusV1.DISPATCHING,
                BrokerCommandOutboxStatusV1.ACKED,
                BrokerCommandOutboxStatusV1.ACKED_REJECTED,
                BrokerCommandOutboxStatusV1.FAILED_RETRYABLE,
                BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
                BrokerCommandOutboxStatusV1.RECONCILING,
                BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
            }
            if mapping.mapping_status is CommandChildMappingStatusV1.BROKER_ACCEPTED:
                if outbox.status not in cancel_nonterminal:
                    raise ValueError("CANCEL mapping/outbox coupled state conflicts")
            elif mapping.mapping_status is CommandChildMappingStatusV1.TERMINAL:
                ack = outbox.ack_receipt_json
                if (
                    outbox.status is not BrokerCommandOutboxStatusV1.ACKED
                    or ack is None
                    or mapping.updated_by_event_id is None
                    or mapping.last_order_event_id != mapping.updated_by_event_id
                ):
                    raise ValueError("CANCEL terminal mapping requires exact callback/reconciliation evidence")
            else:
                raise ValueError("CANCEL cannot regress or detach the accepted SUBMIT mapping")
        if (
            type(expected_mapping_version) is not int
            or type(expected_outbox_row_version) is not int
            or type(expected_lease_epoch) is not int
            or expected_lease_epoch < 0
        ):
            raise TypeError("expected mapping/outbox versions must be strict integers")
        if (expected_lease_owner is None) != (expected_lease_fence_token is None):
            raise ValueError("expected lease owner and fence must be present together")
        if outbox.lease_owner is not None:
            self._verify_lease_owner(outbox.lease_owner)
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT child.mapping_json, current_outbox.carrier_json AS outbox_json,
                           algo.kernel_carrier_json AS algo_json
                    FROM qmt_strategy.execution_child_order AS child
                    JOIN qmt_strategy.execution_algo_command_outbox AS current_outbox
                      ON current_outbox.mapping_id=child.mapping_id
                    JOIN qmt_strategy.execution_algo_instance AS algo
                      ON algo.runtime_id=child.runtime_id AND algo.algo_instance_id=child.algo_instance_id
                    WHERE child.mapping_id=%s AND current_outbox.command_id=%s
                    FOR UPDATE OF algo, child, current_outbox
                    """,
                    (mapping.mapping_id, outbox.command_id),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(outbox.command_id)
                previous_mapping = _model_from_json(ExecutionCommandChildMappingV1, _row_json(row, "mapping_json"))
                previous_outbox = _model_from_json(BrokerCommandOutboxV1, _row_json(row, "outbox_json"))
                previous_algo = _model_from_json(ExecutionAlgoInstancePersistenceV2, _row_json(row, "algo_json"))
                if (
                    previous_outbox.lease_owner != expected_lease_owner
                    or previous_outbox.lease_epoch != expected_lease_epoch
                    or previous_outbox.lease_fence_token != expected_lease_fence_token
                ):
                    raise KernelRepositoryConflict("outbox CAS expected lease differs from durable predecessor")
                mapping_changed = mapping != previous_mapping
                if mapping_changed:
                    if (
                        command.command_type is BrokerCommandTypeV2.CANCEL_ORDER
                        and mapping.mapping_status is CommandChildMappingStatusV1.TERMINAL
                        and (
                            outbox.ack_receipt_json is None
                            or outbox.ack_receipt_json.source.value not in {"CALLBACK", "RECONCILIATION"}
                        )
                    ):
                        raise ValueError(
                            "CANCEL terminal mapping may only be created by callback/reconciliation evidence"
                        )
                    mapping.validate_successor_v1(previous_mapping)
                elif mapping.mapping_version != expected_mapping_version:
                    raise KernelRepositoryConflict("unchanged CANCEL mapping version differs from durable predecessor")
                outbox.validate_successor_v1(previous_outbox)
                if mapping_changed:
                    mapping_values = mapping.model_dump(mode="json")
                    mapping_projection = _mapping_scalar_projection(mapping)
                    cur.execute(
                        """
                        UPDATE qmt_strategy.execution_child_order
                        SET broker_order_id=%s,broker_identity_source_event_id=%s,mapping_status=%s,
                            mapping_version=%s,mapping_payload_sha256=%s,mapping_receipt_sha256=%s,
                            last_order_event_id=%s,last_trade_event_id=%s,updated_by_event_id=%s,
                            mapping_updated_at_utc=%s,updated_at=%s,mapping_json=%s
                        WHERE mapping_id=%s AND mapping_version=%s
                        """,
                        (
                            mapping_projection["broker_order_id"],
                            mapping_projection["broker_identity_source_event_id"],
                            mapping_projection["mapping_status"],
                            mapping_projection["mapping_version"],
                            mapping_projection["mapping_payload_sha256"],
                            mapping_projection["mapping_receipt_sha256"],
                            mapping_projection["last_order_event_id"],
                            mapping_projection["last_trade_event_id"],
                            mapping_projection["updated_by_event_id"],
                            mapping_projection["mapping_updated_at_utc"],
                            mapping_projection["updated_at"],
                            _json(mapping_values),
                            mapping_projection["mapping_id"],
                            expected_mapping_version,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise KernelRepositoryConflict("mapping CAS failed")
                outbox_values = outbox.model_dump(mode="json")
                outbox_projection = _outbox_scalar_projection(outbox)
                cur.execute(
                    """
                    UPDATE qmt_strategy.execution_algo_command_outbox
                    SET status=%s,attempt_count=%s,lease_owner=%s,lease_worker_id=%s,
                        lease_process_incarnation_id=%s,lease_epoch=%s,lease_fence_token=%s,
                        lease_expires_at=%s,dispatch_attempt_id=%s,next_attempt_at_utc=%s,
                        broker_called=%s,broker_order_id=%s,ack_receipt_json=%s,ack_receipt_sha256=%s,
                        non_acceptance_receipt_json=%s,unknown_outcome_receipt_json=%s,
                        reconcile_receipt_json=%s,last_error_json=%s,row_version=%s,
                        updated_at_utc=%s,closed_at_utc=%s,carrier_json=%s,outbox_row_sha256=%s
                    WHERE command_id=%s AND row_version=%s
                      AND lease_owner IS NOT DISTINCT FROM %s
                      AND lease_epoch=%s
                      AND lease_fence_token IS NOT DISTINCT FROM %s
                    """,
                    (
                        outbox_projection["status"],
                        outbox_projection["attempt_count"],
                        outbox_projection["lease_owner"],
                        outbox_projection["lease_worker_id"],
                        outbox_projection["lease_process_incarnation_id"],
                        outbox_projection["lease_epoch"],
                        outbox_projection["lease_fence_token"],
                        outbox_projection["lease_expires_at"],
                        outbox_projection["dispatch_attempt_id"],
                        outbox_projection["next_attempt_at_utc"],
                        outbox_projection["broker_called"],
                        outbox_projection["broker_order_id"],
                        None
                        if outbox_projection["ack_receipt_json"] is None
                        else _json(outbox_projection["ack_receipt_json"]),
                        outbox_projection["ack_receipt_sha256"],
                        None
                        if outbox_projection["non_acceptance_receipt_json"] is None
                        else _json(outbox_projection["non_acceptance_receipt_json"]),
                        None
                        if outbox_projection["unknown_outcome_receipt_json"] is None
                        else _json(outbox_projection["unknown_outcome_receipt_json"]),
                        None
                        if outbox_projection["reconcile_receipt_json"] is None
                        else _json(outbox_projection["reconcile_receipt_json"]),
                        None
                        if outbox_projection["last_error_json"] is None
                        else _json(outbox_projection["last_error_json"]),
                        outbox_projection["row_version"],
                        outbox_projection["updated_at_utc"],
                        outbox_projection["closed_at_utc"],
                        _json(outbox_values),
                        outbox_projection["outbox_row_sha256"],
                        outbox_projection["command_id"],
                        expected_outbox_row_version,
                        previous_outbox.lease_owner,
                        previous_outbox.lease_epoch,
                        previous_outbox.lease_fence_token,
                    ),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("outbox CAS failed")
                cur.execute(
                    """
                    SELECT COUNT(*) AS active_child_count
                    FROM qmt_strategy.execution_child_order
                    WHERE runtime_id=%s AND algo_instance_id=%s AND kernel_contract_version='KERNEL_V2'
                      AND mapping_status IN ('RESERVED','DISPATCHING','BROKER_ACCEPTED','OUTCOME_UNKNOWN')
                    """,
                    (mapping.runtime_id, mapping.algo_instance_id),
                )
                active_count = int(cur.fetchone()["active_child_count"])
                closure = previous_algo.active_child_closure_status
                if previous_algo.status is ExecutionAlgoPersistenceStatusV2.FAILED and active_count == 0:
                    closure = ActiveChildClosureStatusV1.CLEAN
                algo_payload = previous_algo.model_dump(mode="python")
                algo_payload.update(
                    active_child_count=active_count,
                    active_child_closure_status=closure,
                    row_version=previous_algo.row_version + 1,
                    updated_at_utc=max(previous_algo.updated_at_utc, mapping.updated_at_utc, outbox.updated_at_utc),
                )
                updated_algo = ExecutionAlgoInstancePersistenceV2.model_validate(algo_payload)
                self._cas_algo_with_cursor(
                    cur,
                    algo_instance=updated_algo,
                    expected_row_version=previous_algo.row_version,
                )
        chain = self.read_command_identity_chain(outbox.command_id)
        if chain != {"mapping": mapping, "outbox": outbox}:
            raise KernelRepositoryConflict("mapping/outbox CAS readback differs from writer payload")
        if self.read_algo_instance(mapping.algo_instance_id) != updated_algo:
            raise KernelRepositoryConflict("mapping/outbox/algo post-commit readback differs from atomic bundle")
        return chain

    def close_mapping_from_callback(
        self,
        *,
        mapping: ExecutionCommandChildMappingV1,
        callback_event: RuntimeEventEnvelopeV2,
        cancel_command_id: str,
        expected_mapping_version: int,
        expected_algo_row_version: int,
    ) -> dict[str, Any]:
        if not isinstance(mapping, ExecutionCommandChildMappingV1):
            raise TypeError("mapping must be ExecutionCommandChildMappingV1")
        if not isinstance(callback_event, RuntimeEventEnvelopeV2):
            raise TypeError("callback_event must be RuntimeEventEnvelopeV2")
        if type(expected_mapping_version) is not int or type(expected_algo_row_version) is not int:
            raise TypeError("expected callback closure versions must be strict integers")
        if mapping.mapping_status is not CommandChildMappingStatusV1.TERMINAL:
            raise ValueError("callback closure requires a TERMINAL mapping successor")
        allowed_sources = {
            EventTypeV2.ORDER: EventSourceV2.QMT_GATEWAY_CALLBACK,
            EventTypeV2.TRADE: EventSourceV2.QMT_GATEWAY_CALLBACK,
            EventTypeV2.RECONCILE: EventSourceV2.QMT_OMS_RECONCILIATION,
        }
        if (
            callback_event.event_type not in allowed_sources
            or callback_event.source is not allowed_sources[callback_event.event_type]
        ):
            raise ValueError("callback event identity conflicts with terminal mapping")
        payload = callback_event.model_dump(mode="json")["payload"]
        expected_payload = {
            "runtime_id": mapping.runtime_id,
            "algo_instance_id": mapping.algo_instance_id,
            "parent_intent_id": mapping.parent_intent_id,
            "mapping_id": mapping.mapping_id,
            "local_vt_orderid": mapping.local_vt_orderid,
            "broker_order_id": mapping.broker_order_id,
            "terminal": True,
        }
        if any(payload.get(key) != value for key, value in expected_payload.items()):
            raise ValueError("callback event identity conflicts with terminal mapping")
        if mapping.updated_by_event_id != callback_event.event_id:
            raise ValueError("callback event identity conflicts with terminal mapping")
        if callback_event.event_type is EventTypeV2.ORDER and mapping.last_order_event_id != callback_event.event_id:
            raise ValueError("callback event identity conflicts with terminal mapping")
        if callback_event.event_type is EventTypeV2.TRADE and mapping.last_trade_event_id != callback_event.event_id:
            raise ValueError("callback event identity conflicts with terminal mapping")

        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT event.payload AS event_json,
                           child.mapping_json,
                           outbox.carrier_json AS outbox_json,
                           algo.kernel_carrier_json AS algo_json
                    FROM qmt_strategy.execution_runtime_event AS event
                    JOIN qmt_strategy.execution_child_order AS child
                      ON child.mapping_id=%s AND child.kernel_contract_version='KERNEL_V2'
                    JOIN qmt_strategy.execution_algo_command_outbox AS outbox
                      ON outbox.command_id=%s AND outbox.mapping_id=child.mapping_id
                    JOIN qmt_strategy.execution_algo_instance AS algo
                      ON algo.runtime_id=child.runtime_id AND algo.algo_instance_id=child.algo_instance_id
                    WHERE event.event_id=%s AND event.runtime_id=child.runtime_id
                      AND event.event_contract_version='KERNEL_V2'
                    FOR UPDATE OF child, outbox, algo
                    """,
                    (mapping.mapping_id, cancel_command_id, callback_event.event_id),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(cancel_command_id)
                persisted_event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(row, "event_json"))
                previous_mapping = _model_from_json(ExecutionCommandChildMappingV1, _row_json(row, "mapping_json"))
                unchanged_outbox = _model_from_json(BrokerCommandOutboxV1, _row_json(row, "outbox_json"))
                previous_algo = _model_from_json(ExecutionAlgoInstancePersistenceV2, _row_json(row, "algo_json"))
                command = BrokerCommandV2.model_validate_json(
                    json.dumps(
                        unchanged_outbox.model_dump(mode="json")["payload_json"],
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                if persisted_event != callback_event:
                    raise KernelRepositoryConflict("callback event durable payload differs from closure authority")
                if (
                    command.command_type is not BrokerCommandTypeV2.CANCEL_ORDER
                    or command.owned_broker_order_id != mapping.broker_order_id
                    or unchanged_outbox.mapping_id != mapping.mapping_id
                    or unchanged_outbox.runtime_id != mapping.runtime_id
                    or unchanged_outbox.algo_instance_id != mapping.algo_instance_id
                    or unchanged_outbox.parent_intent_id != mapping.parent_intent_id
                ):
                    raise ValueError("callback cancel command identity conflicts with terminal mapping")
                if previous_mapping == mapping:
                    if (
                        expected_mapping_version != mapping.mapping_version
                        or expected_algo_row_version != previous_algo.row_version
                    ):
                        raise KernelRepositoryConflict("idempotent callback closure versions differ from durable facts")
                    updated_algo = previous_algo
                else:
                    if previous_mapping.mapping_version != expected_mapping_version:
                        raise KernelRepositoryConflict(
                            "callback mapping CAS expected version differs from durable predecessor"
                        )
                    if previous_algo.row_version != expected_algo_row_version:
                        raise KernelRepositoryConflict(
                            "callback algo CAS expected version differs from durable predecessor"
                        )
                    mapping.validate_successor_v1(previous_mapping)
                    if previous_mapping.mapping_status is not CommandChildMappingStatusV1.BROKER_ACCEPTED:
                        raise ValueError("callback closure requires a durable BROKER_ACCEPTED predecessor")
                    cur.execute(
                        """
                        SELECT COUNT(*) AS conflict_count
                        FROM qmt_strategy.execution_child_order
                        WHERE runtime_id=%s AND algo_instance_id=%s AND parent_intent_id=%s
                          AND local_vt_orderid=%s AND broker_order_id=%s AND mapping_id<>%s
                        """,
                        (
                            mapping.runtime_id,
                            mapping.algo_instance_id,
                            mapping.parent_intent_id,
                            mapping.local_vt_orderid,
                            mapping.broker_order_id,
                            mapping.mapping_id,
                        ),
                    )
                    if int(cur.fetchone()["conflict_count"]) != 0:
                        raise KernelRepositoryConflict("callback identity matches multiple durable child mappings")
                    mapping_projection = _mapping_scalar_projection(mapping)
                    cur.execute(
                        """
                        UPDATE qmt_strategy.execution_child_order
                        SET broker_order_id=%s,broker_identity_source_event_id=%s,mapping_status=%s,
                            mapping_version=%s,mapping_payload_sha256=%s,mapping_receipt_sha256=%s,
                            last_order_event_id=%s,last_trade_event_id=%s,updated_by_event_id=%s,
                            mapping_updated_at_utc=%s,updated_at=%s,mapping_json=%s
                        WHERE mapping_id=%s AND mapping_version=%s
                        """,
                        (
                            mapping_projection["broker_order_id"],
                            mapping_projection["broker_identity_source_event_id"],
                            mapping_projection["mapping_status"],
                            mapping_projection["mapping_version"],
                            mapping_projection["mapping_payload_sha256"],
                            mapping_projection["mapping_receipt_sha256"],
                            mapping_projection["last_order_event_id"],
                            mapping_projection["last_trade_event_id"],
                            mapping_projection["updated_by_event_id"],
                            mapping_projection["mapping_updated_at_utc"],
                            mapping_projection["updated_at"],
                            _json(mapping.model_dump(mode="json")),
                            mapping_projection["mapping_id"],
                            expected_mapping_version,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise KernelRepositoryConflict("callback mapping CAS failed")
                    cur.execute(
                        """
                        SELECT COUNT(*) AS active_child_count
                        FROM qmt_strategy.execution_child_order
                        WHERE runtime_id=%s AND algo_instance_id=%s AND kernel_contract_version='KERNEL_V2'
                          AND mapping_status IN ('RESERVED','DISPATCHING','BROKER_ACCEPTED','OUTCOME_UNKNOWN')
                        """,
                        (mapping.runtime_id, mapping.algo_instance_id),
                    )
                    active_count = int(cur.fetchone()["active_child_count"])
                    closure = previous_algo.active_child_closure_status
                    if previous_algo.status is ExecutionAlgoPersistenceStatusV2.FAILED and active_count == 0:
                        closure = ActiveChildClosureStatusV1.CLEAN
                    algo_payload = previous_algo.model_dump(mode="python")
                    algo_payload.update(
                        active_child_count=active_count,
                        active_child_closure_status=closure,
                        row_version=previous_algo.row_version + 1,
                        updated_at_utc=max(previous_algo.updated_at_utc, mapping.updated_at_utc),
                    )
                    updated_algo = ExecutionAlgoInstancePersistenceV2.model_validate(algo_payload)
                    self._cas_algo_with_cursor(
                        cur,
                        algo_instance=updated_algo,
                        expected_row_version=previous_algo.row_version,
                    )
                cur.execute(
                    """
                    /* callback closure readback */
                    SELECT child.mapping_json,outbox.carrier_json AS outbox_json,
                           algo.kernel_carrier_json AS algo_json
                    FROM qmt_strategy.execution_child_order AS child
                    JOIN qmt_strategy.execution_algo_command_outbox AS outbox
                      ON outbox.command_id=%s AND outbox.mapping_id=child.mapping_id
                    JOIN qmt_strategy.execution_algo_instance AS algo
                      ON algo.runtime_id=child.runtime_id AND algo.algo_instance_id=child.algo_instance_id
                    WHERE child.mapping_id=%s
                    """,
                    (cancel_command_id, mapping.mapping_id),
                )
                readback_row = cur.fetchone()
                if readback_row is None:
                    raise KernelRepositoryConflict("callback closure readback is incomplete")
                in_transaction_result = {
                    "mapping": _model_from_json(
                        ExecutionCommandChildMappingV1, _row_json(readback_row, "mapping_json")
                    ),
                    "outbox": _model_from_json(BrokerCommandOutboxV1, _row_json(readback_row, "outbox_json")),
                    "algo": _model_from_json(ExecutionAlgoInstancePersistenceV2, _row_json(readback_row, "algo_json")),
                }
                expected_result = {"mapping": mapping, "outbox": unchanged_outbox, "algo": updated_algo}
                if in_transaction_result != expected_result:
                    raise KernelRepositoryConflict("callback closure readback differs from atomic bundle")

        post_commit_chain = self.read_command_identity_chain(cancel_command_id)
        post_commit_algo = self.read_algo_instance(mapping.algo_instance_id)
        result = {
            "mapping": post_commit_chain["mapping"],
            "outbox": post_commit_chain["outbox"],
            "algo": post_commit_algo,
        }
        if result != expected_result:
            raise KernelRepositoryConflict("callback closure post-commit readback differs from atomic bundle")
        return result

    def claim_delivery(
        self,
        *,
        delivery_id: str,
        lease_owner: str,
        lease_epoch: int,
        lease_fence_token: str,
        lease_expires_at: Any,
        updated_at_utc: Any,
        expected_row_version: int,
    ) -> AlgoDeliveryPersistenceV1:
        self._verify_lease_owner(lease_owner)
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery WHERE delivery_id=%s FOR UPDATE",
                    (delivery_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(delivery_id)
                previous = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(row, "carrier_json"))
                if lease_epoch != previous.lease_epoch + 1:
                    raise KernelRepositoryConflict("delivery lease epoch is not the exact durable successor")
                expected_fence = kernel_lease_fence_token_v1(
                    owner_type="DELIVERY",
                    owner_id=delivery_id,
                    lease_epoch=lease_epoch,
                    lease_owner=lease_owner,
                )
                if lease_fence_token != expected_fence:
                    raise KernelRepositoryConflict("delivery lease fence differs from exact repository authority")
                payload = previous.model_dump(mode="python")
                payload.update(
                    status=DeliveryStatusV1.CLAIMED,
                    attempt_count=previous.attempt_count + 1,
                    lease_owner=lease_owner,
                    lease_epoch=lease_epoch,
                    lease_fence_token=lease_fence_token,
                    lease_expires_at=canonical_utc_datetime_v1(lease_expires_at, field_name="lease_expires_at"),
                    row_version=expected_row_version + 1,
                    updated_at_utc=canonical_utc_datetime_v1(updated_at_utc, field_name="updated_at_utc"),
                    next_attempt_at_utc=None,
                )
                claimed = AlgoDeliveryPersistenceV1.model_validate(payload)
                claimed.validate_successor_v1(previous)
                claimed_projection = _delivery_scalar_projection(claimed)
                cur.execute(
                    """
                    UPDATE qmt_strategy.execution_algo_event_delivery
                    SET status='CLAIMED', attempt_count=%s, lease_owner=%s,
                        lease_worker_id=%s, lease_process_incarnation_id=%s, lease_epoch=%s,
                        lease_fence_token=%s, lease_expires_at=%s, next_attempt_at_utc=NULL,
                        row_version=%s, updated_at_utc=%s, carrier_json=%s
                    WHERE delivery_id=%s AND row_version=%s
                    """,
                    (
                        claimed_projection["attempt_count"],
                        claimed_projection["lease_owner"],
                        claimed_projection["lease_worker_id"],
                        claimed_projection["lease_process_incarnation_id"],
                        claimed_projection["lease_epoch"],
                        claimed_projection["lease_fence_token"],
                        claimed_projection["lease_expires_at"],
                        claimed_projection["row_version"],
                        claimed_projection["updated_at_utc"],
                        _json(claimed.model_dump(mode="json")),
                        delivery_id,
                        expected_row_version,
                    ),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("delivery claim CAS failed")
        readback = self.read_delivery(delivery_id)
        if readback != claimed:
            raise KernelRepositoryConflict("delivery claim post-commit readback differs from writer payload")
        return readback

    def claim_outbox_command(
        self,
        *,
        command_id: str,
        lease_owner: str,
        lease_epoch: int,
        lease_fence_token: str,
        lease_expires_at: Any,
        updated_at_utc: Any,
        expected_row_version: int,
    ) -> BrokerCommandOutboxV1:
        self._verify_lease_owner(lease_owner)
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_command_outbox WHERE command_id=%s FOR UPDATE",
                    (command_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(command_id)
                previous = _model_from_json(BrokerCommandOutboxV1, _row_json(row, "carrier_json"))
                if lease_epoch != previous.lease_epoch + 1:
                    raise KernelRepositoryConflict("outbox lease epoch is not the exact durable successor")
                expected_fence = kernel_lease_fence_token_v1(
                    owner_type="OUTBOX_COMMAND",
                    owner_id=command_id,
                    lease_epoch=lease_epoch,
                    lease_owner=lease_owner,
                )
                if lease_fence_token != expected_fence:
                    raise KernelRepositoryConflict("outbox lease fence differs from exact repository authority")
                payload = previous.model_dump(mode="json")
                payload.update(
                    status=BrokerCommandOutboxStatusV1.CLAIMED.value,
                    attempt_count=previous.attempt_count + 1,
                    lease_owner=lease_owner,
                    lease_epoch=lease_epoch,
                    lease_fence_token=lease_fence_token,
                    lease_expires_at=canonical_utc_datetime_v1(lease_expires_at, field_name="lease_expires_at"),
                    row_version=expected_row_version + 1,
                    updated_at_utc=canonical_utc_datetime_v1(updated_at_utc, field_name="updated_at_utc"),
                    next_attempt_at_utc=None,
                )
                payload["outbox_row_sha256"] = self._outbox_hash(payload)
                claimed = _model_from_json(BrokerCommandOutboxV1, payload)
                claimed.validate_successor_v1(previous)
                claimed_projection = _outbox_scalar_projection(claimed)
                cur.execute(
                    """
                    UPDATE qmt_strategy.execution_algo_command_outbox
                    SET status='CLAIMED', attempt_count=%s, lease_owner=%s,
                        lease_worker_id=%s, lease_process_incarnation_id=%s, lease_epoch=%s,
                        lease_fence_token=%s, lease_expires_at=%s, next_attempt_at_utc=NULL,
                        row_version=%s, updated_at_utc=%s, carrier_json=%s, outbox_row_sha256=%s
                    WHERE command_id=%s AND row_version=%s
                    """,
                    (
                        claimed_projection["attempt_count"],
                        claimed_projection["lease_owner"],
                        claimed_projection["lease_worker_id"],
                        claimed_projection["lease_process_incarnation_id"],
                        claimed_projection["lease_epoch"],
                        claimed_projection["lease_fence_token"],
                        claimed_projection["lease_expires_at"],
                        claimed_projection["row_version"],
                        claimed_projection["updated_at_utc"],
                        _json(claimed.model_dump(mode="json")),
                        claimed_projection["outbox_row_sha256"],
                        command_id,
                        expected_row_version,
                    ),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("outbox claim CAS failed")
        readback = self.read_outbox_command(command_id)
        if readback != claimed:
            raise KernelRepositoryConflict("outbox claim post-commit readback differs from writer payload")
        return readback

    def compare_and_swap_algo_instance(
        self,
        *,
        algo_instance: ExecutionAlgoInstancePersistenceV2,
        expected_row_version: int,
    ) -> ExecutionAlgoInstancePersistenceV2:
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._cas_algo_with_cursor(
                    cur,
                    algo_instance=algo_instance,
                    expected_row_version=expected_row_version,
                )
        readback = self.read_algo_instance(algo_instance.algo_instance_id)
        if readback != algo_instance:
            raise KernelRepositoryConflict("algo CAS post-commit readback differs from writer payload")
        return readback

    def append_dispatch_attempt(self, attempt: BrokerDispatchAttemptV1) -> BrokerDispatchAttemptV1:
        if not isinstance(attempt, BrokerDispatchAttemptV1):
            raise TypeError("attempt must be BrokerDispatchAttemptV1")
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_command_outbox WHERE command_id=%s FOR SHARE",
                    (attempt.command_id,),
                )
                outbox_row = cur.fetchone()
                if outbox_row is None:
                    raise KeyError(attempt.command_id)
                outbox = _model_from_json(BrokerCommandOutboxV1, _row_json(outbox_row, "carrier_json"))
                _, separator, process_incarnation_id = (outbox.lease_owner or "").partition(":")
                if (
                    not separator
                    or outbox.attempt_count != attempt.attempt_count
                    or outbox.lease_epoch != attempt.lease_epoch
                    or outbox.lease_fence_token != attempt.lease_fence_token
                    or process_incarnation_id != attempt.process_incarnation_id
                    or (
                        outbox.dispatch_attempt_id is not None
                        and outbox.dispatch_attempt_id != attempt.dispatch_attempt_id
                    )
                ):
                    raise KernelRepositoryConflict("dispatch attempt does not close to the current outbox lease")
                attempt_projection = _dispatch_attempt_scalar_projection(attempt)
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_algo_command_dispatch_attempt(
                        dispatch_attempt_id,stage,command_id,attempt_count,lease_epoch,lease_fence_token,
                        process_incarnation_id,started_at_utc,finished_at_utc,pre_call_complete,
                        broker_called,outcome,error_reason_code,error_context_sha256,
                        authority_receipt_sha256,attempt_receipt_sha256,carrier_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (dispatch_attempt_id,stage) DO NOTHING
                    """,
                    (
                        attempt_projection["dispatch_attempt_id"],
                        attempt_projection["stage"],
                        attempt_projection["command_id"],
                        attempt_projection["attempt_count"],
                        attempt_projection["lease_epoch"],
                        attempt_projection["lease_fence_token"],
                        attempt_projection["process_incarnation_id"],
                        attempt_projection["started_at_utc"],
                        attempt_projection["finished_at_utc"],
                        attempt_projection["pre_call_complete"],
                        attempt_projection["broker_called"],
                        attempt_projection["outcome"],
                        attempt_projection["error_reason_code"],
                        attempt_projection["error_context_sha256"],
                        attempt_projection["authority_receipt_sha256"],
                        attempt_projection["attempt_receipt_sha256"],
                        _json(attempt.model_dump(mode="json")),
                    ),
                )
                cur.execute(
                    """
                    SELECT carrier_json FROM qmt_strategy.execution_algo_command_dispatch_attempt
                    WHERE dispatch_attempt_id=%s AND stage=%s
                    """,
                    (attempt.dispatch_attempt_id, attempt.stage.value),
                )
                row = cur.fetchone()
                persisted = _model_from_json(BrokerDispatchAttemptV1, _row_json(row, "carrier_json"))
                if persisted != attempt:
                    raise KernelRepositoryConflict("dispatch attempt identity exists with different payload")
        readback = self.read_dispatch_attempt(attempt.dispatch_attempt_id, attempt.stage.value)
        if readback != attempt:
            raise KernelRepositoryConflict("dispatch attempt post-commit readback differs from writer payload")
        return readback

    def read_dispatch_attempt(self, dispatch_attempt_id: str, stage: str) -> BrokerDispatchAttemptV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT dispatch_attempt_id,stage,command_id,attempt_count,lease_epoch,
                           lease_fence_token,process_incarnation_id,started_at_utc,finished_at_utc,
                           pre_call_complete,broker_called,outcome,error_reason_code,error_context_sha256,
                           authority_receipt_sha256,attempt_receipt_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_command_dispatch_attempt
                    WHERE dispatch_attempt_id=%s AND stage=%s
                    """,
                    (dispatch_attempt_id, stage),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError((dispatch_attempt_id, stage))
        attempt = _model_from_json(BrokerDispatchAttemptV1, _row_json(row, "carrier_json"))
        _assert_scalar_columns(
            row,
            _dispatch_attempt_scalar_projection(attempt),
            carrier_name="dispatch attempt",
        )
        return attempt

    def write_timer_schedule(self, schedule: ExecutionAlgoTimerScheduleV1) -> ExecutionAlgoTimerScheduleV1:
        if not isinstance(schedule, ExecutionAlgoTimerScheduleV1):
            raise TypeError("schedule must be ExecutionAlgoTimerScheduleV1")
        if schedule.lease_owner is not None:
            self._verify_lease_owner(schedule.lease_owner)
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_timer_schedule WHERE schedule_id=%s FOR UPDATE",
                    (schedule.schedule_id,),
                )
                row = cur.fetchone()
                if row is not None:
                    previous = _model_from_json(ExecutionAlgoTimerScheduleV1, _row_json(row, "carrier_json"))
                    if previous != schedule:
                        schedule.validate_successor_v1(previous)
                else:
                    try:
                        schedule.validate_initial_v1()
                    except ValueError as exc:
                        raise KernelRepositoryConflict(
                            "timer schedule first write requires exact initial state"
                        ) from exc
                schedule_projection = _timer_schedule_scalar_projection(schedule)
                sql_values = (
                    schedule_projection["schedule_id"],
                    schedule_projection["runtime_id"],
                    schedule_projection["algo_instance_id"],
                    schedule_projection["timer_name"],
                    schedule_projection["schedule_epoch"],
                    schedule_projection["due_at_exchange_utc"],
                    schedule_projection["catch_up_policy"],
                    _json(schedule_projection["payload_json"]),
                    schedule_projection["payload_sha256"],
                    schedule_projection["status"],
                    schedule_projection["timer_occurrence_id"],
                    schedule_projection["emitted_event_id"],
                    schedule_projection["lease_owner"],
                    schedule_projection["lease_worker_id"],
                    schedule_projection["lease_process_incarnation_id"],
                    schedule_projection["lease_epoch"],
                    schedule_projection["lease_fence_token"],
                    schedule_projection["lease_expires_at_utc"],
                    schedule_projection["row_version"],
                    schedule_projection["created_at_utc"],
                    schedule_projection["updated_at_utc"],
                    schedule_projection["closed_at_utc"],
                    schedule_projection["schedule_receipt_sha256"],
                    _json(schedule.model_dump(mode="json")),
                )
                if row is None:
                    cur.execute(
                        """
                    INSERT INTO qmt_strategy.execution_algo_timer_schedule(
                        schedule_id,runtime_id,algo_instance_id,timer_name,schedule_epoch,due_at_exchange_utc,
                        catch_up_policy,payload_json,payload_sha256,
                        status,timer_occurrence_id,emitted_event_id,lease_owner,lease_worker_id,
                        lease_process_incarnation_id,lease_epoch,lease_fence_token,lease_expires_at_utc,
                        row_version,created_at_utc,updated_at_utc,closed_at_utc,schedule_receipt_sha256,carrier_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                        """,
                        sql_values,
                    )
                elif previous != schedule:
                    cur.execute(
                        """
                        UPDATE qmt_strategy.execution_algo_timer_schedule
                        SET status=%s,emitted_event_id=%s,lease_owner=%s,lease_worker_id=%s,
                            lease_process_incarnation_id=%s,lease_epoch=%s,lease_fence_token=%s,
                            lease_expires_at_utc=%s,row_version=%s,updated_at_utc=%s,closed_at_utc=%s,
                            schedule_receipt_sha256=%s,carrier_json=%s
                        WHERE schedule_id=%s AND row_version=%s
                          AND lease_owner IS NOT DISTINCT FROM %s
                          AND lease_epoch=%s
                          AND lease_fence_token IS NOT DISTINCT FROM %s
                        """,
                        (
                            schedule_projection["status"],
                            schedule_projection["emitted_event_id"],
                            schedule_projection["lease_owner"],
                            schedule_projection["lease_worker_id"],
                            schedule_projection["lease_process_incarnation_id"],
                            schedule_projection["lease_epoch"],
                            schedule_projection["lease_fence_token"],
                            schedule_projection["lease_expires_at_utc"],
                            schedule_projection["row_version"],
                            schedule_projection["updated_at_utc"],
                            schedule_projection["closed_at_utc"],
                            schedule_projection["schedule_receipt_sha256"],
                            _json(schedule.model_dump(mode="json")),
                            schedule_projection["schedule_id"],
                            previous.row_version,
                            previous.lease_owner,
                            previous.lease_epoch,
                            previous.lease_fence_token,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise KernelRepositoryConflict("timer schedule CAS failed")
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_timer_schedule WHERE schedule_id=%s",
                    (schedule.schedule_id,),
                )
                persisted_row = cur.fetchone()
                if persisted_row is None:
                    raise KernelRepositoryConflict("timer schedule write did not persist its identity")
                persisted = _model_from_json(ExecutionAlgoTimerScheduleV1, _row_json(persisted_row, "carrier_json"))
                if persisted != schedule:
                    raise KernelRepositoryConflict("timer schedule identity exists with different immutable payload")
        readback = self.read_timer_schedule(schedule.schedule_id)
        if readback != schedule:
            raise KernelRepositoryConflict("timer schedule post-commit readback differs from writer payload")
        return readback

    def write_timer_occurrence(self, occurrence: ExecutionAlgoTimerOccurrenceV1) -> ExecutionAlgoTimerOccurrenceV1:
        if not isinstance(occurrence, ExecutionAlgoTimerOccurrenceV1):
            raise TypeError("occurrence must be ExecutionAlgoTimerOccurrenceV1")
        if occurrence.lease_owner is not None:
            self._verify_lease_owner(occurrence.lease_owner)
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_timer_occurrence WHERE timer_occurrence_id=%s FOR UPDATE",
                    (occurrence.timer_occurrence_id,),
                )
                row = cur.fetchone()
                if row is not None:
                    previous = _model_from_json(ExecutionAlgoTimerOccurrenceV1, _row_json(row, "carrier_json"))
                    if previous != occurrence:
                        occurrence.validate_successor_v1(previous)
                else:
                    try:
                        occurrence.validate_initial_v1()
                    except ValueError as exc:
                        raise KernelRepositoryConflict(
                            "timer occurrence first write requires exact initial state"
                        ) from exc
                occurrence_projection = _timer_occurrence_scalar_projection(occurrence)
                sql_values = (
                    occurrence_projection["timer_occurrence_id"],
                    occurrence_projection["schedule_id"],
                    occurrence_projection["runtime_id"],
                    occurrence_projection["algo_instance_id"],
                    occurrence_projection["due_at_exchange_utc"],
                    occurrence_projection["exchange_session_authority_sha256"],
                    occurrence_projection["status"],
                    occurrence_projection["emitted_event_id"],
                    occurrence_projection["catch_up_receipt_sha256"],
                    occurrence_projection["lease_owner"],
                    occurrence_projection["lease_worker_id"],
                    occurrence_projection["lease_process_incarnation_id"],
                    occurrence_projection["lease_epoch"],
                    occurrence_projection["lease_fence_token"],
                    occurrence_projection["lease_expires_at_utc"],
                    occurrence_projection["row_version"],
                    occurrence_projection["created_at_utc"],
                    occurrence_projection["closed_at_utc"],
                    occurrence_projection["occurrence_receipt_sha256"],
                    _json(occurrence.model_dump(mode="json")),
                )
                if row is None:
                    cur.execute(
                        """
                    INSERT INTO qmt_strategy.execution_algo_timer_occurrence(
                        timer_occurrence_id,schedule_id,runtime_id,algo_instance_id,due_at_exchange_utc,
                        exchange_session_authority_sha256,status,emitted_event_id,catch_up_receipt_sha256,
                        lease_owner,lease_worker_id,
                        lease_process_incarnation_id,lease_epoch,lease_fence_token,lease_expires_at_utc,row_version,
                        created_at_utc,closed_at_utc,occurrence_receipt_sha256,carrier_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                        """,
                        sql_values,
                    )
                elif previous != occurrence:
                    cur.execute(
                        """
                        UPDATE qmt_strategy.execution_algo_timer_occurrence
                        SET status=%s,emitted_event_id=%s,catch_up_receipt_sha256=%s,
                            lease_owner=%s,lease_worker_id=%s,
                            lease_process_incarnation_id=%s,lease_epoch=%s,lease_fence_token=%s,
                            lease_expires_at_utc=%s,row_version=%s,closed_at_utc=%s,
                            occurrence_receipt_sha256=%s,carrier_json=%s
                        WHERE timer_occurrence_id=%s AND row_version=%s
                          AND lease_owner IS NOT DISTINCT FROM %s
                          AND lease_epoch=%s
                          AND lease_fence_token IS NOT DISTINCT FROM %s
                        """,
                        (
                            occurrence_projection["status"],
                            occurrence_projection["emitted_event_id"],
                            occurrence_projection["catch_up_receipt_sha256"],
                            occurrence_projection["lease_owner"],
                            occurrence_projection["lease_worker_id"],
                            occurrence_projection["lease_process_incarnation_id"],
                            occurrence_projection["lease_epoch"],
                            occurrence_projection["lease_fence_token"],
                            occurrence_projection["lease_expires_at_utc"],
                            occurrence_projection["row_version"],
                            occurrence_projection["closed_at_utc"],
                            occurrence_projection["occurrence_receipt_sha256"],
                            _json(occurrence.model_dump(mode="json")),
                            occurrence_projection["timer_occurrence_id"],
                            previous.row_version,
                            previous.lease_owner,
                            previous.lease_epoch,
                            previous.lease_fence_token,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise KernelRepositoryConflict("timer occurrence CAS failed")
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_timer_occurrence WHERE timer_occurrence_id=%s",
                    (occurrence.timer_occurrence_id,),
                )
                persisted_row = cur.fetchone()
                if persisted_row is None:
                    raise KernelRepositoryConflict("timer occurrence write did not persist its identity")
                persisted = _model_from_json(ExecutionAlgoTimerOccurrenceV1, _row_json(persisted_row, "carrier_json"))
                if persisted != occurrence:
                    raise KernelRepositoryConflict("timer occurrence identity exists with different immutable payload")
        readback = self.read_timer_occurrence(occurrence.timer_occurrence_id)
        if readback != occurrence:
            raise KernelRepositoryConflict("timer occurrence post-commit readback differs from writer payload")
        return readback

    def read_timer_schedule(self, schedule_id: str) -> ExecutionAlgoTimerScheduleV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT schedule_id,runtime_id,algo_instance_id,timer_name,schedule_epoch,
                           due_at_exchange_utc,catch_up_policy,payload_json,payload_sha256,
                           status,timer_occurrence_id,emitted_event_id,
                           lease_owner,lease_worker_id,lease_process_incarnation_id,lease_epoch,
                           lease_fence_token,lease_expires_at_utc,row_version,created_at_utc,
                           updated_at_utc,closed_at_utc,schedule_receipt_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_timer_schedule WHERE schedule_id=%s
                    """,
                    (schedule_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(schedule_id)
        schedule = _model_from_json(ExecutionAlgoTimerScheduleV1, _row_json(row, "carrier_json"))
        _assert_scalar_columns(
            row,
            _timer_schedule_scalar_projection(schedule),
            carrier_name="timer schedule",
        )
        return schedule

    def read_timer_occurrence(self, timer_occurrence_id: str) -> ExecutionAlgoTimerOccurrenceV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT timer_occurrence_id,schedule_id,runtime_id,algo_instance_id,due_at_exchange_utc,
                           exchange_session_authority_sha256,status,emitted_event_id,
                           catch_up_receipt_sha256,lease_owner,
                           lease_worker_id,lease_process_incarnation_id,lease_epoch,lease_fence_token,
                           lease_expires_at_utc,row_version,created_at_utc,closed_at_utc,
                           occurrence_receipt_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_timer_occurrence WHERE timer_occurrence_id=%s
                    """,
                    (timer_occurrence_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(timer_occurrence_id)
        occurrence = _model_from_json(ExecutionAlgoTimerOccurrenceV1, _row_json(row, "carrier_json"))
        _assert_scalar_columns(
            row,
            _timer_occurrence_scalar_projection(occurrence),
            carrier_name="timer occurrence",
        )
        return occurrence

    def write_exchange_session_authority(self, authority: ExchangeSessionAuthorityV1) -> ExchangeSessionAuthorityV1:
        if not isinstance(authority, ExchangeSessionAuthorityV1):
            raise TypeError("authority must be ExchangeSessionAuthorityV1")
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT trade_date FROM qmt_strategy.execution_runtime WHERE runtime_id=%s FOR SHARE",
                    (authority.runtime_id,),
                )
                runtime_row = cur.fetchone()
                if runtime_row is None:
                    raise KeyError(authority.runtime_id)
                if runtime_row["trade_date"] != date.fromisoformat(authority.exchange_trade_date):
                    raise KernelRepositoryConflict("exchange-session trade date conflicts with runtime owner")
                authority_projection = _exchange_session_scalar_projection(authority)
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_exchange_session_authority(
                        runtime_id,exchange_trade_date,calendar_snapshot_set_id,calendar_snapshot_set_sha256,
                        session_definition_version,authority_sha256,authority_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (runtime_id,exchange_trade_date) DO NOTHING
                    """,
                    (
                        authority_projection["runtime_id"],
                        authority_projection["exchange_trade_date"],
                        authority_projection["calendar_snapshot_set_id"],
                        authority_projection["calendar_snapshot_set_sha256"],
                        authority_projection["session_definition_version"],
                        authority_projection["authority_sha256"],
                        _json(authority.model_dump(mode="json")),
                    ),
                )
                cur.execute(
                    """
                    SELECT authority_json FROM qmt_strategy.execution_exchange_session_authority
                    WHERE runtime_id=%s AND exchange_trade_date=%s
                    """,
                    (authority.runtime_id, authority.exchange_trade_date),
                )
                persisted = _model_from_json(ExchangeSessionAuthorityV1, _row_json(cur.fetchone(), "authority_json"))
                if persisted != authority:
                    raise KernelRepositoryConflict("exchange-session authority drift for runtime/trade date")
        readback = self.read_exchange_session_authority(
            runtime_id=authority.runtime_id,
            exchange_trade_date=date.fromisoformat(authority.exchange_trade_date),
        )
        if readback != authority:
            raise KernelRepositoryConflict("exchange-session post-commit readback differs from writer payload")
        return readback

    def read_exchange_session_authority(
        self, *, runtime_id: str, exchange_trade_date: date
    ) -> ExchangeSessionAuthorityV1:
        if type(exchange_trade_date) is not date:
            raise TypeError("exchange_trade_date must be a date")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT authority.runtime_id,authority.exchange_trade_date,
                           authority.calendar_snapshot_set_id,authority.calendar_snapshot_set_sha256,
                           authority.session_definition_version,authority.authority_sha256,
                           authority.authority_json,runtime.trade_date AS runtime_trade_date
                    FROM qmt_strategy.execution_exchange_session_authority AS authority
                    JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=authority.runtime_id
                    WHERE authority.runtime_id=%s AND authority.exchange_trade_date=%s
                    """,
                    (runtime_id, exchange_trade_date),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError((runtime_id, exchange_trade_date))
        authority = _model_from_json(ExchangeSessionAuthorityV1, _row_json(row, "authority_json"))
        _assert_scalar_columns(
            row,
            _exchange_session_scalar_projection(authority),
            carrier_name="exchange-session authority",
        )
        if row["exchange_trade_date"] != row["runtime_trade_date"]:
            raise KernelRepositoryConflict("exchange-session trade date drifts from runtime owner")
        return authority

    def list_recovery_deliveries(
        self, *, runtime_id: str, trade_date: date, statuses: Sequence[str], limit: int
    ) -> tuple[AlgoDeliveryPersistenceV1, ...]:
        delivery_ids = self._recovery_identities(
            table="execution_algo_event_delivery",
            runtime_id=runtime_id,
            trade_date=trade_date,
            statuses=statuses,
            limit=limit,
        )
        return tuple(self.read_delivery(delivery_id) for delivery_id in delivery_ids)

    def list_recovery_outbox_commands(
        self, *, runtime_id: str, trade_date: date, statuses: Sequence[str], limit: int
    ) -> tuple[BrokerCommandOutboxV1, ...]:
        command_ids = self._recovery_identities(
            table="execution_algo_command_outbox",
            runtime_id=runtime_id,
            trade_date=trade_date,
            statuses=statuses,
            limit=limit,
        )
        return tuple(self.read_outbox_command(command_id) for command_id in command_ids)

    def list_recovery_timer_occurrences(
        self, *, runtime_id: str, trade_date: date, statuses: Sequence[str], limit: int
    ) -> tuple[ExecutionAlgoTimerOccurrenceV1, ...]:
        occurrence_ids = self._recovery_identities(
            table="execution_algo_timer_occurrence",
            runtime_id=runtime_id,
            trade_date=trade_date,
            statuses=statuses,
            limit=limit,
        )
        return tuple(self.read_timer_occurrence(occurrence_id) for occurrence_id in occurrence_ids)

    def _cas_algo_with_cursor(
        self,
        cur: Any,
        *,
        algo_instance: ExecutionAlgoInstancePersistenceV2,
        expected_row_version: int,
    ) -> None:
        cur.execute(
            """
            SELECT kernel_carrier_json
            FROM qmt_strategy.execution_algo_instance
            WHERE algo_instance_id=%s AND kernel_contract_version='KERNEL_V2'
            FOR UPDATE
            """,
            (algo_instance.algo_instance_id,),
        )
        row = cur.fetchone()
        if row is not None:
            previous = _model_from_json(ExecutionAlgoInstancePersistenceV2, _row_json(row, "kernel_carrier_json"))
            algo_instance.validate_successor_v1(previous)
        elif expected_row_version != 0 or algo_instance.row_version != 1:
            raise KernelRepositoryConflict("algo insert requires expected version 0 and row version 1")
        cur.execute(
            """
            SELECT COUNT(*) AS active_child_count
            FROM qmt_strategy.execution_child_order
            WHERE runtime_id=%s AND algo_instance_id=%s AND kernel_contract_version='KERNEL_V2'
              AND mapping_status IN ('RESERVED','DISPATCHING','BROKER_ACCEPTED','OUTCOME_UNKNOWN')
            """,
            (algo_instance.runtime_id, algo_instance.algo_instance_id),
        )
        active_count = int(cur.fetchone()["active_child_count"])
        if algo_instance.active_child_count != active_count:
            raise KernelRepositoryConflict("active_child_count does not match durable mapping reconstruction")
        values = algo_instance.model_dump(mode="json")
        projection = _algo_scalar_projection(algo_instance)
        cur.execute(
            """
            INSERT INTO qmt_strategy.execution_algo_instance(
                algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,
                remaining_quantity,algo_code,status,metadata,created_at,updated_at,kernel_contract_version,
                traded_quantity,plugin_id,plugin_version,plugin_manifest_sha256,plugin_config_json,
                plugin_config_sha256,compatibility_receipt_sha256,state_schema_version,state_json,state_sha256,
                transition_sequence,last_applied_delivery_sequence,last_applied_delivery_id,
                last_closed_delivery_sequence,terminal_delivery_sequence,failure_receipt_id,
                active_child_closure_status,active_child_count,row_version,terminal_at_utc,kernel_carrier_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'{}'::jsonb,%s,%s,'KERNEL_V2',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (algo_instance_id) DO UPDATE SET
                remaining_quantity=EXCLUDED.remaining_quantity,status=EXCLUDED.status,updated_at=EXCLUDED.updated_at,
                traded_quantity=EXCLUDED.traded_quantity,state_schema_version=EXCLUDED.state_schema_version,
                state_json=EXCLUDED.state_json,state_sha256=EXCLUDED.state_sha256,
                transition_sequence=EXCLUDED.transition_sequence,
                last_applied_delivery_sequence=EXCLUDED.last_applied_delivery_sequence,
                last_applied_delivery_id=EXCLUDED.last_applied_delivery_id,
                last_closed_delivery_sequence=EXCLUDED.last_closed_delivery_sequence,
                terminal_delivery_sequence=EXCLUDED.terminal_delivery_sequence,
                failure_receipt_id=EXCLUDED.failure_receipt_id,
                active_child_closure_status=EXCLUDED.active_child_closure_status,
                active_child_count=EXCLUDED.active_child_count,row_version=EXCLUDED.row_version,
                terminal_at_utc=EXCLUDED.terminal_at_utc,kernel_carrier_json=EXCLUDED.kernel_carrier_json
            WHERE qmt_strategy.execution_algo_instance.row_version=%s
            """,
            (
                projection["algo_instance_id"],
                projection["runtime_id"],
                projection["parent_intent_id"],
                projection["strategy_slot_id"],
                projection["symbol"],
                projection["side"],
                projection["target_quantity"],
                projection["remaining_quantity"],
                projection["algo_code"],
                projection["status"],
                projection["created_at"],
                projection["updated_at"],
                projection["traded_quantity"],
                projection["plugin_id"],
                projection["plugin_version"],
                projection["plugin_manifest_sha256"],
                _json(projection["plugin_config_json"]),
                projection["plugin_config_sha256"],
                projection["compatibility_receipt_sha256"],
                projection["state_schema_version"],
                None if projection["state_json"] is None else _json(projection["state_json"]),
                projection["state_sha256"],
                projection["transition_sequence"],
                projection["last_applied_delivery_sequence"],
                projection["last_applied_delivery_id"],
                projection["last_closed_delivery_sequence"],
                projection["terminal_delivery_sequence"],
                projection["failure_receipt_id"],
                projection["active_child_closure_status"],
                projection["active_child_count"],
                projection["row_version"],
                projection["terminal_at_utc"],
                _json(values),
                expected_row_version,
            ),
        )
        if cur.rowcount != 1:
            raise KernelRepositoryConflict("algo instance CAS failed")

    def _verify_lease_owner(self, lease_owner: str) -> None:
        worker_id, separator, process_incarnation_id = lease_owner.partition(":")
        if not separator or not worker_id or not process_incarnation_id:
            raise ValueError("lease_owner must be worker_id:process_incarnation_id")
        with self._connection(transaction=False) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM qmt_strategy.execution_kernel_worker_incarnation
                    WHERE worker_id=%s AND process_incarnation_id=%s
                    """,
                    (worker_id, process_incarnation_id),
                )
                exists = cur.fetchone() is not None
        if not exists:
            raise KernelRepositoryConflict("lease owner references unknown worker incarnation")

    def _recovery_identities(
        self,
        *,
        table: str,
        runtime_id: str,
        trade_date: date,
        statuses: Sequence[str],
        limit: int,
    ) -> tuple[str, ...]:
        limit = _bounded_limit(limit)
        if type(runtime_id) is not str or not runtime_id or runtime_id != runtime_id.strip():
            raise ValueError("runtime_id must be a non-empty trim-stable strict string")
        if type(trade_date) is not date:
            raise TypeError("trade_date must be an exact date")
        exact_statuses = tuple(statuses)
        if not exact_statuses or any(type(item) is not str or not item.strip() for item in exact_statuses):
            raise ValueError("recovery statuses must be a non-empty strict string sequence")
        if len(set(exact_statuses)) != len(exact_statuses):
            raise ValueError("recovery statuses must not contain duplicates")
        table_authority = {
            "execution_algo_event_delivery": (
                {status.value for status in DeliveryStatusV1},
                "delivery_id",
                "target.created_at_utc, target.algo_delivery_sequence, target.delivery_id",
            ),
            "execution_algo_command_outbox": (
                {status.value for status in BrokerCommandOutboxStatusV1},
                "command_id",
                "target.next_attempt_at_utc NULLS FIRST, target.created_at_utc, target.command_id",
            ),
            "execution_algo_timer_occurrence": (
                {status.value for status in ExecutionAlgoTimerOccurrenceStatusV1},
                "timer_occurrence_id",
                "target.due_at_exchange_utc, target.created_at_utc, target.timer_occurrence_id",
            ),
        }
        if table not in table_authority:
            raise ValueError("unsupported recovery table")
        allowed_statuses, identity_column, order_by = table_authority[table]
        invalid = tuple(status for status in exact_statuses if status not in allowed_statuses)
        if invalid:
            raise ValueError(f"unsupported recovery statuses for {table}: {invalid}")
        query = f"""
            SELECT target.{identity_column} AS recovery_identity
            FROM qmt_strategy.{table} AS target
            JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=target.runtime_id
            WHERE target.runtime_id=%s AND runtime.trade_date=%s AND target.status=ANY(%s::text[])
            ORDER BY {order_by}
            LIMIT %s
        """
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (runtime_id, trade_date, list(exact_statuses), limit))
                rows = cur.fetchall()
        return tuple(str(row["recovery_identity"]) for row in rows)

    @staticmethod
    def _outbox_sql_values(outbox: BrokerCommandOutboxV1) -> tuple[Any, ...]:
        values = outbox.model_dump(mode="json")
        projection = _outbox_scalar_projection(outbox)
        return (
            projection["command_id"],
            projection["transition_id"],
            projection["ordinal"],
            projection["runtime_id"],
            projection["algo_instance_id"],
            projection["parent_intent_id"],
            projection["mapping_id"],
            projection["command_type"],
            projection["local_vt_orderid"],
            _json(projection["payload_json"]),
            projection["payload_sha256"],
            projection["status"],
            projection["attempt_count"],
            projection["lease_owner"],
            projection["lease_epoch"],
            projection["lease_fence_token"],
            projection["lease_expires_at"],
            projection["dispatch_attempt_id"],
            projection["deterministic_client_order_ref"],
            projection["next_attempt_at_utc"],
            projection["broker_called"],
            projection["broker_order_id"],
            None if projection["ack_receipt_json"] is None else _json(projection["ack_receipt_json"]),
            projection["ack_receipt_sha256"],
            None
            if projection["non_acceptance_receipt_json"] is None
            else _json(projection["non_acceptance_receipt_json"]),
            None
            if projection["unknown_outcome_receipt_json"] is None
            else _json(projection["unknown_outcome_receipt_json"]),
            None if projection["reconcile_receipt_json"] is None else _json(projection["reconcile_receipt_json"]),
            None if projection["last_error_json"] is None else _json(projection["last_error_json"]),
            projection["row_version"],
            projection["created_at_utc"],
            projection["updated_at_utc"],
            projection["closed_at_utc"],
            _json(values),
            projection["outbox_row_sha256"],
        )

    @staticmethod
    def _outbox_hash(payload: dict[str, Any]) -> str:
        from .plugin_canonical import hash_hex_v1

        return hash_hex_v1(
            "miniqmt_broker_command_outbox_row_v1",
            {key: value for key, value in payload.items() if key != "outbox_row_sha256"},
        )


__all__ = [
    "KernelRepositoryCommitUnknown",
    "KernelRepositoryConflict",
    "KernelRepositorySchemaError",
    "PostgresMiniQMTKernelRepository",
]
