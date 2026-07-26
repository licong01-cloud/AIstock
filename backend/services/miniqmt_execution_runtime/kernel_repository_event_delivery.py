"""Runtime-event and delivery transactions for the K2-A repository facade."""

from __future__ import annotations

from typing import Any, Sequence

import psycopg2
import psycopg2.extras

from .kernel_repository_common import (
    KernelRepositoryConflict,
    _json,
    _model_from_json,
    _row_json,
)
from .plugin_canonical import canonical_utc_datetime_v1
from .kernel_repository_projection import (
    _assert_scalar_columns,
    _delivery_creation_matches,
    _delivery_scalar_projection,
    _event_scalar_projection,
)
from .plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    DeliveryStatusV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
    kernel_lease_fence_token_v1,
    transaction_commit_identity_v1,
)


class KernelRepositoryEventDeliveryMixin:
    """Own event ingress, delivery readback, claim, and atomicity operations."""

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
