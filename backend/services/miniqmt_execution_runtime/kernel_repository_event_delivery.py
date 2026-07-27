"""Runtime-event and delivery transactions for the K2-A repository facade."""

from __future__ import annotations

from datetime import datetime, timedelta
import json
from typing import Any, Sequence

import psycopg2
import psycopg2.extras

from .kernel_repository_common import (
    KernelRepositoryConflict,
    _json,
    _model_from_json,
    _row_json,
)
from .plugin_canonical import canonical_utc_datetime_v1, thaw_json_v1
from .kernel_repository_projection import (
    _assert_scalar_columns,
    _delivery_scalar_projection,
    _event_scalar_projection,
    _mapping_scalar_projection,
)
from .kernel_ingress import route_event_targets_v1
from .plugin_registry import PluginCatalogRuntimeV2
from .plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    BrokerCommandOutboxV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CommandChildMappingStatusV1,
    DeliveryStatusV1,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
    KernelErrorEvidenceV1,
    KernelCallbackMappingUpdateV1,
    ExecutionCommandChildMappingV1,
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

    def read_delivery_tail(self, *, runtime_id: str, algo_instance_id: str) -> AlgoDeliveryPersistenceV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT delivery_id FROM qmt_strategy.execution_algo_event_delivery
                    WHERE runtime_id=%s AND algo_instance_id=%s
                    ORDER BY algo_delivery_sequence DESC LIMIT 1
                    """,
                    (runtime_id, algo_instance_id),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError((runtime_id, algo_instance_id))
        delivery = self.read_delivery(str(row["delivery_id"]))
        if delivery.runtime_id != runtime_id or delivery.algo_instance_id != algo_instance_id:
            raise KernelRepositoryConflict("delivery tail readback owner drift")
        return delivery

    @staticmethod
    def _validate_callback_mapping_update(
        *, event: RuntimeEventEnvelopeV2, update: KernelCallbackMappingUpdateV1 | None
    ) -> None:
        callback_sources = {
            EventTypeV2.ORDER: "QMT_GATEWAY_CALLBACK",
            EventTypeV2.TRADE: "QMT_GATEWAY_CALLBACK",
            EventTypeV2.RECONCILE: "QMT_OMS_RECONCILIATION",
        }
        if event.event_type not in callback_sources:
            if update is not None:
                raise ValueError("non-callback event cannot carry a mapping update")
            return
        if not isinstance(update, KernelCallbackMappingUpdateV1):
            raise ValueError("callback event requires one strict mapping update")
        mapping = update.mapping
        if event.source.value != callback_sources[event.event_type]:
            raise ValueError("callback event source conflicts with mapping update authority")
        payload = thaw_json_v1(event.payload)
        expected_payload = {
            "runtime_id": mapping.runtime_id,
            "algo_instance_id": mapping.algo_instance_id,
            "parent_intent_id": mapping.parent_intent_id,
            "mapping_id": mapping.mapping_id,
            "local_vt_orderid": mapping.local_vt_orderid,
            "broker_order_id": mapping.broker_order_id,
            "terminal": mapping.mapping_status is CommandChildMappingStatusV1.TERMINAL,
        }
        if any(payload.get(key) != value for key, value in expected_payload.items()):
            raise ValueError("callback event payload conflicts with mapping update authority")
        if mapping.updated_by_event_id != event.event_id:
            raise ValueError("callback mapping successor must reference the exact ingress event")
        if event.event_type is EventTypeV2.ORDER and mapping.last_order_event_id != event.event_id:
            raise ValueError("ORDER callback mapping does not retain exact order event lineage")
        if event.event_type is EventTypeV2.TRADE and mapping.last_trade_event_id != event.event_id:
            raise ValueError("TRADE callback mapping does not retain exact trade event lineage")

    def _apply_callback_mapping_update_with_cursor(
        self,
        cur: Any,
        *,
        event: RuntimeEventEnvelopeV2,
        update: KernelCallbackMappingUpdateV1,
    ) -> tuple[ExecutionCommandChildMappingV1, BrokerCommandOutboxV1, ExecutionAlgoInstancePersistenceV2]:
        mapping = update.mapping
        cur.execute(
            """
            SELECT child.mapping_json,outbox.carrier_json AS outbox_json,
                   algo.kernel_carrier_json AS algo_json
            FROM qmt_strategy.execution_child_order AS child
            JOIN qmt_strategy.execution_algo_command_outbox AS outbox
              ON outbox.command_id=%s AND outbox.mapping_id=child.mapping_id
            JOIN qmt_strategy.execution_algo_instance AS algo
              ON algo.runtime_id=child.runtime_id AND algo.algo_instance_id=child.algo_instance_id
            WHERE child.mapping_id=%s AND child.kernel_contract_version='KERNEL_V2'
            FOR UPDATE OF child,outbox,algo
            """,
            (update.reference_command_id, mapping.mapping_id),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError((mapping.mapping_id, update.reference_command_id))
        previous_mapping = _model_from_json(ExecutionCommandChildMappingV1, _row_json(row, "mapping_json"))
        unchanged_outbox = _model_from_json(BrokerCommandOutboxV1, _row_json(row, "outbox_json"))
        previous_algo = _model_from_json(ExecutionAlgoInstancePersistenceV2, _row_json(row, "algo_json"))
        reference_command = BrokerCommandV2.model_validate_json(
            json.dumps(
                thaw_json_v1(unchanged_outbox.payload_json),
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        if (
            unchanged_outbox.mapping_id != mapping.mapping_id
            or unchanged_outbox.runtime_id != mapping.runtime_id
            or unchanged_outbox.algo_instance_id != mapping.algo_instance_id
            or unchanged_outbox.parent_intent_id != mapping.parent_intent_id
            or previous_algo.runtime_id != event.runtime_id
            or previous_algo.algo_instance_id != mapping.algo_instance_id
        ):
            raise ValueError("callback reference command owner conflicts with mapping update")
        if reference_command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            if reference_command.command_id != mapping.command_id:
                raise ValueError("callback SUBMIT reference does not own the durable mapping")
        elif reference_command.command_type is BrokerCommandTypeV2.CANCEL_ORDER:
            if (
                reference_command.local_vt_orderid != mapping.local_vt_orderid
                or reference_command.owned_broker_order_id != mapping.broker_order_id
            ):
                raise ValueError("callback CANCEL reference conflicts with mapped broker identity")
        else:  # pragma: no cover - current command enum exhaustiveness
            raise ValueError("callback reference command type is unsupported")
        if previous_mapping.mapping_version != update.expected_mapping_version:
            raise KernelRepositoryConflict("callback mapping CAS expected version differs from durable predecessor")
        if previous_algo.row_version != update.expected_algo_row_version:
            raise KernelRepositoryConflict("callback algo CAS expected version differs from durable predecessor")
        mapping.validate_successor_v1(previous_mapping)
        cur.execute(
            """
            SELECT COUNT(*) AS conflict_count
            FROM qmt_strategy.execution_child_order
            WHERE runtime_id=%s AND algo_instance_id=%s AND parent_intent_id=%s
              AND (local_vt_orderid=%s OR deterministic_client_order_ref=%s
                   OR (broker_order_id IS NOT NULL AND broker_order_id=%s))
              AND mapping_id<>%s
            """,
            (
                mapping.runtime_id,
                mapping.algo_instance_id,
                mapping.parent_intent_id,
                mapping.local_vt_orderid,
                mapping.deterministic_client_order_ref,
                mapping.broker_order_id,
                mapping.mapping_id,
            ),
        )
        if int(cur.fetchone()["conflict_count"]) != 0:
            raise KernelRepositoryConflict("callback identity matches multiple durable child mappings")
        projection = _mapping_scalar_projection(mapping)
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
                projection["broker_order_id"],
                projection["broker_identity_source_event_id"],
                projection["mapping_status"],
                projection["mapping_version"],
                projection["mapping_payload_sha256"],
                projection["mapping_receipt_sha256"],
                projection["last_order_event_id"],
                projection["last_trade_event_id"],
                projection["updated_by_event_id"],
                projection["mapping_updated_at_utc"],
                projection["updated_at"],
                _json(mapping.model_dump(mode="json")),
                mapping.mapping_id,
                update.expected_mapping_version,
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
        return mapping, unchanged_outbox, updated_algo

    @staticmethod
    def _assert_callback_mapping_update_readback_with_cursor(
        cur: Any, *, update: KernelCallbackMappingUpdateV1
    ) -> None:
        cur.execute(
            """
            SELECT child.mapping_json,outbox.carrier_json AS outbox_json
            FROM qmt_strategy.execution_child_order AS child
            JOIN qmt_strategy.execution_algo_command_outbox AS outbox
              ON outbox.command_id=%s AND outbox.mapping_id=child.mapping_id
            WHERE child.mapping_id=%s AND child.kernel_contract_version='KERNEL_V2'
            """,
            (update.reference_command_id, update.mapping.mapping_id),
        )
        row = cur.fetchone()
        if row is None:
            raise KernelRepositoryConflict("idempotent callback mapping readback is incomplete")
        persisted_mapping = _model_from_json(ExecutionCommandChildMappingV1, _row_json(row, "mapping_json"))
        persisted_outbox = _model_from_json(BrokerCommandOutboxV1, _row_json(row, "outbox_json"))
        if persisted_mapping != update.mapping or persisted_outbox.mapping_id != update.mapping.mapping_id:
            raise KernelRepositoryConflict("idempotent callback mapping readback differs from update authority")

    def ingest_routed_event_atomic(
        self,
        *,
        event: RuntimeEventEnvelopeV2,
        catalog_runtime: PluginCatalogRuntimeV2,
        correlated_algo_instance_ids: tuple[str, ...],
        callback_mapping_update: KernelCallbackMappingUpdateV1 | None = None,
    ) -> RuntimeEventIngressReceiptV1:
        """Append one externally sourced event and its complete ordered fan-out."""

        if not isinstance(event, RuntimeEventEnvelopeV2):
            raise TypeError("event must be RuntimeEventEnvelopeV2")
        if not isinstance(catalog_runtime, PluginCatalogRuntimeV2):
            raise TypeError("catalog_runtime must be PluginCatalogRuntimeV2")
        if not isinstance(correlated_algo_instance_ids, tuple) or any(
            type(item) is not str or not item.strip() for item in correlated_algo_instance_ids
        ):
            raise TypeError("correlated_algo_instance_ids must be a strict tuple of identities")
        if event.event_type is EventTypeV2.ALGO_START:
            raise ValueError("ALGO_START must use the dedicated atomic initialization transaction")
        self._validate_callback_mapping_update(event=event, update=callback_mapping_update)
        callback_input_hashes = () if callback_mapping_update is None else (callback_mapping_update.update_sha256,)
        callback_output_identities = (
            () if callback_mapping_update is None else (callback_mapping_update.mapping.mapping_id,)
        )
        existing_receipt: RuntimeEventIngressReceiptV1 | None = None
        receipt: RuntimeEventIngressReceiptV1 | None = None
        ordered: tuple[AlgoDeliveryPersistenceV1, ...] = ()
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT last_event_sequence,archived_at FROM qmt_strategy.execution_runtime "
                    "WHERE runtime_id=%s FOR UPDATE",
                    (event.runtime_id,),
                )
                runtime_row = cur.fetchone()
                if runtime_row is None:
                    raise KeyError(event.runtime_id)
                if runtime_row["archived_at"] is not None:
                    raise KernelRepositoryConflict("cannot append a K2 event to an archived runtime")
                cur.execute(
                    "SELECT payload,ingress_receipt_json FROM qmt_strategy.execution_runtime_event "
                    "WHERE runtime_id=%s AND event_key_sha256=%s AND event_contract_version='KERNEL_V2'",
                    (event.runtime_id, event.event_key_sha256),
                )
                existing_event_row = cur.fetchone()
                if existing_event_row is not None:
                    persisted_event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(existing_event_row, "payload"))
                    existing_receipt = _model_from_json(
                        RuntimeEventIngressReceiptV1,
                        _row_json(existing_event_row, "ingress_receipt_json"),
                    )
                    if persisted_event != event:
                        raise KernelRepositoryConflict(
                            "event key exists with different immutable envelope, sequence, payload or correlation"
                        )
                    if int(runtime_row["last_event_sequence"]) < persisted_event.sequence:
                        raise KernelRepositoryConflict(
                            "durable runtime sequence regressed behind an existing routed event"
                        )
                    if callback_mapping_update is not None:
                        self._assert_callback_mapping_update_readback_with_cursor(cur, update=callback_mapping_update)
                else:
                    last_sequence = int(runtime_row["last_event_sequence"])
                    if event.sequence != last_sequence + 1:
                        raise KernelRepositoryConflict("event sequence is not the exact runtime successor")
                    if callback_mapping_update is not None:
                        self._apply_callback_mapping_update_with_cursor(
                            cur,
                            event=event,
                            update=callback_mapping_update,
                        )
                    cur.execute(
                        "SELECT kernel_carrier_json FROM qmt_strategy.execution_algo_instance "
                        "WHERE runtime_id=%s AND kernel_contract_version='KERNEL_V2' "
                        "ORDER BY algo_instance_id FOR UPDATE",
                        (event.runtime_id,),
                    )
                    durable_algos = tuple(
                        _model_from_json(
                            ExecutionAlgoInstancePersistenceV2,
                            _row_json(row, "kernel_carrier_json"),
                        )
                        for row in cur.fetchall()
                    )
                    targets = route_event_targets_v1(
                        event=event,
                        algo_instances=durable_algos,
                        catalog_runtime=catalog_runtime,
                        correlated_algo_instance_ids=correlated_algo_instance_ids,
                    )
                    if (
                        not isinstance(targets, tuple)
                        or any(type(item) is not str or not item.strip() for item in targets)
                        or targets != tuple(sorted(targets))
                        or len(targets) != len(set(targets))
                    ):
                        raise ValueError("code-owned routing must return one canonical unique tuple of algo identities")
                    by_id = {item.algo_instance_id: item for item in durable_algos}
                    missing = sorted(set(targets) - set(by_id))
                    if missing:
                        raise KernelRepositoryConflict(
                            f"routing target does not belong to the locked runtime: {missing}"
                        )
                    built: list[AlgoDeliveryPersistenceV1] = []
                    for algo_instance_id in targets:
                        algo = by_id[algo_instance_id]
                        cur.execute(
                            "SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery "
                            "WHERE runtime_id=%s AND algo_instance_id=%s "
                            "ORDER BY algo_delivery_sequence DESC LIMIT 1 FOR UPDATE",
                            (event.runtime_id, algo_instance_id),
                        )
                        previous_row = cur.fetchone()
                        if previous_row is None:
                            raise KernelRepositoryConflict("external event cannot create an algo sequence-one delivery")
                        previous = _model_from_json(
                            AlgoDeliveryPersistenceV1,
                            _row_json(previous_row, "carrier_json"),
                        )
                        delivery = AlgoEventDeliveryV1.create(
                            event=event,
                            algo_instance_id=algo_instance_id,
                            plugin_manifest_sha256=algo.plugin_manifest_sha256,
                            algo_delivery_sequence=previous.algo_delivery_sequence + 1,
                            previous_delivery_id=previous.delivery_id,
                            status=DeliveryStatusV1.PENDING,
                            attempt_count=0,
                            lease_owner=None,
                            lease_expires_at=None,
                            transition_id=None,
                            last_error_json=None,
                            created_at_utc=event.event_time_utc,
                            updated_at_utc=event.event_time_utc,
                        )
                        built.append(
                            AlgoDeliveryPersistenceV1.create(
                                delivery=delivery,
                                lease_epoch=0,
                                lease_fence_token=None,
                                row_version=1,
                                next_attempt_at_utc=None,
                                failure_receipt_id=None,
                                skip_receipt_id=None,
                                closed_at_utc=None,
                            )
                        )
                    ordered = tuple(built)
                    delivery_ids = tuple(item.delivery_id for item in ordered)
                    provisional = RuntimeEventIngressReceiptV1.create(
                        runtime_id=event.runtime_id,
                        event_id=event.event_id,
                        event_key_sha256=event.event_key_sha256,
                        runtime_sequence=event.sequence,
                        ordered_target_algo_instance_ids=targets,
                        ordered_delivery_ids=delivery_ids,
                        transaction_commit_identity="mqtx_pending_routed_event",
                    )
                    transaction_id = transaction_commit_identity_v1(
                        operation=(
                            "INGEST_ROUTED_EVENT_ATOMIC"
                            if callback_mapping_update is None
                            else "INGEST_CALLBACK_EVENT_MAPPING_DELIVERIES_ATOMIC"
                        ),
                        owner_identities=(event.runtime_id,),
                        input_hashes=(event.event_key_sha256, event.payload_sha256, *callback_input_hashes),
                        output_identities=(
                            event.event_id,
                            provisional.ingress_receipt_id,
                            *callback_output_identities,
                            *delivery_ids,
                        ),
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
                    cur.execute(
                        """
                        INSERT INTO qmt_strategy.execution_runtime_event(
                            event_id,runtime_id,sequence,event_type,event_time,source,payload,
                            event_contract_version,event_schema_version,payload_schema_version,event_key_sha256,
                            payload_sha256,observed_at_utc,logical_at_utc,source_identity_json,correlation_json,
                            ingress_receipt_json,ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,'KERNEL_V2',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                    for delivery in ordered:
                        projection = _delivery_scalar_projection(delivery)
                        cur.execute(
                            """
                            INSERT INTO qmt_strategy.execution_algo_event_delivery(
                                delivery_id,event_id,runtime_id,algo_instance_id,plugin_manifest_sha256,
                                algo_delivery_sequence,previous_delivery_sequence,previous_delivery_id,status,
                                attempt_count,lease_owner,lease_worker_id,lease_process_incarnation_id,lease_epoch,
                                lease_fence_token,lease_expires_at,transition_id,last_error_json,next_attempt_at_utc,
                                failure_receipt_id,skip_receipt_id,row_version,created_at_utc,updated_at_utc,
                                closed_at_utc,carrier_json
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            (
                                projection["delivery_id"],
                                projection["event_id"],
                                projection["runtime_id"],
                                projection["algo_instance_id"],
                                projection["plugin_manifest_sha256"],
                                projection["algo_delivery_sequence"],
                                projection["previous_delivery_sequence"],
                                projection["previous_delivery_id"],
                                projection["status"],
                                projection["attempt_count"],
                                projection["lease_owner"],
                                projection["lease_worker_id"],
                                projection["lease_process_incarnation_id"],
                                projection["lease_epoch"],
                                projection["lease_fence_token"],
                                projection["lease_expires_at"],
                                projection["transition_id"],
                                None,
                                projection["next_attempt_at_utc"],
                                projection["failure_receipt_id"],
                                projection["skip_receipt_id"],
                                projection["row_version"],
                                projection["created_at_utc"],
                                projection["updated_at_utc"],
                                projection["closed_at_utc"],
                                _json(delivery.model_dump(mode="json")),
                            ),
                        )
                    cur.execute(
                        "UPDATE qmt_strategy.execution_runtime SET last_event_sequence=%s,updated_at=%s "
                        "WHERE runtime_id=%s AND last_event_sequence=%s",
                        (event.sequence, event.event_time_utc, event.runtime_id, last_sequence),
                    )
                    if cur.rowcount != 1:
                        raise KernelRepositoryConflict("runtime event sequence CAS failed")
        if existing_receipt is not None:
            readback = self.read_event_transaction(existing_receipt.event_id)
            if readback["receipt"] != existing_receipt:
                raise KernelRepositoryConflict("idempotent routed event readback differs from durable receipt")
            if callback_mapping_update is not None:
                chain = self.read_command_identity_chain(callback_mapping_update.reference_command_id)
                if chain["mapping"] != callback_mapping_update.mapping:
                    raise KernelRepositoryConflict("idempotent callback mapping post-commit readback differs")
            return existing_receipt
        if receipt is None:
            raise KernelRepositoryConflict("routed event transaction exited without a receipt")
        readback = self.read_event_transaction(event.event_id)
        if readback["event"] != event or readback["receipt"] != receipt:
            raise KernelRepositoryConflict("routed event post-commit readback differs from writer payload")
        if callback_mapping_update is not None:
            chain = self.read_command_identity_chain(callback_mapping_update.reference_command_id)
            if chain["mapping"] != callback_mapping_update.mapping:
                raise KernelRepositoryConflict("callback mapping post-commit readback differs")
        return receipt

    def write_event_receipt_deliveries(
        self,
        *,
        event: RuntimeEventEnvelopeV2,
        deliveries: Sequence[AlgoDeliveryPersistenceV1],
    ) -> RuntimeEventIngressReceiptV1:
        del deliveries
        if not isinstance(event, RuntimeEventEnvelopeV2):
            raise TypeError("event must be RuntimeEventEnvelopeV2")
        raise KernelRepositoryConflict(
            "direct event/delivery writes are disabled; use initialize_algo_atomic or ingest_routed_event_atomic"
        )

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
        if receipt.ordered_target_algo_instance_ids != tuple(item.algo_instance_id for item in deliveries):
            raise KernelRepositoryConflict("event receipt target set drifts from strict delivery owner order")
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
                claim_time = canonical_utc_datetime_v1(updated_at_utc, field_name="updated_at_utc")
                if previous.status is DeliveryStatusV1.FAILED_RETRYABLE:
                    if previous.next_attempt_at_utc is None:
                        raise KernelRepositoryConflict("retryable delivery has no durable next attempt authority")
                    claim_dt = datetime.fromisoformat(claim_time.replace("Z", "+00:00"))
                    next_attempt_dt = datetime.fromisoformat(previous.next_attempt_at_utc.replace("Z", "+00:00"))
                    if claim_dt < next_attempt_dt:
                        raise KernelRepositoryConflict(
                            "delivery retry is earlier than the durable next attempt authority"
                        )
                cur.execute(
                    """
                    SELECT delivery_id FROM qmt_strategy.execution_algo_event_delivery
                    WHERE runtime_id=%s AND algo_instance_id=%s
                      AND status NOT IN ('APPLIED','FAILED_TERMINAL','SKIPPED_TERMINAL')
                    ORDER BY algo_delivery_sequence ASC LIMIT 1 FOR UPDATE
                    """,
                    (previous.runtime_id, previous.algo_instance_id),
                )
                head_row = cur.fetchone()
                if head_row is None or str(head_row["delivery_id"]) != delivery_id:
                    raise KernelRepositoryConflict("delivery claim is not the minimum non-terminal algo delivery")
                if previous.previous_delivery_id is not None:
                    cur.execute(
                        "SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery "
                        "WHERE delivery_id=%s FOR SHARE",
                        (previous.previous_delivery_id,),
                    )
                    predecessor_row = cur.fetchone()
                    if predecessor_row is None:
                        raise KernelRepositoryConflict("delivery predecessor is missing")
                    predecessor = _model_from_json(
                        AlgoDeliveryPersistenceV1,
                        _row_json(predecessor_row, "carrier_json"),
                    )
                    if predecessor.status not in {
                        DeliveryStatusV1.APPLIED,
                        DeliveryStatusV1.FAILED_TERMINAL,
                        DeliveryStatusV1.SKIPPED_TERMINAL,
                    }:
                        raise KernelRepositoryConflict("delivery predecessor is not terminally closed")
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
                    updated_at_utc=claim_time,
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

    def mark_delivery_retryable(
        self,
        *,
        delivery_id: str,
        expected_row_version: int,
        expected_lease_owner: str,
        expected_lease_epoch: int,
        expected_lease_fence_token: str,
        error_evidence: KernelErrorEvidenceV1,
        failed_at_utc: Any,
    ) -> AlgoDeliveryPersistenceV1:
        if not isinstance(error_evidence, KernelErrorEvidenceV1):
            raise TypeError("error_evidence must be KernelErrorEvidenceV1")
        if not error_evidence.retryable or error_evidence.terminal or error_evidence.broker_called is not False:
            raise ValueError("retryable delivery evidence must be retryable, non-terminal and pre-broker")
        failed_at = canonical_utc_datetime_v1(failed_at_utc, field_name="failed_at_utc")
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery "
                    "WHERE delivery_id=%s FOR UPDATE",
                    (delivery_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(delivery_id)
                previous = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(row, "carrier_json"))
                if (
                    previous.status is not DeliveryStatusV1.CLAIMED
                    or previous.row_version != expected_row_version
                    or previous.lease_owner != expected_lease_owner
                    or previous.lease_epoch != expected_lease_epoch
                    or previous.lease_fence_token != expected_lease_fence_token
                ):
                    raise KernelRepositoryConflict("retryable delivery release lease/fence/CAS authority differs")
                if previous.attempt_count not in {1, 2, 3, 4}:
                    raise KernelRepositoryConflict("only attempts 1-4 may enter FAILED_RETRYABLE")
                failed_dt = datetime.fromisoformat(failed_at.replace("Z", "+00:00"))
                next_attempt = failed_dt + timedelta(seconds=2 ** (previous.attempt_count - 1))
                payload = previous.model_dump(mode="python")
                payload.update(
                    status=DeliveryStatusV1.FAILED_RETRYABLE,
                    lease_owner=None,
                    lease_expires_at=None,
                    lease_fence_token=None,
                    last_error_json=error_evidence.model_dump(mode="json"),
                    next_attempt_at_utc=next_attempt,
                    row_version=previous.row_version + 1,
                    updated_at_utc=failed_at,
                )
                successor = AlgoDeliveryPersistenceV1.model_validate(payload)
                successor.validate_successor_v1(previous)
                projection = _delivery_scalar_projection(successor)
                cur.execute(
                    """
                    UPDATE qmt_strategy.execution_algo_event_delivery
                    SET status='FAILED_RETRYABLE',lease_owner=NULL,lease_worker_id=NULL,
                        lease_process_incarnation_id=NULL,lease_fence_token=NULL,lease_expires_at=NULL,
                        last_error_json=%s,next_attempt_at_utc=%s,row_version=%s,updated_at_utc=%s,carrier_json=%s
                    WHERE delivery_id=%s AND row_version=%s AND lease_owner=%s AND lease_epoch=%s
                      AND lease_fence_token=%s
                    """,
                    (
                        _json(projection["last_error_json"]),
                        projection["next_attempt_at_utc"],
                        projection["row_version"],
                        projection["updated_at_utc"],
                        _json(successor.model_dump(mode="json")),
                        delivery_id,
                        expected_row_version,
                        expected_lease_owner,
                        expected_lease_epoch,
                        expected_lease_fence_token,
                    ),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("retryable delivery release CAS failed")
        readback = self.read_delivery(delivery_id)
        if readback != successor:
            raise KernelRepositoryConflict("retryable delivery post-commit readback differs")
        return readback

    def reclaim_stale_delivery(
        self,
        *,
        delivery_id: str,
        lease_owner: str,
        lease_epoch: int,
        lease_fence_token: str,
        lease_expires_at: Any,
        recovered_at_utc: Any,
        expected_row_version: int,
    ) -> AlgoDeliveryPersistenceV1:
        self._verify_lease_owner(lease_owner)
        recovered_at = canonical_utc_datetime_v1(recovered_at_utc, field_name="recovered_at_utc")
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery "
                    "WHERE delivery_id=%s FOR UPDATE",
                    (delivery_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(delivery_id)
                previous = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(row, "carrier_json"))
                if (
                    previous.status is not DeliveryStatusV1.CLAIMED
                    or previous.row_version != expected_row_version
                    or previous.lease_expires_at is None
                ):
                    raise KernelRepositoryConflict("stale delivery reclaim requires the exact current CLAIMED fact")
                recovered_dt = datetime.fromisoformat(recovered_at.replace("Z", "+00:00"))
                previous_expiry_dt = datetime.fromisoformat(previous.lease_expires_at.replace("Z", "+00:00"))
                if recovered_dt < previous_expiry_dt:
                    raise KernelRepositoryConflict("delivery lease is not stale at the recovery authority time")
                if lease_epoch != previous.lease_epoch + 1:
                    raise KernelRepositoryConflict("stale delivery lease epoch is not the exact successor")
                expected_fence = kernel_lease_fence_token_v1(
                    owner_type="DELIVERY",
                    owner_id=delivery_id,
                    lease_epoch=lease_epoch,
                    lease_owner=lease_owner,
                )
                if lease_fence_token != expected_fence:
                    raise KernelRepositoryConflict("stale delivery fence differs from exact repository authority")
                payload = previous.model_dump(mode="python")
                payload.update(
                    lease_owner=lease_owner,
                    lease_epoch=lease_epoch,
                    lease_fence_token=lease_fence_token,
                    lease_expires_at=canonical_utc_datetime_v1(lease_expires_at, field_name="lease_expires_at"),
                    row_version=previous.row_version + 1,
                    updated_at_utc=recovered_at,
                )
                reclaimed = AlgoDeliveryPersistenceV1.model_validate(payload)
                reclaimed.validate_successor_v1(previous)
                projection = _delivery_scalar_projection(reclaimed)
                cur.execute(
                    """
                    UPDATE qmt_strategy.execution_algo_event_delivery
                    SET lease_owner=%s,lease_worker_id=%s,lease_process_incarnation_id=%s,
                        lease_epoch=%s,lease_fence_token=%s,lease_expires_at=%s,
                        row_version=%s,updated_at_utc=%s,carrier_json=%s
                    WHERE delivery_id=%s AND row_version=%s AND lease_epoch=%s AND lease_fence_token=%s
                    """,
                    (
                        projection["lease_owner"],
                        projection["lease_worker_id"],
                        projection["lease_process_incarnation_id"],
                        projection["lease_epoch"],
                        projection["lease_fence_token"],
                        projection["lease_expires_at"],
                        projection["row_version"],
                        projection["updated_at_utc"],
                        _json(reclaimed.model_dump(mode="json")),
                        delivery_id,
                        expected_row_version,
                        previous.lease_epoch,
                        previous.lease_fence_token,
                    ),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("stale delivery reclaim CAS failed")
        readback = self.read_delivery(delivery_id)
        if readback != reclaimed:
            raise KernelRepositoryConflict("stale delivery reclaim post-commit readback differs")
        return readback
