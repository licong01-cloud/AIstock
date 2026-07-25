"""PostgreSQL-only durable repository for MiniQMT Execution Kernel K2-A."""

from __future__ import annotations

import inspect
import json
from contextlib import contextmanager
from datetime import date
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
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionAlgoTimerOccurrenceStatusV1,
    ExecutionAlgoTimerOccurrenceV1,
    ExecutionAlgoTimerScheduleStatusV1,
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


_K2_SCHEMA_CATALOG_SHA256 = "c9d5f192eb4522f54519c8e0c63540218c2674155471c1455c3150bea7a809c4"


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
                    cur.execute("SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() AS catalog_sha256")
                    catalog_sha256 = str(cur.fetchone()["catalog_sha256"])
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
                    SELECT delivery_id,status,attempt_count,lease_owner,lease_epoch,lease_fence_token,
                           row_version,carrier_json
                    FROM qmt_strategy.execution_algo_event_delivery WHERE delivery_id=%s
                    """,
                    (delivery_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(delivery_id)
        delivery = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(row, "carrier_json"))
        if (
            delivery.delivery_id != row["delivery_id"]
            or delivery.status.value != row["status"]
            or delivery.attempt_count != row["attempt_count"]
            or delivery.lease_owner != row["lease_owner"]
            or delivery.lease_epoch != row["lease_epoch"]
            or delivery.lease_fence_token != row["lease_fence_token"]
            or delivery.row_version != row["row_version"]
        ):
            raise KernelRepositoryConflict("delivery scalar columns drift from strict carrier")
        return delivery

    def read_outbox_command(self, command_id: str) -> BrokerCommandOutboxV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT command_id,status,attempt_count,lease_owner,lease_epoch,lease_fence_token,
                           row_version,outbox_row_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_command_outbox WHERE command_id=%s
                    """,
                    (command_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(command_id)
        outbox = _model_from_json(BrokerCommandOutboxV1, _row_json(row, "carrier_json"))
        if (
            outbox.command_id != row["command_id"]
            or outbox.status.value != row["status"]
            or outbox.attempt_count != row["attempt_count"]
            or outbox.lease_owner != row["lease_owner"]
            or outbox.lease_epoch != row["lease_epoch"]
            or outbox.lease_fence_token != row["lease_fence_token"]
            or outbox.row_version != row["row_version"]
            or outbox.outbox_row_sha256 != row["outbox_row_sha256"]
        ):
            raise KernelRepositoryConflict("outbox scalar columns drift from strict carrier")
        return outbox

    def read_algo_instance(self, algo_instance_id: str) -> ExecutionAlgoInstancePersistenceV2:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT algo.kernel_carrier_json,algo.row_version,algo.active_child_count,
                           algo.active_child_closure_status,
                           COUNT(child.child_order_id) FILTER (
                               WHERE child.kernel_contract_version='KERNEL_V2'
                                 AND child.mapping_status IN ('RESERVED','DISPATCHING','BROKER_ACCEPTED','OUTCOME_UNKNOWN')
                           ) AS reconstructed_active_child_count
                    FROM qmt_strategy.execution_algo_instance AS algo
                    LEFT JOIN qmt_strategy.execution_child_order AS child
                      ON child.runtime_id=algo.runtime_id AND child.algo_instance_id=algo.algo_instance_id
                    WHERE algo.algo_instance_id=%s AND algo.kernel_contract_version='KERNEL_V2'
                    GROUP BY algo.kernel_carrier_json,algo.row_version,algo.active_child_count,
                             algo.active_child_closure_status
                    """,
                    (algo_instance_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(algo_instance_id)
        algo = _model_from_json(ExecutionAlgoInstancePersistenceV2, _row_json(row, "kernel_carrier_json"))
        if (
            algo.row_version != row["row_version"]
            or algo.active_child_count != row["active_child_count"]
            or algo.active_child_closure_status.value != row["active_child_closure_status"]
            or algo.active_child_count != row["reconstructed_active_child_count"]
        ):
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
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_kernel_worker_incarnation(
                        worker_id, process_role, incarnation_sequence, source_revision,
                        process_incarnation_id, started_at_utc, startup_transaction_commit_identity,
                        receipt_sha256, startup_receipt_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        receipt.worker_id,
                        receipt.process_role,
                        receipt.incarnation_sequence,
                        receipt.source_revision,
                        receipt.process_incarnation_id,
                        receipt.started_at_utc,
                        receipt.startup_transaction_commit_identity,
                        receipt.receipt_sha256,
                        _json(receipt.model_dump(mode="json")),
                    ),
                )
        return self.read_worker_startup_receipt(receipt.process_incarnation_id)

    def read_worker_startup_receipt(self, process_incarnation_id: str) -> KernelWorkerStartupReceiptV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT startup_receipt_json
                    FROM qmt_strategy.execution_kernel_worker_incarnation
                    WHERE process_incarnation_id = %s
                    """,
                    (process_incarnation_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(process_incarnation_id)
        return _model_from_json(KernelWorkerStartupReceiptV1, _row_json(row, "startup_receipt_json"))

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
                        event.event_id,
                        event.runtime_id,
                        event.sequence,
                        event.event_type.value,
                        event.event_time_utc,
                        event.source.value,
                        _json(event.model_dump(mode="json")),
                        event.schema_version,
                        event.payload_schema_version,
                        event.event_key_sha256,
                        event.payload_sha256,
                        event.event_time_utc,
                        event.event_time_utc,
                        _json(event.model_dump(mode="json")["source_identity"]),
                        _json(event.model_dump(mode="json")["correlation"]),
                        _json(receipt.model_dump(mode="json")),
                        receipt.receipt_sha256,
                        receipt.routing_rule_version,
                        receipt.transaction_commit_identity,
                    ),
                )
                for delivery in ordered:
                    previous_sequence = (
                        None if delivery.previous_delivery_id is None else delivery.algo_delivery_sequence - 1
                    )
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
                            delivery.delivery_id,
                            delivery.event_id,
                            delivery.runtime_id,
                            delivery.algo_instance_id,
                            delivery.plugin_manifest_sha256,
                            delivery.algo_delivery_sequence,
                            previous_sequence,
                            delivery.previous_delivery_id,
                            delivery.status.value,
                            delivery.attempt_count,
                            delivery.lease_owner,
                            None if delivery.lease_owner is None else delivery.lease_owner.partition(":")[0],
                            None if delivery.lease_owner is None else delivery.lease_owner.partition(":")[2],
                            delivery.lease_epoch,
                            delivery.lease_fence_token,
                            delivery.lease_expires_at,
                            delivery.transition_id,
                            None
                            if delivery.last_error_json is None
                            else _json(delivery.model_dump(mode="json")["last_error_json"]),
                            delivery.next_attempt_at_utc,
                            delivery.failure_receipt_id,
                            delivery.skip_receipt_id,
                            delivery.row_version,
                            delivery.created_at_utc,
                            delivery.updated_at_utc,
                            delivery.closed_at_utc,
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
                    or in_transaction_deliveries != ordered
                ):
                    raise KernelRepositoryConflict("event identity exists with different immutable transaction payload")
        readback = self.read_event_transaction(event.event_id)
        if readback["event"] != event or readback["receipt"] != receipt or readback["deliveries"] != ordered:
            raise KernelRepositoryConflict("event transaction readback closure differs from writer payload")
        return receipt

    def read_event_transaction(self, event_id: str) -> dict[str, Any]:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT payload, ingress_receipt_json
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
                    SELECT carrier_json
                    FROM qmt_strategy.execution_algo_event_delivery
                    WHERE event_id = %s
                    ORDER BY algo_instance_id
                    """,
                    (event_id,),
                )
                delivery_rows = cur.fetchall()
        return {
            "event": _model_from_json(RuntimeEventEnvelopeV2, _row_json(event_row, "payload")),
            "receipt": _model_from_json(RuntimeEventIngressReceiptV1, _row_json(event_row, "ingress_receipt_json")),
            "deliveries": tuple(
                _model_from_json(AlgoDeliveryPersistenceV1, _row_json(row, "carrier_json")) for row in delivery_rows
            ),
        }

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
        if type(child_price_type) is not int:
            raise TypeError("child_price_type must be a strict integer")
        if isinstance(receipt, AlgoTransitionReceiptV1):
            kind = "APPLIED"
            transition_json, failure_json, skip_json = receipt.model_dump(mode="json"), None, None
            receipt_hash = receipt.receipt_sha256
            transition_id = receipt.transition_id
            expected_command_ids = receipt.ordered_command_ids
            if projection_set is None or after_state is None:
                raise ValueError("APPLIED transition requires projection set and after-state")
        elif isinstance(receipt, AlgoFailureReceiptV1):
            kind = "FAILED_TERMINAL"
            transition_json, failure_json, skip_json = None, receipt.model_dump(mode="json"), None
            receipt_hash = receipt.failure_receipt_sha256
            transition_id = receipt.failure_receipt_id
            expected_command_ids = receipt.ordered_cancel_command_ids
            if projection_set is not None or after_state is not None or mappings:
                raise ValueError("failure transition cannot create a new child mapping or applied state")
        elif isinstance(receipt, AlgoSkipReceiptV1):
            kind = "SKIPPED_TERMINAL"
            transition_json, failure_json, skip_json = None, None, receipt.model_dump(mode="json")
            receipt_hash = receipt.skip_receipt_sha256
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
                        transition_id,
                        delivery.delivery_id,
                        receipt.event_id,
                        receipt.runtime_id,
                        receipt.algo_instance_id,
                        receipt.transition_sequence
                        if hasattr(receipt, "transition_sequence")
                        else delivery.algo_delivery_sequence,
                        kind,
                        None if transition_json is None else _json(transition_json),
                        None if failure_json is None else _json(failure_json),
                        None if skip_json is None else _json(skip_json),
                        receipt_hash,
                        None if projection_set is None else _json(projection_set.model_dump(mode="json")),
                        None if projection_set is None else projection_set.projection_set_sha256,
                        None if after_state is None else _json(after_state.model_dump(mode="json")),
                        None if after_state is None else after_state.state_sha256,
                        receipt.transaction_commit_identity,
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
                        delivery.status.value,
                        delivery.attempt_count,
                        delivery.lease_owner,
                        None if delivery.lease_owner is None else delivery.lease_owner.partition(":")[0],
                        None if delivery.lease_owner is None else delivery.lease_owner.partition(":")[2],
                        delivery.lease_epoch,
                        delivery.lease_fence_token,
                        delivery.lease_expires_at,
                        delivery.transition_id,
                        None
                        if delivery.last_error_json is None
                        else _json(delivery.model_dump(mode="json")["last_error_json"]),
                        delivery.next_attempt_at_utc,
                        delivery.failure_receipt_id,
                        delivery.skip_receipt_id,
                        delivery.row_version,
                        delivery.updated_at_utc,
                        delivery.closed_at_utc,
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
        if readback["command_outboxes"] != outboxes or readback["new_child_mappings"] != mappings:
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
                    SELECT outbox.carrier_json AS outbox_json, child.mapping_json,
                           child.created_transition_id
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
        mappings_by_id: dict[str, ExecutionCommandChildMappingV1] = {}
        outboxes: list[BrokerCommandOutboxV1] = []
        for command_row in command_rows:
            outbox = _model_from_json(BrokerCommandOutboxV1, _row_json(command_row, "outbox_json"))
            mapping = _model_from_json(ExecutionCommandChildMappingV1, _row_json(command_row, "mapping_json"))
            outboxes.append(outbox)
            if command_row["created_transition_id"] == transition_id:
                mappings_by_id.setdefault(mapping.mapping_id, mapping)
        return {
            "receipt": _model_from_json(receipt_model, _row_json(row, receipt_key)),
            "projection_set": None
            if row["execution_projection_set_json"] is None
            else _model_from_json(ExecutionProjectionSetV1, _row_json(row, "execution_projection_set_json")),
            "after_state": None
            if row["after_state_json"] is None
            else _model_from_json(AlgoStateSnapshotV2, _row_json(row, "after_state_json")),
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
                    mapping.child_order_id,
                    mapping.runtime_id,
                    mapping.algo_instance_id,
                    mapping.parent_intent_id,
                    mapping.strategy_slot_id,
                    mapping.symbol,
                    mapping.side.value,
                    mapping.requested_quantity,
                    mapping.requested_price_decimal,
                    child_price_type,
                    mapping.updated_at_utc,
                    mapping.mapping_id,
                    mapping.command_id,
                    mapping.local_vt_orderid,
                    mapping.deterministic_client_order_ref,
                    mapping.order_remark,
                    mapping.mapping_status.value,
                    mapping.mapping_version,
                    mapping.payload_sha256,
                    mapping.mapping_receipt_sha256,
                    mapping.broker_identity_source_event_id,
                    mapping.last_order_event_id,
                    mapping.last_trade_event_id,
                    mapping.created_transition_id,
                    mapping.updated_by_event_id,
                    mapping.created_at_utc,
                    mapping.updated_at_utc,
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
                    SELECT outbox.carrier_json AS outbox_json, child.mapping_json
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
        return {
            "outbox": _model_from_json(BrokerCommandOutboxV1, _row_json(row, "outbox_json")),
            "mapping": _model_from_json(ExecutionCommandChildMappingV1, _row_json(row, "mapping_json")),
        }

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
                    or ack.source.value not in {"CALLBACK", "RECONCILIATION"}
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
                    mapping.validate_successor_v1(previous_mapping)
                elif mapping.mapping_version != expected_mapping_version:
                    raise KernelRepositoryConflict("unchanged CANCEL mapping version differs from durable predecessor")
                outbox.validate_successor_v1(previous_outbox)
                if mapping_changed:
                    mapping_values = mapping.model_dump(mode="json")
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
                            mapping.broker_order_id,
                            mapping.broker_identity_source_event_id,
                            mapping.mapping_status.value,
                            mapping.mapping_version,
                            mapping.payload_sha256,
                            mapping.mapping_receipt_sha256,
                            mapping.last_order_event_id,
                            mapping.last_trade_event_id,
                            mapping.updated_by_event_id,
                            mapping.updated_at_utc,
                            mapping.updated_at_utc,
                            _json(mapping_values),
                            mapping.mapping_id,
                            expected_mapping_version,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise KernelRepositoryConflict("mapping CAS failed")
                outbox_values = outbox.model_dump(mode="json")
                lease_worker, _, lease_incarnation = (outbox.lease_owner or "").partition(":")
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
                        outbox.status.value,
                        outbox.attempt_count,
                        outbox.lease_owner,
                        lease_worker or None,
                        lease_incarnation or None,
                        outbox.lease_epoch,
                        outbox.lease_fence_token,
                        outbox.lease_expires_at,
                        outbox.dispatch_attempt_id,
                        outbox.next_attempt_at_utc,
                        outbox.broker_called,
                        outbox.broker_order_id,
                        None if outbox.ack_receipt_json is None else _json(outbox_values["ack_receipt_json"]),
                        outbox.ack_receipt_sha256,
                        None
                        if outbox.non_acceptance_receipt is None
                        else _json(outbox_values["non_acceptance_receipt"]),
                        None
                        if outbox.unknown_outcome_receipt is None
                        else _json(outbox_values["unknown_outcome_receipt"]),
                        None if outbox.reconcile_receipt is None else _json(outbox_values["reconcile_receipt"]),
                        None if outbox.last_error_json is None else _json(outbox_values["last_error_json"]),
                        outbox.row_version,
                        outbox.updated_at_utc,
                        outbox.closed_at_utc,
                        _json(outbox_values),
                        outbox.outbox_row_sha256,
                        outbox.command_id,
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
                        claimed.attempt_count,
                        claimed.lease_owner,
                        lease_owner.partition(":")[0],
                        lease_owner.partition(":")[2],
                        claimed.lease_epoch,
                        claimed.lease_fence_token,
                        claimed.lease_expires_at,
                        claimed.row_version,
                        claimed.updated_at_utc,
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
                        claimed.attempt_count,
                        claimed.lease_owner,
                        lease_owner.partition(":")[0],
                        lease_owner.partition(":")[2],
                        claimed.lease_epoch,
                        claimed.lease_fence_token,
                        claimed.lease_expires_at,
                        claimed.row_version,
                        claimed.updated_at_utc,
                        _json(claimed.model_dump(mode="json")),
                        claimed.outbox_row_sha256,
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
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_algo_command_dispatch_attempt(
                        dispatch_attempt_id,stage,command_id,attempt_count,lease_epoch,lease_fence_token,
                        process_incarnation_id,started_at_utc,finished_at_utc,broker_called,
                        attempt_receipt_sha256,carrier_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (dispatch_attempt_id,stage) DO NOTHING
                    """,
                    (
                        attempt.dispatch_attempt_id,
                        attempt.stage.value,
                        attempt.command_id,
                        attempt.attempt_count,
                        attempt.lease_epoch,
                        attempt.lease_fence_token,
                        attempt.process_incarnation_id,
                        attempt.started_at_utc,
                        attempt.finished_at_utc,
                        attempt.broker_called,
                        attempt.attempt_receipt_sha256,
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
                           lease_fence_token,process_incarnation_id,attempt_receipt_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_command_dispatch_attempt
                    WHERE dispatch_attempt_id=%s AND stage=%s
                    """,
                    (dispatch_attempt_id, stage),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError((dispatch_attempt_id, stage))
        attempt = _model_from_json(BrokerDispatchAttemptV1, _row_json(row, "carrier_json"))
        if (
            attempt.dispatch_attempt_id != row["dispatch_attempt_id"]
            or attempt.stage.value != row["stage"]
            or attempt.command_id != row["command_id"]
            or attempt.attempt_count != row["attempt_count"]
            or attempt.lease_epoch != row["lease_epoch"]
            or attempt.lease_fence_token != row["lease_fence_token"]
            or attempt.process_incarnation_id != row["process_incarnation_id"]
            or attempt.attempt_receipt_sha256 != row["attempt_receipt_sha256"]
        ):
            raise KernelRepositoryConflict("dispatch attempt scalar columns drift from strict carrier")
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
                elif (
                    schedule.status is not ExecutionAlgoTimerScheduleStatusV1.SCHEDULED
                    or schedule.lease_epoch != 0
                    or schedule.lease_owner is not None
                ):
                    raise KernelRepositoryConflict("timer schedule first write must be unleased SCHEDULED epoch zero")
                lease_worker, _, lease_incarnation = (schedule.lease_owner or "").partition(":")
                sql_values = (
                    schedule.schedule_id,
                    schedule.runtime_id,
                    schedule.algo_instance_id,
                    schedule.timer_name,
                    schedule.schedule_epoch,
                    schedule.due_at_exchange_utc,
                    schedule.status.value,
                    schedule.timer_occurrence_id,
                    schedule.emitted_event_id,
                    schedule.lease_owner,
                    lease_worker or None,
                    lease_incarnation or None,
                    schedule.lease_epoch,
                    schedule.lease_fence_token,
                    schedule.lease_expires_at_utc,
                    schedule.row_version,
                    schedule.created_at_utc,
                    schedule.updated_at_utc,
                    schedule.closed_at_utc,
                    schedule.schedule_receipt_sha256,
                    _json(schedule.model_dump(mode="json")),
                )
                if row is None:
                    cur.execute(
                        """
                    INSERT INTO qmt_strategy.execution_algo_timer_schedule(
                        schedule_id,runtime_id,algo_instance_id,timer_name,schedule_epoch,due_at_exchange_utc,
                        status,timer_occurrence_id,emitted_event_id,lease_owner,lease_worker_id,
                        lease_process_incarnation_id,lease_epoch,lease_fence_token,lease_expires_at_utc,
                        row_version,created_at_utc,updated_at_utc,closed_at_utc,schedule_receipt_sha256,carrier_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                            schedule.status.value,
                            schedule.emitted_event_id,
                            schedule.lease_owner,
                            lease_worker or None,
                            lease_incarnation or None,
                            schedule.lease_epoch,
                            schedule.lease_fence_token,
                            schedule.lease_expires_at_utc,
                            schedule.row_version,
                            schedule.updated_at_utc,
                            schedule.closed_at_utc,
                            schedule.schedule_receipt_sha256,
                            _json(schedule.model_dump(mode="json")),
                            schedule.schedule_id,
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
                lease_worker, _, lease_incarnation = (occurrence.lease_owner or "").partition(":")
                sql_values = (
                    occurrence.timer_occurrence_id,
                    occurrence.schedule_id,
                    occurrence.runtime_id,
                    occurrence.algo_instance_id,
                    occurrence.due_at_exchange_utc,
                    occurrence.exchange_session_authority_sha256,
                    occurrence.status.value,
                    occurrence.emitted_event_id,
                    occurrence.lease_owner,
                    lease_worker or None,
                    lease_incarnation or None,
                    occurrence.lease_epoch,
                    occurrence.lease_fence_token,
                    occurrence.lease_expires_at_utc,
                    occurrence.row_version,
                    occurrence.created_at_utc,
                    occurrence.closed_at_utc,
                    occurrence.occurrence_receipt_sha256,
                    _json(occurrence.model_dump(mode="json")),
                )
                if row is None:
                    cur.execute(
                        """
                    INSERT INTO qmt_strategy.execution_algo_timer_occurrence(
                        timer_occurrence_id,schedule_id,runtime_id,algo_instance_id,due_at_exchange_utc,
                        exchange_session_authority_sha256,status,emitted_event_id,lease_owner,lease_worker_id,
                        lease_process_incarnation_id,lease_epoch,lease_fence_token,lease_expires_at_utc,row_version,
                        created_at_utc,closed_at_utc,occurrence_receipt_sha256,carrier_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                        """,
                        sql_values,
                    )
                elif previous != occurrence:
                    cur.execute(
                        """
                        UPDATE qmt_strategy.execution_algo_timer_occurrence
                        SET status=%s,emitted_event_id=%s,lease_owner=%s,lease_worker_id=%s,
                            lease_process_incarnation_id=%s,lease_epoch=%s,lease_fence_token=%s,
                            lease_expires_at_utc=%s,row_version=%s,closed_at_utc=%s,
                            occurrence_receipt_sha256=%s,carrier_json=%s
                        WHERE timer_occurrence_id=%s AND row_version=%s
                          AND lease_owner IS NOT DISTINCT FROM %s
                          AND lease_epoch=%s
                          AND lease_fence_token IS NOT DISTINCT FROM %s
                        """,
                        (
                            occurrence.status.value,
                            occurrence.emitted_event_id,
                            occurrence.lease_owner,
                            lease_worker or None,
                            lease_incarnation or None,
                            occurrence.lease_epoch,
                            occurrence.lease_fence_token,
                            occurrence.lease_expires_at_utc,
                            occurrence.row_version,
                            occurrence.closed_at_utc,
                            occurrence.occurrence_receipt_sha256,
                            _json(occurrence.model_dump(mode="json")),
                            occurrence.timer_occurrence_id,
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
                    SELECT schedule_id,status,lease_owner,lease_epoch,lease_fence_token,row_version,
                           schedule_receipt_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_timer_schedule WHERE schedule_id=%s
                    """,
                    (schedule_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(schedule_id)
        schedule = _model_from_json(ExecutionAlgoTimerScheduleV1, _row_json(row, "carrier_json"))
        if (
            schedule.schedule_id != row["schedule_id"]
            or schedule.status.value != row["status"]
            or schedule.lease_owner != row["lease_owner"]
            or schedule.lease_epoch != row["lease_epoch"]
            or schedule.lease_fence_token != row["lease_fence_token"]
            or schedule.row_version != row["row_version"]
            or schedule.schedule_receipt_sha256 != row["schedule_receipt_sha256"]
        ):
            raise KernelRepositoryConflict("timer schedule scalar columns drift from strict carrier")
        return schedule

    def read_timer_occurrence(self, timer_occurrence_id: str) -> ExecutionAlgoTimerOccurrenceV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT timer_occurrence_id,status,lease_owner,lease_epoch,lease_fence_token,row_version,
                           occurrence_receipt_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_timer_occurrence WHERE timer_occurrence_id=%s
                    """,
                    (timer_occurrence_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(timer_occurrence_id)
        occurrence = _model_from_json(ExecutionAlgoTimerOccurrenceV1, _row_json(row, "carrier_json"))
        if (
            occurrence.timer_occurrence_id != row["timer_occurrence_id"]
            or occurrence.status.value != row["status"]
            or occurrence.lease_owner != row["lease_owner"]
            or occurrence.lease_epoch != row["lease_epoch"]
            or occurrence.lease_fence_token != row["lease_fence_token"]
            or occurrence.row_version != row["row_version"]
            or occurrence.occurrence_receipt_sha256 != row["occurrence_receipt_sha256"]
        ):
            raise KernelRepositoryConflict("timer occurrence scalar columns drift from strict carrier")
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
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_exchange_session_authority(
                        runtime_id,exchange_trade_date,calendar_snapshot_set_id,calendar_snapshot_set_sha256,
                        session_definition_version,authority_sha256,authority_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (runtime_id,exchange_trade_date) DO NOTHING
                    """,
                    (
                        authority.runtime_id,
                        authority.exchange_trade_date,
                        authority.calendar_snapshot_set_id,
                        authority.calendar_snapshot_set_sha256,
                        authority.session_definition_version,
                        authority.authority_sha256,
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
        if (
            authority.runtime_id != row["runtime_id"]
            or date.fromisoformat(authority.exchange_trade_date) != row["exchange_trade_date"]
            or row["exchange_trade_date"] != row["runtime_trade_date"]
            or authority.calendar_snapshot_set_id != row["calendar_snapshot_set_id"]
            or authority.calendar_snapshot_set_sha256 != row["calendar_snapshot_set_sha256"]
            or authority.session_definition_version != row["session_definition_version"]
            or authority.authority_sha256 != row["authority_sha256"]
        ):
            raise KernelRepositoryConflict("exchange-session scalar columns drift from strict authority")
        return authority

    def list_recovery_deliveries(
        self, *, runtime_id: str, trade_date: date, statuses: Sequence[str], limit: int
    ) -> tuple[AlgoDeliveryPersistenceV1, ...]:
        rows = self._recovery_rows(
            table="execution_algo_event_delivery",
            json_column="carrier_json",
            runtime_id=runtime_id,
            trade_date=trade_date,
            statuses=statuses,
            limit=limit,
        )
        return tuple(_model_from_json(AlgoDeliveryPersistenceV1, item) for item in rows)

    def list_recovery_outbox_commands(
        self, *, runtime_id: str, trade_date: date, statuses: Sequence[str], limit: int
    ) -> tuple[BrokerCommandOutboxV1, ...]:
        rows = self._recovery_rows(
            table="execution_algo_command_outbox",
            json_column="carrier_json",
            runtime_id=runtime_id,
            trade_date=trade_date,
            statuses=statuses,
            limit=limit,
        )
        return tuple(_model_from_json(BrokerCommandOutboxV1, item) for item in rows)

    def list_recovery_timer_occurrences(
        self, *, runtime_id: str, trade_date: date, statuses: Sequence[str], limit: int
    ) -> tuple[ExecutionAlgoTimerOccurrenceV1, ...]:
        rows = self._recovery_rows(
            table="execution_algo_timer_occurrence",
            json_column="carrier_json",
            runtime_id=runtime_id,
            trade_date=trade_date,
            statuses=statuses,
            limit=limit,
        )
        return tuple(_model_from_json(ExecutionAlgoTimerOccurrenceV1, item) for item in rows)

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
                algo_instance.algo_instance_id,
                algo_instance.runtime_id,
                algo_instance.parent_intent_id,
                algo_instance.strategy_slot_id,
                algo_instance.symbol,
                algo_instance.side.value,
                algo_instance.target_quantity,
                algo_instance.remaining_quantity,
                algo_instance.algo_code,
                algo_instance.status.value,
                algo_instance.created_at_utc,
                algo_instance.updated_at_utc,
                algo_instance.traded_quantity,
                algo_instance.plugin_id,
                algo_instance.plugin_version,
                algo_instance.plugin_manifest_sha256,
                _json(values["plugin_config_json"]),
                algo_instance.plugin_config_sha256,
                algo_instance.compatibility_receipt_sha256,
                algo_instance.state_schema_version,
                None if algo_instance.state_json is None else _json(values["state_json"]),
                algo_instance.state_sha256,
                algo_instance.transition_sequence,
                algo_instance.last_applied_delivery_sequence,
                algo_instance.last_applied_delivery_id,
                algo_instance.last_closed_delivery_sequence,
                algo_instance.terminal_delivery_sequence,
                algo_instance.failure_receipt_id,
                algo_instance.active_child_closure_status.value,
                algo_instance.active_child_count,
                algo_instance.row_version,
                algo_instance.terminal_at_utc,
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

    def _recovery_rows(
        self,
        *,
        table: str,
        json_column: str,
        runtime_id: str,
        trade_date: date,
        statuses: Sequence[str],
        limit: int,
    ) -> tuple[dict[str, Any], ...]:
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
                "target.created_at_utc, target.algo_delivery_sequence, target.delivery_id",
            ),
            "execution_algo_command_outbox": (
                {status.value for status in BrokerCommandOutboxStatusV1},
                "target.next_attempt_at_utc NULLS FIRST, target.created_at_utc, target.command_id",
            ),
            "execution_algo_timer_occurrence": (
                {status.value for status in ExecutionAlgoTimerOccurrenceStatusV1},
                "target.due_at_exchange_utc, target.created_at_utc, target.timer_occurrence_id",
            ),
        }
        if table not in table_authority or json_column != "carrier_json":
            raise ValueError("unsupported recovery table")
        allowed_statuses, order_by = table_authority[table]
        invalid = tuple(status for status in exact_statuses if status not in allowed_statuses)
        if invalid:
            raise ValueError(f"unsupported recovery statuses for {table}: {invalid}")
        query = f"""
            SELECT target.{json_column}
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
        return tuple(_row_json(row, json_column) for row in rows)

    @staticmethod
    def _outbox_sql_values(outbox: BrokerCommandOutboxV1) -> tuple[Any, ...]:
        values = outbox.model_dump(mode="json")
        return (
            outbox.command_id,
            outbox.transition_id,
            outbox.ordinal,
            outbox.runtime_id,
            outbox.algo_instance_id,
            outbox.parent_intent_id,
            outbox.mapping_id,
            outbox.command_type.value,
            outbox.local_vt_orderid,
            _json(values["payload_json"]),
            outbox.payload_sha256,
            outbox.status.value,
            outbox.attempt_count,
            outbox.lease_owner,
            outbox.lease_epoch,
            outbox.lease_fence_token,
            outbox.lease_expires_at,
            outbox.dispatch_attempt_id,
            outbox.deterministic_client_order_ref,
            outbox.next_attempt_at_utc,
            outbox.broker_called,
            outbox.broker_order_id,
            None if outbox.ack_receipt_json is None else _json(values["ack_receipt_json"]),
            outbox.ack_receipt_sha256,
            None if outbox.non_acceptance_receipt is None else _json(values["non_acceptance_receipt"]),
            None if outbox.unknown_outcome_receipt is None else _json(values["unknown_outcome_receipt"]),
            None if outbox.reconcile_receipt is None else _json(values["reconcile_receipt"]),
            None if outbox.last_error_json is None else _json(values["last_error_json"]),
            outbox.row_version,
            outbox.created_at_utc,
            outbox.updated_at_utc,
            outbox.closed_at_utc,
            _json(outbox.model_dump(mode="json")),
            outbox.outbox_row_sha256,
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
