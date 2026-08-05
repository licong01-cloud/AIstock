"""Runtime-event and delivery transactions for the K2-A repository facade."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import json
from typing import Any, Sequence

import psycopg2
import psycopg2.extras

from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeContractError,
    VnpyFacadeRepositoryEventReadV1,
    VnpyFacadeRepositoryReadKindV1,
)

from .kernel_repository_common import (
    KernelRepositoryConflict,
    _json,
    _model_from_json,
    _row_json,
)
from .plugin_canonical import canonical_utc_datetime_v1, thaw_json_v1
from .vnpy_facade_diagnostics import record_vnpy_facade_repository_read_v1
from .kernel_repository_projection import (
    _assert_scalar_columns,
    _delivery_scalar_projection,
    _event_scalar_projection,
    _mapping_scalar_projection,
)
from .kernel_ingress import route_event_targets_v1
from .kernel_callback_events import (
    build_kernel_command_outcome_event_payload_from_durable_v1,
    strict_readback_kernel_event_payload_v1,
)
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
    SessionPhaseV1,
    RuntimeEventIngressReceiptV1,
    KernelErrorEvidenceV1,
    KernelCallbackMappingUpdateV1,
    KernelCommandOutcomeEventPayloadV1,
    KernelOrderEventPayloadV1,
    KernelOrderReconcileEventPayloadV1,
    KernelTradeEventPayloadV1,
    KernelCommandOutcomeMappingClosureModeV1,
    KernelCommandOutcomeMappingClosureV1,
    ExecutionCommandChildMappingV1,
    kernel_lease_fence_token_v1,
    transaction_commit_identity_v1,
)


def _callback_watermark_sequence(value: str, *, runtime_id: str) -> int:
    prefix = f"{runtime_id}:"
    if type(value) is not str or not value.startswith(prefix):
        raise KernelRepositoryConflict("callback watermark does not belong to the requested runtime")
    suffix = value[len(prefix) :]
    if not suffix.isascii() or not suffix.isdigit():
        raise KernelRepositoryConflict("callback watermark sequence is invalid")
    sequence = int(suffix)
    if sequence < 0 or str(sequence) != suffix:
        raise KernelRepositoryConflict("callback watermark sequence is not canonical")
    return sequence


class KernelRepositoryEventDeliveryMixin:
    """Own event ingress, delivery readback, claim, and atomicity operations."""

    def read_runtime_event(self, event_id: str) -> RuntimeEventEnvelopeV2:
        return self.read_event_transaction(event_id)["event"]

    @staticmethod
    def _read_facade_algo_start_event_with_cursor(
        cur: Any,
        *,
        runtime_id: str,
        algo_instance_id: str,
    ) -> VnpyFacadeRepositoryEventReadV1:
        cur.execute(
            """
            SELECT event.payload AS event_payload, delivery.carrier_json AS delivery_payload
            FROM qmt_strategy.execution_algo_event_delivery AS delivery
            JOIN qmt_strategy.execution_runtime_event AS event
              ON event.runtime_id=delivery.runtime_id AND event.event_id=delivery.event_id
            WHERE delivery.runtime_id=%s AND delivery.algo_instance_id=%s
              AND delivery.algo_delivery_sequence=1
            ORDER BY event.event_id
            LIMIT 2
            """,
            (runtime_id, algo_instance_id),
        )
        rows = tuple(cur.fetchall())
        if len(rows) != 1:
            record_vnpy_facade_repository_read_v1(read_kind="ALGO_START", outcome="INVALID")
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_REPOSITORY_READ_INVALID",
                "facade ALGO_START read requires exactly one first delivery fact",
                context={
                    "read_kind": "ALGO_START",
                    "runtime_id": runtime_id,
                    "algo_instance_id": algo_instance_id,
                    "match_count": len(rows),
                },
            )
        try:
            event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(rows[0], "event_payload"))
            delivery = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(rows[0], "delivery_payload"))
            result = VnpyFacadeRepositoryEventReadV1.create(
                read_kind=VnpyFacadeRepositoryReadKindV1.ALGO_START,
                runtime_id=runtime_id,
                algo_instance_id=algo_instance_id,
                cutoff_delivery_sequence_or_null=None,
                cutoff_event_sequence_or_null=None,
                event=event,
                delivery=delivery,
            )
            record_vnpy_facade_repository_read_v1(read_kind="ALGO_START", outcome="FOUND")
            return result
        except VnpyFacadeContractError:
            raise
        except Exception as exc:
            record_vnpy_facade_repository_read_v1(read_kind="ALGO_START", outcome="INVALID")
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_REPOSITORY_READ_INVALID",
                "facade ALGO_START durable carrier failed strict readback",
                context={
                    "read_kind": "ALGO_START",
                    "runtime_id": runtime_id,
                    "algo_instance_id": algo_instance_id,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            ) from exc

    @staticmethod
    def _read_facade_latest_prior_tick_with_cursor(
        cur: Any,
        *,
        runtime_id: str,
        algo_instance_id: str,
        cutoff_delivery_sequence: int,
        cutoff_event_sequence: int,
        exchange_trade_date: str,
        session_epoch: str,
        session_phase: SessionPhaseV1,
        expected_symbol: str,
    ) -> VnpyFacadeRepositoryEventReadV1 | None:
        cur.execute(
            """
            SELECT event.payload AS event_payload, delivery.carrier_json AS delivery_payload
            FROM qmt_strategy.execution_algo_event_delivery AS delivery
            JOIN qmt_strategy.execution_runtime_event AS event
              ON event.runtime_id=delivery.runtime_id AND event.event_id=delivery.event_id
            WHERE delivery.runtime_id=%s AND delivery.algo_instance_id=%s
              AND delivery.status='APPLIED'
              AND delivery.algo_delivery_sequence < %s
              AND event.sequence < %s
              AND event.event_contract_version='KERNEL_V2'
              AND event.event_type='TICK' AND event.source='B0_QUOTE_V2'
              AND event.payload_schema_version='miniqmt_market_data_view_v2'
            ORDER BY delivery.algo_delivery_sequence DESC,event.sequence DESC,event.event_id DESC
            LIMIT 1
            """,
            (
                runtime_id,
                algo_instance_id,
                cutoff_delivery_sequence,
                cutoff_event_sequence,
            ),
        )
        row = cur.fetchone()
        if row is None:
            record_vnpy_facade_repository_read_v1(read_kind="LATEST_PRIOR_TICK", outcome="UNAVAILABLE")
            return None
        try:
            event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(row, "event_payload"))
            delivery = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(row, "delivery_payload"))
            if event.symbol != expected_symbol:
                record_vnpy_facade_repository_read_v1(read_kind="LATEST_PRIOR_TICK", outcome="UNAVAILABLE")
                return None
            correlation = thaw_json_v1(event.correlation)
            required_session = {
                "exchange_trade_date": exchange_trade_date,
                "session_epoch": session_epoch,
                "session_phase": session_phase.value,
            }
            if correlation != required_session:
                record_vnpy_facade_repository_read_v1(read_kind="LATEST_PRIOR_TICK", outcome="UNAVAILABLE")
                return None
            payload = thaw_json_v1(event.payload)
            required_payload_fields = {
                "symbol",
                "logical_at_utc",
                "bid_price_1",
                "bid_volume_1",
                "ask_price_1",
                "ask_volume_1",
                "last_price",
                "limit_up",
                "limit_down",
                "eligibility_state",
                "freshness_state",
                "generation",
                "quote_source",
                "exchange_time_utc",
                "exchange_trade_date",
                "session_epoch",
                "session_phase",
            }
            source_identity = thaw_json_v1(event.source_identity)
            if (
                not isinstance(payload, dict)
                or not required_payload_fields.issubset(payload)
                or payload.get("symbol") != expected_symbol
                or payload.get("eligibility_state") != "READY"
                or payload.get("freshness_state") != "READY"
                or type(payload.get("generation")) is not int
                or payload["generation"] < 0
                or payload.get("quote_source") != "B0_QUOTE_V2"
                or payload.get("exchange_trade_date") != exchange_trade_date
                or payload.get("session_epoch") != session_epoch
                or payload.get("session_phase") != session_phase.value
                or not isinstance(source_identity, dict)
                or type(source_identity.get("market_data_id")) is not str
                or not source_identity["market_data_id"]
            ):
                record_vnpy_facade_repository_read_v1(read_kind="LATEST_PRIOR_TICK", outcome="UNAVAILABLE")
                return None
            read = VnpyFacadeRepositoryEventReadV1.create(
                read_kind=VnpyFacadeRepositoryReadKindV1.LATEST_PRIOR_TICK,
                runtime_id=runtime_id,
                algo_instance_id=algo_instance_id,
                cutoff_delivery_sequence_or_null=cutoff_delivery_sequence,
                cutoff_event_sequence_or_null=cutoff_event_sequence,
                event=event,
                delivery=delivery,
            )
            record_vnpy_facade_repository_read_v1(read_kind="LATEST_PRIOR_TICK", outcome="FOUND")
            return read
        except VnpyFacadeContractError:
            raise
        except Exception as exc:
            record_vnpy_facade_repository_read_v1(read_kind="LATEST_PRIOR_TICK", outcome="INVALID")
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_REPOSITORY_READ_INVALID",
                "facade latest prior TICK durable carrier failed strict readback",
                context={
                    "read_kind": "LATEST_PRIOR_TICK",
                    "runtime_id": runtime_id,
                    "algo_instance_id": algo_instance_id,
                    "cutoff_event_sequence": cutoff_event_sequence,
                    "cutoff_delivery_sequence": cutoff_delivery_sequence,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            ) from exc

    def read_facade_algo_start_event_v1(
        self,
        *,
        runtime_id: str,
        algo_instance_id: str,
    ) -> VnpyFacadeRepositoryEventReadV1:
        for field_name, value in (("runtime_id", runtime_id), ("algo_instance_id", algo_instance_id)):
            if type(value) is not str or not value or value != value.strip():
                raise TypeError(f"{field_name} must be a trim-stable strict string")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._read_facade_algo_start_event_with_cursor(
                    cur,
                    runtime_id=runtime_id,
                    algo_instance_id=algo_instance_id,
                )

    def read_facade_latest_prior_tick_v1(
        self,
        *,
        runtime_id: str,
        algo_instance_id: str,
        timer_delivery_sequence: int,
        timer_event_sequence: int,
        exchange_trade_date: str,
        session_epoch: str,
        session_phase: SessionPhaseV1,
    ) -> VnpyFacadeRepositoryEventReadV1 | None:
        for field_name, value in (("runtime_id", runtime_id), ("algo_instance_id", algo_instance_id)):
            if type(value) is not str or not value or value != value.strip():
                raise TypeError(f"{field_name} must be a trim-stable strict string")
        for field_name, value in (
            ("timer_delivery_sequence", timer_delivery_sequence),
            ("timer_event_sequence", timer_event_sequence),
        ):
            if type(value) is not int or value <= 0:
                raise TypeError(f"{field_name} must be a positive strict integer")
        if type(exchange_trade_date) is not str or len(exchange_trade_date) != 10:
            raise TypeError("exchange_trade_date must be a YYYY-MM-DD strict string")
        try:
            if date.fromisoformat(exchange_trade_date).isoformat() != exchange_trade_date:
                raise ValueError
        except ValueError as exc:
            raise ValueError("exchange_trade_date must be canonical YYYY-MM-DD") from exc
        if type(session_epoch) is not str or not session_epoch or session_epoch != session_epoch.strip():
            raise TypeError("session_epoch must be a trim-stable strict string")
        if not isinstance(session_phase, SessionPhaseV1):
            raise TypeError("session_phase must be SessionPhaseV1")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT kernel_carrier_json FROM qmt_strategy.execution_algo_instance "
                    "WHERE runtime_id=%s AND algo_instance_id=%s AND kernel_contract_version='KERNEL_V2'",
                    (runtime_id, algo_instance_id),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(algo_instance_id)
                algo = _model_from_json(
                    ExecutionAlgoInstancePersistenceV2,
                    _row_json(row, "kernel_carrier_json"),
                )
                return self._read_facade_latest_prior_tick_with_cursor(
                    cur,
                    runtime_id=runtime_id,
                    algo_instance_id=algo_instance_id,
                    cutoff_delivery_sequence=timer_delivery_sequence,
                    cutoff_event_sequence=timer_event_sequence,
                    exchange_trade_date=exchange_trade_date,
                    session_epoch=session_epoch,
                    session_phase=session_phase,
                    expected_symbol=algo.symbol,
                )

    def read_callback_watermark(self, *, runtime_id: str) -> str:
        if type(runtime_id) is not str or not runtime_id.strip() or runtime_id != runtime_id.strip():
            raise ValueError("runtime_id must be a non-empty trim-stable strict string")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT last_event_sequence FROM qmt_strategy.execution_runtime WHERE runtime_id=%s",
                    (runtime_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(runtime_id)
        sequence = row["last_event_sequence"]
        if type(sequence) is not int or sequence < 0:
            raise KernelRepositoryConflict("runtime callback watermark scalar is invalid")
        return f"{runtime_id}:{sequence}"

    def count_matching_callback_events(
        self,
        *,
        command_id: str,
        runtime_id: str,
        callback_watermark_before: str,
        callback_watermark_after: str,
    ) -> int:
        before = _callback_watermark_sequence(callback_watermark_before, runtime_id=runtime_id)
        after = _callback_watermark_sequence(callback_watermark_after, runtime_id=runtime_id)
        if after <= before:
            raise KernelRepositoryConflict("callback watermark interval must advance monotonically")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint AS count
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id=%s AND event_contract_version='KERNEL_V2'
                      AND sequence>%s AND sequence<=%s
                      AND event_type IN ('ORDER','TRADE','RECONCILE')
                      AND correlation_json->>'reference_command_id'=%s
                    """,
                    (runtime_id, before, after, command_id),
                )
                row = cur.fetchone()
        count = row["count"] if row is not None else None
        if type(count) is not int or count < 0:
            raise KernelRepositoryConflict("matching callback count readback is invalid")
        return count

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
        }
        if event.event_type is not EventTypeV2.TRADE:
            expected_payload["terminal"] = mapping.mapping_status is CommandChildMappingStatusV1.TERMINAL
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
            if (
                unchanged_outbox.broker_order_id is not None
                and unchanged_outbox.broker_order_id != mapping.broker_order_id
            ):
                raise ValueError("callback SUBMIT broker identity conflicts with durable ACK/reconcile evidence")
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
        self._validate_callback_payload_against_locked_authority_v1(
            cur,
            event=event,
            payload=strict_readback_kernel_event_payload_v1(event),
            previous_mapping=previous_mapping,
            successor_mapping=mapping,
        )
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
              AND mapping_status IN (
                  'DEFERRED_DEPENDENT_BUY','RESERVED','DISPATCHING','BROKER_ACCEPTED','OUTCOME_UNKNOWN'
              )
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
    def _validate_callback_payload_against_locked_authority_v1(
        cur: Any,
        *,
        event: RuntimeEventEnvelopeV2,
        payload: object,
        previous_mapping: ExecutionCommandChildMappingV1,
        successor_mapping: ExecutionCommandChildMappingV1,
    ) -> None:
        if isinstance(payload, KernelOrderEventPayloadV1):
            if payload.observed_cumulative_filled_quantity is not None and (
                payload.observed_cumulative_filled_quantity + payload.observed_remaining_quantity
                != previous_mapping.requested_quantity
            ):
                raise KernelRepositoryConflict(
                    "ORDER cumulative and remaining quantities do not close to locked mapping quantity"
                )
            return
        if isinstance(payload, KernelTradeEventPayloadV1):
            if payload.trade_quantity > previous_mapping.requested_quantity:
                raise KernelRepositoryConflict("TRADE quantity exceeds locked mapping requested quantity")
            return
        if not isinstance(payload, KernelOrderReconcileEventPayloadV1):
            raise KernelRepositoryConflict("callback payload does not use an approved strict K3 schema")
        if (
            payload.authoritative_cumulative_filled_quantity + payload.authoritative_remaining_quantity
            != previous_mapping.requested_quantity
        ):
            raise KernelRepositoryConflict(
                "RECONCILE cumulative and remaining quantities do not close to locked mapping quantity"
            )
        refs = tuple(payload.ordered_trade_refs)
        cur.execute(
            """
            SELECT event.sequence,event.payload,delivery.carrier_json
            FROM qmt_strategy.execution_runtime_event AS event
            JOIN qmt_strategy.execution_algo_event_delivery AS delivery
              ON delivery.event_id=event.event_id AND delivery.runtime_id=event.runtime_id
            WHERE event.runtime_id=%s AND event.event_type='TRADE'
              AND event.event_contract_version='KERNEL_V2'
              AND delivery.algo_instance_id=%s
            ORDER BY event.sequence,event.event_id
            FOR SHARE OF event,delivery
            """,
            (event.runtime_id, previous_mapping.algo_instance_id),
        )
        by_trade_id: dict[str, tuple[KernelTradeEventPayloadV1, AlgoDeliveryPersistenceV1, int]] = {}
        for row in cur.fetchall():
            trade_event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(row, "payload"))
            trade_payload = strict_readback_kernel_event_payload_v1(trade_event)
            if not isinstance(trade_payload, KernelTradeEventPayloadV1):
                raise KernelRepositoryConflict("TRADE event readback did not produce the strict trade payload")
            if trade_payload.trade_id in by_trade_id:
                raise KernelRepositoryConflict("durable TRADE identity is duplicated for one algo")
            delivery = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(row, "carrier_json"))
            by_trade_id[trade_payload.trade_id] = (trade_payload, delivery, int(row["sequence"]))
        selected: list[KernelTradeEventPayloadV1] = []
        watermark_sequence = _callback_watermark_sequence(payload.callback_watermark, runtime_id=event.runtime_id)
        applicable_trade_ids = {
            trade_id
            for trade_id, (trade_payload, delivery, sequence) in by_trade_id.items()
            if trade_payload.mapping_id == previous_mapping.mapping_id
            and trade_payload.local_vt_orderid == previous_mapping.local_vt_orderid
            and delivery.status is DeliveryStatusV1.APPLIED
            and sequence <= watermark_sequence
        }
        if applicable_trade_ids != {item.trade_id for item in refs}:
            raise KernelRepositoryConflict(
                "RECONCILE ordered trade refs do not equal the complete applied durable TRADE set"
            )
        for ref in refs:
            durable = by_trade_id.get(ref.trade_id)
            if durable is None:
                raise KernelRepositoryConflict("RECONCILE references a missing durable TRADE fact")
            trade_payload, delivery, sequence = durable
            if (
                trade_payload.fact_sha256 != ref.trade_fact_sha256
                or trade_payload.mapping_id != previous_mapping.mapping_id
                or trade_payload.local_vt_orderid != previous_mapping.local_vt_orderid
                or trade_payload.broker_order_id != previous_mapping.broker_order_id
                or delivery.status is not DeliveryStatusV1.APPLIED
                or sequence > watermark_sequence
            ):
                raise KernelRepositoryConflict("RECONCILE trade set differs from applied durable TRADE authority")
            selected.append(trade_payload)
        if sum(item.trade_quantity for item in selected) != payload.authoritative_cumulative_filled_quantity:
            raise KernelRepositoryConflict("RECONCILE cumulative quantity differs from exact durable TRADE set")
        if payload.terminal is not (successor_mapping.mapping_status is CommandChildMappingStatusV1.TERMINAL):
            raise KernelRepositoryConflict("RECONCILE terminal fact conflicts with mapping successor")

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

    @staticmethod
    def _validate_command_outcome_mapping_closure(
        *, event: RuntimeEventEnvelopeV2, closure: KernelCommandOutcomeMappingClosureV1 | None
    ) -> None:
        if event.event_type is not EventTypeV2.COMMAND_OUTCOME:
            if closure is not None:
                raise ValueError("non-COMMAND_OUTCOME event cannot carry an outcome mapping closure")
            return
        if not isinstance(closure, KernelCommandOutcomeMappingClosureV1):
            raise ValueError("COMMAND_OUTCOME event requires one strict mapping closure")
        correlation = thaw_json_v1(event.correlation)
        persisted_closure = correlation.get("command_outcome_mapping_closure")
        if persisted_closure is None:
            raise ValueError("COMMAND_OUTCOME event correlation must persist its exact mapping closure")
        persisted = KernelCommandOutcomeMappingClosureV1.model_validate_json(
            json.dumps(persisted_closure, sort_keys=True, separators=(",", ":"))
        )
        if persisted != closure:
            raise ValueError("COMMAND_OUTCOME event correlation differs from its mapping closure authority")
        payload = KernelCommandOutcomeEventPayloadV1.model_validate_json(
            json.dumps(thaw_json_v1(event.payload), sort_keys=True, separators=(",", ":"))
        )
        mapping = closure.mapping
        if (
            payload.runtime_id != mapping.runtime_id
            or payload.algo_instance_id != mapping.algo_instance_id
            or payload.parent_intent_id != mapping.parent_intent_id
            or payload.strategy_slot_id != mapping.strategy_slot_id
            or payload.mapping_id != mapping.mapping_id
            or payload.local_vt_orderid != mapping.local_vt_orderid
            or payload.command_id != closure.reference_command_id
        ):
            raise ValueError("COMMAND_OUTCOME payload conflicts with mapping closure owner")
        if closure.mode is KernelCommandOutcomeMappingClosureModeV1.ADVANCE_MAPPING:
            if mapping.updated_by_event_id != event.event_id:
                raise ValueError("ADVANCE_MAPPING successor must reference the exact COMMAND_OUTCOME event")
        elif closure.preceding_callback_event_id != mapping.updated_by_event_id:
            raise ValueError("VERIFY_CALLBACK_PRECEDENCE does not reference the mapping's callback authority")

    def _outcome_chain_with_cursor(
        self,
        cur: Any,
        *,
        closure: KernelCommandOutcomeMappingClosureV1,
    ) -> tuple[ExecutionCommandChildMappingV1, BrokerCommandOutboxV1, ExecutionAlgoInstancePersistenceV2]:
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
            (closure.reference_command_id, closure.mapping.mapping_id),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError((closure.mapping.mapping_id, closure.reference_command_id))
        return (
            _model_from_json(ExecutionCommandChildMappingV1, _row_json(row, "mapping_json")),
            _model_from_json(BrokerCommandOutboxV1, _row_json(row, "outbox_json")),
            _model_from_json(ExecutionAlgoInstancePersistenceV2, _row_json(row, "algo_json")),
        )

    def _apply_command_outcome_mapping_closure_with_cursor(
        self,
        cur: Any,
        *,
        event: RuntimeEventEnvelopeV2,
        closure: KernelCommandOutcomeMappingClosureV1,
    ) -> None:
        previous_mapping, outbox, previous_algo = self._outcome_chain_with_cursor(cur, closure=closure)
        payload = KernelCommandOutcomeEventPayloadV1.model_validate_json(
            json.dumps(thaw_json_v1(event.payload), sort_keys=True, separators=(",", ":"))
        )
        command = BrokerCommandV2.model_validate_json(
            json.dumps(thaw_json_v1(outbox.payload_json), sort_keys=True, separators=(",", ":"))
        )
        if (
            outbox.runtime_id != payload.runtime_id
            or outbox.algo_instance_id != payload.algo_instance_id
            or outbox.mapping_id != payload.mapping_id
            or outbox.command_id != payload.command_id
            or outbox.command_type is not payload.command_type
            or outbox.local_vt_orderid != payload.local_vt_orderid
            or outbox.row_version != payload.outbox_row_version
            or outbox.status.value != payload.outbox_status
            or previous_algo.row_version != closure.expected_algo_row_version
            or command.command_id != closure.reference_command_id
        ):
            raise KernelRepositoryConflict("COMMAND_OUTCOME closure differs from locked outbox/algo readback")
        try:
            expected_payload = build_kernel_command_outcome_event_payload_from_durable_v1(
                mapping=previous_mapping,
                outbox=outbox,
            )
        except (TypeError, ValueError) as exc:
            raise KernelRepositoryConflict(
                "locked outbox facts cannot rebuild an exact COMMAND_OUTCOME authority"
            ) from exc
        if payload != expected_payload:
            raise KernelRepositoryConflict("COMMAND_OUTCOME payload differs from the locked durable outcome authority")
        if closure.mode is KernelCommandOutcomeMappingClosureModeV1.VERIFY_CALLBACK_PRECEDENCE:
            if (
                previous_mapping != closure.mapping
                or previous_mapping.mapping_version != closure.expected_mapping_version
            ):
                raise KernelRepositoryConflict("callback-precedence mapping readback differs from closure")
            cur.execute(
                """
                SELECT event.payload_sha256,delivery.carrier_json
                FROM qmt_strategy.execution_runtime_event AS event
                JOIN qmt_strategy.execution_algo_event_delivery AS delivery
                  ON delivery.delivery_id=%s AND delivery.event_id=event.event_id
                WHERE event.event_id=%s AND event.runtime_id=%s
                  AND delivery.algo_instance_id=%s AND event.event_contract_version='KERNEL_V2'
                FOR SHARE OF event,delivery
                """,
                (
                    closure.preceding_callback_delivery_id,
                    closure.preceding_callback_event_id,
                    event.runtime_id,
                    closure.mapping.algo_instance_id,
                ),
            )
            callback_row = cur.fetchone()
            if callback_row is None:
                raise KernelRepositoryConflict("callback-precedence event/delivery readback is missing")
            callback_delivery = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(callback_row, "carrier_json"))
            if (
                str(callback_row["payload_sha256"]) != closure.preceding_callback_payload_sha256
                or callback_delivery.status is not closure.preceding_callback_delivery_status
            ):
                raise KernelRepositoryConflict("callback-precedence event/delivery facts differ from closure")
            return
        if previous_mapping.mapping_version != closure.expected_mapping_version:
            raise KernelRepositoryConflict("COMMAND_OUTCOME mapping CAS version differs from locked predecessor")
        closure.mapping.validate_successor_v1(previous_mapping)
        projection = _mapping_scalar_projection(closure.mapping)
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
                _json(closure.mapping.model_dump(mode="json")),
                closure.mapping.mapping_id,
                closure.expected_mapping_version,
            ),
        )
        if cur.rowcount != 1:
            raise KernelRepositoryConflict("COMMAND_OUTCOME mapping CAS failed")
        cur.execute(
            """
            SELECT COUNT(*) AS active_child_count
            FROM qmt_strategy.execution_child_order
            WHERE runtime_id=%s AND algo_instance_id=%s AND kernel_contract_version='KERNEL_V2'
              AND mapping_status IN (
                  'DEFERRED_DEPENDENT_BUY','RESERVED','DISPATCHING','BROKER_ACCEPTED','OUTCOME_UNKNOWN'
              )
            """,
            (closure.mapping.runtime_id, closure.mapping.algo_instance_id),
        )
        algo_payload = previous_algo.model_dump(mode="python")
        algo_payload.update(
            active_child_count=int(cur.fetchone()["active_child_count"]),
            row_version=previous_algo.row_version + 1,
            updated_at_utc=max(previous_algo.updated_at_utc, closure.mapping.updated_at_utc),
        )
        self._cas_algo_with_cursor(
            cur,
            algo_instance=ExecutionAlgoInstancePersistenceV2.model_validate(algo_payload),
            expected_row_version=previous_algo.row_version,
        )

    def _assert_command_outcome_mapping_closure_readback_with_cursor(
        self, cur: Any, *, closure: KernelCommandOutcomeMappingClosureV1
    ) -> None:
        mapping, outbox, _algo = self._outcome_chain_with_cursor(cur, closure=closure)
        if mapping != closure.mapping or outbox.command_id != closure.reference_command_id:
            raise KernelRepositoryConflict("idempotent COMMAND_OUTCOME closure readback differs")

    def ingest_routed_event_atomic(
        self,
        *,
        event: RuntimeEventEnvelopeV2,
        catalog_runtime: PluginCatalogRuntimeV2,
        correlated_algo_instance_ids: tuple[str, ...],
        callback_mapping_update: KernelCallbackMappingUpdateV1 | None = None,
        command_outcome_mapping_closure: KernelCommandOutcomeMappingClosureV1 | None = None,
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
        self._validate_command_outcome_mapping_closure(event=event, closure=command_outcome_mapping_closure)
        callback_input_hashes = () if callback_mapping_update is None else (callback_mapping_update.update_sha256,)
        callback_output_identities = (
            () if callback_mapping_update is None else (callback_mapping_update.mapping.mapping_id,)
        )
        outcome_input_hashes = (
            () if command_outcome_mapping_closure is None else (command_outcome_mapping_closure.closure_sha256,)
        )
        outcome_output_identities = (
            () if command_outcome_mapping_closure is None else (command_outcome_mapping_closure.mapping.mapping_id,)
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
                    if command_outcome_mapping_closure is not None:
                        self._assert_command_outcome_mapping_closure_readback_with_cursor(
                            cur, closure=command_outcome_mapping_closure
                        )
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
                    if command_outcome_mapping_closure is not None:
                        self._apply_command_outcome_mapping_closure_with_cursor(
                            cur,
                            event=event,
                            closure=command_outcome_mapping_closure,
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
                    if command_outcome_mapping_closure is not None:
                        owner_id = command_outcome_mapping_closure.mapping.algo_instance_id
                        owner_algo = by_id[owner_id]
                        zero_owner_terminal = (
                            not targets
                            and command_outcome_mapping_closure.mode
                            is KernelCommandOutcomeMappingClosureModeV1.VERIFY_CALLBACK_PRECEDENCE
                            and command_outcome_mapping_closure.preceding_callback_delivery_status
                            is DeliveryStatusV1.APPLIED
                            and owner_algo.status
                            not in {
                                ExecutionAlgoPersistenceStatusV2.ACTIVE,
                                ExecutionAlgoPersistenceStatusV2.PAUSED,
                            }
                        )
                        if targets not in {(owner_id,), ()} or (not targets and not zero_owner_terminal):
                            raise KernelRepositoryConflict(
                                "COMMAND_OUTCOME routing does not close to one owner or verified terminal zero-owner"
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
                            "INGEST_COMMAND_OUTCOME_MAPPING_DELIVERIES_ATOMIC"
                            if command_outcome_mapping_closure is not None
                            else "INGEST_ROUTED_EVENT_ATOMIC"
                            if callback_mapping_update is None
                            else "INGEST_CALLBACK_EVENT_MAPPING_DELIVERIES_ATOMIC"
                        ),
                        owner_identities=(event.runtime_id,),
                        input_hashes=(
                            event.event_key_sha256,
                            event.payload_sha256,
                            *callback_input_hashes,
                            *outcome_input_hashes,
                        ),
                        output_identities=(
                            event.event_id,
                            provisional.ingress_receipt_id,
                            *callback_output_identities,
                            *outcome_output_identities,
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
            if command_outcome_mapping_closure is not None:
                chain = self.read_command_identity_chain(command_outcome_mapping_closure.reference_command_id)
                if chain["mapping"] != command_outcome_mapping_closure.mapping:
                    raise KernelRepositoryConflict("idempotent COMMAND_OUTCOME mapping readback differs")
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
        if command_outcome_mapping_closure is not None:
            chain = self.read_command_identity_chain(command_outcome_mapping_closure.reference_command_id)
            if chain["mapping"] != command_outcome_mapping_closure.mapping:
                raise KernelRepositoryConflict("COMMAND_OUTCOME mapping post-commit readback differs")
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
