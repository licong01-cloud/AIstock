"""Transition, mapping, outbox, callback, and dispatch transactions for K2-A."""

from __future__ import annotations

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
from .plugin_canonical import canonical_utc_datetime_v1
from .kernel_repository_projection import (
    _algo_scalar_projection,
    _assert_scalar_columns,
    _delivery_scalar_projection,
    _dispatch_attempt_scalar_projection,
    _mapping_scalar_projection,
    _outbox_scalar_projection,
    _transition_retry_matches,
    _transition_scalar_projection,
)
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
    BrokerOutcomeReconciliationReceiptV1,
    CommandChildMappingStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    RuntimeEventEnvelopeV2,
    kernel_lease_fence_token_v1,
    transaction_commit_identity_v1,
)


class KernelRepositoryTransitionOutboxMixin:
    """Own the single transition, command, callback, and active-child transaction authority."""

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

    def read_reconciliation_receipt(
        self,
        command_id: str,
        reconcile_attempt: int,
    ) -> BrokerOutcomeReconciliationReceiptV1 | None:
        if type(command_id) is not str or not command_id.strip():
            raise ValueError("command_id must be a non-empty strict string")
        if type(reconcile_attempt) is not int or reconcile_attempt <= 0:
            raise ValueError("reconcile_attempt must be a positive strict integer")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT receipt_json FROM qmt_strategy.execution_broker_reconciliation_attempt
                    WHERE command_id=%s AND reconcile_attempt=%s
                    """,
                    (command_id, reconcile_attempt),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return _model_from_json(BrokerOutcomeReconciliationReceiptV1, _row_json(row, "receipt_json"))

    def append_reconciliation_receipt(
        self,
        receipt: BrokerOutcomeReconciliationReceiptV1,
    ) -> BrokerOutcomeReconciliationReceiptV1:
        if not isinstance(receipt, BrokerOutcomeReconciliationReceiptV1):
            raise TypeError("receipt must be BrokerOutcomeReconciliationReceiptV1")
        idempotent_existing = False
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT runtime_id,reconcile_receipt_json FROM qmt_strategy.execution_algo_command_outbox "
                    "WHERE command_id=%s FOR SHARE",
                    (receipt.command_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(receipt.command_id)
                previous = (
                    None
                    if row["reconcile_receipt_json"] is None
                    else _model_from_json(
                        BrokerOutcomeReconciliationReceiptV1,
                        _row_json(row, "reconcile_receipt_json"),
                    )
                )
                expected_attempt = 1 if previous is None else previous.reconcile_attempt + 1
                if receipt.reconcile_attempt != expected_attempt:
                    cur.execute(
                        "SELECT receipt_json FROM qmt_strategy.execution_broker_reconciliation_attempt "
                        "WHERE command_id=%s AND reconcile_attempt=%s",
                        (receipt.command_id, receipt.reconcile_attempt),
                    )
                    existing_row = cur.fetchone()
                    existing = (
                        None
                        if existing_row is None
                        else _model_from_json(
                            BrokerOutcomeReconciliationReceiptV1,
                            _row_json(existing_row, "receipt_json"),
                        )
                    )
                    if existing == receipt:
                        idempotent_existing = True
                    else:
                        raise KernelRepositoryConflict("reconciliation attempt is not the exact durable successor")
                if not idempotent_existing:
                    cur.execute(
                        """
                        INSERT INTO qmt_strategy.execution_broker_reconciliation_attempt(
                            receipt_sha256,command_id,runtime_id,reconcile_attempt,callback_watermark,
                            outcome,observed_at_utc,receipt_json
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (receipt_sha256) DO NOTHING
                        """,
                        (
                            receipt.receipt_sha256,
                            receipt.command_id,
                            row["runtime_id"],
                            receipt.reconcile_attempt,
                            receipt.callback_watermark,
                            receipt.outcome.value,
                            receipt.observed_at_utc,
                            _json(receipt.model_dump(mode="json")),
                        ),
                    )
        readback = self.read_reconciliation_receipt(receipt.command_id, receipt.reconcile_attempt)
        if readback != receipt:
            raise KernelRepositoryConflict("reconciliation receipt post-commit readback differs")
        return readback

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
            ):
                raise ValueError("SUBMIT mapping/outbox identity does not close")
            accepted_ack_without_event = (
                outbox.status is BrokerCommandOutboxStatusV1.ACKED
                and outbox.ack_receipt_json is not None
                and outbox.ack_receipt_json.accepted
                and outbox.broker_order_id is not None
                and mapping.broker_order_id is None
                and mapping.broker_identity_source_event_id is None
            )
            if mapping.broker_order_id != outbox.broker_order_id and not accepted_ack_without_event:
                raise ValueError("SUBMIT mapping/outbox broker identity does not close")
            coupled_states = {
                CommandChildMappingStatusV1.RESERVED: {
                    BrokerCommandOutboxStatusV1.FAILED_RETRYABLE,
                    BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
                },
                CommandChildMappingStatusV1.DISPATCHING: {
                    BrokerCommandOutboxStatusV1.DISPATCHING,
                    BrokerCommandOutboxStatusV1.ACKED,
                },
                CommandChildMappingStatusV1.BROKER_ACCEPTED: {BrokerCommandOutboxStatusV1.ACKED},
                CommandChildMappingStatusV1.BROKER_REJECTED: {BrokerCommandOutboxStatusV1.ACKED_REJECTED},
                CommandChildMappingStatusV1.OUTCOME_UNKNOWN: {
                    BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
                    BrokerCommandOutboxStatusV1.RECONCILING,
                    BrokerCommandOutboxStatusV1.ACKED,
                    BrokerCommandOutboxStatusV1.ACKED_REJECTED,
                    BrokerCommandOutboxStatusV1.FAILED_RETRYABLE,
                    BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
                },
                CommandChildMappingStatusV1.TERMINAL: {BrokerCommandOutboxStatusV1.FAILED_TERMINAL},
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
                        command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
                        and mapping.mapping_status is CommandChildMappingStatusV1.BROKER_ACCEPTED
                    ):
                        raise ValueError("SUBMIT BROKER_ACCEPTED mapping must use atomic ORDER/TRADE/RECONCILE ingress")
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
                    dispatch_attempt_id=None,
                    broker_called=None,
                    broker_order_id=None,
                    ack_receipt_json=None,
                    ack_receipt_sha256=None,
                    non_acceptance_receipt=None,
                    unknown_outcome_receipt=None,
                    reconcile_receipt=None,
                    last_error_json=None,
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
                        dispatch_attempt_id=NULL,broker_called=NULL,broker_order_id=NULL,
                        ack_receipt_json=NULL,ack_receipt_sha256=NULL,
                        non_acceptance_receipt_json=NULL,unknown_outcome_receipt_json=NULL,
                        reconcile_receipt_json=NULL,last_error_json=NULL,
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
                active_lease_match = (
                    bool(separator)
                    and outbox.attempt_count == attempt.attempt_count
                    and outbox.lease_epoch == attempt.lease_epoch
                    and outbox.lease_fence_token == attempt.lease_fence_token
                    and process_incarnation_id == attempt.process_incarnation_id
                    and (
                        outbox.dispatch_attempt_id is None or outbox.dispatch_attempt_id == attempt.dispatch_attempt_id
                    )
                )
                historical_completion_match = False
                if not active_lease_match and outbox.dispatch_attempt_id == attempt.dispatch_attempt_id:
                    cur.execute(
                        """
                        SELECT carrier_json FROM qmt_strategy.execution_algo_command_dispatch_attempt
                        WHERE dispatch_attempt_id=%s AND stage='CLAIMED'
                        """,
                        (attempt.dispatch_attempt_id,),
                    )
                    claimed_row = cur.fetchone()
                    if claimed_row is not None:
                        claimed_attempt = _model_from_json(
                            BrokerDispatchAttemptV1,
                            _row_json(claimed_row, "carrier_json"),
                        )
                        historical_completion_match = (
                            attempt.stage.value in {"COMPLETION_COMMITTED", "CLOSED"}
                            and outbox.attempt_count == attempt.attempt_count
                            and outbox.lease_epoch == attempt.lease_epoch
                            and claimed_attempt.lease_fence_token == attempt.lease_fence_token
                            and claimed_attempt.process_incarnation_id == attempt.process_incarnation_id
                        )
                if not (active_lease_match or historical_completion_match):
                    raise KernelRepositoryConflict("dispatch attempt does not close to the current outbox lease")
                if not separator and not historical_completion_match:
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
