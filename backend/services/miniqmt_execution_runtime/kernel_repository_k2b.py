"""Atomic K2-B initialization/delivery transactions layered on the K2-A repository."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import psycopg2.extras

from .kernel_delivery import KernelAlgoStartWriteBundleV1, KernelTransitionWriteBundleV1
from .kernel_repository_common import KernelRepositoryConflict, _json, _model_from_json, _row_json
from .kernel_repository_projection import (
    _delivery_creation_matches,
    _delivery_scalar_projection,
    _event_scalar_projection,
    _transition_scalar_projection,
)
from .plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoFailureReceiptV1,
    AlgoSkipReceiptV1,
    AlgoStateSnapshotV2,
    AlgoTransitionReceiptV1,
    DeliveryStatusV1,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
    TimerMutationTypeV1,
    transaction_commit_identity_v1,
)


class KernelRepositoryK2BMixin:
    """Own pure-plugin-to-durable atomic transaction closure."""

    def initialize_algo_atomic(
        self,
        *,
        runtime_id: str,
        event_key_sha256: str,
        bundle_builder: Callable[[int], KernelAlgoStartWriteBundleV1],
    ) -> dict[str, Any]:
        if type(runtime_id) is not str or not runtime_id.strip():
            raise TypeError("runtime_id must be a non-empty string")
        if type(event_key_sha256) is not str or len(event_key_sha256) != 64:
            raise TypeError("event_key_sha256 must be a SHA-256 hex string")
        if not callable(bundle_builder):
            raise TypeError("bundle_builder must be callable")
        expected_start: KernelAlgoStartWriteBundleV1 | None = None
        transition_identity: str | None = None
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT last_event_sequence,archived_at FROM qmt_strategy.execution_runtime "
                    "WHERE runtime_id=%s FOR UPDATE",
                    (runtime_id,),
                )
                runtime_row = cur.fetchone()
                if runtime_row is None:
                    raise KeyError(runtime_id)
                if runtime_row["archived_at"] is not None:
                    raise KernelRepositoryConflict("cannot initialize an algo under an archived runtime")
                cur.execute(
                    "SELECT sequence FROM qmt_strategy.execution_runtime_event "
                    "WHERE runtime_id=%s AND event_key_sha256=%s AND event_contract_version='KERNEL_V2'",
                    (runtime_id, event_key_sha256),
                )
                existing = cur.fetchone()
                runtime_sequence = (
                    int(existing["sequence"]) if existing is not None else int(runtime_row["last_event_sequence"]) + 1
                )
                start = bundle_builder(runtime_sequence)
                if not isinstance(start, KernelAlgoStartWriteBundleV1):
                    raise TypeError("bundle_builder must return KernelAlgoStartWriteBundleV1")
                event = start.event
                initial = start.initial_delivery
                bundle = start.transition_bundle
                if (
                    event.runtime_id != runtime_id
                    or event.event_key_sha256 != event_key_sha256
                    or event.sequence != runtime_sequence
                    or event.event_type is not EventTypeV2.ALGO_START
                ):
                    raise ValueError("ALGO_START event does not close to repository initialization authority")
                try:
                    initial.validate_initial_v1()
                except ValueError as exc:
                    raise ValueError("ALGO_START requires an exact initial PENDING delivery") from exc
                if (
                    initial.event_id != event.event_id
                    or initial.runtime_id != runtime_id
                    or initial.algo_delivery_sequence != 1
                    or bundle.delivery.delivery_id != initial.delivery_id
                    or bundle.algo_instance.algo_instance_id != initial.algo_instance_id
                ):
                    raise ValueError("ALGO_START event/delivery/algo identity closure differs")
                receipt = bundle.receipt
                if isinstance(receipt, AlgoTransitionReceiptV1):
                    kind = "APPLIED"
                    transition_identity = receipt.transition_id
                    if bundle.projection_set is None or bundle.after_state is None:
                        raise ValueError("successful ALGO_START requires projection set and state")
                    transition_inputs = (
                        bundle.projection_set.projection_set_sha256,
                        bundle.after_state.state_sha256,
                        *(item.payload_sha256 for item in bundle.new_child_mappings),
                        *(item.payload_sha256 for item in bundle.command_outboxes),
                        *(item.schedule_receipt_sha256 for item in bundle.timer_schedules),
                        *(item.context_sha256 for item in bundle.diagnostic_observations),
                    )
                elif isinstance(receipt, AlgoFailureReceiptV1):
                    kind = "FAILED_TERMINAL"
                    transition_identity = receipt.failure_receipt_id
                    transition_inputs = (
                        receipt.plugin_manifest_sha256,
                        receipt.context_sha256,
                        *(item.payload_sha256 for item in bundle.command_outboxes),
                        *(item.schedule_receipt_sha256 for item in bundle.timer_schedules),
                    )
                else:
                    raise ValueError("ALGO_START cannot produce a skip receipt")
                provisional_ingress = RuntimeEventIngressReceiptV1.create(
                    runtime_id=runtime_id,
                    event_id=event.event_id,
                    event_key_sha256=event.event_key_sha256,
                    runtime_sequence=event.sequence,
                    ordered_target_algo_instance_ids=(bundle.algo_instance.algo_instance_id,),
                    ordered_delivery_ids=(initial.delivery_id,),
                    transaction_commit_identity="mqtx_pending_algo_start",
                )
                tx_identity = transaction_commit_identity_v1(
                    operation=f"INITIALIZE_ALGO_ATOMIC_{kind}",
                    owner_identities=(
                        runtime_id,
                        bundle.algo_instance.algo_instance_id,
                        event.event_id,
                        initial.delivery_id,
                    ),
                    input_hashes=(event.event_key_sha256, event.payload_sha256, *transition_inputs),
                    output_identities=(
                        event.event_id,
                        provisional_ingress.ingress_receipt_id,
                        initial.delivery_id,
                        transition_identity,
                        *(item.mapping_id for item in bundle.new_child_mappings),
                        *(item.command_id for item in bundle.command_outboxes),
                        *(item.schedule_id for item in bundle.timer_schedules),
                        *(item.observation_id for item in bundle.diagnostic_observations),
                    ),
                )
                ingress_receipt = RuntimeEventIngressReceiptV1.create(
                    runtime_id=runtime_id,
                    event_id=event.event_id,
                    event_key_sha256=event.event_key_sha256,
                    runtime_sequence=event.sequence,
                    ordered_target_algo_instance_ids=(bundle.algo_instance.algo_instance_id,),
                    ordered_delivery_ids=(initial.delivery_id,),
                    transaction_commit_identity=tx_identity,
                )
                self._validate_k2b_bundle(
                    bundle,
                    previous_delivery=initial,
                    previous_algo=None,
                    expected_delivery_row_version=1,
                    expected_algo_row_version=0,
                    expected_transaction_identity=tx_identity,
                )
                if existing is not None:
                    expected_start = start
                else:
                    event_projection = _event_scalar_projection(event, ingress_receipt)
                    cur.execute(
                        """
                        INSERT INTO qmt_strategy.execution_runtime_event(
                            event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
                            event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,
                            observed_at_utc,logical_at_utc,source_identity_json,correlation_json,
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
                            _json(ingress_receipt.model_dump(mode="json")),
                            ingress_receipt.receipt_sha256,
                            ingress_receipt.routing_rule_version,
                            tx_identity,
                        ),
                    )
                    projection = _delivery_scalar_projection(initial)
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
                            _json(initial.model_dump(mode="json")),
                        ),
                    )
                    self._write_k2b_bundle_with_cursor(
                        cur,
                        bundle,
                        previous_delivery=initial,
                        expected_delivery_row_version=1,
                        expected_algo_row_version=0,
                    )
                    cur.execute(
                        "UPDATE qmt_strategy.execution_runtime SET last_event_sequence=%s,updated_at=%s "
                        "WHERE runtime_id=%s AND last_event_sequence=%s",
                        (event.sequence, event.event_time_utc, runtime_id, event.sequence - 1),
                    )
                    if cur.rowcount != 1:
                        raise KernelRepositoryConflict("ALGO_START runtime sequence CAS failed")
                    expected_start = start
        if expected_start is None or transition_identity is None:
            raise KernelRepositoryConflict("ALGO_START transaction exited without complete expected closure")
        event_readback = self.read_event_transaction(expected_start.event.event_id)
        if (
            event_readback["event"] != expected_start.event
            or event_readback["receipt"].transaction_commit_identity
            != expected_start.transition_bundle.receipt.transaction_commit_identity
            or len(event_readback["deliveries"]) != 1
            or not _delivery_creation_matches(event_readback["deliveries"][0], expected_start.initial_delivery)
        ):
            raise KernelRepositoryConflict("ALGO_START event/receipt/delivery post-commit closure differs")
        transition_readback = self._readback_k2b_bundle(transition_identity, expected_start.transition_bundle)
        return {"event": event_readback["event"], "ingress_receipt": event_readback["receipt"], **transition_readback}

    def apply_claimed_delivery_atomic(
        self,
        *,
        delivery_id: str,
        expected_delivery_row_version: int,
        expected_algo_row_version: int,
        expected_lease_owner: str,
        expected_lease_epoch: int,
        expected_lease_fence_token: str,
        bundle_builder: Callable[
            [
                RuntimeEventEnvelopeV2,
                AlgoDeliveryPersistenceV1,
                ExecutionAlgoInstancePersistenceV2,
                AlgoStateSnapshotV2 | None,
                tuple[ExecutionCommandChildMappingV1, ...],
                tuple[ExecutionAlgoTimerScheduleV1, ...],
            ],
            KernelTransitionWriteBundleV1,
        ],
    ) -> dict[str, Any]:
        if type(delivery_id) is not str or not delivery_id.strip():
            raise TypeError("delivery_id must be a non-empty string")
        if not callable(bundle_builder):
            raise TypeError("bundle_builder must be callable")
        transition_identity: str | None = None
        expected_bundle: KernelTransitionWriteBundleV1 | None = None
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery "
                    "WHERE delivery_id=%s FOR UPDATE",
                    (delivery_id,),
                )
                delivery_row = cur.fetchone()
                if delivery_row is None:
                    raise KeyError(delivery_id)
                claimed = _model_from_json(
                    AlgoDeliveryPersistenceV1,
                    _row_json(delivery_row, "carrier_json"),
                )
                if (
                    claimed.status is not DeliveryStatusV1.CLAIMED
                    or claimed.row_version != expected_delivery_row_version
                    or claimed.lease_owner != expected_lease_owner
                    or claimed.lease_epoch != expected_lease_epoch
                    or claimed.lease_fence_token != expected_lease_fence_token
                ):
                    raise KernelRepositoryConflict("claimed delivery lease/fence/CAS authority differs")
                cur.execute(
                    """
                    SELECT delivery_id FROM qmt_strategy.execution_algo_event_delivery
                    WHERE runtime_id=%s AND algo_instance_id=%s
                      AND status NOT IN ('APPLIED','FAILED_TERMINAL','SKIPPED_TERMINAL')
                    ORDER BY algo_delivery_sequence ASC LIMIT 1 FOR UPDATE
                    """,
                    (claimed.runtime_id, claimed.algo_instance_id),
                )
                head = cur.fetchone()
                if head is None or str(head["delivery_id"]) != claimed.delivery_id:
                    raise KernelRepositoryConflict("claimed delivery is not the minimum non-terminal owner fact")
                if claimed.previous_delivery_id is not None:
                    cur.execute(
                        "SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery "
                        "WHERE delivery_id=%s FOR SHARE",
                        (claimed.previous_delivery_id,),
                    )
                    predecessor_row = cur.fetchone()
                    if predecessor_row is None:
                        raise KernelRepositoryConflict("claimed delivery predecessor is missing")
                    predecessor = _model_from_json(
                        AlgoDeliveryPersistenceV1,
                        _row_json(predecessor_row, "carrier_json"),
                    )
                    if predecessor.status not in {
                        DeliveryStatusV1.APPLIED,
                        DeliveryStatusV1.FAILED_TERMINAL,
                        DeliveryStatusV1.SKIPPED_TERMINAL,
                    }:
                        raise KernelRepositoryConflict("claimed delivery predecessor is not terminally closed")
                cur.execute(
                    "SELECT kernel_carrier_json FROM qmt_strategy.execution_algo_instance "
                    "WHERE runtime_id=%s AND algo_instance_id=%s AND kernel_contract_version='KERNEL_V2' FOR UPDATE",
                    (claimed.runtime_id, claimed.algo_instance_id),
                )
                algo_row = cur.fetchone()
                if algo_row is None:
                    raise KeyError(claimed.algo_instance_id)
                algo = _model_from_json(
                    ExecutionAlgoInstancePersistenceV2,
                    _row_json(algo_row, "kernel_carrier_json"),
                )
                if algo.row_version != expected_algo_row_version:
                    raise KernelRepositoryConflict("algo row version differs from delivery worker expectation")
                previous_state: AlgoStateSnapshotV2 | None = None
                if algo.last_applied_delivery_id is not None:
                    cur.execute(
                        """
                        SELECT t.after_state_json
                        FROM qmt_strategy.execution_algo_event_delivery d
                        JOIN qmt_strategy.execution_algo_transition t ON t.transition_id=d.transition_id
                        WHERE d.delivery_id=%s AND d.runtime_id=%s AND d.algo_instance_id=%s
                        FOR SHARE OF d,t
                        """,
                        (algo.last_applied_delivery_id, algo.runtime_id, algo.algo_instance_id),
                    )
                    state_row = cur.fetchone()
                    if state_row is None or state_row["after_state_json"] is None:
                        raise KernelRepositoryConflict("algo latest state has no exact applied transition readback")
                    previous_state = _model_from_json(
                        AlgoStateSnapshotV2,
                        _row_json(state_row, "after_state_json"),
                    )
                    if (
                        previous_state.state_sha256 != algo.state_sha256
                        or previous_state.transition_sequence != algo.last_applied_delivery_sequence
                        or previous_state.last_applied_delivery_id != algo.last_applied_delivery_id
                    ):
                        raise KernelRepositoryConflict("algo latest view drifts from applied state snapshot")
                cur.execute(
                    "SELECT payload FROM qmt_strategy.execution_runtime_event "
                    "WHERE runtime_id=%s AND event_id=%s AND event_contract_version='KERNEL_V2' FOR SHARE",
                    (claimed.runtime_id, claimed.event_id),
                )
                event_row = cur.fetchone()
                if event_row is None:
                    raise KeyError(claimed.event_id)
                event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(event_row, "payload"))
                cur.execute(
                    """
                    SELECT mapping_json FROM qmt_strategy.execution_child_order
                    WHERE runtime_id=%s AND algo_instance_id=%s AND kernel_contract_version='KERNEL_V2'
                      AND mapping_status IN ('RESERVED','DISPATCHING','BROKER_ACCEPTED','OUTCOME_UNKNOWN')
                    ORDER BY child_order_id FOR UPDATE
                    """,
                    (claimed.runtime_id, claimed.algo_instance_id),
                )
                active_mappings = tuple(
                    _model_from_json(ExecutionCommandChildMappingV1, _row_json(row, "mapping_json"))
                    for row in cur.fetchall()
                )
                cur.execute(
                    """
                    SELECT carrier_json FROM qmt_strategy.execution_algo_timer_schedule
                    WHERE runtime_id=%s AND algo_instance_id=%s AND status='SCHEDULED'
                    ORDER BY schedule_id FOR UPDATE
                    """,
                    (claimed.runtime_id, claimed.algo_instance_id),
                )
                active_timer_schedules = tuple(
                    _model_from_json(ExecutionAlgoTimerScheduleV1, _row_json(row, "carrier_json"))
                    for row in cur.fetchall()
                )
                bundle = bundle_builder(
                    event,
                    claimed,
                    algo,
                    previous_state,
                    active_mappings,
                    active_timer_schedules,
                )
                if not isinstance(bundle, KernelTransitionWriteBundleV1):
                    raise TypeError("bundle_builder must return KernelTransitionWriteBundleV1")
                self._validate_k2b_bundle(
                    bundle,
                    previous_delivery=claimed,
                    previous_algo=algo,
                    expected_delivery_row_version=expected_delivery_row_version,
                    expected_algo_row_version=expected_algo_row_version,
                )
                transition_identity = self._write_k2b_bundle_with_cursor(
                    cur,
                    bundle,
                    previous_delivery=claimed,
                    expected_delivery_row_version=expected_delivery_row_version,
                    expected_algo_row_version=expected_algo_row_version,
                )
                expected_bundle = bundle
        if transition_identity is None or expected_bundle is None:
            raise KernelRepositoryConflict("delivery transaction exited without a durable transition identity")
        return self._readback_k2b_bundle(transition_identity, expected_bundle)

    def _validate_k2b_bundle(
        self,
        bundle: KernelTransitionWriteBundleV1,
        *,
        previous_delivery: AlgoDeliveryPersistenceV1,
        previous_algo: ExecutionAlgoInstancePersistenceV2 | None,
        expected_delivery_row_version: int,
        expected_algo_row_version: int,
        expected_transaction_identity: str | None = None,
    ) -> None:
        if bundle.delivery.row_version != expected_delivery_row_version + 1:
            raise KernelRepositoryConflict("delivery bundle row version is not the exact CAS successor")
        if previous_algo is None:
            immutable_delivery_fields = (
                "delivery_id",
                "event_id",
                "runtime_id",
                "algo_instance_id",
                "plugin_manifest_sha256",
                "algo_delivery_sequence",
                "previous_delivery_id",
                "created_at_utc",
            )
            if (
                previous_delivery.algo_delivery_sequence != 1
                or previous_delivery.status is not DeliveryStatusV1.PENDING
                or bundle.delivery.status not in {DeliveryStatusV1.APPLIED, DeliveryStatusV1.FAILED_TERMINAL}
                or any(
                    getattr(bundle.delivery, field) != getattr(previous_delivery, field)
                    for field in immutable_delivery_fields
                )
            ):
                raise KernelRepositoryConflict("ALGO_START delivery is not an exact in-transaction terminal successor")
        else:
            bundle.delivery.validate_successor_v1(previous_delivery)
        if bundle.algo_instance.row_version != expected_algo_row_version + 1:
            raise KernelRepositoryConflict("algo bundle row version is not the exact CAS successor")
        if previous_algo is None:
            if expected_algo_row_version != 0 or bundle.algo_instance.row_version != 1:
                raise KernelRepositoryConflict("ALGO_START final algo requires exact first row version")
        else:
            bundle.algo_instance.validate_successor_v1(previous_algo)
        receipt = bundle.receipt
        if (
            receipt.runtime_id != previous_delivery.runtime_id
            or receipt.algo_instance_id != previous_delivery.algo_instance_id
            or receipt.event_id != previous_delivery.event_id
            or receipt.delivery_id != previous_delivery.delivery_id
            or bundle.delivery.runtime_id != receipt.runtime_id
            or bundle.delivery.algo_instance_id != receipt.algo_instance_id
            or bundle.algo_instance.algo_instance_id != receipt.algo_instance_id
        ):
            raise ValueError("K2-B transition bundle owner identities do not close")
        if isinstance(receipt, AlgoTransitionReceiptV1):
            if bundle.projection_set is None or bundle.after_state is None:
                raise ValueError("applied K2-B bundle requires projection set and after state")
            transition_id = receipt.transition_id
            kind = "APPLIED"
            expected_commands = receipt.ordered_command_ids
            expected_timers = receipt.ordered_timer_mutation_ids
            expected_diagnostics = receipt.ordered_diagnostic_observation_ids
        elif isinstance(receipt, AlgoFailureReceiptV1):
            if bundle.after_state is not None:
                raise ValueError("failure K2-B bundle cannot carry an applied state")
            if bundle.projection_set is not None and (
                bundle.projection_set.runtime_id != receipt.runtime_id
                or bundle.projection_set.algo_instance_id != receipt.algo_instance_id
                or bundle.projection_set.event_id != receipt.event_id
                or bundle.projection_set.delivery_id != receipt.delivery_id
            ):
                raise ValueError("failure K2-B projection set owner differs from failure receipt")
            transition_id = receipt.failure_receipt_id
            kind = "FAILED_TERMINAL"
            expected_commands = receipt.ordered_cancel_command_ids
            expected_timers = tuple(item.mutation_identity_v1() for item in bundle.timer_mutations)
            expected_diagnostics = ()
        elif isinstance(receipt, AlgoSkipReceiptV1):
            if any(
                (
                    bundle.projection_set is not None,
                    bundle.after_state is not None,
                    bool(bundle.new_child_mappings),
                    bool(bundle.command_outboxes),
                    bool(bundle.timer_mutations),
                    bool(bundle.timer_schedules),
                    bool(bundle.diagnostic_observations),
                )
            ):
                raise ValueError("skip K2-B bundle cannot carry effects")
            transition_id = receipt.skip_receipt_id
            kind = "SKIPPED_TERMINAL"
            expected_commands = expected_timers = expected_diagnostics = ()
        else:  # pragma: no cover - dataclass annotation guard
            raise TypeError("bundle receipt is not a strict K2 receipt")
        if tuple(item.command_id for item in bundle.command_outboxes) != expected_commands:
            raise ValueError("K2-B outbox set differs from receipt ordered command set")
        if tuple(item.mutation_identity_v1() for item in bundle.timer_mutations) != expected_timers:
            raise ValueError("K2-B timer mutation set differs from receipt ordered timer set")
        if tuple(item.observation_id for item in bundle.diagnostic_observations) != expected_diagnostics:
            raise ValueError("K2-B diagnostic set differs from receipt ordered diagnostic set")
        if len(bundle.timer_mutations) != len(bundle.timer_schedules):
            raise ValueError("each K2-B timer mutation requires one exact durable schedule successor")
        for mutation, schedule in zip(bundle.timer_mutations, bundle.timer_schedules, strict=True):
            if (
                mutation.schedule_id != schedule.schedule_id
                or mutation.algo_instance_id != schedule.algo_instance_id
                or mutation.timer_name != schedule.timer_name
                or mutation.schedule_epoch != schedule.schedule_epoch
            ):
                raise ValueError("timer mutation does not close to durable schedule")
            if mutation.mutation_type is TimerMutationTypeV1.UPSERT_ONE_SHOT:
                if mutation.due_at_exchange_utc != schedule.due_at_exchange_utc:
                    raise ValueError("timer schedule due time differs from mutation")
            elif schedule.status.value != "CANCELLED":
                raise ValueError("CANCEL timer mutation requires CANCELLED durable schedule")
        mapping_by_command = {item.command_id: item for item in bundle.new_child_mappings}
        if len(mapping_by_command) != len(bundle.new_child_mappings):
            raise ValueError("K2-B child mappings contain duplicate command identity")
        for outbox in bundle.command_outboxes:
            if outbox.transition_id != transition_id:
                raise ValueError("K2-B outbox transition identity differs from receipt")
            mapping = mapping_by_command.get(outbox.command_id)
            if mapping is not None and mapping.mapping_id != outbox.mapping_id:
                raise ValueError("K2-B mapping/outbox identity closure differs")
        input_hashes: tuple[str, ...]
        if kind == "APPLIED":
            assert bundle.projection_set is not None and bundle.after_state is not None
            input_hashes = (
                bundle.projection_set.projection_set_sha256,
                bundle.after_state.state_sha256,
                *(item.payload_sha256 for item in bundle.new_child_mappings),
                *(item.payload_sha256 for item in bundle.command_outboxes),
                *(item.schedule_receipt_sha256 for item in bundle.timer_schedules),
                *(item.context_sha256 for item in bundle.diagnostic_observations),
            )
        elif kind == "FAILED_TERMINAL":
            input_hashes = (
                receipt.plugin_manifest_sha256,
                receipt.context_sha256,
                *((bundle.projection_set.projection_set_sha256,) if bundle.projection_set is not None else ()),
                *(item.payload_sha256 for item in bundle.command_outboxes),
                *(item.schedule_receipt_sha256 for item in bundle.timer_schedules),
            )
        else:
            input_hashes = ()
        expected_tx = expected_transaction_identity or transaction_commit_identity_v1(
            operation=f"APPLY_CLAIMED_DELIVERY_ATOMIC_{kind}",
            owner_identities=(receipt.runtime_id, receipt.algo_instance_id, receipt.event_id, receipt.delivery_id),
            input_hashes=input_hashes,
            output_identities=(
                transition_id,
                *(item.mapping_id for item in bundle.new_child_mappings),
                *(item.command_id for item in bundle.command_outboxes),
                *(item.schedule_id for item in bundle.timer_schedules),
                *(item.observation_id for item in bundle.diagnostic_observations),
            ),
        )
        if receipt.transaction_commit_identity != expected_tx:
            raise ValueError("K2-B receipt does not use repository-owned atomic transaction identity")

    def _write_k2b_bundle_with_cursor(
        self,
        cur: Any,
        bundle: KernelTransitionWriteBundleV1,
        *,
        previous_delivery: AlgoDeliveryPersistenceV1,
        expected_delivery_row_version: int,
        expected_algo_row_version: int,
    ) -> str:
        receipt = bundle.receipt
        if isinstance(receipt, AlgoTransitionReceiptV1):
            kind = "APPLIED"
            transition_id = receipt.transition_id
            transition_json = receipt.model_dump(mode="json")
            failure_json = skip_json = None
        elif isinstance(receipt, AlgoFailureReceiptV1):
            kind = "FAILED_TERMINAL"
            transition_id = receipt.failure_receipt_id
            failure_json = receipt.model_dump(mode="json")
            transition_json = skip_json = None
        else:
            kind = "SKIPPED_TERMINAL"
            transition_id = receipt.skip_receipt_id
            skip_json = receipt.model_dump(mode="json")
            transition_json = failure_json = None
        projection = _transition_scalar_projection(
            receipt=receipt,
            kind=kind,
            transition_sequence=getattr(receipt, "transition_sequence", bundle.delivery.algo_delivery_sequence),
            projection_set=bundle.projection_set,
            after_state=bundle.after_state,
        )
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
                projection["transition_id"],
                projection["delivery_id"],
                projection["event_id"],
                projection["runtime_id"],
                projection["algo_instance_id"],
                projection["transition_sequence"],
                projection["transition_kind"],
                None if transition_json is None else _json(transition_json),
                None if failure_json is None else _json(failure_json),
                None if skip_json is None else _json(skip_json),
                projection["receipt_sha256"],
                None if bundle.projection_set is None else _json(bundle.projection_set.model_dump(mode="json")),
                None if bundle.projection_set is None else bundle.projection_set.projection_set_sha256,
                None if bundle.after_state is None else _json(bundle.after_state.model_dump(mode="json")),
                None if bundle.after_state is None else bundle.after_state.state_sha256,
                receipt.transaction_commit_identity,
            ),
        )
        if cur.rowcount != 1:
            raise KernelRepositoryConflict("K2-B transition identity already exists inside a non-idempotent claim")
        self._write_transition_commands_with_cursor(
            cur,
            transition_id=transition_id,
            mappings=bundle.new_child_mappings,
            outboxes=bundle.command_outboxes,
            child_price_type=2,
        )
        for schedule in bundle.timer_schedules:
            self._write_timer_schedule_with_cursor(cur, schedule)
        for observation in bundle.diagnostic_observations:
            if observation.transition_id != transition_id:
                raise ValueError("diagnostic observation transition owner differs")
            cur.execute(
                """
                INSERT INTO qmt_strategy.execution_algo_diagnostic_observation(
                    observation_id,runtime_id,algo_instance_id,event_id,transition_id,
                    observation_json,context_sha256,observed_at_utc
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (observation_id) DO NOTHING
                """,
                (
                    observation.observation_id,
                    observation.runtime_id,
                    observation.algo_instance_id,
                    observation.event_id,
                    observation.transition_id,
                    _json(observation.model_dump(mode="json")),
                    observation.context_sha256,
                    observation.observed_at_logical_utc,
                ),
            )
            if cur.rowcount != 1:
                raise KernelRepositoryConflict("diagnostic observation identity already exists inside new transition")
        self._cas_algo_with_cursor(
            cur,
            algo_instance=bundle.algo_instance,
            expected_row_version=expected_algo_row_version,
        )
        delivery_projection = _delivery_scalar_projection(bundle.delivery)
        cur.execute(
            """
            UPDATE qmt_strategy.execution_algo_event_delivery
            SET status=%s,attempt_count=%s,lease_owner=%s,lease_worker_id=%s,
                lease_process_incarnation_id=%s,lease_epoch=%s,lease_fence_token=%s,
                lease_expires_at=%s,transition_id=%s,last_error_json=%s,next_attempt_at_utc=%s,
                failure_receipt_id=%s,skip_receipt_id=%s,row_version=%s,updated_at_utc=%s,
                closed_at_utc=%s,carrier_json=%s
            WHERE delivery_id=%s AND row_version=%s
              AND lease_owner IS NOT DISTINCT FROM %s AND lease_epoch=%s
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
                _json(bundle.delivery.model_dump(mode="json")),
                bundle.delivery.delivery_id,
                expected_delivery_row_version,
                previous_delivery.lease_owner,
                previous_delivery.lease_epoch,
                previous_delivery.lease_fence_token,
            ),
        )
        # The final carrier clears lease fields; CAS must match the durable predecessor, not the final values.
        if cur.rowcount != 1:
            raise KernelRepositoryConflict("K2-B delivery CAS failed")
        return transition_id

    def _readback_k2b_bundle(
        self,
        transition_identity: str,
        expected: KernelTransitionWriteBundleV1,
    ) -> dict[str, Any]:
        readback = self.read_transition_bundle(transition_identity)
        algo_readback = self.read_algo_instance(expected.algo_instance.algo_instance_id)
        if readback["receipt"] != expected.receipt or algo_readback != expected.algo_instance:
            raise KernelRepositoryConflict("K2-B transition/algo post-commit readback differs")
        if self.read_delivery(expected.delivery.delivery_id) != expected.delivery:
            raise KernelRepositoryConflict("K2-B delivery post-commit readback differs")
        for schedule in expected.timer_schedules:
            if self.read_timer_schedule(schedule.schedule_id) != schedule:
                raise KernelRepositoryConflict("K2-B timer schedule post-commit readback differs")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT observation_json FROM qmt_strategy.execution_algo_diagnostic_observation "
                    "WHERE transition_id=%s ORDER BY observation_id",
                    (transition_identity,),
                )
                observed = tuple(row["observation_json"] for row in cur.fetchall())
        expected_observed = tuple(
            item.model_dump(mode="json")
            for item in sorted(expected.diagnostic_observations, key=lambda item: item.observation_id)
        )
        if observed != expected_observed:
            raise KernelRepositoryConflict("K2-B diagnostic post-commit readback differs")
        return {**readback, "algo": algo_readback}
