"""Atomic K2-B initialization/delivery transactions layered on the K2-A repository."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
import hashlib
import json
from typing import Any

import psycopg2.extras

from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAuthorityInputV2,
    VnpyFacadeContractError,
    VnpyFacadeRepositoryReadRequestV1,
    VnpyFacadeRepositoryReadSetV1,
)
from .kernel_product_authority import (
    bind_product_transition_receipt_v3,
    build_product_command_authority_set_v3,
)
from .kernel_product_contracts import ProductCommandAuthorityEnvelopeV3
from .kernel_product_evidence import (
    KernelProductEvidenceProviderV3,
    bind_product_transition_bundle_v3,
)

from .kernel_delivery import (
    KernelAlgoCreationRequestV1,
    KernelAlgoCreationRequestV2,
    KernelAlgoStartWriteBundleV1,
    KernelTransitionWriteBundleV1,
    ProductDeliveryProposalV3,
    build_command_lifecycle_projection_v1,
)
from .kernel_product_contracts import ProductRouteOwnerKindV1
from .kernel_materializer import _validate_projection_lineage_v1
from .kernel_repository_common import KernelRepositoryConflict, _json, _model_from_json, _row_json
from .kernel_repository_projection import (
    _delivery_creation_matches,
    _delivery_scalar_projection,
    _event_scalar_projection,
    _mapping_scalar_projection,
    _outbox_scalar_projection,
    _transition_scalar_projection,
)
from .plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoFailureReceiptV1,
    AlgoSkipReceiptV1,
    AlgoStateSnapshotV2,
    AlgoTransitionReceiptV1,
    BrokerCommandOutboxV1,
    DeliveryStatusV1,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    KernelCommandLifecycleProjectionV1,
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
        creation_authority: KernelAlgoCreationRequestV1,
        bundle_builder: Callable[[int], KernelAlgoStartWriteBundleV1],
    ) -> dict[str, Any]:
        return self._initialize_algo_atomic_v2_aware(
            runtime_id=runtime_id,
            event_key_sha256=event_key_sha256,
            creation_authority=creation_authority,
            bundle_builder=bundle_builder,
            final_product_route=False,
        )

    def initialize_product_algo_atomic_v3(
        self,
        *,
        runtime_id: str,
        worker_incarnation_id: str,
        event_key_sha256: str,
        creation_authority: KernelAlgoCreationRequestV2,
        creation_binding: VnpyFacadeAuthorityInputV2,
        bundle_builder: Callable[[int], KernelAlgoStartWriteBundleV1],
    ) -> dict[str, Any]:
        """Persist ALGO_START and its zero-command V3 envelope atomically."""

        if not isinstance(creation_binding, VnpyFacadeAuthorityInputV2):
            raise TypeError("creation_binding must be VnpyFacadeAuthorityInputV2")
        return self._initialize_algo_atomic_v2_aware(
            runtime_id=runtime_id,
            worker_incarnation_id=worker_incarnation_id,
            event_key_sha256=event_key_sha256,
            creation_authority=creation_authority,
            creation_binding=creation_binding,
            bundle_builder=bundle_builder,
            final_product_route=True,
        )

    def _initialize_algo_atomic_v2_aware(
        self,
        *,
        runtime_id: str,
        worker_incarnation_id: str | None = None,
        event_key_sha256: str,
        creation_authority: KernelAlgoCreationRequestV1 | KernelAlgoCreationRequestV2,
        creation_binding: VnpyFacadeAuthorityInputV2 | None = None,
        bundle_builder: Callable[[int], KernelAlgoStartWriteBundleV1],
        final_product_route: bool,
    ) -> dict[str, Any]:
        if type(runtime_id) is not str or not runtime_id.strip():
            raise TypeError("runtime_id must be a non-empty string")
        if type(event_key_sha256) is not str or len(event_key_sha256) != 64:
            raise TypeError("event_key_sha256 must be a SHA-256 hex string")
        if not callable(bundle_builder):
            raise TypeError("bundle_builder must be callable")
        if final_product_route:
            if type(creation_authority) is not KernelAlgoCreationRequestV2:
                raise TypeError("final product creation requires KernelAlgoCreationRequestV2")
            if type(worker_incarnation_id) is not str or not worker_incarnation_id.strip():
                raise TypeError("final product creation requires worker_incarnation_id")
            creation_authority.validate_hashes_v2()
            if not isinstance(creation_binding, VnpyFacadeAuthorityInputV2):
                raise TypeError("final product creation requires creation_binding")
        elif type(creation_authority) is not KernelAlgoCreationRequestV1:
            raise TypeError("shadow creation requires KernelAlgoCreationRequestV1")
        else:
            creation_authority.validate_hashes_v1()
        if creation_authority.runtime_id != runtime_id:
            raise ValueError("creation authority runtime differs from repository owner")
        expected_start: KernelAlgoStartWriteBundleV1 | None = None
        transition_identity: str | None = None
        product_authority_sha256: str | None = None
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                binding_account_group: str | None = None
                if final_product_route:
                    assert type(creation_authority) is KernelAlgoCreationRequestV2
                    binding_account_group = self._lock_and_validate_product_binding_v2_with_cursor(
                        cur, authority=creation_authority
                    )
                cur.execute(
                    "SELECT last_event_sequence,archived_at,trade_date,mode,account_group_id "
                    "FROM qmt_strategy.execution_runtime "
                    "WHERE runtime_id=%s FOR UPDATE",
                    (runtime_id,),
                )
                runtime_row = cur.fetchone()
                if runtime_row is None:
                    raise KeyError(runtime_id)
                if runtime_row["archived_at"] is not None:
                    raise KernelRepositoryConflict("cannot initialize an algo under an archived runtime")
                if final_product_route:
                    assert type(creation_authority) is KernelAlgoCreationRequestV2
                    if runtime_row["account_group_id"] != binding_account_group:
                        raise KernelRepositoryConflict(
                            "K6-D execution runtime account group differs from locked MiniQMT binding"
                        )
                    self._lock_and_validate_product_route_v2_with_cursor(
                        cur,
                        authority=creation_authority,
                        runtime_trade_date=runtime_row["trade_date"],
                        runtime_mode=runtime_row["mode"],
                        worker_incarnation_id=worker_incarnation_id,
                    )
                existing_parent_slot_algos = self._lock_and_validate_creation_authority_with_cursor(
                    cur, creation_authority
                )
                cur.execute(
                    "SELECT sequence FROM qmt_strategy.execution_runtime_event "
                    "WHERE runtime_id=%s AND event_key_sha256=%s AND event_contract_version='KERNEL_V2'",
                    (runtime_id, event_key_sha256),
                )
                existing = cur.fetchone()
                runtime_sequence = (
                    int(existing["sequence"]) if existing is not None else int(runtime_row["last_event_sequence"]) + 1
                )
                if final_product_route and runtime_sequence < creation_authority.effective_new_instance_sequence:
                    raise KernelRepositoryConflict(
                        "K6-D ALGO_START sequence predates the durable product route cutover"
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
                if (
                    bundle.algo_instance.parent_intent_id != creation_authority.parent_intent_id
                    or bundle.algo_instance.strategy_slot_id != creation_authority.strategy_slot_id
                    or bundle.algo_instance.symbol != creation_authority.symbol
                    or bundle.algo_instance.side != creation_authority.side
                    or bundle.algo_instance.target_quantity != creation_authority.parent_quantity
                ):
                    raise ValueError("ALGO_START algo owner differs from locked parent authority")
                if existing_parent_slot_algos and existing_parent_slot_algos != (
                    bundle.algo_instance.algo_instance_id,
                ):
                    raise KernelRepositoryConflict("parent/slot is already owned by a different KERNEL_V2 algo")
                receipt = bundle.receipt
                if isinstance(receipt, AlgoTransitionReceiptV1):
                    kind = "APPLIED"
                    transition_identity = receipt.transition_id
                    if bundle.projection_set is None or bundle.after_state is None:
                        raise ValueError("successful ALGO_START requires projection set and state")
                    if final_product_route:
                        if bundle.applied_transition is None:
                            raise KernelRepositoryConflict("product ALGO_START lacks the applied transition carrier")
                        if bundle.applied_transition.broker_commands:
                            raise KernelRepositoryConflict("product ALGO_START cannot carry broker commands")
                        assert creation_binding is not None
                        bound_receipt = bind_product_transition_receipt_v3(
                            transition=bundle.applied_transition,
                            transition_receipt=receipt,
                            ordered_evidence=(),
                            timer_schedules=bundle.timer_schedules,
                        )
                        bundle = KernelTransitionWriteBundleV1.create(
                            algo_instance=bundle.algo_instance,
                            delivery=bundle.delivery,
                            receipt=bound_receipt,
                            projection_set=bundle.projection_set,
                            after_state=bundle.after_state,
                            applied_transition=bundle.applied_transition,
                            new_child_mappings=bundle.new_child_mappings,
                            command_outboxes=bundle.command_outboxes,
                            updated_child_mappings=bundle.updated_child_mappings,
                            updated_command_outboxes=bundle.updated_command_outboxes,
                            timer_mutations=bundle.timer_mutations,
                            timer_schedules=bundle.timer_schedules,
                            diagnostic_observations=bundle.diagnostic_observations,
                        )
                        receipt = bound_receipt
                        authority = build_product_command_authority_set_v3(
                            transition=bundle.applied_transition,
                            transition_receipt=bound_receipt,
                            projection_set=bundle.projection_set,
                            ordered_evidence=(),
                            catalog=creation_binding.plugin_catalog_snapshot,
                            creation_binding=creation_binding,
                            timer_schedules=bundle.timer_schedules,
                        )
                        product_envelope = ProductCommandAuthorityEnvelopeV3.create(
                            authority_set=authority,
                            creation_authority=creation_binding,
                            ordered_timer_schedules=bundle.timer_schedules,
                        )
                        product_authority_sha256 = authority.authority_set_sha256
                        start = KernelAlgoStartWriteBundleV1(
                            event=start.event,
                            initial_delivery=start.initial_delivery,
                            transition_bundle=bundle,
                        )
                    lifecycle_projection = KernelCommandLifecycleProjectionV1.create(
                        runtime_id=event.runtime_id,
                        algo_instance_id=bundle.algo_instance.algo_instance_id,
                        event_id=event.event_id,
                        delivery_id=initial.delivery_id,
                        ordered_items=(),
                    )
                    transition_inputs = (
                        bundle.projection_set.projection_set_sha256,
                        lifecycle_projection.projection_sha256,
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
                tx_identity = (
                    receipt.transaction_commit_identity
                    if final_product_route and isinstance(receipt, AlgoTransitionReceiptV1)
                    else transaction_commit_identity_v1(
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
                    event=event,
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
                    if final_product_route and isinstance(receipt, AlgoTransitionReceiptV1):
                        prepared = self._prepare_product_materialization_v3(
                            envelope=product_envelope,
                            transition_bundle=bundle,
                            strategy_slot_id=creation_authority.strategy_slot_id,
                        )
                        self._write_prepared_product_materialization_with_cursor(
                            cur,
                            envelope=product_envelope,
                            transition_bundle=bundle,
                            prepared=prepared,
                            previous_delivery=initial,
                            expected_row_version=0,
                            expected_delivery_row_version=1,
                        )
                    else:
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
        result = {"event": event_readback["event"], "ingress_receipt": event_readback["receipt"], **transition_readback}
        if product_authority_sha256 is not None:
            result["product_materialization"] = self.read_product_materialization_v3(product_authority_sha256)[2]
        return result

    @staticmethod
    def _lock_and_validate_product_binding_v2_with_cursor(cur: Any, *, authority: KernelAlgoCreationRequestV2) -> str:
        """Read the immutable MiniQMT SIM binding before taking the runtime lock."""

        cur.execute(
            "SELECT binding_id,release_id,release_hash,broker_backend,broker_account_id,account_group_id,"
            "effective_from,effective_to,binding_hash FROM paper_v2.simulation_release_binding "
            "WHERE binding_id=%s FOR SHARE",
            (authority.binding_id,),
        )
        binding = cur.fetchone()
        if binding is None:
            raise KernelRepositoryConflict("K6-D MiniQMT binding authority is missing")
        if (
            binding["release_id"] != authority.release_id
            or binding["release_hash"] != authority.release_sha256
            or binding["broker_backend"] != "minqmt_sim"
        ):
            raise KernelRepositoryConflict("K6-D binding/release/backend authority differs from creation request")
        account_group_id = binding["account_group_id"]
        broker_account_id = binding["broker_account_id"]
        if (
            type(account_group_id) is not str
            or not account_group_id.strip()
            or type(broker_account_id) is not str
            or not broker_account_id.strip()
        ):
            raise KernelRepositoryConflict("K6-D MiniQMT binding lacks exact account authority")
        trade_date = date.fromisoformat(authority.exchange_trade_date)
        effective_from = binding["effective_from"]
        effective_to = binding["effective_to"]
        if effective_from is not None and (type(effective_from) is not date or trade_date < effective_from):
            raise KernelRepositoryConflict("K6-D binding is not effective on the creation trade date")
        if effective_to is not None and (type(effective_to) is not date or trade_date > effective_to):
            raise KernelRepositoryConflict("K6-D binding is not effective on the creation trade date")
        cur.execute(
            "SELECT release_hash FROM strategy_pkg.strategy_runtime_release WHERE release_id=%s FOR SHARE",
            (authority.release_id,),
        )
        release = cur.fetchone()
        if release is None or release["release_hash"] != authority.release_sha256:
            raise KernelRepositoryConflict("K6-D runtime release strict readback differs from binding authority")
        return account_group_id

    def _lock_and_validate_product_route_v2_with_cursor(
        self,
        cur: Any,
        *,
        authority: KernelAlgoCreationRequestV2,
        runtime_trade_date: Any,
        runtime_mode: Any,
        worker_incarnation_id: str,
    ) -> None:
        """Lock and compare the sole K6-D owner before an ALGO_START write."""

        if type(runtime_trade_date) is not date:
            raise KernelRepositoryConflict("K6-D runtime trade date readback is invalid")
        if runtime_mode != "SIM":
            raise KernelRepositoryConflict("K6-D product ALGO_START requires a SIM execution runtime")
        if authority.exchange_trade_date != runtime_trade_date.isoformat():
            raise KernelRepositoryConflict("K6-D request trade date differs from locked runtime")
        self._verify_k6_product_process_cursor(cur, worker_incarnation_id)
        owner, receipt = self._read_product_route_owner_with_cursor(
            cur,
            runtime_id=authority.runtime_id,
            binding_id=authority.binding_id,
            trade_date=runtime_trade_date,
            lock=True,
        )
        if owner.route_owner is not ProductRouteOwnerKindV1.KERNEL_V2:
            raise KernelRepositoryConflict("K6-D product route owner is not KERNEL_V2")
        if (
            owner.owner_sha256,
            owner.current_receipt_sha256,
            owner.current_route_epoch,
            owner.effective_new_instance_sequence,
        ) != (
            authority.product_route_owner_sha256,
            authority.product_route_cutover_receipt_sha256,
            authority.product_route_epoch,
            authority.effective_new_instance_sequence,
        ):
            raise KernelRepositoryConflict("K6-D creation request route lineage differs from locked owner")
        if (
            receipt.route_owner is not ProductRouteOwnerKindV1.KERNEL_V2
            or receipt.runtime_id != authority.runtime_id
            or receipt.binding_id != authority.binding_id
            or receipt.trade_date != runtime_trade_date
        ):
            raise KernelRepositoryConflict("K6-D locked product route receipt identity is invalid")

    @staticmethod
    def _verify_k6_product_process_cursor(cur: Any, process_incarnation_id: str) -> None:
        cur.execute(
            "SELECT incarnation.worker_id FROM qmt_strategy.execution_kernel_worker_incarnation AS incarnation "
            "JOIN qmt_strategy.execution_kernel_worker_epoch AS epoch "
            "ON epoch.worker_id=incarnation.worker_id AND epoch.process_role=incarnation.process_role "
            "AND epoch.incarnation_sequence=incarnation.incarnation_sequence "
            "WHERE incarnation.process_incarnation_id=%s AND incarnation.process_role='PRODUCT_COORDINATOR' "
            "FOR SHARE OF incarnation,epoch",
            (process_incarnation_id,),
        )
        rows = cur.fetchall()
        if len(rows) != 1:
            raise KernelRepositoryConflict("K6-D product creation uses a stale or ambiguous worker incarnation")

    def _lock_and_validate_creation_authority_with_cursor(
        self,
        cur: Any,
        authority: KernelAlgoCreationRequestV1,
    ) -> tuple[str, ...]:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"{authority.runtime_id}|{authority.parent_intent_id}|{authority.strategy_slot_id}",),
        )
        if type(authority) is KernelAlgoCreationRequestV2:
            self._lock_and_validate_k6d_plan_parent_with_cursor(cur, authority=authority)
        else:
            cur.execute(
                """
                SELECT runtime_id,execution_plan_id,execution_plan_hash,release_id,symbol,side,
                       emitted_parent_quantity,execution_policy_id,execution_policy_sha256
                FROM qmt_strategy.execution_parent_benchmark
                WHERE parent_intent_id=%s
                ORDER BY parent_revision DESC LIMIT 1 FOR SHARE
                """,
                (authority.parent_intent_id,),
            )
            parent = cur.fetchone()
            if parent is None:
                raise KernelRepositoryConflict("ALGO_START parent benchmark authority is missing")
            expected_parent = {
                "runtime_id": authority.runtime_id,
                "execution_plan_id": authority.execution_plan_id,
                "execution_plan_hash": authority.execution_plan_sha256,
                "release_id": authority.release_id,
                "symbol": authority.symbol,
                "side": authority.side.value,
                "emitted_parent_quantity": authority.parent_quantity,
                "execution_policy_id": authority.policy_id,
                "execution_policy_sha256": authority.policy_sha256,
            }
            actual_parent = {key: parent[key] for key in expected_parent}
            actual_parent["emitted_parent_quantity"] = int(actual_parent["emitted_parent_quantity"])
            if actual_parent != expected_parent:
                raise KernelRepositoryConflict("ALGO_START parent benchmark authority conflicts with request")
        cur.execute(
            """
            SELECT release_hash,execution_policy_version_id,execution_policy_sha256
            FROM strategy_pkg.strategy_runtime_release WHERE release_id=%s FOR SHARE
            """,
            (authority.release_id,),
        )
        release = cur.fetchone()
        if release is None:
            raise KernelRepositoryConflict("ALGO_START runtime release authority is missing")
        if (
            str(release["release_hash"]) != authority.release_sha256
            or str(release["execution_policy_version_id"]) != authority.policy_id
            or str(release["execution_policy_sha256"]) != authority.policy_sha256
        ):
            raise KernelRepositoryConflict("ALGO_START release/policy authority conflicts with request")
        cur.execute(
            """
            SELECT algo_instance_id FROM qmt_strategy.execution_algo_instance
            WHERE runtime_id=%s AND parent_intent_id=%s AND strategy_slot_id=%s
              AND kernel_contract_version='KERNEL_V2'
            ORDER BY algo_instance_id FOR UPDATE
            """,
            (authority.runtime_id, authority.parent_intent_id, authority.strategy_slot_id),
        )
        existing = tuple(str(row["algo_instance_id"]) for row in cur.fetchall())
        if len(existing) > 1:
            raise KernelRepositoryConflict("multiple KERNEL_V2 algos own the same parent/slot authority")
        return existing

    @staticmethod
    def _lock_and_validate_k6d_plan_parent_with_cursor(
        cur: Any,
        *,
        authority: KernelAlgoCreationRequestV2,
    ) -> None:
        """Lock the compiler-owned K6-D parent instead of requiring offline TCA evidence."""

        cur.execute(
            "SELECT plan_id,plan_hash,binding_id,release_id,target_trade_date,execution_policy_version_id,"
            "execution_policy_sha256,plan_payload_json FROM paper_v2.execution_plan "
            "WHERE plan_id=%s FOR SHARE",
            (authority.execution_plan_id,),
        )
        plan = cur.fetchone()
        if plan is None:
            raise KernelRepositoryConflict("K6-D ALGO_START execution plan authority is missing")
        payload = plan["plan_payload_json"]
        if type(payload) is not dict:
            raise KernelRepositoryConflict("K6-D ALGO_START execution plan payload is invalid")
        digest = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        expected_runtime_id = "mqrt_sim_" + hashlib.sha256(
            json.dumps(
                {
                    "binding_id": authority.binding_id,
                    "plan_id": authority.execution_plan_id,
                    "trade_date": authority.exchange_trade_date,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:24]
        expected_plan = (
            authority.execution_plan_id,
            authority.execution_plan_sha256,
            authority.binding_id,
            authority.release_id,
            date.fromisoformat(authority.exchange_trade_date),
            authority.policy_id,
            authority.policy_sha256,
        )
        actual_plan = (
            plan["plan_id"],
            plan["plan_hash"],
            plan["binding_id"],
            plan["release_id"],
            plan["target_trade_date"],
            plan["execution_policy_version_id"],
            plan["execution_policy_sha256"],
        )
        if (
            authority.runtime_id != expected_runtime_id
            or actual_plan != expected_plan
            or digest != plan["plan_hash"]
        ):
            raise KernelRepositoryConflict("K6-D ALGO_START execution plan authority conflicts with request")
        if (
            payload.get("schema_version") != "execution_plan_v1"
            or payload.get("binding_id") != authority.binding_id
            or payload.get("release_id") != authority.release_id
            or payload.get("target_trade_date") != authority.exchange_trade_date
        ):
            raise KernelRepositoryConflict("K6-D ALGO_START execution plan envelope conflicts with request")
        intents = payload.get("intents")
        if type(intents) is not list or not intents or any(type(item) is not dict for item in intents):
            raise KernelRepositoryConflict("K6-D ALGO_START execution plan parent set is invalid")
        intent_ids = [item.get("intent_id") for item in intents]
        if any(type(item) is not str or not item.strip() for item in intent_ids) or len(intent_ids) != len(
            set(intent_ids)
        ):
            raise KernelRepositoryConflict("K6-D ALGO_START execution plan parent set is invalid")
        matches = [item for item in intents if item["intent_id"] == authority.parent_intent_id]
        if len(matches) != 1:
            raise KernelRepositoryConflict("K6-D ALGO_START parent is missing from the frozen execution plan")
        parent = matches[0]
        if (
            parent.get("symbol") != authority.symbol
            or parent.get("side") != authority.side.value
            or type(parent.get("order_quantity")) is not int
            or parent["order_quantity"] != authority.parent_quantity
        ):
            raise KernelRepositoryConflict("K6-D ALGO_START frozen execution-plan parent conflicts with request")

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
                tuple[BrokerCommandOutboxV1, ...],
                tuple[ExecutionAlgoTimerScheduleV1, ...],
                VnpyFacadeRepositoryReadSetV1 | None,
            ],
            KernelTransitionWriteBundleV1,
        ],
        facade_read_request: VnpyFacadeRepositoryReadRequestV1 | None = None,
    ) -> dict[str, Any]:
        return self._apply_claimed_delivery_atomic_v3_aware(
            delivery_id=delivery_id,
            expected_delivery_row_version=expected_delivery_row_version,
            expected_algo_row_version=expected_algo_row_version,
            expected_lease_owner=expected_lease_owner,
            expected_lease_epoch=expected_lease_epoch,
            expected_lease_fence_token=expected_lease_fence_token,
            bundle_builder=bundle_builder,
            facade_read_request=facade_read_request,
            product_evidence_provider=None,
        )

    def apply_claimed_product_delivery_atomic_v3(
        self,
        *,
        delivery_id: str,
        expected_delivery_row_version: int,
        expected_algo_row_version: int,
        expected_lease_owner: str,
        expected_lease_epoch: int,
        expected_lease_fence_token: str,
        proposal_builder: Callable[..., ProductDeliveryProposalV3 | KernelTransitionWriteBundleV1],
        product_evidence_provider: KernelProductEvidenceProviderV3,
        facade_read_request: VnpyFacadeRepositoryReadRequestV1 | None = None,
    ) -> dict[str, Any]:
        if not isinstance(product_evidence_provider, KernelProductEvidenceProviderV3):
            raise TypeError("product_evidence_provider must be KernelProductEvidenceProviderV3")
        return self._apply_claimed_delivery_atomic_v3_aware(
            delivery_id=delivery_id,
            expected_delivery_row_version=expected_delivery_row_version,
            expected_algo_row_version=expected_algo_row_version,
            expected_lease_owner=expected_lease_owner,
            expected_lease_epoch=expected_lease_epoch,
            expected_lease_fence_token=expected_lease_fence_token,
            bundle_builder=proposal_builder,
            facade_read_request=facade_read_request,
            product_evidence_provider=product_evidence_provider,
        )

    def _apply_claimed_delivery_atomic_v3_aware(
        self,
        *,
        delivery_id: str,
        expected_delivery_row_version: int,
        expected_algo_row_version: int,
        expected_lease_owner: str,
        expected_lease_epoch: int,
        expected_lease_fence_token: str,
        bundle_builder: Callable[..., Any],
        facade_read_request: VnpyFacadeRepositoryReadRequestV1 | None,
        product_evidence_provider: KernelProductEvidenceProviderV3 | None,
    ) -> dict[str, Any]:
        if type(delivery_id) is not str or not delivery_id.strip():
            raise TypeError("delivery_id must be a non-empty string")
        if not callable(bundle_builder):
            raise TypeError("bundle_builder must be callable")
        if facade_read_request is not None and not isinstance(facade_read_request, VnpyFacadeRepositoryReadRequestV1):
            raise TypeError("facade_read_request must be VnpyFacadeRepositoryReadRequestV1 or None")
        transition_identity: str | None = None
        expected_bundle: KernelTransitionWriteBundleV1 | None = None
        product_authority_sha256: str | None = None
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
                    ORDER BY local_vt_orderid,mapping_id FOR UPDATE
                    """,
                    (claimed.runtime_id, claimed.algo_instance_id),
                )
                locked_mappings = tuple(
                    _model_from_json(ExecutionCommandChildMappingV1, _row_json(row, "mapping_json"))
                    for row in cur.fetchall()
                )
                cur.execute(
                    """
                    SELECT carrier_json FROM qmt_strategy.execution_algo_command_outbox
                    WHERE runtime_id=%s AND algo_instance_id=%s
                    ORDER BY local_vt_orderid,command_id FOR UPDATE
                    """,
                    (claimed.runtime_id, claimed.algo_instance_id),
                )
                locked_command_outboxes = tuple(
                    _model_from_json(BrokerCommandOutboxV1, _row_json(row, "carrier_json")) for row in cur.fetchall()
                )
                active_mappings = tuple(
                    item
                    for item in locked_mappings
                    if item.mapping_status.value
                    in {
                        "RESERVED",
                        "DISPATCHING",
                        "BROKER_ACCEPTED",
                        "OUTCOME_UNKNOWN",
                    }
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
                facade_read_set: VnpyFacadeRepositoryReadSetV1 | None = None
                if facade_read_request is not None:
                    request = facade_read_request
                    if (
                        request.runtime_id != event.runtime_id
                        or request.algo_instance_id != algo.algo_instance_id
                        or request.current_event_id != event.event_id
                        or request.current_event_sequence != event.sequence
                        or request.current_delivery_id != claimed.delivery_id
                        or request.current_delivery_sequence != claimed.algo_delivery_sequence
                    ):
                        raise VnpyFacadeContractError(
                            "MINIQMT_VNPY_FACADE_REPOSITORY_READ_INVALID",
                            "facade repository request differs from the locked event/delivery/algo cutoff",
                            context={
                                "runtime_id": event.runtime_id,
                                "algo_instance_id": algo.algo_instance_id,
                                "event_id": event.event_id,
                                "delivery_id": claimed.delivery_id,
                                "request_sha256": request.request_sha256,
                            },
                        )
                    algo_start_read = self._read_facade_algo_start_event_with_cursor(
                        cur,
                        runtime_id=event.runtime_id,
                        algo_instance_id=algo.algo_instance_id,
                    )
                    latest_tick = None
                    if event.event_type is EventTypeV2.TIMER:
                        latest_tick = self._read_facade_latest_prior_tick_with_cursor(
                            cur,
                            runtime_id=event.runtime_id,
                            algo_instance_id=algo.algo_instance_id,
                            cutoff_delivery_sequence=claimed.algo_delivery_sequence,
                            cutoff_event_sequence=event.sequence,
                            exchange_trade_date=request.exchange_trade_date,
                            session_epoch=request.session_epoch,
                            session_phase=request.session_phase,
                            expected_symbol=algo.symbol,
                        )
                    facade_read_set = VnpyFacadeRepositoryReadSetV1.create(
                        request=request,
                        algo_start_read=algo_start_read,
                        latest_prior_tick_read_or_null=latest_tick,
                    )
                builder_args = (
                    event,
                    claimed,
                    algo,
                    previous_state,
                    locked_mappings,
                    locked_command_outboxes,
                    active_timer_schedules,
                    facade_read_set,
                )
                if product_evidence_provider is not None:
                    product_base_services = product_evidence_provider.build_base_services_with_cursor_v1(
                        cur=cur,
                        event=event,
                        delivery=claimed,
                        algo=algo,
                    )
                    built = bundle_builder(*builder_args, product_base_services)
                else:
                    built = bundle_builder(*builder_args)
                proposal = built if isinstance(built, ProductDeliveryProposalV3) else None
                bundle = proposal.transition_bundle if proposal is not None else built
                if not isinstance(bundle, KernelTransitionWriteBundleV1):
                    raise TypeError(
                        "bundle_builder must return KernelTransitionWriteBundleV1 or ProductDeliveryProposalV3"
                    )
                if product_evidence_provider is None and proposal is not None:
                    raise TypeError("shadow delivery cannot supply a product proposal")
                if product_evidence_provider is not None:
                    if isinstance(bundle.receipt, AlgoTransitionReceiptV1) and proposal is None:
                        raise TypeError("applied product delivery requires one exact V3 proposal")
                    if not isinstance(bundle.receipt, AlgoTransitionReceiptV1) and proposal is not None:
                        raise TypeError("terminal product failure/skip cannot manufacture a command proposal")
                if isinstance(bundle.receipt, AlgoFailureReceiptV1):
                    durable_active_child_ids = tuple(item.child_order_id for item in active_mappings)
                    if bundle.receipt.ordered_active_child_ids != durable_active_child_ids:
                        raise KernelRepositoryConflict(
                            "failure receipt active-child set differs from locked durable authority"
                        )
                    durable_pre_call_mapping_ids = tuple(
                        item.mapping_id for item in active_mappings if item.mapping_status.value == "RESERVED"
                    )
                    if tuple(item.mapping_id for item in bundle.updated_child_mappings) != durable_pre_call_mapping_ids:
                        raise KernelRepositoryConflict(
                            "failure terminal mapping set differs from locked pre-call authority"
                        )
                    durable_cancel_mapping_ids = tuple(
                        item.mapping_id for item in active_mappings if item.broker_order_id is not None
                    )
                    if tuple(item.mapping_id for item in bundle.command_outboxes) != durable_cancel_mapping_ids:
                        raise KernelRepositoryConflict(
                            "failure CANCEL outbox set differs from locked broker-accepted child authority"
                        )
                    expected_active_count = len(active_mappings) - len(bundle.updated_child_mappings)
                    if bundle.algo_instance.active_child_count != expected_active_count:
                        raise KernelRepositoryConflict(
                            "failure algo active-child count differs from post-update durable authority"
                        )
                    has_unknown = any(
                        item.broker_order_id is None and item.mapping_status.value != "RESERVED"
                        for item in active_mappings
                    )
                    expected_closure = (
                        ActiveChildClosureStatusV1.CLEAN
                        if expected_active_count == 0
                        else ActiveChildClosureStatusV1.OUTCOME_UNKNOWN
                        if has_unknown
                        else ActiveChildClosureStatusV1.CANCEL_PENDING
                    )
                    if bundle.algo_instance.active_child_closure_status is not expected_closure:
                        raise KernelRepositoryConflict(
                            "failure algo active-child closure differs from locked durable authority"
                        )
                if previous_state is None:
                    raise KernelRepositoryConflict("claimed active algo has no exact previous state")
                lifecycle_projection = build_command_lifecycle_projection_v1(
                    event=event,
                    delivery=claimed,
                    previous_state=previous_state,
                    mappings=active_mappings,
                    outboxes=locked_command_outboxes,
                )
                self._validate_k2b_bundle(
                    bundle,
                    event=event,
                    previous_delivery=claimed,
                    previous_algo=algo,
                    expected_delivery_row_version=expected_delivery_row_version,
                    expected_algo_row_version=expected_algo_row_version,
                    command_lifecycle_projection_sha256=lifecycle_projection.projection_sha256,
                )
                if proposal is not None and isinstance(bundle.receipt, AlgoTransitionReceiptV1):
                    if bundle.applied_transition is None:
                        raise KernelRepositoryConflict("product proposal lacks applied transition authority")
                    assert product_evidence_provider is not None
                    evidence = product_evidence_provider.build_with_cursor_v1(
                        cur=cur,
                        event=event,
                        delivery=claimed,
                        algo=algo,
                        transition=bundle.applied_transition,
                        base_services=proposal.base_services,
                        route_receipt=proposal.route_receipt,
                    )
                    replay_bundle = proposal.replay_builder(evidence.services)
                    bound = bind_product_transition_bundle_v3(
                        proposal_bundle=bundle,
                        replay_bundle=replay_bundle,
                        evidence=evidence,
                        creation_binding=proposal.creation_binding,
                    )
                    bundle = bound.transition_bundle
                    self._validate_k2b_bundle(
                        bundle,
                        event=event,
                        previous_delivery=claimed,
                        previous_algo=algo,
                        expected_delivery_row_version=expected_delivery_row_version,
                        expected_algo_row_version=expected_algo_row_version,
                        command_lifecycle_projection_sha256=lifecycle_projection.projection_sha256,
                    )
                    prepared = self._prepare_product_materialization_v3(
                        envelope=bound.authority_envelope,
                        transition_bundle=bundle,
                        strategy_slot_id=algo.strategy_slot_id,
                    )
                    self._write_prepared_product_materialization_with_cursor(
                        cur,
                        envelope=bound.authority_envelope,
                        transition_bundle=bundle,
                        prepared=prepared,
                        previous_delivery=claimed,
                        expected_row_version=expected_algo_row_version,
                        expected_delivery_row_version=expected_delivery_row_version,
                    )
                    product_authority_sha256 = bound.authority_envelope.authority_set.authority_set_sha256
                    transition_identity = bundle.receipt.transition_id
                else:
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
        if product_authority_sha256 is not None:
            authority, lifecycle, receipt = self.read_product_materialization_v3(product_authority_sha256)
            return {
                "algo": self.read_algo_instance(authority.algo_instance_id),
                "delivery": self.read_delivery(authority.delivery_id),
                "receipt": receipt,
                "product_authority": authority,
                "product_lifecycle": lifecycle,
            }
        return self._readback_k2b_bundle(transition_identity, expected_bundle)

    def _validate_k2b_bundle(
        self,
        bundle: KernelTransitionWriteBundleV1,
        *,
        event: RuntimeEventEnvelopeV2 | None = None,
        previous_delivery: AlgoDeliveryPersistenceV1,
        previous_algo: ExecutionAlgoInstancePersistenceV2 | None,
        expected_delivery_row_version: int,
        expected_algo_row_version: int,
        expected_transaction_identity: str | None = None,
        command_lifecycle_projection_sha256: str | None = None,
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
            if bundle.updated_child_mappings or bundle.updated_command_outboxes:
                raise ValueError("applied K2-B bundle cannot terminalize prior submit authority")
            if event is not None:
                _validate_projection_lineage_v1(
                    event=event,
                    projection_set=bundle.projection_set,
                    consumed_lineage_refs=receipt.ordered_consumed_lineage_refs,
                    has_broker_commands=bool(bundle.command_outboxes),
                )
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
                    bool(bundle.updated_child_mappings),
                    bool(bundle.updated_command_outboxes),
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
        if len(bundle.updated_child_mappings) != len(bundle.updated_command_outboxes):
            raise ValueError("K2-B mapping/outbox terminal update cardinality differs")
        for mapping, outbox in zip(
            bundle.updated_child_mappings,
            bundle.updated_command_outboxes,
            strict=True,
        ):
            if (
                mapping.mapping_id != outbox.mapping_id
                or mapping.command_id != outbox.command_id
                or mapping.mapping_status.value != "TERMINAL"
                or outbox.status.value != "FAILED_TERMINAL"
                or outbox.broker_called is not False
            ):
                raise ValueError("K2-B pre-call terminal mapping/outbox closure differs")
        input_hashes: tuple[str, ...]
        if kind == "APPLIED":
            assert bundle.projection_set is not None and bundle.after_state is not None
            input_hashes = (
                bundle.projection_set.projection_set_sha256,
                *((command_lifecycle_projection_sha256,) if command_lifecycle_projection_sha256 is not None else ()),
                bundle.after_state.state_sha256,
                *(item.payload_sha256 for item in bundle.new_child_mappings),
                *(item.payload_sha256 for item in bundle.command_outboxes),
                *(item.payload_sha256 for item in bundle.updated_child_mappings),
                *(item.payload_sha256 for item in bundle.updated_command_outboxes),
                *(item.schedule_receipt_sha256 for item in bundle.timer_schedules),
                *(item.context_sha256 for item in bundle.diagnostic_observations),
            )
        elif kind == "FAILED_TERMINAL":
            input_hashes = (
                receipt.plugin_manifest_sha256,
                receipt.context_sha256,
                *((bundle.projection_set.projection_set_sha256,) if bundle.projection_set is not None else ()),
                *(item.payload_sha256 for item in bundle.command_outboxes),
                *(item.payload_sha256 for item in bundle.updated_child_mappings),
                *(item.payload_sha256 for item in bundle.updated_command_outboxes),
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
                *(item.mapping_id for item in bundle.updated_child_mappings),
                *(item.command_id for item in bundle.updated_command_outboxes),
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
        self._write_failure_terminal_updates_with_cursor(
            cur,
            mappings=bundle.updated_child_mappings,
            outboxes=bundle.updated_command_outboxes,
        )
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

    def _write_failure_terminal_updates_with_cursor(
        self,
        cur: Any,
        *,
        mappings: tuple[ExecutionCommandChildMappingV1, ...],
        outboxes: tuple[BrokerCommandOutboxV1, ...],
    ) -> None:
        for mapping, outbox in zip(mappings, outboxes, strict=True):
            cur.execute(
                """
                SELECT child.mapping_json,current_outbox.carrier_json AS outbox_json
                FROM qmt_strategy.execution_child_order AS child
                JOIN qmt_strategy.execution_algo_command_outbox AS current_outbox
                  ON current_outbox.mapping_id=child.mapping_id
                WHERE child.mapping_id=%s AND current_outbox.command_id=%s
                FOR UPDATE OF child,current_outbox
                """,
                (mapping.mapping_id, outbox.command_id),
            )
            row = cur.fetchone()
            if row is None:
                raise KernelRepositoryConflict("failure terminalization lost mapping/outbox authority")
            previous_mapping = _model_from_json(
                ExecutionCommandChildMappingV1,
                _row_json(row, "mapping_json"),
            )
            previous_outbox = _model_from_json(BrokerCommandOutboxV1, _row_json(row, "outbox_json"))
            mapping.validate_successor_v1(previous_mapping)
            outbox.validate_successor_v1(previous_outbox)
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
                    mapping.mapping_id,
                    previous_mapping.mapping_version,
                ),
            )
            if cur.rowcount != 1:
                raise KernelRepositoryConflict("failure mapping terminalization CAS failed")
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
                  AND lease_owner IS NOT DISTINCT FROM %s AND lease_epoch=%s
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
                    None,
                    None,
                    None,
                    None,
                    None,
                    _json(outbox_projection["last_error_json"]),
                    outbox_projection["row_version"],
                    outbox_projection["updated_at_utc"],
                    outbox_projection["closed_at_utc"],
                    _json(outbox.model_dump(mode="json")),
                    outbox_projection["outbox_row_sha256"],
                    outbox.command_id,
                    previous_outbox.row_version,
                    previous_outbox.lease_owner,
                    previous_outbox.lease_epoch,
                    previous_outbox.lease_fence_token,
                ),
            )
            if cur.rowcount != 1:
                raise KernelRepositoryConflict("failure outbox terminalization CAS failed")

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
        for mapping, outbox in zip(
            expected.updated_child_mappings,
            expected.updated_command_outboxes,
            strict=True,
        ):
            chain = self.read_command_identity_chain(outbox.command_id)
            if chain["mapping"] != mapping or chain["outbox"] != outbox:
                raise KernelRepositoryConflict("K2-B failure mapping/outbox post-commit readback differs")
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
