"""K6-C1 atomic product transition materialization and independent readback."""

from __future__ import annotations

from typing import Any

import psycopg2.extras

from .kernel_delivery import KernelTransitionWriteBundleV1
from .kernel_product_authority import (
    evaluate_product_command_authority_v3,
    product_transition_commit_identity_from_authority_v3,
)
from .kernel_product_contracts import (
    DependentBuyCoordinationStatusV1,
    DependentBuyCoordinationV2,
    DependentBuySellDependencyV2,
    ProductCommandAuthorityEnvelopeV3,
    ProductCommandAuthorityItemV3,
    ProductCommandAuthoritySetV3,
    ProductCommandChildMappingV1,
    ProductCommandChildMappingStatusV1,
    ProductCommandDispositionV3,
    ProductCommandLifecycleProjectionItemV3,
    ProductCommandLifecycleProjectionV3,
    ProductLifecycleStatusV3,
    ProductMaterializationReceiptV3,
    validate_kernel_product_payload_v1,
)
from .kernel_repository_common import KernelRepositoryConflict, _json, _model_from_json, _row_json
from .kernel_repository_projection import (
    _assert_scalar_columns,
    _delivery_scalar_projection,
    _mapping_scalar_projection,
    _outbox_scalar_projection,
    _transition_scalar_projection,
)
from .plugin_canonical import hash_hex_v1, thaw_json_v1
from .plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoStateSnapshotV2,
    AlgoTransitionReceiptV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerCommandTypeV2,
    CommandChildMappingStatusV1,
    DeliveryStatusV1,
    DiagnosticObservationV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    KernelErrorEvidenceV1,
    TimerMutationV1,
)


_OUTBOX_TO_PRODUCT_LIFECYCLE = {
    BrokerCommandOutboxStatusV1.PENDING: ProductLifecycleStatusV3.PENDING,
    BrokerCommandOutboxStatusV1.CLAIMED: ProductLifecycleStatusV3.CLAIMED,
    BrokerCommandOutboxStatusV1.DISPATCHING: ProductLifecycleStatusV3.DISPATCHING,
    BrokerCommandOutboxStatusV1.ACKED: ProductLifecycleStatusV3.ACKED,
    BrokerCommandOutboxStatusV1.ACKED_REJECTED: ProductLifecycleStatusV3.ACKED_REJECTED,
    BrokerCommandOutboxStatusV1.FAILED_RETRYABLE: ProductLifecycleStatusV3.FAILED_RETRYABLE,
    BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN: ProductLifecycleStatusV3.OUTCOME_UNKNOWN,
    BrokerCommandOutboxStatusV1.RECONCILING: ProductLifecycleStatusV3.RECONCILING,
    BrokerCommandOutboxStatusV1.FAILED_TERMINAL: ProductLifecycleStatusV3.FAILED_TERMINAL,
}


def _authority_projection_v3(value: ProductCommandAuthoritySetV3) -> dict[str, Any]:
    return {
        "authority_set_sha256": value.authority_set_sha256,
        "transition_id": value.transition_id,
        "runtime_id": value.runtime_id,
        "algo_instance_id": value.algo_instance_id,
        "event_id": value.event_id,
        "delivery_id": value.delivery_id,
        "catalog_sha256": value.catalog_sha256,
        "creation_binding_sha256": value.creation_binding_sha256,
        "facade_conformance_set_sha256": value.facade_conformance_set_sha256,
        "execution_projection_set_sha256": value.execution_projection_set_sha256,
        "transition_receipt_sha256": value.transition_receipt_sha256,
        "materialize_count": value.materialize_count,
        "reject_count": value.reject_count,
        "defer_count": value.defer_count,
        "total_count": value.total_count,
        "aggregate_disposition": value.aggregate_disposition.value,
    }


def _authority_item_projection_v3(value: ProductCommandAuthorityItemV3) -> dict[str, Any]:
    return {
        "transition_id": value.transition_id,
        "effect_ordinal": value.effect_ordinal,
        "command_id": value.command_id,
        "disposition": value.disposition.value,
        "mapping_id": value.mapping_id,
        "outbox_id": value.outbox_id,
        "child_order_id": value.child_order_id,
        "reject_reason_code": value.reject_reason_code,
        "reject_context_sha256": value.reject_context_sha256,
        "item_sha256": value.item_sha256,
        "command_json": value.command_json.model_dump(mode="json"),
        "evaluation_evidence_json": value.evaluation_evidence.model_dump(mode="json"),
        "evaluation_evidence_sha256": value.evaluation_evidence.evidence_sha256,
        "coordination_id": value.coordination_id,
    }


def _product_mapping_projection_v3(
    value: ExecutionCommandChildMappingV1 | ProductCommandChildMappingV1,
    *,
    authority_item: ProductCommandAuthorityItemV3,
) -> dict[str, Any]:
    projection = _mapping_scalar_projection(value)
    projection["status"] = (
        "REJECTED"
        if authority_item.disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS
        and authority_item.command_json.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT
        else "SUBMITTING"
    )
    return projection


def _coordination_projection_v2(value: DependentBuyCoordinationV2) -> dict[str, Any]:
    projection = value.model_dump(
        mode="python",
        exclude={"schema_version", "ordered_sell_dependencies"},
    )
    projection["status"] = value.status.value
    return projection


def _dependency_projection_v2(value: DependentBuySellDependencyV2) -> dict[str, Any]:
    return {
        "runtime_id": value.runtime_id,
        "strategy_id": value.strategy_id,
        "sell_parent_intent_id": value.sell_parent_intent_id,
        "sell_algo_instance_id": value.sell_algo_instance_id,
        "latest_order_fact_ref": value.latest_order_fact_sha256,
        "settled_trade_fact_refs": [item.qmt_trade_fact_sha256 for item in value.ordered_settled_proceeds_refs],
        "settled_cash_ledger_refs": [item.cash_ledger_fact_sha256 for item in value.ordered_settled_proceeds_refs],
        "dependency_status": value.dependency_status.value,
        "dependency_sha256": value.dependency_sha256,
        "latest_order_fact_id": value.latest_order_fact_id,
        "latest_order_fact_sha256": value.latest_order_fact_sha256,
        "ordered_settled_proceeds_refs": [item.model_dump(mode="json") for item in value.ordered_settled_proceeds_refs],
    }


def _assert_coordination_authority_v2(
    current: DependentBuyCoordinationV2,
    initial: DependentBuyCoordinationV2,
) -> None:
    immutable_fields = (
        "coordination_id",
        "runtime_id",
        "binding_id",
        "trade_date",
        "strategy_id",
        "buy_algo_instance_id",
        "buy_parent_intent_id",
        "required_cash",
        "virtual_account_id",
        "session_authority_sha256",
        "release_command_id",
        "release_transition_id",
        "release_command_authority_item_sha256",
        "release_command_payload_sha256",
        "created_at_utc",
    )
    if any(getattr(current, field) != getattr(initial, field) for field in immutable_fields):
        raise KernelRepositoryConflict("deferred product coordination changes immutable authority")
    if current.row_version < initial.row_version or current.decision_sequence < initial.decision_sequence:
        raise KernelRepositoryConflict("deferred product coordination regresses durable version authority")
    current_dependencies = {item.sell_parent_intent_id: item for item in current.ordered_sell_dependencies}
    initial_dependencies = {item.sell_parent_intent_id: item for item in initial.ordered_sell_dependencies}
    if set(current_dependencies) != set(initial_dependencies):
        raise KernelRepositoryConflict("deferred product coordination changes frozen SELL dependency set")
    try:
        for parent_intent_id, initial_dependency in initial_dependencies.items():
            current_dependencies[parent_intent_id].validate_successor_v2(initial_dependency)
    except (TypeError, ValueError) as exc:
        raise KernelRepositoryConflict("deferred product coordination dependency evidence is not monotonic") from exc


def _strict_roundtrip_v1(model_type: type[Any], value: Any, *, field_name: str) -> Any:
    """Rebuild an in-memory carrier so ``model_copy`` cannot bypass validation."""

    if not isinstance(value, model_type):
        raise TypeError(f"{field_name} must be {model_type.__name__}")
    try:
        return model_type.model_validate_json(value.model_dump_json(), strict=True)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} fails strict durable carrier validation") from exc


def _strict_transition_bundle_v1(value: KernelTransitionWriteBundleV1) -> KernelTransitionWriteBundleV1:
    if not isinstance(value, KernelTransitionWriteBundleV1):
        raise TypeError("transition_bundle must be KernelTransitionWriteBundleV1")
    if not isinstance(value.receipt, AlgoTransitionReceiptV1):
        raise ValueError("K6 product materialization requires one APPLIED transition receipt")
    return KernelTransitionWriteBundleV1.create(
        algo_instance=_strict_roundtrip_v1(
            ExecutionAlgoInstancePersistenceV2,
            value.algo_instance,
            field_name="transition_bundle.algo_instance",
        ),
        delivery=_strict_roundtrip_v1(
            AlgoDeliveryPersistenceV1,
            value.delivery,
            field_name="transition_bundle.delivery",
        ),
        receipt=_strict_roundtrip_v1(
            AlgoTransitionReceiptV1,
            value.receipt,
            field_name="transition_bundle.receipt",
        ),
        projection_set=(
            None
            if value.projection_set is None
            else _strict_roundtrip_v1(
                ExecutionProjectionSetV1,
                value.projection_set,
                field_name="transition_bundle.projection_set",
            )
        ),
        after_state=(
            None
            if value.after_state is None
            else _strict_roundtrip_v1(
                AlgoStateSnapshotV2,
                value.after_state,
                field_name="transition_bundle.after_state",
            )
        ),
        new_child_mappings=tuple(
            _strict_roundtrip_v1(
                ExecutionCommandChildMappingV1,
                item,
                field_name=f"transition_bundle.new_child_mappings[{ordinal}]",
            )
            for ordinal, item in enumerate(value.new_child_mappings)
        ),
        command_outboxes=tuple(
            _strict_roundtrip_v1(
                BrokerCommandOutboxV1,
                item,
                field_name=f"transition_bundle.command_outboxes[{ordinal}]",
            )
            for ordinal, item in enumerate(value.command_outboxes)
        ),
        updated_child_mappings=tuple(
            _strict_roundtrip_v1(
                ExecutionCommandChildMappingV1,
                item,
                field_name=f"transition_bundle.updated_child_mappings[{ordinal}]",
            )
            for ordinal, item in enumerate(value.updated_child_mappings)
        ),
        updated_command_outboxes=tuple(
            _strict_roundtrip_v1(
                BrokerCommandOutboxV1,
                item,
                field_name=f"transition_bundle.updated_command_outboxes[{ordinal}]",
            )
            for ordinal, item in enumerate(value.updated_command_outboxes)
        ),
        timer_mutations=tuple(
            _strict_roundtrip_v1(
                TimerMutationV1,
                item,
                field_name=f"transition_bundle.timer_mutations[{ordinal}]",
            )
            for ordinal, item in enumerate(value.timer_mutations)
        ),
        timer_schedules=tuple(
            _strict_roundtrip_v1(
                ExecutionAlgoTimerScheduleV1,
                item,
                field_name=f"transition_bundle.timer_schedules[{ordinal}]",
            )
            for ordinal, item in enumerate(value.timer_schedules)
        ),
        diagnostic_observations=tuple(
            _strict_roundtrip_v1(
                DiagnosticObservationV1,
                item,
                field_name=f"transition_bundle.diagnostic_observations[{ordinal}]",
            )
            for ordinal, item in enumerate(value.diagnostic_observations)
        ),
    )


def _immutable_materialization_lineage_sha256_v3(
    authority: ProductCommandAuthoritySetV3,
    lifecycle: ProductCommandLifecycleProjectionV3,
) -> str:
    """Hash only the durable commit lineage, excluding later worker lifecycle state."""

    lifecycle.validate_against_authority_v3(authority)
    return hash_hex_v1(
        "miniqmt_product_materialization_immutable_lineage_v3",
        [
            {
                "authority_item_sha256": item.item_sha256,
                "effect_ordinal": item.effect_ordinal,
                "command_id": item.command_id,
                "disposition": item.disposition.value,
                "mapping_id": item.mapping_id,
                "outbox_id": item.outbox_id,
                "child_order_id": item.child_order_id,
            }
            for item in authority.ordered_items
        ],
    )


def _coordination_v2(item: ProductCommandAuthorityItemV3, *, created_at_utc: Any) -> DependentBuyCoordinationV2:
    candidate = item.evaluation_evidence.dependent_buy_candidate
    if candidate is None:
        raise ValueError("deferred authority item lacks its strict dependent-BUY candidate")
    coordination = DependentBuyCoordinationV2.create(
        runtime_id=candidate.runtime_id,
        binding_id=candidate.binding_id,
        trade_date=candidate.trade_date,
        strategy_id=candidate.strategy_id,
        buy_algo_instance_id=candidate.buy_algo_instance_id,
        buy_parent_intent_id=candidate.buy_parent_intent_id,
        required_cash=candidate.required_cash,
        virtual_account_id=candidate.virtual_account_id,
        session_authority_sha256=candidate.session_authority_sha256,
        release_command_id=item.command_id,
        release_transition_id=item.transition_id,
        release_command_authority_item_sha256=item.item_sha256,
        release_command_payload_sha256=item.command_payload_sha256,
        ordered_sell_dependencies=candidate.ordered_sell_dependencies,
        status=DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS,
        decision_sequence=0,
        last_decision_sha256=None,
        released_command_id=None,
        released_outbox_id=None,
        row_version=1,
        lease_worker_id=None,
        lease_process_incarnation_id=None,
        lease_epoch=0,
        lease_expires_at_utc=None,
        created_at_utc=created_at_utc,
        updated_at_utc=created_at_utc,
    )
    if coordination.coordination_id != item.coordination_id:
        raise ValueError("deferred coordination identity differs from authority item")
    coordination.validate_initial_v2()
    return coordination


def _terminal_reject_mapping(
    *,
    item: ProductCommandAuthorityItemV3,
    strategy_slot_id: str,
    created_at_utc: Any,
) -> ExecutionCommandChildMappingV1:
    return ExecutionCommandChildMappingV1.create(
        command=item.command_json,
        strategy_slot_id=strategy_slot_id,
        mapping_status=CommandChildMappingStatusV1.TERMINAL,
        mapping_version=1,
        broker_order_id=None,
        broker_identity_source_event_id=None,
        last_order_event_id=None,
        last_trade_event_id=None,
        updated_by_event_id=item.event_id,
        created_at_utc=created_at_utc,
        updated_at_utc=created_at_utc,
    )


def _terminal_reject_outbox(
    *,
    item: ProductCommandAuthorityItemV3,
    mapping_id: str,
    created_at_utc: Any,
) -> BrokerCommandOutboxV1:
    error = KernelErrorEvidenceV1.create(
        stage="K6_PRODUCT_COMMAND_AUTHORITY",
        stable_reason_code=item.reject_reason_code,
        exception=ValueError("product command rejected before broker call"),
        message="product command rejected before broker call",
        retryable=False,
        terminal=True,
        broker_called=False,
        primary_context={
            "runtime_id": item.runtime_id,
            "algo_instance_id": item.algo_instance_id,
            "event_id": item.event_id,
            "delivery_id": item.delivery_id,
            "transition_id": item.transition_id,
            "command_id": item.command_id,
            "authority_item_sha256": item.item_sha256,
            "reject_context_sha256": item.reject_context_sha256,
        },
        secondary_errors=[],
    )
    return BrokerCommandOutboxV1.create(
        command=item.command_json,
        mapping_id=mapping_id,
        status=BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
        attempt_count=0,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at=None,
        dispatch_attempt_id=None,
        callback_watermark_before_call=None,
        next_attempt_at_utc=None,
        broker_called=False,
        broker_order_id=None,
        ack_receipt_json=None,
        ack_receipt_sha256=None,
        non_acceptance_receipt=None,
        unknown_outcome_receipt=None,
        reconcile_receipt=None,
        last_error_json=error.model_dump(mode="json"),
        row_version=1,
        created_at_utc=created_at_utc,
        updated_at_utc=created_at_utc,
        closed_at_utc=created_at_utc,
    )


class KernelProductMaterializationRepositoryMixin:
    """Own the K6-C1 transaction and its independent fresh-process readback."""

    def materialize_product_transition_atomic_v3(
        self,
        *,
        authority_envelope: ProductCommandAuthorityEnvelopeV3,
        transition_bundle: KernelTransitionWriteBundleV1,
        previous_delivery: AlgoDeliveryPersistenceV1,
        expected_delivery_row_version: int,
        expected_algo_row_version: int,
        strategy_slot_id: str,
    ) -> ProductMaterializationReceiptV3:
        if not isinstance(authority_envelope, ProductCommandAuthorityEnvelopeV3):
            raise TypeError("authority_envelope must be ProductCommandAuthorityEnvelopeV3")
        envelope = ProductCommandAuthorityEnvelopeV3.model_validate_json(
            authority_envelope.model_dump_json(), strict=True
        )
        transition_bundle = _strict_transition_bundle_v1(transition_bundle)
        previous_delivery = _strict_roundtrip_v1(
            AlgoDeliveryPersistenceV1,
            previous_delivery,
            field_name="previous_delivery",
        )
        if type(expected_delivery_row_version) is not int or expected_delivery_row_version <= 0:
            raise ValueError("expected_delivery_row_version must be one strict positive integer")
        if type(expected_algo_row_version) is not int or expected_algo_row_version <= 0:
            raise ValueError("expected_algo_row_version must be one strict positive integer")
        if type(strategy_slot_id) is not str or not strategy_slot_id or strategy_slot_id != strategy_slot_id.strip():
            raise ValueError("strategy_slot_id must be a canonical strict identity")
        authority = envelope.authority_set
        receipt = transition_bundle.receipt
        if not isinstance(receipt, AlgoTransitionReceiptV1):
            raise ValueError("K6 product materialization requires one APPLIED transition receipt")
        if transition_bundle.projection_set is None or transition_bundle.after_state is None:
            raise ValueError("K6 product materialization requires projection set and after state")
        if transition_bundle.updated_child_mappings or transition_bundle.updated_command_outboxes:
            raise ValueError("applied K6 product transition cannot silently ignore prior terminal updates")
        if len(transition_bundle.timer_mutations) != len(transition_bundle.timer_schedules):
            raise ValueError("each product timer mutation requires one exact durable schedule")
        if envelope.ordered_timer_schedules != transition_bundle.timer_schedules:
            raise ValueError("product authority envelope timer schedules differ from transition bundle")
        if (
            tuple(item.mutation_identity_v1() for item in transition_bundle.timer_mutations)
            != receipt.ordered_timer_mutation_ids
        ):
            raise ValueError("product timer mutation set differs from transition receipt")
        if (
            tuple(item.observation_id for item in transition_bundle.diagnostic_observations)
            != receipt.ordered_diagnostic_observation_ids
        ):
            raise ValueError("product diagnostic set differs from transition receipt")
        if (
            receipt.receipt_sha256 != authority.transition_receipt_sha256
            or receipt.transition_id != authority.transition_id
            or transition_bundle.projection_set.projection_set_sha256 != authority.execution_projection_set_sha256
            or tuple(item.command_id for item in authority.ordered_items) != receipt.ordered_command_ids
        ):
            raise KernelRepositoryConflict("product authority differs from the K2 transition bundle")
        expected_transaction_identity = product_transition_commit_identity_from_authority_v3(
            authority=authority,
            transition_receipt=receipt,
            timer_schedules=envelope.ordered_timer_schedules,
            diagnostic_observations=transition_bundle.diagnostic_observations,
        )
        if receipt.transaction_commit_identity != expected_transaction_identity:
            raise KernelRepositoryConflict("product transition transaction identity differs from durable authority")

        base_mapping_by_command = {item.command_id: item for item in transition_bundle.new_child_mappings}
        base_outbox_by_command = {item.command_id: item for item in transition_bundle.command_outboxes}
        if len(base_mapping_by_command) != len(transition_bundle.new_child_mappings) or len(
            base_outbox_by_command
        ) != len(transition_bundle.command_outboxes):
            raise ValueError("K2 transition bundle contains duplicate command lineage")
        if set(base_outbox_by_command) != {item.command_id for item in authority.ordered_items}:
            raise ValueError("K2 transition bundle must carry the exact pre-product command set")

        regular_mappings: list[tuple[ExecutionCommandChildMappingV1, ProductCommandAuthorityItemV3]] = []
        product_mappings: list[tuple[ProductCommandChildMappingV1, ProductCommandAuthorityItemV3]] = []
        outboxes: list[BrokerCommandOutboxV1] = []
        coordinations: list[DependentBuyCoordinationV2] = []
        created_at_utc = transition_bundle.after_state.updated_at_utc
        active_new_count = 0
        for item in authority.ordered_items:
            command = item.command_json
            base_outbox = base_outbox_by_command[item.command_id]
            try:
                base_outbox.validate_initial_v1()
            except ValueError as exc:
                raise ValueError("K2 pre-product outbox must be exact initial PENDING authority") from exc
            if thaw_json_v1(base_outbox.payload_json) != command.model_dump(mode="json"):
                raise ValueError("K2 transition outbox payload differs from product command authority")
            if base_outbox.mapping_id != item.mapping_id:
                raise ValueError("K2 transition outbox mapping differs from product command authority")
            base_mapping = base_mapping_by_command.get(item.command_id)
            if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
                if base_mapping is None or (base_mapping.mapping_id, base_mapping.child_order_id) != (
                    item.mapping_id,
                    item.child_order_id,
                ):
                    raise ValueError("SUBMIT base mapping differs from product authority")
                try:
                    base_mapping.validate_initial_v1()
                except ValueError as exc:
                    raise ValueError("K2 pre-product SUBMIT mapping must be exact initial RESERVED authority") from exc
            elif base_mapping is not None:
                raise ValueError("CANCEL command cannot create a new child mapping")
            if item.disposition is ProductCommandDispositionV3.MATERIALIZE:
                if base_mapping is not None:
                    regular_mappings.append((base_mapping, item))
                    active_new_count += 1
                outboxes.append(base_outbox)
            elif item.disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS:
                if base_mapping is not None:
                    regular_mappings.append(
                        (
                            _terminal_reject_mapping(
                                item=item,
                                strategy_slot_id=strategy_slot_id,
                                created_at_utc=created_at_utc,
                            ),
                            item,
                        )
                    )
                outboxes.append(
                    _terminal_reject_outbox(
                        item=item,
                        mapping_id=item.mapping_id,
                        created_at_utc=created_at_utc,
                    )
                )
            else:
                product_mappings.append(
                    (
                        ProductCommandChildMappingV1.create_deferred(
                            authority_item=item,
                            strategy_slot_id=strategy_slot_id,
                            created_at_utc=created_at_utc,
                        ),
                        item,
                    )
                )
                coordinations.append(_coordination_v2(item, created_at_utc=created_at_utc))
                active_new_count += 1

        algo_payload = transition_bundle.algo_instance.model_dump(mode="python")
        durable_previous_active_count = transition_bundle.algo_instance.active_child_count - len(
            transition_bundle.new_child_mappings
        )
        if durable_previous_active_count < 0:
            raise ValueError("K2 transition bundle active-child count is internally inconsistent")
        algo_payload["active_child_count"] = durable_previous_active_count + active_new_count
        algo_instance = ExecutionAlgoInstancePersistenceV2.model_validate(algo_payload)

        try:
            existing_envelope, _, existing_receipt = self._read_product_materialization_envelope_v3(
                authority.authority_set_sha256
            )
        except KeyError:
            existing_envelope = existing_receipt = None
        if existing_envelope is not None:
            if existing_envelope != envelope:
                raise KernelRepositoryConflict("product authority identity exists with different durable closure")
            assert existing_receipt is not None
            return existing_receipt

        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (authority.authority_set_sha256,),
                )
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_product_command_authority "
                    "WHERE authority_set_sha256=%s",
                    (authority.authority_set_sha256,),
                )
                concurrent_row = cur.fetchone()
                if concurrent_row is not None:
                    concurrent_envelope = validate_kernel_product_payload_v1(
                        ProductCommandAuthorityEnvelopeV3,
                        _row_json(concurrent_row, "carrier_json"),
                        stage="CONCURRENT_PRODUCT_AUTHORITY_READBACK",
                    )
                    if concurrent_envelope != envelope:
                        raise KernelRepositoryConflict(
                            "concurrent product authority identity has different durable closure"
                        )
                    return self._read_product_materialization_envelope_v3(authority.authority_set_sha256)[2]
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_algo_event_delivery WHERE delivery_id=%s FOR UPDATE",
                    (previous_delivery.delivery_id,),
                )
                delivery_row = cur.fetchone()
                if delivery_row is None:
                    raise KeyError(previous_delivery.delivery_id)
                durable_delivery = _model_from_json(AlgoDeliveryPersistenceV1, _row_json(delivery_row, "carrier_json"))
                if (
                    durable_delivery != previous_delivery
                    or durable_delivery.row_version != expected_delivery_row_version
                ):
                    raise KernelRepositoryConflict("product materializer delivery predecessor/CAS differs")
                cur.execute(
                    "SELECT kernel_carrier_json FROM qmt_strategy.execution_algo_instance "
                    "WHERE runtime_id=%s AND algo_instance_id=%s FOR UPDATE",
                    (authority.runtime_id, authority.algo_instance_id),
                )
                algo_row = cur.fetchone()
                if algo_row is None:
                    raise KeyError(authority.algo_instance_id)
                durable_algo = _model_from_json(
                    ExecutionAlgoInstancePersistenceV2, _row_json(algo_row, "kernel_carrier_json")
                )
                if durable_algo.row_version != expected_algo_row_version:
                    raise KernelRepositoryConflict("product materializer algo CAS differs")
                algo_instance.validate_successor_v1(durable_algo)
                transition_bundle.delivery.validate_successor_v1(durable_delivery)
                self._insert_product_transition_header_with_cursor(cur, transition_bundle)
                for mapping, item in regular_mappings:
                    self._insert_product_child_with_cursor(cur, mapping, authority_item=item)
                for mapping, item in product_mappings:
                    self._insert_product_child_with_cursor(cur, mapping, authority_item=item)
                self._write_transition_commands_with_cursor(
                    cur,
                    transition_id=authority.transition_id,
                    mappings=(),
                    outboxes=tuple(outboxes),
                    child_price_type=2,
                )
                for coordination in coordinations:
                    self._insert_dependent_buy_coordination_v2_with_cursor(cur, coordination)
                self._insert_product_authority_v3_with_cursor(cur, envelope)
                for schedule in transition_bundle.timer_schedules:
                    self._write_timer_schedule_with_cursor(cur, schedule)
                for observation in transition_bundle.diagnostic_observations:
                    cur.execute(
                        "INSERT INTO qmt_strategy.execution_algo_diagnostic_observation("
                        "observation_id,runtime_id,algo_instance_id,event_id,transition_id,observation_json,"
                        "context_sha256,observed_at_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
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
                self._cas_algo_with_cursor(
                    cur,
                    algo_instance=algo_instance,
                    expected_row_version=expected_algo_row_version,
                )
                self._cas_product_delivery_with_cursor(
                    cur,
                    delivery=transition_bundle.delivery,
                    previous=durable_delivery,
                    expected_row_version=expected_delivery_row_version,
                )
        durable_envelope, _, materialization_receipt = self._read_product_materialization_envelope_v3(
            authority.authority_set_sha256
        )
        if durable_envelope != envelope:
            raise KernelRepositoryConflict("product materialization post-commit authority readback differs")
        return materialization_receipt

    def read_product_materialization_v3(
        self, authority_set_sha256: str
    ) -> tuple[ProductCommandAuthoritySetV3, ProductCommandLifecycleProjectionV3, ProductMaterializationReceiptV3]:
        envelope, lifecycle, receipt = self._read_product_materialization_envelope_v3(authority_set_sha256)
        return envelope.authority_set, lifecycle, receipt

    def _read_product_materialization_envelope_v3(
        self, authority_set_sha256: str
    ) -> tuple[ProductCommandAuthorityEnvelopeV3, ProductCommandLifecycleProjectionV3, ProductMaterializationReceiptV3]:
        if (
            type(authority_set_sha256) is not str
            or len(authority_set_sha256) != 64
            or any(character not in "0123456789abcdef" for character in authority_set_sha256)
        ):
            raise ValueError("authority_set_sha256 must be one lowercase SHA-256")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_product_command_authority WHERE authority_set_sha256=%s",
                    (authority_set_sha256,),
                )
                aggregate_row = cur.fetchone()
                if aggregate_row is None:
                    raise KeyError(authority_set_sha256)
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_product_command_authority_item "
                    "WHERE authority_set_sha256=%s ORDER BY effect_ordinal,command_id",
                    (authority_set_sha256,),
                )
                item_rows = cur.fetchall()
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_algo_transition WHERE transition_id=%s",
                    (aggregate_row["transition_id"],),
                )
                transition_row = cur.fetchone()
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_algo_diagnostic_observation "
                    "WHERE transition_id=%s ORDER BY observation_id",
                    (aggregate_row["transition_id"],),
                )
                diagnostic_rows = cur.fetchall()
                lineage_rows: list[dict[str, Any]] = []
                for item_row in item_rows:
                    cur.execute(
                        "SELECT * FROM qmt_strategy.execution_child_order WHERE mapping_id=%s",
                        (item_row["mapping_id"],),
                    )
                    mapping_row = cur.fetchone()
                    cur.execute(
                        "SELECT * FROM qmt_strategy.execution_algo_command_outbox WHERE command_id=%s",
                        (item_row["command_id"],),
                    )
                    outbox_row = cur.fetchone()
                    coordination_row = None
                    dependency_rows: list[dict[str, Any]] = []
                    if item_row["coordination_id"] is not None:
                        cur.execute(
                            "SELECT * FROM qmt_strategy.execution_dependent_buy_coordination WHERE coordination_id=%s",
                            (item_row["coordination_id"],),
                        )
                        coordination_row = cur.fetchone()
                        cur.execute(
                            "SELECT * FROM qmt_strategy.execution_dependent_buy_dependency "
                            "WHERE coordination_id=%s ORDER BY sell_parent_intent_id",
                            (item_row["coordination_id"],),
                        )
                        dependency_rows = cur.fetchall()
                    lineage_rows.append(
                        {
                            "mapping": mapping_row,
                            "outbox": outbox_row,
                            "coordination": coordination_row,
                            "dependencies": dependency_rows,
                        }
                    )
        envelope = validate_kernel_product_payload_v1(
            ProductCommandAuthorityEnvelopeV3,
            _row_json(aggregate_row, "carrier_json"),
            stage="PRODUCT_COMMAND_AUTHORITY_ENVELOPE_READBACK",
        )
        authority = envelope.authority_set
        _assert_scalar_columns(aggregate_row, _authority_projection_v3(authority), carrier_name="product authority V3")
        items = tuple(
            validate_kernel_product_payload_v1(
                ProductCommandAuthorityItemV3,
                _row_json(row, "carrier_json"),
                stage="PRODUCT_COMMAND_AUTHORITY_ITEM_V3_READBACK",
            )
            for row in item_rows
        )
        if items != authority.ordered_items:
            raise KernelRepositoryConflict("product authority item readback differs from aggregate envelope")
        for row, item in zip(item_rows, items, strict=True):
            _assert_scalar_columns(row, _authority_item_projection_v3(item), carrier_name="product authority item V3")
            rebuilt = evaluate_product_command_authority_v3(
                command=item.command_json,
                evidence=item.evaluation_evidence,
                catalog=envelope.creation_authority.plugin_catalog_snapshot,
                creation_binding=envelope.creation_authority,
            )
            if rebuilt != item:
                raise KernelRepositoryConflict("fresh-process product evaluator differs from durable authority item")
        if transition_row is None:
            raise KernelRepositoryConflict("product authority transition receipt is missing")
        transition_receipt = _model_from_json(
            AlgoTransitionReceiptV1,
            _row_json(transition_row, "transition_receipt_json"),
        )
        if (
            transition_receipt.receipt_sha256 != authority.transition_receipt_sha256
            or transition_receipt.transaction_commit_identity != transition_row["transaction_commit_identity"]
        ):
            raise KernelRepositoryConflict("product authority transition receipt readback differs")
        projection_set = _model_from_json(
            ExecutionProjectionSetV1,
            _row_json(transition_row, "execution_projection_set_json"),
        )
        after_state = _model_from_json(
            AlgoStateSnapshotV2,
            _row_json(transition_row, "after_state_json"),
        )
        _assert_scalar_columns(
            transition_row,
            _transition_scalar_projection(
                receipt=transition_receipt,
                kind="APPLIED",
                transition_sequence=transition_receipt.transition_sequence,
                projection_set=projection_set,
                after_state=after_state,
            ),
            carrier_name="product transition",
        )
        if projection_set.projection_set_sha256 != authority.execution_projection_set_sha256:
            raise KernelRepositoryConflict("product transition projection set differs from authority")
        diagnostics_by_id: dict[str, DiagnosticObservationV1] = {}
        for row in diagnostic_rows:
            observation = _model_from_json(
                DiagnosticObservationV1,
                _row_json(row, "observation_json"),
            )
            if observation.observation_id in diagnostics_by_id:
                raise KernelRepositoryConflict("product transition diagnostic identity is duplicated")
            diagnostics_by_id[observation.observation_id] = observation
            _assert_scalar_columns(
                row,
                {
                    "observation_id": observation.observation_id,
                    "runtime_id": observation.runtime_id,
                    "algo_instance_id": observation.algo_instance_id,
                    "event_id": observation.event_id,
                    "transition_id": observation.transition_id,
                    "context_sha256": observation.context_sha256,
                    "observed_at_utc": observation.observed_at_logical_utc,
                },
                carrier_name="product diagnostic observation",
            )
        try:
            diagnostic_observations = tuple(
                diagnostics_by_id[identity] for identity in transition_receipt.ordered_diagnostic_observation_ids
            )
        except KeyError as exc:
            raise KernelRepositoryConflict("product transition diagnostic observation is missing") from exc
        if len(diagnostics_by_id) != len(diagnostic_observations):
            raise KernelRepositoryConflict("product transition has extra diagnostic observations")
        for expected_schedule in envelope.ordered_timer_schedules:
            current_schedule = self.read_timer_schedule(expected_schedule.schedule_id)
            if current_schedule.immutable_schedule_payload_v1() != expected_schedule.immutable_schedule_payload_v1():
                raise KernelRepositoryConflict("product timer schedule immutable payload differs from authority")
        if transition_receipt.transaction_commit_identity != product_transition_commit_identity_from_authority_v3(
            authority=authority,
            transition_receipt=transition_receipt,
            timer_schedules=envelope.ordered_timer_schedules,
            diagnostic_observations=diagnostic_observations,
        ):
            raise KernelRepositoryConflict("product transition commit identity fails independent recomputation")
        durable_delivery = self.read_delivery(authority.delivery_id)
        durable_algo = self.read_algo_instance(authority.algo_instance_id)
        if (
            durable_delivery.status is not DeliveryStatusV1.APPLIED
            or durable_delivery.transition_id != authority.transition_id
            or durable_delivery.event_id != authority.event_id
            or durable_algo.runtime_id != authority.runtime_id
            or durable_algo.last_applied_delivery_id != authority.delivery_id
            or durable_algo.last_applied_delivery_sequence != transition_receipt.transition_sequence
            or durable_algo.state_sha256 != transition_receipt.after_state_sha256
        ):
            raise KernelRepositoryConflict("product delivery/algo readback differs from committed transition")
        lifecycle_items: list[ProductCommandLifecycleProjectionItemV3] = []
        for item, lineage in zip(items, lineage_rows, strict=True):
            if lineage["mapping"] is None:
                raise KernelRepositoryConflict("product authority mapping is missing")
            outbox = (
                None
                if lineage["outbox"] is None
                else _model_from_json(BrokerCommandOutboxV1, _row_json(lineage["outbox"], "carrier_json"))
            )
            if item.disposition is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY:
                mapping = _model_from_json(
                    ProductCommandChildMappingV1,
                    _row_json(lineage["mapping"], "mapping_json"),
                )
                if lineage["coordination"] is None:
                    raise KernelRepositoryConflict("deferred product authority coordination is missing")
                coordination = validate_kernel_product_payload_v1(
                    DependentBuyCoordinationV2,
                    _row_json(lineage["coordination"], "carrier_json"),
                    stage="DEPENDENT_BUY_COORDINATION_V2_READBACK",
                )
                dependencies = tuple(
                    validate_kernel_product_payload_v1(
                        DependentBuySellDependencyV2,
                        _row_json(row, "carrier_json"),
                        stage="DEPENDENT_BUY_DEPENDENCY_V2_READBACK",
                    )
                    for row in lineage["dependencies"]
                )
                expected_coordination = _coordination_v2(item, created_at_utc=mapping.created_at_utc)
                _assert_coordination_authority_v2(coordination, expected_coordination)
                if dependencies != coordination.ordered_sell_dependencies:
                    raise KernelRepositoryConflict("deferred product authority coordination/dependencies differ")
                _assert_scalar_columns(
                    lineage["coordination"],
                    _coordination_projection_v2(coordination),
                    carrier_name="dependent-BUY coordination V2",
                )
                for dependency_row, dependency in zip(lineage["dependencies"], dependencies, strict=True):
                    _assert_scalar_columns(
                        dependency_row,
                        _dependency_projection_v2(dependency),
                        carrier_name="dependent-BUY dependency V2",
                    )
                if outbox is None:
                    if (
                        coordination.status is DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS
                        and mapping.mapping_status is ProductCommandChildMappingStatusV1.DEFERRED_DEPENDENT_BUY
                    ):
                        lifecycle_status = ProductLifecycleStatusV3.DEFERRED_DEPENDENT_BUY
                        broker_called = None
                    elif (
                        coordination.status
                        in {
                            DependentBuyCoordinationStatusV1.BLOCKED_SELL_PROCEEDS_UNAVAILABLE,
                            DependentBuyCoordinationStatusV1.EOD_RESIDUAL,
                        }
                        and mapping.mapping_status is ProductCommandChildMappingStatusV1.TERMINAL
                    ):
                        lifecycle_status = ProductLifecycleStatusV3.FAILED_TERMINAL
                        broker_called = False
                    else:
                        raise KernelRepositoryConflict(
                            "deferred product coordination/mapping state lacks exact no-outbox closure"
                        )
                else:
                    if (
                        coordination.status is not DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX
                        or mapping.mapping_status is not ProductCommandChildMappingStatusV1.RESERVED
                        or coordination.released_command_id != item.command_id
                        or coordination.released_outbox_id != item.command_id
                        or outbox.command_id != item.command_id
                    ):
                        raise KernelRepositoryConflict(
                            "released dependent-BUY mapping/outbox/coordination identity does not close"
                        )
                    _assert_scalar_columns(
                        lineage["outbox"],
                        _outbox_scalar_projection(outbox),
                        carrier_name="released dependent-BUY command outbox",
                    )
                    lifecycle_status = _OUTBOX_TO_PRODUCT_LIFECYCLE[outbox.status]
                    broker_called = outbox.broker_called
                last_stage = coordination.status.value if outbox is None else outbox.status.value
            else:
                mapping = _model_from_json(
                    ExecutionCommandChildMappingV1,
                    _row_json(lineage["mapping"], "mapping_json"),
                )
                if outbox is None:
                    raise KernelRepositoryConflict("materialized/rejected product authority lacks an outbox")
                _assert_scalar_columns(
                    lineage["outbox"],
                    _outbox_scalar_projection(outbox),
                    carrier_name="product command outbox",
                )
                lifecycle_status = (
                    ProductLifecycleStatusV3.SYNCHRONOUS_REJECTED
                    if item.disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS
                    else _OUTBOX_TO_PRODUCT_LIFECYCLE[outbox.status]
                )
                last_stage = outbox.status.value
                broker_called = outbox.broker_called
            _assert_scalar_columns(
                lineage["mapping"],
                _product_mapping_projection_v3(mapping, authority_item=item),
                carrier_name="product command-child mapping",
            )
            if (mapping.mapping_id, mapping.child_order_id) != (item.mapping_id, item.child_order_id):
                raise KernelRepositoryConflict("product mapping identity differs from authority item")
            if outbox is not None and outbox.mapping_id != item.mapping_id:
                raise KernelRepositoryConflict("product outbox mapping identity differs from authority item")
            lifecycle_items.append(
                ProductCommandLifecycleProjectionItemV3.create(
                    authority_item_sha256=item.item_sha256,
                    effect_ordinal=item.effect_ordinal,
                    command_id=item.command_id,
                    disposition=item.disposition,
                    mapping_id=mapping.mapping_id,
                    outbox_id=None if outbox is None else outbox.command_id,
                    child_order_id=mapping.child_order_id,
                    lifecycle_status=lifecycle_status,
                    last_committed_stage=last_stage,
                    broker_called=broker_called,
                    qmt_order_id=None if outbox is None else outbox.broker_order_id,
                    callback_watermark=None if outbox is None else outbox.callback_watermark_before_call,
                    reconciliation_receipt_sha256=(
                        None
                        if outbox is None or outbox.reconcile_receipt is None
                        else outbox.reconcile_receipt.receipt_sha256
                    ),
                )
            )
        lifecycle = ProductCommandLifecycleProjectionV3.create(
            runtime_id=authority.runtime_id,
            algo_instance_id=authority.algo_instance_id,
            event_id=authority.event_id,
            delivery_id=authority.delivery_id,
            transition_id=authority.transition_id,
            authority_set_sha256=authority.authority_set_sha256,
            ordered_item_projections=tuple(lifecycle_items),
        )
        lifecycle.validate_against_authority_v3(authority)
        independent_readback_sha256 = hash_hex_v1(
            "miniqmt_product_materialization_independent_readback_v3",
            {
                "authority_envelope_sha256": envelope.envelope_sha256,
                "immutable_lineage_sha256": _immutable_materialization_lineage_sha256_v3(authority, lifecycle),
                "transition_receipt_sha256": transition_receipt.receipt_sha256,
            },
        )
        receipt = ProductMaterializationReceiptV3.create(
            authority=authority,
            repository_transaction_id=transition_receipt.transaction_commit_identity,
            independent_readback_sha256=independent_readback_sha256,
        )
        receipt.validate_against_authority_v3(authority)
        return envelope, lifecycle, receipt

    def _insert_product_transition_header_with_cursor(self, cur: Any, bundle: KernelTransitionWriteBundleV1) -> None:
        receipt = bundle.receipt
        assert isinstance(receipt, AlgoTransitionReceiptV1)
        projection = _transition_scalar_projection(
            receipt=receipt,
            kind="APPLIED",
            transition_sequence=receipt.transition_sequence,
            projection_set=bundle.projection_set,
            after_state=bundle.after_state,
        )
        columns = tuple(projection)
        cur.execute(
            f"INSERT INTO qmt_strategy.execution_algo_transition({','.join(columns)}) "
            f"VALUES ({','.join(['%s'] * len(columns))})",
            tuple(
                _json(value) if key.endswith("_json") and value is not None else value
                for key, value in projection.items()
            ),
        )

    def _insert_product_child_with_cursor(
        self,
        cur: Any,
        mapping: ExecutionCommandChildMappingV1 | ProductCommandChildMappingV1,
        *,
        authority_item: ProductCommandAuthorityItemV3,
    ) -> None:
        projection = _product_mapping_projection_v3(mapping, authority_item=authority_item)
        cur.execute(
            "INSERT INTO qmt_strategy.execution_child_order("
            "child_order_id,runtime_id,algo_instance_id,parent_intent_id,strategy_slot_id,symbol,side,quantity,"
            "price,price_type,status,metadata,updated_at,kernel_contract_version,mapping_id,command_id,"
            "local_vt_orderid,deterministic_client_order_ref,order_remark,mapping_status,mapping_version,"
            "mapping_payload_sha256,mapping_receipt_sha256,broker_identity_source_event_id,last_order_event_id,"
            "last_trade_event_id,created_transition_id,updated_by_event_id,mapping_created_at_utc,"
            "mapping_updated_at_utc,mapping_json) VALUES ("
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'{}'::jsonb,%s,'KERNEL_V2',%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                projection["child_order_id"],
                projection["runtime_id"],
                projection["algo_instance_id"],
                projection["parent_intent_id"],
                projection["strategy_slot_id"],
                projection["symbol"],
                projection["side"],
                projection["quantity"],
                projection["price"],
                projection["price_type"],
                projection["status"],
                projection["updated_at"],
                projection["mapping_id"],
                projection["command_id"],
                projection["local_vt_orderid"],
                projection["deterministic_client_order_ref"],
                projection["order_remark"],
                projection["mapping_status"],
                projection["mapping_version"],
                projection["mapping_payload_sha256"],
                projection["mapping_receipt_sha256"],
                projection["broker_identity_source_event_id"],
                projection["last_order_event_id"],
                projection["last_trade_event_id"],
                projection["created_transition_id"],
                projection["updated_by_event_id"],
                projection["mapping_created_at_utc"],
                projection["mapping_updated_at_utc"],
                _json(mapping.model_dump(mode="json")),
            ),
        )
        if cur.rowcount != 1:
            raise KernelRepositoryConflict("product mapping identity already exists outside idempotent authority")

    def _insert_dependent_buy_coordination_v2_with_cursor(
        self, cur: Any, coordination: DependentBuyCoordinationV2
    ) -> None:
        values = coordination.model_dump(mode="python")
        columns = tuple(key for key in values if key not in {"schema_version", "ordered_sell_dependencies"})
        cur.execute(
            f"INSERT INTO qmt_strategy.execution_dependent_buy_coordination({','.join(columns)},carrier_json) "
            f"VALUES ({','.join(['%s'] * (len(columns) + 1))})",
            (*[values[key] for key in columns], _json(coordination.model_dump(mode="json"))),
        )
        for dependency in coordination.ordered_sell_dependencies:
            cur.execute(
                "INSERT INTO qmt_strategy.execution_dependent_buy_dependency("
                "coordination_id,runtime_id,strategy_id,sell_parent_intent_id,sell_algo_instance_id,"
                "latest_order_fact_ref,settled_trade_fact_refs,settled_cash_ledger_refs,dependency_status,"
                "carrier_json,dependency_sha256,latest_order_fact_id,latest_order_fact_sha256,"
                "ordered_settled_proceeds_refs) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    coordination.coordination_id,
                    dependency.runtime_id,
                    dependency.strategy_id,
                    dependency.sell_parent_intent_id,
                    dependency.sell_algo_instance_id,
                    dependency.latest_order_fact_sha256,
                    _json([item.qmt_trade_fact_sha256 for item in dependency.ordered_settled_proceeds_refs]),
                    _json([item.cash_ledger_fact_sha256 for item in dependency.ordered_settled_proceeds_refs]),
                    dependency.dependency_status.value,
                    _json(dependency.model_dump(mode="json")),
                    dependency.dependency_sha256,
                    dependency.latest_order_fact_id,
                    dependency.latest_order_fact_sha256,
                    _json([item.model_dump(mode="json") for item in dependency.ordered_settled_proceeds_refs]),
                ),
            )

    def _insert_product_authority_v3_with_cursor(self, cur: Any, envelope: ProductCommandAuthorityEnvelopeV3) -> None:
        authority = envelope.authority_set
        projection = _authority_projection_v3(authority)
        columns = tuple(projection)
        cur.execute(
            f"INSERT INTO qmt_strategy.execution_product_command_authority({','.join(columns)},carrier_json) "
            f"VALUES ({','.join(['%s'] * (len(columns) + 1))})",
            (*projection.values(), _json(envelope.model_dump(mode="json"))),
        )
        for item in authority.ordered_items:
            item_projection = _authority_item_projection_v3(item)
            columns = ("authority_set_sha256", *item_projection)
            cur.execute(
                f"INSERT INTO qmt_strategy.execution_product_command_authority_item({','.join(columns)},carrier_json) "
                f"VALUES ({','.join(['%s'] * (len(columns) + 1))})",
                (
                    authority.authority_set_sha256,
                    *(
                        _json(value) if key in {"command_json", "evaluation_evidence_json"} else value
                        for key, value in item_projection.items()
                    ),
                    _json(item.model_dump(mode="json")),
                ),
            )

    def _cas_product_delivery_with_cursor(
        self,
        cur: Any,
        *,
        delivery: AlgoDeliveryPersistenceV1,
        previous: AlgoDeliveryPersistenceV1,
        expected_row_version: int,
    ) -> None:
        projection = _delivery_scalar_projection(delivery)
        cur.execute(
            "UPDATE qmt_strategy.execution_algo_event_delivery SET status=%s,attempt_count=%s,lease_owner=%s,"
            "lease_worker_id=%s,lease_process_incarnation_id=%s,lease_epoch=%s,lease_fence_token=%s,"
            "lease_expires_at=%s,transition_id=%s,last_error_json=%s,next_attempt_at_utc=%s,failure_receipt_id=%s,"
            "skip_receipt_id=%s,row_version=%s,updated_at_utc=%s,closed_at_utc=%s,carrier_json=%s "
            "WHERE delivery_id=%s AND row_version=%s AND lease_owner IS NOT DISTINCT FROM %s "
            "AND lease_epoch=%s AND lease_fence_token IS NOT DISTINCT FROM %s",
            (
                projection["status"],
                projection["attempt_count"],
                projection["lease_owner"],
                projection["lease_worker_id"],
                projection["lease_process_incarnation_id"],
                projection["lease_epoch"],
                projection["lease_fence_token"],
                projection["lease_expires_at"],
                projection["transition_id"],
                None if projection["last_error_json"] is None else _json(projection["last_error_json"]),
                projection["next_attempt_at_utc"],
                projection["failure_receipt_id"],
                projection["skip_receipt_id"],
                projection["row_version"],
                projection["updated_at_utc"],
                projection["closed_at_utc"],
                _json(delivery.model_dump(mode="json")),
                delivery.delivery_id,
                expected_row_version,
                previous.lease_owner,
                previous.lease_epoch,
                previous.lease_fence_token,
            ),
        )
        if cur.rowcount != 1:
            raise KernelRepositoryConflict("K6 product delivery CAS failed")


__all__ = ["KernelProductMaterializationRepositoryMixin"]
