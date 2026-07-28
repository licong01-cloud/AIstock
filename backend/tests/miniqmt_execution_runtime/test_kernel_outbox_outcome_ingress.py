from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

import pytest

from backend.execution_algos.vnpy_compat.receipts import build_current_three_compatibility_receipts_v1
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v1,
    current_three_descriptors_v2,
    current_three_process_bindings_v2,
)
from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
    build_kernel_order_event_payload_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_ingress import route_event_targets_v1
from backend.services.miniqmt_execution_runtime.kernel_outbox import (
    KernelOutboxOutcomeIngressError,
    KernelOutboxOutcomeIngressV1,
    KernelOutboxRecoveryV1,
    _outcome_authority_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    BrokerAckSourceV1,
    BrokerCommandAckReceiptV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    BrokerNonAcceptanceReceiptV1,
    BrokerOutcomeReconciliationReceiptV1,
    BrokerUncertainStageV1,
    BrokerUnknownOutcomeReceiptV1,
    CommandChildMappingStatusV1,
    DeliveryStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionCommandChildMappingV1,
    KernelCommandOutcomeMappingClosureModeV1,
    KernelCommandOutcomeV1,
    KernelErrorEvidenceV1,
    OrderTypeV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
    SideV1,
    _algo_instance_id_v2,
    deterministic_client_order_ref_v1,
    kernel_lease_fence_token_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import build_plugin_catalog_v2


@lru_cache(maxsize=1)
def _catalog():
    return build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v2(),
        creation_bindings=current_three_creation_bindings_v1(),
        process_bindings=current_three_process_bindings_v2(),
        pinned_compatibility_receipts=build_current_three_compatibility_receipts_v1(),
    )


def _algo(*, suffix: str = "one", status: ExecutionAlgoPersistenceStatusV2 = ExecutionAlgoPersistenceStatusV2.ACTIVE):
    descriptor = next(
        item for item in _catalog().snapshot.registration_descriptors if item.manifest.algo_code == "SNIPER_MINIQMT"
    )
    config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    config_hash = hash_hex_v1("miniqmt_plugin_config_v2", config)
    algo_id = _algo_instance_id_v2(
        runtime_id=f"runtime_outcome_{suffix}",
        parent_intent_id=f"intent_outcome_{suffix}",
        strategy_slot_id=f"slot_outcome_{suffix}",
        algo_code=descriptor.manifest.algo_code,
        plugin_id=descriptor.manifest.plugin_id,
        plugin_version=descriptor.manifest.plugin_version,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        plugin_config_sha256=config_hash,
    )
    terminal = status not in {
        ExecutionAlgoPersistenceStatusV2.ACTIVE,
        ExecutionAlgoPersistenceStatusV2.PAUSED,
    }
    state = {"status": "RUNNING", "suffix": suffix}
    return ExecutionAlgoInstancePersistenceV2.create(
        algo_instance_id=algo_id,
        runtime_id=f"runtime_outcome_{suffix}",
        parent_intent_id=f"intent_outcome_{suffix}",
        strategy_slot_id=f"slot_outcome_{suffix}",
        symbol="600000.SH",
        side=SideV1.BUY,
        target_quantity=100,
        traded_quantity=0,
        remaining_quantity=100,
        algo_code=descriptor.manifest.algo_code,
        plugin_id=descriptor.manifest.plugin_id,
        plugin_version=descriptor.manifest.plugin_version,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        plugin_config_json=config,
        plugin_config_sha256=config_hash,
        compatibility_receipt_sha256="a" * 64,
        state_schema_version=descriptor.manifest.state_schema_version,
        state_json=state,
        state_sha256=hash_hex_v1("execution_algo_state_v2", state),
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id=f"delivery_outcome_{suffix}",
        last_closed_delivery_sequence=1,
        terminal_delivery_sequence=1 if terminal else None,
        status=status,
        failure_receipt_id=None,
        active_child_closure_status=(
            ActiveChildClosureStatusV1.CLEAN if terminal else ActiveChildClosureStatusV1.NOT_APPLICABLE
        ),
        active_child_count=0 if terminal else 1,
        row_version=1,
        created_at_utc="2026-07-28T01:00:00Z",
        updated_at_utc="2026-07-28T01:00:00Z",
        terminal_at_utc="2026-07-28T01:00:00Z" if terminal else None,
        archived_at_utc=None,
    )


def _chain(
    outcome: str,
    *,
    suffix: str = "one",
    algo_status: ExecutionAlgoPersistenceStatusV2 = ExecutionAlgoPersistenceStatusV2.ACTIVE,
) -> tuple[ExecutionAlgoInstancePersistenceV2, ExecutionCommandChildMappingV1, BrokerCommandOutboxV1]:
    algo = _algo(suffix=suffix, status=algo_status)
    command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=algo.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id=f"transition_outcome_{suffix}",
        ordinal=0,
        local_vt_orderid=None,
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10",
        quantity=100,
        owned_broker_order_id=None,
        reason_code=f"OUTCOME_{outcome}",
        metadata={"suffix": suffix},
    )
    mapping_status = {
        "accepted": CommandChildMappingStatusV1.DISPATCHING,
        "rejected": CommandChildMappingStatusV1.BROKER_REJECTED,
        "pre_call": CommandChildMappingStatusV1.RESERVED,
        "unknown": CommandChildMappingStatusV1.OUTCOME_UNKNOWN,
    }[outcome]
    mapping = ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id=algo.strategy_slot_id,
        mapping_status=mapping_status,
        mapping_version=1 if outcome == "pre_call" else 2,
        broker_order_id=None,
        broker_identity_source_event_id=None,
        last_order_event_id=None,
        last_trade_event_id=None,
        updated_by_event_id=None,
        created_at_utc="2026-07-28T01:00:00Z",
        updated_at_utc=("2026-07-28T01:00:00Z" if outcome == "pre_call" else "2026-07-28T01:00:01Z"),
    )
    client_ref = deterministic_client_order_ref_v1(command_id=command.command_id, mapping_id=mapping.mapping_id)
    common: dict[str, Any] = {
        "attempt_count": 1,
        "lease_owner": None,
        "lease_epoch": 1,
        "lease_fence_token": None,
        "lease_expires_at": None,
        "dispatch_attempt_id": f"attempt_outcome_{suffix}",
        "next_attempt_at_utc": None,
        "non_acceptance_receipt": None,
        "reconcile_receipt": None,
        "row_version": 3,
        "created_at_utc": "2026-07-28T01:00:00Z",
        "updated_at_utc": "2026-07-28T01:00:02Z",
    }
    if outcome in {"accepted", "rejected"}:
        accepted = outcome == "accepted"
        broker_order_id = f"broker_outcome_{suffix}" if accepted else None
        ack = BrokerCommandAckReceiptV1.create(
            command_id=command.command_id,
            mapping_id=mapping.mapping_id,
            deterministic_client_order_ref=client_ref,
            gateway_route_id="gateway_outcome",
            gateway_catalog_sha256="b" * 64,
            source=BrokerAckSourceV1.SYNCHRONOUS_RETURN,
            accepted=accepted,
            broker_order_id=broker_order_id,
            reason_code="BROKER_ACCEPTED" if accepted else "BROKER_REJECTED",
            ack_payload_sha256="c" * 64,
            observed_at_utc="2026-07-28T01:00:02Z",
        )
        values = {
            **common,
            "status": BrokerCommandOutboxStatusV1.ACKED if accepted else BrokerCommandOutboxStatusV1.ACKED_REJECTED,
            "callback_watermark_before_call": f"{algo.runtime_id}:1",
            "broker_called": True,
            "broker_order_id": broker_order_id,
            "ack_receipt_json": ack,
            "ack_receipt_sha256": ack.receipt_sha256,
            "unknown_outcome_receipt": None,
            "last_error_json": None,
            "closed_at_utc": "2026-07-28T01:00:02Z",
        }
    elif outcome == "unknown":
        unknown = BrokerUnknownOutcomeReceiptV1.create(
            command_id=command.command_id,
            dispatch_attempt_id=common["dispatch_attempt_id"],
            mapping_id=mapping.mapping_id,
            lease_fence_token=f"fence_outcome_{suffix}",
            uncertain_stage=BrokerUncertainStageV1.GATEWAY_RETURN,
            callback_watermark=f"{algo.runtime_id}:1",
            reason_code="OUTCOME_UNKNOWN",
            observed_at_utc="2026-07-28T01:00:02Z",
        )
        values = {
            **common,
            "status": BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
            "callback_watermark_before_call": unknown.callback_watermark,
            "broker_called": None,
            "broker_order_id": None,
            "ack_receipt_json": None,
            "ack_receipt_sha256": None,
            "unknown_outcome_receipt": unknown,
            "last_error_json": None,
            "closed_at_utc": None,
        }
    else:
        error = KernelErrorEvidenceV1.create(
            stage="OUTBOX_PRE_CALL",
            stable_reason_code="PRE_CALL_TERMINAL",
            exception=RuntimeError("pre-call terminal"),
            message="pre-call terminal",
            retryable=False,
            terminal=True,
            broker_called=False,
            primary_context={"command_id": command.command_id},
            secondary_errors=[],
        )
        values = {
            **common,
            "status": BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
            "callback_watermark_before_call": None,
            "broker_called": False,
            "broker_order_id": None,
            "ack_receipt_json": None,
            "ack_receipt_sha256": None,
            "unknown_outcome_receipt": None,
            "last_error_json": error.model_dump(mode="json"),
            "closed_at_utc": "2026-07-28T01:00:02Z",
        }
    return algo, mapping, BrokerCommandOutboxV1.create(command=command, mapping_id=mapping.mapping_id, **values)


def _cancel_chain(
    outcome: str,
) -> tuple[ExecutionAlgoInstancePersistenceV2, ExecutionCommandChildMappingV1, BrokerCommandOutboxV1]:
    algo, _, submit_outbox = _chain("accepted", suffix=f"cancel_{outcome}")
    submit = _read_command(submit_outbox)
    broker_order_id = f"broker_cancel_{outcome}"
    mapping = ExecutionCommandChildMappingV1.create(
        command=submit,
        strategy_slot_id=algo.strategy_slot_id,
        mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
        mapping_version=3,
        broker_order_id=broker_order_id,
        broker_identity_source_event_id=f"event_cancel_{outcome}",
        last_order_event_id=f"event_cancel_{outcome}",
        last_trade_event_id=None,
        updated_by_event_id=f"event_cancel_{outcome}",
        created_at_utc="2026-07-28T01:00:00Z",
        updated_at_utc="2026-07-28T01:00:02Z",
    )
    command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=algo.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id=f"transition_cancel_{outcome}",
        ordinal=1,
        local_vt_orderid=mapping.local_vt_orderid,
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal=mapping.requested_price_decimal,
        quantity=mapping.requested_quantity,
        owned_broker_order_id=broker_order_id,
        reason_code=f"CANCEL_{outcome}",
        metadata={"outcome": outcome},
    )
    client_ref = deterministic_client_order_ref_v1(command_id=command.command_id, mapping_id=mapping.mapping_id)
    common: dict[str, Any] = {
        "attempt_count": 1,
        "lease_owner": None,
        "lease_epoch": 1,
        "lease_fence_token": None,
        "lease_expires_at": None,
        "dispatch_attempt_id": f"attempt_cancel_{outcome}",
        "next_attempt_at_utc": None,
        "non_acceptance_receipt": None,
        "reconcile_receipt": None,
        "row_version": 3,
        "created_at_utc": "2026-07-28T01:00:02Z",
        "updated_at_utc": "2026-07-28T01:00:03Z",
    }
    if outcome in {"accepted", "rejected"}:
        accepted = outcome == "accepted"
        ack = BrokerCommandAckReceiptV1.create(
            command_id=command.command_id,
            mapping_id=mapping.mapping_id,
            deterministic_client_order_ref=client_ref,
            gateway_route_id="gateway_outcome",
            gateway_catalog_sha256="b" * 64,
            source=BrokerAckSourceV1.SYNCHRONOUS_RETURN,
            accepted=accepted,
            broker_order_id=broker_order_id if accepted else None,
            reason_code="CANCEL_ACCEPTED" if accepted else "CANCEL_REJECTED",
            ack_payload_sha256="c" * 64,
            observed_at_utc="2026-07-28T01:00:03Z",
        )
        values = {
            **common,
            "status": BrokerCommandOutboxStatusV1.ACKED if accepted else BrokerCommandOutboxStatusV1.ACKED_REJECTED,
            "callback_watermark_before_call": f"{algo.runtime_id}:1",
            "broker_called": True,
            "broker_order_id": broker_order_id if accepted else None,
            "ack_receipt_json": ack,
            "ack_receipt_sha256": ack.receipt_sha256,
            "unknown_outcome_receipt": None,
            "last_error_json": None,
            "closed_at_utc": "2026-07-28T01:00:03Z",
        }
    elif outcome == "unknown":
        unknown = BrokerUnknownOutcomeReceiptV1.create(
            command_id=command.command_id,
            dispatch_attempt_id=common["dispatch_attempt_id"],
            mapping_id=mapping.mapping_id,
            lease_fence_token=kernel_lease_fence_token_v1(
                owner_type="COMMAND",
                owner_id=command.command_id,
                lease_epoch=1,
                lease_owner="worker_cancel_unknown:incarnation_1",
            ),
            uncertain_stage=BrokerUncertainStageV1.GATEWAY_RETURN,
            callback_watermark=f"{algo.runtime_id}:1",
            reason_code="CANCEL_UNKNOWN",
            observed_at_utc="2026-07-28T01:00:03Z",
        )
        values = {
            **common,
            "status": BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
            "callback_watermark_before_call": unknown.callback_watermark,
            "broker_called": None,
            "broker_order_id": None,
            "ack_receipt_json": None,
            "ack_receipt_sha256": None,
            "unknown_outcome_receipt": unknown,
            "last_error_json": None,
            "closed_at_utc": None,
        }
    else:
        error = KernelErrorEvidenceV1.create(
            stage="OUTBOX_PRE_CALL",
            stable_reason_code="CANCEL_PRE_CALL_TERMINAL",
            exception=RuntimeError("cancel pre-call terminal"),
            message="cancel pre-call terminal",
            retryable=False,
            terminal=True,
            broker_called=False,
            primary_context={"command_id": command.command_id},
            secondary_errors=[],
        )
        values = {
            **common,
            "status": BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
            "callback_watermark_before_call": None,
            "broker_called": False,
            "broker_order_id": None,
            "ack_receipt_json": None,
            "ack_receipt_sha256": None,
            "unknown_outcome_receipt": None,
            "last_error_json": error.model_dump(mode="json"),
            "closed_at_utc": "2026-07-28T01:00:03Z",
        }
    return algo, mapping, BrokerCommandOutboxV1.create(command=command, mapping_id=mapping.mapping_id, **values)


class _OutcomeRepository:
    def __init__(self, algo, mapping, outbox) -> None:  # type: ignore[no-untyped-def]
        self.algo = algo
        self.mapping = mapping
        self.outbox = outbox
        self.events: dict[str, dict[str, Any]] = {}
        self.last_sequence = 0
        self.ingress_calls = 0

    def read_command_identity_chain(self, command_id: str) -> dict[str, Any]:
        assert command_id == self.outbox.command_id
        return {"mapping": self.mapping, "outbox": self.outbox}

    def read_event_transaction(self, event_id: str) -> dict[str, Any]:
        try:
            return self.events[event_id]
        except KeyError as exc:
            raise KeyError(event_id) from exc

    def read_algo_instance(self, algo_instance_id: str):  # type: ignore[no-untyped-def]
        assert algo_instance_id == self.algo.algo_instance_id
        return self.algo

    def read_runtime_last_event_sequence(self, runtime_id: str) -> int:
        assert runtime_id == self.algo.runtime_id
        return self.last_sequence

    def _read_exchange_session_projection(self, *, runtime_id: str, observed_at_utc: Any):  # type: ignore[no-untyped-def]
        from types import SimpleNamespace

        from backend.services.miniqmt_execution_runtime.plugin_contracts import SessionPhaseV1

        assert runtime_id == self.algo.runtime_id
        assert observed_at_utc == self.outbox.updated_at_utc
        return SimpleNamespace(
            runtime_id=runtime_id,
            exchange_trade_date="2026-07-28",
            session_epoch="session_outcome_k3a",
            session_phase=SessionPhaseV1.CONTINUOUS_AM,
        )

    def ingest_routed_event_atomic(self, **values: Any) -> RuntimeEventIngressReceiptV1:
        event = values["event"]
        closure = values["command_outcome_mapping_closure"]
        targets = route_event_targets_v1(
            event=event,
            algo_instances=(self.algo,),
            catalog_runtime=values["catalog_runtime"],
            correlated_algo_instance_ids=values["correlated_algo_instance_ids"],
        )
        if closure.mode is KernelCommandOutcomeMappingClosureModeV1.ADVANCE_MAPPING:
            assert closure.expected_mapping_version == self.mapping.mapping_version
            closure.mapping.validate_successor_v1(self.mapping)
            self.mapping = closure.mapping
            self.algo = self.algo.model_copy(update={"row_version": self.algo.row_version + 1})
        else:
            assert closure.mapping == self.mapping
            callback = self.events[closure.preceding_callback_event_id]
            assert callback["event"].payload_sha256 == closure.preceding_callback_payload_sha256
            assert callback["deliveries"][0].delivery_id == closure.preceding_callback_delivery_id
        deliveries = tuple(self._delivery(event, algo_id) for algo_id in targets)
        commit_identity = "tx_" + hash_hex_v1(
            "outcome_test_transaction",
            {"event_id": event.event_id, "closure_sha256": closure.closure_sha256},
        )
        receipt = RuntimeEventIngressReceiptV1.create(
            runtime_id=event.runtime_id,
            event_id=event.event_id,
            event_key_sha256=event.event_key_sha256,
            runtime_sequence=event.sequence,
            ordered_target_algo_instance_ids=targets,
            ordered_delivery_ids=tuple(item.delivery_id for item in deliveries),
            transaction_commit_identity=commit_identity,
        )
        self.events[event.event_id] = {"event": event, "receipt": receipt, "deliveries": deliveries}
        self.last_sequence = event.sequence
        self.ingress_calls += 1
        return receipt

    def _delivery(self, event: RuntimeEventEnvelopeV2, algo_instance_id: str) -> AlgoDeliveryPersistenceV1:
        delivery = AlgoEventDeliveryV1.create(
            event=event,
            algo_instance_id=algo_instance_id,
            plugin_manifest_sha256=self.algo.plugin_manifest_sha256,
            algo_delivery_sequence=self.algo.last_closed_delivery_sequence + 1,
            previous_delivery_id=self.algo.last_applied_delivery_id,
            status=DeliveryStatusV1.PENDING,
            attempt_count=0,
            lease_owner=None,
            lease_expires_at=None,
            transition_id=None,
            last_error_json=None,
            created_at_utc=event.event_time_utc,
            updated_at_utc=event.event_time_utc,
        )
        return AlgoDeliveryPersistenceV1.create(
            delivery=delivery,
            lease_epoch=0,
            lease_fence_token=None,
            row_version=1,
            next_attempt_at_utc=None,
            failure_receipt_id=None,
            skip_receipt_id=None,
            closed_at_utc=None,
        )

    def seed_callback_precedence(
        self,
        *,
        terminal: bool = False,
        rejected: bool = False,
    ) -> RuntimeEventEnvelopeV2:
        command = _read_command(self.outbox)
        if command.command_type is BrokerCommandTypeV2.CANCEL_ORDER:
            suffix = self.algo.runtime_id.removeprefix("runtime_outcome_")
            command = BrokerCommandV2.create(
                command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
                runtime_id=self.algo.runtime_id,
                algo_instance_id=self.algo.algo_instance_id,
                parent_intent_id=self.algo.parent_intent_id,
                transition_id=self.mapping.created_transition_id,
                ordinal=0,
                local_vt_orderid=None,
                symbol=self.mapping.symbol,
                side=self.mapping.side,
                order_type=OrderTypeV1.LIMIT,
                price_decimal=self.mapping.requested_price_decimal,
                quantity=self.mapping.requested_quantity,
                owned_broker_order_id=None,
                reason_code="OUTCOME_accepted",
                metadata={"suffix": suffix},
            )
            assert command.command_id == self.mapping.command_id
        raw_status = 57 if rejected else 56 if terminal else 48
        payload = build_kernel_order_event_payload_v1(
            raw_payload={"order_status": raw_status, "traded_volume": 100 if terminal and not rejected else 0},
            order_event_id=f"order_callback_{self.outbox.command_id}",
            runtime_id=self.algo.runtime_id,
            algo_instance_id=self.algo.algo_instance_id,
            parent_intent_id=self.algo.parent_intent_id,
            strategy_slot_id=self.algo.strategy_slot_id,
            mapping_id=self.mapping.mapping_id,
            command_id=self.outbox.command_id,
            local_vt_orderid=self.mapping.local_vt_orderid,
            broker_order_id=(
                self.mapping.broker_order_id or self.outbox.broker_order_id or "broker_callback_precedence"
            ),
            symbol=self.algo.symbol,
            side=self.algo.side,
            requested_quantity=100,
        )
        callback = RuntimeEventEnvelopeV2.create(
            runtime_id=self.algo.runtime_id,
            sequence=2,
            event_type=EventTypeV2.ORDER,
            event_time_utc="2026-07-28T01:00:01Z",
            monotonic_ns=None,
            source=EventSourceV2.QMT_GATEWAY_CALLBACK,
            symbol=self.algo.symbol,
            payload_schema_version="miniqmt_order_event_v1",
            payload=payload.model_dump(mode="json"),
            source_identity={"order_event_id": payload.order_event_id},
            correlation={
                "algo_instance_id": self.algo.algo_instance_id,
                "mapping_id": self.mapping.mapping_id,
                "reference_command_id": self.outbox.command_id,
            },
        )
        mapping_values = self.mapping.model_dump(mode="python")
        mapping_values.update(
            mapping_status=(
                CommandChildMappingStatusV1.TERMINAL
                if terminal or rejected
                else CommandChildMappingStatusV1.BROKER_ACCEPTED
            ),
            broker_order_id=payload.broker_order_id,
            broker_identity_source_event_id=callback.event_id,
            last_order_event_id=callback.event_id,
            updated_by_event_id=callback.event_id,
            mapping_version=self.mapping.mapping_version + 1,
            updated_at_utc=callback.event_time_utc,
        )
        mapping_values.pop("mapping_payload_sha256", None)
        mapping_values.pop("mapping_receipt_sha256", None)
        self.mapping = ExecutionCommandChildMappingV1.create(
            command=command,
            strategy_slot_id=self.algo.strategy_slot_id,
            mapping_status=mapping_values["mapping_status"],
            mapping_version=mapping_values["mapping_version"],
            broker_order_id=mapping_values["broker_order_id"],
            broker_identity_source_event_id=mapping_values["broker_identity_source_event_id"],
            last_order_event_id=mapping_values["last_order_event_id"],
            last_trade_event_id=self.mapping.last_trade_event_id,
            updated_by_event_id=mapping_values["updated_by_event_id"],
            created_at_utc=self.mapping.created_at_utc,
            updated_at_utc=mapping_values["updated_at_utc"],
        )
        applied = self._delivery(callback, self.algo.algo_instance_id).model_copy(
            update={"status": DeliveryStatusV1.APPLIED, "closed_at_utc": callback.event_time_utc}
        )
        self.events[callback.event_id] = {"event": callback, "receipt": None, "deliveries": (applied,)}
        self.last_sequence = callback.sequence
        return callback


@pytest.mark.parametrize(
    ("outcome", "expected", "mapping_status"),
    [
        ("accepted", "ACCEPTED", CommandChildMappingStatusV1.BROKER_ACCEPTED),
        ("rejected", "REJECTED", CommandChildMappingStatusV1.BROKER_REJECTED),
        ("pre_call", "PRE_CALL_TERMINAL", CommandChildMappingStatusV1.BROKER_REJECTED),
        ("unknown", "OUTCOME_UNKNOWN", CommandChildMappingStatusV1.OUTCOME_UNKNOWN),
    ],
)
def test_outbox_outcome_ingress_publishes_exact_strict_event(outcome, expected, mapping_status) -> None:
    repository = _OutcomeRepository(*_chain(outcome))

    receipt = KernelOutboxOutcomeIngressV1(repository=repository, catalog_runtime=_catalog()).ingest_outbox_outcome_v1(
        command_id=repository.outbox.command_id
    )

    assert receipt.event.event_type is EventTypeV2.COMMAND_OUTCOME
    assert receipt.event.source is EventSourceV2.MINIQMT_EXECUTION_KERNEL
    correlation = thaw_json_v1(receipt.event.correlation)
    assert correlation["exchange_trade_date"] == "2026-07-28"
    assert correlation["session_epoch"] == "session_outcome_k3a"
    assert correlation["session_phase"] == "CONTINUOUS_AM"
    assert receipt.event.payload_schema_version == "miniqmt_command_outcome_v1"
    assert thaw_json_v1(receipt.event.payload)["outcome"] == expected
    assert repository.mapping.mapping_status is mapping_status
    assert receipt.mapping_closure.mapping == repository.mapping
    assert repository.ingress_calls == 1


def test_outcome_ingress_restart_is_idempotent_without_second_mapping_mutation() -> None:
    repository = _OutcomeRepository(*_chain("accepted"))
    ingress = KernelOutboxOutcomeIngressV1(repository=repository, catalog_runtime=_catalog())
    first = ingress.ingest_outbox_outcome_v1(command_id=repository.outbox.command_id)
    persisted_mapping = repository.mapping

    second = ingress.ingest_outbox_outcome_v1(command_id=repository.outbox.command_id)

    assert second.idempotent is True
    assert second.event == first.event
    assert repository.mapping == persisted_mapping
    assert second.mapping_closure.mapping == persisted_mapping
    assert repository.ingress_calls == 1


def test_outcome_ingress_idempotent_closure_survives_later_algo_row_advances() -> None:
    repository = _OutcomeRepository(*_chain("accepted"))
    ingress = KernelOutboxOutcomeIngressV1(repository=repository, catalog_runtime=_catalog())
    first = ingress.ingest_outbox_outcome_v1(command_id=repository.outbox.command_id)
    repository.algo = repository.algo.model_copy(update={"row_version": repository.algo.row_version + 4})

    replay = ingress.ingest_outbox_outcome_v1(command_id=repository.outbox.command_id)

    assert replay.idempotent is True
    assert replay.mapping_closure == first.mapping_closure
    assert repository.ingress_calls == 1


def test_failed_terminal_outcome_authority_priority_is_reconcile_then_unknown_then_error() -> None:
    _, mapping, unknown_outbox = _chain("unknown", suffix="authority_priority")
    command = _read_command(unknown_outbox)
    unknown = unknown_outbox.unknown_outcome_receipt
    assert unknown is not None
    non_acceptance = BrokerNonAcceptanceReceiptV1.create(
        command_id=command.command_id,
        deterministic_client_order_ref=unknown_outbox.deterministic_client_order_ref,
        gateway_route_id="gateway_authority_priority",
        gateway_catalog_sha256="a" * 64,
        query_criteria_sha256="b" * 64,
        callback_watermark_before=unknown.callback_watermark,
        callback_watermark_after=f"{command.runtime_id}:2",
        order_snapshot_sha256="c" * 64,
        trade_snapshot_sha256="d" * 64,
        observed_at_utc="2026-07-28T01:00:03Z",
        reason_code="EXACT_NON_ACCEPTANCE",
    )
    reconcile = BrokerOutcomeReconciliationReceiptV1.create(
        command_id=command.command_id,
        reconcile_attempt=1,
        query_criteria_sha256=non_acceptance.query_criteria_sha256,
        callback_watermark=non_acceptance.callback_watermark_after,
        ordered_matched_order_ids=(),
        ordered_matched_trade_ids=(),
        order_snapshot_sha256=non_acceptance.order_snapshot_sha256,
        trade_snapshot_sha256=non_acceptance.trade_snapshot_sha256,
        outcome="UNIQUE_REJECTED",
        broker_called=True,
        broker_order_id=None,
        reason_code="RECONCILED_REJECTED",
        observed_at_utc="2026-07-28T01:00:04Z",
    )
    error = KernelErrorEvidenceV1.create(
        stage="OUTBOX_RECONCILE",
        stable_reason_code="AUTHORITY_PRIORITY_ERROR",
        exception=RuntimeError("authority priority"),
        message="authority priority",
        retryable=False,
        terminal=True,
        broker_called=False,
        primary_context={"command_id": command.command_id},
        secondary_errors=(),
    )

    def terminal(*, non_acceptance_receipt=None, reconcile_receipt=None, unknown_receipt=None, last_error=None):
        return BrokerCommandOutboxV1.create(
            command=command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
            attempt_count=unknown_outbox.attempt_count,
            lease_owner=None,
            lease_epoch=unknown_outbox.lease_epoch,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=unknown_outbox.dispatch_attempt_id,
            callback_watermark_before_call=unknown_outbox.callback_watermark_before_call,
            next_attempt_at_utc=None,
            broker_called=(None if unknown_receipt is not None and non_acceptance_receipt is None else False),
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=non_acceptance_receipt,
            unknown_outcome_receipt=unknown_receipt,
            reconcile_receipt=reconcile_receipt,
            last_error_json=None if last_error is None else last_error.model_dump(mode="json"),
            row_version=unknown_outbox.row_version + 1,
            created_at_utc=unknown_outbox.created_at_utc,
            updated_at_utc="2026-07-28T01:00:05Z",
            closed_at_utc="2026-07-28T01:00:05Z",
        )

    reconcile_first = terminal(
        non_acceptance_receipt=non_acceptance,
        reconcile_receipt=reconcile,
        unknown_receipt=unknown,
        last_error=error,
    )
    with pytest.raises(ValueError, match="non-acceptance receipt conflicts"):
        terminal(
            non_acceptance_receipt=non_acceptance,
            unknown_receipt=unknown,
            last_error=error,
        )
    unknown_first = terminal(unknown_receipt=unknown, last_error=error)
    error_only = _chain("pre_call", suffix="authority_error_only")[2]

    assert _outcome_authority_v1(reconcile_first) == (
        KernelCommandOutcomeV1.REJECTED,
        reconcile.receipt_sha256,
    )
    assert _outcome_authority_v1(unknown_first) == (
        KernelCommandOutcomeV1.CONFLICT,
        unknown.receipt_sha256,
    )
    assert _outcome_authority_v1(error_only)[0] is KernelCommandOutcomeV1.PRE_CALL_TERMINAL


@pytest.mark.parametrize("outcome", ["accepted", "rejected", "unknown", "pre_call"])
def test_cancel_outcome_always_preserves_original_broker_order_identity(outcome: str) -> None:
    repository = _OutcomeRepository(*_cancel_chain(outcome))
    repository.seed_callback_precedence()
    original_broker_order_id = repository.mapping.broker_order_id

    receipt = KernelOutboxOutcomeIngressV1(repository=repository, catalog_runtime=_catalog()).ingest_outbox_outcome_v1(
        command_id=repository.outbox.command_id
    )

    assert thaw_json_v1(receipt.event.payload)["broker_order_id"] == original_broker_order_id
    assert repository.mapping.broker_order_id == original_broker_order_id
    assert repository.mapping.mapping_status is CommandChildMappingStatusV1.BROKER_ACCEPTED


def test_callback_before_ack_uses_verify_precedence_and_never_overwrites_callback_lineage() -> None:
    repository = _OutcomeRepository(*_chain("accepted"))
    callback = repository.seed_callback_precedence()
    persisted_mapping = repository.mapping

    receipt = KernelOutboxOutcomeIngressV1(repository=repository, catalog_runtime=_catalog()).ingest_outbox_outcome_v1(
        command_id=repository.outbox.command_id
    )

    assert receipt.mapping_closure.mode is KernelCommandOutcomeMappingClosureModeV1.VERIFY_CALLBACK_PRECEDENCE
    assert receipt.mapping_closure.preceding_callback_event_id == callback.event_id
    assert repository.mapping == persisted_mapping


def test_callback_precedence_missing_durable_event_fails_loud() -> None:
    repository = _OutcomeRepository(*_chain("accepted"))
    callback = repository.seed_callback_precedence()
    del repository.events[callback.event_id]

    with pytest.raises(KernelOutboxOutcomeIngressError, match="absent from durable event authority") as raised:
        KernelOutboxOutcomeIngressV1(
            repository=repository,
            catalog_runtime=_catalog(),
        ).ingest_outbox_outcome_v1(command_id=repository.outbox.command_id)

    assert raised.value.reason_code == "MINIQMT_COMMAND_OUTCOME_CALLBACK_PRECEDENCE_MISSING"
    assert raised.value.context["callback_event_id"] == callback.event_id


@pytest.mark.parametrize(
    ("outcome", "rejected_callback"),
    [("rejected", False), ("accepted", True)],
)
def test_callback_before_ack_rejects_submit_acceptance_conflict(outcome: str, rejected_callback: bool) -> None:
    repository = _OutcomeRepository(*_chain(outcome))
    repository.seed_callback_precedence(rejected=rejected_callback)

    with pytest.raises(KernelOutboxOutcomeIngressError, match="callback.*outcome conflict"):
        KernelOutboxOutcomeIngressV1(repository=repository, catalog_runtime=_catalog()).ingest_outbox_outcome_v1(
            command_id=repository.outbox.command_id
        )


def test_terminal_callback_before_ack_allows_zero_owner_delivery_set_only_after_applied_callback() -> None:
    algo, mapping, outbox = _chain(
        "accepted",
        algo_status=ExecutionAlgoPersistenceStatusV2.COMPLETED,
    )
    repository = _OutcomeRepository(algo, mapping, outbox)
    callback = repository.seed_callback_precedence(terminal=True)

    receipt = KernelOutboxOutcomeIngressV1(repository=repository, catalog_runtime=_catalog()).ingest_outbox_outcome_v1(
        command_id=repository.outbox.command_id
    )

    assert receipt.mapping_closure.mode is KernelCommandOutcomeMappingClosureModeV1.VERIFY_CALLBACK_PRECEDENCE
    assert receipt.mapping_closure.preceding_callback_event_id == callback.event_id
    assert receipt.ingress_receipt.ordered_target_algo_instance_ids == ()
    assert receipt.ingress_receipt.ordered_delivery_ids == ()


def test_outcome_ingress_rejects_conflicting_broker_identity() -> None:
    algo, mapping, outbox = _chain("accepted")
    command = _read_command(outbox)
    conflicting = ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id=algo.strategy_slot_id,
        mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
        mapping_version=3,
        broker_order_id="broker_conflicting",
        broker_identity_source_event_id="event_conflicting",
        last_order_event_id="event_conflicting",
        last_trade_event_id=None,
        updated_by_event_id="event_conflicting",
        created_at_utc=mapping.created_at_utc,
        updated_at_utc="2026-07-28T01:00:02Z",
    )
    repository = _OutcomeRepository(algo, conflicting, outbox)

    with pytest.raises(KernelOutboxOutcomeIngressError, match="conflicting broker order identities"):
        KernelOutboxOutcomeIngressV1(repository=repository, catalog_runtime=_catalog()).ingest_outbox_outcome_v1(
            command_id=outbox.command_id
        )


def test_bounded_outcome_recovery_uses_stable_cursor_and_run_limit() -> None:
    rows = tuple(_chain("accepted", suffix=f"scan_{index}")[2] for index in range(3))

    class CandidateRepository:
        def __init__(self) -> None:
            self.calls: list[tuple[tuple[str, str, str] | None, int]] = []

        def list_outbox_outcome_candidates(self, *, after, limit):  # type: ignore[no-untyped-def]
            self.calls.append((after, limit))
            ordered = sorted(rows, key=lambda item: (item.runtime_id, item.updated_at_utc, item.command_id))
            eligible = [
                item
                for item in ordered
                if after is None or (item.runtime_id, item.updated_at_utc, item.command_id) > after
            ]
            return tuple(eligible[:limit])

    class Publisher:
        def __init__(self) -> None:
            self.command_ids: list[str] = []

        def ingest_outbox_outcome_v1(self, *, command_id: str) -> str:
            self.command_ids.append(command_id)
            return command_id

    repository = CandidateRepository()
    publisher = Publisher()
    receipts = KernelOutboxRecoveryV1(repository=repository, outcome_ingress=publisher).run_once(
        page_size=1,
        max_rows=2,
    )

    assert receipts == tuple(publisher.command_ids)
    assert len(receipts) == 2
    assert repository.calls[0] == (None, 1)
    assert repository.calls[1][0] is not None


def _read_command(outbox: BrokerCommandOutboxV1) -> BrokerCommandV2:
    return BrokerCommandV2.model_validate_json(
        json.dumps(thaw_json_v1(outbox.payload_json), sort_keys=True, separators=(",", ":"))
    )
