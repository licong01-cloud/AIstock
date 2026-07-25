from __future__ import annotations

import json
from datetime import UTC, date, datetime, time

import pytest
from pydantic import ValidationError

from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    MarketCode,
    SessionSegment,
    canonical_json_bytes,
)

from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    AlgoFailureReceiptV1,
    AlgoSkipReceiptV1,
    AlgoTransitionReceiptV1,
    BrokerCommandAckReceiptV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    BrokerDispatchAttemptV1,
    BrokerNonAcceptanceReceiptV1,
    BrokerOutcomeReconciliationReceiptV1,
    BrokerUnknownOutcomeReceiptV1,
    CommandChildMappingStatusV1,
    ConsumedLineageRefV1,
    DeliveryStatusV1,
    ExchangeSessionAuthorityV1,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionAlgoTimerOccurrenceStatusV1,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoTimerOccurrenceV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    KernelErrorEvidenceV1,
    KernelWorkerStartupReceiptV1,
    KernelProjectionTypeV1,
    MiniQMTRiskDecisionReceiptV1,
    OMSPreflightProjectionReceiptV1,
    RiskDecisionActionV1,
    RuntimeEventIngressReceiptV1,
    RuntimeEventEnvelopeV2,
    EventSourceV2,
    EventTypeV2,
    SideV1,
    OrderTypeV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    TransactionCommitIdentityV1,
    kernel_lease_fence_token_v1,
    transaction_commit_identity_v1,
)


def _sha(char: str) -> str:
    return char * 64


def _algo_id() -> str:
    return "mqalgo_" + hash_hex_v1(
        "miniqmt_algo_instance_v2",
        {
            "runtime_id": "runtime_k2",
            "parent_intent_id": "intent_k2",
            "strategy_slot_id": "slot_k2",
            "algo_code": "TWAP",
            "plugin_id": "aistock.twap",
            "plugin_version": "1.0.0",
            "plugin_manifest_sha256": _sha("1"),
            "plugin_config_sha256": hash_hex_v1("miniqmt_plugin_config_v2", {"slices": 4}),
        },
    )


def _tick_event() -> RuntimeEventEnvelopeV2:
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k2",
        sequence=1,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-07-25T01:30:00Z",
        monotonic_ns=None,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol="600000.SH",
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={"last_price": "10.000000"},
        source_identity={"market_data_id": "market_data_k2"},
        correlation={"trace_id": "trace_k2"},
    )


def _submit_command() -> BrokerCommandV2:
    return BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="runtime_k2",
        algo_instance_id=_algo_id(),
        parent_intent_id="intent_k2",
        transition_id="mqtransition_k2",
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.000000",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="MINIQMT_ALGO_SLICE_DUE",
        metadata={"slice": 1},
    )


def _mapping(*, status: CommandChildMappingStatusV1, version: int) -> ExecutionCommandChildMappingV1:
    return ExecutionCommandChildMappingV1.create(
        command=_submit_command(),
        strategy_slot_id="slot_k2",
        mapping_status=status,
        mapping_version=version,
        broker_order_id=None,
        broker_identity_source_event_id=None,
        last_order_event_id=None,
        last_trade_event_id=None,
        updated_by_event_id=None,
        created_at_utc="2026-07-25T01:30:00Z",
        updated_at_utc=f"2026-07-25T01:3{version - 1}:00Z",
    )


def _pending_outbox() -> BrokerCommandOutboxV1:
    mapping = _mapping(status=CommandChildMappingStatusV1.RESERVED, version=1)
    return BrokerCommandOutboxV1.create(
        command=_submit_command(),
        mapping_id=mapping.mapping_id,
        status=BrokerCommandOutboxStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at=None,
        dispatch_attempt_id=None,
        next_attempt_at_utc=None,
        broker_called=None,
        broker_order_id=None,
        ack_receipt_json=None,
        ack_receipt_sha256=None,
        non_acceptance_receipt=None,
        unknown_outcome_receipt=None,
        reconcile_receipt=None,
        last_error_json=None,
        row_version=1,
        created_at_utc="2026-07-25T01:30:00Z",
        updated_at_utc="2026-07-25T01:30:00Z",
        closed_at_utc=None,
    )


def _calendar_authority_values(*, snapshot_set_id: str = "calendar_set_k2") -> dict[str, object]:
    segments = (SessionSegment(time(9, 30), time(11, 30)), SessionSegment(time(13), time(15)))
    effective_at = datetime(2026, 7, 24, 16, tzinfo=UTC)
    snapshots = {
        market: CalendarSnapshot(
            calendar_id=f"calendar_{market.value}_20260725",
            market=market,
            trade_date=date(2026, 7, 25),
            timezone="Asia/Shanghai",
            session_segments=segments,
            effective_at_utc=effective_at,
            source_version="aistock_calendar_v1",
        )
        for market in MarketCode
    }
    snapshot_set = CalendarSnapshotSet(snapshot_set_id=snapshot_set_id, snapshot_by_market=snapshots)
    snapshot_json = json.loads(canonical_json_bytes(snapshot_set.canonical_payload()).decode("utf-8"))
    snapshot_json["set_sha256"] = snapshot_set.set_sha256
    ordered = (MarketCode.SH, MarketCode.SZ, MarketCode.BJ)
    return {
        "calendar_snapshot_set_id": snapshot_set.snapshot_set_id,
        "calendar_snapshot_set_json": snapshot_json,
        "calendar_snapshot_set_sha256": snapshot_set.set_sha256,
        "ordered_market_calendar_sha256s": tuple(
            snapshot_set.snapshot_by_market[market].calendar_sha256 for market in ordered
        ),
        "ordered_session_segments": tuple(segment.canonical_payload() for segment in segments),
        "source_effective_at_utc": effective_at,
    }


def test_k2a_public_contract_surface_is_complete() -> None:
    public_contracts = (
        ExecutionAlgoInstancePersistenceV2,
        AlgoDeliveryPersistenceV1,
        AlgoTransitionReceiptV1,
        AlgoFailureReceiptV1,
        AlgoSkipReceiptV1,
        OMSPreflightProjectionReceiptV1,
        BrokerCommandOutboxV1,
        BrokerDispatchAttemptV1,
        BrokerCommandAckReceiptV1,
        BrokerUnknownOutcomeReceiptV1,
        BrokerNonAcceptanceReceiptV1,
        BrokerOutcomeReconciliationReceiptV1,
        ExecutionCommandChildMappingV1,
        KernelWorkerStartupReceiptV1,
        ExecutionAlgoTimerScheduleV1,
        ExecutionAlgoTimerOccurrenceV1,
        ExchangeSessionAuthorityV1,
        TransactionCommitIdentityV1,
    )

    assert all(model.model_config.get("frozen") is True for model in public_contracts)
    assert transaction_commit_identity_v1(
        operation="K2A_SURFACE_PROBE",
        owner_identities=("runtime_k2",),
        input_hashes=(_sha("1"),),
        output_identities=("mqsurface_probe",),
    ).startswith("mqtx_")


def test_ingress_receipt_closes_identity_delivery_set_transaction_and_readback() -> None:
    receipt = RuntimeEventIngressReceiptV1.create(
        runtime_id="runtime_k2",
        event_id="mqrtevt_event_a",
        event_key_sha256=_sha("1"),
        runtime_sequence=7,
        ordered_target_algo_instance_ids=("mqalgo_a", "mqalgo_b"),
        ordered_delivery_ids=("mqdelivery_a", "mqdelivery_b"),
        transaction_commit_identity="mqtx_ingress_a",
    )

    assert receipt.ingress_receipt_id.startswith("mqingress_")
    assert receipt.delivery_set_sha256 == hash_hex_v1(
        "miniqmt_event_delivery_set_v1",
        {
            "event_id": "mqrtevt_event_a",
            "routing_rule_version": "miniqmt_event_routing_v1",
            "ordered_target_algo_instance_ids": ["mqalgo_a", "mqalgo_b"],
            "ordered_delivery_ids": ["mqdelivery_a", "mqdelivery_b"],
        },
    )
    assert RuntimeEventIngressReceiptV1.model_validate(receipt.model_dump(mode="python"), strict=True) == receipt
    assert RuntimeEventIngressReceiptV1.model_validate_json(receipt.model_dump_json()) == receipt

    drift = {**receipt.model_dump(mode="python"), "receipt_sha256": _sha("f")}
    with pytest.raises(ValidationError, match="receipt_sha256"):
        RuntimeEventIngressReceiptV1.model_validate(drift, strict=True)


@pytest.mark.parametrize(
    ("targets", "deliveries", "message"),
    [
        (("mqalgo_b", "mqalgo_a"), ("mqdelivery_b", "mqdelivery_a"), "sorted"),
        (("mqalgo_a", "mqalgo_a"), ("mqdelivery_a", "mqdelivery_b"), "duplicate"),
        (("mqalgo_a",), ("mqdelivery_a", "mqdelivery_b"), "cardinality"),
    ],
)
def test_ingress_receipt_rejects_noncanonical_or_incomplete_delivery_sets(
    targets: tuple[str, ...], deliveries: tuple[str, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        RuntimeEventIngressReceiptV1.create(
            runtime_id="runtime_k2",
            event_id="mqrtevt_event_a",
            event_key_sha256=_sha("1"),
            runtime_sequence=7,
            ordered_target_algo_instance_ids=targets,
            ordered_delivery_ids=deliveries,
            transaction_commit_identity="mqtx_ingress_a",
        )


def test_projection_set_is_exact_sorted_immutable_and_hash_closed() -> None:
    contract = ExecutionProjectionRefV1.create(
        projection_type=KernelProjectionTypeV1.CONTRACT,
        projection_id="contract_a",
        projection_version="v1",
        payload_sha256=_sha("2"),
        source_event_id=None,
        logical_at_utc="2026-07-25T01:30:00Z",
    )
    risk = ExecutionProjectionRefV1.create(
        projection_type=KernelProjectionTypeV1.RISK_DECISION,
        projection_id="risk_a",
        projection_version="v1",
        payload_sha256=_sha("3"),
        source_event_id="mqrtevt_risk_a",
        logical_at_utc="2026-07-25T01:30:00Z",
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id="runtime_k2",
        algo_instance_id="mqalgo_a",
        event_id="mqrtevt_event_a",
        delivery_id="mqdelivery_a",
        projection_refs=(contract, risk),
    )

    assert tuple(item.projection_type for item in projection_set.ordered_projection_refs) == (
        KernelProjectionTypeV1.CONTRACT,
        KernelProjectionTypeV1.RISK_DECISION,
    )
    with pytest.raises(ValidationError, match="projection_set_sha256"):
        ExecutionProjectionSetV1.model_validate(
            {**projection_set.model_dump(mode="python"), "projection_set_sha256": _sha("f")}, strict=True
        )
    with pytest.raises(ValueError, match="sorted"):
        ExecutionProjectionSetV1.create(
            runtime_id="runtime_k2",
            algo_instance_id="mqalgo_a",
            event_id="mqrtevt_event_a",
            delivery_id="mqdelivery_a",
            projection_refs=(risk, contract),
        )
    with pytest.raises(ValueError, match="duplicate projection_type"):
        second_contract = ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.CONTRACT,
            projection_id="contract_b",
            projection_version="v1",
            payload_sha256=_sha("5"),
            source_event_id=None,
            logical_at_utc="2026-07-25T01:30:00Z",
        )
        ExecutionProjectionSetV1.create(
            runtime_id="runtime_k2",
            algo_instance_id="mqalgo_a",
            event_id="mqrtevt_event_a",
            delivery_id="mqdelivery_a",
            projection_refs=(contract, second_contract),
        )


def test_consumed_lineage_and_risk_receipt_have_strict_writer_readback_parity() -> None:
    lineage = ConsumedLineageRefV1.create(
        lineage_type="MARKET_DATA",
        identity="market_data_a",
        payload_sha256=_sha("4"),
    )
    assert ConsumedLineageRefV1.model_validate(lineage.model_dump(mode="python"), strict=True) == lineage

    receipt = MiniQMTRiskDecisionReceiptV1.create(
        runtime_id="runtime_k2",
        algo_instance_id="mqalgo_a",
        event_id="mqrtevt_event_a",
        child_order_id="mqchild_a",
        decision_stage="PRE_SUBMIT",
        action=RiskDecisionActionV1.PASS,
        reason_code="MINIQMT_RISK_PASS",
        reason="configured risk engine passed",
        metadata={"active_child_count": 0, "runtime_id": "runtime_k2"},
        logical_at_utc="2026-07-25T01:30:00Z",
    )
    dumped = receipt.model_dump(mode="json")
    assert MiniQMTRiskDecisionReceiptV1.model_validate_json(receipt.model_dump_json()) == receipt
    assert json.dumps(dumped, sort_keys=True)

    with pytest.raises(ValidationError, match="metadata_sha256"):
        MiniQMTRiskDecisionReceiptV1.model_validate(
            {**receipt.model_dump(mode="python"), "metadata_sha256": _sha("f")}, strict=True
        )


class _BrokenTextError(RuntimeError):
    def __str__(self) -> str:
        raise RuntimeError("renderer broke")


def test_kernel_error_evidence_keeps_primary_failure_json_safe_and_bounded() -> None:
    evidence = KernelErrorEvidenceV1.create(
        stage="DELIVERY_APPLY",
        stable_reason_code="MINIQMT_ALGO_TRANSITION_FAILED",
        exception=_BrokenTextError(),
        message="plugin transition failed",
        retryable=False,
        terminal=True,
        broker_called=None,
        primary_context={"runtime_id": "runtime_k2", "algo_instance_id": "mqalgo_a"},
        secondary_errors=[{"index": index, "message": f"secondary-{index}"} for index in range(20)],
    )

    dumped = evidence.model_dump(mode="json")
    assert dumped["exception_type"].endswith("._BrokenTextError")
    assert dumped["message"] == "plugin transition failed"
    assert len(dumped["bounded_secondary_errors"]) == 16
    assert dumped["bounded_secondary_errors"][0]["reason_code"] == "MINIQMT_KERNEL_EXCEPTION_RENDER_FAILED"
    assert dumped["bounded_secondary_errors"][-1]["reason_code"] == "MINIQMT_KERNEL_SECONDARY_ERRORS_TRUNCATED"
    assert json.dumps(dumped, sort_keys=True)
    assert KernelErrorEvidenceV1.model_validate_json(evidence.model_dump_json()) == evidence


@pytest.mark.parametrize("bad_identity", [None, "", {}, [], 1, True])
def test_k2_transaction_identity_rejects_malformed_identity_without_coercion(bad_identity: object) -> None:
    with pytest.raises((TypeError, ValueError, ValidationError)):
        transaction_commit_identity_v1(
            operation="WRITE_K2",
            owner_identities=(bad_identity,),  # type: ignore[arg-type]
            input_hashes=(_sha("1"),),
            output_identities=("output_k2",),
        )


def test_algo_persistence_enforces_exact_status_quantity_state_and_active_child_closure() -> None:
    config = {"slices": 4}
    state = {"next_slice": 1}
    algo = ExecutionAlgoInstancePersistenceV2.create(
        algo_instance_id=_algo_id(),
        runtime_id="runtime_k2",
        parent_intent_id="intent_k2",
        strategy_slot_id="slot_k2",
        symbol="600000.SH",
        side=SideV1.BUY,
        target_quantity=100,
        traded_quantity=0,
        remaining_quantity=100,
        algo_code="TWAP",
        plugin_id="aistock.twap",
        plugin_version="1.0.0",
        plugin_manifest_sha256=_sha("1"),
        plugin_config_json=config,
        plugin_config_sha256=hash_hex_v1("miniqmt_plugin_config_v2", config),
        compatibility_receipt_sha256=_sha("2"),
        state_schema_version="twap_state_v1",
        state_json=state,
        state_sha256=hash_hex_v1("execution_algo_state_v2", state),
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id="delivery_k2",
        last_closed_delivery_sequence=1,
        terminal_delivery_sequence=None,
        status=ExecutionAlgoPersistenceStatusV2.ACTIVE,
        failure_receipt_id=None,
        active_child_closure_status=ActiveChildClosureStatusV1.NOT_APPLICABLE,
        active_child_count=0,
        row_version=1,
        created_at_utc="2026-07-25T01:30:00Z",
        updated_at_utc="2026-07-25T01:30:00Z",
        terminal_at_utc=None,
        archived_at_utc=None,
    )

    assert algo.traded_quantity + algo.remaining_quantity == algo.target_quantity
    with pytest.raises(TypeError):
        algo.plugin_config_json["slices"] = 5
    for field_name, bad_value in (
        ("status", "REJECTED"),
        ("status", "FAILED_WITH_ACTIVE_CHILD"),
        ("target_quantity", True),
        ("updated_at_utc", "2026-07-25T01:30:00"),
        ("state_sha256", _sha("A")),
    ):
        payload = algo.model_dump(mode="python")
        payload[field_name] = bad_value
        with pytest.raises(ValidationError):
            ExecutionAlgoInstancePersistenceV2.model_validate(payload)

    failed_payload = algo.model_dump(mode="python")
    failed_payload.update(
        status=ExecutionAlgoPersistenceStatusV2.FAILED,
        failure_receipt_id="failure_k2",
        active_child_closure_status=ActiveChildClosureStatusV1.CANCEL_PENDING,
        active_child_count=0,
        terminal_delivery_sequence=1,
        terminal_at_utc="2026-07-25T01:31:00Z",
    )
    with pytest.raises(ValidationError, match="requires active children"):
        ExecutionAlgoInstancePersistenceV2.model_validate(failed_payload)

    closed_failure_payload = algo.model_dump(mode="python")
    closed_failure_payload.update(
        status=ExecutionAlgoPersistenceStatusV2.FAILED,
        failure_receipt_id="failure_k2",
        active_child_closure_status=ActiveChildClosureStatusV1.CANCEL_PENDING,
        active_child_count=1,
        transition_sequence=2,
        last_closed_delivery_sequence=2,
        terminal_delivery_sequence=2,
        terminal_at_utc="2026-07-25T01:31:00Z",
    )
    assert ExecutionAlgoInstancePersistenceV2.model_validate(closed_failure_payload).transition_sequence == 2

    initialization_failure_payload = algo.model_dump(mode="python")
    initialization_failure_payload.update(
        state_schema_version=None,
        state_json=None,
        state_sha256=None,
        transition_sequence=0,
        last_applied_delivery_sequence=0,
        last_applied_delivery_id=None,
        last_closed_delivery_sequence=1,
        status=ExecutionAlgoPersistenceStatusV2.FAILED,
        failure_receipt_id="initialization_failure_k2",
        active_child_closure_status=ActiveChildClosureStatusV1.CLEAN,
        active_child_count=0,
        terminal_delivery_sequence=1,
        terminal_at_utc="2026-07-25T01:31:00Z",
    )
    initialization_failure = ExecutionAlgoInstancePersistenceV2.model_validate(initialization_failure_payload)
    assert initialization_failure.state_json is None
    assert initialization_failure.transition_sequence == 0


def test_transition_receipt_writer_and_json_readback_share_nonrecursive_set_authority() -> None:
    receipt = AlgoTransitionReceiptV1.create(
        delivery_id="delivery_transition_k2",
        event_id="event_transition_k2",
        runtime_id="runtime_k2",
        algo_instance_id=_algo_id(),
        plugin_id="aistock.twap",
        plugin_version="1.0.0",
        plugin_manifest_sha256=_sha("1"),
        transition_sequence=1,
        before_state_sha256_or_INIT="INIT",
        after_state_sha256=_sha("2"),
        ordered_command_ids=("command_transition_k2",),
        ordered_timer_mutation_ids=(),
        ordered_diagnostic_observation_ids=(),
        ordered_consumed_lineage_refs=(),
        execution_projection_set_sha256=_sha("3"),
        effect_set_sha256=_sha("4"),
        terminal_outcome=None,
        logical_applied_at_utc="2026-07-25T01:30:00Z",
        transaction_commit_identity="mqtx_transition_k2",
    )

    assert AlgoTransitionReceiptV1.model_validate_json(receipt.model_dump_json()) == receipt
    drift = receipt.model_dump(mode="python")
    drift["command_set_sha256"] = _sha("f")
    drift["receipt_sha256"] = hash_hex_v1(
        "miniqmt_algo_transition_receipt_v1",
        {key: value for key, value in receipt.model_dump(mode="json").items() if key != "receipt_sha256"}
        | {"command_set_sha256": _sha("f")},
    )
    with pytest.raises(ValidationError, match="set hashes"):
        AlgoTransitionReceiptV1.model_validate(drift)


def test_mapping_and_outbox_close_deterministic_identity_versions_and_nullable_broker_called() -> None:
    reserved = _mapping(status=CommandChildMappingStatusV1.RESERVED, version=1)
    dispatching = _mapping(status=CommandChildMappingStatusV1.DISPATCHING, version=2)
    assert dispatching.validate_successor_v1(reserved) == dispatching
    assert reserved.order_remark == reserved.deterministic_client_order_ref

    drift = dispatching.model_dump(mode="python")
    drift["mapping_version"] = 3
    drift_json = dispatching.model_dump(mode="json")
    drift_json["mapping_version"] = 3
    drift["mapping_receipt_sha256"] = hash_hex_v1(
        "miniqmt_command_child_mapping_receipt_v1",
        {key: value for key, value in drift_json.items() if key != "mapping_receipt_sha256"},
    )
    with pytest.raises(ValueError, match="increment exactly once"):
        ExecutionCommandChildMappingV1.model_validate(drift).validate_successor_v1(reserved)

    outbox = _pending_outbox()
    assert outbox.model_dump(mode="json")["payload_json"]["command_id"] == outbox.command_id
    malformed = outbox.model_dump(mode="python")
    malformed["broker_called"] = False
    malformed_json = outbox.model_dump(mode="json")
    malformed_json["broker_called"] = False
    malformed["outbox_row_sha256"] = hash_hex_v1(
        "miniqmt_broker_command_outbox_row_v1",
        {key: value for key, value in malformed_json.items() if key != "outbox_row_sha256"},
    )
    with pytest.raises(ValidationError, match="pre-dispatch"):
        BrokerCommandOutboxV1.model_validate(malformed)


def test_ack_unknown_and_reconciliation_receipts_reject_fake_success_and_ambiguous_identity() -> None:
    with pytest.raises(ValidationError, match="accepted ACK"):
        BrokerCommandAckReceiptV1.create(
            command_id="command_k2",
            mapping_id="mapping_k2",
            deterministic_client_order_ref="client_ref_k2",
            gateway_route_id="route_k2",
            gateway_catalog_sha256=_sha("1"),
            source="SYNCHRONOUS_RETURN",
            accepted=True,
            broker_order_id=None,
            reason_code="BROKER_ACCEPTED",
            ack_payload_sha256=_sha("2"),
            observed_at_utc="2026-07-25T01:30:00Z",
        )
    unknown = BrokerUnknownOutcomeReceiptV1.create(
        command_id="command_k2",
        dispatch_attempt_id="dispatch_k2",
        mapping_id="mapping_k2",
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="OUTBOX_COMMAND",
            owner_id="command_k2",
            lease_epoch=1,
            lease_owner="worker_k2:incarnation_k2",
        ),
        uncertain_stage="GATEWAY_RETURN",
        callback_watermark="watermark_k2",
        reason_code="MINIQMT_COMMAND_OUTCOME_UNKNOWN",
        observed_at_utc="2026-07-25T01:31:00Z",
    )
    assert unknown.broker_called is None
    with pytest.raises(ValidationError, match="one broker order"):
        BrokerOutcomeReconciliationReceiptV1.create(
            command_id="command_k2",
            reconcile_attempt=1,
            query_criteria_sha256=_sha("1"),
            callback_watermark="watermark_k2",
            ordered_matched_order_ids=("broker_1", "broker_2"),
            ordered_matched_trade_ids=(),
            order_snapshot_sha256=_sha("2"),
            trade_snapshot_sha256=_sha("3"),
            outcome="UNIQUE_ACCEPTED",
            broker_called=True,
            broker_order_id="broker_1",
            reason_code="UNIQUE_MATCH",
            observed_at_utc="2026-07-25T01:32:00Z",
        )


@pytest.mark.parametrize("carrier_kind", ["delivery", "outbox", "timer_schedule", "timer_occurrence"])
def test_k2_lease_carriers_reject_arbitrary_fence_tokens(carrier_kind: str) -> None:
    owner = "worker_k2:incarnation_k2"
    arbitrary = "arbitrary_not_mqfence"
    if carrier_kind == "delivery":
        delivery = AlgoEventDeliveryV1.create(
            event=_tick_event(),
            algo_instance_id=_algo_id(),
            plugin_manifest_sha256=_sha("1"),
            algo_delivery_sequence=1,
            previous_delivery_id=None,
            status=DeliveryStatusV1.CLAIMED,
            attempt_count=1,
            lease_owner=owner,
            lease_expires_at="2026-07-25T01:40:00Z",
            transition_id=None,
            last_error_json=None,
            created_at_utc="2026-07-25T01:30:00Z",
            updated_at_utc="2026-07-25T01:31:00Z",
        )
        with pytest.raises(ValidationError, match="exact kernel fence authority"):
            AlgoDeliveryPersistenceV1.create(
                delivery=delivery,
                lease_epoch=1,
                lease_fence_token=arbitrary,
                row_version=2,
                next_attempt_at_utc=None,
                failure_receipt_id=None,
                skip_receipt_id=None,
                closed_at_utc=None,
            )
        return

    mutation = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=_algo_id(),
        transition_id="transition_fence_k2",
        ordinal=0,
        timer_name="fence_timer",
        schedule_epoch="session_epoch_fence_k2",
        due_at_exchange_utc="2026-07-25T02:00:00Z",
        catch_up_policy="EXPIRE_IF_LATE",
        payload={"slice": 1},
    )
    schedule = ExecutionAlgoTimerScheduleV1.create(
        runtime_id="runtime_k2",
        mutation=mutation,
        status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
        emitted_event_id=None,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at_utc=None,
        row_version=1,
        created_at_utc="2026-07-25T01:30:00Z",
        updated_at_utc="2026-07-25T01:30:00Z",
        closed_at_utc=None,
    )
    if carrier_kind == "timer_schedule":
        with pytest.raises(ValidationError, match="exact kernel fence authority"):
            ExecutionAlgoTimerScheduleV1.create(
                runtime_id="runtime_k2",
                mutation=mutation,
                status=ExecutionAlgoTimerScheduleStatusV1.EMITTING,
                emitted_event_id=None,
                lease_owner=owner,
                lease_epoch=1,
                lease_fence_token=arbitrary,
                lease_expires_at_utc="2026-07-25T02:01:00Z",
                row_version=2,
                created_at_utc="2026-07-25T01:30:00Z",
                updated_at_utc="2026-07-25T01:59:00Z",
                closed_at_utc=None,
            )
        return
    if carrier_kind == "timer_occurrence":
        with pytest.raises(ValidationError, match="exact kernel fence authority"):
            ExecutionAlgoTimerOccurrenceV1.create(
                schedule=schedule,
                exchange_session_authority_sha256=_sha("4"),
                status=ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED,
                emitted_event_id=None,
                catch_up_receipt_sha256=None,
                lease_owner=owner,
                lease_epoch=1,
                lease_fence_token=arbitrary,
                lease_expires_at_utc="2026-07-25T02:01:00Z",
                row_version=1,
                created_at_utc="2026-07-25T01:59:00Z",
                closed_at_utc=None,
            )
        return

    command = _submit_command()
    mapping = _mapping(status=CommandChildMappingStatusV1.RESERVED, version=1)
    with pytest.raises(ValidationError, match="exact kernel fence authority"):
        BrokerCommandOutboxV1.create(
            command=command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.CLAIMED,
            attempt_count=1,
            lease_owner=owner,
            lease_epoch=1,
            lease_fence_token=arbitrary,
            lease_expires_at="2026-07-25T01:40:00Z",
            dispatch_attempt_id=None,
            next_attempt_at_utc=None,
            broker_called=None,
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=2,
            created_at_utc="2026-07-25T01:30:00Z",
            updated_at_utc="2026-07-25T01:31:00Z",
            closed_at_utc=None,
        )


def test_delivery_fence_rejects_wrong_owner_type_id_epoch_or_owner() -> None:
    delivery = AlgoEventDeliveryV1.create(
        event=_tick_event(),
        algo_instance_id=_algo_id(),
        plugin_manifest_sha256=_sha("1"),
        algo_delivery_sequence=1,
        previous_delivery_id=None,
        status=DeliveryStatusV1.CLAIMED,
        attempt_count=1,
        lease_owner="worker_k2:incarnation_k2",
        lease_expires_at="2026-07-25T01:40:00Z",
        transition_id=None,
        last_error_json=None,
        created_at_utc="2026-07-25T01:30:00Z",
        updated_at_utc="2026-07-25T01:31:00Z",
    )
    exact = AlgoDeliveryPersistenceV1.create(
        delivery=delivery,
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=delivery.delivery_id,
            lease_epoch=1,
            lease_owner="worker_k2:incarnation_k2",
        ),
        row_version=2,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        closed_at_utc=None,
    )
    bad_tokens = (
        kernel_lease_fence_token_v1(
            owner_type="OUTBOX_COMMAND",
            owner_id=delivery.delivery_id,
            lease_epoch=1,
            lease_owner="worker_k2:incarnation_k2",
        ),
        kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id="mqdelivery_wrong_owner_id",
            lease_epoch=1,
            lease_owner="worker_k2:incarnation_k2",
        ),
        kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=delivery.delivery_id,
            lease_epoch=2,
            lease_owner="worker_k2:incarnation_k2",
        ),
        kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=delivery.delivery_id,
            lease_epoch=1,
            lease_owner="worker_k2:other_incarnation",
        ),
    )
    for token in bad_tokens:
        payload = exact.model_dump(mode="python")
        payload["lease_fence_token"] = token
        with pytest.raises(ValidationError, match="exact kernel fence authority"):
            AlgoDeliveryPersistenceV1.model_validate(payload)


def test_timer_and_exchange_session_authority_reject_identity_and_calendar_drift() -> None:
    mutation = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=_algo_id(),
        transition_id="transition_k2",
        ordinal=0,
        timer_name="slice_timer",
        schedule_epoch="session_epoch_k2",
        due_at_exchange_utc="2026-07-25T02:00:00Z",
        catch_up_policy="EXPIRE_IF_LATE",
        payload={"slice": 2},
    )
    schedule = ExecutionAlgoTimerScheduleV1.create(
        runtime_id="runtime_k2",
        mutation=mutation,
        status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
        emitted_event_id=None,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at_utc=None,
        row_version=1,
        created_at_utc="2026-07-25T01:30:00Z",
        updated_at_utc="2026-07-25T01:30:00Z",
        closed_at_utc=None,
    )
    occurrence = ExecutionAlgoTimerOccurrenceV1.create(
        schedule=schedule,
        exchange_session_authority_sha256=_sha("4"),
        status=ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED,
        emitted_event_id=None,
        catch_up_receipt_sha256=None,
        lease_owner="worker_k2:incarnation_k2",
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="TIMER_OCCURRENCE",
            owner_id=schedule.timer_occurrence_id,
            lease_epoch=1,
            lease_owner="worker_k2:incarnation_k2",
        ),
        lease_expires_at_utc="2026-07-25T02:01:00Z",
        row_version=1,
        created_at_utc="2026-07-25T01:59:00Z",
        closed_at_utc=None,
    )
    assert occurrence.timer_occurrence_id == schedule.timer_occurrence_id

    authority = ExchangeSessionAuthorityV1.create(
        runtime_id="runtime_k2",
        exchange_trade_date="2026-07-25",
        **_calendar_authority_values(),
    )
    drift = authority.model_dump(mode="python")
    drift["calendar_snapshot_set_sha256"] = _sha("9")
    drift_json = authority.model_dump(mode="json")
    drift_json["calendar_snapshot_set_sha256"] = _sha("9")
    drift["authority_sha256"] = hash_hex_v1(
        "miniqmt_exchange_session_authority_v1",
        {key: value for key, value in drift_json.items() if key != "authority_sha256"},
    )
    with pytest.raises(ValidationError, match="hash conflicts"):
        ExchangeSessionAuthorityV1.model_validate(drift)

    with pytest.raises(ValidationError, match="exact shared CalendarSnapshotSet payload"):
        ExchangeSessionAuthorityV1.create(
            runtime_id="runtime_k2",
            exchange_trade_date="2026-07-25",
            calendar_snapshot_set_id="calendar_set_k2",
            calendar_snapshot_set_json={
                "snapshot_set_id": "calendar_set_k2",
                "set_sha256": _sha("5"),
                "timezone": "Asia/Shanghai",
            },
            calendar_snapshot_set_sha256=_sha("5"),
            ordered_market_calendar_sha256s=(_sha("6"), _sha("7"), _sha("8")),
            ordered_session_segments=({"start_local": "09:30:00", "end_local": "11:30:00"},),
            source_effective_at_utc="2026-07-24T16:00:00Z",
        )
