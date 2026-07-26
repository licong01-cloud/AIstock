from __future__ import annotations

from dataclasses import replace

import pytest

import backend.services.miniqmt_execution_runtime.kernel_ingress as kernel_ingress
from backend.execution_algos.vnpy_compat.receipts import build_current_three_compatibility_receipts_v1
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v1,
    current_three_descriptors_v2,
    current_three_process_bindings_v2,
)
from backend.services.miniqmt_execution_runtime.kernel_ingress import (
    KernelEventRoutingError,
    KernelIngressCoordinatorV1,
    route_event_targets_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    ActiveChildClosureStatusV1,
    DeliveryStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    RuntimeEventEnvelopeV2,
    SideV1,
    _algo_instance_id_v2,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import build_plugin_catalog_v2


def _catalog():
    return build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v2(),
        creation_bindings=current_three_creation_bindings_v1(),
        process_bindings=current_three_process_bindings_v2(),
        pinned_compatibility_receipts=build_current_three_compatibility_receipts_v1(),
    )


def _algo(*, slot: str, status: ExecutionAlgoPersistenceStatusV2, symbol: str = "600000.SH"):
    descriptor = next(
        item for item in _catalog().snapshot.registration_descriptors if item.manifest.algo_code == "SNIPER_MINIQMT"
    )
    config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    config_hash = hash_hex_v1("miniqmt_plugin_config_v2", config)
    algo_id = _algo_instance_id_v2(
        runtime_id="runtime_k2b",
        parent_intent_id=f"intent_{slot}",
        strategy_slot_id=slot,
        algo_code=descriptor.manifest.algo_code,
        plugin_id=descriptor.manifest.plugin_id,
        plugin_version=descriptor.manifest.plugin_version,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        plugin_config_sha256=config_hash,
    )
    state = {"status": "RUNNING", "slot": slot}
    terminal = status in {
        ExecutionAlgoPersistenceStatusV2.COMPLETED,
        ExecutionAlgoPersistenceStatusV2.CANCELLED,
        ExecutionAlgoPersistenceStatusV2.FAILED,
        ExecutionAlgoPersistenceStatusV2.EXPIRED_WITH_RESIDUAL,
    }
    failure_id = "failure_k2b" if status is ExecutionAlgoPersistenceStatusV2.FAILED else None
    closure = ActiveChildClosureStatusV1.CLEAN if terminal else ActiveChildClosureStatusV1.NOT_APPLICABLE
    return ExecutionAlgoInstancePersistenceV2.create(
        algo_instance_id=algo_id,
        runtime_id="runtime_k2b",
        parent_intent_id=f"intent_{slot}",
        strategy_slot_id=slot,
        symbol=symbol,
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
        state_schema_version="sniper_state_v2",
        state_json=state,
        state_sha256=hash_hex_v1("execution_algo_state_v2", state),
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id=f"delivery_{slot}",
        last_closed_delivery_sequence=1,
        terminal_delivery_sequence=1 if terminal else None,
        status=status,
        failure_receipt_id=failure_id,
        active_child_closure_status=closure,
        active_child_count=0,
        row_version=1,
        created_at_utc="2026-07-26T01:20:00Z",
        updated_at_utc="2026-07-26T01:20:00Z",
        terminal_at_utc="2026-07-26T01:20:00Z" if terminal else None,
        archived_at_utc=None,
    )


def _event(event_type: EventTypeV2) -> RuntimeEventEnvelopeV2:
    if event_type is EventTypeV2.TICK:
        values = dict(
            source=EventSourceV2.B0_QUOTE_V2,
            symbol="600000.SH",
            payload_schema_version="miniqmt_market_data_view_v2",
            source_identity={"market_data_id": "market_k2b"},
        )
    elif event_type is EventTypeV2.ACCOUNT:
        values = dict(
            source=EventSourceV2.QMT_OMS_PROJECTION,
            symbol=None,
            payload_schema_version="miniqmt_account_projection_v1",
            source_identity={"projection_version": "account_v1", "projection_sha256": "b" * 64},
        )
    elif event_type is EventTypeV2.EOD:
        values = dict(
            source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
            symbol=None,
            payload_schema_version="miniqmt_eod_event_v1",
            source_identity={
                "runtime_id": "runtime_k2b",
                "trade_date": "2026-07-26",
                "session_epoch": "session_k2b",
            },
        )
    else:
        values = dict(
            source=EventSourceV2.QMT_GATEWAY_CALLBACK,
            symbol="600000.SH",
            payload_schema_version="miniqmt_order_event_v1",
            source_identity={"order_event_id": "order_event_k2b"},
        )
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k2b",
        sequence=2,
        event_type=event_type,
        event_time_utc="2026-07-26T01:30:00Z",
        monotonic_ns=None,
        payload={"kind": event_type.value},
        correlation={},
        **values,
    )


def _owner_event(event_type: EventTypeV2, *, algo=None) -> RuntimeEventEnvelopeV2:
    if event_type is EventTypeV2.ALGO_START:
        if algo is None:
            raise ValueError("ALGO_START test event requires an algo")
        values = (
            EventSourceV2.MINIQMT_EXECUTION_KERNEL,
            "miniqmt_algo_start_v1",
            {
                "algo_instance_id": algo.algo_instance_id,
                "runtime_id": algo.runtime_id,
                "parent_intent_id": algo.parent_intent_id,
                "strategy_slot_id": algo.strategy_slot_id,
                "algo_code": algo.algo_code,
                "plugin_id": algo.plugin_id,
                "plugin_version": algo.plugin_version,
                "plugin_manifest_sha256": algo.plugin_manifest_sha256,
                "plugin_config_sha256": algo.plugin_config_sha256,
            },
            algo.symbol,
            None,
        )
    else:
        matrix = {
            EventTypeV2.TIMER: (
                EventSourceV2.EXCHANGE_SESSION_CLOCK,
                "miniqmt_timer_due_v1",
                {"timer_occurrence_id": "occ_k2b"},
                None,
                10,
            ),
            EventTypeV2.SESSION: (
                EventSourceV2.EXCHANGE_SESSION_CLOCK,
                "miniqmt_session_event_v1",
                {"session_event_id": "session_event_k2b"},
                None,
                None,
            ),
            EventTypeV2.TRADE: (
                EventSourceV2.QMT_GATEWAY_CALLBACK,
                "miniqmt_trade_fact_v1",
                {"trade_id": "trade_k2b"},
                "600000.SH",
                None,
            ),
            EventTypeV2.RECONCILE: (
                EventSourceV2.QMT_OMS_RECONCILIATION,
                "miniqmt_reconciliation_receipt_v1",
                {"receipt_id": "reconcile_k2b", "receipt_sha256": "d" * 64},
                None,
                None,
            ),
            EventTypeV2.OPERATOR: (
                EventSourceV2.SIMULATION_RUNTIME_OPERATOR,
                "miniqmt_operator_command_v1",
                {"operator_command_id": "operator_k2b"},
                None,
                None,
            ),
        }
        values = matrix[event_type]
    source, schema, identity, symbol, monotonic_ns = values
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k2b",
        sequence=2,
        event_type=event_type,
        event_time_utc="2026-07-26T01:30:00Z",
        monotonic_ns=monotonic_ns,
        source=source,
        symbol=symbol,
        payload_schema_version=schema,
        payload={"kind": event_type.value},
        source_identity=identity,
        correlation={},
    )


def _tail(algo, *, event=None) -> AlgoDeliveryPersistenceV1:
    predecessor_event = event or _event(EventTypeV2.TICK)
    delivery = AlgoEventDeliveryV1.create(
        event=predecessor_event,
        algo_instance_id=algo.algo_instance_id,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        algo_delivery_sequence=1,
        previous_delivery_id=None,
        status=DeliveryStatusV1.APPLIED,
        attempt_count=1,
        lease_owner=None,
        lease_expires_at=None,
        transition_id="transition_tail_k2b",
        last_error_json=None,
        created_at_utc="2026-07-26T01:20:00Z",
        updated_at_utc="2026-07-26T01:20:00Z",
    )
    return AlgoDeliveryPersistenceV1.create(
        delivery=delivery,
        lease_epoch=1,
        lease_fence_token=None,
        row_version=3,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        closed_at_utc="2026-07-26T01:20:00Z",
    )


class _IngressRepository:
    def __init__(self, algo, *, missing_tail: bool = False) -> None:
        self.algo = algo
        self.missing_tail = missing_tail
        self.received = None

    def ingest_routed_event_atomic(
        self, *, event, catalog_runtime, correlated_algo_instance_ids, callback_mapping_update=None
    ):
        targets = route_event_targets_v1(
            event=event,
            algo_instances=(self.algo,),
            catalog_runtime=catalog_runtime,
            correlated_algo_instance_ids=correlated_algo_instance_ids,
        )
        if self.missing_tail:
            raise KernelEventRoutingError(
                "MINIQMT_RUNTIME_EVENT_ROUTING_PREDECESSOR_MISSING",
                "repository-owned target has no durable predecessor",
                context={"algo_instance_id": self.algo.algo_instance_id},
            )
        predecessor = _tail(self.algo)
        self.received = {
            "event": event,
            "targets": targets,
            "deliveries": tuple(
                AlgoDeliveryPersistenceV1.create(
                    delivery=AlgoEventDeliveryV1.create(
                        event=event,
                        algo_instance_id=algo_instance_id,
                        plugin_manifest_sha256=self.algo.plugin_manifest_sha256,
                        algo_delivery_sequence=predecessor.algo_delivery_sequence + 1,
                        previous_delivery_id=predecessor.delivery_id,
                        status=DeliveryStatusV1.PENDING,
                        attempt_count=0,
                        lease_owner=None,
                        lease_expires_at=None,
                        transition_id=None,
                        last_error_json=None,
                        created_at_utc=event.event_time_utc,
                        updated_at_utc=event.event_time_utc,
                    ),
                    lease_epoch=0,
                    lease_fence_token=None,
                    row_version=1,
                    next_attempt_at_utc=None,
                    failure_receipt_id=None,
                    skip_receipt_id=None,
                    closed_at_utc=None,
                )
                for algo_instance_id in targets
            ),
            "callback_mapping_update": callback_mapping_update,
        }
        return self.received


class _AuthoritativeIngressRepository:
    def __init__(self, algos) -> None:
        self.algos = tuple(algos)
        self.received = None

    def ingest_routed_event_atomic(
        self, *, event, catalog_runtime, correlated_algo_instance_ids, callback_mapping_update=None
    ):
        targets = route_event_targets_v1(
            event=event,
            algo_instances=self.algos,
            catalog_runtime=catalog_runtime,
            correlated_algo_instance_ids=correlated_algo_instance_ids,
        )
        self.received = {
            "event": event,
            "targets": targets,
            "callback_mapping_update": callback_mapping_update,
        }
        return self.received


def test_tick_routes_only_same_symbol_active_subscribers_in_stable_order() -> None:
    active_b = _algo(slot="slot_b", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    active_a = _algo(slot="slot_a", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    paused = _algo(slot="slot_c", status=ExecutionAlgoPersistenceStatusV2.PAUSED)
    other_symbol = _algo(slot="slot_d", status=ExecutionAlgoPersistenceStatusV2.ACTIVE, symbol="000001.SZ")

    targets = route_event_targets_v1(
        event=_event(EventTypeV2.TICK),
        algo_instances=(paused, active_b, other_symbol, active_a),
        catalog_runtime=_catalog(),
        correlated_algo_instance_ids=(),
    )

    assert targets == tuple(sorted((active_a.algo_instance_id, active_b.algo_instance_id)))


def test_ingress_coordinator_routes_from_repository_owned_complete_algo_set() -> None:
    active = _algo(slot="slot_repository_authority", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    repository = _AuthoritativeIngressRepository((active,))

    result = KernelIngressCoordinatorV1(repository=repository, catalog_runtime=_catalog()).ingest(
        event=_event(EventTypeV2.TICK),
    )

    assert result["targets"] == (active.algo_instance_id,)
    assert repository.received == result


def test_owner_scoped_coordinator_requires_and_routes_exact_durable_algo_identity() -> None:
    active = _algo(slot="slot_owner_coordinator", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    repository = _AuthoritativeIngressRepository((active,))
    coordinator = KernelIngressCoordinatorV1(repository=repository, catalog_runtime=_catalog())
    missing_owner = _owner_event(EventTypeV2.TIMER, algo=active)
    with pytest.raises(KernelEventRoutingError) as raised:
        coordinator.ingest(event=missing_owner)
    assert raised.value.reason_code == "MINIQMT_RUNTIME_EVENT_ROUTING_OWNER_MISSING"

    owned = RuntimeEventEnvelopeV2.create(
        runtime_id=missing_owner.runtime_id,
        sequence=missing_owner.sequence,
        event_type=missing_owner.event_type,
        event_time_utc=missing_owner.event_time_utc,
        monotonic_ns=missing_owner.monotonic_ns,
        source=missing_owner.source,
        symbol=missing_owner.symbol,
        payload_schema_version=missing_owner.payload_schema_version,
        payload=thaw_json_v1(missing_owner.payload),
        source_identity=thaw_json_v1(missing_owner.source_identity),
        correlation={"algo_instance_id": active.algo_instance_id},
    )
    result = coordinator.ingest(event=owned)
    assert result["targets"] == ()
    assert repository.received == result


def test_account_subscription_and_eod_status_semantics_are_exact() -> None:
    active = _algo(slot="slot_active", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    paused = _algo(slot="slot_paused", status=ExecutionAlgoPersistenceStatusV2.PAUSED)
    completed = _algo(slot="slot_done", status=ExecutionAlgoPersistenceStatusV2.COMPLETED)
    algos = (completed, paused, active)

    assert (
        route_event_targets_v1(
            event=_event(EventTypeV2.ACCOUNT),
            algo_instances=algos,
            catalog_runtime=_catalog(),
            correlated_algo_instance_ids=(),
        )
        == ()
    )
    assert route_event_targets_v1(
        event=_event(EventTypeV2.EOD),
        algo_instances=algos,
        catalog_runtime=_catalog(),
        correlated_algo_instance_ids=(),
    ) == tuple(sorted((active.algo_instance_id, paused.algo_instance_id)))


def test_callback_requires_exact_correlated_owner_and_does_not_guess() -> None:
    active = _algo(slot="slot_active", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    event = _event(EventTypeV2.ORDER)

    try:
        route_event_targets_v1(
            event=event,
            algo_instances=(active,),
            catalog_runtime=_catalog(),
            correlated_algo_instance_ids=(),
        )
    except KernelEventRoutingError as exc:
        assert exc.reason_code == "MINIQMT_RUNTIME_EVENT_ROUTING_OWNER_MISSING"
    else:  # pragma: no cover - explicit fail-loud contract
        raise AssertionError("ORDER routing accepted a missing durable owner")


def test_failed_callback_owner_is_zero_target_without_reinvoking_terminal_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = _algo(slot="slot_failed", status=ExecutionAlgoPersistenceStatusV2.FAILED)
    monkeypatch.setattr(
        kernel_ingress,
        "_strict_catalog",
        lambda _runtime: pytest.fail("terminal callback routing must not validate an unused plugin catalog"),
    )
    monkeypatch.setattr(
        kernel_ingress,
        "_manifest_for_algo",
        lambda **_values: pytest.fail("terminal callback routing must not revalidate a plugin target"),
    )
    assert (
        route_event_targets_v1(
            event=_event(EventTypeV2.ORDER),
            algo_instances=(failed,),
            catalog_runtime=_catalog(),
            correlated_algo_instance_ids=(failed.algo_instance_id,),
        )
        == ()
    )


def test_callback_coordinator_rejects_event_only_ingress_without_atomic_mapping_update() -> None:
    coordinator = KernelIngressCoordinatorV1(repository=object(), catalog_runtime=_catalog())

    try:
        coordinator.ingest(
            event=_event(EventTypeV2.ORDER),
        )
    except KernelEventRoutingError as exc:
        assert exc.reason_code == "MINIQMT_RUNTIME_EVENT_CALLBACK_MAPPING_UPDATE_MISSING"
    else:  # pragma: no cover - explicit fail-loud contract
        raise AssertionError("callback event was accepted without its atomic mapping successor")


@pytest.mark.parametrize(
    ("event_type", "status", "expected"),
    [
        (EventTypeV2.ALGO_START, ExecutionAlgoPersistenceStatusV2.ACTIVE, True),
        (EventTypeV2.TIMER, ExecutionAlgoPersistenceStatusV2.ACTIVE, True),
        (EventTypeV2.TIMER, ExecutionAlgoPersistenceStatusV2.PAUSED, False),
        (EventTypeV2.TRADE, ExecutionAlgoPersistenceStatusV2.PAUSED, True),
        (EventTypeV2.RECONCILE, ExecutionAlgoPersistenceStatusV2.ACTIVE, True),
        (EventTypeV2.OPERATOR, ExecutionAlgoPersistenceStatusV2.PAUSED, True),
        (EventTypeV2.SESSION, ExecutionAlgoPersistenceStatusV2.ACTIVE, True),
    ],
)
def test_all_owner_routing_rules_use_exact_status_and_manifest_subscription(event_type, status, expected) -> None:
    algo = _algo(slot=f"slot_{event_type.value}_{status.value}", status=status)
    event = _owner_event(event_type, algo=algo)
    correlated = () if event_type in {EventTypeV2.ALGO_START, EventTypeV2.SESSION} else (algo.algo_instance_id,)
    targets = route_event_targets_v1(
        event=event,
        algo_instances=(algo,),
        catalog_runtime=_catalog(),
        correlated_algo_instance_ids=correlated,
    )
    subscribed = event_type in next(
        item.manifest.subscribed_event_types
        for item in _catalog().snapshot.registration_descriptors
        if item.manifest.manifest_sha256 == algo.plugin_manifest_sha256
    )
    assert targets == ((algo.algo_instance_id,) if expected and subscribed else ())


@pytest.mark.parametrize(
    ("mutator", "reason_code"),
    [
        (
            lambda event, algos, correlated: (event, algos + algos, correlated),
            "MINIQMT_RUNTIME_EVENT_ROUTING_DUPLICATE_OWNER",
        ),
        (
            lambda event, algos, correlated: (
                event,
                (algos[0].model_copy(update={"runtime_id": "runtime_other"}),),
                correlated,
            ),
            "MINIQMT_RUNTIME_EVENT_ROUTING_RUNTIME_CONFLICT",
        ),
        (
            lambda event, algos, correlated: (event, algos, correlated + correlated),
            "MINIQMT_RUNTIME_EVENT_ROUTING_DUPLICATE_CORRELATION",
        ),
        (
            lambda event, algos, correlated: (event, algos, ("algo_unknown_k2b",)),
            "MINIQMT_RUNTIME_EVENT_ROUTING_OWNER_UNKNOWN",
        ),
    ],
)
def test_routing_rejects_duplicate_cross_runtime_and_unknown_ownership(mutator, reason_code) -> None:
    algo = _algo(slot="slot_owner_validation", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    event, algos, correlated = mutator(
        _owner_event(EventTypeV2.TIMER),
        (algo,),
        (algo.algo_instance_id,),
    )
    with pytest.raises(KernelEventRoutingError) as raised:
        route_event_targets_v1(
            event=event,
            algo_instances=algos,
            catalog_runtime=_catalog(),
            correlated_algo_instance_ids=correlated,
        )
    assert raised.value.reason_code == reason_code


def test_routing_rejects_unexpected_or_malformed_correlation_and_catalog_authority() -> None:
    algo = _algo(slot="slot_input_validation", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    with pytest.raises(KernelEventRoutingError) as raised:
        route_event_targets_v1(
            event=_event(EventTypeV2.TICK),
            algo_instances=(algo,),
            catalog_runtime=_catalog(),
            correlated_algo_instance_ids=(algo.algo_instance_id,),
        )
    assert raised.value.reason_code == "MINIQMT_RUNTIME_EVENT_ROUTING_UNEXPECTED_CORRELATION"
    with pytest.raises(TypeError, match="non-empty strings"):
        route_event_targets_v1(
            event=_owner_event(EventTypeV2.TIMER),
            algo_instances=(algo,),
            catalog_runtime=_catalog(),
            correlated_algo_instance_ids=("",),
        )
    runtime = _catalog()
    bad_runtime = replace(runtime, snapshot=runtime.snapshot.model_copy(update={"catalog_sha256": "0" * 64}))
    with pytest.raises(KernelEventRoutingError) as raised:
        route_event_targets_v1(
            event=_event(EventTypeV2.TICK),
            algo_instances=(algo,),
            catalog_runtime=bad_runtime,
            correlated_algo_instance_ids=(),
        )
    assert raised.value.reason_code == "MINIQMT_RUNTIME_EVENT_ROUTING_CATALOG_INVALID"


def test_routing_rejects_missing_or_conflicting_plugin_descriptor() -> None:
    algo = _algo(slot="slot_plugin_validation", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    runtime = _catalog()
    missing_snapshot = runtime.snapshot.model_copy(
        update={"registration_descriptors": (), "catalog_sha256": runtime.snapshot.catalog_sha256}
    )
    with pytest.raises(KernelEventRoutingError) as raised:
        route_event_targets_v1(
            event=_event(EventTypeV2.TICK),
            algo_instances=(algo,),
            catalog_runtime=replace(runtime, snapshot=missing_snapshot),
            correlated_algo_instance_ids=(),
        )
    assert raised.value.reason_code == "MINIQMT_RUNTIME_EVENT_ROUTING_CATALOG_INVALID"

    conflicting = algo.model_copy(update={"algo_code": "TWAP_MINIQMT"})
    with pytest.raises(KernelEventRoutingError) as raised:
        route_event_targets_v1(
            event=_event(EventTypeV2.TICK),
            algo_instances=(conflicting,),
            catalog_runtime=runtime,
            correlated_algo_instance_ids=(),
        )
    assert raised.value.reason_code == "MINIQMT_RUNTIME_EVENT_ROUTING_PLUGIN_IDENTITY_CONFLICT"


def test_ingress_coordinator_builds_exact_successor_and_fails_loud_without_predecessor() -> None:
    algo = _algo(slot="slot_coordinator", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    event = _event(EventTypeV2.TICK)
    repository = _IngressRepository(algo)
    coordinator = KernelIngressCoordinatorV1(repository=repository, catalog_runtime=_catalog())
    result = coordinator.ingest(
        event=event,
    )
    delivery = result["deliveries"][0]
    assert delivery.algo_delivery_sequence == 2
    assert delivery.previous_delivery_id == _tail(algo).delivery_id
    assert delivery.status is DeliveryStatusV1.PENDING

    missing = KernelIngressCoordinatorV1(
        repository=_IngressRepository(algo, missing_tail=True),
        catalog_runtime=_catalog(),
    )
    with pytest.raises(KernelEventRoutingError) as raised:
        missing.ingest(event=event)
    assert raised.value.reason_code == "MINIQMT_RUNTIME_EVENT_ROUTING_PREDECESSOR_MISSING"


def test_non_callback_ingress_rejects_callback_mapping_mutation() -> None:
    algo = _algo(slot="slot_unexpected_callback", status=ExecutionAlgoPersistenceStatusV2.ACTIVE)
    coordinator = KernelIngressCoordinatorV1(repository=_IngressRepository(algo), catalog_runtime=_catalog())
    with pytest.raises(KernelEventRoutingError) as raised:
        coordinator.ingest(
            event=_event(EventTypeV2.TICK),
            callback_mapping_update=object(),
        )
    assert raised.value.reason_code == "MINIQMT_RUNTIME_EVENT_CALLBACK_MAPPING_UPDATE_UNEXPECTED"
