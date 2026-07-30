from __future__ import annotations

from dataclasses import replace

import pytest

from backend.tests.miniqmt_execution_runtime.test_kernel_creation import _request

from backend.services.miniqmt_execution_runtime.kernel_delivery import KernelTransitionWriteBundleV1
from backend.services.miniqmt_execution_runtime.kernel_materializer import (
    materialize_failure_transition_v1,
    materialize_skip_transition_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_repository import (
    KernelRepositoryConflict,
    PostgresMiniQMTKernelRepository,
)
from backend.services.miniqmt_execution_runtime.kernel_repository_k2b import KernelRepositoryK2BMixin
from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CommandChildMappingStatusV1,
    DeliveryStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    OrderTypeV1,
    RuntimeEventEnvelopeV2,
    TimerMutationTypeV1,
    TimerMutationV1,
    kernel_lease_fence_token_v1,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_delivery import _WorkerRepository, _worker_facts


def _repository() -> PostgresMiniQMTKernelRepository:
    return PostgresMiniQMTKernelRepository(conn_factory=lambda: pytest.fail("validation must not access PostgreSQL"))


def _claimed_delivery(event, delivery, algo, state):
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    owner = "worker_validation_k2b:incarnation_validation_k2b"
    fence = kernel_lease_fence_token_v1(
        owner_type="DELIVERY", owner_id=delivery.delivery_id, lease_epoch=1, lease_owner=owner
    )
    claimed = repository.claim_delivery(
        lease_owner=owner,
        lease_epoch=1,
        lease_fence_token=fence,
        lease_expires_at="2026-07-26T01:31:00Z",
        updated_at_utc=event.event_time_utc,
    )
    return claimed


def _failure_bundle():
    event, delivery, algo, state = _worker_facts()
    claimed = _claimed_delivery(event, delivery, algo, state)
    submit = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id="transition_existing_k2b",
        ordinal=0,
        local_vt_orderid=None,
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.000000",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="MINIQMT_ALGO_EXISTING_CHILD",
        metadata={"origin": "validation"},
    )
    mapping = ExecutionCommandChildMappingV1.create(
        command=submit,
        strategy_slot_id=algo.strategy_slot_id,
        mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
        mapping_version=2,
        broker_order_id="broker_validation_k2b",
        broker_identity_source_event_id=event.event_id,
        last_order_event_id=event.event_id,
        last_trade_event_id=None,
        updated_by_event_id=event.event_id,
        created_at_utc="2026-07-26T01:29:00Z",
        updated_at_utc="2026-07-26T01:29:30Z",
    )
    timer_mutation = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=algo.algo_instance_id,
        transition_id="transition_existing_timer_k2b",
        ordinal=0,
        timer_name="next_slice",
        schedule_epoch="session_worker_k2b",
        due_at_exchange_utc="2026-07-26T01:31:00Z",
        catch_up_policy="EXPIRE_IF_LATE",
        payload={"slice": 2},
    )
    timer = ExecutionAlgoTimerScheduleV1.create(
        runtime_id=event.runtime_id,
        mutation=timer_mutation,
        status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
        emitted_event_id=None,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at_utc=None,
        row_version=1,
        created_at_utc="2026-07-26T01:29:00Z",
        updated_at_utc="2026-07-26T01:29:00Z",
        closed_at_utc=None,
    )
    bundle = materialize_failure_transition_v1(
        event=event,
        predecessor_delivery=claimed,
        previous_algo=algo,
        algo_code=algo.algo_code,
        plugin_id=algo.plugin_id,
        plugin_version=algo.plugin_version,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        plugin_config=thaw_json_v1(algo.plugin_config_json),
        plugin_config_sha256=algo.plugin_config_sha256,
        compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
        parent_intent_id=algo.parent_intent_id,
        strategy_slot_id=algo.strategy_slot_id,
        symbol=algo.symbol,
        side=algo.side,
        target_quantity=algo.target_quantity,
        stable_reason_code="MINIQMT_ALGO_TRANSITION_FAILED",
        exception=RuntimeError("deterministic plugin failure"),
        failure_context={"stage": "PLUGIN_TRANSITION"},
        projection_set=None,
        active_mappings=(mapping,),
        active_timer_schedules=(timer,),
        logical_time_utc=event.event_time_utc,
        initialization=False,
    )
    return event, claimed, algo, state, mapping, timer, bundle


def _validate(repository, bundle, claimed, algo) -> None:
    repository._validate_k2b_bundle(
        bundle,
        previous_delivery=claimed,
        previous_algo=algo,
        expected_delivery_row_version=claimed.row_version,
        expected_algo_row_version=algo.row_version,
    )


def test_k2b_public_entry_guards_fail_before_database_access() -> None:
    repository = _repository()
    with pytest.raises(TypeError, match="runtime_id"):
        repository.initialize_algo_atomic(
            runtime_id="",
            event_key_sha256="a" * 64,
            creation_authority=object(),
            bundle_builder=lambda _: None,
        )
    with pytest.raises(TypeError, match="event_key_sha256"):
        repository.initialize_algo_atomic(
            runtime_id="runtime_k2b",
            event_key_sha256="bad",
            creation_authority=object(),
            bundle_builder=lambda _: None,
        )
    with pytest.raises(TypeError, match="bundle_builder"):
        repository.initialize_algo_atomic(
            runtime_id="runtime_k2b",
            event_key_sha256="a" * 64,
            creation_authority=_request(),
            bundle_builder=None,
        )
    with pytest.raises(TypeError, match="delivery_id"):
        repository.apply_claimed_delivery_atomic(
            delivery_id="",
            expected_delivery_row_version=1,
            expected_algo_row_version=1,
            expected_lease_owner="worker",
            expected_lease_epoch=1,
            expected_lease_fence_token="fence",
            bundle_builder=lambda *_args: None,
        )
    with pytest.raises(TypeError, match="bundle_builder"):
        repository.apply_claimed_delivery_atomic(
            delivery_id="delivery_k2b",
            expected_delivery_row_version=1,
            expected_algo_row_version=1,
            expected_lease_owner="worker",
            expected_lease_epoch=1,
            expected_lease_fence_token="fence",
            bundle_builder=None,
        )
    with pytest.raises(TypeError, match="facade_read_request"):
        repository.apply_claimed_delivery_atomic(
            delivery_id="delivery_k2b",
            expected_delivery_row_version=1,
            expected_algo_row_version=1,
            expected_lease_owner="worker",
            expected_lease_epoch=1,
            expected_lease_fence_token="fence",
            bundle_builder=lambda *_args: None,
            facade_read_request=object(),
        )


def test_k2b_failure_and_skip_bundle_authority_accepts_exact_closure() -> None:
    repository = _repository()
    event, claimed, algo, _state, _mapping, _timer, bundle = _failure_bundle()
    _validate(repository, bundle, claimed, algo)

    next_event = RuntimeEventEnvelopeV2.create(
        runtime_id=event.runtime_id,
        sequence=event.sequence + 1,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-07-26T01:32:00Z",
        monotonic_ns=None,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol=algo.symbol,
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={"last_price": "10.010000"},
        source_identity={"market_data_id": "market_skip_validation_k2b"},
        correlation={},
    )
    pending = AlgoEventDeliveryV1.create(
        event=next_event,
        algo_instance_id=algo.algo_instance_id,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        algo_delivery_sequence=claimed.algo_delivery_sequence + 1,
        previous_delivery_id=claimed.delivery_id,
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc=next_event.event_time_utc,
        updated_at_utc=next_event.event_time_utc,
    )
    pending_persistence = AlgoDeliveryPersistenceV1.create(
        delivery=pending,
        lease_epoch=0,
        lease_fence_token=None,
        row_version=1,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        closed_at_utc=None,
    )
    claimed_skip = _claimed_delivery(next_event, pending_persistence, bundle.algo_instance, None)
    skip = materialize_skip_transition_v1(
        event=next_event,
        predecessor_delivery=claimed_skip,
        previous_algo=bundle.algo_instance,
        logical_time_utc=next_event.event_time_utc,
    )
    _validate(repository, skip, claimed_skip, bundle.algo_instance)
    with pytest.raises(ValueError, match="skip K2-B bundle cannot carry effects"):
        _validate(
            repository,
            replace(skip, command_outboxes=(bundle.command_outboxes[0],)),
            claimed_skip,
            bundle.algo_instance,
        )


def test_k2b_bundle_validation_rejects_cas_owner_effect_and_transaction_drift() -> None:
    repository = _repository()
    _event, claimed, algo, state, mapping, _timer, bundle = _failure_bundle()
    cases = (
        (replace(bundle, delivery=bundle.delivery.model_copy(update={"row_version": 99})), KernelRepositoryConflict),
        (
            replace(bundle, algo_instance=bundle.algo_instance.model_copy(update={"row_version": 99})),
            KernelRepositoryConflict,
        ),
        (replace(bundle, receipt=bundle.receipt.model_copy(update={"runtime_id": "runtime_other"})), ValueError),
        (replace(bundle, after_state=state), ValueError),
        (replace(bundle, command_outboxes=()), ValueError),
        (replace(bundle, timer_mutations=()), ValueError),
        (replace(bundle, timer_schedules=()), ValueError),
        (replace(bundle, new_child_mappings=(mapping, mapping)), ValueError),
        (
            replace(
                bundle,
                receipt=bundle.receipt.model_copy(update={"transaction_commit_identity": "mqtx_drift_k2b"}),
            ),
            ValueError,
        ),
    )
    for candidate, error_type in cases:
        with pytest.raises(error_type):
            _validate(repository, candidate, claimed, algo)

    mismatched_projection = ExecutionProjectionSetV1.create(
        runtime_id=claimed.runtime_id,
        algo_instance_id=claimed.algo_instance_id,
        event_id="event_other_k2b",
        delivery_id=claimed.delivery_id,
        projection_refs=(),
    )
    with pytest.raises(ValueError, match="projection set owner"):
        _validate(repository, replace(bundle, projection_set=mismatched_projection), claimed, algo)

    mismatched_timer = bundle.timer_schedules[0].model_copy(update={"timer_name": "other_timer_k2b"})
    with pytest.raises(ValueError, match="timer mutation does not close"):
        _validate(
            repository,
            replace(bundle, timer_schedules=(mismatched_timer,)),
            claimed,
            algo,
        )
    with pytest.raises(ValueError, match="requires CANCELLED"):
        _validate(
            repository,
            replace(bundle, timer_schedules=(_timer,)),
            claimed,
            algo,
        )

    drifted_outbox = bundle.command_outboxes[0].model_copy(update={"transition_id": "transition_other_k2b"})
    with pytest.raises(ValueError, match="transition identity"):
        _validate(
            repository,
            replace(bundle, command_outboxes=(drifted_outbox,)),
            claimed,
            algo,
        )
    mismatched_outbox = bundle.command_outboxes[0].model_copy(update={"mapping_id": "mapping_other_k2b"})
    callback_mapping = mapping.model_copy(update={"command_id": mismatched_outbox.command_id})
    with pytest.raises(ValueError, match="mapping/outbox"):
        _validate(
            repository,
            replace(
                bundle,
                new_child_mappings=(callback_mapping,),
                command_outboxes=(mismatched_outbox,),
            ),
            claimed,
            algo,
        )


class _ReadbackCursor:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, *_args):
        return None

    def fetchall(self):
        return []


class _ReadbackConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return _ReadbackCursor()


class _ReadbackRepository(KernelRepositoryK2BMixin):
    def __init__(self, bundle: KernelTransitionWriteBundleV1) -> None:
        self.bundle = bundle

    def read_transition_bundle(self, _identity):
        return {"receipt": self.bundle.receipt}

    def read_algo_instance(self, _identity):
        return self.bundle.algo_instance

    def read_delivery(self, _identity):
        return self.bundle.delivery

    def read_timer_schedule(self, schedule_id):
        return next(item for item in self.bundle.timer_schedules if item.schedule_id == schedule_id)

    def _connection(self, *, transaction):
        assert transaction is False
        return _ReadbackConnection()


def test_k2b_post_commit_readback_compares_transition_algo_delivery_timer_and_diagnostics() -> None:
    _event, _claimed, _algo, _state, _mapping, _timer, bundle = _failure_bundle()
    repository = _ReadbackRepository(bundle)
    result = repository._readback_k2b_bundle(bundle.receipt.failure_receipt_id, bundle)
    assert result["algo"] == bundle.algo_instance

    repository.bundle = replace(bundle, algo_instance=bundle.algo_instance.model_copy(update={"row_version": 99}))
    with pytest.raises(KernelRepositoryConflict, match="transition/algo"):
        repository._readback_k2b_bundle(bundle.receipt.failure_receipt_id, bundle)
