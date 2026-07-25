from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date
import inspect
import json
import os
from threading import Barrier

import psycopg2
import psycopg2.extras
import pytest

from backend.services.miniqmt_execution_runtime.kernel_repository import (
    KernelRepositoryCommitUnknown,
    KernelRepositoryConflict,
    KernelRepositorySchemaError,
    PostgresMiniQMTKernelRepository,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    AlgoFailureReceiptV1,
    AlgoStateSnapshotV2,
    AlgoTransitionReceiptV1,
    BrokerCommandAckReceiptV1,
    BrokerCommandOutboxV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    BrokerDispatchAttemptV1,
    BrokerOutcomeReconciliationReceiptV1,
    BrokerUnknownOutcomeReceiptV1,
    CommandChildMappingStatusV1,
    DeliveryStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExchangeSessionAuthorityV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionAlgoTimerOccurrenceStatusV1,
    ExecutionAlgoTimerOccurrenceV1,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    KernelErrorEvidenceV1,
    OrderTypeV1,
    RuntimeEventEnvelopeV2,
    SideV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    kernel_lease_fence_token_v1,
    transaction_commit_identity_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.tests.miniqmt_execution_runtime.test_kernel_contracts import (
    _algo_id,
    _calendar_authority_values,
    _tick_event,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_migration_postgres import (
    FORWARD,
    _apply_forward,
    _base_fixture_sql,
    _dev_dsn,
    _fixture_schema,
)


def test_repository_public_transaction_surface_is_complete() -> None:
    required_methods = {
        "preflight_schema",
        "start_worker_incarnation",
        "read_worker_startup_receipt",
        "write_event_receipt_deliveries",
        "read_event_transaction",
        "write_transition_bundle",
        "read_transition_bundle",
        "read_command_identity_chain",
        "compare_and_swap_mapping_outbox",
        "close_mapping_from_callback",
        "claim_delivery",
        "claim_outbox_command",
        "compare_and_swap_algo_instance",
        "append_dispatch_attempt",
        "write_timer_schedule",
        "write_timer_occurrence",
        "write_exchange_session_authority",
        "list_recovery_deliveries",
        "list_recovery_outbox_commands",
        "list_recovery_timer_occurrences",
    }

    assert required_methods <= set(dir(PostgresMiniQMTKernelRepository))
    transition_parameters = inspect.signature(PostgresMiniQMTKernelRepository.write_transition_bundle).parameters
    assert "new_child_mappings" in transition_parameters
    assert "command_outboxes" in transition_parameters


class _SchemaCursor:
    def __init__(self, cursor: object, schema: str) -> None:
        self._cursor = cursor
        self._schema = schema

    def execute(self, query: object, parameters: object = None) -> object:
        rewritten = query.replace("qmt_strategy", self._schema) if isinstance(query, str) else query
        return self._cursor.execute(rewritten, parameters)  # type: ignore[attr-defined]

    def __enter__(self) -> "_SchemaCursor":
        self._cursor.__enter__()  # type: ignore[attr-defined]
        return self

    def __exit__(self, *args: object) -> object:
        return self._cursor.__exit__(*args)  # type: ignore[attr-defined]

    def __getattr__(self, name: str) -> object:
        return getattr(self._cursor, name)


class _SchemaConnection:
    def __init__(self, connection: object, schema: str) -> None:
        self._connection = connection
        self._schema = schema

    def cursor(self, *args: object, **kwargs: object) -> _SchemaCursor:
        return _SchemaCursor(self._connection.cursor(*args, **kwargs), self._schema)  # type: ignore[attr-defined]

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)


class _FirstWriteBarrierCursor(_SchemaCursor):
    def __init__(self, cursor: object, schema: str, barrier: Barrier) -> None:
        super().__init__(cursor, schema)
        self._barrier = barrier
        self._wait_after_fetch = False

    def execute(self, query: object, parameters: object = None) -> object:
        self._wait_after_fetch = isinstance(query, str) and (
            "FROM qmt_strategy.execution_algo_timer_schedule WHERE schedule_id=%s FOR UPDATE" in query
        )
        return super().execute(query, parameters)

    def fetchone(self) -> object:
        row = self._cursor.fetchone()  # type: ignore[attr-defined]
        if self._wait_after_fetch and row is None:
            self._wait_after_fetch = False
            self._barrier.wait(timeout=10)
        return row


class _FirstWriteBarrierConnection(_SchemaConnection):
    def __init__(self, connection: object, schema: str, barrier: Barrier) -> None:
        super().__init__(connection, schema)
        self._barrier = barrier

    def cursor(self, *args: object, **kwargs: object) -> _FirstWriteBarrierCursor:
        return _FirstWriteBarrierCursor(  # type: ignore[attr-defined]
            self._connection.cursor(*args, **kwargs), self._schema, self._barrier
        )


class _FaultInjectionCursor(_SchemaCursor):
    _MARKERS = {
        "mapping": "UPDATE qmt_strategy.execution_child_order",
        "outbox": "UPDATE qmt_strategy.execution_algo_command_outbox",
        "algo": "INSERT INTO qmt_strategy.execution_algo_instance",
        "callback_readback": "/* callback closure readback */",
    }

    def __init__(self, cursor: object, schema: str, fault_point: str) -> None:
        super().__init__(cursor, schema)
        self._fault_point = fault_point

    def execute(self, query: object, parameters: object = None) -> object:
        if isinstance(query, str) and self._MARKERS[self._fault_point] in query:
            raise RuntimeError(f"injected {self._fault_point} write failure")
        return super().execute(query, parameters)


class _FaultInjectionConnection(_SchemaConnection):
    def __init__(self, connection: object, schema: str, fault_point: str) -> None:
        super().__init__(connection, schema)
        self._fault_point = fault_point

    def cursor(self, *args: object, **kwargs: object) -> _FaultInjectionCursor:
        return _FaultInjectionCursor(  # type: ignore[attr-defined]
            self._connection.cursor(*args, **kwargs), self._schema, self._fault_point
        )


def _conn_factory(schema: str):
    @contextmanager
    def factory(*, autocommit: bool = False, manage_transaction: bool = False):
        connection = psycopg2.connect(**_dev_dsn())
        connection.autocommit = autocommit
        proxy = _SchemaConnection(connection, schema)
        try:
            yield proxy
            if manage_transaction and not autocommit:
                connection.commit()
        except Exception:
            if not autocommit:
                connection.rollback()
            raise
        finally:
            connection.close()

    return factory


def _fault_injection_factory(schema: str, fault_point: str):
    @contextmanager
    def factory(*, autocommit: bool = False, manage_transaction: bool = False):
        connection = psycopg2.connect(**_dev_dsn())
        connection.autocommit = autocommit
        proxy = _FaultInjectionConnection(connection, schema, fault_point)
        try:
            yield proxy
            if manage_transaction and not autocommit:
                connection.commit()
        except Exception:
            if not autocommit:
                connection.rollback()
            raise
        finally:
            connection.close()

    return factory


def _commit_unknown_factory(schema: str):
    @contextmanager
    def factory(*, autocommit: bool = False, manage_transaction: bool = False):
        connection = psycopg2.connect(**_dev_dsn())
        connection.autocommit = autocommit
        proxy = _SchemaConnection(connection, schema)
        try:
            yield proxy
            if manage_transaction and not autocommit:
                connection.commit()
                raise KernelRepositoryCommitUnknown("commit return was not observed")
        except KernelRepositoryCommitUnknown:
            raise
        except Exception:
            if not autocommit:
                connection.rollback()
            raise
        finally:
            connection.close()

    return factory


def _post_commit_schedule_drift_factory(schema: str, schedule_id: str):
    @contextmanager
    def factory(*, autocommit: bool = False, manage_transaction: bool = False):
        connection = psycopg2.connect(**_dev_dsn())
        connection.autocommit = autocommit
        proxy = _SchemaConnection(connection, schema)
        try:
            yield proxy
            if manage_transaction and not autocommit:
                connection.commit()
                with connection.cursor() as cur:
                    cur.execute(
                        f"UPDATE {schema}.execution_algo_timer_schedule "
                        "SET due_at_exchange_utc=due_at_exchange_utc + interval '1 second' "
                        "WHERE schedule_id=%s",
                        (schedule_id,),
                    )
                connection.commit()
        except Exception:
            if not autocommit:
                connection.rollback()
            raise
        finally:
            connection.close()

    return factory


def _conn_factory_without_keywords(schema: str):
    managed = _conn_factory(schema)

    @contextmanager
    def factory():
        with managed(autocommit=True) as connection:
            yield connection

    return factory


@contextmanager
def _forbidden_conn_factory(*, autocommit: bool = False, manage_transaction: bool = False):
    del autocommit, manage_transaction
    raise AssertionError("repository touched PostgreSQL before rejecting a forged initial fact")
    yield  # pragma: no cover


def _concurrent_first_write_factory(schema: str, barrier: Barrier):
    @contextmanager
    def factory(*, autocommit: bool = False, manage_transaction: bool = False):
        connection = psycopg2.connect(**_dev_dsn())
        connection.autocommit = autocommit
        proxy = _FirstWriteBarrierConnection(connection, schema, barrier)
        try:
            yield proxy
            if manage_transaction and not autocommit:
                connection.commit()
        except Exception:
            if not autocommit:
                connection.rollback()
            raise
        finally:
            connection.close()

    return factory


def _event(*, last_price: str) -> RuntimeEventEnvelopeV2:
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_repo_k2",
        sequence=1,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-07-25T01:30:00Z",
        monotonic_ns=None,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol="600000.SH",
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={"last_price": last_price},
        source_identity={"market_data_id": "market_repo_k2"},
        correlation={"trace_id": "trace_repo_k2"},
    )


def _delivery(
    event: RuntimeEventEnvelopeV2,
    *,
    algo_instance_id: str = "algo_repo_k2",
    algo_delivery_sequence: int = 1,
    previous_delivery_id: str | None = None,
) -> AlgoDeliveryPersistenceV1:
    delivery = AlgoEventDeliveryV1.create(
        event=event,
        algo_instance_id=algo_instance_id,
        plugin_manifest_sha256="1" * 64,
        algo_delivery_sequence=algo_delivery_sequence,
        previous_delivery_id=previous_delivery_id,
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc="2026-07-25T01:30:00Z",
        updated_at_utc="2026-07-25T01:30:00Z",
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


def _algo(*, row_version: int, active_child_count: int) -> ExecutionAlgoInstancePersistenceV2:
    config = {"slices": 4}
    state = {"next_slice": 1}
    return ExecutionAlgoInstancePersistenceV2.create(
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
        plugin_manifest_sha256="1" * 64,
        plugin_config_json=config,
        plugin_config_sha256=hash_hex_v1("miniqmt_plugin_config_v2", config),
        compatibility_receipt_sha256="2" * 64,
        state_schema_version="twap_state_v1",
        state_json=state,
        state_sha256=hash_hex_v1("execution_algo_state_v2", state),
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id="delivery_state_k2",
        last_closed_delivery_sequence=1,
        terminal_delivery_sequence=None,
        status=ExecutionAlgoPersistenceStatusV2.ACTIVE,
        failure_receipt_id=None,
        active_child_closure_status=ActiveChildClosureStatusV1.NOT_APPLICABLE,
        active_child_count=active_child_count,
        row_version=row_version,
        created_at_utc="2026-07-25T01:20:00Z",
        updated_at_utc=f"2026-07-25T01:2{row_version}:00Z",
        terminal_at_utc=None,
        archived_at_utc=None,
    )


def _submit_chain(transition_id: str) -> tuple[ExecutionCommandChildMappingV1, BrokerCommandOutboxV1]:
    command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="runtime_k2",
        algo_instance_id=_algo_id(),
        parent_intent_id="intent_k2",
        transition_id=transition_id,
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
    mapping = ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id="slot_k2",
        mapping_status=CommandChildMappingStatusV1.RESERVED,
        mapping_version=1,
        broker_order_id=None,
        broker_identity_source_event_id=None,
        last_order_event_id=None,
        last_trade_event_id=None,
        updated_by_event_id=None,
        created_at_utc="2026-07-25T01:30:00Z",
        updated_at_utc="2026-07-25T01:30:00Z",
    )
    outbox = BrokerCommandOutboxV1.create(
        command=command,
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
    return mapping, outbox


def test_repository_public_writers_reject_noninitial_first_facts_before_database_access() -> None:
    repository = PostgresMiniQMTKernelRepository(conn_factory=_forbidden_conn_factory)
    event = _tick_event()
    pending_delivery = _delivery(event, algo_instance_id=_algo_id())
    claimed_payload = pending_delivery.model_dump(mode="python")
    lease_owner = "worker_initial_k2:incarnation_initial_k2"
    claimed_payload.update(
        status=DeliveryStatusV1.CLAIMED,
        attempt_count=1,
        lease_owner=lease_owner,
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=pending_delivery.delivery_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        ),
        lease_expires_at="2026-07-25T01:40:00Z",
        row_version=2,
        updated_at_utc="2026-07-25T01:31:00Z",
    )
    claimed_delivery = AlgoDeliveryPersistenceV1.model_validate(claimed_payload)
    with pytest.raises(ValueError, match="first write requires initial PENDING delivery"):
        repository.write_event_receipt_deliveries(event=event, deliveries=(claimed_delivery,))

    mapping, outbox = _submit_chain("transition_forged_initial_k2")
    command = BrokerCommandV2.model_validate_json(
        json.dumps(outbox.model_dump(mode="json")["payload_json"], sort_keys=True, separators=(",", ":"))
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id="runtime_k2",
        algo_instance_id=_algo_id(),
        event_id=event.event_id,
        delivery_id=claimed_delivery.delivery_id,
        projection_refs=(),
    )
    algo = _algo(row_version=2, active_child_count=1)
    after_state = AlgoStateSnapshotV2.model_validate(
        {
            "schema_version": "execution_algo_state_snapshot_v2",
            "algo_instance_id": _algo_id(),
            "plugin_id": algo.plugin_id,
            "plugin_version": algo.plugin_version,
            "plugin_manifest_sha256": algo.plugin_manifest_sha256,
            "state_schema_version": algo.state_schema_version,
            "transition_sequence": 1,
            "last_applied_delivery_sequence": 1,
            "last_applied_delivery_id": claimed_delivery.delivery_id,
            "last_closed_delivery_sequence": 1,
            "state": algo.model_dump(mode="python")["state_json"],
            "state_sha256": algo.state_sha256,
            "last_applied_event_id": event.event_id,
            "updated_at_utc": "2026-07-25T01:31:00Z",
        }
    )

    def receipt_for(
        candidate_mapping: ExecutionCommandChildMappingV1,
        candidate_outbox: BrokerCommandOutboxV1,
    ) -> AlgoTransitionReceiptV1:
        provisional = AlgoTransitionReceiptV1.create(
            delivery_id=claimed_delivery.delivery_id,
            event_id=event.event_id,
            runtime_id="runtime_k2",
            algo_instance_id=_algo_id(),
            plugin_id=algo.plugin_id,
            plugin_version=algo.plugin_version,
            plugin_manifest_sha256=algo.plugin_manifest_sha256,
            transition_sequence=2,
            before_state_sha256_or_INIT=algo.state_sha256,
            after_state_sha256=after_state.state_sha256,
            ordered_command_ids=(candidate_outbox.command_id,),
            ordered_timer_mutation_ids=(),
            ordered_diagnostic_observation_ids=(),
            ordered_consumed_lineage_refs=(),
            execution_projection_set_sha256=projection_set.projection_set_sha256,
            effect_set_sha256="9" * 64,
            terminal_outcome=None,
            logical_applied_at_utc="2026-07-25T01:31:00Z",
            transaction_commit_identity="mqtx_pending_forged_initial_k2",
        )
        return AlgoTransitionReceiptV1.create(
            **provisional.canonical_payload_v1(
                exclude={"schema_version", "transition_id", "transaction_commit_identity", "receipt_sha256"}
            ),
            transaction_commit_identity=transaction_commit_identity_v1(
                operation="WRITE_APPLIED_TRANSITION_BUNDLE",
                owner_identities=("runtime_k2", _algo_id(), event.event_id, claimed_delivery.delivery_id),
                input_hashes=(
                    projection_set.projection_set_sha256,
                    after_state.state_sha256,
                    candidate_mapping.payload_sha256,
                    candidate_outbox.payload_sha256,
                ),
                output_identities=(
                    provisional.transition_id,
                    candidate_mapping.mapping_id,
                    candidate_outbox.command_id,
                ),
            ),
        )

    forged_mapping = ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id="slot_k2",
        mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
        mapping_version=1,
        broker_order_id="broker_forged_initial_k2",
        broker_identity_source_event_id="event_forged_initial_k2",
        last_order_event_id="event_forged_initial_k2",
        last_trade_event_id=None,
        updated_by_event_id="event_forged_initial_k2",
        created_at_utc=mapping.created_at_utc,
        updated_at_utc=mapping.updated_at_utc,
    )
    with pytest.raises(ValueError, match="first write requires initial RESERVED mapping"):
        repository.write_transition_bundle(
            algo_instance=algo,
            delivery=claimed_delivery,
            receipt=receipt_for(forged_mapping, outbox),
            projection_set=projection_set,
            after_state=after_state,
            expected_algo_row_version=1,
            expected_delivery_row_version=1,
            new_child_mappings=(forged_mapping,),
            command_outboxes=(outbox,),
        )

    ack = BrokerCommandAckReceiptV1.create(
        command_id=command.command_id,
        mapping_id=mapping.mapping_id,
        deterministic_client_order_ref=outbox.deterministic_client_order_ref,
        gateway_route_id="gateway_forged_initial_k2",
        gateway_catalog_sha256="8" * 64,
        source="SYNCHRONOUS_RETURN",
        accepted=True,
        broker_order_id="broker_forged_initial_k2",
        reason_code="FORGED_ACK",
        ack_payload_sha256="7" * 64,
        observed_at_utc="2026-07-25T01:31:00Z",
    )
    forged_outbox = BrokerCommandOutboxV1.create(
        command=command,
        mapping_id=mapping.mapping_id,
        status=BrokerCommandOutboxStatusV1.ACKED,
        attempt_count=1,
        lease_owner=None,
        lease_epoch=1,
        lease_fence_token=None,
        lease_expires_at=None,
        dispatch_attempt_id="dispatch_forged_initial_k2",
        next_attempt_at_utc=None,
        broker_called=True,
        broker_order_id="broker_forged_initial_k2",
        ack_receipt_json=ack,
        ack_receipt_sha256=ack.receipt_sha256,
        non_acceptance_receipt=None,
        unknown_outcome_receipt=None,
        reconcile_receipt=None,
        last_error_json=None,
        row_version=1,
        created_at_utc=outbox.created_at_utc,
        updated_at_utc="2026-07-25T01:31:00Z",
        closed_at_utc="2026-07-25T01:31:00Z",
    )
    with pytest.raises(ValueError, match="first write requires initial PENDING outbox"):
        repository.write_transition_bundle(
            algo_instance=algo,
            delivery=claimed_delivery,
            receipt=receipt_for(mapping, forged_outbox),
            projection_set=projection_set,
            after_state=after_state,
            expected_algo_row_version=1,
            expected_delivery_row_version=1,
            new_child_mappings=(mapping,),
            command_outboxes=(forged_outbox,),
        )


@pytest.mark.parametrize("with_schema_drift", (False, True), ids=("function_only", "function_and_schema"))
def test_repository_preflight_rejects_forged_constant_catalog_function_on_dev_postgres(
    with_schema_drift: bool,
) -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K2 DEV PostgreSQL fixture")
    schema = _fixture_schema()
    raw = psycopg2.connect(**_dev_dsn())
    raw.autocommit = True
    try:
        with raw.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _apply_forward(cur, FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            if with_schema_drift:
                cur.execute(
                    f"ALTER TABLE {schema}.execution_kernel_worker_epoch ALTER COLUMN incarnation_sequence DROP DEFAULT"
                )
            cur.execute(
                f"""
                CREATE OR REPLACE FUNCTION {schema}.miniqmt_k2_catalog_fingerprint()
                RETURNS TEXT LANGUAGE SQL STABLE
                AS $forged$ SELECT '6e4fc4ae4c6e403d3316c124da6ae5933eb33184129569fd6bf1cf750e27f762'::TEXT $forged$
                """
            )
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        with pytest.raises(KernelRepositorySchemaError, match="catalog (function|schema) drift"):
            repository.preflight_schema()
    finally:
        raw.autocommit = True
        with raw.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        raw.close()


@pytest.mark.parametrize(
    "target",
    ("event", "delivery", "algo", "worker", "timer_schedule", "timer_occurrence"),
)
def test_repository_exact_scalar_readback_rejects_carrier_drift_on_dev_postgres(target: str) -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K2 DEV PostgreSQL fixture")
    schema = _fixture_schema()
    raw = psycopg2.connect(**_dev_dsn())
    raw.autocommit = True
    try:
        with raw.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _apply_forward(cur, FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES (%s,%s)",
                ("runtime_k2", date(2026, 7, 25)),
            )
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        algo = _algo(row_version=1, active_child_count=0)
        repository.compare_and_swap_algo_instance(algo_instance=algo, expected_row_version=0)
        worker = repository.start_worker_incarnation(
            worker_id="worker_scalar_k2",
            process_role="timer",
            source_revision="revision_scalar_k2",
            started_at_utc="2026-07-25T01:00:00Z",
        )
        event = _tick_event()
        delivery = _delivery(event, algo_instance_id=_algo_id())
        repository.write_event_receipt_deliveries(event=event, deliveries=(delivery,))
        mutation = TimerMutationV1.create(
            mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
            algo_instance_id=_algo_id(),
            transition_id="transition_scalar_timer_k2",
            ordinal=0,
            timer_name="scalar_timer",
            schedule_epoch="session_epoch_scalar_k2",
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
        repository.write_timer_schedule(schedule)
        lease_owner = f"worker_scalar_k2:{worker.process_incarnation_id}"
        occurrence = ExecutionAlgoTimerOccurrenceV1.create(
            schedule=schedule,
            exchange_session_authority_sha256="4" * 64,
            status=ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED,
            emitted_event_id=None,
            catch_up_receipt_sha256=None,
            lease_owner=lease_owner,
            lease_epoch=1,
            lease_fence_token=kernel_lease_fence_token_v1(
                owner_type="TIMER_OCCURRENCE",
                owner_id=schedule.timer_occurrence_id,
                lease_epoch=1,
                lease_owner=lease_owner,
            ),
            lease_expires_at_utc="2026-07-25T02:01:00Z",
            row_version=1,
            created_at_utc="2026-07-25T02:00:00Z",
            closed_at_utc=None,
        )
        repository.write_timer_occurrence(occurrence)
        drift = {
            "event": (
                f"UPDATE {schema}.execution_runtime_event SET sequence=sequence+1000 WHERE event_id=%s",
                (event.event_id,),
                lambda: repository.read_event_transaction(event.event_id),
            ),
            "delivery": (
                f"UPDATE {schema}.execution_algo_event_delivery SET plugin_manifest_sha256=%s WHERE delivery_id=%s",
                ("2" * 64, delivery.delivery_id),
                lambda: repository.read_delivery(delivery.delivery_id),
            ),
            "algo": (
                f"UPDATE {schema}.execution_algo_instance "
                "SET traded_quantity=traded_quantity+10,remaining_quantity=remaining_quantity-10 "
                "WHERE algo_instance_id=%s",
                (_algo_id(),),
                lambda: repository.read_algo_instance(_algo_id()),
            ),
            "worker": (
                f"UPDATE {schema}.execution_kernel_worker_incarnation "
                "SET source_revision=source_revision || '_drift' WHERE process_incarnation_id=%s",
                (worker.process_incarnation_id,),
                lambda: repository.read_worker_startup_receipt(worker.process_incarnation_id),
            ),
            "timer_schedule": (
                f"UPDATE {schema}.execution_algo_timer_schedule "
                "SET due_at_exchange_utc=due_at_exchange_utc+interval '1 second' WHERE schedule_id=%s",
                (schedule.schedule_id,),
                lambda: repository.read_timer_schedule(schedule.schedule_id),
            ),
            "timer_occurrence": (
                f"UPDATE {schema}.execution_algo_timer_occurrence "
                "SET exchange_session_authority_sha256=%s "
                "WHERE timer_occurrence_id=%s",
                ("5" * 64, occurrence.timer_occurrence_id),
                lambda: repository.read_timer_occurrence(occurrence.timer_occurrence_id),
            ),
        }[target]
        with raw.cursor() as cur:
            cur.execute(drift[0], drift[1])
        with pytest.raises(KernelRepositoryConflict, match="scalar columns drift"):
            drift[2]()
    finally:
        raw.autocommit = True
        with raw.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        raw.close()


def test_repository_real_postgres_startup_event_readback_conflict_rollback_and_bounds() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K2 DEV PostgreSQL fixture")
    schema = _fixture_schema()
    incomplete_schema = _fixture_schema()
    callback_schemas: list[str] = []
    raw = psycopg2.connect(**_dev_dsn())
    raw.autocommit = True
    try:
        with raw.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {incomplete_schema}")
            cur.execute(_base_fixture_sql(schema))
            _apply_forward(cur, FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES (%s,%s),(%s,%s)",
                ("runtime_repo_k2", date(2026, 7, 25), "runtime_k2", date(2026, 7, 25)),
            )
            cur.execute(
                f"INSERT INTO {schema}.execution_algo_instance("
                "algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,"
                "remaining_quantity,algo_code,status) "
                "VALUES ('algo_repo_k2','runtime_repo_k2','intent_repo_k2','slot_repo_k2',"
                "'600000.SH','BUY',100,100,'TWAP','ACTIVE')"
            )

        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        assert all(repository.preflight_schema().values())
        assert all(
            PostgresMiniQMTKernelRepository(conn_factory=_conn_factory_without_keywords(schema))
            .preflight_schema()
            .values()
        )
        with raw.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {schema}.execution_kernel_worker_epoch ALTER COLUMN incarnation_sequence DROP DEFAULT"
            )
        with pytest.raises(KernelRepositorySchemaError, match="catalog drift"):
            repository.preflight_schema()
        with raw.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {schema}.execution_kernel_worker_epoch ALTER COLUMN incarnation_sequence SET DEFAULT 0"
            )
        assert all(repository.preflight_schema().values())
        PostgresMiniQMTKernelRepository(conn_factory=object())
        with pytest.raises(KernelRepositorySchemaError, match="incomplete"):
            PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(incomplete_schema)).preflight_schema()
        with raw.cursor() as cur:
            for table_name in (
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
            ):
                cur.execute(f"CREATE TABLE {incomplete_schema}.{table_name}(id INTEGER)")
        with pytest.raises(KernelRepositorySchemaError, match="fingerprint authority is unavailable"):
            PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(incomplete_schema)).preflight_schema()
        first_start = repository.start_worker_incarnation(
            worker_id="worker_repo_k2",
            process_role="dispatcher",
            source_revision="revision_repo_k2",
            started_at_utc="2026-07-25T01:00:00Z",
        )
        second_start = repository.start_worker_incarnation(
            worker_id="worker_repo_k2",
            process_role="dispatcher",
            source_revision="revision_repo_k2",
            started_at_utc="2026-07-25T01:01:00Z",
        )
        assert second_start.incarnation_sequence == first_start.incarnation_sequence + 1
        assert second_start.process_incarnation_id != first_start.process_incarnation_id
        assert repository.read_worker_startup_receipt(first_start.process_incarnation_id) == first_start
        with pytest.raises(KeyError):
            repository.read_worker_startup_receipt("missing_incarnation")
        with raw.cursor() as cur:
            cur.execute(
                f"UPDATE {schema}.execution_kernel_worker_incarnation SET startup_receipt_json='[]'::jsonb "
                "WHERE process_incarnation_id=%s",
                (first_start.process_incarnation_id,),
            )
        with pytest.raises(KernelRepositoryConflict, match="not a JSON object"):
            repository.read_worker_startup_receipt(first_start.process_incarnation_id)
        with raw.cursor() as cur:
            cur.execute(
                f"UPDATE {schema}.execution_kernel_worker_incarnation SET startup_receipt_json=%s "
                "WHERE process_incarnation_id=%s",
                (psycopg2.extras.Json(first_start.model_dump(mode="json")), first_start.process_incarnation_id),
            )

        algo_v1 = _algo(row_version=1, active_child_count=0)
        assert repository.compare_and_swap_algo_instance(algo_instance=algo_v1, expected_row_version=0) == algo_v1
        with pytest.raises((ValueError, KernelRepositoryConflict)):
            repository.compare_and_swap_algo_instance(algo_instance=algo_v1, expected_row_version=0)
        lease_owner = f"worker_repo_k2:{second_start.process_incarnation_id}"
        submit_event = _tick_event()
        submit_delivery = _delivery(submit_event, algo_instance_id=_algo_id())
        repository.write_event_receipt_deliveries(event=submit_event, deliveries=(submit_delivery,))
        submit_fence = kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=submit_delivery.delivery_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        )
        claimed_submit_delivery = repository.claim_delivery(
            delivery_id=submit_delivery.delivery_id,
            lease_owner=lease_owner,
            lease_epoch=1,
            lease_fence_token=submit_fence,
            lease_expires_at="2026-07-25T01:40:00Z",
            updated_at_utc="2026-07-25T01:31:00Z",
            expected_row_version=1,
        )
        projection_set = ExecutionProjectionSetV1.create(
            runtime_id="runtime_k2",
            algo_instance_id=_algo_id(),
            event_id=submit_event.event_id,
            delivery_id=submit_delivery.delivery_id,
            projection_refs=(),
        )
        after_state = AlgoStateSnapshotV2.model_validate(
            {
                "schema_version": "execution_algo_state_snapshot_v2",
                "algo_instance_id": _algo_id(),
                "plugin_id": algo_v1.plugin_id,
                "plugin_version": algo_v1.plugin_version,
                "plugin_manifest_sha256": algo_v1.plugin_manifest_sha256,
                "state_schema_version": algo_v1.state_schema_version,
                "transition_sequence": 1,
                "last_applied_delivery_sequence": 1,
                "last_applied_delivery_id": submit_delivery.delivery_id,
                "last_closed_delivery_sequence": 1,
                "state": algo_v1.model_dump(mode="python")["state_json"],
                "state_sha256": algo_v1.state_sha256,
                "last_applied_event_id": submit_event.event_id,
                "updated_at_utc": "2026-07-25T01:32:00Z",
            }
        )
        provisional_receipt = AlgoTransitionReceiptV1.create(
            delivery_id=submit_delivery.delivery_id,
            event_id=submit_event.event_id,
            runtime_id="runtime_k2",
            algo_instance_id=_algo_id(),
            plugin_id=algo_v1.plugin_id,
            plugin_version=algo_v1.plugin_version,
            plugin_manifest_sha256=algo_v1.plugin_manifest_sha256,
            transition_sequence=1,
            before_state_sha256_or_INIT=algo_v1.state_sha256,
            after_state_sha256=after_state.state_sha256,
            ordered_command_ids=(),
            ordered_timer_mutation_ids=(),
            ordered_diagnostic_observation_ids=(),
            ordered_consumed_lineage_refs=(),
            execution_projection_set_sha256=projection_set.projection_set_sha256,
            effect_set_sha256="9" * 64,
            terminal_outcome=None,
            logical_applied_at_utc="2026-07-25T01:32:00Z",
            transaction_commit_identity="mqtx_submit_repo_k2",
        )
        mapping, outbox = _submit_chain(provisional_receipt.transition_id)
        submit_transaction_identity = transaction_commit_identity_v1(
            operation="WRITE_APPLIED_TRANSITION_BUNDLE",
            owner_identities=(
                "runtime_k2",
                _algo_id(),
                submit_event.event_id,
                submit_delivery.delivery_id,
            ),
            input_hashes=(
                projection_set.projection_set_sha256,
                after_state.state_sha256,
                mapping.payload_sha256,
                outbox.payload_sha256,
            ),
            output_identities=(
                provisional_receipt.transition_id,
                mapping.mapping_id,
                outbox.command_id,
            ),
        )
        transition_receipt = AlgoTransitionReceiptV1.create(
            **provisional_receipt.canonical_payload_v1(
                exclude={
                    "schema_version",
                    "transition_id",
                    "ordered_command_ids",
                    "command_set_sha256",
                    "timer_set_sha256",
                    "diagnostic_set_sha256",
                    "consumed_lineage_set_sha256",
                    "transaction_commit_identity",
                    "receipt_sha256",
                }
            ),
            ordered_command_ids=(outbox.command_id,),
            transaction_commit_identity=submit_transaction_identity,
        )
        applied_delivery_payload = claimed_submit_delivery.model_dump(mode="python")
        applied_delivery_payload.update(
            status=DeliveryStatusV1.APPLIED,
            lease_owner=None,
            lease_fence_token=None,
            lease_expires_at=None,
            transition_id=transition_receipt.transition_id,
            row_version=3,
            updated_at_utc="2026-07-25T01:32:00Z",
            closed_at_utc="2026-07-25T01:32:00Z",
        )
        applied_delivery = AlgoDeliveryPersistenceV1.model_validate(applied_delivery_payload)
        algo_v2_payload = algo_v1.model_dump(mode="python")
        algo_v2_payload.update(
            active_child_count=1,
            row_version=2,
            last_applied_delivery_id=submit_delivery.delivery_id,
            updated_at_utc="2026-07-25T01:32:00Z",
        )
        algo_v2 = ExecutionAlgoInstancePersistenceV2.model_validate(algo_v2_payload)
        wrong_transaction_receipt = AlgoTransitionReceiptV1.create(
            **transition_receipt.canonical_payload_v1(
                exclude={
                    "schema_version",
                    "transition_id",
                    "command_set_sha256",
                    "timer_set_sha256",
                    "diagnostic_set_sha256",
                    "consumed_lineage_set_sha256",
                    "transaction_commit_identity",
                    "receipt_sha256",
                }
            ),
            transaction_commit_identity="mqtx_wrong_transition_identity",
        )
        with pytest.raises(ValueError, match="repository-owned transaction commit identity"):
            repository.write_transition_bundle(
                algo_instance=algo_v2,
                delivery=applied_delivery,
                receipt=wrong_transaction_receipt,
                projection_set=projection_set,
                after_state=after_state,
                expected_algo_row_version=1,
                expected_delivery_row_version=2,
                new_child_mappings=(mapping,),
                command_outboxes=(outbox,),
            )
        with pytest.raises(KernelRepositoryConflict, match="algo instance CAS failed"):
            repository.write_transition_bundle(
                algo_instance=algo_v2,
                delivery=applied_delivery,
                receipt=transition_receipt,
                projection_set=projection_set,
                after_state=after_state,
                expected_algo_row_version=999,
                expected_delivery_row_version=2,
                new_child_mappings=(mapping,),
                command_outboxes=(outbox,),
            )
        with pytest.raises(KeyError):
            repository.read_transition_bundle(transition_receipt.transition_id)
        with pytest.raises(KeyError):
            repository.read_command_identity_chain(outbox.command_id)
        submit_bundle = repository.write_transition_bundle(
            algo_instance=algo_v2,
            delivery=applied_delivery,
            receipt=transition_receipt,
            projection_set=projection_set,
            after_state=after_state,
            expected_algo_row_version=1,
            expected_delivery_row_version=2,
            new_child_mappings=(mapping,),
            command_outboxes=(outbox,),
        )
        assert submit_bundle["new_child_mappings"] == (mapping,)
        assert submit_bundle["command_outboxes"] == (outbox,)
        assert repository.read_command_identity_chain(outbox.command_id) == {
            "mapping": mapping,
            "outbox": outbox,
        }
        with pytest.raises(KeyError):
            repository.read_command_identity_chain("missing_command")

        outbox_fence = kernel_lease_fence_token_v1(
            owner_type="OUTBOX_COMMAND",
            owner_id=outbox.command_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        )
        claimed_outbox = repository.claim_outbox_command(
            command_id=outbox.command_id,
            lease_owner=lease_owner,
            lease_epoch=1,
            lease_fence_token=outbox_fence,
            lease_expires_at="2026-07-25T01:40:00Z",
            updated_at_utc="2026-07-25T01:31:00Z",
            expected_row_version=1,
        )
        assert claimed_outbox.status is BrokerCommandOutboxStatusV1.CLAIMED
        assert repository.list_recovery_outbox_commands(
            runtime_id="runtime_k2", trade_date=date(2026, 7, 25), statuses=("CLAIMED",), limit=10
        ) == (claimed_outbox,)
        submit_command = BrokerCommandV2.model_validate_json(
            json.dumps(outbox.model_dump(mode="json")["payload_json"], sort_keys=True, separators=(",", ":"))
        )
        mismatched_attempt_fence = kernel_lease_fence_token_v1(
            owner_type="OUTBOX_COMMAND",
            owner_id=outbox.command_id,
            lease_epoch=2,
            lease_owner=lease_owner,
        )
        mismatched_dispatch_attempt = BrokerDispatchAttemptV1.create(
            command_id=outbox.command_id,
            attempt_count=claimed_outbox.attempt_count,
            lease_epoch=2,
            lease_fence_token=mismatched_attempt_fence,
            process_incarnation_id=second_start.process_incarnation_id,
            stage="CLAIMED",
            started_at_utc="2026-07-25T01:31:00Z",
            finished_at_utc=None,
            pre_call_complete=False,
            broker_called=None,
            outcome=None,
            error_reason_code=None,
            error_context_sha256=None,
            authority_receipt_sha256=None,
        )
        with pytest.raises(KernelRepositoryConflict, match="current outbox lease"):
            repository.append_dispatch_attempt(mismatched_dispatch_attempt)
        dispatch_attempt = BrokerDispatchAttemptV1.create(
            command_id=outbox.command_id,
            attempt_count=claimed_outbox.attempt_count,
            lease_epoch=claimed_outbox.lease_epoch,
            lease_fence_token=claimed_outbox.lease_fence_token,
            process_incarnation_id=second_start.process_incarnation_id,
            stage="CLAIMED",
            started_at_utc="2026-07-25T01:31:00Z",
            finished_at_utc=None,
            pre_call_complete=False,
            broker_called=None,
            outcome=None,
            error_reason_code=None,
            error_context_sha256=None,
            authority_receipt_sha256=None,
        )
        assert repository.append_dispatch_attempt(dispatch_attempt) == dispatch_attempt
        assert repository.append_dispatch_attempt(dispatch_attempt) == dispatch_attempt
        dispatching_mapping = ExecutionCommandChildMappingV1.create(
            command=submit_command,
            strategy_slot_id="slot_k2",
            mapping_status=CommandChildMappingStatusV1.DISPATCHING,
            mapping_version=2,
            broker_order_id=None,
            broker_identity_source_event_id=None,
            last_order_event_id=None,
            last_trade_event_id=None,
            updated_by_event_id=None,
            created_at_utc=mapping.created_at_utc,
            updated_at_utc="2026-07-25T01:32:00Z",
        )
        dispatching_outbox = BrokerCommandOutboxV1.create(
            command=submit_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.DISPATCHING,
            attempt_count=claimed_outbox.attempt_count,
            lease_owner=claimed_outbox.lease_owner,
            lease_epoch=claimed_outbox.lease_epoch,
            lease_fence_token=claimed_outbox.lease_fence_token,
            lease_expires_at=claimed_outbox.lease_expires_at,
            dispatch_attempt_id=dispatch_attempt.dispatch_attempt_id,
            next_attempt_at_utc=None,
            broker_called=None,
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=claimed_outbox.row_version + 1,
            created_at_utc=claimed_outbox.created_at_utc,
            updated_at_utc="2026-07-25T01:32:00Z",
            closed_at_utc=None,
        )
        pre_call_evidence = KernelErrorEvidenceV1.create(
            stage="OUTBOX_PRE_CALL",
            stable_reason_code="MINIQMT_PRE_CALL_UNAVAILABLE",
            exception=RuntimeError("gateway provider unavailable"),
            message="gateway provider unavailable",
            retryable=True,
            terminal=False,
            broker_called=False,
            primary_context={"command_id": outbox.command_id, "mapping_id": mapping.mapping_id},
            secondary_errors=(),
        )
        mismatched_outbox = BrokerCommandOutboxV1.create(
            command=submit_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.FAILED_RETRYABLE,
            attempt_count=claimed_outbox.attempt_count,
            lease_owner=None,
            lease_epoch=claimed_outbox.lease_epoch,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=None,
            next_attempt_at_utc="2026-07-25T01:32:01Z",
            broker_called=False,
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=pre_call_evidence.model_dump(mode="json"),
            row_version=claimed_outbox.row_version + 1,
            created_at_utc=claimed_outbox.created_at_utc,
            updated_at_utc="2026-07-25T01:32:00Z",
            closed_at_utc=None,
        )
        with pytest.raises(ValueError, match="mapping/outbox coupled state conflicts"):
            repository.compare_and_swap_mapping_outbox(
                mapping=dispatching_mapping,
                outbox=mismatched_outbox,
                expected_mapping_version=999,
                expected_outbox_row_version=claimed_outbox.row_version,
                expected_lease_owner=claimed_outbox.lease_owner,
                expected_lease_epoch=claimed_outbox.lease_epoch,
                expected_lease_fence_token=claimed_outbox.lease_fence_token,
            )
        stale_owner = f"worker_repo_k2:{first_start.process_incarnation_id}"
        stale_fence = kernel_lease_fence_token_v1(
            owner_type="OUTBOX_COMMAND",
            owner_id=outbox.command_id,
            lease_epoch=claimed_outbox.lease_epoch,
            lease_owner=stale_owner,
        )
        with pytest.raises(KernelRepositoryConflict, match="expected lease"):
            repository.compare_and_swap_mapping_outbox(
                mapping=dispatching_mapping,
                outbox=dispatching_outbox,
                expected_mapping_version=1,
                expected_outbox_row_version=claimed_outbox.row_version,
                expected_lease_owner=stale_owner,
                expected_lease_epoch=claimed_outbox.lease_epoch,
                expected_lease_fence_token=stale_fence,
            )
        assert repository.compare_and_swap_mapping_outbox(
            mapping=dispatching_mapping,
            outbox=dispatching_outbox,
            expected_mapping_version=1,
            expected_outbox_row_version=claimed_outbox.row_version,
            expected_lease_owner=claimed_outbox.lease_owner,
            expected_lease_epoch=claimed_outbox.lease_epoch,
            expected_lease_fence_token=claimed_outbox.lease_fence_token,
        ) == {"mapping": dispatching_mapping, "outbox": dispatching_outbox}
        ack_receipt = BrokerCommandAckReceiptV1.create(
            command_id=outbox.command_id,
            mapping_id=mapping.mapping_id,
            deterministic_client_order_ref=outbox.deterministic_client_order_ref,
            gateway_route_id="gateway_route_repo_k2",
            gateway_catalog_sha256="3" * 64,
            source="SYNCHRONOUS_RETURN",
            accepted=True,
            broker_order_id="broker_repo_k2",
            reason_code="BROKER_ACCEPTED",
            ack_payload_sha256="4" * 64,
            observed_at_utc="2026-07-25T01:32:30Z",
        )
        accepted_mapping = ExecutionCommandChildMappingV1.create(
            command=submit_command,
            strategy_slot_id="slot_k2",
            mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
            mapping_version=3,
            broker_order_id="broker_repo_k2",
            broker_identity_source_event_id="event_broker_ack_k2",
            last_order_event_id="event_broker_ack_k2",
            last_trade_event_id=None,
            updated_by_event_id="event_broker_ack_k2",
            created_at_utc=mapping.created_at_utc,
            updated_at_utc="2026-07-25T01:32:30Z",
        )
        accepted_outbox = BrokerCommandOutboxV1.create(
            command=submit_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.ACKED,
            attempt_count=dispatching_outbox.attempt_count,
            lease_owner=None,
            lease_epoch=dispatching_outbox.lease_epoch,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=dispatching_outbox.dispatch_attempt_id,
            next_attempt_at_utc=None,
            broker_called=True,
            broker_order_id="broker_repo_k2",
            ack_receipt_json=ack_receipt,
            ack_receipt_sha256=ack_receipt.receipt_sha256,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=dispatching_outbox.row_version + 1,
            created_at_utc=dispatching_outbox.created_at_utc,
            updated_at_utc="2026-07-25T01:32:30Z",
            closed_at_utc="2026-07-25T01:32:30Z",
        )
        assert repository.compare_and_swap_mapping_outbox(
            mapping=accepted_mapping,
            outbox=accepted_outbox,
            expected_mapping_version=2,
            expected_outbox_row_version=dispatching_outbox.row_version,
            expected_lease_owner=dispatching_outbox.lease_owner,
            expected_lease_epoch=dispatching_outbox.lease_epoch,
            expected_lease_fence_token=dispatching_outbox.lease_fence_token,
        ) == {"mapping": accepted_mapping, "outbox": accepted_outbox}
        mutation = TimerMutationV1.create(
            mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
            algo_instance_id=_algo_id(),
            transition_id="transition_timer_k2",
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
        assert repository.write_timer_schedule(schedule) == schedule
        assert repository.write_timer_schedule(schedule) == schedule
        commit_unknown_mutation = TimerMutationV1.create(
            mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
            algo_instance_id=_algo_id(),
            transition_id="transition_timer_commit_unknown_k2",
            ordinal=0,
            timer_name="commit_unknown_timer",
            schedule_epoch="session_epoch_k2",
            due_at_exchange_utc="2026-07-25T02:03:00Z",
            catch_up_policy="EXPIRE_IF_LATE",
            payload={"slice": 3},
        )
        commit_unknown_schedule = ExecutionAlgoTimerScheduleV1.create(
            runtime_id="runtime_k2",
            mutation=commit_unknown_mutation,
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
        commit_unknown_repository = PostgresMiniQMTKernelRepository(conn_factory=_commit_unknown_factory(schema))
        with pytest.raises(KernelRepositoryCommitUnknown, match="not observed"):
            commit_unknown_repository.write_timer_schedule(commit_unknown_schedule)
        assert repository.read_timer_schedule(commit_unknown_schedule.schedule_id) == commit_unknown_schedule

        drift_mutation = TimerMutationV1.create(
            mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
            algo_instance_id=_algo_id(),
            transition_id="transition_timer_readback_drift_k2",
            ordinal=0,
            timer_name="readback_drift_timer",
            schedule_epoch="session_epoch_k2",
            due_at_exchange_utc="2026-07-25T02:04:00Z",
            catch_up_policy="EXPIRE_IF_LATE",
            payload={"slice": 4},
        )
        drift_schedule = ExecutionAlgoTimerScheduleV1.create(
            runtime_id="runtime_k2",
            mutation=drift_mutation,
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
        drift_repository = PostgresMiniQMTKernelRepository(
            conn_factory=_post_commit_schedule_drift_factory(schema, drift_schedule.schedule_id)
        )
        with pytest.raises(KernelRepositoryConflict, match="scalar columns drift"):
            drift_repository.write_timer_schedule(drift_schedule)
        occurrence_fence = kernel_lease_fence_token_v1(
            owner_type="TIMER_OCCURRENCE",
            owner_id=schedule.timer_occurrence_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        )
        occurrence = ExecutionAlgoTimerOccurrenceV1.create(
            schedule=schedule,
            exchange_session_authority_sha256="4" * 64,
            status=ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED,
            emitted_event_id=None,
            catch_up_receipt_sha256=None,
            lease_owner=lease_owner,
            lease_epoch=1,
            lease_fence_token=occurrence_fence,
            lease_expires_at_utc="2026-07-25T02:01:00Z",
            row_version=1,
            created_at_utc="2026-07-25T01:59:00Z",
            closed_at_utc=None,
        )
        assert repository.write_timer_occurrence(occurrence) == occurrence
        assert repository.write_timer_occurrence(occurrence) == occurrence
        tie_mutation = TimerMutationV1.create(
            mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
            algo_instance_id=_algo_id(),
            transition_id="transition_timer_tie_k2",
            ordinal=0,
            timer_name="slice_timer_tie",
            schedule_epoch="session_epoch_k2",
            due_at_exchange_utc="2026-07-25T02:00:00Z",
            catch_up_policy="EXPIRE_IF_LATE",
            payload={"slice": 22},
        )
        tie_schedule = ExecutionAlgoTimerScheduleV1.create(
            runtime_id="runtime_k2",
            mutation=tie_mutation,
            status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
            emitted_event_id=None,
            lease_owner=None,
            lease_epoch=0,
            lease_fence_token=None,
            lease_expires_at_utc=None,
            row_version=1,
            created_at_utc=schedule.created_at_utc,
            updated_at_utc=schedule.updated_at_utc,
            closed_at_utc=None,
        )
        repository.write_timer_schedule(tie_schedule)
        tie_fence = kernel_lease_fence_token_v1(
            owner_type="TIMER_OCCURRENCE",
            owner_id=tie_schedule.timer_occurrence_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        )
        tie_occurrence = ExecutionAlgoTimerOccurrenceV1.create(
            schedule=tie_schedule,
            exchange_session_authority_sha256="4" * 64,
            status=ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED,
            emitted_event_id=None,
            catch_up_receipt_sha256=None,
            lease_owner=lease_owner,
            lease_epoch=1,
            lease_fence_token=tie_fence,
            lease_expires_at_utc="2026-07-25T02:01:00Z",
            row_version=1,
            created_at_utc=occurrence.created_at_utc,
            closed_at_utc=None,
        )
        repository.write_timer_occurrence(tie_occurrence)
        recovery_occurrences = repository.list_recovery_timer_occurrences(
            runtime_id="runtime_k2", trade_date=date(2026, 7, 25), statuses=("CLAIMED",), limit=10
        )
        assert recovery_occurrences == tuple(
            sorted((occurrence, tie_occurrence), key=lambda item: item.timer_occurrence_id)
        )
        schedule_fence = kernel_lease_fence_token_v1(
            owner_type="TIMER_SCHEDULE",
            owner_id=schedule.schedule_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        )
        emitting_schedule = ExecutionAlgoTimerScheduleV1.create(
            runtime_id="runtime_k2",
            mutation=mutation,
            status=ExecutionAlgoTimerScheduleStatusV1.EMITTING,
            emitted_event_id=None,
            lease_owner=lease_owner,
            lease_epoch=1,
            lease_fence_token=schedule_fence,
            lease_expires_at_utc="2026-07-25T02:01:00Z",
            row_version=2,
            created_at_utc="2026-07-25T01:30:00Z",
            updated_at_utc="2026-07-25T01:59:00Z",
            closed_at_utc=None,
        )
        assert repository.write_timer_schedule(emitting_schedule) == emitting_schedule
        expired_occurrence = ExecutionAlgoTimerOccurrenceV1.create(
            schedule=emitting_schedule,
            exchange_session_authority_sha256="4" * 64,
            status=ExecutionAlgoTimerOccurrenceStatusV1.EXPIRED,
            emitted_event_id=None,
            catch_up_receipt_sha256=None,
            lease_owner=None,
            lease_epoch=1,
            lease_fence_token=None,
            lease_expires_at_utc=None,
            row_version=2,
            created_at_utc="2026-07-25T01:59:00Z",
            closed_at_utc="2026-07-25T02:02:00Z",
        )
        assert repository.write_timer_occurrence(expired_occurrence) == expired_occurrence

        with pytest.raises(TypeError):
            repository.append_dispatch_attempt("bad")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            repository.write_timer_schedule("bad")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            repository.write_timer_occurrence("bad")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            repository.write_exchange_session_authority("bad")  # type: ignore[arg-type]

        authority = ExchangeSessionAuthorityV1.create(
            runtime_id="runtime_k2",
            exchange_trade_date="2026-07-25",
            **_calendar_authority_values(snapshot_set_id="calendar_set_repo_k2"),
        )
        assert repository.write_exchange_session_authority(authority) == authority
        assert repository.write_exchange_session_authority(authority) == authority
        with raw.cursor() as cur:
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES (%s,%s)",
                ("runtime_session_mismatch", date(2026, 7, 26)),
            )
        mismatched_date_authority = ExchangeSessionAuthorityV1.create(
            runtime_id="runtime_session_mismatch",
            exchange_trade_date="2026-07-25",
            **_calendar_authority_values(snapshot_set_id="calendar_set_repo_mismatch_k2"),
        )
        with pytest.raises(KernelRepositoryConflict, match="trade date conflicts"):
            repository.write_exchange_session_authority(mismatched_date_authority)
        missing_runtime_authority = ExchangeSessionAuthorityV1.create(
            runtime_id="missing_runtime_authority",
            exchange_trade_date="2026-07-25",
            **_calendar_authority_values(snapshot_set_id="calendar_set_repo_missing_runtime_k2"),
        )
        with pytest.raises(KeyError):
            repository.write_exchange_session_authority(missing_runtime_authority)
        with pytest.raises(TypeError):
            repository.read_exchange_session_authority(
                runtime_id="runtime_k2",
                exchange_trade_date="2026-07-25",  # type: ignore[arg-type]
            )

        event = _event(last_price="10.000000")
        delivery = _delivery(event)
        receipt = repository.write_event_receipt_deliveries(event=event, deliveries=(delivery,))
        assert repository.write_event_receipt_deliveries(event=event, deliveries=(delivery,)) == receipt
        assert repository.read_event_transaction(event.event_id)["event"] == event
        with pytest.raises(TypeError):
            repository.write_event_receipt_deliveries(event="bad", deliveries=())  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            repository.write_event_receipt_deliveries(event=event, deliveries=("bad",))  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="owner conflicts"):
            repository.write_event_receipt_deliveries(
                event=event,
                deliveries=(_delivery(_tick_event(), algo_instance_id="other_algo"),),
            )
        with pytest.raises(KeyError):
            repository.read_event_transaction("missing_event")

        conflicting_event = _event(last_price="10.010000")
        with pytest.raises(KernelRepositoryConflict, match="different immutable"):
            repository.write_event_receipt_deliveries(
                event=conflicting_event,
                deliveries=(_delivery(conflicting_event),),
            )
        assert repository.read_event_transaction(event.event_id)["event"] == event

        algo_event = RuntimeEventEnvelopeV2.create(
            runtime_id="runtime_k2",
            sequence=2,
            event_type=EventTypeV2.TICK,
            event_time_utc="2026-07-25T01:33:00Z",
            monotonic_ns=None,
            source=EventSourceV2.B0_QUOTE_V2,
            symbol="600000.SH",
            payload_schema_version="miniqmt_market_data_view_v2",
            payload={"last_price": "10.010000"},
            source_identity={"market_data_id": "market_data_failure_k2"},
            correlation={"trace_id": "trace_failure_k2"},
        )
        algo_delivery = _delivery(
            algo_event,
            algo_instance_id=_algo_id(),
            algo_delivery_sequence=2,
            previous_delivery_id=submit_delivery.delivery_id,
        )
        algo_event_receipt = repository.write_event_receipt_deliveries(
            event=algo_event,
            deliveries=(algo_delivery,),
        )
        delivery_fence = kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=algo_delivery.delivery_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        )
        claimed_delivery = repository.claim_delivery(
            delivery_id=algo_delivery.delivery_id,
            lease_owner=lease_owner,
            lease_epoch=1,
            lease_fence_token=delivery_fence,
            lease_expires_at="2026-07-25T01:40:00Z",
            updated_at_utc="2026-07-25T01:32:00Z",
            expected_row_version=1,
        )
        assert (
            repository.write_event_receipt_deliveries(event=algo_event, deliveries=(algo_delivery,))
            == algo_event_receipt
        )
        assert repository.list_recovery_deliveries(
            runtime_id="runtime_k2", trade_date=date(2026, 7, 25), statuses=("CLAIMED",), limit=10
        ) == (claimed_delivery,)

        evidence = KernelErrorEvidenceV1.create(
            stage="DELIVERY_APPLY",
            stable_reason_code="MINIQMT_ALGO_TRANSITION_FAILED",
            exception=RuntimeError("plugin failed"),
            message="plugin failed",
            retryable=False,
            terminal=True,
            broker_called=None,
            primary_context={"runtime_id": "runtime_k2", "algo_instance_id": _algo_id()},
            secondary_errors=(),
        )
        failure_context = {"primary": evidence.model_dump(mode="json")}
        failure_values = {
            "delivery_id": claimed_delivery.delivery_id,
            "event_id": algo_event.event_id,
            "runtime_id": "runtime_k2",
            "algo_instance_id": _algo_id(),
            "plugin_id": "aistock.twap",
            "plugin_version": "1.0.0",
            "plugin_manifest_sha256": "1" * 64,
            "transition_sequence": 2,
            "stable_reason_code": "MINIQMT_ALGO_TRANSITION_FAILED",
            "exception_type": "builtins.RuntimeError",
            "message": "plugin failed",
            "last_good_state_sha256_or_ABSENT_INITIAL_STATE": algo_v2.state_sha256,
            "ordered_active_child_ids": (mapping.child_order_id,),
            "active_child_closure_status": "CANCEL_PENDING",
        }
        seed_failure_receipt = AlgoFailureReceiptV1.create(
            context=failure_context,
            **failure_values,
            ordered_cancel_command_ids=(),
            transaction_commit_identity="mqtx_pending_failure_seed_repo_k2",
        )

        def cancel_outbox(broker_order_id: str) -> BrokerCommandOutboxV1:
            command = BrokerCommandV2.create(
                command_type=BrokerCommandTypeV2.CANCEL_ORDER,
                runtime_id="runtime_k2",
                algo_instance_id=_algo_id(),
                parent_intent_id="intent_k2",
                transition_id=seed_failure_receipt.failure_receipt_id,
                ordinal=0,
                local_vt_orderid=mapping.local_vt_orderid,
                symbol=mapping.symbol,
                side=mapping.side,
                order_type=OrderTypeV1.LIMIT,
                price_decimal=mapping.requested_price_decimal,
                quantity=mapping.requested_quantity,
                owned_broker_order_id=broker_order_id,
                reason_code="MINIQMT_CANCEL_ACTIVE_CHILD_AFTER_FAILURE",
                metadata={"child_order_id": mapping.child_order_id},
            )
            return BrokerCommandOutboxV1.create(
                command=command,
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
                created_at_utc="2026-07-25T01:33:00Z",
                updated_at_utc="2026-07-25T01:33:00Z",
                closed_at_utc=None,
            )

        def failure_receipt_for(cancel: BrokerCommandOutboxV1) -> AlgoFailureReceiptV1:
            provisional = AlgoFailureReceiptV1.create(
                context=failure_context,
                **failure_values,
                ordered_cancel_command_ids=(cancel.command_id,),
                transaction_commit_identity="mqtx_pending_failure_repo_k2",
            )
            return AlgoFailureReceiptV1.create(
                context=failure_context,
                **failure_values,
                ordered_cancel_command_ids=(cancel.command_id,),
                transaction_commit_identity=transaction_commit_identity_v1(
                    operation="WRITE_FAILED_TERMINAL_TRANSITION_BUNDLE",
                    owner_identities=(
                        "runtime_k2",
                        _algo_id(),
                        algo_event.event_id,
                        claimed_delivery.delivery_id,
                    ),
                    input_hashes=("1" * 64, provisional.context_sha256),
                    output_identities=(provisional.failure_receipt_id, cancel.command_id),
                ),
            )

        wrong_cancel_outbox = cancel_outbox("wrong_broker_repo_k2")
        wrong_failure_receipt = failure_receipt_for(wrong_cancel_outbox)
        cancel_command_outbox = cancel_outbox("broker_repo_k2")
        failure_receipt = failure_receipt_for(cancel_command_outbox)
        failed_delivery_payload = claimed_delivery.model_dump(mode="python")
        failed_delivery_payload.update(
            status=DeliveryStatusV1.FAILED_TERMINAL,
            lease_owner=None,
            lease_fence_token=None,
            lease_expires_at=None,
            last_error_json=evidence.model_dump(mode="json"),
            failure_receipt_id=failure_receipt.failure_receipt_id,
            row_version=3,
            updated_at_utc="2026-07-25T01:33:00Z",
            closed_at_utc="2026-07-25T01:33:00Z",
        )
        failed_delivery = AlgoDeliveryPersistenceV1.model_validate(failed_delivery_payload)
        current_algo = repository.read_algo_instance(_algo_id())
        failed_algo_payload = current_algo.model_dump(mode="python")
        failed_algo_payload.update(
            status=ExecutionAlgoPersistenceStatusV2.FAILED,
            failure_receipt_id=failure_receipt.failure_receipt_id,
            active_child_closure_status=ActiveChildClosureStatusV1.CANCEL_PENDING,
            active_child_count=1,
            row_version=current_algo.row_version + 1,
            transition_sequence=2,
            last_closed_delivery_sequence=2,
            terminal_delivery_sequence=2,
            updated_at_utc="2026-07-25T01:33:00Z",
            terminal_at_utc="2026-07-25T01:33:00Z",
        )
        failed_algo = ExecutionAlgoInstancePersistenceV2.model_validate(failed_algo_payload)
        with pytest.raises(ValueError, match="CANCEL broker order identity conflicts with durable mapping"):
            repository.write_transition_bundle(
                algo_instance=failed_algo,
                delivery=failed_delivery,
                receipt=wrong_failure_receipt,
                projection_set=None,
                after_state=None,
                expected_algo_row_version=999,
                expected_delivery_row_version=2,
                command_outboxes=(wrong_cancel_outbox,),
            )
        transition_bundle = repository.write_transition_bundle(
            algo_instance=failed_algo,
            delivery=failed_delivery,
            receipt=failure_receipt,
            projection_set=None,
            after_state=None,
            expected_algo_row_version=current_algo.row_version,
            expected_delivery_row_version=2,
            command_outboxes=(cancel_command_outbox,),
        )
        assert transition_bundle["receipt"] == failure_receipt
        assert transition_bundle["new_child_mappings"] == ()
        assert transition_bundle["command_outboxes"] == (cancel_command_outbox,)
        assert (
            repository.write_transition_bundle(
                algo_instance=failed_algo,
                delivery=failed_delivery,
                receipt=failure_receipt,
                projection_set=None,
                after_state=None,
                expected_algo_row_version=current_algo.row_version,
                expected_delivery_row_version=2,
                command_outboxes=(cancel_command_outbox,),
            )
            == transition_bundle
        )

        cancel_command = BrokerCommandV2.model_validate_json(
            json.dumps(
                cancel_command_outbox.model_dump(mode="json")["payload_json"],
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        cancel_fence = kernel_lease_fence_token_v1(
            owner_type="OUTBOX_COMMAND",
            owner_id=cancel_command.command_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        )
        claimed_cancel = repository.claim_outbox_command(
            command_id=cancel_command.command_id,
            lease_owner=lease_owner,
            lease_epoch=1,
            lease_fence_token=cancel_fence,
            lease_expires_at="2026-07-25T01:45:00Z",
            updated_at_utc="2026-07-25T01:34:00Z",
            expected_row_version=1,
        )
        cancel_attempt = BrokerDispatchAttemptV1.create(
            command_id=cancel_command.command_id,
            attempt_count=claimed_cancel.attempt_count,
            lease_epoch=claimed_cancel.lease_epoch,
            lease_fence_token=claimed_cancel.lease_fence_token,
            process_incarnation_id=second_start.process_incarnation_id,
            stage="CLAIMED",
            started_at_utc="2026-07-25T01:34:00Z",
            finished_at_utc=None,
            pre_call_complete=False,
            broker_called=None,
            outcome=None,
            error_reason_code=None,
            error_context_sha256=None,
            authority_receipt_sha256=None,
        )
        repository.append_dispatch_attempt(cancel_attempt)
        dispatching_cancel = BrokerCommandOutboxV1.create(
            command=cancel_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.DISPATCHING,
            attempt_count=claimed_cancel.attempt_count,
            lease_owner=claimed_cancel.lease_owner,
            lease_epoch=claimed_cancel.lease_epoch,
            lease_fence_token=claimed_cancel.lease_fence_token,
            lease_expires_at=claimed_cancel.lease_expires_at,
            dispatch_attempt_id=cancel_attempt.dispatch_attempt_id,
            next_attempt_at_utc=None,
            broker_called=None,
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=claimed_cancel.row_version + 1,
            created_at_utc=claimed_cancel.created_at_utc,
            updated_at_utc="2026-07-25T01:34:10Z",
            closed_at_utc=None,
        )
        assert repository.compare_and_swap_mapping_outbox(
            mapping=accepted_mapping,
            outbox=dispatching_cancel,
            expected_mapping_version=accepted_mapping.mapping_version,
            expected_outbox_row_version=claimed_cancel.row_version,
            expected_lease_owner=claimed_cancel.lease_owner,
            expected_lease_epoch=claimed_cancel.lease_epoch,
            expected_lease_fence_token=claimed_cancel.lease_fence_token,
        ) == {"mapping": accepted_mapping, "outbox": dispatching_cancel}

        accepted_cancel_ack = BrokerCommandAckReceiptV1.create(
            command_id=cancel_command.command_id,
            mapping_id=mapping.mapping_id,
            deterministic_client_order_ref=dispatching_cancel.deterministic_client_order_ref,
            gateway_route_id="gateway_route_cancel_callback_k2",
            gateway_catalog_sha256="6" * 64,
            source="SYNCHRONOUS_RETURN",
            accepted=True,
            broker_order_id=accepted_mapping.broker_order_id,
            reason_code="CANCEL_ACCEPTED_AWAITING_ORDER_CALLBACK",
            ack_payload_sha256="7" * 64,
            observed_at_utc="2026-07-25T01:34:15Z",
        )
        accepted_cancel_outbox = BrokerCommandOutboxV1.create(
            command=cancel_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.ACKED,
            attempt_count=dispatching_cancel.attempt_count,
            lease_owner=None,
            lease_epoch=dispatching_cancel.lease_epoch,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=dispatching_cancel.dispatch_attempt_id,
            next_attempt_at_utc=None,
            broker_called=True,
            broker_order_id=accepted_mapping.broker_order_id,
            ack_receipt_json=accepted_cancel_ack,
            ack_receipt_sha256=accepted_cancel_ack.receipt_sha256,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=dispatching_cancel.row_version + 1,
            created_at_utc=dispatching_cancel.created_at_utc,
            updated_at_utc="2026-07-25T01:34:15Z",
            closed_at_utc="2026-07-25T01:34:15Z",
        )

        def clone_cancel_callback_state() -> tuple[str, PostgresMiniQMTKernelRepository]:
            clone_schema = _fixture_schema()
            callback_schemas.append(clone_schema)
            with raw.cursor() as cur:
                cur.execute(f"CREATE SCHEMA {clone_schema}")
                for table_name in (
                    "execution_runtime_event",
                    "execution_algo_event_delivery",
                    "execution_algo_instance",
                    "execution_child_order",
                    "execution_algo_command_outbox",
                ):
                    cur.execute(f"CREATE TABLE {clone_schema}.{table_name} (LIKE {schema}.{table_name} INCLUDING ALL)")
                    cur.execute(f"INSERT INTO {clone_schema}.{table_name} SELECT * FROM {schema}.{table_name}")
            return clone_schema, PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(clone_schema))

        def callback_event(*, sequence: int, broker_order_id: str, suffix: str) -> RuntimeEventEnvelopeV2:
            return RuntimeEventEnvelopeV2.create(
                runtime_id="runtime_k2",
                sequence=sequence,
                event_type=EventTypeV2.ORDER,
                event_time_utc="2026-07-25T01:34:30Z",
                monotonic_ns=None,
                source=EventSourceV2.QMT_GATEWAY_CALLBACK,
                symbol=accepted_mapping.symbol,
                payload_schema_version="miniqmt_order_event_v1",
                payload={
                    "runtime_id": accepted_mapping.runtime_id,
                    "algo_instance_id": accepted_mapping.algo_instance_id,
                    "parent_intent_id": accepted_mapping.parent_intent_id,
                    "mapping_id": accepted_mapping.mapping_id,
                    "local_vt_orderid": accepted_mapping.local_vt_orderid,
                    "broker_order_id": broker_order_id,
                    "terminal": True,
                },
                source_identity={"order_event_id": f"order_callback_{suffix}_k2"},
                correlation={"trace_id": f"trace_callback_{suffix}_k2"},
            )

        def terminal_mapping_for(event_id: str) -> ExecutionCommandChildMappingV1:
            return ExecutionCommandChildMappingV1.create(
                command=submit_command,
                strategy_slot_id="slot_k2",
                mapping_status=CommandChildMappingStatusV1.TERMINAL,
                mapping_version=accepted_mapping.mapping_version + 1,
                broker_order_id=accepted_mapping.broker_order_id,
                broker_identity_source_event_id=accepted_mapping.broker_identity_source_event_id,
                last_order_event_id=event_id,
                last_trade_event_id=None,
                updated_by_event_id=event_id,
                created_at_utc=accepted_mapping.created_at_utc,
                updated_at_utc="2026-07-25T01:34:30Z",
            )

        _, sync_callback_repository = clone_cancel_callback_state()
        sync_callback_repository.compare_and_swap_mapping_outbox(
            mapping=accepted_mapping,
            outbox=accepted_cancel_outbox,
            expected_mapping_version=accepted_mapping.mapping_version,
            expected_outbox_row_version=dispatching_cancel.row_version,
            expected_lease_owner=dispatching_cancel.lease_owner,
            expected_lease_epoch=dispatching_cancel.lease_epoch,
            expected_lease_fence_token=dispatching_cancel.lease_fence_token,
        )
        wrong_event = callback_event(sequence=30, broker_order_id="wrong_broker_callback_k2", suffix="wrong")
        sync_callback_repository.write_event_receipt_deliveries(event=wrong_event, deliveries=())
        with pytest.raises(ValueError, match="callback event identity conflicts"):
            sync_callback_repository.close_mapping_from_callback(
                mapping=terminal_mapping_for(wrong_event.event_id),
                callback_event=wrong_event,
                cancel_command_id=cancel_command.command_id,
                expected_mapping_version=accepted_mapping.mapping_version,
                expected_algo_row_version=sync_callback_repository.read_algo_instance(_algo_id()).row_version,
            )
        order_event = callback_event(
            sequence=31,
            broker_order_id=accepted_mapping.broker_order_id,
            suffix="sync_then_callback",
        )
        sync_callback_repository.write_event_receipt_deliveries(event=order_event, deliveries=())
        callback_terminal_mapping = terminal_mapping_for(order_event.event_id)
        pre_callback_algo = sync_callback_repository.read_algo_instance(_algo_id())
        pre_callback_chain = sync_callback_repository.read_command_identity_chain(cancel_command.command_id)
        for fault_point in ("mapping", "algo", "callback_readback"):
            fault_repository = PostgresMiniQMTKernelRepository(
                conn_factory=_fault_injection_factory(callback_schemas[-1], fault_point)
            )
            with pytest.raises(RuntimeError, match=f"injected {fault_point} write failure"):
                fault_repository.close_mapping_from_callback(
                    mapping=callback_terminal_mapping,
                    callback_event=order_event,
                    cancel_command_id=cancel_command.command_id,
                    expected_mapping_version=accepted_mapping.mapping_version,
                    expected_algo_row_version=pre_callback_algo.row_version,
                )
            assert sync_callback_repository.read_command_identity_chain(cancel_command.command_id) == pre_callback_chain
            assert sync_callback_repository.read_algo_instance(_algo_id()) == pre_callback_algo
        callback_result = sync_callback_repository.close_mapping_from_callback(
            mapping=callback_terminal_mapping,
            callback_event=order_event,
            cancel_command_id=cancel_command.command_id,
            expected_mapping_version=accepted_mapping.mapping_version,
            expected_algo_row_version=pre_callback_algo.row_version,
        )
        assert callback_result["mapping"] == callback_terminal_mapping
        assert callback_result["outbox"] == accepted_cancel_outbox
        assert callback_result["algo"].status is ExecutionAlgoPersistenceStatusV2.FAILED
        assert callback_result["algo"].failure_receipt_id == pre_callback_algo.failure_receipt_id
        assert callback_result["algo"].terminal_delivery_sequence == pre_callback_algo.terminal_delivery_sequence
        assert callback_result["algo"].active_child_count == 0
        assert callback_result["algo"].active_child_closure_status is ActiveChildClosureStatusV1.CLEAN
        assert (
            sync_callback_repository.close_mapping_from_callback(
                mapping=callback_terminal_mapping,
                callback_event=order_event,
                cancel_command_id=cancel_command.command_id,
                expected_mapping_version=callback_terminal_mapping.mapping_version,
                expected_algo_row_version=callback_result["algo"].row_version,
            )
            == callback_result
        )

        _, early_callback_repository = clone_cancel_callback_state()
        early_event = callback_event(
            sequence=32,
            broker_order_id=accepted_mapping.broker_order_id,
            suffix="callback_then_sync",
        )
        early_callback_repository.write_event_receipt_deliveries(event=early_event, deliveries=())
        early_terminal_mapping = terminal_mapping_for(early_event.event_id)
        early_algo = early_callback_repository.read_algo_instance(_algo_id())
        early_result = early_callback_repository.close_mapping_from_callback(
            mapping=early_terminal_mapping,
            callback_event=early_event,
            cancel_command_id=cancel_command.command_id,
            expected_mapping_version=accepted_mapping.mapping_version,
            expected_algo_row_version=early_algo.row_version,
        )
        assert early_result["outbox"] == dispatching_cancel
        assert early_callback_repository.compare_and_swap_mapping_outbox(
            mapping=early_terminal_mapping,
            outbox=accepted_cancel_outbox,
            expected_mapping_version=early_terminal_mapping.mapping_version,
            expected_outbox_row_version=dispatching_cancel.row_version,
            expected_lease_owner=dispatching_cancel.lease_owner,
            expected_lease_epoch=dispatching_cancel.lease_epoch,
            expected_lease_fence_token=dispatching_cancel.lease_fence_token,
        ) == {"mapping": early_terminal_mapping, "outbox": accepted_cancel_outbox}

        rejected_ack = BrokerCommandAckReceiptV1.create(
            command_id=cancel_command.command_id,
            mapping_id=mapping.mapping_id,
            deterministic_client_order_ref=dispatching_cancel.deterministic_client_order_ref,
            gateway_route_id="gateway_route_cancel_repo_k2",
            gateway_catalog_sha256="9" * 64,
            source="SYNCHRONOUS_RETURN",
            accepted=False,
            broker_order_id=None,
            reason_code="BROKER_CANCEL_REJECTED",
            ack_payload_sha256="a" * 64,
            observed_at_utc="2026-07-25T01:34:20Z",
        )
        rejected_cancel = BrokerCommandOutboxV1.create(
            command=cancel_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.ACKED_REJECTED,
            attempt_count=dispatching_cancel.attempt_count,
            lease_owner=None,
            lease_epoch=dispatching_cancel.lease_epoch,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=dispatching_cancel.dispatch_attempt_id,
            next_attempt_at_utc=None,
            broker_called=True,
            broker_order_id=None,
            ack_receipt_json=rejected_ack,
            ack_receipt_sha256=rejected_ack.receipt_sha256,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=dispatching_cancel.row_version + 1,
            created_at_utc=dispatching_cancel.created_at_utc,
            updated_at_utc="2026-07-25T01:34:20Z",
            closed_at_utc="2026-07-25T01:34:20Z",
        )
        algo_before_rejected_cas = repository.read_algo_instance(_algo_id())
        with pytest.raises(KernelRepositoryConflict, match="outbox CAS failed"):
            repository.compare_and_swap_mapping_outbox(
                mapping=accepted_mapping,
                outbox=rejected_cancel,
                expected_mapping_version=accepted_mapping.mapping_version,
                expected_outbox_row_version=999,
                expected_lease_owner=dispatching_cancel.lease_owner,
                expected_lease_epoch=dispatching_cancel.lease_epoch,
                expected_lease_fence_token=dispatching_cancel.lease_fence_token,
            )
        assert repository.read_command_identity_chain(cancel_command.command_id) == {
            "mapping": accepted_mapping,
            "outbox": dispatching_cancel,
        }
        assert repository.read_algo_instance(_algo_id()) == algo_before_rejected_cas
        assert algo_before_rejected_cas.status is ExecutionAlgoPersistenceStatusV2.FAILED

        unknown_receipt = BrokerUnknownOutcomeReceiptV1.create(
            command_id=cancel_command.command_id,
            dispatch_attempt_id=cancel_attempt.dispatch_attempt_id,
            mapping_id=mapping.mapping_id,
            lease_fence_token=cancel_fence,
            uncertain_stage="GATEWAY_RETURN",
            callback_watermark="callback_watermark_cancel_k2",
            reason_code="MINIQMT_CANCEL_OUTCOME_UNKNOWN",
            observed_at_utc="2026-07-25T01:34:30Z",
        )
        unknown_cancel = BrokerCommandOutboxV1.create(
            command=cancel_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
            attempt_count=dispatching_cancel.attempt_count,
            lease_owner=None,
            lease_epoch=dispatching_cancel.lease_epoch,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=dispatching_cancel.dispatch_attempt_id,
            next_attempt_at_utc=None,
            broker_called=None,
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=unknown_receipt,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=dispatching_cancel.row_version + 1,
            created_at_utc=dispatching_cancel.created_at_utc,
            updated_at_utc="2026-07-25T01:34:30Z",
            closed_at_utc=None,
        )
        repository.compare_and_swap_mapping_outbox(
            mapping=accepted_mapping,
            outbox=unknown_cancel,
            expected_mapping_version=accepted_mapping.mapping_version,
            expected_outbox_row_version=dispatching_cancel.row_version,
            expected_lease_owner=dispatching_cancel.lease_owner,
            expected_lease_epoch=dispatching_cancel.lease_epoch,
            expected_lease_fence_token=dispatching_cancel.lease_fence_token,
        )
        not_found = BrokerOutcomeReconciliationReceiptV1.create(
            command_id=cancel_command.command_id,
            reconcile_attempt=1,
            query_criteria_sha256="b" * 64,
            callback_watermark="callback_watermark_cancel_k2",
            ordered_matched_order_ids=(),
            ordered_matched_trade_ids=(),
            order_snapshot_sha256="c" * 64,
            trade_snapshot_sha256="d" * 64,
            outcome="NOT_FOUND",
            broker_called=None,
            broker_order_id=None,
            reason_code="CANCEL_NOT_FOUND_YET",
            observed_at_utc="2026-07-25T01:34:40Z",
        )
        reconciling_cancel = BrokerCommandOutboxV1.create(
            command=cancel_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.RECONCILING,
            attempt_count=unknown_cancel.attempt_count,
            lease_owner=None,
            lease_epoch=unknown_cancel.lease_epoch,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=unknown_cancel.dispatch_attempt_id,
            next_attempt_at_utc=None,
            broker_called=None,
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=unknown_receipt,
            reconcile_receipt=not_found,
            last_error_json=None,
            row_version=unknown_cancel.row_version + 1,
            created_at_utc=unknown_cancel.created_at_utc,
            updated_at_utc="2026-07-25T01:34:40Z",
            closed_at_utc=None,
        )
        repository.compare_and_swap_mapping_outbox(
            mapping=accepted_mapping,
            outbox=reconciling_cancel,
            expected_mapping_version=accepted_mapping.mapping_version,
            expected_outbox_row_version=unknown_cancel.row_version,
            expected_lease_owner=unknown_cancel.lease_owner,
            expected_lease_epoch=unknown_cancel.lease_epoch,
            expected_lease_fence_token=unknown_cancel.lease_fence_token,
        )
        terminal_event_id = "event_cancel_terminal_k2"
        terminal_mapping = ExecutionCommandChildMappingV1.create(
            command=submit_command,
            strategy_slot_id="slot_k2",
            mapping_status=CommandChildMappingStatusV1.TERMINAL,
            mapping_version=accepted_mapping.mapping_version + 1,
            broker_order_id=accepted_mapping.broker_order_id,
            broker_identity_source_event_id=accepted_mapping.broker_identity_source_event_id,
            last_order_event_id=terminal_event_id,
            last_trade_event_id=None,
            updated_by_event_id=terminal_event_id,
            created_at_utc=accepted_mapping.created_at_utc,
            updated_at_utc="2026-07-25T01:35:00Z",
        )
        reconciled = BrokerOutcomeReconciliationReceiptV1.create(
            command_id=cancel_command.command_id,
            reconcile_attempt=2,
            query_criteria_sha256="e" * 64,
            callback_watermark="callback_watermark_cancel_k2_final",
            ordered_matched_order_ids=(accepted_mapping.broker_order_id,),
            ordered_matched_trade_ids=(),
            order_snapshot_sha256="f" * 64,
            trade_snapshot_sha256="0" * 64,
            outcome="UNIQUE_ACCEPTED",
            broker_called=True,
            broker_order_id=accepted_mapping.broker_order_id,
            reason_code="CANCEL_TERMINAL_CONFIRMED",
            observed_at_utc="2026-07-25T01:35:00Z",
        )
        terminal_ack = BrokerCommandAckReceiptV1.create(
            command_id=cancel_command.command_id,
            mapping_id=mapping.mapping_id,
            deterministic_client_order_ref=reconciling_cancel.deterministic_client_order_ref,
            gateway_route_id="gateway_route_cancel_repo_k2",
            gateway_catalog_sha256="1" * 64,
            source="RECONCILIATION",
            accepted=True,
            broker_order_id=accepted_mapping.broker_order_id,
            reason_code="CANCEL_TERMINAL_CONFIRMED",
            ack_payload_sha256="2" * 64,
            observed_at_utc="2026-07-25T01:35:00Z",
        )
        terminal_cancel = BrokerCommandOutboxV1.create(
            command=cancel_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.ACKED,
            attempt_count=reconciling_cancel.attempt_count,
            lease_owner=None,
            lease_epoch=reconciling_cancel.lease_epoch,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=reconciling_cancel.dispatch_attempt_id,
            next_attempt_at_utc=None,
            broker_called=True,
            broker_order_id=accepted_mapping.broker_order_id,
            ack_receipt_json=terminal_ack,
            ack_receipt_sha256=terminal_ack.receipt_sha256,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=unknown_receipt,
            reconcile_receipt=reconciled,
            last_error_json=None,
            row_version=reconciling_cancel.row_version + 1,
            created_at_utc=reconciling_cancel.created_at_utc,
            updated_at_utc="2026-07-25T01:35:00Z",
            closed_at_utc="2026-07-25T01:35:00Z",
        )
        pre_terminal_chain = repository.read_command_identity_chain(cancel_command.command_id)
        pre_terminal_algo = repository.read_algo_instance(_algo_id())
        for fault_point in ("mapping", "outbox", "algo"):
            fault_repository = PostgresMiniQMTKernelRepository(
                conn_factory=_fault_injection_factory(schema, fault_point)
            )
            with pytest.raises(RuntimeError, match=f"injected {fault_point} write failure"):
                fault_repository.compare_and_swap_mapping_outbox(
                    mapping=terminal_mapping,
                    outbox=terminal_cancel,
                    expected_mapping_version=accepted_mapping.mapping_version,
                    expected_outbox_row_version=reconciling_cancel.row_version,
                    expected_lease_owner=reconciling_cancel.lease_owner,
                    expected_lease_epoch=reconciling_cancel.lease_epoch,
                    expected_lease_fence_token=reconciling_cancel.lease_fence_token,
                )
            assert repository.read_command_identity_chain(cancel_command.command_id) == pre_terminal_chain
            assert repository.read_algo_instance(_algo_id()) == pre_terminal_algo
        repository.compare_and_swap_mapping_outbox(
            mapping=terminal_mapping,
            outbox=terminal_cancel,
            expected_mapping_version=accepted_mapping.mapping_version,
            expected_outbox_row_version=reconciling_cancel.row_version,
            expected_lease_owner=reconciling_cancel.lease_owner,
            expected_lease_epoch=reconciling_cancel.lease_epoch,
            expected_lease_fence_token=reconciling_cancel.lease_fence_token,
        )
        closed_algo = repository.read_algo_instance(_algo_id())
        assert closed_algo.status is ExecutionAlgoPersistenceStatusV2.FAILED
        assert closed_algo.active_child_closure_status is ActiveChildClosureStatusV1.CLEAN
        assert closed_algo.active_child_count == 0
        assert closed_algo.failure_receipt_id == failed_algo.failure_receipt_id
        assert closed_algo.terminal_delivery_sequence == failed_algo.terminal_delivery_sequence

        invalid_claim_fence = kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=delivery.delivery_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        )
        with pytest.raises(KernelRepositoryConflict, match="unknown worker incarnation"):
            repository.claim_delivery(
                delivery_id=delivery.delivery_id,
                lease_owner="worker_repo_k2:missing_incarnation",
                lease_epoch=1,
                lease_fence_token=invalid_claim_fence,
                lease_expires_at="2026-07-25T01:35:00Z",
                updated_at_utc="2026-07-25T01:31:00Z",
                expected_row_version=1,
            )
        with pytest.raises(ValueError, match="worker_id:process_incarnation_id"):
            repository.claim_delivery(
                delivery_id=delivery.delivery_id,
                lease_owner="malformed_owner",
                lease_epoch=1,
                lease_fence_token=invalid_claim_fence,
                lease_expires_at="2026-07-25T01:35:00Z",
                updated_at_utc="2026-07-25T01:31:00Z",
                expected_row_version=1,
            )
        with pytest.raises(KeyError):
            repository.claim_delivery(
                delivery_id="missing_delivery",
                lease_owner=lease_owner,
                lease_epoch=2,
                lease_fence_token=invalid_claim_fence,
                lease_expires_at="2026-07-25T01:35:00Z",
                updated_at_utc="2026-07-25T01:31:00Z",
                expected_row_version=1,
            )
        assert (
            repository.list_recovery_timer_occurrences(
                runtime_id="missing_runtime", trade_date=date(2026, 7, 25), statuses=("CLAIMED",), limit=10
            )
            == ()
        )
        with pytest.raises(ValueError, match="non-empty"):
            repository.list_recovery_outbox_commands(
                runtime_id="runtime_k2", trade_date=date(2026, 7, 25), statuses=(), limit=10
            )
        with pytest.raises(ValueError, match="unsupported recovery statuses"):
            repository.list_recovery_outbox_commands(
                runtime_id="runtime_k2", trade_date=date(2026, 7, 25), statuses=("PENDNG",), limit=10
            )
        with pytest.raises(ValueError, match="duplicates"):
            repository.list_recovery_deliveries(
                runtime_id="runtime_k2", trade_date=date(2026, 7, 25), statuses=("PENDING", "PENDING"), limit=10
            )
        with pytest.raises(ValueError, match="strict string"):
            repository.list_recovery_timer_occurrences(
                runtime_id="runtime_k2",
                trade_date=date(2026, 7, 25),
                statuses=(1,),  # type: ignore[arg-type]
                limit=10,
            )
        with pytest.raises(ValueError, match="trim-stable strict string"):
            repository.list_recovery_deliveries(
                runtime_id=" runtime_k2",
                trade_date=date(2026, 7, 25),
                statuses=("PENDING",),
                limit=10,
            )
        with pytest.raises(ValueError, match=r"\[1, 1000\]"):
            repository.list_recovery_deliveries(
                runtime_id="runtime_repo_k2", trade_date=date(2026, 7, 25), statuses=("PENDING",), limit=0
            )
        with pytest.raises(KeyError):
            repository.read_delivery("missing_delivery")
        with pytest.raises(KeyError):
            repository.read_outbox_command("missing_outbox")
        with pytest.raises(KeyError):
            repository.read_algo_instance("missing_algo")
        with pytest.raises(KeyError):
            repository.read_dispatch_attempt("missing_attempt", "CLAIMED")
        with pytest.raises(KeyError):
            repository.read_timer_schedule("missing_schedule")
        with pytest.raises(KeyError):
            repository.read_timer_occurrence("missing_occurrence")
        with pytest.raises(KeyError):
            repository.read_exchange_session_authority(
                runtime_id="missing_runtime", exchange_trade_date=date(2026, 7, 25)
            )

        exact_algo = repository.read_algo_instance(_algo_id())
        exact_schedule = repository.read_timer_schedule(emitting_schedule.schedule_id)
        exact_occurrence = repository.read_timer_occurrence(tie_occurrence.timer_occurrence_id)
        terminal_cancel_json = terminal_cancel.model_dump(mode="json")
        scalar_drift_cases = (
            (
                f"UPDATE {schema}.execution_runtime_event SET sequence=sequence+1000 WHERE event_id=%s",
                (submit_event.event_id,),
                lambda: repository.read_event_transaction(submit_event.event_id),
                f"UPDATE {schema}.execution_runtime_event SET sequence=sequence-1000 WHERE event_id=%s",
                (submit_event.event_id,),
            ),
            (
                f"UPDATE {schema}.execution_algo_transition SET transition_sequence=transition_sequence+1000 "
                "WHERE transition_id=%s",
                (transition_receipt.transition_id,),
                lambda: repository.read_transition_bundle(transition_receipt.transition_id),
                f"UPDATE {schema}.execution_algo_transition SET transition_sequence=transition_sequence-1000 "
                "WHERE transition_id=%s",
                (transition_receipt.transition_id,),
            ),
            (
                f"UPDATE {schema}.execution_kernel_worker_incarnation "
                "SET source_revision=source_revision || '_drift' WHERE process_incarnation_id=%s",
                (first_start.process_incarnation_id,),
                lambda: repository.read_worker_startup_receipt(first_start.process_incarnation_id),
                f"UPDATE {schema}.execution_kernel_worker_incarnation "
                "SET source_revision=%s WHERE process_incarnation_id=%s",
                (first_start.source_revision, first_start.process_incarnation_id),
            ),
            (
                f"UPDATE {schema}.execution_algo_instance "
                "SET traded_quantity=traded_quantity+10,remaining_quantity=remaining_quantity-10 "
                "WHERE algo_instance_id=%s",
                (_algo_id(),),
                lambda: repository.read_algo_instance(_algo_id()),
                f"UPDATE {schema}.execution_algo_instance SET traded_quantity=%s,remaining_quantity=%s "
                "WHERE algo_instance_id=%s",
                (exact_algo.traded_quantity, exact_algo.remaining_quantity, _algo_id()),
            ),
            (
                f"UPDATE {schema}.execution_algo_instance "
                "SET state_schema_version=state_schema_version || '_drift',transition_sequence=transition_sequence-1 "
                "WHERE algo_instance_id=%s",
                (_algo_id(),),
                lambda: repository.read_algo_instance(_algo_id()),
                f"UPDATE {schema}.execution_algo_instance SET state_schema_version=%s,transition_sequence=%s "
                "WHERE algo_instance_id=%s",
                (exact_algo.state_schema_version, exact_algo.transition_sequence, _algo_id()),
            ),
            (
                f"UPDATE {schema}.execution_child_order "
                "SET quantity=quantity-1,broker_order_id=broker_order_id || '_drift',"
                "updated_by_event_id=updated_by_event_id || '_drift' WHERE mapping_id=%s",
                (terminal_mapping.mapping_id,),
                lambda: repository.read_command_identity_chain(terminal_cancel.command_id),
                f"UPDATE {schema}.execution_child_order SET quantity=%s,broker_order_id=%s,updated_by_event_id=%s "
                "WHERE mapping_id=%s",
                (
                    terminal_mapping.requested_quantity,
                    terminal_mapping.broker_order_id,
                    terminal_mapping.updated_by_event_id,
                    terminal_mapping.mapping_id,
                ),
            ),
            (
                f"UPDATE {schema}.execution_algo_command_outbox "
                "SET broker_order_id=broker_order_id || '_drift',ack_receipt_sha256=%s,ack_receipt_json='{}'::jsonb "
                "WHERE command_id=%s",
                ("e" * 64, terminal_cancel.command_id),
                lambda: repository.list_recovery_outbox_commands(
                    runtime_id="runtime_k2",
                    trade_date=date(2026, 7, 25),
                    statuses=("ACKED",),
                    limit=10,
                ),
                f"UPDATE {schema}.execution_algo_command_outbox "
                "SET broker_order_id=%s,ack_receipt_sha256=%s,ack_receipt_json=%s WHERE command_id=%s",
                (
                    terminal_cancel.broker_order_id,
                    terminal_cancel.ack_receipt_sha256,
                    psycopg2.extras.Json(terminal_cancel_json["ack_receipt_json"]),
                    terminal_cancel.command_id,
                ),
            ),
            (
                f"UPDATE {schema}.execution_algo_event_delivery "
                "SET failure_receipt_id=failure_receipt_id || '_drift' WHERE delivery_id=%s",
                (failed_delivery.delivery_id,),
                lambda: repository.list_recovery_deliveries(
                    runtime_id="runtime_k2",
                    trade_date=date(2026, 7, 25),
                    statuses=("FAILED_TERMINAL",),
                    limit=10,
                ),
                f"UPDATE {schema}.execution_algo_event_delivery SET failure_receipt_id=%s WHERE delivery_id=%s",
                (failed_delivery.failure_receipt_id, failed_delivery.delivery_id),
            ),
            (
                f"UPDATE {schema}.execution_algo_timer_schedule "
                "SET due_at_exchange_utc=due_at_exchange_utc + interval '1 second',"
                "schedule_epoch=schedule_epoch || '_drift' WHERE schedule_id=%s",
                (exact_schedule.schedule_id,),
                lambda: repository.read_timer_schedule(exact_schedule.schedule_id),
                f"UPDATE {schema}.execution_algo_timer_schedule SET due_at_exchange_utc=%s,schedule_epoch=%s "
                "WHERE schedule_id=%s",
                (exact_schedule.due_at_exchange_utc, exact_schedule.schedule_epoch, exact_schedule.schedule_id),
            ),
            (
                f"UPDATE {schema}.execution_algo_timer_occurrence "
                "SET due_at_exchange_utc=due_at_exchange_utc + interval '1 second',"
                "exchange_session_authority_sha256=reverse(exchange_session_authority_sha256) "
                "WHERE timer_occurrence_id=%s",
                (exact_occurrence.timer_occurrence_id,),
                lambda: repository.list_recovery_timer_occurrences(
                    runtime_id="runtime_k2",
                    trade_date=date(2026, 7, 25),
                    statuses=("CLAIMED",),
                    limit=10,
                ),
                f"UPDATE {schema}.execution_algo_timer_occurrence "
                "SET due_at_exchange_utc=%s,exchange_session_authority_sha256=%s WHERE timer_occurrence_id=%s",
                (
                    exact_occurrence.due_at_exchange_utc,
                    exact_occurrence.exchange_session_authority_sha256,
                    exact_occurrence.timer_occurrence_id,
                ),
            ),
            (
                f"UPDATE {schema}.execution_algo_event_delivery SET row_version=row_version+1 WHERE delivery_id=%s",
                (failed_delivery.delivery_id,),
                lambda: repository.read_delivery(failed_delivery.delivery_id),
                f"UPDATE {schema}.execution_algo_event_delivery SET row_version=row_version-1 WHERE delivery_id=%s",
                (failed_delivery.delivery_id,),
            ),
            (
                f"UPDATE {schema}.execution_algo_command_outbox SET row_version=row_version+1 WHERE command_id=%s",
                (terminal_cancel.command_id,),
                lambda: repository.read_outbox_command(terminal_cancel.command_id),
                f"UPDATE {schema}.execution_algo_command_outbox SET row_version=row_version-1 WHERE command_id=%s",
                (terminal_cancel.command_id,),
            ),
            (
                f"UPDATE {schema}.execution_algo_instance SET row_version=row_version+1 WHERE algo_instance_id=%s",
                (_algo_id(),),
                lambda: repository.read_algo_instance(_algo_id()),
                f"UPDATE {schema}.execution_algo_instance SET row_version=row_version-1 WHERE algo_instance_id=%s",
                (_algo_id(),),
            ),
            (
                f"UPDATE {schema}.execution_algo_command_dispatch_attempt "
                "SET attempt_count=attempt_count+1 WHERE dispatch_attempt_id=%s AND stage='CLAIMED'",
                (cancel_attempt.dispatch_attempt_id,),
                lambda: repository.read_dispatch_attempt(cancel_attempt.dispatch_attempt_id, "CLAIMED"),
                f"UPDATE {schema}.execution_algo_command_dispatch_attempt "
                "SET attempt_count=attempt_count-1 WHERE dispatch_attempt_id=%s AND stage='CLAIMED'",
                (cancel_attempt.dispatch_attempt_id,),
            ),
            (
                f"UPDATE {schema}.execution_algo_timer_occurrence "
                "SET occurrence_receipt_sha256=reverse(occurrence_receipt_sha256) "
                "WHERE timer_occurrence_id=%s",
                (tie_occurrence.timer_occurrence_id,),
                lambda: repository.read_timer_occurrence(tie_occurrence.timer_occurrence_id),
                f"UPDATE {schema}.execution_algo_timer_occurrence SET occurrence_receipt_sha256=%s "
                "WHERE timer_occurrence_id=%s",
                (tie_occurrence.occurrence_receipt_sha256, tie_occurrence.timer_occurrence_id),
            ),
            (
                f"UPDATE {schema}.execution_exchange_session_authority SET authority_sha256=%s "
                "WHERE runtime_id=%s AND exchange_trade_date=%s",
                ("3" * 64, authority.runtime_id, date(2026, 7, 25)),
                lambda: repository.read_exchange_session_authority(
                    runtime_id=authority.runtime_id, exchange_trade_date=date(2026, 7, 25)
                ),
                f"UPDATE {schema}.execution_exchange_session_authority SET authority_sha256=%s "
                "WHERE runtime_id=%s AND exchange_trade_date=%s",
                (authority.authority_sha256, authority.runtime_id, date(2026, 7, 25)),
            ),
        )
        for drift_sql, parameters, readback, restore_sql, restore_parameters in scalar_drift_cases:
            with raw.cursor() as cur:
                cur.execute(drift_sql, parameters)
            with pytest.raises(KernelRepositoryConflict):
                readback()
            with raw.cursor() as cur:
                cur.execute(restore_sql, restore_parameters)
    finally:
        raw.autocommit = True
        with raw.cursor() as cur:
            for callback_schema in callback_schemas:
                cur.execute(f"DROP SCHEMA IF EXISTS {callback_schema} CASCADE")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            cur.execute(f"DROP SCHEMA IF EXISTS {incomplete_schema} CASCADE")
        raw.close()


def test_repository_concurrent_timer_first_write_rejects_immutable_payload_conflict() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K2 DEV PostgreSQL fixture")
    schema = _fixture_schema()
    raw = psycopg2.connect(**_dev_dsn())
    raw.autocommit = True
    try:
        with raw.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _apply_forward(cur, FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES (%s,%s)",
                ("runtime_timer_race", date(2026, 7, 25)),
            )
            cur.execute(
                f"INSERT INTO {schema}.execution_algo_instance("
                "algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,"
                "remaining_quantity,algo_code,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    _algo_id(),
                    "runtime_timer_race",
                    "intent_timer_race",
                    "slot_timer_race",
                    "600000.SH",
                    "BUY",
                    100,
                    100,
                    "TWAP",
                    "ACTIVE",
                ),
            )

        def schedule(*, due_at: str, payload_slice: int) -> ExecutionAlgoTimerScheduleV1:
            mutation = TimerMutationV1.create(
                mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
                algo_instance_id=_algo_id(),
                transition_id="transition_timer_race",
                ordinal=0,
                timer_name="race_timer",
                schedule_epoch="race_epoch",
                due_at_exchange_utc=due_at,
                catch_up_policy="EXPIRE_IF_LATE",
                payload={"slice": payload_slice},
            )
            return ExecutionAlgoTimerScheduleV1.create(
                runtime_id="runtime_timer_race",
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

        first = schedule(due_at="2026-07-25T02:00:00Z", payload_slice=1)
        conflicting = schedule(due_at="2026-07-25T02:01:00Z", payload_slice=2)
        assert first.schedule_id == conflicting.schedule_id
        barrier = Barrier(2)
        repository = PostgresMiniQMTKernelRepository(conn_factory=_concurrent_first_write_factory(schema, barrier))

        def write(candidate: ExecutionAlgoTimerScheduleV1) -> object:
            try:
                return repository.write_timer_schedule(candidate)
            except Exception as exc:  # The assertion below verifies the exact fail-loud type.
                return exc

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(executor.map(write, (first, conflicting)))

        successes = tuple(item for item in results if isinstance(item, ExecutionAlgoTimerScheduleV1))
        failures = tuple(item for item in results if isinstance(item, Exception))
        assert len(successes) == 1
        assert len(failures) == 1
        assert isinstance(failures[0], KernelRepositoryConflict)
    finally:
        with raw.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        raw.close()
