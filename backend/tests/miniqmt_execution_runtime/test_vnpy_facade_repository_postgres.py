from __future__ import annotations

from datetime import date
import os
from typing import Any

import psycopg2
import psycopg2.extras
import pytest

from backend.services.miniqmt_execution_runtime.kernel_repository import (
    PostgresMiniQMTKernelRepository,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    DeliveryStatusV1,
    EventSourceV2,
    EventTypeV2,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_migration_postgres import (
    FORWARD,
    _apply_forward,
    _base_fixture_sql,
    _dev_dsn,
    _fixture_schema,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import (
    _algo,
    _conn_factory,
    _current_test_descriptor,
    _seed_event_receipt_deliveries,
)


_RUNTIME_ID = "runtime_k2"
_TRADE_DATE = "2026-07-25"
_SESSION_EPOCH = "session_k4_repo"
_PHASE = SessionPhaseV1.CONTINUOUS_AM


def _start_event(*, algo_instance_id: str) -> RuntimeEventEnvelopeV2:
    descriptor = _current_test_descriptor()
    return RuntimeEventEnvelopeV2.create(
        runtime_id=_RUNTIME_ID,
        sequence=1,
        event_type=EventTypeV2.ALGO_START,
        event_time_utc="2026-07-25T01:20:00Z",
        monotonic_ns=None,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        symbol="600000.SH",
        payload_schema_version="miniqmt_algo_start_v1",
        payload={"target_quantity": 100},
        source_identity={
            "algo_instance_id": algo_instance_id,
            "runtime_id": _RUNTIME_ID,
            "parent_intent_id": "intent_k2",
            "strategy_slot_id": "slot_k2",
            "algo_code": descriptor.manifest.algo_code,
            "plugin_id": descriptor.manifest.plugin_id,
            "plugin_version": descriptor.manifest.plugin_version,
            "plugin_manifest_sha256": descriptor.manifest.manifest_sha256,
            "plugin_config_sha256": _algo(row_version=1, active_child_count=0).plugin_config_sha256,
        },
        correlation={},
    )


def _tick_event(
    *,
    sequence: int,
    market_data_id: str,
    eligibility_state: str = "READY",
    freshness_state: str = "READY",
    session_epoch: str = _SESSION_EPOCH,
    session_phase: SessionPhaseV1 = _PHASE,
    symbol: str = "600000.SH",
) -> RuntimeEventEnvelopeV2:
    return RuntimeEventEnvelopeV2.create(
        runtime_id=_RUNTIME_ID,
        sequence=sequence,
        event_type=EventTypeV2.TICK,
        event_time_utc=f"2026-07-25T01:{20 + sequence:02d}:00Z",
        monotonic_ns=None,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol=symbol,
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={
            "symbol": symbol,
            "logical_at_utc": f"2026-07-25T01:{20 + sequence:02d}:00Z",
            "bid_price_1": "10.00",
            "bid_volume_1": 1000,
            "ask_price_1": "10.01",
            "ask_volume_1": 1000,
            "last_price": "10.00",
            "limit_up": "11.00",
            "limit_down": "9.00",
            "eligibility_state": eligibility_state,
            "freshness_state": freshness_state,
            "generation": 1,
            "quote_source": "B0_QUOTE_V2",
            "exchange_time_utc": f"2026-07-25T01:{20 + sequence:02d}:00Z",
            "exchange_trade_date": _TRADE_DATE,
            "session_epoch": session_epoch,
            "session_phase": session_phase.value,
        },
        source_identity={"market_data_id": market_data_id},
        correlation={
            "exchange_trade_date": _TRADE_DATE,
            "session_epoch": session_epoch,
            "session_phase": session_phase.value,
        },
    )


def _applied_delivery(
    event: RuntimeEventEnvelopeV2,
    *,
    algo_instance_id: str,
    sequence: int,
    previous_delivery_id: str | None,
) -> AlgoDeliveryPersistenceV1:
    carrier = AlgoEventDeliveryV1.create(
        event=event,
        algo_instance_id=algo_instance_id,
        plugin_manifest_sha256=_current_test_descriptor().manifest.manifest_sha256,
        algo_delivery_sequence=sequence,
        previous_delivery_id=previous_delivery_id,
        status=DeliveryStatusV1.APPLIED,
        attempt_count=1,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=f"transition_k4_repo_{sequence}",
        last_error_json=None,
        created_at_utc=event.event_time_utc,
        updated_at_utc=event.event_time_utc,
    )
    return AlgoDeliveryPersistenceV1.create(
        delivery=carrier,
        lease_epoch=1,
        lease_fence_token=None,
        row_version=3,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        closed_at_utc=event.event_time_utc,
    )


class _OneRowCursor:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self.row = row
        self.execute_count = 0
        self.fetchone_count = 0

    def execute(self, _sql: str, _parameters: tuple[Any, ...]) -> None:
        self.execute_count += 1

    def fetchone(self) -> dict[str, Any] | None:
        self.fetchone_count += 1
        return self.row


def test_latest_invalid_candidate_is_explicitly_unavailable_without_older_fallback() -> None:
    algo = _algo(row_version=1, active_child_count=0)
    event = _tick_event(sequence=3, market_data_id="market_k4_stale", freshness_state="STALE")
    delivery = _applied_delivery(
        event,
        algo_instance_id=algo.algo_instance_id,
        sequence=3,
        previous_delivery_id="delivery_k4_repo_2",
    )
    cursor = _OneRowCursor(
        {
            "event_payload": event.model_dump(mode="json"),
            "delivery_payload": delivery.model_dump(mode="json"),
        }
    )

    result = PostgresMiniQMTKernelRepository._read_facade_latest_prior_tick_with_cursor(
        cursor,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        cutoff_delivery_sequence=4,
        cutoff_event_sequence=4,
        exchange_trade_date=_TRADE_DATE,
        session_epoch=_SESSION_EPOCH,
        session_phase=_PHASE,
        expected_symbol=algo.symbol,
    )

    assert result is None
    assert cursor.execute_count == 1
    assert cursor.fetchone_count == 1


def _table_snapshot(raw: Any, schema: str) -> tuple[str, str, str]:
    with raw.cursor() as cur:
        snapshots = []
        for table, identity in (
            ("execution_algo_instance", "algo_instance_id"),
            ("execution_runtime_event", "event_id"),
            ("execution_algo_event_delivery", "delivery_id"),
        ):
            cur.execute(
                f"SELECT COALESCE(jsonb_agg(to_jsonb(target) ORDER BY {identity})::text,'[]') "
                f"FROM {schema}.{table} AS target"
            )
            snapshots.append(cur.fetchone()[0])
    return tuple(snapshots)  # type: ignore[return-value]


def test_facade_repository_cutoff_and_zero_write_on_disposable_dev_postgres() -> None:
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
                (_RUNTIME_ID, date.fromisoformat(_TRADE_DATE)),
            )
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        algo = _algo(row_version=1, active_child_count=0)
        with repository._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                repository._cas_algo_with_cursor(cur, algo_instance=algo, expected_row_version=0)

        start_event = _start_event(algo_instance_id=algo.algo_instance_id)
        start_delivery = _applied_delivery(
            start_event,
            algo_instance_id=algo.algo_instance_id,
            sequence=1,
            previous_delivery_id=None,
        )
        _seed_event_receipt_deliveries(repository, event=start_event, deliveries=(start_delivery,))
        ready_event = _tick_event(sequence=2, market_data_id="market_k4_ready")
        ready_delivery = _applied_delivery(
            ready_event,
            algo_instance_id=algo.algo_instance_id,
            sequence=2,
            previous_delivery_id=start_delivery.delivery_id,
        )
        _seed_event_receipt_deliveries(repository, event=ready_event, deliveries=(ready_delivery,))
        invalid_event = _tick_event(
            sequence=3,
            market_data_id="market_k4_invalid",
            session_phase=SessionPhaseV1.CONTINUOUS_PM,
        )
        invalid_delivery = _applied_delivery(
            invalid_event,
            algo_instance_id=algo.algo_instance_id,
            sequence=3,
            previous_delivery_id=ready_delivery.delivery_id,
        )
        _seed_event_receipt_deliveries(repository, event=invalid_event, deliveries=(invalid_delivery,))
        before = _table_snapshot(raw, schema)

        start_read = repository.read_facade_algo_start_event_v1(
            runtime_id=_RUNTIME_ID, algo_instance_id=algo.algo_instance_id
        )
        selected = repository.read_facade_latest_prior_tick_v1(
            runtime_id=_RUNTIME_ID,
            algo_instance_id=algo.algo_instance_id,
            timer_delivery_sequence=3,
            timer_event_sequence=3,
            exchange_trade_date=_TRADE_DATE,
            session_epoch=_SESSION_EPOCH,
            session_phase=_PHASE,
        )
        unavailable = repository.read_facade_latest_prior_tick_v1(
            runtime_id=_RUNTIME_ID,
            algo_instance_id=algo.algo_instance_id,
            timer_delivery_sequence=4,
            timer_event_sequence=4,
            exchange_trade_date=_TRADE_DATE,
            session_epoch=_SESSION_EPOCH,
            session_phase=_PHASE,
        )
        with repository._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                same_cursor = repository._read_facade_latest_prior_tick_with_cursor(
                    cur,
                    runtime_id=_RUNTIME_ID,
                    algo_instance_id=algo.algo_instance_id,
                    cutoff_delivery_sequence=3,
                    cutoff_event_sequence=3,
                    exchange_trade_date=_TRADE_DATE,
                    session_epoch=_SESSION_EPOCH,
                    session_phase=_PHASE,
                    expected_symbol=algo.symbol,
                )

        assert start_read.event == start_event
        assert selected is not None and selected.event == ready_event
        assert same_cursor == selected
        assert unavailable is None
        assert _table_snapshot(raw, schema) == before

        later_event = _tick_event(sequence=5, market_data_id="market_k4_later")
        later_delivery = _applied_delivery(
            later_event,
            algo_instance_id=algo.algo_instance_id,
            sequence=4,
            previous_delivery_id=invalid_delivery.delivery_id,
        )
        _seed_event_receipt_deliveries(repository, event=later_event, deliveries=(later_delivery,))
        later_before_retry = _table_snapshot(raw, schema)
        retry = repository.read_facade_latest_prior_tick_v1(
            runtime_id=_RUNTIME_ID,
            algo_instance_id=algo.algo_instance_id,
            timer_delivery_sequence=3,
            timer_event_sequence=3,
            exchange_trade_date=_TRADE_DATE,
            session_epoch=_SESSION_EPOCH,
            session_phase=_PHASE,
        )

        # A later event/delivery is a real new fact, but the original immutable
        # double cutoff must continue to select the same earlier TICK.
        assert retry == selected
        assert _table_snapshot(raw, schema) == later_before_retry
    finally:
        raw.autocommit = True
        with raw.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        raw.close()
