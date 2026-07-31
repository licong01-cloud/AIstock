"""K5 broker-neutral ALGO_START persistence on disposable DEV PostgreSQL."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import pytest

from backend.services.miniqmt_execution_runtime.k5_shadow_catalog import build_k5_shadow_catalog_runtime_v1
from backend.services.miniqmt_execution_runtime.kernel_creation import KernelAlgoCreationCoordinatorV1
from backend.services.miniqmt_execution_runtime.kernel_ingress import KernelIngressCoordinatorV1
from backend.services.miniqmt_execution_runtime.kernel_repository import PostgresMiniQMTKernelRepository
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    EventSourceV2,
    EventTypeV2,
    ExecutionProjectionRefV1,
    KernelProjectionTypeV1,
    RuntimeEventEnvelopeV2,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_creation import _request
from backend.tests.miniqmt_execution_runtime.test_kernel_migration_postgres import (
    FORWARD,
    _apply_forward,
    _base_fixture_sql,
    _dev_dsn,
    _fixture_schema,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import _conn_factory
from backend.tests.miniqmt_execution_runtime.test_vnpy_k5_shadow_catalog import _gateway


def _creation_request(algo_code: str, config: dict[str, object]):
    gateway = _gateway()
    request = _request()
    contract = {
        "symbol": request.symbol,
        "gateway_name": gateway.gateway_backend,
        "min_volume": "100",
        "volume_increment": "100",
        "pricetick_decimal": "0.01",
    }
    capability = gateway.model_dump(mode="json")
    refs = tuple(
        sorted(
            (
                ExecutionProjectionRefV1.create(
                    projection_type=item.projection_type,
                    projection_id=item.projection_id,
                    projection_version=item.projection_version,
                    payload_sha256=(
                        hash_hex_v1("miniqmt_contract_projection_v1", contract)
                        if item.projection_type is KernelProjectionTypeV1.CONTRACT
                        else hash_hex_v1("miniqmt_market_capability_projection_v1", capability)
                        if item.projection_type is KernelProjectionTypeV1.MARKET_CAPABILITY
                        else item.payload_sha256
                    ),
                    source_event_id=item.source_event_id,
                    logical_at_utc=item.logical_at_utc,
                )
                for item in request.projection_refs
            ),
            key=lambda item: (item.projection_type.value, item.projection_id),
        )
    )
    payload = request.model_dump(mode="python")
    payload.update(
        {
            "parent_intent_id": f"{request.parent_intent_id}_{algo_code.lower()}",
            "strategy_slot_id": f"{request.strategy_slot_id}_{algo_code.lower()}",
            "algo_code": algo_code,
            "plugin_config": config,
            "plugin_config_sha256": hash_hex_v1("miniqmt_plugin_config_v2", config),
            "contract_projection": contract,
            "contract_projection_sha256": hash_hex_v1("miniqmt_contract_projection_v1", contract),
            "market_capability_projection": capability,
            "market_capability_projection_sha256": hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
            "projection_refs": refs,
        }
    )
    return type(request).model_validate(payload, strict=True)


def test_k5_algo_start_restart_readback_is_atomic_and_broker_neutral_on_dev_postgres() -> None:
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
                f"""
                CREATE TABLE {schema}.execution_parent_benchmark(
                    parent_intent_id TEXT NOT NULL,
                    parent_revision INTEGER NOT NULL,
                    runtime_id TEXT NOT NULL,
                    execution_plan_id TEXT NOT NULL,
                    execution_plan_hash TEXT NOT NULL,
                    release_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    emitted_parent_quantity BIGINT NOT NULL,
                    execution_policy_id TEXT NOT NULL,
                    execution_policy_sha256 TEXT NOT NULL,
                    PRIMARY KEY(parent_intent_id,parent_revision)
                );
                CREATE TABLE {schema}.strategy_runtime_release(
                    release_id TEXT PRIMARY KEY,
                    release_hash TEXT NOT NULL,
                    execution_policy_version_id TEXT NOT NULL,
                    execution_policy_sha256 TEXT NOT NULL
                )
                """
            )
            request_cases = (
                _creation_request("ICEBERG", {"display_volume": "100.5", "interval": 1}),
                _creation_request("STOP", {"price_add": "0.01"}),
            )
            base_request = request_cases[0]
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES (%s,%s)",
                (base_request.runtime_id, "2026-07-25"),
            )
            for request in request_cases:
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_parent_benchmark(
                        parent_intent_id,parent_revision,runtime_id,execution_plan_id,execution_plan_hash,
                        release_id,symbol,side,emitted_parent_quantity,execution_policy_id,execution_policy_sha256
                    ) VALUES (%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        request.parent_intent_id,
                        request.runtime_id,
                        request.execution_plan_id,
                        request.execution_plan_sha256,
                        request.release_id,
                        request.symbol,
                        request.side.value,
                        request.parent_quantity,
                        request.policy_id,
                        request.policy_sha256,
                    ),
                )
            cur.execute(
                f"INSERT INTO {schema}.strategy_runtime_release VALUES (%s,%s,%s,%s)",
                (
                    base_request.release_id,
                    base_request.release_sha256,
                    base_request.policy_id,
                    base_request.policy_sha256,
                ),
            )
        gateway = _gateway()
        candidate = build_k5_shadow_catalog_runtime_v1(gateway_catalog=gateway)
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        coordinator = KernelAlgoCreationCoordinatorV1(
            repository=repository,
            catalog_runtime=candidate.catalog_runtime,
            gateway_catalog=gateway,
            facade_authority=candidate.conformance_authority,
        )
        for request in request_cases:

            def create_from_independent_owner():
                return KernelAlgoCreationCoordinatorV1(
                    repository=PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema)),
                    catalog_runtime=candidate.catalog_runtime,
                    gateway_catalog=gateway,
                    facade_authority=candidate.conformance_authority,
                ).create(request)

            with ThreadPoolExecutor(max_workers=2) as pool:
                concurrent_results = tuple(pool.map(lambda _index: create_from_independent_owner(), range(2)))
            created = concurrent_results[0]
            assert concurrent_results == (created, created)
            restarted_repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
            assert restarted_repository.read_algo_instance(created["algo"].algo_instance_id) == created["algo"]
            transaction = restarted_repository.read_event_transaction(created["event"].event_id)
            assert transaction["event"] == created["event"]
            assert len(transaction["deliveries"]) == 1
            assert (
                restarted_repository.read_delivery(transaction["deliveries"][0].delivery_id)
                == transaction["deliveries"][0]
            )
            assert coordinator.create(request) == created
            with raw.cursor() as cur:
                cur.execute(
                    f"SELECT last_event_sequence FROM {schema}.execution_runtime WHERE runtime_id=%s",
                    (request.runtime_id,),
                )
                follow_up_sequence = int(cur.fetchone()[0]) + 1
            if request.algo_code == "ICEBERG":
                event_type = EventTypeV2.TIMER
                event_source = EventSourceV2.EXCHANGE_SESSION_CLOCK
                event_symbol = None
                payload_schema_version = "miniqmt_timer_due_v1"
                source_identity = {"timer_occurrence_id": f"timer_occurrence_{created['algo'].algo_instance_id}"}
                payload = {
                    "timer_occurrence_id": source_identity["timer_occurrence_id"],
                    "schedule_id": f"timer_schedule_{created['algo'].algo_instance_id}",
                    "algo_instance_id": created["algo"].algo_instance_id,
                    "timer_name": "K5_DEV_TIMER",
                    "schedule_epoch": 1,
                    "due_at_exchange_utc": "2026-07-25T01:31:00Z",
                    "effective_due_at_exchange_utc": "2026-07-25T01:31:00Z",
                    "catch_up_policy": "APPLY_ONCE",
                    "timer_payload": {},
                    "timer_payload_sha256": hash_hex_v1("miniqmt_timer_mutation_payload_v1", {}),
                    "exchange_session_authority_sha256": "7" * 64,
                }
                correlation = {"algo_instance_id": created["algo"].algo_instance_id}
            else:
                event_type = EventTypeV2.TICK
                event_source = EventSourceV2.B0_QUOTE_V2
                event_symbol = request.symbol
                payload_schema_version = "miniqmt_market_data_view_v2"
                source_identity = {"market_data_id": f"market_{created['algo'].algo_instance_id}"}
                payload = {
                    "market_data_id": source_identity["market_data_id"],
                    "source_event_id": f"source_{created['algo'].algo_instance_id}",
                    "symbol": request.symbol,
                    "last_price": "10.02",
                    "limit_up": "11",
                    "limit_down": "9",
                }
                correlation = {}
            follow_up = RuntimeEventEnvelopeV2.create(
                runtime_id=request.runtime_id,
                sequence=follow_up_sequence,
                event_type=event_type,
                event_time_utc="2026-07-25T01:31:00Z",
                monotonic_ns=follow_up_sequence,
                source=event_source,
                symbol=event_symbol,
                payload_schema_version=payload_schema_version,
                payload=payload,
                source_identity=source_identity,
                correlation=correlation,
            )
            ingress_receipt = KernelIngressCoordinatorV1(
                repository=restarted_repository,
                catalog_runtime=candidate.catalog_runtime,
            ).ingest(event=follow_up)
            follow_up_transaction = restarted_repository.read_event_transaction(ingress_receipt.event_id)
            assert follow_up_transaction["event"] == follow_up
            assert tuple(item.algo_instance_id for item in follow_up_transaction["deliveries"]) == (
                created["algo"].algo_instance_id,
            )
            with raw.cursor() as cur:
                cur.execute(
                    f"SELECT "
                    f"(SELECT count(*) FROM {schema}.execution_child_order WHERE algo_instance_id=%s),"
                    f"(SELECT count(*) FROM {schema}.execution_algo_command_outbox WHERE algo_instance_id=%s),"
                    f"(SELECT count(*) FROM {schema}.execution_algo_command_dispatch_attempt)",
                    (created["algo"].algo_instance_id,) * 2,
                )
                assert cur.fetchone() == (0, 0, 0)
    finally:
        raw.autocommit = True
        with raw.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        raw.close()
