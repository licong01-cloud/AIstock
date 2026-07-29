from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import os

import psycopg2
import pytest

from backend.services.miniqmt_execution_runtime.kernel_creation import KernelAlgoCreationCoordinatorV1
from backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_orchestration import (
    build_current_three_shadow_creation_request_v1,
    build_current_three_shadow_delivery_input_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_runner import (
    build_current_three_parity_input_from_shadow_v1,
    build_current_three_shadow_event_v1,
    run_current_three_committed_parity_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_current_three_contracts import CurrentThreeParityStatusV1
from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    KernelDeliveryWorkerV1,
    KernelPluginInvocationError,
)
from backend.services.miniqmt_execution_runtime.kernel_ingress import KernelIngressCoordinatorV1
from backend.services.miniqmt_execution_runtime.kernel_repository import PostgresMiniQMTKernelRepository
from backend.services.miniqmt_execution_runtime.plugin_registry import PluginRouteCompatibilityReceiptV1
from backend.services.miniqmt_execution_runtime.repository import PostgresMiniQMTExecutionRuntimeRepository
from backend.tests.miniqmt_execution_runtime.test_current_three_shadow_source_postgres import (
    _ddl as _legacy_ddl,
)
from backend.tests.miniqmt_execution_runtime.test_current_three_shadow_source_postgres import (
    _dev_dsn,
    _schema as _legacy_schema,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_creation import _catalog, _gateway
from backend.tests.miniqmt_execution_runtime.test_kernel_migration_postgres import (
    FORWARD,
    K2C_FORWARD,
    K2D_FORWARD,
    _base_fixture_sql,
    _fixture_schema,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import _SchemaConnection


def _apply_isolated_kernel_schema(cur: object, schema: str) -> None:
    forward = FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    stage1, remainder = forward.split(
        "-- Stage 2: PostgreSQL requires CONCURRENTLY outside a transaction block.", maxsplit=1
    )
    stage2, stage3 = remainder.split(
        "-- Stage 3: named checks/FKs, validation, comments, and independent readback.", maxsplit=1
    )
    cur.execute(stage1)  # type: ignore[attr-defined]
    for statement in stage2.split(";"):
        if "CREATE UNIQUE INDEX CONCURRENTLY" in statement:
            cur.execute(statement.replace("CREATE UNIQUE INDEX CONCURRENTLY", "CREATE UNIQUE INDEX"))  # type: ignore[attr-defined]
    cur.execute(stage3)  # type: ignore[attr-defined]
    cur.execute(K2C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]
    cur.execute(K2D_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]


def test_durable_shadow_uses_real_k2_repository_and_never_creates_dispatch_attempt() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K3_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K3 DEV PostgreSQL fixture")
    legacy_schema = _legacy_schema()
    kernel_schema = _fixture_schema()
    dsn = _dev_dsn()
    admin = psycopg2.connect(**dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(_legacy_ddl(legacy_schema))
            cur.execute(
                f"""INSERT INTO {legacy_schema}.execution_runtime VALUES (
                    'runtime_dev','account_dev','2026-07-29','SIM','RUNNING','CONNECTED','OPEN','hash',2,
                    '{{"repository_commit_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}'::jsonb,
                    NULL,'2026-07-29T01:29:00Z','2026-07-29T01:30:00Z')"""
            )
            cur.execute(
                f"""INSERT INTO {legacy_schema}.execution_algo_instance VALUES (
                    'legacy_algo_dev','runtime_dev','parent_dev','slot_dev','600000.SH','BUY',100,100,
                    'SNIPER_MINIQMT','ACTIVE',
                    '{{"config":{{"price_mode":"LIMIT_TRIGGER_BY_BEST_QUOTE"}},
                       "limit_price_decimal":"10","pricetick_decimal":"0.01",
                       "min_volume":100,"volume_increment":100}}'::jsonb,
                    NULL,'2026-07-29T01:29:00Z','2026-07-29T01:30:00Z')"""
            )
            cur.execute(
                f"""INSERT INTO {legacy_schema}.execution_child_order VALUES (
                    'legacy_child_dev','runtime_dev','legacy_algo_dev','parent_dev','slot_dev','600000.SH','BUY',100,
                    10,11,'SUBMITTED','broker_dev','2026-07-29T01:30:00Z',
                    '{{"reason_code":"sniper_ask_crossed_limit"}}'::jsonb,NULL,'2026-07-29T01:30:00Z')"""
            )
            cur.execute(
                f"""INSERT INTO {legacy_schema}.execution_runtime_event VALUES (
                    'tick_dev','runtime_dev',1,'TICK','2026-07-29T01:30:00Z','gateway',
                    '{{"symbol":"600000.SH","generation":1,"bid_price_1":9.99,"ask_price_1":10,
                       "bid_volume_1":100,"ask_volume_1":100,"market_data_projection_id":"market_dev",
                       "market_data_projection_sha256":"1111111111111111111111111111111111111111111111111111111111111111",
                       "exchange_trade_date":"2026-07-29","session_epoch":"session_shadow_am",
                       "session_phase":"CONTINUOUS_AM"}}'::jsonb,NULL)"""
            )
            cur.execute(
                f"""INSERT INTO {legacy_schema}.execution_runtime_event VALUES (
                    'child_submitted_dev','runtime_dev',2,'CHILD_ORDER_SUBMITTED','2026-07-29T01:30:00Z','gateway',
                    '{{"algo_instance_id":"legacy_algo_dev","parent_intent_id":"parent_dev",
                       "strategy_slot_id":"slot_dev","child_order_id":"legacy_child_dev",
                       "broker_order_id":"broker_dev","accepted":true,"broker_called":true}}'::jsonb,NULL)"""
            )
            cur.execute(_base_fixture_sql(kernel_schema))
            _apply_isolated_kernel_schema(cur, kernel_schema)
            cur.execute(
                f"""
                CREATE TABLE {kernel_schema}.execution_parent_benchmark(
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
                CREATE TABLE {kernel_schema}.strategy_runtime_release(
                    release_id TEXT PRIMARY KEY,
                    release_hash TEXT NOT NULL,
                    execution_policy_version_id TEXT NOT NULL,
                    execution_policy_sha256 TEXT NOT NULL
                )
                """
            )

        @contextmanager
        def legacy_factory(*, autocommit: bool = True, manage_transaction: bool = False):
            connection = psycopg2.connect(**dsn)
            connection.autocommit = autocommit and not manage_transaction
            try:
                yield connection
                if manage_transaction:
                    connection.commit()
            except Exception:
                if not connection.closed:
                    connection.rollback()
                raise
            finally:
                connection.close()

        legacy_repository = PostgresMiniQMTExecutionRuntimeRepository(legacy_factory, _shadow_read_schema=legacy_schema)
        read = legacy_repository.read_current_three_shadow_snapshot("runtime_dev")
        parity_input, raw_events = build_current_three_parity_input_from_shadow_v1(
            read, legacy_algo_instance_id="legacy_algo_dev"
        )
        parity_receipt = run_current_three_committed_parity_v1(read, legacy_algo_instance_id="legacy_algo_dev")
        assert parity_receipt.status is CurrentThreeParityStatusV1.PASSED
        gateway = _gateway()
        request = build_current_three_shadow_creation_request_v1(
            read=read,
            parity_input=parity_input,
            gateway_catalog=gateway,
        )
        with admin.cursor() as cur:
            cur.execute(
                f"INSERT INTO {kernel_schema}.execution_runtime(runtime_id,trade_date) VALUES (%s,%s)",
                (request.runtime_id, date.fromisoformat(request.exchange_trade_date)),
            )
            cur.execute(
                f"""INSERT INTO {kernel_schema}.execution_parent_benchmark(
                    parent_intent_id,parent_revision,runtime_id,execution_plan_id,execution_plan_hash,
                    release_id,symbol,side,emitted_parent_quantity,execution_policy_id,execution_policy_sha256
                ) VALUES (%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
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
                f"INSERT INTO {kernel_schema}.strategy_runtime_release VALUES (%s,%s,%s,%s)",
                (request.release_id, request.release_sha256, request.policy_id, request.policy_sha256),
            )

        @contextmanager
        def kernel_factory(*, autocommit: bool = False, manage_transaction: bool = False):
            connection = psycopg2.connect(**dsn)
            connection.autocommit = autocommit
            proxy = _SchemaConnection(connection, kernel_schema)
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

        repository = PostgresMiniQMTKernelRepository(conn_factory=kernel_factory)
        created = KernelAlgoCreationCoordinatorV1(
            repository=repository,
            catalog_runtime=_catalog(),
            gateway_catalog=gateway,
        ).create(request)
        assert created["algo"].status.value == "ACTIVE", created["receipt"].model_dump(mode="json")
        event = build_current_three_shadow_event_v1(
            parity_input=parity_input,
            raw=raw_events[0],
            sequence=2,
            association=None,
        )
        ingress = KernelIngressCoordinatorV1(repository=repository, catalog_runtime=_catalog())
        receipt = ingress.ingest(event=event)
        repeated_receipt = ingress.ingest(event=event)
        assert repeated_receipt == receipt
        assert len(receipt.ordered_delivery_ids) == 1
        startup = repository.start_worker_incarnation(
            worker_id="worker_k3b_shadow",
            process_role="K3_SHADOW_DELIVERY",
            source_revision="a" * 40,
            started_at_utc="2026-07-29T01:30:00Z",
        )
        route = PluginRouteCompatibilityReceiptV1.create(
            catalog_snapshot=_catalog().snapshot,
            plugin_key=_catalog().plugin_key_for_new_instance(parity_input.algo_code),
            gateway_catalog=gateway,
        ).validate_against_authority_v1(catalog_snapshot=_catalog().snapshot, gateway_catalog=gateway)

        def input_builder(event, delivery, algo, state, _mappings, _outboxes, _timers, lifecycle):
            return build_current_three_shadow_delivery_input_v1(
                read=read,
                parity_input=parity_input,
                event=event,
                delivery=delivery,
                algo=algo,
                previous_state=state,
                lifecycle_projection=lifecycle,
                route_receipt=route,
                expected_legacy_child_order_ids=("legacy_child_dev",),
            )

        result = KernelDeliveryWorkerV1(
            repository=repository,
            catalog_runtime=_catalog(),
            worker_id=startup.worker_id,
            process_incarnation_id=startup.process_incarnation_id,
        ).process_once(
            delivery_id=receipt.ordered_delivery_ids[0],
            lease_expires_at="2026-07-29T01:31:00Z",
            logical_time_utc="2026-07-29T01:30:00Z",
            input_builder=input_builder,
        )
        assert repository.read_delivery(receipt.ordered_delivery_ids[0]).status.value == "APPLIED"
        assert len(result["new_child_mappings"]) == 1
        assert len(result["command_outboxes"]) == 1
        outbox = repository.read_outbox_command(result["command_outboxes"][0].command_id)
        assert outbox.status.value == "PENDING"
        assert outbox.attempt_count == 0
        assert outbox.dispatch_attempt_id is None
        assert outbox.broker_called is None

        restarted_repository = PostgresMiniQMTKernelRepository(conn_factory=kernel_factory)
        restarted_startup = restarted_repository.start_worker_incarnation(
            worker_id="worker_k3b_shadow_restarted",
            process_role="K3_SHADOW_DELIVERY",
            source_revision="b" * 40,
            started_at_utc="2026-07-29T01:30:01Z",
        )
        with pytest.raises(KernelPluginInvocationError) as replay:
            KernelDeliveryWorkerV1(
                repository=restarted_repository,
                catalog_runtime=_catalog(),
                worker_id=restarted_startup.worker_id,
                process_incarnation_id=restarted_startup.process_incarnation_id,
            ).process_once(
                delivery_id=receipt.ordered_delivery_ids[0],
                lease_expires_at="2026-07-29T01:31:01Z",
                logical_time_utc="2026-07-29T01:30:01Z",
                input_builder=input_builder,
            )
        assert replay.value.reason_code == "MINIQMT_ALGO_DELIVERY_NOT_CLAIMABLE"
        assert restarted_repository.read_outbox_command(outbox.command_id) == outbox
        with admin.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {kernel_schema}.execution_algo_command_dispatch_attempt")
            assert cur.fetchone()[0] == 0
            cur.execute(f"SELECT count(*) FROM {kernel_schema}.execution_algo_command_outbox")
            assert cur.fetchone()[0] == 1
            cur.execute(f"SELECT count(*) FROM {kernel_schema}.execution_child_order")
            assert cur.fetchone()[0] == 1
    finally:
        try:
            with admin.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {kernel_schema} CASCADE")
                cur.execute(f"DROP SCHEMA IF EXISTS {legacy_schema} CASCADE")
        finally:
            admin.close()
