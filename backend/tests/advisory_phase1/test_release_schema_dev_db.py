"""Disposable PostgreSQL Phase 1F integration matrix.

The module launches its own pinned PostgreSQL 16 container. It never loads an
AIstock ``.env`` file and destroys every test database plus the whole container.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg2
import psycopg2.errors
import psycopg2.extras
import psycopg2.sql
import pytest

import backend.services.advisory_phase1.release_schema_apply_postgres as apply_module
from backend.services.advisory_phase1.release_schema_apply_postgres import (
    REASON_DDL_EXECUTION_FAILED,
    REASON_DDL_LOCK_TIMEOUT,
    REASON_POST_COMMIT_VERIFY_FAILED,
    ReleaseSchemaApplyError,
    _execute_executor_managed_migration,
    _execute_file_wrapped_migration,
    _set_session_policy,
    _timeout_reason,
    apply_release_schema_plan,
    build_release_schema_plan,
    verify_release_schema_plan,
)
from backend.services.advisory_phase1.release_schema_contract import (
    ManagedSchemaStatus,
    MigrationExecutionStatus,
    PrerequisiteStatus,
    RequestedOperation,
    TargetLabel,
    canonical_json_sha256,
    load_release_schema_contract,
    make_release_plan_request,
    plan_month_partitions,
)
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    DatabaseConnectionConfig,
    readonly_catalog_connection,
    verify_catalog,
)


POSTGRES_IMAGE = "postgres@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777"
POSTGRES_PASSWORD = "phase1f_disposable_only"
BASELINE_SQL = """
CREATE SCHEMA app;
CREATE SCHEMA market;
CREATE TABLE market.trading_calendar (
    cal_date DATE PRIMARY KEY,
    is_trading BOOLEAN NOT NULL
);
"""
BASELINE_WITHOUT_PREREQUISITE_SQL = "CREATE SCHEMA app;"


@dataclass(frozen=True)
class DisposablePostgres:
    container_name: str
    host: str
    port: int
    user: str = "postgres"
    password: str = POSTGRES_PASSWORD

    def admin_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": "postgres",
            "user": self.user,
            "password": self.password,
        }


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("docker", *args),
        check=check,
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.fixture(scope="module")
def disposable_postgres() -> Iterator[DisposablePostgres]:
    if shutil.which("docker") is None:
        if os.getenv("AISTOCK_PHASE1F_REQUIRE_L2") == "1":
            pytest.fail("Docker is required for the Phase 1F L2 merge gate")
        pytest.skip("Docker is unavailable for the disposable Phase 1F L2 matrix")
    name = f"aistock-phase1f-{uuid.uuid4().hex[:12]}"
    started = _docker(
        "run",
        "--detach",
        "--rm",
        "--name",
        name,
        "--env",
        f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
        "--publish",
        "127.0.0.1::5432",
        POSTGRES_IMAGE,
    )
    assert started.stdout.strip(), started.stderr
    server: DisposablePostgres | None = None
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            port_result = _docker("port", name, "5432/tcp", check=False)
            if port_result.returncode == 0 and port_result.stdout.strip():
                port = int(port_result.stdout.strip().rsplit(":", 1)[1])
                candidate = DisposablePostgres(container_name=name, host="127.0.0.1", port=port)
                try:
                    connection = psycopg2.connect(**candidate.admin_kwargs(), connect_timeout=2)
                except psycopg2.OperationalError:
                    time.sleep(0.5)
                else:
                    connection.close()
                    server = candidate
                    break
            else:
                time.sleep(0.5)
        if server is None:
            logs = _docker("logs", name, check=False).stderr[-2000:]
            raise AssertionError(f"disposable PostgreSQL did not become ready: {logs}")
        yield server
    finally:
        _docker("rm", "--force", name, check=False)
        assert _docker("inspect", name, check=False).returncode != 0, "disposable PostgreSQL container was not destroyed"


@pytest.fixture
def database_factory(disposable_postgres: DisposablePostgres) -> Iterator[Callable[..., DatabaseConnectionConfig]]:
    created: list[str] = []

    def create(*, prerequisite: bool = True) -> DatabaseConnectionConfig:
        database = f"aistock_phase1f_test_{uuid.uuid4().hex[:12]}"
        admin = psycopg2.connect(**disposable_postgres.admin_kwargs())
        admin.autocommit = True
        try:
            with admin.cursor() as cursor:
                cursor.execute(psycopg2.sql.SQL("CREATE DATABASE {}").format(psycopg2.sql.Identifier(database)))
        finally:
            admin.close()
        created.append(database)
        config = DatabaseConnectionConfig(
            target_label=TargetLabel.DEV,
            host=disposable_postgres.host,
            port=disposable_postgres.port,
            database=database,
            user=disposable_postgres.user,
            password=disposable_postgres.password,
            environment_contract_hash=canonical_json_sha256(
                {
                    "target_label": "DISPOSABLE",
                    "container_name": disposable_postgres.container_name,
                    "database": database,
                }
            ),
        )
        connection = psycopg2.connect(**config.connect_kwargs())
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                cursor.execute(BASELINE_SQL if prerequisite else BASELINE_WITHOUT_PREREQUISITE_SQL)
        finally:
            connection.close()
        return config

    yield create

    admin = psycopg2.connect(**disposable_postgres.admin_kwargs())
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            for database in created:
                cursor.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                    (database,),
                )
                cursor.execute(psycopg2.sql.SQL("DROP DATABASE {}").format(psycopg2.sql.Identifier(database)))
            cursor.execute("SELECT datname FROM pg_database WHERE datname = ANY(%s)", (created,))
            assert cursor.fetchall() == [], "disposable test database was not destroyed"
    finally:
        admin.close()


def _request(config: DatabaseConnectionConfig, operation: RequestedOperation = RequestedOperation.APPLY):
    contract = load_release_schema_contract()
    return contract, make_release_plan_request(
        contract=contract,
        target_label=config.target_label,
        history_start_trade_date=date(2026, 6, 1),
        history_end_trade_date=date(2026, 8, 31),
        capacity_request_hash="1" * 64,
        capacity_receipt_hash=None,
        phase1e_plan_hashes=(),
        requested_operation=operation,
    )


def _fresh_apply(config: DatabaseConnectionConfig):
    contract, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    receipt = apply_release_schema_plan(plan=plan, config=config, contract=contract)
    assert receipt.operation_status.value == "SUCCESS"
    assert receipt.managed_schema_status is ManagedSchemaStatus.COMPATIBLE
    return contract, receipt


def _execute(config: DatabaseConnectionConfig, sql_text: str) -> None:
    connection = psycopg2.connect(**config.connect_kwargs())
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_text)
    finally:
        connection.close()


def test_full_frozen_apply_verify_reapply_and_evidence(database_factory: Callable[..., DatabaseConnectionConfig]) -> None:
    config = database_factory()
    contract, first = _fresh_apply(config)
    assert first.ddl_executed
    assert first.post_catalog_evidence is not None
    assert first.post_catalog_evidence.object_count == sum(first.post_catalog_evidence.per_kind_counts.values())
    assert first.post_catalog_evidence.total_sha256 == first.post_catalog_fingerprint

    verify_contract, verify_request = _request(config, RequestedOperation.VERIFY)
    verify_plan = build_release_schema_plan(config=config, contract=verify_contract, request=verify_request)
    verified = verify_release_schema_plan(plan=verify_plan, config=config, contract=verify_contract)
    assert verified.operation_status.value == "SUCCESS"
    assert not verified.ddl_executed

    contract, request = _request(config)
    reapply_plan = build_release_schema_plan(config=config, contract=contract, request=request)
    reapply = apply_release_schema_plan(plan=reapply_plan, config=config, contract=contract)
    assert reapply.operation_status.value == "SUCCESS"
    assert reapply.managed_schema_status is ManagedSchemaStatus.COMPATIBLE
    assert not reapply.ddl_executed
    assert reapply.post_catalog_fingerprint == first.post_catalog_fingerprint


def test_missing_external_prerequisite_does_not_block_managed_apply(database_factory: Callable[..., DatabaseConnectionConfig]) -> None:
    config = database_factory(prerequisite=False)
    _, receipt = _fresh_apply(config)
    assert receipt.prerequisite_status is PrerequisiteStatus.MISSING
    assert not receipt.downstream_ready
    assert receipt.operation_status.value == "SUCCESS"


DRIFT_CASES = (
    "DROP TABLE app.advisory_source_observer_cursor CASCADE; CREATE VIEW app.advisory_source_observer_cursor AS SELECT 1 AS wrong",
    "ALTER TABLE app.advisory_source_observer_cursor DROP COLUMN last_trade_date CASCADE",
    "ALTER TABLE app.advisory_source_observer_cursor ALTER COLUMN updated_at DROP DEFAULT",
    "ALTER TABLE app.advisory_source_observation_receipt DROP CONSTRAINT advisory_source_observation_receip_availability_event_hash_fkey",
    "ALTER TABLE app.advisory_source_observer_cursor DROP CONSTRAINT advisory_source_observer_cursor_row_version_check",
    "DROP INDEX app.ux_advisory_dataset_build_active_key; CREATE UNIQUE INDEX ux_advisory_dataset_build_active_key ON app.advisory_dataset_build (logical_build_key_sha256)",
    "ALTER TABLE app.advisory_source_observer_cursor DISABLE TRIGGER trg_verify_advisory_source_observer_cursor_update",
    "CREATE OR REPLACE FUNCTION app.verify_advisory_source_observer_cursor_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
    "DROP TABLE app.advisory_outcome_label_payload_202606; CREATE TABLE app.advisory_outcome_label_payload_202606 PARTITION OF app.advisory_outcome_label_payload FOR VALUES FROM ('2026-06-02') TO ('2026-07-01')",
)


@pytest.mark.parametrize("mutation_sql", DRIFT_CASES)
def test_catalog_drift_matrix_is_rejected(
    database_factory: Callable[..., DatabaseConnectionConfig], mutation_sql: str
) -> None:
    config = database_factory()
    contract, _ = _fresh_apply(config)
    _execute(config, mutation_sql)
    _, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    assert plan.managed_schema_status is ManagedSchemaStatus.DRIFTED, [
        (item.object_id, item.category, item.repairable_by_orders) for item in plan.managed_differences
    ]
    assert plan.pending_ddl_operations == ()
    with pytest.raises(ReleaseSchemaApplyError) as error:
        apply_release_schema_plan(plan=plan, config=config, contract=contract)
    assert error.value.reason_code == "PHASE1F_SCHEMA_DRIFTED"


def test_committed_migration_remains_recorded_when_readback_fails(
    database_factory: Callable[..., DatabaseConnectionConfig], monkeypatch: pytest.MonkeyPatch
) -> None:
    config = database_factory()
    contract, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    original_verify = apply_module.verify_database_catalog
    calls = 0

    def fail_after_first_commit(**kwargs: Any):
        nonlocal calls
        calls += 1
        if calls >= 3:
            raise ReleaseSchemaApplyError(
                REASON_POST_COMMIT_VERIFY_FAILED,
                "injected independent readback failure",
                migration_order=10,
                transaction_stage="POST_COMMIT_SUBSET_READBACK",
            )
        return original_verify(**kwargs)

    monkeypatch.setattr(apply_module, "verify_database_catalog", fail_after_first_commit)
    receipt = apply_release_schema_plan(plan=plan, config=config, contract=contract)
    assert receipt.operation_status.value == "FAILED"
    assert receipt.ddl_executed
    assert receipt.executed_migration_hashes == (contract.managed_migrations[0].file_sha256,)
    assert receipt.per_migration_results[0].status is MigrationExecutionStatus.COMMITTED
    assert receipt.per_migration_results[0].error_code == REASON_POST_COMMIT_VERIFY_FAILED


def test_partial_failure_receipt_and_exact_resume(
    database_factory: Callable[..., DatabaseConnectionConfig], monkeypatch: pytest.MonkeyPatch
) -> None:
    config = database_factory()
    contract, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    original = apply_module._execute_file_wrapped_migration

    def fail_order_30(*, config: DatabaseConnectionConfig, contract: Any, migration: Any) -> None:
        if migration.order == 30:
            raise ReleaseSchemaApplyError(
                REASON_DDL_EXECUTION_FAILED,
                "injected order 30 failure",
                migration_order=30,
                transaction_stage="FILE_WRAPPED_DDL",
            )
        original(config=config, contract=contract, migration=migration)

    monkeypatch.setattr(apply_module, "_execute_file_wrapped_migration", fail_order_30)
    failed = apply_release_schema_plan(plan=plan, config=config, contract=contract)
    assert failed.operation_status.value == "FAILED"
    assert [item.order for item in failed.per_migration_results if item.status is MigrationExecutionStatus.COMMITTED] == [10, 20]
    assert failed.per_migration_results[-1].order == 30
    assert failed.per_migration_results[-1].status is MigrationExecutionStatus.FAILED

    monkeypatch.setattr(apply_module, "_execute_file_wrapped_migration", original)
    _, resume_request = _request(config)
    resume_plan = build_release_schema_plan(config=config, contract=contract, request=resume_request)
    pending_orders = [item.migration_order for item in resume_plan.pending_ddl_operations if item.kind == "MIGRATION"]
    assert pending_orders == [30, 40, 50, 70]
    resumed = apply_release_schema_plan(plan=resume_plan, config=config, contract=contract)
    assert resumed.operation_status.value == "SUCCESS"
    assert resumed.managed_schema_status is ManagedSchemaStatus.COMPATIBLE


@pytest.mark.parametrize("failed_order", (20, 30))
def test_current_migration_is_atomic_on_mid_file_failure(
    database_factory: Callable[..., DatabaseConnectionConfig], failed_order: int
) -> None:
    config = database_factory()
    contract, request = _request(config)
    partitions = plan_month_partitions(
        partition_contract=contract.partition_contract,
        history_start_trade_date=request.history_start_trade_date,
        history_end_trade_date=request.history_end_trade_date,
    )
    for migration in contract.managed_migrations:
        if migration.order >= failed_order:
            break
        if migration.transaction_mode.value == "EXECUTOR_MANAGED":
            _execute_executor_managed_migration(
                config=config,
                contract=contract,
                migration=migration,
                expected_partitions=partitions,
            )
        else:
            _execute_file_wrapped_migration(config=config, contract=contract, migration=migration)

    _execute(
        config,
        """
        CREATE SEQUENCE public.phase1f_ddl_counter;
        CREATE FUNCTION public.phase1f_fail_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF nextval('public.phase1f_ddl_counter') = 3 THEN
                RAISE EXCEPTION 'PHASE1F_TEST_INJECTED_DDL_FAILURE';
            END IF;
        END;
        $$;
        CREATE EVENT TRIGGER phase1f_fail_ddl ON ddl_command_start EXECUTE FUNCTION public.phase1f_fail_ddl();
        """,
    )
    try:
        _, request = _request(config)
        plan = build_release_schema_plan(config=config, contract=contract, request=request)
        assert plan.managed_schema_status is ManagedSchemaStatus.PARTIAL_ADDITIVE, "\n".join(
            f"{item.object_id}|{canonical_json_sha256(item.actual)}|{item.repairable_by_orders}"
            for item in plan.managed_differences
            if item.category == "DRIFTED"
        )
        receipt = apply_release_schema_plan(plan=plan, config=config, contract=contract)
        assert receipt.operation_status.value == "FAILED"
        failed = next(item for item in receipt.per_migration_results if item.order == failed_order)
        assert failed.status is MigrationExecutionStatus.FAILED
    finally:
        _execute(
            config,
            "DROP EVENT TRIGGER IF EXISTS phase1f_fail_ddl; DROP FUNCTION IF EXISTS public.phase1f_fail_ddl(); DROP SEQUENCE IF EXISTS public.phase1f_ddl_counter",
        )

    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cursor:
            if failed_order == 20:
                cursor.execute(
                    "SELECT 1 FROM pg_attribute WHERE attrelid = 'app.advisory_source_revision_set'::regclass AND attname = 'schema_version' AND NOT attisdropped"
                )
            else:
                cursor.execute("SELECT to_regclass('app.advisory_capture_batch')")
            row = cursor.fetchone()
        assert row is None or row[0] is None
    finally:
        connection.close()


def test_lock_timeout_has_no_retry_and_resume_succeeds(database_factory: Callable[..., DatabaseConnectionConfig]) -> None:
    config = database_factory()
    contract, request = _request(config)
    partitions = plan_month_partitions(
        partition_contract=contract.partition_contract,
        history_start_trade_date=request.history_start_trade_date,
        history_end_trade_date=request.history_end_trade_date,
    )
    _execute_executor_managed_migration(
        config=config,
        contract=contract,
        migration=contract.managed_migrations[0],
        expected_partitions=partitions,
    )
    _, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    assert plan.managed_schema_status is ManagedSchemaStatus.PARTIAL_ADDITIVE, "\n".join(
        f"{item.object_id}|{canonical_json_sha256(item.actual)}|{item.repairable_by_orders}"
        for item in plan.managed_differences
        if item.category == "DRIFTED"
    )

    locker = psycopg2.connect(**config.connect_kwargs())
    try:
        with locker.cursor() as cursor:
            cursor.execute("LOCK TABLE app.advisory_source_revision_set IN ACCESS SHARE MODE")
        started = time.monotonic()
        receipt = apply_release_schema_plan(plan=plan, config=config, contract=contract)
        elapsed = time.monotonic() - started
    finally:
        locker.rollback()
        locker.close()
    assert receipt.operation_status.value == "FAILED"
    assert receipt.errors[0]["reason_code"] == REASON_DDL_LOCK_TIMEOUT
    assert 9 <= elapsed < 20
    assert len([item for item in receipt.per_migration_results if item.order == 20]) == 1

    _, resume_request = _request(config)
    resumed = apply_release_schema_plan(
        plan=build_release_schema_plan(config=config, contract=contract, request=resume_request),
        config=config,
        contract=contract,
    )
    assert resumed.operation_status.value == "SUCCESS"


def test_session_policy_and_statement_timeout_classification(database_factory: Callable[..., DatabaseConnectionConfig]) -> None:
    config = database_factory()
    contract = load_release_schema_contract()
    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cursor:
            _set_session_policy(cursor, contract=contract, local=True)
            cursor.execute("SHOW lock_timeout")
            assert cursor.fetchone()[0] == "10s"
            cursor.execute("SHOW statement_timeout")
            assert cursor.fetchone()[0] == "15min"
    finally:
        connection.rollback()
        connection.close()
    assert _timeout_reason(psycopg2.errors.QueryCanceled("canceling statement due to statement timeout")) == "PHASE1F_DDL_STATEMENT_TIMEOUT"


class _CursorSpy:
    def __init__(self, cursor: Any, statements: list[str]) -> None:
        self._cursor = cursor
        self._statements = statements

    def __enter__(self) -> "_CursorSpy":
        self._cursor.__enter__()
        return self

    def __exit__(self, *args: Any) -> Any:
        return self._cursor.__exit__(*args)

    def execute(self, query: Any, variables: Any = None) -> Any:
        rendered = query if isinstance(query, str) else query.as_string(self._cursor.connection)
        self._statements.append(str(rendered))
        return self._cursor.execute(query, variables)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class _ConnectionSpy:
    def __init__(self, connection: Any, statements: list[str]) -> None:
        self._connection = connection
        self._statements = statements

    def cursor(self, *args: Any, **kwargs: Any) -> _CursorSpy:
        return _CursorSpy(self._connection.cursor(*args, **kwargs), self._statements)


def test_verifier_query_spy_is_catalog_only_and_read_only(database_factory: Callable[..., DatabaseConnectionConfig]) -> None:
    config = database_factory()
    contract = load_release_schema_contract()
    partitions = plan_month_partitions(
        partition_contract=contract.partition_contract,
        history_start_trade_date=date(2026, 6, 1),
        history_end_trade_date=date(2026, 8, 31),
    )
    statements: list[str] = []
    with readonly_catalog_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            assert cursor.fetchone()[0] == "on"
        verify_catalog(
            connection=_ConnectionSpy(connection, statements),
            config=config,
            contract=contract,
            expected_partitions=partitions,
        )
    normalized = [" ".join(statement.lower().split()) for statement in statements]
    assert normalized
    assert all(statement.startswith("select ") for statement in normalized)
    assert not any(token in statement for statement in normalized for token in ("insert ", "update ", "delete ", "alter ", "create ", "drop "))
    assert not any(" from app." in statement or " from market." in statement for statement in normalized)
