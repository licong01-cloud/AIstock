"""Disposable PostgreSQL Phase 1F integration matrix.

The module launches its own pinned PostgreSQL 16 container. It never loads an
AIstock ``.env`` file and destroys every test database plus the whole container.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
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
    REASON_PLAN_STALE,
    REASON_POST_COMMIT_VERIFY_FAILED,
    REASON_PHASE1F1_CATALOG_DRIFTED,
    REASON_PHASE1F1_COPY_MISMATCH,
    REASON_PHASE1F1_POST_FAILURE_VERIFY_FAILED,
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
from backend.services.advisory_phase1.snapshot_writer import (
    DeterministicParquetWriter,
    PostgresSnapshotSourceReader,
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
        assert _docker("inspect", name, check=False).returncode != 0, (
            "disposable PostgreSQL container was not destroyed"
        )


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


def _apply_v1_predecessor(config: DatabaseConnectionConfig):
    contract = load_release_schema_contract(
        Path("backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v1.json")
    )
    partitions = plan_month_partitions(
        partition_contract=contract.partition_contract,
        history_start_trade_date=date(2026, 6, 1),
        history_end_trade_date=date(2026, 8, 31),
    )
    for migration in contract.managed_migrations:
        if migration.transaction_mode.value == "EXECUTOR_MANAGED":
            _execute_executor_managed_migration(
                config=config,
                contract=contract,
                migration=migration,
                expected_partitions=partitions,
            )
        else:
            _execute_file_wrapped_migration(config=config, contract=contract, migration=migration)
    return contract


def _seed_v1_cross_month_lineage_and_candidates(config: DatabaseConnectionConfig) -> None:
    _execute(
        config,
        """
        SET session_replication_role = replica;
        WITH seed(suffix, decision_date) AS (
            VALUES ('a'::text, DATE '2026-06-15'), ('b'::text, DATE '2026-07-15')
        )
        INSERT INTO app.advisory_signal_observation (
            canonical_signal_id, signal_schema_version, stable_signal_semantics_hash,
            canonical_signal_scope_hash, decision_as_of_trade_date, selection_as_of_trade_date,
            target_trade_date, decision_cutoff_ts, package_id, manifest_sha256, alpha_mode,
            selection_runtime_semantics_hash, package_effective_config_hash, calendar_version, calendar_hash
        )
        SELECT 'signal-' || suffix, 'advisory_canonical_signal_v1', repeat('s', 64), repeat(suffix, 64),
               decision_date, decision_date, decision_date + 1, '2026-06-15 08:00:00+08',
               'package-' || suffix, repeat('m', 64), 'multi_alpha', repeat('r', 64),
               repeat('c', 64), 'calendar-v1', repeat('k', 64)
          FROM seed;

        WITH seed(suffix) AS (VALUES ('a'::text), ('b'::text))
        INSERT INTO app.advisory_signal_observation_version (
            observation_version_id, canonical_signal_id, observation_schema_version, observation_revision_no,
            supersedes_observation_version_id, signal_source_revision_set_id, signal_source_revision_set_hash,
            phase0a_signal_context_hash, evidence_bundle_hash, stage_evidence_bundle_hash,
            selection_evidence_id, selection_evidence_hash, selection_run_id, selection_run_content_hash,
            selection_score_artifact_id, selection_score_artifact_hash, runtime_profile_version_id,
            runtime_profile_version_hash, hmm_snapshot_id, hmm_snapshot_hash, hmm_snapshot_status,
            risk_policy_hash, universe_policy_hash, symbol_normalization_policy_hash, valid_no_candidate,
            observation_status, evidence_available_at, observation_content_hash, created_by_capture_batch_id
        )
        SELECT 'version-' || suffix, 'signal-' || suffix, 'advisory_signal_observation_version_v1', 1,
               NULL, 'revision-' || suffix, repeat('v', 64), repeat('p', 64), repeat('e', 64), repeat('b', 64),
               'selection-evidence-' || suffix, repeat('q', 64), 'selection-run-' || suffix, repeat('u', 64),
               'artifact-' || suffix, repeat('a', 64), 'profile-' || suffix, repeat('f', 64),
               NULL, NULL, 'NOT_APPLICABLE', repeat('r', 64), repeat('u', 64), repeat('n', 64), FALSE,
               'COMPLETE', '2026-06-15 09:00:00+08', repeat('o', 63) || suffix, 'capture-' || suffix
          FROM seed;

        WITH seed(suffix) AS (VALUES ('a'::text), ('b'::text))
        INSERT INTO app.advisory_signal_stage_evidence (
            stage_evidence_id, observation_version_id, stage, capability_status, input_count, output_count,
            excluded_count, observed_max_rank, source_artifact_id, source_artifact_hash, content_hash,
            semantic_hash, score_direction, tie_break_policy_id, tie_break_policy_hash, reason_codes
        )
        SELECT 'stage-' || suffix, 'version-' || suffix, 'selection_effective', 'FULL', 1, 1, 0, 1,
               'artifact-' || suffix, repeat('a', 64), repeat('h', 63) || suffix, repeat('z', 64),
               'DESC', 'tie-v1', repeat('t', 64), '[]'::jsonb
          FROM seed;

        WITH seed(suffix) AS (VALUES ('a'::text), ('b'::text))
        INSERT INTO app.advisory_signal_observation_lineage (
            lineage_id, canonical_signal_id, observation_version_id, phase0a_audit_id,
            phase0a_audit_manifest_hash, handoff_readiness_hash, admission_scope_id, admission_scope_hash,
            audit_target_id, target_scope_hash, capability, stable_signal_semantics_hash,
            canonical_signal_scope_hash, phase0a_signal_context_hash, oos_interval_id, oos_interval_hash,
            evidence_scope, signal_evidence_level, effective_cutoff_date, program_id, binding_version_id,
            lineage_source_type, source_run_id, review_run_id, list_version_id, lineage_content_hash
        )
        SELECT 'lineage-' || suffix, 'signal-' || suffix, 'version-' || suffix, 'audit-' || suffix,
               repeat('a', 64), repeat('h', 64), 'scope-' || suffix, repeat('s', 64),
               'target-' || suffix, repeat('t', 64), 'FULL', repeat('s', 64), repeat(suffix, 64),
               repeat('p', 64), 'oos-' || suffix, repeat('i', 64), 'RETROSPECTIVE_RESEARCH_ONLY',
               'FULL', DATE '2026-06-14', 'program-' || suffix, 'binding-' || suffix,
               'PHASE0A_AUDIT', 'source-run-' || suffix, NULL, NULL, repeat('l', 63) || suffix
          FROM seed;

        WITH seed(suffix) AS (VALUES ('a'::text), ('b'::text))
        INSERT INTO app.advisory_signal_stage_candidate (
            stage_evidence_id, symbol, membership_status, rank, score_decimal, input_rank,
            input_score_decimal, exclusion_reason_code, component_capability,
            component_evidence_schema_version, component_evidence_json, component_evidence_hash,
            component_reason_codes, candidate_content_hash
        )
        SELECT 'stage-' || suffix, '00000' || CASE WHEN suffix = 'a' THEN '1' ELSE '2' END || '.SZ',
               'INCLUDED', 1, 1.250000000000, 1, 1.250000000000, NULL, 'NOT_APPLICABLE',
               NULL, NULL, NULL, '[]'::jsonb, repeat('d', 63) || suffix
          FROM seed;
        SET session_replication_role = origin;
        """,
    )


def _execute(config: DatabaseConnectionConfig, sql_text: str) -> None:
    connection = psycopg2.connect(**config.connect_kwargs())
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_text)
    finally:
        connection.close()


def _snapshot_compatibility_artifacts(config: DatabaseConnectionConfig, root: Path) -> tuple[tuple[Any, ...], ...]:
    reader = PostgresSnapshotSourceReader(conn_factory=lambda: None, evidence_reader=None)
    writer = DeterministicParquetWriter()
    connection = psycopg2.connect(**config.connect_kwargs())
    artifacts: list[tuple[Any, ...]] = []
    queries = {
        "lineage": """
            SELECT lineage.*, observation.decision_as_of_trade_date AS snapshot_decision_date
              FROM app.advisory_signal_observation_lineage lineage
              JOIN app.advisory_signal_observation_version observation_version
                ON observation_version.observation_version_id = lineage.observation_version_id
              JOIN app.advisory_signal_observation observation
                ON observation.canonical_signal_id = observation_version.canonical_signal_id
             ORDER BY observation.decision_as_of_trade_date, lineage.canonical_signal_id,
                      lineage.observation_version_id, lineage.lineage_id
        """,
        "stage_candidates": """
            SELECT candidate.*, observation.decision_as_of_trade_date AS snapshot_decision_date
              FROM app.advisory_signal_stage_candidate candidate
              JOIN app.advisory_signal_stage_evidence stage_evidence
                ON stage_evidence.stage_evidence_id = candidate.stage_evidence_id
              JOIN app.advisory_signal_observation_version observation_version
                ON observation_version.observation_version_id = stage_evidence.observation_version_id
              JOIN app.advisory_signal_observation observation
                ON observation.canonical_signal_id = observation_version.canonical_signal_id
             ORDER BY observation.decision_as_of_trade_date, candidate.stage_evidence_id, candidate.symbol
        """,
    }
    try:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        for role, query in queries.items():
            raw_rows = reader._query(connection, query, (), name=f"phase1f1_{role}")
            try:
                logical_rows = [
                    reader._logical(role, raw, decision_date=raw["snapshot_decision_date"]) for raw in raw_rows
                ]
            finally:
                raw_rows.close()
            groups: dict[tuple[tuple[str, str], ...], list[Any]] = {}
            for row in logical_rows:
                groups.setdefault(tuple(sorted(row.partition_key.items())), []).append(row)
            for ordinal, (partition_items, rows) in enumerate(sorted(groups.items()), start=1):
                path = root / f"{role}-{ordinal:03d}.parquet"
                descriptor = writer.write_parquet(
                    path=path,
                    logical_path=f"{role}/{ordinal:03d}.parquet",
                    logical_role=role,
                    partition_key=dict(partition_items),
                    ordinal=ordinal,
                    rows=sorted(rows, key=lambda item: item.sort_key),
                )
                artifacts.append(
                    (
                        role,
                        partition_items,
                        descriptor.sha256,
                        descriptor.size_bytes,
                        descriptor.row_count,
                        descriptor.schema_fingerprint,
                        descriptor.partition_content_hash,
                        path.read_bytes(),
                    )
                )
        connection.rollback()
        return tuple(sorted(artifacts, key=lambda item: (item[0], item[1])))
    finally:
        connection.close()


def test_full_frozen_apply_verify_reapply_and_evidence(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
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


def test_phase1f1_v1_rows_migrate_across_months_preserve_views_and_allow_scoped_duplicate_hashes(
    database_factory: Callable[..., DatabaseConnectionConfig],
    tmp_path: Path,
) -> None:
    config = database_factory()
    _apply_v1_predecessor(config)
    _seed_v1_cross_month_lineage_and_candidates(config)
    v1_snapshot_artifacts = _snapshot_compatibility_artifacts(config, tmp_path / "v1")
    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM app.advisory_signal_observation_lineage ORDER BY lineage_id")
            v1_lineage_rows = cursor.fetchall()
            lineage_columns = tuple(column.name for column in cursor.description)
            cursor.execute("SELECT * FROM app.advisory_signal_stage_candidate ORDER BY stage_evidence_id, symbol")
            v1_candidate_rows = cursor.fetchall()
            candidate_columns = tuple(column.name for column in cursor.description)
    finally:
        connection.close()

    contract, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    assert plan.managed_schema_status is ManagedSchemaStatus.PARTIAL_ADDITIVE
    assert plan.legacy_inventory is not None
    assert plan.legacy_inventory.predecessor_layout == "V1_TABLES"
    assert plan.legacy_inventory.lineage_row_count == 2
    assert plan.legacy_inventory.candidate_row_count == 2
    assert plan.legacy_inventory.legacy_months == (date(2026, 6, 1), date(2026, 7, 1))
    assert [item.migration_order for item in plan.pending_ddl_operations if item.kind == "MIGRATION"] == [60, 70, 80]

    receipt = apply_release_schema_plan(plan=plan, config=config, contract=contract)
    assert receipt.operation_status.value == "SUCCESS"
    assert receipt.managed_schema_status is ManagedSchemaStatus.COMPATIBLE
    assert receipt.legacy_inventory == plan.legacy_inventory
    assert _snapshot_compatibility_artifacts(config, tmp_path / "v2") == v1_snapshot_artifacts

    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT relkind FROM pg_class WHERE oid = 'app.advisory_signal_observation_lineage'::regclass"
            )
            assert cursor.fetchone()[0] == "v"
            cursor.execute("SELECT relkind FROM pg_class WHERE oid = 'app.advisory_signal_stage_candidate'::regclass")
            assert cursor.fetchone()[0] == "v"
            cursor.execute("SELECT * FROM app.advisory_signal_observation_lineage ORDER BY lineage_id")
            assert tuple(column.name for column in cursor.description) == lineage_columns
            assert cursor.fetchall() == v1_lineage_rows
            cursor.execute("SELECT * FROM app.advisory_signal_stage_candidate ORDER BY stage_evidence_id, symbol")
            assert tuple(column.name for column in cursor.description) == candidate_columns
            assert cursor.fetchall() == v1_candidate_rows
            cursor.execute(
                """
                SELECT tableoid::regclass::text
                  FROM app.advisory_signal_observation_lineage_payload
                 ORDER BY decision_as_of_trade_date
                """
            )
            assert [row[0] for row in cursor.fetchall()] == [
                "app.advisory_signal_observation_lineage_payload_202606",
                "app.advisory_signal_observation_lineage_payload_202607",
            ]
            cursor.execute(
                """
                SELECT tableoid::regclass::text
                  FROM app.advisory_signal_stage_candidate_payload
                 ORDER BY decision_as_of_trade_date
                """
            )
            assert [row[0] for row in cursor.fetchall()] == [
                "app.advisory_signal_stage_candidate_payload_202606",
                "app.advisory_signal_stage_candidate_payload_202607",
            ]

            with pytest.raises(psycopg2.Error):
                cursor.execute(
                    "UPDATE app.advisory_signal_observation_lineage SET source_run_id = source_run_id "
                    "WHERE lineage_id = 'lineage-a'"
                )
            connection.rollback()

            with pytest.raises(psycopg2.Error):
                cursor.execute(
                    "UPDATE app.advisory_signal_observation_lineage_identity SET source_run_id = 'mutated' "
                    "WHERE lineage_id = 'lineage-a'"
                )
            connection.rollback()
            with pytest.raises(psycopg2.Error):
                cursor.execute(
                    "UPDATE app.advisory_signal_stage_candidate_payload SET rank = 2 "
                    "WHERE stage_evidence_id = 'stage-a'"
                )
            connection.rollback()
            with pytest.raises(psycopg2.errors.UniqueViolation):
                cursor.execute(
                    """
                    INSERT INTO app.advisory_signal_observation_lineage_identity (
                        lineage_id, decision_as_of_trade_date, observation_version_id, phase0a_audit_id,
                        admission_scope_id, program_id, binding_version_id, lineage_source_type,
                        source_run_id, lineage_content_hash
                    ) VALUES (
                        'lineage-a-duplicate', DATE '2026-06-15', 'version-a', 'audit-a', 'scope-a',
                        'program-a', 'binding-a', 'PHASE0A_AUDIT', 'source-run-a', repeat('y', 64)
                    )
                    """
                )
            connection.rollback()
            with pytest.raises(psycopg2.errors.UniqueViolation):
                cursor.execute(
                    """
                    INSERT INTO app.advisory_signal_stage_candidate_identity (
                        stage_evidence_id, symbol, decision_as_of_trade_date
                    ) VALUES ('stage-a', '000001.SZ', DATE '2026-06-15')
                    """
                )
            connection.rollback()

            cursor.execute(
                """
                INSERT INTO app.advisory_signal_stage_evidence (
                    stage_evidence_id, observation_version_id, stage, capability_status, input_count, output_count,
                    excluded_count, observed_max_rank, source_artifact_id, source_artifact_hash, content_hash,
                    semantic_hash, score_direction, tie_break_policy_id, tie_break_policy_hash, reason_codes
                ) VALUES (
                    'stage-a-duplicate', 'version-a', 'advisory_model', 'FULL', 1, 1, 0, 1,
                    'artifact-duplicate', repeat('a', 64), repeat('h', 63) || 'a', repeat('z', 64),
                    'DESC', 'tie-v1', repeat('t', 64), '[]'::jsonb
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO app.advisory_signal_stage_candidate_identity (
                    stage_evidence_id, symbol, decision_as_of_trade_date
                ) VALUES ('stage-a-duplicate', '000001.SZ', DATE '2026-06-15')
                """
            )
            cursor.execute(
                """
                INSERT INTO app.advisory_signal_stage_candidate_payload (
                    decision_as_of_trade_date, stage_evidence_id, symbol, membership_status, rank,
                    score_decimal, input_rank, input_score_decimal, exclusion_reason_code,
                    component_capability, component_evidence_schema_version, component_evidence_json,
                    component_evidence_hash, component_reason_codes, candidate_content_hash
                ) VALUES (
                    DATE '2026-06-15', 'stage-a-duplicate', '000001.SZ', 'INCLUDED', 1,
                    1.250000000000, 1, 1.250000000000, NULL, 'NOT_APPLICABLE', NULL, NULL,
                    NULL, '[]'::jsonb, repeat('d', 63) || 'a'
                )
                """
            )
            cursor.execute(
                "SELECT count(*) FROM app.advisory_signal_stage_evidence WHERE content_hash = repeat('h', 63) || 'a'"
            )
            assert cursor.fetchone()[0] == 2
            cursor.execute(
                "SELECT count(*) FROM app.advisory_signal_stage_candidate_payload WHERE candidate_content_hash = repeat('d', 63) || 'a'"
            )
            assert cursor.fetchone()[0] == 2
        connection.commit()
    finally:
        connection.close()


def test_phase1f1_cutover_failure_rolls_back_copy_and_resumes_from_prepared_state(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
    config = database_factory()
    _apply_v1_predecessor(config)
    _seed_v1_cross_month_lineage_and_candidates(config)
    contract, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    _execute(
        config,
        """
        CREATE FUNCTION public.phase1f1_fail_cutover_view() RETURNS event_trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_TAG = 'CREATE VIEW' THEN
                RAISE EXCEPTION 'PHASE1F1_TEST_CUTOVER_VIEW_FAILURE';
            END IF;
        END;
        $$;
        CREATE EVENT TRIGGER phase1f1_fail_cutover_view ON ddl_command_start
            EXECUTE FUNCTION public.phase1f1_fail_cutover_view();
        """,
    )
    try:
        failed = apply_release_schema_plan(plan=plan, config=config, contract=contract)
    finally:
        _execute(
            config,
            "DROP EVENT TRIGGER IF EXISTS phase1f1_fail_cutover_view; DROP FUNCTION IF EXISTS public.phase1f1_fail_cutover_view()",
        )
    assert failed.operation_status.value == "FAILED"
    assert any(
        item.order == 60 and item.status is MigrationExecutionStatus.COMMITTED for item in failed.per_migration_results
    )
    assert any(
        item.order == 70 and item.status is MigrationExecutionStatus.COMMITTED for item in failed.per_migration_results
    )
    assert any(
        item.order == 80 and item.status is MigrationExecutionStatus.FAILED for item in failed.per_migration_results
    )

    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT relkind FROM pg_class WHERE oid = 'app.advisory_signal_observation_lineage'::regclass"
            )
            assert cursor.fetchone()[0] == "r"
            cursor.execute("SELECT relkind FROM pg_class WHERE oid = 'app.advisory_signal_stage_candidate'::regclass")
            assert cursor.fetchone()[0] == "r"
            cursor.execute("SELECT count(*) FROM app.advisory_signal_observation_lineage_identity")
            assert cursor.fetchone()[0] == 0
            cursor.execute("SELECT count(*) FROM app.advisory_signal_stage_candidate_identity")
            assert cursor.fetchone()[0] == 0
    finally:
        connection.close()

    _, resume_request = _request(config)
    resume_plan = build_release_schema_plan(config=config, contract=contract, request=resume_request)
    assert resume_plan.managed_schema_status is ManagedSchemaStatus.PARTIAL_ADDITIVE
    assert [item.migration_order for item in resume_plan.pending_ddl_operations if item.kind == "MIGRATION"] == [80]
    resumed = apply_release_schema_plan(plan=resume_plan, config=config, contract=contract)
    assert resumed.operation_status.value == "SUCCESS"
    assert resumed.managed_schema_status is ManagedSchemaStatus.COMPATIBLE


def test_phase1f1_legacy_inventory_change_invalidates_the_frozen_plan_before_ddl(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
    config = database_factory()
    _apply_v1_predecessor(config)
    _seed_v1_cross_month_lineage_and_candidates(config)
    contract, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    _execute(
        config,
        """
        INSERT INTO app.advisory_signal_stage_candidate (
            stage_evidence_id, symbol, membership_status, rank, score_decimal, input_rank,
            input_score_decimal, exclusion_reason_code, component_capability,
            component_evidence_schema_version, component_evidence_json, component_evidence_hash,
            component_reason_codes, candidate_content_hash
        ) VALUES (
            'stage-a', '000003.SZ', 'INCLUDED', 2, 0.750000000000, 2, 0.750000000000,
            NULL, 'NOT_APPLICABLE', NULL, NULL, NULL, '[]'::jsonb, repeat('x', 64)
        )
        """,
    )
    with pytest.raises(ReleaseSchemaApplyError) as error:
        apply_release_schema_plan(plan=plan, config=config, contract=contract)
    assert error.value.reason_code == REASON_PLAN_STALE
    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('app.advisory_signal_observation_lineage_identity')")
            assert cursor.fetchone()[0] is None
    finally:
        connection.close()


def test_phase1f1_orphan_legacy_parent_date_fails_during_read_only_plan_build(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
    config = database_factory()
    _apply_v1_predecessor(config)
    _seed_v1_cross_month_lineage_and_candidates(config)
    _execute(
        config,
        """
        SET session_replication_role = replica;
        INSERT INTO app.advisory_signal_observation_lineage (
            lineage_id, canonical_signal_id, observation_version_id, phase0a_audit_id,
            phase0a_audit_manifest_hash, handoff_readiness_hash, admission_scope_id, admission_scope_hash,
            audit_target_id, target_scope_hash, capability, stable_signal_semantics_hash,
            canonical_signal_scope_hash, phase0a_signal_context_hash, oos_interval_id, oos_interval_hash,
            evidence_scope, signal_evidence_level, effective_cutoff_date, program_id, binding_version_id,
            lineage_source_type, source_run_id, review_run_id, list_version_id, lineage_content_hash
        ) VALUES (
            'lineage-orphan', 'signal-a', 'missing-version', 'audit-orphan', repeat('a', 64), repeat('h', 64),
            'scope-orphan', repeat('s', 64), 'target-orphan', repeat('t', 64), 'FULL', repeat('s', 64),
            repeat('o', 64), repeat('p', 64), 'oos-orphan', repeat('i', 64), 'RETROSPECTIVE_RESEARCH_ONLY',
            'FULL', DATE '2026-06-14', 'program-orphan', 'binding-orphan', 'PHASE0A_AUDIT',
            'source-run-orphan', NULL, NULL, repeat('z', 64)
        );
        SET session_replication_role = origin;
        """,
    )
    contract, request = _request(config)
    with pytest.raises(ReleaseSchemaApplyError) as error:
        build_release_schema_plan(config=config, contract=contract, request=request)
    assert error.value.reason_code == "ADVISORY_PHASE1F1_PARENT_DATE_UNRESOLVED"
    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('app.advisory_signal_observation_lineage_identity')")
            assert cursor.fetchone()[0] is None
    finally:
        connection.close()


def test_phase1f1_unknown_v1_child_drift_is_not_silently_repaired(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
    config = database_factory()
    _apply_v1_predecessor(config)
    _execute(
        config,
        "ALTER TABLE app.advisory_signal_observation_lineage "
        "ADD CONSTRAINT phase1f1_unknown_lineage_check CHECK (lineage_id <> '')",
    )
    contract, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    assert plan.managed_schema_status is ManagedSchemaStatus.DRIFTED
    assert not plan.pending_ddl_operations
    assert any(
        item.object_id == "constraint:app.advisory_signal_observation_lineage.phase1f1_unknown_lineage_check"
        and not item.repairable_by_orders
        for item in plan.managed_differences
    )
    with pytest.raises(ReleaseSchemaApplyError) as error:
        apply_release_schema_plan(plan=plan, config=config, contract=contract)
    assert error.value.reason_code == REASON_PHASE1F1_CATALOG_DRIFTED


def test_phase1f1_isolated_legacy_unique_remnant_is_drift_not_unrunnable_partial(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
    config = database_factory()
    contract, _ = _fresh_apply(config)
    _execute(
        config,
        "ALTER TABLE app.advisory_signal_stage_evidence "
        "ADD CONSTRAINT advisory_signal_stage_evidence_content_hash_key UNIQUE (content_hash)",
    )
    _, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    assert plan.managed_schema_status is ManagedSchemaStatus.DRIFTED
    assert not plan.pending_ddl_operations


def test_phase1f1_copy_mismatch_rolls_back_before_authority_swap(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
    config = database_factory()
    _apply_v1_predecessor(config)
    _seed_v1_cross_month_lineage_and_candidates(config)
    contract, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    prepare = next(item for item in contract.managed_migrations if item.order == 60)
    cutover = next(item for item in contract.managed_migrations if item.order == 80)
    _execute_file_wrapped_migration(config=config, contract=contract, migration=prepare)
    apply_module._execute_partitions(
        config=config,
        contract=contract,
        partitions=plan.expected_partitions,
    )
    _execute(
        config,
        """
        CREATE FUNCTION public.phase1f1_drop_candidate_copy() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RETURN NULL;
        END;
        $$;
        CREATE TRIGGER phase1f1_drop_candidate_copy
            BEFORE INSERT ON app.advisory_signal_stage_candidate_payload
            FOR EACH ROW EXECUTE FUNCTION public.phase1f1_drop_candidate_copy();
        """,
    )
    with pytest.raises(ReleaseSchemaApplyError) as error:
        apply_module._execute_phase1f1_cutover_migration(
            config=config,
            contract=contract,
            migration=cutover,
            expected_partitions=plan.expected_partitions,
            legacy_inventory=plan.legacy_inventory,
            request=request,
        )
    assert error.value.reason_code == REASON_PHASE1F1_COPY_MISMATCH
    connection = psycopg2.connect(**config.connect_kwargs())
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT relkind FROM pg_class WHERE oid = 'app.advisory_signal_observation_lineage'::regclass"
            )
            assert cursor.fetchone()[0] == "r"
            cursor.execute("SELECT count(*) FROM app.advisory_signal_observation_lineage_identity")
            assert cursor.fetchone()[0] == 0
            cursor.execute("SELECT count(*) FROM app.advisory_signal_stage_candidate_identity")
            assert cursor.fetchone()[0] == 0
    finally:
        connection.close()


def test_phase1f1_post_failure_readback_error_is_logged_and_recorded(
    database_factory: Callable[..., DatabaseConnectionConfig],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = database_factory()
    _apply_v1_predecessor(config)
    contract, request = _request(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    original_verify = apply_module.verify_database_catalog
    state = {"cutover_failed": False}

    def fail_cutover(**_: Any) -> None:
        state["cutover_failed"] = True
        raise ReleaseSchemaApplyError(REASON_DDL_EXECUTION_FAILED, "injected cutover failure")

    def fail_post_failure_readback(**kwargs: Any) -> Any:
        if state["cutover_failed"]:
            raise RuntimeError("injected post-failure readback failure")
        return original_verify(**kwargs)

    monkeypatch.setattr(apply_module, "_execute_phase1f1_cutover_migration", fail_cutover)
    monkeypatch.setattr(apply_module, "verify_database_catalog", fail_post_failure_readback)
    with caplog.at_level(logging.ERROR, logger=apply_module.LOGGER.name):
        receipt = apply_release_schema_plan(plan=plan, config=config, contract=contract)
    assert receipt.operation_status.value == "FAILED"
    assert [item["reason_code"] for item in receipt.errors] == [
        REASON_DDL_EXECUTION_FAILED,
        REASON_PHASE1F1_POST_FAILURE_VERIFY_FAILED,
    ]
    readback_records = [item for item in caplog.records if "post-failure catalog readback failed" in item.getMessage()]
    assert len(readback_records) == 1
    assert readback_records[0].exc_info is not None


def test_missing_external_prerequisite_does_not_block_managed_apply(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
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
    """
    CREATE OR REPLACE VIEW app.advisory_signal_stage_candidate AS
    SELECT identity.stage_evidence_id,
           identity.symbol,
           payload.membership_status,
           payload.rank,
           payload.score_decimal,
           payload.input_rank,
           payload.input_score_decimal,
           payload.exclusion_reason_code,
           payload.component_capability,
           payload.component_evidence_schema_version,
           payload.component_evidence_json,
           payload.component_evidence_hash,
           payload.component_reason_codes,
           payload.candidate_content_hash,
           payload.created_at
      FROM app.advisory_signal_stage_candidate_identity identity
      JOIN app.advisory_signal_stage_candidate_payload payload
        ON payload.stage_evidence_id = identity.stage_evidence_id
       AND payload.symbol = identity.symbol
       AND payload.decision_as_of_trade_date = identity.decision_as_of_trade_date
     WHERE identity.symbol IS NOT NULL
    """,
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
    assert error.value.reason_code == REASON_PHASE1F1_CATALOG_DRIFTED


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
    assert [
        item.order for item in failed.per_migration_results if item.status is MigrationExecutionStatus.COMMITTED
    ] == [10, 20]
    assert failed.per_migration_results[-1].order == 30
    assert failed.per_migration_results[-1].status is MigrationExecutionStatus.FAILED

    monkeypatch.setattr(apply_module, "_execute_file_wrapped_migration", original)
    _, resume_request = _request(config)
    resume_plan = build_release_schema_plan(config=config, contract=contract, request=resume_request)
    pending_orders = [item.migration_order for item in resume_plan.pending_ddl_operations if item.kind == "MIGRATION"]
    assert pending_orders == [30, 40, 50, 55, 60, 70, 80]
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


def test_lock_timeout_has_no_retry_and_resume_succeeds(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
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


def test_session_policy_and_statement_timeout_classification(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
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
    assert (
        _timeout_reason(psycopg2.errors.QueryCanceled("canceling statement due to statement timeout"))
        == "PHASE1F_DDL_STATEMENT_TIMEOUT"
    )


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


def test_verifier_query_spy_is_catalog_only_and_read_only(
    database_factory: Callable[..., DatabaseConnectionConfig],
) -> None:
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
    assert not any(
        token in statement
        for statement in normalized
        for token in ("insert ", "update ", "delete ", "alter ", "create ", "drop ")
    )
    assert not any(" from app." in statement or " from market." in statement for statement in normalized)
