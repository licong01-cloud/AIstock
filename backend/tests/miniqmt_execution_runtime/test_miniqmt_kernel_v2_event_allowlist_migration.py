from __future__ import annotations

from hashlib import sha256
import os

import psycopg2
import pytest

from backend.services.miniqmt_execution_runtime.quote_event_schema import (
    EVENT_ROUTING_COMPOSITES_V1,
    KERNEL_V2_EVENT_SOURCES,
    KERNEL_V2_EVENT_TYPES,
    QUOTE_V1_EVENT_SOURCES,
    QUOTE_V1_EVENT_TYPES,
    TARGET_EVENT_SOURCES,
    TARGET_EVENT_TYPES,
    allowed_literals_from_constraint,
    event_composites_from_constraint,
    kernel_v2_migration_path,
    kernel_v2_rollback_path,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_migration_postgres import (
    FORWARD as K2_FORWARD,
    _apply_base_forward,
    _base_fixture_sql,
    _dev_dsn,
    _fixture_schema,
)


def _constraint_definition(cursor: object, schema: str, name: str) -> str:
    cursor.execute(  # type: ignore[attr-defined]
        "SELECT pg_get_constraintdef(oid,true) FROM pg_constraint "
        "WHERE conrelid=%s::regclass AND conname=%s AND contype='c'",
        (f"{schema}.execution_runtime_event", name),
    )
    row = cursor.fetchone()  # type: ignore[attr-defined]
    assert row is not None
    return str(row[0])


def _quote_constraints_sql(schema: str) -> str:
    event_types = ",".join(f"'{item}'" for item in sorted(QUOTE_V1_EVENT_TYPES))
    event_sources = ",".join(f"'{item}'" for item in sorted(QUOTE_V1_EVENT_SOURCES))
    return f"""
    ALTER TABLE {schema}.execution_runtime_event
      ADD CONSTRAINT ck_miniqmt_event_type CHECK (event_type IN ({event_types}));
    ALTER TABLE {schema}.execution_runtime_event
      ADD CONSTRAINT ck_miniqmt_event_source CHECK (source IN ({event_sources}));
    """


def _allowlist_definition(column: str, values: frozenset[str]) -> str:
    return f"CHECK ({column} IN ({','.join(repr(item) for item in sorted(values))}))"


def _insert_kernel_event(
    cursor: object, schema: str, *, sequence: int, event_type: str, source: str, payload_schema: str
) -> None:
    sha = f"{sequence:x}" * 64
    cursor.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_runtime_event(
            event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
            event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
            logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
            ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
        ) VALUES (
            %s,'runtime_bug1004',%s,%s,now(),%s,'{{}}'::jsonb,'KERNEL_V2',
            'miniqmt_runtime_event_envelope_v2',%s,%s,%s,now(),now(),
            '{{}}'::jsonb,'{{}}'::jsonb,'{{}}'::jsonb,%s,'miniqmt_event_routing_v1',%s
        )
        """,
        (
            f"event_bug1004_{sequence}",
            sequence,
            event_type,
            source,
            payload_schema,
            sha,
            sha,
            sha,
            f"tx_bug1004_{sequence}",
        ),
    )


def test_successor_migration_contains_complete_authority_and_guarded_rollback() -> None:
    forward = kernel_v2_migration_path().read_text(encoding="utf-8")
    rollback = kernel_v2_rollback_path().read_text(encoding="utf-8")
    assert "SHARE ROW EXCLUSIVE" in forward and "SHARE ROW EXCLUSIVE" in rollback
    assert "exact KERNEL_V2 event CHECK authority already present" in forward
    assert "independent post-DDL event CHECK readback drift" in forward
    assert "rollback refused" in rollback
    assert "kernel_only_count" in rollback and "min_sequence" in rollback and "max_sequence" in rollback
    assert "CREATE TABLE" not in forward and "CREATE INDEX" not in forward
    assert "INSERT INTO" not in forward and "UPDATE " not in forward and "DELETE FROM" not in forward
    assert "UPDATE " not in rollback and "DELETE FROM" not in rollback
    assert "event type/source CHECK expression drift" in forward
    assert "event type/source CHECK expression drift" in rollback
    for value in TARGET_EVENT_TYPES | TARGET_EVENT_SOURCES:
        assert value in forward
    for event_type, source, payload_schema in EVENT_ROUTING_COMPOSITES_V1:
        assert event_type in forward and source in forward and payload_schema in forward


def test_successor_migration_hashes_are_canonical_lf() -> None:
    from backend.services.miniqmt_execution_runtime.quote_event_schema import (
        kernel_v2_migration_file_sha256,
        kernel_v2_rollback_file_sha256,
    )

    for path, actual in (
        (kernel_v2_migration_path(), kernel_v2_migration_file_sha256()),
        (kernel_v2_rollback_path(), kernel_v2_rollback_file_sha256()),
    ):
        canonical = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
        assert actual == sha256(canonical.encode("utf-8")).hexdigest()


def test_python_target_is_exact_union_of_quote_and_kernel_authorities() -> None:
    assert TARGET_EVENT_TYPES == QUOTE_V1_EVENT_TYPES | KERNEL_V2_EVENT_TYPES
    assert TARGET_EVENT_SOURCES == QUOTE_V1_EVENT_SOURCES | KERNEL_V2_EVENT_SOURCES
    assert {item[0] for item in EVENT_ROUTING_COMPOSITES_V1} == KERNEL_V2_EVENT_TYPES
    assert {item[1] for item in EVENT_ROUTING_COMPOSITES_V1} == KERNEL_V2_EVENT_SOURCES


def test_forward_second_apply_kernel_rows_and_guarded_rollback_on_dev_postgres() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K2 DEV PostgreSQL fixture")
    schema = _fixture_schema()
    connection = psycopg2.connect(**_dev_dsn())
    connection.autocommit = True
    forward = kernel_v2_migration_path().read_text(encoding="utf-8").replace("qmt_strategy", schema)
    rollback = kernel_v2_rollback_path().read_text(encoding="utf-8").replace("qmt_strategy", schema)
    try:
        with connection.cursor() as cursor:
            cursor.execute(_base_fixture_sql(schema))
            cursor.execute(_quote_constraints_sql(schema))
            _apply_base_forward(cursor, K2_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            predecessor_composite = _constraint_definition(cursor, schema, "ck_miniqmt_k2_event_composite")

            cursor.execute(
                f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_type, "
                "ADD CONSTRAINT ck_miniqmt_event_type CHECK ("
                f"event_type IN ({','.join(repr(item) for item in sorted(QUOTE_V1_EVENT_TYPES))}) OR event_type IS NULL)"
            )
            with pytest.raises(psycopg2.Error, match="CHECK expression drift"):
                cursor.execute(forward)
            cursor.execute("ROLLBACK")
            cursor.execute(f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_type")
            cursor.execute(
                f"ALTER TABLE {schema}.execution_runtime_event ADD CONSTRAINT ck_miniqmt_event_type "
                f"{_allowlist_definition('event_type', QUOTE_V1_EVENT_TYPES)}"
            )

            cursor.execute(
                f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_miniqmt_k2_event_composite"
            )
            cursor.execute(
                f"ALTER TABLE {schema}.execution_runtime_event ADD CONSTRAINT ck_miniqmt_k2_event_composite "
                + predecessor_composite.replace(
                    "payload_schema_version = 'miniqmt_algo_start_v1'::text",
                    "payload_schema_version = 'miniqmt_algo_start_v1'::text AND event_time IS NOT NULL",
                )
            )
            with pytest.raises(psycopg2.Error, match="composite constraint drift"):
                cursor.execute(forward)
            cursor.execute("ROLLBACK")
            cursor.execute(
                f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_miniqmt_k2_event_composite"
            )
            cursor.execute(
                f"ALTER TABLE {schema}.execution_runtime_event ADD CONSTRAINT ck_miniqmt_k2_event_composite "
                + predecessor_composite
            )

            cursor.execute(forward)
            first_definitions = tuple(
                _constraint_definition(cursor, schema, name)
                for name in ("ck_miniqmt_event_type", "ck_miniqmt_event_source", "ck_miniqmt_k2_event_composite")
            )
            cursor.execute(forward)
            assert (
                tuple(
                    _constraint_definition(cursor, schema, name)
                    for name in ("ck_miniqmt_event_type", "ck_miniqmt_event_source", "ck_miniqmt_k2_event_composite")
                )
                == first_definitions
            )
            assert allowed_literals_from_constraint(first_definitions[0], column="event_type") == TARGET_EVENT_TYPES
            assert allowed_literals_from_constraint(first_definitions[1], column="source") == TARGET_EVENT_SOURCES
            assert event_composites_from_constraint(first_definitions[2]) == EVENT_ROUTING_COMPOSITES_V1

            cursor.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES ('runtime_bug1004','2026-08-10')"
            )
            _insert_kernel_event(
                cursor,
                schema,
                sequence=1,
                event_type="SESSION",
                source="EXCHANGE_SESSION_CLOCK",
                payload_schema="miniqmt_session_event_v1",
            )
            _insert_kernel_event(
                cursor,
                schema,
                sequence=2,
                event_type="ALGO_START",
                source="MINIQMT_EXECUTION_KERNEL",
                payload_schema="miniqmt_algo_start_v2",
            )
            _insert_kernel_event(
                cursor,
                schema,
                sequence=3,
                event_type="COMMAND_OUTCOME",
                source="MINIQMT_EXECUTION_KERNEL",
                payload_schema="miniqmt_command_outcome_v1",
            )
            with pytest.raises(psycopg2.Error, match="rollback refused"):
                cursor.execute(rollback)
            cursor.execute("ROLLBACK")
            cursor.execute(f"DELETE FROM {schema}.execution_runtime_event WHERE runtime_id='runtime_bug1004'")
            cursor.execute(f"DELETE FROM {schema}.execution_runtime WHERE runtime_id='runtime_bug1004'")
            cursor.execute(rollback)
            cursor.execute(rollback)
            assert "SESSION" not in _constraint_definition(cursor, schema, "ck_miniqmt_event_type")
            assert "EXCHANGE_SESSION_CLOCK" not in _constraint_definition(cursor, schema, "ck_miniqmt_event_source")
    finally:
        with connection.cursor() as cursor:
            cursor.execute("ROLLBACK")
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        connection.close()
