from __future__ import annotations

from pathlib import Path

import psycopg2
import pytest

from backend.services.miniqmt_execution_runtime.kernel_repository import PostgresMiniQMTKernelRepository
from backend.tests.miniqmt_execution_runtime.test_kernel_migration_postgres import (
    EVENT_CONTRACT_REPAIR_FORWARD,
    _apply_current_k6_predecessor,
    _base_fixture_sql,
    _dev_dsn,
    _fixture_schema,
    _install_event_contract_predecessor,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import _conn_factory


ROOT = Path(__file__).resolve().parents[3]
PREFLIGHT = ROOT / "backend/migrations/miniqmt_hot_market_data_zero_persistence_20260812.preflight.sql"
FORWARD = ROOT / "backend/migrations/miniqmt_hot_market_data_zero_persistence_20260812.sql"
ROLLBACK = ROOT / "backend/migrations/miniqmt_hot_market_data_zero_persistence_20260812.rollback.sql"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")


def test_successor_migration_requires_exact_base_and_only_adds_no_tick_guard() -> None:
    sql = _text(FORWARD)
    assert "exact BUG-1019 target CHECK authority is required" in sql
    assert "target_source_sha256" not in sql
    assert "c2f8e672b140ec88f667e251bbb5ff812cd0bea2a24f31c45d74c3f8d32eb881" in sql
    assert "ck_miniqmt_no_new_kernel_tick" in sql
    assert "event_contract_version<>'KERNEL_V2' OR event_type<>'TICK'" in sql
    assert "VALIDATE CONSTRAINT ck_miniqmt_no_new_kernel_tick" not in sql
    assert "DROP CONSTRAINT" not in sql
    assert "active pre-V4 algorithm instances remain" in sql


def test_successor_preflight_is_read_only_and_retains_historical_tick_rows() -> None:
    sql = _text(PREFLIGHT)
    for forbidden in ("INSERT INTO", "UPDATE ", "DELETE FROM", "ALTER TABLE", "DROP TABLE"):
        assert forbidden not in sql
    assert "event_type<>'TICK'" in sql
    assert "Historical KERNEL_V2 TICK rows are explicitly preserved read-only" in sql
    assert "apply the exact BUG-1019 event-contract repair first" in sql
    for expected_sha256 in (
        "836b7f7ebf14ee61ec94c9df82b300b42c96ff1046de0a2e0cfb8bc0f400642d",
        "a1b188a1431066f2e8f2d0d51107b8c0532830ca7b88567ba1903c4b3999a3d0",
        "6ac3041d989166511127ec22d9379dd0ecdc09fb5055e72006100319026a6f24",
        "c2f8e672b140ec88f667e251bbb5ff812cd0bea2a24f31c45d74c3f8d32eb881",
        "4a2d33d3fc75a4b468661e1bdbf2ecce9cd13aaab491c7c4d7605a1df3af3857",
        "888bebaf7d9540ecadae15bfb7d2944db59177b4ed2ef5e8beb231b803f9faca",
    ):
        assert expected_sha256 in sql


def test_guarded_rollback_never_restores_durable_tick_writer() -> None:
    sql = _text(ROLLBACK)
    assert "safe no-op" in sql
    assert "ck_miniqmt_no_new_kernel_tick" in sql
    assert "ADD CONSTRAINT" not in sql
    assert "DROP CONSTRAINT" not in sql
    assert "9dd2d0274fe18ad4ab487f006e420e6f11b806818cd876ebabf3d3f286cc4bed" in sql
    assert "position(" not in sql


def test_no_successor_artifact_guesses_nonexistent_diagnostic_columns() -> None:
    combined = "\n".join(_text(path) for path in (PREFLIGHT, FORWARD, ROLLBACK))
    for guessed in ("SELECT event_sequence", "SELECT occurred_at", "SELECT created_at", ",occurred_at", ",created_at"):
        assert guessed.lower() not in combined.lower()


def test_successor_preserves_history_rejects_new_tick_and_keeps_business_events_on_dev_postgres() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1041_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    preflight = _text(PREFLIGHT).replace("qmt_strategy", schema)
    forward = _text(FORWARD).replace("qmt_strategy", schema)
    repair = EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    sha = "a" * 64
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            cur.execute(repair)
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES ('runtime_bug1041','2026-08-12')"
            )
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_runtime_event(
                    event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
                    event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
                    logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
                    ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                ) VALUES (
                    'historical_tick_bug1041','runtime_bug1041',1,'TICK',now(),'B0_QUOTE_V2','{{}}'::jsonb,
                    'KERNEL_V2','miniqmt_runtime_event_envelope_v2','miniqmt_market_data_view_v2',
                    %s,%s,now(),now(),'{{}}'::jsonb,'{{}}'::jsonb,'{{}}'::jsonb,%s,
                    'miniqmt_event_routing_v1','tx_historical_tick_bug1041'
                )
                """,
                (sha, sha, sha),
            )
            cur.execute(preflight)
            cur.execute(forward)
            cur.execute(
                f"SELECT convalidated,pg_get_constraintdef(oid,true) FROM pg_constraint "
                f"WHERE conrelid='{schema}.execution_runtime_event'::regclass "
                "AND conname='ck_miniqmt_no_new_kernel_tick'"
            )
            validated, definition = cur.fetchone()
            assert validated is False
            assert "event_contract_version <> 'KERNEL_V2'" in definition
            cur.execute(
                f"SELECT oid::text,xmin::text FROM pg_constraint "
                f"WHERE conrelid='{schema}.execution_runtime_event'::regclass "
                "AND conname='ck_miniqmt_no_new_kernel_tick'"
            )
            guard_identity = tuple(cur.fetchone())
            cur.execute(forward)
            cur.execute(
                f"SELECT oid::text,xmin::text FROM pg_constraint "
                f"WHERE conrelid='{schema}.execution_runtime_event'::regclass "
                "AND conname='ck_miniqmt_no_new_kernel_tick'"
            )
            assert tuple(cur.fetchone()) == guard_identity
            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_no_new_kernel_tick"):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_runtime_event(
                        event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
                        event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
                        logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
                        ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                    ) VALUES (
                        'new_tick_bug1041','runtime_bug1041',2,'TICK',now(),'B0_QUOTE_V2','{{}}'::jsonb,
                        'KERNEL_V2','miniqmt_runtime_event_envelope_v2','miniqmt_market_data_view_v2',
                        %s,%s,now(),now(),'{{}}'::jsonb,'{{}}'::jsonb,'{{}}'::jsonb,%s,
                        'miniqmt_event_routing_v1','tx_new_tick_bug1041'
                    )
                    """,
                    (sha, sha, sha),
                )
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_runtime_event(
                    event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
                    event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
                    logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
                    ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                ) VALUES (
                    'session_bug1041','runtime_bug1041',2,'SESSION',now(),'EXCHANGE_SESSION_CLOCK','{{}}'::jsonb,
                    'KERNEL_V2','miniqmt_runtime_event_envelope_v2','miniqmt_session_event_v1',
                    %s,%s,now(),now(),'{{}}'::jsonb,'{{}}'::jsonb,'{{}}'::jsonb,%s,
                    'miniqmt_event_routing_v1','tx_session_bug1041'
                )
                """,
                ("b" * 64, "c" * 64, "d" * 64),
            )
            cur.execute(f"SELECT {schema}.miniqmt_k2_catalog_fingerprint()")
            catalog_sha256 = str(cur.fetchone()[0])
            assert len(catalog_sha256) == 64
            assert catalog_sha256 == "87725c53a3789c4ecf92d6f86de424d3b84c35cbfa6a9e85e47532ac4d978f54"
            cur.execute(f"SELECT event_id FROM {schema}.execution_runtime_event ORDER BY sequence,event_id")
            assert [item[0] for item in cur.fetchall()] == ["historical_tick_bug1041", "session_bug1041"]
        assert all(PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema)).preflight_schema().values())
    finally:
        conn.rollback()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()
