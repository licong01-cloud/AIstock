from __future__ import annotations

import hashlib
import os
from pathlib import Path

import psycopg2
import pytest

from backend.tests.miniqmt_execution_runtime.test_kernel_migration_postgres import (
    FORWARD as K2_FORWARD,
    K2C_FORWARD,
    K2D_FORWARD,
    _apply_base_forward,
    _base_fixture_sql,
    _dev_dsn,
    _fixture_schema,
    _insert_valid_k2_constraint_graph,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION_ROOT = REPO_ROOT / "backend" / "migrations"
PREFLIGHT = MIGRATION_ROOT / "miniqmt_execution_kernel_k6_20260801.preflight.sql"
FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k6_20260801.sql"
ROLLBACK = MIGRATION_ROOT / "miniqmt_execution_kernel_k6_20260801.rollback.sql"
CATALOG_SHA256 = "546a209dc2f8721ccee8b5e905117788486307147dfb4fc6bc396842f5cf84ad"


def _canonical_lf_sha256(path: Path) -> str:
    value = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def test_k6_migration_triplet_is_additive_exact_and_guarded() -> None:
    preflight = PREFLIGHT.read_text(encoding="utf-8")
    forward = FORWARD.read_text(encoding="utf-8")
    rollback = ROLLBACK.read_text(encoding="utf-8")
    assert "SET TRANSACTION READ ONLY" in preflight
    assert _canonical_lf_sha256(FORWARD) in preflight
    assert "__K6_" not in preflight + forward + rollback
    assert forward.count("CREATE TABLE IF NOT EXISTS qmt_strategy.execution_") == 7
    assert "execution_dependent_buy_coordination" in forward
    assert "execution_product_command_authority_item" in forward
    assert "execution_product_route_owner" in forward
    assert "DEFERRABLE INITIALLY DEFERRED" in forward
    assert "RELEASE_READY" not in forward
    assert "miniqmt_k6_catalog_fingerprint" in preflight and "miniqmt_k6_catalog_fingerprint" in forward
    assert CATALOG_SHA256 in preflight and CATALOG_SHA256 in forward
    assert "obj_description" in forward and "col_description" in forward
    assert "destructive rollback refused" in rollback
    assert "DROP TABLE" not in forward
    assert "INSERT INTO" not in forward


def _apply_k2_and_k6(cur: object, schema: str) -> None:
    cur.execute(_base_fixture_sql(schema))  # type: ignore[attr-defined]
    _apply_base_forward(cur, K2_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
    cur.execute(K2C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]
    cur.execute(K2D_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]
    cur.execute(PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]
    sql = FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    cur.execute(sql)  # type: ignore[attr-defined]
    cur.execute(sql)  # type: ignore[attr-defined]


def _apply_k2_only(cur: object, schema: str) -> None:
    cur.execute(_base_fixture_sql(schema))  # type: ignore[attr-defined]
    _apply_base_forward(cur, K2_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
    cur.execute(K2C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]
    cur.execute(K2D_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]


def test_k6_preflight_forward_second_apply_readback_and_guarded_rollback_on_dev() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6a_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _apply_k2_and_k6(cur, schema)
            cur.execute(f"SELECT {schema}.miniqmt_k6_catalog_fingerprint()")
            assert str(cur.fetchone()[0]) == CATALOG_SHA256
            cur.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema=%s "
                "AND table_name LIKE 'execution_dependent_buy_%%' OR table_schema=%s "
                "AND table_name LIKE 'execution_product_%%'",
                (schema, schema),
            )
            assert int(cur.fetchone()[0]) == 7
            cur.execute(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
                "WHERE n.nspname=%s AND c.relkind='r' AND c.relname LIKE 'execution_%%' "
                "AND (c.relname LIKE 'execution_dependent_buy_%%' OR c.relname LIKE 'execution_product_%%') "
                "AND coalesce(col_description(c.oid,a.attnum),'')=''",
                (schema,),
            )
            assert int(cur.fetchone()[0]) == 0
            cur.execute(ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(f"SELECT to_regclass('{schema}.execution_product_route_owner')")
            assert cur.fetchone()[0] is None

        with conn.cursor() as cur:
            _apply_k2_and_k6(cur, schema + "b")
            _insert_valid_k2_constraint_graph(cur, schema + "b")
            sha = "a" * 64
            cur.execute(
                f"""
                INSERT INTO {schema}b.execution_product_route_cutover(
                    runtime_id,binding_id,trade_date,route_epoch,route_owner,effective_new_instance_sequence,
                    legacy_active_instance_count,kernel_active_instance_count,catalog_sha256,
                    gateway_capability_catalog_sha256,exchange_session_authority_sha256,migration_readback_sha256,
                    product_authority_schema_sha256,previous_receipt_sha256,created_at_utc,carrier_json,receipt_sha256
                ) VALUES ('runtime_constraints','binding_k6','2026-07-25',1,'LEGACY_DRAIN_ONLY',1,1,0,
                          %s,%s,%s,%s,%s,NULL,now(),'{{}}'::jsonb,%s)
                """,
                (sha, sha, sha, sha, sha, sha),
            )
            with pytest.raises(psycopg2.errors.RaiseException, match="destructive rollback refused"):
                cur.execute(ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema + "b"))
            cur.execute("ROLLBACK")
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema}b CASCADE")
        conn.close()


def test_k6_preflight_and_rollback_fail_closed_for_partial_schema_on_dev() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6 DEV PostgreSQL fixture")
    partial_function_schema = _fixture_schema().replace("k2a_", "k6partialfn_", 1)
    partial_table_schema = _fixture_schema().replace("k2a_", "k6partialtable_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _apply_k2_only(cur, partial_function_schema)
            cur.execute(
                f"CREATE FUNCTION {partial_function_schema}.miniqmt_k6_reject_immutable_mutation() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$"
            )
            with pytest.raises(psycopg2.errors.RaiseException, match="partial schema function count"):
                cur.execute(PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", partial_function_schema))
            cur.execute("ROLLBACK")

        with conn.cursor() as cur:
            _apply_k2_and_k6(cur, partial_table_schema)
            _insert_valid_k2_constraint_graph(cur, partial_table_schema)
            sha = "a" * 64
            cur.execute(
                f"""
                INSERT INTO {partial_table_schema}.execution_product_route_cutover(
                    runtime_id,binding_id,trade_date,route_epoch,route_owner,effective_new_instance_sequence,
                    legacy_active_instance_count,kernel_active_instance_count,catalog_sha256,
                    gateway_capability_catalog_sha256,exchange_session_authority_sha256,migration_readback_sha256,
                    product_authority_schema_sha256,previous_receipt_sha256,created_at_utc,carrier_json,receipt_sha256
                ) VALUES ('runtime_constraints','binding_partial','2026-07-25',1,'LEGACY_DRAIN_ONLY',1,1,0,
                          %s,%s,%s,%s,%s,NULL,now(),'{{}}'::jsonb,%s)
                """,
                (sha, sha, sha, sha, sha, sha),
            )
            cur.execute(f"DROP TABLE {partial_table_schema}.execution_dependent_buy_coordination CASCADE")
            with pytest.raises(psycopg2.errors.RaiseException, match="destructive rollback refused"):
                cur.execute(ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", partial_table_schema))
            cur.execute("ROLLBACK")
            cur.execute(f"SELECT count(*) FROM {partial_table_schema}.execution_product_route_cutover")
            assert int(cur.fetchone()[0]) == 1
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {partial_function_schema} CASCADE")
            cur.execute(f"DROP SCHEMA IF EXISTS {partial_table_schema} CASCADE")
        conn.close()


def test_k6_database_rejects_non_initial_first_coordination_write_on_dev() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6initial_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _apply_k2_and_k6(cur, schema)
            _insert_valid_k2_constraint_graph(cur, schema)
            sha = "a" * 64
            with pytest.raises(psycopg2.errors.RaiseException, match="exact waiting initial state"):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_dependent_buy_coordination(
                        coordination_id,runtime_id,binding_id,trade_date,strategy_id,buy_algo_instance_id,
                        buy_parent_intent_id,required_cash,release_command_payload_sha256,status,
                        decision_sequence,row_version,lease_epoch,created_at_utc,updated_at_utc,carrier_json,
                        coordination_sha256
                    ) VALUES (%s,'runtime_constraints','binding_initial','2026-07-25','strategy_k6',
                              'algo_constraints','intent_constraints','800',%s,'EOD_RESIDUAL',0,1,0,
                              now(),now(),'{{}}'::jsonb,%s)
                    """,
                    (sha, sha, sha),
                )
            cur.execute("ROLLBACK")
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()
