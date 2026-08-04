from __future__ import annotations

from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path

import psycopg2
import pytest

from backend.services.miniqmt_execution_runtime.kernel_repository import (
    KernelRepositorySchemaError,
    PostgresMiniQMTKernelRepository,
)
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
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import _conn_factory


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION_ROOT = REPO_ROOT / "backend" / "migrations"
PREFLIGHT = MIGRATION_ROOT / "miniqmt_execution_kernel_k6_20260801.preflight.sql"
FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k6_20260801.sql"
ROLLBACK = MIGRATION_ROOT / "miniqmt_execution_kernel_k6_20260801.rollback.sql"
K6C_PREFLIGHT = MIGRATION_ROOT / "miniqmt_execution_kernel_k6c_20260802.preflight.sql"
K6C_FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k6c_20260802.sql"
K6C_ROLLBACK = MIGRATION_ROOT / "miniqmt_execution_kernel_k6c_20260802.rollback.sql"
K6B_PREFLIGHT = MIGRATION_ROOT / "miniqmt_execution_kernel_k6b_20260803.preflight.sql"
K6B_FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k6b_20260803.sql"
K6B_ROLLBACK = MIGRATION_ROOT / "miniqmt_execution_kernel_k6b_20260803.rollback.sql"
CATALOG_SHA256 = "546a209dc2f8721ccee8b5e905117788486307147dfb4fc6bc396842f5cf84ad"
K6C0_CATALOG_SHA256 = "f4fc093c83642577009dc5ce8c03550bbb75e00f09ada7bf2489272ddd67bd7d"
K6B_BASE_CATALOG_SHA256 = "6eeff2d2887049a7b3e3c93dd93e56e9af6241e0be1caf2c7ef535cbbde5d9f6"
K6B_K6C_CATALOG_SHA256 = "ef09f8ab2f3e6a1563cd536327ee1d9c04273806c3fdfdea2e704600f330d912"
K6B_CATALOG_SHA256 = "10ae5be030612f923f2fe23f17f1f8b4891358cc8bd9565d54ad27ee3d18393c"


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


def test_k6b_dependency_successor_migration_is_idempotent_and_guarded_on_dev() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6-B DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6b_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _apply_k2_and_k6(cur, schema)
            cur.execute(K6C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(K6B_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            forward = K6B_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            cur.execute(forward)
            cur.execute(forward)
            cur.execute(K6B_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(
                f"SELECT {schema}.miniqmt_k6_catalog_fingerprint(),"
                f"{schema}.miniqmt_k6c_catalog_fingerprint(),"
                f"{schema}.miniqmt_k6b_catalog_fingerprint()"
            )
            assert tuple(cur.fetchone()) == (
                K6B_BASE_CATALOG_SHA256,
                K6B_K6C_CATALOG_SHA256,
                K6B_CATALOG_SHA256,
            )
            repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
            assert repository.preflight_k6b_schema()["k6b_schema_catalog_fingerprint"] is True
            cur.execute(
                "SELECT column_name,is_nullable FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name='execution_dependent_buy_coordination' "
                "AND column_name IN ('virtual_account_id','session_authority_sha256') ORDER BY column_name",
                (schema,),
            )
            assert tuple(cur.fetchall()) == (
                ("session_authority_sha256", "NO"),
                ("virtual_account_id", "NO"),
            )
            cur.execute(
                "SELECT pg_get_triggerdef(t.oid, true) FROM pg_trigger t "
                "JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=%s AND c.relname='execution_dependent_buy_dependency' "
                "AND t.tgname='trg_miniqmt_k6_dependency_successor' AND NOT t.tgisinternal",
                (schema,),
            )
            trigger = cur.fetchone()
            assert trigger is not None and "miniqmt_k6b_validate_dependency_successor" in str(trigger[0])

            cur.execute(
                f"COMMENT ON COLUMN {schema}.execution_dependent_buy_coordination.virtual_account_id "
                "IS 'drifted K6-B authority comment'"
            )
            with pytest.raises(psycopg2.errors.RaiseException, match="successor catalog drift"):
                cur.execute(K6B_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute("ROLLBACK")
            with pytest.raises(KernelRepositorySchemaError, match="catalog fingerprint mismatch"):
                repository.preflight_k6b_schema()
            cur.execute(forward)
            assert repository.preflight_k6b_schema()["k6b_schema_catalog_fingerprint"] is True

            cur.execute(
                f"CREATE OR REPLACE FUNCTION {schema}.miniqmt_k6b_catalog_fingerprint() "
                f"RETURNS TEXT LANGUAGE sql STABLE AS $$ SELECT '{K6B_CATALOG_SHA256}'::TEXT $$"
            )
            with pytest.raises(KernelRepositorySchemaError, match="function definition drift"):
                repository.preflight_k6b_schema()
            cur.execute(forward)
            assert repository.preflight_k6b_schema()["k6b_schema_catalog_fingerprint"] is True
            cur.execute(K6B_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(
                "SELECT pg_get_triggerdef(t.oid, true) FROM pg_trigger t "
                "JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=%s AND c.relname='execution_dependent_buy_dependency' "
                "AND t.tgname='trg_miniqmt_k6_dependency_append_only' AND NOT t.tgisinternal",
                (schema,),
            )
            restored = cur.fetchone()
            assert restored is not None and "miniqmt_k6_reject_immutable_mutation" in str(restored[0])
            cur.execute(
                "SELECT count(*) FROM information_schema.columns WHERE table_schema=%s "
                "AND table_name='execution_dependent_buy_coordination' "
                "AND column_name IN ('virtual_account_id','session_authority_sha256')",
                (schema,),
            )
            assert cur.fetchone()[0] == 0
    finally:
        if not conn.closed:
            conn.rollback()
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def _insert_k6_route_row(cur: object, schema: str, *, binding_id: str) -> None:
    sha = "a" * 64
    cur.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_product_route_cutover(
            runtime_id,binding_id,trade_date,route_epoch,route_owner,effective_new_instance_sequence,
            legacy_active_instance_count,kernel_active_instance_count,catalog_sha256,
            gateway_capability_catalog_sha256,exchange_session_authority_sha256,migration_readback_sha256,
            product_authority_schema_sha256,previous_receipt_sha256,created_at_utc,carrier_json,receipt_sha256
        ) VALUES ('runtime_constraints',%s,'2026-07-25',1,'LEGACY_DRAIN_ONLY',1,1,0,
                  %s,%s,%s,%s,%s,NULL,now(),'{{}}'::jsonb,%s)
        """,
        (binding_id, sha, sha, sha, sha, sha, sha),
    )


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


def test_k6c0_successor_triplet_is_additive_versioned_and_guarded() -> None:
    preflight = K6C_PREFLIGHT.read_text(encoding="utf-8")
    forward = K6C_FORWARD.read_text(encoding="utf-8")
    rollback = K6C_ROLLBACK.read_text(encoding="utf-8")
    assert "SET TRANSACTION READ ONLY" in preflight
    assert _canonical_lf_sha256(K6C_FORWARD) in preflight
    assert "__K6C_" not in preflight + forward + rollback
    assert "miniqmt_execution_kernel_k6_20260801.sql" not in forward
    assert "ALTER TABLE qmt_strategy.execution_product_command_authority_item" in forward
    assert "DEFERRED_DEPENDENT_BUY" in forward and "command_json" in forward
    assert "ck_miniqmt_k2_child_mapping_initial" in forward + rollback
    assert "ck_miniqmt_k6_product_mapping_state" in forward + rollback
    assert "miniqmt_product_command_child_mapping_v1" in forward + rollback
    assert "miniqmt_k6c_catalog_fingerprint" in preflight + forward
    assert K6C0_CATALOG_SHA256 in preflight and K6C0_CATALOG_SHA256 in forward
    assert "zero K6-A durable rows" in forward
    assert "destructive rollback refused" in rollback
    assert "DROP TABLE" not in forward
    assert "DROP TABLE" not in rollback
    assert "INSERT INTO" not in forward


def test_k6c0_preflight_forward_second_apply_readback_and_guarded_rollback_on_dev() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6-C0 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6c0_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _apply_k2_and_k6(cur, schema)
            preflight = K6C_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            forward = K6C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            cur.execute(preflight)
            cur.execute(forward)
            cur.execute(preflight)
            cur.execute(forward)
            cur.execute(f"SELECT {schema}.miniqmt_k6c_catalog_fingerprint()")
            assert str(cur.fetchone()[0]) == K6C0_CATALOG_SHA256
            cur.execute(K6C_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(f"SELECT to_regprocedure('{schema}.miniqmt_k6c_catalog_fingerprint()')")
            assert cur.fetchone()[0] is None
            cur.execute(f"SELECT {schema}.miniqmt_k6_catalog_fingerprint()")
            assert str(cur.fetchone()[0]) == CATALOG_SHA256
    finally:
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_k6c0_forward_and_rollback_refuse_existing_k6_durable_rows_on_dev() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6-C0 DEV PostgreSQL fixture")
    forward_schema = _fixture_schema().replace("k2a_", "k6c0usedfwd_", 1)
    rollback_schema = _fixture_schema().replace("k2a_", "k6c0usedrb_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _apply_k2_and_k6(cur, forward_schema)
            _insert_valid_k2_constraint_graph(cur, forward_schema)
            _insert_k6_route_row(cur, forward_schema, binding_id="binding_forward_refusal")
            with pytest.raises(psycopg2.errors.RaiseException, match="zero K6-A durable rows"):
                cur.execute(K6C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", forward_schema))
            cur.execute("ROLLBACK")
            cur.execute(f"SELECT count(*) FROM {forward_schema}.execution_product_route_cutover")
            assert int(cur.fetchone()[0]) == 1

        with conn.cursor() as cur:
            _apply_k2_and_k6(cur, rollback_schema)
            cur.execute(K6C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", rollback_schema))
            _insert_valid_k2_constraint_graph(cur, rollback_schema)
            _insert_k6_route_row(cur, rollback_schema, binding_id="binding_rollback_refusal")
            with pytest.raises(psycopg2.errors.RaiseException, match="destructive rollback refused"):
                cur.execute(K6C_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", rollback_schema))
            cur.execute("ROLLBACK")
            cur.execute(f"SELECT count(*) FROM {rollback_schema}.execution_product_route_cutover")
            assert int(cur.fetchone()[0]) == 1
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {forward_schema} CASCADE")
            cur.execute(f"DROP SCHEMA IF EXISTS {rollback_schema} CASCADE")
        conn.close()


def test_k6c0_product_mapping_deferred_reserved_state_is_enforced_on_dev() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6-C0 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6c0mapping_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _apply_k2_and_k6(cur, schema)
            cur.execute(K6C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            _insert_valid_k2_constraint_graph(cur, schema)
            cur.execute(
                f"SELECT mapping_created_at_utc FROM {schema}.execution_child_order "
                "WHERE mapping_id='mapping_constraints'"
            )
            mapping_created_at_utc = cur.fetchone()[0]
            product_mapping = {
                "schema_version": "miniqmt_product_command_child_mapping_v1",
                "mapping_id": "mapping_constraints",
                "authority_item_sha256": "b" * 64,
                "coordination_id": "c" * 64,
                "command_id": "command_constraints",
                "runtime_id": "runtime_constraints",
                "algo_instance_id": "algo_constraints",
                "parent_intent_id": "intent_constraints",
                "strategy_slot_id": "slot_constraints",
                "local_vt_orderid": "local_constraints",
                "child_order_id": "child_constraints",
                "deterministic_client_order_ref": "client_constraints",
                "order_remark": "client_constraints",
                "symbol": "600000.SH",
                "side": "BUY",
                "requested_price_decimal": "10",
                "requested_quantity": 100,
                "broker_order_id": None,
                "broker_identity_source_event_id": None,
                "mapping_status": "DEFERRED_DEPENDENT_BUY",
                "mapping_version": 1,
                "payload_sha256": "a" * 64,
                "last_order_event_id": None,
                "last_trade_event_id": None,
                "created_transition_id": "transition_constraints",
                "updated_by_event_id": None,
                "created_at_utc": mapping_created_at_utc.isoformat(),
                "updated_at_utc": mapping_created_at_utc.isoformat(),
                "mapping_receipt_sha256": "a" * 64,
            }
            cur.execute(
                f"""
                UPDATE {schema}.execution_child_order
                SET mapping_status='DEFERRED_DEPENDENT_BUY', mapping_version=1,
                    updated_by_event_id=NULL, mapping_updated_at_utc=mapping_created_at_utc,
                    mapping_json=%s::jsonb
                WHERE mapping_id='mapping_constraints'
                """,
                (json.dumps(product_mapping),),
            )
            corruptions = (
                {"runtime_id": "runtime_forged"},
                {"symbol": "600001.SH"},
                {"requested_quantity": 200},
                {"created_transition_id": "transition_forged"},
                {"strategy_slot_id": None},
            )
            for corruption in corruptions:
                corrupted = {**product_mapping, **corruption}
                with pytest.raises(psycopg2.errors.CheckViolation):
                    cur.execute(
                        f"UPDATE {schema}.execution_child_order SET mapping_json=%s::jsonb "
                        "WHERE mapping_id='mapping_constraints'",
                        (json.dumps(corrupted),),
                    )
                cur.execute("ROLLBACK")
            successor_time = mapping_created_at_utc + timedelta(seconds=1)
            product_mapping.update(
                {
                    "mapping_status": "RESERVED",
                    "mapping_version": 2,
                    "updated_by_event_id": "event_constraints",
                    "updated_at_utc": successor_time.isoformat(),
                }
            )
            cur.execute(
                f"""
                UPDATE {schema}.execution_child_order
                SET mapping_status='RESERVED', mapping_version=2,
                    updated_by_event_id='event_constraints',
                    mapping_updated_at_utc=%s,
                    mapping_json=%s::jsonb
                WHERE mapping_id='mapping_constraints'
                """,
                (successor_time, json.dumps(product_mapping)),
            )
            product_mapping["mapping_version"] = 3
            with pytest.raises(psycopg2.errors.CheckViolation):
                cur.execute(
                    f"""
                    UPDATE {schema}.execution_child_order
                    SET mapping_version=3, mapping_json=%s::jsonb
                    WHERE mapping_id='mapping_constraints'
                    """,
                    (json.dumps(product_mapping),),
                )
            cur.execute("ROLLBACK")
            cur.execute(
                f"SELECT mapping_status,mapping_version FROM {schema}.execution_child_order "
                "WHERE mapping_id='mapping_constraints'"
            )
            assert tuple(cur.fetchone()) == ("RESERVED", 2)
            with pytest.raises(psycopg2.errors.RaiseException, match="product mapping rows"):
                cur.execute(K6C_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute("ROLLBACK")
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()
