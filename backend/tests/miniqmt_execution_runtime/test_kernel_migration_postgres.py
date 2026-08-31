from __future__ import annotations

from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import re
from uuid import uuid4

from dotenv import load_dotenv
import psycopg2
import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION_ROOT = REPO_ROOT / "backend" / "migrations"
PREFLIGHT = MIGRATION_ROOT / "miniqmt_execution_kernel_k2_20260725.preflight.sql"
FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k2_20260725.sql"
ROLLBACK = MIGRATION_ROOT / "miniqmt_execution_kernel_k2_20260725.rollback.sql"
K2C_PREFLIGHT = MIGRATION_ROOT / "miniqmt_execution_kernel_k2c_timer_reclaim_20260727.preflight.sql"
K2C_FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k2c_timer_reclaim_20260727.sql"
K2C_ROLLBACK = MIGRATION_ROOT / "miniqmt_execution_kernel_k2c_timer_reclaim_20260727.rollback.sql"
K2D_PREFLIGHT = MIGRATION_ROOT / "miniqmt_execution_kernel_k2d_reconcile_history_20260727.preflight.sql"
K2D_FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k2d_reconcile_history_20260727.sql"
K2D_ROLLBACK = MIGRATION_ROOT / "miniqmt_execution_kernel_k2d_reconcile_history_20260727.rollback.sql"
K6_FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k6_20260801.sql"
K6C_FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k6c_20260802.sql"
K6B_FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_k6b_20260803.sql"
EVENT_CONTRACT_REPAIR_PREFLIGHT = (
    MIGRATION_ROOT / "miniqmt_execution_kernel_event_contract_repair_20260811.preflight.sql"
)
EVENT_CONTRACT_REPAIR_FORWARD = MIGRATION_ROOT / "miniqmt_execution_kernel_event_contract_repair_20260811.sql"
EVENT_CONTRACT_REPAIR_ROLLBACK = MIGRATION_ROOT / "miniqmt_execution_kernel_event_contract_repair_20260811.rollback.sql"


def test_event_contract_repair_registry_closes_p1d_and_kernel_v2_without_parallel_authority() -> None:
    from backend.services.miniqmt_execution_runtime import quote_event_schema
    from backend.services.miniqmt_execution_runtime.plugin_contracts import EventSourceV2, EventTypeV2

    assert len(quote_event_schema.TARGET_EVENT_TYPES) == 36
    assert len(quote_event_schema.TARGET_EVENT_SOURCES) == 14
    assert {item.value for item in EventTypeV2} <= quote_event_schema.TARGET_EVENT_TYPES
    assert {item.value for item in EventSourceV2} <= quote_event_schema.TARGET_EVENT_SOURCES
    assert len(quote_event_schema.TARGET_KERNEL_EVENT_COMPOSITES) == 12
    assert (
        "ALGO_START",
        "MINIQMT_EXECUTION_KERNEL",
        "miniqmt_algo_start_v1",
    ) in quote_event_schema.TARGET_KERNEL_EVENT_COMPOSITES
    assert (
        "ALGO_START",
        "MINIQMT_EXECUTION_KERNEL",
        "miniqmt_algo_start_v2",
    ) in quote_event_schema.TARGET_KERNEL_EVENT_COMPOSITES
    assert (
        "COMMAND_OUTCOME",
        "MINIQMT_EXECUTION_KERNEL",
        "miniqmt_command_outcome_v1",
    ) in quote_event_schema.TARGET_KERNEL_EVENT_COMPOSITES


def test_event_contract_repair_artifacts_are_atomic_idempotent_and_guarded() -> None:
    from backend.services.miniqmt_execution_runtime.quote_event_schema import (
        EXPECTED_MIGRATION_FILE_SHA256,
        EXPECTED_PREFLIGHT_FILE_SHA256,
        EXPECTED_ROLLBACK_FILE_SHA256,
    )

    preflight = EVENT_CONTRACT_REPAIR_PREFLIGHT.read_text(encoding="utf-8")
    forward = EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8")
    rollback = EVENT_CONTRACT_REPAIR_ROLLBACK.read_text(encoding="utf-8")
    canonical_preflight = preflight.replace("\r\n", "\n").replace("\r", "\n")
    canonical_forward = forward.replace("\r\n", "\n").replace("\r", "\n")
    canonical_rollback = rollback.replace("\r\n", "\n").replace("\r", "\n")
    expected_preflight_sha256 = hashlib.sha256(canonical_preflight.encode("utf-8")).hexdigest()
    expected_forward_sha256 = hashlib.sha256(canonical_forward.encode("utf-8")).hexdigest()
    expected_rollback_sha256 = hashlib.sha256(canonical_rollback.encode("utf-8")).hexdigest()

    assert "REPEATABLE READ, READ ONLY" in preflight
    assert expected_preflight_sha256 == EXPECTED_PREFLIGHT_FILE_SHA256
    assert expected_forward_sha256 == EXPECTED_MIGRATION_FILE_SHA256
    assert expected_rollback_sha256 == EXPECTED_ROLLBACK_FILE_SHA256
    assert f"'{expected_forward_sha256}'::TEXT AS expected_sha256" in preflight
    assert f"'{expected_rollback_sha256}'::TEXT" in preflight
    assert "LOCK TABLE qmt_strategy.execution_runtime_event IN SHARE ROW EXCLUSIVE MODE" in forward
    assert preflight.index("LOCK TABLE") < preflight.index("DO $$")
    for relation in (
        "execution_runtime",
        "execution_runtime_event",
        "execution_algo_instance",
        "execution_child_order",
        "execution_kernel_worker_epoch",
        "execution_kernel_worker_incarnation",
        "execution_algo_event_delivery",
        "execution_algo_transition",
        "execution_algo_command_outbox",
        "execution_algo_command_dispatch_attempt",
        "execution_algo_timer_schedule",
        "execution_algo_timer_occurrence",
        "execution_exchange_session_authority",
        "execution_algo_diagnostic_observation",
        "execution_broker_reconciliation_attempt",
        "execution_dependent_buy_coordination",
        "execution_dependent_buy_dependency",
        "execution_dependent_buy_decision",
        "execution_product_command_authority",
        "execution_product_command_authority_item",
        "execution_product_route_cutover",
        "execution_product_route_owner",
    ):
        assert f"qmt_strategy.{relation}" in rollback[: rollback.index("DO $$")]
    assert "IN SHARE ROW EXCLUSIVE MODE" in rollback[: rollback.index("DO $$")]
    assert "predecessor_constraint_names" in preflight
    assert "target_constraint_names" in preflight
    assert "table_class.relname='execution_runtime_event' AND constraint_record.contype='c'" in forward
    assert "b5cceba58ef9646e441d1fcb346a47cd4648397ac4425a956d1b83b2fc81d473" in forward
    assert forward.count("DROP CONSTRAINT ck_miniqmt_event_type") == 1
    assert forward.count("DROP CONSTRAINT ck_miniqmt_event_source") == 1
    assert forward.count("DROP CONSTRAINT ck_miniqmt_k2_event_composite") == 1
    assert forward.count("DROP CONSTRAINT ck_miniqmt_k2_event_contract") == 1
    assert forward.count("NOT VALID") >= 6
    assert forward.count("VALIDATE CONSTRAINT") >= 6
    assert "COMMAND_OUTCOME" in forward
    assert "miniqmt_algo_start_v2" in forward
    assert ") IS TRUE) NOT VALID" in forward
    assert "IS NOT TRUE" in preflight and "IS NOT TRUE" in forward and "IS NOT TRUE" in rollback
    assert preflight.count("SET LOCAL search_path = pg_catalog, qmt_strategy, pg_temp;") == 1
    assert forward.count("SET LOCAL search_path = pg_catalog, qmt_strategy, pg_temp;") == 2
    assert rollback.count("SET LOCAL search_path = pg_catalog, qmt_strategy, pg_temp;") == 2
    for artifact in (preflight, forward, rollback):
        assert "SET LOCAL search_path = pg_catalog, qmt_strategy;" not in artifact
        assert "AS k2d_independent_catalog_sha256" in artifact
        assert "AS k2d_code_owned_catalog_sha256" in artifact
        assert "AS k2d_catalog_authority_verified" in artifact
    assert "EXECUTE function_body INTO STRICT independent_catalog_sha256" in preflight
    assert "EXECUTE function_body INTO STRICT independent_catalog_sha256" in forward
    assert "EXECUTE function_body INTO STRICT independent_catalog_sha256" in rollback
    for artifact in (preflight, forward, rollback):
        assert "k2d_function_oid" in artifact
        assert "k2d_function_configuration IS NOT NULL" in artifact
        assert "EXECUTE k2d_function_body INTO STRICT k2d_independent_catalog_sha256" in artifact
        assert "independent K2-D catalog drift" in artifact
        assert "2d5fcbf0151d9e5d2a9d8537f834aabfd056a42cc0eeb8c079add68c8964f59f" in artifact
    assert "post-DDL exact constraint readback drift" in forward
    assert "post-commit exact constraint readback drift" in forward
    assert "post-commit exact constraint readback drift" in rollback
    assert "destructive rollback refused" in rollback
    assert "DELETE FROM" not in rollback.upper()
    assert "UPDATE " not in rollback.upper()
    assert "KERNEL_V2" in rollback


def test_k2_migration_public_artifacts_encode_required_stages_and_guards() -> None:
    preflight = PREFLIGHT.read_text(encoding="utf-8")
    forward = FORWARD.read_text(encoding="utf-8")
    rollback = ROLLBACK.read_text(encoding="utf-8")

    assert "SET TRANSACTION READ ONLY" in preflight
    assert "legacy_invalid_row_count" in preflight
    canonical_forward = forward.replace("\r\n", "\n").replace("\r", "\n")
    expected_forward_sha256 = hashlib.sha256(canonical_forward.encode("utf-8")).hexdigest()
    assert f"'{expected_forward_sha256}'::TEXT AS expected_migration_sha256" in preflight
    assert "runner_must_verify_committed_forward_sha256" not in preflight
    assert "event_contract_version" in forward
    assert "kernel_contract_version" in forward
    assert "CREATE UNIQUE INDEX CONCURRENTLY" in forward
    assert "NOT VALID" in forward
    assert "VALIDATE CONSTRAINT" in forward
    assert "COMMENT ON TABLE" in forward
    assert "IF qmt_strategy.miniqmt_k2_catalog_fingerprint()" not in preflight
    assert "SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO actual_catalog_sha256" not in forward
    assert "independently_recomputed_schema_catalog_sha256" in forward
    assert "independently_recomputed_catalog_function_body_sha256" in forward
    assert "K2 post-commit catalog function drift" in forward
    assert preflight.count("FROM pg_index AS index_record") >= 1
    assert forward.count("FROM pg_index AS index_record") >= 3
    assert "ack_receipt_json JSONB" in forward
    assert "non_acceptance_receipt_json JSONB" in forward
    assert "unknown_outcome_receipt_json JSONB" in forward
    assert "reconcile_receipt_json JSONB" in forward
    assert "fk_miniqmt_k2_delivery_event_owner" in forward
    assert "fk_miniqmt_k2_delivery_algo_owner" in forward
    assert "fk_miniqmt_k2_transition_delivery_owner" in forward
    assert "fk_miniqmt_k2_outbox_transition_owner" in forward
    assert "fk_miniqmt_k2_outbox_mapping" in forward
    assert "REFERENCES qmt_strategy.execution_child_order(mapping_id)" in forward
    assert "LEGACY_V1" in forward and "KERNEL_V2" in forward
    assert "REJECTED" not in _algo_status_check(forward)
    assert "FAILED_WITH_ACTIVE_CHILD" not in _algo_status_check(forward)
    assert "kernel_v2_fact_count" in rollback
    assert "destructive rollback refused" in rollback


def test_k2c_timer_reclaim_migration_is_additive_idempotent_and_guarded() -> None:
    preflight = K2C_PREFLIGHT.read_text(encoding="utf-8")
    forward = K2C_FORWARD.read_text(encoding="utf-8")
    rollback = K2C_ROLLBACK.read_text(encoding="utf-8")
    canonical_forward = forward.replace("\r\n", "\n").replace("\r", "\n")
    expected_forward_sha256 = hashlib.sha256(canonical_forward.encode("utf-8")).hexdigest()
    assert "SET TRANSACTION READ ONLY" in preflight
    assert "legacy_invalid_row_count" in preflight
    assert f"'{expected_forward_sha256}'::TEXT AS expected_migration_sha256" in preflight
    assert "row_version = lease_epoch" in forward
    assert "VALIDATE CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial" in forward
    assert "K2-C post-commit readback drift" in forward
    assert "reclaimed_claim_count" in rollback
    assert "destructive rollback refused" in rollback


def test_k2d_reconcile_history_migration_is_additive_idempotent_and_guarded() -> None:
    preflight = K2D_PREFLIGHT.read_text(encoding="utf-8")
    forward = K2D_FORWARD.read_text(encoding="utf-8")
    rollback = K2D_ROLLBACK.read_text(encoding="utf-8")
    canonical_forward = forward.replace("\r\n", "\n").replace("\r", "\n")
    expected_forward_sha256 = hashlib.sha256(canonical_forward.encode("utf-8")).hexdigest()
    assert "SET TRANSACTION READ ONLY" in preflight
    assert f"'{expected_forward_sha256}'::TEXT AS expected_migration_sha256" in preflight
    assert "execution_broker_reconciliation_attempt" in forward
    assert "uq_miniqmt_k2d_reconcile_command_attempt" in forward
    assert "reconcile_attempt BETWEEN 1 AND 10" in forward
    assert "fk_miniqmt_k2d_reconcile_command_runtime" in forward
    assert "FOREIGN KEY (command_id,runtime_id)" in forward
    assert "callback_watermark_before_call" in forward
    assert "miniqmt_k2d_catalog_fingerprint()" in forward
    assert "format_type(attribute.atttypid,attribute.atttypmod)" in forward
    assert "pg_get_indexdef" in forward
    assert "index_class.relname='uq_miniqmt_k2d_outbox_command_runtime'" in forward
    assert forward.count("COMMENT ON COLUMN qmt_strategy.execution_broker_reconciliation_attempt.") == 8
    assert "post-commit readback drift" in forward
    assert "durable_fact_count" in rollback
    assert "destructive rollback refused" in rollback


def test_k2c_timer_reclaim_migration_preflight_forward_second_apply_and_rollback_on_dev() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K2 DEV PostgreSQL fixture")
    schema = _fixture_schema()
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            base_forward = FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            _apply_base_forward(cur, base_forward)
            cur.execute(K2C_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            k2c_forward = K2C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            cur.execute(k2c_forward)
            cur.execute(k2c_forward)
            cur.execute("BEGIN")
            cur.execute(
                "SELECT pg_get_constraintdef(oid,true) FROM pg_constraint "
                "WHERE conrelid=%s::regclass AND conname='ck_miniqmt_k2_timer_occurrence_initial'",
                (f"{schema}.execution_algo_timer_occurrence",),
            )
            assert "row_version = lease_epoch" in str(cur.fetchone()[0])
            cur.execute(K2C_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(
                "SELECT pg_get_constraintdef(oid,true) FROM pg_constraint "
                "WHERE conrelid=%s::regclass AND conname='ck_miniqmt_k2_timer_occurrence_initial'",
                (f"{schema}.execution_algo_timer_occurrence",),
            )
            definition = str(cur.fetchone()[0])
            assert "lease_epoch = 1" in definition and "row_version = 1" in definition
    finally:
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_k2d_reconcile_history_preflight_forward_second_apply_and_rollback_on_dev() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K2 DEV PostgreSQL fixture")
    schema = _fixture_schema()
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _apply_base_forward(cur, FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(K2C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(K2D_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            k2d_forward = K2D_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            cur.execute(k2d_forward)
            cur.execute(k2d_forward)
            cur.execute(
                "SELECT count(*) FROM information_schema.columns WHERE table_schema=%s "
                "AND table_name='execution_broker_reconciliation_attempt'",
                (schema,),
            )
            assert int(cur.fetchone()[0]) == 8
            cur.execute(K2D_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute("SELECT to_regclass(%s)", (f"{schema}.execution_broker_reconciliation_attempt",))
            assert cur.fetchone()[0] is None
    finally:
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def _algo_status_check(sql: str) -> str:
    marker = "ck_miniqmt_k2_algo_status"
    start = sql.index(marker)
    return sql[start : start + 700]


def _runtime_repo_root() -> Path:
    configured = os.getenv("AISTOCK_RUNTIME_REPO_ROOT")
    if configured:
        return Path(configured).resolve()
    git_entry = REPO_ROOT / ".git"
    if git_entry.is_file():
        marker, _, value = git_entry.read_text(encoding="utf-8").strip().partition(":")
        if marker == "gitdir" and value.strip():
            git_dir = Path(value.strip())
            if not git_dir.is_absolute():
                git_dir = (REPO_ROOT / git_dir).resolve()
            if len(git_dir.parents) >= 3:
                common_root = git_dir.parents[2]
                if (common_root / ".git").exists():
                    return common_root
    return REPO_ROOT


def _dev_dsn() -> dict[str, object]:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K2 DEV PostgreSQL fixture")
    load_dotenv(_runtime_repo_root() / ".env", override=False)
    dsn: dict[str, object] = {
        "host": os.getenv("TDX_DB_DEV_HOST"),
        "port": int(os.getenv("TDX_DB_DEV_PORT", "0")),
        "dbname": os.getenv("TDX_DB_DEV_NAME"),
        "user": os.getenv("TDX_DB_DEV_USER"),
        "password": os.getenv("TDX_DB_DEV_PASSWORD"),
        "connect_timeout": 5,
    }
    if dsn["host"] != "127.0.0.1" or dsn["port"] != 5433 or "dev" not in str(dsn["dbname"]).lower():
        raise AssertionError(f"refusing non-DEV K2 migration target {dsn['host']}:{dsn['port']}/{dsn['dbname']}")
    if not dsn["user"] or not dsn["password"]:
        pytest.fail("guarded DEV credentials are unavailable")
    return dsn


def _fixture_schema() -> str:
    schema = "k2a_" + uuid4().hex
    assert re.fullmatch(r"k2a_[0-9a-f]{32}", schema)
    return schema


def _base_fixture_sql(schema: str) -> str:
    return f"""
    CREATE SCHEMA {schema};
    CREATE TABLE {schema}.execution_runtime (
        runtime_id TEXT PRIMARY KEY, trade_date DATE NOT NULL,
        last_event_sequence INTEGER NOT NULL DEFAULT 0,
        archived_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE {schema}.execution_runtime_event (
        event_id TEXT PRIMARY KEY, runtime_id TEXT NOT NULL REFERENCES {schema}.execution_runtime(runtime_id),
        sequence INTEGER NOT NULL, event_type TEXT NOT NULL, event_time TIMESTAMPTZ NOT NULL,
        source TEXT NOT NULL, payload JSONB NOT NULL DEFAULT '{{}}'::jsonb, archived_at TIMESTAMPTZ,
        archive_reason TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_miniqmt_event_runtime_sequence UNIQUE(runtime_id, sequence)
    );
    CREATE TABLE {schema}.execution_algo_instance (
        algo_instance_id TEXT PRIMARY KEY, runtime_id TEXT NOT NULL REFERENCES {schema}.execution_runtime(runtime_id),
        parent_intent_id TEXT NOT NULL, strategy_slot_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
        target_quantity INTEGER NOT NULL, remaining_quantity INTEGER NOT NULL, algo_code TEXT NOT NULL,
        status TEXT NOT NULL, metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb, archived_at TIMESTAMPTZ,
        archive_reason TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT ck_miniqmt_algo_status CHECK (status IN ('ACTIVE','PAUSED','COMPLETED','CANCELLED','FAILED'))
    );
    CREATE TABLE {schema}.execution_child_order (
        child_order_id TEXT PRIMARY KEY, runtime_id TEXT NOT NULL REFERENCES {schema}.execution_runtime(runtime_id),
        algo_instance_id TEXT NOT NULL REFERENCES {schema}.execution_algo_instance(algo_instance_id),
        parent_intent_id TEXT NOT NULL, strategy_slot_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
        quantity INTEGER NOT NULL, price NUMERIC(20,6) NOT NULL, price_type INTEGER NOT NULL, status TEXT NOT NULL,
        broker_order_id TEXT, submitted_at TIMESTAMPTZ, metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        archived_at TIMESTAMPTZ, archive_reason TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """


def _insert_valid_k2_constraint_graph(cur: object, schema: str) -> None:
    sha = "a" * 64
    cur.execute(  # type: ignore[attr-defined]
        f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES ('runtime_constraints','2026-07-25')"
    )
    cur.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_algo_instance(
            algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,
            remaining_quantity,algo_code,status,kernel_contract_version,traded_quantity,plugin_id,
            plugin_version,plugin_manifest_sha256,plugin_config_json,plugin_config_sha256,
            compatibility_receipt_sha256,state_schema_version,state_json,state_sha256,
            transition_sequence,last_applied_delivery_sequence,last_closed_delivery_sequence,
            active_child_closure_status,active_child_count,row_version,kernel_carrier_json
        ) VALUES (
            'algo_constraints','runtime_constraints','intent_constraints','slot_constraints','600000.SH',
            'BUY',100,100,'TWAP','ACTIVE','KERNEL_V2',0,'aistock.twap','1.0.0',%s,'{{}}'::jsonb,
            %s,%s,'twap_state_v1','{{}}'::jsonb,%s,0,0,0,'NOT_APPLICABLE',0,1,'{{}}'::jsonb
        )
        """,
        (sha, sha, sha, sha),
    )
    cur.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_runtime_event(
            event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
            event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
            logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
            ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
        ) VALUES (
            'event_constraints','runtime_constraints',1,'TICK',now(),'B0_QUOTE_V2','{{}}'::jsonb,'KERNEL_V2',
            'miniqmt_runtime_event_envelope_v2','miniqmt_market_data_view_v2',%s,%s,now(),now(),
            '{{}}'::jsonb,'{{}}'::jsonb,'{{}}'::jsonb,%s,'miniqmt_event_routing_v1','tx_constraints'
        )
        """,
        (sha, sha, sha),
    )
    cur.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_algo_event_delivery(
            delivery_id,event_id,runtime_id,algo_instance_id,plugin_manifest_sha256,
            algo_delivery_sequence,status,attempt_count,transition_id,row_version,
            created_at_utc,updated_at_utc,closed_at_utc,carrier_json
        ) VALUES (
            'delivery_constraints','event_constraints','runtime_constraints','algo_constraints',%s,
            1,'APPLIED',1,'transition_constraints',1,now(),now(),now(),'{{}}'::jsonb
        )
        """,
        (sha,),
    )
    cur.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_algo_transition(
            transition_id,delivery_id,event_id,runtime_id,algo_instance_id,transition_sequence,
            transition_kind,transition_receipt_json,receipt_sha256,execution_projection_set_json,
            execution_projection_set_sha256,after_state_json,after_state_sha256,transaction_commit_identity
        ) VALUES (
            'transition_constraints','delivery_constraints','event_constraints','runtime_constraints',
            'algo_constraints',1,'APPLIED','{{}}'::jsonb,%s,'{{}}'::jsonb,%s,'{{}}'::jsonb,%s,'tx_constraints'
        )
        """,
        (sha, sha, sha),
    )
    cur.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_child_order(
            child_order_id,runtime_id,algo_instance_id,parent_intent_id,strategy_slot_id,symbol,side,
            quantity,price,price_type,status,metadata,kernel_contract_version,mapping_id,command_id,
            local_vt_orderid,deterministic_client_order_ref,order_remark,mapping_status,mapping_version,
            mapping_payload_sha256,mapping_receipt_sha256,created_transition_id,mapping_created_at_utc,
            mapping_updated_at_utc,mapping_json
        ) VALUES (
            'child_constraints','runtime_constraints','algo_constraints','intent_constraints','slot_constraints',
            '600000.SH','BUY',100,10.000000,2,'SUBMITTING','{{}}'::jsonb,'KERNEL_V2',
            'mapping_constraints','command_constraints','local_constraints','client_constraints',
            'client_constraints','RESERVED',1,%s,%s,'transition_constraints',now(),now(),'{{}}'::jsonb
        )
        """,
        (sha, sha),
    )


def _catalog_snapshot(cur: object, schema: str) -> tuple[tuple[object, ...], ...]:
    cur.execute(  # type: ignore[attr-defined]
        """
        SELECT c.relname, c.relkind, a.attname, format_type(a.atttypid, a.atttypmod),
               pg_get_expr(ad.adbin, ad.adrelid), con.conname, pg_get_constraintdef(con.oid),
               idx.indexrelid::regclass::text
        FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
        LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum
        LEFT JOIN pg_constraint con ON con.conrelid=c.oid
        LEFT JOIN pg_index idx ON idx.indrelid=c.oid
        WHERE n.nspname=%s AND c.relkind IN ('r','p')
        ORDER BY 1,2,3,6,8
        """,
        (schema,),
    )
    return tuple(tuple(row) for row in cur.fetchall())  # type: ignore[attr-defined]


def _apply_base_forward(cur: object, forward: str) -> None:
    stage1, remainder = forward.split(
        "-- Stage 2: PostgreSQL requires CONCURRENTLY outside a transaction block.", maxsplit=1
    )
    stage2, stage3 = remainder.split(
        "-- Stage 3: named checks/FKs, validation, comments, and independent readback.", maxsplit=1
    )
    cur.execute(stage1)  # type: ignore[attr-defined]
    for statement in stage2.split(";"):
        if "CREATE UNIQUE INDEX CONCURRENTLY" in statement:
            cur.execute(statement)  # type: ignore[attr-defined]
    cur.execute(stage3)  # type: ignore[attr-defined]


def _install_event_contract_predecessor(cur: object, schema: str) -> None:
    cur.execute(  # type: ignore[attr-defined]
        f"""
        ALTER TABLE {schema}.execution_runtime_event
          ADD CONSTRAINT ck_miniqmt_event_id CHECK (btrim(event_id) <> ''),
          ADD CONSTRAINT ck_miniqmt_event_sequence CHECK (sequence > 0),
          ADD CONSTRAINT ck_miniqmt_event_type CHECK (event_type IN (
            'RUNTIME_CREATED','GATEWAY_CONNECTED','GATEWAY_DISCONNECTED','BROKER_SYNC_STARTED','BROKER_SYNCED',
            'ALGO_INSTANCE_CREATED','TIMER','TICK','ALGO_ACTION_EMITTED','CHILD_ORDER_SUBMITTED',
            'CHILD_ORDER_REJECTED','CHILD_ORDER_CANCEL_REQUESTED','ORDER_EVENT','TRADE_EVENT','ACCOUNT_EVENT',
            'RISK_KILL_SWITCH_TRIGGERED','RECONCILE_STARTED','RECONCILE_COMPLETED','OPERATOR_COMMAND_RECEIVED',
            'OPERATOR_COMMAND_EXECUTED','OPERATOR_COMMAND_REJECTED','RUNTIME_STOPPED','QUOTE_OBSERVED',
            'QUOTE_REJECTED','QUOTE_ELIGIBILITY_EVALUATED','QUOTE_MARK_CAPTURED','QUOTE_INGRESS_HEALTH'
          )),
          ADD CONSTRAINT ck_miniqmt_event_source CHECK (source IN (
            'runtime','gateway','oms','algo','operator','recovery','quote_ingress'
          ))
        """
    )


def _apply_current_k6_predecessor(cur: object, schema: str) -> None:
    _apply_base_forward(cur, FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
    cur.execute(K2C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]
    cur.execute(K2D_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]
    k6_forward = K6_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    cur.execute(k6_forward)  # type: ignore[attr-defined]
    cur.execute(K6C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]
    cur.execute(K6B_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))  # type: ignore[attr-defined]


def test_event_contract_repair_preflight_apply_matrix_rollback_and_drift_on_dev_postgres() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1019_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    sha = "a" * 64
    target_composites = (
        ("ALGO_START", "MINIQMT_EXECUTION_KERNEL", "miniqmt_algo_start_v1"),
        ("ALGO_START", "MINIQMT_EXECUTION_KERNEL", "miniqmt_algo_start_v2"),
        ("COMMAND_OUTCOME", "MINIQMT_EXECUTION_KERNEL", "miniqmt_command_outcome_v1"),
        ("TICK", "B0_QUOTE_V2", "miniqmt_market_data_view_v2"),
        ("TIMER", "EXCHANGE_SESSION_CLOCK", "miniqmt_timer_due_v1"),
        ("SESSION", "EXCHANGE_SESSION_CLOCK", "miniqmt_session_event_v1"),
        ("EOD", "EXCHANGE_SESSION_CLOCK", "miniqmt_eod_event_v1"),
        ("ORDER", "QMT_GATEWAY_CALLBACK", "miniqmt_order_event_v1"),
        ("TRADE", "QMT_GATEWAY_CALLBACK", "miniqmt_trade_fact_v1"),
        ("ACCOUNT", "QMT_OMS_PROJECTION", "miniqmt_account_projection_v1"),
        ("RECONCILE", "QMT_OMS_RECONCILIATION", "miniqmt_reconciliation_receipt_v1"),
        ("OPERATOR", "SIMULATION_RUNTIME_OPERATOR", "miniqmt_operator_command_v1"),
    )
    preflight = EVENT_CONTRACT_REPAIR_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    forward = EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    rollback = EVENT_CONTRACT_REPAIR_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            cur.execute(f"SELECT {schema}.miniqmt_k2_catalog_fingerprint()")
            assert cur.fetchone()[0] == "673ac852d725941112752d2eb63c46342e1b53169fadfacd4664fcbb4c27634e"
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES ('runtime_bug1019','2026-08-11')"
            )
            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_event_(type|source)"):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_runtime_event(
                        event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
                        event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
                        logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
                        ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                    ) VALUES (
                        'event_predecessor_repro','runtime_bug1019',1,'SESSION',now(),'EXCHANGE_SESSION_CLOCK',
                        '{{}}'::jsonb,'KERNEL_V2','miniqmt_runtime_event_envelope_v2','miniqmt_session_event_v1',
                        %s,%s,now(),now(),'{{}}'::jsonb,'{{}}'::jsonb,'{{}}'::jsonb,%s,
                        'miniqmt_event_routing_v1','tx_predecessor_repro'
                    )
                    """,
                    (sha, sha, sha),
                )

            cur.execute(preflight)
            cur.execute(forward)
            cur.execute(
                "SELECT oid::text,xmin::text,prosrc,proconfig FROM pg_proc WHERE oid=%s::regprocedure",
                (f"{schema}.miniqmt_k2_catalog_fingerprint()",),
            )
            target_function_authority = tuple(cur.fetchone())
            cur.execute(
                "SELECT oid::text,xmin::text,prosrc,proconfig FROM pg_proc WHERE oid=%s::regprocedure",
                (f"{schema}.miniqmt_k2d_catalog_fingerprint()",),
            )
            target_k2d_function_authority = tuple(cur.fetchone())
            cur.execute(
                "SELECT conname,oid::text FROM pg_constraint "
                "WHERE conrelid=%s::regclass AND contype='c' "
                'ORDER BY conname COLLATE "C"',
                (f"{schema}.execution_runtime_event",),
            )
            target_constraint_authority = tuple(tuple(row) for row in cur.fetchall())
            cur.execute(forward)
            cur.execute(
                "SELECT oid::text,xmin::text,prosrc,proconfig FROM pg_proc WHERE oid=%s::regprocedure",
                (f"{schema}.miniqmt_k2_catalog_fingerprint()",),
            )
            assert tuple(cur.fetchone()) == target_function_authority
            cur.execute(
                "SELECT oid::text,xmin::text,prosrc,proconfig FROM pg_proc WHERE oid=%s::regprocedure",
                (f"{schema}.miniqmt_k2d_catalog_fingerprint()",),
            )
            assert tuple(cur.fetchone()) == target_k2d_function_authority
            cur.execute(
                "SELECT conname,oid::text FROM pg_constraint "
                "WHERE conrelid=%s::regclass AND contype='c' "
                'ORDER BY conname COLLATE "C"',
                (f"{schema}.execution_runtime_event",),
            )
            assert tuple(tuple(row) for row in cur.fetchall()) == target_constraint_authority
            cur.execute(preflight)
            cur.execute(f"SELECT {schema}.miniqmt_k2_catalog_fingerprint()")
            assert cur.fetchone()[0] == "b5cceba58ef9646e441d1fcb346a47cd4648397ac4425a956d1b83b2fc81d473"
            cur.execute(
                "SELECT conname FROM pg_constraint WHERE conrelid=%s::regclass AND contype='c' "
                'ORDER BY conname COLLATE "C"',
                (f"{schema}.execution_runtime_event",),
            )
            assert tuple(row[0] for row in cur.fetchall()) == (
                "ck_miniqmt_event_id",
                "ck_miniqmt_event_sequence",
                "ck_miniqmt_event_source",
                "ck_miniqmt_event_type",
                "ck_miniqmt_k2_event_composite",
                "ck_miniqmt_k2_event_contract",
            )
            from backend.services.miniqmt_execution_runtime.kernel_repository import (
                KernelRepositorySchemaError,
                PostgresMiniQMTKernelRepository,
            )
            from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import (
                _conn_factory,
            )

            base_conn_factory = _conn_factory(schema)
            connection_contract: list[tuple[bool, bool]] = []

            @contextmanager
            def counted_conn_factory(*, autocommit: bool = False, manage_transaction: bool = False):
                connection_contract.append((autocommit, manage_transaction))
                with base_conn_factory(autocommit=autocommit, manage_transaction=manage_transaction) as proxy:
                    yield proxy

            repository_readback = PostgresMiniQMTKernelRepository(conn_factory=counted_conn_factory).preflight_schema()
            assert repository_readback["event_contract_schema"] is True
            assert repository_readback["schema_catalog_fingerprint"] is True
            assert connection_contract == [(False, True)]

            nullable_envelope_fields = (
                "event_schema_version",
                "event_key_sha256",
                "payload_sha256",
                "ingress_receipt_sha256",
                "routing_rule_version",
            )
            for offset, nullable_field in enumerate(nullable_envelope_fields, start=70):
                values = {
                    "event_schema_version": "'miniqmt_runtime_event_envelope_v2'",
                    "event_key_sha256": f"'{offset:064x}'",
                    "payload_sha256": f"'{offset + 100:064x}'",
                    "ingress_receipt_sha256": f"'{offset + 200:064x}'",
                    "routing_rule_version": "'miniqmt_event_routing_v1'",
                }
                values[nullable_field] = "NULL"
                cur.execute("BEGIN")
                with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_k2_event_contract"):
                    cur.execute(
                        f"""
                        INSERT INTO {schema}.execution_runtime_event(
                            event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
                            event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
                            logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
                            ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                        ) VALUES (
                            'event_null_{nullable_field}','runtime_bug1019',{offset},'SESSION',now(),
                            'EXCHANGE_SESSION_CLOCK','{{}}'::jsonb,'KERNEL_V2',
                            {values["event_schema_version"]},'miniqmt_session_event_v1',
                            {values["event_key_sha256"]},{values["payload_sha256"]},now(),now(),
                            '{{}}'::jsonb,'{{}}'::jsonb,'{{}}'::jsonb,
                            {values["ingress_receipt_sha256"]},{values["routing_rule_version"]},
                            'tx_null_{nullable_field}'
                        )
                        """
                    )
                cur.execute("ROLLBACK")

            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_k2_event_composite"):
                cur.execute(
                    f"INSERT INTO {schema}.execution_runtime_event("
                    "event_id,runtime_id,sequence,event_type,event_time,source,payload) VALUES "
                    "('event_bad_legacy_k2_identity','runtime_bug1019',90,'SESSION',now(),"
                    "'EXCHANGE_SESSION_CLOCK','{}'::jsonb)"
                )

            cur.execute("BEGIN")
            cur.execute(f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_miniqmt_k2_event_contract")
            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_k2_event_composite"):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_runtime_event(
                        event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
                        event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
                        logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
                        ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                    ) VALUES (
                        'event_null_kernel_schema','runtime_bug1019',91,'SESSION',now(),'EXCHANGE_SESSION_CLOCK',
                        '{{}}'::jsonb,'KERNEL_V2','miniqmt_runtime_event_envelope_v2',NULL,
                        %s,%s,now(),now(),'{{}}'::jsonb,'{{}}'::jsonb,'{{}}'::jsonb,%s,
                        'miniqmt_event_routing_v1','tx_null_kernel_schema'
                    )
                    """,
                    (sha, sha, sha),
                )
            cur.execute("ROLLBACK")

            for sequence, (event_type, source, payload_schema) in enumerate(target_composites, start=1):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_runtime_event(
                        event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
                        event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
                        logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
                        ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                    ) VALUES (
                        %s,'runtime_bug1019',%s,%s,now(),%s,'{{}}'::jsonb,'KERNEL_V2',
                        'miniqmt_runtime_event_envelope_v2',%s,%s,%s,now(),now(),'{{}}'::jsonb,
                        '{{}}'::jsonb,'{{}}'::jsonb,%s,'miniqmt_event_routing_v1',%s
                    )
                    """,
                    (
                        f"event_target_{sequence}",
                        sequence,
                        event_type,
                        source,
                        payload_schema,
                        f"{sequence:064x}",
                        f"{sequence + 20:064x}",
                        f"{sequence + 40:064x}",
                        f"tx_target_{sequence}",
                    ),
                )
            for sequence, (event_type, _source, payload_schema) in enumerate(target_composites, start=101):
                with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_k2_event_composite"):
                    cur.execute(
                        f"""
                        INSERT INTO {schema}.execution_runtime_event(
                            event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,
                            event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,
                            logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,
                            ingress_receipt_sha256,routing_rule_version,transaction_commit_identity
                        ) VALUES (
                            %s,'runtime_bug1019',%s,%s,now(),'runtime','{{}}'::jsonb,'KERNEL_V2',
                            'miniqmt_runtime_event_envelope_v2',%s,%s,%s,now(),now(),'{{}}'::jsonb,
                            '{{}}'::jsonb,'{{}}'::jsonb,%s,'miniqmt_event_routing_v1',%s
                        )
                        """,
                        (
                            f"event_bad_{sequence}",
                            sequence,
                            event_type,
                            payload_schema,
                            f"{sequence:064x}",
                            f"{sequence + 20:064x}",
                            f"{sequence + 40:064x}",
                            f"tx_bad_{sequence}",
                        ),
                    )

            nullable_identity_cases = (
                ("sequence", "NULL", "'QUOTE_OBSERVED'", "'quote_ingress'", "ck_miniqmt_event_sequence"),
                ("event_type", "1201", "NULL", "'quote_ingress'", "ck_miniqmt_event_type"),
                ("source", "1202", "'QUOTE_OBSERVED'", "NULL", "ck_miniqmt_event_source"),
            )
            for nullable_column, sequence_sql, event_type_sql, source_sql, constraint_name in nullable_identity_cases:
                cur.execute("BEGIN")
                cur.execute(
                    f"ALTER TABLE {schema}.execution_runtime_event ALTER COLUMN {nullable_column} DROP NOT NULL"
                )
                with pytest.raises(psycopg2.errors.CheckViolation, match=constraint_name):
                    cur.execute(
                        f"""
                        INSERT INTO {schema}.execution_runtime_event(
                            event_id,runtime_id,sequence,event_type,event_time,source,payload
                        ) VALUES (
                            'event_null_{nullable_column}','runtime_bug1019',{sequence_sql},{event_type_sql},
                            now(),{source_sql},'{{}}'::jsonb
                        )
                        """
                    )
                cur.execute("ROLLBACK")

            cur.execute("BEGIN")
            cur.execute(
                f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT execution_runtime_event_pkey CASCADE"
            )
            cur.execute(f"ALTER TABLE {schema}.execution_runtime_event ALTER COLUMN event_id DROP NOT NULL")
            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_event_id"):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_runtime_event(
                        event_id,runtime_id,sequence,event_type,event_time,source,payload
                    ) VALUES (
                        NULL,'runtime_bug1019',1203,'QUOTE_OBSERVED',now(),'quote_ingress','{{}}'::jsonb
                    )
                    """
                )
            cur.execute("ROLLBACK")

            cur.execute(
                f"INSERT INTO {schema}.execution_runtime_event("
                "event_id,runtime_id,sequence,event_type,event_time,source,payload) VALUES "
                "('event_legacy_quote','runtime_bug1019',1000,'QUOTE_OBSERVED',now(),'quote_ingress','{}'::jsonb)"
            )
            with pytest.raises(psycopg2.Error, match="destructive rollback refused"):
                cur.execute(rollback)
            cur.execute("ROLLBACK")
            cur.execute(f"DELETE FROM {schema}.execution_runtime_event WHERE event_contract_version='KERNEL_V2'")

            cur.execute("BEGIN")
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_algo_instance(
                    algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,
                    remaining_quantity,algo_code,status,kernel_contract_version,traded_quantity,plugin_id,
                    plugin_version,plugin_manifest_sha256,plugin_config_json,plugin_config_sha256,
                    compatibility_receipt_sha256,state_schema_version,state_json,state_sha256,
                    transition_sequence,last_applied_delivery_sequence,last_closed_delivery_sequence,
                    active_child_closure_status,active_child_count,row_version,kernel_carrier_json
                ) VALUES (
                    'algo_rollback_guard','runtime_bug1019','intent_guard','slot_guard','600000.SH','BUY',100,100,
                    'TWAP','ACTIVE','KERNEL_V2',0,'aistock.twap','1.0.0',%s,'{{}}'::jsonb,%s,%s,
                    'twap_state_v1','{{}}'::jsonb,%s,0,0,0,'NOT_APPLICABLE',0,1,'{{}}'::jsonb
                )
                """,
                (sha, sha, sha, sha),
            )
            with pytest.raises(psycopg2.Error, match="destructive rollback refused"):
                cur.execute(rollback)
            cur.execute("ROLLBACK")

            cur.execute("BEGIN")
            cur.execute(f"ALTER TABLE {schema}.execution_broker_reconciliation_attempt DISABLE TRIGGER ALL")
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_broker_reconciliation_attempt(
                    receipt_sha256,command_id,runtime_id,reconcile_attempt,callback_watermark,
                    outcome,observed_at_utc,receipt_json
                ) VALUES (%s,'orphan_k2d_command','runtime_bug1019',1,'watermark_k2d',
                          'NOT_FOUND',now(),'{{}}'::jsonb)
                """,
                ("c" * 64,),
            )
            cur.execute(f"ALTER TABLE {schema}.execution_broker_reconciliation_attempt ENABLE TRIGGER ALL")
            with pytest.raises(psycopg2.Error, match="destructive rollback refused"):
                cur.execute(rollback)
            cur.execute("ROLLBACK")

            cur.execute("BEGIN")
            cur.execute(
                f"INSERT INTO {schema}.execution_kernel_worker_epoch(worker_id,process_role) "
                "VALUES ('worker_rollback_guard','DISPATCH')"
            )
            with pytest.raises(psycopg2.Error, match="destructive rollback refused"):
                cur.execute(rollback)
            cur.execute("ROLLBACK")

            cur.execute("BEGIN")
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_product_route_cutover(
                    runtime_id,binding_id,trade_date,route_epoch,route_owner,effective_new_instance_sequence,
                    legacy_active_instance_count,kernel_active_instance_count,catalog_sha256,
                    gateway_capability_catalog_sha256,exchange_session_authority_sha256,
                    migration_readback_sha256,product_authority_schema_sha256,created_at_utc,carrier_json,
                    receipt_sha256
                ) VALUES (
                    'runtime_bug1019','binding_rollback_guard','2026-08-11',1,'KERNEL_V2',1,0,0,
                    %s,%s,%s,%s,%s,now(),'{{}}'::jsonb,%s
                )
                """,
                (sha, sha, sha, sha, sha, "b" * 64),
            )
            with pytest.raises(psycopg2.Error, match="destructive rollback refused"):
                cur.execute(rollback)
            cur.execute("ROLLBACK")

            cur.execute(rollback)
            cur.execute(
                "SELECT oid::text,xmin::text,prosrc,proconfig FROM pg_proc WHERE oid=%s::regprocedure",
                (f"{schema}.miniqmt_k2_catalog_fingerprint()",),
            )
            predecessor_function_authority = tuple(cur.fetchone())
            cur.execute(
                "SELECT oid::text,xmin::text,prosrc,proconfig FROM pg_proc WHERE oid=%s::regprocedure",
                (f"{schema}.miniqmt_k2d_catalog_fingerprint()",),
            )
            predecessor_k2d_function_authority = tuple(cur.fetchone())
            assert predecessor_k2d_function_authority == target_k2d_function_authority
            cur.execute(
                "SELECT conname,oid::text FROM pg_constraint "
                "WHERE conrelid=%s::regclass AND contype='c' "
                'ORDER BY conname COLLATE "C"',
                (f"{schema}.execution_runtime_event",),
            )
            predecessor_constraint_authority = tuple(tuple(row) for row in cur.fetchall())
            cur.execute(rollback)
            cur.execute(
                "SELECT oid::text,xmin::text,prosrc,proconfig FROM pg_proc WHERE oid=%s::regprocedure",
                (f"{schema}.miniqmt_k2_catalog_fingerprint()",),
            )
            assert tuple(cur.fetchone()) == predecessor_function_authority
            cur.execute(
                "SELECT oid::text,xmin::text,prosrc,proconfig FROM pg_proc WHERE oid=%s::regprocedure",
                (f"{schema}.miniqmt_k2d_catalog_fingerprint()",),
            )
            assert tuple(cur.fetchone()) == predecessor_k2d_function_authority
            cur.execute(
                "SELECT conname,oid::text FROM pg_constraint "
                "WHERE conrelid=%s::regclass AND contype='c' "
                'ORDER BY conname COLLATE "C"',
                (f"{schema}.execution_runtime_event",),
            )
            assert tuple(tuple(row) for row in cur.fetchall()) == predecessor_constraint_authority
            with pytest.raises(KernelRepositorySchemaError, match="not the exact successor"):
                PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema)).preflight_schema()
            cur.execute(forward)
            cur.execute(f"SELECT pg_get_functiondef('{schema}.miniqmt_k2_catalog_fingerprint()'::regprocedure)")
            genuine_catalog_function = str(cur.fetchone()[0])
            for artifact in (preflight, forward, rollback):
                cur.execute(
                    f"""
                    CREATE OR REPLACE FUNCTION {schema}.miniqmt_k2_catalog_fingerprint()
                    RETURNS TEXT LANGUAGE SQL STABLE
                    AS $forged$ SELECT 'b5cceba58ef9646e441d1fcb346a47cd4648397ac4425a956d1b83b2fc81d473'::TEXT $forged$
                    """
                )
                with pytest.raises(psycopg2.Error, match="catalog function definition drift"):
                    cur.execute(artifact)
                cur.execute("ROLLBACK")
                cur.execute(genuine_catalog_function)
            cur.execute(
                f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_source; "
                f"ALTER TABLE {schema}.execution_runtime_event ADD CONSTRAINT ck_miniqmt_event_source "
                "CHECK (source IN ('runtime')) NOT VALID"
            )
            with pytest.raises(psycopg2.Error, match="exact validated predecessor/target CHECK names"):
                cur.execute(preflight)
            cur.execute("ROLLBACK")
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_event_contract_artifacts_reject_forged_k2d_body_config_and_result_on_dev_postgres() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1019_k2d_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    artifacts = {
        "preflight": EVENT_CONTRACT_REPAIR_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema),
        "forward": EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema),
        "rollback": EVENT_CONTRACT_REPAIR_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema),
    }
    expected_body_sha256 = "9e5236fdc17b79888c864871e71ed6613b12759bbe87e070bd5c1c1db0b95451"
    wrong_catalog_sha256 = "0" * 64
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            cur.execute(f"SELECT pg_get_functiondef('{schema}.miniqmt_k2d_catalog_fingerprint()'::regprocedure)")
            genuine_k2d_function = str(cur.fetchone()[0])

            def restore_genuine_k2d() -> None:
                cur.execute(genuine_k2d_function)
                cur.execute(f"ALTER FUNCTION {schema}.miniqmt_k2d_catalog_fingerprint() RESET ALL")

            for artifact_name, artifact in artifacts.items():
                cur.execute(
                    f"""
                    CREATE OR REPLACE FUNCTION {schema}.miniqmt_k2d_catalog_fingerprint()
                    RETURNS TEXT LANGUAGE SQL STABLE
                    AS $forged_body$ SELECT '2d5fcbf0151d9e5d2a9d8537f834aabfd056a42cc0eeb8c079add68c8964f59f'::TEXT $forged_body$
                    """
                )
                with pytest.raises(psycopg2.Error, match="K2-D catalog function definition drift"):
                    cur.execute(artifact)
                cur.execute("ROLLBACK")
                restore_genuine_k2d()

                cur.execute(
                    f"ALTER FUNCTION {schema}.miniqmt_k2d_catalog_fingerprint() "
                    f"SET search_path = pg_catalog, {schema}"
                )
                with pytest.raises(psycopg2.Error, match="K2-D catalog function definition drift"):
                    cur.execute(artifact)
                cur.execute("ROLLBACK")
                restore_genuine_k2d()

                cur.execute(
                    f"""
                    CREATE OR REPLACE FUNCTION {schema}.miniqmt_k2d_catalog_fingerprint()
                    RETURNS TEXT LANGUAGE SQL STABLE
                    AS $forged_result$ SELECT '{wrong_catalog_sha256}'::TEXT $forged_result$
                    """
                )
                cur.execute(
                    """
                    SELECT encode(sha256(convert_to(
                        btrim(replace(function_record.prosrc,function_schema.nspname,'<schema>'),E' \n\r\t;'),
                        'UTF8'
                    )),'hex')
                    FROM pg_proc AS function_record
                    JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
                    WHERE function_record.oid=%s::regprocedure
                    """,
                    (f"{schema}.miniqmt_k2d_catalog_fingerprint()",),
                )
                forged_body_sha256 = str(cur.fetchone()[0])
                result_gate_artifact = artifact.replace(expected_body_sha256, forged_body_sha256)
                assert result_gate_artifact != artifact, artifact_name
                with pytest.raises(psycopg2.Error, match="independent K2-D catalog drift"):
                    cur.execute(result_gate_artifact)
                cur.execute("ROLLBACK")
                restore_genuine_k2d()

            cur.execute(artifacts["preflight"])
            cur.execute(artifacts["forward"])
            cur.execute(artifacts["preflight"])
            cur.execute(artifacts["rollback"])
            cur.execute(artifacts["preflight"])
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_event_contract_public_artifacts_resist_pg_temp_catalog_shadow_and_emit_independent_receipts() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1019_temp_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    artifacts = {
        "preflight": EVENT_CONTRACT_REPAIR_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema),
        "forward": EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema),
        "rollback": EVENT_CONTRACT_REPAIR_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema),
    }
    expected_k2d_catalog_sha256 = "2d5fcbf0151d9e5d2a9d8537f834aabfd056a42cc0eeb8c079add68c8964f59f"

    def execute_and_read_receipt(cur: object, artifact: str) -> dict[str, object]:
        receipt_sql, marker, trailing = artifact.rpartition("COMMIT;")
        assert marker == "COMMIT;" and not trailing.strip()
        cur.execute(receipt_sql)  # type: ignore[attr-defined]
        description = cur.description  # type: ignore[attr-defined]
        assert description is not None
        columns = tuple(str(item[0]) for item in description)
        row = tuple(cur.fetchone())  # type: ignore[attr-defined]
        cur.execute("COMMIT")  # type: ignore[attr-defined]
        return dict(zip(columns, row, strict=True))

    def assert_independent_receipt(receipt: dict[str, object]) -> None:
        assert receipt["k2_catalog_sha256"] == receipt["k2_independent_catalog_sha256"]
        assert receipt["k2_independent_catalog_sha256"] == receipt["k2_code_owned_catalog_sha256"]
        assert receipt["k2_catalog_authority_verified"] is True
        assert receipt["k2d_catalog_sha256"] == expected_k2d_catalog_sha256
        assert receipt["k2d_independent_catalog_sha256"] == expected_k2d_catalog_sha256
        assert receipt["k2d_code_owned_catalog_sha256"] == expected_k2d_catalog_sha256
        assert receipt["k2d_catalog_authority_verified"] is True

    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            cur.execute("CREATE TEMP TABLE pg_proc(marker TEXT)")

            cur.execute(f"SET search_path = pg_catalog, {schema}")
            cur.execute(
                "SELECT 'pg_proc'::regclass::oid='pg_temp.pg_proc'::regclass::oid"
            )
            assert cur.fetchone() == (True,)

            cur.execute(f"SET search_path = pg_catalog, {schema}, pg_temp")
            cur.execute(
                "SELECT 'pg_proc'::regclass::oid='pg_catalog.pg_proc'::regclass::oid, "
                f"{schema}.miniqmt_k2d_catalog_fingerprint()"
            )
            catalog_resolves_first, exact_catalog_sha256 = cur.fetchone()
            assert catalog_resolves_first is True
            assert exact_catalog_sha256 == expected_k2d_catalog_sha256

            for artifact_name in ("preflight", "forward", "rollback"):
                cur.execute(f"SET search_path = pg_catalog, {schema}")
                receipt = execute_and_read_receipt(cur, artifacts[artifact_name])
                assert_independent_receipt(receipt)

            # Exercise each byte-identical public entry too; the state transitions are
            # genuine no-op/apply/rollback operations after the receipt-bearing pass.
            cur.execute(f"SET search_path = pg_catalog, {schema}")
            cur.execute(artifacts["preflight"])
            cur.execute(artifacts["forward"])
            cur.execute(artifacts["rollback"])
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute("DROP TABLE IF EXISTS pg_temp.pg_proc")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_event_contract_repair_accepts_complete_six_check_predecessor_and_preserves_identity_checks() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1019_six_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    preflight = EVENT_CONTRACT_REPAIR_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    forward = EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    rollback = EVENT_CONTRACT_REPAIR_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    expected_names = (
        "ck_miniqmt_event_id",
        "ck_miniqmt_event_sequence",
        "ck_miniqmt_event_source",
        "ck_miniqmt_event_type",
        "ck_miniqmt_k2_event_composite",
        "ck_miniqmt_k2_event_contract",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            cur.execute(f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_sequence")
            with pytest.raises(psycopg2.Error, match="exact validated predecessor/target CHECK names"):
                cur.execute(preflight)
            cur.execute("ROLLBACK")
            cur.execute(
                f"ALTER TABLE {schema}.execution_runtime_event "
                "ADD CONSTRAINT ck_miniqmt_event_sequence CHECK (sequence > 0)"
            )
            cur.execute(preflight)
            cur.execute(forward)
            cur.execute(preflight)
            cur.execute(rollback)
            cur.execute(
                "SELECT conname FROM pg_constraint WHERE conrelid=%s::regclass AND contype='c' "
                'ORDER BY conname COLLATE "C"',
                (f"{schema}.execution_runtime_event",),
            )
            assert tuple(row[0] for row in cur.fetchall()) == expected_names
            cur.execute(forward)
            cur.execute(preflight)
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_event_contract_rollback_post_commit_ignores_legal_legacy_shared_facts_on_dev_postgres() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1019_legacy_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    forward = EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    rollback = EVENT_CONTRACT_REPAIR_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            cur.execute(forward)
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) "
                "VALUES ('runtime_legacy_rollback','2026-08-11')"
            )
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_algo_instance(
                    algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,
                    target_quantity,remaining_quantity,algo_code,status,kernel_contract_version
                ) VALUES (
                    'algo_legacy_rollback','runtime_legacy_rollback','intent_legacy_rollback',
                    'slot_legacy_rollback','600000.SH','BUY',100,100,'TWAP','ACTIVE','LEGACY_V1'
                )
                """
            )
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_child_order(
                    child_order_id,runtime_id,algo_instance_id,parent_intent_id,strategy_slot_id,
                    symbol,side,quantity,price,price_type,status,kernel_contract_version
                ) VALUES (
                    'child_legacy_rollback','runtime_legacy_rollback','algo_legacy_rollback',
                    'intent_legacy_rollback','slot_legacy_rollback','600000.SH','BUY',100,
                    10.000000,2,'SUBMITTING','LEGACY_V1'
                )
                """
            )

            cur.execute(rollback)
            cur.execute(
                f"SELECT count(*),count(*) FILTER (WHERE kernel_contract_version='KERNEL_V2') "
                f"FROM {schema}.execution_algo_instance"
            )
            assert cur.fetchone() == (1, 0)
            cur.execute(
                f"SELECT count(*),count(*) FILTER (WHERE kernel_contract_version='KERNEL_V2') "
                f"FROM {schema}.execution_child_order"
            )
            assert cur.fetchone() == (1, 0)
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_event_contract_repair_post_commit_assertion_fails_nonzero_and_exact_rerun_recovers() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1019_commit_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    target_composite_sha256 = "4a2d33d3fc75a4b468661e1bdbf2ecce9cd13aaab491c7c4d7605a1df3af3857"
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            forward = EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            prefix, separator, suffix = forward.rpartition(target_composite_sha256)
            assert separator == target_composite_sha256
            tampered_post_commit = prefix + ("0" * 64) + suffix
            with pytest.raises(psycopg2.Error, match="post-commit exact constraint readback drift"):
                cur.execute(tampered_post_commit)
            cur.execute("ROLLBACK")
            cur.execute(
                f"SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') "
                f"FROM pg_constraint WHERE conrelid='{schema}.execution_runtime_event'::regclass "
                "AND conname='ck_miniqmt_k2_event_composite'"
            )
            assert cur.fetchone()[0] == target_composite_sha256
            cur.execute(forward)
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_event_contract_artifacts_reject_unregistered_event_check_in_both_states() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1019_extra_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            preflight = EVENT_CONTRACT_REPAIR_PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            forward = EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            rollback = EVENT_CONTRACT_REPAIR_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema)

            for state in ("predecessor", "target"):
                cur.execute(
                    f"ALTER TABLE {schema}.execution_runtime_event "
                    "ADD CONSTRAINT ck_blocks_legal_tick CHECK (event_type <> 'TICK')"
                )
                for artifact in (preflight, forward, rollback):
                    with pytest.raises(psycopg2.Error, match="exact validated predecessor/target CHECK names"):
                        cur.execute(artifact)
                    cur.execute("ROLLBACK")
                cur.execute(f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_blocks_legal_tick")
                if state == "predecessor":
                    cur.execute(forward)

            cur.execute(forward)
            cur.execute(preflight)
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_event_contract_forward_post_commit_rejects_raced_extra_event_check() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1019_race_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            forward = EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            marker = "-- Independent post-commit assertion.  Any mismatch exits non-zero."
            prefix, separator, suffix = forward.partition(marker)
            assert separator == marker
            cur.execute(prefix)
            cur.execute(
                f"ALTER TABLE {schema}.execution_runtime_event "
                "ADD CONSTRAINT ck_blocks_legal_tick CHECK (event_type <> 'TICK')"
            )
            with pytest.raises(psycopg2.Error, match="post-commit exact constraint readback drift"):
                cur.execute(marker + suffix)
            cur.execute("ROLLBACK")
            cur.execute(
                "SELECT conname FROM pg_constraint WHERE conrelid=%s::regclass AND conname='ck_blocks_legal_tick'",
                (f"{schema}.execution_runtime_event",),
            )
            assert cur.fetchone() == ("ck_blocks_legal_tick",)
            cur.execute(f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_blocks_legal_tick")
            cur.execute(forward)
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_event_contract_rollback_post_commit_rejects_raced_extra_event_check() -> None:
    schema = _fixture_schema().replace("k2a_", "bug1019_rollback_race_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _install_event_contract_predecessor(cur, schema)
            _apply_current_k6_predecessor(cur, schema)
            forward = EVENT_CONTRACT_REPAIR_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            rollback = EVENT_CONTRACT_REPAIR_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema)
            cur.execute(forward)
            marker = "-- Independent post-commit predecessor assertion.  Any mismatch exits non-zero."
            prefix, separator, suffix = rollback.partition(marker)
            assert separator == marker
            cur.execute(prefix)
            cur.execute(
                f"ALTER TABLE {schema}.execution_runtime_event "
                "ADD CONSTRAINT ck_blocks_legal_tick CHECK (event_type <> 'TICK')"
            )
            with pytest.raises(psycopg2.Error, match="rollback post-commit exact constraint readback drift"):
                cur.execute(marker + suffix)
            cur.execute("ROLLBACK")
            cur.execute(
                "SELECT conname FROM pg_constraint WHERE conrelid=%s::regclass AND conname='ck_blocks_legal_tick'",
                (f"{schema}.execution_runtime_event",),
            )
            assert cur.fetchone() == ("ck_blocks_legal_tick",)
            cur.execute(f"ALTER TABLE {schema}.execution_runtime_event DROP CONSTRAINT ck_blocks_legal_tick")
            cur.execute(forward)
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def _apply_forward(cur: object, forward: str) -> None:
    match = re.search(r"ALTER TABLE ([A-Za-z0-9_]+)\.execution_runtime_event", forward)
    target_schema = "qmt_strategy" if match is None else match.group(1)
    cur.execute(  # type: ignore[attr-defined]
        "SELECT pg_get_constraintdef(oid,true) FROM pg_constraint "
        "WHERE conrelid=to_regclass(%s) AND conname='ck_miniqmt_k2_timer_occurrence_initial'",
        (f"{target_schema}.execution_algo_timer_occurrence",),
    )
    row = cur.fetchone()  # type: ignore[attr-defined]
    if row is None or "row_version = lease_epoch" not in str(row[0]):
        _apply_base_forward(cur, forward)
    cur.execute(  # type: ignore[attr-defined]
        "SELECT to_regclass(%s)",
        (f"{target_schema}.execution_broker_reconciliation_attempt",),
    )
    k2d_already_applied = cur.fetchone()[0] is not None  # type: ignore[attr-defined]
    if not k2d_already_applied:
        k2c_forward = K2C_FORWARD.read_text(encoding="utf-8")
        cur.execute(k2c_forward.replace("qmt_strategy", target_schema))  # type: ignore[attr-defined]
    k2d_forward = K2D_FORWARD.read_text(encoding="utf-8")
    cur.execute(k2d_forward.replace("qmt_strategy", target_schema))  # type: ignore[attr-defined]


def _apply_rollback(cur: object, rollback: str) -> None:
    match = re.search(r"FROM ([A-Za-z0-9_]+)\.execution_runtime_event", rollback)
    target_schema = "qmt_strategy" if match is None else match.group(1)
    cur.execute(  # type: ignore[attr-defined]
        K2D_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", target_schema)
    )
    cur.execute(  # type: ignore[attr-defined]
        K2C_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", target_schema)
    )
    stage1, remainder = rollback.split("-- Stage 2: nontransactional concurrent index cleanup.", maxsplit=1)
    stage2, stage3 = remainder.split("-- Stage 3: independent rollback readback.", maxsplit=1)
    cur.execute(stage1)  # type: ignore[attr-defined]
    for statement in stage2.split(";"):
        if "DROP INDEX CONCURRENTLY" in statement:
            cur.execute(statement)  # type: ignore[attr-defined]
    cur.execute(stage3)  # type: ignore[attr-defined]


def test_k2_migration_first_second_apply_constraints_and_guarded_rollback_on_dev_postgres() -> None:
    schema = _fixture_schema()
    preflight = PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    forward = FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    rollback = ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            cur.execute(preflight)
            _apply_forward(cur, forward)
            first_catalog = _catalog_snapshot(cur, schema)
            _apply_forward(cur, forward)
            assert _catalog_snapshot(cur, schema) == first_catalog
            _insert_valid_k2_constraint_graph(cur, schema)

            sha = "a" * 64
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_algo_instance(
                    algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,
                    remaining_quantity,algo_code,status,kernel_contract_version,traded_quantity,plugin_id,
                    plugin_version,plugin_manifest_sha256,plugin_config_json,plugin_config_sha256,
                    compatibility_receipt_sha256,state_schema_version,state_json,state_sha256,
                    transition_sequence,last_applied_delivery_sequence,last_applied_delivery_id,
                    last_closed_delivery_sequence,terminal_delivery_sequence,failure_receipt_id,
                    active_child_closure_status,active_child_count,row_version,terminal_at_utc,kernel_carrier_json
                ) VALUES (
                    'algo_initialization_failed','runtime_constraints','intent_initialization_failed',
                    'slot_initialization_failed','600001.SH','BUY',100,100,'TWAP','FAILED','KERNEL_V2',0,
                    'aistock.twap','1.0.0',%s,'{{}}'::jsonb,%s,%s,NULL,NULL,NULL,0,0,NULL,1,1,
                    'failure_initialization_failed','CLEAN',0,1,now(),'{{}}'::jsonb
                )
                """,
                (sha, sha, sha),
            )
            with pytest.raises(psycopg2.Error):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_algo_instance(
                        algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,
                        remaining_quantity,algo_code,status,kernel_contract_version,traded_quantity,plugin_id,
                        plugin_version,plugin_manifest_sha256,plugin_config_json,plugin_config_sha256,
                        compatibility_receipt_sha256,state_schema_version,state_json,state_sha256,
                        transition_sequence,last_applied_delivery_sequence,last_closed_delivery_sequence,
                        terminal_delivery_sequence,active_child_closure_status,active_child_count,row_version,
                        terminal_at_utc,kernel_carrier_json
                    ) VALUES (
                        'algo_failed_without_receipt','runtime_constraints','intent_failed_without_receipt',
                        'slot_failed_without_receipt','600002.SH','BUY',100,100,'TWAP','FAILED','KERNEL_V2',0,
                        'aistock.twap','1.0.0',%s,'{{}}'::jsonb,%s,%s,NULL,NULL,NULL,0,0,1,1,'CLEAN',0,1,
                        now(),'{{}}'::jsonb
                    )
                    """,
                    (sha, sha, sha),
                )

            with pytest.raises(psycopg2.Error):
                cur.execute(
                    f"INSERT INTO {schema}.execution_runtime_event("
                    "event_id,runtime_id,sequence,event_type,event_time,source,payload,event_contract_version,"
                    "event_schema_version,payload_schema_version,event_key_sha256,payload_sha256,observed_at_utc,"
                    "logical_at_utc,source_identity_json,correlation_json,ingress_receipt_json,"
                    "ingress_receipt_sha256,routing_rule_version,transaction_commit_identity) "
                    "VALUES ('event_bad_source','runtime_constraints',2,'TICK',now(),'QMT_GATEWAY_CALLBACK',"
                    "'{}'::jsonb,'KERNEL_V2','miniqmt_runtime_event_envelope_v2','miniqmt_market_data_view_v2',"
                    "%s,%s,now(),now(),'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,%s,'miniqmt_event_routing_v1','tx')",
                    ("b" * 64, "b" * 64, "b" * 64),
                )
            with pytest.raises(psycopg2.Error):
                cur.execute(
                    f"INSERT INTO {schema}.execution_algo_event_delivery("
                    "delivery_id,event_id,runtime_id,algo_instance_id,plugin_manifest_sha256,"
                    "algo_delivery_sequence,status,attempt_count,row_version,created_at_utc,updated_at_utc,carrier_json) "
                    "VALUES ('delivery_duplicate_sequence','event_constraints','runtime_constraints',"
                    "'algo_constraints',%s,1,'PENDING',0,1,now(),now(),'{}'::jsonb)",
                    ("b" * 64,),
                )
            with pytest.raises(psycopg2.Error):
                cur.execute(
                    f"INSERT INTO {schema}.execution_child_order("
                    "child_order_id,runtime_id,algo_instance_id,parent_intent_id,strategy_slot_id,symbol,side,"
                    "quantity,price,price_type,status,metadata,kernel_contract_version,mapping_id) "
                    "VALUES ('child_half','runtime_constraints','algo_constraints','intent_constraints',"
                    "'slot_constraints','600000.SH','BUY',100,10,2,'SUBMITTING','{}'::jsonb,'KERNEL_V2','half')"
                )
            with pytest.raises(psycopg2.Error):
                cur.execute(
                    f"INSERT INTO {schema}.execution_algo_transition("
                    "transition_id,delivery_id,event_id,runtime_id,algo_instance_id,transition_sequence,"
                    "transition_kind,transition_receipt_json,receipt_sha256,execution_projection_set_json,"
                    "execution_projection_set_sha256,after_state_json,after_state_sha256,transaction_commit_identity) "
                    "VALUES ('transition_orphan','missing_delivery','event_constraints','runtime_constraints',"
                    "'algo_constraints',2,'APPLIED','{}'::jsonb,%s,'{}'::jsonb,%s,'{}'::jsonb,%s,'tx')",
                    ("b" * 64, "b" * 64, "b" * 64),
                )
            with pytest.raises(psycopg2.Error):
                cur.execute(
                    f"INSERT INTO {schema}.execution_algo_command_outbox("
                    "command_id,transition_id,ordinal,runtime_id,algo_instance_id,parent_intent_id,mapping_id,"
                    "command_type,local_vt_orderid,payload_json,payload_sha256,status,attempt_count,"
                    "deterministic_client_order_ref,broker_called,ack_receipt_sha256,row_version,created_at_utc,"
                    "updated_at_utc,closed_at_utc,carrier_json,outbox_row_sha256) VALUES ("
                    "'command_bad_ack','transition_constraints',0,'runtime_constraints','algo_constraints',"
                    "'intent_constraints','mapping_constraints','SUBMIT_LIMIT','local_constraints','{}'::jsonb,"
                    "%s,'ACKED',1,'client_constraints',true,%s,1,now(),now(),now(),'{}'::jsonb,%s)",
                    ("b" * 64, "b" * 64, "b" * 64),
                )

            with pytest.raises(psycopg2.Error):
                cur.execute(
                    f"INSERT INTO {schema}.execution_algo_instance("
                    "algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,"
                    "remaining_quantity,algo_code,status,kernel_contract_version) "
                    "VALUES ('half','missing','intent','slot','600000.SH','BUY',100,100,'TWAP','ACTIVE','KERNEL_V2')"
                )
            with pytest.raises(psycopg2.Error):
                cur.execute(
                    f"INSERT INTO {schema}.execution_algo_event_delivery("
                    "delivery_id,event_id,runtime_id,algo_instance_id,plugin_manifest_sha256,"
                    "algo_delivery_sequence,status,attempt_count,lease_owner,lease_worker_id,"
                    "lease_process_incarnation_id,lease_epoch,lease_fence_token,lease_expires_at,"
                    "row_version,created_at_utc,updated_at_utc,carrier_json) "
                    "VALUES ('delivery','event','runtime','algo',%s,1,'CLAIMED',1,'worker:missing',"
                    "'worker','missing',1,'fence',now()+interval '1 minute',1,now(),now(),'{}'::jsonb)",
                    ("a" * 64,),
                )

            cur.execute(f"DELETE FROM {schema}.execution_child_order WHERE runtime_id='runtime_constraints'")
            cur.execute(f"DELETE FROM {schema}.execution_algo_transition WHERE runtime_id='runtime_constraints'")
            cur.execute(f"DELETE FROM {schema}.execution_algo_event_delivery WHERE runtime_id='runtime_constraints'")
            cur.execute(f"DELETE FROM {schema}.execution_runtime_event WHERE runtime_id='runtime_constraints'")
            cur.execute(f"DELETE FROM {schema}.execution_algo_instance WHERE runtime_id='runtime_constraints'")
            cur.execute(f"DELETE FROM {schema}.execution_runtime WHERE runtime_id='runtime_constraints'")

            _apply_rollback(cur, rollback)
            assert _catalog_snapshot(cur, schema)
            _apply_forward(cur, forward)
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) "
                "VALUES ('runtime_exchange_authority','2026-07-25')"
            )
            cur.execute(
                f"INSERT INTO {schema}.execution_exchange_session_authority("
                "runtime_id,exchange_trade_date,calendar_snapshot_set_id,calendar_snapshot_set_sha256,"
                "session_definition_version,authority_sha256,authority_json) "
                "VALUES ('runtime_exchange_authority','2026-07-25','calendar_set',%s,'session_v1',%s,'{}'::jsonb)",
                ("a" * 64, "b" * 64),
            )
            with pytest.raises(psycopg2.Error, match="destructive rollback refused"):
                _apply_rollback(cur, rollback)
            cur.execute("ROLLBACK")
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


@pytest.mark.parametrize(
    "drift_sql",
    (
        """
        ALTER TABLE {schema}.execution_algo_command_outbox
            DROP CONSTRAINT ck_miniqmt_k2_outbox_broker_called;
        ALTER TABLE {schema}.execution_algo_command_outbox
            ADD CONSTRAINT ck_miniqmt_k2_outbox_broker_called CHECK (broker_called IS NULL) NOT VALID
        """,
        """
        ALTER TABLE {schema}.execution_exchange_session_authority
            DROP CONSTRAINT fk_miniqmt_k2_exchange_session_runtime;
        ALTER TABLE {schema}.execution_exchange_session_authority
            ADD CONSTRAINT fk_miniqmt_k2_exchange_session_runtime FOREIGN KEY (runtime_id)
            REFERENCES {schema}.execution_runtime(runtime_id) NOT VALID
        """,
        """
        DROP INDEX {schema}.uq_miniqmt_k2_child_broker_order;
        CREATE UNIQUE INDEX uq_miniqmt_k2_child_broker_order
            ON {schema}.execution_child_order(broker_order_id)
            WHERE broker_order_id IS NOT NULL
        """,
        "ALTER TABLE {schema}.execution_exchange_session_authority ALTER COLUMN session_definition_version TYPE VARCHAR(128)",
        "ALTER TABLE {schema}.execution_kernel_worker_epoch ALTER COLUMN incarnation_sequence DROP NOT NULL",
        "ALTER TABLE {schema}.execution_kernel_worker_epoch ALTER COLUMN incarnation_sequence DROP DEFAULT",
    ),
    ids=("check", "owner_fk", "partial_predicate", "column_type", "column_nullability", "column_default"),
)
def test_k2_preflight_and_forward_reject_exact_catalog_drift_on_dev_postgres(drift_sql: str) -> None:
    schema = _fixture_schema()
    preflight = PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    forward = FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            cur.execute(preflight)
            _apply_forward(cur, forward)
            cur.execute(drift_sql.format(schema=schema))
            with pytest.raises(psycopg2.Error, match="exact schema catalog drift"):
                cur.execute(preflight)
            cur.execute("ROLLBACK")
            with pytest.raises(psycopg2.Error, match="catalog drift"):
                _apply_forward(cur, forward)
            cur.execute("ROLLBACK")
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


@pytest.mark.parametrize("with_schema_drift", (False, True), ids=("function_only", "function_and_schema"))
def test_k2_preflight_rejects_forged_constant_catalog_function_on_dev_postgres(
    with_schema_drift: bool,
) -> None:
    schema = _fixture_schema()
    preflight = PREFLIGHT.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    forward = FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            cur.execute(preflight)
            _apply_forward(cur, forward)
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
            with pytest.raises(psycopg2.Error, match="catalog (function|schema) drift"):
                cur.execute(preflight)
            cur.execute("ROLLBACK")
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_k2_migration_initial_state_checks_reject_forged_history_on_dev_postgres() -> None:
    schema = _fixture_schema()
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_base_fixture_sql(schema))
            _apply_forward(cur, FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            _insert_valid_k2_constraint_graph(cur, schema)
            sha = "a" * 64
            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_k2_delivery_initial"):
                cur.execute(
                    f"""
                    UPDATE {schema}.execution_algo_event_delivery
                    SET status='PENDING',attempt_count=7,lease_epoch=99,row_version=12,
                        transition_id=NULL,last_error_json=NULL,next_attempt_at_utc=NULL,
                        failure_receipt_id=NULL,skip_receipt_id=NULL,closed_at_utc=NULL
                    WHERE delivery_id='delivery_constraints'
                    """
                )
            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_k2_child_mapping_initial"):
                cur.execute(
                    f"UPDATE {schema}.execution_child_order SET mapping_version=2 "
                    "WHERE mapping_id='mapping_constraints'"
                )
            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_k2_outbox_initial"):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_algo_command_outbox(
                        command_id,transition_id,ordinal,runtime_id,algo_instance_id,parent_intent_id,
                        mapping_id,command_type,local_vt_orderid,payload_json,payload_sha256,status,
                        attempt_count,lease_epoch,deterministic_client_order_ref,row_version,
                        created_at_utc,updated_at_utc,carrier_json,outbox_row_sha256
                    ) VALUES (
                        'command_constraints','transition_constraints',0,'runtime_constraints','algo_constraints',
                        'intent_constraints','mapping_constraints','SUBMIT_LIMIT','local_constraints','{{}}'::jsonb,
                        %s,'PENDING',7,99,'client_constraints',12,now(),now(),'{{}}'::jsonb,%s
                    )
                    """,
                    (sha, sha),
                )
            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_k2_timer_schedule_initial"):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_algo_timer_schedule(
                        schedule_id,runtime_id,algo_instance_id,timer_name,schedule_epoch,due_at_exchange_utc,
                        catch_up_policy,payload_json,payload_sha256,status,timer_occurrence_id,
                        lease_epoch,row_version,created_at_utc,updated_at_utc,schedule_receipt_sha256,carrier_json
                    ) VALUES (
                        'schedule_forged','runtime_constraints','algo_constraints','timer_forged','epoch_forged',now(),
                        'EXPIRE_IF_LATE','{{}}'::jsonb,%s,'SCHEDULED','occurrence_forged',99,2,now(),now(),%s,'{{}}'::jsonb
                    )
                    """,
                    (sha, sha),
                )
            cur.execute(
                f"""
                INSERT INTO {schema}.execution_algo_timer_schedule(
                    schedule_id,runtime_id,algo_instance_id,timer_name,schedule_epoch,due_at_exchange_utc,
                    catch_up_policy,payload_json,payload_sha256,status,timer_occurrence_id,
                    lease_epoch,row_version,created_at_utc,updated_at_utc,schedule_receipt_sha256,carrier_json
                ) VALUES (
                    'schedule_valid','runtime_constraints','algo_constraints','timer_valid','epoch_valid',now(),
                    'EXPIRE_IF_LATE','{{}}'::jsonb,%s,'SCHEDULED','occurrence_valid',0,1,now(),now(),%s,'{{}}'::jsonb
                )
                """,
                (sha, sha),
            )
            with pytest.raises(psycopg2.errors.CheckViolation, match="ck_miniqmt_k2_timer_occurrence_initial"):
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_algo_timer_occurrence(
                        timer_occurrence_id,schedule_id,runtime_id,algo_instance_id,due_at_exchange_utc,
                        exchange_session_authority_sha256,status,lease_epoch,row_version,created_at_utc,
                        occurrence_receipt_sha256,carrier_json
                    ) VALUES (
                        'occurrence_valid','schedule_valid','runtime_constraints','algo_constraints',now(),
                        %s,'CLAIMED',2,2,now(),%s,'{{}}'::jsonb
                    )
                    """,
                    (sha, sha),
                )
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()
