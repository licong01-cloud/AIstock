from __future__ import annotations

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
        runtime_id TEXT PRIMARY KEY, trade_date DATE NOT NULL
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


def _apply_forward(cur: object, forward: str) -> None:
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


def _apply_rollback(cur: object, rollback: str) -> None:
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
            with pytest.raises(psycopg2.Error, match="schema catalog drift"):
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
