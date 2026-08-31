from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import re
from uuid import uuid4

from dotenv import load_dotenv
import psycopg2
import pytest

from backend.services.miniqmt_execution_runtime.kernel_current_three_contracts import CurrentThreeContractError
from backend.services.miniqmt_execution_runtime.repository import PostgresMiniQMTExecutionRuntimeRepository


REPO_ROOT = Path(__file__).resolve().parents[3]


def _runtime_repo_root() -> Path:
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
    if os.getenv("AISTOCK_RUN_MINIQMT_K3_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K3 DEV PostgreSQL fixture")
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
        raise AssertionError(f"refusing non-DEV K3 target {dsn['host']}:{dsn['port']}/{dsn['dbname']}")
    if not dsn["user"] or not dsn["password"]:
        pytest.fail("guarded DEV credentials are unavailable")
    return dsn


def _schema() -> str:
    value = "k3b_" + uuid4().hex
    assert re.fullmatch(r"k3b_[0-9a-f]{32}", value)
    return value


def _ddl(schema: str) -> str:
    return f"""
    CREATE SCHEMA {schema};
    CREATE TABLE {schema}.execution_runtime (
      runtime_id TEXT PRIMARY KEY, account_group_id TEXT NOT NULL, trade_date DATE NOT NULL,
      mode TEXT NOT NULL, event_loop_state TEXT NOT NULL, gateway_state TEXT NOT NULL,
      oms_state TEXT NOT NULL, runtime_config_hash TEXT NOT NULL, last_event_sequence INTEGER NOT NULL,
      metadata JSONB NOT NULL, archived_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    );
    CREATE TABLE {schema}.execution_runtime_event (
      event_id TEXT PRIMARY KEY, runtime_id TEXT NOT NULL REFERENCES {schema}.execution_runtime(runtime_id),
      sequence INTEGER NOT NULL, event_type TEXT NOT NULL, event_time TIMESTAMPTZ NOT NULL,
      source TEXT NOT NULL, payload JSONB NOT NULL, archived_at TIMESTAMPTZ
    );
    CREATE TABLE {schema}.execution_algo_instance (
      algo_instance_id TEXT PRIMARY KEY, runtime_id TEXT NOT NULL REFERENCES {schema}.execution_runtime(runtime_id),
      parent_intent_id TEXT NOT NULL, strategy_slot_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
      target_quantity INTEGER NOT NULL, remaining_quantity INTEGER NOT NULL, algo_code TEXT NOT NULL,
      status TEXT NOT NULL, metadata JSONB NOT NULL, archived_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
    );
    CREATE TABLE {schema}.execution_child_order (
      child_order_id TEXT PRIMARY KEY, runtime_id TEXT NOT NULL REFERENCES {schema}.execution_runtime(runtime_id),
      algo_instance_id TEXT NOT NULL REFERENCES {schema}.execution_algo_instance(algo_instance_id),
      parent_intent_id TEXT NOT NULL, strategy_slot_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
      quantity INTEGER NOT NULL, price NUMERIC(20,6) NOT NULL, price_type INTEGER NOT NULL,
      status TEXT NOT NULL, broker_order_id TEXT, submitted_at TIMESTAMPTZ, metadata JSONB NOT NULL,
      archived_at TIMESTAMPTZ, updated_at TIMESTAMPTZ NOT NULL
    );
    """


def test_postgres_shadow_snapshot_positive_capacity_and_corruption_are_read_only() -> None:
    dsn = _dev_dsn()
    schema = _schema()

    @contextmanager
    def factory(*, autocommit: bool = True, manage_transaction: bool = False):
        conn = psycopg2.connect(**dsn)
        conn.autocommit = autocommit and not manage_transaction
        try:
            yield conn
            if manage_transaction:
                conn.commit()
        except BaseException:
            if not conn.closed:
                conn.rollback()
            raise
        finally:
            conn.close()

    admin = psycopg2.connect(**dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(_ddl(schema))
            cur.execute(
                f"""INSERT INTO {schema}.execution_runtime VALUES (
                    'runtime_dev','account_dev','2026-07-29','SIM','RUNNING','CONNECTED','OPEN','hash',1,
                    '{{"repository_commit_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}'::jsonb,
                    NULL,now(),now())"""
            )
            cur.execute(
                f"""INSERT INTO {schema}.execution_algo_instance VALUES (
                    'legacy_algo_dev','runtime_dev','parent_dev','slot_dev','600000.SH','BUY',100,100,
                    'SNIPER_MINIQMT','ACTIVE',
                    '{{"config":{{"price_mode":"LIMIT_TRIGGER_BY_BEST_QUOTE"}},"legacy_state":{{"status":"RUNNING"}},
                       "limit_price_decimal":"10","pricetick_decimal":"0.01","min_volume":100,"volume_increment":100}}'::jsonb,
                    NULL,now(),now())"""
            )
            cur.execute(
                f"""INSERT INTO {schema}.execution_child_order VALUES (
                    'legacy_child_dev','runtime_dev','legacy_algo_dev','parent_dev','slot_dev','600000.SH','BUY',100,
                    10,11,'SUBMITTED','broker_dev',now(),'{{"reason_code":"sniper_ask_crossed_limit"}}'::jsonb,NULL,now())"""
            )
            cur.execute(
                f"""INSERT INTO {schema}.execution_runtime_event VALUES (
                    'tick_dev','runtime_dev',1,'TICK',now(),'gateway',
                    '{{"symbol":"600000.SH","generation":1,"bid_price_1":9.99,"ask_price_1":10,
                       "bid_volume_1":100,"ask_volume_1":100,"market_data_projection_id":"market_dev",
                       "market_data_projection_sha256":"1111111111111111111111111111111111111111111111111111111111111111"}}'::jsonb,NULL)"""
            )
        repository = PostgresMiniQMTExecutionRuntimeRepository(factory, _shadow_read_schema=schema)
        read = repository.read_current_three_shadow_snapshot("runtime_dev")
        assert read.snapshot.event_count == 1
        assert read.snapshot.algo_count == 1
        assert read.snapshot.child_count == 1
        assert read.strict_readback_v1() == read.snapshot

        with admin.cursor() as cur:
            cur.execute(
                f"""INSERT INTO {schema}.execution_runtime_event VALUES (
                    'timer_gap','runtime_dev',3,'TIMER',now(),'runtime',
                    '{{"timer_name":"TWAP_ACTIVE_SECOND","timer_occurrence_id":"occ_gap",
                       "schedule_epoch":"session_gap","monotonic_ns":3}}'::jsonb,NULL)"""
            )
            cur.execute(f"SELECT COUNT(*) FROM {schema}.execution_runtime_event WHERE runtime_id='runtime_dev'")
            before = cur.fetchone()[0]
        with pytest.raises(CurrentThreeContractError) as exc_info:
            repository.read_current_three_shadow_snapshot("runtime_dev")
        assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_SOURCE_INVALID"
        with admin.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.execution_runtime_event WHERE runtime_id='runtime_dev'")
            assert cur.fetchone()[0] == before

        with admin.cursor() as cur:
            cur.execute(
                f"""INSERT INTO {schema}.execution_runtime VALUES (
                    'runtime_capacity','account_dev','2026-07-29','SIM','RUNNING','CONNECTED','OPEN','hash',0,
                    '{{"repository_commit_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}'::jsonb,
                    NULL,now(),now())"""
            )
            cur.execute(
                f"""INSERT INTO {schema}.execution_algo_instance
                    SELECT 'algo_capacity_'||g,'runtime_capacity','parent_'||g,'slot_'||g,'600000.SH','BUY',
                           100,100,'SNIPER_MINIQMT','ACTIVE','{{}}'::jsonb,NULL,now(),now()
                    FROM generate_series(1,1001) AS g"""
            )
        with pytest.raises(CurrentThreeContractError) as exc_info:
            repository.read_current_three_shadow_snapshot("runtime_capacity")
        assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_SOURCE_CAPACITY_EXCEEDED"
        assert exc_info.value.context["algo_count"] == 1001
    finally:
        try:
            with admin.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            admin.close()
