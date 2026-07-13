"""Explicitly authorized DEV-DB validation for the Phase 1D source observer."""

from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import psycopg2.extras
import pytest
from dotenv import load_dotenv

from backend.services.advisory_phase1.source_capacity import (
    AdvisoryPhase1CapacityProbe,
    CapacityPlanningRequest,
    CapacityStatus,
)
from backend.services.advisory_phase1.source_observer import (
    SOURCE_QUERY_TEMPLATES,
    SourceObserverConfigBundle,
    default_source_observer_config,
)
from backend.services.advisory_phase1.source_observer_postgres import PostgresSourceObserverRepository


_ENV_FILE = Path("F:/Dev/AIstock/.env")
_MIGRATION = Path("backend/db/migrations/add_advisory_phase1_source_observer_20260714.sql")
_ROLLBACK = Path("backend/db/migrations/add_advisory_phase1_source_observer_20260714.rollback.sql")


def _dev_dsn() -> dict[str, Any]:
    if os.getenv("AISTOCK_DEV_DB_E2E") != "1":
        pytest.skip("set AISTOCK_DEV_DB_E2E=1 to authorize the DEV-DB stateful L4 gate")
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=False)
    dsn = {
        "host": os.getenv("TDX_DB_DEV_HOST"),
        "port": int(os.getenv("TDX_DB_DEV_PORT", "0")),
        "dbname": os.getenv("TDX_DB_DEV_NAME"),
        "user": os.getenv("TDX_DB_DEV_USER"),
        "password": os.getenv("TDX_DB_DEV_PASSWORD"),
    }
    if dsn["host"] != "127.0.0.1" or dsn["port"] != 5433 or "dev" not in str(dsn["dbname"] or "").lower():
        raise AssertionError(f"refusing Phase 1D target {dsn['host']}:{dsn['port']}/{dsn['dbname']}")
    if not dsn["user"] or not dsn["password"]:
        pytest.skip("DEV DB credentials are unavailable")
    return dsn


def _single_dataset_config(*, effective_from: datetime, batch_size: int = 10) -> SourceObserverConfigBundle:
    base = default_source_observer_config()
    daily_basic = next(spec for spec in base.dataset_specs if spec.dataset_name == "daily_basic")
    return base.model_copy(
        update={
            "observer_config_id": f"phase1d_dev_e2e_{uuid.uuid4().hex}",
            "effective_from_observed_at": effective_from,
            "audit_scan_batch_size": batch_size,
            "dataset_specs": (daily_basic,),
        }
    )


@contextmanager
def _savepoint_factory(conn: Any) -> Iterator[Any]:
    name = f"observer_{uuid.uuid4().hex}"
    with conn.cursor() as cur:
        cur.execute(f"SAVEPOINT {name}")
    try:
        yield conn
    except Exception:
        with conn.cursor() as cur:
            cur.execute(f"ROLLBACK TO SAVEPOINT {name}")
            cur.execute(f"RELEASE SAVEPOINT {name}")
        raise
    else:
        with conn.cursor() as cur:
            cur.execute(f"RELEASE SAVEPOINT {name}")


@contextmanager
def _dev_transaction_factory(dsn: dict[str, Any]) -> Iterator[Any]:
    conn = psycopg2.connect(**dsn)
    conn.autocommit = False
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()


def test_observer_event_receipt_cursor_atomicity_and_exact_retry_dev_db(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = psycopg2.connect(**_dev_dsn())
    conn.set_session(isolation_level="REPEATABLE READ", autocommit=False)
    marker_date = date(2099, 12, 30)
    refreshed_at = datetime.now(UTC)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_MIGRATION.read_text(encoding="utf-8"))
            cur.execute(_MIGRATION.read_text(encoding="utf-8"))
            cur.execute(
                """
                INSERT INTO market.daily_basic (ts_code, trade_date)
                VALUES ('999999.SZ', %s);
                INSERT INTO market.dataset_date_refresh_audit (
                    dataset, trade_date, data_source, job_id, status, row_count, refreshed_at,
                    error_message, metadata, written_rows, expected_rows, coverage_ratio,
                    quality_status, failure_category
                ) VALUES ('daily_basic', %s, 'tushare', %s, 'success', 1, %s,
                          NULL, '{}'::jsonb, 1, 1, 1.0, 'ok', NULL)
                """,
                (marker_date, marker_date, str(uuid.uuid4()), refreshed_at),
            )
        source = {"trade_date": marker_date, "refreshed_at": refreshed_at}
        config = _single_dataset_config(effective_from=refreshed_at, batch_size=1)
        config_hash = config.config_hash(SOURCE_QUERY_TEMPLATES)
        monkeypatch.setattr(
            PostgresSourceObserverRepository,
            "_begin_observer_transaction",
            staticmethod(
                lambda *, cur, config: (
                    cur.execute("SELECT set_config('statement_timeout', %s, true)", (str(config.statement_timeout_ms),)),
                    cur.execute("SELECT set_config('lock_timeout', %s, true)", (str(config.lock_timeout_ms),)),
                )
            ),
        )
        repository = PostgresSourceObserverRepository(conn_factory=lambda: _savepoint_factory(conn))
        first = repository.observe_once(config=config, registry=SOURCE_QUERY_TEMPLATES)
        assert first.succeeded and first.appended == 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_observation_receipt WHERE observer_config_hash = %s",
                (config_hash,),
            )
            receipt_count = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_availability_event WHERE dataset_name = 'daily_basic' "
                "AND source_role = 'FEATURE_T' AND partition_key ->> 'trade_date' = %s",
                (source["trade_date"].isoformat(),),
            )
            event_count = int(cur.fetchone()[0])
        assert receipt_count == 1 and event_count == 1
        retry = repository.observe_once(config=config, registry=SOURCE_QUERY_TEMPLATES)
        assert retry.succeeded and retry.appended == 0
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_observation_receipt WHERE observer_config_hash = %s",
                (config_hash,),
            )
            assert int(cur.fetchone()[0]) == receipt_count
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION pg_temp.reject_phase1d_receipt() RETURNS TRIGGER AS $$
                BEGIN RAISE EXCEPTION 'PHASE1D_TEST_RECEIPT_FAILURE'; END;
                $$ LANGUAGE plpgsql;
                CREATE TRIGGER trg_phase1d_test_receipt_failure
                BEFORE INSERT ON app.advisory_source_observation_receipt
                FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_phase1d_receipt();
                UPDATE market.dataset_date_refresh_audit
                SET refreshed_at = refreshed_at + INTERVAL '1 second', job_id = %s
                WHERE dataset = 'daily_basic' AND data_source = 'tushare' AND trade_date = %s
                """,
                (str(uuid.uuid4()), source["trade_date"]),
            )
        failed = repository.observe_once(config=config, registry=SOURCE_QUERY_TEMPLATES)
        assert failed.failed == 1 and not failed.succeeded
        assert failed.scope_failures[0]["context"]["transaction_stage"] == "input_transaction"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_observation_receipt WHERE observer_config_hash = %s",
                (config_hash,),
            )
            assert int(cur.fetchone()[0]) == receipt_count
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_availability_event WHERE dataset_name = 'daily_basic' "
                "AND source_role = 'FEATURE_T' AND partition_key ->> 'trade_date' = %s",
                (source["trade_date"].isoformat(),),
            )
            assert int(cur.fetchone()[0]) == event_count
            cur.execute("DROP TRIGGER trg_phase1d_test_receipt_failure ON app.advisory_source_observation_receipt")
        recovered = repository.observe_once(config=config, registry=SOURCE_QUERY_TEMPLATES)
        assert recovered.succeeded and recovered.unchanged == 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_observation_receipt WHERE observer_config_hash = %s",
                (config_hash,),
            )
            assert int(cur.fetchone()[0]) == receipt_count + 1
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_availability_event WHERE dataset_name = 'daily_basic' "
                "AND source_role = 'FEATURE_T' AND partition_key ->> 'trade_date' = %s",
                (source["trade_date"].isoformat(),),
            )
            assert int(cur.fetchone()[0]) == event_count
    finally:
        conn.rollback()
        conn.close()


def test_two_workers_serialize_not_eligible_scope_and_migration_rolls_back_cleanly_dev_db() -> None:
    dsn = _dev_dsn()
    marker_date = date(2099, 12, 31)
    refreshed_at = datetime.now(UTC)
    setup = psycopg2.connect(**dsn)
    setup.autocommit = False
    migration_was_present = False
    try:
        with setup.cursor() as cur:
            cur.execute("SELECT to_regclass('app.advisory_source_observer_cursor')")
            migration_was_present = cur.fetchone()[0] is not None
            if migration_was_present:
                pytest.skip("DEV observer migration already exists; refusing destructive rollback validation")
            cur.execute(_MIGRATION.read_text(encoding="utf-8"))
            cur.execute(
                """
                INSERT INTO market.daily_basic (ts_code, trade_date)
                VALUES ('999998.SZ', %s);
                INSERT INTO market.trading_calendar (cal_date, is_trading)
                VALUES (%s, TRUE);
                INSERT INTO market.dataset_date_refresh_audit (
                    dataset, trade_date, data_source, job_id, status, row_count, refreshed_at,
                    error_message, metadata, quality_status, failure_category
                ) VALUES ('daily_basic', %s, 'tushare', %s, 'success', 1, %s,
                          'phase1d concurrency fixture', '{}'::jsonb, 'failed', 'dev_e2e')
                ON CONFLICT (dataset, trade_date, data_source) DO UPDATE SET
                    job_id = EXCLUDED.job_id, status = EXCLUDED.status, row_count = EXCLUDED.row_count,
                    refreshed_at = EXCLUDED.refreshed_at, error_message = EXCLUDED.error_message,
                    metadata = EXCLUDED.metadata, quality_status = EXCLUDED.quality_status,
                    failure_category = EXCLUDED.failure_category
                """,
                (marker_date, marker_date, marker_date, str(uuid.uuid4()), refreshed_at),
            )
        setup.commit()
        config = _single_dataset_config(effective_from=refreshed_at - timedelta(seconds=1))
        capacity_request = CapacityPlanningRequest(
            observer_config_hash=config.config_hash(SOURCE_QUERY_TEMPLATES),
            query_registry_hash=config.query_registry_hash(SOURCE_QUERY_TEMPLATES),
            as_of_ts=refreshed_at,
            history_start_trade_date=marker_date,
            history_end_trade_date=marker_date,
            program_count_by_style={"SHORT_REBOUND": 1, "LONG_TREND": 1},
            candidate_depth_by_program={"SHORT_REBOUND": 20, "LONG_TREND": 20},
            universe_size_p50=2_000,
            universe_size_p95=4_000,
            universe_size_max=6_000,
            horizons=(5, 10, 20, 60),
            projection_count=2,
            stage_projection_factor=5,
            revision_multiplier_p50=1.0,
            revision_multiplier_p95=1.2,
            revision_multiplier_max=2.0,
            retained_snapshot_count=10,
            concurrent_build_count=2,
            staging_copy_count=2,
            parquet_target_file_bytes=128 * 1024 * 1024,
            memory_budget_bytes=16 * 1024 * 1024 * 1024,
            worker_memory_overheads={
                "arrow_builder_bytes": 128 * 1024 * 1024,
                "hash_buffer_bytes": 64 * 1024 * 1024,
                "verifier_bytes": 128 * 1024 * 1024,
            },
            store_available_bytes=10 * 1024 * 1024 * 1024 * 1024,
            orphan_reserve_bytes=10 * 1024 * 1024 * 1024,
            concurrent_build_bytes=10 * 1024 * 1024 * 1024,
            manifest_overhead_bytes_per_snapshot=1 * 1024 * 1024,
            parquet_measurement_snapshot_limit=10,
            parquet_measurement_file_limit=10_000,
        )
        capacity = AdvisoryPhase1CapacityProbe(
            conn_factory=lambda: _dev_transaction_factory(dsn)
        ).probe(
            request=capacity_request,
            config=config,
            registry=SOURCE_QUERY_TEMPLATES,
        )
        assert capacity.status is CapacityStatus.PARTIAL
        assert capacity.missing_measurements == (
            "logical_row_width:universe_outcomes",
            "parquet_bytes_per_row_p95:universe_outcomes",
            "parquet_role_measurement:universe_outcomes",
        )
        assert capacity.parquet_measurement_summary["provenance"]["measurement_source"] == (
            "app.advisory_dataset_snapshot_file:SEALED"
        )

        def run_worker() -> dict[str, Any]:
            return PostgresSourceObserverRepository(
                conn_factory=lambda: _dev_transaction_factory(dsn)
            ).observe_once(
                config=config,
                registry=SOURCE_QUERY_TEMPLATES,
            ).as_dict()

        with ThreadPoolExecutor(max_workers=2) as pool:
            summaries = tuple(pool.map(lambda _index: run_worker(), range(2)))
        assert all(summary["succeeded"] for summary in summaries)
        assert sum(int(summary["not_eligible"]) for summary in summaries) == 1
        with setup.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_source_observation_receipt WHERE trade_date = %s",
                (marker_date,),
            )
            assert int(cur.fetchone()[0]) == 1
    finally:
        if not migration_was_present:
            try:
                with setup.cursor() as cur:
                    cur.execute(
                        "DELETE FROM market.dataset_date_refresh_audit "
                        "WHERE dataset = 'daily_basic' AND data_source = 'tushare' AND trade_date = %s",
                        (marker_date,),
                    )
                    cur.execute(
                        "DELETE FROM market.daily_basic WHERE ts_code = '999998.SZ' AND trade_date = %s",
                        (marker_date,),
                    )
                    cur.execute("DELETE FROM market.trading_calendar WHERE cal_date = %s", (marker_date,))
                    cur.execute(_ROLLBACK.read_text(encoding="utf-8"))
                setup.commit()
                with setup.cursor() as cur:
                    cur.execute(
                        "SELECT to_regclass('app.advisory_source_observer_cursor'), "
                        "to_regclass('app.advisory_source_observation_receipt')"
                    )
                    assert cur.fetchone() == (None, None)
                    cur.execute(
                        "SELECT count(*) FROM market.dataset_date_refresh_audit "
                        "WHERE dataset = 'daily_basic' AND data_source = 'tushare' AND trade_date = %s",
                        (marker_date,),
                    )
                    assert int(cur.fetchone()[0]) == 0
            except Exception:
                setup.rollback()
                raise
        setup.close()
