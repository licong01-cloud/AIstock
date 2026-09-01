from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import psycopg2
import pytest
from dotenv import load_dotenv

from backend.services.advisory_forward.errors import AdvisoryForwardModelEvaluationError
from backend.services.advisory_forward.evaluation import AdvisoryForwardEvaluationMarketSource
from backend.services.advisory_forward.evaluation import REASON_MARKET_UNAVAILABLE
from backend.services.advisory_forward.repository import AdvisoryForwardPGRepository


pytestmark = pytest.mark.skipif(
    os.getenv("AISTOCK_RUN_ADVISORY_FORWARD_EVALUATION_DEV_DB") != "1",
    reason="explicit DEV-only forward evaluation migration gate is required",
)


ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "backend/db/migrations/add_advisory_forward_model_evaluation_20260823.sql"
ROLLBACK = ROOT / "backend/db/migrations/add_advisory_forward_model_evaluation_20260823.rollback.sql"


def _configs() -> tuple[dict[str, object], dict[str, object]]:
    env_path = Path(os.getenv("AISTOCK_ENV_FILE") or (ROOT / ".env"))
    load_dotenv(env_path, override=False)

    def values(prefix: str) -> dict[str, object]:
        required = ("HOST", "PORT", "NAME", "USER", "PASSWORD")
        missing = [f"{prefix}{name}" for name in required if os.getenv(f"{prefix}{name}") is None]
        if missing:
            pytest.skip(f"explicit database configuration is incomplete: {missing}")
        return {
            "host": str(os.environ[f"{prefix}HOST"]),
            "port": int(os.environ[f"{prefix}PORT"]),
            "dbname": str(os.environ[f"{prefix}NAME"]),
            "user": str(os.environ[f"{prefix}USER"]),
            "password": str(os.environ[f"{prefix}PASSWORD"]),
        }

    return values("TDX_DB_DEV_"), values("TDX_DB_")


def _execute(connection, path: Path) -> None:
    with connection.cursor() as cursor:
        cursor.execute(path.read_text(encoding="utf-8"))


@contextmanager
def _connection_factory(config: dict[str, object]):
    connection = psycopg2.connect(**config, connect_timeout=10)
    try:
        yield connection
    finally:
        connection.close()


def _readback(connection) -> tuple[int, int, int, int, int]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              to_regclass('app.advisory_forward_model_evaluation') IS NOT NULL,
              to_regclass('app.advisory_forward_model_observation_outcome') IS NOT NULL
            """
        )
        tables = cursor.fetchone()
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema='app' AND table_name='advisory_forward_model_observation'
              AND column_name IN ('evaluation_status','evaluation_reason_code','evaluation_error_json','evaluated_at')
            """
        )
        columns = int(cursor.fetchone()[0])
        cursor.execute(
            """
            SELECT COUNT(*) FROM pg_indexes
            WHERE schemaname='app' AND indexname IN (
              'ux_advisory_forward_model_evaluation_epoch_asof',
              'idx_advisory_forward_model_evaluation_program_latest',
              'idx_advisory_forward_model_outcome_program_maturity'
            )
            """
        )
        indexes = int(cursor.fetchone()[0])
        cursor.execute(
            """
            SELECT COUNT(*) FROM pg_trigger
            WHERE NOT tgisinternal AND tgname IN (
              'trg_reject_advisory_forward_model_evaluation_mutation',
              'trg_reject_advisory_forward_model_outcome_mutation'
            )
            """
        )
        triggers = int(cursor.fetchone()[0])
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_attribute attribute
            JOIN pg_class relation ON relation.oid=attribute.attrelid
            JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
            JOIN pg_description description
              ON description.objoid=relation.oid AND description.objsubid=attribute.attnum
            WHERE namespace.nspname='app' AND attribute.attnum > 0
              AND (
                relation.relname IN (
                  'advisory_forward_model_evaluation',
                  'advisory_forward_model_observation_outcome'
                )
                OR (
                  relation.relname='advisory_forward_model_observation'
                  AND attribute.attname IN (
                    'evaluation_status','evaluation_reason_code',
                    'evaluation_error_json','evaluated_at'
                  )
                )
              )
            """
        )
        comments = int(cursor.fetchone()[0])
    return int(bool(tables[0])) + int(bool(tables[1])), columns, indexes, triggers, comments


def test_forward_model_evaluation_migration_apply_retry_rollback_and_reapply_on_dev() -> None:
    dev, production = _configs()
    dev_identity = (dev["host"], dev["port"], dev["dbname"])
    production_identity = (production["host"], production["port"], production["dbname"])
    assert dev_identity != production_identity
    assert "dev" in str(dev["dbname"]).lower()

    connection = psycopg2.connect(**dev, connect_timeout=10)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            database_name = cursor.fetchone()[0]
        assert database_name == dev["dbname"]
        assert int(connection.get_dsn_parameters()["port"]) == int(dev["port"])

        _execute(connection, MIGRATION)
        assert _readback(connection) == (2, 4, 3, 2, 43)
        _execute(connection, MIGRATION)
        assert _readback(connection) == (2, 4, 3, 2, 43)
        _execute(connection, ROLLBACK)
        assert _readback(connection) == (0, 0, 0, 0, 0)
        _execute(connection, MIGRATION)
        assert _readback(connection) == (2, 4, 3, 2, 43)
        repository = AdvisoryForwardPGRepository(conn_factory=lambda: _connection_factory(dev))
        repository.pending_mature_model_observations(on_or_before=date.today(), limit=2)
    finally:
        connection.close()


def test_forward_model_evaluation_market_source_honors_current_dev_limit_evidence() -> None:
    dev, production = _configs()
    assert (dev["host"], dev["port"], dev["dbname"]) != (
        production["host"], production["port"], production["dbname"]
    )
    assert "dev" in str(dev["dbname"]).lower()
    discovery = psycopg2.connect(**dev, connect_timeout=10)
    try:
        with discovery.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*), MAX(l.trade_date)
                FROM market.stk_limit l
                WHERE l.pre_close IS NOT NULL AND l.up_limit IS NOT NULL AND l.down_limit IS NOT NULL
                """
            )
            complete_limit_row_count, end_date = cursor.fetchone()
            if end_date is None:
                cursor.execute(
                    """
                    SELECT MAX(i.trade_date)
                    FROM market.index_daily i
                    WHERE TRIM(i.ts_code)='000300.SH'
                      AND EXISTS (SELECT 1 FROM market.kline_daily_raw k WHERE k.trade_date=i.trade_date)
                    """
                )
                end_date = cursor.fetchone()[0]
            assert end_date is not None
            cursor.execute(
                """
                SELECT cal_date FROM market.trading_calendar
                WHERE is_trading=TRUE AND cal_date <= %s
                ORDER BY cal_date DESC LIMIT 3
                """,
                (end_date,),
            )
            dates = sorted(row[0] for row in cursor.fetchall())
            assert len(dates) == 3
            if complete_limit_row_count:
                cursor.execute(
                    """
                    SELECT TRIM(k.ts_code)
                    FROM market.kline_daily_raw k
                    JOIN market.adj_factor a ON a.ts_code=k.ts_code AND a.trade_date=k.trade_date
                    JOIN market.stk_limit l ON l.ts_code=k.ts_code AND l.trade_date=k.trade_date
                    WHERE k.trade_date = ANY(%s)
                      AND l.pre_close IS NOT NULL AND l.up_limit IS NOT NULL AND l.down_limit IS NOT NULL
                    GROUP BY TRIM(k.ts_code)
                    HAVING COUNT(DISTINCT k.trade_date)=%s
                    ORDER BY TRIM(k.ts_code) LIMIT 5
                    """,
                    (dates, len(dates)),
                )
            else:
                cursor.execute(
                    """
                    SELECT TRIM(k.ts_code)
                    FROM market.kline_daily_raw k
                    JOIN market.adj_factor a ON a.ts_code=k.ts_code AND a.trade_date=k.trade_date
                    WHERE k.trade_date = ANY(%s)
                    GROUP BY TRIM(k.ts_code)
                    HAVING COUNT(DISTINCT k.trade_date)=%s
                    ORDER BY TRIM(k.ts_code) LIMIT 5
                    """,
                    (dates, len(dates)),
                )
            symbols = [row[0] for row in cursor.fetchall()]
            assert symbols
    finally:
        discovery.close()

    source = AdvisoryForwardEvaluationMarketSource(conn_factory=lambda: _connection_factory(dev))
    if not complete_limit_row_count:
        with pytest.raises(AdvisoryForwardModelEvaluationError) as excinfo:
            source.load(
                symbols=symbols,
                benchmark_instrument="000300.SH",
                start_trade_date=dates[0],
                end_trade_date=dates[-1],
            )
        assert excinfo.value.reason_code == REASON_MARKET_UNAVAILABLE
        return

    result = source.load(
        symbols=symbols,
        benchmark_instrument="000300.SH",
        start_trade_date=dates[0],
        end_trade_date=dates[-1],
    )

    assert len(result.trading_calendar) == 4
    assert result.trading_calendar[1].date() == dates[0]
    assert result.trading_calendar[-1].date() == dates[-1]
    assert result.daily.reset_index()["datetime"].max().date() == dates[-1]
    assert result.benchmark_daily.reset_index()["datetime"].max().date() == dates[-1]
    assert len(result.input_sha256) == 64
