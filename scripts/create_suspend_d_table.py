"""Ensure the Tushare suspend_d table and data dashboard registration exist."""

from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv


def main() -> None:
    load_dotenv(override=True)
    cfg = dict(
        host=os.getenv("TDX_DB_HOST", "localhost"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True

    try:
        with conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS market;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS market.suspend_d (
                    trade_date     DATE NOT NULL,
                    ts_code        TEXT NOT NULL,
                    suspend_type   TEXT NOT NULL,
                    suspend_timing TEXT,
                    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (trade_date, ts_code, suspend_type),
                    CONSTRAINT suspend_d_type_chk CHECK (suspend_type IN ('S', 'R'))
                );
                """
            )

            cur.execute(
                "COMMENT ON TABLE market.suspend_d IS %s;",
                ("Tushare suspend_d daily stock suspension and resumption information",),
            )
            comments = {
                "trade_date": "Tushare suspend/resume date",
                "ts_code": "Tushare stock code, for example 000001.SZ",
                "suspend_type": "Suspension type: S=suspended, R=resumed",
                "suspend_timing": "Intraday suspension time range when provided",
                "created_at": "Local ingestion timestamp",
            }
            for col, desc in comments.items():
                cur.execute(f"COMMENT ON COLUMN market.suspend_d.{col} IS %s;", (desc,))

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_suspend_d_ts_code_trade_date
                    ON market.suspend_d (ts_code, trade_date);
                """
            )
            cur.execute(
                "SELECT create_hypertable('market.suspend_d','trade_date', if_not_exists => TRUE);"
            )

            cur.execute(
                """
                INSERT INTO market.data_stats_config
                    (data_kind, table_name, date_column, enabled, extra_info)
                VALUES (
                    'suspend_d',
                    'market.suspend_d',
                    'trade_date',
                    TRUE,
                    jsonb_build_object(
                        'desc',
                        'Tushare suspend_d daily stock suspension and resumption information'
                    )
                )
                ON CONFLICT (data_kind) DO UPDATE
                    SET table_name = EXCLUDED.table_name,
                        date_column = EXCLUDED.date_column,
                        enabled = EXCLUDED.enabled,
                        extra_info = EXCLUDED.extra_info;
                """
            )
    finally:
        conn.close()

    print("market.suspend_d table and data_stats_config for suspend_d ensured.")


if __name__ == "__main__":
    main()
