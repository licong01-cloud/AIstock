"""Ensure the Tushare st event table and dashboard registration exist."""

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
                CREATE TABLE IF NOT EXISTS market.stock_st_events (
                    ts_code     TEXT NOT NULL,
                    name        TEXT,
                    pub_date    DATE NOT NULL,
                    imp_date    DATE NOT NULL,
                    st_type     TEXT NOT NULL,
                    st_reason   TEXT,
                    st_explain  TEXT,
                    source_api  TEXT NOT NULL DEFAULT 'tushare.st',
                    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (ts_code, pub_date, imp_date, st_type)
                );
                """
            )

            cur.execute(
                "COMMENT ON TABLE market.stock_st_events IS %s;",
                ("Tushare st ST risk-warning event records for PIT universe construction",),
            )
            comments = {
                "ts_code": "Tushare stock code, for example 000001.SZ",
                "name": "Stock name reported by Tushare st at the event",
                "pub_date": "Announcement publication date from Tushare st; date only, no exact publish time",
                "imp_date": "Implementation date from Tushare st when the ST status change takes effect",
                "st_type": "ST change type mapped from Tushare st_tpye, preserving upstream spelling via DatasetSpec field map",
                "st_reason": "Structured ST change reason from Tushare st",
                "st_explain": "Detailed ST change explanation from Tushare st",
                "source_api": "Data source API name, fixed to tushare.st for this table",
                "ingested_at": "Local ingestion timestamp",
            }
            for col, desc in comments.items():
                cur.execute(f"COMMENT ON COLUMN market.stock_st_events.{col} IS %s;", (desc,))

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_stock_st_events_ts_code_pub_date
                    ON market.stock_st_events (ts_code, pub_date);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_stock_st_events_imp_date
                    ON market.stock_st_events (imp_date);
                """
            )
            cur.execute(
                "SELECT create_hypertable('market.stock_st_events','pub_date', if_not_exists => TRUE);"
            )

            cur.execute(
                """
                INSERT INTO market.data_stats_config
                    (data_kind, table_name, date_column, updated_column, enabled, extra_info)
                VALUES (
                    'stock_st_events',
                    'market.stock_st_events',
                    'pub_date',
                    'ingested_at',
                    TRUE,
                    jsonb_build_object(
                        'desc',
                        'Tushare st ST risk-warning event records for PIT universe construction',
                        'source_api',
                        'tushare.st',
                        'date_semantics',
                        'pub_date is announcement publication date without exact publish time',
                        'date_sequence',
                        'calendar',
                        'cursor_source',
                        'refresh_audit'
                    )
                )
                ON CONFLICT (data_kind) DO UPDATE
                    SET table_name = EXCLUDED.table_name,
                        date_column = EXCLUDED.date_column,
                        updated_column = EXCLUDED.updated_column,
                        enabled = EXCLUDED.enabled,
                        extra_info = EXCLUDED.extra_info;
                """
            )
    finally:
        conn.close()

    print("market.stock_st_events table and data_stats_config ensured.")


if __name__ == "__main__":
    main()
