"""Register kline_daily_raw and kline_minute_raw in market.data_stats_config.

These tables were managed by the Go backend and were never registered in
data_stats_config, which means the data dashboard's gap-check and auto-range
features could not see them.

Safe to run multiple times — uses ON CONFLICT DO UPDATE.
"""
from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

ENTRIES = [
    {
        "data_kind": "kline_daily_raw",
        "table_name": "market.kline_daily_raw",
        "date_column": "trade_date",
        "desc": "TDX 日K线原始数据",
    },
    {
        "data_kind": "kline_minute_raw",
        "table_name": "market.kline_minute_raw",
        "date_column": "trade_time",
        "desc": "TDX 分钟线原始数据",
    },
]


def main() -> None:
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()

    for entry in ENTRIES:
        cur.execute(
            """
            INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
            VALUES (%s, %s, %s, TRUE, jsonb_build_object('desc', %s, 'is_timeseries', TRUE))
            ON CONFLICT (data_kind) DO UPDATE
                SET table_name = EXCLUDED.table_name,
                    date_column = EXCLUDED.date_column,
                    enabled = EXCLUDED.enabled,
                    extra_info = EXCLUDED.extra_info
            """,
            (entry["data_kind"], entry["table_name"], entry["date_column"], entry["desc"]),
        )
        print(f"[OK] registered {entry['data_kind']} -> {entry['table_name']}")

    conn.commit()
    cur.close()
    conn.close()
    print("[DONE]")


if __name__ == "__main__":
    main()
