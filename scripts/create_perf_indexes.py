"""Create BRIN indexes for performance-critical large tables.

- market.adj_factor: NOT a hypertable, PK (ts_code, trade_date) has NO time-leading
  index. BRIN on trade_date speeds up MAX(trade_date) and date-range queries
  that are issued by every freshness check.
- market.kline_minute_raw: hypertable with PK (ts_code, trade_time, freq).
  Chunk exclusion prunes to ~1 chunk but within-chunk scans still touch ~8M rows.
  BRIN on trade_time reduces page scans for COUNT and EXISTS queries.

Uses CREATE INDEX CONCURRENTLY to avoid locking tables during creation.
Safe to run multiple times — uses IF NOT EXISTS.
"""
from __future__ import annotations

import os
import sys
import time

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

INDEXES = [
    {
        "name": "idx_adj_factor_trade_date_brin",
        "sql": (
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_adj_factor_trade_date_brin "
            "ON market.adj_factor USING BRIN (trade_date) WITH (pages_per_range = 32)"
        ),
        "desc": "adj_factor BRIN on trade_date",
    },
    {
        "name": "idx_kline_minute_brin_time",
        "sql": (
            "CREATE INDEX IF NOT EXISTS idx_kline_minute_brin_time "
            "ON market.kline_minute_raw USING BRIN (trade_time) WITH (pages_per_range = 32)"
        ),
        "desc": "kline_minute_raw BRIN on trade_time",
        "concurrently": False,
    },
]


def main() -> None:
    conn = psycopg2.connect(**DB_CFG)
    conn.autocommit = True  # CONCURRENTLY requires autocommit
    cur = conn.cursor()

    for idx in INDEXES:
        print(f"[CREATE] {idx['desc']} ...")
        t0 = time.time()
        try:
            cur.execute(idx["sql"])
            elapsed = time.time() - t0
            print(f"[OK]   {idx['desc']} ({elapsed:.1f}s)")
        except Exception as exc:
            elapsed = time.time() - t0
            print(f"[FAIL] {idx['desc']} after {elapsed:.1f}s: {exc}")

    cur.close()
    conn.close()
    print("[DONE]")


if __name__ == "__main__":
    main()
