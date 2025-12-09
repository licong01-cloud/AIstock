from __future__ import annotations

import os
import datetime as dt
from typing import List

import psycopg2
import psycopg2.extras as pgx

pgx.register_uuid()

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

TARGET_DATE = dt.date(2025, 11, 28)
TS_CODES: List[str] = ["000001.SZ", "000002.SZ"]


def main() -> None:
    print("[INFO] connecting DB with:", DB_CFG)
    print(f"[INFO] checking minute data on {TARGET_DATE} for codes: {TS_CODES}")

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            for ts_code in TS_CODES:
                print(f"\n[CHECK] ts_code={ts_code}")
                cur.execute(
                    """
                    SELECT
                        MIN(trade_time) AS first_ts,
                        MAX(trade_time) AS last_ts,
                        COUNT(*)        AS cnt
                    FROM market.kline_minute_raw
                    WHERE ts_code = %s
                      AND trade_time::date = %s
                    """,
                    (ts_code, TARGET_DATE),
                )
                row = cur.fetchone() or {}
                cnt = row.get("cnt", 0) or 0
                print(f"  rows on {TARGET_DATE}: {cnt}")
                if cnt == 0:
                    continue

                print("  first_ts:", row.get("first_ts"))
                print("  last_ts :", row.get("last_ts"))

                # 打印前几条样例数据
                cur.execute(
                    """
                    SELECT trade_time, close_li, volume_hand
                    FROM market.kline_minute_raw
                    WHERE ts_code = %s
                      AND trade_time::date = %s
                    ORDER BY trade_time
                    LIMIT 5
                    """,
                    (ts_code, TARGET_DATE),
                )
                samples = cur.fetchall() or []
                print("  sample rows (up to 5):")
                for r in samples:
                    print(
                        "    ",
                        r["trade_time"],
                        "close=", r.get("close_li"),
                        "vol=", r.get("volume_hand"),
                    )


if __name__ == "__main__":
    main()
