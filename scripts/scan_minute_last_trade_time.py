from __future__ import annotations

import os
from collections import Counter
from typing import Dict, List, Tuple

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


SQL_PER_SYMBOL = """
SELECT
  ts_code,
  MIN(trade_time) AS first_ts,
  MAX(trade_time) AS last_ts,
  COUNT(*)        AS cnt
FROM market.kline_minute_raw
GROUP BY ts_code
ORDER BY ts_code;
"""


def main() -> None:
    print("[INFO] connecting DB with:", DB_CFG)
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            print("[INFO] querying per-symbol minute stats ...")
            cur.execute(SQL_PER_SYMBOL)
            rows: List[Dict] = cur.fetchall() or []

    if not rows:
        print("[WARN] market.kline_minute_raw is empty (no rows)")
        return

    print(f"[INFO] got {len(rows)} symbols with minute data")

    # 1) 打印前几只股票的 first/last 时间，做抽样检查
    print("\n[HEAD] sample of first 10 symbols:")
    for row in rows[:10]:
        print(
            f"  {row['ts_code']:<10} first={row['first_ts']} last={row['last_ts']} cnt={row['cnt']}"
        )

    # 2) 按 last_ts 的日期部分聚合，看每个日期有多少只股票的最后一条分钟线停在这天
    counter: Counter[str] = Counter()
    for row in rows:
        last_ts = row["last_ts"]
        if last_ts is None:
            continue
        # last_ts 是 timestamp，取日期部分
        date_key = last_ts.date().isoformat()
        counter[date_key] += 1

    print("\n[SUMMARY] last trade_time date distribution (date -> symbol_count):")
    # 按日期排序输出
    for date_key, cnt in sorted(counter.items()):
        print(f"  {date_key}: {cnt} symbols")

    # 3) 找出占比最高的日期（可能就是“全市场统一卡住”的那天）
    date_counts: List[Tuple[str, int]] = sorted(counter.items(), key=lambda x: x[1], reverse=True)
    if date_counts:
        top_date, top_cnt = date_counts[0]
        total_syms = len(rows)
        ratio = top_cnt * 100.0 / total_syms
        print(
            f"\n[TOP] most common last date: {top_date} -> {top_cnt} / {total_syms} symbols ({ratio:.2f}% of all)"
        )

    # 4) 统计整张表的全局最早分钟线时间/日期（不按股票分组）
    with psycopg2.connect(**DB_CFG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(trade_time), MIN(trade_time)::date FROM market.kline_minute_raw")
            first_ts, first_date = cur.fetchone()
            print("\n[GLOBAL] first_trade_time:", first_ts)
            print("[GLOBAL] first_trade_date:", first_date)


if __name__ == "__main__":
    main()
