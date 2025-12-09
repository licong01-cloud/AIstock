from __future__ import annotations

import json
import os
from typing import Any, Dict

import psycopg2
import psycopg2.extras as pgx

pgx.register_uuid()

DB_CFG: Dict[str, Any] = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

TARGET_JOB_ID = os.getenv("TARGET_JOB_ID", "5d9ddbee-9938-4c8f-a425-a8ca74d26f59")


def main() -> None:
    print("[INFO] DB_CFG:", DB_CFG)
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            # 1) 查看 kline_minute_raw 的主键定义
            print("\n[STEP 1] kline_minute_raw primary key definition")
            cur.execute(
                """
                SELECT tc.constraint_name,
                       kcu.column_name,
                       tc.table_name
                  FROM information_schema.table_constraints AS tc
                  JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                 WHERE tc.table_schema = 'market'
                   AND tc.table_name = 'kline_minute_raw'
                   AND tc.constraint_type = 'PRIMARY KEY'
                 ORDER BY kcu.ordinal_position
                """
            )
            rows = cur.fetchall() or []
            for r in rows:
                print("  ", r)

            # 2) 查看最近一次 kline_minute_raw 增量任务的 job 记录
            print("\n[STEP 2] latest kline_minute_raw incremental job (by created_at)")
            cur.execute(
                """
                SELECT job_id, status, summary, started_at, finished_at, created_at
                  FROM market.ingestion_jobs
                 WHERE (summary->>'data_kind') = 'kline_minute_raw'
                    OR (summary->>'dataset') = 'kline_minute_raw'
                 ORDER BY created_at DESC
                 LIMIT 3
                """
            )
            jobs = cur.fetchall() or []
            for j in jobs:
                summ = j.get("summary")
                if isinstance(summ, str):
                    try:
                        summ = json.loads(summ)
                    except Exception:
                        pass
                print("  job_id=", j["job_id"], "status=", j["status"], "created_at=", j["created_at"]) 
                print("    summary=", summ)

            # 3) 针对目标 job_id 单独查看一遍（便于和日志对齐）
            print("\n[STEP 3] target job detail (if exists)")
            try:
                cur.execute(
                    """
                    SELECT job_id, status, summary, started_at, finished_at, created_at
                      FROM market.ingestion_jobs
                     WHERE job_id = %s::uuid
                    """,
                    (TARGET_JOB_ID,),
                )
                one = cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                print("  [WARN] query target job failed:", exc)
                one = None
            if one:
                summ = one.get("summary")
                if isinstance(summ, str):
                    try:
                        summ = json.loads(summ)
                    except Exception:
                        pass
                print("  job_id=", one["job_id"], "status=", one["status"], "created_at=", one["created_at"]) 
                print("    summary=", summ)
            else:
                print("  [INFO] target job not found in ingestion_jobs")

            # 4) 统计 2025-11-28 及之后分钟线 trade_time 对应的日期分布
            print("\n[STEP 4] trade_time date distribution for 2025-11-28 and later")
            cur.execute(
                """
                SELECT trade_time::date AS d, COUNT(*) AS cnt
                  FROM market.kline_minute_raw
                 WHERE trade_time::date >= DATE '2025-11-28'
                 GROUP BY trade_time::date
                 ORDER BY trade_time::date
                """
            )
            dist = cur.fetchall() or []
            if not dist:
                print("  [INFO] no minute rows on or after 2025-11-28")
            else:
                for r in dist:
                    print(f"  date={r['d']} cnt={r['cnt']}")

            # 5) 抽样查看 2025-11-28 当天的部分记录，确认实际入库的日期/时间
            print("\n[STEP 5] sample rows for 2025-11-28 (first 20)")
            cur.execute(
                """
                SELECT trade_time, ts_code, freq, open_li, high_li, low_li, close_li
                  FROM market.kline_minute_raw
                 WHERE trade_time::date = DATE '2025-11-28'
                 ORDER BY ts_code, trade_time
                 LIMIT 20
                """
            )
            sample = cur.fetchall() or []
            for r in sample:
                print(
                    f"  ts_code={r['ts_code']} trade_time={r['trade_time']} freq={r['freq']} "
                    f"O={r['open_li']} H={r['high_li']} L={r['low_li']} C={r['close_li']}"
                )


if __name__ == "__main__":
    main()
