from __future__ import annotations

import datetime as dt
import os
import time
import uuid
from typing import Any, Dict, List

import psycopg2
import psycopg2.extras as pgx
import requests

pgx.register_uuid()

TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
DB_CFG: Dict[str, Any] = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

TEST_CODE = os.getenv("TEST_TS_CODE", "000001.SZ")


def tdx_post(path: str, json_body: Dict[str, Any]) -> Any:
    url = TDX_API_BASE + path
    resp = requests.post(url, json=json_body, timeout=30)
    resp.raise_for_status()
    if "application/json" in resp.headers.get("Content-Type", ""):
        return resp.json()
    return resp.text


def poll_task(task_id: str, interval: float = 5.0, max_wait: float = 300.0) -> Dict[str, Any]:
    """轮询 Go /api/tasks/{task_id} 状态。"""
    url = f"{TDX_API_BASE}/api/tasks/{task_id}"
    waited = 0.0
    last_status = None
    while waited < max_wait:
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] poll task failed: {exc}")
            time.sleep(interval)
            waited += interval
            continue
        status = (data.get("status") or "").lower()
        if status != last_status:
            print(f"[TASK] status={status}, detail={data}")
            last_status = status
        if status in {"success", "failed", "cancelled", "canceled"}:
            return data
        time.sleep(interval)
        waited += interval
    print(f"[TIMEOUT] task {task_id} not finished within {max_wait} seconds")
    return {}


def inspect_db_for_code(ts_code: str) -> None:
    """查看某个 ts_code 在 2025-11-20 之后的分钟线日期分布。"""
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT trade_time::date AS d, COUNT(*) AS cnt
                  FROM market.kline_minute_raw
                 WHERE ts_code = %s
                   AND trade_time::date >= DATE '2025-11-20'
                 GROUP BY trade_time::date
                 ORDER BY trade_time::date
                """,
                (ts_code,),
            )
            rows: List[Dict[str, Any]] = cur.fetchall() or []
    if not rows:
        print(f"[DB] ts_code={ts_code}: no rows on or after 2025-11-20")
    else:
        print(f"[DB] ts_code={ts_code}: trade_time date distribution since 2025-11-20:")
        for r in rows:
            print(f"  date={r['d']} cnt={r['cnt']}")


def run_one_format(start_time_value: str) -> None:
    job_id = str(uuid.uuid4())
    print("=" * 80)
    print(f"[CASE] start_time={start_time_value!r}, job_id={job_id}")
    payload: Dict[str, Any] = {
        "job_id": job_id,
        "codes": [TEST_CODE],
        "start_time": start_time_value,
        "workers": 1,
        "options": {
            "truncate_before": False,
            "max_rows_per_chunk": 100000,
            "source": "tdx_api",
        },
    }
    print(f"[REQ] POST /api/tasks/ingest-minute-raw-init -> {payload}")
    try:
        data = tdx_post("/api/tasks/ingest-minute-raw-init", payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to start task: {exc}")
        return
    task_id = data.get("task_id") or data.get("taskId")
    print(f"[RESP] task_id={task_id}")
    if not task_id:
        return
    result = poll_task(task_id)
    print(f"[TASK RESULT] {result}")
    inspect_db_for_code(TEST_CODE)


def main() -> None:
    print(f"[INFO] TDX_API_BASE={TDX_API_BASE}")
    print(f"[INFO] DB_CFG={DB_CFG}")
    print(f"[INFO] TEST_CODE={TEST_CODE}")

    # 格式一：RFC3339，带东八区
    start_time_rfc3339 = "2025-11-28T00:00:00+08:00"
    # 格式二：纯日期 YYYY-MM-DD（由 Go 使用 time.Local 解析）
    start_time_date_only = "2025-11-28"

    run_one_format(start_time_rfc3339)
    run_one_format(start_time_date_only)


if __name__ == "__main__":
    main()
