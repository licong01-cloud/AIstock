"""Ingest Tushare stock_basic (latest stock list) into PostgreSQL.

- 仅支持 init（全量），不做增量游标。
- 支持 --truncate 先清空表。
- 仅在出错时写 ingestion_logs，正常不落库日志。
- 进度与 moneyflow/adj_factor 一致：基于单任务 total/done 更新 summary.progress。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import uuid
from typing import Any, Dict, List

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv


load_dotenv(override=True)
pgx.register_uuid()


DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-ingest-stock-basic",
)


def _load_tushare():
    import importlib

    return importlib.import_module("tushare")


def pro_api():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts = _load_tushare()
    return ts.pro_api(token)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Tushare stock_basic (latest stock list)")
    parser.add_argument("--job-id", type=str, default=None, help="Existing job id to attach and update")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate market.stock_basic before ingestion (destructive)",
    )
    parser.add_argument(
        "--bulk-session-tune",
        action="store_true",
        help="Apply session-level tuning for bulk load (SET synchronous_commit=off, work_mem=256MB)",
    )
    return parser.parse_args()


def _ensure_session_tune(conn, enabled: bool) -> None:
    if not enabled:
        return
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = off")
        cur.execute("SET work_mem = '256MB'")


def _create_job(conn, job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
    job_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_jobs (job_id, job_type, status, created_at, started_at, summary)
            VALUES (%s, %s, 'running', NOW(), NOW(), %s)
            """,
            (job_id, job_type, json.dumps(summary, ensure_ascii=False)),
        )
    return job_id


def _start_existing_job(conn, job_id: uuid.UUID, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status='running', started_at=COALESCE(started_at, NOW()), summary=%s
             WHERE job_id=%s
            """,
            (json.dumps(summary, ensure_ascii=False), job_id),
        )


def _finish_job(conn, job_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT summary FROM market.ingestion_jobs WHERE job_id=%s", (job_id,))
        row = cur.fetchone()
        base: Dict[str, Any] = {}
        if row and row[0]:
            try:
                base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
            except Exception:  # noqa: BLE001
                base = {}
        base.update(summary or {})
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status=%s, finished_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (status, json.dumps(base, ensure_ascii=False), job_id),
        )


def _log(conn, job_id: uuid.UUID, level: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES (%s, NOW(), %s, %s)",
            (job_id, level.upper(), message),
        )


def _update_job_progress(conn, job_id: uuid.UUID, total: int, done: int, inserted_rows: int) -> None:
    total_safe = max(total, 0)
    done_safe = min(max(done, 0), total_safe if total_safe > 0 else done)
    progress = 0.0 if total_safe == 0 else max(0.0, min(100.0, 100.0 * float(done_safe) / float(total_safe)))
    counters = {
        "total": total_safe,
        "done": done_safe,
        "running": 0,
        "pending": max(total_safe - done_safe, 0),
        "failed": 0,
        "success": done_safe,
        "inserted_rows": inserted_rows,
    }
    payload = {"counters": counters, "progress": progress, "total_days": total_safe, "done_days": done_safe}
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id = %s
            """,
            (json.dumps(payload, ensure_ascii=False), job_id),
        )


def _fetch_stock_basic(pro) -> List[Dict[str, Any]]:
    # All fields documented by Tushare doc_id=25; None or missing values preserved as-is.
    df = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs",
    )
    rows: List[Dict[str, Any]] = []
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            rows.append(
                {
                    "ts_code": row.get("ts_code"),
                    "symbol": row.get("symbol"),
                    "name": row.get("name"),
                    "area": row.get("area"),
                    "industry": row.get("industry"),
                    "fullname": row.get("fullname"),
                    "enname": row.get("enname"),
                    "market": row.get("market"),
                    "exchange": row.get("exchange"),
                    "curr_type": row.get("curr_type"),
                    "list_status": row.get("list_status"),
                    "list_date": row.get("list_date"),
                    "delist_date": row.get("delist_date"),
                    "is_hs": row.get("is_hs"),
                }
            )
    return rows


def _insert_stock_basic(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.stock_basic (ts_code, symbol, name, area, industry, fullname, enname, market, exchange, curr_type, list_status, list_date, delist_date, is_hs) "
        "VALUES %s ON CONFLICT (ts_code) DO UPDATE SET "
        "symbol=EXCLUDED.symbol, name=EXCLUDED.name, area=EXCLUDED.area, industry=EXCLUDED.industry, "
        "fullname=EXCLUDED.fullname, enname=EXCLUDED.enname, market=EXCLUDED.market, exchange=EXCLUDED.exchange, "
        "curr_type=EXCLUDED.curr_type, list_status=EXCLUDED.list_status, list_date=EXCLUDED.list_date, "
        "delist_date=EXCLUDED.delist_date, is_hs=EXCLUDED.is_hs"
    )
    values = []
    for r in rows:
        ts_code = (r.get("ts_code") or "").strip()
        if not ts_code:
            continue
        values.append(
            (
                ts_code,
                r.get("symbol"),
                r.get("name"),
                r.get("area"),
                r.get("industry"),
                r.get("fullname"),
                r.get("enname"),
                r.get("market"),
                r.get("exchange"),
                r.get("curr_type"),
                r.get("list_status"),
                _parse_ymd(r.get("list_date")),
                _parse_ymd(r.get("delist_date")),
                r.get("is_hs"),
            )
        )
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def _parse_ymd(val) -> dt.date | None:
    if not val:
        return None
    try:
        s = str(val)
        if len(s) == 8:
            return dt.date(int(s[:4]), int(s[4:6]), int(s[6:]))
        return dt.date.fromisoformat(s)
    except Exception:
        return None


def main() -> None:
    args = parse_args()
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        _ensure_session_tune(conn, getattr(args, "bulk_session_tune", False))
        pro = pro_api()

        if args.truncate:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE market.stock_basic")
            print("[WARN] TRUNCATE market.stock_basic executed before ingestion")

        job_summary = {"dataset": "stock_basic", "mode": "init"}
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            _start_existing_job(conn, job_id, job_summary)
        else:
            job_id = _create_job(conn, "init", job_summary)

        _log(conn, job_id, "info", "start tushare stock_basic ingestion")

        try:
            rows = _fetch_stock_basic(pro)
            inserted = _insert_stock_basic(conn, rows)
            _update_job_progress(conn, job_id, total=1, done=1, inserted_rows=inserted)
            _finish_job(conn, job_id, "success", {"stats": {"inserted_rows": inserted}})
            print(f"[DONE] stock_basic inserted_rows={inserted}")
        except Exception as exc:  # noqa: BLE001
            _log(conn, job_id, "error", f"stock_basic failed: {exc}")
            _finish_job(conn, job_id, "failed", {"error": str(exc)})
            print(f"[ERROR] stock_basic failed: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    main()
