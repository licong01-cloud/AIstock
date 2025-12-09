"""Ingest Tushare moneyflow (per-documentation moneyflow) into TimescaleDB.

- init 模式：按交易日区间循环，调用 pro.moneyflow(trade_date=YYYYMMDD)，一次获取该日全部股票的资金流向。
- incremental 模式：从表中当前最大 trade_date+1 开始，一直到 end_date（默认今天）。
- 单次调用最多 6000 行，按交易日循环，不指定 ts_code。
- 日间循环无额外 sleep，遵从用户要求暂不考虑 200/min 限制。
- 支持 ingestion_jobs / ingestion_logs 进度更新，支持 truncate / bulk-session-tune。

Environment:
- TUSHARE_TOKEN
- TDX_DB_HOST/PORT/USER/PASSWORD/NAME
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

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
    application_name="AIstock-ingest-moneyflow-ts",
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
    parser = argparse.ArgumentParser(description="Ingest Tushare moneyflow into TimescaleDB")
    parser.add_argument("--mode", type=str, default="init", choices=["init", "incremental"], help="Ingestion mode")
    parser.add_argument("--start-date", type=str, default=None, help="Start date YYYY-MM-DD (required for init unless truncate-only)")
    parser.add_argument("--end-date", type=str, default=None, help="End date YYYY-MM-DD (defaults to today)")
    parser.add_argument("--job-id", type=str, default=None, help="Existing job id to attach and update")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate market.moneyflow_ts before init (full rebuild, destructive)",
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


def _date_range(d0: dt.date, d1: dt.date) -> List[dt.date]:
    cur = d0
    out: List[dt.date] = []
    step = dt.timedelta(days=1)
    while cur <= d1:
        out.append(cur)
        cur += step
    return out


def _get_max_trade_date(conn) -> Optional[dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT max(trade_date) FROM market.moneyflow_ts")
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return row[0]


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


def _update_job_progress(conn, job_id: uuid.UUID, stats: Dict[str, Any]) -> None:
    total = int(stats.get("total_days") or 0)
    done = int(stats.get("success_days") or 0) + int(stats.get("failed_days") or 0)
    progress = 100.0 * float(done) / float(total) if total > 0 else 0.0
    counters = {
        "total": total,
        "done": done,
        "running": 0,
        "pending": max(total - done, 0),
        "failed": int(stats.get("failed_days") or 0),
        "success": int(stats.get("success_days") or 0),
        "inserted_rows": int(stats.get("inserted_rows") or 0),
    }
    payload = {
        "counters": counters,
        "progress": max(0.0, min(100.0, progress)),
        "total_days": total,
        "done_days": done,
    }
    with conn.cursor() as cur:
        sql = (
            """
            UPDATE market.ingestion_jobs
               SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id = %s
            """
        )
        params = (json.dumps(payload, ensure_ascii=False), job_id)
        cur.execute(sql, params)


def _upsert_moneyflow(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.moneyflow_ts (trade_date, ts_code, "
        "buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount, "
        "buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount, "
        "buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount, "
        "buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount, "
        "net_mf_vol, net_mf_amount) "
        "VALUES %s ON CONFLICT (ts_code, trade_date) DO UPDATE SET "
        "buy_sm_vol=EXCLUDED.buy_sm_vol, buy_sm_amount=EXCLUDED.buy_sm_amount, "
        "sell_sm_vol=EXCLUDED.sell_sm_vol, sell_sm_amount=EXCLUDED.sell_sm_amount, "
        "buy_md_vol=EXCLUDED.buy_md_vol, buy_md_amount=EXCLUDED.buy_md_amount, "
        "sell_md_vol=EXCLUDED.sell_md_vol, sell_md_amount=EXCLUDED.sell_md_amount, "
        "buy_lg_vol=EXCLUDED.buy_lg_vol, buy_lg_amount=EXCLUDED.buy_lg_amount, "
        "sell_lg_vol=EXCLUDED.sell_lg_vol, sell_lg_amount=EXCLUDED.sell_lg_amount, "
        "buy_elg_vol=EXCLUDED.buy_elg_vol, buy_elg_amount=EXCLUDED.buy_elg_amount, "
        "sell_elg_vol=EXCLUDED.sell_elg_vol, sell_elg_amount=EXCLUDED.sell_elg_amount, "
        "net_mf_vol=EXCLUDED.net_mf_vol, net_mf_amount=EXCLUDED.net_mf_amount"
    )
    values = []
    seen = set()
    for r in rows:
        trade_date = r.get("trade_date")
        ts_code = (r.get("ts_code") or "").strip()
        if not trade_date or not ts_code:
            continue
        pk = (ts_code, trade_date)
        if pk in seen:
            continue
        seen.add(pk)
        values.append(
            (
                trade_date,
                ts_code,
                r.get("buy_sm_vol"),
                r.get("buy_sm_amount"),
                r.get("sell_sm_vol"),
                r.get("sell_sm_amount"),
                r.get("buy_md_vol"),
                r.get("buy_md_amount"),
                r.get("sell_md_vol"),
                r.get("sell_md_amount"),
                r.get("buy_lg_vol"),
                r.get("buy_lg_amount"),
                r.get("sell_lg_vol"),
                r.get("sell_lg_amount"),
                r.get("buy_elg_vol"),
                r.get("buy_elg_amount"),
                r.get("sell_elg_vol"),
                r.get("sell_elg_amount"),
                r.get("net_mf_vol"),
                r.get("net_mf_amount"),
            )
        )
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def _fetch_moneyflow_for_date(pro, trade_date: dt.date) -> List[Dict[str, Any]]:
    ymd = trade_date.strftime("%Y%m%d")
    df = pro.moneyflow(trade_date=ymd, limit=6000)
    rows: List[Dict[str, Any]] = []
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": row.get("ts_code"),
                    "buy_sm_vol": row.get("buy_sm_vol"),
                    "buy_sm_amount": row.get("buy_sm_amount"),
                    "sell_sm_vol": row.get("sell_sm_vol"),
                    "sell_sm_amount": row.get("sell_sm_amount"),
                    "buy_md_vol": row.get("buy_md_vol"),
                    "buy_md_amount": row.get("buy_md_amount"),
                    "sell_md_vol": row.get("sell_md_vol"),
                    "sell_md_amount": row.get("sell_md_amount"),
                    "buy_lg_vol": row.get("buy_lg_vol"),
                    "buy_lg_amount": row.get("buy_lg_amount"),
                    "sell_lg_vol": row.get("sell_lg_vol"),
                    "sell_lg_amount": row.get("sell_lg_amount"),
                    "buy_elg_vol": row.get("buy_elg_vol"),
                    "buy_elg_amount": row.get("buy_elg_amount"),
                    "sell_elg_vol": row.get("sell_elg_vol"),
                    "sell_elg_amount": row.get("sell_elg_amount"),
                    "net_mf_vol": row.get("net_mf_vol"),
                    "net_mf_amount": row.get("net_mf_amount"),
                }
            )
    return rows


def run_ingestion(conn, pro, mode: str, start_date: dt.date, end_date: dt.date, job_id: uuid.UUID) -> Dict[str, Any]:
    stats = {"total_days": 0, "success_days": 0, "failed_days": 0, "inserted_rows": 0}
    days = _date_range(start_date, end_date)
    stats["total_days"] = len(days)
    for d in days:
        try:
            rows = _fetch_moneyflow_for_date(pro, d)
            inserted = _upsert_moneyflow(conn, rows)
            stats["inserted_rows"] += inserted
            stats["success_days"] += 1
            print(f"[OK] moneyflow_ts {d} inserted={inserted}")
        except Exception as exc:  # noqa: BLE001
            stats["failed_days"] += 1
            _log(conn, job_id, "error", f"moneyflow_ts {d} failed: {exc}")
            print(f"[ERROR] moneyflow_ts {d} failed: {exc}")
        try:
            _update_job_progress(conn, job_id, stats)
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[WARN] failed to update job progress: {exc}")
    return stats


def main() -> None:
    args = parse_args()
    mode = (args.mode or "init").strip().lower()
    today = dt.date.today()

    if args.end_date:
        try:
            end_date = dt.date.fromisoformat(args.end_date)
        except ValueError:
            print("[ERROR] invalid --end-date format, expected YYYY-MM-DD")
            sys.exit(1)
    else:
        end_date = today

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        _ensure_session_tune(conn, getattr(args, "bulk_session_tune", False))
        pro = pro_api()

        if mode == "init":
            if args.truncate:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE market.moneyflow_ts")
                print("[WARN] truncated market.moneyflow_ts before init")
            if not args.start_date:
                print("[ERROR] --start-date is required in init mode")
                sys.exit(1)
            try:
                start_date = dt.date.fromisoformat(args.start_date)
            except ValueError:
                print("[ERROR] invalid --start-date format, expected YYYY-MM-DD")
                sys.exit(1)
        elif mode == "incremental":
            if args.start_date:
                try:
                    start_date = dt.date.fromisoformat(args.start_date)
                except ValueError:
                    print("[ERROR] invalid --start-date format, expected YYYY-MM-DD")
                    sys.exit(1)
            else:
                max_date = _get_max_trade_date(conn)
                if max_date is None:
                    start_date = end_date
                else:
                    start_date = max_date + dt.timedelta(days=1)
            if start_date > end_date:
                print("[INFO] moneyflow_ts up to date; nothing to do")
                return
        else:
            print(f"[ERROR] unsupported mode: {mode}")
            sys.exit(1)

        job_summary = {
            "dataset": "stock_moneyflow_ts",
            "mode": mode,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat(),
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            _start_existing_job(conn, job_id, job_summary)
        else:
            job_id = _create_job(conn, mode, job_summary)
        _log(conn, job_id, "info", f"start tushare moneyflow {mode} {start_date} -> {end_date}")

        try:
            stats = run_ingestion(conn, pro, mode, start_date, end_date, job_id)
            _finish_job(conn, job_id, "success" if stats["failed_days"] == 0 else "failed", {"stats": stats})
            print(f"[DONE] moneyflow_ts mode={mode} stats={stats}")
        except Exception as exc:  # noqa: BLE001
            _finish_job(conn, job_id, "failed", {"error": str(exc)})
            print(f"[ERROR] moneyflow_ts failed: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    main()
