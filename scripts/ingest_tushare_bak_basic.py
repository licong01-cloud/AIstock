"""Ingest Tushare bak_basic (historical stock list) with init/incremental and per-day batches.

- init: full range by trade_date (start_date required), per-day fetch, upsert by (trade_date, ts_code).
- incremental: trade_date cursor from max(trade_date)+1 to today (or override), per-day fetch.
- supports --truncate before init, --batch-sleep between trade_date batches.
- supports --bulk-session-tune for session-level write tuning.
- only logs errors/warnings to ingestion_logs; normal successes print to stdout.
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
    application_name="AIstock-ingest-bak-basic",
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
    parser = argparse.ArgumentParser(description="Ingest Tushare bak_basic into TimescaleDB")
    parser.add_argument("--mode", type=str, default="init", choices=["init", "incremental"], help="Ingestion mode")
    parser.add_argument("--start-date", type=str, default=None, help="Start trade_date YYYY-MM-DD (or override)")
    parser.add_argument("--end-date", type=str, default=None, help="End trade_date YYYY-MM-DD (defaults to today)")
    parser.add_argument("--job-id", type=str, default=None, help="Existing job id to attach and update")
    parser.add_argument("--batch-sleep", type=float, default=0.2, help="Sleep seconds between trade_date batches")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate market.bak_basic before init (destructive)",
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
        cur.execute("SELECT max(trade_date) FROM market.bak_basic")
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
    progress = 0.0 if total <= 0 else max(0.0, min(100.0, 100.0 * float(done) / float(total)))
    counters = {
        "total": total,
        "done": done,
        "running": 0,
        "pending": max(total - done, 0),
        "failed": int(stats.get("failed_days") or 0),
        "success": int(stats.get("success_days") or 0),
        "inserted_rows": int(stats.get("inserted_rows") or 0),
    }
    payload = {"counters": counters, "progress": progress, "total_days": total, "done_days": done}
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id = %s
            """,
            (json.dumps(payload, ensure_ascii=False), job_id),
        )


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


def _fetch_bak_basic_for_date(pro, trade_date: dt.date) -> List[Dict[str, Any]]:
    ymd = trade_date.strftime("%Y%m%d")
    offset = 0
    limit = 2000
    rows: List[Dict[str, Any]] = []
    while True:
        df = pro.bak_basic(trade_date=ymd, limit=limit, offset=offset)
        if df is None or df.empty:
            break
        for _, row in df.iterrows():
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": row.get("ts_code"),
                    "name": row.get("name"),
                    "industry": row.get("industry"),
                    "area": row.get("area"),
                    "pe": row.get("pe"),
                    "pb": row.get("pb"),
                    "total_share": row.get("total_share"),
                    "float_share": row.get("float_share"),
                    "free_share": row.get("free_share"),
                    "total_mv": row.get("total_mv"),
                    "circ_mv": row.get("circ_mv"),
                }
            )
        if len(df.index) < limit:
            break
        offset += limit
        time.sleep(0.05)
    return rows


def _upsert_bak_basic(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.bak_basic (trade_date, ts_code, name, industry, area, pe, pb, total_share, float_share, free_share, total_mv, circ_mv) "
        "VALUES %s ON CONFLICT (trade_date, ts_code) DO UPDATE SET "
        "name=EXCLUDED.name, industry=EXCLUDED.industry, area=EXCLUDED.area, pe=EXCLUDED.pe, pb=EXCLUDED.pb, "
        "total_share=EXCLUDED.total_share, float_share=EXCLUDED.float_share, free_share=EXCLUDED.free_share, "
        "total_mv=EXCLUDED.total_mv, circ_mv=EXCLUDED.circ_mv"
    )
    values = []
    for r in rows:
        ts_code = (r.get("ts_code") or "").strip()
        trade_date = r.get("trade_date")
        if not ts_code or not trade_date:
            continue
        values.append(
            (
                trade_date,
                ts_code,
                r.get("name"),
                r.get("industry"),
                r.get("area"),
                r.get("pe"),
                r.get("pb"),
                r.get("total_share"),
                r.get("float_share"),
                r.get("free_share"),
                r.get("total_mv"),
                r.get("circ_mv"),
            )
        )
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def run_ingestion(conn, pro, mode: str, start_date: dt.date, end_date: dt.date, job_id: uuid.UUID, batch_sleep: float) -> Dict[str, Any]:
    stats = {"total_days": 0, "success_days": 0, "failed_days": 0, "inserted_rows": 0}
    days = _date_range(start_date, end_date)
    stats["total_days"] = len(days)
    for d in days:
        try:
            rows = _fetch_bak_basic_for_date(pro, d)
            inserted = _upsert_bak_basic(conn, rows)
            stats["inserted_rows"] += inserted
            stats["success_days"] += 1
            print(f"[OK] bak_basic {d} inserted={inserted}")
        except Exception as exc:  # noqa: BLE001
            stats["failed_days"] += 1
            _log(conn, job_id, "error", f"bak_basic {d} failed: {exc}")
            print(f"[ERROR] bak_basic {d} failed: {exc}")
            time.sleep(batch_sleep)
        try:
            _update_job_progress(conn, job_id, stats)
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:
                pass
            msg = f"failed to update job progress: {exc}"
            print(f"[WARN] {msg}")
            try:
                _log(conn, job_id, "warn", msg)
            except Exception:
                pass
        if batch_sleep > 0:
            time.sleep(batch_sleep)
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
            if not args.start_date:
                print("[ERROR] --start-date is required in init mode")
                sys.exit(1)
            try:
                start_date = dt.date.fromisoformat(args.start_date)
            except ValueError:
                print("[ERROR] invalid --start-date format, expected YYYY-MM-DD")
                sys.exit(1)
            if args.truncate:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE market.bak_basic")
                print("[WARN] TRUNCATE market.bak_basic executed before full ingestion")
        elif mode == "incremental":
            if args.start_date:
                try:
                    start_date = dt.date.fromisoformat(args.start_date)
                except ValueError:
                    print("[ERROR] invalid --start-date format, expected YYYY-MM-DD")
                    sys.exit(1)
            else:
                max_date = _get_max_trade_date(conn)
                start_date = (max_date + dt.timedelta(days=1)) if max_date else end_date
            if start_date > end_date:
                print("[INFO] bak_basic up to date; nothing to do")
                return
        else:
            print(f"[ERROR] unsupported mode: {mode}")
            sys.exit(1)

        job_summary = {
            "dataset": "bak_basic",
            "mode": mode,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat(),
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            _start_existing_job(conn, job_id, job_summary)
        else:
            job_id = _create_job(conn, mode, job_summary)

        _log(conn, job_id, "info", f"start tushare bak_basic ingestion {mode} {start_date} -> {end_date}")

        try:
            stats = run_ingestion(conn, pro, mode, start_date, end_date, job_id, args.batch_sleep)
            _finish_job(conn, job_id, "success" if stats["failed_days"] == 0 else "failed", {"stats": stats})
            print(f"[DONE] bak_basic mode={mode} stats={stats}")
        except Exception as exc:  # noqa: BLE001
            _finish_job(conn, job_id, "failed", {"error": str(exc)})
            print(f"[ERROR] bak_basic failed: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    main()
