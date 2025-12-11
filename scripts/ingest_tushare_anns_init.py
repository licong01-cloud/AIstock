"""Ingest Tushare anns_d (上市公司公告) into market.anns.

- init: 按日期全量同步，start_date 必填，end_date 默认今天；每个自然日调用 anns_d。
- incremental: 从当前表中 max(ann_date)+1 开始补齐到 end_date（可被 --start-date 覆盖）。
- 每个日期内使用 limit+offset 分页，直到 df.empty。
- 记录 ingestion_jobs / ingestion_logs 方便任务监视器展示。

注意：此脚本只负责公告元数据入库，不下载 PDF 文件；PDF 下载由单独任务处理。
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
    application_name="AIstock-ingest-anns_d",
)


def _load_tushare() -> Any:
    import importlib

    return importlib.import_module("tushare")


def pro_api():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts = _load_tushare()
    return ts.pro_api(token)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Tushare anns_d into TimescaleDB (market.anns)")
    parser.add_argument("--mode", type=str, default="init", choices=["init", "incremental"], help="Ingestion mode")
    parser.add_argument("--start-date", type=str, default=None, help="Start ann_date YYYY-MM-DD (or override)")
    parser.add_argument("--end-date", type=str, default=None, help="End ann_date YYYY-MM-DD (defaults to today)")
    parser.add_argument("--job-id", type=str, default=None, help="Existing job id to attach and update")
    parser.add_argument("--batch-sleep", type=float, default=0.2, help="Sleep seconds between date batches")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate market.anns before init (destructive)",
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


def _get_max_ann_date(conn) -> Optional[dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT max(ann_date) FROM market.anns")
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
        if len(s) == 8 and s.isdigit():
            return dt.date(int(s[:4]), int(s[4:6]), int(s[6:]))
        return dt.date.fromisoformat(s)
    except Exception:
        return None


def _fetch_anns_for_date(pro, ann_date: dt.date) -> List[Dict[str, Any]]:
    """Fetch anns_d rows for a specific ann_date with pagination.

    We only pull a subset of columns we care about and map them to Python types
    matching market.anns schema.
    """

    ymd = ann_date.strftime("%Y%m%d")
    limit = 2000
    offset = 0
    rows: List[Dict[str, Any]] = []
    while True:
        df = pro.anns_d(ann_date=ymd, limit=limit, offset=offset)
        if df is None or df.empty:
            break
        for _, row in df.iterrows():
            ann_date_val = _parse_ymd(row.get("ann_date") or ymd)
            if not ann_date_val:
                ann_date_val = ann_date
            rec_time_raw = row.get("rec_time") or None
            rec_time_val: Optional[dt.datetime]
            if rec_time_raw:
                try:
                    # Tushare 通常返回 "YYYY-MM-DD HH:MM:SS" 或类似格式
                    rec_time_val = dt.datetime.fromisoformat(str(rec_time_raw))
                except Exception:
                    rec_time_val = None
            else:
                rec_time_val = None
            rows.append(
                {
                    "ann_date": ann_date_val,
                    "ts_code": (row.get("ts_code") or "").strip(),
                    "name": (row.get("name") or "").strip(),
                    "title": (row.get("title") or "").strip(),
                    "url": (row.get("url") or "").strip(),
                    "rec_time": rec_time_val,
                }
            )
        if len(df.index) < limit:
            break
        offset += limit
        time.sleep(0.05)
    return rows


def _upsert_anns(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.anns (ann_date, ts_code, name, title, url, rec_time, download_status) "
        "VALUES %s ON CONFLICT (ts_code, ann_date, title) DO UPDATE SET "
        "name=EXCLUDED.name, url=EXCLUDED.url, rec_time=EXCLUDED.rec_time"
    )

    # 为避免 PostgreSQL 报错 "ON CONFLICT DO UPDATE command cannot affect row a second time"
    # 需要在单次 batch 内对冲突键 (ann_date, ts_code, title) 去重，只保留最后一条记录。
    dedup: Dict[tuple, tuple] = {}
    for r in rows:
        ts_code = (r.get("ts_code") or "").strip()
        ann_date = r.get("ann_date")
        title = (r.get("title") or "").strip()
        if not ts_code or not ann_date or not title:
            continue
        key = (ann_date, ts_code, title)
        dedup[key] = (
            ann_date,
            ts_code,
            r.get("name") or "",
            title,
            r.get("url") or "",
            r.get("rec_time"),
            "pending",
        )

    values = list(dedup.values())
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def run_ingestion(
    conn,
    pro,
    mode: str,
    start_date: dt.date,
    end_date: dt.date,
    job_id: uuid.UUID,
    batch_sleep: float,
) -> Dict[str, Any]:
    stats = {"total_days": 0, "success_days": 0, "failed_days": 0, "inserted_rows": 0}
    days = _date_range(start_date, end_date)
    stats["total_days"] = len(days)
    for d in days:
        try:
            rows = _fetch_anns_for_date(pro, d)
            inserted = _upsert_anns(conn, rows)
            stats["inserted_rows"] += inserted
            stats["success_days"] += 1
            print(f"[OK] anns_d {d} inserted={inserted}")
        except Exception as exc:  # noqa: BLE001
            stats["failed_days"] += 1
            # 该日期批次失败时，先回滚当前事务，避免后续 _log/_update_job_progress
            # 在 aborted transaction 上再次报 InFailedSqlTransaction。
            try:
                conn.rollback()
            except Exception:
                pass
            _log(conn, job_id, "error", f"anns_d {d} failed: {exc}")
            print(f"[ERROR] anns_d {d} failed: {exc}")
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
                    cur.execute("TRUNCATE TABLE market.anns")
                print("[WARN] TRUNCATE market.anns executed before full ingestion")
        elif mode == "incremental":
            if args.start_date:
                try:
                    start_date = dt.date.fromisoformat(args.start_date)
                except ValueError:
                    print("[ERROR] invalid --start-date format, expected YYYY-MM-DD")
                    sys.exit(1)
            else:
                max_date = _get_max_ann_date(conn)
                start_date = (max_date + dt.timedelta(days=1)) if max_date else end_date
            if start_date > end_date:
                print("[INFO] anns_d up to date; nothing to do")
                return
        else:
            print(f"[ERROR] unsupported mode: {mode}")
            sys.exit(1)

        job_summary = {
            "dataset": "anns_d",
            "mode": mode,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat(),
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            _start_existing_job(conn, job_id, job_summary)
        else:
            job_id = _create_job(conn, mode, job_summary)

        _log(conn, job_id, "info", f"start tushare anns_d ingestion {mode} {start_date} -> {end_date}")

        try:
            stats = run_ingestion(conn, pro, mode, start_date, end_date, job_id, args.batch_sleep)
            _finish_job(conn, job_id, "success" if stats["failed_days"] == 0 else "failed", {"stats": stats})
            print(f"[DONE] anns_d mode={mode} stats={stats}")
        except Exception as exc:  # noqa: BLE001
            _finish_job(conn, job_id, "failed", {"error": str(exc)})
            print(f"[ERROR] anns_d failed: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    main()
