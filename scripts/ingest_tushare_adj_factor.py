"""Ingest Tushare adj_factor (stock adjustment factor) into TimescaleDB.

- init 模式：在给定日期区间内，按交易日循环，调用 Tushare pro.adj_factor(trade_date=YYYYMMDD)，
  将全市场复权因子写入 market.adj_factor。可选在开始前 TRUNCATE 目标表。
- incremental 模式：对本地相关股票逐只执行两次完整历史稳定读取，校验覆盖后，在单一事务内仅追加新后缀、
  并替换发生历史重述的股票；不再使用只能发现新增日期的逐日 upsert 作为增量写入路径。

Environment:
- TUSHARE_TOKEN      用于初始化 Tushare pro_api
- TDX_DB_HOST/PORT/USER/PASSWORD/NAME  PostgreSQL 连接信息

该脚本接入 ingestion_jobs / ingestion_logs，便于前端监控任务进度。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv
import requests

from backend.services.adj_factor_history_reconciler import (
    AdjFactorHistoryReconciler,
    PostgresAdjFactorHistoryRepository,
)


load_dotenv(override=True)
pgx.register_uuid()


DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-ingest-adj-factor",
)

_HISTORY_PROVIDER_LOCAL = threading.local()


def _load_tushare():
    import importlib

    return importlib.import_module("tushare")


def pro_api():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts = _load_tushare()
    return ts.pro_api(token)


def _history_provider_factory():
    provider = getattr(_HISTORY_PROVIDER_LOCAL, "provider", None)
    if provider is None:
        provider = pro_api()
        _HISTORY_PROVIDER_LOCAL.provider = provider
    return provider


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Tushare adj_factor into TimescaleDB")
    parser.add_argument("--mode", type=str, default="init", choices=["init", "incremental"], help="Ingestion mode")
    parser.add_argument(
        "--start-date", type=str, default=None, help="Start date YYYY-MM-DD (init or override for incremental)"
    )
    parser.add_argument("--end-date", type=str, default=None, help="End date YYYY-MM-DD (defaults to today)")
    parser.add_argument("--job-id", type=str, default=None, help="Existing job id to attach and update")
    parser.add_argument("--truncate", action="store_true", help="TRUNCATE market.adj_factor before init")
    parser.add_argument("--batch-sleep", type=float, default=0.1, help="Sleep seconds between trade_date batches")
    parser.add_argument(
        "--history-reconcile-workers",
        type=int,
        default=int(os.getenv("ADJ_FACTOR_HISTORY_RECONCILE_WORKERS", "4")),
        help="Parallel readers for mandatory incremental full-history reconciliation",
    )
    parser.add_argument(
        "--history-reconcile-rate-per-minute",
        type=int,
        default=int(os.getenv("ADJ_FACTOR_HISTORY_RATE_PER_MINUTE", "480")),
        help="Shared Tushare request limit across reconciliation workers",
    )
    parser.add_argument(
        "--history-reconcile-max-pages",
        type=int,
        default=int(os.getenv("ADJ_FACTOR_HISTORY_MAX_PAGES", "4")),
        help="Maximum backward pages for provider-limited long histories",
    )
    parser.add_argument(
        "--history-reconcile-symbol",
        action="append",
        default=None,
        help="Limit reconciliation to one symbol; repeat only for controlled diagnostics",
    )
    parser.add_argument(
        "--history-reconcile-dry-run",
        action="store_true",
        help="Audit stable full histories without replacing market.adj_factor rows",
    )
    parser.add_argument(
        "--bulk-session-tune",
        action="store_true",
        help="Ignored placeholder; kept for compatibility with scheduler default args",
    )
    return parser.parse_args()


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
        cur.execute("SELECT max(trade_date) FROM market.adj_factor")
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return row[0]


def _create_job(conn, job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
    job_id = uuid.uuid4()
    with conn.cursor() as cur:
        sql = """
            INSERT INTO market.ingestion_jobs (job_id, job_type, status, created_at, started_at, summary)
            VALUES (%s, %s, 'running', NOW(), NOW(), %s)
            """
        payload = (job_id, job_type, json.dumps(summary, ensure_ascii=False))
        print("[DEBUG] _create_job SQL:", sql.strip().replace("\n", " "))
        print("[DEBUG] _create_job params:", payload)
        cur.execute(
            sql,
            payload,
        )
    return job_id


def _start_existing_job(conn, job_id: uuid.UUID, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        sql = """
            UPDATE market.ingestion_jobs
               SET status='running', started_at=COALESCE(started_at, NOW()), summary=%s
             WHERE job_id=%s
            """
        payload = (json.dumps(summary, ensure_ascii=False), job_id)
        print("[DEBUG] _start_existing_job SQL:", sql.strip().replace("\n", " "))
        print("[DEBUG] _start_existing_job params:", payload)
        cur.execute(
            sql,
            payload,
        )


def _finish_job(conn, job_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        # 保留原有 summary 中的 dataset/mode/范围/进度等信息，仅将新的 summary 字段合并进去，
        # 避免任务结束时覆盖掉前面写入的监控所需元数据。
        merged = json.dumps(summary, ensure_ascii=False)
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status=%s,
                   finished_at=NOW(),
                   summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id=%s
            """,
            (status, merged, job_id),
        )


def _log(conn, job_id: uuid.UUID, level: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES (%s, NOW(), %s, %s)",
            (job_id, level.upper(), message),
        )


def _touch_data_stats_last_updated(conn) -> None:
    """Ensure market.data_stats.last_updated_at is updated for adj_factor.

    This only affects data_kind='adj_factor' and will not touch stats for other datasets.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.data_stats (data_kind, table_name, last_updated_at)
            VALUES ('adj_factor', 'market.adj_factor', NOW())
            ON CONFLICT (data_kind)
            DO UPDATE SET
              last_updated_at = EXCLUDED.last_updated_at,
              table_name = EXCLUDED.table_name
            """
        )


def _update_job_progress(conn, job_id: uuid.UUID, stats: Dict[str, Any]) -> None:
    """Update ingestion_jobs.progress and summary.counters for this adj_factor job.

    This is a lightweight, best-effort update so the frontend progress bar can move.
    It only touches the current job row and the JSON summary, without affecting
    any other datasets or jobs.
    """
    total = int(stats.get("total_days") or 0)
    done = int(stats.get("success_days") or 0) + int(stats.get("failed_days") or 0)
    progress: float
    if total > 0:
        progress = max(0.0, min(100.0, 100.0 * float(done) / float(total)))
    else:
        progress = 0.0

    counters = {
        "total": total,
        "done": done,
        "running": 0,
        "pending": max(total - done, 0),
        "failed": int(stats.get("failed_days") or 0),
        "success": int(stats.get("success_days") or 0),
        "inserted_rows": int(stats.get("inserted_rows") or 0),
    }

    # 将进度信息写入 summary：
    # - summary.counters：供前端直接展示总数/已完成/失败等统计
    # - summary.progress：百分比进度
    # - summary.total_days / summary.done_days：兼容后端 _job_status 中基于天数的进度计算逻辑
    payload = {
        "counters": counters,
        "progress": progress,
        "total_days": total,
        "done_days": done,
    }
    with conn.cursor() as cur:
        sql = """
            UPDATE market.ingestion_jobs
               SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id = %s
            """
        params = (json.dumps(payload, ensure_ascii=False), job_id)
        print("[DEBUG] _update_job_progress SQL:", sql.strip().replace("\n", " "))
        print("[DEBUG] _update_job_progress params:", params)
        cur.execute(
            sql,
            params,
        )


def _insert_adj_factor_init(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = "INSERT INTO market.adj_factor (ts_code, trade_date, adj_factor) VALUES %s"
    values = []
    for r in rows:
        ts_code = (r.get("ts_code") or "").strip()
        trade_date = r.get("trade_date")
        adj = r.get("adj_factor")
        if not ts_code or trade_date is None or adj is None:
            continue
        values.append((ts_code, trade_date, float(adj)))
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def _upsert_adj_factor(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.adj_factor (ts_code, trade_date, adj_factor) "
        "VALUES %s ON CONFLICT (ts_code, trade_date) DO UPDATE SET adj_factor = EXCLUDED.adj_factor"
    )
    values = []
    for r in rows:
        ts_code = (r.get("ts_code") or "").strip()
        trade_date = r.get("trade_date")
        adj = r.get("adj_factor")
        if not ts_code or trade_date is None or adj is None:
            continue
        values.append((ts_code, trade_date, float(adj)))
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def _fetch_adj_factor_for_date(pro, trade_date: dt.date) -> List[Dict[str, Any]]:
    ymd = trade_date.strftime("%Y%m%d")
    df = pro.adj_factor(trade_date=ymd)
    rows: List[Dict[str, Any]] = []
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": row.get("ts_code"),
                    "adj_factor": row.get("adj_factor"),
                }
            )
    return rows


def run_ingestion(
    conn,
    pro,
    mode: str,
    start_date: dt.date,
    end_date: dt.date,
    job_id: uuid.UUID,
    batch_sleep: float,
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {"total_days": 0, "success_days": 0, "failed_days": 0, "inserted_rows": 0}
    days = _date_range(start_date, end_date)
    stats["total_days"] = len(days)
    for idx, d in enumerate(days, start=1):
        try:
            for attempt in range(1, 4):
                try:
                    rows = _fetch_adj_factor_for_date(pro, d)
                    if mode == "init":
                        inserted = _insert_adj_factor_init(conn, rows)
                    else:
                        inserted = _upsert_adj_factor(conn, rows)
                    stats["inserted_rows"] += inserted
                    stats["success_days"] += 1
                    break
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as net_exc:
                    if attempt >= 3:
                        raise
                    wait_seconds = 5 * attempt
                    _log(
                        conn,
                        job_id,
                        "warn",
                        f"adj_factor {d} network error on attempt {attempt}: {net_exc}; retrying in {wait_seconds}s",
                    )
                    print(
                        f"[WARN] adj_factor {d} network error on attempt {attempt}: {net_exc}; retrying in {wait_seconds}s"
                    )
                    time.sleep(wait_seconds)
        except Exception as exc:  # noqa: BLE001
            stats["failed_days"] += 1
            _log(conn, job_id, "error", f"adj_factor {d} failed: {exc}")
            print(f"[WARN] adj_factor {d} failed: {exc}")
        # Best-effort per-day progress update so that the frontend progress bar moves.
        # 使用显式事务提交，确保其他会话能够看到中间进度。
        try:
            _update_job_progress(conn, job_id, stats)
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            # Progress update failure should not break ingestion; rollback this tx, log and continue.
            try:
                conn.rollback()
            except Exception as rollback_exc:  # noqa: BLE001
                print(f"[ERROR] failed to rollback progress transaction: {rollback_exc}")
            msg = f"failed to update job progress: {exc}"
            print(f"[WARN] {msg}")
            try:
                _log(conn, job_id, "warn", msg)
            except Exception as log_exc:  # noqa: BLE001
                print(f"[ERROR] failed to persist progress warning: {log_exc}")
        if batch_sleep > 0:
            time.sleep(batch_sleep)
    return stats


def main() -> None:
    args = parse_args()
    mode = (args.mode or "init").strip().lower()

    today = dt.date.today()
    start_date: Optional[dt.date] = None
    end_date: dt.date

    if args.end_date:
        try:
            end_date = dt.date.fromisoformat(args.end_date)
        except ValueError:
            print("[ERROR] invalid --end-date format, expected YYYY-MM-DD")
            sys.exit(1)
    else:
        end_date = today

    with psycopg2.connect(**DB_CFG) as conn:
        # 启用 autocommit，同时在 run_ingestion 中对进度更新显式调用 conn.commit()
        conn.autocommit = True
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
                    print("[INFO] adj_factor is empty; falling back to init-like behaviour from end_date")
                    start_date = end_date
                else:
                    start_date = max_date + dt.timedelta(days=1)
            if start_date > end_date:
                print("[INFO] adj_factor daily rows are up to date; continuing with history reconciliation")
        else:
            print(f"[ERROR] unsupported mode: {mode}")
            sys.exit(1)

        if mode == "init" and args.truncate:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE market.adj_factor")
            print("[WARN] TRUNCATE market.adj_factor executed before full adj_factor ingestion")

        job_summary = {
            "dataset": "adj_factor",
            "mode": mode,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat(),
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            _start_existing_job(conn, job_id, job_summary)
        else:
            job_id = _create_job(conn, mode, job_summary)
        _log(conn, job_id, "info", f"start tushare adj_factor ingestion {mode} {start_date} -> {end_date}")

        try:
            history_receipt = None
            if mode == "incremental":
                _log(
                    conn,
                    job_id,
                    "info",
                    "start mandatory no-LLM adj_factor complete-history reconciliation; "
                    "this is the only incremental market.adj_factor write path",
                )
                history_receipt = AdjFactorHistoryReconciler(
                    repository=PostgresAdjFactorHistoryRepository(conn),
                    provider_factory=_history_provider_factory,
                    workers=args.history_reconcile_workers,
                    calls_per_minute=args.history_reconcile_rate_per_minute,
                    max_pages=args.history_reconcile_max_pages,
                ).reconcile(
                    end_date=end_date,
                    symbols=args.history_reconcile_symbol,
                    dry_run=args.history_reconcile_dry_run,
                )
                _log(
                    conn,
                    job_id,
                    "info",
                    "adj_factor history reconciliation finished: "
                    f"status={history_receipt['status']} "
                    f"scanned={history_receipt['scanned_symbol_count']} "
                    f"changed={history_receipt['changed_symbol_count']} "
                    f"written_rows={history_receipt['written_row_count']}",
                )
                stats = {
                    "total_days": 0,
                    "success_days": 0,
                    "failed_days": 0,
                    "inserted_rows": history_receipt["written_row_count"],
                }
            else:
                stats = run_ingestion(
                    conn,
                    pro,
                    mode,
                    start_date,
                    end_date,
                    job_id,
                    args.batch_sleep,
                )
                if stats["failed_days"]:
                    raise RuntimeError(f"adj_factor ingestion has {stats['failed_days']} failed date batches")
            # 更新 data_stats.last_updated_at，仅针对 adj_factor
            _touch_data_stats_last_updated(conn)
            # 结束时写一条总览日志，汇总成功/失败天数与插入行数
            _log(
                conn,
                job_id,
                "info",
                f"adj_factor finished: mode={mode} total_days={stats['total_days']} success_days={stats['success_days']} failed_days={stats['failed_days']} inserted_rows={stats['inserted_rows']}",
            )
            finish_summary = {"stats": stats}
            if history_receipt is not None:
                finish_summary["history_reconciliation"] = history_receipt
            _finish_job(conn, job_id, "success", finish_summary)
            print(
                f"[DONE] adj_factor mode={mode} stats={stats} "
                f"history_status={(history_receipt or {}).get('status', 'not_run')}"
            )
        except Exception as exc:  # noqa: BLE001
            _finish_job(conn, job_id, "failed", {"error": str(exc)})
            print(f"[ERROR] adj_factor failed: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    main()
