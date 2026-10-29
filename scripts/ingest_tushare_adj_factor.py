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
from contextlib import ExitStack, closing
import datetime as dt
import json
import os
import math
from pathlib import Path
import sys
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv
import requests

# The scheduler launches this file by absolute path and does not guarantee that
# the repository root is the process working directory. Make the repository
# package importable before importing any ``backend.*`` module.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.adj_factor_history_reconciler import (  # noqa: E402
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
_CHECKPOINT_SCHEMA = "adj_factor_per_security_checkpoint_v1"


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
        from backend.services.data_integrity.contracts import finite_number
        if isinstance(adj, bool) or not finite_number(float(adj), positive=True):
            raise ValueError("adj_factor source must be finite and positive")
        values.append((ts_code, trade_date, float(adj)))
    if not values:
        return 0
    from backend.services.data_integrity.writes import changed_values
    return changed_values(conn, sql+" RETURNING trade_date", values, dataset="adj_factor", source="tushare_adj_factor_init")


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
        from backend.services.data_integrity.contracts import finite_number
        if isinstance(adj, bool) or not finite_number(float(adj), positive=True):
            raise ValueError("adj_factor source must be finite and positive")
        values.append((ts_code, trade_date, float(adj)))
    if not values:
        return 0
    from backend.services.data_integrity.writes import changed_values
    sql += " WHERE adj_factor.adj_factor IS DISTINCT FROM EXCLUDED.adj_factor RETURNING trade_date"
    return changed_values(conn, sql, values, dataset="adj_factor", source="tushare_adj_factor")


def _fetch_adj_factor_for_date(pro, trade_date: dt.date) -> List[Dict[str, Any]]:
    ymd = trade_date.strftime("%Y%m%d")
    df = pro.adj_factor(trade_date=ymd)
    rows: List[Dict[str, Any]] = []
    codes = set()
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            code = str(row.get("ts_code") or "").strip().upper()
            if not code or code in codes or str(row.get("trade_date") or "") != ymd:
                raise ValueError("adj_factor source has an invalid/duplicate security-date identity")
            value = row.get("adj_factor")
            if isinstance(value, bool) or value is None or not math.isfinite(float(value)) or float(value) <= 0:
                raise ValueError("adj_factor source must be finite and positive")
            codes.add(code)
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": code,
                    "adj_factor": value,
                }
            )
    if not rows:
        raise RuntimeError(f"adj_factor {trade_date}: empty provider response is not a completed trading day")
    return rows


def _trading_dates(conn, start: dt.date, end: dt.date) -> List[dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT cal_date,is_trading FROM market.trading_calendar "
                    "WHERE cal_date BETWEEN %s AND %s ORDER BY cal_date", (start, end))
        calendar = cur.fetchall()
    if {r[0] for r in calendar} != set(_date_range(start, end)):
        raise RuntimeError("adj_factor requested range has an unknown trading calendar")
    return [day for day, is_trading in calendar if is_trading]


def _resume_checkpoint(conn, end_date: dt.date, job_id) -> Dict[str, Any]:
    """Resume only an unfinished same-day collection, never a new day's audit."""
    with conn.cursor() as cur:
        cur.execute("""SELECT status,summary->'adj_factor_checkpoint',job_id FROM market.ingestion_jobs
            WHERE summary->>'dataset'='adj_factor' AND summary->>'mode'='incremental'
              AND summary#>>'{adj_factor_checkpoint,end_date}'=%s AND job_id<>%s AND created_at>=CURRENT_DATE
            ORDER BY created_at DESC LIMIT 1""", (end_date.isoformat(), job_id))
        row = cur.fetchone()
    if not row or row[0] != "failed" or not isinstance(row[1], dict):
        return {}
    checkpoint = row[1]
    if checkpoint.get("schema_version") != _CHECKPOINT_SCHEMA or checkpoint.get("end_date") != end_date.isoformat():
        return {}
    return {"completed": {str(k): str(v) for k, v in checkpoint.get("completed", {}).items()},
            "collection_id": str(checkpoint.get("collection_id") or row[2]),
            "collection_started_at": checkpoint.get("collection_started_at")}


def _record_history_state(conn, day: dt.date, status: str, *, failures=(), job_id=None) -> None:
    """Reuse the existing partition metadata; no extra table or success authority."""
    from backend.services.data_integrity.repository import schema_installed, enqueue_validation, IntegrityRepository
    if not schema_installed(conn):
        return
    previous = conn.autocommit
    if previous:
        conn.autocommit = False
    try:
        IntegrityRepository().ensure_partition(conn, "adj_factor", day)
        value = {"status": status, "end_date": day.isoformat(), "job_id": str(job_id),
                 "failed_symbols": sorted({f["symbol"] for f in failures})}
        with conn.cursor() as cur:
            cur.execute("""SELECT metadata->'adj_factor_history' FROM market.dataset_date_refresh_audit
                WHERE dataset='adj_factor' AND trade_date=%s AND data_source='canonical_db' FOR UPDATE""", (day,))
            row = cur.fetchone()
            previous_value = (row[0] if row else None) or {}
            if all(previous_value.get(key) == value[key] for key in ("status", "end_date", "failed_symbols")):
                conn.commit()
                return
            cur.execute("""UPDATE market.dataset_date_refresh_audit SET
                metadata=COALESCE(metadata,'{}'::jsonb)||jsonb_build_object('adj_factor_history',%s::jsonb),
                validated_revision=NULL,validation_id=NULL,validation_status='UNVERIFIED'
                WHERE dataset='adj_factor' AND trade_date=%s AND data_source='canonical_db'""",
                (json.dumps(value), day))
        enqueue_validation(conn, "adj_factor", day)
        # Fence validators of the previous verdict without inventing a change
        # to physical facts. Lock order remains partition, then persistent target.
        with conn.cursor() as cur:
            cur.execute("""UPDATE market.data_sync_targets SET target_status='pending',
                next_retry_at=clock_timestamp(),metadata=metadata-'lease_token'-'lease_until',
                updated_at=clock_timestamp() WHERE dataset='adj_factor' AND target_date=%s
                AND data_source='canonical_db' AND target_scope->>'operation'='VALIDATE_ONLY'""", (day,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if previous:
            conn.autocommit = True


def run_history_ingestion(conn, args, job_id, end_date: dt.date) -> Dict[str, Any]:
    repository = PostgresAdjFactorHistoryRepository(conn)
    with repository.writer_lock():
        # A historical retry must never replace a prefix below a newer local suffix.
        effective_end = max(end_date, _get_max_trade_date(conn) or end_date)
        full_scope = not args.history_reconcile_symbol
        dry_run = args.history_reconcile_dry_run
        seed = {} if dry_run else _resume_checkpoint(conn, effective_end, job_id)
        completed = seed.get("completed", {})
        collection_id = seed.get("collection_id") or str(job_id)
        collection_started_at = seed.get("collection_started_at") or dt.datetime.now(dt.timezone.utc).isoformat()
        last_flush = 0.0
        latest = {}

        def checkpoint(value, *, force=False):
            nonlocal last_flush, latest
            latest = {"schema_version": _CHECKPOINT_SCHEMA, **value, "collection_id": collection_id,
                      "collection_started_at": collection_started_at}
            if dry_run:
                return
            now = time.monotonic()
            done = len(value["completed"])+len(value["failures"])
            if not force and now-last_flush < 30:
                return
            last_flush = now
            counters = {"unit": "security", "total": value["total"], "done": done,
                        "success": len(value["completed"]), "failed": len(value["failures"]),
                        "pending": value["total"]-done, "inserted_rows": value["written_row_count"]}
            payload = {"adj_factor_checkpoint": latest, "counters": counters,
                       "progress": round(100 * done/value["total"], 2) if value["total"] else 0}
            with conn.cursor() as cur:
                cur.execute("""UPDATE market.ingestion_jobs SET summary=COALESCE(summary,'{}'::jsonb)||%s::jsonb
                    WHERE job_id=%s""", (json.dumps(payload), job_id))
            print(f"[PROGRESS] adj_factor checked={done}/{value['total']} "
                  f"failed={len(value['failures'])} written={value['written_row_count']}", flush=True)

        try:
            receipt = AdjFactorHistoryReconciler(
                repository=repository, provider_factory=_history_provider_factory,
                workers=args.history_reconcile_workers,
                calls_per_minute=args.history_reconcile_rate_per_minute,
                max_pages=args.history_reconcile_max_pages,
            ).reconcile(end_date=effective_end, symbols=args.history_reconcile_symbol,
                        dry_run=dry_run, independent=True, progress=checkpoint, completed=completed)
            if latest and not dry_run:
                checkpoint(latest, force=True)
            receipt["collection_id"] = collection_id
            receipt["collection_started_at"] = collection_started_at
            receipt["collection_finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            receipt["source_observation_policy"] = "per_security_stable_reads_same_collection_checkpoints"
            if full_scope and not dry_run:
                _record_history_state(conn, end_date, "PARTIAL" if receipt["failures"] else "COMPLETE",
                                      failures=receipt["failures"], job_id=job_id)
            return receipt
        except Exception:
            if latest and not dry_run:
                checkpoint(latest, force=True)
            if full_scope and not dry_run:
                _record_history_state(conn, end_date, "INTERRUPTED", job_id=job_id)
            raise


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
    days = _trading_dates(conn, start_date, end_date)
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

    # psycopg2's connection context starts a transaction even in autocommit
    # mode. The repository owns fact/checkpoint/verdict transactions; this
    # outer scope owns only connection lifetime and must not roll them back.
    with closing(psycopg2.connect(**DB_CFG)) as conn, ExitStack() as stack:
        # 启用 autocommit，同时在 run_ingestion 中对进度更新显式调用 conn.commit()
        conn.autocommit = True
        # Acquire before touching job ownership, not only before fact writes.
        stack.enter_context(PostgresAdjFactorHistoryRepository(conn).writer_lock())
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
                history_receipt = run_history_ingestion(conn, args, job_id, end_date)
                if history_receipt.get("failures"):
                    _finish_job(conn, job_id, "failed", {"history_reconciliation": history_receipt})
                    raise RuntimeError(f"adj_factor history unresolved: {history_receipt['failed_symbols']}")
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
                with PostgresAdjFactorHistoryRepository(conn).writer_lock():
                    if args.truncate:
                        with conn.cursor() as cur:
                            cur.execute("TRUNCATE TABLE market.adj_factor")
                    stats = run_ingestion(conn, pro, mode, start_date, end_date, job_id, args.batch_sleep)
                if stats["failed_days"]:
                    raise RuntimeError(f"adj_factor ingestion has {stats['failed_days']} failed date batches")
            # 更新 data_stats.last_updated_at，仅针对 adj_factor
            if not (mode == "incremental" and args.history_reconcile_dry_run):
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
