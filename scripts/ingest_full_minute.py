"""Full 1-minute ingestion from TDX API into TimescaleDB.

Iterates over codes and trading dates, pulls 1-minute raw bars via the
TDX local API, and upserts into market.kline_minute_raw. Progress and
errors are tracked through ingestion_runs / ingestion_checkpoints /
ingestion_errors tables.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx
import requests
from requests import exceptions as req_exc

pgx.register_uuid()

TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:8080")
DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)
SUPPORTED_EXCHANGES = {"sh", "sz", "bj"}
EXCHANGE_MAP = {"sh": "SH", "sz": "SZ", "bj": "BJ"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TDX full minute ingestion")
    parser.add_argument("--exchanges", type=str, default="sh,sz", help="Comma separated exchanges (sh,sz,bj)")
    parser.add_argument("--start-date", type=str, required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=dt.date.today().isoformat(), help="End date YYYY-MM-DD")
    parser.add_argument("--batch-size", type=int, default=50, help="Codes per batch")
    parser.add_argument("--limit-codes", type=int, default=None, help="Optional limit on number of codes to process")
    parser.add_argument("--max-empty", type=int, default=3, help="Stop after N consecutive empty days for a code")
    parser.add_argument("--truncate", action="store_true", help="TRUNCATE market.kline_minute_raw before run")
    parser.add_argument("--job-id", type=str, default=None, help="Existing job id to attach and update")
    parser.add_argument("--bulk-session-tune", action="store_true", help="Apply session-level tuning for bulk load")
    return parser.parse_args()


def http_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = TDX_API_BASE.rstrip("/") + path
    max_retries = 3
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("code") != 0:
                raise RuntimeError(f"TDX API error {path}: {data}")
            return data
        except (req_exc.ConnectionError, req_exc.Timeout) as exc:  # 网络类错误重试
            last_exc = exc
            if attempt >= max_retries:
                break
            # 简单退避，避免端口/连接资源瞬间耗尽
            import time

            time.sleep(1 + attempt)
        except Exception:
            # 其他错误不重试，直接抛出以便上层记录错误
            raise
    # 若多次重试仍失败，抛出最后一次异常
    raise last_exc or RuntimeError(f"TDX API request failed after retries: {url}")


def normalize_ts_code(code: str) -> Optional[str]:
    code = (code or "").strip()
    if len(code) != 6 or not code.isdigit():
        return None
    if code.startswith("6"):
        suffix = "SH"
    elif code.startswith(("8", "4")):
        suffix = "BJ"
    else:
        suffix = "SZ"
    return f"{code}.{suffix}"


def fetch_codes(exchanges: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for exch in exchanges:
        params = {"exchange": exch} if exch != "all" else {}
        try:
            data = http_get("/api/codes", params=params)
        except Exception as exc:  # noqa: BLE001
            print(f"[ERROR] 获取交易所 {exch} 股票列表失败: {exc}")
            raise
        payload = data.get("data") if isinstance(data, dict) else None
        if isinstance(payload, dict):
            rows = payload.get("codes") or []
        else:
            rows = payload or []
        for item in rows:
            code = item.get("code") if isinstance(item, dict) else str(item)
            ts_code = normalize_ts_code(code)
            if ts_code and ts_code not in seen:
                seen.add(ts_code)
                result.append(ts_code)
    return result


def date_range(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def fetch_minute(code: str, date: dt.date) -> List[Dict[str, Any]]:
    params = {"code": code, "type": "minute1", "date": date.strftime("%Y%m%d")}
    data = http_get("/api/minute", params=params)
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        items = payload.get("List") or payload.get("list") or payload
        if isinstance(items, dict):
            items = items.get("List") or items.get("list") or []
    else:
        items = payload or []
    return list(items)


def _combine_trade_time(date_hint: dt.date, value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace("Z", "+00:00")
    try:
        dt_obj = dt.datetime.fromisoformat(cleaned)
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
        return dt_obj.isoformat()
    except ValueError:
        pass

    base_date = date_hint
    try:
        time_obj = dt.datetime.strptime(text, "%H:%M:%S").time()
    except ValueError:
        try:
            time_obj = dt.datetime.strptime(text, "%H:%M").time()
        except ValueError:
            return None
    tzinfo = dt.timezone(dt.timedelta(hours=8))
    return dt.datetime.combine(base_date, time_obj).replace(tzinfo=tzinfo).isoformat()


def upsert_minute(conn, ts_code: str, trade_date: dt.date, bars: List[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    sql = (
        "INSERT INTO market.kline_minute_raw (trade_time, ts_code, freq, open_li, high_li, low_li, close_li, volume_hand, amount_li, adjust_type, source) "
        "VALUES %s ON CONFLICT (ts_code, trade_time, freq) DO UPDATE SET "
        "open_li=EXCLUDED.open_li, high_li=EXCLUDED.high_li, low_li=EXCLUDED.low_li, close_li=EXCLUDED.close_li, volume_hand=EXCLUDED.volume_hand, amount_li=EXCLUDED.amount_li"
    )
    values: List[Tuple[Any, ...]] = []
    last_ts: Optional[str] = None
    for row in bars:
        if not isinstance(row, dict):
            continue
        trade_time = row.get("TradeTime") or row.get("trade_time") or row.get("Time") or row.get("time")
        trade_time_iso = _combine_trade_time(trade_date, trade_time)
        open_li = row.get("Open") or row.get("open")
        high_li = row.get("High") or row.get("high")
        low_li = row.get("Low") or row.get("low")
        close_li = row.get("Close") or row.get("close") or row.get("Price") or row.get("price")
        volume_hand = row.get("Volume") or row.get("volume") or 0
        amount_li = row.get("Amount") or row.get("amount") or 0
        if trade_time_iso is None or close_li is None:
            continue
        last_ts = trade_time_iso if last_ts is None or trade_time_iso > last_ts else last_ts
        values.append((trade_time_iso, ts_code, "1m", open_li, high_li, low_li, close_li, volume_hand, amount_li, "none", "tdx_api"))
    if not values:
        return 0, None
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values), last_ts


def create_run(conn, params: Dict[str, Any]) -> uuid.UUID:
    run_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_runs (run_id, mode, dataset, status, created_at, started_at, params)
            VALUES (%s, 'full', 'kline_minute_raw', 'running', NOW(), NOW(), %s)
            """,
            (run_id, json.dumps(params, ensure_ascii=False)),
        )
    return run_id


def start_job(conn, job_id: uuid.UUID, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status='running', started_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (json.dumps(summary, ensure_ascii=False), job_id),
        )
    return None


def finish_run(conn, run_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE market.ingestion_runs SET status=%s, finished_at=NOW(), summary=%s WHERE run_id=%s",
            (status, json.dumps(summary, ensure_ascii=False), run_id),
        )


def create_job(conn, job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
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


def finish_job(conn, job_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    """Finalize job row and merge summary into existing JSON instead of overwriting."""
    with conn.cursor() as cur:
        cur.execute("SELECT summary FROM market.ingestion_jobs WHERE job_id=%s", (job_id,))
        row = cur.fetchone()
        base: Dict[str, Any] = {}
        if row and row[0]:
            try:
                base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
            except Exception:
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

def update_job_summary(conn, job_id: uuid.UUID, patch: Dict[str, Any]) -> None:
    """Read-modify-write ingestion_jobs.summary to accumulate counters.

    实现与 ingest_full_daily_raw.py 中保持一致：
    - 先读取 summary JSON；
    - 对数值字段做累加，其它字段直接覆盖。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT summary FROM market.ingestion_jobs WHERE job_id=%s", (job_id,))
        row = cur.fetchone()
        base: Dict[str, Any] = {}
        if row and row[0]:
            try:
                base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
            except Exception:
                base = {}
        for k, v in (patch or {}).items():
            if isinstance(v, (int, float)) and isinstance(base.get(k), (int, float)):
                base[k] = type(base.get(k))(base.get(k, 0) + v)
            else:
                base[k] = v
        cur.execute(
            "UPDATE market.ingestion_jobs SET summary=%s WHERE job_id=%s",
            (json.dumps(base, ensure_ascii=False), job_id),
        )
        
def create_task(
    conn,
    job_id: uuid.UUID,
    dataset: str,
    ts_code: str,
    date_from: Optional[dt.date],
    date_to: Optional[dt.date],
) -> uuid.UUID:
    task_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_job_tasks (task_id, job_id, dataset, ts_code, date_from, date_to, status, progress)
            VALUES (%s, %s, %s, %s, %s, %s, 'running', 0)
            """,
            (task_id, job_id, dataset, ts_code, date_from, date_to),
        )
    return task_id


def complete_task(conn, task_id: uuid.UUID, success: bool, progress: float, last_error: Optional[str]) -> None:
    status = "success" if success else "failed"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_job_tasks
               SET status=%s, progress=%s, last_error=%s, updated_at=NOW()
             WHERE task_id=%s
            """,
            (status, progress, last_error, task_id),
        )


def upsert_state(
    conn,
    dataset: str,
    ts_code: str,
    last_date: Optional[dt.date],
    last_time: Optional[dt.datetime],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_state (dataset, ts_code, last_success_date, last_success_time)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (dataset, ts_code)
            DO UPDATE SET last_success_date=EXCLUDED.last_success_date,
                          last_success_time=EXCLUDED.last_success_time
            """,
            (dataset, ts_code, last_date, last_time),
        )


def log_ingestion(conn, job_id: uuid.UUID, level: str, message: str) -> None:
    """Print ingestion log line instead of inserting DB row.

    The scheduler will capture stdout when running this script as a
    subprocess and persist a single aggregated log entry per job.
    """

    level_up = str(level or "INFO").upper()
    print(f"[{level_up}] job_id={job_id} {message}")


def upsert_checkpoint(conn, run_id: uuid.UUID, ts_code: str, cursor_date: dt.date, cursor_time: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_checkpoints (run_id, dataset, ts_code, cursor_date, cursor_time, extra)
            VALUES (%s, 'kline_minute_raw', %s, %s, %s, NULL)
            ON CONFLICT (run_id, dataset, ts_code)
            DO UPDATE SET cursor_date=EXCLUDED.cursor_date, cursor_time=EXCLUDED.cursor_time, extra=EXCLUDED.extra
            """,
            (run_id, ts_code, cursor_date, cursor_time),
        )


def log_error(conn, run_id: uuid.UUID, ts_code: Optional[str], message: str, detail: Optional[Dict[str, Any]] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_errors (run_id, dataset, ts_code, message, detail) VALUES (%s, 'kline_minute_raw', %s, %s, %s)",
            (run_id, ts_code, message, json.dumps(detail, ensure_ascii=False) if detail else None),
        )


def chunked(codes: List[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(codes), size):
        yield codes[idx : idx + size]


def main() -> None:
    args = parse_args()
    exchanges = [ex.strip().lower() for ex in args.exchanges.split(",") if ex.strip()]
    invalid = [ex for ex in exchanges if ex != "all" and ex not in SUPPORTED_EXCHANGES]
    if invalid:
        print(f"[ERROR] unsupported exchanges: {invalid}")
        sys.exit(1)

    try:
        start_date = dt.date.fromisoformat(args.start_date)
        end_date = dt.date.fromisoformat(args.end_date)
    except ValueError:
        print("[ERROR] invalid start/end date format")
        sys.exit(1)
    if start_date > end_date:
        print("[ERROR] start-date later than end-date")
        sys.exit(1)

    if args.truncate:
        with psycopg2.connect(**DB_CFG) as conn0:
            conn0.autocommit = False
            with conn0.cursor() as cur:
                cur.execute("SET lock_timeout = '5s'")
                cur.execute("TRUNCATE TABLE market.kline_minute_raw")
            conn0.commit()
        print("[WARN] TRUNCATE market.kline_minute_raw executed by user request")

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '5min'")
        # 可选的批量写入调优，与日线脚本保持一致
        if getattr(args, "bulk_session_tune", False):
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off")
                cur.execute("SET work_mem = '256MB'")
        job_params = {
            "datasets": ["kline_minute_raw"],
            "exchanges": exchanges,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "batch_size": args.batch_size,
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            start_job(conn, job_id, job_params)
        else:
            job_id = create_job(conn, "init", job_params)
        log_ingestion(conn, job_id, "info", "start full minute ingestion job")

        params = {
            "exchanges": exchanges,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "batch_size": args.batch_size,
            "job_id": str(job_id),
        }
        run_id: Optional[uuid.UUID] = None
        job_finished = False
        try:
            run_id = create_run(conn, params)
            print(f"[INFO] job_id={job_id} run_id={run_id} created; fetching codes...")
            log_ingestion(conn, job_id, "info", f"run {run_id} start full minute ingestion")

            codes = fetch_codes(exchanges)
            if args.limit_codes is not None:
                codes = codes[: args.limit_codes]
            if not codes:
                print("[ERROR] no codes retrieved; aborting")
                finish_run(conn, run_id, "failed", {"reason": "no_codes"})
                finish_job(conn, job_id, "failed", {"run_id": str(run_id), "reason": "no_codes"})
                job_finished = True
                return

            stats = {
                "total_codes": len(codes),
                "processed_dates": 0,
                "success_codes": 0,
                "failed_codes": 0,
                "inserted_rows": 0,
            }
            update_job_summary(conn, job_id, {"total_codes": stats["total_codes"], "success_codes": 0, "failed_codes": 0, "inserted_rows": 0})

            total_days = (end_date - start_date).days + 1
            for batch in chunked(codes, args.batch_size):
                for ts_code in batch:
                    code = ts_code.split(".")[0]
                    task_id = create_task(conn, job_id, "kline_minute_raw", ts_code, start_date, end_date)
                    empty_streak = 0
                    code_failed = False
                    processed_days = 0
                    last_ts_dt: Optional[dt.datetime] = None
                    for trade_date in date_range(start_date, end_date):
                        try:
                            bars = fetch_minute(code, trade_date)
                            if not bars:
                                empty_streak += 1
                                if empty_streak >= args.max_empty:
                                    log_ingestion(conn, job_id, "info", f"run {run_id} {ts_code} empty streak={empty_streak}, stop")
                                    break
                                continue
                            empty_streak = 0
                            inserted, last_ts = upsert_minute(conn, ts_code, trade_date, bars)
                            stats["inserted_rows"] += inserted
                            stats["processed_dates"] += 1
                            processed_days += 1
                            upsert_checkpoint(conn, run_id, ts_code, trade_date, last_ts)
                            last_ts_dt = dt.datetime.fromisoformat(last_ts) if last_ts else None
                            if last_ts_dt:
                                upsert_state(conn, "kline_minute_raw", ts_code, trade_date, last_ts_dt)
                            update_job_summary(conn, job_id, {"inserted_rows": inserted})
                            print(f"[OK] {ts_code} {trade_date} inserted={inserted}")
                            log_ingestion(conn, job_id, "info", f"run {run_id} {ts_code} {trade_date} inserted={inserted}")
                        except Exception as exc:  # noqa: BLE001
                            code_failed = True
                            log_error(
                                conn,
                                run_id,
                                ts_code,
                                str(exc),
                                detail={"code": code, "trade_date": trade_date.isoformat()},
                            )
                            print(f"[WARN] {ts_code} {trade_date} failed: {exc}")
                            log_ingestion(conn, job_id, "error", f"run {run_id} {ts_code} {trade_date} failed: {exc}")
                            break
                    progress = 0.0
                    if total_days > 0:
                        progress = min(100.0, (processed_days / total_days) * 100.0)
                    if code_failed:
                        stats["failed_codes"] += 1
                        complete_task(conn, task_id, False, progress, "processing failed")
                        update_job_summary(conn, job_id, {"failed_codes": 1})
                    else:
                        stats["success_codes"] += 1
                        complete_task(conn, task_id, True, 100.0 if progress > 0 else progress, None)
                        update_job_summary(conn, job_id, {"success_codes": 1})

            status = "success" if stats["failed_codes"] == 0 else "failed"
            finish_run(conn, run_id, status, stats)
            run_summary = {"run_id": str(run_id), "stats": stats}
            finish_job(conn, job_id, status, run_summary)
            job_finished = True
            log_ingestion(
                conn,
                job_id,
                "info",
                f"run {run_id} finished status={status} stats={json.dumps(stats, ensure_ascii=False)}",
            )
            print(f"[DONE] job_id={job_id} run_id={run_id} status={status} stats={stats}")
        except Exception as exc:  # noqa: BLE001
            error_summary = {"error": str(exc)}
            if run_id is not None:
                finish_run(conn, run_id, "failed", error_summary)
                error_summary["run_id"] = str(run_id)
            if not job_finished:
                finish_job(conn, job_id, "failed", error_summary)
            log_ingestion(conn, job_id, "error", f"job {job_id} failed: {exc}")
            raise


if __name__ == "__main__":
    main()
