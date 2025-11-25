"""Full daily RAW ingestion from TDX API into TimescaleDB.

Iterates over all exchanges, fetches daily raw (no adjust) bars
from the TDX API, and upserts them into market.kline_daily_raw.
Tracks progress in ingestion_jobs/ingestion_job_tasks and updates
job summary incrementally so UI can show a live progress bar
(complete stocks / total A-shares).

Usage:
  python scripts/ingest_full_daily_raw.py --exchanges sh,sz --start-date 1990-01-01 --truncate
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TDX full daily RAW ingestion")
    parser.add_argument("--exchanges", type=str, default="sh,sz,bj", help="Comma separated exchanges (sh,sz,bj)")
    parser.add_argument("--start-date", type=str, default="1990-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=dt.date.today().isoformat(), help="End date (YYYY-MM-DD)")
    parser.add_argument("--batch-size", type=int, default=100, help="Codes per batch (sequential batches)")
    parser.add_argument("--limit-codes", type=int, default=None, help="Optional limit on number of codes to process")
    parser.add_argument("--truncate", action="store_true", help="TRUNCATE market.kline_daily_raw before run")
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
        except (req_exc.ConnectionError, req_exc.Timeout) as exc:
            last_exc = exc
            if attempt >= max_retries:
                break
            import time

            time.sleep(1 + attempt)
        except Exception:
            raise
    raise last_exc or RuntimeError(f"TDX API request failed after retries: {url}")


def normalize_ts_code(code: str) -> Optional[str]:
    code = (code or "").strip()
    if len(code) != 6 or not code.isdigit():
        return None
    if code.startswith("6"):
        suffix = "SH"
    elif code.startswith("8") or code.startswith("4"):
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
            if isinstance(item, dict):
                code = item.get("code") or item.get("Code")
            else:
                code = str(item)
            ts_code = normalize_ts_code(code)
            if ts_code and ts_code not in seen:
                seen.add(ts_code)
                result.append(ts_code)
    return result


def _to_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        try:
            return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            pass
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return text


def fetch_kline_daily_raw(code: str, start: str, end: str) -> List[Dict[str, Any]]:
    params = {"code": code, "type": "day"}
    data = http_get("/api/kline-all/tdx", params=params)
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        values = payload.get("list") or payload.get("List") or []
    else:
        values = payload or []

    if not values:
        return []

    start_date = start or ""
    end_date = end or ""
    selected: List[Tuple[str, Dict[str, Any]]] = []
    for row in values:
        trade_date = _to_date(row.get("Time") or row.get("Date") or row.get("time") or row.get("date"))
        if trade_date is None:
            continue
        if start_date and trade_date < start_date:
            continue
        if end_date and trade_date > end_date:
            continue
        selected.append((trade_date, dict(row)))

    selected.sort(key=lambda item: item[0])
    return [row for _, row in selected]


def upsert_kline_daily_raw(conn, ts_code: str, bars: List[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    sql = (
        "INSERT INTO market.kline_daily_raw (trade_date, ts_code, open_li, high_li, low_li, close_li, volume_hand, amount_li, adjust_type, source) "
        "VALUES %s ON CONFLICT (ts_code, trade_date) DO UPDATE SET "
        "open_li=EXCLUDED.open_li, high_li=EXCLUDED.high_li, low_li=EXCLUDED.low_li, close_li=EXCLUDED.close_li, volume_hand=EXCLUDED.volume_hand, amount_li=EXCLUDED.amount_li"
    )
    values: List[Tuple[Any, ...]] = []
    last_date: Optional[str] = None
    for row in bars:
        if not isinstance(row, dict):
            continue
        trade_date = _to_date(row.get("Date") or row.get("date") or row.get("Time") or row.get("time"))
        open_li = row.get("Open") or row.get("open")
        high_li = row.get("High") or row.get("high")
        low_li = row.get("Low") or row.get("low")
        close_li = row.get("Close") or row.get("close")
        volume_hand = row.get("Volume") or row.get("volume") or 0
        amount_li = row.get("Amount") or row.get("amount") or 0
        if trade_date is None or open_li is None or high_li is None or low_li is None or close_li is None:
            continue
        last_date = trade_date if last_date is None or trade_date > last_date else last_date
        values.append((trade_date, ts_code, open_li, high_li, low_li, close_li, volume_hand, amount_li, "none", "tdx_api"))
    if not values:
        return 0, None
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values), last_date


# ------------------------ DB helpers ------------------------

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
    return job_id


def finish_job(conn, job_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status=%s, finished_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (status, json.dumps(summary, ensure_ascii=False), job_id),
        )


def update_job_summary(conn, job_id: uuid.UUID, patch: Dict[str, Any]) -> None:
    # Read-modify-write summary JSON to accumulate counters
    with conn.cursor() as cur:
        cur.execute("SELECT summary FROM market.ingestion_jobs WHERE job_id=%s", (job_id,))
        row = cur.fetchone()
        base = {}
        if row and row[0]:
            try:
                base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
            except Exception:
                base = {}
        # merge/increment
        for k, v in (patch or {}).items():
            if isinstance(v, (int, float)) and isinstance(base.get(k), (int, float)):
                base[k] = type(base.get(k))(base.get(k, 0) + v)  # preserve type
            else:
                base[k] = v
        cur.execute(
            "UPDATE market.ingestion_jobs SET summary=%s WHERE job_id=%s",
            (json.dumps(base, ensure_ascii=False), job_id),
        )


def create_run(conn, params: Dict[str, Any]) -> uuid.UUID:
    run_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_runs (
                run_id, mode, dataset, status, created_at, started_at, params
            ) VALUES (%s, 'full', 'kline_daily_raw', 'running', NOW(), NOW(), %s)
            """,
            (run_id, json.dumps(params, ensure_ascii=False)),
        )
    return run_id


def finish_run(conn, run_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE market.ingestion_runs
                   SET status=%s, finished_at=NOW(), summary=%s
                 WHERE run_id=%s""",
            (status, json.dumps(summary, ensure_ascii=False), run_id),
        )


def create_task(
    conn,
    job_id: uuid.UUID,
    dataset: str,
    ts_code: str,
    date_from: Optional[str],
    date_to: Optional[str],
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
    last_date: Optional[str],
    last_time: Optional[str],
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

    The new backend uses scheduler-level aggregation for DB logs; the
    script itself only writes to stdout.
    """

    level_up = str(level or "INFO").upper()
    print(f"[{level_up}] job_id={job_id} {message}")


def upsert_checkpoint(conn, run_id: uuid.UUID, ts_code: str, cursor_date: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_checkpoints (run_id, dataset, ts_code, cursor_date, extra)
            VALUES (%s, 'kline_daily_raw', %s, %s, NULL)
            ON CONFLICT (run_id, dataset, ts_code)
            DO UPDATE SET cursor_date = EXCLUDED.cursor_date, extra = EXCLUDED.extra
            """,
            (run_id, ts_code, cursor_date),
        )


def chunked(iterable: List[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(iterable), size):
        yield iterable[idx : idx + size]


def main() -> None:
    args = parse_args()
    exchanges = [ex.strip().lower() for ex in args.exchanges.split(",") if ex.strip()]
    invalid = [ex for ex in exchanges if ex != "all" and ex not in SUPPORTED_EXCHANGES]
    if invalid:
        print(f"[ERROR] unsupported exchanges: {invalid}")
        sys.exit(1)

    try:
        dt.date.fromisoformat(args.start_date)
        dt.date.fromisoformat(args.end_date)
    except ValueError:
        print("[ERROR] start-date or end-date format invalid")
        sys.exit(1)

    if args.truncate:
        with psycopg2.connect(**DB_CFG) as conn0:
            conn0.autocommit = False
            with conn0.cursor() as cur:
                cur.execute("TRUNCATE TABLE market.kline_daily_raw")
            conn0.commit()
        print("[WARN] TRUNCATE market.kline_daily_raw executed by user request")

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '5min'")
        if args.bulk_session_tune:
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off")
                cur.execute("SET work_mem = '256MB'")
        job_params = {
            "datasets": ["kline_daily_raw"],
            "exchanges": exchanges,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "batch_size": args.batch_size,
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            start_job(conn, job_id, job_params)
        else:
            job_id = create_job(conn, "init", job_params)
        log_ingestion(conn, job_id, "info", "start full daily RAW ingestion job")

        params = {
            "exchanges": exchanges,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "batch_size": args.batch_size,
            "job_id": str(job_id),
        }
        run_id: Optional[uuid.UUID] = None
        job_finished = False
        try:
            run_id = create_run(conn, params)
            print(f"[INFO] job_id={job_id} run_id={run_id} created, fetching codes...")
            log_ingestion(conn, job_id, "info", f"run {run_id} start full daily RAW ingestion")

            codes = fetch_codes(exchanges)
            if args.limit_codes is not None:
                codes = codes[: args.limit_codes]
            if not codes:
                print("[ERROR] /api/codes returned no results; aborting")
                finish_run(conn, run_id, "failed", {"reason": "no_codes"})
                finish_job(conn, job_id, "failed", {"run_id": str(run_id), "reason": "no_codes"})
                job_finished = True
                return

            stats = {
                "total_codes": len(codes),
                "success_codes": 0,
                "failed_codes": 0,
                "inserted_rows": 0,
            }
            update_job_summary(conn, job_id, {"total_codes": stats["total_codes"], "success_codes": 0, "failed_codes": 0, "inserted_rows": 0})

            for batch in chunked(codes, args.batch_size):
                for ts_code in batch:
                    code = ts_code.split(".")[0]
                    task_id = create_task(conn, job_id, "kline_daily_raw", ts_code, args.start_date, args.end_date)
                    try:
                        bars = fetch_kline_daily_raw(code, args.start_date, args.end_date)
                        inserted, last_date = upsert_kline_daily_raw(conn, ts_code, bars)
                        stats["inserted_rows"] += inserted
                        stats["success_codes"] += 1
                        if last_date:
                            upsert_state(conn, "kline_daily_raw", ts_code, last_date, None)
                        upsert_checkpoint(conn, run_id, ts_code, last_date)
                        complete_task(conn, task_id, True, 100.0, None)
                        update_job_summary(conn, job_id, {"inserted_rows": inserted, "success_codes": 1})
                        print(f"[OK] {ts_code}: inserted={inserted} rows last_date={last_date}")
                        log_ingestion(conn, job_id, "info", f"run {run_id} {ts_code} inserted={inserted} last_date={last_date}")
                    except Exception as exc:  # noqa: BLE001
                        stats["failed_codes"] += 1
                        update_job_summary(conn, job_id, {"failed_codes": 1})
                        complete_task(conn, task_id, False, 0.0, str(exc))
                        print(f"[WARN] {ts_code} failed: {exc}")
                        log_ingestion(conn, job_id, "error", f"run {run_id} {ts_code} failed: {exc}")

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
