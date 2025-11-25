"""Incremental ingestion driver for TDX datasets.

Supports incremental updates for:
  - kline_daily_qfq: front-adjusted daily bars
  - kline_minute_raw: 1-minute raw bars

The script uses the ingestion control tables (ingestion_runs,
checkpoints, errors, state) to provide resumable, auditable updates.
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
from psycopg2 import errors as pg_errors
import requests
from requests import exceptions as req_exc
try:
    from tqdm import tqdm  # type: ignore
except Exception:  # noqa: BLE001
    tqdm = None  # type: ignore

pgx.register_uuid()

TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:8080")
DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)
# 支持增量更新的数据集：
# - kline_daily_qfq: 前复权日线
# - kline_daily_raw: 未复权日线（与 ingest_full_daily_raw 对齐）
# - kline_minute_raw: 1 分钟线
SUPPORTED_DATASETS = {"kline_daily_qfq", "kline_daily_raw", "kline_minute_raw"}
EXCHANGE_MAP = {"sh": "SH", "sz": "SZ", "bj": "BJ"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TDX incremental ingestion")
    parser.add_argument(
        "--datasets",
        type=str,
        default="kline_daily_qfq,kline_minute_raw",
        help="Comma separated datasets from {kline_daily_qfq,kline_daily_raw,kline_minute_raw}",
    )
    parser.add_argument("--date", type=str, default=dt.date.today().isoformat(), help="Target date YYYY-MM-DD")
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Optional override start date for daily dataset (YYYY-MM-DD)",
    )
    parser.add_argument("--exchanges", type=str, default="sh,sz", help="Comma separated exchanges (sh,sz,bj)")
    parser.add_argument("--batch-size", type=int, default=100, help="Codes per batch")
    parser.add_argument("--max-empty", type=int, default=2, help="Minute dataset: stop after N empty days")
    parser.add_argument("--job-id", type=str, default=None, help="Attach to existing job id (pre-created by backend)")
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
    elif code.startswith(("8", "4")):
        suffix = "BJ"
    else:
        suffix = "SZ"
    return f"{code}.{suffix}"


def fetch_codes(exchanges: Optional[Iterable[str]]) -> List[str]:
    targets = [ex.strip().lower() for ex in exchanges if ex] if exchanges else ["all"]
    result: List[str] = []
    seen = set()
    for exch in targets:
        params = {"exchange": exch} if exch and exch != "all" else {}
        try:
            data = http_get("/api/codes", params=params)
        except Exception as exc:  # noqa: BLE001 - bubble up for caller handling
            label = exch if exch else "all"
            print(f"[ERROR] 获取交易所 {label} 股票列表失败: {exc}")
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


def get_db_codes(conn, exchanges: Iterable[str]) -> List[str]:
    exchange_values = [EXCHANGE_MAP.get(ex.lower()) for ex in exchanges if ex.lower() in EXCHANGE_MAP]
    query = "SELECT ts_code FROM market.symbol_dim"
    params: Tuple[Any, ...] = ()
    if exchange_values:
        placeholders = ",".join(["%s"] * len(exchange_values))
        query += f" WHERE exchange IN ({placeholders})"
        params = tuple(exchange_values)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [row[0] for row in cur.fetchall()]


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def date_range(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


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


def fetch_daily(code: str, start: str, end: str) -> List[Dict[str, Any]]:
    params = {"code": code, "type": "day", "adjust": "qfq", "start": start, "end": end}
    data = http_get("/api/kline", params=params)
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        values = payload.get("List") or payload.get("list") or []
    else:
        values = payload or []
    return list(values)


def fetch_daily_raw(code: str, start: str, end: str) -> List[Dict[str, Any]]:
    """Fetch *unadjusted* daily bars for a single symbol within [start, end].

    与 ingest_full_daily_raw 中的 fetch_kline_daily_raw 保持语义一致：
    - 调用 /api/kline-all/tdx 获取该标的全部日线；
    - 在本地按日期范围进行过滤；
    - 返回按交易日期升序排列的列表。
    """
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


def fetch_minute(code: str, trade_date: dt.date) -> List[Dict[str, Any]]:
    params = {"code": code, "type": "minute1", "date": trade_date.strftime("%Y%m%d")}
    data = http_get("/api/minute", params=params)
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        items = payload.get("List") or payload.get("list") or payload
        if isinstance(items, dict):
            items = items.get("List") or items.get("list") or []
    else:
        items = payload or []
    return list(items)


def upsert_daily(conn, ts_code: str, bars: List[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    sql = (
        "INSERT INTO market.kline_daily_qfq (trade_date, ts_code, open_li, high_li, low_li, close_li, volume_hand, amount_li, adjust_type, source) "
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
        values.append((trade_date, ts_code, open_li, high_li, low_li, close_li, volume_hand, amount_li, "qfq", "tdx_api"))
    if not values:
        return 0, None
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values), last_date


def upsert_daily_raw(conn, ts_code: str, bars: List[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    """Upsert 未复权日线数据到 market.kline_daily_raw。

    逻辑与 ingest_full_daily_raw 中的 upsert_kline_daily_raw 保持一致：
    - adjust_type 固定为 'none'；
    - source 标记为 'tdx_api'；
    - ON CONFLICT 时更新价格与量。
    """
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


def _combine_trade_time(trade_date: dt.date, value: Any) -> Optional[str]:
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
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            time_obj = dt.datetime.strptime(text, fmt).time()
            tzinfo = dt.timezone(dt.timedelta(hours=8))
            return dt.datetime.combine(trade_date, time_obj).replace(tzinfo=tzinfo).isoformat()
        except ValueError:
            continue
    return None


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


def create_run(conn, dataset: str, params: Dict[str, Any]) -> uuid.UUID:
    run_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_runs (run_id, mode, dataset, status, created_at, started_at, params)
            VALUES (%s, 'incremental', %s, 'running', NOW(), NOW(), %s)
            """,
            (run_id, dataset, json.dumps(params, ensure_ascii=False)),
        )
    return run_id


def update_job_summary(conn, job_id: uuid.UUID, patch: Dict[str, Any]) -> None:
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
        cur.execute("UPDATE market.ingestion_jobs SET summary=%s WHERE job_id=%s", (json.dumps(base, ensure_ascii=False), job_id))


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


def finish_job(conn, job_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    """Merge final summary into existing ingestion_jobs.summary.

    保留最初 job 创建时写入的范围参数（如 date / start_date / exchanges），
    只在其基础上增加/覆盖 run_id、stats 等字段。
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
        base.update(summary or {})
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status=%s, finished_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (status, json.dumps(base, ensure_ascii=False), job_id),
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


def log_ingestion(conn, job_id: uuid.UUID, level: str, message: str) -> None:
    """Lightweight logging for CLI/debug runs.

    Aggregated ingestion logs are handled by the scheduler; here we only
    emit human-readable lines to stdout so they can be captured.
    """

    level_up = str(level or "INFO").upper()
    print(f"[{level_up}] job_id={job_id} {message}")


def get_state(conn, dataset: str, ts_code: str) -> Tuple[Optional[dt.date], Optional[dt.datetime]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_success_date, last_success_time FROM market.ingestion_state WHERE dataset=%s AND ts_code=%s",
            (dataset, ts_code),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        last_date = row[0]
        last_time = row[1]
        return last_date, last_time


def upsert_state(
    conn,
    dataset: str,
    ts_code: str,
    last_date: Optional[dt.date],
    last_time: Optional[str],
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_state (dataset, ts_code, last_success_date, last_success_time, extra)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dataset, ts_code)
            DO UPDATE SET last_success_date=EXCLUDED.last_success_date,
                          last_success_time=EXCLUDED.last_success_time,
                          extra=EXCLUDED.extra
            """,
            (dataset, ts_code, last_date, last_time, json.dumps(extra, ensure_ascii=False) if extra else None),
        )


def upsert_checkpoint(
    conn,
    run_id: uuid.UUID,
    dataset: str,
    ts_code: str,
    cursor_date: Optional[dt.date],
    cursor_time: Optional[str],
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_checkpoints (run_id, dataset, ts_code, cursor_date, cursor_time, extra)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, dataset, ts_code)
            DO UPDATE SET cursor_date=EXCLUDED.cursor_date,
                          cursor_time=EXCLUDED.cursor_time,
                          extra=EXCLUDED.extra
            """,
            (run_id, dataset, ts_code, cursor_date, cursor_time,
             json.dumps(extra, ensure_ascii=False) if extra else None),
        )


def log_error(
    conn,
    run_id: uuid.UUID,
    dataset: str,
    ts_code: Optional[str],
    message: str,
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_errors (run_id, dataset, ts_code, message, detail) VALUES (%s, %s, %s, %s, %s)",
            (run_id, dataset, ts_code, message, json.dumps(detail, ensure_ascii=False) if detail else None),
        )


def ingest_daily(
    conn,
    codes: List[str],
    target_date: dt.date,
    start_override: Optional[dt.date],
    batch_size: int,
    job_id_opt: Optional[str] = None,
) -> None:
    dataset = "kline_daily_qfq"
    params = {
        "target_date": target_date.isoformat(),
        "start_date_override": start_override.isoformat() if start_override else None,
        "batch_size": batch_size,
    }
    job_params = {"datasets": [dataset], **params}
    job_id = uuid.UUID(job_id_opt) if job_id_opt else create_job(conn, "incremental", job_params)
    if job_id_opt:
        start_job(conn, job_id, job_params)
    log_ingestion(conn, job_id, "info", "start incremental daily job")
    params["job_id"] = str(job_id)
    run_id = create_run(conn, dataset, params)
    stats = {"total_codes": 0, "success_codes": 0, "failed_codes": 0, "inserted_rows": 0}
    # Initialize summary counters for UI fallback
    update_job_summary(conn, job_id, {"total_codes": len(codes), "success_codes": 0, "failed_codes": 0, "inserted_rows": 0})
    pbar = None
    if tqdm is not None:
        try:
            pbar = tqdm(total=len(codes), desc="kline_daily_qfq incr", unit="code")
        except Exception:
            pbar = None
    for batch in chunked(codes, batch_size):
        for ts_code in batch:
            task_id = create_task(conn, job_id, dataset, ts_code, start_override, target_date)
            ok = False
            err: Optional[str] = None
            try:
                bars = fetch_daily(ts_code.split(".")[0], params["start_date_override"], target_date.isoformat())
                inserted, last_fetched = upsert_daily(conn, ts_code, bars)
                stats["inserted_rows"] += inserted
                if inserted > 0 and last_fetched:
                    new_last_date = dt.date.fromisoformat(last_fetched)
                    upsert_state(conn, dataset, ts_code, new_last_date, None, None)
                    upsert_checkpoint(conn, run_id, dataset, ts_code, new_last_date, None, None)
                stats["success_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"inserted_rows": int(inserted), "success_codes": 1})
                except Exception:
                    pass
                ok = True
                print(f"[OK] {dataset} {ts_code} inserted={inserted}")
                log_ingestion(conn, job_id, "info", f"run {run_id} {dataset} {ts_code} inserted={inserted}")
            except Exception as exc:  # noqa: BLE001
                # 任何写入/状态更新异常都需要先回滚当前事务，否则后续 SQL 会遇到
                # "current transaction is aborted" 错误。
                try:
                    conn.rollback()
                except Exception:
                    # 在 autocommit 模式下 rollback 可能会抛错，忽略即可
                    pass
                err = str(exc)
                stats["failed_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"failed_codes": 1})
                except Exception:
                    pass
                log_error(
                    conn,
                    run_id,
                    dataset,
                    ts_code,
                    err,
                    detail={"code": ts_code.split(".")[0], "start": params["start_date_override"], "end": target_date.isoformat()},
                )
                print(f"[WARN] {dataset} {ts_code} failed: {err}")
                log_ingestion(conn, job_id, "error", f"run {run_id} {dataset} {ts_code} failed: {err}")
            complete_task(conn, task_id, ok, 100.0 if ok else 0.0, None if ok else err)
            stats["total_codes"] += 1
            if pbar is not None:
                try:
                    pbar.update(1)
                except Exception:
                    pass
    if pbar is not None:
        try:
            pbar.close()
        except Exception:
            pass
    status = "success" if stats["failed_codes"] == 0 else "failed"
    finish_run(conn, run_id, status, stats)
    finish_job(conn, job_id, status, {"run_id": str(run_id), "stats": stats})
    log_ingestion(conn, job_id, "info", f"run {run_id} finished status={status} stats={json.dumps(stats, ensure_ascii=False)}")
    print(f"[DONE] daily status={status} stats={stats}")


def ingest_daily_raw(
    conn,
    codes: List[str],
    target_date: dt.date,
    start_override: Optional[dt.date],
    batch_size: int,
    job_id_opt: Optional[str] = None,
) -> None:
    """Incremental ingestion for kline_daily_raw (未复权日线)。"""
    dataset = "kline_daily_raw"
    params = {
        "target_date": target_date.isoformat(),
        "start_date_override": start_override.isoformat() if start_override else None,
        "batch_size": batch_size,
    }
    job_params = {"datasets": [dataset], **params}
    job_id = uuid.UUID(job_id_opt) if job_id_opt else create_job(conn, "incremental", job_params)
    if job_id_opt:
        start_job(conn, job_id, job_params)
    log_ingestion(conn, job_id, "info", "start incremental daily_raw job")
    params["job_id"] = str(job_id)
    run_id = create_run(conn, dataset, params)
    stats = {"total_codes": 0, "success_codes": 0, "failed_codes": 0, "inserted_rows": 0}
    update_job_summary(conn, job_id, {"total_codes": len(codes), "success_codes": 0, "failed_codes": 0, "inserted_rows": 0})
    pbar = None
    if tqdm is not None:
        try:
            pbar = tqdm(total=len(codes), desc="kline_daily_raw incr", unit="code")
        except Exception:
            pbar = None
    for batch in chunked(codes, batch_size):
        for ts_code in batch:
            task_id = create_task(conn, job_id, dataset, ts_code, start_override, target_date)
            ok = False
            err: Optional[str] = None
            try:
                bars = fetch_daily_raw(ts_code.split(".")[0], params["start_date_override"], target_date.isoformat())
                inserted, last_fetched = upsert_daily_raw(conn, ts_code, bars)
                stats["inserted_rows"] += inserted
                if inserted > 0 and last_fetched:
                    new_last_date = dt.date.fromisoformat(last_fetched)
                    upsert_state(conn, dataset, ts_code, new_last_date, None, None)
                    upsert_checkpoint(conn, run_id, dataset, ts_code, new_last_date, None, None)
                stats["success_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"inserted_rows": int(inserted), "success_codes": 1})
                except Exception:
                    pass
                ok = True
                print(f"[OK] {dataset} {ts_code} inserted={inserted}")
                log_ingestion(conn, job_id, "info", f"run {run_id} {dataset} {ts_code} inserted={inserted}")
            except Exception as exc:  # noqa: BLE001
                try:
                    conn.rollback()
                except Exception:
                    pass
                err = str(exc)
                stats["failed_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"failed_codes": 1})
                except Exception:
                    pass
                log_error(
                    conn,
                    run_id,
                    dataset,
                    ts_code,
                    err,
                    detail={"code": ts_code.split(".")[0], "start": params["start_date_override"], "end": target_date.isoformat()},
                )
                print(f"[WARN] {dataset} {ts_code} failed: {err}")
                log_ingestion(conn, job_id, "error", f"run {run_id} {dataset} {ts_code} failed: {err}")
            complete_task(conn, task_id, ok, 100.0 if ok else 0.0, None if ok else err)
            stats["total_codes"] += 1
            if pbar is not None:
                try:
                    pbar.update(1)
                except Exception:
                    pass
    if pbar is not None:
        try:
            pbar.close()
        except Exception:
            pass
    status = "success" if stats["failed_codes"] == 0 else "failed"
    finish_run(conn, run_id, status, stats)
    finish_job(conn, job_id, status, {"run_id": str(run_id), "stats": stats})
    log_ingestion(conn, job_id, "info", f"run {run_id} finished status={status} stats={json.dumps(stats, ensure_ascii=False)}")
    print(f"[DONE] daily_raw status={status} stats={stats}")


def ingest_minute(
    conn,
    codes: List[str],
    target_date: dt.date,
    start_override: Optional[dt.date],
    batch_size: int,
    max_empty: int,
    job_id_opt: Optional[str] = None,
) -> None:
    dataset = "kline_minute_raw"
    params = {
        "target_date": target_date.isoformat(),
        "start_date_override": start_override.isoformat() if start_override else None,
        "batch_size": batch_size,
        "max_empty": max_empty,
    }
    job_params = {"datasets": [dataset], **params}
    job_id = uuid.UUID(job_id_opt) if job_id_opt else create_job(conn, "incremental", job_params)
    if job_id_opt:
        start_job(conn, job_id, job_params)
    log_ingestion(conn, job_id, "info", "start incremental minute job")
    params["job_id"] = str(job_id)
    run_id = create_run(conn, dataset, params)
    stats = {"total_codes": 0, "success_codes": 0, "failed_codes": 0, "inserted_rows": 0}
    update_job_summary(conn, job_id, {"total_codes": len(codes), "success_codes": 0, "failed_codes": 0, "inserted_rows": 0})
    pbar = None
    if tqdm is not None:
        try:
            pbar = tqdm(total=len(codes), desc="kline_minute_raw incr", unit="code")
        except Exception:
            pbar = None
    for batch in chunked(codes, batch_size):
        for ts_code in batch:
            code = ts_code.split(".")[0]
            range_end = target_date
            if start_override is not None:
                range_start = start_override
            else:
                last_date, _last_time = get_state(conn, dataset, ts_code)
                if last_date is not None:
                    range_start = last_date + dt.timedelta(days=1)
                else:
                    range_start = target_date
            if range_start > range_end:
                task_id = create_task(conn, job_id, dataset, ts_code, range_start, range_end)
                complete_task(conn, task_id, True, 100.0, None)
                stats["total_codes"] += 1
                stats["success_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"success_codes": 1})
                except Exception:
                    pass
                if pbar is not None:
                    try:
                        pbar.update(1)
                    except Exception:
                        pass
                continue
            task_id = create_task(conn, job_id, dataset, ts_code, range_start, range_end)
            code_failed = False
            err: Optional[str] = None
            empty_streak = 0
            for trade_date in date_range(range_start, range_end):
                try:
                    bars = fetch_minute(code, trade_date)
                    if not bars:
                        empty_streak += 1
                        if empty_streak >= max_empty:
                            log_ingestion(
                                conn,
                                job_id,
                                "info",
                                f"run {run_id} {dataset} {ts_code} empty streak={empty_streak}, stop at {trade_date.isoformat()}",
                            )
                            break
                        continue
                    empty_streak = 0
                    inserted, last_ts = upsert_minute(conn, ts_code, trade_date, bars)
                    stats["inserted_rows"] += inserted
                    if inserted > 0:
                        last_dt = dt.datetime.fromisoformat(last_ts) if last_ts else None
                        upsert_state(conn, dataset, ts_code, trade_date, last_dt, None)
                        upsert_checkpoint(conn, run_id, dataset, ts_code, trade_date, last_ts, None)
                    print(f"[OK] {dataset} {ts_code} {trade_date} inserted={inserted}")
                    log_ingestion(conn, job_id, "info", f"run {run_id} {dataset} {ts_code} {trade_date} inserted={inserted}")
                except Exception as exc:  # noqa: BLE001
                    err = str(exc)
                    code_failed = True
                    log_error(
                        conn,
                        run_id,
                        dataset,
                        ts_code,
                        err,
                        detail={
                            "code": code,
                            "date": trade_date.isoformat(),
                            "range_start": range_start.isoformat(),
                            "range_end": range_end.isoformat(),
                        },
                    )
                    print(f"[WARN] {dataset} {ts_code} {trade_date} failed: {err}")
                    log_ingestion(
                        conn,
                        job_id,
                        "error",
                        f"run {run_id} {dataset} {ts_code} {trade_date} failed: {err}",
                    )
                    break
            if code_failed:
                stats["failed_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"failed_codes": 1})
                except Exception:
                    pass
            else:
                stats["success_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"success_codes": 1})
                except Exception:
                    pass
            ok = not code_failed
            complete_task(conn, task_id, ok, 100.0 if ok else 0.0, None if ok else err)
            stats["total_codes"] += 1
            if pbar is not None:
                try:
                    pbar.update(1)
                except Exception:
                    pass
    if pbar is not None:
        try:
            pbar.close()
        except Exception:
            pass
    status = "success" if stats["failed_codes"] == 0 else "failed"
    finish_run(conn, run_id, status, stats)
    finish_job(conn, job_id, status, {"run_id": str(run_id), "stats": stats})
    log_ingestion(conn, job_id, "info", f"run {run_id} finished status={status} stats={json.dumps(stats, ensure_ascii=False)}")
    print(f"[DONE] minute status={status} stats={stats}")


def main() -> None:
    args = parse_args()
    try:
        target_date = dt.date.fromisoformat(args.date)
    except ValueError:
        print("[ERROR] invalid --date format")
        sys.exit(1)

    start_override = None
    if args.start_date:
        try:
            start_override = dt.date.fromisoformat(args.start_date)
        except ValueError:
            print("[ERROR] invalid --start-date format")
            sys.exit(1)

    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    invalid = [d for d in datasets if d not in SUPPORTED_DATASETS]
    if invalid:
        print(f"[ERROR] unsupported datasets: {invalid}")
        sys.exit(1)

    exchanges = [ex.strip().lower() for ex in args.exchanges.split(",") if ex.strip()]

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '5min'")
        if args.bulk_session_tune:
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off")
                cur.execute("SET work_mem = '256MB'")
        codes = get_db_codes(conn, exchanges)
        if not codes:
            print("[ERROR] no codes found in market.symbol_dim; run symbol ingestion first")
            sys.exit(1)

        if "kline_daily_qfq" in datasets:
            ingest_daily(conn, codes, target_date, start_override, args.batch_size, args.job_id)

        if "kline_daily_raw" in datasets:
            ingest_daily_raw(conn, codes, target_date, start_override, args.batch_size, args.job_id)

        if "kline_minute_raw" in datasets:
            ingest_minute(conn, codes, target_date, start_override, args.batch_size, args.max_empty, args.job_id)


if __name__ == "__main__":
    main()
