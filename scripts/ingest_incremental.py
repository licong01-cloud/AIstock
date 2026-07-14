"""Incremental ingestion driver for TDX datasets.

Supports incremental updates for:
  - kline_daily_raw: unadjusted daily bars
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
import requests
from requests import exceptions as req_exc
try:
    from tqdm import tqdm  # type: ignore
except Exception:  # noqa: BLE001
    tqdm = None  # type: ignore

pgx.register_uuid()

TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:19080")
DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-ingest-incremental",
)
# 支持增量更新的数据集：
# - kline_daily_raw: 未复权日线（与 ingest_full_daily_raw 对齐）
# - kline_minute_raw: 1 分钟线
SUPPORTED_DATASETS = {"kline_daily_raw", "kline_minute_raw"}
EXPECTED_MINUTE_BARS_PER_TRADING_DAY = 240
CHINA_TZ = dt.timezone(dt.timedelta(hours=8))
EXCHANGE_MAP = {"sh": "SH", "sz": "SZ", "bj": "BJ"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TDX incremental ingestion")
    parser.add_argument(
        "--datasets",
        type=str,
        default="kline_daily_raw,kline_minute_raw",
        help="Comma separated datasets from {kline_daily_raw,kline_minute_raw}",
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
    parser.add_argument(
        "--max-empty",
        type=int,
        default=0,
        help="Minute dataset: stop after N empty days; <=0 disables early stop (scan full date range)",
    )
    parser.add_argument("--job-id", type=str, default=None, help="Attach to existing job id (pre-created by backend)")
    parser.add_argument("--bulk-session-tune", action="store_true", help="Apply session-level tuning for bulk load")
    parser.add_argument("--workers", type=int, default=1, choices=[1, 2, 4, 8], help="Number of parallel workers (1 = no parallelism)")
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
    query = "SELECT ts_code FROM market.stock_basic"
    params: Tuple[Any, ...] = ()
    if exchange_values:
        placeholders = ",".join(["%s"] * len(exchange_values))
        query += f" WHERE exchange IN ({placeholders})"
        params = tuple(exchange_values)
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return [r[0] for r in rows]


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


def fetch_daily_raw(code: str, start: str, end: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetch *unadjusted* daily bars for a single symbol within [start, end].

    与 ingest_full_daily_raw 中的 fetch_kline_daily_raw 保持语义一致：
    - 调用 /api/kline-all/tdx 获取该标的全部日线（如提供 limit，则仅取最近 N 条）；
    - 在本地按日期范围进行过滤；
    - 返回按交易日期升序排列的列表。
    """
    params: Dict[str, Any] = {"code": code, "type": "day"}
    if limit is not None:
        try:
            parsed_limit = int(limit)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid daily kline limit: {limit!r}") from exc
        if parsed_limit <= 0:
            raise ValueError(f"daily kline limit must be positive: {parsed_limit}")
        params["limit"] = parsed_limit
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


def fetch_minute_range(code: str, start: dt.date, end: dt.date) -> List[Dict[str, Any]]:
    """Fetch true 1-minute OHLC bars for a symbol within [start, end].

    Uses /api/kline-all/tdx and filters locally because the legacy history endpoint ignores dates.
    """
    if start > end:
        raise ValueError(f"minute range start {start} is after end {end}")
    params = {"code": code, "type": "minute1"}
    data = http_get("/api/kline-all/tdx", params=params)
    if not isinstance(data, dict) or "data" not in data:
        raise RuntimeError("TDX /api/kline-all/tdx response is missing data")
    payload = data["data"]
    if isinstance(payload, dict):
        if "list" in payload:
            items = payload["list"]
        elif "List" in payload:
            items = payload["List"]
        else:
            raise RuntimeError("TDX /api/kline-all/tdx data is missing list")
    elif isinstance(payload, list):
        items = payload
    else:
        raise RuntimeError("TDX /api/kline-all/tdx data has invalid schema")
    if not isinstance(items, list):
        raise RuntimeError("TDX /api/kline-all/tdx list has invalid schema")

    selected: List[Tuple[str, str, Dict[str, Any]]] = []
    malformed_rows: List[int] = []
    start_key = start.isoformat()
    end_key = end.isoformat()
    for index, row in enumerate(items):
        if not isinstance(row, dict):
            malformed_rows.append(index)
            continue
        trade_time = row.get("TradeTime") or row.get("trade_time") or row.get("Time") or row.get("time")
        trade_date = _to_date(trade_time)
        if trade_date is None:
            malformed_rows.append(index)
            continue
        try:
            dt.date.fromisoformat(trade_date)
        except ValueError:
            malformed_rows.append(index)
            continue
        if trade_date < start_key or trade_date > end_key:
            continue
        selected.append((trade_date, str(trade_time or ""), dict(row)))

    if malformed_rows:
        raise RuntimeError(
            "TDX /api/kline-all/tdx returned rows without a valid trade timestamp: "
            f"count={len(malformed_rows)} sample_indexes={malformed_rows[:5]}"
        )

    selected.sort(key=lambda item: (item[0], item[1]))
    return [row for _, _, row in selected]


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
            dt_obj = dt_obj.replace(tzinfo=CHINA_TZ)
        return dt_obj.isoformat()
    except ValueError:
        pass
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            time_obj = dt.datetime.strptime(text, fmt).time()
            return dt.datetime.combine(trade_date, time_obj).replace(tzinfo=CHINA_TZ).isoformat()
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
    invalid_rows: List[Dict[str, Any]] = []
    seen_trade_times: set[str] = set()
    last_ts: Optional[str] = None
    for index, row in enumerate(bars):
        if not isinstance(row, dict):
            invalid_rows.append({"index": index, "reason": "row_not_object"})
            continue
        trade_time = row.get("TradeTime") or row.get("trade_time") or row.get("Time") or row.get("time")
        trade_time_iso = _combine_trade_time(trade_date, trade_time)
        open_li = row["Open"] if "Open" in row else row.get("open")
        high_li = row["High"] if "High" in row else row.get("high")
        low_li = row["Low"] if "Low" in row else row.get("low")
        close_li = row["Close"] if "Close" in row else row.get("close")
        volume_hand = row["Volume"] if "Volume" in row else row.get("volume")
        amount_li = row["Amount"] if "Amount" in row else row.get("amount")
        missing_fields = [
            field
            for field, value in (
                ("trade_time", trade_time_iso),
                ("open", open_li),
                ("high", high_li),
                ("low", low_li),
                ("close", close_li),
                ("volume", volume_hand),
                ("amount", amount_li),
            )
            if value is None
        ]
        if missing_fields:
            invalid_rows.append({"index": index, "reason": "missing_required_fields", "fields": missing_fields})
            continue
        parsed_trade_time = dt.datetime.fromisoformat(trade_time_iso)
        if parsed_trade_time.astimezone(CHINA_TZ).date() != trade_date:
            invalid_rows.append(
                {
                    "index": index,
                    "reason": "trade_date_mismatch",
                    "trade_time": trade_time_iso,
                    "expected_date": trade_date.isoformat(),
                }
            )
            continue
        if trade_time_iso in seen_trade_times:
            invalid_rows.append({"index": index, "reason": "duplicate_trade_time", "trade_time": trade_time_iso})
            continue
        seen_trade_times.add(trade_time_iso)
        last_ts = trade_time_iso if last_ts is None or trade_time_iso > last_ts else last_ts
        values.append((trade_time_iso, ts_code, "1m", open_li, high_li, low_li, close_li, volume_hand, amount_li, "none", "tdx_api"))
    if invalid_rows:
        raise ValueError(
            f"invalid minute OHLC payload for {ts_code} {trade_date}: "
            f"count={len(invalid_rows)} samples={invalid_rows[:5]}"
        )
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
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"ingestion job {job_id} has invalid summary JSON") from exc
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
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"ingestion job {job_id} has invalid summary JSON") from exc
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


def _stats_add(stats: Dict[str, Any], key: str, value: int) -> None:
    current = stats.get(key, 0)
    stats[key] = int(current) + int(value) if isinstance(current, (int, float)) else int(value)


def is_trading_day(conn, trade_date: dt.date) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT is_trading
              FROM market.trading_calendar
             WHERE cal_date=%s
            """,
            (trade_date,),
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"market.trading_calendar has no authority row for {trade_date}")
    if not isinstance(row[0], bool):
        raise RuntimeError(f"market.trading_calendar has invalid is_trading for {trade_date}: {row[0]!r}")
    return bool(row[0])


def get_expected_minute_codes(conn, trade_date: dt.date, ts_codes: List[str]) -> List[str]:
    if not ts_codes:
        return []
    requested_codes = sorted(set(ts_codes))
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts_code, list_date, delist_date
              FROM market.stock_basic
             WHERE ts_code = ANY(%s)
             ORDER BY ts_code
            """,
            (requested_codes,),
        )
        stock_rows = cur.fetchall()

    stock_by_code = {str(row[0]): row for row in stock_rows if row and row[0]}
    missing_codes = [ts_code for ts_code in requested_codes if ts_code not in stock_by_code]
    if missing_codes:
        raise RuntimeError(
            "market.stock_basic is missing requested TDX stock codes: "
            f"count={len(missing_codes)} samples={missing_codes[:10]}"
        )

    active_codes: List[str] = []
    invalid_list_dates: List[str] = []
    for ts_code in requested_codes:
        row = stock_by_code[ts_code]
        list_date = row[1]
        delist_date = row[2]
        if isinstance(list_date, dt.datetime):
            list_date = list_date.date()
        elif isinstance(list_date, str):
            list_date = dt.date.fromisoformat(list_date)
        if isinstance(delist_date, dt.datetime):
            delist_date = delist_date.date()
        elif isinstance(delist_date, str):
            delist_date = dt.date.fromisoformat(delist_date)
        if not isinstance(list_date, dt.date):
            invalid_list_dates.append(ts_code)
            continue
        if list_date <= trade_date and (delist_date is None or delist_date > trade_date):
            active_codes.append(ts_code)

    if invalid_list_dates:
        raise RuntimeError(
            "market.stock_basic has no valid list_date for requested codes: "
            f"count={len(invalid_list_dates)} samples={invalid_list_dates[:10]}"
        )
    if not active_codes:
        return []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ts_code
              FROM market.suspend_d
             WHERE trade_date=%s
               AND ts_code = ANY(%s)
               AND suspend_type='S'
            """,
            (trade_date, active_codes),
        )
        suspended_codes = {str(row[0]) for row in cur.fetchall() if row and row[0]}
    return [ts_code for ts_code in active_codes if ts_code not in suspended_codes]


def get_minute_day_stats(conn, trade_date: dt.date, ts_codes: List[str]) -> Dict[str, Dict[str, Any]]:
    if not ts_codes:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH localized AS (
                SELECT ts_code,
                       (trade_time AT TIME ZONE 'Asia/Shanghai')::date AS trade_date,
                       (trade_time AT TIME ZONE 'Asia/Shanghai')::time AS trade_time_local,
                       open_li, high_li, low_li, close_li, volume_hand, amount_li
                  FROM market.kline_minute_raw
                 WHERE freq='1m'
                   AND ts_code = ANY(%s)
                   AND trade_time >= (%s::date + TIME '00:00') AT TIME ZONE 'Asia/Shanghai'
                   AND trade_time < ((%s::date + 1) + TIME '00:00') AT TIME ZONE 'Asia/Shanghai'
            )
            SELECT ts_code,
                   COUNT(*)::int AS total_bars,
                   COUNT(*) FILTER (
                       WHERE EXTRACT(SECOND FROM trade_time_local)=0
                         AND (
                              (trade_time_local BETWEEN TIME '09:31' AND TIME '11:30')
                              OR (trade_time_local BETWEEN TIME '13:01' AND TIME '15:00')
                         )
                   )::int AS core_session_bars,
                   COUNT(*) FILTER (WHERE trade_time_local=TIME '09:30')::int AS opening_bar_count,
                   COUNT(*) FILTER (
                       WHERE NOT (
                           (
                               EXTRACT(SECOND FROM trade_time_local)=0
                               AND (
                                   (trade_time_local BETWEEN TIME '09:31' AND TIME '11:30')
                                   OR (trade_time_local BETWEEN TIME '13:01' AND TIME '15:00')
                               )
                           )
                           OR trade_time_local=TIME '09:30'
                       )
                   )::int AS unexpected_session_bars,
                   COUNT(*) FILTER (
                       WHERE open_li IS NULL OR high_li IS NULL OR low_li IS NULL OR close_li IS NULL
                          OR volume_hand IS NULL OR amount_li IS NULL
                   )::int AS invalid_payload_bars,
                   MIN(trade_time_local)::text AS first_time,
                   MAX(trade_time_local)::text AS last_time
              FROM localized
             GROUP BY ts_code
            """,
            (list(ts_codes), trade_date, trade_date),
        )
        rows = cur.fetchall()
    return {
        str(row[0]): {
            "total_bars": int(row[1]),
            "core_session_bars": int(row[2]),
            "opening_bar_count": int(row[3]),
            "unexpected_session_bars": int(row[4]),
            "invalid_payload_bars": int(row[5]),
            "first_time": str(row[6]) if row[6] is not None else None,
            "last_time": str(row[7]) if row[7] is not None else None,
        }
        for row in rows
        if row and row[0] is not None
    }


def find_minute_day_gaps(
    conn,
    trade_date: dt.date,
    expected_codes: List[str],
    expected_bars: int = EXPECTED_MINUTE_BARS_PER_TRADING_DAY,
) -> List[Dict[str, Any]]:
    day_stats = get_minute_day_stats(conn, trade_date, expected_codes)
    gaps: List[Dict[str, Any]] = []
    for ts_code in expected_codes:
        observed = day_stats.get(
            ts_code,
            {
                "total_bars": 0,
                "core_session_bars": 0,
                "opening_bar_count": 0,
                "unexpected_session_bars": 0,
                "invalid_payload_bars": 0,
                "first_time": None,
                "last_time": None,
            },
        )
        opening_bar_count = int(observed["opening_bar_count"])
        expected_total = expected_bars + opening_bar_count
        is_complete = (
            int(observed["core_session_bars"]) == expected_bars
            and opening_bar_count in (0, 1)
            and int(observed["total_bars"]) == expected_total
            and int(observed["unexpected_session_bars"]) == 0
            and int(observed["invalid_payload_bars"]) == 0
            and observed["last_time"] == "15:00:00"
            and observed["first_time"] == ("09:30:00" if opening_bar_count else "09:31:00")
        )
        if not is_complete:
            gaps.append(
                {
                    "ts_code": ts_code,
                    "actual_bars": int(observed["total_bars"]),
                    "expected_bars": expected_bars,
                    "accepted_total_bars": [expected_bars, expected_bars + 1],
                    **observed,
                }
            )
    return gaps


def filter_minute_bars_for_date(values: List[Dict[str, Any]], trade_date: dt.date) -> List[Dict[str, Any]]:
    target = trade_date.isoformat()
    selected: List[Dict[str, Any]] = []
    for row in values:
        if not isinstance(row, dict):
            continue
        trade_time = row.get("TradeTime") or row.get("trade_time") or row.get("Time") or row.get("time")
        if _to_date(trade_time) == target:
            selected.append(row)
    return selected


def retry_minute_day_gaps(
    conn,
    run_id: uuid.UUID,
    job_id: uuid.UUID,
    dataset: str,
    trade_date: dt.date,
    gaps: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> List[Dict[str, Any]]:
    failures: List[Dict[str, Any]] = []
    for gap in gaps:
        ts_code = str(gap["ts_code"])
        code = ts_code.split(".")[0]
        try:
            values = fetch_minute_range(code, trade_date, trade_date)
            day_bars = filter_minute_bars_for_date(values, trade_date)
            if not day_bars:
                log_error(
                    conn,
                    run_id,
                    dataset,
                    ts_code,
                    "minute completeness retry returned no bars",
                    detail={"date": trade_date.isoformat(), **gap},
                )
                continue
            inserted, last_ts = upsert_minute(conn, ts_code, trade_date, day_bars)
            _stats_add(stats, "inserted_rows", inserted)
            if inserted > 0:
                last_dt = dt.datetime.fromisoformat(last_ts) if last_ts else None
                upsert_state(conn, dataset, ts_code, trade_date, last_dt, None)
                upsert_checkpoint(
                    conn,
                    run_id,
                    dataset,
                    ts_code,
                    trade_date,
                    last_ts,
                    {"repair": "minute_completeness", "actual_bars_before": gap.get("actual_bars")},
                )
                try:
                    update_job_summary(conn, job_id, {"inserted_rows": int(inserted)})
                except Exception as summary_exc:  # noqa: BLE001
                    log_ingestion(
                        conn,
                        job_id,
                        "warning",
                        f"run {run_id} {dataset} {ts_code} {trade_date} job_summary_update failed: {summary_exc}",
                    )
            log_ingestion(
                conn,
                job_id,
                "info",
                f"run {run_id} {dataset} {ts_code} {trade_date} completeness_retry inserted={inserted}",
            )
        except Exception as exc:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception as rollback_exc:  # noqa: BLE001
                log_ingestion(
                    conn,
                    job_id,
                    "warning",
                    f"run {run_id} {dataset} {ts_code} {trade_date} rollback failed after retry error: {rollback_exc}",
                )
            err = str(exc)
            log_error(
                conn,
                run_id,
                dataset,
                ts_code,
                f"minute completeness retry failed: {err}",
                detail={"date": trade_date.isoformat(), **gap},
            )
            log_ingestion(
                conn,
                job_id,
                "error",
                f"run {run_id} {dataset} {ts_code} {trade_date} completeness_retry failed: {err}",
            )
            failures.append(
                {
                    **gap,
                    "reason": "completeness_retry_failed",
                    "error": err,
                }
            )
    return failures


def validate_minute_day_and_repair(
    conn,
    run_id: uuid.UUID,
    job_id: uuid.UUID,
    dataset: str,
    ts_codes: List[str],
    trade_date: dt.date,
    stats: Dict[str, Any],
    expected_bars: int = EXPECTED_MINUTE_BARS_PER_TRADING_DAY,
) -> List[Dict[str, Any]]:
    try:
        if not is_trading_day(conn, trade_date):
            return []
        expected_codes = get_expected_minute_codes(conn, trade_date, ts_codes)
    except Exception as exc:  # noqa: BLE001
        detail = {
            "date": trade_date.isoformat(),
            "reason": "minute_completeness_authority_unavailable",
            "error": str(exc),
        }
        log_error(conn, run_id, dataset, None, "minute completeness authority unavailable", detail=detail)
        log_ingestion(
            conn,
            job_id,
            "error",
            f"run {run_id} {dataset} {trade_date} completeness authority unavailable: {exc}",
        )
        _stats_add(stats, "completeness_failed_codes", max(len(ts_codes), 1))
        return [{"ts_code": None, "actual_bars": 0, "expected_bars": expected_bars, **detail}]

    if not expected_codes:
        log_ingestion(
            conn,
            job_id,
            "info",
            f"run {run_id} {dataset} {trade_date} completeness skipped: no listed non-suspended requested codes",
        )
        return []

    try:
        gaps = find_minute_day_gaps(conn, trade_date, expected_codes, expected_bars)
    except Exception as exc:  # noqa: BLE001
        detail = {
            "date": trade_date.isoformat(),
            "reason": "minute_completeness_readback_failed",
            "error": str(exc),
        }
        log_error(conn, run_id, dataset, None, "minute completeness readback failed", detail=detail)
        log_ingestion(
            conn,
            job_id,
            "error",
            f"run {run_id} {dataset} {trade_date} completeness readback failed: {exc}",
        )
        _stats_add(stats, "completeness_failed_codes", len(expected_codes))
        return [
            {"ts_code": ts_code, "actual_bars": 0, "expected_bars": expected_bars, **detail}
            for ts_code in expected_codes
        ]
    if not gaps:
        log_ingestion(conn, job_id, "info", f"run {run_id} {dataset} {trade_date} completeness ok codes={len(expected_codes)}")
        return []

    _stats_add(stats, "completeness_initial_gap_codes", len(gaps))
    retry_failures = retry_minute_day_gaps(conn, run_id, job_id, dataset, trade_date, gaps, stats)
    try:
        remaining = find_minute_day_gaps(conn, trade_date, expected_codes, expected_bars)
    except Exception as exc:  # noqa: BLE001
        detail = {
            "date": trade_date.isoformat(),
            "reason": "minute_completeness_post_retry_readback_failed",
            "error": str(exc),
        }
        log_error(conn, run_id, dataset, None, "minute completeness post-retry readback failed", detail=detail)
        remaining = [
            {"ts_code": ts_code, "actual_bars": 0, "expected_bars": expected_bars, **detail}
            for ts_code in expected_codes
        ]
    remaining_codes = {str(gap.get("ts_code")) for gap in remaining}
    remaining.extend(
        failure
        for failure in retry_failures
        if str(failure.get("ts_code")) not in remaining_codes
    )
    if remaining:
        _stats_add(stats, "completeness_failed_codes", len(remaining))
        for gap in remaining:
            log_error(
                conn,
                run_id,
                dataset,
                gap.get("ts_code"),
                "minute completeness check failed after retry",
                detail={"date": trade_date.isoformat(), **gap},
            )
        log_ingestion(
            conn,
            job_id,
            "error",
            f"run {run_id} {dataset} {trade_date} completeness failed gaps={json.dumps(remaining, ensure_ascii=False)}",
        )
    else:
        _stats_add(stats, "completeness_repaired_codes", len(gaps))
        log_ingestion(conn, job_id, "info", f"run {run_id} {dataset} {trade_date} completeness repaired codes={len(gaps)}")
    return remaining


def validate_minute_range_and_repair(
    conn,
    run_id: uuid.UUID,
    job_id: uuid.UUID,
    dataset: str,
    ts_codes: List[str],
    start_date: dt.date,
    end_date: dt.date,
    stats: Dict[str, Any],
) -> List[Dict[str, Any]]:
    remaining: List[Dict[str, Any]] = []
    for trade_date in date_range(start_date, end_date):
        remaining.extend(validate_minute_day_and_repair(conn, run_id, job_id, dataset, ts_codes, trade_date, stats))
    return remaining


def ingest_daily_raw(
    conn,
    codes: List[str],
    target_date: dt.date,
    start_override: Optional[dt.date],
    batch_size: int,
    job_id_opt: Optional[str] = None,
) -> bool:
    """Incremental ingestion for kline_daily_raw (未复权日线)。"""
    dataset = "kline_daily_raw"
    params = {
        "target_date": target_date.isoformat(),
        "start_date_override": start_override.isoformat() if start_override else None,
        "batch_size": batch_size,
    }
    # 根据交易日历估算本次需要的未复权日线条数，用于 /api/kline-all/tdx 的 limit，
    # 尤其是距离当前交易日小于一年的增量区间，避免每次都全量拉取。
    raw_limit: Optional[int] = None
    start_str = params.get("start_date_override")
    if start_str:
        try:
            start_dt = dt.date.fromisoformat(start_str)
        except ValueError:
            start_dt = None
        if start_dt is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(cal_date) AS latest
                      FROM market.trading_calendar
                     WHERE is_trading = TRUE
                    """
                )
                row = cur.fetchone()
                latest = row[0] if row and row[0] else None
                if latest is not None and start_dt <= latest:
                    # 仅当起始日期距离当前最新交易日不超过约一年时，才使用 limit 做近似范围切片，
                    # 更长区间仍采用全量拉取以避免遗漏早期数据。
                    if (latest - start_dt).days <= 366:
                        cur.execute(
                            """
                            SELECT COUNT(*)
                              FROM market.trading_calendar
                             WHERE is_trading = TRUE
                               AND cal_date BETWEEN %s AND %s
                            """,
                            (start_dt, latest),
                        )
                        count_row = cur.fetchone()
                        days = int(count_row[0]) if count_row and count_row[0] is not None else 0
                        if days > 0:
                            # 略微增加冗余，确保覆盖到起始日前后的边界情况。
                            raw_limit = days + 5
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
        except Exception as pbar_exc:  # noqa: BLE001
            log_ingestion(conn, job_id, "warning", f"run {run_id} daily progress initialization failed: {pbar_exc}")
            pbar = None
    for batch in chunked(codes, batch_size):
        for ts_code in batch:
            task_id = create_task(conn, job_id, dataset, ts_code, start_override, target_date)
            ok = False
            err: Optional[str] = None
            try:
                bars = fetch_daily_raw(
                    ts_code.split(".")[0],
                    params["start_date_override"],
                    target_date.isoformat(),
                    raw_limit,
                )
                inserted, last_fetched = upsert_daily_raw(conn, ts_code, bars)
                stats["inserted_rows"] += inserted
                if inserted > 0 and last_fetched:
                    new_last_date = dt.date.fromisoformat(last_fetched)
                    upsert_state(conn, dataset, ts_code, new_last_date, None, None)
                    upsert_checkpoint(conn, run_id, dataset, ts_code, new_last_date, None, None)
                stats["success_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"inserted_rows": int(inserted), "success_codes": 1})
                except Exception as summary_exc:  # noqa: BLE001
                    log_ingestion(
                        conn,
                        job_id,
                        "warning",
                        f"run {run_id} {dataset} {ts_code} job_summary_update failed: {summary_exc}",
                    )
                ok = True
                log_ingestion(conn, job_id, "info", f"run {run_id} {dataset} {ts_code} inserted={inserted}")
            except Exception as exc:  # noqa: BLE001
                try:
                    conn.rollback()
                except Exception as rollback_exc:  # noqa: BLE001
                    log_ingestion(
                        conn,
                        job_id,
                        "warning",
                        f"run {run_id} {dataset} {ts_code} rollback failed after ingestion error: {rollback_exc}",
                    )
                err = str(exc)
                stats["failed_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"failed_codes": 1})
                except Exception as summary_exc:  # noqa: BLE001
                    log_ingestion(
                        conn,
                        job_id,
                        "warning",
                        f"run {run_id} {dataset} {ts_code} job_summary_update failed: {summary_exc}",
                    )
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
                except Exception as pbar_exc:  # noqa: BLE001
                    log_ingestion(conn, job_id, "warning", f"run {run_id} daily progress update failed: {pbar_exc}")
                    pbar = None
    if pbar is not None:
        try:
            pbar.close()
        except Exception as pbar_exc:  # noqa: BLE001
            log_ingestion(conn, job_id, "warning", f"run {run_id} daily progress close failed: {pbar_exc}")
    status = "success" if stats["failed_codes"] == 0 else "failed"
    finish_run(conn, run_id, status, stats)
    finish_job(conn, job_id, status, {"run_id": str(run_id), "stats": stats})
    log_ingestion(conn, job_id, "info", f"run {run_id} finished status={status} stats={json.dumps(stats, ensure_ascii=False)}")
    print(f"[DONE] daily_raw status={status} stats={stats}")
    return status == "success"


def ingest_minute(
    conn,
    codes: List[str],
    target_date: dt.date,
    start_override: Optional[dt.date],
    batch_size: int,
    max_empty: int,
    job_id_opt: Optional[str] = None,
) -> bool:
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
    failed_ts_codes: set[str] = set()
    task_ids_by_code: Dict[str, uuid.UUID] = {}
    update_job_summary(conn, job_id, {"total_codes": len(codes), "success_codes": 0, "failed_codes": 0, "inserted_rows": 0})
    pbar = None
    if tqdm is not None:
        try:
            pbar = tqdm(total=len(codes), desc="kline_minute_raw incr", unit="code")
        except Exception as pbar_exc:  # noqa: BLE001
            log_ingestion(conn, job_id, "warning", f"run {run_id} minute progress initialization failed: {pbar_exc}")
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
                task_ids_by_code[ts_code] = task_id
                complete_task(conn, task_id, True, 100.0, None)
                stats["total_codes"] += 1
                stats["success_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"success_codes": 1})
                except Exception as summary_exc:  # noqa: BLE001
                    log_ingestion(
                        conn,
                        job_id,
                        "warning",
                        f"run {run_id} {dataset} {ts_code} job_summary_update failed: {summary_exc}",
                    )
                if pbar is not None:
                    try:
                        pbar.update(1)
                    except Exception as pbar_exc:  # noqa: BLE001
                        log_ingestion(conn, job_id, "warning", f"run {run_id} minute progress update failed: {pbar_exc}")
                        pbar = None
                continue
            task_id = create_task(conn, job_id, dataset, ts_code, range_start, range_end)
            task_ids_by_code[ts_code] = task_id
            code_failed = False
            err: Optional[str] = None
            empty_streak = 0
            # /api/kline-all/tdx 已在服务端分页拉取最多 24000 根；一次请求后本地按目标范围过滤，
            # 不能按 3 天窗口重复下载同一份完整历史。
            window_days = max((range_end - range_start).days + 1, 1)
            cur_start = range_start
            while cur_start <= range_end:
                cur_end = min(range_end, cur_start + dt.timedelta(days=window_days - 1))
                try:
                    values = fetch_minute_range(code, cur_start, cur_end)
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
                            "range_start": cur_start.isoformat(),
                            "range_end": cur_end.isoformat(),
                        },
                    )
                    print(f"[WARN] {dataset} {ts_code} range {cur_start}..{cur_end} failed: {err}")
                    log_ingestion(
                        conn,
                        job_id,
                        "error",
                        f"run {run_id} {dataset} {ts_code} range {cur_start}..{cur_end} failed: {err}",
                    )
                    break

                # 按交易日分组，保持旧逻辑的“逐日 upsert” 语义
                by_date: Dict[str, List[Dict[str, Any]]] = {}
                for row in values:
                    trade_date_str = _to_date(
                        row.get("TradeTime")
                        or row.get("trade_time")
                        or row.get("Time")
                        or row.get("time")
                    )
                    if trade_date_str is None:
                        continue
                    by_date.setdefault(trade_date_str, []).append(row)

                stop_early = False
                for trade_date in date_range(cur_start, cur_end):
                    key = trade_date.isoformat()
                    day_bars = by_date.get(key, [])
                    try:
                        if not day_bars:
                            empty_streak += 1
                            # 当 max_empty <= 0 时，不根据空天数提前停止，始终扫完整日期区间
                            if max_empty > 0 and empty_streak >= max_empty:
                                log_ingestion(
                                    conn,
                                    job_id,
                                    "info",
                                    f"run {run_id} {dataset} {ts_code} empty streak={empty_streak}, stop at {trade_date.isoformat()}",
                                )
                                stop_early = True
                                break
                            continue
                        empty_streak = 0
                        inserted, last_ts = upsert_minute(conn, ts_code, trade_date, day_bars)
                        stats["inserted_rows"] += inserted
                        if inserted > 0:
                            last_dt = dt.datetime.fromisoformat(last_ts) if last_ts else None
                            upsert_state(conn, dataset, ts_code, trade_date, last_dt, None)
                            upsert_checkpoint(conn, run_id, dataset, ts_code, trade_date, last_ts, None)
                        log_ingestion(
                            conn,
                            job_id,
                            "info",
                            f"run {run_id} {dataset} {ts_code} {trade_date} inserted={inserted}",
                        )
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
                        stop_early = True
                        break

                if code_failed or stop_early:
                    break
                cur_start = cur_end + dt.timedelta(days=1)

            if code_failed:
                failed_ts_codes.add(ts_code)
                stats["failed_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"failed_codes": 1})
                except Exception as summary_exc:  # noqa: BLE001
                    log_ingestion(
                        conn,
                        job_id,
                        "warning",
                        f"run {run_id} {dataset} {ts_code} job_summary_update failed: {summary_exc}",
                    )
            else:
                stats["success_codes"] += 1
                try:
                    update_job_summary(conn, job_id, {"success_codes": 1})
                except Exception as summary_exc:  # noqa: BLE001
                    log_ingestion(
                        conn,
                        job_id,
                        "warning",
                        f"run {run_id} {dataset} {ts_code} job_summary_update failed: {summary_exc}",
                    )
            ok = not code_failed
            complete_task(conn, task_id, ok, 100.0 if ok else 0.0, None if ok else err)
            stats["total_codes"] += 1
            if pbar is not None:
                try:
                    pbar.update(1)
                except Exception as pbar_exc:  # noqa: BLE001
                    log_ingestion(conn, job_id, "warning", f"run {run_id} minute progress update failed: {pbar_exc}")
                    pbar = None
    if pbar is not None:
        try:
            pbar.close()
        except Exception as pbar_exc:  # noqa: BLE001
            log_ingestion(conn, job_id, "warning", f"run {run_id} minute progress close failed: {pbar_exc}")
    validation_start = start_override or target_date
    remaining_gaps = validate_minute_range_and_repair(
        conn,
        run_id,
        job_id,
        dataset,
        codes,
        validation_start,
        target_date,
        stats,
    )
    if remaining_gaps:
        has_universe_failure = any(not gap.get("ts_code") for gap in remaining_gaps)
        failed_gap_codes = {str(gap["ts_code"]) for gap in remaining_gaps if gap.get("ts_code")}
        if has_universe_failure:
            failed_gap_codes.update(codes)
        failed_ts_codes.update(failed_gap_codes)
        failure_by_code = {
            str(gap["ts_code"]): gap
            for gap in remaining_gaps
            if gap.get("ts_code")
        }
        for failed_code in failed_gap_codes:
            task_id = task_ids_by_code.get(failed_code)
            if task_id is None:
                continue
            gap = failure_by_code.get(failed_code)
            error_text = (
                "minute completeness validation failed: "
                + json.dumps(gap, ensure_ascii=False, default=str)
                if gap is not None
                else "minute completeness authority or universe validation failed"
            )
            complete_task(conn, task_id, False, 0.0, error_text)
        try:
            update_job_summary(conn, job_id, {"completeness_failed_codes": len(failed_gap_codes)})
        except Exception as summary_exc:  # noqa: BLE001
            log_ingestion(conn, job_id, "warning", f"run {run_id} {dataset} final job_summary_update failed: {summary_exc}")

    stats["failed_codes"] = len(failed_ts_codes)
    stats["success_codes"] = max(int(stats["total_codes"]) - stats["failed_codes"], 0)
    status = "success" if stats["failed_codes"] == 0 and not remaining_gaps else "failed"
    finish_run(conn, run_id, status, stats)
    finish_job(
        conn,
        job_id,
        status,
        {
            "run_id": str(run_id),
            "stats": stats,
            "total_codes": stats["total_codes"],
            "success_codes": stats["success_codes"],
            "failed_codes": stats["failed_codes"],
            "inserted_rows": stats["inserted_rows"],
        },
    )
    log_ingestion(conn, job_id, "info", f"run {run_id} finished status={status} stats={json.dumps(stats, ensure_ascii=False)}")
    print(f"[DONE] minute status={status} stats={stats}")
    return status == "success"


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

    # 默认使用 TDX /api/codes 作为全市场股票来源，不再强依赖 market.symbol_dim。
    try:
        codes = fetch_codes(exchanges)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to fetch codes from TDX /api/codes: {exc}")
        sys.exit(1)
    if not codes:
        print("[ERROR] no codes returned from TDX /api/codes; please check TDX backend status")
        sys.exit(1)

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '5min'")
        if args.bulk_session_tune:
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off")
                cur.execute("SET work_mem = '256MB'")

        dataset_results: List[bool] = []
        if "kline_daily_raw" in datasets:
            dataset_results.append(
                ingest_daily_raw(conn, codes, target_date, start_override, args.batch_size, args.job_id)
            )

        if "kline_minute_raw" in datasets:
            dataset_results.append(
                ingest_minute(conn, codes, target_date, start_override, args.batch_size, args.max_empty, args.job_id)
            )

    if not dataset_results or not all(dataset_results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
