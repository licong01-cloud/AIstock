"""Sync Eastmoney announcement metadata into market.anns.

The endpoint is the same public JSON API used by AKShare's
stock_notice_report. It is useful as a free metadata source when Tushare
anns_d permission is unavailable or cninfo rate-limits long backfills.

This script does not download announcement PDFs. For conflicting rows it keeps
existing cninfo/static document URLs, while filling missing metadata and exact
Eastmoney display times where available.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import math
import os
import re
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx
import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    application_name="AIstock-eastmoney-anns-metadata-sync",
)

EASTMONEY_QUERY_URL = "https://np-anotice-stock.eastmoney.com/api/security/ann"
EASTMONEY_DETAIL_PREFIX = "https://data.eastmoney.com/notices/detail"
PAGE_SIZE = 100
CN_TZ = dt.timezone(dt.timedelta(hours=8))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/notices/hsa/5.html",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

TAG_RE = re.compile(r"<[^>]+>")
THREAD_LOCAL = threading.local()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Eastmoney announcement metadata to market.anns")
    parser.add_argument("--start-date", default="2018-08-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", default="2026-04-30", help="End date YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent date workers")
    parser.add_argument("--request-sleep", type=float, default=0.03, help="Sleep seconds between page requests")
    parser.add_argument("--max-retries", type=int, default=5, help="HTTP retries per page")
    parser.add_argument(
        "--mode",
        choices=["all", "missing"],
        default="all",
        help="all repairs all dates; missing syncs only dates with zero local rows",
    )
    parser.add_argument(
        "--audit-jsonl",
        default=None,
        help="Path for per-date audit JSONL; defaults to reports/anns/eastmoney_sync_audit_<ts>.jsonl",
    )
    parser.add_argument(
        "--bulk-session-tune",
        action="store_true",
        help="Apply per-worker session tuning for bulk inserts",
    )
    return parser.parse_args()


def parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date: {value}") from exc


def date_range(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cur = start
    one = dt.timedelta(days=1)
    while cur <= end:
        yield cur
        cur += one


def get_session() -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        THREAD_LOCAL.session = session
    return session


def get_conn(bulk_session_tune: bool) -> psycopg2.extensions.connection:
    conn = getattr(THREAD_LOCAL, "conn", None)
    if conn is None or conn.closed:
        conn = psycopg2.connect(**DB_CFG)
        conn.autocommit = False
        if bulk_session_tune:
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off")
                cur.execute("SET work_mem = '128MB'")
        THREAD_LOCAL.conn = conn
    return conn


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = html.unescape(TAG_RE.sub("", text))
    return re.sub(r"\s+", " ", text).strip()


def code_to_ts_code(stock_code: Any) -> Optional[str]:
    code = re.sub(r"\D", "", str(stock_code or ""))
    if len(code) != 6:
        return None
    if code.startswith(("4", "8", "92")):
        return None
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "2", "3")):
        return f"{code}.SZ"
    return None


def parse_em_datetime(value: Any) -> Optional[dt.datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    # Eastmoney sometimes uses "YYYY-MM-DD HH:MM:SS:fff".
    match = re.match(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?::(\d{1,6}))?$", raw)
    if match:
        date_part, time_part, frac = match.groups()
        microsecond = int((frac or "0")[:6].ljust(6, "0"))
        try:
            base = dt.datetime.fromisoformat(f"{date_part}T{time_part}")
            return base.replace(microsecond=microsecond, tzinfo=CN_TZ)
        except ValueError:
            return None
    try:
        return dt.datetime.fromisoformat(raw).replace(tzinfo=CN_TZ)
    except ValueError:
        return None


def parse_em_notice_date(value: Any, fallback: dt.date) -> dt.date:
    parsed = parse_em_datetime(value)
    return parsed.date() if parsed is not None else fallback


def strip_security_prefix(title: str, name: str, stock_code: str) -> str:
    cleaned = title.strip()
    candidates = [name.strip(), stock_code.strip()]
    for prefix in [c for c in candidates if c]:
        for delimiter in (":", "："):
            marker = f"{prefix}{delimiter}"
            if cleaned.startswith(marker):
                return cleaned[len(marker) :].strip()
    for delimiter in (":", "："):
        position = cleaned.find(delimiter)
        if 1 <= position <= 12:
            return cleaned[position + 1 :].strip()
    return cleaned


def eastmoney_payload(query_date: dt.date, page_index: int) -> Dict[str, str]:
    d = query_date.isoformat()
    return {
        "sr": "-1",
        "page_size": str(PAGE_SIZE),
        "page_index": str(page_index),
        "ann_type": "A",
        "client_source": "web",
        "f_node": "0",
        "s_node": "0",
        "begin_time": d,
        "end_time": d,
    }


def fetch_page(
    session: requests.Session,
    query_date: dt.date,
    page_index: int,
    max_retries: int,
) -> Dict[str, Any]:
    params = eastmoney_payload(query_date, page_index)
    last_error: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(EASTMONEY_QUERY_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if not data.get("success", False) and "data" not in data:
                raise RuntimeError(f"Eastmoney API returned unsuccessful payload: {data!r}")
            payload = data.get("data") or {}
            if not isinstance(payload, dict):
                raise RuntimeError(f"unexpected data payload: {type(payload)!r}")
            return payload
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(min(10.0, 0.6 * attempt * attempt))
    raise RuntimeError(f"Eastmoney page failed date={query_date} page={page_index}: {last_error}")


def rows_from_item(item: Dict[str, Any], query_date: dt.date) -> List[Tuple[Any, ...]]:
    raw_title = normalize_text(item.get("title_ch") or item.get("title"))
    if not raw_title:
        return []
    ann_date = parse_em_notice_date(item.get("notice_date"), fallback=query_date)
    rec_time = (
        parse_em_datetime(item.get("display_time"))
        or parse_em_datetime(item.get("eiTime"))
        or parse_em_datetime(item.get("sort_date"))
        or parse_em_datetime(item.get("notice_date"))
    )
    art_code = normalize_text(item.get("art_code"))
    codes = item.get("codes") or []
    out: List[Tuple[Any, ...]] = []
    for code_item in codes:
        if not isinstance(code_item, dict):
            continue
        stock_code = re.sub(r"\D", "", str(code_item.get("stock_code") or ""))
        ts_code = code_to_ts_code(stock_code)
        if not ts_code:
            continue
        name = normalize_text(code_item.get("short_name"))
        title = strip_security_prefix(raw_title, name=name, stock_code=stock_code)
        if not title:
            continue
        url = f"{EASTMONEY_DETAIL_PREFIX}/{stock_code}/{art_code}.html" if art_code and stock_code else ""
        if not url:
            continue
        out.append((ann_date, ts_code, name, title, url, rec_time, "pending"))
    return out


def fetch_eastmoney_date(
    query_date: dt.date,
    request_sleep: float,
    max_retries: int,
) -> Tuple[List[Tuple[Any, ...]], Dict[str, Any]]:
    session = get_session()
    first = fetch_page(session, query_date, 1, max_retries=max_retries)
    source_total = int(first.get("total_hits") or 0)
    if source_total <= 0:
        return [], {
            "ann_date": query_date.isoformat(),
            "source_total": source_total,
            "pages_seen": 0,
            "raw_documents": 0,
            "normalized_count": 0,
            "unique_count": 0,
            "duplicate_count": 0,
            "pagination_complete": True,
            "empty_pages": [],
            "empty_pages_recovered": [],
        }

    expected_pages = int(math.ceil(source_total / PAGE_SIZE))
    rows_by_key: Dict[Tuple[str, dt.date, str], Tuple[Any, ...]] = {}
    raw_documents = 0
    normalized_count = 0
    duplicate_count = 0
    empty_pages: List[int] = []
    empty_pages_recovered: List[int] = []

    for page in range(1, expected_pages + 1):
        payload = first if page == 1 else fetch_page(session, query_date, page, max_retries=max_retries)
        items = payload.get("list") or []
        if not items:
            recovered = False
            for retry in range(1, 4):
                time.sleep(min(4.0, 0.5 * retry * retry))
                payload = fetch_page(session, query_date, page, max_retries=max_retries)
                items = payload.get("list") or []
                if items:
                    recovered = True
                    empty_pages_recovered.append(page)
                    break
            if not recovered:
                empty_pages.append(page)
                continue

        raw_documents += len(items)
        for item in items:
            if not isinstance(item, dict):
                continue
            for row in rows_from_item(item, query_date):
                normalized_count += 1
                key = (row[1], row[0], row[3])
                if key in rows_by_key:
                    duplicate_count += 1
                rows_by_key[key] = row
        if request_sleep > 0:
            time.sleep(request_sleep)

    audit = {
        "ann_date": query_date.isoformat(),
        "source_total": source_total,
        "expected_pages": expected_pages,
        "pages_seen": expected_pages - len(empty_pages),
        "raw_documents": raw_documents,
        "normalized_count": normalized_count,
        "unique_count": len(rows_by_key),
        "duplicate_count": duplicate_count,
        "pagination_complete": raw_documents >= source_total,
        "empty_pages": empty_pages,
        "empty_pages_recovered": empty_pages_recovered,
    }
    return list(rows_by_key.values()), audit


def upsert_rows(conn: psycopg2.extensions.connection, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO market.anns (ann_date, ts_code, name, title, url, rec_time, download_status)
        VALUES %s
        ON CONFLICT (ts_code, ann_date, title) DO UPDATE SET
            name = COALESCE(NULLIF(EXCLUDED.name, ''), market.anns.name),
            url = CASE
                    WHEN market.anns.url LIKE '%%cninfo.com.cn%%'
                      OR market.anns.url LIKE '%%static.cninfo.com.cn%%'
                    THEN market.anns.url
                    ELSE EXCLUDED.url
                  END,
            rec_time = COALESCE(EXCLUDED.rec_time, market.anns.rec_time),
            updated_at = NOW()
    """
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def db_count_for_date(conn: psycopg2.extensions.connection, ann_date: dt.date) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM market.anns WHERE ann_date = %s", (ann_date,))
        return int(cur.fetchone()[0])


def sync_one_date(
    query_date: dt.date,
    request_sleep: float,
    max_retries: int,
    bulk_session_tune: bool,
) -> Dict[str, Any]:
    conn = get_conn(bulk_session_tune=bulk_session_tune)
    try:
        rows, audit = fetch_eastmoney_date(query_date, request_sleep=request_sleep, max_retries=max_retries)
        touched = upsert_rows(conn, rows)
        db_count_after = db_count_for_date(conn, query_date)
        conn.commit()
        audit.update({"status": "success", "upsert_touched": touched, "db_count_after": db_count_after})
        return audit
    except Exception as exc:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:
            pass
        return {
            "ann_date": query_date.isoformat(),
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def missing_dates(start: dt.date, end: dt.date) -> List[dt.date]:
    conn = psycopg2.connect(**DB_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ann_date
                  FROM market.anns
                 WHERE ann_date BETWEEN %s AND %s
                 GROUP BY ann_date
                """,
                (start, end),
            )
            existing = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()
    return [d for d in date_range(start, end) if d not in existing]


def audit_path_from_args(value: Optional[str]) -> Path:
    if value:
        return Path(value)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return ROOT / "reports" / "anns" / f"eastmoney_sync_audit_{ts}.jsonl"


def run() -> int:
    args = parse_args()
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    if start > end:
        print("[ERROR] start-date is after end-date", file=sys.stderr)
        return 2
    if args.workers <= 0:
        print("[ERROR] workers must be positive", file=sys.stderr)
        return 2

    dates = missing_dates(start, end) if args.mode == "missing" else list(date_range(start, end))
    audit_path = audit_path_from_args(args.audit_jsonl)
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        json.dumps(
            {
                "event": "start",
                "source": "eastmoney",
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "mode": args.mode,
                "dates": len(dates),
                "workers": args.workers,
                "audit_jsonl": str(audit_path),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    if not dates:
        print("[DONE] no dates to sync", flush=True)
        return 0

    stats = {
        "success_days": 0,
        "failed_days": 0,
        "zero_days": 0,
        "source_total": 0,
        "raw_documents": 0,
        "unique_count": 0,
        "upsert_touched": 0,
    }
    started = time.time()
    next_index = 0
    futures: Dict[Future[Dict[str, Any]], dt.date] = {}

    with ThreadPoolExecutor(max_workers=args.workers) as executor, audit_path.open("a", encoding="utf-8") as audit_file:
        while next_index < len(dates) and len(futures) < args.workers * 2:
            d = dates[next_index]
            futures[
                executor.submit(
                    sync_one_date,
                    d,
                    args.request_sleep,
                    args.max_retries,
                    args.bulk_session_tune,
                )
            ] = d
            next_index += 1

        completed = 0
        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                futures.pop(future)
                result = future.result()
                completed += 1
                audit_file.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")
                audit_file.flush()

                if result.get("status") == "success":
                    stats["success_days"] += 1
                    if int(result.get("source_total") or 0) == 0:
                        stats["zero_days"] += 1
                    for key in ("source_total", "raw_documents", "unique_count", "upsert_touched"):
                        stats[key] += int(result.get(key) or 0)
                    level = "OK"
                else:
                    stats["failed_days"] += 1
                    level = "FAIL"

                elapsed = time.time() - started
                rate = completed / elapsed if elapsed > 0 else 0.0
                print(
                    (
                        f"[{level}] {completed}/{len(dates)} date={result.get('ann_date')} "
                        f"src={result.get('source_total')} docs={result.get('raw_documents')} "
                        f"uniq={result.get('unique_count')} db={result.get('db_count_after')} "
                        f"fail={stats['failed_days']} rate={rate:.2f}/s"
                    ),
                    flush=True,
                )

                while next_index < len(dates) and len(futures) < args.workers * 2:
                    d = dates[next_index]
                    futures[
                        executor.submit(
                            sync_one_date,
                            d,
                            args.request_sleep,
                            args.max_retries,
                            args.bulk_session_tune,
                        )
                    ] = d
                    next_index += 1

    elapsed = time.time() - started
    summary = {
        "event": "done",
        "source": "eastmoney",
        "elapsed_sec": round(elapsed, 3),
        "dates": len(dates),
        **stats,
        "audit_jsonl": str(audit_path),
    }
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0 if stats["failed_days"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run())
