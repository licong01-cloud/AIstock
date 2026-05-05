"""Sync cninfo announcement metadata into market.anns.

This is a metadata-only backfill/repair utility. It does not download PDF files.
Rows are normalized into the existing market.anns schema used by anns_d:
ann_date, ts_code, name, title, url, rec_time.

The cninfo API may under-report totalpages for large dates, so pagination uses
totalAnnouncement/page size plus a small guard window and does not stop on an
early hasMore=false while raw rows are still below totalAnnouncement.
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
    application_name="AIstock-cninfo-anns-metadata-sync",
)

CNINFO_QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_STATIC_PREFIX = "https://static.cninfo.com.cn/"
PAGE_SIZE = 30
CN_TZ = dt.timezone(dt.timedelta(hours=8))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120 Safari/537.36"
    ),
    "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
    "Origin": "http://www.cninfo.com.cn",
    "X-Requested-With": "XMLHttpRequest",
}

TAG_RE = re.compile(r"<[^>]+>")
THREAD_LOCAL = threading.local()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync cninfo announcement metadata to market.anns")
    parser.add_argument("--start-date", default="2018-08-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", default="2026-04-30", help="End date YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent date workers")
    parser.add_argument("--request-sleep", type=float, default=0.02, help="Sleep seconds between page requests")
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
        help="Path for per-date audit JSONL; defaults to reports/anns/cninfo_sync_audit_<ts>.jsonl",
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


def code_to_ts_code(sec_code: Any) -> Optional[str]:
    code = re.sub(r"\D", "", str(sec_code or ""))
    if len(code) != 6:
        return None
    if code.startswith(("4", "8", "92")):
        return None
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "2", "3")):
        return f"{code}.SZ"
    return None


def adjunct_to_url(adjunct_url: Any, announcement_id: Any, ann_date: dt.date) -> str:
    raw = str(adjunct_url or "").strip()
    if raw.startswith(("http://", "https://")):
        return raw
    if raw:
        return CNINFO_STATIC_PREFIX + raw.lstrip("/")
    ann_id = str(announcement_id or "").strip()
    if ann_id:
        return f"{CNINFO_STATIC_PREFIX}finalpage/{ann_date.isoformat()}/{ann_id}.PDF"
    return ""


def rec_time_from_ms(ms: Any) -> Optional[dt.datetime]:
    try:
        if ms in (None, ""):
            return None
        value = int(ms)
        if value <= 0:
            return None
        return dt.datetime.fromtimestamp(value / 1000.0, tz=CN_TZ)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def cninfo_payload(ann_date: dt.date, page_num: int) -> Dict[str, str]:
    d = ann_date.isoformat()
    return {
        "pageNum": str(page_num),
        "pageSize": str(PAGE_SIZE),
        "column": "szse",
        "tabName": "fulltext",
        "plate": "",
        "stock": "",
        "searchkey": "",
        "secid": "",
        "category": "",
        "trade": "",
        "seDate": f"{d}~{d}",
        "sortName": "",
        "sortType": "",
        "isHLtitle": "false",
    }


def fetch_page(
    session: requests.Session,
    ann_date: dt.date,
    page_num: int,
    max_retries: int,
) -> Dict[str, Any]:
    payload = cninfo_payload(ann_date, page_num)
    last_error: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.post(CNINFO_QUERY_URL, data=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"unexpected JSON type: {type(data)!r}")
            return data
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(min(12.0, 0.8 * attempt * attempt))
    raise RuntimeError(f"cninfo page failed date={ann_date} page={page_num}: {last_error}")


def row_from_item(item: Dict[str, Any], ann_date: dt.date) -> Optional[Tuple[Any, ...]]:
    ts_code = code_to_ts_code(item.get("secCode"))
    if not ts_code:
        return None
    title = normalize_text(item.get("announcementTitle") or item.get("shortTitle"))
    if not title:
        return None
    name = normalize_text(item.get("secName") or item.get("tileSecName"))
    url = adjunct_to_url(item.get("adjunctUrl"), item.get("announcementId"), ann_date)
    if not url:
        return None
    rec_time = rec_time_from_ms(item.get("announcementTime"))
    return (ann_date, ts_code, name, title, url, rec_time, "pending")


def fetch_cninfo_date(
    ann_date: dt.date,
    request_sleep: float,
    max_retries: int,
) -> Tuple[List[Tuple[Any, ...]], Dict[str, Any]]:
    session = get_session()
    page = 1
    source_total = 0
    totalpages = 0
    raw_count = 0
    normalized_count = 0
    duplicate_count = 0
    rows_by_key: Dict[Tuple[str, dt.date, str], Tuple[Any, ...]] = {}
    pages_seen = 0
    early_has_more_false_pages: List[int] = []
    empty_pages: List[int] = []
    empty_pages_recovered: List[int] = []

    while True:
        payload = fetch_page(session, ann_date, page, max_retries=max_retries)
        announcements = payload.get("announcements") or []
        if page == 1:
            source_total = int(payload.get("totalAnnouncement") or 0)
            totalpages = int(payload.get("totalpages") or 0)
            if source_total <= 0 and not announcements:
                break
        expected_pages = max(totalpages, int(math.ceil(source_total / PAGE_SIZE))) if source_total > 0 else totalpages

        if not announcements:
            if source_total > 0 and page <= expected_pages + 3:
                recovered = False
                for retry in range(1, 4):
                    time.sleep(min(4.0, 0.6 * retry * retry))
                    payload = fetch_page(session, ann_date, page, max_retries=max_retries)
                    announcements = payload.get("announcements") or []
                    if announcements:
                        recovered = True
                        empty_pages_recovered.append(page)
                        break
                if not recovered:
                    empty_pages.append(page)
                    page += 1
                    if request_sleep > 0:
                        time.sleep(request_sleep)
                    continue
            else:
                break

        if not announcements:
            break

        raw_count += len(announcements)
        pages_seen += 1
        for item in announcements:
            if not isinstance(item, dict):
                continue
            row = row_from_item(item, ann_date)
            if row is None:
                continue
            normalized_count += 1
            key = (row[1], row[0], row[3])
            if key in rows_by_key:
                duplicate_count += 1
            rows_by_key[key] = row

        has_more = bool(payload.get("hasMore"))
        if not has_more and raw_count < source_total:
            early_has_more_false_pages.append(page)

        # Stop only when the source says no more and the raw count has reached
        # totalAnnouncement, or after a small guard window beyond expected pages.
        if (not has_more and raw_count >= source_total) or (expected_pages > 0 and page >= expected_pages + 3):
            break

        page += 1
        if request_sleep > 0:
            time.sleep(request_sleep)

    audit = {
        "ann_date": ann_date.isoformat(),
        "source_total": source_total,
        "source_totalpages": totalpages,
        "pages_seen": pages_seen,
        "raw_count": raw_count,
        "normalized_count": normalized_count,
        "unique_count": len(rows_by_key),
        "duplicate_count": duplicate_count,
        "pagination_complete": raw_count >= source_total,
        "early_has_more_false_pages": early_has_more_false_pages,
        "empty_pages": empty_pages,
        "empty_pages_recovered": empty_pages_recovered,
    }
    return list(rows_by_key.values()), audit


def upsert_rows(
    conn: psycopg2.extensions.connection,
    rows: List[Tuple[Any, ...]],
    *,
    source: str = "cninfo",
    job_id: Optional[str] = None,
) -> int:
    if not rows:
        return 0
    observed_rows = [(*row, source, source, job_id, job_id) for row in rows]
    sql = """
        INSERT INTO market.anns (
            ann_date, ts_code, name, title, url, rec_time, download_status,
            first_seen_at, last_seen_at, first_seen_source, last_seen_source,
            first_seen_job_id, last_seen_job_id, observed_time_quality
        )
        VALUES %s
        ON CONFLICT (ts_code, ann_date, title) DO UPDATE SET
            name = EXCLUDED.name,
            url = EXCLUDED.url,
            rec_time = COALESCE(EXCLUDED.rec_time, market.anns.rec_time),
            last_seen_at = NOW(),
            last_seen_source = EXCLUDED.last_seen_source,
            last_seen_job_id = EXCLUDED.last_seen_job_id,
            updated_at = NOW()
    """
    with conn.cursor() as cur:
        pgx.execute_values(
            cur,
            sql,
            observed_rows,
            template="(%s,%s,%s,%s,%s,%s,%s,NOW(),NOW(),%s,%s,%s,%s,'LOCAL_FIRST_SEEN')",
            page_size=1000,
        )
    return len(rows)


def db_count_for_date(conn: psycopg2.extensions.connection, ann_date: dt.date) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM market.anns WHERE ann_date = %s", (ann_date,))
        return int(cur.fetchone()[0])


def sync_one_date(
    ann_date: dt.date,
    request_sleep: float,
    max_retries: int,
    bulk_session_tune: bool,
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    conn = get_conn(bulk_session_tune=bulk_session_tune)
    try:
        rows, audit = fetch_cninfo_date(
            ann_date=ann_date,
            request_sleep=request_sleep,
            max_retries=max_retries,
        )
        touched = upsert_rows(conn, rows, source="cninfo", job_id=job_id)
        db_count_after = db_count_for_date(conn, ann_date)
        conn.commit()
        audit.update(
            {
                "status": "success",
                "upsert_touched": touched,
                "db_count_after": db_count_after,
            }
        )
        return audit
    except Exception as exc:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:
            pass
        return {
            "ann_date": ann_date.isoformat(),
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
    return ROOT / "reports" / "anns" / f"cninfo_sync_audit_{ts}.jsonl"


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

    if args.mode == "missing":
        dates = missing_dates(start, end)
    else:
        dates = list(date_range(start, end))

    audit_path = audit_path_from_args(args.audit_jsonl)
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        json.dumps(
            {
                "event": "start",
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
        "raw_count": 0,
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
                    for key in ("source_total", "raw_count", "unique_count", "upsert_touched"):
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
                        f"src={result.get('source_total')} raw={result.get('raw_count')} "
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
        "elapsed_sec": round(elapsed, 3),
        "dates": len(dates),
        **stats,
        "audit_jsonl": str(audit_path),
    }
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0 if stats["failed_days"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run())
