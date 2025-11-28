"""Ingest Tushare Tongdaxin (TDX) board datasets into TimescaleDB.

Datasets:
- tdx_board_index  (tdx_index):   board basic info
- tdx_board_member (tdx_member):  board membership
- tdx_board_daily  (tdx_daily):   board daily market data
- tdx_board_all: run index -> member -> daily in sequence

Mode:
- init: full sync over a date range (default: last 365 days)
- incremental: continue from max(trade_date) + 1 to today (per table)

Environment:
- TUSHARE_TOKEN for Tushare
- TDX_DB_HOST/PORT/USER/PASSWORD/NAME for PostgreSQL

Notes:
- Respects Tushare row limits by looping over dates and board codes when necessary
- Adds simple rate limiting with small sleeps
- Integrates with ingestion_jobs and ingestion_job_tasks/logs for Task Monitor
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv
try:
    # optional console progress when running via CLI
    from tqdm import tqdm  # type: ignore
except Exception:  # noqa: BLE001
    tqdm = None  # type: ignore

# Load env from .env if present before reading env vars
load_dotenv(override=True)

pgx.register_uuid()

# Optional dependency; import lazily when needed

def _load_tushare():
    import importlib
    ts = importlib.import_module("tushare")
    return ts


def _pro_api():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts = _load_tushare()
    return ts.pro_api(token)


# Build DB config from environment
DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-ingest-tdx-board",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Tushare TDX board datasets")
    parser.add_argument("--dataset", type=str, required=True,
                        help="One of tdx_board_index,tdx_board_member,tdx_board_daily,tdx_board_all")
    parser.add_argument("--mode", type=str, default="init", choices=["init", "incremental"])
    parser.add_argument("--start-date", type=str, default=(dt.date.today() - dt.timedelta(days=365)).isoformat())
    parser.add_argument("--end-date", type=str, default=dt.date.today().isoformat())
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--job-id", type=str, default=None)
    parser.add_argument("--bulk-session-tune", action="store_true")
    return parser.parse_args()


def _date_iter(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cur = start
    step = dt.timedelta(days=1)
    while cur <= end:
        yield cur
        cur += step


def _yyyymmdd(d: dt.date | str) -> str:
    if isinstance(d, str):
        try:
            d = dt.date.fromisoformat(d)
        except Exception:
            return d
    return d.strftime("%Y%m%d")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _with_conn():
    return psycopg2.connect(**DB_CFG)


def _ensure_session_tune(conn, enabled: bool) -> None:
    if not enabled:
        return
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = off")
        cur.execute("SET work_mem = '256MB'")


def _get_max_date(conn, table: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(trade_date) FROM {table}")
        row = cur.fetchone()
        return row[0].isoformat() if row and row[0] else None


def _create_job(conn, job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
    job_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_jobs (job_id, job_type, status, created_at, started_at, summary)
            VALUES (%s, %s, 'running', NOW(), NOW(), %s)
            """,
            (job_id, job_type, _json(summary)),
        )


def _update_job_summary(conn, job_id: uuid.UUID, patch: Dict[str, Any]) -> None:
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
            (_json(base), job_id),
        )


def _update_task_progress(conn, task_id: uuid.UUID, progress: float) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_job_tasks
               SET progress=%s, updated_at=NOW()
             WHERE task_id=%s
            """,
            (progress, task_id),
        )


def _finish_job(conn, job_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status=%s, finished_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (status, _json(summary), job_id),
        )


def _create_task(conn, job_id: uuid.UUID, dataset: str, date_from: Optional[str], date_to: Optional[str]) -> uuid.UUID:
    task_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_job_tasks (task_id, job_id, dataset, ts_code, date_from, date_to, status, progress)
            VALUES (%s, %s, %s, NULL, %s, %s, 'running', 0)
            """,
            (task_id, job_id, dataset, date_from, date_to),
        )
    return task_id


def _complete_task(conn, task_id: uuid.UUID, success: bool, progress: float, last_error: Optional[str]) -> None:
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


def _log(conn, job_id: uuid.UUID, level: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES (%s, NOW(), %s, %s)",
            (job_id, level.upper(), message),
        )


# ---------------- DB upserts ----------------

def upsert_board_index(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.tdx_board_index (trade_date, ts_code, name, idx_type, idx_count) "
        "VALUES %s ON CONFLICT (trade_date, ts_code) DO UPDATE SET "
        "name=EXCLUDED.name, idx_type=EXCLUDED.idx_type, idx_count=EXCLUDED.idx_count"
    )
    values: List[Tuple[Any, ...]] = []
    for r in rows:
        trade_date = r.get("trade_date")
        ts_code = r.get("ts_code")
        name = r.get("name")
        idx_type = r.get("idx_type")
        idx_count = r.get("idx_count")
        if not (trade_date and ts_code):
            continue
        values.append((trade_date, ts_code, name, idx_type, int(idx_count) if idx_count is not None else None))
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def upsert_board_member(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.tdx_board_member (trade_date, ts_code, con_code, con_name) "
        "VALUES %s ON CONFLICT (trade_date, ts_code, con_code) DO UPDATE SET "
        "con_name=EXCLUDED.con_name"
    )
    values: List[Tuple[Any, ...]] = []
    for r in rows:
        trade_date = r.get("trade_date")
        ts_code = r.get("ts_code")
        con_code = r.get("con_code")
        con_name = r.get("con_name")
        if not (trade_date and ts_code and con_code):
            continue
        values.append((trade_date, ts_code, con_code, con_name))
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def upsert_board_daily(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.tdx_board_daily (trade_date, ts_code, open, high, low, close, pre_close, change, pct_chg, vol, amount) "
        "VALUES %s ON CONFLICT (trade_date, ts_code) DO UPDATE SET "
        "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, "
        "pre_close=EXCLUDED.pre_close, change=EXCLUDED.change, pct_chg=EXCLUDED.pct_chg, vol=EXCLUDED.vol, amount=EXCLUDED.amount"
    )
    values: List[Tuple[Any, ...]] = []
    for r in rows:
        # numeric fields might be None
        values.append(
            (
                r.get("trade_date"),
                r.get("ts_code"),
                r.get("open"), r.get("high"), r.get("low"), r.get("close"),
                r.get("pre_close"), r.get("change"), r.get("pct_chg"),
                r.get("vol"), r.get("amount"),
            )
        )
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


# ---------------- Fetchers ----------------

def fetch_index_for_date(pro, trade_date: str) -> List[Dict[str, Any]]:
    # fields minimal set per doc sample
    df = pro.tdx_index(trade_date=trade_date, fields="ts_code,name,idx_type,idx_count")
    rows: List[Dict[str, Any]] = []
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            rows.append({
                "trade_date": dt.date.fromisoformat(trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:8]).isoformat(),
                "ts_code": str(row.get("ts_code") or "").strip(),
                "name": row.get("name"),
                "idx_type": row.get("idx_type"),
                "idx_count": row.get("idx_count"),
            })
    return rows


def _list_boards_from_db(conn, trade_date: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ts_code FROM market.tdx_board_index WHERE trade_date=%s",
            (trade_date,),
        )
        return [r[0] for r in cur.fetchall()] or []


def fetch_member_for_board_date(pro, trade_date: str, ts_code: str) -> List[Dict[str, Any]]:
    df = pro.tdx_member(trade_date=trade_date, ts_code=ts_code)
    rows: List[Dict[str, Any]] = []
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            rows.append({
                "trade_date": dt.date.fromisoformat(trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:8]).isoformat(),
                "ts_code": str(row.get("ts_code") or "").strip(),
                "con_code": str(row.get("con_code") or "").strip(),
                "con_name": row.get("con_name"),
            })
    return rows


def fetch_daily_for_date(pro, trade_date: str) -> List[Dict[str, Any]]:
    # Prefer bulk by date if API supports it
    rows: List[Dict[str, Any]] = []
    api = getattr(pro, "tdx_daily", None)
    if api is None:
        raise RuntimeError("tushare.pro_api missing tdx_daily; please upgrade tushare");
    df = api(trade_date=trade_date)
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            rows.append({
                "trade_date": dt.date.fromisoformat(trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:8]).isoformat(),
                "ts_code": str(row.get("ts_code") or "").strip(),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "pre_close": row.get("pre_close"),
                "change": row.get("change"),
                "pct_chg": row.get("pct_chg"),
                "vol": row.get("vol"),
                "amount": row.get("amount"),
            })
    return rows


# ---------------- Main runner ----------------

def run_dataset(conn, pro, dataset: str, mode: str, start_date: str, end_date: str, job_id: uuid.UUID) -> Dict[str, Any]:
    stats = {"inserted_rows": 0, "success_days": 0, "failed_days": 0}
    if mode == "init":
        table: Optional[str] = None
        if dataset == "tdx_board_index":
            table = "market.tdx_board_index"
        elif dataset == "tdx_board_member":
            table = "market.tdx_board_member"
        elif dataset == "tdx_board_daily":
            table = "market.tdx_board_daily"
        if table is not None:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table}")
    d0 = dt.date.fromisoformat(start_date)
    d1 = dt.date.fromisoformat(end_date)

    task_id = _create_task(conn, job_id, dataset, start_date, end_date)
    total_days = max(0, (d1 - d0).days + 1)
    processed_days = 0
    # accumulate total_days at job level, so Task Monitor can compute percent
    _update_job_summary(conn, job_id, {"total_days": total_days})

    pbar = None
    if tqdm is not None:
        try:
            pbar = tqdm(total=total_days, desc=f"{dataset} {mode}", unit="day")
        except Exception:
            pbar = None

    for d in _date_iter(d0, d1):
        ymd = _yyyymmdd(d)
        try:
            if dataset == "tdx_board_index":
                rows = fetch_index_for_date(pro, ymd)
                if mode == "init":
                    inserted = insert_board_index_init(conn, rows)
                else:
                    inserted = upsert_board_index(conn, rows)
            elif dataset == "tdx_board_member":
                # get boards for this date from DB (populated by index)
                boards = _list_boards_from_db(conn, d.isoformat())
                if not boards:
                    # lazy ensure index for the date
                    idx_rows = fetch_index_for_date(pro, ymd)
                    upsert_board_index(conn, idx_rows)
                    boards = _list_boards_from_db(conn, d.isoformat())
                inserted = 0
                for b in boards:
                    rows = fetch_member_for_board_date(pro, ymd, b)
                    if rows:
                        if mode == "init":
                            inserted += insert_board_member_init(conn, rows)
                        else:
                            inserted += upsert_board_member(conn, rows)
                    time.sleep(0.2)
            elif dataset == "tdx_board_daily":
                rows = fetch_daily_for_date(pro, ymd)
                if mode == "init":
                    inserted = insert_board_daily_init(conn, rows)
                else:
                    inserted = upsert_board_daily(conn, rows)
            else:
                raise ValueError(f"Unsupported dataset: {dataset}")
            stats["inserted_rows"] += inserted
            stats["success_days"] += 1
            _log(conn, job_id, "info", f"{dataset} {d} inserted={inserted}")
        except Exception as exc:  # noqa: BLE001
            stats["failed_days"] += 1
            _log(conn, job_id, "error", f"{dataset} {d} failed: {exc}")
        # progress accounting (count processed day regardless success/fail)
        processed_days += 1
        # update job-level counters for the UI
        try:
            inc = int(inserted)
        except Exception:
            inc = 0
        _update_job_summary(conn, job_id, {"done_days": 1, "inserted_rows": inc})
        # update task progress percentage
        if total_days > 0:
            _update_task_progress(conn, task_id, min(100.0, (processed_days / total_days) * 100.0))
        if pbar is not None:
            try:
                pbar.update(1)
            except Exception:
                pass
        time.sleep(0.1)

    if pbar is not None:
        try:
            pbar.close()
        except Exception:
            pass
    _complete_task(conn, task_id, success=(stats["failed_days"] == 0), progress=100.0, last_error=None)
    return stats


def main() -> None:
    args = parse_args()
    dataset = (args.dataset or "").strip().lower()
    mode = (args.mode or "init").strip().lower()

    with _with_conn() as conn:
        conn.autocommit = True
        _ensure_session_tune(conn, args.bulk_session_tune)

        job_params = {
            "dataset": dataset,
            "mode": mode,
            "start_date": args.start_date,
            "end_date": args.end_date,
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE market.ingestion_jobs
                       SET status='running', started_at=COALESCE(started_at,NOW()), summary=%s
                     WHERE job_id=%s
                    """,
                    (_json(job_params), job_id),
                )
        else:
            job_id = _create_job(conn, "init" if mode == "init" else "incremental", job_params)
        _log(conn, job_id, "info", f"start tushare tdx board ingestion {dataset} {mode}")

        # initialize total_days upfront for UI progress fallback
        try:
            d0 = dt.date.fromisoformat(args.start_date)
            d1 = dt.date.fromisoformat(args.end_date)
            days = max(0, (d1 - d0).days + 1)
            parts = 3 if dataset == "tdx_board_all" else 1
            total_days = days * parts
            _update_job_summary(conn, job_id, {"total_days": total_days, "done_days": 0})
        except Exception:
            pass
        pro = _pro_api()

        total_summary: Dict[str, Any] = {"inserted_rows": 0, "parts": {}}
        try:
            if dataset == "tdx_board_all":
                parts = ["tdx_board_index", "tdx_board_member", "tdx_board_daily"]
                for part in parts:
                    s = run_dataset(conn, pro, part, mode, args.start_date, args.end_date, job_id)
                    total_summary["parts"][part] = s
                    total_summary["inserted_rows"] += s.get("inserted_rows", 0)
            else:
                s = run_dataset(conn, pro, dataset, mode, args.start_date, args.end_date, job_id)
                total_summary["parts"][dataset] = s
                total_summary["inserted_rows"] += s.get("inserted_rows", 0)
            _finish_job(conn, job_id, "success", total_summary)
            _log(conn, job_id, "info", f"finished success: {total_summary}")
            print("[DONE]", total_summary)
        except Exception as exc:  # noqa: BLE001
            _finish_job(conn, job_id, "failed", {"error": str(exc), **total_summary})
            _log(conn, job_id, "error", f"failed: {exc}")
            print("[ERROR]", exc)
            sys.exit(1)


if __name__ == "__main__":
    main()
