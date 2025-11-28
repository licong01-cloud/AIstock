"""Rebuild adjusted daily K-line (QFQ/HFQ) from RAW using Tushare adj_factor.

This script reads RAW daily bars from market.kline_daily_raw, fetches
Tushare adj_factor, computes adjusted prices, and upserts into
market.kline_daily_qfq (QFQ) and/or market.kline_daily_hfq (HFQ).

- Supports per-exchange code selection (sh,sz,bj)
- Supports date range filtering
- Supports pre-created job via --job-id for UI progress polling
- Updates ingestion_jobs summary with progress counters

Usage example:
  python scripts/rebuild_adjusted_daily.py --which both --exchanges sh,sz,bj --start-date 1990-01-01 --truncate
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras as pgx
import tushare as ts
try:
    from tqdm import tqdm  # type: ignore
except Exception:  # noqa: BLE001
    tqdm = None  # type: ignore
from dotenv import load_dotenv

pgx.register_uuid()

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-rebuild-adjusted-daily",
)

EXCHANGE_MAP = {"sh": "SH", "sz": "SZ", "bj": "BJ"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild adjusted daily from RAW using Tushare adj_factor")
    parser.add_argument("--which", type=str, default="both", choices=["both", "qfq", "hfq"], help="Which adjusted series to build")
    parser.add_argument("--exchanges", type=str, default="sh,sz,bj", help="Comma separated exchanges (sh,sz,bj)")
    parser.add_argument("--start-date", type=str, default="1990-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=dt.date.today().isoformat(), help="End date (YYYY-MM-DD)")
    parser.add_argument("--batch-size", type=int, default=100, help="Codes per batch")
    parser.add_argument("--truncate", action="store_true", help="Delete target rows for selected codes/date range before rebuild")
    parser.add_argument("--job-id", type=str, default=None, help="Attach to existing job id (pre-created by backend)")
    parser.add_argument("--bulk-session-tune", action="store_true", help="Apply session-level tuning for bulk load (SET synchronous_commit=off, work_mem=256MB)")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        choices=[1, 2, 4, 8],
        help="Number of parallel workers (1 = no parallelism)",
    )
    return parser.parse_args()


def _to_yyyymmdd(date_text: str) -> str:
    return date_text.replace("-", "")


def _to_date(text: str) -> dt.date:
    return dt.date.fromisoformat(text)


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
    """Merge final summary into existing ingestion_jobs.summary instead of overwriting."""
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


def log_ingestion(conn, job_id: uuid.UUID, level: str, message: str) -> None:
    """Print ingestion log line instead of inserting DB row.

    In the new backend, scheduler aggregates stdout into a single
    market.ingestion_logs row per job; scripts only need to emit text.
    """

    level_up = str(level or "INFO").upper()
    print(f"[{level_up}] job_id={job_id} {message}")


def create_task(conn, job_id: uuid.UUID, dataset: str, ts_code: str, date_from: Optional[str], date_to: Optional[str]) -> uuid.UUID:
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


# ------------------------ Data helpers ------------------------

def get_codes(conn, exchanges: Iterable[str]) -> List[str]:
    ex_vals = [EXCHANGE_MAP.get(ex.lower()) for ex in exchanges if ex.lower() in EXCHANGE_MAP]
    params: Tuple[Any, ...] = ()
    sql = "SELECT ts_code FROM market.symbol_dim"
    if ex_vals:
        placeholders = ",".join(["%s"] * len(ex_vals))
        sql += f" WHERE exchange IN ({placeholders})"
        params = tuple(ex_vals)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [row[0] for row in cur.fetchall()]


def get_raw_bars(conn, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = (
        "SELECT trade_date, open_li, high_li, low_li, close_li, volume_hand, amount_li "
        "FROM market.kline_daily_raw WHERE ts_code=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date ASC"
    )
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(sql, (ts_code, start_date, end_date))
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["trade_date", "open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li"])
    df = pd.DataFrame(rows)
    return df


def upsert_adjusted(
    conn,
    table: str,
    ts_code: str,
    df: pd.DataFrame,
    source: str,
) -> int:
    # df must contain: trade_date, open_li, high_li, low_li, close_li, volume_hand, amount_li
    values: List[Tuple[Any, ...]] = []
    for _, r in df.iterrows():
        values.append(
            (
                r["trade_date"],
                ts_code,
                int(round(r["open_li"])) if pd.notna(r["open_li"]) else None,
                int(round(r["high_li"])) if pd.notna(r["high_li"]) else None,
                int(round(r["low_li"])) if pd.notna(r["low_li"]) else None,
                int(round(r["close_li"])) if pd.notna(r["close_li"]) else None,
                int(r["volume_hand"]) if pd.notna(r["volume_hand"]) else 0,
                int(r["amount_li"]) if pd.notna(r["amount_li"]) else 0,
                source,
            )
        )
    if not values:
        return 0
    sql = (
        f"INSERT INTO {table} (trade_date, ts_code, open_li, high_li, low_li, close_li, volume_hand, amount_li, source) "
        f"VALUES %s ON CONFLICT (ts_code, trade_date) DO UPDATE SET "
        f"open_li=EXCLUDED.open_li, high_li=EXCLUDED.high_li, low_li=EXCLUDED.low_li, close_li=EXCLUDED.close_li, volume_hand=EXCLUDED.volume_hand, amount_li=EXCLUDED.amount_li"
    )
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


# ------------------------ Tushare ------------------------

def get_adj_factor(pro, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    # tushare uses yyyymmdd
    try:
        df = pro.adj_factor(ts_code=ts_code, start_date=_to_yyyymmdd(start_date), end_date=_to_yyyymmdd(end_date))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"tushare adj_factor failed for {ts_code}: {exc}")
    if df is None or df.empty:
        return pd.DataFrame(columns=["trade_date", "adj_factor"])  # empty
    # normalize 'trade_date' to YYYY-MM-DD string and sort asc
    df = df.loc[:, ["trade_date", "adj_factor"]].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    df = df.sort_values("trade_date").reset_index(drop=True)
    return df


def build_adjusted_series(raw_df: pd.DataFrame, factor_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if raw_df.empty:
        return raw_df.copy(), raw_df.copy()
    # align factors to raw trade dates; forward-fill missing
    f = factor_df.set_index("trade_date")
    r = raw_df.set_index("trade_date")
    f2 = f.reindex(r.index)
    f2["adj_factor"] = f2["adj_factor"].ffill().bfill()
    last = f2["adj_factor"].iloc[-1]
    first = f2["adj_factor"].iloc[0]
    if pd.isna(last) or pd.isna(first):
        # if still NaN, fallback 1.0
        f2["adj_factor"] = f2["adj_factor"].fillna(1.0)
        last = f2["adj_factor"].iloc[-1]
        first = f2["adj_factor"].iloc[0]
    # ratios
    ratio_qfq = f2["adj_factor"] / (last if last else 1.0)
    ratio_hfq = f2["adj_factor"] / (first if first else 1.0)
    # apply
    def _apply_ratio(df_base: pd.DataFrame, ratio: pd.Series) -> pd.DataFrame:
        out = df_base.copy()
        for col in ["open_li", "high_li", "low_li", "close_li"]:
            out[col] = (out[col].astype(float) * ratio.values).round()
        # keep volumes as raw
        return out
    qfq_df = _apply_ratio(r, ratio_qfq)
    hfq_df = _apply_ratio(r, ratio_hfq)
    # restore index
    qfq_df = qfq_df.reset_index()
    hfq_df = hfq_df.reset_index()
    return qfq_df, hfq_df


# ------------------------ Main ------------------------

def main() -> None:
    load_dotenv(override=True)
    args = parse_args()
    exchanges = [ex.strip().lower() for ex in args.exchanges.split(",") if ex.strip()]
    try:
        _ = _to_date(args.start_date)
        _ = _to_date(args.end_date)
    except ValueError:
        print("[ERROR] start-date or end-date format invalid")
        sys.exit(1)

    ts_token = os.getenv("TUSHARE_TOKEN")
    if not ts_token:
        print("[ERROR] TUSHARE_TOKEN not set in environment")
        sys.exit(1)
    ts.set_token(ts_token)
    pro = ts.pro_api()

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '5min'")
        if args.bulk_session_tune:
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off")
                cur.execute("SET work_mem = '256MB'")
                cur.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        codes = get_codes(conn, exchanges)
        if not codes:
            print("[ERROR] no codes in market.symbol_dim for exchanges; abort")
            sys.exit(1)

        job_params = {
            "datasets": ["adjust_daily"],
            "which": args.which,
            "exchanges": exchanges,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "batch_size": int(args.batch_size),
            "workers": int(args.workers),
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            start_job(conn, job_id, job_params)
        else:
            job_id = create_job(conn, "init", job_params)
        update_job_summary(conn, job_id, {"total_codes": len(codes), "success_codes": 0, "failed_codes": 0, "inserted_rows": 0})
        log_ingestion(conn, job_id, "info", f"adjust rebuild start which={args.which} codes={len(codes)} range={args.start_date}..{args.end_date}")

        # pre-clean target range if requested (after job started so UI can see progress)
        if args.truncate:
            arr = codes
            for tbl in (["market.kline_daily_qfq"] if args.which == "qfq" else ["market.kline_daily_hfq"] if args.which == "hfq" else ["market.kline_daily_qfq", "market.kline_daily_hfq"]):
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM " + tbl + " WHERE trade_date BETWEEN %s AND %s AND ts_code = ANY(%s)",
                        (args.start_date, args.end_date, arr),
                    )
            print("[WARN] target adjusted tables cleaned per request")

        total_inserted = 0
        success = 0
        failed = 0
        # initialize summary for UI fallback
        update_job_summary(conn, job_id, {"total_codes": len(codes), "success_codes": 0, "failed_codes": 0, "inserted_rows": 0})

        def _process_code(ts_code: str) -> Tuple[bool, int, bool, str]:
            """Worker function: process a single ts_code using its own DB connection and Tushare client.

            Returns (ok, inserted_rows, no_raw, error_message).
            """
            inserted = 0
            no_raw = False
            err_msg = ""
            try:
                ts_token = os.getenv("TUSHARE_TOKEN")
                if not ts_token:
                    return False, 0, False, "TUSHARE_TOKEN not set in environment"
                ts.set_token(ts_token)
                pro = ts.pro_api()
                with psycopg2.connect(**DB_CFG) as wconn:
                    wconn.autocommit = True
                    with wconn.cursor() as cur:
                        cur.execute("SET lock_timeout = '5s'")
                        cur.execute("SET statement_timeout = '5min'")
                    raw_df = get_raw_bars(wconn, ts_code, args.start_date, args.end_date)
                    if raw_df.empty:
                        no_raw = True
                        return True, 0, True, "no raw rows"
                    factor_df = get_adj_factor(pro, ts_code, args.start_date, args.end_date)
                    qfq_df, hfq_df = build_adjusted_series(raw_df, factor_df)
                    if args.which in {"qfq", "both"}:
                        inserted += upsert_adjusted(wconn, "market.kline_daily_qfq", ts_code, qfq_df, "tushare_adj")
                    if args.which in {"hfq", "both"}:
                        inserted += upsert_adjusted(wconn, "market.kline_daily_hfq", ts_code, hfq_df, "tushare_adj")
                return True, inserted, no_raw, ""
            except Exception as exc:  # noqa: BLE001
                err_msg = str(exc)
                return False, inserted, no_raw, err_msg

        pbar = None
        if tqdm is not None:
            try:
                pbar = tqdm(total=len(codes), desc=f"adjust_{args.which}", unit="code")
            except Exception:
                pbar = None

        with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
            future_map = {}
            for ts_code in codes:
                task_id = create_task(conn, job_id, "adjust_daily", ts_code, args.start_date, args.end_date)
                fut = executor.submit(_process_code, ts_code)
                future_map[fut] = (ts_code, task_id)

            for fut in as_completed(future_map):
                ts_code, task_id = future_map[fut]
                ok = False
                inserted_rows = 0
                no_raw = False
                err_msg = ""
                try:
                    ok, inserted_rows, no_raw, err_msg = fut.result()
                except Exception as exc:  # noqa: BLE001
                    ok = False
                    err_msg = str(exc)
                if ok:
                    if no_raw:
                        success += 1
                        complete_task(conn, task_id, True, 100.0, None)
                        log_ingestion(conn, job_id, "info", f"{ts_code} no raw rows in range {args.start_date}..{args.end_date}")
                    else:
                        success += 1
                        total_inserted += inserted_rows
                        complete_task(conn, task_id, True, 100.0, None)
                        update_job_summary(conn, job_id, {"inserted_rows": int(inserted_rows), "success_codes": 1})
                        print(f"[OK] {ts_code} adjusted -> {args.which} inserted_rows={inserted_rows}")
                        log_ingestion(conn, job_id, "info", f"{ts_code} adjusted inserted_rows={inserted_rows}")
                else:
                    failed += 1
                    complete_task(conn, task_id, False, 0.0, err_msg or "adjust failed")
                    update_job_summary(conn, job_id, {"failed_codes": 1})
                    print(f"[WARN] {ts_code} adjust failed: {err_msg}")
                    log_ingestion(conn, job_id, "error", f"{ts_code} adjust failed: {err_msg}")
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
        status = "success" if failed == 0 else "failed"
        finish_job(
            conn,
            job_id,
            status,
            {
                "which": args.which,
                "exchanges": exchanges,
                "stats": {"success_codes": success, "failed_codes": failed, "inserted_rows": total_inserted},
            },
        )
        print(f"[DONE] adjust rebuild status={status} success={success} failed={failed} inserted={total_inserted}")
        log_ingestion(conn, job_id, "info", f"adjust rebuild finished status={status} success={success} failed={failed} inserted={total_inserted}")


if __name__ == "__main__":
    main()
