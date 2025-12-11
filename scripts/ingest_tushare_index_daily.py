from __future__ import annotations

"""Ingest Tushare index_daily (指数日线行情) into market.index_daily.

- 支持 init / incremental 两种模式。
- 支持通过 --index-markets 仅同步指定市场(来自 index_basic.market 字段)。
- 按 ts_code 循环调用 pro.index_daily(ts_code=..., start_date=..., end_date=...)，
  严格检查单次返回行数 < 8000；若某个 ts_code 在指定日期范围内返回行数 >= 8000，
  视为触达接口上限，整批任务失败并在日志中提示需要缩小日期范围。
- 使用 ingestion_jobs / ingestion_logs 记录任务状态，方便任务监视器展示。
"""

import argparse
import datetime as dt
import json
import os
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv


load_dotenv(override=True)
pgx.register_uuid()


DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-ingest-index_daily",
)


def _load_tushare() -> Any:
    import importlib

    return importlib.import_module("tushare")


def pro_api():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts = _load_tushare()
    return ts.pro_api(token)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Tushare index_daily into TimescaleDB (market.index_daily)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="init",
        choices=["init", "incremental"],
        help="Ingestion mode",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start trade_date YYYY-MM-DD or YYYYMMDD",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End trade_date YYYY-MM-DD or YYYYMMDD (defaults to today)",
    )
    parser.add_argument(
        "--index-markets",
        type=str,
        default=None,
        help="Comma-separated index_basic.market filters, e.g. CSI,SSE,SZSE",
    )
    parser.add_argument(
        "--job-id",
        type=str,
        default=None,
        help="Existing job id to attach and update",
    )
    parser.add_argument(
        "--batch-sleep",
        type=float,
        default=0.0,
        help="Sleep seconds between ts_code batches",
    )
    return parser.parse_args()


def _parse_ymd(val: Optional[str]) -> Optional[dt.date]:
    if not val:
        return None
    s = str(val)
    try:
        if len(s) == 8 and s.isdigit():
            return dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        return dt.date.fromisoformat(s)
    except Exception:  # noqa: BLE001
        return None


def _create_job(conn, job_type: str, summary: Dict[str, Any]):
    import uuid

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


def _start_existing_job(conn, job_id, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status='running', started_at=COALESCE(started_at, NOW()), summary=%s
             WHERE job_id=%s
            """,
            (json.dumps(summary, ensure_ascii=False), job_id),
        )


def _finish_job(conn, job_id, status: str, summary: Optional[Dict[str, Any]] = None) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT summary FROM market.ingestion_jobs WHERE job_id=%s", (job_id,))
        row = cur.fetchone()
        base: Dict[str, Any] = {}
        if row and row[0]:
            try:
                base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
            except Exception:  # noqa: BLE001
                base = {}
        if summary:
            base.update(summary)
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status=%s, finished_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (status, json.dumps(base, ensure_ascii=False), job_id),
        )


def _log(conn, job_id, level: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES (%s, NOW(), %s, %s)",
            (job_id, level.upper(), message),
        )


def _fetch_index_codes(
    conn,
    markets: Optional[Sequence[str]] = None,
) -> List[Tuple[str, Optional[str]]]:
    """加载需要同步的指数 ts_code 列表。

    返回 (ts_code, market) 元组列表，方便日志中标记 market。
    若 markets 为空或 None，则不加 market 过滤，使用全部指数。
    """

    sql = "SELECT ts_code, market FROM market.index_basic"
    params: Tuple[Any, ...] = ()
    if markets:
        sql += " WHERE market = ANY(%s)"
        params = (list(markets),)
    sql += " ORDER BY ts_code"

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [(str(r[0]), (str(r[1]) if r[1] is not None else None)) for r in rows]


def _upsert_index_daily(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.index_daily (ts_code, trade_date, close, open, high, low, pre_close, "
        "change, pct_chg, vol, amount) "
        "VALUES %s "
        "ON CONFLICT (ts_code, trade_date) DO UPDATE SET "
        "close=EXCLUDED.close, open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
        "pre_close=EXCLUDED.pre_close, change=EXCLUDED.change, pct_chg=EXCLUDED.pct_chg, "
        "vol=EXCLUDED.vol, amount=EXCLUDED.amount"
    )

    def _to_date(val: Any) -> Optional[dt.date]:
        if val is None:
            return None
        if isinstance(val, dt.date):
            return val
        return _parse_ymd(str(val))

    values = [
        (
            (r.get("ts_code") or "").strip(),
            _to_date(r.get("trade_date")),
            r.get("close"),
            r.get("open"),
            r.get("high"),
            r.get("low"),
            r.get("pre_close"),
            r.get("change"),
            r.get("pct_chg"),
            r.get("vol"),
            r.get("amount"),
        )
        for r in rows
        if r.get("ts_code") and _to_date(r.get("trade_date")) is not None
    ]
    if not values:
        return 0

    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def _get_global_max_trade_date(conn) -> Optional[dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT max(trade_date) FROM market.index_daily")
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return row[0]


def run_ingestion(
    conn,
    pro,
    mode: str,
    start_date: Optional[dt.date],
    end_date: dt.date,
    markets: Optional[List[str]],
    job_id,
    batch_sleep: float,
) -> Dict[str, Any]:
    # 计算默认开始日期（仅 incremental 且未传 start_date 时使用全局 max(trade_date)+1）。
    if mode == "incremental" and start_date is None:
        max_dt = _get_global_max_trade_date(conn)
        if max_dt is None:
            # 若没有任何历史数据，则从 end_date 开始，相当于只拉当日。
            start_date = end_date
        else:
            start_date = max_dt + dt.timedelta(days=1)

    if start_date is None:
        raise RuntimeError("start_date must not be None in run_ingestion")

    if start_date > end_date:
        _log(conn, job_id, "info", f"index_daily up to date: start_date {start_date} > end_date {end_date}")
        return {"total_ts": 0, "done_ts": 0, "failed_ts": 0, "inserted_rows": 0}

    # 加载 ts_code 列表
    codes = _fetch_index_codes(conn, markets)
    if not codes:
        _log(conn, job_id, "warn", "no index_basic rows found for specified markets; nothing to do")
        return {"total_ts": 0, "done_ts": 0, "failed_ts": 0, "inserted_rows": 0}

    total_ts = len(codes)
    done_ts = 0
    failed_ts = 0
    inserted_rows = 0

    # Tushare index_daily 要求日期为 YYYYMMDD
    start_ymd = start_date.strftime("%Y%m%d")
    end_ymd = end_date.strftime("%Y%m%d")

    for idx, (ts_code, mkt) in enumerate(codes, start=1):
        try:
            df = pro.index_daily(ts_code=ts_code, start_date=start_ymd, end_date=end_ymd)
            if df is None or df.empty:
                done_ts += 1
            else:
                if len(df.index) >= 8000:
                    msg = (
                        f"index_daily ts_code={ts_code} market={mkt or ''} returned {len(df.index)} rows "
                        f"for range {start_ymd}..{end_ymd}, which reaches or exceeds 8000-row limit; "
                        "please narrow date range and rerun."
                    )
                    _log(conn, job_id, "error", msg)
                    raise RuntimeError(msg)

                rows: List[Dict[str, Any]] = []
                for _, row in df.iterrows():
                    rows.append(
                        {
                            "ts_code": row.get("ts_code") or ts_code,
                            "trade_date": row.get("trade_date"),
                            "close": row.get("close"),
                            "open": row.get("open"),
                            "high": row.get("high"),
                            "low": row.get("low"),
                            "pre_close": row.get("pre_close"),
                            "change": row.get("change"),
                            "pct_chg": row.get("pct_chg"),
                            "vol": row.get("vol"),
                            "amount": row.get("amount"),
                        }
                    )

                inserted = _upsert_index_daily(conn, rows)
                inserted_rows += inserted
                done_ts += 1
        except Exception as exc:  # noqa: BLE001
            failed_ts += 1
            _log(
                conn,
                job_id,
                "error",
                f"index_daily ts_code={ts_code} market={mkt or ''} failed: {exc}",
            )
            # 对于 8000 行上限错误等，直接中止整个任务。
            raise
        finally:
            # 简单进度日志（避免太多日志，主要通过 summary 暴露）。
            if idx % 100 == 0 or idx == total_ts:
                try:
                    summary = {
                        "counters": {
                            "total": total_ts,
                            "done": done_ts,
                            "failed": failed_ts,
                            "success": done_ts,
                            "inserted_rows": inserted_rows,
                        },
                        "progress": 0.0
                        if total_ts <= 0
                        else max(0.0, min(100.0, 100.0 * float(done_ts + failed_ts) / float(total_ts))),
                    }
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE market.ingestion_jobs
                               SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
                             WHERE job_id = %s
                            """,
                            (json.dumps(summary, ensure_ascii=False), job_id),
                        )
                    conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

        if batch_sleep > 0:
            import time

            time.sleep(batch_sleep)

    return {
        "total_ts": total_ts,
        "done_ts": done_ts,
        "failed_ts": failed_ts,
        "inserted_rows": inserted_rows,
    }


def main() -> None:
    args = parse_args()
    mode = (args.mode or "init").strip().lower()

    today = dt.date.today()
    # 解析 end_date
    if args.end_date:
        end_date = _parse_ymd(args.end_date)
        if not end_date:
            print("[ERROR] invalid --end-date format, expected YYYY-MM-DD or YYYYMMDD")
            sys.exit(1)
    else:
        end_date = today

    # 解析 start_date（对 init 强制要求；incremental 可选）
    start_date: Optional[dt.date]
    if mode == "init":
        if not args.start_date:
            print("[ERROR] --start-date is required in init mode")
            sys.exit(1)
        start_date = _parse_ymd(args.start_date)
        if not start_date:
            print("[ERROR] invalid --start-date format, expected YYYY-MM-DD or YYYYMMDD")
            sys.exit(1)
    elif mode == "incremental":
        start_date = _parse_ymd(args.start_date) if args.start_date else None
    else:
        print(f"[ERROR] unsupported mode: {mode}")
        sys.exit(1)

    # 解析 index_markets
    markets: Optional[List[str]]
    if args.index_markets:
        markets = [s.strip().upper() for s in args.index_markets.split(",") if s.strip()]
        markets = markets or None
    else:
        markets = None

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        pro = pro_api()

        job_summary: Dict[str, Any] = {
            "dataset": "index_daily",
            "mode": mode,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat(),
            "index_markets": markets,
        }
        if args.job_id:
            import uuid

            job_id = uuid.UUID(args.job_id)
            _start_existing_job(conn, job_id, job_summary)
        else:
            job_id = _create_job(conn, mode, job_summary)

        _log(
            conn,
            job_id,
            "info",
            f"start tushare index_daily ingestion mode={mode} start={start_date} end={end_date} markets={markets}",
        )

        try:
            stats = run_ingestion(
                conn=conn,
                pro=pro,
                mode=mode,
                start_date=start_date,
                end_date=end_date,
                markets=markets,
                job_id=job_id,
                batch_sleep=float(args.batch_sleep or 0.0),
            )
            summary = {
                "stats": stats,
                "inserted_rows": int(stats.get("inserted_rows") or 0),
            }
            status = "success" if int(stats.get("failed_ts") or 0) == 0 else "failed"
            _finish_job(conn, job_id, status, summary)
            print(f"[DONE] index_daily mode={mode} stats={stats}")
        except Exception as exc:  # noqa: BLE001
            _finish_job(conn, job_id, "failed", {"error": str(exc)})
            print(f"[ERROR] index_daily failed: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    main()
