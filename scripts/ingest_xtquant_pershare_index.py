"""Ingest xtquant PershareIndex (per-share financial indicators) with init/incremental and parallel support.

- init: full range by report_date, fetch all A-share stocks via xtdata, upsert by (report_date, ts_code).
- incremental: report_date cursor from max(report_date)+1 to today (or override).
- supports --workers for multi-process parallelism (each process has its own xtdata connection).
- supports --truncate before init, --batch-sleep between batches.
- supports --bulk-session-tune for session-level write tuning.
- only logs errors/warnings to ingestion_logs; normal successes print to stdout.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import multiprocessing
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# 确保 xtquant 可导入（AIstock 项目根目录下有 xtquant 包）
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

load_dotenv(os.path.join(_PROJECT_ROOT, ".env"), override=True)
pgx.register_uuid()

DB_CFG: Dict[str, Any] = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-ingest-xtquant-pershare",
)

# ---------------------------------------------------------------------------
# PershareIndex 字段定义（与建表 DDL 及 pershare_new.ini 一致）
# ---------------------------------------------------------------------------
NUMERIC_FIELDS: List[str] = [
    "s_fa_ocfps",
    "s_fa_bps",
    "s_fa_eps_basic",
    "s_fa_eps_diluted",
    "s_fa_undistributedps",
    "s_fa_surpluscapitalps",
    "adjusted_earnings_per_share",
    "du_return_on_equity",
    "sales_gross_profit",
    "equity_roe",
    "net_roe",
    "total_roe",
    "gross_profit",
    "net_profit",
    "actual_tax_rate",
    "inc_revenue_rate",
    "du_profit_rate",
    "inc_net_profit_rate",
    "adjusted_net_profit_rate",
    "inc_total_revenue_annual",
    "inc_net_profit_to_shareholders_annual",
    "adjusted_profit_to_profit_annual",
    "pre_pay_operate_income",
    "sales_cash_flow",
    "gear_ratio",
    "inventory_turnover",
]

ALL_COLUMNS: List[str] = ["report_date", "ts_code", "ann_date"] + NUMERIC_FIELDS

_UPSERT_SQL: str = (
    f"INSERT INTO market.xtquant_pershare_index ({', '.join(ALL_COLUMNS)}) "
    "VALUES %s "
    "ON CONFLICT (report_date, ts_code) DO UPDATE SET "
    + ", ".join(
        f"{c} = EXCLUDED.{c}"
        for c in ALL_COLUMNS
        if c not in ("report_date", "ts_code")
    )
)


# ===================================================================
# xtquant 连接与数据获取
# ===================================================================

def _connect_xtdata() -> None:
    """连接 QMT miniQMT 客户端"""
    from xtquant import xtdata

    host = os.getenv("MINIQMT_HOST", "127.0.0.1")
    port = int(os.getenv("MINIQMT_PORT", "58610"))
    xtdata.enable_hello = False
    xtdata.connect(ip=host, port=port)


def _get_all_a_stocks() -> List[str]:
    """获取全部沪深A股代码列表"""
    from xtquant import xtdata

    stocks = xtdata.get_stock_list_in_sector("沪深A股")
    if not stocks:
        raise RuntimeError("无法获取沪深A股列表，请检查 QMT/miniQMT 客户端是否运行")
    return sorted(stocks)


def _download_pershare_for_stocks(
    stock_list: List[str], start_time: str, end_time: str
) -> None:
    """先将 PershareIndex 数据下载到 xtquant 本地缓存

    xtquant 的 get_financial_data 只能读取本地缓存，
    必须先调用 download_financial_data 将数据从服务端拉取到本地。
    """
    from xtquant import xtdata

    xtdata.download_financial_data(
        stock_list=stock_list,
        table_list=["PershareIndex"],
        start_time=start_time,
        end_time=end_time,
    )


def _fetch_pershare_for_stocks(
    stock_list: List[str], start_time: str, end_time: str
) -> Dict[str, Any]:
    """获取指定股票列表的 PershareIndex 数据

    流程：先 download 到本地缓存，再 get 读取。
    返回: {stock_code: DataFrame, ...}
    """
    from xtquant import xtdata

    # 第一步：下载到本地缓存
    _download_pershare_for_stocks(stock_list, start_time, end_time)

    # 第二步：从本地缓存读取
    result = xtdata.get_financial_data(
        stock_list=stock_list,
        table_list=["PershareIndex"],
        start_time=start_time,
        end_time=end_time,
        report_type="report_time",
    )
    flat: Dict[str, Any] = {}
    for stock_code, tables in (result or {}).items():
        if isinstance(tables, dict) and "PershareIndex" in tables:
            df = tables["PershareIndex"]
            if df is not None and not df.empty:
                flat[stock_code] = df
    return flat


# ===================================================================
# 数据转换
# ===================================================================

def _safe_numeric(val: Any) -> Optional[float]:
    """将 xtquant 返回的数值转为 Python float 或 None"""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _parse_xtquant_date(val: Any) -> Optional[dt.date]:
    """将 xtquant 的日期字段（'YYYYMMDD' 字符串或毫秒时间戳）转为 date"""
    if val is None or val == "":
        return None
    if isinstance(val, str):
        val_s = val.strip()
        if not val_s or val_s == "NaT":
            return None
        try:
            return dt.datetime.strptime(val_s[:8], "%Y%m%d").date()
        except ValueError:
            return None
    if isinstance(val, (int, float)):
        if math.isnan(val):
            return None
        try:
            return dt.datetime.fromtimestamp(val / 1000).date()
        except (OSError, ValueError):
            return None
    return None


def _transform_rows(stock_code: str, df: Any) -> List[Tuple]:
    """将单只股票的 PershareIndex DataFrame 转为数据库行元组列表"""
    rows: List[Tuple] = []
    for _, row in df.iterrows():
        report_date = _parse_xtquant_date(row.get("m_timetag"))
        if not report_date:
            continue
        ann_date = _parse_xtquant_date(row.get("m_anntime"))
        db_row: List[Any] = [report_date, stock_code, ann_date]
        for field in NUMERIC_FIELDS:
            db_row.append(_safe_numeric(row.get(field)))
        rows.append(tuple(db_row))
    return rows


# ===================================================================
# 数据库操作
# ===================================================================

def _upsert_batch(conn: Any, rows: List[Tuple]) -> int:
    """批量 upsert，返回受影响行数"""
    if not rows:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, _UPSERT_SQL, rows, page_size=500)
    return len(rows)


def _get_max_report_date(conn: Any) -> Optional[dt.date]:
    """查询数据库中最大的 report_date 作为增量游标"""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(report_date) FROM market.xtquant_pershare_index")
        row = cur.fetchone()
        return row[0] if row and row[0] else None


# ===================================================================
# Job 管理（与 ingest_tushare_daily_basic.py 模式一致）
# ===================================================================

def _create_job(conn: Any, job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
    job_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_jobs (job_id, job_type, status, created_at, started_at, summary)
            VALUES (%s, %s, 'running', NOW(), NOW(), %s)
            """,
            (job_id, job_type, json.dumps(summary, ensure_ascii=False, default=str)),
        )
    return job_id


def _start_existing_job(conn: Any, job_id: uuid.UUID, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status='running', started_at=COALESCE(started_at, NOW()), summary=%s
             WHERE job_id=%s
            """,
            (json.dumps(summary, ensure_ascii=False, default=str), job_id),
        )


def _finish_job(conn: Any, job_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
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
            (status, json.dumps(base, ensure_ascii=False, default=str), job_id),
        )


def _log(conn: Any, job_id: uuid.UUID, level: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES (%s, NOW(), %s, %s)",
            (job_id, level.upper(), message),
        )


def _update_job_progress(conn: Any, job_id: uuid.UUID, stats: Dict[str, Any]) -> None:
    total = int(stats.get("total_stocks", 0))
    done = int(stats.get("success", 0)) + int(stats.get("failed", 0))
    progress = 0.0 if total <= 0 else max(0.0, min(100.0, 100.0 * float(done) / float(total)))
    counters = {
        "total": total,
        "done": done,
        "running": 0,
        "pending": max(total - done, 0),
        "failed": int(stats.get("failed", 0)),
        "success": int(stats.get("success", 0)),
        "inserted_rows": int(stats.get("inserted_rows", 0)),
    }
    payload = {"counters": counters, "progress": progress}
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id = %s
            """,
            (json.dumps(payload, ensure_ascii=False), job_id),
        )


# ===================================================================
# 单进程 ingestion
# ===================================================================

def _ingest_single_process(
    stock_list: List[str],
    start_time: str,
    end_time: str,
    conn: Any,
    job_id: uuid.UUID,
    batch_sleep: float,
) -> Dict[str, int]:
    """单进程模式：按批次顺序处理全部股票"""
    total = len(stock_list)
    success = 0
    failed = 0
    inserted = 0

    # 按 100 个股票为一批（xtquant 内部再按 20 个分批请求）
    batch_size = 100
    batches = [stock_list[i : i + batch_size] for i in range(0, total, batch_size)]

    for batch_idx, batch in enumerate(batches):
        try:
            data = _fetch_pershare_for_stocks(batch, start_time, end_time)
            batch_rows: List[Tuple] = []
            for stock_code, df in data.items():
                rows = _transform_rows(stock_code, df)
                batch_rows.extend(rows)

            if batch_rows:
                cnt = _upsert_batch(conn, batch_rows)
                inserted += cnt
                conn.commit()

            success += len(batch)
        except Exception as exc:
            failed += len(batch)
            try:
                conn.rollback()
            except Exception:
                pass
            _log(conn, job_id, "ERROR", f"Batch {batch_idx} ({len(batch)} stocks) failed: {exc}")
            conn.commit()

        # 更新进度
        try:
            _update_job_progress(conn, job_id, {
                "total_stocks": total,
                "success": success,
                "failed": failed,
                "inserted_rows": inserted,
            })
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

        done_pct = int((success + failed) / total * 100) if total > 0 else 0
        print(f"[PROGRESS] batch {batch_idx + 1}/{len(batches)} "
              f"success={success} failed={failed} inserted={inserted} ({done_pct}%)")

        if batch_sleep > 0:
            time.sleep(batch_sleep)

    return {"success": success, "failed": failed, "inserted_rows": inserted}


# ===================================================================
# 多进程并行 ingestion
# ===================================================================

def _worker_fn(args_tuple: Tuple) -> Dict[str, int]:
    """子进程 worker：独立连接 xtdata 和 DB，处理分配的股票分片

    每个子进程独立建立 xtdata 连接（xtdata.__client 是进程级单例，不可跨进程/线程共享）。
    """
    chunk, start_time, end_time, db_cfg, batch_sleep, worker_id = args_tuple

    # 每个子进程独立连接 xtdata
    _connect_xtdata()

    # 每个子进程独立连接数据库
    conn = psycopg2.connect(**db_cfg)
    conn.autocommit = True

    success = 0
    failed = 0
    inserted = 0

    batch_size = 100
    batches = [chunk[i : i + batch_size] for i in range(0, len(chunk), batch_size)]

    for batch_idx, batch in enumerate(batches):
        try:
            data = _fetch_pershare_for_stocks(batch, start_time, end_time)
            batch_rows: List[Tuple] = []
            for stock_code, df in data.items():
                rows = _transform_rows(stock_code, df)
                batch_rows.extend(rows)
            if batch_rows:
                cnt = _upsert_batch(conn, batch_rows)
                conn.commit()
                inserted += cnt
            success += len(batch)
        except Exception as exc:
            failed += len(batch)
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[Worker-{worker_id}] Batch {batch_idx} failed: {exc}", file=sys.stderr)

        if batch_sleep > 0:
            time.sleep(batch_sleep)

    conn.close()
    print(f"[Worker-{worker_id}] Done: success={success} failed={failed} inserted={inserted}")
    return {"success": success, "failed": failed, "inserted_rows": inserted}


def _ingest_parallel(
    stock_list: List[str],
    start_time: str,
    end_time: str,
    workers: int,
    batch_sleep: float,
    conn: Any,
    job_id: uuid.UUID,
) -> Dict[str, int]:
    """多进程模式：将股票列表均匀分片，每个子进程独立处理"""
    # 均匀分片
    chunks: List[List[str]] = [[] for _ in range(workers)]
    for i, stock in enumerate(stock_list):
        chunks[i % workers].append(stock)
    chunks = [c for c in chunks if c]  # 去空

    args_list = [
        (chunk, start_time, end_time, DB_CFG, batch_sleep, idx)
        for idx, chunk in enumerate(chunks)
    ]

    _log(conn, job_id, "INFO", f"Starting {len(chunks)} worker processes, total {len(stock_list)} stocks")
    conn.commit()

    with multiprocessing.Pool(processes=len(chunks)) as pool:
        results = pool.map(_worker_fn, args_list)

    # 汇总
    total_success = sum(r["success"] for r in results)
    total_failed = sum(r["failed"] for r in results)
    total_inserted = sum(r["inserted_rows"] for r in results)

    return {
        "success": total_success,
        "failed": total_failed,
        "inserted_rows": total_inserted,
    }


# ===================================================================
# CLI 参数解析
# ===================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest xtquant PershareIndex into TimescaleDB"
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
        help="Start report_date YYYY-MM-DD (or YYYYMMDD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End report_date YYYY-MM-DD (defaults to today)",
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
        default=0.3,
        help="Sleep seconds between stock batches",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate market.xtquant_pershare_index before init (destructive)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel worker processes (default 1 = sequential)",
    )
    parser.add_argument(
        "--bulk-session-tune",
        action="store_true",
        help="Apply session-level tuning for bulk load (SET synchronous_commit=off, work_mem=256MB)",
    )
    return parser.parse_args()


def _parse_ymd(val: Optional[str]) -> Optional[dt.date]:
    """解析 YYYY-MM-DD 或 YYYYMMDD 格式的日期字符串"""
    if not val:
        return None
    try:
        s = str(val).strip().replace("-", "")
        if len(s) == 8:
            return dt.date(int(s[:4]), int(s[4:6]), int(s[6:]))
        return dt.date.fromisoformat(str(val).strip())
    except Exception:
        return None


# ===================================================================
# 主入口
# ===================================================================

def main() -> None:
    args = parse_args()
    mode = (args.mode or "init").strip().lower()
    today = dt.date.today()

    # 解析结束日期
    end_date = _parse_ymd(args.end_date) or today

    # 数据库连接
    conn = psycopg2.connect(**DB_CFG)
    conn.autocommit = True

    if args.bulk_session_tune:
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit = off")
            cur.execute("SET work_mem = '256MB'")

    # 连接 xtdata
    _connect_xtdata()
    stock_list = _get_all_a_stocks()
    print(f"[INFO] Got {len(stock_list)} A-share stocks from xtdata")

    # 解析起始日期
    if mode == "init":
        if not args.start_date:
            start_date = dt.date(2010, 1, 1)
            print("[INFO] init mode: no --start-date, defaulting to 2010-01-01")
        else:
            start_date = _parse_ymd(args.start_date)
            if not start_date:
                print(f"[ERROR] invalid --start-date: {args.start_date}")
                sys.exit(1)
        if args.truncate:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE market.xtquant_pershare_index")
            print("[WARN] TRUNCATE market.xtquant_pershare_index executed before full ingestion")
    elif mode == "incremental":
        if args.start_date:
            start_date = _parse_ymd(args.start_date)
            if not start_date:
                print(f"[ERROR] invalid --start-date: {args.start_date}")
                sys.exit(1)
        else:
            max_date = _get_max_report_date(conn)
            if max_date:
                start_date = max_date + dt.timedelta(days=1)
            else:
                start_date = dt.date(2010, 1, 1)
                print("[INFO] incremental mode: no existing data, defaulting to 2010-01-01")
        if start_date > end_date:
            print("[INFO] xtquant_pershare_index up to date; nothing to do")
            conn.close()
            return
    else:
        print(f"[ERROR] unsupported mode: {mode}")
        sys.exit(1)

    # xtquant 日期格式 YYYYMMDD
    start_time = start_date.strftime("%Y%m%d")
    end_time = end_date.strftime("%Y%m%d")

    # Job 管理
    job_summary: Dict[str, Any] = {
        "dataset": "xtquant_pershare_index",
        "mode": mode,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_codes": len(stock_list),
        "workers": args.workers,
    }

    if args.job_id:
        job_id = uuid.UUID(args.job_id)
        _start_existing_job(conn, job_id, job_summary)
    else:
        job_id = _create_job(conn, mode, job_summary)
    conn.commit()

    _log(conn, job_id, "INFO",
         f"start xtquant_pershare_index ingestion {mode} {start_time} -> {end_time}, "
         f"stocks={len(stock_list)}, workers={args.workers}")
    conn.commit()

    print(f"[INFO] Job {job_id} started: mode={mode}, stocks={len(stock_list)}, "
          f"range={start_time}~{end_time}, workers={args.workers}")

    # 执行 ingestion
    try:
        if args.workers <= 1:
            result = _ingest_single_process(
                stock_list, start_time, end_time, conn, job_id, args.batch_sleep
            )
        else:
            result = _ingest_parallel(
                stock_list, start_time, end_time, args.workers, args.batch_sleep, conn, job_id
            )

        final_status = "success" if result["failed"] == 0 else "success"
        final_summary: Dict[str, Any] = {
            "stats": result,
            "counters": {
                "total": len(stock_list),
                "done": result["success"] + result["failed"],
                "success": result["success"],
                "failed": result["failed"],
                "inserted_rows": result["inserted_rows"],
            },
            "progress": 100.0,
        }
        _finish_job(conn, job_id, final_status, final_summary)
        conn.commit()
        _log(conn, job_id, "INFO", f"Completed: {result}")
        conn.commit()
        print(f"[DONE] Job {job_id} completed: {result}")
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        _finish_job(conn, job_id, "failed", {"error": str(exc)})
        conn.commit()
        _log(conn, job_id, "ERROR", str(exc))
        conn.commit()
        print(f"[ERROR] Job {job_id} failed: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
