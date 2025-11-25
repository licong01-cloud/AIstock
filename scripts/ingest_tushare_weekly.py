"""Ingest weekly K-line data into TimescaleDB kline_weekly_qfq from daily QFQ.

Design:
- init 模式：在给定日期区间内，从 market.kline_daily_qfq 聚合生成周线，写入
  market.kline_weekly_qfq。
- incremental 模式：如果未指定 --start-date，则从当前表中最大 week_end_date
  的下一周开始聚合；否则使用显式起始日期。

Notes:
- 这里直接使用本地日线表做聚合，不依赖外部 Tushare 调用，避免大规模外部
  请求与频率限制问题。
- 与其他 ingest 脚本一致，接入 ingestion_jobs / ingestion_logs，便于前端监控。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import uuid
from typing import Any, Dict, Optional

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv


load_dotenv(override=True)
pgx.register_uuid()


DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate daily QFQ into weekly_kline_qfq")
    parser.add_argument("--mode", type=str, default="init", choices=["init", "incremental"], help="Ingestion mode")
    parser.add_argument("--start-date", type=str, default=None, help="Start date YYYY-MM-DD (required for init mode unless table is empty)")
    parser.add_argument("--end-date", type=str, default=None, help="End date YYYY-MM-DD (defaults to today)")
    parser.add_argument("--job-id", type=str, default=None, help="Existing job id to attach and update")
    parser.add_argument("--bulk-session-tune", action="store_true", help="Enable bulk session tuning (ignored if omitted)")
    return parser.parse_args()


def _with_conn():
    return psycopg2.connect(**DB_CFG)


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _ensure_session_tune(conn, enabled: bool) -> None:
    if not enabled:
        return
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = off")
        cur.execute("SET work_mem = '256MB'")


def _get_max_week_end_date(conn) -> Optional[dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT max(week_end_date) FROM market.kline_weekly_qfq")
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return row[0]


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
    return job_id


def _start_existing_job(conn, job_id: uuid.UUID, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status='running', started_at=COALESCE(started_at, NOW()), summary=%s
             WHERE job_id=%s
            """,
            (_json(summary), job_id),
        )


def _finish_job(conn, job_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT summary FROM market.ingestion_jobs WHERE job_id=%s", (job_id,))
        row = cur.fetchone()
        base: Dict[str, Any] = {}
        if row and row[0]:
            try:
                base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
            except Exception:  # noqa: BLE001
                base = {}
        base.update(summary or {})
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status=%s, finished_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (status, _json(base), job_id),
        )


def _log(conn, job_id: uuid.UUID, level: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES (%s, NOW(), %s, %s)",
            (job_id, level.upper(), message),
        )


def _aggregate_weekly(conn, start_date: dt.date, end_date: dt.date, job_id: uuid.UUID) -> Dict[str, Any]:
    """Aggregate daily QFQ rows into weekly QFQ rows.

    week_end_date 定义为每周周五日期：date_trunc('week', trade_date) + 4 天。
    若该周无周五交易，则该周最后一个交易日的周五日期仍作为 week_end_date 锚点。
    """

    stats = {"total_weeks": 0, "inserted_rows": 0}
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH daily AS (
                SELECT
                    ts_code,
                    trade_date,
                    (date_trunc('week', trade_date)::date + INTERVAL '4 days')::date AS week_end_date,
                    open_li,
                    high_li,
                    low_li,
                    close_li,
                    volume_hand,
                    amount_li
                  FROM market.kline_daily_qfq
                 WHERE trade_date BETWEEN %s AND %s
            ), agg AS (
                SELECT
                    ts_code,
                    week_end_date,
                    (array_agg(open_li ORDER BY trade_date))[1] AS open_li,
                    MAX(high_li) AS high_li,
                    MIN(low_li) AS low_li,
                    (array_agg(close_li ORDER BY trade_date DESC))[1] AS close_li,
                    SUM(volume_hand) AS volume_hand,
                    SUM(amount_li) AS amount_li
                  FROM daily
                 GROUP BY ts_code, week_end_date
            )
            INSERT INTO market.kline_weekly_qfq (
                week_end_date,
                ts_code,
                open_li,
                high_li,
                low_li,
                close_li,
                volume_hand,
                amount_li,
                adjust_type,
                source
            )
            SELECT
                week_end_date,
                ts_code,
                open_li,
                high_li,
                low_li,
                close_li,
                volume_hand,
                amount_li,
                'qfq'::CHAR(3) AS adjust_type,
                'tdx_api'::VARCHAR(16) AS source
              FROM agg
            ON CONFLICT (ts_code, week_end_date) DO UPDATE SET
                open_li = EXCLUDED.open_li,
                high_li = EXCLUDED.high_li,
                low_li = EXCLUDED.low_li,
                close_li = EXCLUDED.close_li,
                volume_hand = EXCLUDED.volume_hand,
                amount_li = EXCLUDED.amount_li,
                adjust_type = EXCLUDED.adjust_type,
                source = EXCLUDED.source
            RETURNING 1
            """,
            (start_date, end_date),
        )
        rows = cur.fetchall()
        inserted = len(rows)
        stats["inserted_rows"] = inserted
        # 粗略估算周数：按 (end_date - start_date)/7
        try:
            stats["total_weeks"] = max(1, int((end_date - start_date).days / 7) + 1)
        except Exception:
            stats["total_weeks"] = 0
    _log(conn, job_id, "INFO", f"weekly_qfq aggregated {stats['inserted_rows']} rows from {start_date} to {end_date}")
    return stats


def main() -> None:
    args = parse_args()
    mode = (args.mode or "init").strip().lower()

    today = dt.date.today()
    if args.end_date:
        try:
            end_date = dt.date.fromisoformat(args.end_date)
        except ValueError:
            print("[ERROR] invalid --end-date format, expected YYYY-MM-DD")
            sys.exit(1)
    else:
        end_date = today

    with _with_conn() as conn:
        conn.autocommit = True
        _ensure_session_tune(conn, bool(args.bulk_session_tune))

        start_date: Optional[dt.date]
        if mode == "init":
            if args.start_date:
                try:
                    start_date = dt.date.fromisoformat(args.start_date)
                except ValueError:
                    print("[ERROR] invalid --start-date format, expected YYYY-MM-DD")
                    sys.exit(1)
            else:
                # 若未给出 start-date，则从日线表最早日期开始
                with conn.cursor() as cur:
                    cur.execute("SELECT MIN(trade_date) FROM market.kline_daily_qfq")
                    row = cur.fetchone()
                    if not row or row[0] is None:
                        print("[ERROR] kline_daily_qfq is empty; cannot infer start-date")
                        sys.exit(1)
                    start_date = row[0]
        elif mode == "incremental":
            if args.start_date:
                try:
                    start_date = dt.date.fromisoformat(args.start_date)
                except ValueError:
                    print("[ERROR] invalid --start-date format, expected YYYY-MM-DD")
                    sys.exit(1)
            else:
                max_week = _get_max_week_end_date(conn)
                if max_week is None:
                    # 若周线表为空，退化为从日线最早日期开始
                    with conn.cursor() as cur:
                        cur.execute("SELECT MIN(trade_date) FROM market.kline_daily_qfq")
                        row = cur.fetchone()
                        if not row or row[0] is None:
                            print("[INFO] kline_daily_qfq is empty; nothing to aggregate")
                            return
                        start_date = row[0]
                else:
                    start_date = max_week + dt.timedelta(days=1)
            if start_date > end_date:
                print("[INFO] kline_weekly_qfq up to date; nothing to do")
                return
        else:
            print(f"[ERROR] unsupported mode: {mode}")
            sys.exit(1)

        job_summary = {
            "dataset": "kline_weekly",
            "mode": mode,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat(),
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            _start_existing_job(conn, job_id, job_summary)
        else:
            job_id = _create_job(conn, mode, job_summary)
        _log(conn, job_id, "INFO", f"start weekly aggregation {mode} {start_date} -> {end_date}")

        try:
            stats = _aggregate_weekly(conn, start_date, end_date, job_id)
            _finish_job(conn, job_id, "success", {"stats": stats})
            print(f"[DONE] kline_weekly_qfq mode={mode} stats={stats}")
        except Exception as exc:  # noqa: BLE001
            _finish_job(conn, job_id, "failed", {"error": str(exc)})
            print(f"[ERROR] weekly aggregation failed: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    main()
