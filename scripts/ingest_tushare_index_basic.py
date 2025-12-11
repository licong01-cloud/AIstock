from __future__ import annotations

"""Ingest Tushare index_basic (指数基础信息) into market.index_basic.

- 仅支持 init 模式：一次性全量同步所有指数基础信息。
- 可选 --truncate 在初始化前清空目标表。
- 复用 ingestion_jobs / ingestion_logs 记录任务状态，方便任务监视器展示。
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

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
    application_name="AIstock-ingest-index_basic",
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
        description="Ingest Tushare index_basic into TimescaleDB (market.index_basic)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="init",
        choices=["init"],
        help="Ingestion mode (only init is supported)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate market.index_basic before init (destructive)",
    )
    parser.add_argument(
        "--job-id",
        type=str,
        default=None,
        help="Existing job id to attach and update",
    )
    return parser.parse_args()


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


def _upsert_index_basic(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.index_basic (ts_code, name, fullname, market, publisher, index_type, "
        "category, base_date, base_point, list_date, weight_rule, \"desc\", exp_date) "
        "VALUES %s "
        "ON CONFLICT (ts_code) DO UPDATE SET "
        "name=EXCLUDED.name, fullname=EXCLUDED.fullname, market=EXCLUDED.market, "
        "publisher=EXCLUDED.publisher, index_type=EXCLUDED.index_type, category=EXCLUDED.category, "
        "base_date=EXCLUDED.base_date, base_point=EXCLUDED.base_point, list_date=EXCLUDED.list_date, "
        "weight_rule=EXCLUDED.weight_rule, \"desc\"=EXCLUDED.\"desc\", exp_date=EXCLUDED.exp_date"
    )
    values = [
        (
            (r.get("ts_code") or "").strip(),
            (r.get("name") or "").strip(),
            (r.get("fullname") or "").strip(),
            (r.get("market") or "").strip(),
            (r.get("publisher") or "").strip(),
            (r.get("index_type") or "").strip(),
            (r.get("category") or "").strip(),
            r.get("base_date"),
            r.get("base_point"),
            r.get("list_date"),
            (r.get("weight_rule") or "").strip(),
            r.get("desc"),
            r.get("exp_date"),
        )
        for r in rows
        if r.get("ts_code")
    ]
    if not values:
        return 0
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    return len(values)


def run_ingestion(conn, pro, job_id) -> Dict[str, Any]:
    # 按市场维度拆分抓取，避免未来单市场潜在的条数限制导致数据不完整。
    # 官方文档中的 market 说明：
    # MSCI: MSCI 指数
    # CSI:  中证指数
    # SSE:  上交所指数
    # SZSE: 深交所指数
    # CICC: 中金指数
    # SW:   申万指数
    # OTH:  其他指数
    markets = ["MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH"]

    import pandas as pd  # 延迟导入，避免脚本顶部污染

    frames: List[Any] = []
    for mkt in markets:
        try:
            df = pro.index_basic(market=mkt)
        except Exception as exc:  # noqa: BLE001
            _log(conn, job_id, "error", f"index_basic market={mkt} failed: {exc}")
            continue
        if df is None or df.empty:
            continue
        frames.append(df)

    if not frames:
        _log(conn, job_id, "warn", "tushare index_basic returned empty dataframe for all markets")
        return {"total": 0, "inserted": 0}

    df_all = pd.concat(frames, ignore_index=True)
    # 以 ts_code 为主键去重，避免同一指数在多市场标记下重复。
    if "ts_code" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["ts_code"])

    rows: List[Dict[str, Any]] = []
    for _, row in df_all.iterrows():
        rows.append({
            "ts_code": row.get("ts_code"),
            "name": row.get("name"),
            "fullname": row.get("fullname"),
            "market": row.get("market"),
            "publisher": row.get("publisher"),
            "index_type": row.get("index_type"),
            "category": row.get("category"),
            "base_date": row.get("base_date"),
            "base_point": row.get("base_point"),
            "list_date": row.get("list_date"),
            "weight_rule": row.get("weight_rule"),
            "desc": row.get("desc"),
            "exp_date": row.get("exp_date"),
        })

    total = len(rows)
    inserted = _upsert_index_basic(conn, rows)
    return {"total": total, "inserted": inserted}


def main() -> None:
    args = parse_args()
    mode = (args.mode or "init").strip().lower()
    if mode != "init":
        print(f"[ERROR] unsupported mode: {mode}")
        sys.exit(1)

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        pro = pro_api()

        if args.truncate:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE market.index_basic")
            print("[WARN] TRUNCATE market.index_basic executed before init")

        job_summary = {
            "dataset": "index_basic",
            "mode": mode,
        }
        if args.job_id:
            import uuid

            job_id = uuid.UUID(args.job_id)
            _start_existing_job(conn, job_id, job_summary)
        else:
            job_id = _create_job(conn, mode, job_summary)

        _log(conn, job_id, "info", "start tushare index_basic ingestion")

        try:
            stats = run_ingestion(conn, pro, job_id)
            # stats 中包含 total/inserted，此外显式写入 inserted_rows，方便任务监视器展示“新增行数”。
            summary = {"stats": stats, "inserted_rows": int(stats.get("inserted") or 0)}
            _finish_job(conn, job_id, "success", summary)
            print(f"[DONE] index_basic mode={mode} stats={stats}")
        except Exception as exc:  # noqa: BLE001
            _finish_job(conn, job_id, "failed", {"error": str(exc)})
            print(f"[ERROR] index_basic failed: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    main()
