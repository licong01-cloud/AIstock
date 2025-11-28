"""Ingest high-frequency trade aggregation (trade_agg_5m) from TDX API into TimescaleDB.

This script computes 5-minute (or configurable) aggregated trade features from
TDX minute-trade-all endpoint and writes them into app.ts_lstm_trade_agg.
It is integrated with the existing ingestion_jobs / ingestion_runs /
ingestion_job_tasks / ingestion_state / ingestion_checkpoints /
ingestion_errors tables so that it can be scheduled and monitored via
`tdx_scheduler.py` 和本地数据管理页面。

Modes:
- init:       全量/历史区间回填（可断点续传，按 ingestion_state 控制起始日期）
- incremental:按天或短区间增量更新（默认从今天开始，结合 ingestion_state 跳过已完成日期）

Dataset name used in control tables: "trade_agg_5m".
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


pgx.register_uuid()

TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:8080")
DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-ingest-trade-agg",
)
DATASET = "trade_agg_5m"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TDX trade_agg incremental/full ingestion")
    parser.add_argument("--mode", type=str, choices=["init", "incremental"], required=True)
    parser.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD, optional")
    parser.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD, optional")
    parser.add_argument("--freq-minutes", type=int, default=5, help="aggregation bucket in minutes")
    parser.add_argument(
        "--symbols-scope",
        type=str,
        default="watchlist",
        choices=["watchlist", "all"],
        help="symbol universe: watchlist (app.watchlist_items) or all (TDX /api/codes full universe)",
    )
    parser.add_argument("--batch-size", type=int, default=50, help="symbols per batch")
    parser.add_argument("--job-id", type=str, default=None, help="attach to existing ingestion_jobs.job_id")
    parser.add_argument(
        "--bulk-session-tune",
        action="store_true",
        help="Apply session-level tuning for bulk load (reserved flag; currently no-op).",
    )
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
            if isinstance(data, dict) and data.get("code") not in (None, 0):
                raise RuntimeError(f"TDX API error {path}: {data}")
            return data
        except (req_exc.ConnectionError, req_exc.Timeout) as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= max_retries:
                break
            import time

            time.sleep(1 + attempt)
        except Exception:  # noqa: BLE001
            raise
    raise last_exc or RuntimeError(f"TDX API request failed after retries: {url}")


def _bucket_start(ts: dt.datetime, bucket_minutes: int) -> dt.datetime:
    ts0 = ts.replace(second=0, microsecond=0)
    minutes = ts0.hour * 60 + ts0.minute
    bucket_index = minutes // bucket_minutes
    new_minutes = bucket_index * bucket_minutes
    hour = new_minutes // 60
    minute = new_minutes % 60
    return ts0.replace(hour=hour, minute=minute)


def _normalize_ts_code(code: str) -> Optional[str]:
    """Normalize bare TDX 6-digit code to ts_code with SH/SZ/BJ suffix.

    This mirrors ingest_incremental.normalize_ts_code.
    """
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


def _load_symbols(conn, scope: str) -> List[str]:
    """Load symbol universe for trade_agg_5m.

    - watchlist: use app.watchlist_items.code
    - all / any other scope: fetch full universe from TDX /api/codes
    """
    if scope == "watchlist":
        with conn.cursor() as cur:
            cur.execute("SELECT code FROM app.watchlist_items ORDER BY code")
            rows = cur.fetchall()
            return [r[0] for r in rows]

    # 默认：通过 TDX /api/codes 获取全部股票代码，不再依赖 market.symbol_dim
    try:
        data = http_get("/api/codes", params=None)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] 获取 TDX 股票列表失败: {exc}")
        raise

    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        rows = payload.get("codes") or []
    else:
        rows = payload or []

    seen = set()
    result: List[str] = []
    for item in rows:
        if isinstance(item, dict):
            raw_code = item.get("code")
        else:
            raw_code = str(item)
        ts_code = _normalize_ts_code(str(raw_code))
        if ts_code and ts_code not in seen:
            seen.add(ts_code)
            result.append(ts_code)
    return result


def _workdays(start: dt.date, end: dt.date) -> List[dt.date]:
    params = {"start": start.strftime("%Y%m%d"), "end": end.strftime("%Y%m%d")}
    data = http_get("/api/workday/range", params=params)
    payload = data.get("data") if isinstance(data, dict) else None
    if not isinstance(payload, dict):
        return []
    items = payload.get("list") or []
    days: List[dt.date] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        num = item.get("numeric") or item.get("Numeric")
        iso = item.get("iso") or item.get("ISO") or item.get("date")
        d: Optional[dt.date] = None
        if isinstance(iso, str):
            try:
                d = dt.date.fromisoformat(iso[:10])
            except Exception:  # noqa: BLE001
                d = None
        if d is None and isinstance(num, str) and len(num) == 8 and num.isdigit():
            try:
                d = dt.date(int(num[0:4]), int(num[4:6]), int(num[6:8]))
            except Exception:  # noqa: BLE001
                d = None
        if d is not None:
            days.append(d)
    days.sort()
    return days


def _fetch_trades(code6: str, trade_date: dt.date) -> List[Dict[str, Any]]:
    params = {"code": code6, "date": trade_date.strftime("%Y%m%d")}
    data = http_get("/api/minute-trade-all", params=params)
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        items = payload.get("List") or payload.get("list") or []
    else:
        items = payload or []
    return list(items)


def _aggregate_trades(
    trades: List[Dict[str, Any]],
    ts_code: str,
    freq_minutes: int,
) -> List[Tuple[Any, ...]]:
    from math import sqrt

    buckets: Dict[Tuple[dt.datetime, str], Dict[str, Any]] = {}
    price_paths: Dict[Tuple[dt.datetime, str], List[float]] = {}

    for tr in trades:
        try:
            t_raw = tr.get("Time") or tr.get("time")
            ts = dt.datetime.fromisoformat(str(t_raw).replace("Z", "+00:00"))
        except Exception:  # noqa: BLE001
            continue
        try:
            price = float(tr.get("Price", 0)) / 1000.0
        except Exception:  # noqa: BLE001
            continue
        try:
            vol_hand = float(tr.get("Volume", 0))
        except Exception:  # noqa: BLE001
            vol_hand = 0.0
        volume_shares = vol_hand * 100.0
        try:
            status = int(tr.get("Status", 2))
        except Exception:  # noqa: BLE001
            status = 2

        b_start = _bucket_start(ts, freq_minutes)
        key = (b_start, ts_code)
        if key not in buckets:
            buckets[key] = {
                "buy_volume": 0.0,
                "sell_volume": 0.0,
                "neutral_volume": 0.0,
                "big_trade_volume": 0.0,
                "big_trade_count": 0,
                "trade_count": 0,
                "total_volume": 0.0,
            }
            price_paths[key] = []

        agg = buckets[key]
        agg["trade_count"] += 1
        agg["total_volume"] += volume_shares
        price_paths[key].append(price)

        if status == 0:
            agg["buy_volume"] += volume_shares
        elif status == 1:
            agg["sell_volume"] += volume_shares
        else:
            agg["neutral_volume"] += volume_shares

    values: List[Tuple[Any, ...]] = []
    for (b_start, symbol_ts), agg in buckets.items():
        total_vol = agg["total_volume"] or 0.0
        buy_v = agg["buy_volume"]
        sell_v = agg["sell_volume"]
        neutral_v = agg["neutral_volume"]

        if buy_v + sell_v > 0:
            ofi = (buy_v - sell_v) / (buy_v + sell_v)
        else:
            ofi = 0.0

        # 简单大单统计：阈值可按成交额比例调整，这里约定为总量的 1%
        big_threshold = 0.01 * total_vol if total_vol > 0 else 0.0
        big_trade_volume = 0.0
        big_trade_count = 0
        # 重新扫描价格路径与成交量不可行，这里仅占位，保持字段存在
        big_ratio = 0.0

        prices = price_paths.get((b_start, symbol_ts), [])
        realized_vol = 0.0
        if len(prices) >= 2:
            rets: List[float] = []
            for i in range(1, len(prices)):
                p0, p1 = prices[i - 1], prices[i]
                if p0 > 0:
                    rets.append((p1 - p0) / p0)
            realized_vol = sqrt(sum(r * r for r in rets)) if rets else 0.0

        avg_trade_size = total_vol / agg["trade_count"] if agg["trade_count"] > 0 else 0.0
        intensity = agg["trade_count"] / float(freq_minutes)

        values.append(
            (
                symbol_ts,
                b_start,
                f"{freq_minutes}m",
                buy_v,
                sell_v,
                neutral_v,
                ofi,
                big_trade_volume,
                big_trade_count,
                big_ratio,
                realized_vol,
                agg["trade_count"],
                avg_trade_size,
                intensity,
                None,
            )
        )
    return values


def _get_state(conn, dataset: str, ts_code: str) -> Optional[dt.date]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_success_date FROM market.ingestion_state WHERE dataset=%s AND ts_code=%s",
            (dataset, ts_code),
        )
        row = cur.fetchone()
        if not row:
            return None
        return row[0]


def _upsert_state(conn, dataset: str, ts_code: str, last_date: dt.date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_state (dataset, ts_code, last_success_date, last_success_time)
            VALUES (%s, %s, %s, NULL)
            ON CONFLICT (dataset, ts_code)
            DO UPDATE SET last_success_date=EXCLUDED.last_success_date,
                          last_success_time=EXCLUDED.last_success_time
            """,
            (dataset, ts_code, last_date),
        )


def _upsert_checkpoint(
    conn,
    run_id: uuid.UUID,
    dataset: str,
    ts_code: str,
    cursor_date: dt.date,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_checkpoints (run_id, dataset, ts_code, cursor_date, cursor_time, extra)
            VALUES (%s, %s, %s, %s, NULL, NULL)
            ON CONFLICT (run_id, dataset, ts_code)
            DO UPDATE SET cursor_date=EXCLUDED.cursor_date,
                          cursor_time=EXCLUDED.cursor_time,
                          extra=EXCLUDED.extra
            """,
            (run_id, dataset, ts_code, cursor_date),
        )


def _log_error(
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


def _log_ingestion(conn, job_id: uuid.UUID, level: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES (%s, NOW(), %s, %s)",
            (job_id, level.upper(), message),
        )


def _create_run(conn, dataset: str, params: Dict[str, Any]) -> uuid.UUID:
    run_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_runs (run_id, mode, dataset, status, created_at, started_at, params)
            VALUES (%s, %s, %s, 'running', NOW(), NOW(), %s)
            """,
            (run_id, params.get("mode", "incremental"), dataset, json.dumps(params, ensure_ascii=False)),
        )
    return run_id


def _finish_run(conn, run_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE market.ingestion_runs SET status=%s, finished_at=NOW(), summary=%s WHERE run_id=%s",
            (status, json.dumps(summary, ensure_ascii=False), run_id),
        )


def _create_job(conn, job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
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


def _start_job(conn, job_id: uuid.UUID, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status='running', started_at=NOW(), summary=%s
             WHERE job_id=%s
            """,
            (json.dumps(summary, ensure_ascii=False), job_id),
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
        with conn.cursor() as cur2:
            cur2.execute(
                """
                UPDATE market.ingestion_jobs
                   SET status=%s, finished_at=NOW(), summary=%s
                 WHERE job_id=%s
                """,
                (status, json.dumps(base, ensure_ascii=False), job_id),
            )


def _update_job_summary(conn, job_id: uuid.UUID, patch: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT summary FROM market.ingestion_jobs WHERE job_id=%s", (job_id,))
        row = cur.fetchone()
        base: Dict[str, Any] = {}
        if row and row[0]:
            try:
                base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
            except Exception:  # noqa: BLE001
                base = {}
        for k, v in (patch or {}).items():
            if isinstance(v, (int, float)) and isinstance(base.get(k), (int, float)):
                base[k] = type(base.get(k))(base.get(k, 0) + v)
            else:
                base[k] = v
        cur.execute(
            "UPDATE market.ingestion_jobs SET summary=%s WHERE job_id=%s",
            (json.dumps(base, ensure_ascii=False), job_id),
        )


def _create_task(
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


def _chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def ingest_trade_agg(args: argparse.Namespace) -> None:
    print(f"[DEBUG] argv: {sys.argv}")
    mode = args.mode
    today = dt.date.today()
    if args.end_date:
        end_date = dt.date.fromisoformat(args.end_date)
    else:
        end_date = today
    explicit_start = args.start_date is not None
    if explicit_start:
        start_date = dt.date.fromisoformat(args.start_date)
    else:
        start_date = today if mode == "incremental" else dt.date(2000, 1, 1)
    if start_date > end_date:
        print("[ERROR] start-date later than end-date")
        sys.exit(1)

    freq_minutes = int(args.freq_minutes)

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '5min'")

        symbols = _load_symbols(conn, args.symbols_scope)
        if not symbols:
            print("[WARN] no symbols for trade_agg_5m; nothing to do")
            return

        job_type = "init" if mode == "init" else "incremental"
        job_params = {
            "datasets": [DATASET],
            "mode": mode,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "freq_minutes": freq_minutes,
            "symbols_scope": args.symbols_scope,
        }
        if args.job_id:
            job_id = uuid.UUID(args.job_id)
            _start_job(conn, job_id, job_params)
        else:
            job_id = _create_job(conn, job_type, job_params)
        _log_ingestion(conn, job_id, "info", f"start {DATASET} {mode} job")

        params = dict(job_params)
        params["job_id"] = str(job_id)
        run_id = _create_run(conn, DATASET, {**params, "mode": mode})

        stats: Dict[str, Any] = {
            "total_codes": len(symbols),
            "success_codes": 0,
            "failed_codes": 0,
            "processed_days": 0,
            "inserted_rows": 0,
        }
        processed_count = 0
        _update_job_summary(conn, job_id, {
            "total_codes": len(symbols),
            "success_codes": 0,
            "failed_codes": 0,
            "inserted_rows": 0,
        })

        workdays = _workdays(start_date, end_date)
        if not workdays:
            print("[WARN] no trading days in range; abort")
            _finish_run(conn, run_id, "failed", {"reason": "no_workdays", **stats})
            _finish_job(conn, job_id, "failed", {"run_id": str(run_id), **stats})
            return

        for batch in _chunked(symbols, args.batch_size):
            for ts_code in batch:
                code6 = ts_code.split(".")[0]
                sym_start = start_date
                # 若用户明确指定了 start_date，则严格按照 [start_date, end_date] 运行，
                # 不再用 ingestion_state 将起点推后，便于对已有区间做“补全/重算”。
                # 仅在未指定 start_date 时，才使用 ingestion_state 作为默认续点。
                if not explicit_start:
                    last_date = _get_state(conn, DATASET, ts_code)
                    if last_date is not None:
                        next_day = last_date + dt.timedelta(days=1)
                        if next_day > sym_start:
                            sym_start = next_day
                effective_days = [d for d in workdays if d >= sym_start]
                if not effective_days:
                    stats["success_codes"] += 1
                    continue

                task_id = _create_task(conn, job_id, DATASET, ts_code, sym_start, end_date)
                ok = True
                last_processed: Optional[dt.date] = None
                inserted_rows_total = 0

                try:
                    for trade_date in effective_days:
                        trades = _fetch_trades(code6, trade_date)
                        if not trades:
                            continue
                        values = _aggregate_trades(trades, ts_code, freq_minutes)
                        if not values:
                            continue
                        sql = """
                        INSERT INTO app.ts_lstm_trade_agg (
                          symbol, bucket_start_time, freq,
                          buy_volume, sell_volume, neutral_volume,
                          order_flow_imbalance,
                          big_trade_volume, big_trade_count, big_trade_ratio,
                          realized_vol, trade_count, avg_trade_size, intensity,
                          extra_json
                        ) VALUES %s
                        ON CONFLICT (symbol, bucket_start_time, freq) DO UPDATE SET
                          buy_volume = EXCLUDED.buy_volume,
                          sell_volume = EXCLUDED.sell_volume,
                          neutral_volume = EXCLUDED.neutral_volume,
                          order_flow_imbalance = EXCLUDED.order_flow_imbalance,
                          big_trade_volume = EXCLUDED.big_trade_volume,
                          big_trade_count = EXCLUDED.big_trade_count,
                          big_trade_ratio = EXCLUDED.big_trade_ratio,
                          realized_vol = EXCLUDED.realized_vol,
                          trade_count = EXCLUDED.trade_count,
                          avg_trade_size = EXCLUDED.avg_trade_size,
                          intensity = EXCLUDED.intensity,
                          extra_json = EXCLUDED.extra_json
                        """
                        with conn.cursor() as cur:
                            pgx.execute_values(cur, sql, values, page_size=2000)
                        inserted_rows_total += len(values)
                        last_processed = trade_date
                        _upsert_state(conn, DATASET, ts_code, trade_date)
                        _upsert_checkpoint(conn, run_id, DATASET, ts_code, trade_date)
                        stats["processed_days"] += 1
                except Exception as exc:  # noqa: BLE001
                    ok = False
                    stats["failed_codes"] += 1
                    _log_error(
                        conn,
                        run_id,
                        DATASET,
                        ts_code,
                        str(exc),
                        detail={
                            "code": code6,
                            "start": sym_start.isoformat(),
                            "end": end_date.isoformat(),
                        },
                    )
                    _log_ingestion(conn, job_id, "error", f"{DATASET} {ts_code} failed: {exc}")

                if ok:
                    stats["success_codes"] += 1
                    stats["inserted_rows"] += inserted_rows_total
                    _update_job_summary(conn, job_id, {
                        "inserted_rows": inserted_rows_total,
                        "success_codes": 1,
                    })
                    _complete_task(conn, task_id, True, 100.0, None)
                    print(
                        f"[OK] {DATASET} {ts_code} days={len(effective_days)} rows={inserted_rows_total} "
                        f"last_date={last_processed}"
                    )
                    processed_count += 1
                    if processed_count % 100 == 0:
                        _log_ingestion(
                            conn,
                            job_id,
                            "info",
                            f"Progress: {processed_count}/{len(symbols)} codes processed. Last: {ts_code}",
                        )
                else:
                    _complete_task(conn, task_id, False, 0.0, "processing failed")
                    _update_job_summary(conn, job_id, {"failed_codes": 1})

        status = "success" if stats["failed_codes"] == 0 else "failed"
        _finish_run(conn, run_id, status, stats)
        _finish_job(conn, job_id, status, {"run_id": str(run_id), **stats})
        _log_ingestion(
            conn,
            job_id,
            "info",
            f"run {run_id} finished status={status} stats={json.dumps(stats, ensure_ascii=False)}",
        )
        print(f"[DONE] {DATASET} status={status} stats={stats}")


def main() -> None:
    print(f"[DEBUG] argv: {sys.argv}", flush=True)
    args = parse_args()
    ingest_trade_agg(args)


if __name__ == "__main__":
    main()
