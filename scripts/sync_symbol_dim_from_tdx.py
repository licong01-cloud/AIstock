"""Sync market.symbol_dim from TDX /api/codes.

This script treats TDX /api/codes as the master source of the A-share
universe and upserts rows into market.symbol_dim.

- ts_code: 6-digit code + suffix (SH/SZ/BJ)
- exchange: SH/SZ/BJ
- name: optional security name from TDX

It is idempotent: running multiple times will keep symbol_dim aligned with
TDX while preserving any extra columns not mentioned in the INSERT.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx
import requests
from requests import exceptions as req_exc


TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:8080")
DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

EXCHANGE_MAP = {"sh": "SH", "sz": "SZ", "bj": "BJ"}


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


def fetch_codes_from_tdx(exchanges: Optional[Iterable[str]]) -> List[Dict[str, Any]]:
    """Fetch raw code records from TDX /api/codes for the given exchanges.

    Returns a list of dicts with at least {"code", "exchange", "name"?}.
    """
    targets = [ex.strip().lower() for ex in exchanges if ex] if exchanges else ["all"]
    seen: set[Tuple[str, str]] = set()
    result: List[Dict[str, Any]] = []

    for exch in targets:
        params = {"exchange": exch} if exch and exch != "all" else {}
        data = http_get("/api/codes", params=params)
        payload = data.get("data") if isinstance(data, dict) else None
        if isinstance(payload, dict):
            rows = payload.get("codes") or []
        else:
            rows = payload or []
        for item in rows:
            if not isinstance(item, dict):
                continue
            raw_code = str(item.get("code") or "").strip()
            raw_exch = str(item.get("exchange") or "").strip().lower()
            name = item.get("name")
            if not raw_code or not raw_exch:
                continue
            key = (raw_code, raw_exch)
            if key in seen:
                continue
            seen.add(key)
            result.append({"code": raw_code, "exchange": raw_exch, "name": name})

    return result


def build_symbol_dim_rows(raw_items: List[Dict[str, Any]]) -> List[Tuple[str, str, str, Optional[str]]]:
    rows: List[Tuple[str, str, str, Optional[str]]] = []
    for item in raw_items:
        raw_code = item.get("code")
        raw_exch = item.get("exchange")
        name = item.get("name")
        ts_code = normalize_ts_code(str(raw_code))
        if not ts_code:
            continue
        exch_norm = EXCHANGE_MAP.get(str(raw_exch).lower())
        if not exch_norm:
            # 保留原始 exchange 以避免丢失信息
            exch_norm = str(raw_exch).upper() or "?"
        # (ts_code, symbol, exchange, name)
        rows.append((ts_code, str(raw_code), exch_norm, str(name) if name is not None else None))
    return rows


def upsert_symbol_dim(conn, rows: List[Tuple[str, str, str, Optional[str]]]) -> int:
    if not rows:
        return 0
    sql = (
        "INSERT INTO market.symbol_dim (ts_code, symbol, exchange, name) "
        "VALUES %s"
    )
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def update_data_stats(conn, inserted_count: int) -> None:
    """Update market.data_stats for data_kind='symbol_dim'."""
    # 统计最新总数
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MIN(ts_code), MAX(ts_code) FROM market.symbol_dim")
        row = cur.fetchone()
        total_count = row[0] if row else 0
        min_ts = row[1]
        max_ts = row[2]

    now = datetime.datetime.now().isoformat()
    extra_info = json.dumps({
        "desc": "全市场股票基础信息表",
        "total_count": total_count,
        "last_sync_count": inserted_count,
        "min_ts_code": min_ts,
        "max_ts_code": max_ts,
    }, ensure_ascii=False)

    sql = """
    INSERT INTO market.data_stats (
        data_kind, table_name, min_date, max_date, row_count, last_updated_at, extra_info
    ) VALUES (
        'symbol_dim', 'market.symbol_dim', NULL, NULL, %s, %s, %s
    ) ON CONFLICT (data_kind) DO UPDATE SET
        row_count = EXCLUDED.row_count,
        last_updated_at = EXCLUDED.last_updated_at,
        extra_info = EXCLUDED.extra_info
    """
    with conn.cursor() as cur:
        cur.execute(sql, (total_count, now, extra_info))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync market.symbol_dim from TDX /api/codes")
    parser.add_argument(
        "--exchanges",
        type=str,
        default="sh,sz",
        help="Comma separated exchanges to sync from {sh,sz,bj}; use 'all' to let backend decide",
    )
    parser.add_argument(
        "--job-id",
        type=str,
        default="",
        help="Ingestion job ID for logging context",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    exchanges = [ex.strip().lower() for ex in args.exchanges.split(",") if ex.strip()]
    job_id_info = f"[Job={args.job_id}] " if args.job_id else ""

    print(f"{job_id_info}Using DB config host={DB_CFG['host']} db={DB_CFG['dbname']}")
    print(f"{job_id_info}Fetching codes from TDX (exchanges={exchanges}) ...")
    try:
        raw_items = fetch_codes_from_tdx(exchanges)
    except Exception as e:
        print(f"{job_id_info}[ERROR] Failed to fetch codes: {e}")
        raise

    print(f"{job_id_info}Fetched {len(raw_items)} raw code records from TDX /api/codes")

    rows = build_symbol_dim_rows(raw_items)
    print(f"{job_id_info}Prepared {len(rows)} symbol_dim rows (distinct ts_code)")

    if not rows:
        print(f"{job_id_info}[WARN] No rows to upsert into market.symbol_dim; aborting.")
        return

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '5min'")
            cur.execute("TRUNCATE TABLE market.symbol_dim")
        
        inserted = upsert_symbol_dim(conn, rows)
        update_data_stats(conn, inserted)

    print(f"{job_id_info}[DONE] Upserted {inserted} rows into market.symbol_dim and updated data_stats")


if __name__ == "__main__":
    main()
