"""Seed market.dataset_date_refresh_audit from existing local market tables.

This is an explicit operator utility. Runtime services still fail fast when the
audit table is missing or a required dataset/date has not been marked ready.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2.extras
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.db.pg_pool import get_conn


@dataclass(frozen=True)
class AuditSeedSpec:
    dataset: str
    table_name: str
    date_column: str
    sparse_ok: bool = False
    data_source: str = "seed_existing_rows"


SPECS = {
    "suspend_d": AuditSeedSpec(
        dataset="suspend_d",
        table_name="market.suspend_d",
        date_column="trade_date",
        sparse_ok=True,
        data_source="tushare",
    ),
    "stk_limit": AuditSeedSpec(
        dataset="stk_limit",
        table_name="market.stk_limit",
        date_column="trade_date",
        sparse_ok=False,
        data_source="tushare",
    ),
    "kline_daily_raw": AuditSeedSpec(
        dataset="kline_daily_raw",
        table_name="market.kline_daily_raw",
        date_column="trade_date",
    ),
    "daily_basic": AuditSeedSpec(
        dataset="daily_basic",
        table_name="market.daily_basic",
        date_column="trade_date",
    ),
    "stock_moneyflow_ts": AuditSeedSpec(
        dataset="stock_moneyflow_ts",
        table_name="market.moneyflow_ts",
        date_column="trade_date",
    ),
    "sector_data": AuditSeedSpec(
        dataset="sector_data",
        table_name="market.sector_data",
        date_column="trade_date",
    ),
    "index_daily": AuditSeedSpec(
        dataset="index_daily",
        table_name="market.index_daily",
        date_column="trade_date",
    ),
    "cyq_perf": AuditSeedSpec(
        dataset="cyq_perf",
        table_name="market.cyq_perf",
        date_column="trade_date",
        data_source="tushare",
    ),
}


def _parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _resolve_date_range(spec: AuditSeedSpec, start_date: date | None, end_date: date | None) -> tuple[date, date]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT min({spec.date_column}), max({spec.date_column}) FROM {spec.table_name}")
            row = cur.fetchone()
    min_date, max_date = row if row else (None, None)
    resolved_start = start_date or min_date
    resolved_end = end_date or max_date
    if resolved_start is None or resolved_end is None:
        raise RuntimeError(f"{spec.dataset}: cannot resolve date range from {spec.table_name}")
    if resolved_start > resolved_end:
        raise RuntimeError(f"{spec.dataset}: start_date cannot be after end_date")
    return resolved_start, resolved_end


def _load_row_counts(spec: AuditSeedSpec, start_date: date, end_date: date) -> dict[date, int]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {spec.date_column}, count(*)
                FROM {spec.table_name}
                WHERE {spec.date_column} >= %s AND {spec.date_column} <= %s
                GROUP BY {spec.date_column}
                ORDER BY {spec.date_column}
                """,
                (start_date, end_date),
            )
            return {row[0]: int(row[1]) for row in cur.fetchall()}


def _load_trading_days(start_date: date, end_date: date) -> list[date]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cal_date
                FROM market.trading_calendar
                WHERE cal_date >= %s AND cal_date <= %s AND is_trading = TRUE
                ORDER BY cal_date
                """,
                (start_date, end_date),
            )
            return [row[0] for row in cur.fetchall()]


def seed_dataset(spec: AuditSeedSpec, start_date: date | None, end_date: date | None) -> int:
    start_date, end_date = _resolve_date_range(spec, start_date, end_date)
    row_counts = _load_row_counts(spec, start_date, end_date)
    if spec.sparse_ok:
        target_dates = _load_trading_days(start_date, end_date)
        if not target_dates:
            raise RuntimeError("market.trading_calendar has no trading days for sparse dataset audit seed")
    else:
        target_dates = sorted(row_counts)
    if not target_dates:
        raise RuntimeError(f"{spec.dataset}: no local rows found for audit seed")

    values: list[tuple[Any, ...]] = []
    for trade_date in target_dates:
        values.append(
            (
                spec.dataset,
                trade_date,
                spec.data_source,
                "success",
                int(row_counts.get(trade_date, 0)),
                int(row_counts.get(trade_date, 0)),
                "empty_valid" if spec.sparse_ok and int(row_counts.get(trade_date, 0)) == 0 else "ok",
                psycopg2.extras.Json(
                    {
                        "seeded_from_existing_rows": True,
                        "table": spec.table_name,
                        "script": "scripts/seed_dataset_refresh_audit.py",
                    }
                ),
            )
        )

    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO market.dataset_date_refresh_audit (
                    dataset, trade_date, data_source, status, row_count,
                    written_rows, quality_status, metadata
                ) VALUES %s
                ON CONFLICT (dataset, trade_date, data_source) DO UPDATE SET
                    status = EXCLUDED.status,
                    row_count = EXCLUDED.row_count,
                    written_rows = EXCLUDED.written_rows,
                    quality_status = EXCLUDED.quality_status,
                    failure_category = NULL,
                    refreshed_at = NOW(),
                    error_message = NULL,
                    metadata = EXCLUDED.metadata
                """,
                values,
            )
    return len(values)


def main() -> None:
    load_dotenv(REPO_ROOT / ".env", override=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=sorted(SPECS), required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()

    count = seed_dataset(
        SPECS[args.dataset],
        _parse_date(args.start_date),
        _parse_date(args.end_date),
    )
    print(f"seeded {count} audit rows for {args.dataset}")


if __name__ == "__main__":
    main()
