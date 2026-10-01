"""Read-only one-time validation for a completed ETF share-size backfill."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from backend.db.pg_pool import get_conn


SUMMARY_SQL = """
SELECT COUNT(*)::bigint AS row_count,
       COUNT(DISTINCT ts_code)::bigint AS symbol_count,
       COUNT(DISTINCT trade_date)::bigint AS trade_date_count,
       MIN(trade_date) AS min_date,
       MAX(trade_date) AS max_date,
       COUNT(*) FILTER (WHERE total_share IS NULL)::bigint AS total_share_nulls,
       COUNT(*) FILTER (WHERE total_size IS NULL)::bigint AS total_size_nulls,
       COUNT(*) FILTER (WHERE nav IS NULL)::bigint AS nav_nulls,
       COUNT(*) FILTER (WHERE close IS NULL)::bigint AS close_nulls,
       COUNT(*) FILTER (
           WHERE total_share::text IN ('NaN', 'Infinity', '-Infinity')
              OR total_size::text IN ('NaN', 'Infinity', '-Infinity')
              OR nav::text IN ('NaN', 'Infinity', '-Infinity')
              OR close::text IN ('NaN', 'Infinity', '-Infinity')
       )::bigint AS non_finite_rows,
       COUNT(*) FILTER (
           WHERE total_share < 0 OR total_size < 0 OR nav < 0 OR close < 0
       )::bigint AS negative_rows
FROM market.etf_share_size
"""

ANOMALY_SQL = """
WITH bounds AS (
    SELECT MIN(trade_date) AS min_date, MAX(trade_date) AS max_date
    FROM market.etf_share_size
), expected AS (
    SELECT cal_date
    FROM market.trading_calendar, bounds
    WHERE is_trading = TRUE
      AND cal_date BETWEEN bounds.min_date AND bounds.max_date
), actual AS (
    SELECT DISTINCT trade_date FROM market.etf_share_size
)
SELECT
    (SELECT COUNT(*) FROM expected e LEFT JOIN actual a ON a.trade_date = e.cal_date
      WHERE a.trade_date IS NULL)::bigint AS missing_trade_dates,
    (SELECT COUNT(*) FROM actual a LEFT JOIN market.trading_calendar c
       ON c.cal_date = a.trade_date AND c.is_trading = TRUE
      WHERE c.cal_date IS NULL)::bigint AS non_trading_dates,
    (SELECT COUNT(*) FROM (
        SELECT trade_date, ts_code, COUNT(*)
        FROM market.etf_share_size
        GROUP BY trade_date, ts_code
        HAVING COUNT(*) > 1
    ) duplicate_keys)::bigint AS duplicate_keys
"""

DAILY_SQL = """
SELECT MIN(row_count)::bigint AS min_rows_per_day,
       MAX(row_count)::bigint AS max_rows_per_day,
       AVG(row_count)::numeric(18,2) AS avg_rows_per_day
FROM (
    SELECT trade_date, COUNT(*)::bigint AS row_count
    FROM market.etf_share_size
    GROUP BY trade_date
) daily
"""


def _row_to_dict(cursor: Any, row: Any) -> dict[str, Any]:
    names = [item[0] for item in cursor.description]
    return dict(zip(names, row, strict=True))


def _json_default(value: Any) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def validate() -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(SUMMARY_SQL)
            summary = _row_to_dict(cur, cur.fetchone())
            cur.execute(ANOMALY_SQL)
            anomalies = _row_to_dict(cur, cur.fetchone())
            cur.execute(DAILY_SQL)
            daily = _row_to_dict(cur, cur.fetchone())

    blocking = {
        key: int(anomalies.get(key) or 0)
        for key in ("missing_trade_dates", "non_trading_dates", "duplicate_keys")
    }
    blocking.update(
        {
            "non_finite_rows": int(summary.get("non_finite_rows") or 0),
            "negative_rows": int(summary.get("negative_rows") or 0),
        }
    )
    return {
        "schema_version": "etf_share_size_full_validation_v1",
        "read_only": True,
        "summary": summary,
        "daily": daily,
        "anomalies": anomalies,
        "blocking_counts": blocking,
        "ok": int(summary.get("row_count") or 0) > 0 and not any(blocking.values()),
        "notes": [
            "null source values are reported but are not filled or treated as structural failures",
            "run once after the authorized historical backfill; this is not a monthly release gate",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="optional JSON receipt path")
    args = parser.parse_args()
    result = validate()
    payload = json.dumps(result, ensure_ascii=False, indent=2, default=_json_default)
    print(payload)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
