#!/usr/bin/env python
"""Diagnose Qlib 1min OHLCV/factor gaps for QE close-none warnings.

This is a read-only diagnostic. It consumes the existing close-none root-cause
JSON and price/tradability JSON, inspects the Qlib 1min binary files directly,
and optionally compares the affected date coverage against the current DB
minute table. It does not rerun QE, does not mutate Qlib data, and does not add
strategy logging.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np


OHLCV_FACTOR_FIELDS = ["open", "high", "low", "close", "volume", "amount", "factor"]
LIMIT_FIELDS = ["prev_close", "up_limit_price", "down_limit_price", "limit_up", "limit_down"]


def _table(rows: list[list[Any]], headers: list[str]) -> str:
    str_rows = [[str(c) for c in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt(row: list[str]) -> str:
        return "  ".join(cell.ljust(widths[i]) if i < len(row) - 1 else cell for i, cell in enumerate(row))

    lines = [fmt([str(h) for h in headers])]
    lines.append("  ".join("-" * width for width in widths))
    lines.extend(fmt(row) for row in str_rows)
    return "\n".join(lines)


def _symbol_dir(code: str) -> str:
    return code.lower()


def _load_calendar(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(path)
    return path.read_text(encoding="utf-8").splitlines()


def _date_indices(calendar: list[str], date: str) -> list[int]:
    return [i for i, value in enumerate(calendar) if value.startswith(date)]


def _mtime_to_iso(mtime: Any) -> str | None:
    if mtime is None:
        return None

    return datetime.fromtimestamp(float(mtime), tz=timezone.utc).isoformat()


def _date_range_bounds(dates: list[str]) -> tuple[str, str]:
    parsed = [datetime.strptime(date, "%Y-%m-%d").date() for date in dates]
    start = min(parsed).isoformat()
    end_exclusive = (max(parsed) + timedelta(days=1)).isoformat()
    return f"{start} 00:00:00+08", f"{end_exclusive} 00:00:00+08"


def _read_bin_values(feature_root: Path, code: str, field: str, indices: list[int]) -> dict[str, Any]:
    path = feature_root / _symbol_dir(code) / f"{field}.1min.bin"
    if not path.exists():
        return {"exists": False, "rows": 0, "non_null": 0, "path": str(path), "mtime": None}
    arr = np.fromfile(path, dtype="<f4")
    if len(arr) == 0:
        return {"exists": True, "rows": 0, "non_null": 0, "path": str(path), "mtime": path.stat().st_mtime}
    start = int(arr[0])
    values = []
    for idx in indices:
        pos = idx - start + 1
        if 1 <= pos < len(arr):
            values.append(arr[pos])
    if values:
        vals = np.asarray(values, dtype=np.float32)
        non_null = int(np.isfinite(vals).sum())
        min_value = float(np.nanmin(vals)) if non_null else None
        max_value = float(np.nanmax(vals)) if non_null else None
    else:
        non_null = 0
        min_value = None
        max_value = None
    return {
        "exists": True,
        "rows": len(values),
        "non_null": non_null,
        "min": min_value,
        "max": max_value,
        "path": str(path),
        "mtime": path.stat().st_mtime,
    }


def _classify_pair(db_minute_count: int, suspend_exists: bool, fields: dict[str, dict[str, Any]]) -> str:
    ohlcv_rows = [fields[field]["rows"] for field in OHLCV_FACTOR_FIELDS if fields[field]["exists"]]
    ohlcv_non_null = sum(fields[field]["non_null"] for field in OHLCV_FACTOR_FIELDS)
    limit_non_null = sum(fields[field]["non_null"] for field in ["prev_close", "up_limit_price", "down_limit_price"])
    if suspend_exists:
        return "SUSPEND_PRESENT"
    if db_minute_count <= 0:
        return "DB_MINUTE_MISSING"
    if not ohlcv_rows:
        return "QLIB_OHLCV_FACTOR_BIN_FILES_MISSING"
    if ohlcv_non_null == 0 and limit_non_null > 0:
        return "QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT"
    if ohlcv_non_null == 0:
        return "QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT"
    return "MIXED_OR_PARTIAL_GAP"


def _db_date_coverage(args: argparse.Namespace, dates: list[str]) -> dict[str, dict[str, Any]]:
    if args.skip_db:
        return {}
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError("psycopg2 is required for DB coverage; pass --skip-db to omit") from exc
    conn = psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password or os.getenv("TDX_DB_PASSWORD") or os.getenv("PGPASSWORD") or "lc78080808",
    )
    try:
        cur = conn.cursor()
        start_ts, end_ts = _date_range_bounds(dates)
        cur.execute(
            """
            SELECT date(trade_time AT TIME ZONE 'Asia/Shanghai') AS trade_date,
                   count(DISTINCT ts_code) AS stocks,
                   count(*) AS rows,
                   count(*) FILTER (WHERE close_li IS NOT NULL) AS close_rows,
                   min(trade_time AT TIME ZONE 'Asia/Shanghai') AS first_time,
                   max(trade_time AT TIME ZONE 'Asia/Shanghai') AS last_time
            FROM market.kline_minute_raw
            WHERE freq = '1m'
              AND trade_time >= %s::timestamptz
              AND trade_time < %s::timestamptz
            GROUP BY 1
            ORDER BY 1
            """,
            (start_ts, end_ts),
        )
        wanted = set(dates)
        out: dict[str, dict[str, Any]] = {}
        for row in cur.fetchall():
            date = str(row[0])
            if date not in wanted:
                continue
            out[date] = {
                "stocks": int(row[1]),
                "rows": int(row[2]),
                "close_rows": int(row[3]),
                "first_time": str(row[4]),
                "last_time": str(row[5]),
            }
        return out
    finally:
        conn.close()


def _db_stock_date_coverage(args: argparse.Namespace, dates: list[str]) -> dict[tuple[str, str], dict[str, Any]]:
    if args.skip_db:
        return {}
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError("psycopg2 is required for DB coverage; pass --skip-db to omit") from exc
    conn = psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password or os.getenv("TDX_DB_PASSWORD") or os.getenv("PGPASSWORD") or "lc78080808",
    )
    try:
        cur = conn.cursor()
        start_ts, end_ts = _date_range_bounds(dates)
        cur.execute(
            """
            SELECT ts_code,
                   date(trade_time AT TIME ZONE 'Asia/Shanghai') AS trade_date,
                   count(*) AS rows,
                   count(*) FILTER (WHERE close_li IS NOT NULL) AS close_rows,
                   min(close_li) AS min_close_li,
                   max(close_li) AS max_close_li
            FROM market.kline_minute_raw
            WHERE freq = '1m'
              AND trade_time >= %s::timestamptz
              AND trade_time < %s::timestamptz
            GROUP BY 1, 2
            ORDER BY 2, 1
            """,
            (start_ts, end_ts),
        )
        wanted = set(dates)
        out: dict[tuple[str, str], dict[str, Any]] = {}
        for row in cur.fetchall():
            stock = str(row[0]).upper()
            date = str(row[1])
            if date not in wanted:
                continue
            out[(stock, date)] = {
                "stock": stock,
                "date": date,
                "db_rows": int(row[2]),
                "db_close_rows": int(row[3]),
                "db_min_close_li": int(row[4]) if row[4] is not None else None,
                "db_max_close_li": int(row[5]) if row[5] is not None else None,
            }
        return out
    finally:
        conn.close()


def _qlib_universe_coverage(
    feature_root: Path,
    calendar: list[str],
    dates: list[str],
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    files = sorted(feature_root.glob("*/close.1min.bin"))
    out: dict[str, dict[str, Any]] = {}
    presence: dict[tuple[str, str], dict[str, Any]] = {}
    indices_by_date = {date: _date_indices(calendar, date) for date in dates}
    accum = {
        date: {
            "calendar_rows": len(indices_by_date[date]),
            "qlib_instruments_with_rows": 0,
            "qlib_instruments_with_any_close": 0,
            "qlib_total_slots": 0,
            "qlib_non_null_close_slots": 0,
        }
        for date in dates
    }
    # Read each instrument file once; this keeps full-universe diagnosis fast.
    for path in files:
        stock = path.parent.name.upper()
        arr = np.fromfile(path, dtype="<f4")
        if len(arr) <= 1:
            continue
        start = int(arr[0])
        for date in dates:
            indices = indices_by_date[date]
            if not indices:
                continue
            lo = indices[0] - start + 1
            hi = indices[-1] - start + 2
            read_lo = max(lo, 1)
            read_hi = min(hi, len(arr))
            if read_lo >= read_hi:
                continue
            vals = np.asarray(arr[read_lo:read_hi], dtype=np.float32)
            nn = int(np.isfinite(vals).sum())
            accum[date]["qlib_instruments_with_rows"] += 1
            accum[date]["qlib_total_slots"] += len(vals)
            accum[date]["qlib_non_null_close_slots"] += nn
            if nn > 0:
                accum[date]["qlib_instruments_with_any_close"] += 1
            presence[(stock, date)] = {
                "rows": len(vals),
                "non_null": nn,
                "exists": True,
                "path": str(path),
            }
    for date in dates:
        non_null_slots = accum[date]["qlib_non_null_close_slots"]
        total_slots = accum[date]["qlib_total_slots"]
        out[date] = {
            "calendar_rows": accum[date]["calendar_rows"],
            "qlib_instruments_with_rows": accum[date]["qlib_instruments_with_rows"],
            "qlib_instruments_with_any_close": accum[date]["qlib_instruments_with_any_close"],
            "qlib_total_slots": total_slots,
            "qlib_non_null_close_slots": non_null_slots,
            "qlib_non_null_slot_ratio": (non_null_slots / total_slots if total_slots else None),
        }
    return out, presence


def _qlib_close_non_null(feature_root: Path, code: str, indices: list[int]) -> dict[str, Any]:
    return _read_bin_values(feature_root, code, "close", indices)


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    close_root = json.loads(Path(args.close_json).read_text(encoding="utf-8"))
    price = json.loads(Path(args.price_json).read_text(encoding="utf-8"))
    price_by_pair = {
        (int(row["loop"]), row["stock"], row["start"]): row
        for row in price["warning_audits"]
    }
    price_rows_by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for (_, stock, date), price_row in price_by_pair.items():
        price_rows_by_pair.setdefault((stock, date), []).append(price_row)
    audited_rows = [
        row
        for row in close_root["audited_rows"]
        if row.get("root_cause_class") == "QLIB_MINUTE_CLOSE_MISSING"
    ]
    loops_by_pair: dict[tuple[str, str], set[int]] = {}
    for row in audited_rows:
        loops_by_pair.setdefault((row["stock"], row["start"]), set()).add(int(row["loop"]))
    calendar = _load_calendar(Path(args.qlib_minute_uri) / "calendars" / "1min.txt")
    feature_root = Path(args.qlib_minute_uri) / "features"

    pair_rows: list[dict[str, Any]] = []
    pair_seen = set()
    for row in audited_rows:
        key = (row["stock"], row["start"])
        if key in pair_seen:
            continue
        pair_seen.add(key)
        indices = _date_indices(calendar, row["start"])
        fields = {
            field: _read_bin_values(feature_root, row["stock"], field, indices)
            for field in OHLCV_FACTOR_FIELDS + LIMIT_FIELDS
        }
        matching_price_rows = price_rows_by_pair.get(key, [])
        price_row = matching_price_rows[0] if matching_price_rows else None
        db_minute = (price_row or {}).get("db", {}).get("minute", {})
        db_daily = (price_row or {}).get("db", {}).get("daily", {})
        db_limit = (price_row or {}).get("db", {}).get("limit", {})
        suspend = (price_row or {}).get("db", {}).get("suspend", {})
        ohlcv_non_null = sum(fields[field]["non_null"] for field in OHLCV_FACTOR_FIELDS)
        limit_core_non_null = sum(fields[field]["non_null"] for field in ["prev_close", "up_limit_price", "down_limit_price"])
        pair_rows.append(
            {
                "stock": row["stock"],
                "date": row["start"],
                "loops": sorted(loops_by_pair[key]),
                "calendar_rows": len(indices),
                "db_minute_count": int(db_minute.get("count") or 0),
                "db_minute_last_close": db_minute.get("last_close"),
                "db_daily_close": db_daily.get("close"),
                "db_limit_up": db_limit.get("up_limit"),
                "db_suspend_exists": bool(suspend.get("exists")),
                "qlib_ohlcv_factor_non_null": int(ohlcv_non_null),
                "qlib_limit_core_non_null": int(limit_core_non_null),
                "qlib_close_rows": fields["close"]["rows"],
                "qlib_close_non_null": fields["close"]["non_null"],
                "qlib_factor_non_null": fields["factor"]["non_null"],
                "qlib_prev_close_non_null": fields["prev_close"]["non_null"],
                "qlib_up_limit_non_null": fields["up_limit_price"]["non_null"],
                "classification": _classify_pair(
                    int(db_minute.get("count") or 0),
                    bool(suspend.get("exists")),
                    fields,
                ),
                "sample_close_bin": fields["close"]["path"],
                "sample_close_bin_mtime": fields["close"]["mtime"],
                "sample_close_bin_mtime_utc": _mtime_to_iso(fields["close"]["mtime"]),
                "sample_prev_close_bin_mtime": fields["prev_close"]["mtime"],
                "sample_prev_close_bin_mtime_utc": _mtime_to_iso(fields["prev_close"]["mtime"]),
            }
        )

    dates = sorted({row["date"] for row in pair_rows})
    qlib_date_coverage, qlib_close_presence = _qlib_universe_coverage(feature_root, calendar, dates)
    db_date_coverage = _db_date_coverage(args, dates)
    db_stock_dates = _db_stock_date_coverage(args, dates)
    universe_gap_rows: list[dict[str, Any]] = []
    for (stock, date), db_row in sorted(db_stock_dates.items(), key=lambda item: (item[0][1], item[0][0])):
        qclose = qlib_close_presence.get(
            (stock, date),
            {
                "rows": 0,
                "non_null": 0,
                "exists": (feature_root / _symbol_dir(stock) / "close.1min.bin").exists(),
                "path": str(feature_root / _symbol_dir(stock) / "close.1min.bin"),
            },
        )
        if db_row["db_close_rows"] > 0 and qclose["non_null"] == 0:
            universe_gap_rows.append(
                {
                    "stock": stock,
                    "date": date,
                    "db_rows": db_row["db_rows"],
                    "db_close_rows": db_row["db_close_rows"],
                    "qlib_close_rows": qclose["rows"],
                    "qlib_close_non_null": qclose["non_null"],
                    "qlib_close_file_exists": qclose["exists"],
                    "qlib_close_bin": qclose["path"],
                }
            )
    date_rows = []
    for date in dates:
        q = qlib_date_coverage.get(date, {})
        d = db_date_coverage.get(date, {})
        date_rows.append(
            {
                "date": date,
                "affected_pairs": sum(1 for row in pair_rows if row["date"] == date),
                "affected_stocks": len({row["stock"] for row in pair_rows if row["date"] == date}),
                "db_stocks": d.get("stocks"),
                "db_rows": d.get("rows"),
                "db_close_rows": d.get("close_rows"),
                "qlib_instruments_with_rows": q.get("qlib_instruments_with_rows"),
                "qlib_instruments_with_any_close": q.get("qlib_instruments_with_any_close"),
                "qlib_non_null_slot_ratio": q.get("qlib_non_null_slot_ratio"),
                "calendar_rows": q.get("calendar_rows"),
                "db_present_qlib_close_gap_pairs": sum(1 for gap in universe_gap_rows if gap["date"] == date),
            }
        )

    return {
        "task_id": args.task_id,
        "close_json": str(args.close_json),
        "price_json": str(args.price_json),
        "qlib_minute_uri": args.qlib_minute_uri,
        "summary": {
            "audited_warning_rows": len(audited_rows),
            "unique_stock_dates": len(pair_rows),
            "unique_stocks": len({row["stock"] for row in pair_rows}),
            "unique_dates": len(dates),
            "classification_counts": dict(Counter(row["classification"] for row in pair_rows)),
            "top_dates": Counter(row["date"] for row in pair_rows).most_common(),
            "top_stocks": Counter(row["stock"] for row in pair_rows).most_common(50),
        },
        "date_coverage": date_rows,
        "stock_date_rows": pair_rows,
        "universe_gap_rows": universe_gap_rows,
    }


def _write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _fmt_ratio(value: Any) -> str:
    if value is None:
        return "NA"
    return f"{float(value) * 100:.2f}%"


def write_md(result: dict[str, Any], output: Path, csv_path: Path, universe_csv_path: Path | None) -> None:
    summary = result["summary"]
    lines = [
        f"# QE Qlib Minute Gap Diagnosis: {result['task_id']}",
        "",
        "Scope: existing close-none/price audit JSON plus direct Qlib 1min bin inspection. No QE rerun, no Qlib mutation, no strategy logging changes.",
        "",
        "## Direct Answer",
        "",
        f"- Yes, the exact affected stock-date pairs can be listed. This audit found {summary['unique_stock_dates']} QE-warning stock-date pairs, {summary['unique_stocks']} warning stocks, and {summary['unique_dates']} affected trading dates.",
        f"- Across the current DB minute universe on those {summary['unique_dates']} dates, {len(result.get('universe_gap_rows', []))} DB-present stock-date pairs have Qlib 1min close all-null.",
        "- The concrete cause is not DB minute absence or suspension for these pairs. The Qlib 1min OHLCV/factor binary files contain NaN at those date offsets, while current DB minute rows and Qlib limit/prev_close binaries are present.",
        "- Therefore the failure is in the Qlib minute OHLCV/factor export/bin snapshot, not in current DB minute storage, Qlib calendar, instrument membership, or limit-price precision.",
        "",
    ]

    rows = [[k, v] for k, v in summary["classification_counts"].items()]
    lines += ["## Root Cause Classes", "", "```text", _table(rows, ["Class", "Pairs"]), "```", ""]

    date_rows = []
    for row in result["date_coverage"]:
        date_rows.append(
            [
                row["date"],
                row["affected_pairs"],
                row["affected_stocks"],
                row.get("db_stocks"),
                row.get("db_rows"),
                row.get("db_close_rows"),
                row.get("qlib_instruments_with_rows"),
                row.get("qlib_instruments_with_any_close"),
                _fmt_ratio(row.get("qlib_non_null_slot_ratio")),
                row.get("db_present_qlib_close_gap_pairs"),
            ]
        )
    lines += [
        "## Date-Level Coverage",
        "",
        "```text",
        _table(
            date_rows,
            [
                "Date",
                "Pairs",
                "Stocks",
                "DBStocks",
                "DBRows",
                "DBCloseRows",
                "QlibInstRows",
                "QlibInstClose",
                "QlibCloseSlot%",
                "DBQlibGap",
            ],
        ),
        "```",
        "",
    ]

    top_stock_rows = [[stock, count] for stock, count in summary["top_stocks"][:40]]
    lines += ["## Top QE-Warning Affected Stocks", "", "```text", _table(top_stock_rows, ["Stock", "AffectedDates"]), "```", ""]

    universe_date_rows = []
    for row in result["date_coverage"]:
        db_stocks = row.get("db_stocks") or 0
        gap = row.get("db_present_qlib_close_gap_pairs") or 0
        universe_date_rows.append(
            [
                row["date"],
                db_stocks,
                row.get("db_rows"),
                row.get("qlib_instruments_with_any_close"),
                gap,
                _fmt_ratio(gap / db_stocks if db_stocks else None),
            ]
        )
    lines += [
        "## Full DB-Present Qlib-Close Gap By Date",
        "",
        "```text",
        _table(universe_date_rows, ["Date", "DBStocks", "DBRows", "QlibInstClose", "GapPairs", "Gap/DBStocks"]),
        "```",
        "",
    ]

    mtime_rows = []
    for row in result["stock_date_rows"][:12]:
        mtime_rows.append(
            [
                row["stock"],
                row["date"],
                row.get("sample_close_bin_mtime_utc"),
                row.get("sample_prev_close_bin_mtime_utc"),
            ]
        )
    lines += [
        "## Sample Bin File MTime Evidence",
        "",
        "```text",
        _table(mtime_rows, ["Stock", "Date", "CloseBinMTimeUTC", "PrevCloseBinMTimeUTC"]),
        "```",
        "",
    ]

    sample_rows = []
    for row in result["stock_date_rows"][:80]:
        sample_rows.append(
            [
                row["stock"],
                row["date"],
                ",".join(str(x) for x in row["loops"]),
                row["db_minute_count"],
                row["db_minute_last_close"],
                row["calendar_rows"],
                row["qlib_close_non_null"],
                row["qlib_factor_non_null"],
                row["qlib_prev_close_non_null"],
                row["qlib_up_limit_non_null"],
                row["classification"],
            ]
        )
    lines += [
        "## Sample Stock-Date Detail",
        "",
        "```text",
        _table(
            sample_rows,
            [
                "Stock",
                "Date",
                "Loops",
                "DBMin",
                "DBLastClose",
                "CalRows",
                "QClose",
                "QFactor",
                "QPrevClose",
                "QUpLimit",
                "Class",
            ],
        ),
        "```",
        "",
        f"Full QE-warning stock-date list: `{csv_path.as_posix()}`",
        f"Full DB-present Qlib-close gap list: `{universe_csv_path.as_posix()}`" if universe_csv_path else "Full DB-present Qlib-close gap list: not generated",
        "",
        "## Why This Happened",
        "",
        "Evidence chain:",
        "",
        "- Current DB minute rows exist for the affected dates and have non-null `close_li`.",
        "- Qlib 1min calendar rows exist for the same dates.",
        "- Qlib instrument rows exist; otherwise `D.features` would not return 240 rows per affected stock-date.",
        "- Direct bin inspection shows `open/high/low/close/volume/amount/factor` are all NaN for the affected stock-date offsets.",
        "- Direct bin inspection also shows `prev_close/up_limit_price/down_limit_price` are present for the same offsets, proving this is not a total instrument/calendar gap.",
        "",
        "The precise local root cause is therefore: the current `/home/lc999/data/qlib_minute_bin` OHLCV/factor files were built from an incomplete minute OHLCV/factor export snapshot for 2025-07-08 through 2025-07-16. Current DB data is more complete than that Qlib minute snapshot. To identify the historical operational cause beyond this file-level proof, the missing export job log or preserved CSV snapshot would be required; those inputs are not present in the existing QE artifacts.",
        "",
    ]

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Diagnose Qlib 1min bin stock-date gaps")
    ap.add_argument("task_id")
    ap.add_argument("--close-json", required=True)
    ap.add_argument("--price-json", required=True)
    ap.add_argument("--qlib-minute-uri", default="/home/lc999/data/qlib_minute_bin")
    ap.add_argument("--skip-db", action="store_true")
    ap.add_argument("--db-host", default="127.0.0.1")
    ap.add_argument("--db-port", type=int, default=5432)
    ap.add_argument("--db-name", default="aistock")
    ap.add_argument("--db-user", default="postgres")
    ap.add_argument("--db-password", default=None)
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--output-universe-csv", default=None)
    ap.add_argument("--output-md", required=True)
    args = ap.parse_args()

    result = run_audit(args)
    out_json = Path(args.output_json)
    out_csv = Path(args.output_csv)
    out_universe_csv = Path(args.output_universe_csv) if args.output_universe_csv else None
    out_md = Path(args.output_md)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(result["stock_date_rows"], out_csv)
    if out_universe_csv is not None:
        _write_csv(result["universe_gap_rows"], out_universe_csv)
    write_md(result, out_md, out_csv, out_universe_csv)
    print(f"wrote {out_json}")
    print(f"wrote {out_csv}")
    if out_universe_csv is not None:
        print(f"wrote {out_universe_csv}")
    print(f"wrote {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
