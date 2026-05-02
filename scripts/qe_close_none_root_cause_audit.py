#!/usr/bin/env python
"""Root-cause audit for Qlib `$close=None` warnings from existing QE artifacts.

This script consumes the JSON emitted by `qe_price_tradability_audit.py`.
It does not rerun QE and does not modify QE workspaces. For each warning row it
checks what is independently observable from DB state and Qlib bin feature
queries, then separates proven market states from remaining Qlib exchange
lookup inconsistencies.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _safe_float(value: Any) -> float | None:
    try:
        x = float(value)
        return x if np.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _table(rows: list[list[Any]], headers: list[str]) -> str:
    str_rows = [[str(c) for c in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    def fmt(row: list[str]) -> str:
        return "  ".join(cell.ljust(widths[i]) if i < len(row) - 1 else cell for i, cell in enumerate(row))

    lines = [fmt([str(h) for h in headers])]
    lines.append("  ".join("-" * w for w in widths))
    lines.extend(fmt(row) for row in str_rows)
    return "\n".join(lines)


def _init_qlib(day_uri: str, minute_uri: str):
    import qlib
    from qlib.constant import REG_CN

    qlib.init(provider_uri={"day": day_uri, "1min": minute_uri}, region=REG_CN)
    from qlib.data import D

    return D


def _qlib_day_features(D: Any, code: str, date: str) -> dict[str, Any]:
    fields = ["$close", "$factor", "$up_limit_price", "$down_limit_price"]
    try:
        df = D.features([code], fields, start_time=date, end_time=date, freq="day")
    except Exception as exc:  # Reported explicitly; not a silent fallback.
        return {"exists": False, "non_null_close": 0, "error": f"{type(exc).__name__}: {exc}"}
    if df is None or df.empty:
        return {"exists": False, "non_null_close": 0, "error": None}
    close = df["$close"] if "$close" in df else pd.Series(dtype=float)
    row = df.dropna(how="all").iloc[-1] if len(df.dropna(how="all")) else df.iloc[-1]
    return {
        "exists": True,
        "rows": int(len(df)),
        "non_null_close": int(close.notna().sum()),
        "close": _safe_float(row.get("$close")),
        "factor": _safe_float(row.get("$factor")),
        "up_limit": _safe_float(row.get("$up_limit_price")),
        "down_limit": _safe_float(row.get("$down_limit_price")),
        "error": None,
    }


def _qlib_minute_features(D: Any, code: str, date: str) -> dict[str, Any]:
    fields = ["$close", "$factor"]
    try:
        df = D.features(
            [code],
            fields,
            start_time=f"{date} 09:31:00",
            end_time=f"{date} 15:00:00",
            freq="1min",
        )
    except Exception as exc:  # Reported explicitly; not a silent fallback.
        return {"exists": False, "rows": 0, "non_null_close": 0, "error": f"{type(exc).__name__}: {exc}"}
    if df is None or df.empty:
        return {"exists": False, "rows": 0, "non_null_close": 0, "error": None}
    close = df["$close"] if "$close" in df else pd.Series(dtype=float)
    non_null = df.dropna(how="all")
    row = non_null.iloc[-1] if len(non_null) else df.iloc[-1]
    return {
        "exists": True,
        "rows": int(len(df)),
        "non_null_close": int(close.notna().sum()),
        "close": _safe_float(row.get("$close")),
        "factor": _safe_float(row.get("$factor")),
        "error": None,
    }


def _calendar_status(D: Any, dates: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for date in dates:
        try:
            day_cal = D.calendar(start_time=date, end_time=date, freq="day")
            min_cal = D.calendar(start_time=f"{date} 09:31:00", end_time=f"{date} 15:00:00", freq="1min")
        except Exception as exc:
            out[date] = {"day_calendar": False, "minute_calendar_rows": 0, "error": f"{type(exc).__name__}: {exc}"}
            continue
        out[date] = {
            "day_calendar": bool(len(day_cal)),
            "minute_calendar_rows": int(len(min_cal)),
            "error": None,
        }
    return out


def _instrument_status(D: Any, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], bool | None]:
    by_date: dict[str, set[str]] = {}
    for _code, date in pairs:
        if date in by_date:
            continue
        try:
            by_date[date] = set(D.list_instruments(D.instruments("all"), start_time=date, end_time=date, as_list=True))
        except Exception:
            by_date[date] = set()
    return {(code, date): (code in by_date.get(date, set())) for code, date in pairs}


def _classify(row: dict[str, Any], qlib: dict[str, Any]) -> str:
    db_state = row["db_state"]
    if db_state.startswith("SUSPEND_D_PRESENT"):
        return "SUSPEND_CONFIRMED"
    if db_state != "DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED":
        return "NON_TARGET_DB_STATE"
    if qlib["day"].get("error") or qlib["minute"].get("error") or qlib["calendar"].get("error"):
        return "QLIB_QUERY_ERROR_REPORTED"
    if not qlib["calendar"].get("day_calendar") or qlib["calendar"].get("minute_calendar_rows", 0) == 0:
        return "QLIB_CALENDAR_MISSING"
    if not qlib.get("instrument_in_all"):
        return "QLIB_INSTRUMENT_ALL_MISSING"
    if qlib["day"].get("non_null_close", 0) == 0:
        return "QLIB_DAY_CLOSE_MISSING"
    if qlib["minute"].get("non_null_close", 0) == 0:
        return "QLIB_MINUTE_CLOSE_MISSING"
    return "QLIB_FEATURES_PRESENT_EXCHANGE_CLOSE_NONE"


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    price = json.loads(Path(args.price_audit_json).read_text(encoding="utf-8"))
    rows = price["warning_audits"]
    target_rows = [r for r in rows if r["db_state"] == "DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED"]
    if args.max_rows > 0:
        target_rows = target_rows[: args.max_rows]

    D = _init_qlib(args.qlib_day_uri, args.qlib_minute_uri)
    pairs = sorted({(r["stock"], r["start"]) for r in target_rows})
    dates = sorted({date for _code, date in pairs})
    calendars = _calendar_status(D, dates)
    instruments = _instrument_status(D, pairs)

    feature_cache: dict[tuple[str, str], dict[str, Any]] = {}
    for code, date in pairs:
        feature_cache[(code, date)] = {
            "day": _qlib_day_features(D, code, date),
            "minute": _qlib_minute_features(D, code, date),
            "calendar": calendars[date],
            "instrument_in_all": instruments[(code, date)],
        }

    audited: list[dict[str, Any]] = []
    for r in target_rows:
        qlib = feature_cache[(r["stock"], r["start"])]
        audited.append({**r, "qlib": qlib, "root_cause_class": _classify(r, qlib)})

    all_state_counts = Counter(r["db_state"] for r in rows)
    class_counts = Counter(r["root_cause_class"] for r in audited)
    return {
        "task_id": args.task_id,
        "price_audit_json": str(args.price_audit_json),
        "total_warning_rows": len(rows),
        "all_db_state_counts": dict(all_state_counts),
        "target_db_present_rows": len(target_rows),
        "unique_target_pairs": len(pairs),
        "unique_target_dates": len(dates),
        "root_cause_counts": dict(class_counts),
        "audited_rows": audited,
    }


def write_md(result: dict[str, Any], output: Path) -> None:
    lines = [
        f"# QE Close-None Root Cause Audit: {result['task_id']}",
        "",
        "Scope: existing price/tradability audit JSON plus Qlib bin feature queries only. No QE task was rerun and no strategy logging was changed.",
        "",
    ]

    rows = [[k, v] for k, v in sorted(result["all_db_state_counts"].items())]
    lines += ["## Warning DB-State Baseline", "", "```text", _table(rows, ["DBState", "Rows"]), "```", ""]

    rows = [[k, v] for k, v in sorted(result["root_cause_counts"].items())]
    lines += ["## DB-Present Not-Suspended Root Cause Classes", "", "```text", _table(rows, ["Class", "Rows"]), "```", ""]

    sample_rows = []
    for r in result["audited_rows"][:40]:
        q = r["qlib"]
        sample_rows.append(
            [
                r["loop"],
                r["stock"],
                r["start"],
                r["root_cause_class"],
                q["calendar"].get("day_calendar"),
                q["calendar"].get("minute_calendar_rows"),
                q.get("instrument_in_all"),
                q["day"].get("non_null_close"),
                q["minute"].get("non_null_close"),
            ]
        )
    lines += [
        "## Sample Rows",
        "",
        "```text",
        _table(
            sample_rows,
            ["Loop", "Stock", "Date", "Class", "DayCal", "MinCalRows", "Instrument", "QDayClose", "QMinClose"],
        ),
        "```",
        "",
    ]

    lines += [
        "## Evidence Notes",
        "",
        "- `SUSPEND_CONFIRMED` means DB `suspend_d` already explains the no-trade state.",
        "- `QLIB_MINUTE_CLOSE_MISSING` means DB daily/minute/limit rows exist and DB `suspend_d` has no suspension row, while Qlib 1min calendar/instrument rows exist but `$close` is all null for the stock-date pair.",
        "- `QLIB_FEATURES_PRESENT_EXCHANGE_CLOSE_NONE` means DB data and Qlib feature API both have day/minute close, calendar and instrument membership, while the original Qlib exchange warning still reported `$close=None`.",
        "- This script does not infer the exact Qlib exchange branch when artifacts do not persist that branch; it separates proven suspension/feature-coverage causes from remaining exchange/instrument-state lookup causes.",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="QE close-none root-cause audit from existing artifacts")
    ap.add_argument("task_id")
    ap.add_argument("--price-audit-json", required=True)
    ap.add_argument("--qlib-day-uri", default="/home/lc999/data/qlib_bin")
    ap.add_argument("--qlib-minute-uri", default="/home/lc999/data/qlib_minute_bin")
    ap.add_argument("--max-rows", type=int, default=0, help="0 means audit all DB-present warning rows")
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--output-md", required=True)
    args = ap.parse_args()
    result = run_audit(args)
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_md(result, Path(args.output_md))
    print(f"wrote {out_json}")
    print(f"wrote {args.output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
