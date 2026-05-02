#!/usr/bin/env python
"""Planner and guarded repair tool for direct Qlib 1min bin repair.

The scan/build-plan/verify-plan subcommands are read-only. The apply-plan
subcommand mutates only explicitly planned offsets after confirmation text,
pre-apply checksums, backup creation, and readback validation.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np


PATCH_FIELDS = ["open", "high", "low", "close", "volume", "amount", "factor", "limit_up", "limit_down"]
VERIFY_ONLY_FIELDS = ["prev_close", "up_limit_price", "down_limit_price"]
ALL_FIELDS = PATCH_FIELDS + VERIFY_ONLY_FIELDS
DB_PASSWORD_DEFAULT = "lc78080808"


def table(rows: list[list[Any]], headers: list[str]) -> str:
    text_rows = [[str(cell) for cell in row] for row in rows]
    widths = [len(str(h)) for h in headers]
    for row in text_rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt(row: list[str]) -> str:
        padded = [cell.ljust(widths[idx]) for idx, cell in enumerate(row[:-1])]
        return "  ".join(padded + [row[-1]]) if row else ""

    return "\n".join([fmt([str(h) for h in headers]), "  ".join("-" * w for w in widths)] + [fmt(r) for r in text_rows])


def symbol_dir(stock: str) -> str:
    return stock.lower()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_parent(path)
    fields = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_json(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def closest_existing_parent(path: Path) -> Path:
    current = path
    while not current.exists():
        if current.parent == current:
            raise FileNotFoundError(f"no existing parent for {path}")
        current = current.parent
    return current


def load_calendar(root: Path) -> list[str]:
    path = root / "calendars" / "1min.txt"
    if not path.exists():
        raise FileNotFoundError(path)
    return path.read_text(encoding="utf-8").splitlines()


def date_indices(calendar: list[str]) -> dict[str, list[int]]:
    out: dict[str, list[int]] = defaultdict(list)
    for idx, value in enumerate(calendar):
        out[value[:10]].append(idx)
    return dict(out)


def read_bin(path: Path) -> tuple[int, np.ndarray]:
    if not path.exists():
        raise FileNotFoundError(path)
    arr = np.fromfile(path, dtype="<f4")
    if len(arr) == 0:
        raise ValueError(f"empty bin file: {path}")
    return int(arr[0]), arr


def write_bin_atomic(path: Path, arr: np.ndarray) -> None:
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        arr.astype("<f4", copy=False).tofile(tmp)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def values_at(arr: np.ndarray, start_idx: int, indices: list[int]) -> np.ndarray:
    positions = [idx - start_idx + 1 for idx in indices]
    if not positions:
        return np.asarray([], dtype=np.float32)
    if min(positions) < 1 or max(positions) >= len(arr):
        raise IndexError(f"bin offset out of range: start={start_idx} min={min(positions)} max={max(positions)} len={len(arr)}")
    return arr[positions].astype(np.float32, copy=False)


def parse_dates(value: str | None) -> list[str]:
    if not value:
        return []
    return sorted({part.strip()[:10] for part in value.replace(";", ",").split(",") if part.strip()})


def parse_stock_dates(path: Path) -> list[tuple[str, str]]:
    rows = read_csv(path)
    out: list[tuple[str, str]] = []
    seen = set()
    for row in rows:
        stock = (row.get("stock") or row.get("ts_code") or row.get("instrument") or "").strip().upper()
        date = (row.get("date") or row.get("trade_date") or "").strip()[:10]
        if not stock or not date:
            raise ValueError(f"input row missing stock/date: {row}")
        key = (stock, date)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return sorted(out, key=lambda x: (x[1], x[0]))


def db_connect(args: argparse.Namespace):
    import psycopg2

    return psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password or os.getenv("TDX_DB_PASSWORD") or os.getenv("PGPASSWORD") or DB_PASSWORD_DEFAULT,
    )


def date_bounds(dates: Iterable[str]) -> tuple[str, str]:
    parsed = [datetime.strptime(d, "%Y-%m-%d").date() for d in dates]
    start = min(parsed).isoformat()
    end_excl = (max(parsed) + timedelta(days=1)).isoformat()
    return f"{start} 00:00:00+08", f"{end_excl} 00:00:00+08"


def fetch_pairs_for_dates(args: argparse.Namespace, dates: list[str]) -> list[tuple[str, str]]:
    if not dates:
        raise ValueError("--dates is required when --input-stock-date-csv is omitted")
    start_ts, end_ts = date_bounds(dates)
    wanted = set(dates)
    with db_connect(args) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ts_code, date(trade_time AT TIME ZONE 'Asia/Shanghai') AS trade_date
            FROM market.kline_minute_raw
            WHERE freq='1m'
              AND trade_time >= %s::timestamptz
              AND trade_time < %s::timestamptz
            GROUP BY 1, 2
            ORDER BY 2, 1
            """,
            (start_ts, end_ts),
        )
        return [(str(r[0]).upper(), str(r[1])) for r in cur.fetchall() if str(r[1]) in wanted]


def fetch_db_counts(args: argparse.Namespace, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
    if not pairs:
        return {}
    dates = sorted({date for _, date in pairs})
    stocks = sorted({stock for stock, _ in pairs})
    wanted = set(pairs)
    start_ts, end_ts = date_bounds(dates)
    with db_connect(args) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ts_code,
                   date(trade_time AT TIME ZONE 'Asia/Shanghai') AS trade_date,
                   count(*) AS rows,
                   count(open_li) AS open_rows,
                   count(high_li) AS high_rows,
                   count(low_li) AS low_rows,
                   count(close_li) AS close_rows,
                   count(volume_hand) AS volume_rows,
                   count(amount_li) AS amount_rows
            FROM market.kline_minute_raw
            WHERE freq='1m'
              AND ts_code = ANY(%s)
              AND trade_time >= %s::timestamptz
              AND trade_time < %s::timestamptz
            GROUP BY 1, 2
            """,
            (stocks, start_ts, end_ts),
        )
        out: dict[tuple[str, str], dict[str, Any]] = {}
        for row in cur.fetchall():
            key = (str(row[0]).upper(), str(row[1]))
            if key in wanted:
                out[key] = {
                    "db_rows": int(row[2]),
                    "db_open_rows": int(row[3]),
                    "db_high_rows": int(row[4]),
                    "db_low_rows": int(row[5]),
                    "db_close_rows": int(row[6]),
                    "db_volume_rows": int(row[7]),
                    "db_amount_rows": int(row[8]),
                }
        return out


def fetch_adj_counts(args: argparse.Namespace, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], int]:
    if not pairs:
        return {}
    dates = sorted({date for _, date in pairs})
    stocks = sorted({stock for stock, _ in pairs})
    with db_connect(args) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ts_code, trade_date, count(adj_factor)
            FROM market.adj_factor
            WHERE ts_code = ANY(%s)
              AND trade_date >= %s::date
              AND trade_date <= %s::date
            GROUP BY 1, 2
            """,
            (stocks, min(dates), max(dates)),
        )
        return {(str(r[0]).upper(), str(r[1])): int(r[2]) for r in cur.fetchall()}


def fetch_limits(args: argparse.Namespace, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
    if not pairs:
        return {}
    dates = sorted({date for _, date in pairs})
    stocks = sorted({stock for stock, _ in pairs})
    with db_connect(args) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ts_code, trade_date, pre_close, up_limit, down_limit
            FROM market.stk_limit
            WHERE ts_code = ANY(%s)
              AND trade_date >= %s::date
              AND trade_date <= %s::date
            """,
            (stocks, min(dates), max(dates)),
        )
        out: dict[tuple[str, str], dict[str, Any]] = {}
        for r in cur.fetchall():
            out[(str(r[0]).upper(), str(r[1]))] = {
                "pre_close": float(r[2]) if r[2] is not None else None,
                "up_limit": float(r[3]) if r[3] is not None else None,
                "down_limit": float(r[4]) if r[4] is not None else None,
            }
        return out


def command_scan(args: argparse.Namespace) -> int:
    root = Path(args.qlib_minute_uri)
    calendar = load_calendar(root)
    idx_by_date = date_indices(calendar)
    pairs = parse_stock_dates(Path(args.input_stock_date_csv)) if args.input_stock_date_csv else fetch_pairs_for_dates(args, parse_dates(args.dates))
    if args.max_pairs:
        pairs = pairs[: args.max_pairs]
    if not pairs:
        raise ValueError("no stock-date pairs to scan")

    db_counts = fetch_db_counts(args, pairs) if not args.skip_db else {}
    adj_counts = fetch_adj_counts(args, pairs) if not args.skip_db else {}
    limits = fetch_limits(args, pairs) if not args.skip_db else {}

    scan_rows: list[dict[str, Any]] = []
    matrix_rows: list[dict[str, Any]] = []
    grouped: dict[str, list[str]] = defaultdict(list)
    for stock, date in pairs:
        grouped[stock].append(date)

    for stock, dates in grouped.items():
        cache: dict[str, tuple[bool, int | None, np.ndarray | None, Path]] = {}
        for field in ALL_FIELDS:
            path = root / "features" / symbol_dir(stock) / f"{field}.1min.bin"
            if path.exists():
                start, arr = read_bin(path)
                cache[field] = (True, start, arr, path)
            else:
                cache[field] = (False, None, None, path)

        for date in dates:
            indices = idx_by_date.get(date, [])
            if not indices:
                raise ValueError(f"date {date} not in Qlib calendar")
            field_counts: dict[str, int] = {}
            for field in ALL_FIELDS:
                exists, start, arr, path = cache[field]
                rows = 0
                non_null = 0
                if exists and start is not None and arr is not None:
                    vals = values_at(arr, start, indices)
                    rows = int(len(vals))
                    non_null = int(np.isfinite(vals).sum())
                field_counts[field] = non_null
                matrix_rows.append(
                    {
                        "stock": stock,
                        "date": date,
                        "field": field,
                        "calendar_rows": len(indices),
                        "qlib_rows": rows,
                        "qlib_non_null": non_null,
                        "qlib_file_exists": exists,
                        "qlib_bin": str(path),
                    }
                )

            patch_required = [f for f in PATCH_FIELDS if field_counts.get(f, 0) < len(indices)]
            verify_required = [f for f in VERIFY_ONLY_FIELDS if field_counts.get(f, 0) < len(indices)]
            lim = limits.get((stock, date), {})
            row = {
                "stock": stock,
                "date": date,
                "calendar_rows": len(indices),
                **db_counts.get((stock, date), {}),
                "adj_rows": adj_counts.get((stock, date)),
                "has_limit_row": bool(lim),
                "prev_close": lim.get("pre_close"),
                "up_limit": lim.get("up_limit"),
                "down_limit": lim.get("down_limit"),
                **{f"qlib_{field}_non_null": field_counts.get(field, 0) for field in ALL_FIELDS},
                "patch_required_fields": ",".join(patch_required),
                "verify_required_fields": ",".join(verify_required),
                "patchable_candidate": bool(patch_required),
            }
            scan_rows.append(row)

    write_csv(Path(args.output_scan_csv), scan_rows)
    write_csv(Path(args.output_field_matrix_csv), matrix_rows)
    summary = scan_summary(scan_rows)
    if args.output_md:
        write_scan_md(summary, Path(args.output_md), Path(args.output_scan_csv), Path(args.output_field_matrix_csv))
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def scan_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dates = sorted({r["date"] for r in rows})
    by_date = []
    for date in dates:
        sub = [r for r in rows if r["date"] == date]
        by_date.append([date, len(sub), len({r["stock"] for r in sub}), sum(str(r["patchable_candidate"]) == "True" for r in sub)])
    missing = Counter()
    for row in rows:
        for field in PATCH_FIELDS:
            if int(row.get(f"qlib_{field}_non_null") or 0) < int(row["calendar_rows"]):
                missing[field] += 1
    return {
        "pairs": len(rows),
        "stocks": len({r["stock"] for r in rows}),
        "dates": len(dates),
        "date_range": [min(dates), max(dates)] if dates else None,
        "patchable_candidates": sum(str(r["patchable_candidate"]) == "True" for r in rows),
        "missing_field_counts": dict(missing),
        "by_date": by_date,
    }


def write_scan_md(summary: dict[str, Any], path: Path, scan_csv: Path, matrix_csv: Path) -> None:
    lines = [
        "# P0 Qlib Minute Bin Gap Scan",
        "",
        "This is a read-only scan. It did not modify Qlib bin files.",
        "",
        "## Summary",
        "",
        "```text",
        table(
            [
                ["pairs", summary["pairs"]],
                ["stocks", summary["stocks"]],
                ["dates", summary["dates"]],
                ["date_range", summary["date_range"]],
                ["patchable_candidates", summary["patchable_candidates"]],
            ],
            ["Metric", "Value"],
        ),
        "```",
        "",
        "## By Date",
        "",
        "```text",
        table(summary["by_date"], ["Date", "Pairs", "Stocks", "PatchableCandidates"]),
        "```",
        "",
        "## Missing Patch Fields",
        "",
        "```text",
        table([[k, v] for k, v in summary["missing_field_counts"].items()], ["Field", "StockDatePairs"]),
        "```",
        "",
        f"Scan CSV: `{scan_csv.as_posix()}`",
        f"Field matrix CSV: `{matrix_csv.as_posix()}`",
    ]
    ensure_parent(path)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def command_build_plan(args: argparse.Namespace) -> int:
    root = Path(args.qlib_minute_uri)
    calendar_path = root / "calendars" / "1min.txt"
    idx_by_date = date_indices(load_calendar(root))
    rows = read_csv(Path(args.scan_csv))
    records = []
    skipped = []
    files: dict[str, dict[str, Any]] = {}

    for row in rows:
        stock = row["stock"].upper()
        date = row["date"][:10]
        cal_rows = int(row["calendar_rows"])
        patch_fields = [f for f in row.get("patch_required_fields", "").split(",") if f]
        if not patch_fields:
            skipped.append({"stock": stock, "date": date, "reason": "no_patch_fields"})
            continue
        if args.require_all_patch_fields:
            missing = [f for f in PATCH_FIELDS if f not in patch_fields]
            if missing:
                skipped.append({"stock": stock, "date": date, "reason": "not_all_patch_fields_missing:" + ",".join(missing)})
                continue
        db_rows = int(float(row.get("db_rows") or 0))
        if db_rows != cal_rows:
            skipped.append({"stock": stock, "date": date, "reason": f"db_rows_{db_rows}_calendar_{cal_rows}"})
            continue
        indices = idx_by_date.get(date, [])
        if len(indices) != cal_rows:
            skipped.append({"stock": stock, "date": date, "reason": "calendar_index_count_mismatch"})
            continue
        file_missing = False
        for field in patch_fields:
            path = root / "features" / symbol_dir(stock) / f"{field}.1min.bin"
            if not path.exists():
                skipped.append({"stock": stock, "date": date, "reason": f"missing_file:{field}"})
                file_missing = True
                break
            if str(path) not in files:
                stat = path.stat()
                files[str(path)] = {
                    "stock": stock,
                    "field": field,
                    "path": str(path),
                    "size": int(stat.st_size),
                    "mtime_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    "sha256": sha256_file(path) if args.hash_files else None,
                }
        if file_missing:
            continue
        records.append(
            {
                "stock": stock,
                "date": date,
                "calendar_rows": cal_rows,
                "calendar_start_index": int(indices[0]),
                "calendar_end_index": int(indices[-1]),
                "patch_fields": patch_fields,
                "verify_fields": VERIFY_ONLY_FIELDS,
            }
        )

    plan = {
        "schema_version": 1,
        "mode": "dry_run_only_no_bin_mutation",
        "qlib_minute_uri": str(root),
        "calendar_path": str(calendar_path),
        "calendar_sha256": sha256_file(calendar_path),
        "scan_csv": str(Path(args.scan_csv)),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "records": records,
        "unique_files": sorted(files.values(), key=lambda x: (x["stock"], x["field"])),
        "skipped": skipped,
        "summary": {
            "records": len(records),
            "unique_stocks": len({r["stock"] for r in records}),
            "unique_dates": len({r["date"] for r in records}),
            "unique_files": len(files),
            "skipped": len(skipped),
            "hash_files": bool(args.hash_files),
        },
    }
    plan["plan_sha256"] = sha256_json({k: v for k, v in plan.items() if k != "plan_sha256"})
    write_json(Path(args.output_plan_json), plan)
    write_plan_md(plan, Path(args.output_plan_md), Path(args.output_plan_json))
    print(json.dumps(plan["summary"], ensure_ascii=False, indent=2))
    return 0


def write_plan_md(plan: dict[str, Any], path: Path, plan_json: Path) -> None:
    by_date = Counter(r["date"] for r in plan["records"])
    by_field = Counter(field for r in plan["records"] for field in r["patch_fields"])
    lines = [
        "# P0 Qlib Minute Bin Patch Plan Dry Run",
        "",
        "This plan is read-only metadata for a future explicit repair. It did not write Qlib bin files.",
        "",
        "## Summary",
        "",
        "```text",
        table([[k, v] for k, v in plan["summary"].items()], ["Metric", "Value"]),
        "```",
        "",
        "## By Date",
        "",
        "```text",
        table([[k, v] for k, v in sorted(by_date.items())], ["Date", "Records"]),
        "```",
        "",
        "## Patch Fields",
        "",
        "```text",
        table([[k, v] for k, v in sorted(by_field.items())], ["Field", "RecordCount"]),
        "```",
        "",
        f"Plan JSON: `{plan_json.as_posix()}`",
        f"Plan SHA256: `{plan['plan_sha256']}`",
    ]
    ensure_parent(path)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def command_verify_plan(args: argparse.Namespace) -> int:
    plan = json.loads(Path(args.plan_json).read_text(encoding="utf-8"))
    root = Path(plan["qlib_minute_uri"])
    calendar = load_calendar(root)
    idx_by_date = date_indices(calendar)
    records = [(r["stock"], r["date"]) for r in plan["records"]]
    failures: list[str] = []
    warnings: list[str] = []

    if sha256_file(Path(plan["calendar_path"])) != plan["calendar_sha256"]:
        failures.append("calendar_sha256_mismatch")

    db_counts = fetch_db_counts(args, records)
    adj_counts = fetch_adj_counts(args, records)
    limits = fetch_limits(args, records)
    for rec in plan["records"]:
        stock = rec["stock"]
        date = rec["date"]
        key = (stock, date)
        cal_rows = len(idx_by_date.get(date, []))
        if cal_rows != int(rec["calendar_rows"]):
            failures.append(f"{stock} {date} calendar_rows_mismatch")
        db = db_counts.get(key)
        if not db:
            failures.append(f"{stock} {date} db_rows_missing")
            continue
        for col, count_key in [
            ("rows", "db_rows"),
            ("open_li", "db_open_rows"),
            ("high_li", "db_high_rows"),
            ("low_li", "db_low_rows"),
            ("close_li", "db_close_rows"),
            ("volume_hand", "db_volume_rows"),
            ("amount_li", "db_amount_rows"),
        ]:
            if int(db[count_key]) != cal_rows:
                failures.append(f"{stock} {date} {col}_{db[count_key]}_calendar_{cal_rows}")
        if adj_counts.get(key, 0) <= 0:
            failures.append(f"{stock} {date} adj_factor_missing")
        limit = limits.get(key)
        if not limit:
            failures.append(f"{stock} {date} stk_limit_missing")
        else:
            for field in ["pre_close", "up_limit", "down_limit"]:
                if limit.get(field) is None:
                    failures.append(f"{stock} {date} {field}_missing")
        for field in rec["patch_fields"]:
            path = root / "features" / symbol_dir(stock) / f"{field}.1min.bin"
            if not path.exists():
                failures.append(f"{stock} {date} missing_file_{field}")
                continue
            try:
                start, arr = read_bin(path)
                values_at(arr, start, idx_by_date[date])
            except Exception as exc:
                failures.append(f"{stock} {date} {field}_offset_error:{exc}")

    adjacent = validate_adjacent_factor(args, plan, max_stocks=args.max_adjacent_stocks)
    failures.extend(adjacent["failures"])
    warnings.extend(adjacent["warnings"])
    if args.output_factor_basis_csv:
        write_csv(Path(args.output_factor_basis_csv), adjacent.get("factor_basis_rows", []))
    result = {
        "mode": "verify_plan_read_only_no_bin_mutation",
        "plan_json": str(args.plan_json),
        "checked_records": len(records),
        "checked_unique_stocks": len({stock for stock, _ in records}),
        "ok": not failures,
        "failures": failures,
        "warnings": warnings,
        "adjacent_validation": adjacent,
    }
    write_json(Path(args.output_verify_json), result)
    write_verify_md(result, Path(args.output_verify_md))
    print(json.dumps({"ok": result["ok"], "failures": len(failures), "warnings": len(warnings)}, ensure_ascii=False, indent=2))
    if failures and not args.allow_failures:
        return 2
    return 0


def validate_adjacent_factor(args: argparse.Namespace, plan: dict[str, Any], max_stocks: int | None) -> dict[str, Any]:
    root = Path(plan["qlib_minute_uri"])
    calendar = load_calendar(root)
    idx_by_date = date_indices(calendar)
    stocks = sorted({r["stock"] for r in plan["records"]})
    if max_stocks:
        stocks = stocks[:max_stocks]
    date_min = min(r["date"] for r in plan["records"])
    date_max = max(r["date"] for r in plan["records"])
    start = (datetime.strptime(date_min, "%Y-%m-%d") - timedelta(days=20)).date().isoformat()
    end = (datetime.strptime(date_max, "%Y-%m-%d") + timedelta(days=20)).date().isoformat()
    failures: list[str] = []
    warnings: list[str] = []
    samples: list[dict[str, Any]] = []
    factor_basis_rows: list[dict[str, Any]] = []
    gap_dates_by_stock: dict[str, list[str]] = defaultdict(list)
    for record in plan["records"]:
        gap_dates_by_stock[record["stock"]].append(record["date"])

    denominator_abs_tol = 1e-4
    denominator_rel_tol = 1e-5
    intraday_factor_tol = 1e-6
    qfq_upper_tol = 1e-5
    with db_connect(args) as conn:
        cur = conn.cursor()
        # Batch-load DB aggregates once. Per-stock queries are too slow for the
        # 2,696-stock dry-run plan, and the full validation must be reproducible.
        cur.execute(
            """
            SELECT ts_code, max(adj_factor)
            FROM market.adj_factor
            WHERE ts_code = ANY(%s)
              AND trade_date >= %s::date
              AND trade_date <= %s::date
            GROUP BY 1
            """,
            (stocks, calendar[0][:10], calendar[-1][:10]),
        )
        max_adj = {str(r[0]).upper(): float(r[1]) for r in cur.fetchall() if r[1] is not None}
        cur.execute(
            """
            SELECT ts_code, trade_date, adj_factor
            FROM market.adj_factor
            WHERE ts_code = ANY(%s)
              AND trade_date >= %s::date
              AND trade_date <= %s::date
            """,
            (stocks, start, end),
        )
        adj_by_key = {(str(r[0]).upper(), str(r[1])): float(r[2]) for r in cur.fetchall() if r[2] is not None}
        cur.execute(
            """
            SELECT ts_code,
                   date(trade_time AT TIME ZONE 'Asia/Shanghai') AS trade_date,
                   count(*) AS rows
            FROM market.kline_minute_raw
            WHERE ts_code = ANY(%s)
              AND freq='1m'
              AND trade_time >= %s::date
              AND trade_time < (%s::date + interval '1 day')
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            (stocks, start, end),
        )
        rows_by_stock: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for stock_value, date_value, row_count in cur.fetchall():
            rows_by_stock[str(stock_value).upper()].append((str(date_value), int(row_count)))

        for stock in stocks:
            close_path = root / "features" / symbol_dir(stock) / "close.1min.bin"
            factor_path = root / "features" / symbol_dir(stock) / "factor.1min.bin"
            if not close_path.exists() or not factor_path.exists():
                failures.append(f"{stock} adjacent_file_missing")
                continue
            close_start, close_arr = read_bin(close_path)
            factor_start, factor_arr = read_bin(factor_path)
            stock_denominators: list[float] = []
            stock_samples: list[dict[str, Any]] = []
            before_samples = 0
            after_samples = 0
            stock_gap_dates = sorted(set(gap_dates_by_stock.get(stock, [])))
            stock_gap_min = min(stock_gap_dates) if stock_gap_dates else date_min
            stock_gap_max = max(stock_gap_dates) if stock_gap_dates else date_max
            for date, row_count in rows_by_stock.get(stock, []):
                idxs = idx_by_date.get(date)
                adj_factor = adj_by_key.get((stock, date))
                if not idxs or row_count != len(idxs) or adj_factor is None:
                    continue
                try:
                    q_close = values_at(close_arr, close_start, idxs)
                    q_factor = values_at(factor_arr, factor_start, idxs)
                except Exception:
                    continue
                if not np.isfinite(q_close).all() or not np.isfinite(q_factor).all():
                    continue
                factor_first = float(q_factor[0])
                factor_min = float(np.nanmin(q_factor))
                factor_max = float(np.nanmax(q_factor))
                if factor_first <= 0 or factor_first > 1.0 + qfq_upper_tol:
                    failures.append(f"{stock} adjacent_factor_invalid date={date} qlib_factor={factor_first}")
                    continue
                if abs(factor_max - factor_min) > intraday_factor_tol:
                    failures.append(f"{stock} adjacent_factor_not_constant date={date} min={factor_min} max={factor_max}")
                    continue
                denominator = float(adj_factor / factor_first)
                stock_denominators.append(denominator)
                sample = {
                    "stock": stock,
                    "date": date,
                    "adj_factor": float(adj_factor),
                    "qlib_factor": factor_first,
                    "inferred_denominator": denominator,
                }
                stock_samples.append(sample)
                if date < stock_gap_min:
                    before_samples += 1
                elif date > stock_gap_max:
                    after_samples += 1

            if len(stock_denominators) < 2:
                failures.append(f"{stock} insufficient_adjacent_factor_samples count={len(stock_denominators)}")
                continue

            den_arr = np.asarray(stock_denominators, dtype=np.float64)
            denominator = float(np.median(den_arr))
            den_min = float(np.min(den_arr))
            den_max = float(np.max(den_arr))
            den_abs_spread = den_max - den_min
            den_rel_spread = den_abs_spread / max(abs(denominator), 1.0)
            if den_abs_spread > denominator_abs_tol and den_rel_spread > denominator_rel_tol:
                failures.append(
                    f"{stock} adjacent_factor_denominator_unstable "
                    f"min={den_min} max={den_max} abs_spread={den_abs_spread} rel_spread={den_rel_spread}"
                )
                continue
            if before_samples == 0 or after_samples == 0:
                warnings.append(f"{stock} adjacent_factor_samples_not_bracketing_gap before={before_samples} after={after_samples}")

            planned_factors: list[float] = []
            for gap_date in stock_gap_dates:
                gap_adj = adj_by_key.get((stock, gap_date))
                if gap_adj is None:
                    failures.append(f"{stock} {gap_date} adj_factor_value_missing")
                    continue
                planned_factor = float(np.float32(gap_adj / denominator))
                if not np.isfinite(planned_factor) or planned_factor <= 0 or planned_factor > 1.0 + qfq_upper_tol:
                    failures.append(f"{stock} {gap_date} planned_factor_invalid value={planned_factor} denominator={denominator}")
                planned_factors.append(planned_factor)

            db_max = max_adj.get(stock)
            db_max_diff = None if db_max is None else float(db_max - denominator)
            db_max_matches = None if db_max is None else abs(db_max - denominator) <= denominator_abs_tol
            factor_basis_rows.append(
                {
                    "stock": stock,
                    "sample_count": len(stock_denominators),
                    "before_gap_samples": before_samples,
                    "after_gap_samples": after_samples,
                    "inferred_denominator": denominator,
                    "denominator_min": den_min,
                    "denominator_max": den_max,
                    "denominator_abs_spread": den_abs_spread,
                    "denominator_rel_spread": den_rel_spread,
                    "db_max_adj": db_max,
                    "db_max_minus_inferred": db_max_diff,
                    "db_max_matches_inferred": db_max_matches,
                    "planned_factor_min": min(planned_factors) if planned_factors else None,
                    "planned_factor_max": max(planned_factors) if planned_factors else None,
                }
            )
            samples.extend(stock_samples[:3])

    db_max_differs = sum(1 for r in factor_basis_rows if r["db_max_matches_inferred"] is False)
    bracketing_warnings = sum(1 for w in warnings if "adjacent_factor_samples_not_bracketing_gap" in w)
    planned_values = [
        value
        for row in factor_basis_rows
        for value in [row.get("planned_factor_min"), row.get("planned_factor_max")]
        if value is not None
    ]
    summary = {
        "method": "infer_official_denominator_from_adjacent_qlib_factor_and_db_adj_factor",
        "denominator_abs_tolerance": denominator_abs_tol,
        "denominator_rel_tolerance": denominator_rel_tol,
        "checked_stocks": len(stocks),
        "factor_basis_rows": len(factor_basis_rows),
        "sample_count": sum(int(row["sample_count"]) for row in factor_basis_rows),
        "db_max_differs_from_inferred_stocks": db_max_differs,
        "bracketing_warnings": bracketing_warnings,
        "planned_factor_min": min(planned_values) if planned_values else None,
        "planned_factor_max": max(planned_values) if planned_values else None,
    }
    return {
        "checked_stocks": len(stocks),
        "sample_count": len(samples),
        "samples": samples[:200],
        "factor_basis_summary": summary,
        "factor_basis_rows": factor_basis_rows,
        "failures": failures,
        "warnings": warnings,
    }


def write_verify_md(result: dict[str, Any], path: Path) -> None:
    basis = result["adjacent_validation"].get("factor_basis_summary", {})
    rows = [
        ["ok", result["ok"]],
        ["checked_records", result["checked_records"]],
        ["checked_unique_stocks", result["checked_unique_stocks"]],
        ["failures", len(result["failures"])],
        ["warnings", len(result["warnings"])],
        ["adjacent_checked_stocks", result["adjacent_validation"]["checked_stocks"]],
        ["factor_basis_method", basis.get("method")],
        ["factor_basis_rows", basis.get("factor_basis_rows")],
        ["factor_basis_samples", basis.get("sample_count")],
        ["db_max_differs_from_inferred_stocks", basis.get("db_max_differs_from_inferred_stocks")],
        ["planned_factor_min", basis.get("planned_factor_min")],
        ["planned_factor_max", basis.get("planned_factor_max")],
    ]
    lines = [
        "# P0 Qlib Minute Bin Patch Plan Verification",
        "",
        "This is a read-only verification. It did not write Qlib bin files.",
        "",
        "```text",
        table(rows, ["Metric", "Value"]),
        "```",
        "",
    ]
    if basis:
        basis_rows = result["adjacent_validation"].get("factor_basis_rows", [])
        diff_rows = [r for r in basis_rows if r.get("db_max_matches_inferred") is False][:20]
        if diff_rows:
            lines += [
                "## Factor Basis Evidence",
                "",
                "The verifier infers each stock's official Qlib factor denominator from adjacent non-null Qlib `$factor` and current DB `adj_factor`; current DB max-adj is reported only as evidence and is not used as a silent fallback.",
                "",
                "```text",
                table(
                    [
                        [
                            r["stock"],
                            f"{float(r['inferred_denominator']):.8g}",
                            f"{float(r['db_max_adj']):.8g}" if r.get("db_max_adj") is not None else None,
                            f"{float(r['db_max_minus_inferred']):.8g}" if r.get("db_max_minus_inferred") is not None else None,
                            r["sample_count"],
                            f"{float(r['denominator_abs_spread']):.3g}",
                        ]
                        for r in diff_rows
                    ],
                    ["Stock", "InferredDen", "DbMaxAdj", "DbMax-Den", "Samples", "DenSpread"],
                ),
                "```",
                "",
            ]
    if result["failures"]:
        lines += ["## Failures", ""] + [f"- {x}" for x in result["failures"][:200]] + [""]
    if result["warnings"]:
        lines += ["## Warnings", ""] + [f"- {x}" for x in result["warnings"][:200]] + [""]
    ensure_parent(path)
    path.write_text("\n".join(lines), encoding="utf-8")


def load_factor_basis(path: Path) -> dict[str, float]:
    rows = read_csv(path)
    basis: dict[str, float] = {}
    for row in rows:
        stock = row.get("stock", "").upper()
        value = row.get("inferred_denominator")
        if not stock or value in (None, ""):
            continue
        denominator = float(value)
        if not np.isfinite(denominator) or denominator <= 0:
            raise ValueError(f"{stock} invalid inferred_denominator={value}")
        basis[stock] = denominator
    if not basis:
        raise ValueError(f"no factor basis rows loaded from {path}")
    return basis


def fetch_stock_patch_values(
    conn: Any,
    stock: str,
    dates: list[str],
    denominator: float,
    calendar: list[str],
    idx_by_date: dict[str, list[int]],
) -> dict[str, dict[str, np.ndarray]]:
    date_set = set(dates)
    start_date = min(dates)
    end_exclusive = (datetime.strptime(max(dates), "%Y-%m-%d").date() + timedelta(days=1)).isoformat()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date(trade_time AT TIME ZONE 'Asia/Shanghai') AS trade_date,
               to_char(trade_time AT TIME ZONE 'Asia/Shanghai', 'YYYY-MM-DD HH24:MI:SS') AS minute_key,
               open_li, high_li, low_li, close_li, volume_hand, amount_li
        FROM market.kline_minute_raw
        WHERE freq='1m'
          AND ts_code=%s
          AND trade_time >= %s::timestamptz
          AND trade_time < %s::timestamptz
        ORDER BY trade_time
        """,
        (stock, f"{start_date} 00:00:00+08", f"{end_exclusive} 00:00:00+08"),
    )
    rows_by_date: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for row in cur.fetchall():
        date = str(row[0])
        if date in date_set:
            rows_by_date[date].append(row)

    cur.execute(
        """
        SELECT trade_date, adj_factor
        FROM market.adj_factor
        WHERE ts_code=%s
          AND trade_date >= %s::date
          AND trade_date <= %s::date
        """,
        (stock, min(dates), max(dates)),
    )
    adj_by_date = {str(row[0]): float(row[1]) for row in cur.fetchall() if row[1] is not None}

    cur.execute(
        """
        SELECT trade_date, up_limit, down_limit
        FROM market.stk_limit
        WHERE ts_code=%s
          AND trade_date >= %s::date
          AND trade_date <= %s::date
        """,
        (stock, min(dates), max(dates)),
    )
    limit_by_date = {
        str(row[0]): (
            float(row[1]) if row[1] is not None else None,
            float(row[2]) if row[2] is not None else None,
        )
        for row in cur.fetchall()
    }

    values_by_date: dict[str, dict[str, np.ndarray]] = {}
    for date in dates:
        idxs = idx_by_date.get(date, [])
        expected_times = [calendar[idx] for idx in idxs]
        rows = rows_by_date.get(date, [])
        if len(rows) != len(idxs):
            raise ValueError(f"{stock} {date} db_rows_{len(rows)}_calendar_{len(idxs)}")
        actual_times = [str(row[1]) for row in rows]
        if actual_times != expected_times:
            raise ValueError(f"{stock} {date} minute_calendar_mismatch")
        if date not in adj_by_date:
            raise ValueError(f"{stock} {date} adj_factor_missing")
        if date not in limit_by_date:
            raise ValueError(f"{stock} {date} stk_limit_missing")

        up_limit, down_limit = limit_by_date[date]
        if up_limit is None or down_limit is None:
            raise ValueError(f"{stock} {date} limit_price_missing")

        scale = 1000.0
        qfq = np.float32(adj_by_date[date] / denominator)
        if not np.isfinite(qfq) or qfq <= 0 or qfq > np.float32(1.0 + 1e-5):
            raise ValueError(f"{stock} {date} planned_factor_invalid:{qfq}")

        open_li = np.asarray([float(row[2]) for row in rows], dtype=np.float64)
        high_li = np.asarray([float(row[3]) for row in rows], dtype=np.float64)
        low_li = np.asarray([float(row[4]) for row in rows], dtype=np.float64)
        close_li = np.asarray([float(row[5]) for row in rows], dtype=np.float64)
        volume_hand = np.asarray([float(row[6]) for row in rows], dtype=np.float64)
        amount_li = np.asarray([float(row[7]) for row in rows], dtype=np.float64)
        for field_name, arr in [
            ("open_li", open_li),
            ("high_li", high_li),
            ("low_li", low_li),
            ("close_li", close_li),
            ("volume_hand", volume_hand),
            ("amount_li", amount_li),
        ]:
            if not np.isfinite(arr).all():
                raise ValueError(f"{stock} {date} {field_name}_contains_nan")

        close_yuan = close_li / scale
        values_by_date[date] = {
            "open": (open_li / scale * qfq).astype(np.float32),
            "high": (high_li / scale * qfq).astype(np.float32),
            "low": (low_li / scale * qfq).astype(np.float32),
            "close": (close_li / scale * qfq).astype(np.float32),
            "volume": (volume_hand * 100.0 / qfq).astype(np.float32),
            "amount": (amount_li / scale).astype(np.float32),
            "factor": np.full(len(rows), qfq, dtype=np.float32),
            "limit_up": (close_yuan >= up_limit).astype(np.float32),
            "limit_down": (close_yuan <= down_limit).astype(np.float32),
        }
    return values_by_date


def backup_files(root: Path, files: list[dict[str, Any]], backup_root: Path, plan: dict[str, Any]) -> dict[str, Any]:
    if backup_root.exists():
        raise FileExistsError(f"backup root already exists: {backup_root}")
    total_bytes = sum(int(file_info["size"]) for file_info in files) + Path(plan["calendar_path"]).stat().st_size
    parent = closest_existing_parent(backup_root.parent)
    free_bytes = shutil.disk_usage(parent).free
    required = int(total_bytes * 1.10)
    if free_bytes < required:
        raise RuntimeError(f"insufficient disk for backup: free={free_bytes} required={required}")

    backup_root.mkdir(parents=True)
    copied: list[dict[str, Any]] = []
    calendar_path = Path(plan["calendar_path"])
    calendar_dest = backup_root / calendar_path.relative_to(root)
    ensure_parent(calendar_dest)
    shutil.copy2(calendar_path, calendar_dest)
    copied.append({"source": str(calendar_path), "backup": str(calendar_dest), "size": calendar_path.stat().st_size, "sha256": sha256_file(calendar_dest)})

    for idx, file_info in enumerate(files, start=1):
        source = Path(file_info["path"])
        rel = source.relative_to(root)
        dest = backup_root / rel
        ensure_parent(dest)
        shutil.copy2(source, dest)
        copied.append({"source": str(source), "backup": str(dest), "size": int(file_info["size"]), "sha256": sha256_file(dest)})
        if idx % 2000 == 0:
            print(f"[backup] copied {idx}/{len(files)} files")

    manifest = {
        "schema_version": 1,
        "mode": "qlib_minute_bin_repair_backup",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_root": str(root),
        "backup_root": str(backup_root),
        "plan_sha256": plan.get("plan_sha256"),
        "file_count": len(files),
        "total_bytes": total_bytes,
        "copied": copied,
    }
    write_json(backup_root / "backup_manifest.json", manifest)
    return {
        "backup_root": str(backup_root),
        "file_count": len(files),
        "total_bytes": total_bytes,
        "manifest": str(backup_root / "backup_manifest.json"),
    }


def command_apply_plan(args: argparse.Namespace) -> int:
    expected_confirm = "APPLY_Q_LIB_MINUTE_BIN_REPAIR"
    if args.confirm_apply != expected_confirm:
        raise ValueError(f"--confirm-apply must equal {expected_confirm}")

    plan_path = Path(args.plan_json)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    root = Path(plan["qlib_minute_uri"])
    calendar = load_calendar(root)
    idx_by_date = date_indices(calendar)
    records = plan["records"]
    files = plan["unique_files"]
    if not records:
        raise ValueError("plan has no records")
    if sha256_file(Path(plan["calendar_path"])) != plan["calendar_sha256"]:
        raise RuntimeError("calendar_sha256_mismatch")
    for file_info in files:
        if not file_info.get("sha256"):
            raise ValueError("apply-plan requires plan built with --hash-files")
        path = Path(file_info["path"])
        if not path.exists():
            raise FileNotFoundError(path)
        actual = sha256_file(path)
        if actual != file_info["sha256"]:
            raise RuntimeError(f"pre_apply_sha256_mismatch path={path} expected={file_info['sha256']} actual={actual}")

    factor_basis = load_factor_basis(Path(args.factor_basis_csv))
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        stock = record["stock"]
        if stock not in factor_basis:
            raise ValueError(f"{stock} factor_basis_missing")
        grouped[stock].append(record)

    print(f"[preflight] building patch values for {len(grouped)} stocks / {len(records)} stock-date records")
    patch_values: dict[str, dict[str, dict[str, np.ndarray]]] = {}
    with db_connect(args) as conn:
        for idx, (stock, stock_records) in enumerate(sorted(grouped.items()), start=1):
            dates = sorted({record["date"] for record in stock_records})
            patch_values[stock] = fetch_stock_patch_values(conn, stock, dates, factor_basis[stock], calendar, idx_by_date)
            if idx % 200 == 0:
                print(f"[preflight] validated {idx}/{len(grouped)} stocks")

    backup_root = Path(args.backup_root) if args.backup_root else root.parent / f"{root.name}_backup_direct_repair_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup = backup_files(root, files, backup_root, plan)

    by_field = Counter()
    patched_files = 0
    patched_values = 0
    readback_max_abs_diff = 0.0
    patch_details: list[dict[str, Any]] = []
    for stock_idx, (stock, stock_records) in enumerate(sorted(grouped.items()), start=1):
        fields = sorted({field for record in stock_records for field in record["patch_fields"]})
        records_by_date = {record["date"]: record for record in stock_records}
        for field in fields:
            path = root / "features" / symbol_dir(stock) / f"{field}.1min.bin"
            start, arr = read_bin(path)
            arr_new = arr.copy()
            expected_chunks: list[np.ndarray] = []
            positions_all: list[int] = []
            for date, record in sorted(records_by_date.items()):
                if field not in record["patch_fields"]:
                    continue
                idxs = idx_by_date[date]
                positions = [idx - start + 1 for idx in idxs]
                if min(positions) < 1 or max(positions) >= len(arr_new):
                    raise IndexError(f"{stock} {date} {field} offset_out_of_range")
                current = arr_new[positions]
                if np.isfinite(current).any():
                    raise RuntimeError(f"{stock} {date} {field} target_offsets_already_finite")
                expected = patch_values[stock][date][field]
                if len(expected) != len(positions) or not np.isfinite(expected).all():
                    raise RuntimeError(f"{stock} {date} {field} expected_values_invalid")
                arr_new[positions] = expected
                expected_chunks.append(expected)
                positions_all.extend(positions)
                by_field[field] += len(expected)
                patched_values += len(expected)
            write_bin_atomic(path, arr_new)
            rb_start, rb_arr = read_bin(path)
            if rb_start != start:
                raise RuntimeError(f"{path} start_index_changed")
            actual = rb_arr[positions_all]
            expected_all = np.concatenate(expected_chunks).astype(np.float32)
            diff = np.abs(actual.astype(np.float32) - expected_all)
            max_diff = float(np.max(diff)) if len(diff) else 0.0
            readback_max_abs_diff = max(readback_max_abs_diff, max_diff)
            if max_diff > 1e-5:
                raise RuntimeError(f"{stock} {field} readback_diff_too_large:{max_diff}")
            patched_files += 1
            patch_details.append({"stock": stock, "field": field, "file": str(path), "values": len(positions_all), "readback_max_abs_diff": max_diff})
        if stock_idx % 200 == 0:
            print(f"[apply] patched {stock_idx}/{len(grouped)} stocks")

    post_hashes = []
    for file_info in files:
        path = Path(file_info["path"])
        post_hashes.append({"path": str(path), "pre_sha256": file_info["sha256"], "post_sha256": sha256_file(path)})

    result = {
        "schema_version": 1,
        "mode": "qlib_minute_bin_repair_apply",
        "plan_json": str(plan_path),
        "factor_basis_csv": str(Path(args.factor_basis_csv)),
        "output_apply_json": str(Path(args.output_apply_json)),
        "qlib_minute_uri": str(root),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "backup": backup,
        "summary": {
            "records": len(records),
            "stocks": len(grouped),
            "patched_files": patched_files,
            "patched_values": patched_values,
            "readback_max_abs_diff": readback_max_abs_diff,
        },
        "by_field_values": dict(sorted(by_field.items())),
        "patch_details_sample": patch_details[:200],
        "post_hashes_sample": post_hashes[:200],
    }
    write_json(Path(args.output_apply_json), result)
    write_apply_md(result, Path(args.output_apply_md))
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


def write_apply_md(result: dict[str, Any], path: Path) -> None:
    summary = result["summary"]
    lines = [
        "# P0 Qlib Minute Bin Repair Apply",
        "",
        "This command wrote verified Qlib 1min bin offsets after pre-apply checksum validation and backup creation.",
        "",
        "## Summary",
        "",
        "```text",
        table(
            [
                ["records", summary["records"]],
                ["stocks", summary["stocks"]],
                ["patched_files", summary["patched_files"]],
                ["patched_values", summary["patched_values"]],
                ["readback_max_abs_diff", summary["readback_max_abs_diff"]],
                ["backup_root", result["backup"]["backup_root"]],
                ["backup_file_count", result["backup"]["file_count"]],
                ["backup_total_bytes", result["backup"]["total_bytes"]],
            ],
            ["Metric", "Value"],
        ),
        "```",
        "",
        "## Patched Values By Field",
        "",
        "```text",
        table([[k, v] for k, v in result["by_field_values"].items()], ["Field", "Values"]),
        "```",
        "",
        f"Apply JSON: `{result.get('output_apply_json', '')}`",
        f"Backup manifest: `{result['backup']['manifest']}`",
    ]
    ensure_parent(path)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def add_db_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", default="aistock")
    parser.add_argument("--db-user", default="postgres")
    parser.add_argument("--db-password", default=None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Qlib 1min bin direct-repair dry-run planner")
    sub = parser.add_subparsers(dest="cmd", required=True)

    scan = sub.add_parser("scan")
    scan.add_argument("--qlib-minute-uri", default="/home/lc999/data/qlib_minute_bin")
    scan.add_argument("--input-stock-date-csv", default=None)
    scan.add_argument("--dates", default=None)
    scan.add_argument("--skip-db", action="store_true")
    scan.add_argument("--max-pairs", type=int, default=None)
    scan.add_argument("--output-scan-csv", required=True)
    scan.add_argument("--output-field-matrix-csv", required=True)
    scan.add_argument("--output-md", default=None)
    add_db_args(scan)
    scan.set_defaults(func=command_scan)

    plan = sub.add_parser("build-plan")
    plan.add_argument("--qlib-minute-uri", default="/home/lc999/data/qlib_minute_bin")
    plan.add_argument("--scan-csv", required=True)
    plan.add_argument("--hash-files", action="store_true")
    plan.add_argument("--require-all-patch-fields", action=argparse.BooleanOptionalAction, default=True)
    plan.add_argument("--output-plan-json", required=True)
    plan.add_argument("--output-plan-md", required=True)
    plan.set_defaults(func=command_build_plan)

    verify = sub.add_parser("verify-plan")
    verify.add_argument("--plan-json", required=True)
    verify.add_argument("--output-verify-json", required=True)
    verify.add_argument("--output-verify-md", required=True)
    verify.add_argument("--output-factor-basis-csv", default=None)
    verify.add_argument("--max-adjacent-stocks", type=int, default=None)
    verify.add_argument("--allow-failures", action="store_true")
    add_db_args(verify)
    verify.set_defaults(func=command_verify_plan)

    apply_cmd = sub.add_parser("apply-plan")
    apply_cmd.add_argument("--plan-json", required=True)
    apply_cmd.add_argument("--factor-basis-csv", required=True)
    apply_cmd.add_argument("--backup-root", default=None)
    apply_cmd.add_argument("--confirm-apply", required=True)
    apply_cmd.add_argument("--output-apply-json", required=True)
    apply_cmd.add_argument("--output-apply-md", required=True)
    add_db_args(apply_cmd)
    apply_cmd.set_defaults(func=command_apply_plan)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
