#!/usr/bin/env python
"""QE price/tradability audit for Qlib warnings and V25 price basis.

Run this in WSL/rdagent-gpu so Qlib binary data can be read. It also connects
to the local PostgreSQL market schema to compare DB daily/minute/suspend/limit
records against QE/Qlib artifacts.
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


CLOSE_NONE_RE = re.compile(
    r"stock_id:(?P<stock>[^,\s]+),\s*trade_time:\(Timestamp\('(?P<start>[^']+)'\),\s*Timestamp\('(?P<end>[^']+)'\)\),\s*\$close\):\s*None"
)


def _parse_loops(value: str) -> list[int]:
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return sorted(dict.fromkeys(out))


def _safe_float(v: Any) -> float | None:
    try:
        x = float(v)
        return x if np.isfinite(x) else None
    except Exception:
        return None


def _find_artifact_dir(loop_dir: Path) -> Path:
    candidates = list(loop_dir.glob("mlruns/*/*/artifacts/pred.pkl"))
    if not candidates:
        raise FileNotFoundError(f"missing mlruns artifacts under {loop_dir}")
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0].parent


def _connect_db(args: argparse.Namespace):
    conn = psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password or os.environ.get("TDX_DB_PASSWORD", ""),
        dbname=args.db_name,
        connect_timeout=5,
    )
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = %s", (int(args.db_statement_timeout_ms),))
    return conn


def _empty_market_row() -> dict[str, Any]:
    return {
        "daily": {"exists": False, "open": None, "high": None, "low": None, "close": None, "volume_hand": None},
        "minute": {
            "audited": False,
            "count": None,
            "start": None,
            "end": None,
            "min_close": None,
            "max_close": None,
            "volume_hand": None,
            "last_close": None,
        },
        "limit": {"exists": False, "pre_close": None, "up_limit": None, "down_limit": None},
        "suspend": {"exists": False, "rows": []},
    }


def _chunked(values: list[tuple[str, str]], size: int):
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _db_market_rows_batch(
    cur,
    pairs: list[tuple[str, str]],
    *,
    chunk_size: int = 500,
    include_minute: bool = True,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Fetch daily/minute/limit/suspend state for stock-date pairs in batches.

    This avoids one DB round-trip per warning row while preserving exact
    stock/date evidence. It uses a session-local temp pair table and only
    reads durable market tables.
    """
    uniq = sorted(dict.fromkeys((str(code), str(date)) for code, date in pairs))
    rows = {pair: _empty_market_row() for pair in uniq}
    if not uniq:
        return rows

    for chunk in _chunked(uniq, chunk_size):
        cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_qe_audit_pairs (ts_code text, trade_date date, PRIMARY KEY (ts_code, trade_date)) ON COMMIT DROP")
        cur.execute("TRUNCATE tmp_qe_audit_pairs")
        execute_values(cur, "INSERT INTO tmp_qe_audit_pairs (ts_code, trade_date) VALUES %s", chunk)
        cur.execute("ANALYZE tmp_qe_audit_pairs")

        cur.execute(
            """
            SELECT p.ts_code, p.trade_date::text, d.open_li, d.high_li, d.low_li, d.close_li, d.volume_hand
            FROM tmp_qe_audit_pairs p
            LEFT JOIN LATERAL (
                SELECT open_li, high_li, low_li, close_li, volume_hand
                FROM market.kline_daily_raw d
                WHERE d.ts_code = p.ts_code AND d.trade_date = p.trade_date
                LIMIT 1
            ) d ON true
            """
        )
        for code, date, open_li, high_li, low_li, close_li, volume_hand in cur.fetchall():
            if close_li is None:
                continue
            rows[(code, date)]["daily"] = {
                "exists": True,
                "open": float(open_li) / 1000.0 if open_li is not None else None,
                "high": float(high_li) / 1000.0 if high_li is not None else None,
                "low": float(low_li) / 1000.0 if low_li is not None else None,
                "close": float(close_li) / 1000.0,
                "volume_hand": int(volume_hand) if volume_hand is not None else None,
            }

        if include_minute:
            cur.execute(
                """
                SELECT p.ts_code, p.trade_date::text,
                       m.cnt, m.start_time, m.end_time,
                       m.min_close, m.max_close, m.volume_hand, m.last_close
                FROM tmp_qe_audit_pairs p
                LEFT JOIN LATERAL (
                    SELECT count(*) AS cnt,
                           min(trade_time) AS start_time,
                           max(trade_time) AS end_time,
                           min(close_li) AS min_close,
                           max(close_li) AS max_close,
                           sum(volume_hand) AS volume_hand,
                           (array_agg(close_li ORDER BY trade_time DESC) FILTER (WHERE close_li IS NOT NULL))[1] AS last_close
                    FROM market.kline_minute_raw m
                    WHERE m.ts_code = p.ts_code
                      AND m.freq = '1m'
                      AND m.trade_time >= p.trade_date
                      AND m.trade_time < p.trade_date + interval '1 day'
                ) m ON true
                """
            )
            for code, date, cnt, start, end, min_close, max_close, volume_hand, last_close in cur.fetchall():
                rows[(code, date)]["minute"] = {
                    "audited": True,
                    "count": int(cnt or 0),
                    "start": start.isoformat() if start else None,
                    "end": end.isoformat() if end else None,
                    "min_close": float(min_close) / 1000.0 if min_close is not None else None,
                    "max_close": float(max_close) / 1000.0 if max_close is not None else None,
                    "volume_hand": int(volume_hand or 0),
                    "last_close": float(last_close) / 1000.0 if last_close is not None else None,
                }

        cur.execute(
            """
            SELECT p.ts_code, p.trade_date::text, l.pre_close, l.up_limit, l.down_limit
            FROM tmp_qe_audit_pairs p
            LEFT JOIN LATERAL (
                SELECT pre_close, up_limit, down_limit
                FROM market.stk_limit l
                WHERE l.ts_code = p.ts_code AND l.trade_date = p.trade_date
                LIMIT 1
            ) l ON true
            """
        )
        for code, date, pre_close, up_limit, down_limit in cur.fetchall():
            if pre_close is None and up_limit is None and down_limit is None:
                continue
            rows[(code, date)]["limit"] = {
                "exists": True,
                "pre_close": _safe_float(pre_close),
                "up_limit": _safe_float(up_limit),
                "down_limit": _safe_float(down_limit),
            }

        cur.execute(
            """
            SELECT s.ts_code, s.trade_date::text, s.suspend_type, s.suspend_timing
            FROM market.suspend_d s
            JOIN tmp_qe_audit_pairs p
              ON s.ts_code = p.ts_code AND s.trade_date = p.trade_date
            ORDER BY s.ts_code, s.trade_date, s.suspend_type NULLS LAST
            """
        )
        for code, date, suspend_type, suspend_timing in cur.fetchall():
            suspend = rows[(code, date)]["suspend"]
            suspend["exists"] = True
            suspend["rows"].append({"suspend_type": suspend_type, "suspend_timing": suspend_timing})
    return rows


def _db_market_row(cur, code: str, date: str) -> dict[str, Any]:
    return _db_market_rows_batch(cur, [(code, date)]).get((code, date), _empty_market_row())


def _classify_db_state(row: dict[str, Any]) -> str:
    daily = row["daily"]["exists"]
    minute = row["minute"]
    minute_audited = bool(minute.get("audited"))
    minute_count = minute.get("count")
    suspend = row["suspend"]["exists"]
    limit = row["limit"]["exists"]
    if minute_audited and daily and (minute_count or 0) >= 240 and limit and not suspend:
        return "DB_DAILY_MINUTE_LIMIT_PRESENT_NOT_SUSPENDED"
    if minute_audited and suspend and not daily and (minute_count or 0) == 0:
        return "SUSPEND_D_PRESENT_NO_DB_PRICE"
    if minute_audited and suspend and daily and (minute_count or 0) == 0:
        return "SUSPEND_D_PRESENT_DAILY_PRESENT_MINUTE_MISSING"
    if minute_audited and daily and (minute_count or 0) == 0 and not suspend:
        return "DB_DAILY_PRESENT_MINUTE_MISSING"
    if minute_audited and not daily and (minute_count or 0) == 0 and not suspend:
        return "DB_PRICE_MISSING_NO_SUSPEND_D"
    if not minute_audited and daily and limit and not suspend:
        return "DB_DAILY_LIMIT_PRESENT_NOT_SUSPENDED_MINUTE_NOT_AUDITED"
    if not minute_audited and suspend and not daily:
        return "SUSPEND_D_PRESENT_NO_DAILY_PRICE_MINUTE_NOT_AUDITED"
    if not minute_audited and suspend and daily:
        return "SUSPEND_D_PRESENT_DAILY_PRESENT_MINUTE_NOT_AUDITED"
    if not minute_audited and daily:
        return "DB_DAILY_PRESENT_MINUTE_NOT_AUDITED"
    if not minute_audited and not daily and not suspend:
        return "DB_DAILY_MISSING_NO_SUSPEND_MINUTE_NOT_AUDITED"
    return "MIXED_DB_STATE"


def _parse_close_none_warnings(loop_dir: Path) -> list[dict[str, Any]]:
    log_path = loop_dir / "run.log"
    if not log_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(log_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        m = CLOSE_NONE_RE.search(line)
        if not m:
            continue
        start_dt = pd.Timestamp(m.group("start"))
        end_dt = pd.Timestamp(m.group("end"))
        rows.append({
            "line": line_no,
            "stock": m.group("stock"),
            "start": str(start_dt.date()),
            "end": str(end_dt.date()),
        })
    return rows


def _init_qlib(day_uri: str, minute_uri: str):
    import qlib
    from qlib.constant import REG_CN

    qlib.init(provider_uri={"day": day_uri, "1min": minute_uri}, region=REG_CN)
    from qlib.data import D

    return D


def _qlib_day_row(D: Any, code: str, date: str) -> dict[str, Any]:
    fields = ["$close", "$factor", "$up_limit_price", "$down_limit_price"]
    try:
        df = D.features([code], fields, start_time=date, end_time=date, freq="day")
        if df is None or df.empty:
            return {"exists": False, "error": None}
        row = df.iloc[-1]
        return {
            "exists": True,
            "close": _safe_float(row.get("$close")),
            "factor": _safe_float(row.get("$factor")),
            "up_limit": _safe_float(row.get("$up_limit_price")),
            "down_limit": _safe_float(row.get("$down_limit_price")),
        }
    except Exception as exc:
        return {"exists": False, "error": f"{type(exc).__name__}: {exc}"}


def _qlib_minute_last_row(D: Any, code: str, date: str) -> dict[str, Any]:
    fields = ["$close", "$factor"]
    try:
        df = D.features([code], fields, start_time=f"{date} 09:31:00", end_time=f"{date} 15:00:00", freq="1min")
        if df is None or df.empty:
            return {"exists": False, "rows": 0, "error": None}
        row = df.dropna(how="all").iloc[-1]
        return {
            "exists": True,
            "rows": int(len(df)),
            "close": _safe_float(row.get("$close")),
            "factor": _safe_float(row.get("$factor")),
        }
    except Exception as exc:
        return {"exists": False, "rows": 0, "error": f"{type(exc).__name__}: {exc}"}


def _basis_compare(qlib_close: float | None, qlib_factor: float | None, db_close: float | None) -> dict[str, Any]:
    if qlib_close is None or db_close is None:
        return {"basis": "MISSING", "best_diff": None}
    candidates = {"close_raw": abs(qlib_close - db_close)}
    if qlib_factor and qlib_factor > 0:
        candidates["close_div_factor"] = abs(qlib_close / qlib_factor - db_close)
        candidates["close_mul_factor"] = abs(qlib_close * qlib_factor - db_close)
    basis, diff = min(candidates.items(), key=lambda kv: kv[1])
    return {"basis": basis, "best_diff": float(diff), "diffs": {k: float(v) for k, v in candidates.items()}}


def _trade_samples(workspace: Path, loops: list[int], max_samples: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for loop in loops:
        enhanced_path = workspace / f"Loop{loop}" / "qlib_results_enhanced.json"
        if not enhanced_path.exists():
            continue
        enhanced = json.loads(enhanced_path.read_text(encoding="utf-8"))
        for code, trades in (enhanced.get("stock_trades") or {}).items():
            if not isinstance(trades, list):
                continue
            for tr in trades:
                if tr.get("date") and tr.get("type") in {"buy", "sell"}:
                    rows.append({"loop": loop, "stock": code, "date": str(tr["date"]), "type": tr.get("type")})
    # Deterministic spread across the full trade list.
    if len(rows) <= max_samples:
        return rows
    idx = np.linspace(0, len(rows) - 1, max_samples).round().astype(int)
    return [rows[int(i)] for i in idx]


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    workspace = Path(args.workspace) if args.workspace else Path("/mnt/f/Dev/RD-Agent-main/qe_workspace") / args.task_id
    loops = _parse_loops(args.loops)
    D = _init_qlib(args.qlib_day_uri, args.qlib_minute_uri)
    conn = _connect_db(args)
    cur = conn.cursor()
    try:
        warning_rows: list[dict[str, Any]] = []
        for loop in loops:
            for w in _parse_close_none_warnings(workspace / f"Loop{loop}"):
                w["loop"] = loop
                warning_rows.append(w)
        warning_unique: dict[tuple[int, str, str], dict[str, Any]] = {}
        for w in warning_rows:
            warning_unique.setdefault((w["loop"], w["stock"], w["start"]), w)
        warning_values = list(warning_unique.values())
        if args.max_warning_audits > 0:
            warning_values = warning_values[: args.max_warning_audits]
        warning_db = _db_market_rows_batch(
            cur,
            [(w["stock"], w["start"]) for w in warning_values],
            include_minute=False,
        )
        if args.max_warning_minute_audits < 0:
            minute_values = warning_values
        elif args.max_warning_minute_audits == 0:
            minute_values = []
        else:
            minute_values = warning_values[: args.max_warning_minute_audits]
        minute_db = _db_market_rows_batch(
            cur,
            [(w["stock"], w["start"]) for w in minute_values],
            chunk_size=50,
            include_minute=True,
        )
        for key, minute_row in minute_db.items():
            if key in warning_db:
                warning_db[key]["minute"] = minute_row["minute"]
        warning_audits: list[dict[str, Any]] = []
        for w in warning_values:
            db_row = warning_db.get((w["stock"], w["start"]), _empty_market_row())
            warning_audits.append({**w, "db_state": _classify_db_state(db_row), "db": db_row})

        samples = _trade_samples(workspace, loops, args.max_price_samples)
        sample_db = _db_market_rows_batch(cur, [(s["stock"], s["date"]) for s in samples], chunk_size=50, include_minute=True)
        price_rows: list[dict[str, Any]] = []
        for s in samples:
            db_row = sample_db.get((s["stock"], s["date"]), _empty_market_row())
            qd = _qlib_day_row(D, s["stock"], s["date"])
            qm = {"exists": False, "rows": 0, "error": "skipped"} if args.skip_qlib_minute_samples else _qlib_minute_last_row(D, s["stock"], s["date"])
            day_basis = _basis_compare(qd.get("close"), qd.get("factor"), db_row["daily"]["close"])
            minute_basis = _basis_compare(qm.get("close"), qm.get("factor"), db_row["minute"]["last_close"])
            q_up = qd.get("up_limit")
            q_down = qd.get("down_limit")
            d_up = db_row["limit"]["up_limit"]
            d_down = db_row["limit"]["down_limit"]
            price_rows.append({
                **s,
                "db_state": _classify_db_state(db_row),
                "db_daily_close": db_row["daily"]["close"],
                "db_minute_count": db_row["minute"]["count"],
                "db_minute_last_close": db_row["minute"]["last_close"],
                "qlib_day": qd,
                "qlib_minute": qm,
                "day_basis": day_basis,
                "minute_basis": minute_basis,
                "up_limit_abs_diff": abs(q_up - d_up) if q_up is not None and d_up is not None else None,
                "down_limit_abs_diff": abs(q_down - d_down) if q_down is not None and d_down is not None else None,
            })
        return {
            "task_id": args.task_id,
            "workspace": str(workspace),
            "loops": loops,
            "warning_rows": len(warning_rows),
            "warning_unique_total": len(warning_unique),
            "warning_unique_audited": len(warning_audits),
            "warning_minute_audited": sum(1 for w in warning_audits if w["db"]["minute"].get("audited")),
            "warning_audits": warning_audits,
            "price_basis_samples": price_rows,
        }
    finally:
        conn.close()


def _fmt_num(v: Any, width: int = 9, digits: int = 3) -> str:
    x = _safe_float(v)
    if x is None:
        return " " * (width - 2) + "NA"
    return f"{x:>{width}.{digits}f}"


def _table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    out = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    out.append("  ".join("-" * w for w in widths))
    out.extend("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)) for row in rows)
    return "\n".join(out)


def write_md(result: dict[str, Any], output: Path) -> None:
    lines = [f"# QE Price and Tradability Audit: {result['task_id']}", ""]
    lines.append("Scope: read-only comparison of Qlib warning evidence, DB daily/minute/suspend/limit records, and Qlib price basis samples.")
    lines.append(f"Warnings parsed: rows={result.get('warning_rows')} unique_total={result.get('warning_unique_total')} unique_audited={result.get('warning_unique_audited')} minute_audited={result.get('warning_minute_audited')}.")
    lines.append("")

    state_counts = Counter(w["db_state"] for w in result["warning_audits"])
    rows = [[k, str(v)] for k, v in sorted(state_counts.items())]
    lines += ["## P0 Qlib `$close=None` Warning Classification", "", "```text", _table(rows, ["DBState", "UniqueWarnings"]), "```", ""]

    example_rows = []
    for w in result["warning_audits"][:30]:
        example_rows.append([
            str(w["loop"]),
            w["stock"],
            w["start"],
            w["db_state"],
            str(w["db"]["daily"]["exists"]),
            str(w["db"]["minute"].get("count")),
            str(w["db"]["suspend"]["exists"]),
            str(w["db"]["limit"]["exists"]),
        ])
    lines += ["## P0 Warning Examples", "", "```text", _table(example_rows, ["Loop", "Stock", "Date", "DBState", "Daily", "MinRows", "Suspend", "Limit"]), "```", ""]

    minute_counts = Counter(_classify_db_state(w["db"]) for w in result["warning_audits"] if w["db"]["minute"].get("audited"))
    rows = [[k, str(v)] for k, v in sorted(minute_counts.items())]
    lines += ["## P0 Warning Rows With DB Minute Audit", "", "```text", _table(rows, ["DBState", "MinuteAuditedRows"]), "```", ""]

    day_basis_counts = Counter(r["day_basis"]["basis"] for r in result["price_basis_samples"])
    minute_basis_counts = Counter(r["minute_basis"]["basis"] for r in result["price_basis_samples"])
    rows = []
    for basis in sorted(set(day_basis_counts) | set(minute_basis_counts)):
        rows.append([basis, str(day_basis_counts.get(basis, 0)), str(minute_basis_counts.get(basis, 0))])
    lines += ["## P0 Price Basis Classification", "", "```text", _table(rows, ["Basis", "DaySamples", "MinuteSamples"]), "```", ""]

    diffs = result["price_basis_samples"]
    valid_day = [r["day_basis"]["best_diff"] for r in diffs if r["day_basis"].get("best_diff") is not None]
    valid_min = [r["minute_basis"]["best_diff"] for r in diffs if r["minute_basis"].get("best_diff") is not None]
    valid_up = [r["up_limit_abs_diff"] for r in diffs if r.get("up_limit_abs_diff") is not None]
    valid_down = [r["down_limit_abs_diff"] for r in diffs if r.get("down_limit_abs_diff") is not None]
    rows = [[
        str(len(diffs)),
        _fmt_num(max(valid_day) if valid_day else None, 10, 6),
        _fmt_num(max(valid_min) if valid_min else None, 10, 6),
        _fmt_num(max(valid_up) if valid_up else None, 10, 6),
        _fmt_num(max(valid_down) if valid_down else None, 10, 6),
    ]]
    lines += ["## P0 Price Precision Summary", "", "```text", _table(rows, ["Samples", "MaxDayDiff", "MaxMinDiff", "MaxUpDiff", "MaxDownDiff"]), "```", ""]

    example_rows = []
    for r in diffs[:30]:
        example_rows.append([
            str(r["loop"]),
            r["stock"],
            r["date"],
            r["type"],
            r["day_basis"]["basis"],
            _fmt_num(r["day_basis"].get("best_diff"), 10, 6),
            r["minute_basis"]["basis"],
            _fmt_num(r["minute_basis"].get("best_diff"), 10, 6),
            str(r["db_minute_count"]),
        ])
    lines += ["## P0 Price Basis Examples", "", "```text", _table(example_rows, ["Loop", "Stock", "Date", "Side", "DayBasis", "DayDiff", "MinBasis", "MinDiff", "MinRows"]), "```", ""]

    lines += [
        "## Evidence Notes",
        "",
        "- Warning classification is factual: it reports whether DB daily/minute/limit/suspend records exist for the same stock/date where Qlib logged `$close=None`.",
        "- Price basis classification compares DB raw close against Qlib `$close`, `$close/$factor`, and `$close*$factor`; the smallest absolute difference determines the basis label.",
        "- If Qlib warning rows are classified as DB-present, this audit proves those rows are not caused by complete DB daily/minute absence for that stock/date.",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="QE price/tradability audit")
    ap.add_argument("task_id")
    ap.add_argument("--workspace", default=None)
    ap.add_argument("--loops", default="19-28")
    ap.add_argument("--qlib-day-uri", default="/home/lc999/data/qlib_bin")
    ap.add_argument("--qlib-minute-uri", default="/home/lc999/data/qlib_minute_bin")
    ap.add_argument("--db-host", default="127.0.0.1")
    ap.add_argument("--db-port", type=int, default=5432)
    ap.add_argument("--db-user", default="postgres")
    ap.add_argument("--db-password", default="")
    ap.add_argument("--db-name", default="aistock")
    ap.add_argument("--max-price-samples", type=int, default=200)
    ap.add_argument("--max-warning-audits", type=int, default=0, help="limit unique Qlib warning rows audited; 0 audits all unique rows")
    ap.add_argument("--max-warning-minute-audits", type=int, default=40, help="minute-table checks for warning rows; -1 audits all, 0 disables")
    ap.add_argument("--skip-qlib-minute-samples", action="store_true", help="skip per-sample Qlib 1min reads when only DB warning classification is needed")
    ap.add_argument("--db-statement-timeout-ms", type=int, default=120000)
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
