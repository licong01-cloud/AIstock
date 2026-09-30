"""Bounded read-only placeholder plan, DEV-only apply with exact-content CAS.

No timestamp rewriting. Production SQL requires separate operator authorization.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from backend.db.pg_pool import _db_cfg
from backend.services.minute_data_session_contract import CHINA_TZ, guard_minute_values

COLUMNS = "trade_time,ts_code,freq,open_li,high_li,low_li,close_li,volume_hand,amount_li,adjust_type,source"
DELETE_SQL = """DELETE FROM market.kline_minute_raw
WHERE ts_code=%s AND freq='1m' AND trade_time >= %s AND trade_time < %s
AND volume_hand=0 AND amount_li=0 AND adjust_type='none' AND source='tdx_api'"""


def digest(rows):
    return hashlib.sha256(json.dumps(rows, default=str, sort_keys=True,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def build_plan(conn, start, end):
    if end < start or (end-start).days > 31:
        raise ValueError("explicit window must be ordered and at most 32 days")
    plans = []
    unresolved = []
    day = start
    while day <= end:
        lower = dt.datetime.combine(day, dt.time(), CHINA_TZ)
        upper = lower + dt.timedelta(days=1)
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT ts_code FROM market.kline_minute_raw "
                        "WHERE freq='1m' AND trade_time >= %s AND trade_time < %s "
                        "AND (trade_time AT TIME ZONE 'Asia/Shanghai')::time = TIME '13:00'",
                        (lower, upper))
            symbols = [row[0] for row in cur.fetchall()]
        for symbol in sorted(symbols):
            with conn.cursor() as cur:
                cur.execute(f"SELECT {COLUMNS} FROM market.kline_minute_raw "
                            "WHERE ts_code=%s AND freq='1m' AND trade_time >= %s AND trade_time < %s "
                            "ORDER BY trade_time", (symbol, lower, upper))
                rows = cur.fetchall()
            try:
                if any(row[9:] != ("none", "tdx_api") for row in rows):
                    raise ValueError("non-TDX or adjusted rows must be preserved")
                guarded = guard_minute_values(conn, symbol, day, rows)
                if guarded or not rows:
                    raise ValueError("not a suspended provider placeholder")
            except ValueError as exc:
                unresolved.append({"symbol": symbol, "day": str(day), "reason": str(exc)})
                continue
            plans.append({"symbol": symbol, "day": str(day), "row_count": len(rows),
                          "rows_sha256": digest(rows)})
        day += dt.timedelta(days=1)
    return {"schema_version": "suspended_minute_placeholder_plan_v1", "start": str(start), "end": str(end),
            "targets": plans, "unresolved": unresolved, "delete_sql": DELETE_SQL,
            "production_write": False, "candidate_write": False, "runtime_action": False}


def apply_dev(conn, plan):
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        if cur.fetchone()[0] != "aistock_dev":
            raise ValueError("DEV-only apply; production requires separate exact authorization")
        cur.execute("LOCK TABLE market.kline_minute_raw IN SHARE ROW EXCLUSIVE MODE")
    current = build_plan(conn, dt.date.fromisoformat(plan["start"]), dt.date.fromisoformat(plan["end"]))
    if current != plan or current["unresolved"]:
        raise ValueError("placeholder plan drift/unresolved evidence; no deletion permitted")
    deleted = 0
    for target in current["targets"]:
        start = dt.datetime.combine(dt.date.fromisoformat(target["day"]), dt.time(), CHINA_TZ)
        with conn.cursor() as cur:
            cur.execute(DELETE_SQL, (target["symbol"], start, start+dt.timedelta(days=1)))
            if cur.rowcount != target["row_count"]:
                raise ValueError("placeholder delete count drift; rollback required")
            deleted += cur.rowcount
    after = build_plan(conn, dt.date.fromisoformat(plan["start"]), dt.date.fromisoformat(plan["end"]))
    if after["targets"] or after["unresolved"]:
        raise ValueError("placeholder post-apply readback failed")
    return {"deleted_rows": deleted, "post_readback": after, "dev_write": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, choices=("aistock", "aistock_dev"))
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--start", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--end", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--apply-dev", action="store_true")
    parser.add_argument("--expected-plan-sha256")
    args = parser.parse_args()
    if args.apply_dev and (args.database != "aistock_dev" or not args.expected_plan_sha256):
        parser.error("DEV apply requires exact plan digest; production apply is forbidden")
    load_dotenv(args.env_file, override=False)
    cfg = _db_cfg()
    if args.database == "aistock_dev":
        cfg = {key: os.environ[f"TDX_DB_DEV_{suffix}"] for key, suffix in (
            ("host", "HOST"), ("port", "PORT"), ("user", "USER"),
            ("password", "PASSWORD"), ("dbname", "NAME"))}
        if cfg["dbname"] != "aistock_dev":
            raise ValueError("configured DEV database differs from required target")
    with psycopg2.connect(**cfg, connect_timeout=5) as conn:
        conn.set_session(readonly=not args.apply_dev, isolation_level="SERIALIZABLE")
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout='60s'")
        plan = build_plan(conn, args.start, args.end)
        receipt = {"plan": plan, "plan_sha256": digest(plan)}
        if args.apply_dev:
            if digest(plan) != args.expected_plan_sha256:
                raise ValueError("plan digest differs from reviewed dry run")
            receipt["apply"] = apply_dev(conn, plan)
        print(json.dumps(receipt, ensure_ascii=False, default=str))
    return 2 if plan["unresolved"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
