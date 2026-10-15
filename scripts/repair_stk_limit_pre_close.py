"""Capture same-day provider facts; validate an exact NULL-only repair in DEV.

There is deliberately no production apply mode. The generated SQL requires
separate authorization; it changes pre_close only, never LocalSIM state.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import dotenv_values  # noqa: E402
import psycopg2.extras as pgx  # noqa: E402
from scripts.repair_sw_sector_revision import connect  # noqa: E402
from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.tushare_dataset_specs import STK_LIMIT, stk_limit_prices_valid  # noqa: E402
from backend.services.tushare_sync_engine import TushareSyncEngine  # noqa: E402

SCHEMA = "local_data_stk_limit_pre_close_repair_v1"
FIELDS = ("ts_code", "trade_date", "pre_close", "up_limit", "down_limit")


def digest(value):
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def write_exclusive(path, value):
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(value) + b"\n")


def output_dir(raw):
    path = Path(raw)
    if not path.is_absolute() or path.drive.upper() != "X:":
        raise ValueError("repair outputs must use an absolute X-drive directory")
    path.mkdir(parents=True, exist_ok=False)
    return path


def normalized_record(row):
    result = {name: row[name] for name in FIELDS}
    for name in ("pre_close", "up_limit", "down_limit"):
        if result[name] is not None:
            value = Decimal(str(result[name]))
            if not value.is_finite():
                raise ValueError("non-finite source/readback fact")
            result[name] = format(value.normalize(), "f")
    result["trade_date"] = str(result["trade_date"]).replace("-", "")
    return result


def plan_repairs(day, source_rows, before):
    """Use actual provider prices and identical dates/limit identities only."""
    source = {}
    for raw in source_rows:
        row = normalized_record(raw)
        code = row["ts_code"]
        if row["trade_date"] != day.strftime("%Y%m%d") or not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", code):
            raise ValueError("source key/date differs")
        if code in source:
            raise ValueError("duplicate provider key")
        source[code] = row
    patches, unresolved = [], []
    for old in before:
        old = normalized_record(old)
        if old["pre_close"] is not None:
            continue
        code = old["ts_code"]
        if old["trade_date"] != day.strftime("%Y%m%d"):
            raise ValueError("database date differs")
        row = source.get(code)
        if row is None or not stk_limit_prices_valid(row):
            unresolved.append({"ts_code": code, "reason": "provider_missing_or_invalid"})
            continue
        if any(old[name] is None or Decimal(old[name]) != Decimal(row[name]) for name in ("up_limit", "down_limit")):
            unresolved.append({"ts_code": code, "reason": "provider_limit_identity_differs"})
            continue
        patches.append(row)
    return sorted(patches, key=lambda row: row["ts_code"]), unresolved


def repair_sql(day, rows, *, database):
    if database not in {"aistock", "aistock_dev"} or not rows:
        raise ValueError("empty or unsupported database target")
    # Validate again before rendering unquoted numeric literals and finite keys.
    checked, unresolved = plan_repairs(day, rows, [{**r, "pre_close": None} for r in rows])
    if unresolved or checked != rows:
        raise ValueError("repair rows are not canonical source-backed keys")
    values = ",\n".join(
        f"('{day.isoformat()}'::date,'{r['ts_code']}',{r['pre_close']}::numeric,{r['up_limit']}::numeric,{r['down_limit']}::numeric)"
        for r in rows
    )
    return f"""-- Exact source-backed NULL-only pre_close repair; other columns unchanged.
BEGIN;
SET LOCAL statement_timeout='60s';
SET LOCAL lock_timeout='5s';
DO $repair$
DECLARE expected_count integer := {len(rows)}; matched integer;
BEGIN
 IF current_database() <> '{database}' THEN RAISE EXCEPTION 'wrong database'; END IF;
 WITH src(trade_date,ts_code,pre_close,up_limit,down_limit) AS (VALUES {values})
 SELECT count(*) INTO matched FROM src JOIN market.stk_limit t USING(trade_date,ts_code)
 WHERE t.up_limit=src.up_limit AND t.down_limit=src.down_limit
 AND (t.pre_close IS NULL OR t.pre_close=src.pre_close);
 IF matched <> expected_count THEN RAISE EXCEPTION 'repair identity drift'; END IF;
 WITH src(trade_date,ts_code,pre_close,up_limit,down_limit) AS (VALUES {values})
 UPDATE market.stk_limit t SET pre_close=src.pre_close FROM src
 WHERE t.trade_date=src.trade_date AND t.ts_code=src.ts_code AND t.pre_close IS NULL
 AND t.up_limit=src.up_limit AND t.down_limit=src.down_limit;
 WITH src(trade_date,ts_code,pre_close,up_limit,down_limit) AS (VALUES {values})
 SELECT count(*) INTO matched FROM src JOIN market.stk_limit t USING(trade_date,ts_code)
 WHERE t.pre_close=src.pre_close AND t.up_limit=src.up_limit AND t.down_limit=src.down_limit;
 IF matched <> expected_count THEN RAISE EXCEPTION 'repair readback differs'; END IF;
END $repair$;
COMMIT;
"""


def capture(env_file, day, out):
    engine = TushareSyncEngine()
    engine._tushare_token_cache = dotenv_values(env_file).get("TUSHARE_TOKEN")
    if not engine._tushare_token_cache:
        raise ValueError("Tushare credential location unavailable")
    rows = engine._fetch_from_tushare(STK_LIMIT, {"trade_date": day.strftime("%Y%m%d")})
    engine._validate_rows_for_date(STK_LIMIT, rows, day)
    rows = sorted((normalized_record(row) for row in rows), key=lambda row: row["ts_code"])
    with closing(connect(env_file, "production", readonly=True)) as conn:
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute("SELECT ts_code,trade_date,pre_close,up_limit,down_limit FROM market.stk_limit WHERE trade_date=%s ORDER BY ts_code", (day,))
            before = [normalized_record(row) for row in cur.fetchall()]
        conn.rollback()
    patches, unresolved = plan_repairs(day, rows, before)
    payload = {"schema_version": SCHEMA, "trade_date": day.isoformat(), "provider": "tushare.stk_limit",
               "source_contract_url": "https://tushare.pro/document/2?doc_id=183",
               "units": "CNY_per_share", "captured_at": datetime.now(timezone.utc).isoformat(),
               "historical_publication_time_proven": False, "source_rows_sha256": digest(rows),
               "source_rows": rows, "before_rows_sha256": digest(before), "before_rows": before,
               "patches": patches, "unresolved": unresolved, "database_write": False,
               "changed_columns": ["pre_close"], "target_database": "aistock"}
    write_exclusive(out / "source-plan.json", payload)
    with (out / "production-repair.sql").open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(repair_sql(day, patches, database="aistock"))
    return {"source_rows": len(rows), "patches": len(patches), "unresolved": len(unresolved),
            "source_rows_sha256": digest(rows), "plan": str(out / "source-plan.json"),
            "sql_sha256": hashlib.sha256((out / "production-repair.sql").read_bytes()).hexdigest()}


def validate_dev(env_file, source_path, out):
    raw = source_path.read_bytes()
    plan = json.loads(raw)
    if plan["schema_version"] != SCHEMA or digest(plan["source_rows"]) != plan["source_rows_sha256"]:
        raise ValueError("source identity differs")
    day = date.fromisoformat(plan["trade_date"])
    patches, unresolved = plan_repairs(day, plan["source_rows"], plan["before_rows"])
    if patches != plan["patches"] or unresolved != plan["unresolved"] or digest(plan["before_rows"]) != plan["before_rows_sha256"]:
        raise ValueError("repair plan differs from frozen facts")
    with closing(connect(env_file, "dev", readonly=False)) as conn:
        with conn.cursor() as cur:
            pgx.execute_values(cur, "INSERT INTO market.stk_limit(ts_code,trade_date,pre_close,up_limit,down_limit) VALUES %s ON CONFLICT(trade_date,ts_code) DO NOTHING",
                [(r["ts_code"], day, None, r["up_limit"], r["down_limit"]) for r in patches])
        conn.commit()
        sql = repair_sql(day, patches, database="aistock_dev")
        for _ in range(2):
            with conn.cursor() as cur:
                cur.execute(sql)
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute("SELECT ts_code,trade_date,pre_close,up_limit,down_limit FROM market.stk_limit WHERE trade_date=%s AND ts_code=ANY(%s) ORDER BY ts_code", (day, [r["ts_code"] for r in patches]))
            actual = [normalized_record(row) for row in cur.fetchall()]
        conn.rollback()
    if actual != patches:
        raise ValueError("DEV exact typed readback differs")
    receipt = {"schema_version": SCHEMA, "source_plan_sha256": hashlib.sha256(raw).hexdigest(),
               "dev_database": "aistock_dev", "verified_keys": len(actual), "dev_readback_sha256": digest(actual),
               "repeat_apply_passed": True, "production_database_written": False,
               "runtime_action": False, "replay_state_written": False}
    write_exclusive(out / "dev-receipt.json", receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("capture", "dev"))
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--trade-date", type=date.fromisoformat)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.mode == "capture" and args.trade_date is None:
        parser.error("capture requires --trade-date")
    if args.mode == "dev" and args.source is None:
        parser.error("dev requires --source")
    out = output_dir(args.out)
    result = (capture(args.env_file, args.trade_date, out) if args.mode == "capture"
              else validate_dev(args.env_file, args.source, out))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
