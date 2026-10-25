"""Capture same-day provider facts; validate an exact invalid-price repair in DEV.

There is deliberately no production apply mode. The generated SQL requires
separate authorization; it changes pre_close only, never LocalSIM state.
"""
from __future__ import annotations

import argparse
from collections import Counter
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


def normalized_record(row, *, allow_invalid=False):
    result = {name: row[name] for name in FIELDS}
    for name in ("pre_close", "up_limit", "down_limit"):
        if result[name] is not None:
            value = Decimal(str(result[name]))
            if not value.is_finite() and not allow_invalid:
                raise ValueError("non-finite source/readback fact")
            result[name] = format(value.normalize(), "f")
    result["trade_date"] = str(result["trade_date"]).replace("-", "")
    return result


def reference_state(value):
    """Classify storage, not applicability; suspension never manufactures a price."""
    if value is None:
        return "NULL"
    number = Decimal(str(value))
    if not number.is_finite():
        return "NON_FINITE"
    return "NON_POSITIVE" if number <= 0 else "FINITE_POSITIVE"


def _documented_no_limit_codes(day, listing_facts, event_facts):
    codes = set()
    for fact in listing_facts:
        code = fact["ts_code"]
        if (set(fact) != {"ts_code", "list_date"} or not re.fullmatch(r"[489][0-9]{5}\.BJ", code)
                or str(fact["list_date"]) != day.isoformat() or code in codes):
            raise ValueError("no-limit listing identity differs")
        codes.add(code)
    for fact in event_facts:
        code = fact["ts_code"]
        if (set(fact) != {"ts_code", "event_date", "event_type", "source_url", "source_path", "source_content_sha256"}
                or not re.fullmatch(r"[489][0-9]{5}\.BJ", code) or code in codes
                or fact["event_date"] != day.isoformat() or fact["event_type"] != "DELISTING_FIRST_DAY"
                or not fact["source_url"].startswith("https://www.bse.cn/disclosure/")
                or hashlib.sha256(Path(fact["source_path"]).read_bytes()).hexdigest() != fact["source_content_sha256"]):
            raise ValueError("no-limit event authority differs")
        codes.add(code)
    return codes


def plan_repairs(day, source_rows, before, *, no_limit_listing_facts=(), no_limit_event_facts=()):
    """Use actual provider prices and identical dates/limit identities only."""
    no_limit_codes = _documented_no_limit_codes(day, no_limit_listing_facts, no_limit_event_facts)
    source = {}
    for raw in source_rows:
        row = normalized_record(raw)
        code = row["ts_code"]
        if row["trade_date"] != day.strftime("%Y%m%d") or not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", code):
            raise ValueError("source key/date differs")
        if code in source:
            raise ValueError("duplicate provider key")
        source[code] = row
    patches, unresolved, seen = [], [], set()
    for old in before:
        old = normalized_record(old, allow_invalid=True)
        code = old["ts_code"]
        if old["trade_date"] != day.strftime("%Y%m%d"):
            raise ValueError("database date differs")
        if code in seen:
            raise ValueError("duplicate database key")
        seen.add(code)
        if reference_state(old["pre_close"]) == "FINITE_POSITIVE":
            continue
        row = source.get(code)
        # BSE IPO sentinels are not ordinary limits. Require an independent
        # listing-date fact and an actual same-day provider reference price.
        documented_no_limit = bool(row and code in no_limit_codes
                                   and row["up_limit"] == "99999.99" and row["down_limit"] == "0"
                                   and reference_state(row["pre_close"]) == "FINITE_POSITIVE")
        if row is None or not (stk_limit_prices_valid(row) or documented_no_limit):
            unresolved.append({"ts_code": code, "reason": "provider_missing_or_invalid"})
            continue
        if any(old[name] is None or not Decimal(old[name]).is_finite()
               or Decimal(old[name]) != Decimal(row[name]) for name in ("up_limit", "down_limit")):
            unresolved.append({"ts_code": code, "reason": "provider_limit_identity_differs"})
            continue
        patches.append(row)
    return sorted(patches, key=lambda row: row["ts_code"]), unresolved


def repair_sql(day, rows, *, database, before=None, no_limit_listing_facts=(), no_limit_event_facts=()):
    if database not in {"aistock", "aistock_dev"} or not rows:
        raise ValueError("empty or unsupported database target")
    if before is not None:
        return exact_repair_sql(day, rows, database=database, before=before,
                                no_limit_listing_facts=no_limit_listing_facts, no_limit_event_facts=no_limit_event_facts)
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


def exact_repair_sql(day, rows, *, database, before, no_limit_listing_facts=(), no_limit_event_facts=()):
    normalized = sorted((normalized_record(row) for row in rows), key=lambda row: row["ts_code"])
    if normalized != rows:
        raise ValueError("repair rows must be canonical and ordered")
    codes = {row["ts_code"] for row in rows}
    previous = [normalized_record(row, allow_invalid=True) for row in before if row["ts_code"] in codes]
    checked, unresolved = plan_repairs(day, rows, previous, no_limit_listing_facts=no_limit_listing_facts,
                                       no_limit_event_facts=no_limit_event_facts)
    if unresolved or checked != rows:
        raise ValueError("repair must bind an invalid original value for every key")
    old_by_code = {row["ts_code"]: row for row in previous}

    def numeric_literal(value):
        if value is None:
            return "NULL::numeric"
        number = Decimal(str(value))
        return f"'{number}'::numeric" if not number.is_finite() else f"{number}::numeric"

    values = ",\n".join(
        f"('{day.isoformat()}'::date,'{r['ts_code']}',{r['pre_close']}::numeric,"
        f"{r['up_limit']}::numeric,{r['down_limit']}::numeric,"
        f"{numeric_literal(old_by_code[r['ts_code']]['pre_close'])})" for r in rows
    )
    source = f"WITH src(trade_date,ts_code,pre_close,up_limit,down_limit,old_pre_close) AS (VALUES {values})"
    return f"""-- Exact same-day source-backed pre_close repair; CAS original value; no other columns.
BEGIN;
SET LOCAL statement_timeout='60s';
SET LOCAL lock_timeout='5s';
DO $repair$
DECLARE expected_count integer := {len(rows)}; matched integer;
BEGIN
 IF current_database() <> '{database}' THEN RAISE EXCEPTION 'wrong database'; END IF;
 {source}
 SELECT count(*) INTO matched FROM src JOIN market.stk_limit t USING(trade_date,ts_code)
 WHERE t.up_limit=src.up_limit AND t.down_limit=src.down_limit
 AND (t.pre_close IS NOT DISTINCT FROM src.old_pre_close OR t.pre_close=src.pre_close);
 IF matched <> expected_count THEN RAISE EXCEPTION 'repair identity drift'; END IF;
 {source}
 UPDATE market.stk_limit t SET pre_close=src.pre_close FROM src
 WHERE t.trade_date=src.trade_date AND t.ts_code=src.ts_code
 AND t.pre_close IS NOT DISTINCT FROM src.old_pre_close
 AND t.up_limit=src.up_limit AND t.down_limit=src.down_limit;
 {source}
 SELECT count(*) INTO matched FROM src JOIN market.stk_limit t USING(trade_date,ts_code)
 WHERE t.pre_close=src.pre_close AND t.up_limit=src.up_limit AND t.down_limit=src.down_limit;
 IF matched <> expected_count THEN RAISE EXCEPTION 'repair readback differs'; END IF;
END $repair$;
COMMIT;
"""


def capture(env_file, day, out, *, required_codes=None):
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
            before = [normalized_record(row, allow_invalid=True) for row in cur.fetchall()
                      if required_codes is None or row["ts_code"] in required_codes]
            cur.execute("SELECT ts_code,list_date FROM market.stock_basic WHERE list_date=%s AND ts_code=ANY(%s) AND ts_code LIKE '%%.BJ' ORDER BY ts_code",
                        (day, [row["ts_code"] for row in before]))
            listing_facts = [{"ts_code": row["ts_code"], "list_date": str(row["list_date"])} for row in cur.fetchall()]
        conn.rollback()
    patches, unresolved = plan_repairs(day, rows, before, no_limit_listing_facts=listing_facts)
    payload = {"schema_version": SCHEMA, "trade_date": day.isoformat(), "provider": "tushare.stk_limit",
               "source_contract_url": "https://tushare.pro/document/2?doc_id=183",
               "units": "CNY_per_share", "captured_at": datetime.now(timezone.utc).isoformat(),
               "historical_publication_time_proven": False, "source_rows_sha256": digest(rows),
               "source_rows": rows, "before_rows_sha256": digest(before), "before_rows": before,
               "patches": patches, "unresolved": unresolved, "database_write": False,
               "no_limit_listing_facts": listing_facts, "no_limit_listing_facts_sha256": digest(listing_facts),
               "changed_columns": ["pre_close"], "target_database": "aistock",
               "repair_original_value_contract": "exact_invalid_reference_cas_v1"}
    write_exclusive(out / "source-plan.json", payload)
    sql_path = out / "production-repair.sql"
    if patches:
        with sql_path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(repair_sql(day, patches, database="aistock", before=before,
                                    no_limit_listing_facts=listing_facts))
    return {"source_rows": len(rows), "patches": len(patches), "unresolved": len(unresolved),
            "source_rows_sha256": digest(rows), "plan": str(out / "source-plan.json"),
            "sql_sha256": hashlib.sha256(sql_path.read_bytes()).hexdigest() if patches else None}


def validate_dev(env_file, source_path, out):
    raw = source_path.read_bytes()
    plan = json.loads(raw)
    if plan["schema_version"] != SCHEMA or digest(plan["source_rows"]) != plan["source_rows_sha256"]:
        raise ValueError("source identity differs")
    day = date.fromisoformat(plan["trade_date"])
    listing_facts = plan.get("no_limit_listing_facts", [])
    if listing_facts and digest(listing_facts) != plan.get("no_limit_listing_facts_sha256"):
        raise ValueError("listing fact identity differs")
    event_facts = plan.get("no_limit_event_facts", [])
    if event_facts and digest(event_facts) != plan.get("no_limit_event_facts_sha256"):
        raise ValueError("event fact identity differs")
    patches, unresolved = plan_repairs(day, plan["source_rows"], plan["before_rows"],
                                       no_limit_listing_facts=listing_facts, no_limit_event_facts=event_facts)
    if patches != plan["patches"] or unresolved != plan["unresolved"] or digest(plan["before_rows"]) != plan["before_rows_sha256"]:
        raise ValueError("repair plan differs from frozen facts")
    if not patches:
        receipt = {"schema_version": SCHEMA, "source_plan_sha256": hashlib.sha256(raw).hexdigest(),
                   "verified_keys": 0, "unresolved_count": len(unresolved), "status": "NO_REPAIRABLE_FACTS",
                   "dev_database_written": False, "production_database_written": False,
                   "runtime_action": False, "replay_state_written": False}
        write_exclusive(out / "dev-receipt.json", receipt)
        return receipt
    old_by_code = {row["ts_code"]: row for row in plan["before_rows"]}
    with closing(connect(env_file, "dev", readonly=False)) as conn:
        with conn.cursor() as cur:
            pgx.execute_values(cur, "INSERT INTO market.stk_limit(ts_code,trade_date,pre_close,up_limit,down_limit) VALUES %s ON CONFLICT(trade_date,ts_code) DO NOTHING",
                [(r["ts_code"], day, old_by_code[r["ts_code"]]["pre_close"], r["up_limit"], r["down_limit"])
                 for r in patches])
        conn.commit()
        sql = repair_sql(day, patches, database="aistock_dev",
                         before=plan["before_rows"] if plan.get("repair_original_value_contract") else None,
                         no_limit_listing_facts=listing_facts, no_limit_event_facts=event_facts)
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


def applicable_classification(row):
    """Keep daily selection eligibility separate from holding quote requirements."""
    valid = stk_limit_prices_valid(row)
    if row["has_trade"]:
        if not row["limit_row_exists"]:
            return "TRADED_LIMIT_ROW_MISSING"
        if not valid and reference_state(row["pre_close"]) == "FINITE_POSITIVE":
            return "TRADED_LIMIT_APPLICABILITY_UNPROVEN"
        return "TRADED_VALID" if valid else "TRADED_PRICE_INVALID"
    if row["suspend_s"] and not row["suspend_r"]:
        return "DOCUMENTED_SUSPENSION_NO_TRADE"
    if row["suspend_s"] and row["suspend_r"]:
        return "MIXED_SUSPENSION_NO_TRADE_UNPROVEN"
    return "PIT_NO_TRADE_UNPROVEN"


def audit(env_file, start, end, pit_key, out):
    """Explicit bounded historical audit; never invoked as a daily full-history scan."""
    if end < start or (end - start).days > 366 or not pit_key:
        raise ValueError("audit requires an explicit PIT key and a range of at most one year")
    engine = TushareSyncEngine()
    summaries = []
    all_counts = Counter()
    with closing(connect(env_file, "production", readonly=True)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status,dirty FROM market.stock_universe_pit_state WHERE universe_key=%s", (pit_key,))
            if cur.fetchone() != ("ready", False):
                raise ValueError("PIT authority is not ready/clean")
        days = engine._trading_dates_between(conn, start, end)
        if not days:
            raise ValueError("formal trading calendar has no dates in requested range")
        for day in days:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(r"""WITH traded AS MATERIALIZED (
                    SELECT DISTINCT ts_code FROM market.kline_daily_raw
                    WHERE trade_date=%(day)s AND adjust_type='none' AND close_li>0
                      AND (volume_hand>0 OR volume_shares>0)
                      AND ts_code ~ '^(00|30)[0-9]{4}\.SZ$|^6[0-9]{5}\.SH$|^[489][0-9]{5}\.BJ$'
                ), pit AS MATERIALIZED (
                    SELECT DISTINCT ts_code FROM market.stock_universe_pit_spans
                    WHERE universe_key=%(pit)s AND %(day)s BETWEEN eligible_start AND eligible_end
                ), required AS (SELECT ts_code FROM traded UNION SELECT ts_code FROM pit)
                SELECT r.ts_code,%(day)s::date AS trade_date,s.pre_close,s.up_limit,s.down_limit,
                    s.ts_code IS NOT NULL AS limit_row_exists,
                    t.ts_code IS NOT NULL AS has_trade,p.ts_code IS NOT NULL AS in_pit,
                    EXISTS(SELECT 1 FROM market.suspend_d d WHERE d.ts_code=r.ts_code
                           AND d.trade_date=%(day)s AND d.suspend_type='S') AS suspend_s,
                    EXISTS(SELECT 1 FROM market.suspend_d d WHERE d.ts_code=r.ts_code
                           AND d.trade_date=%(day)s AND d.suspend_type='R') AS suspend_r
                FROM required r LEFT JOIN market.stk_limit s ON s.ts_code=r.ts_code AND s.trade_date=%(day)s
                LEFT JOIN traded t ON t.ts_code=r.ts_code LEFT JOIN pit p ON p.ts_code=r.ts_code
                ORDER BY r.ts_code""", {"day": day, "pit": pit_key})
                rows = cur.fetchall()
            counts = Counter()
            issues = []
            for raw in rows:
                row = {**raw, **normalized_record(raw, allow_invalid=True)}
                category = applicable_classification(row)
                counts[category] += 1
                counts["PIT_KEYS"] += int(row["in_pit"])
                counts["TRADED_KEYS"] += int(row["has_trade"])
                if category != "TRADED_VALID":
                    issues.append({**row, "classification": category,
                                   "reference_state": reference_state(row["pre_close"])})
            payload = {"trade_date": day.isoformat(), "counts": dict(counts), "issues": issues}
            filename = f"audit-{day.isoformat()}.json"
            write_exclusive(out / filename, payload)
            summaries.append({"trade_date": day.isoformat(), "counts": dict(counts),
                              "issues_sha256": digest(issues), "file": filename})
            all_counts.update(counts)
        conn.rollback()
    receipt = {"schema_version": "local_data_stk_limit_applicable_audit_v1",
               "captured_at": datetime.now(timezone.utc).isoformat(),
               "start_date": start.isoformat(), "end_date": end.isoformat(), "pit_key": pit_key,
               "counts": dict(all_counts), "trade_date_count": len(days), "daily": summaries,
               "applicability": "daily_PIT_union_real_traded_equities_including_holding_quotes",
               "suspension_semantics": "S_without_R_and_no_trade_is_documented_not_a_manufactured_price",
               "database_read": True, "database_write": False, "runtime_action": False,
               "candidate_write": False, "profile_write": False, "replay_write": False}
    write_exclusive(out / "audit-receipt.json", receipt)
    return {key: value for key, value in receipt.items() if key != "daily"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("capture", "dev", "audit"))
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--trade-date", type=date.fromisoformat)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--pit-key")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.mode == "capture" and args.trade_date is None:
        parser.error("capture requires --trade-date")
    if args.mode == "dev" and args.source is None:
        parser.error("dev requires --source")
    if args.mode == "audit" and not (args.start_date and args.end_date and args.pit_key):
        parser.error("audit requires --start-date, --end-date and --pit-key")
    out = output_dir(args.out)
    if args.mode == "audit":
        result = audit(args.env_file, args.start_date, args.end_date, args.pit_key, out)
    elif args.mode == "capture":
        result = capture(args.env_file, args.trade_date, out)
    else:
        result = validate_dev(args.env_file, args.source, out)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
