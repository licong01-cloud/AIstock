"""Explicit source-bound sector repair planning/DEV validation; never production apply."""
from __future__ import annotations

import argparse
import hashlib
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import psycopg2  # noqa: E402
import psycopg2.extras as pgx  # noqa: E402
from dotenv import dotenv_values  # noqa: E402

from backend.services.sector_data_builder import (  # noqa: E402
    _BUILD_DAY_SQL, _DELETE_STALE_DAY_SQL, _PREFLIGHT_DAY_SQL,
    DEFAULT_ST_PIT_UNIVERSE_KEY, IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX,
)
from backend.services.sector_data_repair import (  # noqa: E402
    projection_revision_plan, membership_revision_spans, _MARK_SQL, _CLEAR_SQL,
)
from backend.services.sw_industry_history_store import canonical_bytes, digest  # noqa: E402

FIELDS = ("l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name",
          "ts_code", "name", "in_date", "out_date", "is_new")
UPSERT = (f"INSERT INTO market.sw_index_member ({','.join(FIELDS)}) VALUES %s "
          "ON CONFLICT (l2_code,ts_code,in_date) DO UPDATE SET " +
          ",".join(f"{f}=EXCLUDED.{f}" for f in FIELDS if f not in {"l2_code", "ts_code", "in_date"}))


def receipt_value(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {k: receipt_value(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [receipt_value(v) for v in value]
    return value


def connect(env_file, target, *, readonly=True):
    cfg = dotenv_values(env_file)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    expected = "aistock_dev" if target == "dev" else "aistock"
    if cfg[prefix + "NAME"] != expected:
        raise ValueError("explicit database target differs")
    conn = psycopg2.connect(host=cfg[prefix + "HOST"], port=cfg[prefix + "PORT"],
                            dbname=expected, user=cfg[prefix + "USER"], password=cfg[prefix + "PASSWORD"],
                            connect_timeout=10, application_name="codex_bug1883_sector_repair",
                            options="-c statement_timeout=120000 -c lock_timeout=5000")
    conn.set_session(readonly=readonly, isolation_level="REPEATABLE READ")
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        if cur.fetchone()[0] != expected:
            raise ValueError("database readback identity differs")
    conn.rollback()
    return conn


def load_plan(conn, source_id, start, end):
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute("SELECT * FROM market.sw_index_member ORDER BY l2_code,ts_code,in_date")
        previous = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT fact_sha256,payload FROM market.sw_industry_source_fact_v1 WHERE source_id=%s AND payload->>'endpoint'='index_member_all' ORDER BY fact_sha256", (source_id,))
        facts = [dict(r) for r in cur.fetchall()]
        if not facts:
            raise ValueError("formal dated source capture is absent")
        cur.execute("SELECT fact_sha256,bundle_sha256,payload FROM market.sw_industry_classification_history_v1 WHERE taxonomy_version='SW2021' ORDER BY fact_sha256")
        history = [dict(r) for r in cur.fetchall()]
        if len({r["bundle_sha256"] for r in history}) != 1:
            raise ValueError("approved history needs a single explicit immutable bundle")
    plan = projection_revision_plan(previous, facts, start, end, approved_history=history)
    unknown = plan["unverified_existing_identities"]
    with conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT spans.ts_code FROM market.stock_universe_pit_spans spans
            JOIN market.stock_universe_pit_state state USING (universe_key)
            WHERE state.status='ready' AND NOT state.dirty
              AND (state.universe_key=%s OR state.universe_key LIKE %s)
              AND spans.eligible_start<=%s AND spans.eligible_end>=%s
              AND spans.ts_code=ANY(%s)""",
            (DEFAULT_ST_PIT_UNIVERSE_KEY, IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX + "%", end, start,
             [r["ts_code"] for r in unknown]))
        eligible = {r[0] for r in cur.fetchall()}
    plan["unverified_required_identities"] = [r for r in unknown if r["ts_code"] in eligible]
    plan["unverified_outside_consumption_count"] = len(unknown) - len(plan["unverified_required_identities"])
    plan.update(source_id=source_id, previous_projection_sha256=digest(previous))
    plan["approved_history_rowset_sha256"] = digest(sorted(r["fact_sha256"] for r in history))
    return plan


def audit_day(conn, day, *, projection_changes=()):
    """Compare every persisted field with the formal same-day source expression."""
    prefix, rest = _BUILD_DAY_SQL.split("INSERT INTO market.sector_data (", 1)
    if projection_changes:
        with conn.cursor() as cur:
            changed = [r["row"] for r in projection_changes]
            literal = ",".join(cur.mogrify("(%s::text,%s::text,%s::date,%s::date)",
                (r["ts_code"], r["l2_code"], r["in_date"], r.get("out_date"))).decode("utf-8") for r in changed)
            overlay = "(SELECT ts_code,l2_code,in_date,out_date FROM market.sw_index_member m WHERE NOT EXISTS (SELECT 1 FROM (VALUES " + literal + ") AS n(ts_code,l2_code,in_date,out_date) WHERE n.ts_code=m.ts_code AND n.l2_code=m.l2_code AND n.in_date=m.in_date) UNION ALL SELECT * FROM (VALUES " + literal + ") AS n(ts_code,l2_code,in_date,out_date)) AS member"
        prefix = prefix.replace("market.sw_index_member AS member", overlay)
    columns, expression = rest.split(")\nSELECT", 1)
    expression = "SELECT" + expression.split("ON CONFLICT (trade_date, ts_code)", 1)[0]
    cols = [value.strip() for value in columns.split(",")]
    values = cols[2:]
    sql = "WITH expected (" + ",".join(cols) + ") AS (" + prefix + expression + ") "
    sql += "SELECT COUNT(*) FROM expected e FULL JOIN (SELECT * FROM market.sector_data WHERE trade_date=%(trade_date)s) a USING(trade_date,ts_code) WHERE e.ts_code IS NULL OR a.ts_code IS NULL OR ("
    sql += ",".join("e." + col for col in values) + ") IS DISTINCT FROM (" + ",".join("a." + col for col in values) + ")"
    with conn.cursor() as cur:
        cur.execute(sql, params(day))
        return cur.fetchone()[0]


def params(day):
    return dict(trade_date=day, live_universe_key=DEFAULT_ST_PIT_UNIVERSE_KEY,
                qe_universe_pattern=IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX + "%")


def repair_sql(conn, plan, dates, target):
    """Exact, bounded SQL for operator review; no inferred removals or DDL."""
    with conn.cursor() as cur:
        rows = [item["row"] for item in plan["changes"]]
        chunks = ["BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;",
                  "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='120s';",
                  "DO $target$ BEGIN IF current_database()<>" + cur.mogrify("%s", ("aistock_dev" if target == "dev" else "aistock",)).decode("utf-8") + " THEN RAISE EXCEPTION 'database target differs'; END IF; END $target$;",
                  "LOCK TABLE market.sw_index_member IN SHARE ROW EXCLUSIVE MODE;"]
        spans = [(max(a, plan["start_date"]), min(b or plan["end_date"], plan["end_date"]))
                 for a, b in membership_revision_spans([r["previous"] for r in plan["changes"] if r["previous"]], rows)
                 if a <= plan["end_date"] and (b is None or b >= plan["start_date"])]
        pending = pgx.execute_values(cur, "SELECT DISTINCT trade_date FROM market.sector_data JOIN (VALUES %s) AS spans(a,b) ON trade_date>=a AND trade_date<=b ORDER BY trade_date", spans, template="(%s::date,%s::date)", fetch=True) if spans else []
        affected = [r[0] for r in pending]
        if affected:
            chunks.append(cur.mogrify(_MARK_SQL, ("approved_sw_membership_repair", affected)).decode("utf-8") + ";")
        if rows:
            literals = [cur.mogrify("(" + ",".join(["%s"] * len(FIELDS)) + ")", [r.get(f) for f in FIELDS]).decode("utf-8") for r in rows]
            chunks.append(UPSERT.replace("%s", ",".join(literals), 1) + ";")
        for day in dates:
            preflight = cur.mogrify(_PREFLIGHT_DAY_SQL, params(day)).decode("utf-8")
            chunks.append("DO $sector_preflight$ BEGIN IF EXISTS (SELECT 1 FROM (" + preflight + ") AS p(a,b,c,d,e,f,g) WHERE a<>0 OR b<>0 OR c<>0 OR d<>0 OR e<>0 OR f<>0) THEN RAISE EXCEPTION 'sector repair preflight failed'; END IF; END $sector_preflight$;")
            for sql in (_DELETE_STALE_DAY_SQL, _BUILD_DAY_SQL):
                chunks.append(cur.mogrify(sql, params(day)).decode("utf-8") + ";")
            # The count is existing valid facts, not the number of changed rows.
            chunks.append(cur.mogrify(_CLEAR_SQL, (day, None, day)).decode("utf-8") + ";")
        chunks.append("COMMIT;")
        return "\n".join(chunks), affected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--target", choices=("dev", "production"), required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--apply-dev", action="store_true")
    parser.add_argument("--stage-facts-dev", action="store_true", help="Stage bounded production quote/moneyflow read-only inputs into existing DEV")
    parser.add_argument("--expected-plan-sha256")
    args = parser.parse_args()
    if args.start_date < date(2021, 7, 30) or args.start_date > args.end_date:
        raise ValueError("SW2021 compatibility repair window differs")
    if (args.apply_dev or args.stage_facts_dev) and args.target != "dev":
        raise ValueError("this CLI never applies production writes")
    if args.output_dir.resolve().drive.upper() != "X:" or args.output_dir.exists():
        raise ValueError("receipt output requires a new X-drive directory")
    conn = connect(args.env_file, args.target, readonly=not args.apply_dev)
    try:
        plan = load_plan(conn, args.source_id, args.start_date, args.end_date)
        stage = {}
        if args.stage_facts_dev:
            source = connect(args.env_file, "production")
            try:
                with source.cursor() as cur:
                    for table in ("sw_daily", "moneyflow_ts"):
                        cur.execute(f"SELECT * FROM market.{table} WHERE trade_date BETWEEN %s AND %s ORDER BY ts_code,trade_date", (args.start_date, args.end_date))
                        columns = [col.name for col in cur.description]
                        values = cur.fetchall()
                        if not values:
                            raise ValueError("DEV source validation inputs are absent: " + table)
                        sha = hashlib.sha256()
                        for row in values:
                            sha.update(canonical_bytes(receipt_value(dict(zip(columns, row)))) + b"\n")
                        stage[table] = dict(columns=columns, values=values, rowset_sha256=sha.hexdigest())
                plan["dev_staged_source_pins"] = {table: dict(row_count=len(item["values"]), rowset_sha256=item["rowset_sha256"]) for table, item in stage.items()}
            finally:
                source.rollback()
                source.close()
        with conn.cursor() as cur:
            if stage:
                index = stage["moneyflow_ts"]["columns"].index("trade_date")
                dates = sorted({row[index] for row in stage["moneyflow_ts"]["values"]})
            else:
                cur.execute("SELECT DISTINCT trade_date FROM market.moneyflow_ts WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date", (args.start_date, args.end_date))
                dates = [r[0] for r in cur.fetchall()]
        before = {str(day): audit_day(conn, day, projection_changes=plan["changes"]) for day in dates}
        repair_dates = dates if stage else [day for day in dates if before[str(day)]]
        sql, affected = repair_sql(conn, plan, repair_dates, args.target)
        conn.rollback()
        result = dict(schema_version="sw_sector_revision_receipt_v1", plan=plan,
                      plan_sha256=digest(plan), repair_dates=repair_dates, audited_dates=dates,
                      before_fact_mismatch_counts=before, invalidated_dates=affected,
                      sql_sha256=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                      dev_database_write=False, production_database_write=False,
                      candidate_write=False, profile_write=False, runtime_action=False)
        if args.apply_dev:
            if args.expected_plan_sha256 != digest(plan):
                raise ValueError("DEV apply requires the exact reviewed dry-run plan pin")
            if plan["unverified_required_identities"]:
                raise ValueError("unverified local member identities need explicit resolution before apply")
            with conn.cursor() as cur:
                cur.execute("LOCK TABLE market.sw_index_member IN SHARE ROW EXCLUSIVE MODE")
            locked_plan = load_plan(conn, args.source_id, args.start_date, args.end_date)
            if stage:
                locked_plan["dev_staged_source_pins"] = plan["dev_staged_source_pins"]
            if digest(locked_plan) != digest(plan):
                raise ValueError("projection changed after reviewed dry-run")
            for table, item in stage.items():
                with conn.cursor() as cur:
                    pgx.execute_values(cur, "INSERT INTO market." + table + " (" + ",".join(item["columns"]) + ") VALUES %s ON CONFLICT (ts_code,trade_date) DO NOTHING", item["values"], page_size=1000)
            result["dev_staged_source_pins"] = plan.get("dev_staged_source_pins", {})
            # No COMMIT until independent readback is complete.
            with conn.cursor() as cur:
                cur.execute(sql.replace("BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;", "").removesuffix("COMMIT;"))
            after = load_plan(conn, args.source_id, args.start_date, args.end_date)
            if after["changes"] or any(after[key] != plan[key] for key in ("source_rows_sha256", "approved_history_rowset_sha256")):
                raise ValueError("source-bound projection readback or source immutability differs")
            result["daily_fact_mismatch_counts"] = {str(day): audit_day(conn, day) for day in dates}
            if any(result["daily_fact_mismatch_counts"].values()):
                raise ValueError("sector source facts readback differs")
            result["source_facts_unchanged"] = True
            result["projection_after_sha256"] = load_plan(conn, args.source_id, args.start_date, args.end_date)["previous_projection_sha256"]
            with conn.cursor() as cur:
                cur.execute("SELECT ts_code,trade_date,sw2_pct_change,sw2_close,sw2_vol,sw2_amount,sw2_mf_net_amt FROM market.sector_data WHERE ts_code='002086.SZ' AND trade_date BETWEEN %s AND %s ORDER BY trade_date", (args.start_date, args.end_date))
                result["focus_readback"] = receipt_value(cur.fetchall())
            conn.commit()
            result["dev_database_write"] = True
        args.output_dir.mkdir(parents=True, exist_ok=False)
        for name, raw in (("repair_plan.json", canonical_bytes(result) + b"\n"), ("repair.sql", sql.encode("utf-8"))):
            with (args.output_dir / name).open("xb") as handle:
                handle.write(raw)
        compact = {k: v for k, v in result.items() if k not in {"plan", "focus_readback", "invalidated_dates", "repair_dates", "audited_dates", "before_fact_mismatch_counts", "daily_fact_mismatch_counts"}}
        compact.update(audited_date_count=len(dates), repair_date_count=len(repair_dates), invalidated_date_count=len(affected),
                       before_fact_mismatch_count=sum(before.values()),
                       after_fact_mismatch_count=sum(result["daily_fact_mismatch_counts"].values()) if args.apply_dev else None,
                       unverified_required_count=len(plan["unverified_required_identities"]))
        print(canonical_bytes(compact).decode("utf-8"))
        print("changes=" + str(len(plan["changes"])) + " unverified=" + str(len(plan["unverified_existing_identities"])))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
