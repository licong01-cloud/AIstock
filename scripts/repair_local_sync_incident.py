"""BUG-1948: freeze recent provider facts, DEV-only apply, exact production plan.

No production apply mode. Preserve finite facts and frozen release inputs.
Adj-factor additions require a matching same-provider prior boundary, otherwise
the symbol must use the full-history reconciler instead of this bounded repair.
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
from backend.services.tushare_sync_engine import (  # noqa: E402
    _query_tushare_dataapi, _stk_limit_source_prices_valid, TushareSyncEngine,
)
from backend.services.tushare_dataset_specs import STK_LIMIT  # noqa: E402

SCHEMA = "local_data_sync_incident_repair_v1"
DAYS = ("20261014", "20261015", "20261016")
ANCHOR = "20261013"
A_SHARE = re.compile(r"(?:(?:00|30)[0-9]{4}\.SZ|6[0-9]{5}\.SH|[489][0-9]{5}\.BJ)\Z")


def digest(value):
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def write(path, value):
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(value) + b"\n")


def normalize(rows):
    return sorted([{k: str(v) if isinstance(v, (date, Decimal)) else v for k, v in row.items()}
                   for row in rows], key=lambda r: (r["trade_date"], r["ts_code"]))


def capture(env_file, out):
    token = dotenv_values(env_file).get("TUSHARE_TOKEN")
    if not token:
        raise ValueError("provider credential location unavailable")
    batches = []
    for api, days, fields in [("adj_factor", (ANCHOR, *DAYS), "ts_code,trade_date,adj_factor"),
                              ("stk_limit", (DAYS[-1],), ",".join(STK_LIMIT.columns))]:
        for day in days:
            def fetch():
                result = _query_tushare_dataapi(api, {"trade_date": day}, fields, token=token)
                if not result or len(result) >= 6000 or any(
                    row.get("trade_date") != day or set(fields.split(",")) - set(row) for row in result
                ) or len({r["ts_code"] for r in result}) != len(result):
                    raise ValueError(f"{api}/{day}: incomplete/ambiguous provider batch")
                return normalize(result)
            first, second = fetch(), fetch()
            if digest(first) != digest(second):
                raise ValueError(f"{api}/{day}: provider changed between readbacks")
            batches.append({"api": api, "day": day, "fields": fields, "rows": second,
                            "sha256": digest(second), "stable_reads": 2})
            print(json.dumps({"api": api, "day": day, "rows": len(second)}), flush=True)
    with closing(connect(env_file, "production")) as conn:
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute("SELECT ts_code,trade_date,adj_factor FROM market.adj_factor "
                        "WHERE trade_date BETWEEN '2026-10-13' AND '2026-10-16' ORDER BY trade_date,ts_code")
            local = normalize(cur.fetchall())
            cur.execute("SELECT ts_code,list_date FROM market.stock_basic ORDER BY ts_code")
            listings = {r["ts_code"]: str(r["list_date"]) for r in cur.fetchall()}
        conn.rollback()
    payload = {"schema_version": SCHEMA, "captured_at": datetime.now(timezone.utc).isoformat(),
               "batches": batches, "local_adj_boundary": local, "listings": listings,
               "database_write": False, "full_history_restatement_performed": False}
    write(out / "source.json", payload)
    return payload


def groups(payload):
    if payload["schema_version"] != SCHEMA:
        raise ValueError("source schema differs")
    facts = {}
    for batch in payload["batches"]:
        if digest(batch["rows"]) != batch["sha256"] or batch["stable_reads"] != 2:
            raise ValueError("source hash/stability differs")
        facts[batch["api"], batch["day"]] = batch["rows"]
    local = {(r["ts_code"], r["trade_date"]): r for r in payload["local_adj_boundary"]}
    anchor = {r["ts_code"]: r for r in facts["adj_factor", ANCHOR]}
    additions, unresolved, boundaries = [], [], {}
    for day in DAYS:
        for r in facts["adj_factor", day]:
            symbol = r["ts_code"]
            if not A_SHARE.fullmatch(symbol):
                raise ValueError("unsupported asset in adj-factor provider response")
            value = Decimal(str(r["adj_factor"]))
            if not value.is_finite() or value <= 0:
                raise ValueError("invalid source adj_factor")
            old = local.get((symbol, "2026-10-13"))
            source_old = anchor.get(symbol)
            listed = payload["listings"].get(symbol)
            if old and source_old and Decimal(str(old["adj_factor"])) == Decimal(str(source_old["adj_factor"])):
                boundaries[symbol] = old
            elif listed and "2026-10-13" < listed <= "2026-10-16":
                pass  # Newly listed: no earlier local history to splice.
            else:
                unresolved.append({"symbol": symbol, "day": day, "reason": "full_history_reconcile_required"})
                continue
            additions.append((day, symbol, str(value)))
    limits = facts["stk_limit", DAYS[-1]]
    for r in limits:
        if not _stk_limit_source_prices_valid(r["pre_close"], r["up_limit"], r["down_limit"]):
            raise ValueError("invalid/nullable limit source: do not invent a price")
    return additions, limits, list(boundaries.values()), unresolved


def repair_sql(cur, payload, database):
    additions, limits, boundaries, unresolved = groups(payload)
    blocks = []
    for table, columns, rows in [
        ("adj_factor", ["trade_date", "ts_code", "adj_factor"], additions),
        ("stk_limit", list(STK_LIMIT.columns), [[r[c] for c in STK_LIMIT.columns] for r in limits]),
    ]:
        # Cast VALUES explicitly; SQL compares facts, not textual spellings.
        types = ["text" if c == "ts_code" else "date" if c == "trade_date" else "numeric" for c in columns]
        template = "(" + ",".join("%s::" + typ for typ in types) + ")"
        values = ",".join(cur.mogrify(template, row).decode("utf-8") for row in rows)
        nonkeys = [c for c in columns if c not in {"ts_code", "trade_date"}]
        identity = "t.ts_code=s.ts_code AND t.trade_date=s.trade_date"
        different = " OR ".join(f"(t.{c} IS NOT NULL AND t.{c} IS DISTINCT FROM s.{c})" for c in nonkeys)
        same = " AND ".join(f"t.{c} IS NOT DISTINCT FROM s.{c}" for c in nonkeys)
        src = f"WITH s({','.join(columns)}) AS (VALUES {values}) "
        blocks += [src + f"SELECT count(*) INTO n FROM market.{table} t JOIN s ON {identity} WHERE {different};",
                   "IF n<>0 THEN RAISE EXCEPTION 'existing finite fact differs; separate revision required'; END IF;",
                   src + f"INSERT INTO market.{table}({','.join(columns)}) SELECT {','.join(columns)} FROM s "
                   "ON CONFLICT(trade_date,ts_code) DO UPDATE SET " +
                   ",".join(f"{c}=COALESCE({table}.{c},EXCLUDED.{c})" for c in nonkeys) + ";",
                   src + f"SELECT count(*) INTO n FROM s JOIN market.{table} t ON {identity} WHERE {same};",
                   f"IF n<>{len(rows)} THEN RAISE EXCEPTION 'typed readback differs'; END IF;"]
    # Production must still match the frozen prior normalization boundary.
    guards = []
    for r in boundaries:
        guards.append(cur.mogrify("PERFORM 1 FROM market.adj_factor WHERE ts_code=%s "
                                 "AND trade_date=%s::date AND adj_factor=%s::numeric FOR SHARE; IF NOT FOUND THEN "
                                 "RAISE EXCEPTION 'prior factor boundary drift'; END IF;",
                                 (r["ts_code"], r["trade_date"], r["adj_factor"])).decode("utf-8"))
    sql = ("BEGIN; SET LOCAL statement_timeout='120s'; SET LOCAL lock_timeout='5s';\n"
           "DO $repair$ DECLARE n bigint; BEGIN\nIF current_database()<>" +
           cur.mogrify("%s", (database,)).decode("utf-8") + " THEN RAISE EXCEPTION 'wrong target'; END IF;\n" +
           "\n".join(guards if database == "aistock" else []) + "\n" + "\n".join(blocks) + "\nEND $repair$;\nCOMMIT;\n")
    return sql, {"adj_factor_keys": len(additions), "stk_limit_keys": len(limits), "unresolved": unresolved,
                 "source_sha256": digest(payload), "database_audit_status_written": False}


def validate_dev(env_file, source, out):
    payload = json.loads(source.read_bytes())
    with closing(connect(env_file, "dev", readonly=False)) as conn:
        with conn.cursor() as cur:
            sql, result = repair_sql(cur, payload, "aistock_dev")
            for _ in range(2):
                cur.execute(sql)
        conn.commit()
        with conn.cursor() as cur:
            production_sql, _ = repair_sql(cur, payload, "aistock")
        conn.rollback()
    # DEV real producer must preserve the exact zero lower bound.
    limits = groups(payload)[1]
    with closing(connect(env_file, "dev", readonly=False)) as conn:
        engine = TushareSyncEngine()
        engine._upsert_plain_batch(conn, STK_LIMIT, limits)
        receipt, quality = engine._stk_limit_readback_quality(conn, date(2026, 10, 16))
        if quality["quality_status"] != "ok":
            raise ValueError("DEV producer readback not ready")
        conn.commit()
    result.update({"dev_apply_and_repeat": True, "dev_producer_readback": receipt,
                   "production_database_written": False, "runtime_action": False})
    with (out / "production-repair.sql").open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(production_sql)
    result["production_sql_sha256"] = hashlib.sha256(production_sql.encode("utf-8")).hexdigest()
    write(out / "dev-receipt.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("capture", "validate-dev"))
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    result = capture(args.env_file, args.out) if args.action == "capture" else validate_dev(args.env_file, args.source, args.out)
    print(json.dumps({k: v for k, v in result.items() if k not in {
        "batches", "listings", "local_adj_boundary", "unresolved"}}, default=str))
    if "unresolved" in result:
        print(json.dumps({"unresolved_count": len(result["unresolved"])}))


if __name__ == "__main__":
    main()
