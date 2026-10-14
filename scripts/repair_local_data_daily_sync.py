"""Capture real provider facts and validate null-only repairs in existing DEV.

No production apply command is supplied. Source/plan/DEV receipts are exclusive
application outputs; a separately authorized production apply remains distinct.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import dotenv_values  # noqa: E402
import psycopg2.extras as pgx  # noqa: E402
from scripts.repair_sw_sector_revision import connect  # noqa: E402
from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.tushare_sync_engine import (  # noqa: E402
    _query_tushare_dataapi, _etf_share_size_publication_quality,
)
from backend.services.data_refresh_audit import DataRefreshAuditRepository  # noqa: E402
from backend.services.tushare_dataset_specs import ETF_SHARE_SIZE, MARGIN_DETAIL  # noqa: E402

DAY = "20261008"
DAY_CODES = {"000031.SZ", "000032.SZ", "000034.SZ"}
ETF_DAYS = ("20260930", "20261008", "20261009", "20261012", "20261013")
DAILY_FIELDS = "ts_code,trade_date,open,high,low,close,vol,amount"
RAW_COLUMNS = ("trade_date", "ts_code", "open_li", "high_li", "low_li", "close_li",
               "volume_hand", "amount_li", "adjust_type", "source", "volume_shares",
               "volume_shares_source", "volume_shares_sha256")


def digest(payload):
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def exclusive_json(path, payload):
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(payload) + b"\n")


def exact_integer(value, scale):
    scaled = Decimal(str(value)) * scale
    if not scaled.is_finite() or scaled != scaled.to_integral_value():
        raise ValueError("provider fact does not satisfy exact integer storage precision")
    return int(scaled)


def daily_row(row):
    shares = exact_integer(row["vol"], 100)
    result = {
        "trade_date": date.fromisoformat(f'{DAY[:4]}-{DAY[4:6]}-{DAY[6:]}'),
        "ts_code": row["ts_code"], "adjust_type": "none", "source": "tushare_api",
        "volume_hand": int(Decimal(str(row["vol"])).to_integral_value(rounding=ROUND_FLOOR)),
        "volume_shares": shares, "volume_shares_source": "tushare_daily",
        "volume_shares_sha256": digest(row),
        "amount_li": exact_integer(row["amount"], 1_000_000),
    }
    for field in ("open", "high", "low", "close"):
        result[field + "_li"] = exact_integer(row[field], 1000)
    if not 0 < result["low_li"] <= min(result["open_li"], result["close_li"]) <= max(result["open_li"], result["close_li"]) <= result["high_li"]:
        raise ValueError("invalid source OHLC")
    return result


def capture(env_file):
    token = dotenv_values(env_file).get("TUSHARE_TOKEN")
    if not token:
        raise ValueError("TUSHARE_TOKEN location is unavailable")
    requests = [("daily", DAY, DAILY_FIELDS)]
    requests += [(ETF_SHARE_SIZE.tushare_api, day, ",".join(ETF_SHARE_SIZE.columns)) for day in ETF_DAYS]
    requests += [(MARGIN_DETAIL.tushare_api, "20261013", ",".join(MARGIN_DETAIL.columns))]
    batches = []
    for api, day, fields in requests:
        rows = _query_tushare_dataapi(api, {"trade_date": day}, fields, token=token)
        if not rows or len(rows) >= 6000:
            raise ValueError(f"{api}/{day}: empty or possibly truncated response")
        identities = {(r.get("ts_code"), r.get("trade_date")) for r in rows}
        if len(identities) != len(rows) or any(r.get("trade_date") != day for r in rows):
            raise ValueError("provider identity/date conflict")
        if any(set(fields.split(",")) - set(r) for r in rows):
            raise ValueError("provider field schema is incomplete")
        batches.append({"api": api, "request_date": day, "fields": fields,
                        "row_count": len(rows), "rows_sha256": digest(rows), "rows": rows})
        print(json.dumps({"api": api, "date": day, "rows": len(rows)}, sort_keys=True))
    return {"schema_version": "local_data_daily_sync_source_v1", "provider": "tushare",
            "captured_at": datetime.now(timezone.utc).isoformat(), "batches": batches}


def source_groups(source):
    if source.get("schema_version") != "local_data_daily_sync_source_v1" or source.get("provider") != "tushare":
        raise ValueError("source identity is unsupported")
    expected = {("daily", DAY), ("margin_detail", "20261013")}
    expected.update(("etf_share_size", day) for day in ETF_DAYS)
    if len(source["batches"]) != len(expected) or {(b["api"], b["request_date"]) for b in source["batches"]} != expected:
        raise ValueError("source request scope differs from the exact repair plan")
    for batch in source["batches"]:
        if digest(batch["rows"]) != batch["rows_sha256"]:
            raise ValueError("source rows hash differs")
        if any(row.get("trade_date") != batch["request_date"] for row in batch["rows"]):
            raise ValueError("source fact date differs from the frozen request")
        if batch["api"] == "daily":
            rows = [daily_row(row) for row in batch["rows"] if row["ts_code"] in DAY_CODES]
            if {r["ts_code"] for r in rows} != DAY_CODES:
                raise ValueError("three missing daily keys are not all present at provider")
            yield "kline_daily_raw", RAW_COLUMNS, rows
        else:
            if batch["api"] not in {"etf_share_size", "margin_detail"}:
                raise ValueError("source dataset is outside this repair scope")
            spec = ETF_SHARE_SIZE if batch["api"] == "etf_share_size" else MARGIN_DETAIL
            rows = []
            for raw in batch["rows"]:
                row = dict(raw)
                value = row["trade_date"]
                row["trade_date"] = date(int(value[:4]), int(value[4:6]), int(value[6:]))
                rows.append(row)
            yield spec.name, tuple(spec.columns), rows


def existing_rows(conn, table, columns, rows):
    day = rows[0]["trade_date"]
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(f"SELECT {','.join(columns)} FROM market.{table} WHERE trade_date=%s AND ts_code=ANY(%s)",
                    (day, [r["ts_code"] for r in rows]))
        return {str(r["ts_code"]).strip(): dict(r) for r in cur.fetchall()}


def plan_group(table, columns, rows, before):
    patches, stats = [], Counter()
    for row in rows:
        old = before.get(row["ts_code"])
        if old is None:
            patches.append(row)
            stats["missing_rows"] += 1
            stats["source_null_fields"] += sum(v is None for v in row.values())
        else:
            fields = [field for field in columns if old[field] is None and row[field] is not None]
            if fields:
                # Raw facts are insert-only. An existing raw row may not be edited.
                if table == "kline_daily_raw":
                    raise ValueError("daily raw identity appeared concurrently; no overwrite permitted")
                patch = {field: row[field] if field in fields or field in {"trade_date", "ts_code"} else None for field in columns}
                patches.append(patch)
                stats["null_fields_to_fill"] += len(fields)
                stats["rows_to_fill"] += 1
            stats["preserved_nonnull_fields"] += sum(v is not None for v in old.values())
    return patches, dict(stats)


def readback_audit_contract(table, rows, actual, source_sha256, *, provider_rows_sha256=None):
    """Certify the captured full-date sources, never a partial raw repair."""
    if table == "kline_daily_raw":
        return None
    if table not in {"etf_share_size", "margin_detail"}:
        raise ValueError("unsupported repair audit dataset")
    if not rows or len({(r["ts_code"], r["trade_date"]) for r in rows}) != len(rows):
        raise ValueError("source keys must be nonempty and unique")
    if (len({r["trade_date"] for r in rows}) != 1 or len(source_sha256) != 64
            or any(c not in "0123456789abcdef" for c in source_sha256)):
        raise ValueError("source date/hash identity is invalid")
    if any(r["ts_code"] not in actual for r in rows):
        raise ValueError("source keys remain absent in repair readback")
    columns = tuple(rows[0])
    for row in rows:
        old = actual[row["ts_code"]]
        if old["ts_code"] != row["ts_code"] or old["trade_date"] != row["trade_date"]:
            raise ValueError("readback key differs from the source")
        if any(row[f] is not None and old[f] is None for f in columns):
            raise ValueError("source-backed fields remain empty in repair readback")
    metadata = {"schema_version": "local_data_source_repair_readback_v1",
                "source_sha256": source_sha256, "source_rowset_sha256": digest(rows),
                "readback_row_count": len(rows), "partial_raw_repair": False,
                "readback_rows_sha256": digest([actual[r["ts_code"]] for r in rows])}
    if table == "etf_share_size":
        quality, status = _etf_share_size_publication_quality(rows)
        if status["quality_status"] != "ok":
            raise ValueError("ETF source publication is still incomplete")
        metadata["publication_quality"] = quality
    else:
        metadata["provider_publication"] = {
            "source": "tushare_response", "row_count": len(rows),
            "source_rows_sha256": provider_rows_sha256 or digest(rows),
        }
    return metadata


def guarded_audit_statements(conn, table, columns, rows, actual, source_sha256, *, provider_rows_sha256=None):
    """Reuse the repository upsert, preceded by exact immutable-plan readback."""
    if table == "kline_daily_raw":
        return [], None
    if table not in {"etf_share_size", "margin_detail"}:
        raise ValueError("unsupported repair audit dataset")
    payload = json.dumps([actual[r["ts_code"]] for r in rows], default=str, allow_nan=False)
    # Normalize the projected values through the actual table types, so a
    # planned numeric value and its database readback have the same digest.
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as typed:
        typed.execute(f"SELECT {','.join(columns)} FROM "
                      f"jsonb_populate_recordset(NULL::market.{table},%s::jsonb)", (payload,))
        actual = {r["ts_code"]: dict(r) for r in typed.fetchall()}
    metadata = readback_audit_contract(table, rows, actual, source_sha256,
                                       provider_rows_sha256=provider_rows_sha256)
    if metadata is None:
        return [], None
    statements = []
    with conn.cursor() as cur:
        payload = json.dumps([actual[r["ts_code"]] for r in rows], default=str, allow_nan=False)
        left = ",".join(f"a.{field}" for field in columns)
        right = ",".join(f"e.{field}" for field in columns)
        guard = f"""DO $repair_readback$ BEGIN
            IF EXISTS (SELECT 1 FROM jsonb_populate_recordset(NULL::market.{table},%s::jsonb) e
              LEFT JOIN market.{table} a USING (ts_code,trade_date)
              WHERE ROW({left}) IS DISTINCT FROM ROW({right}))
            THEN RAISE EXCEPTION 'source repair readback changed: {table}'; END IF;
            END $repair_readback$;"""
        statements.append(cur.mogrify(guard, (payload,)).decode("utf-8"))

        class Collector:
            def cursor(self):
                return self

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, sql, args):
                statements.append(cur.mogrify(sql, args).decode("utf-8") + ";")

        DataRefreshAuditRepository().record_success(
            dataset=table, trade_date=rows[0]["trade_date"], row_count=len(rows),
            data_source="tushare", metadata=metadata, written_rows=0,
            expected_rows=len(rows), coverage_ratio=1, conn=Collector(),
        )
    return statements, metadata


def run_plan(env_file, target, source, *, apply=False, sql_output=None):
    if apply and target != "dev":
        raise ValueError("this tool does not authorize production writes")
    results = []
    statements = ["BEGIN;", "SET LOCAL lock_timeout='5s';", "SET LOCAL statement_timeout='120s';",
                  "DO $$ BEGIN IF current_database() <> 'aistock' THEN RAISE EXCEPTION 'wrong production database'; END IF; END $$;"]
    with closing(connect(env_file, target, readonly=not apply)) as conn:
        for table, columns, rows in source_groups(source):
            before = existing_rows(conn, table, columns, rows)
            patches, stats = plan_group(table, columns, rows, before)
            updates = ",".join(f"{field}=COALESCE({table}.{field},EXCLUDED.{field})" for field in columns if field not in {"trade_date", "ts_code"})
            conflict = "DO NOTHING" if table == "kline_daily_raw" else "DO UPDATE SET " + updates
            if table != "kline_daily_raw":
                missing = " OR ".join(f"({table}.{field} IS NULL AND EXCLUDED.{field} IS NOT NULL)"
                                      for field in columns if field not in {"trade_date", "ts_code"})
                conflict += " WHERE " + missing
            sql = f"INSERT INTO market.{table} ({','.join(columns)}) VALUES %s ON CONFLICT (ts_code,trade_date) {conflict}"
            if sql_output and patches:
                with conn.cursor() as cur:
                    template = "(" + ",".join(["%s"] * len(columns)) + ")"
                    literals = ",\n".join(cur.mogrify(template, [row[c] for c in columns]).decode("utf-8") for row in patches)
                    statements.append(sql.replace("VALUES %s", "VALUES " + literals) + ";")
            if apply and patches:
                with conn.cursor() as cur:
                    pgx.execute_values(cur, sql, [[row[c] for c in columns] for row in patches], page_size=1000)
                after = existing_rows(conn, table, columns, rows)
                remaining, remaining_stats = plan_group(table, columns, rows, after)
                if remaining:
                    raise ValueError("DEV repair readback still has fillable gaps")
                for code, old in before.items():
                    if any(value is not None and after[code][field] != value for field, value in old.items()):
                        raise ValueError("existing finite fact changed")
                stats.update(readback_fillable_rows=len(remaining), idempotent=True, preserved_facts=True)
            projected = {row["ts_code"]: {
                field: before.get(row["ts_code"], {}).get(field)
                if before.get(row["ts_code"], {}).get(field) is not None else row[field]
                for field in columns
            } for row in rows}
            if apply:
                projected = existing_rows(conn, table, columns, rows)
            audit_sql, audit = guarded_audit_statements(
                conn, table, columns, rows, projected, digest(source),
                provider_rows_sha256=next((b["rows_sha256"] for b in source["batches"]
                    if b["api"] == table and b["request_date"] == rows[0]["trade_date"].strftime("%Y%m%d")), None),
            )
            if sql_output:
                statements.extend(audit_sql)
            if apply and audit_sql:
                with conn.cursor() as cur:
                    for statement in audit_sql:
                        cur.execute(statement)
            results.append({"table": table, "date": rows[0]["trade_date"].isoformat(),
                            "source_rows": len(rows), "stats": stats,
                            "patch_rows_sha256": digest(patches),
                            "refresh_audit": audit,
                            "refresh_audit_written": bool(apply and audit),
                            "partial_raw_audit_skipped": table == "kline_daily_raw"})
        if apply:
            conn.commit()
        else:
            conn.rollback()
    if sql_output:
        statements.append("COMMIT;")
        with sql_output.open("xb") as handle:
            handle.write(("-- BUG-1893; source SHA256 " + digest(source) + "\n" + "\n".join(statements) + "\n").encode("utf-8"))
    return {"schema_version": "local_data_daily_sync_repair_receipt_v1", "target": target,
            "source_sha256": digest(source), "database_write": apply,
            "production_database_write": False, "results": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path("F:/Dev/AIstock/.env"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--apply-dev", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    if output.drive.upper() != "X:" or output.exists():
        raise ValueError("new exclusive X-drive output directory is required")
    output.mkdir(parents=True, exist_ok=False)
    source = json.loads(args.source.read_text(encoding="utf-8")) if args.source else capture(args.env_file)
    exclusive_json(output / "source.json", source)
    exclusive_json(output / "production-readonly-plan.json", run_plan(args.env_file, "production", source,
                   sql_output=output / "production-repair.sql"))
    dev = run_plan(args.env_file, "dev", source, apply=args.apply_dev)
    exclusive_json(output / "dev-receipt.json", dev)
    print(json.dumps(dev, sort_keys=True))


if __name__ == "__main__":
    main()
