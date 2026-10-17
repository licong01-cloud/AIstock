"""Read-only, exact-key disclosure/database verification; never repairs prices."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests
import psycopg2.extras as pgx

from backend.data_service.suspension_day_authority import (
    canonical_bytes, digest, load_authority, project_suspension_days,
)
from scripts.repair_sw_sector_revision import connect


def _write_new(path, value):
    data = canonical_bytes(value) + b"\n"
    with path.open("xb") as stream:
        stream.write(data)
    return {"path": str(path), "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}


def _download(entry, root):
    response = requests.get(entry["source_url"], timeout=(15, 60), headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    data = response.content
    actual = hashlib.sha256(data).hexdigest()
    if actual != entry["source_content_sha256"] or len(data) != entry["source_file_size"]:
        raise ValueError(f"disclosure bytes drifted: {entry['ts_code']}/{entry['trade_date']}")
    path = root / actual
    with path.open("xb") as stream:
        stream.write(data)
    return {"symbol": entry["ts_code"], "trade_date": entry["trade_date"], "url": entry["source_url"],
            "announcement_id": entry["announcement_id"], "path": str(path), "sha256": actual,
            "size": len(data), "historical_publication_time_proven": False}


def _read_database(env_file, target, entries, *, dev_rows=()):
    if dev_rows and target != "dev":
        raise ValueError("transactional validation is DEV-only")
    conn = connect(env_file, target, readonly=not bool(dev_rows))
    raw, prices = [], []
    try:
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout=30000")
            cur.execute("SET LOCAL application_name='codex_bug1942_suspension_verification'")
            if dev_rows:
                pgx.execute_values(cur, "INSERT INTO market.suspend_d(ts_code,trade_date,suspend_type,suspend_timing) "
                                   "VALUES %s ON CONFLICT DO NOTHING",
                                   [(row["ts_code"], row["trade_date"], row["suspend_type"], row["suspend_timing"])
                                    for row in dev_rows])
            for entry in entries:
                key = (entry["ts_code"], entry["trade_date"])
                cur.execute("SELECT ts_code,trade_date,suspend_type,suspend_timing FROM market.suspend_d "
                            "WHERE ts_code=%s AND trade_date=%s ORDER BY suspend_type", key)
                raw.extend(dict(row, trade_date=row["trade_date"].isoformat()) for row in cur.fetchall())
                cur.execute("SELECT count(*)::int AS daily_rows, "
                            "count(*) FILTER (WHERE volume_hand>0 OR amount_li>0)::int AS positive_daily_rows "
                            "FROM market.kline_daily_raw WHERE ts_code=%s AND trade_date=%s", key)
                daily = dict(cur.fetchone())
                cur.execute("SELECT count(*)::int AS minute_rows, "
                            "count(*) FILTER (WHERE volume_hand>0 OR amount_li>0)::int AS positive_minute_rows "
                            "FROM market.kline_minute_raw WHERE ts_code=%s AND trade_time >= %s::date "
                            "AND trade_time < %s::date + interval '1 day'", (*key, key[1]))
                prices.append({"ts_code": key[0], "trade_date": key[1], **daily, **dict(cur.fetchone())})
        states = project_suspension_days(raw)
        resolved = {(row["ts_code"], row["trade_date"]): row["state"] for row in states}
        passed = all(resolved.get((row["ts_code"], row["trade_date"])) == "FULL_DAY_SUSPENDED"
                     and row["positive_daily_rows"] == row["positive_minute_rows"] == 0 for row in prices)
        return {"target": target, "raw_events": raw, "raw_events_sha256": digest(raw),
                "resolutions": states, "price_readback": prices, "passed": passed,
                "read_only": not bool(dev_rows), "dev_transactional_validation_rows": len(dev_rows),
                "transaction_rolled_back": True, "committed_database_write": False}
    finally:
        conn.rollback()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path("F:/Dev/AIstock/.env"))
    args = parser.parse_args()
    root = args.output.resolve()
    if root.drive.upper() != "X:" or root.exists():
        raise ValueError("a new X-drive evidence directory is required")
    root.mkdir(parents=True, exist_ok=False)
    authority = load_authority()
    _write_new(root / "authority.json", authority)
    receipt = {"schema_version": "aistock_suspension_authority_verification_v1",
               "captured_at": datetime.now(timezone.utc).isoformat(), "authority_sha256": digest(authority),
               "outcomes_read": False, "database_write": False, "candidate_write": False,
               "active_profile_write": False, "runtime_action": False, "replay_write": False,
               "historical_publication_time_proven": False, "ready": False}
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            receipt["sources"] = list(pool.map(lambda entry: _download(entry, root), authority["entries"]))
        production = _read_database(args.env_file, "production", authority["entries"])
        dev_before = _read_database(args.env_file, "dev", authority["entries"])
        dev = _read_database(args.env_file, "dev", authority["entries"], dev_rows=production["raw_events"])
        dev_after = _read_database(args.env_file, "dev", authority["entries"])
        dev["rollback_verified"] = dev_before["raw_events_sha256"] == dev_after["raw_events_sha256"]
        dev["before_raw_events_sha256"] = dev_before["raw_events_sha256"]
        dev["after_raw_events_sha256"] = dev_after["raw_events_sha256"]
        receipt["databases"] = [dev, production]
        receipt["ready"] = dev["rollback_verified"] and all(row["passed"] for row in receipt["databases"])
    except Exception as exc:
        receipt["error_type"] = type(exc).__name__
        # Never echo connection/credential-bearing exception messages.
    ref = _write_new(root / "verification.json", receipt)
    print(json.dumps({"receipt": ref, "ready": receipt["ready"], "error_type": receipt.get("error_type")}, ensure_ascii=False))
    return 0 if receipt["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
