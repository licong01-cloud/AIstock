"""Read-only ETF deployment preflight; apply is restricted to existing DEV.

Production must use the separately authorized, reviewed migration and exact
schedule plan returned here. Never initialize the entire schedule catalog.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from backend.db.init_tushare_schedules import get_default_schedule_catalog
from backend.db.pg_pool import _db_cfg

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "backend/db/migrations/add_etf_share_sources_20260923.sql"
DATASETS = ("etf_share_size", "etf_basic_snapshots")
SCHEDULE_SQL = """
INSERT INTO market.ingestion_schedules
    (schedule_id, dataset, mode, frequency, enabled, options, created_at, updated_at)
VALUES (gen_random_uuid(), %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
ON CONFLICT (dataset, mode) DO NOTHING
"""


def schedule_plan():
    catalog = get_default_schedule_catalog()
    if not catalog["complete"]:
        raise ValueError("canonical schedule catalog invalid")
    return [entry for entry in catalog["templates"] if entry["dataset"] in DATASETS]


def preflight(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        database = cur.fetchone()[0]
        cur.execute("SELECT table_name, column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema='market' AND table_name=ANY(%s)", (list(DATASETS),))
        columns = {}
        for table, column, data_type in cur.fetchall():
            columns.setdefault(table, {})[column] = data_type
        cur.execute("SELECT hypertable_name FROM timescaledb_information.hypertables "
                    "WHERE hypertable_schema='market' AND hypertable_name=ANY(%s)", (list(DATASETS),))
        hypertables = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT data_kind, table_name, enabled FROM market.data_stats_config "
                    "WHERE data_kind=ANY(%s)", (list(DATASETS),))
        stats = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
        cur.execute("SELECT dataset, mode, enabled FROM market.ingestion_schedules "
                    "WHERE dataset=ANY(%s)", (list(DATASETS),))
        schedules = cur.fetchall()
    expected = {
        "etf_share_size": {"trade_date": "date", "ts_code": "text", **dict.fromkeys(
            ("total_share", "total_size", "nav", "close"), "numeric")},
        "etf_basic_snapshots": {"snapshot_date": "date", "ts_code": "text"},
    }
    issues = []
    for dataset in DATASETS:
        if dataset not in columns:
            issues.append(f"{dataset}:deployment_table_missing")
        elif any(columns[dataset].get(k) != v for k, v in expected[dataset].items()):
            issues.append(f"{dataset}:deployment_schema_mismatch")
        if dataset not in hypertables:
            issues.append(f"{dataset}:hypertable_missing")
        if stats.get(dataset) != (f"market.{dataset}", True):
            issues.append(f"{dataset}:stats_registration_missing")
    for entry in schedule_plan():
        if (entry["dataset"], entry["mode"], True) not in schedules:
            issues.append(f"{entry['dataset']}:enabled_schedule_missing")
    return {"schema_version": "etf_source_deployment_readback_v1", "database": database,
            "complete": not issues, "issues": issues, "columns": columns,
            "hypertables": sorted(hypertables), "schedules": schedules,
            "migration": str(MIGRATION), "migration_sha256": hashlib.sha256(MIGRATION.read_bytes()).hexdigest(),
            "schedule_sql": SCHEDULE_SQL, "schedule_plan": schedule_plan(),
            "production_write": False, "runtime_action": False}


def apply_dev(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        if cur.fetchone()[0] != "aistock_dev":
            raise ValueError("DEV apply requires existing aistock_dev; production is separately authorized")
        cur.execute(MIGRATION.read_text(encoding="utf-8-sig"))
        for entry in schedule_plan():
            cur.execute(SCHEDULE_SQL, (entry["dataset"], entry["mode"], entry["frequency"],
                                      entry["enabled"], json.dumps(entry["options"])))
    conn.commit()
    result = preflight(conn)
    result["dev_write"] = True
    if not result["complete"]:
        raise RuntimeError(f"DEV deployment readback incomplete: {result['issues']}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, choices=("aistock", "aistock_dev"))
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--apply-dev", action="store_true")
    args = parser.parse_args()
    if args.apply_dev and args.database != "aistock_dev":
        parser.error("production writes forbidden by this command")
    load_dotenv(args.env_file, override=False)
    cfg = _db_cfg()
    if args.database == "aistock_dev":
        cfg = {key: os.environ[f"TDX_DB_DEV_{suffix}"] for key, suffix in (
            ("host", "HOST"), ("port", "PORT"), ("user", "USER"),
            ("password", "PASSWORD"), ("dbname", "NAME"))}
        if cfg["dbname"] != "aistock_dev":
            raise ValueError("configured DEV database differs from required target")
    with psycopg2.connect(**cfg, connect_timeout=5) as conn:
        if not args.apply_dev:
            conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout='60s'")
        result = apply_dev(conn) if args.apply_dev else preflight(conn)
        print(json.dumps(result, ensure_ascii=False, default=str))
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
