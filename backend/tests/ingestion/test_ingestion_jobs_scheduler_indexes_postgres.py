from __future__ import annotations

import os
from pathlib import Path
import re
from typing import Any

from dotenv import load_dotenv
import psycopg2
import pytest


ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "backend" / "migrations" / "ingestion_jobs_scheduler_scan_indexes_20260816.sql"
EXPECTED_INDEXES = {
    "ix_ingestion_jobs_stale_running_started_at",
    "ix_ingestion_jobs_stale_queued_created_at",
    "ix_ingestion_jobs_recent_dataset_mode_created_at",
    "ix_ingestion_jobs_go_init_success_finished_at",
}


def _runtime_repo_root() -> Path:
    git_file = ROOT / ".git"
    if git_file.is_file():
        match = re.search(r"gitdir:\s*(.+)", git_file.read_text(encoding="utf-8"))
        if match:
            git_dir = Path(match.group(1).strip())
            if git_dir.parent.name == "worktrees":
                common_root = git_dir.parents[2]
                if (common_root / ".git").exists():
                    return common_root
    return ROOT


def _dev_dsn() -> dict[str, object]:
    if os.getenv("AISTOCK_RUN_BUG1106_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized BUG-1106 DEV PostgreSQL gate")
    load_dotenv(_runtime_repo_root() / ".env", override=False)
    dsn: dict[str, object] = {
        "host": os.getenv("TDX_DB_DEV_HOST"),
        "port": int(os.getenv("TDX_DB_DEV_PORT", "0")),
        "dbname": os.getenv("TDX_DB_DEV_NAME"),
        "user": os.getenv("TDX_DB_DEV_USER"),
        "password": os.getenv("TDX_DB_DEV_PASSWORD"),
        "connect_timeout": 5,
    }
    if dsn["host"] != "127.0.0.1" or dsn["port"] != 5433 or "dev" not in str(dsn["dbname"]).lower():
        raise AssertionError(f"refusing non-DEV BUG-1106 target {dsn['host']}:{dsn['port']}/{dsn['dbname']}")
    if not dsn["user"] or not dsn["password"]:
        pytest.fail("guarded DEV credentials are unavailable")
    return dsn


def _plan_index_names(plan: Any) -> set[str]:
    names: set[str] = set()
    if isinstance(plan, dict):
        index_name = plan.get("Index Name")
        if isinstance(index_name, str):
            names.add(index_name)
        for value in plan.values():
            names.update(_plan_index_names(value))
    elif isinstance(plan, list):
        for value in plan:
            names.update(_plan_index_names(value))
    return names


def _explain_index_names(cur, sql: str) -> set[str]:
    cur.execute("EXPLAIN (FORMAT JSON) " + sql)
    payload = cur.fetchone()[0]
    return _plan_index_names(payload)


def test_migration_is_idempotent_and_contains_no_business_dml() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    assert sql.count("CREATE INDEX IF NOT EXISTS") == 4
    assert "ANALYZE market.ingestion_jobs" in sql
    assert "INSERT " not in sql.upper()
    assert "UPDATE " not in sql.upper()
    assert "DELETE " not in sql.upper()
    assert "DROP " not in sql.upper()


def test_dev_postgres_uses_bounded_scheduler_index_paths() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(sql)
            cur.execute(
                """
                SELECT indexname,
                       obj_description(
                           to_regclass(format('%%I.%%I', schemaname, indexname)),
                           'pg_class'
                       )
                  FROM pg_indexes
                 WHERE schemaname = 'market'
                   AND tablename = 'ingestion_jobs'
                   AND indexname = ANY(%s)
                 ORDER BY indexname
                """,
                (sorted(EXPECTED_INDEXES),),
            )
            rows = cur.fetchall()
            assert {row[0] for row in rows} == EXPECTED_INDEXES
            assert all(str(row[1] or "").startswith("BUG-1106:") for row in rows)

            cur.execute("SET enable_seqscan = off")
            running_indexes = _explain_index_names(
                cur,
                """
                UPDATE market.ingestion_jobs
                   SET status = 'timeout'
                 WHERE status = 'running'
                   AND started_at IS NOT NULL
                   AND started_at < NOW() - INTERVAL '120 minutes'
                """,
            )
            queued_indexes = _explain_index_names(
                cur,
                """
                UPDATE market.ingestion_jobs
                   SET status = 'failed'
                 WHERE status IN ('queued', 'pending')
                   AND started_at IS NULL
                   AND COALESCE(summary->>'triggered_by', '') IN ('schedule', 'data_sync_target_due')
                   AND (
                       (COALESCE(summary->>'triggered_by', '') = 'schedule'
                        AND created_at < NOW() - INTERVAL '5 minutes')
                       OR
                       (COALESCE(summary->>'triggered_by', '') = 'data_sync_target_due'
                        AND created_at < NOW() - INTERVAL '120 minutes')
                   )
                """,
            )
            recent_indexes = _explain_index_names(
                cur,
                """
                SELECT job_id
                  FROM market.ingestion_jobs
                 WHERE lower(summary->>'dataset') = 'sector_data'
                   AND lower(COALESCE(summary->>'mode', '')) = 'incremental'
                   AND (
                       (
                           status IN ('queued', 'pending', 'running')
                           AND COALESCE(started_at, created_at) >= NOW() - INTERVAL '120 minutes'
                       )
                       OR
                       (
                           status = 'success'
                           AND created_at >= NOW() - INTERVAL '300 seconds'
                       )
                   )
                 ORDER BY created_at DESC
                 LIMIT 1
                """,
            )
            go_audit_indexes = _explain_index_names(
                cur,
                """
                SELECT job_id, summary
                  FROM market.ingestion_jobs
                 WHERE status = 'success'
                   AND finished_at >= NOW() - INTERVAL '3 days'
                   AND summary->>'via' = 'go_init'
                 ORDER BY finished_at DESC
                 LIMIT 50
                """,
            )

        assert "ix_ingestion_jobs_stale_running_started_at" in running_indexes
        assert "ix_ingestion_jobs_stale_queued_created_at" in queued_indexes
        assert "ix_ingestion_jobs_recent_dataset_mode_created_at" in recent_indexes
        assert "ix_ingestion_jobs_go_init_success_finished_at" in go_audit_indexes
    finally:
        conn.close()
