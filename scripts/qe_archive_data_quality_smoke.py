from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from backend.db.init_qe_archive_schema import (
    QE_ARCHIVE_SCHEMA_VERSION,
    iter_qe_archive_columns,
    iter_qe_archive_tables,
)
from backend.db.pg_pool import get_conn


def _check_db() -> dict[str, Any]:
    expected_tables = set(iter_qe_archive_tables())
    expected_columns = set(iter_qe_archive_columns())
    result: dict[str, Any] = {
        "schema_version": QE_ARCHIVE_SCHEMA_VERSION,
        "checks": {},
        "failures": [],
        "warnings": [],
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT n.nspname || '.' || c.relname AS table_name
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'qe_archive'
                  AND c.relkind IN ('r','p')
                """
            )
            existing_tables = {row[0] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT version
                FROM qe_archive.schema_version
                WHERE version = %s
                """,
                (QE_ARCHIVE_SCHEMA_VERSION,),
            )
            schema_version_row = cur.fetchone()

            cur.execute(
                """
                SELECT n.nspname || '.' || c.relname AS table_name
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
                WHERE n.nspname = 'qe_archive'
                  AND c.relkind IN ('r','p')
                  AND d.description IS NOT NULL
                  AND d.description <> ''
                """
            )
            commented_tables = {row[0] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT n.nspname || '.' || c.relname AS table_name, a.attname AS column_name
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
                WHERE n.nspname = 'qe_archive'
                  AND c.relkind IN ('r','p')
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                  AND d.description IS NOT NULL
                  AND d.description <> ''
                """
            )
            commented_columns = set(cur.fetchall())

            cur.execute("SELECT COUNT(*) FROM qe_archive.run")
            run_count = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM qe_archive.outbox_event WHERE status = 'pending'")
            pending_outbox_count = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT status, COUNT(*)
                FROM qe_archive.archive_job
                GROUP BY status
                ORDER BY status
                """
            )
            archive_job_status_counts = {str(status): int(count) for status, count in cur.fetchall()}

    missing_tables = sorted(expected_tables - existing_tables)
    missing_table_comments = sorted(expected_tables - commented_tables)
    missing_column_comments = sorted(expected_columns - commented_columns)

    result["checks"] = {
        "expected_table_count": len(expected_tables),
        "existing_table_count": len(expected_tables & existing_tables),
        "missing_tables": missing_tables,
        "schema_version_present": bool(schema_version_row),
        "expected_column_count": len(expected_columns),
        "commented_table_count": len(expected_tables & commented_tables),
        "missing_table_comments": missing_table_comments,
        "commented_column_count": len(expected_columns & commented_columns),
        "missing_column_comments": [
            {"table": table, "column": column}
            for table, column in missing_column_comments
        ],
        "run_count": run_count,
        "pending_outbox_count": pending_outbox_count,
        "archive_job_status_counts": archive_job_status_counts,
    }

    if missing_tables:
        result["failures"].append(f"missing qe_archive tables: {missing_tables}")
    if not schema_version_row:
        result["failures"].append(f"missing schema version: {QE_ARCHIVE_SCHEMA_VERSION}")
    if missing_table_comments:
        result["failures"].append(f"missing table comments: {missing_table_comments}")
    if missing_column_comments:
        result["failures"].append(f"missing column comments: {missing_column_comments}")
    if pending_outbox_count > 0:
        result["warnings"].append(
            f"pending outbox events exist: {pending_outbox_count}; this is informational for read-only smoke"
        )

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only QE archive data-quality smoke check.")
    parser.add_argument("--output", type=Path, default=None, help="Optional JSON output path.")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env", override=True)
    result = _check_db()

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
