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


def _check_run(
    cur: Any,
    *,
    run_id: str,
    min_metrics: int,
    min_curves: int,
    min_factors: int,
    require_account_summary: bool,
) -> tuple[dict[str, Any], list[str], list[str]]:
    failures: list[str] = []
    warnings: list[str] = []

    cur.execute(
        """
        SELECT run_id, source_system, run_type, status, research_valid, invalid_reason,
               factor_count, freq, label_horizon, completed_at
        FROM qe_archive.run
        WHERE run_id = %s
        """,
        (run_id,),
    )
    run_row = cur.fetchone()
    if not run_row:
        return {"run_id": run_id, "exists": False}, [f"run_id not found: {run_id}"], []

    cur.execute(
        """
        SELECT config_capture_complete, jsonb_array_length(missing_config_items)
        FROM qe_archive.run_config
        WHERE run_id = %s
        """,
        (run_id,),
    )
    config_row = cur.fetchone()

    cur.execute(
        """
        SELECT reproducibility_level, verification_status, jsonb_array_length(missing_items)
        FROM qe_archive.run_reproducibility_manifest
        WHERE run_id = %s
        """,
        (run_id,),
    )
    manifest_row = cur.fetchone()

    count_tables = {
        "source_count": "run_source",
        "data_context_count": "run_data_context",
        "account_summary_count": "run_account_summary",
        "metric_count": "run_metric",
        "curve_count": "run_curve",
        "factor_count_rows": "run_factor",
        "symbol_summary_count": "run_symbol_summary",
        "trade_count": "run_trade",
        "execution_event_count": "run_execution_event",
        "artifact_count": "run_artifact",
        "raw_payload_count": "raw_payload",
        "priority_score_count": "run_priority_score",
    }
    counts: dict[str, int] = {}
    for key, table in count_tables.items():
        cur.execute(f"SELECT COUNT(*) FROM qe_archive.{table} WHERE run_id = %s", (run_id,))
        counts[key] = int(cur.fetchone()[0])

    run_checks = {
        "run_id": run_row[0],
        "exists": True,
        "source_system": run_row[1],
        "run_type": run_row[2],
        "status": run_row[3],
        "research_valid": run_row[4],
        "invalid_reason": run_row[5],
        "declared_factor_count": run_row[6],
        "freq": run_row[7],
        "label_horizon": run_row[8],
        "completed_at": run_row[9].isoformat() if run_row[9] else None,
        "config_capture_complete": config_row[0] if config_row else None,
        "missing_config_item_count": int(config_row[1]) if config_row else None,
        "reproducibility_level": manifest_row[0] if manifest_row else None,
        "manifest_verification_status": manifest_row[1] if manifest_row else None,
        "manifest_missing_item_count": int(manifest_row[2]) if manifest_row else None,
        **counts,
    }

    if config_row is None:
        failures.append(f"run_config missing for run_id: {run_id}")
    if manifest_row is None:
        failures.append(f"run_reproducibility_manifest missing for run_id: {run_id}")
    if counts["source_count"] < 1:
        failures.append(f"run_source missing for run_id: {run_id}")
    if counts["data_context_count"] < 1:
        failures.append(f"run_data_context missing for run_id: {run_id}")
    if counts["raw_payload_count"] < 1:
        failures.append(f"raw_payload missing for run_id: {run_id}")
    if counts["metric_count"] < min_metrics:
        failures.append(f"metric_count {counts['metric_count']} < required {min_metrics} for run_id: {run_id}")
    if counts["curve_count"] < min_curves:
        failures.append(f"curve_count {counts['curve_count']} < required {min_curves} for run_id: {run_id}")
    if counts["factor_count_rows"] < min_factors:
        failures.append(f"factor_count_rows {counts['factor_count_rows']} < required {min_factors} for run_id: {run_id}")
    if require_account_summary and counts["account_summary_count"] < 1:
        failures.append(f"run_account_summary missing for run_id: {run_id}")
    if config_row and config_row[0] is False:
        warnings.append(f"config_capture_complete=false for run_id: {run_id}")
    if manifest_row and manifest_row[0] != "full":
        warnings.append(f"reproducibility_level={manifest_row[0]} for run_id: {run_id}")

    return run_checks, failures, warnings


def _check_db(
    *,
    run_id: str | None = None,
    min_metrics: int = 0,
    min_curves: int = 0,
    min_factors: int = 0,
    require_account_summary: bool = False,
) -> dict[str, Any]:
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
            run_checks = None
            run_failures: list[str] = []
            run_warnings: list[str] = []
            if run_id:
                run_checks, run_failures, run_warnings = _check_run(
                    cur,
                    run_id=run_id,
                    min_metrics=min_metrics,
                    min_curves=min_curves,
                    min_factors=min_factors,
                    require_account_summary=require_account_summary,
                )

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
    if run_checks is not None:
        result["checks"]["run_detail"] = run_checks

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
    result["failures"].extend(run_failures)
    result["warnings"].extend(run_warnings)

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only QE archive data-quality smoke check.")
    parser.add_argument("--output", type=Path, default=None, help="Optional JSON output path.")
    parser.add_argument("--run-id", default=None, help="Optional archived run_id to validate in detail.")
    parser.add_argument("--min-metrics", type=int, default=0, help="Minimum run_metric rows when --run-id is set.")
    parser.add_argument("--min-curves", type=int, default=0, help="Minimum run_curve rows when --run-id is set.")
    parser.add_argument("--min-factors", type=int, default=0, help="Minimum run_factor rows when --run-id is set.")
    parser.add_argument(
        "--require-account-summary",
        action="store_true",
        help="Require qe_archive.run_account_summary for --run-id.",
    )
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env", override=True)
    result = _check_db(
        run_id=args.run_id,
        min_metrics=args.min_metrics,
        min_curves=args.min_curves,
        min_factors=args.min_factors,
        require_account_summary=args.require_account_summary,
    )

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
