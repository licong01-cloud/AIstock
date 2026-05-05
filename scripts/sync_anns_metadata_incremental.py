"""Incrementally sync announcement metadata into market.anns.

Default mode is a rolling two-natural-day sync for hourly scheduling.  The
script intentionally syncs metadata only; announcement PDF downloads remain a
separate anns_pdf task.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv

import sync_cninfo_anns_metadata as cninfo
import sync_eastmoney_anns_metadata as eastmoney


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    application_name="AIstock-anns-metadata-incremental-sync",
)

CN_TZ = dt.timezone(dt.timedelta(hours=8))
pgx.register_uuid()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync announcement metadata to market.anns")
    parser.add_argument("--mode", choices=["init", "incremental"], default="incremental")
    parser.add_argument("--start-date", default=None, help="Start ann_date YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End ann_date YYYY-MM-DD")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=2,
        help="Natural-day rolling window for incremental mode when dates are omitted",
    )
    parser.add_argument(
        "--source",
        choices=["eastmoney", "cninfo", "both"],
        default="eastmoney",
        help="Metadata source. Default eastmoney is preferred for hourly sync.",
    )
    parser.add_argument("--workers", type=int, default=1, help="Concurrent date workers")
    parser.add_argument("--request-sleep", type=float, default=0.05, help="Sleep seconds between page requests")
    parser.add_argument("--max-retries", type=int, default=5, help="HTTP retries per page")
    parser.add_argument("--job-id", default=None, help="Existing ingestion_jobs.job_id to update")
    parser.add_argument(
        "--audit-jsonl",
        default=None,
        help="Path for per-source/date audit JSONL; defaults to reports/anns/anns_metadata_sync_<ts>.jsonl",
    )
    parser.add_argument(
        "--bulk-session-tune",
        action="store_true",
        help="Apply session-level tuning in worker DB connections",
    )
    return parser.parse_args()


def parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date: {value}") from exc


def date_range(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cur = start
    one = dt.timedelta(days=1)
    while cur <= end:
        yield cur
        cur += one


def resolve_date_range(args: argparse.Namespace) -> tuple[dt.date, dt.date]:
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    today = dt.datetime.now(CN_TZ).date()

    if args.lookback_days < 1 or args.lookback_days > 30:
        raise ValueError("--lookback-days must be between 1 and 30")

    if args.mode == "init":
        if start is None or end is None:
            raise ValueError("--start-date and --end-date are required in init mode")
    else:
        if start is None and end is None:
            end = today
            start = end - dt.timedelta(days=args.lookback_days - 1)
        elif start is None and end is not None:
            start = end - dt.timedelta(days=args.lookback_days - 1)
        elif start is not None and end is None:
            end = today

    if start is None or end is None:
        raise ValueError("failed to resolve sync date range")
    if start > end:
        raise ValueError("--start-date must be <= --end-date")
    return start, end


def audit_path_from_args(value: Optional[str]) -> Path:
    if value:
        return Path(value)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return ROOT / "reports" / "anns" / f"anns_metadata_sync_{ts}.jsonl"


def sources_from_arg(value: str) -> List[str]:
    source = (value or "eastmoney").strip().lower()
    if source == "both":
        return ["eastmoney", "cninfo"]
    return [source]


def _json_dump(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _create_job(conn: psycopg2.extensions.connection, job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
    job_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_jobs (job_id, job_type, status, created_at, started_at, summary)
            VALUES (%s, %s, 'running', NOW(), NOW(), %s)
            """,
            (job_id, job_type, _json_dump(summary)),
        )
    return job_id


def _start_existing_job(conn: psycopg2.extensions.connection, job_id: uuid.UUID, summary: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status = 'running',
                   started_at = COALESCE(started_at, NOW()),
                   summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id = %s
            """,
            (_json_dump(summary), job_id),
        )


def _update_job_summary(conn: psycopg2.extensions.connection, job_id: uuid.UUID, patch: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id = %s
            """,
            (_json_dump(patch), job_id),
        )


def _finish_job(
    conn: psycopg2.extensions.connection,
    job_id: uuid.UUID,
    status: str,
    summary: Dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status = %s,
                   finished_at = NOW(),
                   summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id = %s
            """,
            (status, _json_dump(summary), job_id),
        )


def _log(conn: psycopg2.extensions.connection, job_id: uuid.UUID, level: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES (%s, NOW(), %s, %s)",
            (job_id, level.upper(), message),
        )


def _empty_source_stats() -> Dict[str, int]:
    return {
        "success_days": 0,
        "failed_days": 0,
        "zero_days": 0,
        "source_total": 0,
        "raw_documents": 0,
        "unique_count": 0,
        "upsert_touched": 0,
    }


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _sync_one_source(
    source: str,
    dates: List[dt.date],
    args: argparse.Namespace,
    audit_file,
    job_conn: psycopg2.extensions.connection,
    job_id: uuid.UUID,
    total_batches: int,
    done_before: int,
    failed_before: int,
    upsert_before: int,
) -> tuple[Dict[str, int], int]:
    if source == "eastmoney":
        sync_func = eastmoney.sync_one_date
        raw_keys = ("raw_documents",)
    elif source == "cninfo":
        sync_func = cninfo.sync_one_date
        raw_keys = ("raw_count", "raw_documents")
    else:
        raise ValueError(f"unsupported source: {source}")

    stats = _empty_source_stats()
    done = done_before

    futures: Dict[Future[Dict[str, Any]], dt.date] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for ann_date in dates:
            futures[
                executor.submit(
                    sync_func,
                    ann_date,
                    args.request_sleep,
                    args.max_retries,
                    args.bulk_session_tune,
                )
            ] = ann_date

        for future in as_completed(futures):
            ann_date = futures[future]
            try:
                result = future.result()
            except Exception as exc:  # noqa: BLE001
                result = {
                    "ann_date": ann_date.isoformat(),
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }

            result["source"] = source
            audit_file.write(_json_dump(result) + "\n")
            audit_file.flush()

            done += 1
            if result.get("status") == "success":
                stats["success_days"] += 1
                if _int_value(result.get("source_total")) == 0:
                    stats["zero_days"] += 1
                stats["source_total"] += _int_value(result.get("source_total"))
                stats["unique_count"] += _int_value(result.get("unique_count"))
                stats["upsert_touched"] += _int_value(result.get("upsert_touched"))
                stats["raw_documents"] += max(_int_value(result.get(k)) for k in raw_keys)
            else:
                stats["failed_days"] += 1
                _log(
                    job_conn,
                    job_id,
                    "error",
                    f"anns_metadata {source} {result.get('ann_date')} failed: {result.get('error')}",
                )

            progress = 0.0 if total_batches <= 0 else min(100.0, 100.0 * done / total_batches)
            counters = {
                "total": total_batches,
                "done": done,
                "running": 0,
                "pending": max(total_batches - done, 0),
                "failed": failed_before + stats["failed_days"],
                "success": done - failed_before - stats["failed_days"],
                "inserted_rows": upsert_before + stats["upsert_touched"],
            }
            _update_job_summary(job_conn, job_id, {"counters": counters, "progress": progress})

            print(
                json.dumps(
                    {
                        "event": "date_done",
                        "source": source,
                        "ann_date": result.get("ann_date"),
                        "status": result.get("status"),
                        "source_total": result.get("source_total"),
                        "unique_count": result.get("unique_count"),
                        "upsert_touched": result.get("upsert_touched"),
                        "done": done,
                        "total": total_batches,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    return stats, done


def run() -> int:
    args = parse_args()
    if args.workers <= 0 or args.workers > 8:
        print("[ERROR] --workers must be between 1 and 8", file=sys.stderr)
        return 2

    try:
        start, end = resolve_date_range(args)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    dates = list(date_range(start, end))
    sources = sources_from_arg(args.source)
    audit_path = audit_path_from_args(args.audit_jsonl)
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    initial_summary = {
        "dataset": "anns_metadata",
        "mode": args.mode,
        "source": args.source,
        "sources": sources,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "lookback_days": args.lookback_days,
        "workers": args.workers,
        "request_sleep": args.request_sleep,
        "audit_jsonl": str(audit_path),
    }

    job_conn = psycopg2.connect(**DB_CFG)
    job_conn.autocommit = True
    try:
        if args.job_id:
            job_id = uuid.UUID(str(args.job_id))
            _start_existing_job(job_conn, job_id, initial_summary)
        else:
            job_id = _create_job(job_conn, args.mode, initial_summary)

        _log(
            job_conn,
            job_id,
            "info",
            f"start anns_metadata sync source={args.source} {start.isoformat()} -> {end.isoformat()}",
        )

        print(
            json.dumps(
                {
                    "event": "start",
                    **initial_summary,
                    "dates": len(dates),
                    "job_id": str(job_id),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

        started = time.time()
        total_batches = len(dates) * len(sources)
        done = 0
        source_stats: Dict[str, Dict[str, int]] = {}
        total_stats = _empty_source_stats()

        with audit_path.open("a", encoding="utf-8") as audit_file:
            for source in sources:
                stats, done = _sync_one_source(
                    source,
                    dates,
                    args,
                    audit_file,
                    job_conn,
                    job_id,
                    total_batches,
                    done,
                    total_stats["failed_days"],
                    total_stats["upsert_touched"],
                )
                source_stats[source] = stats
                for key, value in stats.items():
                    total_stats[key] += value

        elapsed = time.time() - started
        counters = {
            "total": total_batches,
            "done": done,
            "running": 0,
            "pending": 0,
            "failed": total_stats["failed_days"],
            "success": total_batches - total_stats["failed_days"],
            "inserted_rows": total_stats["upsert_touched"],
        }
        final_summary = {
            **initial_summary,
            "elapsed_sec": round(elapsed, 3),
            "dates": len(dates),
            "source_stats": source_stats,
            "stats": total_stats,
            "inserted_rows": total_stats["upsert_touched"],
            "upsert_touched": total_stats["upsert_touched"],
            "counters": counters,
            "progress": 100.0,
        }
        status = "success" if total_stats["failed_days"] == 0 else "failed"
        _finish_job(job_conn, job_id, status, final_summary)
        _log(job_conn, job_id, "info" if status == "success" else "error", f"done anns_metadata status={status}")

        print(json.dumps({"event": "done", "job_id": str(job_id), "status": status, **final_summary}, ensure_ascii=False), flush=True)
        return 0 if status == "success" else 1
    except Exception as exc:  # noqa: BLE001
        try:
            if "job_id" in locals():
                _finish_job(job_conn, job_id, "failed", {"error": str(exc)})
                _log(job_conn, job_id, "error", f"anns_metadata fatal error: {exc}")
        finally:
            print(f"[ERROR] anns_metadata failed: {exc}", file=sys.stderr)
        return 1
    finally:
        job_conn.close()


if __name__ == "__main__":
    raise SystemExit(run())
