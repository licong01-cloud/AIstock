"""Repair today's raw one-minute bars with an idempotent full-day upsert.

The command is intentionally limited to the current China trading date. It
loads an explicit DEV or production database profile, rewrites every expected
non-suspended symbol through the table's unique key, and publishes readiness
only after the exact 240/241-bar session check passes for the whole universe.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

import psycopg2
import requests
from dotenv import load_dotenv
from requests import exceptions as request_errors


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.data_refresh_audit import DataRefreshAuditRepository  # noqa: E402
from scripts import ingest_incremental as incremental  # noqa: E402


CHINA_TZ = ZoneInfo("Asia/Shanghai")
DATASET = "kline_minute_raw"
DATA_SOURCE = "tdx_api"
EXPECTED_BARS = 240
DB_ENV_KEYS = ("HOST", "PORT", "NAME", "USER", "PASSWORD")


class RepairError(RuntimeError):
    """Raised when the bounded repair cannot prove a safe result."""


@dataclass(frozen=True)
class HistoricalSnapshot:
    trade_date: str | None
    row_count: int
    open_sum: int
    high_sum: int
    low_sum: int
    close_sum: int
    volume_sum: int
    amount_sum: int


def china_today() -> dt.date:
    return dt.datetime.now(CHINA_TZ).date()


def _discover_env_file(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.is_file():
            raise RepairError(f"environment file does not exist: {path}")
        return path

    local = REPO_ROOT / ".env"
    if local.is_file():
        return local

    try:
        common_dir = subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "--path-format=absolute", "--git-common-dir"],
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
        ).strip()
    except (OSError, subprocess.CalledProcessError, UnicodeError) as exc:
        raise RepairError("cannot discover the canonical repository environment file") from exc
    candidate = Path(common_dir).resolve().parent / ".env"
    if not candidate.is_file():
        raise RepairError(f"canonical repository environment file does not exist: {candidate}")
    return candidate


def _database_config(target_db: str, env_file: Path) -> dict[str, Any]:
    load_dotenv(env_file, override=True)
    prefix = "TDX_DB_DEV_" if target_db == "dev" else "TDX_DB_"
    missing = [f"{prefix}{suffix}" for suffix in DB_ENV_KEYS if not os.getenv(f"{prefix}{suffix}")]
    if missing:
        raise RepairError("database environment is incomplete: " + ", ".join(missing))

    config = {
        "host": os.environ[f"{prefix}HOST"],
        "port": int(os.environ[f"{prefix}PORT"]),
        "dbname": os.environ[f"{prefix}NAME"],
        "user": os.environ[f"{prefix}USER"],
        "password": os.environ[f"{prefix}PASSWORD"],
        "connect_timeout": 10,
        "application_name": f"AIstock-repair-today-minute-{target_db}",
    }
    if target_db == "dev":
        host = str(config["host"]).lower()
        if host not in {"127.0.0.1", "localhost"} or config["port"] != 5433 or "dev" not in str(config["dbname"]).lower():
            raise RepairError(
                "refusing DEV repair because TDX_DB_DEV_* is not the existing local DEV database "
                f"(host={config['host']}, port={config['port']}, dbname={config['dbname']!r})"
            )
    return config


def _production_confirmation(target_date: dt.date, dbname: str) -> str:
    return f"UPSERT_{DATASET}_{target_date.isoformat()}_{dbname}"


def _parse_trade_time(target_date: dt.date, raw: Any) -> dt.datetime:
    text = str(raw or "").strip()
    if not text:
        raise RepairError("minute row has no trade time")

    parsed: dt.datetime | None = None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        for fmt in ("%H:%M:%S", "%H:%M", "%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S"):
            try:
                value = dt.datetime.strptime(text, fmt)
            except ValueError:
                continue
            parsed = value if "%Y" in fmt else dt.datetime.combine(target_date, value.time())
            break
    if parsed is None:
        raise RepairError(f"minute row has invalid trade time: {text!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CHINA_TZ)
    else:
        parsed = parsed.astimezone(CHINA_TZ)
    if parsed.date() != target_date:
        raise RepairError(
            f"source returned a row outside the authorized date: expected={target_date}, actual={parsed.date()}"
        )
    return parsed


def _source_row_date(raw: Any) -> dt.date:
    text = str(raw or "").strip()
    if not text:
        raise RepairError("minute source row has no timestamp")
    parsed: dt.datetime | None = None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = dt.datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        raise RepairError(f"minute source row has invalid timestamp: {text!r}")
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(CHINA_TZ)
    return parsed.date()


def normalize_target_bars(target_date: dt.date, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise RepairError("minute source returned a non-object row")
        raw_time = row.get("TradeTime") or row.get("trade_time") or row.get("Time") or row.get("time")
        parsed = _parse_trade_time(target_date, raw_time)
        item = dict(row)
        item["TradeTime"] = parsed.isoformat()
        normalized.append(item)
    return normalized


def _http_get(api_base: str, path: str, params: Mapping[str, Any]) -> dict[str, Any]:
    url = api_base.rstrip("/") + path
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            response = requests.get(url, params=dict(params), timeout=20)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RepairError(f"TDX API returned an invalid payload for {path}")
            if payload.get("code") not in (0, None):
                last_error = RepairError(f"TDX API returned an error for {path}")
                if attempt < 3:
                    time.sleep(1 + attempt)
                    continue
                break
            return payload
        except (request_errors.ConnectionError, request_errors.Timeout) as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(1 + attempt)
                continue
            break
    raise RepairError(f"TDX API request failed after retries: {path}") from last_error


def _is_a_share_code(code: str, exchange: str) -> bool:
    if len(code) != 6 or not code.isdigit():
        return False
    if exchange == "sz":
        return code.startswith(("000", "001", "002", "003", "30"))
    if exchange == "sh":
        return code.startswith("6")
    if exchange == "bj":
        return code.startswith(("43", "8", "92"))
    return False


def fetch_current_stock_codes(api_base: str) -> list[str]:
    payload = _http_get(api_base, "/api/codes", {"exchange": "all"})
    data = payload.get("data")
    rows = data.get("codes") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise RepairError("TDX stock directory has no code list")

    codes: set[str] = set()
    invalid_rows: list[int] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            invalid_rows.append(index)
            continue
        code = str(row.get("code") or "").strip()
        exchange = str(row.get("exchange") or "").strip().lower()
        if _is_a_share_code(code, exchange):
            codes.add(f"{code}.{exchange.upper()}")
    if invalid_rows:
        raise RepairError(f"TDX stock directory contains invalid rows: {invalid_rows[:10]}")
    if not codes:
        raise RepairError("TDX stock directory contains no A-share codes")
    return sorted(codes)


def fetch_minute_single_day(api_base: str, ts_code: str, target_date: dt.date) -> list[dict[str, Any]]:
    code = ts_code.split(".", 1)[0]
    payload = _http_get(
        api_base,
        "/api/kline-all/tdx",
        {"code": code, "type": "minute1"},
    )
    data = payload.get("data")
    if isinstance(data, dict):
        if "List" in data:
            rows: Any = data["List"]
        elif "list" in data:
            rows = data["list"]
        else:
            raise RepairError(f"TDX minute payload has no list for {ts_code}")
        if rows is None and int(data.get("count") or 0) == 0:
            rows = []
    elif isinstance(data, list):
        rows = data
    else:
        raise RepairError(f"TDX minute payload has invalid data for {ts_code}")
    if not isinstance(rows, list):
        raise RepairError(f"TDX minute payload is not a list for {ts_code}")
    selected: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise RepairError(f"TDX minute payload contains a non-object row for {ts_code}")
        raw_time = row.get("TradeTime") or row.get("trade_time") or row.get("Time") or row.get("time")
        if _source_row_date(raw_time) == target_date:
            selected.append(row)
    return normalize_target_bars(target_date, selected)


def _all_stock_codes(conn: Any) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT ts_code FROM market.stock_basic WHERE ts_code IS NOT NULL ORDER BY ts_code")
        return [str(row[0]) for row in cur.fetchall()]


def _latest_historical_data_date(conn: Any, target_date: dt.date) -> dt.date | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX((trade_time AT TIME ZONE 'Asia/Shanghai')::date)
              FROM market.kline_minute_raw
             WHERE trade_time < (%s::date + TIME '00:00') AT TIME ZONE 'Asia/Shanghai'
            """,
            (target_date,),
        )
        row = cur.fetchone()
    return row[0] if row and row[0] else None


def historical_snapshot(conn: Any, target_date: dt.date) -> HistoricalSnapshot:
    historical_date = _latest_historical_data_date(conn, target_date)
    if historical_date is None:
        return HistoricalSnapshot(None, 0, 0, 0, 0, 0, 0, 0)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::bigint,
                   COALESCE(SUM(open_li::numeric), 0),
                   COALESCE(SUM(high_li::numeric), 0),
                   COALESCE(SUM(low_li::numeric), 0),
                   COALESCE(SUM(close_li::numeric), 0),
                   COALESCE(SUM(volume_hand::numeric), 0),
                   COALESCE(SUM(amount_li::numeric), 0)
              FROM market.kline_minute_raw
             WHERE trade_time >= (%s::date + TIME '00:00') AT TIME ZONE 'Asia/Shanghai'
               AND trade_time < ((%s::date + 1) + TIME '00:00') AT TIME ZONE 'Asia/Shanghai'
            """,
            (historical_date, historical_date),
        )
        row = cur.fetchone()
    values = row or (0, 0, 0, 0, 0, 0, 0)
    return HistoricalSnapshot(historical_date.isoformat(), *(int(value or 0) for value in values))


def _target_stats(conn: Any, target_date: dt.date) -> tuple[int, dt.datetime | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::bigint, MAX(trade_time)
              FROM market.kline_minute_raw
             WHERE trade_time >= (%s::date + TIME '00:00') AT TIME ZONE 'Asia/Shanghai'
               AND trade_time < ((%s::date + 1) + TIME '00:00') AT TIME ZONE 'Asia/Shanghai'
            """,
            (target_date, target_date),
        )
        row = cur.fetchone()
    return (int(row[0] or 0), row[1]) if row else (0, None)


def _target_code_counts(conn: Any, target_date: dt.date) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts_code, COUNT(*)::int
              FROM market.kline_minute_raw
             WHERE trade_time >= (%s::date + TIME '00:00') AT TIME ZONE 'Asia/Shanghai'
               AND trade_time < ((%s::date + 1) + TIME '00:00') AT TIME ZONE 'Asia/Shanghai'
             GROUP BY ts_code
            """,
            (target_date, target_date),
        )
        rows = cur.fetchall()
    return {str(row[0]): int(row[1]) for row in rows if row and row[0] is not None}


def _chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _fetch_batch(
    api_base: str,
    target_date: dt.date,
    codes: list[str],
    workers: int,
) -> Iterable[tuple[str, list[dict[str, Any]] | None, str | None]]:
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="minute-repair-fetch") as executor:
        futures = {executor.submit(fetch_minute_single_day, api_base, code, target_date): code for code in codes}
        for future in as_completed(futures):
            code = futures[future]
            try:
                yield code, future.result(), None
            except Exception as exc:  # noqa: BLE001
                yield code, None, str(exc)


def inspect_completeness(conn: Any, target_date: dt.date, expected: list[str]) -> dict[str, Any]:
    if not incremental.is_trading_day(conn, target_date):
        raise RepairError(f"authorized date is not a trading day: {target_date}")
    if not expected:
        raise RepairError(f"TDX stock directory has no A-share symbols for {target_date}")
    gaps = incremental.find_minute_day_gaps(conn, target_date, expected, EXPECTED_BARS)
    return {
        "expected_codes": expected,
        "expected_code_count": len(expected),
        "gap_count": len(gaps),
        "gap_samples": gaps[:20],
    }


def repair_today(
    conn: Any,
    *,
    target_date: dt.date,
    api_base: str,
    apply: bool,
    workers: int,
    batch_size: int,
    target_db: str,
) -> dict[str, Any]:
    expected_codes = fetch_current_stock_codes(api_base)
    preflight = inspect_completeness(conn, target_date, expected_codes)
    history_before = historical_snapshot(conn, target_date)
    base_result: dict[str, Any] = {
        "schema_version": "minute_today_upsert_repair_v1",
        "dataset": DATASET,
        "target_date": target_date.isoformat(),
        "target_db": target_db,
        "write_mode": "upsert_only",
        "universe_source": "tdx_api_codes",
        "expected_code_count": preflight["expected_code_count"],
        "initial_gap_count": preflight["gap_count"],
        "initial_gap_samples": preflight["gap_samples"],
        "historical_guard_before": asdict(history_before),
        "applied": apply,
    }
    if not apply:
        return {**base_result, "status": "plan_only"}

    expected_codes = list(preflight["expected_codes"])
    job_summary = {
        "dataset": DATASET,
        "mode": "repair_today_upsert",
        "target_date": target_date.isoformat(),
        "target_db": target_db,
        "expected_codes": len(expected_codes),
        "triggered_by": "operator_repair_today",
    }
    job_id = incremental.create_job(conn, "incremental", job_summary)
    run_id = incremental.create_run(conn, DATASET, {**job_summary, "job_id": str(job_id)})
    touched_rows = 0
    codes_with_data = 0
    source_bar_counts: dict[str, int] = {}
    source_empty_codes: list[str] = []
    fetch_failures: list[dict[str, str]] = []

    for batch in _chunks(expected_codes, batch_size):
        batch_touched_rows = 0
        batch_codes_with_data = 0
        batch_failure_count = 0
        for ts_code, bars, error in _fetch_batch(api_base, target_date, batch, workers):
            if error is not None:
                fetch_failures.append({"ts_code": ts_code, "error": error})
                batch_failure_count += 1
                incremental.log_error(
                    conn,
                    run_id,
                    DATASET,
                    ts_code,
                    "today minute repair fetch failed",
                    {"target_date": target_date.isoformat(), "error": error},
                )
                continue
            source_bar_counts[ts_code] = len(bars)
            if not bars:
                source_empty_codes.append(ts_code)
                continue
            try:
                inserted, _last_ts = incremental.upsert_minute(conn, ts_code, target_date, bars)
            except Exception as exc:  # noqa: BLE001
                fetch_failures.append({"ts_code": ts_code, "error": str(exc)})
                batch_failure_count += 1
                incremental.log_error(
                    conn,
                    run_id,
                    DATASET,
                    ts_code,
                    "today minute repair upsert failed",
                    {"target_date": target_date.isoformat(), "error": str(exc)},
                )
                continue
            touched_rows += int(inserted)
            codes_with_data += 1
            batch_touched_rows += int(inserted)
            batch_codes_with_data += 1
        incremental.update_job_summary(
            conn,
            job_id,
            {
                "processed_codes": len(batch),
                "codes_with_data": batch_codes_with_data,
                "touched_rows": batch_touched_rows,
                "fetch_failure_count": batch_failure_count,
            },
        )

    history_after = historical_snapshot(conn, target_date)
    history_unchanged = history_before == history_after
    row_count, data_max_at = _target_stats(conn, target_date)
    actual_bar_counts = _target_code_counts(conn, target_date)
    count_mismatches = [
        {
            "ts_code": ts_code,
            "source_bars": source_bar_counts.get(ts_code),
            "database_bars": actual_bar_counts.get(ts_code, 0),
        }
        for ts_code in expected_codes
        if ts_code in source_bar_counts and actual_bar_counts.get(ts_code, 0) != source_bar_counts[ts_code]
    ]
    unexpected_codes = sorted(set(actual_bar_counts) - set(expected_codes))
    source_data_codes = [code for code, count in source_bar_counts.items() if count > 0]
    structural_gaps = incremental.find_minute_day_gaps(conn, target_date, source_data_codes, EXPECTED_BARS)
    source_parity = not count_mismatches and not unexpected_codes and len(source_bar_counts) == len(expected_codes)
    status = "success" if source_parity and not fetch_failures and history_unchanged else "failed"
    final_summary = {
        **job_summary,
        "run_id": str(run_id),
        "status": status,
        "touched_rows": touched_rows,
        "codes_with_data": codes_with_data,
        "fetch_failure_count": len(fetch_failures),
        "source_empty_code_count": len(source_empty_codes),
        "source_parity_mismatch_count": len(count_mismatches),
        "unexpected_database_code_count": len(unexpected_codes),
        "noncanonical_source_shape_count": len(structural_gaps),
        "source_parity": source_parity,
        "history_unchanged": history_unchanged,
    }

    audit = DataRefreshAuditRepository()
    audit_metadata = {
        "schema_version": "minute_today_upsert_repair_audit_v1",
        "repair_job_id": str(job_id),
        "repair_run_id": str(run_id),
        "expected_code_count": len(expected_codes),
        "source_data_code_count": len(source_data_codes),
        "source_empty_code_count": len(source_empty_codes),
        "source_parity": source_parity,
        "source_parity_mismatch_count": len(count_mismatches),
        "unexpected_database_code_count": len(unexpected_codes),
        "noncanonical_source_shape_count": len(structural_gaps),
        "accepted_total_bars": [EXPECTED_BARS, EXPECTED_BARS + 1],
        "upsert_only": True,
        "history_unchanged": history_unchanged,
    }
    if status == "success":
        audit.record_success(
            dataset=DATASET,
            trade_date=target_date,
            row_count=row_count,
            job_id=str(job_id),
            data_source=DATA_SOURCE,
            metadata=audit_metadata,
            data_max_at=data_max_at,
            written_rows=touched_rows,
            expected_rows=sum(source_bar_counts.values()),
            coverage_ratio=1.0,
            quality_status="ok",
            conn=conn,
        )
    else:
        audit.record_failure(
            dataset=DATASET,
            trade_date=target_date,
            error_message="today minute upsert repair did not pass source parity and history guards",
            job_id=str(job_id),
            data_source=DATA_SOURCE,
            metadata={
                **audit_metadata,
                "fetch_failure_samples": fetch_failures[:20],
                "source_parity_mismatch_samples": count_mismatches[:20],
                "unexpected_database_code_samples": unexpected_codes[:20],
                "noncanonical_source_shape_samples": structural_gaps[:20],
            },
            data_max_at=data_max_at,
            written_rows=touched_rows,
            expected_rows=sum(source_bar_counts.values()),
            coverage_ratio=max((len(source_bar_counts) - len(fetch_failures)) / len(expected_codes), 0.0),
            quality_status="low_coverage",
            failure_category="minute_source_parity_failed",
            conn=conn,
        )

    incremental.finish_run(conn, run_id, status, final_summary)
    incremental.finish_job(conn, job_id, status, final_summary)
    return {
        **base_result,
        **final_summary,
        "job_id": str(job_id),
        "row_count": row_count,
        "fetch_failure_samples": fetch_failures[:20],
        "source_empty_code_samples": source_empty_codes[:20],
        "source_parity_mismatch_samples": count_mismatches[:20],
        "unexpected_database_code_samples": unexpected_codes[:20],
        "noncanonical_source_shape_samples": structural_gaps[:20],
        "historical_guard_after": asdict(history_after),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-db", choices=("dev", "production"), required=True)
    parser.add_argument("--env-file")
    parser.add_argument("--date", help="Must equal the current Asia/Shanghai date")
    parser.add_argument("--api-base")
    parser.add_argument("--workers", type=int, default=4, choices=(1, 2, 4, 8, 12, 16))
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--apply", action="store_true", help="Execute the bounded upsert; default is read-only planning")
    parser.add_argument("--confirm-production")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    target_date = dt.date.fromisoformat(args.date) if args.date else china_today()
    today = china_today()
    if target_date != today:
        raise RepairError(f"repair is limited to today: expected={today}, requested={target_date}")
    if args.batch_size < 1 or args.batch_size > 1000:
        raise RepairError("batch size must be between 1 and 1000")

    env_file = _discover_env_file(args.env_file)
    db_config = _database_config(args.target_db, env_file)
    if args.target_db == "production" and args.apply:
        expected = _production_confirmation(target_date, str(db_config["dbname"]))
        if args.confirm_production != expected:
            raise RepairError(f"production confirmation mismatch; expected exact value: {expected}")

    api_base = args.api_base or os.getenv("TDX_API_BASE", "http://127.0.0.1:19080")
    with psycopg2.connect(**db_config) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '10min'")
        result = repair_today(
            conn,
            target_date=target_date,
            api_base=api_base,
            apply=bool(args.apply),
            workers=int(args.workers),
            batch_size=int(args.batch_size),
            target_db=str(args.target_db),
        )
    print(json.dumps(result, ensure_ascii=False, default=str, sort_keys=True))
    return 0 if result.get("status") in {"plan_only", "success"} else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
