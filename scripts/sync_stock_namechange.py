"""Synchronize authoritative Tushare security-name intervals.

The command is read-only by default. Database writes require ``--apply`` and
an exact ``--expected-database`` identity. It never deletes source rows.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.tushare_dataset_specs import DatasetSpec, QueryMode  # noqa: E402
from backend.services.tushare_sync_engine import TushareSyncEngine  # noqa: E402


SOURCE_API = "tushare.namechange"
DEFAULT_ROW_LIMIT = 5000
MAX_WORKERS = 4
STABILITY_ATTEMPTS = 4
NAMECHANGE_SPEC = DatasetSpec(
    name="stock_namechange",
    tushare_api="namechange",
    target_table="market.stock_namechange",
    primary_keys=["ts_code", "name", "start_date"],
    query_mode=QueryMode.BY_CODE,
    columns={
        "ts_code": "text",
        "name": "text",
        "start_date": "date",
        "end_date": "date",
        "ann_date": "date",
        "change_reason": "text",
    },
    fetch_params={"limit": 5000},
    row_limit=5000,
    batch_sleep=0.3,
)


class StockNamechangeSyncError(RuntimeError):
    """Fail-closed synchronization error."""


@dataclass(frozen=True)
class NamechangeRow:
    ts_code: str
    name: str
    start_date: dt.date
    end_date: dt.date | None
    ann_date: dt.date | None
    change_reason: str | None
    source_record_sha256: str
    source_payload: dict[str, Any]

    @property
    def key(self) -> tuple[str, str, dt.date]:
        return self.ts_code, self.name, self.start_date


def _parse_date(value: Any, *, required: bool) -> dt.date | None:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        if required:
            raise StockNamechangeSyncError("Tushare namechange row is missing a required date")
        return None
    try:
        return dt.datetime.strptime(text.split(".", 1)[0], "%Y%m%d").date()
    except ValueError as exc:
        raise StockNamechangeSyncError(f"invalid Tushare date: {text!r}") from exc


def _canonical_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    def clean(value: Any) -> str:
        text = str(value or "").strip()
        return "" if text.lower() == "nan" else text

    return {
        "ts_code": clean(row.get("ts_code")).upper(),
        "name": clean(row.get("name")),
        "start_date": clean(row.get("start_date")).split(".", 1)[0],
        "end_date": clean(row.get("end_date")).split(".", 1)[0] or None,
        "ann_date": clean(row.get("ann_date")).split(".", 1)[0] or None,
        "change_reason": clean(row.get("change_reason")) or None,
    }


def normalize_provider_rows(rows: Iterable[Mapping[str, Any]]) -> list[NamechangeRow]:
    """Normalize, validate, deduplicate and deterministically sort provider rows."""

    by_key: dict[tuple[str, str, dt.date], NamechangeRow] = {}
    for raw in rows:
        payload = _canonical_payload(raw)
        ts_code = payload["ts_code"]
        name = payload["name"]
        if not ts_code or not name:
            raise StockNamechangeSyncError("Tushare namechange row is missing ts_code or name")
        start_date = _parse_date(payload["start_date"], required=True)
        assert start_date is not None
        end_date = _parse_date(payload["end_date"], required=False)
        ann_date = _parse_date(payload["ann_date"], required=False)
        if end_date is not None and end_date < start_date:
            raise StockNamechangeSyncError(
                f"invalid name interval for {ts_code}: {start_date}..{end_date}"
            )
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        normalized = NamechangeRow(
            ts_code=ts_code,
            name=name,
            start_date=start_date,
            end_date=end_date,
            ann_date=ann_date,
            change_reason=payload["change_reason"],
            source_record_sha256=hashlib.sha256(encoded).hexdigest(),
            source_payload=payload,
        )
        existing = by_key.get(normalized.key)
        if existing is not None and existing.source_record_sha256 != normalized.source_record_sha256:
            raise StockNamechangeSyncError(f"conflicting duplicate provider row: {normalized.key}")
        by_key[normalized.key] = normalized
    normalized_rows = [
        by_key[key]
        for key in sorted(by_key, key=lambda value: (value[0], value[2], value[1]))
    ]
    validate_non_overlapping_intervals(normalized_rows)
    return normalized_rows


def validate_non_overlapping_intervals(rows: Iterable[NamechangeRow]) -> None:
    grouped: dict[str, list[NamechangeRow]] = {}
    for row in rows:
        grouped.setdefault(row.ts_code, []).append(row)
    for ts_code, code_rows in grouped.items():
        ordered = sorted(code_rows, key=lambda row: (row.start_date, row.name))
        for previous, current in zip(ordered, ordered[1:]):
            if previous.end_date is None or current.start_date <= previous.end_date:
                raise StockNamechangeSyncError(
                    "overlapping Tushare namechange intervals for "
                    f"{ts_code}: {previous.start_date}..{previous.end_date} "
                    f"and {current.start_date}..{current.end_date}"
                )


def _stable_provider_fetch(
    fetch: Any,
    *,
    label: str,
    attempts: int = STABILITY_ATTEMPTS,
) -> list[Mapping[str, Any]]:
    """Require two consecutive identical normalized provider responses."""

    previous_digest: str | None = None
    previous_rows: list[NamechangeRow] | None = None
    observed: list[tuple[int, str]] = []
    for attempt in range(1, attempts + 1):
        normalized = normalize_provider_rows(fetch())
        digest = hashlib.sha256(
            "\n".join(row.source_record_sha256 for row in normalized).encode("ascii")
        ).hexdigest()
        observed.append((len(normalized), digest))
        if previous_digest == digest and previous_rows is not None:
            return [row.source_payload for row in previous_rows]
        previous_digest = digest
        previous_rows = normalized
        if attempt < attempts:
            time.sleep(0.25 * attempt)
    raise StockNamechangeSyncError(
        f"Tushare namechange response is unstable for {label}: "
        + json.dumps(observed, ensure_ascii=False)
    )


def _fetch_by_code(codes: Sequence[str], max_workers: int) -> list[Mapping[str, Any]]:
    def fetch(code: str) -> list[Mapping[str, Any]]:
        def call() -> list[Mapping[str, Any]]:
            return TushareSyncEngine()._fetch_from_tushare(
                NAMECHANGE_SPEC,
                {"ts_code": code},
            )

        return _stable_provider_fetch(call, label=code)

    rows: list[Mapping[str, Any]] = []
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch, code): code for code in codes}
        for future in as_completed(futures):
            code = futures[future]
            try:
                rows.extend(future.result())
            except Exception as exc:  # provider errors must fail the whole sync
                errors[code] = f"{type(exc).__name__}: {exc}"
    if errors:
        raise StockNamechangeSyncError(
            "Tushare namechange code queries failed: "
            + json.dumps(errors, ensure_ascii=False, sort_keys=True)
        )
    return rows


def _fetch_by_range(start_date: dt.date, end_date: dt.date) -> list[Mapping[str, Any]]:
    def call() -> list[Mapping[str, Any]]:
        return TushareSyncEngine()._fetch_from_tushare(
            NAMECHANGE_SPEC,
            {
                "start_date": start_date.strftime("%Y%m%d"),
                "end_date": end_date.strftime("%Y%m%d"),
            },
        )

    return _stable_provider_fetch(
        call,
        label=f"{start_date.isoformat()}..{end_date.isoformat()}",
    )


def _db_config(prefix: str) -> dict[str, Any]:
    required = ("HOST", "PORT", "USER", "PASSWORD", "NAME")
    values = {key: os.getenv(f"{prefix}{key}") for key in required}
    missing = [f"{prefix}{key}" for key, value in values.items() if value in (None, "")]
    if missing:
        raise StockNamechangeSyncError(f"database environment is incomplete: {missing}")
    return {
        "host": values["HOST"],
        "port": int(str(values["PORT"])),
        "user": values["USER"],
        "password": values["PASSWORD"],
        "dbname": values["NAME"],
    }


def _apply_rows(rows: Sequence[NamechangeRow], *, prefix: str, expected_database: str) -> dict[str, Any]:
    with psycopg2.connect(**_db_config(prefix)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), to_regclass('market.stock_namechange')")
            database, table = cur.fetchone()
            if database != expected_database:
                raise StockNamechangeSyncError(
                    f"database identity mismatch: expected={expected_database!r} actual={database!r}"
                )
            if table is None:
                raise StockNamechangeSyncError(
                    "market.stock_namechange is absent; apply the reviewed migration first"
                )
            cur.execute("SELECT COUNT(*) FROM market.stock_namechange")
            before_rows = int(cur.fetchone()[0])
            if rows:
                ts_codes = sorted({row.ts_code for row in rows})
                cur.execute(
                    """
                    SELECT ts_code, name, start_date, end_date, ann_date, change_reason,
                           source_record_sha256, source_payload
                      FROM market.stock_namechange
                     WHERE ts_code = ANY(%s)
                    """,
                    (ts_codes,),
                )
                combined = {
                    (str(existing[0]), str(existing[1]), existing[2]): NamechangeRow(
                        ts_code=str(existing[0]),
                        name=str(existing[1]),
                        start_date=existing[2],
                        end_date=existing[3],
                        ann_date=existing[4],
                        change_reason=existing[5],
                        source_record_sha256=str(existing[6]),
                        source_payload=dict(existing[7] or {}),
                    )
                    for existing in cur.fetchall()
                }
                combined.update({row.key: row for row in rows})
                validate_non_overlapping_intervals(combined.values())
                changed = pgx.execute_values(
                    cur,
                    """
                    INSERT INTO market.stock_namechange (
                        ts_code, name, start_date, end_date, ann_date, change_reason,
                        source_api, source_record_sha256, source_payload
                    ) VALUES %s
                    ON CONFLICT (ts_code, name, start_date) DO UPDATE
                       SET end_date = EXCLUDED.end_date,
                           ann_date = EXCLUDED.ann_date,
                           change_reason = EXCLUDED.change_reason,
                           source_record_sha256 = EXCLUDED.source_record_sha256,
                           source_payload = EXCLUDED.source_payload,
                           updated_at = NOW()
                     WHERE market.stock_namechange.source_record_sha256
                           IS DISTINCT FROM EXCLUDED.source_record_sha256
                    RETURNING ts_code, name, start_date
                    """,
                    [
                        (
                            row.ts_code,
                            row.name,
                            row.start_date,
                            row.end_date,
                            row.ann_date,
                            row.change_reason,
                            SOURCE_API,
                            row.source_record_sha256,
                            pgx.Json(row.source_payload),
                        )
                        for row in rows
                    ],
                    page_size=500,
                    fetch=True,
                )
                affected = len(changed)
            else:
                affected = 0
            cur.execute("SELECT COUNT(*) FROM market.stock_namechange")
            table_rows = int(cur.fetchone()[0])
        conn.commit()
    return {
        "database": database,
        "affected_rows": affected,
        "table_rows_before": before_rows,
        "table_rows": table_rows,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--ts-code", action="append", dest="ts_codes")
    mode.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--row-limit", type=int, default=DEFAULT_ROW_LIMIT)
    parser.add_argument("--db-env-prefix", choices=("TDX_DB_DEV_", "TDX_DB_"), default="TDX_DB_DEV_")
    parser.add_argument("--expected-database")
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_dotenv(ROOT / ".env", override=False)
    token = str(os.getenv("TUSHARE_TOKEN") or "").strip()
    if not token:
        raise StockNamechangeSyncError("TUSHARE_TOKEN is not configured")
    if args.max_workers < 1 or args.max_workers > MAX_WORKERS:
        raise StockNamechangeSyncError(f"--max-workers must be within 1..{MAX_WORKERS}")
    if args.row_limit < 1:
        raise StockNamechangeSyncError("--row-limit must be positive")
    if args.start_date:
        if not args.end_date:
            raise StockNamechangeSyncError("--end-date is required with --start-date")
        start_date = dt.date.fromisoformat(args.start_date)
        end_date = dt.date.fromisoformat(args.end_date)
        if end_date < start_date:
            raise StockNamechangeSyncError("--end-date must not precede --start-date")
        range_rows = _fetch_by_range(start_date, end_date)
        affected_codes = sorted(
            {
                str(row.get("ts_code") or "").strip().upper()
                for row in range_rows
                if str(row.get("ts_code") or "").strip()
            }
        )
        raw_rows = _fetch_by_code(affected_codes, args.max_workers) if affected_codes else []
        request = {
            "mode": "announcement_range_then_full_code_reconcile",
            "start_date": args.start_date,
            "end_date": args.end_date,
            "range_probe_row_count": len(range_rows),
            "affected_code_count": len(affected_codes),
            "affected_codes": affected_codes,
        }
    else:
        if args.end_date:
            raise StockNamechangeSyncError("--end-date requires --start-date")
        codes = sorted({str(code).strip().upper() for code in args.ts_codes or [] if str(code).strip()})
        if not codes:
            raise StockNamechangeSyncError("at least one non-empty --ts-code is required")
        raw_rows = _fetch_by_code(codes, args.max_workers)
        request = {"mode": "codes", "code_count": len(codes), "codes": codes}
    if len(raw_rows) >= args.row_limit:
        raise StockNamechangeSyncError(
            f"provider returned {len(raw_rows)} rows at row_limit={args.row_limit}; narrow the request"
        )
    rows = normalize_provider_rows(raw_rows)
    digest = hashlib.sha256(
        "\n".join(row.source_record_sha256 for row in rows).encode("ascii")
    ).hexdigest()
    result: dict[str, Any] = {
        "schema_version": "stock_namechange_sync_receipt_v1",
        "source_api": SOURCE_API,
        "provider_stability_contract": "two_consecutive_normalized_responses_v1",
        "request": request,
        "provider_row_count": len(raw_rows),
        "normalized_row_count": len(rows),
        "content_sha256": digest,
        "apply": bool(args.apply),
    }
    if args.apply:
        if not args.expected_database:
            raise StockNamechangeSyncError("--expected-database is required with --apply")
        result["database_write"] = _apply_rows(
            rows,
            prefix=args.db_env_prefix,
            expected_database=args.expected_database,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except StockNamechangeSyncError as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2) from exc
