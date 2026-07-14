"""Safely backfill missing fields in one ``market.daily_basic`` snapshot.

The command is dry-run by default.  Apply mode requires an explicit confirmation
token and performs one atomic upsert.  Existing non-NULL values are preserved;
provider NULLs never overwrite database values.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "aistock_daily_basic_field_backfill_v1"
CONFIRM_APPLY = "APPLY_DAILY_BASIC_FIELD_BACKFILL"
DEFAULT_FIELDS = ("turnover_rate_f", "volume_ratio")
KEY_COLUMNS = ("trade_date", "ts_code")
DATA_COLUMNS = (
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
)
ALL_COLUMNS = (*KEY_COLUMNS, *DATA_COLUMNS)


class DailyBasicBackfillError(RuntimeError):
    """Raised when a repair cannot be proved safe."""


@dataclass(frozen=True)
class SnapshotStats:
    row_count: int
    non_null: dict[str, int]


@dataclass(frozen=True)
class DatabasePreview:
    stats: SnapshotStats
    existing_codes: set[str]
    missing_by_field: dict[str, int]
    non_null_codes_by_field: dict[str, set[str]]
    values_by_field: dict[str, dict[str, Any]]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DailyBasicBackfillError(message)


def parse_trade_date(value: str) -> dt.date:
    text = value.strip()
    try:
        if len(text) == 8 and text.isdigit():
            parsed = dt.datetime.strptime(text, "%Y%m%d").date()
        else:
            parsed = dt.date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("trade date must be YYYY-MM-DD or YYYYMMDD") from exc
    if parsed >= dt.date.today():
        raise argparse.ArgumentTypeError("trade date must be earlier than today")
    return parsed


def parse_fields(value: str | Sequence[str]) -> tuple[str, ...]:
    raw = value.split(",") if isinstance(value, str) else list(value)
    fields = tuple(dict.fromkeys(item.strip() for item in raw if item.strip()))
    _require(bool(fields), "at least one field is required")
    unknown = sorted(set(fields) - set(DATA_COLUMNS))
    _require(not unknown, f"unsupported daily_basic fields: {unknown}")
    return fields


def _python_value(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def fetch_tushare_snapshot(trade_date: dt.date, *, pro: Any | None = None) -> pd.DataFrame:
    if pro is None:
        token = (os.getenv("TUSHARE_TOKEN") or "").strip()
        _require(bool(token), "TUSHARE_TOKEN is not configured")
        import tushare as ts

        pro = ts.pro_api(token)
    fields = ",".join(ALL_COLUMNS)
    frame = pro.daily_basic(trade_date=trade_date.strftime("%Y%m%d"), fields=fields)
    if frame is None:
        return pd.DataFrame(columns=ALL_COLUMNS)
    return frame.reindex(columns=ALL_COLUMNS).copy()


def validate_snapshot(
    frame: pd.DataFrame,
    *,
    trade_date: dt.date,
    fill_fields: Sequence[str],
    min_rows: int,
    min_non_null_ratio: float,
) -> SnapshotStats:
    _require(min_rows > 0, "min_rows must be positive")
    _require(0.0 < min_non_null_ratio <= 1.0, "min_non_null_ratio must be in (0, 1]")
    _require(not frame.empty, f"Tushare daily_basic returned no rows for {trade_date}")
    _require(set(ALL_COLUMNS).issubset(frame.columns), "Tushare snapshot is missing declared columns")

    expected = trade_date.strftime("%Y%m%d")
    observed_dates = {
        str(value).replace("-", "")
        for value in frame["trade_date"].dropna().unique().tolist()
    }
    _require(observed_dates == {expected}, f"provider trade_date mismatch: {sorted(observed_dates)}")
    _require(frame["ts_code"].notna().all(), "provider snapshot contains NULL ts_code")
    _require(not frame["ts_code"].duplicated().any(), "provider snapshot contains duplicate ts_code")
    _require(len(frame) >= min_rows, f"provider row_count={len(frame)} below min_rows={min_rows}")

    non_null = {field: int(frame[field].notna().sum()) for field in fill_fields}
    for field, count in non_null.items():
        ratio = count / len(frame)
        _require(
            ratio >= min_non_null_ratio,
            f"provider field {field} non_null_ratio={ratio:.6f} below {min_non_null_ratio:.6f}",
        )
    return SnapshotStats(row_count=len(frame), non_null=non_null)


def db_config() -> dict[str, Any]:
    return {
        "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TDX_DB_PORT", "5432")),
        "user": os.getenv("TDX_DB_USER", "postgres"),
        "password": os.getenv("TDX_DB_PASSWORD", ""),
        "dbname": os.getenv("TDX_DB_NAME", "aistock"),
        "application_name": "AIstock-daily-basic-field-backfill",
        "options": "-c client_encoding=utf8 -c statement_timeout=60000",
    }


def connect_db() -> Any:
    return psycopg2.connect(**db_config())


def _select_columns(fields: Sequence[str]) -> str:
    return ", ".join(("ts_code", *fields))


def preview_database(conn: Any, trade_date: dt.date, fields: Sequence[str]) -> DatabasePreview:
    sql = f"SELECT {_select_columns(fields)} FROM market.daily_basic WHERE trade_date = %s ORDER BY ts_code"
    with conn.cursor() as cur:
        cur.execute(sql, (trade_date,))
        rows = cur.fetchall()
    existing_codes: set[str] = set()
    missing = {field: 0 for field in fields}
    non_null = {field: 0 for field in fields}
    non_null_codes = {field: set() for field in fields}
    values = {field: {} for field in fields}
    for row in rows:
        code = str(row[0])
        existing_codes.add(code)
        for index, field in enumerate(fields, start=1):
            if row[index] is None:
                missing[field] += 1
            else:
                non_null[field] += 1
                non_null_codes[field].add(code)
                values[field][code] = row[index]
    return DatabasePreview(
        stats=SnapshotStats(row_count=len(rows), non_null=non_null),
        existing_codes=existing_codes,
        missing_by_field=missing,
        non_null_codes_by_field=non_null_codes,
        values_by_field=values,
    )


def build_upsert_sql(fill_fields: Sequence[str]) -> str:
    fields = parse_fields(fill_fields)
    columns = ", ".join(ALL_COLUMNS)
    assignments = ", ".join(
        f"{field} = COALESCE(target.{field}, EXCLUDED.{field})" for field in fields
    )
    predicate = " OR ".join(
        f"(target.{field} IS NULL AND EXCLUDED.{field} IS NOT NULL)" for field in fields
    )
    return (
        f"INSERT INTO market.daily_basic AS target ({columns}) VALUES %s "
        "ON CONFLICT (trade_date, ts_code) DO UPDATE SET "
        f"{assignments} WHERE {predicate}"
    )


def snapshot_rows(frame: pd.DataFrame, trade_date: dt.date) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for record in frame.to_dict(orient="records"):
        values: list[Any] = [trade_date, str(record["ts_code"]).strip()]
        values.extend(_python_value(record[column]) for column in DATA_COLUMNS)
        rows.append(tuple(values))
    return rows


def _source_non_null_codes(frame: pd.DataFrame, field: str) -> set[str]:
    return set(frame.loc[frame[field].notna(), "ts_code"].astype(str))


def verify_after(
    frame: pd.DataFrame,
    before: DatabasePreview,
    preview: DatabasePreview,
    *,
    fields: Sequence[str],
) -> None:
    source_codes = set(frame["ts_code"].astype(str))
    missing_codes = sorted(source_codes - preview.existing_codes)
    _require(not missing_codes, f"database is missing {len(missing_codes)} provider rows after upsert")
    for field in fields:
        expected_codes = _source_non_null_codes(frame, field)
        missing_count = len(expected_codes - preview.non_null_codes_by_field[field])
        _require(
            missing_count == 0,
            f"database field {field} still lacks at least {missing_count} provider non-NULL values",
        )
        source_values = {
            str(row["ts_code"]): row[field]
            for row in frame.loc[frame[field].notna(), ["ts_code", field]].to_dict(orient="records")
        }
        filled_codes = expected_codes - before.non_null_codes_by_field[field]
        mismatched = [
            code
            for code in sorted(filled_codes)
            if not _numeric_equal(preview.values_by_field[field].get(code), source_values[code])
        ]
        _require(
            not mismatched,
            f"database field {field} differs from provider for {len(mismatched)} filled rows; "
            f"samples={mismatched[:10]}",
        )


def _numeric_equal(left: Any, right: Any) -> bool:
    try:
        return Decimal(str(left)).normalize() == Decimal(str(_python_value(right))).normalize()
    except (InvalidOperation, ValueError):
        return left == _python_value(right)


def _report(
    *,
    trade_date: dt.date,
    fields: Sequence[str],
    source: SnapshotStats,
    before: DatabasePreview,
    after: DatabasePreview | None,
    source_codes: set[str],
    source_non_null_codes: dict[str, set[str]],
    applied: bool,
) -> dict[str, Any]:
    inserted_codes = sorted(source_codes - before.existing_codes)
    db_only_codes = sorted(before.existing_codes - source_codes)
    planned_fills = {
        field: len(source_non_null_codes[field] - before.non_null_codes_by_field[field])
        for field in fields
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "applied" if applied else "preview",
        "trade_date": trade_date.isoformat(),
        "fields": list(fields),
        "db_writes": applied,
        "ddl": False,
        "source": asdict(source),
        "before": {
            "row_count": before.stats.row_count,
            "non_null": before.stats.non_null,
            "missing": before.missing_by_field,
        },
        "planned_missing_field_fills": planned_fills,
        "source_only_code_count": len(inserted_codes),
        "source_only_code_samples": inserted_codes[:20],
        "db_only_code_count": len(db_only_codes),
        "db_only_code_samples": db_only_codes[:20],
        "after": None
        if after is None
        else {
            "row_count": after.stats.row_count,
            "non_null": after.stats.non_null,
            "missing": after.missing_by_field,
        },
    }


def execute_backfill(
    *,
    trade_date: dt.date,
    fields: Sequence[str],
    apply: bool,
    min_rows: int,
    min_non_null_ratio: float,
    pro: Any | None = None,
    connection_factory: Callable[[], Any] = connect_db,
) -> dict[str, Any]:
    fill_fields = parse_fields(fields)
    frame = fetch_tushare_snapshot(trade_date, pro=pro)
    source = validate_snapshot(
        frame,
        trade_date=trade_date,
        fill_fields=fill_fields,
        min_rows=min_rows,
        min_non_null_ratio=min_non_null_ratio,
    )
    source_codes = set(frame["ts_code"].astype(str))
    source_non_null_codes = {
        field: _source_non_null_codes(frame, field) for field in fill_fields
    }
    conn = connection_factory()
    try:
        conn.autocommit = False
        before = preview_database(conn, trade_date, fill_fields)
        _require(before.stats.row_count > 0, f"database has no daily_basic rows for {trade_date}")
        if not apply:
            conn.rollback()
            return _report(
                trade_date=trade_date,
                fields=fill_fields,
                source=source,
                before=before,
                after=None,
                source_codes=source_codes,
                source_non_null_codes=source_non_null_codes,
                applied=False,
            )

        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"aistock.daily_basic.backfill:{trade_date.isoformat()}",),
            )
            execute_values(cur, build_upsert_sql(fill_fields), snapshot_rows(frame, trade_date), page_size=1000)
        after = preview_database(conn, trade_date, fill_fields)
        verify_after(frame, before, after, fields=fill_fields)
        conn.commit()
        return _report(
            trade_date=trade_date,
            fields=fill_fields,
            source=source,
            before=before,
            after=after,
            source_codes=source_codes,
            source_non_null_codes=source_non_null_codes,
            applied=True,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill missing fields in one market.daily_basic date.")
    parser.add_argument("--trade-date", required=True, type=parse_trade_date)
    parser.add_argument("--fields", default=",".join(DEFAULT_FIELDS))
    parser.add_argument("--min-rows", type=int, default=5000)
    parser.add_argument("--min-non-null-ratio", type=float, default=0.95)
    parser.add_argument("--apply", action="store_true", help="Apply the atomic upsert; omitted means dry-run.")
    parser.add_argument("--confirm-apply", default="", help="Exact confirmation token required with --apply.")
    parser.add_argument("--output", help="Optional JSON report path.")
    return parser


def emit(report: dict[str, Any], output: str | None) -> None:
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    print(text, end="")


def main(argv: Iterable[str] | None = None) -> int:
    load_dotenv(REPO_ROOT / ".env", override=False)
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        fields = parse_fields(args.fields)
        if args.apply:
            _require(
                args.confirm_apply == CONFIRM_APPLY,
                f"--apply requires --confirm-apply {CONFIRM_APPLY}",
            )
        report = execute_backfill(
            trade_date=args.trade_date,
            fields=fields,
            apply=args.apply,
            min_rows=args.min_rows,
            min_non_null_ratio=args.min_non_null_ratio,
        )
        emit(report, args.output)
        return 0
    except DailyBasicBackfillError as exc:
        emit(
            {
                "schema_version": SCHEMA_VERSION,
                "status": "failed",
                "trade_date": args.trade_date.isoformat(),
                "db_writes": False,
                "ddl": False,
                "error": str(exc),
            },
            args.output,
        )
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
