"""Deterministic coverage audit for full-day stock suspensions.

The Tushare ``suspend_d`` contract is queried by trade date, but upstream
history can still omit dates inside a continuous full-day suspension.  This
module does not infer or create suspension rows.  It compares every listed
SH/SZ trading day with raw daily bars, raw minute bars, and exact full-day
``suspend_d`` evidence, then returns a fail-closed receipt for operators and
the ingestion scheduler.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import psycopg2.extras as pgx


SCHEMA_VERSION = "local_data_suspend_d_coverage_audit_v1"
DEFAULT_STATEMENT_TIMEOUT_MS = 300_000
MAX_TARGETED_MINUTE_PROBE_DAYS = 30

FULLY_COVERED = "FULLY_COVERED_SUSPENSION"
PARTIAL_AUTHORITY_GAP = "PARTIAL_SUSPEND_AUTHORITY_GAP"
MINUTE_WITHOUT_DAILY = "MINUTE_WITHOUT_DAILY"
INTRADAY_WITHOUT_DAILY = "INTRADAY_SUSPEND_WITHOUT_DAILY"
UNEXPLAINED_NO_TRADE = "UNEXPLAINED_NO_TRADE_GAP"
FULL_DAY_SUSPEND_WITH_DAILY = "FULL_DAY_SUSPEND_WITH_DAILY"


class SuspendCoverageError(RuntimeError):
    """Raised when the audit cannot establish a bounded, valid window."""


@dataclass(frozen=True)
class CoverageWindow:
    start_date: dt.date
    end_date: dt.date
    trading_day_count: int


def _as_date(value: Any, *, field: str) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise SuspendCoverageError(f"invalid {field}: {value!r}") from exc


def classify_missing_run(
    *,
    missing_days: int,
    full_day_suspend_days: int,
    minute_positive_days: int | None,
    intraday_suspend_days: int,
) -> str:
    """Classify one consecutive no-positive-daily-bar interval.

    Minute evidence takes precedence because a day with minute trades is a
    daily/minute consistency defect, not a suspension.  An intraday halt never
    explains an absent full-day bar.
    """

    if missing_days <= 0:
        raise SuspendCoverageError("missing_days must be positive")
    counts = tuple(
        value
        for value in (full_day_suspend_days, minute_positive_days, intraday_suspend_days)
        if value is not None
    )
    if any(value < 0 or value > missing_days for value in counts):
        raise SuspendCoverageError("run evidence counts must be within missing_days")
    if minute_positive_days:
        return MINUTE_WITHOUT_DAILY
    if full_day_suspend_days == missing_days:
        return FULLY_COVERED
    if full_day_suspend_days:
        return PARTIAL_AUTHORITY_GAP
    if intraday_suspend_days:
        return INTRADAY_WITHOUT_DAILY
    return UNEXPLAINED_NO_TRADE


def resolve_coverage_window(
    conn: Any,
    *,
    end_date: dt.date,
    start_date: dt.date | None = None,
    lookback_trading_days: int | None = None,
) -> CoverageWindow:
    """Resolve requested bounds to actual exchange trading dates."""

    end_date = _as_date(end_date, field="end_date")
    if start_date is not None and lookback_trading_days is not None:
        raise SuspendCoverageError("start_date and lookback_trading_days are mutually exclusive")
    if lookback_trading_days is not None and lookback_trading_days <= 0:
        raise SuspendCoverageError("lookback_trading_days must be positive")

    with conn.cursor() as cur:
        if lookback_trading_days is not None:
            cur.execute(
                """
                SELECT cal_date
                  FROM market.trading_calendar
                 WHERE is_trading=TRUE AND cal_date <= %s
                 ORDER BY cal_date DESC
                 OFFSET %s LIMIT 1
                """,
                (end_date, lookback_trading_days - 1),
            )
            row = cur.fetchone()
            if row is None:
                raise SuspendCoverageError(
                    f"trading calendar has fewer than {lookback_trading_days} days through {end_date}"
                )
            requested_start = _as_date(row[0], field="resolved_start_date")
        else:
            if start_date is None:
                raise SuspendCoverageError("start_date or lookback_trading_days is required")
            requested_start = _as_date(start_date, field="start_date")
        if requested_start > end_date:
            raise SuspendCoverageError("start_date must not be after end_date")
        cur.execute(
            """
            SELECT MIN(cal_date), MAX(cal_date), COUNT(*)::int
              FROM market.trading_calendar
             WHERE is_trading=TRUE AND cal_date BETWEEN %s AND %s
            """,
            (requested_start, end_date),
        )
        row = cur.fetchone()
    if row is None or row[0] is None or row[1] is None or int(row[2]) <= 0:
        raise SuspendCoverageError("coverage window contains no trading dates")
    return CoverageWindow(
        start_date=_as_date(row[0], field="resolved_start_date"),
        end_date=_as_date(row[1], field="resolved_end_date"),
        trading_day_count=int(row[2]),
    )


_MISSING_RUNS_SQL = """
WITH cal AS (
    SELECT cal_date,
           row_number() OVER (ORDER BY cal_date)::int AS trading_ordinal
      FROM market.trading_calendar
     WHERE is_trading=TRUE AND cal_date BETWEEN %(start_date)s AND %(end_date)s
),
symbols AS (
    SELECT ts_code,
           list_date::date AS list_date,
           delist_date::date AS delist_date,
           list_status
      FROM market.stock_basic
     WHERE ts_code ~ '^[036][0-9]{5}\\.(SH|SZ)$'
       AND list_date IS NOT NULL
       AND list_date::date <= %(end_date)s
       AND (delist_date IS NULL OR delist_date::date > %(start_date)s)
),
eligible AS (
    SELECT s.ts_code, s.list_date, s.delist_date, s.list_status,
           c.cal_date, c.trading_ordinal
      FROM symbols s
      JOIN cal c
        ON c.cal_date >= GREATEST(s.list_date, %(start_date)s::date)
       AND (s.delist_date IS NULL OR c.cal_date < s.delist_date)
),
daily AS (
    SELECT ts_code, trade_date,
           BOOL_OR(COALESCE(volume_hand, 0) > 0) AS has_positive_daily
      FROM market.kline_daily_raw
     WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s
     GROUP BY ts_code, trade_date
),
suspension AS (
    SELECT ts_code, trade_date,
           BOOL_OR(
               suspend_type='S'
               AND NULLIF(BTRIM(COALESCE(suspend_timing, '')), '') IS NULL
           ) AS has_full_day_suspend,
           BOOL_OR(
               suspend_type='S'
               AND NULLIF(BTRIM(COALESCE(suspend_timing, '')), '') IS NOT NULL
           ) AS has_intraday_suspend
      FROM market.suspend_d
     WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s
     GROUP BY ts_code, trade_date
),
missing_days AS (
    SELECT e.*,
           COALESCE(s.has_full_day_suspend, FALSE) AS has_full_day_suspend,
           COALESCE(s.has_intraday_suspend, FALSE) AS has_intraday_suspend
      FROM eligible e
      LEFT JOIN daily d ON d.ts_code=e.ts_code AND d.trade_date=e.cal_date
      LEFT JOIN suspension s ON s.ts_code=e.ts_code AND s.trade_date=e.cal_date
     WHERE NOT COALESCE(d.has_positive_daily, FALSE)
),
numbered AS (
    SELECT m.*,
           trading_ordinal
             - row_number() OVER (PARTITION BY ts_code ORDER BY trading_ordinal)::int AS run_group
      FROM missing_days m
)
SELECT ts_code, MIN(cal_date) AS start_date, MAX(cal_date) AS end_date,
       COUNT(*)::int AS missing_days,
       COUNT(*) FILTER (WHERE has_full_day_suspend)::int AS full_day_suspend_days,
       COUNT(*) FILTER (WHERE has_intraday_suspend)::int AS intraday_suspend_days,
       MIN(list_date) AS list_date, MIN(delist_date) AS delist_date,
       MIN(list_status) AS list_status
  FROM numbered
 GROUP BY ts_code, run_group
 ORDER BY ts_code, start_date
"""


_FULL_DAY_SUSPEND_PRICE_CONFLICT_SQL = """
WITH full_day_suspend AS (
    SELECT DISTINCT ts_code, trade_date
      FROM market.suspend_d
     WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s
       AND suspend_type='S'
       AND NULLIF(BTRIM(COALESCE(suspend_timing, '')), '') IS NULL
),
positive_daily AS (
    SELECT ts_code, trade_date
      FROM market.kline_daily_raw
     WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s
     GROUP BY ts_code, trade_date
    HAVING BOOL_OR(COALESCE(volume_hand, 0) > 0)
)
SELECT s.ts_code, s.trade_date
  FROM full_day_suspend s
  JOIN positive_daily d USING (ts_code, trade_date)
 ORDER BY s.ts_code, s.trade_date
"""


def _fetch_missing_runs(conn: Any, window: CoverageWindow) -> list[dict[str, Any]]:
    params = {"start_date": window.start_date, "end_date": window.end_date}
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(_MISSING_RUNS_SQL, params)
        rows = [dict(row) for row in cur.fetchall()]
    probe_rows: list[tuple[int, dict[str, Any]]] = []
    for run_id, row in enumerate(rows):
        row["minute_positive_days"] = None
        row["minute_probe_status"] = "NOT_REQUIRED_FULL_DAY_SUSPEND_COVERAGE"
        if int(row["full_day_suspend_days"]) == int(row["missing_days"]):
            continue
        if (
            _as_date(row["start_date"], field="run_start_date") == window.start_date
            or _as_date(row["end_date"], field="run_end_date") == window.end_date
        ):
            row["minute_probe_status"] = "SKIPPED_WINDOW_BOUNDARY_RUN"
            continue
        if int(row["missing_days"]) > MAX_TARGETED_MINUTE_PROBE_DAYS:
            row["minute_probe_status"] = "SKIPPED_LONG_UNRESOLVED_RUN"
            continue
        row["minute_probe_status"] = "CHECKED"
        probe_rows.append((run_id, row))
    if probe_rows:
        run_ids = [item[0] for item in probe_rows]
        codes = [str(item[1]["ts_code"]) for item in probe_rows]
        starts = [_as_date(item[1]["start_date"], field="run_start_date") for item in probe_rows]
        ends = [_as_date(item[1]["end_date"], field="run_end_date") for item in probe_rows]
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH targets AS (
                    SELECT *
                      FROM unnest(%s::int[], %s::text[], %s::date[], %s::date[])
                           AS t(run_id, ts_code, start_date, end_date)
                )
                SELECT t.run_id, COUNT(DISTINCT m.trade_time::date)::int
                  FROM targets t
                  LEFT JOIN market.kline_minute_raw m
                    ON m.ts_code=t.ts_code
                   AND m.trade_time >= t.start_date
                   AND m.trade_time < t.end_date + INTERVAL '1 day'
                   AND m.trade_time >= %s
                   AND m.trade_time < %s::date + INTERVAL '1 day'
                   AND COALESCE(m.volume_hand, 0) > 0
                 GROUP BY t.run_id
                """,
                (run_ids, codes, starts, ends, min(starts), max(ends)),
            )
            minute_counts = {int(run_id): int(count) for run_id, count in cur.fetchall()}
        for run_id, row in probe_rows:
            row["minute_positive_days"] = minute_counts.get(run_id, 0)
    return rows


def _fetch_full_day_suspend_price_conflicts(
    conn: Any, window: CoverageWindow
) -> list[dict[str, Any]]:
    params = {"start_date": window.start_date, "end_date": window.end_date}
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(_FULL_DAY_SUSPEND_PRICE_CONFLICT_SQL, params)
        return [dict(row) for row in cur.fetchall()]


def _serialize_run(row: Mapping[str, Any]) -> dict[str, Any]:
    missing_days = int(row["missing_days"])
    full_days = int(row["full_day_suspend_days"])
    minute_days = (
        int(row["minute_positive_days"])
        if row.get("minute_positive_days") is not None
        else None
    )
    intraday_days = int(row["intraday_suspend_days"])
    classification = classify_missing_run(
        missing_days=missing_days,
        full_day_suspend_days=full_days,
        minute_positive_days=minute_days,
        intraday_suspend_days=intraday_days,
    )
    return {
        "ts_code": str(row["ts_code"]),
        "start_date": _as_date(row["start_date"], field="run_start_date").isoformat(),
        "end_date": _as_date(row["end_date"], field="run_end_date").isoformat(),
        "missing_days": missing_days,
        "full_day_suspend_days": full_days,
        "uncovered_days": missing_days - full_days,
        "minute_positive_days": minute_days,
        "minute_probe_status": row.get("minute_probe_status", "PRECOMPUTED"),
        "intraday_suspend_days": intraday_days,
        "classification": classification,
        "list_date": _as_date(row["list_date"], field="list_date").isoformat(),
        "delist_date": (
            _as_date(row["delist_date"], field="delist_date").isoformat()
            if row.get("delist_date") is not None
            else None
        ),
        "list_status": row.get("list_status"),
    }


def build_coverage_receipt(
    *,
    window: CoverageWindow,
    raw_runs: Sequence[Mapping[str, Any]],
    raw_conflicts: Sequence[Mapping[str, Any]],
    max_findings: int,
) -> dict[str, Any]:
    if max_findings <= 0:
        raise SuspendCoverageError("max_findings must be positive")
    runs = [_serialize_run(row) for row in raw_runs]
    conflicts = [
        {
            "ts_code": str(row["ts_code"]),
            "trade_date": _as_date(row["trade_date"], field="conflict_trade_date").isoformat(),
            "classification": FULL_DAY_SUSPEND_WITH_DAILY,
        }
        for row in raw_conflicts
    ]
    unresolved = [row for row in runs if row["classification"] != FULLY_COVERED]
    classification_counts: dict[str, int] = {}
    for row in runs:
        key = str(row["classification"])
        classification_counts[key] = classification_counts.get(key, 0) + 1
    if conflicts:
        classification_counts[FULL_DAY_SUSPEND_WITH_DAILY] = len(conflicts)

    coverage_complete = not unresolved and not conflicts
    return {
        "schema_version": SCHEMA_VERSION,
        "window": {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "trading_day_count": window.trading_day_count,
        },
        "semantics": {
            "universe": "listed_a_share_shsz_by_stock_basic_pit_boundaries",
            "full_day_suspend": "suspend_type=S and suspend_timing is null_or_blank",
            "intraday_suspend_is_full_day_authority": False,
            "missing_market_data_implies_suspension": False,
            "minute_cross_check_scope": (
                "uncovered_runs_with_a_daily_baseline_inside_the_audit_window; "
                "targeted probes are capped at 30 trading days; skipped runs remain "
                "unresolved_without_minute_inference"
            ),
            "database_write_performed": False,
        },
        "summary": {
            "coverage_complete": coverage_complete,
            "run_count": len(runs),
            "covered_run_count": len(runs) - len(unresolved),
            "unresolved_run_count": len(unresolved),
            "unresolved_symbol_count": len({row["ts_code"] for row in unresolved}),
            "missing_trade_day_count": sum(row["missing_days"] for row in runs),
            "full_day_suspend_covered_day_count": sum(
                row["full_day_suspend_days"] for row in runs
            ),
            "uncovered_day_count": sum(row["uncovered_days"] for row in unresolved),
            "full_day_suspend_price_conflict_count": len(conflicts),
            "classification_counts": dict(sorted(classification_counts.items())),
        },
        "unresolved_runs": unresolved[:max_findings],
        "unresolved_runs_truncated": len(unresolved) > max_findings,
        "full_day_suspend_price_conflicts": conflicts[:max_findings],
        "full_day_suspend_price_conflicts_truncated": len(conflicts) > max_findings,
        "database_write_performed": False,
    }


def audit_suspend_d_coverage(
    conn: Any,
    *,
    end_date: dt.date,
    start_date: dt.date | None = None,
    lookback_trading_days: int | None = None,
    max_findings: int = 500,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> dict[str, Any]:
    """Run the audit in a read-only transaction and return a compact receipt."""

    if statement_timeout_ms <= 0:
        raise SuspendCoverageError("statement_timeout_ms must be positive")
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SET LOCAL statement_timeout = %s", (statement_timeout_ms,))
    window = resolve_coverage_window(
        conn,
        start_date=start_date,
        end_date=end_date,
        lookback_trading_days=lookback_trading_days,
    )
    runs = _fetch_missing_runs(conn, window)
    conflicts = _fetch_full_day_suspend_price_conflicts(conn, window)
    return build_coverage_receipt(
        window=window,
        raw_runs=runs,
        raw_conflicts=conflicts,
        max_findings=max_findings,
    )
