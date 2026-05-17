"""Build PIT stock-universe spans from local ST/listing events.

The generated spans are eligibility ranges for new buys / selection. They do
not delete feature data. Rules are intentionally conservative:

- SH/SZ only; BJ/BSE are excluded.
- IPO eligibility starts after ``list_date + ipo_filter_days``.
- Negative ST events exclude from the first trading day after ``pub_date``.
- ST removal/recovery can re-enter from
  ``max(next_trading_day(pub_date), first_trading_day_on_or_after(imp_date))``.
- The default ``st_only_active`` scope does not implement full delisting /
  paused listing PIT. It uses the SH/SZ stock list active as of the requested
  generation end date and applies ST PIT exits/restores.
- Existing ST status on the universe start date is seeded from
  ``market.stock_st`` so stocks already under ST before the event backfill
  window are not allowed until a later recovery event.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_UNIVERSE_KEY = "shsz_st_pit_active_v1"
DEFAULT_RULE_VERSION = "st_pub_next_trade_restore_active_l_v1"
DEFAULT_SCOPE = "st_only_active"
SUPPORTED_SCOPES = {"st_only_active", "legacy_delist_pause_pit"}

RESTORE_KEYWORDS = ("撤销", "撤消")
RESTORE_STILL_RISK_KEYWORDS = ("并实行ST", "并实施ST", "实行ST", "实施ST", "变为ST", "转为ST", "叠加")
TERMINAL_KEYWORDS = ("退市整理期", "终止上市", "摘牌")


@dataclass(frozen=True)
class StockRow:
    ts_code: str
    name: str | None
    exchange: str | None
    list_status: str | None
    list_date: dt.date | None
    delist_date: dt.date | None


@dataclass(frozen=True)
class EventRow:
    ts_code: str
    event_kind: str
    action_date: dt.date
    source: str
    source_pub_date: dt.date | None = None
    source_imp_date: dt.date | None = None
    source_effective_date: dt.date | None = None
    st_type: str | None = None
    st_reason: str | None = None
    st_explain: str | None = None
    terminal: bool = False
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class SpanRow:
    universe_key: str
    ts_code: str
    eligible_start: dt.date
    eligible_end: dt.date
    entry_reason: str
    exit_reason: str
    base_list_date: dt.date | None
    ipo_eligible_date: dt.date | None
    entry_event_date: dt.date | None = None
    exit_event_date: dt.date | None = None
    terminal_exit: bool = False
    metadata: dict[str, Any] | None = None


def _parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    return dt.date.fromisoformat(str(value)[:10])


def _db_config() -> dict[str, Any]:
    # Keep explicit caller-provided DB targets intact. DEV/side-port validation
    # sets TDX_DB_* before importing this builder; overriding here can redirect
    # a safe validation rebuild back to the default production .env.
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    return {
        "host": os.getenv("TDX_DB_HOST", "localhost"),
        "port": int(os.getenv("TDX_DB_PORT", "5432")),
        "user": os.getenv("TDX_DB_USER", "postgres"),
        "password": os.getenv("TDX_DB_PASSWORD", ""),
        "dbname": os.getenv("TDX_DB_NAME", "aistock"),
    }


def _ensure_tables(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS market;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market.stock_universe_pit_spans (
                universe_key      TEXT NOT NULL,
                ts_code           TEXT NOT NULL,
                eligible_start    DATE NOT NULL,
                eligible_end      DATE NOT NULL,
                entry_reason      TEXT NOT NULL,
                exit_reason       TEXT NOT NULL,
                base_list_date    DATE,
                ipo_eligible_date DATE,
                entry_event_date  DATE,
                exit_event_date   DATE,
                terminal_exit     BOOLEAN NOT NULL DEFAULT FALSE,
                rule_version      TEXT NOT NULL,
                generated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (universe_key, ts_code, eligible_start, eligible_end)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market.stock_universe_pit_events (
                event_id              BIGSERIAL PRIMARY KEY,
                universe_key          TEXT NOT NULL,
                ts_code               TEXT NOT NULL,
                event_kind            TEXT NOT NULL,
                action_date           DATE NOT NULL,
                source                TEXT NOT NULL,
                source_pub_date       DATE,
                source_imp_date       DATE,
                source_effective_date DATE,
                st_type               TEXT,
                st_reason             TEXT,
                st_explain            TEXT,
                terminal              BOOLEAN NOT NULL DEFAULT FALSE,
                rule_version          TEXT NOT NULL,
                generated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                metadata              JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            """
        )
        comments = {
            "stock_universe_pit_spans": "PIT stock selection eligibility spans generated from local listing/ST events; feature data is not deleted",
            "stock_universe_pit_events": "Classified PIT universe events used to generate stock selection eligibility spans",
        }
        for table, comment in comments.items():
            cur.execute(f"COMMENT ON TABLE market.{table} IS %s;", (comment,))
        span_comments = {
            "universe_key": "Logical universe identifier, e.g. shsz_pit_v1",
            "ts_code": "Tushare stock code, e.g. 000001.SZ",
            "eligible_start": "Inclusive first date allowed for new selection/buy",
            "eligible_end": "Inclusive last date allowed for new selection/buy",
            "entry_reason": "Reason this eligibility span starts, e.g. ipo_365d or st_restore",
            "exit_reason": "Reason this eligibility span ends, e.g. st_negative, terminal_exit, or generation_end",
            "base_list_date": "Listing date from market.stock_basic",
            "ipo_eligible_date": "First eligible trading date after IPO warm-up rule",
            "entry_event_date": "PIT event action date that reopened eligibility, if any",
            "exit_event_date": "PIT event action date that closed eligibility, if any",
            "terminal_exit": "True when the span ends because the stock is terminally removed from phase-1 universe",
            "rule_version": "Universe generation rule version",
            "generated_at": "Local generation timestamp",
            "metadata": "Additional structured trace metadata",
        }
        event_comments = {
            "event_id": "Surrogate event row id",
            "universe_key": "Logical universe identifier, e.g. shsz_pit_v1",
            "ts_code": "Tushare stock code, e.g. 000001.SZ",
            "event_kind": "Classified event kind: st_negative, st_restore, delist_event, delisted, or paused_listing",
            "action_date": "PIT date when this event affects new selection/buy eligibility",
            "source": "Source table/interface that produced this event",
            "source_pub_date": "Original announcement publication date when available",
            "source_imp_date": "Original implementation date when available",
            "source_effective_date": "Original terminal effective date when available",
            "st_type": "Tushare st_type mapped from upstream st_tpye",
            "st_reason": "Tushare ST change reason",
            "st_explain": "Tushare ST detailed reason",
            "terminal": "True when this event permanently removes the stock in phase 1",
            "rule_version": "Universe generation rule version",
            "generated_at": "Local generation timestamp",
            "metadata": "Additional structured trace metadata",
        }
        for col, comment in span_comments.items():
            cur.execute(f"COMMENT ON COLUMN market.stock_universe_pit_spans.{col} IS %s;", (comment,))
        for col, comment in event_comments.items():
            cur.execute(f"COMMENT ON COLUMN market.stock_universe_pit_events.{col} IS %s;", (comment,))
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stock_universe_pit_spans_ts_code
                ON market.stock_universe_pit_spans (universe_key, ts_code, eligible_start);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stock_universe_pit_events_ts_code
                ON market.stock_universe_pit_events (universe_key, ts_code, action_date);
            """
        )
        cur.execute(
            """
            INSERT INTO market.data_stats_config
                (data_kind, table_name, date_column, updated_column, enabled, extra_info)
            VALUES (
                'stock_universe_pit_spans',
                'market.stock_universe_pit_spans',
                'eligible_end',
                'generated_at',
                TRUE,
                jsonb_build_object(
                    'desc', 'PIT stock selection eligibility spans',
                    'rule_version', %s,
                    'date_sequence', 'trading',
                    'source', 'stock_st_events+stock_basic',
                    'is_timeseries', false,
                    'coverage_start_column', 'eligible_start',
                    'coverage_end_column', 'eligible_end'
                )
            )
            ON CONFLICT (data_kind) DO UPDATE
                SET table_name = EXCLUDED.table_name,
                    date_column = EXCLUDED.date_column,
                    updated_column = EXCLUDED.updated_column,
                    enabled = EXCLUDED.enabled,
                    extra_info = EXCLUDED.extra_info;
            """,
            (DEFAULT_RULE_VERSION,),
        )
        cur.execute(
            """
            INSERT INTO market.data_stats_config
                (data_kind, table_name, date_column, updated_column, enabled, extra_info)
            VALUES (
                'stock_universe_pit_events',
                'market.stock_universe_pit_events',
                'action_date',
                'generated_at',
                TRUE,
                jsonb_build_object(
                    'desc', 'Classified ST PIT universe events',
                    'rule_version', %s,
                    'date_sequence', 'trading',
                    'source', 'stock_st_events+stock_st',
                    'is_timeseries', false
                )
            )
            ON CONFLICT (data_kind) DO UPDATE
                SET table_name = EXCLUDED.table_name,
                    date_column = EXCLUDED.date_column,
                    updated_column = EXCLUDED.updated_column,
                    enabled = EXCLUDED.enabled,
                    extra_info = EXCLUDED.extra_info;
            """,
            (DEFAULT_RULE_VERSION,),
        )


def _load_trading_days(conn: Any, start_date: dt.date, end_date: dt.date) -> list[dt.date]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cal_date::date
              FROM market.trading_calendar
             WHERE is_trading = TRUE
               AND cal_date BETWEEN %s AND %s
             ORDER BY cal_date
            """,
            (start_date, end_date),
        )
        return [row[0] for row in cur.fetchall()]


class TradingCalendar:
    def __init__(self, days: list[dt.date]):
        if not days:
            raise RuntimeError("trading_calendar has no trading days for requested range")
        self.days = days

    def on_or_after(self, value: dt.date) -> dt.date | None:
        for day in self.days:
            if day >= value:
                return day
        return None

    def after(self, value: dt.date) -> dt.date | None:
        for day in self.days:
            if day > value:
                return day
        return None

    def before(self, value: dt.date) -> dt.date | None:
        prev: dt.date | None = None
        for day in self.days:
            if day >= value:
                return prev
            prev = day
        return prev


def _load_stock_basic(conn: Any, *, active_only: bool = False, active_as_of: dt.date | None = None) -> list[StockRow]:
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        status_filter = ""
        params: list[Any] = []
        if active_only and active_as_of is not None:
            status_filter = """
               AND (list_date IS NULL OR list_date::date <= %s)
               AND (delist_date IS NULL OR delist_date::date > %s)
            """
            params.extend([active_as_of, active_as_of])
        elif active_only:
            status_filter = "AND COALESCE(list_status, '') = 'L'"
        cur.execute(
            f"""
            SELECT ts_code, name, exchange, list_status, list_date::date, delist_date::date
              FROM market.stock_basic
             WHERE exchange IN ('SSE', 'SZSE')
               AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
               {status_filter}
             ORDER BY ts_code
            """,
            tuple(params),
        )
        rows = []
        for row in cur.fetchall():
            rows.append(
                StockRow(
                    ts_code=str(row["ts_code"]),
                    name=row.get("name"),
                    exchange=row.get("exchange"),
                    list_status=row.get("list_status"),
                    list_date=row.get("list_date"),
                    delist_date=row.get("delist_date"),
                )
            )
        return rows


def _classify_st_event(
    st_type: str | None,
    reason: str | None,
    explain: str | None,
    name: str | None = None,
    *,
    terminal_as_negative: bool = False,
) -> tuple[str, bool]:
    text = " ".join([st_type or "", reason or "", explain or ""])
    if any(keyword in text for keyword in TERMINAL_KEYWORDS):
        if terminal_as_negative:
            return "st_negative", False
        return "delist_event", True
    has_restore = any(keyword in (st_type or "") for keyword in RESTORE_KEYWORDS)
    name_still_risky = bool(name and (name.startswith("ST") or name.startswith("*ST")))
    still_risky = (
        any(keyword in (st_type or "") for keyword in RESTORE_STILL_RISK_KEYWORDS)
        or "保持不变" in text
        or name_still_risky
    )
    if has_restore and not still_risky:
        return "st_restore", False
    return "st_negative", False


def _load_st_events(
    conn: Any,
    calendar: TradingCalendar,
    end_date: dt.date,
    *,
    terminal_as_negative: bool = False,
) -> list[EventRow]:
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ts_code, name, pub_date::date, imp_date::date, st_type, st_reason, st_explain
              FROM market.stock_st_events
             WHERE pub_date <= %s
             ORDER BY ts_code, pub_date, imp_date, st_type
            """,
            (end_date,),
        )
        events: list[EventRow] = []
        for row in cur.fetchall():
            pub_date = row["pub_date"]
            imp_date = row["imp_date"]
            event_kind, terminal = _classify_st_event(
                row.get("st_type"),
                row.get("st_reason"),
                row.get("st_explain"),
                row.get("name"),
                terminal_as_negative=terminal_as_negative,
            )
            visible_date = calendar.after(pub_date)
            if visible_date is None:
                continue
            if event_kind == "st_restore":
                restore_base = max(visible_date, imp_date) if imp_date else visible_date
                action_date = calendar.on_or_after(restore_base)
            else:
                action_date = visible_date
            if action_date is None:
                continue
            events.append(
                EventRow(
                    ts_code=str(row["ts_code"]),
                    event_kind=event_kind,
                    action_date=action_date,
                    source="market.stock_st_events",
                    source_pub_date=pub_date,
                    source_imp_date=imp_date,
                    st_type=row.get("st_type"),
                    st_reason=row.get("st_reason"),
                    st_explain=row.get("st_explain"),
                    terminal=terminal,
                    metadata={"visible_date": visible_date.isoformat(), "event_name": row.get("name")},
                )
            )
        return events


def _load_initial_st_events(
    conn: Any,
    calendar: TradingCalendar,
    start_date: dt.date,
    *,
    active_only: bool = False,
    active_as_of: dt.date | None = None,
) -> list[EventRow]:
    action_date = calendar.on_or_after(start_date)
    if action_date is None:
        return []
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        status_filter = ""
        params: list[Any] = [start_date]
        if active_only and active_as_of is not None:
            status_filter = """
               AND (b.list_date IS NULL OR b.list_date::date <= %s)
               AND (b.delist_date IS NULL OR b.delist_date::date > %s)
            """
            params.extend([active_as_of, active_as_of])
        elif active_only:
            status_filter = "AND COALESCE(b.list_status, '') = 'L'"
        cur.execute(
            f"""
            SELECT DISTINCT s.ts_code
              FROM market.stock_st s
              JOIN market.stock_basic b ON b.ts_code = s.ts_code
             WHERE s.ann_date = %s
               AND b.exchange IN ('SSE', 'SZSE')
               AND (s.ts_code LIKE '%%.SH' OR s.ts_code LIKE '%%.SZ')
                {status_filter}
             ORDER BY s.ts_code
            """,
            tuple(params),
        )
        return [
            EventRow(
                ts_code=str(row["ts_code"]),
                event_kind="st_negative",
                action_date=action_date,
                source="market.stock_st_initial",
                source_effective_date=start_date,
                terminal=False,
                metadata={"seed_reason": "already_st_on_universe_start"},
            )
            for row in cur.fetchall()
        ]


def _stock_basic_terminal_events(stocks: Iterable[StockRow], calendar: TradingCalendar) -> list[EventRow]:
    events: list[EventRow] = []
    for stock in stocks:
        status = (stock.list_status or "").upper()
        if status == "D" and stock.delist_date:
            action_date = calendar.on_or_after(stock.delist_date)
            if action_date:
                events.append(
                    EventRow(
                        ts_code=stock.ts_code,
                        event_kind="delisted",
                        action_date=action_date,
                        source="market.stock_basic",
                        source_effective_date=stock.delist_date,
                        terminal=True,
                        metadata={"list_status": status},
                    )
                )
        elif status == "P":
            effective = stock.delist_date or stock.list_date
            if effective:
                action_date = calendar.on_or_after(effective)
                if action_date:
                    events.append(
                        EventRow(
                            ts_code=stock.ts_code,
                            event_kind="paused_listing",
                            action_date=action_date,
                            source="market.stock_basic",
                            source_effective_date=effective,
                            terminal=True,
                            metadata={"list_status": status},
                        )
                    )
    return events


def _load_stock_basic_scope_counts(conn: Any, *, active_as_of: dt.date | None = None) -> dict[str, int]:
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT COALESCE(list_status, '') AS list_status, COUNT(*) AS cnt
              FROM market.stock_basic
             WHERE exchange IN ('SSE', 'SZSE')
               AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
             GROUP BY COALESCE(list_status, '')
            """
        )
        counts = {str(row["list_status"] or "UNKNOWN"): int(row["cnt"]) for row in cur.fetchall()}
        asof_counts: dict[str, int] = {}
        if active_as_of is not None:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE (list_date IS NULL OR list_date::date <= %(active_as_of)s)
                          AND (delist_date IS NULL OR delist_date::date > %(active_as_of)s)
                    ) AS active_asof,
                    COUNT(*) FILTER (WHERE delist_date IS NOT NULL AND delist_date::date <= %(active_as_of)s)
                        AS delisted_asof
                  FROM market.stock_basic
                 WHERE exchange IN ('SSE', 'SZSE')
                   AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
                """,
                {"active_as_of": active_as_of},
            )
            row = dict(cur.fetchone() or {})
            asof_counts = {
                "active_asof": int(row.get("active_asof") or 0),
                "delisted_asof": int(row.get("delisted_asof") or 0),
            }
    return {
        "active_l": counts.get("L", 0),
        "delisted_d": counts.get("D", 0),
        "paused_p": counts.get("P", 0),
        "other": sum(v for k, v in counts.items() if k not in {"L", "D", "P"}),
        "total": sum(counts.values()),
        **asof_counts,
    }


def _build_spans(
    stocks: list[StockRow],
    events: list[EventRow],
    *,
    universe_key: str,
    start_date: dt.date,
    end_date: dt.date,
    ipo_filter_days: int,
    calendar: TradingCalendar,
) -> list[SpanRow]:
    events_by_stock: dict[str, list[EventRow]] = defaultdict(list)
    for event in events:
        events_by_stock[event.ts_code].append(event)
    for values in events_by_stock.values():
        values.sort(key=lambda e: (e.action_date, 0 if e.terminal else 1 if e.event_kind != "st_restore" else 2))

    spans: list[SpanRow] = []
    for stock in stocks:
        if not stock.list_date:
            continue
        ipo_base = stock.list_date + dt.timedelta(days=ipo_filter_days)
        ipo_eligible = calendar.on_or_after(ipo_base)
        if ipo_eligible is None:
            continue
        current_start = max(ipo_eligible, start_date)
        eligible = current_start <= end_date
        entry_reason = "ipo_365d" if ipo_filter_days == 365 else f"ipo_{ipo_filter_days}d"
        entry_event_date: dt.date | None = None
        terminal = False

        for event in events_by_stock.get(stock.ts_code, []):
            if event.action_date < current_start and eligible:
                # Historical events before this universe window can still define state.
                if event.event_kind in {"st_negative", "delist_event", "delisted", "paused_listing"}:
                    eligible = False
                    terminal = event.terminal
                elif event.event_kind == "st_restore" and not terminal:
                    eligible = True
                continue
            if event.action_date > end_date:
                break
            if event.event_kind in {"st_negative", "delist_event", "delisted", "paused_listing"}:
                if eligible:
                    span_end = calendar.before(event.action_date)
                    if span_end and current_start <= span_end:
                        spans.append(
                            SpanRow(
                                universe_key=universe_key,
                                ts_code=stock.ts_code,
                                eligible_start=current_start,
                                eligible_end=span_end,
                                entry_reason=entry_reason,
                                exit_reason=event.event_kind,
                                base_list_date=stock.list_date,
                                ipo_eligible_date=ipo_eligible,
                                entry_event_date=entry_event_date,
                                exit_event_date=event.action_date,
                                terminal_exit=event.terminal,
                                metadata={"stock_name": stock.name, "exchange": stock.exchange},
                            )
                        )
                eligible = False
                if event.terminal:
                    terminal = True
                    break
            elif event.event_kind == "st_restore" and not terminal and not eligible:
                current_start = max(event.action_date, ipo_eligible, start_date)
                if current_start <= end_date:
                    eligible = True
                    entry_reason = "st_restore"
                    entry_event_date = event.action_date

        if eligible and not terminal and current_start <= end_date:
            spans.append(
                SpanRow(
                    universe_key=universe_key,
                    ts_code=stock.ts_code,
                    eligible_start=current_start,
                    eligible_end=end_date,
                    entry_reason=entry_reason,
                    exit_reason="generation_end",
                    base_list_date=stock.list_date,
                    ipo_eligible_date=ipo_eligible,
                    entry_event_date=entry_event_date,
                    exit_event_date=None,
                    terminal_exit=False,
                    metadata={"stock_name": stock.name, "exchange": stock.exchange},
                )
            )
    return spans


def _validate(spans: list[SpanRow], events: list[EventRow]) -> dict[str, Any]:
    spans_by_stock: dict[str, list[SpanRow]] = defaultdict(list)
    for span in spans:
        spans_by_stock[span.ts_code].append(span)
    invalid_spans = [
        {"ts_code": s.ts_code, "start": s.eligible_start.isoformat(), "end": s.eligible_end.isoformat()}
        for s in spans
        if s.eligible_start > s.eligible_end
    ]
    overlap_errors = []
    for ts_code, values in spans_by_stock.items():
        values.sort(key=lambda s: s.eligible_start)
        for prev, curr in zip(values, values[1:]):
            if curr.eligible_start <= prev.eligible_end:
                overlap_errors.append(
                    {
                        "ts_code": ts_code,
                        "prev": [prev.eligible_start.isoformat(), prev.eligible_end.isoformat()],
                        "curr": [curr.eligible_start.isoformat(), curr.eligible_end.isoformat()],
                    }
                )

    event_action_violations = []
    terminal_reentry_violations = []
    for event in events:
        if event.event_kind in {"st_negative", "delist_event", "delisted", "paused_listing"}:
            for span in spans_by_stock.get(event.ts_code, []):
                if span.eligible_start <= event.action_date <= span.eligible_end:
                    event_action_violations.append(
                        {
                            "ts_code": event.ts_code,
                            "event_kind": event.event_kind,
                            "action_date": event.action_date.isoformat(),
                            "span": [span.eligible_start.isoformat(), span.eligible_end.isoformat()],
                        }
                    )
        if event.terminal:
            for span in spans_by_stock.get(event.ts_code, []):
                if span.eligible_start >= event.action_date:
                    terminal_reentry_violations.append(
                        {
                            "ts_code": event.ts_code,
                            "event_kind": event.event_kind,
                            "action_date": event.action_date.isoformat(),
                            "span": [span.eligible_start.isoformat(), span.eligible_end.isoformat()],
                        }
                    )

    return {
        "invalid_spans": invalid_spans[:100],
        "invalid_span_count": len(invalid_spans),
        "overlap_errors": overlap_errors[:100],
        "overlap_error_count": len(overlap_errors),
        "event_action_violations": event_action_violations[:100],
        "event_action_violation_count": len(event_action_violations),
        "terminal_reentry_violations": terminal_reentry_violations[:100],
        "terminal_reentry_violation_count": len(terminal_reentry_violations),
    }


def _write_results(
    conn: Any,
    *,
    universe_key: str,
    rule_version: str,
    spans: list[SpanRow],
    events: list[EventRow],
) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM market.stock_universe_pit_spans WHERE universe_key = %s", (universe_key,))
        cur.execute("DELETE FROM market.stock_universe_pit_events WHERE universe_key = %s", (universe_key,))
        _insert_events(cur, universe_key=universe_key, rule_version=rule_version, events=events)
        _insert_spans(cur, rule_version=rule_version, spans=spans)


def _insert_events(
    cur: Any,
    *,
    universe_key: str,
    rule_version: str,
    events: list[EventRow],
) -> None:
    if not events:
        return
    pgx.execute_values(
        cur,
        """
        INSERT INTO market.stock_universe_pit_events (
            universe_key, ts_code, event_kind, action_date, source,
            source_pub_date, source_imp_date, source_effective_date,
            st_type, st_reason, st_explain, terminal, rule_version, metadata
        ) VALUES %s
        """,
        [
            (
                universe_key,
                e.ts_code,
                e.event_kind,
                e.action_date,
                e.source,
                e.source_pub_date,
                e.source_imp_date,
                e.source_effective_date,
                e.st_type,
                e.st_reason,
                e.st_explain,
                e.terminal,
                rule_version,
                pgx.Json(e.metadata or {}),
            )
            for e in events
        ],
    )


def _insert_spans(
    cur: Any,
    *,
    rule_version: str,
    spans: list[SpanRow],
) -> None:
    if not spans:
        return
    pgx.execute_values(
        cur,
        """
        INSERT INTO market.stock_universe_pit_spans (
            universe_key, ts_code, eligible_start, eligible_end,
            entry_reason, exit_reason, base_list_date, ipo_eligible_date,
            entry_event_date, exit_event_date, terminal_exit,
            rule_version, metadata
        ) VALUES %s
        ON CONFLICT (universe_key, ts_code, eligible_start, eligible_end) DO UPDATE
            SET entry_reason = EXCLUDED.entry_reason,
                exit_reason = EXCLUDED.exit_reason,
                base_list_date = EXCLUDED.base_list_date,
                ipo_eligible_date = EXCLUDED.ipo_eligible_date,
                entry_event_date = EXCLUDED.entry_event_date,
                exit_event_date = EXCLUDED.exit_event_date,
                terminal_exit = EXCLUDED.terminal_exit,
                rule_version = EXCLUDED.rule_version,
                generated_at = NOW(),
                metadata = EXCLUDED.metadata
        """,
        [
            (
                s.universe_key,
                s.ts_code,
                s.eligible_start,
                s.eligible_end,
                s.entry_reason,
                s.exit_reason,
                s.base_list_date,
                s.ipo_eligible_date,
                s.entry_event_date,
                s.exit_event_date,
                s.terminal_exit,
                rule_version,
                pgx.Json(s.metadata or {}),
            )
            for s in spans
        ],
    )


def _write_incremental_extension(
    conn: Any,
    *,
    universe_key: str,
    rule_version: str,
    spans: list[SpanRow],
    events: list[EventRow],
    incremental_from: dt.date,
) -> dict[str, int]:
    """Extend an existing PIT universe without deleting historical coverage."""
    previous_end = incremental_from - dt.timedelta(days=1)
    delta_spans = [
        span
        for span in spans
        if span.eligible_end >= incremental_from
        or (span.exit_event_date is not None and span.exit_event_date >= incremental_from)
    ]
    delta_events = [event for event in events if event.action_date >= incremental_from]
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM market.stock_universe_pit_spans
             WHERE universe_key = %s
               AND (
                    eligible_start >= %s
                    OR (exit_reason = 'generation_end' AND eligible_end = %s)
               )
            """,
            (universe_key, incremental_from, previous_end),
        )
        deleted_spans = cur.rowcount
        cur.execute(
            """
            DELETE FROM market.stock_universe_pit_events
             WHERE universe_key = %s
               AND action_date >= %s
            """,
            (universe_key, incremental_from),
        )
        deleted_events = cur.rowcount
        _insert_events(cur, universe_key=universe_key, rule_version=rule_version, events=delta_events)
        _insert_spans(cur, rule_version=rule_version, spans=delta_spans)
    return {
        "deleted_spans": int(deleted_spans),
        "deleted_events": int(deleted_events),
        "inserted_or_updated_spans": len(delta_spans),
        "inserted_events": len(delta_events),
    }


def _write_report(report_path: Path, summary: dict[str, Any]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, ensure_ascii=False, default=str, indent=2), encoding="utf-8")


def _write_all_txt(path: Path, spans: Iterable[SpanRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"{span.ts_code}\t{span.eligible_start.isoformat()}\t{span.eligible_end.isoformat()}"
        for span in sorted(spans, key=lambda s: (s.ts_code, s.eligible_start, s.eligible_end))
    ]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def build(args: argparse.Namespace) -> dict[str, Any]:
    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date) or dt.date.today()
    write_mode = getattr(args, "write_mode", "replace") or "replace"
    incremental_from = _parse_date(getattr(args, "incremental_from", None))
    if start_date is None:
        raise RuntimeError("--start-date is required")
    if start_date > end_date:
        raise RuntimeError("--start-date must be <= --end-date")
    if write_mode not in {"replace", "incremental"}:
        raise RuntimeError(f"unsupported write_mode: {write_mode}")
    if write_mode == "incremental":
        if incremental_from is None:
            raise RuntimeError("--incremental-from is required when write_mode=incremental")
        if incremental_from < start_date or incremental_from > end_date:
            raise RuntimeError("--incremental-from must be within [--start-date, --end-date]")
    rule_version = args.rule_version or DEFAULT_RULE_VERSION
    scope = getattr(args, "scope", DEFAULT_SCOPE) or DEFAULT_SCOPE
    if scope not in SUPPORTED_SCOPES:
        raise RuntimeError(f"unsupported --scope: {scope}")
    st_only_active = scope == "st_only_active"

    with psycopg2.connect(**_db_config()) as conn:
        _ensure_tables(conn)
        calendar_days = _load_trading_days(
            conn,
            start_date - dt.timedelta(days=max(args.ipo_filter_days + 31, 400)),
            end_date + dt.timedelta(days=31),
        )
        calendar = TradingCalendar(calendar_days)
        scope_counts = _load_stock_basic_scope_counts(conn, active_as_of=end_date)
        stocks = _load_stock_basic(conn, active_only=st_only_active, active_as_of=end_date)
        initial_st_events = _load_initial_st_events(
            conn,
            calendar,
            start_date,
            active_only=st_only_active,
            active_as_of=end_date,
        )
        st_events = _load_st_events(conn, calendar, end_date, terminal_as_negative=st_only_active)
        terminal_events = [] if st_only_active else _stock_basic_terminal_events(stocks, calendar)
        events = sorted(
            initial_st_events + st_events + terminal_events,
            key=lambda e: (e.ts_code, e.action_date, e.event_kind),
        )
        spans = _build_spans(
            stocks,
            events,
            universe_key=args.universe_key,
            start_date=start_date,
            end_date=end_date,
            ipo_filter_days=args.ipo_filter_days,
            calendar=calendar,
        )
        validation = _validate(spans, events)
        event_counts = Counter(event.event_kind for event in events)
        span_counts = Counter(span.exit_reason for span in spans)
        spans_by_code: dict[str, list[SpanRow]] = defaultdict(list)
        for span in spans:
            spans_by_code[span.ts_code].append(span)
        multi_span_codes = sorted(code for code, values in spans_by_code.items() if len(values) > 1)
        sample_multi_span = {
            code: [
                {
                    "start": span.eligible_start.isoformat(),
                    "end": span.eligible_end.isoformat(),
                    "entry_reason": span.entry_reason,
                    "exit_reason": span.exit_reason,
                }
                for span in spans_by_code[code][:5]
            ]
            for code in multi_span_codes[:10]
        }
        summary = {
            "universe_key": args.universe_key,
            "rule_version": rule_version,
            "scope": scope,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "ipo_filter_days": args.ipo_filter_days,
            "write_mode": write_mode,
            "incremental_from": incremental_from.isoformat() if incremental_from else None,
            "st_pit": True,
            "delist_pit": not st_only_active,
            "pause_pit": not st_only_active,
            "survivorship_bias": (
                "D/P stocks as of generation end are excluded by ST-only active universe scope; delisting PIT is not implemented"
                if st_only_active
                else "legacy delist/pause PIT terminal events enabled"
            ),
            "counts": {
                "stock_basic_shsz": len(stocks),
                "stock_basic_scope_counts": scope_counts,
                "asof_D_P_excluded_count": (
                    scope_counts["delisted_d"] + scope_counts["paused_p"] if st_only_active else 0
                ),
                "events": len(events),
                "spans": len(spans),
                "eligible_instruments": len(spans_by_code),
                "multi_span_instruments": len(multi_span_codes),
                "event_counts": dict(event_counts),
                "span_exit_counts": dict(span_counts),
            },
            "validation": validation,
            "samples": {"multi_span_instruments": sample_multi_span},
        }
        if not args.dry_run:
            if write_mode == "incremental":
                summary["write_delta"] = _write_incremental_extension(
                    conn,
                    universe_key=args.universe_key,
                    rule_version=rule_version,
                    spans=spans,
                    events=events,
                    incremental_from=incremental_from,
                )
            else:
                _write_results(
                    conn,
                    universe_key=args.universe_key,
                    rule_version=rule_version,
                    spans=spans,
                    events=events,
                )
                summary["write_delta"] = {
                    "deleted_spans": "all",
                    "deleted_events": "all",
                    "inserted_or_updated_spans": len(spans),
                    "inserted_events": len(events),
                }
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SELECT market.refresh_data_stats();")
            conn.commit()

    report_dir = Path(args.reports_dir)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"{args.universe_key}_summary_{stamp}.json"
    _write_report(report_path, summary)
    if args.write_all_txt:
        all_txt_path = report_dir / f"{args.universe_key}_all_{stamp}.txt"
        _write_all_txt(all_txt_path, spans)
        summary["all_txt_path"] = str(all_txt_path)
        _write_report(report_path, summary)
    summary["report_path"] = str(report_path)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Build first-stage PIT universe spans")
    parser.add_argument("--universe-key", default=DEFAULT_UNIVERSE_KEY)
    parser.add_argument("--rule-version", default=DEFAULT_RULE_VERSION)
    parser.add_argument("--scope", choices=sorted(SUPPORTED_SCOPES), default=DEFAULT_SCOPE)
    parser.add_argument("--start-date", default="2018-08-01")
    parser.add_argument("--end-date", default=dt.date.today().isoformat())
    parser.add_argument("--ipo-filter-days", type=int, default=365)
    parser.add_argument("--reports-dir", default=str(PROJECT_ROOT / "reports" / "stock_universe_pit"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write-all-txt", action="store_true")
    parser.add_argument("--write-mode", choices=["replace", "incremental"], default="replace")
    parser.add_argument("--incremental-from")
    args = parser.parse_args()

    summary = build(args)
    print(json.dumps(summary, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
