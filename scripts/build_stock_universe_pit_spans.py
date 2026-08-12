"""Build PIT stock-universe spans from local ST/listing events.

The generated spans are eligibility ranges for new buys / selection. They do
not delete feature data. Rules are intentionally conservative:

- SH/SZ only; BJ/BSE are excluded.
- A-shares only; B-share boards (200/201xxx.SZ, 900xxx.SH) are permanently
  excluded from every generated universe (BUG-927).
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
from typing import Any, Iterable, Mapping

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_IPO_TRADING_SESSIONS,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SCOPE,
    CANONICAL_PIT_UNIVERSE_KEY,
    canonical_rule_parameters_digest,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_UNIVERSE_KEY = "shsz_st_pit_active_v1"
DEFAULT_RULE_VERSION = "st_pub_next_trade_restore_active_l_v1"
DEFAULT_SCOPE = "st_only_active"
SUPPORTED_SCOPES = {"st_only_active", "legacy_delist_pause_pit", CANONICAL_PIT_SCOPE}
SUPPORTED_IPO_FILTER_UNITS = {"calendar_days", "trading_sessions"}

RESTORE_KEYWORDS = ("撤销", "撤消")
RESTORE_STILL_RISK_KEYWORDS = ("并实行ST", "并实施ST", "实行ST", "实施ST", "变为ST", "转为ST", "叠加")
TERMINAL_KEYWORDS = ("退市整理期", "终止上市", "摘牌")

# BUG-927: AIstock has no B-share backtest or trading coverage, so B-share
# boards (Shenzhen B: 200/201xxx.SZ, Shanghai B: 900xxx.SH) must never enter
# any generated stock universe.  ``%%`` escaping matches the psycopg2 style
# used by every query in this module.
B_SHARE_TS_CODE_PATTERNS = ("200%%.SZ", "201%%.SZ", "900%%.SH")


def is_b_share_ts_code(ts_code: str) -> bool:
    """Return True when ``ts_code`` belongs to a B-share board."""
    code = str(ts_code or "").strip().upper()
    prefix, dot, suffix = code.partition(".")
    if not dot:
        return False
    for pattern in B_SHARE_TS_CODE_PATTERNS:
        pattern_prefix, _, pattern_suffix = pattern.replace("%%", "%").partition(".")
        if suffix == pattern_suffix and prefix.startswith(pattern_prefix.rstrip("%")):
            return True
    return False


def a_share_ts_code_filter(column: str = "ts_code") -> str:
    """SQL AND-fragment excluding B-share codes from ``column``."""
    return "".join(f" AND {column} NOT LIKE '{pattern}'" for pattern in B_SHARE_TS_CODE_PATTERNS)


class CanonicalPitEvidenceError(RuntimeError):
    code = "BLOCKED_CANONICAL_PIT_EVIDENCE_INCOMPLETE"

    def __init__(self, message: str, *, context: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.context = dict(context)


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
            CREATE INDEX IF NOT EXISTS idx_stock_universe_pit_spans_rule_version
                ON market.stock_universe_pit_spans (universe_key, rule_version);
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

    def after_sessions(self, value: dt.date, completed_sessions: int) -> dt.date | None:
        """Return the first session after ``completed_sessions`` have elapsed.

        A listed stock is not eligible during its first N exchange sessions.
        For a listing on ``days[0]`` and N=2, eligibility begins at ``days[2]``.
        """

        if completed_sessions < 0:
            raise ValueError("completed_sessions must be non-negative")
        sessions = [day for day in self.days if day >= value]
        if len(sessions) <= completed_sessions:
            return None
        return sessions[completed_sessions]


def reconstruct_missing_st_snapshot(
    previous_codes: Iterable[str],
    next_codes: Iterable[str],
    effective_events: Iterable[tuple[str, str]],
) -> frozenset[str]:
    """Close one or more missing ST snapshot days without guessing.

    The caller supplies every event effective inside the gap.  The derived
    state must equal the next observed snapshot; otherwise the source gap is
    ambiguous and candidate construction must stop.
    """

    state = {str(code).strip().upper() for code in previous_codes if str(code).strip()}
    for raw_code, action in effective_events:
        code = str(raw_code or "").strip().upper()
        if not code:
            raise RuntimeError("ST gap event has empty code")
        if action == "st_negative":
            state.add(code)
        elif action in {"st_restore", "terminal_exit"}:
            state.discard(code)
        else:
            raise RuntimeError(f"unsupported ST gap event action: {action!r}")
    expected = {str(code).strip().upper() for code in next_codes if str(code).strip()}
    if state != expected:
        missing_events = sorted(expected - state)
        unexplained_removals = sorted(state - expected)
        raise CanonicalPitEvidenceError(
            "ST snapshot gap cannot be closed by effective events: "
            f"missing_events={missing_events} unexplained_removals={unexplained_removals}",
            context={
                "missing_events": missing_events,
                "unexplained_removals": unexplained_removals,
            },
        )
    return frozenset(state)


def audit_st_snapshot_continuity(
    snapshots: Mapping[dt.date, Iterable[str]],
    *,
    trading_days: Iterable[dt.date],
    events: Iterable[EventRow],
) -> dict[str, Any]:
    """Prove every missing trading-day ST snapshot from adjacent snapshots.

    Snapshot gaps are accepted only when applying every effective ST/lifecycle
    event since the previous observed day reproduces the next observed day
    exactly. Boundary gaps cannot be proven and therefore fail closed.
    """

    days = tuple(sorted(set(trading_days)))
    if not days:
        raise RuntimeError("ST snapshot continuity requires trading days")
    day_set = set(days)
    normalized = {
        day: frozenset(str(code).strip().upper() for code in codes if str(code).strip())
        for day, codes in snapshots.items()
        if day in day_set
    }
    if days[0] not in normalized or days[-1] not in normalized:
        raise CanonicalPitEvidenceError(
            "ST snapshot boundary is missing and cannot be reconstructed without two observed anchors: "
            f"start_present={days[0] in normalized} end_present={days[-1] in normalized}",
            context={
                "start": days[0].isoformat(),
                "end": days[-1].isoformat(),
                "start_present": days[0] in normalized,
                "end_present": days[-1] in normalized,
            },
        )

    relevant_events = sorted(
        (
            event
            for event in events
            if event.event_kind in {
                "st_negative",
                "st_restore",
                "delisted",
                "paused_listing",
                "delisting_confirmed",
            }
        ),
        key=lambda event: (event.action_date, event.ts_code, event.event_kind),
    )
    observed_days = sorted(normalized)
    missing_days = [day for day in days if day not in normalized]
    reconstructed: list[dt.date] = []
    for previous_day, next_day in zip(observed_days, observed_days[1:]):
        gap_days = [day for day in days if previous_day < day < next_day]
        if not gap_days:
            continue
        gap_events = [
            (
                event.ts_code,
                "terminal_exit"
                if event.event_kind in {"delisted", "paused_listing", "delisting_confirmed"}
                else event.event_kind,
            )
            for event in relevant_events
            if previous_day < event.action_date <= next_day
        ]
        reconstruct_missing_st_snapshot(
            normalized[previous_day],
            normalized[next_day],
            gap_events,
        )
        reconstructed.extend(gap_days)
    if len(reconstructed) != len(missing_days):
        unresolved = sorted(set(missing_days).difference(reconstructed))
        raise CanonicalPitEvidenceError(
            f"ST snapshot gap has no adjacent observed anchors: {unresolved}",
            context={"unresolved_dates": [day.isoformat() for day in unresolved]},
        )
    return {
        "trading_day_count": len(days),
        "observed_snapshot_day_count": len(normalized),
        "missing_snapshot_day_count": len(missing_days),
        "reconstructed_snapshot_day_count": len(reconstructed),
        "missing_snapshot_dates": [day.isoformat() for day in missing_days],
        "status": "ready",
    }


def _ipo_eligible_date(
    *,
    list_date: dt.date,
    filter_value: int,
    filter_unit: str,
    calendar: TradingCalendar,
) -> dt.date | None:
    if filter_unit == "calendar_days":
        return calendar.on_or_after(list_date + dt.timedelta(days=filter_value))
    if filter_unit == "trading_sessions":
        return calendar.after_sessions(list_date, filter_value)
    raise RuntimeError(f"unsupported IPO filter unit: {filter_unit}")


def _canonical_calendar_start(
    stocks: Iterable[StockRow], *, start_date: dt.date, fallback_lookback_days: int
) -> dt.date:
    earliest_list_date = min((stock.list_date for stock in stocks if stock.list_date), default=start_date)
    return min(start_date - dt.timedelta(days=fallback_lookback_days), earliest_list_date)


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
               {a_share_ts_code_filter("ts_code")}
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
            f"""
            SELECT ts_code, name, pub_date::date, imp_date::date, st_type, st_reason, st_explain
              FROM market.stock_st_events
             WHERE pub_date <= %s
               {a_share_ts_code_filter("ts_code")}
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
               {a_share_ts_code_filter("s.ts_code")}
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


def _stock_basic_terminal_events(
    stocks: Iterable[StockRow],
    calendar: TradingCalendar,
    *,
    allow_paused_list_date_fallback: bool = True,
) -> list[EventRow]:
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
            # stock_basic has no dedicated historical pause date. Legacy mode
            # retains its old fallback, but canonical PIT must never reinterpret
            # the IPO listing date as the pause date.
            effective = stock.delist_date or (stock.list_date if allow_paused_list_date_fallback else None)
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


def _load_confirmed_delisting_events(conn: Any, calendar: TradingCalendar, end_date: dt.date) -> list[EventRow]:
    """Load announcement-as-of confirmed terminal events from the shared ledger."""

    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT ts_code, source_event_date::date, available_at, effective_trade_date::date,
                   COALESCE((available_at AT TIME ZONE 'Asia/Shanghai')::date, source_event_date::date)
                       AS known_date,
                   source_type, source_pk, reason, evidence
             FROM market.event_signal
             WHERE event_type = 'stock_delisting_confirmed'
               AND time_mode = 'backtest'
               AND signal_status IN ('ACTIVE', 'RESOLVED', 'EXPIRED')
               AND COALESCE((available_at AT TIME ZONE 'Asia/Shanghai')::date, source_event_date::date) <= %s
               {a_share_ts_code_filter("ts_code")}
             ORDER BY ts_code, effective_trade_date, available_at, signal_id
            """,
            (end_date,),
        )
        output: list[EventRow] = []
        for row in cur.fetchall():
            effective = row.get("effective_trade_date")
            available_at = row.get("available_at")
            known_date = row.get("known_date")
            if known_date is None:
                raise CanonicalPitEvidenceError(
                    "confirmed delisting event has no observable knowledge date",
                    context={"ts_code": str(row["ts_code"]), "source_pk": row.get("source_pk")},
                )
            if effective is None or effective < known_date:
                raise CanonicalPitEvidenceError(
                    "confirmed delisting effective_trade_date violates point-in-time ordering",
                    context={
                        "ts_code": str(row["ts_code"]),
                        "known_date": known_date.isoformat(),
                        "effective_trade_date": effective.isoformat() if effective else None,
                    },
                )
            # event_signal.effective_trade_date is the shared, leakage-safe
            # authority. It already accounts for exact publish time and the
            # pre-open cutoff; recomputing it here would drift from that rule.
            action_date = calendar.on_or_after(effective)
            if action_date is None:
                continue
            output.append(
                EventRow(
                    ts_code=str(row["ts_code"]),
                    event_kind="delisting_confirmed",
                    action_date=action_date,
                    source="market.event_signal",
                    source_pub_date=row.get("source_event_date"),
                    source_effective_date=effective,
                    terminal=True,
                    metadata={
                        "source_type": row.get("source_type"),
                        "source_pk": row.get("source_pk"),
                        "available_at": available_at.isoformat() if available_at else None,
                        "reason": row.get("reason"),
                        "evidence": row.get("evidence") or {},
                    },
                )
            )
    return output


def audit_canonical_stock_lifecycle(stocks: Iterable[StockRow]) -> dict[str, Any]:
    """Fail closed when stock_basic cannot support a historical PIT lifecycle."""

    rows = list(stocks)
    missing_list_date = sorted(stock.ts_code for stock in rows if stock.list_date is None)
    unsupported_status = sorted(
        stock.ts_code for stock in rows if str(stock.list_status or "").upper() not in {"L", "D", "P"}
    )
    delisted_missing_date = sorted(
        stock.ts_code
        for stock in rows
        if str(stock.list_status or "").upper() == "D" and stock.delist_date is None
    )
    unresolved = sorted(set(missing_list_date + unsupported_status + delisted_missing_date))
    ledger = {
        "schema_version": "canonical_pit_exception_ledger_v1",
        "stock_count": len(rows),
        "missing_list_date_count": len(missing_list_date),
        "missing_list_date_codes": missing_list_date,
        "unsupported_list_status_count": len(unsupported_status),
        "unsupported_list_status_codes": unsupported_status,
        "delisted_missing_date_count": len(delisted_missing_date),
        "delisted_missing_date_codes": delisted_missing_date,
        "unresolved_exception_count": len(unresolved),
        "unresolved_exception_codes": unresolved,
        "status": "ready" if not unresolved else "blocked",
    }
    if unresolved:
        raise CanonicalPitEvidenceError(
            f"canonical stock lifecycle evidence is incomplete: unresolved_count={len(unresolved)}",
            context=ledger,
        )
    return ledger


def audit_canonical_terminal_evidence(
    stocks: Iterable[StockRow],
    *,
    announcement_events: Iterable[EventRow],
    end_date: dt.date | None = None,
) -> dict[str, Any]:
    """Require announcement/event evidence for every historical D/P security."""

    terminal_codes = {
        event.ts_code
        for event in announcement_events
        if event.terminal
        and event.source != "market.stock_basic"
        and (end_date is None or event.action_date <= end_date)
    }
    required = sorted(
        stock.ts_code
        for stock in stocks
        if str(stock.list_status or "").upper() == "P"
        or (
            str(stock.list_status or "").upper() == "D"
            and (end_date is None or (stock.delist_date is not None and stock.delist_date <= end_date))
        )
    )
    missing = sorted(set(required).difference(terminal_codes))
    receipt = {
        "required_terminal_security_count": len(required),
        "announcement_terminal_evidence_count": len(set(required).intersection(terminal_codes)),
        "missing_terminal_evidence_count": len(missing),
        "missing_terminal_evidence_codes": missing,
        "status": "ready" if not missing else "blocked",
    }
    if missing:
        raise CanonicalPitEvidenceError(
            "canonical terminal announcement evidence is incomplete: "
            f"missing_count={len(missing)} missing_codes={missing}",
            context=receipt,
        )
    return receipt


def _audit_canonical_st_snapshots(
    conn: Any,
    *,
    calendar: TradingCalendar,
    start_date: dt.date,
    end_date: dt.date,
    events: Iterable[EventRow],
) -> dict[str, Any]:
    days = tuple(day for day in calendar.days if start_date <= day <= end_date)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ann_date::date
              FROM market.stock_st
             WHERE ann_date::date BETWEEN %s AND %s
               {a_share_ts_code_filter("ts_code")}
             ORDER BY ann_date
            """,
            (start_date, end_date),
        )
        observed = {row[0] for row in cur.fetchall()}
    if not days:
        raise RuntimeError("canonical ST snapshot audit requires trading days")
    if days[0] not in observed or days[-1] not in observed:
        raise CanonicalPitEvidenceError(
            "ST snapshot boundary is missing and cannot be reconstructed without two observed anchors: "
            f"start_present={days[0] in observed} end_present={days[-1] in observed}",
            context={
                "start": days[0].isoformat(),
                "end": days[-1].isoformat(),
                "start_present": days[0] in observed,
                "end_present": days[-1] in observed,
            },
        )

    missing_indexes = [index for index, day in enumerate(days) if day not in observed]
    grouped_indexes: list[list[int]] = []
    for index in missing_indexes:
        if not grouped_indexes or index != grouped_indexes[-1][-1] + 1:
            grouped_indexes.append([index])
        else:
            grouped_indexes[-1].append(index)
    relevant_events = sorted(
        (
            event
            for event in events
            if event.event_kind in {
                "st_negative",
                "st_restore",
                "delisted",
                "paused_listing",
                "delisting_confirmed",
            }
        ),
        key=lambda event: (event.action_date, event.ts_code, event.event_kind),
    )
    for indexes in grouped_indexes:
        previous_day = days[indexes[0] - 1]
        next_day = days[indexes[-1] + 1]
        snapshots: dict[dt.date, set[str]] = {previous_day: set(), next_day: set()}
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ann_date::date, ts_code
                  FROM market.stock_st
                 WHERE ann_date::date = ANY(%s)
                   {a_share_ts_code_filter("ts_code")}
                 ORDER BY ann_date, ts_code
                """,
                ([previous_day, next_day],),
            )
            for snapshot_date, ts_code in cur.fetchall():
                snapshots[snapshot_date].add(str(ts_code))
        gap_events = [
            (
                event.ts_code,
                "terminal_exit"
                if event.event_kind in {"delisted", "paused_listing", "delisting_confirmed"}
                else event.event_kind,
            )
            for event in relevant_events
            if previous_day < event.action_date <= next_day
        ]
        reconstruct_missing_st_snapshot(
            snapshots[previous_day],
            snapshots[next_day],
            gap_events,
        )
    missing_days = [days[index] for index in missing_indexes]
    return {
        "trading_day_count": len(days),
        "observed_snapshot_day_count": len(observed.intersection(days)),
        "missing_snapshot_day_count": len(missing_days),
        "reconstructed_snapshot_day_count": len(missing_days),
        "missing_snapshot_dates": [day.isoformat() for day in missing_days],
        "status": "ready",
    }


def _load_stock_basic_scope_counts(conn: Any, *, active_as_of: dt.date | None = None) -> dict[str, int]:
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT COALESCE(list_status, '') AS list_status, COUNT(*) AS cnt
              FROM market.stock_basic
             WHERE exchange IN ('SSE', 'SZSE')
               AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
               {a_share_ts_code_filter("ts_code")}
             GROUP BY COALESCE(list_status, '')
            """
        )
        counts = {str(row["list_status"] or "UNKNOWN"): int(row["cnt"]) for row in cur.fetchall()}
        asof_counts: dict[str, int] = {}
        if active_as_of is not None:
            cur.execute(
                f"""
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
                   {a_share_ts_code_filter("ts_code")}
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
    ipo_filter_unit: str = "calendar_days",
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
        ipo_eligible = _ipo_eligible_date(
            list_date=stock.list_date,
            filter_value=ipo_filter_days,
            filter_unit=ipo_filter_unit,
            calendar=calendar,
        )
        if ipo_eligible is None:
            continue
        current_start = max(ipo_eligible, start_date)
        eligible = current_start <= end_date
        entry_reason = (
            f"ipo_{ipo_filter_days}td"
            if ipo_filter_unit == "trading_sessions"
            else "ipo_365d"
            if ipo_filter_days == 365
            else f"ipo_{ipo_filter_days}d"
        )
        entry_event_date: dt.date | None = None
        terminal = False

        for event in events_by_stock.get(stock.ts_code, []):
            if event.action_date < current_start and eligible:
                # Historical events before this universe window can still define state.
                if event.event_kind in {"st_negative", "delist_event", "delisted", "paused_listing", "delisting_confirmed"}:
                    eligible = False
                    terminal = event.terminal
                elif event.event_kind == "st_restore" and not terminal:
                    eligible = True
                continue
            if event.action_date > end_date:
                break
            if event.event_kind in {"st_negative", "delist_event", "delisted", "paused_listing", "delisting_confirmed"}:
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
        if event.event_kind in {"st_negative", "delist_event", "delisted", "paused_listing", "delisting_confirmed"}:
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
    ipo_filter_unit = getattr(args, "ipo_filter_unit", "calendar_days") or "calendar_days"
    if ipo_filter_unit not in SUPPORTED_IPO_FILTER_UNITS:
        raise RuntimeError(f"unsupported --ipo-filter-unit: {ipo_filter_unit}")
    if scope == CANONICAL_PIT_SCOPE:
        canonical_errors = []
        if args.universe_key != CANONICAL_PIT_UNIVERSE_KEY:
            canonical_errors.append("universe_key")
        if rule_version != CANONICAL_PIT_RULE_VERSION:
            canonical_errors.append("rule_version")
        if ipo_filter_unit != "trading_sessions" or args.ipo_filter_days != CANONICAL_PIT_IPO_TRADING_SESSIONS:
            canonical_errors.append("ipo_warmup")
        if canonical_errors:
            raise RuntimeError(f"canonical PIT parameters differ: {canonical_errors}")
    st_only_active = scope == "st_only_active"
    canonical_scope = scope == CANONICAL_PIT_SCOPE

    with psycopg2.connect(**_db_config()) as conn:
        _ensure_tables(conn)
        scope_counts = _load_stock_basic_scope_counts(conn, active_as_of=end_date)
        stocks = _load_stock_basic(conn, active_only=st_only_active, active_as_of=end_date)
        exception_ledger = audit_canonical_stock_lifecycle(stocks) if canonical_scope else None
        calendar_start = start_date - dt.timedelta(days=max(args.ipo_filter_days + 31, 400))
        if canonical_scope:
            # A 252-session IPO threshold must be counted from the actual
            # listing session, not from an arbitrary backtest lookback window.
            calendar_start = _canonical_calendar_start(
                stocks,
                start_date=start_date,
                fallback_lookback_days=max(args.ipo_filter_days + 31, 400),
            )
        calendar_days = _load_trading_days(
            conn,
            calendar_start,
            end_date + dt.timedelta(days=31),
        )
        calendar = TradingCalendar(calendar_days)
        initial_st_events = _load_initial_st_events(
            conn,
            calendar,
            start_date,
            active_only=st_only_active,
            active_as_of=end_date,
        )
        st_events = _load_st_events(conn, calendar, end_date, terminal_as_negative=st_only_active)
        terminal_events = (
            []
            if st_only_active
            else _stock_basic_terminal_events(
                stocks,
                calendar,
                allow_paused_list_date_fallback=not canonical_scope,
            )
        )
        confirmed_delisting_events = _load_confirmed_delisting_events(conn, calendar, end_date) if canonical_scope else []
        terminal_evidence = (
            audit_canonical_terminal_evidence(
                stocks,
                announcement_events=st_events + confirmed_delisting_events,
                end_date=end_date,
            )
            if canonical_scope
            else None
        )
        events = sorted(
            initial_st_events + st_events + terminal_events + confirmed_delisting_events,
            key=lambda e: (e.ts_code, e.action_date, e.event_kind),
        )
        st_snapshot_continuity = (
            _audit_canonical_st_snapshots(
                conn,
                calendar=calendar,
                start_date=start_date,
                end_date=end_date,
                events=st_events + terminal_events + confirmed_delisting_events,
            )
            if canonical_scope
            else None
        )
        spans = _build_spans(
            stocks,
            events,
            universe_key=args.universe_key,
            start_date=start_date,
            end_date=end_date,
            ipo_filter_days=args.ipo_filter_days,
            ipo_filter_unit=ipo_filter_unit,
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
            "ipo_filter_unit": ipo_filter_unit,
            "write_mode": write_mode,
            "incremental_from": incremental_from.isoformat() if incremental_from else None,
            "st_pit": True,
            "rule_parameters_digest": canonical_rule_parameters_digest() if canonical_scope else None,
            "exception_ledger_status": exception_ledger["status"] if exception_ledger else None,
            "exception_ledger": exception_ledger,
            "st_snapshot_continuity": st_snapshot_continuity,
            "terminal_evidence": terminal_evidence,
            "delist_pit": not st_only_active,
            "pause_pit": not st_only_active,
            "survivorship_bias": (
                "D/P stocks as of generation end are excluded by ST-only active universe scope; delisting PIT is not implemented"
                if st_only_active
                else "canonical historical lifecycle and announcement-as-of terminal events enabled"
                if canonical_scope
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
            # Ingestion owns global data-stat refreshes. Calling the catalog-wide
            # refresh here makes a small PIT rebuild scan unrelated large tables
            # such as moneyflow_ts and can abort strict live selection.

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
    parser.add_argument(
        "--ipo-filter-unit",
        choices=sorted(SUPPORTED_IPO_FILTER_UNITS),
        default="calendar_days",
    )
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
