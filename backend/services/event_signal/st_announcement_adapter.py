"""ST-first announcement adapter for unified event signals.

This module is intentionally isolated from QE, Selection Center, Paper v2, and
live trading consumers.  It converts already-classified announcement titles into
standardized event facts/signals for ST, delisting, and risk-warning research.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import psycopg2.extras
from dotenv import load_dotenv

from backend.db.init_announcement_event_schema import init_announcement_event_schema
from backend.db.init_unified_event_signal_schema import init_unified_event_signal_schema
from backend.db.pg_pool import get_conn
from backend.services.announcements.title_classifier import (
    ENGINE_NAME as ANNOUNCEMENT_ENGINE_NAME,
    RULE_VERSION as ANNOUNCEMENT_RULE_VERSION,
    rule_config_hash as announcement_rule_config_hash,
)
from backend.services.event_signal.announcement_adapter import (
    SOURCE_TYPE,
    build_event_key,
    finish_run,
    start_run,
    upsert_facts,
    upsert_signals,
)


ROOT = Path(__file__).resolve().parents[3]
ST_UNIFIED_RULE_VERSION = "unified_event_signal_rules_st_first_v1_20260506"
ENGINE_NAME = "STFirstAnnouncementEventSignalAdapter"

ST_FIRST_EVENT_TYPES: tuple[str, ...] = (
    "stock_delisting_confirmed",
    "stock_delisting_risk_warning",
    "stock_st_imposed",
    "stock_st_added_or_continued",
    "stock_st_removal_applied",
    "stock_st_removed_confirmed",
    "convertible_bond_delisting_or_redemption",
    "generic_bond_delisting_or_repayment",
)

ST_SIGNAL_EVENT_TYPES: tuple[str, ...] = (
    "stock_delisting_confirmed",
    "stock_delisting_risk_warning",
    "stock_st_imposed",
    "stock_st_added_or_continued",
    "stock_st_removal_applied",
)


@dataclass(frozen=True)
class AdapterSummary:
    """Compact metrics returned by one ST-first adapter run."""

    run_id: str
    rule_version: str
    source_rule_version: str
    time_mode: str
    processed_rows: int
    fact_rows: int
    signal_rows: int
    cross_checked_rows: int
    st_event_matched_rows: int
    status: str


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _jsonb(value: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(value, dumps=_json_dumps)


def _date_or_none(value: Any) -> Optional[dt.date]:
    if value is None or isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def st_first_rule_config(
    *,
    source_rule_version: str = ANNOUNCEMENT_RULE_VERSION,
    event_types: tuple[str, ...] = ST_FIRST_EVENT_TYPES,
) -> dict[str, Any]:
    """Return deterministic config persisted for the ST-first rule version."""

    return {
        "version": ST_UNIFIED_RULE_VERSION,
        "engine_name": ENGINE_NAME,
        "adapters": {
            "announcement_st_first": {
                "source_rule_version": source_rule_version,
                "source_engine_name": ANNOUNCEMENT_ENGINE_NAME,
                "source_fact_table": "market.ann_event_classification",
                "source_announcement_table": "market.anns",
                "cross_check_table": "market.stock_st_events",
                "event_types": list(event_types),
                "signal_event_types": list(ST_SIGNAL_EVENT_TYPES),
            }
        },
        "phase": "st_first_announcement_rules_v1",
        "llm_enabled": False,
        "pdf_enabled": False,
        "trading_consumption_enabled": False,
    }


def st_first_rule_hash(source_rule_version: str = ANNOUNCEMENT_RULE_VERSION) -> str:
    payload = {
        "st_first": st_first_rule_config(source_rule_version=source_rule_version),
        "announcement_rule_hash": announcement_rule_config_hash(),
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def seed_st_first_rule_set(
    conn: Any,
    *,
    rule_version: str = ST_UNIFIED_RULE_VERSION,
    source_rule_version: str = ANNOUNCEMENT_RULE_VERSION,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.event_signal_rule_set
                (
                    rule_version, engine_name, rule_source, rule_scope,
                    config_hash, config, source_rule_versions, is_active, updated_at
                )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, TRUE, NOW())
            ON CONFLICT (rule_version) DO UPDATE SET
                engine_name = EXCLUDED.engine_name,
                rule_source = EXCLUDED.rule_source,
                rule_scope = EXCLUDED.rule_scope,
                config_hash = EXCLUDED.config_hash,
                config = EXCLUDED.config,
                source_rule_versions = EXCLUDED.source_rule_versions,
                is_active = TRUE,
                updated_at = NOW()
            """,
            (
                rule_version,
                ENGINE_NAME,
                "market.ann_event_classification_market.anns_market.stock_st_events",
                "announcement_st_first_v1",
                st_first_rule_hash(source_rule_version),
                _json_dumps(st_first_rule_config(source_rule_version=source_rule_version)),
                _json_dumps({"announcement_title": source_rule_version}),
            ),
        )


def build_run_id(*, source_rule_version: str, time_mode: str, run_mode: str) -> str:
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"event_signal_st_first_{run_mode}_{time_mode}_{source_rule_version}_{now}"


def fetch_st_classification_batch(
    conn: Any,
    *,
    source_rule_version: str,
    rule_version: str,
    time_mode: str,
    last_classification_id: int,
    batch_size: int,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    event_types: tuple[str, ...] = ST_FIRST_EVENT_TYPES,
    missing_only: bool = False,
) -> list[dict[str, Any]]:
    params: list[Any] = [source_rule_version, time_mode, list(event_types), last_classification_id]
    date_sql = ""
    if start_date is not None:
        date_sql += " AND c.ann_date >= %s"
        params.append(start_date)
    if end_date is not None:
        date_sql += " AND c.ann_date <= %s"
        params.append(end_date)

    missing_sql = ""
    if missing_only:
        missing_sql = """
           AND NOT EXISTS (
               SELECT 1
                 FROM market.event_fact ef
                WHERE ef.event_key = concat('event_fact:announcement:', c.ann_id, ':', %s, ':', c.time_mode)
           )
        """
        params.append(rule_version)

    params.append(batch_size)
    sql = f"""
        SELECT
            c.classification_id,
            c.ann_id,
            c.ts_code,
            c.ann_date,
            c.title_hash,
            c.rule_version AS source_rule_version,
            c.event_type,
            c.risk_level,
            c.action,
            c.needs_llm,
            c.matched_rule,
            c.matched_text,
            c.source_time_quality,
            c.effective_trade_date,
            c.effective_rule,
            c.available_at,
            c.confidence,
            c.severity_score,
            c.classification_detail,
            c.time_mode,
            a.title,
            NULL::bigint AS ann_signal_id,
            'ACTIVE'::text AS ann_signal_status,
            NULL::text AS ann_signal_reason,
            '{{}}'::jsonb AS ann_signal_evidence
          FROM market.ann_event_classification c
          JOIN market.anns a
            ON a.id = c.ann_id
         WHERE c.rule_version = %s
           AND c.time_mode = %s
           AND c.event_type = ANY(%s)
           AND c.classification_id > %s
           {date_sql}
           {missing_sql}
         ORDER BY c.classification_id
         LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, tuple(params))
        return [dict(row) for row in cur.fetchall()]


def fetch_stock_st_events_for_rows(conn: Any, rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    if not rows:
        return {}
    ts_codes = sorted({str(row["ts_code"]) for row in rows if row.get("ts_code")})
    dates = [date for row in rows for date in (_date_or_none(row.get("ann_date")), _date_or_none(row.get("effective_trade_date"))) if date]
    if not ts_codes or not dates:
        return {}
    start = min(dates) - dt.timedelta(days=10)
    end = max(dates) + dt.timedelta(days=10)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ts_code, name, pub_date, imp_date, st_type, st_reason,
                   st_explain, source_api, ingested_at
              FROM market.stock_st_events
             WHERE ts_code = ANY(%s)
               AND (
                    pub_date BETWEEN %s AND %s
                 OR imp_date BETWEEN %s AND %s
               )
             ORDER BY ts_code, pub_date, imp_date
            """,
            (ts_codes, start, end, start, end),
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in cur.fetchall():
            grouped.setdefault(str(row["ts_code"]), []).append(dict(row))
        return grouped


def _day_distance(left: Optional[dt.date], right: Optional[dt.date]) -> int:
    if left is None or right is None:
        return 999_999
    return abs((left - right).days)


def select_best_st_event(row: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    """Select the nearest stock_st_events row for one announcement row."""

    ann_date = _date_or_none(row.get("ann_date"))
    effective_date = _date_or_none(row.get("effective_trade_date"))
    if not candidates:
        return {
            "checked": True,
            "matched": False,
            "match_reason": "no_stock_st_events_for_symbol_in_window",
        }

    def score(candidate: dict[str, Any]) -> tuple[int, int, int]:
        pub_date = _date_or_none(candidate.get("pub_date"))
        imp_date = _date_or_none(candidate.get("imp_date"))
        pub_distance = _day_distance(ann_date, pub_date)
        imp_distance = _day_distance(effective_date, imp_date)
        return (min(pub_distance, imp_distance), pub_distance, imp_distance)

    best = min(candidates, key=score)
    best_score = score(best)
    if best_score[0] > 5:
        return {
            "checked": True,
            "matched": False,
            "match_reason": "nearest_stock_st_event_outside_5_day_window",
            "nearest_pub_date": best.get("pub_date"),
            "nearest_imp_date": best.get("imp_date"),
            "nearest_distance_days": best_score[0],
        }
    return {
        "checked": True,
        "matched": True,
        "match_reason": "nearest_stock_st_event_within_5_day_window",
        "pub_date": best.get("pub_date"),
        "imp_date": best.get("imp_date"),
        "st_type": best.get("st_type"),
        "st_reason": best.get("st_reason"),
        "st_explain": best.get("st_explain"),
        "source_api": best.get("source_api"),
        "distance_days": best_score[0],
    }


def attach_st_cross_checks(
    rows: list[dict[str, Any]],
    stock_st_events_by_code: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], int]:
    """Attach ST cross-check evidence without changing raw/classification tables."""

    matched_rows = 0
    enriched: list[dict[str, Any]] = []
    for row in rows:
        copied = dict(row)
        cross_check = select_best_st_event(copied, stock_st_events_by_code.get(str(copied.get("ts_code")), []))
        if cross_check.get("matched"):
            matched_rows += 1
        evidence = copied.get("ann_signal_evidence") or {}
        if not isinstance(evidence, dict):
            evidence = {"source_signal_evidence": evidence}
        evidence.update(
            {
                "adapter": ENGINE_NAME,
                "st_first_rule_version": ST_UNIFIED_RULE_VERSION,
                "source_ann_rule_version": copied.get("source_rule_version"),
                "title": copied.get("title"),
                "st_cross_check": cross_check,
            }
        )
        detail = copied.get("classification_detail") or {}
        if not isinstance(detail, dict):
            detail = {"source_classification_detail": detail}
        detail = dict(detail)
        detail["st_cross_check"] = cross_check
        copied["ann_signal_evidence"] = evidence
        copied["classification_detail"] = detail
        copied["ann_signal_reason"] = f"{copied.get('risk_level')} {copied.get('event_type')}: {copied.get('title') or ''}"[:1000]
        enriched.append(copied)
    return enriched, matched_rows


def sync_st_first_announcement_event_signals(
    *,
    source_rule_version: str = ANNOUNCEMENT_RULE_VERSION,
    rule_version: str = ST_UNIFIED_RULE_VERSION,
    time_mode: str = "backtest",
    run_mode: str = "incremental",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    batch_size: int = 5000,
    limit: Optional[int] = None,
    missing_only: bool = False,
    ensure_schema: bool = True,
) -> AdapterSummary:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    if ensure_schema:
        init_announcement_event_schema()
        init_unified_event_signal_schema()

    run_id = build_run_id(source_rule_version=source_rule_version, time_mode=time_mode, run_mode=run_mode)
    processed_rows = 0
    fact_rows = 0
    signal_rows = 0
    cross_checked_rows = 0
    st_event_matched_rows = 0
    last_classification_id = 0

    with get_conn() as conn:
        seed_st_first_rule_set(conn, rule_version=rule_version, source_rule_version=source_rule_version)
        start_run(
            conn,
            run_id=run_id,
            rule_version=rule_version,
            source_rule_version=source_rule_version,
            time_mode=time_mode,
            run_mode=run_mode,
            date_from=start_date,
            date_to=end_date,
        )
        try:
            while True:
                remaining = None if limit is None else limit - processed_rows
                if remaining is not None and remaining <= 0:
                    break
                rows = fetch_st_classification_batch(
                    conn,
                    source_rule_version=source_rule_version,
                    rule_version=rule_version,
                    time_mode=time_mode,
                    last_classification_id=last_classification_id,
                    batch_size=min(batch_size, remaining) if remaining is not None else batch_size,
                    start_date=start_date,
                    end_date=end_date,
                    missing_only=missing_only,
                )
                if not rows:
                    break
                last_classification_id = int(rows[-1]["classification_id"])
                stock_st_events = fetch_stock_st_events_for_rows(conn, rows)
                rows, matched_rows = attach_st_cross_checks(rows, stock_st_events)
                event_ids = upsert_facts(conn, rows, run_id=run_id, rule_version=rule_version)
                batch_signal_rows = upsert_signals(
                    conn,
                    rows,
                    event_ids_by_key=event_ids,
                    run_id=run_id,
                    rule_version=rule_version,
                    time_mode=time_mode,
                )

                processed_rows += len(rows)
                fact_rows += len(rows)
                signal_rows += batch_signal_rows
                cross_checked_rows += len(rows)
                st_event_matched_rows += matched_rows

            finish_run(
                conn,
                run_id=run_id,
                status="SUCCESS",
                source_input_rows=processed_rows,
                fact_rows=fact_rows,
                signal_rows=signal_rows,
                metrics={
                    "source_type": SOURCE_TYPE,
                    "adapter": ENGINE_NAME,
                    "last_classification_id": last_classification_id,
                    "missing_only": missing_only,
                    "limit": limit,
                    "event_types": list(ST_FIRST_EVENT_TYPES),
                    "cross_checked_rows": cross_checked_rows,
                    "st_event_matched_rows": st_event_matched_rows,
                    "st_event_match_rate": (st_event_matched_rows / cross_checked_rows) if cross_checked_rows else None,
                },
            )
            return AdapterSummary(
                run_id,
                rule_version,
                source_rule_version,
                time_mode,
                processed_rows,
                fact_rows,
                signal_rows,
                cross_checked_rows,
                st_event_matched_rows,
                "SUCCESS",
            )
        except Exception as exc:
            finish_run(
                conn,
                run_id=run_id,
                status="FAILED",
                source_input_rows=processed_rows,
                fact_rows=fact_rows,
                signal_rows=signal_rows,
                error_message=str(exc)[:2000],
                metrics={
                    "source_type": SOURCE_TYPE,
                    "adapter": ENGINE_NAME,
                    "last_classification_id": last_classification_id,
                    "cross_checked_rows": cross_checked_rows,
                    "st_event_matched_rows": st_event_matched_rows,
                },
            )
            raise


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate ST-first unified event signals from classified announcements")
    parser.add_argument("--source-rule-version", default=ANNOUNCEMENT_RULE_VERSION)
    parser.add_argument("--rule-version", default=ST_UNIFIED_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--run-mode", choices=["backfill", "incremental", "smoke", "repair", "research"], default="incremental")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--missing-only", action="store_true")
    parser.add_argument("--skip-schema", action="store_true")
    return parser.parse_args()


def main() -> None:
    load_dotenv(ROOT / ".env", override=False)
    args = parse_args()
    summary = sync_st_first_announcement_event_signals(
        source_rule_version=args.source_rule_version,
        rule_version=args.rule_version,
        time_mode=args.time_mode,
        run_mode=args.run_mode,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        batch_size=args.batch_size,
        limit=args.limit,
        missing_only=args.missing_only,
        ensure_schema=not args.skip_schema,
    )
    print(_json_dumps(summary.__dict__))


if __name__ == "__main__":
    main()
