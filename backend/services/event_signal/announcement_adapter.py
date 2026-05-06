"""Adapter from existing announcement title classifications to unified events.

This module only writes the new event_signal tables.  It does not change
announcement synchronization, QE, Selection Center, Paper v2, or trading paths.
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


ROOT = Path(__file__).resolve().parents[3]
UNIFIED_RULE_VERSION = "unified_event_signal_rules_v0_20260506"
ENGINE_NAME = "AnnouncementEventSignalAdapter"
SOURCE_TYPE = "announcement"
SIGNAL_RISK_LEVELS = ("P0_BLOCK", "P1_HIGH", "P2_REVIEW")


@dataclass(frozen=True)
class AdapterSummary:
    """Compact metrics returned by one adapter run."""

    run_id: str
    rule_version: str
    source_rule_version: str
    time_mode: str
    processed_rows: int
    fact_rows: int
    signal_rows: int
    status: str


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _jsonb(value: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(value, dumps=_json_dumps)


def unified_rule_config(source_rule_version: str = ANNOUNCEMENT_RULE_VERSION) -> dict[str, Any]:
    """Return the deterministic config persisted for this unified rule version."""

    return {
        "version": UNIFIED_RULE_VERSION,
        "adapters": {
            "announcement": {
                "source_rule_version": source_rule_version,
                "source_engine_name": ANNOUNCEMENT_ENGINE_NAME,
                "signal_risk_levels": list(SIGNAL_RISK_LEVELS),
                "source_fact_table": "market.ann_event_classification",
                "source_signal_table": "market.ann_risk_signal",
            },
            "tushare_financial_raw": {
                "source_tables": [
                    "market.tushare_forecast_raw",
                    "market.tushare_express_raw",
                    "market.tushare_fina_indicator_raw",
                ],
                "enabled_event_families": [
                    "financial_forecast",
                    "financial_express",
                    "financial_indicator",
                    "financial_relation",
                ],
                "positive_alpha_enabled": False,
                "llm_enabled": False,
            },
        },
        "phase": "announcement_and_financial_rules_v0",
        "llm_enabled": False,
        "pdf_enabled": False,
        "trading_consumption_enabled": False,
    }


def unified_rule_hash(source_rule_version: str = ANNOUNCEMENT_RULE_VERSION) -> str:
    """Hash the unified adapter config plus upstream announcement rule hash."""

    payload = {
        "unified": unified_rule_config(source_rule_version),
        "announcement_rule_hash": announcement_rule_config_hash(),
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def build_run_id(*, source_rule_version: str, time_mode: str, run_mode: str) -> str:
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"event_signal_announcement_{run_mode}_{time_mode}_{source_rule_version}_{now}"


def build_event_key(ann_id: int, *, rule_version: str = UNIFIED_RULE_VERSION, time_mode: str = "backtest") -> str:
    return f"event_fact:{SOURCE_TYPE}:{ann_id}:{rule_version}:{time_mode}"


def build_signal_key(ann_id: int, *, rule_version: str = UNIFIED_RULE_VERSION, time_mode: str = "backtest") -> str:
    return f"event_signal:{SOURCE_TYPE}:{ann_id}:{rule_version}:{time_mode}:risk"


def seed_unified_rule_set(
    conn: Any,
    *,
    rule_version: str = UNIFIED_RULE_VERSION,
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
                "market.ann_event_classification_ann_risk_signal_and_tushare_event_raw_tables",
                "announcement_and_financial_events_v0",
                unified_rule_hash(source_rule_version),
                _json_dumps(unified_rule_config(source_rule_version)),
                _json_dumps(
                    {
                        "announcement_title": source_rule_version,
                        "tushare_financial": "financial_event_rules_v0_20260506",
                    }
                ),
            ),
        )


def start_run(
    conn: Any,
    *,
    run_id: str,
    rule_version: str,
    source_rule_version: str,
    time_mode: str,
    run_mode: str,
    date_from: Optional[dt.date],
    date_to: Optional[dt.date],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.event_signal_run
                (
                    run_id, rule_version, run_mode, time_mode,
                    source_scope, date_from, date_to, status
                )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, 'RUNNING')
            ON CONFLICT (run_id) DO NOTHING
            """,
            (
                run_id,
                rule_version,
                run_mode,
                time_mode,
                _json_dumps(
                    {
                        "source_type": SOURCE_TYPE,
                        "source_rule_version": source_rule_version,
                        "source_tables": ["market.ann_event_classification", "market.ann_risk_signal"],
                    }
                ),
                date_from,
                date_to,
            ),
        )


def finish_run(
    conn: Any,
    *,
    run_id: str,
    status: str,
    source_input_rows: int,
    fact_rows: int,
    signal_rows: int,
    error_message: Optional[str] = None,
    metrics: Optional[dict[str, Any]] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.event_signal_run
               SET finished_at = NOW(),
                   status = %s,
                   source_input_rows = %s,
                   fact_rows = %s,
                   relation_rows = 0,
                   signal_rows = %s,
                   error_message = %s,
                   metrics = %s::jsonb,
                   updated_at = NOW()
             WHERE run_id = %s
            """,
            (
                status,
                source_input_rows,
                fact_rows,
                signal_rows,
                error_message,
                _json_dumps(metrics or {}),
                run_id,
            ),
        )


def fetch_classification_batch(
    conn: Any,
    *,
    source_rule_version: str,
    rule_version: str,
    time_mode: str,
    last_classification_id: int,
    batch_size: int,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    missing_only: bool = False,
) -> list[dict[str, Any]]:
    params: list[Any] = [source_rule_version, time_mode, last_classification_id]
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
            s.signal_id AS ann_signal_id,
            s.signal_status AS ann_signal_status,
            s.reason AS ann_signal_reason,
            s.evidence AS ann_signal_evidence
          FROM market.ann_event_classification c
          JOIN market.anns a
            ON a.id = c.ann_id
          LEFT JOIN market.ann_risk_signal s
            ON s.ann_id = c.ann_id
           AND s.rule_version = c.rule_version
           AND s.time_mode = c.time_mode
         WHERE c.rule_version = %s
           AND c.time_mode = %s
           AND c.classification_id > %s
           {date_sql}
           {missing_sql}
         ORDER BY c.classification_id
         LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, tuple(params))
        return [dict(row) for row in cur.fetchall()]


def _fact_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "adapter": ENGINE_NAME,
        "source_rule_version": row["source_rule_version"],
        "classification_id": row["classification_id"],
        "ann_id": row["ann_id"],
        "title": row.get("title"),
        "title_hash": row.get("title_hash"),
        "risk_level": row.get("risk_level"),
        "action": row.get("action"),
        "needs_llm": row.get("needs_llm"),
        "matched_rule": row.get("matched_rule"),
        "matched_text": row.get("matched_text"),
        "classification_detail": row.get("classification_detail") or {},
    }


def build_fact_tuple(
    row: dict[str, Any],
    *,
    run_id: str,
    rule_version: str = UNIFIED_RULE_VERSION,
) -> tuple[Any, ...]:
    return (
        build_event_key(int(row["ann_id"]), rule_version=rule_version, time_mode=row["time_mode"]),
        row["ts_code"],
        "announcement",
        row["event_type"],
        "ACTIVE",
        SOURCE_TYPE,
        str(row["ann_id"]),
        f"announcement:{row['ann_id']}",
        row["ann_date"],
        row.get("available_at"),
        row["source_time_quality"],
        row.get("available_at"),
        row["effective_trade_date"],
        row["time_mode"],
        None,
        rule_version,
        run_id,
        row["confidence"],
        _jsonb(_fact_payload(row)),
        row.get("title_hash"),
    )


def upsert_facts(
    conn: Any,
    rows: list[dict[str, Any]],
    *,
    run_id: str,
    rule_version: str,
) -> dict[str, int]:
    if not rows:
        return {}
    values = [build_fact_tuple(row, run_id=run_id, rule_version=rule_version) for row in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO market.event_fact
                (
                    event_key, ts_code, event_family, event_type, event_status,
                    source_type, source_pk, source_record_key,
                    source_event_date, source_available_at, source_time_quality,
                    available_at, effective_trade_date, time_mode, report_period,
                    rule_version, run_id, fact_confidence, facts, source_payload_hash
                )
            VALUES %s
            ON CONFLICT (event_key) DO UPDATE SET
                ts_code = EXCLUDED.ts_code,
                event_family = EXCLUDED.event_family,
                event_type = EXCLUDED.event_type,
                event_status = EXCLUDED.event_status,
                source_type = EXCLUDED.source_type,
                source_pk = EXCLUDED.source_pk,
                source_record_key = EXCLUDED.source_record_key,
                source_event_date = EXCLUDED.source_event_date,
                source_available_at = EXCLUDED.source_available_at,
                source_time_quality = EXCLUDED.source_time_quality,
                available_at = EXCLUDED.available_at,
                effective_trade_date = EXCLUDED.effective_trade_date,
                time_mode = EXCLUDED.time_mode,
                report_period = EXCLUDED.report_period,
                rule_version = EXCLUDED.rule_version,
                run_id = EXCLUDED.run_id,
                fact_confidence = EXCLUDED.fact_confidence,
                facts = EXCLUDED.facts,
                source_payload_hash = EXCLUDED.source_payload_hash,
                generated_at = NOW(),
                updated_at = NOW()
            """,
            values,
            page_size=1000,
        )
        keys = [value[0] for value in values]
        cur.execute(
            "SELECT event_key, event_id FROM market.event_fact WHERE event_key = ANY(%s)",
            (keys,),
        )
        return {key: int(event_id) for key, event_id in cur.fetchall()}


def _signal_evidence(row: dict[str, Any], event_id: int, rule_version: str) -> dict[str, Any]:
    evidence = row.get("ann_signal_evidence") or {}
    if not isinstance(evidence, dict):
        evidence = {"source_signal_evidence": evidence}
    evidence.update(
        {
            "adapter": ENGINE_NAME,
            "source_type": SOURCE_TYPE,
            "source_ann_rule_version": row["source_rule_version"],
            "unified_rule_version": rule_version,
            "classification_id": row["classification_id"],
            "ann_signal_id": row.get("ann_signal_id"),
            "event_id": event_id,
            "ann_id": row["ann_id"],
            "title": row.get("title"),
        }
    )
    return evidence


def build_signal_tuple(
    row: dict[str, Any],
    *,
    event_id: int,
    run_id: str,
    rule_version: str = UNIFIED_RULE_VERSION,
) -> tuple[Any, ...]:
    reason = row.get("ann_signal_reason") or f"{row['risk_level']} {row['event_type']}"
    return (
        build_signal_key(int(row["ann_id"]), rule_version=rule_version, time_mode=row["time_mode"]),
        row["ts_code"],
        event_id,
        [event_id],
        [],
        SOURCE_TYPE,
        str(row["ann_id"]),
        row["ann_date"],
        row["source_time_quality"],
        row.get("available_at"),
        row["effective_trade_date"],
        row["time_mode"],
        "announcement",
        row["event_type"],
        row["risk_level"],
        row["action"],
        "risk",
        row.get("ann_signal_status") or "ACTIVE",
        row["severity_score"],
        row["confidence"],
        0,
        reason,
        _jsonb(_signal_evidence(row, event_id, rule_version)),
        row["effective_rule"],
        rule_version,
        run_id,
    )


def upsert_signals(
    conn: Any,
    rows: list[dict[str, Any]],
    *,
    event_ids_by_key: dict[str, int],
    run_id: str,
    rule_version: str,
    time_mode: str,
) -> int:
    processed_source_pks = [str(row["ann_id"]) for row in rows]
    signal_rows = [row for row in rows if row["risk_level"] in SIGNAL_RISK_LEVELS]
    values: list[tuple[Any, ...]] = []
    for row in signal_rows:
        event_key = build_event_key(int(row["ann_id"]), rule_version=rule_version, time_mode=row["time_mode"])
        event_id = event_ids_by_key[event_key]
        values.append(build_signal_tuple(row, event_id=event_id, run_id=run_id, rule_version=rule_version))

    with conn.cursor() as cur:
        if processed_source_pks:
            if values:
                cur.execute(
                    """
                    DELETE FROM market.event_signal
                     WHERE source_type = %s
                       AND rule_version = %s
                       AND time_mode = %s
                       AND source_pk = ANY(%s)
                       AND NOT (signal_key = ANY(%s))
                    """,
                    (SOURCE_TYPE, rule_version, time_mode, processed_source_pks, [value[0] for value in values]),
                )
            else:
                cur.execute(
                    """
                    DELETE FROM market.event_signal
                     WHERE source_type = %s
                       AND rule_version = %s
                       AND time_mode = %s
                       AND source_pk = ANY(%s)
                    """,
                    (SOURCE_TYPE, rule_version, time_mode, processed_source_pks),
                )

        if not values:
            return 0

        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO market.event_signal
                (
                    signal_key, ts_code, event_id, source_event_ids, relation_ids,
                    source_type, source_pk, source_event_date, source_time_quality,
                    available_at, effective_trade_date, time_mode,
                    event_family, event_type, risk_level, action, signal_type,
                    signal_status, severity_score, confidence, alpha_score,
                    reason, evidence, effective_rule, rule_version, run_id
                )
            VALUES %s
            ON CONFLICT (signal_key) DO UPDATE SET
                ts_code = EXCLUDED.ts_code,
                event_id = EXCLUDED.event_id,
                source_event_ids = EXCLUDED.source_event_ids,
                relation_ids = EXCLUDED.relation_ids,
                source_type = EXCLUDED.source_type,
                source_pk = EXCLUDED.source_pk,
                source_event_date = EXCLUDED.source_event_date,
                source_time_quality = EXCLUDED.source_time_quality,
                available_at = EXCLUDED.available_at,
                effective_trade_date = EXCLUDED.effective_trade_date,
                time_mode = EXCLUDED.time_mode,
                event_family = EXCLUDED.event_family,
                event_type = EXCLUDED.event_type,
                risk_level = EXCLUDED.risk_level,
                action = EXCLUDED.action,
                signal_type = EXCLUDED.signal_type,
                signal_status = EXCLUDED.signal_status,
                severity_score = EXCLUDED.severity_score,
                confidence = EXCLUDED.confidence,
                alpha_score = EXCLUDED.alpha_score,
                reason = EXCLUDED.reason,
                evidence = EXCLUDED.evidence,
                effective_rule = EXCLUDED.effective_rule,
                rule_version = EXCLUDED.rule_version,
                run_id = EXCLUDED.run_id,
                generated_at = NOW(),
                updated_at = NOW()
            """,
            values,
            page_size=1000,
        )
        return len(values)


def sync_announcement_event_signals(
    *,
    source_rule_version: str = ANNOUNCEMENT_RULE_VERSION,
    rule_version: str = UNIFIED_RULE_VERSION,
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
    last_classification_id = 0

    with get_conn() as conn:
        seed_unified_rule_set(conn, rule_version=rule_version, source_rule_version=source_rule_version)
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
                rows = fetch_classification_batch(
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

            finish_run(
                conn,
                run_id=run_id,
                status="SUCCESS",
                source_input_rows=processed_rows,
                fact_rows=fact_rows,
                signal_rows=signal_rows,
                metrics={
                    "source_type": SOURCE_TYPE,
                    "last_classification_id": last_classification_id,
                    "missing_only": missing_only,
                    "limit": limit,
                },
            )
            status = "SUCCESS"
        except Exception as exc:
            finish_run(
                conn,
                run_id=run_id,
                status="FAILED",
                source_input_rows=processed_rows,
                fact_rows=fact_rows,
                signal_rows=signal_rows,
                error_message=str(exc)[:4000],
                metrics={"source_type": SOURCE_TYPE, "last_classification_id": last_classification_id},
            )
            raise

    return AdapterSummary(
        run_id=run_id,
        rule_version=rule_version,
        source_rule_version=source_rule_version,
        time_mode=time_mode,
        processed_rows=processed_rows,
        fact_rows=fact_rows,
        signal_rows=signal_rows,
        status=status,
    )


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync announcement classifications into unified event_signal tables")
    parser.add_argument("--source-rule-version", default=ANNOUNCEMENT_RULE_VERSION)
    parser.add_argument("--rule-version", default=UNIFIED_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--run-mode", choices=["backfill", "incremental", "smoke", "repair", "research"], default="incremental")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--missing-only", action="store_true")
    parser.add_argument("--no-ensure-schema", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(ROOT / ".env", override=True)
    load_dotenv(override=False)
    summary = sync_announcement_event_signals(
        source_rule_version=args.source_rule_version,
        rule_version=args.rule_version,
        time_mode=args.time_mode,
        run_mode=args.run_mode,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        batch_size=args.batch_size,
        limit=args.limit,
        missing_only=args.missing_only,
        ensure_schema=not args.no_ensure_schema,
    )
    print(_json_dumps(summary.__dict__))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
