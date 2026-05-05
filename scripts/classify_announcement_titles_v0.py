"""Classify market.anns titles and optionally persist v0 event/risk signals.

This script is intentionally metadata-only: it does not download PDFs and does
not call an LLM.  It writes deterministic, versioned results so the same output
can be consumed by historical backtests and future live polling.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg2.extras
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.init_announcement_event_schema import init_announcement_event_schema  # noqa: E402
from backend.db.pg_pool import get_conn  # noqa: E402
from backend.services.announcements.title_classifier import (  # noqa: E402
    ENGINE_NAME,
    RULES,
    RULE_VERSION,
    AnnouncementTitleClassifier,
    ClassificationResult,
    EffectiveDateResult,
    rule_config_hash,
    rule_config_json,
    taxonomy_rows,
    title_hash,
)


SIGNAL_RISK_LEVELS = {"P0_BLOCK", "P1_HIGH", "P2_REVIEW"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classify announcement titles from market.anns")
    parser.add_argument("--start-date", default=None, help="Inclusive YYYY-MM-DD announcement date")
    parser.add_argument("--end-date", default=None, help="Inclusive YYYY-MM-DD announcement date")
    parser.add_argument("--limit", type=int, default=None, help="Optional max rows for smoke tests")
    parser.add_argument("--batch-size", type=int, default=5000, help="Read/write batch size")
    parser.add_argument("--rule-version", default=RULE_VERSION, help="Rule version to write")
    parser.add_argument("--persist", action="store_true", help="Persist classification rows into DB")
    parser.add_argument(
        "--generate-signals",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When persisting, write ann_risk_signal rows for P0/P1/P2 rows",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Only process announcements missing this rule_version in ann_event_classification",
    )
    parser.add_argument(
        "--truncate-version",
        action="store_true",
        help="Delete existing rows for the selected date range and rule_version before processing",
    )
    parser.add_argument("--json-out", default=None, help="Summary JSON output path")
    parser.add_argument("--md-out", default=None, help="Summary Markdown output path")
    return parser.parse_args()


def parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def default_report_paths() -> tuple[Path, Path]:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = ROOT / "reports" / "anns"
    report_dir.mkdir(parents=True, exist_ok=True)
    return (
        report_dir / f"announcement_title_classification_v0_{ts}.json",
        ROOT / "docs" / "analysis" / f"announcement_title_classification_v0_{ts}.md",
    )


def seed_rule_metadata(conn: Any, classifier: AnnouncementTitleClassifier) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO market.ann_event_taxonomy
                (event_type, risk_level, default_action, needs_llm, description)
            VALUES %s
            ON CONFLICT (event_type) DO UPDATE SET
                risk_level = EXCLUDED.risk_level,
                default_action = EXCLUDED.default_action,
                needs_llm = EXCLUDED.needs_llm,
                description = EXCLUDED.description,
                is_active = TRUE,
                updated_at = NOW()
            """,
            [
                (
                    row["event_type"],
                    row["risk_level"],
                    row["default_action"],
                    row["needs_llm"],
                    row["description"],
                )
                for row in taxonomy_rows(classifier.rules)
            ],
        )
        cur.execute(
            """
            INSERT INTO market.ann_rule_set
                (rule_version, engine_name, rule_source, rule_count, config_hash, config, is_active, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, TRUE, NOW())
            ON CONFLICT (rule_version) DO UPDATE SET
                engine_name = EXCLUDED.engine_name,
                rule_source = EXCLUDED.rule_source,
                rule_count = EXCLUDED.rule_count,
                config_hash = EXCLUDED.config_hash,
                config = EXCLUDED.config,
                is_active = TRUE,
                updated_at = NOW()
            """,
            (
                classifier.rule_version,
                ENGINE_NAME,
                "title_rules_v0_from_local_market_anns",
                len(classifier.rules),
                rule_config_hash(classifier.rules),
                json.dumps(rule_config_json(classifier.rules), ensure_ascii=False),
            ),
        )


def load_date_bounds(conn: Any, start_date: Optional[dt.date], end_date: Optional[dt.date]) -> tuple[dt.date, dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT min(ann_date), max(ann_date) FROM market.anns")
        min_date, max_date = cur.fetchone()
    if min_date is None or max_date is None:
        raise RuntimeError("market.anns is empty; cannot classify announcement titles")
    return start_date or min_date, end_date or max_date


def load_trading_days(conn: Any, start_date: dt.date, end_date: dt.date) -> list[dt.date]:
    calendar_end = end_date + dt.timedelta(days=45)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cal_date
              FROM market.trading_calendar
             WHERE is_trading = TRUE
               AND cal_date >= %s
               AND cal_date <= %s
             ORDER BY cal_date
            """,
            (start_date - dt.timedelta(days=5), calendar_end),
        )
        rows = [row[0] for row in cur.fetchall()]
    if not rows:
        raise RuntimeError("market.trading_calendar has no rows for requested classification range")
    return rows


def delete_existing_range(conn: Any, rule_version: str, start_date: dt.date, end_date: dt.date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM market.ann_risk_signal s
             USING market.anns a
             WHERE s.ann_id = a.id
               AND s.rule_version = %s
               AND a.ann_date >= %s
               AND a.ann_date <= %s
            """,
            (rule_version, start_date, end_date),
        )
        cur.execute(
            """
            DELETE FROM market.ann_event_classification c
             USING market.anns a
             WHERE c.ann_id = a.id
               AND c.rule_version = %s
               AND a.ann_date >= %s
               AND a.ann_date <= %s
            """,
            (rule_version, start_date, end_date),
        )


def fetch_batch(
    conn: Any,
    *,
    start_date: dt.date,
    end_date: dt.date,
    last_id: int,
    batch_size: int,
    rule_version: str,
    missing_only: bool,
    remaining_limit: Optional[int],
) -> list[dict[str, Any]]:
    limit = min(batch_size, remaining_limit) if remaining_limit is not None else batch_size
    if limit <= 0:
        return []
    missing_sql = ""
    params: list[Any] = [start_date, end_date, last_id]
    if missing_only:
        missing_sql = """
           AND NOT EXISTS (
               SELECT 1
                 FROM market.ann_event_classification c
                WHERE c.ann_id = a.id
                  AND c.rule_version = %s
           )
        """
        params.append(rule_version)
    params.append(limit)
    sql = f"""
        SELECT a.id, a.ann_date, a.ts_code, a.name, a.title, a.rec_time
          FROM market.anns a
         WHERE a.ann_date >= %s
           AND a.ann_date <= %s
           AND a.id > %s
           {missing_sql}
         ORDER BY a.id
         LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, tuple(params))
        return [dict(row) for row in cur.fetchall()]


def classification_tuple(
    row: dict[str, Any],
    result: ClassificationResult,
    effective: EffectiveDateResult,
    detail: dict[str, Any],
) -> tuple[Any, ...]:
    return (
        row["id"],
        row["ts_code"],
        row["ann_date"],
        title_hash(row["title"]),
        result.rule_version,
        result.event_type,
        result.risk_level,
        result.action,
        result.needs_llm,
        result.matched_rule,
        result.matched_text,
        effective.source_time_quality,
        effective.effective_trade_date,
        effective.effective_rule,
        result.confidence,
        result.severity_score,
        json.dumps(detail, ensure_ascii=False, default=str),
    )


def signal_tuple(
    row: dict[str, Any],
    result: ClassificationResult,
    effective: EffectiveDateResult,
) -> tuple[Any, ...]:
    evidence = {
        "ann_id": row["id"],
        "title": row["title"],
        "matched_rule": result.matched_rule,
        "matched_text": result.matched_text,
        "source_time_quality": effective.source_time_quality,
        "effective_rule": effective.effective_rule,
    }
    reason = f"{result.risk_level} {result.event_type}: {result.description}"
    return (
        row["id"],
        row["ts_code"],
        row["ann_date"],
        result.rule_version,
        result.event_type,
        result.risk_level,
        result.action,
        effective.source_time_quality,
        effective.effective_trade_date,
        "ACTIVE",
        result.severity_score,
        result.confidence,
        reason,
        json.dumps(evidence, ensure_ascii=False, default=str),
    )


def persist_batch(
    conn: Any,
    *,
    classifications: list[tuple[Any, ...]],
    signals: list[tuple[Any, ...]],
    processed_ann_ids: list[int],
    rule_version: str,
    generate_signals: bool,
) -> None:
    with conn.cursor() as cur:
        if classifications:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO market.ann_event_classification
                    (
                        ann_id, ts_code, ann_date, title_hash, rule_version,
                        event_type, risk_level, action, needs_llm,
                        matched_rule, matched_text, source_time_quality,
                        effective_trade_date, effective_rule,
                        confidence, severity_score, classification_detail
                    )
                VALUES %s
                ON CONFLICT (ann_id, rule_version) DO UPDATE SET
                    ts_code = EXCLUDED.ts_code,
                    ann_date = EXCLUDED.ann_date,
                    title_hash = EXCLUDED.title_hash,
                    event_type = EXCLUDED.event_type,
                    risk_level = EXCLUDED.risk_level,
                    action = EXCLUDED.action,
                    needs_llm = EXCLUDED.needs_llm,
                    matched_rule = EXCLUDED.matched_rule,
                    matched_text = EXCLUDED.matched_text,
                    source_time_quality = EXCLUDED.source_time_quality,
                    effective_trade_date = EXCLUDED.effective_trade_date,
                    effective_rule = EXCLUDED.effective_rule,
                    confidence = EXCLUDED.confidence,
                    severity_score = EXCLUDED.severity_score,
                    classification_detail = EXCLUDED.classification_detail,
                    classified_at = NOW(),
                    updated_at = NOW()
                """,
                classifications,
                page_size=1000,
            )

        if generate_signals and processed_ann_ids:
            risk_ids = [item[0] for item in signals]
            if risk_ids:
                cur.execute(
                    """
                    DELETE FROM market.ann_risk_signal
                     WHERE rule_version = %s
                       AND ann_id = ANY(%s)
                       AND NOT (ann_id = ANY(%s))
                    """,
                    (rule_version, processed_ann_ids, risk_ids),
                )
            else:
                cur.execute(
                    """
                    DELETE FROM market.ann_risk_signal
                     WHERE rule_version = %s
                       AND ann_id = ANY(%s)
                    """,
                    (rule_version, processed_ann_ids),
                )
        if generate_signals and signals:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO market.ann_risk_signal
                    (
                        ann_id, ts_code, ann_date, rule_version,
                        event_type, risk_level, action, source_time_quality,
                        effective_trade_date, signal_status, severity_score,
                        confidence, reason, evidence
                    )
                VALUES %s
                ON CONFLICT (ann_id, rule_version) DO UPDATE SET
                    ts_code = EXCLUDED.ts_code,
                    ann_date = EXCLUDED.ann_date,
                    event_type = EXCLUDED.event_type,
                    risk_level = EXCLUDED.risk_level,
                    action = EXCLUDED.action,
                    source_time_quality = EXCLUDED.source_time_quality,
                    effective_trade_date = EXCLUDED.effective_trade_date,
                    signal_status = EXCLUDED.signal_status,
                    severity_score = EXCLUDED.severity_score,
                    confidence = EXCLUDED.confidence,
                    reason = EXCLUDED.reason,
                    evidence = EXCLUDED.evidence,
                    generated_at = NOW(),
                    updated_at = NOW()
                """,
                signals,
                page_size=1000,
            )


def sorted_counter(counter: Counter[str]) -> dict[str, int]:
    return dict(counter.most_common())


def write_reports(summary: dict[str, Any], json_path: Path, md_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    lines: list[str] = [
        "# AIstock Announcement Title Classification v0",
        "",
        f"- Rule version: `{summary['rule_version']}`",
        f"- Rows processed: `{summary['processed_rows']}`",
        f"- Persisted: `{summary['persisted']}`",
        f"- Risk signals written/touched: `{summary['signal_rows']}`",
        f"- Date range: `{summary['start_date']}` to `{summary['end_date']}`",
        "",
        "## Risk Level Counts",
        "",
        "| risk_level | rows |",
        "| --- | ---: |",
    ]
    for key, value in summary["counts_by_risk_level"].items():
        lines.append(f"| {key} | {value} |")
    lines.extend(["", "## Event Type Counts", "", "| event_type | rows |", "| --- | ---: |"])
    for key, value in summary["counts_by_event_type"].items():
        lines.append(f"| {key} | {value} |")
    lines.extend(["", "## Engine Interpretation", ""])
    lines.append("- `P0_BLOCK`: hard risk title; first-stage consumers can forbid new buys without PDF/LLM.")
    lines.append("- `P1_HIGH`: high-risk warning; PDF/LLM is optional for explanation, not required before warning.")
    lines.append("- `P2_REVIEW`: review candidate; use later PDF/LLM only when material to positions or risk policy.")
    lines.append("- `P3_POSITIVE_CANDIDATE`: record-only; positive alpha stays disabled until event-study validation.")
    lines.append("- `P4_NEUTRAL`: archive or discard for first-stage trading decisions.")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be positive when provided")

    load_dotenv(ROOT / ".env", override=True)
    classifier = AnnouncementTitleClassifier(rule_version=args.rule_version)
    json_path, md_path = default_report_paths()
    if args.json_out:
        json_path = Path(args.json_out)
    if args.md_out:
        md_path = Path(args.md_out)

    if args.persist:
        init_announcement_event_schema()

    started = time.time()
    counts_by_level: Counter[str] = Counter()
    counts_by_type: Counter[str] = Counter()
    counts_by_action: Counter[str] = Counter()
    counts_by_time_quality: Counter[str] = Counter()
    counts_by_effective_rule: Counter[str] = Counter()
    counts_by_year_level: dict[str, Counter[str]] = defaultdict(Counter)
    samples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    processed = 0
    signal_rows = 0
    last_id = 0

    with get_conn() as conn:
        start_date, end_date = load_date_bounds(conn, parse_date(args.start_date), parse_date(args.end_date))
        trading_days = load_trading_days(conn, start_date, end_date)
        if args.persist:
            seed_rule_metadata(conn, classifier)
            if args.truncate_version:
                delete_existing_range(conn, classifier.rule_version, start_date, end_date)

        while True:
            remaining_limit = None if args.limit is None else args.limit - processed
            rows = fetch_batch(
                conn,
                start_date=start_date,
                end_date=end_date,
                last_id=last_id,
                batch_size=args.batch_size,
                rule_version=classifier.rule_version,
                missing_only=args.missing_only,
                remaining_limit=remaining_limit,
            )
            if not rows:
                break

            classification_rows: list[tuple[Any, ...]] = []
            signal_write_rows: list[tuple[Any, ...]] = []
            processed_ids: list[int] = []

            for row in rows:
                last_id = int(row["id"])
                processed_ids.append(last_id)
                result = classifier.classify(row["title"])
                effective = classifier.infer_effective_date(row["ann_date"], row.get("rec_time"), trading_days)
                detail = {
                    "engine": ENGINE_NAME,
                    "rule_version": classifier.rule_version,
                    "description": result.description,
                    "title": row["title"],
                    "rec_time": row.get("rec_time"),
                }

                classification_rows.append(classification_tuple(row, result, effective, detail))
                if result.risk_level in SIGNAL_RISK_LEVELS:
                    signal_write_rows.append(signal_tuple(row, result, effective))

                counts_by_level[result.risk_level] += 1
                counts_by_type[result.event_type] += 1
                counts_by_action[result.action] += 1
                counts_by_time_quality[effective.source_time_quality] += 1
                counts_by_effective_rule[effective.effective_rule] += 1
                counts_by_year_level[str(row["ann_date"].year)][result.risk_level] += 1
                if len(samples[result.event_type]) < 5:
                    samples[result.event_type].append(
                        {
                            "ann_id": row["id"],
                            "ann_date": row["ann_date"],
                            "ts_code": row["ts_code"],
                            "title": row["title"],
                            "risk_level": result.risk_level,
                            "effective_trade_date": effective.effective_trade_date,
                        }
                    )

            if args.persist:
                persist_batch(
                    conn,
                    classifications=classification_rows,
                    signals=signal_write_rows,
                    processed_ann_ids=processed_ids,
                    rule_version=classifier.rule_version,
                    generate_signals=args.generate_signals,
                )

            processed += len(rows)
            signal_rows += len(signal_write_rows)
            if processed % max(args.batch_size * 10, 1) == 0:
                print(
                    json.dumps(
                        {
                            "event": "progress",
                            "processed": processed,
                            "last_id": last_id,
                            "signals": signal_rows,
                            "elapsed_sec": round(time.time() - started, 1),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
            if args.limit is not None and processed >= args.limit:
                break

    summary = {
        "rule_version": classifier.rule_version,
        "rule_count": len(RULES),
        "rule_config_hash": rule_config_hash(classifier.rules),
        "processed_rows": processed,
        "signal_rows": signal_rows,
        "persisted": bool(args.persist),
        "generate_signals": bool(args.generate_signals),
        "missing_only": bool(args.missing_only),
        "start_date": start_date,
        "end_date": end_date,
        "elapsed_sec": round(time.time() - started, 3),
        "counts_by_risk_level": sorted_counter(counts_by_level),
        "counts_by_event_type": sorted_counter(counts_by_type),
        "counts_by_action": sorted_counter(counts_by_action),
        "counts_by_time_quality": sorted_counter(counts_by_time_quality),
        "counts_by_effective_rule": sorted_counter(counts_by_effective_rule),
        "counts_by_year_level": {year: dict(counter) for year, counter in sorted(counts_by_year_level.items())},
        "samples": {key: value for key, value in samples.items()},
    }
    write_reports(summary, json_path, md_path)
    print(
        json.dumps(
            {
                "json": str(json_path),
                "markdown": str(md_path),
                "processed_rows": processed,
                "signal_rows": signal_rows,
                "elapsed_sec": summary["elapsed_sec"],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
