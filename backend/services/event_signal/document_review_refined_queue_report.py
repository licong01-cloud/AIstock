"""Read-only refined queue report for future announcement PDF/LLM review.

This report applies materiality downgrades and dedupe on row-level title
classifications.  It is deliberately read-only: no queue table, no PDF download,
no LLM call, and no trading-consumer integration.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Optional

import psycopg2.extras
from dotenv import load_dotenv

from backend.db.pg_pool import get_conn
from backend.services.announcements.title_classifier import RULE_VERSION as ANNOUNCEMENT_RULE_VERSION
from backend.services.event_signal.document_queue_refiner import (
    RefinedDocumentDecision,
    dedupe_refined_decisions,
    refine_document_review_decision,
    summarize_refined_decisions,
)


ROOT = Path(__file__).resolve().parents[3]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=2)


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def fetch_classification_rows_batch(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    last_classification_id: int,
    batch_size: int,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
) -> list[dict[str, Any]]:
    params: list[Any] = [rule_version, time_mode, last_classification_id]
    date_sql = ""
    if start_date is not None:
        date_sql += " AND c.ann_date >= %s"
        params.append(start_date)
    if end_date is not None:
        date_sql += " AND c.ann_date <= %s"
        params.append(end_date)
    params.append(batch_size)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT c.classification_id,
                   c.ann_id,
                   c.ts_code,
                   c.ann_date,
                   c.event_type,
                   c.risk_level,
                   c.action,
                   c.needs_llm,
                   c.effective_trade_date,
                   c.matched_text,
                   a.title
              FROM market.ann_event_classification c
              JOIN market.anns a
                ON a.id = c.ann_id
             WHERE c.rule_version = %s
               AND c.time_mode = %s
               AND c.classification_id > %s
               {date_sql}
             ORDER BY c.classification_id
             LIMIT %s
            """,
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def _bump(bucket: dict[str, int], key: str, value: int = 1) -> None:
    bucket[key] = bucket.get(key, 0) + value


def _compact_decision(decision: RefinedDocumentDecision) -> dict[str, Any]:
    return {
        "ann_id": decision.base.ann_id,
        "ts_code": decision.base.ts_code,
        "event_type": decision.base.event_type,
        "risk_level": decision.base.risk_level,
        "title": decision.base.title,
        "effective_trade_date": decision.base.effective_trade_date,
        "refined_action": decision.refined_action,
        "refined_llm_stage": decision.refined_llm_stage,
        "refined_priority_score": decision.refined_priority_score,
        "materiality": decision.materiality.to_dict(),
        "reason_codes": decision.reason_codes,
    }


def build_refined_queue_summary(
    rows: list[dict[str, Any]],
    *,
    window_days: int = 30,
    top_n: int = 30,
) -> dict[str, Any]:
    decisions = [refine_document_review_decision(row, window_days=window_days) for row in rows]
    deduped = dedupe_refined_decisions([decision for decision in decisions if decision.require_document])
    summary = summarize_refined_decisions(decisions)
    deduped_summary = summarize_refined_decisions(deduped)
    return {
        "raw": summary,
        "deduped_document_queue": deduped_summary,
        "dedupe_removed_document_rows": summary["document_rows"] - deduped_summary["document_rows"],
        "top_document_candidates": [_compact_decision(decision) for decision in deduped[:top_n]],
    }


def stream_refined_queue_summary(
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
    batch_size: int = 20000,
    limit: Optional[int] = None,
    window_days: int = 30,
    top_n: int = 50,
) -> dict[str, Any]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    rows_processed = 0
    last_classification_id = 0
    raw_by_action: dict[str, int] = {}
    raw_by_stage: dict[str, int] = {}
    raw_by_event_type: dict[str, int] = {}
    raw_document_rows = 0
    raw_llm_rows = 0
    raw_material_rows = 0
    best_document_by_key: dict[str, RefinedDocumentDecision] = {}
    top_sample: list[RefinedDocumentDecision] = []

    with get_conn() as conn:
        while True:
            remaining = None if limit is None else limit - rows_processed
            if remaining is not None and remaining <= 0:
                break
            rows = fetch_classification_rows_batch(
                conn,
                rule_version=rule_version,
                time_mode=time_mode,
                last_classification_id=last_classification_id,
                batch_size=min(batch_size, remaining) if remaining is not None else batch_size,
                start_date=start_date,
                end_date=end_date,
            )
            if not rows:
                break
            last_classification_id = int(rows[-1]["classification_id"])
            for row in rows:
                decision = refine_document_review_decision(row, window_days=window_days)
                rows_processed += 1
                _bump(raw_by_action, decision.refined_action)
                _bump(raw_by_stage, decision.refined_llm_stage)
                _bump(raw_by_event_type, decision.base.event_type)
                if decision.require_document:
                    raw_document_rows += 1
                    current = best_document_by_key.get(decision.dedupe_key)
                    if current is None or decision.refined_priority_score > current.refined_priority_score:
                        best_document_by_key[decision.dedupe_key] = decision
                    top_sample.append(decision)
                    top_sample.sort(key=lambda item: (-item.refined_priority_score, item.base.ann_id or 0))
                    if len(top_sample) > top_n:
                        top_sample.pop()
                if decision.require_llm:
                    raw_llm_rows += 1
                if decision.materiality.is_material:
                    raw_material_rows += 1

    deduped = dedupe_refined_decisions(best_document_by_key.values())
    deduped_summary = summarize_refined_decisions(deduped)
    return {
        "raw": {
            "rows": rows_processed,
            "document_rows": raw_document_rows,
            "llm_rows": raw_llm_rows,
            "material_rows": raw_material_rows,
            "by_action": dict(sorted(raw_by_action.items())),
            "by_llm_stage": dict(sorted(raw_by_stage.items())),
            "by_event_type": dict(sorted(raw_by_event_type.items())),
        },
        "deduped_document_queue": deduped_summary,
        "dedupe_removed_document_rows": raw_document_rows - deduped_summary["document_rows"],
        "last_classification_id": last_classification_id,
        "top_document_candidates": [_compact_decision(decision) for decision in deduped[:top_n]],
    }


def write_refined_queue_report(*, payload: dict[str, Any], output_dir: Path, report_id: str) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    md_path = output_dir / f"{report_id}.md"
    json_path.write_text(_json_dumps(payload), encoding="utf-8")

    raw = payload["summary"]["raw"]
    deduped = payload["summary"]["deduped_document_queue"]
    lines = [
        "# Refined Announcement Document Review Queue Report",
        "",
        f"- Report id: `{report_id}`",
        f"- Rule version: `{payload['rule_version']}`",
        f"- Time mode: `{payload['time_mode']}`",
        f"- Source rows processed: `{raw['rows']}`",
        f"- Raw document rows: `{raw['document_rows']}`",
        f"- Deduped document rows: `{deduped['document_rows']}`",
        f"- Dedupe removed document rows: `{payload['summary']['dedupe_removed_document_rows']}`",
        f"- Raw LLM rows: `{raw['llm_rows']}`",
        f"- Material rows: `{raw['material_rows']}`",
        "",
        "## Refined Actions",
        "",
        "| action | rows |",
        "| --- | ---: |",
    ]
    for action, rows in raw["by_action"].items():
        lines.append(f"| {action} | {rows} |")
    lines.extend(
        [
            "",
            "## Top Document Candidates",
            "",
            "| ann_id | ts_code | event_type | priority | action | materiality | title |",
            "| ---: | --- | --- | ---: | --- | --- | --- |",
        ]
    )
    for row in payload["summary"]["top_document_candidates"][:30]:
        materiality = row["materiality"]
        amount = materiality.get("max_amount_yuan") or ""
        percent = materiality.get("max_percent") or ""
        title = str(row.get("title") or "").replace("|", " ")[:80]
        lines.append(
            f"| {row.get('ann_id')} | {row.get('ts_code')} | {row.get('event_type')} | "
            f"{row.get('refined_priority_score')} | {row.get('refined_action')} | "
            f"amount={amount};percent={percent} | {title} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def run_refined_queue_report(
    *,
    rule_version: str = ANNOUNCEMENT_RULE_VERSION,
    time_mode: str = "backtest",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    batch_size: int = 20000,
    limit: Optional[int] = None,
    window_days: int = 30,
    output_dir: Path = Path("reports/event_signal/document_review_refined_queue"),
) -> dict[str, Any]:
    summary = stream_refined_queue_summary(
        rule_version=rule_version,
        time_mode=time_mode,
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size,
        limit=limit,
        window_days=window_days,
    )
    report_id = "document_review_refined_queue_{}_{}_{}".format(
        start_date.isoformat() if start_date else "all",
        end_date.isoformat() if end_date else "all",
        dt.datetime.now().strftime("%Y%m%d_%H%M%S"),
    ).replace("-", "")
    payload = {
        "report_id": report_id,
        "rule_version": rule_version,
        "time_mode": time_mode,
        "start_date": start_date,
        "end_date": end_date,
        "batch_size": batch_size,
        "limit": limit,
        "window_days": window_days,
        "summary": summary,
    }
    payload["outputs"] = write_refined_queue_report(payload=payload, output_dir=output_dir, report_id=report_id)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estimate refined future announcement document-review queue size")
    parser.add_argument("--rule-version", default=ANNOUNCEMENT_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-size", type=int, default=20000)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--output-dir", default="reports/event_signal/document_review_refined_queue")
    return parser.parse_args()


def main() -> int:
    load_dotenv(ROOT / ".env", override=False)
    args = parse_args()
    payload = run_refined_queue_report(
        rule_version=args.rule_version,
        time_mode=args.time_mode,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        batch_size=args.batch_size,
        limit=args.limit,
        window_days=args.window_days,
        output_dir=Path(args.output_dir),
    )
    print(
        _json_dumps(
            {
                "report_id": payload["report_id"],
                "outputs": payload["outputs"],
                "summary": {
                    "raw": {
                        "rows": payload["summary"]["raw"]["rows"],
                        "document_rows": payload["summary"]["raw"]["document_rows"],
                        "llm_rows": payload["summary"]["raw"]["llm_rows"],
                    },
                    "deduped_document_rows": payload["summary"]["deduped_document_queue"]["document_rows"],
                },
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
