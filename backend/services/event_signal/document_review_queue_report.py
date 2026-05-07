"""Read-only queue-size report for future announcement document review.

This module estimates how many already-classified announcements would enter the
future PDF/LLM review pipeline under the deterministic planner.  It does not
create queue tables, download files, call LLMs, or touch trading consumers.
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
from backend.services.event_signal.document_review_planner import plan_document_review


ROOT = Path(__file__).resolve().parents[3]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=2)


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def load_classification_group_counts(
    conn: Any,
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
) -> list[dict[str, Any]]:
    params: list[Any] = [rule_version, time_mode]
    date_sql = ""
    if start_date is not None:
        date_sql += " AND ann_date >= %s"
        params.append(start_date)
    if end_date is not None:
        date_sql += " AND ann_date <= %s"
        params.append(end_date)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT event_type, risk_level, action, needs_llm, count(*)::bigint AS rows
              FROM market.ann_event_classification
             WHERE rule_version = %s
               AND time_mode = %s
               {date_sql}
             GROUP BY event_type, risk_level, action, needs_llm
             ORDER BY rows DESC, event_type
            """,
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def estimate_queue_from_group_counts(group_counts: list[dict[str, Any]]) -> dict[str, Any]:
    by_action: dict[str, int] = {}
    by_stage: dict[str, int] = {}
    by_event_type: dict[str, int] = {}
    detail_rows: list[dict[str, Any]] = []
    total = 0
    document_rows = 0
    llm_rows = 0
    document_llm_rows = 0

    for group in group_counts:
        rows = int(group.get("rows") or 0)
        total += rows
        decision = plan_document_review(
            {
                "event_type": group.get("event_type"),
                "risk_level": group.get("risk_level"),
                "action": group.get("action"),
                "needs_llm": group.get("needs_llm"),
            }
        )
        by_action[decision.queue_action] = by_action.get(decision.queue_action, 0) + rows
        by_stage[decision.llm_stage] = by_stage.get(decision.llm_stage, 0) + rows
        by_event_type[decision.event_type] = by_event_type.get(decision.event_type, 0) + rows
        if decision.require_document:
            document_rows += rows
        if decision.require_llm:
            llm_rows += rows
        if decision.require_document and decision.require_llm:
            document_llm_rows += rows
        detail_rows.append(
            {
                "event_type": decision.event_type,
                "risk_level": decision.risk_level,
                "needs_llm": group.get("needs_llm"),
                "source_rows": rows,
                "queue_action": decision.queue_action,
                "llm_stage": decision.llm_stage,
                "priority_score": decision.priority_score,
                "route_event_types": decision.route_event_types,
                "reason_codes": decision.reason_codes,
            }
        )

    detail_rows.sort(key=lambda row: (-int(row["source_rows"]), row["event_type"], row["queue_action"]))
    return {
        "source_rows": total,
        "document_required_or_candidate_rows": document_rows,
        "llm_candidate_rows": llm_rows,
        "document_llm_candidate_rows": document_llm_rows,
        "by_action": dict(sorted(by_action.items())),
        "by_llm_stage": dict(sorted(by_stage.items())),
        "by_event_type": dict(sorted(by_event_type.items())),
        "details": detail_rows,
    }


def write_queue_report(*, payload: dict[str, Any], output_dir: Path, report_id: str) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    md_path = output_dir / f"{report_id}.md"
    json_path.write_text(_json_dumps(payload), encoding="utf-8")

    lines = [
        "# Announcement Document Review Queue Estimate",
        "",
        f"- Report id: `{report_id}`",
        f"- Rule version: `{payload['rule_version']}`",
        f"- Time mode: `{payload['time_mode']}`",
        f"- Source rows: `{payload['summary']['source_rows']}`",
        f"- Document required/candidate rows: `{payload['summary']['document_required_or_candidate_rows']}`",
        f"- LLM candidate rows: `{payload['summary']['llm_candidate_rows']}`",
        f"- Document + LLM candidate rows: `{payload['summary']['document_llm_candidate_rows']}`",
        "",
        "## By Action",
        "",
        "| action | rows |",
        "| --- | ---: |",
    ]
    for action, rows in payload["summary"]["by_action"].items():
        lines.append(f"| {action} | {rows} |")
    lines.extend(
        [
            "",
            "## Largest Event Groups",
            "",
            "| event_type | rows | action | stage | reasons |",
            "| --- | ---: | --- | --- | --- |",
        ]
    )
    for row in payload["summary"]["details"][:30]:
        lines.append(
            f"| {row['event_type']} | {row['source_rows']} | {row['queue_action']} | "
            f"{row['llm_stage']} | {','.join(row['reason_codes'])} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def run_document_review_queue_report(
    *,
    rule_version: str = ANNOUNCEMENT_RULE_VERSION,
    time_mode: str = "backtest",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    output_dir: Path = Path("reports/event_signal/document_review_queue"),
) -> dict[str, Any]:
    with get_conn() as conn:
        group_counts = load_classification_group_counts(
            conn,
            rule_version=rule_version,
            time_mode=time_mode,
            start_date=start_date,
            end_date=end_date,
        )
    summary = estimate_queue_from_group_counts(group_counts)
    report_id = "document_review_queue_{}_{}_{}".format(
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
        "summary": summary,
    }
    payload["outputs"] = write_queue_report(payload=payload, output_dir=output_dir, report_id=report_id)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estimate future announcement document-review queue size")
    parser.add_argument("--rule-version", default=ANNOUNCEMENT_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default="reports/event_signal/document_review_queue")
    return parser.parse_args()


def main() -> int:
    load_dotenv(ROOT / ".env", override=False)
    args = parse_args()
    payload = run_document_review_queue_report(
        rule_version=args.rule_version,
        time_mode=args.time_mode,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        output_dir=Path(args.output_dir),
    )
    print(_json_dumps({"report_id": payload["report_id"], "outputs": payload["outputs"], "summary": payload["summary"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
