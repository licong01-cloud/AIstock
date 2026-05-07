"""Read-only first-wave sample report for announcement document analysis.

The report caps the refined document queue into a manageable parser/LLM
validation sample. It does not persist queue rows, download files, or call LLMs.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from backend.db.pg_pool import get_conn
from backend.services.announcements.title_classifier import RULE_VERSION as ANNOUNCEMENT_RULE_VERSION
from backend.services.event_signal.document_first_wave_sampler import (
    FirstWaveConfig,
    compact_first_wave_row,
    select_first_wave_candidates,
    summarize_first_wave,
)
from backend.services.event_signal.document_queue_refiner import dedupe_refined_decisions, refine_document_review_decision
from backend.services.event_signal.document_review_refined_queue_report import fetch_classification_rows_batch


ROOT = Path(__file__).resolve().parents[3]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=2)


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def build_first_wave_payload(
    *,
    refined_document_decisions: list[Any],
    config: FirstWaveConfig,
    top_n: int = 100,
) -> dict[str, Any]:
    deduped = dedupe_refined_decisions(refined_document_decisions)
    selected = select_first_wave_candidates(deduped, config=config)
    return {
        "eligible_deduped_document_rows": len(deduped),
        "first_wave": summarize_first_wave(selected, eligible_count=len(deduped)),
        "selected_examples": [compact_first_wave_row(decision) for decision in selected[:top_n]],
    }


def collect_refined_document_decisions(
    *,
    rule_version: str,
    time_mode: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
    batch_size: int,
    limit: Optional[int],
    window_days: int,
) -> tuple[list[Any], int, int]:
    decisions: list[Any] = []
    rows_processed = 0
    last_classification_id = 0
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
                rows_processed += 1
                decision = refine_document_review_decision(row, window_days=window_days)
                if decision.require_document:
                    decisions.append(decision)
    return decisions, rows_processed, last_classification_id


def write_first_wave_report(*, payload: dict[str, Any], output_dir: Path, report_id: str) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    md_path = output_dir / f"{report_id}.md"
    json_path.write_text(_json_dumps(payload), encoding="utf-8")

    first_wave = payload["summary"]["first_wave"]
    lines = [
        "# Announcement Document First-wave Sample Report",
        "",
        f"- Report id: `{report_id}`",
        f"- Rule version: `{payload['rule_version']}`",
        f"- Time mode: `{payload['time_mode']}`",
        f"- Source rows processed: `{payload['source_rows_processed']}`",
        f"- Eligible deduped document rows: `{payload['summary']['eligible_deduped_document_rows']}`",
        f"- Selected rows: `{first_wave['selected_rows']}`",
        f"- Material selected rows: `{first_wave['material_rows']}`",
        f"- Total cap: `{payload['config']['total_cap']}`",
        f"- Per event-year cap: `{payload['config']['per_event_year_cap']}`",
        "",
        "## Selected By Event Type",
        "",
        "| event_type | rows |",
        "| --- | ---: |",
    ]
    for event_type, rows in first_wave["by_event_type"].items():
        lines.append(f"| {event_type} | {rows} |")
    lines.extend(
        [
            "",
            "## Selected By Year",
            "",
            "| year | rows |",
            "| --- | ---: |",
        ]
    )
    for year, rows in first_wave["by_year"].items():
        lines.append(f"| {year} | {rows} |")
    lines.extend(
        [
            "",
            "## Examples",
            "",
            "| ann_id | ts_code | event_type | priority | title |",
            "| ---: | --- | --- | ---: | --- |",
        ]
    )
    for row in payload["summary"]["selected_examples"][:30]:
        title = str(row.get("title") or "").replace("|", " ")[:80]
        lines.append(
            f"| {row.get('ann_id')} | {row.get('ts_code')} | {row.get('event_type')} | "
            f"{row.get('priority_score')} | {title} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def run_first_wave_report(
    *,
    rule_version: str = ANNOUNCEMENT_RULE_VERSION,
    time_mode: str = "backtest",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    batch_size: int = 50000,
    limit: Optional[int] = None,
    window_days: int = 30,
    total_cap: int = 5000,
    default_event_type_cap: int = 300,
    per_event_year_cap: int = 120,
    output_dir: Path = Path("reports/event_signal/document_first_wave"),
) -> dict[str, Any]:
    decisions, rows_processed, last_classification_id = collect_refined_document_decisions(
        rule_version=rule_version,
        time_mode=time_mode,
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size,
        limit=limit,
        window_days=window_days,
    )
    config = FirstWaveConfig(
        total_cap=total_cap,
        default_event_type_cap=default_event_type_cap,
        per_event_year_cap=per_event_year_cap,
    )
    summary = build_first_wave_payload(refined_document_decisions=decisions, config=config)
    report_id = "document_first_wave_{}_{}_{}".format(
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
        "source_rows_processed": rows_processed,
        "last_classification_id": last_classification_id,
        "window_days": window_days,
        "config": {
            "total_cap": config.total_cap,
            "default_event_type_cap": config.default_event_type_cap,
            "per_event_year_cap": config.per_event_year_cap,
            "event_type_caps": dict(config.event_type_caps),
        },
        "summary": summary,
    }
    payload["outputs"] = write_first_wave_report(payload=payload, output_dir=output_dir, report_id=report_id)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a capped first-wave announcement document sample report")
    parser.add_argument("--rule-version", default=ANNOUNCEMENT_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--total-cap", type=int, default=5000)
    parser.add_argument("--default-event-type-cap", type=int, default=300)
    parser.add_argument("--per-event-year-cap", type=int, default=120)
    parser.add_argument("--output-dir", default="reports/event_signal/document_first_wave")
    return parser.parse_args()


def main() -> int:
    load_dotenv(ROOT / ".env", override=False)
    args = parse_args()
    payload = run_first_wave_report(
        rule_version=args.rule_version,
        time_mode=args.time_mode,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        batch_size=args.batch_size,
        limit=args.limit,
        window_days=args.window_days,
        total_cap=args.total_cap,
        default_event_type_cap=args.default_event_type_cap,
        per_event_year_cap=args.per_event_year_cap,
        output_dir=Path(args.output_dir),
    )
    print(
        _json_dumps(
            {
                "report_id": payload["report_id"],
                "outputs": payload["outputs"],
                "source_rows_processed": payload["source_rows_processed"],
                "eligible_deduped_document_rows": payload["summary"]["eligible_deduped_document_rows"],
                "selected_rows": payload["summary"]["first_wave"]["selected_rows"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
