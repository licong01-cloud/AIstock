from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

from backend.services.qe_archive.archive_service import QEArchiveService  # noqa: E402
from backend.services.qe_archive.backfill_service import QEArchiveBackfillService  # noqa: E402
from backend.services.qe_archive.source_assembler import QEArchiveSourceAssembler  # noqa: E402


WRITE_CONFIRM_TEXT = "QE_ARCHIVE_WRITE"


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _process_payload(
    *,
    payload: dict[str, Any],
    event_type: str,
    dry_run: bool,
) -> dict[str, Any]:
    result = QEArchiveService().process_payload(
        payload,
        event_type=event_type,
        source_system=payload.get("source_system"),
        source_id=payload.get("source_id"),
        source_sub_id=payload.get("source_sub_id"),
        dry_run=dry_run,
    )
    return {
        "run_id": result.run_id,
        "dry_run": dry_run,
        "source_system": payload.get("source_system"),
        "source_id": payload.get("source_id"),
        "source_sub_id": payload.get("source_sub_id"),
        "stats": result.stats,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Assemble existing QE DB rows into qe_archive payloads. "
            "Default mode is dry-run and does not write qe_archive rows."
        )
    )
    parser.add_argument("--source", choices=("experiment", "loop", "multi-alpha", "all"), default="all")
    parser.add_argument("--experiment-id", help="Archive or preview one qe_experiments row.")
    parser.add_argument("--loop-id", help="Archive or preview one qe_evolution_loops row.")
    parser.add_argument("--task-id", help="Task id used with --loop-index.")
    parser.add_argument("--loop-index", type=int, help="Loop index used with --task-id.")
    parser.add_argument("--limit", type=int, default=10, help="Maximum rows to preview/backfill per source.")
    parser.add_argument("--status", default="completed", help="Source status for list mode. Default: completed.")
    parser.add_argument("--write", action="store_true", help="Write qe_archive rows instead of dry-run preview.")
    parser.add_argument(
        "--confirm-write",
        default="",
        help=f"Required with --write. Must equal {WRITE_CONFIRM_TEXT!r}.",
    )
    parser.add_argument("--output", type=Path, help="Optional JSON report output path.")
    args = parser.parse_args()

    if args.write and args.confirm_write != WRITE_CONFIRM_TEXT:
        parser.error(f"--write requires --confirm-write {WRITE_CONFIRM_TEXT}")

    load_dotenv(REPO_ROOT / ".env", override=False)
    assembler = QEArchiveSourceAssembler()
    backfill_service = QEArchiveBackfillService()
    dry_run = not args.write
    results: list[dict[str, Any]] = []

    def process_multi_alpha() -> dict[str, Any]:
        return backfill_service.backfill_multi_alpha_combine_runs(
            write=args.write,
            confirm_write=args.confirm_write,
            include_archived=False,
            limit=args.limit,
        )

    if args.source == "multi-alpha":
        report = process_multi_alpha()
        text = json.dumps(report, ensure_ascii=False, indent=2, default=_json_default)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text, encoding="utf-8")
        print(text)
        return 0

    if args.experiment_id:
        payload = assembler.assemble_experiment_payload(args.experiment_id)
        results.append(_process_payload(payload=payload, event_type="qe.experiment.completed", dry_run=dry_run))
    elif args.loop_id or (args.task_id and args.loop_index is not None):
        payload = assembler.assemble_loop_payload(
            loop_id=args.loop_id,
            task_id=args.task_id,
            loop_index=args.loop_index,
        )
        results.append(_process_payload(payload=payload, event_type="qe.loop.completed", dry_run=dry_run))
    else:
        if args.source in {"experiment", "all"}:
            for experiment_id in assembler.list_experiment_ids(status=args.status, limit=args.limit):
                payload = assembler.assemble_experiment_payload(experiment_id)
                results.append(_process_payload(payload=payload, event_type="qe.experiment.completed", dry_run=dry_run))
        if args.source in {"loop", "all"}:
            for ref in assembler.list_loop_refs(status=args.status, limit=args.limit):
                payload = assembler.assemble_loop_payload(
                    loop_id=ref["loop_id"],
                    task_id=ref.get("task_id"),
                    loop_index=ref.get("loop_index"),
                )
                results.append(_process_payload(payload=payload, event_type="qe.loop.completed", dry_run=dry_run))

    report = {
        "dry_run": dry_run,
        "write_enabled": args.write,
        "source": args.source,
        "status": args.status,
        "processed_count": len(results),
        "results": results,
    }
    if args.source == "all":
        report["multi_alpha_report"] = process_multi_alpha()
        report["processed_count"] += int(report["multi_alpha_report"].get("processed_count") or 0)
    text = json.dumps(report, ensure_ascii=False, indent=2, default=_json_default)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
