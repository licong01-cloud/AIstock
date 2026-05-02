"""API-oriented QE archive backfill orchestration.

The service replaces manual script-only historical补录 with a reusable backend
entry point. It still requires explicit write confirmation for API-triggered
mutations and never opens QE/RD-Agent worker workspace files.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .archive_service import QEArchiveService
from .repository import QEArchiveRepository
from .source_assembler import QEArchiveSourceAssembler


WRITE_CONFIRM_TEXT = "QE_ARCHIVE_WRITE"
SUPPORTED_SOURCES = {"experiment", "loop", "task", "all"}


@dataclass(frozen=True)
class QEArchiveBackfillOptions:
    source: str = "loop"
    experiment_ids: Sequence[str] = ()
    task_ids: Sequence[str] = ()
    loop_ids: Sequence[str] = ()
    task_id: str | None = None
    loop_index: int | None = None
    status: str = "completed"
    limit: int = 20
    write: bool = False
    confirm_write: str = ""
    validate_after_write: bool = True
    min_metrics: int = 0
    min_curves: int = 0
    min_factors: int = 0
    require_account_summary: bool = False


class QEArchiveBackfillService:
    """Assemble existing QE DB rows and archive them through one API path."""

    def __init__(
        self,
        *,
        assembler: QEArchiveSourceAssembler | None = None,
        archive_service: QEArchiveService | None = None,
        repository: QEArchiveRepository | None = None,
    ) -> None:
        self._assembler = assembler or QEArchiveSourceAssembler()
        self._repository = repository or QEArchiveRepository()
        self._archive_service = archive_service or QEArchiveService(repository=self._repository)

    def process_backfill(self, options: QEArchiveBackfillOptions) -> dict[str, Any]:
        source = (options.source or "loop").strip().lower()
        if source not in SUPPORTED_SOURCES:
            raise ValueError(f"unsupported QE archive backfill source: {options.source}")
        if options.write and options.confirm_write != WRITE_CONFIRM_TEXT:
            raise ValueError(f"write mode requires confirm_write={WRITE_CONFIRM_TEXT!r}")

        candidates = self._build_candidates(options, source=source)
        results = [
            self._process_candidate(candidate, write=options.write, options=options)
            for candidate in candidates
        ]
        return {
            "dry_run": not options.write,
            "write_enabled": options.write,
            "source": source,
            "status": options.status,
            "processed_count": len(results),
            "results": results,
            "archive_summary": self._repository.get_archive_summary() if options.write else None,
        }

    def list_backfill_candidates(
        self,
        *,
        status: str = "completed",
        limit: int = 100,
        include_archived: bool = False,
    ) -> dict[str, Any]:
        candidates = self._assembler.list_backfill_candidates(
            status=status,
            limit=limit,
            include_archived=include_archived,
        )
        return {
            "status": status,
            "include_archived": include_archived,
            "count": len(candidates),
            "candidates": candidates,
        }

    def archive_loop_completed(
        self,
        *,
        loop_id: str,
        task_id: str | None = None,
        loop_index: int | None = None,
        validate_after_write: bool = True,
    ) -> dict[str, Any]:
        """Archive one completed loop, intended for realtime hook/API reuse."""

        options = QEArchiveBackfillOptions(
            source="loop",
            loop_ids=[loop_id],
            task_id=task_id,
            loop_index=loop_index,
            write=True,
            confirm_write=WRITE_CONFIRM_TEXT,
            validate_after_write=validate_after_write,
        )
        return self.process_backfill(options)

    def archive_experiment_completed(
        self,
        *,
        experiment_id: str,
        validate_after_write: bool = True,
    ) -> dict[str, Any]:
        """Archive one completed single experiment, intended for realtime reuse."""

        options = QEArchiveBackfillOptions(
            source="experiment",
            experiment_ids=[experiment_id],
            write=True,
            confirm_write=WRITE_CONFIRM_TEXT,
            validate_after_write=validate_after_write,
        )
        return self.process_backfill(options)

    def _build_candidates(self, options: QEArchiveBackfillOptions, *, source: str) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []

        for experiment_id in _dedupe_non_empty(options.experiment_ids):
            payload = self._assembler.assemble_experiment_payload(experiment_id)
            candidates.append({"event_type": "qe.experiment.completed", "payload": payload})

        task_ids = _dedupe_non_empty(options.task_ids)
        if task_ids:
            for ref in self._assembler.list_loop_refs_for_tasks(task_ids, status=options.status):
                payload = self._assembler.assemble_loop_payload(
                    loop_id=ref.get("loop_id"),
                    task_id=ref.get("task_id"),
                    loop_index=ref.get("loop_index"),
                )
                candidates.append({"event_type": "qe.loop.completed", "payload": payload})

        for loop_id in _dedupe_non_empty(options.loop_ids):
            payload = self._assembler.assemble_loop_payload(loop_id=loop_id)
            candidates.append({"event_type": "qe.loop.completed", "payload": payload})

        if options.task_id and options.loop_index is not None and not options.loop_ids:
            payload = self._assembler.assemble_loop_payload(
                task_id=options.task_id,
                loop_index=options.loop_index,
            )
            candidates.append({"event_type": "qe.loop.completed", "payload": payload})

        if candidates:
            return candidates

        limit = max(1, min(int(options.limit or 20), 500))
        if source in {"experiment", "all"}:
            for experiment_id in self._assembler.list_experiment_ids(status=options.status, limit=limit):
                payload = self._assembler.assemble_experiment_payload(experiment_id)
                candidates.append({"event_type": "qe.experiment.completed", "payload": payload})
        if source in {"loop", "all"}:
            for ref in self._assembler.list_loop_refs(status=options.status, limit=limit):
                payload = self._assembler.assemble_loop_payload(
                    loop_id=ref.get("loop_id"),
                    task_id=ref.get("task_id"),
                    loop_index=ref.get("loop_index"),
                )
                candidates.append({"event_type": "qe.loop.completed", "payload": payload})
        if source == "task":
            candidate_task_ids = [
                str(row["task_id"])
                for row in self._assembler.list_backfill_candidates(
                    status=options.status,
                    limit=limit,
                    include_archived=False,
                )
                if row.get("candidate_type") == "evolution_task" and row.get("task_id")
            ]
            for ref in self._assembler.list_loop_refs_for_tasks(candidate_task_ids, status=options.status):
                payload = self._assembler.assemble_loop_payload(
                    loop_id=ref.get("loop_id"),
                    task_id=ref.get("task_id"),
                    loop_index=ref.get("loop_index"),
                )
                candidates.append({"event_type": "qe.loop.completed", "payload": payload})
        return candidates

    def _process_candidate(
        self,
        candidate: Mapping[str, Any],
        *,
        write: bool,
        options: QEArchiveBackfillOptions,
    ) -> dict[str, Any]:
        payload = dict(candidate["payload"])
        result = self._archive_service.process_payload(
            payload,
            event_type=str(candidate["event_type"]),
            source_system=payload.get("source_system"),
            source_id=payload.get("source_id"),
            source_sub_id=payload.get("source_sub_id"),
            dry_run=not write,
        )
        item = {
            "run_id": result.run_id,
            "dry_run": not write,
            "event_type": str(candidate["event_type"]),
            "source_system": payload.get("source_system"),
            "source_id": payload.get("source_id"),
            "source_sub_id": payload.get("source_sub_id"),
            "stats": result.stats,
        }
        if write and options.validate_after_write:
            item["quality"] = self._validate_run(result.run_id, options)
        return item

    def _validate_run(self, run_id: str, options: QEArchiveBackfillOptions) -> dict[str, Any]:
        quality = self._repository.get_run_quality_summary(run_id)
        failures: list[str] = []
        if not quality.get("exists"):
            failures.append(f"run_id not found: {run_id}")
        if int(quality.get("metric_count") or 0) < options.min_metrics:
            failures.append(f"metric_count below required minimum {options.min_metrics}")
        if int(quality.get("curve_count") or 0) < options.min_curves:
            failures.append(f"curve_count below required minimum {options.min_curves}")
        if int(quality.get("factor_count_rows") or 0) < options.min_factors:
            failures.append(f"factor_count_rows below required minimum {options.min_factors}")
        if options.require_account_summary and int(quality.get("account_summary_count") or 0) < 1:
            failures.append("account summary is required but missing")
        quality["failures"] = failures
        quality["passed"] = not failures
        return quality


def _dedupe_non_empty(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result
