"""API-oriented QE archive backfill orchestration.

The service replaces manual script-only historical补录 with a reusable backend
entry point. It still requires explicit write confirmation for API-triggered
mutations and never opens QE/RD-Agent worker workspace files.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .archive_service import QEArchiveService
from .bootstrap_marker import REBOOTSTRAP_CONFIRM_TEXT, assert_can_broad_backfill, mark_bootstrap
from .ingest_history import record_ingest_history
from .models import BackfillRunItemRecord, BackfillRunRecord
from .policy import resolve_archive_policy
from .repository import QEArchiveRepository
from .skip_registry import record_policy_skip
from .source_assembler import QEArchiveSourceAssembler


WRITE_CONFIRM_TEXT = "QE_ARCHIVE_WRITE"
BACKFILL_CONFIRM_TEXT = "QE_ARCHIVE_BACKFILL"
SUPPORTED_SOURCES = {"experiment", "loop", "task", "all"}
SOURCE_MODE_TO_SOURCE = {
    "completed_single_experiments": "experiment",
    "completed_custom_evo_loops": "loop",
    "all_completed_qe_sources": "all",
    "specific_ids": "all",
}


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
    include_archived: bool = False
    write: bool = False
    confirm_write: str = ""
    validate_after_write: bool = True
    min_metrics: int = 0
    min_curves: int = 0
    min_factors: int = 0
    require_account_summary: bool = False


@dataclass(frozen=True)
class QEArchiveBackfillRunOptions:
    source_mode: str = "completed_custom_evo_loops"
    experiment_ids: Sequence[str] = ()
    task_ids: Sequence[str] = ()
    loop_ids: Sequence[str] = ()
    task_id: str | None = None
    loop_index: int | None = None
    status: str = "completed"
    limit: int = 20
    include_archived: bool = False
    validate_after_write: bool = True
    min_metrics: int = 0
    min_curves: int = 0
    min_factors: int = 0
    require_account_summary: bool = False
    confirm_backfill: str = ""
    force_rebackfill: str = ""
    requested_by: str = "mcp_or_ui"

    def to_legacy_options(self, *, write: bool) -> QEArchiveBackfillOptions:
        source = SOURCE_MODE_TO_SOURCE.get(self.source_mode)
        if source is None:
            raise ValueError(f"unsupported source_mode: {self.source_mode}")
        return QEArchiveBackfillOptions(
            source=source,
            experiment_ids=self.experiment_ids,
            task_ids=self.task_ids,
            loop_ids=self.loop_ids,
            task_id=self.task_id,
            loop_index=self.loop_index,
            status=self.status,
            limit=self.limit,
            include_archived=self.include_archived or bool(self.force_rebackfill == REBOOTSTRAP_CONFIRM_TEXT),
            write=write,
            confirm_write=WRITE_CONFIRM_TEXT if write else "",
            validate_after_write=self.validate_after_write,
            min_metrics=self.min_metrics,
            min_curves=self.min_curves,
            min_factors=self.min_factors,
            require_account_summary=self.require_account_summary,
        )


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

    def preview_backfill(self, options: QEArchiveBackfillRunOptions) -> dict[str, Any]:
        legacy = options.to_legacy_options(write=False)
        backfill_run_id = self._repository.upsert_backfill_run(
            BackfillRunRecord(
                source_mode=options.source_mode,
                mode="preview",
                status="completed",
                request_payload=_options_to_dict(options),
                requested_by=options.requested_by,
            )
        )
        candidates = self._build_candidates(legacy, source=legacy.source)
        results = [self._preview_candidate(candidate, backfill_run_id=backfill_run_id) for candidate in candidates]
        self._repository.update_backfill_run_status(
            backfill_run_id,
            status="completed",
            candidate_count=len(results),
            processed_count=0,
            ingested_count=0,
            skipped_count=sum(1 for item in results if item.get("archive_policy") != "AUTO"),
            failed_count=0,
        )
        return {
            "dry_run": True,
            "write_enabled": False,
            "backfill_run_id": backfill_run_id,
            "source_mode": options.source_mode,
            "source": legacy.source,
            "status": options.status,
            "processed_count": len(results),
            "results": results,
        }

    def execute_backfill(self, options: QEArchiveBackfillRunOptions) -> dict[str, Any]:
        if options.confirm_backfill != BACKFILL_CONFIRM_TEXT:
            raise ValueError(f"execute mode requires confirm_backfill={BACKFILL_CONFIRM_TEXT!r}")
        legacy = options.to_legacy_options(write=True)
        source_type = _source_type_for_mode(options.source_mode)
        if options.source_mode != "specific_ids":
            assert_can_broad_backfill(
                source_type,
                force_token=options.force_rebackfill,
                repository=self._repository,
            )
        mode = "rebootstrap" if options.force_rebackfill == REBOOTSTRAP_CONFIRM_TEXT else "execute"
        backfill_run_id = self._repository.upsert_backfill_run(
            BackfillRunRecord(
                source_mode=options.source_mode,
                mode=mode,
                status="running",
                request_payload=_options_to_dict(options),
                force_rebackfill=mode == "rebootstrap",
                confirm_token_used=True,
                requested_by=options.requested_by,
            )
        )
        if options.source_mode != "specific_ids":
            mark_bootstrap(
                source_type=source_type,
                mode=mode,
                backfill_run_id=backfill_run_id,
                status="running",
                operator=options.requested_by,
                repository=self._repository,
            )
        candidates = self._build_candidates(legacy, source=legacy.source)
        results: list[dict[str, Any]] = []
        failed_count = 0
        for candidate in candidates:
            try:
                results.append(
                    self._process_candidate(
                        candidate,
                        write=True,
                        options=legacy,
                        backfill_run_id=backfill_run_id,
                    )
                )
            except Exception as exc:
                failed_count += 1
                payload = dict(candidate.get("payload") or {})
                source_id = str(payload.get("source_id") or payload.get("task_id") or payload.get("experiment_id") or "unknown")
                source_sub_id = payload.get("source_sub_id") or payload.get("loop_id")
                self._repository.upsert_backfill_run_item(
                    BackfillRunItemRecord(
                        backfill_run_id=backfill_run_id,
                        source_system=str(payload.get("source_system") or "qe"),
                        source_type="loop" if candidate.get("event_type") == "qe.loop.completed" else "experiment",
                        source_id=source_id,
                        source_sub_id=str(source_sub_id) if source_sub_id else None,
                        status="failed",
                        error_message=f"{type(exc).__name__}: {exc}",
                    )
                )
                results.append({"source_id": source_id, "source_sub_id": source_sub_id, "error": f"{type(exc).__name__}: {exc}"})
        ingested_count = sum(1 for item in results if item.get("run_id") and not item.get("dry_run"))
        skipped_count = sum(1 for item in results if item.get("skipped_reason"))
        final_status = "completed" if failed_count == 0 else ("partial" if ingested_count or skipped_count else "failed")
        self._repository.update_backfill_run_status(
            backfill_run_id,
            status=final_status,
            candidate_count=len(candidates),
            processed_count=len(results),
            ingested_count=ingested_count,
            skipped_count=skipped_count,
            failed_count=failed_count,
        )
        if options.source_mode != "specific_ids":
            mark_bootstrap(
                source_type=source_type,
                mode=mode,
                backfill_run_id=backfill_run_id,
                status="completed" if final_status in {"completed", "partial"} else "failed",
                operator=options.requested_by,
                ingested_count=ingested_count,
                skipped_count=skipped_count,
                failed_count=failed_count,
                stats={"final_status": final_status},
                repository=self._repository,
            )
        return {
            "dry_run": False,
            "write_enabled": True,
            "backfill_run_id": backfill_run_id,
            "source_mode": options.source_mode,
            "source": legacy.source,
            "status": options.status,
            "processed_count": len(results),
            "results": results,
            "archive_summary": self._repository.get_archive_summary(),
        }

    def resume_backfill_run(self, backfill_run_id: str) -> dict[str, Any]:
        record = self._repository.get_backfill_run(backfill_run_id)
        if not record:
            raise ValueError(f"backfill run not found: {backfill_run_id}")
        if record.get("status") not in {"failed", "partial"}:
            raise ValueError("only failed or partial backfill runs can be resumed")
        payload = dict(record.get("request_payload") or {})
        options = QEArchiveBackfillRunOptions(**payload, confirm_backfill=BACKFILL_CONFIRM_TEXT)
        return self.execute_backfill(options)

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
        limit: int = 20,
        page: int = 1,
        page_size: int | None = None,
        include_archived: bool = False,
    ) -> dict[str, Any]:
        effective_page_size = max(1, min(int(page_size or limit or 20), 500))
        effective_page = max(1, int(page or 1))
        offset = (effective_page - 1) * effective_page_size
        candidates = self._assembler.list_backfill_candidates(
            status=status,
            limit=effective_page_size + 1,
            offset=offset,
            include_archived=include_archived,
        )
        has_more = len(candidates) > effective_page_size
        page_candidates = candidates[:effective_page_size]
        return {
            "status": status,
            "include_archived": include_archived,
            "page": effective_page,
            "page_size": effective_page_size,
            "offset": offset,
            "count": len(page_candidates),
            "has_more": has_more,
            "candidates": page_candidates,
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
            for ref in self._assembler.list_loop_refs_for_tasks(
                task_ids,
                status=options.status,
                include_archived=options.include_archived,
            ):
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
            for experiment_id in self._assembler.list_experiment_ids(
                status=options.status,
                limit=limit,
                include_archived=options.include_archived,
            ):
                payload = self._assembler.assemble_experiment_payload(experiment_id)
                candidates.append({"event_type": "qe.experiment.completed", "payload": payload})
        if source in {"loop", "all"}:
            for ref in self._assembler.list_loop_refs(
                status=options.status,
                limit=limit,
                include_archived=options.include_archived,
            ):
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
                    include_archived=options.include_archived,
                )
                if row.get("candidate_type") == "evolution_task" and row.get("task_id")
            ]
            for ref in self._assembler.list_loop_refs_for_tasks(
                candidate_task_ids,
                status=options.status,
                include_archived=options.include_archived,
            ):
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
        backfill_run_id: str | None = None,
    ) -> dict[str, Any]:
        payload = dict(candidate["payload"])
        event_type = str(candidate["event_type"])
        source_type = "loop" if event_type == "qe.loop.completed" else "experiment"
        source_id = str(payload.get("source_id") or payload.get("task_id") or payload.get("experiment_id"))
        source_sub_id = payload.get("source_sub_id") or payload.get("loop_id")
        decision = resolve_archive_policy(
            source_system=str(payload.get("source_system") or "qe"),
            source_type=source_type,
            source_id=source_id,
            source_sub_id=str(source_sub_id) if source_sub_id else None,
            payload=payload,
            runtime_config=payload.get("config") if isinstance(payload.get("config"), Mapping) else {},
        )
        if write and not decision.should_archive:
            skip_id = record_policy_skip(
                decision,
                event_type=event_type,
                trigger_reason="backfill",
                repository=self._repository,
            )
            record_ingest_history(
                source_system=decision.source_system,
                source_type=source_type,
                source_id=source_id,
                source_sub_id=str(source_sub_id) if source_sub_id else None,
                trigger_reason="backfill",
                archive_policy=decision.archive_policy,
                ingest_status="manual_only" if decision.is_manual_only else "skipped",
                payload_sha256=decision.payload_sha256,
                runtime_config_sha256=decision.runtime_config_sha256,
                backfill_run_id=backfill_run_id,
                stats={"archive_policy_source": decision.archive_policy_source, "reason": decision.reason},
                repository=self._repository,
                created_by="qe_archive_backfill",
            )
            if backfill_run_id:
                self._repository.upsert_backfill_run_item(
                    BackfillRunItemRecord(
                        backfill_run_id=backfill_run_id,
                        source_system=decision.source_system,
                        source_type=source_type,
                        source_id=source_id,
                        source_sub_id=str(source_sub_id) if source_sub_id else None,
                        archive_policy=decision.archive_policy,
                        status="skipped",
                        skip_id=skip_id,
                        stats={"reason": decision.reason},
                    )
                )
            return {
                "dry_run": False,
                "event_type": event_type,
                "source_system": decision.source_system,
                "source_id": source_id,
                "source_sub_id": source_sub_id,
                "archive_policy": decision.archive_policy,
                "skipped_reason": decision.reason,
                "skip_id": skip_id,
            }
        result = self._archive_service.process_payload(
            payload,
            event_type=event_type,
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
            "archive_policy": decision.archive_policy,
            "stats": result.stats,
        }
        if write and options.validate_after_write:
            item["quality"] = self._validate_run(result.run_id, options)
        if write:
            record_ingest_history(
                source_system=decision.source_system,
                source_type=source_type,
                source_id=source_id,
                source_sub_id=str(source_sub_id) if source_sub_id else None,
                trigger_reason="backfill",
                archive_policy=decision.archive_policy,
                ingest_status="completed",
                run_id=result.run_id,
                backfill_run_id=backfill_run_id,
                payload_sha256=decision.payload_sha256,
                runtime_config_sha256=decision.runtime_config_sha256,
                result_fingerprint=result.run_id,
                stats=result.stats,
                repository=self._repository,
                created_by="qe_archive_backfill",
            )
            if backfill_run_id:
                self._repository.upsert_backfill_run_item(
                    BackfillRunItemRecord(
                        backfill_run_id=backfill_run_id,
                        source_system=decision.source_system,
                        source_type=source_type,
                        source_id=source_id,
                        source_sub_id=str(source_sub_id) if source_sub_id else None,
                        archive_policy=decision.archive_policy,
                        status="ingested",
                        run_id=result.run_id,
                        stats=result.stats,
                    )
                )
        return item

    def _preview_candidate(self, candidate: Mapping[str, Any], *, backfill_run_id: str) -> dict[str, Any]:
        payload = dict(candidate["payload"])
        event_type = str(candidate["event_type"])
        source_type = "loop" if event_type == "qe.loop.completed" else "experiment"
        source_id = str(payload.get("source_id") or payload.get("task_id") or payload.get("experiment_id"))
        source_sub_id = payload.get("source_sub_id") or payload.get("loop_id")
        decision = resolve_archive_policy(
            source_system=str(payload.get("source_system") or "qe"),
            source_type=source_type,
            source_id=source_id,
            source_sub_id=str(source_sub_id) if source_sub_id else None,
            payload=payload,
            runtime_config=payload.get("config") if isinstance(payload.get("config"), Mapping) else {},
        )
        self._repository.upsert_backfill_run_item(
            BackfillRunItemRecord(
                backfill_run_id=backfill_run_id,
                source_system=decision.source_system,
                source_type=source_type,
                source_id=source_id,
                source_sub_id=str(source_sub_id) if source_sub_id else None,
                archive_policy=decision.archive_policy,
                status="candidate",
                stats={"archive_policy_source": decision.archive_policy_source, "reason": decision.reason},
            )
        )
        return {
            "event_type": event_type,
            "source_system": decision.source_system,
            "source_id": source_id,
            "source_sub_id": source_sub_id,
            "archive_policy": decision.archive_policy,
            "archive_policy_source": decision.archive_policy_source,
            "reason": decision.reason,
            "will_archive": decision.should_archive,
            "dry_run": True,
        }

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


def _source_type_for_mode(source_mode: str) -> str:
    if source_mode == "completed_single_experiments":
        return "experiment"
    if source_mode == "completed_custom_evo_loops":
        return "loop"
    if source_mode == "all_completed_qe_sources":
        return "all"
    return "specific"


def _options_to_dict(options: QEArchiveBackfillRunOptions) -> dict[str, Any]:
    return {
        "source_mode": options.source_mode,
        "experiment_ids": list(options.experiment_ids or []),
        "task_ids": list(options.task_ids or []),
        "loop_ids": list(options.loop_ids or []),
        "task_id": options.task_id,
        "loop_index": options.loop_index,
        "status": options.status,
        "limit": options.limit,
        "include_archived": options.include_archived,
        "validate_after_write": options.validate_after_write,
        "min_metrics": options.min_metrics,
        "min_curves": options.min_curves,
        "min_factors": options.min_factors,
        "require_account_summary": options.require_account_summary,
        "force_rebackfill": options.force_rebackfill,
        "requested_by": options.requested_by,
    }
