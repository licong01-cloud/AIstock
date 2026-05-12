"""API-facing QE archive outbox worker helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from .backfill_service import QEArchiveBackfillService
from .listeners.qe_experiment_completed_listener import (
    QEExperimentCompletedModelSyncListener,
    qe_experiment_completed_model_sync_handler,
)
from .models import ClaimedOutboxEvent
from .repository import QEArchiveRepository
from .worker import ArchiveWorkerEventResult, ArchiveWorkerRunResult, QEArchiveWorker


WORKER_CONFIRM_TEXT = "QE_ARCHIVE_WORKER_RUN"
SUPPORTED_WORKER_EVENT_TYPES = ("qe.loop.completed", "qe.experiment.completed")


class QEArchiveWorkerService:
    """Build a one-shot outbox worker with concrete QE archive handlers."""

    def __init__(
        self,
        *,
        repository: QEArchiveRepository | None = None,
        backfill_service: QEArchiveBackfillService | None = None,
        model_sync_listener: QEExperimentCompletedModelSyncListener | None = None,
        enabled: bool | None = None,
        worker_id: str = "qe_archive_api_worker",
    ) -> None:
        self._repository = repository or QEArchiveRepository()
        self._backfill_service = backfill_service or QEArchiveBackfillService(repository=self._repository)
        self._model_sync_listener = model_sync_listener or qe_experiment_completed_model_sync_handler()
        self._enabled = enabled
        self._worker_id = worker_id

    def run_once(self, *, limit: int = 10) -> dict[str, Any]:
        worker = QEArchiveWorker(
            repository=self._repository,
            worker_id=self._worker_id,
            enabled=self._enabled,
            handlers={
                "qe.loop.completed": self._handle_loop_completed,
                "qe.experiment.completed": self._handle_experiment_completed,
            },
        )
        return _worker_result_to_dict(worker.run_once(limit=limit))

    def _handle_loop_completed(self, event: ClaimedOutboxEvent) -> ArchiveWorkerEventResult:
        payload = _payload_dict(event.payload)
        loop_id = _non_empty(payload.get("loop_id")) or event.source_sub_id
        task_id = _non_empty(payload.get("task_id")) or event.source_id
        if not loop_id:
            return ArchiveWorkerEventResult(success=False, error="qe.loop.completed event is missing loop_id")

        report = self._backfill_service.archive_loop_completed(
            task_id=task_id,
            loop_id=loop_id,
            loop_index=_optional_int(payload.get("loop_index")),
        )
        return _archive_report_to_worker_result(report)

    def _handle_experiment_completed(self, event: ClaimedOutboxEvent) -> ArchiveWorkerEventResult:
        if self._model_sync_listener.can_handle(event):
            return self._model_sync_listener.handle(event)

        payload = _payload_dict(event.payload)
        experiment_id = _non_empty(payload.get("experiment_id")) or event.source_id
        if not experiment_id:
            return ArchiveWorkerEventResult(success=False, error="qe.experiment.completed event is missing experiment_id")

        report = self._backfill_service.archive_experiment_completed(experiment_id=experiment_id)
        return _archive_report_to_worker_result(report)


def _archive_report_to_worker_result(report: Mapping[str, Any]) -> ArchiveWorkerEventResult:
    results = report.get("results")
    if not isinstance(results, list) or not results:
        return ArchiveWorkerEventResult(
            success=False,
            error="archive backfill report did not include any processed result",
            stats={"archive_report": dict(report)},
        )
    first = results[0] if isinstance(results[0], Mapping) else {}
    quality = first.get("quality") if isinstance(first, Mapping) else None
    if isinstance(quality, Mapping) and quality.get("passed") is False:
        return ArchiveWorkerEventResult(
            success=False,
            run_id=_non_empty(first.get("run_id")),
            error=f"archive quality validation failed: {quality.get('failures')}",
            stats={"archive_report": dict(report)},
        )
    return ArchiveWorkerEventResult(
        success=True,
        run_id=_non_empty(first.get("run_id")),
        stats={"archive_report": dict(report)},
    )


def _worker_result_to_dict(result: ArchiveWorkerRunResult) -> dict[str, Any]:
    return asdict(result)


def _payload_dict(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(payload or {})


def _non_empty(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
