"""Disabled-by-default QE archive outbox worker state machine."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .models import ArchiveJobRecord, ClaimedOutboxEvent
from .repository import QEArchiveRepository


QE_ARCHIVE_WORKER_ENABLED_ENV = "QE_ARCHIVE_WORKER_ENABLED"


def _env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class ArchiveWorkerEventResult:
    success: bool
    run_id: str | None = None
    stats: Mapping[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass(frozen=True)
class ArchiveWorkerRunResult:
    claimed: int = 0
    completed: int = 0
    failed: int = 0
    skipped_reason: str | None = None


ArchiveEventHandler = Callable[[ClaimedOutboxEvent], ArchiveWorkerEventResult]


class QEArchiveWorker:
    """Process archive outbox events only when explicitly enabled.

    This class is not registered with FastAPI startup or any scheduler. It is a
    reusable state machine for later CLI/API-controlled archive workers.
    """

    def __init__(
        self,
        repository: QEArchiveRepository | None = None,
        *,
        worker_id: str = "qe_archive_worker_local",
        enabled: bool | None = None,
        handlers: Mapping[str, ArchiveEventHandler] | None = None,
        max_retries: int = 5,
        retry_after_seconds: int = 60,
    ) -> None:
        self._repository = repository or QEArchiveRepository()
        self._worker_id = worker_id
        self._enabled = enabled
        self._handlers = dict(handlers or {})
        self._max_retries = max_retries
        self._retry_after_seconds = retry_after_seconds

    @property
    def enabled(self) -> bool:
        if self._enabled is not None:
            return self._enabled
        return _env_truthy(os.getenv(QE_ARCHIVE_WORKER_ENABLED_ENV))

    def run_once(self, *, limit: int = 10) -> ArchiveWorkerRunResult:
        if not self.enabled:
            return ArchiveWorkerRunResult(skipped_reason="disabled")
        if not self._handlers:
            return ArchiveWorkerRunResult(skipped_reason="no_handlers")

        events = self._repository.claim_outbox_events(
            worker_id=self._worker_id,
            limit=limit,
            event_types=tuple(self._handlers.keys()),
        )
        completed = 0
        failed = 0
        for event in events:
            if self._process_event(event):
                completed += 1
            else:
                failed += 1
        return ArchiveWorkerRunResult(
            claimed=len(events),
            completed=completed,
            failed=failed,
        )

    def _process_event(self, event: ClaimedOutboxEvent) -> bool:
        handler = self._handlers.get(event.event_type)
        if handler is None:
            return False

        job_id = self._repository.create_archive_job(
            ArchiveJobRecord(
                event_id=event.event_id,
                job_type=event.event_type,
                level="A",
                stats={"source_id": event.source_id, "source_sub_id": event.source_sub_id},
            )
        )
        try:
            result = handler(event)
            if result.success:
                self._repository.complete_archive_job(
                    job_id,
                    run_id=result.run_id,
                    stats=result.stats,
                )
                self._repository.complete_outbox_event(event.event_id)
                return True
            error = result.error or "archive event handler returned unsuccessful result"
        except Exception as exc:  # pragma: no cover - exercised through tests with concrete exception.
            error = f"{type(exc).__name__}: {exc}"

        self._repository.fail_archive_job(job_id, error, stats={"event_id": event.event_id})
        self._repository.fail_outbox_event(
            event.event_id,
            error,
            retry_after_seconds=self._retry_after_seconds,
            max_retries=self._max_retries,
        )
        return False
