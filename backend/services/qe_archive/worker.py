"""Disabled-by-default QE archive outbox worker state machine."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .models import ArchiveJobRecord, ClaimedOutboxEvent
from .repository import (
    MULTI_ALPHA_OUTBOX_SKIP_SOURCE_SYSTEMS,
    PAPER_DAEMON_TELEMETRY_NOT_ARCHIVED,
    PAPER_V2_ARCHIVE_DEFERRED_THROWAWAY,
    PAPER_V2_DEFERRED_ARCHIVE_EVENT_TYPES,
    PAPER_V2_OUTBOX_SKIP_SOURCE_SYSTEMS,
    UNSUPPORTED_OUTBOX_EVENT_TYPE,
    QEArchiveRepository,
)


QE_ARCHIVE_WORKER_ENABLED_ENV = "QE_ARCHIVE_WORKER_ENABLED"


def _env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class ArchiveWorkerEventResult:
    success: bool
    run_id: str | None = None
    stats: Mapping[str, Any] = field(default_factory=dict)
    error: str | None = None
    skipped_reason: str | None = None


@dataclass(frozen=True)
class ArchiveWorkerRunResult:
    claimed: int = 0
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    skipped_reason: str | None = None


ArchiveEventHandler = Callable[[ClaimedOutboxEvent], ArchiveWorkerEventResult]


def archive_handler_adapter(handler: Any) -> ArchiveEventHandler:
    """P1.1 (Codex round 2): adapt a contract.ArchiveHandler instance to the
    worker's ArchiveEventHandler protocol.

    Bridges:
      - signature: handler.handle(event, archive_job) -> ArchiveResult
        worker callable: (event) -> ArchiveWorkerEventResult
      - exceptions: handler raises on unrecoverable failure (P1.2);
        adapter catches and reports success=False with error string so the
        worker fails the job per its existing retry/dead-letter policy.

    Stub ArchiveJobRecord is constructed so handler.handle's signature is
    satisfied; the real archive_job record is owned by QEArchiveWorker._process_event
    and is not passed through the adapter (worker creates it before dispatching).
    """
    # Late import to avoid circular dependency between worker.py and handlers/.
    from .handlers.contract import ArchiveResult, HandlerStatus

    def _adapted(event: ClaimedOutboxEvent) -> ArchiveWorkerEventResult:
        run_id = (event.payload or {}).get("run_id") or event.source_sub_id or event.source_id
        stub_job = ArchiveJobRecord(
            event_id=event.event_id,
            job_type=event.event_type,
            level="A",
            stats={"adapter_stub": True},
        )
        try:
            result: ArchiveResult = handler.handle(event, stub_job)
        except Exception as exc:
            return ArchiveWorkerEventResult(
                success=False,
                run_id=run_id,
                stats={},
                error=f"{type(exc).__name__}: {exc}",
            )
        success = result.status == HandlerStatus.SUCCESS or result.status == HandlerStatus.NOOP
        # Treat NOOP as success-with-zero-rows: the event is correctly handled
        # (deferred / replay-skipped) and should not retry-storm the worker.
        return ArchiveWorkerEventResult(
            success=success,
            run_id=run_id,
            stats={
                "rows_inserted": result.rows_inserted,
                "rows_upserted": result.rows_upserted,
                "handler_status": result.status.value,
                **dict(result.stats or {}),
            },
            error=result.error_message if not success else None,
            skipped_reason=(result.stats or {}).get("skipped_reason") if result.status == HandlerStatus.NOOP else None,
        )

    return _adapted


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
            routing_class="archive",
        )
        completed = 0
        failed = 0
        skipped = 0
        for event in events:
            outcome = self._process_event(event)
            if outcome == "completed":
                completed += 1
            elif outcome == "skipped":
                skipped += 1
            else:
                failed += 1

        policy_events = self._claim_policy_skip_events(limit=max(0, limit - len(events)))
        for event in policy_events:
            if self._skip_policy_event(event):
                skipped += 1
            else:
                failed += 1
        return ArchiveWorkerRunResult(
            claimed=len(events) + len(policy_events),
            completed=completed,
            failed=failed,
            skipped=skipped,
        )

    def _process_event(self, event: ClaimedOutboxEvent) -> str:
        handler = self._handlers.get(event.event_type)
        if handler is None:
            return "failed"

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
            if result.skipped_reason:
                self._repository.skip_outbox_event(
                    event,
                    reason_code=result.skipped_reason,
                    trigger_reason="realtime",
                )
                self._repository.complete_archive_job(
                    job_id,
                    run_id=None,
                    stats={
                        **dict(result.stats or {}),
                        "terminal_outbox_status": "skipped",
                    },
                )
                return "skipped"
            if result.success:
                self._repository.complete_archive_job(
                    job_id,
                    run_id=result.run_id,
                    stats=result.stats,
                )
                self._repository.complete_outbox_event(event.event_id)
                return "completed"
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
        return "failed"

    def _claim_policy_skip_events(self, *, limit: int) -> list[ClaimedOutboxEvent]:
        if limit <= 0 or not hasattr(self._repository, "skip_outbox_event"):
            return []
        # Claim policy-only rows explicitly so unsupported telemetry or
        # non-archiveable macb rows do not remain as silent pending black holes.
        return self._repository.claim_outbox_events(
            worker_id=f"{self._worker_id}:policy_skip",
            limit=limit,
            source_systems=PAPER_V2_OUTBOX_SKIP_SOURCE_SYSTEMS + MULTI_ALPHA_OUTBOX_SKIP_SOURCE_SYSTEMS,
            routing_class=None,
            allow_missing_routing_class=False,
        )

    def _skip_policy_event(self, event: ClaimedOutboxEvent) -> bool:
        reason_code = _paper_policy_skip_reason(event)
        self._repository.skip_outbox_event(
            event,
            reason_code=reason_code,
            trigger_reason="realtime",
        )
        return True


def _paper_policy_skip_reason(event: ClaimedOutboxEvent) -> str:
    """Return the explicit policy-skip reason for paper outbox rows."""

    routing_class = (event.payload or {}).get("routing_class")
    if event.event_type == "qe.multi_alpha.combine.completed":
        return UNSUPPORTED_OUTBOX_EVENT_TYPE
    if event.event_type.startswith("paper.daemon.") or (
        event.source_system == "paper_v2.daemon" and routing_class == "telemetry"
    ):
        return PAPER_DAEMON_TELEMETRY_NOT_ARCHIVED
    if event.event_type in PAPER_V2_DEFERRED_ARCHIVE_EVENT_TYPES:
        return PAPER_V2_ARCHIVE_DEFERRED_THROWAWAY
    return UNSUPPORTED_OUTBOX_EVENT_TYPE
