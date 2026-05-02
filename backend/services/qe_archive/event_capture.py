"""Disabled-by-default QE archive event capture helpers.

This module is intentionally not wired into QE routers yet. It provides the
next ingestion step while keeping current QE production request paths unchanged.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from .models import OutboxEventRecord
from .repository import QEArchiveRepository


QE_ARCHIVE_EVENT_CAPTURE_ENV = "QE_ARCHIVE_EVENT_CAPTURE_ENABLED"


def _env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


class QEArchiveEventCapture:
    """Create qe_archive outbox events without touching existing QE flows."""

    def __init__(
        self,
        repository: QEArchiveRepository | None = None,
        *,
        enabled: bool | None = None,
    ) -> None:
        self._repository = repository or QEArchiveRepository()
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        if self._enabled is not None:
            return self._enabled
        return _env_truthy(os.getenv(QE_ARCHIVE_EVENT_CAPTURE_ENV))

    def enqueue_loop_completed(
        self,
        *,
        task_id: str,
        loop_id: str,
        loop_index: int | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> bool:
        event_payload = dict(payload or {})
        event_payload.setdefault("task_id", task_id)
        event_payload.setdefault("loop_id", loop_id)
        if loop_index is not None:
            event_payload.setdefault("loop_index", loop_index)
        return self._insert_event(
            event_type="qe.loop.completed",
            source_system="qe",
            source_id=task_id,
            source_sub_id=loop_id,
            payload=event_payload,
        )

    def enqueue_experiment_completed(
        self,
        *,
        experiment_id: str,
        payload: Mapping[str, Any] | None = None,
    ) -> bool:
        event_payload = dict(payload or {})
        event_payload.setdefault("experiment_id", experiment_id)
        return self._insert_event(
            event_type="qe.experiment.completed",
            source_system="qe",
            source_id=experiment_id,
            source_sub_id=None,
            payload=event_payload,
        )

    def _insert_event(
        self,
        *,
        event_type: str,
        source_system: str,
        source_id: str,
        source_sub_id: str | None,
        payload: Mapping[str, Any],
    ) -> bool:
        if not self.enabled:
            return False
        return self._repository.insert_outbox_event(
            OutboxEventRecord(
                event_type=event_type,
                source_system=source_system,
                source_id=source_id,
                source_sub_id=source_sub_id,
                payload=payload,
            )
        )
