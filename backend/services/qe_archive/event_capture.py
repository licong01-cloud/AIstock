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
        return bool(
            self.enqueue_loop_completed_result(
                task_id=task_id,
                loop_id=loop_id,
                loop_index=loop_index,
                payload=payload,
            ).get("inserted")
        )

    def enqueue_loop_completed_result(
        self,
        *,
        task_id: str,
        loop_id: str,
        loop_index: int | None = None,
        payload: Mapping[str, Any] | None = None,
        archive_policy: str = "AUTO",
        archive_policy_source: str = "default",
        trigger_reason: str = "realtime",
        payload_sha256: str | None = None,
        runtime_config_sha256: str | None = None,
    ) -> dict[str, Any]:
        event_payload = dict(payload or {})
        event_payload.setdefault("task_id", task_id)
        event_payload.setdefault("loop_id", loop_id)
        event_payload.setdefault("archive_policy", archive_policy)
        event_payload.setdefault("archive_policy_source", archive_policy_source)
        event_payload.setdefault("trigger_reason", trigger_reason)
        if payload_sha256:
            event_payload.setdefault("payload_sha256", payload_sha256)
        if runtime_config_sha256:
            event_payload.setdefault("runtime_config_sha256", runtime_config_sha256)
        if loop_index is not None:
            event_payload.setdefault("loop_index", loop_index)
        return self._insert_event_result(
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
        return bool(
            self.enqueue_experiment_completed_result(
                experiment_id=experiment_id,
                payload=payload,
            ).get("inserted")
        )

    def enqueue_experiment_completed_result(
        self,
        *,
        experiment_id: str,
        payload: Mapping[str, Any] | None = None,
        archive_policy: str = "AUTO",
        archive_policy_source: str = "default",
        trigger_reason: str = "realtime",
        payload_sha256: str | None = None,
        runtime_config_sha256: str | None = None,
    ) -> dict[str, Any]:
        event_payload = dict(payload or {})
        event_payload.setdefault("experiment_id", experiment_id)
        event_payload.setdefault("archive_policy", archive_policy)
        event_payload.setdefault("archive_policy_source", archive_policy_source)
        event_payload.setdefault("trigger_reason", trigger_reason)
        if payload_sha256:
            event_payload.setdefault("payload_sha256", payload_sha256)
        if runtime_config_sha256:
            event_payload.setdefault("runtime_config_sha256", runtime_config_sha256)
        return self._insert_event_result(
            event_type="qe.experiment.completed",
            source_system="qe",
            source_id=experiment_id,
            source_sub_id=None,
            payload=event_payload,
        )

    def _insert_event_result(
        self,
        *,
        event_type: str,
        source_system: str,
        source_id: str,
        source_sub_id: str | None,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        event = OutboxEventRecord(
            event_type=event_type,
            source_system=source_system,
            source_id=source_id,
            source_sub_id=source_sub_id,
            payload=payload,
        )
        if not self.enabled:
            return {"inserted": False, "skipped_reason": "disabled", "event_id": event.event_id}
        inserted = self._repository.insert_outbox_event(event)
        return {
            "inserted": inserted,
            "event_id": event.event_id,
            "event_type": event.event_type,
            "source_system": event.source_system,
            "source_id": event.source_id,
            "source_sub_id": event.source_sub_id,
            "duplicate": not inserted,
        }
