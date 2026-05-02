"""Best-effort realtime QE archive ingestion hooks.

The hook is disabled by default. When explicitly enabled, QE completion paths
can call it after their own DB transaction succeeds; archive failures are
reported in logs/API results and must not change QE loop or experiment status.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from .backfill_service import QEArchiveBackfillService
from .event_capture import QEArchiveEventCapture


QE_ARCHIVE_REALTIME_ENABLED_ENV = "QE_ARCHIVE_REALTIME_ENABLED"
QE_ARCHIVE_REALTIME_MODE_ENV = "QE_ARCHIVE_REALTIME_MODE"
REALTIME_MODE_OUTBOX = "outbox"
REALTIME_MODE_DIRECT = "direct"
logger = logging.getLogger("aistock.qe_archive.realtime_ingestion")


def _env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


class QEArchiveRealtimeIngestion:
    """Small facade for completion-time archive ingestion.

    The safe default for enabled runtime hooks is durable outbox capture. Direct
    archive writes remain available only for explicit rollback/diagnostic use.
    """

    def __init__(
        self,
        *,
        service: QEArchiveBackfillService | None = None,
        event_capture: QEArchiveEventCapture | None = None,
        enabled: bool | None = None,
        mode: str | None = None,
    ) -> None:
        self._service = service or QEArchiveBackfillService()
        self._event_capture = event_capture or QEArchiveEventCapture(enabled=True)
        self._enabled = enabled
        self._mode = mode

    @property
    def enabled(self) -> bool:
        if self._enabled is not None:
            return self._enabled
        return _env_truthy(os.getenv(QE_ARCHIVE_REALTIME_ENABLED_ENV))

    @property
    def mode(self) -> str:
        value = self._mode if self._mode is not None else os.getenv(QE_ARCHIVE_REALTIME_MODE_ENV)
        normalized = (value or REALTIME_MODE_OUTBOX).strip().lower()
        if normalized in {REALTIME_MODE_OUTBOX, REALTIME_MODE_DIRECT}:
            return normalized
        return REALTIME_MODE_OUTBOX

    def archive_loop_completed(
        self,
        *,
        task_id: str,
        loop_id: str,
        loop_index: int | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"archived": False, "skipped_reason": "disabled"}
        if self.mode == REALTIME_MODE_OUTBOX:
            result = self._event_capture.enqueue_loop_completed_result(
                task_id=task_id,
                loop_id=loop_id,
                loop_index=loop_index,
                payload={"capture_reason": "qe_loop_completed_hook"},
            )
            return {"archived": False, "queued": bool(result.get("inserted")), "mode": self.mode, **result}
        return self._service.archive_loop_completed(
            task_id=task_id,
            loop_id=loop_id,
            loop_index=loop_index,
        )

    def archive_experiment_completed(self, *, experiment_id: str) -> dict[str, Any]:
        if not self.enabled:
            return {"archived": False, "skipped_reason": "disabled"}
        if self.mode == REALTIME_MODE_OUTBOX:
            result = self._event_capture.enqueue_experiment_completed_result(
                experiment_id=experiment_id,
                payload={"capture_reason": "qe_experiment_completed_hook"},
            )
            return {"archived": False, "queued": bool(result.get("inserted")), "mode": self.mode, **result}
        return self._service.archive_experiment_completed(experiment_id=experiment_id)


def safe_archive_loop_completed(
    *,
    task_id: str,
    loop_id: str,
    loop_index: int | None = None,
) -> dict[str, Any]:
    """Archive one completed loop without raising into the QE runtime path."""

    try:
        result = QEArchiveRealtimeIngestion().archive_loop_completed(
            task_id=task_id,
            loop_id=loop_id,
            loop_index=loop_index,
        )
        if result.get("skipped_reason") != "disabled":
            logger.info("QE archive realtime loop ingestion result: %s", result)
        return result
    except Exception as exc:  # pragma: no cover - runtime protection path.
        logger.warning(
            "QE archive realtime loop ingestion failed: task=%s loop=%s error=%s",
            task_id,
            loop_id,
            exc,
            exc_info=True,
        )
        return {"archived": False, "error": f"{type(exc).__name__}: {exc}"}


def safe_archive_experiment_completed(*, experiment_id: str) -> dict[str, Any]:
    """Archive one completed experiment without raising into the QE runtime path."""

    try:
        result = QEArchiveRealtimeIngestion().archive_experiment_completed(experiment_id=experiment_id)
        if result.get("skipped_reason") != "disabled":
            logger.info("QE archive realtime experiment ingestion result: %s", result)
        return result
    except Exception as exc:  # pragma: no cover - runtime protection path.
        logger.warning(
            "QE archive realtime experiment ingestion failed: experiment=%s error=%s",
            experiment_id,
            exc,
            exc_info=True,
        )
        return {"archived": False, "error": f"{type(exc).__name__}: {exc}"}
