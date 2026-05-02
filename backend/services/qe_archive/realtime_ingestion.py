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


QE_ARCHIVE_REALTIME_ENABLED_ENV = "QE_ARCHIVE_REALTIME_ENABLED"
logger = logging.getLogger("aistock.qe_archive.realtime_ingestion")


def _env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


class QEArchiveRealtimeIngestion:
    """Small facade for completion-time archive writes."""

    def __init__(
        self,
        *,
        service: QEArchiveBackfillService | None = None,
        enabled: bool | None = None,
    ) -> None:
        self._service = service or QEArchiveBackfillService()
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        if self._enabled is not None:
            return self._enabled
        return _env_truthy(os.getenv(QE_ARCHIVE_REALTIME_ENABLED_ENV))

    def archive_loop_completed(
        self,
        *,
        task_id: str,
        loop_id: str,
        loop_index: int | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"archived": False, "skipped_reason": "disabled"}
        return self._service.archive_loop_completed(
            task_id=task_id,
            loop_id=loop_id,
            loop_index=loop_index,
        )

    def archive_experiment_completed(self, *, experiment_id: str) -> dict[str, Any]:
        if not self.enabled:
            return {"archived": False, "skipped_reason": "disabled"}
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
