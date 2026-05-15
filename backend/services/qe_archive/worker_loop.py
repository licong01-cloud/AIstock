"""Opt-in FastAPI lifespan worker loop for QE archive outbox consumption."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from .worker_service import QEArchiveWorkerService

QE_ARCHIVE_WORKER_AUTOSTART_ENV = "QE_ARCHIVE_WORKER_AUTOSTART"
QE_ARCHIVE_WORKER_INTERVAL_SECONDS_ENV = "QE_ARCHIVE_WORKER_INTERVAL_SECONDS"
QE_ARCHIVE_WORKER_BATCH_SIZE_ENV = "QE_ARCHIVE_WORKER_BATCH_SIZE"
QE_ARCHIVE_WORKER_ID_ENV = "QE_ARCHIVE_WORKER_ID"
logger = logging.getLogger("aistock.qe_archive.worker_loop")


def env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def autostart_enabled() -> bool:
    return env_truthy(os.getenv(QE_ARCHIVE_WORKER_AUTOSTART_ENV)) and env_truthy(os.getenv("QE_ARCHIVE_WORKER_ENABLED"))


async def run_archive_worker_loop(stop_event: asyncio.Event) -> None:
    interval = max(1, int((os.getenv(QE_ARCHIVE_WORKER_INTERVAL_SECONDS_ENV) or "60").strip() or "60"))
    batch_size = max(1, min(int((os.getenv(QE_ARCHIVE_WORKER_BATCH_SIZE_ENV) or "10").strip() or "10"), 100))
    worker_id = (os.getenv(QE_ARCHIVE_WORKER_ID_ENV) or "qe_archive_lifespan_worker").strip()
    service = QEArchiveWorkerService(worker_id=worker_id, enabled=True)
    logger.info("QE archive worker loop started worker_id=%s interval=%s batch_size=%s", worker_id, interval, batch_size)
    while not stop_event.is_set():
        try:
            report: dict[str, Any] = service.run_once(limit=batch_size)
            if report.get("claimed") or report.get("failed"):
                logger.info("QE archive worker loop report: %s", report)
        except Exception as exc:
            logger.warning("QE archive worker loop iteration failed: %s", exc, exc_info=True)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue
