from __future__ import annotations

import logging
import os
import threading
from datetime import UTC, datetime
from typing import Any, Callable

from backend.services.advisory_forward.service import AdvisoryForwardService


LOGGER = logging.getLogger("aistock.advisory.forward.scheduler")
DEFAULT_INTERVAL_SECONDS = 300


class AdvisoryForwardScheduler:
    def __init__(
        self,
        *,
        service: AdvisoryForwardService | Any | None = None,
        service_factory: Callable[[], AdvisoryForwardService | Any] | None = None,
    ) -> None:
        self._service = service
        self._service_factory = service_factory or AdvisoryForwardService
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._run_lock = threading.Lock()
        self._interval_seconds = DEFAULT_INTERVAL_SECONDS
        self._last_run_at: datetime | None = None
        self._last_result: dict[str, Any] | None = None
        self._last_error: dict[str, Any] | None = None

    def start(self, *, interval_seconds: int | None = None) -> dict[str, Any]:
        interval = _interval_seconds() if interval_seconds is None else int(interval_seconds)
        if interval <= 0:
            raise ValueError("Advisory forward scheduler interval_seconds must be positive")
        with self._lock:
            self._interval_seconds = interval
            if self._thread and self._thread.is_alive():
                return self.status()
            self._ensure_service()
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="advisory-forward-scheduler",
                daemon=True,
            )
            self._thread.start()
        LOGGER.info("Advisory forward scheduler started interval=%ss", interval)
        return self.status()

    def shutdown(self, *, wait: bool = False) -> dict[str, Any]:
        with self._lock:
            self._stop_event.set()
            thread = self._thread
        if wait and thread and thread.is_alive():
            thread.join(timeout=5.0)
        LOGGER.info("Advisory forward scheduler stopped")
        return self.status()

    def run_once(self) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            busy = {
                "schema_version": "advisory_forward_run_once_v1",
                "publication_due": False,
                "status": "ALREADY_RUNNING",
                "reason_code": "ADVISORY_FORWARD_RUN_ALREADY_IN_PROGRESS",
                "results": [],
            }
            self._last_result = busy
            return busy
        started = datetime.now(UTC)
        self._last_run_at = started
        try:
            try:
                result = self._ensure_service().run_once()
            except Exception as exc:
                self._last_error = {
                    "at": started.isoformat(),
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                }
                LOGGER.exception("Advisory forward scheduler run_once failed")
                raise
            self._last_result = result
            self._last_error = None
            return result
        finally:
            self._run_lock.release()

    def _ensure_service(self) -> AdvisoryForwardService | Any:
        with self._lock:
            if self._service is None:
                self._service = self._service_factory()
            return self._service

    def status(self) -> dict[str, Any]:
        thread = self._thread
        env_raw = (os.getenv("AISTOCK_ADVISORY_FORWARD_SCHEDULER_ENABLED") or "").strip().lower()
        return {
            "schema_version": "advisory_forward_scheduler_status_v1",
            "configured_enabled": env_raw in {"1", "true", "yes", "y", "on"},
            "running": bool(thread and thread.is_alive() and not self._stop_event.is_set()),
            "thread_alive": bool(thread and thread.is_alive()),
            "interval_seconds": self._interval_seconds,
            "last_run_at": self._last_run_at.isoformat() if self._last_run_at else None,
            "last_result": self._last_result,
            "last_error": self._last_error,
        }

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                LOGGER.error(
                    "Advisory forward scheduler loop retained the failure and will retry after interval: %s",
                    self._last_error,
                )
            self._stop_event.wait(self._interval_seconds)


def _interval_seconds() -> int:
    raw = (os.getenv("AISTOCK_ADVISORY_FORWARD_POLL_SECONDS") or "300").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("AISTOCK_ADVISORY_FORWARD_POLL_SECONDS must be an integer") from exc
    if value <= 0:
        raise ValueError("AISTOCK_ADVISORY_FORWARD_POLL_SECONDS must be positive")
    return value


advisory_forward_scheduler = AdvisoryForwardScheduler()
