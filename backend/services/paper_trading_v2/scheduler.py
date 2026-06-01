"""Background scheduler for durable Paper Trading v2 sessions.

The scheduler only calls the same fail-fast session tick API used by the UI.
It does not change data sources, algorithms, or rerun policies, and it never
marks a failed tick as success.
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import UTC, datetime
from typing import Any

from backend.services.trading_core.errors import TradingCoreError

from .auto_run import AutoRunCoordinator
from .repository import PaperTradingV2Repository
from .session import TICKABLE_SESSION_STATUSES, PaperTradingSessionRunner

logger = logging.getLogger("aistock.paper_trading_v2.scheduler")


class PaperTradingV2SessionScheduler:
    """Poll active Paper v2 sessions and process bounded ticks."""

    def __init__(
        self,
        *,
        repository: PaperTradingV2Repository | Any | None = None,
        runner: PaperTradingSessionRunner | None = None,
        auto_run_coordinator: AutoRunCoordinator | Any | None = None,
    ) -> None:
        self.repository = repository or PaperTradingV2Repository()
        self.runner = runner or PaperTradingSessionRunner(repository=self.repository)
        self.auto_run_coordinator = auto_run_coordinator or AutoRunCoordinator(repository=self.repository)
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._interval_seconds = self._default_interval()
        self._last_run_at: datetime | None = None
        self._last_result: dict[str, Any] | None = None

    def start(self, *, interval_seconds: int | None = None) -> dict[str, Any]:
        interval = int(interval_seconds or self._interval_seconds)
        if interval <= 0:
            raise ValueError("paper v2 scheduler interval_seconds must be positive")
        with self._lock:
            self._interval_seconds = interval
            if self._thread and self._thread.is_alive():
                return self.status()
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="paper-v2-session-scheduler",
                daemon=True,
            )
            self._thread.start()
            logger.info("Paper Trading v2 session scheduler started interval=%ss", interval)
            return self.status()

    def shutdown(self, wait: bool = False) -> dict[str, Any]:
        with self._lock:
            self._stop_event.set()
            thread = self._thread
        if wait and thread and thread.is_alive():
            thread.join(timeout=5.0)
        logger.info("Paper Trading v2 session scheduler stopped")
        return self.status()

    def status(self) -> dict[str, Any]:
        thread = self._thread
        return {
            "running": bool(thread and thread.is_alive() and not self._stop_event.is_set()),
            "thread_alive": bool(thread and thread.is_alive()),
            "interval_seconds": self._interval_seconds,
            "tickable_statuses": sorted(item.value for item in TICKABLE_SESSION_STATUSES),
            "last_run_at": self._last_run_at.isoformat() if self._last_run_at else None,
            "last_result": self._last_result,
            "auto_run": self.auto_run_coordinator.status(),
        }

    def bootstrap_status(self) -> dict[str, Any]:
        env_scheduler = (os.getenv("ENABLE_PAPER_TRADING_V2_SCHEDULER") or "").strip().lower()
        scheduler_autostart = env_scheduler in {"1", "true", "yes", "on"}
        return {
            "scheduler_autostart_env": bool(scheduler_autostart),
            "scheduler_env_raw": env_scheduler or None,
            "scheduler": self.status(),
            "auto_run": self.auto_run_coordinator.status(),
            "production_note": "backend process restart only auto-runs when ENABLE_PAPER_TRADING_V2_SCHEDULER is true-like",
        }

    def run_once(self, *, limit: int = 50, as_of_time: datetime | None = None) -> dict[str, Any]:
        if limit <= 0 or limit > 500:
            raise ValueError("paper v2 scheduler run_once limit must be in 1..500")
        started = datetime.now(UTC)
        self._last_run_at = started
        self._last_result = {
            "started_at": started.isoformat(),
            "in_progress": True,
            "completed_at": None,
            "session_count": None,
            "processed": [],
            "errors": [],
        }
        auto_run_recovery = self.auto_run_coordinator.recover_enabled_portfolios(
            limit=limit,
            as_of_time=as_of_time,
        )
        sessions = self.repository.list_tickable_sessions(statuses=TICKABLE_SESSION_STATUSES, limit=limit)
        result: dict[str, Any] = {
            "started_at": started.isoformat(),
            "in_progress": True,
            "completed_at": None,
            "auto_run_recovery": auto_run_recovery,
            "session_count": len(sessions),
            "processed": [],
            "errors": [],
        }
        self._last_result = result
        for session in sessions:
            try:
                progress = self.runner.tick(session.session_id, as_of_time=as_of_time)
                result["processed"].append(
                    {
                        "session_id": session.session_id,
                        "portfolio_id": session.portfolio_id,
                        "status": progress.session.status.value,
                        "phase": progress.session.phase.value,
                        "current_trade_date": progress.current_trade_date.isoformat()
                        if progress.current_trade_date
                        else None,
                    }
                )
                self._last_result = result
            except TradingCoreError as exc:
                payload = exc.to_dict()
                payload["context"] = {
                    **payload.get("context", {}),
                    "session_id": session.session_id,
                    "portfolio_id": session.portfolio_id,
                }
                result["errors"].append(payload)
                logger.warning("Paper v2 scheduler tick failed: %s", payload)
                self._last_result = result
            except Exception as exc:  # pragma: no cover - defensive guard
                payload = {
                    "error_code": "TRADING_CORE_ERROR",
                    "message": "paper v2 scheduler tick crashed",
                    "context": {
                        "session_id": session.session_id,
                        "portfolio_id": session.portfolio_id,
                        "reason": f"{type(exc).__name__}: {exc}",
                    },
                }
                result["errors"].append(payload)
                logger.exception("Paper v2 scheduler tick crashed for session=%s", session.session_id)
                self._last_result = result
        result["completed_at"] = datetime.now(UTC).isoformat()
        result["in_progress"] = False
        self._last_run_at = started
        self._last_result = result
        return result

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                logger.exception("Paper v2 scheduler run_once crashed")
            if self._stop_event.wait(timeout=self._interval_seconds):
                break

    @staticmethod
    def _default_interval() -> int:
        raw = (os.getenv("PAPER_TRADING_V2_SCHEDULER_INTERVAL_SEC") or "30").strip()
        try:
            value = int(raw)
        except ValueError:
            return 30
        return value if value > 0 else 30


paper_trading_v2_scheduler = PaperTradingV2SessionScheduler()
