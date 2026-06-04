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

from .auto_run import AutoRunCoordinator, MINIQMT_ACCOUNT_GROUP_BINDING_MODE, MINIQMT_LEGACY_BINDING_MODES
from .models import PaperSessionStatus, PortfolioStatus
from .repository import PaperTradingV2Repository
from .session import TICKABLE_SESSION_STATUSES, PaperTradingSessionRunner

logger = logging.getLogger("aistock.paper_trading_v2.scheduler")

SESSION_TICK_TIMEOUT_ERROR_CODE = "PAPER_V2_SESSION_TICK_TIMEOUT"
SESSION_TICK_TIMEOUT_EVENT = "SESSION_TICK_TIMEOUT_FAILED"
SESSION_TICK_TIMEOUT_POLICY = "mark_session_failed_release_scheduler_guard"


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
        self._active_session_ticks: dict[str, dict[str, Any]] = {}
        self._abandoned_session_ticks: dict[str, dict[str, Any]] = {}

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
            "active_session_ticks": self._active_session_tick_status(),
            "abandoned_session_ticks": self._abandoned_session_tick_status(),
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
            "miniqmt_account_group_slots": {
                "enabled": True,
                "account_binding_mode": MINIQMT_ACCOUNT_GROUP_BINDING_MODE,
                "legacy_modes_normalized": sorted(MINIQMT_LEGACY_BINDING_MODES),
                "status_api_exposes_slots": True,
                "unified_path_active": True,
            },
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
            blocked_portfolio_ids=self._active_abandoned_portfolio_ids(),
        )
        sessions = self.repository.list_tickable_sessions(statuses=TICKABLE_SESSION_STATUSES, limit=limit)
        result: dict[str, Any] = {
            "started_at": started.isoformat(),
            "in_progress": True,
            "completed_at": None,
            "auto_run_recovery": auto_run_recovery,
            "session_count": len(sessions),
            "session_timeout_seconds": self._default_session_timeout_seconds(),
            "processed": [],
            "errors": [],
        }
        self._last_result = result
        for session in sessions:
            try:
                timeout_payload = self._active_session_timeout_payload(session, now=started)
                if timeout_payload is not None:
                    result["errors"].append(timeout_payload)
                    self._last_result = result
                    continue
                timeout_seconds = self._default_session_timeout_seconds()
                progress = self._run_session_tick_with_timeout(
                    session,
                    as_of_time=as_of_time,
                    timeout_seconds=timeout_seconds,
                    started=started,
                )
                if progress is None:
                    payload = self._build_session_tick_timeout_payload(
                        session,
                        timeout_seconds=timeout_seconds,
                        started=started,
                    )
                    self._mark_session_tick_timeout_failed(session, payload=payload)
                    result["errors"].append(payload)
                    logger.warning("Paper v2 scheduler session tick timed out: %s", payload)
                    self._last_result = result
                    continue
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

    def _run_session_tick_with_timeout(
        self,
        session: Any,
        *,
        as_of_time: datetime | None,
        timeout_seconds: float,
        started: datetime,
    ) -> Any | None:
        result_holder: dict[str, Any] = {}
        error_holder: dict[str, BaseException] = {}

        def _worker() -> None:
            try:
                progress = self.runner.tick(session.session_id, as_of_time=as_of_time)
                result_holder["progress"] = progress
                if self._session_tick_was_abandoned(session.session_id):
                    self._mark_abandoned_session_tick_completed(
                        session,
                        progress=progress,
                        timeout_seconds=timeout_seconds,
                        started=started,
                    )
            except BaseException as exc:  # noqa: BLE001 - propagated when the worker completes before timeout.
                error_holder["error"] = exc

        thread = threading.Thread(
            target=_worker,
            name=f"paper-v2-session-tick-{session.session_id[:12]}",
            daemon=True,
        )
        with self._lock:
            self._active_session_ticks[session.session_id] = {
                "thread": thread,
                "started_at": started,
                "portfolio_id": session.portfolio_id,
                "status_before": session.status.value,
                "phase_before": session.phase.value,
            }
        thread.start()
        thread.join(timeout=max(0.0, float(timeout_seconds)))
        if thread.is_alive():
            timed_out_at = datetime.now(UTC)
            with self._lock:
                active = self._active_session_ticks.pop(session.session_id, None)
            if active is not None:
                active["timed_out_at"] = timed_out_at
                active["abandoned_at"] = timed_out_at
                active["policy"] = SESSION_TICK_TIMEOUT_POLICY
                self._remember_abandoned_session_tick(session.session_id, active)
            return None
        with self._lock:
            self._active_session_ticks.pop(session.session_id, None)
        if "error" in error_holder:
            raise error_holder["error"]
        return result_holder.get("progress")

    def _build_session_tick_timeout_payload(
        self,
        session: Any,
        *,
        timeout_seconds: float,
        started: datetime,
    ) -> dict[str, Any]:
        return {
            "error_code": SESSION_TICK_TIMEOUT_ERROR_CODE,
            "message": "paper v2 scheduler session tick exceeded bounded timeout",
            "context": {
                "session_id": session.session_id,
                "portfolio_id": session.portfolio_id,
                "timeout_seconds": timeout_seconds,
                "started_at": started.isoformat(),
                "status_before": session.status.value,
                "phase_before": session.phase.value,
                "policy": SESSION_TICK_TIMEOUT_POLICY,
                "scheduler_guard_released": True,
                "terminal_state": PaperSessionStatus.FAILED.value,
                "portfolio_after_timeout_state": PortfolioStatus.FAILED.value,
                "portfolio_terminal_state": PortfolioStatus.FAILED.value,
                "auto_run_recoverable": False,
                "orphan_worker_status_field": "abandoned_session_ticks",
            },
        }

    def _mark_session_tick_timeout_failed(
        self,
        session: Any,
        *,
        payload: dict[str, Any],
        event_type: str = SESSION_TICK_TIMEOUT_EVENT,
    ) -> None:
        self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.FAILED,
            phase=session.phase,
            started_at=session.started_at,
            completed_at=datetime.now(UTC),
            last_error=payload,
        )
        portfolio_status = self._timeout_portfolio_status(session, payload)
        self.repository.update_portfolio_status(session.portfolio_id, portfolio_status)
        self.repository.save_session_event(
            session_id=session.session_id,
            event_type=event_type,
            message=payload["message"],
            context=payload["context"],
        )
        self.repository.save_error(
            run_id=None,
            portfolio_id=session.portfolio_id,
            error=payload,
        )

    def _session_tick_was_abandoned(self, session_id: str) -> bool:
        with self._lock:
            return session_id in self._abandoned_session_ticks

    def _mark_abandoned_session_tick_completed(
        self,
        session: Any,
        *,
        progress: Any | None,
        timeout_seconds: float,
        started: datetime,
    ) -> None:
        payload = self._build_session_tick_timeout_payload(
            session,
            timeout_seconds=timeout_seconds,
            started=started,
        )
        context = payload["context"]
        context["stale_worker_completed_at"] = datetime.now(UTC).isoformat()
        if progress is not None and getattr(progress, "session", None) is not None:
            stale_session = progress.session
            context["stale_worker_result_status"] = getattr(getattr(stale_session, "status", None), "value", None)
            context["stale_worker_result_phase"] = getattr(getattr(stale_session, "phase", None), "value", None)
        self._mark_session_tick_timeout_failed(
            session,
            payload=payload,
            event_type="SESSION_TICK_TIMEOUT_STALE_WORKER_COMPLETED",
        )

    def _timeout_portfolio_status(self, session: Any, payload: dict[str, Any]) -> PortfolioStatus:
        context = payload.setdefault("context", {})
        try:
            portfolio = self.repository.get_portfolio(session.portfolio_id)
        except Exception:  # pragma: no cover - repository implementations already fail loudly elsewhere.
            portfolio = None
        if getattr(portfolio, "auto_run_enabled", False):
            context.update(
                {
                    "auto_run_enabled": True,
                    "auto_run_recoverable": True,
                    "portfolio_after_timeout_state": PortfolioStatus.READY.value,
                    "portfolio_recovery_state": PortfolioStatus.READY.value,
                    "portfolio_terminal_state": None,
                    "recovery_policy": "auto_run_recover_after_stale_worker_completes",
                }
            )
            return PortfolioStatus.READY
        context.update(
            {
                "auto_run_enabled": bool(getattr(portfolio, "auto_run_enabled", False)),
                "auto_run_recoverable": False,
                "portfolio_after_timeout_state": PortfolioStatus.FAILED.value,
                "portfolio_terminal_state": PortfolioStatus.FAILED.value,
            }
        )
        return PortfolioStatus.FAILED

    def _remember_abandoned_session_tick(self, session_id: str, active: dict[str, Any]) -> None:
        with self._lock:
            self._abandoned_session_ticks[session_id] = active
            if len(self._abandoned_session_ticks) <= 50:
                return
            for old_session_id, item in list(self._abandoned_session_ticks.items()):
                thread = item.get("thread")
                if not getattr(thread, "is_alive", lambda: False)():
                    self._abandoned_session_ticks.pop(old_session_id, None)
            while len(self._abandoned_session_ticks) > 50:
                oldest_id = min(
                    self._abandoned_session_ticks,
                    key=lambda key: self._abandoned_session_ticks[key].get("abandoned_at") or datetime.max.replace(tzinfo=UTC),
                )
                self._abandoned_session_ticks.pop(oldest_id, None)

    def _active_abandoned_portfolio_ids(self) -> set[str]:
        portfolio_ids: set[str] = set()
        with self._lock:
            for session_id, active in list(self._abandoned_session_ticks.items()):
                thread = active.get("thread")
                if not getattr(thread, "is_alive", lambda: False)():
                    self._abandoned_session_ticks.pop(session_id, None)
                    continue
                portfolio_id = active.get("portfolio_id")
                if portfolio_id:
                    portfolio_ids.add(str(portfolio_id))
        return portfolio_ids

    def _active_session_timeout_payload(self, session: Any, *, now: datetime) -> dict[str, Any] | None:
        with self._lock:
            active = self._active_session_ticks.get(session.session_id)
            if not active:
                return None
            thread = active.get("thread")
            if not getattr(thread, "is_alive", lambda: False)():
                self._active_session_ticks.pop(session.session_id, None)
                return None
            started_at = active.get("started_at")
            elapsed = (now - started_at).total_seconds() if isinstance(started_at, datetime) else None
            timeout_seconds = self._default_session_timeout_seconds()
            if elapsed is not None and elapsed >= timeout_seconds:
                self._active_session_ticks.pop(session.session_id, None)
                timed_out_at = active.get("timed_out_at") if isinstance(active.get("timed_out_at"), datetime) else now
                active["timed_out_at"] = timed_out_at
                active["abandoned_at"] = now
                active["policy"] = SESSION_TICK_TIMEOUT_POLICY
                self._remember_abandoned_session_tick(session.session_id, active)
            else:
                elapsed_context = elapsed
                return {
                    "error_code": "PAPER_V2_SESSION_TICK_STILL_RUNNING",
                    "message": "paper v2 scheduler skipped duplicate tick while previous session worker is still within timeout",
                    "context": {
                        "session_id": session.session_id,
                        "portfolio_id": session.portfolio_id,
                        "elapsed_seconds": elapsed_context,
                        "timeout_seconds": timeout_seconds,
                        "started_at": started_at.isoformat() if isinstance(started_at, datetime) else None,
                        "status_before": active.get("status_before"),
                        "phase_before": active.get("phase_before"),
                        "policy": "skip_duplicate_until_timeout",
                    },
                }
        payload = self._build_session_tick_timeout_payload(session, timeout_seconds=timeout_seconds, started=started_at or now)
        self._mark_session_tick_timeout_failed(session, payload=payload)
        return payload

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

    @staticmethod
    def _default_session_timeout_seconds() -> float:
        raw = (os.getenv("PAPER_TRADING_V2_SCHEDULER_SESSION_TIMEOUT_SECONDS") or "60").strip()
        try:
            value = float(raw)
        except ValueError:
            return 60.0
        return value if value > 0 else 60.0

    def _active_session_tick_status(self) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        snapshots: list[dict[str, Any]] = []
        with self._lock:
            for session_id, active in list(self._active_session_ticks.items()):
                thread = active.get("thread")
                if not getattr(thread, "is_alive", lambda: False)():
                    self._active_session_ticks.pop(session_id, None)
                    continue
                started_at = active.get("started_at")
                snapshots.append(
                    {
                        "session_id": session_id,
                        "portfolio_id": active.get("portfolio_id"),
                        "started_at": started_at.isoformat() if isinstance(started_at, datetime) else None,
                        "elapsed_seconds": (now - started_at).total_seconds() if isinstance(started_at, datetime) else None,
                        "timed_out_at": active.get("timed_out_at").isoformat()
                        if isinstance(active.get("timed_out_at"), datetime)
                        else None,
                        "status_before": active.get("status_before"),
                        "phase_before": active.get("phase_before"),
                    }
                )
        return snapshots

    def _abandoned_session_tick_status(self) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        snapshots: list[dict[str, Any]] = []
        with self._lock:
            for session_id, active in list(self._abandoned_session_ticks.items()):
                thread = active.get("thread")
                if not getattr(thread, "is_alive", lambda: False)():
                    self._abandoned_session_ticks.pop(session_id, None)
                    continue
                started_at = active.get("started_at")
                timed_out_at = active.get("timed_out_at")
                abandoned_at = active.get("abandoned_at")
                snapshots.append(
                    {
                        "session_id": session_id,
                        "portfolio_id": active.get("portfolio_id"),
                        "started_at": started_at.isoformat() if isinstance(started_at, datetime) else None,
                        "elapsed_seconds": (now - started_at).total_seconds() if isinstance(started_at, datetime) else None,
                        "timed_out_at": timed_out_at.isoformat() if isinstance(timed_out_at, datetime) else None,
                        "abandoned_at": abandoned_at.isoformat() if isinstance(abandoned_at, datetime) else None,
                        "status_before": active.get("status_before"),
                        "phase_before": active.get("phase_before"),
                        "policy": active.get("policy"),
                    }
                )
        return snapshots


paper_trading_v2_scheduler = PaperTradingV2SessionScheduler()
