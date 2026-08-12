"""Bounded QE dispatch observer.

One process-local cycle owns automatic node health and remote task progress.
It reads one aggregate database snapshot every minute, probes only ``/health``
for each node, and persists only real state changes.  Metrics and manual task
discovery are explicit operator actions rather than heartbeat side effects.
"""

from __future__ import annotations

import asyncio
import logging
import os

logger = logging.getLogger("aistock.node_health_scheduler")


def _observer_interval_seconds() -> int:
    try:
        configured = int(os.getenv("AISTOCK_DISPATCH_OBSERVER_INTERVAL_SECONDS", "60"))
    except (TypeError, ValueError):
        configured = 60
    return max(60, configured)


_OBSERVER_INTERVAL = _observer_interval_seconds()


class NodeHealthScheduler:
    def __init__(self) -> None:
        self._stop_event: asyncio.Event | None = None
        self._observer_task: asyncio.Task | None = None

    def _ensure_stop_event(self) -> asyncio.Event:
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        return self._stop_event

    async def _observer_loop(self) -> None:
        from ..services.dispatch_service import DispatchService

        svc = DispatchService()
        stop = self._ensure_stop_event()

        # Collector recovery is a startup action only.  A failed collector owns
        # its own bounded reconnect loop and is not rediscovered every cycle.
        try:
            resumed = await svc.resume_log_collectors()
            if resumed:
                logger.info("resumed %d dispatch log collectors at startup", resumed)
        except Exception as exc:
            logger.warning("startup dispatch log collector recovery failed: %s", exc)

        while not stop.is_set():
            try:
                result = await svc.observe_dispatch_state()
                if result["node_changes"] or result["task_changes"]:
                    logger.info(
                        "dispatch observer applied changes: nodes=%d tasks=%d",
                        result["node_changes"],
                        result["task_changes"],
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("dispatch observer cycle failed: %s", exc, exc_info=True)

            try:
                await asyncio.wait_for(stop.wait(), timeout=_OBSERVER_INTERVAL)
            except asyncio.TimeoutError:
                continue

    def start(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Start the single observer on the backend event loop."""
        if self._observer_task is not None and not self._observer_task.done():
            return
        self._stop_event = asyncio.Event()
        target_loop = loop or asyncio.get_running_loop()
        self._observer_task = target_loop.create_task(self._observer_loop())
        logger.info("dispatch observer started (interval=%ds)", _OBSERVER_INTERVAL)

    def shutdown(self) -> None:
        if self._stop_event:
            self._stop_event.set()
        if self._observer_task:
            self._observer_task.cancel()
        logger.info("dispatch observer stopped")


node_health_scheduler = NodeHealthScheduler()
