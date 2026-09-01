from __future__ import annotations

import asyncio
import threading


class DurableOrchestratorWakeup:
    """Process-local, coalescing wake-up signal for the durable worker.

    Submission/control services are synchronous and can run in worker threads,
    while the durable orchestrator waits on the backend asyncio loop.  Keep the
    bridge deliberately process-local: PostgreSQL remains the durable source of
    truth and the worker's 60-second safety sweep covers restarts and other
    backend processes.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribers: set[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = set()

    def register(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        event: asyncio.Event,
    ) -> None:
        with self._lock:
            self._subscribers.add((loop, event))

    def unregister(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        event: asyncio.Event,
    ) -> None:
        with self._lock:
            self._subscribers.discard((loop, event))

    def notify(self) -> None:
        with self._lock:
            subscribers = tuple(self._subscribers)
        for loop, event in subscribers:
            if loop.is_closed():
                self.unregister(loop=loop, event=event)
                continue
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                # The loop can close between is_closed() and scheduling.  The
                # committed PostgreSQL row is still covered by the next
                # process's startup scan / 60-second safety sweep.
                self.unregister(loop=loop, event=event)


durable_orchestrator_wakeup = DurableOrchestratorWakeup()


def notify_durable_orchestrator() -> None:
    durable_orchestrator_wakeup.notify()
