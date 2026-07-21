"""Long-running service loop for durable HMM evolution evaluation work.

The service owns no schedule and creates no research work.  It only asks the
existing :class:`HMMEvolutionWorker` to claim bounded slices from the durable
queue.  Task state remains authoritative in ``hmm_evolution.*``.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

logger = logging.getLogger(__name__)


class WorkerRunner(Protocol):
    """Small protocol that keeps the service loop independently testable."""

    def assert_runnable(self) -> None: ...

    def run_once(self) -> bool: ...


class StopSignal(Protocol):
    """Subset of ``threading.Event`` required by the service."""

    def is_set(self) -> bool: ...

    def wait(self, timeout: float | None = None) -> bool: ...


@dataclass(frozen=True)
class WorkerServiceConfig:
    """Bounded operational configuration for the queue consumer."""

    poll_seconds: float = 5.0

    def __post_init__(self) -> None:
        value = float(self.poll_seconds)
        if not math.isfinite(value) or not 0.1 <= value <= 300.0:
            raise ValueError("poll_seconds must be finite and between 0.1 and 300")


@dataclass(frozen=True)
class WorkerServiceResult:
    """Non-authoritative process receipt for structured shutdown logging."""

    cycles: int
    processed_slices: int


class HMMEvolutionWorkerService:
    """Continuously consume already-queued evaluation work until stopped."""

    def __init__(
        self,
        worker: WorkerRunner,
        *,
        config: WorkerServiceConfig | None = None,
    ) -> None:
        self._worker = worker
        self._config = config or WorkerServiceConfig()

    def run(
        self,
        stop_signal: StopSignal,
        on_cycle: Callable[[bool], None] | None = None,
    ) -> WorkerServiceResult:
        """Run until ``stop_signal`` is set, propagating every worker failure.

        ``on_cycle`` is invoked after every completed cycle with the cycle's
        claimed flag; it is the durable worker-status heartbeat seam.  A cycle
        that raises simply never reports, which supervision reads as staleness.
        """

        self._worker.assert_runnable()
        cycles = 0
        processed_slices = 0
        logger.info(
            "HMM evolution worker service started poll_seconds=%s",
            self._config.poll_seconds,
        )
        while not stop_signal.is_set():
            claimed = self._worker.run_once()
            cycles += 1
            if on_cycle is not None:
                on_cycle(claimed)
            if claimed:
                processed_slices += 1
                continue
            logger.debug(
                "HMM evolution worker service idle poll_seconds=%s",
                self._config.poll_seconds,
            )
            stop_signal.wait(self._config.poll_seconds)
        result = WorkerServiceResult(
            cycles=cycles,
            processed_slices=processed_slices,
        )
        logger.info(
            "HMM evolution worker service stopped cycles=%s processed_slices=%s",
            result.cycles,
            result.processed_slices,
        )
        return result
