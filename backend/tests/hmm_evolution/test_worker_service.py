from __future__ import annotations

from collections import deque

import pytest

from backend.services.hmm_evolution.errors import SourceUnavailableError
from backend.services.hmm_evolution.worker_service import (
    HMMEvolutionWorkerService,
    WorkerServiceConfig,
)


class FakeStopSignal:
    def __init__(self) -> None:
        self.stopped = False
        self.waits: list[float | None] = []

    def is_set(self) -> bool:
        return self.stopped

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout)
        self.stopped = True
        return True


class FakeWorker:
    def __init__(
        self,
        outcomes: list[bool],
        *,
        stop_signal: FakeStopSignal | None = None,
        failure: Exception | None = None,
    ) -> None:
        self.outcomes = deque(outcomes)
        self.stop_signal = stop_signal
        self.failure = failure
        self.assert_calls = 0
        self.run_calls = 0

    def assert_runnable(self) -> None:
        self.assert_calls += 1

    def run_once(self) -> bool:
        self.run_calls += 1
        if self.failure is not None:
            raise self.failure
        claimed = self.outcomes.popleft()
        if claimed and self.stop_signal is not None:
            self.stop_signal.stopped = True
        return claimed


def test_worker_service_drains_queue_before_interruptible_idle_wait() -> None:
    stop_signal = FakeStopSignal()
    worker = FakeWorker([True, True, False])
    service = HMMEvolutionWorkerService(
        worker,
        config=WorkerServiceConfig(poll_seconds=0.25),
    )

    result = service.run(stop_signal)

    assert result.cycles == 3
    assert result.processed_slices == 2
    assert worker.assert_calls == 1
    assert worker.run_calls == 3
    assert stop_signal.waits == [0.25]


def test_worker_service_finishes_claimed_slice_before_stopping() -> None:
    stop_signal = FakeStopSignal()
    worker = FakeWorker([True], stop_signal=stop_signal)

    result = HMMEvolutionWorkerService(worker).run(stop_signal)

    assert result.cycles == 1
    assert result.processed_slices == 1
    assert worker.run_calls == 1
    assert stop_signal.waits == []


@pytest.mark.parametrize(
    "failure",
    [
        SourceUnavailableError("QE source is unavailable"),
        RuntimeError("database connection failed"),
    ],
)
def test_worker_service_propagates_failures_instead_of_treating_them_as_idle(
    failure: Exception,
) -> None:
    worker = FakeWorker([], failure=failure)

    with pytest.raises(type(failure), match=str(failure)):
        HMMEvolutionWorkerService(worker).run(FakeStopSignal())


@pytest.mark.parametrize("poll_seconds", [0, 0.09, 301, float("nan"), float("inf")])
def test_worker_service_rejects_unsafe_poll_intervals(poll_seconds: float) -> None:
    with pytest.raises(ValueError, match="between 0.1 and 300"):
        WorkerServiceConfig(poll_seconds=poll_seconds)
