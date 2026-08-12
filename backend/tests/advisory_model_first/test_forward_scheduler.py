from __future__ import annotations

import time
import threading

from backend.services.advisory_forward.scheduler import AdvisoryForwardScheduler


class _Service:
    def __init__(self) -> None:
        self.calls = 0

    def run_once(self):
        self.calls += 1
        return {"schema_version": "advisory_forward_run_once_v1", "results": []}


def test_scheduler_is_explicit_and_reports_last_run(monkeypatch) -> None:
    monkeypatch.delenv("AISTOCK_ADVISORY_FORWARD_SCHEDULER_ENABLED", raising=False)
    service = _Service()
    scheduler = AdvisoryForwardScheduler(service=service)

    assert scheduler.status()["configured_enabled"] is False
    assert scheduler.status()["running"] is False

    result = scheduler.run_once()

    assert result["schema_version"] == "advisory_forward_run_once_v1"
    assert service.calls == 1
    assert scheduler.status()["last_run_at"] is not None


def test_scheduler_thread_only_starts_when_start_is_called() -> None:
    service = _Service()
    scheduler = AdvisoryForwardScheduler(service=service)

    assert scheduler.status()["thread_alive"] is False
    scheduler.start(interval_seconds=60)
    try:
        for _ in range(50):
            if service.calls:
                break
            time.sleep(0.01)
        assert scheduler.status()["running"] is True
        assert service.calls >= 1
    finally:
        scheduler.shutdown(wait=True)
    assert scheduler.status()["running"] is False


def test_scheduler_reports_concurrent_run_without_starting_duplicate_work() -> None:
    started = threading.Event()
    release = threading.Event()

    class _BlockingService:
        def run_once(self):
            started.set()
            assert release.wait(timeout=2)
            return {"schema_version": "advisory_forward_run_once_v1", "results": []}

    scheduler = AdvisoryForwardScheduler(service=_BlockingService())
    thread = threading.Thread(target=scheduler.run_once)
    thread.start()
    assert started.wait(timeout=1)
    try:
        busy = scheduler.run_once()
        assert busy["status"] == "ALREADY_RUNNING"
        assert busy["reason_code"] == "ADVISORY_FORWARD_RUN_ALREADY_IN_PROGRESS"
    finally:
        release.set()
        thread.join(timeout=2)
    assert thread.is_alive() is False
