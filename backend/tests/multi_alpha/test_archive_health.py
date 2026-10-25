from __future__ import annotations

import asyncio
from typing import Any, Mapping

import pytest

from backend.services.multi_alpha.durable_orchestrator import (
    DurableMultiAlphaOrchestrator,
    DurableOrchestratorConfig,
)
from backend.services.qe_archive.event_capture import QEArchiveEventCapture


class _Repository:
    def __init__(self) -> None:
        self.run = {
            "id": "macb_archive",
            "status": "succeeded",
            "roster_hash": "roster",
            "reason": {"reason_code": "done"},
        }
        self.events: dict[tuple[str, str], dict[str, Any]] = {}

    def list_runs_pending_archive(
        self,
        *,
        limit: int,
        archive_retry_backoff_seconds: int = 0,
    ) -> list[Mapping[str, Any]]:
        assert limit == 10
        terminal_delivery = any(
            phase in {"archive_enqueued", "archive_duplicate", "archive_skipped_disabled"}
            for _event_id, phase in self.events
        )
        return [] if terminal_delivery else [dict(self.run)]

    def append_archive_delivery_event(
        self,
        *,
        run_id: str,
        phase: str,
        archive_event_id: str,
        payload: Mapping[str, Any],
        reason_code: str | None = None,
    ) -> Mapping[str, Any]:
        key = (archive_event_id, phase)
        self.events.setdefault(
            key,
            {
                "run_id": run_id,
                "phase": phase,
                "archive_event_id": archive_event_id,
                "payload": dict(payload),
                "reason_code": reason_code,
            },
        )
        return self.events[key]


class _Capture:
    def __init__(self, results: list[Any], *, enabled: bool = True) -> None:
        self.results = list(results)
        self.calls = 0
        self.enabled = enabled

    def enqueue_multi_alpha_combine_completed_result(self, **_kwargs: Any) -> Mapping[str, Any]:
        self.calls += 1
        result = self.results[min(self.calls - 1, len(self.results) - 1)]
        if isinstance(result, Exception):
            raise result
        return result


class _Noop:
    pass


def _orchestrator(repository: _Repository, capture: Any = None) -> DurableMultiAlphaOrchestrator:
    return DurableMultiAlphaOrchestrator(
        repository=repository,  # type: ignore[arg-type]
        planner=_Noop(),  # type: ignore[arg-type]
        adapter=_Noop(),  # type: ignore[arg-type]
        archive_capture=capture,  # type: ignore[arg-type]
        active_import_service=_Noop(),  # type: ignore[arg-type]
        recovery_worker=_Noop(),  # type: ignore[arg-type]
        config=DurableOrchestratorConfig(
            poll_seconds=0.2,
            lease_seconds=60,
            heartbeat_seconds=5,
            items_per_pass=1,
            archive_batch_size=10,
        ),
        owner_id="worker",
    )


@pytest.mark.parametrize(
    "capture_env,realtime_env,enabled",
    [(None, None, True), ("", " ", True), ("true", "1", True),
     ("false", None, False), (None, "0", False), ("true", "off", False),
     ("no", "true", False)],
)
def test_formal_runtime_defaults_to_archive_but_honors_explicit_disable(
    monkeypatch, capture_env, realtime_env, enabled,
) -> None:
    for name, value in (("QE_ARCHIVE_EVENT_CAPTURE_ENABLED", capture_env),
                        ("QE_ARCHIVE_REALTIME_ENABLED", realtime_env)):
        monkeypatch.delenv(name, raising=False)
        if value is not None:
            monkeypatch.setenv(name, value)
    events = {}

    class Outbox:
        def insert_outbox_event(self, event):
            inserted = event.event_id not in events
            events[event.event_id] = event
            return inserted

    monkeypatch.setattr("backend.services.qe_archive.event_capture.QEArchiveRepository", Outbox)
    repository = _Repository()
    orchestrator = _orchestrator(repository)
    assert asyncio.run(orchestrator.archive_pass()) == int(enabled)
    assert len(events) == int(enabled)
    if enabled:
        # Lost delivery acknowledgement is retried via the same outbox identity.
        repository.events.clear()
        assert asyncio.run(orchestrator.archive_pass()) == 1
        assert len(events) == 1
        assert next(iter(repository.events.values()))["phase"] == "archive_duplicate"
    assert asyncio.run(orchestrator.archive_pass()) == 0
    if capture_env is None:
        assert QEArchiveEventCapture().enabled is False  # diagnostic adapter stays opt-in


def test_archive_pass_exits_before_scanning_when_capture_disabled() -> None:
    """A globally disabled archive capture must not query terminal runs at all."""
    repository = _Repository()
    capture = _Capture(
        [{"inserted": True, "event_id": "qear_evt_archive", "duplicate": False}],
        enabled=False,
    )
    orchestrator = _orchestrator(repository, capture)

    assert asyncio.run(orchestrator.archive_pass()) == 0
    assert capture.calls == 0
    assert repository.events == {}


def test_post_terminal_archive_enqueue_is_visible_and_retry_is_idempotent() -> None:
    repository = _Repository()
    capture = _Capture(
        [
            {
                "inserted": True,
                "event_id": "qear_evt_archive",
                "duplicate": False,
            },
            {
                "inserted": True,
                "event_id": "qear_evt_archive",
                "duplicate": False,
            },
        ]
    )
    orchestrator = _orchestrator(repository, capture)

    assert asyncio.run(orchestrator.archive_pass()) == 1
    assert repository.run["status"] == "succeeded"
    assert ("qear_evt_archive", "archive_enqueued") in repository.events

    # archive_enqueued is a final disposition for the current configuration;
    # the terminal run is no longer re-scanned on the next cycle.
    assert asyncio.run(orchestrator.archive_pass()) == 0
    assert capture.calls == 1


def test_archive_enqueue_error_is_durable_and_never_rolls_back_terminal_run() -> None:
    repository = _Repository()
    capture = _Capture([RuntimeError("outbox unavailable")])
    orchestrator = _orchestrator(repository, capture)

    assert asyncio.run(orchestrator.archive_pass()) == 0
    assert repository.run["status"] == "succeeded"
    event = next(iter(repository.events.values()))
    assert event["phase"] == "archive_error"
    assert event["payload"]["error"]["message"] == "outbox unavailable"
