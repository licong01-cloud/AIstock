from __future__ import annotations

import asyncio
from typing import Any, Mapping

from backend.services.multi_alpha.durable_orchestrator import (
    DurableMultiAlphaOrchestrator,
    DurableOrchestratorConfig,
)


class _Repository:
    def __init__(self) -> None:
        self.run = {
            "id": "macb_archive",
            "status": "succeeded",
            "roster_hash": "roster",
            "reason": {"reason_code": "done"},
        }
        self.events: dict[tuple[str, str], dict[str, Any]] = {}

    def list_runs_pending_archive(self, *, limit: int) -> list[Mapping[str, Any]]:
        assert limit == 10
        terminal_delivery = any(
            phase in {"archive_enqueued", "archive_duplicate"}
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
    def __init__(self, results: list[Any]) -> None:
        self.results = list(results)
        self.calls = 0

    def enqueue_multi_alpha_combine_completed_result(self, **_kwargs: Any) -> Mapping[str, Any]:
        self.calls += 1
        result = self.results[min(self.calls - 1, len(self.results) - 1)]
        if isinstance(result, Exception):
            raise result
        return result


class _Noop:
    pass


def _orchestrator(repository: _Repository, capture: _Capture) -> DurableMultiAlphaOrchestrator:
    return DurableMultiAlphaOrchestrator(
        repository=repository,  # type: ignore[arg-type]
        planner=_Noop(),  # type: ignore[arg-type]
        adapter=_Noop(),  # type: ignore[arg-type]
        archive_capture=capture,  # type: ignore[arg-type]
        active_import_service=_Noop(),  # type: ignore[arg-type]
        config=DurableOrchestratorConfig(
            poll_seconds=0.2,
            lease_seconds=60,
            heartbeat_seconds=5,
            items_per_pass=1,
            archive_batch_size=10,
        ),
        owner_id="worker",
    )


def test_post_terminal_archive_states_are_visible_and_retry_is_idempotent() -> None:
    repository = _Repository()
    capture = _Capture(
        [
            {
                "inserted": False,
                "skipped_reason": "disabled",
                "event_id": "qear_evt_archive",
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
    assert ("qear_evt_archive", "archive_skipped_disabled") in repository.events

    assert asyncio.run(orchestrator.archive_pass()) == 1
    assert ("qear_evt_archive", "archive_enqueued") in repository.events
    assert repository.run["status"] == "succeeded"

    assert asyncio.run(orchestrator.archive_pass()) == 0
    assert capture.calls == 2


def test_archive_enqueue_error_is_durable_and_never_rolls_back_terminal_run() -> None:
    repository = _Repository()
    capture = _Capture([RuntimeError("outbox unavailable")])
    orchestrator = _orchestrator(repository, capture)

    assert asyncio.run(orchestrator.archive_pass()) == 0
    assert repository.run["status"] == "succeeded"
    event = next(iter(repository.events.values()))
    assert event["phase"] == "archive_error"
    assert event["payload"]["error"]["message"] == "outbox unavailable"
