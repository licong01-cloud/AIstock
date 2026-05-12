from __future__ import annotations

import hashlib
import json
from pathlib import Path

from backend.services.qe_archive.listeners.qe_experiment_completed_listener import (
    MODEL_SYNC_SCHEMA_VERSION,
    QE_EXPERIMENT_COMPLETED_EVENT_TYPE,
    QEExperimentCompletedModelSyncListener,
)
from backend.services.qe_archive.models import ClaimedOutboxEvent
from backend.services.qe_archive.worker_service import QEArchiveWorkerService


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _event(tmp_path: Path, *, apply: bool = False, expected_hash: str | None = None) -> ClaimedOutboxEvent:
    source_dir = tmp_path / "qe_models"
    source_dir.mkdir(exist_ok=True)
    model_path = source_dir / "early.pt"
    model_path.write_bytes(b"early-model")
    return ClaimedOutboxEvent(
        event_id="evt_qe_model_sync",
        event_type=QE_EXPERIMENT_COMPLETED_EVENT_TYPE,
        source_system="qe",
        source_id="qe_20260512_demo",
        payload={
            "schema_version": MODEL_SYNC_SCHEMA_VERSION,
            "routing_class": "model_sync",
            "model_sync": {
                "source_dir": str(source_dir),
                "cache_root": str(tmp_path / "cache"),
                "algo_code": "V25_1_SMALL_CAP",
                "models": ["early.pt"],
                "expected_sha256": {"early.pt": expected_hash or _sha256(b"early-model")},
                "apply": apply,
            },
        },
    )


def test_listener_dry_run_plans_without_copying(tmp_path: Path) -> None:
    listener = QEExperimentCompletedModelSyncListener()

    result = listener.handle(_event(tmp_path))

    assert result.success is True
    assert result.stats["mode"] == "dry_run"
    assert result.stats["algo_code"] == "V25_1_SMALL_CAP"
    assert result.stats["plans"][0]["action"] == "copy"
    assert not (tmp_path / "cache" / "V25_1_SMALL_CAP" / "early.pt").exists()


def test_listener_refuses_apply_without_listener_permission(tmp_path: Path) -> None:
    listener = QEExperimentCompletedModelSyncListener(allow_apply=False)

    result = listener.handle(_event(tmp_path, apply=True))

    assert result.success is False
    assert "allow_apply is false" in str(result.error)
    assert not (tmp_path / "cache" / "V25_1_SMALL_CAP" / "early.pt").exists()


def test_listener_rejects_string_boolean_apply_guardrail(tmp_path: Path) -> None:
    listener = QEExperimentCompletedModelSyncListener(allow_apply=True)
    event = _event(tmp_path)
    event.payload["model_sync"]["apply"] = "false"

    result = listener.handle(event)

    assert result.success is False
    assert "model_sync.apply must be a JSON boolean" in str(result.error)
    assert not (tmp_path / "cache" / "V25_1_SMALL_CAP" / "early.pt").exists()


def test_listener_apply_copies_only_when_enabled(tmp_path: Path) -> None:
    listener = QEExperimentCompletedModelSyncListener(allow_apply=True)

    result = listener.handle(_event(tmp_path, apply=True))

    destination = tmp_path / "cache" / "V25_1_SMALL_CAP" / "early.pt"
    sidecar = destination.with_name(destination.name + ".aistock-sync.json")
    assert result.success is True
    assert destination.read_bytes() == b"early-model"
    metadata = json.loads(sidecar.read_text(encoding="utf-8"))
    assert metadata["algo_code"] == "V25_1_SMALL_CAP"
    assert result.stats["applied"][0]["destination"] == str(destination)


def test_listener_rejects_wrong_event_type(tmp_path: Path) -> None:
    listener = QEExperimentCompletedModelSyncListener()
    event = _event(tmp_path)
    bad = ClaimedOutboxEvent(
        event_id=event.event_id,
        event_type="factor.recompute.completed",
        source_system=event.source_system,
        source_id=event.source_id,
        payload=event.payload,
    )

    result = listener.handle(bad)

    assert result.success is False
    assert "unsupported" in str(result.error)


def test_worker_service_routes_model_sync_event_to_listener(tmp_path: Path) -> None:
    event = _event(tmp_path)

    class FakeRepository:
        def __init__(self) -> None:
            self.completed_events: list[str] = []
            self.completed_jobs: list[tuple[str, str | None, dict]] = []

        def claim_outbox_events(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs["event_types"] == ("qe.loop.completed", "qe.experiment.completed")
            return [event]

        def create_archive_job(self, job):  # type: ignore[no-untyped-def]
            assert job.event_id == event.event_id
            return "job_model_sync"

        def complete_archive_job(self, job_id, *, run_id=None, stats=None):  # type: ignore[no-untyped-def]
            self.completed_jobs.append((job_id, run_id, dict(stats or {})))

        def complete_outbox_event(self, event_id):  # type: ignore[no-untyped-def]
            self.completed_events.append(event_id)

        def fail_archive_job(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("model sync dry-run should not fail job")

        def fail_outbox_event(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("model sync dry-run should not retry outbox")

    class FailingBackfillService:
        def archive_experiment_completed(self, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("model_sync events must not route to archive backfill")

        def archive_loop_completed(self, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("unexpected loop archive")

    repository = FakeRepository()
    result = QEArchiveWorkerService(
        repository=repository,  # type: ignore[arg-type]
        backfill_service=FailingBackfillService(),  # type: ignore[arg-type]
        enabled=True,
    ).run_once(limit=1)

    assert result["claimed"] == 1
    assert result["completed"] == 1
    assert result["failed"] == 0
    assert repository.completed_events == [event.event_id]
    assert repository.completed_jobs[0][0] == "job_model_sync"
    assert repository.completed_jobs[0][1] == event.source_id
    assert repository.completed_jobs[0][2]["mode"] == "dry_run"
    assert not (tmp_path / "cache" / "V25_1_SMALL_CAP" / "early.pt").exists()
