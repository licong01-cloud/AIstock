"""Unit tests for archive_handler_adapter (P1.1 Codex round 2).

Pure: no DB. Uses minimal fake handlers / events.
"""
from __future__ import annotations

import pytest

from backend.services.qe_archive.handlers.contract import (
    ArchiveResult,
    HandlerStatus,
)
from backend.services.qe_archive.models import ArchiveJobRecord, ClaimedOutboxEvent
from backend.services.qe_archive.worker import (
    ArchiveWorkerEventResult,
    archive_handler_adapter,
)


def _evt(event_id: str = "evt_x", payload: dict | None = None) -> ClaimedOutboxEvent:
    return ClaimedOutboxEvent(
        event_id=event_id,
        event_type="paper.portfolio_run.completed",
        source_system="paper_v2",
        source_id="prun_test",
        source_sub_id="prun_test",
        payload=payload or {"run_id": "prun_test", "schema_version": 1, "routing_class": "archive"},
    )


class _SuccessHandler:
    def handle(self, event, archive_job):
        assert isinstance(archive_job, ArchiveJobRecord)
        return ArchiveResult(
            status=HandlerStatus.SUCCESS,
            rows_inserted=42, rows_upserted=3,
            stats={"foo": "bar"},
        )


class _NoopHandler:
    def handle(self, event, archive_job):
        return ArchiveResult(status=HandlerStatus.NOOP, stats={"reason": "deferred"})


class _FailedHandler:
    def handle(self, event, archive_job):
        return ArchiveResult(
            status=HandlerStatus.FAILED,
            error_message="handler refused to complete",
        )


class _RaisingHandler:
    def handle(self, event, archive_job):
        raise ValueError("simulated unrecoverable error")


class _RuntimeRaisingHandler:
    def handle(self, event, archive_job):
        raise RuntimeError("oh no")


class TestArchiveHandlerAdapter:
    def test_success_path(self):
        adapter = archive_handler_adapter(_SuccessHandler())
        result = adapter(_evt())
        assert isinstance(result, ArchiveWorkerEventResult)
        assert result.success is True
        assert result.error is None
        assert result.stats["rows_inserted"] == 42
        assert result.stats["rows_upserted"] == 3
        assert result.stats["handler_status"] == "success"
        assert result.stats["foo"] == "bar"

    def test_noop_treated_as_success(self):
        """NOOP means handler correctly deferred / replay-skipped — must not
        retry-storm the worker. Adapter reports success=True with handler_status=noop.
        """
        adapter = archive_handler_adapter(_NoopHandler())
        result = adapter(_evt())
        assert result.success is True
        assert result.stats["handler_status"] == "noop"
        assert result.stats["rows_inserted"] == 0

    def test_failed_status_returns_failure(self):
        adapter = archive_handler_adapter(_FailedHandler())
        result = adapter(_evt())
        assert result.success is False
        assert result.error == "handler refused to complete"

    def test_raise_returns_failure_with_type_in_error(self):
        adapter = archive_handler_adapter(_RaisingHandler())
        result = adapter(_evt())
        assert result.success is False
        assert "ValueError" in result.error
        assert "simulated unrecoverable error" in result.error

    def test_raise_runtime_error_caught(self):
        adapter = archive_handler_adapter(_RuntimeRaisingHandler())
        result = adapter(_evt())
        assert result.success is False
        assert "RuntimeError" in result.error
        assert "oh no" in result.error

    def test_run_id_preserved_from_payload(self):
        adapter = archive_handler_adapter(_SuccessHandler())
        result = adapter(_evt(payload={
            "run_id": "prun_specific_id",
            "schema_version": 1, "routing_class": "archive",
        }))
        assert result.run_id == "prun_specific_id"

    def test_run_id_falls_back_to_source_sub_id(self):
        adapter = archive_handler_adapter(_SuccessHandler())
        evt = ClaimedOutboxEvent(
            event_id="x", event_type="paper.x.y", source_system="paper_v2",
            source_id="src", source_sub_id="prun_from_subid", payload={},
        )
        result = adapter(evt)
        assert result.run_id == "prun_from_subid"

    def test_run_id_falls_back_to_source_id_when_no_payload_or_subid(self):
        adapter = archive_handler_adapter(_SuccessHandler())
        evt = ClaimedOutboxEvent(
            event_id="x", event_type="paper.x.y", source_system="paper_v2",
            source_id="prun_from_source_id", source_sub_id=None, payload=None,
        )
        result = adapter(evt)
        assert result.run_id == "prun_from_source_id"
