from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from backend.routers.multi_alpha import (
    DurableControlRequest,
    DurableRecoveryExecuteRequest,
    _durable_event_stream,
    _durable_control_http_error,
    execute_multi_alpha_durable_child_recovery,
    list_multi_alpha_durable_children,
    list_multi_alpha_durable_events,
    stream_multi_alpha_durable_events,
    submit_multi_alpha_durable_control,
)
from backend.routers import multi_alpha as multi_alpha_router


def test_unknown_durable_control_exception_maps_to_http_500() -> None:
    error = _durable_control_http_error(RuntimeError("unexpected internal failure"))

    assert error.status_code == 500
    assert error.detail["reason_code"] == "multi_alpha_durable_control_failed"


def test_recovery_execute_requires_preview_command_identity() -> None:
    with pytest.raises(ValidationError):
        DurableRecoveryExecuteRequest.model_validate(
            {
                "retry_mode": "backtest_only",
                "scope_hash": "a" * 64,
            }
        )


def test_generic_control_endpoint_rejects_unbound_child_retry() -> None:
    with pytest.raises(HTTPException) as caught:
        submit_multi_alpha_durable_control(
            run_id="macb_source",
            request=DurableControlRequest(
                action="child_retry",
                child_id="macbc_target",
                request={"retry_mode": "backtest_only"},
            ),
            idempotency_key="recovery-key",
        )

    assert caught.value.status_code == 400
    assert caught.value.detail["reason_code"] == "multi_alpha_invalid_control_command"
    assert "preview-bound" in caught.value.detail["message"]


def test_recovery_execute_rejects_command_identity_different_from_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Repository:
        def get_child(self, child_id: str):
            return {"child_id": child_id, "run_id": "macb_source"}

    class ControlService:
        def __init__(self) -> None:
            self.repository = Repository()

        def submit(self, **_kwargs):
            raise AssertionError("stale preview must be rejected before command submission")

    class RecoveryService:
        def __init__(self, _repository) -> None:
            pass

        def preview(self, **_kwargs):
            return SimpleNamespace(
                command_id="macmd_" + "a" * 64,
                scope_hash="b" * 64,
                scope={"retry_mode": "backtest_only"},
            )

    monkeypatch.setattr(multi_alpha_router, "DurableMultiAlphaControlService", ControlService)
    monkeypatch.setattr(multi_alpha_router, "DurableRecoveryService", RecoveryService)

    with pytest.raises(HTTPException) as caught:
        execute_multi_alpha_durable_child_recovery(
            run_id="macb_source",
            child_id="macbc_target",
            request=DurableRecoveryExecuteRequest(
                retry_mode="backtest_only",
                scope_hash="b" * 64,
                preview_command_id="macmd_" + "c" * 64,
            ),
            idempotency_key="stable-recovery-key",
        )

    assert caught.value.status_code == 409
    assert caught.value.detail["reason_code"] == "recovery_scope_stale"


def test_durable_event_page_uses_stable_cursor_and_has_more(monkeypatch: pytest.MonkeyPatch) -> None:
    class Repository:
        def get_run(self, run_id: str):
            return {"id": run_id, "status": "running"}

        def list_events(self, run_id: str, *, after_event_id: int, limit: int):
            assert run_id == "macb_run"
            assert after_event_id == 10
            assert limit == 3
            return [
                {"event_id": 11, "run_id": run_id, "event_type": "phase_changed"},
                {"event_id": 12, "run_id": run_id, "event_type": "heartbeat"},
                {"event_id": 13, "run_id": run_id, "event_type": "completed"},
            ]

    monkeypatch.setattr(multi_alpha_router, "MultiAlphaDurableRepository", Repository)

    response = list_multi_alpha_durable_events("macb_run", after_event_id=10, limit=2)

    assert response["data"]["next_event_id"] == 12
    assert response["data"]["has_more"] is True
    assert [row["event_id"] for row in response["data"]["events"]] == [11, 12]


def test_children_can_include_all_run_attempts_without_n_plus_one(monkeypatch: pytest.MonkeyPatch) -> None:
    class Repository:
        def list_children(self, _run_id: str):
            return [
                {"child_id": "child_1", "selected_attempt_id": "attempt_2"},
                {"child_id": "child_2", "selected_attempt_id": None},
            ]

        def list_attempts_for_run(self, run_id: str):
            assert run_id == "macb_run"
            return [
                {"attempt_id": "attempt_1", "child_id": "child_1"},
                {"attempt_id": "attempt_2", "child_id": "child_1"},
            ]

    class Service:
        def __init__(self) -> None:
            self.repository = Repository()

        def capabilities(self, *, run_id: str):
            assert run_id == "macb_run"
            return {"run_id": run_id}

    monkeypatch.setattr(multi_alpha_router, "DurableMultiAlphaControlService", Service)

    response = list_multi_alpha_durable_children("macb_run", include_attempts=True)

    children = response["data"]["children"]
    assert [attempt["selected"] for attempt in children[0]["attempts"]] == [False, True]
    assert children[1]["attempts"] == []


def test_durable_event_page_missing_run_is_structured_404(monkeypatch: pytest.MonkeyPatch) -> None:
    class Repository:
        def get_run(self, _run_id: str):
            return None

    monkeypatch.setattr(multi_alpha_router, "MultiAlphaDurableRepository", Repository)

    with pytest.raises(HTTPException) as caught:
        list_multi_alpha_durable_events("missing", after_event_id=0, limit=100)

    assert caught.value.status_code == 404
    assert caught.value.detail["reason_code"] == "multi_alpha_entity_not_found"


def test_durable_event_stream_replays_backlog_then_closes_terminal() -> None:
    class Repository:
        def __init__(self) -> None:
            self.calls = 0

        def list_events(self, run_id: str, *, after_event_id: int, limit: int):
            assert run_id == "macb_run"
            assert limit == 500
            self.calls += 1
            if self.calls == 1:
                assert after_event_id == 4
                return [{"event_id": 5, "run_id": run_id, "event_type": "completed"}]
            assert after_event_id == 5
            return []

        def get_run(self, run_id: str):
            return {"id": run_id, "status": "succeeded"}

    output = list(
        _durable_event_stream(
            repository=Repository(),
            run_id="macb_run",
            after_event_id=4,
            poll_interval_seconds=0,
            heartbeat_seconds=999,
        )
    )

    assert "id: 5" in output[0]
    assert "event: durable_event" in output[0]
    assert "event: stream_end" in output[1]
    assert '"last_event_id":5' in output[1]


def test_durable_event_stream_reports_repository_failure_without_empty_success() -> None:
    class Repository:
        def list_events(self, _run_id: str, *, after_event_id: int, limit: int):
            raise RuntimeError(f"database unavailable at cursor={after_event_id} limit={limit}")

    output = list(
        _durable_event_stream(
            repository=Repository(),
            run_id="macb_run",
            after_event_id=7,
            poll_interval_seconds=0,
        )
    )

    assert len(output) == 1
    assert "event: stream_error" in output[0]
    assert "multi_alpha_event_stream_failed" in output[0]
    assert "database unavailable" in output[0]


def test_durable_event_stream_rejects_invalid_last_event_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(multi_alpha_router, "MultiAlphaDurableRepository", lambda: None)

    with pytest.raises(HTTPException) as caught:
        stream_multi_alpha_durable_events("macb_run", after_event_id=0, last_event_id="not-an-int")

    assert caught.value.status_code == 400
    assert caught.value.detail["reason_code"] == "multi_alpha_invalid_event_cursor"
