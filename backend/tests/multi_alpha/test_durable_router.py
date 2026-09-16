"""Focused HTTP contracts for the durable multi-alpha router."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from backend.routers import multi_alpha as multi_alpha_router
from backend.routers.multi_alpha import (
    CombineBacktestRunRequest,
    DurableRecoveryExecuteRequest,
    _durable_event_stream,
    execute_multi_alpha_durable_child_recovery,
    list_multi_alpha_durable_events,
)


@pytest.mark.parametrize(
    ("selection", "valid"),
    [
        ({"include_baseline": True, "include_loo": False}, True),
        ({"include_baseline": True, "include_loo": "false"}, False),
    ],
)
def test_combine_request_prediction_selection_contract(
    selection: dict, valid: bool
) -> None:
    payload = {
        "roster": [
            {"leg_id": "leg_a", "seed_run_ids": ["qe_a"]},
            {"leg_id": "leg_b", "seed_run_ids": ["qe_b"]},
        ],
        "oos_start": "2024-07-01",
        "oos_end": "2026-06-29",
        "prediction_task_selection": selection,
    }
    if not valid:
        with pytest.raises(ValidationError):
            CombineBacktestRunRequest.model_validate(payload)
        return
    request = CombineBacktestRunRequest.model_validate(payload)
    assert request.prediction_task_selection == selection


def test_recovery_execute_fails_closed_on_stale_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Repository:
        def get_child(self, child_id: str):
            return {"child_id": child_id, "run_id": "macb_source"}

    class ControlService:
        def __init__(self) -> None:
            self.repository = Repository()

        def submit(self, **_kwargs):
            raise AssertionError("stale preview must fail before submission")

    class RecoveryService:
        def __init__(self, _repository) -> None:
            pass

        def preview(self, **_kwargs):
            return SimpleNamespace(
                command_id="macmd_" + "a" * 64,
                scope_hash="b" * 64,
                scope={"retry_mode": "backtest_only"},
            )

    monkeypatch.setattr(
        multi_alpha_router, "DurableMultiAlphaControlService", ControlService
    )
    monkeypatch.setattr(
        multi_alpha_router, "DurableRecoveryService", RecoveryService
    )
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


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed", "cancelled"])
def test_event_stream_replays_terminal_event_once(terminal_status: str) -> None:
    class Repository:
        calls = 0

        def list_events(self, run_id: str, *, after_event_id: int, limit: int):
            assert run_id == "macb_run"
            assert limit == 500
            self.calls += 1
            if self.calls == 1:
                assert after_event_id == 4
                return [{"event_id": 5, "event_type": terminal_status}]
            return []

        def get_run(self, run_id: str):
            return {"id": run_id, "status": terminal_status}

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
    assert "event: stream_end" in output[1]
    assert '"last_event_id":5' in output[1]


def test_event_page_missing_run_is_structured_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Repository:
        def get_run(self, _run_id: str):
            return None

    monkeypatch.setattr(multi_alpha_router, "MultiAlphaDurableRepository", Repository)
    with pytest.raises(HTTPException) as caught:
        list_multi_alpha_durable_events("missing", after_event_id=0, limit=100)
    assert caught.value.status_code == 404
    assert caught.value.detail["reason_code"] == "multi_alpha_entity_not_found"


def test_event_stream_reports_repository_failure() -> None:
    class Repository:
        def list_events(self, _run_id: str, *, after_event_id: int, limit: int):
            raise RuntimeError(f"database unavailable at {after_event_id}/{limit}")

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
