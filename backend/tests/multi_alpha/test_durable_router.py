from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from backend.routers.multi_alpha import (
    DurableControlRequest,
    DurableRecoveryExecuteRequest,
    _durable_control_http_error,
    execute_multi_alpha_durable_child_recovery,
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
