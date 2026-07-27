from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.service import ResponseBoundHistoricalRangeDispatcher


class _Background:
    def __init__(self) -> None:
        self.task = None

    def add_task(self, func, *args, **kwargs):
        self.task = (func, args, kwargs)


def test_dispatcher_captures_only_json_round_tripped_identity() -> None:
    background = _Background()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: SimpleNamespace())
    dispatcher.schedule(
        background,
        command="CATALOG_EXECUTE",
        payload={"batch_id": "ahrb_1", "operation_id": "ahrop_1", "range_run_ids": ("run_1",)},
    )
    assert background.task is not None
    _, args, kwargs = background.task
    assert kwargs == {}
    assert args[0] == "CATALOG_EXECUTE"
    assert args[1] == {"batch_id": "ahrb_1", "operation_id": "ahrop_1", "range_run_ids": ["run_1"]}


def test_dispatcher_rejects_request_scoped_non_json_objects() -> None:
    background = _Background()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: SimpleNamespace())
    try:
        dispatcher.schedule(background, command="CATALOG_EXECUTE", payload={"connection": object()})
    except TypeError:
        pass
    else:
        raise AssertionError("request-scoped object must not be captured")
    assert background.task is None


class _FailureQuery:
    def get_operation_internal(self, operation_id: str):
        return {"operation_id": operation_id, "status": "QUEUED", "row_version": 1, "attempt_no": 0}


class _FailureRepository:
    def __init__(self) -> None:
        self.transitions = []

    def transition_operation(self, **kwargs):
        self.transitions.append(kwargs)
        return kwargs


def _failure_runtime(repository: _FailureRepository):
    return SimpleNamespace(query=_FailureQuery(), repository=repository)


def test_runtime_reconstruction_failure_records_structured_retryable_receipt() -> None:
    repository = _FailureRepository()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(
        runtime_factory=lambda: (_ for _ in ()).throw(RuntimeError("runtime unavailable")),
        failure_runtime_factory=lambda: _failure_runtime(repository),
    )
    with pytest.raises(RuntimeError, match="runtime unavailable"):
        dispatcher._run("CATALOG_EXECUTE", {"operation_id": "ahrop_1", "batch_id": "ahrb_1"})
    receipt = repository.transitions[0]["error_json"]
    assert repository.transitions[0]["target_status"].value == "RETRYABLE_FAILED"
    assert receipt["stage"] == "RUNTIME_RECONSTRUCTION"
    assert receipt["schema_version"] == "advisory_historical_range_background_failure_receipt_v1"


def test_request_reconstruction_failure_records_structured_retryable_receipt() -> None:
    repository = _FailureRepository()
    runtime = SimpleNamespace(
        query=_FailureQuery(),
        repository=repository,
        outcome_requests=SimpleNamespace(build=lambda *_: (_ for _ in ()).throw(ValueError("bad request"))),
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(
        runtime_factory=lambda: runtime,
        failure_runtime_factory=lambda: _failure_runtime(repository),
    )
    with pytest.raises(Exception):
        dispatcher._run(
            "REFRESH_OUTCOMES",
            {"operation_id": "ahrop_2", "batch_id": "ahrb_1", "command_payload": {}},
        )
    assert repository.transitions[0]["error_json"]["stage"] == "REQUEST_RECONSTRUCTION"


def test_preclaim_failure_records_structured_retryable_receipt() -> None:
    repository = _FailureRepository()
    repository.claim_catalog_operation = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[attr-defined]
        RuntimeError("claim failed")
    )
    runtime = SimpleNamespace(query=_FailureQuery(), repository=repository)
    dispatcher = ResponseBoundHistoricalRangeDispatcher(
        runtime_factory=lambda: runtime,
        failure_runtime_factory=lambda: _failure_runtime(repository),
    )
    with pytest.raises(RuntimeError, match="claim failed"):
        dispatcher._run("CATALOG_EXECUTE", {"operation_id": "ahrop_3", "batch_id": "ahrb_1"})
    assert repository.transitions[0]["error_json"]["stage"] == "CLAIM_AND_EXECUTION"
