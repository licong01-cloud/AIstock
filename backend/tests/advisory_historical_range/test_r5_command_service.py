from __future__ import annotations

from types import SimpleNamespace

from backend.services.advisory_historical_range.api_models import HistoricalRangeCommandRequest
from backend.services.advisory_historical_range.models import HistoricalRangeOperationStatus
from backend.services.advisory_historical_range.service import (
    HistoricalRangeApplicationService,
    HistoricalRangeRuntime,
)


class _Query:
    def get_batch(self, batch_id: str):
        return {"batch_id": batch_id, "row_version": 7, "request_payload_sha256": "a" * 64}

    def get_operation(self, operation_id: str):
        return {
            "operation_id": operation_id,
            "batch_id": "ahrb_1",
            "status": HistoricalRangeOperationStatus.QUEUED.value,
            "row_version": 1,
            "lease_expired": False,
        }


class _Repository:
    def __init__(self, *, exact_retry: bool = False) -> None:
        self.requests = []
        self.exact_retry = exact_retry

    def get_or_create_operation(self, request):
        self.requests.append(request)
        return ({"operation_id": request.operation_id}, self.exact_retry)


class _Background:
    def __init__(self) -> None:
        self.tasks = []

    def add_task(self, func, *args, **kwargs):
        self.tasks.append((func, args, kwargs))


def _runtime(repository: _Repository) -> HistoricalRangeRuntime:
    return HistoricalRangeRuntime(
        query=_Query(),  # type: ignore[arg-type]
        repository=repository,  # type: ignore[arg-type]
        planning=SimpleNamespace(),
        execution=SimpleNamespace(),
        outcome=SimpleNamespace(),
        bridge=SimpleNamespace(),
        outcome_requests=SimpleNamespace(),
        bridge_requests=SimpleNamespace(),
        options_projector=lambda: {},
    )


def test_resume_persists_exact_operation_before_background_dispatch() -> None:
    repository = _Repository()
    service = HistoricalRangeApplicationService(runtime_factory=lambda: _runtime(repository))
    background = _Background()
    response = service.resume_batch(
        "ahrb_1",
        HistoricalRangeCommandRequest(operation_idempotency_key="resume-key", expected_row_version=7),
        background_tasks=background,
    )
    assert len(repository.requests) == 1
    persisted = repository.requests[0]
    assert persisted.operation_type.value == "RESUME"
    assert persisted.expected_row_version == 7
    assert response["data"]["operation_id"] == persisted.operation_id
    assert response["data"]["links"]["operation"].endswith(persisted.operation_id)
    assert len(background.tasks) == 1


def test_resume_returns_repository_exact_retry_without_duplicate_dispatch_identity() -> None:
    repository = _Repository(exact_retry=True)
    service = HistoricalRangeApplicationService(runtime_factory=lambda: _runtime(repository))
    response = service.resume_batch(
        "ahrb_1",
        HistoricalRangeCommandRequest(operation_idempotency_key="resume-key", expected_row_version=7),
        background_tasks=_Background(),
    )
    assert response["data"]["exact_retry"] is True
    assert len(repository.requests) == 1


def test_planning_resume_enforces_expected_row_version_before_dispatch() -> None:
    repository = _Repository()
    runtime = _runtime(repository)
    runtime.query.get_batch = lambda batch_id: {  # type: ignore[method-assign]
        "batch_id": batch_id,
        "row_version": 8,
        "request_payload_sha256": None,
        "catalog_operation_id": "ahrop_catalog",
    }
    service = HistoricalRangeApplicationService(runtime_factory=lambda: runtime)
    try:
        service.resume_batch(
            "ahrb_1",
            HistoricalRangeCommandRequest(operation_idempotency_key="resume-key", expected_row_version=7),
            background_tasks=_Background(),
        )
    except Exception as exc:
        assert getattr(exc, "reason_code", None) == "ADVISORY_HR_OPERATION_BATCH_VERSION_CONFLICT"
        assert getattr(exc, "http_status", None) == 409
    else:
        raise AssertionError("stale PLANNING resume must fail")
