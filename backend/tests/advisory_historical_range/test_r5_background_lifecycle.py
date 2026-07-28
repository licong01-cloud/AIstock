from __future__ import annotations

from types import SimpleNamespace
from datetime import UTC, datetime, timedelta

import pytest

from backend.services.advisory_historical_range.service import ResponseBoundHistoricalRangeDispatcher
from backend.services.advisory_historical_range.models import HistoricalRangeBackgroundDispatchFailureV1
from backend.services.advisory_historical_range.models import HistoricalRangeContractError
from backend.services.advisory_historical_range.repository import (
    PostgresHistoricalRangePreclaimFailureRepository,
)


class _Background:
    def __init__(self) -> None:
        self.task = None

    def add_task(self, func, *args, **kwargs):
        self.task = (func, args, kwargs)


def test_catalog_dispatcher_defers_rollover_deadline_until_after_chunk_execution() -> None:
    operation = {
        "operation_id": "ahrop_1",
        "batch_id": "ahrb_1",
        "status": "WAITING_INPUT",
        "row_version": 3,
        "attempt_no": 1,
        "fencing_token": 1,
    }
    query = SimpleNamespace(get_operation_internal=lambda _operation_id: operation)
    repository = SimpleNamespace(
        claim_catalog_operation=lambda **_kwargs: {
            **operation,
            "status": "RUNNING",
            "row_version": 4,
            "attempt_no": 2,
            "fencing_token": 2,
        }
    )
    calls = []

    def execute_claimed_chunk(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(operation={"status": "WAITING_INPUT"}, sealed_batch=None)

    runtime = SimpleNamespace(
        query=query,
        repository=repository,
        planning=SimpleNamespace(execute_claimed_chunk=execute_claimed_chunk),
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: runtime)

    dispatcher._run_catalog(
        runtime=runtime,
        payload={"operation_id": "ahrop_1", "batch_id": "ahrb_1"},
        worker_id="worker-1",
    )

    assert len(calls) == 1
    assert calls[0]["next_lease_duration"] == timedelta(minutes=5)
    assert "next_lease_expires_at" not in calls[0]


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


@pytest.mark.parametrize(
    "payload,missing_identity",
    [
        ({"operation_id": "ahrop_1"}, "batch_id"),
        ({"batch_id": "ahrb_1"}, "operation_id"),
    ],
)
def test_dispatcher_rejects_missing_durable_identity(payload, missing_identity) -> None:
    background = _Background()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: SimpleNamespace())
    with pytest.raises(ValueError, match=missing_identity):
        dispatcher.schedule(background, command="CATALOG_EXECUTE", payload=payload)
    assert background.task is None


class _FailureRecorder:
    def __init__(self) -> None:
        self.failures = []

    def record_retryable_failure(self, failure):
        self.failures.append(failure)
        return failure.model_dump(mode="json")


class _FailureQuery:
    def get_operation_internal(self, operation_id: str):
        return {
            "operation_id": operation_id,
            "batch_id": "ahrb_1",
            "status": "QUEUED",
            "row_version": 1,
            "attempt_no": 0,
        }


def test_runtime_reconstruction_failure_records_structured_retryable_record() -> None:
    recorder = _FailureRecorder()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(
        runtime_factory=lambda: (_ for _ in ()).throw(RuntimeError("runtime unavailable")),
        failure_recorder_factory=lambda: recorder,
    )
    with pytest.raises(RuntimeError, match="runtime unavailable"):
        dispatcher._run("CATALOG_EXECUTE", {"operation_id": "ahrop_1", "batch_id": "ahrb_1"})
    failure = recorder.failures[0]
    assert failure.stage == "RUNTIME_RECONSTRUCTION"
    assert failure.schema_version == "advisory_historical_range_background_dispatch_failure_v1"
    assert failure.retryable is True


def test_request_reconstruction_failure_records_structured_retryable_record() -> None:
    recorder = _FailureRecorder()
    runtime = SimpleNamespace(
        query=_FailureQuery(),
        outcome_requests=SimpleNamespace(build=lambda *_: (_ for _ in ()).throw(ValueError("bad request"))),
    )
    dispatcher = ResponseBoundHistoricalRangeDispatcher(
        runtime_factory=lambda: runtime,
        failure_recorder_factory=lambda: recorder,
    )
    with pytest.raises(Exception):
        dispatcher._run(
            "REFRESH_OUTCOMES",
            {"operation_id": "ahrop_2", "batch_id": "ahrb_1", "command_payload": {}},
        )
    assert recorder.failures[0].stage == "REQUEST_RECONSTRUCTION"


def test_preclaim_failure_records_structured_retryable_record() -> None:
    recorder = _FailureRecorder()
    repository = SimpleNamespace()
    repository.claim_catalog_operation = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("claim failed")
    )
    runtime = SimpleNamespace(query=_FailureQuery(), repository=repository)
    dispatcher = ResponseBoundHistoricalRangeDispatcher(
        runtime_factory=lambda: runtime,
        failure_recorder_factory=lambda: recorder,
    )
    with pytest.raises(RuntimeError, match="claim failed"):
        dispatcher._run("CATALOG_EXECUTE", {"operation_id": "ahrop_3", "batch_id": "ahrb_1"})
    assert recorder.failures[0].stage == "CLAIM_AND_EXECUTION"


class _DbCursor:
    def __init__(self, *, update_succeeds: bool = True, select_row=None) -> None:
        self.statements: list[str] = []
        self._row = None
        self._update_succeeds = update_succeeds
        self._select_row = select_row

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params):
        self.statements.append(statement)
        if statement.lstrip().startswith("UPDATE"):
            payload = params[0].adapted
            default_row = {
                "operation_id": params[1],
                "batch_id": params[2],
                "status": "RETRYABLE_FAILED",
                "error_json": payload,
            }
            self._row = default_row if self._update_succeeds else None
        elif statement.lstrip().startswith("SELECT"):
            self._row = self._select_row

    def fetchone(self):
        return self._row


class _DbConnection:
    def __init__(self, *, update_succeeds: bool = True, select_row=None) -> None:
        self.cursor_instance = _DbCursor(
            update_succeeds=update_succeeds,
            select_row=select_row,
        )

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return self.cursor_instance


def test_db_only_preclaim_failure_record_does_not_require_artifact_store() -> None:
    connection = _DbConnection()
    repository = PostgresHistoricalRangePreclaimFailureRepository(
        conn_factory=lambda: connection
    )
    failure = HistoricalRangeBackgroundDispatchFailureV1(
        operation_id="ahrop_1",
        batch_id="ahrb_1",
        command="CATALOG_EXECUTE",
        stage="RUNTIME_RECONSTRUCTION",
        reason_code="ADVISORY_HR_BACKGROUND_DISPATCH_FAILED",
        error_type="RuntimeError",
        recorded_at=datetime.now(UTC),
    )
    row = repository.record_retryable_failure(failure)
    assert row["status"] == "RETRYABLE_FAILED"
    assert row["error_json"]["schema_version"] == failure.schema_version
    assert "artifact" not in " ".join(connection.cursor_instance.statements).lower()


def test_db_only_preclaim_failure_does_not_overwrite_an_already_claimed_operation() -> None:
    current = {
        "operation_id": "ahrop_1",
        "batch_id": "ahrb_1",
        "status": "RUNNING",
        "row_version": 2,
    }
    connection = _DbConnection(update_succeeds=False, select_row=current)
    repository = PostgresHistoricalRangePreclaimFailureRepository(
        conn_factory=lambda: connection
    )
    failure = HistoricalRangeBackgroundDispatchFailureV1(
        operation_id="ahrop_1",
        batch_id="ahrb_1",
        command="CATALOG_EXECUTE",
        stage="CLAIM_AND_EXECUTION",
        reason_code="ADVISORY_HR_BACKGROUND_DISPATCH_FAILED",
        error_type="RuntimeError",
        recorded_at=datetime.now(UTC),
    )
    assert repository.record_retryable_failure(failure) == current
    assert len(connection.cursor_instance.statements) == 2


def test_db_only_preclaim_failure_rejects_cross_batch_identity() -> None:
    connection = _DbConnection(
        update_succeeds=False,
        select_row={
            "operation_id": "ahrop_1",
            "batch_id": "ahrb_other",
            "status": "RUNNING",
            "row_version": 2,
        },
    )
    repository = PostgresHistoricalRangePreclaimFailureRepository(
        conn_factory=lambda: connection
    )
    failure = HistoricalRangeBackgroundDispatchFailureV1(
        operation_id="ahrop_1",
        batch_id="ahrb_1",
        command="CATALOG_EXECUTE",
        stage="CLAIM_AND_EXECUTION",
        reason_code="ADVISORY_HR_BACKGROUND_DISPATCH_FAILED",
        error_type="RuntimeError",
        recorded_at=datetime.now(UTC),
    )
    with pytest.raises(HistoricalRangeContractError, match="different batch"):
        repository.record_retryable_failure(failure)


def test_db_only_preclaim_failure_rejects_an_unrecorded_queued_state() -> None:
    connection = _DbConnection(
        update_succeeds=False,
        select_row={
            "operation_id": "ahrop_1",
            "batch_id": "ahrb_1",
            "status": "QUEUED",
            "row_version": 1,
        },
    )
    repository = PostgresHistoricalRangePreclaimFailureRepository(
        conn_factory=lambda: connection
    )
    failure = HistoricalRangeBackgroundDispatchFailureV1(
        operation_id="ahrop_1",
        batch_id="ahrb_1",
        command="CATALOG_EXECUTE",
        stage="RUNTIME_RECONSTRUCTION",
        reason_code="ADVISORY_HR_BACKGROUND_DISPATCH_FAILED",
        error_type="RuntimeError",
        recorded_at=datetime.now(UTC),
    )
    with pytest.raises(HistoricalRangeContractError, match="remained queued"):
        repository.record_retryable_failure(failure)
