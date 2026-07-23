from datetime import UTC, date, datetime, timedelta
from types import MethodType, SimpleNamespace

from backend.services.advisory_historical_range.executor import (
    HistoricalRangeBatchExecutionResultV1,
    HistoricalRangeBatchExecutionService,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeBatchStatus,
    HistoricalRangeDayStatus,
    HistoricalRangeExecutionBatchV1,
    HistoricalRangeExecutionOperationV1,
    HistoricalRangeExecutionRunV1,
    HistoricalRangeExecutionOperationReceiptV1,
    HistoricalRangeOperationCancelledDayResultV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeProgramStatus,
    HistoricalRangeRunExecutionReceiptV1,
)
from backend.tests.advisory_historical_range.conftest import artifact_ref, digest


class _OperationRepository:
    def __init__(self) -> None:
        self.batch = HistoricalRangeExecutionBatchV1(
            batch_id="batch-operation",
            status=HistoricalRangeBatchStatus.WAITING_INPUT,
            row_version=4,
            resolved_request_hash=digest("request"),
            request_ref=artifact_ref(HistoricalRangeArtifactKind.REQUEST, "request"),
            date_plan_ref=artifact_ref(HistoricalRangeArtifactKind.DATE_PLAN, "date-plan"),
            artifact_root_identity_hash=digest("root"),
        )
        self.run = HistoricalRangeExecutionRunV1(
            batch_id=self.batch.batch_id,
            range_run_id="range-operation",
            research_program_id="program-operation",
            status=HistoricalRangeProgramStatus.RETRYABLE_FAILED,
            row_version=3,
            materialized_day_count=0,
            day_plan_cursor_ordinal=0,
        )
        self.operation: HistoricalRangeExecutionOperationV1 | None = None
        self.cancellation_contexts = ()
        self.cancelled_day_results = ()

    def get_or_create_operation(self, request):
        if self.operation is None:
            self.operation = HistoricalRangeExecutionOperationV1(
                operation_id=request.operation_id,
                batch_id=request.batch_id,
                operation_type=request.operation_type.value,
                operation_idempotency_key=request.operation_idempotency_key,
                idempotency_payload_hash=request.request_payload_sha256,
                resolved_request_hash=self.batch.resolved_request_hash,
                expected_row_version=request.expected_row_version,
                status=HistoricalRangeOperationStatus.QUEUED,
                row_version=1,
                attempt_no=0,
            )
            return {}, False
        assert self.operation.idempotency_payload_hash == request.request_payload_sha256
        return {}, True

    def load_execution_operation(self, **_kwargs):
        return self.operation

    def claim_execution_operation(self, **kwargs):
        self.operation = self.operation.model_copy(
            update={
                "status": HistoricalRangeOperationStatus.RUNNING,
                "row_version": self.operation.row_version + 1,
                "attempt_no": self.operation.attempt_no + 1,
                "worker_id": kwargs["worker_id"],
                "lease_token": kwargs["lease_token"],
                "lease_expires_at": datetime.now(UTC) + timedelta(hours=1),
                "fencing_token": int(self.operation.fencing_token or 0) + 1,
            }
        )
        return self.operation

    def heartbeat_execution_operation(self, **_kwargs):
        return self.operation

    def load_execution_batch(self, **_kwargs):
        return self.batch

    def list_execution_runs(self, **_kwargs):
        return (self.run,)

    def transition_batch(self, **kwargs):
        self.batch = self.batch.model_copy(
            update={"status": kwargs["target_status"], "row_version": self.batch.row_version + 1}
        )
        return {"status": self.batch.status.value, "row_version": self.batch.row_version}

    def transition_run(self, **kwargs):
        self.run = self.run.model_copy(
            update={"status": kwargs["target_status"], "row_version": self.run.row_version + 1}
        )
        return {"status": self.run.status.value, "row_version": self.run.row_version}

    def load_run_finalization_facts(self, **_kwargs):
        return SimpleNamespace(blocking_status=HistoricalRangeDayStatus.RETRYABLE_FAILED)

    def list_operation_attempt_receipt_refs(self, **_kwargs):
        return ()

    def finish_execution_operation(self, **kwargs):
        self.operation = self.operation.model_copy(
            update={
                "status": HistoricalRangeOperationStatus.COMPLETED,
                "row_version": self.operation.row_version + 1,
                "worker_id": None,
                "lease_token": None,
                "lease_expires_at": None,
                "fencing_token": None,
                "result_ref": kwargs["receipt_ref"],
                "result_status": kwargs["receipt"].result_status.value,
            }
        )
        return self.operation

    def load_cancellation_day_contexts(self, **_kwargs):
        return self.cancellation_contexts

    def load_cancelled_day_results(self, **_kwargs):
        return self.cancelled_day_results

    def cancel_execution_batch(self, **kwargs):
        attempts = kwargs["attempts"]
        self.cancelled_day_results = tuple(
            HistoricalRangeOperationCancelledDayResultV1(
                range_run_id=context.range_run_id,
                research_program_id=context.research_program_id,
                day_run_id=context.day_run_id,
                ordinal=context.ordinal,
                row_version=context.row_version + 1,
                attempt_no=attempts[context.day_run_id].attempt_no,
                fencing_token=attempts[context.day_run_id].fencing_token,
                attempt_receipt_ref=attempts[context.day_run_id].attempt_receipt_ref,
            )
            for context in self.cancellation_contexts
        )
        self.batch = self.batch.model_copy(
            update={"status": HistoricalRangeBatchStatus.CANCELLING, "row_version": self.batch.row_version + 1}
        )
        return (self.run.range_run_id,)


class _OperationDayExecutor:
    def __init__(self) -> None:
        self._repository = _OperationRepository()
        self.payloads: dict[str, dict] = {}
        self.upstreams: dict[str, tuple] = {}

    def publish_range_receipt(self, *, payload, upstream_refs=(), **_kwargs):
        ref = artifact_ref(HistoricalRangeArtifactKind.RANGE_RECEIPT, payload)
        self.payloads[ref.semantic_content_hash] = payload
        self.upstreams[ref.semantic_content_hash] = upstream_refs
        return ref

    def publish_day_attempt_receipt(self, *, payload, upstream_refs=(), **_kwargs):
        ref = artifact_ref(HistoricalRangeArtifactKind.DAY_RECEIPT, payload.model_dump(mode="json"))
        self.payloads[ref.semantic_content_hash] = payload.model_dump(mode="json")
        self.upstreams[ref.semantic_content_hash] = upstream_refs
        return ref

    def load_range_receipt(self, ref):
        return self.payloads[ref.semantic_content_hash]

    def reconcile_run(self, *, range_run_id):
        assert range_run_id == self._repository.run.range_run_id
        final_ref = artifact_ref(HistoricalRangeArtifactKind.RANGE_RECEIPT, f"final-{range_run_id}")
        self._repository.run = self._repository.run.model_copy(
            update={
                "status": HistoricalRangeProgramStatus.CANCELLED,
                "row_version": self._repository.run.row_version + 1,
                "final_receipt_ref": final_ref,
                "final_receipt_hash": final_ref.semantic_content_hash,
            }
        )


def test_resume_operation_is_typed_and_exactly_idempotent() -> None:
    day_executor = _OperationDayExecutor()
    service = HistoricalRangeBatchExecutionService(day_executor=day_executor)

    def _execute(_self, **_kwargs):
        final_ref = artifact_ref(HistoricalRangeArtifactKind.RANGE_RECEIPT, "completed-run")
        repository = day_executor._repository
        repository.run = repository.run.model_copy(
            update={
                "status": HistoricalRangeProgramStatus.COMPLETED,
                "row_version": repository.run.row_version + 1,
                "final_receipt_ref": final_ref,
                "final_receipt_hash": final_ref.semantic_content_hash,
            }
        )
        repository.batch = repository.batch.model_copy(
            update={"status": HistoricalRangeBatchStatus.COMPLETED, "row_version": repository.batch.row_version + 1}
        )
        return HistoricalRangeBatchExecutionResultV1(
            batch_id=repository.batch.batch_id,
            executed_day_count=1,
            successful_day_count=1,
            waiting_day_count=0,
            retryable_day_count=0,
            failed_day_count=0,
            blocking_day_run_ids=(),
        )

    service.execute_until_blocked = MethodType(_execute, service)
    first = service.resume_until_blocked(
        batch_id="batch-operation",
        worker_id="worker",
        operation_idempotency_key="resume-key",
        expected_batch_row_version=4,
    )
    retry = service.resume_until_blocked(
        batch_id="batch-operation",
        worker_id="worker",
        operation_idempotency_key="resume-key",
        expected_batch_row_version=4,
    )
    assert first == retry
    assert first.successful_day_count == 1
    assert day_executor._repository.operation.status is HistoricalRangeOperationStatus.COMPLETED


def test_cancel_operation_closes_unmaterialized_tail_without_fake_day() -> None:
    day_executor = _OperationDayExecutor()
    service = HistoricalRangeBatchExecutionService(day_executor=day_executor)
    result = service.cancel_batch(
        batch_id="batch-operation",
        worker_id="worker",
        operation_idempotency_key="cancel-key",
        expected_batch_row_version=4,
    )
    assert result.executed_day_count == 0
    assert result.blocking_day_run_ids == ()
    assert day_executor._repository.batch.status is HistoricalRangeBatchStatus.CANCELLED
    assert day_executor._repository.run.status is HistoricalRangeProgramStatus.CANCELLED
    assert day_executor._repository.operation.status is HistoricalRangeOperationStatus.COMPLETED


def test_cancel_operation_closes_materialized_day_row_version_and_attempt_receipt() -> None:
    day_executor = _OperationDayExecutor()
    repository = day_executor._repository
    repository.cancellation_contexts = (
        SimpleNamespace(
            batch_id=repository.batch.batch_id,
            range_run_id=repository.run.range_run_id,
            research_program_id=repository.run.research_program_id,
            day_run_id="day-cancelled",
            ordinal=1,
            row_version=2,
            status=HistoricalRangeDayStatus.WAITING_PREVIOUS_DAY,
            attempt_no=0,
            worker_id=None,
            lease_token=None,
            fencing_token=None,
            resolved_request_hash=repository.batch.resolved_request_hash,
            request_ref=repository.batch.request_ref,
            previous_list_hash=None,
            previous_day_receipt_ref=None,
            decision_trade_date=date(2026, 7, 1),
        ),
    )
    service = HistoricalRangeBatchExecutionService(day_executor=day_executor)

    service.cancel_batch(
        batch_id="batch-operation",
        worker_id="worker",
        operation_idempotency_key="cancel-day-key",
        expected_batch_row_version=4,
    )

    operation = repository.operation
    receipt = HistoricalRangeExecutionOperationReceiptV1.model_validate(
        day_executor.payloads[operation.result_ref.semantic_content_hash]
    )
    assert len(receipt.cancelled_day_results) == 1
    cancelled = receipt.cancelled_day_results[0]
    assert cancelled.day_run_id == "day-cancelled"
    assert cancelled.row_version == 3
    assert cancelled.attempt_receipt_ref in day_executor.upstreams[operation.result_ref.semantic_content_hash]


def test_cancel_operation_resumes_from_durable_cancelling_state() -> None:
    day_executor = _OperationDayExecutor()
    repository = day_executor._repository
    repository.batch = repository.batch.model_copy(
        update={"status": HistoricalRangeBatchStatus.CANCELLING, "row_version": 5}
    )
    service = HistoricalRangeBatchExecutionService(day_executor=day_executor)

    result = service.cancel_batch(
        batch_id="batch-operation",
        worker_id="worker",
        operation_idempotency_key="cancel-resume-key",
        expected_batch_row_version=5,
    )

    assert result.executed_day_count == 0
    assert repository.batch.status is HistoricalRangeBatchStatus.CANCELLED
    assert repository.run.status is HistoricalRangeProgramStatus.CANCELLED
    assert repository.operation.status is HistoricalRangeOperationStatus.COMPLETED


def test_resume_operation_rebuilds_result_from_terminal_run_receipt() -> None:
    day_executor = _OperationDayExecutor()
    repository = day_executor._repository
    day_ref = artifact_ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "completed-day")
    run_receipt = HistoricalRangeRunExecutionReceiptV1(
        range_run_id=repository.run.range_run_id,
        research_program_id=repository.run.research_program_id,
        status="COMPLETED",
        resolved_request_hash=repository.batch.resolved_request_hash,
        ordered_success_day_receipt_refs=(day_ref,),
        first_list_hash=digest("first-list"),
        latest_list_hash=digest("latest-list"),
        successful_day_count=1,
        failed_day_count=0,
        unexecuted_day_count=0,
    )
    run_ref = artifact_ref(HistoricalRangeArtifactKind.RANGE_RECEIPT, run_receipt.model_dump(mode="json"))
    day_executor.payloads[run_ref.semantic_content_hash] = run_receipt.model_dump(mode="json")
    repository.run = repository.run.model_copy(
        update={
            "status": HistoricalRangeProgramStatus.COMPLETED,
            "row_version": 6,
            "final_receipt_ref": run_ref,
            "final_receipt_hash": run_ref.semantic_content_hash,
        }
    )
    repository.batch = repository.batch.model_copy(
        update={"status": HistoricalRangeBatchStatus.COMPLETED, "row_version": 8}
    )
    service = HistoricalRangeBatchExecutionService(day_executor=day_executor)

    result = service.resume_until_blocked(
        batch_id="batch-operation",
        worker_id="worker",
        operation_idempotency_key="resume-terminal-key",
        expected_batch_row_version=8,
    )

    assert result.executed_day_count == 1
    assert result.successful_day_count == 1
    assert repository.operation.status is HistoricalRangeOperationStatus.COMPLETED
