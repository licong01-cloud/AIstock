"""R5 HTTP facade and response-bound durable command dispatcher."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

from .api_models import (
    DISPATCHABLE_OPERATION_STATUSES,
    HistoricalRangeBuildBridgeRequest,
    HistoricalRangeCommandRequest,
    HistoricalRangeCreateRequest,
    HistoricalRangeRefreshOutcomesRequest,
)
from .canonical import canonical_json_sha256
from .dataset_bridge import HistoricalRangeDatasetBridgeApplicationService
from .executor import HistoricalRangeBatchExecutionService
from .artifact_store import HistoricalRangeArtifactStore
from .models import (
    OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION,
    HistoricalRangeArtifactKind,
    ExistingProgramSpecV1,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeBackgroundDispatchFailureV1,
    HistoricalRangeDatasetBridgeRequestV1,
    HistoricalRangeDatasetBridgeReceiptV1,
    HistoricalRangeOperationAttemptV1,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOperationType,
    HistoricalRangeOutcomeRefreshRequestV1,
    HistoricalRangeOutcomeRefreshReceiptV1,
    HistoricalRangeResearchBatchRequestV1,
    ResearchProgramSpecV1,
    derive_prefixed_id,
)
from .outcome_service import HistoricalRangeOutcomeApplicationService
from .planning_service import HistoricalRangePlanningService
from .query_repository import PostgresHistoricalRangeQueryRepository
from .repository import PostgresHistoricalRangeRepository
from .runtime_factories import HistoricalRangeOutcomeCommandPlan

LOGGER = logging.getLogger(__name__)


class BackgroundTaskRegistrar(Protocol):
    def add_task(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None: ...


class HistoricalRangePreclaimFailureRecorder(Protocol):
    def record_retryable_failure(
        self, failure: HistoricalRangeBackgroundDispatchFailureV1
    ) -> Mapping[str, Any]: ...


class HistoricalRangeServiceError(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        http_status: int = 422,
        retryable: bool = False,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.http_status = http_status
        self.retryable = retryable
        self.context = dict(context or {})


class HistoricalRangeOutcomeRequestFactory(Protocol):
    def build(self, batch_id: str, request: HistoricalRangeRefreshOutcomesRequest) -> HistoricalRangeOutcomeCommandPlan: ...


class HistoricalRangeBridgeRequestFactory(Protocol):
    def build(self, batch_id: str, request: HistoricalRangeBuildBridgeRequest) -> HistoricalRangeDatasetBridgeRequestV1: ...


@dataclass(frozen=True)
class HistoricalRangeRuntime:
    query: PostgresHistoricalRangeQueryRepository
    repository: PostgresHistoricalRangeRepository
    planning: HistoricalRangePlanningService
    execution: HistoricalRangeBatchExecutionService
    outcome: HistoricalRangeOutcomeApplicationService
    bridge: HistoricalRangeDatasetBridgeApplicationService
    outcome_requests: HistoricalRangeOutcomeRequestFactory
    bridge_requests: HistoricalRangeBridgeRequestFactory
    options_projector: Callable[[], dict[str, Any]]
    artifact_store: HistoricalRangeArtifactStore | None = None
    outcome_service_factory: (
        Callable[[HistoricalRangeOutcomeRefreshRequestV1], HistoricalRangeOutcomeApplicationService]
        | None
    ) = None


RuntimeFactory = Callable[[], HistoricalRangeRuntime]
FailureRecorderFactory = Callable[[], HistoricalRangePreclaimFailureRecorder]


class _RuntimePreclaimFailureRecorder:
    """Compatibility adapter for explicitly composed runtimes used outside HTTP composition."""

    def __init__(self, runtime_factory: RuntimeFactory) -> None:
        self._runtime_factory = runtime_factory

    def record_retryable_failure(
        self, failure: HistoricalRangeBackgroundDispatchFailureV1
    ) -> Mapping[str, Any]:
        runtime = self._runtime_factory()
        operation = runtime.query.get_operation_internal(failure.operation_id)
        if operation.get("status") != HistoricalRangeOperationStatus.QUEUED.value:
            return operation
        return runtime.repository.transition_operation(
            operation_id=failure.operation_id,
            expected_row_version=int(operation["row_version"]),
            target_status=HistoricalRangeOperationStatus.RETRYABLE_FAILED,
            attempt_no=int(operation.get("attempt_no") or 0),
            error_json=failure.model_dump(mode="json"),
        )


class ResponseBoundHistoricalRangeDispatcher:
    """Register only immutable identities; reconstruct all resources in the worker."""

    def __init__(
        self,
        *,
        runtime_factory: RuntimeFactory,
        failure_recorder_factory: FailureRecorderFactory | None = None,
    ) -> None:
        if runtime_factory is None:
            raise ValueError("historical-range dispatcher requires runtime_factory")
        self._runtime_factory = runtime_factory
        self._failure_recorder_factory = failure_recorder_factory or (
            lambda: _RuntimePreclaimFailureRecorder(runtime_factory)
        )

    def schedule(
        self,
        background_tasks: BackgroundTaskRegistrar,
        *,
        command: str,
        payload: Mapping[str, Any],
    ) -> None:
        frozen = _immutable_payload(payload)
        if not command.strip():
            raise ValueError("historical-range background command is required")
        for identity in ("batch_id", "operation_id"):
            if not str(frozen.get(identity) or "").strip():
                raise ValueError(f"historical-range background {identity} is required")
        background_tasks.add_task(self._run, command, frozen)

    def _run(self, command: str, payload: Mapping[str, Any]) -> None:
        started = datetime.now(UTC)
        operation_id = str(payload.get("operation_id") or "")
        batch_id = str(payload.get("batch_id") or "")
        stage = "RUNTIME_RECONSTRUCTION"
        try:
            runtime = self._runtime_factory()
            worker_id = f"r5-{command.lower()}-{uuid4().hex[:16]}"
            stage = "REQUEST_RECONSTRUCTION"
            if command == "CATALOG_EXECUTE":
                stage = "CLAIM_AND_EXECUTION"
                self._run_catalog(runtime=runtime, payload=payload, worker_id=worker_id)
            elif command == "EXECUTION_RESUME":
                stage = "CLAIM_AND_EXECUTION"
                runtime.execution.resume_until_blocked(
                    batch_id=batch_id,
                    worker_id=worker_id,
                    operation_idempotency_key=str(payload["operation_idempotency_key"]),
                    expected_batch_row_version=int(payload["expected_row_version"]),
                )
            elif command == "CANCEL":
                stage = "CLAIM_AND_EXECUTION"
                runtime.execution.cancel_batch(
                    batch_id=batch_id,
                    worker_id=worker_id,
                    operation_idempotency_key=str(payload["operation_idempotency_key"]),
                    expected_batch_row_version=int(payload["expected_row_version"]),
                )
            elif command == "REFRESH_OUTCOMES":
                plan = runtime.outcome_requests.build(
                    batch_id,
                    HistoricalRangeRefreshOutcomesRequest.model_validate(payload["command_payload"]),
                )
                stage = "CLAIM_AND_EXECUTION"
                self._run_outcome_plan(
                    runtime=runtime,
                    plan=plan,
                    operation_id=operation_id,
                    batch_id=batch_id,
                    worker_id=worker_id,
                )
            elif command == "BUILD_DATASET_BRIDGE":
                request = runtime.bridge_requests.build(
                    batch_id,
                    HistoricalRangeBuildBridgeRequest.model_validate(payload["command_payload"]),
                )
                stage = "CLAIM_AND_EXECUTION"
                runtime.bridge.build_until_stable_boundary(
                    request=request,
                    resolved_request_hash=runtime.query.resolved_request_hash(batch_id),
                    worker_id=worker_id,
                )
            else:
                raise ValueError(f"unsupported historical-range background command: {command}")
            LOGGER.info(
                "historical-range command finished command=%s operation_id=%s batch_id=%s duration_ms=%s",
                command,
                operation_id,
                batch_id,
                int((datetime.now(UTC) - started).total_seconds() * 1000),
            )
        except Exception as exc:
            self._record_unclaimed_failure(
                command=command,
                payload=payload,
                stage=stage,
                error=exc,
            )
            LOGGER.exception(
                "historical-range command failed command=%s operation_id=%s batch_id=%s stage=%s",
                command,
                operation_id,
                batch_id,
                stage,
            )
            raise

    def _record_unclaimed_failure(
        self,
        *,
        command: str,
        payload: Mapping[str, Any],
        stage: str,
        error: Exception,
    ) -> None:
        operation_id = str(payload.get("operation_id") or "")
        if not operation_id:
            return
        try:
            failure = HistoricalRangeBackgroundDispatchFailureV1(
                operation_id=operation_id,
                batch_id=str(payload.get("batch_id") or ""),
                command=command,
                stage=stage,
                reason_code=str(
                    getattr(error, "reason_code", None)
                    or "ADVISORY_HR_BACKGROUND_DISPATCH_FAILED"
                ),
                error_type=type(error).__name__,
                retryable=True,
                recorded_at=datetime.now(UTC),
            )
            self._failure_recorder_factory().record_retryable_failure(failure)
        except Exception:
            # A database outage cannot be converted into another database write.
            # The operation remains exact-retryable under its existing identity.
            LOGGER.exception(
                "historical-range durable failure recording unavailable operation_id=%s stage=%s",
                operation_id,
                stage,
            )

    def _run_outcome_plan(
        self,
        *,
        runtime: HistoricalRangeRuntime,
        plan: HistoricalRangeOutcomeCommandPlan,
        operation_id: str,
        batch_id: str,
        worker_id: str,
    ) -> None:
        if runtime.artifact_store is None:
            raise HistoricalRangeServiceError(
                "ADVISORY_HR_CONFIGURATION_UNAVAILABLE",
                "outcome orchestration requires the explicit historical artifact store",
                http_status=503,
            )
        operation = runtime.query.get_operation_internal(operation_id)
        if operation["status"] in {
            HistoricalRangeOperationStatus.COMPLETED.value,
            HistoricalRangeOperationStatus.FAILED.value,
        }:
            return
        if (
            operation["status"] == HistoricalRangeOperationStatus.RUNNING.value
            and not operation.get("lease_expired")
        ):
            return
        claimed = self._claim_outcome_parent(
            runtime=runtime,
            operation=operation,
            plan=plan,
            worker_id=worker_id,
        )
        outcome_refs: dict[str, HistoricalRangeArtifactRefV1] = {}
        summary_refs: dict[str, HistoricalRangeArtifactRefV1] = {}
        receipts = []
        orchestration_error: Exception | None = None
        try:
            for request in plan.requests:
                service = (
                    runtime.outcome_service_factory(request)
                    if runtime.outcome_service_factory is not None
                    else runtime.outcome
                )
                receipt, _ref = service.refresh_until_stable_boundary(
                    request=request,
                    resolved_request_hash=runtime.query.resolved_request_hash(batch_id),
                    worker_id=f"{worker_id}-{request.range_run_ids[0][-12:]}",
                )
                receipts.append(receipt)
                for ref in receipt.outcome_refs:
                    outcome_refs[ref.semantic_content_hash] = ref
                for ref in receipt.summary_refs:
                    summary_refs[ref.semantic_content_hash] = ref
        except Exception as exc:  # noqa: BLE001 - durable parent receipt owns the boundary.
            orchestration_error = exc
            LOGGER.exception(
                "historical-range outcome sub-operation failed operation_id=%s batch_id=%s",
                operation_id,
                batch_id,
            )
        statuses = {receipt.status for receipt in receipts}
        if orchestration_error is not None or "FAILED" in statuses:
            receipt_status = "FAILED"
            target_status = HistoricalRangeOperationStatus.FAILED
        elif "RETRYABLE_FAILED" in statuses:
            receipt_status = "RETRYABLE_FAILED"
            target_status = HistoricalRangeOperationStatus.RETRYABLE_FAILED
        elif "WAITING_INPUT" in statuses:
            receipt_status = "WAITING_INPUT"
            target_status = HistoricalRangeOperationStatus.WAITING_INPUT
        else:
            receipt_status = "COMPLETED"
            target_status = HistoricalRangeOperationStatus.COMPLETED
        reasons = tuple(
            sorted({reason for receipt in receipts for reason in receipt.reason_codes})
        )
        if orchestration_error is not None:
            reasons = tuple(
                sorted(
                    {
                        *reasons,
                        str(
                            getattr(
                                orchestration_error,
                                "reason_code",
                                "ADVISORY_HR_OUTCOME_SUB_OPERATION_FAILED",
                            )
                        ),
                    }
                )
            )
        aggregate = HistoricalRangeOutcomeRefreshReceiptV1(
            operation_id=operation_id,
            request_hash=plan.request_hash,
            status=receipt_status,
            processed_count=len(outcome_refs),
            outcome_refs=tuple(
                sorted(
                    outcome_refs.values(),
                    key=lambda ref: (
                        ref.artifact_kind.value,
                        ref.semantic_content_hash,
                        ref.relative_path,
                    ),
                )
            ),
            summary_refs=tuple(
                sorted(
                    summary_refs.values(),
                    key=lambda ref: (
                        ref.artifact_kind.value,
                        ref.semantic_content_hash,
                        ref.relative_path,
                    ),
                )
            ),
            reason_codes=reasons or (() if receipt_status == "COMPLETED" else ("ADVISORY_HR_SUB_OPERATION_INCOMPLETE",)),
        )
        stored = runtime.artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
            producer_contract_version="advisory_phase1r_r5_outcome_command_v1",
            payload_schema_version=OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION,
            resolved_request_hash=runtime.query.resolved_request_hash(batch_id),
            payload=aggregate.model_dump(mode="json"),
            upstream_refs=tuple((*aggregate.outcome_refs, *aggregate.summary_refs)),
        )
        finished_at = datetime.now(UTC)
        error_json = (
            {
                "reason_codes": list(aggregate.reason_codes),
                "stage": "OUTCOME_SUB_OPERATIONS",
                "error_type": (
                    type(orchestration_error).__name__
                    if orchestration_error is not None
                    else "DomainSubOperationFailure"
                ),
            }
            if target_status is HistoricalRangeOperationStatus.FAILED
            else None
        )
        attempt = HistoricalRangeOperationAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahropa",
                {
                    "operation_id": operation_id,
                    "attempt_no": claimed["attempt_no"],
                    "fencing_token": claimed["fencing_token"],
                },
            ),
            operation_id=operation_id,
            attempt_no=int(claimed["attempt_no"]),
            worker_id=str(claimed["worker_id"]),
            lease_token=str(claimed["lease_token"]),
            fencing_token=int(claimed["fencing_token"]),
            status=target_status.value,
            input_cursor_json=claimed.get("stable_keyset_cursor_json"),
            result_cursor_json=None,
            input_hash=plan.request_hash,
            result_hash=stored.ref.semantic_content_hash,
            attempt_receipt_ref=stored.ref,
            reason_codes=aggregate.reason_codes,
            error_json=error_json,
            started_at=claimed.get("started_at") or finished_at,
            finished_at=finished_at,
        )
        runtime.repository.transition_operation(
            operation_id=operation_id,
            expected_row_version=int(claimed["row_version"]),
            target_status=target_status,
            attempt_no=int(claimed["attempt_no"]),
            fencing_token=int(claimed["fencing_token"]),
            result_status=receipt_status,
            result_ref=stored.ref if target_status in {
                HistoricalRangeOperationStatus.COMPLETED,
                HistoricalRangeOperationStatus.FAILED,
                HistoricalRangeOperationStatus.WAITING_INPUT,
            } else None,
            error_json=error_json,
            attempt=attempt,
        )

    @staticmethod
    def _claim_outcome_parent(
        *,
        runtime: HistoricalRangeRuntime,
        operation: Mapping[str, Any],
        plan: HistoricalRangeOutcomeCommandPlan,
        worker_id: str,
    ) -> dict[str, Any]:
        expired_attempt = None
        if operation["status"] == HistoricalRangeOperationStatus.RUNNING.value:
            if runtime.artifact_store is None:
                raise ValueError("expired outcome operation requires artifact store")
            receipt = HistoricalRangeOutcomeRefreshReceiptV1(
                operation_id=str(operation["operation_id"]),
                request_hash=plan.request_hash,
                status="RETRYABLE_FAILED",
                processed_count=0,
                reason_codes=("ADVISORY_HR_OPERATION_LEASE_EXPIRED",),
            )
            stored = runtime.artifact_store.publish_payload(
                artifact_kind=HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
                producer_contract_version="advisory_phase1r_r5_outcome_command_v1",
                payload_schema_version=OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION,
                resolved_request_hash=runtime.query.resolved_request_hash(str(operation["batch_id"])),
                payload=receipt.model_dump(mode="json"),
            )
            now = datetime.now(UTC)
            expired_attempt = HistoricalRangeOperationAttemptV1(
                attempt_id=derive_prefixed_id(
                    "ahropa",
                    {
                        "operation_id": operation["operation_id"],
                        "attempt_no": operation["attempt_no"],
                        "fencing_token": operation["fencing_token"],
                    },
                ),
                operation_id=str(operation["operation_id"]),
                attempt_no=int(operation["attempt_no"]),
                worker_id=str(operation["worker_id"]),
                lease_token=str(operation["lease_token"]),
                fencing_token=int(operation["fencing_token"]),
                status=HistoricalRangeOperationStatus.RETRYABLE_FAILED.value,
                input_cursor_json=operation.get("stable_keyset_cursor_json"),
                result_cursor_json=operation.get("stable_keyset_cursor_json"),
                input_hash=plan.request_hash,
                result_hash=stored.ref.semantic_content_hash,
                attempt_receipt_ref=stored.ref,
                reason_codes=("ADVISORY_HR_OPERATION_LEASE_EXPIRED",),
                error_json={"reason_code": "ADVISORY_HR_OPERATION_LEASE_EXPIRED", "error_type": "LeaseExpired"},
                started_at=operation.get("started_at") or now,
                finished_at=now,
            )
        return runtime.repository.transition_operation(
            operation_id=str(operation["operation_id"]),
            expected_row_version=int(operation["row_version"]),
            target_status=HistoricalRangeOperationStatus.RUNNING,
            attempt_no=int(operation.get("attempt_no") or 0) + 1,
            worker_id=worker_id,
            lease_token=uuid4().hex,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=30),
            fencing_token=int(operation.get("fencing_token") or 0) + 1,
            started_at=datetime.now(UTC),
            expired_attempt=expired_attempt,
        )

    def _run_catalog(
        self,
        *,
        runtime: HistoricalRangeRuntime,
        payload: Mapping[str, Any],
        worker_id: str,
    ) -> None:
        operation_id = str(payload["operation_id"])
        batch_id = str(payload["batch_id"])
        operation = runtime.query.get_operation_internal(operation_id)
        if operation["status"] == HistoricalRangeOperationStatus.COMPLETED.value:
            runtime.planning.seal_completed_catalog(operation_id=operation_id)
            runtime.execution.execute_until_blocked(batch_id=batch_id, worker_id=worker_id)
            return
        if operation["status"] == HistoricalRangeOperationStatus.RUNNING.value and not operation.get("lease_expired"):
            return
        expired_attempt = self._expired_catalog_attempt(runtime=runtime, operation=operation) if (
            operation["status"] == HistoricalRangeOperationStatus.RUNNING.value
        ) else None
        attempt_no = int(operation.get("attempt_no") or 0) + 1
        claimed = runtime.repository.claim_catalog_operation(
            operation_id=operation_id,
            expected_row_version=int(operation["row_version"]),
            worker_id=worker_id,
            lease_token=derive_prefixed_id(
                "ahrclease", {"operation_id": operation_id, "worker_id": worker_id, "attempt_no": attempt_no}
            ),
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            expired_attempt=expired_attempt,
        )
        while True:
            next_row_version = int(claimed["row_version"])
            next_fencing_token = int(claimed["fencing_token"])
            result = runtime.planning.execute_claimed_chunk(
                operation_id=operation_id,
                expected_row_version=next_row_version,
                expected_fencing_token=next_fencing_token,
                next_worker_id=worker_id,
                next_lease_token=derive_prefixed_id(
                    "ahrclease",
                    {"operation_id": operation_id, "worker_id": worker_id, "row_version": next_row_version + 1},
                ),
                next_lease_duration=timedelta(minutes=5),
            )
            claimed = result.operation
            if result.sealed_batch is not None:
                runtime.execution.execute_until_blocked(batch_id=batch_id, worker_id=worker_id)
                return
            if str(claimed["status"]) != HistoricalRangeOperationStatus.RUNNING.value:
                return

    @staticmethod
    def _expired_catalog_attempt(
        *, runtime: HistoricalRangeRuntime, operation: Mapping[str, Any]
    ) -> HistoricalRangeOperationAttemptV1:
        batch = runtime.query.get_batch(str(operation["batch_id"]))
        ref_payload = operation.get("latest_checkpoint_ref") or batch.get("requirement_plan_ref")
        if not isinstance(ref_payload, dict):
            raise HistoricalRangeServiceError(
                "ADVISORY_HR_OPERATION_LEASE_IDENTITY_MISSING",
                "expired catalog operation has no durable checkpoint",
                retryable=True,
                context={"operation_id": operation["operation_id"]},
            )
        ref = HistoricalRangeArtifactRefV1.model_validate(ref_payload)
        lease_expires_at = datetime.fromisoformat(str(operation["lease_expires_at"]))
        return HistoricalRangeOperationAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahroa",
                {
                    "operation_id": operation["operation_id"],
                    "attempt_no": operation["attempt_no"],
                    "fencing_token": operation["fencing_token"],
                },
            ),
            operation_id=str(operation["operation_id"]),
            attempt_no=int(operation["attempt_no"]),
            worker_id=str(operation["worker_id"]),
            lease_token=str(operation["lease_token"]),
            fencing_token=int(operation["fencing_token"]),
            status=HistoricalRangeOperationStatus.RETRYABLE_FAILED.value,
            input_cursor_json=operation.get("stable_keyset_cursor_json"),
            result_cursor_json=operation.get("stable_keyset_cursor_json"),
            input_hash=str(operation["planning_identity_hash"]),
            result_hash=ref.semantic_content_hash,
            attempt_receipt_ref=ref,
            reason_codes=("ADVISORY_HR_OPERATION_LEASE_EXPIRED",),
            error_json={
                "reason_codes": ["ADVISORY_HR_OPERATION_LEASE_EXPIRED"],
                "stage": "SOURCE_CATALOG",
                "error_type": "TimeoutError",
            },
            started_at=lease_expires_at - timedelta(seconds=1),
            finished_at=datetime.now(UTC),
        )


class HistoricalRangeApplicationService:
    def __init__(
        self,
        *,
        runtime_factory: RuntimeFactory | None = None,
        query_runtime_factory: RuntimeFactory | None = None,
        mutation_runtime_factory: RuntimeFactory | None = None,
        failure_recorder_factory: FailureRecorderFactory | None = None,
    ) -> None:
        query_factory = query_runtime_factory or runtime_factory
        mutation_factory = mutation_runtime_factory or runtime_factory
        if query_factory is None or mutation_factory is None:
            raise ValueError("historical-range application service requires query and mutation runtime factories")
        self._query_runtime_factory = query_factory
        self._mutation_runtime_factory = mutation_factory
        failure_recorder_factory = failure_recorder_factory or (
            lambda: _RuntimePreclaimFailureRecorder(mutation_factory)
        )
        self._dispatcher = ResponseBoundHistoricalRangeDispatcher(
            runtime_factory=mutation_factory,
            failure_recorder_factory=failure_recorder_factory,
        )

    def list_batch_options(self) -> dict[str, Any]:
        return self._query_runtime_factory().options_projector()

    def list_batches(self, **kwargs: Any) -> dict[str, Any]:
        return self._query_runtime_factory().query.list_batches(**kwargs)

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        return self._query_runtime_factory().query.get_batch(batch_id)

    def list_runs(self, batch_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._query_runtime_factory().query.list_runs(batch_id=batch_id, **kwargs)

    def list_operations(self, batch_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._query_runtime_factory().query.list_operations(batch_id=batch_id, **kwargs)

    def get_operation(self, operation_id: str) -> dict[str, Any]:
        runtime = self._query_runtime_factory()
        operation = runtime.query.get_operation(operation_id)
        if (
            operation.get("operation_type") == HistoricalRangeOperationType.BUILD_DATASET_BRIDGE.value
            and operation.get("result_ref") is not None
        ):
            if runtime.artifact_store is None:
                raise HistoricalRangeServiceError(
                    "ADVISORY_HR_CONFIGURATION_UNAVAILABLE",
                    "typed Dataset bridge receipt requires the explicit historical artifact root",
                    http_status=503,
                    retryable=True,
                )
            ref = HistoricalRangeArtifactRefV1.model_validate(operation["result_ref"])
            envelope = runtime.artifact_store.load(ref)
            receipt = HistoricalRangeDatasetBridgeReceiptV1.model_validate(envelope.payload)
            operation["bridge_receipt"] = receipt.model_dump(mode="json")
            operation["snapshot"] = (
                {"snapshot_id": receipt.sealed_snapshot_id, "status": "SEALED"}
                if receipt.sealed_snapshot_id is not None
                else None
            )
        return operation

    def get_run(self, range_run_id: str) -> dict[str, Any]:
        return self._query_runtime_factory().query.get_run(range_run_id)

    def list_days(self, range_run_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._query_runtime_factory().query.list_days(range_run_id=range_run_id, **kwargs)

    def get_day(self, range_run_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._query_runtime_factory().query.get_day(range_run_id=range_run_id, **kwargs)

    def get_list(self, range_run_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._query_runtime_factory().query.get_list(range_run_id=range_run_id, **kwargs)

    def list_outcomes(self, range_run_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._query_runtime_factory().query.list_outcomes(range_run_id=range_run_id, **kwargs)

    def list_summaries(self, range_run_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._query_runtime_factory().query.list_summaries(range_run_id=range_run_id, **kwargs)

    def create_batch(
        self,
        request: HistoricalRangeCreateRequest,
        *,
        idempotency_key: str,
        background_tasks: BackgroundTaskRegistrar,
        requested_by: str = "local-user",
    ) -> dict[str, Any]:
        if not idempotency_key or len(idempotency_key) > 200:
            raise HistoricalRangeServiceError(
                "ADVISORY_HR_IDEMPOTENCY_KEY_INVALID",
                "Idempotency-Key header must contain 1 to 200 characters",
            )
        runtime = self._mutation_runtime_factory()
        domain_request = HistoricalRangeResearchBatchRequestV1(
            client_idempotency_key=idempotency_key,
            requested_by=requested_by,
            program_specs=tuple(_domain_program_spec(item) for item in request.program_specs),
            start_trade_date=request.start_trade_date,
            end_trade_date=request.end_trade_date,
        )
        created = runtime.planning.create(domain_request)
        batch = runtime.query.get_batch(created.batch_id)
        operation = runtime.query.get_operation(created.catalog_operation_id)
        scheduled = _operation_dispatchable(operation)
        if scheduled:
            self._dispatcher.schedule(
                background_tasks,
                command="CATALOG_EXECUTE",
                payload={
                    "batch_id": created.batch_id,
                    "operation_id": created.catalog_operation_id,
                    "request_hash": domain_request.user_request_semantic_hash,
                },
            )
        return _mutation_response(
            batch=batch,
            operation=operation,
            exact_retry=created.idempotent,
            dispatch_state="SCHEDULED" if scheduled else "NOT_SCHEDULED",
            create_operation_id=created.create_operation_id,
        )

    def resume_batch(
        self,
        batch_id: str,
        request: HistoricalRangeCommandRequest,
        *,
        background_tasks: BackgroundTaskRegistrar,
    ) -> dict[str, Any]:
        runtime = self._mutation_runtime_factory()
        batch = runtime.query.get_batch(batch_id)
        if batch.get("request_payload_sha256") is None:
            if int(batch["row_version"]) != request.expected_row_version:
                raise HistoricalRangeServiceError(
                    "ADVISORY_HR_OPERATION_BATCH_VERSION_CONFLICT",
                    "resume operation expected batch row version differs from current state",
                    http_status=409,
                    retryable=True,
                    context={
                        "batch_id": batch_id,
                        "expected": request.expected_row_version,
                        "actual": int(batch["row_version"]),
                    },
                )
            operation_id = str(batch.get("catalog_operation_id") or "")
            if not operation_id:
                raise HistoricalRangeServiceError(
                    "ADVISORY_HR_CATALOG_OPERATION_MISSING",
                    "planning batch has no durable catalog operation",
                    http_status=409,
                    context={"batch_id": batch_id},
                )
            operation = runtime.query.get_operation(operation_id)
            scheduled = _operation_dispatchable(operation)
            if scheduled:
                self._dispatcher.schedule(
                    background_tasks,
                    command="CATALOG_EXECUTE",
                    payload={"batch_id": batch_id, "operation_id": operation_id},
                )
            return _mutation_response(
                batch=batch,
                operation=operation,
                exact_retry=True,
                dispatch_state="SCHEDULED" if scheduled else "NOT_SCHEDULED",
            )
        operation, exact_retry = _persist_execution_operation(
            runtime=runtime,
            batch_id=batch_id,
            request=request,
            operation_type=HistoricalRangeOperationType.RESUME,
        )
        scheduled = _operation_dispatchable(operation)
        if scheduled:
            self._dispatcher.schedule(
                background_tasks,
                command="EXECUTION_RESUME",
                payload={
                    "batch_id": batch_id,
                    "operation_id": operation["operation_id"],
                    "operation_idempotency_key": request.operation_idempotency_key,
                    "expected_row_version": request.expected_row_version,
                },
            )
        return _mutation_response(batch=batch, operation=operation, exact_retry=exact_retry, dispatch_state=("SCHEDULED" if scheduled else "NOT_SCHEDULED"))

    def cancel_batch(
        self,
        batch_id: str,
        request: HistoricalRangeCommandRequest,
        *,
        background_tasks: BackgroundTaskRegistrar,
    ) -> dict[str, Any]:
        runtime = self._mutation_runtime_factory()
        batch = runtime.query.get_batch(batch_id)
        operation, exact_retry = _persist_execution_operation(
            runtime=runtime,
            batch_id=batch_id,
            request=request,
            operation_type=HistoricalRangeOperationType.CANCEL,
        )
        scheduled = _operation_dispatchable(operation)
        if scheduled:
            self._dispatcher.schedule(
                background_tasks,
                command="CANCEL",
                payload={
                    "batch_id": batch_id,
                    "operation_id": operation["operation_id"],
                    "operation_idempotency_key": request.operation_idempotency_key,
                    "expected_row_version": request.expected_row_version,
                },
            )
        return _mutation_response(batch=batch, operation=operation, exact_retry=exact_retry, dispatch_state=("SCHEDULED" if scheduled else "NOT_SCHEDULED"))

    def refresh_outcomes(
        self,
        batch_id: str,
        request: HistoricalRangeRefreshOutcomesRequest,
        *,
        background_tasks: BackgroundTaskRegistrar,
    ) -> dict[str, Any]:
        runtime = self._mutation_runtime_factory()
        batch = runtime.query.get_batch(batch_id)
        command_plan = runtime.outcome_requests.build(batch_id, request)
        operation, exact_retry = _persist_domain_operation(
            runtime=runtime,
            batch_id=batch_id,
            operation_type=HistoricalRangeOperationType.REFRESH_OUTCOMES,
            operation_idempotency_key=request.operation_idempotency_key,
            request_hash=command_plan.request_hash,
            expected_row_version=request.expected_row_version,
        )
        scheduled = _operation_dispatchable(operation)
        if scheduled:
            self._dispatcher.schedule(
                background_tasks,
                command="REFRESH_OUTCOMES",
                payload={
                    "batch_id": batch_id,
                    "operation_id": operation["operation_id"],
                    "command_payload": request.model_dump(mode="json"),
                },
            )
        return _mutation_response(batch=batch, operation=operation, exact_retry=exact_retry, dispatch_state=("SCHEDULED" if scheduled else "NOT_SCHEDULED"))

    def build_dataset_bridge(
        self,
        batch_id: str,
        request: HistoricalRangeBuildBridgeRequest,
        *,
        background_tasks: BackgroundTaskRegistrar,
    ) -> dict[str, Any]:
        runtime = self._mutation_runtime_factory()
        batch = runtime.query.get_batch(batch_id)
        domain_request = runtime.bridge_requests.build(batch_id, request)
        operation, exact_retry = _persist_domain_operation(
            runtime=runtime,
            batch_id=batch_id,
            operation_type=HistoricalRangeOperationType.BUILD_DATASET_BRIDGE,
            operation_idempotency_key=request.operation_idempotency_key,
            request_hash=str(domain_request.request_hash),
            expected_row_version=request.expected_row_version,
        )
        scheduled = _operation_dispatchable(operation)
        if scheduled:
            self._dispatcher.schedule(
                background_tasks,
                command="BUILD_DATASET_BRIDGE",
                payload={
                    "batch_id": batch_id,
                    "operation_id": operation["operation_id"],
                    "command_payload": request.model_dump(mode="json"),
                },
            )
        return _mutation_response(batch=batch, operation=operation, exact_retry=exact_retry, dispatch_state=("SCHEDULED" if scheduled else "NOT_SCHEDULED"))


def _domain_program_spec(value: Any) -> ExistingProgramSpecV1 | ResearchProgramSpecV1:
    payload = value.model_dump(mode="python")
    if payload.get("source_kind") == "EXISTING_PROGRAM":
        return ExistingProgramSpecV1.model_validate(payload)
    return ResearchProgramSpecV1.model_validate(payload)


def _persist_execution_operation(
    *, runtime: HistoricalRangeRuntime, batch_id: str, request: HistoricalRangeCommandRequest,
    operation_type: HistoricalRangeOperationType,
) -> tuple[dict[str, Any], bool]:
    schema = "resume" if operation_type is HistoricalRangeOperationType.RESUME else "cancel"
    payload: dict[str, Any] = {
        "schema_version": f"advisory_historical_range_{schema}_operation_input_v1",
        "batch_id": batch_id,
        "operation_type": operation_type.value,
        "expected_batch_row_version": request.expected_row_version,
    }
    if operation_type is HistoricalRangeOperationType.RESUME:
        payload.update({
            "max_program_concurrency": 2,
            "candidate_prefetch_per_program": 2,
            "day_slice_size": 4,
            "lease_seconds": 3600,
        })
    else:
        payload["lease_seconds"] = 3600
    return _persist_domain_operation(
        runtime=runtime,
        batch_id=batch_id,
        operation_type=operation_type,
        operation_idempotency_key=request.operation_idempotency_key,
        request_hash=canonical_json_sha256(payload),
        expected_row_version=request.expected_row_version,
        executor_identity=True,
    )


def _persist_domain_operation(
    *, runtime: HistoricalRangeRuntime, batch_id: str, operation_type: HistoricalRangeOperationType,
    operation_idempotency_key: str, request_hash: str, expected_row_version: int,
    executor_identity: bool = False,
) -> tuple[dict[str, Any], bool]:
    identity_key = "operation_idempotency_key" if executor_identity else "idempotency_key"
    operation_id = derive_prefixed_id(
        "ahrop",
        {"batch_id": batch_id, "operation_type": operation_type.value, identity_key: operation_idempotency_key},
    )
    operation, exact_retry = runtime.repository.get_or_create_operation(
        HistoricalRangeOperationRequestV1(
            operation_id=operation_id,
            batch_id=batch_id,
            operation_type=operation_type,
            operation_idempotency_key=operation_idempotency_key,
            request_payload_sha256=request_hash,
            expected_row_version=expected_row_version,
        )
    )
    return runtime.query.get_operation(str(operation["operation_id"])), exact_retry


def _operation_dispatchable(operation: Mapping[str, Any]) -> bool:
    status = str(operation.get("status"))
    return status in DISPATCHABLE_OPERATION_STATUSES or (
        status == HistoricalRangeOperationStatus.RUNNING.value and bool(operation.get("lease_expired"))
    )


def _mutation_response(
    *, batch: Mapping[str, Any], operation: Mapping[str, Any], exact_retry: bool,
    dispatch_state: str, create_operation_id: str | None = None,
) -> dict[str, Any]:
    batch_id = str(batch["batch_id"])
    operation_id = str(operation["operation_id"])
    data = {
        "batch": dict(batch),
        "operation": dict(operation),
        "operation_id": operation_id,
        "canonical_batch_id": batch.get("canonical_batch_id"),
        "exact_retry": exact_retry,
        "dispatch_state": dispatch_state,
        "links": {
            "self": f"/api/v1/advisory/historical-range-batches/{batch_id}",
            "runs": f"/api/v1/advisory/historical-range-batches/{batch_id}/runs",
            "operation": f"/api/v1/advisory/historical-range-operations/{operation_id}",
        },
    }
    if create_operation_id is not None:
        data["create_operation_id"] = create_operation_id
        data["create_operation"] = {"operation_id": create_operation_id}
    return {"ok": True, "data": data}


def _immutable_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    # Canonical round-trip rejects non-JSON request/session/connection objects.
    import json

    return json.loads(json.dumps(dict(payload), ensure_ascii=True, separators=(",", ":"), sort_keys=True))
