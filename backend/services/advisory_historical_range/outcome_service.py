"""Durable bounded REFRESH_OUTCOMES application service."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

import psycopg2

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonicalize
from backend.services.advisory_historical_range.models import (
    OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeContractError,
    HistoricalRangeOperationAttemptV1,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOperationType,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeOutcomeRefreshReceiptV1,
    HistoricalRangeOutcomeRefreshRequestV1,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeWorkItemV1,
    REASON_DATABASE_CAPACITY_EXHAUSTED,
    REASON_DATABASE_UNAVAILABLE,
    REASON_OUTCOME_CALCULATION_FAILED,
    REASON_OUTCOME_SOURCE_UNAVAILABLE,
    REASON_REPOSITORY_CONFLICT,
    derive_prefixed_id,
)
from backend.services.advisory_historical_range.outcome_projection import (
    HistoricalRangeOutcomeProjectionBuilder,
    HistoricalRangeProjectionResultV1,
)
from backend.services.advisory_historical_range.outcome_planner import (
    HistoricalRangeOutcomeSliceV1,
)
from backend.services.advisory_historical_range.outcome_source import HistoricalRangeOutcomeSourceError


logger = logging.getLogger(__name__)


class HistoricalRangeOutcomeWorkPlanner(Protocol):
    def plan_slice(
        self,
        *,
        request: HistoricalRangeOutcomeRefreshRequestV1,
        cursor: dict[str, Any] | None,
        limit: int,
    ) -> HistoricalRangeOutcomeSliceV1: ...


class HistoricalRangeOutcomeEvaluator(Protocol):
    def evaluate(self, work_item: HistoricalRangeOutcomeWorkItemV1) -> HistoricalRangeProjectionResultV1: ...


class HistoricalRangeSummaryCoordinator(Protocol):
    def refresh(self, *, range_run_id: str) -> HistoricalRangeArtifactRefV1 | None: ...


class HistoricalRangeOutcomeRepository(Protocol):
    def get_or_create_operation(self, request: HistoricalRangeOperationRequestV1) -> tuple[dict[str, Any], bool]: ...
    def transition_operation(self, **kwargs: Any) -> dict[str, Any]: ...
    def find_outcome_by_input(
        self, *, outcome_logical_id: str, outcome_input_hash: str
    ) -> HistoricalRangeOutcomeFactV1 | None: ...
    def load_latest_outcome(self, *, outcome_logical_id: str) -> HistoricalRangeOutcomeFactV1 | None: ...
    def append_outcome(self, fact: HistoricalRangeOutcomeFactV1) -> bool: ...

    def append_outcomes(self, facts: tuple[HistoricalRangeOutcomeFactV1, ...]) -> tuple[bool, ...]: ...
    def list_operation_attempt_receipt_refs(self, *, operation_id: str) -> tuple[HistoricalRangeArtifactRefV1, ...]: ...


class HistoricalRangeOutcomeApplicationService:
    def __init__(
        self,
        *,
        repository: HistoricalRangeOutcomeRepository,
        artifact_store: HistoricalRangeArtifactStore,
        planner: HistoricalRangeOutcomeWorkPlanner,
        evaluator: HistoricalRangeOutcomeEvaluator,
        summary_coordinator: HistoricalRangeSummaryCoordinator | None = None,
        projection_builder: HistoricalRangeOutcomeProjectionBuilder | None = None,
    ) -> None:
        self._repository = repository
        self._artifact_store = artifact_store
        self._planner = planner
        self._evaluator = evaluator
        self._summary_coordinator = summary_coordinator
        self._projection_builder = projection_builder or HistoricalRangeOutcomeProjectionBuilder()

    def refresh_until_stable_boundary(
        self,
        *,
        request: HistoricalRangeOutcomeRefreshRequestV1,
        resolved_request_hash: str,
        worker_id: str,
    ) -> tuple[HistoricalRangeOutcomeRefreshReceiptV1, HistoricalRangeArtifactRefV1]:
        operation_request = HistoricalRangeOperationRequestV1(
            operation_id=derive_prefixed_id(
                "ahrop",
                {
                    "batch_id": request.batch_id,
                    "operation_type": HistoricalRangeOperationType.REFRESH_OUTCOMES.value,
                    "idempotency_key": request.operation_idempotency_key,
                },
            ),
            batch_id=request.batch_id,
            operation_type=HistoricalRangeOperationType.REFRESH_OUTCOMES,
            operation_idempotency_key=request.operation_idempotency_key,
            request_payload_sha256=str(request.request_hash),
            expected_row_version=request.expected_batch_row_version,
        )
        operation, idempotent = self._repository.get_or_create_operation(operation_request)
        if (
            idempotent
            and operation.get("result_ref") is not None
            and str(operation.get("status"))
            in {
                HistoricalRangeOperationStatus.COMPLETED.value,
                HistoricalRangeOperationStatus.FAILED.value,
            }
        ):
            ref = HistoricalRangeArtifactRefV1.model_validate(operation["result_ref"])
            envelope = self._artifact_store.load(ref)
            receipt = HistoricalRangeOutcomeRefreshReceiptV1.model_validate(envelope.payload)
            if receipt.request_hash != request.request_hash:
                raise ValueError("exact retry receipt differs from refresh request")
            return receipt, ref
        outcome_refs, summary_refs, recovery_cursor = self._load_prior_attempt_outputs(
            operation_id=operation_request.operation_id,
            request=request,
            resolved_request_hash=resolved_request_hash,
        )
        operation = self._claim(
            operation=operation,
            request=request,
            resolved_request_hash=resolved_request_hash,
            worker_id=worker_id,
            recovery_cursor=recovery_cursor,
            outcome_refs=outcome_refs,
            summary_refs=summary_refs,
        )
        attempt_no = int(operation["attempt_no"])
        fencing_token = int(operation["fencing_token"])
        lease_token = str(operation["lease_token"])
        started_at = datetime.now(UTC)
        cursor = operation.get("stable_keyset_cursor_json")
        processed_count = len(outcome_refs)
        touched_runs: set[str] = (
            set(request.range_run_ids)
            if self._summary_coordinator is not None
            else set()
        )
        current_item: HistoricalRangeOutcomeWorkItemV1 | None = None

        def remember_outcome(ref: HistoricalRangeArtifactRefV1) -> None:
            nonlocal processed_count
            identity = ref.semantic_content_hash
            if identity not in outcome_refs:
                outcome_refs[identity] = ref
                processed_count += 1

        try:
            while True:
                planned = self._planner.plan_slice(
                    request=request,
                    cursor=cursor,
                    limit=request.max_items_per_slice,
                )
                pending_outcomes: list[tuple[Any, HistoricalRangeArtifactRefV1]] = []

                def flush_pending_outcomes() -> None:
                    if not pending_outcomes:
                        return
                    self._repository.append_outcomes(
                        tuple(fact for fact, _ref in pending_outcomes)
                    )
                    for _fact, ref in pending_outcomes:
                        remember_outcome(ref)
                    pending_outcomes.clear()

                for item in planned.items:
                    current_item = item
                    touched_runs.add(item.range_run_id)
                    existing = self._repository.find_outcome_by_input(
                        outcome_logical_id=str(item.outcome_logical_id),
                        outcome_input_hash=str(item.outcome_input_hash),
                    )
                    if existing is not None:
                        remember_outcome(existing.outcome_artifact_ref)
                        continue
                    predecessor = self._repository.load_latest_outcome(outcome_logical_id=str(item.outcome_logical_id))
                    try:
                        result = self._evaluator.evaluate(item)
                    except HistoricalRangeOutcomeSourceError:
                        flush_pending_outcomes()
                        raise
                    except HistoricalRangeContractError:
                        flush_pending_outcomes()
                        raise
                    except (
                        psycopg2.OperationalError,
                        psycopg2.InterfaceError,
                        psycopg2.errors.SerializationFailure,
                        psycopg2.errors.DeadlockDetected,
                        psycopg2.errors.LockNotAvailable,
                    ) as error:
                        logger.exception(
                            "historical_range_outcome_database_unavailable_during_evaluation "
                            "operation_id=%s range_run_id=%s subject_type=%s "
                            "subject_id=%s projection=%s horizon_trade_days=%s error_type=%s",
                            operation_request.operation_id,
                            item.range_run_id,
                            item.subject_type.value,
                            item.subject_id,
                            item.projection.value,
                            item.horizon_trade_days,
                            type(error).__name__,
                        )
                        flush_pending_outcomes()
                        raise HistoricalRangeContractError(
                            REASON_DATABASE_UNAVAILABLE,
                            "database became unavailable during outcome evaluation",
                        ) from error
                    except Exception as error:
                        logger.exception(
                            "historical_range_outcome_calculation_failed operation_id=%s range_run_id=%s subject_id=%s",
                            operation_request.operation_id,
                            item.range_run_id,
                            item.subject_id,
                        )
                        result = HistoricalRangeProjectionResultV1(
                            projection_group=item.projection,
                            evaluation_window_type=item.evaluation_window_type,
                            horizon_trade_days=item.horizon_trade_days,
                            maturity_status=HistoricalRangeOutcomeStatus.FAILED,
                            reason_codes=(REASON_OUTCOME_CALCULATION_FAILED, type(error).__name__),
                        )
                    if (
                        predecessor is not None
                        and item.revision_reason.value == "MATURITY_ADVANCE"
                        and result.maturity_status is predecessor.maturity_status
                    ):
                        remember_outcome(predecessor.outcome_artifact_ref)
                        continue
                    version = 1 if predecessor is None else predecessor.outcome_version + 1
                    artifact = self._projection_builder.build_artifact(
                        work_item=item,
                        result=result,
                        outcome_version=version,
                        predecessor=predecessor,
                    )
                    stored = self._artifact_store.publish_payload(
                        artifact_kind=HistoricalRangeArtifactKind.OUTCOME,
                        producer_contract_version=item.outcome_contract_version,
                        payload_schema_version=artifact.schema_version,
                        resolved_request_hash=resolved_request_hash,
                        payload=canonicalize(artifact.model_dump(mode="python")),
                        range_run_id=item.range_run_id,
                        upstream_refs=artifact.direct_upstream_refs,
                    )
                    self._artifact_store.load(stored.ref)
                    fact = self._projection_builder.build_fact(
                        work_item=item,
                        result=result,
                        artifact=artifact,
                        outcome_artifact_ref=stored.ref,
                        outcome_version=version,
                        predecessor=predecessor,
                    )
                    pending_outcomes.append((fact, stored.ref))
                flush_pending_outcomes()
                cursor = planned.next_cursor
                if planned.exhausted:
                    break
                operation = self._repository.transition_operation(
                    operation_id=operation_request.operation_id,
                    expected_row_version=int(operation["row_version"]),
                    target_status=HistoricalRangeOperationStatus.RUNNING,
                    attempt_no=attempt_no,
                    worker_id=worker_id,
                    lease_token=lease_token,
                    lease_expires_at=datetime.now(UTC) + timedelta(seconds=request.lease_seconds),
                    fencing_token=fencing_token,
                    stable_keyset_cursor_json=cursor,
                )
            if self._summary_coordinator is not None:
                for range_run_id in sorted(touched_runs):
                    ref = self._summary_coordinator.refresh(range_run_id=range_run_id)
                    if ref is not None:
                        if ref.artifact_kind is not HistoricalRangeArtifactKind.SUMMARY:
                            raise ValueError("summary coordinator returned a non-SUMMARY ref")
                        summary_refs[ref.semantic_content_hash] = ref
            receipt = HistoricalRangeOutcomeRefreshReceiptV1(
                operation_id=operation_request.operation_id,
                request_hash=str(request.request_hash),
                status="COMPLETED",
                processed_count=processed_count,
                outcome_refs=tuple(outcome_refs[key] for key in sorted(outcome_refs)),
                summary_refs=tuple(summary_refs[key] for key in sorted(summary_refs)),
            )
            return self._finish(
                operation=operation,
                receipt=receipt,
                resolved_request_hash=resolved_request_hash,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                target_status=HistoricalRangeOperationStatus.COMPLETED,
                cursor=None,
                error_json=None,
            )
        except (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
            psycopg2.errors.SerializationFailure,
            psycopg2.errors.DeadlockDetected,
            psycopg2.errors.LockNotAvailable,
        ) as error:
            logger.exception(
                "historical_range_outcome_database_unavailable operation_id=%s "
                "error_type=%s",
                operation_request.operation_id,
                type(error).__name__,
            )
            return self._retryable(
                operation=operation,
                request=request,
                resolved_request_hash=resolved_request_hash,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                processed_count=processed_count,
                outcome_refs=tuple(outcome_refs[key] for key in sorted(outcome_refs)),
                summary_refs=tuple(summary_refs[key] for key in sorted(summary_refs)),
                reason_code=REASON_DATABASE_UNAVAILABLE,
            )
        except HistoricalRangeOutcomeSourceError as error:
            receipt = HistoricalRangeOutcomeRefreshReceiptV1(
                operation_id=operation_request.operation_id,
                request_hash=str(request.request_hash),
                status="WAITING_INPUT",
                stable_keyset_cursor=cursor,
                processed_count=processed_count,
                outcome_refs=tuple(outcome_refs[key] for key in sorted(outcome_refs)),
                summary_refs=tuple(summary_refs[key] for key in sorted(summary_refs)),
                reason_codes=(error.reason_code or REASON_OUTCOME_SOURCE_UNAVAILABLE,),
            )
            return self._finish(
                operation=operation,
                receipt=receipt,
                resolved_request_hash=resolved_request_hash,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                target_status=HistoricalRangeOperationStatus.WAITING_INPUT,
                cursor=receipt.stable_keyset_cursor,
                error_json=None,
            )
        except HistoricalRangeContractError as error:
            if error.reason_code in {
                REASON_DATABASE_CAPACITY_EXHAUSTED,
                REASON_DATABASE_UNAVAILABLE,
            }:
                logger.exception(
                    "historical_range_outcome_database_contract_unavailable "
                    "operation_id=%s reason_code=%s error_type=%s",
                    operation_request.operation_id,
                    error.reason_code,
                    type(error).__name__,
                )
                return self._retryable(
                    operation=operation,
                    request=request,
                    resolved_request_hash=resolved_request_hash,
                    worker_id=worker_id,
                    lease_token=lease_token,
                    fencing_token=fencing_token,
                    attempt_no=attempt_no,
                    started_at=started_at,
                    processed_count=processed_count,
                    outcome_refs=tuple(outcome_refs[key] for key in sorted(outcome_refs)),
                    summary_refs=tuple(summary_refs[key] for key in sorted(summary_refs)),
                    reason_code=error.reason_code,
                )
            return self._fail(
                operation=operation,
                request=request,
                resolved_request_hash=resolved_request_hash,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                processed_count=processed_count,
                outcome_refs=tuple(outcome_refs[key] for key in sorted(outcome_refs)),
                summary_refs=tuple(summary_refs[key] for key in sorted(summary_refs)),
                reason_code=error.reason_code,
                error_type=type(error).__name__,
            )
        except Exception as error:
            logger.exception(
                "historical_range_outcome_refresh_unhandled operation_id=%s "
                "range_run_id=%s subject_type=%s subject_id=%s projection=%s "
                "evaluation_window_type=%s horizon_trade_days=%s",
                operation_request.operation_id,
                current_item.range_run_id if current_item is not None else None,
                current_item.subject_type.value if current_item is not None else None,
                current_item.subject_id if current_item is not None else None,
                current_item.projection.value if current_item is not None else None,
                current_item.evaluation_window_type.value if current_item is not None else None,
                current_item.horizon_trade_days if current_item is not None else None,
            )
            return self._fail(
                operation=operation,
                request=request,
                resolved_request_hash=resolved_request_hash,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                processed_count=processed_count,
                outcome_refs=tuple(outcome_refs[key] for key in sorted(outcome_refs)),
                summary_refs=tuple(summary_refs[key] for key in sorted(summary_refs)),
                reason_code=REASON_OUTCOME_CALCULATION_FAILED,
                error_type=type(error).__name__,
            )

    def _load_prior_attempt_outputs(
        self,
        *,
        operation_id: str,
        request: HistoricalRangeOutcomeRefreshRequestV1,
        resolved_request_hash: str,
    ) -> tuple[
        dict[str, HistoricalRangeArtifactRefV1],
        dict[str, HistoricalRangeArtifactRefV1],
        dict[str, Any] | None,
    ]:
        outcome_refs: dict[str, HistoricalRangeArtifactRefV1] = {}
        summary_refs: dict[str, HistoricalRangeArtifactRefV1] = {}
        recovery_cursor: dict[str, Any] | None = None
        for ref in self._repository.list_operation_attempt_receipt_refs(operation_id=operation_id):
            if ref.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT:
                raise ValueError("outcome operation prior attempt has an invalid receipt kind")
            envelope = self._artifact_store.load(ref)
            receipt = HistoricalRangeOutcomeRefreshReceiptV1.model_validate(envelope.payload)
            if (
                envelope.resolved_request_hash != resolved_request_hash
                or receipt.operation_id != operation_id
                or receipt.request_hash != request.request_hash
            ):
                raise ValueError("outcome prior-attempt receipt differs from the exact request")
            for outcome_ref in receipt.outcome_refs:
                outcome_refs[outcome_ref.semantic_content_hash] = outcome_ref
            for summary_ref in receipt.summary_refs:
                summary_refs[summary_ref.semantic_content_hash] = summary_ref
            recovery_cursor = receipt.stable_keyset_cursor
        return outcome_refs, summary_refs, recovery_cursor

    def _claim(
        self,
        *,
        operation: dict[str, Any],
        request: HistoricalRangeOutcomeRefreshRequestV1,
        resolved_request_hash: str,
        worker_id: str,
        recovery_cursor: dict[str, Any] | None,
        outcome_refs: dict[str, HistoricalRangeArtifactRefV1],
        summary_refs: dict[str, HistoricalRangeArtifactRefV1],
    ) -> dict[str, Any]:
        expired_attempt = None
        replace_cursor = False
        claim_cursor = operation.get("stable_keyset_cursor_json")
        if str(operation["status"]) == HistoricalRangeOperationStatus.RUNNING.value:
            lease_expires_at = operation.get("lease_expires_at")
            if lease_expires_at is None or lease_expires_at > datetime.now(UTC):
                raise HistoricalRangeContractError(
                    REASON_REPOSITORY_CONFLICT,
                    "REFRESH_OUTCOMES operation already has an active lease",
                )
            expired_receipt = HistoricalRangeOutcomeRefreshReceiptV1(
                operation_id=str(operation["operation_id"]),
                request_hash=str(request.request_hash),
                status="RETRYABLE_FAILED",
                stable_keyset_cursor=recovery_cursor,
                processed_count=len(outcome_refs),
                outcome_refs=tuple(outcome_refs[key] for key in sorted(outcome_refs)),
                summary_refs=tuple(summary_refs[key] for key in sorted(summary_refs)),
                reason_codes=(REASON_REPOSITORY_CONFLICT,),
            )
            expired_upstream = tuple((*expired_receipt.outcome_refs, *expired_receipt.summary_refs))
            stored = self._artifact_store.publish_payload(
                artifact_kind=HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
                producer_contract_version="advisory_phase1r_r4_outcome_refresh_v1",
                payload_schema_version=OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION,
                resolved_request_hash=resolved_request_hash,
                payload=expired_receipt.model_dump(mode="json"),
                upstream_refs=expired_upstream,
            )
            expired_at = datetime.now(UTC)
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
                input_cursor_json=recovery_cursor,
                result_cursor_json=recovery_cursor,
                input_hash=str(request.request_hash),
                result_hash=stored.ref.semantic_content_hash,
                attempt_receipt_ref=stored.ref,
                reason_codes=(REASON_REPOSITORY_CONFLICT,),
                error_json={
                    "reason_code": REASON_REPOSITORY_CONFLICT,
                    "error_type": "LeaseExpired",
                },
                started_at=operation.get("started_at") or expired_at,
                finished_at=expired_at,
            )
            claim_cursor = recovery_cursor
            replace_cursor = True
        return self._repository.transition_operation(
            operation_id=str(operation["operation_id"]),
            expected_row_version=int(operation["row_version"]),
            target_status=HistoricalRangeOperationStatus.RUNNING,
            attempt_no=int(operation["attempt_no"]) + 1,
            worker_id=worker_id,
            lease_token=uuid4().hex,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=request.lease_seconds),
            fencing_token=int(operation.get("fencing_token") or 0) + 1,
            stable_keyset_cursor_json=claim_cursor,
            replace_stable_keyset_cursor=replace_cursor,
            started_at=datetime.now(UTC),
            expired_attempt=expired_attempt,
        )

    def _finish(
        self,
        *,
        operation: dict[str, Any],
        receipt: HistoricalRangeOutcomeRefreshReceiptV1,
        resolved_request_hash: str,
        worker_id: str,
        lease_token: str,
        fencing_token: int,
        attempt_no: int,
        started_at: datetime,
        target_status: HistoricalRangeOperationStatus,
        cursor: dict[str, Any] | None,
        error_json: dict[str, Any] | None,
    ) -> tuple[HistoricalRangeOutcomeRefreshReceiptV1, HistoricalRangeArtifactRefV1]:
        upstream = tuple((*receipt.outcome_refs, *receipt.summary_refs))
        stored = self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
            producer_contract_version="advisory_phase1r_r4_outcome_refresh_v1",
            payload_schema_version=OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION,
            resolved_request_hash=resolved_request_hash,
            payload=receipt.model_dump(mode="json"),
            upstream_refs=upstream,
        )
        finished_at = datetime.now(UTC)
        attempt = HistoricalRangeOperationAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahropa",
                {"operation_id": receipt.operation_id, "attempt_no": attempt_no, "fencing_token": fencing_token},
            ),
            operation_id=receipt.operation_id,
            attempt_no=attempt_no,
            worker_id=worker_id,
            lease_token=lease_token,
            fencing_token=fencing_token,
            status=target_status.value,
            input_cursor_json=operation.get("stable_keyset_cursor_json"),
            result_cursor_json=cursor,
            input_hash=receipt.request_hash,
            result_hash=stored.ref.semantic_content_hash,
            attempt_receipt_ref=stored.ref,
            reason_codes=receipt.reason_codes,
            error_json=error_json,
            started_at=started_at,
            finished_at=finished_at,
        )
        self._repository.transition_operation(
            operation_id=receipt.operation_id,
            expected_row_version=int(operation["row_version"]),
            target_status=target_status,
            attempt_no=attempt_no,
            fencing_token=fencing_token,
            stable_keyset_cursor_json=cursor,
            result_status=receipt.status,
            result_ref=stored.ref,
            error_json=error_json,
            finished_at=finished_at
            if target_status in {HistoricalRangeOperationStatus.COMPLETED, HistoricalRangeOperationStatus.FAILED}
            else None,
            attempt=attempt,
        )
        return receipt, stored.ref

    def _fail(
        self,
        *,
        operation: dict[str, Any],
        request: HistoricalRangeOutcomeRefreshRequestV1,
        resolved_request_hash: str,
        worker_id: str,
        lease_token: str,
        fencing_token: int,
        attempt_no: int,
        started_at: datetime,
        processed_count: int,
        outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...],
        summary_refs: tuple[HistoricalRangeArtifactRefV1, ...],
        reason_code: str,
        error_type: str,
    ) -> tuple[HistoricalRangeOutcomeRefreshReceiptV1, HistoricalRangeArtifactRefV1]:
        error_json = {"reason_code": reason_code, "error_type": error_type}
        logger.error(
            "historical_range_outcome_refresh_failed operation_id=%s reason_code=%s error_type=%s",
            operation["operation_id"],
            reason_code,
            error_type,
        )
        receipt = HistoricalRangeOutcomeRefreshReceiptV1(
            operation_id=str(operation["operation_id"]),
            request_hash=str(request.request_hash),
            status="FAILED",
            stable_keyset_cursor=operation.get("stable_keyset_cursor_json"),
            processed_count=processed_count,
            outcome_refs=outcome_refs,
            summary_refs=summary_refs,
            reason_codes=(reason_code,),
        )
        return self._finish(
            operation=operation,
            receipt=receipt,
            resolved_request_hash=resolved_request_hash,
            worker_id=worker_id,
            lease_token=lease_token,
            fencing_token=fencing_token,
            attempt_no=attempt_no,
            started_at=started_at,
            target_status=HistoricalRangeOperationStatus.FAILED,
            cursor=receipt.stable_keyset_cursor,
            error_json=error_json,
        )

    def _retryable(
        self,
        *,
        operation: dict[str, Any],
        request: HistoricalRangeOutcomeRefreshRequestV1,
        resolved_request_hash: str,
        worker_id: str,
        lease_token: str,
        fencing_token: int,
        attempt_no: int,
        started_at: datetime,
        processed_count: int,
        outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...],
        summary_refs: tuple[HistoricalRangeArtifactRefV1, ...],
        reason_code: str,
    ) -> tuple[HistoricalRangeOutcomeRefreshReceiptV1, HistoricalRangeArtifactRefV1]:
        cursor = operation.get("stable_keyset_cursor_json")
        receipt = HistoricalRangeOutcomeRefreshReceiptV1(
            operation_id=str(operation["operation_id"]),
            request_hash=str(request.request_hash),
            status="RETRYABLE_FAILED",
            stable_keyset_cursor=cursor,
            processed_count=processed_count,
            outcome_refs=outcome_refs,
            summary_refs=summary_refs,
            reason_codes=(reason_code,),
        )
        stored = self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
            producer_contract_version="advisory_phase1r_r4_outcome_refresh_v1",
            payload_schema_version=OUTCOME_REFRESH_RECEIPT_SCHEMA_VERSION,
            resolved_request_hash=resolved_request_hash,
            payload=receipt.model_dump(mode="json"),
            upstream_refs=tuple((*outcome_refs, *summary_refs)),
        )
        finished_at = datetime.now(UTC)
        error_json = {
            "reason_code": reason_code,
            "error_type": (
                "DatabaseUnavailable" if reason_code == REASON_DATABASE_UNAVAILABLE else "RetryableCapacityError"
            ),
        }
        attempt = HistoricalRangeOperationAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahropa",
                {"operation_id": receipt.operation_id, "attempt_no": attempt_no, "fencing_token": fencing_token},
            ),
            operation_id=receipt.operation_id,
            attempt_no=attempt_no,
            worker_id=worker_id,
            lease_token=lease_token,
            fencing_token=fencing_token,
            status=HistoricalRangeOperationStatus.RETRYABLE_FAILED.value,
            input_cursor_json=operation.get("stable_keyset_cursor_json"),
            result_cursor_json=cursor,
            input_hash=receipt.request_hash,
            result_hash=stored.ref.semantic_content_hash,
            attempt_receipt_ref=stored.ref,
            reason_codes=(reason_code,),
            error_json=error_json,
            started_at=started_at,
            finished_at=finished_at,
        )
        self._repository.transition_operation(
            operation_id=receipt.operation_id,
            expected_row_version=int(operation["row_version"]),
            target_status=HistoricalRangeOperationStatus.RETRYABLE_FAILED,
            attempt_no=attempt_no,
            fencing_token=fencing_token,
            stable_keyset_cursor_json=cursor,
            error_json=error_json,
            attempt=attempt,
        )
        return receipt, stored.ref
