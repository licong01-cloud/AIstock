"""Phase 1G G4 orchestration for historical advisory observation capture.

This module owns orchestration only.  It consumes the frozen G1-G3 contracts,
uses caller-injected database connections and never imports Selection, Paper,
simulation, QE, QMT or trading runtime modules.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from enum import Enum
import logging
import math
import time
from typing import Any, Callable, Iterator
from uuid import uuid4

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_phase0a.policy import (
    canonical_json_sha256,
    canonicalize,
)
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatch,
    CaptureBatchRequest,
    CaptureBatchStatus,
    CapturePlan,
    PostgresCaptureBatchRepository,
)
from backend.services.advisory_phase1.control_binding import (
    ControlBindingEvent,
    ControlBindingRequest,
    ControlType,
    PostgresControlBindingRepository,
    REASON_CONTROL_BINDING_UNAVAILABLE,
)
from backend.services.advisory_phase1.historical_trace_projection_postgres import (
    project_phase1g_target_snapshot,
)
from backend.services.advisory_phase1.observation_capture import (
    Phase1GObservationSemanticDraft,
    build_observation_semantic_draft,
)
from backend.services.advisory_phase1.phase1g_artifact_ref import (
    Phase1GImmutableArtifactResolver,
    build_phase1g_target_execution_request,
)
from backend.services.advisory_phase1.phase1g_contract import (
    DEFAULT_CAPTURE_POLICY_REGISTRY,
    REASON_ATTEMPT_RECEIPT_STORE_FAILED,
    REASON_BATCH_IN_PROGRESS,
    REASON_BATCH_RECEIPT_STORE_FAILED,
    REASON_BATCH_STATE_CONFLICT,
    REASON_CAPTURE_TIMEOUT,
    REASON_OPERATION_DEFERRED,
    REASON_PLAN_INVALID,
    REASON_PLAN_STALE,
    REASON_RESULT_STORE_FAILED,
    REASON_TARGET_DIAGNOSTIC,
    REASON_UNEXPECTED_ERROR,
    Phase1GAttemptReceipt,
    Phase1GAttemptStatus,
    Phase1GBatchAttemptReceipt,
    Phase1GBatchStatus,
    Phase1GCapturePolicyRegistry,
    Phase1GCaptureResult,
    Phase1GExecutionBatchPlan,
    Phase1GExecutionBatchRequest,
    Phase1GIdentityHashRef,
    Phase1GOutputArtifactRef,
    Phase1GSelectedObservationMapping,
    Phase1GTargetAttemptRef,
    Phase1GTargetCommitProjection,
    Phase1GTargetExecutionPlan,
    Phase1GTargetExecutionRequest,
    Phase1GTraceOutboxMapping,
    Phase1GTransactionalWriteRequest,
    build_phase1g_execution_batch_plan,
)
from backend.services.advisory_phase1.phase1g_historical_trace_contract import (
    Phase1GTargetProjectionSnapshot,
    materialize_phase1g_stage_trace_envelope,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EExecutionPlanProjection,
    Phase1EOperationDisposition,
    Phase1EPlanUnitKind,
    Phase1EPlannedOperationType,
)
from backend.services.advisory_phase1.phase1g_result_store import (
    Phase1GResultStore,
    Phase1GResultStoreError,
    StoredPhase1GArtifact,
)
from backend.services.advisory_phase1.phase1g_schema_guard import (
    Phase1GReleaseSchemaGuard,
    Phase1GSchemaGuardEvidence,
)
from backend.services.advisory_phase1.phase1g_transactional_writer import (
    Phase1GTransactionalTargetInput,
    Phase1GTransactionalWriter,
)
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    DatabaseConnectionConfig,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.stage_trace import (
    TraceCaptureBinding,
    TraceCaptureContext,
    TraceCapturePolicy,
)
from backend.services.advisory_phase1.trace_outbox import (
    PostgresTraceOutboxRepository,
    TraceOutboxRecord,
)


logger = logging.getLogger(__name__)

ConnectionFactory = Callable[[], Any]
NowProvider = Callable[[], datetime]
MonotonicProvider = Callable[[], float]

TRACE_CAPTURE_POLICY = TraceCapturePolicy(
    policy_id="ADVISORY_PHASE1G_HISTORICAL_OBSERVATION_TRACE",
    policy_version="1",
    max_candidates=100_000,
    max_bytes=64 * 1024 * 1024,
    max_capture_ms=60_000,
)


class Phase1GServiceError(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = canonicalize(context or {})


class Phase1GOperationStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Phase1GInvocationBatchStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    FAILED = "FAILED"


class Phase1GExitClass(str, Enum):
    SUCCESS = "SUCCESS"
    BUSINESS_FAILURE = "BUSINESS_FAILURE"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    INFRASTRUCTURE_FAILURE = "INFRASTRUCTURE_FAILURE"


class _ServiceContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Phase1GCaptureRequestSemanticPreview(_ServiceContract):
    capture_request_hash: str = Field(min_length=64, max_length=64)
    canonical_payload: dict[str, Any]
    capture_plan_set_count: int = Field(ge=1)
    capture_plan_set_hash: str = Field(min_length=64, max_length=64)


class Phase1GTargetInvocationOutcome(_ServiceContract):
    target_request_hash: str = Field(min_length=64, max_length=64)
    target_plan_hash: str = Field(min_length=64, max_length=64)
    operation_status: Phase1GOperationStatus
    reason_codes: tuple[str, ...] = ()
    dml_executed: bool
    committed_phases: tuple[str, ...] = ()
    capture_batch_id: str | None = None
    capture_attempt_no: int | None = Field(default=None, ge=1)
    capture_batch_status: str | None = None
    capture_result_ref: Phase1GOutputArtifactRef | None = None
    capture_result_hash: str | None = Field(default=None, min_length=64, max_length=64)
    attempt_receipt_ref: Phase1GOutputArtifactRef | None = None
    attempt_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)
    error_context: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _close_outcome(self) -> "Phase1GTargetInvocationOutcome":
        object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))
        object.__setattr__(
            self, "committed_phases", tuple(sorted(set(self.committed_phases)))
        )
        if self.operation_status is Phase1GOperationStatus.SUCCESS and (
            self.capture_result_ref is None
            or self.capture_result_hash is None
            or self.attempt_receipt_ref is None
            or self.attempt_receipt_hash is None
        ):
            raise ValueError("successful target outcome requires durable result and attempt")
        return self


class Phase1GBatchInvocationOutcome(_ServiceContract):
    batch_request_hash: str = Field(min_length=64, max_length=64)
    batch_plan_hash: str = Field(min_length=64, max_length=64)
    target_outcomes: tuple[Phase1GTargetInvocationOutcome, ...]
    succeeded_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    batch_status: Phase1GInvocationBatchStatus
    reason_codes: tuple[str, ...] = ()
    batch_attempt_receipt_ref: Phase1GOutputArtifactRef | None = None
    batch_attempt_receipt_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    exit_class: Phase1GExitClass

    @model_validator(mode="after")
    def _close_outcome(self) -> "Phase1GBatchInvocationOutcome":
        ordered = tuple(
            sorted(self.target_outcomes, key=lambda item: item.target_request_hash)
        )
        object.__setattr__(self, "target_outcomes", ordered)
        object.__setattr__(
            self, "reason_codes", tuple(sorted(set(self.reason_codes)))
        )
        if self.succeeded_count + self.failed_count != len(ordered):
            raise ValueError("batch outcome counts do not close")
        return self


class _LoadedTarget:
    def __init__(
        self,
        *,
        target_request: Phase1GTargetExecutionRequest,
        receipt: Any,
        phase1e_plan: Phase1EExecutionPlanProjection,
        schema_evidence: Phase1GSchemaGuardEvidence,
        snapshot: Phase1GTargetProjectionSnapshot,
        capture_plans: tuple[CapturePlan, ...],
        desired_control: ControlBindingRequest,
        preview: Phase1GCaptureRequestSemanticPreview,
    ) -> None:
        self.target_request = target_request
        self.receipt = receipt
        self.phase1e_plan = phase1e_plan
        self.schema_evidence = schema_evidence
        self.snapshot = snapshot
        self.capture_plans = capture_plans
        self.desired_control = desired_control
        self.preview = preview


class Phase1GService:
    """Plan and capture exact Phase 1G targets without runtime activation."""

    def __init__(
        self,
        *,
        connection_config: DatabaseConnectionConfig,
        transaction_connection_factory: ConnectionFactory,
        readonly_connection_factory: ConnectionFactory,
        artifact_resolver: Phase1GImmutableArtifactResolver,
        result_store: Phase1GResultStore,
        schema_guard: Phase1GReleaseSchemaGuard | None = None,
        capture_policy_registry: Phase1GCapturePolicyRegistry = DEFAULT_CAPTURE_POLICY_REGISTRY,
        trace_capture_policy: TraceCapturePolicy = TRACE_CAPTURE_POLICY,
        now_provider: NowProvider | None = None,
        monotonic_provider: MonotonicProvider | None = None,
    ) -> None:
        if trace_capture_policy.max_candidates > capture_policy_registry.absolute_max_candidates:
            raise ValueError("trace candidate policy exceeds registry")
        if trace_capture_policy.max_bytes > capture_policy_registry.absolute_max_bytes:
            raise ValueError("trace byte policy exceeds registry")
        if trace_capture_policy.max_capture_ms > capture_policy_registry.absolute_max_capture_ms:
            raise ValueError("trace duration policy exceeds registry")
        self._connection_config = connection_config
        self._transaction_connection_factory = transaction_connection_factory
        self._readonly_connection_factory = readonly_connection_factory
        self._artifact_resolver = artifact_resolver
        self._result_store = result_store
        self._schema_guard = schema_guard or Phase1GReleaseSchemaGuard()
        self._registry = capture_policy_registry
        self._trace_policy = trace_capture_policy
        self._now = now_provider or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic_provider or time.monotonic
        self._capture_deadline: ContextVar[float | None] = ContextVar(
            "phase1g_capture_deadline", default=None
        )
        self._schema_cache: dict[str, Phase1GSchemaGuardEvidence] = {}
        self._capture_repository = PostgresCaptureBatchRepository(
            conn_factory=self._transaction_scope
        )

    def plan_batch(
        self, batch_request: Phase1GExecutionBatchRequest
    ) -> Phase1GExecutionBatchPlan:
        batch_request = Phase1GExecutionBatchRequest.model_validate(
            batch_request.model_dump(mode="json")
        )
        target_plans: list[Phase1GTargetExecutionPlan] = []
        failures: list[dict[str, Any]] = []
        for target in batch_request.targets:
            try:
                loaded = self._load_target(target)
                mutable = self._observe_mutable_state(
                    loaded=loaded,
                    target=target,
                    capture_plans=loaded.capture_plans,
                    desired_control=loaded.desired_control,
                    preview=loaded.preview,
                )
                historical = loaded.snapshot.historical_trace_projection
                replay = loaded.snapshot.source_replay_result
                target_plans.append(
                    Phase1GTargetExecutionPlan(
                        target_request=target,
                        release_receipt_hash=loaded.schema_evidence.release_receipt_hash,
                        release_catalog_fingerprint=loaded.schema_evidence.catalog_fingerprint,
                        database_identity=loaded.schema_evidence.database_identity,
                        phase1e_plan_id=target.phase1e_plan_id,
                        phase1e_plan_hash=target.phase1e_plan_hash,
                        source_resolution_expected_hash=str(
                            replay.source_replay_result_hash
                        ),
                        expected_source_events=tuple(
                            Phase1GIdentityHashRef(
                                identity=item.availability_event_id,
                                content_hash=item.event_content_hash,
                            )
                            for item in replay.expected_source_event_refs
                        ),
                        expected_dse=Phase1GIdentityHashRef(
                            identity=historical.dse.evidence_id,
                            content_hash=historical.dse.artifact_hash,
                        ),
                        expected_selection_artifact=Phase1GIdentityHashRef(
                            identity=historical.artifact.artifact_id,
                            content_hash=historical.artifact.artifact_payload_sha256,
                        ),
                        expected_package=Phase1GIdentityHashRef(
                            identity=(
                                f"{historical.package_manifest.package_id}@"
                                f"{historical.package_manifest.manifest_sha256}"
                            ),
                            content_hash=str(
                                historical.package_manifest.package_manifest_projection_hash
                            ),
                        ),
                        expected_capture_plan_set_hash=loaded.preview.capture_plan_set_hash,
                        expected_capture_plan_set_count=len(loaded.capture_plans),
                        expected_rows=self._expected_rows(loaded.snapshot),
                        expected_bytes=loaded.snapshot.projected_bytes,
                        capture_policy_registry_hash=str(self._registry.registry_hash),
                        observed_current_binding_head_hash=mutable[0],
                        observed_capture_batch_state_hash=mutable[1],
                        observed_outbox_identity_hashes=mutable[2],
                        observed_at=self._utc_now(),
                    )
                )
            except Exception as exc:
                reason, context = self._reason_context(exc)
                if reason == REASON_UNEXPECTED_ERROR:
                    logger.exception(
                        "phase1g g4 target planning failed unexpectedly",
                        extra={
                            "target_request_hash_prefix": str(target.request_hash)[:12],
                            "reason_code": reason,
                            "exception_type": type(exc).__name__,
                        },
                    )
                failures.append(
                    {
                        "target_request_hash": str(target.request_hash),
                        "reason_code": reason,
                        **context,
                    }
                )
        if failures:
            raise Phase1GServiceError(
                REASON_PLAN_INVALID,
                "one or more Phase 1G targets cannot be planned",
                context={"target_failures": failures},
            )
        return build_phase1g_execution_batch_plan(
            batch_request=batch_request, target_plans=tuple(target_plans)
        )

    def capture_batch(
        self, batch_plan: Phase1GExecutionBatchPlan
    ) -> Phase1GBatchInvocationOutcome:
        batch_plan = Phase1GExecutionBatchPlan.model_validate(
            batch_plan.model_dump(mode="json")
        )
        outcomes = self._capture_targets(batch_plan.target_plans)
        succeeded = sum(
            item.operation_status is Phase1GOperationStatus.SUCCESS
            for item in outcomes
        )
        failed = len(outcomes) - succeeded
        batch_status = (
            Phase1GInvocationBatchStatus.SUCCESS
            if failed == 0
            else (
                Phase1GInvocationBatchStatus.FAILED
                if succeeded == 0
                else Phase1GInvocationBatchStatus.PARTIAL_FAILURE
            )
        )
        batch_artifact: StoredPhase1GArtifact | None = None
        batch_reason_codes: tuple[str, ...] = ()
        durable = all(item.attempt_receipt_hash is not None for item in outcomes)
        if durable:
            receipt = Phase1GBatchAttemptReceipt(
                batch_request_hash=batch_plan.batch_request_hash,
                batch_plan_hash=str(batch_plan.batch_plan_hash),
                target_count=len(outcomes),
                succeeded_count=succeeded,
                failed_count=failed,
                target_attempt_refs=tuple(
                    Phase1GTargetAttemptRef(
                        target_request_hash=item.target_request_hash,
                        target_plan_hash=item.target_plan_hash,
                        attempt_receipt_hash=str(item.attempt_receipt_hash),
                        operation_status=(
                            Phase1GAttemptStatus.SUCCESS
                            if item.operation_status is Phase1GOperationStatus.SUCCESS
                            else Phase1GAttemptStatus.FAILED
                        ),
                        capture_result_hash=item.capture_result_hash,
                    )
                    for item in outcomes
                ),
                batch_status=(
                    Phase1GBatchStatus.SUCCESS
                    if failed == 0
                    else (
                        Phase1GBatchStatus.FAILED
                        if succeeded == 0
                        else Phase1GBatchStatus.PARTIAL_FAILURE
                    )
                ),
            )
            try:
                batch_artifact = self._result_store.publish_batch(receipt)
            except Phase1GResultStoreError as exc:
                logger.exception(
                    "phase1g g4 batch receipt publication failed",
                    extra={"reason_code": REASON_BATCH_RECEIPT_STORE_FAILED},
                )
                batch_reason_codes = (
                    str(exc.reason_code or REASON_BATCH_RECEIPT_STORE_FAILED),
                )
            except Exception as exc:
                logger.exception(
                    "phase1g g4 batch receipt publication failed unexpectedly",
                    extra={
                        "reason_code": REASON_UNEXPECTED_ERROR,
                        "exception_type": type(exc).__name__,
                    },
                )
                batch_reason_codes = (REASON_UNEXPECTED_ERROR,)
        exit_class = self._exit_class(outcomes, batch_artifact, durable)
        return Phase1GBatchInvocationOutcome(
            batch_request_hash=batch_plan.batch_request_hash,
            batch_plan_hash=str(batch_plan.batch_plan_hash),
            target_outcomes=outcomes,
            succeeded_count=succeeded,
            failed_count=failed,
            batch_status=batch_status,
            reason_codes=batch_reason_codes,
            batch_attempt_receipt_ref=(batch_artifact.ref if batch_artifact else None),
            batch_attempt_receipt_hash=(
                batch_artifact.ref.semantic_content_hash if batch_artifact else None
            ),
            exit_class=exit_class,
        )

    def _capture_targets(
        self, target_plans: tuple[Phase1GTargetExecutionPlan, ...]
    ) -> tuple[Phase1GTargetInvocationOutcome, ...]:
        outcomes: list[Phase1GTargetInvocationOutcome] = []
        for target_plan in target_plans:
            try:
                outcomes.append(self._capture_target(target_plan))
            except Exception as exc:
                logger.exception(
                    "phase1g g4 target invocation escaped its terminal handler",
                    extra={
                        "target_request_hash_prefix": str(
                            target_plan.target_request.request_hash
                        )[:12],
                        "reason_code": REASON_UNEXPECTED_ERROR,
                        "exception_type": type(exc).__name__,
                    },
                )
                outcomes.append(
                    Phase1GTargetInvocationOutcome(
                        target_request_hash=str(
                            target_plan.target_request.request_hash
                        ),
                        target_plan_hash=str(target_plan.target_plan_hash),
                        operation_status=Phase1GOperationStatus.FAILED,
                        reason_codes=(REASON_UNEXPECTED_ERROR,),
                        dml_executed=False,
                        error_context={
                            "cause_reason_code": REASON_UNEXPECTED_ERROR,
                            "exception_type": type(exc).__name__,
                        },
                    )
                )
        return tuple(outcomes)

    def verify_result(self, ref: Phase1GOutputArtifactRef) -> Phase1GCaptureResult:
        model = self._result_store.load(ref)
        if not isinstance(model, Phase1GCaptureResult):
            raise Phase1GServiceError(REASON_RESULT_STORE_FAILED, "artifact is not a result")
        return model

    def verify_attempt(self, ref: Phase1GOutputArtifactRef) -> Phase1GAttemptReceipt:
        model = self._result_store.load(ref)
        if not isinstance(model, Phase1GAttemptReceipt):
            raise Phase1GServiceError(
                REASON_ATTEMPT_RECEIPT_STORE_FAILED, "artifact is not an attempt receipt"
            )
        return model

    def _capture_target(
        self, target_plan: Phase1GTargetExecutionPlan
    ) -> Phase1GTargetInvocationOutcome:
        started = self._utc_now()
        deadline = self._monotonic() + self._registry.absolute_max_capture_ms / 1000
        token = self._capture_deadline.set(deadline)
        try:
            return self._capture_target_with_deadline(
                target_plan=target_plan,
                started=started,
                deadline=deadline,
            )
        finally:
            self._capture_deadline.reset(token)

    def _capture_target_with_deadline(
        self,
        *,
        target_plan: Phase1GTargetExecutionPlan,
        started: datetime,
        deadline: float,
    ) -> Phase1GTargetInvocationOutcome:
        invocation_id = f"p1ga_{uuid4().hex}"
        phases: list[str] = []
        dml = False
        owns_running_batch = False
        batch: CaptureBatch | None = None
        result_artifact: StoredPhase1GArtifact | None = None
        try:
            self._require_budget(deadline)
            loaded = self._load_target(target_plan.target_request)
            self._assert_immutable_plan(target_plan, loaded)
            chain, current_head, outboxes = self._read_mutable_facts(
                target=target_plan.target_request,
                capture_plans=loaded.capture_plans,
                desired_control=loaded.desired_control,
                preview=loaded.preview,
            )
            self._assert_mutable_successor(
                target_plan=target_plan,
                desired_control=loaded.desired_control,
                chain=chain,
                current_head=current_head,
                outboxes=outboxes,
            )
            self._assert_outboxes_match_frozen(loaded=loaded, outboxes=outboxes)
            self._require_budget(deadline)
            event, event_dml = self._select_control_event(
                desired=loaded.desired_control,
                chain=chain,
                current_head=current_head,
            )
            if event_dml:
                dml = True
                phases.append("CONTROL_BINDING")
            request = self._materialize_request(
                loaded=loaded,
                event=event,
                attempt_no=(chain[-1].capture_attempt_no + 1 if chain else 1),
            )
            if request.capture_request_hash != loaded.preview.capture_request_hash:
                raise Phase1GServiceError(
                    REASON_PLAN_STALE,
                    "actual capture request differs from pre-DML semantic preview",
                )
            batch = chain[-1] if chain else None
            batch, state_phases = self._select_or_acquire_batch(
                request=request, chain=chain
            )
            owns_running_batch = batch.status is CaptureBatchStatus.RUNNING
            if state_phases:
                dml = True
                phases.extend(state_phases)
            if batch.status is CaptureBatchStatus.COMPLETE:
                projections, drafts = self._read_complete_projections(
                    target_plan=target_plan,
                    loaded=loaded,
                    batch=batch,
                )
            else:
                projections = {}
                drafts = {}
                for plan in sorted(loaded.capture_plans, key=lambda item: str(item.plan_hash)):
                    self._require_budget(deadline)
                    batch = self._latest_batch(str(request.capture_request_hash))
                    target_input, draft = self._build_transactional_target(
                        target_plan=target_plan,
                        loaded=loaded,
                        batch=batch,
                        capture_plan=plan,
                    )
                    projection = self._writer_for_current_budget().write_target(
                        target_input
                    )
                    projections[str(plan.plan_hash)] = projection
                    drafts[str(plan.plan_hash)] = draft
                    dml = True
                    phases.append("TARGET_EVIDENCE")
                self._require_budget(deadline)
                batch = self._latest_batch(str(request.capture_request_hash))
                try:
                    batch = self._capture_repository.complete(
                        capture_batch_id=request.capture_batch_id,
                        expected_row_version=batch.row_version,
                        fencing_token=batch.fencing_token,
                    )
                    dml = True
                    phases.append("BATCH_COMPLETED")
                except Exception:
                    classified = self._latest_batch(str(request.capture_request_hash))
                    if classified.status is not CaptureBatchStatus.COMPLETE:
                        raise
                    batch = classified
                    phases.append("BATCH_COMPLETED")
                projections, drafts = self._read_complete_projections(
                    target_plan=target_plan,
                    loaded=loaded,
                    batch=batch,
                )
            self._require_budget(deadline)
            result = self._build_result(
                target_plan=target_plan,
                loaded=loaded,
                batch=batch,
                projections=projections,
                drafts=drafts,
            )
            result_artifact = self._result_store.publish_result(result)
            if not result_artifact.idempotent:
                phases.append("RESULT_PUBLISHED")
            return self._publish_attempt_outcome(
                target_plan=target_plan,
                invocation_id=invocation_id,
                started=started,
                operation_status=Phase1GOperationStatus.SUCCESS,
                reason_codes=(),
                dml=dml,
                phases=phases,
                batch=batch,
                result_artifact=result_artifact,
                error_context=None,
                deadline=deadline,
            )
        except Exception as exc:
            reason, context = self._reason_context(exc)
            if (
                owns_running_batch
                and batch is not None
                and batch.status is CaptureBatchStatus.RUNNING
            ):
                try:
                    with self._failure_fencing_scope(
                        timeout_failure=reason == REASON_CAPTURE_TIMEOUT
                    ):
                        latest = self._latest_batch(
                            str(batch.request.capture_request_hash)
                        )
                        if latest.status is CaptureBatchStatus.RUNNING:
                            batch = self._capture_repository.fail(
                                capture_batch_id=latest.request.capture_batch_id,
                                expected_row_version=latest.row_version,
                                fencing_token=latest.fencing_token,
                                reason_codes=(reason,),
                            )
                            dml = True
                            phases.append("BATCH_FAILED")
                except Exception as transition_exc:
                    logger.exception(
                        "phase1g g4 failed-batch transition failed",
                        extra={
                            "reason_code": reason,
                            "transition_reason_code": getattr(
                                transition_exc, "reason_code", REASON_UNEXPECTED_ERROR
                            ),
                        },
                    )
            if not isinstance(exc, (Phase1GServiceError, SourceLedgerError, Phase1GResultStoreError)):
                logger.exception(
                    "phase1g g4 target capture failed unexpectedly",
                    extra={
                        "target_request_hash_prefix": str(
                            target_plan.target_request.request_hash
                        )[:12],
                        "reason_code": reason,
                    },
                )
            return self._publish_attempt_outcome(
                target_plan=target_plan,
                invocation_id=invocation_id,
                started=started,
                operation_status=Phase1GOperationStatus.FAILED,
                reason_codes=(reason,),
                dml=dml,
                phases=phases,
                batch=batch,
                result_artifact=None,
                error_context=context,
                deadline=deadline,
            )

    def _load_target(self, target: Phase1GTargetExecutionRequest) -> _LoadedTarget:
        if target.target_label is not self._connection_config.target_label:
            raise Phase1GServiceError(
                REASON_PLAN_INVALID, "target label differs from exact connection target"
            )
        release_artifact = self._artifact_resolver.resolve(
            target.release_schema_receipt_ref
        )
        plan_artifact = self._artifact_resolver.resolve(target.phase1e_plan_ref)
        phase1e_plan = plan_artifact.payload
        if not isinstance(phase1e_plan, Phase1EExecutionPlanProjection):
            raise Phase1GServiceError(REASON_PLAN_INVALID, "Phase 1E artifact type is invalid")
        receipt = release_artifact.payload
        self._assert_executable_phase1e(phase1e_plan)
        rebuilt = build_phase1g_target_execution_request(
            target_label=target.target_label,
            release_schema_receipt_ref=target.release_schema_receipt_ref,
            phase1e_plan_ref=target.phase1e_plan_ref,
            phase1e_plan=phase1e_plan,
            requested_at=target.requested_at,
            capture_policy=self._registry,
        )
        if rebuilt != target:
            raise Phase1GServiceError(
                REASON_PLAN_STALE, "target request differs from immutable Phase 1E artifacts"
            )
        receipt_hash = str(target.release_schema_receipt_ref.semantic_content_hash)
        evidence = self._schema_cache.get(receipt_hash)
        if evidence is None:
            evidence = self._schema_guard.verify(
                receipt=receipt,
                target_label=target.target_label,
                connection_config=self._connection_config,
            )
            self._schema_cache[receipt_hash] = evidence
        statement_timeout_ms, lock_timeout_ms = self._timeout_pair()
        snapshot = project_phase1g_target_snapshot(
            conn_factory=self._readonly_scope,
            phase1e_plan=phase1e_plan,
            target_request=target,
            statement_timeout_ms=statement_timeout_ms,
            lock_timeout_ms=lock_timeout_ms,
        )
        capture_plans = self._capture_plans(phase1e_plan)
        self._assert_capacity(snapshot)
        desired_control = self._desired_control(target, capture_plans)
        preview = self._preview_request(
            target=target,
            capture_plans=capture_plans,
            desired_control=desired_control,
            phase1e_plan=phase1e_plan,
        )
        return _LoadedTarget(
            target_request=target,
            receipt=receipt,
            phase1e_plan=phase1e_plan,
            schema_evidence=evidence,
            snapshot=snapshot,
            capture_plans=capture_plans,
            desired_control=desired_control,
            preview=preview,
        )

    def _assert_immutable_plan(
        self, target_plan: Phase1GTargetExecutionPlan, loaded: _LoadedTarget
    ) -> None:
        historical = loaded.snapshot.historical_trace_projection
        replay = loaded.snapshot.source_replay_result
        actual = {
            "release_receipt_hash": loaded.schema_evidence.release_receipt_hash,
            "release_catalog_fingerprint": loaded.schema_evidence.catalog_fingerprint,
            "database_identity": loaded.schema_evidence.database_identity,
            "phase1e_plan_id": loaded.phase1e_plan.plan_id,
            "phase1e_plan_hash": loaded.phase1e_plan.plan_hash,
            "source_resolution_expected_hash": replay.source_replay_result_hash,
            "expected_source_events": tuple(
                Phase1GIdentityHashRef(
                    identity=item.availability_event_id,
                    content_hash=item.event_content_hash,
                )
                for item in replay.expected_source_event_refs
            ),
            "expected_dse": Phase1GIdentityHashRef(
                identity=historical.dse.evidence_id,
                content_hash=historical.dse.artifact_hash,
            ),
            "expected_selection_artifact": Phase1GIdentityHashRef(
                identity=historical.artifact.artifact_id,
                content_hash=historical.artifact.artifact_payload_sha256,
            ),
            "expected_package": Phase1GIdentityHashRef(
                identity=(
                    f"{historical.package_manifest.package_id}@"
                    f"{historical.package_manifest.manifest_sha256}"
                ),
                content_hash=str(
                    historical.package_manifest.package_manifest_projection_hash
                ),
            ),
            "expected_capture_plan_set_hash": loaded.preview.capture_plan_set_hash,
            "expected_capture_plan_set_count": len(loaded.capture_plans),
            "expected_rows": self._expected_rows(loaded.snapshot),
            "expected_bytes": loaded.snapshot.projected_bytes,
            "capture_policy_registry_hash": str(self._registry.registry_hash),
        }
        for field_name, value in actual.items():
            if getattr(target_plan, field_name) != value:
                raise Phase1GServiceError(
                    REASON_PLAN_STALE,
                    "immutable Phase 1G target fact changed",
                    context={"field_name": field_name},
                )

    @staticmethod
    def _assert_executable_phase1e(plan: Phase1EExecutionPlanProjection) -> None:
        if plan.plan_unit_kind is Phase1EPlanUnitKind.TARGET_DIAGNOSTIC:
            raise Phase1GServiceError(
                REASON_TARGET_DIAGNOSTIC,
                "Phase 1E target is diagnostic-only",
                context={
                    "cause_reason_code": REASON_TARGET_DIAGNOSTIC,
                    "phase1e_reason_codes": tuple(plan.reason_codes),
                    "capacity_status": plan.capacity_status,
                    "source_readiness": plan.source_readiness,
                },
            )
        operations = {item.operation_type: item for item in plan.planned_operations}
        source = operations.get(Phase1EPlannedOperationType.SOURCE_RESOLUTION)
        observation = operations.get(Phase1EPlannedOperationType.OBSERVATION_CAPTURE)
        if (
            observation is not None
            and observation.operation_disposition is Phase1EOperationDisposition.DEFERRED
        ):
            raise Phase1GServiceError(
                REASON_OPERATION_DEFERRED,
                "Phase 1E observation capture is explicitly deferred",
                context={
                    "cause_reason_code": REASON_OPERATION_DEFERRED,
                    "phase1e_reason_codes": tuple(plan.reason_codes),
                    "capacity_status": plan.capacity_status,
                    "source_readiness": plan.source_readiness,
                },
            )
        if (
            source is None
            or source.operation_disposition is not Phase1EOperationDisposition.COMPLETE_REQUEST
            or observation is None
            or observation.operation_disposition is not Phase1EOperationDisposition.SEMANTIC_TEMPLATE
            or plan.resource_values_frozen is not True
        ):
            raise Phase1GServiceError(
                REASON_PLAN_INVALID, "Phase 1E plan is not executable for observation capture"
            )

    @staticmethod
    def _capture_plans(plan: Phase1EExecutionPlanProjection) -> tuple[CapturePlan, ...]:
        operation = next(
            (
                item
                for item in plan.planned_operations
                if item.operation_type is Phase1EPlannedOperationType.OBSERVATION_CAPTURE
            ),
            None,
        )
        payload = operation.request_template_payload if operation is not None else None
        raw_plan = payload.get("capture_plan") if isinstance(payload, dict) else None
        if not isinstance(raw_plan, dict):
            raise Phase1GServiceError(REASON_PLAN_INVALID, "capture plan is missing")
        capture_plan = CapturePlan.model_validate(raw_plan)
        if (
            capture_plan.evidence_bundle_hash
            != plan.evidence_binding.phase1_handoff_bundle_hash
        ):
            raise Phase1GServiceError(
                REASON_PLAN_INVALID,
                "capture plan evidence bundle differs from the Phase 1E handoff bundle",
            )
        return (capture_plan,)

    def _assert_capacity(self, snapshot: Phase1GTargetProjectionSnapshot) -> None:
        self._registry.assert_within_bounds(
            planned_candidates=snapshot.projected_candidate_rows,
            planned_bytes=snapshot.projected_bytes,
        )
        if (
            snapshot.projected_candidate_rows > self._trace_policy.max_candidates
            or snapshot.projected_bytes > self._trace_policy.max_bytes
        ):
            raise Phase1GServiceError(
                REASON_PLAN_INVALID,
                "target exceeds frozen trace capture capacity",
                context={
                    "projected_candidates": snapshot.projected_candidate_rows,
                    "projected_bytes": snapshot.projected_bytes,
                },
            )

    def _desired_control(
        self,
        target: Phase1GTargetExecutionRequest,
        capture_plans: tuple[CapturePlan, ...],
    ) -> ControlBindingRequest:
        handoff_hashes = {item.handoff_readiness_hash for item in capture_plans}
        if len(handoff_hashes) != 1:
            raise Phase1GServiceError(REASON_PLAN_INVALID, "capture plan handoff hashes differ")
        payload = {
            "capture_policy_registry_id": self._registry.registry_id,
            "capture_policy_registry_version": self._registry.registry_version,
            "capture_policy_registry_hash": self._registry.registry_hash,
            "capture_policy_id": self._trace_policy.policy_id,
            "capture_policy_version": self._trace_policy.policy_version,
            "capture_policy_hash": self._trace_policy.policy_hash,
            "admission_scope_id": target.admission_scope_id,
            "admission_scope_hash": target.admission_scope_hash,
            "source_projection_contract_version": self._registry.source_resolver.contract_version,
            "source_projection_contract_hash": self._registry.source_resolver.contract_hash,
            "historical_trace_contract_version": self._registry.dse_projection.contract_version,
            "historical_trace_contract_hash": self._registry.dse_projection.contract_hash,
            "observation_writer_contract_version": self._registry.observation_writer.contract_version,
            "observation_writer_contract_hash": self._registry.observation_writer.contract_hash,
            "result_store_policy_hash": target.result_store_policy_hash,
            "max_candidates": self._trace_policy.max_candidates,
            "max_bytes": self._trace_policy.max_bytes,
            "max_capture_ms": self._trace_policy.max_capture_ms,
            "lease_seconds": self._registry.lease_seconds,
            "statement_timeout_ms": self._registry.statement_timeout_ms,
            "lock_timeout_ms": self._registry.lock_timeout_ms,
        }
        return ControlBindingRequest(
            control_type=ControlType.TRACE_CAPTURE,
            environment=target.target_label.value,
            admission_scope_set_hash=canonical_json_sha256(
                {"admission_scope_hashes": [target.admission_scope_hash]}
            ),
            governance_scope_hash=None,
            config_source="advisory_phase1g_g4_service_v1",
            config_payload=payload,
            config_or_store_backend_hash=canonical_json_sha256(payload),
            enabled=True,
            binding_event_revision_no=1,
            predecessor_binding_event_hash=None,
            created_by_service_principal="advisory_phase1g_capture_service",
        )

    def _preview_request(
        self,
        *,
        target: Phase1GTargetExecutionRequest,
        capture_plans: tuple[CapturePlan, ...],
        desired_control: ControlBindingRequest,
        phase1e_plan: Phase1EExecutionPlanProjection,
    ) -> Phase1GCaptureRequestSemanticPreview:
        del desired_control
        binding = self._trace_binding(
            target=target,
            handoff_readiness_hash=capture_plans[0].handoff_readiness_hash,
            control_event_hash="0" * 64,
            capture_batch_id="preview",
            fencing_token=1,
        )
        request = CaptureBatchRequest(
            capture_batch_id="preview", binding=binding, plans=capture_plans
        )
        operation = next(
            item
            for item in phase1e_plan.planned_operations
            if item.operation_type is Phase1EPlannedOperationType.OBSERVATION_CAPTURE
        )
        slots = tuple(
            sorted(str(item.get("slot") or "") for item in operation.required_output_slots)
        )
        unresolved_slots = tuple(
            sorted(
                str(item.get("slot") or "")
                for item in getattr(
                    operation, "unresolved_input_refs", operation.required_output_slots
                )
            )
        )
        if slots != (
            "capture_batch_id",
            "capture_fencing_token",
            "control_binding_event_hash",
        ) or unresolved_slots != slots:
            raise Phase1GServiceError(
                REASON_PLAN_INVALID, "Phase 1E observation output slots are not exact"
            )
        if (
            operation.expected_final_request_hash is not None
            and operation.expected_final_request_hash != request.capture_request_hash
        ):
            raise Phase1GServiceError(
                REASON_PLAN_INVALID, "Phase 1E final observation request hash differs"
            )
        plan_payload = [item.model_dump(mode="json") for item in capture_plans]
        return Phase1GCaptureRequestSemanticPreview(
            capture_request_hash=str(request.capture_request_hash),
            canonical_payload=request.canonical_payload(),
            capture_plan_set_count=len(capture_plans),
            capture_plan_set_hash=canonical_json_sha256(plan_payload),
        )

    def _trace_binding(
        self,
        *,
        target: Phase1GTargetExecutionRequest,
        handoff_readiness_hash: str,
        control_event_hash: str,
        capture_batch_id: str,
        fencing_token: int,
    ) -> TraceCaptureBinding:
        return TraceCaptureBinding(
            control_binding_event_hash=control_event_hash,
            binding_id=f"p1g_trace_{target.admission_scope_hash[:20]}",
            binding_version=self._trace_policy.policy_version,
            handoff_readiness_hash=handoff_readiness_hash,
            admission_scope_id=target.admission_scope_id,
            admission_scope_hash=target.admission_scope_hash,
            capture_batch_id=capture_batch_id,
            capture_fencing_token=fencing_token,
            capture_policy=self._trace_policy,
        )

    def _materialize_request(
        self,
        *,
        loaded: _LoadedTarget,
        event: ControlBindingEvent,
        attempt_no: int,
    ) -> CaptureBatchRequest:
        request_hash = loaded.preview.capture_request_hash
        batch_id = f"acb_{request_hash[:20]}_a{attempt_no}"
        binding = self._trace_binding(
            target=loaded.target_request,
            handoff_readiness_hash=loaded.capture_plans[0].handoff_readiness_hash,
            control_event_hash=event.binding_event_hash,
            capture_batch_id=batch_id,
            fencing_token=1,
        )
        return CaptureBatchRequest(
            capture_batch_id=batch_id,
            binding=binding,
            plans=loaded.capture_plans,
            capture_request_hash=request_hash,
        )

    def _observe_mutable_state(
        self,
        *,
        loaded: _LoadedTarget,
        target: Phase1GTargetExecutionRequest,
        capture_plans: tuple[CapturePlan, ...],
        desired_control: ControlBindingRequest,
        preview: Phase1GCaptureRequestSemanticPreview,
    ) -> tuple[str, str, tuple[str, ...]]:
        chain, head, outboxes = self._read_mutable_facts(
            target=target,
            capture_plans=capture_plans,
            desired_control=desired_control,
            preview=preview,
        )
        self._assert_outboxes_match_frozen(loaded=loaded, outboxes=outboxes)
        return (
            self._binding_state_hash(desired_control, head),
            self._chain_state_hash(chain),
            tuple(sorted(item.envelope.trace_content_hash for item in outboxes)),
        )

    def _read_mutable_facts(
        self,
        *,
        target: Phase1GTargetExecutionRequest,
        capture_plans: tuple[CapturePlan, ...],
        desired_control: ControlBindingRequest,
        preview: Phase1GCaptureRequestSemanticPreview,
    ) -> tuple[tuple[CaptureBatch, ...], ControlBindingEvent | None, tuple[TraceOutboxRecord, ...]]:
        del preview
        with self._readonly_scope() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._apply_read_timeouts(cur)
                try:
                    head = PostgresControlBindingRepository.current_readonly(
                        cur, desired_control.binding_chain_key
                    )
                except SourceLedgerError as exc:
                    if exc.reason_code != REASON_CONTROL_BINDING_UNAVAILABLE:
                        raise
                    head = None
                preview_binding = self._trace_binding(
                    target=target,
                    handoff_readiness_hash=capture_plans[0].handoff_readiness_hash,
                    control_event_hash="0" * 64,
                    capture_batch_id="preview",
                    fencing_token=1,
                )
                preview_request = CaptureBatchRequest(
                    capture_batch_id="preview",
                    binding=preview_binding,
                    plans=capture_plans,
                )
                chain = PostgresCaptureBatchRepository.read_request_chain_exact_readonly(
                    cur, str(preview_request.capture_request_hash)
                )
                outboxes = tuple(
                    record
                    for plan in capture_plans
                    if (
                        record := PostgresTraceOutboxRepository.read_exact_by_natural_key_readonly(
                            cur, self._outbox_natural_key(plan)
                        )
                    )
                    is not None
                )
        return chain, head, outboxes

    def _assert_outboxes_match_frozen(
        self,
        *,
        loaded: _LoadedTarget,
        outboxes: tuple[TraceOutboxRecord, ...],
    ) -> None:
        plans = {self._outbox_natural_key(plan): plan for plan in loaded.capture_plans}
        for record in outboxes:
            natural_key = (
                str(record.envelope.trace_content["selection_identity"]["selection_run_id"]),
                str(record.envelope.trace_content["selection_identity"]["package_id"]),
                str(record.envelope.trace_content["selection_identity"]["manifest_sha256"]),
                str(
                    record.envelope.trace_content["selection_identity"]
                    ["decision_as_of_trade_date"]
                ),
                str(record.binding.capture_policy.policy_hash),
                record.binding.admission_scope_hash,
            )
            plan = plans.get(natural_key)
            if plan is None:
                raise Phase1GServiceError(
                    REASON_BATCH_STATE_CONFLICT,
                    "outbox natural identity is outside the frozen capture plans",
                )
            desired_binding = self._trace_binding(
                target=loaded.target_request,
                handoff_readiness_hash=plan.handoff_readiness_hash,
                control_event_hash="0" * 64,
                capture_batch_id="preview",
                fencing_token=1,
            )
            if self._binding_semantic_payload(
                record.binding
            ) != self._binding_semantic_payload(desired_binding):
                raise Phase1GServiceError(
                    REASON_BATCH_STATE_CONFLICT,
                    "persisted outbox binding differs from the frozen desired config",
                )
            expected = materialize_phase1g_stage_trace_envelope(
                context=self._trace_context(plan, record.binding),
                projection=loaded.snapshot.historical_trace_projection,
            )
            if expected != record.envelope:
                raise Phase1GServiceError(
                    REASON_PLAN_STALE,
                    "persisted outbox content differs from the frozen target projection",
                )

    def _assert_mutable_successor(
        self,
        *,
        target_plan: Phase1GTargetExecutionPlan,
        desired_control: ControlBindingRequest,
        chain: tuple[CaptureBatch, ...],
        current_head: ControlBindingEvent | None,
        outboxes: tuple[TraceOutboxRecord, ...],
    ) -> None:
        current_binding_hash = self._binding_state_hash(desired_control, current_head)
        if not chain and current_binding_hash != target_plan.observed_current_binding_head_hash:
            if current_head is None or not self._event_matches_desired(current_head, desired_control):
                raise Phase1GServiceError(
                    REASON_PLAN_STALE, "control binding changed after planning"
                )
        if chain:
            first = chain[0]
            event = self._read_control_event(first.request.binding.control_binding_event_hash)
            if not self._event_matches_desired(event, desired_control):
                raise Phase1GServiceError(
                    REASON_BATCH_STATE_CONFLICT,
                    "capture chain binding differs from frozen desired config",
                )
        actual_outbox_hashes = tuple(
            sorted(item.envelope.trace_content_hash for item in outboxes)
        )
        baseline = target_plan.observed_outbox_identity_hashes
        if any(value not in actual_outbox_hashes for value in baseline):
            raise Phase1GServiceError(
                REASON_PLAN_STALE, "planned outbox identity disappeared"
            )
        if len(actual_outbox_hashes) > target_plan.expected_capture_plan_set_count:
            raise Phase1GServiceError(
                REASON_BATCH_STATE_CONFLICT, "unexpected Phase 1G outbox identity exists"
            )

    def _select_control_event(
        self,
        *,
        desired: ControlBindingRequest,
        chain: tuple[CaptureBatch, ...],
        current_head: ControlBindingEvent | None,
    ) -> tuple[ControlBindingEvent, bool]:
        if chain:
            event = self._read_control_event(
                chain[0].request.binding.control_binding_event_hash
            )
            if not self._event_matches_desired(event, desired):
                raise Phase1GServiceError(
                    REASON_BATCH_STATE_CONFLICT, "persisted chain binding is divergent"
                )
            return event, False
        before_hash = current_head.binding_event_hash if current_head is not None else None
        with self._transaction_scope() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                event = PostgresControlBindingRepository.get_or_append_exact_in_transaction(
                    cur, desired
                )
        return event, event.binding_event_hash != before_hash

    def _select_or_acquire_batch(
        self, *, request: CaptureBatchRequest, chain: tuple[CaptureBatch, ...]
    ) -> tuple[CaptureBatch, tuple[str, ...]]:
        try:
            phases: list[str] = []
            if not chain:
                batch = self._capture_repository.create(request)
                phases.append("BATCH_CREATED")
            else:
                tail = chain[-1]
                if tail.request.canonical_payload() != request.canonical_payload():
                    raise Phase1GServiceError(
                        REASON_BATCH_STATE_CONFLICT, "capture request chain content differs"
                    )
                if tail.status is CaptureBatchStatus.COMPLETE:
                    return tail, ()
                if tail.status is CaptureBatchStatus.RUNNING:
                    database_now = self._database_now()
                    if (
                        tail.lease_expires_at is not None
                        and tail.lease_expires_at > database_now
                    ):
                        raise Phase1GServiceError(
                            REASON_BATCH_IN_PROGRESS,
                            "capture request is already running",
                        )
                    tail = self._capture_repository.expire(
                        capture_batch_id=tail.request.capture_batch_id,
                        expected_row_version=tail.row_version,
                        fencing_token=tail.fencing_token,
                    )
                    phases.append("BATCH_EXPIRED")
                if tail.status in {
                    CaptureBatchStatus.FAILED,
                    CaptureBatchStatus.EXPIRED,
                    CaptureBatchStatus.ABORTED,
                }:
                    batch = self._capture_repository.recover(
                        request=request,
                        predecessor_capture_batch_id=tail.request.capture_batch_id,
                        expected_predecessor_row_version=tail.row_version,
                        predecessor_fencing_token=tail.fencing_token,
                    )
                    phases.append("BATCH_RECOVERED")
                elif tail.status is CaptureBatchStatus.PLANNED:
                    batch = tail
                else:
                    raise Phase1GServiceError(
                        REASON_BATCH_STATE_CONFLICT,
                        "capture request chain has invalid tail",
                    )
            if batch.status is CaptureBatchStatus.PLANNED:
                batch = self._capture_repository.acquire(
                    capture_batch_id=batch.request.capture_batch_id,
                    expected_row_version=batch.row_version,
                    lease_seconds=self._registry.lease_seconds,
                )
                phases.append("BATCH_ACQUIRED")
            return batch, tuple(phases)
        except SourceLedgerError as exc:
            refreshed = self._latest_batch(str(request.capture_request_hash))
            if refreshed.status is CaptureBatchStatus.COMPLETE:
                return refreshed, False
            if refreshed.status in {
                CaptureBatchStatus.PLANNED,
                CaptureBatchStatus.RUNNING,
            }:
                raise Phase1GServiceError(
                    REASON_BATCH_IN_PROGRESS,
                    "concurrent capture invocation owns the active successor",
                    context={"cause_reason_code": exc.reason_code},
                ) from exc
            raise Phase1GServiceError(
                REASON_BATCH_STATE_CONFLICT,
                "capture state changed concurrently and is not a legal successor",
                context={"cause_reason_code": exc.reason_code},
            ) from exc

    def _build_transactional_target(
        self,
        *,
        target_plan: Phase1GTargetExecutionPlan,
        loaded: _LoadedTarget,
        batch: CaptureBatch,
        capture_plan: CapturePlan,
    ) -> tuple[Phase1GTransactionalTargetInput, Phase1GObservationSemanticDraft]:
        current_binding = self._trace_binding(
            target=target_plan.target_request,
            handoff_readiness_hash=capture_plan.handoff_readiness_hash,
            control_event_hash=batch.request.binding.control_binding_event_hash,
            capture_batch_id=batch.request.capture_batch_id,
            fencing_token=batch.fencing_token,
        )
        existing = self._read_outbox(capture_plan)
        if existing is None:
            persisted_binding = current_binding
            envelope_context = self._trace_context(capture_plan, persisted_binding)
            capture_started = self._monotonic()
            envelope = materialize_phase1g_stage_trace_envelope(
                context=envelope_context,
                projection=loaded.snapshot.historical_trace_projection,
            )
            if (
                self._monotonic() - capture_started
                > self._trace_policy.max_capture_ms / 1000
            ):
                raise Phase1GServiceError(
                    REASON_CAPTURE_TIMEOUT,
                    "stage trace materialization exceeded the frozen trace duration",
                )
        else:
            persisted_binding = existing.binding
            envelope = existing.envelope
        trace_context = self._trace_context(capture_plan, current_binding)
        draft = build_observation_semantic_draft(
            plan=capture_plan, envelope=envelope, binding=persisted_binding
        )
        write_request = Phase1GTransactionalWriteRequest(
            target_request_hash=str(target_plan.target_request.request_hash),
            phase1e_plan_id=target_plan.phase1e_plan_id,
            phase1e_plan_hash=target_plan.phase1e_plan_hash,
            g2_target_projection_snapshot_hash=str(
                loaded.snapshot.target_projection_snapshot_hash
            ),
            capture_batch_id=batch.request.capture_batch_id,
            capture_request_hash=str(batch.request.capture_request_hash),
            capture_attempt_no=batch.capture_attempt_no,
            expected_batch_row_version=batch.row_version,
            capture_fencing_token=batch.fencing_token,
            control_binding_event_hash=current_binding.control_binding_event_hash,
            capture_plan_hash=str(capture_plan.plan_hash),
            trace_capture_context_hash=canonical_json_sha256(
                trace_context.model_dump(mode="json")
            ),
            trace_capture_binding_hash=str(current_binding.binding_hash),
            trace_outbox_id=envelope.trace_outbox_id,
            stage_trace_envelope_hash=envelope.trace_content_hash,
            observation_semantic_key=draft.semantic_observation_key,
            observation_semantic_draft_hash=str(draft.draft_content_hash),
            expected_rows=max(
                target_plan.expected_rows,
                9 + 2 * len(draft.candidate_semantic_rows),
            ),
            expected_bytes=max(target_plan.expected_bytes, envelope.size_bytes),
        )
        return (
            Phase1GTransactionalTargetInput(
                request=write_request,
                target_snapshot=loaded.snapshot,
                capture_plan=capture_plan,
                trace_context=trace_context,
                persisted_binding=persisted_binding,
                current_writer_binding=current_binding,
                envelope=envelope,
                semantic_draft=draft,
            ),
            draft,
        )

    def _read_complete_projections(
        self,
        *,
        target_plan: Phase1GTargetExecutionPlan,
        loaded: _LoadedTarget,
        batch: CaptureBatch,
    ) -> tuple[dict[str, Phase1GTargetCommitProjection], dict[str, Phase1GObservationSemanticDraft]]:
        if batch.status is not CaptureBatchStatus.COMPLETE:
            raise Phase1GServiceError(
                REASON_BATCH_STATE_CONFLICT, "stable result requires COMPLETE batch"
            )
        projections: dict[str, Phase1GTargetCommitProjection] = {}
        drafts: dict[str, Phase1GObservationSemanticDraft] = {}
        for plan in sorted(loaded.capture_plans, key=lambda item: str(item.plan_hash)):
            target_input, draft = self._build_transactional_target(
                target_plan=target_plan,
                loaded=loaded,
                batch=batch,
                capture_plan=plan,
            )
            projections[str(plan.plan_hash)] = (
                self._writer_for_current_budget().read_committed_target(target_input)
            )
            drafts[str(plan.plan_hash)] = draft
        return projections, drafts

    def _build_result(
        self,
        *,
        target_plan: Phase1GTargetExecutionPlan,
        loaded: _LoadedTarget,
        batch: CaptureBatch,
        projections: dict[str, Phase1GTargetCommitProjection],
        drafts: dict[str, Phase1GObservationSemanticDraft],
    ) -> Phase1GCaptureResult:
        if (
            batch.status is not CaptureBatchStatus.COMPLETE
            or batch.capture_receipt_hash is None
            or batch.membership_hash is None
            or batch.membership_count is None
        ):
            raise Phase1GServiceError(
                REASON_BATCH_STATE_CONFLICT, "COMPLETE batch receipt is incomplete"
            )
        replay = loaded.snapshot.source_replay_result
        selected = []
        traces = []
        for plan_hash in sorted(projections):
            projection = projections[plan_hash]
            draft = drafts[plan_hash]
            selected.append(
                Phase1GSelectedObservationMapping(
                    capture_plan_hash=plan_hash,
                    canonical_signal_id=projection.canonical_signal_id,
                    observation_version_id=projection.observation_version_id,
                    observation_content_hash=projection.observation_content_hash,
                    lineage_id=projection.lineage_id,
                    lineage_content_hash=projection.lineage_content_hash,
                    stage_evidence_bundle_hash=str(
                        draft.semantic_observation_payload[
                            "stage_evidence_bundle_hash"
                        ]
                    ),
                    source_revision_set_id=projection.source_revision_set_id,
                    source_revision_set_hash=projection.source_revision_set_hash,
                    trace_outbox_id=projection.trace_outbox_id,
                    trace_content_hash=projection.trace_content_hash,
                )
            )
            traces.append(
                Phase1GTraceOutboxMapping(
                    capture_plan_hash=plan_hash,
                    trace_outbox_id=projection.trace_outbox_id,
                    trace_content_hash=projection.trace_content_hash,
                )
            )
        return Phase1GCaptureResult(
            target_request_hash=str(target_plan.target_request.request_hash),
            phase1f_receipt_hash=target_plan.release_receipt_hash,
            phase1f_catalog_fingerprint=target_plan.release_catalog_fingerprint,
            phase1e_plan_id=target_plan.phase1e_plan_id,
            phase1e_plan_hash=target_plan.phase1e_plan_hash,
            source_resolution_receipt_hash=str(
                replay.replayed_resolution_receipt.source_resolution_receipt_hash
            ),
            source_revision_set_id=replay.source_revision_set.source_revision_set_id,
            source_revision_set_hash=str(
                replay.source_revision_set.source_revision_set_hash
            ),
            control_binding_event_hash=batch.request.binding.control_binding_event_hash,
            capture_batch_id=batch.request.capture_batch_id,
            capture_request_hash=str(batch.request.capture_request_hash),
            capture_attempt_no=batch.capture_attempt_no,
            capture_receipt_hash=batch.capture_receipt_hash,
            membership_count=batch.membership_count,
            membership_hash=batch.membership_hash,
            capture_plan_set_count=target_plan.expected_capture_plan_set_count,
            capture_plan_set_hash=target_plan.expected_capture_plan_set_hash,
            selected_observation_mappings=tuple(selected),
            trace_outbox_mappings=tuple(traces),
        )

    def _publish_attempt_outcome(
        self,
        *,
        target_plan: Phase1GTargetExecutionPlan,
        invocation_id: str,
        started: datetime,
        operation_status: Phase1GOperationStatus,
        reason_codes: tuple[str, ...],
        dml: bool,
        phases: list[str],
        batch: CaptureBatch | None,
        result_artifact: StoredPhase1GArtifact | None,
        error_context: dict[str, Any] | None,
        deadline: float,
    ) -> Phase1GTargetInvocationOutcome:
        receipt = Phase1GAttemptReceipt(
            target_plan_hash=str(target_plan.target_plan_hash),
            target_request_hash=str(target_plan.target_request.request_hash),
            attempt_invocation_id=invocation_id,
            started_at=started,
            finished_at=max(self._utc_now(), started),
            operation_status=(
                Phase1GAttemptStatus.SUCCESS
                if operation_status is Phase1GOperationStatus.SUCCESS
                else Phase1GAttemptStatus.FAILED
            ),
            reason_codes=reason_codes,
            dml_executed=dml,
            committed_phases=tuple(phases),
            capture_batch_id=(batch.request.capture_batch_id if batch else None),
            capture_attempt_no=(batch.capture_attempt_no if batch else None),
            capture_batch_status=(batch.status.value if batch else None),
            capture_result_ref=(result_artifact.ref if result_artifact else None),
            capture_result_hash=(
                result_artifact.ref.semantic_content_hash if result_artifact else None
            ),
            error_context=error_context,
        )
        try:
            self._require_budget(deadline)
            attempt_artifact = self._result_store.publish_attempt(receipt)
        except Phase1GServiceError as exc:
            return Phase1GTargetInvocationOutcome(
                target_request_hash=str(target_plan.target_request.request_hash),
                target_plan_hash=str(target_plan.target_plan_hash),
                operation_status=Phase1GOperationStatus.FAILED,
                reason_codes=(exc.reason_code,),
                dml_executed=dml,
                committed_phases=tuple(phases),
                capture_batch_id=(batch.request.capture_batch_id if batch else None),
                capture_attempt_no=(batch.capture_attempt_no if batch else None),
                capture_batch_status=(batch.status.value if batch else None),
                capture_result_ref=(result_artifact.ref if result_artifact else None),
                capture_result_hash=(
                    result_artifact.ref.semantic_content_hash
                    if result_artifact
                    else None
                ),
                error_context={"cause_reason_code": exc.reason_code},
            )
        except Phase1GResultStoreError:
            logger.exception(
                "phase1g g4 target attempt publication failed",
                extra={"reason_code": REASON_ATTEMPT_RECEIPT_STORE_FAILED},
            )
            return Phase1GTargetInvocationOutcome(
                target_request_hash=str(target_plan.target_request.request_hash),
                target_plan_hash=str(target_plan.target_plan_hash),
                operation_status=Phase1GOperationStatus.FAILED,
                reason_codes=(REASON_ATTEMPT_RECEIPT_STORE_FAILED,),
                dml_executed=dml,
                committed_phases=tuple(phases),
                capture_batch_id=(batch.request.capture_batch_id if batch else None),
                capture_attempt_no=(batch.capture_attempt_no if batch else None),
                capture_batch_status=(batch.status.value if batch else None),
                capture_result_ref=(result_artifact.ref if result_artifact else None),
                capture_result_hash=(
                    result_artifact.ref.semantic_content_hash
                    if result_artifact
                    else None
                ),
                error_context={"cause_reason_code": REASON_ATTEMPT_RECEIPT_STORE_FAILED},
            )
        return Phase1GTargetInvocationOutcome(
            target_request_hash=str(target_plan.target_request.request_hash),
            target_plan_hash=str(target_plan.target_plan_hash),
            operation_status=operation_status,
            reason_codes=reason_codes,
            dml_executed=dml,
            committed_phases=tuple(phases),
            capture_batch_id=(batch.request.capture_batch_id if batch else None),
            capture_attempt_no=(batch.capture_attempt_no if batch else None),
            capture_batch_status=(batch.status.value if batch else None),
            capture_result_ref=(result_artifact.ref if result_artifact else None),
            capture_result_hash=(
                result_artifact.ref.semantic_content_hash if result_artifact else None
            ),
            attempt_receipt_ref=attempt_artifact.ref,
            attempt_receipt_hash=attempt_artifact.ref.semantic_content_hash,
            error_context=error_context,
        )

    def _read_control_event(self, event_hash: str) -> ControlBindingEvent:
        with self._readonly_scope() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._apply_read_timeouts(cur)
                return PostgresControlBindingRepository.read_exact_readonly(
                    cur, event_hash
                )

    def _read_outbox(self, plan: CapturePlan) -> TraceOutboxRecord | None:
        with self._readonly_scope() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._apply_read_timeouts(cur)
                return PostgresTraceOutboxRepository.read_exact_by_natural_key_readonly(
                    cur, self._outbox_natural_key(plan)
                )

    def _latest_batch(self, request_hash: str) -> CaptureBatch:
        with self._readonly_scope() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._apply_read_timeouts(cur)
                chain = PostgresCaptureBatchRepository.read_request_chain_exact_readonly(
                    cur, request_hash
                )
        if not chain:
            raise Phase1GServiceError(
                REASON_BATCH_STATE_CONFLICT, "capture request chain disappeared"
            )
        return chain[-1]

    def _database_now(self) -> datetime:
        with self._readonly_scope() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._apply_read_timeouts(cur)
                cur.execute("SELECT clock_timestamp() AS database_now")
                return cur.fetchone()["database_now"]

    def _trace_context(
        self, plan: CapturePlan, binding: TraceCaptureBinding
    ) -> TraceCaptureContext:
        return TraceCaptureContext(
            selection_run_id=plan.selection_run_id,
            package_id=plan.package_id,
            manifest_sha256=plan.manifest_sha256,
            decision_as_of_trade_date=plan.decision_as_of_trade_date,
            data_source="DB_HISTORICAL",
            execution_origin="ADVISORY_RUN",
            research_scope="HISTORICAL_RESEARCH_ONLY",
            execution_prohibited=True,
            binding=binding,
        )

    def _outbox_natural_key(
        self, plan: CapturePlan
    ) -> tuple[str, str, str, str, str, str]:
        return (
            plan.selection_run_id,
            plan.package_id,
            plan.manifest_sha256,
            plan.decision_as_of_trade_date,
            str(self._trace_policy.policy_hash),
            plan.admission_scope_hash,
        )

    @staticmethod
    def _event_matches_desired(
        event: ControlBindingEvent, desired: ControlBindingRequest
    ) -> bool:
        request = event.request
        return (
            request.binding_chain_key == desired.binding_chain_key
            and request.config_source == desired.config_source
            and request.config_or_store_backend_hash
            == desired.config_or_store_backend_hash
            and canonicalize(request.config_payload)
            == canonicalize(desired.config_payload)
            and request.enabled is True
            and request.created_by_service_principal
            == desired.created_by_service_principal
        )

    @staticmethod
    def _binding_semantic_payload(binding: TraceCaptureBinding) -> dict[str, Any]:
        return canonicalize(
            binding.model_dump(
                mode="json",
                exclude={
                    "control_binding_event_hash",
                    "capture_batch_id",
                    "capture_fencing_token",
                    "binding_hash",
                },
            )
        )

    @staticmethod
    def _binding_state_hash(
        desired: ControlBindingRequest, event: ControlBindingEvent | None
    ) -> str:
        return (
            event.binding_event_hash
            if event is not None
            else canonical_json_sha256(
                {"binding_chain_key": desired.binding_chain_key, "state": "ABSENT"}
            )
        )

    @staticmethod
    def _chain_state_hash(chain: tuple[CaptureBatch, ...]) -> str:
        return canonical_json_sha256(
            [
                {
                    "capture_batch_id": item.request.capture_batch_id,
                    "capture_request_hash": item.request.capture_request_hash,
                    "capture_attempt_no": item.capture_attempt_no,
                    "predecessor_capture_batch_id": item.predecessor_capture_batch_id,
                    "status": item.status.value,
                    "row_version": item.row_version,
                    "fencing_token": item.fencing_token,
                    "lease_expires_at": item.lease_expires_at,
                    "membership_count": item.membership_count,
                    "membership_hash": item.membership_hash,
                    "capture_receipt_hash": item.capture_receipt_hash,
                    "reason_codes": item.reason_codes,
                    "request": item.request.model_dump(mode="json"),
                }
                for item in chain
            ]
        )

    @staticmethod
    def _expected_rows(snapshot: Phase1GTargetProjectionSnapshot) -> int:
        return 9 + 2 * snapshot.projected_stage_rows

    def _require_budget(self, deadline: float) -> None:
        if self._monotonic() >= deadline:
            raise Phase1GServiceError(
                REASON_CAPTURE_TIMEOUT, "Phase 1G target capture deadline expired"
            )

    def _apply_read_timeouts(self, cur: Any) -> None:
        statement_timeout_ms, lock_timeout_ms = self._timeout_pair()
        cur.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            (str(statement_timeout_ms),),
        )
        cur.execute(
            "SELECT set_config('lock_timeout', %s, true)",
            (str(lock_timeout_ms),),
        )

    def _timeout_pair(self) -> tuple[int, int]:
        deadline_context = getattr(self, "_capture_deadline", None)
        deadline = deadline_context.get() if deadline_context is not None else None
        if deadline is None:
            return (
                self._registry.statement_timeout_ms,
                self._registry.lock_timeout_ms,
            )
        remaining_ms = math.ceil((deadline - self._monotonic()) * 1000)
        if remaining_ms <= 0:
            raise Phase1GServiceError(
                REASON_CAPTURE_TIMEOUT, "Phase 1G target capture deadline expired"
            )
        statement_timeout_ms = min(
            self._registry.statement_timeout_ms, remaining_ms
        )
        return (
            statement_timeout_ms,
            min(self._registry.lock_timeout_ms, statement_timeout_ms),
        )

    def _writer_for_current_budget(self) -> Phase1GTransactionalWriter:
        statement_timeout_ms, lock_timeout_ms = self._timeout_pair()
        return Phase1GTransactionalWriter(
            transaction_connection_factory=self._transaction_connection_factory,
            readonly_connection_factory=self._readonly_connection_factory,
            statement_timeout_ms=statement_timeout_ms,
            lock_timeout_ms=lock_timeout_ms,
        )

    @contextmanager
    def _failure_fencing_scope(self, *, timeout_failure: bool) -> Iterator[None]:
        if not timeout_failure:
            yield
            return
        token = self._capture_deadline.set(None)
        try:
            yield
        finally:
            self._capture_deadline.reset(token)

    @contextmanager
    def _readonly_scope(self) -> Iterator[Any]:
        conn = self._readonly_connection_factory()
        try:
            if bool(getattr(conn, "autocommit", False)):
                raise Phase1GServiceError(
                    REASON_UNEXPECTED_ERROR, "read-only connection cannot autocommit"
                )
            conn.set_session(
                readonly=True, autocommit=False, isolation_level="REPEATABLE READ"
            )
            yield conn
            conn.rollback()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                logger.exception("phase1g g4 read-only rollback failed")
            raise
        finally:
            conn.close()

    @contextmanager
    def _transaction_scope(self) -> Iterator[Any]:
        conn = self._transaction_connection_factory()
        try:
            if bool(getattr(conn, "autocommit", False)):
                raise Phase1GServiceError(
                    REASON_UNEXPECTED_ERROR, "transaction connection cannot autocommit"
                )
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                statement_timeout_ms, lock_timeout_ms = self._timeout_pair()
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (str(statement_timeout_ms),),
                )
                cur.execute(
                    "SELECT set_config('lock_timeout', %s, true)",
                    (str(lock_timeout_ms),),
                )
            yield conn
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                logger.exception("phase1g g4 transaction rollback failed")
            raise
        finally:
            conn.close()

    def _utc_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise Phase1GServiceError(
                REASON_UNEXPECTED_ERROR, "now provider must return timezone-aware UTC"
            )
        return value.astimezone(timezone.utc)

    @staticmethod
    def _reason_context(exc: Exception) -> tuple[str, dict[str, Any]]:
        if isinstance(
            exc,
            (psycopg2.errors.QueryCanceled, psycopg2.errors.LockNotAvailable),
        ) or str(getattr(exc, "pgcode", "")) in {"57014", "55P03"}:
            reason = REASON_CAPTURE_TIMEOUT
        else:
            reason = str(getattr(exc, "reason_code", REASON_UNEXPECTED_ERROR))
        cause = getattr(exc, "context", {})
        context: dict[str, Any] = {"cause_reason_code": reason}
        if isinstance(cause, dict):
            for key in (
                "field_name",
                "conflict_kind",
                "target_label",
                "cause_reason_code",
                "exception_type",
                "phase1e_reason_codes",
                "capacity_status",
                "source_readiness",
            ):
                if key in cause:
                    context[key] = cause[key]
        if reason == REASON_UNEXPECTED_ERROR:
            context["exception_type"] = type(exc).__name__
        return reason, canonicalize(context)

    @staticmethod
    def _exit_class(
        outcomes: tuple[Phase1GTargetInvocationOutcome, ...],
        batch_artifact: StoredPhase1GArtifact | None,
        durable: bool,
    ) -> Phase1GExitClass:
        if not durable or (durable and batch_artifact is None):
            return Phase1GExitClass.INFRASTRUCTURE_FAILURE
        succeeded = sum(
            item.operation_status is Phase1GOperationStatus.SUCCESS
            for item in outcomes
        )
        if succeeded == len(outcomes):
            return Phase1GExitClass.SUCCESS
        if succeeded:
            return Phase1GExitClass.PARTIAL_FAILURE
        infrastructure_reasons = {
            REASON_RESULT_STORE_FAILED,
            REASON_ATTEMPT_RECEIPT_STORE_FAILED,
            REASON_BATCH_RECEIPT_STORE_FAILED,
        }
        if any(
            set(item.reason_codes) & infrastructure_reasons
            or any(reason.endswith("UNEXPECTED_ERROR") for reason in item.reason_codes)
            for item in outcomes
        ):
            return Phase1GExitClass.INFRASTRUCTURE_FAILURE
        return Phase1GExitClass.BUSINESS_FAILURE
