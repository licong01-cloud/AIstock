from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from backend.services.multi_alpha.combine_backtest import delta, metric_columns
from backend.services.multi_alpha.durable_execution_adapter import (
    DurableChildNotComputable,
    DurablePublishedArtifacts,
    DurableSubmissionIntent,
    QEWorkspacePredBacktestAdapter,
)
from backend.services.multi_alpha.durable_cancellation import (
    DurableCancellationDeliveryWorker,
)
from backend.services.multi_alpha.durable_control import DurableMultiAlphaControlService
from backend.services.multi_alpha.durable_models import (
    DurableRunSpec,
    OwnershipToken,
    artifact_manifest_hash_for,
    make_attempt_id,
)
from backend.services.multi_alpha.durable_plan import DeterministicChildPlanner
from backend.services.multi_alpha.durable_recovery import DurableRecoveryWorker
from backend.services.multi_alpha.durable_repository import (
    TERMINAL_CHILD_STATUSES,
    MultiAlphaDurableRepository,
    MultiAlphaDurableRepositoryError,
)
from backend.services.multi_alpha.durable_runtime_health import (
    heartbeat_durable_orchestrator,
    mark_durable_orchestrator_ready,
    mark_durable_orchestrator_starting,
    mark_durable_orchestrator_stopped,
    mark_durable_orchestrator_unavailable,
)
from backend.services.qe_archive.event_capture import QEArchiveEventCapture
from backend.services.quantevolver.qe_active_execution_capacity import (
    QEActiveExecutionImportService,
    QEWorkspaceSubmissionOutcome,
)


logger = logging.getLogger(__name__)

ACTIVE_REMOTE_STATUSES = frozenset(
    {"reserved", "started", "queued", "pending", "submitted", "running", "processing"}
)
REMOTE_FAILURE_STATUSES = frozenset({"failed", "error"})
REMOTE_CANCELLED_STATUSES = frozenset({"cancelled", "canceled", "interrupted"})
TERMINAL_RUN_STATUSES = frozenset(
    {"succeeded", "partial_failed", "partial_recovered", "failed", "cancelled"},
)
RETRYABLE_RESULT_COLLECTION_REASON_CODES = frozenset(
    {
        "multi_alpha_child_result_not_visible",
        "qe_workspace_result_transport_unavailable",
    }
)
RETRYABLE_TERMINAL_RESERVATION_REASON_CODES = frozenset(
    {
        "qe_execution_reservation_owner_mismatch",
        "qe_execution_reservation_stale_owner",
        "qe_execution_reservation_stale_row_version",
        "qe_execution_reservation_lease_expired",
        "qe_execution_reservation_cas_failed",
    }
)


class DurableOrchestratorError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class DurableOrchestratorConfig:
    poll_seconds: float = 2.0
    lease_seconds: int = 600
    heartbeat_seconds: float = 30.0
    items_per_pass: int = 8
    archive_batch_size: int = 200

    def __post_init__(self) -> None:
        if not 0.2 <= self.poll_seconds <= 60.0:
            raise DurableOrchestratorError(
                "durable orchestrator poll interval must be between 0.2 and 60 seconds",
                reason_code="multi_alpha_durable_config_invalid",
                context={"poll_seconds": self.poll_seconds},
            )
        if not 60 <= self.lease_seconds <= 3600:
            raise DurableOrchestratorError(
                "durable orchestrator lease must be between 60 and 3600 seconds",
                reason_code="multi_alpha_durable_config_invalid",
                context={"lease_seconds": self.lease_seconds},
            )
        if not 5.0 <= self.heartbeat_seconds < self.lease_seconds / 2:
            raise DurableOrchestratorError(
                "durable orchestrator heartbeat must be at least 5 seconds and shorter than half the lease",
                reason_code="multi_alpha_durable_config_invalid",
                context={
                    "heartbeat_seconds": self.heartbeat_seconds,
                    "lease_seconds": self.lease_seconds,
                },
            )
        if not 1 <= self.items_per_pass <= 100:
            raise DurableOrchestratorError(
                "durable orchestrator items_per_pass must be between 1 and 100",
                reason_code="multi_alpha_durable_config_invalid",
                context={"items_per_pass": self.items_per_pass},
            )
        if not 1 <= self.archive_batch_size <= 1000:
            raise DurableOrchestratorError(
                "durable orchestrator archive batch must be between 1 and 1000",
                reason_code="multi_alpha_durable_config_invalid",
                context={"archive_batch_size": self.archive_batch_size},
            )

    @classmethod
    def from_env(cls) -> DurableOrchestratorConfig:
        return cls(
            poll_seconds=_env_float(
                "AISTOCK_MULTI_ALPHA_DURABLE_POLL_SECONDS",
                2.0,
            ),
            lease_seconds=_env_int(
                "AISTOCK_MULTI_ALPHA_DURABLE_LEASE_SECONDS",
                600,
            ),
            heartbeat_seconds=_env_float(
                "AISTOCK_MULTI_ALPHA_DURABLE_HEARTBEAT_SECONDS",
                30.0,
            ),
            items_per_pass=_env_int(
                "AISTOCK_MULTI_ALPHA_DURABLE_ITEMS_PER_PASS",
                8,
            ),
            archive_batch_size=_env_int(
                "AISTOCK_MULTI_ALPHA_DURABLE_ARCHIVE_BATCH_SIZE",
                200,
            ),
        )


@dataclass(frozen=True)
class DurableOrchestratorCycleResult:
    planned_runs: int
    dispatched_attempts: int
    reconciled_attempts: int
    finalized_runs: int
    archive_events: int
    applied_control_commands: int = 0
    executed_recovery_commands: int = 0
    delivered_cancellations: int = 0
    paused_runs: int = 0

    @property
    def work_count(self) -> int:
        return (
            self.planned_runs
            + self.dispatched_attempts
            + self.reconciled_attempts
            + self.finalized_runs
            + self.archive_events
            + self.applied_control_commands
            + self.executed_recovery_commands
            + self.delivered_cancellations
            + self.paused_runs
        )


class DurableBusinessResultAssembler:
    """Write existing scheme/LOO business rows without duplicating metric formulas."""

    def __init__(
        self,
        *,
        repository: MultiAlphaDurableRepository,
        adapter: QEWorkspacePredBacktestAdapter,
    ) -> None:
        self._repository = repository
        self._adapter = adapter

    def assemble_child(
        self,
        *,
        run: Mapping[str, Any],
        child: Mapping[str, Any],
        children: Sequence[Mapping[str, Any]],
    ) -> bool:
        if str(child.get("status") or "") != "reconciling":
            return False
        selected_attempt_id = str(child.get("selected_attempt_id") or "").strip()
        if not selected_attempt_id:
            raise DurableOrchestratorError(
                "reconciling child is missing selected_attempt_id",
                reason_code="multi_alpha_selected_attempt_required",
                context={"child_id": child.get("child_id")},
            )
        business_input = self._load_child_metrics(
            run_id=str(run["id"]),
            child=child,
            attempt_id=selected_attempt_id,
        )
        metrics = business_input.metrics
        child_kind = str(child["child_kind"])
        if child_kind == "baseline":
            self._repository.transition_child_with_event(
                str(child["child_id"]),
                expected_statuses=("reconciling",),
                next_status="succeeded",
                phase="business_result_persisted",
                selected_attempt_id=selected_attempt_id,
                event_payload={"business_result_kind": "baseline"},
            )
            return True

        if child_kind == "scheme":
            baseline_metrics = self._baseline_metrics(
                run=run,
                children=children,
            )
            if baseline_metrics is _WAITING:
                return False
            if baseline_metrics is _UNAVAILABLE:
                error = {
                    "reason_code": "scheme_baseline_unavailable",
                    "message": "requested baseline child did not produce a successful result",
                    "context": {"baseline_leg_id": run.get("baseline_leg_id")},
                }
                metadata = business_input.materialization_metadata
                self._repository.finalize_scheme_child_without_result(
                    str(child["child_id"]),
                    expected_statuses=("reconciling",),
                    next_status="not_computable",
                    reason_code="scheme_baseline_unavailable",
                    error=error,
                    weights=_mapping(metadata.get("weights")),
                    per_window_weights=_mapping_sequence(
                        metadata.get("per_window_weights")
                    ),
                    selected_attempt_id=selected_attempt_id,
                )
                return True
            metadata = business_input.materialization_metadata
            result = {
                "weighting_scheme": child["weighting_scheme"],
                "weights_json": _mapping(metadata.get("weights")),
                "per_window_weights_json": _mapping_sequence(
                    metadata.get("per_window_weights")
                ),
                **metric_columns(metrics),
                "vs_baseline_sharpe_delta": delta(
                    metrics.get("sharpe"),
                    baseline_metrics.get("sharpe") if isinstance(baseline_metrics, Mapping) else None,
                ),
                "vs_baseline_calmar_delta": delta(
                    metrics.get("calmar"),
                    baseline_metrics.get("calmar") if isinstance(baseline_metrics, Mapping) else None,
                ),
                "pred_persisted": bool(metrics.get("pred_persisted")),
                "skipped": False,
                "skipped_reason": None,
            }
            self._repository.finalize_scheme_child_result(
                str(child["child_id"]),
                selected_attempt_id=selected_attempt_id,
                result=result,
            )
            return True

        if child_kind == "loo":
            full_scheme = next(
                (
                    item
                    for item in children
                    if item.get("child_kind") == "scheme"
                    and item.get("weighting_scheme") == child.get("weighting_scheme")
                ),
                None,
            )
            if full_scheme is None:
                raise DurableOrchestratorError(
                    "planned LOO child has no matching full scheme child",
                    reason_code="multi_alpha_loo_full_scheme_missing",
                    context={"child_id": child.get("child_id")},
                )
            full_status = str(full_scheme.get("status") or "")
            if full_status not in TERMINAL_CHILD_STATUSES:
                return False
            if full_status != "succeeded":
                self._repository.transition_child_with_event(
                    str(child["child_id"]),
                    expected_statuses=("reconciling",),
                    next_status="not_computable",
                    phase="loo_full_scheme_unavailable",
                    selected_attempt_id=selected_attempt_id,
                    reason_code="loo_full_scheme_unavailable",
                    event_payload={
                        "weighting_scheme": child.get("weighting_scheme"),
                        "full_scheme_status": full_status,
                    },
                )
                return True
            full_attempt_id = str(full_scheme.get("selected_attempt_id") or "").strip()
            if not full_attempt_id:
                raise DurableOrchestratorError(
                    "successful full scheme is missing selected attempt",
                    reason_code="multi_alpha_selected_attempt_required",
                    context={"child_id": full_scheme.get("child_id")},
                )
            full_business_input = self._load_child_metrics(
                run_id=str(run["id"]),
                child=full_scheme,
                attempt_id=full_attempt_id,
            )
            full_metrics = full_business_input.metrics
            self._repository.finalize_loo_child_result(
                str(child["child_id"]),
                selected_attempt_id=selected_attempt_id,
                result={
                    "weighting_scheme": child["weighting_scheme"],
                    "dropped_leg_id": child["dropped_leg_id"],
                    "marginal_sharpe": delta(
                        full_metrics.get("sharpe"),
                        metrics.get("sharpe"),
                    ),
                    "marginal_calmar": delta(
                        full_metrics.get("calmar"),
                        metrics.get("calmar"),
                    ),
                    "marginal_cagr": delta(
                        full_metrics.get("cagr"),
                        metrics.get("cagr"),
                    ),
                },
            )
            return True

        raise DurableOrchestratorError(
            "durable child kind is unsupported by the business result assembler",
            reason_code="multi_alpha_child_kind_invalid",
            context={"child_id": child.get("child_id"), "child_kind": child_kind},
        )

    def _baseline_metrics(
        self,
        *,
        run: Mapping[str, Any],
        children: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any] | object:
        if not run.get("baseline_leg_id"):
            return {}
        baseline = next(
            (item for item in children if item.get("child_kind") == "baseline"),
            None,
        )
        if baseline is None:
            raise DurableOrchestratorError(
                "run requests a baseline but the deterministic child plan has none",
                reason_code="multi_alpha_baseline_child_missing",
                context={"run_id": run.get("id")},
            )
        status = str(baseline.get("status") or "")
        if status not in TERMINAL_CHILD_STATUSES:
            return _WAITING
        if status != "succeeded":
            return _UNAVAILABLE
        selected_attempt_id = str(baseline.get("selected_attempt_id") or "").strip()
        if not selected_attempt_id:
            raise DurableOrchestratorError(
                "successful baseline child is missing selected attempt",
                reason_code="multi_alpha_selected_attempt_required",
                context={"child_id": baseline.get("child_id")},
            )
        business_input = self._load_child_metrics(
            run_id=str(run["id"]),
            child=baseline,
            attempt_id=selected_attempt_id,
        )
        return business_input.metrics

    def _load_child_metrics(
        self,
        *,
        run_id: str,
        child: Mapping[str, Any],
        attempt_id: str,
    ) -> DurableBusinessInput:
        attempt = self._repository.get_attempt(attempt_id)
        if attempt is None or str(attempt.get("status") or "") != "succeeded":
            raise DurableOrchestratorError(
                "selected child attempt is missing or not succeeded",
                reason_code="multi_alpha_business_result_attempt_not_succeeded",
                context={
                    "child_id": child.get("child_id"),
                    "attempt_id": attempt_id,
                    "attempt_status": attempt.get("status") if attempt else None,
                },
            )
        execution_kind = str(attempt.get("execution_kind") or "remote_execution")
        if execution_kind in {"reference_result", "derived_result"}:
            result_manifest = _mapping(attempt.get("result_manifest_json"))
            expected_result_hash = artifact_manifest_hash_for(result_manifest)
            if str(attempt.get("result_manifest_hash") or "") != expected_result_hash:
                raise DurableOrchestratorError(
                    "reference/derived selected attempt result manifest hash is invalid",
                    reason_code="multi_alpha_reference_result_manifest_mismatch",
                    context={"attempt_id": attempt_id},
                )
            metrics = result_manifest.get("metrics")
            metadata = result_manifest.get("materialization_metadata")
            if not isinstance(metrics, Mapping) or not isinstance(metadata, Mapping):
                raise DurableOrchestratorError(
                    "reference/derived selected attempt lacks frozen metrics or materialization metadata",
                    reason_code="multi_alpha_reference_result_payload_missing",
                    context={"attempt_id": attempt_id, "execution_kind": execution_kind},
                )
            return DurableBusinessInput(
                metrics=dict(metrics),
                materialization_metadata=dict(metadata),
            )
        artifacts = self._adapter.load_published_artifacts(
            run_id=run_id,
            child_id=str(child["child_id"]),
            attempt_id=attempt_id,
        )
        return DurableBusinessInput(
            metrics=self._adapter.load_collected_metrics(artifacts),
            materialization_metadata=self._adapter.load_materialization_metadata(artifacts),
        )


_WAITING = object()
_UNAVAILABLE = object()


@dataclass(frozen=True)
class DurableBusinessInput:
    """Metrics and frozen materialization metadata for one selected attempt."""

    metrics: Mapping[str, Any]
    materialization_metadata: Mapping[str, Any]


class DurableMultiAlphaOrchestrator:
    """QE-only durable planner, dispatcher, reconciler, finalizer and archive pass."""

    def __init__(
        self,
        *,
        repository: MultiAlphaDurableRepository | None = None,
        planner: DeterministicChildPlanner | None = None,
        adapter: QEWorkspacePredBacktestAdapter | None = None,
        archive_capture: QEArchiveEventCapture | None = None,
        active_import_service: QEActiveExecutionImportService | None = None,
        control_service: DurableMultiAlphaControlService | None = None,
        cancellation_delivery_worker: DurableCancellationDeliveryWorker | None = None,
        recovery_worker: DurableRecoveryWorker | None = None,
        config: DurableOrchestratorConfig | None = None,
        owner_id: str | None = None,
    ) -> None:
        self._repository = repository or MultiAlphaDurableRepository()
        self._planner = planner or DeterministicChildPlanner(self._repository)
        self._adapter = adapter or QEWorkspacePredBacktestAdapter(
            repository=self._repository
        )
        self._business_assembler = DurableBusinessResultAssembler(
            repository=self._repository,
            adapter=self._adapter,
        )
        self._archive_capture = archive_capture or QEArchiveEventCapture()
        self._active_import_service = active_import_service or QEActiveExecutionImportService()
        self._control_service = control_service or DurableMultiAlphaControlService(
            self._repository,
        )
        self._cancellation_delivery_worker = (
            cancellation_delivery_worker
            or DurableCancellationDeliveryWorker(repository=self._repository)
        )
        self._recovery_worker = recovery_worker or DurableRecoveryWorker(
            repository=self._repository,
            adapter=self._adapter,
        )
        self._config = config or DurableOrchestratorConfig.from_env()
        self._owner_id = owner_id or (
            f"macb-worker:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
        )
        self._activation_import_completed = False
        self._p0_2_schema_ready = False
        self._last_claimed_run_id: str | None = None
        self._last_claimed_attempt_id: str | None = None
        self._last_claimed_command_id: str | None = None
        self._last_claimed_delivery_id: str | None = None

    @property
    def owner_id(self) -> str:
        return self._owner_id

    async def initialize(self) -> Mapping[str, Any]:
        baseline_health = await asyncio.to_thread(
            self._repository.preflight_schema,
            raise_on_error=True,
        )
        p0_2_health = await asyncio.to_thread(
            self._repository.preflight_p0_2_schema,
            raise_on_error=False,
        )
        self._p0_2_schema_ready = bool(p0_2_health.ready)
        imported = await self._active_import_service.import_current_active_sources_verified()
        self._activation_import_completed = True
        result = {
            "schema_health": baseline_health.as_dict(),
            "p0_2_schema_health": p0_2_health.as_dict(),
            "active_execution_import": {
                "discovered_count": imported.discovered_count,
                "imported_count": imported.imported_count,
                "deduplicated_count": imported.deduplicated_count,
                "unresolved": [dict(item) for item in imported.unresolved],
                "queue_only_nodes": {
                    node_id: [dict(item) for item in diagnostics]
                    for node_id, diagnostics in imported.queue_only_nodes.items()
                },
            },
        }
        if imported.unresolved:
            logger.error(
                "QE active execution import has unresolved identities: %s",
                result["active_execution_import"],
            )
        else:
            logger.info("QE active execution import completed: %s", result)
        return result

    async def run_cycle(self) -> DurableOrchestratorCycleResult:
        if not self._activation_import_completed:
            await self.initialize()
        if self._p0_2_schema_ready:
            control = await self._run_control_pass()
            recoveries = await self._run_recovery_pass()
            cancellations = await self._run_cancel_delivery_pass()
        else:
            # P0-2 is additive.  Before its separately deployed DDL exists the
            # already-live P0-1B planner/dispatcher/reconciler must continue to
            # run; only the P0-2 command consumers remain unavailable.
            control = 0
            recoveries = 0
            cancellations = 0
        planned = await self._run_bounded_pass(self.planner_pass_once)
        dispatched = await self._run_attempt_pass(self.dispatch_pass_once)
        cancel_reconciled = (
            await self._run_attempt_pass(self.cancel_reconcile_pass_once)
            if self._p0_2_schema_ready
            else 0
        )
        reconciled = cancel_reconciled + await self._run_attempt_pass(self.reconcile_pass_once)
        paused = await self._run_pause_drain_pass() if self._p0_2_schema_ready else 0
        finalized = await self._run_finalizer_pass()
        archived = await self.archive_pass()
        return DurableOrchestratorCycleResult(
            planned_runs=planned,
            dispatched_attempts=dispatched,
            reconciled_attempts=reconciled,
            finalized_runs=finalized,
            archive_events=archived,
            applied_control_commands=control,
            executed_recovery_commands=recoveries,
            delivered_cancellations=cancellations,
            paused_runs=paused,
        )

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        stale_after_seconds = max(
            60,
            self._config.lease_seconds * 2,
            int(self._config.poll_seconds * 4),
        )
        mark_durable_orchestrator_starting(
            owner_id=self._owner_id,
            stale_after_seconds=stale_after_seconds,
        )
        try:
            while not stop_event.is_set():
                if not self._activation_import_completed:
                    try:
                        await self.initialize()
                        mark_durable_orchestrator_ready(owner_id=self._owner_id)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        error = _exception_payload(exc)
                        mark_durable_orchestrator_unavailable(
                            status="starting",
                            error=error,
                        )
                        logger.exception(
                            "multi_alpha_durable_initialization_unavailable; retrying: %s",
                            error,
                        )
                        try:
                            await asyncio.wait_for(
                                stop_event.wait(),
                                timeout=self._config.poll_seconds,
                            )
                            break
                        except asyncio.TimeoutError:
                            continue
                try:
                    cycle = await self.run_cycle()
                    mark_durable_orchestrator_ready(owner_id=self._owner_id)
                    if cycle.work_count:
                        logger.info("multi-alpha durable cycle completed: %s", cycle)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    error = _exception_payload(exc)
                    heartbeat_durable_orchestrator(error=error)
                    logger.exception(
                        "multi-alpha durable cycle failed without changing remote execution ownership: %s",
                        error,
                    )
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self._config.poll_seconds,
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            mark_durable_orchestrator_stopped()

    async def planner_pass_once(self, *, excluded_run_ids: Sequence[str] = ()) -> bool:
        run = await asyncio.to_thread(
            self._repository.claim_next_run,
            owner_id=self._owner_id,
            lease_seconds=self._config.lease_seconds,
            statuses=("queued", "preparing"),
            excluded_run_ids=excluded_run_ids,
        )
        if run is None:
            return False
        run_id = str(run["id"])
        self._last_claimed_run_id = run_id
        token = _ownership_token(run)
        try:
            if run["status"] == "queued":
                run = await asyncio.to_thread(
                    self._repository.transition_run_with_event,
                    run_id,
                    token=token,
                    expected_statuses=("queued",),
                    next_status="preparing",
                    phase="planning_children",
                    progress={"planner": "deterministic_child_plan"},
                )
                token = _ownership_token(run)
            run_spec = _run_spec_from_row(run)
            request = self._adapter.request_from_run(run)
            plan = await asyncio.to_thread(
                self._planner.plan,
                run_spec=run_spec,
                request=request,
            )
            node_id = str(request.backtest_config.get("node_id") or "wsl2-5080")
            for planned_child in plan.children:
                if not await self._planner_parent_allows_progress(run_id=run_id):
                    await self._yield_run_if_owned(
                        run_id=run_id,
                        token=token,
                        phase="planner_stopped_by_control",
                    )
                    return True
                child = self._repository.get_child(str(planned_child["child_id"]))
                if child is None:
                    raise DurableOrchestratorError(
                        "planned durable child disappeared before materialization",
                        reason_code="multi_alpha_durable_child_not_found",
                        context={"child_id": planned_child["child_id"]},
                    )
                if child["status"] not in {"pending", "materializing"}:
                    continue
                if child["status"] == "pending":
                    child = await asyncio.to_thread(
                        self._repository.transition_child_with_event,
                        str(child["child_id"]),
                        expected_statuses=("pending",),
                        next_status="materializing",
                        phase="materializing",
                    )
                attempt_id = make_attempt_id(str(child["child_id"]), 1)
                try:
                    materialization, token, operation_error = await self._run_sync_with_run_heartbeat(
                        run_id=run_id,
                        token=token,
                        operation=lambda: self._adapter.materialize_child_input(
                            run_id=run_id,
                            child_id=str(child["child_id"]),
                            attempt_id=attempt_id,
                        ),
                    )
                    if operation_error is not None:
                        raise operation_error
                except DurableChildNotComputable as exc:
                    await asyncio.to_thread(
                        self._terminalize_unexecutable_child,
                        child=child,
                        next_status="not_computable",
                        error=_exception_payload(exc),
                    )
                    continue
                except Exception as exc:
                    await asyncio.to_thread(
                        self._terminalize_unexecutable_child,
                        child=child,
                        next_status="failed",
                        error=_exception_payload(exc),
                    )
                    continue
                if not await self._planner_parent_allows_progress(run_id=run_id):
                    await self._defer_materialization_for_control(
                        child_id=str(child["child_id"]),
                        run_id=run_id,
                        phase="materialization_completed_after_control",
                    )
                    await self._yield_run_if_owned(
                        run_id=run_id,
                        token=token,
                        phase="planner_stopped_by_control",
                    )
                    return True
                try:
                    artifacts, token, operation_error = await self._run_sync_with_run_heartbeat(
                        run_id=run_id,
                        token=token,
                        operation=lambda: self._adapter.publish_artifacts(materialization),
                    )
                    if operation_error is not None:
                        raise operation_error
                except Exception as exc:
                    await asyncio.to_thread(
                        self._terminalize_unexecutable_child,
                        child=child,
                        next_status="failed",
                        error=_exception_payload(exc),
                    )
                    continue
                if not await self._planner_parent_allows_progress(run_id=run_id):
                    await self._defer_materialization_for_control(
                        child_id=str(child["child_id"]),
                        run_id=run_id,
                        phase="artifact_published_after_control",
                    )
                    await self._yield_run_if_owned(
                        run_id=run_id,
                        token=token,
                        phase="planner_stopped_by_control",
                    )
                    return True
                await asyncio.to_thread(
                    self._planner.ensure_initial_attempt,
                    child_id=str(child["child_id"]),
                    node_id=node_id,
                )
                await asyncio.to_thread(
                    self._repository.transition_child_with_event,
                    str(child["child_id"]),
                    expected_statuses=("materializing",),
                    next_status="queued",
                    phase="artifact_published",
                    prediction_artifact_uri=str(artifacts.artifact_manifest_path),
                    prediction_artifact_hash=str(
                        artifacts.artifact_manifest["manifest_hash"]
                    ),
                    event_payload={
                        "attempt_id": attempt_id,
                        "artifact_manifest_hash": artifacts.artifact_manifest[
                            "manifest_hash"
                        ],
                        "materialized_row_count": len(materialization.prediction_frame),
                    },
                )
            refreshed = await asyncio.to_thread(
                self._repository.heartbeat_run,
                run_id,
                token=token,
                lease_seconds=self._config.lease_seconds,
            )
            token = _ownership_token(refreshed)
            running = await asyncio.to_thread(
                self._repository.transition_run_with_event,
                run_id,
                token=token,
                expected_statuses=("preparing",),
                next_status="running",
                phase="children_planned",
                progress={
                    "planned_child_count": len(plan.children),
                    "planner_version": plan.planner_version,
                },
            )
            await asyncio.to_thread(
                self._repository.yield_run_ownership,
                run_id,
                token=_ownership_token(running),
                phase="waiting_children",
            )
            return True
        except Exception as exc:
            logger.exception(
                "durable planner failed for run_id=%s: %s",
                run_id,
                _exception_payload(exc),
            )
            await self._fail_or_yield_planner_run(run_id=run_id, token=token, error=exc)
            return True

    async def dispatch_pass_once(
        self,
        *,
        excluded_attempt_ids: Sequence[str] = (),
    ) -> bool:
        attempt = await asyncio.to_thread(
            self._repository.claim_next_attempt,
            owner_id=self._owner_id,
            lease_seconds=self._config.lease_seconds,
            claim_kind="dispatch",
            excluded_attempt_ids=excluded_attempt_ids,
        )
        if attempt is None:
            return False
        attempt_id = str(attempt["attempt_id"])
        self._last_claimed_attempt_id = attempt_id
        token = _ownership_token(attempt)
        try:
            child = _required(
                "child",
                attempt["child_id"],
                self._repository.get_child(str(attempt["child_id"])),
            )
            run = _required(
                "run",
                attempt["run_id"],
                self._repository.get_run(str(attempt["run_id"])),
            )
            artifacts = self._adapter.load_published_artifacts(
                run_id=str(run["id"]),
                child_id=str(child["child_id"]),
                attempt_id=attempt_id,
            )
            request = self._adapter.request_from_run(run)
            node_id = str(
                attempt.get("node_id")
                or request.backtest_config.get("node_id")
                or "wsl2-5080"
            )
            intent = self._adapter.prepare_submission_intent(
                run=run,
                child=child,
                attempt=attempt,
                node_id=node_id,
            )
            refreshed = await asyncio.to_thread(
                self._repository.heartbeat_attempt,
                attempt_id,
                token=token,
                lease_seconds=self._config.lease_seconds,
            )
            token = _ownership_token(refreshed)
            outcome = await self._adapter.submit(
                artifacts=artifacts,
                intent=intent,
                attempt_token=token,
            )
            if outcome.waiting_capacity:
                return True
            token = self._owned_attempt_token_from_outcome(
                attempt_id=attempt_id,
                outcome=outcome,
                fallback=token,
            )
            await self._apply_remote_status(
                run=run,
                child=child,
                attempt_id=attempt_id,
                token=token,
                intent=intent,
                artifacts=artifacts,
                remote_status=str(outcome.remote_status or "submission_unknown"),
                remote_payload=dict(outcome.detail or {}),
            )
            return True
        except Exception as exc:
            logger.exception(
                "durable dispatch failed for attempt_id=%s: %s",
                attempt_id,
                _exception_payload(exc),
            )
            await self._fail_attempt_from_current_owner(
                attempt_id=attempt_id,
                token=token,
                error=exc,
                phase="dispatch_failed",
            )
            return True

    async def cancel_reconcile_pass_once(
        self,
        *,
        excluded_attempt_ids: Sequence[str] = (),
    ) -> bool:
        attempt = await asyncio.to_thread(
            self._repository.claim_next_attempt,
            owner_id=self._owner_id,
            lease_seconds=self._config.lease_seconds,
            claim_kind="cancel",
            excluded_attempt_ids=excluded_attempt_ids,
        )
        if attempt is None:
            return False
        attempt_id = str(attempt["attempt_id"])
        self._last_claimed_attempt_id = attempt_id
        token = _ownership_token(attempt)
        try:
            child = _required(
                "child",
                attempt["child_id"],
                self._repository.get_child(str(attempt["child_id"])),
            )
            run = _required(
                "run",
                attempt["run_id"],
                self._repository.get_run(str(attempt["run_id"])),
            )
            await self._reconcile_cancelled_attempt_claimed(
                attempt=attempt,
                token=token,
                child=child,
                run=run,
            )
            return True
        except Exception as exc:
            logger.exception(
                "durable cancellation reconciliation failed for attempt_id=%s: %s",
                attempt_id,
                _exception_payload(exc),
            )
            await self._keep_cancel_attempt_reconciling(
                attempt_id=attempt_id,
                token=token,
                child=child if "child" in locals() else {"child_id": attempt.get("child_id"), "run_id": attempt.get("run_id")},
                phase="cancel_reconciliation_error",
                evidence={"error": _exception_payload(exc)},
            )
            return True

    async def reconcile_pass_once(
        self,
        *,
        excluded_attempt_ids: Sequence[str] = (),
    ) -> bool:
        attempt = await asyncio.to_thread(
            self._repository.claim_next_attempt,
            owner_id=self._owner_id,
            lease_seconds=self._config.lease_seconds,
            claim_kind="reconcile",
            excluded_attempt_ids=excluded_attempt_ids,
        )
        if attempt is None:
            return False
        attempt_id = str(attempt["attempt_id"])
        self._last_claimed_attempt_id = attempt_id
        token = _ownership_token(attempt)
        try:
            child = _required(
                "child",
                attempt["child_id"],
                self._repository.get_child(str(attempt["child_id"])),
            )
            run = _required(
                "run",
                attempt["run_id"],
                self._repository.get_run(str(attempt["run_id"])),
            )
            if str(run.get("status") or "") in {"cancel_requested", "cancelling"}:
                await self._reconcile_cancelled_attempt_claimed(
                    attempt=attempt,
                    token=token,
                    child=child,
                    run=run,
                )
                return True
            node_id = str(attempt.get("node_id") or "").strip()
            if not node_id:
                raise DurableOrchestratorError(
                    "submitted durable attempt is missing node_id",
                    reason_code="multi_alpha_submission_identity_incomplete",
                    context={"attempt_id": attempt_id},
                )
            intent = self._adapter.prepare_submission_intent(
                run=run,
                child=child,
                attempt=attempt,
                node_id=node_id,
            )
            artifacts = self._adapter.load_published_artifacts(
                run_id=str(run["id"]),
                child_id=str(child["child_id"]),
                attempt_id=attempt_id,
            )
            try:
                inspection = await self._adapter.inspect_remote(intent=intent)
            except Exception as exc:
                await self._keep_attempt_reconciling(
                    attempt_id=attempt_id,
                    token=token,
                    child=child,
                    phase="remote_status_unknown",
                    error=exc,
                )
                return True
            remote_status = str(inspection.status.get("status") or "").strip().lower()
            if remote_status == "not_reserved":
                outcome = await self._adapter.submit(
                    artifacts=artifacts,
                    intent=intent,
                    attempt_token=token,
                )
                if outcome.waiting_capacity:
                    raise DurableOrchestratorError(
                        "existing durable attempt reservation unexpectedly returned capacity waiting",
                        reason_code="multi_alpha_existing_reservation_lost",
                        context={"attempt_id": attempt_id},
                    )
                token = self._owned_attempt_token_from_outcome(
                    attempt_id=attempt_id,
                    outcome=outcome,
                    fallback=token,
                )
                remote_status = str(outcome.remote_status or "submission_unknown")
                remote_payload = dict(outcome.detail or {})
            else:
                remote_payload = {
                    **dict(inspection.status),
                    "submission_receipt": _receipt_evidence(inspection.receipt),
                }
            await self._apply_remote_status(
                run=run,
                child=child,
                attempt_id=attempt_id,
                token=token,
                intent=intent,
                artifacts=artifacts,
                remote_status=remote_status,
                remote_payload=remote_payload,
            )
            return True
        except Exception as exc:
            logger.exception(
                "durable reconcile failed for attempt_id=%s: %s",
                attempt_id,
                _exception_payload(exc),
            )
            await self._fail_attempt_from_current_owner(
                attempt_id=attempt_id,
                token=token,
                error=exc,
                phase="reconcile_failed",
            )
            return True

    async def finalizer_pass_once(
        self,
        *,
        excluded_run_ids: Sequence[str] = (),
    ) -> bool:
        run = await asyncio.to_thread(
            self._repository.claim_next_finalizable_run,
            owner_id=self._owner_id,
            lease_seconds=self._config.lease_seconds,
            excluded_run_ids=excluded_run_ids,
        )
        if run is None:
            return False
        run_id = str(run["id"])
        self._last_claimed_run_id = run_id
        token = _ownership_token(run)
        progressed = False
        try:
            children = self._repository.list_children(run_id)
            for child in children:
                if child["status"] != "reconciling":
                    continue
                if not child.get("selected_attempt_id"):
                    succeeded_attempts = [
                        attempt
                        for attempt in self._repository.list_attempts(
                            str(child["child_id"])
                        )
                        if attempt.get("status") == "succeeded"
                    ]
                    if succeeded_attempts:
                        selected = max(
                            succeeded_attempts,
                            key=lambda item: (
                                int(item.get("attempt_no") or 0),
                                str(item.get("attempt_id") or ""),
                            ),
                        )
                        await asyncio.to_thread(
                            self._repository.set_child_reconciling_attempt,
                            str(child["child_id"]),
                            selected_attempt_id=str(selected["attempt_id"]),
                            phase="business_result_selection_recovered",
                            event_payload={"restart_recovery": True},
                        )
                        child = _required(
                            "child",
                            child["child_id"],
                            self._repository.get_child(str(child["child_id"])),
                        )
                        children = self._repository.list_children(run_id)
                if await asyncio.to_thread(
                    self._business_assembler.assemble_child,
                    run=run,
                    child=child,
                    children=children,
                ):
                    progressed = True
                    children = self._repository.list_children(run_id)
            if not children or any(
                str(child["status"]) not in TERMINAL_CHILD_STATUSES
                for child in children
            ):
                await asyncio.to_thread(
                    self._repository.yield_run_ownership,
                    run_id,
                    token=token,
                    phase="waiting_business_dependencies",
                )
                return progressed
            next_status, reason = _parent_status(children=children, run=run)
            refreshed = await asyncio.to_thread(
                self._repository.heartbeat_run,
                run_id,
                token=token,
                lease_seconds=self._config.lease_seconds,
            )
            token = _ownership_token(refreshed)
            scheme_children = [
                child for child in children if child["child_kind"] == "scheme"
            ]
            loo_children = [
                child for child in children if child["child_kind"] == "loo"
            ]
            completed_after_deadline_children = self._completed_after_deadline_children(
                children
            )
            await asyncio.to_thread(
                self._repository.finalize_run_with_business_readback,
                run_id,
                token=token,
                expected_statuses=(str(run["status"]),),
                next_status=next_status,
                expected_child_count=len(children),
                expected_scheme_result_count=sum(
                    1
                    for child in scheme_children
                    if str(child.get("status") or "") != "not_recovered"
                ),
                expected_loo_result_count=sum(
                    1 for child in loo_children if child["status"] == "succeeded"
                ),
                progress={
                    "planned_child_count": len(children),
                    "successful_scheme_count": sum(
                        1
                        for child in scheme_children
                        if child["status"] == "succeeded"
                    ),
                    "terminal_child_count": len(children),
                    "completed_after_deadline": bool(
                        completed_after_deadline_children
                    ),
                    "completed_after_deadline_child_count": len(
                        completed_after_deadline_children
                    ),
                    "completed_after_deadline_children": completed_after_deadline_children,
                },
                reason_code=reason.get("reason_code"),
                error=reason if next_status != "succeeded" else None,
            )
            return True
        except Exception as exc:
            logger.exception(
                "durable finalizer failed for run_id=%s: %s",
                run_id,
                _exception_payload(exc),
            )
            await self._append_item_error(
                run_id=run_id,
                phase="business_finalize_error",
                error=exc,
            )
            await self._yield_run_if_owned(run_id=run_id, token=token, phase="business_finalize_error")
            return True

    async def archive_pass(self) -> int:
        runs = await asyncio.to_thread(
            self._repository.list_runs_pending_archive,
            limit=self._config.archive_batch_size,
        )
        completed = 0
        for run in runs:
            run_id = str(run["id"])
            try:
                result = await asyncio.to_thread(
                    self._archive_capture.enqueue_multi_alpha_combine_completed_result,
                    run_id=run_id,
                    roster_hash=str(run.get("roster_hash") or "") or None,
                    status=str(run.get("status") or "") or None,
                    payload={
                        "reason_code": "multi_alpha_combine_terminal",
                        "logical_status": run.get("status"),
                        "terminal_reason": _mapping(run.get("reason")),
                        "durable": True,
                    },
                )
                event_id = str(result.get("event_id") or "").strip()
                if result.get("inserted"):
                    phase = "archive_enqueued"
                elif result.get("duplicate"):
                    phase = "archive_duplicate"
                elif result.get("skipped_reason") == "disabled":
                    phase = "archive_skipped_disabled"
                else:
                    phase = "archive_error"
                await asyncio.to_thread(
                    self._repository.append_archive_delivery_event,
                    run_id=run_id,
                    phase=phase,
                    archive_event_id=event_id,
                    payload={"archive_result": dict(result)},
                    reason_code=(
                        None
                        if phase in {"archive_enqueued", "archive_duplicate"}
                        else str(result.get("skipped_reason") or "archive_capture_failed")
                    ),
                )
                completed += 1
            except Exception as exc:
                payload = _exception_payload(exc)
                logger.exception(
                    "durable archive capture failed for run_id=%s: %s",
                    run_id,
                    payload,
                )
                event_id = f"archive_error:{type(exc).__name__}:{run_id}"
                await asyncio.to_thread(
                    self._repository.append_archive_delivery_event,
                    run_id=run_id,
                    phase="archive_error",
                    archive_event_id=event_id,
                    payload={"error": payload},
                    reason_code=str(payload["reason_code"]),
                )
        return completed

    async def _apply_remote_status(
        self,
        *,
        run: Mapping[str, Any],
        child: Mapping[str, Any],
        attempt_id: str,
        token: OwnershipToken,
        intent: DurableSubmissionIntent,
        artifacts: DurablePublishedArtifacts,
        remote_status: str,
        remote_payload: Mapping[str, Any],
    ) -> None:
        normalized = str(remote_status or "").strip().lower()
        token, deadline_evidence = await self._record_execution_deadline_evidence(
            run=run,
            attempt_id=attempt_id,
            token=token,
            remote_status=normalized,
            remote_payload=remote_payload,
        )
        if normalized in ACTIVE_REMOTE_STATUSES:
            current = _required(
                "attempt",
                attempt_id,
                self._repository.get_attempt(attempt_id),
            )
            token = self._owned_attempt_token_from_row(
                attempt_id=attempt_id,
                current=current,
                lineage=token,
            )
            await self._ensure_child_running(child_id=str(child["child_id"]))
            if current["status"] == "submitting" and normalized in {"running", "processing"}:
                current = await asyncio.to_thread(
                    self._repository.transition_attempt_with_event,
                    attempt_id,
                    token=token,
                    expected_statuses=("submitting",),
                    next_status="running",
                    phase="remote_running",
                    remote_status=normalized,
                    event_payload={"remote": dict(remote_payload)},
                )
                token = _ownership_token(current)
            elif current["status"] in {"submitting", "running", "reconciling"}:
                await self._append_item_event(
                    run_id=str(run["id"]),
                    child_id=str(child["child_id"]),
                    attempt_id=attempt_id,
                    event_type="status",
                    phase="remote_active",
                    payload={"remote_status": normalized, "remote": dict(remote_payload)},
                )
            await asyncio.to_thread(
                self._repository.yield_attempt_ownership,
                attempt_id,
                token=token,
                phase="waiting_remote",
            )
            return
        if normalized in {"reserved_not_started", "submission_unknown"}:
            await self._keep_attempt_reconciling(
                attempt_id=attempt_id,
                token=token,
                child=child,
                phase=normalized,
                error=DurableOrchestratorError(
                    "remote execution acceptance is durable but execution state is not yet authoritative",
                    reason_code=f"qe_workspace_{normalized}",
                    context=dict(remote_payload),
                ),
            )
            return
        if normalized == "completed":
            try:
                await asyncio.to_thread(
                    self._adapter.record_remote_terminal,
                    intent=intent,
                    owner_id=self._owner_id,
                    remote_status="completed",
                )
            except Exception as exc:
                error = _exception_payload(exc)
                if error["reason_code"] not in RETRYABLE_TERMINAL_RESERVATION_REASON_CODES:
                    raise
                await self._keep_attempt_reconciling(
                    attempt_id=attempt_id,
                    token=token,
                    child=child,
                    phase="terminal_reservation_reconciliation_pending",
                    error=exc,
                )
                return
            current = _required(
                "attempt",
                attempt_id,
                self._repository.get_attempt(attempt_id),
            )
            token = self._owned_attempt_token_from_row(
                attempt_id=attempt_id,
                current=current,
                lineage=token,
            )
            if current["status"] in {"submitting", "running"}:
                current = await asyncio.to_thread(
                    self._repository.transition_attempt_with_event,
                    attempt_id,
                    token=token,
                    expected_statuses=(str(current["status"]),),
                    next_status="reconciling",
                    phase="remote_completed",
                    remote_status="completed",
                    event_payload={"remote": dict(remote_payload)},
                )
                token = _ownership_token(current)
            await self._ensure_child_reconciling(child_id=str(child["child_id"]))
            try:
                result = await self._adapter.collect_result(
                    intent=intent,
                    artifacts=artifacts,
                    execution_deadline_evidence=deadline_evidence,
                )
            except Exception as exc:
                error = _exception_payload(exc)
                if error["reason_code"] not in RETRYABLE_RESULT_COLLECTION_REASON_CODES:
                    await self._terminalize_attempt_and_child(
                        attempt_id=attempt_id,
                        token=token,
                        attempt_status="failed",
                        child_status="failed",
                        phase="completed_result_invalid",
                        error=error,
                    )
                    return
                current = _required(
                    "attempt",
                    attempt_id,
                    self._repository.get_attempt(attempt_id),
                )
                token = self._owned_attempt_token_from_row(
                    attempt_id=attempt_id,
                    current=current,
                    lineage=token,
                )
                await self._append_item_error(
                    run_id=str(run["id"]),
                    child_id=str(child["child_id"]),
                    attempt_id=attempt_id,
                    phase="completed_result_collection_pending",
                    error=exc,
                )
                await asyncio.to_thread(
                    self._repository.yield_attempt_ownership,
                    attempt_id,
                    token=token,
                    phase="completed_result_collection_pending",
                )
                return
            current = _required(
                "attempt",
                attempt_id,
                self._repository.get_attempt(attempt_id),
            )
            token = self._owned_attempt_token_from_row(
                attempt_id=attempt_id,
                current=current,
                lineage=token,
            )
            succeeded = await asyncio.to_thread(
                self._repository.transition_attempt_with_event,
                attempt_id,
                token=token,
                expected_statuses=("reconciling",),
                next_status="succeeded",
                phase="result_persisted",
                remote_status="completed",
                result_manifest=result.result_manifest,
                event_payload={
                    "result_manifest_hash": result.result_manifest["manifest_hash"],
                },
            )
            await asyncio.to_thread(
                self._repository.set_child_reconciling_attempt,
                str(child["child_id"]),
                selected_attempt_id=str(succeeded["attempt_id"]),
                phase="business_result_pending",
                event_payload={
                    "result_manifest_hash": result.result_manifest["manifest_hash"],
                },
            )
            return
        if normalized in REMOTE_FAILURE_STATUSES:
            await asyncio.to_thread(
                self._adapter.record_remote_terminal,
                intent=intent,
                owner_id=self._owner_id,
                remote_status="failed",
            )
            await self._terminalize_attempt_and_child(
                attempt_id=attempt_id,
                token=token,
                attempt_status="failed",
                child_status="failed",
                phase="remote_failed",
                error={
                    "reason_code": "qe_workspace_remote_failed",
                    "message": "QE Workspace reported a terminal execution failure",
                    "context": {"remote_status": normalized, "remote": dict(remote_payload)},
                },
            )
            return
        if normalized in REMOTE_CANCELLED_STATUSES:
            await asyncio.to_thread(
                self._adapter.record_remote_terminal,
                intent=intent,
                owner_id=self._owner_id,
                remote_status="cancelled",
            )
            await self._terminalize_attempt_and_child(
                attempt_id=attempt_id,
                token=token,
                attempt_status="cancelled",
                child_status=(
                    "cancelled"
                    if bool(remote_payload.get("cancellation_reconciliation"))
                    else "failed"
                ),
                phase="remote_cancelled",
                error={
                    "reason_code": "qe_workspace_remote_cancelled",
                    "message": (
                        "QE Workspace confirmed the durable P0-2 cancellation request"
                        if bool(remote_payload.get("cancellation_reconciliation"))
                        else "QE Workspace cancelled a child without a P0-2 user control request"
                    ),
                    "context": {"remote_status": normalized, "remote": dict(remote_payload)},
                },
            )
            return
        await self._keep_attempt_reconciling(
            attempt_id=attempt_id,
            token=token,
            child=child,
            phase="remote_status_unmapped",
            error=DurableOrchestratorError(
                "QE Workspace returned an unmapped non-authoritative status",
                reason_code="qe_workspace_remote_status_unmapped",
                context={"remote_status": normalized, "remote": dict(remote_payload)},
            ),
        )

    async def _reconcile_cancelled_attempt_claimed(
        self,
        *,
        attempt: Mapping[str, Any],
        token: OwnershipToken,
        child: Mapping[str, Any],
        run: Mapping[str, Any],
    ) -> None:
        """Reconcile cancellation without ever re-submitting a lost reservation."""

        attempt_id = str(attempt["attempt_id"])
        node_id = str(attempt.get("node_id") or "").strip()
        if not node_id:
            await self._keep_cancel_attempt_reconciling(
                attempt_id=attempt_id,
                token=token,
                child=child,
                phase="cancel_identity_incomplete",
                evidence={
                    "reason_code": "cancel_remote_identity_incomplete",
                    "attempt_id": attempt_id,
                },
            )
            return
        intent = self._adapter.prepare_submission_intent(
            run=run,
            child=child,
            attempt=attempt,
            node_id=node_id,
        )
        try:
            inspection = await self._adapter.inspect_remote(intent=intent)
        except Exception as exc:
            await self._keep_cancel_attempt_reconciling(
                attempt_id=attempt_id,
                token=token,
                child=child,
                phase="cancel_remote_status_unknown",
                evidence={"error": _exception_payload(exc)},
            )
            return
        remote_status = str(inspection.status.get("status") or "").strip().lower()
        remote_payload = {
            **dict(inspection.status),
            "submission_receipt": _receipt_evidence(inspection.receipt),
            "cancellation_reconciliation": True,
        }
        if remote_status == "not_reserved":
            await self._terminalize_attempt_and_child(
                attempt_id=attempt_id,
                token=token,
                attempt_status="cancelled",
                child_status="cancelled",
                phase="cancel_remote_not_reserved",
                error={
                    "reason_code": "cancel_remote_not_reserved",
                    "message": "cancellation observed no durable QE submission reservation",
                    "context": {"remote_status": remote_status, "remote": remote_payload},
                },
            )
            return
        if remote_status in ACTIVE_REMOTE_STATUSES | {"reserved_not_started"}:
            await self._keep_cancel_attempt_reconciling(
                attempt_id=attempt_id,
                token=token,
                child=child,
                phase="cancel_waiting_remote_terminal",
                evidence={"remote_status": remote_status, "remote": remote_payload},
            )
            return
        if remote_status == "completed":
            try:
                artifacts = self._adapter.load_published_artifacts(
                    run_id=str(run["id"]),
                    child_id=str(child["child_id"]),
                    attempt_id=attempt_id,
                )
            except Exception as exc:
                await self._keep_cancel_attempt_reconciling(
                    attempt_id=attempt_id,
                    token=token,
                    child=child,
                    phase="cancel_completion_artifacts_unavailable",
                    evidence={"error": _exception_payload(exc), "remote": remote_payload},
                )
                return
            await self._apply_remote_status(
                run=run,
                child=child,
                attempt_id=attempt_id,
                token=token,
                intent=intent,
                artifacts=artifacts,
                remote_status=remote_status,
                remote_payload=remote_payload,
            )
            return
        if remote_status in REMOTE_FAILURE_STATUSES:
            await self._terminalize_attempt_and_child(
                attempt_id=attempt_id,
                token=token,
                attempt_status="failed",
                child_status="failed",
                phase="cancel_remote_failed_before_cancellation",
                error={
                    "reason_code": "qe_workspace_remote_failed",
                    "message": "QE Workspace reported failure while cancellation was reconciling",
                    "context": {"remote_status": remote_status, "remote": remote_payload},
                },
            )
            return
        if remote_status in REMOTE_CANCELLED_STATUSES:
            await self._terminalize_attempt_and_child(
                attempt_id=attempt_id,
                token=token,
                attempt_status="cancelled",
                child_status="cancelled",
                phase="cancel_remote_confirmed",
                error={
                    "reason_code": "qe_workspace_remote_cancelled",
                    "message": "QE Workspace confirmed cancellation for the durable control request",
                    "context": {"remote_status": remote_status, "remote": remote_payload},
                },
            )
            return
        await self._keep_cancel_attempt_reconciling(
            attempt_id=attempt_id,
            token=token,
            child=child,
            phase="cancel_remote_status_unmapped",
            evidence={"remote_status": remote_status, "remote": remote_payload},
        )

    async def _keep_cancel_attempt_reconciling(
        self,
        *,
        attempt_id: str,
        token: OwnershipToken,
        child: Mapping[str, Any],
        phase: str,
        evidence: Mapping[str, Any],
    ) -> None:
        current = _required("attempt", attempt_id, self._repository.get_attempt(attempt_id))
        token = self._owned_attempt_token_from_row(
            attempt_id=attempt_id,
            current=current,
            lineage=token,
        )
        if current["status"] in {"submitting", "running"}:
            current = await asyncio.to_thread(
                self._repository.transition_attempt_with_event,
                attempt_id,
                token=token,
                expected_statuses=(str(current["status"]),),
                next_status="reconciling",
                phase=phase,
                remote_status=str(evidence.get("remote_status") or phase),
                reason_code="cancel_reconciliation_pending",
                event_payload={"cancellation": dict(evidence)},
            )
            token = _ownership_token(current)
        else:
            current = await asyncio.to_thread(
                self._repository.heartbeat_attempt,
                attempt_id,
                token=token,
                lease_seconds=self._config.lease_seconds,
            )
            token = _ownership_token(current)
        await self._append_item_event(
            run_id=str(child.get("run_id") or ""),
            child_id=str(child.get("child_id") or "") or None,
            attempt_id=attempt_id,
            event_type="control",
            phase=phase,
            reason_code="cancel_reconciliation_pending",
            payload={"cancellation": dict(evidence)},
        )
        await asyncio.to_thread(
            self._repository.yield_attempt_ownership,
            attempt_id,
            token=token,
            phase=phase,
        )

    async def _record_execution_deadline_evidence(
        self,
        *,
        run: Mapping[str, Any],
        attempt_id: str,
        token: OwnershipToken,
        remote_status: str,
        remote_payload: Mapping[str, Any],
    ) -> tuple[OwnershipToken, Mapping[str, Mapping[str, Any]]]:
        attempt = _required(
            "attempt",
            attempt_id,
            self._repository.get_attempt(attempt_id),
        )
        token = self._owned_attempt_token_from_row(
            attempt_id=attempt_id,
            current=attempt,
            lineage=token,
        )
        existing = _deadline_evidence_from_attempt(attempt)
        if not (
            run.get("started_at")
            or run.get("created_at")
            or attempt.get("submitted_at")
        ):
            heartbeat = await asyncio.to_thread(
                self._repository.heartbeat_attempt,
                attempt_id,
                token=token,
                lease_seconds=self._config.lease_seconds,
            )
            return _ownership_token(heartbeat), existing
        request = self._adapter.request_from_run(run)
        observed = _execution_deadline_evidence(
            run=run,
            attempt=attempt,
            scheme_timeout_seconds=request.scheme_timeout_seconds,
            run_timeout_seconds=request.run_timeout_seconds,
            remote_status=remote_status,
            remote_payload=remote_payload,
        )
        if not observed:
            heartbeat = await asyncio.to_thread(
                self._repository.heartbeat_attempt,
                attempt_id,
                token=token,
                lease_seconds=self._config.lease_seconds,
            )
            return _ownership_token(heartbeat), existing
        updated = await asyncio.to_thread(
            self._repository.record_attempt_deadline_evidence,
            attempt_id,
            token=token,
            evidence=observed,
        )
        updated_token = _ownership_token(updated)
        heartbeat = await asyncio.to_thread(
            self._repository.heartbeat_attempt,
            attempt_id,
            token=updated_token,
            lease_seconds=self._config.lease_seconds,
        )
        return _ownership_token(heartbeat), _deadline_evidence_from_attempt(updated)

    def _completed_after_deadline_children(
        self,
        children: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        completed: list[dict[str, Any]] = []
        for child in children:
            if child.get("status") != "succeeded" or not child.get("selected_attempt_id"):
                continue
            attempt = self._repository.get_attempt(str(child["selected_attempt_id"]))
            if attempt is None:
                raise DurableOrchestratorError(
                    "selected successful attempt disappeared during parent finalization",
                    reason_code="multi_alpha_durable_attempt_not_found",
                    context={
                        "child_id": child.get("child_id"),
                        "attempt_id": child.get("selected_attempt_id"),
                    },
                )
            result_manifest = _mapping(attempt.get("result_manifest_json"))
            if result_manifest.get("completed_after_deadline") is not True:
                continue
            completed.append(
                {
                    "child_id": str(child["child_id"]),
                    "child_key": str(child["child_key"]),
                    "attempt_id": str(attempt["attempt_id"]),
                    "execution_deadline": _mapping(
                        result_manifest.get("execution_deadline")
                    ),
                }
            )
        return completed

    async def _keep_attempt_reconciling(
        self,
        *,
        attempt_id: str,
        token: OwnershipToken,
        child: Mapping[str, Any],
        phase: str,
        error: Exception,
    ) -> None:
        current = _required(
            "attempt",
            attempt_id,
            self._repository.get_attempt(attempt_id),
        )
        token = self._owned_attempt_token_from_row(
            attempt_id=attempt_id,
            current=current,
            lineage=token,
        )
        if current["status"] in {"submitting", "running"}:
            current = await asyncio.to_thread(
                self._repository.transition_attempt_with_event,
                attempt_id,
                token=token,
                expected_statuses=(str(current["status"]),),
                next_status="reconciling",
                phase=phase,
                remote_status=phase,
                reason_code=_exception_payload(error)["reason_code"],
                event_payload={"error": _exception_payload(error)},
            )
            token = _ownership_token(current)
        else:
            current = await asyncio.to_thread(
                self._repository.heartbeat_attempt,
                attempt_id,
                token=token,
                lease_seconds=self._config.lease_seconds,
            )
            token = _ownership_token(current)
            await self._append_item_error(
                run_id=str(child["run_id"]),
                child_id=str(child["child_id"]),
                attempt_id=attempt_id,
                phase=phase,
                error=error,
            )
        await self._ensure_child_reconciling(child_id=str(child["child_id"]))
        await asyncio.to_thread(
            self._repository.yield_attempt_ownership,
            attempt_id,
            token=token,
            phase=phase,
        )

    async def _terminalize_attempt_and_child(
        self,
        *,
        attempt_id: str,
        token: OwnershipToken,
        attempt_status: str,
        child_status: str,
        phase: str,
        error: Mapping[str, Any],
    ) -> None:
        attempt = _required(
            "attempt",
            attempt_id,
            self._repository.get_attempt(attempt_id),
        )
        token = self._owned_attempt_token_from_row(
            attempt_id=attempt_id,
            current=attempt,
            lineage=token,
        )
        if attempt["status"] not in {"queued", "submitting", "running", "reconciling"}:
            return
        await asyncio.to_thread(
            self._repository.transition_attempt_with_event,
            attempt_id,
            token=token,
            expected_statuses=(str(attempt["status"]),),
            next_status=attempt_status,
            phase=phase,
            remote_status=str(error.get("context", {}).get("remote_status") or phase),
            reason_code=str(error["reason_code"]),
            error=error,
            event_payload={"error": dict(error)},
        )
        child = _required(
            "child",
            attempt["child_id"],
            self._repository.get_child(str(attempt["child_id"])),
        )
        if child["status"] in TERMINAL_CHILD_STATUSES:
            return
        if child["child_kind"] == "scheme":
            weights, per_window, metadata_error = self._failure_materialization_metadata(
                run_id=str(child["run_id"]),
                child_id=str(child["child_id"]),
                attempt_id=attempt_id,
            )
            enriched_error = dict(error)
            if metadata_error is not None:
                enriched_error["materialization_metadata_error"] = metadata_error
            await asyncio.to_thread(
                self._repository.finalize_scheme_child_without_result,
                str(child["child_id"]),
                expected_statuses=(str(child["status"]),),
                next_status=child_status,
                reason_code=str(error["reason_code"]),
                error=enriched_error,
                weights=weights,
                per_window_weights=per_window,
                selected_attempt_id=attempt_id,
            )
            return
        await asyncio.to_thread(
            self._repository.transition_child_with_event,
            str(child["child_id"]),
            expected_statuses=(str(child["status"]),),
            next_status=child_status,
            phase=phase,
            selected_attempt_id=attempt_id,
            reason_code=str(error["reason_code"]),
            event_payload={"error": dict(error)},
        )

    async def _fail_attempt_from_current_owner(
        self,
        *,
        attempt_id: str,
        token: OwnershipToken,
        error: Exception,
        phase: str,
    ) -> None:
        attempt = self._repository.get_attempt(attempt_id)
        if attempt is None:
            return
        try:
            token = self._owned_attempt_token_from_row(
                attempt_id=attempt_id,
                current=attempt,
                lineage=token,
            )
        except DurableOrchestratorError:
            await self._append_item_error(
                run_id=str(attempt.get("run_id") or ""),
                child_id=str(attempt.get("child_id") or "") or None,
                attempt_id=attempt_id,
                phase=phase,
                error=error,
            )
            return
        await self._terminalize_attempt_and_child(
            attempt_id=attempt_id,
            token=token,
            attempt_status="failed",
            child_status="failed",
            phase=phase,
            error=_exception_payload(error),
        )

    async def _ensure_child_running(self, *, child_id: str) -> None:
        child = _required("child", child_id, self._repository.get_child(child_id))
        if child["status"] == "queued":
            await asyncio.to_thread(
                self._repository.transition_child_with_event,
                child_id,
                expected_statuses=("queued",),
                next_status="running",
                phase="remote_reserved",
            )

    async def _ensure_child_reconciling(self, *, child_id: str) -> None:
        await self._ensure_child_running(child_id=child_id)
        child = _required("child", child_id, self._repository.get_child(child_id))
        if child["status"] == "running":
            await asyncio.to_thread(
                self._repository.transition_child_with_event,
                child_id,
                expected_statuses=("running",),
                next_status="reconciling",
                phase="remote_reconciling",
            )
        elif child["status"] == "cancel_requested":
            await asyncio.to_thread(
                self._repository.transition_child_with_event,
                child_id,
                expected_statuses=("cancel_requested",),
                next_status="reconciling",
                phase="completion_raced_with_cancel",
                reason_code="completion_raced_with_cancel",
            )

    def _terminalize_unexecutable_child(
        self,
        *,
        child: Mapping[str, Any],
        next_status: str,
        error: Mapping[str, Any],
    ) -> None:
        if child["child_kind"] == "scheme":
            self._repository.finalize_scheme_child_without_result(
                str(child["child_id"]),
                expected_statuses=(str(child["status"]),),
                next_status=next_status,
                reason_code=str(error["reason_code"]),
                error=error,
            )
            return
        self._repository.transition_child_with_event(
            str(child["child_id"]),
            expected_statuses=(str(child["status"]),),
            next_status=next_status,
            phase="materialization_unavailable",
            reason_code=str(error["reason_code"]),
            event_payload={"error": dict(error)},
        )

    def _materialize_and_publish(
        self,
        *,
        run_id: str,
        child_id: str,
        attempt_id: str,
    ) -> tuple[Any, DurablePublishedArtifacts]:
        materialization = self._adapter.materialize_child_input(
            run_id=run_id,
            child_id=child_id,
            attempt_id=attempt_id,
        )
        return materialization, self._adapter.publish_artifacts(materialization)

    async def _planner_parent_allows_progress(self, *, run_id: str) -> bool:
        run = self._repository.get_run(run_id)
        return run is not None and str(run.get("status") or "") in {"preparing", "running"}

    async def _defer_materialization_for_control(
        self,
        *,
        child_id: str,
        run_id: str,
        phase: str,
    ) -> None:
        """Keep control arrival distinct from a materialization technical failure."""

        run = self._repository.get_run(run_id)
        child = self._repository.get_child(child_id)
        if run is None or child is None or str(child.get("status") or "") != "materializing":
            return
        run_status = str(run.get("status") or "")
        if run_status in {"pause_requested", "paused"}:
            next_status = "pending"
            reason_code = "materialization_deferred_by_pause"
        elif run_status in {"cancel_requested", "cancelling", "cancelled"}:
            next_status = "cancelled"
            reason_code = "materialization_cancelled_by_control"
        else:
            return
        await asyncio.to_thread(
            self._repository.transition_child_with_event,
            child_id,
            expected_statuses=("materializing",),
            next_status=next_status,
            phase=phase,
            reason_code=reason_code,
            event_payload={"run_status": run_status, "control_deferred": True},
        )

    async def _run_sync_with_run_heartbeat(
        self,
        *,
        run_id: str,
        token: OwnershipToken,
        operation: Any,
    ) -> tuple[Any | None, OwnershipToken, Exception | None]:
        task = asyncio.create_task(asyncio.to_thread(operation))
        current_token = token
        while True:
            done, _pending = await asyncio.wait(
                {task},
                timeout=self._config.heartbeat_seconds,
            )
            if task in done:
                try:
                    return task.result(), current_token, None
                except Exception as exc:
                    # The operation may fail after one or more successful run
                    # heartbeats.  Return the original exception together with
                    # the renewed ownership token so the planner can preserve
                    # exact business-error classification without reusing a
                    # stale CAS row version on the next child.
                    return None, current_token, exc
            try:
                row = await asyncio.to_thread(
                    self._repository.heartbeat_run,
                    run_id,
                    token=current_token,
                    lease_seconds=self._config.lease_seconds,
                )
                current_token = _ownership_token(row)
            except Exception:
                await task
                raise

    async def _fail_or_yield_planner_run(
        self,
        *,
        run_id: str,
        token: OwnershipToken,
        error: Exception,
    ) -> None:
        run = self._repository.get_run(run_id)
        if run is None or str(run.get("owner_id") or "") != self._owner_id:
            await self._append_item_error(
                run_id=run_id,
                phase="planner_error",
                error=error,
            )
            return
        current_token = _ownership_token(run)
        if run["status"] in {"queued", "preparing"}:
            try:
                await asyncio.to_thread(
                    self._repository.transition_run_with_event,
                    run_id,
                    token=current_token,
                    expected_statuses=(str(run["status"]),),
                    next_status="failed",
                    phase="planner_failed",
                    reason_code=_exception_payload(error)["reason_code"],
                    error=_exception_payload(error),
                    event_payload={"error": _exception_payload(error)},
                )
                return
            except MultiAlphaDurableRepositoryError:
                logger.exception("planner failure could not terminalize run_id=%s", run_id)
        await self._yield_run_if_owned(run_id=run_id, token=token, phase="planner_error")

    async def _yield_run_if_owned(
        self,
        *,
        run_id: str,
        token: OwnershipToken,
        phase: str,
    ) -> None:
        run = self._repository.get_run(run_id)
        if run is None or str(run.get("owner_id") or "") != self._owner_id:
            return
        await asyncio.to_thread(
            self._repository.yield_run_ownership,
            run_id,
            token=_ownership_token(run),
            phase=phase,
        )

    def _failure_materialization_metadata(
        self,
        *,
        run_id: str,
        child_id: str,
        attempt_id: str,
    ) -> tuple[Mapping[str, Any], Sequence[Mapping[str, Any]], Mapping[str, Any] | None]:
        try:
            artifacts = self._adapter.load_published_artifacts(
                run_id=run_id,
                child_id=child_id,
                attempt_id=attempt_id,
            )
            metadata = self._adapter.load_materialization_metadata(artifacts)
            return (
                _mapping(metadata.get("weights")),
                _mapping_sequence(metadata.get("per_window_weights")),
                None,
            )
        except Exception as exc:
            failure = _exception_payload(exc)
            logger.exception(
                "MULTI_ALPHA_FAILURE_METADATA_LOAD_FAILED run_id=%s child_id=%s attempt_id=%s",
                run_id,
                child_id,
                attempt_id,
            )
            return {}, (), failure

    def _owned_attempt_token_from_outcome(
        self,
        *,
        attempt_id: str,
        outcome: QEWorkspaceSubmissionOutcome,
        fallback: OwnershipToken,
    ) -> OwnershipToken:
        if outcome.source_claim is not None:
            return self._owned_attempt_token_from_row(
                attempt_id=attempt_id,
                current=outcome.source_claim,
                lineage=fallback,
            )
        current = self._repository.get_attempt(attempt_id)
        if current is not None:
            return self._owned_attempt_token_from_row(
                attempt_id=attempt_id,
                current=current,
                lineage=fallback,
            )
        return fallback

    def _owned_attempt_token_from_row(
        self,
        *,
        attempt_id: str,
        current: Mapping[str, Any],
        lineage: OwnershipToken,
    ) -> OwnershipToken:
        refreshed = _ownership_token(current)
        if (
            refreshed.owner_id != lineage.owner_id
            or refreshed.fencing_token != lineage.fencing_token
            or refreshed.row_version < lineage.row_version
        ):
            raise DurableOrchestratorError(
                "durable attempt ownership changed while this worker was active",
                reason_code="multi_alpha_stale_attempt_lineage",
                context={
                    "attempt_id": attempt_id,
                    "expected_owner_id": lineage.owner_id,
                    "actual_owner_id": refreshed.owner_id,
                    "expected_fencing_token": lineage.fencing_token,
                    "actual_fencing_token": refreshed.fencing_token,
                    "minimum_row_version": lineage.row_version,
                    "actual_row_version": refreshed.row_version,
                },
            )
        return refreshed

    async def _append_item_event(
        self,
        *,
        run_id: str,
        event_type: str,
        phase: str,
        payload: Mapping[str, Any],
        child_id: str | None = None,
        attempt_id: str | None = None,
        reason_code: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._repository.append_event,
            run_id=run_id,
            child_id=child_id,
            attempt_id=attempt_id,
            event_type=event_type,
            phase=phase,
            reason_code=reason_code,
            payload=payload,
        )

    async def _append_item_error(
        self,
        *,
        run_id: str,
        phase: str,
        error: Exception,
        child_id: str | None = None,
        attempt_id: str | None = None,
    ) -> None:
        payload = _exception_payload(error)
        await self._append_item_event(
            run_id=run_id,
            child_id=child_id,
            attempt_id=attempt_id,
            event_type="error",
            phase=phase,
            reason_code=str(payload["reason_code"]),
            payload={"error": payload},
        )

    async def _run_bounded_pass(self, pass_once: Any) -> int:
        completed = 0
        excluded: list[str] = []
        for _ in range(self._config.items_per_pass):
            before = set(excluded)
            claimed = await pass_once(excluded_run_ids=tuple(excluded))
            if not claimed:
                break
            completed += 1
            latest = self._latest_claimed_identity("run", before)
            if latest:
                excluded.append(latest)
        return completed

    async def _run_control_pass(self) -> int:
        completed = 0
        excluded: list[str] = []
        for _ in range(self._config.items_per_pass):
            command = await asyncio.to_thread(
                self._control_service.apply_one_local_command,
                owner_id=self._owner_id,
                lease_seconds=self._config.lease_seconds,
                excluded_command_ids=tuple(excluded),
            )
            if command is None:
                break
            completed += 1
            command_id = str(command.get("command_id") or "").strip()
            self._last_claimed_command_id = command_id or None
            if command_id:
                excluded.append(command_id)
        return completed

    async def _run_pause_drain_pass(self) -> int:
        completed = 0
        excluded: list[str] = []
        for _ in range(self._config.items_per_pass):
            run = await asyncio.to_thread(
                self._repository.claim_next_pause_drain_run,
                owner_id=self._owner_id,
                lease_seconds=self._config.lease_seconds,
                excluded_run_ids=tuple(excluded),
            )
            if run is None:
                break
            run_id = str(run["id"])
            self._last_claimed_run_id = run_id
            token = _ownership_token(run)
            try:
                paused = await asyncio.to_thread(
                    self._repository.transition_run_with_event,
                    run_id,
                    token=token,
                    expected_statuses=("pause_requested",),
                    next_status="paused",
                    phase="pause_drained",
                    progress={"control": "pause", "drain": "complete"},
                    reason_code="pause_drained",
                )
                await asyncio.to_thread(
                    self._repository.yield_run_ownership,
                    run_id,
                    token=_ownership_token(paused),
                    phase="paused_waiting_resume",
                )
            except Exception as exc:
                logger.exception(
                    "durable pause drain failed for run_id=%s: %s",
                    run_id,
                    _exception_payload(exc),
                )
                await self._yield_run_if_owned(
                    run_id=run_id,
                    token=token,
                    phase="pause_drain_error",
                )
            completed += 1
            excluded.append(run_id)
        return completed

    async def _run_recovery_pass(self) -> int:
        completed = 0
        excluded: list[str] = []
        for _ in range(self._config.items_per_pass):
            claimed = await asyncio.to_thread(
                self._recovery_worker.execute_once,
                owner_id=self._owner_id,
                lease_seconds=self._config.lease_seconds,
                excluded_command_ids=tuple(excluded),
            )
            if not claimed:
                break
            completed += 1
            command_id = self._recovery_worker.last_claimed_command_id
            self._last_claimed_command_id = command_id
            if command_id:
                excluded.append(command_id)
        return completed

    async def _run_cancel_delivery_pass(self) -> int:
        completed = 0
        excluded: list[str] = []
        for _ in range(self._config.items_per_pass):
            claimed = await self._cancellation_delivery_worker.deliver_once(
                owner_id=self._owner_id,
                lease_seconds=self._config.lease_seconds,
                excluded_delivery_ids=tuple(excluded),
            )
            if not claimed:
                break
            completed += 1
            delivery_id = self._cancellation_delivery_worker.last_claimed_delivery_id
            self._last_claimed_delivery_id = delivery_id
            if delivery_id:
                excluded.append(delivery_id)
        return completed

    async def _run_attempt_pass(self, pass_once: Any) -> int:
        completed = 0
        excluded: list[str] = []
        for _ in range(self._config.items_per_pass):
            before = set(excluded)
            claimed = await pass_once(excluded_attempt_ids=tuple(excluded))
            if not claimed:
                break
            completed += 1
            latest = self._latest_claimed_identity("attempt", before)
            if latest:
                excluded.append(latest)
        return completed

    async def _run_finalizer_pass(self) -> int:
        completed = 0
        excluded: list[str] = []
        for _ in range(self._config.items_per_pass):
            before = set(excluded)
            claimed = await self.finalizer_pass_once(excluded_run_ids=tuple(excluded))
            if not claimed:
                break
            completed += 1
            latest = self._latest_claimed_identity("run", before)
            if latest:
                excluded.append(latest)
        return completed

    def _latest_claimed_identity(self, kind: str, before: set[str]) -> str | None:
        if kind == "run":
            identity = self._last_claimed_run_id
        elif kind == "attempt":
            identity = self._last_claimed_attempt_id
        else:
            raise DurableOrchestratorError(
                "bounded durable pass requested an unsupported claim identity kind",
                reason_code="multi_alpha_durable_claim_kind_invalid",
                context={"kind": kind},
            )
        return identity if identity and identity not in before else None


async def run_durable_multi_alpha_orchestrator(stop_event: asyncio.Event) -> None:
    try:
        await DurableMultiAlphaOrchestrator().run_forever(stop_event)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        error = _exception_payload(exc)
        mark_durable_orchestrator_unavailable(status="failed", error=error)
        logger.exception("multi-alpha durable orchestrator terminated: %s", error)
        raise


def _run_spec_from_row(run: Mapping[str, Any]) -> DurableRunSpec:
    return DurableRunSpec(
        run_id=str(run["id"]),
        task_id=str(run["task_id"]),
        request_hash=str(run["request_hash"]),
        roster_hash=str(run["roster_hash"]),
        roster=_mapping_sequence(run.get("roster_json")),
        oos_start=run["oos_start"],
        oos_end=run["oos_end"],
        normalize_method=str(run["normalize_method"]),
        walk_forward=_mapping(run.get("walk_forward_json")),
        backtest_config=_mapping(run.get("backtest_config_json")),
        baseline_leg_id=(str(run["baseline_leg_id"]) if run.get("baseline_leg_id") else None),
        retry_of_run_id=(str(run["retry_of_run_id"]) if run.get("retry_of_run_id") else None),
        node_parallelism=_mapping(run.get("node_parallelism_json")),
        recovery_kind=(str(run["recovery_kind"]) if run.get("recovery_kind") else None),
        recovery_scope=_mapping(run.get("recovery_scope_json")),
        recovery_scope_hash=(str(run["recovery_scope_hash"]) if run.get("recovery_scope_hash") else None),
        execution_identity=(
            _mapping(run.get("execution_identity_json"))
            if run.get("execution_identity_json") is not None
            else None
        ),
        execution_identity_hash=(
            str(run["execution_identity_hash"])
            if run.get("execution_identity_hash")
            else None
        ),
        execution_identity_evidence=(
            _mapping(run.get("execution_identity_evidence_json"))
            if run.get("execution_identity_evidence_json") is not None
            else None
        ),
    )


def _parent_status(
    *,
    children: Sequence[Mapping[str, Any]],
    run: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    run_status = str(run.get("status") or "")
    failures = {
        str(child["child_key"]): str(child["status"])
        for child in children
        if child["status"] != "succeeded"
    }
    baseline = next(
        (child for child in children if child["child_kind"] == "baseline"),
        None,
    )
    schemes = [child for child in children if child["child_kind"] == "scheme"]
    successful_scheme_count = sum(1 for child in schemes if child["status"] == "succeeded")
    if run_status in {"cancel_requested", "cancelling"}:
        if children and all(str(child["status"]) == "succeeded" for child in children):
            return "succeeded", {
                "reason_code": "cancel_raced_with_completion",
                "logical_status": "succeeded",
                "failed_child_tasks": {},
                "successful_scheme_count": successful_scheme_count,
                "successful_child_count": len(children),
                "cancelled_scope": [],
                "preserved_results": True,
            }
        cancelled_scope = [
            {
                "child_id": str(child["child_id"]),
                "child_key": str(child["child_key"]),
                "status": str(child["status"]),
            }
            for child in children
            if str(child["status"]) != "succeeded"
        ]
        return "cancelled", {
            "reason_code": "operator_cancelled",
            "logical_status": "cancelled",
            "failed_child_tasks": failures,
            "successful_scheme_count": successful_scheme_count,
            "successful_child_count": sum(
                1 for child in children if str(child["status"]) == "succeeded"
            ),
            "cancelled_scope": cancelled_scope,
            "preserved_results": any(
                str(child["status"]) == "succeeded" for child in children
            ),
        }
    preserved_unavailable = [
        {
            "child_id": str(child["child_id"]),
            "child_key": str(child["child_key"]),
            "status": str(child["status"]),
        }
        for child in children
        if str(child.get("status") or "") == "not_recovered"
    ]
    recovered_scope_children = [
        child for child in children if str(child.get("status") or "") != "not_recovered"
    ]
    recovered_scope_succeeded = bool(recovered_scope_children) and all(
        str(child.get("status") or "") == "succeeded"
        for child in recovered_scope_children
    )
    if (
        str(run.get("recovery_kind") or "") == "child_targeted"
        and preserved_unavailable
        and recovered_scope_succeeded
    ):
        return "partial_recovered", {
            "reason_code": "recovery_scope_completed_with_preserved_unavailable",
            "logical_status": "partial_recovered",
            "failed_child_tasks": failures,
            "successful_scheme_count": successful_scheme_count,
            "successful_child_count": sum(
                1 for child in children if str(child.get("status") or "") == "succeeded"
            ),
            "preserved_unavailable": preserved_unavailable,
        }
    if run.get("baseline_leg_id") and (baseline is None or baseline["status"] != "succeeded"):
        status = "failed"
        reason_code = "multi_alpha_baseline_failed"
    elif successful_scheme_count == 0:
        status = "failed"
        reason_code = "multi_alpha_no_successful_scheme"
    elif failures:
        status = "partial_failed"
        reason_code = "combine_backtest_child_tasks_failed"
    else:
        status = "succeeded"
        reason_code = "multi_alpha_combine_succeeded"
    return status, {
        "reason_code": reason_code,
        "logical_status": status,
        "failed_child_tasks": failures,
        "successful_scheme_count": successful_scheme_count,
    }


def _ownership_token(row: Mapping[str, Any]) -> OwnershipToken:
    return OwnershipToken(
        owner_id=str(row.get("owner_id") or ""),
        fencing_token=int(row.get("fencing_token") or 0),
        row_version=int(row.get("row_version") or 0),
    )


def _exception_payload(exc: Exception) -> dict[str, Any]:
    return {
        "reason_code": str(getattr(exc, "reason_code", type(exc).__name__)),
        "message": str(exc),
        "context": dict(getattr(exc, "context", {}) or {}),
        "error_type": type(exc).__name__,
    }


def _receipt_evidence(receipt: Any) -> dict[str, Any]:
    fields = (
        "schema_version",
        "task_id",
        "loop_id",
        "status",
        "submission_intent_hash",
        "request_digest",
        "created_at",
        "updated_at",
        "started_at",
        "running_at",
        "finished_at",
        "pid",
        "process_identity",
    )
    return {field: getattr(receipt, field, None) for field in fields}


def _deadline_evidence_from_attempt(
    attempt: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    result_manifest = _mapping(attempt.get("result_manifest_json"))
    raw = result_manifest.get("execution_deadline")
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise DurableOrchestratorError(
            "persisted execution deadline evidence is not an object",
            reason_code="multi_alpha_deadline_evidence_invalid",
            context={"attempt_id": attempt.get("attempt_id")},
        )
    evidence: dict[str, Mapping[str, Any]] = {}
    for kind, payload in raw.items():
        if str(kind) not in {"scheme", "run"} or not isinstance(payload, Mapping):
            raise DurableOrchestratorError(
                "persisted execution deadline evidence contains an invalid entry",
                reason_code="multi_alpha_deadline_evidence_invalid",
                context={
                    "attempt_id": attempt.get("attempt_id"),
                    "deadline_kind": kind,
                },
            )
        evidence[str(kind)] = dict(payload)
    return evidence


def _execution_deadline_evidence(
    *,
    run: Mapping[str, Any],
    attempt: Mapping[str, Any],
    scheme_timeout_seconds: int,
    run_timeout_seconds: int,
    remote_status: str,
    remote_payload: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    effective_observed_at = datetime.now(timezone.utc)
    timestamp_source = "orchestrator_observed_at"
    receipt_raw = remote_payload.get("submission_receipt")
    receipt = dict(receipt_raw) if isinstance(receipt_raw, Mapping) else {}
    terminal_remote = remote_status in (
        {"completed"} | REMOTE_FAILURE_STATUSES | REMOTE_CANCELLED_STATUSES
    )
    if terminal_remote and receipt.get("finished_at"):
        effective_observed_at = _as_utc_datetime(
            receipt["finished_at"],
            field_name="submission_receipt.finished_at",
        )
        timestamp_source = "submission_receipt.finished_at"

    evidence: dict[str, Mapping[str, Any]] = {}
    candidates = (
        ("scheme", attempt.get("submitted_at"), scheme_timeout_seconds),
        ("run", run.get("started_at") or run.get("created_at"), run_timeout_seconds),
    )
    for kind, start_value, timeout_seconds in candidates:
        if start_value is None:
            continue
        if isinstance(timeout_seconds, bool) or int(timeout_seconds) <= 0:
            raise DurableOrchestratorError(
                "execution deadline timeout must be a positive integer",
                reason_code="multi_alpha_execution_deadline_invalid",
                context={"deadline_kind": kind, "timeout_seconds": timeout_seconds},
            )
        started_at = _as_utc_datetime(
            start_value,
            field_name=f"{kind}_deadline.started_at",
        )
        deadline_at = started_at + timedelta(seconds=int(timeout_seconds))
        if effective_observed_at <= deadline_at:
            continue
        evidence[kind] = {
            "timeout_seconds": int(timeout_seconds),
            "started_at": _utc_iso(started_at),
            "deadline_at": _utc_iso(deadline_at),
            "effective_observed_at": _utc_iso(effective_observed_at),
            "elapsed_seconds": round(
                (effective_observed_at - started_at).total_seconds(),
                6,
            ),
            "timestamp_source": timestamp_source,
            "remote_status": remote_status,
        }
    return evidence


def _as_utc_datetime(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise DurableOrchestratorError(
                "execution deadline timestamp is empty",
                reason_code="multi_alpha_execution_deadline_timestamp_invalid",
                context={"field": field_name},
            )
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DurableOrchestratorError(
                "execution deadline timestamp is invalid",
                reason_code="multi_alpha_execution_deadline_timestamp_invalid",
                context={"field": field_name, "value": raw},
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _required(kind: str, identity: Any, value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        raise DurableOrchestratorError(
            f"durable {kind} is missing",
            reason_code=f"multi_alpha_durable_{kind}_not_found",
            context={f"{kind}_id": identity},
        )
    return value


def _mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise DurableOrchestratorError(
            "durable JSON object field has an invalid type",
            reason_code="multi_alpha_durable_json_contract_invalid",
            context={"actual_type": type(value).__name__},
        )
    return dict(value)


def _mapping_sequence(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise DurableOrchestratorError(
            "durable JSON array field has an invalid type",
            reason_code="multi_alpha_durable_json_contract_invalid",
            context={"actual_type": type(value).__name__},
        )
    rows: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise DurableOrchestratorError(
                "durable JSON array must contain objects",
                reason_code="multi_alpha_durable_json_contract_invalid",
                context={"actual_type": type(item).__name__},
            )
        rows.append(dict(item))
    return rows


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise DurableOrchestratorError(
            f"{name} must be an integer",
            reason_code="multi_alpha_durable_config_invalid",
            context={"name": name, "value": raw},
        ) from exc
    return value


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise DurableOrchestratorError(
            f"{name} must be numeric",
            reason_code="multi_alpha_durable_config_invalid",
            context={"name": name, "value": raw},
        ) from exc
    return value
