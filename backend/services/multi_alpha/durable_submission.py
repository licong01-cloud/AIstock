from __future__ import annotations

import hashlib
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Mapping

from backend.services.multi_alpha.combine_backtest import (
    CombineBacktestRequest,
    MultiAlphaCombineBacktestError,
    make_run_id,
    parse_request,
    persisted_backtest_config_for,
    preflight_pred_backtest_runtime,
    request_snapshot_for,
    roster_hash_for,
    validate_node_parallelism,
)
from backend.services.multi_alpha.durable_models import (
    DurableContractError,
    DurableRunSpec,
    DurableTaskSpec,
    durable_run_request_payload,
    implicit_task_group_key,
    make_implicit_task_id,
    request_hash_for,
)
from backend.services.multi_alpha.durable_identity import (
    DurableExecutionIdentityResolver,
    ExecutionIdentityResolution,
)
from backend.services.multi_alpha.durable_repository import (
    MultiAlphaDurableRepository,
    MultiAlphaDurableRepositoryError,
)
from backend.services.multi_alpha.durable_runtime_health import (
    DurableOrchestratorUnavailableError,
    require_durable_orchestrator_ready,
)
from backend.services.quantevolver.qe_execution_reservation import (
    QEExecutionReservationError,
    QEExecutionReservationRepository,
)


TERMINAL_DURABLE_RUN_STATUSES = frozenset(
    {"succeeded", "partial_failed", "partial_recovered", "failed", "cancelled"}
)
RuntimePreflight = Callable[..., None]
ExecutionSchemaPreflight = Callable[[], None]
OrchestratorReadinessPreflight = Callable[[], Mapping[str, Any]]
Clock = Callable[[], datetime]
MonotonicClock = Callable[[], float]
Sleeper = Callable[[float], None]


class DurableCombineSubmissionError(MultiAlphaCombineBacktestError):
    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        http_status_code: int,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message, reason_code=reason_code, context=context)
        self.http_status_code = http_status_code


class DurableCombineSubmissionService:
    """Create durable QE-only multi-alpha runs without owning execution lifetime."""

    def __init__(
        self,
        *,
        repository: MultiAlphaDurableRepository | None = None,
        runtime_preflight: RuntimePreflight = preflight_pred_backtest_runtime,
        execution_schema_preflight: ExecutionSchemaPreflight | None = None,
        orchestrator_readiness_preflight: OrchestratorReadinessPreflight = (
            require_durable_orchestrator_ready
        ),
        clock: Clock,
        monotonic: MonotonicClock = time.monotonic,
        sleep: Sleeper = time.sleep,
        source_kind: str = "api",
        created_by: str = "multi_alpha_durable_submission",
        execution_identity_resolver: DurableExecutionIdentityResolver | None = None,
    ) -> None:
        self._repository = repository or MultiAlphaDurableRepository()
        self._runtime_preflight = runtime_preflight
        self._execution_schema_preflight = (
            execution_schema_preflight or _preflight_execution_schema
        )
        self._orchestrator_readiness_preflight = orchestrator_readiness_preflight
        self._clock = clock
        self._monotonic = monotonic
        self._sleep = sleep
        self._source_kind = source_kind
        self._created_by = created_by
        self._execution_identity_resolver = execution_identity_resolver or DurableExecutionIdentityResolver()

    def submit(
        self,
        payload: Mapping[str, Any],
        *,
        run_async_override: bool | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        try:
            normalized_idempotency_key = _normalize_submission_idempotency_key(idempotency_key)
            request = parse_request(payload)
            if run_async_override is not None:
                request = _replace_run_async(request, run_async_override)
            wait_timeout_seconds = (
                None if request.run_async else _wait_timeout_seconds(payload, request)
            )
            node_id = str(request.backtest_config.get("node_id") or "wsl2-5080")
            self._runtime_preflight(backtest_config=request.backtest_config, node_id=node_id)
            self._repository.preflight_schema(raise_on_error=True)
            # P0-2 is an additive control/recovery capability.  Its DDL must
            # not turn the already-deployed P0-1B submission path into a new
            # research admission gate while deployment is staged.  Capture the
            # exact readiness fact and preserve it in the response; only code
            # that actually uses P0-2 command/recovery tables requires the
            # stronger preflight.
            p0_2_schema_health = self._repository.preflight_p0_2_schema(raise_on_error=False)
            self._execution_schema_preflight()
            self._orchestrator_readiness_preflight()
            roster_hash = roster_hash_for(request.roster)
            roster = request_snapshot_for(request)["roster"]
            group_key = implicit_task_group_key(
                roster_hash=roster_hash,
                normalize_method=request.normalize_method,
                walk_forward=request.walk_forward,
            )
            task = self._resolve_task(
                payload=payload,
                request=request,
                roster_hash=roster_hash,
                roster=roster,
                group_key=group_key,
            )
            retry_of_run_id = _optional_prefixed_identity(
                payload.get("retry_of_run_id"),
                prefix="macb_",
                field_name="retry_of_run_id",
            )
            execution_identity = self._resolve_execution_identity(
                request=request,
                node_id=node_id,
                p0_2_schema_ready=p0_2_schema_health.ready,
                p0_2_schema_health=p0_2_schema_health.as_dict(),
            )
            run_spec = self._build_run_spec(
                task_id=str(task["task_id"]),
                request=request,
                roster_hash=roster_hash,
                roster=roster,
                retry_of_run_id=retry_of_run_id,
                execution_identity=execution_identity,
                persist_execution_identity=p0_2_schema_health.ready,
                idempotency_key=normalized_idempotency_key,
            )
            run = self._repository.create_run(run_spec)
        except DurableCombineSubmissionError:
            raise
        except MultiAlphaDurableRepositoryError as exc:
            raise _repository_error(exc) from exc
        except QEExecutionReservationError as exc:
            raise DurableCombineSubmissionError(
                "QE execution reservation schema is unavailable",
                reason_code="multi_alpha_durable_execution_schema_unavailable",
                http_status_code=503,
                context=exc.context,
            ) from exc
        except DurableOrchestratorUnavailableError as exc:
            raise DurableCombineSubmissionError(
                "QE-only durable multi-alpha worker is unavailable",
                reason_code="multi_alpha_durable_orchestrator_unavailable",
                http_status_code=503,
                context={"worker_health": exc.health},
            ) from exc
        except DurableContractError as exc:
            raise _contract_error(exc) from exc

        response = _submission_payload(
            run,
            task_id=str(task["task_id"]),
            execution_identity_evidence=execution_identity.evidence,
            execution_identity_persisted=p0_2_schema_health.ready,
        )
        if request.run_async:
            return response
        if wait_timeout_seconds is None:
            raise DurableCombineSubmissionError(
                "synchronous durable submission lost its validated wait timeout",
                reason_code="multi_alpha_wait_timeout_internal_error",
                http_status_code=500,
                context={"run_id": run_spec.run_id},
            )
        return self._wait_for_terminal(
            run_id=run_spec.run_id,
            task_id=str(task["task_id"]),
            timeout_seconds=wait_timeout_seconds,
            execution_identity_evidence=execution_identity.evidence,
            execution_identity_persisted=p0_2_schema_health.ready,
        )

    def _resolve_task(
        self,
        *,
        payload: Mapping[str, Any],
        request: CombineBacktestRequest,
        roster_hash: str,
        roster: list[dict[str, Any]],
        group_key: str,
    ) -> Mapping[str, Any]:
        explicit_task_id = _optional_prefixed_identity(
            payload.get("task_id"),
            prefix="mact_",
            field_name="task_id",
        )
        if explicit_task_id is not None:
            task = self._repository.get_task(explicit_task_id)
            if task is None:
                raise DurableCombineSubmissionError(
                    "explicit multi-alpha task does not exist",
                    reason_code="multi_alpha_durable_task_not_found",
                    http_status_code=404,
                    context={"task_id": explicit_task_id},
                )
            self._repository.assert_task_compatible(
                task,
                roster_hash=roster_hash,
                roster=roster,
                normalize_method=request.normalize_method,
                walk_forward=request.walk_forward,
                legacy_group_key=group_key,
            )
            return task

        task = self._repository.find_task_for_implicit_group(
            legacy_group_key=group_key,
            roster_hash=roster_hash,
            roster=roster,
            normalize_method=request.normalize_method,
            walk_forward=request.walk_forward,
        )
        if task is not None:
            return task

        default_request = request_snapshot_for(request)
        spec = DurableTaskSpec(
            task_id=make_implicit_task_id(group_key),
            task_name=f"Multi-alpha {roster_hash} {request.normalize_method}",
            roster_hash=roster_hash,
            roster=roster,
            default_request=default_request,
            source_kind=self._source_kind,
            description="Implicit durable task created from the existing combine UI group identity.",
            legacy_group_key=group_key,
            created_by=self._created_by,
        )
        return self._repository.create_task(spec)

    def _build_run_spec(
        self,
        *,
        task_id: str,
        request: CombineBacktestRequest,
        roster_hash: str,
        roster: list[dict[str, Any]],
        retry_of_run_id: str | None,
        execution_identity: ExecutionIdentityResolution,
        persist_execution_identity: bool,
        idempotency_key: str | None,
    ) -> DurableRunSpec:
        node_id = str(request.backtest_config.get("node_id") or "wsl2-5080")
        node_parallelism = validate_node_parallelism(
            node_id=node_id,
            backtest_config=request.backtest_config,
        )
        persisted_backtest_config = persisted_backtest_config_for(request)
        request_payload = durable_run_request_payload(
            roster_hash=roster_hash,
            roster=roster,
            oos_start=request.oos_start,
            oos_end=request.oos_end,
            normalize_method=request.normalize_method,
            walk_forward=request.walk_forward,
            backtest_config=persisted_backtest_config,
            baseline_leg_id=request.baseline_leg_id,
            retry_of_run_id=retry_of_run_id,
            node_parallelism=node_parallelism,
            execution_identity=(
                execution_identity.identity.payload
                if persist_execution_identity and execution_identity.identity is not None
                else None
            ),
            execution_identity_hash=(
                execution_identity.identity.identity_hash
                if persist_execution_identity and execution_identity.identity is not None
                else None
            ),
            execution_identity_evidence=(execution_identity.evidence if persist_execution_identity else None),
        )
        if idempotency_key is not None:
            identity = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
            run_id = f"macb_idem_{identity[:40]}"
        else:
            timestamp = self._clock()
            base_run_id = make_run_id(
                roster_hash=roster_hash,
                oos_start=request.oos_start,
                oos_end=request.oos_end,
                ts=timestamp,
            )
            run_id = f"{base_run_id}_{uuid.uuid4().hex[:8]}"
        return DurableRunSpec(
            run_id=run_id,
            task_id=task_id,
            request_hash=request_hash_for(request_payload),
            roster_hash=roster_hash,
            roster=roster,
            oos_start=request.oos_start,
            oos_end=request.oos_end,
            normalize_method=request.normalize_method,
            walk_forward=request.walk_forward,
            backtest_config=persisted_backtest_config,
            baseline_leg_id=request.baseline_leg_id,
            retry_of_run_id=retry_of_run_id,
            node_parallelism=node_parallelism,
            execution_identity=(
                execution_identity.identity.payload
                if persist_execution_identity and execution_identity.identity is not None
                else None
            ),
            execution_identity_hash=(
                execution_identity.identity.identity_hash
                if persist_execution_identity and execution_identity.identity is not None
                else None
            ),
            execution_identity_evidence=(execution_identity.evidence if persist_execution_identity else None),
        )

    def _resolve_execution_identity(
        self,
        *,
        request: CombineBacktestRequest,
        node_id: str,
        p0_2_schema_ready: bool,
        p0_2_schema_health: Mapping[str, Any],
    ) -> ExecutionIdentityResolution:
        if not p0_2_schema_ready:
            # This does not reject, downgrade, or reinterpret the QE run.  It
            # explicitly tells the caller why P0-2-only immutable identity
            # fields cannot be persisted until the separately authorized DDL
            # is applied, while the P0-1B submission remains byte-for-byte
            # compatible with the deployed schema.
            return ExecutionIdentityResolution(
                identity=None,
                evidence={
                    "schema_version": "multi_alpha_execution_identity_evidence_v1",
                    "complete": False,
                    "reason_code": "multi_alpha_p0_2_schema_unavailable",
                    "missing": ["p0_2_execution_identity_persistence_schema"],
                    "acquisition_suggestions": [
                        "apply the separately authorized P0-2 additive migration in DEV before using durable recovery identity capture",
                        "continue the ordinary QE run; record this infrastructure limitation rather than treating it as a research rejection",
                    ],
                    "observations": {"p0_2_schema_health": dict(p0_2_schema_health)},
                },
            )
        try:
            return self._execution_identity_resolver.resolve(request=request, node_id=node_id)
        except Exception as exc:
            # Identity capture must never disappear behind a fallback or become
            # a research-direction admission gate.  Preserve the exact error in
            # the newly durable run so recovery/UI/MCP can surface it later.
            return ExecutionIdentityResolution(
                identity=None,
                evidence={
                    "schema_version": "multi_alpha_execution_identity_evidence_v1",
                    "complete": False,
                    "reason_code": "multi_alpha_execution_identity_capture_failed",
                    "missing": ["execution_identity_capture"],
                    "acquisition_suggestions": [
                        "inspect the recorded identity-capture error and restore the immutable dataset/runtime/prediction evidence",
                        "continue QE research analysis with the explicit identity limitation rather than treating it as a research rejection",
                    ],
                    "observations": {
                        "node_id": node_id,
                        "error": {"type": type(exc).__name__, "message": str(exc)},
                    },
                },
            )

    def _wait_for_terminal(
        self,
        *,
        run_id: str,
        task_id: str,
        timeout_seconds: int,
        execution_identity_evidence: Mapping[str, Any],
        execution_identity_persisted: bool,
    ) -> dict[str, Any]:
        deadline = self._monotonic() + timeout_seconds
        while True:
            run = self._repository.get_run(run_id)
            if run is None:
                raise DurableCombineSubmissionError(
                    "durable run disappeared while the synchronous caller was waiting",
                    reason_code="multi_alpha_durable_run_not_found",
                    http_status_code=500,
                    context={"run_id": run_id},
                )
            response = _submission_payload(
                run,
                task_id=task_id,
                execution_identity_evidence=execution_identity_evidence,
                execution_identity_persisted=execution_identity_persisted,
            )
            if response["status"] in TERMINAL_DURABLE_RUN_STATUSES:
                response["wait_timed_out"] = False
                return response
            if self._monotonic() >= deadline:
                response["wait_timed_out"] = True
                return response
            self._sleep(min(0.25, max(0.0, deadline - self._monotonic())))


def _submission_payload(
    run: Mapping[str, Any],
    *,
    task_id: str,
    execution_identity_evidence: Mapping[str, Any] | None = None,
    execution_identity_persisted: bool | None = None,
) -> dict[str, Any]:
    reason = run.get("reason") if isinstance(run.get("reason"), Mapping) else {}
    progress = run.get("progress_json") if isinstance(run.get("progress_json"), Mapping) else reason.get("progress")
    response = {
        "task_id": task_id,
        "run_id": str(run["id"]),
        "status": str(run.get("status") or "queued"),
        "phase": str(run.get("phase") or reason.get("phase") or "submitted"),
        "progress": dict(progress) if isinstance(progress, Mapping) else {},
        "durable": True,
    }
    if execution_identity_evidence is not None:
        response["execution_identity_evidence"] = dict(execution_identity_evidence)
    if execution_identity_persisted is not None:
        response["execution_identity_persisted"] = bool(execution_identity_persisted)
    return response


def _preflight_execution_schema() -> None:
    QEExecutionReservationRepository().preflight_schema(raise_on_error=True)


def _wait_timeout_seconds(payload: Mapping[str, Any], request: CombineBacktestRequest) -> int:
    value = payload.get("wait_timeout_seconds")
    if value is None:
        value = request.run_timeout_seconds
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise DurableCombineSubmissionError(
            "wait_timeout_seconds must be a positive integer",
            reason_code="multi_alpha_wait_timeout_invalid",
            http_status_code=400,
            context={"value": value},
        )
    return value


def _normalize_submission_idempotency_key(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or len(normalized) > 200:
        raise DurableCombineSubmissionError(
            "Idempotency-Key must contain 1 to 200 non-whitespace characters",
            reason_code="multi_alpha_submission_idempotency_key_invalid",
            http_status_code=400,
            context={"length": len(normalized)},
        )
    return normalized


def _optional_prefixed_identity(value: Any, *, prefix: str, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized.startswith(prefix) or len(normalized) <= len(prefix):
        raise DurableCombineSubmissionError(
            f"{field_name} must start with {prefix}",
            reason_code="multi_alpha_durable_identity_invalid",
            http_status_code=400,
            context={"field": field_name, "value": normalized},
        )
    return normalized


def _replace_run_async(request: CombineBacktestRequest, run_async: bool) -> CombineBacktestRequest:
    return CombineBacktestRequest(
        roster=request.roster,
        oos_start=request.oos_start,
        oos_end=request.oos_end,
        weighting_schemes=request.weighting_schemes,
        normalize_method=request.normalize_method,
        walk_forward=request.walk_forward,
        rank_fusion=request.rank_fusion,
        backtest_config=request.backtest_config,
        baseline_leg_id=request.baseline_leg_id,
        topk=request.topk,
        min_date_coverage=request.min_date_coverage,
        run_async=run_async,
        scheme_timeout_seconds=request.scheme_timeout_seconds,
        run_timeout_seconds=request.run_timeout_seconds,
    )


def _repository_error(exc: MultiAlphaDurableRepositoryError) -> DurableCombineSubmissionError:
    if exc.reason_code == "multi_alpha_schema_unavailable":
        status_code = 503
        reason_code = "multi_alpha_durable_schema_unavailable"
    elif exc.reason_code == "multi_alpha_p0_2_schema_unavailable":
        status_code = 503
        reason_code = "multi_alpha_durable_p0_2_schema_unavailable"
    elif "identity" in exc.reason_code or "conflict" in exc.reason_code:
        status_code = 409
        reason_code = exc.reason_code
    else:
        status_code = 500
        reason_code = exc.reason_code
    return DurableCombineSubmissionError(
        str(exc),
        reason_code=reason_code,
        http_status_code=status_code,
        context=exc.context,
    )


def _contract_error(exc: DurableContractError) -> DurableCombineSubmissionError:
    status_code = 409 if "identity" in exc.reason_code else 400
    return DurableCombineSubmissionError(
        str(exc),
        reason_code=exc.reason_code,
        http_status_code=status_code,
        context=exc.context,
    )
