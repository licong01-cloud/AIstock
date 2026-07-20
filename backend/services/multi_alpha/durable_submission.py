from __future__ import annotations

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


TERMINAL_DURABLE_RUN_STATUSES = frozenset({"succeeded", "partial_failed", "failed", "cancelled"})
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

    def submit(
        self,
        payload: Mapping[str, Any],
        *,
        run_async_override: bool | None = None,
    ) -> dict[str, Any]:
        try:
            request = parse_request(payload)
            if run_async_override is not None:
                request = _replace_run_async(request, run_async_override)
            wait_timeout_seconds = (
                None if request.run_async else _wait_timeout_seconds(payload, request)
            )
            node_id = str(request.backtest_config.get("node_id") or "wsl2-5080")
            self._runtime_preflight(backtest_config=request.backtest_config, node_id=node_id)
            self._repository.preflight_schema(raise_on_error=True)
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
            run_spec = self._build_run_spec(
                task_id=str(task["task_id"]),
                request=request,
                roster_hash=roster_hash,
                roster=roster,
                retry_of_run_id=retry_of_run_id,
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

        response = _submission_payload(run, task_id=str(task["task_id"]))
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
        )
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
        )

    def _wait_for_terminal(
        self,
        *,
        run_id: str,
        task_id: str,
        timeout_seconds: int,
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
            response = _submission_payload(run, task_id=task_id)
            if response["status"] in TERMINAL_DURABLE_RUN_STATUSES:
                response["wait_timed_out"] = False
                return response
            if self._monotonic() >= deadline:
                response["wait_timed_out"] = True
                return response
            self._sleep(min(0.25, max(0.0, deadline - self._monotonic())))


def _submission_payload(run: Mapping[str, Any], *, task_id: str) -> dict[str, Any]:
    reason = run.get("reason") if isinstance(run.get("reason"), Mapping) else {}
    progress = run.get("progress_json") if isinstance(run.get("progress_json"), Mapping) else reason.get("progress")
    return {
        "task_id": task_id,
        "run_id": str(run["id"]),
        "status": str(run.get("status") or "queued"),
        "phase": str(run.get("phase") or reason.get("phase") or "submitted"),
        "progress": dict(progress) if isinstance(progress, Mapping) else {},
        "durable": True,
    }


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
