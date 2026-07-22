"""Read-only multi-alpha diagnostic APIs."""

from __future__ import annotations

import json
import time
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from backend.services.multi_alpha import (
    MultiAlphaCombineBacktestError,
    MultiAlphaCombineBacktestService,
    MultiAlphaCombiner,
    MultiAlphaCombinerError,
    MultiAlphaOrthogonalityError,
    MultiAlphaOrthogonalityService,
)
from backend.services.multi_alpha.combine_backtest import COMBINE_BACKTEST_STALE_FAIL_CONFIRM, error_payload
from backend.services.multi_alpha.combine_ui_adapter import (
    CombineUIAdapterError,
    MultiAlphaCombineUIAdapter,
    error_payload as combine_ui_error_payload,
)
from backend.services.multi_alpha.durable_submission import DurableCombineSubmissionError
from backend.services.multi_alpha.durable_control import (
    DurableControlError,
    DurableMultiAlphaControlService,
)
from backend.services.multi_alpha.durable_recovery import DurableRecoveryService
from backend.services.multi_alpha.durable_repository import (
    MultiAlphaDurableRepository,
    MultiAlphaDurableRepositoryError,
    TERMINAL_RUN_STATUSES,
)


router = APIRouter(prefix="/multi-alpha", tags=["multi-alpha"])


class CombineLegPayload(BaseModel):
    id: str
    pred_frame: list[dict[str, Any]]
    ic: float | None = None
    topk_return: float | None = None
    realized_returns: list[float] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CombinePreviewRequest(BaseModel):
    legs: list[CombineLegPayload]
    weighting_scheme: str = "equal"
    normalize_method: str = "zscore"
    walk_forward: dict[str, Any] | None = None
    head: int = Field(default=20, ge=0, le=1000)


class CombineBacktestRunRequest(BaseModel):
    task_id: str | None = None
    roster: list[dict[str, Any]]
    oos_start: str
    oos_end: str
    weighting_schemes: list[str] = Field(default_factory=lambda: ["equal", "orthogonality_aware", "ic_weighted", "risk_parity"])
    normalize_method: str = "zscore"
    walk_forward: dict[str, Any] = Field(default_factory=lambda: {"enabled": True, "window": 60, "min_periods": 2})
    rank_fusion: dict[str, Any] = Field(default_factory=dict)
    backtest_config: dict[str, Any] = Field(default_factory=dict)
    baseline_leg_id: str | None = None
    topk: int = Field(default=20, ge=1, le=500)
    min_date_coverage: float = Field(default=0.8, gt=0, le=1)
    run_async: bool = True
    scheme_timeout_seconds: int | None = Field(default=None, ge=1)
    run_timeout_seconds: int | None = Field(default=None, ge=1)
    wait_timeout_seconds: int | None = Field(default=None, ge=1)


class CombineBacktestStaleFailRequest(BaseModel):
    max_age_seconds: int = Field(ge=1)
    dry_run: bool = True
    confirmation: str | None = None


class CombineBacktestRetryRequest(BaseModel):
    payload: CombineBacktestRunRequest | None = None


class CombineBacktestArchiveRequest(BaseModel):
    dry_run: bool = True


class DurableControlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str
    request: dict[str, Any] = Field(default_factory=dict)
    child_id: str | None = None
    attempt_id: str | None = None
    scope: dict[str, Any] | None = None


class DurableStopRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: dict[str, Any] = Field(default_factory=dict)


class DurableAttemptCancelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: dict[str, Any] = Field(default_factory=dict)


class DurableRecoveryPreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    retry_mode: str


class DurableRecoveryExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    retry_mode: str
    scope_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    preview_command_id: str = Field(pattern=r"^macmd_[0-9a-f]{64}$")


def _parse_run_ids(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        result.extend(part.strip() for part in str(value or "").split(",") if part.strip())
    return result


def _durable_control_http_error(exc: Exception) -> HTTPException:
    reason_code = str(getattr(exc, "reason_code", "multi_alpha_durable_control_failed"))
    context = dict(getattr(exc, "context", {}) or {})
    if reason_code in {"multi_alpha_entity_not_found", "multi_alpha_recovery_target_not_found"}:
        status_code = 404
    elif reason_code in {
        "control_idempotency_conflict",
        "multi_alpha_active_command_conflict",
        "control_cancel_in_progress",
        "multi_alpha_state_transition_conflict",
        "recovery_scope_stale",
        "multi_alpha_selected_attempt_conflict",
    }:
        status_code = 409
    elif reason_code in {"multi_alpha_p0_2_schema_unavailable", "multi_alpha_schema_unavailable"}:
        status_code = 503
    elif reason_code == "multi_alpha_durable_control_failed":
        status_code = 500
    else:
        status_code = 400
    return HTTPException(
        status_code=status_code,
        detail={"reason_code": reason_code, "message": str(exc), "context": context},
    )


def _durable_command_response(result: Any) -> dict[str, Any]:
    return {
        "status": "success",
        "data": {
            "command": dict(result.command),
            "idempotent_identity_confirmed": result.idempotent_identity_confirmed,
            "capabilities": dict(result.capabilities),
        },
    }


def _require_durable_run(repository: MultiAlphaDurableRepository, run_id: str) -> dict[str, Any]:
    run = repository.get_run(run_id)
    if run is None:
        raise DurableControlError(
            "durable multi-alpha run was not found",
            reason_code="multi_alpha_entity_not_found",
            context={"run_id": run_id},
        )
    return run


def _sse_message(*, event: str, data: Any, event_id: int | None = None) -> str:
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    serialized = json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"))
    lines.extend(f"data: {line}" for line in serialized.splitlines() or [""])
    return "\n".join(lines) + "\n\n"


def _durable_event_stream(
    *,
    repository: MultiAlphaDurableRepository,
    run_id: str,
    after_event_id: int,
    poll_interval_seconds: float = 1.0,
    heartbeat_seconds: float = 15.0,
):
    cursor = after_event_id
    last_emit = time.monotonic()
    while True:
        try:
            events = repository.list_events(run_id, after_event_id=cursor, limit=500)
            for event in events:
                event_id = int(event["event_id"])
                if event_id <= cursor:
                    raise MultiAlphaDurableRepositoryError(
                        "durable event cursor did not advance",
                        reason_code="multi_alpha_event_cursor_regressed",
                        context={"run_id": run_id, "cursor": cursor, "event_id": event_id},
                    )
                cursor = event_id
                last_emit = time.monotonic()
                yield _sse_message(event="durable_event", data=event, event_id=event_id)

            run = _require_durable_run(repository, run_id)
            if str(run.get("status") or "") in TERMINAL_RUN_STATUSES and not events:
                yield _sse_message(
                    event="stream_end",
                    data={"run_id": run_id, "status": run.get("status"), "last_event_id": cursor},
                    event_id=cursor if cursor > 0 else None,
                )
                return

            now = time.monotonic()
            if now - last_emit >= heartbeat_seconds:
                yield f": heartbeat run_id={run_id} after_event_id={cursor}\n\n"
                last_emit = now
            time.sleep(poll_interval_seconds)
        except GeneratorExit:
            return
        except Exception as exc:
            reason_code = str(getattr(exc, "reason_code", "multi_alpha_event_stream_failed"))
            context = dict(getattr(exc, "context", {}) or {})
            yield _sse_message(
                event="stream_error",
                data={
                    "run_id": run_id,
                    "reason_code": reason_code,
                    "message": str(exc),
                    "context": context,
                    "last_event_id": cursor,
                },
                event_id=cursor if cursor > 0 else None,
            )
            return


def _require_run_scoped_child(
    service: DurableMultiAlphaControlService,
    *,
    run_id: str,
    child_id: str,
) -> dict[str, Any]:
    child = service.repository.get_child(child_id)
    if child is None or str(child.get("run_id") or "") != run_id:
        raise DurableControlError(
            "durable child does not belong to this run",
            reason_code="multi_alpha_entity_not_found",
            context={"run_id": run_id, "child_id": child_id},
        )
    return child


def _require_run_scoped_attempt(
    service: DurableMultiAlphaControlService,
    *,
    run_id: str,
    attempt_id: str,
) -> dict[str, Any]:
    attempt = service.repository.get_attempt(attempt_id)
    if attempt is None or str(attempt.get("run_id") or "") != run_id:
        raise DurableControlError(
            "durable attempt does not belong to this run",
            reason_code="multi_alpha_entity_not_found",
            context={"run_id": run_id, "attempt_id": attempt_id},
        )
    return attempt


@router.get("/orthogonality", summary="Compute read-only orthogonality diagnostics for finalized alpha legs")
def get_multi_alpha_orthogonality(
    run_ids: Annotated[list[str], Query(description="Run ids, either repeated or comma-separated")],
    k: Annotated[int, Query(ge=1, le=500)] = 25,
) -> dict:
    try:
        data = MultiAlphaOrthogonalityService().compute(run_ids=_parse_run_ids(run_ids), k=k)
    except MultiAlphaOrthogonalityError as exc:
        status_code = 404 if "missing" in str(exc).lower() or "not found" in str(exc).lower() else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    return {"status": "success", "data": data}


@router.post("/combine/preview", summary="Preview pure in-memory multi-alpha prediction score combination")
def preview_multi_alpha_combination(request: CombinePreviewRequest) -> dict:
    try:
        legs = [leg.model_dump() for leg in request.legs]
        data = MultiAlphaCombiner().combine(
            legs=legs,
            weighting_scheme=request.weighting_scheme,
            normalize_method=request.normalize_method,
            walk_forward=request.walk_forward,
        ).to_payload(head=request.head)
    except MultiAlphaCombinerError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "success", "data": data}


@router.post("/combine-backtest/run", summary="Submit a Tier-1 multi-alpha combine-backtest job")
def submit_multi_alpha_combine_backtest(
    request: CombineBacktestRunRequest,
    response: Response,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().submit_run(
            request.model_dump(),
            idempotency_key=idempotency_key,
        )
    except DurableCombineSubmissionError as exc:
        raise HTTPException(status_code=exc.http_status_code, detail=error_payload(exc)) from exc
    except MultiAlphaCombineBacktestError as exc:
        raise HTTPException(status_code=400, detail=error_payload(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=error_payload(exc)) from exc
    if data.get("wait_timed_out") is True:
        response.status_code = 202
    return {"status": "success", "data": data}


@router.get("/combine-backtest/runs/{run_id}", summary="Get a multi-alpha combine-backtest run with scheme/LOO results")
def get_multi_alpha_combine_backtest(run_id: str) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().get_run(run_id)
    except MultiAlphaCombineBacktestError as exc:
        status_code = 404 if exc.reason_code == "run_not_found" else 400
        raise HTTPException(status_code=status_code, detail=error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.get(
    "/combine-backtest/runs/{run_id}/control-capabilities",
    summary="Read durable QE multi-alpha control state and evidence without hiding recovery directions",
)
def get_multi_alpha_durable_control_capabilities(
    run_id: str,
    child_id: str | None = None,
    attempt_id: str | None = None,
) -> dict:
    try:
        data = DurableMultiAlphaControlService().capabilities(
            run_id=run_id,
            child_id=child_id,
            attempt_id=attempt_id,
        )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return {"status": "success", "data": data}


@router.get(
    "/combine-backtest/runs/{run_id}/children",
    summary="List durable QE multi-alpha children with their exact selected-attempt state",
)
def list_multi_alpha_durable_children(
    run_id: str,
    include_attempts: bool = Query(default=False),
) -> dict:
    try:
        service = DurableMultiAlphaControlService()
        service.capabilities(run_id=run_id)
        children = service.repository.list_children(run_id)
        if include_attempts:
            attempts_by_child: dict[str, list[dict[str, Any]]] = {}
            for attempt in service.repository.list_attempts_for_run(run_id):
                attempts_by_child.setdefault(str(attempt.get("child_id") or ""), []).append(attempt)
            for child in children:
                selected_attempt_id = child.get("selected_attempt_id")
                child["attempts"] = [
                    {**attempt, "selected": attempt.get("attempt_id") == selected_attempt_id}
                    for attempt in attempts_by_child.get(str(child.get("child_id") or ""), [])
                ]
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return {"status": "success", "data": {"children": children, "count": len(children)}}


@router.get(
    "/combine-backtest/runs/{run_id}/events",
    summary="List append-only durable QE multi-alpha events by stable cursor",
)
def list_multi_alpha_durable_events(
    run_id: str,
    after_event_id: int = Query(default=0, ge=0),
    limit: int = Query(default=500, ge=1, le=1000),
) -> dict[str, Any]:
    try:
        repository = MultiAlphaDurableRepository()
        _require_durable_run(repository, run_id)
        rows = repository.list_events(run_id, after_event_id=after_event_id, limit=limit + 1)
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    events = rows[:limit]
    next_event_id = int(events[-1]["event_id"]) if events else after_event_id
    return {
        "status": "success",
        "data": {
            "run_id": run_id,
            "events": events,
            "count": len(events),
            "after_event_id": after_event_id,
            "next_event_id": next_event_id,
            "has_more": len(rows) > limit,
        },
    }


@router.get(
    "/combine-backtest/runs/{run_id}/events/stream",
    summary="Stream durable QE multi-alpha events with cursor replay",
)
def stream_multi_alpha_durable_events(
    run_id: str,
    after_event_id: int = Query(default=0, ge=0),
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
) -> StreamingResponse:
    header_cursor = 0
    if last_event_id is not None and str(last_event_id).strip():
        try:
            header_cursor = int(str(last_event_id).strip())
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={
                    "reason_code": "multi_alpha_invalid_event_cursor",
                    "message": "Last-Event-ID must be a non-negative integer",
                    "context": {"last_event_id": last_event_id},
                },
            ) from exc
        if header_cursor < 0:
            raise HTTPException(
                status_code=400,
                detail={
                    "reason_code": "multi_alpha_invalid_event_cursor",
                    "message": "Last-Event-ID must be a non-negative integer",
                    "context": {"last_event_id": last_event_id},
                },
            )
    cursor = max(after_event_id, header_cursor)
    try:
        repository = MultiAlphaDurableRepository()
        _require_durable_run(repository, run_id)
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return StreamingResponse(
        _durable_event_stream(repository=repository, run_id=run_id, after_event_id=cursor),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get(
    "/combine-backtest/runs/{run_id}/children/{child_id}",
    summary="Read one durable QE multi-alpha child and all append-only attempts",
)
def get_multi_alpha_durable_child(run_id: str, child_id: str) -> dict:
    try:
        service = DurableMultiAlphaControlService()
        child = _require_run_scoped_child(service, run_id=run_id, child_id=child_id)
        attempts = service.repository.list_attempts(child_id)
        capabilities = service.capabilities(run_id=run_id, child_id=child_id)
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return {
        "status": "success",
        "data": {"child": child, "attempts": attempts, "capabilities": capabilities},
    }


@router.get(
    "/combine-backtest/runs/{run_id}/children/{child_id}/attempts",
    summary="List append-only attempts for one durable child",
)
def list_multi_alpha_durable_child_attempts(run_id: str, child_id: str) -> dict:
    try:
        service = DurableMultiAlphaControlService()
        _require_run_scoped_child(service, run_id=run_id, child_id=child_id)
        attempts = service.repository.list_attempts(child_id)
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return {"status": "success", "data": {"attempts": attempts, "count": len(attempts)}}


@router.post(
    "/combine-backtest/runs/{run_id}/pause",
    summary="Persist cooperative durable pause; in-flight QE attempts continue to reconcile",
)
def pause_multi_alpha_durable_run(
    run_id: str,
    request: DurableStopRequest,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        result = DurableMultiAlphaControlService().submit(
            run_id=run_id,
            action="pause",
            idempotency_key=str(idempotency_key or ""),
            requested_by="multi_alpha_http_api",
            request=request.request,
        )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return _durable_command_response(result)


@router.post(
    "/combine-backtest/runs/{run_id}/resume",
    summary="Persist durable resume for a previously drained QE multi-alpha run",
)
def resume_multi_alpha_durable_run(
    run_id: str,
    request: DurableStopRequest,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        result = DurableMultiAlphaControlService().submit(
            run_id=run_id,
            action="resume",
            idempotency_key=str(idempotency_key or ""),
            requested_by="multi_alpha_http_api",
            request=request.request,
        )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return _durable_command_response(result)


@router.post(
    "/combine-backtest/runs/{run_id}/cancel",
    summary="Persist durable run cancellation; typed QE termination reconciles asynchronously",
)
def cancel_multi_alpha_durable_run(
    run_id: str,
    request: DurableStopRequest,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        result = DurableMultiAlphaControlService().submit(
            run_id=run_id,
            action="cancel",
            idempotency_key=str(idempotency_key or ""),
            requested_by="multi_alpha_http_api",
            request=request.request,
        )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return _durable_command_response(result)


@router.post(
    "/combine-backtest/runs/{run_id}/reconcile",
    summary="Persist a durable QE multi-alpha observation/reconciliation command",
)
def reconcile_multi_alpha_durable_run(
    run_id: str,
    request: DurableStopRequest,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        result = DurableMultiAlphaControlService().submit(
            run_id=run_id,
            action="reconcile",
            idempotency_key=str(idempotency_key or ""),
            requested_by="multi_alpha_http_api",
            request=request.request,
        )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return _durable_command_response(result)


@router.post(
    "/combine-backtest/runs/{run_id}/children/{child_id}/recovery/preview",
    summary="Preview frozen child recovery closure and evidence without submitting remote QE work",
)
def preview_multi_alpha_durable_child_recovery(
    run_id: str,
    child_id: str,
    request: DurableRecoveryPreviewRequest,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        service = DurableMultiAlphaControlService()
        _require_run_scoped_child(service, run_id=run_id, child_id=child_id)
        if not str(idempotency_key or "").strip():
            raise DurableControlError(
                "Idempotency-Key is required for a recovery preview",
                reason_code="multi_alpha_idempotency_key_required",
            )
        preview = DurableRecoveryService(service.repository).preview(
            source_run_id=run_id,
            target_child_id=child_id,
            retry_mode=request.retry_mode,
            idempotency_key=str(idempotency_key),
        )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    except Exception as exc:
        raise _durable_control_http_error(exc) from exc
    return {"status": "success", "data": preview.as_dict()}


@router.post(
    "/combine-backtest/runs/{run_id}/children/{child_id}/recovery",
    summary="Persist a frozen child-targeted QE recovery command; execution remains asynchronous",
)
def execute_multi_alpha_durable_child_recovery(
    run_id: str,
    child_id: str,
    request: DurableRecoveryExecuteRequest,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        service = DurableMultiAlphaControlService()
        _require_run_scoped_child(service, run_id=run_id, child_id=child_id)
        if not str(idempotency_key or "").strip():
            raise DurableControlError(
                "Idempotency-Key is required for durable control",
                reason_code="multi_alpha_idempotency_key_required",
            )
        preview = DurableRecoveryService(service.repository).preview(
            source_run_id=run_id,
            target_child_id=child_id,
            retry_mode=request.retry_mode,
            idempotency_key=str(idempotency_key),
        )
        if request.preview_command_id != preview.command_id:
            raise DurableControlError(
                "submitted recovery preview belongs to a different idempotent command",
                reason_code="recovery_scope_stale",
                context={
                    "submitted_preview_command_id": request.preview_command_id,
                    "current_command_id": preview.command_id,
                },
            )
        if request.scope_hash != preview.scope_hash:
            raise DurableControlError(
                "submitted recovery scope no longer matches the current frozen source facts",
                reason_code="recovery_scope_stale",
                context={
                    "submitted_scope_hash": request.scope_hash,
                    "current_scope_hash": preview.scope_hash,
                    "evidence": dict(preview.evidence),
                },
            )
        result = service.submit(
            run_id=run_id,
            action="child_retry",
            idempotency_key=str(idempotency_key),
            requested_by="multi_alpha_http_api",
            request={
                "retry_mode": request.retry_mode,
                "preview_scope_hash": preview.scope_hash,
            },
            child_id=child_id,
            scope=preview.scope,
        )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    except Exception as exc:
        raise _durable_control_http_error(exc) from exc
    payload = _durable_command_response(result)
    payload["data"]["recovery_preview"] = preview.as_dict()
    return payload


@router.get(
    "/combine-backtest/runs/{run_id}/commands",
    summary="List durable QE multi-alpha control/recovery commands by stable cursor",
)
def list_multi_alpha_durable_commands(
    run_id: str,
    after_command_seq: int | None = Query(default=None, ge=0),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict:
    try:
        commands = DurableMultiAlphaControlService().repository.list_commands(
            run_id,
            after_command_seq=after_command_seq,
            limit=limit,
        )
    except MultiAlphaDurableRepositoryError as exc:
        raise _durable_control_http_error(exc) from exc
    return {"status": "success", "data": {"commands": commands, "count": len(commands)}}


@router.get(
    "/combine-backtest/runs/{run_id}/commands/{command_id}",
    summary="Read one durable QE multi-alpha control or recovery command",
)
def get_multi_alpha_durable_command(run_id: str, command_id: str) -> dict:
    try:
        service = DurableMultiAlphaControlService()
        command = service.repository.get_command(command_id)
        if command is None or str(command.get("run_id") or "") != run_id:
            raise DurableControlError(
                "durable command does not belong to this run",
                reason_code="multi_alpha_entity_not_found",
                context={"run_id": run_id, "command_id": command_id},
            )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return {"status": "success", "data": {"command": command}}


@router.post(
    "/combine-backtest/runs/{run_id}/control",
    summary="Persist a durable QE multi-alpha control/recovery command; remote reconciliation is asynchronous",
)
def submit_multi_alpha_durable_control(
    run_id: str,
    request: DurableControlRequest,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        service = DurableMultiAlphaControlService()
        action = str(request.action or "").strip().lower()
        if action == "child_retry":
            raise DurableControlError(
                "child_retry requires the dedicated preview-bound recovery endpoint",
                reason_code="multi_alpha_invalid_control_command",
                context={
                    "required_endpoint": (
                        f"/combine-backtest/runs/{run_id}/children/"
                        "{child_id}/recovery"
                    ),
                },
            )
        else:
            result = service.submit(
                run_id=run_id,
                action=request.action,
                idempotency_key=str(idempotency_key or ""),
                requested_by="multi_alpha_http_api",
                request=request.request,
                child_id=request.child_id,
                attempt_id=request.attempt_id,
                scope=request.scope,
            )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return _durable_command_response(result)


@router.post(
    "/combine-backtest/runs/{run_id}/stop",
    summary="Compatibility alias for durable QE multi-alpha cancel; never pause or delete",
)
def stop_multi_alpha_durable_run(
    run_id: str,
    request: DurableStopRequest,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        result = DurableMultiAlphaControlService().submit(
            run_id=run_id,
            action="stop",
            idempotency_key=str(idempotency_key or ""),
            requested_by="multi_alpha_http_api",
            request=request.request,
        )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return {
        "status": "success",
        "data": {
            "command": dict(result.command),
            "idempotent_identity_confirmed": result.idempotent_identity_confirmed,
            "capabilities": dict(result.capabilities),
        },
    }


@router.post(
    "/combine-backtest/runs/{run_id}/attempts/{attempt_id}/cancel",
    summary="Persist exact-attempt cancellation; never broadcast to other QE loops",
)
def cancel_multi_alpha_durable_attempt(
    run_id: str,
    attempt_id: str,
    request: DurableAttemptCancelRequest,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict:
    try:
        service = DurableMultiAlphaControlService()
        attempt = _require_run_scoped_attempt(service, run_id=run_id, attempt_id=attempt_id)
        result = service.submit(
            run_id=run_id,
            action="attempt_cancel",
            idempotency_key=str(idempotency_key or ""),
            requested_by="multi_alpha_http_api",
            request=request.request,
            child_id=str(attempt["child_id"]),
            attempt_id=attempt_id,
        )
    except (DurableControlError, MultiAlphaDurableRepositoryError) as exc:
        raise _durable_control_http_error(exc) from exc
    return {
        "status": "success",
        "data": {
            "command": dict(result.command),
            "idempotent_identity_confirmed": result.idempotent_identity_confirmed,
            "capabilities": dict(result.capabilities),
        },
    }


@router.get("/combine-backtest/runs/{run_id}/retry-draft", summary="Build an auditable retry payload for a combine-backtest run")
def get_multi_alpha_combine_backtest_retry_draft(run_id: str) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().get_retry_draft(run_id)
    except MultiAlphaCombineBacktestError as exc:
        status_code = 404 if exc.reason_code == "run_not_found" else 400
        raise HTTPException(status_code=status_code, detail=error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.post("/combine-backtest/runs/{run_id}/retry", summary="Create a new combine-backtest run from a frozen or explicit retry payload")
def retry_multi_alpha_combine_backtest(run_id: str, request: CombineBacktestRetryRequest) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().retry_run(
            run_id,
            payload=request.payload.model_dump() if request.payload is not None else None,
        )
    except MultiAlphaCombineBacktestError as exc:
        if exc.reason_code == "run_not_found":
            status_code = 404
        elif exc.reason_code == "combine_backtest_retry_payload_required":
            status_code = 409
        else:
            status_code = 400
        raise HTTPException(status_code=status_code, detail=error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.delete("/combine-backtest/runs/{run_id}", summary="Delete one terminal combine-backtest run and optionally its workspace")
def delete_multi_alpha_combine_backtest(run_id: str, cleanup_workspace: bool = True) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().delete_run(run_id, cleanup_workspace=cleanup_workspace)
    except MultiAlphaCombineBacktestError as exc:
        if exc.reason_code in {"run_not_found", "combine_backtest_delete_missing"}:
            status_code = 404
        elif exc.reason_code == "recovery_source_copy_in_progress":
            status_code = 409
        else:
            status_code = 400
        raise HTTPException(status_code=status_code, detail=error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.get("/combine-backtest/runs/{run_id}/logs", summary="Get structured progress events and safe workspace log tails")
def get_multi_alpha_combine_backtest_logs(
    run_id: str,
    tail_lines: int = Query(default=200, ge=1, le=1000),
) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().get_run_logs(run_id, tail_lines=tail_lines)
    except MultiAlphaCombineBacktestError as exc:
        status_code = 404 if exc.reason_code == "run_not_found" else 400
        raise HTTPException(status_code=status_code, detail=error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.get("/combine-backtest/runs/{run_id}/archive-status", summary="Get QE Archive status for one combine-backtest run")
def get_multi_alpha_combine_backtest_archive_status(run_id: str) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().get_archive_status(run_id)
    except MultiAlphaCombineBacktestError as exc:
        status_code = 404 if exc.reason_code == "run_not_found" else 400
        raise HTTPException(status_code=status_code, detail=error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.get(
    "/combine-backtest/runs/{run_id}/archive-detail",
    summary="Read immutable QE Archive detail for one combine-backtest run",
)
def get_multi_alpha_combine_backtest_archive_detail(run_id: str) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().get_archive_snapshot(run_id)
    except MultiAlphaCombineBacktestError as exc:
        status_code = 404 if exc.reason_code == "combine_backtest_archive_snapshot_not_found" else 400
        raise HTTPException(status_code=status_code, detail=error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.post("/combine-backtest/runs/{run_id}/archive", summary="Preview or write one combine-backtest run to QE Archive")
def archive_multi_alpha_combine_backtest(run_id: str, request: CombineBacktestArchiveRequest) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().archive_run(run_id, dry_run=request.dry_run)
    except MultiAlphaCombineBacktestError as exc:
        status_code = 404 if exc.reason_code == "run_not_found" else 400
        raise HTTPException(status_code=status_code, detail=error_payload(exc)) from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail={"reason_code": "combine_backtest_archive_failed", "message": str(exc)},
        ) from exc
    return {"status": "success", "data": data}


@router.get("/combine-backtest/runs", summary="List multi-alpha combine-backtest runs")
def list_multi_alpha_combine_backtests(status: str | None = None, limit: int = Query(default=20, ge=1, le=200)) -> dict:
    data = MultiAlphaCombineBacktestService().list_runs(status=status, limit=limit)
    return {"status": "success", "data": {"runs": data, "count": len(data)}}


@router.post("/combine-backtest/runs/stale/mark-failed", summary="Dry-run or explicitly fail stale running combine-backtest runs")
def mark_stale_multi_alpha_combine_backtests_failed(request: CombineBacktestStaleFailRequest) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().mark_stale_running_runs_failed(
            max_age_seconds=request.max_age_seconds,
            dry_run=request.dry_run,
            confirmation=request.confirmation,
        )
    except MultiAlphaCombineBacktestError as exc:
        raise HTTPException(status_code=400, detail=error_payload(exc)) from exc
    return {
        "status": "success",
        "data": data,
        "confirmation_required": COMBINE_BACKTEST_STALE_FAIL_CONFIRM,
    }


@router.get("/combine/tasks", summary="List combine-backtest UI tasks grouped by roster/config")
def list_multi_alpha_combine_ui_tasks(
    status: str | None = None,
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    try:
        data = MultiAlphaCombineUIAdapter().list_tasks(status=status, limit=limit, offset=offset)
    except CombineUIAdapterError as exc:
        raise HTTPException(status_code=400, detail=combine_ui_error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.get("/combine/tasks/{task_key}", summary="Get one combine-backtest UI task with loops")
def get_multi_alpha_combine_ui_task(task_key: str, scheme: str | None = None) -> dict:
    try:
        data = MultiAlphaCombineUIAdapter().get_task(task_key, scheme=scheme)
    except CombineUIAdapterError as exc:
        status_code = 404 if exc.reason_code == "combine_ui_task_not_found" else 400
        raise HTTPException(status_code=status_code, detail=combine_ui_error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.get("/combine/tasks/{task_key}/trajectory", summary="Get combine-backtest UI trajectory")
def get_multi_alpha_combine_ui_trajectory(task_key: str, scheme: str | None = None) -> dict:
    try:
        data = MultiAlphaCombineUIAdapter().get_trajectory(task_key, scheme=scheme)
    except CombineUIAdapterError as exc:
        status_code = 404 if exc.reason_code == "combine_ui_task_not_found" else 400
        raise HTTPException(status_code=status_code, detail=combine_ui_error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.get("/combine/tasks/{task_key}/custom-evo-config", summary="Get combine-backtest UI config rows")
def get_multi_alpha_combine_ui_custom_evo_config(task_key: str, scheme: str | None = None) -> dict:
    try:
        data = MultiAlphaCombineUIAdapter().get_custom_evo_config(task_key, scheme=scheme)
    except CombineUIAdapterError as exc:
        status_code = 404 if exc.reason_code == "combine_ui_task_not_found" else 400
        raise HTTPException(status_code=status_code, detail=combine_ui_error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.get("/combine/tasks/{task_key}/loops/{loop_index}", summary="Get one combine-backtest UI loop")
def get_multi_alpha_combine_ui_loop(task_key: str, loop_index: int, scheme: str | None = None) -> dict:
    try:
        data = MultiAlphaCombineUIAdapter().get_loop(task_key, loop_index, scheme=scheme)
    except CombineUIAdapterError as exc:
        status_code = 404 if exc.reason_code in {"combine_ui_task_not_found", "combine_ui_loop_not_found"} else 400
        raise HTTPException(status_code=status_code, detail=combine_ui_error_payload(exc)) from exc
    return {"status": "success", "data": data}
