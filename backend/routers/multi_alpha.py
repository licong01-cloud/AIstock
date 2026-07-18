"""Read-only multi-alpha diagnostic APIs."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

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


class CombineBacktestStaleFailRequest(BaseModel):
    max_age_seconds: int = Field(ge=1)
    dry_run: bool = True
    confirmation: str | None = None


class CombineBacktestRetryRequest(BaseModel):
    payload: CombineBacktestRunRequest | None = None


class CombineBacktestArchiveRequest(BaseModel):
    dry_run: bool = True


def _parse_run_ids(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        result.extend(part.strip() for part in str(value or "").split(",") if part.strip())
    return result


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
def submit_multi_alpha_combine_backtest(request: CombineBacktestRunRequest) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().submit_run(request.model_dump())
    except MultiAlphaCombineBacktestError as exc:
        raise HTTPException(status_code=400, detail=error_payload(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=error_payload(exc)) from exc
    return {"status": "success", "data": data}


@router.get("/combine-backtest/runs/{run_id}", summary="Get a multi-alpha combine-backtest run with scheme/LOO results")
def get_multi_alpha_combine_backtest(run_id: str) -> dict:
    try:
        data = MultiAlphaCombineBacktestService().get_run(run_id)
    except MultiAlphaCombineBacktestError as exc:
        status_code = 404 if exc.reason_code == "run_not_found" else 400
        raise HTTPException(status_code=status_code, detail=error_payload(exc)) from exc
    return {"status": "success", "data": data}


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
        status_code = 404 if exc.reason_code in {"run_not_found", "combine_backtest_delete_missing"} else 400
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
