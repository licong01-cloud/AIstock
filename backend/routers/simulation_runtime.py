"""Operator API for unified LocalSim and MiniQMT simulation runtime."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from backend.services.simulation_runtime import SimulationBrokerBackend, SimulationDailyRunStatus
from backend.services.simulation_runtime.ops import SimulationRuntimeOpsService
from backend.services.trading_core.errors import DataUnavailableError, TradingCoreError, UnsupportedFeatureError

router = APIRouter(prefix="/simulation-runtime", tags=["simulation-runtime"])


def get_simulation_runtime_ops_service() -> SimulationRuntimeOpsService:
    return SimulationRuntimeOpsService()


def _raise_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, UnsupportedFeatureError):
        status_code = 422
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def _parse_backend(raw: str | None) -> SimulationBrokerBackend | None:
    if raw is None:
        return None
    try:
        return SimulationBrokerBackend(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "INVALID_BROKER_BACKEND",
                "message": "broker_backend must be local_sim or minqmt_sim",
                "context": {"broker_backend": raw},
            },
        ) from exc


def _parse_status(raw: str | None) -> SimulationDailyRunStatus | None:
    if raw is None:
        return None
    try:
        return SimulationDailyRunStatus(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "INVALID_SIMULATION_RUN_STATUS",
                "message": "status is not a valid SimulationDailyRunStatus",
                "context": {"status": raw},
            },
        ) from exc


@router.get("/scheduler/status")
def get_scheduler_status(
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "scheduler": service.scheduler_status()}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/runs")
def list_simulation_runs(
    trade_date: date | None = None,
    broker_backend: str | None = None,
    strategy_id: str | None = None,
    status: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        payload = service.list_runs(
            trade_date=trade_date,
            broker_backend=_parse_backend(broker_backend),
            strategy_id=strategy_id,
            status=_parse_status(status),
            limit=limit,
        )
        return {"ok": True, **payload}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/runs/{run_id}")
def get_simulation_run(
    run_id: str,
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, **service.get_run_detail(run_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/execution-plans/{plan_id}")
def get_execution_plan(
    plan_id: str,
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, **service.get_execution_plan_detail(plan_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/live-admission/evidence")
def get_live_admission_evidence(
    paper_v2_run_id: str,
    miniqmt_sim_run_id: str,
    target_broker_backend: str = "minqmt_live",
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        payload = service.build_live_admission_evidence(
            paper_v2_run_id=paper_v2_run_id,
            miniqmt_sim_run_id=miniqmt_sim_run_id,
            target_broker_backend=target_broker_backend,
        )
        return {"ok": True, **payload}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/scheduler/start")
def start_scheduler(
    interval_seconds: int | None = Body(None, ge=1, le=3600),
    default_submit: bool | None = Body(None),
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        return service.start_scheduler(interval_seconds=interval_seconds, default_submit=default_submit)
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/scheduler/stop")
def stop_scheduler(
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        return service.stop_scheduler()
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/scheduler/tick")
def scheduler_tick(
    as_of_time: datetime | None = Body(None),
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        return service.scheduler_tick(as_of_time=as_of_time)
    except TradingCoreError as exc:
        _raise_http(exc)
