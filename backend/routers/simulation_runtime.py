"""Operator API for unified LocalSim and MiniQMT simulation runtime."""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from backend.infra.qmt_client import get_qmt_client_singleton
from backend.services.miniqmt_execution_runtime import (
    JsonFileMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionRuntimeClient,
    MiniQMTOperatorCommandStatus,
    QmtClientMiniQMTGateway,
)
from backend.services.simulation_runtime import SimulationBrokerBackend, SimulationDailyRunStatus
from backend.services.simulation_runtime.models import OperatorCommand
from backend.services.simulation_runtime.ops import SimulationRuntimeOpsService
from backend.services.trading_core.errors import DataUnavailableError, TradingCoreError, UnsupportedFeatureError

router = APIRouter(prefix="/simulation-runtime", tags=["simulation-runtime"])

DESTRUCTIVE_MINIQMT_OPERATOR_COMMANDS = frozenset(
    {"CANCEL_ALL_OPEN_ORDERS", "FLATTEN_ALL_POSITIONS", "FLATTEN_STRATEGY_SLOT", "RESET_STRATEGY_SLOT"}
)


def get_simulation_runtime_ops_service() -> SimulationRuntimeOpsService:
    return SimulationRuntimeOpsService()


def get_miniqmt_runtime_client() -> MiniQMTExecutionRuntimeClient:
    store_path = Path(
        os.getenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH")
        or "tmp/miniqmt_execution_runtime/runtime-state.json"
    )
    return MiniQMTExecutionRuntimeClient(repository=JsonFileMiniQMTExecutionRuntimeRepository(store_path))


def get_miniqmt_gateway() -> QmtClientMiniQMTGateway:
    return QmtClientMiniQMTGateway(
        qmt_client=get_qmt_client_singleton(),
        order_remark_prefix="simrt-opcmd",
    )


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


def _require_operator_confirmation(command: OperatorCommand, confirm_text: str | None) -> None:
    expected = f"EXECUTE {command.command_type}"
    if command.command_type not in DESTRUCTIVE_MINIQMT_OPERATOR_COMMANDS:
        return
    if str(confirm_text or "").strip().upper() == expected:
        return
    raise HTTPException(
        status_code=409,
        detail={
            "error_code": "MINIQMT_OPERATOR_CONFIRMATION_REQUIRED",
            "message": "destructive MiniQMT operator command requires explicit confirmation",
            "context": {
                "command_type": command.command_type,
                "expected_confirm_text": expected,
            },
        },
    )


def _operator_payload(command: OperatorCommand, raw_payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(command.payload or {})
    if command.strategy_slot_id:
        payload.setdefault("strategy_slot_id", command.strategy_slot_id)
    if command.alpha_signal_book_id:
        payload.setdefault("alpha_signal_book_id", command.alpha_signal_book_id)
    if isinstance(raw_payload.get("positions"), list):
        payload["positions"] = raw_payload["positions"]
    return payload


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


@router.post("/miniqmt/operator-commands")
def execute_miniqmt_operator_command(
    payload: dict[str, Any] = Body(...),
    runtime_client: MiniQMTExecutionRuntimeClient = Depends(get_miniqmt_runtime_client),
    gateway: Any | None = Depends(get_miniqmt_gateway),
) -> dict[str, Any]:
    try:
        command_fields = {
            key: payload[key]
            for key in (
                "command_id",
                "command_type",
                "account_group_id",
                "strategy_slot_id",
                "alpha_signal_book_id",
                "requested_by",
                "reason",
                "payload",
            )
            if key in payload
        }
        command = OperatorCommand.model_validate(command_fields)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "MINIQMT_OPERATOR_COMMAND_INVALID",
                "message": "invalid MiniQMT operator command payload",
                "context": {"reason": str(exc)},
            },
        ) from exc
    _require_operator_confirmation(command, str(payload.get("confirm_text") or ""))
    trade_date_raw = payload.get("trade_date")
    runtime_config_hash = str(payload.get("runtime_config_hash") or "").strip()
    if not trade_date_raw or not runtime_config_hash:
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "MINIQMT_OPERATOR_RUNTIME_CONTEXT_REQUIRED",
                "message": "trade_date and runtime_config_hash are required",
                "context": {
                    "trade_date_present": bool(trade_date_raw),
                    "runtime_config_hash_present": bool(runtime_config_hash),
                },
            },
        )
    try:
        trade_date = date.fromisoformat(str(trade_date_raw))
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "MINIQMT_OPERATOR_TRADE_DATE_INVALID",
                "message": "trade_date must be ISO date format",
                "context": {"trade_date": trade_date_raw},
            },
        ) from exc

    result, evidence = runtime_client.execute_operator_command(
        account_group_id=command.account_group_id,
        trade_date=trade_date,
        runtime_config_hash=runtime_config_hash,
        runtime_id=str(payload.get("runtime_id") or "").strip() or None,
        command_id=command.command_id,
        command_type=command.command_type,
        reason=command.reason,
        payload=_operator_payload(command, payload),
        gateway=gateway,
        source="simulation_runtime_operator_command",
    )
    return {
        "ok": result.status == MiniQMTOperatorCommandStatus.EXECUTED,
        "operator_command": command.model_dump(mode="json"),
        "result": result.model_dump(mode="json"),
        "runtime_evidence": evidence.to_dict(),
        "production_runtime_restart_required": False,
    }


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
