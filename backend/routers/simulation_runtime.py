"""Operator API for unified LocalSim and MiniQMT simulation runtime."""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from backend.infra.qmt_client import get_qmt_client_singleton
from backend.services.miniqmt_execution_runtime import (
    MiniQMTExecutionRuntimeClient,
    MiniQMTOperatorCommandStatus,
    QmtClientMiniQMTEventLoopGateway,
    default_miniqmt_execution_runtime_repository,
)
from backend.services.miniqmt_execution_runtime.kernel_diagnostics import KernelDiagnosticsReadServiceV1
from backend.services.simulation_runtime import SimulationBrokerBackend, SimulationDailyRunStatus
from backend.services.simulation_runtime.models import OperatorCommand
from backend.services.simulation_runtime.localsim_cutover_readiness import LocalSimCutoverReadiness
from backend.services.simulation_runtime.localsim_dependencies import (
    build_localsim_product_service,
    build_localsim_profile_service,
    build_localsim_query_service,
    build_localsim_economic_query_service,
)
from backend.services.simulation_runtime.localsim_economic_query import LocalSimEconomicQueryService
from backend.services.simulation_runtime.localsim_product_control import (
    LocalSimAccountCreateRequestV1,
    LocalSimBulkLifecycleRequestV1,
    LocalSimBulkLifecycleResponseV1,
    LocalSimControlResponseV1,
    LocalSimLifecycleRequestV1,
    LocalSimProductControlPlaneService,
    LocalSimReplayCancelRequestV1,
    LocalSimReplayCreateRequestV1,
    LocalSimRuntimeProfileCreateRequestV1,
    LocalSimRuntimeProfileResponseV1,
    LocalSimRuntimeProfileRetireRequestV1,
    LocalSimRuntimeProfileVersionCreateRequestV1,
    LocalSimSuccessorReleaseRequestV1,
)
from backend.services.simulation_runtime.localsim_query import LocalSimListResponseV1, LocalSimQueryService
from backend.services.simulation_runtime.localsim_runtime_profile_service import LocalSimRuntimeProfileService
from backend.services.simulation_runtime.ops import SimulationRuntimeOpsService
from backend.services.simulation_runtime.successor_models import LocalSimReplayStatus, SimulationAccountStatus
from backend.services.simulation_runtime.tca_read_api import ExecutionTcaReadService
from backend.services.qmt_strategy_ledger.tca_read_service import TcaReadError
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    RuntimeConfigInvalidError,
    TradingCoreError,
    UnsupportedFeatureError,
)

router = APIRouter(prefix="/simulation-runtime", tags=["simulation-runtime"])

DESTRUCTIVE_MINIQMT_OPERATOR_COMMANDS = frozenset(
    {
        "CANCEL_ALL_OPEN_ORDERS",
        "FLATTEN_ALL_POSITIONS",
        "FLATTEN_STRATEGY_SLOT",
        "RESET_STRATEGY_SLOT",
        "RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
    }
)


def get_simulation_runtime_ops_service() -> SimulationRuntimeOpsService:
    return SimulationRuntimeOpsService(kernel_diagnostics_reader=KernelDiagnosticsReadServiceV1().read)


def get_miniqmt_runtime_client() -> MiniQMTExecutionRuntimeClient:
    return MiniQMTExecutionRuntimeClient(repository=default_miniqmt_execution_runtime_repository())


def get_miniqmt_runtime_repository() -> Any:
    return get_miniqmt_runtime_client().repository


def get_miniqmt_gateway() -> QmtClientMiniQMTEventLoopGateway:
    return QmtClientMiniQMTEventLoopGateway(
        qmt_client=get_qmt_client_singleton(),
        order_remark_prefix="simrt-opcmd",
    )


def get_execution_tca_read_service() -> ExecutionTcaReadService:
    return ExecutionTcaReadService()


def get_localsim_product_service() -> LocalSimProductControlPlaneService:
    return build_localsim_product_service()


def get_localsim_profile_service() -> LocalSimRuntimeProfileService:
    return build_localsim_profile_service()


def get_localsim_query_service() -> LocalSimQueryService:
    return build_localsim_query_service()


def get_localsim_economic_query_service() -> LocalSimEconomicQueryService:
    return build_localsim_economic_query_service()


def get_localsim_readiness() -> LocalSimCutoverReadiness:
    return LocalSimCutoverReadiness()


def _raise_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, UnsupportedFeatureError):
        status_code = 422
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def _raise_localsim_http(exc: TradingCoreError) -> None:
    reason_code = str((exc.context or {}).get("reason_code") or "")
    if reason_code == "LOCALSIM_CUTOVER_NOT_READY":
        status_code = 503
    elif isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, InvalidStateTransitionError):
        status_code = 409
    elif isinstance(exc, RuntimeConfigInvalidError):
        status_code = 422
    else:
        status_code = 400
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def _raise_tca_http(exc: TcaReadError) -> None:
    raise HTTPException(status_code=exc.http_status, detail=exc.to_dict()) from exc


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


def _required_query_text(value: str | None, field_name: str) -> str:
    raw = str(value or "").strip()
    if raw:
        return raw
    raise HTTPException(
        status_code=422,
        detail={
            "error_code": "MINIQMT_RUNTIME_QUERY_PARAMETER_REQUIRED",
            "message": f"{field_name} is required",
            "context": {"reason_code": "MINIQMT_RUNTIME_QUERY_PARAMETER_REQUIRED", "field": field_name},
        },
    )


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
    for field_name in ("run_id", "binding_id", "runtime_id"):
        if raw_payload.get(field_name):
            payload.setdefault(field_name, str(raw_payload[field_name]).strip())
    if isinstance(raw_payload.get("positions"), list):
        payload["positions"] = raw_payload["positions"]
    return payload


def _parse_localsim_enum(raw: str | None, enum_type: Any, *, field_name: str) -> Any | None:
    if raw is None:
        return None
    try:
        return enum_type(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "LOCALSIM_QUERY_ENUM_INVALID",
                "message": f"{field_name} is invalid",
                "context": {"field": field_name, "value": raw},
                "retryable": False,
            },
        ) from exc


@router.get("/localsim/cutover-readiness")
def get_localsim_cutover_readiness(
    readiness: LocalSimCutoverReadiness = Depends(get_localsim_readiness),
) -> dict[str, Any]:
    try:
        return {"ok": True, "readiness": readiness.read().model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.post("/localsim/accounts", response_model=LocalSimControlResponseV1)
def create_localsim_account(
    request: LocalSimAccountCreateRequestV1,
    service: LocalSimProductControlPlaneService = Depends(get_localsim_product_service),
) -> LocalSimControlResponseV1:
    try:
        return service.create_account(request, created_by="aistock_api")
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/accounts", response_model=LocalSimListResponseV1)
def list_localsim_accounts(
    package_id: str | None = Query(None),
    status: str | None = Query(None),
    cursor: str | None = Query(None),
    limit: int = Query(100, ge=1, le=200),
    service: LocalSimQueryService = Depends(get_localsim_query_service),
) -> LocalSimListResponseV1:
    try:
        return service.accounts(
            package_id=str(package_id).strip() if package_id else None,
            status=_parse_localsim_enum(status, SimulationAccountStatus, field_name="status"),
            cursor=cursor,
            limit=limit,
        )
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/accounts/{account_id}", response_model=LocalSimControlResponseV1)
def get_localsim_account(
    account_id: str,
    service: LocalSimQueryService = Depends(get_localsim_query_service),
) -> LocalSimControlResponseV1:
    try:
        account = service.repository.get_account(account_id)
        return LocalSimControlResponseV1(account=account, ledger_scope=service.repository.get_ledger_scope(account_id))
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/accounts/{account_id}/releases", response_model=LocalSimListResponseV1)
def list_localsim_account_releases(
    account_id: str,
    limit: int = Query(100, ge=1, le=200),
    service: LocalSimQueryService = Depends(get_localsim_query_service),
) -> LocalSimListResponseV1:
    try:
        return LocalSimListResponseV1(
            items=service.repository.list_releases_for_account(account_id, limit=limit),
            limit=limit,
        )
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/accounts/{account_id}/bindings", response_model=LocalSimListResponseV1)
def list_localsim_account_bindings(
    account_id: str,
    limit: int = Query(100, ge=1, le=200),
    service: LocalSimQueryService = Depends(get_localsim_query_service),
) -> LocalSimListResponseV1:
    try:
        return LocalSimListResponseV1(
            items=service.repository.list_bindings_for_account(account_id, limit=limit),
            limit=limit,
        )
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/accounts/{account_id}/runs")
def list_localsim_account_runs(
    account_id: str,
    limit: int = Query(100, ge=1, le=200),
    service: LocalSimEconomicQueryService = Depends(get_localsim_economic_query_service),
) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            "schema_version": "localsim_run_list_response_v1",
            "account_id": account_id,
            "runs": service.list_runs(account_id, limit=limit),
        }
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/accounts/{account_id}/ledger")
def get_localsim_account_ledger(
    account_id: str,
    limit: int = Query(200, ge=1, le=500),
    service: LocalSimEconomicQueryService = Depends(get_localsim_economic_query_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "ledger": service.ledger(account_id, limit=limit)}
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/accounts/{account_id}/performance")
def get_localsim_account_performance(
    account_id: str,
    limit: int = Query(500, ge=1, le=500),
    service: LocalSimEconomicQueryService = Depends(get_localsim_economic_query_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "performance": service.performance(account_id, limit=limit)}
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.post("/localsim/accounts/{account_id}/successor-releases", response_model=LocalSimControlResponseV1)
def create_localsim_successor_release(
    account_id: str,
    request: LocalSimSuccessorReleaseRequestV1,
    service: LocalSimProductControlPlaneService = Depends(get_localsim_product_service),
) -> LocalSimControlResponseV1:
    try:
        return service.create_successor_release(account_id=account_id, request=request, created_by="aistock_api")
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.post("/localsim/accounts/bulk-lifecycle", response_model=LocalSimBulkLifecycleResponseV1)
def transition_localsim_accounts_bulk(
    request: LocalSimBulkLifecycleRequestV1,
    service: LocalSimProductControlPlaneService = Depends(get_localsim_product_service),
) -> LocalSimBulkLifecycleResponseV1:
    try:
        return service.transition_accounts_bulk(request)
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.post("/localsim/accounts/{account_id}/{action}", response_model=LocalSimControlResponseV1)
def transition_localsim_account(
    account_id: str,
    action: Literal["pause", "resume", "retire"],
    request: LocalSimLifecycleRequestV1,
    service: LocalSimProductControlPlaneService = Depends(get_localsim_product_service),
) -> LocalSimControlResponseV1:
    try:
        return service.transition_account(
            account_id=account_id,
            action=action,
            expected_version=request.expected_version,
        )
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.post("/localsim/replays", response_model=LocalSimControlResponseV1)
def create_localsim_replay(
    request: LocalSimReplayCreateRequestV1,
    service: LocalSimProductControlPlaneService = Depends(get_localsim_product_service),
) -> LocalSimControlResponseV1:
    try:
        return service.create_replay(request, created_by="aistock_api")
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/replays", response_model=LocalSimListResponseV1)
def list_localsim_replays(
    simulation_account_id: str | None = Query(None),
    status: str | None = Query(None),
    cursor: str | None = Query(None),
    limit: int = Query(100, ge=1, le=200),
    service: LocalSimQueryService = Depends(get_localsim_query_service),
) -> LocalSimListResponseV1:
    try:
        return service.replays(
            simulation_account_id=str(simulation_account_id).strip() if simulation_account_id else None,
            status=_parse_localsim_enum(status, LocalSimReplayStatus, field_name="status"),
            cursor=cursor,
            limit=limit,
        )
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/replays/{replay_job_id}", response_model=LocalSimControlResponseV1)
def get_localsim_replay(
    replay_job_id: str,
    service: LocalSimQueryService = Depends(get_localsim_query_service),
) -> LocalSimControlResponseV1:
    try:
        return LocalSimControlResponseV1(replay=service.repository.get_replay_job(replay_job_id))
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.post("/localsim/replays/{replay_job_id}/cancel", response_model=LocalSimControlResponseV1)
def cancel_localsim_replay(
    replay_job_id: str,
    request: LocalSimReplayCancelRequestV1,
    service: LocalSimProductControlPlaneService = Depends(get_localsim_product_service),
) -> LocalSimControlResponseV1:
    try:
        return service.cancel_replay(replay_job_id=replay_job_id, expected_version=request.expected_version)
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.post("/localsim/runtime-profiles", response_model=LocalSimRuntimeProfileResponseV1)
def create_localsim_runtime_profile(
    request: LocalSimRuntimeProfileCreateRequestV1,
    service: LocalSimRuntimeProfileService = Depends(get_localsim_profile_service),
    readiness: LocalSimCutoverReadiness = Depends(get_localsim_readiness),
) -> LocalSimRuntimeProfileResponseV1:
    try:
        readiness.require_ready()
        return LocalSimRuntimeProfileResponseV1(
            profile=service.create_profile_for_package(
                package_id=request.package_id,
                profile_name=request.profile_name,
                created_by="aistock_api",
            )
        )
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.post(
    "/localsim/runtime-profiles/{profile_id}/versions",
    response_model=LocalSimRuntimeProfileResponseV1,
)
def create_localsim_runtime_profile_version(
    profile_id: str,
    request: LocalSimRuntimeProfileVersionCreateRequestV1,
    service: LocalSimRuntimeProfileService = Depends(get_localsim_profile_service),
    readiness: LocalSimCutoverReadiness = Depends(get_localsim_readiness),
) -> LocalSimRuntimeProfileResponseV1:
    try:
        readiness.require_ready()
        profile, version = service.create_version(
            profile_id=profile_id,
            expected_profile_version=request.expected_profile_version,
            config_json=request.config.model_dump(mode="json"),
            created_by="aistock_api",
        )
        return LocalSimRuntimeProfileResponseV1(profile=profile, version=version)
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.post(
    "/localsim/runtime-profiles/{profile_id}/retire",
    response_model=LocalSimRuntimeProfileResponseV1,
)
def retire_localsim_runtime_profile(
    profile_id: str,
    request: LocalSimRuntimeProfileRetireRequestV1,
    service: LocalSimRuntimeProfileService = Depends(get_localsim_profile_service),
    readiness: LocalSimCutoverReadiness = Depends(get_localsim_readiness),
) -> LocalSimRuntimeProfileResponseV1:
    try:
        readiness.require_ready()
        return LocalSimRuntimeProfileResponseV1(
            profile=service.retire_profile(profile_id=profile_id, expected_version=request.expected_version)
        )
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/runtime-profiles", response_model=LocalSimListResponseV1)
def list_localsim_runtime_profiles(
    package_id: str | None = Query(None),
    cursor: str | None = Query(None),
    limit: int = Query(100, ge=1, le=200),
    service: LocalSimQueryService = Depends(get_localsim_query_service),
) -> LocalSimListResponseV1:
    try:
        return service.profiles(package_id=package_id, cursor=cursor, limit=limit)
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/runtime-profiles/{profile_id}", response_model=LocalSimRuntimeProfileResponseV1)
def get_localsim_runtime_profile(
    profile_id: str,
    service: LocalSimRuntimeProfileService = Depends(get_localsim_profile_service),
) -> LocalSimRuntimeProfileResponseV1:
    try:
        return LocalSimRuntimeProfileResponseV1(profile=service.get_profile(profile_id))
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/localsim/runtime-profiles/{profile_id}/versions", response_model=LocalSimListResponseV1)
def list_localsim_runtime_profile_versions(
    profile_id: str,
    cursor: str | None = Query(None),
    limit: int = Query(100, ge=1, le=200),
    service: LocalSimQueryService = Depends(get_localsim_query_service),
) -> LocalSimListResponseV1:
    try:
        return service.profile_versions(profile_id=profile_id, cursor=cursor, limit=limit)
    except TradingCoreError as exc:
        _raise_localsim_http(exc)


@router.get("/execution-parents")
def list_execution_parents(
    binding_id: str = Query(""),
    trade_date: str = Query(""),
    terminal_state: str | None = Query(None),
    limit: str = Query("100"),
    cursor: str | None = Query(None),
    service: ExecutionTcaReadService = Depends(get_execution_tca_read_service),
) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            **service.list_execution_parents(
                binding_id=binding_id,
                trade_date=trade_date,
                terminal_state=terminal_state,
                limit=limit,
                cursor=cursor,
            ),
        }
    except TcaReadError as exc:
        _raise_tca_http(exc)


@router.get("/execution-parents/{parent_id}")
def get_execution_parent(
    parent_id: str,
    revision: str | None = Query(None),
    service: ExecutionTcaReadService = Depends(get_execution_tca_read_service),
) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            **service.get_execution_parent(parent_intent_id=parent_id, parent_revision=revision),
        }
    except TcaReadError as exc:
        _raise_tca_http(exc)


@router.get("/execution-parents/{parent_id}/tca")
def get_execution_tca(
    parent_id: str,
    revision: str = Query(""),
    snapshot_kind: str = Query(""),
    tca_version: str | None = Query(None),
    receipt_id: str | None = Query(None),
    as_of: str | None = Query(None),
    service: ExecutionTcaReadService = Depends(get_execution_tca_read_service),
) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            **service.get_execution_tca(
                parent_intent_id=parent_id,
                parent_revision=revision,
                snapshot_kind=snapshot_kind,
                tca_version=tca_version,
                receipt_id=receipt_id,
                as_of=as_of,
            ),
        }
    except TcaReadError as exc:
        _raise_tca_http(exc)


@router.get("/scheduler/status")
def get_scheduler_status(
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "scheduler": service.scheduler_status()}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/scheduler/verification-status")
def get_scheduler_verification_status(
    broker_backend: str | None = Query(None),
    run_id: str | None = Query(None),
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    """Return a bounded read-only scheduler smoke scoped to one broker or run subject."""

    try:
        return {
            "ok": True,
            "scheduler": service.scheduler_verification_status(
                broker_backend=_parse_backend(broker_backend),
                run_id=run_id,
            ),
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/platform-diagnostics")
def get_platform_diagnostics(
    trade_date: date | None = None,
    binding_id: str | None = None,
    run_id: str | None = None,
    runtime_id: str | None = None,
    plan_id: str | None = None,
    kernel_cursor: str | None = None,
    limit: int = Query(100, ge=1, le=100),
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
    runtime_repository: Any = Depends(get_miniqmt_runtime_repository),
) -> dict[str, Any]:
    """Return bounded platform health facts without mutating feeds, runs, DB, or broker state."""

    try:
        return {
            "ok": True,
            **service.platform_diagnostics(
                trade_date=trade_date,
                binding_id=binding_id,
                run_id=run_id,
                runtime_id=runtime_id,
                plan_id=plan_id,
                kernel_cursor=kernel_cursor,
                limit=limit,
                runtime_repository=runtime_repository,
            ),
        }
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


@router.get("/runs/{run_id}/terminal-evidence")
def get_simulation_run_terminal_evidence(
    run_id: str,
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, **service.get_run_terminal_evidence(run_id)}
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


@router.get("/miniqmt/runtime-events")
def list_miniqmt_runtime_events(
    runtime_id: str | None = None,
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
    runtime_repository: Any = Depends(get_miniqmt_runtime_repository),
) -> dict[str, Any]:
    try:
        payload = service.list_miniqmt_runtime_events(
            runtime_repository=runtime_repository,
            runtime_id=_required_query_text(runtime_id, "runtime_id"),
        )
        return {"ok": True, **payload}
    except HTTPException:
        raise
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/miniqmt/quote-diagnostics")
def list_miniqmt_quote_diagnostics(
    runtime_id: str | None = None,
    symbol: str | None = None,
    cursor: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
    runtime_repository: Any = Depends(get_miniqmt_runtime_repository),
) -> dict[str, Any]:
    """Strictly read-only P1-D quote evidence diagnostics."""

    try:
        payload = service.list_miniqmt_quote_diagnostics(
            runtime_repository=runtime_repository,
            runtime_id=_required_query_text(runtime_id, "runtime_id"),
            symbol=_required_query_text(symbol, "symbol") if symbol is not None else None,
            cursor=cursor,
            limit=limit,
        )
        return {"ok": True, **payload}
    except HTTPException:
        raise
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/miniqmt/quote-evidence")
def list_miniqmt_quote_evidence(
    runtime_id: str | None = None,
    market_data_id: str | None = None,
    evidence_id: str | None = None,
    include_archived: bool = False,
    cursor: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
    runtime_repository: Any = Depends(get_miniqmt_runtime_repository),
) -> dict[str, Any]:
    """Strictly read-only durable evidence envelope and link readback."""

    if (market_data_id is None) == (evidence_id is None):
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "MINIQMT_QUOTE_EVIDENCE_QUERY_INVALID",
                "message": "exactly one of market_data_id or evidence_id is required",
            },
        )
    try:
        payload = service.list_miniqmt_quote_evidence(
            runtime_repository=runtime_repository,
            runtime_id=_required_query_text(runtime_id, "runtime_id"),
            market_data_id=_required_query_text(market_data_id, "market_data_id")
            if market_data_id is not None
            else None,
            evidence_id=_required_query_text(evidence_id, "evidence_id") if evidence_id is not None else None,
            include_archived=include_archived,
            cursor=cursor,
            limit=limit,
        )
        return {"ok": True, **payload}
    except HTTPException:
        raise
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/miniqmt/operator-commands")
def execute_miniqmt_operator_command(
    payload: dict[str, Any] = Body(...),
    runtime_client: MiniQMTExecutionRuntimeClient = Depends(get_miniqmt_runtime_client),
    gateway: Any | None = Depends(get_miniqmt_gateway),
    service: SimulationRuntimeOpsService = Depends(get_simulation_runtime_ops_service),
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

    operator_payload = _operator_payload(command, payload)
    if command.command_type == "RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT" and not operator_payload.get("run_id"):
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "MINIQMT_OPERATOR_RUN_ID_REQUIRED",
                "message": "stale runtime no-side-effect recovery requires run_id",
                "context": {
                    "reason_code": "MINIQMT_OPERATOR_RUN_ID_REQUIRED",
                    "command_type": command.command_type,
                },
            },
        )
    scheduler = getattr(service.scheduler, "lifecycle_scheduler", service.scheduler)
    if command.command_type == "RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT":
        try:
            scheduler.require_no_side_effect_reconciling_run_for_operator_recovery(
                run_id=str(operator_payload["run_id"])
            )
        except TradingCoreError as exc:
            _raise_http(exc)

    result, evidence = runtime_client.execute_operator_command(
        account_group_id=command.account_group_id,
        trade_date=trade_date,
        runtime_config_hash=runtime_config_hash,
        runtime_id=str(payload.get("runtime_id") or "").strip() or None,
        command_id=command.command_id,
        command_type=command.command_type,
        reason=command.reason,
        payload=operator_payload,
        gateway=gateway,
        source="simulation_runtime_operator_command",
    )
    run_recovery = None
    if (
        command.command_type == "RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT"
        and result.status == MiniQMTOperatorCommandStatus.EXECUTED
    ):
        try:
            recovered = scheduler.recover_no_side_effect_reconciling_run_after_operator_cleanup(
                run_id=str(operator_payload["run_id"]),
                operator_result=result,
                source="simulation_runtime_operator_command",
            )
        except TradingCoreError as exc:
            _raise_http(exc)
        run_recovery = {
            "run_id": recovered.run_id,
            "status": recovered.status.value,
            "last_stage": recovered.run_payload_json.get("last_stage"),
            "recovery": recovered.run_payload_json.get("miniqmt_no_side_effect_reconciling_recovery"),
        }
    return {
        "ok": result.status == MiniQMTOperatorCommandStatus.EXECUTED,
        "operator_command": command.model_dump(mode="json"),
        "result": result.model_dump(mode="json"),
        "runtime_evidence": evidence.to_dict(),
        "run_recovery": run_recovery,
        "production_runtime_restart_required": False,
    }
