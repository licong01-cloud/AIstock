"""Independent Advisory Center API."""

from __future__ import annotations

from datetime import date, datetime
import logging
from typing import Any, Callable
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field

from backend.services.advisory_phase0a.historical_research import (
    HistoricalAdvisoryResearchRunner,
    HistoricalResearchBatchRequest,
    historical_research_batch_to_dict,
    historical_research_program_run_to_dict,
    historical_research_receipt_to_dict,
)
from backend.services.advisory_phase0a.historical_research_postgres import (
    PersistedHistoricalSelectionEvidenceAdapter,
    PostgresHistoricalResearchProgramResolver,
    PostgresHistoricalResearchRepository,
    PostgresHistoricalResearchTradingDateResolver,
)
from backend.services.advisory_program import (
    AdvisoryProgramService,
    PRICE_BASIS_NEXT_OPEN,
    program_to_dict,
    review_result_to_dict,
)
from backend.services.trading_core.errors import DataUnavailableError, TradingCoreError, UnsupportedFeatureError
from backend.services.advisory_historical_range.api_models import (
    HistoricalRangeBuildBridgeRequest,
    HistoricalRangeCommandRequest,
    HistoricalRangeCreateRequest,
    HistoricalRangeRefreshOutcomesRequest,
)
from backend.services.advisory_historical_range.composition import (
    build_environment_historical_range_r5_application_service,
)
from backend.services.advisory_historical_range.models import HistoricalRangeContractError
from backend.services.advisory_historical_range.query_repository import (
    HistoricalRangeNotFoundError,
    HistoricalRangeQueryError,
)
from backend.services.advisory_historical_range.service import (
    HistoricalRangeApplicationService,
    HistoricalRangeServiceError,
)

router = APIRouter(prefix="/advisory", tags=["advisory"])
LOGGER = logging.getLogger(__name__)


class AdvisoryProgramCreateRequest(BaseModel):
    program_name: str = Field(min_length=1)
    package_mode: str
    package_ids: list[str] = Field(min_length=1)
    target_count: int = Field(default=20, gt=0, le=100)
    package_weights: dict[str, float] | None = None
    review_policy: dict[str, Any] = Field(default_factory=dict)
    entry_price_basis: str = PRICE_BASIS_NEXT_OPEN
    exit_price_basis: str = PRICE_BASIS_NEXT_OPEN
    review_schedule: dict[str, Any] = Field(default_factory=lambda: {"frequency": "daily_after_close"})
    created_by: str | None = None
    status: str = "DRAFT"


class AdvisoryProgramUpdateRequest(BaseModel):
    program_name: str | None = None
    package_mode: str | None = None
    package_ids: list[str] | None = None
    target_count: int | None = Field(default=None, gt=0, le=100)
    package_weights: dict[str, float] | None = None
    review_policy: dict[str, Any] | None = None
    entry_price_basis: str | None = None
    exit_price_basis: str | None = None
    review_schedule: dict[str, Any] | None = None
    status: str | None = None
    expected_program_version: int | None = Field(default=None, ge=1)
    expected_binding_version_id: str | None = Field(default=None, min_length=1)
    effective_from_trade_date: date | None = None


class AdvisoryStatusRequest(BaseModel):
    status: str


class AdvisoryCloneRequest(BaseModel):
    program_name: str | None = None
    created_by: str | None = None


class AdvisoryReviewRequest(BaseModel):
    trade_date: date
    target_trade_date: date | None = None
    selection_as_of_trade_date: date | None = None
    selection_run_id: str | None = None
    data_source: str = "DB_HISTORICAL"
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    candidates: list[dict[str, Any]] | None = None
    market_by_symbol: dict[str, dict[str, Any]] = Field(default_factory=dict)


class AdvisoryBindingPayload(BaseModel):
    package_mode: str
    package_ids: list[str] = Field(min_length=1)
    package_weights: dict[str, float] | None = None
    target_count: int | None = Field(default=None, gt=0, le=100)
    runtime_config_json: dict[str, Any] | None = None


class AdvisoryBindingApplyRequest(BaseModel):
    binding: AdvisoryBindingPayload
    activation_reason: str = Field(min_length=1)
    expected_program_version: int = Field(ge=1)
    expected_binding_version_id: str = Field(min_length=1)
    source_replay_run_id: str | None = None
    effective_from_trade_date: date | None = None
    created_by: str | None = None


class AdvisoryLegacyBindingRepairRequest(BaseModel):
    binding: AdvisoryBindingPayload
    repair_reason: str = Field(min_length=1)
    expected_program_version: int = Field(ge=1)
    expected_binding_version_id: str = Field(min_length=1)
    effective_from_trade_date: date | None = None
    created_by: str | None = None


class AdvisoryReplayRequest(BaseModel):
    start_date: date
    end_date: date
    candidates_by_date: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    market_by_date: dict[str, dict[str, dict[str, Any]]] = Field(default_factory=dict)
    data_source: str = "DB_HISTORICAL"
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    entry_price_basis: str | None = None
    exit_price_basis: str | None = None
    draft_binding: AdvisoryBindingPayload | None = None
    compare_to_binding_version_id: str | None = None
    include_daily_items: bool = True


class AdvisoryQualityReportRequest(BaseModel):
    records: list[dict[str, Any]] = Field(default_factory=list)
    min_bucket_size: int = Field(default=30, ge=1)


def get_advisory_program_service() -> AdvisoryProgramService:
    return AdvisoryProgramService()


def get_historical_research_runner() -> HistoricalAdvisoryResearchRunner:
    return HistoricalAdvisoryResearchRunner(
        repository=PostgresHistoricalResearchRepository(),
        trading_date_resolver=PostgresHistoricalResearchTradingDateResolver(),
        program_resolver=PostgresHistoricalResearchProgramResolver(),
        evidence_adapter=PersistedHistoricalSelectionEvidenceAdapter(),
    )


def get_historical_range_application_service() -> HistoricalRangeApplicationService:
    try:
        return build_environment_historical_range_r5_application_service()
    except Exception as exc:
        _raise_historical_range_http(exc)


def _raise_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, UnsupportedFeatureError):
        status_code = 422
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def _raise_historical_research_http(exc: TradingCoreError) -> None:
    status_code = 409 if exc.to_dict().get("reason_code") == "ADVISORY_PHASE0A2D_RESEARCH_RUN_CONFLICT" else 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def _raise_historical_range_http(exc: Exception) -> None:
    correlation_id = f"ahr-corr-{uuid4().hex}"
    status_code = 500
    reason_code = "ADVISORY_HR_INTERNAL_ERROR"
    retryable = False
    context: dict[str, Any] = {}
    if isinstance(exc, HistoricalRangeServiceError):
        status_code = exc.http_status
        reason_code = exc.reason_code
        retryable = exc.retryable
        context = exc.context
    elif isinstance(exc, HistoricalRangeNotFoundError):
        status_code = 404
        reason_code = exc.reason_code
        context = exc.context
    elif isinstance(exc, HistoricalRangeQueryError):
        status_code = 422
        reason_code = exc.reason_code
        context = exc.context
    elif isinstance(exc, HistoricalRangeContractError):
        reason_code = exc.reason_code
        context = exc.context
        status_code = 409 if "CONFLICT" in reason_code or "MISMATCH" in reason_code else 422
        retryable = status_code == 409
    elif isinstance(exc, TradingCoreError):
        payload = exc.to_dict()
        reason_code = str(payload.get("reason_code") or "ADVISORY_HR_DOMAIN_ERROR")
        context = dict(payload.get("context") or {})
        status_code = 409 if "CONFLICT" in reason_code or "MISMATCH" in reason_code else 422
    elif isinstance(exc, ValueError):
        status_code = 422
        reason_code = "ADVISORY_HR_REQUEST_INVALID"
    unexpected = status_code == 500
    if unexpected:
        LOGGER.exception(
            "unexpected historical-range HTTP failure correlation_id=%s error_type=%s",
            correlation_id,
            type(exc).__name__,
            exc_info=exc,
        )
    detail = {
        "error_code": "ADVISORY_HISTORICAL_RANGE_ERROR",
        "reason_code": reason_code,
        "message": "Unexpected historical-range service error" if unexpected else str(exc),
        "retryable": retryable,
        "context": {} if unexpected else context,
        "correlation_id": correlation_id,
    }
    raise HTTPException(status_code=status_code, detail=detail) from exc


def _historical_range_call(call: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return call()
    except HTTPException:
        raise
    except Exception as exc:
        _raise_historical_range_http(exc)


def _page_envelope(resource: str, result: dict[str, Any]) -> dict[str, Any]:
    return {"ok": True, "data": {resource: result["items"]}, "page": result["page"]}


def _set_mutation_status(response: Response, payload: dict[str, Any]) -> None:
    operation = payload["data"]["operation"]
    response.status_code = 200 if operation.get("status") in {"COMPLETED", "FAILED"} else 202


@router.post("/research-batches")
def create_historical_research_batch(
    req: HistoricalResearchBatchRequest,
    runner: HistoricalAdvisoryResearchRunner = Depends(get_historical_research_runner),
) -> dict[str, Any]:
    try:
        receipt = runner.run(req)
        batch = runner.get_batch(receipt.batch_id)
        return {
            "ok": True,
            "batch": historical_research_batch_to_dict(batch),
            "receipt": historical_research_receipt_to_dict(receipt),
        }
    except TradingCoreError as exc:
        _raise_historical_research_http(exc)


@router.get("/research-batches/{batch_id}")
def get_historical_research_batch(
    batch_id: str,
    runner: HistoricalAdvisoryResearchRunner = Depends(get_historical_research_runner),
) -> dict[str, Any]:
    try:
        batch = runner.get_batch(batch_id)
        receipt = runner.get_batch_receipt(batch_id)
        return {
            "ok": True,
            "batch": historical_research_batch_to_dict(batch),
            "receipt": historical_research_receipt_to_dict(receipt) if receipt is not None else None,
        }
    except TradingCoreError as exc:
        _raise_historical_research_http(exc)


@router.get("/research-batches/{batch_id}/programs/{program_id}")
def get_historical_research_program(
    batch_id: str,
    program_id: str,
    runner: HistoricalAdvisoryResearchRunner = Depends(get_historical_research_runner),
) -> dict[str, Any]:
    try:
        batch = runner.get_batch(batch_id)
        if program_id not in batch.program_ids:
            raise DataUnavailableError(
                "historical research Program is not part of this batch",
                context={"batch_id": batch_id, "program_id": program_id},
            )
        program_run = runner.get_program_run(program_id=program_id, decision_trade_date=batch.decision_trade_date)
        if program_run is None:
            raise DataUnavailableError(
                "historical research Program run does not exist",
                context={"batch_id": batch_id, "program_id": program_id},
            )
        return {
            "ok": True,
            "batch_id": batch_id,
            "program_run": historical_research_program_run_to_dict(program_run),
        }
    except TradingCoreError as exc:
        _raise_historical_research_http(exc)


@router.get("/historical-range-options")
def historical_range_options(
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    return _historical_range_call(service.list_batch_options)


@router.get("/historical-range-batches")
def list_historical_range_batches(
    status: list[str] = Query(default=[]),
    program_id: str | None = None,
    created_before: datetime | None = None,
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.list_batches(
            statuses=status,
            program_id=program_id,
            created_before=created_before,
            cursor=cursor,
            limit=limit,
        )
    )
    return _page_envelope("batches", result)


@router.post("/historical-range-batches", status_code=202)
def create_historical_range_batch(
    req: HistoricalRangeCreateRequest,
    background_tasks: BackgroundTasks,
    response: Response,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.create_batch(
            req,
            idempotency_key=idempotency_key,
            background_tasks=background_tasks,
        )
    )
    _set_mutation_status(response, result)
    return result


@router.get("/historical-range-batches/{batch_id}")
def get_historical_range_batch(
    batch_id: str,
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    batch = _historical_range_call(lambda: service.get_batch(batch_id))
    return {"ok": True, "data": {"batch": batch}}


@router.get("/historical-range-batches/{batch_id}/runs")
def list_historical_range_runs(
    batch_id: str,
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(lambda: service.list_runs(batch_id, cursor=cursor, limit=limit))
    return _page_envelope("runs", result)


@router.get("/historical-range-batches/{batch_id}/operations")
def list_historical_range_operations(
    batch_id: str,
    operation_type: list[str] = Query(default=[]),
    status: list[str] = Query(default=[]),
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.list_operations(
            batch_id,
            operation_types=operation_type,
            statuses=status,
            cursor=cursor,
            limit=limit,
        )
    )
    return _page_envelope("operations", result)


@router.post("/historical-range-batches/{batch_id}/resume", status_code=202)
def resume_historical_range_batch(
    batch_id: str,
    req: HistoricalRangeCommandRequest,
    background_tasks: BackgroundTasks,
    response: Response,
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.resume_batch(batch_id, req, background_tasks=background_tasks)
    )
    _set_mutation_status(response, result)
    return result


@router.post("/historical-range-batches/{batch_id}/cancel", status_code=202)
def cancel_historical_range_batch(
    batch_id: str,
    req: HistoricalRangeCommandRequest,
    background_tasks: BackgroundTasks,
    response: Response,
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.cancel_batch(batch_id, req, background_tasks=background_tasks)
    )
    _set_mutation_status(response, result)
    return result


@router.post("/historical-range-batches/{batch_id}/refresh-outcomes", status_code=202)
def refresh_historical_range_outcomes(
    batch_id: str,
    req: HistoricalRangeRefreshOutcomesRequest,
    background_tasks: BackgroundTasks,
    response: Response,
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.refresh_outcomes(batch_id, req, background_tasks=background_tasks)
    )
    _set_mutation_status(response, result)
    return result


@router.post("/historical-range-batches/{batch_id}/build-dataset-bridge", status_code=202)
def build_historical_range_dataset_bridge(
    batch_id: str,
    req: HistoricalRangeBuildBridgeRequest,
    background_tasks: BackgroundTasks,
    response: Response,
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.build_dataset_bridge(batch_id, req, background_tasks=background_tasks)
    )
    _set_mutation_status(response, result)
    return result


@router.get("/historical-range-operations/{operation_id}")
def get_historical_range_operation(
    operation_id: str,
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    operation = _historical_range_call(lambda: service.get_operation(operation_id))
    return {"ok": True, "data": {"operation": operation}}


@router.get("/historical-range-runs/{range_run_id}")
def get_historical_range_run(
    range_run_id: str,
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    run = _historical_range_call(lambda: service.get_run(range_run_id))
    return {"ok": True, "data": {"run": run}}


@router.get("/historical-range-runs/{range_run_id}/days")
def list_historical_range_days(
    range_run_id: str,
    status: list[str] = Query(default=[]),
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.list_days(range_run_id, statuses=status, cursor=cursor, limit=limit)
    )
    return _page_envelope("days", result)


@router.get("/historical-range-runs/{range_run_id}/days/{trade_date}")
def get_historical_range_day(
    range_run_id: str,
    trade_date: date,
    candidate_cursor: str | None = None,
    candidate_limit: int = Query(default=50, ge=1, le=500),
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.get_day(
            range_run_id,
            trade_date=trade_date,
            candidate_cursor=candidate_cursor,
            candidate_limit=candidate_limit,
        )
    )
    return {"ok": True, "data": {"day": result["day"], "candidates": result["candidates"]}, "page": result["candidate_page"]}


@router.get("/historical-range-runs/{range_run_id}/lists/{trade_date}")
def get_historical_range_list(
    range_run_id: str,
    trade_date: date,
    item_cursor: str | None = None,
    item_limit: int = Query(default=50, ge=1, le=500),
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.get_list(
            range_run_id,
            trade_date=trade_date,
            item_cursor=item_cursor,
            item_limit=item_limit,
        )
    )
    return {"ok": True, "data": {"list": result["list"], "items": result["items"]}, "page": result["item_page"]}


@router.get("/historical-range-runs/{range_run_id}/outcomes")
def list_historical_range_outcomes(
    range_run_id: str,
    subject_type: str | None = None,
    projection: str | None = None,
    maturity_status: str | None = None,
    horizon: int | None = Query(default=None, ge=1),
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.list_outcomes(
            range_run_id,
            subject_type=subject_type,
            projection=projection,
            maturity_status=maturity_status,
            horizon=horizon,
            cursor=cursor,
            limit=limit,
        )
    )
    return _page_envelope("outcomes", result)


@router.get("/historical-range-runs/{range_run_id}/summaries")
def list_historical_range_summaries(
    range_run_id: str,
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    service: HistoricalRangeApplicationService = Depends(get_historical_range_application_service),
) -> dict[str, Any]:
    result = _historical_range_call(
        lambda: service.list_summaries(range_run_id, cursor=cursor, limit=limit)
    )
    return _page_envelope("summaries", result)


@router.get("/programs")
def list_programs(
    include_archived: bool = False,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "programs": [program_to_dict(row) for row in service.list_programs(include_archived=include_archived)]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs")
def create_program(
    req: AdvisoryProgramCreateRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        program = service.create_program(**req.model_dump())
        return {"ok": True, "program": program_to_dict(program), "binding": service.active_binding(program.program_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/programs/{program_id}")
def get_program(
    program_id: str,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "program": program_to_dict(service.get_program(program_id))}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.patch("/programs/{program_id}")
def update_program(
    program_id: str,
    req: AdvisoryProgramUpdateRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        updates = {key: value for key, value in req.model_dump().items() if value is not None}
        program = service.update_program(program_id, updates)
        return {"ok": True, "program": program_to_dict(program), "binding": service.active_binding(program.program_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/programs/{program_id}/bindings")
def list_bindings(
    program_id: str,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "bindings": service.binding_versions(program_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/programs/{program_id}/bindings/active")
def active_binding(
    program_id: str,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "binding": service.active_binding(program_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/programs/{program_id}/bindings/defaults")
def binding_defaults(
    program_id: str,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, **service.binding_defaults(program_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/bindings/apply")
def apply_binding(
    program_id: str,
    req: AdvisoryBindingApplyRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        result = service.apply_binding(
            program_id,
            binding=req.binding.model_dump(),
            activation_reason=req.activation_reason,
            source_replay_run_id=req.source_replay_run_id,
            effective_from_trade_date=req.effective_from_trade_date,
            created_by=req.created_by,
            expected_program_version=req.expected_program_version,
            expected_binding_version_id=req.expected_binding_version_id,
        )
        return {"ok": True, **result}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/bindings/repair-legacy")
def repair_legacy_binding(
    program_id: str,
    req: AdvisoryLegacyBindingRepairRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        result = service.repair_legacy_binding(
            program_id,
            binding=req.binding.model_dump(),
            repair_reason=req.repair_reason,
            expected_program_version=req.expected_program_version,
            expected_binding_version_id=req.expected_binding_version_id,
            effective_from_trade_date=req.effective_from_trade_date,
            created_by=req.created_by,
        )
        return {"ok": True, **result}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/status")
def set_program_status(
    program_id: str,
    req: AdvisoryStatusRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "program": program_to_dict(service.change_status(program_id, req.status))}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/enable")
def enable_program(program_id: str, service: AdvisoryProgramService = Depends(get_advisory_program_service)) -> dict[str, Any]:
    try:
        return {"ok": True, "program": program_to_dict(service.change_status(program_id, "ENABLED"))}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/pause")
def pause_program(program_id: str, service: AdvisoryProgramService = Depends(get_advisory_program_service)) -> dict[str, Any]:
    try:
        return {"ok": True, "program": program_to_dict(service.change_status(program_id, "PAUSED"))}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/archive")
def archive_program(program_id: str, service: AdvisoryProgramService = Depends(get_advisory_program_service)) -> dict[str, Any]:
    try:
        return {"ok": True, "program": program_to_dict(service.change_status(program_id, "ARCHIVED"))}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/clone")
def clone_program(
    program_id: str,
    req: AdvisoryCloneRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        program = service.clone_program(program_id, program_name=req.program_name, created_by=req.created_by)
        return {"ok": True, "program": program_to_dict(program), "binding": service.active_binding(program.program_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/leaderboard")
def leaderboard(
    sort_by: str = Query(default="win_rate"),
    include_archived: bool = False,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "leaderboard": service.leaderboard(sort_by=sort_by, include_archived=include_archived)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/programs/{program_id}/active-pool")
def active_pool(program_id: str, service: AdvisoryProgramService = Depends(get_advisory_program_service)) -> dict[str, Any]:
    try:
        return {"ok": True, "active_pool": service.active_pool(program_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/programs/{program_id}/reviews")
def reviews(
    program_id: str,
    limit: int = Query(default=100, gt=0, le=500),
    offset: int = Query(default=0, ge=0),
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, **service.review_history_page(program_id, limit=limit, offset=offset)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/programs/{program_id}/list-versions")
def list_versions(
    program_id: str,
    limit: int = Query(default=100, gt=0, le=500),
    offset: int = Query(default=0, ge=0),
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "list_versions": service.recommendation_list_versions(program_id, limit=limit, offset=offset)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/list-versions/{list_version_id}")
def list_version_detail(
    list_version_id: str,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, **service.recommendation_list_version_detail(list_version_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/programs/{program_id}/returns")
def returns(program_id: str, service: AdvisoryProgramService = Depends(get_advisory_program_service)) -> dict[str, Any]:
    try:
        return {"ok": True, "returns": service.return_history(program_id), "metrics": service.program_metrics(program_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/reviews/preview")
def preview_review(
    program_id: str,
    req: AdvisoryReviewRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        result = service.run_review_from_selection(program_id, preview=True, **req.model_dump())
        return {"ok": True, "review": review_result_to_dict(result)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/reviews/run")
def run_review(
    program_id: str,
    req: AdvisoryReviewRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        result = service.run_review_from_selection(program_id, preview=False, **req.model_dump())
        return {"ok": True, "review": review_result_to_dict(result)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/programs/{program_id}/replay")
def run_replay(
    program_id: str,
    req: AdvisoryReplayRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            "replay": service.run_replay(
                program_id,
                start_date=req.start_date,
                end_date=req.end_date,
                candidates_by_date=req.candidates_by_date,
                market_by_date=req.market_by_date,
                data_source=req.data_source,
                runtime_config=req.runtime_config,
                entry_price_basis=req.entry_price_basis,
                exit_price_basis=req.exit_price_basis,
                draft_binding=req.draft_binding.model_dump() if req.draft_binding else None,
                compare_to_binding_version_id=req.compare_to_binding_version_id,
                include_daily_items=req.include_daily_items,
            ),
            "deprecated": True,
            "replacement": "/api/v1/advisory/historical-range-batches",
            "sunset_policy": "compatibility_read_and_api_retained_no_phase1r_fallback",
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/quality-report")
def quality_report(
    req: AdvisoryQualityReportRequest,
    service: AdvisoryProgramService = Depends(get_advisory_program_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "report": service.quality_report(req.records, min_bucket_size=req.min_bucket_size)}
    except TradingCoreError as exc:
        _raise_http(exc)
