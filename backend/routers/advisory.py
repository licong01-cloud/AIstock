"""Independent Advisory Center API."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.advisory_program import (
    AdvisoryProgramService,
    PRICE_BASIS_NEXT_OPEN,
    program_to_dict,
    review_result_to_dict,
)
from backend.services.trading_core.errors import DataUnavailableError, TradingCoreError, UnsupportedFeatureError

router = APIRouter(prefix="/advisory", tags=["advisory"])


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


class AdvisoryStatusRequest(BaseModel):
    status: str


class AdvisoryCloneRequest(BaseModel):
    program_name: str | None = None
    created_by: str | None = None


class AdvisoryReviewRequest(BaseModel):
    trade_date: date
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
    runtime_config_json: dict[str, Any] = Field(default_factory=dict)


class AdvisoryBindingApplyRequest(BaseModel):
    binding: AdvisoryBindingPayload
    activation_reason: str = Field(min_length=1)
    source_replay_run_id: str | None = None
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


def _raise_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, UnsupportedFeatureError):
        status_code = 422
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


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
        return {"ok": True, "program": program_to_dict(program)}
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
        return {"ok": True, "program": program_to_dict(service.update_program(program_id, updates))}
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
        return {"ok": True, "program": program_to_dict(service.clone_program(program_id, program_name=req.program_name, created_by=req.created_by))}
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
