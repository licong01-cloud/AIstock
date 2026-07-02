"""Selection Center API."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.advisory_lifecycle import (
    AdvisoryLifecycleService,
    AdvisoryMarketSnapshot,
    AdvisoryWatchlistItem,
    FusionPolicy,
    InMemoryAdvisoryReviewRepository,
    PackageSelectionEvidence,
)
from backend.services.advisory_quality import generate_quality_report
from backend.services.selection_center.models import SelectionMode
from backend.services.selection_center.industry_tree import SelectionIndustryTreeService
from backend.services.selection_center.service import SelectionCenterService
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError, TradingCoreError, UnsupportedFeatureError
from backend.services.trading_core.exit_guard import ExitGuardPolicy

router = APIRouter(prefix="/selection-center", tags=["selection-center"])


class RunSelectionRequest(BaseModel):
    package_ids: list[str] = Field(min_length=1)
    trade_date: date
    data_source: str
    mode: SelectionMode = SelectionMode.SINGLE_PACKAGE
    runtime_config: dict[str, Any] = Field(default_factory=dict)


class AggregateExistingRunsRequest(BaseModel):
    source_run_ids: list[str] = Field(min_length=2)
    mode: SelectionMode
    runtime_config: dict[str, Any] = Field(default_factory=dict)


class CreatePaperPortfolioFromSelectionRequest(BaseModel):
    portfolio_name: str = Field(min_length=1)
    initial_cash: float = Field(gt=0)
    start_date: date
    data_source: MinuteDataSource
    fee_policy: dict[str, Any] | None = None
    risk_policy: dict[str, Any] | None = None
    execution_policy: dict[str, Any] | None = None


class AddSelectionRunToWatchlistRequest(BaseModel):
    category_id: int | None = Field(default=None, gt=0)
    category_name: str | None = None
    top_k: int = Field(default=20, gt=0, le=50)
    on_conflict: str = "ignore"


class DeleteSelectionRunsRequest(BaseModel):
    run_ids: list[str] = Field(min_length=1)
    confirm_delete: bool = False


class AdvisoryWatchlistItemInput(BaseModel):
    watchlist_item_id: int
    code: str
    lifecycle_status: str = "HOLDING"
    advisory_enabled: bool = True
    planned_entry_price: float | None = None
    actual_entry_price: float | None = None
    actual_entry_date: date | None = None
    high_since_entry: float | None = None
    days_since_entry: int | None = None


class AdvisoryMarketSnapshotInput(BaseModel):
    code: str
    trade_date: date
    current_price: float | None
    suspend_status: str | None = None
    high_since_entry: float | None = None
    latest_factor: float | None = None


class AdvisoryPackageEvidenceInput(BaseModel):
    package_id: str
    evidence_id: str | None = None
    code: str
    trade_date: date
    score: float | None = None
    rank: int | None = None
    candidate_count: int | None = None
    presence: str = "selected_topK"
    feature_availability_ts: datetime | None = None


class MultiPackageAdvisoryReviewPreviewRequest(BaseModel):
    items: list[AdvisoryWatchlistItemInput] = Field(min_length=1)
    package_evidence_by_code: dict[str, dict[str, AdvisoryPackageEvidenceInput]]
    market_by_code: dict[str, AdvisoryMarketSnapshotInput]
    trade_date: date
    exit_guard_policy: dict[str, Any]
    fusion_policy: dict[str, Any]


class AdvisoryQualityReportRequest(BaseModel):
    records: list[dict[str, Any]] = Field(default_factory=list)
    min_bucket_size: int = Field(default=30, ge=1)


def _raise_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, UnsupportedFeatureError):
        status_code = 422
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def get_selection_center_service() -> SelectionCenterService:
    return SelectionCenterService()


def get_industry_tree_service() -> SelectionIndustryTreeService:
    return SelectionIndustryTreeService()


@router.post("/runs")
def run_selection(
    req: RunSelectionRequest,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        run = service.run_packages(
            package_ids=req.package_ids,
            mode=req.mode,
            trade_date=req.trade_date,
            data_source=req.data_source,
            runtime_config=req.runtime_config,
        )
        return {"ok": True, "run": run.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/selectable-packages")
def list_selectable_packages(
    limit: int = 200,
    view: str = "full",
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        packages = service.list_selectable_packages(limit=limit, view=view)
        return {"ok": True, "packages": packages}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/industry-tree")
def get_selection_industry_tree(
    service: SelectionIndustryTreeService = Depends(get_industry_tree_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "tree": service.sw2_tree()}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/aggregate-runs")
def aggregate_existing_selection_runs(
    req: AggregateExistingRunsRequest,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        run = service.aggregate_existing_runs(
            source_run_ids=req.source_run_ids,
            mode=req.mode,
            runtime_config=req.runtime_config,
        )
        return {"ok": True, "run": run.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/runs")
def list_selection_runs(
    limit: int = 100,
    page: int | None = None,
    page_size: int | None = None,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        if page is not None or page_size is not None:
            page_data = service.list_runs_page(page=page or 1, page_size=page_size or min(limit, 50))
            return {
                "ok": True,
                "runs": [run.model_dump(mode="json") for run in page_data["runs"]],
                "pagination": page_data["pagination"],
            }
        runs = service.list_runs(limit=limit)
        return {"ok": True, "runs": [run.model_dump(mode="json") for run in runs]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/pit-cutoff")
def resolve_selection_pit_cutoff(
    trade_date: date,
    pit_mode: str = "PREVIOUS_TRADING_DAY_CLOSE",
    cutoff_date: date | None = None,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            "point_in_time_context": service.resolve_point_in_time_context(
                trade_date=trade_date,
                pit_mode=pit_mode,
                explicit_cutoff_date=cutoff_date,
            ),
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/runs/{run_id}")
def get_selection_run(
    run_id: str,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        run = service.get_run(run_id)
        return {"ok": True, "run": run.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/runs/{run_id}/fusion-diagnostics")
def get_selection_run_fusion_diagnostics(
    run_id: str,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        run = service.get_run(run_id)
        diagnostics = []
        for row in run.aggregate_results:
            scores = row.component_scores or {}
            diagnostics.append(
                {
                    "symbol": row.symbol,
                    "rank": row.rank,
                    "score": row.score,
                    "fusion_score": scores.get("fusion_score", row.score),
                    "source_package_ids": scores.get("source_package_ids", []),
                    "package_raw_scores": scores.get("package_raw_scores", {}),
                    "package_ranks": scores.get("package_ranks", {}),
                    "package_rank_scores": scores.get("package_rank_scores", {}),
                    "package_presence": scores.get("package_presence", {}),
                    "support_count": scores.get("support_count"),
                    "rank_dispersion": scores.get("rank_dispersion"),
                    "fusion_policy_sha256": scores.get("fusion_policy_sha256"),
                }
            )
        first_scores = (run.aggregate_results[0].component_scores or {}) if run.aggregate_results else {}
        return {
            "ok": True,
            "run_id": run.run_id,
            "mode": run.mode.value,
            "package_ids": run.package_ids,
            "fusion_method": first_scores.get("fusion_method"),
            "fusion_policy_sha256": first_scores.get("fusion_policy_sha256"),
            "diagnostics": diagnostics,
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.delete("/runs/{run_id}")
def delete_selection_run(
    run_id: str,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        deleted_counts = service.delete_run(run_id)
        return {"ok": True, "run_id": run_id, "deleted_counts": deleted_counts}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/runs/bulk-delete")
def delete_selection_runs(
    req: DeleteSelectionRunsRequest,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        if not req.confirm_delete:
            raise StrategyPackageValidationError(
                "selection run delete requires explicit confirmation",
                context={"run_ids": req.run_ids, "confirm_delete": req.confirm_delete},
            )
        result = service.delete_runs(req.run_ids)
        return {"ok": True, **result}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/runs/{run_id}/aggregate-results")
def get_selection_aggregate_results(
    run_id: str,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        run = service.get_run(run_id)
        return {"ok": True, "run_id": run.run_id, "aggregate_results": [item.model_dump(mode="json") for item in run.aggregate_results]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/runs/{run_id}/add-to-watchlist")
def add_selection_run_to_watchlist(
    run_id: str,
    req: AddSelectionRunToWatchlistRequest,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        result = service.add_run_to_watchlist(
            run_id=run_id,
            category_id=req.category_id,
            category_name=req.category_name,
            top_k=req.top_k,
            on_conflict=req.on_conflict,
        )
        return {"ok": True, "result": result}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/advisory/multi-package-review/preview")
def preview_multi_package_advisory_review(req: MultiPackageAdvisoryReviewPreviewRequest) -> dict[str, Any]:
    try:
        service = AdvisoryLifecycleService(repository=InMemoryAdvisoryReviewRepository())
        records = service.run_multi_package_daily_review(
            items=[AdvisoryWatchlistItem(**item.model_dump()) for item in req.items],
            package_evidence_by_code={
                code: {
                    package_id: PackageSelectionEvidence(**evidence.model_dump())
                    for package_id, evidence in by_package.items()
                }
                for code, by_package in req.package_evidence_by_code.items()
            },
            market_by_code={
                code: AdvisoryMarketSnapshot(**snapshot.model_dump())
                for code, snapshot in req.market_by_code.items()
            },
            trade_date=req.trade_date,
            policy=ExitGuardPolicy.from_dict(req.exit_guard_policy),
            fusion_policy=FusionPolicy(**req.fusion_policy),
        )
        return {"ok": True, "records": [asdict(record) for record in records]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/advisory/quality-report")
def build_advisory_quality_report(req: AdvisoryQualityReportRequest) -> dict[str, Any]:
    try:
        return {"ok": True, "report": generate_quality_report(req.records, min_bucket_size=req.min_bucket_size)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/runs/{run_id}/excluded-results")
def get_selection_excluded_results(
    run_id: str,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        run = service.get_run(run_id)
        return {
            "ok": True,
            "run_id": run.run_id,
            "excluded_results": {
                package_id: [item.model_dump(mode="json") for item in exclusions]
                for package_id, exclusions in run.excluded_results.items()
            },
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/runs/{run_id}/create-paper-portfolio")
def create_paper_portfolio_from_selection_run(
    run_id: str,
    req: CreatePaperPortfolioFromSelectionRequest,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        result = service.create_paper_portfolio_from_run(
            run_id=run_id,
            portfolio_name=req.portfolio_name,
            initial_cash=req.initial_cash,
            start_date=req.start_date,
            data_source=req.data_source,
            fee_policy=req.fee_policy,
            risk_policy=req.risk_policy,
            execution_policy=req.execution_policy,
        )
        return {
            "ok": True,
            "portfolio": result["portfolio"].model_dump(mode="json"),
            "link": result["link"].model_dump(mode="json"),
            "paper_runtime_config": result["paper_runtime_config"],
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/runs/{run_id}/paper-portfolio-links")
def list_selection_paper_portfolio_links(
    run_id: str,
    service: SelectionCenterService = Depends(get_selection_center_service),
) -> dict[str, Any]:
    try:
        links = service.list_paper_portfolio_links(run_id)
        return {"ok": True, "run_id": run_id, "links": [link.model_dump(mode="json") for link in links]}
    except TradingCoreError as exc:
        _raise_http(exc)
