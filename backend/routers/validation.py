from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.services.validation.finding_store import ValidationFindingStore
from backend.services.validation.history_store import ValidationHistoryStore
from backend.services.validation.models import ValidationResponse
from backend.services.validation.plan_catalog import ValidationCatalogError, ValidationPlanCatalog


router = APIRouter(prefix="/validation", tags=["validation"])


def get_history_store() -> ValidationHistoryStore:
    return ValidationHistoryStore()


def get_plan_catalog() -> ValidationPlanCatalog:
    return ValidationPlanCatalog()


def get_finding_store() -> ValidationFindingStore:
    return ValidationFindingStore()


def _success(data):
    return ValidationResponse(data=data)


@router.get("/health", response_model=ValidationResponse, summary="Validation Center read-only health")
def get_validation_health(
    history_store: ValidationHistoryStore = Depends(get_history_store),
    plan_catalog: ValidationPlanCatalog = Depends(get_plan_catalog),
    finding_store: ValidationFindingStore = Depends(get_finding_store),
):
    catalog = _load_catalog_or_500(plan_catalog)
    return _success(
        {
            "status": "ok",
            "mode": "read_only",
            "history": history_store.health(),
            "plan_catalog": {
                "catalog_path": catalog["catalog_path"],
                "missing": catalog["missing"],
                "plan_count": len(catalog["plans"]),
            },
            "quality": finding_store.health(),
            "production_8001_touched": False,
        }
    )


@router.get("/plans", response_model=ValidationResponse, summary="List validation test plans")
def list_validation_plans(
    plan_catalog: ValidationPlanCatalog = Depends(get_plan_catalog),
):
    return _success(_load_catalog_or_500(plan_catalog))


@router.get("/plans/{plan_key}", response_model=ValidationResponse, summary="Get validation test plan")
def get_validation_plan(
    plan_key: str,
    plan_catalog: ValidationPlanCatalog = Depends(get_plan_catalog),
):
    _load_catalog_or_500(plan_catalog)
    plan = plan_catalog.get_plan(plan_key)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"validation plan not found: {plan_key}")
    return _success(plan)


@router.get("/runs", response_model=ValidationResponse, summary="List validation history runs")
def list_validation_runs(
    module: str | None = Query(None),
    level: str | None = Query(None),
    status: str | None = Query(None),
    search: str | None = Query(None),
    include_markdown_only: bool = Query(True),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    history_store: ValidationHistoryStore = Depends(get_history_store),
):
    return _success(
        history_store.list_runs(
            module=module,
            level=level,
            status=status,
            search=search,
            include_markdown_only=include_markdown_only,
            page=page,
            page_size=page_size,
        )
    )


@router.get("/runs/{run_id}", response_model=ValidationResponse, summary="Get validation run detail")
def get_validation_run(
    run_id: str,
    history_store: ValidationHistoryStore = Depends(get_history_store),
):
    run = history_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"validation run not found: {run_id}")
    return _success(run)


@router.get("/coverage", response_model=ValidationResponse, summary="List validation coverage snapshots")
def list_validation_coverage(
    module: str | None = Query(None),
    status: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    history_store: ValidationHistoryStore = Depends(get_history_store),
):
    return _success(
        history_store.list_coverage_snapshots(
            module=module,
            status=status,
            page=page,
            page_size=page_size,
        )
    )


@router.get(
    "/coverage/{snapshot_id}",
    response_model=ValidationResponse,
    summary="Get validation coverage snapshot detail",
)
def get_validation_coverage(
    snapshot_id: str,
    history_store: ValidationHistoryStore = Depends(get_history_store),
):
    snapshot = history_store.get_coverage_snapshot(snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"coverage snapshot not found: {snapshot_id}")
    return _success(snapshot)


@router.get("/evidence", response_model=ValidationResponse, summary="List validation evidence manifests")
def list_validation_evidence(
    module: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    history_store: ValidationHistoryStore = Depends(get_history_store),
):
    return _success(history_store.list_evidence_manifests(module=module, page=page, page_size=page_size))


@router.get(
    "/evidence/{manifest_id}",
    response_model=ValidationResponse,
    summary="Get validation evidence manifest detail",
)
def get_validation_evidence(
    manifest_id: str,
    history_store: ValidationHistoryStore = Depends(get_history_store),
):
    manifest = history_store.get_evidence_manifest(manifest_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"evidence manifest not found: {manifest_id}")
    return _success(manifest)


@router.get("/findings", response_model=ValidationResponse, summary="List validation quality findings")
def list_validation_findings(
    source_type: str | None = Query(None),
    module: str | None = Query(None),
    severity: str | None = Query(None),
    status: str | None = Query(None),
    search: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    finding_store: ValidationFindingStore = Depends(get_finding_store),
):
    return _success(
        finding_store.list_findings(
            source_type=source_type,
            module=module,
            severity=severity,
            status=status,
            search=search,
            page=page,
            page_size=page_size,
        )
    )


@router.get("/findings/summary", response_model=ValidationResponse, summary="Quality finding summary")
def get_validation_finding_summary(
    finding_store: ValidationFindingStore = Depends(get_finding_store),
):
    return _success(finding_store.finding_summary())


@router.get("/findings/{finding_id}", response_model=ValidationResponse, summary="Get quality finding detail")
def get_validation_finding(
    finding_id: str,
    finding_store: ValidationFindingStore = Depends(get_finding_store),
):
    finding = finding_store.get_finding(finding_id)
    if finding is None:
        raise HTTPException(status_code=404, detail=f"quality finding not found: {finding_id}")
    return _success(finding)


@router.get("/bugs", response_model=ValidationResponse, summary="List validation bug registry records")
def list_validation_bugs(
    module: str | None = Query(None),
    severity: str | None = Query(None),
    status: str | None = Query(None),
    agent: str | None = Query(None),
    search: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    finding_store: ValidationFindingStore = Depends(get_finding_store),
):
    return _success(
        finding_store.list_bugs(
            module=module,
            severity=severity,
            status=status,
            agent=agent,
            search=search,
            page=page,
            page_size=page_size,
        )
    )


@router.get("/bugs/summary", response_model=ValidationResponse, summary="Validation bug registry summary")
def get_validation_bug_summary(
    finding_store: ValidationFindingStore = Depends(get_finding_store),
):
    return _success(finding_store.bug_summary())


@router.get("/bugs/{bug_id}", response_model=ValidationResponse, summary="Get validation bug detail")
def get_validation_bug(
    bug_id: str,
    finding_store: ValidationFindingStore = Depends(get_finding_store),
):
    bug = finding_store.get_bug(bug_id)
    if bug is None:
        raise HTTPException(status_code=404, detail=f"validation bug not found: {bug_id}")
    return _success(bug)


@router.get(
    "/bugs/{bug_id}/agent-context",
    response_model=ValidationResponse,
    summary="Get machine-readable bug repair context",
)
def get_validation_bug_agent_context(
    bug_id: str,
    finding_store: ValidationFindingStore = Depends(get_finding_store),
):
    context = finding_store.bug_agent_context(bug_id)
    if context is None:
        raise HTTPException(status_code=404, detail=f"validation bug not found: {bug_id}")
    return _success(context)


@router.get("/summary", response_model=ValidationResponse, summary="Validation Center read-only summary")
def get_validation_summary(
    history_store: ValidationHistoryStore = Depends(get_history_store),
    plan_catalog: ValidationPlanCatalog = Depends(get_plan_catalog),
    finding_store: ValidationFindingStore = Depends(get_finding_store),
):
    catalog = _load_catalog_or_500(plan_catalog)
    summary = history_store.summary()
    summary["plan_count"] = len(catalog["plans"])
    summary["quality"] = {
        "finding_count": finding_store.finding_summary()["finding_count"],
        "bug_count": finding_store.bug_summary()["bug_count"],
    }
    return _success(summary)


def _load_catalog_or_500(plan_catalog: ValidationPlanCatalog):
    try:
        return plan_catalog.load()
    except ValidationCatalogError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
