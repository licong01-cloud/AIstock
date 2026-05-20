from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.validation.active_discovery import ActiveDiscoveryService
from backend.services.validation.execution_runner import ValidationExecutionRunner, ValidationRunnerError
from backend.services.validation.finding_store import ValidationFindingStore
from backend.services.validation.git_activity_provider import GitActivityProviderError, GitCommitActivityProvider
from backend.services.validation.git_status_provider import GitStatusProviderError, GitWorkspaceStatusProvider
from backend.services.validation.history_store import ValidationHistoryStore
from backend.services.validation.models import ValidationResponse
from backend.services.validation.module_quality import ModuleQualityService
from backend.services.validation.plan_catalog import ValidationCatalogError, ValidationPlanCatalog
from backend.services.validation.pipeline_center import ValidationPipelineCenterService
from backend.services.validation.ui_target_catalog import (
    ValidationUiTargetCatalog,
    ValidationUiTargetCatalogError,
)


router = APIRouter(prefix="/validation", tags=["validation"])


def get_history_store() -> ValidationHistoryStore:
    return ValidationHistoryStore()


def get_plan_catalog() -> ValidationPlanCatalog:
    return ValidationPlanCatalog()


def get_finding_store() -> ValidationFindingStore:
    return ValidationFindingStore()


def get_execution_runner() -> ValidationExecutionRunner:
    return ValidationExecutionRunner()


def get_git_status_provider() -> GitWorkspaceStatusProvider:
    return GitWorkspaceStatusProvider()


def get_git_activity_provider() -> GitCommitActivityProvider:
    return GitCommitActivityProvider()


def get_module_quality_service() -> ModuleQualityService:
    return ModuleQualityService()


def get_ui_target_catalog() -> ValidationUiTargetCatalog:
    return ValidationUiTargetCatalog()


def get_pipeline_center_service(
    history_store: ValidationHistoryStore = Depends(get_history_store),
    plan_catalog: ValidationPlanCatalog = Depends(get_plan_catalog),
    finding_store: ValidationFindingStore = Depends(get_finding_store),
    execution_runner: ValidationExecutionRunner = Depends(get_execution_runner),
    git_status_provider: GitWorkspaceStatusProvider = Depends(get_git_status_provider),
    module_quality_service: ModuleQualityService = Depends(get_module_quality_service),
    ui_target_catalog: ValidationUiTargetCatalog = Depends(get_ui_target_catalog),
) -> ValidationPipelineCenterService:
    return ValidationPipelineCenterService(
        history_store=history_store,
        plan_catalog=plan_catalog,
        finding_store=finding_store,
        execution_runner=execution_runner,
        git_status_provider=git_status_provider,
        module_quality_service=module_quality_service,
        ui_target_catalog=ui_target_catalog,
    )


def get_active_discovery_service(
    history_store: ValidationHistoryStore = Depends(get_history_store),
    finding_store: ValidationFindingStore = Depends(get_finding_store),
    execution_runner: ValidationExecutionRunner = Depends(get_execution_runner),
    module_quality_service: ModuleQualityService = Depends(get_module_quality_service),
    ui_target_catalog: ValidationUiTargetCatalog = Depends(get_ui_target_catalog),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
) -> ActiveDiscoveryService:
    return ActiveDiscoveryService(
        history_store=history_store,
        finding_store=finding_store,
        execution_runner=execution_runner,
        module_quality_service=module_quality_service,
        ui_target_catalog=ui_target_catalog,
        pipeline_center=pipeline_center,
    )


class ValidationExecutionStartRequest(BaseModel):
    plan_key: str = Field(..., min_length=1)
    requested_by: str = Field("operator", min_length=1, max_length=80)
    backend_port: int | None = None
    frontend_port: int | None = None
    timeout_seconds: int | None = Field(None, gt=0)
    confirm_text: str | None = None


class ValidationDiscoveryReviewRequest(BaseModel):
    action: str = Field(..., min_length=1, max_length=80)
    reviewer: str = Field("operator", min_length=1, max_length=80)
    comment: str | None = None
    evidence_checklist: list[str] = Field(default_factory=list)


class ValidationDiscoveryPromoteRequest(BaseModel):
    confirm_promote: str = Field(..., min_length=1)
    reviewer: str | None = None
    comment: str | None = None
    evidence_checklist: list[str] = Field(default_factory=list)


class ValidationDiscoveryTaskRequest(BaseModel):
    task_id: str | None = None
    title: str | None = None
    source: str | None = None
    module: str | None = None
    risk_level: str | None = None
    detectors: list[str] = Field(default_factory=list)
    resource_policy_id: str | None = None
    requested_by: str | None = None
    reason: str | None = None
    cleanup_required: bool | None = None
    confirm_schedule: str | None = None


class ValidationDiscoveryRunTaskRequest(BaseModel):
    dry_run: bool = True
    confirm_run: str | None = None


class ValidationDiscoveryCancelTaskRequest(BaseModel):
    reason: str | None = None


class ValidationDiscoveryAgentTaskRequest(BaseModel):
    agent_runtime: str | None = None
    agent_name: str | None = None
    workspace: str | None = None
    branch: str | None = None
    llm_provider_declared: str | None = None
    llm_model_declared: str | None = None
    prompt_id: str | None = None
    prompt_version: int | None = None
    context_pack_id: str | None = None
    result_id: str | None = None
    candidate_title: str | None = None
    summary: str | None = None
    confidence: float | None = None
    requires_deterministic_verification: bool = True
    evidence_manifest_id: str | None = None
    status: str | None = None


class ValidationDiscoveryEvidenceRequest(BaseModel):
    evidence_manifest_id: str | None = None
    artifacts: list[dict[str, Any]] = Field(default_factory=list)
    logs: list[dict[str, Any]] = Field(default_factory=list)
    api_responses: list[dict[str, Any]] = Field(default_factory=list)
    mcp_responses: list[dict[str, Any]] = Field(default_factory=list)
    screenshots: list[dict[str, Any]] = Field(default_factory=list)
    reproduce_command: str | None = None


class ValidationDiscoveryAdapterRunRequest(BaseModel):
    dry_run: bool = True
    confirm_run: str | None = None
    profiles: list[str] = Field(default_factory=list)


def _success(data):
    return ValidationResponse(data=data)


@router.get("/health", response_model=ValidationResponse, summary="Validation Center read-only health")
def get_validation_health(
    history_store: ValidationHistoryStore = Depends(get_history_store),
    plan_catalog: ValidationPlanCatalog = Depends(get_plan_catalog),
    finding_store: ValidationFindingStore = Depends(get_finding_store),
    execution_runner: ValidationExecutionRunner = Depends(get_execution_runner),
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
            "runner": execution_runner.health(),
            "production_8001_touched": False,
        }
    )


@router.get("/platform/health", response_model=ValidationResponse, summary="Validation Center platform health and nightly summary")
def get_validation_platform_health(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.platform_health_summary())


@router.get("/catalog/integrity", response_model=ValidationResponse, summary="Validation catalog integrity report")
def get_validation_catalog_integrity(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.catalog_integrity_summary())


@router.get("/nightly/summary", response_model=ValidationResponse, summary="Summarize nightly validation readiness")
def get_validation_nightly_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.nightly_summary())


@router.get("/nightly/runs", response_model=ValidationResponse, summary="List recent nightly validation runs")
def list_validation_nightly_runs(
    limit: int = Query(10, ge=1, le=50),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.nightly_runs(limit=limit))


@router.get("/nightly/runner-health", response_model=ValidationResponse, summary="Summarize nightly runner health")
def get_validation_nightly_runner_health(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.nightly_runner_health())


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


@router.get(
    "/git/workspace-status",
    response_model=ValidationResponse,
    summary="Get read-only git workspace dirty-file status",
)
def get_validation_git_workspace_status(
    git_status_provider: GitWorkspaceStatusProvider = Depends(get_git_status_provider),
):
    try:
        return _success(git_status_provider.workspace_status())
    except GitStatusProviderError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/git/branch-status",
    response_model=ValidationResponse,
    summary="Get read-only git branch and upstream status",
)
def get_validation_git_branch_status(
    git_status_provider: GitWorkspaceStatusProvider = Depends(get_git_status_provider),
):
    try:
        return _success(git_status_provider.branch_status())
    except GitStatusProviderError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/git/commit-activity",
    response_model=ValidationResponse,
    summary="Get read-only recent git commits mapped to modules",
)
def get_validation_git_commit_activity(
    limit: int = Query(50, ge=1, le=200),
    git_activity_provider: GitCommitActivityProvider = Depends(get_git_activity_provider),
):
    try:
        return _success(git_activity_provider.commit_activity(limit=limit))
    except (GitActivityProviderError, GitStatusProviderError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/modules/quality-summary",
    response_model=ValidationResponse,
    summary="Get module quality, commit, coverage, and guardrail priority summary",
)
def get_validation_module_quality_summary(
    commit_limit: int = Query(50, ge=1, le=200),
    module_quality_service: ModuleQualityService = Depends(get_module_quality_service),
):
    try:
        return _success(module_quality_service.module_quality_summary(commit_limit=commit_limit))
    except (GitActivityProviderError, GitStatusProviderError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/cards/summary", response_model=ValidationResponse, summary="Get phase-1 pipeline card summary")
def get_validation_cards_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.cards_summary())


@router.get("/merge-gate/summary", response_model=ValidationResponse, summary="Get read-only merge gate summary")
def get_validation_merge_gate_summary(
    branch: str | None = Query(None),
    target: str = Query("main"),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.merge_gate_summary(branch=branch, target=target))


@router.get("/merge-gate/detail", response_model=ValidationResponse, summary="Get read-only merge gate detail")
def get_validation_merge_gate_detail(
    branch: str | None = Query(None),
    target: str = Query("main"),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.merge_gate_detail(branch=branch, target=target))


@router.get("/issues/workflow/summary", response_model=ValidationResponse, summary="Summarize issue repair workflow")
def get_validation_issue_workflow_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.issue_workflow_summary())


@router.get("/issues/workflow", response_model=ValidationResponse, summary="List issue repair workflow records")
def list_validation_issue_workflow(
    module: str | None = Query(None),
    severity: str | None = Query(None),
    workflow_state: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(
        pipeline_center.issue_workflow_items(
            module=module,
            severity=severity,
            workflow_state=workflow_state,
            page=page,
            page_size=page_size,
        )
    )


@router.get("/issues/{bug_id}/workflow", response_model=ValidationResponse, summary="Get issue repair workflow detail")
def get_validation_issue_workflow(
    bug_id: str,
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    detail = pipeline_center.issue_workflow_detail(bug_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"validation bug not found: {bug_id}")
    return _success(detail)


@router.get("/modules/detail-summary", response_model=ValidationResponse, summary="Get module quality detail summary")
def get_validation_modules_detail_summary(
    include: str | None = Query(None),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    _ = include
    return _success(pipeline_center.modules_detail_summary())


@router.get("/pipeline/tests/summary", response_model=ValidationResponse, summary="Summarize pipeline tests")
def get_validation_pipeline_tests_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.pipeline_tests_summary())


@router.get("/pipeline/tests", response_model=ValidationResponse, summary="List pipeline tests")
def list_validation_pipeline_tests(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.pipeline_tests(page=page, page_size=page_size))


@router.get("/pipeline/tests/{test_id}", response_model=ValidationResponse, summary="Get pipeline test detail")
def get_validation_pipeline_test(
    test_id: str,
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    detail = pipeline_center.pipeline_test_detail(test_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"pipeline test not found: {test_id}")
    return _success(detail)


@router.get("/features/summary", response_model=ValidationResponse, summary="Summarize feature validation")
def get_validation_features_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.features_summary())


@router.get("/features", response_model=ValidationResponse, summary="List feature validation targets")
def list_validation_features(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.features(page=page, page_size=page_size))


@router.get("/features/{route_id}", response_model=ValidationResponse, summary="Get feature validation detail")
def get_validation_feature(
    route_id: str,
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    detail = pipeline_center.feature_detail(route_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"validation feature not found: {route_id}")
    return _success(detail)


@router.get("/github/issues/summary", response_model=ValidationResponse, summary="Summarize GitHub issue sync state")
def get_validation_github_issues_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.github_issues_summary())


@router.get("/github/issues", response_model=ValidationResponse, summary="List GitHub issue sync records")
def list_validation_github_issues(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.github_issues(page=page, page_size=page_size))


@router.get("/git/branches/detail-summary", response_model=ValidationResponse, summary="Get branch and worktree detail summary")
def get_validation_git_branches_detail_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.git_branches_detail_summary())


@router.get("/github/prs/summary", response_model=ValidationResponse, summary="Summarize GitHub pull requests")
def get_validation_github_prs_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.github_prs_summary())


@router.get("/github/prs", response_model=ValidationResponse, summary="List GitHub pull requests")
def list_validation_github_prs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.github_prs(page=page, page_size=page_size))


@router.get("/legacy-debt/summary", response_model=ValidationResponse, summary="Summarize legacy debt")
def get_validation_legacy_debt_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.legacy_debt_summary())


@router.get("/legacy-debt/groups", response_model=ValidationResponse, summary="List legacy debt groups")
def list_validation_legacy_debt_groups(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.legacy_debt_groups(page=page, page_size=page_size))


@router.get("/legacy-debt/groups/{debt_group_id}", response_model=ValidationResponse, summary="Get legacy debt group detail")
def get_validation_legacy_debt_group(
    debt_group_id: str,
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    detail = pipeline_center.legacy_debt_group_detail(debt_group_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"legacy debt group not found: {debt_group_id}")
    return _success(detail)


@router.get("/automation/summary", response_model=ValidationResponse, summary="Summarize MCP and automation readiness")
def get_validation_automation_summary(
    pipeline_center: ValidationPipelineCenterService = Depends(get_pipeline_center_service),
):
    return _success(pipeline_center.automation_summary())


@router.get("/discovery/summary", response_model=ValidationResponse, summary="Summarize active bug discovery")
def get_validation_discovery_summary(
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.summary())


@router.get("/discovery/nightly-reports", response_model=ValidationResponse, summary="List active discovery nightly reports")
def list_validation_discovery_nightly_reports(
    limit: int = Query(7, ge=1, le=30),
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.list_nightly_reports(limit=limit))


@router.get("/discovery/nightly-reports/{report_id}", response_model=ValidationResponse, summary="Get active discovery nightly report")
def get_validation_discovery_nightly_report(
    report_id: str,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.get_nightly_report(report_id))


@router.get("/discovery/nightly-reports/{report_id}/llm", response_model=ValidationResponse, summary="Get active discovery LLM report")
def get_validation_discovery_nightly_llm_report(
    report_id: str,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.get_nightly_llm_report(report_id))


@router.get("/discovery/candidates", response_model=ValidationResponse, summary="List active discovery issue candidates")
def list_validation_discovery_candidates(
    module: str | None = Query(None),
    severity: str | None = Query(None),
    review_status: str | None = Query(None),
    source: str | None = Query(None),
    search: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(
        discovery.list_candidates(
            module=module,
            severity=severity,
            review_status=review_status,
            source=source,
            search=search,
            page=page,
            page_size=page_size,
        )
    )


@router.get("/discovery/candidates/{candidate_id}", response_model=ValidationResponse, summary="Get active discovery issue candidate")
def get_validation_discovery_candidate(
    candidate_id: str,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    candidate = discovery.get_candidate(candidate_id)
    if candidate is None:
        raise HTTPException(status_code=404, detail=f"active discovery candidate not found: {candidate_id}")
    return _success(candidate)


@router.post("/discovery/candidates/{candidate_id}/review", response_model=ValidationResponse, summary="Review active discovery candidate")
def review_validation_discovery_candidate(
    candidate_id: str,
    request: ValidationDiscoveryReviewRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    try:
        return _success(discovery.review_candidate(candidate_id, request.model_dump()))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"active discovery candidate not found: {candidate_id}") from exc


@router.post("/discovery/candidates/{candidate_id}/promote", response_model=ValidationResponse, summary="Request active discovery candidate promotion")
def promote_validation_discovery_candidate(
    candidate_id: str,
    request: ValidationDiscoveryPromoteRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    try:
        return _success(discovery.promote_candidate(candidate_id, request.model_dump()))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"active discovery candidate not found: {candidate_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/discovery/tasks", response_model=ValidationResponse, summary="List active discovery tasks")
def list_validation_discovery_tasks(
    source: str | None = Query(None),
    status: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.list_tasks(source=source, status=status, page=page, page_size=page_size))


@router.post("/discovery/tasks", response_model=ValidationResponse, summary="Schedule active discovery task")
def schedule_validation_discovery_task(
    request: ValidationDiscoveryTaskRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    try:
        return _success(discovery.schedule_task(request.model_dump(exclude_none=True)))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/discovery/tasks/{task_id}/run", response_model=ValidationResponse, summary="Run active discovery task")
def run_validation_discovery_task(
    task_id: str,
    request: ValidationDiscoveryRunTaskRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    try:
        return _success(discovery.run_task(task_id, request.model_dump()))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"active discovery task not found: {task_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/discovery/tasks/{task_id}/cancel", response_model=ValidationResponse, summary="Cancel active discovery task")
def cancel_validation_discovery_task(
    task_id: str,
    request: ValidationDiscoveryCancelTaskRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    try:
        return _success(discovery.cancel_task(task_id, request.model_dump(exclude_none=True)))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"active discovery task not found: {task_id}") from exc


@router.post("/discovery/agent-tasks/{task_id}/claim", response_model=ValidationResponse, summary="Claim active discovery agent task")
def claim_validation_discovery_agent_task(
    task_id: str,
    request: ValidationDiscoveryAgentTaskRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.claim_agent_task(task_id, request.model_dump(exclude_none=True)))


@router.get("/discovery/agent-tasks/{task_id}/context-pack", response_model=ValidationResponse, summary="Get active discovery agent context pack")
def get_validation_discovery_agent_context_pack(
    task_id: str,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.get_agent_context_pack(task_id))


@router.post("/discovery/agent-tasks/{task_id}/results", response_model=ValidationResponse, summary="Submit active discovery agent result")
def submit_validation_discovery_agent_result(
    task_id: str,
    request: ValidationDiscoveryAgentTaskRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.submit_agent_result(task_id, request.model_dump(exclude_none=True)))


@router.post("/discovery/agent-tasks/{task_id}/evidence", response_model=ValidationResponse, summary="Attach active discovery agent evidence")
def attach_validation_discovery_agent_evidence(
    task_id: str,
    request: ValidationDiscoveryEvidenceRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.attach_agent_evidence(task_id, request.model_dump(exclude_none=True)))


@router.post("/discovery/agent-tasks/{task_id}/complete", response_model=ValidationResponse, summary="Complete active discovery agent task")
def complete_validation_discovery_agent_task(
    task_id: str,
    request: ValidationDiscoveryAgentTaskRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    try:
        return _success(discovery.complete_agent_task(task_id, request.model_dump(exclude_none=True)))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"active discovery task not found: {task_id}") from exc


@router.get("/discovery/llm-profiles", response_model=ValidationResponse, summary="List active discovery LLM profiles")
def list_validation_discovery_llm_profiles(
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.list_llm_profiles())


@router.get("/discovery/tool-adapters", response_model=ValidationResponse, summary="List active discovery tool adapters")
def list_validation_discovery_tool_adapters(
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.list_tool_adapters())


@router.post("/discovery/tool-adapters/{adapter_id}/dry-run", response_model=ValidationResponse, summary="Dry-run active discovery tool adapter")
def run_validation_discovery_tool_adapter(
    adapter_id: str,
    request: ValidationDiscoveryAdapterRunRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    try:
        return _success(discovery.run_tool_adapter(adapter_id, request.model_dump()))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"active discovery adapter not found: {adapter_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/discovery/llm-evals", response_model=ValidationResponse, summary="Get active discovery LLM eval summary")
def get_validation_discovery_llm_evals(
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.run_llm_eval({"dry_run": True}))


@router.post("/discovery/llm-evals/run", response_model=ValidationResponse, summary="Run active discovery LLM eval dry-run")
def run_validation_discovery_llm_eval(
    request: ValidationDiscoveryAdapterRunRequest,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    return _success(discovery.run_llm_eval(request.model_dump()))


@router.get("/discovery/traces/{trace_id}", response_model=ValidationResponse, summary="Get active discovery trace/evidence")
def get_validation_discovery_trace(
    trace_id: str,
    discovery: ActiveDiscoveryService = Depends(get_active_discovery_service),
):
    trace = discovery.get_trace(trace_id)
    if trace is None:
        raise HTTPException(status_code=404, detail=f"active discovery trace not found: {trace_id}")
    return _success(trace)


@router.get("/ui-targets", response_model=ValidationResponse, summary="List route-level validation UI targets")
def list_validation_ui_targets(
    nav_group: str | None = Query(None),
    module: str | None = Query(None),
    coverage_status: str | None = Query(None),
    risk_level: str | None = Query(None),
    search: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    ui_target_catalog: ValidationUiTargetCatalog = Depends(get_ui_target_catalog),
):
    try:
        return _success(
            ui_target_catalog.list_targets(
                nav_group=nav_group,
                module=module,
                coverage_status=coverage_status,
                risk_level=risk_level,
                search=search,
                page=page,
                page_size=page_size,
            )
        )
    except ValidationUiTargetCatalogError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/ui-targets/summary", response_model=ValidationResponse, summary="Summarize route-level UI validation targets")
def get_validation_ui_targets_summary(
    ui_target_catalog: ValidationUiTargetCatalog = Depends(get_ui_target_catalog),
):
    try:
        return _success(ui_target_catalog.summary())
    except ValidationUiTargetCatalogError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/ui-targets/{route_id}", response_model=ValidationResponse, summary="Get route-level UI validation target detail")
def get_validation_ui_target(
    route_id: str,
    ui_target_catalog: ValidationUiTargetCatalog = Depends(get_ui_target_catalog),
):
    try:
        target = ui_target_catalog.get_target(route_id)
    except ValidationUiTargetCatalogError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if target is None:
        raise HTTPException(status_code=404, detail=f"validation UI target not found: {route_id}")
    return _success(target)


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


@router.get("/executions", response_model=ValidationResponse, summary="List controlled validation executions")
def list_validation_executions(
    status: str | None = Query(None),
    plan_key: str | None = Query(None),
    module: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    execution_runner: ValidationExecutionRunner = Depends(get_execution_runner),
):
    return _success(
        execution_runner.list_jobs(
            status=status,
            plan_key=plan_key,
            module=module,
            page=page,
            page_size=page_size,
        )
    )


@router.get("/executions/{job_id}", response_model=ValidationResponse, summary="Get controlled validation execution")
def get_validation_execution(
    job_id: str,
    execution_runner: ValidationExecutionRunner = Depends(get_execution_runner),
):
    job = execution_runner.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"validation execution not found: {job_id}")
    return _success(job)


@router.get("/executions/{job_id}/log", response_model=ValidationResponse, summary="Get controlled validation execution log")
def get_validation_execution_log(
    job_id: str,
    tail_lines: int = Query(300, ge=1, le=2000),
    execution_runner: ValidationExecutionRunner = Depends(get_execution_runner),
):
    payload = execution_runner.get_job_log(job_id, tail_lines=tail_lines)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"validation execution not found: {job_id}")
    return _success(payload)


@router.get(
    "/executions/{job_id}/evidence",
    response_model=ValidationResponse,
    summary="Get controlled validation execution evidence",
)
def get_validation_execution_evidence(
    job_id: str,
    execution_runner: ValidationExecutionRunner = Depends(get_execution_runner),
):
    payload = execution_runner.get_job_evidence(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"validation execution not found: {job_id}")
    return _success(payload)


@router.post("/executions", response_model=ValidationResponse, summary="Start allowlisted validation execution")
def start_validation_execution(
    request: ValidationExecutionStartRequest,
    execution_runner: ValidationExecutionRunner = Depends(get_execution_runner),
):
    try:
        return _success(
            execution_runner.start_job(
                plan_key=request.plan_key,
                requested_by=request.requested_by,
                backend_port=request.backend_port,
                frontend_port=request.frontend_port,
                timeout_seconds=request.timeout_seconds,
                confirm_text=request.confirm_text,
            )
        )
    except ValidationRunnerError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/summary", response_model=ValidationResponse, summary="Validation Center read-only summary")
def get_validation_summary(
    history_store: ValidationHistoryStore = Depends(get_history_store),
    plan_catalog: ValidationPlanCatalog = Depends(get_plan_catalog),
    finding_store: ValidationFindingStore = Depends(get_finding_store),
    execution_runner: ValidationExecutionRunner = Depends(get_execution_runner),
):
    catalog = _load_catalog_or_500(plan_catalog)
    summary = history_store.summary()
    summary["plan_count"] = len(catalog["plans"])
    summary["quality"] = {
        "finding_count": finding_store.finding_summary()["finding_count"],
        "bug_count": finding_store.bug_summary()["bug_count"],
    }
    summary["runner"] = execution_runner.health()
    return _success(summary)


def _load_catalog_or_500(plan_catalog: ValidationPlanCatalog):
    try:
        return plan_catalog.load()
    except ValidationCatalogError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
