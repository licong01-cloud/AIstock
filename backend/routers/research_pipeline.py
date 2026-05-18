"""Research Pipeline APIs shared by UI and MCP."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from backend.services.research_pipeline import (
    RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM,
    RESEARCH_PROMOTE_CONFIRM,
    RESEARCH_RETRY_STAGE_CONFIRM,
    RESEARCH_RUN_STAGE_CONFIRM,
    ResearchPipelineService,
)
from backend.services.research_pipeline.service import (
    ResearchPipelineConflictError,
    ResearchPipelineError,
    ResearchPipelineNotFoundError,
)

router = APIRouter(prefix="/research-pipeline", tags=["research-pipeline"])


class ResearchPipelineResponse(BaseModel):
    status: str = "success"
    data: Any


class ResearchExperimentCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_type: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    description: str | None = None
    criteria_json: dict[str, Any] = Field(default_factory=dict)
    baseline_ref_json: dict[str, Any] = Field(default_factory=dict)
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_by: str = "codex"
    stages: list[dict[str, Any] | str] | None = None

    @model_validator(mode="before")
    @classmethod
    def _accept_mcp_name_alias(cls, value: Any) -> Any:
        if isinstance(value, dict) and "title" not in value and "name" in value:
            value = dict(value)
            value["title"] = value.pop("name")
        return value


class StageActionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    payload: dict[str, Any] = Field(default_factory=dict)
    confirm: str = ""

    def action_payload(self) -> dict[str, Any]:
        data = dict(self.payload or {})
        data.update(getattr(self, "model_extra", None) or {})
        return data


class ComparisonRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    baseline_ref_json: dict[str, Any] = Field(default_factory=dict)
    candidate_ref_json: dict[str, Any] = Field(default_factory=dict)
    metrics_json: dict[str, Any] = Field(default_factory=dict)
    criteria_json: dict[str, Any] = Field(default_factory=dict)
    verdict: str = "inconclusive"
    reason_md: str | None = None
    stage_attempt_id: str | None = None
    created_by: str = "codex"

    def comparison_payload(self) -> dict[str, Any]:
        data = self.model_dump()
        data.update(getattr(self, "model_extra", None) or {})
        return data


class IssueCreateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    title: str = Field(..., min_length=1)
    severity: str = "medium"
    experiment_id: str | None = None
    created_by: str = "codex"


class PromoteRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    issue_url: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    confirm: str = ""

    def promotion_payload(self) -> dict[str, Any]:
        data = dict(self.payload or {})
        data.update(getattr(self, "model_extra", None) or {})
        return data


class RejectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(..., min_length=1)
    created_by: str = "codex"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class HMMBackfillPreviewRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    source_mode: str = "historical_file"
    source_scope: dict[str, Any] = Field(default_factory=dict)
    policy: dict[str, Any] = Field(default_factory=dict)
    created_by: str = "codex"

    def payload(self) -> dict[str, Any]:
        data = self.model_dump()
        data.update(getattr(self, "model_extra", None) or {})
        return data


class HMMBackfillExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    preview_id: str | None = None
    source_mode: str = "historical_file"
    source_scope: dict[str, Any] = Field(default_factory=dict)
    policy: dict[str, Any] = Field(default_factory=dict)
    dry_run: bool = True
    confirm: str = ""
    created_by: str = "codex"

    def payload(self) -> dict[str, Any]:
        data = self.model_dump()
        data.update(getattr(self, "model_extra", None) or {})
        return data


def get_research_pipeline_service() -> ResearchPipelineService:
    return ResearchPipelineService()


def _success(data: Any) -> ResearchPipelineResponse:
    return ResearchPipelineResponse(data=data)


def _map_error(exc: Exception) -> HTTPException:
    if isinstance(exc, ResearchPipelineNotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, ResearchPipelineConflictError):
        return HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, (ResearchPipelineError, ValueError, ValidationError)):
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


@router.get("/health", response_model=ResearchPipelineResponse)
def health() -> ResearchPipelineResponse:
    return _success({"service": "research-pipeline", "status": "ok"})


@router.post("/experiments", response_model=ResearchPipelineResponse)
def create_experiment(
    request: ResearchExperimentCreateRequest,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.create_experiment(**request.model_dump()))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/experiments", response_model=ResearchPipelineResponse)
def list_experiments(
    status: str | None = Query(None),
    pipeline_type: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(
            service.list_experiments(
                status=status,
                pipeline_type=pipeline_type,
                search=search,
                limit=limit,
                offset=offset,
            )
        )
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/experiments/{experiment_id}", response_model=ResearchPipelineResponse)
def get_experiment(
    experiment_id: str,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.get_experiment(experiment_id))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/experiments/{experiment_id}/stages/{stage_name}/run", response_model=ResearchPipelineResponse)
def run_stage(
    experiment_id: str,
    stage_name: str,
    request: StageActionRequest,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    if request.confirm != RESEARCH_RUN_STAGE_CONFIRM:
        raise HTTPException(status_code=400, detail=f"confirm must equal {RESEARCH_RUN_STAGE_CONFIRM}")
    try:
        return _success(service.run_stage(experiment_id, stage_name, request.action_payload()))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/experiments/{experiment_id}/stages/{stage_name}/retry", response_model=ResearchPipelineResponse)
def retry_stage(
    experiment_id: str,
    stage_name: str,
    request: StageActionRequest,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    if request.confirm != RESEARCH_RETRY_STAGE_CONFIRM:
        raise HTTPException(status_code=400, detail=f"confirm must equal {RESEARCH_RETRY_STAGE_CONFIRM}")
    try:
        return _success(service.retry_stage(experiment_id, stage_name, request.action_payload()))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/experiments/{experiment_id}/stages/{stage_name}", response_model=ResearchPipelineResponse)
def get_stage_result(
    experiment_id: str,
    stage_name: str,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.get_stage_result(experiment_id, stage_name))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/experiments/{experiment_id}/compare", response_model=ResearchPipelineResponse)
def compare_baseline(
    experiment_id: str,
    request: ComparisonRequest,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.compare_baseline(experiment_id, request.comparison_payload()))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/experiments/{experiment_id}/artifact-refs", response_model=ResearchPipelineResponse)
def list_artifact_refs(
    experiment_id: str,
    domain_type: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.list_artifact_refs(experiment_id, domain_type=domain_type, status=status, limit=limit))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/experiments/{experiment_id}/backtest-records", response_model=ResearchPipelineResponse)
def list_backtest_records(
    experiment_id: str,
    research_domain: str | None = Query("hmm"),
    dedup_status: str | None = Query(None),
    qe_archive_representative: bool | None = Query(None),
    source_task_id: str | None = Query(None),
    hmm_config_sig: str | None = Query(None),
    non_hmm_config_sig: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(
            service.list_backtest_records(
                experiment_id,
                research_domain=research_domain,
                dedup_status=dedup_status,
                qe_archive_representative=qe_archive_representative,
                source_task_id=source_task_id,
                hmm_config_sig=hmm_config_sig,
                non_hmm_config_sig=non_hmm_config_sig,
                limit=limit,
                offset=offset,
            )
        )
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/experiments/{experiment_id}/backfill-runs", response_model=ResearchPipelineResponse)
def list_backfill_runs(
    experiment_id: str,
    limit: int = Query(50, ge=1, le=200),
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.list_backfill_runs(experiment_id, limit=limit))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/experiments/{experiment_id}/hmm-backtests/backfill-preview", response_model=ResearchPipelineResponse)
def hmm_backfill_preview(
    experiment_id: str,
    request: HMMBackfillPreviewRequest,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.preview_hmm_backfill(experiment_id, request.payload()))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/experiments/{experiment_id}/hmm-backtests/backfill-execute", response_model=ResearchPipelineResponse)
def hmm_backfill_execute(
    experiment_id: str,
    request: HMMBackfillExecuteRequest,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    if request.confirm != RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM:
        raise HTTPException(status_code=400, detail=f"confirm must equal {RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM}")
    try:
        return _success(service.execute_hmm_backfill(experiment_id, request.payload()))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/backfill-runs/{backfill_run_id}", response_model=ResearchPipelineResponse)
def get_backfill_run(
    backfill_run_id: str,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.get_backfill_run(backfill_run_id))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/pipeline-types", response_model=ResearchPipelineResponse)
def get_pipeline_types(
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    return _success(service.get_pipeline_types())


@router.post("/issues", response_model=ResearchPipelineResponse)
def create_issue(
    request: IssueCreateRequest,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.create_issue(request.model_dump()))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/experiments/{experiment_id}/promote", response_model=ResearchPipelineResponse)
def promote(
    experiment_id: str,
    request: PromoteRequest,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    if request.confirm != RESEARCH_PROMOTE_CONFIRM:
        raise HTTPException(status_code=400, detail=f"confirm must equal {RESEARCH_PROMOTE_CONFIRM}")
    try:
        return _success(service.promote(experiment_id, issue_url=request.issue_url, payload=request.promotion_payload()))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/experiments/{experiment_id}/reject", response_model=ResearchPipelineResponse)
def reject(
    experiment_id: str,
    request: RejectRequest,
    service: ResearchPipelineService = Depends(get_research_pipeline_service),
) -> ResearchPipelineResponse:
    try:
        return _success(service.reject(experiment_id, request.model_dump()))
    except Exception as exc:
        raise _map_error(exc) from exc
