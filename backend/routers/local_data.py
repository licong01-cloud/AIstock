"""Stable `/api/v1/local-data/*` facade for assistant and MCP access."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from backend.services.local_data_management import LocalDataManagementService

router = APIRouter(prefix="/local-data", tags=["local-data"])


def get_local_data_service() -> LocalDataManagementService:
    return LocalDataManagementService()


class ConfirmedRunRequest(BaseModel):
    confirm_run: str | None = None


class ConfirmedChangeRequest(BaseModel):
    confirm_change: str | None = None


class ConfirmedDeleteRequest(BaseModel):
    confirm_delete: str | None = None


class DatasetSyncRequest(ConfirmedRunRequest):
    dataset: str
    mode: str
    options: dict[str, Any] = Field(default_factory=dict)


class IncrementalRunRequest(ConfirmedRunRequest):
    data_kind: str
    start_date: str
    workers: int = 1


class InitRunRequest(ConfirmedRunRequest):
    dataset: str
    options: dict[str, Any] = Field(default_factory=dict)
    confirm_delete: str | None = None


class SinglePresetRunRequest(ConfirmedRunRequest):
    dataset: str
    workers: int | None = None


class ScheduleUpsertRequest(ConfirmedChangeRequest):
    payload: dict[str, Any]


class ScheduleBatchCreateRequest(ConfirmedChangeRequest):
    items: list[dict[str, Any]]


class ScheduleToggleRequest(ConfirmedChangeRequest):
    enabled: bool


class ScheduleResetApplyRequest(BaseModel):
    plan: dict[str, Any] | None = None
    confirm_change: str | None = None
    confirm_delete: str | None = None


class SourceTestRunRequest(ConfirmedRunRequest):
    payload: dict[str, Any] = Field(default_factory=dict)


class SourceTestScheduleUpsertRequest(ConfirmedChangeRequest):
    payload: dict[str, Any]


class SourceTestScheduleToggleRequest(ConfirmedChangeRequest):
    enabled: bool


class RepairPlanRequest(BaseModel):
    dataset: str | None = None
    include_destructive: bool = False


class RepairApplyRequest(BaseModel):
    plan: dict[str, Any]
    confirm_repair: str | None = None


class CalendarSyncRequest(ConfirmedRunRequest):
    payload: dict[str, Any] = Field(default_factory=dict)


class SectorBuildRequest(ConfirmedRunRequest):
    start_date: str
    end_date: str


class SectorExportRequest(ConfirmedRunRequest):
    snapshot_id: str
    start_date: str
    end_date: str


@router.get("/overview")
def overview(service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.overview()


@router.get("/datasets/{dataset}/status")
def dataset_status(dataset: str, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.dataset_status(dataset)


@router.get("/data-stats")
def list_data_stats(service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.list_data_stats()


@router.get("/gaps")
def gaps(
    data_kind: str,
    start_date: str | None = None,
    end_date: str | None = None,
    refresh: bool = False,
    service: LocalDataManagementService = Depends(get_local_data_service),
) -> dict[str, Any]:
    return service.check_gaps(data_kind=data_kind, start_date=start_date, end_date=end_date, refresh=refresh)


@router.get("/auto-range")
def auto_range(data_kind: str, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.compute_auto_range(data_kind=data_kind)


@router.get("/alerts")
def alerts(
    severity_min: str = "warning",
    limit: int = Query(50, ge=1, le=500),
    service: LocalDataManagementService = Depends(get_local_data_service),
) -> dict[str, Any]:
    return service.list_alerts(severity_min=severity_min, limit=limit)


@router.get("/alerts/unack-count")
def unack_alert_count(service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.get_unack_alert_count()


@router.post("/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: str, request: ConfirmedChangeRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.acknowledge_alert(alert_id=alert_id, confirm_change=request.confirm_change)


@router.get("/targets")
def targets(
    status: str | None = None,
    dataset: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    service: LocalDataManagementService = Depends(get_local_data_service),
) -> dict[str, Any]:
    return service.list_sync_targets(status=status, dataset=dataset, limit=limit)


@router.get("/targets/{target_id}")
def target_detail(target_id: str, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.get_sync_target(target_id=target_id)


@router.get("/targets/{target_id}/attempts")
def target_attempts(target_id: str, limit: int = Query(50, ge=1, le=500), service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.list_sync_attempts(target_id=target_id, limit=limit)


@router.get("/sync-attempts")
def sync_attempts(
    target_id: str | None = None,
    status: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    service: LocalDataManagementService = Depends(get_local_data_service),
) -> dict[str, Any]:
    return service.list_sync_attempts(target_id=target_id, status=status, limit=limit)


@router.get("/jobs")
def jobs(limit: int = Query(50, ge=1, le=500), active_only: bool = False, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.list_jobs(limit=limit, active_only=active_only)


@router.get("/jobs/{job_id}")
def job_detail(job_id: str, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.get_job(job_id=job_id)


@router.get("/jobs/{job_id}/logs")
def job_logs(job_id: str, limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0), service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.get_job_logs(job_id=job_id, limit=limit, offset=offset)


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, request: ConfirmedChangeRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.cancel_job(job_id=job_id, confirm_change=request.confirm_change)


@router.delete("/jobs/queued")
def clear_queued_jobs(request: ConfirmedDeleteRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.clear_queued_jobs(confirm_delete=request.confirm_delete)


@router.delete("/jobs/{job_id}")
def delete_job(job_id: str, request: ConfirmedDeleteRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.delete_job(job_id=job_id, confirm_delete=request.confirm_delete)


@router.post("/run")
def run_dataset_sync(request: DatasetSyncRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.run_dataset_sync(dataset=request.dataset, mode=request.mode, options=request.options, confirm_run=request.confirm_run)


@router.post("/incremental")
def run_incremental(request: IncrementalRunRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.run_incremental(data_kind=request.data_kind, start_date=request.start_date, workers=request.workers, confirm_run=request.confirm_run)


@router.post("/init")
def run_init(request: InitRunRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.run_init(dataset=request.dataset, options=request.options, confirm_run=request.confirm_run, confirm_delete=request.confirm_delete)


@router.post("/stats/refresh")
def refresh_stats(request: ConfirmedRunRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.refresh_stats(confirm_run=request.confirm_run)


@router.get("/schedules")
def schedules(service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.list_schedules()


@router.get("/schedules/defaults")
def schedule_defaults(service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.get_schedule_defaults()


@router.get("/schedules/preset-stats")
def preset_stats(service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.get_preset_stats()


@router.get("/schedules/preset-daily-status")
def preset_daily_status(service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.get_preset_daily_status()


@router.post("/schedules")
def upsert_schedule(request: ScheduleUpsertRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.upsert_schedule(payload=request.payload, confirm_change=request.confirm_change)


@router.post("/schedules/batch-create")
def batch_create_schedules(request: ScheduleBatchCreateRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.batch_create_schedules(items=request.items, confirm_change=request.confirm_change)


@router.post("/schedules/{schedule_id}/toggle")
def toggle_schedule(schedule_id: str, request: ScheduleToggleRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.toggle_schedule(schedule_id=schedule_id, enabled=request.enabled, confirm_change=request.confirm_change)


@router.post("/schedules/{schedule_id}/run")
def run_schedule(schedule_id: str, request: ConfirmedRunRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.run_schedule(schedule_id=schedule_id, confirm_run=request.confirm_run)


@router.delete("/schedules/{schedule_id}")
def delete_schedule(schedule_id: str, request: ConfirmedDeleteRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.delete_schedule(schedule_id=schedule_id, confirm_delete=request.confirm_delete)


@router.post("/schedules/reset-plan")
def schedule_reset_plan(delete_missing: bool = False, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.plan_schedule_reset(delete_missing=delete_missing)


@router.post("/schedules/reset-apply")
def schedule_reset_apply(request: ScheduleResetApplyRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.apply_schedule_reset(plan=request.plan, confirm_change=request.confirm_change, confirm_delete=request.confirm_delete)


@router.post("/schedules/run-single-preset")
def run_single_preset(request: SinglePresetRunRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.run_single_preset(dataset=request.dataset, workers=request.workers, confirm_run=request.confirm_run)


@router.post("/schedules/run-all-presets")
def run_all_presets(request: ConfirmedRunRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.run_all_presets(confirm_run=request.confirm_run)


@router.post("/calendar/sync")
def sync_calendar(request: CalendarSyncRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.sync_calendar(payload=request.payload, confirm_run=request.confirm_run)


@router.post("/sector-data/build")
def build_sector_data(request: SectorBuildRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.build_sector_data(start_date=request.start_date, end_date=request.end_date, confirm_run=request.confirm_run)


@router.post("/sector-data/export")
def export_sector_data(request: SectorExportRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.export_sector_data(
        snapshot_id=request.snapshot_id,
        start_date=request.start_date,
        end_date=request.end_date,
        confirm_run=request.confirm_run,
    )


@router.post("/tushare/sync-all")
def sync_tushare_all(request: ConfirmedRunRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.sync_tushare_all(confirm_run=request.confirm_run)


@router.post("/testing/run")
def run_source_test(request: SourceTestRunRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.run_source_test(payload=request.payload, confirm_run=request.confirm_run)


@router.get("/testing/runs")
def source_test_runs(limit: int = Query(20, ge=1, le=500), offset: int = Query(0, ge=0), service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.list_source_test_runs(limit=limit, offset=offset)


@router.get("/testing/schedules")
def source_test_schedules(service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.list_source_test_schedules()


@router.post("/testing/schedules")
def upsert_source_test_schedule(request: SourceTestScheduleUpsertRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.upsert_source_test_schedule(payload=request.payload, confirm_change=request.confirm_change)


@router.post("/testing/schedules/{schedule_id}/toggle")
def toggle_source_test_schedule(schedule_id: str, request: SourceTestScheduleToggleRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.toggle_source_test_schedule(schedule_id=schedule_id, enabled=request.enabled, confirm_change=request.confirm_change)


@router.post("/testing/schedules/{schedule_id}/run")
def run_source_test_schedule(schedule_id: str, request: ConfirmedRunRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.run_source_test_schedule(schedule_id=schedule_id, confirm_run=request.confirm_run)


@router.post("/repair-plan")
def repair_plan(request: RepairPlanRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.plan_repair(dataset=request.dataset, include_destructive=request.include_destructive)


@router.post("/repair-apply")
def repair_apply(request: RepairApplyRequest, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.apply_repair(plan=request.plan, confirm_repair=request.confirm_repair)


@router.get("/repair-status")
def repair_status(plan_id: str | None = None, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.get_repair_status(plan_id=plan_id)


@router.get("/business-impact")
def business_impact(dataset: str | None = None, service: LocalDataManagementService = Depends(get_local_data_service)) -> dict[str, Any]:
    return service.explain_business_impact(dataset=dataset)
