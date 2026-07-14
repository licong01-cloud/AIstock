"""Facade service for local data management MCP operations.

The Research Assistant and MCP gateway use this service through
``/api/v1/local-data/*``.  The facade keeps the existing ingestion APIs as the
execution boundary while adding stable summaries, risk metadata, confirmations,
repair plans, and post-action rechecks.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from psycopg2.extras import RealDictCursor

from backend.db.init_tushare_schedules import get_default_schedule_catalog
from backend.db.pg_pool import get_conn
from backend.routers import ingestion

LOCAL_DATA_CONFIRM_RUN = "RUN_LOCAL_DATA"
LOCAL_DATA_CONFIRM_CHANGE = "APPLY_LOCAL_DATA_CHANGE"
LOCAL_DATA_CONFIRM_DELETE = "DELETE_LOCAL_DATA_RESOURCE"
LOCAL_DATA_CONFIRM_REPAIR = "APPLY_LOCAL_DATA_REPAIR"

CONFIRM_BY_RISK = {
    "run_data_job": LOCAL_DATA_CONFIRM_RUN,
    "write_control_plane": LOCAL_DATA_CONFIRM_CHANGE,
    "destructive": LOCAL_DATA_CONFIRM_DELETE,
    "repair_apply": LOCAL_DATA_CONFIRM_REPAIR,
}

STATUS_LABELS = {
    "fresh": "正常",
    "stale": "缓存滞后",
    "audit_missing": "等待发布",
    "unknown": "未知",
    "pending": "等待处理",
    "retry": "重试中",
    "final_blocked": "最终阻断",
    "reconciled": "正常",
}


class LocalDataManagementService:
    """Stable facade over existing local-data endpoints and status tables."""

    def __init__(self, *, connection_provider: Callable[[], Any] = get_conn, source: Any = ingestion) -> None:
        self.connection_provider = connection_provider
        self.source = source

    def overview(self) -> dict[str, Any]:
        stats = self._source("GET /api/data-stats", self.source.list_data_stats, "local_data_list_data_stats", "read_only")
        jobs = self._source(
            "GET /api/ingestion/jobs",
            self.source.list_ingestion_jobs,
            "local_data_list_jobs",
            "read_only",
            limit=10,
            active_only=False,
        )
        alerts = self._source(
            "GET /api/ingestion/alerts/active",
            self.source.get_active_alerts,
            "local_data_list_alerts",
            "read_only",
            severity_min="warning",
            limit=20,
        )
        targets = self.list_sync_targets(limit=50)["data"]
        datasets = list(stats.get("data", {}).get("items") or [])
        recent_jobs = list(jobs.get("data", {}).get("items") or [])
        active_alerts = list(alerts.get("data", {}).get("alerts") or [])
        sync_targets = list(targets.get("items") or [])

        stale_count = sum(1 for item in datasets if str(item.get("cache_state") or "") in {"stale", "audit_missing", "unknown"})
        blocked_count = sum(1 for item in sync_targets if item.get("target_status") == "final_blocked")
        retry_count = sum(1 for item in sync_targets if item.get("target_status") == "retry")
        running_count = sum(1 for item in recent_jobs if item.get("status") in {"running", "queued", "pending"})
        has_error_alert = any(item.get("severity") in {"error", "critical"} for item in active_alerts)
        status = "red" if blocked_count or has_error_alert else ("yellow" if stale_count or retry_count or active_alerts else "green")
        summary = self._health_summary(status, stale_count, blocked_count, retry_count, len(active_alerts))
        return self._response(
            operation="local_data_health_overview",
            risk_level="read_only",
            source_endpoint="local-data composite",
            data={
                "status": status,
                "status_label": {"green": "正常", "yellow": "需要关注", "red": "存在阻断"}[status],
                "summary": summary,
                "dataset_count": len(datasets),
                "stale_dataset_count": stale_count,
                "running_job_count": running_count,
                "active_alert_count": len(active_alerts),
                "blocked_target_count": blocked_count,
                "retry_target_count": retry_count,
                "affected_modules": self._affected_modules(sync_targets),
                "datasets": datasets[:30],
                "recent_jobs": recent_jobs[:10],
                "active_alerts": active_alerts[:20],
                "sync_targets": sync_targets[:30],
                "next_actions": self._next_actions(stale_count, blocked_count, retry_count, len(active_alerts)),
            },
            summary=summary,
        )

    def dataset_status(self, dataset: str) -> dict[str, Any]:
        dataset_key = self._dataset_key(dataset)
        stats = self.source.list_data_stats()
        rows = list(stats.get("items") or [])
        row = next((item for item in rows if str(item.get("data_kind")) == dataset_key), None)
        if row is None:
            raise HTTPException(status_code=404, detail=f"unknown local data dataset: {dataset_key}")
        related_jobs = [
            item
            for item in (self.source.list_ingestion_jobs(limit=20, active_only=False).get("items") or [])
            if (item.get("meta") or {}).get("dataset") == dataset_key
        ]
        targets = self._fetch_targets(dataset=dataset_key, limit=20)
        label = STATUS_LABELS.get(str(row.get("cache_state") or "unknown"), "未知")
        return self._response(
            operation="local_data_get_dataset_status",
            risk_level="read_only",
            source_endpoint="GET /api/data-stats + ingestion jobs + data_sync_targets",
            data={
                "dataset": dataset_key,
                "status_label": label,
                "ready_date": row.get("ready_date"),
                "physical_max_date": row.get("physical_max_date"),
                "stats_max_date": row.get("stats_max_date"),
                "cache_state": row.get("cache_state"),
                "last_job": related_jobs[0] if related_jobs else None,
                "sync_targets": targets,
                "detail": row,
            },
            summary=f"{dataset_key} 当前状态：{label}。",
        )

    def list_data_stats(self) -> dict[str, Any]:
        return self._source("GET /api/data-stats", self.source.list_data_stats, "local_data_list_data_stats", "read_only")

    def check_gaps(self, *, data_kind: str, start_date: str | None = None, end_date: str | None = None, refresh: bool = False) -> dict[str, Any]:
        data_kind = self._dataset_key(data_kind)
        data = self.source.get_data_gaps(data_kind=data_kind, start_date=start_date, end_date=end_date, refresh=refresh)
        gap_count = len(data.get("missing_ranges") or data.get("gaps") or [])
        return self._response(
            operation="local_data_check_gaps",
            risk_level="read_only",
            source_endpoint="GET /api/data-stats/gaps",
            data=data,
            summary=f"{data_kind} 缺口检查完成，发现 {gap_count} 个缺口区间。",
        )

    def compute_auto_range(self, *, data_kind: str) -> dict[str, Any]:
        data_kind = self._dataset_key(data_kind)
        data = self.source.get_ingestion_auto_range(data_kind=data_kind)
        return self._response(
            operation="local_data_compute_auto_range",
            risk_level="read_only",
            source_endpoint="GET /api/ingestion/auto-range",
            data=data,
            summary=f"{data_kind} 自动补齐区间：{data.get('start_date')} 至 {data.get('latest_date')}。",
        )

    def list_alerts(self, *, severity_min: str = "warning", limit: int = 50) -> dict[str, Any]:
        return self._source(
            "GET /api/ingestion/alerts/active",
            self.source.get_active_alerts,
            "local_data_list_alerts",
            "read_only",
            severity_min=severity_min,
            limit=limit,
        )

    def get_unack_alert_count(self) -> dict[str, Any]:
        return self._source("GET /api/ingestion/alerts/unack-count", self.source.get_unack_alert_count, "local_data_get_unack_alert_count", "read_only")

    def acknowledge_alert(self, *, alert_id: str, confirm_change: str | None) -> dict[str, Any]:
        self._require("write_control_plane", confirm_change)
        return self._source(
            "POST /api/ingestion/alerts/{alert_id}/acknowledge",
            self.source.acknowledge_alert,
            "local_data_acknowledge_alert",
            "write_control_plane",
            alert_id=self._safe_identifier(alert_id, "alert_id"),
        )

    def list_sync_targets(self, *, status: str | None = None, dataset: str | None = None, limit: int = 100) -> dict[str, Any]:
        targets = self._fetch_targets(status=status, dataset=dataset, limit=limit)
        return self._response(
            operation="local_data_list_sync_targets",
            risk_level="read_only",
            source_endpoint="market.data_sync_targets facade",
            data={"items": targets, "count": len(targets)},
            summary=f"查询到 {len(targets)} 个数据同步目标。",
        )

    def get_sync_target(self, *, target_id: str) -> dict[str, Any]:
        target_id = self._safe_identifier(target_id, "target_id")
        targets = self._fetch_targets(target_id=target_id, limit=1)
        if not targets:
            raise HTTPException(status_code=404, detail=f"data sync target not found: {target_id}")
        attempts = self._fetch_attempts(target_id=target_id, limit=50)
        return self._response(
            operation="local_data_get_sync_target",
            risk_level="read_only",
            source_endpoint="market.data_sync_targets + data_sync_attempts facade",
            data={"target": targets[0], "attempts": attempts},
            summary=f"同步目标 {target_id} 当前状态：{targets[0].get('target_status')}。",
        )

    def list_sync_attempts(self, *, target_id: str | None = None, status: str | None = None, limit: int = 50) -> dict[str, Any]:
        safe_target_id = self._safe_identifier(target_id, "target_id") if target_id else None
        attempts = self._fetch_attempts(target_id=safe_target_id, status=status, limit=limit)
        target_text = safe_target_id or "全部目标"
        return self._response(
            operation="local_data_list_sync_attempts",
            risk_level="read_only",
            source_endpoint="market.data_sync_attempts facade",
            data={"items": attempts, "count": len(attempts)},
            summary=f"同步目标 {target_text} 有 {len(attempts)} 条尝试记录。",
        )

    def list_jobs(self, *, limit: int = 50, active_only: bool = False) -> dict[str, Any]:
        return self._source("GET /api/ingestion/jobs", self.source.list_ingestion_jobs, "local_data_list_jobs", "read_only", limit=limit, active_only=active_only)

    def get_job(self, *, job_id: str) -> dict[str, Any]:
        return self._source(
            "GET /api/ingestion/job/{job_id}",
            self.source.get_ingestion_job,
            "local_data_get_job",
            "read_only",
            job_id=uuid.UUID(str(job_id)),
        )

    def get_job_logs(self, *, job_id: str | None = None, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        safe_id = uuid.UUID(str(job_id)) if job_id else None
        data = self.source.list_ingestion_logs(job_id=safe_id, limit=limit, offset=offset)
        logs = [
            {"ts": item.get("ts"), "level": item.get("level"), "message": item.get("message"), "job_id": item.get("job_id")}
            for item in (data.get("items") or [])
        ]
        return self._response(
            operation="local_data_get_job_logs",
            risk_level="read_only",
            source_endpoint="GET /api/ingestion/logs",
            data={"items": logs, "total": data.get("total"), "limit": limit, "offset": offset},
            summary=f"返回 {len(logs)} 条任务日志摘要。",
        )

    def cancel_job(self, *, job_id: str, confirm_change: str | None) -> dict[str, Any]:
        self._require("write_control_plane", confirm_change)
        return self._source(
            "POST /api/ingestion/job/{job_id}/cancel",
            self.source.cancel_ingestion_job,
            "local_data_cancel_job",
            "write_control_plane",
            job_id=uuid.UUID(str(job_id)),
        )

    def clear_queued_jobs(self, *, confirm_delete: str | None) -> dict[str, Any]:
        self._require("destructive", confirm_delete)
        return self._source("DELETE /api/ingestion/jobs/queued", self.source.delete_queued_ingestion_jobs, "local_data_clear_queued_jobs", "destructive")

    def delete_job(self, *, job_id: str, confirm_delete: str | None) -> dict[str, Any]:
        self._require("destructive", confirm_delete)
        return self._source(
            "DELETE /api/ingestion/job/{job_id}",
            self.source.delete_ingestion_job,
            "local_data_delete_job",
            "destructive",
            job_id=uuid.UUID(str(job_id)),
        )

    def run_dataset_sync(self, *, dataset: str, mode: str, options: dict[str, Any] | None, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        payload = self.source.IngestionRunRequest(dataset=dataset, mode=mode, triggered_by="local_data_mcp", options=options or {})
        return self._source("POST /api/ingestion/run", self.source.trigger_ingestion_run, "local_data_run_dataset_sync", "run_data_job", payload=payload)

    def run_incremental(self, *, data_kind: str, start_date: str, workers: int = 1, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        payload = self.source.GoIncrementalRequest(data_kind=data_kind, start_date=start_date, workers=workers)
        return self._source("POST /api/ingestion/incremental", self.source.trigger_go_incremental, "local_data_run_incremental", "run_data_job", payload=payload)

    def run_init(self, *, dataset: str, options: dict[str, Any] | None, confirm_run: str | None, confirm_delete: str | None = None) -> dict[str, Any]:
        risk = "destructive" if bool((options or {}).get("truncate_before")) else "run_data_job"
        self._require(risk, confirm_delete if risk == "destructive" else confirm_run)
        payload = self.source.IngestionInitRequest(dataset=dataset, options=options or {})
        return self._source("POST /api/ingestion/init", self.source.start_ingestion_init, "local_data_run_init", risk, payload=payload)

    def run_schedule(self, *, schedule_id: str, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        return self._source("POST /api/ingestion/schedule/{id}/run", self.source.run_ingestion_schedule, "local_data_run_schedule", "run_data_job", schedule_id=uuid.UUID(str(schedule_id)))

    def run_single_preset(self, *, dataset: str, workers: int | None, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        payload = self.source.RunSinglePresetRequest(dataset=dataset, workers=workers)
        return self._source("POST /api/ingestion/schedule/run-single-preset", self.source.run_single_preset, "local_data_run_single_preset", "run_data_job", req=payload)

    def run_all_presets(self, *, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        return self._source("POST /api/ingestion/schedule/run-all-presets", self.source.run_all_preset_schedules, "local_data_run_all_presets", "run_data_job")

    def refresh_stats(self, *, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        refresh = self.source.refresh_data_stats()
        post_check = self.source.list_data_stats()
        return self._response(
            operation="local_data_refresh_stats",
            risk_level="run_data_job",
            source_endpoint="POST /api/data-stats/refresh",
            data={"refresh": refresh, "post_check": post_check},
            summary="data_stats 已刷新，并已完成刷新后复查。",
        )

    def sync_calendar(self, *, payload: dict[str, Any] | None = None, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        request = self.source.CalendarSyncRequest(**payload) if payload else None
        return self._source("POST /api/calendar/sync", self.source.calendar_sync, "local_data_sync_calendar", "run_data_job", payload=request)

    def build_sector_data(self, *, start_date: str, end_date: str, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        return self._source("POST /api/sector-data/build", self.source.build_sector_data, "local_data_build_sector_data", "run_data_job", start_date=start_date, end_date=end_date)

    def export_sector_data(self, *, snapshot_id: str, start_date: str, end_date: str, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        return self._source(
            "POST /api/sector-data/export",
            self.source.export_sector_data,
            "local_data_export_sector_data",
            "run_data_job",
            snapshot_id=snapshot_id,
            start_date=start_date,
            end_date=end_date,
        )

    def sync_tushare_all(self, *, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        return self._source("POST /api/ingestion/tushare/sync-all", self.source.tushare_sync_all, "local_data_sync_tushare_all", "run_data_job")

    def list_schedules(self) -> dict[str, Any]:
        return self._source("GET /api/ingestion/schedule", self.source.list_ingestion_schedules, "local_data_list_schedules", "read_only")

    def get_schedule_defaults(self) -> dict[str, Any]:
        catalog = self._validated_default_schedule_catalog()
        templates = catalog["templates"]
        return self._response(
            operation="local_data_get_schedule_defaults",
            risk_level="read_only",
            source_endpoint="local default schedule template",
            data={
                "items": templates,
                "catalog_version": catalog["version"],
                "catalog_fingerprint": catalog["fingerprint"],
            },
            summary=f"当前推荐默认计划包含 {len(templates)} 个任务。",
        )

    def get_preset_stats(self) -> dict[str, Any]:
        return self._source(
            "GET /api/ingestion/schedule/preset-stats",
            self.source.get_preset_stats,
            "local_data_get_preset_stats",
            "read_only",
        )

    def get_preset_daily_status(self) -> dict[str, Any]:
        return self._source(
            "GET /api/ingestion/schedule/preset-daily-status",
            self.source.get_preset_daily_status,
            "local_data_get_preset_daily_status",
            "read_only",
        )

    def upsert_schedule(self, *, payload: dict[str, Any], confirm_change: str | None) -> dict[str, Any]:
        self._require("write_control_plane", confirm_change)
        request = self.source.IngestionScheduleUpsertRequest(**payload)
        return self._source("POST /api/ingestion/schedule", self.source.upsert_ingestion_schedule, "local_data_upsert_schedule", "write_control_plane", payload=request)

    def batch_create_schedules(self, *, items: list[dict[str, Any]], confirm_change: str | None) -> dict[str, Any]:
        self._require("write_control_plane", confirm_change)
        payload = self.source.BatchCreateSchedulesRequest(items=[self.source.BatchScheduleItem(**item) for item in items])
        return self._source("POST /api/ingestion/schedule/batch-create", self.source.batch_create_ingestion_schedules, "local_data_batch_create_schedules", "write_control_plane", payload=payload)

    def toggle_schedule(self, *, schedule_id: str, enabled: bool, confirm_change: str | None) -> dict[str, Any]:
        self._require("write_control_plane", confirm_change)
        request = self.source.ToggleRequest(enabled=enabled)
        return self._source("POST /api/ingestion/schedule/{id}/toggle", self.source.toggle_ingestion_schedule, "local_data_toggle_schedule", "write_control_plane", schedule_id=uuid.UUID(str(schedule_id)), payload=request)

    def delete_schedule(self, *, schedule_id: str, confirm_delete: str | None) -> dict[str, Any]:
        self._require("destructive", confirm_delete)
        return self._source("DELETE /api/ingestion/schedule/{id}", self.source.delete_ingestion_schedule, "local_data_delete_schedule", "destructive", schedule_id=uuid.UUID(str(schedule_id)))

    def plan_schedule_reset(self, *, delete_missing: bool = False) -> dict[str, Any]:
        if delete_missing:
            raise HTTPException(
                status_code=400,
                detail="destructive schedule reset is disabled; review and delete one schedule explicitly if required",
            )
        catalog = self._validated_default_schedule_catalog()
        current = self.source.list_ingestion_schedules().get("items") or []
        current_by_key = {(item.get("dataset"), item.get("mode")): item for item in current}
        desired_by_key = {(item.get("dataset"), item.get("mode")): item for item in catalog["templates"]}
        actions: list[dict[str, Any]] = []
        for key, desired in desired_by_key.items():
            existing = current_by_key.get(key)
            if not existing:
                actions.append({"action": "create", "key": list(key), "desired": desired})
                continue
            diff = {
                field: {"current": existing.get(field), "desired": desired.get(field)}
                for field in ("frequency", "enabled", "options")
                if existing.get(field) != desired.get(field)
            }
            if diff:
                actions.append({"action": "update", "key": list(key), "schedule_id": existing.get("schedule_id"), "diff": diff, "desired": desired})
        plan_payload = {
            "actions": actions,
            "delete_missing": False,
            "catalog_version": catalog["version"],
            "catalog_fingerprint": catalog["fingerprint"],
        }
        plan = {"plan_id": self._plan_id(plan_payload), **plan_payload}
        return self._response(
            operation="local_data_plan_schedule_reset",
            risk_level="plan_only",
            source_endpoint="current schedules + default schedule template",
            data=plan,
            summary=f"计划任务重置预案包含 {len(actions)} 个变更，尚未写入。",
        )

    def apply_schedule_reset(self, *, plan: dict[str, Any] | None, confirm_change: str | None, confirm_delete: str | None = None) -> dict[str, Any]:
        canonical_plan = self.plan_schedule_reset(delete_missing=False)["data"]
        plan_data = plan or canonical_plan
        actions = list(plan_data.get("actions") or [])
        destructive = any(action.get("action") == "delete" for action in actions)
        if destructive:
            raise HTTPException(status_code=400, detail="destructive schedule reset actions are not supported")
        if plan_data != canonical_plan:
            raise HTTPException(
                status_code=409,
                detail="schedule reset plan is stale or does not match the current canonical catalog; regenerate the plan",
            )
        self._require("write_control_plane", confirm_change)
        results: list[dict[str, Any]] = []
        for action in actions:
            if action.get("action") in {"create", "update"}:
                result = self.upsert_schedule(payload=dict(action.get("desired") or {}), confirm_change=LOCAL_DATA_CONFIRM_CHANGE)["data"]
            elif action.get("action") == "delete":
                result = self.delete_schedule(schedule_id=str(action.get("schedule_id")), confirm_delete=LOCAL_DATA_CONFIRM_DELETE)["data"]
            else:
                raise HTTPException(status_code=400, detail=f"unsupported schedule reset action: {action.get('action')}")
            results.append({"action": action.get("action"), "result": result})
        return self._response(
            operation="local_data_apply_schedule_reset",
            risk_level="destructive" if destructive else "write_control_plane",
            source_endpoint="schedule reset plan",
            data={"plan_id": plan_data.get("plan_id"), "applied": results, "post_check": self.source.list_ingestion_schedules()},
            summary=f"计划任务重置已执行 {len(results)} 个变更，并已复查。",
        )

    @staticmethod
    def _validated_default_schedule_catalog() -> dict[str, Any]:
        catalog = get_default_schedule_catalog()
        if not catalog.get("complete"):
            errors = "; ".join(str(item) for item in catalog.get("errors") or []) or "empty catalog"
            raise HTTPException(status_code=503, detail=f"default schedule catalog is incomplete: {errors}")
        return catalog

    def run_source_test(self, *, payload: dict[str, Any] | None, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        request = self.source.TestingRunRequest(**(payload or {"triggered_by": "local_data_mcp"}))
        return self._source("POST /api/testing/run", self.source.trigger_testing_run, "local_data_run_source_test", "run_data_job", payload=request)

    def list_source_test_runs(self, *, limit: int = 20, offset: int = 0) -> dict[str, Any]:
        return self._source("GET /api/testing/runs", self.source.list_testing_runs, "local_data_list_source_test_runs", "read_only", limit=limit, offset=offset)

    def list_source_test_schedules(self) -> dict[str, Any]:
        return self._source("GET /api/testing/schedule", self.source.list_testing_schedules, "local_data_list_source_test_schedules", "read_only")

    def upsert_source_test_schedule(self, *, payload: dict[str, Any], confirm_change: str | None) -> dict[str, Any]:
        self._require("write_control_plane", confirm_change)
        request = self.source.TestingScheduleUpsertRequest(**payload)
        return self._source("POST /api/testing/schedule", self.source.upsert_testing_schedule, "local_data_upsert_source_test_schedule", "write_control_plane", payload=request)

    def toggle_source_test_schedule(self, *, schedule_id: str, enabled: bool, confirm_change: str | None) -> dict[str, Any]:
        self._require("write_control_plane", confirm_change)
        request = self.source.ToggleRequest(enabled=enabled)
        return self._source("POST /api/testing/schedule/{id}/toggle", self.source.toggle_testing_schedule, "local_data_toggle_source_test_schedule", "write_control_plane", schedule_id=uuid.UUID(str(schedule_id)), payload=request)

    def run_source_test_schedule(self, *, schedule_id: str, confirm_run: str | None) -> dict[str, Any]:
        self._require("run_data_job", confirm_run)
        return self._source("POST /api/testing/schedule/{id}/run", self.source.run_testing_schedule, "local_data_run_source_test_schedule", "run_data_job", schedule_id=uuid.UUID(str(schedule_id)))

    def plan_repair(self, *, dataset: str | None = None, include_destructive: bool = False) -> dict[str, Any]:
        overview = self.overview()["data"]
        steps: list[dict[str, Any]] = []
        if dataset:
            status = self.dataset_status(dataset)["data"]
            if status.get("cache_state") in {"stale", "audit_missing", "unknown"}:
                steps.append({"step_id": "compute_auto_range", "action": "compute_auto_range", "dataset": dataset, "risk_level": "read_only"})
                steps.append({"step_id": "run_dataset_sync", "action": "run_dataset_sync", "dataset": dataset, "mode": "incremental", "risk_level": "run_data_job"})
        if overview.get("stale_dataset_count"):
            steps.append({"step_id": "refresh_stats", "action": "refresh_stats", "risk_level": "run_data_job"})
        if overview.get("active_alert_count"):
            steps.append({"step_id": "review_alerts", "action": "review_alerts", "risk_level": "read_only", "message": "先查看告警，不自动确认。"})
        if overview.get("blocked_target_count"):
            steps.append({"step_id": "blocked_targets", "action": "manual_review", "risk_level": "write_control_plane", "message": "存在 final_blocked target，需要人工决定是否重新运行同步。"})
        plan = {
            "plan_id": self._plan_id({"dataset": dataset, "include_destructive": include_destructive, "steps": steps}),
            "dataset": dataset,
            "steps": steps,
            "requires_confirmation": any(step.get("risk_level") != "read_only" for step in steps),
            "destructive_included": include_destructive,
            "overview_summary": overview.get("summary"),
        }
        return self._response(
            operation="local_data_plan_repair",
            risk_level="plan_only",
            source_endpoint="overview + dataset status + targets + alerts",
            data=plan,
            summary=f"本地数据修复计划包含 {len(steps)} 个步骤，尚未执行。",
        )

    def apply_repair(self, *, plan: dict[str, Any], confirm_repair: str | None) -> dict[str, Any]:
        self._require("repair_apply", confirm_repair)
        results: list[dict[str, Any]] = []
        for step in list((plan or {}).get("steps") or []):
            action = step.get("action")
            try:
                if action == "compute_auto_range":
                    result = self.compute_auto_range(data_kind=str(step.get("dataset")))["data"]
                elif action == "run_dataset_sync":
                    result = self.run_dataset_sync(
                        dataset=str(step.get("dataset")),
                        mode=str(step.get("mode") or "incremental"),
                        options=step.get("options") or {},
                        confirm_run=LOCAL_DATA_CONFIRM_RUN,
                    )["data"]
                elif action == "refresh_stats":
                    result = self.refresh_stats(confirm_run=LOCAL_DATA_CONFIRM_RUN)["data"]
                elif action in {"review_alerts", "manual_review"}:
                    result = {"skipped_execution": True, "reason": step.get("message") or "requires human review"}
                else:
                    raise ValueError(f"unsupported repair action: {action}")
            except Exception as exc:  # noqa: BLE001
                results.append({"step": step, "status": "failed", "error": str(exc)})
                return self._response(
                    operation="local_data_apply_repair",
                    risk_level="repair_apply",
                    source_endpoint="repair plan executor",
                    data={"plan_id": (plan or {}).get("plan_id"), "status": "failed", "results": results},
                    summary=f"修复执行在步骤 {step.get('step_id') or action} 失败，已停止。",
                )
            results.append({"step": step, "status": "completed", "result": result})
        return self._response(
            operation="local_data_apply_repair",
            risk_level="repair_apply",
            source_endpoint="repair plan executor",
            data={"plan_id": (plan or {}).get("plan_id"), "status": "completed", "results": results, "post_check": self.overview()["data"]},
            summary=f"修复执行完成 {len(results)} 个步骤，并已复查总体状态。",
        )

    def get_repair_status(self, *, plan_id: str | None = None) -> dict[str, Any]:
        return self._response(
            operation="local_data_get_repair_status",
            risk_level="read_only",
            source_endpoint="overview + jobs + targets",
            data={"plan_id": plan_id, "overview": self.overview()["data"]},
            summary="已返回修复状态复查摘要。",
        )

    def explain_business_impact(self, *, dataset: str | None = None) -> dict[str, Any]:
        detail = {
            "QE": "回测和实验创建依赖 PIT 股票池、行情、因子和数据 readiness。",
            "Selection Center": "选股执行依赖行情、策略包输入数据和 readiness 缓存。",
            "Paper v2": "模拟盘依赖候选策略包、选股结果和本地行情数据状态。",
            "股票分析": "股票分析报告依赖行情、公告、行业和资金流等本地数据。",
        }
        return self._response(
            operation="local_data_explain_business_impact",
            risk_level="read_only",
            source_endpoint="local data memory graph",
            data={"dataset": dataset, "affected_modules": list(detail), "details": detail},
            summary="本地数据状态会影响 QE、Selection Center、Paper v2 和股票分析。",
        )

    def _source(self, endpoint: str, func: Callable[..., Any], operation: str, risk_level: str, **kwargs: Any) -> dict[str, Any]:
        return self._response(operation=operation, risk_level=risk_level, source_endpoint=endpoint, data=func(**kwargs), summary=f"{operation} 已完成。")

    def _response(self, *, operation: str, risk_level: str, source_endpoint: str, data: Any, summary: str) -> dict[str, Any]:
        return jsonable_encoder(
            {
                "success": True,
                "operation": operation,
                "risk_level": risk_level,
                "summary": summary,
                "data": data,
                "trace": {
                    "source_endpoint": source_endpoint,
                    "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "confirmation_required": risk_level in CONFIRM_BY_RISK,
                    "required_confirmation": CONFIRM_BY_RISK.get(risk_level),
                },
            }
        )

    def _fetch_targets(self, *, target_id: str | None = None, status: str | None = None, dataset: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        filters: list[str] = []
        params: list[Any] = []
        if target_id:
            filters.append("target_id = %s")
            params.append(target_id)
        if status:
            filters.append("target_status = %s")
            params.append(status)
        if dataset:
            filters.append("dataset = %s")
            params.append(self._dataset_key(dataset))
        where = " WHERE " + " AND ".join(filters) if filters else ""
        params.append(max(1, min(int(limit or 100), 500)))
        sql = f"""
            SELECT target_id, dataset, data_source, target_date, target_scope,
                   target_status, priority, required_before, next_retry_at,
                   expected_rows, observed_rows, data_max_at, attempt_count,
                   last_attempt_id, last_attempt_status, last_error_message,
                   metadata, created_at, updated_at, reconciled_at, blocked_at
              FROM market.data_sync_targets
              {where}
             ORDER BY priority ASC, COALESCE(required_before, created_at) ASC, created_at DESC
             LIMIT %s
        """
        with self.connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return jsonable_encoder([dict(row) for row in cur.fetchall()])

    def _fetch_attempts(self, *, target_id: str | None = None, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        filters: list[str] = []
        params: list[Any] = []
        if target_id:
            filters.append("target_id = %s")
            params.append(target_id)
        if status:
            filters.append("status = %s")
            params.append(status)
        where = " WHERE " + " AND ".join(filters) if filters else ""
        params.append(max(1, min(int(limit or 50), 500)))
        with self.connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT attempt_id, target_id, attempt_no, status, trigger_source,
                           worker_id, run_id, job_id, started_at, finished_at,
                           rows_written, rows_observed, coverage_ratio, data_max_at,
                           error_message, retry_after, context_json
                      FROM market.data_sync_attempts
                      {where}
                     ORDER BY attempt_no DESC
                     LIMIT %s
                    """,
                    params,
                )
                return jsonable_encoder([dict(row) for row in cur.fetchall()])

    @staticmethod
    def _dataset_key(dataset: str) -> str:
        value = str(dataset or "").strip().lower()
        if not value:
            raise HTTPException(status_code=400, detail="dataset/data_kind is required")
        return value

    @staticmethod
    def _safe_identifier(value: str, name: str) -> str:
        text = str(value or "").strip()
        if not text or any(ch in text for ch in "/\\\r\n\t"):
            raise HTTPException(status_code=400, detail=f"invalid {name}")
        return text

    @staticmethod
    def _require(risk_level: str, provided: str | None) -> None:
        expected = CONFIRM_BY_RISK.get(risk_level)
        if expected and provided != expected:
            raise HTTPException(status_code=400, detail=f"confirmation required: {expected}")

    @staticmethod
    def _plan_id(payload: dict[str, Any]) -> str:
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]
        return f"ldmp_{digest}"

    @staticmethod
    def _health_summary(status: str, stale: int, blocked: int, retry: int, alerts: int) -> str:
        if status == "green":
            return "本地数据管理状态正常，暂未发现需要处理的阻断项。"
        parts = []
        if stale:
            parts.append(f"{stale} 个数据集缓存滞后或状态未知")
        if blocked:
            parts.append(f"{blocked} 个同步目标最终阻断")
        if retry:
            parts.append(f"{retry} 个同步目标等待重试")
        if alerts:
            parts.append(f"{alerts} 条活跃告警")
        return "本地数据需要关注：" + "，".join(parts) + "。"

    @staticmethod
    def _affected_modules(targets: list[dict[str, Any]]) -> list[str]:
        modules = {"QE", "Selection Center", "Paper v2", "股票分析"}
        if targets:
            modules.add("本地数据同步")
        return sorted(modules)

    @staticmethod
    def _next_actions(stale: int, blocked: int, retry: int, alerts: int) -> list[str]:
        actions = []
        if stale:
            actions.append("生成修复计划，先计算缺口和自动补齐区间。")
        if retry:
            actions.append("检查 retry target 的最近 attempt 和下一次重试时间。")
        if blocked:
            actions.append("查看 final_blocked target 的失败原因，人工确认后再重新调度。")
        if alerts:
            actions.append("查看告警详情；确认告警不会改变 readiness 事实。")
        return actions or ["保持观察；如新增数据同步任务，运行后复查 overview。"]
