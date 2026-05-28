"""Local Data Management MCP tool wrappers.

The local_data module is intentionally a thin MCP Gateway layer. It validates
path fragments and confirmation text, then calls the loopback
``/api/v1/local-data/*`` facade. It must not import runtime task modules,
repositories, process-spawning helpers, or database access code.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


LOCAL_DATA_CANCEL_JOB_CONFIRM = "APPLY_LOCAL_DATA_CHANGE"
LOCAL_DATA_CLEAR_QUEUED_JOBS_CONFIRM = "DELETE_LOCAL_DATA_RESOURCE"
LOCAL_DATA_DELETE_JOB_CONFIRM = "DELETE_LOCAL_DATA_RESOURCE"
LOCAL_DATA_RUN_DATASET_SYNC_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_RUN_INCREMENTAL_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_RUN_INIT_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_RUN_SCHEDULE_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_RUN_SINGLE_PRESET_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_RUN_ALL_PRESETS_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_REFRESH_STATS_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_SYNC_CALENDAR_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_BUILD_SECTOR_DATA_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_EXPORT_SECTOR_DATA_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_SYNC_TUSHARE_ALL_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_UPSERT_SCHEDULE_CONFIRM = "APPLY_LOCAL_DATA_CHANGE"
LOCAL_DATA_BATCH_CREATE_SCHEDULES_CONFIRM = "APPLY_LOCAL_DATA_CHANGE"
LOCAL_DATA_TOGGLE_SCHEDULE_CONFIRM = "APPLY_LOCAL_DATA_CHANGE"
LOCAL_DATA_DELETE_SCHEDULE_CONFIRM = "DELETE_LOCAL_DATA_RESOURCE"
LOCAL_DATA_APPLY_SCHEDULE_RESET_CONFIRM = "APPLY_LOCAL_DATA_CHANGE"
LOCAL_DATA_RUN_SOURCE_TEST_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_UPSERT_SOURCE_TEST_SCHEDULE_CONFIRM = "APPLY_LOCAL_DATA_CHANGE"
LOCAL_DATA_TOGGLE_SOURCE_TEST_SCHEDULE_CONFIRM = "APPLY_LOCAL_DATA_CHANGE"
LOCAL_DATA_RUN_SOURCE_TEST_SCHEDULE_CONFIRM = "RUN_LOCAL_DATA"
LOCAL_DATA_APPLY_REPAIR_CONFIRM = "APPLY_LOCAL_DATA_REPAIR"

TOOL_NAMES = (
    "local_data_health_overview",
    "local_data_get_dataset_status",
    "local_data_list_data_stats",
    "local_data_check_gaps",
    "local_data_compute_auto_range",
    "local_data_list_alerts",
    "local_data_get_unack_alert_count",
    "local_data_list_sync_targets",
    "local_data_get_sync_target",
    "local_data_list_sync_attempts",
    "local_data_list_jobs",
    "local_data_get_job",
    "local_data_get_job_logs",
    "local_data_cancel_job_confirmed",
    "local_data_clear_queued_jobs_confirmed",
    "local_data_delete_job_confirmed",
    "local_data_run_dataset_sync_confirmed",
    "local_data_run_incremental_confirmed",
    "local_data_run_init_confirmed",
    "local_data_run_schedule_confirmed",
    "local_data_run_single_preset_confirmed",
    "local_data_run_all_presets_confirmed",
    "local_data_refresh_stats_confirmed",
    "local_data_sync_calendar_confirmed",
    "local_data_build_sector_data_confirmed",
    "local_data_export_sector_data_confirmed",
    "local_data_sync_tushare_all_confirmed",
    "local_data_list_schedules",
    "local_data_get_schedule_defaults",
    "local_data_upsert_schedule_confirmed",
    "local_data_batch_create_schedules_confirmed",
    "local_data_toggle_schedule_confirmed",
    "local_data_delete_schedule_confirmed",
    "local_data_plan_schedule_reset",
    "local_data_apply_schedule_reset_confirmed",
    "local_data_get_preset_stats",
    "local_data_get_preset_daily_status",
    "local_data_run_source_test_confirmed",
    "local_data_list_source_test_runs",
    "local_data_list_source_test_schedules",
    "local_data_upsert_source_test_schedule_confirmed",
    "local_data_toggle_source_test_schedule_confirmed",
    "local_data_run_source_test_schedule_confirmed",
    "local_data_plan_repair",
    "local_data_apply_repair_confirmed",
    "local_data_get_repair_status",
    "local_data_explain_business_impact",
)
TOOL_COUNT = len(TOOL_NAMES)


def _fragment(registry: "ModuleRegistry", value: Any, name: str) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a string or integer path fragment; got {value!r}")
    raw = str(value) if isinstance(value, int) else value
    return registry.sanitize(raw, name)


def _optional_fragment(registry: "ModuleRegistry", value: Any, name: str) -> str | None:
    if value is None:
        return None
    return _fragment(registry, value, name)


def _body(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return dict(payload or {})


def _confirmed_body(
    registry: "ModuleRegistry",
    *,
    confirm: str | None,
    expected: str,
    payload: dict[str, Any] | None = None,
    field: str = "confirm",
) -> dict[str, Any]:
    registry.confirm(confirm, expected, field)
    body = _body(payload)
    if field != "confirm":
        body.pop("confirm", None)
    body[field] = expected
    return body


def register(registry: "ModuleRegistry") -> None:
    """Register Local Data Management tools on the shared MCP gateway."""

    client = registry.client("local-data")

    @registry.mcp.tool(name="local_data_health_overview")
    def local_data_health_overview() -> Any:
        """Return a summarized local-data health overview."""

        return client.get("/overview")

    @registry.mcp.tool(name="local_data_get_dataset_status")
    def local_data_get_dataset_status(dataset: str) -> Any:
        """Return status, readiness, physical range, alerts, and last job for one dataset."""

        safe_dataset = _fragment(registry, dataset, "dataset")
        return client.get(f"/datasets/{safe_dataset}/status")

    @registry.mcp.tool(name="local_data_list_data_stats")
    def local_data_list_data_stats(
        dataset: str | None = None,
        source: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Any:
        """List dashboard data_stats records through the local-data facade."""

        return client.get(
            "/data-stats",
            params={"dataset": dataset, "source": source, "status": status, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="local_data_check_gaps")
    def local_data_check_gaps(
        dataset: str | None = None,
        severity: str | None = None,
        include_resolved: bool = False,
    ) -> Any:
        """Return local-data gap summaries without starting repair work."""

        safe_dataset = _optional_fragment(registry, dataset, "dataset")
        return client.get(
            "/gaps",
            params={"data_kind": safe_dataset, "severity": severity, "include_resolved": include_resolved},
        )

    @registry.mcp.tool(name="local_data_compute_auto_range")
    def local_data_compute_auto_range(
        dataset: str | None = None,
        mode: str | None = None,
        reference_date: str | None = None,
    ) -> Any:
        """Return auto-fill date range recommendations."""

        safe_dataset = _optional_fragment(registry, dataset, "dataset")
        return client.get(
            "/auto-range",
            params={"data_kind": safe_dataset, "mode": mode, "reference_date": reference_date},
        )

    @registry.mcp.tool(name="local_data_list_alerts")
    def local_data_list_alerts(
        status: str | None = "active",
        severity: str | None = None,
        dataset: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Any:
        """List local-data alerts with summarized severity and impact."""

        return client.get(
            "/alerts",
            params={"status": status, "severity": severity, "dataset": dataset, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="local_data_get_unack_alert_count")
    def local_data_get_unack_alert_count() -> Any:
        """Return the unacknowledged alert count."""

        return client.get("/alerts/unack-count")

    @registry.mcp.tool(name="local_data_list_sync_targets")
    def local_data_list_sync_targets(
        status: str | None = None,
        dataset: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Any:
        """List data_sync_targets through the local-data facade."""

        return client.get(
            "/targets",
            params={"status": status, "dataset": dataset, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="local_data_get_sync_target")
    def local_data_get_sync_target(target_id: str | int) -> Any:
        """Return one data_sync_target detail and attempt summary."""

        safe_target_id = _fragment(registry, target_id, "target_id")
        return client.get(f"/targets/{safe_target_id}")

    @registry.mcp.tool(name="local_data_list_sync_attempts")
    def local_data_list_sync_attempts(
        target_id: str | int | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Any:
        """List data_sync_attempts timeline rows."""

        safe_target_id = _optional_fragment(registry, target_id, "target_id")
        return client.get(
            "/sync-attempts",
            params={"target_id": safe_target_id, "status": status, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="local_data_list_jobs")
    def local_data_list_jobs(
        status: str | None = None,
        dataset: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Any:
        """List recent, running, and failed local-data jobs."""

        return client.get(
            "/jobs",
            params={"status": status, "dataset": dataset, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="local_data_get_job")
    def local_data_get_job(job_id: str | int) -> Any:
        """Return one local-data job detail."""

        safe_job_id = _fragment(registry, job_id, "job_id")
        return client.get(f"/jobs/{safe_job_id}")

    @registry.mcp.tool(name="local_data_get_job_logs")
    def local_data_get_job_logs(
        job_id: str | int,
        level: str | None = None,
        limit: int = 200,
        summary_only: bool = True,
    ) -> Any:
        """Return summarized job logs and key errors."""

        safe_job_id = _fragment(registry, job_id, "job_id")
        return client.get(
            f"/jobs/{safe_job_id}/logs",
            params={"level": level, "limit": limit, "summary_only": summary_only},
        )

    @registry.mcp.tool(name="local_data_cancel_job_confirmed")
    def local_data_cancel_job_confirmed(
        job_id: str | int,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Cancel a running local-data job after explicit confirmation."""

        safe_job_id = _fragment(registry, job_id, "job_id")
        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_CANCEL_JOB_CONFIRM, payload=payload, field="confirm_change")
        return client.post(f"/jobs/{safe_job_id}/cancel", body)

    @registry.mcp.tool(name="local_data_clear_queued_jobs_confirmed")
    def local_data_clear_queued_jobs_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Clear queued local-data jobs after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_CLEAR_QUEUED_JOBS_CONFIRM, payload=payload, field="confirm_delete")
        return client.delete("/jobs/queued", body)

    @registry.mcp.tool(name="local_data_delete_job_confirmed")
    def local_data_delete_job_confirmed(
        job_id: str | int,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Delete a no-longer-useful historical job after explicit confirmation."""

        safe_job_id = _fragment(registry, job_id, "job_id")
        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_DELETE_JOB_CONFIRM, payload=payload, field="confirm_delete")
        return client.delete(f"/jobs/{safe_job_id}", body)

    @registry.mcp.tool(name="local_data_run_dataset_sync_confirmed")
    def local_data_run_dataset_sync_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Create a dataset sync job after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_RUN_DATASET_SYNC_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/run", body)

    @registry.mcp.tool(name="local_data_run_incremental_confirmed")
    def local_data_run_incremental_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Create an incremental sync job after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_RUN_INCREMENTAL_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/incremental", body)

    @registry.mcp.tool(name="local_data_run_init_confirmed")
    def local_data_run_init_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Create an init job after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_RUN_INIT_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/init", body)

    @registry.mcp.tool(name="local_data_run_schedule_confirmed")
    def local_data_run_schedule_confirmed(
        schedule_id: str | int,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Run a schedule immediately after explicit confirmation."""

        safe_schedule_id = _fragment(registry, schedule_id, "schedule_id")
        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_RUN_SCHEDULE_CONFIRM, payload=payload, field="confirm_run")
        return client.post(f"/schedules/{safe_schedule_id}/run", body)

    @registry.mcp.tool(name="local_data_run_single_preset_confirmed")
    def local_data_run_single_preset_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Run one preset schedule after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_RUN_SINGLE_PRESET_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/schedules/run-single-preset", body)

    @registry.mcp.tool(name="local_data_run_all_presets_confirmed")
    def local_data_run_all_presets_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Run all preset schedules after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_RUN_ALL_PRESETS_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/schedules/run-all-presets", body)

    @registry.mcp.tool(name="local_data_refresh_stats_confirmed")
    def local_data_refresh_stats_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Refresh data_stats cache after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_REFRESH_STATS_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/stats/refresh", body)

    @registry.mcp.tool(name="local_data_sync_calendar_confirmed")
    def local_data_sync_calendar_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Sync the trading calendar after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_SYNC_CALENDAR_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/calendar/sync", body)

    @registry.mcp.tool(name="local_data_build_sector_data_confirmed")
    def local_data_build_sector_data_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Build sector data after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_BUILD_SECTOR_DATA_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/sector-data/build", body)

    @registry.mcp.tool(name="local_data_export_sector_data_confirmed")
    def local_data_export_sector_data_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Export sector data after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_EXPORT_SECTOR_DATA_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/sector-data/export", body)

    @registry.mcp.tool(name="local_data_sync_tushare_all_confirmed")
    def local_data_sync_tushare_all_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Run bulk Tushare sync after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_SYNC_TUSHARE_ALL_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/tushare/sync-all", body)

    @registry.mcp.tool(name="local_data_list_schedules")
    def local_data_list_schedules(
        enabled: bool | None = None,
        dataset: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Any:
        """List local-data ingestion schedules."""

        return client.get(
            "/schedules",
            params={"enabled": enabled, "dataset": dataset, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="local_data_get_schedule_defaults")
    def local_data_get_schedule_defaults() -> Any:
        """Return recommended default local-data schedule templates."""

        return client.get("/schedules/defaults")

    @registry.mcp.tool(name="local_data_upsert_schedule_confirmed")
    def local_data_upsert_schedule_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Create or update one ingestion schedule after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_UPSERT_SCHEDULE_CONFIRM, payload=payload, field="confirm_change")
        return client.post("/schedules", body)

    @registry.mcp.tool(name="local_data_batch_create_schedules_confirmed")
    def local_data_batch_create_schedules_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Batch create or update ingestion schedules after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_BATCH_CREATE_SCHEDULES_CONFIRM, payload=payload, field="confirm_change")
        return client.post("/schedules/batch-create", body)

    @registry.mcp.tool(name="local_data_toggle_schedule_confirmed")
    def local_data_toggle_schedule_confirmed(
        schedule_id: str | int,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Enable or disable one schedule after explicit confirmation."""

        safe_schedule_id = _fragment(registry, schedule_id, "schedule_id")
        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_TOGGLE_SCHEDULE_CONFIRM, payload=payload, field="confirm_change")
        return client.post(f"/schedules/{safe_schedule_id}/toggle", body)

    @registry.mcp.tool(name="local_data_delete_schedule_confirmed")
    def local_data_delete_schedule_confirmed(
        schedule_id: str | int,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Delete one schedule after explicit confirmation."""

        safe_schedule_id = _fragment(registry, schedule_id, "schedule_id")
        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_DELETE_SCHEDULE_CONFIRM, payload=payload, field="confirm_delete")
        return client.delete(f"/schedules/{safe_schedule_id}", body)

    @registry.mcp.tool(name="local_data_plan_schedule_reset")
    def local_data_plan_schedule_reset(payload: dict[str, Any] | None = None) -> Any:
        """Generate a schedule reset diff plan without writing changes."""

        body = _body(payload)
        return client.post("/schedules/reset-plan", {}, params={"delete_missing": body.get("delete_missing")})

    @registry.mcp.tool(name="local_data_apply_schedule_reset_confirmed")
    def local_data_apply_schedule_reset_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Apply a schedule reset plan after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_APPLY_SCHEDULE_RESET_CONFIRM, payload=payload, field="confirm_change")
        return client.post("/schedules/reset-apply", body)

    @registry.mcp.tool(name="local_data_get_preset_stats")
    def local_data_get_preset_stats() -> Any:
        """Return preset schedule coverage statistics."""

        return client.get("/schedules/preset-stats")

    @registry.mcp.tool(name="local_data_get_preset_daily_status")
    def local_data_get_preset_daily_status(trade_date: str | None = None) -> Any:
        """Return today's preset schedule status, or status for a requested trade date."""

        return client.get("/schedules/preset-daily-status", params={"trade_date": trade_date})

    @registry.mcp.tool(name="local_data_run_source_test_confirmed")
    def local_data_run_source_test_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Run a data source test after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_RUN_SOURCE_TEST_CONFIRM, payload=payload, field="confirm_run")
        return client.post("/testing/run", body)

    @registry.mcp.tool(name="local_data_list_source_test_runs")
    def local_data_list_source_test_runs(
        status: str | None = None,
        source: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Any:
        """List data source test run history."""

        return client.get(
            "/testing/runs",
            params={"status": status, "source": source, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="local_data_list_source_test_schedules")
    def local_data_list_source_test_schedules(
        enabled: bool | None = None,
        source: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Any:
        """List data source test schedules."""

        return client.get(
            "/testing/schedules",
            params={"enabled": enabled, "source": source, "limit": limit, "offset": offset},
        )

    @registry.mcp.tool(name="local_data_upsert_source_test_schedule_confirmed")
    def local_data_upsert_source_test_schedule_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Create or update one source test schedule after explicit confirmation."""

        body = _confirmed_body(
            registry,
            confirm=confirm,
            expected=LOCAL_DATA_UPSERT_SOURCE_TEST_SCHEDULE_CONFIRM,
            payload=payload,
            field="confirm_change",
        )
        return client.post("/testing/schedules", body)

    @registry.mcp.tool(name="local_data_toggle_source_test_schedule_confirmed")
    def local_data_toggle_source_test_schedule_confirmed(
        schedule_id: str | int,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Enable or disable one source test schedule after explicit confirmation."""

        safe_schedule_id = _fragment(registry, schedule_id, "schedule_id")
        body = _confirmed_body(
            registry,
            confirm=confirm,
            expected=LOCAL_DATA_TOGGLE_SOURCE_TEST_SCHEDULE_CONFIRM,
            payload=payload,
            field="confirm_change",
        )
        return client.post(f"/testing/schedules/{safe_schedule_id}/toggle", body)

    @registry.mcp.tool(name="local_data_run_source_test_schedule_confirmed")
    def local_data_run_source_test_schedule_confirmed(
        schedule_id: str | int,
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Run one source test schedule after explicit confirmation."""

        safe_schedule_id = _fragment(registry, schedule_id, "schedule_id")
        body = _confirmed_body(
            registry,
            confirm=confirm,
            expected=LOCAL_DATA_RUN_SOURCE_TEST_SCHEDULE_CONFIRM,
            payload=payload,
            field="confirm_run",
        )
        return client.post(f"/testing/schedules/{safe_schedule_id}/run", body)

    @registry.mcp.tool(name="local_data_plan_repair")
    def local_data_plan_repair(payload: dict[str, Any] | None = None) -> Any:
        """Generate a local-data repair plan without executing writes."""

        return client.post("/repair-plan", _body(payload))

    @registry.mcp.tool(name="local_data_apply_repair_confirmed")
    def local_data_apply_repair_confirmed(
        payload: dict[str, Any] | None = None,
        confirm: str | None = None,
    ) -> Any:
        """Apply a local-data repair plan after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, expected=LOCAL_DATA_APPLY_REPAIR_CONFIRM, payload=payload, field="confirm_repair")
        return client.post("/repair-apply", body)

    @registry.mcp.tool(name="local_data_get_repair_status")
    def local_data_get_repair_status(
        repair_id: str | int | None = None,
        task_id: str | int | None = None,
    ) -> Any:
        """Return repair progress and remaining blockers."""

        safe_task_id = _optional_fragment(registry, task_id, "task_id")
        if repair_id is not None:
            safe_repair_id = _fragment(registry, repair_id, "repair_id")
            return client.get("/repair-status", params={"plan_id": safe_repair_id, "task_id": safe_task_id})
        return client.get("/repair-status", params={"task_id": safe_task_id})

    @registry.mcp.tool(name="local_data_explain_business_impact")
    def local_data_explain_business_impact(
        dataset: str | None = None,
        module: str | None = None,
    ) -> Any:
        """Explain how local-data readiness affects QE, selection, Paper v2, and stock analysis."""

        safe_dataset = _optional_fragment(registry, dataset, "dataset")
        return client.get("/business-impact", params={"dataset": safe_dataset, "module": module})

    registry.register_tool_count("local_data", TOOL_COUNT)
