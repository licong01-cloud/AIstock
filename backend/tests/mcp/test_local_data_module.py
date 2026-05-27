"""Contract tests for the Local Data Management MCP module."""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from backend.mcp.modules import local_data
from backend.mcp.registry import ModuleRegistry


class FakeMCP:
    def __init__(self) -> None:
        self.tools: dict[str, Callable[..., Any]] = {}

    def tool(self, name: str | None = None, **_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.tools[name or func.__name__] = func
            return func

        return decorator


def _decode_json_body(request: httpx.Request) -> dict[str, Any]:
    if not request.content:
        return {}
    return json.loads(request.content.decode("utf-8"))


def _registry_with_capture() -> tuple[ModuleRegistry, FakeMCP, list[dict[str, Any]]]:
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call = {
            "method": request.method,
            "path": request.url.path,
            "query": dict(request.url.params),
            "body": _decode_json_body(request),
        }
        calls.append(call)
        return httpx.Response(
            200,
            json={
                "summary": "local-data facade called",
                "status": "ok",
                "business_impact": [],
                "next_actions": [],
                "trace_refs": {"path": call["path"]},
                "raw_ref": {"call_index": len(calls)},
            },
        )

    mcp = FakeMCP()
    registry = ModuleRegistry(
        mcp=mcp,
        base_url="http://127.0.0.1:8011/api/v1",
        env_name="test",
        transport=httpx.MockTransport(handler),
    )
    local_data.register(registry)
    return registry, mcp, calls


def test_local_data_module_registers_design_tool_catalog() -> None:
    registry, mcp, _calls = _registry_with_capture()

    assert local_data.TOOL_COUNT == 47
    assert registry.tool_count("local_data") == local_data.TOOL_COUNT
    assert registry.total_tool_count() == local_data.TOOL_COUNT
    assert set(mcp.tools) == set(local_data.TOOL_NAMES)
    assert not {name for name in mcp.tools if "factor" in name or "xtquant" in name or "miniqmt" in name or "paper" in name}


def test_local_data_tools_call_only_local_data_facade() -> None:
    _registry, mcp, calls = _registry_with_capture()
    tools = mcp.tools

    tools["local_data_health_overview"]()
    tools["local_data_get_dataset_status"]("stock_daily")
    tools["local_data_list_data_stats"](dataset="stock_daily", status="stale", limit=2, offset=1)
    tools["local_data_check_gaps"](dataset="stock_daily", severity="high")
    tools["local_data_compute_auto_range"](dataset="stock_daily", mode="incremental", reference_date="2026-05-22")
    tools["local_data_list_alerts"](status="active", severity="high", dataset="stock_daily", limit=3, offset=1)
    tools["local_data_get_unack_alert_count"]()
    tools["local_data_list_sync_targets"](status="pending", dataset="stock_daily", limit=4, offset=2)
    tools["local_data_get_sync_target"](42)
    tools["local_data_list_sync_attempts"](target_id=42, status="failed", limit=5, offset=1)
    tools["local_data_list_jobs"](status="failed", dataset="stock_daily", limit=6, offset=2)
    tools["local_data_get_job"]("job_1")
    tools["local_data_get_job_logs"]("job_1", level="ERROR", limit=5, summary_only=True)
    tools["local_data_cancel_job_confirmed"](
        "job_1",
        payload={"reason": "operator requested"},
        confirm=local_data.LOCAL_DATA_CANCEL_JOB_CONFIRM,
    )
    tools["local_data_clear_queued_jobs_confirmed"](confirm=local_data.LOCAL_DATA_CLEAR_QUEUED_JOBS_CONFIRM)
    tools["local_data_delete_job_confirmed"]("job_2", confirm=local_data.LOCAL_DATA_DELETE_JOB_CONFIRM)
    tools["local_data_run_dataset_sync_confirmed"](
        {"dataset": "stock_daily", "confirm": "WRONG"},
        confirm=local_data.LOCAL_DATA_RUN_DATASET_SYNC_CONFIRM,
    )
    tools["local_data_run_incremental_confirmed"](
        {"dataset": "stock_daily"},
        confirm=local_data.LOCAL_DATA_RUN_INCREMENTAL_CONFIRM,
    )
    tools["local_data_run_init_confirmed"]({"dataset": "stock_daily"}, confirm=local_data.LOCAL_DATA_RUN_INIT_CONFIRM)
    tools["local_data_run_schedule_confirmed"](7, {"reason": "manual"}, confirm=local_data.LOCAL_DATA_RUN_SCHEDULE_CONFIRM)
    tools["local_data_run_single_preset_confirmed"]({"preset": "daily"}, confirm=local_data.LOCAL_DATA_RUN_SINGLE_PRESET_CONFIRM)
    tools["local_data_run_all_presets_confirmed"]({"scope": "safe-defaults"}, confirm=local_data.LOCAL_DATA_RUN_ALL_PRESETS_CONFIRM)
    tools["local_data_refresh_stats_confirmed"]({"datasets": ["stock_daily"]}, confirm=local_data.LOCAL_DATA_REFRESH_STATS_CONFIRM)
    tools["local_data_sync_calendar_confirmed"]({"start_date": "2026-01-01"}, confirm=local_data.LOCAL_DATA_SYNC_CALENDAR_CONFIRM)
    tools["local_data_build_sector_data_confirmed"]({"taxonomy": "sw"}, confirm=local_data.LOCAL_DATA_BUILD_SECTOR_DATA_CONFIRM)
    tools["local_data_export_sector_data_confirmed"]({"format": "parquet"}, confirm=local_data.LOCAL_DATA_EXPORT_SECTOR_DATA_CONFIRM)
    tools["local_data_sync_tushare_all_confirmed"]({"scope": "announcements"}, confirm=local_data.LOCAL_DATA_SYNC_TUSHARE_ALL_CONFIRM)
    tools["local_data_list_schedules"](enabled=True, dataset="stock_daily", limit=8, offset=2)
    tools["local_data_get_schedule_defaults"]()
    tools["local_data_upsert_schedule_confirmed"]({"name": "daily"}, confirm=local_data.LOCAL_DATA_UPSERT_SCHEDULE_CONFIRM)
    tools["local_data_batch_create_schedules_confirmed"]({"items": []}, confirm=local_data.LOCAL_DATA_BATCH_CREATE_SCHEDULES_CONFIRM)
    tools["local_data_toggle_schedule_confirmed"]("sched_1", {"enabled": False}, confirm=local_data.LOCAL_DATA_TOGGLE_SCHEDULE_CONFIRM)
    tools["local_data_delete_schedule_confirmed"]("sched_2", confirm=local_data.LOCAL_DATA_DELETE_SCHEDULE_CONFIRM)
    tools["local_data_plan_schedule_reset"]({"mode": "default"})
    tools["local_data_apply_schedule_reset_confirmed"]({"plan_id": "plan_1"}, confirm=local_data.LOCAL_DATA_APPLY_SCHEDULE_RESET_CONFIRM)
    tools["local_data_get_preset_stats"]()
    tools["local_data_get_preset_daily_status"]("2026-05-22")
    tools["local_data_run_source_test_confirmed"]({"source": "tushare"}, confirm=local_data.LOCAL_DATA_RUN_SOURCE_TEST_CONFIRM)
    tools["local_data_list_source_test_runs"](status="failed", source="tushare", limit=9, offset=3)
    tools["local_data_list_source_test_schedules"](enabled=False, source="tushare", limit=10, offset=4)
    tools["local_data_upsert_source_test_schedule_confirmed"](
        {"source": "tushare"},
        confirm=local_data.LOCAL_DATA_UPSERT_SOURCE_TEST_SCHEDULE_CONFIRM,
    )
    tools["local_data_toggle_source_test_schedule_confirmed"](
        8,
        {"enabled": True},
        confirm=local_data.LOCAL_DATA_TOGGLE_SOURCE_TEST_SCHEDULE_CONFIRM,
    )
    tools["local_data_run_source_test_schedule_confirmed"](8, {"reason": "manual"}, confirm=local_data.LOCAL_DATA_RUN_SOURCE_TEST_SCHEDULE_CONFIRM)
    tools["local_data_plan_repair"]({"dataset": "stock_daily"})
    tools["local_data_apply_repair_confirmed"]({"plan_id": "repair_plan_1"}, confirm=local_data.LOCAL_DATA_APPLY_REPAIR_CONFIRM)
    tools["local_data_get_repair_status"]("repair_1", task_id="task_1")
    tools["local_data_explain_business_impact"](dataset="stock_daily", module="qe")

    assert len(calls) == local_data.TOOL_COUNT
    assert all(call["path"].startswith("/api/v1/local-data/") for call in calls)
    assert [(call["method"], call["path"]) for call in calls] == [
        ("GET", "/api/v1/local-data/overview"),
        ("GET", "/api/v1/local-data/datasets/stock_daily/status"),
        ("GET", "/api/v1/local-data/data-stats"),
        ("GET", "/api/v1/local-data/gaps"),
        ("GET", "/api/v1/local-data/auto-range"),
        ("GET", "/api/v1/local-data/alerts"),
        ("GET", "/api/v1/local-data/alerts/unack-count"),
        ("GET", "/api/v1/local-data/targets"),
        ("GET", "/api/v1/local-data/targets/42"),
        ("GET", "/api/v1/local-data/sync-attempts"),
        ("GET", "/api/v1/local-data/jobs"),
        ("GET", "/api/v1/local-data/jobs/job_1"),
        ("GET", "/api/v1/local-data/jobs/job_1/logs"),
        ("POST", "/api/v1/local-data/jobs/job_1/cancel"),
        ("DELETE", "/api/v1/local-data/jobs/queued"),
        ("DELETE", "/api/v1/local-data/jobs/job_2"),
        ("POST", "/api/v1/local-data/run"),
        ("POST", "/api/v1/local-data/incremental"),
        ("POST", "/api/v1/local-data/init"),
        ("POST", "/api/v1/local-data/schedules/7/run"),
        ("POST", "/api/v1/local-data/schedules/run-single-preset"),
        ("POST", "/api/v1/local-data/schedules/run-all-presets"),
        ("POST", "/api/v1/local-data/stats/refresh"),
        ("POST", "/api/v1/local-data/calendar/sync"),
        ("POST", "/api/v1/local-data/sector-data/build"),
        ("POST", "/api/v1/local-data/sector-data/export"),
        ("POST", "/api/v1/local-data/tushare/sync-all"),
        ("GET", "/api/v1/local-data/schedules"),
        ("GET", "/api/v1/local-data/schedules/defaults"),
        ("POST", "/api/v1/local-data/schedules"),
        ("POST", "/api/v1/local-data/schedules/batch-create"),
        ("POST", "/api/v1/local-data/schedules/sched_1/toggle"),
        ("DELETE", "/api/v1/local-data/schedules/sched_2"),
        ("POST", "/api/v1/local-data/schedules/reset-plan"),
        ("POST", "/api/v1/local-data/schedules/reset-apply"),
        ("GET", "/api/v1/local-data/schedules/preset-stats"),
        ("GET", "/api/v1/local-data/schedules/preset-daily-status"),
        ("POST", "/api/v1/local-data/testing/run"),
        ("GET", "/api/v1/local-data/testing/runs"),
        ("GET", "/api/v1/local-data/testing/schedules"),
        ("POST", "/api/v1/local-data/testing/schedules"),
        ("POST", "/api/v1/local-data/testing/schedules/8/toggle"),
        ("POST", "/api/v1/local-data/testing/schedules/8/run"),
        ("POST", "/api/v1/local-data/repair-plan"),
        ("POST", "/api/v1/local-data/repair-apply"),
        ("GET", "/api/v1/local-data/repair-status"),
        ("GET", "/api/v1/local-data/business-impact"),
    ]

    assert calls[13]["body"] == {"reason": "operator requested", "confirm_change": local_data.LOCAL_DATA_CANCEL_JOB_CONFIRM}
    assert calls[16]["body"] == {"dataset": "stock_daily", "confirm_run": local_data.LOCAL_DATA_RUN_DATASET_SYNC_CONFIRM}
    assert calls[33]["body"] == {}
    assert calls[44]["body"] == {"plan_id": "repair_plan_1", "confirm_repair": local_data.LOCAL_DATA_APPLY_REPAIR_CONFIRM}
    assert calls[46]["query"] == {"dataset": "stock_daily", "module": "qe"}


@pytest.mark.parametrize(
    ("tool_name", "args", "expected"),
    [
        ("local_data_cancel_job_confirmed", ("job_1",), local_data.LOCAL_DATA_CANCEL_JOB_CONFIRM),
        ("local_data_clear_queued_jobs_confirmed", (), local_data.LOCAL_DATA_CLEAR_QUEUED_JOBS_CONFIRM),
        ("local_data_delete_job_confirmed", ("job_1",), local_data.LOCAL_DATA_DELETE_JOB_CONFIRM),
        ("local_data_run_dataset_sync_confirmed", (), local_data.LOCAL_DATA_RUN_DATASET_SYNC_CONFIRM),
        ("local_data_run_incremental_confirmed", (), local_data.LOCAL_DATA_RUN_INCREMENTAL_CONFIRM),
        ("local_data_run_init_confirmed", (), local_data.LOCAL_DATA_RUN_INIT_CONFIRM),
        ("local_data_run_schedule_confirmed", (1,), local_data.LOCAL_DATA_RUN_SCHEDULE_CONFIRM),
        ("local_data_run_single_preset_confirmed", (), local_data.LOCAL_DATA_RUN_SINGLE_PRESET_CONFIRM),
        ("local_data_run_all_presets_confirmed", (), local_data.LOCAL_DATA_RUN_ALL_PRESETS_CONFIRM),
        ("local_data_refresh_stats_confirmed", (), local_data.LOCAL_DATA_REFRESH_STATS_CONFIRM),
        ("local_data_sync_calendar_confirmed", (), local_data.LOCAL_DATA_SYNC_CALENDAR_CONFIRM),
        ("local_data_build_sector_data_confirmed", (), local_data.LOCAL_DATA_BUILD_SECTOR_DATA_CONFIRM),
        ("local_data_export_sector_data_confirmed", (), local_data.LOCAL_DATA_EXPORT_SECTOR_DATA_CONFIRM),
        ("local_data_sync_tushare_all_confirmed", (), local_data.LOCAL_DATA_SYNC_TUSHARE_ALL_CONFIRM),
        ("local_data_upsert_schedule_confirmed", (), local_data.LOCAL_DATA_UPSERT_SCHEDULE_CONFIRM),
        ("local_data_batch_create_schedules_confirmed", (), local_data.LOCAL_DATA_BATCH_CREATE_SCHEDULES_CONFIRM),
        ("local_data_toggle_schedule_confirmed", (1,), local_data.LOCAL_DATA_TOGGLE_SCHEDULE_CONFIRM),
        ("local_data_delete_schedule_confirmed", (1,), local_data.LOCAL_DATA_DELETE_SCHEDULE_CONFIRM),
        ("local_data_apply_schedule_reset_confirmed", (), local_data.LOCAL_DATA_APPLY_SCHEDULE_RESET_CONFIRM),
        ("local_data_run_source_test_confirmed", (), local_data.LOCAL_DATA_RUN_SOURCE_TEST_CONFIRM),
        ("local_data_upsert_source_test_schedule_confirmed", (), local_data.LOCAL_DATA_UPSERT_SOURCE_TEST_SCHEDULE_CONFIRM),
        ("local_data_toggle_source_test_schedule_confirmed", (1,), local_data.LOCAL_DATA_TOGGLE_SOURCE_TEST_SCHEDULE_CONFIRM),
        ("local_data_run_source_test_schedule_confirmed", (1,), local_data.LOCAL_DATA_RUN_SOURCE_TEST_SCHEDULE_CONFIRM),
        ("local_data_apply_repair_confirmed", (), local_data.LOCAL_DATA_APPLY_REPAIR_CONFIRM),
    ],
)
def test_confirmed_local_data_tools_reject_before_http(tool_name: str, args: tuple[Any, ...], expected: str) -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError, match=expected):
        mcp.tools[tool_name](*args, confirm="WRONG")

    assert calls == []


@pytest.mark.parametrize(
    ("tool_name", "args", "kwargs"),
    [
        ("local_data_get_dataset_status", ("../stock_daily",), {}),
        ("local_data_compute_auto_range", (), {"dataset": "stock/daily"}),
        ("local_data_get_sync_target", ("target/1",), {}),
        ("local_data_list_sync_attempts", (), {"target_id": "target/1"}),
        ("local_data_get_job", ("job/1",), {}),
        ("local_data_get_job_logs", ("job/1",), {}),
        ("local_data_cancel_job_confirmed", ("job/1",), {"confirm": local_data.LOCAL_DATA_CANCEL_JOB_CONFIRM}),
        ("local_data_run_schedule_confirmed", ("schedule/1",), {"confirm": local_data.LOCAL_DATA_RUN_SCHEDULE_CONFIRM}),
        ("local_data_toggle_schedule_confirmed", ("schedule/1",), {"confirm": local_data.LOCAL_DATA_TOGGLE_SCHEDULE_CONFIRM}),
        ("local_data_delete_schedule_confirmed", ("schedule/1",), {"confirm": local_data.LOCAL_DATA_DELETE_SCHEDULE_CONFIRM}),
        (
            "local_data_toggle_source_test_schedule_confirmed",
            ("schedule/1",),
            {"confirm": local_data.LOCAL_DATA_TOGGLE_SOURCE_TEST_SCHEDULE_CONFIRM},
        ),
        (
            "local_data_run_source_test_schedule_confirmed",
            ("schedule/1",),
            {"confirm": local_data.LOCAL_DATA_RUN_SOURCE_TEST_SCHEDULE_CONFIRM},
        ),
        ("local_data_get_repair_status", ("repair/1",), {}),
        ("local_data_get_repair_status", (), {"task_id": "task/1"}),
        ("local_data_explain_business_impact", (), {"dataset": "stock/daily"}),
    ],
)
def test_local_data_tools_reject_unsafe_path_fragments_before_http(
    tool_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError):
        mcp.tools[tool_name](*args, **kwargs)

    assert calls == []


def test_local_data_module_does_not_import_backend_runtime_or_db_layers() -> None:
    source = inspect.getsource(local_data)

    forbidden = [
        "backend.routers",
        "backend.services",
        "local_data_management",
        "tdx_scheduler",
        "subprocess",
        "psycopg",
        "sqlalchemy",
    ]
    assert not [token for token in forbidden if token in source]
