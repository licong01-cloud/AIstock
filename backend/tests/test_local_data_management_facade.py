from __future__ import annotations

import uuid

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.db.init_tushare_schedules import get_default_schedule_catalog
from backend.routers import ingestion, local_data
from backend.services.local_data_management import (
    LOCAL_DATA_CONFIRM_CHANGE,
    LOCAL_DATA_CONFIRM_REPAIR,
    LOCAL_DATA_CONFIRM_RUN,
    LocalDataManagementService,
)


class FakeSource:
    class IngestionRunRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class IngestionScheduleUpsertRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class BatchScheduleItem:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class BatchCreateSchedulesRequest:
        def __init__(self, items):
            self.items = items

    class ToggleRequest:
        def __init__(self, enabled):
            self.enabled = enabled

    class TestingRunRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class TestingScheduleUpsertRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class GoIncrementalRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class IngestionInitRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class RunSinglePresetRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class CalendarSyncRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.job_id = uuid.uuid4()
        self.schedule_id = uuid.uuid4()

    def list_data_stats(self):
        return {
            "items": [
                {
                    "data_kind": "daily_basic",
                    "ready_date": "2026-05-22",
                    "physical_max_date": "2026-05-21",
                    "stats_max_date": "2026-05-21",
                    "cache_state": "stale",
                }
            ]
        }

    def list_ingestion_jobs(self, limit=50, active_only=False):
        return {"items": [{"job_id": str(self.job_id), "status": "running", "meta": {"dataset": "daily_basic"}}]}

    def get_ingestion_job(self, job_id):
        return {"job_id": str(job_id), "status": "running", "meta": {"dataset": "daily_basic"}}

    def get_active_alerts(self, severity_min="warning", limit=50):
        return {"alerts": [{"alert_id": "alert_1", "severity": "warning", "title": "测试告警"}], "count": 1}

    def get_unack_alert_count(self):
        return {"count": 1}

    def get_data_gaps(self, **kwargs):
        return {"data_kind": kwargs["data_kind"], "missing_ranges": [{"start": "2026-05-21", "end": "2026-05-21"}]}

    def get_ingestion_auto_range(self, data_kind):
        return {"data_kind": data_kind, "start_date": "2026-05-22", "latest_date": "2026-05-23"}

    def list_ingestion_schedules(self):
        return {
            "items": [
                {
                    "schedule_id": str(self.schedule_id),
                    "dataset": "stock_moneyflow_ts",
                    "mode": "incremental",
                    "frequency": "weekly",
                    "enabled": False,
                    "options": {},
                }
            ]
        }

    def trigger_ingestion_run(self, payload):
        self.calls.append(("trigger_ingestion_run", payload))
        return {"job_id": "job_1", "status": "queued"}

    def upsert_ingestion_schedule(self, payload):
        self.calls.append(("upsert_ingestion_schedule", payload))
        return {"schedule_id": str(getattr(payload, "schedule_id", "") or self.schedule_id), "dataset": payload.dataset}

    def refresh_data_stats(self):
        self.calls.append(("refresh_data_stats", None))
        return {"success": True}

    def get_preset_stats(self):
        return {"items": [{"dataset": "daily_basic", "ready_date": "2026-05-22"}]}

    def get_preset_daily_status(self):
        return {"items": {"daily_basic": {"status": "success"}}}

    def export_sector_data(self, snapshot_id, start_date, end_date):
        self.calls.append(("export_sector_data", {"snapshot_id": snapshot_id, "start_date": start_date, "end_date": end_date}))
        return {"snapshot_id": snapshot_id, "start": start_date, "end": end_date}


class EmptyCursor:
    description = [
        ("target_id",),
        ("dataset",),
        ("data_source",),
        ("target_date",),
        ("target_scope",),
        ("target_status",),
        ("priority",),
        ("required_before",),
        ("next_retry_at",),
        ("expected_rows",),
        ("observed_rows",),
        ("data_max_at",),
        ("attempt_count",),
        ("last_attempt_id",),
        ("last_attempt_status",),
        ("last_error_message",),
        ("metadata",),
        ("created_at",),
        ("updated_at",),
        ("reconciled_at",),
        ("blocked_at",),
    ]

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        return []

    def fetchone(self):
        return None


class EmptyConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def cursor(self, **_kwargs):
        return EmptyCursor()


def service_with_fake_source() -> LocalDataManagementService:
    return LocalDataManagementService(connection_provider=EmptyConnection, source=FakeSource())


def test_overview_returns_human_summary_and_business_impact() -> None:
    result = service_with_fake_source().overview()

    assert result["operation"] == "local_data_health_overview"
    assert result["risk_level"] == "read_only"
    assert "本地数据" in result["summary"]
    assert "QE" in result["data"]["affected_modules"]
    assert result["data"]["stale_dataset_count"] == 1
    assert result["trace"]["source_endpoint"] == "local-data composite"


def test_overview_counts_active_jobs_and_targets_independently_from_preview_limits() -> None:
    class VisibilitySource(FakeSource):
        def list_ingestion_jobs(self, limit=50, active_only=False):
            if active_only:
                return {
                    "items": [
                        {"job_id": str(uuid.uuid4()), "status": "running", "meta": {"dataset": "daily_basic"}}
                        for _ in range(6)
                    ]
                }
            return {"items": [{"job_id": str(uuid.uuid4()), "status": "success", "meta": {"dataset": "stock_basic"}}]}

    class VisibilityService(LocalDataManagementService):
        def _fetch_overview_counts(self):  # type: ignore[override]
            return {"active_job_count": 6, "retry_target_count": 22, "blocked_target_count": 1}

    result = VisibilityService(connection_provider=EmptyConnection, source=VisibilitySource()).overview()

    assert result["data"]["running_job_count"] == 6
    assert result["data"]["retry_target_count"] == 22
    assert result["data"]["blocked_target_count"] == 1
    assert len(result["data"]["active_jobs"]) == 6
    assert result["data"]["recent_jobs"][0]["status"] == "success"


def test_dataset_status_fetches_latest_job_for_that_dataset_directly() -> None:
    latest_job_id = uuid.uuid4()

    class DatasetSource(FakeSource):
        def list_ingestion_jobs(self, limit=50, active_only=False):
            raise AssertionError("dataset status must not filter a globally bounded job list")

        def get_ingestion_job(self, job_id):
            assert job_id == latest_job_id
            return {"job_id": str(job_id), "status": "success", "meta": {"dataset": "daily_basic"}}

    class DatasetService(LocalDataManagementService):
        def _fetch_latest_job_id(self, dataset):  # type: ignore[override]
            assert dataset == "daily_basic"
            return latest_job_id

    result = DatasetService(connection_provider=EmptyConnection, source=DatasetSource()).dataset_status("daily_basic")

    assert result["data"]["last_job"]["job_id"] == str(latest_job_id)
    assert result["data"]["last_job"]["meta"]["dataset"] == "daily_basic"


def test_visibility_queries_use_aggregate_counts_and_status_aware_target_order() -> None:
    executed: list[tuple[str, object]] = []

    class RecordingCursor(EmptyCursor):
        def execute(self, sql, params=None):
            executed.append((" ".join(sql.split()), params))

        def fetchone(self):
            return {"active_job_count": 6, "retry_target_count": 22, "blocked_target_count": 1}

    class RecordingConnection(EmptyConnection):
        def cursor(self, **_kwargs):
            return RecordingCursor()

    service = LocalDataManagementService(connection_provider=RecordingConnection, source=FakeSource())

    assert service._fetch_overview_counts() == {
        "active_job_count": 6,
        "retry_target_count": 22,
        "blocked_target_count": 1,
    }
    assert service._fetch_targets(limit=3) == []

    count_sql = executed[0][0]
    target_sql = executed[1][0]
    assert "SELECT COUNT(*) FROM market.ingestion_jobs" in count_sql
    assert "SELECT COUNT(*) FROM market.data_sync_targets" in count_sql
    assert "CASE target_status" in target_sql
    assert "WHEN 'final_blocked' THEN 0" in target_sql
    assert "updated_at DESC" in target_sql


def test_confirmed_write_refuses_to_call_source_without_confirmation() -> None:
    source = FakeSource()
    service = LocalDataManagementService(connection_provider=EmptyConnection, source=source)

    with pytest.raises(HTTPException, match="confirmation required"):
        service.run_dataset_sync(dataset="daily_basic", mode="incremental", options={}, confirm_run=None)

    assert source.calls == []


def test_confirmed_run_calls_source_after_confirmation() -> None:
    source = FakeSource()
    service = LocalDataManagementService(connection_provider=EmptyConnection, source=source)

    result = service.run_dataset_sync(dataset="daily_basic", mode="incremental", options={}, confirm_run=LOCAL_DATA_CONFIRM_RUN)

    assert result["risk_level"] == "run_data_job"
    assert source.calls[0][0] == "trigger_ingestion_run"


def test_schedule_reset_plan_is_plan_only_and_does_not_write() -> None:
    source = FakeSource()
    service = LocalDataManagementService(connection_provider=EmptyConnection, source=source)

    result = service.plan_schedule_reset()

    assert result["risk_level"] == "plan_only"
    assert result["data"]["actions"]
    assert result["data"]["catalog_fingerprint"] == get_default_schedule_catalog()["fingerprint"]
    moneyflow = next(
        action["desired"]
        for action in result["data"]["actions"]
        if action["key"] == ["stock_moneyflow_ts", "incremental"]
    )
    assert moneyflow["options"]["at"] == "17:20"
    assert all(action["key"] != ["kline_weekly", "incremental"] for action in result["data"]["actions"])
    assert source.calls == []


def test_schedule_reset_refuses_destructive_delete_missing_plan() -> None:
    source = FakeSource()
    service = LocalDataManagementService(connection_provider=EmptyConnection, source=source)

    with pytest.raises(HTTPException, match="destructive schedule reset is disabled"):
        service.plan_schedule_reset(delete_missing=True)

    assert source.calls == []


def test_schedule_reset_apply_rejects_forged_or_stale_plan_without_writes() -> None:
    source = FakeSource()
    service = LocalDataManagementService(connection_provider=EmptyConnection, source=source)
    plan = service.plan_schedule_reset()["data"]
    plan["actions"][0]["desired"]["frequency"] = "1m"

    with pytest.raises(HTTPException, match="stale or does not match"):
        service.apply_schedule_reset(plan=plan, confirm_change=LOCAL_DATA_CONFIRM_CHANGE)

    with pytest.raises(HTTPException, match="destructive schedule reset actions are not supported"):
        service.apply_schedule_reset(
            plan={"plan_id": "forged", "actions": [{"action": "delete", "schedule_id": str(source.schedule_id)}]},
            confirm_change=LOCAL_DATA_CONFIRM_CHANGE,
        )

    assert source.calls == []


def test_default_schedule_catalog_is_complete_and_router_uses_it(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = get_default_schedule_catalog()
    calls: list[tuple[str, str, str, bool, dict[str, object]]] = []

    monkeypatch.setattr(
        ingestion,
        "_upsert_ingestion_schedule_entry",
        lambda dataset, mode, frequency, enabled, options: calls.append(
            (dataset, mode, frequency, enabled, options)
        )
        or {"dataset": dataset, "mode": mode},
    )
    monkeypatch.setattr(ingestion.scheduler, "refresh_schedules", lambda: None)

    items = ingestion._ensure_default_ingestion_schedules()

    assert catalog["complete"] is True
    assert len(catalog["templates"]) == 29
    assert len(items) == len(catalog["templates"])
    assert [(item[0], item[1]) for item in calls] == [
        (item["dataset"], item["mode"]) for item in catalog["templates"]
    ]


def test_schedule_reset_apply_requires_confirmation() -> None:
    service = service_with_fake_source()
    plan = service.plan_schedule_reset()["data"]

    with pytest.raises(HTTPException, match="confirmation required"):
        service.apply_schedule_reset(plan=plan, confirm_change=None)

    result = service.apply_schedule_reset(plan=plan, confirm_change=LOCAL_DATA_CONFIRM_CHANGE)
    assert result["data"]["applied"]
    assert result["data"]["post_check"]


def test_repair_apply_stops_on_first_failure_and_records_error() -> None:
    class FailingRepairService(LocalDataManagementService):
        def compute_auto_range(self, *, data_kind: str):  # type: ignore[override]
            raise RuntimeError(f"failed for {data_kind}")

    service = FailingRepairService(connection_provider=EmptyConnection, source=FakeSource())
    plan = {"plan_id": "p1", "steps": [{"step_id": "compute_auto_range", "action": "compute_auto_range", "dataset": "daily_basic"}]}

    result = service.apply_repair(plan=plan, confirm_repair=LOCAL_DATA_CONFIRM_REPAIR)

    assert result["data"]["status"] == "failed"
    assert "failed for daily_basic" in result["data"]["results"][0]["error"]


def test_router_exposes_local_data_facade_with_dependency_override() -> None:
    app = FastAPI()
    app.include_router(local_data.router, prefix="/api/v1")
    app.dependency_overrides[local_data.get_local_data_service] = service_with_fake_source

    client = TestClient(app)
    overview = client.get("/api/v1/local-data/overview")
    assert overview.status_code == 200
    assert overview.json()["operation"] == "local_data_health_overview"

    denied = client.post("/api/v1/local-data/run", json={"dataset": "daily_basic", "mode": "incremental"})
    assert denied.status_code == 400

    accepted = client.post(
        "/api/v1/local-data/run",
        json={"dataset": "daily_basic", "mode": "incremental", "confirm_run": LOCAL_DATA_CONFIRM_RUN},
    )
    assert accepted.status_code == 200
    assert accepted.json()["risk_level"] == "run_data_job"

    attempts = client.get("/api/v1/local-data/sync-attempts")
    assert attempts.status_code == 200
    assert attempts.json()["operation"] == "local_data_list_sync_attempts"

    preset_stats = client.get("/api/v1/local-data/schedules/preset-stats")
    assert preset_stats.status_code == 200
    assert preset_stats.json()["operation"] == "local_data_get_preset_stats"

    preset_daily = client.get("/api/v1/local-data/schedules/preset-daily-status")
    assert preset_daily.status_code == 200
    assert preset_daily.json()["operation"] == "local_data_get_preset_daily_status"

    export = client.post(
        "/api/v1/local-data/sector-data/export",
        json={
            "snapshot_id": "snap_test",
            "start_date": "2026-01-01",
            "end_date": "2026-01-02",
            "confirm_run": LOCAL_DATA_CONFIRM_RUN,
        },
    )
    assert export.status_code == 200
    assert export.json()["operation"] == "local_data_export_sector_data"
