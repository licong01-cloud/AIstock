import datetime as dt
import json
import uuid
from types import SimpleNamespace

import pytest

import backend.db.init_tushare_schedules as schedule_catalog_module
import backend.ingestion.tdx_scheduler as scheduler_module
from backend.db.init_tushare_schedules import _DEFAULT_SCHEDULES, _validate_default_schedules
from backend.ingestion.tdx_scheduler import TDXScheduler
from backend.services.audit_backed_data_health import AuditDatasetCheckResult


def _scheduler_with_execute_capture():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    def _execute(sql, params=()):
        calls.append((sql, params))

    scheduler._execute = _execute
    return scheduler, calls


def test_canonical_weekend_compensation_is_weekly_saturday_and_defaults_are_unique():
    _validate_default_schedules(_DEFAULT_SCHEDULES)

    weekend = next(item for item in _DEFAULT_SCHEDULES if item["dataset"] == "_weekend_compensation")
    assert weekend["frequency"] == "weekly"
    assert weekend["day_of_week"] == "saturday"
    assert weekend["at"] == "10:00"

    with pytest.raises(ValueError, match="mode-insensitive default dataset has multiple schedules"):
        _validate_default_schedules(
            [
                {"dataset": "stock_basic", "mode": "init", "frequency": "daily"},
                {"dataset": "stock_basic", "mode": "incremental", "frequency": "daily"},
            ]
        )


def test_default_schedule_catalog_rejects_mode_insensitive_duplicate(monkeypatch):
    duplicate = dict(next(item for item in _DEFAULT_SCHEDULES if item["dataset"] == "stock_basic"))
    duplicate["mode"] = "incremental"
    monkeypatch.setattr(schedule_catalog_module, "_DEFAULT_SCHEDULES", [*_DEFAULT_SCHEDULES, duplicate])

    catalog = schedule_catalog_module.get_default_schedule_catalog()

    assert catalog["complete"] is False
    assert "mode-insensitive default dataset has multiple schedules: stock_basic" in catalog["errors"]


def test_schedule_hygiene_reports_without_automatic_cleanup():
    findings = TDXScheduler._schedule_hygiene_findings(
        [
            {"schedule_id": "stock-init", "dataset": "stock_basic", "mode": "init", "frequency": "daily", "options": {}},
            {
                "schedule_id": "stock-incremental",
                "dataset": "stock_basic",
                "mode": "incremental",
                "frequency": "daily",
                "options": {},
            },
            {"schedule_id": "suspend-hourly", "dataset": "suspend_d", "mode": "incremental", "frequency": "1h", "options": {}},
            {
                "schedule_id": "suspend-fixed",
                "dataset": "_suspend_d_preopen_0905",
                "mode": "incremental",
                "frequency": "daily",
                "options": {"at": "09:05"},
            },
            {"schedule_id": "anns", "dataset": "anns_metadata", "mode": "incremental", "frequency": "1h", "options": {}},
            {
                "schedule_id": "weekend",
                "dataset": "_weekend_compensation",
                "mode": "incremental",
                "frequency": "daily",
                "options": {"at": "10:00"},
            },
        ]
    )

    by_code = {finding["code"]: finding for finding in findings}
    assert by_code["duplicate_mode_insensitive_dataset"]["action"] == "review_only_no_automatic_delete"
    assert by_code["unbounded_high_frequency_review"]["action"] == "preserve_until_availability_window_is_approved"
    assert by_code["overlapping_suspend_refresh_cadence"]["action"] == "review_only_preserve_pretrade_coverage"
    assert by_code["weekend_compensation_not_weekly"]["action"] == "align_with_canonical_saturday_schedule"


def test_non_saturday_legacy_weekend_schedule_skips_before_claim_or_job_creation():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    scheduler._weekend_compensation_due = lambda: False
    scheduler._next_run_for = lambda _schedule_id: None
    scheduler._update_ingestion_schedule = lambda *args, **kwargs: calls.append((args, kwargs))
    scheduler._claim_scheduled_fire = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("non-Saturday legacy cadence must stop before claiming a fire")
    )

    scheduler._scheduled_ingestion_run(
        "weekend-schedule",
        "_weekend_compensation",
        "incremental",
        {"at": "10:00"},
        "daily",
    )

    assert calls[0][0] == ("weekend-schedule",)
    assert calls[0][1]["last_status"] == "skipped"
    assert calls[0][1]["last_error"] == "not_saturday"


def test_success_ingestion_schedule_update_clears_previous_error():
    scheduler, calls = _scheduler_with_execute_capture()

    scheduler._update_ingestion_schedule("schedule-id", last_status="success")

    sql, params = calls[0]
    assert "last_status=%s" in sql
    assert "last_error=NULL" in sql
    assert "schedule-id" in params


def test_queued_ingestion_schedule_update_keeps_previous_error():
    scheduler, calls = _scheduler_with_execute_capture()

    scheduler._update_ingestion_schedule("schedule-id", last_status="queued")

    sql, _ = calls[0]
    assert "last_status=%s" in sql
    assert "last_error" not in sql


def test_stale_queued_reconciliation_marks_schedule_created_jobs_failed():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    seen = {}
    job_id = uuid.uuid4()
    schedule_id = uuid.uuid4()

    def _fetchall(sql, params=()):
        seen["sql"] = sql
        seen["params"] = params
        return [{"job_id": job_id, "schedule_id": str(schedule_id)}]

    def _execute(sql, params=()):
        seen["schedule_sql"] = sql
        seen["schedule_params"] = params

    scheduler._fetchall = _fetchall
    scheduler._execute = _execute

    count = scheduler._reconcile_stale_queued_ingestion_jobs(
        older_than_minutes=0,
        dataset="ANNS_METADATA",
        mode="INCREMENTAL",
        reason="unit_test",
    )

    assert count == 1
    assert "status = 'failed'" in seen["sql"]
    assert "started_at IS NULL" in seen["sql"]
    assert "triggered_by', '') = 'schedule'" in seen["sql"]
    assert "triggered_by', '') = 'data_sync_target_due'" in seen["sql"]
    assert seen["params"][2] == "120"
    assert seen["params"][3] == "anns_metadata"
    assert seen["params"][5] == "incremental"
    assert "UPDATE market.ingestion_schedules" in seen["schedule_sql"]
    assert "last_status = 'failed'" in seen["schedule_sql"]
    assert seen["schedule_params"] == ("unit_test", str(schedule_id))


def test_stale_target_queue_job_releases_target_without_deleting_history(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    job_id = uuid.uuid4()

    scheduler._fetchall = lambda _sql, _params=(): [
        {
            "job_id": job_id,
            "schedule_id": None,
            "data_sync_target_id": "target-1",
            "triggered_by": "data_sync_target_due",
        }
    ]
    scheduler._execute = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("target-created jobs must not update an unrelated schedule")
    )

    class _Repo:
        def mark_retry(self, target_id, **kwargs):
            calls.append(("retry", target_id, kwargs))

        def record_attempt(self, record):
            calls.append(("attempt", record.status, record.target_id, record.job_id))

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _Repo())

    assert scheduler._reconcile_stale_queued_ingestion_jobs() == 1
    assert calls[0][0:2] == ("retry", "target-1")
    assert calls[1] == ("attempt", "retry", "target-1", str(job_id))


def test_stale_running_reconciliation_times_out_only_expired_job_and_updates_schedule():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    seen = {}
    job_id = uuid.uuid4()
    schedule_id = uuid.uuid4()

    def _fetchall(sql, params=()):
        seen["sql"] = sql
        seen["params"] = params
        return [{"job_id": job_id, "schedule_id": str(schedule_id), "data_sync_target_id": None}]

    def _execute(sql, params=()):
        seen["schedule_sql"] = sql
        seen["schedule_params"] = params

    scheduler._fetchall = _fetchall
    scheduler._execute = _execute

    count = scheduler._reconcile_stale_running_ingestion_jobs(
        older_than_minutes=120,
        dataset="SECTOR_DATA",
        mode="INCREMENTAL",
        reason="unit_test_stale_running",
    )

    assert count == 1
    assert "status = 'timeout'" in seen["sql"]
    assert "status = 'running'" in seen["sql"]
    assert "started_at IS NOT NULL" in seen["sql"]
    assert "started_at < NOW() - (%s || ' minutes')::interval" in seen["sql"]
    assert "finished_at = NOW()" in seen["sql"]
    assert seen["params"][1:] == (
        "120",
        "sector_data",
        "sector_data",
        "incremental",
        "incremental",
    )
    summary_patch = json.loads(seen["params"][0])
    assert summary_patch["schema_version"] == "ingestion_stale_running_reconciliation_v1"
    assert summary_patch["stale_previous_status"] == "running"
    assert summary_patch["stale_running_timeout_minutes"] == 120
    assert "UPDATE market.ingestion_schedules" in seen["schedule_sql"]
    assert "last_status IN ('queued', 'running')" in seen["schedule_sql"]
    assert seen["schedule_params"] == ("unit_test_stale_running", str(schedule_id))


def test_stale_running_target_job_releases_retry_target_with_explicit_attempt(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    job_id = uuid.uuid4()
    scheduler._fetchall = lambda _sql, _params=(): [
        {
            "job_id": job_id,
            "schedule_id": None,
            "data_sync_target_id": "target-stale-running",
            "triggered_by": "data_sync_target_due",
        }
    ]
    scheduler._execute = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("target-created stale job must not update an unrelated schedule")
    )

    class _Repo:
        def mark_retry(self, target_id, **kwargs):
            calls.append(("retry", target_id, kwargs))

        def record_attempt(self, record):
            calls.append(("attempt", record))

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _Repo())

    assert scheduler._reconcile_stale_running_ingestion_jobs() == 1
    assert calls[0][0:2] == ("retry", "target-stale-running")
    attempt = calls[1][1]
    assert attempt.status == "retry"
    assert attempt.trigger_source == "stale_running_reconciliation"
    assert attempt.job_id == str(job_id)
    assert attempt.context_json["previous_status"] == "running"


def test_script_ingestion_job_started_at_uses_database_clock(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    job_id = uuid.uuid4()

    class CompletedProcess:
        returncode = 0
        stdout = ""
        stderr = ""

    def _execute(sql, params=()):
        calls.append((sql, params))

    scheduler._execute = _execute
    scheduler._extract_job_id_from_cmd = lambda _cmd: job_id
    scheduler._update_ingestion_schedule = lambda *args, **kwargs: None
    scheduler._log_ingestion_run = lambda *args, **kwargs: None
    scheduler._extract_cmd_arg = lambda *args, **kwargs: None
    scheduler._parse_cmd_date = lambda *args, **kwargs: None
    scheduler._record_refresh_audit_from_table_range = lambda *args, **kwargs: None

    monkeypatch.setattr(
        "backend.ingestion.tdx_scheduler.subprocess.run",
        lambda *args, **kwargs: CompletedProcess(),
    )

    scheduler._run_ingestion_process(
        uuid.uuid4(),
        str(uuid.uuid4()),
        "anns_metadata",
        "incremental",
        "schedule",
        ["python", "scripts/sync_anns_metadata_incremental.py", "--job-id", str(job_id)],
    )

    first_sql, first_params = calls[0]
    assert "started_at=COALESCE(started_at, NOW())" in first_sql
    assert first_params == (job_id,)



def test_refresh_schedules_reconciles_due_sync_targets():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    scheduler._reconcile_stale_running_ingestion_jobs = lambda: calls.append("stale_running")
    scheduler._reconcile_stale_queued_ingestion_jobs = lambda: calls.append("stale_queued")
    scheduler._fetchall = lambda sql, params=(): []
    scheduler._update_jobs = lambda testing, ingestion: calls.append(("jobs", list(testing), list(ingestion)))
    scheduler._reconcile_due_data_sync_targets = lambda: calls.append("due")

    scheduler.refresh_schedules()

    assert calls == ["stale_running", "stale_queued", ("jobs", [], []), "due"]


def test_recent_submission_keeps_running_job_visible_outside_recent_window():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    seen = {}

    def _fetchall(sql, params=()):
        seen["sql"] = sql
        seen["params"] = params
        return [{"job_id": uuid.uuid4()}]

    scheduler._fetchall = _fetchall

    assert scheduler._recent_dataset_submission_exists("index_daily", "incremental") is True
    assert "status IN ('queued', 'pending', 'running')" in seen["sql"]
    assert "status = 'success'" in seen["sql"]
    assert seen["params"][:2] == ("index_daily", "incremental")


def test_recent_submission_can_exclude_current_delayed_job():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    seen = {}
    job_id = uuid.uuid4()

    def _fetchall(sql, params=()):
        seen["sql"] = sql
        seen["params"] = params
        return []

    scheduler._fetchall = _fetchall

    assert scheduler._recent_dataset_submission_exists(
        "sector_data",
        "incremental",
        exclude_job_id=str(job_id),
    ) is False
    assert "job_id::text <> %s::text" in seen["sql"]
    assert seen["params"][2:4] == (str(job_id), str(job_id))


def test_due_target_already_covered_is_reconciled_without_job(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    target_date = dt.date(2026, 7, 13)
    target = {
        "target_id": "target-1",
        "dataset": "index_daily",
        "target_date": target_date,
        "attempt_count": 9,
    }

    class _Repo:
        def list_fillable_targets(self, **_kwargs):
            return [target]

        def record_attempt(self, record):
            calls.append(("attempt", record.status, record.target_id, record.trigger_source))
            return {"attempt_id": "attempt-1"}

        def claim_fillable_target(self, *_args, **_kwargs):
            raise AssertionError("covered target must not be leased")

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _Repo())
    scheduler._schedule_map_for_enabled_datasets = lambda: {
        "index_daily": {"schedule_id": "schedule-1", "mode": "incremental"}
    }
    scheduler._check_dataset_recovered = lambda dataset, expected_date=None: SimpleNamespace(
        status="ok", dataset=dataset, expected_date=expected_date
    )
    scheduler._reconcile_recovered_target_state = lambda **kwargs: calls.append(("reconcile_state", kwargs))
    scheduler._enqueue_target_retry = lambda **_kwargs: (_ for _ in ()).throw(
        AssertionError("covered target must not submit a job")
    )

    submitted = scheduler._reconcile_due_data_sync_targets()

    assert submitted == []
    assert calls[0] == (
        "reconcile_state",
        {
            "dataset": "index_daily",
            "target_id": "target-1",
            "target_date": target_date,
            "context": {"precheck": "already_recovered", "expected_date": "2026-07-13"},
        },
    )
    assert calls[1] == ("attempt", "reconciled", "target-1", "data_sync_target_precheck")


def test_sw_daily_target_uses_sw_sector_schedule_owner(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    target = {
        "target_id": "target-sw",
        "dataset": "sw_daily",
        "target_date": dt.date(2026, 7, 14),
        "attempt_count": 0,
        "metadata": {"schedule_dataset": "sw_sector"},
    }

    class _Repo:
        def list_fillable_targets(self, **_kwargs):
            return [target]

        def claim_fillable_target(self, target_id, **_kwargs):
            calls.append(("claim", target_id))
            return True

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _Repo())
    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: dt.datetime(2026, 7, 14, 8, 0, tzinfo=dt.timezone.utc),
    )
    scheduler._check_dataset_recovered = lambda dataset, expected_date=None: calls.append(
        ("check", dataset, expected_date)
    ) or SimpleNamespace(status="stale", failure_category="audit_stale")
    scheduler._target_retry_window_open = lambda dataset, **kwargs: calls.append(("window", dataset)) or True
    scheduler._recent_dataset_submission_exists = lambda dataset, mode: calls.append(("recent", dataset, mode)) or False
    scheduler._enqueue_target_retry = lambda **kwargs: calls.append(("enqueue", kwargs)) or uuid.uuid4()

    submitted = scheduler._reconcile_due_data_sync_targets(
        {"sw_sector": {"schedule_id": "schedule-sw", "mode": "incremental"}}
    )

    assert submitted == ["sw_sector"]
    assert ("check", "sw_daily", dt.date(2026, 7, 14)) in calls
    assert ("recent", "sw_sector", "incremental") in calls
    enqueue = next(call[1] for call in calls if call[0] == "enqueue")
    assert enqueue["target"] is target
    assert enqueue["execution_dataset"] == "sw_sector"


def test_due_target_without_schedule_owner_records_retry_state(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    class _Repo:
        def list_fillable_targets(self, **_kwargs):
            return [
                {
                    "target_id": "target-orphan",
                    "dataset": "orphan_dataset",
                    "target_date": dt.date(2026, 7, 14),
                }
            ]

        def mark_retry(self, target_id, **kwargs):
            calls.append(("retry", target_id, kwargs))

        def record_attempt(self, record):
            calls.append(("attempt", record.status, record.error_message))

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _Repo())
    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: dt.datetime(2026, 7, 14, 8, 0, tzinfo=dt.timezone.utc),
    )
    scheduler._check_dataset_recovered = lambda *_args, **_kwargs: SimpleNamespace(status="stale")
    scheduler._enqueue_target_retry = lambda **_kwargs: (_ for _ in ()).throw(
        AssertionError("a target without an owner must not submit")
    )

    assert scheduler._reconcile_due_data_sync_targets({}) == []
    assert calls[0][0:2] == ("retry", "target-orphan")
    assert calls[0][2]["context"]["schedule_dataset"] == "orphan_dataset"
    assert calls[1] == ("attempt", "retry", "schedule_owner_missing")


def test_index_daily_target_retry_waits_until_post_close(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: dt.datetime(2026, 7, 14, 8, 59, tzinfo=dt.timezone.utc),
    )

    assert scheduler._target_retry_window_open("index_daily") is False

    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: dt.datetime(2026, 7, 14, 9, 0, tzinfo=dt.timezone.utc),
    )
    assert scheduler._target_retry_window_open("index_daily") is True


def test_expired_due_target_is_final_blocked_without_job(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    target_date = dt.date(2026, 7, 13)
    deadline = dt.datetime(2026, 7, 13, 15, 30, tzinfo=dt.timezone.utc)

    class _Repo:
        def list_fillable_targets(self, **_kwargs):
            return [
                {
                    "target_id": "target-1",
                    "dataset": "index_daily",
                    "target_date": target_date,
                    "required_before": deadline,
                    "attempt_count": 9,
                }
            ]

        def mark_final_blocked(self, target_id, **kwargs):
            calls.append(("final", target_id, kwargs))

        def record_attempt(self, record):
            calls.append(("attempt", record.status, record.target_id, record.trigger_source))
            return {"attempt_id": "attempt-1"}

        def claim_fillable_target(self, *_args, **_kwargs):
            raise AssertionError("expired target must not be leased")

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _Repo())
    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: dt.datetime(2026, 7, 14, 7, 0, tzinfo=dt.timezone.utc),
    )
    scheduler._schedule_map_for_enabled_datasets = lambda: {
        "index_daily": {"schedule_id": "schedule-1", "mode": "incremental"}
    }
    scheduler._check_dataset_recovered = lambda _dataset, _expected=None: SimpleNamespace(
        status="stale", failure_category="audit_stale"
    )
    scheduler._flush_final_data_sync_alerts = lambda targets: calls.append(("alert", targets))
    scheduler._enqueue_target_retry = lambda **_kwargs: (_ for _ in ()).throw(
        AssertionError("expired target must not submit a job")
    )

    submitted = scheduler._reconcile_due_data_sync_targets()

    assert submitted == []
    assert calls[0][0:2] == ("final", "target-1")
    assert calls[1] == ("attempt", "final_blocked", "target-1", "data_sync_target_precheck")
    assert calls[2][0] == "alert"


def test_enqueue_target_retry_anchors_original_date_and_isolates_schedule(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    target_date = dt.date(2026, 7, 13)
    deadline = dt.datetime(2026, 7, 13, 15, 30, tzinfo=dt.timezone.utc)

    scheduler._lock = scheduler_module.threading.RLock()
    scheduler._tracker = SimpleNamespace(is_running=lambda _key: False)
    scheduler._recent_dataset_submission_exists = lambda *_args, **_kwargs: False
    scheduler._execute = lambda sql, params=(): calls.append(("execute", sql, params))
    scheduler._submit_ingestion = lambda *args, **kwargs: calls.append(("submit", args, kwargs)) or uuid.uuid4()

    class _Repo:
        def record_attempt(self, record):
            calls.append(("attempt", record))
            return {"attempt_id": "attempt-1"}

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _Repo())

    job_id = scheduler._enqueue_target_retry(
        target={
            "target_id": "target-1",
            "dataset": "index_daily",
            "target_date": target_date,
            "required_before": deadline,
        },
        schedule={"schedule_id": "schedule-1", "mode": "incremental"},
        retry_mode="incremental",
        triggered_by="data_sync_target_due",
        attempt=10,
    )

    assert job_id is not None
    submit_args = next(call[1] for call in calls if call[0] == "submit")
    assert submit_args[0] is None
    assert submit_args[1:4] == ("index_daily", "incremental", "data_sync_target_due")
    options = submit_args[4]
    assert options["target_date"] == "2026-07-13"
    assert options["start_date"] == "2026-07-13"
    assert options["end_date"] == "2026-07-13"
    assert options["target_required_before"] == deadline.isoformat()


def test_audit_checker_uses_physical_fallback_for_stale_explicit_target_date():
    from backend.services.audit_backed_data_health import AuditBackedDataHealthChecker

    checker = AuditBackedDataHealthChecker.__new__(AuditBackedDataHealthChecker)
    checker._latest_trading_day = lambda: dt.date(2026, 7, 14)
    checker._fetch_audit_rows = lambda _dataset, _expected: (
        {
            "trade_date": dt.date(2026, 7, 10),
            "row_count": 871,
            "quality_status": "ok",
            "failure_category": "audit_stale",
        },
        None,
    )
    checker._status_from_audit = lambda **_kwargs: AuditDatasetCheckResult(
        dataset="index_daily",
        table_name="market.index_daily",
        date_column="trade_date",
        tier="light",
        max_date=dt.date(2026, 7, 10),
        expected_date=dt.date(2026, 7, 13),
        status="stale",
        failure_category="audit_stale",
    )
    checker._from_physical_fallback = lambda _dataset, expected, _elapsed: AuditDatasetCheckResult(
        dataset="index_daily",
        table_name="market.index_daily",
        date_column="trade_date",
        tier="light",
        max_date=dt.date(2026, 7, 13),
        expected_date=expected,
        status="ok",
        source="physical_fallback",
    )

    result = checker.check_dataset("index_daily", expected_date=dt.date(2026, 7, 13))

    assert result.status == "ok"
    assert result.max_date == dt.date(2026, 7, 13)
    assert result.source == "physical_fallback"


def test_finalize_data_sync_target_retry_closes_recovered_target(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    class _Future:
        def exception(self):
            return None

    class _TargetRepo:
        def record_attempt(self, record):
            calls.append(("attempt", record.status, record.target_id))
            return {"attempt_id": "attempt-1"}

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _TargetRepo())
    scheduler._target_date_for_retry = lambda _dataset, _options=None: dt.date(2026, 5, 18)
    scheduler._final_deadline_for_target = lambda _dataset, _target_date: dt.datetime(
        2026, 5, 18, 23, 30, tzinfo=dt.timezone.utc
    )
    scheduler._check_dataset_recovered = lambda _dataset, expected_date=None: SimpleNamespace(
        status="ok", expected_date=expected_date
    )
    scheduler._reconcile_recovered_target_state = lambda **kwargs: calls.append(("reconcile_state", kwargs))

    job_id = str(uuid.uuid4())
    scheduler._finalize_data_sync_target_retry(
        _Future(),
        target_id="target-1",
        dataset="cyq_perf",
        mode="incremental",
        options={"job_id": job_id, "triggered_by": "unit"},
    )

    assert calls[0] == (
        "reconcile_state",
        {
            "dataset": "cyq_perf",
            "target_id": "target-1",
            "target_date": dt.date(2026, 5, 18),
            "context": {
                "mode": "incremental",
                "job_id": job_id,
                "triggered_by": "unit",
                "finalizer": "data_sync_target_retry",
                "health_dataset": "cyq_perf",
                "schedule_dataset": "cyq_perf",
            },
        },
    )
    assert calls[1] == ("attempt", "reconciled", "target-1")


def test_recovered_target_reconciles_readiness_family_and_matching_retry_alerts(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    executions = []
    transaction_args = []

    class _Cursor:
        rowcount = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=()):
            executions.append((sql, params))
            if "UPDATE market.data_sync_targets" in sql:
                self.rowcount = 2
            elif "UPDATE market.data_alerts" in sql:
                self.rowcount = 2
            else:
                self.rowcount = 0

        def fetchall(self):
            return [
                {"target_id": "target-original"},
                {"target_id": "target-delayed"},
                {"target_id": "target-auto"},
            ]

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return _Cursor()

    def _get_conn(**kwargs):
        transaction_args.append(kwargs)
        return _Connection()

    monkeypatch.setattr(scheduler_module, "get_conn", _get_conn)

    result = scheduler._reconcile_recovered_target_state(
        dataset="sector_data",
        target_id="target-original",
        target_date=dt.date(2026, 7, 15),
        context={"precheck": "already_recovered"},
    )

    assert transaction_args == [{"autocommit": False, "manage_transaction": True}]
    assert result == {"targets_reconciled": 2, "alerts_acknowledged": 2}
    select_sql, select_params = executions[0]
    assert "data_source = 'readiness_gate'" in select_sql
    assert select_params == (
        "target-original",
        dt.date(2026, 7, 15),
        "sector_data",
        dt.date(2026, 7, 15),
    )
    target_sql, target_params = executions[1]
    assert "blocked_at = NULL" in target_sql
    assert target_params[1] == ["target-original", "target-delayed", "target-auto"]
    alert_sql, alert_params = executions[2]
    assert "alert_type = 'retry_exhausted'" in alert_sql
    assert "details->>'target_id' = ANY(%s)" in alert_sql
    assert alert_params == (
        "sector_data",
        ["target-original", "target-delayed", "target-auto"],
    )


def test_recovered_target_state_failure_is_not_silently_ignored(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)

    class _BrokenConnection:
        def __enter__(self):
            raise RuntimeError("db unavailable")

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(scheduler_module, "get_conn", lambda **_kwargs: _BrokenConnection())

    with pytest.raises(RuntimeError, match="db unavailable"):
        scheduler._reconcile_recovered_target_state(
            dataset="sector_data",
            target_id="target-original",
            target_date=dt.date(2026, 7, 15),
        )


def test_delayed_retry_persists_target_before_timer(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    scheduler._delayed_retry_keys = set()
    calls = []

    class _Timer:
        def __init__(self, delay_seconds, fn):
            calls.append(("timer", delay_seconds))
            self.daemon = False

        def start(self):
            calls.append(("timer_start",))

    monkeypatch.setattr(scheduler_module.threading, "Timer", _Timer)
    scheduler._target_date_for_retry = lambda _dataset, _opts=None: dt.date(2026, 5, 18)
    scheduler._final_deadline_for_target = lambda _dataset, _target_date: dt.datetime(
        2026, 5, 18, 23, 30, tzinfo=dt.timezone.utc
    )
    scheduler._record_retry_target = lambda dataset, target_date, **kwargs: calls.append(
        ("target", dataset, target_date, kwargs["target_status"], kwargs["next_retry_at"], kwargs["required_before"])
    ) or "target-1"

    scheduler._schedule_delayed_retry(
        "cyq_perf",
        "incremental",
        delay_minutes=60,
        reason="success_without_data_update",
        options={"triggered_by": "unit"},
    )

    target_call = calls[0]
    assert target_call[0] == "target"
    assert target_call[1] == "cyq_perf"
    assert target_call[3] == "retry"
    assert target_call[4] is not None
    assert target_call[5] is not None
    assert ("timer", 3600) in calls


def test_delayed_retry_excludes_its_own_job_from_recent_dedupe(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    scheduler._delayed_retry_keys = set()
    calls = []
    job_id = uuid.uuid4()

    class _Timer:
        def __init__(self, _delay_seconds, fn):
            self._fn = fn
            self.daemon = False

        def start(self):
            self._fn()

    monkeypatch.setattr(scheduler_module.threading, "Timer", _Timer)
    scheduler._target_date_for_retry = lambda _dataset, _opts=None: dt.date(2026, 7, 15)
    scheduler._final_deadline_for_target = lambda _dataset, _target_date: dt.datetime(
        2026, 7, 15, 18, 0, tzinfo=dt.timezone.utc
    )
    scheduler._record_retry_target = lambda *_args, **_kwargs: "target-1"

    def _recent(dataset, mode, window_seconds=60, exclude_job_id=None):
        calls.append(("recent", dataset, mode, window_seconds, exclude_job_id))
        return False

    scheduler._recent_dataset_submission_exists = _recent
    scheduler.run_ingestion_now = lambda dataset, mode, **kwargs: calls.append(
        ("run", dataset, mode, kwargs)
    ) or uuid.uuid4()

    scheduler._schedule_delayed_retry(
        "sector_data",
        "incremental",
        delay_minutes=0,
        reason="upstream not ready",
        options={"job_id": str(job_id), "delayed_attempt": 1},
    )

    assert calls[0] == ("recent", "sector_data", "incremental", 60, str(job_id))
    assert calls[1][0:3] == ("run", "sector_data", "incremental")
    assert calls[1][3]["options"]["job_id"] == str(job_id)


def test_latest_completed_trading_day_stays_on_previous_day_before_close():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    seen = []
    scheduler._latest_trading_day = lambda as_of_date=None: seen.append(as_of_date) or as_of_date

    before_close = scheduler._latest_completed_trading_day(
        dt.datetime(2026, 7, 16, 0, 2, tzinfo=scheduler_module._CN_TZ)
    )
    after_close = scheduler._latest_completed_trading_day(
        dt.datetime(2026, 7, 16, 16, 1, tzinfo=scheduler_module._CN_TZ)
    )

    assert before_close == dt.date(2026, 7, 15)
    assert after_close == dt.date(2026, 7, 16)
    assert seen == [dt.date(2026, 7, 15), dt.date(2026, 7, 16)]


def test_final_deadline_uses_china_local_time():
    scheduler = TDXScheduler.__new__(TDXScheduler)

    deadline = scheduler._final_deadline_for_target("cyq_perf", dt.date(2026, 5, 18))

    assert deadline == dt.datetime(2026, 5, 18, 15, 30, tzinfo=dt.timezone.utc)


def test_sector_data_recovery_deadline_extends_to_next_day_0200_china_time():
    scheduler = TDXScheduler.__new__(TDXScheduler)

    deadline = scheduler._final_deadline_for_target("sector_data", dt.date(2026, 7, 15))

    assert deadline == dt.datetime(2026, 7, 15, 18, 0, tzinfo=dt.timezone.utc)


def test_sector_data_upstream_delay_finalizes_job_as_delayed(monkeypatch):
    import backend.services.sector_data_builder as sector_builder_module

    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    job_id = uuid.uuid4()
    max_dates = {
        "market.sector_data": dt.date(2026, 7, 14),
        "market.sw_daily": dt.date(2026, 7, 14),
        "market.moneyflow_ts": dt.date(2026, 7, 15),
    }

    monkeypatch.setattr(sector_builder_module, "SectorDataBuilder", lambda: object())
    scheduler._query_max_date = lambda table, _date_col: max_dates[table]
    scheduler._latest_completed_trading_day = lambda: dt.date(2026, 7, 15)
    scheduler._execute = lambda sql, params=(): calls.append(("execute", sql, params))
    scheduler._schedule_delayed_retry = lambda *args, **kwargs: calls.append(
        ("retry", args, kwargs)
    )
    scheduler._log_ingestion_run = lambda *args, **kwargs: calls.append(
        ("log", args, kwargs)
    )

    scheduler._run_sector_data_build(
        uuid.uuid4(),
        None,
        "incremental",
        "auto_retry",
        {"job_id": str(job_id), "delayed_attempt": 0},
    )

    sql_text = "\n".join(call[1] for call in calls if call[0] == "execute")
    assert "status='running', started_at=NOW(), finished_at=NULL" in sql_text
    assert "status='delayed', finished_at=NOW()" in sql_text
    assert "status='success'" not in sql_text
    retry = next(call for call in calls if call[0] == "retry")
    assert retry[1][0:2] == ("sector_data", "incremental")
    assert retry[2]["options"]["job_id"] == str(job_id)
    assert retry[2]["options"]["delayed_attempt"] == 1


def test_margin_detail_uses_previous_trading_day_and_t_plus_one_window():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    scheduler._latest_trading_day = lambda as_of_date=None: (
        dt.date(2026, 7, 14) if as_of_date is None else dt.date(2026, 7, 13)
    )
    scheduler._next_trading_day = lambda anchor_date, *, inclusive=False: (
        dt.date(2026, 7, 14) if anchor_date == dt.date(2026, 7, 13) else dt.date(2026, 7, 15)
    )

    assert scheduler._target_date_for_retry("margin_detail") == dt.date(2026, 7, 13)
    assert scheduler._final_deadline_for_target("margin_detail", dt.date(2026, 7, 13)) == dt.datetime(
        2026, 7, 14, 15, 30, tzinfo=dt.timezone.utc
    )
    assert scheduler._target_retry_window_open(
        "margin_detail",
        target_date=dt.date(2026, 7, 13),
        now=dt.datetime(2026, 7, 14, 11, 9, tzinfo=dt.timezone.utc),
    ) is False
    assert scheduler._target_retry_window_open(
        "margin_detail",
        target_date=dt.date(2026, 7, 13),
        now=dt.datetime(2026, 7, 14, 11, 10, tzinfo=dt.timezone.utc),
    ) is True


def test_margin_detail_auto_range_stops_at_previous_trading_day():
    scheduler = TDXScheduler.__new__(TDXScheduler)

    def _fetchall(sql, _params=()):
        if "FROM market.data_stats_config" in sql:
            return [{"table_name": "market.margin_detail", "date_column": "trade_date", "extra_info": {}}]
        if "MAX(trade_date)" in sql:
            return [{"mx": dt.date(2026, 7, 10)}]
        return []

    scheduler._fetchall = _fetchall
    scheduler._latest_trading_day = lambda as_of_date=None: (
        dt.date(2026, 7, 14) if as_of_date is None else dt.date(2026, 7, 13)
    )
    scheduler._next_trading_day = lambda _anchor_date, *, inclusive=False: dt.date(2026, 7, 13)

    assert scheduler._compute_auto_range("margin_detail") == (
        dt.date(2026, 7, 13),
        dt.date(2026, 7, 13),
    )


def test_freshness_target_persists_schedule_owner_retry_time_and_t_plus_one_deadline(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    captured = []

    class _Repo:
        def upsert_target(self, record):
            captured.append(record)
            return {"target_id": "target-margin"}

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _Repo())
    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: dt.datetime(2026, 7, 14, 10, 0, tzinfo=dt.timezone.utc),
    )
    scheduler._latest_trading_day = lambda as_of_date=None: (
        dt.date(2026, 7, 14) if as_of_date is None else dt.date(2026, 7, 13)
    )
    scheduler._next_trading_day = lambda _anchor_date, *, inclusive=False: dt.date(2026, 7, 14)
    result = SimpleNamespace(
        dataset="margin_detail",
        status="stale",
        expected_date=dt.date(2026, 7, 14),
        failure_category="audit_stale",
        summary=lambda: {"status": "stale"},
    )

    assert scheduler._record_freshness_retry_targets([result]) == ["target-margin"]

    record = captured[0]
    assert record.dataset == "margin_detail"
    assert record.target_date == dt.date(2026, 7, 13)
    assert record.next_retry_at == dt.datetime(2026, 7, 14, 11, 0, tzinfo=dt.timezone.utc)
    assert record.required_before == dt.datetime(2026, 7, 14, 15, 30, tzinfo=dt.timezone.utc)
    assert record.metadata["schedule_dataset"] == "margin_detail"
    assert record.metadata["raw_expected_date"] == "2026-07-14"


def test_auto_retry_exhaustion_before_final_deadline_marks_retry_not_alert(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    class _Repo:
        def mark_retry(self, target_id, **kwargs):
            calls.append(("retry", target_id, kwargs))

        def mark_final_blocked(self, target_id, **kwargs):
            calls.append(("final", target_id, kwargs))

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _Repo())
    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: dt.datetime(2026, 5, 18, 12, 0, tzinfo=dt.timezone.utc),
    )
    scheduler._target_date_for_retry = lambda _dataset: dt.date(2026, 5, 18)
    scheduler._final_deadline_for_target = lambda _dataset, _target_date: dt.datetime(
        2026, 5, 18, 15, 30, tzinfo=dt.timezone.utc
    )
    scheduler._flush_final_data_sync_alerts = lambda targets: calls.append(("alert", targets))
    scheduler._schedule_delayed_retry = lambda *args, **kwargs: calls.append(("delayed", args, kwargs))
    scheduler._recent_dataset_submission_exists = lambda *_args, **_kwargs: False
    scheduler._compute_auto_range = lambda _dataset: (dt.date(2026, 5, 18), dt.date(2026, 5, 18))
    scheduler._submit_ingestion = lambda *args, **kwargs: calls.append(("submit", args, kwargs))
    scheduler._check_dataset_recovered = lambda _dataset: SimpleNamespace(
        status="stale",
        coverage_pct=0,
    )
    scheduler._job_update_outcome = lambda _job_id: {"status": "failed", "inserted_rows": 0}
    scheduler._fetchall = lambda sql, params=(): []
    scheduler._schedule_map_for_enabled_datasets = lambda: {
        "cyq_perf": {"schedule_id": "schedule-1", "mode": "incremental"}
    }
    scheduler._record_retry_target = lambda *args, **kwargs: "target-1"
    scheduler._execute = lambda sql, params=(): None
    scheduler._tracker = SimpleNamespace(get_future=lambda key: None)

    scheduler._db_cfg = {}

    def _fake_check_datasets(_datasets):
        return [
            SimpleNamespace(
                dataset="cyq_perf",
                status="stale",
                is_fresh=False,
                max_date=dt.date(2026, 5, 17),
                failure_category="audit_missing",
                source_dataset=None,
            )
        ]

    class _Checker:
        def __init__(self, _db_cfg):
            pass

        def check_all(self):
            result = _fake_check_datasets(["cyq_perf"])[0]
            result.expected_date = dt.date(2026, 5, 18)
            result.coverage_pct = 0
            result.gaps = []
            return [result]

        def check_datasets(self, datasets):
            return _fake_check_datasets(datasets)

    monkeypatch.setattr(scheduler_module, "AuditBackedDataHealthChecker", _Checker)
    monkeypatch.setattr(scheduler_module.time, "sleep", lambda _seconds: None)

    scheduler._run_auto_retry_stale(
        run_id=uuid.uuid4(),
        schedule_id=None,
        triggered_by="unit",
        options={"datasets": ["cyq_perf"]},
    )

    assert any(call[0] == "retry" for call in calls)
    assert all(call[1][0] is None for call in calls if call[0] == "submit")
    assert not any(call[0] == "final" for call in calls)
    assert not any(call[0] == "alert" for call in calls)


def test_auto_retry_recovery_reconciles_target_family_before_reporting_success(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: dt.datetime(2026, 5, 18, 12, 0, tzinfo=dt.timezone.utc),
    )
    scheduler._target_date_for_retry = lambda _dataset: dt.date(2026, 5, 18)
    scheduler._recent_dataset_submission_exists = lambda *_args, **_kwargs: False
    scheduler._compute_auto_range = lambda _dataset: (dt.date(2026, 5, 18), dt.date(2026, 5, 18))
    scheduler._submit_ingestion = lambda *args, **kwargs: calls.append(("submit", args, kwargs))
    scheduler._check_dataset_recovered = lambda _dataset: SimpleNamespace(status="ok", coverage_pct=100)
    scheduler._fetchall = lambda sql, params=(): []
    scheduler._schedule_map_for_enabled_datasets = lambda: {
        "cyq_perf": {"schedule_id": "schedule-1", "mode": "incremental"}
    }
    scheduler._record_retry_target = lambda *args, **kwargs: "target-1"
    scheduler._reconcile_recovered_target_state = lambda **kwargs: calls.append(("reconcile_state", kwargs))
    scheduler._flush_final_data_sync_alerts = lambda targets: calls.append(("alert", targets))
    scheduler._execute = lambda sql, params=(): None
    scheduler._tracker = SimpleNamespace(get_future=lambda key: None)
    scheduler._db_cfg = {}

    class _Checker:
        def __init__(self, _db_cfg):
            pass

        def check_all(self):
            return [
                SimpleNamespace(
                    dataset="cyq_perf",
                    status="stale",
                    is_fresh=False,
                    max_date=dt.date(2026, 5, 17),
                    expected_date=dt.date(2026, 5, 18),
                    coverage_pct=0,
                    gaps=[],
                    failure_category="audit_missing",
                    source_dataset=None,
                )
            ]

    monkeypatch.setattr(scheduler_module, "AuditBackedDataHealthChecker", _Checker)
    monkeypatch.setattr(scheduler_module.time, "sleep", lambda _seconds: None)

    scheduler._run_auto_retry_stale(
        run_id=uuid.uuid4(),
        schedule_id=None,
        triggered_by="unit",
        options={"datasets": ["cyq_perf"]},
    )

    reconcile = next(call[1] for call in calls if call[0] == "reconcile_state")
    assert reconcile == {
        "dataset": "cyq_perf",
        "target_id": "target-1",
        "target_date": dt.date(2026, 5, 18),
        "context": {"triggered_by": "auto_retry", "attempt": 1},
    }
    assert not any(call[0] == "alert" for call in calls)


def test_auto_range_uses_calendar_service_for_trade_date_dataset():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    def _fetchall(sql, params=()):
        calls.append((sql, params))
        if "FROM market.data_stats_config" in sql:
            return [
                {
                    "table_name": "market.stk_limit",
                    "date_column": "trade_date",
                    "extra_info": {},
                }
            ]
        if "MAX(trade_date)" in sql:
            return [{"mx": dt.date(2026, 5, 22)}]
        return []

    scheduler._fetchall = _fetchall
    scheduler._latest_trading_day = lambda as_of_date=None: dt.date(2026, 5, 25)
    scheduler._next_trading_day = lambda anchor_date, *, inclusive=False: dt.date(2026, 5, 25)

    assert scheduler._compute_auto_range("stk_limit") == (dt.date(2026, 5, 25), dt.date(2026, 5, 25))
    assert not any("market.trading_calendar" in sql for sql, _params in calls)


def test_auto_range_calendar_dataset_keeps_natural_day_progression():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    def _fetchall(sql, params=()):
        calls.append((sql, params))
        if "FROM market.data_stats_config" in sql:
            return [
                {
                    "table_name": "market.stock_st_events",
                    "date_column": "pub_date",
                    "extra_info": {"date_sequence": "calendar"},
                }
            ]
        if "MAX(pub_date)" in sql:
            return [{"mx": dt.date.today() - dt.timedelta(days=1)}]
        return []

    scheduler._fetchall = _fetchall
    scheduler._latest_trading_day = lambda as_of_date=None: (_ for _ in ()).throw(AssertionError("trading service unused"))
    scheduler._next_trading_day = lambda anchor_date, *, inclusive=False: (_ for _ in ()).throw(AssertionError("next service unused"))

    start, end = scheduler._compute_auto_range("stock_st_events")

    assert start == dt.date.today()
    assert end == dt.date.today()
    assert not any("market.trading_calendar" in sql for sql, _params in calls)


def test_suspend_d_refresh_range_uses_calendar_service_without_direct_sql():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    scheduler._fetchall = lambda sql, params=(): (_ for _ in ()).throw(AssertionError(sql))
    scheduler._is_trading_day = lambda day: False
    scheduler._next_trading_day = lambda anchor_date, *, inclusive=False: dt.date(2026, 5, 25)

    assert scheduler._resolve_suspend_d_refresh_range(
        "current_or_next_trading_day",
        today=dt.date(2026, 5, 24),
    ) == (dt.date(2026, 5, 25), dt.date(2026, 5, 25))


def test_suspend_d_current_trading_day_rejects_non_trading_day():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    scheduler._is_trading_day = lambda day: False

    try:
        scheduler._resolve_suspend_d_refresh_range("current_trading_day", today=dt.date(2026, 5, 24))
    except RuntimeError as exc:
        assert "non-trading day" in str(exc)
    else:
        raise AssertionError("expected current_trading_day to reject non-trading day")
