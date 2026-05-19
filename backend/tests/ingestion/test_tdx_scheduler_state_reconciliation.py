import uuid
import datetime as dt
from types import SimpleNamespace

import backend.ingestion.tdx_scheduler as scheduler_module
from backend.ingestion.tdx_scheduler import TDXScheduler


def _scheduler_with_execute_capture():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    def _execute(sql, params=()):
        calls.append((sql, params))

    scheduler._execute = _execute
    return scheduler, calls


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
    assert seen["params"][2] == "anns_metadata"
    assert seen["params"][4] == "incremental"
    assert "UPDATE market.ingestion_schedules" in seen["schedule_sql"]
    assert "last_status = 'failed'" in seen["schedule_sql"]
    assert seen["schedule_params"] == ("unit_test", str(schedule_id))


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


def test_cyq_perf_routes_to_engine_without_legacy_script():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    submitted = []

    class _Executor:
        def submit(self, fn, *args):
            submitted.append((fn.__name__, args))

            class _Future:
                def add_done_callback(self, _callback):
                    return None

            return _Future()

    class _Tracker:
        def is_running(self, _key):
            return False

        def add(self, key, future):
            submitted.append(("tracker_add", (key, future)))

        def remove(self, _key):
            return None

    scheduler._executor = _Executor()
    scheduler._tracker = _Tracker()

    scheduler._submit_ingestion(None, "cyq_perf", "incremental", "unit", {})

    assert submitted[0][0] == "_run_tushare_engine_sync"
    assert submitted[0][1][2] == "cyq_perf"


def test_cyq_chips_still_uses_legacy_script_until_per_date_audit_policy_exists():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    cmd = scheduler._build_ingestion_command("cyq_chips", "incremental", {})

    assert any("ingest_tushare_cyq.py" in str(part) for part in cmd)
    assert "--dataset" in cmd
    assert "cyq_chips" in cmd


def test_data_freshness_check_does_not_flush_alerts(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    scheduler._db_cfg = {}
    calls = []

    result = SimpleNamespace(
        dataset="cyq_perf",
        status="stale",
        expected_date=__import__("datetime").date(2026, 5, 18),
        failure_category="audit_missing",
        max_date=None,
        quality_status="unknown",
        summary=lambda: {
            "dataset": "cyq_perf",
            "status": "stale",
            "failure_category": "audit_missing",
        },
    )

    class _Checker:
        def __init__(self, _db_cfg):
            pass

        def check_all(self):
            return [result]

    class _TargetRepo:
        def upsert_target(self, target):
            calls.append(("target", target.dataset, target.target_date, target.reason))

    def _execute(sql, params=()):
        calls.append(("execute", sql, params))

    scheduler._execute = _execute
    scheduler._update_ingestion_schedule = lambda *args, **kwargs: calls.append(("schedule", args, kwargs))
    monkeypatch.setattr(scheduler_module, "AuditBackedDataHealthChecker", _Checker)
    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _TargetRepo())

    scheduler._run_data_freshness_check(uuid.uuid4(), None, "unit", {"job_id": str(uuid.uuid4())})

    assert "generate" not in calls
    assert "flush" not in calls
    assert any(call[:2] == ("target", "cyq_perf") for call in calls if isinstance(call, tuple))


def test_final_blocked_targets_alert_only_after_dataset_deadline(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    class _TargetRepo:
        def upsert_target(self, target):
            calls.append(("target", target.dataset, target.status, target.failure_category))
            return "target-1"

    class _AlertGate:
        def flush_final_alerts(self, targets):
            calls.append(("alerts", targets))
            return {"error": len(targets)}

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _TargetRepo())
    monkeypatch.setattr(scheduler_module, "DataSyncAlertGate", lambda: _AlertGate())
    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: __import__("datetime").datetime(2026, 5, 18, 15, 0, tzinfo=__import__("datetime").timezone.utc),
    )

    scheduler._mark_final_blocked_targets(
        [
            {
                "dataset": "cyq_perf",
                "is_fresh": False,
                "health_status": "stale",
                "today_job_status": "success",
                "inserted_rows": 0,
                "failure_category": "empty_invalid",
            }
        ],
        __import__("datetime").date(2026, 5, 18),
    )

    assert calls == []

    monkeypatch.setattr(
        scheduler_module,
        "_now",
        lambda: __import__("datetime").datetime(2026, 5, 18, 16, 0, tzinfo=__import__("datetime").timezone.utc),
    )

    scheduler._mark_final_blocked_targets(
        [
            {
                "dataset": "cyq_perf",
                "is_fresh": False,
                "health_status": "stale",
                "retry_status": "exhausted",
                "today_job_status": "success",
                "inserted_rows": 0,
                "failure_category": "empty_invalid",
            }
        ],
        __import__("datetime").date(2026, 5, 18),
    )

    assert ("target", "cyq_perf", "final_blocked", "empty_invalid") in calls
    alert_call = [call for call in calls if call[0] == "alerts"][0]
    assert alert_call[1][0]["dataset"] == "cyq_perf"
    assert alert_call[1][0]["status"] == "final_blocked"


def test_compute_auto_range_bootstraps_cyq_perf_when_audit_resolver_has_no_cursor():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    def _fetchall(sql, params=()):
        calls.append((sql, params))
        if "FROM market.data_stats_config" in sql:
            return [
                {
                    "table_name": "market.cyq_perf",
                    "date_column": "trade_date",
                    "extra_info": {
                        "cursor_source": "refresh_audit",
                        "bootstrap_start_date": "2018-01-01",
                    },
                }
            ]
        if "MAX(cal_date) AS latest" in sql:
            return [{"latest": dt.date(2026, 5, 18)}]
        raise AssertionError(sql)

    scheduler._fetchall = _fetchall
    scheduler._resolve_refresh_audit_cursor = lambda dataset: None

    start_date, end_date = scheduler._compute_auto_range("cyq_perf")

    assert start_date == dt.date(2018, 1, 1)
    assert end_date == dt.date(2026, 5, 18)
    assert all("dataset_date_refresh_audit" not in sql for sql, _params in calls)


def test_compute_auto_range_seeds_audit_from_physical_rows_before_bootstrap():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    resolver_calls = []

    def _fetchall(sql, params=()):
        if "FROM market.data_stats_config" in sql:
            return [
                {
                    "table_name": "market.cyq_perf",
                    "date_column": "trade_date",
                    "extra_info": {
                        "cursor_source": "refresh_audit",
                        "bootstrap_start_date": "2018-01-01",
                    },
                }
            ]
        if "MAX(cal_date) AS latest" in sql:
            return [{"latest": dt.date(2026, 5, 18)}]
        raise AssertionError(sql)

    def _resolve(dataset):
        resolver_calls.append(dataset)
        return dt.date(2026, 5, 18)

    scheduler._fetchall = _fetchall
    scheduler._resolve_refresh_audit_cursor = _resolve

    start_date, end_date = scheduler._compute_auto_range("cyq_perf")

    assert (start_date, end_date) == (None, None)
    assert resolver_calls == ["cyq_perf"]


def test_compute_auto_range_resumes_from_seed_safe_cursor_before_gap():
    scheduler = TDXScheduler.__new__(TDXScheduler)

    def _fetchall(sql, params=()):
        if "FROM market.data_stats_config" in sql:
            return [
                {
                    "table_name": "market.cyq_perf",
                    "date_column": "trade_date",
                    "extra_info": {
                        "cursor_source": "refresh_audit",
                        "bootstrap_start_date": "2018-01-01",
                    },
                }
            ]
        if "MAX(cal_date) AS latest" in sql:
            return [{"latest": dt.date(2026, 5, 18)}]
        if "SELECT MIN(cal_date) AS nxt" in sql:
            assert params == (dt.date(2026, 5, 14),)
            return [{"nxt": dt.date(2026, 5, 15)}]
        raise AssertionError(sql)

    scheduler._fetchall = _fetchall
    scheduler._resolve_refresh_audit_cursor = lambda _dataset: dt.date(2026, 5, 14)

    start_date, end_date = scheduler._compute_auto_range("cyq_perf")

    assert start_date == dt.date(2026, 5, 15)
    assert end_date == dt.date(2026, 5, 18)


def test_scheduler_refresh_audit_cursor_delegates_to_tushare_engine(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []
    fake_conn = object()

    class _ConnCtx:
        def __enter__(self):
            return fake_conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Engine:
        def _get_incremental_cursor(self, conn, spec):
            calls.append((conn, spec.name))
            return dt.date(2026, 5, 18)

    monkeypatch.setattr(scheduler_module, "_get_conn", lambda _db_cfg: _ConnCtx())
    monkeypatch.setattr(scheduler_module, "TushareSyncEngine", _Engine)

    cursor = scheduler._resolve_refresh_audit_cursor("cyq_perf")

    assert cursor == dt.date(2026, 5, 18)
    assert calls == [(fake_conn, "cyq_perf")]


def test_refresh_schedules_reconciles_due_sync_targets():
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    scheduler._reconcile_stale_queued_ingestion_jobs = lambda: calls.append("stale")
    scheduler._fetchall = lambda sql, params=(): []
    scheduler._update_jobs = lambda testing, ingestion: calls.append(("jobs", list(testing), list(ingestion)))
    scheduler._reconcile_due_data_sync_targets = lambda: calls.append("due")

    scheduler.refresh_schedules()

    assert calls == ["stale", ("jobs", [], []), "due"]


def test_finalize_data_sync_target_retry_closes_recovered_target(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    calls = []

    class _Future:
        def exception(self):
            return None

    class _TargetRepo:
        def update_target_status(self, **kwargs):
            calls.append(("target", kwargs))

        def record_attempt(self, **kwargs):
            calls.append(("attempt", kwargs))
            return "attempt-1"

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _TargetRepo())
    scheduler._target_date_for_retry = lambda _dataset, _options=None: dt.date(2026, 5, 18)
    scheduler._final_deadline_for_target = lambda _dataset, _target_date: dt.datetime(
        2026, 5, 18, 23, 30, tzinfo=dt.timezone.utc
    )
    scheduler._check_dataset_recovered = lambda _dataset: SimpleNamespace(status="ok")

    scheduler._finalize_data_sync_target_retry(
        _Future(),
        target_id="target-1",
        dataset="cyq_perf",
        mode="incremental",
        options={"job_id": str(uuid.uuid4()), "triggered_by": "unit"},
    )

    target_call = calls[0]
    assert target_call[0] == "target"
    assert target_call[1]["target_id"] == "target-1"
    assert target_call[1]["status"] == "success"
    assert target_call[1]["clear_failure"] is True
    assert target_call[1]["clear_retry"] is True


def test_delayed_retry_persists_target_before_timer(monkeypatch):
    scheduler = TDXScheduler.__new__(TDXScheduler)
    scheduler._delayed_retry_keys = set()
    calls = []

    class _TargetRepo:
        def upsert_target(self, target):
            calls.append(("target", target.dataset, target.status, target.next_retry_at, target.final_deadline_at))
            return "target-1"

    class _Timer:
        def __init__(self, delay_seconds, fn):
            calls.append(("timer", delay_seconds))
            self.daemon = False

        def start(self):
            calls.append(("timer_start",))

    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: _TargetRepo())
    monkeypatch.setattr(scheduler_module.threading, "Timer", _Timer)
    scheduler._target_date_for_retry = lambda _dataset, _opts=None: dt.date(2026, 5, 18)

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
    assert target_call[2] == "retry_waiting"
    assert target_call[3] is not None
    assert target_call[4] is not None
    assert ("timer", 3600) in calls
