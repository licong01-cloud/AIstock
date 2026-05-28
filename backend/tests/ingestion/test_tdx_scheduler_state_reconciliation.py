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
        def mark_reconciled(self, target_id, **kwargs):
            calls.append(("target", target_id, kwargs))

        def record_attempt(self, record):
            calls.append(("attempt", record.status, record.target_id))
            return {"attempt_id": "attempt-1"}

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

    assert calls[0][0] == "target"
    assert calls[0][1] == "target-1"
    assert calls[0][2]["context"]["finalizer"] == "data_sync_target_retry"
    assert calls[1] == ("attempt", "reconciled", "target-1")


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


def test_final_deadline_uses_china_local_time():
    scheduler = TDXScheduler.__new__(TDXScheduler)

    deadline = scheduler._final_deadline_for_target("cyq_perf", dt.date(2026, 5, 18))

    assert deadline == dt.datetime(2026, 5, 18, 15, 30, tzinfo=dt.timezone.utc)


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
    scheduler._submit_ingestion = lambda *args, **kwargs: None
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
    assert not any(call[0] == "final" for call in calls)
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
