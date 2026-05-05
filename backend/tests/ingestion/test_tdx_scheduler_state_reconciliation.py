import uuid

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
