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

    def _fetchall(sql, params=()):
        seen["sql"] = sql
        seen["params"] = params
        return [{"job_id": job_id}]

    scheduler._fetchall = _fetchall

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
