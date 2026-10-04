from __future__ import annotations

from datetime import UTC, date, datetime
import json
import os

import pytest

from backend.services.dataset_release.monthly_repair_journal import (
    ManagedRepairImpactJournal,
    MonthlyRepairJournalError,
)


class Cursor:
    def __init__(self, connection: "Connection") -> None:
        self.connection = connection
        self.rows: list[tuple[object, ...]] = []

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self.connection.calls.append((sql, params))
        if "transaction_timestamp" in sql:
            self.rows = [(self.connection.now,)]
        elif "active_writer" in sql:
            self.rows = list(self.connection.active)
        else:
            self.rows = list(self.connection.overlaps)

    def fetchone(self):  # type: ignore[no-untyped-def]
        return self.rows[0] if self.rows else None

    def fetchall(self):  # type: ignore[no-untyped-def]
        return list(self.rows)


class Connection:
    def __init__(self) -> None:
        self.now = datetime(2026, 10, 1, tzinfo=UTC)
        self.active: list[tuple[object, ...]] = []
        self.overlaps: list[tuple[object, ...]] = []
        self.calls: list[tuple[str, object]] = []

    def cursor(self) -> Cursor:
        return Cursor(self)


def _future_empty_suspend(connection: Connection, *, ledger: str = 'ingestion_jobs') -> tuple:
    scope = {
        'dataset': 'suspend_d', 'actual_dataset': 'suspend_d',
        'schedule_dataset': 'suspend_d', 'mode': 'incremental',
        'date_strategy': 'current_and_next_trading_day',
        'start_date': '2026-10-08', 'end_date': '2026-10-08',
        'refresh_start_date': '2026-10-08', 'refresh_end_date': '2026-10-08',
        'inserted_rows': 0,
        'stats': {'dataset': 'suspend_d', 'inserted_rows': 0, 'total_batches': 1,
                  'success_batches': 1, 'failed_batches': 0},
    }
    return (ledger, 'future-empty', 'success' if ledger == 'ingestion_jobs' else 'reconciled',
            'suspend_d', connection.now, None, connection.now, scope, True)


def test_evidenced_empty_future_suspend_does_not_invalidate_september_source() -> None:
    connection = Connection()
    connection.overlaps = [_future_empty_suspend(connection),
                           _future_empty_suspend(connection, ledger='data_sync_attempts')]
    assert ManagedRepairImpactJournal(source_cutoff=date(2026, 9, 30)).overlapping_repairs(
        connection, 'managed-writer-ledgers-v2:2026-10-01T00:00:00+00:00',
    ) == ()


REGISTERED_SUSPEND_SCOPES = (
    ('suspend_d', 'current_and_next_trading_day'),
    ('_suspend_d_tminus1_1730', 'next_trading_day'),
    ('_suspend_d_morning_0730', 'current_or_next_trading_day'),
    ('_suspend_d_preopen_0850', 'current_or_next_trading_day'),
    ('_suspend_d_preopen_0905', 'current_or_next_trading_day'),
    ('_suspend_d_midday_1240', 'current_or_next_trading_day'),
    ('_suspend_d_close_1610', 'current_and_next_trading_day'),
)


@pytest.mark.parametrize('schedule,strategy', REGISTERED_SUSPEND_SCOPES)
@pytest.mark.parametrize('ledger', ['ingestion_jobs', 'data_sync_attempts'])
def test_registered_empty_future_suspend_scopes_are_non_overlapping(
    schedule: str, strategy: str, ledger: str,
) -> None:
    connection = Connection()
    row = _future_empty_suspend(connection, ledger=ledger)
    row[7].update(schedule_dataset=schedule, date_strategy=strategy)
    connection.overlaps = [row]
    assert ManagedRepairImpactJournal(source_cutoff=date(2026, 9, 30)).overlapping_repairs(
        connection, 'managed-writer-ledgers-v2:2026-10-01T00:00:00+00:00',
    ) == ()


def test_suspend_scope_contract_matches_registered_default_schedules() -> None:
    import ast
    from pathlib import Path

    source = Path(__file__).resolve().parents[2] / 'db' / 'init_tushare_schedules.py'
    tree = ast.parse(source.read_text(encoding='utf-8'))
    registration = next(node for node in tree.body if isinstance(node, ast.AnnAssign)
                        and isinstance(node.target, ast.Name)
                        and node.target.id == '_DEFAULT_SCHEDULES')
    schedules = ast.literal_eval(registration.value)
    assert set(REGISTERED_SUSPEND_SCOPES) == {
        (row['dataset'], row['date_strategy']) for row in schedules
        if row['dataset'] == 'suspend_d' or row['dataset'].startswith('_suspend_d_')
    }


@pytest.mark.parametrize('schedule,strategy', [
    ('_suspend_d_custom', 'current_or_next_trading_day'),
    ('_suspend_d_midday_1240', 'current_and_next_trading_day'),
    ('suspend_d', 'current_or_next_trading_day'),
    (None, 'current_or_next_trading_day'),
    ([], 'current_or_next_trading_day'),
    ('_suspend_d_midday_1240', {}),
])
def test_unknown_or_mismatched_suspend_scope_is_still_an_overlap(schedule, strategy) -> None:
    connection = Connection()
    row = _future_empty_suspend(connection)
    row[7].update(schedule_dataset=schedule, date_strategy=strategy)
    connection.overlaps = [row]
    assert len(ManagedRepairImpactJournal(source_cutoff=date(2026, 9, 30)).overlapping_repairs(
        connection, 'managed-writer-ledgers-v2:2026-10-01T00:00:00+00:00',
    )) == 1


@pytest.mark.parametrize('schedule,strategy', [
    ('suspend_d', 'current_and_next_trading_day'),
    ('_suspend_d_midday_1240', 'current_or_next_trading_day'),
])
@pytest.mark.parametrize('case', [
    'no_cutoff', 'historical', 'on_cutoff', 'two_dates', 'unknown_date', 'compact_date',
    'missing_scope', 'missing_callback', 'failed', 'unfinished', 'non_suspend',
    'wrong_dataset', 'unknown_mode', 'unknown_strategy', 'positive_job_rows',
    'positive_stats', 'failed_batch', 'extra_batch', 'missing_stats', 'null_proof',
])
def test_future_scope_exception_keeps_unproven_writes_fail_closed(
    case: str, schedule: str, strategy: str,
) -> None:
    connection = Connection()
    row = list(_future_empty_suspend(connection))
    scope = row[7]
    scope.update(schedule_dataset=schedule, date_strategy=strategy)
    cutoff = None if case == 'no_cutoff' else date(2026, 9, 30)
    if case in {'historical', 'on_cutoff', 'unknown_date', 'compact_date'}:
        value = {'historical': '2026-09-29', 'on_cutoff': '2026-09-30',
                 'unknown_date': '2026-02-30', 'compact_date': '20261008'}[case]
        for key in ('start_date', 'end_date', 'refresh_start_date', 'refresh_end_date'):
            scope[key] = value
    elif case == 'two_dates':
        scope['refresh_end_date'] = '2026-10-09'
    elif case == 'missing_scope':
        scope.pop('refresh_start_date')
    elif case == 'missing_callback':
        row[8] = False
    elif case == 'null_proof':
        row[8] = None
    elif case == 'failed':
        row[2] = 'failed'
    elif case == 'unfinished':
        row[6] = None
    elif case == 'non_suspend':
        row[3] = 'adj_factor'
    elif case == 'wrong_dataset':
        scope['actual_dataset'] = 'adj_factor'
    elif case == 'unknown_mode':
        scope['mode'] = 'full'
    elif case == 'unknown_strategy':
        scope['date_strategy'] = 'history'
    elif case == 'positive_job_rows':
        scope['inserted_rows'] = 1
    elif case == 'positive_stats':
        scope['stats']['inserted_rows'] = 1
    elif case == 'failed_batch':
        scope['stats']['failed_batches'] = 1
    elif case == 'extra_batch':
        scope['stats']['total_batches'] = 2
    elif case == 'missing_stats':
        scope.pop('stats')
    connection.overlaps = [tuple(row)]
    assert len(ManagedRepairImpactJournal(source_cutoff=cutoff).overlapping_repairs(
        connection, 'managed-writer-ledgers-v2:2026-10-01T00:00:00+00:00',
    )) == 1


def test_overlap_query_bound_cannot_certify_a_truncated_empty_set() -> None:
    connection = Connection()
    connection.overlaps = [_future_empty_suspend(connection)] * 1000
    with pytest.raises(MonthlyRepairJournalError, match='scope is incomplete'):
        ManagedRepairImpactJournal(source_cutoff=date(2026, 9, 30)).overlapping_repairs(
            connection, 'managed-writer-ledgers-v2:2026-10-01T00:00:00+00:00',
        )


def test_journal_watermark_rejects_nonterminal_managed_job() -> None:
    connection = Connection()
    connection.active = [("ingestion_jobs", "job-1", "running", "daily_basic")]
    with pytest.raises(MonthlyRepairJournalError, match="not terminal"):
        ManagedRepairImpactJournal().initial_watermark(connection)


def test_journal_reports_success_and_failed_jobs_after_snapshot() -> None:
    connection = Connection()
    journal = ManagedRepairImpactJournal()
    watermark = journal.initial_watermark(connection)
    connection.overlaps = [
        (
            "ingestion_jobs",
            "job-2",
            "success",
            "adj_factor",
            connection.now,
            connection.now,
            connection.now,
        ),
        (
            "data_sync_attempts",
            "attempt-3",
            "failed",
            "kline_minute_raw",
            connection.now,
            connection.now,
            connection.now,
        ),
    ]
    assert journal.overlapping_repairs(connection, watermark) == (
        "ingestion_jobs:job-2:adj_factor:success",
        "data_sync_attempts:attempt-3:kline_minute_raw:failed",
    )


def test_journal_rejects_unknown_or_malformed_overlap_scope() -> None:
    connection = Connection()
    connection.overlaps = [
        ("ingestion_jobs", "job-4", "success", "other", None, None, None)
    ]
    with pytest.raises(MonthlyRepairJournalError, match="unknown dataset"):
        ManagedRepairImpactJournal().overlapping_repairs(
            connection,
            "managed-writer-ledgers-v2:2026-10-01T00:00:00+00:00",
        )
    connection.overlaps = [("unknown", "job-5", "success", "daily_basic", None, None, None)]
    with pytest.raises(MonthlyRepairJournalError, match="unknown ledger"):
        ManagedRepairImpactJournal().overlapping_repairs(
            connection,
            "managed-writer-ledgers-v2:2026-10-01T00:00:00+00:00",
        )


def test_journal_rejects_nonterminal_managed_data_sync_attempt() -> None:
    connection = Connection()
    connection.active = [
        ("data_sync_attempts", "attempt-9", "started", "kline_daily_raw")
    ]
    with pytest.raises(MonthlyRepairJournalError, match="not terminal"):
        ManagedRepairImpactJournal().initial_watermark(connection)


def test_writer_ledgers_use_their_own_completion_contracts() -> None:
    connection = Connection()
    ManagedRepairImpactJournal().initial_watermark(connection)
    sql, params = connection.calls[1]
    assert {"success", "failed", "cancelled", "completed", "timeout", "delayed"} == set(params[0])
    assert {"failed", "retry", "final_blocked", "reconciled"} == set(params[2])
    assert "attempt.finished_at IS NULL" in sql
    assert "job.finished_at IS NULL" in sql
    assert "attempt.status IS NULL" in sql


def test_started_event_requires_same_execution_completion_not_latest_target() -> None:
    connection = Connection()
    ManagedRepairImpactJournal().initial_watermark(connection)
    sql, _params = connection.calls[1]
    for predicate in (
        "completed.target_id = attempt.target_id",
        "completed.job_id IS NOT DISTINCT FROM attempt.job_id",
        "completed.run_id IS NOT DISTINCT FROM attempt.run_id",
        "completed.finished_at IS NOT NULL",
        "owner_job.job_id::text = attempt.job_id",
        "owner_job.finished_at IS NOT NULL",
    ):
        assert predicate in sql
    assert "last_attempt_id" not in sql
    assert "completed.attempt_no >" not in sql  # callbacks may arrive before 'started'
    assert "completed.created_at >=" not in sql


@pytest.mark.parametrize("status", ["started", "unknown", None, "reconciled_without_finish"])
def test_unknown_unclosed_or_malformed_sync_writer_still_blocks(status: str | None) -> None:
    connection = Connection()
    connection.active = [("data_sync_attempts", "attempt-unsafe", status, "daily_basic")]
    with pytest.raises(MonthlyRepairJournalError, match="not terminal"):
        ManagedRepairImpactJournal().initial_watermark(connection)


@pytest.fixture
def readonly_dev_connection():  # type: ignore[no-untyped-def]
    if os.getenv("AISTOCK_MONTHLY_JOURNAL_DEV_READBACK") != "1":
        pytest.skip("explicit existing DEV read-only validation only")
    import psycopg2

    target = {key: os.environ[f"TDX_DB_DEV_{env}"] for key, env in (
        ("host", "HOST"), ("port", "PORT"), ("dbname", "NAME"),
        ("user", "USER"), ("password", "PASSWORD"),
    )}
    assert str(target["port"]) == "5433" and "dev" in str(target["dbname"]).lower()
    connection = psycopg2.connect(**target, application_name="BUG-1659-readonly-DEV")
    connection.set_session(readonly=True, autocommit=False)
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


@pytest.mark.parametrize('case,excluded', [
    ('closed_empty', True), ('positive', False), ('unknown_quality', False),
    ('closed_midday', True), ('mismatched_schedule', False), ('unknown_schedule', False),
    ('wrong_job', False), ('wrong_date', False), ('wrong_scope', False),
    ('owner_failed', False), ('callback_failed', False), ('unclosed', False),
    ('historical', False),
])
def test_actual_overlap_sql_in_readonly_dev(readonly_dev_connection, case: str, excluded: bool) -> None:
    """Run the production overlap SQL on typed CTE facts, without DEV writes."""
    moment = '2026-10-02T00:00:00+00:00'
    scope = _future_empty_suspend(Connection())[7]
    if case == 'closed_midday':
        scope.update(schedule_dataset='_suspend_d_midday_1240',
                     date_strategy='current_or_next_trading_day')
    elif case == 'mismatched_schedule':
        scope['schedule_dataset'] = '_suspend_d_midday_1240'
    elif case == 'unknown_schedule':
        scope['schedule_dataset'] = '_suspend_d_custom'
    if case == 'historical':
        for key in ('start_date', 'end_date', 'refresh_start_date', 'refresh_end_date'):
            scope[key] = '2026-09-30'
    jobs = [dict(job_id='j', status='failed' if case == 'owner_failed' else 'success',
                 summary=scope, created_at=moment, started_at=moment, finished_at=moment)]
    attempts = [dict(attempt_id='a', target_id='t', job_id='other' if case == 'wrong_job' else 'j',
                     status='failed' if case == 'callback_failed' else 'reconciled',
                     rows_written=1 if case == 'positive' else 0, rows_observed=0,
                     context_json={'quality_status': 'unknown' if case == 'unknown_quality' else 'empty_valid'},
                     created_at=moment, started_at=moment,
                     finished_at=None if case == 'unclosed' else moment)]
    targets = [dict(target_id='t', dataset='suspend_d',
                    target_date='2026-10-09' if case == 'wrong_date' else scope['start_date'],
                    target_scope={'query_mode': 'by_code' if case == 'wrong_scope' else 'by_date'})]
    fake = Connection()
    journal = ManagedRepairImpactJournal(source_cutoff=date(2026, 9, 30))
    journal.overlapping_repairs(fake, 'managed-writer-ledgers-v2:2026-10-01T00:00:00+00:00')
    sql, params = fake.calls[0]
    for table, cte in (('market.ingestion_jobs', 'fixture_jobs'),
                       ('market.data_sync_attempts', 'fixture_attempts'),
                       ('market.data_sync_targets', 'fixture_targets')):
        sql = sql.replace(table, cte)
    sql = '''WITH fixture_jobs AS (
        SELECT * FROM jsonb_to_recordset(%s::jsonb) AS j(
            job_id text,status text,summary jsonb,created_at timestamptz,
            started_at timestamptz,finished_at timestamptz)
    ), fixture_attempts AS (
        SELECT * FROM jsonb_to_recordset(%s::jsonb) AS a(
            attempt_id text,target_id text,job_id text,status text,
            rows_written bigint,rows_observed bigint,context_json jsonb,
            created_at timestamptz,started_at timestamptz,finished_at timestamptz)
    ), fixture_targets AS (
        SELECT * FROM jsonb_to_recordset(%s::jsonb) AS t(
            target_id text,dataset text,target_date date,target_scope jsonb)
    ), ''' + sql.strip().removeprefix('WITH ')
    with readonly_dev_connection.cursor() as cursor:
        cursor.execute('SHOW transaction_read_only')
        assert cursor.fetchone()[0] == 'on'
        cursor.execute(sql, (json.dumps(jobs), json.dumps(attempts), json.dumps(targets), *params))
        rows = cursor.fetchall()
    assert len(rows) == 2
    assert all(journal._empty_future_suspend(row) is excluded for row in rows)


@pytest.mark.parametrize("case,blocked", [
    ("reconciled", False), ("retry", False), ("failed", False), ("final_blocked", False),
    ("unfinished_reconciled", True), ("unfinished_retry", True),
    ("unfinished_failed", True), ("unfinished_final_blocked", True),
    ("null_status", True), ("unknown_status", True), ("unclosed_start", True),
    ("same_execution", False), ("reverse_callback", False), ("different_job", True),
    ("different_run", True), ("different_target", True), ("no_execution_identity", True),
    ("run_only_execution", False), ("callback_without_finish", True),
    ("blank_execution_identity", True),
    ("owner_success", False), ("owner_timeout", False), ("owner_delayed", False),
    ("owner_unfinished", True), ("owner_unknown", True), ("owner_running", True),
    ("owner_null_status", True), ("running_with_completed_callback", True),
    ("unrelated_owner", False), ("unclassified_owner", True),
])
def test_actual_postgres_writer_query_in_readonly_dev(
    readonly_dev_connection, case: str, blocked: bool,
) -> None:
    """Execute the production query, substituting typed read-only CTE facts.

    No temporary tables, fixtures written to DEV, schema changes or new DB.
    CI keeps the deterministic local contract tests; this matrix is an explicit
    existing-DEV gate, not a synthetic production/source-ready receipt.
    """
    moment = "2026-10-01T00:00:00+00:00"
    attempt = dict(attempt_id="a", target_id="t", status="started", job_id="j",
                   run_id=None, created_at=moment, started_at=moment, finished_at=None)
    completed = {**attempt, "attempt_id": "b", "status": "reconciled", "finished_at": moment}
    jobs = []
    attempts = [attempt]
    if case in {"failed", "retry", "final_blocked", "reconciled"}:
        attempt.update(status=case, finished_at=moment)
    elif case.startswith("unfinished_"):
        attempt["status"] = case.removeprefix("unfinished_")
    elif case in {"null_status", "unknown_status"}:
        attempt["status"] = None if case == "null_status" else "mystery"
    elif case in {"same_execution", "reverse_callback", "different_job", "different_run",
                  "different_target", "no_execution_identity", "run_only_execution",
                  "callback_without_finish", "blank_execution_identity"}:
        if case == "different_job":
            completed["job_id"] = "other"
        if case == "different_run":
            completed["run_id"] = "other"
        if case == "different_target":
            completed["target_id"] = "other"
        if case == "no_execution_identity":
            attempt["job_id"] = completed["job_id"] = None
        if case == "run_only_execution":
            attempt["job_id"] = completed["job_id"] = None
            attempt["run_id"] = completed["run_id"] = "run"
        if case == "callback_without_finish":
            completed["finished_at"] = None
        if case == "blank_execution_identity":
            attempt["job_id"] = completed["job_id"] = " "
        attempts.append(completed)
        if case == "reverse_callback":
            attempts.reverse()
    elif case.startswith("owner_") or case in {
        "running_with_completed_callback", "unrelated_owner", "unclassified_owner",
    }:
        status = case.removeprefix("owner_")
        status = {"unfinished": "success", "unknown": "mystery", "null_status": None}.get(status, status)
        if case == "running_with_completed_callback":
            status = "running"
            attempts.append(completed)
        if case in {"unrelated_owner", "unclassified_owner"}:
            status = "running"
            attempts = []
        summary = {"dataset": "other" if case == "unrelated_owner" else "daily_basic"}
        if case == "unclassified_owner":
            summary = {}
        jobs = [dict(job_id="j", status=status, summary=summary, created_at=moment,
                     finished_at=None if case in {"owner_unfinished", "owner_running"} else moment)]

    fake = Connection()
    ManagedRepairImpactJournal().initial_watermark(fake)
    sql, params = fake.calls[1]
    for table, cte in (("market.ingestion_jobs", "fixture_jobs"),
                       ("market.data_sync_attempts", "fixture_attempts"),
                       ("market.data_sync_targets", "fixture_targets")):
        sql = sql.replace(table, cte)
    fixture_sql = """WITH fixture_jobs AS (
        SELECT * FROM jsonb_to_recordset(%s::jsonb) AS j(
            job_id text,status text,summary jsonb,created_at timestamptz,finished_at timestamptz)
    ), fixture_attempts AS (
        SELECT * FROM jsonb_to_recordset(%s::jsonb) AS a(
            attempt_id text,target_id text,status text,job_id text,run_id text,
            created_at timestamptz,started_at timestamptz,finished_at timestamptz)
    ), fixture_targets AS (
        SELECT * FROM jsonb_to_recordset(%s::jsonb) AS t(target_id text,dataset text)
    ), """ + sql.strip().removeprefix("WITH ")
    with readonly_dev_connection.cursor() as cursor:
        cursor.execute("SHOW transaction_read_only")
        assert cursor.fetchone()[0] == "on"
        cursor.execute(fixture_sql, (
            json.dumps(jobs), json.dumps(attempts),
            json.dumps([{"target_id": "t", "dataset": "daily_basic"},
                        {"target_id": "other", "dataset": "daily_basic"}]), *params,
        ))
        assert bool(cursor.fetchall()) is blocked


@pytest.mark.parametrize("status", ["failed", "retry", "final_blocked", "reconciled", "started"])
def test_every_sync_event_after_freeze_invalidates_source_even_when_finished(status: str) -> None:
    connection = Connection()
    connection.overlaps = [("data_sync_attempts", "attempt-post-freeze", status,
                            "daily_basic", connection.now, None, connection.now)]
    assert ManagedRepairImpactJournal().overlapping_repairs(
        connection, "managed-writer-ledgers-v2:2026-10-01T00:00:00+00:00",
    ) == (f"data_sync_attempts:attempt-post-freeze:daily_basic:{status}",)
