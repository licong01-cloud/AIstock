from __future__ import annotations

import datetime as dt
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.ingest_tushare_adj_factor as ingest_adj_factor

from backend.ingestion.tdx_scheduler import TDXScheduler


def _argument_value(args: list[str], name: str) -> str:
    return args[args.index(name) + 1]


class _Tracker:
    def __init__(self) -> None:
        self.added: list[tuple[str, Future[None]]] = []
        self.removed: list[str] = []

    def is_running(self, key: str) -> bool:
        return False

    def add(self, key: str, future: Future[None]) -> None:
        self.added.append((key, future))

    def remove(self, key: str) -> None:
        self.removed.append(key)


class _Executor:
    def __init__(self) -> None:
        self.submissions: list[tuple[object, tuple[object, ...]]] = []

    def submit(self, function, *args):
        self.submissions.append((function, args))
        future: Future[None] = Future()
        future.set_result(None)
        return future


def test_incremental_adj_factor_scheduler_uses_reconciliation_mode_without_truncate() -> None:
    args = TDXScheduler._default_ingestion_args(
        "adj_factor",
        "incremental",
        {
            "start_date": "2026-09-13",
            "end_date": "2026-09-14",
            "job_id": "job-1",
            "truncate": True,
        },
    )

    assert _argument_value(args, "--mode") == "incremental"
    assert "init" not in args
    assert "--truncate" not in args
    assert _argument_value(args, "--start-date") == "2026-09-13"
    assert _argument_value(args, "--end-date") == "2026-09-14"
    assert _argument_value(args, "--job-id") == "job-1"


def test_submit_ingestion_routes_adj_factor_to_dedicated_reconciliation_script(
    monkeypatch,
) -> None:
    scheduler = TDXScheduler.__new__(TDXScheduler)
    scheduler._executor = _Executor()
    scheduler._tracker = _Tracker()
    process_calls: list[tuple[object, ...]] = []
    engine_calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        scheduler,
        "_run_ingestion_process",
        lambda *args: process_calls.append(args),
    )
    monkeypatch.setattr(
        scheduler,
        "_run_tushare_engine_sync",
        lambda *args: engine_calls.append(args),
    )

    scheduler._submit_ingestion(
        schedule_id="schedule-1",
        dataset="adj_factor",
        mode="incremental",
        triggered_by="schedule",
        options={"end_date": "2026-09-15", "job_id": "job-1"},
    )

    assert len(scheduler._executor.submissions) == 1
    submitted_function, submitted_args = scheduler._executor.submissions[0]
    assert submitted_function == scheduler._run_ingestion_process
    assert engine_calls == []
    command = submitted_args[-1]
    assert Path(command[1]).name == "ingest_tushare_adj_factor.py"
    assert _argument_value(command, "--mode") == "incremental"
    assert _argument_value(command, "--job-id") == "job-1"


def test_incremental_adj_factor_scheduler_passes_optional_history_controls() -> None:
    args = TDXScheduler._default_ingestion_args(
        "adj_factor",
        "incremental",
        {
            "history_reconcile_workers": 7,
            "history_reconcile_rate_per_minute": 420,
            "history_reconcile_max_pages": 6,
            "history_reconcile_symbol": "300506.SZ",
            "history_reconcile_dry_run": True,
        },
    )

    assert _argument_value(args, "--history-reconcile-workers") == "7"
    assert _argument_value(args, "--history-reconcile-rate-per-minute") == "420"
    assert _argument_value(args, "--history-reconcile-max-pages") == "6"
    assert _argument_value(args, "--history-reconcile-symbol") == "300506.SZ"
    assert "--history-reconcile-dry-run" in args


def test_incremental_script_uses_history_reconciliation_as_its_only_factor_write_path(
    monkeypatch,
) -> None:
    class _Connection:
        autocommit = False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

    calls: list[tuple[str, object]] = []

    class _Reconciler:
        def __init__(self, **kwargs):
            calls.append(("constructed", kwargs))

        def reconcile(self, **kwargs):
            calls.append(("reconciled", kwargs))
            return {
                "status": "unchanged",
                "scanned_symbol_count": 5_778,
                "changed_symbol_count": 0,
                "written_row_count": 0,
            }

    args = SimpleNamespace(
        mode="incremental",
        start_date=None,
        end_date="2026-09-14",
        job_id=None,
        truncate=False,
        batch_sleep=0,
        history_reconcile_workers=4,
        history_reconcile_rate_per_minute=480,
        history_reconcile_max_pages=4,
        history_reconcile_symbol=None,
        history_reconcile_dry_run=False,
        bulk_session_tune=False,
    )
    monkeypatch.setattr(ingest_adj_factor, "parse_args", lambda: args)
    monkeypatch.setattr(ingest_adj_factor.psycopg2, "connect", lambda **kwargs: _Connection())
    monkeypatch.setattr(ingest_adj_factor, "pro_api", lambda: object())
    monkeypatch.setattr(ingest_adj_factor, "_get_max_trade_date", lambda connection: dt.date(2026, 9, 13))
    monkeypatch.setattr(ingest_adj_factor, "_create_job", lambda *args: "job-id")
    monkeypatch.setattr(ingest_adj_factor, "_log", lambda *args: None)
    monkeypatch.setattr(ingest_adj_factor, "_touch_data_stats_last_updated", lambda *args: None)
    monkeypatch.setattr(
        ingest_adj_factor,
        "_finish_job",
        lambda connection, job_id, status, summary: calls.append(("finished", (status, summary))),
    )
    monkeypatch.setattr(ingest_adj_factor, "PostgresAdjFactorHistoryRepository", lambda connection: object())
    monkeypatch.setattr(ingest_adj_factor, "AdjFactorHistoryReconciler", _Reconciler)
    monkeypatch.setattr(
        ingest_adj_factor,
        "run_ingestion",
        lambda *args, **kwargs: pytest.fail("incremental mode must not use the date-only upsert path"),
    )

    ingest_adj_factor.main()

    assert [name for name, _ in calls] == ["constructed", "reconciled", "finished"]
    status, summary = calls[-1][1]
    assert status == "success"
    assert summary["history_reconciliation"]["status"] == "unchanged"
