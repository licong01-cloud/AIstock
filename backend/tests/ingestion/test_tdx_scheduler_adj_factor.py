from __future__ import annotations

import datetime as dt
from contextlib import nullcontext
from concurrent.futures import Future
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import scripts.ingest_tushare_adj_factor as ingest_adj_factor
from backend.ingestion.tdx_scheduler import TDXScheduler


def test_known_history_conflict_cannot_be_reconciled_by_permissive_freshness(monkeypatch):
    import backend.ingestion.tdx_scheduler as scheduler_module
    scheduler = object.__new__(TDXScheduler)
    repository = Mock()
    monkeypatch.setattr(scheduler_module, "DataSyncTargetRepository", lambda: repository)
    scheduler._target_date_for_retry = lambda *args: dt.date(2026, 10, 27)
    scheduler._job_update_outcome = lambda _job: {
        "status": "failed", "history_reconciliation": {"failures": [
            {"symbol": "920047.BJ", "retryable": False, "error": "source history conflict"}]}}
    scheduler._check_dataset_recovered = Mock(return_value=SimpleNamespace(status="ok"))
    scheduler._reconcile_recovered_target_state = Mock()
    done = Future()
    done.set_result(None)
    scheduler._finalize_data_sync_target_retry(done, target_id="target", dataset="adj_factor",
        mode="incremental", options={"job_id": "c197fd66-9de8-4e0c-a994-f774a42a89e7"})
    repository.mark_final_blocked.assert_called_once()
    scheduler._reconcile_recovered_target_state.assert_not_called()
    scheduler._check_dataset_recovered.assert_not_called()


@pytest.mark.parametrize("records", [[],
    [{"ts_code": "300506.SZ", "trade_date": "20261026", "adj_factor": 1}],
    [{"ts_code": "300506.SZ", "trade_date": "20261027", "adj_factor": float("nan")}],
    [{"ts_code": "300506.SZ", "trade_date": "20261027", "adj_factor": 1}]*2,
])
def test_daily_provider_empty_wrong_date_invalid_or_duplicate_cannot_succeed(records):
    import pandas as pd
    class Provider:
        def adj_factor(self, **_kwargs):
            return pd.DataFrame(records)
    with pytest.raises((RuntimeError, ValueError)):
        ingest_adj_factor._fetch_adj_factor_for_date(Provider(), dt.date(2026, 10, 27))



def _argument_value(args: list[str], name: str) -> str:
    return args[args.index(name) + 1]


def test_adj_factor_script_starts_by_absolute_path_outside_repository(tmp_path: Path) -> None:
    script_path = Path(ingest_adj_factor.__file__).resolve()

    completed = subprocess.run(
        [sys.executable, str(script_path), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        encoding="utf-8",
        errors="replace",
    )

    assert completed.returncode == 0, completed.stderr
    assert "--history-reconcile-dry-run" in completed.stdout
    assert "ModuleNotFoundError" not in completed.stderr


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
    def run_history(connection, arguments, job_id, end_date):
        reconciler = _Reconciler(repository=object())
        return reconciler.reconcile(end_date=end_date)
    monkeypatch.setattr(ingest_adj_factor, "run_history_ingestion", run_history)
    monkeypatch.setattr(ingest_adj_factor, "PostgresAdjFactorHistoryRepository",
                        lambda _connection: SimpleNamespace(writer_lock=nullcontext))
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
