from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

import scripts.ingest_tushare_adj_factor as ingest_adj_factor

from backend.ingestion.tdx_scheduler import TDXScheduler


def _argument_value(args: list[str], name: str) -> str:
    return args[args.index(name) + 1]


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
