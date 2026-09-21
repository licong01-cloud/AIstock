from __future__ import annotations

from datetime import UTC, datetime

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
