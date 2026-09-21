"""Conservative repair-impact watermark backed by both managed writer ledgers.

The monthly source snapshot may only reuse a baseline when writes performed by
data-owned jobs are observable.  AIstock currently has two official writer
ledgers: ``market.ingestion_jobs`` and the
``market.data_sync_attempts``/``market.data_sync_targets`` pair.  This adapter
does not infer that a missing row means no write: an unbounded or non-terminal
managed job blocks the source view, and every managed writer committed after
the snapshot watermark invalidates the materialized source attempt.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, Sequence


class RepairJournalConnection(Protocol):
    def cursor(self): ...  # type: ignore[no-untyped-def]


class MonthlyRepairJournalError(RuntimeError):
    pass


TERMINAL_JOB_STATUSES = frozenset({"success", "failed", "cancelled", "completed"})
MANAGED_SOURCE_DATASETS = frozenset(
    {
        "adj_factor",
        "bak_basic",
        "cyq_perf",
        "daily_basic",
        "index_daily",
        "index_membership_pit",
        "industry_classification",
        "kline_daily_raw",
        "kline_minute_raw",
        "margin_detail",
        "moneyflow",
        "stk_limit",
        "stock_basic",
        "stock_universe_pit",
        "suspend_d",
        "sw_daily",
    }
)


def _watermark(value: str) -> datetime:
    prefix = "managed-writer-ledgers-v2:"
    if not value.startswith(prefix):
        raise MonthlyRepairJournalError("repair watermark schema differs")
    try:
        parsed = datetime.fromisoformat(value[len(prefix) :])
    except ValueError as exc:
        raise MonthlyRepairJournalError("repair watermark timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise MonthlyRepairJournalError("repair watermark timestamp lacks timezone")
    return parsed


@dataclass(frozen=True, slots=True)
class ManagedRepairImpactJournal:
    """Read one fail-closed watermark for release-owned source datasets."""

    managed_datasets: frozenset[str] = MANAGED_SOURCE_DATASETS

    def __post_init__(self) -> None:
        if not self.managed_datasets or any(not str(value).strip() for value in self.managed_datasets):
            raise ValueError("managed repair journal dataset set is empty or invalid")

    def initial_watermark(self, connection: RepairJournalConnection) -> str:
        """Reject active managed writers and bind the watermark to snapshot time."""

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT transaction_timestamp()
                """
            )
            row = cursor.fetchone()
            cursor.execute(
                """
                WITH active_ingestion AS (
                    SELECT 'ingestion_jobs'::text AS ledger_kind,
                           job_id::text AS ledger_identity,
                           lower(status) AS status,
                           COALESCE(
                               NULLIF(summary->>'actual_dataset', ''),
                               NULLIF(summary->>'schedule_dataset', ''),
                               NULLIF(summary->>'dataset', '')
                           ) AS dataset
                      FROM market.ingestion_jobs
                     WHERE lower(status) <> ALL(%s)
                ), active_sync AS (
                    SELECT 'data_sync_attempts'::text AS ledger_kind,
                           attempt.attempt_id::text AS ledger_identity,
                           lower(attempt.status) AS status,
                           target.dataset
                      FROM market.data_sync_attempts AS attempt
                      JOIN market.data_sync_targets AS target
                        ON target.target_id=attempt.target_id
                     WHERE lower(attempt.status) <> ALL(%s)
                       AND target.dataset = ANY(%s)
                )
                SELECT ledger_kind,ledger_identity,status,dataset
                  FROM (
                        SELECT * FROM active_ingestion
                         WHERE dataset IS NULL OR dataset = ANY(%s)
                        UNION ALL
                        SELECT * FROM active_sync
                  ) AS active_writer
                 ORDER BY ledger_kind,ledger_identity
                 LIMIT 50
                """,
                (
                    sorted(TERMINAL_JOB_STATUSES),
                    sorted(TERMINAL_JOB_STATUSES),
                    sorted(self.managed_datasets),
                    sorted(self.managed_datasets),
                ),
            )
            active = list(cursor.fetchall() or ())
        if not row or not isinstance(row[0], datetime) or row[0].tzinfo is None:
            raise MonthlyRepairJournalError("database did not return a timezone-aware journal watermark")
        if active:
            identities = [f"{item[0]}:{item[1]}" for item in active[:3]]
            raise MonthlyRepairJournalError(
                f"managed source writers are not terminal at source freeze: {identities}"
            )
        return f"managed-writer-ledgers-v2:{row[0].isoformat()}"

    def overlapping_repairs(
        self,
        connection: RepairJournalConnection,
        watermark: str,
    ) -> Sequence[str]:
        """Return every managed job which could have written after freeze.

        The query deliberately does not depend on mtime, row counts or a job's
        claimed success.  A failed job can have committed partial batches and
        therefore invalidates the snapshot as well.
        """

        observed_at = _watermark(watermark)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH ingestion_overlap AS (
                    SELECT 'ingestion_jobs'::text AS ledger_kind,
                           job_id::text AS ledger_identity,
                           lower(status) AS status,
                           COALESCE(
                               NULLIF(summary->>'actual_dataset', ''),
                               NULLIF(summary->>'schedule_dataset', ''),
                               NULLIF(summary->>'dataset', '')
                           ) AS dataset,
                           created_at,started_at,finished_at
                      FROM market.ingestion_jobs
                     WHERE created_at > %s OR started_at > %s OR finished_at > %s
                ), sync_overlap AS (
                    SELECT 'data_sync_attempts'::text AS ledger_kind,
                           attempt.attempt_id::text AS ledger_identity,
                           lower(attempt.status) AS status,
                           target.dataset,
                           attempt.created_at,attempt.started_at,attempt.finished_at
                      FROM market.data_sync_attempts AS attempt
                      JOIN market.data_sync_targets AS target
                        ON target.target_id=attempt.target_id
                     WHERE target.dataset = ANY(%s)
                       AND (
                            attempt.created_at > %s
                            OR attempt.started_at > %s
                            OR attempt.finished_at > %s
                       )
                )
                SELECT ledger_kind,ledger_identity,status,dataset,
                       created_at,started_at,finished_at
                  FROM (
                        SELECT * FROM ingestion_overlap
                         WHERE dataset IS NULL OR dataset = ANY(%s)
                        UNION ALL
                        SELECT * FROM sync_overlap
                  ) AS overlapping_writer
                 ORDER BY ledger_kind,ledger_identity
                 LIMIT 1000
                """,
                (
                    observed_at,
                    observed_at,
                    observed_at,
                    sorted(self.managed_datasets),
                    observed_at,
                    observed_at,
                    observed_at,
                    sorted(self.managed_datasets),
                ),
            )
            rows = list(cursor.fetchall() or ())
        result: list[str] = []
        for ledger_kind, identity, status, raw_dataset, *_timestamps in rows:
            dataset = str(raw_dataset or "")
            if ledger_kind not in {"ingestion_jobs", "data_sync_attempts"}:
                raise MonthlyRepairJournalError("managed writer query returned an unknown ledger")
            if dataset and dataset not in self.managed_datasets:
                raise MonthlyRepairJournalError("managed writer query returned an unknown dataset")
            if ledger_kind == "data_sync_attempts" and not dataset:
                raise MonthlyRepairJournalError("managed sync writer lacks dataset identity")
            result.append(
                f"{ledger_kind}:{identity}:{dataset or 'unclassified'}:{str(status).lower()}"
            )
        return tuple(result)


__all__ = (
    "MANAGED_SOURCE_DATASETS",
    "ManagedRepairImpactJournal",
    "MonthlyRepairJournalError",
    "RepairJournalConnection",
    "TERMINAL_JOB_STATUSES",
)
