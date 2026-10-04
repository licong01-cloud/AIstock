"""Conservative repair-impact watermark backed by both managed writer ledgers.

The monthly source snapshot may only reuse a baseline when writes performed by
data-owned jobs are observable.  AIstock currently has two official writer
ledgers: ``market.ingestion_jobs`` and the
``market.data_sync_attempts``/``market.data_sync_targets`` pair.  This adapter
does not infer that a missing row means no write: an unbounded or non-terminal
managed job blocks the source view. Post-watermark writers invalidate the
attempt unless their non-overlapping scope is independently evidenced.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Mapping, Protocol, Sequence


class RepairJournalConnection(Protocol):
    def cursor(self): ...  # type: ignore[no-untyped-def]


class MonthlyRepairJournalError(RuntimeError):
    pass


# These are two different contracts. A terminal attempt can request a future
# retry without being an active writer or proving source data completeness.
TERMINAL_JOB_STATUSES = frozenset(
    {"success", "failed", "cancelled", "completed", "timeout", "delayed"}
)
TERMINAL_SYNC_STATUSES = frozenset({"failed", "retry", "final_blocked", "reconciled"})
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

# Exact producer contracts registered in backend/db/init_tushare_schedules.py.
# A schedule-name prefix or a date strategy alone cannot prove writer scope.
_EMPTY_FUTURE_SUSPEND_SCHEDULES = frozenset({
    ('suspend_d', 'current_and_next_trading_day'),
    ('_suspend_d_tminus1_1730', 'next_trading_day'),
    ('_suspend_d_morning_0730', 'current_or_next_trading_day'),
    ('_suspend_d_preopen_0850', 'current_or_next_trading_day'),
    ('_suspend_d_preopen_0905', 'current_or_next_trading_day'),
    ('_suspend_d_midday_1240', 'current_or_next_trading_day'),
    ('_suspend_d_close_1610', 'current_and_next_trading_day'),
})


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
    source_cutoff: date | None = None

    def __post_init__(self) -> None:
        if not self.managed_datasets or any(not str(value).strip() for value in self.managed_datasets):
            raise ValueError("managed repair journal dataset set is empty or invalid")
        if self.source_cutoff is not None and type(self.source_cutoff) is not date:
            raise ValueError("managed source cutoff must be an exact date")

    def _empty_future_suspend(self, row: Sequence[object]) -> bool:
        """Recognize only the registered empty BY_DATE replacement contract.

        Zero inserted rows alone is not evidence: replacement can delete rows.
        The SQL also binds an empty_valid completion to the same job and date.
        A single strictly post-cutoff date makes that deletion non-overlapping.
        Positive writes, failures, other producers and unknown scopes stay
        conservative until their own complete write bounds are registered.
        """
        if self.source_cutoff is None or len(row) != 9 or row[3] != 'suspend_d':
            return False
        ledger, _, status, _, _, _, finished, scope, empty_completion = row
        expected_status = 'success' if ledger == 'ingestion_jobs' else 'reconciled'
        if status != expected_status or finished is None or empty_completion is not True:
            return False
        if not isinstance(scope, Mapping):
            return False
        expected = {
            'dataset': 'suspend_d', 'actual_dataset': 'suspend_d',
            'mode': 'incremental',
        }
        if any(scope.get(key) != value for key, value in expected.items()):
            return False
        schedule, strategy = scope.get('schedule_dataset'), scope.get('date_strategy')
        if not isinstance(schedule, str) or not isinstance(strategy, str):
            return False
        if (schedule, strategy) not in _EMPTY_FUTURE_SUSPEND_SCHEDULES:
            return False
        stats = scope.get('stats')
        if type(scope.get('inserted_rows')) is not int or scope['inserted_rows'] != 0:
            return False
        if not isinstance(stats, Mapping) or stats.get('dataset') != 'suspend_d':
            return False
        if any(type(stats.get(key)) is not int or stats[key] != value for key, value in {
            'inserted_rows': 0, 'total_batches': 1, 'success_batches': 1, 'failed_batches': 0,
        }.items()):
            return False
        dates = [scope.get(key) for key in (
            'start_date', 'end_date', 'refresh_start_date', 'refresh_end_date',
        )]
        if not isinstance(dates[0], str) or any(value != dates[0] for value in dates):
            return False
        try:
            bounded_date = date.fromisoformat(dates[0])
        except ValueError:
            return False
        return bounded_date.isoformat() == dates[0] and bounded_date > self.source_cutoff

    def initial_watermark(self, connection: RepairJournalConnection) -> str:
        """Reject active managed writers and bind the watermark to snapshot time."""

        # Dispatch is asynchronous: a terminal callback may be appended before
        # the dispatcher's 'started' event. Client started_at/finished_at and DB
        # created_at are also different clocks. Bind completion to the exact
        # immutable execution identity, not insertion order, wall-clock ordering
        # or the latest event for a target (which can have concurrent jobs).

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
                      FROM market.ingestion_jobs AS job
                     WHERE job.status IS NULL
                        OR lower(job.status) <> ALL(%s)
                        OR job.finished_at IS NULL
                ), active_sync AS (
                    SELECT 'data_sync_attempts'::text AS ledger_kind,
                           attempt.attempt_id::text AS ledger_identity,
                           lower(attempt.status) AS status,
                           target.dataset
                      FROM market.data_sync_attempts AS attempt
                      JOIN market.data_sync_targets AS target
                        ON target.target_id=attempt.target_id
                     WHERE target.dataset = ANY(%s)
                       AND (
                            attempt.status IS NULL
                            OR lower(attempt.status) <> ALL(%s)
                            OR attempt.finished_at IS NULL
                       )
                       AND NOT (
                            lower(COALESCE(attempt.status, '')) = 'started'
                            AND (NULLIF(btrim(attempt.job_id), '') IS NOT NULL
                                 OR NULLIF(btrim(attempt.run_id), '') IS NOT NULL)
                            AND (EXISTS (
                                SELECT 1 FROM market.data_sync_attempts AS completed
                                 WHERE completed.target_id = attempt.target_id
                                   AND completed.job_id IS NOT DISTINCT FROM attempt.job_id
                                   AND completed.run_id IS NOT DISTINCT FROM attempt.run_id
                                   AND lower(completed.status) = ANY(%s)
                                   AND completed.finished_at IS NOT NULL
                            ) OR EXISTS (
                                SELECT 1 FROM market.ingestion_jobs AS owner_job
                                 WHERE owner_job.job_id::text = attempt.job_id
                                   AND lower(owner_job.status) = ANY(%s)
                                   AND owner_job.finished_at IS NOT NULL
                            ))
                       )
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
                    sorted(self.managed_datasets),
                    sorted(TERMINAL_SYNC_STATUSES),
                    sorted(TERMINAL_SYNC_STATUSES),
                    sorted(TERMINAL_JOB_STATUSES),
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
                           job.created_at,job.started_at,job.finished_at,
                           job.summary AS writer_scope,
                           EXISTS (
                               SELECT 1 FROM market.data_sync_attempts AS completed
                               JOIN market.data_sync_targets AS target
                                 ON target.target_id=completed.target_id
                                WHERE completed.job_id=job.job_id::text
                                  AND target.dataset='suspend_d'
                                  AND target.target_date::text=job.summary->>'refresh_start_date'
                                  AND target.target_scope='{"query_mode":"by_date"}'::jsonb
                                  AND completed.status='reconciled'
                                  AND completed.finished_at IS NOT NULL
                                  AND completed.rows_written=0 AND completed.rows_observed=0
                                  AND completed.context_json->>'quality_status'='empty_valid'
                           ) AS empty_date_completion
                      FROM market.ingestion_jobs AS job
                     WHERE job.created_at > %s OR job.started_at > %s OR job.finished_at > %s
                ), sync_overlap AS (
                    SELECT 'data_sync_attempts'::text AS ledger_kind,
                           attempt.attempt_id::text AS ledger_identity,
                           lower(attempt.status) AS status,
                           target.dataset,
                           attempt.created_at,attempt.started_at,attempt.finished_at,
                           owner_job.summary AS writer_scope,
                           (
                               owner_job.status='success' AND owner_job.finished_at IS NOT NULL
                               AND target.target_date::text=owner_job.summary->>'refresh_start_date'
                               AND target.target_scope='{"query_mode":"by_date"}'::jsonb
                               AND attempt.rows_written=0 AND attempt.rows_observed=0
                               AND attempt.context_json->>'quality_status'='empty_valid'
                           ) AS empty_date_completion
                      FROM market.data_sync_attempts AS attempt
                      JOIN market.data_sync_targets AS target
                        ON target.target_id=attempt.target_id
                      LEFT JOIN market.ingestion_jobs AS owner_job
                        ON owner_job.job_id::text=attempt.job_id
                     WHERE target.dataset = ANY(%s)
                       AND (
                            attempt.created_at > %s
                            OR attempt.started_at > %s
                            OR attempt.finished_at > %s
                       )
                )
                SELECT ledger_kind,ledger_identity,status,dataset,
                       created_at,started_at,finished_at,writer_scope,empty_date_completion
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
        if len(rows) >= 1000:
            raise MonthlyRepairJournalError("managed overlap query reached its bound; scope is incomplete")
        result: list[str] = []
        for row in rows:
            ledger_kind, identity, status, raw_dataset, *_timestamps = row
            dataset = str(raw_dataset or "")
            if ledger_kind not in {"ingestion_jobs", "data_sync_attempts"}:
                raise MonthlyRepairJournalError("managed writer query returned an unknown ledger")
            if dataset and dataset not in self.managed_datasets:
                raise MonthlyRepairJournalError("managed writer query returned an unknown dataset")
            if ledger_kind == "data_sync_attempts" and not dataset:
                raise MonthlyRepairJournalError("managed sync writer lacks dataset identity")
            if self._empty_future_suspend(row):
                continue
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
