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

MANAGED_WRITER_SCOPE_POLICY = 'bounded_post_cutoff_completed_date_writers_v2'


def _completed_suspend_job_sql(owner: str) -> str:
    """Prove a whole registered date window from immutable completion events.

    ``owner`` is a code-owned SQL alias, never a caller-provided identifier.
    No physical market-data scan or mutable latest-attempt pointer is used.
    """
    if owner not in {'job', 'owner_job'}:
        raise ValueError('unknown managed writer SQL alias')
    return f"""(
        {owner}.status='success' AND {owner}.finished_at IS NOT NULL
        AND (SELECT COUNT(*)>0
                    AND COUNT(*)=COUNT(DISTINCT completed_target.target_date)
                    AND COUNT(*)::text={owner}.summary#>>'{{stats,total_batches}}'
                    AND MIN(completed_target.target_date)::text={owner}.summary->>'refresh_start_date'
                    AND MAX(completed_target.target_date)::text={owner}.summary->>'refresh_end_date'
                    AND SUM(completed.rows_written)::text={owner}.summary->>'inserted_rows'
                    AND BOOL_AND(COALESCE(completed_target.dataset='suspend_d'
                        AND completed_target.target_scope='{{"query_mode":"by_date"}}'::jsonb
                        AND completed.finished_at IS NOT NULL
                        AND completed.rows_written=completed.rows_observed
                        AND ((completed.rows_written=0
                              AND completed.context_json->>'quality_status'='empty_valid')
                             OR (completed.rows_written>0
                                 AND completed.context_json->>'quality_status'='ok')), FALSE))
               FROM market.data_sync_attempts AS completed
               JOIN market.data_sync_targets AS completed_target
                 ON completed_target.target_id=completed.target_id
              WHERE completed.job_id={owner}.job_id::text
                AND completed.status='reconciled')
    )"""

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

    def _bounded_future_tdx(self, row: Sequence[object]) -> bool:
        """Exclude only the bounded raw Go incremental producer, never a date claim alone.

        TDXScheduler._run_tdx_go_api_sync supplies both bounds and disables
        truncate_before. Go rawRows filters BEFORE insert/precision update.
        This proves non-overlap, not job completion or next-month completeness.
        adj_factor and other producers may rewrite history and are not eligible.
        Sync events additionally require the exact owner job and target date.
        """
        if self.source_cutoff is None or len(row) != 10:
            return False
        ledger, _, _, dataset, _, _, _, scope, _, target_date = row
        if ledger not in {'ingestion_jobs', 'data_sync_attempts'}:
            return False
        if dataset not in {'kline_daily_raw', 'kline_minute_raw'} or not isinstance(scope, Mapping):
            return False
        if any(scope.get(key) != value for key, value in {
            'dataset': dataset, 'data_kind': dataset, 'mode': 'incremental', 'via': 'go_init',
        }.items()):
            return False
        for key in ('actual_dataset', 'schedule_dataset'):
            if key in scope and scope[key] not in (None, dataset):
                return False
        if 'datasets' in scope and scope['datasets'] != [dataset]:
            return False
        if 'truncate_before' in scope and scope['truncate_before'] is not False:
            return False
        bounds = [scope.get(key) for key in ('start_date', 'end_date')]
        if any(not isinstance(value, str) for value in bounds):
            return False
        try:
            start, end = (date.fromisoformat(value) for value in bounds)
        except ValueError:
            return False
        if bounds != [start.isoformat(), end.isoformat()] or not self.source_cutoff < start <= end:
            return False
        if ledger == 'data_sync_attempts':
            return type(target_date) is date and start <= target_date <= end
        return target_date is None

    def _empty_future_suspend(self, row: Sequence[object]) -> bool:
        """Recognize only the registered empty BY_DATE replacement contract.

        Zero inserted rows alone is not evidence: replacement can delete rows.
        The SQL also binds an empty_valid completion to the same job and date.
        A single strictly post-cutoff date makes that deletion non-overlapping.
        Positive writes, failures, other producers and unknown scopes stay
        conservative until their own complete write bounds are registered.
        """
        if self.source_cutoff is None or len(row) not in (9, 10) or row[3] != 'suspend_d':
            return False
        ledger, _, status, _, _, _, finished, scope, empty_completion = row[:9]
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

    def _completed_future_suspend(self, row: Sequence[object]) -> bool:
        """Exclude a fully evidenced BY_DATE window strictly after the cutoff.

        Positive rows and multi-day replacements are safe only when the exact
        owner job, all dates and counts close against immutable ledger facts.
        The registered producer validates every row's request date before any
        write; row counts or a post-cutoff target date alone are insufficient.
        """
        if self.source_cutoff is None or len(row) != 10 or row[3] != 'suspend_d':
            return False
        ledger, _, status, _, _, _, finished, scope, completion, target = row
        if completion is not True or not isinstance(scope, Mapping):
            return False
        if ledger == 'ingestion_jobs':
            if status != 'success' or finished is None or target is not None:
                return False
        elif ledger == 'data_sync_attempts':
            if status not in {'started', 'reconciled'} or type(target) is not date:
                return False
            if status == 'reconciled' and finished is None:
                return False
        else:
            return False
        if any(scope.get(key) != value for key, value in {
            'dataset': 'suspend_d', 'actual_dataset': 'suspend_d', 'mode': 'incremental',
        }.items()):
            return False
        schedule, strategy = scope.get('schedule_dataset'), scope.get('date_strategy')
        if not isinstance(schedule, str) or not isinstance(strategy, str):
            return False
        if (schedule, strategy) not in _EMPTY_FUTURE_SUSPEND_SCHEDULES:
            return False
        bounds = [scope.get(key) for key in ('start_date', 'end_date')]
        if any(not isinstance(value, str) for value in bounds):
            return False
        try:
            start, end = (date.fromisoformat(value) for value in bounds)
        except ValueError:
            return False
        if bounds != [start.isoformat(), end.isoformat()] or not self.source_cutoff < start <= end:
            return False
        if bounds != [scope.get('refresh_start_date'), scope.get('refresh_end_date')]:
            return False
        if ledger == 'data_sync_attempts' and not start <= target <= end:
            return False
        stats, inserted = scope.get('stats'), scope.get('inserted_rows')
        if type(inserted) is not int or inserted < 0 or not isinstance(stats, Mapping):
            return False
        if stats.get('dataset') != 'suspend_d' or stats.get('mode') != 'incremental':
            return False
        expected = {'inserted_rows': inserted, 'total_batches': (end-start).days+1,
                    'success_batches': (end-start).days+1, 'failed_batches': 0}
        return all(type(stats.get(key)) is int and stats[key] == value for key, value in expected.items())

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
                           ) AS dataset,
                           job.created_at,job.started_at,job.finished_at,
                           job.summary AS writer_scope,
                           FALSE AS empty_date_completion,NULL::date AS target_date
                      FROM market.ingestion_jobs AS job
                     WHERE job.status IS NULL
                        OR lower(job.status) <> ALL(%s)
                        OR job.finished_at IS NULL
                ), active_sync AS (
                    SELECT 'data_sync_attempts'::text AS ledger_kind,
                           attempt.attempt_id::text AS ledger_identity,
                           lower(attempt.status) AS status,
                           target.dataset,
                           attempt.created_at,attempt.started_at,attempt.finished_at,
                           owner.summary AS writer_scope,
                           FALSE AS empty_date_completion,target.target_date
                      FROM market.data_sync_attempts AS attempt
                      JOIN market.data_sync_targets AS target
                        ON target.target_id=attempt.target_id
                      LEFT JOIN market.ingestion_jobs AS owner
                        ON owner.job_id::text=attempt.job_id
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
                SELECT ledger_kind,ledger_identity,status,dataset,
                       created_at,started_at,finished_at,writer_scope,empty_date_completion,target_date
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
        if len(active) >= 50:
            raise MonthlyRepairJournalError("managed active query reached its bound; scope is incomplete")
        active = [item for item in active if not self._bounded_future_tdx(item)]
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
                f"""
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
                           {_completed_suspend_job_sql('job')} AS empty_date_completion,
                           NULL::date AS target_date
                      FROM market.ingestion_jobs AS job
                     WHERE job.created_at > %s OR job.started_at > %s OR job.finished_at > %s
                ), sync_overlap AS (
                    SELECT 'data_sync_attempts'::text AS ledger_kind,
                           attempt.attempt_id::text AS ledger_identity,
                           lower(attempt.status) AS status,
                           target.dataset,
                           attempt.created_at,attempt.started_at,attempt.finished_at,
                           owner_job.summary AS writer_scope,
                           ({_completed_suspend_job_sql('owner_job')}
                            AND target.target_scope='{{"query_mode":"by_date"}}'::jsonb
                           ) AS empty_date_completion,target.target_date
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
                       created_at,started_at,finished_at,writer_scope,empty_date_completion,target_date
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
            if self._empty_future_suspend(row) or self._completed_future_suspend(row) or self._bounded_future_tdx(row):
                continue
            result.append(
                f"{ledger_kind}:{identity}:{dataset or 'unclassified'}:{str(status).lower()}"
            )
        return tuple(result)


__all__ = (
    "MANAGED_SOURCE_DATASETS",
    "MANAGED_WRITER_SCOPE_POLICY",
    "ManagedRepairImpactJournal",
    "MonthlyRepairJournalError",
    "RepairJournalConnection",
    "TERMINAL_JOB_STATUSES",
)
