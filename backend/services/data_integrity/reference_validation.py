"""Retrospective reference checks without backdating a provider observation.

An ETF snapshot is physically dated. Mutable security/classification masters
are not: a completed old job cannot reconstruct their old identity set, names
or classification vintage. Keep that limitation explicit rather than querying
today's provider and relabelling the response as a historical snapshot.
"""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from psycopg2 import sql

from .contracts import CheckCounts, PartitionState, ValidationResult
from .legacy_validation import legacy_completion_result

_REFERENCES = frozenset({'stock_basic', 'sw_index_classify', 'sw_index_member', 'etf_basic_snapshots'})
_SHANGHAI = ZoneInfo('Asia/Shanghai')


def _complete_snapshot_job(spec, day, job):
    """Require the real capture day, exact scope and all native query batches.

    inserted_rows is the native single-call upsert affected-row count, not a
    general source row_count. Only the retained full initialization of a dated
    snapshot may use it as its physical cardinality cross-check.
    """
    summary = job.get('summary') or {}
    stats = summary.get('stats') or {}
    try:
        created, finished = job['created_at'], job['finished_at']
        return (job.get('status') == 'success' and bool(job.get('job_id'))
            and summary.get('dataset') == stats.get('dataset') == spec.name
            and str(job['job_id']) == str(stats.get('job_id'))
            and not summary.get('error') and not stats.get('error')
            and stats.get('mode') == 'init'
            and summary.get('start_date') == summary.get('end_date') == day.isoformat()
            and isinstance(created, datetime) and created.tzinfo is not None
            and isinstance(finished, datetime) and finished.tzinfo is not None
            and created <= finished
            and created.astimezone(_SHANGHAI).date() == finished.astimezone(_SHANGHAI).date() == day
            and all(type(stats.get(k)) is int for k in
                ('total_batches', 'success_batches', 'failed_batches', 'inserted_rows'))
            and stats['total_batches'] == stats['success_batches'] == len(spec.single_call_param_sets)
            and stats['total_batches'] > 0 and stats['failed_batches'] == 0
            and stats['inserted_rows'] > 0)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def reference_result(spec, day, revision, jobs, actual):
    # Reuse the formal physical identity/numeric validator; no second schema.
    physical = legacy_completion_result(spec, day, revision, None, None, actual)
    checks = {**physical.checks, 'coverage_basis': 'retained_dated_reference_snapshot',
        'requested_reference_date': str(day), 'current_master_is_historical_snapshot': False}
    if physical.state == PartitionState.INCOMPLETE:
        return ValidationResult(physical.state, physical.counts, checks, physical.gaps, physical.reason)
    size = physical.checks['physical_rows']
    quality = (CheckCounts('physical_fact_quality', size, size, 0, 0, 0, 0),)
    if not spec.snapshot_date_column:
        checks['coverage_basis'] = 'current_mutable_reference_not_a_dated_snapshot'
        return ValidationResult(PartitionState.UNVERIFIED, quality, checks,
            reason='historical_reference_snapshot_not_retained')
    # A post-tracking mutation requires its own normal revision-bound scan.
    dated = [j for j in jobs if isinstance(j.get('created_at'), datetime)
        and j['created_at'].tzinfo is not None and j['created_at'].astimezone(_SHANGHAI).date() == day]
    # Select the latest native attempt BEFORE checking success: a failed or
    # partial later overwrite must not be hidden by an earlier convenient job.
    job = max(dated, key=lambda j: j['created_at']) if dated else None
    if revision != 0 or job is None or not _complete_snapshot_job(spec, day, job):
        return ValidationResult(PartitionState.UNVERIFIED, quality, checks,
            reason='historical_reference_capture_job_unproven')
    expected = job['summary']['stats']['inserted_rows']
    checks.update(legacy_job_id=str(job['job_id']), source_observed_at=job['finished_at'].isoformat())
    found = len(actual)
    count = CheckCounts('dated_snapshot_rows', expected, min(found, expected), 0,
                        max(expected-found, 0), 0, 0)
    if found > expected:
        return ValidationResult(PartitionState.UNVERIFIED, quality, checks,
            reason='historical_reference_snapshot_changed')
    if found < expected:
        return ValidationResult(PartitionState.INCOMPLETE, (count,)+quality, checks,
            ({'classification': 'MISSING', 'count': expected-found, 'symbol_identity': 'UNKNOWN'},))
    return ValidationResult(PartitionState.VALID, (count,)+quality, checks)


def validate_reference_partition(conn, obligation, day: date, revision: int, observed, *, read):
    if obligation.dataset not in _REFERENCES or observed or revision != 0:
        # Existing source failures and tracked revisions retain their native
        # semantics. Never let a historical job mask a newer failed scan.
        return None
    from backend.services.tushare_dataset_specs import DATASET_REGISTRY
    spec = DATASET_REGISTRY[obligation.dataset]
    query = sql.SQL('SELECT {} FROM {}').format(
        sql.SQL(',').join(sql.Identifier(field) for field in spec.columns),
        sql.Identifier(*spec.target_table.split('.')))
    if spec.snapshot_date_column:
        actual = read(conn, query+sql.SQL(' WHERE {}=%s').format(sql.Identifier(spec.snapshot_date_column)), (day,))
        jobs = read(conn, """SELECT job_id,status,summary,created_at,finished_at
            FROM market.ingestion_jobs WHERE summary->>'dataset'=%s AND summary->>'start_date'=%s
              AND summary->>'end_date'=%s ORDER BY created_at DESC""",
            (spec.name, str(day), str(day)))
    else:
        actual, jobs = read(conn, query), []
    return reference_result(spec, day, revision, jobs, actual)
