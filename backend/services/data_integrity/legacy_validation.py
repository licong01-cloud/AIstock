"""Validate retained legacy completions without inventing a provider scan.

Only the original completed producer job can supply legacy query scope. Current
DB facts are checked now; this never proves a historical vintage or rebinds an
old receipt to a new revision. Reference snapshots keep their dated boundary.
"""
from __future__ import annotations

from datetime import date

from .contracts import CheckCounts, PartitionState, ValidationResult, finite_number
from .provider import value_identity
from .stock_scope import filter_dataset_rows

_DATED = frozenset({'dividend', 'stock_st', 'stock_st_events', 'suspend_d', 'margin_detail', 'etf_share_size'})
_FINANCIAL = frozenset({'tushare_forecast_raw', 'tushare_express_raw', 'tushare_fina_indicator_raw'})
_REFERENCES = frozenset({'stock_basic', 'sw_index_classify', 'sw_index_member', 'etf_basic_snapshots'})
_NUMERIC = frozenset({'numeric', 'bigint', 'integer', 'int', 'float', 'double precision'})


def _unknown(reason, checks):
    return ValidationResult(PartitionState.UNVERIFIED,
        (CheckCounts('contract_check', 1, 0, 0, 0, 0, 1),), checks, reason=reason)


def _complete_job(spec, day, source, job):
    if not source or not job or source.get('data_source') != 'tushare':
        return False
    metadata, summary = source.get('metadata') or {}, job.get('summary') or {}
    stats = summary.get('stats') or {}
    if (source.get('dataset') != spec.name or source.get('trade_date') != day
            or source.get('status') != 'success' or source.get('quality_status') not in {'ok', 'empty_valid'}
            or metadata.get('tushare_api') != spec.tushare_api or metadata.get('mode') != spec.query_mode.value
            or metadata.get('audit_from_target_table') or metadata.get('provider_scan')
            or metadata.get('integrity_observed_revision') is not None
            or any(metadata.get(k) is False for k in ('pagination_complete', 'provider_query_scope_complete'))
            or job.get('status') != 'success' or not source.get('job_id')
            or str(job.get('job_id')) != str(source['job_id'])
            or summary.get('dataset') != spec.name or stats.get('dataset') != spec.name
            or str(stats.get('job_id')) != str(source['job_id'])
            or summary.get('error') or stats.get('error')):
        return False
    try:
        if not date.fromisoformat(summary['start_date']) <= day <= date.fromisoformat(summary['end_date']):
            return False
        total, success, failed = (stats[k] for k in ('total_batches', 'success_batches', 'failed_batches'))
        if (any(type(value) is not int for value in (total, success, failed))
                or total <= 0 or success != total or failed != 0):
            return False
        if spec.name in _FINANCIAL:
            periods = metadata.get('periods')
            if (not isinstance(periods, list) or not periods or periods != stats.get('periods')
                    or metadata.get('date_column') != spec.date_column
                    or metadata.get('sparse_event_dataset') is not True
                    or total != len(set(periods))):
                return False
            for period in periods:
                value_identity(period, 'date')
        elif spec.name == 'etf_basic_snapshots':
            if total != len(spec.single_call_param_sets):
                return False
        elif spec.name not in _DATED:
            return False
        count = source.get('row_count')
        if type(count) is not int or count < 0:
            return False
        # A legacy single-date query at its cap is not proof of pagination.
        if spec.name in _DATED and spec.row_limit > 0 and count >= spec.row_limit:
            return False
    except (KeyError, TypeError, ValueError):
        return False
    return True


def legacy_completion_result(spec, day, revision, source, job, actual):
    """Return current-content validation, preserving any unproven source scope.

    Revision zero is the pre-tracking compatibility case. A changed tracked
    partition must use its normal revision-bound source contract instead.
    """
    facts = filter_dataset_rows(spec, actual)
    checks = {'physical_rows': len(facts), 'physical_rows_before_scope_filter': len(actual),
        'coverage_basis': 'completed_legacy_job_current_database_partition',
        'source_observed_at': str(source.get('refreshed_at')) if source else None,
        'legacy_job_id': str(source.get('job_id')) if source and source.get('job_id') else None,
        'historical_vintage_proven': False, 'provider_scan_reconstructed': False,
        'source_receipt_rewritten': False}
    invalid, identities = [], set()
    for position, row in enumerate(facts):
        try:
            key = tuple(value_identity(row.get(field), spec.columns[field]) for field in spec.primary_keys)
            if (any(value in (None, '') and field not in spec.nullable_identity_columns
                    for field, value in zip(spec.primary_keys, key)) or key in identities):
                raise ValueError('missing or duplicate physical identity')
            identities.add(key)
            if spec.name not in _REFERENCES and value_identity(row.get(spec.date_column), 'date') != str(day):
                raise ValueError('physical date differs')
            if spec.name == 'etf_basic_snapshots' and value_identity(row.get('snapshot_date'), 'date') != str(day):
                raise ValueError('snapshot date differs')
            if any(value is not None and kind in _NUMERIC and not finite_number(value)
                    for field, kind in spec.columns.items() for value in [row.get(field)]):
                raise ValueError('non-finite supplied number')
        except (KeyError, TypeError, ValueError, ArithmeticError):
            invalid.append({'row': position, 'symbol': str(row.get('ts_code', 'UNKNOWN'))})
    physical = CheckCounts('physical_fact_quality', len(facts), len(facts)-len(invalid), 0, 0, len(invalid), 0)
    if invalid:
        return ValidationResult(PartitionState.INCOMPLETE, (physical,), checks,
            ({'classification': 'INVALID', 'physical_keys': invalid},), reason='legacy_physical_fact_invalid')
    if spec.name in _REFERENCES - {'etf_basic_snapshots'}:
        # Current reference rows are useful facts, but not an old dated snapshot.
        return _unknown('historical_reference_scope_unproven', checks)
    if revision != 0 or not _complete_job(spec, day, source, job):
        return _unknown('historical_reference_scope_unproven' if spec.name in _REFERENCES
                        else 'historical_source_query_unproven', checks)
    expected = source['row_count']
    if len(actual) > expected:
        return _unknown('legacy_partition_count_changed', checks)
    missing = expected-len(actual)
    coverage = CheckCounts('legacy_completed_source_rows', expected, len(actual), 0, missing, 0, 0)
    if missing:
        return ValidationResult(PartitionState.INCOMPLETE, (coverage, physical), checks,
            ({'classification': 'MISSING', 'count': missing, 'symbol_identity': 'UNKNOWN'},))
    if spec.min_expected_rows is not None and len(actual) < spec.min_expected_rows:
        return ValidationResult(PartitionState.INCOMPLETE, (coverage, physical), checks,
            reason='legacy_source_quality_below_existing_contract')
    if spec.name == 'etf_share_size':
        from ..tushare_sync_engine import _etf_share_size_publication_quality
        publication, quality = _etf_share_size_publication_quality(facts)
        checks['publication_quality'] = publication
        if quality['quality_status'] != 'ok':
            return ValidationResult(PartitionState.INCOMPLETE, (coverage, physical), checks,
                reason='required_source_field_unpublished')
    if spec.name == 'suspend_d':
        from backend.data_service.suspension_day_authority import publication_quality
        publication, quality = publication_quality(facts, day)
        checks['suspension_day_authority'] = publication
        if quality['quality_status'] not in {'ok', 'empty_valid'}:
            return ValidationResult(PartitionState.INCOMPLETE, (coverage, physical), checks,
                reason='suspension_semantics_unresolved')
    if spec.name in _FINANCIAL:
        checks['query_periods'] = source['metadata']['periods']
    return ValidationResult(PartitionState.VALID, (coverage, physical,
        CheckCounts('completed_legacy_query', 1, 1, 0, 0, 0, 0)), checks)


def validate_legacy_partition(conn, obligation, day, revision, observed, *, read):
    """One bounded query for facts and one for the exact retained producer job."""
    from psycopg2 import sql
    from backend.services.tushare_dataset_specs import DATASET_REGISTRY
    from .provider import announcement_spec
    dataset = obligation.dataset
    if dataset not in _DATED | _FINANCIAL | _REFERENCES | {'anns_metadata'}:
        return None
    # Never let an old completion mask a new failed/partial/bound observation.
    source = observed[0] if observed else None
    if revision != 0 or source and ((source.get('metadata') or {}).get('integrity_observed_revision') is not None
                   or (source.get('metadata') or {}).get('provider_scan')):
        return None
    if source and source.get('status') != 'success':
        return ValidationResult(PartitionState.WAIT_UPSTREAM, (),
            {'provider_observation': source.get('data_source')},
            reason=source.get('failure_category') or 'source_scan_failed')
    if source and source.get('quality_status') not in {'ok', 'empty_valid'}:
        return ValidationResult(PartitionState.INCOMPLETE, (),
            {'provider_quality': source.get('quality_status')},
            reason=source.get('failure_category') or 'source_quality_incomplete')
    spec = announcement_spec() if dataset == 'anns_metadata' else DATASET_REGISTRY[dataset]
    query = sql.SQL('SELECT {} FROM {}').format(
        sql.SQL(',').join(sql.Identifier(field) for field in spec.columns),
        sql.Identifier(*spec.target_table.split('.')))
    if dataset in _REFERENCES and spec.snapshot_date_column is None:
        actual = read(conn, query)
    else:
        actual = read(conn, query+sql.SQL(' WHERE {}=%s').format(sql.Identifier(spec.date_column)), (day,))
    jobs = []
    if source and source.get('job_id'):
        jobs = read(conn, 'SELECT job_id,status,summary FROM market.ingestion_jobs WHERE job_id=%s',
                    (source['job_id'],))
    return legacy_completion_result(spec, day, revision, source, jobs[0] if jobs else None, actual)
