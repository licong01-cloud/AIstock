"""Raw event acquisition contracts; no business consumers or factor library."""
import datetime as dt
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pytest

from backend.services.tushare_dataset_specs import DATASET_REGISTRY

EVENTS = ('stk_shock', 'stk_high_shock', 'stk_alert', 'idx_anns', 'stk_seasoned')


@pytest.mark.parametrize('name', EVENTS)
def test_event_specs_are_optional_natural_date_audit_cursors(name):
    spec = DATASET_REGISTRY[name]
    assert spec.required_for_daily_complete is False
    assert spec.incremental_cursor_from_audit is True
    assert spec.trading_day_only is False
    assert spec.date_column == 'request_date'
    assert spec.rate_per_minute <= (50 if name == 'idx_anns' else 60)


def test_alert_preserves_real_dates_and_unknown_release_time():
    from backend.services.tushare_event_inputs import observation
    row = {'ts_code': '000001.SZ', 'name': '样例', 'start_date': '20261030',
           'end_date': '20261113', 'type': '重点监控'}
    fact = observation('stk_alert', row, dt.date(2026, 10, 30))
    assert fact['event_date'] == dt.date(2026, 10, 30)
    assert fact['raw_payload'] == row
    assert 'trade_date' not in fact['raw_payload']
    assert fact['published_at'] is None


def test_same_stock_day_multiple_reasons_are_distinct_versions():
    from backend.services.tushare_event_inputs import observation
    row = {'ts_code': '000001.SZ', 'name': '样例', 'trade_date': '20261030',
           'trade_market': '深交所', 'reason': '原因一', 'period': '3日'}
    day = dt.date(2026, 10, 30)
    a = observation('stk_shock', row, day)
    b = observation('stk_shock', {**row, 'reason': '原因二'}, day)
    assert a['source_row_sha256'] != b['source_row_sha256']
    assert a['source_row_sha256'] == observation('stk_shock', dict(reversed(list(row.items()))), day)['source_row_sha256']


def test_seasoned_unknown_project_and_raw_units_are_not_invented():
    from backend.services.tushare_event_inputs import observation
    spec = DATASET_REGISTRY['stk_seasoned']
    row = dict.fromkeys(spec.columns)
    row.update(ts_code='000001.SZ', ann_date='20261030', fo_vol_act='100000.00', fo_raise_total_act='1250000.00')
    value = observation('stk_seasoned', row, dt.date(2026, 10, 30))
    assert value['source_record_key'] == 'UNKNOWN'
    assert value['raw_payload']['fo_vol_act'] == '100000.00'
    assert value['raw_payload']['fo_raise_total_act'] == '1250000.00'


@pytest.mark.parametrize('code', ['920001.BJ', '200001.SZ', '900901.SH', '513310.SH'])
def test_only_supported_equity_events_are_stored(code):
    from backend.services.tushare_event_inputs import accepted_rows
    row = {'ts_code': code, 'name': '非沪深A股', 'start_date': '20261030', 'end_date': None, 'type': '提示'}
    assert accepted_rows('stk_alert', [row], dt.date(2026, 10, 30), dt.date(2026, 10, 30)) == []


def test_field_and_date_errors_fail_not_empty_success():
    from backend.services.tushare_event_inputs import accepted_rows
    with pytest.raises(ValueError):
        accepted_rows('idx_anns', [{'ann_date': '20261030'}], dt.date(2026, 10, 30), dt.date(2026, 10, 30))
    with pytest.raises(ValueError):
        accepted_rows('idx_anns', [dict(ann_date='20261029', title='t', url='u', source='中证指数', type=None)],
                      dt.date(2026, 10, 30), dt.date(2026, 10, 30))


def announcement(day, title='t', source='中证指数'):
    return dict(ann_date=day.strftime('%Y%m%d'), title=title, url='https://example.org/'+title, source=source, type=None)


def test_caps_split_dates_before_sharding():
    from backend.services.tushare_event_inputs import EventFetcher
    spec = replace(DATASET_REGISTRY['idx_anns'], row_limit=2)
    left, right = dt.date(2026, 10, 30), dt.date(2026, 10, 31)
    fetcher = EventFetcher(Mock(), Mock(), spec, 'job')
    fetcher.request = Mock(side_effect=[[announcement(left), announcement(right)],
                                       [announcement(left)], [announcement(right)]])
    result = list(fetcher.windows(left, right))
    assert [(a, b) for a, b, _, _ in result] == [(left, left), (right, right)]
    assert fetcher.request.call_count == 3


def test_capped_single_day_shards_sources_and_rejects_ignored_filter():
    from backend.services.tushare_event_inputs import EventFetcher, INDEX_SOURCES
    day = dt.date(2026, 10, 30)
    fetcher = EventFetcher(Mock(), Mock(), replace(DATASET_REGISTRY['idx_anns'], row_limit=2), 'job')
    entries = [announcement(day, source=source) for source in INDEX_SOURCES]
    fetcher.request = Mock(side_effect=[entries[:2], *[[row] for row in entries]])
    result = list(fetcher.windows(day, day))
    assert len(result[0][2]) == 4
    assert result[0][3]['complete_shard_count'] == 4
    fetcher.request = Mock(side_effect=[entries[:2], [entries[1]]])
    with pytest.raises(ValueError, match='ignored exact source'):
        list(fetcher.windows(day, day))


def test_single_day_shard_cap_never_becomes_success():
    from backend.services.tushare_event_inputs import EventFetcher
    day = dt.date(2026, 10, 30)
    fetcher = EventFetcher(Mock(), Mock(), replace(DATASET_REGISTRY['idx_anns'], row_limit=1), 'job')
    fetcher.request = Mock(return_value=[announcement(day)])
    with pytest.raises(ValueError, match='still reaches cap'):
        list(fetcher.windows(day, day))


def test_transport_failure_is_finite_and_not_zero_records(monkeypatch):
    from backend.services.tushare_event_inputs import EventFetcher
    import backend.services.tushare_sync_engine as engine_module
    from backend.services import tushare_event_inputs as events
    monkeypatch.setattr(events.time, 'sleep', lambda _: None)
    request = Mock(side_effect=RuntimeError('upstream failed'))
    monkeypatch.setattr(engine_module, '_query_tushare_dataapi', request)
    fetcher = EventFetcher(Mock(), Mock(), DATASET_REGISTRY['stk_shock'], 'job')
    with pytest.raises(RuntimeError, match='after 3 attempts'):
        fetcher.request({'trade_date': '20261030'})
    assert request.call_count == 3 and fetcher.attempts == 3


def test_job_budget_never_calls_provider(monkeypatch):
    from backend.services.tushare_event_inputs import EventFetcher, EventBudgetWait
    import backend.services.tushare_sync_engine as engine_module
    request = Mock()
    monkeypatch.setattr(engine_module, '_query_tushare_dataapi', request)
    fetcher = EventFetcher(Mock(), Mock(), DATASET_REGISTRY['idx_anns'], 'job')
    fetcher.attempts = 800
    with pytest.raises(EventBudgetWait):
        fetcher.request({})
    request.assert_not_called()


def test_index_attempts_claim_budget_before_each_retry(monkeypatch):
    from backend.services import tushare_event_inputs as events
    import backend.services.tushare_sync_engine as engine_module
    monkeypatch.setattr(events.time, 'sleep', lambda _: None)
    claim = Mock()
    monkeypatch.setattr(events, 'claim_index_request', claim)
    request = Mock(side_effect=[RuntimeError('429'), []])
    monkeypatch.setattr(engine_module, '_query_tushare_dataapi', request)
    fetcher = events.EventFetcher(Mock(), Mock(), DATASET_REGISTRY['idx_anns'], 'job')
    assert fetcher.request({}) == []
    assert request.call_count == claim.call_count == 2


def test_event_transaction_rolls_back_before_returning_to_autocommit():
    from backend.services.tushare_event_inputs import transaction
    conn = Mock(autocommit=True)
    with pytest.raises(RuntimeError), transaction(conn):
        assert conn.autocommit is False
        raise RuntimeError('injected')
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    assert conn.autocommit is True


def test_incremental_cursor_is_api_specific_with_pending_old_targets():
    from backend.services.tushare_event_inputs import incremental_start
    conn, cursor = Mock(), Mock()
    conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
    conn.cursor.return_value.__exit__ = Mock(return_value=False)
    cursor.fetchone.return_value = (dt.date(2026, 11, 5),)
    engine = Mock(_late_recheck_dates={})
    engine._unclosed_publication_dates.return_value = [dt.date(2026, 2, 1)]
    start, details = incremental_start(engine, conn, DATASET_REGISTRY['stk_alert'], dt.date(2026, 11, 6))
    assert start == dt.date(2026, 10, 31)
    assert 'source_api' not in cursor.execute.call_args[0][0]  # per-dataset source audit, never shared table MAX
    assert cursor.execute.call_args[0][1][0] == 'stk_alert'
    assert engine._late_recheck_dates['stk_alert'] == [dt.date(2026, 2, 1)]
    assert details['event_source_cursor_only']


def test_event_jobs_do_not_add_daily_obligations_and_all_schedules_are_registered():
    from backend.services.data_integrity.catalog import obligations, optional_input_contract
    from backend.db.init_tushare_schedules import get_default_schedule_catalog
    catalogue = get_default_schedule_catalog()
    assert catalogue['complete']
    assert not set(EVENTS) & {o.dataset for o in obligations()[0]}
    for name in EVENTS:
        assert optional_input_contract(name).category == 'event'
        primary = next(t for t in catalogue['templates'] if t['dataset'] == name)
        assert primary['options']['skip_auto_range'] is True
    root = Path(__file__).resolve().parents[2]
    page = (root/'frontend/src/app/local-data/page.tsx').read_text(encoding='utf-8')
    assert page.count('EVENT_INPUT_DATASETS') >= 8
    sql = (root/'backend/db/migrations/add_tushare_event_inputs_v1.sql').read_text(encoding='utf-8')
    assert all(name in sql for name in EVENTS)
    assert "frequency,FALSE,options::jsonb" in sql  # old runtime never executes an unknown new name


def test_generic_sync_and_scheduler_project_review_option_is_explicit():
    import inspect
    from backend.services.tushare_sync_engine import TushareSyncEngine
    assert inspect.signature(TushareSyncEngine.sync).parameters['review_open_projects'].default is False


@pytest.mark.parametrize('dataset', EVENTS)
def test_init_requires_real_explicit_window_before_job_creation(dataset):
    from backend.routers.ingestion import IngestionRunRequest, trigger_ingestion_run
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as error:
        trigger_ingestion_run(IngestionRunRequest(dataset=dataset, mode='init', options={}))
    assert error.value.status_code == 400 and 'requires start_date' in error.value.detail


@pytest.mark.parametrize('alias,dataset', [('_stk_shock_morning', 'stk_shock'),
    ('_stk_high_shock_morning', 'stk_high_shock'), ('_stk_alert_morning', 'stk_alert'),
    ('_idx_anns_morning', 'idx_anns'), ('_stk_seasoned_morning', 'stk_seasoned'),
    ('_stk_seasoned_project_review', 'stk_seasoned')])
def test_secondary_schedules_share_same_overlap_owner(monkeypatch, alias, dataset):
    from backend.ingestion.tdx_scheduler import TDXScheduler
    from types import SimpleNamespace
    instance = object.__new__(TDXScheduler)
    observed = []
    instance._tracker = SimpleNamespace(is_running=lambda key: observed.append(key) or True)
    monkeypatch.setattr(instance, '_update_ingestion_schedule', lambda *_a, **_kw: None)
    monkeypatch.setattr(instance, '_next_run_for', lambda *_: None)
    instance._scheduled_ingestion_run('test', alias, 'incremental', {'skip_auto_range': True})
    assert observed == [f'ingestion:{dataset}:incremental']


def test_successful_sparse_zero_days_are_query_covered_not_missing_events(monkeypatch):
    from backend.routers import ingestion
    day = dt.date(2026, 10, 31)
    monkeypatch.setattr(ingestion, '_fetchall', lambda *_: [dict(trade_date=day, status='success',
        metadata={'event_scan': {'complete': True, 'parameters': {'start_date': '20261031', 'end_date': '20261031'}}})])
    result = ingestion.get_data_gaps(data_kind='idx_anns', start_date=str(day), end_date=str(day), refresh=True)
    assert result['status'] == 'QUERY_COVERED'
    assert result['covered_query_days'] == 1 and result['missing_event_count'] is None
    assert result['historical_PIT'] is False


def test_failed_source_window_keeps_retry_target_and_stops_no_false_empty(monkeypatch):
    from backend.services import tushare_event_inputs as events
    day = dt.date(2026, 10, 30)
    engine = Mock(_late_recheck_dates={})
    engine._upsert_sync_target.return_value = 'target'
    monkeypatch.setattr(events.EventFetcher, 'windows', Mock(side_effect=RuntimeError('source failed')))
    result = events.sync_events(engine, Mock(), DATASET_REGISTRY['stk_alert'], day, day, 'job')
    assert result.ok is False and result.failed_batches == 1
    engine._refresh_audit.record_success.assert_not_called()
    engine._refresh_audit.record_failure.assert_called_once()
    assert engine._record_sync_attempt.call_args[0] == ('target', 'retry')
    assert engine._record_sync_attempt.call_args.kwargs['error_message'] == 'source failed'
    assert engine._record_sync_attempt.call_args.kwargs['context']['failure_category'] == 'event_query_incomplete'
    assert engine._upsert_sync_target.call_args.kwargs['metadata']['retry_intent'] == 'historical_event_backfill'


@pytest.mark.parametrize('tamper', [False, True])
def test_empty_current_query_never_erases_stored_row_count_and_history_is_verified(monkeypatch, tamper):
    from backend.services.tushare_event_inputs import observation, validate_partition
    from backend.services.data_integrity import validation
    day = dt.date(2026, 10, 30)
    raw = announcement(day)
    key = observation('idx_anns', raw, day)['source_row_sha256']
    physical = dict(source_row_sha256=key, raw_payload={**raw, 'title': 'tampered'} if tamper else raw,
                    event_date=day, ts_code=None, published_at=None,
                    publication_time_quality='UNKNOWN', vintage_quality='NON_VINTAGE')
    observations = Mock(side_effect=[[
        {'status': 'success', 'metadata': {'integrity_observed_revision': 1,
            'event_scan': {'schema_version': 'tushare_event_scan_v1', 'complete': True,
                           'source_rows_sha256': [], 'observed_rows': 0}}}], [physical]])
    monkeypatch.setattr(validation, 'rows', observations)
    result = validate_partition(Mock(), DATASET_REGISTRY['idx_anns'], day, 1)
    assert result.checks['physical_rows'] == 1 and result.checks['provider_observed_rows'] == 0
    assert result.state == ('INCOMPLETE' if tamper else 'VALID')


@pytest.mark.parametrize('name', ('stk_shock', 'stk_high_shock', 'stk_alert'))
def test_risk_default_history_starts_at_shared_training_boundary(name):
    assert DATASET_REGISTRY[name].initial_start_date == '2018-08-01'


@pytest.mark.parametrize('name', EVENTS)
def test_explicit_event_history_is_not_clipped_by_default_cursor_floor(monkeypatch, name):
    from backend.services import tushare_event_inputs as events
    # A default cursor is not proof that older source facts cannot exist.
    day = dt.date(2025, 7, 14)
    spec = replace(DATASET_REGISTRY[name], initial_start_date='2026-01-01')
    engine = Mock(_late_recheck_dates={})
    engine._upsert_sync_target.return_value = 'target'
    fetch = Mock(return_value=iter([(day, day, [], {'start_date': '20250714', 'end_date': '20250714'})]))
    monkeypatch.setattr(events.EventFetcher, 'windows', fetch)
    store = Mock(return_value=(0, 0))
    monkeypatch.setattr(events, 'store_day', store)
    result = events.sync_events(engine, Mock(), spec, day, day, 'job')
    fetch.assert_called_once_with(day, day)
    store.assert_called_once()
    assert result.ok and result.success_batches == result.total_batches == 1
    assert result.inserted_rows == 0  # query-covered empty is not a fabricated event


def test_event_incremental_without_cursor_uses_training_floor():
    from backend.services.tushare_event_inputs import incremental_start
    conn, cursor = Mock(), Mock()
    conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
    conn.cursor.return_value.__exit__ = Mock(return_value=False)
    cursor.fetchone.return_value = (None,)
    engine = Mock(_late_recheck_dates={})
    engine._unclosed_publication_dates.return_value = []
    start, details = incremental_start(engine, conn, DATASET_REGISTRY['stk_alert'], dt.date(2026, 11, 6))
    assert start == dt.date(2018, 8, 1)
    assert details['safe_audit_cursor_date'] is None


def test_event_late_queue_ignores_validation_only_and_completed_query_proofs():
    from backend.services.tushare_sync_engine import TushareSyncEngine
    conn, cursor = Mock(), Mock()
    conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
    conn.cursor.return_value.__exit__ = Mock(return_value=False)
    cursor.fetchall.return_value = [(dt.date(2024, 4, 1),)]
    assert TushareSyncEngine._unclosed_publication_dates(conn, DATASET_REGISTRY['stk_alert'],
        dt.date(2026, 11, 7)) == [dt.date(2024, 4, 1)]
    query = cursor.execute.call_args.args[0]
    assert "VALIDATE_ONLY" in query and "event_scan" in query
    assert "integrity_observed_revision" in query and "data_revision" in query
    assert "LIMIT 5" in query and "tushare_risk_event_raw" not in query


def shared_row(dataset='stk_alert', *, revision=0):
    from backend.data_service import shared_tushare_inputs as shared
    day = dt.date(2026, 10, 30)
    stamp = dt.datetime(2026, 11, 7, 1, 0, tzinfo=dt.timezone.utc)
    if dataset == 'stk_alert':
        raw = dict(ts_code='000001.SZ', name='test', start_date='20261030', end_date=None, type=str(revision))
    elif dataset == 'vix_index':
        raw = dict(trade_date='20261030', close='19.25', pct_change='-1.3')
    elif dataset == 'stock_daily_afterhours':
        raw = dict(trade_date='20261030', ts_code='000001.SZ', ah_vol=None, ah_amount='0')
    else:
        raise AssertionError('unexpected test dataset')
    import hashlib
    return dict(source_api=DATASET_REGISTRY[dataset].tushare_api, event_date=day,
                ts_code=raw.get('ts_code'), raw_payload=raw,
                source_row_sha256=hashlib.sha256(shared.payload_bytes(raw)).hexdigest(),
                captured_at=stamp, last_observed_at=stamp, published_at=None,
                publication_time_quality='UNKNOWN', vintage_quality='NON_VINTAGE', source_record_key='UNKNOWN')


def mock_shared_snapshot(monkeypatch, rows=None):
    from contextlib import contextmanager
    from backend.data_service import shared_tushare_inputs as shared
    import pyarrow as pa
    entries = rows if rows is not None else [shared_row(), shared_row(revision=1)]
    conn = Mock()

    @contextmanager
    def snapshot():
        yield conn

    monkeypatch.setattr(shared, 'capture_read_snapshot', snapshot)

    def batches(actual, dataset, *_args, **_kwargs):
        assert actual is conn
        values = [shared._flatten(dataset, x) for x in entries]
        yield pa.Table.from_pylist(values, schema=shared._schema(dataset))

    monkeypatch.setattr(shared, '_batches', batches)
    monkeypatch.setattr(shared, '_coverage', lambda *_a: {'successful_query_days': 1,
                                                       'semantics': 'CURRENT_LEDGER_NOT_HISTORICAL_PIT'})
    return shared


@pytest.mark.parametrize('dataset', ('vix_index', 'stock_daily_afterhours', 'stk_alert'))
def test_shared_source_contract_and_raw_projection_preserve_values(dataset):
    from backend.data_service import shared_tushare_inputs as shared
    row = shared_row(dataset)
    result = shared._flatten(dataset, row)
    assert result['source_row_sha256'] == row['source_row_sha256']
    assert result['published_at'] is None and result['source_record_key'] == 'UNKNOWN'
    assert shared.input_contract(dataset)['historical_PIT_proven'] is False
    assert shared.input_contract(dataset)['pit_filter_applied'] is False
    if dataset == 'stock_daily_afterhours':
        assert result['ah_vol'] is None and result['ah_amount'] == 0
        assert shared.input_contract(dataset)['units']['ah_amount'] == 'CNY_1000'


@pytest.mark.parametrize('change', ('hash', 'date', 'symbol', 'publication', 'capture', 'payload'))
def test_shared_source_projection_rejects_real_identity_or_clock_errors(change):
    from backend.data_service import shared_tushare_inputs as shared
    row = shared_row()
    if change == 'hash':
        row['source_row_sha256'] = '0'*64
    elif change == 'date':
        row['event_date'] = dt.date(2026, 10, 29)
    elif change == 'symbol':
        row['ts_code'] = '000002.SZ'
    elif change == 'publication':
        row['published_at'] = row['captured_at']
    elif change == 'capture':
        row['captured_at'] = row['captured_at'].replace(tzinfo=None)
    else:
        row['raw_payload']['invented'] = 'x'
    with pytest.raises(ValueError):
        shared._flatten('stk_alert', row)


@pytest.mark.parametrize('args', [
    ('unknown', '2026-10-30', '2026-10-30', None, None),
    ('stk_alert', '2026103', '2026-10-30', None, None),
    ('stk_alert', '2026-10-31', '2026-10-30', None, None),
    ('vix_index', '2026-10-30', '2026-10-30', ['000001.SZ'], None),
    ('idx_anns', '2026-10-30', '2026-10-30', [], None),
    ('stk_alert', '2026-10-30', '2026-10-30', ['920305.BJ'], None),
    ('stk_alert', '2026-10-30', '2026-10-30', '000001.SZ', None),
    ('stk_alert', '2026-10-30', '2026-10-30', None, dt.datetime(2026, 11, 7)),
])
def test_shared_requests_reject_ambiguous_scope_or_time(args):
    from backend.data_service import shared_tushare_inputs as shared
    with pytest.raises(ValueError):
        shared._request(*args)


def test_shared_read_preserves_all_revisions_not_business_dedup(monkeypatch):
    shared = mock_shared_snapshot(monkeypatch)
    result = shared.read_history('stk_alert', '2026-10-30', '2026-10-30')
    assert len(result.data) == result.metadata['rows'] == 2
    assert result.data['ts_code'].nunique() == 1
    assert result.metadata['vintage_quality'] == 'NON_VINTAGE'


def test_shared_source_sql_preserves_empty_code_filter_and_capture_cutoff():
    from backend.data_service import shared_tushare_inputs as shared
    conn, cur = Mock(), Mock()
    conn.cursor.return_value.__enter__ = Mock(return_value=cur)
    conn.cursor.return_value.__exit__ = Mock(return_value=False)
    cur.fetchmany.return_value = []
    stamp = dt.datetime(2026, 11, 6, tzinfo=dt.timezone.utc)
    day = dt.date(2026, 10, 30)
    assert list(shared._batches(conn, 'stk_alert', day, day, [], stamp)) == []
    statement, params = cur.execute.call_args[0]
    assert 'captured_at<=%s' in str(statement) and 'ts_code=ANY(%s)' in str(statement)
    assert params == ['stk_alert', day, day, [], stamp]


def test_shared_bundle_roundtrip_uses_same_snapshot_and_keeps_source_warning(monkeypatch, tmp_path):
    import json
    shared = mock_shared_snapshot(monkeypatch)
    source = tmp_path/'quality.json'
    source.write_text(json.dumps({'status': 'WITH_WARNING', 'unknown_upstream_records': [{'ann_date': '2024928 '}]}))
    root = tmp_path/'v1'
    result = shared.export_bundle(root, '2026-10-30', '2026-10-30', datasets=['stk_alert'], quality_report=source)
    read = shared.read_bundle(root, 'stk_alert', expected_manifest_sha256=result['manifest_sha256'])
    assert len(read.data) == 2 and result['source_warning_count'] == 1
    assert result['database_write'] is False and result['active_profile_write'] is False
    assert shared.verify_bundle(root)['datasets'] == {'stk_alert': 2}
    with pytest.raises(FileExistsError):
        shared.export_bundle(root, '2026-10-30', '2026-10-30', datasets=['stk_alert'])
    with pytest.raises(ValueError):
        shared.read_bundle(root, 'stk_alert', expected_manifest_sha256='0'*64)


@pytest.mark.parametrize('damage', ('file', 'manifest', 'missing', 'traversal', 'schema_metadata'))
def test_shared_bundle_rejects_file_or_manifest_drift(monkeypatch, tmp_path, damage):
    import json
    shared = mock_shared_snapshot(monkeypatch)
    root = tmp_path/'v1'
    shared.export_bundle(root, '2026-10-30', '2026-10-30', datasets=['stk_alert'])
    if damage == 'file':
        with (root/'stk_alert.parquet').open('ab') as stream:
            stream.write(b'tamper')
    elif damage == 'missing':
        (root/'stk_alert.parquet').unlink()
    else:
        path = root/'manifest.json'
        manifest = json.loads(path.read_text())
        if damage == 'manifest':
            manifest['created_at'] = 'fake'
        elif damage == 'traversal':
            manifest['datasets']['stk_alert']['file'] = '../outside.parquet'
            manifest['manifest_digest'] = shared._digest({k: v for k, v in manifest.items() if k != 'manifest_digest'})
        else:
            manifest['datasets']['stk_alert']['units'] = {'provider_fields': 'invented_units'}
            manifest['manifest_digest'] = shared._digest({k: v for k, v in manifest.items() if k != 'manifest_digest'})
        path.write_text(json.dumps(manifest))
    with pytest.raises((ValueError, FileNotFoundError)):
        shared.read_bundle(root, 'stk_alert')


def test_shared_api_is_a_lazy_data_service_entry(monkeypatch):
    from backend.data_service import api, shared_tushare_inputs as shared
    expected = object()
    reader = Mock(return_value=expected)
    monkeypatch.setattr(shared, 'read_history', reader)
    assert api.get_shared_input_history('vix_index', '2018-08-01', '2018-08-03') is expected
    reader.assert_called_once_with('vix_index', '2018-08-01', '2018-08-03', symbols=None, observed_by=None)


def test_shared_all_seven_empty_files_keep_explicit_schema_and_no_fake_events(monkeypatch, tmp_path):
    shared = mock_shared_snapshot(monkeypatch, rows=[])
    root = tmp_path/'empty'
    result = shared.export_bundle(root, '2018-08-01', '2018-08-01')
    assert set(result['datasets']) == set(shared.DATASETS)
    assert set(result['datasets'].values()) == {0}
    assert shared.verify_bundle(root)['datasets'] == result['datasets']
    for dataset in shared.DATASETS:
        read = shared.read_bundle(root, dataset)
        assert read.data.empty and read.data.columns.tolist() == shared._schema(dataset).names


def test_shared_verify_detects_raw_projection_tamper_even_after_file_rehash(monkeypatch, tmp_path):
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    shared = mock_shared_snapshot(monkeypatch)
    root = tmp_path/'v1'
    shared.export_bundle(root, '2026-10-30', '2026-10-30', datasets=['stk_alert'])
    file = root/'stk_alert.parquet'
    table = pq.read_table(file)
    index = table.column_names.index('type')
    pq.write_table(table.set_column(index, table.schema.field(index), pa.array(['invented', '1'])), file)
    manifest = json.loads((root/'manifest.json').read_text())
    manifest['files'][file.name] = {'sha256': shared._sha(file), 'bytes': file.stat().st_size}
    manifest['manifest_digest'] = shared._digest({k: v for k, v in manifest.items() if k != 'manifest_digest'})
    (root/'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='raw payload differ'):
        shared.verify_bundle(root)


def test_shared_partial_export_retains_failure_without_ready_manifest(monkeypatch, tmp_path):
    shared = mock_shared_snapshot(monkeypatch)

    def failed(*_a, **_kw):
        raise RuntimeError('real SQL failure')
        yield

    monkeypatch.setattr(shared, '_batches', failed)
    root = tmp_path/'partial'
    with pytest.raises(RuntimeError, match='real SQL failure'):
        shared.export_bundle(root, '2026-10-30', '2026-10-30', datasets=['stk_alert'])
    assert not (root/'manifest.json').exists()
    with pytest.raises(FileNotFoundError):
        shared.read_bundle(root, 'stk_alert')


def test_shared_file_reader_does_not_open_database_or_network(monkeypatch, tmp_path):
    shared = mock_shared_snapshot(monkeypatch)
    root = tmp_path/'v1'
    shared.export_bundle(root, '2026-10-30', '2026-10-30', datasets=['stk_alert'])
    monkeypatch.setattr(shared, 'capture_read_snapshot', Mock(side_effect=AssertionError('DB accessed')))
    assert shared.read_bundle(root, 'stk_alert').metadata['historical_PIT_proven'] is False
    assert shared.verify_bundle(root)['database_access'] is False


def test_shared_frozen_field_contract_survives_later_live_registry_extension(monkeypatch, tmp_path):
    shared = mock_shared_snapshot(monkeypatch)
    root = tmp_path/'v1'
    shared.export_bundle(root, '2026-10-30', '2026-10-30', datasets=['stk_alert'])
    old = DATASET_REGISTRY['stk_alert']
    monkeypatch.setitem(DATASET_REGISTRY, 'stk_alert', replace(old, columns={**old.columns, 'later_field': 'text'}))
    result = shared.read_bundle(root, 'stk_alert')
    assert 'later_field' not in result.data.columns
    assert shared.verify_bundle(root)['datasets'] == {'stk_alert': 2}


def test_shared_files_reject_reparse_points_without_following_target():
    from types import SimpleNamespace
    from backend.data_service import shared_tushare_inputs as shared
    path = Mock()
    path.lstat.return_value = SimpleNamespace(st_file_attributes=1024)
    path.is_symlink.return_value = False
    with pytest.raises(ValueError, match='reparse'):
        shared._plain(path)
