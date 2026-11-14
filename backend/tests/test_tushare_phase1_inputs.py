"""Direct contracts of the first optional Tushare inputs."""
import datetime as dt
import json
import uuid
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.tushare_dataset_specs import DAILY_BASIC, DATASET_REGISTRY


def test_phase1_specs_and_optional_obligations():
    from backend.services.data_integrity.catalog import obligations
    vix = DATASET_REGISTRY['vix_index']
    after = DATASET_REGISTRY['stock_daily_afterhours']
    assert vix.primary_keys == ['trade_date']
    assert set(vix.columns) == {'trade_date', 'close', 'pct_change'}
    assert not vix.trading_day_only and vix.row_limit == 300
    assert after.tushare_api == 'daily' and after.initial_start_date == '2026-07-06'
    assert set(after.columns) == {'trade_date', 'ts_code', 'ah_vol', 'ah_amount'}
    assert DAILY_BASIC.columns['limit_status'] == 'smallint'
    names = {x.dataset for x in obligations()[0]}
    assert not {'vix_index', 'stock_daily_afterhours'} & names
    assert 'daily_basic' in names


@pytest.mark.parametrize('value', [None, 0, '6', Decimal('2.0')])
def test_limit_status_nullable_integer(value):
    from backend.services.tushare_supplemental_inputs import normalize_rows
    result = normalize_rows(DAILY_BASIC, [{'limit_status': value}])
    assert result[0]['limit_status'] == (None if value is None else int(value))


@pytest.mark.parametrize('value', [True, -1, 7, 2.5, 'NaN', 'Infinity'])
def test_limit_status_rejects_invalid(value):
    from backend.services.tushare_supplemental_inputs import normalize_rows
    with pytest.raises(ValueError, match='limit_status'):
        normalize_rows(DAILY_BASIC, [{'limit_status': value}])


def test_source_values_units_and_scope():
    from backend.services.tushare_supplemental_inputs import normalize_rows
    from backend.services.data_integrity.stock_scope import filter_dataset_rows
    spec = DATASET_REGISTRY['stock_daily_afterhours']
    rows = [dict(trade_date='20261105', ts_code=code, ah_vol='0', ah_amount=None)
            for code in ('000001.SZ', '688001.SH', '920001.BJ', '900001.SH', '200001.SZ')]
    result = normalize_rows(spec, filter_dataset_rows(spec, rows))
    assert [r['ts_code'] for r in result] == ['000001.SZ', '688001.SH']
    assert result[0]['ah_vol'] == Decimal('0') and result[0]['ah_amount'] is None
    vix = normalize_rows(DATASET_REGISTRY['vix_index'],
                         [dict(trade_date='20261104', close='19.2500', pct_change='-2.34')])
    assert vix[0]['close'] == Decimal('19.2500')
    assert vix[0]['pct_change'] == Decimal('-2.34')


@pytest.mark.parametrize('dataset,field,value', [
    ('vix_index', 'close', None), ('vix_index', 'close', 0),
    ('vix_index', 'pct_change', 'NaN'),
    ('stock_daily_afterhours', 'ah_vol', -1),
    ('stock_daily_afterhours', 'ah_amount', float('inf')),
])
def test_new_input_invalid_values(dataset, field, value):
    from backend.services.tushare_supplemental_inputs import normalize_rows
    row = dict(trade_date='20261104', ts_code='000001.SZ', close='19', pct_change='-1',
               ah_vol=0, ah_amount=None)
    row[field] = value
    with pytest.raises(ValueError, match=field):
        normalize_rows(DATASET_REGISTRY[dataset], [row])


def test_vix_windows_bounded_calendar_and_no_chinese_calendar():
    from backend.services.tushare_supplemental_inputs import request_windows
    windows = list(request_windows(dt.date(2018, 8, 1), dt.date(2026, 11, 5)))
    assert windows[0][0] == dt.date(2018, 8, 1)
    assert windows[-1][1] == dt.date(2026, 11, 5)
    assert all((b-a).days < 90 for a, b in windows)
    assert all(b + dt.timedelta(days=1) == c for (_, b), (c, _) in zip(windows, windows[1:]))


def test_vix_DST_uses_US_date_without_weekday_holiday_guess():
    from backend.services.tushare_supplemental_inputs import latest_vix_query_date
    assert latest_vix_query_date(dt.datetime(2026, 7, 4, 0, tzinfo=dt.timezone.utc)) == dt.date(2026, 7, 3)
    assert latest_vix_query_date(dt.datetime(2026, 1, 6, 0, tzinfo=dt.timezone.utc)) == dt.date(2026, 1, 5)
    # US 16:00 hasn't reached conservative 17:00 bound yet; not evidence of a trading session.
    assert latest_vix_query_date(dt.datetime(2026, 7, 3, 20, tzinfo=dt.timezone.utc)) == dt.date(2026, 7, 2)


def test_raw_original_strings_hash_and_identity():
    import hashlib
    from backend.services.tushare_supplemental_inputs import raw_observation
    spec = DATASET_REGISTRY['vix_index']
    row = dict(trade_date='20261104', close='19.2500', pct_change='-2.3400')
    observation = raw_observation(spec, row)
    assert observation[3] is None
    payload = observation[5].adapted
    assert payload == row
    encoded = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    assert hashlib.sha256(encoded.encode()).hexdigest() == observation[4]
    assert raw_observation(spec, dict(reversed(list(row.items()))))[4] == observation[4]


@pytest.mark.parametrize('write_error', [False, True])
def test_original_and_projection_atomic_transaction(monkeypatch, write_error):
    import backend.services.tushare_supplemental_inputs as module
    from backend.tests.test_tushare_sync_engine import _FakeConn
    conn, captured = _FakeConn(), []
    monkeypatch.setattr(module, 'execute_values', lambda _cur, _sql, rows: captured.extend(rows))
    spec = DATASET_REGISTRY['stock_daily_afterhours']
    rows = [dict(ts_code='000001.SZ', trade_date='20261104', ah_vol='0', ah_amount=None)]
    def write(facts):
        assert not conn.autocommit
        assert facts[0]['ah_vol'] == Decimal('0')
        if write_error:
            raise RuntimeError('injected database failure')
        return len(facts)
    if write_error:
        with pytest.raises(RuntimeError, match='injected'):
            module.store_observations(conn, spec, rows, write)
    else:
        assert module.store_observations(conn, spec, rows, write) == 1
    assert len(captured) == 1 and captured[0][-1].adapted['ah_vol'] == '0'
    assert conn.commits == int(not write_error) and conn.rollbacks == int(write_error)
    assert conn.autocommit


def vix_engine(monkeypatch, rows):
    import backend.services.tushare_sync_engine as module
    import backend.services.tushare_us_market_inputs as us
    monkeypatch.setattr(us, 'read_calendar', lambda *_: {})
    engine = module.TushareSyncEngine()
    audits, writes = [], []
    monkeypatch.setattr(engine, '_fetch_from_tushare', lambda _s, params: rows)
    monkeypatch.setattr(engine, '_upsert_batch', lambda _c, _s, facts: writes.extend(facts) or len(facts))
    monkeypatch.setattr(engine, '_update_progress', lambda *_: None)
    monkeypatch.setattr(engine, '_upsert_sync_target', lambda *_a, **_kw: 'test-target')
    monkeypatch.setattr(engine, '_record_sync_attempt', lambda *_a, **_kw: None)
    monkeypatch.setattr(module.time, 'sleep', lambda *_: None)
    engine._refresh_audit = SimpleNamespace(record_success=lambda **kw: audits.append(kw),
                                            record_failure=lambda **kw: audits.append(kw))
    return engine, audits, writes


def test_vix_empty_date_never_proves_holiday_or_calendar_complete(monkeypatch):
    engine, audits, writes = vix_engine(monkeypatch, [dict(trade_date='20261105', close='20', pct_change='-1')])
    result = engine._sync_by_date(None, DATASET_REGISTRY['vix_index'], dt.date(2026, 11, 5),
                                  dt.date(2026, 11, 7), uuid.uuid4())
    assert result.ok and len(writes) == 1 and len(audits) == 3
    assert [audit['quality_status'] for audit in audits] == ['unknown', 'unknown', 'unknown']
    assert all(audit['metadata']['session_calendar_completeness'] == 'NOT_EVALUATED' for audit in audits)


@pytest.mark.parametrize('rows', [
    [dict(trade_date='20261104', close='20', pct_change='-1')],
    [dict(trade_date='20261105', close='20', pct_change='-1')]*2,
])
def test_vix_invalid_window_never_writes_or_audits_success(monkeypatch, rows):
    engine, audits, writes = vix_engine(monkeypatch, rows)
    result = engine._sync_by_date(None, DATASET_REGISTRY['vix_index'], dt.date(2026, 11, 5),
                                  dt.date(2026, 11, 5), uuid.uuid4())
    assert not result.ok and writes == []
    assert len(audits) == 1 and audits[0]['quality_status'] == 'error'


def test_vix_cap_rejects_before_persistence(monkeypatch):
    import backend.services.tushare_sync_engine as module
    engine = module.TushareSyncEngine()
    monkeypatch.setattr(engine, '_tushare_token', lambda: 'test-only')
    monkeypatch.setattr(module, '_query_tushare_dataapi', lambda *_a, **_kw:
                        [dict(trade_date='20261105', close='20', pct_change='-1')]*300)
    with pytest.raises(RuntimeError, match='row_limit=300'):
        engine._fetch_from_tushare(DATASET_REGISTRY['vix_index'], {'start_date': '20261101'})


def test_UI_and_default_automation_are_registered():
    from backend.db.init_tushare_schedules import get_default_schedule_catalog
    catalogue = get_default_schedule_catalog()
    assert catalogue['complete'] and not catalogue['errors']
    rows = {r['dataset']: r for r in catalogue['templates']}
    assert rows['vix_index']['options'] == {'at': '08:00', 'skip_auto_range': True}
    assert rows['_vix_index_midday']['options']['at'] == '12:00'
    assert rows['stock_daily_afterhours']['options']['at'] == '20:35'
    root = Path(__file__).resolve().parents[2]
    text = (root / 'frontend/src/app/local-data/page.tsx').read_text(encoding='utf-8')
    for name in ('vix_index', 'stock_daily_afterhours'):
        assert text.count(f'key: "{name}"') == 2  # init and manual management
        assert f'dataset: "{name}"' in text
        assert f'lower === "{name}"' in text
        assert f'k === "{name}"' in text
    assert '美国交易日完整性尚未评估' in text


def test_actual_ingestion_catalogue_and_range_use_US_clock(monkeypatch):
    from backend.routers import ingestion
    from backend.tests.test_tushare_sync_engine import _FakeConn
    import backend.services.tushare_supplemental_inputs as supplemental
    conn = _FakeConn()
    conn.fetchone_value = (dt.date(2026, 11, 4),)
    monkeypatch.setattr(ingestion, 'get_conn', lambda: conn)
    monkeypatch.setattr(ingestion, '_fetchone', lambda *_args: None)
    def fetch(sql, _params):
        if 'data_stats_config' in sql:
            return [dict(table_name='market.vix_index', date_column='trade_date',
                         extra_info={'date_sequence': 'calendar', 'cursor_source': 'refresh_audit'})]
        return [{'mx': dt.date(2026, 11, 4)}]
    monkeypatch.setattr(ingestion, '_fetchall', fetch)
    monkeypatch.setattr(supplemental, 'latest_vix_query_date', lambda: dt.date(2026, 11, 5))
    monkeypatch.setattr(ingestion, '_latest_trading_day_on_or_before',
                        lambda *_: pytest.fail('must not query Chinese calendar for VIX'))
    result = ingestion.get_ingestion_auto_range('vix_index')
    assert result['start_date'] == '2026-10-30' and result['latest_date'] == '2026-11-05'
    catalogue = {r['name']: r for r in ingestion.list_tushare_datasets()['datasets']}
    assert catalogue['vix_index']['required_for_daily_complete'] is False
    assert catalogue['stock_daily_afterhours']['initial_start_date'] == '2026-07-06'
    assert ingestion._infer_source('stock_daily_afterhours') == 'tushare'
    monkeypatch.setattr(ingestion, 'get_conn', lambda: conn)
    import backend.services.tushare_us_market_inputs as us
    monkeypatch.setattr(us, 'gap_report', lambda *_: {'status': 'UNKNOWN'})
    assert ingestion.get_data_gaps('vix_index', start_date=None, end_date=None, refresh=False)['status'] == 'UNKNOWN'


@pytest.mark.parametrize('dataset', ['vix_index', 'stock_daily_afterhours'])
def test_actual_init_API_requires_explicit_window_before_creating_job(dataset):
    from backend.routers.ingestion import IngestionRunRequest, trigger_ingestion_run
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as error:
        trigger_ingestion_run(IngestionRunRequest(dataset=dataset, mode='init', options={}))
    assert error.value.status_code == 400 and 'requires start_date' in error.value.detail


def test_midday_vix_alias_shares_same_overlap_owner(monkeypatch):
    from backend.ingestion.tdx_scheduler import TDXScheduler
    instance = object.__new__(TDXScheduler)
    observed = []
    instance._tracker = SimpleNamespace(is_running=lambda key: observed.append(key) or True)
    monkeypatch.setattr(instance, '_update_ingestion_schedule', lambda *_a, **_kw: None)
    monkeypatch.setattr(instance, '_next_run_for', lambda *_: None)
    instance._scheduled_ingestion_run('test', '_vix_index_midday', 'incremental', {})
    assert observed == ['ingestion:vix_index:incremental']


def test_optional_partition_contract_does_not_add_daily_obligation():
    from backend.services.data_integrity.catalog import optional_input_contract, obligations
    assert optional_input_contract('vix_index').category == 'event'
    assert optional_input_contract('stock_daily_afterhours').category == 'daily_series'
    assert optional_input_contract('daily_basic') is None
    assert 'vix_index' not in {r.dataset for r in obligations()[0]}


def test_empty_US_query_does_not_seed_unprovable_daily_validation(monkeypatch):
    from backend.services.data_refresh_audit import DataRefreshAuditRepository
    from backend.tests.test_tushare_sync_engine import _FakeConn
    import backend.services.data_integrity.repository as repository
    monkeypatch.setattr(repository, 'enqueue_validation', lambda *_a, **_kw:
                        pytest.fail('empty US date must not create validation retry'))
    conn = _FakeConn()
    DataRefreshAuditRepository().record_success(dataset='vix_index', trade_date=dt.date(2026, 11, 7),
        row_count=0, written_rows=0, quality_status='unknown',
        metadata={'provider_query_complete': True, 'session_calendar_completeness': 'NOT_EVALUATED'}, conn=conn)
    assert conn.commits == 1 and conn.autocommit
    assert len(conn.executed) == 1 and 'dataset_date_refresh_audit' in conn.executed[0][0]
