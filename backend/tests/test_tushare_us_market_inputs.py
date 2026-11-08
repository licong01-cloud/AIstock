"""US inputs never inherit the A-share calendar, stock filters or readiness."""
import datetime as dt
from decimal import Decimal

import pytest

from backend.services.tushare_dataset_specs import DATASET_REGISTRY


def test_specs_and_optional_scope():
    from backend.services.data_integrity.catalog import obligations
    for name in ('index_global', 'us_tradecal'):
        spec = DATASET_REGISTRY[name]
        assert not spec.required_for_daily_complete and not spec.trading_day_only
        assert spec.initial_start_date == '2018-08-01'
    assert DATASET_REGISTRY['us_tradecal'].date_column == 'cal_date'
    assert 'pct_chg' in DATASET_REGISTRY['index_global'].columns
    assert not {'index_global', 'us_tradecal'} & {item.dataset for item in obligations()[0]}


def index_row(**changes):
    return dict(ts_code='SPX', trade_date='20261105', open='100', close='102',
                high='103', low='99', pre_close='100', change='2', pct_chg='2',
                swing=None, vol=None, amount=None, **changes)


def test_index_units_optional_nulls_raw_and_identity():
    from backend.services.tushare_supplemental_inputs import normalize_rows, raw_observation
    spec = DATASET_REGISTRY['index_global']
    original = index_row()
    row = normalize_rows(spec, [original])[0]
    assert row['close'] == Decimal('102') and row['pct_chg'] == Decimal('2')
    assert row['vol'] is None and row['amount'] is None
    assert raw_observation(spec, original)[5].adapted == original


@pytest.mark.parametrize('field,value', [('close', None), ('high', 'NaN'), ('low', 105),
                                        ('open', 0), ('ts_code', 'HSI'), ('vol', -1)])
def test_bad_index_never_written(field, value):
    from backend.services.tushare_supplemental_inputs import normalize_rows
    row = index_row()
    row[field] = value
    with pytest.raises(ValueError):
        normalize_rows(DATASET_REGISTRY['index_global'], [row])


@pytest.mark.parametrize('flag', [False, 2, 'NaN', None])
def test_invalid_calendar_flag(flag):
    from backend.services.tushare_supplemental_inputs import normalize_rows
    with pytest.raises(ValueError):
        normalize_rows(DATASET_REGISTRY['us_tradecal'], [
            dict(cal_date='20261105', is_open=flag, pretrade_date='20261104')])


def test_calendar_original_is_preserved():
    from backend.services.tushare_supplemental_inputs import normalize_rows, raw_observation
    spec = DATASET_REGISTRY['us_tradecal']
    raw = dict(cal_date='20261107', is_open=0, pretrade_date='20261106')
    assert normalize_rows(spec, [raw])[0]['is_open'] == 0
    assert raw_observation(spec, raw)[2] == dt.date(2026, 11, 7)
    assert raw_observation(spec, raw)[5].adapted == raw


def test_session_quality_does_not_guess_holidays():
    from backend.services.tushare_us_market_inputs import partition_quality
    assert partition_quality('index_global', 1, 3) == ('ok', 3)
    assert partition_quality('index_global', 1, 2) == ('low_coverage', 3)
    assert partition_quality('index_global', 0, 0) == ('empty_valid', 0)
    assert partition_quality('index_global', None, 0) == ('unknown', None)
    with pytest.raises(ValueError, match='closed'):
        partition_quality('index_global', 0, 3)


def test_catalog_has_one_owner_and_existing_compensation():
    from backend.db.init_tushare_schedules import get_default_schedule_catalog
    catalog = get_default_schedule_catalog()
    assert catalog['complete'], catalog['errors']
    entries = {item['dataset']: item for item in catalog['templates']}
    assert entries['us_tradecal']['options']['at'] == '07:30'
    assert entries['index_global']['options']['at'] == '08:05'
    assert entries['_index_global_midday']['options']['at'] == '12:05'
    for name in ('stk_shock', 'stk_high_shock', 'stk_alert', 'idx_anns', 'stk_seasoned'):
        assert entries[name]['enabled'] and entries[f'_{name}_morning']['enabled']
    assert entries['_vix_index_midday']['options']['at'] == '12:00'


def test_ended_US_date_DST_not_chinese_calendar():
    from backend.services.tushare_supplemental_inputs import latest_vix_query_date
    assert latest_vix_query_date(dt.datetime(2026, 7, 3, 21, tzinfo=dt.timezone.utc)) == dt.date(2026, 7, 3)
    assert latest_vix_query_date(dt.datetime(2026, 1, 5, 21, tzinfo=dt.timezone.utc)) == dt.date(2026, 1, 4)


def test_incremental_tail_and_downtime_catchup():
    from backend.services.tushare_us_market_inputs import incremental_start
    from backend.tests.test_tushare_sync_engine import _FakeConn
    conn = _FakeConn()
    spec = DATASET_REGISTRY['index_global']
    end = dt.date(2026, 11, 6)
    conn.fetchone_value = (None,)
    assert incremental_start(conn, spec, end)[0] == dt.date(2018, 8, 1)
    conn.fetchone_value = (dt.date(2026, 11, 6),)
    assert incremental_start(conn, spec, end)[0] == dt.date(2026, 10, 31)
    conn.fetchone_value = (dt.date(2026, 10, 1),)
    assert incremental_start(conn, spec, end)[0] == dt.date(2026, 10, 2)


def test_native_range_calendar_and_missing_code_visibility(monkeypatch):
    from types import SimpleNamespace
    import uuid
    import backend.services.tushare_us_market_inputs as us
    from backend.services.tushare_sync_engine import TushareSyncEngine
    day = dt.date(2026, 11, 5)
    engine, audits, writes, requests = TushareSyncEngine(), [], [], []
    monkeypatch.setattr(us, 'read_calendar', lambda *_: {day: 1, day+dt.timedelta(days=1): 0})
    def fetch(_spec, params):
        requests.append(params)
        row = index_row()
        row['ts_code'] = params['ts_code']
        return [] if row['ts_code'] == 'DJI' else [row]
    monkeypatch.setattr(engine, '_fetch_from_tushare', fetch)
    monkeypatch.setattr(engine, '_upsert_batch', lambda _c, _s, facts: writes.extend(facts) or len(facts))
    monkeypatch.setattr(engine, '_update_progress', lambda *_: None)
    engine._refresh_audit = SimpleNamespace(record_success=lambda **kw: audits.append(kw),
                                            record_failure=lambda **kw: pytest.fail(str(kw)))
    result = us.sync_ranges(engine, None, DATASET_REGISTRY['index_global'], day,
                            day+dt.timedelta(days=1), uuid.uuid4())
    assert result.ok and len(writes) == 2 and {p['ts_code'] for p in requests} == set(us.US_INDICES)
    assert [a['quality_status'] for a in audits] == ['low_coverage', 'empty_valid']
    assert result.as_dict()['quality_warnings'][0]['expected_rows'] == 3
    assert not audits[0]['metadata']['coverage_complete']


@pytest.mark.parametrize('failure', ['transport', 'wrong_code', 'duplicate', 'outside'])
def test_native_failure_never_succeeds_or_writes(monkeypatch, failure):
    from types import SimpleNamespace
    import uuid
    import backend.services.tushare_us_market_inputs as us
    from backend.services.tushare_sync_engine import TushareSyncEngine
    engine, audits = TushareSyncEngine(), []
    monkeypatch.setattr(us, 'read_calendar', lambda *_: {})
    def fetch(_spec, params):
        if failure == 'transport':
            raise RuntimeError('transport failed')
        row = index_row()
        row['ts_code'] = 'HSI' if failure == 'wrong_code' else params['ts_code']
        if failure == 'outside':
            row['trade_date'] = '20261104'
        return [row, row] if failure == 'duplicate' else [row]
    monkeypatch.setattr(engine, '_fetch_from_tushare', fetch)
    monkeypatch.setattr(engine, '_upsert_batch', lambda *_: pytest.fail('must not write'))
    monkeypatch.setattr(engine, '_update_progress', lambda *_: None)
    engine._refresh_audit = SimpleNamespace(record_success=lambda **kw: pytest.fail(str(kw)),
                                            record_failure=lambda **kw: audits.append(kw))
    result = us.sync_ranges(engine, None, DATASET_REGISTRY['index_global'], dt.date(2026, 11, 5),
                            dt.date(2026, 11, 5), uuid.uuid4())
    assert not result.ok and len(audits) == 1 and audits[0]['quality_status'] == 'error'


@pytest.mark.parametrize('dataset', ['us_tradecal', 'index_global'])
def test_API_UI_operability_and_init_boundary(dataset, monkeypatch):
    from pathlib import Path
    from fastapi import HTTPException
    from backend.routers.ingestion import (IngestionRunRequest, trigger_ingestion_run,
                                           list_tushare_datasets, _infer_source)
    from backend.routers import ingestion
    monkeypatch.setattr(ingestion, '_fetchone', lambda *_: None)
    assert _infer_source(dataset) == 'tushare'
    entries = {row['name']: row for row in list_tushare_datasets()['datasets']}
    assert not entries[dataset]['required_for_daily_complete']
    with pytest.raises(HTTPException, match='requires start_date'):
        trigger_ingestion_run(IngestionRunRequest(dataset=dataset, mode='init', options={}))
    text = (Path(__file__).resolve().parents[2]/'frontend/src/app/local-data/page.tsx').read_text(encoding='utf-8')
    assert text.count(f'key: "{dataset}"') == 2
    assert f'dataset: "{dataset}"' in text and f'lower === "{dataset}"' in text


def test_midday_alias_uses_same_overlap_guard(monkeypatch):
    from types import SimpleNamespace
    from backend.ingestion.tdx_scheduler import TDXScheduler
    instance = object.__new__(TDXScheduler)
    observed = []
    instance._tracker = SimpleNamespace(is_running=lambda key: observed.append(key) or True)
    monkeypatch.setattr(instance, '_update_ingestion_schedule', lambda *_a, **_kw: None)
    monkeypatch.setattr(instance, '_next_run_for', lambda *_: None)
    instance._scheduled_ingestion_run('test', '_index_global_midday', 'incremental', {})
    assert observed == ['ingestion:index_global:incremental']


def test_US_idempotence_does_not_enqueue_equity_validation(monkeypatch):
    from backend.tests.test_tushare_sync_engine import _FakeConn
    from backend.services.tushare_sync_engine import TushareSyncEngine
    import backend.services.tushare_sync_engine as module
    import backend.services.data_integrity.repository as repository
    conn = _FakeConn()
    monkeypatch.setattr(module.pgx, 'execute_values', lambda *_a, **_kw: [])
    monkeypatch.setattr(repository, 'mark_changed', lambda *_a, **_kw:
                        pytest.fail('US source must not enqueue A-share validation'))
    count = TushareSyncEngine()._upsert_plain_batch(conn, DATASET_REGISTRY['index_global'], [index_row()])
    assert count == 0


def test_known_US_gap_is_retried_after_rolling_tail(monkeypatch):
    from backend.tests.test_tushare_sync_engine import _FakeConn, _FakeCursor
    from backend.services.tushare_sync_engine import TushareSyncEngine
    conn, engine = _FakeConn(), TushareSyncEngine()
    conn.fetchone_value = (dt.date(2026, 11, 6),)
    monkeypatch.setattr(_FakeCursor, 'fetchall', lambda _: [(dt.date(2026, 8, 3),)])
    start, _ = engine._resolve_incremental_start_date(conn, DATASET_REGISTRY['index_global'], dt.date(2026, 11, 6))
    assert start == dt.date(2026, 10, 31)
    assert engine._late_recheck_dates['index_global'] == [dt.date(2026, 8, 3)]
