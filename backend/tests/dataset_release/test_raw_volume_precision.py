from decimal import Decimal

import pytest

from backend.services.dataset_release.canonical_stock_transformer import raw_volume_shares
from backend.services.dataset_release.monthly_frozen_source_audit import _ohlcv
from backend.services.dataset_release.canonical_stock_transformer import (
    CanonicalStockTransformError, _normalize_raw_values, _transform_raw_row,
)
from backend.services.dataset_release.source_authority import PRODUCTION_QUERY_SPECS


def row(**extra):
    return dict(ts_code='001255.SZ', open_li=10000, high_li=11000, low_li=9000,
                close_li=10000, volume_hand=9250, amount_li=9250500000, **extra)


def precision(**extra):
    return dict(volume_shares=Decimal('925050'), volume_shares_source='tushare_daily',
                volume_shares_sha256='a' * 64, **extra)


def test_exact_share_volume_survives_audit_and_qfq_bin_transform():
    from datetime import datetime
    fact = row(**precision())
    assert raw_volume_shares(fact) == 925050
    assert _ohlcv(fact)['vol'] == 925050
    normalized = _normalize_raw_values(fact, source='daily', ordinal=1)
    result = _transform_raw_row(normalized, timestamp=datetime(2026, 9, 1), qfq=0.5,
                                limit=dict(up_limit=11, down_limit=9, pre_close=10))
    assert result['volume'] == 1850100
    assert result['open'] == 5
    assert result['amount'] == 9250500
    assert fact['volume_hand'] == 9250  # legacy source field was not rewritten


@pytest.mark.parametrize('extra', [{}, {'volume_shares': None}])
def test_legacy_frozen_volume_stays_byte_semantically_compatible(extra):
    assert raw_volume_shares(row(**extra)) == 925000
    assert _ohlcv(row(**extra))['vol'] == 925000


@pytest.mark.parametrize('bad', [True, -1, float('nan'), float('inf'), '925050'])
def test_invalid_precision_never_silently_falls_back(bad):
    fact = row(**{**precision(), 'volume_shares': bad})
    with pytest.raises(ValueError):
        raw_volume_shares(fact)
    with pytest.raises(CanonicalStockTransformError):
        _normalize_raw_values(fact, source='minute', ordinal=2)


@pytest.mark.parametrize('field,value', [('volume_shares_source', None),
                                         ('volume_shares_source', 'invented'),
                                         ('volume_shares_sha256', 'bad')])
def test_precision_requires_provider_provenance(field, value):
    with pytest.raises(ValueError):
        raw_volume_shares(row(**{**precision(), field: value}))


@pytest.mark.parametrize('query_id', ['kline_daily_raw', 'kline_minute_raw'])
def test_precise_fields_are_frozen_with_a_new_query_identity(query_id):
    spec = PRODUCTION_QUERY_SPECS[query_id]
    for field in precision():
        assert field in spec.value_columns
        assert field in spec.required_columns
        assert field not in spec.non_null_value_columns
        assert f'source_row.{field}' in spec.sql
    assert 'share_precision_v1' in spec.query_version


@pytest.mark.parametrize('query_id', ['kline_daily_raw', 'kline_minute_raw'])
@pytest.mark.parametrize('nullable', [False, True])
def test_raw_precision_metadata_survives_real_source_sealing(tmp_path, query_id, nullable):
    import gzip
    import json
    from datetime import date
    from types import SimpleNamespace

    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.profile import ResourcePolicy
    from backend.services.dataset_release.source_authority import MonthlySourceAuthority, SourceTableSchema

    query = PRODUCTION_QUERY_SPECS[query_id]
    provider = 'tushare_daily' if query_id == 'kline_daily_raw' else 'tushare_stk_mins'
    payload = row(**{**precision(), 'volume_shares': 925050, 'volume_shares_source': provider})
    if nullable:
        payload.update({name: None for name in precision()})
    if query_id == 'kline_daily_raw':
        payload['trade_date'] = '2026-09-01'
    else:
        payload.update(trade_time='2026-09-01 09:31:00', freq='1min')

    class Session:
        def stream(self, *_args, **_kwargs):
            yield {'row_key': json.dumps([payload[name] for name in query.key_columns]),
                   'row_payload': json.dumps(payload)}

    ControlStore.initialize(tmp_path)
    authority = MonthlySourceAuthority(
        SimpleNamespace(resource_policy=ResourcePolicy(),
                        pressure_ladder={'date_chunk_months': (1,), 'minute_batch': (20,)}), CASStore(tmp_path),
        sector_source_policy='classification_published_snapshot_v1',
    )
    partition = authority._seal_query_partition(
        Session(), query=query, partition_key='2026-09-01_2026-09-30',
        params={'start': date(2026, 9, 1), 'end': date(2026, 9, 30)}, tokens=(),
        table_schema=SourceTableSchema(query.table_identity, query.required_columns),
    )
    frozen = json.loads(gzip.decompress((tmp_path / partition.rows_ref.relative_path).read_bytes()).splitlines()[1])
    frozen_payload = json.loads(frozen['row_payload'])
    assert frozen_payload == payload
    assert partition.summary.row_count == 1
    assert raw_volume_shares(frozen_payload) == (925000 if nullable else 925050)


@pytest.mark.parametrize('field', ['volume_shares_source', 'volume_shares_sha256'])
@pytest.mark.parametrize('bad', [True, 123, 1.5, [], '', '  '])
def test_source_precision_provenance_retains_text_type_contract(field, bad):
    from backend.services.dataset_release.source_authority import _validate_payload_type
    from backend.services.dataset_release.errors import SourceManifestError

    with pytest.raises(SourceManifestError, match='text value is invalid'):
        _validate_payload_type(field, bad, partition='2026-09')


@pytest.mark.parametrize('query_id', ['kline_daily_raw', 'kline_minute_raw'])
@pytest.mark.parametrize('nullable', [False, True])
def test_production_precision_sql_in_existing_readonly_dev(query_id, nullable):
    import json
    import os
    from datetime import date

    if os.getenv('AISTOCK_MONTHLY_RAW_PRECISION_DEV_READBACK') != '1':
        pytest.skip('explicit existing DEV read-only validation only')
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from backend.services.dataset_release.source_authority import (
        SourceTableSchema, _query_partition_spec, _validate_query_row,
    )

    target = {key: os.environ[f'TDX_DB_DEV_{env}'] for key, env in (
        ('host', 'HOST'), ('port', 'PORT'), ('dbname', 'NAME'),
        ('user', 'USER'), ('password', 'PASSWORD'),
    )}
    assert target['port'] == '5433' and 'dev' in target['dbname'].lower()
    provider = 'tushare_daily' if query_id == 'kline_daily_raw' else 'tushare_stk_mins'
    payload = row(**{**precision(), 'volume_shares': 925050, 'volume_shares_source': provider})
    payload.update(trade_date='2026-09-01', trade_time='2026-09-01 09:31:00+08', freq='1min')
    if nullable:
        payload.update({name: None for name in precision()})
    query = PRODUCTION_QUERY_SPECS[query_id]
    sql = '''WITH fixture_raw AS (SELECT * FROM jsonb_to_record(%(fixture_row)s::jsonb) AS fact(
        ts_code text, trade_date date, trade_time timestamptz, freq text,
        open_li bigint, high_li bigint, low_li bigint, close_li bigint,
        volume_hand bigint, amount_li bigint, volume_shares numeric,
        volume_shares_source text, volume_shares_sha256 text)) ''' + query.sql.replace(
            query.table_identity, 'fixture_raw')
    with psycopg2.connect(**target, application_name='BUG-1817-readonly-DEV') as connection:
        connection.set_session(readonly=True, autocommit=False)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('SHOW transaction_read_only')
            assert cursor.fetchone()['transaction_read_only'] == 'on'
            cursor.execute(sql, dict(fixture_row=json.dumps(payload), codes=[payload['ts_code']],
                                     start=date(2026, 9, 1), end=date(2026, 9, 30)))
            rows = cursor.fetchall()
    assert len(rows) == 1
    spec = _query_partition_spec(query, '2026-09', SourceTableSchema(query.table_identity, query.required_columns))
    frozen = json.loads(_validate_query_row(rows[0], query, spec)['row_payload'])
    assert frozen['volume_shares_source'] == payload['volume_shares_source']
    assert frozen['volume_shares_sha256'] == payload['volume_shares_sha256']
    assert raw_volume_shares(frozen) == (925000 if nullable else 925050)


@pytest.mark.parametrize('orphan', [False, True])
def test_factor_h5_keeps_exact_volume_and_mixed_nullable_legacy_rows(orphan):
    import pandas as pd
    from backend.services.dataset_release.factor_materializer import _build_qfq_daily, FactorMaterializationError
    legacy = {**row(), 'ts_code': '000001.SZ', 'trade_date': '2026-09-01'}
    exact = {**row(**precision()), 'trade_date': '2026-09-01'}
    if orphan:
        exact['volume_shares'] = None
    raw = pd.DataFrame([legacy, exact])
    adj = pd.DataFrame([dict(ts_code=c, trade_date='2026-09-01', adj_factor=.5)
                        for c in ('000001.SZ', '001255.SZ')])
    args = dict(base_factors={'000001.SZ': 1., '001255.SZ': 1.}, previous_adj_tail=pd.DataFrame())
    if orphan:
        with pytest.raises(FactorMaterializationError):
            _build_qfq_daily(raw, adj, **args)
    else:
        actual, _ = _build_qfq_daily(raw, adj, **args)
        assert actual.loc[(pd.Timestamp('2026-09-01'), '001255.SZ'), 'volume'] == 1850100
        assert actual.loc[(pd.Timestamp('2026-09-01'), '000001.SZ'), 'volume'] == 1850000
