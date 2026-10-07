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
