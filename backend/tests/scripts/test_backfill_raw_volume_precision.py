from datetime import date
import hashlib
import json

import pytest

from scripts.backfill_raw_volume_precision import load_facts, SCHEMA
from scripts import backfill_raw_volume_precision as repair


def test_raw_readback_has_explicit_partition_bounds():
    from unittest.mock import MagicMock
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchall.return_value = []
    repair.read_rows(conn, [('001255.SZ',date(2026,9,1)),('688582.SH',date(2026,9,3))])
    query, args = cur.execute.call_args.args
    assert "d.trade_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-03'" in query
    assert args[1] == [date(2026,9,1),date(2026,9,3)]


def test_precision_cas_keeps_exact_keys_with_month_partition_bounds(monkeypatch):
    captured = []
    patches = [(date(2026,9,1),'001255.SZ',925050,'a'*64,9250)]
    def execute(cur,sql,values,**kwargs):
        captured.append((sql,values,kwargs))
        return [('001255.SZ',date(2026,9,1))]
    monkeypatch.setattr(repair,'execute_values',execute)
    assert repair.apply_precision_patches(object(),patches) == [('001255.SZ',date(2026,9,1))]
    sql, values, kwargs = captured[0]
    assert "t.trade_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-01'" in sql
    assert 't.trade_date=v.day AND t.ts_code=v.code' in sql
    assert 't.volume_hand=v.hands AND t.volume_shares IS NULL' in sql
    assert 't.volume_shares_source IS NULL AND t.volume_shares_sha256 IS NULL' in sql
    assert values == patches and kwargs['fetch']


def manifest(tmp_path, *, vol=9250.5):
    raw = json.dumps(dict(api='daily',captured_at='2026-10-07T00:00:00Z', rows=[dict(
        ts_code='001255.SZ',trade_date='20260901',open=10,high=11,low=9,close=10,vol=vol)])).encode()
    path = tmp_path / 'daily.json'
    path.write_bytes(raw)
    return dict(schema_version=SCHEMA,start_date='2026-09-01',end_date='2026-09-30',
                keys=[dict(symbol='001255.SZ',trade_date='2026-09-01')],
                sources=[dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest())])


def test_pinned_official_decimal_hands_are_not_rounded(tmp_path):
    facts = load_facts(manifest(tmp_path))
    assert facts[('001255.SZ',date(2026,9,1))][0] == 925050


@pytest.mark.parametrize('vol', [None,-1,True,9250.5001])
def test_invalid_or_fractional_share_facts_are_rejected(tmp_path, vol):
    with pytest.raises((ValueError, TypeError)):
        load_facts(manifest(tmp_path,vol=vol))


def test_no_unpinned_out_of_month_or_duplicate_repair(tmp_path):
    plan = manifest(tmp_path)
    plan['sources'][0]['sha256'] = 'a'*64
    with pytest.raises(ValueError,match='pin'):
        load_facts(plan)
    plan = manifest(tmp_path)
    plan['keys'].append(plan['keys'][0])
    with pytest.raises(ValueError,match='duplicate'):
        load_facts(plan)
    plan = manifest(tmp_path)
    plan['keys'][0]['trade_date'] = '2026-08-31'
    with pytest.raises(ValueError,match='outside'):
        load_facts(plan)
