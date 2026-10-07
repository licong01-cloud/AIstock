from datetime import date
import hashlib
import json

import pytest

from scripts.backfill_raw_volume_precision import load_facts, SCHEMA


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
