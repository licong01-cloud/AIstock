from datetime import date
from types import SimpleNamespace

import numpy as np
import pytest

from backend.services.dataset_release.monthly_minute_coverage import (
    calendar_sessions, historical_catalog_gaps, patch_minute_fields,
    automatic_repair_plan, capture_historical_coverage,
)
from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS


def _bin(path, values):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(np.array(values, dtype='<f4').tobytes())


def test_population_history_is_not_limited_to_tail_month(tmp_path):
    provider = tmp_path / 'minute'
    (provider / 'instruments').mkdir(parents=True)
    (provider / 'instruments/all.txt').write_text('600811.SH\t2024-01-02\t2024-12-13\n')
    spans = [('600811.SH', '2024-12-13', '2025-03-14'), ('601989.SH', '2025-07-08', '2025-08-06')]
    days = ['2024-12-13', '2024-12-16', '2025-03-14', '2025-07-08']
    assert historical_catalog_gaps(spans, provider=provider, days=days, before=date(2026, 9, 1)) == [
        {'ts_code': '600811.SH', 'trade_date': '2024-12-16'},
        {'ts_code': '600811.SH', 'trade_date': '2025-03-14'},
        {'ts_code': '601989.SH', 'trade_date': '2025-07-08'},
    ]


def test_calendar_preserves_auction_only_where_frozen(tmp_path):
    calendar = tmp_path / '1min.txt'
    calendar.write_text('2024-07-01 09:30:00\n2024-07-01 09:31:00\n2025-07-08 09:31:00\n')
    assert calendar_sessions(calendar) == {'2024-07-01': ['2024-07-01 09:30:00', '2024-07-01 09:31:00'],
        '2025-07-08': ['2025-07-08 09:31:00']}
    calendar.write_text('2024-07-01 09:31:00\n2024-07-01 09:31:00\n')
    with pytest.raises(ValueError, match='ordered'):
        calendar_sessions(calendar)


def _providers(root):
    daily, minute = root / 'daily', root / 'minute'
    for field, value in [('factor', 0.5), ('up_limit_price', 11), ('down_limit_price', 9), ('prev_close', 10)]:
        _bin(daily / f'features/600811.sh/{field}.day.bin', [0, value])
    return daily, minute


def _rows():
    return [{'ts_code': '600811.SH', 'trade_time': '2024-12-16 09:30:00', 'open_li': 10000,
        'high_li': 10500, 'low_li': 9900, 'close_li': 10200, 'volume_hand': 2,
        'amount_li': 2040000, 'volume_shares': None, 'volume_shares_source': None,
        'volume_shares_sha256': None}]


def test_patch_all_execution_fields_without_overwriting_or_filling_other_times(tmp_path):
    daily, minute = _providers(tmp_path)
    stamps = ['2024-12-16 09:30:00', '2024-12-16 09:31:00']
    patches, summary = patch_minute_fields(rows=_rows(), daily=daily, minute=minute,
        day_calendar=['2024-12-16'], minute_calendar=stamps)
    assert len(patches) == len(QLIB_STOCK_FIELDS) == 12
    assert summary['filled_cells'] == 12
    assert np.frombuffer(patches['features/600811.sh/open.1min.bin'], dtype='<f4').tolist() == [0, 5]
    assert np.frombuffer(patches['features/600811.sh/amount.1min.bin'], dtype='<f4').tolist() == [0, 2040]
    assert np.frombuffer(patches['features/600811.sh/volume.1min.bin'], dtype='<f4').tolist() == [0, 400]
    # Apply to a private copy. A second execution is a no-op, not a new identity.
    for relative, raw in patches.items():
        path = minute / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    assert patch_minute_fields(rows=_rows(), daily=daily, minute=minute,
        day_calendar=['2024-12-16'], minute_calendar=stamps)[0] == {}
    _bin(minute / 'features/600811.sh/close.1min.bin', [0, 99])
    with pytest.raises(ValueError, match='overwrite'):
        patch_minute_fields(rows=_rows(), daily=daily, minute=minute,
            day_calendar=['2024-12-16'], minute_calendar=stamps)


def test_patch_duplicate_and_noncalendar_rows_fail_closed(tmp_path):
    daily, minute = _providers(tmp_path)
    for rows in (_rows() * 2, [{**_rows()[0], 'trade_time': '2024-12-16 13:00:00'}]):
        with pytest.raises(ValueError, match='calendar|duplicated'):
            patch_minute_fields(rows=rows, daily=daily, minute=minute,
                day_calendar=['2024-12-16'], minute_calendar=['2024-12-16 09:30:00'])


def test_history_capture_uses_daily_fact_catalog_not_pit_and_same_snapshot(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_minute_repair
    provider = tmp_path / 'components/minute'
    (provider / 'instruments').mkdir(parents=True)
    (provider / 'calendars').mkdir()
    (provider / 'instruments/all.txt').write_text('600811.SH\t2024-01-02\t2024-12-13\n')
    (provider / 'calendars/1min.txt').write_text('2024-12-16 09:30:00\n2024-12-16 09:31:00\n2024-12-16 13:00:00\n')
    daily = tmp_path / 'components/daily/instruments'
    daily.mkdir(parents=True)
    (daily / 'all.txt').write_text('600811.SH\t2024-12-16\t2025-03-14\n')
    prefix = SimpleNamespace(root=tmp_path, manifest={'components': {
        'minute_meta_export': {'path': 'components/minute/meta.json'},
        'daily_meta_export': {'path': 'components/daily/meta.json'}}})
    # ST/exited securities may have no buy-eligible interval at all.
    pit = SimpleNamespace(spans=())
    connection = object()
    def capture(inputs, **kwargs):
        assert inputs == {'stock_dates': [{'ts_code': '600811.SH', 'trade_date': '2024-12-16'}]}
        assert kwargs['connection'] is connection
        assert kwargs['frozen_sessions']['2024-12-16'] == ['2024-12-16 09:30:00', '2024-12-16 09:31:00']
        assert kwargs['allow_absent'] is True
        return {'facts': ['sealed']}
    monkeypatch.setattr(monthly_minute_repair, 'capture_database_minutes', capture)
    assert capture_historical_coverage(prefix=prefix, pit=pit, cutoff=date(2026,9,30), connection=connection) == {'facts': ['sealed']}
    # A new daily identity missing from BOTH old catalogues must not fall
    # through the month-only minute tail. Its exact observed date is captured.
    (daily / 'all.txt').write_text('')
    assert capture_historical_coverage(prefix=prefix, pit=pit, cutoff=date(2026,9,30), connection=connection,
        additional_quote_days=(('600811.SH','2024-12-16'),)) == {'facts': ['sealed']}


def test_auto_binding_uses_frozen_bytes_and_rejects_conflicting_explicit_fact(tmp_path):
    from backend.services.dataset_release.cas_store import CASStore
    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.monthly_minute_repair import SCHEMA, BINDING
    ControlStore.initialize(tmp_path)
    cas = CASStore(tmp_path)
    authority = {'schema_version': SCHEMA, 'inputs': {'stock_dates': []}, 'facts': _rows(), 'stock_dates': 1, 'row_count': 1}
    reference = cas.put_json(authority).as_dict()
    plan = {'target_cutoff': '2026-09-30', 'predecessor': {'dataset_manifest_sha256': 'a'*64}}
    prepared = automatic_repair_plan(plan, build_inputs={'historical_minute_coverage_ref': reference}, cas=cas, operation_id='dmr_test')
    assert prepared[BINDING]['authority_ref'] == reference
    assert prepared[BINDING]['operation_id'] == 'dmr_test'
    assert BINDING not in plan  # caller's durable plan is not mutated
    other = {**authority, 'facts': [{**_rows()[0], 'close_li': 12000}]}
    explicit = {**plan, BINDING: {'authority_ref': cas.put_json(other).as_dict()}}
    with pytest.raises(ValueError, match='conflict'):
        automatic_repair_plan(explicit, build_inputs={'historical_minute_coverage_ref': reference}, cas=cas, operation_id='dmr_test')
