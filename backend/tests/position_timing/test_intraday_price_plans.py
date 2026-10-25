"""Price plans retain causal sources, complete populations and unknowns."""

from datetime import date, datetime, time, timedelta
from decimal import Decimal
import json
from pathlib import Path

import numpy as np
import pytest

from backend.services.position_timing.intraday.contracts import CHINA, PlanDraft, PriceConstraint
from backend.services.position_timing.intraday.price_plan_campaign import CampaignPriceSpec, build, inspect
from backend.services.position_timing.intraday.price_plans import PricePlanPolicy, PriceReference, build_price_plan
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import freeze
from backend.services.position_timing.intraday.store import IntradayError
from backend.services.strategy_package.manifest import compute_manifest_json_sha256


def clock(day, hour=9):
    return datetime.combine(day, time(hour), CHINA)


@pytest.mark.parametrize('side,bounds', [('BUY', ('9.97', '10.03')), ('SELL', ('10.03', '10.09'))])
def test_side_specific_raw_bounds_round_outward(side, bounds):
    day = date(2026, 1, 5)
    reference = PriceReference(symbol='000001.SZ', trade_date=day, reference_price_raw='10.03',
                               source_file=FileRef(path='/raw', sha256='a' * 64),
                               source_available_at=clock(day), reason_code='REFERENCE_AVAILABLE')
    result = build_price_plan(reference, side, PricePlanPolicy(), clock(day))
    interval = result.event['price_constraint']['intervals'][0]
    assert (Decimal(interval['lower']), Decimal(interval['upper'])) == tuple(map(Decimal, bounds))
    assert result.event['price_constraint']['source_sha256'] == reference.source_file.sha256
    cancelled = build_price_plan(reference, side, PricePlanPolicy(), clock(day, 10),
                                 revision=2, lifecycle='CANCELLED', previous=result.event)
    with pytest.raises(ValueError, match='active event series'):
        build_price_plan(reference, side, PricePlanPolicy(), clock(day, 11), revision=3, previous=cancelled.event)
    with pytest.raises(ValueError, match='availability clock'):
        build_price_plan(reference, side, PricePlanPolicy(), clock(day) - timedelta(seconds=1))


def campaign(tmp_path, *, later_price=float('nan')):
    # Small functional fixture; real delivery additionally checks the pinned full
    # consumer schemas against every event in the 181-day population.
    root = tmp_path / 'candidate'
    components = {}
    def component(relative, content):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        import hashlib
        components[relative] = {'path': relative, 'sha256': hashlib.sha256(content).hexdigest(), 'size': len(content)}
    group = 'components/daily_bin_candidate'
    component(group + '/calendars/day.txt', b'2026-01-05\n2026-01-06\n')
    for symbol, prices in [('000001.sz', [10, later_price]), ('600000.sh', [20, 21])]:
        component(f'{group}/features/{symbol}/prev_close.day.bin', np.array([0, *prices], dtype='<f4').tobytes())
    candidate = freeze(root / 'qe_dataset_manifest.json', {'schema_version': 'qe_dataset_manifest_v1', 'components': components})
    manifest, package_id = {}, 'fixture_package'
    manifest_sha = compute_manifest_json_sha256(manifest)
    package = freeze(tmp_path / 'package.json', {'package': {'package_id': package_id, 'manifest': manifest}})
    rows = []
    keys = ('symbol', 'rank', 'target_weight', 'target_quantity', 'reference_price',
            'selection_entry_price', 'suggested_entry_price_band', 'price_guard_policy_sha256')
    for day, cutoff, symbol in [('2026-01-05', '2025-12-31', '000001.SZ'), ('2026-01-06', '2026-01-05', '600000.SH')]:
        targets = [{k: symbol if k == 'symbol' else None for k in keys}]
        projection = freeze(tmp_path / f'{day}_projection.json', {
            'aggregate_results': targets, 'manifest_sha256_by_package': {package_id: manifest_sha},
            'runtime_config': {'point_in_time_context': {'cutoff_date': cutoff, 'effective_trade_date': day}}})
        completion = freeze(tmp_path / f'{day}_completion.json', {'consumer_scope': 'LOCALSIM', 'status': 'SIGNAL_COMMITTED',
                             'clocks': {'target_trade_date': day, 'data_cutoff': cutoff}})
        rows.append({'trade_date': day, 'data_cutoff': cutoff, 'target_count': 1, 'targets': targets,
                     'projection': projection, 'completion': completion})
    inventory = freeze(tmp_path / 'inventory.json', {'schema_version': 'localsim_timing_campaign_selection_inventory_v1',
                       'package_id': package_id, 'manifest_sha256': manifest_sha, 'days': rows, 'expected_trade_days': 2,
                       'missing_trade_dates': [], 'conflicting_trade_dates': []})
    preflight = freeze(tmp_path / 'preflight.json', {'schema_version': 'localsim_timing_campaign_preflight_v1',
                       'inputs': {'package': package, 'candidate_manifest': candidate}, 'selection_inventory': inventory,
                       'manifest_sha256': manifest_sha, 'package_id': package_id,
                       'start_trade_date': '2026-01-05', 'candidate_end_trade_date': '2026-01-06'})
    schema = freeze(tmp_path / 'fixture_schema.json', {'type': 'object'})
    return CampaignPriceSpec(preflight=preflight, templates_schema=schema, event_schema=schema,
                             producer_source_commit='1' * 40)


def read(ref):
    return json.loads(Path(ref['path']).read_text())


def test_whole_campaign_preserves_prior_sell_unknown_and_exact_retry(tmp_path):
    spec = campaign(tmp_path)
    result = build(spec, tmp_path / 'artifacts')
    templates = read(result['templates'])
    first, second = [read(day['events']) for day in templates['days']]
    assert {(e['symbol'], e['side']) for e in first} == {('000001.SZ', 'BUY'), ('000001.SZ', 'SELL')}
    assert {(e['symbol'], e['side']) for e in second} == {('600000.SH', 'BUY'), ('600000.SH', 'SELL'), ('000001.SZ', 'SELL')}
    unknown = next(e for e in second if e['symbol'] == '000001.SZ')
    assert unknown['price_constraint']['mode'] == 'NONE' and not unknown['price_constraint']['intervals']
    audit = read(result['reference_audit'])
    assert audit['unknown_references'][0]['reason_code'] == 'REFERENCE_NONFINITE'
    assert result['counts'] == {'BUY': 2, 'SELL': 3, 'AVAILABLE': 4, 'UNKNOWN': 1}
    assert result['consumer_status'] == 'CONSUMER_UNKNOWN_REFERENCE_SUPPORT_REQUIRED'
    # Existing C1 cannot consume this legal UNKNOWN. B0/C2 NONE stays valid.
    kwargs = dict(symbol='000001.SZ', side='SELL', target_trade_date='2026-01-06', source_ref='fixture', source_revision='1',
                  valid_from=unknown['valid_from'], valid_until=unknown['valid_until'], price_constraint=PriceConstraint())
    PlanDraft(**kwargs)
    with pytest.raises(ValueError, match='price touch requires'):
        PlanDraft(**kwargs, policy_id='PRICE_TOUCH_V1')
    retry = build(spec, tmp_path / 'artifacts')
    assert retry['status'] == 'ALREADY_MATERIALIZED' and retry['manifest'] == result['manifest']
    assert inspect(FileRef.model_validate(result['manifest']))['integrity'] == 'VERIFIED'
    path = Path(json.loads(Path(spec.preflight.path).read_text())['selection_inventory']['path'])
    path.write_text('{}')
    with pytest.raises(IntradayError, match='IDENTITY_MISMATCH'):
        inspect(FileRef.model_validate(result['manifest']))


def test_future_price_changes_do_not_change_prior_numeric_preference(tmp_path):
    outputs = [build(campaign(tmp_path / str(i), later_price=later), tmp_path / f'artifacts{i}')
               for i, later in enumerate((11, 99))]
    first = [read(read(r['templates'])['days'][0]['events'])[0]['price_constraint']['intervals'] for r in outputs]
    assert first[0] == first[1]


def test_missing_reference_is_unknown_but_changed_source_fails_closed(tmp_path):
    spec = campaign(tmp_path)
    raw = tmp_path / 'candidate/components/daily_bin_candidate/features/000001.sz/prev_close.day.bin'
    raw.unlink()
    result = build(spec, tmp_path / 'artifacts')
    assert read(result['reference_audit'])['unknown_references'][0]['reason_code'] == 'REFERENCE_FILE_MISSING'
    other = tmp_path / 'candidate/components/daily_bin_candidate/features/600000.sh/prev_close.day.bin'
    other.write_bytes(np.array([0, 1, 2], dtype='<f4').tobytes())
    with pytest.raises(ValueError, match='differs'):
        build(spec, tmp_path / 'artifacts')
