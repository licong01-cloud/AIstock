from copy import deepcopy
from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.economic_sector_daily_family_v1 import EconomicSectorPriceDailyFamilyV1, SCOPE_KEYS
from backend.services.advisory_model_first.economic_sector_daily_core_v1 import FEATURES
from backend.services.advisory_model_first.economic_sector_price_value_v1 import sector_price_set_v1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.realtime_feature_source import PriceRangeRealtimeContext
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.tests.advisory_model_first.test_economic_common_core_daily_source_v1 import packet as packet
from backend.tests.advisory_model_first.test_economic_sector_daily_core_v1 import sector_packet as sector_packet
from backend.tests.advisory_model_first.test_economic_sector_daily_source_v1 import build
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def setup(packet, sector_packet):
    source, core_db, sector_db, day = build(packet, sector_packet)
    fitted = sector_fit_fixture()
    scope = dict(package_id='original', program_id='program', manifest_sha256='a'*64,
        parent_policy_identity='b'*64, policy_identity='c'*64, universe_selection={'mode': 'stock_universe', 'pool_ids': []})
    checked = []
    bundle = SimpleNamespace(plan=SimpleNamespace(model_id='M1'), fitted=fitted, scope=scope,
        bundle_sha256='d'*64, verify_unchanged=lambda: checked.append(True),
        decision_use='NAVIGATION_ONLY', deployable=False, source_evidence='RECOVERED_LIMITED', native_identity='UNPROVEN')

    def classified(*, candidates):
        return sector_packet['classification_rows'].copy(), {'native_capture': False}

    consumer = EconomicSectorPriceDailyFamilyV1(bundle=bundle, source=source,
        classification_source=SimpleNamespace(load_day=classified))
    day.pop('classification_rows')
    day['scope'] = deepcopy(scope)
    day['price_contexts'] = {symbol: PriceRangeRealtimeContext(symbol, 10., day['calendar'][-2],
        'market.kline_daily_raw.close_li', 1000., 1., 'D_VISIBLE_NO_ACTION', 'MAIN', date(2000, 1, 1),
        100, False, .01) for symbol in day['candidates'].instrument}
    return consumer, day, source, core_db, sector_db, checked


def test_family_single_batch_real_source_and_original_price_math_are_identical(packet, sector_packet):
    consumer, day, source, core_db, sector_db, _ = setup(packet, sector_packet)
    single = consumer.predict_day(**day)
    batch, _ = consumer.predict_batch(packets=[deepcopy(day), deepcopy(day)])
    # Read timestamps are disclosure, not a reason to alter the price result.
    assert single['candidates'] == batch['candidates']
    inputs = {key: day[key] for key in ('candidates', 'calendar', 'component_roles', 'terminal_weights')}
    frame, _ = source.load_day(**inputs, classification_rows=sector_packet['classification_rows'])
    for actual, row in zip(single['candidates'], frame.to_dict('records'), strict=True):
        expected = sector_price_set_v1(fitted=consumer._bundle.fitted, d_features={name: row[name] for name in FEATURES},
            arm='candidate', reference_cny=10., legal_low_cny=9., legal_high_cny=11.)
        assert tuple((band['low_cny'], band['high_cny']) for band in actual['intervals']) == expected.intervals_cny
        assert actual['legal_node_count'] == 201 and actual['unknown_node_count'] == 159
        assert len(actual['intervals']) == 2 and all(band['expected_net_bps_min'] > 0 for band in actual['intervals'])
    assert single['model_family'] == 'M1_SECTOR_PRICE_VALUE_V1'
    assert single['prediction_sha256'] == sha({key: value for key, value in single.items() if key != 'prediction_sha256'})
    assert not single['package_qualification_rechecked'] and not single['deployable']
    assert 'NOT_WIN_PROBABILITY' in single['expected_net_semantics']
    assert core_db.rollbacks == sector_db.rollbacks == 3


def test_missing_sector_and_price_context_keep_every_original_candidate(packet, sector_packet):
    sector_packet['classification_rows'].loc[0, 'classification_l2_code'] = None
    consumer, day, _, _, _, _ = setup(packet, sector_packet)
    day['price_contexts'].pop(day['candidates'].instrument.iloc[1])
    result = consumer.predict_day(**day)
    assert [row['instrument'] for row in result['candidates']] == day['candidates'].instrument.tolist()
    assert [row['status'] for row in result['candidates']] == ['UNKNOWN_INPUT_OR_SUPPORT', 'QUERY_DOMAIN_UNAVAILABLE']
    assert all(not row['intervals'] for row in result['candidates'])


@pytest.mark.parametrize('field', SCOPE_KEYS)
def test_wrong_bundle_scope_is_input_error_before_source_not_package_qualification(packet, sector_packet, field):
    consumer, day, _, core_db, _, _ = setup(packet, sector_packet)
    day['scope'][field] = 'another'
    with pytest.raises(AdvisoryModelFirstError, match='scope'):
        consumer.predict_day(**day)
    assert not core_db.calls


@pytest.mark.parametrize('poison', ['future_price', 'foreign_symbol', 'bool_tick', 'bad_list_date', 'no_limit', 'over_budget'])
def test_price_coordinates_and_complete_grid_budget(packet, sector_packet, poison):
    consumer, day, _, _, _, _ = setup(packet, sector_packet)
    symbol = day['candidates'].instrument.iloc[0]
    fields = dict(future_price={'decision_price_trade_date': day['calendar'][-1]}, foreign_symbol={'symbol': '600000.SH'},
        bool_tick={'tick_size': True}, no_limit={'board_type': 'STAR', 'listed_trading_days': 1},
        bad_list_date={'list_date': '2000-01-01'}, over_budget={'decision_raw_close': 1000.})
    day['price_contexts'][symbol] = replace(day['price_contexts'][symbol], **fields[poison])
    if poison in ('no_limit', 'over_budget'):
        actual = consumer.predict_day(**day)['candidates'][0]
        assert actual['status'] == ('QUERY_DOMAIN_UNAVAILABLE' if poison == 'no_limit' else 'QUERY_DOMAIN_OVER_BUDGET')
        assert actual['legal_node_count'] == 0 and not actual['intervals']
    else:
        with pytest.raises(AdvisoryModelFirstError):
            consumer.predict_day(**day)


def test_avoid_empty_roster_source_content_and_budget_remain_distinct(packet, sector_packet):
    consumer, day, source, _, _, checked = setup(packet, sector_packet)
    fitted = consumer._bundle.fitted
    # A real JSON head that predicts a negative net value, not a threshold override.
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import sector_fit_identity_v1
    fitted.models['candidate_mean']['initial'] = .95
    consumer._bundle.fitted = replace(fitted, model_sha256=sector_fit_identity_v1(fitted.recipe, fitted.models, fitted.support))
    assert all(row['status'] == 'NO_ACCEPTABLE_PRICE' for row in consumer.predict_day(**day)['candidates'])
    day['candidates'] = day['candidates'].iloc[:0]
    sector_packet['classification_rows'] = sector_packet['classification_rows'].iloc[:0]
    day['price_contexts'] = {}
    assert consumer.predict_day(**day)['status'] == 'NO_CANDIDATES'
    assert len(checked) == 4
    with pytest.raises(AdvisoryModelFirstError):
        consumer.predict_batch(packets=[day]*21)
    original = source.load_batch

    def poisoned(**kwargs):
        outputs = original(**kwargs)
        outputs[0][1]['feature_sha256'] = 'f'*64
        return outputs

    source.load_batch = poisoned
    with pytest.raises(AdvisoryModelFirstError, match='content'):
        consumer.predict_day(**day)


def test_source_reordering_cannot_change_original_candidate_identity(packet, sector_packet):
    consumer, day, source, _, _, _ = setup(packet, sector_packet)
    original = source.load_batch

    def poisoned(**kwargs):
        outputs = original(**kwargs)
        frame, receipt = outputs[0]
        reordered = frame.iloc[::-1].reset_index(drop=True)
        from backend.services.advisory_model_first.economic_daily_feature_core_v1 import _records
        receipt['feature_sha256'] = sha(_records(reordered))
        receipt['identity_sha256'] = sha({key: value for key, value in receipt.items() if key != 'identity_sha256'})
        return [(reordered, receipt)]

    source.load_batch = poisoned
    with pytest.raises(AdvisoryModelFirstError):
        consumer.predict_day(**day)


def test_object_empty_roster_preserves_source_content_validation(packet, sector_packet):
    consumer, day, source, _, _, _ = setup(packet, sector_packet)
    day['candidates'] = day['candidates'].iloc[:0].astype(object)
    sector_packet['classification_rows'] = sector_packet['classification_rows'].iloc[:0].astype(object)
    day['price_contexts'] = {}
    result = consumer.predict_day(**day)
    assert result['status'] == 'NO_CANDIDATES' and result['candidates'] == []
    assert result['fit_count'] == 0 and result['database_written'] is result['outcomes_read'] is False
    original = source.load_batch

    def foreign_row(**kwargs):
        from backend.services.advisory_model_first.economic_daily_feature_core_v1 import _records
        frame, receipt = original(**kwargs)[0]
        frame.loc[0] = [*day['calendar'][-2:], '600000.SH', *[float('nan')]*len(FEATURES)]
        receipt['feature_sha256'] = sha(_records(frame))
        receipt['identity_sha256'] = sha({key: value for key, value in receipt.items() if key != 'identity_sha256'})
        return [(frame, receipt)]

    source.load_batch = foreign_row
    with pytest.raises(AdvisoryModelFirstError, match='content'):
        consumer.predict_day(**day)


def test_pandas_row_labels_are_not_candidate_identity_and_numeric_strings_are_not_features(packet, sector_packet):
    consumer, day, source, _, _, _ = setup(packet, sector_packet)
    day['candidates'].index = [700, 701]
    sector_packet['classification_rows'].index = [700, 701]
    assert len(consumer.predict_day(**day)['candidates']) == 2
    original = source.load_batch

    def poisoned(**kwargs):
        from backend.services.advisory_model_first.economic_daily_feature_core_v1 import _records
        frame, receipt = original(**kwargs)[0]
        frame[FEATURES[0]] = frame[FEATURES[0]].astype(object)
        frame.loc[0, FEATURES[0]] = '0.05'
        receipt['feature_sha256'] = sha(_records(frame))
        receipt['identity_sha256'] = sha({key: value for key, value in receipt.items() if key != 'identity_sha256'})
        return [(frame, receipt)]

    source.load_batch = poisoned
    with pytest.raises(AdvisoryModelFirstError):
        consumer.predict_day(**day)
