import json
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_limit_state_v1 import CLOCK, LIMIT_STATE_FEATURES, STATUS, UNKNOWN, LimitStatePlanV1, limit_state_fit_identity_v1, limit_state_rows_v1, train_limit_state_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def limit_state_fixture():
    calendar = pd.bdate_range('2025-01-02', periods=23)
    candidates = pd.DataFrame([{KEY[0]: calendar[index], KEY[1]: calendar[index+1], KEY[2]: '000001.SZ'} for index in (0, 19, 20)])
    prices = pd.DataFrame({'trade_date': calendar, 'instrument': '000001.SZ', 'raw_high_cny': 10.5, 'raw_low_cny': 9.5,
        'raw_close_cny': 10., 'up_limit': 11., 'down_limit': 9.})
    prices.loc[[0, 1], 'raw_high_cny'] = 11.
    prices.loc[[2, 3, 4], 'raw_low_cny'] = 9.
    return dict(candidates=candidates, prices=prices, calendar=calendar)


def limit_state_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), limit_state_features=list(LIMIT_STATE_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        limit_state_fit_identity_v1(recipe, original.models, original.support))


def test_manual_original_order_same_OHLC_different_limit_information_and_known_zero_one():
    args = limit_state_fixture()
    result = limit_state_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result[STATUS].tolist() == ['UNKNOWN_20D_HISTORY', 'AVAILABLE', 'AVAILABLE']
    assert result.loc[0, LIMIT_STATE_FEATURES[2]] == .5
    np.testing.assert_allclose(result.loc[1, list(LIMIT_STATE_FEATURES)].astype(float), [.1, .15, .5])
    args['prices'].up_limit = 12.
    args['prices'].down_limit = 8.
    np.testing.assert_allclose(limit_state_rows_v1(**args).loc[1, list(LIMIT_STATE_FEATURES)].astype(float), [0., 0., .5])
    args['prices'].raw_high_cny = 12.
    args['prices'].raw_low_cny = 8.
    args['prices'].raw_close_cny = 12.
    np.testing.assert_allclose(limit_state_rows_v1(**args).loc[1, list(LIMIT_STATE_FEATURES)].astype(float), [1., 1., 1.])


@pytest.mark.parametrize(('column', 'index', 'unknown'), [('raw_high_cny', 5, (0,)), ('up_limit', 5, (0,)),
    ('raw_low_cny', 5, (1,)), ('down_limit', 5, (1,)), ('raw_close_cny', 19, (2,)), ('up_limit', 19, (0, 2))])
def test_missing_is_dependency_local_not_deleted_or_filled(column, index, unknown):
    args = limit_state_fixture()
    args['prices'].loc[index, column] = np.nan
    result = limit_state_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY])
    assert set(json.loads(result.loc[1, UNKNOWN])) == {LIMIT_STATE_FEATURES[position] for position in unknown}
    for position in set(range(3))-set(unknown):
        assert pd.notna(result.loc[1, LIMIT_STATE_FEATURES[position]])


def test_missing_whole_bar_quiet_NaN_equal_corridor_and_half_tick_without_interior_clip():
    args = limit_state_fixture()
    args['prices'] = args['prices'].drop(index=5)
    result = limit_state_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result.loc[1, LIMIT_STATE_FEATURES[2]] == .5
    assert set(json.loads(result.loc[1, UNKNOWN])) == set(LIMIT_STATE_FEATURES[:2])
    args = limit_state_fixture()
    args['prices']['raw_high_cny'] = args['prices'].raw_high_cny.astype(object)
    args['prices'].loc[5, 'raw_high_cny'] = Decimal('NaN')
    assert set(json.loads(limit_state_rows_v1(**args).loc[1, UNKNOWN])) == {LIMIT_STATE_FEATURES[0]}
    args['candidates'] = args['candidates'].iloc[:1]
    args['prices'].loc[0, ['raw_close_cny', 'up_limit', 'down_limit']] = 10.
    assert json.loads(limit_state_rows_v1(**args).loc[0, UNKNOWN])[LIMIT_STATE_FEATURES[2]] == 'UNKNOWN_LIMIT_CORRIDOR'
    args['prices'].loc[0, ['up_limit', 'down_limit']] = [11., 9.]
    for close, expected in ((8.996, 0.), (11.004, 1.), (9.001, .0005), (10.999, .9995)):
        args['prices'].loc[0, 'raw_close_cny'] = close
        assert limit_state_rows_v1(**args).loc[0, LIMIT_STATE_FEATURES[2]] == pytest.approx(expected)


@pytest.mark.parametrize('bad', [True, 0., np.inf, 'bad', 10**1000, Decimal('sNaN'), Decimal('1e-10000')])
def test_consumed_bad_number_is_not_missing(bad):
    args = limit_state_fixture()
    args['prices']['raw_high_cny'] = args['prices'].raw_high_cny.astype(object)
    args['prices'].loc[5, 'raw_high_cny'] = bad
    with pytest.raises(ValueError):
        limit_state_rows_v1(**args)


@pytest.mark.parametrize('defect', ['high_low', 'upper_lower', 'high_bound', 'low_bound', 'D_close_bound'])
def test_known_true_price_conflicts_fail_calculation(defect):
    args = limit_state_fixture()
    index, column, value = {'high_low': (5, 'raw_high_cny', 9.), 'upper_lower': (5, 'down_limit', 12.),
        'high_bound': (5, 'raw_high_cny', 11.01), 'low_bound': (5, 'raw_low_cny', 8.99),
        'D_close_bound': (19, 'raw_close_cny', 11.01)}[defect]
    args['prices'].loc[index, column] = value
    with pytest.raises(ValueError, match='bounds'):
        limit_state_rows_v1(**args)


def test_unconsumed_historical_close_future_and_warmup_prices_do_not_form_gates():
    args = limit_state_fixture()
    args['candidates'] = args['candidates'].iloc[1:2].reset_index(drop=True)
    expected = limit_state_rows_v1(**args)
    args['prices']['raw_close_cny'] = args['prices'].raw_close_cny.astype(object)
    args['prices'].loc[:18, 'raw_close_cny'] = 'not consumed'
    args['prices'].loc[20:, ['raw_high_cny', 'raw_low_cny', 'up_limit', 'down_limit']] = -999.
    pd.testing.assert_frame_equal(expected, limit_state_rows_v1(**args))
    args = limit_state_fixture()
    args['candidates'] = args['candidates'].iloc[:1]
    args['prices'].loc[0, ['raw_high_cny', 'raw_low_cny']] = -999.
    assert limit_state_rows_v1(**args).loc[0, LIMIT_STATE_FEATURES[2]] == .5


def test_duplicate_wrong_next_session_and_empty_schema():
    args = limit_state_fixture()
    with pytest.raises(Exception, match='duplicate'):
        limit_state_rows_v1(**{**args, 'prices': pd.concat([args['prices'], args['prices'].iloc[:1]], ignore_index=True)})
    wrong = args['candidates'].copy()
    wrong.loc[0, KEY[1]] = args['calendar'][2]
    with pytest.raises(ValueError, match='next session'):
        limit_state_rows_v1(**{**args, 'candidates': wrong})
    result = limit_state_rows_v1(**{**args, 'candidates': args['candidates'].iloc[:0]})
    assert result.empty and result.columns.tolist() == [*KEY, *LIMIT_STATE_FEATURES, STATUS, CLOCK, UNKNOWN]


def test_common_mature_13_16_training_no_test_consumption_and_support_not_filtered():
    rows, configuration = rows_fixture()
    rows[list(LIMIT_STATE_FEATURES)] = [.1, .15, .5]
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, LIMIT_STATE_FEATURES[0]] = np.nan
    rows.loc[0, STATUS] = 'UNKNOWN_LIMIT_SOURCE'
    events = []
    fitted = train_limit_state_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    assert fitted.support.contains(20.)
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *LIMIT_STATE_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_limit_state_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_limit_state_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_fixed59_plan_no_policy_search_or_activation():
    from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='limit_state_predecessor'))
    plan = LimitStatePlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 59 and plan.parameters['source_select_budget'] == 0 and not plan.deployable
    for update in ({'window_sessions': 60}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'model_id': 'M14'}):
        with pytest.raises(ValueError):
            LimitStatePlanV1.model_validate({**values, **update})
