import json
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.services.advisory_model_first.economic_traded_price_distribution_v1 import CLOCK, STATUS, TRADED_PRICE_FEATURES, UNKNOWN, TradedPriceDistributionPlanV1, traded_price_distribution_rows_v1, traded_price_fit_identity_v1, train_traded_price_v1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def traded_price_fixture():
    calendar = pd.bdate_range('2025-01-02', periods=23)
    candidates = pd.DataFrame([{KEY[0]: calendar[index], KEY[1]: calendar[index+1], KEY[2]: '000001.SZ'} for index in (0, 19, 20)])
    prices = pd.DataFrame({'trade_date': calendar, 'instrument': '000001.SZ',
        'raw_close_cny': [8., 12.]*9+[10., 10., 11., 12., 13.], 'adj_factor': 1.})
    volumes = prices[['trade_date', 'instrument']].assign(volume_hand=1.)
    atr = candidates.assign(atr14_close=.125, feature_visible_through=candidates[KEY[0]])
    return dict(candidates=candidates, prices=prices, volumes=volumes, atr=atr, calendar=calendar)


def traded_price_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), traded_price_distribution_features=list(TRADED_PRICE_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        traded_price_fit_identity_v1(recipe, original.models, original.support))


def test_three_manual_fields_price_distribution_is_not_mean_or_date_HHI():
    args = traded_price_fixture()
    result = traded_price_distribution_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result[STATUS].tolist() == ['UNKNOWN_20D_HISTORY', 'AVAILABLE', 'AVAILABLE']
    np.testing.assert_allclose(result.loc[1, list(TRADED_PRICE_FEATURES)].astype(float), [.55, np.sqrt(.036), .1])
    original_mean = args['prices'].raw_close_cny.iloc[:20].mean()
    args['prices'].raw_close_cny = 10.
    assert original_mean == args['prices'].raw_close_cny.iloc[:20].mean()
    changed = traded_price_distribution_rows_v1(**args)
    np.testing.assert_allclose(changed.loc[1, list(TRADED_PRICE_FEATURES)].astype(float), [1., 0., 1.])


@pytest.mark.parametrize('case', ['common_scale', 'only_D_volume', 'zero_volume', 'zero_ATR', 'inclusive_ATR'])
def test_normal_coordinates_zero_volume_and_exact_price_boundaries(case):
    args = traded_price_fixture()
    expected = traded_price_distribution_rows_v1(**args)
    if case == 'common_scale':
        args['prices'].adj_factor *= 1e250
        args['volumes'].volume_hand *= 1e250
        pd.testing.assert_frame_equal(expected, traded_price_distribution_rows_v1(**args))
        return
    if case in ('only_D_volume', 'zero_volume'):
        args['volumes'].volume_hand = 0.
        args['prices']['raw_close_cny'] = args['prices'].raw_close_cny.astype(object)
        args['prices'].loc[:18, 'raw_close_cny'] = 'unconsumed poison'
        if case == 'only_D_volume':
            args['volumes'].loc[19, 'volume_hand'] = 1.
        else:
            result = traded_price_distribution_rows_v1(**args)
            assert result.loc[1, STATUS] == 'UNKNOWN_NO_TRADED_VOLUME'
            assert result.loc[1, list(TRADED_PRICE_FEATURES)].isna().all()
            return
    else:
        args['prices'].raw_close_cny = 10.
        args['prices'].loc[:18, 'raw_close_cny'] = 8.75
        args['atr'].atr14_close = 0. if case == 'zero_ATR' else .125
    result = traded_price_distribution_rows_v1(**args)
    target = [1., 0., 1.] if case == 'only_D_volume' else [1., np.sqrt(.95*.05)*.125, .05 if case == 'zero_ATR' else 1.]
    # The 19 old prices carry weight .95 and D carries .05.
    np.testing.assert_allclose(result.loc[1, list(TRADED_PRICE_FEATURES)].astype(float), target)


@pytest.mark.parametrize('missing', ['volume_row', 'price', 'D_anchor', 'atr', 'clock'])
def test_unknown_is_field_local_and_preserves_original_roster(missing):
    args = traded_price_fixture()
    if missing == 'volume_row':
        args['volumes'] = args['volumes'].drop(index=5)
    elif missing in ('price', 'D_anchor'):
        args['prices'].loc[5 if missing == 'price' else 19, 'adj_factor'] = np.nan
    elif missing == 'atr':
        args['atr'].loc[1, 'atr14_close'] = np.nan
    else:
        args['atr'].loc[1, 'feature_visible_through'] = pd.NaT
    result = traded_price_distribution_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY])
    assert result.loc[1, STATUS].startswith('UNKNOWN')
    unknown = json.loads(result.loc[1, UNKNOWN])
    assert set(unknown) == set(TRADED_PRICE_FEATURES[2:] if missing in ('atr', 'clock') else TRADED_PRICE_FEATURES)
    if missing in ('atr', 'clock'):
        assert result.loc[1, list(TRADED_PRICE_FEATURES[:2])].notna().all()


@pytest.mark.parametrize('bad', [True, -.01, np.inf, 'bad', Decimal('sNaN'), 10**1000])
def test_bad_consumed_values_are_errors_not_missing(bad):
    args = traded_price_fixture()
    args['volumes']['volume_hand'] = args['volumes'].volume_hand.astype(object)
    args['volumes'].loc[5, 'volume_hand'] = bad
    with pytest.raises(ValueError):
        traded_price_distribution_rows_v1(**args)


@pytest.mark.parametrize('defect', ['duplicate_volume', 'duplicate_ATR', 'duplicate_price', 'future_clock', 'wrong_T'])
def test_consumed_identity_contradictions_fail_closed(defect):
    args = traded_price_fixture()
    if defect.startswith('duplicate'):
        name = {'duplicate_volume': 'volumes', 'duplicate_price': 'prices', 'duplicate_ATR': 'atr'}[defect]
        args[name] = pd.concat([args[name], args[name].iloc[:1]], ignore_index=True)
    elif defect == 'future_clock':
        args['atr'].loc[1, 'feature_visible_through'] = args['calendar'][20]
    else:
        args['candidates'].loc[1, KEY[1]] = args['calendar'][21]
    with pytest.raises(Exception, match='duplicate|future|next session'):
        traded_price_distribution_rows_v1(**args)


def test_projection_future_poison_empty_and_zero_weight_price_not_consumed():
    args = traded_price_fixture()
    args['candidates'] = args['candidates'].iloc[1:2].reset_index(drop=True)
    expected = traded_price_distribution_rows_v1(**args)
    args['prices'].loc[20:, 'raw_close_cny'] = -999.
    args['volumes'].loc[20:, 'volume_hand'] = -999.
    args['atr'].loc[2, 'atr14_close'] = -999.
    args['atr'].loc[2, 'feature_visible_through'] = pd.Timestamp('2030-01-01')
    pd.testing.assert_frame_equal(expected, traded_price_distribution_rows_v1(**args))
    args['volumes'].loc[5, 'volume_hand'] = 0.
    args['prices'] = args['prices'].drop(index=5)
    assert traded_price_distribution_rows_v1(**args).loc[0, STATUS] == 'AVAILABLE'
    args['candidates'] = args['candidates'].iloc[:0]
    empty = traded_price_distribution_rows_v1(**args)
    assert empty.empty and empty.columns.tolist() == [*KEY, *TRADED_PRICE_FEATURES, STATUS, CLOCK, UNKNOWN]


def test_fixed_training_same_mature_13_16_and_test_has_no_fit_effect():
    rows, configuration = rows_fixture()
    rows[list(TRADED_PRICE_FEATURES)] = [.55, .1, .4]
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, TRADED_PRICE_FEATURES[0]] = np.nan
    rows.loc[0, STATUS] = 'UNKNOWN_PRICE_HISTORY'
    events = []
    fitted = train_traded_price_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *TRADED_PRICE_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_traded_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_traded_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_plan43_is_frozen_and_does_not_allow_new_policy_or_activation():
    from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='traded_price_distribution_predecessor'),
        volume_snapshot_manifest_ref=dict(artifact_uri='F:/unit/campaign/volume/prepared/manifest.json', sha256='f'*64, size_bytes=1, role='traded_price_distribution_volume_snapshot'))
    plan = TradedPriceDistributionPlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 43 and plan.parameters['source_select_budget'] == 0 and not plan.deployable
    for update in ({'window_sessions': 60}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'model_id': 'M10'}):
        with pytest.raises(ValueError):
            TradedPriceDistributionPlanV1.model_validate({**values, **update})
