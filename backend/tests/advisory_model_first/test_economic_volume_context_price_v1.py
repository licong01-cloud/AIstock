from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1, information_price_set_v1
from backend.services.advisory_model_first.economic_volume_context_price_v1 import VOLUME_CONTEXT_FEATURES, VolumeContextPricePlanV1, train_volume_context_price_v1, volume_context_fit_identity_v1, volume_context_nodes_v1, volume_context_requests_v1, volume_context_rows_v1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_price_path_value_v1 import price_path_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def volume_context_fixture():
    args = price_path_fixture()
    args['volumes'] = args['prices'][['trade_date', 'instrument']].assign(volume_hand=np.arange(1, 22, dtype=float))
    return args


def volume_context_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), volume_context_features=list(VOLUME_CONTEXT_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        volume_context_fit_identity_v1(recipe, original.models, original.support))


def test_fixed_plan35_and_manual_three_fields_original_keys():
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='volume_context_predecessor'))
    plan = VolumeContextPricePlanV1(**values)
    assert plan.parameters['physical_fit_budget'] == 4 and plan.parameters['campaign_fit_budget'] == 35 and not plan.deployable
    for update in ({'window_sessions': 60}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'},
                   {'predecessor_manifest_ref': {**values['predecessor_manifest_ref'], 'artifact_uri': 'C:/unit/prior/evaluated/manifest.json'}}):
        with pytest.raises(ValueError):
            VolumeContextPricePlanV1.model_validate({**values, **update})
    args = volume_context_fixture()
    result = volume_context_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY])
    assert result.volume_context_feature_status.tolist() == ['UNKNOWN_20D_WARMUP', 'AVAILABLE', 'AVAILABLE']
    np.testing.assert_allclose(result.loc[1, list(VOLUME_CONTEXT_FEATURES)].astype(float), [11/(2158/210)-1, -1/209, 2870/210**2])
    requests = volume_context_requests_v1(candidates=args['candidates'], calendar=args['calendar'])
    assert len(requests) == 21 and requests[0] == (args['calendar'][0], '000001.SZ')


def test_scales_split_coordinates_and_future_volume_price_factor_poison():
    args = volume_context_fixture()
    expected = volume_context_rows_v1(**args)
    adjusted = {**args, 'prices': args['prices'].copy(), 'volumes': args['volumes'].copy()}
    adjusted['prices'].raw_close_cny *= 3
    adjusted['prices'].adj_factor *= 7
    adjusted['volumes'].volume_hand *= 11
    np.testing.assert_allclose(expected.loc[:, VOLUME_CONTEXT_FEATURES], volume_context_rows_v1(**adjusted).loc[:, VOLUME_CONTEXT_FEATURES], atol=1e-12)
    split = {**args, 'prices': args['prices'].copy(), 'volumes': args['volumes'].copy()}
    split['prices'].loc[10:, 'raw_close_cny'] /= 2
    split['prices'].loc[10:, 'adj_factor'] *= 2
    split['volumes'].loc[10:, 'volume_hand'] *= 2
    pd.testing.assert_frame_equal(expected, volume_context_rows_v1(**split))
    early = {**args, 'candidates': args['candidates'].iloc[[1]].copy()}
    poisoned = {**early, 'prices': args['prices'].copy(), 'volumes': args['volumes'].copy()}
    poisoned['prices'].loc[20, ['raw_close_cny', 'adj_factor']] = -99999
    poisoned['volumes'].loc[20, 'volume_hand'] = -99999
    pd.testing.assert_frame_equal(volume_context_rows_v1(**early), volume_context_rows_v1(**poisoned))


@pytest.mark.parametrize('defect', ['missing_quantity', 'null_quantity', 'missing_price', 'all_zero', 'only_first_traded'])
def test_normal_source_gaps_zero_quantity_keep_unknown_and_all_original_keys(defect):
    args = volume_context_fixture()
    if defect == 'missing_quantity':
        args['volumes'] = args['volumes'].iloc[1:].copy()
    elif defect == 'null_quantity':
        args['volumes']['volume_hand'] = args['volumes'].volume_hand.astype(object)
        args['volumes'].loc[0, 'volume_hand'] = pd.NA
    elif defect == 'missing_price':
        args['prices'] = args['prices'].iloc[1:].copy()
    else:
        args['volumes'].volume_hand = 0.
        if defect == 'only_first_traded':
            args['volumes'].loc[0, 'volume_hand'] = 100.
    result = volume_context_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result.loc[1, list(VOLUME_CONTEXT_FEATURES)].isna().all()
    assert result.loc[1, 'volume_context_feature_status'] == ('UNKNOWN_NO_TRADED_VOLUME' if defect in ('all_zero', 'only_first_traded') else 'UNKNOWN_VOLUME_CONTEXT_SOURCE')


@pytest.mark.parametrize('defect', ['negative_volume', 'bool_volume', 'decimal_nan', 'duplicate', 'foreign_quantity', 'wrong_T'])
def test_quantity_and_original_identity_contradictions_fail_closed(defect):
    args = volume_context_fixture()
    if defect in ('negative_volume', 'bool_volume', 'decimal_nan'):
        args['volumes']['volume_hand'] = args['volumes'].volume_hand.astype(object)
        args['volumes'].loc[0, 'volume_hand'] = {'negative_volume': -1, 'bool_volume': False, 'decimal_nan': Decimal('NaN')}[defect]
    elif defect == 'duplicate':
        args['volumes'] = pd.concat([args['volumes'], args['volumes'].iloc[[0]]])
    elif defect == 'foreign_quantity':
        args['volumes'].loc[0, 'instrument'] = '000002.SZ'
    else:
        args['candidates'].loc[1, KEY[1]] = args['calendar'][21]
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        volume_context_rows_v1(**args)


def test_flat_price_partial_zero_volume_and_common_price_holes():
    args = volume_context_fixture()
    args['prices'].raw_close_cny = 10.
    args['volumes'].volume_hand = 0.
    args['volumes'].loc[19, 'volume_hand'] = 100.
    result = volume_context_rows_v1(**args)
    np.testing.assert_allclose(result.loc[1, list(VOLUME_CONTEXT_FEATURES)].astype(float), [0., 0., 1.])
    empty = {**args, 'candidates': args['candidates'].iloc[:0], 'volumes': args['volumes'].iloc[:0]}
    assert volume_context_rows_v1(**empty).empty
    fitted = volume_context_fit_fixture()
    features = {**dict.fromkeys(D_FEATURES, .02), **dict(zip(VOLUME_CONTEXT_FEATURES, (.05, .1, .08), strict=True))}
    grid = information_price_set_v1(fitted=fitted, d_features=features, arm='candidate', reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3,
        model_id='M9', information_features=VOLUME_CONTEXT_FEATURES)
    assert grid.intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    rows = pd.DataFrame([{**features, 'actual_gap_bps': 100.}])
    rows[VOLUME_CONTEXT_FEATURES[0]] = np.nan
    for arm in ('matched', 'candidate'):
        assert volume_context_nodes_v1(fitted=fitted, rows=rows, arm=arm).status.tolist() == ['UNKNOWN_INPUT_OR_SUPPORT']


def test_new_router_training_is_13_16_mature_common_and_test_blind():
    rows, configuration = rows_fixture()
    rows[list(VOLUME_CONTEXT_FEATURES)] = [.05, .1, .08]
    rows['volume_context_feature_status'] = 'AVAILABLE'
    rows.loc[0, list(VOLUME_CONTEXT_FEATURES)] = np.nan
    rows.loc[0, 'volume_context_feature_status'] = 'UNKNOWN_VOLUME_CONTEXT_SOURCE'
    events = []
    fitted = train_volume_context_price_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *VOLUME_CONTEXT_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_volume_context_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_volume_context_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_tail_quantity_normalization_does_not_erase_real_trades_at_extreme_coordinate_scale():
    args = volume_context_fixture()
    args['prices'].raw_close_cny = 10.*2.**-600
    args['prices'].adj_factor = 2.**600
    args['prices'].loc[0, 'raw_close_cny'] = 10.*2.**600
    args['prices'].loc[0, 'adj_factor'] = 2.**-600
    args['volumes'].volume_hand = 1.
    result = volume_context_rows_v1(**args)
    assert result.loc[1, 'volume_context_feature_status'] == 'AVAILABLE'
    np.testing.assert_allclose(result.loc[1, list(VOLUME_CONTEXT_FEATURES)].astype(float), [0., 0., 1.])
