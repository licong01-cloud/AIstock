from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_breadth_state_price_v1 import BREADTH_SOURCE_COLUMNS, BREADTH_STATE_FEATURES, BreadthStatePricePlanV1, breadth_state_fit_identity_v1, breadth_state_rows_v1, train_breadth_state_price_v1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def breadth_state_fixture():
    calendar = pd.bdate_range('2025-01-02', periods=23)
    breadth = pd.DataFrame([{KEY[0]: day, KEY[1]: calendar[index+1], KEY[2]: symbol,
        'market_up_ratio': .1+.8*min(index, 19)/19, 'feature_visible_through': day}
        for index, day in enumerate(calendar[:-1]) for symbol in ('000001.SZ', '000002.SZ')])
    candidates = breadth.loc[breadth[KEY[0]].isin(calendar[[0, 19, 20]]), KEY].reset_index(drop=True)
    return dict(candidates=candidates, breadth=breadth, calendar=calendar)


def breadth_state_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), breadth_state_features=list(BREADTH_STATE_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        breadth_state_fit_identity_v1(recipe, original.models, original.support))


def test_fixed_plan39_and_manual_three_fields_shared_market_original_keys():
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='breadth_state_predecessor'))
    plan = BreadthStatePricePlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 39 and plan.parameters['source_select_budget'] == 0 and not plan.deployable
    for update in ({'window_sessions': 60}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}):
        with pytest.raises(ValueError):
            BreadthStatePricePlanV1.model_validate({**values, **update})
    args = breadth_state_fixture()
    result = breadth_state_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY])
    assert result.breadth_state_feature_status.tolist() == ['UNKNOWN_20D_HISTORY']*2+['AVAILABLE']*4
    np.testing.assert_allclose(result.loc[2, list(BREADTH_STATE_FEATURES)].astype(float), [.5, .8/19, .5])
    np.testing.assert_array_equal(result.loc[2, list(BREADTH_STATE_FEATURES)], result.loc[3, list(BREADTH_STATE_FEATURES)])


@pytest.mark.parametrize('constant', [0., .3, .5, 1.])
def test_constant_ratio_zero_slope_strict_half_threshold(constant):
    args = breadth_state_fixture()
    args['breadth'].market_up_ratio = constant
    result = breadth_state_rows_v1(**args)
    np.testing.assert_allclose(result.loc[2, list(BREADTH_STATE_FEATURES)].astype(float), [constant, 0., float(constant < .5)])
    assert result.loc[2, BREADTH_STATE_FEATURES[1]] == 0.


@pytest.mark.parametrize('missing', ['row', 'value', 'clock'])
def test_normal_missing_history_keeps_original_population_without_compression(missing):
    args = breadth_state_fixture()
    indices = args['breadth'][KEY[0]].eq(args['calendar'][7])
    if missing == 'row':
        args['breadth'] = args['breadth'].loc[~indices]
    else:
        args['breadth'].loc[indices, 'market_up_ratio' if missing == 'value' else 'feature_visible_through'] = np.nan if missing == 'value' else pd.NaT
    result = breadth_state_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result.loc[2, 'breadth_state_feature_status'] == 'UNKNOWN_BREADTH_HISTORY'
    assert result.loc[2, list(BREADTH_STATE_FEATURES)].isna().all()


@pytest.mark.parametrize('bad', [True, -.01, 1.01, np.inf, 'bad', Decimal('NaN')])
def test_consumed_bad_breadth_is_error_not_unknown(bad):
    args = breadth_state_fixture()
    args['breadth']['market_up_ratio'] = args['breadth'].market_up_ratio.astype(object)
    args['breadth'].loc[0, 'market_up_ratio'] = bad
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        breadth_state_rows_v1(**args)


@pytest.mark.parametrize('defect', ['duplicate', 'conflict', 'clock_conflict', 'future_clock', 'wrong_T'])
def test_original_identity_and_consumed_clocks_fail_closed(defect):
    args = breadth_state_fixture()
    if defect == 'duplicate':
        args['breadth'] = pd.concat([args['breadth'], args['breadth'].iloc[:1]], ignore_index=True)
    elif defect == 'conflict':
        args['breadth'].loc[0, 'market_up_ratio'] = .9
    elif defect == 'clock_conflict':
        args['breadth'].loc[0, 'feature_visible_through'] = args['calendar'][0]-pd.Timedelta(days=1)
    elif defect == 'future_clock':
        args['breadth'].loc[:1, 'feature_visible_through'] = args['calendar'][1]
    else:
        args['breadth'].loc[0, KEY[1]] = args['calendar'][2]
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        breadth_state_rows_v1(**args)


def test_future_value_clock_poison_cannot_change_earlier_output_and_empty_schema():
    args = breadth_state_fixture()
    args['candidates'] = args['candidates'].iloc[2:4].reset_index(drop=True)
    expected = breadth_state_rows_v1(**args)
    future = args['breadth'][KEY[0]].gt(args['calendar'][19])
    args['breadth'].loc[future, 'market_up_ratio'] = -999.
    args['breadth'].loc[future, 'feature_visible_through'] = pd.Timestamp('2030-01-01')
    pd.testing.assert_frame_equal(expected, breadth_state_rows_v1(**args))
    args['candidates'] = args['candidates'].iloc[:0]
    result = breadth_state_rows_v1(**args)
    assert result.empty and result.columns.tolist() == [*KEY, *BREADTH_STATE_FEATURES, 'breadth_state_feature_status', 'breadth_state_feature_visible_through']
    assert len(BREADTH_SOURCE_COLUMNS) == 5


def test_training_13_16_same_mature_population_and_no_test_consumption():
    rows, configuration = rows_fixture()
    rows[list(BREADTH_STATE_FEATURES)] = [.4, .01, .6]
    rows['breadth_state_feature_status'] = 'AVAILABLE'
    rows.loc[0, list(BREADTH_STATE_FEATURES)] = np.nan
    rows.loc[0, 'breadth_state_feature_status'] = 'UNKNOWN_BREADTH_HISTORY'
    events = []
    fitted = train_breadth_state_price_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *BREADTH_STATE_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_breadth_state_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_breadth_state_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))
