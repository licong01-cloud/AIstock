import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_price_path_value_v1 import PRICE_PATH_FEATURES, PricePathPlanV1, price_path_fit_identity_v1, price_path_nodes_v1, price_path_price_set_v1, price_path_rows_v1, train_price_path_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def price_path_fixture():
    days = pd.bdate_range('2025-01-02', periods=23)
    candidates = pd.DataFrame([{KEY[0]: days[i], KEY[1]: days[i+1], KEY[2]: '000001.SZ'} for i in (3, 19, 20)])
    prices = pd.DataFrame(dict(trade_date=days[:21], instrument='000001.SZ', raw_close_cny=[10.]*18+[12., 11., 13.], adj_factor=1.))
    return dict(candidates=candidates, prices=prices, calendar=days)


def price_path_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), price_path_features=list(PRICE_PATH_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        price_path_fit_identity_v1(recipe, original.models, original.support))


def test_plan_fixed_fields_four_fits27_no_activation_or_window_override():
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='price_path_predecessor'))
    plan = PricePathPlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 27 and plan.parameters['physical_fit_budget'] == 4
    assert plan.parameters['information_features'] == list(PRICE_PATH_FEATURES) and not plan.deployable
    for update in ({'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'window_sessions': 10},
                   {'predecessor_manifest_ref': {**values['predecessor_manifest_ref'], 'artifact_uri': 'F:/other/prior/evaluated/manifest.json'}},
                   {'budget_anchor_ref': {**values['budget_anchor_ref'], 'artifact_uri': 'C:/unit/preregistered/manifest.json'}}):
        with pytest.raises(ValueError):
            PricePathPlanV1.model_validate({**values, **update})


def test_manual_geometry_split_adjustment_scale_invariance_and_future_poison():
    args = price_path_fixture()
    result = price_path_rows_v1(**args)
    assert result.price_path_feature_status.tolist() == ['UNKNOWN_20D_WARMUP', 'AVAILABLE', 'AVAILABLE']
    assert result.loc[1, 'price_drawdown20'] == pytest.approx(11/12-1)
    assert result.loc[1, 'price_path_efficiency19'] == pytest.approx(np.log(11/10)/(np.log(12/10)+abs(np.log(11/12))))
    assert result.loc[1, 'price_up_day_share19'] == pytest.approx(1/19)
    assert result[KEY].equals(args['candidates'][KEY])
    adjusted = args['prices'].copy()
    adjusted.loc[18:, 'raw_close_cny'] /= 2
    adjusted.loc[18:, 'adj_factor'] = 2.
    pd.testing.assert_frame_equal(result, price_path_rows_v1(**{**args, 'prices': adjusted}))
    scaled = args['prices'].copy()
    scaled.raw_close_cny *= 7.
    np.testing.assert_allclose(result.loc[:, PRICE_PATH_FEATURES], price_path_rows_v1(**{**args, 'prices': scaled}).loc[:, PRICE_PATH_FEATURES], atol=1e-12)
    poisoned = args['prices'].copy()
    poisoned['future_return'] = -999999.
    extra = poisoned.iloc[[-1]].copy()
    extra['trade_date'] = args['calendar'][-1]
    extra[['raw_close_cny', 'adj_factor']] = -999999.
    pd.testing.assert_frame_equal(result, price_path_rows_v1(**{**args, 'prices': pd.concat([poisoned, extra])}))
    # Later candidate data must not determine an earlier candidate's window.
    earlier = args['candidates'].iloc[[1]].copy()
    poisoned.loc[20, ['raw_close_cny', 'adj_factor']] = -999999.
    pd.testing.assert_frame_equal(price_path_rows_v1(**{**args, 'candidates': earlier}),
        price_path_rows_v1(**{**args, 'candidates': earlier, 'prices': poisoned}))


@pytest.mark.parametrize('defect', ['missing_session', 'null_factor', 'nullable_factor', 'flat'])
def test_normal_missing_suspension_and_flat_preserve_all_keys_unknown(defect):
    args = price_path_fixture()
    if defect == 'missing_session':
        args['prices'] = args['prices'].iloc[1:].copy()
    elif defect == 'null_factor':
        args['prices'].loc[0, 'adj_factor'] = np.nan
    elif defect == 'nullable_factor':
        args['prices']['adj_factor'] = args['prices'].adj_factor.astype(object)
        args['prices'].loc[0, 'adj_factor'] = pd.NA
    else:
        args['prices'].raw_close_cny = 10.
    result = price_path_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result.loc[1, list(PRICE_PATH_FEATURES)].isna().all()
    assert result.loc[1, 'price_path_feature_status'] == ('UNKNOWN_FLAT_PATH' if defect == 'flat' else 'UNKNOWN_PRICE_PATH_SOURCE')


@pytest.mark.parametrize('defect', ['duplicate', 'wrong_T', 'zero', 'bool', 'infinite', 'text'])
def test_contradictions_fail_closed(defect):
    args = price_path_fixture()
    if defect == 'duplicate':
        args['prices'] = pd.concat([args['prices'], args['prices'].iloc[[0]]])
    elif defect == 'wrong_T':
        args['candidates'].loc[1, KEY[1]] = args['calendar'][21]
    else:
        args['prices']['adj_factor'] = args['prices'].adj_factor.astype(object)
        args['prices'].loc[0, 'adj_factor'] = {'zero': 0., 'bool': True, 'infinite': np.inf, 'text': '1'}[defect]
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        price_path_rows_v1(**args)


def test_empty_frame_and_path_identity_common_unknown_and_price_holes():
    args = price_path_fixture()
    empty = price_path_rows_v1(**{**args, 'candidates': args['candidates'].iloc[:0]})
    assert empty.empty and list(empty.columns) == [*KEY, *PRICE_PATH_FEATURES, 'price_path_feature_status', 'price_path_feature_visible_through']
    fitted = price_path_fit_fixture()
    features = {**dict.fromkeys(D_FEATURES, .02), **dict(zip(PRICE_PATH_FEATURES, (-.1, .5, .6), strict=True))}
    grid = price_path_price_set_v1(fitted=fitted, d_features=features, arm='candidate', reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3)
    assert grid.intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    query = pd.DataFrame([{**features, 'actual_gap_bps': 100.}])
    assert price_path_nodes_v1(fitted=fitted, rows=query, arm='candidate').status.tolist() == ['ACCEPTABLE']
    query[PRICE_PATH_FEATURES[0]] = np.nan
    for arm in ('matched', 'candidate'):
        assert price_path_nodes_v1(fitted=fitted, rows=query, arm=arm).status.tolist() == ['UNKNOWN_INPUT_OR_SUPPORT']
    with pytest.raises(ValueError, match='identity'):
        price_path_nodes_v1(fitted=sector_fit_fixture(), rows=query, arm='candidate')


def test_four_unit_fits_common_population_and_no_test_information_consumption():
    rows, configuration = rows_fixture()
    rows[list(PRICE_PATH_FEATURES)] = [-.1, .5, .6]
    rows['price_path_feature_status'] = 'AVAILABLE'
    rows.loc[0, list(PRICE_PATH_FEATURES)] = np.nan
    rows.loc[0, 'price_path_feature_status'] = 'UNKNOWN_PRICE_PATH_SOURCE'
    events = []
    fitted = train_price_path_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.recipe['price_path_features'] == list(PRICE_PATH_FEATURES)
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *PRICE_PATH_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_price_path_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_price_path_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))
