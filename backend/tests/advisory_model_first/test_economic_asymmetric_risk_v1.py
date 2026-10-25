import json
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_asymmetric_risk_v1 import ASYMMETRIC_RISK_FEATURES, CLOCK, STATUS, UNKNOWN, AsymmetricRiskPlanV1, asymmetric_risk_fit_identity_v1, asymmetric_risk_rows_v1, train_asymmetric_risk_v1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def asymmetric_risk_fixture():
    calendar = pd.bdate_range('2025-01-02', periods=22)
    candidates = pd.DataFrame([{KEY[0]: calendar[i], KEY[1]: calendar[i+1], KEY[2]: '000001.SZ'} for i in (19, 20)])
    prices = pd.DataFrame(dict(trade_date=calendar[:21], instrument='000001.SZ',
        raw_close_cny=np.exp(np.r_[0., np.cumsum([.01]*19+[-.02])]), adj_factor=1.))
    return dict(candidates=candidates, prices=prices, calendar=calendar)


def asymmetric_risk_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), asymmetric_risk_features=list(ASYMMETRIC_RISK_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        asymmetric_risk_fit_identity_v1(recipe, original.models, original.support))


def test_manual_same_endpoint_last14_and_geometry_different_amplitude():
    from backend.services.advisory_model_first.economic_price_path_value_v1 import PRICE_PATH_FEATURES, price_path_rows_v1
    args = asymmetric_risk_fixture()
    result = asymmetric_risk_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result[STATUS].eq('AVAILABLE').all()
    np.testing.assert_allclose(result[list(ASYMMETRIC_RISK_FEATURES)].astype(float), [[0., .01, 0.], [.02/np.sqrt(19), .01*np.sqrt(18/19), 0.]], atol=1e-15)
    old = price_path_rows_v1(**args)
    r = np.array([.002]*4+[.042]+[.01]*14+[-.02])
    args['prices']['raw_close_cny'] = np.exp(np.r_[0., r.cumsum()])
    np.testing.assert_allclose(old.loc[0, list(PRICE_PATH_FEATURES)].astype(float), price_path_rows_v1(**args).loc[0, list(PRICE_PATH_FEATURES)].astype(float), atol=1e-14)
    changed = asymmetric_risk_rows_v1(**args)
    assert changed.loc[0, ASYMMETRIC_RISK_FEATURES[1]] == pytest.approx(np.sqrt((4*.002**2+.042**2+14*.01**2)/19))
    assert args['prices'].raw_close_cny.iloc[-1] == pytest.approx(np.exp(.17))


@pytest.mark.parametrize('missing', [None, np.nan, Decimal('NaN')])
def test_normal_missing_keeps_original_order_UNKNOWN(missing):
    args = asymmetric_risk_fixture()
    args['prices']['raw_close_cny'] = args['prices'].raw_close_cny.astype(object)
    args['prices'].loc[0, 'raw_close_cny'] = missing
    result = asymmetric_risk_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result[STATUS].tolist() == ['UNKNOWN_PRICE_SOURCE', 'AVAILABLE']
    assert set(json.loads(result.loc[0, UNKNOWN])) == set(ASYMMETRIC_RISK_FEATURES)


def test_flat_single_sided_clustered_downside_and_stable_log_extremes():
    args = asymmetric_risk_fixture()
    for price, factor in ((1e308, 1e308), (1e-300, 1e-300)):
        args['prices']['raw_close_cny'], args['prices']['adj_factor'] = price, factor
        np.testing.assert_allclose(asymmetric_risk_rows_v1(**args)[list(ASYMMETRIC_RISK_FEATURES)].astype(float), 0.)
    args['prices']['adj_factor'] = 1.
    args['prices']['raw_close_cny'] = np.exp(np.r_[0., np.cumsum([-.01]*20)])
    np.testing.assert_allclose(asymmetric_risk_rows_v1(**args)[list(ASYMMETRIC_RISK_FEATURES)].astype(float), [[.01, 0., .01]]*2, atol=1e-15)
    r = np.array([-.02]*9+[.01]*9+[0., 0.])
    args['prices']['raw_close_cny'] = np.exp(np.r_[0., r.cumsum()])
    np.testing.assert_allclose(asymmetric_risk_rows_v1(**args).loc[0, list(ASYMMETRIC_RISK_FEATURES)].astype(float),
        [.02*np.sqrt(9/19), .01*np.sqrt(9/19), .02*np.sqrt(8/18)], atol=1e-15)
    args['candidates'].loc[0, [KEY[0], KEY[1]]] = args['calendar'][2:4]
    assert asymmetric_risk_rows_v1(**args).loc[0, STATUS] == 'UNKNOWN_20D_HISTORY'


@pytest.mark.parametrize('bad', [True, 'bad', np.inf, 0., -.1, Decimal('Infinity'), Decimal('sNaN'), Decimal('1e-400')])
def test_bad_consumed_price_factor_not_silent_unknown(bad):
    args = asymmetric_risk_fixture()
    args['prices']['adj_factor'] = args['prices'].adj_factor.astype(object)
    args['prices'].loc[0, 'adj_factor'] = bad
    with pytest.raises(ValueError):
        asymmetric_risk_rows_v1(**args)


def test_only_original_requested_source_no_future_foreign_Y_action_population():
    args = asymmetric_risk_fixture()
    expected = asymmetric_risk_rows_v1(**args)
    args['prices'] = pd.concat([args['prices'], args['prices'].iloc[:1].assign(trade_date=args['calendar'][-1], raw_close_cny=-999.),
        args['prices'].iloc[:1].assign(instrument='000009.SZ', raw_close_cny=-999.)], ignore_index=True)
    args['prices'] = args['prices'].assign(raw_open_cny=-999., gross_value_ratio=-999., training_eligible=False, model_action='SKIP')
    pd.testing.assert_frame_equal(expected, asymmetric_risk_rows_v1(**args))
    args['prices'] = args['prices'].drop(index=0)
    assert asymmetric_risk_rows_v1(**args).loc[0, STATUS] == 'UNKNOWN_PRICE_SOURCE'


def test_duplicate_wrong_T_and_empty_structured_schema():
    args = asymmetric_risk_fixture()
    with pytest.raises(AdvisoryModelFirstError, match='duplicate'):
        asymmetric_risk_rows_v1(**{**args, 'prices': pd.concat([args['prices'], args['prices'].iloc[:1]], ignore_index=True)})
    wrong = args['candidates'].copy()
    wrong.loc[0, KEY[1]] = args['calendar'][-1]
    with pytest.raises(ValueError, match='next session'):
        asymmetric_risk_rows_v1(**{**args, 'candidates': wrong})
    result = asymmetric_risk_rows_v1(**{**args, 'candidates': args['candidates'].iloc[:0]})
    assert result.empty and result.columns.tolist() == [*KEY, *ASYMMETRIC_RISK_FEATURES, STATUS, CLOCK, UNKNOWN]


def test_common_mature_13_16_train_only_support_unchanged():
    rows, configuration = rows_fixture()
    rows[list(ASYMMETRIC_RISK_FEATURES)], rows[STATUS] = [.01, .02, .005], 'AVAILABLE'
    rows.loc[0, ASYMMETRIC_RISK_FEATURES[0]], rows.loc[0, STATUS] = np.nan, 'UNKNOWN_PRICE_SOURCE'
    events = []
    fitted = train_asymmetric_risk_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1 and fitted.support.contains(20.)
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *ASYMMETRIC_RISK_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_asymmetric_risk_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_asymmetric_risk_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_fixed71_plan_no_window_search_or_activation():
    from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='asymmetric_risk_predecessor'))
    plan = AsymmetricRiskPlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 71 and plan.parameters['source_select_budget'] == 0 and plan.parameters['window_sessions'] == 20
    assert plan.experiment_id.startswith('advasymrisk_') and not plan.deployable
    for update in ({'window_sessions': 10}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'model_id': 'M17'}):
        with pytest.raises(ValueError):
            AsymmetricRiskPlanV1.model_validate({**values, **update})

