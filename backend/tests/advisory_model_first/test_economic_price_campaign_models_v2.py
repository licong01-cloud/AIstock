from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_price_campaign_models_v2 import export_gbdt_v2, predict_campaign_v2, predict_json_v2, train_campaign_v2
from backend.services.advisory_model_first.economic_price_campaign_models_v2 import PriceCampaignFitV2, fit_identity_v2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.advisory_model_first.economic_value_anchor_inference_v1 import build_value_anchor_gap_support_v1
from backend.services.advisory_model_first.economic_value_anchor_training_v1 import assemble_value_anchor_rows_v1


def rows_fixture():
    days = pd.bdate_range('2025-01-02', periods=36)
    config = SimpleNamespace(train_start=days[0], train_end=days[27], validation_start=days[28], validation_end=days[31], test_start=days[32], test_end=days[34])
    random = np.random.default_rng(17)
    inputs = pd.DataFrame([{KEY[0]: day, KEY[1]: days[index+1], KEY[2]: f'{symbol:06d}.SZ',
        **dict(zip(D_FEATURES, random.normal(size=12), strict=True)), 'feature_visible_through': day, 'daily_input_sha256': 'a'*64}
        for index, day in enumerate(days[:-1]) for symbol in range(4)])
    labels = inputs[KEY].assign(value_label_status='AVAILABLE',
        gross_value_ratio=np.where(np.arange(len(inputs)) % 2, 1.04, .96),
        path_min_value_ratio=.93, label_information_end=inputs[KEY[1]])
    args = dict(inputs=inputs, labels=labels, observations=inputs[KEY].assign(actual_gap_bps=20.), configuration=config)
    return assemble_value_anchor_rows_v1(**args), config


@pytest.mark.parametrize('model_id,fit_count,index_count', [('M2', 4, 0), ('M3', 5, 0), ('M4', 2, 1)])
def test_real_fixed_fits_json_and_test_poison_parity(model_id, fit_count, index_count):
    rows, config = rows_fixture()
    events = []
    fitted = train_campaign_v2(rows=rows, configuration=config, model_id=model_id, before_fit=events.append)
    assert len(events) == fit_count+index_count and fitted.diagnostics['fitted_head_count'] == fit_count
    poisoned = rows.copy()
    poisoned.loc[poisoned.split.eq('test'), [*D_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    replay = train_campaign_v2(rows=poisoned, configuration=config, model_id=model_id, before_fit=lambda name: None)
    assert fitted.model_sha256 == replay.model_sha256
    query = rows.loc[rows.split.eq('train')].iloc[:5]
    for arm in ('matched', 'candidate'):
        estimates = predict_campaign_v2(fitted=fitted, rows=query, arm=arm)
        assert len(estimates) == len(query)
    assert fitted.diagnostics['test_used_for_training_or_calibration'] is False


def test_tree_float32_boundary_parity_and_corrupt_graph_refused():
    from sklearn.ensemble import GradientBoostingRegressor
    values = np.array([[.1], [.2], [.3], [.4]])
    model = GradientBoostingRegressor(n_estimators=2, max_depth=1, random_state=17).fit(values, [1., 1., 2., 2.])
    body = export_gbdt_v2(model)
    threshold = body['trees'][0]['threshold'][0]
    query = np.array([[threshold], [np.nextafter(threshold, np.inf)], [.11]])
    np.testing.assert_allclose(predict_json_v2(body, query), model.predict(query), rtol=1e-12)
    body['trees'][0]['left'][0] = 0
    with pytest.raises(ValueError, match='cyclic'):
        predict_json_v2(body, query)


def test_zero_probability_atom_and_gamma_strict_support_not_defaulted():
    model = dict(kind='logistic', coef=[[0.], [0.], [0.]], intercept=[0., 0., 0.], classes=[-1, 0, 1])
    probability = predict_json_v2(model, np.array([[0.]]))[0]
    np.testing.assert_allclose(probability, [1/3]*3)
    assert probability[2]*30-probability[0]*15 == 5.
    rows, config = rows_fixture()
    rows['gross_value_ratio'] = 1.04
    with pytest.raises(ValueError, match='amplitude support'):
        train_campaign_v2(rows=rows, configuration=config, model_id='M3', before_fit=lambda name: pytest.fail('unsupported must not fit'))


def test_training_labels_cannot_change_unlabelled_support_and_late_target_not_used():
    rows, config = rows_fixture()
    train = rows.loc[rows.split.eq('train')].copy()
    train.loc[train[KEY[1]].gt(config.train_end), 'actual_gap_bps'] = np.nan
    first = build_value_anchor_gap_support_v1(train)
    train[['gross_value_ratio', 'path_min_value_ratio', 'training_eligible']] = np.nan
    assert build_value_anchor_gap_support_v1(train) == first
    rows.loc[rows.split.eq('train'), 'label_information_end'] = config.test_end
    with pytest.raises(ValueError, match='mature shared'):
        train_campaign_v2(rows=rows, configuration=config, model_id='M2', before_fit=lambda name: pytest.fail('must not fit'))


def test_local_date_and_distance_support_use_only_stored_train_index():
    recipe = dict(features=list(D_FEATURES)+['gap_bps_div_100'], means=[0.]*13, scales=[1.]*13)
    local = dict(kind='local', values=[[0.]*13]*100, y=[1.04]*100, lower=[.99]*100,
        days=[f'2025-01-{index % 20+1:02d}' for index in range(100)])
    models = dict(local=local)
    support = ValueAnchorGapSupportV1(((-10., 10.),))
    def fitted():
        return PriceCampaignFitV2('M4', recipe, models, support, {}, fit_identity_v2('M4', recipe, models, support))
    query = pd.DataFrame([{**dict.fromkeys(D_FEATURES, 0.), 'actual_gap_bps': 0.}])
    assert predict_campaign_v2(fitted=fitted(), rows=query, arm='candidate')[0].mean_gross_value_ratio == pytest.approx(1.04)
    local['days'] = ['2025-01-01']*100
    assert predict_campaign_v2(fitted=fitted(), rows=query, arm='candidate') == [None]
    local['days'] = [f'2025-01-{index % 20+1:02d}' for index in range(100)]
    query[D_FEATURES[0]] = 5.
    assert predict_campaign_v2(fitted=fitted(), rows=query, arm='candidate') == [None]


def test_actual_multiclass_export_parity_includes_zero_atom():
    from sklearn.linear_model import LogisticRegression
    values = np.array([[-2.], [-1.], [0.], [.1], [1.], [2.]])
    model = LogisticRegression(C=1., solver='lbfgs', max_iter=500, l1_ratio=0).fit(values, [-1, -1, 0, 0, 1, 1])
    body = dict(kind='logistic', coef=model.coef_.tolist(), intercept=model.intercept_.tolist(), classes=model.classes_.tolist())
    np.testing.assert_allclose(predict_json_v2(body, values), model.predict_proba(values), atol=1e-12)
    with pytest.raises(ValueError, match='underflowed'):
        predict_json_v2(dict(kind='gamma', coef=[0.], intercept=-1000.), values)
