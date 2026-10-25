from dataclasses import replace
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_parent_raw_score_v1 import parent_raw_score_rows_v1
from backend.services.advisory_model_first.economic_parent_scale_state_v1 import RAW_FEATURES, SCALE_FEATURES, STATE_FEATURES, STATUS, _moments, parent_scale_state_fit_identity_v1, parent_scale_state_nodes_v1, parent_scale_state_price_set_v1, parent_scale_state_rows_v1, train_parent_scale_state_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1, information_matrix_v1
from backend.tests.advisory_model_first.test_economic_parent_raw_score_v1 import MAPPING, raw_fit_fixture
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture

DTYPES = {'raw__lstm': 'float32', 'norm__lstm': 'float64', 'raw__fund': 'float64', 'norm__fund': 'float64'}


def scale_fit_fixture():
    old = raw_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), parent_scale_state_features=list(SCALE_FEATURES), matched_information_features=list(RAW_FEATURES))
    models = {name: {**body, 'features': 15 if name.startswith('matched') else 19} for name, body in old.models.items()}
    return SectorPriceFitV1(recipe, models, old.support, old.diagnostics, parent_scale_state_fit_identity_v1(recipe, models, old.support))


def scale_rows_fixture(*, missing=False):
    days = pd.bdate_range('2025-01-02', periods=3)
    keys = pd.DataFrame([{KEY[0]: days[0], KEY[1]: days[1], KEY[2]: f'{i:06d}.SZ'} for i in range(3)])
    rankings = keys.assign(raw__lstm=np.array([1., 3., 5.], dtype=np.float32), norm__lstm=[0., 1., 2.],
        raw__fund=[-2., -1., 0.], norm__fund=[2., 3., 4.], trade_date=days[0], package_id='pkg', manifest_sha256='a'*64)
    if missing:
        rankings.loc[2, 'raw__lstm'] = np.nan
    args = dict(base_rows=keys.assign(**dict.fromkeys(D_FEATURES, 0.), gross_value_ratio=1.04), rankings=rankings,
        candidates=keys, calendar=days, component_raw_columns=MAPPING, package_id='pkg', package_manifest_sha256='a'*64)
    args['base_rows'] = parent_raw_score_rows_v1(**args)
    return {**args, 'score_dtypes': DTYPES}


def test_cross_candidate_new_information_missing_and_constant_norm_do_not_drop_rows():
    args = scale_rows_fixture(missing=True)
    rows = parent_scale_state_rows_v1(**args)
    assert len(rows) == 3 and rows[KEY].equals(args['base_rows'][KEY]) and rows.gross_value_ratio.eq(1.04).all()
    relabeled = {**args, 'base_rows': args['base_rows'].set_axis([10, 20, 30])}
    assert parent_scale_state_rows_v1(**relabeled).equals(rows)
    assert rows[STATUS].tolist() == ['AVAILABLE', 'AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT']
    assert np.allclose(rows.loc[:, STATE_FEATURES], [1., 2., -4., 1.])
    args['rankings']['norm__lstm'] = 1.
    assert parent_scale_state_rows_v1(**args)[STATUS].eq('UNKNOWN_SOURCE_OR_INPUT').all()
    assert np.isnan(_moments(np.array([1., 1.]), np.array([1., 1.]), 'float64', 'float64')[1])
    # Identical own raw/norm permits different peer distributions and states.
    for center, scale in [(0., 1.), (-1., 2.)]:
        args = scale_rows_fixture()
        args['rankings']['norm__lstm'] = [1., 0., -1.]
        args['rankings']['raw__lstm'] = np.array([1., center, center-scale], dtype=np.float32)
        args['base_rows'][RAW_FEATURES[0]] = args['rankings']['raw__lstm']
        result = parent_scale_state_rows_v1(**args)
        assert result[RAW_FEATURES[0]].iloc[0] == 1. and result[STATE_FEATURES[0]].iloc[0] == center
        assert result[STATE_FEATURES[1]].iloc[0] == scale


def test_schema_precision_identity_and_requested_projection_not_outside_poison():
    args = scale_rows_fixture()
    args['rankings']['raw__lstm'] = np.array([.10000001, .10000003, .10000005], dtype=np.float32)
    args['base_rows'][RAW_FEATURES[0]] = args['rankings']['raw__lstm']
    result = parent_scale_state_rows_v1(**args)
    assert len(result) == 3 and result[STATUS].eq('AVAILABLE').all()
    foreign = args['rankings'].iloc[:1].assign(instrument='999999.SZ', norm__lstm='ignored', trade_date=args['calendar'][2])
    args['rankings'] = pd.concat([args['rankings'], foreign], ignore_index=True)
    assert parent_scale_state_rows_v1(**args)[KEY].equals(result[KEY])
    args['rankings'].loc[1, 'norm__fund'] += 1.
    with pytest.raises(ValueError, match='relationship'):
        parent_scale_state_rows_v1(**args)
    args = scale_rows_fixture()
    args['base_rows'].loc[0, RAW_FEATURES[0]] = 99.
    with pytest.raises(ValueError, match='base differs'):
        parent_scale_state_rows_v1(**args)


@pytest.mark.parametrize('value,valid', [(None, True), (Decimal('NaN'), True), (True, False), ('1', False), (np.inf, False), (Decimal('sNaN'), False)])
def test_normal_norm_missing_vs_bad_numeric_preserves_population(value, valid):
    args = scale_rows_fixture()
    args['rankings']['norm__lstm'] = pd.Series([value, 1., 2.], dtype=object)
    if valid:
        assert parent_scale_state_rows_v1(**args)[STATUS].eq('AVAILABLE').all()
    else:
        with pytest.raises(ValueError, match='numeric|finite'):
            parent_scale_state_rows_v1(**args)


def test_15_19_shared_train_test_poison_original_support_and_price_set():
    rows, configuration = rows_fixture()
    for name in SCALE_FEATURES:
        rows[name] = .01
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
    events = []
    fitted = train_parent_scale_state_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.diagnostics['train_rows'] == 107
    assert fitted.models['matched_mean']['features'] == 15 and fitted.models['candidate_mean']['features'] == 19
    assert fitted.recipe['matched_information_features'] == list(RAW_FEATURES)
    for arm, width in [('matched', 15), ('candidate', 19)]:
        query = rows.iloc[1:3]
        assert information_matrix_v1(query, arm=arm, information_features=SCALE_FEATURES, matched_information_features=RAW_FEATURES).shape == (2, width)
        assert len(parent_scale_state_nodes_v1(fitted=fitted, rows=query, arm=arm)) == 2
    poisoned = rows.copy()
    poisoned.loc[poisoned.split.eq('test'), [*D_FEATURES, *SCALE_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert train_parent_scale_state_v1(rows=poisoned, configuration=configuration, before_fit=lambda _: None).model_sha256 == fitted.model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_parent_scale_state_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('no fit'))
    fitted = scale_fit_fixture()
    features = dict.fromkeys((*D_FEATURES, *SCALE_FEATURES), .01)
    query = pd.DataFrame([{**features, 'actual_gap_bps': g, STATUS: s} for g, s in [(-100., 'AVAILABLE'), (0., 'AVAILABLE'), (100., 'UNKNOWN_SOURCE_OR_INPUT')]])
    for arm in ('matched', 'candidate'):
        assert parent_scale_state_nodes_v1(fitted=fitted, rows=query, arm=arm).status.tolist() == ['ACCEPTABLE', 'UNKNOWN_INPUT_OR_SUPPORT', 'UNKNOWN_INPUT_OR_SUPPORT']
        assert parent_scale_state_price_set_v1(fitted=fitted, d_features=features, arm=arm, reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3).intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    assert parent_scale_state_nodes_v1(fitted=fitted, rows=query.iloc[:0], arm='candidate').empty
    with pytest.raises(ValueError, match='15/19'):
        parent_scale_state_nodes_v1(fitted=replace(fitted, recipe={**fitted.recipe, 'matched_information_features': []}), rows=query, arm='matched')
