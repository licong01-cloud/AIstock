from dataclasses import replace
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_parent_normalized_trajectory_v1 import CLOCK, INFORMATION_FEATURES, NORM_DELTA_FEATURES, STATUS, TRAJECTORY_CLOCK, TRAJECTORY_FEATURES, TRAJECTORY_STATUS, parent_normalized_trajectory_fit_identity_v1, parent_normalized_trajectory_nodes_v1, parent_normalized_trajectory_price_set_v1, parent_normalized_trajectory_rows_v1, train_parent_normalized_trajectory_v1
from backend.services.advisory_model_first.economic_parent_raw_trajectory_v1 import parent_raw_trajectory_rows_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1, information_matrix_v1
from backend.tests.advisory_model_first.test_economic_parent_raw_trajectory_v1 import trajectory_rows_fixture
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def normalized_fit_fixture():
    old = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), parent_normalized_trajectory_features=list(INFORMATION_FEATURES), matched_information_features=list(TRAJECTORY_FEATURES))
    models = {name: {**body, 'features': 17 if name.startswith('matched') else 19} for name, body in old.models.items()}
    return SectorPriceFitV1(recipe, models, old.support, old.diagnostics, parent_normalized_trajectory_fit_identity_v1(recipe, models, old.support))


def normalized_rows_fixture():
    args = trajectory_rows_fixture()
    args['base_rows'] = parent_raw_trajectory_rows_v1(**args)
    args['rankings'] = args['rankings'].drop(columns=['raw__lstm', 'raw__fund']).assign(norm__lstm=[.4, .4, .1, .7], norm__fund=[.2, .2, .0, .5])
    return args


def test_historical_crosssection_adds_information_exact5_keys_original_labels_and_unknown():
    # Own past raw=1 in both worlds; other stocks alter past zscore, not own raw trajectory.
    worlds = [np.array([1., 2., 3.]), np.array([1., 0., -1.])]
    assert worlds[0][0] == worlds[1][0] and not np.isclose(*[(w[0]-w.mean())/w.std() for w in worlds])
    args = normalized_rows_fixture()
    args['base_rows'].index = [7, 3]
    args['base_rows'].loc[:, TRAJECTORY_FEATURES] = .1
    foreign = args['rankings'].iloc[:1].assign(instrument='999999.SZ', norm__lstm='unconsumed poison', trade_date=args['calendar'][9])
    args['rankings'] = pd.concat([args['rankings'].iloc[::-1], foreign], ignore_index=True)
    rows = parent_normalized_trajectory_rows_v1(**args)
    assert rows[KEY].equals(args['base_rows'][KEY].reset_index(drop=True)) and rows.gross_value_ratio.eq(1.04).all()
    assert rows[STATUS].eq('AVAILABLE').all() and rows[CLOCK].equals(rows[KEY[0]])
    assert np.allclose(rows.loc[:, NORM_DELTA_FEATURES], [[.3, .2], [-.3, -.3]])
    for kind in ('missing_lag', 'raw_unknown', 'warmup'):
        one = normalized_rows_fixture()
        if kind == 'missing_lag':
            one['rankings'] = one['rankings'].iloc[:3]
        elif kind == 'raw_unknown':
            one['base_rows'].loc[1, TRAJECTORY_STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
        else:
            one['base_rows'][KEY[0]] = one['calendar'][0]
            one['base_rows'][KEY[1]] = one['calendar'][1]
            one['base_rows'][TRAJECTORY_CLOCK] = one['calendar'][0]
            one['candidates'] = one['base_rows'][KEY]
        result = parent_normalized_trajectory_rows_v1(**one)
        assert len(result) == 2 and result[STATUS].iloc[1] == 'UNKNOWN_SOURCE_OR_INPUT'


def test_current_required_lag_target_dup_clock_package_and_original_raw_cutoff():
    for field, value in [('trade_date', pd.Timestamp('2025-01-03')), ('target_trade_date', pd.Timestamp('2025-01-06')), ('package_id', 'other'), ('manifest_sha256', 'b'*64)]:
        args = normalized_rows_fixture()
        args['rankings'].loc[2, field] = value
        with pytest.raises(ValueError, match='identity'):
            parent_normalized_trajectory_rows_v1(**args)
    for change in ('duplicate', 'current_missing', 'base_future', 'base_missing'):
        args = normalized_rows_fixture()
        if change == 'duplicate':
            args['rankings'] = pd.concat([args['rankings'], args['rankings'].iloc[2:3]])
        elif change == 'current_missing':
            args['rankings'] = args['rankings'].iloc[1:]
        elif change == 'base_future':
            args['base_rows'].loc[0, TRAJECTORY_CLOCK] = args['calendar'][6]
        else:
            args['base_rows'] = args['base_rows'].iloc[:1]
        with pytest.raises(ValueError):
            parent_normalized_trajectory_rows_v1(**args)


@pytest.mark.parametrize('value,valid', [(0., True), (-.1, True), (None, True), (Decimal('NaN'), True), (True, False), ('0.1', False), (np.inf, False), (Decimal('sNaN'), False)])
def test_consumed_normalized_scores_missing_is_not_malformed(value, valid):
    args = normalized_rows_fixture()
    args['rankings']['norm__lstm'] = args['rankings']['norm__lstm'].astype(object)
    args['rankings'].at[2, 'norm__lstm'] = value
    if valid:
        rows = parent_normalized_trajectory_rows_v1(**args)
        assert len(rows) == 2 and rows[STATUS].iloc[0] == ('UNKNOWN_SOURCE_OR_INPUT' if value is None or isinstance(value, Decimal) else 'AVAILABLE')
    else:
        with pytest.raises(ValueError, match='numeric|finite'):
            parent_normalized_trajectory_rows_v1(**args)


def test_finite_normalized_difference_overflow_is_not_unknown():
    args = normalized_rows_fixture()
    args['rankings'].loc[0, 'norm__lstm'] = 1e308
    args['rankings'].loc[2, 'norm__lstm'] = -1e308
    with pytest.raises(ValueError, match='overflow'):
        parent_normalized_trajectory_rows_v1(**args)


def test_actual17_19_shared_supervision_test_poison_json_parity_and_maturity():
    rows, configuration = rows_fixture()
    for name in INFORMATION_FEATURES:
        rows[name] = .01
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
    events = []
    fitted = train_parent_normalized_trajectory_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.diagnostics['train_rows'] == 107
    assert fitted.models['matched_mean']['features'] == 17 and fitted.models['candidate_mean']['features'] == 19
    assert fitted.recipe['matched_information_features'] == list(TRAJECTORY_FEATURES)
    for arm, width in [('matched', 17), ('candidate', 19)]:
        query = rows.iloc[1:3]
        matrix = information_matrix_v1(query, arm=arm, information_features=INFORMATION_FEATURES, matched_information_features=TRAJECTORY_FEATURES)
        assert matrix.shape == (2, width) and np.array_equal(matrix[:, 12:16], query.loc[:, TRAJECTORY_FEATURES].to_numpy())
        assert len(parent_normalized_trajectory_nodes_v1(fitted=fitted, rows=query, arm=arm)) == 2
    poisoned = rows.copy()
    poisoned.loc[poisoned.split.eq('test'), [*D_FEATURES, *INFORMATION_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert train_parent_normalized_trajectory_v1(rows=poisoned, configuration=configuration, before_fit=lambda _: None).model_sha256 == fitted.model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_parent_normalized_trajectory_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('no fit'))


def test_price_support_holes_shared_unknown_empty_model_width_and_bad_numeric():
    fitted = normalized_fit_fixture()
    features = dict.fromkeys((*D_FEATURES, *INFORMATION_FEATURES), .01)
    query = pd.DataFrame([{**features, 'actual_gap_bps': gap, STATUS: status} for gap, status in [(-100., 'AVAILABLE'), (0., 'AVAILABLE'), (100., 'UNKNOWN_SOURCE_OR_INPUT')]])
    for arm in ('matched', 'candidate'):
        assert parent_normalized_trajectory_nodes_v1(fitted=fitted, rows=query, arm=arm).status.tolist() == ['ACCEPTABLE', 'UNKNOWN_INPUT_OR_SUPPORT', 'UNKNOWN_INPUT_OR_SUPPORT']
        assert parent_normalized_trajectory_price_set_v1(fitted=fitted, d_features=features, arm=arm, reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3).intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    corrupt = {**fitted.models, 'matched_mean': {**fitted.models['matched_mean'], 'features': 15}}
    with pytest.raises(ValueError, match='17/19'):
        parent_normalized_trajectory_nodes_v1(fitted=replace(fitted, models=corrupt), rows=query, arm='matched')
    assert parent_normalized_trajectory_nodes_v1(fitted=fitted, rows=query.iloc[:0], arm='candidate').empty
    query.at[0, NORM_DELTA_FEATURES[0]] = np.inf
    with pytest.raises(ValueError, match='finite'):
        parent_normalized_trajectory_nodes_v1(fitted=fitted, rows=query, arm='candidate')
