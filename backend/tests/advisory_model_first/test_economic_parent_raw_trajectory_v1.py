from dataclasses import replace
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_parent_raw_trajectory_v1 import CLOCK, DELTA_FEATURES, RAW_CLOCK, RAW_FEATURES, RAW_STATUS, STATUS, TRAJECTORY_FEATURES, parent_raw_trajectory_fit_identity_v1, parent_raw_trajectory_nodes_v1, parent_raw_trajectory_price_set_v1, parent_raw_trajectory_rows_v1, train_parent_raw_trajectory_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1, information_matrix_v1
from backend.tests.advisory_model_first.test_economic_parent_raw_score_v1 import MAPPING
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def trajectory_fit_fixture():
    old = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), parent_raw_trajectory_features=list(TRAJECTORY_FEATURES), matched_information_features=list(RAW_FEATURES))
    models = {name: {**body, 'features': 15 if name.startswith('matched') else 17} for name, body in old.models.items()}
    return SectorPriceFitV1(recipe, models, old.support, old.diagnostics, parent_raw_trajectory_fit_identity_v1(recipe, models, old.support))


def trajectory_rows_fixture():
    days = pd.bdate_range('2025-01-02', periods=12)
    keys = pd.DataFrame([{KEY[0]: days[5], KEY[1]: days[6], KEY[2]: f'{i:06d}.SZ'} for i in range(2)])
    base = keys.assign(**dict.fromkeys(D_FEATURES, 0.), **{RAW_FEATURES[0]: .3, RAW_FEATURES[1]: -.1},
        **{RAW_STATUS: 'AVAILABLE', RAW_CLOCK: days[5]}, gross_value_ratio=1.04)
    current = keys.assign(raw__lstm=.3, raw__fund=-.1, trade_date=days[5], package_id='pkg', manifest_sha256='a'*64)
    past = current.assign(**{KEY[0]: days[0], KEY[1]: days[1]}, trade_date=days[0], raw__lstm=[.1, .5], raw__fund=[-.3, .2])
    return dict(base_rows=base, rankings=pd.concat([current, past], ignore_index=True), candidates=keys,
        calendar=days, component_raw_columns=MAPPING, package_id='pkg', package_manifest_sha256='a'*64)


def test_fixed5_exact_history_adds_information_keeps_keys_clock_unknown_no_nearest_date():
    args = trajectory_rows_fixture()
    args['base_rows'].index = [7, 3]
    foreign = args['rankings'].iloc[:1].assign(instrument='999999.SZ', raw__lstm='unconsumed', trade_date=args['calendar'][9])
    args['rankings'] = pd.concat([args['rankings'].iloc[::-1], foreign], ignore_index=True)
    rows = parent_raw_trajectory_rows_v1(**args)
    assert rows[KEY].equals(args['base_rows'][KEY].reset_index(drop=True)) and rows.gross_value_ratio.eq(1.04).all()
    assert rows[STATUS].eq('AVAILABLE').all() and rows[CLOCK].equals(rows[KEY[0]])
    assert np.allclose(rows.loc[:, DELTA_FEATURES], [[.2, .2], [-.2, -.3]])
    # Same current raw, different past prediction: not recoverable from own current score.
    assert rows[RAW_FEATURES[0]].nunique() == 1 and rows[DELTA_FEATURES[0]].nunique() == 2
    missing = trajectory_rows_fixture()
    missing['rankings'] = missing['rankings'].iloc[:3]
    rows = parent_raw_trajectory_rows_v1(**missing)
    assert len(rows) == 2 and rows[STATUS].tolist() == ['AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT']
    warmup = trajectory_rows_fixture()
    warmup['candidates'] = warmup['rankings'].iloc[2:][KEY].reset_index(drop=True)
    warmup['base_rows'] = warmup['base_rows'].assign(**{KEY[0]: warmup['calendar'][0], KEY[1]: warmup['calendar'][1], RAW_CLOCK: warmup['calendar'][0], RAW_FEATURES[0]: [.1, .5], RAW_FEATURES[1]: [-.3, .2]})
    assert parent_raw_trajectory_rows_v1(**warmup)[STATUS].eq('UNKNOWN_SOURCE_OR_INPUT').all()


def test_requested_lag_identity_dup_current_base_future_and_original_nextT():
    for field, value in [('trade_date', pd.Timestamp('2025-01-03')), ('target_trade_date', pd.Timestamp('2025-01-06')), ('package_id', 'other'), ('manifest_sha256', 'b'*64)]:
        args = trajectory_rows_fixture()
        args['rankings'].loc[2, field] = value
        with pytest.raises(ValueError, match='identity'):
            parent_raw_trajectory_rows_v1(**args)
    args = trajectory_rows_fixture()
    args['rankings'] = pd.concat([args['rankings'], args['rankings'].iloc[2:3]])
    with pytest.raises(ValueError, match='identity'):
        parent_raw_trajectory_rows_v1(**args)
    for change in ('raw', 'clock', 'status', 'target'):
        args = trajectory_rows_fixture()
        field, value = {'raw': (RAW_FEATURES[0], .99), 'clock': (RAW_CLOCK, args['calendar'][6]),
            'status': (RAW_STATUS, 'UNKNOWN_SOURCE_OR_INPUT'), 'target': (KEY[1], args['calendar'][7])}[change]
        args['base_rows'].loc[0, field] = value
        with pytest.raises(ValueError):
            parent_raw_trajectory_rows_v1(**args)


@pytest.mark.parametrize('value,valid', [(0., True), (-.1, True), (None, True), (Decimal('NaN'), True), (True, False), ('0.1', False), (np.inf, False), (Decimal('sNaN'), False)])
def test_requested_lag_numeric_missing_is_not_malformed(value, valid):
    args = trajectory_rows_fixture()
    args['rankings']['raw__lstm'] = args['rankings']['raw__lstm'].astype(object)
    args['rankings'].at[2, 'raw__lstm'] = value
    if valid:
        rows = parent_raw_trajectory_rows_v1(**args)
        assert len(rows) == 2 and rows[STATUS].iloc[0] == ('UNKNOWN_SOURCE_OR_INPUT' if value is None or isinstance(value, Decimal) else 'AVAILABLE')
    else:
        with pytest.raises(ValueError, match='numeric|finite'):
            parent_raw_trajectory_rows_v1(**args)


def test_finite_difference_overflow_is_not_normal_missing():
    overflow = trajectory_rows_fixture()
    overflow['base_rows'].loc[0, RAW_FEATURES[0]] = 1e308
    overflow['rankings'].loc[0, 'raw__lstm'] = 1e308
    overflow['rankings'].loc[2, 'raw__lstm'] = -1e308
    with pytest.raises(ValueError, match='overflow'):
        parent_raw_trajectory_rows_v1(**overflow)


def test_actual15_17_same_supervision_test_poison_json_parity_mature_labels():
    rows, configuration = rows_fixture()
    for name in TRAJECTORY_FEATURES:
        rows[name] = .01
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
    events = []
    fitted = train_parent_raw_trajectory_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.diagnostics['train_rows'] == 107
    assert fitted.models['matched_mean']['features'] == 15 and fitted.models['candidate_mean']['features'] == 17
    assert fitted.recipe['matched_information_features'] == list(RAW_FEATURES)
    query = rows.iloc[1:3]
    for arm, width in [('matched', 15), ('candidate', 17)]:
        matrix = information_matrix_v1(query, arm=arm, information_features=TRAJECTORY_FEATURES, matched_information_features=RAW_FEATURES)
        assert matrix.shape == (2, width) and np.array_equal(matrix[:, 12:14], query.loc[:, RAW_FEATURES].to_numpy())
        assert len(parent_raw_trajectory_nodes_v1(fitted=fitted, rows=query, arm=arm)) == 2
    poisoned = rows.copy()
    poisoned.loc[poisoned.split.eq('test'), [*D_FEATURES, *TRAJECTORY_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert train_parent_raw_trajectory_v1(rows=poisoned, configuration=configuration, before_fit=lambda _: None).model_sha256 == fitted.model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_parent_raw_trajectory_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('no fit'))


def test_price_holes_shared_unknown_empty_bad_model_width_and_numeric():
    fitted = trajectory_fit_fixture()
    features = dict.fromkeys((*D_FEATURES, *TRAJECTORY_FEATURES), .01)
    query = pd.DataFrame([{**features, 'actual_gap_bps': gap, STATUS: status} for gap, status in [(-100., 'AVAILABLE'), (0., 'AVAILABLE'), (100., 'UNKNOWN_SOURCE_OR_INPUT')]])
    for arm in ('matched', 'candidate'):
        assert parent_raw_trajectory_nodes_v1(fitted=fitted, rows=query, arm=arm).status.tolist() == ['ACCEPTABLE', 'UNKNOWN_INPUT_OR_SUPPORT', 'UNKNOWN_INPUT_OR_SUPPORT']
        assert parent_raw_trajectory_price_set_v1(fitted=fitted, d_features=features, arm=arm, reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3).intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    corrupt = {**fitted.models, 'matched_mean': {**fitted.models['matched_mean'], 'features': 13}}
    with pytest.raises(ValueError, match='15/17'):
        parent_raw_trajectory_nodes_v1(fitted=replace(fitted, models=corrupt), rows=query, arm='matched')
    assert parent_raw_trajectory_nodes_v1(fitted=fitted, rows=query.iloc[:0], arm='candidate').empty
    query.at[0, DELTA_FEATURES[0]] = np.inf
    with pytest.raises(ValueError, match='finite'):
        parent_raw_trajectory_nodes_v1(fitted=fitted, rows=query, arm='candidate')
