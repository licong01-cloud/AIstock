from dataclasses import replace
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_parent_raw_v1 import JOINT_FEATURES, RAW_FEATURES, SECTOR_FEATURES, SOURCE_CLOCK, SOURCE_STATUS, STATUS, sector_parent_raw_fit_identity_v1, sector_parent_raw_nodes_v1, sector_parent_raw_price_set_v1, sector_parent_raw_rows_v1, train_sector_parent_raw_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1, information_matrix_v1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def joint_fit_fixture():
    old = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), sector_parent_raw_features=list(JOINT_FEATURES), matched_information_features=list(SECTOR_FEATURES))
    models = {name: {**body, 'features': 16 if name.startswith('matched') else 18} for name, body in old.models.items()}
    return SectorPriceFitV1(recipe, models, old.support, old.diagnostics, sector_parent_raw_fit_identity_v1(recipe, models, old.support))


def joint_rows_fixture():
    days = pd.bdate_range('2025-01-02', periods=3)
    keys = pd.DataFrame([{KEY[0]: days[0], KEY[1]: days[1], KEY[2]: f'{i:06d}.SZ'} for i in range(2)])
    sector = keys.assign(**dict.fromkeys(D_FEATURES, 0.), **dict.fromkeys(SECTOR_FEATURES, .01),
        sector_feature_status='AVAILABLE', sector_feature_visible_through=days[0], gross_value_ratio=1.04)
    flow = keys.assign(**dict.fromkeys(RAW_FEATURES, .2), parent_raw_score_feature_status='AVAILABLE',
        parent_raw_score_feature_visible_through=days[0], gross_value_ratio='unused poison', irrelevant=True)
    return dict(sector_rows=sector, raw_rows=flow, candidates=keys, calendar=days)


def test_union_exact_keys_order_clock_and_normal_unknown_without_second_labels():
    args = joint_rows_fixture()
    args['raw_rows'] = args['raw_rows'].iloc[::-1]
    args['raw_rows'].loc[1, RAW_FEATURES[0]] = np.nan
    result = sector_parent_raw_rows_v1(**args)
    assert result[KEY].equals(args['sector_rows'][KEY]) and result.gross_value_ratio.eq(1.04).all()
    assert result[STATUS].tolist() == ['AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT']
    assert all(name in result for name in (*SOURCE_CLOCK, *SOURCE_STATUS)) and 'irrelevant' not in result
    args['raw_rows'].loc[1, SOURCE_CLOCK[1]] = args['calendar'][1]
    with pytest.raises(ValueError, match='clock'):
        sector_parent_raw_rows_v1(**args)
    args = joint_rows_fixture()
    args['raw_rows'] = args['raw_rows'].iloc[:1]
    with pytest.raises(ValueError, match='keys'):
        sector_parent_raw_rows_v1(**args)
    args['raw_rows'] = pd.concat([args['raw_rows']]*2, ignore_index=True)
    with pytest.raises(ValueError, match='keys'):
        sector_parent_raw_rows_v1(**args)


@pytest.mark.parametrize('value,valid', [(0., True), (-.1, True), (None, True), (Decimal('NaN'), True), (True, False), ('0.1', False), (np.inf, False), (Decimal('sNaN'), False)])
def test_consumed_numeric_missing_versus_malformed(value, valid):
    args = joint_rows_fixture()
    args['raw_rows'][RAW_FEATURES[0]] = pd.Series([value, .1], dtype=object)
    if valid:
        rows = sector_parent_raw_rows_v1(**args)
        assert len(rows) == 2 and rows[STATUS].iloc[0] == ('UNKNOWN_SOURCE_OR_INPUT' if value is None or isinstance(value, Decimal) else 'AVAILABLE')
    else:
        with pytest.raises(ValueError, match='numeric|finite'):
            sector_parent_raw_rows_v1(**args)


def test_actual_16_18_shared_supervision_test_poison_and_json_parity():
    rows, configuration = rows_fixture()
    for name in JOINT_FEATURES:
        rows[name] = .01
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
    events = []
    fitted = train_sector_parent_raw_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.diagnostics['train_rows'] == 107
    assert fitted.models['matched_mean']['features'] == 16 and fitted.models['candidate_mean']['features'] == 18
    assert fitted.recipe['matched_information_features'] == list(SECTOR_FEATURES)
    query = rows.iloc[1:3]
    for arm, width in [('matched', 16), ('candidate', 18)]:
        matrix = information_matrix_v1(query, arm=arm, information_features=JOINT_FEATURES, matched_information_features=SECTOR_FEATURES)
        assert matrix.shape == (2, width) and np.array_equal(matrix[:, 12:15], query.loc[:, SECTOR_FEATURES].to_numpy())
        assert len(sector_parent_raw_nodes_v1(fitted=fitted, rows=query, arm=arm)) == 2
    poisoned = rows.copy()
    poisoned.loc[poisoned.split.eq('test'), [*D_FEATURES, *JOINT_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    replay = train_sector_parent_raw_v1(rows=poisoned, configuration=configuration, before_fit=lambda _: None)
    assert replay.model_sha256 == fitted.model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_sector_parent_raw_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('no fit'))


def test_joint_support_holes_shared_unknown_and_relabelled_width_rejected():
    fitted = joint_fit_fixture()
    features = dict.fromkeys((*D_FEATURES, *JOINT_FEATURES), .01)
    query = pd.DataFrame([{**features, 'actual_gap_bps': gap, STATUS: status} for gap, status in [(-100., 'AVAILABLE'), (0., 'AVAILABLE'), (100., 'UNKNOWN_SOURCE_OR_INPUT')]])
    for arm in ('matched', 'candidate'):
        assert sector_parent_raw_nodes_v1(fitted=fitted, rows=query, arm=arm).status.tolist() == ['ACCEPTABLE', 'UNKNOWN_INPUT_OR_SUPPORT', 'UNKNOWN_INPUT_OR_SUPPORT']
        assert sector_parent_raw_price_set_v1(fitted=fitted, d_features=features, arm=arm, reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3).intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    corrupt = {**fitted.models, 'matched_mean': {**fitted.models['matched_mean'], 'features': 13}}
    with pytest.raises(ValueError, match='16/18'):
        sector_parent_raw_nodes_v1(fitted=replace(fitted, models=corrupt), rows=query, arm='matched')
    assert sector_parent_raw_nodes_v1(fitted=fitted, rows=query.iloc[:0], arm='candidate').empty
