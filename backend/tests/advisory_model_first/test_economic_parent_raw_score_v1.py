from dataclasses import replace
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_parent_raw_score_v1 import RAW_FEATURES, STATUS, parent_raw_score_fit_identity_v1, parent_raw_score_nodes_v1, parent_raw_score_price_set_v1, parent_raw_score_rows_v1, train_parent_raw_score_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1, information_matrix_v1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture

MAPPING = {'lstm': 'raw__lstm', 'fund': 'raw__fund'}


def raw_fit_fixture():
    old = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), parent_raw_score_features=list(RAW_FEATURES))
    models = {name: {**body, 'features': 13 if name.startswith('matched') else 15} for name, body in old.models.items()}
    return SectorPriceFitV1(recipe, models, old.support, old.diagnostics, parent_raw_score_fit_identity_v1(recipe, models, old.support))


def raw_rows_fixture():
    days = pd.bdate_range('2025-01-02', periods=3)
    keys = pd.DataFrame([{KEY[0]: days[0], KEY[1]: days[1], KEY[2]: f'{i:06d}.SZ'} for i in range(2)])
    base = keys.assign(**dict.fromkeys(D_FEATURES, 0.), gross_value_ratio=1.04)
    rankings = keys.assign(raw__lstm=.1, raw__fund=-.1, trade_date=days[0], package_id='pkg', manifest_sha256='a'*64)
    return dict(base_rows=base, rankings=rankings, candidates=keys, calendar=days,
        component_raw_columns=MAPPING, package_id='pkg', package_manifest_sha256='a'*64)


def test_raw_information_affine_invariance_not_probability_and_exact_population():
    raw = np.array([-.7, .1, 2.3])
    shifted = raw*3+9
    assert np.allclose((raw-raw.mean())/raw.std(), (shifted-shifted.mean())/shifted.std()) and not np.array_equal(raw, shifted)
    args = raw_rows_fixture()
    # Review-only or future stocks are projected away before numeric parsing.
    foreign = args['rankings'].iloc[:1].assign(instrument='999999.SZ', raw__lstm='not consumed', trade_date=args['calendar'][2])
    args['rankings'] = pd.concat([args['rankings'].iloc[::-1], foreign], ignore_index=True)
    args['rankings'].loc[0, 'raw__lstm'] = None
    result = parent_raw_score_rows_v1(**args)
    assert result[KEY].equals(args['base_rows'][KEY]) and result.gross_value_ratio.eq(1.04).all()
    assert result[STATUS].tolist() == ['AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT'] and len(result) == 2
    for field, value in [('trade_date', args['calendar'][1]), ('package_id', 'other'), ('manifest_sha256', 'b'*64)]:
        invalid = raw_rows_fixture()
        invalid['rankings'].loc[0, field] = value
        with pytest.raises(ValueError, match='identity'):
            parent_raw_score_rows_v1(**invalid)
    for duplicate in (False, True):
        invalid = raw_rows_fixture()
        invalid['rankings'] = pd.concat([invalid['rankings'], invalid['rankings'].iloc[:1]]) if duplicate else invalid['rankings'].iloc[:1]
        with pytest.raises(ValueError, match='identity'):
            parent_raw_score_rows_v1(**invalid)


@pytest.mark.parametrize('value,valid', [(0., True), (-.1, True), (None, True), (Decimal('NaN'), True), (True, False), ('0.1', False), (np.inf, False), (Decimal('sNaN'), False)])
def test_consumed_raw_missing_versus_malformed(value, valid):
    args = raw_rows_fixture()
    args['rankings']['raw__lstm'] = pd.Series([value, .1], dtype=object)
    if valid:
        result = parent_raw_score_rows_v1(**args)
        assert len(result) == 2 and result[STATUS].iloc[0] == ('UNKNOWN_SOURCE_OR_INPUT' if value is None or isinstance(value, Decimal) else 'AVAILABLE')
    else:
        with pytest.raises(ValueError, match='numeric|finite'):
            parent_raw_score_rows_v1(**args)


def test_actual_13_15_shared_supervision_test_poison_and_json_parity():
    rows, configuration = rows_fixture()
    for name in RAW_FEATURES:
        rows[name] = .01
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
    events = []
    fitted = train_parent_raw_score_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.diagnostics['train_rows'] == 107
    assert fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 15
    assert fitted.recipe['parent_raw_score_features'] == list(RAW_FEATURES) and 'matched_information_features' not in fitted.recipe
    for arm, width in [('matched', 13), ('candidate', 15)]:
        query = rows.iloc[1:3]
        matrix = information_matrix_v1(query, arm=arm, information_features=RAW_FEATURES)
        assert matrix.shape == (2, width)
        assert len(parent_raw_score_nodes_v1(fitted=fitted, rows=query, arm=arm)) == 2
    poisoned = rows.copy()
    poisoned.loc[poisoned.split.eq('test'), [*D_FEATURES, *RAW_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    replay = train_parent_raw_score_v1(rows=poisoned, configuration=configuration, before_fit=lambda _: None)
    assert replay.model_sha256 == fitted.model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_parent_raw_score_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('no fit'))


def test_price_support_holes_shared_unknown_empty_and_identity():
    fitted = raw_fit_fixture()
    features = dict.fromkeys((*D_FEATURES, *RAW_FEATURES), .01)
    query = pd.DataFrame([{**features, 'actual_gap_bps': gap, STATUS: status} for gap, status in [(-100., 'AVAILABLE'), (0., 'AVAILABLE'), (100., 'UNKNOWN_SOURCE_OR_INPUT')]])
    for arm in ('matched', 'candidate'):
        assert parent_raw_score_nodes_v1(fitted=fitted, rows=query, arm=arm).status.tolist() == ['ACCEPTABLE', 'UNKNOWN_INPUT_OR_SUPPORT', 'UNKNOWN_INPUT_OR_SUPPORT']
        assert parent_raw_score_price_set_v1(fitted=fitted, d_features=features, arm=arm, reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3).intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    corrupt = {**fitted.models, 'candidate_mean': {**fitted.models['candidate_mean'], 'features': 16}}
    with pytest.raises(ValueError, match='13/15'):
        parent_raw_score_nodes_v1(fitted=replace(fitted, models=corrupt), rows=query, arm='candidate')
    assert parent_raw_score_nodes_v1(fitted=fitted, rows=query.iloc[:0], arm='candidate').empty
