import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1, sector_fit_identity_v1, sector_matrix_v1, sector_nodes_v1, sector_price_set_v1, train_sector_price_v1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture


def sector_fit_fixture():
    recipe = dict(d_features=list(D_FEATURES), sector_features=list(SECTOR_FEATURES))
    models = {arm+'_'+head: dict(kind='gbdt', features=13 if arm == 'matched' else 16,
        initial=1.04 if head == 'mean' else .99, learning_rate=.05,
        trees=[dict(left=[-1], right=[-1], feature=[-2], threshold=[-2.], value=[0.])])
        for arm in ('matched', 'candidate') for head in ('mean', 'path')}
    support = ValueAnchorGapSupportV1(((-300., -100.), (100., 300.)))
    return SectorPriceFitV1(recipe, models, support, dict(fitted_head_count=4, index_build_count=0), sector_fit_identity_v1(recipe, models, support))


def test_four_fixed_fits_shared_train_test_poison_and_support_not_class_filtered():
    rows, configuration = rows_fixture()
    for name in SECTOR_FEATURES:
        rows[name] = .02
    rows['sector_feature_status'] = 'AVAILABLE'
    rows.loc[0, list(SECTOR_FEATURES)] = np.nan
    rows.loc[0, 'sector_feature_status'] = 'UNKNOWN_CLASSIFICATION_OR_MAPPING'
    events = []
    fitted = train_sector_price_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == fitted.diagnostics['fitted_head_count'] == 4
    assert fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    poisoned = rows.copy()
    poisoned.loc[poisoned.split.eq('test'), [*D_FEATURES, *SECTOR_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    replay = train_sector_price_v1(rows=poisoned, configuration=configuration, before_fit=lambda name: None)
    assert fitted.model_sha256 == replay.model_sha256
    assert fitted.support.contains(20.)
    # Equal information values preserve the M1 mathematical kernel exactly;
    # M5 gets its own field names and identity, not sector-labelled sidecars.
    from backend.services.advisory_model_first.economic_selection_state_price_v1 import STATE_FEATURES, train_selection_state_price_v1
    renamed = rows.rename(columns={**dict(zip(SECTOR_FEATURES, STATE_FEATURES, strict=True)), 'sector_feature_status': 'state_feature_status'})
    same = train_selection_state_price_v1(rows=renamed, configuration=configuration, before_fit=lambda _: None)
    assert same.models == fitted.models and same.support == fitted.support
    assert same.model_sha256 != fitted.model_sha256
    query = rows.loc[rows.split.eq('train')].iloc[1:3]
    assert sector_matrix_v1(query, arm='matched').shape == (2, 13)
    assert sector_matrix_v1(query, arm='candidate').shape == (2, 16)
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_sector_price_v1(rows=rows, configuration=configuration, before_fit=lambda name: pytest.fail('must not fit'))


def test_price_grid_holes_single_batch_and_common_unknown_mask():
    fitted = sector_fit_fixture()
    features = {**dict.fromkeys(D_FEATURES, .02), **dict.fromkeys(SECTOR_FEATURES, .01)}
    grid = sector_price_set_v1(fitted=fitted, d_features=features, arm='candidate', reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3)
    assert grid.intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    query = pd.DataFrame([{**features, 'actual_gap_bps': gap} for gap in (-100., 0., 100.)])
    batch = sector_nodes_v1(fitted=fitted, rows=query, arm='candidate')
    assert batch.status.tolist() == ['ACCEPTABLE', 'UNKNOWN_INPUT_OR_SUPPORT', 'ACCEPTABLE']
    for index in query.index:
        assert sector_nodes_v1(fitted=fitted, rows=query.loc[[index]], arm='candidate').loc[index, 'status'] == batch.loc[index, 'status']
    query[SECTOR_FEATURES[0]] = np.nan
    for arm in ('matched', 'candidate'):
        assert sector_nodes_v1(fitted=fitted, rows=query, arm=arm).status.eq('UNKNOWN_INPUT_OR_SUPPORT').all()


def test_M7_router_does_not_change_old_information_identity_formula():
    from backend.services.advisory_model_first.economic_price_path_value_v1 import PRICE_PATH_FEATURES
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import _information_key, information_fit_identity_v1
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
    fitted = sector_fit_fixture()
    for model in ('M1', 'M5', 'M6'):
        assert information_fit_identity_v1(fitted.recipe, fitted.models, fitted.support, model_id=model) == sha(
            dict(model_id=model, recipe=fitted.recipe, models=fitted.models, support=list(fitted.support.intervals_bps)))
    assert _information_key('M7', PRICE_PATH_FEATURES) == 'price_path_features'
    with pytest.raises(ValueError, match='block/model'):
        _information_key('M7', SECTOR_FEATURES)


def test_M8_router_keeps_old_information_identity_formula():
    from backend.services.advisory_model_first.economic_market_risk_price_v1 import MARKET_RISK_FEATURES
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import _information_key, information_fit_identity_v1
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
    fitted = sector_fit_fixture()
    for model in ('M1', 'M5', 'M6', 'M7'):
        assert information_fit_identity_v1(fitted.recipe, fitted.models, fitted.support, model_id=model) == sha(
            dict(model_id=model, recipe=fitted.recipe, models=fitted.models, support=list(fitted.support.intervals_bps)))
    assert _information_key('M8', MARKET_RISK_FEATURES) == 'market_risk_features'
    with pytest.raises(ValueError, match='block/model'):
        _information_key('M8', SECTOR_FEATURES)


def test_information_routes_keep_previous_identity_formulas_and_reject_wrong_blocks():
    from backend.services.advisory_model_first.economic_volume_context_price_v1 import VOLUME_CONTEXT_FEATURES
    from backend.services.advisory_model_first.economic_breadth_state_price_v1 import BREADTH_STATE_FEATURES
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import _information_key, information_fit_identity_v1
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
    fitted = sector_fit_fixture()
    for model in ('M1', 'M5', 'M6', 'M7', 'M8', 'M9'):
        assert information_fit_identity_v1(fitted.recipe, fitted.models, fitted.support, model_id=model) == sha(
            dict(model_id=model, recipe=fitted.recipe, models=fitted.models, support=list(fitted.support.intervals_bps)))
    assert _information_key('M9', VOLUME_CONTEXT_FEATURES) == 'volume_context_features'
    with pytest.raises(ValueError, match='block/model'):
        _information_key('M9', SECTOR_FEATURES)
    assert _information_key('M10', BREADTH_STATE_FEATURES) == 'breadth_state_features'
    with pytest.raises(ValueError, match='block/model'):
        _information_key('M10', VOLUME_CONTEXT_FEATURES)
