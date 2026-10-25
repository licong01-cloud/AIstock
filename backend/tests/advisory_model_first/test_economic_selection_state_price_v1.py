import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_selection_state_price_v1 import STATE_FEATURES, SelectionStatePricePlanV1, selection_state_fit_identity_v1, selection_state_nodes_v1, selection_state_price_set_v1, selection_state_rows_v1, train_selection_state_price_v1
from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture


def test_single_fixed_plan_preserves_family_budget_roles_and_no_activation():
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values['budget_anchor_ref'] = dict(artifact_uri='F:/unit/campaign/advprice2_fixture/preregistered/manifest.json',
        sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor')
    plan = SelectionStatePricePlanV1(**values)
    assert str(plan.campaign_root).replace('\\', '/') == 'F:/unit/campaign'
    assert plan.parameters['campaign_fit_budget'] == 19 and plan.parameters['physical_fit_budget'] == 4
    assert plan.parameters['information_features'] == list(STATE_FEATURES) and not plan.deployable
    for update in ({'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'train_start': '2020-01-01'},
                   {'budget_anchor_ref': {**values['budget_anchor_ref'], 'role': 'foreign_role'}},
                   {'budget_anchor_ref': {**values['budget_anchor_ref'], 'artifact_uri': 'C:/unit/preregistered/manifest.json'}},
                   {'budget_anchor_ref': {**values['budget_anchor_ref'], 'artifact_uri': 'relative/preregistered/manifest.json'}},
                   {'budget_anchor_ref': {**values['budget_anchor_ref'], 'artifact_uri': 'F:/unit/prepared/manifest.json'}}):
        with pytest.raises(ValueError):
            SelectionStatePricePlanV1.model_validate({**values, **update})


def state_fixture():
    days = pd.bdate_range('2025-01-02', periods=24)
    history = pd.DataFrame([{KEY[0]: day, KEY[1]: days[index+1], KEY[2]: f'{rank:06d}.SZ', 'selection_effective_rank': rank}
        for index, day in enumerate(days[:-1]) for rank in range(1, 41)])
    candidates = history.loc[history[KEY[0]].isin((days[0], days[20])) & history.selection_effective_rank.le(2)].copy()
    return dict(candidates=candidates, rankings=history, calendar=days)


def test_complete_history_absence_censored41_future_poison_and_single_batch():
    args = state_fixture()
    days, history = args['calendar'], args['rankings']
    history.loc[history[KEY[0]].eq(days[19]) & history.instrument.eq('000001.SZ'), 'instrument'] = '999999.SZ'
    result = selection_state_rows_v1(**args)
    assert len(result) == 4 and result.state_feature_status.tolist() == ['UNKNOWN_20D_WARMUP']*2+['AVAILABLE']*2
    row = result.loc[result[KEY[0]].eq(days[20]) & result.instrument.eq('000001.SZ')].iloc[0]
    assert row.selection_top5_rate5 == .8 and row.selection_top20_rate20 == .95
    assert row.selection_censored_rank_improvement1 == 40
    poisoned = history.copy()
    poisoned.loc[poisoned[KEY[0]].gt(days[20]), 'selection_effective_rank'] = -999
    poisoned['net_return_bps'] = -999999.
    pd.testing.assert_frame_equal(result, selection_state_rows_v1(**{**args, 'rankings': poisoned}))
    single = selection_state_rows_v1(**{**args, 'candidates': args['candidates'].iloc[[2]]})
    pd.testing.assert_frame_equal(single.reset_index(drop=True), result.iloc[[2]].reset_index(drop=True))


@pytest.mark.parametrize('missing_day', [False, True])
def test_missing_or_incomplete_list_is_unknown_not_zero_or_dropped(missing_day):
    args = state_fixture()
    history, day = args['rankings'], args['calendar'][10]
    args['rankings'] = history.loc[~(history[KEY[0]].eq(day) & (True if missing_day else history.selection_effective_rank.eq(40)))]
    result = selection_state_rows_v1(**args)
    assert len(result) == 4
    affected = result.loc[result[KEY[0]].eq(args['calendar'][20])]
    assert affected.state_feature_status.eq('UNKNOWN_HISTORICAL_LIST').all() and affected.loc[:, STATE_FEATURES].isna().all().all()


@pytest.mark.parametrize('defect', ['duplicate_rank', 'wrong_T', 'foreign_candidate'])
def test_contradictory_rank_clock_or_candidate_fails_closed(defect):
    args = state_fixture()
    if defect == 'duplicate_rank':
        args['rankings'].loc[1, 'selection_effective_rank'] = 1
    elif defect == 'wrong_T':
        args['rankings'].loc[0, KEY[1]] = args['calendar'][2]
    else:
        args['candidates'].loc[args['candidates'].index[0], KEY[2]] = '999999.SZ'
    with pytest.raises(ValueError):
        selection_state_rows_v1(**args)


def test_fixed_M5_four_fit_common_supervision_and_test_poison():
    from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
    from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
    rows, configuration = rows_fixture()
    rows[list(STATE_FEATURES)] = [.4, .6, 1.]
    rows['state_feature_status'] = 'AVAILABLE'
    rows.loc[0, list(STATE_FEATURES)] = np.nan
    rows.loc[0, 'state_feature_status'] = 'UNKNOWN_HISTORICAL_LIST'
    events = []
    fitted = train_selection_state_price_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.recipe['selection_features'] == list(STATE_FEATURES) and 'sector_features' not in fitted.recipe
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    assert fitted.support.contains(20.) and not fitted.diagnostics['deployable']
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *STATE_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    replay = train_selection_state_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: None)
    assert fitted.model_sha256 == replay.model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_selection_state_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_M5_price_sets_preserve_holes_common_unknown_and_model_identity():
    from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
    from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), selection_features=list(STATE_FEATURES))
    fitted = SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        selection_state_fit_identity_v1(recipe, original.models, original.support))
    features = {**dict.fromkeys(D_FEATURES, .02), **dict(zip(STATE_FEATURES, (.4, .6, 1.), strict=True))}
    grid = selection_state_price_set_v1(fitted=fitted, d_features=features, arm='candidate', reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3)
    assert grid.intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    query = pd.DataFrame([{**features, 'actual_gap_bps': gap} for gap in (-100., 0., 100.)])
    assert selection_state_nodes_v1(fitted=fitted, rows=query, arm='candidate').status.tolist() == ['ACCEPTABLE', 'UNKNOWN_INPUT_OR_SUPPORT', 'ACCEPTABLE']
    query[STATE_FEATURES[0]] = np.nan
    for arm in ('matched', 'candidate'):
        assert selection_state_nodes_v1(fitted=fitted, rows=query, arm=arm).status.eq('UNKNOWN_INPUT_OR_SUPPORT').all()
    with pytest.raises(ValueError, match='identity'):
        selection_state_nodes_v1(fitted=original, rows=query, arm='candidate')
