import json
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_candidate_cohort_v1 import CLOCK, CANDIDATE_COHORT_FEATURES, STATUS, UNKNOWN, CandidateCohortPlanV1, candidate_cohort_fit_identity_v1, candidate_cohort_rows_v1, train_candidate_cohort_v1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def candidate_cohort_fixture():
    calendar = pd.bdate_range('2025-01-02', periods=3)
    candidates = pd.DataFrame([{KEY[0]: calendar[day], KEY[1]: calendar[day+1], KEY[2]: symbol}
        for day in (0, 1) for symbol in ('000001.SZ', '000002.SZ', '000003.SZ')])
    features = candidates.assign(ret_1=[.1, -.05, 0.]*2, feature_visible_through=candidates[KEY[0]])
    return dict(candidates=candidates, features=features, calendar=calendar)


def candidate_cohort_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), candidate_cohort_features=list(CANDIDATE_COHORT_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        candidate_cohort_fit_identity_v1(recipe, original.models, original.support))


def test_manual_same_stock_and_market_different_companions_original_order():
    args = candidate_cohort_fixture()
    args['candidates'] = args['candidates'].iloc[[4, 0, 2, 5, 1, 3]].reset_index(drop=True)
    result = candidate_cohort_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result[STATUS].eq('AVAILABLE').all()
    values = np.array([.1, -.05, 0.])
    np.testing.assert_allclose(result.loc[1, list(CANDIDATE_COHORT_FEATURES)].astype(float), [values.mean(), values.std(ddof=0), 1/3])
    args['features'].loc[1, 'ret_1'] = .2
    changed = candidate_cohort_rows_v1(**args)
    assert changed.loc[1, CANDIDATE_COHORT_FEATURES[0]] == pytest.approx(.1)
    assert changed.loc[1, CANDIDATE_COHORT_FEATURES[2]] == pytest.approx(2/3)
    assert args['features'].loc[0, 'ret_1'] == .1
    pd.testing.assert_frame_equal(result.loc[[0, 3, 5]], changed.loc[[0, 3, 5]])


@pytest.mark.parametrize('missing', ['member', 'ret', 'clock', 'quiet_nan'])
def test_original_complete_group_missing_keeps_all_members_and_dates_UNKNOWN(missing):
    args = candidate_cohort_fixture()
    if missing == 'member':
        args['features'] = args['features'].drop(index=1)
    elif missing == 'clock':
        args['features'].loc[1, 'feature_visible_through'] = pd.NaT
    elif missing == 'quiet_nan':
        args['features']['ret_1'] = args['features'].ret_1.astype(object)
        args['features'].loc[1, 'ret_1'] = Decimal('NaN')
    else:
        args['features'].loc[1, 'ret_1'] = np.nan
    result = candidate_cohort_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY])
    assert result[STATUS].tolist() == ['UNKNOWN_COHORT_SOURCE']*3+['AVAILABLE']*3
    assert set(json.loads(result.loc[0, UNKNOWN])) == set(CANDIDATE_COHORT_FEATURES)
    assert result.loc[:2, list(CANDIDATE_COHORT_FEATURES)].isna().all().all()


def test_flat_zero_one_singleton_and_stable_extreme_statistics():
    args = candidate_cohort_fixture()
    for ret, share in ((0., 0.), (.1, 1.), (-.1, 0.)):
        args['features'].ret_1 = ret
        row = candidate_cohort_rows_v1(**args).iloc[0]
        np.testing.assert_allclose(row[list(CANDIDATE_COHORT_FEATURES)].astype(float), [ret, 0., share], atol=1e-15)
    args['features'].ret_1 = [1e308, 1e308, 0.]*2
    row = candidate_cohort_rows_v1(**args).iloc[0]
    np.testing.assert_allclose(row[list(CANDIDATE_COHORT_FEATURES[:2])].astype(float)/1e308, [2/3, np.sqrt(2)/3])
    args['candidates'] = args['candidates'].iloc[:1]
    row = candidate_cohort_rows_v1(**args).iloc[0]
    np.testing.assert_allclose(row[list(CANDIDATE_COHORT_FEATURES)].astype(float), [1e308, 0., 1.])


@pytest.mark.parametrize('bad', [True, np.inf, 'bad', 10**1000, Decimal('sNaN'), Decimal('1e-10000'), -1., -1.1])
def test_consumed_known_bad_return_not_unknown(bad):
    args = candidate_cohort_fixture()
    args['features']['ret_1'] = args['features'].ret_1.astype(object)
    args['features'].loc[1, 'ret_1'] = bad
    with pytest.raises(ValueError):
        candidate_cohort_rows_v1(**args)


def test_no_Y_maturity_action_population_filter_or_future_foreign_consumption():
    args = candidate_cohort_fixture()
    expected = candidate_cohort_rows_v1(**args)
    args['features'] = args['features'].assign(values_available=False, training_eligible=False, model_action='SKIP', gross_value_ratio=-99999.)
    pd.testing.assert_frame_equal(expected, candidate_cohort_rows_v1(**args))
    args['candidates'] = args['candidates'].iloc[:3].reset_index(drop=True)
    expected = candidate_cohort_rows_v1(**args)
    args['features'].loc[3:, 'ret_1'] = -999.
    args['features'].loc[3:, 'feature_visible_through'] = args['calendar'][-1]
    foreign = args['features'].iloc[:1].assign(instrument='000009.SZ', ret_1=-999., feature_visible_through=args['calendar'][-1])
    args['features'] = pd.concat([args['features'], foreign], ignore_index=True)
    pd.testing.assert_frame_equal(expected, candidate_cohort_rows_v1(**args))


def test_consumed_future_duplicate_wrong_T_and_empty_schema():
    args = candidate_cohort_fixture()
    args['features'].loc[1, 'feature_visible_through'] = args['calendar'][1]
    with pytest.raises(ValueError, match='future feature'):
        candidate_cohort_rows_v1(**args)
    args = candidate_cohort_fixture()
    with pytest.raises(Exception, match='duplicate'):
        candidate_cohort_rows_v1(**{**args, 'features': pd.concat([args['features'], args['features'].iloc[:1]], ignore_index=True)})
    wrong = args['candidates'].copy()
    wrong.loc[0, KEY[1]] = args['calendar'][-1]
    with pytest.raises(ValueError, match='next session'):
        candidate_cohort_rows_v1(**{**args, 'candidates': wrong})
    result = candidate_cohort_rows_v1(**{**args, 'candidates': args['candidates'].iloc[:0]})
    assert result.empty and result.columns.tolist() == [*KEY, *CANDIDATE_COHORT_FEATURES, STATUS, CLOCK, UNKNOWN]


def test_common_mature_13_16_training_no_test_consumption_and_support_not_filtered():
    rows, configuration = rows_fixture()
    rows[list(CANDIDATE_COHORT_FEATURES)] = [.01, .03, .6]
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, CANDIDATE_COHORT_FEATURES[0]] = np.nan
    rows.loc[0, STATUS] = 'UNKNOWN_COHORT_SOURCE'
    events = []
    fitted = train_candidate_cohort_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1 and fitted.support.contains(20.)
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *CANDIDATE_COHORT_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_candidate_cohort_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_candidate_cohort_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_fixed63_plan_no_policy_search_or_activation():
    from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='candidate_cohort_predecessor'))
    plan = CandidateCohortPlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 63 and plan.parameters['source_select_budget'] == 0 and not plan.deployable
    for update in ({'ddof': 1}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'model_id': 'M15'}):
        with pytest.raises(ValueError):
            CandidateCohortPlanV1.model_validate({**values, **update})
