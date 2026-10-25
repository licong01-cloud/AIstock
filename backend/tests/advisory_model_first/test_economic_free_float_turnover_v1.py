import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_free_float_turnover_v1 import CLOCK, FREE_FLOAT_FEATURES, STATUS, UNKNOWN, FreeFloatTurnoverPlanV1, free_float_turnover_fit_identity_v1, free_float_turnover_rows_v1, train_free_float_turnover_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def free_float_fixture():
    calendar = pd.bdate_range('2025-01-02', periods=23)
    candidates = pd.DataFrame([{KEY[0]: calendar[index], KEY[1]: calendar[index+1], KEY[2]: '000001.SZ'} for index in (0, 19, 20)])
    basics = pd.DataFrame({'trade_date': calendar, 'instrument': '000001.SZ', 'turnover_rate_f': np.arange(1., 24.), 'free_share': 10000.})
    return dict(candidates=candidates, basics=basics, calendar=calendar)


def free_float_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), free_float_features=list(FREE_FLOAT_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        free_float_turnover_fit_identity_v1(recipe, original.models, original.support))


def test_fixed_percent_and_share_units_hand_calculated_and_new_denominator():
    args = free_float_fixture()
    result = free_float_turnover_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result[STATUS].tolist() == ['UNKNOWN_20D_HISTORY', 'AVAILABLE', 'AVAILABLE']
    np.testing.assert_allclose(result.loc[1, list(FREE_FLOAT_FEATURES)].astype(float), [.105, np.std(np.arange(1., 21.)/100), np.log(1e8)])
    assert set(json.loads(result.loc[0, UNKNOWN])) == set(FREE_FLOAT_FEATURES[:2])
    args['basics'].turnover_rate_f *= 2.
    args['basics'].free_share *= 3.
    changed = free_float_turnover_rows_v1(**args)
    np.testing.assert_allclose(changed.loc[1, list(FREE_FLOAT_FEATURES[:2])].astype(float), 2*result.loc[1, list(FREE_FLOAT_FEATURES[:2])].astype(float))
    assert changed.loc[1, FREE_FLOAT_FEATURES[2]]-result.loc[1, FREE_FLOAT_FEATURES[2]] == pytest.approx(np.log(3.))


def test_known_zero_turnover_and_stable_large_numbers_are_not_unknown():
    args = free_float_fixture()
    args['basics'].turnover_rate_f = 0.
    zero = free_float_turnover_rows_v1(**args)
    assert zero.loc[1, STATUS] == 'AVAILABLE' and zero.loc[1, list(FREE_FLOAT_FEATURES[:2])].tolist() == [0., 0.]
    args['basics'].turnover_rate_f = np.linspace(1e306, 1e308, len(args['basics']))
    args['basics'].free_share = 1e308
    result = free_float_turnover_rows_v1(**args)
    assert np.isfinite(result.loc[1, list(FREE_FLOAT_FEATURES)].astype(float)).all()
    assert result.loc[1, FREE_FLOAT_FEATURES[0]] > 1e300 and result.loc[1, FREE_FLOAT_FEATURES[1]] > 1e300
    assert result.loc[1, FREE_FLOAT_FEATURES[2]] == pytest.approx(np.log(1e308)+np.log(10000.))


@pytest.mark.parametrize(('missing', 'unknown'), [('rate', (0, 1)), ('D_shares', (2,)), ('row', (0, 1))])
def test_local_unknown_dependencies_retain_population_and_known_fields(missing, unknown):
    args = free_float_fixture()
    if missing == 'row':
        args['basics'] = args['basics'].drop(index=5)
    else:
        column, index = ('turnover_rate_f', 5) if missing == 'rate' else ('free_share', 19)
        args['basics'].loc[index, column] = np.nan
    result = free_float_turnover_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result.loc[1, STATUS].startswith('UNKNOWN')
    assert set(json.loads(result.loc[1, UNKNOWN])) == {FREE_FLOAT_FEATURES[index] for index in unknown}
    for index in set(range(3))-set(unknown):
        assert pd.notna(result.loc[1, FREE_FLOAT_FEATURES[index]])


@pytest.mark.parametrize(('field', 'bad'), [('turnover_rate_f', -1.), ('turnover_rate_f', True), ('turnover_rate_f', np.inf),
    ('turnover_rate_f', 'bad'), ('turnover_rate_f', 10**1000), ('free_share', 0.), ('free_share', np.inf), ('free_share', True)])
def test_consumed_known_bad_values_raise_not_unknown(field, bad):
    args = free_float_fixture()
    args['basics'][field] = args['basics'][field].astype(object)
    args['basics'].loc[19, field] = bad
    with pytest.raises(ValueError):
        free_float_turnover_rows_v1(**args)


def test_unconsumed_history_share_future_poison_duplicates_wrong_T_and_empty():
    args = free_float_fixture()
    args['candidates'] = args['candidates'].iloc[1:2].reset_index(drop=True)
    expected = free_float_turnover_rows_v1(**args)
    args['basics'].free_share = args['basics'].free_share.astype(object)
    args['basics'].loc[:18, 'free_share'] = 'unconsumed historical share'
    args['basics'].loc[20:, ['turnover_rate_f', 'free_share']] = -999.
    pd.testing.assert_frame_equal(expected, free_float_turnover_rows_v1(**args))
    with pytest.raises(Exception, match='duplicate'):
        free_float_turnover_rows_v1(**{**args, 'basics': pd.concat([args['basics'], args['basics'].iloc[:1]])})
    wrong = args['candidates'].copy()
    wrong.loc[0, KEY[1]] = args['calendar'][21]
    with pytest.raises(ValueError, match='next session'):
        free_float_turnover_rows_v1(**{**args, 'candidates': wrong})
    empty = free_float_turnover_rows_v1(**{**args, 'candidates': args['candidates'].iloc[:0]})
    assert empty.empty and empty.columns.tolist() == [*KEY, *FREE_FLOAT_FEATURES, STATUS, CLOCK, UNKNOWN]


def test_common_mature_13_16_training_test_poison_and_maturity_failure():
    rows, configuration = rows_fixture()
    rows[list(FREE_FLOAT_FEATURES)] = [.1, .02, np.log(1e8)]
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, FREE_FLOAT_FEATURES[0]] = np.nan
    rows.loc[0, STATUS] = 'UNKNOWN_FREE_FLOAT_SOURCE'
    events = []
    fitted = train_free_float_turnover_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *FREE_FLOAT_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_free_float_turnover_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_free_float_turnover_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_fixed51_plan_rejects_search_or_activation():
    from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='free_float_predecessor'))
    plan = FreeFloatTurnoverPlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 51 and plan.parameters['source_select_budget'] == 1 and not plan.deployable
    for update in ({'window_sessions': 60}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'model_id': 'M12'}):
        with pytest.raises(ValueError):
            FreeFloatTurnoverPlanV1.model_validate({**values, **update})
