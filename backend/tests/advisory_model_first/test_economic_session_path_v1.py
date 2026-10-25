import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.services.advisory_model_first.economic_session_path_v1 import CLOCK, SESSION_PATH_FEATURES, STATUS, UNKNOWN, SessionPathPlanV1, session_path_fit_identity_v1, session_path_rows_v1, train_session_path_v1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def session_path_fixture():
    calendar = pd.bdate_range('2025-01-02', periods=23)
    candidates = pd.DataFrame([{KEY[0]: calendar[index], KEY[1]: calendar[index+1], KEY[2]: '000001.SZ'} for index in (0, 19, 20)])
    prices = pd.DataFrame({'trade_date': calendar, 'instrument': '000001.SZ', 'raw_close_cny': 10., 'raw_open_cny': 11., 'adj_factor': 1.})
    return dict(candidates=candidates, prices=prices, calendar=calendar)


def session_path_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), session_path_features=list(SESSION_PATH_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        session_path_fit_identity_v1(recipe, original.models, original.support))


def test_manual_decomposition_same_Close_different_Open_and_flat_known_zero():
    args = session_path_fixture()
    result = session_path_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result[STATUS].tolist() == ['UNKNOWN_20D_HISTORY', 'AVAILABLE', 'AVAILABLE']
    np.testing.assert_allclose(result.loc[1, list(SESSION_PATH_FEATURES)].astype(float), [np.log(1.1), -np.log(1.1), np.log(1.1)])
    args['prices'].raw_open_cny = 10.
    np.testing.assert_allclose(session_path_rows_v1(**args).loc[1, list(SESSION_PATH_FEATURES)].astype(float), [0., 0., 0.], atol=1e-15)


def test_adjustment_and_price_units_cancel_and_o_plus_h_is_Close_log_change():
    args = session_path_fixture()
    args['prices'].adj_factor = np.linspace(1., 2., 23)
    expected = session_path_rows_v1(**args)
    assert sum(expected.loc[1, list(SESSION_PATH_FEATURES[:2])].astype(float))*19 == pytest.approx(np.log(args['prices'].adj_factor.iloc[19]))
    args['prices'].adj_factor *= 1e250
    args['prices'][['raw_open_cny', 'raw_close_cny']] *= 1e-240
    result = session_path_rows_v1(**args)
    np.testing.assert_allclose(result.loc[1:, list(SESSION_PATH_FEATURES)], expected.loc[1:, list(SESSION_PATH_FEATURES)], atol=1e-12)


@pytest.mark.parametrize(('missing', 'unknown'), [('first_close', (0, 2)), ('D_close', (1,)), ('AF', (0, 2)), ('Open', (0, 1, 2)), ('row', (0, 1, 2))])
def test_true_field_dependencies_unknown_do_not_compress_or_drop(missing, unknown):
    args = session_path_fixture()
    if missing == 'row':
        args['prices'] = args['prices'].drop(index=5)
    else:
        column, index = {'first_close': ('raw_close_cny', 0), 'D_close': ('raw_close_cny', 19), 'AF': ('adj_factor', 5), 'Open': ('raw_open_cny', 5)}[missing]
        args['prices'].loc[index, column] = np.nan
    result = session_path_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result.loc[1, STATUS].startswith('UNKNOWN')
    assert set(json.loads(result.loc[1, UNKNOWN])) == {SESSION_PATH_FEATURES[index] for index in unknown}
    for index in set(range(3))-set(unknown):
        assert pd.notna(result.loc[1, SESSION_PATH_FEATURES[index]])


@pytest.mark.parametrize('bad', [True, 0., np.inf, 'bad', 10**1000])
def test_consumed_known_bad_positive_price_is_not_unknown(bad):
    args = session_path_fixture()
    args['prices']['raw_open_cny'] = args['prices'].raw_open_cny.astype(object)
    args['prices'].loc[5, 'raw_open_cny'] = bad
    with pytest.raises(ValueError):
        session_path_rows_v1(**args)


def test_first_Open_future_unconsumed_poison_duplicate_and_empty_schema():
    args = session_path_fixture()
    args['candidates'] = args['candidates'].iloc[1:2].reset_index(drop=True)
    expected = session_path_rows_v1(**args)
    args['prices']['raw_open_cny'] = args['prices'].raw_open_cny.astype(object)
    args['prices'].loc[0, 'raw_open_cny'] = 'unconsumed first Open'
    args['prices'].loc[20:, ['raw_close_cny', 'adj_factor']] = -999.
    pd.testing.assert_frame_equal(expected, session_path_rows_v1(**args))
    duplicate = pd.concat([args['prices'], args['prices'].iloc[:1]], ignore_index=True)
    with pytest.raises(Exception, match='duplicate'):
        session_path_rows_v1(**{**args, 'prices': duplicate})
    wrong = args['candidates'].copy()
    wrong.loc[0, KEY[1]] = args['calendar'][21]
    with pytest.raises(ValueError, match='next session'):
        session_path_rows_v1(**{**args, 'candidates': wrong})
    args['candidates'] = args['candidates'].iloc[:0]
    empty = session_path_rows_v1(**args)
    assert empty.empty and empty.columns.tolist() == [*KEY, *SESSION_PATH_FEATURES, STATUS, CLOCK, UNKNOWN]


def test_common_mature_13_16_training_test_poison_and_maturity_failure():
    rows, configuration = rows_fixture()
    rows[list(SESSION_PATH_FEATURES)] = [.001, -.001, .01]
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, SESSION_PATH_FEATURES[0]] = np.nan
    rows.loc[0, STATUS] = 'UNKNOWN_SESSION_PATH_SOURCE'
    events = []
    fitted = train_session_path_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *SESSION_PATH_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_session_path_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_session_path_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_fixed47_plan_rejects_policy_search_or_activation():
    from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='session_path_predecessor'))
    plan = SessionPathPlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 47 and plan.parameters['source_select_budget'] == 0 and not plan.deployable
    for update in ({'window_sessions': 60}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'model_id': 'M11'}):
        with pytest.raises(ValueError):
            SessionPathPlanV1.model_validate({**values, **update})
