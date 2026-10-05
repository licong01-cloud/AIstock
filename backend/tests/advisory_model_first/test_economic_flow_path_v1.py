import json
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_flow_path_v1 import AMOUNT_FIELDS, CLOCK, FLOW_PATH_FEATURES, STATUS, UNKNOWN, FlowPathPlanV1, flow_path_fit_identity_v1, flow_path_rows_v1, train_flow_path_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def flow_path_fixture():
    calendar = pd.bdate_range('2025-01-02', periods=7)
    candidates = pd.DataFrame([{KEY[0]: calendar[i], KEY[1]: calendar[i+1], KEY[2]: '000001.SZ'} for i in (4, 5)])
    imbalance = np.array([-.2, .2, -.2, .2, .2, -.1])
    amounts = pd.DataFrame(dict(trade_date=calendar[:6], instrument='000001.SZ',
        buy_lg_amount=1+imbalance, sell_lg_amount=1-imbalance, buy_elg_amount=0., sell_elg_amount=0.))
    amounts.attrs.update(moneyflow_unit_contract='tushare_moneyflow_shares_yuan_v1', moneyflow_amount_unit='cny')
    return dict(candidates=candidates, amounts=amounts, calendar=calendar)


def flow_path_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), flow_path_features=list(FLOW_PATH_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        flow_path_fit_identity_v1(recipe, original.models, original.support))


def test_original_five_sessions_order_information_old_summaries_equal():
    from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MONEYFLOW_FEATURES, moneyflow_rows_v1
    args = flow_path_fixture()
    result = flow_path_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result[STATUS].eq('AVAILABLE').all()
    np.testing.assert_allclose(result[list(FLOW_PATH_FEATURES)].astype(float), [[.6, .4, .3], [.6, 0., .275]])
    for size in ('sm', 'md'):
        args['amounts'][f'buy_{size}_amount'] = args['amounts'][f'sell_{size}_amount'] = 1.
    old = moneyflow_rows_v1(**args)
    alternate = np.array([-.2, -.2, .2, .2, .2, -.1])
    args['amounts']['buy_lg_amount'], args['amounts']['sell_lg_amount'] = 1+alternate, 1-alternate
    pd.testing.assert_series_equal(old.loc[0, list(MONEYFLOW_FEATURES)], moneyflow_rows_v1(**args).loc[0, list(MONEYFLOW_FEATURES)])
    np.testing.assert_allclose(flow_path_rows_v1(**args).loc[0, list(FLOW_PATH_FEATURES)].astype(float), [.6, .6, .1])


@pytest.mark.parametrize('missing', [None, np.nan, Decimal('NaN')])
def test_normal_missing_keeps_original_stock_date_and_UNKNOWN(missing):
    args = flow_path_fixture()
    args['amounts']['buy_lg_amount'] = args['amounts'].buy_lg_amount.astype(object)
    args['amounts'].loc[0, 'buy_lg_amount'] = missing
    result = flow_path_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result[STATUS].tolist() == ['UNKNOWN_FLOW_SOURCE', 'AVAILABLE']
    assert set(json.loads(result.loc[0, UNKNOWN])) == set(FLOW_PATH_FEATURES)
    assert result.loc[0, list(FLOW_PATH_FEATURES)].isna().all()


def test_zero_flow_vs_zero_imbalance_warmup_constant_positive_and_overflow():
    args = flow_path_fixture()
    args['amounts'].loc[0, list(AMOUNT_FIELDS)] = 0.
    assert flow_path_rows_v1(**args)[STATUS].tolist() == ['UNKNOWN_ZERO_FLOW_DENOMINATOR', 'AVAILABLE']
    args['amounts'].loc[:, list(AMOUNT_FIELDS)] = [1e308, 1e308, 1e308, 1e308]
    np.testing.assert_allclose(flow_path_rows_v1(**args)[list(FLOW_PATH_FEATURES)].astype(float), 0.)
    args['amounts'].loc[:, list(AMOUNT_FIELDS)] = [1e308, 0., 1e308, 0.]
    np.testing.assert_allclose(flow_path_rows_v1(**args)[list(FLOW_PATH_FEATURES)].astype(float), [[1., 1., 0.]]*2)
    args['amounts'].loc[4, 'sell_lg_amount'] = args['amounts'].loc[4, 'buy_lg_amount']
    args['amounts'].loc[4, 'sell_elg_amount'] = args['amounts'].loc[4, 'buy_elg_amount']
    assert flow_path_rows_v1(**args).loc[0, FLOW_PATH_FEATURES[1]] == 0.
    args['candidates'].loc[0, [KEY[0], KEY[1]]] = args['calendar'][2:4]
    assert flow_path_rows_v1(**args).loc[0, STATUS] == 'UNKNOWN_5D_HISTORY'


@pytest.mark.parametrize('bad', [True, 'bad', np.inf, -.1, Decimal('Infinity'), Decimal('sNaN'), Decimal('1e-400')])
def test_consumed_bad_amount_not_silent_unknown(bad):
    args = flow_path_fixture()
    args['amounts']['buy_lg_amount'] = args['amounts'].buy_lg_amount.astype(object)
    args['amounts'].loc[0, 'buy_lg_amount'] = bad
    with pytest.raises(ValueError):
        flow_path_rows_v1(**args)


def test_only_original_requested_source_no_future_foreign_labels_or_small_sides():
    args = flow_path_fixture()
    expected = flow_path_rows_v1(**args)
    attrs = dict(args['amounts'].attrs)
    args['amounts'] = pd.concat([args['amounts'], args['amounts'].iloc[:1].assign(trade_date=args['calendar'][-1], buy_lg_amount=-999.),
        args['amounts'].iloc[:1].assign(instrument='000009.SZ', buy_lg_amount=-999.)], ignore_index=True)
    args['amounts'].attrs.update(attrs)
    args['amounts']['buy_sm_amount'] = -999.
    args['amounts']['gross_value_ratio'] = -999.
    args['amounts']['training_eligible'] = False
    pd.testing.assert_frame_equal(expected, flow_path_rows_v1(**args))
    args['amounts'] = args['amounts'].drop(index=0)
    assert flow_path_rows_v1(**args).loc[0, STATUS] == 'UNKNOWN_FLOW_SOURCE'


def test_duplicate_wrong_T_units_and_empty_structured_schema():
    args = flow_path_fixture()
    with pytest.raises(AdvisoryModelFirstError, match='duplicate'):
        flow_path_rows_v1(**{**args, 'amounts': pd.concat([args['amounts'], args['amounts'].iloc[:1]], ignore_index=True)})
    wrong = args['candidates'].copy()
    wrong.loc[0, KEY[1]] = args['calendar'][-1]
    with pytest.raises(ValueError, match='next session'):
        flow_path_rows_v1(**{**args, 'candidates': wrong})
    result = flow_path_rows_v1(**{**args, 'candidates': args['candidates'].iloc[:0]})
    assert result.empty and result.columns.tolist() == [*KEY, *FLOW_PATH_FEATURES, STATUS, CLOCK, UNKNOWN]
    args['amounts'].attrs['moneyflow_amount_unit'] = '10k_cny'
    with pytest.raises(ValueError, match='once-normalized'):
        flow_path_rows_v1(**args)


def test_common_mature_13_16_train_only_support_unchanged():
    rows, configuration = rows_fixture()
    rows[list(FLOW_PATH_FEATURES)], rows[STATUS] = [.6, .4, .3], 'AVAILABLE'
    rows.loc[0, FLOW_PATH_FEATURES[0]], rows.loc[0, STATUS] = np.nan, 'UNKNOWN_FLOW_SOURCE'
    events = []
    fitted = train_flow_path_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1 and fitted.support.contains(20.)
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *FLOW_PATH_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_flow_path_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_flow_path_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_fixed67_plan_original_source_no_search_activation():
    from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='flow_path_predecessor'),
        flow_source_manifest_ref=dict(artifact_uri='F:/unit/campaign/flow/prepared/manifest.json', sha256='f'*64, size_bytes=1, role='flow_path_source'))
    plan = FlowPathPlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 67 and plan.parameters['source_select_budget'] == 0 and plan.parameters['window_sessions'] == 5
    assert plan.experiment_id.startswith('advflowpath_') and not plan.deployable
    for update in ({'window_sessions': 10}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'model_id': 'M16'}):
        with pytest.raises(ValueError):
            FlowPathPlanV1.model_validate({**values, **update})
