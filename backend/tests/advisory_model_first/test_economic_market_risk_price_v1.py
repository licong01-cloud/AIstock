from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_market_risk_price_v1 import MARKET_RISK_FEATURES, MarketRiskPricePlanV1, market_risk_fit_identity_v1, market_risk_nodes_v1, market_risk_price_set_v1, market_risk_rows_v1, train_market_risk_price_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_price_path_value_v1 import price_path_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MoneyflowPricePlanV1
from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture


def market_risk_fixture():
    args = price_path_fixture()
    args['benchmark'] = args['prices'].loc[:, ['trade_date']].assign(instrument='000300.SH', close=args['prices'].raw_close_cny*10)
    return args


def market_risk_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), market_risk_features=list(MARKET_RISK_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        market_risk_fit_identity_v1(recipe, original.models, original.support))


def test_plan_fixed_market_block31_and_legacy_parameters_unchanged():
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='market_risk_predecessor'))
    plan = MarketRiskPricePlanV1(**values)
    assert plan.parameters['physical_fit_budget'] == 4 and plan.parameters['campaign_fit_budget'] == 31
    assert plan.parameters['information_features'] == list(MARKET_RISK_FEATURES) and not plan.deployable
    legacy = MoneyflowPricePlanV1(**{**values, 'predecessor_manifest_ref': {**values['predecessor_manifest_ref'], 'role': 'moneyflow_predecessor'}})
    assert legacy.parameters['campaign_fit_budget'] == 23
    for update in ({'deployable': True}, {'window_sessions': 60}, {'decision_use': 'ACTIVATION_EVIDENCE'},
                   {'budget_anchor_ref': {**values['budget_anchor_ref'], 'artifact_uri': 'C:/unit/preregistered/manifest.json'}}):
        with pytest.raises(ValueError):
            MarketRiskPricePlanV1.model_validate({**values, **update})


def test_manual_time_volatility_drawdown_beta_and_scale_future_poison():
    args = market_risk_fixture()
    result = market_risk_rows_v1(**args)
    assert result.market_risk_feature_status.tolist() == ['UNKNOWN_20D_WARMUP', 'AVAILABLE', 'AVAILABLE']
    changes = np.r_[np.zeros(17), np.log(1.2), np.log(11/12)]
    assert result.loc[1, 'market_volatility19_bps'] == pytest.approx(changes.std(ddof=0)*10000)
    assert result.loc[1, 'market_drawdown20'] == pytest.approx(11/12-1)
    assert result.loc[1, 'stock_market_beta19'] == pytest.approx(1.)
    assert result[KEY].equals(args['candidates'][KEY])
    scaled = {**args, 'prices': args['prices'].copy(), 'benchmark': args['benchmark'].copy()}
    scaled['prices'].raw_close_cny *= 3.
    scaled['benchmark'].close *= 7.
    np.testing.assert_allclose(result.loc[:, MARKET_RISK_FEATURES], market_risk_rows_v1(**scaled).loc[:, MARKET_RISK_FEATURES], atol=1e-10)
    poisoned = args['benchmark'].iloc[[-1]].copy()
    poisoned.trade_date = args['calendar'][-1]
    poisoned.close = -999999.
    pd.testing.assert_frame_equal(result, market_risk_rows_v1(**{**args, 'benchmark': pd.concat([args['benchmark'], poisoned])}))
    earlier = args['candidates'].iloc[[1]].copy()
    later = args['benchmark'].copy()
    later.loc[20, 'close'] = -999999.
    pd.testing.assert_frame_equal(market_risk_rows_v1(**{**args, 'candidates': earlier}),
        market_risk_rows_v1(**{**args, 'candidates': earlier, 'benchmark': later}))


@pytest.mark.parametrize('defect', ['missing_index', 'missing_stock', 'nullable_factor', 'flat_benchmark'])
def test_normal_missing_and_flat_benchmark_keep_original_unknown(defect):
    args = market_risk_fixture()
    if defect == 'missing_index':
        args['benchmark'] = args['benchmark'].iloc[1:].copy()
    elif defect == 'missing_stock':
        args['prices'] = args['prices'].iloc[1:].copy()
    elif defect == 'nullable_factor':
        args['prices']['adj_factor'] = args['prices'].adj_factor.astype(object)
        args['prices'].loc[0, 'adj_factor'] = pd.NA
    else:
        args['benchmark'].close = 100.
    result = market_risk_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY]) and result.loc[1, list(MARKET_RISK_FEATURES)].isna().all()
    assert result.loc[1, 'market_risk_feature_status'] == ('UNKNOWN_FLAT_BENCHMARK' if defect == 'flat_benchmark' else 'UNKNOWN_MARKET_RISK_SOURCE')


@pytest.mark.parametrize('defect', ['foreign_index', 'duplicate', 'wrong_T', 'zero_index', 'bool_factor', 'decimal_nan'])
def test_identity_and_known_numeric_contradictions_fail_closed(defect):
    args = market_risk_fixture()
    if defect == 'foreign_index':
        args['benchmark'].loc[0, 'instrument'] = '000905.SH'
    elif defect == 'duplicate':
        args['prices'] = pd.concat([args['prices'], args['prices'].iloc[[0]]])
    elif defect == 'wrong_T':
        args['candidates'].loc[1, KEY[1]] = args['calendar'][21]
    elif defect == 'zero_index':
        args['benchmark'].loc[0, 'close'] = 0.
    elif defect == 'decimal_nan':
        args['benchmark']['close'] = args['benchmark'].close.astype(object)
        args['benchmark'].loc[0, 'close'] = Decimal('NaN')
    else:
        args['prices']['adj_factor'] = args['prices'].adj_factor.astype(object)
        args['prices'].loc[0, 'adj_factor'] = True
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        market_risk_rows_v1(**args)


def test_flat_stock_is_valid_beta_zero_empty_and_price_common_mask():
    args = market_risk_fixture()
    args['prices'].raw_close_cny = 10.
    result = market_risk_rows_v1(**args)
    assert result.loc[1, 'market_risk_feature_status'] == 'AVAILABLE' and result.loc[1, 'stock_market_beta19'] == 0.
    assert market_risk_rows_v1(**{**args, 'candidates': args['candidates'].iloc[:0]}).empty
    fitted = market_risk_fit_fixture()
    features = {**dict.fromkeys(D_FEATURES, .02), **dict(zip(MARKET_RISK_FEATURES, (150., -.1, 1.), strict=True))}
    grid = market_risk_price_set_v1(fitted=fitted, d_features=features, arm='candidate', reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3)
    assert grid.intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    query = pd.DataFrame([{**features, 'actual_gap_bps': 100.}])
    assert market_risk_nodes_v1(fitted=fitted, rows=query, arm='candidate').status.tolist() == ['ACCEPTABLE']
    query[MARKET_RISK_FEATURES[0]] = np.nan
    for arm in ('matched', 'candidate'):
        assert market_risk_nodes_v1(fitted=fitted, rows=query, arm=arm).status.tolist() == ['UNKNOWN_INPUT_OR_SUPPORT']
    with pytest.raises(ValueError, match='identity'):
        market_risk_nodes_v1(fitted=sector_fit_fixture(), rows=query, arm='candidate')


def test_identical_benchmark_returns_have_zero_variance_not_roundoff_beta():
    args = market_risk_fixture()
    args['benchmark'].close = 10.*2.**np.arange(len(args['benchmark']))
    result = market_risk_rows_v1(**args)
    assert result.loc[1, 'market_risk_feature_status'] == 'UNKNOWN_FLAT_BENCHMARK'


def test_four_unit_fits_common_supervision_and_no_test_consumption():
    rows, configuration = rows_fixture()
    rows[list(MARKET_RISK_FEATURES)] = [150., -.1, 1.]
    rows['market_risk_feature_status'] = 'AVAILABLE'
    rows.loc[0, list(MARKET_RISK_FEATURES)] = np.nan
    rows.loc[0, 'market_risk_feature_status'] = 'UNKNOWN_MARKET_RISK_SOURCE'
    events = []
    fitted = train_market_risk_price_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *MARKET_RISK_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_market_risk_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_market_risk_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))
