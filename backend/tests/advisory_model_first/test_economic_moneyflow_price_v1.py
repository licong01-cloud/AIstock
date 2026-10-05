import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_moneyflow_price_source_v1 import normalize_moneyflow_boundary_v1
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import AMOUNT_FIELDS, MONEYFLOW_FEATURES, MoneyflowPricePlanV1, moneyflow_fit_identity_v1, moneyflow_nodes_v1, moneyflow_price_set_v1, moneyflow_rows_v1, train_moneyflow_price_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def flow_fixture():
    days = pd.bdate_range('2025-01-02', periods=8)
    candidates = pd.DataFrame([{KEY[0]: days[i], KEY[1]: days[i+1], KEY[2]: '000001.SZ'} for i in (0, 4, 5)])
    raw = pd.DataFrame([{'instrument': '000001.SZ', 'trade_date': day, **dict.fromkeys(AMOUNT_FIELDS, 1.),
        'buy_lg_amount': float(index+1), 'buy_elg_amount': 2.} for index, day in enumerate(days[:6])])
    return dict(candidates=candidates, amounts=normalize_moneyflow_boundary_v1(raw), calendar=days)


def moneyflow_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), moneyflow_features=list(MONEYFLOW_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        moneyflow_fit_identity_v1(recipe, original.models, original.support))


def test_plan_has_one_fixed_new_block_and_cumulative23_no_activation():
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='moneyflow_predecessor'))
    plan = MoneyflowPricePlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 23 and plan.parameters['physical_fit_budget'] == 4
    assert plan.parameters['information_features'] == list(MONEYFLOW_FEATURES) and not plan.deployable
    for update in ({'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'train_start': '2020-01-01'},
                   {'predecessor_manifest_ref': {**values['predecessor_manifest_ref'], 'artifact_uri': 'F:/elsewhere/prior/evaluated/manifest.json'}},
                   {'budget_anchor_ref': {**values['budget_anchor_ref'], 'artifact_uri': 'C:/unit/preregistered/manifest.json'}}):
        with pytest.raises(ValueError):
            MoneyflowPricePlanV1.model_validate({**values, **update})


def test_amount_weighted_five_sessions_no_future_labels_and_preserved_order():
    args = flow_fixture()
    result = moneyflow_rows_v1(**args)
    assert result.moneyflow_feature_status.tolist() == ['UNKNOWN_5D_WARMUP', 'AVAILABLE', 'AVAILABLE']
    assert result.loc[1, 'large_order_imbalance_D'] == pytest.approx(5/9)
    assert result.loc[1, 'large_order_turnover_share_D'] == pytest.approx(9/13)
    assert result.loc[1, 'large_order_imbalance_5D'] == pytest.approx(15/35)
    assert result[KEY].equals(args['candidates'][KEY])
    poisoned = args['amounts'].copy()
    poisoned['future_return'] = -999999.
    extra = poisoned.iloc[[-1]].copy()
    extra['trade_date'] = args['calendar'][-1]
    extra[list(AMOUNT_FIELDS)] = -999999.
    future = pd.concat([poisoned, extra], ignore_index=True)
    future.attrs = args['amounts'].attrs.copy()
    pd.testing.assert_frame_equal(result, moneyflow_rows_v1(**{**args, 'amounts': future}))


@pytest.mark.parametrize('defect', ['missing_day', 'null_amount', 'zero_denominator'])
def test_normal_missing_and_zero_denominator_keep_every_original_key(defect):
    args = flow_fixture()
    if defect == 'missing_day':
        args['amounts'] = args['amounts'].iloc[1:].copy()
    elif defect == 'null_amount':
        args['amounts'].loc[0, AMOUNT_FIELDS[0]] = np.nan
    else:
        args['amounts'].loc[:, AMOUNT_FIELDS] = 0.
    result = moneyflow_rows_v1(**args)
    assert len(result) == len(args['candidates']) and result.loc[1, list(MONEYFLOW_FEATURES)].isna().all()
    assert result.loc[1, 'moneyflow_feature_status'].startswith('UNKNOWN')


@pytest.mark.parametrize('defect', ['duplicate', 'foreign', 'wrong_T', 'units', 'negative'])
def test_identity_or_amount_contradictions_fail_closed(defect):
    args = flow_fixture()
    if defect == 'duplicate':
        args['amounts'] = pd.concat([args['amounts'], args['amounts'].iloc[[0]]])
    elif defect == 'foreign':
        args['amounts'].loc[0, 'instrument'] = 'FOREIGN'
    elif defect == 'wrong_T':
        args['candidates'].loc[1, KEY[1]] = args['calendar'][6]
    elif defect == 'units':
        args['amounts'].attrs.clear()
    else:
        args['amounts'].loc[0, AMOUNT_FIELDS[0]] = -1.
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        moneyflow_rows_v1(**args)


def test_empty_and_all_unknown_frames_are_real_unknown_not_isfinite_error():
    args = flow_fixture()
    empty = moneyflow_rows_v1(**{**args, 'candidates': args['candidates'].iloc[:0], 'amounts': args['amounts'].iloc[:0]})
    assert empty.empty and list(empty.columns) == [*KEY, *MONEYFLOW_FEATURES, 'moneyflow_feature_status', 'moneyflow_feature_visible_through']
    args['amounts'].loc[:, AMOUNT_FIELDS] = np.nan
    assert moneyflow_rows_v1(**args).moneyflow_feature_status.str.startswith('UNKNOWN').all()


def test_M6_common_mask_price_holes_and_independent_model_identity():
    fitted = moneyflow_fit_fixture()
    features = {**dict.fromkeys(D_FEATURES, .02), **dict(zip(MONEYFLOW_FEATURES, (.4, .6, .2), strict=True))}
    grid = moneyflow_price_set_v1(fitted=fitted, d_features=features, arm='candidate', reference_cny=10., legal_low_cny=9.7, legal_high_cny=10.3)
    assert grid.intervals_cny == ((9.7, 9.9), (10.1, 10.3))
    rows = pd.DataFrame([{**features, 'actual_gap_bps': 100.}])
    assert moneyflow_nodes_v1(fitted=fitted, rows=rows, arm='candidate').status.tolist() == ['ACCEPTABLE']
    rows[MONEYFLOW_FEATURES[0]] = np.nan
    for arm in ('candidate', 'matched'):
        assert moneyflow_nodes_v1(fitted=fitted, rows=rows, arm=arm).status.tolist() == ['UNKNOWN_INPUT_OR_SUPPORT']
    with pytest.raises(ValueError, match='identity'):
        moneyflow_nodes_v1(fitted=sector_fit_fixture(), rows=rows, arm='candidate')


def test_fixed_four_unit_fits_common_supervision_and_test_poison():
    from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
    rows, configuration = rows_fixture()
    rows[list(MONEYFLOW_FEATURES)] = [.4, .6, .2]
    rows['moneyflow_feature_status'] = 'AVAILABLE'
    rows.loc[0, list(MONEYFLOW_FEATURES)] = np.nan
    rows.loc[0, 'moneyflow_feature_status'] = 'UNKNOWN_MONEYFLOW_SOURCE'
    events = []
    fitted = train_moneyflow_price_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.recipe['moneyflow_features'] == list(MONEYFLOW_FEATURES)
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *MONEYFLOW_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    replay = train_moneyflow_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: None)
    assert fitted.model_sha256 == replay.model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_moneyflow_price_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))
