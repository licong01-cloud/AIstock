import json
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_valuation_context_v1 import CLOCK, VALUATION_FEATURES, STATUS, UNKNOWN, ValuationContextPlanV1, valuation_context_fit_identity_v1, valuation_context_rows_v1, train_valuation_context_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def valuation_fixture():
    calendar = pd.bdate_range('2025-01-02', periods=4)
    candidates = pd.DataFrame([{KEY[0]: calendar[index], KEY[1]: calendar[index+1], KEY[2]: '000001.SZ'} for index in (0, 1, 2)])
    basics = pd.DataFrame({'trade_date': calendar, 'instrument': '000001.SZ', 'pe_ttm': [20., -10., 0., 1.],
        'pb': [2., -4., 0., 1.], 'dv_ttm': [3., 0., 1., 1.]})
    return dict(candidates=candidates, basics=basics, calendar=calendar)


def valuation_fit_fixture():
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), valuation_features=list(VALUATION_FEATURES))
    return SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        valuation_context_fit_identity_v1(recipe, original.models, original.support))


def test_signed_inverse_units_zero_denominators_and_no_warmup():
    args = valuation_fixture()
    result = valuation_context_rows_v1(**args)
    assert result[KEY].equals(args['candidates'][KEY])
    assert result[STATUS].tolist() == ['AVAILABLE', 'AVAILABLE', 'UNKNOWN_ZERO_VALUATION_DENOMINATOR']
    np.testing.assert_allclose(result.loc[:1, list(VALUATION_FEATURES)].astype(float), [[.05, .5, .03], [-.1, -.25, 0.]])
    assert set(json.loads(result.loc[2, UNKNOWN])) == set(VALUATION_FEATURES[:2])
    assert result.loc[2, VALUATION_FEATURES[2]] == .01 and result[CLOCK].equals(result[KEY[0]])
    args['basics'].pe_ttm *= 2
    assert valuation_context_rows_v1(**args).loc[0, VALUATION_FEATURES[0]] == .025


@pytest.mark.parametrize('missing', ['pe_ttm', 'pb', 'dv_ttm', 'row'])
def test_field_local_unknown_keeps_candidates_and_other_known_fields(missing):
    args = valuation_fixture()
    if missing == 'row':
        args['basics'] = args['basics'].drop(index=0)
    else:
        args['basics'].loc[0, missing] = np.nan
    result = valuation_context_rows_v1(**args)
    unknown = json.loads(result.loc[0, UNKNOWN])
    expected = set(VALUATION_FEATURES) if missing == 'row' else {VALUATION_FEATURES[['pe_ttm', 'pb', 'dv_ttm'].index(missing)]}
    assert result[KEY].equals(args['candidates'][KEY]) and set(unknown) == expected
    for name in set(VALUATION_FEATURES)-expected:
        assert pd.notna(result.loc[0, name])


@pytest.mark.parametrize(('field', 'bad'), [('pe_ttm', True), ('pe_ttm', np.inf), ('pb', '2'),
    ('pe_ttm', 10**1000), ('pb', Decimal('NaN')), ('pe_ttm', Decimal('1e-1000')),
    ('pe_ttm', 1e-320), ('dv_ttm', Decimal('1e-1000')), ('dv_ttm', 5e-324), ('dv_ttm', -1.)])
def test_consumed_bad_values_underflow_and_inverse_overflow_raise(field, bad):
    args = valuation_fixture()
    args['basics'][field] = args['basics'][field].astype(object)
    args['basics'].loc[0, field] = bad
    with pytest.raises(ValueError):
        valuation_context_rows_v1(**args)


def test_projection_before_values_and_actual_duplicate_wrong_T_empty():
    args = valuation_fixture()
    args['candidates'] = args['candidates'].iloc[:1].reset_index(drop=True)
    expected = valuation_context_rows_v1(**args)
    args['basics'][['pe_ttm', 'pb', 'dv_ttm']] = args['basics'][['pe_ttm', 'pb', 'dv_ttm']].astype(object)
    args['basics'].loc[1:, ['pe_ttm', 'pb', 'dv_ttm']] = 'unconsumed future poison'
    args['basics'] = pd.concat([args['basics'], args['basics'].iloc[1:2]])
    pd.testing.assert_frame_equal(expected, valuation_context_rows_v1(**args))
    with pytest.raises(Exception, match='duplicate'):
        valuation_context_rows_v1(**{**args, 'basics': pd.concat([args['basics'], args['basics'].iloc[:1]])})
    wrong = args['candidates'].copy()
    wrong.loc[0, KEY[1]] = args['calendar'][2]
    with pytest.raises(ValueError, match='next session'):
        valuation_context_rows_v1(**{**args, 'candidates': wrong})
    empty = valuation_context_rows_v1(**{**args, 'candidates': args['candidates'].iloc[:0]})
    assert empty.empty and empty.columns.tolist() == [*KEY, *VALUATION_FEATURES, STATUS, CLOCK, UNKNOWN]


def test_common_mature_13_16_training_test_poison_and_maturity_failure():
    rows, configuration = rows_fixture()
    rows[list(VALUATION_FEATURES)] = [.05, .5, .03]
    rows[STATUS] = 'AVAILABLE'
    rows.loc[0, VALUATION_FEATURES[0]] = np.nan
    rows.loc[0, STATUS] = 'UNKNOWN_VALUATION_SOURCE'
    events = []
    fitted = train_valuation_context_v1(rows=rows, configuration=configuration, before_fit=events.append)
    assert len(events) == 4 and fitted.models['matched_mean']['features'] == 13 and fitted.models['candidate_mean']['features'] == 16
    assert fitted.diagnostics['train_rows'] == int((rows.split.eq('train') & rows.training_eligible).sum())-1
    rows.loc[rows.split.eq('test'), [*D_FEATURES, *VALUATION_FEATURES, 'gross_value_ratio', 'path_min_value_ratio', 'actual_gap_bps']] = 99999.
    assert fitted.model_sha256 == train_valuation_context_v1(rows=rows, configuration=configuration, before_fit=lambda _: None).model_sha256
    rows.loc[rows.split.eq('train'), 'label_information_end'] = configuration.test_end
    with pytest.raises(ValueError, match='mature common'):
        train_valuation_context_v1(rows=rows, configuration=configuration, before_fit=lambda _: pytest.fail('must not fit'))


def test_fixed55_plan_rejects_search_or_activation():
    from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
    values = plan_fixture().model_dump(exclude={'schema_version', 'campaign_id', 'model_id'})
    values.update(budget_anchor_ref=dict(artifact_uri='F:/unit/campaign/original/preregistered/manifest.json', sha256='d'*64, size_bytes=1, role='price_campaign_budget_anchor'),
        predecessor_manifest_ref=dict(artifact_uri='F:/unit/campaign/prior/evaluated/manifest.json', sha256='e'*64, size_bytes=1, role='valuation_predecessor'))
    plan = ValuationContextPlanV1(**values)
    assert plan.parameters['campaign_fit_budget'] == 55 and plan.parameters['source_select_budget'] == 1 and not plan.deployable
    for update in ({'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'model_id': 'M13'}):
        with pytest.raises(ValueError):
            ValuationContextPlanV1.model_validate({**values, **update})
