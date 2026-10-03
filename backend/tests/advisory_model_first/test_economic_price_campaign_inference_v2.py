import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_price_campaign_inference_v2 import campaign_nodes_v2, campaign_price_set_v2
from backend.services.advisory_model_first.economic_price_campaign_models_v2 import PriceCampaignFitV2, fit_identity_v2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1


def fit_fixture():
    recipe = dict(features=list(D_FEATURES)+['gap_bps_div_100'], means=[0.]*13, scales=[1.]*13)
    models = {arm+'_'+head: dict(kind='linear', coef=[0.]*18, intercept=1.04 if head == 'mean' else .99)
        for arm in ('matched', 'candidate') for head in ('mean', 'path')}
    # Candidate uses trees in real fitting; this linear fixture exercises pure contract only.
    for head in ('mean', 'path'):
        models['candidate_'+head]['coef'] = [0.]*13
    support = ValueAnchorGapSupportV1(((-100., -50.), (50., 100.)))
    return PriceCampaignFitV2('M2', recipe, models, support, {}, fit_identity_v2('M2', recipe, models, support))


def test_tick_holes_empty_and_batch_single_nodes_identical():
    fitted = fit_fixture()
    features = dict.fromkeys(D_FEATURES, 0.)
    advice = campaign_price_set_v2(fitted=fitted, d_features=features, arm='candidate', reference_cny=10., legal_low_cny=9.9, legal_high_cny=10.1)
    assert advice.intervals_cny == ((9.9, 9.95), (10.05, 10.1)) and not advice.deployable
    rows = pd.DataFrame([{**features, 'actual_gap_bps': gap} for gap in (-100., 0., 100.)])
    batch = campaign_nodes_v2(fitted=fitted, rows=rows, arm='candidate')
    for index in rows.index:
        pd.testing.assert_frame_equal(batch.loc[[index]], campaign_nodes_v2(fitted=fitted, rows=rows.loc[[index]], arm='candidate'))
    assert batch.status.tolist() == ['ACCEPTABLE', 'UNKNOWN_INPUT_OR_SUPPORT', 'ACCEPTABLE']
    assert not campaign_price_set_v2(fitted=fitted, d_features=features, arm='candidate', reference_cny=10., legal_low_cny=10., legal_high_cny=10.).intervals_cny


def test_unknown_rows_retained_but_boolean_or_mutated_model_fails():
    fitted = fit_fixture()
    rows = pd.DataFrame([{**dict.fromkeys(D_FEATURES, 0.), 'actual_gap_bps': 60.}])
    rows[D_FEATURES[0]] = np.nan
    assert len(campaign_nodes_v2(fitted=fitted, rows=rows, arm='candidate')) == 1
    rows[D_FEATURES[0]] = True
    with pytest.raises(ValueError, match='boolean'):
        campaign_nodes_v2(fitted=fitted, rows=rows, arm='candidate')
    fitted.models['candidate_mean']['intercept'] = 2.
    with pytest.raises(ValueError, match='identity'):
        campaign_nodes_v2(fitted=fitted, rows=rows, arm='candidate')
