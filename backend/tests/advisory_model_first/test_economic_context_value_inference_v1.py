from dataclasses import replace

import numpy as np
import pytest

from backend.services.advisory_model_first.economic_context_value_inference_v1 import context_value_nodes_v1, context_value_price_set_v1
from backend.services.advisory_model_first.economic_context_value_training_v1 import context_fit_identity_v1, train_context_value_v1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.tests.advisory_model_first.test_economic_context_value_training_v1 import captured_fit, rows_fixture


def fitted_fixture(monkeypatch):
    args,rows=rows_fixture()
    captured_fit(monkeypatch)
    return rows,train_context_value_v1(rows=rows,configuration=args['configuration'],before_fit=lambda *a:None)


def test_unknown_extreme_gap_and_single_batch_are_not_default_buys(monkeypatch):
    rows,fit=fitted_fixture(monkeypatch)
    query=rows.iloc[:3].copy()
    query.loc[0,'classification_l2_code']=None
    query.loc[1,'actual_gap_bps']=-800.
    query.loc[2,'gross_value_ratio']=999.
    nodes=context_value_nodes_v1(fitted=fit,rows=query,arm='candidate')
    assert nodes.status.tolist()==['UNKNOWN_CONTEXT_OR_SUPPORT','UNKNOWN_CONTEXT_OR_SUPPORT','ACCEPTABLE']
    for index in query.index:
        single=context_value_nodes_v1(fitted=fit,rows=query.loc[[index]],arm='candidate')
        assert single.status.iloc[0]==nodes.status.loc[index]
    query.loc[2,D_FEATURES[0]]=np.nan
    assert context_value_nodes_v1(fitted=fit,rows=query,arm='candidate').status.iloc[2]=='UNKNOWN_CONTEXT_OR_SUPPORT'


def test_price_set_is_cost_once_tick_legal_and_no_future_open_or_labels(monkeypatch):
    rows,fit=fitted_fixture(monkeypatch)
    features=rows.iloc[0].loc[list(D_FEATURES)].to_dict()
    advice=context_value_price_set_v1(fitted=fit,d_features=features,category='110100',arm='candidate',reference_cny=10.,legal_low_cny=10.01,legal_high_cny=10.03)
    assert advice.intervals_cny==((10.02,10.02),) and not advice.deployable
    point=context_value_nodes_v1(fitted=fit,rows=rows.iloc[:1],arm='candidate').iloc[0]
    assert point.expected_net_bps==pytest.approx((1.04*(1-.000595)/(1.002*(1+.000095))-1)*10000)
    assert context_value_price_set_v1(fitted=fit,d_features=features,category=None,arm='candidate',reference_cny=10.,legal_low_cny=10.,legal_high_cny=11.).intervals_cny==()


def test_illegal_model_does_not_fall_back_as_unknown(monkeypatch):
    rows,fit=fitted_fixture(monkeypatch)
    fit.models['candidate']['mean']['intercept']=-1.
    bad=replace(fit,model_sha256=context_fit_identity_v1(fit.recipe,fit.models,fit.support))
    with pytest.raises(ValueError):
        context_value_nodes_v1(fitted=bad,rows=rows.iloc[:1],arm='candidate')
    with pytest.raises(ValueError):
        context_value_nodes_v1(fitted=fit,rows=rows.iloc[:0],arm='legacy')
