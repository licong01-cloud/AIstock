from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_economic_entry_information_training import arguments
from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
from backend.services.advisory_model_first.economic_entry_information_training import EconomicInformationArmV4
from backend.services.advisory_model_first.economic_entry_information_inference import predict_information_entry_nodes_v4
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


class Head:
    """Numerical contract double, not fitted/economic evidence."""
    def __init__(self,risk=False,bad=False):
        self.risk,self.bad=risk,bad
    def predict(self,frame,**kwargs):
        return np.full(len(frame),np.nan if self.bad else 100.) if self.risk else np.where(frame.query_gap_bps.eq(10),-5.,5.)


def fitted(study,arm):
    request=arguments(study)[0]["request"]
    names=ECONOMIC_FEATURE_NAMES if arm=="MATCHED_NINE" else request.feature_names
    return EconomicInformationArmV4(request,arm,names,Head(),Head(risk=True),{name:(-100.,100.) for name in names},
        {0:{"observation_count":50,"decision_day_count":10,"observed_min_gap_bps":0.,"observed_max_gap_bps":50.}}, {})


@pytest.mark.parametrize("arm",["MATCHED_NINE","INFORMATION_THIRTEEN"])
def test_both_arms_share_missing_information_gate_and_batch_node_semantics(study,arm):
    model=fitted(study,arm)
    matrix=pd.DataFrame(.5,index=range(4),columns=model.request.feature_names)
    matrix["query_gap_bps"]=[10.,20.,200.,20.]
    matrix.loc[3,"ret_10"]=np.nan
    output=predict_information_entry_nodes_v4(fitted=model,matrix=matrix)
    assert output.model_action.tolist()==["SKIP","TAKE","UNAVAILABLE","UNAVAILABLE"]
    assert output.expected_net_return_bps.iloc[2:].isna().all()
    for index in range(len(matrix)):
        single=predict_information_entry_nodes_v4(fitted=model,matrix=matrix.iloc[[index]])
        pd.testing.assert_frame_equal(single,output.iloc[[index]])


@pytest.mark.parametrize("defect",["bool","missing","family","bounds","support","output"])
def test_malformed_schema_support_or_output_is_not_silent_unknown(study,defect):
    model=fitted(study,"INFORMATION_THIRTEEN")
    matrix=pd.DataFrame(.5,index=[0],columns=model.request.feature_names)
    if defect=="bool":
        matrix["ret_10"]=True
    elif defect=="missing":
        matrix=matrix.drop(columns="ret_10")
    elif defect=="family":
        model=replace(model,feature_names=ECONOMIC_FEATURE_NAMES)
    elif defect=="bounds":
        model=replace(model,feature_bounds={**model.feature_bounds,"ret_10":(1.,-1.)})
    elif defect=="support":
        model=replace(model,price_support={0:{**model.price_support[0],"observation_count":True}})
    else:
        model=replace(model,risk_model=Head(risk=True,bad=True))
    with pytest.raises(AdvisoryModelFirstError):
        predict_information_entry_nodes_v4(fitted=model,matrix=matrix)
