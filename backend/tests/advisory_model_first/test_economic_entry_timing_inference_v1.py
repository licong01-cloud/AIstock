from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_economic_entry_information_inference import Head
from backend.tests.advisory_model_first.test_economic_entry_timing_training_v1 import arguments
from backend.services.advisory_model_first.economic_entry_timing_contracts_v1 import ARMS, CANDIDATE_NAMES, CONTROL_NAMES
from backend.services.advisory_model_first.economic_entry_timing_training_v1 import TimingArmV1
from backend.services.advisory_model_first.economic_entry_timing_inference_v1 import predict_timing_entry_nodes_v1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def fitted(study, arm):
    request = arguments(study)[0]["request"]
    return TimingArmV1(request, arm, ARMS[arm], Head(), Head(risk=True), {name: (-100., 100.) for name in CANDIDATE_NAMES},
        {0: {"observation_count": 50, "decision_day_count": 10, "observed_min_gap_bps": 0., "observed_max_gap_bps": 50.}}, {})


@pytest.mark.parametrize("arm", list(ARMS))
def test_common_fifteen_support_even_control_arm_and_batch_single_parity(study, arm):
    model = fitted(study, arm)
    query = pd.DataFrame(.5, index=range(5), columns=CANDIDATE_NAMES)
    query["query_gap_bps"] = [10., 20., 200., 20., 20.]
    query.loc[3, "overnight_volatility_19"] = np.nan
    query.loc[4, "overnight_intraday_contrast_19"] = 101.
    result = predict_timing_entry_nodes_v1(fitted=model, matrix=query)
    assert result.model_action.tolist() == ["SKIP", "TAKE", "UNAVAILABLE", "UNAVAILABLE", "UNAVAILABLE"]
    assert result.expected_net_return_bps.iloc[2:].isna().all()
    single = pd.concat([predict_timing_entry_nodes_v1(fitted=model, matrix=query.iloc[[index]]) for index in query.index])
    pd.testing.assert_frame_equal(single, result)


@pytest.mark.parametrize("defect", ["old_names", "extra_future", "bounds", "price_support", "risk_output"])
def test_bad_family_future_columns_support_or_output_is_not_silent_unknown(study, defect):
    model = fitted(study, "TIMING_FIFTEEN")
    query = pd.DataFrame(.5, index=[0], columns=CANDIDATE_NAMES)
    if defect == "old_names":
        model = replace(model, feature_names=CONTROL_NAMES)
    elif defect == "extra_future":
        query["label_available"] = False
    elif defect == "bounds":
        model = replace(model, common_bounds={**model.common_bounds, "overnight_volatility_19": (2., 1.)})
    elif defect == "price_support":
        model = replace(model, price_support={0: {**model.price_support[0], "observation_count": True}})
    else:
        model = replace(model, risk_model=Head(risk=True, bad=True))
    with pytest.raises(AdvisoryModelFirstError):
        predict_timing_entry_nodes_v1(fitted=model, matrix=query)
