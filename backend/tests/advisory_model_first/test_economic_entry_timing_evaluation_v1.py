import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_economic_entry_timing_inference_v1 import fitted
from backend.tests.advisory_model_first.test_economic_entry_timing_training_v1 import arguments
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_timing_evaluation_v1 import query_timing_actual_opens_v1, timing_descriptive_mde_v1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def test_descriptive_mde_is_fixed_scaled_and_never_claims_degenerate_power():
    values = [0., 10., -20., 5., 30., -2.]*5
    one, two = timing_descriptive_mde_v1(values), timing_descriptive_mde_v1([value*2 for value in values])
    assert one == timing_descriptive_mde_v1(values)
    assert two["normal_approximation_mde_bps"] == pytest.approx(2*one["normal_approximation_mde_bps"])
    assert timing_descriptive_mde_v1([0.]*30)["normal_approximation_mde_bps"] is None
    assert timing_descriptive_mde_v1(values[:5])["status"] == "UNPROVEN_SHORT_OR_DEGENERATE_SERIES"
    with pytest.raises(AdvisoryModelFirstError):
        timing_descriptive_mde_v1([float("nan")])


def query_args(study):
    inputs = arguments(study)[0]["inputs"]
    candidates = inputs.loc[inputs[KEY[0]] >= pd.Timestamp(study["request"].test_start), KEY].copy()
    candidates["selection_effective_rank"] = candidates.instrument.str[:6].astype(int)
    references = candidates.loc[:, KEY].assign(target_reference_raw_cny=10., reference_visible_through=candidates[KEY[0]],
        source_sha256=study["identity"].reference_source_sha256)
    prices = candidates.loc[:, [KEY[1], "instrument"]].rename(columns={KEY[1]: "trade_date"})
    prices["raw_open_cny"], prices["up_limit"], prices["down_limit"] = 10., 20., 1.
    prices["suspended"], prices["tradability_unknown"] = False, False
    prices["source_sha256"], prices["price_coordinate_sha256"] = study["identity"].price_source_sha256, study["identity"].price_coordinate_sha256
    return dict(fitted=fitted(study, "TIMING_FIFTEEN"), identity=study["identity"], candidates=candidates, inputs=inputs, references=references, prices=prices)


def test_T_condition_does_not_read_future_prices_or_maturity_and_preserves_unknowns(study):
    args = query_args(study)
    first = query_timing_actual_opens_v1(**args)
    assert len(first) == 18 and first.model_action.eq("TAKE").all()
    args["prices"]["future_close"] = .0001
    args["prices"]["future_label_available"] = False
    pd.testing.assert_frame_equal(first, query_timing_actual_opens_v1(**args))
    args["prices"].loc[args["prices"].index[0], "suspended"] = True
    result = query_timing_actual_opens_v1(**args)
    assert len(result) == len(first) and result.model_action.iloc[0] == "NOT_APPLICABLE"
    args["prices"].loc[args["prices"].index[1], "raw_open_cny"] = 20.
    result = query_timing_actual_opens_v1(**args)
    assert result.reason_code.iloc[1] == "OPEN_LIMIT_OR_EXECUTABILITY_UNPROVEN"


@pytest.mark.parametrize("defect", ["hash", "clock", "ranks", "foreign_candidate", "contradictory_limit"])
def test_observation_identity_mismatch_is_not_unknown_success(study, defect):
    args = query_args(study)
    if defect == "hash":
        args["prices"].loc[args["prices"].index[0], "source_sha256"] = "0"*64
    elif defect == "clock":
        args["references"].loc[args["references"].index[0], "reference_visible_through"] = args["references"][KEY[1]].iloc[0]
    elif defect == "ranks":
        args["candidates"]["selection_effective_rank"] = 1
    elif defect == "foreign_candidate":
        args["candidates"].loc[args["candidates"].index[0], "instrument"] = "999999.SZ"
    else:
        args["prices"].loc[args["prices"].index[0], "raw_open_cny"] = 21.
    with pytest.raises(AdvisoryModelFirstError):
        query_timing_actual_opens_v1(**args)
