import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_economic_entry_information_inference import fitted
from backend.tests.advisory_model_first.test_economic_entry_information_training import arguments
from backend.services.advisory_model_first.economic_entry_information_evaluation import query_information_actual_opens_v4
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def query_args(study,arm):
    information=arguments(study)[0]["information"]
    candidates=study["features"].loc[study["features"][KEY[0]]>=pd.Timestamp(study["request"].test_start),KEY].copy()
    candidates["selection_effective_rank"]=candidates.instrument.str[:6].astype(int)
    references=candidates.loc[:,KEY].copy()
    references["target_reference_raw_cny"]=10.
    references["reference_visible_through"]=references[KEY[0]]
    references["source_sha256"]=study["identity"].reference_source_sha256
    prices=candidates.loc[:,[KEY[1],"instrument"]].rename(columns={KEY[1]:"trade_date"})
    prices["raw_open_cny"],prices["up_limit"],prices["down_limit"]=10.,20.,1.
    prices["suspended"],prices["tradability_unknown"]=False,False
    prices["source_sha256"]=study["identity"].price_source_sha256
    prices["price_coordinate_sha256"]=study["identity"].price_coordinate_sha256
    return dict(fitted=fitted(study,arm),identity=study["identity"],candidates=candidates,features=study["features"],
        information=information,references=references,prices=prices)


@pytest.mark.parametrize("arm",["MATCHED_NINE","INFORMATION_THIRTEEN"])
def test_real_T_condition_keeps_roster_and_never_reads_future_labels(study,arm):
    args=query_args(study,arm)
    original=query_information_actual_opens_v4(**args)
    assert len(original)==18 and original.model_action.eq("TAKE").all()
    poisoned=args["features"].copy()
    poisoned["future_episode_return"]=999999.
    extras=args["information"].copy()
    extras["future_risk_status"]="UNAVAILABLE"
    quotes=args["prices"].copy()
    quotes["future_close"]=.001
    pd.testing.assert_frame_equal(original,query_information_actual_opens_v4(**{**args,"features":poisoned,"information":extras,"prices":quotes}))
    quotes.loc[quotes.index[0],"suspended"]=True
    changed=query_information_actual_opens_v4(**{**args,"prices":quotes})
    assert len(changed)==len(original) and changed.model_action.iloc[0]=="NOT_APPLICABLE"


@pytest.mark.parametrize("defect",["hash","reference_clock","raw_identity","candidate_rank"])
def test_observation_identity_contradictions_fail_closed(study,defect):
    args=query_args(study,"INFORMATION_THIRTEEN")
    if defect=="hash":
        args["information"]=args["information"].copy()
        args["information"].loc[0,"ret_10"]=1.
    elif defect=="reference_clock":
        args["references"].loc[0,"reference_visible_through"]=args["references"][KEY[1]].iloc[0]
    elif defect=="raw_identity":
        args["prices"].loc[0,"source_sha256"]="0"*64
    else:
        args["candidates"]["selection_effective_rank"]=1
    with pytest.raises(AdvisoryModelFirstError):
        query_information_actual_opens_v4(**args)
