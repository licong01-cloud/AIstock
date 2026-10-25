import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_economic_entry_aligned_training import _inputs
from backend.services.advisory_model_first.economic_daily_information_v1 import FEATURES
from backend.services.advisory_model_first.economic_entry_information_contracts import EconomicEntryInformationTrainingRequestV4
from backend.services.advisory_model_first.economic_entry_information_training import (
    assemble_information_rows_v4, information_common_fit_rows_sha256, information_rows_sha256, train_information_entry_v4,
)
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_aligned_training import risk_labels_content_sha256
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def arguments(study):
    parent = _inputs(study)
    info = study["features"].loc[:,KEY].copy()
    for name in FEATURES:
        info[name]=.5
    info["information_visible_through"]=info[KEY[0]]
    info["information_day_sha256"]="a"*64
    info.loc[1,"ret_10"]=np.nan  # Genuine missing source; keep the original candidate.
    parent_request=parent.pop("request")
    rows=assemble_information_rows_v4(**parent,information=info,parent_request=parent_request)
    request=EconomicEntryInformationTrainingRequestV4(parent_request=parent_request,
        information_ref=EvidenceReferenceV1(role="entry_information_v4_manifest",artifact_uri="F:/unit_fixture/info.json",sha256="b"*64,size_bytes=20),
        information_rows_sha256=information_rows_sha256(info),common_fit_rows_sha256=information_common_fit_rows_sha256(rows),implementation_sha256="c"*64)
    return {**parent,"information":info,"request":request},rows


def test_common_cohort_four_heads_and_test_poison_never_fits(study):
    args,rows=arguments(study)
    assert len(rows)==len(study["labels"]) and not rows.information_training_eligible.iloc[1]
    first=train_information_entry_v4(**args)
    assert first.diagnostics["model_configuration_count"]==2 and first.diagnostics["fitted_head_count"]==4
    assert {value.diagnostics["train_rows"] for value in first.arms.values()}=={52}
    labels=tuple(value.model_copy(update={"entry_net_max_loss_bps":9999.}) if value.original.decision_date>=study["request"].test_start else value
                 for value in args["labels"])
    info=args["information"].copy()
    info.loc[info[KEY[0]]>=pd.Timestamp(study["request"].test_start),list(FEATURES)]=9999.
    parent=args["request"].parent_request.model_copy(update={"risk_labels_content_sha256":risk_labels_content_sha256(labels)})
    request=args["request"].model_copy(update={"parent_request":parent,"information_rows_sha256":information_rows_sha256(info)})
    second=train_information_entry_v4(**{**args,"labels":labels,"information":info,"request":request})
    for arm in first.arms:
        assert first.arms[arm].return_model.model_to_string()==second.arms[arm].return_model.model_to_string()
        assert first.arms[arm].risk_model.model_to_string()==second.arms[arm].risk_model.model_to_string()
        assert first.arms[arm].feature_bounds==second.arms[arm].feature_bounds
    assert first.diagnostics["candidate_rows_preserved"]==len(study["labels"])


@pytest.mark.parametrize("defect", ["source_hash", "common_hash", "future", "duplicate", "lost", "bool"])
def test_source_cohort_and_D_clock_contradictions_fail_closed(study,defect):
    args,_=arguments(study)
    info=args["information"].copy()
    request=args["request"]
    if defect=="source_hash":
        request=request.model_copy(update={"information_rows_sha256":"d"*64})
    elif defect=="common_hash":
        request=request.model_copy(update={"common_fit_rows_sha256":"d"*64})
    elif defect=="future":
        info.loc[0,"information_visible_through"]=info[KEY[0]].max()
    elif defect=="duplicate":
        info=pd.concat([info,info.iloc[:1]])
    elif defect=="lost":
        info=info.iloc[1:]
    else:
        info["ret_10"]=True
    if defect not in {"source_hash","common_hash","duplicate"}:
        request=request.model_copy(update={"information_rows_sha256":information_rows_sha256(info)})
    with pytest.raises(AdvisoryModelFirstError):
        train_information_entry_v4(**{**args,"information":info,"request":request})
