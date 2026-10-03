"""Small inherited supervision fixture, real four heads and test isolation."""
import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_economic_entry_aligned_training import _inputs
from backend.services.advisory_model_first.economic_entry_aligned_training import assemble_aligned_rows_v3, common_fit_rows_sha256, risk_labels_content_sha256
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_timing_contracts_v1 import CANDIDATE_NAMES, TimingTrainingRequestV1
from backend.services.advisory_model_first.economic_entry_timing_training_v1 import (
    assemble_timing_rows_v1, timing_common_fit_sha256, timing_input_rows_sha256, train_timing_entry_v1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def arguments(study):
    inherited = _inputs(study)
    inputs = study["features"].loc[:, KEY].copy()
    for name in CANDIDATE_NAMES[:-1]:
        inputs[name] = .5
    inputs["feature_visible_through"] = inputs[KEY[0]]
    inputs["daily_input_sha256"] = "a"*64
    inputs.loc[1, "overnight_volatility_19"] = np.nan
    parent = inherited["request"]
    rows = assemble_timing_rows_v1(original_features=inherited["features"], labels=inherited["labels"], inputs=inputs, parent_request=parent)
    request = TimingTrainingRequestV1(parent_request=parent,
        input_ref=EvidenceReferenceV1(role="timing_input_manifest_v1", artifact_uri="F:/unit_fixture/manifest.json", sha256="b"*64, size_bytes=10),
        input_rows_sha256=timing_input_rows_sha256(inputs), recipe_sha256="c"*64,
        common_fit_rows_sha256=timing_common_fit_sha256(rows), implementation_sha256="d"*64)
    return dict(original_features=inherited["features"], labels=inherited["labels"], inputs=inputs, request=request), rows


def test_real_four_heads_shared_population_and_test_poison_cannot_change_fit(study):
    args, rows = arguments(study)
    first = train_timing_entry_v1(**args)
    assert first.diagnostics["fitted_head_count"] == 4 and len(first.arms) == 2
    assert {value.diagnostics["train_rows"] for value in first.arms.values()} == {52}
    assert not rows.timing_training_eligible.iloc[1] and len(rows) == len(study["labels"])
    poisoned = args["inputs"].copy()
    poisoned.loc[poisoned[KEY[0]] >= pd.Timestamp(study["request"].test_start), list(CANDIDATE_NAMES[:-1])] = 9999.
    labels = tuple(value.model_copy(update={"entry_net_max_loss_bps": 9999.})
        if value.original.decision_date >= study["request"].test_start else value for value in args["labels"])
    parent = args["request"].parent_request.model_copy(update={"risk_labels_content_sha256": risk_labels_content_sha256(labels)})
    request = args["request"].model_copy(update={"parent_request": parent, "input_rows_sha256": timing_input_rows_sha256(poisoned)})
    second = train_timing_entry_v1(**{**args, "inputs": poisoned, "labels": labels, "request": request})
    for arm in first.arms:
        for head in ("return_model", "risk_model"):
            assert getattr(first.arms[arm], head).model_to_string() == getattr(second.arms[arm], head).model_to_string()
        assert first.arms[arm].common_bounds == second.arms[arm].common_bounds
        assert first.arms[arm].price_support == second.arms[arm].price_support
    assert first.arms["CORE_THIRTEEN"].common_bounds == first.arms["TIMING_FIFTEEN"].common_bounds


def test_new_core_not_old_missing_feature_mask_drives_eligibility_and_purge_remains(study):
    args, _ = arguments(study)
    original = args["original_features"].copy()
    original.loc[2, "ret_1"] = np.nan
    parent = args["request"].parent_request
    prior = assemble_aligned_rows_v3(features=original, labels=args["labels"], source_request=parent.source_request)
    parent = parent.model_copy(update={"common_fit_rows_sha256": common_fit_rows_sha256(prior, parent.source_request.feature_names)})
    rows = assemble_timing_rows_v1(original_features=original, labels=args["labels"], inputs=args["inputs"], parent_request=parent)
    assert not prior.common_training_eligible.iloc[2] and rows.timing_training_eligible.iloc[2]
    assert rows.split.eq("PURGED_LABEL_END").sum() == prior.split.eq("PURGED_LABEL_END").sum()


@pytest.mark.parametrize("defect", ["hash", "clock", "lost", "label_column"])
def test_input_identity_clock_population_and_feature_whitelist_fail_before_fit(study, defect, monkeypatch):
    import lightgbm as lgb
    args, _ = arguments(study)
    if defect == "hash":
        args["request"] = args["request"].model_copy(update={"common_fit_rows_sha256": "e"*64})
    elif defect == "clock":
        args["inputs"].loc[0, "feature_visible_through"] = args["inputs"][KEY[1]].iloc[0]
    elif defect == "lost":
        args["inputs"] = args["inputs"].iloc[1:]
        args["request"] = args["request"].model_copy(update={"input_rows_sha256": timing_input_rows_sha256(args["inputs"])})
    else:
        args["inputs"]["future_label_available"] = True
    monkeypatch.setattr(lgb, "train", lambda *a, **k: pytest.fail("invalid contract must not fit"))
    with pytest.raises(AdvisoryModelFirstError):
        train_timing_entry_v1(**args)
