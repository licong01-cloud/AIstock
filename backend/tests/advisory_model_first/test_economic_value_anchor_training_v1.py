from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_value_anchor_training_v1 import ValueAnchorStudyPlanV1, assemble_value_anchor_rows_v1, train_value_anchor_v1, value_anchor_predict_v1


def training_fixture():
    days = pd.bdate_range("2025-01-02", periods=19)
    configuration = SimpleNamespace(train_start=days[0], train_end=days[9], validation_start=days[10], validation_end=days[13], test_start=days[14], test_end=days[17])
    inputs = pd.DataFrame([{KEY[0]: day, KEY[1]: days[index+1], KEY[2]: f"{symbol:06d}.SZ",
        **dict.fromkeys(D_FEATURES, .5+index*.01), "feature_visible_through": day, "daily_input_sha256": "a"*64}
        for index, day in enumerate(days[:-1]) for symbol in range(1, 6)])
    labels = inputs.loc[:, KEY].assign(value_label_status="AVAILABLE", gross_value_ratio=1.04, path_min_value_ratio=.97, label_information_end=inputs[KEY[1]])
    observations = inputs.loc[:, KEY].assign(actual_gap_bps=20.)
    refs = {"artifact_uri": "F:/unit/manifest.json", "sha256": "a"*64, "size_bytes": 1}
    plan = ValueAnchorStudyPlanV1(parent_plan_ref={**refs, "role": "value_parent_plan"}, parent_prepared_manifest_ref={**refs, "role": "value_prices_snapshot"},
        feature_manifest_ref={**refs, "role": "value_d_snapshot"}, implementation_sha256="b"*64)
    return dict(inputs=inputs, labels=labels, observations=observations, configuration=configuration), plan


class UnitModel:
    def __init__(self, level):
        self.level = level

    def predict(self, matrix, **kwargs):
        return np.full(len(matrix), self.level)


def test_two_heads_D_only_purge_and_test_poison_cannot_change_fit_or_support(monkeypatch):
    import lightgbm as lgb
    args, plan = training_fixture()
    calls = []

    def capture(parameters, dataset, **kwargs):
        calls.append((parameters, dataset.data.copy(), dataset.label.copy(), kwargs))
        return UnitModel(float(np.mean(dataset.label)))

    monkeypatch.setattr(lgb, "train", capture)
    rows = assemble_value_anchor_rows_v1(**args)
    before = train_value_anchor_v1(rows=rows, plan=plan, configuration=args["configuration"])
    assert len(calls) == 2 and rows.purged.sum() == 15
    assert tuple(calls[0][1].columns) == D_FEATURES and calls[0][3]["num_boost_round"] == 200
    train_calls = [(values.copy(), target.copy()) for _, values, target, _ in calls]
    rows.loc[rows.split.eq("test"), [*D_FEATURES, "gross_value_ratio", "path_min_value_ratio", "actual_gap_bps"]] = 999999.
    after = train_value_anchor_v1(rows=rows, plan=plan, configuration=args["configuration"])
    for (first_matrix, first_target), (_, matrix, target, _) in zip(train_calls, calls[2:], strict=True):
        pd.testing.assert_frame_equal(first_matrix, matrix)
        pd.testing.assert_series_equal(first_target, target)
    assert before.gap_support == after.gap_support and before.constant == after.constant
    assert before.diagnostics["test_used_for_training_or_calibration"] is False
    with pytest.raises(ValueError):
        value_anchor_predict_v1(before, rows.loc[:1, list(reversed(D_FEATURES))])


@pytest.mark.parametrize("defect", ["clock", "missing_candidate", "bool", "invalid_label"])
def test_supervision_identity_and_corrupt_values_fail_before_fit(defect):
    args, _ = training_fixture()
    if defect == "clock":
        args["inputs"].loc[0, "feature_visible_through"] = args["inputs"][KEY[1]].iloc[0]
    elif defect == "missing_candidate":
        args["labels"] = args["labels"].iloc[1:]
    elif defect == "bool":
        args["inputs"][D_FEATURES[0]] = True
    else:
        args["labels"].loc[0, "gross_value_ratio"] = -1.
    with pytest.raises(ValueError):
        assemble_value_anchor_rows_v1(**args)
