from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryTrainingRequestV3
from backend.services.advisory_model_first.economic_entry_aligned_inference import predict_aligned_entry_nodes_v3
from backend.services.advisory_model_first.economic_entry_aligned_training import (
    assemble_aligned_rows_v3, common_fit_rows_sha256, prepare_aligned_rows_v3,
    risk_labels_content_sha256, train_aligned_entry_model_v3,
)
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryLossLabelV2
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def _inputs(study):
    import lightgbm as lgb
    risk = tuple(EntryLossLabelV2(original=value, original_label_sha256=value.label_sha256, status="AVAILABLE",
                                 entry_net_max_loss_bps=50., episode_peak_to_trough_drawdown_bps=value.daily_mark_max_drawdown_bps)
                 for value in study["labels"])
    risk = (risk[0].model_copy(update={"status": "UNAVAILABLE", "reason_code": "ENTRY_OPEN_LIMIT_EXECUTION_UNPROVEN",
                                     "entry_net_max_loss_bps": None, "episode_peak_to_trough_drawdown_bps": None}), *risk[1:])
    rows = assemble_aligned_rows_v3(features=study["features"], labels=risk, source_request=study["request"])
    request = AlignedEntryTrainingRequestV3(source_request=study["request"], risk_labels_content_sha256=risk_labels_content_sha256(risk),
                                           common_fit_rows_sha256=common_fit_rows_sha256(rows, study["request"].feature_names),
                                           implementation_sha256="a" * 64, lightgbm_version=lgb.__version__)
    return dict(features=study["features"], labels=risk, request=request)


def test_same_cohort_both_heads_purge_and_future_poison_cannot_fit(study):
    args = _inputs(study)
    rows = prepare_aligned_rows_v3(**args)
    assert len(rows) == len(study["labels"]) and not rows.common_training_eligible.iloc[0]
    assert "risk_target_bps" not in rows
    first = train_aligned_entry_model_v3(**args)
    poisoned = tuple(value.model_copy(update={"entry_net_max_loss_bps": 9999.})
                     if value.original.decision_date >= study["request"].test_start else value for value in args["labels"])
    request = args["request"].model_copy(update={"risk_labels_content_sha256": risk_labels_content_sha256(poisoned)})
    args["features"]["future_high"] = 1e9
    second = train_aligned_entry_model_v3(**{**args, "labels": poisoned, "request": request})
    assert first.return_model.model_to_string() == second.return_model.model_to_string()
    assert first.risk_model.model_to_string() == second.risk_model.model_to_string()
    assert first.diagnostics["train_rows"] == 53 and first.diagnostics["purged_rows"] == 24
    assert first.diagnostics["new_return_heads"] == first.diagnostics["new_risk_heads"] == 1
    assert request.training_input_sha256 != request.source_request.input_identity_sha256


@pytest.mark.parametrize("defect", ["mask", "labels", "matrix", "version"])
def test_registered_inputs_or_runtime_drift_fail_before_fit(study, defect, monkeypatch):
    import lightgbm as lgb
    args = _inputs(study)
    if defect == "mask":
        args["request"] = args["request"].model_copy(update={"common_fit_rows_sha256": "b" * 64})
    elif defect == "labels":
        args["request"] = args["request"].model_copy(update={"risk_labels_content_sha256": "b" * 64})
    elif defect == "matrix":
        args["features"].loc[1, "ret_1"] += 1
    else:
        args["request"] = args["request"].model_copy(update={"lightgbm_version": "invalid"})
    monkeypatch.setattr(lgb, "train", lambda *a, **k: pytest.fail("contradictory registration must not train"))
    with pytest.raises(AdvisoryModelFirstError):
        train_aligned_entry_model_v3(**args)


def test_one_numerical_kernel_batch_single_unknown_and_invalid_output(study):
    args = _inputs(study)
    fitted = train_aligned_entry_model_v3(**args)
    query = args["features"].iloc[6:12].copy()
    query["query_gap_bps"] = np.arange(6) * 10.
    batch = predict_aligned_entry_nodes_v3(fitted=fitted, matrix=query)
    single = pd.concat([predict_aligned_entry_nodes_v3(fitted=fitted, matrix=query.iloc[[index]]) for index in range(6)])
    pd.testing.assert_frame_equal(batch, single)
    query.loc[query.index[0], "ret_1"] = None
    unknown = predict_aligned_entry_nodes_v3(fitted=fitted, matrix=query)
    assert unknown.model_action.iloc[0] == "UNAVAILABLE" and pd.isna(unknown.expected_net_return_bps.iloc[0])
    fitted.risk_model.predict = lambda frame, **kwargs: np.full(len(frame), -1.)
    with pytest.raises(AdvisoryModelFirstError, match="invalid numerical estimates"):
        predict_aligned_entry_nodes_v3(fitted=fitted, matrix=query)
