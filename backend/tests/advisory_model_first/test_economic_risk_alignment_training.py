from __future__ import annotations

import hashlib

import pytest

from backend.services.advisory_model_first.economic_entry_training import (
    prepare_economic_training_rows, train_economic_entry_model,
)
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryLossLabelV2
from backend.services.advisory_model_first.economic_risk_alignment_training import (
    prepare_entry_loss_training_v2, return_reuse_rows_sha256, train_entry_loss_head_v2,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def _training(study):
    args = {name: study[name] for name in ("features", "labels", "request")}
    parent = train_economic_entry_model(**args)
    rows = prepare_economic_training_rows(**args)
    risk = tuple(EntryLossLabelV2(original=value, original_label_sha256=value.label_sha256, status="AVAILABLE",
                                 entry_net_max_loss_bps=50., episode_peak_to_trough_drawdown_bps=value.daily_mark_max_drawdown_bps)
                 for value in study["labels"])
    return dict(features=study["features"], labels=risk, parent=parent,
                expected_parent_rows_sha256=return_reuse_rows_sha256(rows, parent.request))


def test_fixed_risk_head_reuses_return_and_test_outcomes_never_fit(study):
    import lightgbm as lgb
    args = _training(study)
    parent_digest = hashlib.sha256(args["parent"].return_model.model_to_string().encode()).hexdigest()
    first = train_entry_loss_head_v2(**args, expected_parent_return_sha256=parent_digest, expected_lightgbm_version=lgb.__version__)
    poisoned = tuple(value.model_copy(update={"entry_net_max_loss_bps": 9999.})
                     if value.original.decision_date >= study["request"].test_start else value for value in args["labels"])
    second = train_entry_loss_head_v2(**{**args, "labels": poisoned}, expected_parent_return_sha256=parent_digest,
                                      expected_lightgbm_version=lgb.__version__)
    assert first.risk_model.model_to_string() == second.risk_model.model_to_string()
    assert first.return_model is args["parent"].return_model
    assert first.diagnostics["new_return_heads"] == 0 and first.diagnostics["new_risk_heads"] == 1


def test_eligibility_drift_blocks_fit_instead_of_dropping_or_defaulting(study, monkeypatch):
    import lightgbm as lgb
    args = _training(study)
    values = list(args["labels"])
    values[0] = values[0].model_copy(update={"status": "UNAVAILABLE", "reason_code": "ENTRY_OPEN_LIMIT_EXECUTION_UNPROVEN",
                                          "entry_net_max_loss_bps": None, "episode_peak_to_trough_drawdown_bps": None})
    args["labels"] = tuple(values)
    rows, audit = prepare_entry_loss_training_v2(**args)
    assert len(rows) == len(values) and audit["differences_by_split"] == {"train": 1, "validation": 0}
    monkeypatch.setattr(lgb, "train", lambda *a, **k: pytest.fail("ineligible candidate must not fit"))
    with pytest.raises(AdvisoryModelFirstError, match="eligibility differs"):
        train_entry_loss_head_v2(**args, expected_parent_return_sha256="f" * 64, expected_lightgbm_version=lgb.__version__)


def test_parent_matrix_tamper_cannot_reuse_even_when_hash_column_claims_same_source(study):
    args = _training(study)
    args["features"].loc[0, "ret_1"] += 1.
    with pytest.raises(AdvisoryModelFirstError, match="matrix or return targets changed"):
        prepare_entry_loss_training_v2(**args)
