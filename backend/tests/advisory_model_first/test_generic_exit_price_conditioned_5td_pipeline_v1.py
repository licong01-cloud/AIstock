"""Fold supervision projection, no-QE-overlap and durable exact resume contracts."""
from datetime import datetime, timezone
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import generic_exit_price_conditioned_5td_models_v1 as models
from backend.services.advisory_model_first import generic_exit_price_conditioned_5td_pipeline_v1 as m
from backend.services.advisory_model_first.generic_exit_price_conditioned_5td_contracts_v1 import FEATURES, S_FIELDS, TRAIN_FIELDS


def inputs(tmp_path):
    prepared = tmp_path/"prepared"
    prepared.mkdir()
    state = {**dict.fromkeys(FEATURES, .01), "s_date": "2025-01-06", "remaining_session_fraction": .5}
    rows = pd.DataFrame([dict(**state, episode_id="past", entry_date="2025-01-02", held=True, y_hold_bps=20., sell_scenario_bps=10.),
        dict(**state, episode_id="future", entry_date="2025-01-06", held=True, y_hold_bps=1e20, sell_scenario_bps=1e20)])
    rows.to_parquet(prepared/"rows.parquet", index=False)
    rows.loc[:, S_FIELDS].to_parquet(prepared/"s_inputs.parquet", index=False)
    return dict(ordinal=1, train_episode_ids=["past"], evaluation_episode_ids=["future"])


def idle(count=0):
    return dict(checked_at_utc=datetime.now(timezone.utc).isoformat(),
        running_counts=dict(single=count, custom_evo=0, multi_alpha=0))


def test_busy_zero_fit_and_completed_fold_resume_never_queries_or_refits(tmp_path, monkeypatch):
    fold = inputs(tmp_path)
    with pytest.raises(ValueError, match="fresh QE"):
        m._train_fold(tmp_path, "same-plan", fold, lambda: idle(1))
    assert not (tmp_path/"folds"/"1"/"fit_attempt.json").exists()
    observations = []
    original_reader = pd.read_parquet
    def reader(path, **kwargs):
        observations.append((path.name, kwargs))
        return original_reader(path, **kwargs)
    class FakeRidge:
        def __init__(self, alpha):
            assert alpha == 1.
        def fit(self, x, y):
            assert x.shape == (1, 20) and list(y) == [20.]
            self.coef_, self.intercept_ = np.zeros(20), 20.
            return self
        def predict(self, x):
            return np.full(len(x), 20.)
    monkeypatch.setattr(pd, "read_parquet", reader)
    monkeypatch.setattr(models, "Ridge", FakeRidge)
    model, curves = m._train_fold(tmp_path, "same-plan", fold, idle)
    assert model["physical_fits"] == 1 and curves.episode_id.tolist() == ["future"]
    assert observations[0] == ("rows.parquet", dict(columns=list(TRAIN_FIELDS), filters=[("episode_id", "in", ["past"])]))
    assert observations[1][1]["columns"] == list(S_FIELDS)
    again, same = m._train_fold(tmp_path, "same-plan", fold, lambda: pytest.fail("resume never new QE/fit"))
    assert again == model and same.curve_sha256.tolist() == curves.curve_sha256.tolist()


def test_unresolved_started_marker_is_not_an_automatic_retry(tmp_path):
    fold = inputs(tmp_path)
    folder = tmp_path/"folds"/"1"
    folder.mkdir(parents=True)
    m._write(folder/"fit_attempt.json", b'{"status":"STARTED","plan_sha256":"same-plan"}')
    with pytest.raises(ValueError, match="automatically repeated"):
        m._train_fold(tmp_path, "same-plan", fold, lambda: pytest.fail("must not refit"))


def test_unknown_timeline_not_zero_and_constant_paired_block_statistics():
    result = m._paired_stats([5., np.nan, *([5.]*18)])
    assert result["mean_bps"] == 5. and result["ci95_bps"] == [5., 5.] and result["mde80_bps"] == 0.
    assert m._paired_stats([np.nan]*20)["mean_bps"] is None


def test_complete_curve_receipt_binds_exact_plan_and_file_identity(tmp_path, monkeypatch):
    fold = inputs(tmp_path)
    folder = tmp_path/"folds"/"1"
    folder.mkdir(parents=True)
    m._write(folder/"fit_attempt.json", json.dumps(dict(status="COMPLETE", plan_sha256="another-plan")).encode())
    with pytest.raises(ValueError, match="automatically repeated"):
        m._train_fold(tmp_path, "same-plan", fold, lambda: pytest.fail("identity drift"))
