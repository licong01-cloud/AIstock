"""Fixed new-candidate recipe and train-only handling; no physical research fit."""
import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import generic_exit_held_path_5td_pipeline_v1 as m
from backend.services.advisory_model_first.generic_exit_held_path_5td_contracts_v1 import MODEL_FEATURES


def test_fixed_25_inputs_train_only_medians_and_missing_flags():
    train = pd.DataFrame([{**dict.fromkeys(MODEL_FEATURES, float(i)), MODEL_FEATURES[2]: np.nan} for i in range(4)])
    evaluation = pd.DataFrame([dict.fromkeys(MODEL_FEATURES, 1e20)])
    matrix, recipe = m._matrix(train)
    future, same = m._matrix(evaluation, recipe)
    assert matrix.shape == (4, 25) and future.shape == (1, 25) and same == recipe
    assert recipe["median"][0] == 1.5 and recipe["median"][2] == 0.
    assert np.all(matrix[:, 15] == 1) and future[0, 0] > 1e10


def test_new_candidate_fit_calls_exact_recipe_only_after_callback(monkeypatch):
    rows = pd.DataFrame([dict(episode_id=episode, s_date="2025-01-02", held=True,
        y_hold_bps=target, **dict.fromkeys(MODEL_FEATURES, value)) for episode, value, target in
        (("past", 1., 50.), ("future", 1e20, 1e20))])
    fold = dict(ordinal=1, train_episode_ids=["past"], evaluation_episode_ids=["future"])
    steps = []
    class FixedRidge:
        def __init__(self, alpha):
            assert alpha == 1
        def fit(self, x, y):
            assert steps == ["before"] and x.shape == (1, 25) and list(y) == [50.]
            self.coef_, self.intercept_ = np.zeros(25), 0.
            steps.append("candidate_fit")
            return self
        def predict(self, x):
            return np.zeros(len(x))
    monkeypatch.setattr(m, "Ridge", FixedRidge)
    model, predictions = m.fit_held_path_fold_v1(rows=rows, fold=fold, before_fit=lambda ordinal: steps.append("before"))
    assert steps == ["before", "candidate_fit"] and model["physical_fits"] == 1
    assert predictions.episode_id.tolist() == ["future"]
    rows.loc[0, "y_hold_bps"] = np.nan
    empty_model, empty = m.fit_held_path_fold_v1(rows=rows, fold=fold, before_fit=lambda ordinal: pytest.fail("no train must not fit"))
    assert empty_model["physical_fits"] == 0 and empty.empty


def test_nonfinite_feature_is_explicit_error_not_imputation():
    with pytest.raises(ValueError, match="infinite"):
        m._matrix(pd.DataFrame([dict.fromkeys(MODEL_FEATURES, np.inf)]))


def test_two_control_pairing_uncertainty_is_fixed_and_unknown_not_zero():
    values = [5., np.nan, *([5.]*18)]
    result = m._paired_block_stats(values)
    assert result["mean_bps"] == 5. and result["ci95_bps"] == [5., 5.] and result["mde80_bps"] == 0.
    empty = m._paired_block_stats([np.nan]*20)
    assert empty["mean_bps"] is None and empty["ci95_bps"] is None
