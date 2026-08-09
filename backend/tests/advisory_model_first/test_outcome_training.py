from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.outcome_contracts import OUTCOME_HORIZONS
from backend.services.advisory_model_first import outcome_training
from backend.services.advisory_model_first.outcome_training import (
    _holding_ranges,
    _require_probabilities,
    _train_booster,
    train_outcome_models,
)


class _FakeModel:
    def __init__(self, head: str) -> None:
        self.head = head
        self.best_iteration = 1

    def predict(self, matrix: pd.DataFrame):
        if self.head == "holding_bucket":
            probabilities = np.asarray([0.05, 0.15, 0.50, 0.20, 0.10], dtype=float)
            return np.tile(probabilities, (len(matrix), 1))
        if self.head.startswith(("positive_excess", "signal_survival")):
            return np.where(np.arange(len(matrix)) % 2 == 0, 0.65, 0.35)
        if "q10" in self.head:
            return np.full(len(matrix), 0.03)
        if "q50" in self.head:
            return np.full(len(matrix), 0.01)
        if "q90" in self.head:
            return np.full(len(matrix), 0.02)
        raise AssertionError(self.head)


def _matrix() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.bdate_range("2024-01-01", periods=406)
    instruments = ["000001.SZ", "000002.SZ"]
    rows = [(date, instruments[index % 2], index % 2 + 1) for date in dates for index in range(2)]
    identity = pd.DataFrame(
        {
            "decision_as_of_trade_date": [row[0] for row in rows],
            "target_trade_date": [row[0] + pd.offsets.BDay(1) for row in rows],
            "instrument": [row[1] for row in rows],
            "selection_effective_rank": [row[2] for row in rows],
        }
    )
    model_values = pd.DataFrame(
        {
            column: (np.arange(len(identity)) + position) % 17 / 17.0
            for position, column in enumerate(MODEL_FEATURE_COLUMNS)
        }
    )
    features = pd.concat([identity, model_values], axis=1)
    features["l2_code_id"] = np.where(np.arange(len(features)) % 2 == 0, 1, 2)
    labels = features[["decision_as_of_trade_date", "target_trade_date", "instrument"]].copy()
    split = np.full(len(labels), "purged", dtype=object)
    for date_position, date in enumerate(dates):
        mask = labels["decision_as_of_trade_date"] == date
        if date_position < 226:
            split[mask] = "train"
        elif 251 <= date_position < 301:
            split[mask] = "validation"
        elif date_position >= 326:
            split[mask] = "test"
    labels["split"] = split
    for horizon in OUTCOME_HORIZONS:
        labels[f"modelable_{horizon}"] = labels["split"].isin(["train", "validation", "test"])
        alternating = np.arange(len(labels)) % 2
        labels[f"excess_return_{horizon}"] = (alternating * 2 - 1) * (horizon / 1000.0)
        labels[f"positive_excess_{horizon}"] = alternating
        labels[f"signal_survival_{horizon}"] = 1 - alternating
        labels[f"path_mfe_{horizon}"] = 0.01 + horizon / 1000.0
        labels[f"path_mae_loss_{horizon}"] = 0.005 + alternating / 1000.0
    labels["holding_modelable"] = labels["split"].isin(["train", "validation", "test"])
    labels["optimal_holding_bucket"] = [OUTCOME_HORIZONS[index % 5] for index in range(len(labels))]
    return features, labels


def _parent_test_predictions(features: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    test = features.loc[labels["split"] == "test", keys].copy()
    test["advisory_model_rank"] = test.groupby("decision_as_of_trade_date").cumcount().add(1)
    return test


def test_outcome_training_builds_every_head_and_monotonic_ranges(monkeypatch) -> None:
    features, labels = _matrix()

    def fake_train_booster(**kwargs):
        return _FakeModel(str(kwargs["head"])), {"validation": {"metric": [1.0]}}

    monkeypatch.setattr(outcome_training, "_train_booster", fake_train_booster)
    result = train_outcome_models(
        features=features,
        labels=labels,
        parent_test_predictions=_parent_test_predictions(features, labels),
        seed=7,
    )

    assert len(result.models) == 46
    assert result.metrics["model_count"] == 46
    assert result.metrics["test_date_count"] == 80
    assert result.metrics["calibration_state"] == "UNCALIBRATED"
    assert result.test_predictions["excess_return_q10_5"].le(
        result.test_predictions["excess_return_q50_5"]
    ).all()
    assert result.test_predictions["excess_return_q50_5"].le(
        result.test_predictions["excess_return_q90_5"]
    ).all()
    assert result.test_predictions["holding_mode_days"].eq(5).all()
    assert result.test_predictions["holding_range_low_days"].le(
        result.test_predictions["holding_range_high_days"]
    ).all()
    assert result.test_predictions["path_mfe_q50_5"].le(
        result.test_predictions["path_mfe_q90_5"]
    ).all()
    assert result.test_predictions["path_mae_loss_q50_5"].le(
        result.test_predictions["path_mae_loss_q90_5"]
    ).all()
    assert "holding_modelable" not in result.test_predictions
    assert "holding_label_status" not in result.test_predictions
    assert result.metrics["group_summaries"]["selection_top5"]["decision_date_count"] == 80
    assert result.metrics["group_summaries"]["m2_model_top5"]["decision_date_count"] == 80


def test_binary_head_rejects_constant_train_label(monkeypatch) -> None:
    features, labels = _matrix()
    labels.loc[labels["split"] == "train", "positive_excess_1"] = 1
    monkeypatch.setattr(
        outcome_training,
        "_train_booster",
        lambda **kwargs: (_FakeModel(str(kwargs["head"])), {}),
    )

    with pytest.raises(AdvisoryModelFirstError) as error:
        train_outcome_models(
            features=features,
            labels=labels,
            parent_test_predictions=_parent_test_predictions(features, labels),
            seed=7,
        )
    assert error.value.reason_code == "ADVISORY_OUTCOME_CLASS_VARIATION_MISSING"


def test_holding_range_uses_20_and_80_percent_distribution_buckets() -> None:
    low, high = _holding_ranges(np.asarray([[0.05, 0.15, 0.50, 0.20, 0.10]]))
    assert low.tolist() == [3]
    assert high.tolist() == [10]


def test_binary_probability_contract_rejects_values_outside_zero_one() -> None:
    with pytest.raises(AdvisoryModelFirstError) as error:
        _require_probabilities(np.asarray([0.2, 1.01]), head="positive_excess_h1")
    assert error.value.reason_code == "ADVISORY_OUTCOME_TRAINING_FAILED"


def test_real_lightgbm_binary_head_trains_with_frozen_feature_order() -> None:
    pytest.importorskip("lightgbm")
    row_count = 140
    matrix = pd.DataFrame(
        {
            column: (np.arange(row_count) + position) % 11 / 11.0
            for position, column in enumerate(MODEL_FEATURE_COLUMNS)
        }
    )
    matrix["l2_code_id"] = pd.Categorical(np.where(np.arange(row_count) % 2 == 0, 1, 2), categories=[1, 2])
    target = pd.Series(np.arange(row_count) % 2)
    train_mask = pd.Series(np.arange(row_count) < 100)
    validation_mask = ~train_mask

    model, history = _train_booster(
        matrix=matrix,
        target=target,
        train_mask=train_mask,
        validation_mask=validation_mask,
        objective="binary",
        seed=7,
        head="binary_smoke",
    )

    predictions = np.asarray(model.predict(matrix.loc[validation_mask]))
    assert predictions.shape == (40,)
    assert np.isfinite(predictions).all()
    assert history
