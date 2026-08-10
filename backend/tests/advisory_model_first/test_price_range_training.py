from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import price_range_training
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.price_range_training import (
    _require_probabilities,
    train_price_range_models,
)


class _FakeModel:
    def __init__(self, head: str) -> None:
        self.head = head
        self.best_iteration = 3

    def predict(self, matrix: pd.DataFrame):
        if self.head == "entry_executable_probability":
            return np.where(np.arange(len(matrix)) % 2 == 0, 0.7, 0.3)
        values = {"entry_gap_q10": -0.02, "entry_gap_q50": 0.01, "entry_gap_q90": 0.04}
        return np.full(len(matrix), values[self.head])


def _matrix() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.bdate_range("2024-01-01", periods=406)
    rows = [(date, instrument) for date in dates for instrument in ("000001.SZ", "000002.SZ")]
    identity = pd.DataFrame(
        {
            "decision_as_of_trade_date": [row[0] for row in rows],
            "target_trade_date": [row[0] + pd.offsets.BDay(1) for row in rows],
            "instrument": [row[1] for row in rows],
        }
    )
    values = pd.DataFrame(
        {
            column: (np.arange(len(identity)) + position) % 17 / 17.0
            for position, column in enumerate(MODEL_FEATURE_COLUMNS)
        }
    )
    features = pd.concat([identity, values], axis=1)
    features["selection_effective_rank"] = np.tile([1, 2], len(dates))
    features["parent_combined_score"] = np.tile([0.8, 0.6], len(dates))
    features["l2_code_id"] = np.tile([1, 2], len(dates))
    labels = identity.copy()
    split = np.full(len(labels), "purged", dtype=object)
    for position, current in enumerate(dates):
        mask = labels["decision_as_of_trade_date"].eq(current)
        if position < 226:
            split[mask] = "train"
        elif 251 <= position < 301:
            split[mask] = "validation"
        elif position >= 326:
            split[mask] = "test"
    executable = np.tile([1, 0], len(dates))
    labels["split"] = split
    labels["entry_label_status"] = "AVAILABLE"
    labels["entry_label_reason"] = np.where(executable == 1, "target_open_executable", "suspended")
    labels["entry_executable"] = executable
    labels["entry_gap_return"] = np.where(executable == 1, 0.01, np.nan)
    labels["binary_modelable"] = labels["split"].isin(["train", "validation", "test"])
    labels["gap_modelable"] = labels["binary_modelable"] & labels["entry_executable"].eq(1)
    return features, labels


def test_price_range_training_builds_exact_four_heads_and_conditional_ranges(monkeypatch) -> None:
    features, labels = _matrix()
    monkeypatch.setattr(
        price_range_training,
        "_train_booster",
        lambda **kwargs: (_FakeModel(str(kwargs["head"])), {"validation": {"metric": [1.0]}}),
    )
    result = train_price_range_models(features=features, labels=labels, seed=7)

    assert set(result.models) == {
        "entry_executable_probability",
        "entry_gap_q10",
        "entry_gap_q50",
        "entry_gap_q90",
    }
    assert result.metrics["model_count"] == 4
    assert result.metrics["test_date_count"] == 80
    assert result.metrics["calibration_state"] == "UNCALIBRATED"
    assert len(result.test_predictions) == 160
    assert result.test_predictions["entry_executable_probability"].notna().all()
    assert result.test_predictions["entry_gap_q10"].le(result.test_predictions["entry_gap_q50"]).all()
    assert result.test_predictions["entry_gap_q50"].le(result.test_predictions["entry_gap_q90"]).all()
    assert result.test_predictions["entry_gap_condition"].eq("ENTRY_EXECUTABLE").all()


def test_price_range_binary_head_rejects_constant_train_label(monkeypatch) -> None:
    features, labels = _matrix()
    labels.loc[labels["split"].eq("train"), "entry_executable"] = 1
    labels.loc[labels["split"].eq("train"), "entry_gap_return"] = 0.01
    labels.loc[labels["split"].eq("train"), "gap_modelable"] = True
    monkeypatch.setattr(
        price_range_training,
        "_train_booster",
        lambda **kwargs: (_FakeModel(str(kwargs["head"])), {}),
    )
    with pytest.raises(AdvisoryModelFirstError) as error:
        train_price_range_models(features=features, labels=labels, seed=7)
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_LABEL_VARIATION_MISSING"


def test_price_range_probability_contract_rejects_out_of_range_value() -> None:
    with pytest.raises(AdvisoryModelFirstError) as error:
        _require_probabilities(np.asarray([0.2, 1.01]), head="entry_executable_probability")
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_TRAINING_FAILED"


def test_price_range_training_records_inherited_m1_feature_unavailability(monkeypatch) -> None:
    features, labels = _matrix()
    train_date = labels.loc[labels["split"].eq("train"), "decision_as_of_trade_date"].iloc[0]
    features = features.loc[~features["decision_as_of_trade_date"].eq(train_date)].copy()
    monkeypatch.setattr(
        price_range_training,
        "_train_booster",
        lambda **kwargs: (_FakeModel(str(kwargs["head"])), {}),
    )

    result = train_price_range_models(features=features, labels=labels, seed=7)

    assert result.metrics["feature_unavailable_row_count"] == 2
    assert result.metrics["feature_unavailable_date_count"] == 1
    assert result.metrics["feature_unavailable_rows_by_split"]["train"] == 2
    assert len(result.test_predictions) == 160


def test_price_range_training_rejects_missing_test_feature_row(monkeypatch) -> None:
    features, labels = _matrix()
    test_index = labels.index[labels["split"].eq("test")][0]
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    missing_key = tuple(labels.loc[test_index, keys])
    mask = pd.Series(True, index=features.index)
    for column, value in zip(keys, missing_key):
        mask &= features[column].eq(value)
    features = features.loc[~mask].copy()
    monkeypatch.setattr(
        price_range_training,
        "_train_booster",
        lambda **kwargs: (_FakeModel(str(kwargs["head"])), {}),
    )

    with pytest.raises(AdvisoryModelFirstError) as error:
        train_price_range_models(features=features, labels=labels, seed=7)
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_SAMPLE_INSUFFICIENT"


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("entry_executable", 0.5),
        ("binary_modelable", False),
        ("gap_modelable", False),
    ],
)
def test_price_range_training_rejects_malformed_label_contract(
    monkeypatch,
    column: str,
    value: object,
) -> None:
    features, labels = _matrix()
    train_index = labels.index[
        labels["split"].eq("train") & labels["entry_executable"].eq(1)
    ][0]
    if column == "entry_executable":
        labels[column] = labels[column].astype(float)
    labels.loc[train_index, column] = value
    monkeypatch.setattr(
        price_range_training,
        "_train_booster",
        lambda **kwargs: (_FakeModel(str(kwargs["head"])), {}),
    )

    with pytest.raises(AdvisoryModelFirstError) as error:
        train_price_range_models(features=features, labels=labels, seed=7)
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_LABEL_INPUT_UNAVAILABLE"
