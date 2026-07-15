from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("lightgbm")
pytest.importorskip("qlib")

from qlib.data.dataset.handler import DataHandlerLP  # noqa: E402

from aistock_models.aistock_models.lambdarank import LambdaRankModel  # noqa: E402


def _instrument_major_frame() -> pd.DataFrame:
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-01-05"), "000001.SZ"),
            (pd.Timestamp("2026-01-06"), "000001.SZ"),
            (pd.Timestamp("2026-01-05"), "000002.SZ"),
            (pd.Timestamp("2026-01-06"), "000002.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    columns = pd.MultiIndex.from_tuples(
        [("feature", "f1"), ("label", "LABEL0")]
    )
    return pd.DataFrame(
        [[1.0, 0.1], [2.0, 0.4], [3.0, 0.2], [4.0, 0.3]],
        index=index,
        columns=columns,
    )


def test_query_sort_makes_each_date_physically_contiguous():
    model = LambdaRankModel(relevance_bins=5)
    sorted_frame = model._sort_by_query(_instrument_major_frame(), segment="train")
    assert list(sorted_frame.index.get_level_values("datetime")) == [
        pd.Timestamp("2026-01-05"),
        pd.Timestamp("2026-01-05"),
        pd.Timestamp("2026-01-06"),
        pd.Timestamp("2026-01-06"),
    ]
    assert model._build_query_groups(sorted_frame.index) == [2, 2]


def test_cross_sectional_relevance_depends_only_on_each_query_order():
    model = LambdaRankModel(relevance_bins=5)
    sorted_frame = model._sort_by_query(_instrument_major_frame(), segment="train")
    labels = sorted_frame["label"].to_numpy(dtype="float64").ravel()
    relevance = model._cross_sectional_relevance(sorted_frame.index, labels)
    assert relevance.tolist() == [0, 4, 4, 0]

    validation_outliers = labels * 1_000_000.0 + 999.0
    validation_relevance = model._cross_sectional_relevance(
        sorted_frame.index, validation_outliers
    )
    assert validation_relevance.tolist() == relevance.tolist()


def test_query_group_builder_rejects_noncontiguous_dates():
    model = LambdaRankModel()
    with pytest.raises(ValueError, match="sorted by datetime"):
        model._build_query_groups(_instrument_major_frame().index)


@pytest.mark.parametrize("relevance_bins", [True, 20.5, "20.5", 1, 32])
def test_relevance_bins_rejects_silent_coercion(relevance_bins):
    with pytest.raises(ValueError, match="relevance_bins"):
        LambdaRankModel(relevance_bins=relevance_bins)


def test_predict_uses_inference_data_so_long_horizon_tail_is_preserved():
    class _Dataset:
        def __init__(self) -> None:
            self.data_key = None

        def prepare(self, segment, col_set, data_key):
            assert segment == "test"
            assert col_set == "feature"
            self.data_key = data_key
            index = pd.MultiIndex.from_tuples(
                [(pd.Timestamp("2026-06-29"), "000001.SZ")],
                names=["datetime", "instrument"],
            )
            return pd.DataFrame([[1.0]], index=index, columns=["f1"])

    class _Predictor:
        @staticmethod
        def predict(values):
            assert values.shape == (1, 1)
            return np.array([0.25], dtype="float64")

    dataset = _Dataset()
    model = LambdaRankModel()
    model.model = _Predictor()

    prediction = model.predict(dataset)

    assert dataset.data_key == DataHandlerLP.DK_I
    assert prediction.iloc[0] == pytest.approx(0.25)
