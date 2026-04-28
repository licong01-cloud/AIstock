import numpy as np
import pandas as pd
import pytest

from backend import inference_engine


def _feature_frame():
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-04-24"), "000001.SZ"),
            (pd.Timestamp("2026-04-24"), "000002.SZ"),
            (pd.Timestamp("2026-04-24"), "000003.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame(
        {
            "factor_a": [1.0, np.nan, 3.0],
            "factor_b": [4.0, 5.0, np.inf],
        },
        index=index,
    )


def test_strict_score_frame_uses_scored_subset_index(monkeypatch):
    monkeypatch.setenv("AISTOCK_STRICT_INFERENCE", "1")
    features = _feature_frame()

    scored = inference_engine._drop_invalid_feature_rows_for_strict(features)
    result = inference_engine._build_score_frame_for_scored_features(scored, np.array([0.42]))

    assert list(result.index) == [features.index[0]]
    assert result["score"].tolist() == [0.42]
    assert inference_engine.LAST_STRICT_FEATURE_FILTER["input_rows"] == 3
    assert inference_engine.LAST_STRICT_FEATURE_FILTER["kept_rows"] == 1
    assert inference_engine.LAST_STRICT_FEATURE_FILTER["dropped_rows"] == 2


def test_score_frame_rejects_length_mismatch_after_filtering(monkeypatch):
    monkeypatch.setenv("AISTOCK_STRICT_INFERENCE", "1")
    scored = inference_engine._drop_invalid_feature_rows_for_strict(_feature_frame())

    with pytest.raises(ValueError, match="score length mismatch"):
        inference_engine._build_score_frame_for_scored_features(scored, np.array([0.1, 0.2]))


def test_inference_natural_days_uses_legacy_default(monkeypatch):
    monkeypatch.delenv("AISTOCK_INFERENCE_NATURAL_DAY_MULTIPLIER", raising=False)
    monkeypatch.delenv("AISTOCK_INFERENCE_NATURAL_DAY_BUFFER", raising=False)

    assert inference_engine._inference_natural_days_needed(260) == 400


def test_inference_natural_days_can_be_widened_for_strict_package_inference(monkeypatch):
    monkeypatch.setenv("AISTOCK_INFERENCE_NATURAL_DAY_MULTIPLIER", "1.8")
    monkeypatch.setenv("AISTOCK_INFERENCE_NATURAL_DAY_BUFFER", "20")

    assert inference_engine._inference_natural_days_needed(260) == 488


def test_inference_natural_days_rejects_invalid_config(monkeypatch):
    monkeypatch.setenv("AISTOCK_INFERENCE_NATURAL_DAY_MULTIPLIER", "0")

    with pytest.raises(ValueError, match="invalid inference data-window configuration"):
        inference_engine._inference_natural_days_needed(260)
