from datetime import date
import pickle

import numpy as np
import pandas as pd
import pytest

from backend import inference_engine
from backend.data_service.preprocessor import get_required_data_window, infer_factor_lookback_days


class _FakeDataset:
    def __init__(self, processors):
        self.handler = _FakeHandler(processors)


class _FakeHandler:
    def __init__(self, processors):
        self.infer_processors = processors


class _FillFactorAProcessor:
    def __call__(self, df):
        df[("feature", "factor_a")] = df[("feature", "factor_a")].fillna(0.0)
        return df


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
    assert inference_engine.LAST_STRICT_FEATURE_FILTER["invalid_column_details"][0]["invalid_count"] == 1


def test_saved_qe_infer_processors_apply_before_strict_filter(tmp_path):
    processor_path = tmp_path / "model" / "dataset"
    processor_path.parent.mkdir()
    processor_path.write_bytes(pickle.dumps(_FakeDataset([_FillFactorAProcessor()])))

    features = _feature_frame().iloc[:2].copy()
    processed = inference_engine._apply_saved_qe_infer_processors(
        features,
        task_dir=tmp_path,
        primary_assets={"dataset_processor_relpath": "model/dataset"},
    )

    assert processed.loc[features.index[1], "factor_a"] == 0.0
    assert list(processed.columns) == ["factor_a", "factor_b"]


def test_score_frame_rejects_length_mismatch_after_filtering(monkeypatch):
    monkeypatch.setenv("AISTOCK_STRICT_INFERENCE", "1")
    scored = inference_engine._drop_invalid_feature_rows_for_strict(_feature_frame())

    with pytest.raises(ValueError, match="score length mismatch"):
        inference_engine._build_score_frame_for_scored_features(scored, np.array([0.1, 0.2]))



def test_factor_lookback_infers_dynamic_250d_suffix():
    assert infer_factor_lookback_days("m_turnover_percentile_250d") == 250
    assert infer_factor_lookback_days("PriceStrength_120D") == 120
    assert infer_factor_lookback_days("m_roc120d") == 120
    assert infer_factor_lookback_days("PriceStrength_10D") == 20


def test_required_data_window_uses_largest_factor_lookback():
    assert get_required_data_window(["KLEN", "m_turnover_percentile_250d"]) == 250
    assert get_required_data_window(["ROC60", "m_atr_percentile_250d"]) == 250
    assert get_required_data_window(["ROC60"]) == 61


class _CalendarCursor:
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql, params):
        self.params = params

    def fetchone(self):
        target_date, offset = self.params
        eligible = [row for row in self.rows if row <= target_date]
        eligible.sort(reverse=True)
        if offset < len(eligible):
            return (eligible[offset],)
        return None


class _CalendarConn:
    def __init__(self, rows, cursor_holder):
        self.rows = rows
        self.cursor_holder = cursor_holder

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        cursor = _CalendarCursor(self.rows)
        self.cursor_holder.append(cursor)
        return cursor


def test_resolve_inference_start_date_uses_trading_calendar_offset(monkeypatch):
    rows = [
        date(2026, 1, 2),
        date(2026, 1, 5),
        date(2026, 1, 6),
        date(2026, 1, 7),
        date(2026, 1, 8),
        date(2026, 1, 9),
        date(2026, 1, 12),
        date(2026, 1, 13),
        date(2026, 1, 14),
        date(2026, 1, 15),
    ]
    cursors = []
    monkeypatch.setattr(inference_engine, "get_conn", lambda: _CalendarConn(rows, cursors))

    start_date, source = inference_engine.InferenceEngine()._resolve_inference_start_date(
        pd.Timestamp("2026-01-15"),
        required_window=5,
        buffer_days=2,
    )

    assert start_date.date() == date(2026, 1, 7)
    assert source == "trading_calendar"
    assert cursors[0].params == (date(2026, 1, 15), 6)


def test_resolve_inference_start_date_strict_fails_without_calendar_history(monkeypatch):
    monkeypatch.setenv("AISTOCK_STRICT_INFERENCE", "1")
    monkeypatch.setattr(inference_engine, "get_conn", lambda: _CalendarConn([date(2026, 1, 15)], []))

    with pytest.raises(ValueError, match="insufficient trading-calendar history"):
        inference_engine.InferenceEngine()._resolve_inference_start_date(
            pd.Timestamp("2026-01-15"),
            required_window=5,
            buffer_days=2,
        )

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
