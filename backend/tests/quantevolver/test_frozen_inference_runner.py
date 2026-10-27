import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_spec = importlib.util.spec_from_file_location("qe_frozen_inference", Path(__file__).resolve().parents[3] / "scripts/qe_frozen_inference.py")
worker = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(worker)


def pipeline():
    obj = worker.FrozenPipeline.__new__(worker.FrozenPipeline)
    obj.spec = {"feature_names": ["F"], "step_len": 20}
    obj.manifest = {"source_task_id": "original", "source_loop_index": 1}
    obj.test = tuple(map(pd.Timestamp, ["2026-01-05", "2026-09-30"]))
    obj.source_test = ["2026-01-05", "2026-06-30"]
    obj.prediction_checked = False
    obj.raw_rows = 3
    obj.raw_start = "2025-12-01"
    index = pd.MultiIndex.from_tuples([(pd.Timestamp(d), "SH600000") for d in ["2026-01-05", "2026-06-30", "2026-09-30"]],
                                    names=["datetime", "instrument"])
    obj.frame = pd.DataFrame({"score": np.array([0.1, 0.2, 0.3], dtype="float64")}, index=index)
    obj.reference = obj.frame.iloc[:2].copy()
    return obj


def test_original_overlap_verified_before_account():
    obj = pipeline()
    obj.check_prediction(obj.frame)
    assert obj.result["fit_calls"] == 0 and obj.result["reference_equivalent"]
    assert obj.result["reference_rows"] == 2
    with pytest.raises(ValueError, match="ALREADY_CHECKED"):
        obj.check_prediction(obj.frame)


@pytest.mark.parametrize("failure", ["scores", "panel", "short_window", "duplicate", "nan"])
def test_reference_drift_cannot_be_accepted(failure):
    obj = pipeline()
    frame = obj.frame.copy()
    if failure == "scores":
        frame.iloc[0, 0] += 0.01
    elif failure == "panel":
        frame = frame.drop(frame.index[1])
    elif failure == "short_window":
        frame = frame.iloc[:-1]
    elif failure == "duplicate":
        frame = pd.concat([frame, frame.iloc[:1]])
    else:
        frame.iloc[0, 0] = np.nan
    with pytest.raises(ValueError, match="QE_FROZEN_"):
        obj.check_prediction(frame)
    assert not obj.prediction_checked


def test_unverified_prediction_cannot_finish():
    with pytest.raises(ValueError, match="REFERENCE_CHECK_REQUIRED"):
        pipeline().finish()


def test_hidden_worker_manifest_cannot_select_training(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    worker.validate_mode({}, backtest_only=False)
    (tmp_path / "qe_frozen_inference_manifest.json").write_text("{}")
    with pytest.raises(ValueError, match="MANIFEST_OPT_IN_MISMATCH"):
        worker.validate_mode({}, backtest_only=True)
    with pytest.raises(ValueError, match="REQUIRES_BACKTEST_ONLY"):
        worker.validate_mode({"qe_frozen_inference": {}}, backtest_only=False)


def test_small_numeric_drift_is_not_silently_allowed_to_change_trades():
    obj = pipeline()
    frame = obj.frame.copy()
    frame.iloc[0, 0] += 1e-12
    with pytest.raises(ValueError, match="NUMERIC_DRIFT_REQUIRES_REVIEW"):
        obj.check_prediction(frame)
    assert not obj.prediction_checked
