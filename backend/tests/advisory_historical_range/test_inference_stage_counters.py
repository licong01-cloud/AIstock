from datetime import datetime

import pytest

from backend.inference_engine import InferenceEngine
from backend.services.advisory_historical_range import inference_session


def test_stage_timings_separate_forward_from_preparation(monkeypatch):
    ticks = iter([0.0, 12.0, 14.0, 15.0])
    monkeypatch.setattr(inference_session.time, "perf_counter", lambda: next(ticks))
    timings = inference_session.InferenceStageTimings()
    timings.mark("input_and_factors")
    timings.mark("model_forward")
    result = timings.result(score_count=5000)
    assert result["stage_seconds"] == {"input_and_factors": 12.0, "model_forward": 2.0}
    assert result["total_seconds"] == 15.0
    assert result["counters"]["score_count"] == 5000


def test_performance_reference_cannot_write_even_when_invoked_without_cli(monkeypatch):
    engine = InferenceEngine(performance_reference=True)
    monkeypatch.setattr(engine, "_get_latest_available_trade_date", lambda *_: pytest.fail("database lookup must not occur"))
    with pytest.raises(ValueError, match="reference must be read-only"):
        engine._run_inference_impl(strategy_id="", version_tag="performance-probe", trade_date=datetime(2026, 1, 5))
